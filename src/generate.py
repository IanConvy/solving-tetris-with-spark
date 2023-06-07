import pyspark.sql.functions as sfn
import pyspark.sql.types as stype
import pyspark.sql as sql

import analyze

# This module is used to compute the table of average tower heights that can
# be expected from every configuration.

def get_board_columns(height, width):

    # This function generates a set of Spark Columns that
    # each compute the height of a Tetris column based on the
    # board id in the DataFrame row.

    cols = {}
    for x in range(1, width + 1):
        height_power = (height + 1)**(x - 1)
        shift_to_ones = sfn.floor(sfn.col("id") / height_power)
        digit = shift_to_ones % (height + 1)
        cols[f"{x}"] = digit
    return cols

def get_board_id(height, width):

    # This function generates a Spark Column that computes the 
    # board id based on the heights of the Tetris columns in the
    # same row.

    board_id = sfn.lit(0)
    for x in range(1, width + 1):
        power = (height + 1)**(x - 1)
        board_id += sfn.col(str(x))*power
    return board_id

def generate_connections(height, width, spark_session):

    # Ths function finds all mappings from one tower configuration to
    # another that occur when placing a Tetris piece. The resulting table
    # has columns "id", "new_id", and "piece", with "new_id" being the tower
    # configuration that can be reached by adding "piece" to the board represented
    # by "id".

    max_id = (height + 1)**width 
    ids = spark_session.range(max_id) # DataFrame populated by all possible board ids 
    all_cols = get_board_columns(height, width)
    boards = ids.withColumns(all_cols) # Dataframe now has board heights as well as ids
    schema = stype.StructType([stype.StructField("id", stype.LongType(), False), stype.StructField("new_id", stype.LongType(), True), stype.StructField("piece", stype.StringType(), False)])
    all_connections = spark_session.createDataFrame([], schema = schema) # Empty dataframe that will be iteratively updated
    for (piece_name, maps) in analyze.pieces.items():
        for (surface, piece_heights) in maps:
            piece_width = len(piece_heights)
            if piece_width <= width: # Pieces wider than the board cannot be placed
                for pos in range(1, width - piece_width + 2): # Iterate over all positions that the piece can be stacked at
                    col_list = [sfn.col(f"{x + pos}") for x in range(piece_width)] # List columns that are intersected by piece
                    diff_list = [col_2 - col_1 for (col_1, col_2) in zip(col_list[:-1], col_list[1:])] # Get relative heights of columns
                    board_surfaces = boards.select("*", *diff_list)
                    matched_boards = board_surfaces
                    for (col, diff) in zip(diff_list, surface):
                        matched_boards = matched_boards.filter(col == diff) # Piece can only be placed if it exactly matches the tower surface
                    raised_cols = {f"{x + pos}":(col + added) for (x, (col, added)) in enumerate(zip(col_list, piece_heights))}
                    raised_boards = matched_boards.select(*boards.columns).withColumns(raised_cols) # Generate new board after adding piece
                    valid_boards = raised_boards
                    for col in col_list:
                        valid_boards = valid_boards.filter(col <= height) # Remove tower configuration that are too tall
                    new_id_col = get_board_id(height, width)
                    connections = valid_boards.select("id", new_id_col.alias("new_id"), sfn.lit(piece_name).alias("piece"))
                    all_connections = all_connections.union(connections)
    all_connections.write.format("parquet").mode("OVERWRITE").save(f"saved/connections/{height}-{width}")
    
def generate_avgs(height, width, spark_session):

    # This function uses the connection table created by generate_connections
    # to compute the average number of successful piece placements before a hole
    # is formed. This average is calculated with respect to optimal piece placement,
    # which is enforced via a recursive algorithm.

    all_connections = spark_session.read.format("parquet").load(f"saved/connections/{height}-{width}")
    ids = spark_session.range((height + 1)**width)
    avgs = old_avgs = ids.withColumn("avg", sfn.lit(0.0)) # This is the "average" for a piece sequence of length 0
    max_depth = int(height*width / 4)
    for depth in range(max_depth + 1):
        if depth > 0: # Iterations greater than length 0 are generated from previously-saved results
            avgs.write.format("parquet").mode("OVERWRITE").save(f"saved/avgs/{width}-{height}/{depth}")
            old_avgs = spark_session.read.format("parquet").load(f"saved/avgs/{width}-{height}/{depth}")
        print(f"Depth: {depth}")
        # The first step of the algorithm is to append the previous averages for each "new_id" column in the connections table
        connection_avgs = all_connections.alias("c").join(old_avgs.alias("a"), sfn.col("c.new_id") == sfn.col("a.id"), "left")
        connection_avgs = connection_avgs.select("c.id", "c.piece", "a.avg").fillna(0.0)
        # Then, the maximum avg is chosen for each starting configuration and piece
        best_choice = connection_avgs.groupBy("id", "piece").agg(sfn.max("avg").alias("max_avg"))
        # Finally, a weighted average of one plus the previous max averages is computed
        avgs = best_choice.select("id", (1/7 + sfn.col("max_avg")/7).alias("max_avg")).groupBy("id").agg(sfn.sum("max_avg").alias("avg"))
        depth +=1

if __name__ == "__main__":
    spark = sql.SparkSession.builder.config("spark.driver.memory", "15g").getOrCreate()
    # generate_connections(20, 6, spark)
    generate_avgs(20, 6, spark)
