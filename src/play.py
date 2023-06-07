import random
import tkinter as tk

import pyspark.sql as sql
import pyspark.sql.functions as sfn
import matplotlib.pyplot as plt

import analyze

# This module plays games of Tetris using the averages computed in 
# generate.py as an optimal strategy.

class Avgs():

    # This class handles the retrieval of averages from the tables computed
    # in generate.py.

    def __init__(self, spark, height, width, memory = False):

        # The class can operate in memory mode if the number of tower configurations is
        # small enough, or it can read data directly from the parquet files.

        num_pieces = int(height*width / 4)
        self.board_avgs = spark.read.format("parquet").load(f"saved/avgs/{width}-{height}/{num_pieces}")
        if memory:
            self.board_dict = {row.asDict()["id"]:row.asDict()["avg"] for row in self.board_avgs.toLocalIterator()}
        self.memory = memory

    def get(self, board_id):
        if self.memory:
            avg = float(self.board_dict.get(int(board_id), 0))
        else:
            rows = self.board_avgs.filter(sfn.col("id") == board_id).select("avg").collect()
            avg = rows[0].asDict()["avg"] if rows else 0
        return avg

def play_game(start_board, height, num_moves, avgs):

    # This function plays a game of Tetris using the
    # provided starting configuration, height, and number
    # of moves. For each piece, the placement with the
    # highest average is chosen.

    board = start_board
    moves = [start_board]
    for _ in range(num_moves, 0, -1):
        piece = random.choice(list(analyze.pieces.keys()))
        connections = analyze.get_connections(board, height)
        choices = []
        for board in connections[piece]:
            board_id = analyze.get_board_id(board, height)
            avg = avgs.get(board_id)
            choices.append((board, avg))
        if len(choices) == 0:
            break
        board = max(choices, key = lambda tupl:tupl[1])[0]
        moves.append(board)
    return moves

def multiple_runs(runs, start_board, height, num_pieces, avgs):

    # This function plays a given number of Tetris games using the
    # specified parameters, and then prints the theoretical average,
    # observed average, and best performance.

    expected = avgs.get(analyze.get_board_id(start_board, height))
    move_lists = []
    for game in range(runs):
        moves = play_game(start_board, height, num_pieces, avgs)
        move_lists.append(moves)
        print(f"Game {game + 1} / {runs}", end = "\r")
    totals = [len(moves[1:]) for moves in move_lists]
    print("")
    print(f"Exp: {expected:.4f} | Avg: {sum(totals) / runs:.4f} | Best:{max(totals)}")
    return move_lists

if __name__ == "__main__":
    board = 1
    height = 20
    width = 4
    runs = int(1e5)
    num_pieces = int(height*width / 4)

    spark = sql.SparkSession.builder.config("spark.driver.memory", "15g").config('spark.ui.showConsoleProgress', False).getOrCreate()
    avgs = Avgs(spark, height, width, memory = True)
    start_board = analyze.get_board_columns(board, height, width)
    multiple_runs(runs, start_board, height, num_pieces, avgs)
