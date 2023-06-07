import math

# This module contains functions which help to analyze or process
# the play field.

def get_board_columns(board_id, height, width):

    # This function computes the column heights from the
    # given board id.

    board = []
    for x in range(1, width + 1):
        height_power = (height + 1)**(x - 1)
        shift_to_ones = math.floor(board_id / height_power)
        digit = shift_to_ones % (height + 1)
        board.append(digit)
    return board

def get_board_id(board, height):

    # This function computes the board id from the
    # given column heights.

    width = len(board)
    board_id = 0
    for x in range(1, width + 1):
        power = (height + 1)**(x - 1)
        board_id += board[x - 1]*power
    return board_id

def test_piece(board, piece_name, orient, max_height):

    # This function find the positions that a piece can be 
    # placed in without creating any holes. 

    (piece_surface, piece_heights) = pieces[piece_name][orient]
    board_width = len(board)
    piece_width = len(piece_heights)
    positions = []
    for x in range(board_width - piece_width + 1):
        board_slice = board[x:(x + piece_width)]
        board_diff = [col_2 - col_1 for (col_1, col_2) in zip(board_slice[:-1], board_slice[1:])]
        if piece_surface == board_diff:
            if all([p_height + b_height <= max_height for (p_height, b_height) in zip(piece_heights, board_slice)]):
                positions.append(x)
    return positions

def add_piece(board, position, piece, orient):

    # This function generates a new board by adding 
    # a piece at the specified position and orientation.

    piece_heights = pieces[piece][orient][1]
    piece_width = len(piece_heights)
    new_board = board.copy()
    for i in range(piece_width):
        new_board[i + position] += piece_heights[i]
    return new_board

def get_connections(board, max_height):

    # This function computes all boards that can be 
    # constructed from the given board by adding a piece.

    result = {}
    for (piece, piece_map) in pieces.items():
        result[piece] = []
        for orient in range(len(piece_map)):
            for position in test_piece(board, piece, orient, max_height):
                new_board = add_piece(board, position, piece, orient)
                result[piece].append(new_board)
    return result

def get_piece_maps():

    # This function defines the shapes of the seven tetrominos. Each tuple
    # represents a different orientation for the piece, with the left list
    # defining the bottom surface of the piece and the right list giving its
    # height.

    pieces = {}
    pieces["i"] = [([0, 0, 0], [1, 1, 1, 1]), ([], [4])]
    pieces["t"] = [([-1, 1], [1, 2, 1]), ([-1], [1, 3]), ([0, 0], [1, 2, 1]), ([1], [3, 1])]
    pieces["sq"] = [([0], [2, 2])]
    pieces["zr"] = [([0, 1], [1, 2, 1]), ([-1], [2, 2])]
    pieces["zl"] = [([-1, 0], [1, 2, 1]), ([1], [2, 2])]
    pieces["lr"] = [([1, 0], [2, 1, 1]), ([-2], [1, 3]), ([0, 0], [1, 1, 2]), ([0], [3, 1])]
    pieces["ll"] = [([0, -1], [1, 1, 2]), ([0], [1, 3]), ([0, 0], [2, 1, 1]), ([2], [3, 1])]
    return pieces

pieces = get_piece_maps()