import pandas as pd
import chess.pgn
import io

def format_result(result):
    # Changes result numbers back into PGN format
    if result == 1.0:
        return "1-0"
    elif result == 0.0:
        return "0-1"
    elif result == 0.5:
        return "1/2-1/2"
    return "*"

def convert_parquet_to_pgn(parquet_path, output_path):
    df = pd.read_parquet(parquet_path)

    with open(output_path, "w", encoding="utf-8") as f:
        for _, row in df.iterrows():
            game = chess.pgn.Game()
            game.headers["White"] = str(row["White"])
            game.headers["Black"] = str(row["Black"])
            game.headers["WhiteElo"] = str(row["WhiteElo"])
            game.headers["BlackElo"] = str(row["BlackElo"])
            game.headers["Result"] = format_result(row["Result"])
            game.headers["Event"] = str(row["Event"]) if pd.notnull(row["Event"]) else "?"

            if pd.notnull(row["BaseTime"]) and pd.notnull(row["Increment"]):
                game.headers["TimeControl"] = f"{int(row['BaseTime'])}+{int(row['Increment'])}"

            node = game
            moves = str(row["Moves"]).strip().split()
            board = game.board()
            for move in moves:
                if move.replace(".", "").isdigit():
                    continue
                node = node.add_variation(board.push_san(move))   
            f.write(str(game) + "\n\n")