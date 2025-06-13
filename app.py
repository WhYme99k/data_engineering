from flask import Flask, request, jsonify
import os
from get_PGN import convert_parquet_to_pgn

app = Flask(__name__)

PARQUET_DIR = "duckdb_cli-windows-amd64"
PGN_OUTPUT_DIR = "pgn_output"
PREMIUM_PASSWORD = "chess_premium"

os.makedirs(PGN_OUTPUT_DIR, exist_ok=True)

@app.route('/export_pgn', methods=['POST'])
def export_pgn():
    data = request.json
    files = data.get("files", [])
    password = data.get("password", "")

    results = []

    for file in files:
        file_path = os.path.join(PARQUET_DIR, file)

        if "premium" in file.lower() and password != PREMIUM_PASSWORD:
            results.append({"file": file, "status": "denied", "reason": "premium access required"})
            continue

        if not os.path.isfile(file_path):
            results.append({"file": file, "status": "error", "reason": "file not found"})
            continue

        output_file = os.path.join(PGN_OUTPUT_DIR, file.replace(".parquet", ".pgn"))
        try:
            convert_parquet_to_pgn(file_path, output_file)
            results.append({"file": file, "status": "success", "output": output_file})
        except Exception as e:
            results.append({"file": file, "status": "error", "reason": str(e)})

    return jsonify(results)

if __name__ == '__main__':
    app.run(debug=True)
