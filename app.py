from flask import Flask, jsonify
from src.db.connection import get_db
from src.routes.simulate import bp_simulate

app = Flask(__name__)
app.register_blueprint(bp_simulate)

@app.route("/health", methods=["GET"])
def health():
    conn = get_db()
    if conn is None:
        return jsonify({"status": "DB not connected"}), 500
    return jsonify({"status": "ok"}), 200

if __name__ == "__main__":
    app.run(debug=True)