from flask import Blueprint, jsonify, request

bp_simulate = Blueprint("simulate", __name__)

@bp_simulate.route("/simulate", methods=["POST"])
def simulate():
    return jsonify({"message": "simulate endpoint placeholder"}), 200

@bp_simulate.route("/upload-dataset", methods=["POST"])
def upload_dataset():
    return jsonify({"message": "dataset upload placeholder"}), 200