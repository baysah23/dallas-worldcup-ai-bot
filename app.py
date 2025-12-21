# app_schedule_pagination_fixed.py
from flask import Flask, request, jsonify, send_from_directory
import json

app = Flask(__name__)

# Expect these to already exist in your real app
ALL_MATCHES = globals().get("ALL_MATCHES", [])
DALLAS_MATCHES = globals().get("DALLAS_MATCHES", [])

@app.route("/schedule.json")
def schedule():
    scope = request.args.get("scope", "dallas")
    q = (request.args.get("q") or "").lower()

    matches = DALLAS_MATCHES if scope == "dallas" else ALL_MATCHES

    if q:
        matches = [m for m in matches if q in json.dumps(m).lower()]

    return jsonify({
        "scope": scope,
        "count": len(matches),
        "matches": matches
    })

@app.route("/")
def home():
    return send_from_directory(".", "index.html")
