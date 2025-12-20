from dotenv import load_dotenv
load_dotenv()

import os
import json
import re
import uuid
from datetime import datetime

from flask import Flask, request, jsonify, send_from_directory, make_response
from openai import OpenAI
from pydantic import BaseModel, ValidationError

import gspread
from google.oauth2.service_account import Credentials

app = Flask(__name__)
client = OpenAI()

# -----------------------------
# Cache-busting for Render/browser
# -----------------------------
app.config["SEND_FILE_MAX_AGE_DEFAULT"] = 0

@app.after_request
def add_no_cache_headers(response):
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response

# -----------------------------
# Simple server-side session memory
# -----------------------------
SESSIONS = {}  # session_id -> list of {"role": "user"|"assistant", "content": "..."}
MAX_TURNS = 24 # keep last 24 messages (12 user+assistant pairs)

def get_session_id():
    sid = request.cookies.get("sid")
    if not sid:
        sid = str(uuid.uuid4())
    if sid not in SESSIONS:
        SESSIONS[sid] = []
    return sid

def add_to_session(sid, role, content):
    SESSIONS[sid].append({"role": role, "content": content})
    # Trim to last MAX_TURNS messages
    if len(SESSIONS[sid]) > MAX_TURNS:
        SESSIONS[sid] = SESSIONS[sid][-MAX_TURNS:]

# -----------------------------
# Load business profile once
# -----------------------------
BUSINESS_PROFILE_PATH = "business_profile.txt"
if not os.path.exists(BUSINESS_PROFILE_PATH):
    raise FileNotFoundError("business_profile.txt not found in the same folder as app.py.")

with open(BUSINESS_PROFILE_PATH, "r", encoding="utf-8") as f:
    business_profile = f.read()

# -----------------------------
# Google Sheets config
# -----------------------------
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]
SHEET_NAME = os.environ.get("GOOGLE_SHEET_NAME", "World Cup AI Reservations")

def get_gspread_client():
    if os.environ.get("GOOGLE_CREDS_JSON"):
        creds_info = json.loads(os.environ["GOOGLE_CREDS_JSON"])
        creds = Credentials.from_service_account_info(creds_info, scopes=SCOPES)
        return gspread.authorize(creds)

    creds_file = "google_creds.json"
    if os.path.exists(creds_file):
        creds = Credentials.from_service_account_file(creds_file, scopes=SCOPES)
        return gspread.authorize(creds)

    raise RuntimeError("Google credentials not found. Set GOOGLE_CREDS_JSON (Render) or provide google_creds.json locally.")

def append_lead_to_sheet(name, phone, date, time, party_size):
    gc = get_gspread_client()
    ws = gc.open(SHEET_NAME).sheet1
    ws.append_row([
        datetime.now().isoformat(timespec="seconds"),
        name,
        phone,
        date,
        time,
        int(party_size),
    ])

# -----------------------------
# Lead schema
# -----------------------------
class ReservationLead(BaseModel):
    name: str
    phone: str
    date: str
    time: str
    party_size: int

# -----------------------------
# Routes
# -----------------------------
@app.route("/")
def home():
    return send_from_directory(".", "index.html")

@app.route("/health")
def health():
    return jsonify({"status": "ok"})

@app.route("/version")
def version():
    mtime = os.path.getmtime("index.html")
    return jsonify({
        "index_html_last_modified_epoch": mtime,
        "index_html_last_modified": datetime.fromtimestamp(mtime).isoformat(timespec="seconds"),
    })

@app.route("/test-sheet")
def test_sheet():
    try:
        append_lead_to_sheet("TEST_NAME", "2145551212", "12/20/2025", "7pm", 4)
        return jsonify({"ok": True, "sheet": SHEET_NAME, "message": "✅ Test row appended."})
    except Exception as e:
        return jsonify({"ok": False, "sheet": SHEET_NAME, "error": repr(e)}), 500

@app.route("/chat", methods=["POST"])
def chat():
    sid = get_session_id()
    data = request.get_json(force=True)

    user_message = (data.get("message") or "").strip()
    client_history = data.get("history") or []  # optional

    if not user_message:
        resp = make_response(jsonify({"reply": "Please type a message."}))
        resp.set_cookie("sid", sid, max_age=60*60*24*30)  # 30 days
        return resp

    system_msg = f"""
You are a World Cup 2026 AI Concierge for a Dallas-area business.

Use the following business profile as your source of truth:
{business_profile}

Reservation rules:
- If the user wants a reservation, collect ONLY: date, time, party size, name, phone number.
- You MUST remember details already provided in this chat and never ask for the same field twice.
- If the user asks "recall my reservation so far", summarize what you have and ask only for missing fields.
- Once you have all 5, output ONE line:
  RESERVATION_JSON={{"name":"...","phone":"...","date":"...","time":"...","party_size":4}}
- Be friendly, fast, and concise.
- Auto-detect language and respond in it (English, Spanish, Portuguese, French).
- Never mention being an AI unless asked directly.
"""

    # Prefer server-side history. If client sends history, merge lightly (optional).
    history = SESSIONS.get(sid, []).copy()

    # If client_history exists and server history is empty (first visit), use it.
    if client_history and not history:
        for m in client_history:
            if isinstance(m, dict) and m.get("role") in ("user", "assistant") and isinstance(m.get("content"), str):
                history.append({"role": m["role"], "content": m["content"]})

    messages = [{"role": "system", "content": system_msg}] + history + [{"role": "user", "content": user_message}]

    resp_ai = client.responses.create(
        model="gpt-4o-mini",
        input=messages,
    )

    reply = (resp_ai.output_text or "").strip()

    # Save both sides to server session
    add_to_session(sid, "user", user_message)
    add_to_session(sid, "assistant", reply)

    # If JSON present, save to Google Sheets
    if "RESERVATION_JSON" in reply:
        try:
            match = re.search(r"RESERVATION_JSON\s*=\s*(\{.*?\})", reply, re.DOTALL)
            if match:
                lead_dict = json.loads(match.group(1).strip())
                lead = ReservationLead(**lead_dict)

                append_lead_to_sheet(lead.name, lead.phone, lead.date, lead.time, lead.party_size)

                cleaned = re.sub(r"\s*RESERVATION_JSON\s*=\s*\{.*?\}\s*", "\n", reply, flags=re.DOTALL).strip()
                confirmation = (
                    f"✅ Saved to Google Sheet: {SHEET_NAME}\n"
                    f"Name: {lead.name}\nPhone: {lead.phone}\nDate: {lead.date}\nTime: {lead.time}\nParty size: {lead.party_size}"
                )
                reply_out = (cleaned + "\n\n" + confirmation).strip()
            else:
                reply_out = reply
        except (json.JSONDecodeError, ValidationError) as e:
            reply_out = reply + f"\n\n⚠️ JSON/validation error: {repr(e)}"
        except Exception as e:
            reply_out = reply + f"\n\n⚠️ Google Sheets save error: {repr(e)}"
    else:
        reply_out = reply

    resp = make_response(jsonify({"reply": reply_out}))
    resp.set_cookie("sid", sid, max_age=60*60*24*30)  # 30 days
    return resp

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5050))
    app.run(host="0.0.0.0", port=port, debug=False)