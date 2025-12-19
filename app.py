from dotenv import load_dotenv
load_dotenv()

import os
import json
import re
from datetime import datetime

from flask import Flask, request, jsonify, send_from_directory
from openai import OpenAI
from pydantic import BaseModel, ValidationError

import gspread
from google.oauth2.service_account import Credentials

app = Flask(__name__)
client = OpenAI()

# -----------------------------
# Load business profile once
# -----------------------------
BUSINESS_PROFILE_PATH = "business_profile.txt"
if not os.path.exists(BUSINESS_PROFILE_PATH):
    raise FileNotFoundError("business_profile.txt not found in the same folder as app.py.")

with open(BUSINESS_PROFILE_PATH, "r", encoding="utf-8") as f:
    business_profile = f.read()

# -----------------------------
# Google Sheets config (FIXED SCOPES)
# -----------------------------
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

SHEET_NAME = os.environ.get("GOOGLE_SHEET_NAME", "World Cup AI Reservations")


def get_gspread_client():
    """
    Render-safe credentials loading:
      - Preferred (Render): GOOGLE_CREDS_JSON env var (paste full JSON content)
      - Local fallback: google_creds.json in project folder
    """
    if os.environ.get("GOOGLE_CREDS_JSON"):
        creds_info = json.loads(os.environ["GOOGLE_CREDS_JSON"])
        creds = Credentials.from_service_account_info(creds_info, scopes=SCOPES)
        return gspread.authorize(creds)

    creds_file = "google_creds.json"
    if os.path.exists(creds_file):
        creds = Credentials.from_service_account_file(creds_file, scopes=SCOPES)
        return gspread.authorize(creds)

    raise RuntimeError(
        "Google credentials not found. Set GOOGLE_CREDS_JSON (Render) or provide google_creds.json locally."
    )


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


@app.route("/test-sheet")
def test_sheet():
    """
    Quick diagnostic: writes a test row to the target sheet.
    Visit: /test-sheet
    """
    try:
        append_lead_to_sheet("TEST_NAME", "2145551212", "12/20/2025", "7pm", 4)
        return jsonify({"ok": True, "sheet": SHEET_NAME, "message": "✅ Test row appended."})
    except Exception as e:
        return jsonify({"ok": False, "sheet": SHEET_NAME, "error": repr(e)}), 500


@app.route("/chat", methods=["POST"])
def chat():
    data = request.get_json(force=True)

    # Support both single message and optional history
    user_message = (data.get("message") or "").strip()
    history = data.get("history") or []  # list of {"role": "user|assistant", "content": "..."}

    if not user_message:
        return jsonify({"reply": "Please type a message."})

    system_msg = f"""
You are a World Cup 2026 AI Concierge for a Dallas-area business.

Use the following business profile as your source of truth:
{business_profile}

Rules for reservations:
- If the user wants a reservation, collect ONLY: date, time, party size, name, phone number.
- Once you have all 5, output ONE line exactly like:
  RESERVATION_JSON={{"name":"...","phone":"...","date":"...","time":"...","party_size":4}}
- Do not put extra characters inside the braces.
"""

    # Build messages with history so the bot stops forgetting
    messages = [{"role": "system", "content": system_msg}]
    for m in history:
        if isinstance(m, dict) and m.get("role") in ("user", "assistant") and isinstance(m.get("content"), str):
            messages.append({"role": m["role"], "content": m["content"]})
    messages.append({"role": "user", "content": user_message})

    resp = client.responses.create(
        model="gpt-4o-mini",
        input=messages,
    )

    reply = (resp.output_text or "").strip()

    # -----------------------------
    # Extract reservation JSON robustly
    # -----------------------------
    if "RESERVATION_JSON" in reply:
        try:
            # Capture only the {...} JSON object after RESERVATION_JSON=
            match = re.search(r"RESERVATION_JSON\s*=\s*(\{.*?\})", reply, re.DOTALL)
            if not match:
                return jsonify({"reply": reply})

            json_text = match.group(1).strip()
            lead_dict = json.loads(json_text)
            lead = ReservationLead(**lead_dict)

            # Save to Google Sheets
            append_lead_to_sheet(lead.name, lead.phone, lead.date, lead.time, lead.party_size)

            # Remove JSON from visible reply
            visible_reply = re.sub(
                r"\s*RESERVATION_JSON\s*=\s*\{.*?\}\s*",
                "\n",
                reply,
                flags=re.DOTALL
            ).strip()

            confirmation = (
                f"✅ Saved to Google Sheet: {SHEET_NAME}\n"
                f"Name: {lead.name}\n"
                f"Phone: {lead.phone}\n"
                f"Date: {lead.date}\n"
                f"Time: {lead.time}\n"
                f"Party size: {lead.party_size}"
            )

            if visible_reply:
                return jsonify({"reply": (visible_reply + "\n\n" + confirmation).strip()})
            return jsonify({"reply": confirmation})

        except (json.JSONDecodeError, ValidationError) as e:
            return jsonify({"reply": reply + f"\n\n⚠️ JSON/validation error: {repr(e)}"})

        except Exception as e:
            return jsonify({
                "reply": reply + f"\n\n⚠️ Google Sheets save error: {repr(e)}\nSheet: {SHEET_NAME}"
            }), 500

    return jsonify({"reply": reply})


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5050))
    app.run(host="0.0.0.0", port=port, debug=False)
