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
# Cache-busting (helps Render show latest HTML/JS)
# -----------------------------
app.config["SEND_FILE_MAX_AGE_DEFAULT"] = 0

@app.after_request
def add_no_cache_headers(response):
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response


# -----------------------------
# Config
# -----------------------------
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

SHEET_NAME = os.environ.get("GOOGLE_SHEET_NAME", "World Cup AI Reservations")

# UI/Branding configuration (served to frontend)
SUPPORTED_LANGUAGES = [
    {"code": "auto", "label": "Auto"},
    {"code": "en", "label": "English"},
    {"code": "es", "label": "Español"},
    {"code": "pt", "label": "Português"},
    {"code": "fr", "label": "Français"},
]

# FIFA confirms tournament kickoff June 11, 2026 (we use a safe local countdown)
TOURNAMENT_START_LOCAL = os.environ.get("TOURNAMENT_START_LOCAL", "2026-06-11T00:00:00")

# Dallas match-day banner text (customize anytime in Render env vars)
MATCHDAY_BANNER = os.environ.get(
    "MATCHDAY_BANNER",
    "Dallas Match-Day Banner: Opening at 11am on match days • Walk-ins welcome • Reserve ahead"
)

# Optional: load business profile from a file (default business_profile.txt)
BUSINESS_PROFILE_PATH = os.environ.get("BUSINESS_PROFILE_PATH", "business_profile.txt")
if not os.path.exists(BUSINESS_PROFILE_PATH):
    raise FileNotFoundError(f"{BUSINESS_PROFILE_PATH} not found in the same folder as app.py.")

with open(BUSINESS_PROFILE_PATH, "r", encoding="utf-8") as f:
    business_profile = f.read()

# Dynamic schedule loaded from server file (so you can update without touching index.html)
SCHEDULE_FILE = os.environ.get("DALLAS_SCHEDULE_FILE", "matches_dallas.json")


def load_dallas_schedule():
    """
    Loads Dallas match schedule from a JSON file if present.
    If missing, uses a built-in default skeleton (dates + stages).
    JSON format:
    [
      {"date":"2026-06-14","time":"15:00","tz":"CT","stage":"Group Stage","match":"Netherlands vs Japan","venue":"Arlington Stadium"},
      ...
    ]
    """
    if os.path.exists(SCHEDULE_FILE):
        with open(SCHEDULE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)

    # Built-in fallback (you can replace with the full FIFA list via matches_dallas.json)
    return [
        {"date": "2026-06-14", "time": "15:00", "tz": "CT", "stage": "Group Stage", "match": "TBD", "venue": "Dallas Stadium (Arlington)"},
        {"date": "2026-06-17", "time": "15:00", "tz": "CT", "stage": "Group Stage", "match": "TBD", "venue": "Dallas Stadium (Arlington)"},
        {"date": "2026-06-22", "time": "12:00", "tz": "CT", "stage": "Group Stage", "match": "TBD", "venue": "Dallas Stadium (Arlington)"},
        {"date": "2026-06-25", "time": "18:00", "tz": "CT", "stage": "Group Stage", "match": "TBD", "venue": "Dallas Stadium (Arlington)"},
        {"date": "2026-06-27", "time": "21:00", "tz": "CT", "stage": "Group Stage", "match": "TBD", "venue": "Dallas Stadium (Arlington)"},
        {"date": "2026-06-30", "time": "TBD", "tz": "CT", "stage": "Round of 32", "match": "TBD", "venue": "Dallas Stadium (Arlington)"},
        {"date": "2026-07-03", "time": "TBD", "tz": "CT", "stage": "Round of 32", "match": "TBD", "venue": "Dallas Stadium (Arlington)"},
        {"date": "2026-07-06", "time": "TBD", "tz": "CT", "stage": "Round of 16", "match": "TBD", "venue": "Dallas Stadium (Arlington)"},
        {"date": "2026-07-14", "time": "TBD", "tz": "CT", "stage": "Semi-final", "match": "TBD", "venue": "Dallas Stadium (Arlington)"},
    ]


# -----------------------------
# Google Sheets helpers
# -----------------------------
def get_gspread_client():
    """
    Render: set GOOGLE_CREDS_JSON env var (paste full one-line JSON).
    Local fallback: google_creds.json in project folder.
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


@app.route("/version")
def version():
    try:
        mtime = os.path.getmtime("index.html")
        return jsonify({
            "index_html_last_modified_epoch": mtime,
            "index_html_last_modified": datetime.fromtimestamp(mtime).isoformat(timespec="seconds"),
        })
    except Exception as e:
        return jsonify({"error": repr(e)}), 500


@app.route("/api/config")
def api_config():
    return jsonify({
        "supported_languages": SUPPORTED_LANGUAGES,
        "tournament_start_local": TOURNAMENT_START_LOCAL,
        "matchday_banner": MATCHDAY_BANNER,
        "sheet_name": SHEET_NAME,
    })


@app.route("/api/schedule")
def api_schedule():
    return jsonify({
        "host_city": "Dallas (Arlington)",
        "venue": "Dallas Stadium (Arlington)",
        "matches": load_dallas_schedule()
    })


@app.route("/test-sheet")
def test_sheet():
    try:
        append_lead_to_sheet("TEST_NAME", "2145551212", "2026-06-14", "15:00", 4)
        return jsonify({"ok": True, "sheet": SHEET_NAME, "message": "✅ Test row appended."})
    except Exception as e:
        return jsonify({"ok": False, "sheet": SHEET_NAME, "error": repr(e)}), 500


@app.route("/chat", methods=["POST"])
def chat():
    data = request.get_json(force=True)

    user_message = (data.get("message") or "").strip()
    history = data.get("history") or []  # list of {"role":"user|assistant","content":"..."}
    preferred_lang = (data.get("preferred_language") or "auto").strip().lower()

    if not user_message:
        return jsonify({"reply": "Please type a message."})

    # Optional “language toggle” instruction (your UI buttons will set preferred_language)
    lang_instruction = ""
    if preferred_lang in ("en", "es", "pt", "fr"):
        lang_map = {"en": "English", "es": "Spanish", "pt": "Portuguese", "fr": "French"}
        lang_instruction = f"\nIMPORTANT: Respond in {lang_map[preferred_lang]}.\n"

    system_msg = f"""
You are a World Cup 2026 Concierge for a Dallas-area business.

Use the following business profile as your source of truth:
{business_profile}

Goals:
1) Answer customer questions about the business (hours, address, specials)
2) Help customers make reservations or inquiries
3) Be friendly, fast, and concise
4) Auto-detect language and respond in it (English, Spanish, Portuguese, French)
{lang_instruction}

Reservation workflow (IMPORTANT):
- If the user wants a reservation, collect ONLY: date, time, party size, name, phone number.
- Once you have all 5, output ONE line exactly like:
  RESERVATION_JSON={{"name":"...","phone":"...","date":"...","time":"...","party_size":4}}
- Do not put extra characters inside the braces.
- If something is unknown, say you’re not sure and offer a callback to the business phone.
- Never mention being an AI unless the user asks directly.
"""

    messages = [{"role": "system", "content": system_msg}]

    # Include browser-provided history (prevents the “looping questions” problem)
    for m in history:
        if isinstance(m, dict) and m.get("role") in ("user", "assistant") and isinstance(m.get("content"), str):
            messages.append({"role": m["role"], "content": m["content"]})

    messages.append({"role": "user", "content": user_message})

    resp = client.responses.create(
        model="gpt-4o-mini",
        input=messages,
    )

    reply = (resp.output_text or "").strip()

    # Save reservation if JSON is present
    if "RESERVATION_JSON" in reply:
        try:
            match = re.search(r"RESERVATION_JSON\s*=\s*(\{.*?\})", reply, re.DOTALL)
            if match:
                lead_dict = json.loads(match.group(1).strip())
                lead = ReservationLead(**lead_dict)

                append_lead_to_sheet(
                    lead.name, lead.phone, lead.date, lead.time, lead.party_size
                )

                # Remove JSON from visible message
                visible_reply = re.sub(
                    r"\s*RESERVATION_JSON\s*=\s*\{.*?\}\s*",
                    "\n",
                    reply,
                    flags=re.DOTALL
                ).strip()

                confirmation = (
                    f"✅ Reservation saved!\n"
                    f"Name: {lead.name}\n"
                    f"Phone: {lead.phone}\n"
                    f"Date: {lead.date}\n"
                    f"Time: {lead.time}\n"
                    f"Party size: {lead.party_size}"
                )

                if visible_reply:
                    return jsonify({"reply": (visible_reply + "\n\n" + confirmation).strip()})
                return jsonify({"reply": confirmation})

        except (json.JSONDecodeError, ValidationError):
            return jsonify({"reply": reply})
        except Exception as e:
            return jsonify({"reply": reply + f"\n\n⚠️ Google Sheets save error: {repr(e)}"}), 500

    return jsonify({"reply": reply})


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5050))
    app.run(host="0.0.0.0", port=port, debug=False)