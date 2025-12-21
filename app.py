from dotenv import load_dotenv
load_dotenv()

import os
import json
import re
import uuid
from datetime import datetime, timezone

from flask import Flask, request, jsonify, send_from_directory
from openai import OpenAI

import gspread
from google.oauth2.service_account import Credentials

# -----------------------------
# App setup
# -----------------------------
app = Flask(__name__)
client = OpenAI()

# Disable caching so Render updates show immediately
app.config["SEND_FILE_MAX_AGE_DEFAULT"] = 0

@app.after_request
def add_no_cache_headers(response):
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response

# -----------------------------
# Business profile
# -----------------------------
BUSINESS_PROFILE_PATH = "business_profile.txt"
if not os.path.exists(BUSINESS_PROFILE_PATH):
    raise FileNotFoundError("business_profile.txt not found in the same folder as app.py.")

with open(BUSINESS_PROFILE_PATH, "r", encoding="utf-8") as f:
    business_profile = f.read()

# -----------------------------
# Google Sheets
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

    # Local fallback
    creds_file = "google_creds.json"
    if os.path.exists(creds_file):
        creds = Credentials.from_service_account_file(creds_file, scopes=SCOPES)
        return gspread.authorize(creds)

    raise RuntimeError("Google credentials not found. Set GOOGLE_CREDS_JSON (Render) or provide google_creds.json locally.")

def append_lead_to_sheet(lead: dict):
    gc = get_gspread_client()
    ws = gc.open(SHEET_NAME).sheet1
    ws.append_row([
        datetime.now().isoformat(timespec="seconds"),
        lead.get("name", ""),
        lead.get("phone", ""),
        lead.get("date", ""),
        lead.get("time", ""),
        int(lead.get("party_size", 0)),
        lead.get("source", "web"),
        lead.get("language", "auto"),
    ])

# -----------------------------
# Dallas match schedule (server-driven)
# You can update this list anytime without changing index.html.
# Times are CT; keeping them as strings for display.
# -----------------------------
DALLAS_MATCHES = [
    {"date": "2026-06-14", "time_ct": "3:00 PM", "stage": "Group Stage", "match": "Netherlands vs Japan"},
    {"date": "2026-06-17", "time_ct": "3:00 PM", "stage": "Group Stage", "match": "England vs Croatia"},
    {"date": "2026-06-22", "time_ct": "12:00 PM", "stage": "Group Stage", "match": "Argentina vs Austria"},
    {"date": "2026-06-25", "time_ct": "6:00 PM", "stage": "Group Stage", "match": "Japan vs TBD"},
    {"date": "2026-06-27", "time_ct": "9:00 PM", "stage": "Group Stage", "match": "Jordan vs Argentina"},
    {"date": "2026-06-30", "time_ct": "TBD", "stage": "Round of 32", "match": "TBD"},
    {"date": "2026-07-03", "time_ct": "TBD", "stage": "Round of 32", "match": "TBD"},
    {"date": "2026-07-06", "time_ct": "TBD", "stage": "Round of 16", "match": "TBD"},
    {"date": "2026-07-14", "time_ct": "TBD", "stage": "Semifinal", "match": "Semifinal (Dallas)"},
]

def schedule_as_text():
    lines = []
    for m in DALLAS_MATCHES:
        lines.append(f"- {m['date']} {m['time_ct']} CT — {m['stage']}: {m['match']}")
    return "\n".join(lines)

# -----------------------------
# Reservation state machine (server-side)
# -----------------------------
# In-memory sessions (good for MVP; later can move to Redis)
SESSIONS = {}

RE_PHONE = re.compile(r"(\+?1?\s*)?(\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4})")
RE_PARTY = re.compile(r"(party|table)\s*(of|for)?\s*(\d{1,2})", re.IGNORECASE)
RE_TIME = re.compile(r"\b(\d{1,2})(?::(\d{2}))?\s*(am|pm)\b", re.IGNORECASE)

def normalize_phone(raw: str) -> str:
    digits = re.sub(r"\D", "", raw or "")
    if len(digits) == 11 and digits.startswith("1"):
        digits = digits[1:]
    if len(digits) == 10:
        return f"+1{digits}"
    # fallback to original
    return raw.strip()

def parse_date_to_iso(user_text: str) -> str | None:
    """
    Minimal, safe parser for:
    - YYYY-MM-DD
    - MM/DD/YYYY or M/D/YYYY
    - 'June 23' -> assumes 2026 (World Cup year) for this project
    """
    t = (user_text or "").strip()

    # YYYY-MM-DD
    m = re.search(r"\b(20\d{2})-(\d{2})-(\d{2})\b", t)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"

    # MM/DD/YYYY
    m = re.search(r"\b(\d{1,2})/(\d{1,2})/(20\d{2})\b", t)
    if m:
        mm = int(m.group(1))
        dd = int(m.group(2))
        yyyy = int(m.group(3))
        if 1 <= mm <= 12 and 1 <= dd <= 31:
            return f"{yyyy:04d}-{mm:02d}-{dd:02d}"

    # Month name + day (assume 2026)
    m = re.search(r"\b(jan|feb|mar|apr|may|jun|june|jul|aug|sep|sept|oct|nov|dec)[a-z]*\s+(\d{1,2})\b", t, re.IGNORECASE)
    if m:
        mon = m.group(1).lower()
        day = int(m.group(2))
        month_map = {
            "jan":1,"feb":2,"mar":3,"apr":4,"may":5,"jun":6,"june":6,"jul":7,"aug":8,"sep":9,"sept":9,"oct":10,"nov":11,"dec":12
        }
        mm = month_map.get(mon[:4], month_map.get(mon[:3]))
        if mm and 1 <= day <= 31:
            return f"2026-{mm:02d}-{day:02d}"

    return None

def parse_time_display(user_text: str) -> str | None:
    """
    Returns something like '7pm' or '7:30pm' (for display).
    """
    m = RE_TIME.search(user_text or "")
    if not m:
        return None
    hh = int(m.group(1))
    mm = m.group(2)
    ap = (m.group(3) or "").lower()
    if not (1 <= hh <= 12) or ap not in ("am", "pm"):
        return None
    if mm:
        return f"{hh}:{mm}{ap}"
    return f"{hh}{ap}"

def is_match_day(date_iso: str) -> bool:
    return any(m["date"] == date_iso for m in DALLAS_MATCHES)

def extract_fields_from_message(msg: str) -> dict:
    """
    Extracts any of: phone, party_size, time, date_iso, name
    Name is only extracted if message looks like a name-only input.
    """
    out = {}

    # phone
    pm = RE_PHONE.search(msg or "")
    if pm:
        out["phone"] = normalize_phone(pm.group(0))

    # party
    m = RE_PARTY.search(msg or "")
    if m:
        out["party_size"] = int(m.group(3))

    # time
    t = parse_time_display(msg or "")
    if t:
        out["time"] = t

    # date
    d = parse_date_to_iso(msg or "")
    if d:
        out["date"] = d

    # name heuristic: short text, mostly letters, not just "reservation"
    cleaned = (msg or "").strip()
    if cleaned and len(cleaned) <= 40:
        if re.fullmatch(r"[A-Za-z][A-Za-z\s'.-]{0,39}", cleaned) and cleaned.lower() not in ("reservation", "book", "book a table"):
            out["name"] = cleaned.strip()

    return out

def missing_fields(state: dict) -> list[str]:
    req = ["date", "time", "party_size", "name", "phone"]
    return [k for k in req if not state.get(k)]

def next_prompt_for_missing(missing: list[str], lang: str) -> str:
    # Minimal localized prompts (English + Spanish; auto -> English)
    is_es = (lang or "").lower().startswith("es")
    if not missing:
        return ""

    field = missing[0]
    prompts_en = {
        "date": "What date would you like to reserve? (Example: 2026-06-23)",
        "time": "What time? (Example: 7pm)",
        "party_size": "How many people? (Example: party of 4)",
        "name": "What name should I put the reservation under?",
        "phone": "What phone number should we use for the reservation?"
    }
    prompts_es = {
        "date": "¿Para qué fecha quieres la reserva? (Ej: 2026-06-23)",
        "time": "¿A qué hora? (Ej: 7pm)",
        "party_size": "¿Para cuántas personas? (Ej: mesa para 4)",
        "name": "¿A nombre de quién es la reserva?",
        "phone": "¿Qué número de teléfono usamos para la reserva?"
    }
    return (prompts_es if is_es else prompts_en)[field]

def recall_text(state: dict, lang: str) -> str:
    is_es = (lang or "").lower().startswith("es")
    lines = []
    if is_es:
        lines.append("📌 **Tu reserva hasta ahora:**")
        labels = {"date":"Fecha","time":"Hora","party_size":"Personas","name":"Nombre","phone":"Teléfono"}
    else:
        lines.append("📌 **Your reservation so far:**")
        labels = {"date":"Date","time":"Time","party_size":"Party size","name":"Name","phone":"Phone"}

    for k in ["date","time","party_size","name","phone"]:
        v = state.get(k)
        lines.append(f"- {labels[k]}: {v if v else '—'}")
    missing = missing_fields(state)
    if missing:
        lines.append("")
        lines.append(("Missing:" if not is_es else "Falta:") + " " + ", ".join(missing))
    return "\n".join(lines)

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

@app.route("/api/schedule")
def api_schedule():
    return jsonify({
        "timezone": "America/Chicago",
        "matches": DALLAS_MATCHES
    })

@app.route("/api/recall")
def api_recall():
    session_id = request.args.get("session_id") or ""
    state = SESSIONS.get(session_id, {}).get("reservation", {})
    lang = SESSIONS.get(session_id, {}).get("language", "auto")
    return jsonify({"session_id": session_id, "state": state, "text": recall_text(state, lang)})

@app.route("/test-sheet")
def test_sheet():
    try:
        append_lead_to_sheet({
            "name": "TEST_NAME",
            "phone": "+12145551212",
            "date": "2026-06-23",
            "time": "7pm",
            "party_size": 4,
            "source": "test",
            "language": "auto"
        })
        return jsonify({"ok": True, "sheet": SHEET_NAME, "message": "✅ Test row appended."})
    except Exception as e:
        return jsonify({"ok": False, "sheet": SHEET_NAME, "error": repr(e)}), 500

@app.route("/chat", methods=["POST"])
def chat():
    data = request.get_json(force=True)

    user_message = (data.get("message") or "").strip()
    history = data.get("history") or []  # [{role,content}]
    session_id = (data.get("session_id") or "").strip() or str(uuid.uuid4())
    preferred_lang = (data.get("preferred_language") or "auto").strip().lower()

    if not user_message:
        return jsonify({"reply": "Please type a message.", "session_id": session_id})

    # Init session
    if session_id not in SESSIONS:
        SESSIONS[session_id] = {"reservation": {}, "language": "auto"}

    # Update preferred language if user toggled it
    if preferred_lang in ("auto", "en", "es", "pt", "fr"):
        SESSIONS[session_id]["language"] = preferred_lang

    lang = SESSIONS[session_id]["language"]
    state = SESSIONS[session_id]["reservation"]

    # User asked to recall
    if re.search(r"\b(recall|show)\b.*\b(reservation)\b", user_message, re.IGNORECASE) or user_message.strip().lower() in ("recall reservation", "recall reservation so far"):
        return jsonify({"reply": recall_text(state, lang), "session_id": session_id})

    # Detect reservation intent
    wants_res = bool(re.search(r"\b(reservation|reserve|book a table|book table|table)\b", user_message, re.IGNORECASE))

    # Always attempt to extract fields (even across turns)
    extracted = extract_fields_from_message(user_message)
    for k, v in extracted.items():
        state[k] = v

    # If user is in reservation flow (intent OR already started collecting)
    in_flow = wants_res or any(state.get(k) for k in ("date","time","party_size","name","phone"))

    if in_flow:
        # Validate impossible dates like June 31 (basic)
        if "date" in extracted and extracted["date"]:
            # quick sanity: reject June 31 (or any YYYY-MM-DD where day=31 and month in 04,06,09,11)
            try:
                yyyy, mm, dd = map(int, extracted["date"].split("-"))
                if mm in (4, 6, 9, 11) and dd == 31:
                    state["date"] = None
                    msg = "June only has 30 days. Please provide a valid date (example: 2026-06-23)."
                    return jsonify({"reply": msg, "session_id": session_id})
            except Exception:
                pass

        miss = missing_fields(state)
        if miss:
            prompt = next_prompt_for_missing(miss, lang)
            # Include a small status line so users feel progress
            status = recall_text(state, lang)
            return jsonify({"reply": f"{status}\n\n{prompt}", "session_id": session_id})

        # Completed -> save lead
        lead = {
            "name": state["name"],
            "phone": state["phone"],
            "date": state["date"],
            "time": state["time"],
            "party_size": int(state["party_size"]),
            "source": "web",
            "language": lang,
        }
        append_lead_to_sheet(lead)

        # Match-day banner if date is a Dallas match day
        md = is_match_day(state["date"])
        # Clear reservation state after save (so new reservations start clean)
        SESSIONS[session_id]["reservation"] = {}

        if lang == "es":
            base = (
                f"✅ ¡Reserva guardada!\n"
                f"- Nombre: {lead['name']}\n- Tel: {lead['phone']}\n- Fecha: {lead['date']}\n- Hora: {lead['time']}\n- Personas: {lead['party_size']}\n"
            )
            if md:
                base += "\n🏟️ ¡Ese día hay partido en Dallas! Te recomendamos llegar temprano."
        else:
            base = (
                f"✅ Reservation saved!\n"
                f"- Name: {lead['name']}\n- Phone: {lead['phone']}\n- Date: {lead['date']}\n- Time: {lead['time']}\n- Party size: {lead['party_size']}\n"
            )
            if md:
                base += "\n🏟️ That date is a Dallas match day — we recommend arriving early."

        return jsonify({"reply": base, "session_id": session_id})

    # Otherwise: normal Q&A through model, with schedule context
    lang_instruction = {
        "auto": "Auto-detect the user's language and respond in it (English/Spanish/Portuguese/French).",
        "en": "Respond in English.",
        "es": "Responde en Español.",
        "pt": "Responda em Português.",
        "fr": "Réponds en Français."
    }.get(lang, "Auto-detect the user's language and respond in it.")

    system_msg = f"""
You are a World Cup 2026 Dallas AI Concierge for a local business.

Business profile (source of truth):
{business_profile}

Dallas World Cup match dates at AT&T Stadium (for match-day planning):
{schedule_as_text()}

Rules:
- Be friendly, fast, and concise.
- If the user asks for reservations, tell them to type "reservation" and you'll guide them step-by-step.
- {lang_instruction}
- Never mention being an AI unless asked directly.
"""

    messages = [{"role": "system", "content": system_msg}]
    for m in history:
        if isinstance(m, dict) and m.get("role") in ("user", "assistant") and isinstance(m.get("content"), str):
            messages.append({"role": m["role"], "content": m["content"]})
    messages.append({"role": "user", "content": user_message})

    resp = client.responses.create(
        model="gpt-4o-mini",
        input=messages,
    )

    reply = (resp.output_text or "").strip() or "Sorry — I couldn’t generate a response."
    return jsonify({"reply": reply, "session_id": session_id})

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5050))
    app.run(host="0.0.0.0", port=port, debug=False)