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

# -----------------------------
# App setup
# -----------------------------
app = Flask(__name__)
client = OpenAI()

# Cache-busting so Render updates show immediately
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
# Google Sheets config
# -----------------------------
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]
SHEET_NAME = os.environ.get("GOOGLE_SHEET_NAME", "World Cup AI Reservations")


def get_gspread_client():
    # Render: GOOGLE_CREDS_JSON env var
    if os.environ.get("GOOGLE_CREDS_JSON"):
        creds_info = json.loads(os.environ["GOOGLE_CREDS_JSON"])
        creds = Credentials.from_service_account_info(creds_info, scopes=SCOPES)
        return gspread.authorize(creds)

    # Local fallback
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
# Dallas match schedule (server-driven)
# Sources: Dallas/FWC26 host site + Arlington/VisitDallas press releases + FIFA host city info.
# (Knockout opponents are TBD until bracket is known)
# -----------------------------
DALLAS_MATCHES = [
    {"date": "2026-06-14", "time_ct": "15:00", "stage": "Group Stage", "match": "Netherlands vs Japan", "venue": "Dallas Stadium (Arlington)"},
    {"date": "2026-06-17", "time_ct": "15:00", "stage": "Group Stage", "match": "England vs Croatia", "venue": "Dallas Stadium (Arlington)"},
    {"date": "2026-06-22", "time_ct": "12:00", "stage": "Group Stage", "match": "Argentina vs Austria", "venue": "Dallas Stadium (Arlington)"},
    {"date": "2026-06-25", "time_ct": "18:00", "stage": "Group Stage", "match": "Japan vs UEFA Playoff Winner (TBD)", "venue": "Dallas Stadium (Arlington)"},
    {"date": "2026-06-27", "time_ct": "21:00", "stage": "Group Stage", "match": "Jordan vs Argentina", "venue": "Dallas Stadium (Arlington)"},
    {"date": "2026-06-30", "time_ct": "TBD", "stage": "Round of 32", "match": "TBD vs TBD", "venue": "Dallas Stadium (Arlington)"},
    {"date": "2026-07-03", "time_ct": "TBD", "stage": "Round of 32", "match": "TBD vs TBD", "venue": "Dallas Stadium (Arlington)"},
    {"date": "2026-07-06", "time_ct": "TBD", "stage": "Round of 16", "match": "TBD vs TBD", "venue": "Dallas Stadium (Arlington)"},
    {"date": "2026-07-14", "time_ct": "TBD", "stage": "Semi-final", "match": "TBD vs TBD", "venue": "Dallas Stadium (Arlington)"},
]

# -----------------------------
# Session + reservation state machine
# -----------------------------
# In-memory sessions (fine for now). For true scaling later: Redis/DB.
sessions = {}

REQUIRED_FIELDS = ["date", "time", "party_size", "name", "phone"]

class ReservationLead(BaseModel):
    name: str
    phone: str
    date: str
    time: str
    party_size: int

def get_session_id():
    sid = request.cookies.get("sid")
    if not sid:
        sid = str(uuid.uuid4())
    return sid

def get_session():
    sid = get_session_id()
    if sid not in sessions:
        sessions[sid] = {
            "lang": "en",  # en, es, pt, fr
            "draft": {k: None for k in REQUIRED_FIELDS},
            "updated_at": datetime.now().isoformat(timespec="seconds"),
        }
    return sid, sessions[sid]

def normalize_lang(lang: str):
    lang = (lang or "").lower().strip()
    if lang in ["en", "english"]:
        return "en"
    if lang in ["es", "spanish", "español", "espanol"]:
        return "es"
    if lang in ["pt", "portuguese", "português", "portugues"]:
        return "pt"
    if lang in ["fr", "french", "français", "francais"]:
        return "fr"
    return "en"

LANG_PROMPTS = {
    "en": "Respond in English.",
    "es": "Responde en español.",
    "pt": "Responda em português.",
    "fr": "Répondez en français.",
}

def draft_summary(draft: dict):
    def fmt(v):
        return v if v else "—"
    return (
        f"Reservation so far:\n"
        f"- Date: {fmt(draft.get('date'))}\n"
        f"- Time: {fmt(draft.get('time'))}\n"
        f"- Party size: {fmt(draft.get('party_size'))}\n"
        f"- Name: {fmt(draft.get('name'))}\n"
        f"- Phone: {fmt(draft.get('phone'))}"
    )

def missing_fields(draft: dict):
    return [k for k in REQUIRED_FIELDS if not draft.get(k)]

def is_phone(s: str):
    digits = re.sub(r"\D", "", s or "")
    return len(digits) >= 10

def extract_phone(text: str):
    digits = re.sub(r"\D", "", text or "")
    if len(digits) >= 10:
        return digits[-10:]
    return None

def extract_party_size(text: str):
    # matches "party of 4", "table for 5", "5 people"
    m = re.search(r"\b(party of|table for|for)\s*(\d{1,2})\b", text, re.I)
    if m:
        return int(m.group(2))
    m2 = re.search(r"\b(\d{1,2})\s*(people|persons|guests)\b", text, re.I)
    if m2:
        return int(m2.group(1))
    # standalone number could be party size, but risky. Only accept if user says "party" somewhere.
    if re.search(r"\bparty\b|\btable\b|\bguests\b", text, re.I):
        m3 = re.search(r"\b(\d{1,2})\b", text)
        if m3:
            return int(m3.group(1))
    return None

def extract_time(text: str):
    # examples: 7pm, 7:30 pm, 19:00
    m = re.search(r"\b(\d{1,2})(?::(\d{2}))?\s*(am|pm)\b", text, re.I)
    if m:
        h = int(m.group(1))
        minute = m.group(2) or "00"
        ampm = m.group(3).lower()
        return f"{h}:{minute} {ampm}"
    m2 = re.search(r"\b([01]?\d|2[0-3]):([0-5]\d)\b", text)
    if m2:
        return f"{m2.group(1)}:{m2.group(2)}"
    return None

def extract_date(text: str):
    # Accept YYYY-MM-DD
    m = re.search(r"\b(20\d{2})-(\d{2})-(\d{2})\b", text)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    # Accept MM/DD/YYYY or M/D/YYYY
    m2 = re.search(r"\b(\d{1,2})/(\d{1,2})/(20\d{2})\b", text)
    if m2:
        mm = int(m2.group(1))
        dd = int(m2.group(2))
        yyyy = int(m2.group(3))
        return f"{yyyy:04d}-{mm:02d}-{dd:02d}"
    # Accept "June 23" or "Jun 23"
    months = {
        "jan": 1, "january": 1,
        "feb": 2, "february": 2,
        "mar": 3, "march": 3,
        "apr": 4, "april": 4,
        "may": 5,
        "jun": 6, "june": 6,
        "jul": 7, "july": 7,
        "aug": 8, "august": 8,
        "sep": 9, "sept": 9, "september": 9,
        "oct": 10, "october": 10,
        "nov": 11, "november": 11,
        "dec": 12, "december": 12,
    }
    m3 = re.search(r"\b([A-Za-z]{3,9})\s+(\d{1,2})\b", text)
    if m3:
        mon = months.get(m3.group(1).lower())
        if mon:
            day = int(m3.group(2))
            # World Cup year default if not provided:
            return f"2026-{mon:02d}-{day:02d}"
    return None

def extract_name(text: str):
    t = (text or "").strip()
    # If it looks like a phone/date/time/party, skip
    if is_phone(t):
        return None
    if extract_date(t) or extract_time(t) or extract_party_size(t) is not None:
        return None
    # single letter name should be accepted (user typed "J")
    if 1 <= len(t) <= 40:
        return t
    return None

def update_draft_from_text(draft: dict, text: str):
    # fill only missing
    if not draft.get("date"):
        d = extract_date(text)
        if d:
            draft["date"] = d

    if not draft.get("time"):
        tm = extract_time(text)
        if tm:
            draft["time"] = tm

    if not draft.get("party_size"):
        ps = extract_party_size(text)
        if ps is not None:
            draft["party_size"] = ps

    if not draft.get("phone"):
        ph = extract_phone(text)
        if ph:
            draft["phone"] = ph

    if not draft.get("name"):
        nm = extract_name(text)
        if nm:
            draft["name"] = nm

def next_question_for_missing(missing: list, lang: str):
    # Keep it simple, friendly, and in chosen language
    if lang == "es":
        prompts = {
            "date": "¿Para qué fecha te gustaría la reserva? (Ej: 2026-06-23)",
            "time": "¿A qué hora? (Ej: 7pm)",
            "party_size": "¿Para cuántas personas?",
            "name": "¿A nombre de quién?",
            "phone": "¿Cuál es tu número de teléfono (10 dígitos)?",
        }
    elif lang == "pt":
        prompts = {
            "date": "Para qual data você quer a reserva? (Ex: 2026-06-23)",
            "time": "Qual horário? (Ex: 7pm)",
            "party_size": "Para quantas pessoas?",
            "name": "Em nome de quem?",
            "phone": "Qual é o seu telefone (10 dígitos)?",
        }
    elif lang == "fr":
        prompts = {
            "date": "Pour quelle date souhaitez-vous la réservation ? (Ex : 2026-06-23)",
            "time": "À quelle heure ? (Ex : 19h ou 7pm)",
            "party_size": "Pour combien de personnes ?",
            "name": "Au nom de qui ?",
            "phone": "Quel est votre numéro de téléphone (10 chiffres) ?",
        }
    else:
        prompts = {
            "date": "What date would you like? (e.g., 2026-06-23)",
            "time": "What time? (e.g., 7pm)",
            "party_size": "Party size?",
            "name": "Name for the reservation?",
            "phone": "Phone number (10 digits)?",
        }

    return prompts.get(missing[0], "What details can you share?")

def reset_draft(session: dict):
    session["draft"] = {k: None for k in REQUIRED_FIELDS}

# -----------------------------
# Routes
# -----------------------------
@app.route("/")
def home():
    resp = make_response(send_from_directory(".", "index.html"))
    # ensure session cookie exists
    sid = request.cookies.get("sid")
    if not sid:
        resp.set_cookie("sid", str(uuid.uuid4()), httponly=True, samesite="Lax")
    return resp

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

@app.route("/schedule")
def schedule():
    return jsonify({
        "host_city": "Dallas / Arlington",
        "venue": "Dallas Stadium (Arlington)",
        "matches": DALLAS_MATCHES,
        "note": "Knockout matchups may remain TBD until bracket is finalized."
    })

@app.route("/reservation/status")
def reservation_status():
    _, session = get_session()
    return jsonify({
        "draft": session["draft"],
        "missing": missing_fields(session["draft"]),
        "lang": session["lang"],
        "summary": draft_summary(session["draft"])
    })

@app.route("/reservation/reset", methods=["POST"])
def reservation_reset():
    _, session = get_session()
    reset_draft(session)
    return jsonify({"ok": True})

@app.route("/reserve", methods=["POST"])
def reserve():
    """
    Direct reservation submit from the form (bypasses the model).
    """
    _, session = get_session()
    data = request.get_json(force=True) or {}

    try:
        lead = ReservationLead(
            name=str(data.get("name") or "").strip(),
            phone=str(data.get("phone") or "").strip(),
            date=str(data.get("date") or "").strip(),
            time=str(data.get("time") or "").strip(),
            party_size=int(data.get("party_size")),
        )
        append_lead_to_sheet(lead.name, lead.phone, lead.date, lead.time, lead.party_size)
        reset_draft(session)

        return jsonify({
            "ok": True,
            "message": "✅ Reservation saved!",
            "lead": lead.model_dump(),
        })
    except Exception as e:
        return jsonify({"ok": False, "error": repr(e)}), 400

@app.route("/test-sheet")
def test_sheet():
    try:
        append_lead_to_sheet("TEST_NAME", "2145551212", "2026-06-23", "7pm", 4)
        return jsonify({"ok": True, "sheet": SHEET_NAME, "message": "✅ Test row appended."})
    except Exception as e:
        return jsonify({"ok": False, "sheet": SHEET_NAME, "error": repr(e)}), 500


@app.route("/chat", methods=["POST"])
def chat():
    sid, session = get_session()
    data = request.get_json(force=True) or {}

    user_message = (data.get("message") or "").strip()
    history = data.get("history") or []
    preferred_lang = normalize_lang(data.get("lang") or "")

    if preferred_lang:
        session["lang"] = preferred_lang

    lang = session["lang"]

    if not user_message:
        return jsonify({"reply": "Please type a message."})

    # Commands for recall/reset
    lowered = user_message.lower().strip()
    if lowered in ["recall reservation", "recall reservation so far", "reservation status", "what do you have so far", "recall"]:
        return jsonify({"reply": draft_summary(session["draft"])})

    if lowered in ["reset reservation", "start over", "clear reservation", "reset"]:
        reset_draft(session)
        if lang == "es":
            return jsonify({"reply": "✅ Listo. Empecemos de nuevo. ¿Para qué fecha te gustaría la reserva?"})
        if lang == "pt":
            return jsonify({"reply": "✅ Pronto. Vamos recomeçar. Para qual data você quer a reserva?"})
        if lang == "fr":
            return jsonify({"reply": "✅ D’accord. Recommençons. Pour quelle date souhaitez-vous la réservation ?"})
        return jsonify({"reply": "✅ Done. Let’s start over. What date would you like?"})

    # If message looks like reservation intent, use state machine first
    intent_reservation = bool(re.search(r"\breservation\b|\bbook\b|\btable\b|\breserve\b", lowered))

    # Always try to update draft from any message
    update_draft_from_text(session["draft"], user_message)
    miss = missing_fields(session["draft"])

    # If we're mid-reservation or user said reservation, drive the state machine
    if intent_reservation or any(session["draft"].get(k) for k in REQUIRED_FIELDS):
        if miss:
            return jsonify({"reply": next_question_for_missing(miss, lang)})

        # All fields collected -> save
        try:
            lead = ReservationLead(**session["draft"])
            append_lead_to_sheet(lead.name, lead.phone, lead.date, lead.time, lead.party_size)
            reset_draft(session)

            if lang == "es":
                return jsonify({"reply": f"✅ ¡Reserva guardada!\nNombre: {lead.name}\nTel: {lead.phone}\nFecha: {lead.date}\nHora: {lead.time}\nPersonas: {lead.party_size}"})
            if lang == "pt":
                return jsonify({"reply": f"✅ Reserva salva!\nNome: {lead.name}\nTel: {lead.phone}\nData: {lead.date}\nHora: {lead.time}\nPessoas: {lead.party_size}"})
            if lang == "fr":
                return jsonify({"reply": f"✅ Réservation enregistrée !\nNom : {lead.name}\nTél : {lead.phone}\nDate : {lead.date}\nHeure : {lead.time}\nPersonnes : {lead.party_size}"})
            return jsonify({"reply": f"✅ Reservation saved!\nName: {lead.name}\nPhone: {lead.phone}\nDate: {lead.date}\nTime: {lead.time}\nParty size: {lead.party_size}"})
        except Exception as e:
            return jsonify({"reply": f"⚠️ Could not save reservation: {repr(e)}"}), 500

    # Otherwise, use the model for general Q&A (hours, address, specials)
    system_msg = f"""
You are a World Cup 2026 Concierge for a Dallas-area business.

Use this business profile as the source of truth:
{business_profile}

{LANG_PROMPTS.get(lang, LANG_PROMPTS["en"])}

Be friendly, fast, and concise.
If something is unknown, say you're not sure and offer a callback to the business phone.
Never mention being an AI unless the user asks directly.
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

    reply = (resp.output_text or "").strip()
    return jsonify({"reply": reply})


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5050))
    app.run(host="0.0.0.0", port=port, debug=False)