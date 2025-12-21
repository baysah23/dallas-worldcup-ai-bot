from dotenv import load_dotenv
load_dotenv()

import os
import json
import re
import time
from datetime import datetime, date
from typing import Dict, Any, List, Optional

from flask import Flask, request, jsonify, send_from_directory
from openai import OpenAI

import gspread
from google.oauth2.service_account import Credentials

# =============================
# App setup
# =============================
app = Flask(__name__)
client = OpenAI()

# Cache-busting (helps Render show latest index.html)
app.config["SEND_FILE_MAX_AGE_DEFAULT"] = 0


@app.after_request
def add_no_cache_headers(response):
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response


# =============================
# Config (edit these safely)
# =============================
BUSINESS_PROFILE_PATH = "business_profile.txt"

# Business rules (basic + safe defaults)
BUSINESS_RULES = {
    "max_party_size": 12,
    # 24h clock ranges local time
    "hours": {
        "mon": {"open": "11:00", "close": "23:00"},
        "tue": {"open": "11:00", "close": "23:00"},
        "wed": {"open": "11:00", "close": "23:00"},
        "thu": {"open": "11:00", "close": "24:00"},
        "fri": {"open": "11:00", "close": "01:00"},
        "sat": {"open": "10:00", "close": "01:00"},
        "sun": {"open": "10:00", "close": "23:00"},
    },
    # YYYY-MM-DD dates closed
    "closed_dates": [
        # Example: "2026-07-04"
    ],
    # Match-day banner message
    "match_day_banner": "Opening at 11am on match days",
}

# World Cup countdown target (tournament starts June 11, 2026)
COUNTDOWN_TARGET_ISO = "2026-06-11T00:00:00"

# Dallas/Arlington World Cup match schedule (Dallas Stadium / Arlington)
# Note: some KO match times may be TBD; the UI will still work.
DALLAS_MATCHES = [
    {"id": "M11", "date": "2026-06-14", "time": "15:00", "stage": "Group", "home": "Netherlands", "away": "Japan"},
    {"id": "M22", "date": "2026-06-17", "time": "15:00", "stage": "Group", "home": "England", "away": "Croatia"},
    {"id": "M43", "date": "2026-06-22", "time": "12:00", "stage": "Group", "home": "Argentina", "away": "Austria"},
    {"id": "M57", "date": "2026-06-25", "time": "18:00", "stage": "Group", "home": "Japan", "away": "TBD"},
    {"id": "M70", "date": "2026-06-27", "time": "21:00", "stage": "Group", "home": "Jordan", "away": "Argentina"},
    {"id": "R32-1", "date": "2026-06-30", "time": "TBD", "stage": "Round of 32", "home": "TBD", "away": "TBD"},
    {"id": "R32-2", "date": "2026-07-03", "time": "TBD", "stage": "Round of 32", "home": "TBD", "away": "TBD"},
    {"id": "R16", "date": "2026-07-06", "time": "TBD", "stage": "Round of 16", "home": "TBD", "away": "TBD"},
    {"id": "SF", "date": "2026-07-14", "time": "TBD", "stage": "Semi-final", "home": "TBD", "away": "TBD"},
]


# =============================
# Internationalized UI strings
# =============================
UI_STRINGS = {
    "en": {
        "welcome": "Hi! Want to book a table for a match day? Ask me anything.",
        "helper": "What date would you like? (Example: June 23, 2026)\n\n(You can also type: “Recall reservation so far”)",
        "lang_set": "Language set to English.",
        "recall_title": "Reservation so far",
        "need_fields": "I still need:",
        "ask_date": "What date would you like? (Example: June 23, 2026)",
        "ask_time": "What time? (Example: 7pm)",
        "ask_party": "How many people? (Example: 4)",
        "ask_name": "What name should I put the reservation under?",
        "ask_phone": "What’s the best phone number for confirmation?",
        "saved": "✅ Reservation saved!",
        "invalid_date": "That date looks invalid. Please provide a valid date (Example: June 23, 2026).",
        "too_large_party": "That party size is above our maximum of {max_size}. Please choose a smaller party size.",
        "closed": "We’re closed on that date. Please pick another date.",
        "unknown": "I’m not sure about that. Want me to have the business call you back?",
    },
    "es": {
        "welcome": "¡Hola! ¿Quieres reservar una mesa para un día de partido? Pregúntame lo que sea.",
        "helper": "¿Qué fecha te gustaría? (Ejemplo: 23 de junio de 2026)\n\n(También puedes escribir: “Recordar mi reserva hasta ahora”)",
        "lang_set": "Idioma configurado en Español.",
        "recall_title": "Tu reserva hasta ahora",
        "need_fields": "Aún necesito:",
        "ask_date": "¿Qué fecha te gustaría? (Ejemplo: 23 de junio de 2026)",
        "ask_time": "¿A qué hora? (Ejemplo: 7pm)",
        "ask_party": "¿Para cuántas personas? (Ejemplo: 4)",
        "ask_name": "¿A nombre de quién va la reserva?",
        "ask_phone": "¿Cuál es tu número de teléfono para confirmar?",
        "saved": "✅ ¡Reserva guardada!",
        "invalid_date": "Esa fecha parece inválida. Por favor da una fecha válida (Ejemplo: 23 de junio de 2026).",
        "too_large_party": "Ese tamaño de grupo supera nuestro máximo de {max_size}. Por favor elige un número menor.",
        "closed": "Estamos cerrados esa fecha. Por favor elige otra fecha.",
        "unknown": "No estoy seguro. ¿Quieres que el negocio te llame?",
    },
    "pt": {
        "welcome": "Olá! Quer reservar uma mesa para dia de jogo? Pergunte o que quiser.",
        "helper": "Qual data você gostaria? (Exemplo: 23 de junho de 2026)\n\n(Você também pode digitar: “Relembrar minha reserva até agora”)",
        "lang_set": "Idioma definido para Português.",
        "recall_title": "Sua reserva até agora",
        "need_fields": "Ainda preciso de:",
        "ask_date": "Qual data você gostaria? (Exemplo: 23 de junho de 2026)",
        "ask_time": "Que horas? (Exemplo: 19:00 / 7pm)",
        "ask_party": "Para quantas pessoas? (Exemplo: 4)",
        "ask_name": "Em nome de quem devo colocar a reserva?",
        "ask_phone": "Qual telefone para confirmação?",
        "saved": "✅ Reserva salva!",
        "invalid_date": "Essa data parece inválida. Envie uma data válida (Exemplo: 23 de junho de 2026).",
        "too_large_party": "Esse grupo passa do máximo de {max_size}. Por favor escolha um número menor.",
        "closed": "Estamos fechados nessa data. Por favor escolha outra data.",
        "unknown": "Não tenho certeza. Quer que o estabelecimento te ligue?",
    },
    "fr": {
        "welcome": "Bonjour ! Vous voulez réserver une table pour un jour de match ? Demandez-moi n’importe quoi.",
        "helper": "Quelle date souhaitez-vous ? (Exemple : 23 juin 2026)\n\n(Vous pouvez aussi taper : « Rappeler ma réservation jusqu’ici »)",
        "lang_set": "Langue réglée sur le Français.",
        "recall_title": "Votre réservation jusqu’ici",
        "need_fields": "Il me manque :",
        "ask_date": "Quelle date souhaitez-vous ? (Exemple : 23 juin 2026)",
        "ask_time": "Quelle heure ? (Exemple : 19h / 7pm)",
        "ask_party": "Combien de personnes ? (Exemple : 4)",
        "ask_name": "À quel nom dois-je mettre la réservation ?",
        "ask_phone": "Quel numéro de téléphone pour la confirmation ?",
        "saved": "✅ Réservation enregistrée !",
        "invalid_date": "Cette date semble invalide. Merci de donner une date valide (Exemple : 23 juin 2026).",
        "too_large_party": "Ce groupe dépasse notre maximum de {max_size}. Merci de choisir un nombre plus petit.",
        "closed": "Nous sommes fermés à cette date. Merci de choisir une autre date.",
        "unknown": "Je ne suis pas sûr. Voulez-vous que l’établissement vous rappelle ?",
    },
}

SUPPORTED_LANGS = ["en", "es", "pt", "fr"]


# =============================
# Menu in 4 languages (example)
# Replace with your real menu as you like.
# =============================
MENU = {
    "en": [
        {"name": "Spicy Chicken Sandwich", "desc": "Crispy chicken, slaw, house sauce", "price": "$14"},
        {"name": "Loaded Nachos", "desc": "Cheese, pico, jalapeños, crema", "price": "$12"},
        {"name": "Wings (10pc)", "desc": "Buffalo / BBQ / Lemon Pepper", "price": "$15"},
        {"name": "Burger + Fries", "desc": "Angus beef, cheddar, pickles", "price": "$16"},
    ],
    "es": [
        {"name": "Sándwich de Pollo Picante", "desc": "Pollo crujiente, ensalada, salsa de la casa", "price": "$14"},
        {"name": "Nachos Cargados", "desc": "Queso, pico, jalapeños, crema", "price": "$12"},
        {"name": "Alitas (10)", "desc": "Buffalo / BBQ / Limón Pimienta", "price": "$15"},
        {"name": "Hamburguesa + Papas", "desc": "Carne Angus, cheddar, pepinillos", "price": "$16"},
    ],
    "pt": [
        {"name": "Sanduíche de Frango Apimentado", "desc": "Frango crocante, salada, molho da casa", "price": "$14"},
        {"name": "Nachos", "desc": "Queijo, pico, jalapeños, creme", "price": "$12"},
        {"name": "Asas (10)", "desc": "Buffalo / BBQ / Limão e pimenta", "price": "$15"},
        {"name": "Hambúrguer + Batatas", "desc": "Carne Angus, cheddar, picles", "price": "$16"},
    ],
    "fr": [
        {"name": "Sandwich Poulet Épicé", "desc": "Poulet croustillant, salade, sauce maison", "price": "$14"},
        {"name": "Nachos Garnis", "desc": "Fromage, pico, jalapeños, crème", "price": "$12"},
        {"name": "Ailes (10)", "desc": "Buffalo / BBQ / Citron Poivre", "price": "$15"},
        {"name": "Burger + Frites", "desc": "Bœuf Angus, cheddar, cornichons", "price": "$16"},
    ],
}


# =============================
# Load business profile
# =============================
if not os.path.exists(BUSINESS_PROFILE_PATH):
    raise FileNotFoundError("business_profile.txt not found in the same folder as app.py.")

with open(BUSINESS_PROFILE_PATH, "r", encoding="utf-8") as f:
    business_profile = f.read()


# =============================
# Google Sheets setup
# =============================
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


def append_lead_to_sheet(lead: Dict[str, Any]):
    gc = get_gspread_client()
    ws = gc.open(SHEET_NAME).sheet1
    ws.append_row([
        datetime.now().isoformat(timespec="seconds"),
        lead.get("name", ""),
        lead.get("phone", ""),
        lead.get("date", ""),
        lead.get("time", ""),
        int(lead.get("party_size", 0) or 0),
        lead.get("notes", ""),
    ])


def fetch_leads(limit: int = 200) -> List[Dict[str, Any]]:
    gc = get_gspread_client()
    ws = gc.open(SHEET_NAME).sheet1
    records = ws.get_all_records()
    return records[-limit:]


# =============================
# Simple in-memory sessions + rate limiting
# =============================
SESSIONS: Dict[str, Dict[str, Any]] = {}  # session_id -> {lang, reservation, last_seen}
RATE: Dict[str, List[float]] = {}  # ip -> timestamps

RATE_LIMIT_PER_MIN = 30  # adjust as needed


def rate_limited(ip: str) -> bool:
    now = time.time()
    window_start = now - 60
    RATE.setdefault(ip, [])
    RATE[ip] = [t for t in RATE[ip] if t >= window_start]
    if len(RATE[ip]) >= RATE_LIMIT_PER_MIN:
        return True
    RATE[ip].append(now)
    return False


def get_session(session_id: str) -> Dict[str, Any]:
    if session_id not in SESSIONS:
        SESSIONS[session_id] = {
            "lang": "en",
            "reservation": {"active": False, "fields": {}},
            "last_seen": time.time(),
        }
    SESSIONS[session_id]["last_seen"] = time.time()
    return SESSIONS[session_id]


def normalize_lang(lang: str) -> str:
    lang = (lang or "").lower().strip()
    return lang if lang in SUPPORTED_LANGS else "en"


def get_strings(lang: str) -> Dict[str, str]:
    lang = normalize_lang(lang)
    return UI_STRINGS[lang]


# =============================
# Reservation parsing + validation
# =============================
MONTHS = {
    "january": 1, "jan": 1,
    "february": 2, "feb": 2,
    "march": 3, "mar": 3,
    "april": 4, "apr": 4,
    "may": 5,
    "june": 6, "jun": 6,
    "july": 7, "jul": 7,
    "august": 8, "aug": 8,
    "september": 9, "sep": 9, "sept": 9,
    "october": 10, "oct": 10,
    "november": 11, "nov": 11,
    "december": 12, "dec": 12,
}


def parse_party_size(text: str) -> Optional[int]:
    m = re.search(r"\bparty\s*of\s*(\d{1,2})\b", text, re.I)
    if m:
        return int(m.group(1))
    m = re.search(r"\btable\s*of\s*(\d{1,2})\b", text, re.I)
    if m:
        return int(m.group(1))
    m = re.search(r"\b(\d{1,2})\s*(people|guests|persons|ppl)\b", text, re.I)
    if m:
        return int(m.group(1))
    # single number, if user says "5" only
    if text.strip().isdigit() and 1 <= int(text.strip()) <= 50:
        return int(text.strip())
    return None


def parse_phone(text: str) -> Optional[str]:
    digits = re.sub(r"\D", "", text)
    if len(digits) == 10:
        return digits
    if len(digits) == 11 and digits.startswith("1"):
        return digits[1:]
    return None


def parse_time(text: str) -> Optional[str]:
    # Accept "7pm", "7:30pm", "19:00"
    t = text.strip().lower()
    m = re.search(r"\b(\d{1,2})(?::(\d{2}))?\s*(am|pm)\b", t)
    if m:
        h = int(m.group(1))
        mm = int(m.group(2) or 0)
        ap = m.group(3)
        if h == 12:
            h = 0
        if ap == "pm":
            h += 12
        return f"{h:02d}:{mm:02d}"
    m = re.search(r"\b([01]?\d|2[0-3]):([0-5]\d)\b", t)
    if m:
        return f"{int(m.group(1)):02d}:{int(m.group(2)):02d}"
    return None


def parse_date_basic(text: str) -> Optional[str]:
    """
    Returns YYYY-MM-DD or None.
    Supports:
      - 6/23/2026 or 6/23/26
      - June 23 2026 / Jun 23
    """
    s = text.strip().lower()

    # mm/dd/yyyy
    m = re.search(r"\b(\d{1,2})/(\d{1,2})/(\d{2,4})\b", s)
    if m:
        mm = int(m.group(1))
        dd = int(m.group(2))
        yy = int(m.group(3))
        if yy < 100:
            yy += 2000
        try:
            dt = date(yy, mm, dd)
            return dt.isoformat()
        except ValueError:
            return None

    # "june 23" or "jun 23, 2026"
    m = re.search(r"\b([a-z]{3,9})\s+(\d{1,2})(?:st|nd|rd|th)?(?:,\s*(\d{4}))?\b", s)
    if m:
        mon = m.group(1)
        dd = int(m.group(2))
        yy = int(m.group(3) or 2026)  # assume 2026 for World Cup context
        if mon in MONTHS:
            mm = MONTHS[mon]
            try:
                dt = date(yy, mm, dd)
                return dt.isoformat()
            except ValueError:
                return None
    return None


def is_match_day(iso_date: str) -> bool:
    for m in DALLAS_MATCHES:
        if m["date"] == iso_date:
            return True
    return False


def validate_business_rules(fields: Dict[str, Any]) -> Optional[str]:
    # party size
    ps = fields.get("party_size")
    if ps is not None and int(ps) > int(BUSINESS_RULES["max_party_size"]):
        return "too_large_party"

    # closed date
    d = fields.get("date")
    if d and d in BUSINESS_RULES["closed_dates"]:
        return "closed"

    return None


def reservation_summary(fields: Dict[str, Any]) -> str:
    parts = []
    if fields.get("date"):
        parts.append(f"Date: {fields['date']}")
    if fields.get("time"):
        parts.append(f"Time: {fields['time']}")
    if fields.get("party_size"):
        parts.append(f"Party size: {fields['party_size']}")
    if fields.get("name"):
        parts.append(f"Name: {fields['name']}")
    if fields.get("phone"):
        parts.append(f"Phone: {fields['phone']}")
    return "\n".join(parts) if parts else "(none yet)"


def missing_fields(fields: Dict[str, Any]) -> List[str]:
    required = ["date", "time", "party_size", "name", "phone"]
    return [k for k in required if not fields.get(k)]


def next_question(lang: str, fields: Dict[str, Any]) -> str:
    s = get_strings(lang)
    miss = missing_fields(fields)
    if not miss:
        return ""
    # Ask only the next missing field
    nxt = miss[0]
    if nxt == "date":
        return s["ask_date"]
    if nxt == "time":
        return s["ask_time"]
    if nxt == "party_size":
        return s["ask_party"]
    if nxt == "name":
        return s["ask_name"]
    if nxt == "phone":
        return s["ask_phone"]
    return s["ask_date"]


def detect_reservation_intent(text: str) -> bool:
    t = text.lower()
    return any(w in t for w in ["reservation", "reserve", "book", "table", "booking"])


def detect_recall(text: str) -> bool:
    t = text.lower().strip()
    return "recall reservation" in t or "recall" == t or "reservation so far" in t or "recordar" in t or "rappeler" in t or "relembrar" in t


# =============================
# API routes
# =============================
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
        "matches": DALLAS_MATCHES,
    })


@app.route("/api/config")
def api_config():
    return jsonify({
        "countdown_target": COUNTDOWN_TARGET_ISO,
        "business_rules": BUSINESS_RULES,
        "supported_languages": SUPPORTED_LANGS,
    })


@app.route("/api/menu")
def api_menu():
    lang = normalize_lang(request.args.get("lang") or "en")
    return jsonify({"lang": lang, "items": MENU.get(lang, MENU["en"])})


@app.route("/api/ui")
def api_ui():
    lang = normalize_lang(request.args.get("lang") or "en")
    return jsonify({"lang": lang, "strings": UI_STRINGS[lang]})


# =============================
# Admin dashboard (live leads)
# =============================
@app.route("/admin")
def admin():
    # Minimal protection: set ADMIN_KEY env var and append ?key=...
    admin_key = os.environ.get("ADMIN_KEY")
    if admin_key:
        if request.args.get("key") != admin_key:
            return "Unauthorized", 401

    try:
        leads = fetch_leads(limit=300)
    except Exception as e:
        return f"Error loading leads: {repr(e)}", 500

    rows = ""
    for r in reversed(leads):
        rows += "<tr>" + "".join([
            f"<td>{str(r.get('timestamp',''))}</td>",
            f"<td>{str(r.get('name',''))}</td>",
            f"<td>{str(r.get('phone',''))}</td>",
            f"<td>{str(r.get('date',''))}</td>",
            f"<td>{str(r.get('time',''))}</td>",
            f"<td>{str(r.get('party_size',''))}</td>",
            f"<td>{str(r.get('notes',''))}</td>",
        ]) + "</tr>"

    return f"""
<!doctype html>
<html>
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width,initial-scale=1"/>
  <title>Admin - Leads</title>
  <style>
    body{{font-family:Arial, sans-serif; background:#0b0f19; color:#e8e8e8; padding:16px;}}
    table{{width:100%; border-collapse:collapse;}}
    th,td{{border:1px solid rgba(255,255,255,0.15); padding:8px; font-size:13px;}}
    th{{background:#10162a; text-align:left;}}
    .top{{display:flex;justify-content:space-between;align-items:center;margin-bottom:12px;}}
    a{{color:#8ab4ff;}}
  </style>
</head>
<body>
  <div class="top">
    <h2>Reservation Leads (Google Sheet: {SHEET_NAME})</h2>
    <a href="/admin?key={request.args.get('key','')}">Refresh</a>
  </div>
  <table>
    <thead>
      <tr>
        <th>timestamp</th><th>name</th><th>phone</th><th>date</th><th>time</th><th>party_size</th><th>notes</th>
      </tr>
    </thead>
    <tbody>
      {rows}
    </tbody>
  </table>
</body>
</html>
"""


# =============================
# Chat endpoint (reservation state machine + LLM fallback)
# =============================
@app.route("/chat", methods=["POST"])
def chat():
    ip = request.headers.get("x-forwarded-for", request.remote_addr) or "unknown"
    if rate_limited(ip):
        return jsonify({"reply": "⚠️ Too many requests. Please wait a minute and try again."}), 429

    data = request.get_json(force=True)
    user_message = (data.get("message") or "").strip()
    session_id = (data.get("session_id") or "").strip() or "anon"
    lang = normalize_lang(data.get("lang") or "en")

    if not user_message:
        return jsonify({"reply": "Please type a message."})

    sess = get_session(session_id)
    sess["lang"] = lang  # keep latest
    s = get_strings(lang)

    # Start / keep reservation state machine
    resv = sess["reservation"]
    fields = resv["fields"]

    # Handle recall request deterministically
    if detect_recall(user_message):
        summary = reservation_summary(fields)
        return jsonify({"reply": f"📌 {s['recall_title']}:\n{summary}\n\n{s['helper']}"})

    # If user indicates reservation OR reservation already active, enter reservation mode
    if detect_reservation_intent(user_message) or resv.get("active"):
        resv["active"] = True

        # Extract fields from message
        d = parse_date_basic(user_message)
        if d:
            fields["date"] = d

        t = parse_time(user_message)
        if t:
            fields["time"] = t

        ps = parse_party_size(user_message)
        if ps:
            fields["party_size"] = ps

        ph = parse_phone(user_message)
        if ph:
            fields["phone"] = ph

        # Name: simplistic — if user sends a short alphabetic token and name missing
        if not fields.get("name"):
            name_candidate = user_message.strip()
            if 1 <= len(name_candidate) <= 30 and re.match(r"^[A-Za-z][A-Za-z\s\.\-']+$", name_candidate):
                fields["name"] = name_candidate.strip()

        # Validate date (e.g., June 31 -> parse_date_basic returns None)
        # If user typed a month word + day but parse failed, give invalid date message
        if re.search(r"\b(jan|feb|mar|apr|may|jun|june|jul|aug|sep|sept|oct|nov|dec)\b", user_message.lower()) and "date" not in fields:
            return jsonify({"reply": s["invalid_date"]})

        # Business rules validation
        problem = validate_business_rules(fields)
        if problem == "too_large_party":
            return jsonify({"reply": s["too_large_party"].format(max_size=BUSINESS_RULES["max_party_size"])})
        if problem == "closed":
            return jsonify({"reply": s["closed"]})

        # If still missing fields, ask next question
        miss = missing_fields(fields)
        if miss:
            q = next_question(lang, fields)
            return jsonify({"reply": q + "\n\n(" + s["helper"] + ")"})

        # All fields present -> save lead
        lead = {
            "name": fields["name"],
            "phone": fields["phone"],
            "date": fields["date"],
            "time": fields["time"],
            "party_size": int(fields["party_size"]),
            "notes": "match_day" if is_match_day(fields["date"]) else "",
        }

        try:
            append_lead_to_sheet(lead)
        except Exception as e:
            return jsonify({"reply": f"⚠️ Could not save reservation. {repr(e)}"})

        # Clear reservation session
        sess["reservation"] = {"active": False, "fields": {}}

        return jsonify({
            "reply": (
                f"{s['saved']}\n"
                f"Name: {lead['name']}\n"
                f"Phone: {lead['phone']}\n"
                f"Date: {lead['date']}\n"
                f"Time: {lead['time']}\n"
                f"Party size: {lead['party_size']}\n"
                f"{BUSINESS_RULES['match_day_banner'] if is_match_day(lead['date']) else ''}"
            ).strip()
        })

    # Otherwise: general Q&A -> use OpenAI
    # Keep it concise and multilingual by forcing response language.
    lang_instruction = {
        "en": "Respond in English.",
        "es": "Responde en Español.",
        "pt": "Responda em Português.",
        "fr": "Répondez en Français.",
    }[lang]

    system_msg = f"""
You are a World Cup 2026 concierge for a Dallas-area business.

Use this business profile as the single source of truth:
{business_profile}

Goals:
- Answer customer questions about the business (hours, address, specials, menu).
- Be friendly, fast, and concise.

Rules:
- If asked to make a reservation, ask for ONLY: date, time, party size, name, phone number.
- If you don't know something, say you're not sure and offer a callback to the business phone.
- Never mention being an AI unless asked directly.
{lang_instruction}
"""

    resp = client.responses.create(
        model="gpt-4o-mini",
        input=[
            {"role": "system", "content": system_msg},
            {"role": "user", "content": user_message},
        ],
    )

    reply = (resp.output_text or "").strip()
    return jsonify({"reply": reply})


# =============================
# Run
# =============================
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5050))
    app.run(host="0.0.0.0", port=port, debug=False)