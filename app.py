from dotenv import load_dotenv
load_dotenv()

import os
import json
import re
import time
from datetime import datetime, date
from typing import Dict, Any, Optional, List, Tuple

from flask import Flask, request, jsonify, send_from_directory
from openai import OpenAI

import gspread
from google.oauth2.service_account import Credentials


BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# ============================================================
# App + cache-busting (helps Render show latest index.html)
# ============================================================
app = Flask(__name__)
client = OpenAI()

app.config["SEND_FILE_MAX_AGE_DEFAULT"] = 0


@app.after_request
def add_no_cache_headers(response):
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response


# ============================================================
# ENV + Config
# ============================================================
ADMIN_KEY = os.environ.get("ADMIN_KEY", "")  # required to view /admin
RATE_LIMIT_PER_MIN = int(os.environ.get("RATE_LIMIT_PER_MIN", "30"))

SHEET_NAME = os.environ.get("GOOGLE_SHEET_NAME", "World Cup AI Reservations")

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


# ============================================================
# Business Profile
# ============================================================
BUSINESS_PROFILE_PATH = os.path.join(BASE_DIR, "business_profile.txt")
if not os.path.exists(BUSINESS_PROFILE_PATH):
    raise FileNotFoundError("business_profile.txt not found in the same folder as app.py.")

with open(BUSINESS_PROFILE_PATH, "r", encoding="utf-8") as f:
    BUSINESS_PROFILE = f.read().strip()


# ============================================================
# Business Rules (edit here)
# ============================================================
BUSINESS_RULES = {
    # hours in 24h local time; for simplicity we only enforce "open/closed" by day
    "hours": {
        "mon": "11:00-22:00",
        "tue": "11:00-22:00",
        "wed": "11:00-22:00",
        "thu": "11:00-23:00",
        "fri": "11:00-01:00",
        "sat": "10:00-01:00",
        "sun": "10:00-22:00",
    },
    # ISO dates (YYYY-MM-DD)
    "closed_dates": [
        # "2026-12-25",
    ],
    "max_party_size": 12,
    "match_day_banner": "🏟️ Match-day mode: Opening at 11am on match days!",
}


# ============================================================
# Dallas Match Schedule (dynamic from server)
# - You can replace this list anytime.
# - Format: ISO date/time in local venue time (for display)
# ============================================================
DALLAS_MATCHES = [
    {'id': 'dal-011', 'date': '2026-06-14', 'time': '15:00', 'venue': 'Dallas Stadium (Arlington)', 'teams': 'Netherlands vs Japan (Group F)'},
    {'id': 'dal-022', 'date': '2026-06-17', 'time': '15:00', 'venue': 'Dallas Stadium (Arlington)', 'teams': 'England vs Croatia (Group L)'},
    {'id': 'dal-043', 'date': '2026-06-22', 'time': '12:00', 'venue': 'Dallas Stadium (Arlington)', 'teams': 'Argentina vs Austria (Group J)'},
    {'id': 'dal-057', 'date': '2026-06-25', 'time': '18:00', 'venue': 'Dallas Stadium (Arlington)', 'teams': 'Japan vs UEFA Play-off Final B winner (Group F, TBD)'},
    {'id': 'dal-070', 'date': '2026-06-27', 'time': '21:00', 'venue': 'Dallas Stadium (Arlington)', 'teams': 'Jordan vs Argentina (Group J)'},
    {'id': 'dal-r32-1', 'date': '2026-06-30', 'time': '12:00', 'venue': 'Dallas Stadium (Arlington)', 'teams': 'Round of 32: Group E runner-up vs Group I runner-up'},
    {'id': 'dal-r32-2', 'date': '2026-07-03', 'time': '13:00', 'venue': 'Dallas Stadium (Arlington)', 'teams': 'Round of 32: Group D runner-up vs Group G runner-up'},
    {'id': 'dal-r16', 'date': '2026-07-06', 'time': '14:00', 'venue': 'Dallas Stadium (Arlington)', 'teams': 'Round of 16 (TBD vs TBD)'},
    {'id': 'dal-sf', 'date': '2026-07-14', 'time': '14:00', 'venue': 'Dallas Stadium (Arlington)', 'teams': 'Semifinal (TBD vs TBD)'},
]



# ============================================================
# Menu (4 languages) — edit/add items here
# ============================================================
MENU = {
    "en": {
        "title": "Menu",
        "items": [
            {"name": "Spicy Chicken Sandwich", "price": "$14", "desc": "Crispy chicken, spicy sauce, pickles, fries optional."},
            {"name": "Wings (6/12)", "price": "$10/$18", "desc": "Buffalo, lemon pepper, BBQ."},
            {"name": "Nachos", "price": "$12", "desc": "Cheese, jalapeño, pico, crema."},
            {"name": "Burger & Fries", "price": "$15", "desc": "Angus beef, cheddar, lettuce, tomato."},
        ],
    },
    "es": {
        "title": "Menú",
        "items": [
            {"name": "Sándwich de Pollo Picante", "price": "$14", "desc": "Pollo crujiente, salsa picante, pepinillos; papas opcionales."},
            {"name": "Alitas (6/12)", "price": "$10/$18", "desc": "Buffalo, limón pimienta, BBQ."},
            {"name": "Nachos", "price": "$12", "desc": "Queso, jalapeño, pico, crema."},
            {"name": "Hamburguesa y Papas", "price": "$15", "desc": "Carne Angus, cheddar, lechuga, tomate."},
        ],
    },
    "pt": {
        "title": "Cardápio",
        "items": [
            {"name": "Sanduíche de Frango Apimentado", "price": "$14", "desc": "Frango crocante, molho picante, picles; batatas opcionais."},
            {"name": "Asinhas (6/12)", "price": "$10/$18", "desc": "Buffalo, limão com pimenta, BBQ."},
            {"name": "Nachos", "price": "$12", "desc": "Queijo, jalapeño, pico, creme."},
            {"name": "Hambúrguer e Batatas", "price": "$15", "desc": "Carne Angus, cheddar, alface, tomate."},
        ],
    },
    "fr": {
        "title": "Menu",
        "items": [
            {"name": "Sandwich Poulet Épicé", "price": "$14", "desc": "Poulet croustillant, sauce épicée, cornichons; frites en option."},
            {"name": "Ailes (6/12)", "price": "$10/$18", "desc": "Buffalo, citron-poivre, BBQ."},
            {"name": "Nachos", "price": "$12", "desc": "Fromage, jalapeño, pico, crème."},
            {"name": "Burger & Frites", "price": "$15", "desc": "Bœuf Angus, cheddar, laitue, tomate."},
        ],
    },
}


# ============================================================
# Language strings (prompts + “recall”)
# ============================================================
LANG = {
    "en": {
        "welcome": "Hi! Want to book a table for a match day? Ask me anything.",
        "ask_date": "What date would you like? (Example: June 23, 2026)\n\n(You can also type: “Recall reservation so far”)",
        "ask_time": "What time would you like?",
        "ask_party": "How many people are in your party?",
        "ask_name": "What name should we put the reservation under?",
        "ask_phone": "What phone number should we use?",
        "recall_title": "Reservation so far:",
        "recall_empty": "No reservation details yet. Say “reservation” to start.",
        "saved": "✅ Reservation saved!",
        "rule_party": "⚠️ That party size is above our limit. Please call the business to confirm a larger group.",
        "rule_closed": "⚠️ We’re closed on that date. Want the next available day?",
    },
    "es": {
        "welcome": "¡Hola! ¿Quieres reservar una mesa para un día de partido? Pregúntame lo que sea.",
        "ask_date": "¿Qué fecha te gustaría? (Ejemplo: 23 de junio de 2026)\n\n(También puedes escribir: “Recordar reserva”)",
        "ask_time": "¿A qué hora te gustaría?",
        "ask_party": "¿Cuántas personas serán?",
        "ask_name": "¿A nombre de quién será la reserva?",
        "ask_phone": "¿Qué número de teléfono debemos usar?",
        "recall_title": "Reserva hasta ahora:",
        "recall_empty": "Aún no hay detalles. Escribe “reserva” para comenzar.",
        "saved": "✅ ¡Reserva guardada!",
        "rule_party": "⚠️ Ese tamaño de grupo supera nuestro límite. Llama al negocio para confirmar un grupo grande.",
        "rule_closed": "⚠️ Estamos cerrados ese día. ¿Quieres el siguiente día disponible?",
    },
    "pt": {
        "welcome": "Olá! Quer reservar uma mesa para dia de jogo? Pergunte qualquer coisa.",
        "ask_date": "Qual data você gostaria? (Exemplo: 23 de junho de 2026)\n\n(Você também pode digitar: “Relembrar reserva”)",
        "ask_time": "Que horas você gostaria?",
        "ask_party": "Quantas pessoas?",
        "ask_name": "Em qual nome devemos colocar a reserva?",
        "ask_phone": "Qual número de telefone devemos usar?",
        "recall_title": "Reserva até agora:",
        "recall_empty": "Ainda não há detalhes. Digite “reserva” para começar.",
        "saved": "✅ Reserva salva!",
        "rule_party": "⚠️ Esse tamanho de grupo excede o limite. Ligue para confirmar um grupo maior.",
        "rule_closed": "⚠️ Estaremos fechados nessa data. Quer o próximo dia disponível?",
    },
    "fr": {
        "welcome": "Bonjour ! Vous voulez réserver une table pour un jour de match ? Demandez-moi !",
        "ask_date": "Quelle date souhaitez-vous ? (Exemple : 23 juin 2026)\n\n(Vous pouvez aussi écrire : « Rappeler la réservation »)",
        "ask_time": "À quelle heure ?",
        "ask_party": "Pour combien de personnes ?",
        "ask_name": "Au nom de qui ?",
        "ask_phone": "Quel numéro de téléphone devons-nous utiliser ?",
        "recall_title": "Réservation jusqu’ici :",
        "recall_empty": "Aucun détail pour l’instant. Dites « réservation » pour commencer.",
        "saved": "✅ Réservation enregistrée !",
        "rule_party": "⚠️ Ce nombre dépasse notre limite. Veuillez appeler pour un grand groupe.",
        "rule_closed": "⚠️ Nous sommes fermés ce jour-là. Voulez-vous le prochain jour disponible ?",
    },
}

SUPPORTED_LANGS = ["en", "es", "pt", "fr"]


def norm_lang(lang: Optional[str]) -> str:
    if not lang:
        return "en"
    lang = lang.lower().strip()
    if lang in SUPPORTED_LANGS:
        return lang
    # allow common aliases
    if lang.startswith("sp"):
        return "es"
    if lang.startswith("po"):
        return "pt"
    if lang.startswith("fr"):
        return "fr"
    return "en"


# ============================================================
# Rate limiting (in-memory per IP)
# ============================================================
_rate_buckets: Dict[str, Dict[str, Any]] = {}


def client_ip() -> str:
    # Render often sets X-Forwarded-For
    xff = request.headers.get("X-Forwarded-For", "")
    if xff:
        return xff.split(",")[0].strip()
    return request.remote_addr or "unknown"


def check_rate_limit(ip: str) -> Tuple[bool, int]:
    """
    Returns (allowed, remaining_in_window).
    Fixed window: per-minute.
    """
    now = int(time.time())
    window = now // 60
    b = _rate_buckets.get(ip)
    if not b or b["window"] != window:
        _rate_buckets[ip] = {"window": window, "count": 1}
        return True, max(RATE_LIMIT_PER_MIN - 1, 0)

    if b["count"] >= RATE_LIMIT_PER_MIN:
        return False, 0

    b["count"] += 1
    remaining = max(RATE_LIMIT_PER_MIN - b["count"], 0)
    return True, remaining


# ============================================================
# Google Sheets helpers
# ============================================================
def get_gspread_client():
    if os.environ.get("GOOGLE_CREDS_JSON"):
        creds_info = json.loads(os.environ["GOOGLE_CREDS_JSON"])
        creds = Credentials.from_service_account_info(creds_info, scopes=SCOPES)
        return gspread.authorize(creds)

    creds_file = "google_creds.json"
    if os.path.exists(creds_file):
        creds = Credentials.from_service_account_file(creds_file, scopes=SCOPES)
        return gspread.authorize(creds)

    raise RuntimeError("Google credentials not found. Set GOOGLE_CREDS_JSON or provide google_creds.json locally.")


def append_lead_to_sheet(lead: Dict[str, Any]):
    gc = get_gspread_client()
    ws = gc.open(SHEET_NAME).sheet1
    ws.append_row([
        datetime.now().isoformat(timespec="seconds"),
        lead.get("name", ""),
        lead.get("phone", ""),
        lead.get("date", ""),
        lead.get("time", ""),
        int(lead.get("party_size") or 0),
        lead.get("language", "en"),
    ])


def read_leads(limit: int = 200) -> List[List[str]]:
    gc = get_gspread_client()
    ws = gc.open(SHEET_NAME).sheet1
    rows = ws.get_all_values()
    if not rows:
        return []
    header = rows[0]
    body = rows[1:]
    body = body[-limit:]
    return [header] + body


# ============================================================
# Schedule helpers
# ============================================================
def parse_iso_date(d: str) -> Optional[date]:
    try:
        return datetime.strptime(d, "%Y-%m-%d").date()
    except Exception:
        return None


def get_next_match(now_date: date) -> Optional[Dict[str, Any]]:
    future = []
    for m in DALLAS_MATCHES:
        md = parse_iso_date(m.get("date", ""))
        if not md:
            continue
        if md >= now_date:
            future.append((md, m))
    future.sort(key=lambda x: x[0])
    return future[0][1] if future else None


def is_match_day(now_date: date) -> bool:
    for m in DALLAS_MATCHES:
        md = parse_iso_date(m.get("date", ""))
        if md == now_date:
            return True
    return False


# ============================================================
# Reservation state machine (in-memory sessions)
# ============================================================
_sessions: Dict[str, Dict[str, Any]] = {}


def get_session_id() -> str:
    """
    Front-end should send session_id for stable memory.
    Fallback: IP + UA.
    """
    data = request.get_json(silent=True) or {}
    sid = (data.get("session_id") or "").strip()
    if sid:
        return sid
    return f"{client_ip()}::{request.headers.get('User-Agent','')[:40]}"


def get_session(sid: str) -> Dict[str, Any]:
    s = _sessions.get(sid)
    if not s:
        s = {
            "mode": "idle",         # idle | reserving
            "lang": "en",
            "lead": {"name": "", "phone": "", "date": "", "time": "", "party_size": 0, "language": "en"},
            "updated_at": time.time(),
        }
        _sessions[sid] = s
    return s


def want_recall(text: str, lang: str) -> bool:
    t = text.lower().strip()
    triggers = [
        "recall reservation", "recall", "reservation so far",
        "recordar reserva", "recordar", "reserva hasta ahora",
        "relembrar reserva", "relembrar", "reserva até agora",
        "rappeler", "réservation", "reservation jusqu",
    ]
    return any(x in t for x in triggers)


def want_reservation(text: str) -> bool:
    t = text.lower()
    return any(k in t for k in ["reservation", "reserve", "book a table", "table for", "reserva", "réservation"])



def extract_name_candidate(text: str) -> Optional[str]:
    """Best-effort name extraction from a mixed reservation sentence.
    Example: 'Jeff party of 6 5pm June 18 2157779999' -> 'Jeff'
    """
    raw = (text or "").strip()
    if not raw:
        return None

    lowered = raw.lower().strip()
    # Don't treat control intents as names
    if lowered in {"reservation", "reserve", "reserva", "réservation", "book", "booking"}:
        return None
    if want_reservation(raw) or want_recall(raw, "en") or "recall" in lowered:
        return None

    # Take everything before the first keyword that likely starts non-name info
    lower = raw.lower()
    cut_keywords = [" party", "party", " table", "table", " at ", " on ", " for ", " pm", " am", "/", "-", ":", "phone"]
    cut_at = len(raw)
    for kw in cut_keywords:
        idx = lower.find(kw)
        if idx != -1:
            cut_at = min(cut_at, idx)
    head = raw[:cut_at].strip()

    # Remove digits and symbols; keep letters/spaces/apostrophes
    head = re.sub(r"[^A-Za-zÀ-ÿ' ]+", " ", head)
    head = re.sub(r"\s+", " ", head).strip()

    # Must contain at least 2 letters total
    if len(re.sub(r"[^A-Za-zÀ-ÿ]+", "", head)) < 2:
        return None

    # Limit to a few words
    parts = head.split()
    head = " ".join(parts[:4]).strip()
    if 2 <= len(head) <= 40:
        return head
    return None


def extract_party_size(text: str) -> Optional[int]:
    t = text.lower()
    m = re.search(r"party\s*of\s*(\d+)", t)
    if m:
        return int(m.group(1))
    m = re.search(r"table\s*of\s*(\d+)", t)
    if m:
        return int(m.group(1))
    m = re.search(r"\b(\d+)\b", t)
    if m:
        n = int(m.group(1))
        if 1 <= n <= 200:
            return n
    return None


def extract_phone(text: str) -> Optional[str]:
    digits = re.sub(r"\D+", "", text)
    if len(digits) == 11 and digits.startswith("1"):
        digits = digits[1:]
    if len(digits) == 10:
        return digits
    return None


def extract_time(text: str) -> Optional[str]:
    t = text.lower().strip()
    # 7pm / 7 pm / 19:30
    m = re.search(r"\b(\d{1,2})(?::(\d{2}))?\s*(am|pm)\b", t)
    if m:
        hh = int(m.group(1))
        mm = m.group(2) or "00"
        ap = m.group(3)
        return f"{hh}:{mm} {ap}"
    m = re.search(r"\b(\d{1,2}):(\d{2})\b", t)
    if m:
        return f"{m.group(1)}:{m.group(2)}"
    return None


def extract_date(text: str) -> Optional[str]:
    t = text.strip()

    # ISO: 2026-06-23
    m = re.search(r"\b(20\d{2})-(\d{2})-(\d{2})\b", t)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"

    # US: 06/23/2026 or 6/23/26
    m = re.search(r"\b(\d{1,2})/(\d{1,2})/(\d{2,4})\b", t)
    if m:
        mm = int(m.group(1))
        dd = int(m.group(2))
        yy = int(m.group(3))
        if yy < 100:
            yy += 2000
        return f"{yy:04d}-{mm:02d}-{dd:02d}"

    # Month name + day (+ optional year)
    lower = t.lower()
    month_map = {
        # English
        "january": 1, "jan": 1, "february": 2, "feb": 2, "march": 3, "mar": 3,
        "april": 4, "apr": 4, "may": 5, "june": 6, "jun": 6, "july": 7, "jul": 7,
        "august": 8, "aug": 8, "september": 9, "sep": 9, "sept": 9, "october": 10, "oct": 10,
        "november": 11, "nov": 11, "december": 12, "dec": 12,
        # Spanish
        "enero": 1, "febrero": 2, "marzo": 3, "abril": 4, "mayo": 5, "junio": 6, "julio": 7,
        "agosto": 8, "septiembre": 9, "octubre": 10, "noviembre": 11, "diciembre": 12,
        # Portuguese
        "janeiro": 1, "fevereiro": 2, "março": 3, "abril": 4, "maio": 5, "junho": 6, "julho": 7,
        "agosto": 8, "setembro": 9, "outubro": 10, "novembro": 11, "dezembro": 12,
        # French
        "janvier": 1, "février": 2, "fevrier": 2, "mars": 3, "avril": 4, "mai": 5,
        "juin": 6, "juillet": 7, "août": 8, "aout": 8, "septembre": 9, "octobre": 10,
        "novembre": 11, "décembre": 12, "decembre": 12,
    }

    # Find month word, then day number
    for word, mon in month_map.items():
        if re.search(rf"\b{re.escape(word)}\b", lower):
            m = re.search(rf"\b{re.escape(word)}\b\D*(\d{{1,2}})", lower)
            if m:
                dd = int(m.group(1))
                y = 2026  # default year for World Cup focus
                my = re.search(r"\b(20\d{2})\b", lower)
                if my:
                    y = int(my.group(1))
                return f"{y:04d}-{mon:02d}-{dd:02d}"

    return None


def validate_date_iso(d_iso: str) -> bool:
    try:
        datetime.strptime(d_iso, "%Y-%m-%d")
        return True
    except Exception:
        return False


def apply_business_rules(lead: Dict[str, Any]) -> Optional[str]:
    # closed date check
    d_iso = lead.get("date", "")
    if d_iso and d_iso in set(BUSINESS_RULES.get("closed_dates", [])):
        return "closed"

    # party size check
    ps = int(lead.get("party_size") or 0)
    if ps and ps > int(BUSINESS_RULES.get("max_party_size", 999)):
        return "party"

    return None


def recall_text(sess: Dict[str, Any]) -> str:
    lang = sess.get("lang", "en")
    L = LANG[lang]
    lead = sess["lead"]
    parts = []
    if any([lead.get("date"), lead.get("time"), lead.get("party_size"), lead.get("name"), lead.get("phone")]):
        parts.append(L["recall_title"])
        parts.append(f"- Date: {lead.get('date') or '—'}")
        parts.append(f"- Time: {lead.get('time') or '—'}")
        parts.append(f"- Party size: {lead.get('party_size') or '—'}")
        parts.append(f"- Name: {lead.get('name') or '—'}")
        parts.append(f"- Phone: {lead.get('phone') or '—'}")
        return "\n".join(parts)
    return L["recall_empty"]


def next_question(sess: Dict[str, Any]) -> str:
    lang = sess.get("lang", "en")
    L = LANG[lang]
    lead = sess["lead"]

    if not lead.get("date"):
        return L["ask_date"]
    if not lead.get("time"):
        return L["ask_time"]
    if not lead.get("party_size"):
        return L["ask_party"]
    if not lead.get("name"):
        return L["ask_name"]
    if not lead.get("phone"):
        return L["ask_phone"]
    return ""


# ============================================================
# Public endpoints
# ============================================================
@app.route("/")
def home():
    return send_from_directory(BASE_DIR, "index.html")


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


# ============================================================
# Backward-compatible API routes for older index.html builds
# ============================================================
DEFAULT_COUNTDOWN_TARGET = os.environ.get("COUNTDOWN_TARGET", "2026-06-11T00:00:00")

UI_HELPER = {
    "en": 'Try: "reservation" to book • "Recall reservation so far" • Ask about the menu • Ask about Dallas match days.',
    "es": 'Prueba: "reserva" para reservar • "Recordar reserva" • Pregunta por el menú • Pregunta por los partidos en Dallas.',
    "pt": 'Tente: "reserva" para reservar • "Relembrar reserva" • Pergunte sobre o cardápio • Pergunte sobre jogos em Dallas.',
    "fr": 'Essayez : "réservation" pour réserver • "Rappeler la réservation" • Demandez le menu • Demandez les matchs à Dallas.',
}

UI_LANG_SET = {
    "en": "Language set to English.",
    "es": "Idioma cambiado a Español.",
    "pt": "Idioma definido para Português.",
    "fr": "Langue définie sur Français.",
}


@app.route("/api/config")
def api_config():
    return jsonify({
        "countdown_target": DEFAULT_COUNTDOWN_TARGET,
        "business_rules": BUSINESS_RULES,
    })


@app.route("/api/ui")
def api_ui():
    lang = norm_lang(request.args.get("lang", "en"))
    strings = dict(LANG[lang])
    # extra strings expected by older frontends
    strings["helper"] = UI_HELPER.get(lang, UI_HELPER["en"])
    strings["lang_set"] = UI_LANG_SET.get(lang, UI_LANG_SET["en"])
    return jsonify({"lang": lang, "strings": strings})


@app.route("/api/schedule")
def api_schedule():
    # same payload as /schedule.json, but kept for older frontends
    return schedule_json()


@app.route("/api/menu")
def api_menu():
    # older frontend expects {items:[...]} (no wrapper)
    lang = norm_lang(request.args.get("lang", "en"))
    return jsonify({"lang": lang, "items": MENU[lang]["items"]})

@app.route("/menu.json")
def menu_json():
    lang = norm_lang(request.args.get("lang", "en"))
    return jsonify({"lang": lang, "menu": MENU[lang]})


@app.route("/schedule.json")
def schedule_json():
    today = datetime.now().date()
    nxt = get_next_match(today)
    return jsonify({
        "today": today.isoformat(),
        "is_match_day": is_match_day(today),
        "match_day_banner": BUSINESS_RULES.get("match_day_banner", ""),
        "next_match": nxt,
        "matches": DALLAS_MATCHES,
    })


@app.route("/test-sheet")
def test_sheet():
    try:
        append_lead_to_sheet({
            "name": "TEST_NAME",
            "phone": "2145551212",
            "date": "2026-06-23",
            "time": "7:00 pm",
            "party_size": 4,
            "language": "en",
        })
        return jsonify({"ok": True, "sheet": SHEET_NAME, "message": "✅ Test row appended."})
    except Exception as e:
        return jsonify({"ok": False, "sheet": SHEET_NAME, "error": repr(e)}), 500


# ============================================================
# Admin dashboard
# ============================================================
@app.route("/admin")
def admin():
    key = request.args.get("key", "")
    if not ADMIN_KEY or key != ADMIN_KEY:
        return "Unauthorized", 401

    try:
        rows = read_leads(limit=300)
    except Exception as e:
        return f"Error reading leads: {repr(e)}", 500

    if not rows:
        return "<h2>No leads yet.</h2>"

    header = rows[0]
    body = rows[1:]

    # Simple HTML table
    html = []
    html.append("<html><head><meta charset='utf-8'><title>Leads Admin</title>")
    html.append("<style>body{font-family:Arial;padding:16px} table{border-collapse:collapse;width:100%} th,td{border:1px solid #ddd;padding:8px;font-size:12px} th{background:#f4f4f4}</style>")
    html.append("</head><body>")
    html.append(f"<h2>Leads Admin — {SHEET_NAME}</h2>")
    html.append(f"<p>Rows shown: {len(body)}</p>")
    html.append("<table><thead><tr>")
    for h in header:
        html.append(f"<th>{h}</th>")
    html.append("</tr></thead><tbody>")
    for r in body[::-1]:
        html.append("<tr>")
        for i in range(len(header)):
            val = r[i] if i < len(r) else ""
            html.append(f"<td>{val}</td>")
        html.append("</tr>")
    html.append("</tbody></table>")
    html.append("</body></html>")
    return "\n".join(html)


# ============================================================
# Chat endpoint with reservation state machine
# ============================================================
@app.route("/chat", methods=["POST"])
def chat():
    ip = client_ip()
    allowed, remaining = check_rate_limit(ip)
    if not allowed:
        return jsonify({
            "reply": "⚠️ Too many requests. Please wait a minute and try again.",
            "rate_limit_remaining": 0,
        }), 429

    data = request.get_json(force=True) or {}
    msg = (data.get("message") or "").strip()
    lang = norm_lang(data.get("language"))
    sid = get_session_id()
    sess = get_session(sid)

    # update session language if user toggled
    sess["lang"] = lang
    sess["lead"]["language"] = lang

    if not msg:
        return jsonify({"reply": "Please type a message.", "rate_limit_remaining": remaining})

    # recall support (all languages)
    if want_recall(msg, lang):
        return jsonify({"reply": recall_text(sess), "rate_limit_remaining": remaining})

    # start reservation flow if user indicates intent
    if sess["mode"] == "idle" and want_reservation(msg):
        sess["mode"] = "reserving"
        # attempt to extract any info from this first message
        d_iso = extract_date(msg)
        if d_iso and validate_date_iso(d_iso):
            sess["lead"]["date"] = d_iso
        t = extract_time(msg)
        if t:
            sess["lead"]["time"] = t
        ps = extract_party_size(msg)
        if ps:
            sess["lead"]["party_size"] = ps
        ph = extract_phone(msg)
        if ph:
            sess["lead"]["phone"] = ph

        nm = extract_name_candidate(msg)
        if nm:
            sess["lead"]["name"] = nm

        q = next_question(sess)
        return jsonify({"reply": q, "rate_limit_remaining": remaining})

    # If reserving, keep collecting fields deterministically
    if sess["mode"] == "reserving":
        # Try extract fields from the new message
        d_iso = extract_date(msg)
        if d_iso:
            if validate_date_iso(d_iso):
                sess["lead"]["date"] = d_iso
            else:
                # invalid date like June 31
                # ask for date again
                return jsonify({"reply": LANG[lang]["ask_date"], "rate_limit_remaining": remaining})

        t = extract_time(msg)
        if t:
            sess["lead"]["time"] = t

        ps = extract_party_size(msg)
        if ps:
            sess["lead"]["party_size"] = ps

        ph = extract_phone(msg)
        if ph:
            sess["lead"]["phone"] = ph

        # name (best-effort): support mixed messages like:
        #   Jeff party of 6 5pm June 18 2157779999
        if not sess["lead"]["name"]:
            nm = extract_name_candidate(msg)
            if nm:
                sess["lead"]["name"] = nm

        # apply business rules if we have enough to check
        rule = apply_business_rules(sess["lead"])
        if rule == "party":
            sess["mode"] = "idle"
            return jsonify({"reply": LANG[lang]["rule_party"], "rate_limit_remaining": remaining})
        if rule == "closed":
            sess["mode"] = "idle"
            return jsonify({"reply": LANG[lang]["rule_closed"], "rate_limit_remaining": remaining})

        # If complete, save + confirm
        lead = sess["lead"]
        if lead.get("date") and lead.get("time") and lead.get("party_size") and lead.get("name") and lead.get("phone"):
            try:
                append_lead_to_sheet(lead)
                # reset session mode but keep language
                sess["mode"] = "idle"
                saved_msg = LANG[lang]["saved"]
                confirm = (
                    f"{saved_msg}\n"
                    f"Name: {lead['name']}\n"
                    f"Phone: {lead['phone']}\n"
                    f"Date: {lead['date']}\n"
                    f"Time: {lead['time']}\n"
                    f"Party size: {lead['party_size']}"
                )
                # clear lead for next reservation
                sess["lead"] = {"name": "", "phone": "", "date": "", "time": "", "party_size": 0, "language": lang}
                return jsonify({"reply": confirm, "rate_limit_remaining": remaining})
            except Exception as e:
                return jsonify({"reply": f"⚠️ Could not save reservation: {repr(e)}", "rate_limit_remaining": remaining}), 500

        # Otherwise ask next missing field (in selected language)
        q = next_question(sess)
        return jsonify({"reply": q, "rate_limit_remaining": remaining})

    # Otherwise: normal Q&A using OpenAI (with language + business profile + menu)
    # (We keep this for general questions, not reservations.)
    system_msg = f"""
You are a World Cup 2026 Dallas business concierge.

Business profile (source of truth):
{BUSINESS_PROFILE}

Menu (source of truth, language={lang}):
{json.dumps(MENU.get(lang, MENU["en"]), ensure_ascii=False)}

Rules:
- Be friendly, fast, and concise.
- Always respond in the user's chosen language: {lang}.
- If user asks about the Dallas World Cup match schedule, tell them you can show the on-page schedule.
- If user asks to make a reservation, instruct them to type "reservation" (or equivalent) to start.
"""

    resp = client.responses.create(
        model="gpt-4o-mini",
        input=[
            {"role": "system", "content": system_msg},
            {"role": "user", "content": msg},
        ],
    )

    reply = (resp.output_text or "").strip()
    return jsonify({"reply": reply, "rate_limit_remaining": remaining})


# ============================================================
# Run
# ============================================================
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5050))
    app.run(host="0.0.0.0", port=port, debug=False)