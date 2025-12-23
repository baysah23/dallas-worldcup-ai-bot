from dotenv import load_dotenv
load_dotenv()

import os
import json
import re
import time
from datetime import datetime, date, timezone
from typing import Dict, Any, Optional, List, Tuple

from flask import Flask, request, jsonify, send_from_directory
try:
    from openai import OpenAI  # type: ignore
except Exception:
    OpenAI = None  # type: ignore
try:
    import gspread  # type: ignore
except Exception:
    gspread = None  # type: ignore
try:
    from google.oauth2.service_account import Credentials  # type: ignore
except Exception:
    Credentials = None  # type: ignore
# ============================================================
# App + cache-busting (helps Render show latest index.html)
# ============================================================

# ---- Safety helpers (prevents Internal Server Error on missing deps) ----
def _gs_ready() -> bool:
    return (gspread is not None) and (Credentials is not None)

def _openai_ready() -> bool:
    return OpenAI is not None

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
BUSINESS_PROFILE_PATH = "business_profile.txt"
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
# World Cup 2026 schedule data
# - We load ALL matches from a public JSON feed (no API key).
# - Dallas-only schedule is filtered from the full list.
# ============================================================
FIXTURE_FEED_URL = os.environ.get(
    "FIXTURE_FEED_URL",
    "https://fixturedownload.com/feed/json/fifa-world-cup-2026",
)

# Location label used by the feed for Dallas/Arlington matches.
# (Verified in the fixture feed as "Dallas Stadium".)
DALLAS_LOCATION_KEYWORDS = ["dallas stadium", "arlington", "at&t"]

# In-memory cache (plus optional disk cache) so we don't hit the feed too often.
_fixtures_cache: Dict[str, Any] = {"loaded_at": 0, "matches": []}
FIXTURE_CACHE_SECONDS = int(os.environ.get("FIXTURE_CACHE_SECONDS", str(6 * 60 * 60)))  # 6h
FIXTURE_CACHE_FILE = os.environ.get("FIXTURE_CACHE_FILE", "/tmp/wc26_fixtures.json")


def _safe_read_json_file(path: str) -> Optional[Any]:
    try:
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception:
        return None
    return None


def _safe_write_json_file(path: str, payload: Any) -> None:
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False)
    except Exception:
        # best effort; don't fail the app if disk isn't writable
        pass


def _fetch_fixture_feed() -> List[Dict[str, Any]]:
    """
    Fetch fixture feed and return raw list of matches.
    Expected schema example:
      {
        "MatchNumber": 1,
        "RoundNumber": 1,
        "DateUtc": "2026-06-11 19:00:00Z",
        "Location": "Mexico City Stadium",
        "HomeTeam": "Mexico",
        "AwayTeam": "South Africa",
        "Group": "Group A",
        ...
      }
    """
    import urllib.request

    req = urllib.request.Request(
        FIXTURE_FEED_URL,
        headers={"User-Agent": "worldcup-concierge/1.0"},
        method="GET",
    )
    with urllib.request.urlopen(req, timeout=15) as resp:
        raw = resp.read().decode("utf-8", errors="replace")
    payload = json.loads(raw)
    if not isinstance(payload, list):
        raise ValueError("Fixture feed response was not a list")
    return payload


def _fmt_time_12h(dt_utc: datetime) -> str:
    """Return a friendly 12-hour local-time string like '7:00 PM'.

    Notes:
    - dt_utc is assumed to be timezone-aware (UTC).
    - We display in the server's local timezone (Render is typically UTC unless TZ is set).
    - Uses a cross-platform formatter (Windows doesn't support %-I).
    """
    local_dt = dt_utc.astimezone()  # local tz on host
    # %I gives zero-padded hour; strip leading 0 for '7:00 PM'
    return local_dt.strftime("%I:%M %p").lstrip("0")


def _parse_dateutc(date_utc_str: str) -> Optional[datetime]:
    """
    Example: '2026-06-11 19:00:00Z'
    """
    try:
        s = (date_utc_str or "").strip()
        if not s:
            return None
        # Normalize to ISO-ish for fromisoformat by removing Z
        if s.endswith("Z"):
            s2 = s[:-1].replace(" ", "T")
            dt = datetime.fromisoformat(s2)
            return dt.replace(tzinfo=timezone.utc)
        # fallback
        s2 = s.replace(" ", "T")
        dt = datetime.fromisoformat(s2)
        return dt.replace(tzinfo=timezone.utc)
    except Exception:
        return None


def load_all_matches(force: bool = False) -> List[Dict[str, Any]]:
    """
    Returns a normalized list of matches.

    Each match:
      {
        "id": "wc-001",
        "match_number": 1,
        "stage": "Group A" / "Round of 32" / ...
        "date": "2026-06-11",
        "time": "7:00 PM",        # server-local formatted
        "datetime_utc": "2026-06-11T19:00:00Z",
        "venue": "Mexico City Stadium",
        "home": "Mexico",
        "away": "South Africa",
      }
    """
    now = int(time.time())
    if not force and _fixtures_cache["matches"] and (now - int(_fixtures_cache["loaded_at"] or 0) < FIXTURE_CACHE_SECONDS):
        return _fixtures_cache["matches"]

    # Try disk cache first (fast + survives restarts)
    disk = _safe_read_json_file(FIXTURE_CACHE_FILE)
    if disk and isinstance(disk, dict) and isinstance(disk.get("matches"), list):
        loaded_at = int(disk.get("loaded_at") or 0)
        if not force and loaded_at and (now - loaded_at < FIXTURE_CACHE_SECONDS):
            _fixtures_cache["loaded_at"] = loaded_at
            _fixtures_cache["matches"] = disk["matches"]
            return _fixtures_cache["matches"]

    # Fetch fresh
    raw_matches = _fetch_fixture_feed()

    norm: List[Dict[str, Any]] = []
    for m in raw_matches:
        dt = _parse_dateutc(m.get("DateUtc") or "")
        if not dt:
            continue

        match_num = int(m.get("MatchNumber") or 0) or None
        match_id = f"wc-{match_num:03d}" if match_num else f"wc-{len(norm)+1:03d}"

        norm.append({
            "id": match_id,
            "match_number": match_num,
            "stage": (m.get("Group") or "").strip() or "Match",
            "date": dt.date().isoformat(),
            "time": _fmt_time_12h(dt),
            "datetime_utc": dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "venue": (m.get("Location") or "").strip(),
            "home": (m.get("HomeTeam") or "").strip(),
            "away": (m.get("AwayTeam") or "").strip(),
        })

    # Sort by kickoff UTC
    norm.sort(key=lambda x: x.get("datetime_utc") or "")
    _fixtures_cache["loaded_at"] = now
    _fixtures_cache["matches"] = norm
    _safe_write_json_file(FIXTURE_CACHE_FILE, {"loaded_at": now, "matches": norm})

    return norm


def is_dallas_match(m: Dict[str, Any]) -> bool:
    v = (m.get("venue") or "").lower()
    return any(k in v for k in DALLAS_LOCATION_KEYWORDS)


def filter_matches(scope: str, q: str = "") -> List[Dict[str, Any]]:
    scope = (scope or "dallas").lower().strip()
    q = (q or "").strip().lower()

    matches = load_all_matches()
    if scope != "all":
        matches = [m for m in matches if is_dallas_match(m)]

    if q:
        def hit(m):
            return (q in (m.get("home","").lower())
                    or q in (m.get("away","").lower())
                    or q in (m.get("venue","").lower())
                    or q in (m.get("stage","").lower())
                    or q in (m.get("date","").lower()))
        matches = [m for m in matches if hit(m)]

    return matches


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
        "welcome": "⚽ Welcome, World Cup fan! I'm your Dallas Match-Day Concierge.\nType reservation to book a table, or ask about Dallas matches, all matches, or the menu.",
        "ask_date": "What date would you like? (Example: June 23, 2026)\n\n(You can also type: “Recall reservation so far”)",
        "ask_time": "What time would you like?",
        "ask_party": "How many people are in your party?",
        "ask_name": "What name should we put the reservation under?",
        "ask_phone": "What phone number should we use?",
        "recall_title": "📌 Reservation so far:",
        "recall_empty": "No reservation details yet. Say “reservation” to start.",
        "saved": "✅ Reservation saved!",
        "rule_party": "⚠️ That party size is above our limit. Please call the business to confirm a larger group.",
        "rule_closed": "⚠️ We’re closed on that date. Want the next available day?",
    },
    "es": {
        "welcome": "⚽ ¡Bienvenido, fan del Mundial! Soy tu concierge de días de partido en Dallas.\nEscribe reserva para reservar una mesa, o pregunta por los partidos (Dallas / todos) o el menú.",
        "ask_date": "¿Qué fecha te gustaría? (Ejemplo: 23 de junio de 2026)\n\n(También puedes escribir: “Recordar reserva”)",
        "ask_time": "¿A qué hora te gustaría?",
        "ask_party": "¿Cuántas personas serán?",
        "ask_name": "¿A nombre de quién será la reserva?",
        "ask_phone": "¿Qué número de teléfono debemos usar?",
        "recall_title": "📌 Reserva hasta ahora:",
        "recall_empty": "Aún no hay detalles. Escribe “reserva” para comenzar.",
        "saved": "✅ ¡Reserva guardada!",
        "rule_party": "⚠️ Ese tamaño de grupo supera nuestro límite. Llama al negocio para confirmar un grupo grande.",
        "rule_closed": "⚠️ Estamos cerrados ese día. ¿Quieres el siguiente día disponible?",
    },
    "pt": {
        "welcome": "⚽ Bem-vindo, fã da Copa do Mundo! Sou seu concierge de dias de jogo em Dallas.\nDigite reserva para reservar uma mesa, ou pergunte sobre jogos em Dallas, todos os jogos ou o cardápio.",
        "ask_date": "Qual data você gostaria? (Exemplo: 23 de junho de 2026)\n\n(Você também pode digitar: “Relembrar reserva”)",
        "ask_time": "Que horas você gostaria?",
        "ask_party": "Quantas pessoas?",
        "ask_name": "Em qual nome devemos colocar a reserva?",
        "ask_phone": "Qual número de telefone devemos usar?",
        "recall_title": "📌 Reserva até agora:",
        "recall_empty": "Ainda não há detalhes. Digite “reserva” para começar.",
        "saved": "✅ Reserva salva!",
        "rule_party": "⚠️ Esse tamanho de grupo excede o limite. Ligue para confirmar um grupo maior.",
        "rule_closed": "⚠️ Estaremos fechados nessa data. Quer o próximo dia disponível?",
    },
    "fr": {
        "welcome": "⚽ Bienvenue, fan de la Coupe du Monde ! Je suis votre concierge des jours de match à Dallas.\nTapez réservation pour réserver une table, ou demandez les matchs (Dallas / tous) ou le menu.",
        "ask_date": "Quelle date souhaitez-vous ? (Exemple : 23 juin 2026)\n\n(Vous pouvez aussi écrire : « Rappeler la réservation »)",
        "ask_time": "À quelle heure ?",
        "ask_party": "Pour combien de personnes ?",
        "ask_name": "Au nom de qui ?",
        "ask_phone": "Quel numéro de téléphone devons-nous utiliser ?",
        "recall_title": "📌 Réservation jusqu’ici :",
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
        creds = (Credentials.from_service_account_info if Credentials else (_ for _ in ()).throw(RuntimeError('Missing Google Sheets dependency')))(creds_info, scopes=SCOPES)
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


def extract_party_size(text: str) -> Optional[int]:
    """Extract party size from free text.

    IMPORTANT: avoid mis-reading dates like 'June 13' as a party size.
    We only accept a standalone number when the message does NOT look like a date/time.
    """
    raw = (text or "").strip()
    if not raw:
        return None
    t = raw.lower()

    # Strong patterns
    m = re.search(r"party\s*of\s*(\d+)", t)
    if m:
        return int(m.group(1))
    m = re.search(r"table\s*of\s*(\d+)", t)
    if m:
        return int(m.group(1))
    m = re.search(r"for\s*(\d+)\s*(people|persons|guests|pax)\b", t)
    if m:
        return int(m.group(1))

    # If the text looks like a date or time, do not treat numbers as party size.
    months = [
        "january","jan","february","feb","march","mar","april","apr","may","june","jun","july","jul",
        "august","aug","september","sep","sept","october","oct","november","nov","december","dec",
        "enero","febrero","marzo","abril","mayo","junio","julio","agosto","septiembre","octubre","noviembre","diciembre",
        "janeiro","fevereiro","março","marco","abril","maio","junho","julho","agosto","setembro","outubro","novembro","dezembro",
        "janvier","février","fevrier","mars","avril","mai","juin","juillet","août","aout","septembre","octobre","novembre","décembre","decembre",
    ]
    if any(mo in t for mo in months):
        return None
    if re.search(r"\b\d{4}-\d{1,2}-\d{1,2}\b", t):
        return None
    if re.search(r"\b\d{1,2}[/-]\d{1,2}([/-]\d{2,4})?\b", t):
        return None
    if re.search(r"\b\d{1,2}(:\d{2})\s*(am|pm)?\b", t):
        return None

    # Fallback: a plain number, but keep it reasonable
    m = re.search(r"\b(\d+)\b", t)
    if m:
        n = int(m.group(1))
        if 1 <= n <= 200:
            return n
    return None



def extract_phone(text: str) -> Optional[str]:
    """Extract a US phone number from free text.
    Looks for a 10-digit sequence (optionally preceded by country code 1),
    and ignores other digits (party size, dates, times).
    """
    s = (text or "").strip()
    if not s:
        return None

    # Prefer explicit 10-digit runs anywhere in the string
    digits_only = re.sub(r"\D+", "", s)

    # Try to find a 10-digit chunk inside the full digit stream (e.g., '...2157779999')
    m = re.search(r"(\d{10})", digits_only)
    if m:
        return m.group(1)

    # Try common separated formats: (215) 777-9999, 215-777-9999, 1 215 777 9999
    m = re.search(r"(?:\b1\D*)?(\d{3})\D*(\d{3})\D*(\d{4})\b", s)
    if m:
        return f"{m.group(1)}{m.group(2)}{m.group(3)}"

    return None


def extract_name(text: str) -> Optional[str]:
    """Best-effort name extraction from a mixed reservation message."""
    raw = (text or "").strip()
    if not raw:
        return None
    lower = raw.lower().strip()

    # Don't treat reservation trigger words as a person's name
    trigger_words = {
        "reservation", "reserve", "reserving", "book", "booking", "book a table",
        "reserva", "reservar", "réservation", "réservation"
    }
    if lower in trigger_words:
        return None

    # If message contains 'party', take words before 'party'
    if "party" in lower:
        pre = raw[:lower.find("party")].strip()
        pre = re.sub(r"[^A-Za-z\s\-\'\.]", "", pre).strip()
        pre = re.sub(r"\s+", " ", pre)
        if 1 <= len(pre) <= 40:
            return pre

    # Otherwise, take leading letters until the first digit
    name_part = ""
    for ch in raw:
        if ch.isdigit():
            break
        name_part += ch
    name_part = name_part.strip()
    name_part = re.sub(r"\s+", " ", name_part)
    name_part = re.sub(r"[^A-Za-z\s\-\'\.]", "", name_part).strip()
    if 1 <= len(name_part) <= 40:
        return name_part

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


def extract_name_candidate(text: str) -> Optional[str]:
    """Best-effort name extraction from a mixed reservation message.

    Example:
      'jeff party of 6 5pm june 18 2157779999' -> 'jeff'
    """
    s = (text or "").strip()
    if not s:
        return None

    lower = s.lower().strip()
    # Don't treat trigger words as names
    if lower in ["reservation", "reserva", "réservation", "reserve", "book", "book a table"]:
        return None

    # Remove phone numbers (many formats)
    s = re.sub(r"\b(?:\+?1\s*)?\(?\d{3}\)?[-.\s]*\d{3}[-.\s]*\d{4}\b", " ", s)

    # Remove explicit party/table patterns
    s = re.sub(r"\bparty\s*of\s*\d+\b", " ", s, flags=re.I)
    s = re.sub(r"\btable\s*(?:for|of)\s*\d+\b", " ", s, flags=re.I)

    # Remove time patterns (5pm, 5:30 pm, 17:00)
    s = re.sub(r"\b\d{1,2}(?::\d{2})?\s*(?:am|pm)\b", " ", s, flags=re.I)
    s = re.sub(r"\b\d{1,2}:\d{2}\b", " ", s)

    # Remove ISO / slash dates
    s = re.sub(r"\b20\d{2}-\d{2}-\d{2}\b", " ", s)
    s = re.sub(r"\b\d{1,2}/\d{1,2}/\d{2,4}\b", " ", s)

    # Remove month-name dates (English/Spanish/Portuguese/French month words)
    month_words = [
        "january","jan","february","feb","march","mar","april","apr","may","june","jun","july","jul",
        "august","aug","september","sep","sept","october","oct","november","nov","december","dec",
        "enero","febrero","marzo","abril","mayo","junio","julio","agosto","septiembre","octubre","noviembre","diciembre",
        "janeiro","fevereiro","março","marco","abril","maio","junho","julho","agosto","setembro","outubro","novembro","dezembro",
        "janvier","février","fevrier","mars","avril","mai","juin","juillet","août","aout","septembre","octobre","novembre","décembre","decembre",
    ]
    s = re.sub(r"\b(?:" + "|".join(re.escape(w) for w in month_words) + r")\b", " ", s, flags=re.I)

    # Remove standalone small numbers (party size, day)
    s = re.sub(r"\b\d{1,3}\b", " ", s)

    # Remove reservation keywords
    s = re.sub(r"\b(reservation|reserve|book|booking|table|party|for|of)\b", " ", s, flags=re.I)

    # Keep letters/apostrophes/spaces only
    s = re.sub(r"[^A-Za-z'\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()

    if not s:
        return None

    # Take up to first 3 words as name (e.g., 'Jeff Smith')
    parts = s.split()
    name = " ".join(parts[:3]).strip()
    if 1 <= len(name) <= 40:
        return name
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
    if any([lead.get("date"), lead.get("time"), lead.get("party_size"), lead.get("name"), lead.get("phone")]):
        parts = [
            L["recall_title"],
            f"Date: {lead.get('date') or '—'}",
            f"Time: {lead.get('time') or '—'}",
            f"Party size: {lead.get('party_size') or '—'}",
            f"Name: {lead.get('name') or '—'}",
            f"Phone: {lead.get('phone') or '—'}",
        ]
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


@app.route("/menu.json")
def menu_json():
    lang = norm_lang(request.args.get("lang", "en"))
    return jsonify({"lang": lang, "menu": MENU[lang]})


@app.route("/schedule.json")
def schedule_json():
    """
    Query params:
      scope= dallas | all   (default: dallas)
      q= search text (team, venue, group, date)
    """
    scope = (request.args.get("scope") or "dallas").lower().strip()
    q = request.args.get("q") or ""

    try:
        matches = filter_matches(scope=scope, q=q)

        today = datetime.now().date()
        if scope == "all":
            # "match day" for Dallas means: any Dallas match today
            is_match = any(m.get("date") == today.isoformat() and is_dallas_match(m) for m in load_all_matches())
        else:
            is_match = any(m.get("date") == today.isoformat() for m in matches)

        # next match (by datetime_utc already sorted in load_all_matches/filter_matches)
        nxt = None
        now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        for m in matches:
            if (m.get("datetime_utc") or "") >= now_utc:
                nxt = m
                break

        return jsonify({
            "scope": scope,
            "query": q,
            "today": today.isoformat(),
            "is_match_day": bool(is_match),
            "match_day_banner": BUSINESS_RULES.get("match_day_banner", ""),
            "next_match": nxt,
            "matches": matches,
        })
    except Exception as e:
        return jsonify({
            "scope": scope,
            "query": q,
            "today": datetime.now().date().isoformat(),
            "is_match_day": False,
            "match_day_banner": BUSINESS_RULES.get("match_day_banner", ""),
            "next_match": None,
            "matches": [],
            "notice": f"Schedule temporarily unavailable: {repr(e)}",
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
    lang = norm_lang(data.get("language") or data.get("lang"))
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

        # IMPORTANT: do NOT treat the word "reservation" as the name.
        # Clear any accidental name just in case.
        if msg.lower().strip() in ["reservation", "reserva", "réservation"]:
            sess["lead"]["name"] = ""

        q = next_question(sess)
        return jsonify({"reply": q, "rate_limit_remaining": remaining})

    # If reserving, keep collecting fields deterministically
    if sess["mode"] == "reserving":
        # Extract fields from message (order doesn't matter)
        d_iso = extract_date(msg)
        if d_iso:
            if validate_date_iso(d_iso):
                sess["lead"]["date"] = d_iso
            else:
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

        # NAME extraction
        if not sess["lead"]["name"]:
            cand = extract_name_candidate(msg)
            if cand:
                sess["lead"]["name"] = cand

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
                sess["lead"] = {"name": "", "phone": "", "date": "", "time": "", "party_size": 0, "language": lang}
                return jsonify({"reply": confirm, "rate_limit_remaining": remaining})
            except Exception as e:
                return jsonify({"reply": f"⚠️ Could not save reservation: {repr(e)}", "rate_limit_remaining": remaining}), 500

        # Otherwise ask next missing field
        q = next_question(sess)
        return jsonify({"reply": q, "rate_limit_remaining": remaining})

    # Otherwise: normal Q&A using OpenAI (with language + business profile + menu)
    system_msg = f"""
You are a World Cup 2026 Dallas business concierge.

Business profile (source of truth):
{BUSINESS_PROFILE}

Menu (source of truth, language={lang}):
{json.dumps(MENU.get(lang, MENU["en"]), ensure_ascii=False)}

Rules:
- Be friendly, fast, and concise.
- Always respond in the user's chosen language: {lang}.
- If user asks about the World Cup match schedule, tell them to use the schedule panel on the page.
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


# ============================================================
# FAN PICK POLL (persisted)
# - /poll/match.json : returns configured Match of the Day + totals/% + lock state
# - /poll/vote       : vote once per voter token (client stores token); persists totals
# - /admin/poll/config : edit sponsor label + match_id without redeploy
# Storage:
#   Preferred: same Google Sheet (worksheets PollConfig + PollVotes)
#   Fallback: local JSON at /tmp/wc26_poll.json
# ============================================================
POLL_CONFIG_WS_TITLE = os.environ.get("POLL_CONFIG_WS_TITLE", "PollConfig").strip()
POLL_VOTES_WS_TITLE  = os.environ.get("POLL_VOTES_WS_TITLE",  "PollVotes").strip()
POLL_LOCAL_FILE      = os.environ.get("POLL_LOCAL_FILE", "/tmp/wc26_poll.json")

def _safe_read_json(path: str, default):
    try:
        if not os.path.exists(path):
            return default
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default

def _safe_write_json(path: str, payload) -> None:
    try:
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        os.replace(tmp, path)
    except Exception:
        pass

def _now_iso_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def _poll_defaults():
    return {"sponsor":"Fan Pick","match_id":""}

def _poll_local():
    return _safe_read_json(POLL_LOCAL_FILE, {"config": _poll_defaults(), "votes": {}, "voters": {}})

def _poll_local_save(data):
    _safe_write_json(POLL_LOCAL_FILE, data)

def _poll_open_ws(title: str):
    # Uses your existing Google Sheets connection helpers if present
    if not _gs_ready():
        raise RuntimeError("Google Sheets not available")
    # Prefer your existing open/worksheet helper if it exists
    if "open_sheet" in globals():
        return globals()["open_sheet"](title)
    if "get_ws" in globals():
        return globals()["get_ws"](title)
    # Fallback: open via existing credentials vars in your app
    creds_json = os.environ.get("GOOGLE_CREDENTIALS_JSON","").strip()
    if not creds_json:
        raise RuntimeError("Missing GOOGLE_CREDENTIALS_JSON")
    info = json.loads(creds_json)
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    gc = gspread.authorize(creds)
    sh = gc.open(SHEET_NAME)
    try:
        return sh.worksheet(title)
    except Exception:
        return sh.add_worksheet(title=title, rows=1000, cols=20)

def _poll_get_config():
    cfg = _poll_defaults()
    try:
        ws = _poll_open_ws(POLL_CONFIG_WS_TITLE)
        rows = ws.get_all_values() or []
        if not rows:
            ws.update("A1", [["key","value"],["sponsor",cfg["sponsor"]],["match_id",""]])
            return cfg
        if len(rows[0]) < 2 or (rows[0][0] or "").lower() != "key":
            ws.update("A1", [["key","value"]])
            rows = ws.get_all_values() or []
        for r in rows[1:]:
            if len(r) >= 2:
                k = (r[0] or "").strip()
                v = (r[1] or "").strip()
                if k:
                    cfg[k] = v
        return cfg
    except Exception:
        data = _poll_local()
        return data.get("config") or cfg

def _poll_set_config(new_cfg: dict):
    cfg = _poll_get_config()
    cfg["sponsor"] = (new_cfg.get("sponsor") or cfg.get("sponsor") or "Fan Pick").strip() or "Fan Pick"
    cfg["match_id"] = (new_cfg.get("match_id") or "").strip()
    try:
        ws = _poll_open_ws(POLL_CONFIG_WS_TITLE)
        rows = ws.get_all_values() or []
        if not rows:
            ws.update("A1", [["key","value"],["sponsor",cfg["sponsor"]],["match_id",cfg["match_id"]]])
            return cfg
        index = {(r[0] or "").strip().lower(): i for i, r in enumerate(rows[1:], start=2) if len(r) >= 1}
        for k in ["sponsor","match_id"]:
            if k in index:
                ws.update(f"B{index[k]}", cfg[k])
            else:
                ws.append_row([k, cfg[k]])
        return cfg
    except Exception:
        data = _poll_local()
        data["config"] = cfg
        _poll_local_save(data)
        return cfg

def _poll_ensure_votes_ws():
    ws = _poll_open_ws(POLL_VOTES_WS_TITLE)
    rows = ws.get_all_values() or []
    if not rows:
        ws.update("A1", [["match_id","home","away","kickoff_utc","locked","winner","home_votes","away_votes","last_updated_utc"]])
    return ws

def _poll_percent(h: int, a: int):
    total = h + a
    if total <= 0:
        return 0, 0, 0
    hp = int(round((h/total)*100))
    ap = max(0, 100-hp)
    return hp, ap, total

def _poll_locked(kickoff_utc: str, locked_flag: str):
    if (locked_flag or "").strip().lower() in ["true","1","yes","y"]:
        return True
    if not kickoff_utc:
        return False
    try:
        dt = datetime.strptime(kickoff_utc, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        return datetime.now(timezone.utc) >= dt
    except Exception:
        return False

def _poll_get_record(match_id: str):
    match_id = (match_id or "").strip()
    if not match_id:
        raise ValueError("missing match_id")
    try:
        ws = _poll_ensure_votes_ws()
        rows = ws.get_all_values() or []
        header = rows[0] if rows else []
        idx = {}
        for i, r in enumerate(rows[1:], start=2):
            if len(r) >= 1:
                idx[(r[0] or "").strip()] = i
        rownum = idx.get(match_id)
        if not rownum:
            ws.append_row([match_id,"","","","false","","0","0",_now_iso_utc()], value_input_option="USER_ENTERED")
            rows = ws.get_all_values() or []
            for i, r in enumerate(rows[1:], start=2):
                if (r[0] or "").strip() == match_id:
                    rownum = i
                    break
        r = ws.row_values(rownum) if rownum else []
        def g(col, default=""):
            try:
                j = header.index(col)
                return (r[j] if j < len(r) else default).strip()
            except Exception:
                return default
        rec = {
            "_row": rownum,
            "match_id": match_id,
            "home": g("home",""),
            "away": g("away",""),
            "kickoff_utc": g("kickoff_utc",""),
            "locked": g("locked","false"),
            "winner": g("winner",""),
            "home_votes": int(g("home_votes","0") or 0),
            "away_votes": int(g("away_votes","0") or 0),
        }
        return rec, "sheet"
    except Exception:
        data = _poll_local()
        votes = data.get("votes") or {}
        rec = votes.get(match_id) or {
            "match_id": match_id, "home":"", "away":"", "kickoff_utc":"", "locked":"false", "winner":"",
            "home_votes": 0, "away_votes": 0
        }
        votes[match_id] = rec
        data["votes"] = votes
        _poll_local_save(data)
        return rec, "local"

def _poll_save_record(rec: dict, store: str):
    if store == "sheet":
        ws = _poll_ensure_votes_ws()
        rows = ws.get_all_values() or []
        header = rows[0]
        rownum = rec.get("_row")
        def set_cell(col, val):
            if col in header and rownum:
                ws.update_cell(rownum, header.index(col)+1, str(val))
        set_cell("home", rec.get("home",""))
        set_cell("away", rec.get("away",""))
        set_cell("kickoff_utc", rec.get("kickoff_utc",""))
        set_cell("locked", rec.get("locked","false"))
        set_cell("winner", rec.get("winner",""))
        set_cell("home_votes", rec.get("home_votes",0))
        set_cell("away_votes", rec.get("away_votes",0))
        set_cell("last_updated_utc", _now_iso_utc())
        return
    data = _poll_local()
    votes = data.get("votes") or {}
    mid = rec.get("match_id")
    votes[mid] = rec
    data["votes"] = votes
    _poll_local_save(data)

@app.route("/poll/match.json")
def poll_match_json():
    cfg = _poll_get_config()
    mid = (cfg.get("match_id") or "").strip()
    if not mid:
        return jsonify({"ok": False, "error": "No match configured"}), 200
    rec, store = _poll_get_record(mid)
    locked = _poll_locked(rec.get("kickoff_utc",""), rec.get("locked","false"))
    hp, ap, total = _poll_percent(int(rec.get("home_votes",0)), int(rec.get("away_votes",0)))
    return jsonify({
        "ok": True,
        "sponsor": cfg.get("sponsor","Fan Pick"),
        "match": {
            "id": mid,
            "home": rec.get("home",""),
            "away": rec.get("away",""),
            "kickoff_utc": rec.get("kickoff_utc",""),
            "locked": locked,
            "winner": rec.get("winner",""),
        },
        "totals": {"home": rec.get("home_votes",0), "away": rec.get("away_votes",0), "home_pct": hp, "away_pct": ap, "total": total},
    }), 200

@app.route("/poll/vote", methods=["POST"])
def poll_vote():
    payload = request.get_json(silent=True) or {}
    mid = (payload.get("match_id") or "").strip()
    team = (payload.get("team") or "").strip().lower()  # "home" / "away"
    voter = (payload.get("voter") or "").strip() or secrets.token_urlsafe(12)
    home = (payload.get("home") or "").strip()
    away = (payload.get("away") or "").strip()
    kickoff_utc = (payload.get("kickoff_utc") or "").strip()

    if not mid or team not in ["home","away"]:
        return jsonify({"ok": False, "error": "Bad request"}), 400

    # De-dupe per voter token in local store (works even if totals are in sheet)
    local = _poll_local()
    voters = local.get("voters") or {}
    prev = voters.get(voter)
    rec, store = _poll_get_record(mid)

    # Fill missing fields once
    if home and not rec.get("home"):
        rec["home"] = home
    if away and not rec.get("away"):
        rec["away"] = away
    if kickoff_utc and not rec.get("kickoff_utc"):
        rec["kickoff_utc"] = kickoff_utc

    locked = _poll_locked(rec.get("kickoff_utc",""), rec.get("locked","false"))
    if locked:
        hp, ap, total = _poll_percent(int(rec.get("home_votes",0)), int(rec.get("away_votes",0)))
        return jsonify({"ok": True, "locked": True, "voter": voter, "totals": {"home": rec.get("home_votes",0), "away": rec.get("away_votes",0), "home_pct": hp, "away_pct": ap, "total": total}, "winner": rec.get("winner","")}), 200

    if prev and prev.get("match_id") == mid:
        hp, ap, total = _poll_percent(int(rec.get("home_votes",0)), int(rec.get("away_votes",0)))
        return jsonify({"ok": True, "already_voted": True, "voter": voter, "totals": {"home": rec.get("home_votes",0), "away": rec.get("away_votes",0), "home_pct": hp, "away_pct": ap, "total": total}, "winner": rec.get("winner","")}), 200

    if team == "home":
        rec["home_votes"] = int(rec.get("home_votes",0)) + 1
    else:
        rec["away_votes"] = int(rec.get("away_votes",0)) + 1

    _poll_save_record(rec, store)

    voters[voter] = {"match_id": mid, "team": team, "ts": _now_iso_utc()}
    local["voters"] = voters
    _poll_local_save(local)

    hp, ap, total = _poll_percent(int(rec.get("home_votes",0)), int(rec.get("away_votes",0)))
    return jsonify({"ok": True, "voter": voter, "totals": {"home": rec.get("home_votes",0), "away": rec.get("away_votes",0), "home_pct": hp, "away_pct": ap, "total": total}}), 200

@app.route("/admin/poll/config", methods=["GET","POST"])
def admin_poll_config():
    key = request.args.get("key","")
    if not ADMIN_KEY or key != ADMIN_KEY:
        return "Unauthorized", 401
    if request.method == "POST":
        sponsor = (request.form.get("sponsor") or "").strip()
        match_id = (request.form.get("match_id") or "").strip()
        _poll_set_config({"sponsor": sponsor, "match_id": match_id})
        return "Saved. <a href='/admin?key=%s'>Back to Admin</a>" % key
    cfg = _poll_get_config()
    s = (cfg.get("sponsor") or "Fan Pick")
    m = (cfg.get("match_id") or "")
    return f"""<!doctype html><html><head><meta charset='utf-8'/><meta name='viewport' content='width=device-width,initial-scale=1'/>
    <title>Fan Poll Config</title>
    <style>
      body{font-family:Arial,system-ui;background:#0b1020;color:#eaf0ff;padding:18px}
      .box{max-width:760px;background:#0f1b33;border:1px solid rgba(255,255,255,.14);border-radius:16px;padding:14px}
      input{padding:10px;border-radius:10px;border:1px solid rgba(255,255,255,.2);background:#0b152c;color:#eaf0ff;width:100%;max-width:560px}
      button{padding:10px 14px;border-radius:10px;border:0;background:#d4af37;font-weight:900;color:#0b1020}
      a{color:#b9c7ee}
    </style></head><body>
      <div class='box'>
        <h2 style='margin:0 0 10px 0'>Fan Pick Settings</h2>
        <form method='post'>
          <label>Presented by (Sponsor label)</label><br/>
          <input name='sponsor' value='{s}'/><br/><br/>
          <label>Match of the Day (match_id)</label><br/>
          <input name='match_id' value='{m}' placeholder='Example: 2026-06-11-ATTS-ABC'/><br/>
          <div style='opacity:.75;font-size:13px;margin-top:8px'>Paste the match_id used by your schedule cards.</div><br/>
          <button type='submit'>Save</button>
        </form>
        <div style='margin-top:12px'><a href='/admin?key={key}'>← Back to Admin</a></div>
      </div>
    </body></html>"""



@app.route("/countries/qualified.json")
def qualified_countries_alias():
    # Alias for frontend fan-zone picker
    try:
        return worldcup_qualified()  # existing route handler in your app
    except Exception:
        # fallback: return empty list rather than 500
        return jsonify({"as_of": "", "countries": []}), 200

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5050))
    app.run(host="0.0.0.0", port=port, debug=False)
