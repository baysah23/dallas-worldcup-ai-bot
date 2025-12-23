from dotenv import load_dotenv
load_dotenv()

import os
import json
import re
import time
from datetime import datetime, date, timezone
from typing import Dict, Any, Optional, List, Tuple

from flask import Flask, request, jsonify, send_from_directory
from openai import OpenAI

import gspread
from google.oauth2.service_account import Credentials

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
        creds = Credentials.from_service_account_info(creds_info, scopes=SCOPES)
        return gspread.authorize(creds)

    creds_file = "google_creds.json"
    if os.path.exists(creds_file):
        creds = Credentials.from_service_account_file(creds_file, scopes=SCOPES)
        return gspread.authorize(creds)

    raise RuntimeError("Google credentials not found. Set GOOGLE_CREDS_JSON or provide google_creds.json locally.")



def _normalize_header(h: str) -> str:
    return (h or "").strip().lower().replace(" ", "_")


def ensure_sheet_schema(ws) -> List[str]:
    """
    Make sure row 1 is the header and includes the CRM columns we need.
    Returns the final header list (as stored in the sheet).
    """
    desired = ["timestamp", "name", "phone", "date", "time", "party_size", "language", "status", "vip"]

    existing = ws.row_values(1) or []
    existing_norm = [_normalize_header(x) for x in existing]

    # If sheet is empty, write the full header
    if not any(x.strip() for x in existing):
        ws.update("A1", [desired])
        return desired

    # If the existing header doesn't even contain "timestamp" (common sign row1 isn't a header),
    # don't try to reshuffle rows automatically—just ensure required columns exist at the end.
    header = existing[:]  # keep original display names
    header_norm = existing_norm[:]

    # Append missing columns
    for col in desired:
        if col not in header_norm:
            header.append(col)
            header_norm.append(col)

    # If we changed anything, write header back (row 1)
    if header != existing:
        ws.update("A1", [header])

    return header


def header_map(header: List[str]) -> Dict[str, int]:
    """Return {normalized_header: 1-based column_index}"""
    m = {}
    for i, h in enumerate(header):
        m[_normalize_header(h)] = i + 1
    return m


def append_lead_to_sheet(lead: Dict[str, Any]):
    gc = get_gspread_client()
    ws = gc.open(SHEET_NAME).sheet1

    header = ensure_sheet_schema(ws)
    hmap = header_map(header)

    # Defaults
    status = (lead.get("status") or "New").strip() or "New"
    vip = "Yes" if str(lead.get("vip") or "").strip().lower() in ["1", "true", "yes", "y"] else "No"

    row = [""] * len(header)

    def setv(key: str, val: Any):
        k = _normalize_header(key)
        if k in hmap:
            row[hmap[k] - 1] = val

    setv("timestamp", datetime.now().isoformat(timespec="seconds"))
    setv("name", lead.get("name", ""))
    setv("phone", lead.get("phone", ""))
    setv("date", lead.get("date", ""))
    setv("time", lead.get("time", ""))
    setv("party_size", int(lead.get("party_size") or 0))
    setv("language", lead.get("language", "en"))
    setv("status", status)
    setv("vip", vip)

    # Append at bottom (keeps headers at the top)
    ws.append_row(row, value_input_option="USER_ENTERED")


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

# ============================================================
# Admin dashboard (CRM-lite)
#   - inline Status dropdown + VIP toggle
#   - quick filters + metrics
#   - export CSV
# ============================================================
def _hesc(s: Any) -> str:
    s = "" if s is None else str(s)
    return (s.replace("&", "&amp;")
             .replace("<", "&lt;")
             .replace(">", "&gt;")
             .replace('"', "&quot;")
             .replace("'", "&#39;"))


@app.route("/admin/update-lead", methods=["POST"])
def admin_update_lead():
    key = request.args.get("key", "")
    if not ADMIN_KEY or key != ADMIN_KEY:
        return jsonify({"ok": False, "error": "Unauthorized"}), 401

    data = request.get_json(silent=True) or {}
    row_num = int(data.get("row") or 0)
    if row_num < 2:
        return jsonify({"ok": False, "error": "Bad row"}), 400

    status = (data.get("status") or "").strip()
    vip = (data.get("vip") or "").strip()

    allowed_status = ["New", "Confirmed", "Seated", "No-Show"]
    if status and status not in allowed_status:
        return jsonify({"ok": False, "error": "Invalid status"}), 400

    if vip:
        vip_norm = vip.lower()
        if vip_norm in ["true", "1", "yes", "y", "on"]:
            vip = "Yes"
        elif vip_norm in ["false", "0", "no", "n", "off"]:
            vip = "No"
        elif vip not in ["Yes", "No"]:
            return jsonify({"ok": False, "error": "Invalid vip"}), 400

    gc = get_gspread_client()
    ws = gc.open(SHEET_NAME).sheet1
    header = ensure_sheet_schema(ws)
    hmap = header_map(header)

    updates = 0
    if status:
        col = hmap.get("status")
        if col:
            ws.update_cell(row_num, col, status)
            updates += 1

    if vip:
        col = hmap.get("vip")
        if col:
            ws.update_cell(row_num, col, vip)
            updates += 1

    return jsonify({"ok": True, "updated": updates})


@app.route("/admin/export.csv")
def admin_export_csv():
    key = request.args.get("key", "")
    if not ADMIN_KEY or key != ADMIN_KEY:
        return "Unauthorized", 401

    gc = get_gspread_client()
    ws = gc.open(SHEET_NAME).sheet1
    rows = ws.get_all_values() or []
    if not rows:
        return "", 200, {"Content-Type": "text/csv; charset=utf-8"}

    # Basic CSV with escaping
    def csv_escape(x: str) -> str:
        x = "" if x is None else str(x)
        if any(c in x for c in [",", '"', "\n", "\r"]):
            return '"' + x.replace('"', '""') + '"'
        return x

    out_lines = []
    for r in rows:
        out_lines.append(",".join(csv_escape(c) for c in r))
    payload = "\n".join(out_lines)

    return payload, 200, {
        "Content-Type": "text/csv; charset=utf-8",
        "Content-Disposition": "attachment; filename=leads_export.csv",
        "Cache-Control": "no-store",
    }


@app.route("/admin")
def admin():
    key = request.args.get("key", "")
    if not ADMIN_KEY or key != ADMIN_KEY:
        return "Unauthorized", 401

    try:
        rows = read_leads(limit=600)
    except Exception as e:
        return f"Error reading leads: {repr(e)}", 500

    if not rows:
        return "<h2>No leads yet.</h2>"

    header = rows[0]
    body = rows[1:]

    # Build header indices (best effort even if header labels differ)
    hnorm = [_normalize_header(h) for h in header]
    def idx(name: str) -> int:
        n = _normalize_header(name)
        return hnorm.index(n) if n in hnorm else -1

    i_ts = idx("timestamp")
    i_name = idx("name")
    i_phone = idx("phone")
    i_date = idx("date")
    i_time = idx("time")
    i_party = idx("party_size")
    i_lang = idx("language")
    i_status = idx("status")
    i_vip = idx("vip")

    # Metrics
    def colval(r, i, default=""):
        return (r[i] if 0 <= i < len(r) else default).strip() if isinstance(r, list) else default

    status_counts = {"New": 0, "Confirmed": 0, "Seated": 0, "No-Show": 0}
    vip_count = 0
    for r in body:
        s = colval(r, i_status, "New") or "New"
        if s not in status_counts:
            status_counts[s] = 0
        status_counts[s] += 1
        if colval(r, i_vip, "No").lower() in ["yes", "true", "1", "y"]:
            vip_count += 1

    # Render newest first but keep correct sheet row numbers
    # body is oldest->newest, sheet rows start at 2
    numbered = [(i + 2, r) for i, r in enumerate(body)]
    numbered = list(reversed(numbered))

    html = []
    html.append("<!doctype html><html><head><meta charset='utf-8'><meta name='viewport' content='width=device-width, initial-scale=1'/>")
    html.append("<title>Leads Admin</title>")
    html.append("""
<style>
:root{--bg:#0b1020;--panel:#0f1b33;--line:rgba(255,255,255,.10);--text:#eaf0ff;--muted:#b9c7ee;--gold:#d4af37;--good:#2ea043;--warn:#ffcc66;--bad:#ff5d5d;}
body{margin:0;font-family:Arial,system-ui,sans-serif;background:radial-gradient(1200px 700px at 20% 10%, #142a5b 0%, var(--bg) 55%);color:var(--text);}
.wrap{max-width:1200px;margin:0 auto;padding:18px;}
.topbar{display:flex;align-items:center;justify-content:space-between;gap:12px;margin-bottom:14px;}
.h1{font-size:18px;font-weight:800;letter-spacing:.3px}
.sub{color:var(--muted);font-size:12px}
.pills{display:flex;gap:8px;flex-wrap:wrap}
.pill{border:1px solid var(--line);background:rgba(255,255,255,.03);padding:8px 10px;border-radius:999px;font-size:12px}
.pill b{color:var(--gold)}
.controls{display:flex;gap:10px;flex-wrap:wrap;margin:12px 0 14px}
.inp, select{background:rgba(255,255,255,.04);border:1px solid var(--line);color:var(--text);border-radius:10px;padding:9px 10px;font-size:12px;outline:none}
.btn{cursor:pointer;background:linear-gradient(180deg, rgba(212,175,55,.18), rgba(212,175,55,.06));border:1px solid rgba(212,175,55,.35);color:var(--text);border-radius:12px;padding:9px 12px;font-size:12px}
.btn:active{transform:translateY(1px)}
.tablewrap{border:1px solid var(--line);border-radius:16px;overflow:hidden;background:rgba(255,255,255,.03);box-shadow:0 10px 35px rgba(0,0,0,.35)}
table{border-collapse:collapse;width:100%}
thead th{position:sticky;top:0;background:rgba(10,16,34,.95);backdrop-filter: blur(6px);z-index:2}
th,td{border-bottom:1px solid var(--line);padding:10px 10px;font-size:12px;vertical-align:top}
th{color:var(--muted);font-weight:700;text-transform:uppercase;letter-spacing:.06em;font-size:11px}
tr:hover td{background:rgba(255,255,255,.03)}
.badge{display:inline-flex;align-items:center;gap:6px;border:1px solid var(--line);border-radius:999px;padding:4px 8px;font-size:11px}
.badge.vip{border-color:rgba(212,175,55,.5);box-shadow:0 0 0 1px rgba(212,175,55,.18) inset}
.dot{width:7px;height:7px;border-radius:999px;background:var(--muted);display:inline-block}
.dot.new{background:var(--warn)}
.dot.confirmed{background:var(--good)}
.dot.seated{background:#7aa7ff}
.dot.noshow{background:var(--bad)}
.small{font-size:11px;color:var(--muted)}
.right{text-align:right}
.tel{color:var(--text);text-decoration:none;border-bottom:1px dotted rgba(255,255,255,.25)}
.toast{position:fixed;right:14px;bottom:14px;background:rgba(0,0,0,.55);border:1px solid var(--line);padding:10px 12px;border-radius:12px;font-size:12px;display:none}
</style>
""")

    html.append("</head><body><div class='wrap'>")
    html.append("<div class='topbar'>")
    html.append(f"<div><div class='h1'>Leads Admin — {_hesc(SHEET_NAME)}</div><div class='sub'>Rows shown: {len(body)} • Key required • CRM-lite</div></div>")
    html.append("<div class='pills'>")
    html.append(f"<div class='pill'><b>VIP</b> {vip_count}</div>")
    html.append(f"<div class='pill'><b>New</b> {status_counts.get('New',0)}</div>")
    html.append(f"<div class='pill'><b>Confirmed</b> {status_counts.get('Confirmed',0)}</div>")
    html.append(f"<div class='pill'><b>Seated</b> {status_counts.get('Seated',0)}</div>")
    html.append(f"<div class='pill'><b>No‑Show</b> {status_counts.get('No-Show',0)}</div>")
    html.append("</div></div>")

    html.append("<div class='controls'>")
    html.append("<input class='inp' id='q' placeholder='Search name / phone / date / notes...' style='min-width:260px'/>")
    html.append("""
<select id="fStatus">
  <option value="">All Status</option>
  <option>New</option>
  <option>Confirmed</option>
  <option>Seated</option>
  <option>No-Show</option>
</select>
<label class="small" style="display:flex;align-items:center;gap:6px">
  <input type="checkbox" id="fVip"/> VIP only
</label>
""")
    html.append(f"<a class='btn' href='/admin/export.csv?key={_hesc(key)}'>Export CSV</a>")
    html.append(f"<button class='btn' onclick='location.reload()'>Refresh</button>")
    html.append("</div>")

    html.append("<div class='tablewrap'><table id='tbl'><thead><tr>")
    # Show a clean set of columns (not every raw column), but keep the raw order if indices missing
    cols = [
        ("Timestamp", i_ts),
        ("Name", i_name),
        ("Phone", i_phone),
        ("Date", i_date),
        ("Time", i_time),
        ("Party", i_party),
        ("Lang", i_lang),
        ("Status", i_status),
        ("VIP", i_vip),
        ("Actions", -999),
    ]
    for label, _ in cols:
        html.append(f"<th>{_hesc(label)}</th>")
    html.append("</tr></thead><tbody>")

    def status_dot(s: str) -> str:
        s2 = (s or "").lower()
        if s2 == "confirmed": return "confirmed"
        if s2 == "seated": return "seated"
        if s2 in ["no-show", "noshow", "no_show"]: return "noshow"
        return "new"

    for row_num, r in numbered:
        name = colval(r, i_name)
        phone = colval(r, i_phone)
        d = colval(r, i_date)
        t = colval(r, i_time)
        ps = colval(r, i_party)
        lang = colval(r, i_lang)
        status = colval(r, i_status, "New") or "New"
        vip = colval(r, i_vip, "No") or "No"
        ts = colval(r, i_ts)

        is_vip = vip.lower() in ["yes", "true", "1", "y"]
        badge = f"<span class='badge{' vip' if is_vip else ''}'><span class='dot {status_dot(status)}'></span>{_hesc(status)}{' • VIP' if is_vip else ''}</span>"

        html.append(f"<tr data-row='{row_num}' data-status='{_hesc(status)}' data-vip='{ '1' if is_vip else '0' }'>")
        html.append(f"<td class='small'>{_hesc(ts)}</td>")
        html.append(f"<td><b>{_hesc(name)}</b></td>")
        html.append(f"<td><a class='tel' href='tel:{_hesc(phone)}'>{_hesc(phone)}</a></td>")
        html.append(f"<td>{_hesc(d)}</td>")
        html.append(f"<td>{_hesc(t)}</td>")
        html.append(f"<td class='right'>{_hesc(ps)}</td>")
        html.append(f"<td class='right'>{_hesc(lang)}</td>")

        # Status dropdown
        html.append("<td>")
        html.append(f"{badge}<div style='height:6px'></div>")
        html.append(f"""
<select onchange="updateLead({row_num}, this.value, null)">
  <option {'selected' if status=='New' else ''}>New</option>
  <option {'selected' if status=='Confirmed' else ''}>Confirmed</option>
  <option {'selected' if status=='Seated' else ''}>Seated</option>
  <option {'selected' if status=='No-Show' else ''}>No-Show</option>
</select>
""")
        html.append("</td>")

        # VIP toggle
        checked = "checked" if is_vip else ""
        html.append("<td class='right'>")
        html.append(f"<label class='small' style='display:inline-flex;align-items:center;gap:6px;justify-content:flex-end'><input type='checkbox' {checked} onchange=\"updateLead({row_num}, null, this.checked)\"/> VIP</label>")
        html.append("</td>")

        # Quick actions
        html.append("<td class='right'>")
        html.append(f"<button class='btn' style='padding:7px 10px' onclick=\"updateLead({row_num}, 'Confirmed', true)\">Confirm</button> ")
        html.append(f"<button class='btn' style='padding:7px 10px' onclick=\"updateLead({row_num}, 'Seated', true)\">Seat</button>")
        html.append("</td>")

        html.append("</tr>")

    html.append("</tbody></table></div>")
    html.append("<div class='toast' id='toast'></div>")

    html.append("""
<script>
const ADMIN_KEY = __ADMIN_KEY__;
function toast(msg){
  const t = document.getElementById('toast');
  t.textContent = msg;
  t.style.display = 'block';
  clearTimeout(window.__toastTimer);
  window.__toastTimer = setTimeout(()=>t.style.display='none', 1400);
}

async function updateLead(row, status, vipBool){
  const payload = {row: row};
  if(status !== null){ payload.status = status; }
  if(vipBool !== null){
    payload.vip = vipBool ? "Yes" : "No";
  }
  try{
    const res = await fetch(`/admin/update-lead?key=${encodeURIComponent(ADMIN_KEY)}`, {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(payload)
    });
    const data = await res.json();
    if(!data.ok) throw new Error(data.error || 'Update failed');
    toast('Saved ✓');
    // Update row dataset for filtering
    const tr = document.querySelector(`tr[data-row="${row}"]`);
    if(tr){
      if(payload.status){ tr.dataset.status = payload.status; }
      if(payload.vip !== undefined){ tr.dataset.vip = payload.vip === "Yes" ? "1" : "0"; }
    }
  }catch(err){
    console.error(err);
    toast('Error: ' + err.message);
  }
}

// Filters
const qEl = document.getElementById('q');
const fStatus = document.getElementById('fStatus');
const fVip = document.getElementById('fVip');

function applyFilters(){
  const q = (qEl.value || '').toLowerCase().trim();
  const st = (fStatus.value || '').trim();
  const vipOnly = fVip.checked;

  const rows = Array.from(document.querySelectorAll('#tbl tbody tr'));
  for(const tr of rows){
    const text = tr.innerText.toLowerCase();
    const okQ = !q || text.includes(q);
    const okS = !st || (tr.dataset.status === st);
    const okV = !vipOnly || (tr.dataset.vip === "1");
    tr.style.display = (okQ && okS && okV) ? '' : 'none';
  }
}

qEl.addEventListener('input', applyFilters);
fStatus.addEventListener('change', applyFilters);
fVip.addEventListener('change', applyFilters);
</script>
""".replace("__ADMIN_KEY__", json.dumps(key)))


    html.append("</div></body></html>")
    return "".join(html)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5050))
    app.run(host="0.0.0.0", port=port, debug=False)
