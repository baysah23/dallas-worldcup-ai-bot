from dotenv import load_dotenv
load_dotenv()

import os
import json
import hashlib
import re
import time
import datetime
from datetime import datetime, date, timezone, timedelta
from typing import Dict, Any, Optional, List, Tuple

from flask import Flask, request, jsonify, send_from_directory, send_file, make_response

# OpenAI client (compat: works with both newer and older openai python packages)
try:
    from openai import OpenAI  # new SDK
    client = OpenAI()
    _OPENAI_MODE = "new"
except Exception:
    import openai  # legacy SDK
    _OPENAI_MODE = "legacy"

    class _CompatResponses:
        @staticmethod
        def create(model: str, input):
            messages = input
            r = openai.ChatCompletion.create(model=model, messages=messages)
            class _Resp: pass
            resp = _Resp()
            resp.output_text = (r["choices"][0]["message"]["content"] or "")
            return resp

    class _CompatClient:
        responses = _CompatResponses()

    client = _CompatClient()
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
    # === PHASE 2: SMART CACHE HEADERS (AUTO-INSERTED) ===
    # Keep the app feeling fast and consistent on mobile by allowing short caching
    # for read-only JSON, while still preventing stale admin/chat experiences.
    try:
        path = request.path or ""
    except Exception:
        path = ""

    # Never cache: HTML shell + admin/chat actions
    if path == "/" or path.startswith("/admin") or path.startswith("/chat"):
        response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"
        # === PHASE 6: JSON ETAG (AUTO-INSERTED) ===
        try:
            # Add weak ETag for JSON to improve revalidation on mobile
            ctype = response.headers.get("Content-Type","")
            if (getattr(response, "mimetype", "") == "application/json") or ctype.startswith("application/json"):
                payload = response.get_data()
                etag = hashlib.sha1(payload).hexdigest()
                response.set_etag(etag, weak=True)
                response.headers.setdefault("Cache-Control", "public, max-age=60")
        except Exception:
            pass

        return response

    # Short cache for JSON (schedule/menu/qualified lists). Helps flaky mobile networks.
    if path.endswith(".json") or path.startswith("/api/") or path.startswith("/countries/") or path.startswith("/worldcup/"):
        response.headers["Cache-Control"] = "public, max-age=60"  # 1 minute
        return response

    # Default: allow browser heuristics
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
# Business Profile (optional)
# ============================================================
# Old versions required business_profile.txt. For portability (Render + local),
# we treat it as optional and fall back to an env var or a small default profile.
BUSINESS_PROFILE_PATH = "business_profile.txt"
BUSINESS_PROFILE = os.environ.get("BUSINESS_PROFILE", "").strip()
if not BUSINESS_PROFILE and os.path.exists(BUSINESS_PROFILE_PATH):
    try:
        with open(BUSINESS_PROFILE_PATH, "r", encoding="utf-8") as f:
            BUSINESS_PROFILE = f.read().strip()
    except Exception:
        BUSINESS_PROFILE = ""
if not BUSINESS_PROFILE:
    BUSINESS_PROFILE = "You are World Cup Concierge — a premium reservation assistant for World Cup fans. Keep replies concise, helpful, and action-oriented."

# ============================================================
# Business Rules (edit here)
# ============================================================
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

# If the remote feed is empty/unavailable (e.g., schedule not published yet),
# we serve a small premium "demo" dataset so the Schedule UI never goes blank.
# As soon as the feed returns real matches, the app automatically switches to it.
DEMO_FIXTURES_RAW: List[Dict[str, Any]] = [
    {
        "MatchNumber": 1,
        "RoundNumber": 1,
        "DateUtc": "2026-06-11 19:00:00Z",
        "Location": "Dallas Stadium",
        "HomeTeam": "United States",
        "AwayTeam": "Mexico",
        "Group": "Group A",
        "HomeTeamScore": None,
        "AwayTeamScore": None,
        "Status": "Scheduled",
    },
    {
        "MatchNumber": 2,
        "RoundNumber": 1,
        "DateUtc": "2026-06-12 23:00:00Z",
        "Location": "Dallas Stadium",
        "HomeTeam": "Canada",
        "AwayTeam": "Japan",
        "Group": "Group A",
        "HomeTeamScore": None,
        "AwayTeamScore": None,
        "Status": "Scheduled",
    },
    {
        "MatchNumber": 3,
        "RoundNumber": 1,
        "DateUtc": "2026-06-13 02:00:00Z",
        "Location": "Mexico City Stadium",
        "HomeTeam": "Mexico",
        "AwayTeam": "Spain",
        "Group": "Group A",
        "HomeTeamScore": None,
        "AwayTeamScore": None,
        "Status": "Scheduled",
    },
    {
        "MatchNumber": 4,
        "RoundNumber": 1,
        "DateUtc": "2026-06-13 19:00:00Z",
        "Location": "Dallas Stadium",
        "HomeTeam": "France",
        "AwayTeam": "Brazil",
        "Group": "Group B",
        "HomeTeamScore": None,
        "AwayTeamScore": None,
        "Status": "Scheduled",
    },
]

# In-memory cache (plus optional disk cache) so we don't hit the feed too often.
_fixtures_cache: Dict[str, Any] = {"loaded_at": 0, "matches": [], "source": "empty", "last_error": None}
FIXTURE_CACHE_SECONDS = int(os.environ.get("FIXTURE_CACHE_SECONDS", str(6 * 60 * 60)))  # 6h
FIXTURE_CACHE_FILE = os.environ.get("FIXTURE_CACHE_FILE", "/tmp/wc26_fixtures.json")
POLL_STORE_FILE = os.environ.get("POLL_STORE_FILE", "/tmp/wc26_poll_votes.json")


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
        # Network can be slow/unreliable on some hosts. Use a safer timeout + small retry.
    last_err = None
    for _attempt in range(3):
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                raw = resp.read().decode("utf-8", errors="replace")
            last_err = None
            break
        except Exception as _e:
            last_err = _e
    if last_err is not None:
        raise last_err

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

    # Fetch fresh (but fall back to any cache if the network times out)
    raw_matches: List[Dict[str, Any]] = []
    try:
        raw_matches = _fetch_fixture_feed()
        _fixtures_cache["last_error"] = None
        _fixtures_cache["source"] = "remote"
    except Exception as e:
        _fixtures_cache["last_error"] = repr(e)
        # If we have ANY disk cache (even stale), use it instead of breaking the UI
        disk2 = _safe_read_json_file(FIXTURE_CACHE_FILE)
        if disk2 and isinstance(disk2, dict) and isinstance(disk2.get("matches"), list) and disk2["matches"]:
            _fixtures_cache["source"] = "disk-stale"
            return disk2["matches"]
        # If we have ANY in-memory cache, use it.
        if _fixtures_cache.get("matches"):
            _fixtures_cache["source"] = "mem-stale"
            return _fixtures_cache["matches"]
        # Absolute fallback: demo dataset so Schedule never goes blank.
        raw_matches = list(DEMO_FIXTURES_RAW)
        _fixtures_cache["source"] = "demo"

    # If the feed responded but returned no matches, fall back gracefully.
    if not raw_matches:
        disk2 = _safe_read_json_file(FIXTURE_CACHE_FILE)
        if disk2 and isinstance(disk2, dict) and isinstance(disk2.get("matches"), list) and disk2["matches"]:
            _fixtures_cache["source"] = "disk-stale"
            return disk2["matches"]
        raw_matches = list(DEMO_FIXTURES_RAW)
        _fixtures_cache["source"] = "demo"
    norm: List[Dict[str, Any]] = []
    for m in raw_matches:
        dt = _parse_dateutc(m.get("DateUtc") or "")
        if not dt:
            continue

        match_num = int(m.get("MatchNumber") or 0) or None
        match_id = f"wc-{match_num:03d}" if match_num else f"wc-{len(norm)+1:03d}"

                # Scores (best-effort; feed includes finals once matches are completed)
        def _to_int(x):
            try:
                if x is None or x == "":
                    return None
                return int(float(str(x).strip()))
            except Exception:
                return None

        hs = _to_int(m.get("HomeTeamScore") if "HomeTeamScore" in m else m.get("HomeScore"))
        as_ = _to_int(m.get("AwayTeamScore") if "AwayTeamScore" in m else m.get("AwayScore"))

        # Status (UI hints; true "live" requires a live data provider)
        nowu = datetime.now(timezone.utc)
        kickoff = dt.replace(tzinfo=timezone.utc)
        status = "upcoming"
        if nowu >= kickoff and nowu <= (kickoff + timedelta(hours=2, minutes=30)):
            status = "live"
        if nowu > (kickoff + timedelta(hours=2, minutes=30)) and (hs is not None or as_ is not None):
            status = "finished"

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
            "home_score": hs,
            "away_score": as_,
            "status": status,
        })
# Sort by kickoff UTC
    norm.sort(key=lambda x: x.get("datetime_utc") or "")
    _fixtures_cache["loaded_at"] = now
    _fixtures_cache["matches"] = norm
    if _fixtures_cache.get("source") not in ("demo", "disk-stale", "mem-stale"):
        _fixtures_cache["source"] = "remote"
    if _fixtures_cache.get("source") == "remote":
        _fixtures_cache["last_error"] = None
    _safe_write_json_file(FIXTURE_CACHE_FILE, {"loaded_at": now, "matches": norm})

    return norm


def is_dallas_match(m: Dict[str, Any]) -> bool:
    v = (m.get("venue") or "").lower()
    return any(k in v for k in DALLAS_LOCATION_KEYWORDS)


def filter_matches(scope: str, q: str = "") -> List[Dict[str, Any]]:
    scope = (scope or "all").lower().strip()
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
            {
                "category_id": "chef",
                "name": "Chef’s Wagyu Sliders",
                "price": "$24",
                "desc": "A5-style sear, truffle aioli, brioche. Limited matchday batch.",
                "tag": "Chef Special"
            },
            {
                "category_id": "chef",
                "name": "Citrus Ceviche Bowl",
                "price": "$19",
                "desc": "Fresh catch, lime, chili, avocado, crunchy tostadas.",
                "tag": "Chef Special"
            },
            {
                "category_id": "bites",
                "name": "Stadium Nachos XL",
                "price": "$16",
                "desc": "Three-cheese blend, jalapeño, pico, crema, choice of protein.",
                "tag": "Share"
            },
            {
                "category_id": "bites",
                "name": "Peri-Peri Wings (8/16)",
                "price": "$14/$24",
                "desc": "Crispy wings, peri-peri glaze, citrus salt.",
                "tag": "Hot"
            },
            {
                "category_id": "classics",
                "name": "Concierge Burger",
                "price": "$18",
                "desc": "Angus, cheddar, lettuce, tomato, house sauce, fries.",
                "tag": "Classic"
            },
            {
                "category_id": "classics",
                "name": "Spicy Chicken Sandwich",
                "price": "$16",
                "desc": "Crispy chicken, spicy sauce, pickles, fries optional.",
                "tag": "Fan Favorite"
            },
            {
                "category_id": "sweets",
                "name": "Gold Medal Churros",
                "price": "$10",
                "desc": "Cinnamon sugar, chocolate dip.",
                "tag": "Sweet"
            },
            {
                "category_id": "drinks",
                "name": "Matchday Mocktail",
                "price": "$9",
                "desc": "Citrus, mint, sparkling finish.",
                "tag": "Zero Proof"
            },
            {
                "category_id": "drinks",
                "name": "Premium Espresso",
                "price": "$5",
                "desc": "Double shot, smooth crema.",
                "tag": "Coffee"
            }
        ]
    },
    "es": {
        "title": "Menú",
        "items": [
            {
                "category_id": "chef",
                "name": "Mini hamburguesas Wagyu del Chef",
                "price": "$24",
                "desc": "Sellado estilo A5, alioli de trufa, brioche. Lote limitado.",
                "tag": "Especial del Chef"
            },
            {
                "category_id": "chef",
                "name": "Bowl de Ceviche Cítrico",
                "price": "$19",
                "desc": "Pesca fresca, lima, chile, aguacate, tostadas.",
                "tag": "Especial del Chef"
            },
            {
                "category_id": "bites",
                "name": "Nachos XL del Estadio",
                "price": "$16",
                "desc": "Tres quesos, jalapeño, pico, crema, proteína a elección.",
                "tag": "Para compartir"
            },
            {
                "category_id": "bites",
                "name": "Alitas Peri-Peri (8/16)",
                "price": "$14/$24",
                "desc": "Alitas crujientes, glaseado peri-peri, sal cítrica.",
                "tag": "Picante"
            },
            {
                "category_id": "classics",
                "name": "Hamburguesa Concierge",
                "price": "$18",
                "desc": "Angus, cheddar, lechuga, tomate, salsa de la casa, papas.",
                "tag": "Clásico"
            },
            {
                "category_id": "classics",
                "name": "Sándwich de Pollo Picante",
                "price": "$16",
                "desc": "Pollo crujiente, salsa picante, pepinillos, papas opcionales.",
                "tag": "Favorito"
            },
            {
                "category_id": "sweets",
                "name": "Churros Medalla de Oro",
                "price": "$10",
                "desc": "Azúcar y canela, dip de chocolate.",
                "tag": "Dulce"
            },
            {
                "category_id": "drinks",
                "name": "Mocktail de Partido",
                "price": "$9",
                "desc": "Cítricos, menta, final espumoso.",
                "tag": "Sin alcohol"
            },
            {
                "category_id": "drinks",
                "name": "Espresso Premium",
                "price": "$5",
                "desc": "Doble shot, crema suave.",
                "tag": "Café"
            }
        ]
    },
    "pt": {
        "title": "Cardápio",
        "items": [
            {
                "category_id": "chef",
                "name": "Mini Burgers Wagyu do Chef",
                "price": "$24",
                "desc": "Selagem estilo A5, aioli de trufa, brioche. Lote limitado.",
                "tag": "Especial do Chef"
            },
            {
                "category_id": "chef",
                "name": "Bowl de Ceviche Cítrico",
                "price": "$19",
                "desc": "Peixe fresco, limão, pimenta, abacate, tostadas.",
                "tag": "Especial do Chef"
            },
            {
                "category_id": "bites",
                "name": "Nachos XL do Estádio",
                "price": "$16",
                "desc": "Três queijos, jalapeño, pico, creme, proteína à escolha.",
                "tag": "Compartilhar"
            },
            {
                "category_id": "bites",
                "name": "Asinhas Peri-Peri (8/16)",
                "price": "$14/$24",
                "desc": "Asinhas crocantes, glaze peri-peri, sal cítrico.",
                "tag": "Picante"
            },
            {
                "category_id": "classics",
                "name": "Burger Concierge",
                "price": "$18",
                "desc": "Angus, cheddar, alface, tomate, molho da casa, fritas.",
                "tag": "Clássico"
            },
            {
                "category_id": "classics",
                "name": "Sanduíche de Frango Picante",
                "price": "$16",
                "desc": "Frango crocante, molho picante, picles, fritas opcionais.",
                "tag": "Favorito"
            },
            {
                "category_id": "sweets",
                "name": "Churros Medalha de Ouro",
                "price": "$10",
                "desc": "Canela e açúcar, molho de chocolate.",
                "tag": "Doce"
            },
            {
                "category_id": "drinks",
                "name": "Mocktail de Jogo",
                "price": "$9",
                "desc": "Cítricos, hortelã, final com gás.",
                "tag": "Sem álcool"
            },
            {
                "category_id": "drinks",
                "name": "Espresso Premium",
                "price": "$5",
                "desc": "Dose dupla, crema suave.",
                "tag": "Café"
            }
        ]
    },
    "fr": {
        "title": "Menu",
        "items": [
            {
                "category_id": "chef",
                "name": "Mini-burgers Wagyu du Chef",
                "price": "$24",
                "desc": "Saisie style A5, aïoli à la truffe, brioche. Série limitée.",
                "tag": "Spécialité du Chef"
            },
            {
                "category_id": "chef",
                "name": "Bol de Ceviche aux Agrumes",
                "price": "$19",
                "desc": "Poisson frais, citron vert, piment, avocat, tostadas.",
                "tag": "Spécialité du Chef"
            },
            {
                "category_id": "bites",
                "name": "Nachos XL du Stade",
                "price": "$16",
                "desc": "Trois fromages, jalapeño, pico, crème, protéine au choix.",
                "tag": "À partager"
            },
            {
                "category_id": "bites",
                "name": "Ailes Peri-Peri (8/16)",
                "price": "$14/$24",
                "desc": "Ailes croustillantes, glaçage peri-peri, sel aux agrumes.",
                "tag": "Épicé"
            },
            {
                "category_id": "classics",
                "name": "Burger Concierge",
                "price": "$18",
                "desc": "Angus, cheddar, salade, tomate, sauce maison, frites.",
                "tag": "Classique"
            },
            {
                "category_id": "classics",
                "name": "Sandwich Poulet Épicé",
                "price": "$16",
                "desc": "Poulet croustillant, sauce épicée, pickles, frites en option.",
                "tag": "Favori"
            },
            {
                "category_id": "sweets",
                "name": "Churros Médaille d’Or",
                "price": "$10",
                "desc": "Cannelle-sucre, sauce chocolat.",
                "tag": "Sucré"
            },
            {
                "category_id": "drinks",
                "name": "Mocktail de Match",
                "price": "$9",
                "desc": "Agrumes, menthe, touche pétillante.",
                "tag": "Sans alcool"
            },
            {
                "category_id": "drinks",
                "name": "Espresso Premium",
                "price": "$5",
                "desc": "Double, crème onctueuse.",
                "tag": "Café"
            }
        ]
    }
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
    """Read leads from Google Sheets with a small cache.

    Switching between Admin tabs can trigger repeated reads; caching avoids
    Sheets 429 quota errors. If Sheets errors, we fall back to cached rows.
    """
    now = time.time()

    rows_cached = _LEADS_CACHE.get("rows")
    if isinstance(rows_cached, list) and (now - float(_LEADS_CACHE.get("ts", 0.0)) < 90.0):
        if not rows_cached:
            return []
        header = rows_cached[0]
        body = rows_cached[1:]
        body = body[-limit:]
        return [header] + body

    try:
        gc = get_gspread_client()
        ws = gc.open(SHEET_NAME).sheet1
        rows = ws.get_all_values() or []
        _LEADS_CACHE["ts"] = now
        _LEADS_CACHE["rows"] = rows

        if not rows:
            return []
        header = rows[0]
        body = rows[1:]
        body = body[-limit:]
        return [header] + body
    except Exception:
        rows = _LEADS_CACHE.get("rows") or []
        if rows:
            header = rows[0]
            body = rows[1:]
            body = body[-limit:]
            return [header] + body
        return []

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
            # status/vip are CRM fields shown in /admin.
            "lead": {
                "name": "",
                "phone": "",
                "date": "",
                "time": "",
                "party_size": 0,
                "language": "en",
                "status": "New",
                "vip": "No",
            },
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


    # Don't treat VIP intents/buttons as names
    if (lower == 'vip' or lower.startswith('vip ') or 'vip table' in lower or 'vip hold' in lower) and ('reservation' in lower or 'reserve' in lower or 'table' in lower or 'hold' in lower or lower == 'vip'):
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
    # Prevent stale caching so deploys always serve the latest index.html
    resp = make_response(send_from_directory(".", "index.html"))
    resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    resp.headers["Pragma"] = "no-cache"
    resp.headers["Expires"] = "0"
    return resp



@app.route("/<path:path>")
def catch_all(path):
    # Serve static files if they exist; otherwise serve the SPA shell.
    try:
        if os.path.exists(path) and os.path.isfile(path):
            return send_from_directory(".", path)
        # allow /static/...
        if path.startswith("static/") and os.path.exists(path):
            return send_from_directory(".", path)
    except Exception:
        pass
    return home()

@app.route("/health")
def health():
    return jsonify({"status": "ok"})





@app.route("/menu.json")
def menu_json():
    lang = norm_lang(request.args.get("lang", "en"))
    return jsonify({"lang": lang, "menu": MENU[lang]})



# ============================================================
# Fan Zone (public demo JSON for the UI)
# ============================================================
FANZONE_DEMO = {
    "en": [
        {"date": "2026-06-11", "city": "Host City", "title": "Official Fan Festival", "location": "City Center", "description": "Live screenings, music, and food."},
        {"date": "2026-06-12", "city": "Host City", "title": "Watch Party Night", "location": "Partner Venue", "description": "Reservations recommended."},
    ],
    "es": [
        {"date": "2026-06-11", "city": "Ciudad Sede", "title": "Festival Oficial de Aficionados", "location": "Centro", "description": "Pantallas, música y comida."},
        {"date": "2026-06-12", "city": "Ciudad Sede", "title": "Noche de Partido", "location": "Lugar Asociado", "description": "Se recomienda reservar."},
    ],
    "pt": [
        {"date": "2026-06-11", "city": "Cidade-Sede", "title": "Festival Oficial do Torcedor", "location": "Centro", "description": "Transmissão ao vivo, música e comida."},
        {"date": "2026-06-12", "city": "Cidade-Sede", "title": "Noite de Jogo", "location": "Local Parceiro", "description": "Reservas recomendadas."},
    ],
    "fr": [
        {"date": "2026-06-11", "city": "Ville Hôte", "title": "Festival Officiel des Fans", "location": "Centre-ville", "description": "Diffusion live, musique et food."},
        {"date": "2026-06-12", "city": "Ville Hôte", "title": "Soirée Match", "location": "Lieu Partenaire", "description": "Réservation conseillée."},
    ],
}

def norm_lang(lang: str) -> str:
    lang = (lang or "en").lower().strip()
    return lang if lang in ("en","es","pt","fr") else "en"

@app.route("/fanzone.json")
def fanzone_json():
    lang = norm_lang(request.args.get("lang"))
    return jsonify({"lang": lang, "events": FANZONE_DEMO.get(lang, FANZONE_DEMO["en"])})

@app.route("/schedule.json")
def schedule_json():
    """
    Query params:
      scope= dallas | all   (default: all)
      q= search text (team, venue, group, date)
    """
    scope = "all"  # Global schedule only (Dallas-only removed)
    q = request.args.get("q") or ""

    try:
        matches = filter_matches(scope=scope, q=q)

        today = datetime.now().date()
        # Match-day indicator for global view: any match today.
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




# ============================================================
# Qualified teams (World Cup 2026) — server-side fetch
# - Returns the teams that have qualified *so far* (qualification is ongoing).
# - Source: Wikipedia qualified teams table (updates over time).
# ============================================================
# --- Default country list (no external deps) ---
DEFAULT_COUNTRY_LIST = [
  "United States",
  "Canada",
  "Mexico",
  "Afghanistan",
  "Aland Islands",
  "Albania",
  "Algeria",
  "American Samoa",
  "Andorra",
  "Angola",
  "Anguilla",
  "Antarctica",
  "Antigua and Barbuda",
  "Argentina",
  "Armenia",
  "Aruba",
  "Australia",
  "Austria",
  "Azerbaijan",
  "Bahamas",
  "Bahrain",
  "Bangladesh",
  "Barbados",
  "Belarus",
  "Belgium",
  "Belize",
  "Benin",
  "Bermuda",
  "Bhutan",
  "Bolivia",
  "Bonaire, Sint Eustatius and Saba",
  "Bosnia and Herzegovina",
  "Botswana",
  "Bouvet Island",
  "Brazil",
  "British Indian Ocean Territory",
  "Brunei",
  "Bulgaria",
  "Burkina Faso",
  "Burundi",
  "Cambodia",
  "Cameroon",
  "Cape Verde",
  "Cayman Islands",
  "Central African Republic",
  "Chad",
  "Chile",
  "China",
  "Christmas Island",
  "Cocos (Keeling) Islands",
  "Colombia",
  "Comoros",
  "Congo",
  "Cook Islands",
  "Costa Rica",
  "Croatia",
  "Cuba",
  "Curaçao",
  "Cyprus",
  "Czech Republic",
  "Côte d'Ivoire",
  "Denmark",
  "Djibouti",
  "Dominica",
  "Dominican Republic",
  "DR Congo",
  "East Timor",
  "Ecuador",
  "Egypt",
  "El Salvador",
  "Equatorial Guinea",
  "Eritrea",
  "Estonia",
  "Ethiopia",
  "Falkland Islands (Malvinas)",
  "Faroe Islands",
  "Fiji",
  "Finland",
  "France",
  "French Guiana",
  "French Polynesia",
  "French Southern Territories",
  "Gabon",
  "Gambia",
  "Georgia",
  "Germany",
  "Ghana",
  "Gibraltar",
  "Greece",
  "Greenland",
  "Grenada",
  "Guadeloupe",
  "Guam",
  "Guatemala",
  "Guernsey",
  "Guinea",
  "Guinea-Bissau",
  "Guyana",
  "Haiti",
  "Heard Island and McDonald Islands",
  "Holy See (Vatican City State)",
  "Honduras",
  "Hong Kong",
  "Hungary",
  "Iceland",
  "India",
  "Indonesia",
  "Iran",
  "Iraq",
  "Ireland",
  "Isle of Man",
  "Israel",
  "Italy",
  "Jamaica",
  "Japan",
  "Jersey",
  "Jordan",
  "Kazakhstan",
  "Kenya",
  "Kiribati",
  "Kuwait",
  "Kyrgyzstan",
  "Laos",
  "Latvia",
  "Lebanon",
  "Lesotho",
  "Liberia",
  "Libya",
  "Liechtenstein",
  "Lithuania",
  "Luxembourg",
  "Macao",
  "Madagascar",
  "Malawi",
  "Malaysia",
  "Maldives",
  "Mali",
  "Malta",
  "Marshall Islands",
  "Martinique",
  "Mauritania",
  "Mauritius",
  "Mayotte",
  "Micronesia",
  "Moldova",
  "Monaco",
  "Mongolia",
  "Montenegro",
  "Montserrat",
  "Morocco",
  "Mozambique",
  "Myanmar",
  "Namibia",
  "Nauru",
  "Nepal",
  "Netherlands",
  "New Caledonia",
  "New Zealand",
  "Nicaragua",
  "Niger",
  "Nigeria",
  "Niue",
  "Norfolk Island",
  "North Korea",
  "North Macedonia",
  "Northern Mariana Islands",
  "Norway",
  "Oman",
  "Pakistan",
  "Palau",
  "Palestine",
  "Panama",
  "Papua New Guinea",
  "Paraguay",
  "Peru",
  "Philippines",
  "Pitcairn",
  "Poland",
  "Portugal",
  "Puerto Rico",
  "Qatar",
  "Reunion",
  "Romania",
  "Russia",
  "Rwanda",
  "Saint Barthélemy",
  "Saint Helena, Ascension and Tristan da Cunha",
  "Saint Kitts and Nevis",
  "Saint Lucia",
  "Saint Martin",
  "Saint Pierre and Miquelon",
  "Saint Vincent and the Grenadines",
  "Samoa",
  "San Marino",
  "Sao Tome and Principe",
  "Saudi Arabia",
  "Senegal",
  "Serbia",
  "Seychelles",
  "Sierra Leone",
  "Singapore",
  "Sint Maarten (Dutch part)",
  "Slovakia",
  "Slovenia",
  "Solomon Islands",
  "Somalia",
  "South Africa",
  "South Georgia and the South Sandwich Islands",
  "South Korea",
  "South Sudan",
  "Spain",
  "Sri Lanka",
  "Sudan",
  "Suriname",
  "Svalbard and Jan Mayen",
  "Swaziland",
  "Sweden",
  "Switzerland",
  "Syria",
  "Taiwan",
  "Tajikistan",
  "Tanzania",
  "Thailand",
  "Togo",
  "Tokelau",
  "Tonga",
  "Trinidad and Tobago",
  "Tunisia",
  "Turkey",
  "Turkmenistan",
  "Turks and Caicos Islands",
  "Tuvalu",
  "Uganda",
  "Ukraine",
  "United Arab Emirates",
  "United Kingdom",
  "United States Minor Outlying Islands",
  "Uruguay",
  "Uzbekistan",
  "Vanuatu",
  "Venezuela",
  "Vietnam",
  "Virgin Islands, British",
  "Virgin Islands, U.S.",
  "Wallis and Futuna",
  "Western Sahara",
  "Yemen",
  "Zambia",
  "Zimbabwe"
]


_qualified_cache: Dict[str, Any] = {"loaded_at": 0, "teams": []}

# NOTE:
# The full 48-team field for the 2026 World Cup is not known until qualification completes.
# For the Fan Zone country selector we want a fast, reliable list that never blocks the UI.
# We therefore default to an "eligible countries" list derived from pycountry (local data),
# with hosts pinned to the top. If you want to switch back to a remote "qualified so far"
# source, set USE_REMOTE_QUALIFIED=1 and provide QUALIFIED_SOURCE_URL.
USE_REMOTE_QUALIFIED = os.environ.get("USE_REMOTE_QUALIFIED", "1") == "1"
QUALIFIED_CACHE_SECONDS = int(os.environ.get("QUALIFIED_CACHE_SECONDS", str(12 * 60 * 60)))  # 12h
QUALIFIED_SOURCE_URL = os.environ.get(
    "QUALIFIED_SOURCE_URL",
    # Prefer the main tournament page's "Qualified teams" table.
    # It updates as teams qualify and is less likely to include non-team rows.
    "https://en.wikipedia.org/api/rest_v1/page/html/2026_FIFA_World_Cup",
)

def _local_country_list() -> List[str]:
    """Return World Cup 2026 participant list derived from fixtures (no network).

    We derive the participant set from the match fixtures already loaded by the app
    (load_all_matches), extracting unique home + away team names.

    Some fixture sources include *placeholders* during qualification or bracket setup
    (e.g., "1A", "2B", "DEN/MKD/CZE/IRL"). We intentionally filter those out so the
    Fan Zone selector only shows real teams.
    """
    def _is_real_team(name: str) -> bool:
        n = (name or "").strip()
        if not n:
            return False
        # Common placeholders / undecided tokens
        if n.lower() in {"tbd", "to be decided", "to be determined", "winner", "loser", "n/a"}:
            return False
        # Group/slot placeholders like "1A", "2B", "3ABCDF" etc.
        if re.fullmatch(r"\d+[A-Za-z]{1,10}", n):
            return False
        # Any remaining digits usually indicate placeholders ("Match 12", "3rd Place", etc.)
        if any(ch.isdigit() for ch in n):
            return False
        # Slash-delimited options are not a single participant (e.g., "BOL/SUR/IRQ")
        if "/" in n:
            return False
        return True

    try:
        teams = set()
        for match in load_all_matches() or []:
            h = (match.get("home") or "").strip()
            a = (match.get("away") or "").strip()
            if _is_real_team(h):
                teams.add(h)
            if _is_real_team(a):
                teams.add(a)

        # If we got a sensible participant count, return it.
        # (Final tournament = 48; allow some slack for different fixture sources.)
        if 10 <= len(teams) <= 70:
            # Ensure hosts present even if a fixture source omits them.
            for host in ["United States", "Canada", "Mexico"]:
                teams.add(host)
            return sorted(teams)
    except Exception:
        pass

    # Hard fallback
    return ["United States", "Canada", "Mexico"]

def _fetch_qualified_teams_remote() -> List[str]:
    """
    Fetch the *currently qualified* 2026 World Cup teams from Wikipedia (best-effort).

    We use the MediaWiki API for the "2026 FIFA World Cup qualification" page and
    extract the "Qualified teams" table specifically. This avoids accidentally
    returning hundreds of FIFA members.
    """
    url = QUALIFIED_SOURCE_URL
    import urllib.request

    # 1) Fetch HTML (or MediaWiki parse JSON containing HTML)
    req = urllib.request.Request(
        url,
        headers={"User-Agent": "worldcup-concierge/1.0"},
        method="GET",
    )
    with urllib.request.urlopen(req, timeout=12) as resp:
        raw = resp.read().decode("utf-8", errors="ignore")

    html_blob = raw
    # If user configured MediaWiki API JSON, extract the HTML blob.
    if raw.lstrip().startswith("{"):
        try:
            data = json.loads(raw)
            html_blob = (data.get("parse", {}).get("text", {}) or {}).get("*", "") or ""
        except Exception:
            html_blob = ""

    if not html_blob:
        return []

    # 2) Find the "Qualified teams" section and then choose the most likely table.
    # We do NOT assume the first table is the right one (Wikipedia pages often have
    # navigation/other tables near section headers).
    anchor_pos = -1
    for anchor in (
        'id="Qualified_teams"',
        'id="Qualified_teams_and_rankings"',
        'id="Qualified_teams_and_rankings"',
        'id="Qualified_teams_and_rankings"',
    ):
        anchor_pos = html_blob.find(anchor)
        if anchor_pos != -1:
            break
    if anchor_pos == -1:
        # Some renderings use a <span id="Qualified_teams"> marker.
        anchor_pos = html_blob.find('<span id="Qualified_teams"')
    if anchor_pos == -1:
        return []

    sub = html_blob[anchor_pos:]

    # Grab a few candidate wikitables and pick the first one that looks like a
    # qualified-teams table (must contain a "Team" header AND a "Method/Qualification"-ish header).
    candidates = re.findall(
        r"<table[^>]*class=\"[^\"]*wikitable[^\"]*\"[^>]*>.*?</table>",
        sub,
        flags=re.S | re.I,
    )

    def _looks_like_qualified_table(tbl: str) -> bool:
        """Heuristic: pick the actual "Qualified teams" table, not nearby nav/summary tables."""
        t = tbl.lower()
        # Must have a "Team" header.
        if not re.search(r">\s*team\s*<", t):
            return False
        # Must have at least one of the usual columns.
        if not any(k in t for k in ["qualification", "qualified", "method", "date"]):
            return False
        # Should not be a navbox.
        if "navbox" in t or "nowrap" in t and "navbox" in t:
            return False
        return True

    table = ""
    for cand in candidates[:6]:
        if _looks_like_qualified_table(cand):
            table = cand
            break
    if not table and candidates:
        table = candidates[0]
    if not table:
        return []

    # 3) Extract team names from the first column of each row.
    teams: List[str] = []
    skip_exact = {
        "team",
        "qualified teams",
        "method of qualification",
        "date of qualification",
        "qualification",
        "notes",
    }
    skip_contains = [
        "confederation",
        "afc",
        "caf",
        "concacaf",
        "conmebol",
        "uefa",
        "ofc",
        "tbd",
        "to be determined",
    ]

    for row in re.findall(r"<tr[^>]*>(.*?)</tr>", table, flags=re.S | re.I):
        # First cell in the row
        cell_m = re.search(r"<t[hd][^>]*>(.*?)</t[hd]>", row, flags=re.S | re.I)
        if not cell_m:
            continue
        cell = cell_m.group(1)

        # Prefer the first wiki link text that isn't a File:/Category:/Help: etc.
        link_m = None
        for m in re.finditer(r"<a[^>]+href=\"([^\"]+)\"[^>]*>([^<]+)</a>", cell, flags=re.I):
            href = (m.group(1) or "").strip()
            txt = (m.group(2) or "").strip()
            if not txt:
                continue
            if href.startswith("/wiki/") and ":" not in href:
                link_m = m
                break
        name = (link_m.group(2) if link_m else re.sub(r"<[^>]+>", " ", cell))
        name = re.sub(r"\s+", " ", name).strip()
        name = re.sub(r"\s*\[\d+\]\s*", " ", name).strip()

        low = name.lower()
        if not name or low in skip_exact:
            continue
        if any(s in low for s in skip_contains):
            continue
        if name not in teams:
            teams.append(name)

    # If we somehow matched the wrong table, the result can explode. Guard hard:
    # - today this should be small (qualification is ongoing)
    # - even once complete it's 48.
    if len(teams) > 80:
        return []

    # 4) Ensure hosts included and return.
    for h in ["United States", "Canada", "Mexico"]:
        if h not in teams:
            teams.insert(0, h)

    # Sanity guard: if parsing goes sideways and returns a giant list,
    # treat it as a failure so we don't show non-World-Cup countries.
    if len(teams) > 70:
        return ["United States", "Canada", "Mexico"]
    return teams


def get_qualified_teams(force: bool = False) -> List[str]:
    """Return countries for the Fan Zone selector (fast + reliable).

    Default behavior (no network):
      - Return a stable list of countries from local pycountry data.

    Optional (network):
      - If USE_REMOTE_QUALIFIED=1, we refresh from QUALIFIED_SOURCE_URL on a TTL.
    """
    now = int(time.time())

    # Ensure we always have something usable
    if not _qualified_cache.get("teams"):
        _qualified_cache["teams"] = _local_country_list()
        _qualified_cache["loaded_at"] = now

    if not USE_REMOTE_QUALIFIED:
        return list(_qualified_cache["teams"])

    fresh = (now - int(_qualified_cache.get("loaded_at") or 0) < QUALIFIED_CACHE_SECONDS)
    if force or not fresh:
        try:
            teams = _fetch_qualified_teams_remote()
            if teams:
                _qualified_cache["teams"] = teams
                _qualified_cache["loaded_at"] = now
        except Exception:
            # Keep existing cache on failure
            pass

    return list(_qualified_cache["teams"])

# ============================================================
# Live scores + group standings (dynamic, non-breaking)
#  - If the fixture feed provides final scores, we surface them.
#  - True live scores require a licensed live data provider.
# ============================================================


# ---- Live/Standings reliability layer (ETag + short server cache) ----
_live_payload_cache: Dict[Tuple[str, str], Dict[str, Any]] = {}
_stand_payload_cache: Dict[str, Dict[str, Any]] = {}
_payload_cache_ttl_sec = 15

def _json_with_etag(payload: Dict[str, Any]):
    """Return JSON with a stable ETag header (always 200).

    Why: some fetch() code treats HTTP 304 as an error (res.ok === false),
    which can look like the app is 'broken'. This keeps reliability benefits
    (ETag + short server cache) without ever returning 304.
    """
    body = json.dumps(payload, separators=(",", ":"), ensure_ascii=False)
    etag = hashlib.sha256(body.encode("utf-8")).hexdigest()[:16]
    resp = jsonify(payload)
    resp.headers["ETag"] = f"\"{etag}\""
    resp.headers["Cache-Control"] = "no-store"
    return resp

def _now_ts() -> int:
    return int(time.time())
def _utc_now():
    return datetime.now(timezone.utc)

@app.route("/worldcup/live.json")
def worldcup_live_json():
    """Return matches in a 'live window' plus recently finished.

    Query params:
      scope= dallas | all (default: all)
      window_hours= hours around now to include (default: 8)
    """
    scope = (request.args.get("scope") or "all").lower().strip()
    try:
        window_h = float(request.args.get("window_hours") or "8")
    except Exception:
        window_h = 8.0

    cache_key = (scope, str(window_h))
    c = _live_payload_cache.get(cache_key)
    if c and (_now_ts() - int(c.get("_cached_at", 0)) <= _payload_cache_ttl_sec):
        return _json_with_etag(c["payload"])
    try:
        matches = filter_matches(scope=scope, q="")
    except Exception:
        matches = []

    nowu = _utc_now()
    win = timedelta(hours=window_h)
    out = []
    for m in matches:
        dt = (m.get("datetime_utc") or "").strip()
        if not dt:
            continue
        try:
            k = datetime.strptime(dt, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        except Exception:
            continue

        # Include: live window (pre + in-game + short post) OR explicitly marked live/finished
        if (k - win) <= nowu <= (k + win):
            out.append(m)
        elif (m.get("status") in ["live", "finished"]):
            out.append(m)

    payload = {
        "scope": scope,
        "updated_at": _now_ts(),
        "count": len(out),
        "matches": out,
        "quality": {
            "scored_matches": sum(1 for m in out if (m.get("score_home") is not None or m.get("score_away") is not None)),
            "has_any_scores": any((m.get("score_home") is not None or m.get("score_away") is not None) for m in out),
        },
        "note": "Scores are shown only when present in the fixture feed. For true real-time live scores, wire in a licensed live data provider/API key.",
    }
    _live_payload_cache[cache_key] = {"_cached_at": _now_ts(), "payload": payload}
    _stand_payload_cache[scope] = {"_cached_at": _now_ts(), "payload": payload}
    return _json_with_etag(payload)
def _compute_group_standings(matches: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Compute group standings from available completed group matches."""
    groups: Dict[str, Dict[str, Any]] = {}

    def ensure_team(g: str, team: str):
        if not team:
            return
        groups.setdefault(g, {})
        groups[g].setdefault(team, {
            "team": team,
            "p": 0, "w": 0, "d": 0, "l": 0,
            "gf": 0, "ga": 0, "gd": 0,
            "pts": 0,
        })

    for m in matches:
        stage = (m.get("stage") or "").strip()
        if not stage.lower().startswith("group"):
            continue
        hs = m.get("home_score")
        as_ = m.get("away_score")
        if hs is None or as_ is None:
            continue  # can't compute without a result
        try:
            hs = int(hs); as_ = int(as_)
        except Exception:
            continue

        g = stage
        home = (m.get("home") or "").strip() or "TBD"
        away = (m.get("away") or "").strip() or "TBD"
        ensure_team(g, home); ensure_team(g, away)
        if home == "TBD" or away == "TBD":
            continue

        ht = groups[g][home]
        at = groups[g][away]
        ht["p"] += 1; at["p"] += 1
        ht["gf"] += hs; ht["ga"] += as_
        at["gf"] += as_; at["ga"] += hs

        if hs > as_:
            ht["w"] += 1; at["l"] += 1
            ht["pts"] += 3
        elif hs < as_:
            at["w"] += 1; ht["l"] += 1
            at["pts"] += 3
        else:
            ht["d"] += 1; at["d"] += 1
            ht["pts"] += 1; at["pts"] += 1

    # finalize GD + sorting
    out: Dict[str, Any] = {}
    for g, teams in groups.items():
        rows = list(teams.values())
        for r in rows:
            r["gd"] = int(r["gf"]) - int(r["ga"])
        rows.sort(key=lambda r: (r["pts"], r["gd"], r["gf"], r["team"]), reverse=True)
        out[g] = rows
    return out


@app.route("/worldcup/standings.json")
def worldcup_standings_json():
    scope = (request.args.get("scope") or "all").lower().strip()
    c = _stand_payload_cache.get(scope)
    if c and (_now_ts() - int(c.get("_cached_at", 0)) <= _payload_cache_ttl_sec):
        return _json_with_etag(c["payload"])
    try:
        matches = filter_matches(scope=scope, q="")
    except Exception:
        matches = []

    standings = _compute_group_standings(matches)
    payload = {
        "scope": scope,
        "updated_at": _now_ts(),
        "groups": standings,
        "count_groups": len(standings),
        "note": "Standings are computed from group matches that have scores present in the fixture feed.",
    }
    return _json_with_etag(payload)


@app.route("/worldcup/feed_status.json")
def worldcup_feed_status():
    """Small health payload for the schedule feed (used for debugging + UI fallbacks)."""
    loaded_at = int(_fixtures_cache.get("loaded_at") or 0)
    age = _now_ts() - loaded_at if loaded_at else None
    return jsonify({
        "feed_url": FIXTURE_FEED_URL,
        "cache_loaded_at": loaded_at,
        "cache_age_sec": age,
        "cache_ttl_sec": FIXTURE_CACHE_SECONDS,
        "source": _fixtures_cache.get("source") or "unknown",
        "last_error": _fixtures_cache.get("last_error"),
        "disk_cache_file": FIXTURE_CACHE_FILE,
    })


@app.route("/worldcup/qualified.json")
def qualified_json():
    teams = get_qualified_teams()
    return jsonify({
        "updated_at": int(_qualified_cache.get("loaded_at") or 0),
        "count": len(teams),
        "teams": teams,
        "countries": teams,   # alias for front-end compatibility
        "qualified": teams,   # alias for front-end compatibility
        "note": "Teams qualified so far for World Cup 2026 (hosts always included).",
    })



@app.route("/countries/qualified.json")
def qualified_json_alias():
    # Alias for compatibility with older front-ends/tests
    return qualified_json()


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
# Chat endpoint with reservation state machine
#   - MUST always return JSON with {reply: ...}
#   - "reservation" triggers deterministic lead capture
# ============================================================
@app.route("/chat", methods=["POST"])
def chat():
    try:
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

        # Update session language if user toggled
        sess["lang"] = lang
        sess["lead"]["language"] = lang

        if not msg:
            return jsonify({"reply": "Please type a message.", "rate_limit_remaining": remaining})

        # Recall support (all languages)
        if want_recall(msg, lang):
            return jsonify({"reply": recall_text(sess), "rate_limit_remaining": remaining})

        # Start reservation flow if user indicates intent
        if sess["mode"] == "idle" and want_reservation(msg):
            sess["mode"] = "reserving"

            # Mark VIP if user clicked a VIP button or mentions VIP
            if re.search(r"\bvip\b", msg.lower()):
                sess["lead"]["vip"] = "Yes"
            # IMPORTANT: do NOT treat the word "reservation" as the name.
            if msg.lower().strip() in ["reservation", "reserva", "réservation"]:
                sess["lead"]["name"] = ""

            q = next_question(sess)
            return jsonify({"reply": q, "rate_limit_remaining": remaining})

        # If reserving, keep collecting fields deterministically
        if sess["mode"] == "reserving":
            # Allow VIP to be set at any time during reservation flow
            if re.search(r"\bvip\b", msg.lower()):
                sess["lead"]["vip"] = "Yes"
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

            # Name extraction
            if not sess["lead"].get("name"):
                cand = extract_name_candidate(msg)
                if cand:
                    sess["lead"]["name"] = cand

            # Apply business rules if we have enough to check
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
                        f"Party size: {lead['party_size']}\n"
                        f"Status: {lead.get('status','New')}\n"
                        f"VIP: {lead.get('vip','No')}"
                    )
                    sess["lead"] = {
                        "name": "",
                        "phone": "",
                        "date": "",
                        "time": "",
                        "party_size": 0,
                        "language": lang,
                        "status": "New",
                        "vip": "No",
                    }
                    return jsonify({"reply": confirm, "rate_limit_remaining": remaining})
                except Exception as e:
                    # Always return JSON so the UI never shows "no reply received".
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
    {json.dumps(MENU.get(lang, MENU['en']), ensure_ascii=False)}

    Rules:
    - Be friendly, fast, and concise.
    - Always respond in the user's chosen language: {lang}.
    - If user asks about the World Cup match schedule, tell them to use the schedule panel on the page.
    - If user asks to make a reservation, instruct them to type "reservation" (or equivalent) to start.
    """

        try:
            resp = client.responses.create(
                model=os.environ.get("CHAT_MODEL", "gpt-4o-mini"),
                input=[
                    {"role": "system", "content": system_msg},
                    {"role": "user", "content": msg},
                ],
            )
            reply = (resp.output_text or "").strip() or "(No response)"
            return jsonify({"reply": reply, "rate_limit_remaining": remaining})
        except Exception as e:
            # If OPENAI_API_KEY isn't set (or any API issue), fail gracefully.
            fallback = "⚠️ Chat is temporarily unavailable. Please try again, or type 'reservation' to book a table."
            return jsonify({"reply": f"{fallback}\n\nDebug: {type(e).__name__}", "rate_limit_remaining": remaining}), 200

    except Exception as e:
        # Never break the UI: always return JSON.
        fallback = "⚠️ Chat is temporarily unavailable. Please try again, or type 'reservation' to book a table."
        return jsonify({"reply": f"{fallback}\n\nDebug: {type(e).__name__}", "rate_limit_remaining": 0}), 200


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


# ============================================================
# Fan Poll + Admin-config (Sponsor label, Match of the Day)
# - Persists votes in Google Sheet (worksheet "PollVotes") when available.
# - Stores admin-editable config in Google Sheet (worksheet "Config") when available.
# - Falls back to local JSON file if Sheets isn't configured.
# ============================================================

DATA_DIR = os.environ.get("DATA_DIR", os.path.join("/tmp", "worldcup_app_data"))
# Store small JSON state in a writable directory (Render slug is read-only)
os.makedirs(DATA_DIR, exist_ok=True)
CONFIG_FILE = os.path.join(DATA_DIR, "app_config.json")

# -----------------------------
# Lightweight in-process caches
# (reduces Google Sheets quota hits)
# -----------------------------
_CONFIG_CACHE: Dict[str, Any] = {"ts": 0.0, "cfg": None}   # ttl ~5s
_LEADS_CACHE: Dict[str, Any] = {"ts": 0.0, "rows": None}  # ttl ~30s
_sessions: Dict[str, Dict[str, Any]] = {}  # in-memory chat/reservation sessions


def _safe_read_json(path: str) -> dict:
    try:
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f) or {}
    except Exception:
        pass
    return {}

def _safe_write_json(path: str, data: dict) -> None:
    try:
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, sort_keys=True)
        os.replace(tmp, path)
    except Exception:
        pass

def _ensure_ws(gc, title: str):
    sh = gc.open(SHEET_NAME)
    try:
        return sh.worksheet(title)
    except Exception:
        return sh.add_worksheet(title=title, rows=2000, cols=20)

def get_config() -> Dict[str, str]:
    """Config is authoritative in local CONFIG_FILE.

    We still *attempt* to read the Google Sheet (Config tab) for compatibility,
    but local values always win. This prevents 'saved but not reflected' when
    Sheets write fails (quota) and later reads return older data.
    """
    now = time.time()
    cached = _CONFIG_CACHE.get("cfg")
    if isinstance(cached, dict) and (now - float(_CONFIG_CACHE.get("ts", 0.0)) < 5.0):
        return dict(cached)

    cfg: Dict[str, str] = {
        "poll_sponsor_text": "Fan Pick presented by World Cup Dallas HQ",
        "match_of_day_id": "",
        "motd_home": "",
        "motd_away": "",
        "motd_datetime_utc": "",
        "poll_lock_mode": "auto",
    }

    local = _safe_read_json(CONFIG_FILE)
    if isinstance(local, dict):
        for k, v in local.items():
            if str(k).startswith("_"):
                continue
            cfg[str(k)] = "" if v is None else str(v)

    try:
        gc = get_gspread_client()
        ws = _ensure_ws(gc, "Config")
        rows = ws.get_all_values()
        for r in rows[1:]:
            if len(r) >= 2 and r[0]:
                k = r[0]
                v = r[1]
                if (k not in cfg) or (cfg.get(k, "") == ""):
                    cfg[k] = v
    except Exception:
        pass

    _CONFIG_CACHE["ts"] = now
    _CONFIG_CACHE["cfg"] = dict(cfg)
    return cfg

def set_config(pairs: Dict[str, str]) -> Dict[str, str]:
    """Persist config.

    1) Write to local CONFIG_FILE (authoritative + works on Render).
    2) Best-effort sync to Google Sheet (Config tab) for visibility/back-compat.
    """
    clean: Dict[str, str] = {}
    for k, v in (pairs or {}).items():
        if not k:
            continue
        clean[str(k)] = "" if v is None else str(v)

    local = _safe_read_json(CONFIG_FILE)
    if not isinstance(local, dict):
        local = {}
    local.update(clean)
    local["_updated_at"] = datetime.utcnow().isoformat(timespec="seconds") + "Z"
    _safe_write_json(CONFIG_FILE, local)

    try:
        gc = get_gspread_client()
        ws = _ensure_ws(gc, "Config")

        rows = ws.get_all_values()
        if not rows:
            ws.append_row(["key", "value"])
            rows = ws.get_all_values()

        existing = {r[0]: (i + 1) for i, r in enumerate(rows) if len(r) >= 1 and r[0]}
        for k, v in clean.items():
            if k in existing:
                ws.update_cell(existing[k], 2, v)
            else:
                ws.append_row([k, v])
    except Exception:
        pass


    # Invalidate cache so changes are visible immediately after saving (even within the cache window).
    _CONFIG_CACHE["ts"] = 0.0
    _CONFIG_CACHE["cfg"] = None
    merged = get_config()
    _CONFIG_CACHE["ts"] = time.time()
    _CONFIG_CACHE["cfg"] = dict(merged)
    return merged

def _match_id(m: Dict[str, Any]) -> str:
    # Stable-ish id: datetime_utc + home + away (safe for URL/storage)
    dt = (m.get("datetime_utc") or "").strip()
    home = (m.get("home") or "").strip()
    away = (m.get("away") or "").strip()
    base = f"{dt}|{home}|{away}"
    base = re.sub(r"[^A-Za-z0-9|:_-]+", "_", base)
    return base[:180]

def _get_match_of_day() -> Optional[Dict[str, Any]]:
    cfg = get_config()

    # Manual override (works even if fixtures can't load in this environment)
    manual_home = (cfg.get("motd_home") or "").strip()
    manual_away = (cfg.get("motd_away") or "").strip()
    manual_dt = (cfg.get("motd_datetime_utc") or "").strip()
    if manual_home and manual_away:
        return {
            "id": (cfg.get("match_of_day_id") or "manual").strip() or "manual",
            "datetime_utc": manual_dt,  # may be empty; lock will remain false if empty
            "home": manual_home,
            "away": manual_away,
            "stage": (cfg.get("motd_stage") or "").strip(),
            "venue": (cfg.get("motd_venue") or "").strip(),
            "date": (cfg.get("motd_date") or "").strip(),
            "time": (cfg.get("motd_time") or "").strip(),
        }

    override_id = (cfg.get("match_of_day_id") or "").strip()
    # If no explicit Match of the Day is configured, fall back to the next upcoming match
    # from your fixtures. This keeps the poll usable "out of the box" and ensures it stays
    # in sync whenever your fixtures data is updated.

    try:
        matches = load_all_matches()
    except Exception:
        matches = []

    if override_id:
        for m in matches:
            if _match_id(m) == override_id:
                return m

    # Default: next upcoming match globally (all matches is already sorted by datetime_utc)
    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    for m in matches:
        if (m.get("datetime_utc") or "") >= now_utc:
            return m
    return matches[0] if matches else None


def _poll_is_locked(match: Optional[Dict[str, Any]]) -> bool:
    """Return whether the poll is locked.

    Default behavior is auto-lock at kickoff.
    Admin can override via config key poll_lock_mode: auto | locked | unlocked.
    """
    if not match:
        return False
    try:
        mode = (get_config().get("poll_lock_mode") or "auto").strip().lower()
    except Exception:
        mode = "auto"
    if mode == "locked":
        return True
    if mode == "unlocked":
        return False
    # auto
    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    kickoff = (match.get("datetime_utc") or "").strip()
    return bool(kickoff and now_utc >= kickoff)

def _poll_is_post_match(match: Optional[Dict[str, Any]]) -> bool:
    # Best-effort: assume 2h match duration, then post-match highlight
    try:
        kickoff = (match.get("datetime_utc") or "").strip()
        if not kickoff:
            return False
        k = datetime.strptime(kickoff, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        return datetime.now(timezone.utc) >= (k + timedelta(hours=2))
    except Exception:
        return False

def _poll_store_read() -> Dict[str, Any]:
    data = _safe_read_json_file(POLL_STORE_FILE, default={})
    if not isinstance(data, dict):
        data = {}
    data.setdefault("matches", {})
    if not isinstance(data["matches"], dict):
        data["matches"] = {}
    return data

def _poll_store_write(data: Dict[str, Any]) -> None:
    _safe_write_json_file(POLL_STORE_FILE, data)

def _poll_match_bucket(match_id: str) -> Dict[str, Any]:
    data = _poll_store_read()
    matches = data.get("matches", {})
    bucket = matches.get(match_id) or {}
    if not isinstance(bucket, dict):
        bucket = {}
    bucket.setdefault("clients", {})
    bucket.setdefault("counts", {})
    if not isinstance(bucket["clients"], dict):
        bucket["clients"] = {}
    if not isinstance(bucket["counts"], dict):
        bucket["counts"] = {}
    # Persist any normalization back
    matches[match_id] = bucket
    data["matches"] = matches
    _poll_store_write(data)
    return bucket

def _poll_has_voted(match_id: str, client_id: str) -> Optional[str]:
    bucket = _poll_match_bucket(match_id)
    return (bucket.get("clients") or {}).get(client_id)

def _poll_counts(match_id: str) -> Dict[str, int]:
    bucket = _poll_match_bucket(match_id)
    counts = bucket.get("counts") or {}
    out: Dict[str, int] = {}
    if isinstance(counts, dict):
        for k, v in counts.items():
            try:
                out[str(k)] = int(v)
            except Exception:
                out[str(k)] = 0
    return out

def _poll_record_vote(match_id: str, client_id: str, team: str) -> bool:
    if not (match_id and client_id and team):
        return False

    data = _poll_store_read()
    matches = data.get("matches", {})
    bucket = matches.get(match_id) or {}
    if not isinstance(bucket, dict):
        bucket = {}
    clients = bucket.get("clients") or {}
    counts = bucket.get("counts") or {}
    if not isinstance(clients, dict):
        clients = {}
    if not isinstance(counts, dict):
        counts = {}

    if client_id in clients:
        return False  # already voted

    clients[client_id] = team
    counts[team] = int(counts.get(team, 0)) + 1

    bucket["clients"] = clients
    bucket["counts"] = counts
    matches[match_id] = bucket
    data["matches"] = matches
    _poll_store_write(data)
    return True


@app.route("/api/config")
def api_config():
    cfg = get_config()
    public = {
        "poll_sponsor_text": cfg.get("poll_sponsor_text", ""),
        "match_of_day_id": cfg.get("match_of_day_id", ""),
        "motd_home": cfg.get("motd_home", ""),
        "motd_away": cfg.get("motd_away", ""),
        "motd_datetime_utc": cfg.get("motd_datetime_utc", ""),
        "poll_lock_mode": cfg.get("poll_lock_mode", "auto"),
    }
    return jsonify(public)


@app.route("/api/poll/state")
def api_poll_state():
    """Return match poll state.

    This endpoint must *always* return JSON so the Fan Zone UI never breaks.
    If anything goes wrong (fixtures/config/poll store), we fall back to a safe placeholder.
    """
    try:
        motd = _get_match_of_day()
        if not motd:
            # Keep the UI responsive even if matches failed to load.
            cfg = get_config()
            return jsonify({
                "ok": True,
                "locked": True,
                "post_match": False,
                "winner": None,
                "sponsor_text": cfg.get("poll_sponsor_text", ""),
                "match": {"id": "placeholder", "home": "Team A", "away": "Team B", "kickoff": ""},
                "counts": {"Team A": 0, "Team B": 0},
                "percent": {"Team A": 0.0, "Team B": 0.0},
                "percentages": {"Team A": 0.0, "Team B": 0.0},
                "total": 0,
                "total_votes": 0,
                "note": "Matches not available yet."
            }), 200

        mid = _match_id(motd)
        locked = _poll_is_locked(motd)
        post_match = _poll_is_post_match(motd)

        teams = [motd.get("home") or "Team A", motd.get("away") or "Team B"]
        counts = _poll_counts(mid)
        total = sum(counts.get(t, 0) for t in teams)
        pct = {}
        for t in teams:
            pct[t] = (counts.get(t, 0) / total * 100.0) if total > 0 else 0.0

        # Winner is purely UI-only: leader when locked, or after match.
        winner = None
        if total > 0:
            winner = max(teams, key=lambda t: counts.get(t, 0))
        cfg = get_config()
        return jsonify({
            "ok": True,
            "match": {
                "id": mid,
                "date": motd.get("date"),
                "time": motd.get("time"),
                "datetime_utc": motd.get("datetime_utc"),
                "home": teams[0],
                "away": teams[1],
                "stage": motd.get("stage"),
                "venue": motd.get("venue"),
            },
            "locked": locked,
            "post_match": post_match,
            "winner": winner,
            "counts": {t: int(counts.get(t, 0)) for t in teams},
            "percentages": {t: round(pct[t], 1) for t in teams},
            "percent": {t: round(pct[t], 1) for t in teams},
            "total_votes": int(total),
            "total": int(total),
            "sponsor_text": cfg.get("poll_sponsor_text", ""),
        })
    except Exception:
        # Absolute last resort: return a safe placeholder instead of 500/HTML.
        cfg = {}
        try:
            cfg = get_config()
        except Exception:
            cfg = {}
        return jsonify({
            "ok": True,
            "locked": True,
            "post_match": False,
            "winner": None,
            "sponsor_text": cfg.get("poll_sponsor_text", "") if isinstance(cfg, dict) else "",
            "match": {"id": "placeholder", "home": "Team A", "away": "Team B", "kickoff": ""},
            "counts": {"Team A": 0, "Team B": 0},
            "percent": {"Team A": 0.0, "Team B": 0.0},
            "percentages": {"Team A": 0.0, "Team B": 0.0},
            "total": 0,
            "total_votes": 0,
            "note": "Poll temporarily unavailable."
        }), 200

@app.route("/api/poll/vote", methods=["POST"])
def api_poll_vote():
    data = request.get_json(silent=True) or {}
    client_id = (data.get("client_id") or "").strip()
    team = (data.get("team") or "").strip()

    motd = _get_match_of_day()
    if not motd:
        # Keep the UI responsive even if matches failed to load.
        cfg = get_config()
        return jsonify({
            "ok": True,
            "locked": True,
            "post_match": False,
            "winner": None,
            "sponsor_text": cfg.get("poll_sponsor_text", ""),
            "match": {"id": "placeholder", "home": "Team A", "away": "Team B", "kickoff": ""},
            "counts": {"Team A": 0, "Team B": 0},
            "percent": {"Team A": 0.0, "Team B": 0.0},
            "percentages": {"Team A": 0.0, "Team B": 0.0},
            "total": 0,
            "total_votes": 0,
            "note": "Matches not available yet."
        }), 200

    mid = _match_id(motd)
    if _poll_is_locked(motd):
        return jsonify({"ok": False, "error": "Poll locked at kickoff"}), 423

    teams = [motd.get("home") or "Team A", motd.get("away") or "Team B"]
    if team not in teams:
        return jsonify({"ok": False, "error": "Invalid team"}), 400

    already = _poll_has_voted(mid, client_id)
    if already:
        return jsonify({"ok": False, "error": "Already voted", "voted_for": already}), 409

    ok = _poll_record_vote(mid, client_id, team)
    if not ok:
        return jsonify({"ok": False, "error": "Could not record vote"}), 500

    # return updated state
    return api_poll_state()

@app.route("/admin/update-config", methods=["POST"])
def admin_update_config():
    key = request.args.get("key", "")
    if not ADMIN_KEY or key != ADMIN_KEY:
        return jsonify({"ok": False, "error": "Unauthorized"}), 401

    data = request.get_json(silent=True) or {}

    try:

        # Allow clearing values by sending empty strings.
        sponsor = (data.get("poll_sponsor_text") if data.get("poll_sponsor_text") is not None else "")
        match_id = (data.get("match_of_day_id") if data.get("match_of_day_id") is not None else "")

        # Normalize to the same safe/stable format used by fixtures + _match_id()
        match_id_norm = str(match_id).strip()
        if match_id_norm:
            match_id_norm = re.sub(r"[^A-Za-z0-9|:_-]+", "_", match_id_norm)[:180]

        motd_home = (data.get("motd_home") if data.get("motd_home") is not None else "")
        motd_away = (data.get("motd_away") if data.get("motd_away") is not None else "")
        motd_datetime_utc = (data.get("motd_datetime_utc") if data.get("motd_datetime_utc") is not None else "")

        poll_lock_mode = (data.get("poll_lock_mode") if data.get("poll_lock_mode") is not None else "auto")

        pairs = {
            "poll_sponsor_text": str(sponsor).strip(),
            "match_of_day_id": match_id_norm,
            "motd_home": str(motd_home).strip(),
            "motd_away": str(motd_away).strip(),
            "motd_datetime_utc": str(motd_datetime_utc).strip(),
            "poll_lock_mode": (str(poll_lock_mode).strip() or "auto"),
        }

        cfg = set_config(pairs)
        return jsonify({"ok": True, "config": {
            "poll_sponsor_text": cfg.get("poll_sponsor_text",""),
            "match_of_day_id": cfg.get("match_of_day_id",""),
            "motd_home": cfg.get("motd_home",""),
            "motd_away": cfg.get("motd_away",""),
            "motd_datetime_utc": cfg.get("motd_datetime_utc",""),
            "poll_lock_mode": cfg.get("poll_lock_mode","auto"),
        }})




    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

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
select option{background:rgba(15,27,51,1);color:var(--text)}
select option:hover{background:rgba(255,255,255,.12)}
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
    html.append(f"<div style=\"display:flex;gap:8px;margin:10px 0 14px 0;flex-wrap:wrap;\"><a href=\"/admin?key={key}\" style=\"text-decoration:none;color:var(--text);padding:8px 12px;border:1px solid var(--line);border-radius:999px;background:rgba(255,255,255,.04);font-weight:800\">Leads</a><a href=\"/admin/fanzone?key={key}\" style=\"text-decoration:none;color:var(--text);padding:8px 12px;border:1px solid rgba(212,175,55,.35);border-radius:999px;background:rgba(212,175,55,.10);font-weight:900\">Fan Zone</a></div>")

    # Fan Zone controls moved to /admin/fanzone


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





@app.route("/admin/fanzone")
def admin_fanzone():
    key = request.args.get("key", "")
    if not ADMIN_KEY or key != ADMIN_KEY:
        return "Unauthorized", 401

    html = []
    html.append("<!doctype html><html><head><meta charset='utf-8'><meta name='viewport' content='width=device-width, initial-scale=1'/>")
    html.append("<title>Fan Zone Admin</title>")
    html.append("""<style>
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
select option{background:rgba(15,27,51,1);color:var(--text)}
select option:hover{background:rgba(255,255,255,.12)}
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
</style>""")
    html.append("</head><body><div class='wrap'>")

    html.append("<div class='topbar'>")
    html.append(f"<div><div class='h1'>Fan Zone Admin — {_hesc(SHEET_NAME or 'World Cup')}</div><div class='sub'>Poll controls (Sponsor text + Match of the Day) • Key required</div></div>")
    html.append("<div style='display:flex;gap:10px;align-items:center;flex-wrap:wrap'>")
    html.append(f"<a class='btn' href='/admin?key={key}' style='text-decoration:none;display:inline-block'>Leads</a>")
    html.append(f"<a class='btn' href='/admin/fanzone?key={key}' style='text-decoration:none;display:inline-block'>Poll Controls</a>")
    html.append("</div></div>")

    html.append(f"<div style='display:flex;gap:8px;margin:10px 0 14px 0;flex-wrap:wrap;'>"
                f"<a href='/admin?key={key}' style='text-decoration:none;color:var(--text);padding:8px 12px;border:1px solid var(--line);border-radius:999px;background:rgba(255,255,255,.04);font-weight:800'>Leads</a>"
                f"<a href='/admin/fanzone?key={key}' style='text-decoration:none;color:var(--text);padding:8px 12px;border:1px solid rgba(212,175,55,.35);border-radius:999px;background:rgba(212,175,55,.10);font-weight:900'>Fan Zone</a>"
                f"</div>")

    html.append(r"""
<div class="panelcard" style="margin:14px 0;border:1px solid var(--line);border-radius:16px;padding:12px;background:rgba(255,255,255,.03);box-shadow:0 10px 35px rgba(0,0,0,.25)">
  <div style="display:flex;align-items:flex-end;justify-content:space-between;gap:12px;flex-wrap:wrap">
    <div>
      <div style="font-weight:800;letter-spacing:.02em">Fan Zone • Poll Controls</div>
      <div class="sub">Edit sponsor text + set Match of the Day (no redeploy). Also shows live poll status.</div>
    </div>
    <button class="btn" id="btnSaveConfig">Save settings</button>
  </div>

  <div class="controls" style="margin:12px 0 0 0">
    <div style="display:flex;flex-direction:column;gap:6px;min-width:320px;flex:1">
      <div class="sub">Sponsor label (“Presented by …”)</div>
      <input class="inp" id="pollSponsorText" placeholder="Fan Pick presented by …" />
    </div>
    <div style="display:flex;flex-direction:column;gap:6px;min-width:320px;flex:1">
      <div class="sub">Match of the Day</div>
      <select id="motdSelect"></select>
      <div class="sub" style="margin-top:8px">If schedule options don’t load (or you want to override), set Match of the Day manually:</div>
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-top:8px">
        <div><div class="sub">Home team</div><input id="motdHome" placeholder="Home team"/></div>
        <div><div class="sub">Away team</div><input id="motdAway" placeholder="Away team"/></div>
      </div>
      <div style="margin-top:10px">
        <div class="sub">Kickoff (UTC, ISO 8601, e.g. 2026-06-11T19:00:00Z) — used to lock poll at kickoff</div>
        <input id="motdKickoff" placeholder="2026-06-11T19:00:00Z"/>
      </div>
      <div style="margin-top:10px">
        <div class="sub">Poll lock</div>
        <select id="pollLockMode" class="inp">
          <option value="auto">Auto (lock at kickoff)</option>
          <option value="unlocked">Force Unlocked (admin override)</option>
          <option value="locked">Force Locked</option>
        </select>
        <div class="small">If you need to reopen voting after kickoff, set <b>Force Unlocked</b>.</div>
      </div>
    </div>
  </div>

  <div id="pollStatus" style="margin-top:12px;border-top:1px solid var(--line);padding-top:12px">
    <div class="sub">Loading poll status…</div>
  </div>
</div>
<script>
(function(){
  const ADMIN_KEY = (new URLSearchParams(location.search)).get("key") || "";

  const $ = (id)=>document.getElementById(id);

  function esc(s){ return (s||"").replace(/[&<>"]/g, c=>({ "&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;" }[c])); }

  async function loadScheduleOptions(selected){
    const sel = $("motdSelect");
    if(!sel) return;

    sel.innerHTML = "<option value=''>Auto (next upcoming match)</option>";

    try{
      const res = await fetch("/schedule.json?scope=all&q=", {cache:"no-store"});
      const data = await res.json();
      const matches = (data && data.matches) ? data.matches : [];
      let added = 0;

      for(const m of matches){
        if(added > 180) break;
        const id = (m.datetime_utc || "") + "|" + (m.home||"") + "|" + (m.away||"");
        const safeId = id.replace(/[^A-Za-z0-9|:_-]+/g,"_").slice(0,180);
        const label = `${m.date||""} ${m.time||""} • ${m.home||""} vs ${m.away||""} • ${m.venue||""}`;

        const opt = document.createElement("option");
        opt.value = safeId;
        opt.textContent = label;
        opt.setAttribute("data-home", m.home||"");
        opt.setAttribute("data-away", m.away||"");
        opt.setAttribute("data-dt", m.datetime_utc||"");
        if(selected && selected === safeId) opt.selected = true;
        sel.appendChild(opt);
        added++;
      }
    }catch(e){}

    // sync manual fields to selection
    sel.onchange = ()=>{
      try{
        const opt = sel.options[sel.selectedIndex];
        if(!opt) return;
        // IMPORTANT: don't erase a manual override when switching back to Auto.
        if((opt.value||"") === ""){
          const curHome = ($("motdHome")?.value || "").trim();
          const curAway = ($("motdAway")?.value || "").trim();
          const curKick = ($("motdKickoff")?.value || "").trim();
          if(curHome || curAway || curKick) return;
        }
        if($("motdHome")) $("motdHome").value = opt.getAttribute("data-home") || "";
        if($("motdAway")) $("motdAway").value = opt.getAttribute("data-away") || "";
        if($("motdKickoff")) $("motdKickoff").value = opt.getAttribute("data-dt") || "";
      }catch(e){}
    };

    // trigger once
    try{ sel.onchange(); }catch(e){}
  }

  async function loadConfig(){
    try{
      const res = await fetch("/api/config", {cache:"no-store"});
      const cfg = await res.json();
      if($("pollSponsorText")) $("pollSponsorText").value = (cfg.poll_sponsor_text || "");
      if($("motdHome")) $("motdHome").value = (cfg.motd_home || "");
      if($("motdAway")) $("motdAway").value = (cfg.motd_away || "");
      if($("motdKickoff")) $("motdKickoff").value = (cfg.motd_datetime_utc || "");
      if($("pollLockMode")) $("pollLockMode").value = (cfg.poll_lock_mode || "auto");
      await loadScheduleOptions(cfg.match_of_day_id || "");
    }catch(e){
      await loadScheduleOptions("");
    }
  }

  function renderPollStatus(state){
    const wrap = $("pollStatus");
    if(!wrap) return;

    if(!state || !state.ok){
      wrap.innerHTML = "<div class='sub'>Poll status unavailable.</div>";
      return;
    }

    const m = state.match || {};
    const teams = [m.home, m.away].filter(Boolean);
    const counts = state.counts || {};
    const pct = state.percentages || {};
    const locked = !!state.locked;
    const post = !!state.post_match;
    const winner = state.winner || "";
    const sponsor = state.sponsor_text || "";
    const total = state.total_votes || 0;

    const rows = teams.map(t=>{
      const isWin = post && winner && winner === t;
      const p = Number(pct[t] || 0);
      const c = Number(counts[t] || 0);
      const barColor = isWin ? "rgba(212,175,55,.85)" : "rgba(46,160,67,.75)";
      return `
        <div style="display:flex;align-items:center;gap:10px;margin:8px 0">
          <div style="flex:0 0 160px;font-weight:700">${esc(t)} ${isWin ? "🏆" : ""}</div>
          <div style="flex:1;border:1px solid rgba(255,255,255,.12);border-radius:999px;overflow:hidden;height:10px;background:rgba(255,255,255,.04)">
            <div style="height:100%;width:${p}%;background:${barColor}"></div>
          </div>
          <div style="flex:0 0 120px;text-align:right;color:var(--muted)">${p.toFixed(1)}% • ${c}</div>
        </div>
      `;
    }).join("");

    wrap.innerHTML = `
      <div style="display:flex;justify-content:space-between;align-items:flex-end;gap:10px;flex-wrap:wrap">
        <div>
          <div class="sub">Current match</div>
          <div style="font-weight:800">${esc(m.home || "")} vs ${esc(m.away || "")}</div>
          <div class="small">${esc(m.date || "")} ${esc(m.time || "")} • ${esc(m.venue || "")}</div>
        </div>
        <div style="text-align:right">
          <div class="sub">Status</div>
          <div style="font-weight:800">${locked ? (post ? "Post-match" : "Locked (kickoff)") : "Open"}</div>
        </div>
      </div>
      ${sponsor ? `<div class="small" style="margin-top:8px">Sponsor: <b>${esc(sponsor)}</b></div>` : ""}
      <div class="small" style="margin-top:6px">Total votes: <b>${total}</b></div>
      <div style="margin-top:6px">${rows}</div>
    `;
  }

  async function loadPoll(){
    try{
      const res = await fetch("/api/poll/state", {cache:"no-store"});
      const st = await res.json();
      renderPollStatus(st);
    }catch(e){
      renderPollStatus(null);
    }
  }

  async function saveConfig(){
    const sponsor = ($("pollSponsorText")?.value || "").trim();
    const matchId = ($("motdSelect")?.value || "").trim();
    const home = ($("motdHome")?.value || "").trim();
    const away = ($("motdAway")?.value || "").trim();
    const kickoff = ($("motdKickoff")?.value || "").trim();
    const lockMode = ($("pollLockMode")?.value || "auto").trim();
    try{
      // Disable button briefly to prevent double-click race with the 5s poll refresh.
      const btn = $("btnSaveConfig");
      if(btn){ btn.disabled = true; btn.textContent = "Saving…"; }
      const res = await fetch(`/admin/update-config?key=${encodeURIComponent(ADMIN_KEY)}`,{
        method:"POST",
        cache:"no-store",
        headers:{ "Content-Type":"application/json" },
        body: JSON.stringify({
          poll_sponsor_text: sponsor,
          match_of_day_id: matchId,
          motd_home: home,
          motd_away: away,
          motd_datetime_utc: kickoff,
          poll_lock_mode: lockMode
        })
      });
      let out=null; try{ out = await res.json(); }catch(e){ const t = await res.text(); out={ok:false,error:(t||'Non-JSON response')}; }
      if(out && out.ok){
        await loadConfig();
        await loadPoll();
      } else {
        alert("Save failed: " + (out.error || "unknown"));
      }
    }catch(e){
      alert("Save failed.");
    }finally{
      const btn = $("btnSaveConfig");
      if(btn){ btn.disabled = false; btn.textContent = "Save settings"; }
    }
  }

  $("btnSaveConfig")?.addEventListener("click", saveConfig);

  loadConfig().then(loadPoll);
  setInterval(loadPoll, 5000);
})();
</script>
""".replace("__ADMIN_KEY__", json.dumps(key)))

    html.append("</div></body></html>")
    return "".join(html)



# ============================================================
# Leads intake (used by the new UI)
# - Stores locally to static/data/leads.jsonl
# - Optionally appends to Google Sheets if configured (same creds as admin/chat)
# ============================================================
LEADS_STORE_PATH = os.environ.get("LEADS_STORE_PATH", "static/data/leads.jsonl")
GOOGLE_SHEET_ID = os.environ.get("GOOGLE_SHEET_ID", "").strip()

def _append_lead_local(row: dict) -> None:
    try:
        os.makedirs(os.path.dirname(LEADS_STORE_PATH), exist_ok=True)
        with open(LEADS_STORE_PATH, "a", encoding="utf-8") as f:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")
    except Exception:
        pass

def _append_lead_google_sheet(row: dict) -> bool:
    try:
        gc = get_gspread_client()
        if GOOGLE_SHEET_ID:
            sh = gc.open_by_key(GOOGLE_SHEET_ID)
        else:
            sh = gc.open(SHEET_NAME)
        # Use a dedicated Leads worksheet if present; fall back to first sheet
        try:
            ws = sh.worksheet("Leads")
        except Exception:
            ws = sh.sheet1
        ws.append_row([
            row.get("ts",""),
            row.get("page",""),
            row.get("intent",""),
            row.get("contact",""),
            row.get("budget",""),
            row.get("party_size",""),
            row.get("datetime",""),
            row.get("notes",""),
            row.get("lang",""),
            row.get("ip",""),
            row.get("ua",""),
        ], value_input_option="USER_ENTERED")
        return True
    except Exception:
        return False

@app.route("/lead", methods=["POST"])
def lead():
    payload = request.get_json(silent=True) or {}
    row = {
        "ts": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "page": (payload.get("page") or "").strip(),
        "intent": (payload.get("intent") or "").strip(),
        "contact": (payload.get("contact") or "").strip(),
        "budget": (payload.get("budget") or "").strip(),
        "party_size": (payload.get("party_size") or "").strip(),
        "datetime": (payload.get("datetime") or "").strip(),
        "notes": (payload.get("notes") or "").strip(),
        "lang": (payload.get("lang") or "").strip(),
        "ip": request.headers.get("X-Forwarded-For", request.remote_addr),
        "ua": request.headers.get("User-Agent",""),
    }
    _append_lead_local(row)
    ok = False
    if GOOGLE_SHEET_ID or os.environ.get("GOOGLE_CREDS_JSON") or os.path.exists("google_creds.json"):
        ok = _append_lead_google_sheet(row)
    return jsonify({"ok": True, "sheet_ok": bool(ok)})


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5050))
    app.run(host="0.0.0.0", port=port, debug=False)
