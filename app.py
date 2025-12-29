import os
import json
import time
from typing import Any, Dict, List, Optional

from flask import Flask, jsonify, request, send_from_directory

# Optional (used if available)
import requests

app = Flask(__name__, static_folder="static", static_url_path="/static")

# ============================================================
# Mobile-first: always serve index.html at /
# ============================================================
@app.get("/")
def home():
    # Render will serve from repo root by default; keep it simple.
    return send_from_directory(".", "index.html")

# ============================================================
# Languages (4)
# ============================================================
SUPPORTED_LANGS = {"en", "es", "pt", "fr"}

def norm_lang(v: Optional[str]) -> str:
    v = (v or "en").strip().lower()
    return v if v in SUPPORTED_LANGS else "en"

# ============================================================
# Menu (demo premium items; replace later with your real partners)
# ============================================================
MENU_DEMO = {
    "en": [
        {"category": "Reservations", "name": "VIP Table Inquiry", "price": "", "description": "Tell us your city, venue, date/time, and party size."},
        {"category": "Reservations", "name": "Group Booking", "price": "", "description": "Concierge support for groups, corporate, and match-day traffic."},
        {"category": "Experiences", "name": "Premium Fan Experience", "price": "", "description": "Curated fan zones, watch parties, and stadium-area recommendations."},
    ],
    "es": [
        {"category": "Reservas", "name": "Consulta de Mesa VIP", "price": "", "description": "Indica ciudad, lugar, fecha/hora y tamaño del grupo."},
        {"category": "Reservas", "name": "Reserva para Grupos", "price": "", "description": "Soporte concierge para grupos, empresas y días de partido."},
        {"category": "Experiencias", "name": "Experiencia Premium", "price": "", "description": "Fan zones, watch parties y recomendaciones cerca del estadio."},
    ],
    "pt": [
        {"category": "Reservas", "name": "Consulta de Mesa VIP", "price": "", "description": "Informe cidade, local, data/hora e número de pessoas."},
        {"category": "Reservas", "name": "Reserva para Grupos", "price": "", "description": "Concierge para grupos, empresas e dias de jogo."},
        {"category": "Experiências", "name": "Experiência Premium", "price": "", "description": "Fan zones, watch parties e dicas perto do estádio."},
    ],
    "fr": [
        {"category": "Réservations", "name": "Demande de Table VIP", "price": "", "description": "Ville, lieu, date/heure et taille du groupe."},
        {"category": "Réservations", "name": "Réservation Groupe", "price": "", "description": "Conciergerie pour groupes, entreprises et jours de match."},
        {"category": "Expériences", "name": "Expérience Premium", "price": "", "description": "Fan zones, watch parties et recommandations près du stade."},
    ],
}

@app.get("/menu.json")
def menu_json():
    lang = norm_lang(request.args.get("lang"))
    return jsonify({"items": MENU_DEMO.get(lang, MENU_DEMO["en"])})

# ============================================================
# Fan Zone (demo events; you can later connect to partners/feeds)
# ============================================================
FANZONE_DEMO = {
    "en": [
        {"date": "2026-06-11", "city": "Host City", "title": "Official Fan Zone", "location": "City Center", "description": "Live screenings, music, and food."},
        {"date": "2026-06-12", "city": "Host City", "title": "Watch Party (Premium)", "location": "Partner Venue", "description": "Reservations recommended."},
    ],
    "es": [
        {"date": "2026-06-11", "city": "Ciudad Sede", "title": "Fan Zone Oficial", "location": "Centro", "description": "Pantallas, música y comida."},
        {"date": "2026-06-12", "city": "Ciudad Sede", "title": "Watch Party (Premium)", "location": "Lugar Asociado", "description": "Se recomienda reservar."},
    ],
    "pt": [
        {"date": "2026-06-11", "city": "Cidade-Sede", "title": "Fan Zone Oficial", "location": "Centro", "description": "Transmissão ao vivo, música e comida."},
        {"date": "2026-06-12", "city": "Cidade-Sede", "title": "Watch Party (Premium)", "location": "Local Parceiro", "description": "Reservas recomendadas."},
    ],
    "fr": [
        {"date": "2026-06-11", "city": "Ville Hôte", "title": "Fan Zone Officielle", "location": "Centre-ville", "description": "Diffusion live, musique et food."},
        {"date": "2026-06-12", "city": "Ville Hôte", "title": "Watch Party (Premium)", "location": "Lieu Partenaire", "description": "Réservation conseillée."},
    ],
}

@app.get("/fanzone.json")
def fanzone_json():
    lang = norm_lang(request.args.get("lang"))
    return jsonify({"events": FANZONE_DEMO.get(lang, FANZONE_DEMO["en"])})

# ============================================================
# Schedule
# - Tries a public feed (optional) then falls back to demo data
# - scope=global|city  (default global)
# - city=<city name> (optional; only used when scope=city)
# ============================================================
FIXTURE_FEED_URL = os.environ.get("FIXTURE_FEED_URL", "").strip()
SCHEDULE_CACHE_TTL = int(os.environ.get("SCHEDULE_CACHE_TTL", "1800"))  # 30m
_cache: Dict[str, Any] = {"ts": 0, "matches": []}

DEMO_MATCHES: List[Dict[str, Any]] = [
    {"date": "2026-06-11", "time": "19:00", "group": "Group A", "home": "Team A", "away": "Team B", "venue": "Stadium 1", "city": "Host City"},
    {"date": "2026-06-12", "time": "16:00", "group": "Group B", "home": "Team C", "away": "Team D", "venue": "Stadium 2", "city": "Host City"},
    {"date": "2026-06-13", "time": "21:00", "group": "Group C", "home": "Team E", "away": "Team F", "venue": "Stadium 3", "city": "Another City"},
]

def _parse_fixture_feed(data: Any) -> List[Dict[str, Any]]:
    # fixturedownload feeds often return a list of dicts with keys like:
    # DateUtc, Location, HomeTeam, AwayTeam, Group, RoundNumber, etc.
    if not isinstance(data, list):
        return []
    out: List[Dict[str, Any]] = []
    for row in data:
        if not isinstance(row, dict):
            continue
        date_utc = str(row.get("DateUtc") or "").strip()
        # Convert "YYYY-MM-DD HH:MM:SSZ" -> date/time strings (keep simple)
        date = ""
        time_s = ""
        if date_utc:
            # tolerate multiple formats
            parts = date_utc.replace("T", " ").replace("Z", "").split()
            if len(parts) >= 1:
                date = parts[0]
            if len(parts) >= 2:
                time_s = parts[1][:5]
        out.append({
            "date": date or str(row.get("Date") or ""),
            "time": time_s or str(row.get("Time") or ""),
            "group": str(row.get("Group") or row.get("Stage") or row.get("RoundName") or ""),
            "stage": str(row.get("RoundName") or ""),
            "home": str(row.get("HomeTeam") or row.get("Home") or ""),
            "away": str(row.get("AwayTeam") or row.get("Away") or ""),
            "venue": str(row.get("Location") or row.get("Venue") or row.get("Stadium") or ""),
            "city": str(row.get("City") or ""),
        })
    # Drop totally blank rows
    out = [m for m in out if (m.get("home") or m.get("away") or m.get("venue") or m.get("date"))]
    return out

def _fetch_schedule_from_feed() -> List[Dict[str, Any]]:
    if not FIXTURE_FEED_URL:
        return []
    try:
        r = requests.get(FIXTURE_FEED_URL, timeout=10)
        r.raise_for_status()
        return _parse_fixture_feed(r.json())
    except Exception:
        return []

def get_schedule_matches() -> List[Dict[str, Any]]:
    now = time.time()
    if (now - _cache["ts"]) < SCHEDULE_CACHE_TTL and _cache["matches"]:
        return _cache["matches"]
    matches = _fetch_schedule_from_feed()
    if not matches:
        matches = DEMO_MATCHES
    _cache["ts"] = now
    _cache["matches"] = matches
    return matches

@app.get("/schedule.json")
def schedule_json():
    scope = (request.args.get("scope") or "global").strip().lower()
    city = (request.args.get("city") or "").strip().lower()

    matches = get_schedule_matches()

    if scope == "city" and city:
        matches = [m for m in matches if city in str(m.get("city","")).lower() or city in str(m.get("venue","")).lower()]

    # Basic match-day signal: any match today in the current scope
    today = time.strftime("%Y-%m-%d", time.gmtime())
    is_matchday = any((m.get("date") == today) for m in matches)

    return jsonify({"matches": matches, "is_matchday": is_matchday})

# ============================================================
# Chat (simple; uses OpenAI if key exists, else a premium fallback)
# ============================================================
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "").strip()

def fallback_reply(msg: str, lang: str) -> str:
    # Lux, concierge tone with multilingual support message
    if lang == "es":
        return "Perfecto. Dime tu ciudad, lugar/área, fecha/hora y tamaño del grupo. También puedo ayudarte con partidos, fan zones y VIP."
    if lang == "pt":
        return "Perfeito. Diga sua cidade, local/área, data/hora e tamanho do grupo. Também ajudo com jogos, fan zones e VIP."
    if lang == "fr":
        return "Parfait. Indique la ville, le lieu/zone, la date/heure et la taille du groupe. Je peux aussi aider pour matchs, fan zones et VIP."
    return "Perfect. Tell me the city, venue/area, date/time, and party size. I can also help with matches, fan zones, and VIP."

@app.post("/chat")
def chat():
    payload = request.get_json(silent=True) or {}
    user_msg = str(payload.get("message") or "").strip()
    lang = norm_lang(payload.get("language"))
    if not user_msg:
        return jsonify({"reply": fallback_reply("", lang)})

    # If no API key, return a clean fallback so app stays functional
    if not OPENAI_API_KEY:
        return jsonify({"reply": fallback_reply(user_msg, lang)})

    # Optional: integrate OpenAI SDK if you want; for now keep safe fallback
    return jsonify({"reply": fallback_reply(user_msg, lang)})

# ============================================================
# Health
# ============================================================

# ============================================================
# Version
# ============================================================
@app.get('/version')
def version():
    return jsonify({'build':'STEP7-20251229-174837'})
@app.get("/health")
def health():
    return jsonify({"ok": True})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "10000")))
