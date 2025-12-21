from dotenv import load_dotenv
load_dotenv()

import os
import json
import re
import uuid
import time
from datetime import datetime, date, timedelta
from typing import Dict, Any, List, Optional

from flask import Flask, request, jsonify, send_from_directory, make_response
from openai import OpenAI

import gspread
from google.oauth2.service_account import Credentials


# ============================================================
# App + cache busting (fixes Render serving old index.html)
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
# Business profile
# ============================================================
BUSINESS_PROFILE_PATH = "business_profile.txt"
if not os.path.exists(BUSINESS_PROFILE_PATH):
    raise FileNotFoundError("business_profile.txt not found in the same folder as app.py.")

with open(BUSINESS_PROFILE_PATH, "r", encoding="utf-8") as f:
    business_profile = f.read()


# ============================================================
# Business rules (env overrides supported)
# ============================================================
# Default hours (local Dallas time assumed by user; server may run UTC but we use date-only for match-day)
OPEN_TIME_NORMAL = os.environ.get("OPEN_TIME_NORMAL", "11:00")      # HH:MM
CLOSE_TIME_NORMAL = os.environ.get("CLOSE_TIME_NORMAL", "23:00")    # HH:MM
OPEN_TIME_MATCHDAY = os.environ.get("OPEN_TIME_MATCHDAY", "11:00")  # HH:MM
MAX_PARTY_SIZE = int(os.environ.get("MAX_PARTY_SIZE", "12"))

# Closed dates: comma-separated YYYY-MM-DD (e.g., "2026-06-19,2026-07-04")
CLOSED_DATES = set([d.strip() for d in os.environ.get("CLOSED_DATES", "").split(",") if d.strip()])

# Optional: weekly closed days "Mon,Tue" etc
CLOSED_WEEKDAYS = set([d.strip().lower() for d in os.environ.get("CLOSED_WEEKDAYS", "").split(",") if d.strip()])

BUSINESS_PHONE_FALLBACK = os.environ.get("BUSINESS_PHONE_FALLBACK", "")


# ============================================================
# Google Sheets (read/write)
# ============================================================
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

    # local fallback
    creds_file = "google_creds.json"
    if os.path.exists(creds_file):
        creds = Credentials.from_service_account_file(creds_file, scopes=SCOPES)
        return gspread.authorize(creds)

    raise RuntimeError("Google credentials not found. Set GOOGLE_CREDS_JSON (Render) or provide google_creds.json locally.")

def append_lead_to_sheet(lead: Dict[str, Any]):
    gc = get_gspread_client()
    ws = gc.open(SHEET_NAME).sheet1

    # Ensure header exists
    if ws.row_count < 1 or (ws.get("A1:F1") == [[]]):
        ws.append_row(["timestamp", "name", "phone", "date", "time", "party_size"])

    ws.append_row([
        datetime.now().isoformat(timespec="seconds"),
        lead["name"],
        lead["phone"],
        lead["date"],
        lead["time"],
        int(lead["party_size"]),
    ])

def read_leads(limit: int = 200) -> List[List[str]]:
    gc = get_gspread_client()
    ws = gc.open(SHEET_NAME).sheet1
    values = ws.get_all_values()
    if not values:
        return []
    header, rows = values[0], values[1:]
    rows = rows[-limit:]
    return [header] + rows


# ============================================================
# Dallas match schedule (server-driven)
# Source for dates/opponents: MLSsoccer "Every game by city & stadium" (Dallas AT&T Stadium).
# You can edit/extend this list any time, and the UI updates automatically.
# ============================================================
DALLAS_MATCHES = [
    # Group stage (dates from MLSSoccer Dallas section)
    {"date": "2026-06-14", "time": "TBD", "home": "Netherlands", "away": "Japan", "stage": "Group F"},
    {"date": "2026-06-17", "time": "TBD", "home": "England", "away": "Croatia", "stage": "Group L"},
    {"date": "2026-06-22", "time": "TBD", "home": "Argentina", "away": "Austria", "stage": "Group J"},
    {"date": "2026-06-25", "time": "TBD", "home": "Japan", "away": "UEFA Playoff B Winner", "stage": "Group F"},
    {"date": "2026-06-27", "time": "TBD", "home": "Jordan", "away": "Argentina", "stage": "Group J"},
    # Knockout stage (Round names from the same Dallas listing)
    {"date": "2026-06-30", "time": "TBD", "home": "Group E runner-up", "away": "Group I runner-up", "stage": "Round of 32"},
    {"date": "2026-07-03", "time": "TBD", "home": "Group D runner-up", "away": "Group G runner-up", "stage": "Round of 32"},
    {"date": "2026-07-06", "time": "TBD", "home": "Winner Match 83", "away": "Winner Match 84", "stage": "Round of 16"},
    {"date": "2026-07-14", "time": "TBD", "home": "Winner Match 97", "away": "Winner Match 98", "stage": "Semifinal"},
]


# ============================================================
# Sessions (state machine reservation flow)
# ============================================================
# In-memory sessions: good for MVP. For production scaling later, move to Redis.
SESSIONS: Dict[str, Dict[str, Any]] = {}

def get_or_create_sid() -> str:
    sid = request.cookies.get("sid")
    if sid and sid in SESSIONS:
        return sid
    sid = str(uuid.uuid4())
    SESSIONS[sid] = {
        "history": [],
        "language": "auto",   # auto | en | es | pt | fr
        "draft": {"name": None, "phone": None, "date": None, "time": None, "party_size": None},
    }
    return sid

def normalize_phone(s: str) -> Optional[str]:
    digits = re.sub(r"\D+", "", s or "")
    if len(digits) == 10:
        return digits
    if len(digits) == 11 and digits.startswith("1"):
        return digits[1:]
    return None

MONTHS = {
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

def parse_date_text(text: str) -> Optional[str]:
    """
    Accepts:
      - YYYY-MM-DD
      - MM/DD/YYYY
      - 'June 23', 'Jun 23', 'June 23 2026'
    Defaults year to 2026 if missing (World Cup context).
    """
    t = (text or "").strip().lower()

    # YYYY-MM-DD
    m = re.search(r"\b(20\d{2})-(\d{2})-(\d{2})\b", t)
    if m:
        y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
        try:
            return date(y, mo, d).isoformat()
        except ValueError:
            return None

    # MM/DD/YYYY
    m = re.search(r"\b(\d{1,2})/(\d{1,2})/(20\d{2})\b", t)
    if m:
        mo, d, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
        try:
            return date(y, mo, d).isoformat()
        except ValueError:
            return None

    # Month name + day (+ optional year)
    m = re.search(r"\b([a-z]{3,9})\s+(\d{1,2})(?:\s*,?\s*(20\d{2}))?\b", t)
    if m:
        mon_raw = m.group(1).lower()
        day = int(m.group(2))
        y = int(m.group(3)) if m.group(3) else 2026
        mo = MONTHS.get(mon_raw[:3], MONTHS.get(mon_raw))
        if not mo:
            return None
        try:
            return date(y, mo, day).isoformat()
        except ValueError:
            return None

    return None

def parse_time_text(text: str) -> Optional[str]:
    """
    Accepts:
      - '7pm', '7:30 pm', '19:00'
    Returns a display-friendly string like '7:00 PM'
    """
    t = (text or "").strip().lower()
    # 24h
    m = re.search(r"\b([01]?\d|2[0-3]):([0-5]\d)\b", t)
    if m:
        hh, mm = int(m.group(1)), m.group(2)
        ampm = "AM" if hh < 12 else "PM"
        hh12 = hh % 12
        if hh12 == 0:
            hh12 = 12
        return f"{hh12}:{mm} {ampm}"

    # 12h
    m = re.search(r"\b(\d{1,2})(?::([0-5]\d))?\s*(am|pm)\b", t)
    if m:
        hh = int(m.group(1))
        mm = m.group(2) or "00"
        ampm = m.group(3).upper()
        if hh < 1 or hh > 12:
            return None
        return f"{hh}:{mm} {ampm}"

    return None

def parse_party_size(text: str) -> Optional[int]:
    t = (text or "").lower()
    m = re.search(r"\bparty\s*(?:of|size)?\s*(\d{1,2})\b", t)
    if m:
        return int(m.group(1))
    m = re.search(r"\btable\s*(?:for|of)?\s*(\d{1,2})\b", t)
    if m:
        return int(m.group(1))
    # lone number fallback (careful)
    m = re.search(r"\b(\d{1,2})\b", t)
    if m and ("people" in t or "guests" in t or "party" in t or "table" in t):
        return int(m.group(1))
    return None

def is_match_day(iso_date: str) -> bool:
    return any(m["date"] == iso_date for m in DALLAS_MATCHES)

def within_business_rules(draft: Dict[str, Any]) -> (bool, str):
    """
    Validate: max party size, closed days/dates, time within hours (light validation).
    """
    # party size
    ps = draft.get("party_size")
    if isinstance(ps, int) and ps > MAX_PARTY_SIZE:
        return False, f"Max party size is {MAX_PARTY_SIZE}. Please choose a smaller party size."

    # date closed
    d = draft.get("date")
    if d:
        if d in CLOSED_DATES:
            return False, f"We’re closed on {d}. Please choose another date."
        try:
            dt = date.fromisoformat(d)
            weekday = dt.strftime("%a").lower()  # mon/tue...
            if weekday in CLOSED_WEEKDAYS:
                return False, f"We’re closed on {dt.strftime('%A')}s. Please choose another date."
        except Exception:
            pass

    # time window (simple check when both date/time exist)
    t = draft.get("time")
    if d and t:
        # Very light check: if user gives a time we accept; for strict enforcement we’d parse to minutes
        # and compare to OPEN/CLOSE. We’ll implement strict parsing here anyway.
        open_str = OPEN_TIME_MATCHDAY if is_match_day(d) else OPEN_TIME_NORMAL
        close_str = CLOSE_TIME_NORMAL

        def hhmm_to_minutes(hhmm: str) -> int:
            hh, mm = hhmm.split(":")
            return int(hh) * 60 + int(mm)

        def time_to_minutes(tstr: str) -> Optional[int]:
            # expects like "7:00 PM"
            m = re.match(r"^\s*(\d{1,2}):(\d{2})\s*(AM|PM)\s*$", tstr.strip(), re.I)
            if not m:
                return None
            hh, mm, ap = int(m.group(1)), int(m.group(2)), m.group(3).upper()
            if hh == 12:
                hh = 0
            mins = hh * 60 + mm
            if ap == "PM":
                mins += 12 * 60
            return mins

        open_m = hhmm_to_minutes(open_str)
        close_m = hhmm_to_minutes(close_str)
        req_m = time_to_minutes(t)
        if req_m is not None:
            if req_m < open_m or req_m > close_m:
                return False, f"Our hours are {open_str}–{close_str}. Please choose a time within business hours."

    return True, "OK"

def draft_summary(draft: Dict[str, Any]) -> str:
    def val(x): return x if x else "—"
    return (
        "Reservation so far:\n"
        f"- Date: {val(draft.get('date'))}\n"
        f"- Time: {val(draft.get('time'))}\n"
        f"- Party size: {val(draft.get('party_size'))}\n"
        f"- Name: {val(draft.get('name'))}\n"
        f"- Phone: {val(draft.get('phone'))}"
    )

def missing_fields(draft: Dict[str, Any]) -> List[str]:
    return [k for k in ["date", "time", "party_size", "name", "phone"] if not draft.get(k)]

def next_question_for_missing(missing: List[str], language: str) -> str:
    # simple bilingual-ish prompts; most of the time the model will translate, but we keep it simple
    prompts = {
        "date": "What date would you like? (Example: June 23, 2026)",
        "time": "What time? (Example: 7pm)",
        "party_size": f"How many people? (Max {MAX_PARTY_SIZE})",
        "name": "Name for the reservation?",
        "phone": "Phone number (10 digits)?",
    }
    return prompts[missing[0]] if missing else "All set."

def detect_recall_intent(text: str) -> bool:
    t = (text or "").lower().strip()
    return ("recall" in t and "reservation" in t) or t in ("/recall", "recall")

def detect_language_toggle(text: str) -> Optional[str]:
    t = (text or "").lower()
    if "respond in spanish" in t or t == "/es":
        return "es"
    if "respond in english" in t or t == "/en":
        return "en"
    if "respond in portuguese" in t or t == "/pt":
        return "pt"
    if "respond in french" in t or t == "/fr":
        return "fr"
    return None


# ============================================================
# Rate limiting (simple in-memory per IP)
# ============================================================
RATE_WINDOW_SECONDS = int(os.environ.get("RATE_WINDOW_SECONDS", "300"))  # 5 minutes
RATE_MAX_REQUESTS = int(os.environ.get("RATE_MAX_REQUESTS", "30"))       # 30 requests/5 min

RATE_BUCKET: Dict[str, List[float]] = {}

def rate_limit_ok(ip: str) -> bool:
    now = time.time()
    bucket = RATE_BUCKET.get(ip, [])
    bucket = [ts for ts in bucket if now - ts < RATE_WINDOW_SECONDS]
    if len(bucket) >= RATE_MAX_REQUESTS:
        RATE_BUCKET[ip] = bucket
        return False
    bucket.append(now)
    RATE_BUCKET[ip] = bucket
    return True


# ============================================================
# Routes
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
            "index_html_last_modified": datetime.fromtimestamp(mtime).isoformat(timespec="seconds")
        })
    except Exception as e:
        return jsonify({"error": repr(e)}), 500

@app.route("/schedule")
def schedule():
    # Server-driven Dallas schedule
    return jsonify({"city": "Dallas (AT&T Stadium, Arlington)", "matches": DALLAS_MATCHES})

@app.route("/rules")
def rules():
    # Used by UI for banners/hours/max party
    return jsonify({
        "open_time_normal": OPEN_TIME_NORMAL,
        "close_time_normal": CLOSE_TIME_NORMAL,
        "open_time_matchday": OPEN_TIME_MATCHDAY,
        "max_party_size": MAX_PARTY_SIZE,
    })

@app.route("/test-sheet")
def test_sheet():
    try:
        append_lead_to_sheet({
            "name": "TEST_NAME",
            "phone": "2145551212",
            "date": "2026-06-23",
            "time": "7:00 PM",
            "party_size": 4
        })
        return jsonify({"ok": True, "sheet": SHEET_NAME, "message": "✅ Test row appended."})
    except Exception as e:
        return jsonify({"ok": False, "sheet": SHEET_NAME, "error": repr(e)}), 500

@app.route("/admin")
def admin():
    """
    Live leads viewer (reads from Google Sheet).
    Protect this later with a password. MVP: open.
    """
    try:
        data = read_leads(limit=250)
        if not data:
            return "<h2>No leads yet.</h2>"

        header = data[0]
        rows = data[1:]

        html = """
        <html><head><meta charset="utf-8"/>
        <title>Admin - Leads</title>
        <style>
          body { font-family: Arial, sans-serif; padding: 18px; }
          table { border-collapse: collapse; width: 100%; }
          th, td { border: 1px solid #ddd; padding: 8px; font-size: 13px; }
          th { background: #f4f4f4; text-align: left; }
          .muted { color: #666; font-size: 12px; }
        </style></head><body>
        <h2>Leads (Google Sheet)</h2>
        <div class="muted">Sheet: """ + SHEET_NAME + """</div>
        <br/>
        <table>
          <thead><tr>
        """
        for col in header:
            html += f"<th>{col}</th>"
        html += "</tr></thead><tbody>"

        for r in reversed(rows):
            html += "<tr>" + "".join(f"<td>{(c or '')}</td>" for c in r) + "</tr>"

        html += "</tbody></table></body></html>"
        return html

    except Exception as e:
        return f"<h2>Admin error</h2><pre>{repr(e)}</pre>", 500

@app.route("/chat", methods=["POST"])
def chat():
    ip = request.headers.get("X-Forwarded-For", request.remote_addr) or "unknown"
    if not rate_limit_ok(ip):
        return jsonify({"reply": "⚠️ Too many requests. Please wait a moment and try again."}), 429

    sid = get_or_create_sid()
    sess = SESSIONS[sid]

    data = request.get_json(force=True)
    user_message = (data.get("message") or "").strip()
    history = data.get("history") or []
    ui_lang = (data.get("lang") or "").strip().lower()  # en/es/pt/fr

    if not user_message:
        resp = make_response(jsonify({"reply": "Please type a message."}))
        resp.set_cookie("sid", sid, max_age=60*60*24*14)  # 14 days
        return resp

    # Apply language toggle from UI or text command
    if ui_lang in ("en", "es", "pt", "fr"):
        sess["language"] = ui_lang
    cmd_lang = detect_language_toggle(user_message)
    if cmd_lang:
        sess["language"] = cmd_lang
        user_message = "OK."  # prevent confusing the flow

    # Recall reservation so far
    if detect_recall_intent(user_message):
        reply = draft_summary(sess["draft"])
        resp = make_response(jsonify({"reply": reply}))
        resp.set_cookie("sid", sid, max_age=60*60*24*14)
        return resp

    # Reservation state machine extraction
    draft = sess["draft"]

    # Fill from message
    # Date
    if not draft.get("date"):
        d = parse_date_text(user_message)
        if d:
            draft["date"] = d

    # Time
    if not draft.get("time"):
        tm = parse_time_text(user_message)
        if tm:
            draft["time"] = tm

    # Party size
    if not draft.get("party_size"):
        ps = parse_party_size(user_message)
        if ps:
            draft["party_size"] = ps

    # Phone
    if not draft.get("phone"):
        ph = normalize_phone(user_message)
        if ph:
            draft["phone"] = ph

    # Name (simple heuristic: single word/short phrase, avoid capturing "reservation")
    if not draft.get("name"):
        t = user_message.strip()
        if 1 <= len(t) <= 40 and re.search(r"[a-zA-Z]", t) and not any(w in t.lower() for w in ["reservation", "table", "party", "pm", "am", "june", "july"]):
            # don't treat phone as name
            if not normalize_phone(t):
                draft["name"] = t

    # Business rules validation (when some fields exist)
    ok, msg = within_business_rules(draft)
    if not ok:
        # If invalid, keep draft but ask to correct
        reply = f"⚠️ {msg}\n\n{draft_summary(draft)}"
        resp = make_response(jsonify({"reply": reply}))
        resp.set_cookie("sid", sid, max_age=60*60*24*14)
        return resp

    # If missing fields, ask the next missing field (deterministic, no model needed)
    miss = missing_fields(draft)
    if miss:
        # But still answer general questions using model if user isn't clearly booking
        if user_message.lower() in ("hello", "hi", "hey"):
            reply = "Hi! Want to book a table for a match day? Ask me anything."
        else:
            reply = next_question_for_missing(miss, sess["language"])
            reply += "\n\n(You can also type: “Recall reservation so far”)"
        resp = make_response(jsonify({"reply": reply}))
        resp.set_cookie("sid", sid, max_age=60*60*24*14)
        return resp

    # All fields collected -> save + confirm
    lead = {
        "name": draft["name"],
        "phone": draft["phone"],
        "date": draft["date"],
        "time": draft["time"],
        "party_size": int(draft["party_size"]),
    }

    try:
        append_lead_to_sheet(lead)
        # reset draft after save
        sess["draft"] = {"name": None, "phone": None, "date": None, "time": None, "party_size": None}
        reply = (
            "✅ Reservation saved!\n"
            f"Name: {lead['name']}\n"
            f"Phone: {lead['phone']}\n"
            f"Date: {lead['date']}\n"
            f"Time: {lead['time']}\n"
            f"Party size: {lead['party_size']}"
        )
    except Exception as e:
        reply = f"⚠️ Could not save right now. {repr(e)}"
        if BUSINESS_PHONE_FALLBACK:
            reply += f"\nPlease call: {BUSINESS_PHONE_FALLBACK}"

    resp = make_response(jsonify({"reply": reply}))
    resp.set_cookie("sid", sid, max_age=60*60*24*14)
    return resp


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5050))
    app.run(host="0.0.0.0", port=port, debug=False)