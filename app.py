from dotenv import load_dotenv
load_dotenv()

import os
import pathlib

def _env_bool(name: str, default: bool = False) -> bool:
    """Parse boolean env vars safely."""
    try:
        v = os.environ.get(name)
        if v is None:
            return default
        s = str(v).strip().lower()
        if s == "":
            return default
        return s in ("1", "true", "yes", "y", "on")
    except Exception:
        return default

# Multi-venue flag (back-compat):
# - Preferred: MULTI_VENUE=1
# - Legacy: MULTI_VENUE_ENABLED=1
MULTI_VENUE = _env_bool("MULTI_VENUE", default=_env_bool("MULTI_VENUE_ENABLED", default=False))
MULTI_VENUE_ENABLED = MULTI_VENUE

import html
import traceback
import json
import csv
import io
import hashlib
import hmac
import base64
import secrets
import re
import time
import datetime
from datetime import datetime, date, timezone, timedelta
from typing import Dict, Any, Optional, List, Tuple

import time
import urllib.request
import urllib.error
from flask import Flask, request, jsonify, send_from_directory, send_file, make_response, g, render_template, render_template_string, redirect

# ============================================================
# Enterprise persistence: Redis (optional, recommended)
# - Enable by setting REDIS_URL (managed Redis)
# - If REDIS_URL is not set or redis lib missing, falls back to filesystem (/tmp)
# ============================================================

# ===== Build/version (single source of truth; NEVER redefine elsewhere) =====
CODE_VERSION = "1.4.9"
APP_VERSION = (os.environ.get("APP_VERSION") or CODE_VERSION).strip()

REDIS_URL = os.environ.get("REDIS_URL", "").strip()
_REDIS = None
_REDIS_ENABLED = False
try:
    import redis  # type: ignore
    if REDIS_URL:
        _REDIS = redis.from_url(REDIS_URL, decode_responses=True)
        _REDIS.ping()
        _REDIS_ENABLED = True
except Exception:
    _REDIS = None
    _REDIS_ENABLED = False

# Namespace for keys (safe for multi-app reuse)
_REDIS_NS = os.environ.get("REDIS_NAMESPACE", "wc26").strip() or "wc26"

# CI smoke file/key for enterprise gate (safe; does not touch production data)
CI_SMOKE_FILE = os.environ.get("CI_SMOKE_FILE", "/tmp/wc26_ci_smoke.json")

# Track if Redis write failed and we fell back to disk (enterprise detector)
_REDIS_FALLBACK_USED = False
_REDIS_FALLBACK_LAST_PATH = ""

# Map our on-disk JSON files to Redis keys when enabled (single source of truth)
# NOTE: These files still exist for local/dev fallback.
# Drafts: per-venue only. Override DRAFTS_FILE for localhost (e.g. Windows) or production path.
DRAFTS_FILE = os.environ.get("DRAFTS_FILE", "/tmp/wc26_{venue}_drafts.json")
_REDIS_PATH_KEY_MAP = {  # values are suffixes; full key includes namespace + venue
    os.environ.get("AI_QUEUE_FILE", "/tmp/wc26_{venue}_ai_queue.json"): "ai_queue",
    DRAFTS_FILE: "drafts",
    os.environ.get("AI_SETTINGS_FILE", "/tmp/wc26_{venue}_ai_settings.json"): "ai_settings",
    os.environ.get("PARTNER_POLICIES_FILE", "/tmp/wc26_{venue}_partner_policies.json"): "partner_policies",
    os.environ.get("BUSINESS_RULES_FILE", "/tmp/wc26_{venue}_business_rules.json"): "business_rules",
    os.environ.get("MENU_FILE", "/tmp/wc26_{venue}_menu_override.json"): "menu_override",
    os.environ.get("ALERT_SETTINGS_FILE", "/tmp/wc26_{venue}_alert_settings.json"): "alert_settings",
    os.environ.get("ALERT_STATE_FILE", "/tmp/wc26_{venue}_alert_state.json"): "alert_state",
    os.environ.get("POLL_STORE_FILE", "/tmp/wc26_{venue}_poll_votes.json"): "poll_store",
    os.environ.get("FIXTURE_CACHE_FILE", "/tmp/wc26_{venue}_fixtures.json"): "fixtures_cache",
}

def _redis_init_if_needed() -> None:
    """Ensure Redis is initialized under Gunicorn.

    Some deploy configs can import modules in a way that bypasses earlier init blocks.
    This function is safe to call multiple times.
    """
    global _REDIS, _REDIS_ENABLED
    if _REDIS_ENABLED:
        return
    url = (os.environ.get("REDIS_URL", "") or "").strip()
    if not url:
        return
    try:
        import redis  # type: ignore
        r = redis.from_url(url, decode_responses=True)
        r.ping()
        _REDIS = r
        _REDIS_ENABLED = True
        globals()["_REDIS_URL_EFFECTIVE"] = url
        globals()["_REDIS_ERROR"] = ""
        try:
            print(f"[REDIS] enabled (late) url={url.split('@')[-1]}")
        except Exception:
            pass
        return
    except Exception as e:
        globals()["_REDIS_ERROR"] = repr(e)
        globals()["_REDIS_URL_EFFECTIVE"] = url
        # TLS fallback
        try:
            if url.startswith("redis://"):
                import redis  # type: ignore
                r = redis.from_url("rediss://" + url[len("redis://"):], decode_responses=True)
                r.ping()
                _REDIS = r
                _REDIS_ENABLED = True
                globals()["_REDIS_URL_EFFECTIVE"] = "rediss://" + url[len("redis://"):]
                globals()["_REDIS_ERROR"] = ""
                try:
                    print(f"[REDIS] enabled (late rediss) url={url.split('@')[-1]}")
                except Exception:
                    pass
        except Exception as e2:
            globals()["_REDIS_ERROR"] = repr(e2)
            try:
                print(f"[REDIS] disabled err={globals()['_REDIS_ERROR']}")
            except Exception:
                pass

# run once at import (Gunicorn-safe)
try:
    _redis_init_if_needed()
except Exception:
    pass

def _redis_get_json(key: str, default=None):
    if not (_REDIS_ENABLED and _REDIS and key):
        return default
    try:
        raw = _REDIS.get(key)
        if not raw:
            return default
        return json.loads(raw)
    except Exception:
        return default

def _redis_set_json(key: str, payload) -> bool:
    if not (_REDIS_ENABLED and _REDIS and key):
        return False
    try:
        _REDIS.set(key, json.dumps(payload, ensure_ascii=False))
        return True
    except Exception:
        return False


# OpenAI client (compat: works with both newer and older openai python packages)
# NOTE: We keep the server running even if OpenAI SDK isn't installed.
# Chat endpoints will return a clear config error instead of crashing the whole app.
client = None
_OPENAI_MODE = "missing"
_OPENAI_AVAILABLE = False
try:
    from openai import OpenAI  # new SDK
    client = OpenAI()
    _OPENAI_MODE = "new"
    _OPENAI_AVAILABLE = True
except Exception:
    try:
        import openai  # legacy SDK
        _OPENAI_MODE = "legacy"
        _OPENAI_AVAILABLE = True

        class _CompatResponses:
            @staticmethod
            def create(model: str, input):
                messages = input
                r = openai.ChatCompletion.create(model=model, messages=messages)

                class _Resp:
                    pass

                resp = _Resp()
                resp.output_text = (r["choices"][0]["message"]["content"] or "")
                return resp

        class _CompatClient:
            responses = _CompatResponses()

        client = _CompatClient()
    except Exception:
        # Keep app alive; chat will gracefully error.
        client = None
        _OPENAI_MODE = "missing"
        _OPENAI_AVAILABLE = False
try:
    import gspread
    from google.oauth2.service_account import Credentials
    _GSPREAD_AVAILABLE = True
except Exception:
    gspread = None
    Credentials = None
    _GSPREAD_AVAILABLE = False

# ============================================================
# App + cache-busting (helps Render show latest index.html)
#
# IMPORTANT: make template/static resolution robust across:
# - `python app.py`
# - `gunicorn app:app`
# - dynamic loaders / CI import checks
# ============================================================
_BASE_DIR = os.path.dirname(os.path.abspath(__file__))
_TEMPLATE_DIR = os.path.join(_BASE_DIR, "templates")
_STATIC_DIR = os.path.join(_BASE_DIR, "static")

# Local reservations when Google Sheets is not configured (no Google Cloud project needed)
RESERVATIONS_LOCAL_PATH = os.environ.get("RESERVATIONS_LOCAL_PATH", "data/reservations.jsonl")


def _generate_reservation_id() -> str:
    """Unique ID for a reservation (e.g. WC-A1B2C3D4). User can recall with this."""
    return "WC-" + secrets.token_hex(4).upper()


def _append_reservation_local(lead: dict, reservation_id: Optional[str] = None) -> None:
    """Save one reservation to local JSONL. reservation_id is required for recall by ID."""
    try:
        path = os.path.join(_BASE_DIR, RESERVATIONS_LOCAL_PATH)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        rid = (reservation_id or lead.get("reservation_id") or "").strip()
        
        # Enhanced reservation structure with tier/category and other metadata
        row = {
            "reservation_id": rid,
            "timestamp": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            # Per-venue isolation: tag each local reservation with its venue_id.
            "venue_id": _venue_id(),
            "name": lead.get("name", ""),
            "phone": lead.get("phone", ""),
            "date": lead.get("date", ""),
            "time": lead.get("time", ""),
            "party_size": lead.get("party_size", 0),
            "language": lead.get("language", "en"),
            "status": (lead.get("status") or "New").strip() or "New",
            "tier": (lead.get("tier") or "regular").strip().lower() or "regular",  # New: tier/category field
            "vip": "Yes" if str(lead.get("vip") or "").strip().lower() in ("1", "true", "yes", "y") else "No",
            "budget": lead.get("budget", ""),
            "notes": lead.get("notes", ""),
            "vibe": lead.get("vibe", ""),  # e.g., "VIP Vibe", "Premium", "Standard"
        }
        with open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")
    except Exception:
        pass


def _get_reservation_by_id(reservation_id: str) -> Optional[Dict[str, Any]]:
    """Find reservation by unique ID: local file first, then Google Sheets (if available)."""
    rid = (reservation_id or "").strip().upper()
    if not rid:
        return None
    # Current venue context (used for isolation across venues)
    try:
        current_vid = _slugify_venue_id(_venue_id())
    except Exception:
        current_vid = DEFAULT_VENUE_ID
    # Normalize: allow "WC-abc123" or "abc123"
    if not rid.startswith("WC-"):
        rid = "WC-" + rid
    # 1) Local file
    try:
        path = os.path.join(_BASE_DIR, RESERVATIONS_LOCAL_PATH)
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                for line in reversed(list(f)):
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        row = json.loads(line)
                        if (row.get("reservation_id") or "").strip().upper() != rid:
                            continue
                        row_vid = _slugify_venue_id(str(row.get("venue_id") or DEFAULT_VENUE_ID))
                        if row_vid != current_vid:
                            continue
                        return row
                    except Exception:
                        continue
    except Exception:
        pass
    # 2) Google Sheets (if configured)
    try:
        # Use the same venue-scoped worksheet as the Admin Leads view.
        ws = get_sheet()
        header = (ws.row_values(1) or [])
        hnorm = [_normalize_header(h) for h in header]
        if "reservation_id" not in hnorm:
            return None
        col = hnorm.index("reservation_id") + 1
        vcol = None
        if "venue_id" in hnorm:
            vcol = hnorm.index("venue_id") + 1
        rows = ws.get_all_values() or []
        for r in rows[1:]:
            if len(r) >= col and (r[col - 1] or "").strip().upper() == rid:
                if vcol and vcol - 1 < len(r):
                    row_vid = _slugify_venue_id(str(r[vcol - 1] or DEFAULT_VENUE_ID))
                    if row_vid != current_vid:
                        continue
                out = {}
                for i, h in enumerate(header):
                    if i < len(r):
                        out[_normalize_header(h)] = r[i]
                return {
                    "reservation_id": out.get("reservation_id", rid),
                    "name": out.get("name", ""),
                    "phone": out.get("phone", ""),
                    "date": out.get("date", ""),
                    "time": out.get("time", ""),
                    "party_size": out.get("party_size", ""),
                    "status": out.get("status", "New"),
                    "vip": out.get("vip", "No"),
                }
    except Exception:
        pass
    return None


def _normalize_reservation_id(raw: str) -> Optional[str]:
    """Normalize a string to WC-XXXX format. Returns None if not a valid-looking ID."""
    t = (raw or "").strip().upper()
    if not t or len(t) < 4:
        return None
    if t.startswith("WC-") and len(t) > 3:
        return t
    if t.startswith("WC") and len(t) > 2:
        return "WC-" + t[2:] if len(t) > 2 else None
    if re.match(r"^[A-Z0-9\-]{4,}$", t):
        return "WC-" + t
    return None


def _is_bare_reservation_id(text: str) -> bool:
    """True if message is just a reservation ID (e.g. WC-BAC819C0) with no other words."""
    t = (text or "").strip()
    if not t:
        return False
    return bool(re.match(r"^\s*WC-?[A-Za-z0-9]{4,}\s*$", t, re.I))


def extract_recall_id(text: str) -> Optional[str]:
    """If message is 'recall <id>' or 'recall WC-xxx', return the id (WC-xxx or normalized). Else None.
    Handles formats like: 'recall WC-XXXX', 'recall : WC-XXXX', 'recall **WC-XXXX**', etc.
    """
    t = (text or "").strip()
    if not t:
        return None
    lower = t.lower()
    if not (lower.startswith("recall") or "recall" in lower.split()[:2]):
        return None
    # Match "recall" followed by optional whitespace/punctuation, then capture the ID
    # Handles: "recall WC-XXXX", "recall : WC-XXXX", "recall **WC-XXXX**", etc.
    m = re.search(r"recall\W*?(WC-?[A-Za-z0-9]+)", t, re.I)
    if m:
        raw = (m.group(1) or "").strip().upper()
        if raw.startswith("WC-"):
            return raw
        if raw.startswith("WC"):
            return "WC-" + raw[2:] if len(raw) > 2 else None
        if len(raw) >= 4:
            return "WC-" + raw
    # Fallback: try to extract any alphanumeric sequence after "recall" (for IDs without WC- prefix)
    m = re.search(r"recall\W+([A-Za-z0-9\-]{4,})", t, re.I)
    if m:
        raw = (m.group(1) or "").strip().upper()
        if len(raw) >= 4:
            return "WC-" + raw
    return None


def format_reservation_row(row: Optional[Dict[str, Any]]) -> str:
    """Format a reservation dict for display. Returns instruction message if row is None."""
    if not row or not any([row.get("date"), row.get("name"), row.get("phone")]):
        return "No reservation found for that ID. Check the ID and try again (e.g. recall WC-XXXX)."
    parts = [
        "📌 Reservation:",
        f"ID: {row.get('reservation_id') or '—'}",
        f"Name: {row.get('name') or '—'}",
        f"Phone: {row.get('phone') or '—'}",
        f"Date: {row.get('date') or '—'}",
        f"Time: {row.get('time') or '—'}",
        f"Party size: {row.get('party_size') or '—'}",
        f"Status: {row.get('status') or '—'}",
        f"VIP: {row.get('vip') or '—'}",
    ]
    return "\n".join(parts)


app = Flask(__name__, template_folder=_TEMPLATE_DIR, static_folder=_STATIC_DIR)

# ============================================================
# HARD SAFETY: forbid Flask dev server in production (Render)
# - wsgi.py sets WCG_WSGI=1 BEFORE importing app.py
# ============================================================
if os.environ.get("RENDER") == "true":
    # Must be loaded via gunicorn + wsgi.py
    if os.environ.get("WCG_WSGI") != "1":
        raise RuntimeError(
            "FATAL: Flask dev server detected in production. "
            "Use: gunicorn wsgi:application"
        )



app.config["SEND_FILE_MAX_AGE_DEFAULT"] = 0


# ============================================================
# Public platform probes (Azure/App Service friendly)
# - These MUST be fast and must not touch Redis/DB/Sheets/OpenAI/network.
# - They exist so platforms can verify the container is serving HTTP.
# ============================================================
@app.get("/_wsgi")
def public_wsgi_probe():
    server_sw = request.environ.get("SERVER_SOFTWARE", "") or ""
    looks_like_gunicorn = ("gunicorn" in server_sw.lower())
    return jsonify({
        "ok": True,
        "service": "wc-concierge-app",
        "looks_like_gunicorn": bool(looks_like_gunicorn),
        "server_software": server_sw,
        "python": (os.environ.get("PYTHON_VERSION") or ""),
    })
    
# privacy page
@app.get("/privacy")
def privacy_policy():
    html = """
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>Privacy Policy — World Cup Concierge</title>
  <style>
    body{margin:0;font-family:system-ui,-apple-system,Segoe UI,Roboto,Arial,sans-serif;background:#0b1220;color:#e8eefc;}
    .wrap{max-width:900px;margin:0 auto;padding:40px 18px;}
    h1{font-size:28px;margin:0 0 10px;}
    p,li{line-height:1.6;color:rgba(232,238,252,.9);}
    .card{background:rgba(255,255,255,.06);border:1px solid rgba(255,255,255,.10);border-radius:16px;padding:18px;}
    a{color:#b9c7ee;}
    .muted{color:rgba(232,238,252,.75);font-size:13px;margin-top:18px;}
    hr{border:0;border-top:1px solid rgba(255,255,255,.12);margin:14px 0;}
  </style>
</head>
<body>
  <div class="wrap">
    <h1>Privacy Policy</h1>
    <div class="card">
      <p><strong>World Cup Concierge</strong> collects personal information such as name, phone number, and email address when users voluntarily submit information through our website or applications.</p>

      <p>Phone numbers are collected solely for transactional and operational purposes, including reservation confirmations, VIP status updates, system alerts, and customer support communications.</p>

      <hr>

      <p><strong>SMS Consent</strong></p>

      <p>By providing your phone number and submitting a form on this website, you expressly consent to receive transactional SMS (text) messages from <strong>World Cup Concierge</strong>, operated by <strong>NYLA AI Solutions, LLC</strong>. Consent is not a condition of purchase.</p>

      <p>SMS messages may include reservation confirmations, VIP status updates, operational alerts, and customer support notifications. Message and data rates may apply. Message frequency varies.</p>

      <p>You may opt out of SMS communications at any time by replying <strong>STOP</strong>. For assistance, reply <strong>HELP</strong>.</p>

      <p>We do not send marketing or promotional SMS messages, and we do not sell or share personal information with third parties for marketing purposes.</p>

      <p>
        This website-owned number is used for World Cup Concierge communications only
        and is not presented as a shared messaging number for multiple independent businesses.
        </p>
      <p>For questions about this policy, contact: <a href="mailto:support@worldcupconcierge.app">support@worldcupconcierge.app</a></p>

      <p class="muted">World Cup Concierge is a product operated by NYLA AI Solutions, LLC.</p>

      <div class="muted">Last updated: January 2026</div>
    </div>
  </div>
</body>
</html>
"""
    resp = make_response(html, 200)
    resp.headers["Content-Type"] = "text/html; charset=utf-8"
    resp.headers["Cache-Control"] = "no-store"
    return resp






@app.get("/terms")
def terms_and_conditions():
    html = """
<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>Terms & Conditions — World Cup Concierge</title>
<style>
body{margin:0;font-family:system-ui,-apple-system,Segoe UI,Roboto,Arial,sans-serif;background:#0b1220;color:#e8eefc;}
.wrap{max-width:900px;margin:0 auto;padding:40px 18px;}
h1{font-size:28px;margin:0 0 10px;}
h2{margin-top:22px;font-size:18px;}
p,li{line-height:1.6;color:rgba(232,238,252,.9);}
.card{background:rgba(255,255,255,.06);border:1px solid rgba(255,255,255,.10);border-radius:16px;padding:18px;}
a{color:#b9c7ee;}
.muted{color:rgba(232,238,252,.75);font-size:13px;margin-top:18px;}
hr{border:0;border-top:1px solid rgba(255,255,255,.12);margin:14px 0;}
</style>
</head>
<body>
<div class="wrap">
<h1>Terms & Conditions</h1>
<div class="card">

<p><strong>World Cup Concierge</strong> is operated by <strong>NYLA AI Solutions, LLC</strong>. By accessing or using our website, applications, or services, you agree to these Terms & Conditions.</p>

<h2>1. Service Description</h2>
<p>
  World Cup Concierge provides hospitality operations tools, guest inquiry
  workflows, demo access, and support communication tools for venues and event
  operators.
</p>
<h2>2. User Responsibilities</h2>
<ul>
<li>You agree to provide accurate information when submitting reservation or contact forms.</li>
<li>You agree not to misuse, disrupt, or attempt unauthorized access to the platform.</li>
<li>You are responsible for maintaining the confidentiality of any access credentials.</li>
</ul>

<h2>3. SMS Communications</h2>
<p>
  By submitting your phone number through our website or applications, you
  consent to receive transactional SMS messages from World Cup Concierge related
  to demo requests, direct inquiries, and support responses.
</p>

<p>
  Message frequency varies. Message and data rates may apply. Consent is not a
  condition of purchase.
</p>

<p>
  You may opt out at any time by replying STOP. For help, reply HELP.
</p>

<p>
  We do not send marketing or promotional SMS messages from this website-owned
  number.
</p>

<p>
  SMS communications from this website-owned number are limited to World Cup
  Concierge platform communications and are not used for unsolicited marketing
  or shared multi-business promotional messaging.
</p>

<h2>4. Reservation Disclaimer</h2>
<p>Submission of a reservation request does not guarantee confirmation. Final approval is subject to venue availability and venue policies.</p>

<h2>5. Limitation of Liability</h2>
<p>NYLA AI Solutions, LLC is not liable for venue decisions, service disruptions, delays, or third-party platform failures. Services are provided “as is” without warranties of any kind.</p>

<h2>6. Intellectual Property</h2>
<p>All branding, content, software, and design elements are the property of NYLA AI Solutions, LLC and may not be copied or redistributed without written permission.</p>

<h2>7. Privacy</h2>
<p>Your use of the service is also governed by our Privacy Policy available at 
<a href="/privacy">/privacy</a>.</p>

<h2>8. Modifications</h2>
<p>We reserve the right to update these Terms at any time. Continued use of the service constitutes acceptance of the revised Terms.</p>

<h2>9. Governing Law</h2>
<p>These Terms are governed by the laws of the State of Texas, United States.</p>

<h2>10. Contact</h2>
<p>
  For questions regarding these Terms, contact:
  <a href="mailto:support@worldcupconcierge.app">support@worldcupconcierge.app</a>
</p>

<p>
  World Cup Concierge is a product operated by NYLA AI Solutions, LLC.
</p>

<p>Last updated: January 2026</p>

</div>
</div>
</body>
</html>
"""
    resp = make_response(html, 200)
    resp.headers["Content-Type"] = "text/html; charset=utf-8"
    resp.headers["Cache-Control"] = "no-store"
    return resp

@app.get("/_prod_gate")
def public_prod_gate():
    # Keep this endpoint *very* cheap: only runtime/process proof.
    server_sw = request.environ.get("SERVER_SOFTWARE", "") or ""
    looks_like_gunicorn = ("gunicorn" in server_sw.lower())
    ok_all = bool(looks_like_gunicorn)
    return jsonify({
        "ok": ok_all,
        "checks": {
            "gunicorn": {"ok": bool(looks_like_gunicorn), "server_software": server_sw},
        },
        "service": "wc-concierge-app",
    }), (200 if ok_all else 500)


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
    if path == "/" or path.startswith("/admin") or path.startswith("/chat") or path.startswith("/super"):
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
# Role-based admin keys (Owner vs Manager)
# - Back-compat: ADMIN_KEY continues to work as the Owner key.
# - Provide managers a separate key so they can view ops/leads without editing rules/menu.
ADMIN_OWNER_KEY = (os.environ.get("ADMIN_OWNER_KEY") or ADMIN_KEY or "").strip()
SUPER_ADMIN_KEY = (os.environ.get("SUPER_ADMIN_KEY") or "").strip()

# Multi-venue flag is initialized near the top via MULTI_VENUE / MULTI_VENUE_ENABLED env vars.
# Either a single key via ADMIN_MANAGER_KEY, or a comma-separated list via ADMIN_MANAGER_KEYS
_ADMIN_MANAGER_KEYS_RAW = (os.environ.get("ADMIN_MANAGER_KEYS") or os.environ.get("ADMIN_MANAGER_KEY") or "").strip()
ADMIN_MANAGER_KEYS = [k.strip() for k in _ADMIN_MANAGER_KEYS_RAW.split(",") if k.strip()]

RATE_LIMIT_PER_MIN = int(os.environ.get("RATE_LIMIT_PER_MIN", "30"))

SHEET_NAME = os.environ.get("GOOGLE_SHEET_NAME", "World Cup AI Reservations")

# ============================================================
# Multi-venue (tenant) support (safe defaults)
# - Enable by placing per-venue configs in ./config/venues/*.yaml|*.yml|*.json
# - Optional lock for dedicated deployments: VENUE_LOCK=<venue_id>
# - If no venue is resolved, we fall back to DEFAULT_VENUE_ID (or "default")
# ============================================================
VENUE_LOCK = (os.environ.get("VENUE_LOCK") or "").strip()
DEFAULT_VENUE_ID = (os.environ.get("DEFAULT_VENUE_ID") or "default").strip() or "default"
VENUES_DIR = os.environ.get("VENUES_DIR", os.path.join(os.path.dirname(__file__), "config", "venues"))
_MULTI_VENUE_CACHE: Dict[str, Any] = {"ts": 0.0, "venues": {}}

def _invalidate_venues_cache():
    # Multi-venue list cache
    try:
        _MULTI_VENUE_CACHE["ts"] = 0.0
        _MULTI_VENUE_CACHE["venues"] = {}
    except Exception:
        pass

    # Any optional per-venue config caches (only if they exist in this file)
    try:
        if isinstance(globals().get("_VENUE_CFG_CACHE"), dict):
            globals()["_VENUE_CFG_CACHE"].clear()
    except Exception:
        pass

    try:
        if isinstance(globals().get("_VENUE_CFG_BY_ID"), dict):
            globals()["_VENUE_CFG_BY_ID"].clear()
    except Exception:
        pass

def _slugify_venue_id(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"[^a-z0-9]+", "-", s)
    s = re.sub(r"-{2,}", "-", s).strip("-")
    return s or "default"


def _load_venues_from_disk() -> Dict[str, Any]:
    """Load venue configs from VENUES_DIR. Returns dict keyed by venue_id."""
    now = time.time()
    cached = _MULTI_VENUE_CACHE.get("venues")
    if isinstance(cached, dict) and (now - float(_MULTI_VENUE_CACHE.get("ts") or 0.0) < 2.0):
        return cached

    venues: Dict[str, Any] = {}
    try:
        if not os.path.isdir(VENUES_DIR):
            _MULTI_VENUE_CACHE["ts"] = now
            _MULTI_VENUE_CACHE["venues"] = venues
            return venues

        files: List[str] = []
        for fn in os.listdir(VENUES_DIR):
            if fn.lower().endswith((".yaml", ".yml", ".json")):
                files.append(os.path.join(VENUES_DIR, fn))
        files.sort()

        yaml_mod = None
        try:
            import yaml  # type: ignore
            yaml_mod = yaml
        except Exception:
            yaml_mod = None

        for fp in files:
            try:
                raw = pathlib.Path(fp).read_text(encoding="utf-8")
            except Exception:
                continue

            cfg = None
            if fp.lower().endswith(".json"):
                try:
                    cfg = json.loads(raw)
                except Exception:
                    cfg = None
            else:
                if yaml_mod is None:
                    # YAML venue file present but PyYAML missing: skip YAML so JSON venues still work
                    try:
                        print("[VENUES] skipping YAML (pyyaml not installed): " + os.path.basename(fp))
                    except Exception:
                        pass
                    continue
                try:
                    cfg = yaml_mod.safe_load(raw)
                except Exception:
                    cfg = None

            if not isinstance(cfg, dict):
                continue

            vid = _slugify_venue_id(str(cfg.get("venue_id") or cfg.get("venue") or ""))
            cfg["venue_id"] = vid
            cfg["_path"] = fp
            venues[vid] = cfg

    except Exception as e:
        # Keep app running; multi-tenant just becomes unavailable.
        try:
            print(f"[VENUES] load failed: {e}")
        except Exception:
            pass
        venues = {}

    _MULTI_VENUE_CACHE["ts"] = now
    _MULTI_VENUE_CACHE["venues"] = venues
    return venues


def _iter_venue_json_configs() -> List[Dict[str, Any]]:
    """Return venue config descriptors from VENUES_DIR.

    Used by cross-venue endpoints (Super Admin / enterprise ops).
    Never throws; returns [] if VENUES_DIR missing or unreadable.
    Each item contains:
      - venue_id
      - path (best-effort)
      - config (parsed dict)
    """
    out: List[Dict[str, Any]] = []
    try:
        venues = _load_venues_from_disk() or {}
        for vid, cfg in venues.items():
            if not isinstance(cfg, dict):
                continue
            out.append({
                "venue_id": str(vid),
                "path": str((cfg or {}).get("_path") or ""),
                "config": cfg or {},
            })
    except Exception:
        return []
    return out


def _resolve_venue_id() -> str:
    """Resolve venue_id from request (path/query/cookie/header) with optional VENUE_LOCK.

    In multi-venue mode, VENUE_LOCK is only used as a FALLBACK (not a hard override)
    so that explicit venue indicators in the URL/header still win.
    In single-venue mode, VENUE_LOCK is a hard override as before.
    """

    # 1) /v/<venue_id>/... from path (always wins — this IS the venue)
    try:
        m = re.match(r"^/v/([^/]+)", (request.path or ""))
        if m:
            return _slugify_venue_id(m.group(1))
    except Exception:
        pass

    # 2) ?venue=<venue_id> query param (explicit per-request)
    try:
        q = (request.args.get("venue") or "").strip()
        if q:
            return _slugify_venue_id(q)
    except Exception:
        pass

    # 3) X-Venue-Id header (must beat cookie for per-tab isolation)
    try:
        h = (request.headers.get("X-Venue-Id") or "").strip()
        if h:
            return _slugify_venue_id(h)
    except Exception:
        pass

    # 4) VENUE_LOCK: hard lock for dedicated single-venue deployments
    if VENUE_LOCK:
        return VENUE_LOCK

    # 5) venue_id cookie (fallback only)
    try:
        c = (request.cookies.get("venue_id") or "").strip()
        if c:
            return _slugify_venue_id(c)
    except Exception:
        pass

    return DEFAULT_VENUE_ID

@app.before_request
def _set_venue_ctx():
    try:
        g.venue_id = _resolve_venue_id()
    except Exception:
        g.venue_id = DEFAULT_VENUE_ID

@app.before_request
def _tenant_guard_admin_writes():
    # Fail-closed: admin writes MUST include an explicit venue (query or header).
    try:
        if not (request.path or "").startswith("/admin"):
            return None  # do not affect /super/*
        if (request.method or "GET").upper() not in ("POST", "PUT", "PATCH", "DELETE"):
            return None  # reads allowed

        raw_q = (request.args.get("venue") or "").strip()
        raw_h = (request.headers.get("X-Venue-Id") or "").strip()

        # IMPORTANT: do NOT allow cookie-only venue on writes (prevents cross-tab bleed)
        raw = raw_q or raw_h
        if not raw:
            try:
                _audit("tenant.guard.block", {"reason": "missing_explicit_venue", "path": request.path, "method": request.method})
                _notify("tenant.guard.block", {"reason": "missing_explicit_venue", "path": request.path, "method": request.method}, targets=["owner"])
            except Exception:
                pass
            return jsonify({"ok": False, "error": "venue_required"}), 403

        expected = _slugify_venue_id(raw)
        if (not expected) or (expected == DEFAULT_VENUE_ID) or (expected == "default"):
            return jsonify({"ok": False, "error": "venue_required"}), 403

        # Force request context to the explicit venue (overrides any cookie bleed)
        try:
            g.venue_id = expected
        except Exception:
            pass

        return None
    except Exception:
        return jsonify({"ok": False, "error": "venue_required"}), 403


def _venue_id() -> str:
    try:
        return _slugify_venue_id(getattr(g, "venue_id", "") or DEFAULT_VENUE_ID)
    except Exception:
        return DEFAULT_VENUE_ID


def _venue_cfg(venue_id: Optional[str] = None) -> Dict[str, Any]:
    venues = _load_venues_from_disk()
    vid = _slugify_venue_id(venue_id or _venue_id())
    cfg = venues.get(vid) if isinstance(venues, dict) else None
    return cfg if isinstance(cfg, dict) else {"venue_id": vid, "status": "implicit"}


def _venue_sheet_id(venue_id: Optional[str] = None) -> str:
    cfg = _venue_cfg(venue_id)
    data = cfg.get("data") if isinstance(cfg.get("data"), dict) else {}
    sid = str((data or {}).get("google_sheet_id") or (cfg.get("google_sheet_id") or "")).strip()
    return sid


def _venue_sheet_tab(venue_id: Optional[str] = None) -> str:
    cfg = _venue_cfg(venue_id)
    data = cfg.get("data") if isinstance(cfg.get("data"), dict) else {}
    tab = str((data or {}).get("sheet_name") or (cfg.get("sheet_name") or "")).strip()
    return tab


def _venue_features(venue_id: Optional[str] = None) -> Dict[str, Any]:
    cfg = _venue_cfg(venue_id)
    feat = cfg.get("features") if isinstance(cfg.get("features"), dict) else {}
    return dict(feat or {})


def _venue_is_active(venue_id: Optional[str] = None) -> bool:
    """Return whether a venue is active (fan-facing intake allowed)."""
    try:
        cfg = _venue_cfg(venue_id)
        if not isinstance(cfg, dict):
            return True

        # Back-compat rules:
        # - If cfg has boolean `active`, that is the source of truth.
        # - Otherwise fall back to `status` string (active|inactive|disabled|off).
        # - Default: active.
        if "active" in cfg:
            v = cfg.get("active")
            if isinstance(v, bool):
                return v
            sv = str(v or "").strip().lower()
            if sv in ("0", "false", "no", "n", "off"):
                return False
            if sv in ("1", "true", "yes", "y", "on"):
                return True

        st = str(cfg.get("status") or "").strip().lower()
        if st in ("inactive", "disabled", "off"):
            return False

        return True
    except Exception:
        return True


def _public_base_url() -> str:
    """Return the correct public base URL when behind proxies (Azure / Render / Cloudflare)."""
    proto = (request.headers.get("X-Forwarded-Proto") or request.scheme or "https").split(",")[0].strip()
    host = (request.headers.get("X-Forwarded-Host") or request.host or "").split(",")[0].strip()
    return f"{proto}://{host}".rstrip("/")

# Back-compat rules:
# - If cfg has boolean `active`, that is the source of truth.
# - Otherwise fall back to `status` string (active|inactive|disabled|off).
# - Default: active.
    
    try:
        cfg = _venue_cfg(venue_id)
        if not isinstance(cfg, dict):
            return True
        if "active" in cfg:
            v = cfg.get("active")
            if isinstance(v, bool):
                return v
            sv = str(v or "").strip().lower()
            if sv in ("0","false","no","n","off"):
                return False
            if sv in ("1","true","yes","y","on"):
                return True
        st = str(cfg.get("status") or "").strip().lower()
        if st in ("inactive","disabled","off"):
            return False
        return True
    except Exception:
        return True



def _open_default_spreadsheet(gc, venue_id: Optional[str] = None):
    """Open the venue-scoped spreadsheet (by sheet_id if present) else fall back to SHEET_NAME."""
    sid = _venue_sheet_id(venue_id)
    if sid:
        return gc.open_by_key(sid)
    return gc.open(SHEET_NAME)

def get_sheet(tab: Optional[str] = None, venue_id: Optional[str] = None):
    """Return a worksheet for the specified venue (or current venue)."""
    gc = get_gspread_client()
    sh = _open_default_spreadsheet(gc, venue_id=venue_id)

    # If no explicit tab requested, prefer the venue config's sheet_name.
    if not tab:
        tab = _venue_sheet_tab(venue_id) or ""

    if tab:
        return sh.worksheet(tab)
    return sh.sheet1


def _check_sheet_id(sheet_id: str) -> Dict[str, Any]:
    """Best-effort Google Sheet validation for onboarding.

    Retries once on transient errors (auth, network, rate limits).
    Never raises. Returns:
      { ok: bool, sheet_id, title, error, checked_at, details? }
    """
    def _try_check(sid: str) -> Dict[str, Any]:
        chk: Dict[str, Any] = {
            "ok": False,
            "sheet_id": sid,
            "title": "",
            "error": "",
            "checked_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        }
        if not sid:
            chk["error"] = "missing sheet_id"
            return chk
        try:
            gc = get_gspread_client()
            sh = gc.open_by_key(sid)
            chk["title"] = str(getattr(sh, "title", "") or "")
            chk["ok"] = True
            try:
                ws = sh.sheet1
                header = ws.row_values(1) or []
                chk["details"] = {"header": header[:64]}
            except Exception:
                pass
        except Exception as e:
            chk["ok"] = False
            chk["error"] = str(e)
        return chk
    
    sid = str(sheet_id or "").strip()
    
    # First attempt
    result = _try_check(sid)
    if result["ok"]:
        return result
    
    # Retry once after brief delay for transient failures
    # (gspread client init, auth token refresh, rate limit recovery)
    try:
        time.sleep(0.5)
        result = _try_check(sid)
    except Exception:
        pass
    
    return result


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


def _venue_business_profile(venue_id: Optional[str] = None) -> str:
    """
    Return a venue-specific business profile if configured; otherwise fall back
    to the global BUSINESS_PROFILE (which is sourced from business_profile.txt or env).

    NOTE: Fallback still uses BUSINESS_PROFILE / business_profile.txt.
    """
    try:
        cfg = _venue_cfg(venue_id)
        ident = cfg.get("identity") if isinstance(cfg.get("identity"), dict) else {}
        bp = ident.get("business_profile")
        if isinstance(bp, str) and bp.strip():
            return bp.strip()
    except Exception:
        pass

    # Fallback: use the global BUSINESS_PROFILE (Demo/business_profile.txt).
    # This is intentional so existing deployments continue to work.
    return BUSINESS_PROFILE

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
# Admin-persisted overrides (Rules + Menu)
# ============================================================
BUSINESS_RULES_FILE = os.environ.get("BUSINESS_RULES_FILE", "/tmp/wc26_{venue}_business_rules.json")
MENU_FILE = os.environ.get("MENU_FILE", "/tmp/wc26_{venue}_menu_override.json")


# ============================================================
# AI Automation Settings (Admin/Manager controlled)
# - Managers can enable/disable + choose mode
# - Owners can edit advanced behavior (model/prompts/thresholds)
# - Persisted on disk so it survives restarts (Render filesystem: /tmp is fine)
# ============================================================

# ============================================================
# Monitoring + Alerts (Human-in-the-middle friendly)
# - Read-only health checks for Sheets / Queue / Fixtures / Outbound provider readiness
# - Alerts are best-effort and never crash the app
# - Rate-limited to prevent spam
# ============================================================
ALERT_SETTINGS_FILE = os.environ.get("ALERT_SETTINGS_FILE", "/tmp/wc26_{venue}_alert_settings.json")
ALERT_STATE_FILE = os.environ.get("ALERT_STATE_FILE", "/tmp/wc26_{venue}_alert_state.json")

def _default_alert_settings() -> Dict[str, Any]:
    return {
        "enabled": False,
        "channels": {
            "slack": {"enabled": False, "webhook_url": os.environ.get("SLACK_WEBHOOK_URL", "")},
            "email": {"enabled": False, "to": os.environ.get("ALERT_EMAIL_TO", ""), "from": os.environ.get("ALERT_EMAIL_FROM", os.environ.get("SENDGRID_FROM",""))},
            "sms": {"enabled": False, "to": os.environ.get("ALERT_SMS_TO", "")},
        },
        "rate_limit_seconds": 600,   # 10 min per unique alert key
        "checks": {
            "fixtures_stale_seconds": int(os.environ.get("FIXTURES_STALE_SECONDS", "86400") or 86400), # 24h
        },
        "updated_at": None,
        "updated_by": None,
        "updated_role": None,
    }

ALERT_SETTINGS: Dict[str, Any] = _default_alert_settings()

def _load_alert_settings_from_disk() -> None:
    global ALERT_SETTINGS
    payload = _safe_read_json_file(ALERT_SETTINGS_FILE)
    if isinstance(payload, dict) and payload:
        ALERT_SETTINGS = _deep_merge(_default_alert_settings(), payload)

def _save_alert_settings_to_disk(patch: Dict[str, Any], actor: str, role: str) -> Dict[str, Any]:
    global ALERT_SETTINGS
    merged = _deep_merge(ALERT_SETTINGS, patch or {})
    merged["updated_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    merged["updated_by"] = actor
    merged["updated_role"] = role
    ALERT_SETTINGS = merged
    _safe_write_json_file(ALERT_SETTINGS_FILE, ALERT_SETTINGS)
    return ALERT_SETTINGS

def _alert_state() -> Dict[str, Any]:
    st = _safe_read_json_file(ALERT_STATE_FILE, default={})
    return st if isinstance(st, dict) else {}

def _save_alert_state(st: Dict[str, Any]) -> None:
    _safe_write_json_file(ALERT_STATE_FILE, st or {})

def _should_send_alert(key: str) -> bool:
    try:
        if not ALERT_SETTINGS.get("enabled"):
            return False
        st = _alert_state()
        now = int(time.time())
        last = int((st.get(key) or 0))
        rl = int(ALERT_SETTINGS.get("rate_limit_seconds") or 600)
        if now - last < rl:
            return False
        st[key] = now
        st['_last_any'] = now
        _save_alert_state(st)
        return True
    except Exception:
        return False

def _http_post_json(url: str, payload: Dict[str, Any], timeout: int = 8) -> None:
    data = json.dumps(payload or {}).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers={"Content-Type":"application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        r.read()

def _send_slack(text: str) -> bool:
    try:
        ch = (ALERT_SETTINGS.get("channels") or {}).get("slack") or {}
        if not (ch.get("enabled") and ch.get("webhook_url")):
            return False
        _http_post_json(str(ch.get("webhook_url")), {"text": text})
        return True
    except Exception:
        return False

def _send_email(subject: str, body: str) -> bool:
    """
    Send alert email using the SAME SendGrid path as the landing page,
    so behavior is consistent and proven to work.
    """
    try:
        ch = (ALERT_SETTINGS.get("channels") or {}).get("email") or {}
        if not ch.get("enabled"):
            return False

        to_addr = str(ch.get("to") or "").strip()
        if not to_addr:
            return False

        ok, msg = _outbound_send_email(
            to_email=to_addr,
            subject=subject,
            body_text=body
        )
        if not ok:
            print("[ALERT EMAIL FAILED]", msg)
        return bool(ok)
    except Exception as e:
        print("[ALERT EMAIL ERROR]", repr(e))
        return False


def _send_sms(text: str) -> bool:
    # Twilio (best-effort). If not configured, return False.
    try:
        ch = (ALERT_SETTINGS.get("channels") or {}).get("sms") or {}
        to_num = str(ch.get("to") or "").strip()
        if not (ch.get("enabled") and to_num):
            return False
        # reuse existing Twilio helpers if present
        if "_twilio_send_sms" in globals():
            return bool(_twilio_send_sms(to_num, text))
        return False
    except Exception:
        return False

def _dispatch_alert(title: str, details: str, key: str, severity: str = "error") -> Dict[str, Any]:
    sent = {"slack": False, "email": False, "sms": False}
    if not _should_send_alert(key):
        return {"ok": True, "sent": sent, "rate_limited": True}
    msg = f"{'🚨' if severity=='error' else '⚠️'} {title}\n{details}".strip()
    sent["slack"] = _send_slack(msg)
    sent["email"] = _send_email(title, msg)
    # SMS only for error severity (you can widen later)
    if severity == "error":
        sent["sms"] = _send_sms(msg[:1400])
    # always in-app notify too
    try:
        _notify("monitor.alert", {"title": title, "details": details, "severity": severity}, targets=["owner","manager"])
    except Exception:
        pass
    return {"ok": True, "sent": sent, "rate_limited": False}

AI_SETTINGS_FILE = os.environ.get("AI_SETTINGS_FILE", "/tmp/wc26_{venue}_ai_settings.json")

def _default_ai_settings() -> Dict[str, Any]:
    return {
        "enabled": False,                 # master kill-switch
        "mode": "suggest",               # off | suggest | auto
        "require_approval": True,        # even in auto mode, queue actions for approval unless explicitly disabled
        "min_confidence": 0.70,          # 0..1 gate for auto actions (if approval not required)
        "notify": {"owner": True, "manager": True},  # for future webhooks/email; currently used for in-app audit/events
        "model": os.environ.get("OPENAI_MODEL", "gpt-4o-mini"),
        "system_prompt": "You are an operations copilot for a premium World Cup concierge app. Output concise, structured JSON only.",
        "allow_actions": {
            "vip_tag": True,
            "status_update": False,
            "reply_draft": True,
            "send_sms": False,
            "send_email": False,
            "send_whatsapp": False,
        },
        # Which sheet status values count as "new" for Run AI (New).
        # Per-venue overrides can set `new_status_values` in AI settings.
        "new_status_values": ["new", ""],
        # Feature gates (safe rollout). These further restrict what AI can do, even if allow_actions is true.
        "features": {
            "auto_vip_tag": True,
            "auto_status_update": False,
            "auto_reply_draft": True,
        },
        "updated_at": None,
        "updated_by": None,  # actor hash
        "updated_role": None,
    }

# In-memory cache of last-loaded settings (per-process). Always treat as
# per-request, per-venue via _get_ai_settings().
AI_SETTINGS: Dict[str, Any] = _default_ai_settings()

def _get_ai_settings() -> Dict[str, Any]:
    """
    Return AI settings for the current venue.

    - Reads from AI_SETTINGS_FILE using _safe_read_json_file, which is already
      venue-aware (via {venue} + Redis path map).
    - Deep-merges on top of defaults so new fields appear automatically.
    """
    payload = _safe_read_json_file(AI_SETTINGS_FILE, default=None)
    if isinstance(payload, dict) and payload:
        return _deep_merge(_default_ai_settings(), payload)
    return _default_ai_settings()


def _ai_feature_allows(action_type: str, settings: Optional[Dict[str, Any]] = None) -> bool:
    """Secondary gates for AI actions (feature flags).

    Even if allow_actions permits an action, features can disable it for safe rollout.
    """
    s = settings or _get_ai_settings() or {}
    feat = s.get("features") or {}
    at = (action_type or "").strip().lower()
    if at == "vip_tag":
        return bool(feat.get("auto_vip_tag", True))
    if at == "status_update":
        return bool(feat.get("auto_status_update", False))
    if at == "reply_draft":
        return bool(feat.get("auto_reply_draft", True))
    return True


def _load_ai_settings_from_disk() -> None:
    """
    Back-compat boot loader. Kept so existing code that inspects AI_SETTINGS
    during startup has a sane default, but all runtime callers should prefer
    _get_ai_settings() which is venue-aware.
    """
    global AI_SETTINGS
    AI_SETTINGS = _get_ai_settings()

def _save_ai_settings_to_disk(patch: Dict[str, Any], actor: str, role: str) -> Dict[str, Any]:
    """
    Persist AI settings for the current venue.

    - Loads current venue's settings.
    - Deep-merges the patch.
    - Stamps audit metadata.
    - Writes back via _safe_write_json_file (Redis or disk, per-venue).
    """
    global AI_SETTINGS
    current = _get_ai_settings()
    merged = _deep_merge(current, patch or {})
    merged["updated_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    merged["updated_by"] = actor
    merged["updated_role"] = role
    AI_SETTINGS = merged
    _safe_write_json_file(AI_SETTINGS_FILE, AI_SETTINGS)
    return AI_SETTINGS


def _deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    """Shallow+deep merge for dicts (override wins)."""
    out = dict(base or {})
    for k, v in (override or {}).items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = _deep_merge(out.get(k) or {}, v)
        else:
            out[k] = v
    return out



# ============================================================
# Partner/Venue Policy Rules (Human-in-the-middle, hard constraints)
# - Per partner/venue rules that AI and operators must obey.
# - Enforced BOTH when suggestions are created and when actions are applied.
# - Stored on disk (/tmp) so it survives restarts on Render.
# ============================================================
PARTNER_POLICIES_FILE = os.environ.get("PARTNER_POLICIES_FILE", "/tmp/wc26_{venue}_partner_policies.json")

def _default_partner_policy() -> Dict[str, Any]:
    # Restrictive defaults (safe):
    # - Never allow AI to auto-change status unless explicitly enabled per partner AND allowed in AI settings.
    # - VIP tagging allowed, but can be constrained by min budget.
    return {
        "vip_min_budget": 0,                 # e.g., 1500 to require a minimum budget for VIP tagging
        "never_status_update": True,         # hard block status_update actions unless set False
        "allowed_statuses": ["New", "Confirmed", "Seated", "No-Show", "Handled"],
        # Outbound channels (added for upcoming phase; kept restrictive-safe by default)
        "outbound_allowed": {"email": True, "sms": True, "whatsapp": True},
        "outbound_require_role": "manager",  # who can send outbound (human click) once enabled
    }

_PARTNER_POLICIES: Dict[str, Any] = {"default": _default_partner_policy()}

def _load_partner_policies_from_disk() -> None:
    global _PARTNER_POLICIES
    payload = _safe_read_json_file(PARTNER_POLICIES_FILE, default=None)
    if isinstance(payload, dict) and payload:
        # Merge default policy into each partner so missing keys don't break.
        out: Dict[str, Any] = {}
        for k, v in payload.items():
            if not isinstance(v, dict):
                continue
            out[str(k)] = _deep_merge(_default_partner_policy(), v)
        if "default" not in out:
            out["default"] = _default_partner_policy()
        _PARTNER_POLICIES = out

def _save_partner_policy(partner: str, policy_patch: Dict[str, Any]) -> Dict[str, Any]:
    """Persist a single partner policy (best effort)."""
    global _PARTNER_POLICIES
    partner = (partner or "").strip() or "default"
    cur = _PARTNER_POLICIES.get(partner) if isinstance(_PARTNER_POLICIES, dict) else None
    if not isinstance(cur, dict):
        cur = _default_partner_policy()
    merged = _deep_merge(cur, policy_patch or {})
    _PARTNER_POLICIES[partner] = merged
    _safe_write_json_file(PARTNER_POLICIES_FILE, _PARTNER_POLICIES)
    return merged


# ============================================================
# Draft templates (per-venue; file or Redis — NEVER in venue config)
# - Venue config files must remain static (identity, keys, feature flags only).
# - Drafts are operational reply templates (SMS, email, WhatsApp, AI Queue).
# - Storage: per-venue file e.g. /tmp/wc26_qa-sandbox_drafts.json (or Redis when enabled).
# - Override DRAFTS_FILE via env if needed. Never read/write drafts from VENUES_DIR.
# ============================================================
def _default_drafts() -> Dict[str, Any]:
    return {
        "updated_at": None,
        "updated_by": None,
        "updated_role": None,
        "drafts": {
            "sms_confirm": {
                "channel": "sms",
                "title": "SMS — Confirmation",
                "body": "Hi {name}, you're confirmed for {date} at {time} for {party_size}.",
            },
            "email_confirm": {
                "channel": "email",
                "title": "Email confirmation",
                "subject": "",
                "body": "Hi {name}, your table is confirmed for {date}.",
            }
        }
    }


def _load_drafts_from_disk() -> Dict[str, Any]:
    """Load drafts for current venue (g.venue_id). Uses DRAFTS_FILE or Redis when enabled.
    Returns defaults only if file doesn't exist. If file exists (even with empty drafts), returns it as-is."""
    payload = _safe_read_json_file(DRAFTS_FILE, default=None)
    if isinstance(payload, dict) and payload:
        # If payload has 'drafts' key, it means user has saved before (even if empty) - return as-is, don't merge defaults
        if "drafts" in payload:
            return payload
        # Otherwise merge defaults (for backward compatibility with old format)
        return _deep_merge(_default_drafts(), payload)
    return _default_drafts()


def _save_drafts_to_disk(patch: Dict[str, Any], actor: str, role: str) -> Dict[str, Any]:
    """Save drafts for current venue to DRAFTS_FILE or Redis only. Never writes to venue config.
    When patch contains 'drafts', that object fully replaces stored drafts (so delete = remove key and save)."""
    current = _load_drafts_from_disk()
    merged = _deep_merge(current, patch or {})
    # Full replace: if caller sent a "drafts" key, use it as-is so removing a key in the UI and saving actually deletes it
    if "drafts" in (patch or {}):
        merged["drafts"] = dict((patch or {}).get("drafts") or {})
    merged["updated_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    merged["updated_by"] = (actor or "").strip()
    merged["updated_role"] = (role or "").strip()
    _safe_write_json_file(DRAFTS_FILE, merged)
    return merged


def _migrate_drafts_out_of_venue_config(venue_id: str) -> None:
    """One-time: if venue config has 'drafts', copy to dedicated store and remove from config."""
    path = os.path.join(VENUES_DIR, f"{venue_id}.json")
    if not os.path.exists(path):
        return
    try:
        with open(path, "r", encoding="utf-8") as f:
            vcfg = json.load(f) or {}
    except Exception:
        return
    drafts = vcfg.get("drafts")
    if not isinstance(drafts, dict) or not drafts:
        return
    # Save to dedicated store (g.venue_id must be set; caller sets it)
    _save_drafts_to_disk({"drafts": drafts}, actor="migration", role="system")
    # Remove from venue config so config stays static
    vcfg.pop("drafts", None)
    vcfg.pop("updated_at", None)  # draft-specific meta
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(vcfg, f, indent=2, ensure_ascii=False, sort_keys=True)
    except Exception:
        pass


def _load_lead_from_sheet_row(row_num: int) -> Dict[str, Any]:
    """Load lead data from Google Sheet row number. Returns dict with name, date, time, party_size, etc."""
    if row_num < 2:
        return {}
    try:
        gc = get_gspread_client()
        ws = _open_default_spreadsheet(gc, venue_id=_venue_id()).sheet1
        header = ws.row_values(1) or []
        hmap = header_map(header)
        row_data = ws.row_values(row_num) or []
        
        lead = {}
        for key in ["name", "date", "time", "party_size", "phone", "email", "budget", "notes"]:
            col = hmap.get(key)
            if col and col <= len(row_data):
                val = str(row_data[col - 1] or "").strip()
                if val:
                    lead[key] = val
        
        return lead
    except Exception:
        return {}


def _format_draft_template(template: str, data: Dict[str, Any]) -> str:
    """Replace placeholders in draft template with actual values.
    Supports: {name}, {date}, {time}, {party_size}, {phone}, {email}, {budget}, {notes}
    Missing values are replaced with empty string or placeholder name."""
    if not template:
        return ""
    
    replacements = {
        "name": str(data.get("name") or "").strip(),
        "date": str(data.get("date") or "").strip(),
        "time": str(data.get("time") or "").strip(),
        "party_size": str(data.get("party_size") or "").strip(),
        "phone": str(data.get("phone") or "").strip(),
        "email": str(data.get("email") or "").strip(),
        "budget": str(data.get("budget") or "").strip(),
        "notes": str(data.get("notes") or "").strip(),
    }
    
    result = template
    for key, value in replacements.items():
        placeholder = "{" + key + "}"
        result = result.replace(placeholder, value if value else "")
    
    return result


def _select_draft_for_channel(channel: str, context: Optional[str] = None) -> Optional[str]:
    """Select appropriate draft key based on channel and context.
    Returns draft key like 'email_confirm', 'sms_confirm', 'sms_more_info', etc.
    Context can be 'confirm', 'more_info', etc. If None, defaults to 'confirm'."""
    channel = (channel or "").strip().lower()
    context = (context or "confirm").strip().lower()
    
    # Map channel + context to draft key
    if channel == "email":
        return f"email_{context}" if context != "confirm" else "email_confirm"
    elif channel in ("sms", "whatsapp"):
        return f"sms_{context}" if context != "confirm" else "sms_confirm"
    
    return None


def _get_draft_content(draft_key: str, data: Dict[str, Any]) -> Tuple[Optional[str], Optional[str]]:
    """Get formatted draft body and subject for given draft key.
    Returns (body, subject) tuple. Both can be None if draft not found."""
    drafts_data = _load_drafts_from_disk()
    drafts = drafts_data.get("drafts") or {}
    
    draft = drafts.get(draft_key)
    if not isinstance(draft, dict):
        return None, None
    
    body_template = str(draft.get("body") or "").strip()
    subject_template = str(draft.get("subject") or "").strip()
    
    if not body_template:
        return None, None
    
    body = _format_draft_template(body_template, data)
    subject = _format_draft_template(subject_template, data) if subject_template else None
    
    return body, subject


def _derive_partner_id(lead: Optional[Dict[str, Any]] = None, payload: Optional[Dict[str, Any]] = None) -> str:
    """Best-effort partner/venue identifier, so policies can apply even if schema varies."""
    lead = lead or {}
    payload = payload or {}
    for key in ["partner", "venue", "partner_id", "venue_id"]:
        v = payload.get(key) or lead.get(key)
        if v:
            return str(v).strip()
    # fallbacks from common fields
    for key in ["entry_point", "business_context", "tier", "queue"]:
        v = payload.get(key) or lead.get(key)
        if v:
            s = str(v).strip()
            # take first token if it looks like "VENUE_XYZ ..."
            return s.split()[0]
    return "default"

def _partner_policy(partner: str) -> Dict[str, Any]:
    partner = (partner or "").strip() or "default"
    base = _PARTNER_POLICIES.get("default") if isinstance(_PARTNER_POLICIES, dict) else None
    if not isinstance(base, dict):
        base = _default_partner_policy()
    p = _PARTNER_POLICIES.get(partner) if isinstance(_PARTNER_POLICIES, dict) else None
    if not isinstance(p, dict):
        return base
    return _deep_merge(base, p)

def _parse_budget_to_number(val: Any) -> float:
    try:
        s = str(val or "").strip()
        if not s:
            return 0.0
        s = s.replace("$", "").replace(",", "")
        # accept "1500+" or "1500 usd"
        m = re.search(r"(\d+(\.\d+)?)", s)
        if not m:
            return 0.0
        return float(m.group(1))
    except Exception:
        return 0.0

def _policy_check_action(partner: str, action_type: str, payload: Dict[str, Any], role: str = "") -> Tuple[bool, str]:
    """Return (allowed, reason). Hard blocks only."""
    pol = _partner_policy(partner)
    at = (action_type or "").strip().lower()

    # Hard block status updates unless partner explicitly allows
    if at == "status_update":
        if bool(pol.get("never_status_update", True)):
            return False, "Blocked by partner policy: status updates disabled"
        status = str((payload or {}).get("status") or "").strip()
        allowed = pol.get("allowed_statuses") or []
        if status and isinstance(allowed, list) and allowed and status not in allowed:
            return False, f"Blocked by partner policy: status '{status}' not allowed"

    # VIP tagging minimum budget
    if at == "vip_tag":
        vip = str((payload or {}).get("vip") or "").strip()
        if vip == "VIP":
            min_budget = float(pol.get("vip_min_budget") or 0)
            # budget may be on payload (preferred) or absent; treat absent as 0
            b = _parse_budget_to_number((payload or {}).get("budget"))
            if b < min_budget:
                return False, f"Blocked by partner policy: VIP requires budget ≥ {int(min_budget)}"

    # Outbound sending (reserved for next phase)
    if at in ("send_sms", "send_email", "send_whatsapp", "send_confirmation", "send_reservation_received", "send_update", "send_vip_update"):
        if at in ("send_confirmation", "send_reservation_received", "send_update", "send_vip_update"):
            ch = "sms"
        else:
            ch = at.replace("send_", "")
        allowed = (pol.get("outbound_allowed") or {})
        if isinstance(allowed, dict) and not bool(allowed.get(ch, False)):
            return False, f"Blocked by partner policy: outbound {ch} disabled"
        req_role = str(pol.get("outbound_require_role") or "manager").strip().lower()
        if req_role == "owner" and (role or "").lower() != "owner":
            return False, "Blocked by partner policy: outbound requires owner"

    return True, ""



# ============================================================
# Outbound adapters (human-in-the-middle only)
# - These are ONLY executed when a human clicks "Send Now"
# - Missing credentials must fail gracefully (no crash)
# ============================================================

try:
    import requests  # type: ignore
except Exception:
    requests = None  # type: ignore

# ============================================================
# Landing lead capture (public)
# - Sheet ID + alert email are configurable via env vars
# ============================================================
LANDING_LEADS_SHEET_ID = os.environ.get(
    "LANDING_LEADS_SHEET_ID",
    "1PH0pqj6qKLmtXc0G46hO63-39CdmFSin6RqfYIJ5uSM"
).strip()

LANDING_LEAD_ALERT_TO = os.environ.get(
    "LANDING_LEAD_ALERT_TO",
    "bayz23@gmail.com"
).strip()

def _append_landing_lead_to_sheet(lead: Dict[str, Any]) -> Tuple[bool, str]:
    """
    Append a landing lead to the configured Google Sheet.
    Expects your existing Google creds env + gspread helpers to already work in this app.
    Returns (ok, message).
    """
    try:
        # Reuse your existing spreadsheet open + worksheet patterns if available.
        # We try to open by key and write to a tab named "Landing Leads" (auto-create if missing).
        gc = get_gspread_client()
        sh = gc.open_by_key(LANDING_LEADS_SHEET_ID)

        tab_name = os.environ.get("LANDING_LEADS_TAB", "Landing Leads").strip() or "Landing Leads"
        try:
            ws = sh.worksheet(tab_name)
        except Exception:
            ws = sh.add_worksheet(title=tab_name, rows=2000, cols=20)

        # Ensure header row exists
        header = ["ts", "name", "venue", "email", "phone", "contact_method", "contact_time", "source", "notes"]
        try:
            existing = ws.row_values(1)
        except Exception:
            existing = []
        if not existing:
            ws.append_row(header, value_input_option="RAW")

        row = [
            str(lead.get("ts") or ""),
            str(lead.get("name") or ""),
            str(lead.get("venue") or ""),
            str(lead.get("email") or ""),
            str(lead.get("phone") or ""),
            str(lead.get("contact_method") or ""),
            str(lead.get("contact_time") or ""),
            str(lead.get("source") or "landing"),
            str(lead.get("notes") or ""),
        ]

        ws.append_row(row, value_input_option="RAW")
        return True, "saved"
    except Exception as e:
        return False, f"sheet_error: {e}"


def _outbound_send_email(to_email: str, subject: str, body_text: str) -> Tuple[bool, str]:
    """Send email via SendGrid (recommended). Returns (ok, message)."""
    api_key = os.environ.get("SENDGRID_API_KEY", "").strip()
    from_email = os.environ.get("SENDGRID_FROM", "").strip()
    if not api_key or not from_email:
        return False, "Email not configured (missing SENDGRID_API_KEY or SENDGRID_FROM)"
    if requests is None:
        return False, "Email not available (missing requests library)"

    try:
        url = "https://api.sendgrid.com/v3/mail/send"
        payload = {
            "personalizations": [{"to": [{"email": to_email}]}],
            "from": {"email": from_email},
            "subject": subject or "World Cup Concierge",
            "content": [{"type": "text/plain", "value": body_text or ""}],
        }
        r = requests.post(url, headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}, json=payload, timeout=12)
        if 200 <= r.status_code < 300:
            return True, "Email sent"
        return False, f"SendGrid error {r.status_code}"
    except Exception as e:
        return False, f"Email send failed: {e}"

SMS_SUBSCRIPTIONS_FILE = os.environ.get("SMS_SUBSCRIPTIONS_FILE", "/tmp/wc26_sms_subscriptions.json")
SMS_EVENTS_FILE = os.environ.get("SMS_EVENTS_FILE", "/tmp/wc26_sms_events.jsonl")
TWILIO_REQUIRE_SIGNATURE = str(os.environ.get("REQUIRE_TWILIO_SIGNATURE", "true")).strip().lower() == "true"
PUBLIC_BASE_URL = (os.environ.get("PUBLIC_BASE_URL", "") or "").strip().rstrip("/")
_STOP_KEYWORDS = {"STOP", "STOPALL", "UNSUBSCRIBE", "CANCEL", "END", "QUIT"}
_START_KEYWORDS = {"START", "UNSTOP", "YES"}
_HELP_KEYWORDS = {"HELP", "INFO"}

def _normalize_phone_e164(number: str, default_country_code: str = "+1") -> Optional[str]:
    """
    Normalize user/staff-entered phone numbers to E.164.
    Rules:
    - strip spaces, dashes, parentheses, dots
    - keep numbers already starting with '+'
    - if exactly 10 digits, default to US/CA (+1)
    - otherwise prefix '+' and keep provided country code digits
    """
    raw = (number or "").strip()
    if not raw:
        return None
    if raw.lower().startswith("whatsapp:"):
        raw = raw.split(":", 1)[1].strip()
    cleaned = re.sub(r"[^\d+]", "", raw)
    if not cleaned:
        return None
    if cleaned.startswith("+"):
        digits = re.sub(r"\D", "", cleaned[1:])
        if not digits:
            return None
        out = "+" + digits
    else:
        digits = re.sub(r"\D", "", cleaned).lstrip("0")
        if not digits:
            return None
        if len(digits) == 10:
            out = default_country_code + digits
        else:
            out = "+" + digits
    d = re.sub(r"\D", "", out)
    if len(d) < 8 or len(d) > 15:
        return None
    return out

def _sms_read_subscriptions() -> Dict[str, Any]:
    try:
        if not os.path.exists(SMS_SUBSCRIPTIONS_FILE):
            return {}
        with open(SMS_SUBSCRIPTIONS_FILE, "r", encoding="utf-8") as f:
            obj = json.load(f)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}

def _sms_write_subscriptions(data: Dict[str, Any]) -> None:
    try:
        os.makedirs(os.path.dirname(SMS_SUBSCRIPTIONS_FILE), exist_ok=True)
        with open(SMS_SUBSCRIPTIONS_FILE, "w", encoding="utf-8") as f:
            json.dump(data or {}, f, ensure_ascii=False, indent=2)
    except Exception:
        pass

def _sms_set_subscribed(phone_e164: str, is_subscribed: bool, reason: str) -> None:
    try:
        key = (phone_e164 or "").strip()
        if not key:
            return
        d = _sms_read_subscriptions()
        d[key] = {
            "is_subscribed": bool(is_subscribed),
            "reason": str(reason or ""),
            "updated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        }
        _sms_write_subscriptions(d)
    except Exception:
        pass

def _sms_can_send_to(phone_e164: str) -> bool:
    try:
        row = (_sms_read_subscriptions() or {}).get((phone_e164 or "").strip()) or {}
        return bool(row.get("is_subscribed", True))
    except Exception:
        return True

def _sms_log_event(kind: str, payload: Dict[str, Any]) -> None:
    try:
        os.makedirs(os.path.dirname(SMS_EVENTS_FILE), exist_ok=True)
        obj = {
            "ts": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "kind": str(kind or ""),
            "payload": payload or {},
        }
        with open(SMS_EVENTS_FILE, "a", encoding="utf-8") as f:
            f.write(json.dumps(obj, ensure_ascii=False) + "\n")
    except Exception:
        pass

def _twilio_public_url_for_request(req) -> str:
    if PUBLIC_BASE_URL:
        return f"{PUBLIC_BASE_URL}{req.path}"
    return req.url

def _twilio_validate_signature(req, form_data: Dict[str, Any]) -> bool:
    token = os.environ.get("TWILIO_AUTH_TOKEN", "").strip()
    sig = (req.headers.get("X-Twilio-Signature") or "").strip()
    if not token:
        return False
    if not sig:
        return False
    try:
        url = _twilio_public_url_for_request(req)
        s = url
        for k in sorted(form_data.keys()):
            v = form_data.get(k)
            if isinstance(v, list):
                for vv in v:
                    s += k + str(vv)
            else:
                s += k + str(v)
        digest = hmac.new(token.encode("utf-8"), s.encode("utf-8"), hashlib.sha1).digest()
        expected = base64.b64encode(digest).decode("utf-8")
        return hmac.compare_digest(expected, sig)
    except Exception:
        return False

def _twiml_response(message_text: Optional[str] = None):
    body = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Response>"
    if message_text:
        msg = str(message_text).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
        body += f"<Message>{msg}</Message>"
    body += "</Response>"
    out = make_response(body, 200)
    out.headers["Content-Type"] = "application/xml; charset=utf-8"
    return out

def _outbound_send_twilio(channel: str, to_number_or_id: str, body_text: str) -> Tuple[bool, str]:
    """
    Send SMS/WhatsApp via Twilio.
    - SMS: TWILIO_FROM (your Twilio SMS number).
    - WhatsApp: TWILIO_WHATSAPP_FROM (must be a WhatsApp-enabled sender in Twilio — sandbox or
      approved WhatsApp Business number). Your SMS number is not automatically WhatsApp-enabled;
      set TWILIO_WHATSAPP_FROM to a number registered for WhatsApp in Twilio Console.
    """
    sid = os.environ.get("TWILIO_ACCOUNT_SID", "").strip()
    token = os.environ.get("TWILIO_AUTH_TOKEN", "").strip()
    if not sid or not token:
        return False, "Twilio not configured (missing TWILIO_ACCOUNT_SID or TWILIO_AUTH_TOKEN)"
    if requests is None:
        return False, "Twilio not available (missing requests library)"

    ch = (channel or "").strip().lower()
    frm = ""
    to = (to_number_or_id or "").strip()
    to_e164 = _normalize_phone_e164(to)
    if not to_e164:
        return False, "Invalid recipient number (must be valid international/E.164 format)"
    if not _sms_can_send_to(to_e164):
        return False, "Recipient opted out (STOP)."

    if ch == "sms":
        msg_service_sid = os.environ.get("TWILIO_MESSAGING_SERVICE_SID", "").strip()
        frm = os.environ.get("TWILIO_FROM", "").strip()
        if not msg_service_sid and not frm:
            return False, "SMS not configured (set TWILIO_MESSAGING_SERVICE_SID or TWILIO_FROM)"
    elif ch == "whatsapp":
        frm = os.environ.get("TWILIO_WHATSAPP_FROM", "").strip() or os.environ.get("TWILIO_FROM", "").strip()
        if not frm:
            return False, (
                "WhatsApp not configured. Set TWILIO_WHATSAPP_FROM to a WhatsApp-enabled sender in Twilio "
                "(Console → Messaging → Senders → WhatsApp). Your SMS number is not automatically WhatsApp-enabled."
            )
        frm_num = _normalize_phone_e164(frm)
        if not frm_num:
            return False, "WhatsApp sender invalid (must be E.164 in TWILIO_WHATSAPP_FROM/TWILIO_FROM)"
        frm = "whatsapp:" + frm_num
        to = "whatsapp:" + to_e164
    else:
        return False, "Unsupported Twilio channel"

    try:
        url = f"https://api.twilio.com/2010-04-01/Accounts/{sid}/Messages.json"
        data = {"To": to if ch == "whatsapp" else to_e164, "Body": body_text or ""}
        if ch == "sms":
            msg_service_sid = os.environ.get("TWILIO_MESSAGING_SERVICE_SID", "").strip()
            if msg_service_sid:
                data["MessagingServiceSid"] = msg_service_sid
            else:
                frm_num = _normalize_phone_e164(frm)
                if not frm_num:
                    return False, "SMS sender invalid (TWILIO_FROM must be valid E.164)"
                data["From"] = frm_num
        else:
            data["From"] = frm
        r = requests.post(url, data=data, auth=(sid, token), timeout=12)
        try:
            rj = r.json() if r.text else {}
        except Exception:
            rj = {}
        if 200 <= r.status_code < 300:
            _sms_log_event("outbound.sent", {"channel": ch, "to": to_e164, "sid": rj.get("sid"), "status": rj.get("status")})
            return True, "Message sent"
        err_msg = f"Twilio error {r.status_code}"
        try:
            code = rj.get("code") or rj.get("error_code")
            msg = (rj.get("message") or rj.get("error_message") or "").strip()
            if code is not None or msg:
                err_msg = f"Twilio {code or r.status_code}: {msg or err_msg}"
            if ch == "whatsapp" and (r.status_code == 400 or code in (21608, 21212)):
                err_msg += " — Use a WhatsApp-enabled sender (set TWILIO_WHATSAPP_FROM; see Twilio Console → WhatsApp Senders)."
        except Exception:
            pass
        _sms_log_event("outbound.failed", {"channel": ch, "to": to_e164, "status_code": r.status_code, "error": err_msg})
        return False, err_msg
    except Exception as e:
        _sms_log_event("outbound.exception", {"channel": ch, "to": to_e164, "error": str(e)})
        return False, f"Twilio send failed: {e}"

def _outbound_send_whatsapp_template(to_number_or_id: str, template_sid: str, variables: Dict[str, Any]) -> Tuple[bool, str]:
    """
    Send WhatsApp via approved Twilio Content Template (production-safe for business-initiated sends).
    """
    sid = os.environ.get("TWILIO_ACCOUNT_SID", "").strip()
    token = os.environ.get("TWILIO_AUTH_TOKEN", "").strip()
    frm = os.environ.get("TWILIO_WHATSAPP_FROM", "").strip()
    if not sid or not token:
        return False, "Twilio not configured (missing TWILIO_ACCOUNT_SID or TWILIO_AUTH_TOKEN)"
    if requests is None:
        return False, "Twilio not available (missing requests library)"
    if not frm:
        return False, "WhatsApp not configured (missing TWILIO_WHATSAPP_FROM)"
    if not template_sid:
        return False, "WhatsApp template SID missing"

    to = (to_number_or_id or "").strip()
    to_e164 = _normalize_phone_e164(to)
    if not to_e164:
        return False, "Invalid recipient number (must be valid international/E.164 format)"
    frm_e164 = _normalize_phone_e164(frm)
    if not frm_e164:
        return False, "WhatsApp sender invalid (must be E.164 in TWILIO_WHATSAPP_FROM)"
    if not _sms_can_send_to(to_e164):
        return False, "Recipient opted out (STOP)."

    try:
        url = f"https://api.twilio.com/2010-04-01/Accounts/{sid}/Messages.json"
        data = {
            "From": "whatsapp:" + frm_e164,
            "To": "whatsapp:" + to_e164,
            "ContentSid": template_sid,
            "ContentVariables": json.dumps(variables or {}, ensure_ascii=False),
        }
        r = requests.post(url, data=data, auth=(sid, token), timeout=12)
        try:
            rj = r.json() if r.text else {}
        except Exception:
            rj = {}
        if 200 <= r.status_code < 300:
            _sms_log_event("whatsapp_template.sent", {
                "to": to_e164,
                "sid": rj.get("sid"),
                "status": rj.get("status"),
                "template_sid": template_sid,
                "variables": variables or {},
            })
            return True, "WhatsApp template sent"
        err_msg = f"Twilio error {r.status_code}"
        try:
            code = rj.get("code") or rj.get("error_code")
            msg = (rj.get("message") or rj.get("error_message") or "").strip()
            if code is not None or msg:
                err_msg = f"Twilio {code or r.status_code}: {msg or err_msg}"
        except Exception:
            pass
        _sms_log_event("whatsapp_template.failed", {
            "to": to_e164,
            "status_code": r.status_code,
            "template_sid": template_sid,
            "variables": variables or {},
            "error": err_msg,
        })
        return False, err_msg
    except Exception as e:
        _sms_log_event("whatsapp_template.exception", {
            "to": to_e164,
            "template_sid": template_sid,
            "variables": variables or {},
            "error": str(e),
        })
        return False, f"WhatsApp template send failed: {e}"

def _get_whatsapp_template_sid(kind: str) -> str:
    k = (kind or "").strip().lower()
    mapping = {
        "reservation_received": os.environ.get("WA_TEMPLATE_RESERVATION_RECEIVED", "").strip(),
        "reservation_confirmed": os.environ.get("WA_TEMPLATE_RESERVATION_CONFIRMED", "").strip(),
        "reservation_update": os.environ.get("WA_TEMPLATE_RESERVATION_UPDATE", "").strip(),
        "vip_update": os.environ.get("WA_TEMPLATE_VIP_UPDATE", "").strip(),
    }
    return mapping.get(k, "")

def _send_notification_bundle(kind: str, to_number_or_id: str, sms_body: str, wa_variables: Dict[str, Any]) -> Dict[str, Any]:
    result: Dict[str, Any] = {"ok": True, "sms": None, "whatsapp": None}
    sms_ok, sms_msg = _outbound_send_twilio("sms", to_number_or_id, sms_body)
    result["sms"] = {"ok": sms_ok, "message": sms_msg}
    wa_template_sid = _get_whatsapp_template_sid(kind)
    if wa_template_sid:
        wa_ok, wa_msg = _outbound_send_whatsapp_template(to_number_or_id, wa_template_sid, wa_variables or {})
        result["whatsapp"] = {"ok": wa_ok, "message": wa_msg}
    else:
        result["whatsapp"] = {"ok": False, "message": f"Missing WhatsApp template SID for kind={kind}"}
    result["ok"] = bool(result["sms"]["ok"] or result["whatsapp"]["ok"])
    result["message"] = f"SMS: {result['sms']['message']} | WhatsApp: {result['whatsapp']['message']}"
    return result

def _build_notification_bundle_spec(action_type: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Build deterministic SMS + WhatsApp-template spec for bundled outbound actions.
    Returns:
      {ok, kind, to_number, sms_body, wa_variables} or {ok:False, error}
    """
    at = (action_type or "").strip().lower()
    pl = payload or {}
    to_number = str(pl.get("to") or pl.get("phone") or "").strip()
    venue_name = str(pl.get("venue_name") or "the venue").strip()
    match_label = str(pl.get("match_label") or "").strip()
    update_text = str(pl.get("message") or pl.get("body") or "").strip()
    if not to_number:
        return {"ok": False, "error": "Missing recipient number"}

    if at == "send_confirmation":
        sms_body = f"{venue_name} has confirmed your reservation."
        if match_label:
            sms_body += f" Match: {match_label}."
        return {
            "ok": True,
            "kind": "reservation_confirmed",
            "to_number": to_number,
            "sms_body": sms_body,
            "wa_variables": {"1": venue_name, "2": match_label or "your requested time"},
        }

    if at == "send_reservation_received":
        sms_body = f"Your reservation request for {venue_name} has been received."
        if match_label:
            sms_body += f" Match: {match_label}."
        return {
            "ok": True,
            "kind": "reservation_received",
            "to_number": to_number,
            "sms_body": sms_body,
            "wa_variables": {"1": venue_name, "2": match_label or ""},
        }

    if at == "send_update":
        if not update_text:
            return {"ok": False, "error": "Missing update text"}
        sms_body = f"{venue_name}: {update_text}"
        return {
            "ok": True,
            "kind": "reservation_update",
            "to_number": to_number,
            "sms_body": sms_body,
            "wa_variables": {"1": venue_name, "2": update_text},
        }

    if at == "send_vip_update":
        if not update_text:
            return {"ok": False, "error": "Missing update text"}
        sms_body = f"{venue_name}: {update_text}"
        return {
            "ok": True,
            "kind": "vip_update",
            "to_number": to_number,
            "sms_body": sms_body,
            "wa_variables": {"1": venue_name, "2": update_text},
        }

    return {"ok": False, "error": "Unsupported bundled notification type"}

def _outbound_send(action_type: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    """Execute outbound send (human-triggered). Never called automatically.
    Payload can contain 'message' or 'body' (both supported for compatibility).
    If payload has 'row' and draft placeholders remain, will reload lead data and re-format."""
    at = (action_type or "").strip().lower()
    pl = payload or {}
    
    # If message has placeholders and we have row data, try to reload and re-format
    message = str(pl.get("message") or pl.get("body") or "").strip()
    row_num = pl.get("row")
    if row_num and "{" in message:
        try:
            lead_data = _load_lead_from_sheet_row(int(row_num))
            if lead_data:
                message = _format_draft_template(message, lead_data)
        except Exception:
            pass  # Use original message if reload fails
    
    if at == "send_email":
        to_email = str(pl.get("to") or pl.get("email") or "").strip()
        subject = str(pl.get("subject") or "World Cup Concierge").strip()
        body = message
        if not to_email:
            return {"ok": False, "error": "Missing recipient email"}
        ok, msg = _outbound_send_email(to_email, subject, body)
        return {"ok": ok, "message": msg}
    if at in ("send_sms", "send_whatsapp"):
        ch = at.replace("send_", "")
        to_num = str(pl.get("to") or pl.get("phone") or "").strip()
        body = message
        if not to_num:
            return {"ok": False, "error": "Missing recipient number"}
        ok, msg = _outbound_send_twilio(ch, to_num, body)
        return {"ok": ok, "message": msg}
    if at in ("send_confirmation", "send_reservation_received", "send_update", "send_vip_update"):
        spec = _build_notification_bundle_spec(at, pl)
        if not spec.get("ok"):
            return {"ok": False, "error": spec.get("error") or "Invalid notification payload"}
        return _send_notification_bundle(
            kind=str(spec.get("kind") or ""),
            to_number_or_id=str(spec.get("to_number") or ""),
            sms_body=str(spec.get("sms_body") or ""),
            wa_variables=(spec.get("wa_variables") or {}),
        )
    return {"ok": False, "error": "Unsupported outbound action"}

@app.route("/admin/api/outbound/template-preview", methods=["POST"])
def admin_api_outbound_template_preview():
    ok, resp = _require_admin(min_role="manager")
    if not ok:
        return resp
    data = request.get_json(silent=True) or {}
    at = str(data.get("type") or data.get("action_type") or "").strip().lower()
    pl = data.get("payload") or {}
    if not isinstance(pl, dict):
        pl = {}
    if at not in ("send_confirmation", "send_reservation_received", "send_update", "send_vip_update"):
        return jsonify({"ok": False, "error": "Template preview supported only for bundled notification actions"}), 400
    spec = _build_notification_bundle_spec(at, pl)
    if not spec.get("ok"):
        return jsonify({"ok": False, "error": spec.get("error") or "Invalid payload"}), 400
    kind = str(spec.get("kind") or "")
    to_number = str(spec.get("to_number") or "")
    to_e164 = _normalize_phone_e164(to_number)
    wa_sid = _get_whatsapp_template_sid(kind)
    preview = {
        "kind": kind,
        "to_input": to_number,
        "to_e164": to_e164 or "",
        "sms_body": str(spec.get("sms_body") or ""),
        "wa_template_sid": wa_sid,
        "wa_template_configured": bool(wa_sid),
        "wa_variables": spec.get("wa_variables") or {},
    }
    return jsonify({"ok": True, "preview": preview})

@app.route("/sms/inbound", methods=["POST"])
def sms_inbound_webhook():
    """
    Twilio inbound SMS webhook:
    - validates signature (optional env gate)
    - handles STOP/START/HELP keywords
    - logs inbound events
    """
    form = request.form.to_dict(flat=True) if request.form else {}
    sig_ok = _twilio_validate_signature(request, form)
    if TWILIO_REQUIRE_SIGNATURE and not sig_ok:
        return jsonify({"ok": False, "error": "Invalid Twilio signature"}), 403

    from_raw = str(form.get("From") or "").strip()
    body = str(form.get("Body") or "").strip()
    kw = body.strip().upper()
    from_e164 = _normalize_phone_e164(from_raw) or from_raw
    _sms_log_event("inbound.received", {"from": from_raw, "from_e164": from_e164, "body": body, "signature_valid": sig_ok})

    if kw in _STOP_KEYWORDS:
        if from_e164 and from_e164.startswith("+"):
            _sms_set_subscribed(from_e164, False, "stop_keyword")
        _sms_log_event("inbound.optout", {"from": from_e164, "keyword": kw})
        return _twiml_response("You have been unsubscribed. Reply START to resubscribe.")

    if kw in _START_KEYWORDS:
        if from_e164 and from_e164.startswith("+"):
            _sms_set_subscribed(from_e164, True, "start_keyword")
        _sms_log_event("inbound.optin", {"from": from_e164, "keyword": kw})
        return _twiml_response("You have been resubscribed to messages.")

    if kw in _HELP_KEYWORDS:
        return _twiml_response("World Cup Concierge support: support@worldcupconcierge.app. Reply STOP to opt out.")

    # For regular replies, acknowledge without auto-reply text (avoids loops).
    return _twiml_response()

@app.route("/sms/status", methods=["POST"])
def sms_status_webhook():
    """
    Twilio delivery status callback webhook.
    Logs status updates for operational visibility.
    """
    form = request.form.to_dict(flat=True) if request.form else {}
    sig_ok = _twilio_validate_signature(request, form)
    if TWILIO_REQUIRE_SIGNATURE and not sig_ok:
        return jsonify({"ok": False, "error": "Invalid Twilio signature"}), 403

    _sms_log_event(
        "status.callback",
        {
            "message_sid": form.get("MessageSid") or form.get("SmsSid"),
            "status": form.get("MessageStatus") or form.get("SmsStatus"),
            "to": form.get("To"),
            "from": form.get("From"),
            "error_code": form.get("ErrorCode"),
            "error_message": form.get("ErrorMessage"),
            "signature_valid": sig_ok,
        },
    )
    return jsonify({"ok": True})
# ============================================================
# AI Action Queue (Approval / Deny / Override)
# - Queue stores proposed AI actions (e.g., tag VIP, update status, draft reply)
# - Managers can approve/deny
# - Owners can override payload before applying
# - Persisted to disk (/tmp) for durability
# ============================================================
AI_QUEUE_FILE = os.environ.get("AI_QUEUE_FILE", "/tmp/wc26_{venue}_ai_queue.json")

def _ai_queue_path_for_current_venue() -> str:
    """
    Resolve the on-disk queue file for the current venue.
    - If AI_QUEUE_FILE contains '{venue}', format it with _venue_id().
    - Otherwise, use it as-is (single-venue / legacy behavior).
    """
    base = AI_QUEUE_FILE
    if "{venue}" in base:
        try:
            vid = _venue_id()
        except Exception:
            vid = "default"
        try:
            return base.format(venue=vid)
        except Exception:
            return base
    return base

def _load_ai_queue() -> List[Dict[str, Any]]:
    q = _safe_read_json_file(_ai_queue_path_for_current_venue(), default=[])
    if isinstance(q, list):
        # newest first
        q.sort(key=lambda x: str(x.get("created_at") or ""), reverse=True)
        return q
    return []

def _save_ai_queue(queue: List[Dict[str, Any]]) -> None:
    _safe_write_json_file(_ai_queue_path_for_current_venue(), queue or [])


# ============================================================
# Health Checks
# ============================================================
def _health_check_sheets() -> Dict[str, Any]:
    t0 = time.time()
    try:
        # best-effort: if your app has get_sheet()/get_gspread_client helpers, use them.
        if "get_sheet" in globals():
            sh = get_sheet()
            # touch a cell
            _ = sh.row_values(1)
        elif "get_gspread_client" in globals():
            gc = get_gspread_client()
            # avoid assumptions about sheet id; just verify auth works
            _ = bool(gc)
        else:
            return {"name": "sheets", "ok": False, "severity": "error", "message": "Sheets client not configured in this build."}
        return {"name": "sheets", "ok": True, "severity": "ok", "message": "Sheets reachable"}
    except Exception as e:
        return {"name": "sheets", "ok": False, "severity": "error", "message": f"Sheets check failed: {e}"}
    finally:
        pass

def _health_check_queue() -> Dict[str, Any]:
    try:
        q = _load_ai_queue()
        # write a tiny health marker file to validate disk writes
        marker_path = os.environ.get("HEALTH_MARKER_FILE", "/tmp/wc26_health_marker.txt")
        with open(marker_path, "w", encoding="utf-8") as f:
            f.write(datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"))
        return {"name": "queue", "ok": True, "severity": "ok", "message": f"Queue OK ({len(q)} items)"}
    except Exception as e:
        return {"name": "queue", "ok": False, "severity": "error", "message": f"Queue check failed: {e}"}

def _health_check_fixtures() -> Dict[str, Any]:
    # best-effort: if you have a fixtures cache timestamp, use it; otherwise report unknown.
    try:
        stale_s = int(((ALERT_SETTINGS.get("checks") or {}).get("fixtures_stale_seconds")) or 86400)
        # Try common cache file env/paths
        candidates = [
            os.environ.get("FIXTURES_CACHE_FILE", ""),
            "/tmp/wc26_fixtures_cache.json",
            "/tmp/fixtures_cache.json",
        ]
        candidates = [c for c in candidates if c]
        newest_age = None
        newest_path = None
        now = time.time()
        for p in candidates:
            if os.path.exists(p):
                age = now - os.path.getmtime(p)
                if newest_age is None or age < newest_age:
                    newest_age = age
                    newest_path = p
        if newest_age is None:
            return {"name": "fixtures", "ok": True, "severity": "warn", "message": "No fixtures cache file detected (skipping staleness check)."}
        if newest_age > stale_s:
            return {"name": "fixtures", "ok": False, "severity": "warn", "message": f"Fixtures cache stale ({int(newest_age)}s) at {newest_path}"}
        return {"name": "fixtures", "ok": True, "severity": "ok", "message": f"Fixtures cache fresh ({int(newest_age)}s) at {newest_path}"}
    except Exception as e:
        return {"name": "fixtures", "ok": False, "severity": "warn", "message": f"Fixtures check failed: {e}"}

def _health_check_outbound() -> Dict[str, Any]:
    try:
        # We only validate readiness (env vars). Sending is still human-triggered.
        sg_ok = bool(os.environ.get("SENDGRID_API_KEY")) and bool(os.environ.get("SENDGRID_FROM"))
        tw_sid = bool(os.environ.get("TWILIO_ACCOUNT_SID")) and bool(os.environ.get("TWILIO_AUTH_TOKEN"))
        sms_ok = tw_sid and bool(os.environ.get("TWILIO_MESSAGING_SERVICE_SID", "").strip() or os.environ.get("TWILIO_FROM", "").strip())
        whatsapp_sender_ok = tw_sid and bool(os.environ.get("TWILIO_WHATSAPP_FROM", "").strip())
        wa_templates_ok = all([
            os.environ.get("WA_TEMPLATE_RESERVATION_RECEIVED", "").strip(),
            os.environ.get("WA_TEMPLATE_RESERVATION_CONFIRMED", "").strip(),
            os.environ.get("WA_TEMPLATE_RESERVATION_UPDATE", "").strip(),
            os.environ.get("WA_TEMPLATE_VIP_UPDATE", "").strip(),
        ])
        whatsapp_ok = whatsapp_sender_ok and wa_templates_ok
        if not sg_ok and not sms_ok:
            return {"name": "outbound", "ok": True, "severity": "warn", "message": "Outbound providers not configured (send disabled until configured)."}
        parts = []
        parts.append("SendGrid OK" if sg_ok else "SendGrid missing")
        parts.append("SMS OK" if sms_ok else "SMS missing (set TWILIO_MESSAGING_SERVICE_SID or TWILIO_FROM)")
        if tw_sid:
            if whatsapp_ok:
                parts.append("WhatsApp OK")
            elif whatsapp_sender_ok:
                parts.append("WhatsApp sender OK, templates missing")
            else:
                parts.append("WhatsApp: set TWILIO_WHATSAPP_FROM (must be a WhatsApp-enabled sender in Twilio Console)")
        else:
            parts.append("WhatsApp missing (Twilio not configured)")
        return {"name": "outbound", "ok": True, "severity": "ok" if (sg_ok or sms_ok) else "warn", "message": ", ".join(parts)}
    except Exception as e:
        return {"name": "outbound", "ok": False, "severity": "warn", "message": f"Outbound readiness check failed: {e}"}

def _run_health_checks() -> Dict[str, Any]:
    t0 = time.time()
    checks = [
        _health_check_sheets(),
        _health_check_queue(),
        _health_check_fixtures(),
        _health_check_outbound(),
    ]
    # overall
    overall_ok = all(c.get("ok") for c in checks if c.get("severity") == "error") and not any((c.get("severity") == "error" and not c.get("ok")) for c in checks)
    worst = "ok"
    for c in checks:
        sev = c.get("severity")
        if sev == "error" and not c.get("ok"):
            worst = "error"
            break
        if sev == "warn" and worst != "error":
            worst = "warn"
    return {
        "ok": True,
        "status": worst,
        "checks": checks,
        "elapsed_ms": int((time.time() - t0) * 1000),
        "ts": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }

def _maybe_alert_on_health(report: Dict[str, Any]) -> Dict[str, Any]:
    try:
        checks = report.get("checks") or []
        errors = [c for c in checks if c.get("severity") == "error" and not c.get("ok")]
        warns = [c for c in checks if c.get("severity") == "warn" and not c.get("ok")]
        out = {"alerts": []}
        for c in errors:
            key = f"health:{c.get('name')}:error"
            out["alerts"].append(_dispatch_alert(f"Health check failed: {c.get('name')}", c.get("message",""), key=key, severity="error"))
            try: _audit("monitor.health.fail", {"check": c.get("name"), "message": c.get("message")}) 
            except Exception: pass
        for c in warns:
            key = f"health:{c.get('name')}:warn"
            out["alerts"].append(_dispatch_alert(f"Health warning: {c.get('name')}", c.get("message",""), key=key, severity="warn"))
            try: _audit("monitor.health.warn", {"check": c.get("name"), "message": c.get("message")})
            except Exception: pass
        return out
    except Exception:
        return {"alerts": []}


def _queue_new_id() -> str:
    return secrets.token_hex(8)

def _queue_add(entry: Dict[str, Any]) -> Dict[str, Any]:
    queue = _load_ai_queue()
    queue.append(entry)
    _save_ai_queue(queue)
    return entry

def _queue_find(queue: List[Dict[str, Any]], qid: str) -> Optional[Dict[str, Any]]:
    for it in queue:
        if str(it.get("id")) == str(qid):
            return it
    return None

def _queue_apply_action(action: Dict[str, Any], ctx: Dict[str, Any]) -> Dict[str, Any]:
    '''
    Apply a single approved action against the system.
    Supported actions (minimal + safe):
      - vip_tag: update lead VIP field
      - status_update: update lead Status
      - reply_draft: store draft text in audit (does NOT send)
    '''
    typ = str(action.get("type") or "").strip()

    # Partner policy hard-gate (enforced even if AI/settings allow it)
    payload = action.get("payload") or {}
    try:
        partner_id = _derive_partner_id(payload=payload)
    except Exception:
        partner_id = "default"
    ok_pol, why_pol = _policy_check_action(partner_id, typ, payload, role=str((ctx or {}).get("role") or ""))
    if not ok_pol:
        _audit("policy.block", {"partner": partner_id, "type": typ, "reason": why_pol, "row": payload.get("row")})
        return {"ok": False, "error": why_pol}

    # Feature flag gate (secondary to allow_actions)
    settings = _get_ai_settings()
    if not _ai_feature_allows(typ, settings=settings):
        return {"ok": False, "error": f"{typ} disabled by feature flag"}
    allow = (settings.get("allow_actions") or {})

    # lead row reference (Google Sheet row number)
    row_num = int(payload.get("row") or payload.get("sheet_row") or 0)

    if typ == "reply_draft":
        if not allow.get("reply_draft", True):
            return {"ok": False, "error": "reply_draft not allowed"}
        draft = str(payload.get("draft") or "").strip()
        if not draft:
            return {"ok": False, "error": "Missing draft"}
        _audit("ai.reply_draft", {"row": row_num or None, "draft": draft[:2000]})
        return {"ok": True, "applied": "reply_draft"}

    # VIP / Status require a valid row number
    if row_num < 2:
        return {"ok": False, "error": "Missing/invalid sheet row"}

    # Use the same Sheets update logic as /admin/update-lead
    try:
        gc = get_gspread_client()
        ws = _open_default_spreadsheet(gc, venue_id=_venue_id()).sheet1
        header = ws.row_values(1) or []
        hmap = header_map(header)

        if typ == "vip_tag":
            if not allow.get("vip_tag", True):
                return {"ok": False, "error": "vip_tag not allowed"}
            vip_raw = str(payload.get("vip") or "").strip()
            # Deterministic default for vip_tag actions:
            # if model omitted explicit vip value, this action means "set VIP".
            if not vip_raw:
                vip_raw = "VIP"
            # Normalize VIP values: accept both "VIP"/"Regular" (AI) and "Yes"/"No" (admin)
            vip_normalized = "Yes"
            if vip_raw.lower() in ("vip", "yes", "true", "y", "1"):
                vip_normalized = "Yes"
            elif vip_raw.lower() in ("regular", "no", "false", "n", "0"):
                vip_normalized = "No"
            else:
                return {"ok": False, "error": "Invalid vip"}
            
            col = hmap.get("vip")
            if not col:
                return {"ok": False, "error": "VIP column not found"}
            ws.update_cell(row_num, col, vip_normalized)
            
            # Also update tier column to keep Segment display in sync
            tier_val = "VIP" if vip_normalized == "Yes" else "Regular"
            tier_col = hmap.get("tier")
            if tier_col:
                ws.update_cell(row_num, tier_col, tier_val)
            
            _audit("ai.vip_tag.apply", {"row": row_num, "vip": vip_normalized})
            return {"ok": True, "applied": "vip_tag"}

        if typ == "status_update":
            if not allow.get("status_update", False):
                return {"ok": False, "error": "status_update not allowed"}
            status = str(payload.get("status") or "").strip()
            allowed_status = ["New", "Confirmed", "Seated", "No-Show", "Handled"]
            if status not in allowed_status:
                return {"ok": False, "error": "Invalid status"}
            col = hmap.get("status")
            if not col:
                return {"ok": False, "error": "Status column not found"}
            ws.update_cell(row_num, col, status)
            _audit("ai.status_update.apply", {"row": row_num, "status": status})
            return {"ok": True, "applied": "status_update"}

        return {"ok": False, "error": "Unsupported action type"}
    except Exception as e:
        return {"ok": False, "error": f"Apply failed: {e}"}

def _ai_build_lead_prompt(lead: Dict[str, Any]) -> str:
    # Keep prompt small + deterministic
    return (
        "Lead intake:\n"
        f"- intent: {lead.get('intent','')}\n"
        f"- contact: {lead.get('contact','')}\n"
        f"- budget: {lead.get('budget','')}\n"
        f"- party_size: {lead.get('party_size','')}\n"
        f"- datetime: {lead.get('datetime','')}\n"
        f"- notes: {lead.get('notes','')}\n"
        f"- lang: {lead.get('lang','')}\n"
        "\nReturn JSON only."
    )

def _ai_suggest_actions_for_lead(lead: Dict[str, Any], sheet_row: int) -> Dict[str, Any]:
    """
    Ask the model for suggested workflow actions for a new lead.
    Returns dict: {ok, confidence, actions:[{type,payload,reason}], notes}
    """
    settings = _get_ai_settings()
    if not settings.get("enabled"):
        return {"ok": False, "error": "AI disabled"}

    # Build allowed action schema based on settings
    allow = settings.get("allow_actions") or {}
    # reply_draft requires draft text in payload, which this lead-intake
    # workflow does not generate. Proactive drafts are handled elsewhere.
    allowed_types = [k for k, v in allow.items() if v and k != "reply_draft"]
    if not allowed_types:
        return {"ok": False, "error": "No actions allowed"}

    system_msg = (settings.get("system_prompt") or "").strip()
    # Tight JSON contract
    contract = {
        "confidence": "number 0..1",
        "actions": [
            {
                "type": f"one of: {allowed_types}",
                "payload": "object (include row)",
                "reason": "short string"
            }
        ],
        "notes": "short string"
    }
    user_msg = _ai_build_lead_prompt(lead) + "\n\nJSON schema:\n" + json.dumps(contract)

    # Call OpenAI (best-effort). If not configured, return a safe suggestion locally.
    try:
        if not _OPENAI_AVAILABLE or client is None:
            raise RuntimeError("OpenAI SDK missing")
        resp = client.responses.create(
            model=settings.get("model") or os.environ.get("CHAT_MODEL", "gpt-4o-mini"),
            input=[
                {"role": "system", "content": system_msg},
                {"role": "user", "content": user_msg},
            ],
        )
        raw = (resp.output_text or "").strip()
    except Exception:
        # Local heuristic fallback: budget or VIP keywords -> suggest VIP tag
        raw = ""
        notes = (lead.get("notes") or "").lower()
        budget = str(lead.get("budget") or "").lower()
        is_vip = any(k in notes for k in ["vip", "bottle", "table", "suite"]) or any(k in budget for k in ["1000", "1500", "2000", "2500"])
        actions = []
        if is_vip and allow.get("vip_tag", False) and sheet_row:
            actions.append({"type":"vip_tag","payload":{"row": sheet_row, "vip":"VIP"},"reason":"Lead looks VIP/high-intent"})
        return {"ok": True, "confidence": 0.55 if actions else 0.3, "actions": actions, "notes":"Heuristic suggestion (AI not configured)"}

    # Parse JSON safely
    parsed = None
    try:
        parsed = json.loads(raw)
    except Exception:
        # Try to extract JSON blob from text
        try:
            m = re.search(r"\{.*\}", raw, flags=re.S)
            parsed = json.loads(m.group(0)) if m else None
        except Exception:
            parsed = None

    if not isinstance(parsed, dict):
        return {"ok": False, "error": "Bad AI JSON"}

    confidence = float(parsed.get("confidence") or 0)
    actions_in = parsed.get("actions") or []
    actions_out = []
    for a in actions_in if isinstance(actions_in, list) else []:
        if not isinstance(a, dict):
            continue
        typ = str(a.get("type") or "").strip()
        if typ not in allowed_types:
            continue
        if not _ai_feature_allows(typ, settings):
            continue
        payload = a.get("payload") or {}
        if not isinstance(payload, dict):
            payload = {}
        # Production contract: vip_tag payload should always be explicit.
        if typ == "vip_tag" and not str(payload.get("vip") or "").strip():
            payload["vip"] = "VIP"
        # Always enforce row linkage to prevent "random actions"
        if sheet_row:
            payload["row"] = int(sheet_row)
        # Row-required actions must never enter queue without a valid sheet row.
        # (send_* can work without row because they can target explicit contact payloads.)
        if typ in ("vip_tag", "status_update"):
            row_num = int(payload.get("row") or payload.get("sheet_row") or 0)
            if row_num < 2:
                continue
        # Partner policy filter (keeps bad suggestions out of the queue)
        try:
            partner_id = _derive_partner_id(lead=lead, payload=payload)
        except Exception:
            partner_id = "default"
        # Provide budget context for VIP min budget policies (best effort)
        if "budget" not in payload and lead.get("budget") is not None:
            payload["budget"] = lead.get("budget")
        ok_pol, _why_pol = _policy_check_action(partner_id, typ, payload, role="system")
        if not ok_pol:
            continue
        actions_out.append({"type": typ, "payload": payload, "reason": str(a.get("reason") or "").strip()[:240]})
    return {"ok": True, "confidence": confidence, "actions": actions_out, "notes": str(parsed.get("notes") or "").strip()[:240]}

def _ai_enqueue_or_apply_for_new_lead(lead: Dict[str, Any], sheet_row: int) -> None:
    """
    Run AI triage for a new lead and either:
      - enqueue actions for approval (default)
      - or auto-apply actions when enabled + safe gates pass
    Never raises.
    """
    try:
        settings = _get_ai_settings()
        if not settings.get("enabled"):
            return
        mode = str(settings.get("mode") or "suggest").lower()
        require_approval = bool(settings.get("require_approval", True))
        min_conf = float(settings.get("min_confidence") or 0.7)

        sug = _ai_suggest_actions_for_lead(lead, sheet_row)
        if not sug.get("ok"):
            return

        confidence = float(sug.get("confidence") or 0)
        actions = sug.get("actions") or []
        if not actions:
            return

        # Small lead context for reviewers (safe extra fields in payload).
        lead_ctx = {
            "intent": lead.get("intent", ""),
            "contact": lead.get("contact") or lead.get("phone") or "",
            "budget": lead.get("budget", ""),
            "party_size": lead.get("party_size", ""),
            "datetime": lead.get("datetime", ""),
            "notes": lead.get("notes", ""),
            "language": lead.get("lang", lead.get("language", "")),
        }

        # Default: queue actions for approval unless explicitly safe to auto-apply
        if mode != "auto" or require_approval or confidence < min_conf:
            for a in actions:
                a_type = a.get("type")
                a_payload = a.get("payload") or {}
                if not isinstance(a_payload, dict):
                    a_payload = {}
                # Attach context for easier staff triage in the UI.
                a_payload["lead"] = lead_ctx

                entry = {
                    "id": _queue_new_id(),
                    "type": str(a_type or "").strip(),
                    "payload": a_payload,
                    "confidence": confidence,
                    "rationale": str(a.get("reason") or sug.get("notes") or "")[:1500],
                    "status": "pending",
                    "created_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "created_by": "system",
                    "created_role": "system",
                    "reviewed_at": None,
                    "reviewed_by": None,
                    "reviewed_role": None,
                    "applied_result": None,
                }
                _queue_add(entry)
                _audit("ai.queue.created", {"id": entry["id"], "source": "lead_intake", "type": entry.get("type"), "confidence": confidence})
                _notify("ai.queue.created", {"id": entry["id"], "source": "lead_intake", "type": entry.get("type"), "confidence": confidence, "lead": lead_ctx}, targets=["owner","manager"])
            return

        # Auto-apply each action (still audited). Keep queue clean when all succeed.
        ctx = {"role": "owner", "actor": "system"}  # system executes as owner for apply, but it's audited
        applied = []
        errors = []
        for a in actions:
            res = _queue_apply_action(a, ctx)
            if res.get("ok"):
                applied.append(res.get("applied") or a.get("type"))
            else:
                errors.append(res.get("error") or "unknown")
                # Enqueue failed actions so staff can inspect/override.
                a_type = a.get("type")
                a_payload = a.get("payload") or {}
                if not isinstance(a_payload, dict):
                    a_payload = {}
                a_payload["lead"] = lead_ctx
                entry = {
                    "id": _queue_new_id(),
                    "type": str(a_type or "").strip(),
                    "payload": a_payload,
                    "confidence": confidence,
                    "rationale": str(a.get("reason") or sug.get("notes") or res.get("error") or "")[:1500],
                    "status": "pending",
                    "created_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "created_by": "system",
                    "created_role": "system",
                    "reviewed_at": None,
                    "reviewed_by": None,
                    "reviewed_role": None,
                    "applied_result": res,
                }
                _queue_add(entry)
        _audit("ai.queue.auto_applied", {"source": "lead_intake", "applied": applied, "errors": errors, "confidence": confidence})
    except Exception:
        return

def _load_rules_from_disk() -> None:
    global BUSINESS_RULES
    payload = _safe_read_json_file(BUSINESS_RULES_FILE)
    if isinstance(payload, dict) and payload:
        # Merge on top of defaults so missing keys don't break anything.
        BUSINESS_RULES = _deep_merge(BUSINESS_RULES, payload)


def _coerce_rules(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Validate/coerce incoming Rules payload from the Admin UI.

    Returns a partial dict of rule keys to merge on top of BUSINESS_RULES.
    """
    updated: Dict[str, Any] = {}

    # Max party size
    if "max_party_size" in payload:
        try:
            v = int(payload.get("max_party_size") or 0)
            if v > 0:
                updated["max_party_size"] = v
        except Exception:
            pass

    # Match-day banner text
    if "match_day_banner" in payload:
        try:
            s = str(payload.get("match_day_banner") or "")
            updated["match_day_banner"] = s[:280]  # keep it short/safe
        except Exception:
            pass

    # Closed dates: accept textarea string (newline separated) OR list[str]
    if "closed_dates" in payload:
        raw = payload.get("closed_dates")
        dates: list[str] = []
        if isinstance(raw, str):
            for line in raw.splitlines():
                line = line.strip()
                if line:
                    dates.append(line)
        elif isinstance(raw, list):
            for x in raw:
                try:
                    s = str(x).strip()
                    if s:
                        dates.append(s)
                except Exception:
                    continue
        updated["closed_dates"] = dates

    # Hours: dict with keys mon..sun; values like "11:00-22:00" or ""
    if "hours" in payload and isinstance(payload.get("hours"), dict):
        h_in = payload.get("hours") or {}
        out: Dict[str, str] = {}
        for d in ["mon", "tue", "wed", "thu", "fri", "sat", "sun"]:
            v = h_in.get(d, "")
            try:
                out[d] = str(v).strip()
            except Exception:
                out[d] = ""
        updated["hours"] = out

    return updated

def _normalize_menu_payload(payload: Any) -> Dict[str, Any]:
    """Validate/coerce incoming Menu JSON.

    Expected shape (example):
      {
        "en": {"sections": [{"title":"Chef Specials","items":[{"name":"A","price":"$16","desc":"...","tag":"Share"}]}]},
        "es": {...}
      }

    We keep the structure flexible but ensure:
    - top-level is an object/dict
    - each language value is an object with a 'sections' list
    - each section has 'title' (str) and 'items' (list)
    - each item has at least 'name' (str); other fields optional strings
    """
    if not isinstance(payload, dict):
        raise ValueError("Menu JSON must be an object (dictionary) at the top level")

    def _s(x: Any, max_len: int = 200) -> str:
        if x is None:
            return ""
        if isinstance(x, (int, float, bool)):
            x = str(x)
        if not isinstance(x, str):
            raise ValueError("Menu fields must be strings")
        x = x.strip()
        if len(x) > max_len:
            x = x[:max_len]
        return x

    out: Dict[str, Any] = {}
    for lang, lang_obj in payload.items():
        # allow reserved meta keys like _meta
        if str(lang).startswith('_'):
            continue
        lang_key = _s(lang, 20) or "en"
        if not isinstance(lang_obj, dict):
            raise ValueError(f"Language '{lang_key}' must be an object")
        sections = lang_obj.get("sections", [])
        if not isinstance(sections, list):
            raise ValueError(f"Language '{lang_key}': 'sections' must be a list")
        norm_sections = []
        for sec in sections:
            if not isinstance(sec, dict):
                raise ValueError(f"Language '{lang_key}': each section must be an object")
            title = _s(sec.get("title", ""), 120)
            items = sec.get("items", [])
            if not isinstance(items, list):
                raise ValueError(f"Language '{lang_key}': section '{title}': 'items' must be a list")
            norm_items = []
            for it in items:
                if not isinstance(it, dict):
                    raise ValueError(f"Language '{lang_key}': section '{title}': each item must be an object")
                name = _s(it.get("name", ""), 120)
                if not name:
                    raise ValueError(f"Language '{lang_key}': section '{title}': item missing 'name'")
                norm_items.append({
                    "name": name,
                    "price": _s(it.get("price", ""), 40),
                    "desc": _s(it.get("desc", ""), 240),
                    "tag": _s(it.get("tag", ""), 60),
                })
            norm_sections.append({"title": title or "Menu", "items": norm_items})
        out[lang_key] = {"sections": norm_sections}
    return out



def _bump_menu_meta(menu_obj: Dict[str, Any]) -> Dict[str, Any]:
    """Attach/increment menu override metadata."""
    try:
        prev = _MENU_OVERRIDE.get('_meta') if isinstance(_MENU_OVERRIDE, dict) else None  # type: ignore[name-defined]
    except Exception:
        prev = None
    ver = 0
    if isinstance(prev, dict):
        try:
            ver = int(prev.get('version') or 0)
        except Exception:
            ver = 0
    ver += 1
    ts = datetime.now(timezone.utc).isoformat(timespec='seconds').replace('+00:00','Z')
    menu_obj['_meta'] = {'version': ver, 'updated_at': ts}
    return menu_obj


def _persist_rules(_updated: Dict[str, Any]) -> None:
    """Persist current BUSINESS_RULES to disk (best effort)."""
    try:
        _safe_write_json_file(BUSINESS_RULES_FILE, BUSINESS_RULES)
    except Exception:
        pass


def _load_menu_from_disk() -> Optional[Dict[str, Any]]:
    payload = _safe_read_json_file(MENU_FILE)
    if isinstance(payload, dict) and payload:
        return payload
    return None

# Load persisted overrides at boot (best effort)
try:
    _load_rules_from_disk()
except Exception:
    pass

try:
    _load_ai_settings_from_disk()
except Exception:
    pass

try:
    _load_partner_policies_from_disk()
except Exception:
    pass



_MENU_OVERRIDE: Optional[Dict[str, Any]] = None
try:
    _MENU_OVERRIDE = _load_menu_from_disk()
except Exception:
    _MENU_OVERRIDE = None


def _admin_auth() -> Dict[str, str]:
    # Return admin auth context: {ok, role, actor, venue_id}.

    key = (request.args.get("key", "") or "").strip()
    if not key:
        return {"ok": False, "role": "", "actor": "", "venue_id": ""}

    vid = _venue_id()

    # Global owner key (all venues)
    if ADMIN_OWNER_KEY and key == ADMIN_OWNER_KEY:
        actor = hashlib.sha1(key.encode("utf-8")).hexdigest()[:10]
        return {"ok": True, "role": "owner", "actor": actor, "venue_id": vid}

    # Venue-scoped keys
    vc = _venue_cfg(vid)
    access = vc.get("access") if isinstance(vc.get("access"), dict) else {}
    akeys = access.get("admin_keys") if isinstance(access.get("admin_keys"), list) else []
    mkeys = access.get("manager_keys") if isinstance(access.get("manager_keys"), list) else []

    # ✅ Accept per-venue generated keys stored under cfg["keys"]
    k = vc.get("keys") if isinstance(vc.get("keys"), dict) else {}
    k_admin = str((k or {}).get("admin_key") or "").strip()
    k_mgr = str((k or {}).get("manager_key") or "").strip()

    if k_admin and key == k_admin:
        actor = hashlib.sha1(key.encode("utf-8")).hexdigest()[:10]
        return {"ok": True, "role": "owner", "actor": actor, "venue_id": vid}

    if k_mgr and key == k_mgr:
        actor = hashlib.sha1(key.encode("utf-8")).hexdigest()[:10]
        return {"ok": True, "role": "manager", "actor": actor, "venue_id": vid}

    if key in [str(x).strip() for x in akeys if str(x).strip()]:
        actor = hashlib.sha1(key.encode("utf-8")).hexdigest()[:10]
        return {"ok": True, "role": "owner", "actor": actor, "venue_id": vid}

    if key in [str(x).strip() for x in mkeys if str(x).strip()]:
        actor = hashlib.sha1(key.encode("utf-8")).hexdigest()[:10]
        return {"ok": True, "role": "manager", "actor": actor, "venue_id": vid}

    # Legacy manager keys (fallback)
    if ADMIN_MANAGER_KEYS and key in ADMIN_MANAGER_KEYS:
        actor = hashlib.sha1(key.encode("utf-8")).hexdigest()[:10]
        return {"ok": True, "role": "manager", "actor": actor, "venue_id": vid}

    return {"ok": False, "role": "", "actor": "", "venue_id": ""}


def _admin_ctx() -> Dict[str, str]:
    """Small wrapper used by UI + audit: never throws."""
    try:
        ctx = _admin_auth() or {}
    except Exception:
        ctx = {}
    if not isinstance(ctx, dict):
        ctx = {}
    return {
        "ok": bool(ctx.get("ok")),
        "role": str(ctx.get("role") or ""),
        "actor": str(ctx.get("actor") or ""),
        "venue_id": str(ctx.get("venue_id") or ""),
    }



@app.get("/admin/api/whoami")
def admin_api_whoami():
    """Return the server-truth role for the current key (owner/manager) so UI locks can't drift."""
    ctx = _admin_ctx()
    return jsonify(ok=bool(ctx.get("ok")), role=ctx.get("role", ""), actor=ctx.get("actor", ""), venue_id=ctx.get("venue_id",""))


def _write_venue_config(venue_id: str, pack: Dict[str, Any]) -> Tuple[bool, str, str]:
    """Persist venue config into VENUES_DIR as <venue_id>.json (best effort)."""
    wrote = False
    write_path = ""
    err = ""
    try:
        os.makedirs(VENUES_DIR, exist_ok=True)
        write_path = os.path.join(VENUES_DIR, f"{venue_id}.json")
        with open(write_path, "w", encoding="utf-8") as f:
            json.dump(pack, f, indent=2, sort_keys=True)
        wrote = True
        # refresh cache immediately
        _invalidate_venues_cache()
    except Exception as e:
        err = str(e)
    return wrote, write_path, err

from flask import request, jsonify

@app.post("/api/lead")
def api_lead():
    # Read JSON body from landing page
    lead = request.get_json(silent=True) or {}

    # Minimal validation (don’t block too hard)
    name = str(lead.get("name") or "").strip()
    venue = str(lead.get("venue") or "").strip()
    email = str(lead.get("email") or "").strip()

    if not email:
        return jsonify(ok=False, error="missing_email"), 400

    # Save to Google Sheet
    ok_sheet, msg_sheet = _append_landing_lead_to_sheet(lead)

    # Email alert (best-effort; don’t fail the lead if email isn’t configured)
    subj = "New demo request — World Cup Concierge"
    body = (
    f"New landing lead:\n\n"
    f"Name: {name}\n"
    f"Venue: {venue}\n"
    f"Email: {email}\n"
    f"Phone: {lead.get('phone','')}\n"
    f"Best contact: {lead.get('contact_method','')} ({lead.get('contact_time','')})\n"
    f"TS: {lead.get('ts','')}\n"
    f"Source: {lead.get('source','landing')}\n"
)
    _outbound_send_email(LANDING_LEAD_ALERT_TO, subj, body)

    # Respond to frontend
    if ok_sheet:
        return jsonify(ok=True, saved=True), 200
    return jsonify(ok=False, saved=False, error=msg_sheet), 500


@app.post("/super/api/venues/set_active")
def super_api_venues_set_active():
    """Activate or deactivate venue per spec."""
    ok, resp = _require_super_admin()
    if not ok:
        return resp

    body = request.get_json(silent=True) or {}
    venue_id = _slugify_venue_id(str(body.get("venue_id") or "").strip())
    if not venue_id:
        return jsonify({"ok": False, "error": "Missing venue_id"}), 400

    active_in = body.get("active")
    if isinstance(active_in, bool):
        active = active_in
    else:
        active = str(active_in or "").strip().lower() in ("1", "true", "yes", "y", "on")

    venues = _load_venues_from_disk() or {}
    cfg = venues.get(venue_id) if isinstance(venues, dict) else None
    if not isinstance(cfg, dict):
        return jsonify({"ok": False, "error": "Venue not found"}), 404

    cfg["active"] = bool(active)
    cfg["status"] = "active" if active else "inactive"
    cfg["updated_at"] = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

    wrote, write_path, err = _write_venue_config(venue_id, cfg)

    # CRITICAL: cache bust
    try:
        _invalidate_venues_cache()
    except Exception:
        pass

    return jsonify({
        "ok": True,
        "venue_id": venue_id,
        "active": bool(active),
        "persisted": wrote,
        "path": write_path,
        "error": err,
    })

@app.post("/super/api/venues/set_identity")
def super_api_venues_set_identity():
    """Update venue identity metadata per spec."""
    ok, resp = _require_super_admin()
    if not ok:
        return resp

    body = request.get_json(silent=True) or {}
    venue_id = _slugify_venue_id(str(body.get("venue_id") or "").strip())
    if not venue_id:
        return jsonify({"ok": False, "error": "Missing venue_id"}), 400

    location_line = str(body.get("location_line") or "").strip()
    if not location_line:
        return jsonify({"ok": False, "error": "location_line is required"}), 400

    show_in = body.get("show_location_line")
    show = True if show_in is None else bool(show_in)

    venues = _load_venues_from_disk() or {}
    cfg = venues.get(venue_id) if isinstance(venues, dict) else None
    if not isinstance(cfg, dict):
        return jsonify({"ok": False, "error": "Venue not found"}), 404

    # Set new schema (top-level)
    cfg["show_location_line"] = bool(show)
    cfg["location_line"] = location_line
    cfg["updated_at"] = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

    wrote, write_path, err = _write_venue_config(venue_id, cfg)
    if not wrote:
        return jsonify({"ok": False, "error": err or "Failed to write venue config"}), 500

    try:
        _invalidate_venues_cache()
    except Exception:
        pass

    return jsonify({
        "ok": True,
        "venue_id": venue_id,
        "show_location_line": bool(cfg.get("show_location_line")),
        "location_line": str(cfg.get("location_line") or ""),
        "persisted": wrote,
        "path": write_path,
        "error": err,
    })


@app.post("/super/api/venues/delete")
def super_api_venues_delete():
    """
    Soft-delete a venue from the Super Admin console.
    - Marks the venue as inactive + deleted in its JSON config
    - Removes it from the in-memory venues cache
    - Does NOT touch historical Sheets data or leads
    """
    ok, resp = _require_super_admin()
    if not ok:
        return resp

    body = request.get_json(silent=True) or {}
    venue_id = _slugify_venue_id(str(body.get("venue_id") or "").strip())
    if not venue_id:
        return jsonify({"ok": False, "error": "Missing venue_id"}), 400

    venues = _load_venues_from_disk() or {}
    cfg = venues.get(venue_id) if isinstance(venues, dict) else None
    if not isinstance(cfg, dict):
        return jsonify({"ok": False, "error": "Venue not found"}), 404

    # Soft-delete: keep config on disk but mark deleted + inactive so UI and intake stop using it.
    cfg["active"] = False
    cfg["status"] = "deleted"
    cfg["deleted"] = True
    cfg["updated_at"] = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

    wrote, write_path, err = _write_venue_config(venue_id, cfg)

    try:
        _invalidate_venues_cache()
    except Exception:
        pass

    # Best-effort audit for traceability
    try:
        _audit("venue.delete", {"venue_id": venue_id})
    except Exception:
        pass

    return jsonify({
        "ok": True,
        "venue_id": venue_id,
        "deleted": True,
        "persisted": wrote,
        "path": write_path,
        "error": err,
    })

@app.post("/admin/api/venues/set_active")
def admin_api_venues_set_active():
    """Owner-only: set a venue active/inactive flag (data preserved, fan intake blocked)."""
    ok, resp = _require_admin(min_role="owner")
    if not ok:
        return resp

    body = request.get_json(silent=True) or {}
    venue_id = _slugify_venue_id(str(body.get("venue_id") or "").strip())
    if not venue_id:
        return jsonify({"ok": False, "error": "Missing venue_id"}), 400

    active_in = body.get("active")
    if isinstance(active_in, bool):
        active = active_in
    else:
        active = str(active_in or "").strip().lower() in ("1","true","yes","y","on")

    venues = _load_venues_from_disk() or {}
    cfg = venues.get(venue_id) if isinstance(venues, dict) else None
    if not isinstance(cfg, dict):
        return jsonify({"ok": False, "error": "Venue not found"}), 404

    cfg["active"] = bool(active)
    cfg["status"] = "active" if active else "inactive"
    cfg["updated_at"] = datetime.now(timezone.utc).isoformat().replace("+00:00","Z")

    wrote, write_path, err = _write_venue_config(venue_id, cfg)
    try:
        _audit("admin.venues.set_active", {"venue_id": venue_id, "active": bool(active), "persisted": wrote, "path": write_path, "error": err})
    except Exception:
        pass

    return jsonify({"ok": True, "venue_id": venue_id, "active": bool(active), "persisted": wrote, "path": write_path, "error": err})

@app.post("/admin/api/venues/create")
def admin_api_venues_create():
    """Owner-only: generate a Venue Pack (does NOT write files)."""
    ok, resp = _require_admin(min_role="owner")
    if not ok:
        return resp

    body = request.get_json(silent=True) or {}

    venue_name = str(body.get("venue_name") or "").strip() or "New Venue"
    venue_id = _slugify_venue_id(str(body.get("venue_id") or venue_name))
    plan = str(body.get("plan") or "standard").strip().lower() or "standard"

    admin_key = secrets.token_hex(16)
    manager_key = secrets.token_hex(16)

    base = _public_base_url()

    pack = {
        "venue_name": venue_name,
        "venue_id": venue_id,
        "status": "active",
        "plan": plan,

        # ✅ Consistent, env-safe links (matches create_and_save)
        "admin_url": f"{base}/admin?key={admin_key}&venue={venue_id}",
        "manager_url": f"{base}/admin?key={manager_key}&venue={venue_id}",
        "qr_url": f"{base}/v/{venue_id}",

        # ✅ Consistent schema (no more "keys" vs "access" mismatch)
        "access": {
            "admin_keys": [admin_key],
            "manager_keys": [manager_key],
        },
        "data": {
            # allow passing a sheet id at creation time (optional)
            "google_sheet_id": str(body.get("google_sheet_id") or "").strip(),
            "redis_namespace": f"{_REDIS_NS}:{venue_id}",
        },
        "features": body.get("features")
        if isinstance(body.get("features"), dict)
        else {"vip": True, "waitlist": False, "ai_queue": True},
        "created_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),

        # keep your yaml template (optional convenience)
        "yaml_template": (
            "venue_id: " + venue_id + "\n"
            "venue_name: \"" + venue_name.replace('\"','') + "\"\n"
            "status: active\n"
            "plan: " + plan + "\n\n"
            "access:\n"
            "  admin_keys:\n"
            "    - \"" + admin_key + "\"\n"
            "  manager_keys:\n"
            "    - \"" + manager_key + "\"\n\n"
            "data:\n"
            "  google_sheet_id: \"\"  # paste sheet id\n"
            "  redis_namespace: \"wc26:" + venue_id + "\"\n\n"
            "features:\n"
            "  vip: true\n"
            "  waitlist: false\n"
            "  ai_queue: true\n"
        ),
    }

    return jsonify({
        "ok": True,
        "admin_key": admin_key,
        "manager_key": manager_key,
        "pack": pack,
    })

    return jsonify({"ok": True, "pack": pack})

@app.post("/admin/api/venues/create_and_save")
def admin_api_venues_create_and_save():
    """Owner-only: generate venue pack AND persist to config/venues/<venue>.json."""
    ok, resp = _require_admin(min_role="owner")
    if not ok:
        return resp

    body = request.get_json(silent=True) or {}

    venue_name = str(body.get("venue_name") or "").strip() or "New Venue"
    venue_id = _slugify_venue_id(str(body.get("venue_id") or venue_name))
    plan = str(body.get("plan") or "standard").strip().lower() or "standard"

    admin_key = secrets.token_hex(16)
    manager_key = secrets.token_hex(16)

    base = _public_base_url()

    pack = {
        "venue_name": venue_name,
        "venue_id": venue_id,
        "status": "active",
        "plan": plan,

        # ✅ environment-safe links
        "admin_url": f"{base}/admin?key={admin_key}&venue={venue_id}",
        "manager_url": f"{base}/admin?key={manager_key}&venue={venue_id}",
        "qr_url": f"{base}/v/{venue_id}",

        "access": {
            "admin_keys": [admin_key],
            "manager_keys": [manager_key],
        },
        "data": {
            "google_sheet_id": str(body.get("google_sheet_id") or "").strip(),
            "redis_namespace": f"{_REDIS_NS}:{venue_id}",
        },
        "features": body.get("features")
        if isinstance(body.get("features"), dict)
        else {"vip": True, "waitlist": False, "ai_queue": True},
        "created_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    }

    wrote, write_path, err = _write_venue_config(venue_id, pack)
    if not wrote:
        return jsonify({
            "ok": False,
            "error": err or "Failed to write venue config",
        }), 500

    _invalidate_venues_cache()

    return jsonify({
        "ok": True,
        "venue_id": venue_id,
        "path": write_path,
        "admin_key": admin_key,
        "manager_key": manager_key,
        "pack": pack,   # ✅ THIS is required
    })

    wrote, write_path, err = _write_venue_config(venue_id, pack)

    if not wrote:
        return jsonify({
            "ok": False,
            "error": err or "Failed to write venue config",
        }), 500

    _invalidate_venues_cache()

    return jsonify({
        "ok": True,
        "venue_id": venue_id,
        "path": write_path,
        "admin_key": admin_key,
        "manager_key": manager_key,
        "pack": pack,
    })

@app.post("/super/admin/api/venue/create")
def super_admin_api_venue_create():
    """
    Super Admin alias for venue creation.
    Delegates to the existing owner-only create_and_save logic.
    """
    return admin_api_venues_create_and_save()

    # === ONE-CLICK ONBOARDING (v1.0 polish) ===
    # If a Sheet ID is provided at creation time, validate it immediately
    # and persist PASS/FAIL + READY state onto the venue config.
    try:
        sid = str(((pack.get("data") or {}).get("google_sheet_id")) or "").strip()
    except Exception:
        sid = ""
    if sid:
        chk = _check_sheet_id(sid)
        try:
            pack["_sheet_check"] = chk
        except Exception:
            pass
        try:
            pack["ready"] = bool(chk.get("ok"))
            pack["ready_state"] = "PASS" if bool(chk.get("ok")) else "FAIL"
            pack["ready_checked_at"] = chk.get("checked_at")
        except Exception:
            pass
        # write again so this is atomic for operators (one call → ready/pass-fail recorded)
        try:
            wrote2, write_path2, err2 = _write_venue_config(venue_id, pack)
            wrote = bool(wrote or wrote2)
            if write_path2:
                write_path = write_path2
            if err2:
                err = err2
        except Exception:
            pass

    return jsonify({"ok": True, "pack": pack, "persisted": wrote, "path": write_path, "error": err})
    return jsonify({"ok": True, "pack": pack, "persisted": wrote, "path": write_path, "error": err})


@app.route("/")
def marketing_landing():
    return send_from_directory("landing", "index.html")

# Serve landing CSS/JS at root paths (because index.html references /styles.css and /app.js)
@app.route("/styles.css")
def landing_styles():
    return send_from_directory("landing", "styles.css", mimetype="text/css")

@app.route("/app.js")
def landing_js():
    return send_from_directory("landing", "app.js", mimetype="application/javascript")

@app.route("/assets/<path:filename>")
def landing_assets(filename):
    return send_from_directory("landing/assets", filename)



@app.route("/admin/api/_build", methods=["GET"])
def admin_api_build():
    """
    Lightweight build/health metadata endpoint (must NEVER 500).
    """
    try:
        ok, resp = _require_admin(min_role="manager")
        if not ok:
            return resp

        return jsonify({
            "ok": True,
            "version": APP_VERSION,
            "code_version": CODE_VERSION,
            "app_version_env": os.environ.get("APP_VERSION"),
            "env": (os.environ.get("APP_ENV") or "").strip() or "prod",
            "redis_enabled": bool(globals().get("_REDIS_ENABLED") and globals().get("_REDIS")),
            "redis_namespace": globals().get("_REDIS_NS", ""),
            "multi_venue": bool(globals().get("MULTI_VENUE_ENABLED", False)),
            "pid": os.getpid(),
        })
    except Exception as e:
        # Fail-closed but informative for admins
        return jsonify({
            "ok": False,
            "error": "build_endpoint_exception",
            "detail": str(e),
        }), 500



@app.get("/admin/api/leads/export")
def admin_api_leads_export():
    """Export leads as CSV. Supports ?days=7 or ?days=30."""
    ok, resp = _require_admin(min_role="manager")
    if not ok:
        return resp
    days = int(request.args.get("days") or 0)
    try:
        rows = read_leads(limit=2000) or []
    except Exception:
        rows = []
    if days in (7, 30):
        rows = _filter_leads_by_days(rows, days)

    buf = io.StringIO()
    w = csv.writer(buf)
    for r in (rows or []):
        w.writerow(r if isinstance(r, list) else [])
    csv_bytes = buf.getvalue().encode("utf-8")

    suffix = f"{days}d" if days in (7, 30) else "all"
    fname = f"leads_{suffix}_{datetime.now(timezone.utc).strftime('%Y-%m-%d')}.csv"
    resp2 = make_response(csv_bytes)
    resp2.headers["Content-Type"] = "text/csv; charset=utf-8"
    resp2.headers["Content-Disposition"] = f'attachment; filename="{fname}"'
    return resp2

@app.route("/admin/api/_redis_smoke", methods=["GET"])
def admin_api_redis_smoke():
    """Enterprise smoke: verify Redis write/read works and NO disk fallback occurs."""
    ok, resp = _require_admin(min_role="manager")
    if not ok:
        return resp

    # runtime re-init (Gunicorn-safe)
    try:
        _redis_init_if_needed()
    except Exception:
        pass

    if not (_REDIS_ENABLED and _REDIS):
        return jsonify({
            "ok": False,
            "error": "Redis not enabled",
            "redis_enabled": bool(_REDIS_ENABLED),
            "redis_url_present": bool((os.environ.get("REDIS_URL","") or "").strip()),
            "redis_namespace": _REDIS_NS,
        }), 503

    # reset fallback flags for this check
    global _REDIS_FALLBACK_USED, _REDIS_FALLBACK_LAST_PATH
    _REDIS_FALLBACK_USED = False
    _REDIS_FALLBACK_LAST_PATH = ""

    payload = {
        "ts": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "pid": os.getpid(),
        "nonce": secrets.token_hex(6),
    }

    # Use the app's persistence layer (tests the real path mapping)
    try:
        _safe_write_json_file(CI_SMOKE_FILE, payload)
        got = _safe_read_json_file(CI_SMOKE_FILE, default=None)
    except Exception as e:
        return jsonify({
            "ok": False,
            "error": f"Smoke failed: {e}",
            "redis_enabled": True,
            "fallback_used": bool(_REDIS_FALLBACK_USED),
            "fallback_last_path": _REDIS_FALLBACK_LAST_PATH,
        }), 500

    match = (got == payload)
    ok_all = bool(match) and (not _REDIS_FALLBACK_USED)

    return jsonify({
        "ok": ok_all,
        "redis_enabled": True,
        "redis_namespace": _REDIS_NS,
        "key": "ci_smoke",
        "fallback_used": bool(_REDIS_FALLBACK_USED),
        "fallback_last_path": _REDIS_FALLBACK_LAST_PATH,
        "match": bool(match),
        "payload": payload,
        "got": got,
    }), (200 if ok_all else 500)

    rs = _redis_runtime_status()
    return jsonify({
        "ok": True,
        "redis_enabled": bool(rs.get("redis_enabled")),
        "redis_namespace": rs.get("redis_namespace") or "",
        "redis_url_present": bool(rs.get("redis_url_present")),
        "redis_url_effective": rs.get("redis_url_effective") or "",
        "redis_error": rs.get("redis_error") or "",
        "build_verify": os.environ.get("BUILD_VERIFY", ""),
        "chat_logicfix_build": os.environ.get("CHAT_LOGICFIX_BUILD", ""),
    })


def _is_super_admin_request() -> bool:
    """Return True iff caller presents SUPER_ADMIN_KEY.

    Accepted (per spec):
      - X-Super-Key header (preferred)
      - ?super_key= query param
      - super_key cookie (set after successful console load)
      - ?key= query param (back-compat for /super/* calls)
    """
    try:
        sk = SUPER_ADMIN_KEY
        if not sk:
            return False
        for src in (
            request.headers.get("X-Super-Key"),
            request.args.get("super_key"),
            request.cookies.get("super_key"),
            request.args.get("key"),
        ):
            if src and str(src).strip() == sk:
                return True
    except Exception:
        pass
    return False

def _require_admin(min_role: str = "manager"):
    # key can come from query (?key=), header, or cookie
    key = (request.args.get("key") or request.headers.get("X-Admin-Key") or request.cookies.get("admin_key") or "").strip()
    if not key:
        return False, (jsonify({"ok": False, "error": "unauthorized"}), 401)
    
    # 🔐 GLOBAL OWNER KEY — MUST SHORT-CIRCUIT (even if venue cfg fails)
    if key and (ADMIN_OWNER_KEY or "") and key == (ADMIN_OWNER_KEY or ""):
        g.admin_role = "owner"
        g.admin_actor = "owner:" + hashlib.sha1(key.encode("utf-8")).hexdigest()[:10]
        if _ROLE_RANK.get(g.admin_role, 0) >= _ROLE_RANK.get(min_role, 0):
            if not _venue_is_active(_venue_id()):
                return False, (jsonify({"ok": False, "error": "venue_inactive"}), 403)
            return True, None
        return False, (jsonify({"ok": False, "error": "forbidden"}), 403)

    # ---- 1) VENUE-SCOPED KEYS (preferred) ----
    try:
        cfg = _venue_cfg()
        access = cfg.get("access") if isinstance(cfg.get("access"), dict) else {}
        v_admin = access.get("admin_keys") or []
        v_mgr = access.get("manager_keys") or []

        # Accept per-venue generated keys stored under cfg["keys"]
        k = cfg.get("keys") if isinstance(cfg.get("keys"), dict) else {}
        k_admin = str((k or {}).get("admin_key") or "").strip()
        k_mgr = str((k or {}).get("manager_key") or "").strip()
        if k_admin and k_admin not in v_admin:
            v_admin = list(v_admin) + [k_admin]
        if k_mgr and k_mgr not in v_mgr:
            v_mgr = list(v_mgr) + [k_mgr]

        if key in v_admin:
            g.admin_role = "owner"
            g.admin_actor = "owner:" + hashlib.sha1(key.encode("utf-8")).hexdigest()[:10]
        elif key in v_mgr:
            g.admin_role = "manager"
            g.admin_actor = "manager:" + hashlib.sha1(key.encode("utf-8")).hexdigest()[:10]
        else:
            # no match in venue keys; continue to env fallback
            pass

        if getattr(g, "admin_role", None):
            if _ROLE_RANK.get(g.admin_role, 0) >= _ROLE_RANK.get(min_role, 0):
                if not _venue_is_active(_venue_id()):
                    return False, (jsonify({"ok": False, "error": "venue_inactive"}), 403)
                return True, None
            return False, (jsonify({"ok": False, "error": "forbidden"}), 403)
    except Exception:
        pass

    # ---- 2) ENV FALLBACK KEYS (back-compat) ----
    if key == (ADMIN_OWNER_KEY or ""):
        g.admin_role = "owner"
        g.admin_actor = "owner:" + hashlib.sha1(key.encode("utf-8")).hexdigest()[:10]
    elif key in (ADMIN_MANAGER_KEYS or []):
        g.admin_role = "manager"
        g.admin_actor = "manager:" + hashlib.sha1(key.encode("utf-8")).hexdigest()[:10]
    else:
        return False, (jsonify({"ok": False, "error": "unauthorized"}), 401)

    if _ROLE_RANK.get(g.admin_role, 0) < _ROLE_RANK.get(min_role, 0):
        return False, (jsonify({"ok": False, "error": "forbidden"}), 403)

    if not _venue_is_active(_venue_id()):
        return False, (jsonify({"ok": False, "error": "venue_inactive"}), 403)
    return True, None

def _redis_runtime_status() -> Dict[str, Any]:
    """Runtime Redis truth (Gunicorn-safe): re-init + ping where possible."""
    try:
        _redis_init_if_needed()
    except Exception:
        pass

    url_present = bool((os.environ.get("REDIS_URL", "") or "").strip())
    err = str(globals().get("_REDIS_ERROR", "") or "")
    url_eff = str(globals().get("_REDIS_URL_EFFECTIVE", "") or "")

    return {
        "redis_enabled": bool(_REDIS_ENABLED),
        "redis_namespace": _REDIS_NS,
        "redis_url_present": url_present,
        "redis_url_effective": url_eff,
        "redis_error": err,
    }

def get_menu_for_lang(lang: str) -> Dict[str, Any]:
    """Return a normalized menu payload for a given language.

    Public /menu.json expects:
      { "lang": "en", "title": "Menu", "sections": [ { "title": "...", "items": [...] }, ... ] }

    - Admin overrides (uploaded via /admin) are stored in MENU_FILE and win.
    - Otherwise, we transform the built-in flat MENU[lang]["items"] into sections.
    """
    global _MENU_OVERRIDE
    lang = norm_lang(lang)

    # 1) Admin override (already normalized by _normalize_menu_payload)
    if isinstance(_MENU_OVERRIDE, dict):
        m = _MENU_OVERRIDE.get(lang)
        if isinstance(m, dict) and isinstance(m.get("sections"), list) and m.get("sections"):
            base_title = "Menu"
            try:
                base = MENU.get(lang, MENU.get("en", {}))
                if isinstance(base, dict) and base.get("title"):
                    base_title = str(base.get("title"))
            except Exception:
                pass
            meta = _MENU_OVERRIDE.get("_meta") if isinstance(_MENU_OVERRIDE, dict) else None
            return {"title": base_title, "sections": m.get("sections"), "meta": meta or {}}

    # 2) Built-in fallback: group flat items into sections
    base = MENU.get(lang, MENU.get("en", {}))
    base_title = (base.get("title") if isinstance(base, dict) else None) or "Menu"
    items = []
    if isinstance(base, dict) and isinstance(base.get("items"), list):
        items = base.get("items") or []

    # human-ish section titles (default built-in categories)
    title_map = {
        "chef": "Chef Specials",
        "bites": "Bites",
        "classics": "Classics",
        "sweets": "Sweets",
        "drinks": "Drinks",
    }

    buckets: Dict[str, List[Dict[str, str]]] = {}
    for it in items:
        if not isinstance(it, dict):
            continue
        cid = str(it.get("category_id") or "menu").strip().lower() or "menu"
        buckets.setdefault(cid, []).append({
            "name": str(it.get("name") or "").strip(),
            "price": str(it.get("price") or "").strip(),
            "desc": str(it.get("desc") or "").strip(),
            "tag": str(it.get("tag") or "").strip(),
        })

    sections = []
    for cid, arr in buckets.items():
        arr2 = [x for x in arr if x.get("name")]
        if not arr2:
            continue
        sections.append({"title": title_map.get(cid, cid.replace("_", " ").title()), "items": arr2})

    # stable-ish ordering for the default categories
    order_titles = ["Chef Specials", "Bites", "Classics", "Sweets", "Drinks", "Menu"]
    sections.sort(key=lambda s: (order_titles.index(s.get("title")) if s.get("title") in order_titles else 999, s.get("title","")))

    meta = _MENU_OVERRIDE.get("_meta") if isinstance(_MENU_OVERRIDE, dict) else None
    return {"title": str(base_title), "sections": sections, "meta": meta or {"version": 0, "updated_at": ""}}



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
FIXTURE_CACHE_FILE = os.environ.get("FIXTURE_CACHE_FILE", "/tmp/wc26_{venue}_fixtures.json")
POLL_STORE_FILE = os.environ.get("POLL_STORE_FILE", "/tmp/wc26_{venue}_poll_votes.json")


def _safe_read_json_file(path: str, default: Any = None) -> Any:
    """Read JSON from Redis (if enabled) or disk safely."""
    # BUG FIX: Lookup Redis key BEFORE expanding {venue} placeholder
    # The _REDIS_PATH_KEY_MAP uses template paths with {venue}
    original_path = str(path)
    try:
        vid = _venue_id()
        # Check Redis using the TEMPLATE path (with {venue})
        if _REDIS_ENABLED:
            suffix = _REDIS_PATH_KEY_MAP.get(original_path)
            if suffix:
                full_key = f"{_REDIS_NS}:{vid}:{suffix}"
                return _redis_get_json(full_key, default=default)
    except Exception:
        pass

    # Expand {venue} for disk fallback
    try:
        vid = _venue_id()
        if '{venue}' in str(path):
            path = str(path).replace('{venue}', vid)
    except Exception:
        pass

    try:
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception:
        return default
    return default

def _safe_write_json_file(path: str, payload: Any) -> None:
    global _REDIS_FALLBACK_USED, _REDIS_FALLBACK_LAST_PATH
    """Write JSON to Redis (if enabled) or disk safely."""
    # BUG FIX: Lookup Redis key BEFORE expanding {venue} placeholder
    # The _REDIS_PATH_KEY_MAP uses template paths with {venue}
    original_path = str(path)
    try:
        vid = _venue_id()
        # Check Redis using the TEMPLATE path (with {venue})
        if _REDIS_ENABLED:
            suffix = _REDIS_PATH_KEY_MAP.get(original_path)
            if suffix:
                full_key = f"{_REDIS_NS}:{_venue_id()}:{suffix}"
                ok = _redis_set_json(full_key, payload)
                if ok:
                    return
                # Redis was enabled, but write failed — mark fallback for enterprise gate
                _REDIS_FALLBACK_USED = True
                _REDIS_FALLBACK_LAST_PATH = original_path
    except Exception:
        # Mark fallback on unexpected redis path errors too (best effort)
        try:
            _REDIS_FALLBACK_USED = True
            _REDIS_FALLBACK_LAST_PATH = original_path
        except Exception:
            pass


    # Expand {venue} for disk fallback
    try:
        vid = _venue_id()
        if '{venue}' in str(path):
            path = str(path).replace('{venue}', vid)
    except Exception:
        pass

    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False)
    except Exception:
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
        "ask_date": "What date would you like? (Example: June 23, 2026)\n\n(To recall a past reservation, type: recall followed by your reservation ID, e.g. recall WC-XXXX)",
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


def _lead_ts_to_dt(ts: str) -> Optional[datetime]:
    """Parse common timestamp formats from the sheet into an aware UTC datetime (best-effort)."""
    try:
        raw = str(ts or "").strip()
        if not raw:
            return None
        if raw.endswith("Z"):
            try:
                dt = datetime.fromisoformat(raw[:-1])
                return (dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc))
            except Exception:
                pass
        try:
            dt = datetime.fromisoformat(raw)
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except Exception:
            pass
        for fmt in ("%Y-%m-%d %H:%M:%S", "%m/%d/%Y %H:%M:%S", "%Y-%m-%d %H:%M", "%m/%d/%Y %H:%M"):
            try:
                return datetime.strptime(raw, fmt).replace(tzinfo=timezone.utc)
            except Exception:
                continue
        return None
    except Exception:
        return None

def _filter_leads_by_days(rows: List[List[str]], days: int) -> List[List[str]]:
    """Return leads filtered to last N days. Expects header in rows[0]."""
    try:
        if not rows or len(rows) < 2 or days <= 0:
            return rows
        header = rows[0]
        body = rows[1:]
        i_ts = -1
        for i, h in enumerate(header):
            if _normalize_header(h) == "timestamp":
                i_ts = i
                break
        if i_ts < 0:
            return rows
        cutoff = datetime.now(timezone.utc) - timedelta(days=int(days))
        kept = []
        for r in body:
            ts = r[i_ts] if isinstance(r, list) and i_ts < len(r) else ""
            dt = _lead_ts_to_dt(ts)
            if dt and dt >= cutoff:
                kept.append(r)
        return [header] + kept
    except Exception:
        return rows



def ensure_sheet_schema(ws) -> List[str]:
    """
    Make sure row 1 is the header and includes the CRM columns we need.
    Returns the final header list (as stored in the sheet).
    """
    desired = [
        "timestamp",
        "reservation_id",
        "venue_id",
        "name",
        "phone",
        "date",
        "time",
        "party_size",
        "language",
        "status",
        "vip",
        "entry_point",
        "tier",
        "queue",
        "business_context",
        "budget",
        "notes",
        "vibe",
    ]

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


def append_lead_to_sheet(lead: Dict[str, Any], venue_id: Optional[str] = None) -> None:
    """
    Append a lead into the correct venue worksheet, tagging it with venue_id.

    - venue_id (optional): when provided, it is treated as the source of truth
      for which venue owns this lead; otherwise we fall back to the current
      request context via _venue_id().
    """
    # Resolve effective venue (explicit > request context)
    vid = _slugify_venue_id(venue_id or _venue_id())
    ws = get_sheet(venue_id=vid)

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
    setv("reservation_id", (lead.get("reservation_id") or "").strip())
    setv("venue_id", vid)
    setv("name", lead.get("name", ""))
    setv("phone", lead.get("phone", ""))
    setv("date", lead.get("date", ""))
    setv("time", lead.get("time", ""))
    setv("party_size", int(lead.get("party_size") or 0))
    setv("language", lead.get("language", "en"))
    setv("status", status)
    setv("vip", vip)
    setv("entry_point", lead.get("entry_point", ""))
    setv("tier", lead.get("tier", ""))
    setv("queue", lead.get("queue", ""))
    setv("business_context", lead.get("business_context", ""))
    setv("budget", lead.get("budget", ""))
    setv("notes", lead.get("notes", ""))
    setv("vibe", lead.get("vibe", ""))

    # Append at bottom (keeps headers at the top)
    ws.append_row(row, value_input_option="USER_ENTERED")
    try:
        _LEADS_CACHE_BY_VENUE.pop(_slugify_venue_id(vid), None)
    except Exception:
        pass


# Small per-venue read cache to avoid Sheets 429s
_LEADS_CACHE_BY_VENUE: Dict[str, Dict[str, Any]] = {}

def read_leads(limit: int = 200, venue_id: Optional[str] = None) -> List[List[str]]:
    """Read leads from the venue's Google Sheet tab (best-effort, cached).

    Returns rows including header row (row 1) as rows[0].
    Always ensures header includes venue_id, then returns only rows for this venue
    (shared spreadsheet + per-row venue_id is required for multi-tenant isolation).
    """
    vid = _slugify_venue_id(venue_id or _venue_id())
    now = time.time()

    cache = _LEADS_CACHE_BY_VENUE.get(vid) or {}
    rows_cached = cache.get("rows")
    if isinstance(rows_cached, list) and (now - float(cache.get("ts") or 0.0) < 9.0):
        out = rows_cached[:limit] if limit else rows_cached
        return out

    try:
        ws = get_sheet(venue_id=vid)  # uses venue sheet_name when present
        # Must persist venue_id column so writes tag rows; reads filter by it.
        ensure_sheet_schema(ws)
        rows = ws.get_all_values() or []

        # Per-row venue isolation (required when multiple venues share one workbook/tab).
        if not rows or len(rows) < 2:
            _LEADS_CACHE_BY_VENUE[vid] = {"ts": now, "rows": rows or [[]], "body_sheet_rows": []}
            return rows[:limit] if limit else rows

        header = rows[0]
        hmap = header_map(header)
        vcol = hmap.get("venue_id")
        if not vcol:
            # Fail closed: do not show other venues' rows if schema is broken.
            _LEADS_CACHE_BY_VENUE[vid] = {"ts": now, "rows": [header], "body_sheet_rows": []}
            return [header]

        body = rows[1:]
        kept = []
        body_sheet_rows: List[int] = []
        for i, r in enumerate(rows[1:], start=2):
            if not isinstance(r, list):
                continue
            pad = vcol - len(r)
            if pad > 0:
                r = r + [""] * pad
            row_vid = _slugify_venue_id(str((r[vcol - 1] if len(r) >= vcol else "") or DEFAULT_VENUE_ID))
            if row_vid == vid:
                kept.append(r)
                body_sheet_rows.append(i)
        rows = [header] + kept

        # cache regardless; even empty is useful to avoid hammering
        _LEADS_CACHE_BY_VENUE[vid] = {"ts": now, "rows": rows, "body_sheet_rows": body_sheet_rows}
        return rows[:limit] if limit else rows
    except Exception:
        # fallback to cached rows on error (may be missing body_sheet_rows on stale cache)
        rows = rows_cached if isinstance(rows_cached, list) else []
        if rows:
            return rows[:limit] if limit else rows
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
    t = (text or "").lower().strip()
    t_clean = t.rstrip(".!? \t")
    # Standalone "VIP" (or "vip." etc.) = user wants VIP reservation
    if t_clean == "vip":
        return True
    # Heuristics: catch natural phrases like "reservation", "need a table",
    # "book a table", "table for 4", "vip reservation", etc.
    triggers = [
        "reservation",
        "reserve",
        "book a table",
        "book table",
        "table for",
        "need a table",
        "need table",
        "reserva",
        "réservation",
        "vip reservation",
        "vip table",
        "vip reserve",
        "vip book",
        "vip hold",
    ]
    return any(k in t for k in triggers)


def extract_party_size(text: str) -> Optional[int]:
    """Extract party size from free text.

    IMPORTANT: avoid mis-reading dates like 'June 13' as a party size,
    but still support natural phrases like 'party size is 6' or 'party size to 3'
    even when message contains a long digit string (e.g. phone number).
    """
    raw = (text or "").strip()
    if not raw:
        return None
    t = raw.lower()

    # Strong patterns first – "party size to N" wins when message also has phone digits.
    m = re.search(r"party\s*(?:size)?\s+to\s+(\d+)", t)
    if m:
        n = int(m.group(1))
        if 1 <= n <= 200:
            return n
    m = re.search(r"party\s*(?:size)?\s*(?:is|=|:)?\s*(\d+)", t)
    if m:
        return int(m.group(1))
    m = re.search(r"party\s*of\s*(\d+)", t)
    if m:
        return int(m.group(1))
    m = re.search(r"table\s*(?:for|of)?\s*(\d+)", t)
    if m:
        return int(m.group(1))
    m = re.search(r"\b(\d+)\s*(people|persons|guests|pax)\b", t)
    if m:
        return int(m.group(1))
    m = re.search(r"for\s*(\d+)\s*(people|persons|guests|pax)\b", t)
    if m:
        return int(m.group(1))

    # If the text looks like a date or time and we didn't hit any of the
    # strong patterns above, be conservative and avoid treating numbers as
    # party size.
    months = [
        "january","jan","february","feb","march","mar","april","apr","may","june","jun","july","jul",
        "august","aug","september","sep","sept","october","oct","november","nov","december","dec",
        "enero","febrero","marzo","abril","mayo","junio","julio","agosto","septiembre","octubre","noviembre","diciembre",
        "janeiro","fevereiro","março","marco","abril","maio","junho","julho","agosto","setembro","outubro","novembro","dezembro",
        "janvier","février","fevrier","mars","avril","mai","juin","juillet","août","aout","septembre","octobre","novembre","décembre","decembre",
    ]
    if any(mo in t for mo in months):
        return None
    # Also check for partial month names that might indicate a date (e.g., "fe" for feb, "ju" for jun/jul)
    # This prevents "fe 18" from being read as party size 18
    month_prefixes = ["fe", "feb", "mar", "ap", "apr", "ma", "may", "ju", "jun", "jul", "au", "aug", "se", "sep", "sept", "oc", "oct", "no", "nov", "de", "dec"]
    if any(t.startswith(pref) or f" {pref}" in t or f",{pref}" in t for pref in month_prefixes if len(pref) >= 2):
        # If there's a number right after a month prefix, it's likely a date, not party size
        for pref in month_prefixes:
            if len(pref) >= 2 and (t.startswith(pref) or f" {pref}" in t or f",{pref}" in t):
                # Check if there's a digit within 5 chars after the prefix
                idx = t.find(pref)
                if idx >= 0:
                    after = t[idx + len(pref):idx + len(pref) + 5]
                    if re.search(r"\d", after):
                        return None
    if re.search(r"\b\d{4}-\d{1,2}-\d{1,2}\b", t):
        return None
    if re.search(r"\b\d{1,2}[/-]\d{1,2}([/-]\d{2,4})?\b", t):
        return None
    # Time patterns: "4 pm", "4:30 pm", "4am", etc. - don't treat as party size
    if re.search(r"\b\d{1,2}(:\d{2})?\s*(am|pm)\b", t):
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
        pre = re.sub(r"[^A-Za-z\s\-'\.]", "", pre).strip()
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
    name_part = re.sub(r"[^A-Za-z\s\-'\.]", "", name_part).strip()
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
    
    # Fuzzy match: handle partial month names like "fe" -> "feb"
    # Only try if we didn't find a full match above, and only for unambiguous prefixes
    # Check for common typos/abbreviations: "fe" -> february, "feb" -> february
    if lower.startswith("fe") and len(lower) >= 3:
        m = re.search(r"^fe[b]?\D*(\d{1,2})", lower)
        if m:
            dd = int(m.group(1))
            y = 2026
            my = re.search(r"\b(20\d{2})\b", lower)
            if my:
                y = int(my.group(1))
            return f"{y:04d}-02-{dd:02d}"

    return None


def extract_name_candidate(text: str) -> Optional[str]:
    """Best-effort name extraction from a mixed reservation message.

    Example:
      'jeff party of 6 5pm june 18 2157779999' -> 'jeff'
    """
    s = (text or "").strip()
    if not s:
        return None

    # Explicit patterns: "name is X", "my name is X", "under name X", "for name X"
    m = re.search(
        r"\b(?:my\s+name\s+is|name\s+is|under\s+name|for\s+name)\s+([A-Za-z][A-Za-z\s']{0,40})",
        s,
        flags=re.I,
    )
    if m:
        name = m.group(1).strip()
        if name:
            return name

    lower = s.lower().strip()
    # Don't treat trigger words as names
    if lower in ["reservation", "reserva", "réservation", "reserve", "book", "book a table"]:
        return None


    # Don't treat VIP intents/buttons as names
    if (lower == 'vip' or lower.startswith('vip ') or 'vip table' in lower or 'vip hold' in lower) and ('reservation' in lower or 'reserve' in lower or 'table' in lower or 'hold' in lower or lower == 'vip'):
        return None
    # If the message clearly looks like a date/time/party-size description
    # (e.g. 'June 20, 2026 at 8 pm for 6 people'), skip heuristic name guessing
    # and let the bot explicitly ask for a name.
    if re.search(r"\b(?:am|pm)\b", lower) or \
       re.search(r"\bparty\s*(?:size)?\s*(?:is|=|:)?\s*\d+", lower) or \
       re.search(r"\btable\s*(?:for|of)\s*\d+", lower):
        return None

    # If the message clearly looks like a date/time/party-size description
    # (e.g. 'June 20, 2026 at 8 pm for 6 people'), skip heuristic name guessing
    # and let the bot explicitly ask for a name.
    if re.search(r"\b(?:am|pm)\b", lower) or re.search(r"\bparty\s*(?:size)?\s*(?:is|=|:)?\s*\d+", lower) or re.search(r"\btable\s*(?:for|of)\s*\d+", lower):
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

    # Drop obvious non-name filler words that can appear in mixed messages
    stopwords = {"and", "is", "size", "the", "for", "of"}
    parts = [p for p in s.split() if p.lower() not in stopwords]
    if not parts:
        return None

    # Take up to first 3 words as name (e.g., 'Jeff Smith')
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


def recall_text(sess: Dict[str, Any], session_id: Optional[str] = None) -> str:
    # Recall is by reservation ID only (no session). Use the ID you got when you made the reservation.
    return "To recall a reservation, type **recall** followed by your reservation ID (e.g. **recall WC-XXXX**). You received this ID when you made the reservation."


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

@app.get("/v/<venue_id>")
def fan_venue(venue_id):
    vid = _slugify_venue_id(venue_id)

    # REQUIRE a real venue config to exist on disk
    venues = _load_venues_from_disk() or {}
    cfg = venues.get(vid) if isinstance(venues, dict) else None
    if not isinstance(cfg, dict) or not cfg:
        return ("Not found", 404)

    # REQUIRE venue to be active
    if not _venue_is_active(vid):
        return ("Not found", 404)

    # Serve fan SPA shell for valid active venues only
    resp = make_response(send_from_directory(".", "index.html"))
    resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    resp.headers["Pragma"] = "no-cache"
    resp.headers["Expires"] = "0"
    return resp

FANZONE_ADMIN_HTML = r"""
<!doctype html>
<html>
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>Fan Zone Admin</title>
  <style>
    *,*::before,*::after{box-sizing:border-box}
    :root{--bg:#0b1220;--card:rgba(255,255,255,.06);--stroke:rgba(255,255,255,.14);--text:#eef2ff;--muted:rgba(238,242,255,.68);--line:rgba(255,255,255,.14);}
    html,body{height:100%;margin:0;background:var(--bg);color:var(--text);font-family:ui-sans-serif,system-ui,-apple-system,Segoe UI,Roboto;}
    .wrap{max-width:1100px;margin:0 auto;padding:16px}
    .card{background:var(--card);border:1px solid var(--stroke);border-radius:16px;padding:18px}
    .btn{padding:10px 14px;border-radius:12px;border:1px solid var(--stroke);background:rgba(255,255,255,.12);color:var(--text);cursor:pointer}
    .btn2{padding:10px 14px;border-radius:12px;border:1px solid var(--stroke);background:rgba(255,255,255,.06);color:var(--text);cursor:pointer}
    .inp,select,input[type="text"],input[type="number"]{
      width:100%;box-sizing:border-box;
      border-radius:12px;border:1px solid var(--stroke);
      background:rgba(0,0,0,.25);color:var(--text);padding:10px;
      font-size:13px;outline:none;min-width:0;
    }
    .inp::placeholder,input[type="text"]::placeholder,input[type="number"]::placeholder{
      color:rgba(238,242,255,.4);
    }
    select option{
      background:#0b1220;
      color:#eef2ff;
    }
    .sub{color:var(--muted);font-size:12px;margin-top:4px}
    .small{color:var(--muted);font-size:12px}
    .row{display:flex;justify-content:space-between;gap:10px;padding:8px 10px;border:1px solid rgba(255,255,255,.12);border-radius:12px;background:rgba(255,255,255,.05)}
    .mono{font-family:ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace}
    .h2{font-weight:800;font-size:18px}
    .field{display:flex;flex-direction:column;gap:10px;min-width:0;overflow:visible}
    .sub + .inp,.sub + select,.sub + input{margin-top:4px}
    .sub{word-break:break-word;overflow-wrap:break-word}
    .controls{
      display:grid;
      grid-template-columns:1fr;
      gap:18px;
      align-items:start;
      width:100%;
      overflow:visible;
    }
    .controls > *{min-width:0;width:100%}
    @media(min-width:900px){
      .controls{grid-template-columns:1fr 1fr;gap:20px}
    }
    .pair{display:grid;grid-template-columns:1fr 1fr;gap:14px;width:100%;min-width:0}
    .pair > div{display:flex;flex-direction:column;gap:6px;min-width:0;width:100%}
    .pair input{width:100%;min-width:0}
    #toast{position:fixed;left:50%;transform:translateX(-50%);bottom:18px;background:rgba(0,0,0,.65);border:1px solid rgba(255,255,255,.18);color:#eef2ff;padding:10px 12px;border-radius:12px;opacity:0;pointer-events:none;transition:opacity .18s}
    #toast.show{opacity:1}
  </style>
</head>
<body>
  <div class="wrap">
    <div class="card">
      <div style="display:flex;justify-content:space-between;gap:10px;flex-wrap:wrap;align-items:center">
        <div>
          <div style="font-weight:800;font-size:18px">Fan Zone • Poll Controls</div>
          <div class="sub">Edit sponsor text + set Match of the Day (no redeploy). Also shows live poll status.</div>
          <div class="sub">Venue: <span id="vid"></span></div>
        </div>
        <div style="display:flex;gap:10px;align-items:center">
          <button type="button" class="btn" id="btnSaveConfig">Save settings</button>
          <a class="btn2" style="text-decoration:none" id="back">Back to Admin</a>
        </div>
      </div>

      <div class="controls" style="margin-top:14px">
        <!-- Left column -->
        <div class="field">
          <div class="sub">Sponsor label (“Presented by …”)</div>
          <input class="inp" id="pollSponsorText" placeholder="Fan Pick presented by …" />
          <div class="small">Saved into config and shown in Fan Zone.</div>

          <div class="sub" style="margin-top:12px">Poll lock</div>
          <select id="pollLockMode" class="inp">
            <option value="auto">Auto (lock at kickoff)</option>
            <option value="unlocked">Force Unlocked</option>
            <option value="locked">Force Locked</option>
          </select>
          <div class="small">If you need to reopen voting after kickoff, choose Force Unlocked.</div>
        </div>

        <!-- Right column -->
        <div class="field">
          <div class="sub">Match of the Day</div>
          <select id="motdSelect" class="inp"></select>

          <div class="sub" style="margin-top:6px">Manual override (optional):</div>
          <div class="pair">
            <div>
              <div class="sub">Home team</div>
              <input class="inp" id="motdHome" placeholder="Home team"/>
            </div>
            <div>
              <div class="sub">Away team</div>
              <input class="inp" id="motdAway" placeholder="Away team"/>
            </div>
          </div>

          <div class="sub" style="margin-top:6px">Kickoff (UTC ISO, e.g. 2026-06-11T19:00:00Z)</div>
          <input class="inp" id="motdKickoff" placeholder="2026-06-11T19:00:00Z"/>
        </div>
      </div>

      <div id="pollStatus" style="margin-top:12px;border-top:1px solid var(--line);padding-top:12px">
        <div class="sub">Loading poll status…</div>
      </div>
    </div>
  </div>

  <div id="toast"></div>

<script>
(function(){
  const qs = new URLSearchParams(location.search);
  const ADMIN_KEY = qs.get("key") || "";
  const VENUE = qs.get("venue") || "";
  const $ = (id)=>document.getElementById(id);

  $("vid").textContent = VENUE || "default";
  $("back").href = "/admin?key="+encodeURIComponent(ADMIN_KEY)+"&venue="+encodeURIComponent(VENUE);

  function toast(msg){
    const el = $("toast"); if(!el) return;
    el.textContent = String(msg||"");
    el.classList.add("show");
    clearTimeout(toast._t);
    toast._t = setTimeout(()=>el.classList.remove("show"), 1800);
  }
  function escapeHtml(s){
    return String(s??"").replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  }
  function setPollStatus(html){
    const box = $("pollStatus"); if(!box) return;
    box.innerHTML = html;
  }

  let pollStatusTimer = null;
  // Only prefill form fields once from live state; after that, auto-refresh
  // must NOT overwrite any edits the admin is making.
  let hasPrefilledFromPoll = false;

  async function loadPollStatus(){
    try{
      setPollStatus('<div class="sub">Loading poll status…</div>');
      const res = await fetch(`/api/poll/state?venue=${encodeURIComponent(VENUE)}`, {cache:"no-store"});
      const data = await res.json().catch(()=>null);
      if(!data || data.ok === false){
        setPollStatus('<div class="sub">Poll status unavailable</div>');
        return;
      }
      // Prefill form fields only once on initial load so the 5s auto-refresh
      // for poll status never clobbers in-progress edits to Match of the Day.
      if(!hasPrefilledFromPoll){
        hasPrefilledFromPoll = true;
        try{
          const sponsorEl = $("pollSponsorText");
          if(sponsorEl && typeof data.sponsor_text === "string") sponsorEl.value = data.sponsor_text;
          const m = data.match || {};
          const motdHome = $("motdHome"); if(motdHome && m.home) motdHome.value = m.home;
          const motdAway = $("motdAway"); if(motdAway && m.away) motdAway.value = m.away;
          const motdKickoff = $("motdKickoff"); if(motdKickoff && (m.datetime_utc || m.kickoff)) motdKickoff.value = (m.datetime_utc || m.kickoff);
        }catch(e){}
      }

      const locked = !!data.locked;
      const title = (data.title || "Match of the Day Poll");
      const top = (data.top && data.top.length) ? data.top : [];
      let rows = "";
      for(const r of top){
        const name = String(r.name||"");
        const votes = String(r.votes||0);
        rows += `<div class="row"><div>${escapeHtml(name)}</div><div class="mono">${escapeHtml(votes)}</div></div>`;
      }
      if(!rows) rows = '<div class="sub">No votes yet</div>';

      setPollStatus(
        `<div class="h2">${escapeHtml(title)}</div>` +
        `<div class="small">${locked ? "🔒 Locked" : "🟢 Open"}</div>` +
        `<div style="margin-top:10px;display:grid;gap:8px">${rows}</div>`
      );
    }catch(e){
      setPollStatus('<div class="sub">Poll status unavailable</div>');
    }finally{
      if(pollStatusTimer) clearTimeout(pollStatusTimer);
      pollStatusTimer = setTimeout(loadPollStatus, 5000);
    }
  }

  async function loadMatchesForDropdown(){
    const sel = $("motdSelect");
    if(!sel) return;
    try{
      sel.disabled = true;
      const res = await fetch("/schedule.json?scope=all&q=", {cache:"no-store"});
      const data = await res.json().catch(()=>null);
      const matches = (data && Array.isArray(data.matches)) ? data.matches : [];
      const current = sel.value || "";
      sel.innerHTML = '<option value="">Select a match…</option>';
      let added = 0;
      for(const m of matches){
        if(added >= 250) break;
        const dt = String(m.datetime_utc||"");
        const home = String(m.home||"");
        const away = String(m.away||"");
        if(!dt || !home || !away) continue;
        const id = (dt + "|" + home + "|" + away).replace(/[^A-Za-z0-9|:_-]+/g,"_").slice(0,180);
        const label = `${m.date||""} ${m.time||""} • ${home} vs ${away} • ${m.venue||""}`.trim();
        const opt = document.createElement("option");
        opt.value = id;
        opt.textContent = label || (home + " vs " + away);
        opt.setAttribute("data-home", home);
        opt.setAttribute("data-away", away);
        opt.setAttribute("data-dt", dt);
        if(current && current === id) opt.selected = true;
        sel.appendChild(opt);
        added++;
      }
      sel.disabled = false;
      if(sel.value){
        fillMatchFieldsFromOption(sel.selectedOptions[0]);
      }
    }catch(e){
      sel.disabled = false;
      toast("Couldn’t load matches");
    }
  }

  function fillMatchFieldsFromOption(opt){
    if(!opt) return;
    const home = opt.getAttribute("data-home") || "";
    const away = opt.getAttribute("data-away") || "";
    const dt = opt.getAttribute("data-dt") || "";
    if($("motdHome")) $("motdHome").value = home;
    if($("motdAway")) $("motdAway").value = away;
    if($("motdKickoff")) $("motdKickoff").value = dt;
  }

 
// Fan Zone venue bootstrap (safe if repeated)
window.VENUE = (window.VENUE || new URLSearchParams(location.search).get("venue") || "").trim();


  async function saveFanZoneConfig(){
  const btn = $("btnSaveConfig");
  const sel = $("motdSelect");
  const lockEl = $("pollLockMode");
  if(!btn) return;

  // 🔒 venue guard (critical)
  if(!window.VENUE || String(window.VENUE).trim()===""){
    toast("Missing venue", "error");
    return;
  }

  const payload = {
    poll_sponsor_text: ($("pollSponsorText")?.value || "").trim(),
    match_of_day_id: (sel?.value || "").trim(),
    motd_home: ($("motdHome")?.value || "").trim(),
    motd_away: ($("motdAway")?.value || "").trim(),
    motd_datetime_utc: ($("motdKickoff")?.value || "").trim(),
    poll_lock_mode: (lockEl?.value || "auto").trim(),
  };

  const prev = btn.textContent;
  btn.disabled = true;
  btn.textContent = "Saving…";

  try{
    const res = await fetch(
      `/admin/update-config?key=${encodeURIComponent(ADMIN_KEY)}&venue=${encodeURIComponent(VENUE)}`,
      {
        method:"POST",
        headers:{ "Content-Type":"application/json" },
        body: JSON.stringify(payload)
      }
    );

    const data = await res.json().catch(()=>null);
    if(!res.ok || !data || data.ok === false){
      toast((data && data.error) ? data.error : "Save failed", "error");
      btn.textContent = prev; btn.disabled = false;
      return;
    }

    btn.textContent = "Saved ✓";
    toast("Saved", "ok");
    setTimeout(()=>{ btn.textContent = prev; btn.disabled = false; }, 900);

    loadPollStatus(); // re-read from backend
  }catch(e){
    toast("Save failed", "error");
    btn.textContent = prev; btn.disabled = false;
  }
}

  function boot(){
    const sel = $("motdSelect");
    if(sel){
      sel.addEventListener("change", ()=>fillMatchFieldsFromOption(sel.selectedOptions[0]));
      loadMatchesForDropdown();
    }
    const btn = $("btnSaveConfig");
    if(btn) btn.addEventListener("click", (e)=>{ e.preventDefault(); saveFanZoneConfig(); });
    loadPollStatus();
  }

  if(!ADMIN_KEY){
    setPollStatus('<div class="sub">Missing key</div>');
    return;
  }
  boot();
})();
</script>
</body>
</html>
"""

@app.get("/admin/fanzone")
def admin_fanzone_page():
    ok, resp = _require_admin(min_role="manager")
    if not ok:
        return resp

    raw = (request.args.get("venue") or "").strip()
    vid = _slugify_venue_id(raw) if raw else _venue_id()

    # If caller explicitly passed a venue, it must exist
    cfg = _venue_cfg(vid)
    if raw and (cfg.get("status") == "implicit" or cfg.get("venue_id") != vid):
        abort(403)

    out = make_response(render_template_string(FANZONE_ADMIN_HTML))
    try:
        out.set_cookie("venue_id", vid, httponly=False, samesite="Lax", path="/admin")
    except Exception:
        pass
    return out

@app.route("/<path:path>")
def catch_all(path):
    # Serve static files if they exist
    try:
        if os.path.exists(path) and os.path.isfile(path):
            return send_from_directory(".", path)
        if path.startswith("static/") and os.path.exists(path):
            return send_from_directory(".", path)
    except Exception:
        pass

    # HARD BLOCK: never serve fan UI for inactive / nonexistent venues
    # Only the explicit /v/<venue_id> route is allowed to render the fan page
    if path.startswith("v/"):
        return ("Not found", 404)

    # Otherwise serve the SPA shell (home)
    return home()

@app.route("/health")
def health():
    return jsonify({"status": "ok"})





@app.route("/menu.json")
def menu_json():
    # No-store so mobile always sees the latest uploaded menu immediately.
    lang = norm_lang(request.args.get("lang", "en"))
    payload = get_menu_for_lang(lang) or {}
    resp = make_response(jsonify({
        "lang": lang,
        "title": payload.get("title", "Menu"),
        "sections": payload.get("sections", []),
    }))
    resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    resp.headers["Pragma"] = "no-cache"
    resp.headers["Expires"] = "0"
    return resp



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
    raw_venue = (request.args.get("venue") or "").strip()
    # Fallback: derive venue from Referer path (e.g. .../v/qa-sandbox) if not in query
    if not raw_venue and request.referrer:
        m = re.search(r"/v/([^/?#]+)", request.referrer)
        if m:
            raw_venue = m.group(1).strip()
    try:
        vid = _slugify_venue_id(raw_venue or getattr(g, "venue_id", "") or _venue_id()) if (raw_venue or getattr(g, "venue_id", None)) else None
    except Exception:
        vid = None
    sponsor_text = ""
    if vid:
        # Read venue file directly so sponsor_text is always what admin last saved (no cache)
        path = os.path.join(VENUES_DIR, f"{vid}.json")
        if os.path.exists(path):
            try:
                with open(path, "r", encoding="utf-8") as f:
                    vcfg = json.load(f) or {}
                fz = (vcfg.get("fan_zone") or {}) if isinstance(vcfg.get("fan_zone"), dict) else {}
                sponsor_text = str(fz.get("poll_sponsor_text") or "").strip()
            except Exception:
                pass
    return jsonify({
        "lang": lang,
        "events": FANZONE_DEMO.get(lang, FANZONE_DEMO["en"]),
        "sponsor_text": sponsor_text,
    })

@app.route("/schedule.json")
def schedule_json():
    """
    Query params:
      scope= all   (Dallas-only removed)
      q= search text (team, venue, group, date)
    """
    scope = "all"  # Global app: always show all matches
    q = request.args.get("q") or ""

    try:
        matches = filter_matches(scope=scope, q=q)

        today = datetime.now().date()
        if scope == "all":
            # "match day" means: any match today (global)
            is_match = any(m.get("date") == today.isoformat() for m in matches)
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
    """Compute group standings from group fixtures.

    Behavior:
      - Always returns groups + team rows when group fixtures exist (even if no scores yet),
        so the Groups tab never renders empty.
      - If scores are present (home_score/away_score), it updates P/W/D/L/GF/GA/PTS.
    """
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

    # Pass 1: seed groups + team list from *all* group fixtures (scores or not)
    for m in matches:
        stage = (m.get("stage") or "").strip()
        if not stage.lower().startswith("group"):
            continue
        g = stage
        home = (m.get("home") or "").strip() or "TBD"
        away = (m.get("away") or "").strip() or "TBD"
        # Avoid polluting tables with TBD vs TBD when teams aren't known yet
        if home != "TBD":
            ensure_team(g, home)
        if away != "TBD":
            ensure_team(g, away)

    # Pass 2: apply results where scores exist
    for m in matches:
        stage = (m.get("stage") or "").strip()
        if not stage.lower().startswith("group"):
            continue
        hs = m.get("home_score")
        as_ = m.get("away_score")
        if hs is None or as_ is None:
            continue  # no result yet
        try:
            hs = int(hs); as_ = int(as_)
        except Exception:
            continue

        g = stage
        home = (m.get("home") or "").strip() or "TBD"
        away = (m.get("away") or "").strip() or "TBD"
        if home == "TBD" or away == "TBD":
            continue

        ensure_team(g, home)
        ensure_team(g, away)

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
        # If no points yet, keep a stable alphabetical order; otherwise standard sorting.
        any_points = any(int(r.get("pts", 0) or 0) > 0 or int(r.get("p", 0) or 0) > 0 for r in rows)
        if any_points:
            rows.sort(key=lambda r: (r["pts"], r["gd"], r["gf"], r["team"]), reverse=True)
        else:
            rows.sort(key=lambda r: (r["team"],))
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
        "note": "Groups are seeded from fixtures; points update automatically once scores are present in the feed.",
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

def _handle_reservation_turn(sess: Dict[str, Any], msg: str, lang: str, remaining: int) -> Dict[str, Any]:
    """
    Single turn of the deterministic reservation state machine.

    - Always updates sess["lead"] from the current message.
    - Applies business rules (max party size, closed dates).
    - Either:
      * finishes the reservation and returns a confirmation reply, OR
      * asks for the next missing field.
    """
    # Allow VIP to be set at any time during reservation flow
    if re.search(r"\bvip\b", msg.lower()):
        sess["lead"]["vip"] = "Yes"

    # Extract structured fields from free text
    d_iso = extract_date(msg)
    if d_iso:
        if validate_date_iso(d_iso):
            sess["lead"]["date"] = d_iso
        else:
            return {"reply": LANG[lang]["ask_date"], "rate_limit_remaining": remaining}

    t = extract_time(msg)
    if t:
        sess["lead"]["time"] = t

    ps = extract_party_size(msg)
    if ps:
        sess["lead"]["party_size"] = ps

    ph = extract_phone(msg)
    if ph:
        sess["lead"]["phone"] = ph

    lead = sess["lead"]

    # Name extraction – only once we already have date, time, and party_size.
    if not lead.get("name") and lead.get("date") and lead.get("time") and lead.get("party_size"):
        cand = extract_name_candidate(msg)
        if cand:
            lead["name"] = cand

    # Apply business rules if we have enough to check
    rule = apply_business_rules(lead)
    if rule == "party":
        sess["mode"] = "idle"
        return {"reply": LANG[lang]["rule_party"], "rate_limit_remaining": remaining}
    if rule == "closed":
        sess["mode"] = "idle"
        return {"reply": LANG[lang]["rule_closed"], "rate_limit_remaining": remaining}

    # If complete, save + confirm
    if lead.get("date") and lead.get("time") and lead.get("party_size") and lead.get("name") and lead.get("phone"):
        ops2 = get_ops()

        # If waitlist is enabled, tag the reservation as Waitlist (still saved to the same sheet).
        if ops2.get("waitlist_mode"):
            lead["status"] = (lead.get("status") or "Waitlist").strip() or "Waitlist"
            if lead["status"].lower() == "new":
                lead["status"] = "Waitlist"

        if ops2.get("pause_reservations") and not ops2.get("waitlist_mode"):
            sess["mode"] = "idle"
            return {"reply": "⏸️ Reservations were just paused. Please check back soon.", "rate_limit_remaining": remaining}

        if ops2.get("vip_only") and str(lead.get("vip", "No")).strip().lower() != "yes":
            sess["mode"] = "idle"
            return {"reply": "🔒 VIP-only is active right now. Type VIP and start again to continue.", "rate_limit_remaining": remaining}

        rid = _generate_reservation_id()
        lead["reservation_id"] = rid
        try:
            append_lead_to_sheet(lead)
        except Exception:
            pass  # fallback: local file below
        # Always save to local file too so recall by ID works (even after page reload)
        try:
            _append_reservation_local(lead)
        except Exception:
            return {"reply": "⚠️ Could not save reservation.", "rate_limit_remaining": remaining}

        # Proactive AI: suggest a reply draft as soon as the reservation lands.
        # Best-effort only; never block the chat flow if AI generation fails.
        try:
            _auto_suggest_reply_draft_for_reservation(lead)
        except Exception:
            pass

        sess["mode"] = "idle"
        saved_msg = ("✅ Added to waitlist!" if str(lead.get("status", "")).strip().lower() == "waitlist" else LANG[lang]["saved"])
        confirm = (
            f"{saved_msg}\n\n"
            f"Your reservation ID is: **{rid}** — save it!\n"
            f"To recall this reservation later, type: **recall {rid}**\n\n"
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
        # Remember the last reservation ID in-session so we can give better
        # guidance when the user later asks about VIP upgrades.
        try:
            sess["last_reservation_id"] = rid
        except Exception:
            pass
        return {"reply": confirm, "rate_limit_remaining": remaining}

    # Otherwise ask next missing field
    q = next_question(sess)
    return {"reply": q, "rate_limit_remaining": remaining}


def _is_thanks(msg: str) -> bool:
    """True if the message is primarily a thank-you (any language)."""
    if not (msg or isinstance(msg, str)):
        return False
    t = msg.lower().strip().rstrip(".!?")
    thanks = (
        "thank you", "thanks", "thankyou", "ty ", " ty", "thx",
        "gracias", "muchas gracias", "obrigado", "obrigada", "merci", "merci beaucoup",
    )
    if t in ("thanks", "thank you", "thankyou", "ty", "thx", "gracias", "merci", "obrigado", "obrigada"):
        return True
    if any(t.startswith(p) or t == p for p in thanks):
        return True
    if re.match(r"^(thank\s+you|thanks|gracias|merci|obrigad[oa])\s*[.!?]*$", t):
        return True
    return False


def _thanks_reply(lang: str) -> str:
    """Localized, polished reply after user says thank you."""
    try:
        lang = norm_lang(lang)
    except Exception:
        lang = "en"
    replies = {
        "en": "You're welcome! If you have any other questions, feel free to ask.",
        "es": "De nada. Si tienes más preguntas, no dudes en preguntar.",
        "pt": "De nada. Se tiver mais alguma dúvida, é só perguntar.",
        "fr": "Je vous en prie. Si vous avez d'autres questions, n'hésitez pas à demander.",
    }
    return replies.get(lang, replies["en"])


def _is_menu_or_specials_question(msg: str) -> bool:
    """True if the user is asking about menu, specials, food, drink, or pricing (direct to Menu tab)."""
    if not (msg or isinstance(msg, str)):
        return False
    t = msg.lower().strip()
    # Phrases that mean "what's on the menu" / "today's special" / food & drink
    patterns = [
        r"today['\u2019]?s?\s*special",
        r"today\s*special",
        r"what['\u2019]?s?\s*(on\s*)?(the\s*)?menu",
        r"what\s+is\s+today",
        r"specials?\s+(today|for\s+today)",
        r"\bmenu\b",
        r"\bfood\b",
        r"\bdrink",
        r"\bdrinks\b",
        r"\bpric(e|es|ing)\b",
        r"\bdiet\b",
        r"\ballerg",
        r"\bvegan\b",
        r"\bvegetarian\b",
        r"\bgluten\b",
        r"what\s+do\s+you\s+have",
        r"what\s+can\s+i\s+(get|order|eat|drink)",
        r"do\s+you\s+have\s+(any|a)\s+",
        r"any\s+special",
        r"game\s+day\s+(special|bucket|deal)",
    ]
    for p in patterns:
        if re.search(p, t):
            return True
    # Short queries that are clearly menu/specials
    if t in ("menu", "specials", "today's special", "today special", "what's on the menu", "prices", "food", "drinks"):
        return True
    return False


def _menu_redirect_reply(lang: str) -> str:
    """Message that we're navigating the user to the Menu tab (app will switch section)."""
    try:
        lang = norm_lang(lang)
    except Exception:
        lang = "en"
    replies = {
        "en": "Taking you to the **Menu** tab for our full menu and specials. One moment…",
        "es": "Llevándote a la pestaña **Menú** para ver la carta y especiales. Un momento…",
        "pt": "Levando você ao separador **Menu** para o cardápio e promoções. Um momento…",
        "fr": "Ouverture de l'onglet **Menu** pour la carte et les offres. Un instant…",
    }
    return replies.get(lang, replies["en"])


def _find_sheet_row_by_reservation_id(reservation_id: str, venue_id: Optional[str] = None) -> Optional[Tuple[int, Dict[str, int], List[str]]]:
    """Find actual Google Sheet row (1-based) for reservation_id within this venue only."""
    rid = (reservation_id or "").strip().upper()
    if not rid:
        return None
    if not rid.startswith("WC-"):
        rid = "WC-" + rid
    vid = _slugify_venue_id(venue_id or _venue_id())
    try:
        ws = get_sheet(venue_id=vid)
        ensure_sheet_schema(ws)
        rows = ws.get_all_values() or []
        if not rows or len(rows) < 2:
            return None
        header = rows[0]
        hmap = header_map(header)
        if "reservation_id" not in hmap:
            return None
        rcol = hmap["reservation_id"]
        vcol = hmap.get("venue_id")
        for i, r in enumerate(rows[1:], start=2):
            if not isinstance(r, list) or len(r) < rcol:
                continue
            if (r[rcol - 1] or "").strip().upper() != rid:
                continue
            if vcol:
                if len(r) < vcol:
                    continue
                row_vid = _slugify_venue_id(str(r[vcol - 1] or DEFAULT_VENUE_ID))
                if row_vid != vid:
                    continue
            else:
                continue
            return (i, hmap, header)
    except Exception:
        pass
    return None


def _update_reservation_local(reservation_id: str, updates: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Update a reservation stored in local JSONL (date, time, party_size). Returns updated row dict or None."""
    rid = (reservation_id or "").strip().upper()
    if not rid or not rid.startswith("WC-"):
        rid = "WC-" + rid if (reservation_id or "").strip() else ""
    if not rid:
        return None
    try:
        current_vid = _slugify_venue_id(_venue_id())
    except Exception:
        current_vid = DEFAULT_VENUE_ID
    path = os.path.join(_BASE_DIR, RESERVATIONS_LOCAL_PATH)
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            lines = f.readlines()
    except Exception:
        return None
    updated_row = None
    out_lines = []
    for line in lines:
        if not line.strip():
            out_lines.append(line)
            continue
        try:
            row = json.loads(line)
            row_rid = (row.get("reservation_id") or "").strip().upper()
            row_vid = _slugify_venue_id(str(row.get("venue_id") or DEFAULT_VENUE_ID))
            if row_rid != rid or row_vid != current_vid:
                out_lines.append(line)
                continue
            # Apply updates (same keys as sheet path)
            if "date" in updates and updates["date"] and str(updates["date"]).strip():
                row["date"] = str(updates["date"]).strip()
            if "time" in updates and updates["time"] is not None and str(updates["time"]).strip():
                row["time"] = str(updates["time"]).strip()
            if "party_size" in updates and updates["party_size"] is not None:
                try:
                    row["party_size"] = int(updates["party_size"])
                except (TypeError, ValueError):
                    pass
            if "name" in updates and updates["name"] is not None and str(updates["name"]).strip():
                row["name"] = str(updates["name"]).strip()
            if "phone" in updates and updates["phone"] is not None and str(updates["phone"]).strip():
                row["phone"] = str(updates["phone"]).strip()
            updated_row = row
            out_lines.append(json.dumps(row, ensure_ascii=False) + "\n")
        except Exception:
            out_lines.append(line)
    if updated_row is None:
        return None
    try:
        with open(path, "w", encoding="utf-8") as f:
            f.writelines(out_lines)
    except Exception:
        return None
    return updated_row


def update_reservation_by_id(reservation_id: str, updates: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Update an existing reservation by ID (date, time, party_size, name, phone). Writes to sheet or local JSONL; returns updated row dict or None."""
    found = _find_sheet_row_by_reservation_id(reservation_id, venue_id=_venue_id())
    if found:
        row_num, hmap, header = found
        try:
            vid = _venue_id()
        except Exception:
            return None
        ws = get_sheet(venue_id=vid)
        if not ws:
            return None
        updated = 0
        if "date" in updates and updates["date"] and hmap.get("date"):
            ws.update_cell(row_num, hmap["date"], str(updates["date"]).strip())
            updated += 1
        if "time" in updates and updates["time"] is not None and str(updates["time"]).strip() and hmap.get("time"):
            ws.update_cell(row_num, hmap["time"], str(updates["time"]).strip())
            updated += 1
        if "party_size" in updates and updates["party_size"] is not None and hmap.get("party_size"):
            try:
                ps = int(updates["party_size"])
                ws.update_cell(row_num, hmap["party_size"], ps)
                updated += 1
            except (TypeError, ValueError):
                pass
        if "name" in updates and updates["name"] is not None and str(updates["name"]).strip() and hmap.get("name"):
            ws.update_cell(row_num, hmap["name"], str(updates["name"]).strip())
            updated += 1
        if "phone" in updates and updates["phone"] is not None and str(updates["phone"]).strip() and hmap.get("phone"):
            ws.update_cell(row_num, hmap["phone"], str(updates["phone"]).strip())
            updated += 1
        # NEW: Handle VIP status updates + sync with tier/segment column
        if "vip" in updates and updates["vip"] is not None:
            vip_val = str(updates["vip"]).strip()
            if vip_val in ["Yes", "No"]:
                col = hmap.get("vip")
                if col:
                    ws.update_cell(row_num, col, vip_val)
                    updated += 1
                # Also update tier column to keep Segment display in sync
                tier_val = "VIP" if vip_val == "Yes" else "Regular"
                tier_col = hmap.get("tier")
                if tier_col:
                    ws.update_cell(row_num, tier_col, tier_val)
        if updated == 0:
            return _get_reservation_by_id(reservation_id)
        try:
            _LEADS_CACHE_BY_VENUE.pop(_slugify_venue_id(vid), None)
        except Exception:
            pass
        return _get_reservation_by_id(reservation_id)
    # Reservation not in sheet: try local JSONL (e.g. when using data/reservations.jsonl only)
    return _update_reservation_local(reservation_id, updates)


def _want_modify_reservation(msg: str) -> bool:
    """True if user is asking to change/update their reservation (time, date, party size, name, phone). Dynamic: many phrasings."""
    if not (msg or isinstance(msg, str)):
        return False
    t = msg.lower().strip()
    # Action verbs and field words – any combination indicates modify intent
    actions = r"\b(change|update|modify|edit|set|fix|correct|switch|replace|make)\b"
    # "change the name", "update my name", "set name to", "fix the time", "correct the date", etc.
    fields = r"\b(reservation|time|date|party|booking|name|phone|number|guests|people|size)\b"
    if re.search(actions, t) and re.search(fields, t):
        return True
    # "name to X", "name is X", "time to 9", "phone to ..." (user states new value directly)
    if re.search(r"\bname\s+(?:to|is|as|=|:)\s+", t):
        return True
    if re.search(r"\bphone\s+(?:to|is|as|=|:)\s+", t) or re.search(r"\bnumber\s+(?:to|is|as|=|:)\s+", t):
        return True
    if re.search(r"\btime\s+to\s+\d", t) or re.search(r"\b(?:at|for)\s+\d{1,2}\s*(?:am|pm)\b", t):
        return True
    # "change it to 9 pm", "make it 6 people", "set it to Ahmad"
    if re.search(r"\b(change|update|set|make)\s+it\s+(?:to\s+)?", t):
        return True
    # "instead of X", "rather Y"
    if re.search(r"\d+\s*pm\s+instead\s+of\s+\d|\d+\s*am\s+instead\s+of\s+\d", t):
        return True
    # "just the name", "only the time", "the name too", "name too"
    if re.search(r"\b(just|only|also|too)\s+(?:the\s+)?(name|time|date|phone|party)\b", t):
        return True
    if re.search(r"\b(name|time|date|phone|party)\s+too\b", t):
        return True
    return False


def _extract_modification_name(msg: str) -> Optional[str]:
    """Extract new name from an update message. Handles many phrasings: name to X, change name to X, set name as X, call me X, etc.
    FILTERS OUT: 'vip', 'reservation', 'reserve', etc. to prevent trigger words being treated as names.
    """
    raw = (msg or "").strip()
    if not raw:
        return None
    t = raw.lower()
    # IMPORTANT: don't treat VIP or other trigger words as names
    if t.strip() in ["vip", "reservation", "reserva", "réservation", "reserve"]:
        return None
    # If message is ALL trigger words + VIP, don't extract a name
    if "vip" in t and not re.search(r"[a-z][a-z]{3,}", t.replace("vip", "")):
        return None
    # If message is clearly "make it vip" or "set it vip", don't extract a name
    if re.search(r"\b(?:make|set|mark)\s+(?:it|this)\s+(?:to\s+)?vip\b", t):
        return None
    
    # Stop name at " and ", ",", digits, or end so "name to Ahmad and time to 9 pm" -> "Ahmad"
    end_look = r"(?=\s+and\s+|\s*,\s*|\d|$)"
    # (the|my)? name (to|is|as|=|:) X
    m = re.search(r"(?:the\s+|my\s+)?name\s+(?:to|is|as|=|:)\s*([A-Za-z][A-Za-z\s\-']*?)" + end_look, raw, re.IGNORECASE)
    if m:
        name = m.group(1).strip()
        if 1 <= len(name) <= 40 and name.lower() not in ["vip"]:
            return name
    # (update|change|modify|edit|set|fix) (the)? name to X
    m = re.search(r"(?:update|change|modify|edit|set|fix)\s+(?:the\s+|my\s+)?name\s+to\s+([A-Za-z][A-Za-z\s\-']*?)" + end_look, raw, re.IGNORECASE)
    if m:
        name = m.group(1).strip()
        if 1 <= len(name) <= 40 and name.lower() not in ["vip"]:
            return name
    # set name as X, change name as X
    m = re.search(r"(?:set|change|update)\s+(?:the\s+)?name\s+as\s+([A-Za-z][A-Za-z\s\-']*?)" + end_look, raw, re.IGNORECASE)
    if m:
        name = m.group(1).strip()
        if 1 <= len(name) <= 40 and name.lower() not in ["vip"]:
            return name
    # "call me X", "under the name X", "under X" (when X looks like a name)
    m = re.search(r"call\s+me\s+([A-Za-z][A-Za-z\s\-']*?)" + end_look, raw, re.IGNORECASE)
    if m:
        name = m.group(1).strip()
        if 1 <= len(name) <= 40 and name.lower() not in ["vip"]:
            return name
    m = re.search(r"under\s+(?:the\s+name\s+)?([A-Za-z][A-Za-z\s\-']*?)" + end_look, raw, re.IGNORECASE)
    if m:
        name = m.group(1).strip()
        if 1 <= len(name) <= 40 and name.lower() not in ["vip"]:
            return name
    # "make it X" or "set it to X" when message is mostly a single name (no time/date pattern)
    # BUT: skip if it's "make it vip" (that's a VIP modifier, not a name)
    if not re.search(r"\b(?:make|set|mark)\s+(?:it|this)\s+(?:to\s+)?vip\b", t):
        if re.search(r"\b(?:make|set)\s+it\s+(?:to\s+)?([A-Za-z][A-Za-z\s\-']+)\s*$", raw, re.IGNORECASE) and not re.search(r"\d{1,2}\s*(?:am|pm|\d)", t):
            m = re.search(r"\b(?:make|set)\s+it\s+(?:to\s+)?([A-Za-z][A-Za-z\s\-']+)\s*$", raw, re.IGNORECASE)
            if m:
                name = m.group(1).strip()
                if 1 <= len(name) <= 40 and name.lower() not in ["vip"]:
                    return name
    # "new name X", "name should be X"
    m = re.search(r"(?:new\s+)?name\s+(?:should\s+be\s+)?([A-Za-z][A-Za-z\s\-']*?)" + end_look, raw, re.IGNORECASE)
    if m:
        name = m.group(1).strip()
        if 1 <= len(name) <= 40 and name.lower() not in ["vip"]:
            return name
    return None


def _get_modification_fields_mentioned(msg: str) -> List[str]:
    """Return list of field keys (time, date, party_size, name, phone) mentioned in msg, in fixed order. Used when user says 'update X and Y too' with no values."""
    low = (msg or "").lower()
    fields = []
    if re.search(r"\btime\b", low) or re.search(r"\bhour\b", low):
        fields.append("time")
    if re.search(r"\bdate\b", low):
        fields.append("date")
    if re.search(r"\bparty\b", low) or re.search(r"\bpeople\b", low) or re.search(r"\bguests\b", low):
        fields.append("party_size")
    if re.search(r"\bname\b", low):
        fields.append("name")
    if re.search(r"\bphone\b", low) or re.search(r"\bnumber\b", low):
        fields.append("phone")
    return fields


def _modify_awaiting_prompt(field: str) -> str:
    """Return the prompt to ask for the given modification field (time, date, party_size, name, phone)."""
    prompts = {
        "time": "What time would you like for this reservation? (e.g. 7 pm or 11 pm)",
        "date": "What date would you like? (e.g. June 23, 2026)",
        "party_size": "How many people will be in your party?",
        "name": "What name should I put on the reservation?",
        "phone": "What phone number should I use for this reservation?",
    }
    return prompts.get(field, "What would you like to change?")


def _extract_modification(msg: str) -> Dict[str, Any]:
    """Extract requested reservation changes from message. Returns dict with date, time, party_size, name, phone, vip (only set if parsed)."""
    out = {}
    t = extract_time(msg)
    if t:
        out["time"] = t
    d = extract_date(msg)
    if d and validate_date_iso(d):
        out["date"] = d
    ps = extract_party_size(msg)
    if ps is not None:
        out["party_size"] = ps
    name = _extract_modification_name(msg)
    if name:
        out["name"] = name
    ph = extract_phone(msg)
    if ph:
        out["phone"] = ph
    # NEW: Extract VIP status from modification request
    if re.search(r"\bvip\b", msg.lower()):
        out["vip"] = "Yes"
    return out


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

        # ✅ NEW: block chat for inactive venues (prevents lingering fan access)
        vid = _venue_id()
        if not _venue_is_active(vid):
            return jsonify({
                "reply": "This venue is currently inactive.",
                "rate_limit_remaining": remaining,
            }), 403

        data = request.get_json(force=True) or {}

        # Venue context for fan chat:
        try:
            raw_vid = str(data.get("venue_id") or "").strip()
            if raw_vid:
                g.venue_id = _slugify_venue_id(raw_vid)
        except Exception:
            # If anything goes wrong, fall back to whatever _set_venue_ctx resolved.
            pass
        msg = (data.get("message") or "").strip()
        lang = norm_lang(data.get("language") or data.get("lang"))
        sid = get_session_id()
        sess = get_session(sid)

        # Update session language if user toggled
        sess["lang"] = lang
        sess["lead"]["language"] = lang

        if not msg:
            return jsonify({"reply": "Please type a message.", "rate_limit_remaining": remaining})

        # Apply name/phone/time/date/party when we previously asked for that field for the recalled reservation (supports multiple in sequence)
        if sess.get("recalled_reservation_id") and sess.get("modify_awaiting"):
            rid = sess["recalled_reservation_id"]
            awaiting = sess.get("modify_awaiting")
            # Normalize: (current field, rest list) — modify_awaiting can be a string or list
            if isinstance(awaiting, list):
                if not awaiting:
                    sess.pop("modify_awaiting", None)
                    current, rest = None, []
                else:
                    current, rest = awaiting[0], awaiting[1:]
            else:
                current, rest = awaiting, []
            reply = None
            if current == "name" and msg and len(msg) <= 80 and not re.search(r"\d{3}[-.\s]?\d{3}", msg):
                name = msg.strip()
                if 1 <= len(name) <= 40:
                    updated_row = update_reservation_by_id(rid, {"name": name})
                    if updated_row:
                        sess["recalled_reservation"] = updated_row
                        if rest:
                            sess["modify_awaiting"] = rest
                            reply = _modify_awaiting_prompt(rest[0])
                        else:
                            sess.pop("modify_awaiting", None)
                            reply = "✅ Reservation updated.\n\n" + format_reservation_row(updated_row)
                    else:
                        reply = "I couldn't update the name. Please try again or contact the venue."
                else:
                    reply = "What name should I put on the reservation? (e.g. first and last name)"
            elif current == "phone":
                ph = extract_phone(msg)
                if ph:
                    updated_row = update_reservation_by_id(rid, {"phone": ph})
                    if updated_row:
                        sess["recalled_reservation"] = updated_row
                        if rest:
                            sess["modify_awaiting"] = rest
                            reply = _modify_awaiting_prompt(rest[0])
                        else:
                            sess.pop("modify_awaiting", None)
                            reply = "✅ Reservation updated.\n\n" + format_reservation_row(updated_row)
                    else:
                        reply = "I couldn't update the phone number. Please try again or contact the venue."
                else:
                    sess["modify_awaiting"] = awaiting
                    reply = "What phone number should I use for this reservation? (e.g. 10 digits)"
            elif current == "time":
                t = extract_time(msg)
                if t:
                    updated_row = update_reservation_by_id(rid, {"time": t})
                    if updated_row:
                        sess["recalled_reservation"] = updated_row
                        if rest:
                            sess["modify_awaiting"] = rest
                            reply = _modify_awaiting_prompt(rest[0])
                        else:
                            sess.pop("modify_awaiting", None)
                            reply = "✅ Reservation updated.\n\n" + format_reservation_row(updated_row)
                    else:
                        reply = "I couldn't update the time. Please try again or contact the venue."
                else:
                    sess["modify_awaiting"] = awaiting
                    reply = "What time would you like for this reservation? (e.g. 7 pm or 11 pm)"
            elif current == "date":
                d = extract_date(msg)
                if d and validate_date_iso(d):
                    updated_row = update_reservation_by_id(rid, {"date": d})
                    if updated_row:
                        sess["recalled_reservation"] = updated_row
                        if rest:
                            sess["modify_awaiting"] = rest
                            reply = _modify_awaiting_prompt(rest[0])
                        else:
                            sess.pop("modify_awaiting", None)
                            reply = "✅ Reservation updated.\n\n" + format_reservation_row(updated_row)
                    else:
                        reply = "I couldn't update the date. Please try again or contact the venue."
                else:
                    sess["modify_awaiting"] = awaiting
                    reply = "What date would you like? (e.g. June 23, 2026)"
            elif current == "party_size":
                ps = extract_party_size(msg)
                if ps is not None and 1 <= ps <= 200:
                    updated_row = update_reservation_by_id(rid, {"party_size": ps})
                    if updated_row:
                        sess["recalled_reservation"] = updated_row
                        if rest:
                            sess["modify_awaiting"] = rest
                            reply = _modify_awaiting_prompt(rest[0])
                        else:
                            sess.pop("modify_awaiting", None)
                            reply = "✅ Reservation updated.\n\n" + format_reservation_row(updated_row)
                    else:
                        reply = "I couldn't update the party size. Please try again or contact the venue."
                else:
                    sess["modify_awaiting"] = awaiting
                    reply = "How many people will be in your party?"
            else:
                sess["modify_awaiting"] = awaiting
            if reply is not None:
                return jsonify({"reply": reply, "rate_limit_remaining": remaining})

        # Recall: (1) bare "WC-XXXX" or (2) "recall WC-XXXX" or (3) "recall with this id" using last-entered ID
        rid = None
        if _is_bare_reservation_id(msg):
            rid = _normalize_reservation_id(msg)
            if rid:
                sess["last_entered_reservation_id"] = rid
        if not rid and want_recall(msg, lang):
            rid = extract_recall_id(msg)
            if not rid:
                low = msg.lower()
                if re.search(r"(this|that|the)\s+id", low) or "with this id" in low or "with that id" in low:
                    rid = sess.get("last_entered_reservation_id")
        if rid:
            row = _get_reservation_by_id(rid)
            if row:
                sess["recalled_reservation_id"] = rid
                sess["recalled_reservation"] = row
                sess["last_entered_reservation_id"] = rid
                
                # SPECIAL: Handle "make it vip" explicitly for recalled reservations
                if re.search(r"\b(?:make|mark|set|upgrade)\s+(?:it|this)\s+(?:to\s+)?vip\b", msg.lower()):
                    updated_row = update_reservation_by_id(rid, {"vip": "Yes"})
                    if updated_row:
                        sess["recalled_reservation"] = updated_row
                        reply = "✅ Reservation upgraded to VIP!\n\n" + format_reservation_row(updated_row)
                    else:
                        reply = "I couldn't upgrade that reservation to VIP. Please try again or contact the venue."
                    return jsonify({"reply": reply, "rate_limit_remaining": remaining})
                
                mod = _extract_modification(msg)
                if mod:
                    updated_row = update_reservation_by_id(rid, mod)
                    if updated_row:
                        sess["recalled_reservation"] = updated_row
                        reply = "✅ Reservation updated.\n\n" + format_reservation_row(updated_row)
                    else:
                        reply = format_reservation_row(row) + "\n\n(I couldn't update the reservation; please try again or contact the venue.)"
                else:
                    reply = format_reservation_row(row)
            else:
                sess.pop("recalled_reservation_id", None)
                sess.pop("recalled_reservation", None)
                reply = format_reservation_row(None)
            return jsonify({"reply": reply, "rate_limit_remaining": remaining})
        if want_recall(msg, lang) and not rid:
            reply = "To recall a reservation, type **recall** followed by your reservation ID (e.g. **recall WC-XXXX**), or paste your ID (e.g. WC-BAC819C0) and I'll look it up."
            return jsonify({"reply": reply, "rate_limit_remaining": remaining})

        # NEW: Handle "make it vip" specifically when a reservation is recalled
        if sess.get("recalled_reservation_id") and re.search(r"\b(?:make|mark|set|upgrade)\s+(?:it|this)\s+(?:to\s+)?vip\b", msg.lower()):
            rid = sess["recalled_reservation_id"]
            updated_row = update_reservation_by_id(rid, {"vip": "Yes"})
            if updated_row:
                sess["recalled_reservation"] = updated_row
                reply = "✅ Reservation upgraded to VIP!\n\n" + format_reservation_row(updated_row)
            else:
                reply = "I couldn't upgrade that reservation to VIP. Please try **recall " + rid + "** again, or contact the venue."
            return jsonify({"reply": reply, "rate_limit_remaining": remaining})

        # Modify existing (recalled) reservation: keep context, don't reset to new flow (BUG-CHAT-001)
        if _want_modify_reservation(msg) and sess.get("recalled_reservation_id"):
            rid = sess["recalled_reservation_id"]
            mod = _extract_modification(msg)
            if mod:
                sess.pop("modify_awaiting", None)
                updated_row = update_reservation_by_id(rid, mod)
                if updated_row:
                    sess["recalled_reservation"] = updated_row
                    reply = "✅ Reservation updated.\n\n" + format_reservation_row(updated_row)
                else:
                    reply = "I couldn't update that reservation. Please try **recall " + rid + "** again, or contact the venue."
            else:
                # Ask for the specific change(s); support multiple fields e.g. "update party size and name too"
                mentioned = _get_modification_fields_mentioned(msg)
                if len(mentioned) >= 2:
                    sess["modify_awaiting"] = mentioned
                    reply = _modify_awaiting_prompt(mentioned[0])
                elif len(mentioned) == 1:
                    sess["modify_awaiting"] = mentioned[0]
                    reply = _modify_awaiting_prompt(mentioned[0])
                else:
                    sess.pop("modify_awaiting", None)
                    reply = "What would you like to change — **time**, **date**, **party size**, **name**, or **phone**? Tell me the new value (e.g. 11 pm or Ahmad)."
            return jsonify({"reply": reply, "rate_limit_remaining": remaining})

        # If user wants to modify but no recalled context, direct them to recall first (skip when already making a new reservation)
        if _want_modify_reservation(msg) and sess.get("mode") != "reserving":
            reply = (
                "Please recall your reservation first: type **recall WC-XXXX** with your reservation ID, "
                "then tell me what you'd like to change (e.g. time, date, or party size)."
            )
            return jsonify({"reply": reply, "rate_limit_remaining": remaining})

        # Start reservation flow if user indicates intent (first turn for this reservation)
        if sess["mode"] == "idle" and want_reservation(msg):
            ops = get_ops()

            # Match-day ops toggles
            if ops.get("vip_only") and not re.search(r"\bvip\b", msg.lower()):
                return jsonify({"reply": "🔒 Reservations are VIP-only right now. If you have VIP access, type **VIP** to continue. Otherwise, I can add you to the waitlist.", "rate_limit_remaining": remaining})

            if ops.get("pause_reservations") and not ops.get("waitlist_mode"):
                return jsonify({"reply": "⏸️ Reservations are temporarily paused. Please check back soon, or ask a staff member for help.", "rate_limit_remaining": remaining})

            sess["mode"] = "reserving"
            sess.pop("recalled_reservation_id", None)
            sess.pop("recalled_reservation", None)

            # If we are in waitlist mode, capture the same details but save as Waitlist (keeps fan UI unchanged).
            if ops.get("waitlist_mode"):
                sess["lead"]["status"] = "Waitlist"

            # Mark VIP if user clicked a VIP button or mentions VIP
            if re.search(r"\bvip\b", msg.lower()):
                sess["lead"]["vip"] = "Yes"

            # IMPORTANT: do NOT treat trigger words as the name.
            if msg.lower().strip() in ["reservation", "reserva", "réservation", "vip"]:
                sess["lead"]["name"] = ""

            payload = _handle_reservation_turn(sess, msg, lang, remaining)
            return jsonify(payload)

        # If reserving, keep collecting fields deterministically
        if sess["mode"] == "reserving":
            payload = _handle_reservation_turn(sess, msg, lang, remaining)
            # _handle_reservation_turn may have set mode back to idle on completion/rule hit.
            return jsonify(payload)

        # If user asks to "make it VIP" after a reservation, provide clear guidance
        low = msg.lower()
        if "vip" in low and any(kw in low for kw in ["make it", "make me", "mark it", "upgrade", "make this"]):
            last_id = (sess.get("last_reservation_id") or "").strip()
            if last_id:
                reply = (
                    f"I've already saved your reservation with ID **{last_id}**.\n\n"
                    "VIP upgrades are handled by the venue staff. Please share this reservation ID with your host or manager, "
                    "and they can mark it as VIP in the Admin Leads view."
                )
            else:
                reply = (
                    "VIP upgrades are handled by the venue staff. Once you have a reservation ID, "
                    "share it with your host or manager and they can mark it as VIP in the Admin Leads view.\n\n"
                    "If you’d like, I can also help you make a new reservation now — just tell me the date, time, and party size."
                )
            return jsonify({"reply": reply, "rate_limit_remaining": remaining})

        # Deterministic replies: thank you (polished welcome) + menu/specials (direct to Menu tab only)
        if _is_thanks(msg):
            return jsonify({"reply": _thanks_reply(lang), "rate_limit_remaining": remaining})
        if _is_menu_or_specials_question(msg):
            return jsonify({
                "reply": _menu_redirect_reply(lang),
                "rate_limit_remaining": remaining,
                "navigate_to": "menu",
            })

        # Otherwise: normal Q&A using OpenAI (venue profile only; no menu — chat is for reservations, menu in Menu tab)
        vid_for_profile = _venue_id()
        bp = _venue_business_profile(vid_for_profile)
        system_msg = f"""
    You are a World Cup 2026 business concierge for this venue. This chat is for reservations only.

    Business profile (source of truth for venue info only; do NOT quote menu items or specials):
    {bp}

Rules:
- Be friendly, fast, and concise.
- Always respond in the user's chosen language: {lang}. This includes short greetings: if the user says "hi", "hello", "hola", "bonjour", etc., respond with a brief friendly greeting in language {lang} only (e.g. in Spanish if lang is es, in French if lang is fr).
- If the user wants a reservation (or says "VIP" to start a VIP reservation), do NOT tell them to type "reservation". Start collecting details immediately.
- For the World Cup match schedule, tell them to use the **Schedule** tab on the page, then offer to help with a reservation.
- For menu, food, drink, specials, prices, or diet questions: do NOT list or quote any menu items. Tell them to use the **Menu** tab on this page for the full menu and pricing, then offer to help with a reservation.
- If you are unsure or missing info, do NOT dead-end. Redirect to the Menu or Schedule tabs as needed and keep the conversation focused on reservations.
- Always keep the reservation flow alive by asking for missing details: party size and preferred time.
"""

        try:
            if not _OPENAI_AVAILABLE or client is None:
                raise RuntimeError("OpenAI SDK not installed / not configured")
            resp = client.responses.create(
                model=os.environ.get("CHAT_MODEL", "gpt-4o-mini"),
                input=[
                    {"role": "system", "content": system_msg},
                    {"role": "user", "content": msg},
                ],
            )
            reply = (resp.output_text or "").strip() or "(No response)"

            # If the model gives a dead-end answer, force redirect + continue booking
            if re.search(r"\b(i (can't|cannot)|not sure|i don't know|unable to|no information)\b", reply.lower()):
                reply = (
                    "For accurate details, please check the **Menu** or **Schedule** tabs on this page.\n\n"
                    "I can still help with a reservation — **how many guests** and **what time**?"
                )

            # Safety: if the model quoted menu/specials/prices (e.g. from business profile), redirect to Menu tab
            if re.search(r"\$\s*\d+|\d+\s*dollars?|(today'?s?|game\s+day)\s+special|(\d+\s*beers?|bucket\s+for)", reply.lower()):
                reply = _menu_redirect_reply(lang)
                return jsonify({"reply": reply, "rate_limit_remaining": remaining, "navigate_to": "menu"})

            return jsonify({"reply": reply, "rate_limit_remaining": remaining})
        except Exception as e:
            # Customer-safe fallback (no “chat unavailable”), still routes + continues booking
            fallback = (
                "For accurate details, please check the **Menu** or **Schedule** tabs on this page.\n\n"
                "I can still help with a reservation — **how many guests** and **what time**?"
            )
            return jsonify({"reply": fallback, "rate_limit_remaining": remaining}), 200

    except Exception as e:
        # Never break the UI: always return JSON.
        fallback = (
            "For accurate details, please check the **Menu** or **Schedule** tabs on this page.\n\n"
            "I can still help with a reservation — **how many guests** and **what time**?"
        )
        return jsonify({"reply": fallback, "rate_limit_remaining": 0}), 200


@app.route("/chat/clear", methods=["POST"])
def chat_clear():
    """Clear server-side chat/reservation session for the given session_id.
    Used when the user switches language so the conversation and reservation state reset.
    """
    data = request.get_json(silent=True) or {}
    sid = (data.get("session_id") or "").strip()
    lang = norm_lang(data.get("lang") or data.get("language") or "en")
    if sid:
        # Reset session to initial state (same sid, fresh state)
        _sessions[sid] = {
            "mode": "idle",
            "lang": lang,
            "lead": {
                "name": "",
                "phone": "",
                "date": "",
                "time": "",
                "party_size": 0,
                "language": lang,
                "status": "New",
                "vip": "No",
            },
            "updated_at": time.time(),
        }
    return jsonify({"ok": True})


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
CONFIG_FILE = os.path.join(DATA_DIR, "app_config_{venue}.json")
AUDIT_LOG_FILE = os.path.join(DATA_DIR, "audit_log.jsonl")

NOTIFICATIONS_FILE = os.path.join(DATA_DIR, "notifications.jsonl")
NOTIFY_WEBHOOK_URL = os.environ.get("NOTIFY_WEBHOOK_URL", "").strip()

def _notify(event: str, details: Optional[Dict[str, Any]] = None, targets: Optional[List[str]] = None, level: str = "info") -> None:
    """
    Lightweight notifications (Step 8)
    - Stored locally in an append-only JSONL file
    - Optionally POSTs to a webhook (best-effort) if NOTIFY_WEBHOOK_URL is set
    - Now tagged with venue_id so multi-venue read/clear never bleed across venues
    Targets: ["owner"], ["manager"], or ["owner","manager"], or ["all"]
    """
    try:
        if not targets:
            targets = ["owner", "manager"]

        # Best-effort venue scoping for multi-venue installs
        venue_id = None
        try:
            if "_venue_id" in globals():
                venue_id = _venue_id()
        except Exception:
            venue_id = None

        entry = {
            "ts": datetime.utcnow().isoformat(timespec="seconds") + "Z",
            "event": str(event),
            "level": str(level),
            "targets": targets,
            "details": details or {},
        }
        if venue_id:
            entry["venue_id"] = venue_id

        os.makedirs(os.path.dirname(NOTIFICATIONS_FILE), exist_ok=True)
        with open(NOTIFICATIONS_FILE, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")
        if NOTIFY_WEBHOOK_URL:
            try:
                payload = json.dumps(entry).encode("utf-8")
                req = urllib.request.Request(
                    NOTIFY_WEBHOOK_URL,
                    data=payload,
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                urllib.request.urlopen(req, timeout=3)  # nosec - best effort
            except Exception:
                pass
    except Exception:
        pass

def _read_notifications(
    limit: int = 50,
    role: str = "manager",
    venue_id: Optional[str] = None,
    cutoff: Optional[datetime] = None,
) -> List[Dict[str, Any]]:
    """
    Read newest notifications first; filter by role.
    Optionally filter by `cutoff` (UTC datetime): only notifications with
    `ts >= cutoff` are included.
    Managers see entries targeted to manager/all; Owners see everything.
    """
    try:
        if not os.path.exists(NOTIFICATIONS_FILE):
            return []
        items: List[Dict[str, Any]] = []
        # When time filtering is active, we may need to read more candidates
        # because we might skip a bunch outside the cutoff.
        read_multiplier = 10 if cutoff else 3
        with open(NOTIFICATIONS_FILE, "rb") as f:
            f.seek(0, os.SEEK_END)
            size = f.tell()
            buf = b""
            step = 4096
            cutoff_reached = False
            while size > 0 and (not cutoff_reached) and len(items) < limit * read_multiplier:
                read_size = step if size >= step else size
                size -= read_size
                f.seek(size)
                buf = f.read(read_size) + buf
                lines = buf.splitlines()
                if size > 0 and buf and not buf.startswith(b"\n"):
                    buf = lines[0]
                    lines = lines[1:]
                else:
                    buf = b""
                for ln in reversed(lines):
                    if not ln.strip():
                        continue
                    try:
                        it = json.loads(ln.decode("utf-8"))
                        if cutoff:
                            # Early-stop optimization: we scan newest -> oldest.
                            # Once we hit timestamps older than cutoff, no future
                            # (older) entries can match time-filter.
                            ts_str = str(it.get("ts") or "").strip()
                            dt = _timestamp_to_datetime(ts_str)
                            if dt is not None:
                                if dt.tzinfo is None:
                                    dt = dt.replace(tzinfo=timezone.utc)
                                if dt < cutoff:
                                    cutoff_reached = True
                                    break
                        items.append(it)
                    except Exception:
                        continue
                    if len(items) >= limit * read_multiplier:
                        break
        out: List[Dict[str, Any]] = []
        for it in items:
            t = it.get("targets") or []
            v = (it.get("venue_id") or "").strip()

            # Venue scoping (strict):
            # - If venue_id is provided, only show items whose venue_id exactly matches.
            # - Legacy notifications without venue_id are ignored to avoid cross-venue bleed.
            if venue_id:
                if v != venue_id:
                    continue

            if role == "owner":
                pass_to_out = True
            else:
                pass_to_out = ("all" in t or "manager" in t)

            if pass_to_out:
                # Apply cutoff based on `ts` (UTC ISO timestamp).
                if cutoff:
                    ts_str = str(it.get("ts") or "").strip()
                    dt = _timestamp_to_datetime(ts_str)
                    if dt is None:
                        continue
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    if dt < cutoff:
                        continue
                out.append(it)
                if len(out) >= limit:
                    break
        return out
    except Exception:
        return []
    
def _audit(event: str, details: Optional[Dict[str, Any]] = None) -> None:
    """Append a single-line JSON audit entry (best-effort, non-blocking).
    Writes to Redis (per-venue) and falls back to local file.
    """
    try:
        ctx = _admin_ctx() if "_admin_ctx" in globals() else {}

        # Resolve venue id once so both Redis and file-backed logs are per-venue.
        vid = _venue_id() if "_venue_id" in globals() else "default"

        entry = {
            "ts": datetime.utcnow().isoformat(timespec="seconds") + "Z",
            "event": str(event),
            "role": ctx.get("role", ""),
            "actor": ctx.get("actor", ""),
            "ip": client_ip() if request else "",
            "path": getattr(request, "path", ""),
            "details": details or {},
            "venue_id": vid,
        }

        # --- 1) Redis write (per-venue) ---
        try:
            if "_redis_init_if_needed" in globals():
                _redis_init_if_needed()
            if globals().get("_REDIS_ENABLED") and globals().get("_REDIS"):
                rkey = f"{_REDIS_NS}:{vid}:audit_log"
                _REDIS.lpush(rkey, json.dumps(entry, ensure_ascii=False))
                _REDIS.ltrim(rkey, 0, 2000)  # keep last ~2000 entries
        except Exception:
            pass

        # --- 2) File fallback (legacy / dev) ---
        try:
            os.makedirs(os.path.dirname(AUDIT_LOG_FILE), exist_ok=True)
            with open(AUDIT_LOG_FILE, "a", encoding="utf-8") as f:
                f.write(json.dumps(entry, ensure_ascii=False) + "\n")
        except Exception:
            pass

    except Exception:
        pass


# -----------------------------
# Lightweight in-process caches
# (per-venue only)
# -----------------------------
_CONFIG_CACHE: Dict[str, Any] = {}   # keyed by venue_id -> {"ts":..., "cfg":...}
_LEADS_CACHE: Dict[str, Any] = {}    # keyed by venue_id -> {"ts":..., "rows":...}
_sessions: Dict[str, Dict[str, Any]] = {}  # in-memory chat/reservation sessions

def _last_audit_event(event_name: str, scan_limit: int = 800) -> Optional[Dict[str, Any]]:
    """Return the most recent audit entry for a given event (best-effort)."""
    try:
        if not os.path.exists(AUDIT_LOG_FILE):
            return None
        with open(AUDIT_LOG_FILE, "r", encoding="utf-8") as f:
            lines = f.readlines()[-max(50, min(int(scan_limit), 5000)):]
        for ln in reversed(lines):
            ln = (ln or "").strip()
            if not ln:
                continue
            try:
                e = json.loads(ln)
            except Exception:
                continue
            if e.get("event") == event_name:
                return e
    except Exception:
        return None
    return None


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

def _ensure_ws(gc, title: str, venue_id: Optional[str] = None):
    sh = _open_default_spreadsheet(gc, venue_id=venue_id)
    try:
        return sh.worksheet(title)
    except Exception:
        return sh.add_worksheet(title=title, rows=2000, cols=20)

def get_config() -> Dict[str, str]:
    vid = _venue_id()
    cache = _CONFIG_CACHE.setdefault(vid, {"ts": 0.0, "cfg": None})

    now = time.time()
    cached = cache.get("cfg")
    if isinstance(cached, dict) and (now - float(cache.get("ts", 0.0)) < 5.0):
        return dict(cached)

    cfg: Dict[str, str] = {
        "poll_sponsor_text": "",
        "match_of_day_id": "",
        "motd_home": "",
        "motd_away": "",
        "motd_datetime_utc": "",
        "poll_lock_mode": "auto",
        "ops_pause_reservations": "false",
        "ops_vip_only": "false",
        "ops_waitlist_mode": "false",
    }

    # Sponsor and fan_zone: always from venue config (what admin sets); no hardcoded fallback.
    try:
        venue_cfg_path = os.path.join(VENUES_DIR, f"{vid}.json")
        if os.path.exists(venue_cfg_path):
            with open(venue_cfg_path, "r", encoding="utf-8") as f:
                vcfg = json.load(f) or {}
            fan_zone = vcfg.get("fan_zone") or {}
            if isinstance(fan_zone, dict):
                for k, v in fan_zone.items():
                    if str(k).startswith("_"):
                        continue
                    cfg[str(k)] = "" if v is None else str(v)
    except Exception:
        pass

    # Legacy: also check the old CONFIG_FILE location (for back-compat)
    path = str(CONFIG_FILE).replace("{venue}", vid)
    local = _safe_read_json(path)
    if isinstance(local, dict):
        for k, v in local.items():
            if str(k).startswith("_"):
                continue
            # Only apply if not already set from venue config
            if str(k) not in cfg or cfg.get(str(k), "") == "":
                cfg[str(k)] = "" if v is None else str(v)

    try:
        gc = get_gspread_client()
        ws = _ensure_ws(gc, "Config", venue_id=vid)
        rows = ws.get_all_values()
        for r in rows[1:]:
            if len(r) >= 2 and r[0]:
                k = r[0]
                v = r[1]
                if (k not in cfg) or (cfg.get(k, "") == ""):
                    cfg[k] = v
    except Exception:
        pass

    cache["ts"] = now
    cache["cfg"] = dict(cfg)
    return cfg


def _cfg_bool(cfg: Dict[str, Any], key: str, default: bool = False) -> bool:
    try:
        v = cfg.get(key)
        if isinstance(v, bool):
            return v
        s = str(v or "").strip().lower()
        if s in ("1","true","yes","y","on"):
            return True
        if s in ("0","false","no","n","off",""):
            return False
    except Exception:
        pass
    return bool(default)

def get_ops(cfg: Optional[Dict[str, Any]] = None) -> Dict[str, bool]:
    cfg = cfg or get_config()
    return {
        "pause_reservations": _cfg_bool(cfg, "ops_pause_reservations", False),
        "vip_only": _cfg_bool(cfg, "ops_vip_only", False),
        "waitlist_mode": _cfg_bool(cfg, "ops_waitlist_mode", False),
    }

def set_config(pairs: Dict[str, str]) -> Dict[str, str]:
    vid = _venue_id()
    cache = _CONFIG_CACHE.setdefault(vid, {"ts": 0.0, "cfg": None})

    clean: Dict[str, str] = {}
    for k, v in (pairs or {}).items():
        if not k:
            continue
        clean[str(k)] = "" if v is None else str(v)

    path = str(CONFIG_FILE).replace("{venue}", vid)
    local = _safe_read_json(path)
    if not isinstance(local, dict):
        local = {}
    local.update(clean)
    local["_updated_at"] = datetime.utcnow().isoformat(timespec="seconds") + "Z"
    _safe_write_json(path, local)

    try:
        gc = get_gspread_client()
        ws = _ensure_ws(gc, "Config", venue_id=vid)

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

    cache["ts"] = 0.0
    cache["cfg"] = None
    return get_config()
  
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


def _ensure_venue_ctx_from_poll(body: Optional[Dict[str, Any]] = None) -> str:
    """Resolve venue for poll APIs from query/header/body and apply to g.venue_id."""
    raw_candidates = [
        (request.args.get("venue") or "").strip(),
        (request.headers.get("X-Venue-Id") or "").strip(),
        (body.get("venue_id") or "").strip() if isinstance(body, dict) else "",
        getattr(g, "venue_id", "").strip(),
    ]
    raw = ""
    for v in raw_candidates:
        if v:
            raw = str(v).strip()
            if raw:
                break
    vid = _slugify_venue_id(raw) if raw else _venue_id()
    try:
        g.venue_id = vid
    except Exception:
        pass
    return vid

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




def _poll_client_id(provided: str) -> str:
    """Return a stable anonymous client id for poll voting.

    - If the front-end provides a client_id, we use it.
    - Otherwise we derive a deterministic id from request metadata.
    We hash so we never store raw IP/UA in the poll store/audit.
    """
    p = (provided or "").strip()
    if p:
        return p[:120]
    try:
        ua = (request.headers.get("User-Agent") or "").strip()
        al = (request.headers.get("Accept-Language") or "").strip()
        ip = (request.headers.get("X-Forwarded-For") or request.remote_addr or "").split(",")[0].strip()
        raw = f"{ip}|{ua}|{al}"
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]
    except Exception:
        return "anon"

@app.route("/api/poll/state")
def api_poll_state():
    """
    Return match poll state (VENUE-AWARE).
    Always returns JSON so the Fan Zone UI never breaks.
    """
    try:
        vid = _ensure_venue_ctx_from_poll()

        def _venue_fanzone_cfg():
            try:
                path = os.path.join(VENUES_DIR, f"{vid}.json")
                if not os.path.exists(path):
                    return {}
                with open(path, "r", encoding="utf-8") as f:
                    vcfg = json.load(f) or {}
                return vcfg.get("fan_zone") or {}
            except Exception:
                return {}

        fz_cfg = _venue_fanzone_cfg()

        motd = _get_match_of_day()
        if not motd:
            # Safe placeholder when fixtures are unavailable
            return jsonify({
                "ok": True,
                "locked": True,
                "post_match": False,
                "winner": None,
                "sponsor_text": fz_cfg.get("poll_sponsor_text", ""),
                "match": {
                    "id": "placeholder",
                    "home": "Team A",
                    "away": "Team B",
                    "kickoff": ""
                },
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

        teams = [
            motd.get("home") or "Team A",
            motd.get("away") or "Team B"
        ]

        client_id_raw = (request.args.get("client_id") or "").strip()
        client_id = _poll_client_id(client_id_raw) if client_id_raw else ""
        voted_for = _poll_has_voted(mid, client_id) if client_id else None
        can_vote = (not locked) and (not voted_for)

        counts = _poll_counts(mid)
        total = sum(counts.get(t, 0) for t in teams)

        pct = {}
        for t in teams:
            pct[t] = (counts.get(t, 0) / total * 100.0) if total > 0 else 0.0

        winner = None
        if total > 0:
            winner = max(teams, key=lambda t: counts.get(t, 0))

        top = [{"name": t, "votes": int(counts.get(t, 0))} for t in teams]

        return jsonify({
            "ok": True,
            "can_vote": can_vote,
            "voted_for": voted_for,
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
            "title": "Match of the Day Poll",
            "top": top,
            "counts": {t: int(counts.get(t, 0)) for t in teams},
            "percentages": {t: round(pct[t], 1) for t in teams},
            "percent": {t: round(pct[t], 1) for t in teams},
            "total_votes": int(total),
            "total": int(total),
            # ✅ venue-scoped sponsor text
            "sponsor_text": fz_cfg.get("poll_sponsor_text", ""),
        })
    except Exception:
        # Absolute fallback — never break UI
        return jsonify({
            "ok": True,
            "locked": True,
            "post_match": False,
            "winner": None,
            "sponsor_text": "",
            "match": {
                "id": "placeholder",
                "home": "Team A",
                "away": "Team B",
                "kickoff": ""
            },
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
    _ensure_venue_ctx_from_poll(data)
    client_id_raw = (data.get("client_id") or "").strip()
    client_id = _poll_client_id(client_id_raw)
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
    try:
        _audit("poll.vote", {"match_id": mid, "team": team, "client": client_id[:12]})
    except Exception:
        pass
    return api_poll_state()

def _update_venue_fan_zone(venue_id: str, pairs: Dict[str, str]) -> None:
    """Write key/value pairs into the venue JSON under fan_zone. get_config() reads from fan_zone first, so ops must be persisted here for the admin UI to see them. Invalidates config cache for this venue."""
    if not venue_id or not pairs:
        return
    try:
        path = os.path.join(VENUES_DIR, f"{venue_id}.json")
        if not os.path.exists(path):
            return
        with open(path, "r", encoding="utf-8") as f:
            vcfg = json.load(f) or {}
        vcfg.setdefault("fan_zone", {})
        for k, v in pairs.items():
            if str(k).startswith("_"):
                continue
            vcfg["fan_zone"][str(k)] = "" if v is None else str(v)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(vcfg, f, indent=2, ensure_ascii=False)
        _invalidate_venues_cache()
        _CONFIG_CACHE.pop(venue_id, None)
    except Exception:
        pass


@app.route("/admin/update-config", methods=["POST"])
def admin_update_config():
    ok, resp = _require_admin(min_role="manager")
    if not ok:
        return resp

    data = request.get_json(silent=True) or {}

    # ✅ venue isolation
    venue = (request.args.get("venue") or data.get("venue") or "").strip()
    if not venue:
        return jsonify({"ok": False, "error": "Missing venue"}), 400

    try:
        sponsor = (data.get("poll_sponsor_text") if data.get("poll_sponsor_text") is not None else "")
        match_id = (data.get("match_of_day_id") if data.get("match_of_day_id") is not None else "")

        match_id_norm = str(match_id).strip()
        if match_id_norm:
            match_id_norm = re.sub(r"[^A-Za-z0-9|:_-]+", "_", match_id_norm)[:180]

        motd_home = (data.get("motd_home") if data.get("motd_home") is not None else "")
        motd_away = (data.get("motd_away") if data.get("motd_away") is not None else "")
        motd_datetime_utc = (data.get("motd_datetime_utc") if data.get("motd_datetime_utc") is not None else "")

        poll_lock_mode = (data.get("poll_lock_mode") if data.get("poll_lock_mode") is not None else "auto")

        ops_pause = data.get("ops_pause_reservations")
        ops_vip = data.get("ops_vip_only")
        ops_wait = data.get("ops_waitlist_mode")

        def _norm_bool(v) -> str:
            if isinstance(v, bool):
                return "true" if v else "false"
            s = str(v or "").strip().lower()
            if s in ("1","true","yes","y","on"):
                return "true"
            if s in ("0","false","no","n","off",""):
                return "false"
            return "false"

        pairs = {
            "poll_sponsor_text": str(sponsor).strip(),
            "match_of_day_id": match_id_norm,
            "motd_home": str(motd_home).strip(),
            "motd_away": str(motd_away).strip(),
            "motd_datetime_utc": str(motd_datetime_utc).strip(),
            "poll_lock_mode": (str(poll_lock_mode).strip() or "auto"),
            "ops_pause_reservations": _norm_bool(ops_pause),
            "ops_vip_only": _norm_bool(ops_vip),
            "ops_waitlist_mode": _norm_bool(ops_wait),
        }

        # ✅ load + write ONLY this venue file
        path = os.path.join(VENUES_DIR, f"{venue}.json")
        if not os.path.exists(path):
            return jsonify({"ok": False, "error": f"Unknown venue: {venue}"}), 404

        with open(path, "r", encoding="utf-8") as f:
            vcfg = json.load(f) or {}

        vcfg.setdefault("fan_zone", {})
        vcfg["fan_zone"].update(pairs)

        with open(path, "w", encoding="utf-8") as f:
            json.dump(vcfg, f, indent=2, ensure_ascii=False)

        # ✅ invalidate caches (if present)
        try:
            _invalidate_venues_cache()
        except Exception:
            pass

        _audit("config.update", {"venue": venue, "keys": list(pairs.keys())})

        cfg = vcfg.get("fan_zone", {}) or {}
        return jsonify({"ok": True, "venue": venue, "config": {
            "poll_sponsor_text": cfg.get("poll_sponsor_text",""),
            "match_of_day_id": cfg.get("match_of_day_id",""),
            "motd_home": cfg.get("motd_home",""),
            "motd_away": cfg.get("motd_away",""),
            "motd_datetime_utc": cfg.get("motd_datetime_utc",""),
            "poll_lock_mode": cfg.get("poll_lock_mode","auto"),
            "ops_pause_reservations": cfg.get("ops_pause_reservations","false"),
            "ops_vip_only": cfg.get("ops_vip_only","false"),
            "ops_waitlist_mode": cfg.get("ops_waitlist_mode","false"),
        }})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 400

@app.route("/admin/api/drafts", methods=["GET", "POST"])
def admin_api_drafts():
    """Manage message drafts in dedicated per-venue storage (file or Redis). Never writes to venue config.
    GET: Returns drafts from DRAFTS_FILE / Redis for the given venue.
    POST: Saves drafts to DRAFTS_FILE / Redis (owner-only).
    Query: key (required), venue (required). Works on localhost, staging, production.
    """
    ok, resp = _require_admin(min_role="manager")
    if not ok:
        return resp

    if request.method == "POST":
        ok2, resp2 = _require_admin(min_role="owner")
        if not ok2:
            return resp2

    venue = (request.args.get("venue") or "").strip()
    if not venue:
        return jsonify({"ok": False, "error": "Missing venue parameter (required for venue-specific drafts)"}), 400

    venue_id = _slugify_venue_id(venue)
    # Ensure venue exists (we do not write to venue config, but we require a valid venue)
    venue_cfg_path = os.path.join(VENUES_DIR, f"{venue_id}.json")
    if not os.path.exists(venue_cfg_path):
        return jsonify({"ok": False, "error": f"Venue not found: {venue_id}"}), 404

    # Set venue context so _load_drafts_from_disk / _save_drafts_to_disk use correct path/Redis key
    g.venue_id = venue_id

    # One-time migration: if this venue's config still has 'drafts', move to dedicated store
    _migrate_drafts_out_of_venue_config(venue_id)

    if request.method == "GET":
        data = _load_drafts_from_disk()
        drafts = data.get("drafts") if isinstance(data.get("drafts"), dict) else {}
        meta = {
            "updated_at": data.get("updated_at"),
            "updated_by": data.get("updated_by"),
            "updated_role": data.get("updated_role"),
        }
        return jsonify({"ok": True, "drafts": drafts, "meta": meta})

    # POST
    data = request.get_json(silent=True) or {}
    drafts_data = data.get("drafts")
    if drafts_data is None:
        return jsonify({"ok": False, "error": "Missing drafts object"}), 400
    if not isinstance(drafts_data, dict):
        return jsonify({"ok": False, "error": "drafts must be a JSON object"}), 400

    # Sanitize: keep only string keys and dict values with body (body max 5000)
    clean = {}
    for k, v in drafts_data.items():
        if not isinstance(k, str) or not k.strip():
            continue
        if not isinstance(v, dict):
            continue
        body = str(v.get("body") or "").strip()
        if not body:
            continue
        clean[k.strip()] = {
            "channel": str(v.get("channel") or "").strip(),
            "title": str(v.get("title") or "").strip(),
            "subject": str(v.get("subject") or "").strip(),
            "body": body[:5000],
        }

    ctx = _admin_ctx()
    saved = _save_drafts_to_disk({"drafts": clean}, actor=ctx.get("actor", ""), role=ctx.get("role", ""))
    _audit("drafts.update", {"venue": venue_id, "actor": ctx.get("actor", ""), "draft_keys": list(clean.keys())})
    meta = {"updated_at": saved.get("updated_at")}
    return jsonify({"ok": True, "drafts": clean, "meta": meta})

@app.route("/admin/api/rules", methods=["GET","POST"])
def admin_api_rules():
    ok, resp = _require_admin(min_role="manager")
    if not ok:
        return resp


    # Managers can view; only Owners can modify.
    if request.method != "GET":
        ok2, resp2 = _require_admin(min_role="owner")
        if not ok2:
            return resp2

    global BUSINESS_RULES
    if request.method == "GET":
        return jsonify({"ok": True, "rules": BUSINESS_RULES})

    payload = request.get_json(silent=True) or {}
    updated = _coerce_rules(payload)
    if not updated:
        return jsonify({"ok": False, "error": "No valid rule fields provided"}), 400

    # Update in-memory + persist
    BUSINESS_RULES = _deep_merge(BUSINESS_RULES, updated)
    _persist_rules(updated)
    _audit("rules.update", {"keys": list(updated.keys())})
    return jsonify({"ok": True, "rules": BUSINESS_RULES})

@app.route("/admin/api/menu", methods=["GET","POST"])
def admin_api_menu():
    ok, resp = _require_admin(min_role="manager")
    if not ok:
        return resp


    # Managers can view; only Owners can modify.
    if request.method != "GET":
        ok2, resp2 = _require_admin(min_role="owner")
        if not ok2:
            return resp2

    global _MENU_OVERRIDE
    if request.method == "GET":
        return jsonify({"ok": True, "menu": _MENU_OVERRIDE or MENU})

    payload = request.get_json(silent=True)
    if payload is None:
        return jsonify({"ok": False, "error": "Expected JSON body"}), 400

    try:
        normed = _normalize_menu_payload(payload)
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 400

    _MENU_OVERRIDE = _bump_menu_meta(normed)
    _safe_write_json_file(MENU_FILE, _MENU_OVERRIDE)
    _audit("menu.update", {"langs": [k for k in _MENU_OVERRIDE.keys() if not str(k).startswith('_')], "version": _MENU_OVERRIDE.get('_meta',{}).get('version')})
    return jsonify({"ok": True, "menu": _MENU_OVERRIDE})

@app.route("/admin/api/menu-upload", methods=["POST"])
def admin_api_menu_upload():
    ok, resp = _require_admin(min_role="owner")
    if not ok:
        return resp

    global _MENU_OVERRIDE
    if "file" not in request.files:
        return jsonify({"ok": False, "error": "Missing file field 'file'"}), 400

    f = request.files["file"]
    raw = f.read()
    try:
        payload = json.loads(raw.decode("utf-8", errors="strict"))
        normed = _normalize_menu_payload(payload)
    except Exception as e:
        return jsonify({"ok": False, "error": f"Invalid menu file: {e}"}), 400

    _MENU_OVERRIDE = _bump_menu_meta(normed)
    _safe_write_json_file(MENU_FILE, _MENU_OVERRIDE)
    _audit("menu.upload", {"size_bytes": len(raw), "version": _MENU_OVERRIDE.get('_meta',{}).get('version')})
    return jsonify({"ok": True, "menu": _MENU_OVERRIDE})



@app.route("/admin/api/ops", methods=["GET", "POST"])
def admin_api_ops():
    ok, resp = _require_admin(min_role="manager")
    if not ok:
        return resp

    if request.method == "GET":
        cfg = get_config()
        meta = _last_audit_event("ops.update")
        return jsonify({"ok": True, "ops": get_ops(cfg), "meta": meta})

    data = request.get_json(silent=True) or {}
    def _norm_bool(v) -> str:
        if isinstance(v, bool):
            return "true" if v else "false"
        s = str(v or "").strip().lower()
        if s in ("1","true","yes","y","on"):
            return "true"
        if s in ("0","false","no","n","off",""):
            return "false"
        return "false"

    pairs = {
        "ops_pause_reservations": _norm_bool(data.get("pause_reservations")),
        "ops_vip_only": _norm_bool(data.get("vip_only")),
        "ops_waitlist_mode": _norm_bool(data.get("waitlist_mode")),
    }
    _update_venue_fan_zone(_venue_id(), pairs)
    cfg = set_config(pairs)
    _audit("ops.update", {"ops": get_ops(cfg)})
    meta = _last_audit_event("ops.update")
    return jsonify({"ok": True, "ops": get_ops(cfg), "meta": meta})
# ============================================================
# Match-Day Presets

# ============================================================
# AI Automation Settings (Admin API)
# - Managers: enable/disable + mode + approval requirement
# - Owners: everything (model/prompts/thresholds/allowed actions)
# ============================================================
@app.route("/admin/api/ai/settings", methods=["GET", "POST"])
def admin_api_ai_settings():
    ok, resp = _require_admin(min_role="manager")
    if not ok:
        return resp

    try:
        ctx = _admin_ctx() or {}
    except Exception:
        ctx = {}
    role = ctx.get("role", "")
    actor = ctx.get("actor", "")


    if request.method == "GET":
        try:
            settings = _get_ai_settings()
            return jsonify({"ok": True, "role": role, "settings": settings})
        except Exception as e:
            return jsonify({"ok": False, "error": str(e), "role": role, "settings": {}})

    data = request.get_json(silent=True) or {}

    def as_bool(v: Any) -> bool:
        if isinstance(v, bool):
            return v
        s = str(v or "").strip().lower()
        return s in ("1", "true", "yes", "y", "on")

    def as_float(v: Any, default: float) -> float:
        try:
            return float(v)
        except Exception:
            return default

    patch: Dict[str, Any] = {}
    current = _get_ai_settings()

    # Always-allowed fields (manager+)
    if "enabled" in data:
        patch["enabled"] = bool(as_bool(data.get("enabled")))
    if "mode" in data:
        mode = str(data.get("mode") or "").strip().lower()
        if mode not in ("off", "suggest", "auto"):
            return jsonify({"ok": False, "error": "Invalid mode"}), 400
        patch["mode"] = mode
    if "require_approval" in data:
        patch["require_approval"] = bool(as_bool(data.get("require_approval")))

    # Managers can adjust confidence gate (safe)
    if "min_confidence" in data:
        mc = as_float(data.get("min_confidence"), float(current.get("min_confidence") or 0.7))
        mc = max(0.0, min(1.0, mc))
        patch["min_confidence"] = mc

    # Feature flags (manager-safe). These do NOT grant new powers; they only further restrict actions.
    feats_in = data.get("features")
    if isinstance(feats_in, dict):
        feats_patch = {}
        if "auto_vip_tag" in feats_in:
            feats_patch["auto_vip_tag"] = as_bool(feats_in.get("auto_vip_tag"))
        if "auto_status_update" in feats_in:
            feats_patch["auto_status_update"] = as_bool(feats_in.get("auto_status_update"))
        if "auto_reply_draft" in feats_in:
            feats_patch["auto_reply_draft"] = as_bool(feats_in.get("auto_reply_draft"))
        if feats_patch:
            patch["features"] = feats_patch


    # Owner-only advanced fields
    if role == "owner":
        if "model" in data:
            patch["model"] = str(data.get("model") or "").strip() or current.get("model")
        if "system_prompt" in data:
            sp = str(data.get("system_prompt") or "").strip()
            # Keep prompt bounded to avoid accidental huge payloads
            if len(sp) > 6000:
                return jsonify({"ok": False, "error": "system_prompt too long"}), 400
            if sp:
                patch["system_prompt"] = sp
        if "allow_actions" in data and isinstance(data.get("allow_actions"), dict):
            # Only allow known keys (including outbound for AI-suggested messages)
            allow = {}
            for k in ("vip_tag", "status_update", "reply_draft", "send_sms", "send_email", "send_whatsapp"):
                if k in data["allow_actions"]:
                    allow[k] = bool(as_bool(data["allow_actions"].get(k)))
            patch["allow_actions"] = _deep_merge(current.get("allow_actions") or {}, allow)

        if "notify" in data and isinstance(data.get("notify"), dict):
            notify = {}
            for k in ("owner", "manager"):
                if k in data["notify"]:
                    notify[k] = bool(as_bool(data["notify"].get(k)))
            patch["notify"] = _deep_merge(current.get("notify") or {}, notify)

    # If manager tried to send advanced fields, ignore (do not error)
    settings = _save_ai_settings_to_disk(patch, actor=actor, role=role)
    _audit("ai.settings.update", {"patch": patch, "mode": settings.get("mode"), "enabled": settings.get("enabled")})
    return jsonify({"ok": True, "role": role, "settings": settings})




# ============================================================
# AI Queue API (Admin/Manager)
# ============================================================



@app.route("/admin/api/ai/run", methods=["POST"])
def admin_api_ai_run():
    """Manually run AI triage from Admin UI (Option B).

    This does NOT auto-apply anything. It only proposes actions into the AI Queue
    so managers/owners can approve/deny.
    Payload:
      { "mode": "new", "limit": 5 }  -> run on newest leads with status "New"
      { "row": 12 }                 -> run on a specific sheet row (2..N)
    """
    ok, resp = _require_admin(min_role="manager")
    if not ok:
        return resp

    ctx = _admin_ctx()
    actor = ctx.get("actor", "")
    role = ctx.get("role", "")

    data = request.get_json(silent=True) or {}
    mode = (data.get("mode") or "new").strip().lower()
    try:
        limit = int(data.get("limit") or 5)
    except Exception:
        limit = 5
    limit = max(1, min(25, limit))

    try:
        row_num = int(data.get("row") or 0)
    except Exception:
        row_num = 0

    # Per-venue AI settings (used for selecting which statuses count as "new")
    ai_settings = _get_ai_settings()
    raw_new_statuses = ai_settings.get("new_status_values")
    if isinstance(raw_new_statuses, list) and raw_new_statuses:
        new_status_values = {
            str(v or "").strip().lower() for v in raw_new_statuses
        }
    else:
        # Back-compat: default to historical behavior
        new_status_values = {"new", ""}

    # Load sheet (best-effort). If Sheets isn't configured, return a friendly error.
    try:
        gc = get_gspread_client()
        ws = _open_default_spreadsheet(gc, venue_id=_venue_id()).sheet1
        header = ensure_sheet_schema(ws)
        hmap = header_map(header)
    except Exception as e:
        return jsonify({"ok": False, "error": "Sheets not available", "detail": str(e)[:300]}), 400

    def _lead_from_row(row_vals: list) -> dict:
        def get(col: str) -> str:
            idx = hmap.get(_normalize_header(col))
            if not idx:
                return ""
            i0 = idx - 1
            return (row_vals[i0] if i0 < len(row_vals) else "") or ""
        lead = {
            "name": get("name"),
            "phone": get("phone"),
            "email": get("email"),
            "date": get("date"),
            "time": get("time"),
            "party_size": get("party_size"),
            "language": get("language"),
            "entry_point": get("entry_point"),
            "tier": get("tier"),
            "queue": get("queue"),
            "business_context": get("business_context"),
            "budget": get("budget"),
            "notes": get("notes"),
            "vibe": get("vibe"),
            "status": get("status"),
            "vip": get("vip"),
        }
        # Normalize vip to bool-ish for downstream policy + prompts
        lead["vip"] = str(lead.get("vip") or "").strip().lower() in ["1", "true", "yes", "y"]

        # Populate generic AI prompt fields so _ai_build_lead_prompt sees rich context.
        # These mirror the mapping used by /admin/api/ai/replay and lead intake.
        if not lead.get("intent"):
            # Prefer explicit intent/entry_point/queue-like signals.
            intent_parts = []
            ep = (lead.get("entry_point") or "").strip()
            if ep:
                intent_parts.append(ep)
            q = (lead.get("queue") or "").strip()
            if q:
                intent_parts.append(q)
            lead["intent"] = " / ".join(intent_parts)[:200]

        if not lead.get("contact"):
            # Combine human-readable name + primary contact channel.
            name = (lead.get("name") or "").strip()
            phone = (lead.get("phone") or "").strip()
            email = (lead.get("email") or "").strip()
            contact = (f"{name} {phone}".strip() or email).strip()
            lead["contact"] = contact

        if not lead.get("datetime"):
            # Fold date + time into a single string, as used elsewhere.
            dt = " ".join(
                part for part in [(lead.get("date") or "").strip(), (lead.get("time") or "").strip()] if part
            ).strip()
            lead["datetime"] = dt

        if not lead.get("lang"):
            # Mirror "language" into the generic lang field when present.
            lead["lang"] = (lead.get("language") or "").strip()

        return lead

    targets = []
    if row_num >= 2:
        try:
            row_vals = ws.row_values(row_num) or []
            targets = [(row_num, _lead_from_row(row_vals))]
        except Exception as e:
            return jsonify({"ok": False, "error": "Failed to read row", "detail": str(e)[:300]}), 400
    else:
        # mode "new": newest first, status == "New"
        try:
            rows = ws.get_all_values()
        except Exception as e:
            return jsonify({"ok": False, "error": "Failed to read sheet", "detail": str(e)[:300]}), 400

        if not rows or len(rows) < 2:
            return jsonify({"ok": True, "ran": 0, "proposed": 0, "queue_ids": []})

        status_col = hmap.get("status")  # 1-based, optional
        # Walk from bottom (newest) upward, collect rows whose status is in the
        # configurable "new_status_values" list. If there is no status column,
        # treat all rows as eligible and rely on limit for bounding.
        for i in range(len(rows) - 1, 0, -1):
            if len(targets) >= limit:
                break
            rv = rows[i]
            if status_col:
                st = ""
                if (status_col - 1) < len(rv):
                    st = (rv[status_col - 1] or "").strip().lower()
                if st not in new_status_values:
                    continue
            sheet_row = i + 1  # rows includes header at index 0
            targets.append((sheet_row, _lead_from_row(rv)))

    proposed_ids = []
    ran = 0

    # Run AI and push proposals into the queue
    for sheet_row, lead in targets:
        ran += 1
        out = _ai_suggest_actions_for_lead(lead, sheet_row=sheet_row)
        if not out or not out.get("ok"):
            continue
        conf = float(out.get("confidence") or 0.0)
        try:
            conf = max(0.0, min(1.0, conf))
        except Exception:
            conf = 0.0
        actions = out.get("actions") or []
        for a in actions:
            typ = str(a.get("type") or "").strip()
            payload = dict(a.get("payload") or {})
            if typ == "vip_tag" and not str(payload.get("vip") or "").strip():
                payload["vip"] = "VIP"
            # Always attach sheet_row for safe apply handlers
            if "sheet_row" not in payload:
                payload["sheet_row"] = sheet_row
            payload["row"] = sheet_row  # outbound/send also uses "row"
            # Enrich outbound payload from drafts + lead when body/to missing
            if typ in ("send_sms", "send_email", "send_whatsapp"):
                lead_data = {k: str(lead.get(k) or "").strip() for k in ["name", "date", "time", "party_size", "phone", "email"]}
                if not payload.get("to"):
                    payload["to"] = lead_data.get("email") if typ == "send_email" else lead_data.get("phone")
                if not payload.get("body") and not payload.get("message"):
                    ch = "email" if typ == "send_email" else "sms"
                    draft_key = _select_draft_for_channel(ch, "confirm")
                    body, subj = _get_draft_content(draft_key, lead_data)
                    if body:
                        payload["body"] = payload["message"] = body
                    if subj and typ == "send_email":
                        payload["subject"] = subj
            rationale = str(a.get("reason") or out.get("notes") or "")[:1500]
            entry = {
                "id": _queue_new_id(),
                "type": typ,
                "payload": payload,
                "confidence": conf,
                "rationale": rationale,
                "status": "pending",
                "created_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "created_by": actor,
                "created_role": role,
                "reviewed_at": None,
                "reviewed_by": None,
                "reviewed_role": None,
                "applied_result": None,
            }
            _queue_add(entry)
            proposed_ids.append(entry["id"])

    if proposed_ids:
        _audit("ai.run.manual", {"mode": mode, "row": row_num or None, "ran": ran, "proposed": len(proposed_ids)})
        _notify("ai.run.manual", {"ran": ran, "proposed": len(proposed_ids), "by": actor, "role": role}, targets=["owner","manager"])

    return jsonify({"ok": True, "ran": ran, "proposed": len(proposed_ids), "queue_ids": proposed_ids})
@app.route("/admin/api/ai/queue", methods=["GET"])
def admin_api_ai_queue_list():
    ok, resp = _require_admin(min_role="manager")
    if not ok:
        return resp
    try:
        ctx = _admin_ctx() or {}
    except Exception:
        ctx = {}
    role = ctx.get("role", "")
    queue = _load_ai_queue()
    # optional status filter
    status = (request.args.get("status") or "").strip().lower()
    if status:
        queue = [q for q in queue if str(q.get("status") or "").lower() == status]

    # optional time filter (server-side): created_at within last N minutes
    time_param = (request.args.get("time") or "").strip()
    time_minutes = _parse_time_range_minutes(time_param) if time_param else None
    if time_minutes is None and time_param:
        try:
            time_minutes = int(time_param)
        except Exception:
            time_minutes = None
    if time_minutes and time_minutes > 0:
        cutoff = datetime.now(timezone.utc) - timedelta(minutes=time_minutes)
        def _parse_created_at(s: Any) -> Optional[datetime]:
            try:
                if s is None:
                    return None
                ts = str(s).strip()
                if not ts:
                    return None
                # Normalize Z -> +00:00 for fromisoformat
                ts = ts.replace("Z", "+00:00")
                dt = datetime.fromisoformat(ts)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt.astimezone(timezone.utc)
            except Exception:
                return None
        queue = [
            q for q in queue
            if (_parse_created_at(q.get("created_at")) or cutoff) >= cutoff
        ]

    # optional type filter
    type_param = (request.args.get("type") or "").strip().lower()
    if type_param:
        queue = [
            q for q in queue
            if str(q.get("type") or "").strip().lower() == type_param
        ]

    # optional min confidence filter
    conf_param = (request.args.get("conf") or "").strip()
    if conf_param:
        try:
            min_conf = float(conf_param)
        except Exception:
            min_conf = None
        if min_conf is not None:
            queue = [
                q for q in queue
                if float(q.get("confidence") or 0.0) >= min_conf
            ]

    # Always return newest first (so recent-time UX is consistent)
    def _parse_created_at(s: Any) -> Optional[datetime]:
        try:
            if s is None:
                return None
            ts = str(s).strip()
            if not ts:
                return None
            ts = ts.replace("Z", "+00:00")
            dt = datetime.fromisoformat(ts)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        except Exception:
            return None
    queue.sort(key=lambda q: _parse_created_at(q.get("created_at")) or datetime.min.replace(tzinfo=timezone.utc), reverse=True)

    # optional search across queue item + payload fields
    q_param = (request.args.get("q") or "").strip().lower()
    if q_param:
        # Keep this lightweight and safe: build a small search blob per item.
        def _payload_blob(it: Dict[str, Any]) -> str:
            try:
                p = it.get("payload") or {}
                if not isinstance(p, dict):
                    p = {}
                parts = [
                    str(it.get("type") or ""),
                    str(it.get("status") or ""),
                    str(it.get("rationale") or ""),
                    str(it.get("created_at") or ""),
                    str(it.get("confidence") or ""),
                ]
                # Common fields for our queue item types
                for k in ("reservation_id", "row", "sheet_row", "draft", "to", "subject", "message", "phone", "email"):
                    if k in p:
                        parts.append(str(p.get(k) or ""))
                # Fallback: include full payload JSON
                parts.append(json.dumps(p, ensure_ascii=False))
                return " ".join(parts).lower()
            except Exception:
                return str(it).lower()

        queue = [it for it in queue if q_param in _payload_blob(it)]
    return jsonify({"ok": True, "role": role, "queue": queue[:500]})


@app.route("/admin/api/ai/queue/clear", methods=["POST"])
def admin_api_ai_queue_clear():
    """
    Clear all AI queue items for the current venue.
    - Manager+ only
    - Empties the per-venue AI_QUEUE_FILE
    """
    ok, resp = _require_admin(min_role="manager")
    if not ok:
        return resp

    try:
        queue = _load_ai_queue()
        cleared = len(queue)
        _save_ai_queue([])
        try:
            _audit("ai.queue.clear", {"cleared": cleared})
        except Exception:
            pass
        return jsonify({"ok": True, "cleared": cleared})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500



@app.route("/admin/api/outbound/propose", methods=["POST"])
def admin_api_outbound_propose():
    """
    Create an outbound send item in the AI Queue (pending).
    Human approval + explicit send click required.
    Body: {
      "partner": "VENUE_ABC" (optional),
      "channel": "email|sms|whatsapp",
      "to": "...",
      "subject": "..." (email only),
      "message": "..." (optional - will use draft if not provided and draft_key/row provided),
      "draft_key": "email_confirm" (optional - use specific draft),
      "context": "confirm" (optional - for draft selection),
      "row": 12 (optional lead row - used for draft template replacement)
    }
    """
    ok, resp = _require_admin(min_role="manager")
    if not ok:
        return resp
    ctx = _admin_ctx()
    role = ctx.get("role", "")
    actor = ctx.get("actor", "")

    data = request.get_json(silent=True) or {}
    channel = str(data.get("channel") or "").strip().lower()
    if channel not in ("email", "sms", "whatsapp"):
        return jsonify({"ok": False, "error": "Invalid channel"}), 400
    action_type = f"send_{channel}"
    
    # Load lead data if row provided (for template replacement)
    lead_data = {}
    row_num = data.get("row")
    if row_num:
        try:
            lead_data = _load_lead_from_sheet_row(int(row_num))
        except Exception:
            pass
    
    # Determine message body and subject
    message = data.get("message") or ""
    subject = data.get("subject") or ""
    draft_key = data.get("draft_key")
    
    # If no message provided, try to use draft
    if not message.strip():
        if not draft_key:
            # Auto-select draft based on channel and context
            context = data.get("context", "confirm")
            draft_key = _select_draft_for_channel(channel, context)
        
        if draft_key:
            draft_body, draft_subject = _get_draft_content(draft_key, lead_data)
            if draft_body:
                message = draft_body
                if draft_subject and not subject:
                    subject = draft_subject
                # Store draft_key in payload for reference
                payload = {
                    "partner": data.get("partner"),
                    "row": row_num,
                    "to": data.get("to"),
                    "subject": subject,
                    "message": message,
                    "body": message,  # UI uses 'body'
                    "draft_key": draft_key,  # Track which draft was used
                }
            else:
                # Draft not found or empty
                payload = {
                    "partner": data.get("partner"),
                    "row": row_num,
                    "to": data.get("to"),
                    "subject": subject,
                    "message": message,
                    "body": message,
                }
        else:
            # No draft available
            payload = {
                "partner": data.get("partner"),
                "row": row_num,
                "to": data.get("to"),
                "subject": subject,
                "message": message,
                "body": message,
            }
    else:
        # Message provided explicitly - format it if it has placeholders and we have lead data
        if lead_data and ("{" in message):
            message = _format_draft_template(message, lead_data)
        payload = {
            "partner": data.get("partner"),
            "row": row_num,
            "to": data.get("to"),
            "subject": subject,
            "message": message,
            "body": message,
        }
    
    # Best-effort partner id for policy gating
    partner = _derive_partner_id(payload=payload)

    allowed, reason = _policy_check_action(partner, action_type, payload, role=role)
    if not allowed:
        _audit("policy.block", {"partner": partner, "type": action_type, "reason": reason, "source": "outbound.propose"})
        return jsonify({"ok": False, "error": reason}), 403

    q = _load_ai_queue()
    qid = _queue_new_id()
    item = {
        "id": qid,
        "type": action_type,
        "payload": payload,
        "status": "pending",
        "created_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "created_by": actor,
        "created_role": role,
        "partner": partner,
        "human_required": True,
    }
    q.insert(0, item)
    _save_ai_queue(q)
    _audit("outbound.queue", {"id": qid, "partner": partner, "type": action_type, "by": actor, "draft_key": draft_key})
    _notify("outbound.queue", {"id": qid, "partner": partner, "type": action_type, "by": actor, "role": role}, targets=["owner","manager"])
    return jsonify({"ok": True, "id": qid})


@app.route("/admin/api/ai/queue/propose", methods=["POST"])
def admin_api_ai_queue_propose():
    # Minimal: allow manager+ to create a proposal (used for testing; later AI will call this)
    ok, resp = _require_admin(min_role="manager")
    if not ok:
        return resp
    ctx = _admin_ctx()
    actor = ctx.get("actor", "")
    role = ctx.get("role", "")

    data = request.get_json(silent=True) or {}
    typ = str(data.get("type") or "").strip()
    if typ not in ("vip_tag", "status_update", "reply_draft"):
        return jsonify({"ok": False, "error": "Invalid type"}), 400

    confidence = float(data.get("confidence") or 0.0)
    confidence = max(0.0, min(1.0, confidence))

    entry = {
        "id": _queue_new_id(),
        "type": typ,
        "payload": data.get("payload") or {},
        "confidence": confidence,
        "rationale": str(data.get("rationale") or "")[:1500],
        "status": "pending",
        "created_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "created_by": actor,
        "created_role": role,
        "reviewed_at": None,
        "reviewed_by": None,
        "reviewed_role": None,
        "applied_result": None,
    }
    _queue_add(entry)
    _audit("ai.queue.propose", {"id": entry["id"], "type": typ, "confidence": confidence})
    return jsonify({"ok": True, "id": entry["id"], "entry": entry})


@app.route("/admin/api/ai/queue/<qid>/deny", methods=["POST"])
def admin_api_ai_queue_deny(qid: str):
    ok, resp = _require_admin(min_role="manager")
    if not ok:
        return resp
    ctx = _admin_ctx()
    actor = ctx.get("actor", "")
    role = ctx.get("role", "")

    queue = _load_ai_queue()
    it = _queue_find(queue, qid)
    if not it:
        return jsonify({"ok": False, "error": "Not found"}), 404

    if str(it.get("status")) != "pending":
        return jsonify({"ok": False, "error": "Not pending"}), 400

    # Client spec: deny removes item from queue immediately (keeps queue clean), audits + notifies
    queue = [q for q in queue if str(q.get("id")) != str(qid)]
    _save_ai_queue(queue)
    _audit("ai.queue.deny", {"id": qid, "type": it.get("type")})
    _notify("ai.queue.deny", {"id": qid, "type": it.get("type"), "by": actor, "role": role}, targets=["owner","manager"])
    return jsonify({"ok": True})


@app.route("/admin/api/ai/queue/<qid>/delete", methods=["POST"])
def admin_api_ai_queue_delete(qid: str):
    """
    Hard-remove a queue item, regardless of status.
    - Manager+ only
    - Does not apply or deny the action; simply drops it from the queue
    """
    ok, resp = _require_admin(min_role="manager")
    if not ok:
        return resp

    queue = _load_ai_queue()
    it = _queue_find(queue, qid)
    if not it:
        return jsonify({"ok": False, "error": "Not found"}), 404

    queue = [q for q in queue if str(q.get("id")) != str(qid)]
    _save_ai_queue(queue)
    try:
        _audit("ai.queue.delete", {"id": qid, "type": it.get("type"), "status": it.get("status")})
    except Exception:
        pass
    return jsonify({"ok": True})

@app.route("/admin/api/ai/queue/<qid>/approve", methods=["POST"])
def admin_api_ai_queue_approve(qid: str):
    ok, resp = _require_admin(min_role="manager")
    if not ok:
        return resp
    ctx = _admin_ctx()
    actor = ctx.get("actor", "")
    role = ctx.get("role", "")

    queue = _load_ai_queue()
    it = _queue_find(queue, qid)
    if not it:
        return jsonify({"ok": False, "error": "Not found"}), 404
    if str(it.get("status")) != "pending":
        return jsonify({"ok": False, "error": "Not pending"}), 400

    it_type = str(it.get("type") or "").strip().lower()

    # Outbound sends are NEVER executed on approval. Approval only unlocks a human "Send Now" click.
    applied = None
    if it_type in ("send_email", "send_sms", "send_whatsapp", "send_confirmation", "send_reservation_received", "send_update", "send_vip_update"):
        applied = {"ok": True, "note": "Approved — ready to send (human click required)"}
        # Keep item in queue; just mark as approved/reviewed.
        it["status"] = "approved"
        it["reviewed_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        it["reviewed_by"] = actor
        it["reviewed_role"] = role
        it["applied_result"] = applied
    else:
        # Non-outbound: apply then remove on success; keep on failure.
        settings = _get_ai_settings()
        if settings.get("enabled") and (settings.get("mode") in ("auto", "suggest", "off")):
            applied = _queue_apply_action({"type": it.get("type"), "payload": it.get("payload")}, ctx)
        # Success -> remove from queue; Failure -> keep (pending) with applied_result for debugging.
        if applied and applied.get("ok"):
            queue = [q for q in queue if str(q.get("id")) != str(qid)]
        else:
            it["applied_result"] = applied

    _save_ai_queue(queue)
    _audit("ai.queue.approve", {"id": qid, "type": it.get("type"), "applied": bool(applied and applied.get("ok"))})
    _notify("ai.queue.approve", {"id": qid, "type": it.get("type"), "applied": bool(applied and applied.get("ok")), "by": actor, "role": role}, targets=["owner","manager"])
    return jsonify({"ok": True, "applied_result": applied})



@app.route("/admin/api/ai/queue/<qid>/send", methods=["POST"])
def admin_api_ai_queue_send(qid: str):
    """
    Human-triggered outbound send for approved queue items.
    This endpoint is the ONLY place outbound messages are executed.
    """
    ok, resp = _require_admin(min_role="manager")
    if not ok:
        return resp
    ctx = _admin_ctx()
    actor = ctx.get("actor", "")
    role = ctx.get("role", "")

    queue = _load_ai_queue()
    it = _queue_find(queue, qid)
    if not it:
        return jsonify({"ok": False, "error": "Not found"}), 404

    it_type = str(it.get("type") or "").strip().lower()
    if it_type not in ("send_email", "send_sms", "send_whatsapp", "send_confirmation", "send_reservation_received", "send_update", "send_vip_update"):
        return jsonify({"ok": False, "error": "Not an outbound item"}), 400

    if str(it.get("status")) != "approved":
        return jsonify({"ok": False, "error": "Must be approved first"}), 400

    if it.get("sent_at"):
        return jsonify({"ok": False, "error": "Already sent"}), 400

    payload = it.get("payload") or {}
    # Partner policy must allow this outbound channel and role must meet requirement
    partner = _derive_partner_id(payload=payload)
    allowed, reason = _policy_check_action(partner, it_type, payload, role=role)
    if not allowed:
        _audit("policy.block", {"partner": partner, "type": it_type, "reason": reason, "id": qid})
        return jsonify({"ok": False, "error": reason}), 403

    # Execute send
    res = _outbound_send(it_type, payload)

    ok_send = bool(res.get("ok"))
    if ok_send:
        # Spec: Send Now (outbound): removes only if send succeeds; keeps failures for retry.
        queue = [q for q in queue if str(q.get("id")) != str(qid)]
        _save_ai_queue(queue)
    else:
        # Keep item for retry; optionally attach last error for debugging.
        it["send_result"] = res
        _save_ai_queue(queue)

    _audit("outbound.send", {"id": qid, "partner": partner, "type": it_type, "ok": ok_send, "by": actor})
    _notify("outbound.send", {"id": qid, "partner": partner, "type": it_type, "ok": ok_send, "by": actor, "role": role}, targets=["owner","manager"])
    if ok_send:
        return jsonify({"ok": True, "result": res})
    err = res.get("message") or res.get("error") or "Send failed"
    return jsonify({"ok": False, "result": res, "error": err})


@app.route("/admin/api/ai/queue/<qid>/override", methods=["POST"])
def admin_api_ai_queue_override(qid: str):
    ok, resp = _require_admin(min_role="owner")
    if not ok:
        return resp
    ctx = _admin_ctx()
    actor = ctx.get("actor", "")
    role = ctx.get("role", "")

    data = request.get_json(silent=True) or {}
    queue = _load_ai_queue()
    it = _queue_find(queue, qid)
    if not it:
        return jsonify({"ok": False, "error": "Not found"}), 404

    if str(it.get("status")) not in ("pending", "approved"):
        return jsonify({"ok": False, "error": "Not editable"}), 400

    # override payload/type (owner-only)
    if "type" in data:
        typ = str(data.get("type") or "").strip()
        if typ not in ("vip_tag", "status_update", "reply_draft", "send_email", "send_sms", "send_whatsapp", "send_confirmation", "send_reservation_received", "send_update", "send_vip_update"):
            return jsonify({"ok": False, "error": "Invalid type"}), 400
        it["type"] = typ
    if "payload" in data and isinstance(data.get("payload"), dict):
        it["payload"] = data.get("payload") or {}

    # Apply immediately for non-outbound actions.
    applied = None
    it_type = str(it.get("type") or "").strip().lower()
    settings = _get_ai_settings()
    if settings.get("enabled") and it_type in ("vip_tag", "status_update", "reply_draft"):
        applied = _queue_apply_action({"type": it.get("type"), "payload": it.get("payload")}, ctx)

    it["status"] = "approved"
    it["reviewed_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    it["reviewed_by"] = actor
    it["reviewed_role"] = role
    it["applied_result"] = applied
    _save_ai_queue(queue)
    _audit("ai.queue.override", {"id": qid, "type": it.get("type"), "applied": bool(applied and applied.get("ok"))})
    _notify("ai.queue.override", {"id": qid, "type": it.get("type"), "applied": bool(applied and applied.get("ok")), "by": actor, "role": role}, targets=["owner","manager"])
    return jsonify({"ok": True, "applied_result": applied})


# ============================================================
# Match-Day Presets (Admin)
# - One-click bundles that flip multiple Ops toggles + Rule values
# - Logged to audit
# ============================================================
MATCHDAY_PRESETS: Dict[str, Dict[str, Any]] = {
    "Kickoff Rush": {
        "ops": {"pause_reservations": False, "vip_only": True, "waitlist_mode": True},
        "rules": {"max_party_size": 6, "match_day_banner": "🏟️ Kickoff Rush: VIP priority + waitlist enabled"},
    },
    "Halftime Surge": {
        "ops": {"pause_reservations": False, "vip_only": False, "waitlist_mode": True},
        "rules": {"max_party_size": 4, "match_day_banner": "⏱️ Halftime Surge: fast seating + waitlist enabled"},
    },
    "Post-game": {
        "ops": {"pause_reservations": False, "vip_only": False, "waitlist_mode": False},
        "rules": {"max_party_size": 10, "match_day_banner": "🌙 Post-game: larger groups welcome"},
    },
}



# ============================================================
# Monitoring APIs (Health + Alert Settings)
# ============================================================
@app.route("/admin/api/health", methods=["GET"])
def admin_api_health():
    ok, resp = _require_admin(min_role="manager")
    if not ok:
        return resp
    report = _run_health_checks()
    st = _alert_state()
    last_any = st.get('_last_any')
    last_any_ts = ''
    try:
        if last_any:
            last_any_ts = datetime.fromtimestamp(int(last_any), timezone.utc).isoformat()
    except Exception:
        last_any_ts = ''
    return jsonify({"ok": True, "report": report, "alerts_enabled": bool(ALERT_SETTINGS.get("enabled")), "alerts_last_any": last_any or 0, "alerts_last_any_ts": last_any_ts})

@app.route("/admin/api/health/run", methods=["POST"])
def admin_api_health_run():
    ok, resp = _require_admin(min_role="manager")
    if not ok:
        return resp
    report = _run_health_checks()
    alerts = _maybe_alert_on_health(report)
    st = _alert_state()
    last_any = st.get('_last_any')
    last_any_ts = ''
    try:
        if last_any:
            last_any_ts = datetime.fromtimestamp(int(last_any), timezone.utc).isoformat()
    except Exception:
        last_any_ts = ''
    return jsonify({"ok": True, "report": report, "alerts": alerts, "alerts_enabled": bool(ALERT_SETTINGS.get("enabled")), "alerts_last_any": last_any or 0, "alerts_last_any_ts": last_any_ts})

@app.route("/admin/api/alerts/settings", methods=["GET","POST"])
def admin_api_alert_settings():
    if request.method == "GET":
        ok, resp = _require_admin(min_role="manager")
        if not ok:
            return resp
        # hide secrets (webhook) from managers
        ctx = _admin_ctx()
        role = ctx.get("role","manager")
        s = dict(ALERT_SETTINGS or {})
        if role != "owner":
            try:
                s2 = json.loads(json.dumps(s))
                if isinstance(s2.get("channels"), dict) and isinstance(s2["channels"].get("slack"), dict):
                    wh = str(s2["channels"]["slack"].get("webhook_url") or "")
                    s2["channels"]["slack"]["webhook_url"] = ("***" if wh else "")
                return jsonify({"ok": True, "settings": s2, "role": role})
            except Exception:
                pass
        return jsonify({"ok": True, "settings": s, "role": role})

    # POST: owner-only
    ok, resp = _require_admin(min_role="owner")
    if not ok:
        return resp
    ctx = _admin_ctx()
    actor = ctx.get("actor","")
    role = ctx.get("role","owner")
    patch = request.get_json(silent=True) or {}
    if not isinstance(patch, dict):
        return jsonify({"ok": False, "error": "Invalid payload"}), 400
    saved = _save_alert_settings_to_disk(patch, actor=actor, role=role)
    _audit("alerts.settings.update", {"enabled": bool(saved.get("enabled"))})
    _notify("alerts.settings.update", {"enabled": bool(saved.get("enabled")), "by": actor, "role": role}, targets=["owner","manager"])
    return jsonify({"ok": True, "settings": saved})

# ============================================================
# Notifications API (in-app notifications for admin/manager)
# ============================================================

@app.route("/admin/api/alerts/test", methods=["POST"])
def admin_api_alert_test():
    ok, resp = _require_admin(min_role="owner")
    if not ok:
        return resp
    ctx = _admin_ctx()
    who = ctx.get("user") or "owner"
    details = f"Test alert triggered by {who} at {datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')}"
    # Use a unique key so tests aren't rate-limited.
    key = "test." + str(int(time.time()))
    res = _dispatch_alert("World Cup Concierge: Test Alert", details, key, severity="warn")
    return jsonify({"ok": True, "result": res})

@app.route("/admin/api/notifications", methods=["GET"])
def admin_api_notifications():
    ok, resp = _require_admin(min_role="manager")
    if not ok:
        return resp
    try:
        ctx = _admin_ctx() or {}
    except Exception:
        ctx = {}
    role = ctx.get("role", "manager")

    # Resolve venue for scoping notifications
    try:
        vid = _venue_id()
    except Exception:
        vid = ""

    try:
        limit = int(request.args.get("limit", 50) or 50)
    except Exception:
        limit = 50
    limit = max(1, min(200, limit))

    # Optional time-range filter (minutes shorthand or raw minutes).
    time_param = str(request.args.get("time", "") or "").strip()
    time_minutes = _parse_time_range_minutes(time_param) if time_param else None
    if time_minutes is None and time_param:
        try:
            time_minutes = int(time_param)
            if time_minutes <= 0:
                time_minutes = None
        except Exception:
            time_minutes = None

    cutoff: Optional[datetime] = None
    if time_minutes and time_minutes > 0:
        cutoff = datetime.now(timezone.utc) - timedelta(minutes=time_minutes)

    items = _read_notifications(
        limit=limit,
        role=role,
        venue_id=(vid or None),
        cutoff=cutoff,
    )
    return jsonify({"ok": True, "role": role, "venue_id": vid, "items": items})

@app.route("/admin/api/notifications/clear", methods=["POST"])
def admin_api_notifications_clear():
    ok, resp = _require_admin(min_role="manager")
    if not ok:
        return resp
    # Scope clear to current venue only; never clear other venues' notifications
    try:
        vid = _venue_id()
    except Exception:
        vid = ""

    try:
        if not os.path.exists(NOTIFICATIONS_FILE):
            _audit("notifications.clear", {"venue_id": vid, "cleared": 0})
            return jsonify({"ok": True})

        kept_lines: List[str] = []
        cleared = 0
        with open(NOTIFICATIONS_FILE, "r", encoding="utf-8") as f:
            for line in f:
                ln = (line or "").strip()
                if not ln:
                    continue
                try:
                    obj = json.loads(ln)
                except Exception:
                    kept_lines.append(line)
                    continue
                v = (obj.get("venue_id") or "").strip()
                # If this entry is for the current venue, drop it; otherwise keep
                if vid and v == vid:
                    cleared += 1
                    continue
                kept_lines.append(line)

        with open(NOTIFICATIONS_FILE, "w", encoding="utf-8") as f:
            for line in kept_lines:
                f.write(line if line.endswith("\n") else (line + "\n"))

        _audit("notifications.clear", {"venue_id": vid, "cleared": cleared})
        return jsonify({"ok": True, "cleared": cleared})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/admin/api/notifications/clear-one", methods=["POST"])
def admin_api_notifications_clear_one():
    """
    Clear a single notification by timestamp.
    - Used by Owner/Manager UI "Clear" button per notification
    - Immediate effect; next poll will not return the cleared entry
    """
    ok, resp = _require_admin(min_role="manager")
    if not ok:
        return resp

    payload = request.get_json(silent=True) or {}
    ts = str(payload.get("ts") or "").strip()
    if not ts:
        return jsonify({"ok": False, "error": "Missing ts"}), 400

    try:
        try:
            vid = _venue_id()
        except Exception:
            vid = ""

        if not os.path.exists(NOTIFICATIONS_FILE):
            return jsonify({"ok": True, "cleared": 0})

        kept_lines: List[str] = []
        cleared = 0
        with open(NOTIFICATIONS_FILE, "r", encoding="utf-8") as f:
            for line in f:
                ln = (line or "").strip()
                if not ln:
                    continue
                try:
                    obj = json.loads(ln)
                except Exception:
                    kept_lines.append(line)
                    continue

                obj_ts = str(obj.get("ts") or "").strip()
                obj_vid = (obj.get("venue_id") or "").strip()

                # Only clear the first matching entry whose venue_id exactly matches.
                # Legacy entries without venue_id are never cleared here to avoid cross-venue effects.
                if (
                    cleared == 0
                    and obj_ts == ts
                    and vid
                    and obj_vid == vid
                ):
                    cleared += 1
                    continue

                kept_lines.append(line)

        with open(NOTIFICATIONS_FILE, "w", encoding="utf-8") as f:
            for line in kept_lines:
                f.write(line if line.endswith("\n") else (line + "\n"))

        try:
            _audit("notifications.clear_one", {"ts": ts, "venue_id": vid, "cleared": cleared})
        except Exception:
            pass

        return jsonify({"ok": True, "cleared": cleared})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/admin/api/presets", methods=["GET"])
def admin_api_presets():
    ok, resp = _require_admin(min_role="manager")
    if not ok:
        return resp
    return jsonify({"ok": True, "presets": list(MATCHDAY_PRESETS.keys())})

@app.route("/admin/api/presets/apply", methods=["POST"])
def admin_api_presets_apply():
    ok, resp = _require_admin(min_role="manager")
    if not ok:
        return resp

    data = request.get_json(silent=True) or {}
    name = (data.get("name") or "").strip()
    preset = MATCHDAY_PRESETS.get(name)
    if not preset:
        return jsonify({"ok": False, "error": "Unknown preset"}), 400

    # Apply Ops (manager allowed). Persist to venue fan_zone first so get_config() sees them (it reads fan_zone before CONFIG_FILE).
    ops = preset.get("ops") or {}
    def _b(v):
        return "true" if bool(v) else "false"
    pairs = {
        "ops_pause_reservations": _b(ops.get("pause_reservations", False)),
        "ops_vip_only": _b(ops.get("vip_only", False)),
        "ops_waitlist_mode": _b(ops.get("waitlist_mode", False)),
    }
    _update_venue_fan_zone(_venue_id(), pairs)
    cfg = set_config(pairs)

    # Apply Rules (owner only)
    rules_patch = preset.get("rules") or {}
    rules_applied = False
    rules_error = ""
    if rules_patch:
        ok2, resp2 = _require_admin(min_role="owner")
        if ok2:
            global BUSINESS_RULES
            BUSINESS_RULES = _deep_merge(BUSINESS_RULES, rules_patch)
            _persist_rules(rules_patch)
            rules_applied = True
        else:
            # Managers are allowed to apply Ops presets, but Rules patches require Owner.
            rules_error = "owner_required"

    _audit("preset.apply", {"name": name, "ops": get_ops(cfg), "rules_patch": rules_patch})
    return jsonify({
        "ok": True,
        "name": name,
        "ops": get_ops(cfg),
        "rules": BUSINESS_RULES,
        "rules_applied": bool(rules_applied),
        "rules_error": rules_error,
    })


@app.route("/admin/api/audit", methods=["GET"])
def admin_api_audit():
    ok, resp = _require_admin(min_role="manager")
    if not ok:
        return resp

    try:
        limit = int(request.args.get("limit", "200") or 200)
        limit = max(1, min(limit, 1000))
    except Exception:
        limit = 200

    # Optional time-range filter (minutes shorthand or raw minutes).
    time_param = str(request.args.get("time", "") or "").strip()
    time_minutes = _parse_time_range_minutes(time_param) if time_param else None
    if time_minutes is None and time_param:
        try:
            time_minutes = int(time_param)
            if time_minutes <= 0:
                time_minutes = None
        except Exception:
            time_minutes = None

    cutoff: Optional[datetime] = None
    if time_minutes and time_minutes > 0:
        cutoff = datetime.now(timezone.utc) - timedelta(minutes=time_minutes)

    def within_timerange(entry: Dict[str, Any]) -> bool:
        if not cutoff:
            return True
        ts_str = str(entry.get("ts") or "").strip()
        dt = _timestamp_to_datetime(ts_str)
        if dt is None:
            return False
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt >= cutoff

    entries: List[Dict[str, Any]] = []

    # ------------------------------------------------------------
    # 1) Redis-first (ONLY return if Redis actually has entries)
    # ------------------------------------------------------------
    try:
        _redis_init_if_needed()
        if globals().get("_REDIS_ENABLED") and globals().get("_REDIS"):
            vid = _venue_id()
            rkey = f"{_REDIS_NS}:{vid}:audit_log"
            read_n = limit
            # For correctness with time filtering, fetch more than `limit`
            # so the cutoff filter doesn't accidentally exclude everything.
            if cutoff:
                read_n = min(max(limit * 8, 500), 2000)
            raw = _REDIS.lrange(rkey, 0, read_n - 1)  # newest first

            unfiltered_entries: List[Dict[str, Any]] = []
            for item in raw or []:
                try:
                    obj = json.loads(item)
                except Exception:
                    continue
                # Extra guard: only accept entries for this venue (if tagged)
                v = (obj.get("venue_id") or "").strip()
                if v and v != vid:
                    continue
                unfiltered_entries.append(obj)

            had_any = bool(unfiltered_entries)
            if cutoff:
                entries = [e for e in unfiltered_entries if within_timerange(e)]
            else:
                entries = unfiltered_entries

            entries = entries[:limit]
            if had_any:  # 🔑 do NOT short-circuit on empty Redis (no data at all)
                return jsonify({"ok": True, "entries": entries, "source": "redis"})
    except Exception:
        pass

    # ------------------------------------------------------------
    # 2) File fallback (dev / legacy behavior)
    # ------------------------------------------------------------
    try:
        if os.path.exists(AUDIT_LOG_FILE):
            with open(AUDIT_LOG_FILE, "r", encoding="utf-8") as f:
                # If time filtering is active, read more than `limit` so we
                # don't accidentally exclude matching entries just because of slicing.
                read_n = limit if not cutoff else max(limit * 5, 2000)
                lines = f.readlines()[-read_n:]
            for ln in lines:
                ln = (ln or "").strip()
                if not ln:
                    continue
                try:
                    obj = json.loads(ln)
                except Exception:
                    continue
                v = (obj.get("venue_id") or "").strip()
                # For file-backed logs, strictly scope by venue_id when present.
                if v and v != vid:
                    continue
                if cutoff and not within_timerange(obj):
                    continue
                entries.append(obj)
    except Exception:
        pass

    # Newest first
    try:
        entries = list(reversed(entries))
    except Exception:
        pass

    return jsonify({"ok": True, "entries": entries, "source": "file"})


@app.route("/admin/api/audit/clear", methods=["POST"])
def admin_api_audit_clear():
    """
    Clear all audit entries for the current venue.
    - Owner-only (audits are sensitive)
    - Clears both Redis-backed list and local file fallback
    """
    ok, resp = _require_admin(min_role="owner")
    if not ok:
        return resp

    cleared = 0
    vid = _venue_id()

    # Redis: delete per-venue audit list
    try:
        _redis_init_if_needed()
        if globals().get("_REDIS_ENABLED") and globals().get("_REDIS"):
            rkey = f"{_REDIS_NS}:{vid}:audit_log"
            try:
                existing = _REDIS.lrange(rkey, 0, -1) or []
                cleared += len(existing)
            except Exception:
                pass
            try:
                _REDIS.delete(rkey)
            except Exception:
                pass
    except Exception:
        pass

    # File fallback: remove only this venue's entries, keep others
    try:
        if os.path.exists(AUDIT_LOG_FILE):
            kept_lines: list[str] = []
            with open(AUDIT_LOG_FILE, "r", encoding="utf-8") as f:
                for line in f:
                    ln = (line or "").strip()
                    if not ln:
                        continue
                    try:
                        obj = json.loads(ln)
                    except Exception:
                        kept_lines.append(line)
                        continue
                    v = (obj.get("venue_id") or "").strip()
                    # Only drop entries for this venue; keep all others
                    if v and v == vid:
                        cleared += 1
                        continue
                    kept_lines.append(line)

            with open(AUDIT_LOG_FILE, "w", encoding="utf-8") as f:
                for line in kept_lines:
                    f.write(line if line.endswith("\n") else (line + "\n"))
    except Exception:
        pass

    try:
        _audit("audit.clear_all", {"cleared": cleared})
    except Exception:
        pass

    return jsonify({"ok": True, "cleared": cleared})


@app.route("/admin/api/audit/clear_one", methods=["POST"])
def admin_api_audit_clear_one():
    """
    Clear a single audit entry by (ts, event, actor).
    - Owner-only
    - Updates both Redis list and file fallback where possible
    """
    ok, resp = _require_admin(min_role="owner")
    if not ok:
        return resp

    payload = request.get_json(silent=True) or {}
    ts = str(payload.get("ts") or "").strip()
    event = str(payload.get("event") or "").strip()
    actor = str(payload.get("actor") or "").strip()
    if not ts or not event:
        return jsonify({"ok": False, "error": "Missing ts or event"}), 400

    vid = _venue_id()
    cleared = 0

    # Redis: rebuild list without the matching entry for this venue
    try:
        _redis_init_if_needed()
        if globals().get("_REDIS_ENABLED") and globals().get("_REDIS"):
            rkey = f"{_REDIS_NS}:{vid}:audit_log"
            raw = _REDIS.lrange(rkey, 0, -1) or []
            kept: list = []
            for blob in raw:
                try:
                    obj = json.loads(blob)
                except Exception:
                    kept.append(blob)
                    continue
                v = (obj.get("venue_id") or "").strip()
                if v and v != vid:
                    kept.append(blob)
                    continue
                if (
                    cleared == 0
                    and str(obj.get("ts") or "").strip() == ts
                    and str(obj.get("event") or "").strip() == event
                    and (not actor or str(obj.get("actor") or "").strip() == actor)
                ):
                    cleared += 1
                    continue
                kept.append(blob)
            try:
                _REDIS.delete(rkey)
            except Exception:
                pass
            if kept:
                try:
                    # Preserve original order: newest first
                    _REDIS.rpush(rkey, *kept)
                except Exception:
                    pass
    except Exception:
        pass

    # File fallback: rewrite without the matching entry for this venue
    try:
        if os.path.exists(AUDIT_LOG_FILE):
            new_lines: list[str] = []
            with open(AUDIT_LOG_FILE, "r", encoding="utf-8") as f:
                for line in f:
                    ln = (line or "").strip()
                    if not ln:
                        continue
                    try:
                        obj = json.loads(ln)
                    except Exception:
                        new_lines.append(line)
                        continue
                    v = (obj.get("venue_id") or "").strip()
                    if v and v != vid:
                        new_lines.append(line)
                        continue
                    if (
                        cleared == 0
                        and str(obj.get("ts") or "").strip() == ts
                        and str(obj.get("event") or "").strip() == event
                        and (not actor or str(obj.get("actor") or "").strip() == actor)
                    ):
                        cleared += 1
                        continue
                    new_lines.append(line)
            with open(AUDIT_LOG_FILE, "w", encoding="utf-8") as f:
                for line in new_lines:
                    f.write(line if line.endswith("\n") else (line + "\n"))
    except Exception:
        pass

    try:
        _audit("audit.clear_one", {"ts": ts, "event": event, "actor": actor, "cleared": cleared})
    except Exception:
        pass

    return jsonify({"ok": True, "cleared": cleared})

# ============================================================
# Partner / Venue Policies API (Hard rules)
# - Managers can read/list
# - Owners can set/delete
# ============================================================
@app.route("/admin/api/partner-policies/list", methods=["GET"])
def admin_api_partner_policies_list():
    ok, resp = _require_admin(min_role="manager")
    if not ok:
        return resp
    try:
        _load_partner_policies_from_disk()
        partners = sorted([k for k in (_PARTNER_POLICIES or {}).keys() if k and k != "default"])
        return jsonify({"ok": True, "partners": partners, "default": _PARTNER_POLICIES.get("default", _default_partner_policy())})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 400

@app.route("/admin/api/partner-policies", methods=["GET"])
def admin_api_partner_policies_get():
    ok, resp = _require_admin(min_role="manager")
    if not ok:
        return resp
    try:
        _load_partner_policies_from_disk()
        partner = (request.args.get("partner") or "").strip() or "default"
        pol = _PARTNER_POLICIES.get(partner) if isinstance(_PARTNER_POLICIES, dict) else None
        if not isinstance(pol, dict):
            pol = _default_partner_policy()
        return jsonify({"ok": True, "partner": partner, "policy": pol})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 400

@app.route("/admin/api/partner-policies/set", methods=["POST"])
def admin_api_partner_policies_set():
    ok, resp = _require_admin(min_role="owner")
    if not ok:
        return resp
    try:
        _load_partner_policies_from_disk()
        body = request.get_json(silent=True) or {}
        partner = (body.get("partner") or "").strip() or "default"
        policy = body.get("policy") or {}
        if not isinstance(policy, dict):
            return jsonify({"ok": False, "error": "Invalid policy payload"}), 400
        if "vip_min_budget" in policy:
            try:
                policy["vip_min_budget"] = int(policy["vip_min_budget"] or 0)
            except Exception:
                policy["vip_min_budget"] = 0
        if "allowed_statuses" in policy and policy["allowed_statuses"] is None:
            policy.pop("allowed_statuses", None)
        merged = _save_partner_policy(partner, policy)
        _audit("partner_policy.save", {"partner": partner, "policy": merged})
        return jsonify({"ok": True, "partner": partner, "policy": merged})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 400

@app.route("/admin/api/partner-policies/delete", methods=["POST"])
def admin_api_partner_policies_delete():
    ok, resp = _require_admin(min_role="owner")
    if not ok:
        return resp
    try:
        _load_partner_policies_from_disk()
        body = request.get_json(silent=True) or {}
        partner = (body.get("partner") or "").strip() or "default"
        if partner == "default":
            return jsonify({"ok": False, "error": "Cannot delete default policy"}), 400
        if isinstance(_PARTNER_POLICIES, dict) and partner in _PARTNER_POLICIES:
            _PARTNER_POLICIES.pop(partner, None)
            _safe_write_json_file(PARTNER_POLICIES_FILE, _PARTNER_POLICIES)
            _audit("partner_policy.delete", {"partner": partner})
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 400

@app.route("/admin/update-lead", methods=["POST"])
def admin_update_lead():
    ok, resp = _require_admin(min_role="manager")
    if not ok:
        return resp

    data = request.get_json(silent=True) or {}
    row_num = int(data.get("row") or data.get("sheet_row") or 0)
    if row_num < 2:
        return jsonify({"ok": False, "error": "Bad row"}), 400

    status = (data.get("status") or "").strip()
    vip = (data.get("vip") or "").strip()

    allowed_status = ["New", "Confirmed", "Seated", "No-Show", "Handled"]
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

    # Use the current venue's worksheet so updates are venue-isolated.
    vid = _venue_id()
    ws = get_sheet(venue_id=vid)
    header = ensure_sheet_schema(ws)
    hmap = header_map(header)

    # Deterministic safety: ensure the target row exists and belongs to current venue.
    row_vals = ws.row_values(row_num) or []
    if not row_vals:
        return jsonify({"ok": False, "error": "Row not found"}), 404
    vcol = hmap.get("venue_id")
    if vcol:
        row_vid = _slugify_venue_id(str((row_vals[vcol - 1] if len(row_vals) >= vcol else "") or DEFAULT_VENUE_ID))
        if row_vid != _slugify_venue_id(vid):
            return jsonify({"ok": False, "error": "Row does not belong to current venue"}), 403

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
        # Also update tier column to keep Segment display in sync
        tier_val = "VIP" if vip == "Yes" else "Regular"
        tier_col = hmap.get("tier")
        if tier_col:
            ws.update_cell(row_num, tier_col, tier_val)
            updates += 1
    # Keep leads view in sync: invalidate venue leads cache after write.
    try:
        _LEADS_CACHE_BY_VENUE.pop(_slugify_venue_id(vid), None)
    except Exception:
        pass
    _audit("lead.handled", {"row": row_num}) if (status == "Handled") else _audit("lead.update", {"row": row_num})
    return jsonify({"ok": True, "updated": updates})


@app.route("/admin/export.csv")
def admin_export_csv():
    ok, resp = _require_admin(min_role="manager")
    if not ok:
        return "Unauthorized", 401

    key = (request.args.get("key","") or "").strip()

    gc = get_gspread_client()
    ws = _open_default_spreadsheet(gc, venue_id=_venue_id()).sheet1
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



@app.get("/admin_tpl")
def admin_tpl():
    # Template-based admin console (Phase 1+)
    ok, resp = _require_admin(min_role="manager")
    if not ok:
        return resp
    ctx = _admin_ctx()
    role = ctx.get("role", "manager")
    is_owner = (role == "owner")
    page_title = ("Owner Admin Console" if is_owner else "Manager Ops Console")
    page_sub = ("Full control — Admin key" if is_owner else "Operations control — Manager key")
    return render_template("admin_console.html",
                           page_title=page_title,
                           page_sub=page_sub,
                           role=role)


@app.get("/admin/drafts")
def admin_drafts_page():
    """Standalone Drafts page: no tabs, fetch on load. Requires owner."""
    ok, resp = _require_admin(min_role="owner")
    if not ok:
        return resp
    key = (request.args.get("key") or "").strip()
    venue = (request.args.get("venue") or "").strip()
    return render_template("admin_drafts.html", key=key, venue=venue)


@app.route("/admin")
def admin():
    """
    Admin Dashboard v1 (Steps 1–3)
    - Tabs: Leads | Rules | Menu
    - Rules config persists to BUSINESS_RULES_FILE
    - Menu upload persists to MENU_FILE and updates /menu.json (fan UI unchanged)
    """
    ok, resp = _require_admin(min_role="manager")
    if not ok:
        return resp

    key = (request.args.get("key", "") or "").strip()

    try:
       ctx = _admin_ctx() or {}
    except Exception:
        ctx = {}
    role = ctx.get("role", "manager")


    # CI-safe guard: never allow admin GET to throw before HTML render
    try:
        pass
    except Exception:
        pass

    # ✅ KEEP THE REST OF YOUR ORIGINAL /admin CODE BELOW THIS LINE
    # (everything that builds `html = []` and ends with `return ...`)

    # Role-based branding (visual only)
    is_owner = (role == "owner")
    page_title = ("Owner Admin Console" if is_owner else "Manager Ops Console")
    page_sub = ("Full control — Admin key" if is_owner else "Operations control — Manager key")

    # Leads (best-effort)
    rows = []
    leads_err = None
    try:
        rows = read_leads(limit=600) or []
        days = int(request.args.get("days") or 0)
        if days in (7, 30):
            rows = _filter_leads_by_days(rows, days)
    except Exception as e:
        leads_err = repr(e)
        rows = []

    header = rows[0] if rows else []
    body = rows[1:] if len(rows) > 1 else []

    def idx(name: str) -> int:
        name = _normalize_header(name)
        for i, h in enumerate(header):
            if _normalize_header(h) == name:
                return i
        return -1

    i_ts = idx("timestamp")
    i_name = idx("name")
    i_phone = idx("phone")
    i_date = idx("date")
    i_time = idx("time")
    i_party = idx("party_size")
    i_lang = idx("language")
    i_status = idx("status")
    i_vip = idx("vip")
    i_entry = idx("entry_point")
    i_tier = idx("tier")
    i_queue = idx("queue")
    i_ctx = idx("business_context")
    i_budget = idx("budget")
    i_notes = idx("notes")
    i_vibe = idx("vibe")

    def colval(r, i, default=""):
        return (r[i] if 0 <= i < len(r) else default).strip() if isinstance(r, list) else default

    # Metrics
    status_counts = {"New": 0, "Confirmed": 0, "Seated": 0, "No-Show": 0}
    vip_count = 0
    for r in body:
        s = colval(r, i_status, "New") or "New"
        status_counts[s] = status_counts.get(s, 0) + 1
        if colval(r, i_vip, "No").lower() in ["yes", "true", "1", "y"]:
            vip_count += 1

    # Render newest first but keep correct sheet row numbers (row 1 header, leads start at 2)
    numbered = [(i + 2, r) for i, r in enumerate(body)]
    numbered = list(reversed(numbered))

    admin_key_q = f"?key={key}&venue={_venue_id()}"
    days_q = ""
    try:
        d0 = int(request.args.get("days") or 0)
        if d0 in (7,30):
            days_q = f"&days={d0}"
    except Exception:
        days_q = ""

    html = []
    html.append("<!doctype html><html><head><meta charset='utf-8'>")
    html.append("<meta name='viewport' content='width=device-width, initial-scale=1'/><meta name='color-scheme' content='dark light'/>")
    html.append(f"<title>{page_title} — World Cup Concierge</title>")
    html.append("\n<div class=\"card\" style=\"margin-top:12px\">\n  <div class=\"row\" style=\"display:flex;gap:10px;flex-wrap:wrap;align-items:center\">\n    <a class=\"btn2\" href=\"/admin?key=__KEY__\">All</a>\n    <a class=\"btn2\" href=\"/admin?key=__KEY__&days=7\">Last 7 days</a>\n    <a class=\"btn2\" href=\"/admin?key=__KEY__&days=30\">Last 30 days</a>\n    <a class=\"btn\" href=\"/admin/api/leads/export?key=__KEY____DAYS__\">Export CSV</a>\n    <span class=\"note\" style=\"margin-left:auto;opacity:.75\">Export matches current filter</span>\n  </div>\n</div>\n".replace('__KEY__', __import__('html').escape(key)).replace('__DAYS__', days_q))
    html.append(r"""
<style>
:root{
  color-scheme:dark;
  --bg:#0b1020;
  --panel:#0f1b33;
  --line:rgba(255,255,255,.10);
  --text:#eaf0ff;
  --muted:#b9c7ee;
  --gold:#d4af37;
  --good:#2ea043;
  --warn:#ffcc66;
  --bad:#ff5d5d;

  /* menu / dropdown tokens */
  --menu-bg:#0f172a;
  --menu-hover:#1e293b;
  --menu-text:#f8fafc;
}

body{
  margin:0;
  font-family:Arial,system-ui,sans-serif;
  background:radial-gradient(900px 700px at 20% 10%, #142a5b 0%, var(--bg) 55%);
  color:var(--text);
}

.wrap{max-width:1600px;margin:0 auto;padding:18px;}
.topbar{display:flex;align-items:flex-start;justify-content:space-between;gap:12px;margin-bottom:12px;}
.h1{font-size:18px;font-weight:800;letter-spacing:.3px}
.sub{color:var(--muted);font-size:12px;margin-top:4px;word-break:break-word;overflow-wrap:break-word}

.pills{display:flex;gap:8px;flex-wrap:wrap;margin-top:8px}
.pill{
  border:1px solid var(--line);
  background:rgba(255,255,255,.03);
  padding:8px 10px;
  border-radius:999px;
  font-size:12px;
  color:var(--text)
}
.pillbtn{cursor:pointer}

.pillselect,
select{
  border:1px solid var(--line);
  background:rgba(255,255,255,.03);
  color:#eaf0ff;
  padding:8px 10px;
  border-radius:999px;
  font-size:12px;
  outline:none;
  z-index:9999;
}

.pillselect option,
select option{
  background:#0f1b33 !important;
  color:#eaf0ff !important;
}

.pillselect option:hover,
select option:hover{
  background:var(--menu-hover) !important;
}

/* iOS Safari native picker fix */
@supports (-webkit-touch-callout: none) {
  html{color-scheme:dark;}
  .pillselect,select{
    color-scheme:dark;
    -webkit-appearance:none;
    appearance:none;
  }
  .pillselect option,select option{
    background:#0f172a;
    color:#ffffff;
  }
}

.pills .pill input[type="checkbox"]{
  transform:translateY(1px);
  margin-left:8px;
}

.pill b{color:var(--gold)}

.tabs{display:flex;gap:10px;flex-wrap:wrap;margin:12px 0 14px}
.tabgroup{
  display:flex;
  align-items:center;
  gap:8px;
  flex-wrap:wrap;
  padding:6px 8px;
  border-radius:14px;
  background:rgba(255,255,255,.04);
  border:1px solid rgba(255,255,255,.07)
}
.tablabel{font-size:11px;letter-spacing:.14em;text-transform:uppercase;opacity:.65;margin-right:4px}
.tabbtn{
  border:1px solid var(--line);
  background:rgba(255,255,255,.03);
  color:var(--text);
  padding:10px 12px;
  border-radius:12px;
  font-size:13px;
  font-weight:700;
  cursor:pointer
}
.tabbtn.active{
  border-color:rgba(212,175,55,.6);
  box-shadow:0 0 0 1px rgba(212,175,55,.25) inset
}
.tabbtn.locked{opacity:.45;cursor:not-allowed}
.tabbtn.locked:hover{transform:none}

.card{
  background:rgba(255,255,255,.04);
  border:1px solid var(--line);
  border-radius:14px;
  padding:16px;
  margin:10px 0;
  overflow:visible;
}

.h2{font-size:14px;font-weight:800;margin:0 0 8px}
.small{font-size:12px;color:var(--muted)}

.tablewrap{
  overflow:auto;
  border-radius:12px;
  border:1px solid var(--line)
}

table{width:100%;border-collapse:collapse;font-size:12px}
th,td{
  padding:10px 8px;
  border-bottom:1px solid rgba(255,255,255,.08);
  vertical-align:top
}
th{
  position:sticky;
  top:0;
  background:rgba(10,16,32,.9);
  text-align:left
}

/* Leads filter area: same dark theme as rest of admin (inherits .card background) */
.leads-filters-section{ background:transparent; }
.leads-dd-wrap{ position:relative; min-width:140px; }
button.inp.leads-dd-btn{ text-align:left; cursor:pointer; display:flex; align-items:center; justify-content:space-between; gap:8px; }
.leads-dd-panel{
  position:absolute; left:0; right:0; top:calc(100% + 4px); z-index:80;
  background:linear-gradient(180deg,rgba(15,26,51,.98),rgba(11,18,32,.99));
  border:1px solid var(--line); border-radius:12px; padding:8px 6px; max-height:240px; overflow-y:auto;
  box-shadow:0 12px 40px rgba(0,0,0,.55);
}
.leads-dd-panel.hidden{ display:none !important; }
.leads-dd-panel label{ display:flex; align-items:center; gap:8px; padding:8px 10px; cursor:pointer; border-radius:8px; font-size:13px; color:var(--text); margin:0; }
.leads-dd-panel label:hover{ background:rgba(255,255,255,.07); }
.leads-dd-panel input{ accent-color:#d4af37; width:16px; height:16px; }
#leadsHoverTip{
  position:fixed; z-index:9999; max-width:min(420px,90vw); padding:12px 14px;
  background:linear-gradient(180deg,rgba(15,26,51,.99),rgba(11,18,32,.99));
  border:1px solid var(--line); border-radius:12px; color:var(--text); font-size:13px; line-height:1.45;
  box-shadow:0 12px 40px rgba(0,0,0,.6); pointer-events:none; white-space:pre-wrap; word-break:break-word;
  display:none;
}

/* Leads table: wide enough to show all columns; horizontal scroll when needed; tooltips on truncated cells */
.leads-tablewrap{
  border-radius:12px;
  border:1px solid var(--line);
  margin-top:8px;
  overflow-x:auto;
  overflow-y:visible;
  min-width:100%;
}
#leadsTable{
  table-layout:fixed;
  min-width:1400px;
  width:100%;
  border-collapse:collapse;
  font-size:12px;
}
#leadsTable th,
#leadsTable td{
  padding:8px 10px;
  border-bottom:1px solid rgba(255,255,255,.08);
  vertical-align:middle;
  box-sizing:border-box;
}
#leadsTable th{ position:sticky; top:0; background:rgba(10,16,32,.95); z-index:1; }
#leadsTable th:nth-child(1),#leadsTable td:nth-child(1){ width:3%; min-width:36px; }
#leadsTable th:nth-child(2),#leadsTable td:nth-child(2){ width:10%; min-width:100px; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
#leadsTable th:nth-child(3),#leadsTable td:nth-child(3){ width:7%; min-width:80px; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
#leadsTable th:nth-child(4),#leadsTable td:nth-child(4){ width:9%; min-width:90px; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
#leadsTable th:nth-child(5),#leadsTable td:nth-child(5){ width:5%; min-width:64px; }
#leadsTable th:nth-child(6),#leadsTable td:nth-child(6){ width:5%; min-width:56px; }
#leadsTable th:nth-child(7),#leadsTable td:nth-child(7){ width:4%; min-width:48px; text-align:center; }
#leadsTable th:nth-child(8),#leadsTable td:nth-child(8){ width:5%; min-width:64px; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
#leadsTable th:nth-child(9),#leadsTable td:nth-child(9){ width:6%; min-width:80px; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
#leadsTable th:nth-child(10),#leadsTable td:nth-child(10){ width:5%; min-width:64px; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
#leadsTable th:nth-child(11),#leadsTable td:nth-child(11){ width:5%; min-width:56px; }
#leadsTable th:nth-child(12),#leadsTable td:nth-child(12){ width:8%; min-width:80px; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
#leadsTable th:nth-child(13),#leadsTable td:nth-child(13){ width:9%; min-width:90px; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
#leadsTable th:nth-child(14),#leadsTable td:nth-child(14){ width:9%; min-width:90px; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
#leadsTable th:nth-child(15),#leadsTable td:nth-child(15){ width:5%; min-width:64px; }
#leadsTable th:nth-child(16),#leadsTable td:nth-child(16){ width:6%; min-width:100px; white-space:nowrap; }
#leadsTable td:nth-child(16) .btn2{ margin-right:4px; }
#leadsTable td[title]{ cursor:help; }
#leadsTable td:nth-child(14) select,
#leadsTable td:nth-child(15) select{
  width:100%;
  min-width:0;
  box-sizing:border-box;
  font-size:12px;
}

.badge{
  display:inline-block;
  padding:4px 8px;
  border-radius:999px;
  border:1px solid var(--line);
  background:rgba(255,255,255,.03);
  font-size:11px;
  color:var(--text)
}
.badge.good{border-color:rgba(46,160,67,.35)}
.badge.warn{border-color:rgba(255,204,102,.35)}
.badge.bad{border-color:rgba(255,93,93,.35)}

.inp,textarea,select{
  width:100%;
  box-sizing:border-box;
  background:rgba(255,255,255,.04);
  border:1px solid var(--line);
  color:#eaf0ff;
  padding:10px;
  border-radius:12px;
  font-size:13px;
  outline:none
}
.inp::placeholder,
textarea::placeholder{
  color:rgba(234,240,255,.5);
}
.inp::-webkit-input-placeholder,
textarea::-webkit-input-placeholder{
  color:rgba(234,240,255,.5);
}
.inp:-moz-placeholder,
textarea:-moz-placeholder{
  color:rgba(234,240,255,.5);
}
.card .inp + .inp,
.grid2 .inp + .inp{
  margin-top:8px;
}

/* label / helper sitting right above an input — breathing room */
label.small + .inp,
label.small + select,
label.small + textarea,
.small + .inp,
.small + select,
.small + textarea{
  margin-top:8px;
}

/* two-column form grid (alerts, policies, etc.) */
.grid2{
  display:grid;
  grid-template-columns:1fr 1fr;
  gap:16px 20px;
}
.grid2 > div{
  display:flex;
  flex-direction:column;
  gap:8px;
}
.grid2 > div > label.small{margin-bottom:0}
@media(max-width:900px){.grid2{grid-template-columns:1fr}}

.row{display:grid;grid-template-columns:1fr 1fr;gap:10px}
@media(max-width:900px){.row{grid-template-columns:1fr}}

.btn{
  border:1px solid rgba(212,175,55,.45);
  background:rgba(212,175,55,.12);
  color:var(--text);
  padding:10px 12px;
  border-radius:12px;
  font-size:13px;
  font-weight:800;
  cursor:pointer
}
.btn2{
  border:1px solid var(--line);
  background:rgba(255,255,255,.03);
  color:var(--text);
  padding:10px 12px;
  border-radius:12px;
  font-size:13px;
  font-weight:700;
  cursor:pointer
}
.btnTiny{
  margin-left:6px;
  min-width:32px;
  height:32px;
  line-height:30px;
  padding:0 8px;
  border-radius:10px;
  border:1px solid rgba(255,255,255,.10);
  background:rgba(255,255,255,.06);
  color:var(--text);
  cursor:pointer
}
.btnTiny:hover{background:rgba(255,255,255,.10)}

.modal-overlay{
  display:none;
  position:fixed;
  top:0;left:0;right:0;bottom:0;
  background:rgba(0,0,0,.75);
  z-index:9999;
  align-items:center;
  justify-content:center;
  animation:fadeIn .2s ease
}
.modal-overlay.show{display:flex}
.modal-box{
  background:linear-gradient(180deg, rgba(15,27,51,.98), rgba(11,16,32,.98));
  border:1px solid rgba(255,255,255,.12);
  border-radius:18px;
  padding:20px;
  max-width:700px;
  width:90%;
  max-height:85vh;
  overflow:auto;
  box-shadow:0 12px 40px rgba(0,0,0,.5);
  animation:slideUp .25s ease
}
.modal-header{
  display:flex;
  justify-content:space-between;
  align-items:center;
  margin-bottom:14px
}
.modal-title{font-size:18px;font-weight:800}
.modal-close{
  background:rgba(255,255,255,.06);
  border:1px solid rgba(255,255,255,.12);
  color:var(--text);
  border-radius:8px;
  padding:6px 12px;
  cursor:pointer;
  font-size:13px;
  font-weight:700
}
.modal-close:hover{background:rgba(255,255,255,.12)}
@keyframes fadeIn{from{opacity:0}to{opacity:1}}
@keyframes slideUp{from{opacity:0;transform:translateY(20px)}to{opacity:1;transform:translateY(0)}}

.note{margin-top:8px;font-size:12px;color:var(--muted)}
.hidden{display:none}
.locked{opacity:.45;filter:saturate(.7);cursor:not-allowed}
.locked::after{content:'⛔ No permission';margin-left:6px;font-size:12px;opacity:.8}

.code{
  font-family:ui-monospace,SFMono-Regular,Menlo,Monaco,Consolas,"Liberation Mono","Courier New",monospace;
  font-size:12px
}

.miniState{
  margin-left:10px;
  padding:2px 8px;
  border-radius:999px;
  border:1px solid rgba(255,255,255,.14);
  background:rgba(255,255,255,.04);
  font-size:12px;
  letter-spacing:.01em;
  opacity:0;
  transition:opacity .18s ease;
}
</style>
""")
    html.append("</head><body><div class='wrap'>")

    html.append("<div class='topbar'>")
    html.append("<div>")
    html.append(f"<div class='h1'>{page_title}</div>")
    html.append(f"<div class='sub'>Tabs + Rules Config + Menu Upload (fan UI unchanged) · {page_sub}</div>")
    html.append("<div class='pills'>")
    html.append(f"<span class='pill'><b>Ops</b> {len(body)}</span>")
    html.append(f"<span class='pill'><b>VIP</b> {vip_count}</span>")
    html.append("<button class='pill' id='notifBtn' type='button' onclick=\"openNotifications()\">🔔 <b id='notifCount'>0</b></button>")

    html.append("<button class='pill pillbtn' id='refreshBtn' type='button' onclick=\"refreshAll('manual')\">↻ <b>Refresh</b></button>")
    html.append("<button class='pill pillbtn' id='autoBtn' type='button' onclick=\"toggleAutoRefresh()\">⟳ <b id='autoLabel'>Auto: Off</b></button>")
    html.append("<select class='pillselect' id='autoEvery' onchange=\"autoEveryChanged()\"><option value='10'>10s</option><option value='30' selected>30s</option><option value='60'>60s</option></select>")
    html.append("<span class='pill' id='lastRef'>Last refresh: —</span>")
    for k, v in status_counts.items():
        html.append(f"<span class='pill'><b>{k}</b> {v}</span>")
    html.append("</div>")
    html.append("</div>")
    html.append("<div style='text-align:right'>")
    html.append(f"<div class='small'>Admin key: <span class='code'>••••••</span></div>")
    html.append(
    "<div style='margin-top:8px;display:flex;gap:8px;justify-content:flex-end;flex-wrap:wrap'>"
    f"<a class='btn2' style='text-decoration:none' href='/admin/export.csv{admin_key_q}'>Export CSV</a>"
    "<a class='btn2' style='text-decoration:none' href='#fanzone' "
    "onclick=\"showTab('fanzone');return false;\">Fan Zone</a>"
    "</div>"
)
    html.append("</div>")
    html.append("</div>")  # topbar

    html.append(r"""
<div class="tabs">
  <div class="tabgroup">
    <span class="tablabel">Operate</span>
    <button type="button" class="tabbtn active" data-tab="ops" onclick="showTab('ops');return false;">Ops</button>
    <button type="button" class="tabbtn" data-tab="leads" onclick="showTab('leads');return false;">Leads</button>
    <button type="button" class="tabbtn" data-tab="aiq" onclick="showTab('aiq');return false;">AI Queue</button>
    <button type="button" class="tabbtn" data-tab="monitor" onclick="showTab('monitor');return false;">Monitoring</button>
    <button type="button" class="tabbtn" data-tab="audit" onclick="showTab('audit');return false;">Audit</button>
  </div>
  <div class="tabgroup">
    <span class="tablabel">Configure</span>
    <button type="button" class="tabbtn" data-tab="ai" data-minrole="owner" onclick="showTab('ai');return false;">AI Settings</button>
    <button type="button" class="tabbtn" data-tab="rules" data-minrole="owner" onclick="showTab('rules');return false;">Rules</button>
    <button type="button" class="tabbtn" data-tab="menu" data-minrole="owner" onclick="showTab('menu');return false;">Menu</button>
    <button type="button" class="tabbtn" data-minrole="owner" onclick="showDraftsModal();return false;">Drafts</button>
    <button type="button" class="tabbtn" data-tab="policies" data-minrole="owner" onclick="showTab('policies');return false;">Policies</button>
  </div>
</div>

<!-- OPS TAB -->
<div id="tab-ops" class="tabpane">
  <div class="card" id="ops-controls">
    <div class="h2">Ops</div>
    <div class="small">Fast operational controls (audited).</div>
    <div class="small" id="ops-meta" style="margin-top:6px;opacity:.85"></div>

    <div style="margin-top:12px;display:flex;flex-direction:column;gap:10px">
      <label class="small" style="display:flex;align-items:center;gap:10px">
        <input type="checkbox" id="ops-pause" onchange="saveOps()">
        <span><b>Pause Reservations</b></span>
        <span id="mini-pause" class="note" style="margin-left:auto;opacity:0;transition:opacity .18s"></span>
      </label>

      <label class="small" style="display:flex;align-items:center;gap:10px">
        <input type="checkbox" id="ops-viponly" onchange="saveOps()">
        <span><b>VIP Only</b></span>
        <span id="mini-vip" class="note" style="margin-left:auto;opacity:0;transition:opacity .18s"></span>
      </label>

      <label class="small" style="display:flex;align-items:center;gap:10px">
        <input type="checkbox" id="ops-waitlist" onchange="saveOps()">
        <span><b>Waitlist Mode</b> <span style="opacity:.75">(AI Waiting List)</span></span>
        <span id="mini-wait" class="note" style="margin-left:auto;opacity:0;transition:opacity .18s"></span>
      </label>
    </div>

    <div id="ops-msg" class="note" style="margin-top:10px"></div>
    <div class="small" style="margin-top:10px;opacity:.72">
      Tip: toggles auto-save (“Saving…” → “Saved”).
    </div>
  </div>

  <div class="card" id="matchdayCard">
    <div class="h2">Match Day Ops</div>
    <div class="small">One-click presets that set multiple Ops toggles + key Rules. Audited.</div>
    <div style="margin-top:12px;display:flex;gap:10px;flex-wrap:wrap">
      <button type="button" class="btn2" onclick="applyPreset('Kickoff Rush')">Kickoff Rush</button>
      <button type="button" class="btn2" onclick="applyPreset('Halftime Surge')">Halftime Surge</button>
      <button type="button" class="btn2" onclick="applyPreset('Post-game')">Post-game</button>
    </div>
    <div id="preset-msg" class="note" style="margin-top:10px"></div>
    <div class="small" style="margin-top:10px;opacity:.72">
      Tip: presets update Ops + Rules instantly so staff can shift modes fast.
    </div>
  </div>

  <div class="card" id="notifCard">
    <div style="display:flex;align-items:center;justify-content:space-between;gap:10px;flex-wrap:wrap">
      <div class="h2" style="margin:0">Notifications</div>
      <button type="button" class="btn2" style="margin-left:auto" onclick="clearAllNotifs()">Clear all</button>
    </div>
    <div style="margin-top:10px;display:flex;gap:10px;align-items:center;flex-wrap:wrap">
      <span class="small">Time range</span>
      <select class="inp" id="notif-time" style="width:220px">
        <option value="">All time</option>
        <option value="30">Last 30 minutes</option>
        <option value="60">Last 1 hour</option>
        <option value="120">Last 2 hours</option>
        <option value="1440">Last 24 hours</option>
        <option value="10080">Last 7 days</option>
      </select>
    </div>
    <div id="notifBody" class="small" style="margin-top:8px"></div>
    <div id="notif-msg" class="note" style="margin-top:8px"></div>
  </div>
</div>

<!-- LEADS TAB -->

<div id="tab-monitor" class="tabpane hidden">
  <div class="card" id="healthCard">
    <div class="h2">System Health</div>
    <div class="small">Read-only checks for Sheets, Queue, Fixtures, and outbound readiness. Alerts are optional.</div>
    <div style="margin-top:12px;display:flex;gap:10px;flex-wrap:wrap;align-items:center">
      <button type="button" class="btn" onclick="runHealth()">Run checks</button>
      <button type="button" class="btn2" onclick="loadHealth()">Refresh</button>
      <span id="health-msg" class="note"></span>
      <span style="margin-left:auto" class="note" id="health-ts"></span>
    </div>
    <div id="health-body" class="small" style="margin-top:12px;white-space:pre-wrap"></div>
  </div>
  <div class="card" id="forecastCard">
  <div class="h2">Tonight Forecast</div>
  <div class="small">Read-only load forecast (last 7/30 days). Helps staffing + VIP readiness.</div>
  <div style="margin-top:10px;display:flex;gap:10px;flex-wrap:wrap;align-items:center">
    <button class="btn2" type="button" onclick="loadForecast()">Refresh</button>
    <span id="forecast-msg" class="note"></span>
  </div>
  <div id="forecastBody" class="small" style="margin-top:10px;line-height:1.4"></div>
</div>
  <div class="card" id="alertsCard">
    <div class="h2">Alerts Settings</div>
    <div class="small">Configure monitoring alerts (Slack/Email/SMS). Alerts are rate-limited and best-effort (never crash the app).</div>

    <div style="display:grid;grid-template-columns:1fr 1fr;gap:16px 20px;margin-top:14px">
      <div style="display:flex;flex-direction:column;gap:0">
        <label class="small" style="display:flex;gap:8px;align-items:center">
          <input id="al-enabled" type="checkbox"/>
          <span>Enable alerts</span>
        </label>
        <div class="small" style="opacity:.8;margin-top:8px">When enabled, <b>Run checks</b> can emit alerts on failures (rate-limited).</div>
      </div>
      <div style="display:flex;flex-direction:column;gap:0">
        <label class="small" style="display:block;margin-bottom:8px">Rate limit (seconds)</label>
        <input id="al-rate" class="inp" type="number" min="60" step="60" placeholder="600"/>
      </div>
    </div>

    <div style="display:grid;grid-template-columns:1fr 1fr;gap:16px 20px;margin-top:14px">
      <div style="display:flex;flex-direction:column;gap:0">
        <label class="small" style="display:flex;gap:8px;align-items:center">
          <input id="al-slack-en" type="checkbox"/>
          <span>Slack</span>
        </label>
        <input id="al-slack-url" class="inp" placeholder="Slack webhook URL" style="margin-top:8px"/>
      </div>
      <div style="display:flex;flex-direction:column;gap:0">
        <label class="small" style="display:flex;gap:8px;align-items:center">
          <input id="al-email-en" type="checkbox"/>
          <span>Email</span>
        </label>
        <input id="al-email-to" class="inp" placeholder="Alert email TO" style="margin-top:8px"/>
        <input id="al-email-from" class="inp" placeholder="Alert email FROM (optional)" style="margin-top:10px"/>
      </div>
    </div>

    <div style="display:grid;grid-template-columns:1fr 1fr;gap:16px 20px;margin-top:14px">
      <div style="display:flex;flex-direction:column;gap:0">
        <label class="small" style="display:flex;gap:8px;align-items:center">
          <input id="al-sms-en" type="checkbox"/>
          <span>SMS (critical only)</span>
        </label>
        <input id="al-sms-to" class="inp" placeholder="Alert SMS TO (E.164)" style="margin-top:8px"/>
        <div class="small" style="opacity:.75;margin-top:8px">SMS only sends on <b>error</b> severity alerts (to prevent spam).</div>
      </div>
      <div style="display:flex;flex-direction:column;gap:0">
        <label class="small" style="display:block;margin-bottom:8px">Fixtures stale threshold (seconds)</label>
        <input id="al-fixtures-stale" class="inp" type="number" min="3600" step="3600" placeholder="86400"/>
      </div>
    </div>

    <div style="margin-top:14px;display:flex;gap:10px;flex-wrap:wrap;align-items:center">
      <button type="button" class="btn2" onclick="loadAlerts()">Load</button>
      <button type="button" class="btn" data-min-role="owner" onclick="saveAlerts()">Save</button>
      <button type="button" class="btn2" data-min-role="owner" onclick="testAlert()">Send test alert</button>
      <span id="al-msg" class="note"></span>
    </div>
  </div>
</div>

<div id="tab-leads" class="tabpane hidden">
""")

    # Leads table
    if leads_err:
        html.append(f"<div class='card'><div class='h2'>Leads</div><div class='small'>Error reading leads: {leads_err}</div></div>")
    elif not body:
        html.append("<div class='card'><div class='h2'>Leads</div><div class='small'>No leads yet.</div></div>")
    else:
        html.append("""<div class='card'><div class='h2'>Leads</div><div class='small'>Newest first. Update Status/VIP and save.</div>
<div class='leads-filters-section' style='margin-top:16px;padding:14px;border-radius:8px;border:1px solid var(--line)'>
  <div class='small' style='font-weight:700;margin-bottom:12px;color:var(--text)'>Filter leads</div>
  <div style='display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));gap:12px;align-items:end'>
    <div class='leads-dd-wrap'><label class='small' style='display:block;margin-bottom:4px;font-weight:600'>Status</label><button type='button' class='inp leads-dd-btn' id='flt-status-btn' aria-expanded='false'>All statuses <span style='opacity:.6'>▾</span></button><div class='leads-dd-panel hidden' id='flt-status-panel'>
<label><input type='checkbox' value='new'> New</label><label><input type='checkbox' value='contacted'> Contacted</label><label><input type='checkbox' value='reserved'> Reserved</label><label><input type='checkbox' value='seated'> Seated</label><label><input type='checkbox' value='completed'> Completed</label><label><input type='checkbox' value='no-show'> No-Show</label><label><input type='checkbox' value='cancelled'> Cancelled</label><label><input type='checkbox' value='waitlist'> Waitlist</label><label><input type='checkbox' value='confirmed'> Confirmed</label><label><input type='checkbox' value='handled'> Handled</label>
</div></div>
    <div class='leads-dd-wrap'><label class='small' style='display:block;margin-bottom:4px;font-weight:600'>Tier</label><button type='button' class='inp leads-dd-btn' id='flt-tier-btn' aria-expanded='false'>All tiers <span style='opacity:.6'>▾</span></button><div class='leads-dd-panel hidden' id='flt-tier-panel'>
<label><input type='checkbox' value='regular'> Regular</label><label><input type='checkbox' value='entry'> Entry</label><label><input type='checkbox' value='reserve now'> Reserve now</label><label><input type='checkbox' value='vip'> VIP</label><label><input type='checkbox' value='vip vibe'> VIP vibe</label><label><input type='checkbox' value='premium'> Premium</label>
</div></div>
    <div><label class='small' style='display:block;margin-bottom:4px;font-weight:600'>Time range</label><select class='inp' id='flt-time' style='min-width:140px'><option value=''>All time</option><option value='30'>Last 30 min</option><option value='60'>Last 1 hour</option><option value='120'>Last 2 hours</option><option value='1440'>Last 24 hours</option><option value='10080'>Last 7 days</option></select></div>
    <div><label class='small' style='display:block;margin-bottom:4px;font-weight:600'>Source</label><select class='inp' id='flt-entry' style='min-width:140px'><option value='all'>All sources</option></select></div>
    <div style='display:flex;gap:8px;flex-wrap:wrap;align-items:flex-end'><button class='btn' id='btn-leads-apply' type='button'>Apply</button><button class='btn2' id='btn-leads-reset' type='button'>Reset</button></div>
  </div>
  <div style='margin-top:10px'><span id='leadsCount' class='small'>0 shown</span></div>
</div>
</div>""")
        html.append("<div class='tablewrap leads-tablewrap'><table id='leadsTable'>")
        html.append("<thead><tr>"                    "<th>Row</th><th>Timestamp</th><th>Name</th><th>Contact</th>"                    "<th>Date</th><th>Time</th><th>Party</th>"                    "<th>Segment</th><th>Entry</th><th>Queue</th><th>Budget</th>"                    "<th>Context</th><th>Notes</th>"                    "<th>Status</th><th>VIP</th><th>Save</th>"                    "</tr></thead><tbody id='leadsTableBody'>")
        from urllib.parse import quote as _urlq
        def _tip_td(txt, short_min=8):
            t = (txt or "").strip()
            if len(t) < short_min:
                return ""
            return ' class="leads-cell-tip" data-tip="' + _urlq(t, safe="") + '"'
        for sheet_row, r in numbered:
            ts = colval(r, i_ts, "")
            nm = colval(r, i_name, "")
            ph = colval(r, i_phone, "")
            d = colval(r, i_date, "")
            t = colval(r, i_time, "")
            ps = colval(r, i_party, "")
            lg = colval(r, i_lang, "en")
            st = colval(r, i_status, "New") or "New"
            vip = colval(r, i_vip, "No") or "No"
            ep = colval(r, i_entry, "")
            tier = colval(r, i_tier, "")
            # Canonical VIP detection (tier and vip must stay in sync for UI)
            tier_s = str(tier or "").strip().lower()
            vip_s = str(vip or "").strip().lower()
            is_vip = (("vip" in tier_s) or (vip_s in ["yes","true","1","y","vip"]))
            tier_key = "vip" if is_vip else "regular"
            queue = colval(r, i_queue, "")
            bctx = colval(r, i_ctx, "")
            budget = colval(r, i_budget, "")
            notes = colval(r, i_notes, "")
            vibe = colval(r, i_vibe, "")

            def opt(selected, label):
                sel = " selected" if selected else ""
                return f"<option value=\"{_hesc(label)}\"{sel}>{_hesc(label)}</option>"

            html.append(f"<tr data-tier='{_hesc(tier_key)}' data-entry='{_hesc(ep)}'>")
            html.append(f"<td class='code'>{sheet_row}</td>")
            html.append(f"<td>{ts}</td>")
            html.append(f"<td>{nm}</td>")
            html.append(f"<td>{ph}</td>")
            html.append(f"<td>{d}</td>")
            html.append(f"<td>{t}</td>")
            html.append(f"<td>{ps}</td>")
            # Segment badge (VIP vs Regular)
            seg = "⭐ VIP" if tier_key == "vip" else "Regular"
            seg_cls = "badge warn" if seg.startswith("⭐") else "badge"
            html.append("<td" + _tip_td(seg, 4) + "><span class='" + seg_cls + "'>" + _hesc(seg) + "</span></td>")
            html.append("<td" + _tip_td(ep, 1) + "><span class='pill'>" + _hesc(ep or "—") + "</span></td>")
            html.append("<td" + _tip_td(queue, 4) + "><span class='badge good'>" + _hesc(queue or "—") + "</span></td>")
            html.append(f"<td>{_hesc(budget)}</td>")
            # Context + Notes (compact); title on td for hover tooltip when truncated
            ctx_txt = (bctx or "").strip()
            note_txt = (notes or "").strip()
            if vibe and vibe.strip():
                note_txt = (note_txt + (" | " if note_txt else "") + f"vibe: {vibe.strip()}").strip()
            def _cell_details(label, txt):
                if not txt:
                    return "<span class='small'>—</span>"
                short = txt if len(txt) <= 34 else (txt[:34] + "…")
                return "<details><summary class='small'>" + _hesc(short) + "</summary><div style='margin-top:6px;white-space:pre-wrap' class='small'>" + _hesc(txt) + "</div></details>"
            _ctx_tip = _tip_td(ctx_txt, 1) if ctx_txt else ""
            _note_tip = _tip_td(note_txt, 1) if note_txt else ""
            html.append("<td" + _ctx_tip + ">" + _cell_details("context", ctx_txt) + "</td>")
            html.append("<td" + _note_tip + ">" + _cell_details("notes", note_txt) + "</td>")


            html.append("<td>")
            html.append(f"<select class='inp' id='status-{sheet_row}'>"
                        f"{opt(st=='New','New')}{opt(st=='Confirmed','Confirmed')}{opt(st=='Seated','Seated')}{opt(st=='No-Show','No-Show')}{opt(st=='Handled','Handled')}"
                        "</select>")
            html.append("</td>")

            html.append("<td>")
            html.append(f"<select class='inp' id='vip-{sheet_row}'>"
                        f"{opt(is_vip, 'Yes')}{opt(not is_vip, 'No')}"
                        "</select>")
            html.append("</td>")


            html.append("<td>")
            html.append(f"<button class='btn primary' type='button' onclick='saveLead({sheet_row})'>Save</button> <button class='btnTiny' type='button' title='Set status to Handled' onclick='markHandled({sheet_row})'>Handled</button>")
            html.append("</td>")

            html.append("</tr>")
        html.append("</tbody></table></div>")

    html.append("</div>")  # tab-leads

    html.append(r"""

<div id="tab-ai" class="tabpane hidden">
  <div class="card">
    <div class="h2">AI Automation</div>
    <div class="small">Managers can enable/disable AI and choose how much autonomy it has. Owners can tune advanced behavior.</div>
  </div>

  <div class="row">
    <div class="card" style="margin:0">
      <div class="h2" style="margin-bottom:6px">Runtime controls</div>

      <label class="small" style="display:flex;gap:10px;align-items:center">
        <input type="checkbox" id="ai-enabled">
        <span><b>Enable AI</b> (master switch)</span>
      </label>

      <div style="margin-top:10px">
        <label class="small">Mode</label>
        <select class="inp" id="ai-mode">
          <option value="off">Off</option>
          <option value="suggest">Suggest (recommended)</option>
          <option value="auto">Auto (guardrails)</option>
        </select>
        <div class="note">Suggest = AI drafts/recommends; humans approve. Auto = AI can apply actions (still gated).</div>
      </div>

      <div style="margin-top:10px">
        <label class="small" style="display:flex;gap:10px;align-items:center">
          <input type="checkbox" id="ai-approval">
          <span><b>Require approval</b> (recommended)</span>
        </label>
      </div>

      <div style="margin-top:10px">
        <label class="small">Min confidence (0–1)</label>
        <input class="inp" id="ai-minconf" type="number" min="0" max="1" step="0.05" />
      </div>

      <div style="margin-top:12px;display:flex;gap:10px;flex-wrap:wrap">
        <button type="button" class="btn" onclick="saveAI()">Save AI Settings</button>
        <button class="btn2" onclick="loadAI()">Reload</button>
        <span id="ai-msg" class="note"></span>
      </div>
    </div>

    <div class="card" style="margin:0">
      <div class="h2" style="margin-bottom:6px">Owner settings</div>
      <div class="small">Only Owners can edit these fields.</div>

      <div style="margin-top:10px">
        <label class="small">Model</label>
        <input class="inp" id="ai-model" placeholder="gpt-4o-mini"/>
      </div>

      <div style="margin-top:10px">
        <label class="small">System prompt</label>
        <textarea class="inp" id="ai-prompt" rows="6"></textarea>
      </div>

      <div style="margin-top:10px">
        <div class="small" style="font-weight:800;margin-bottom:6px">Allowed actions</div>
        <label class="small"><input type="checkbox" id="ai-act-vip"> VIP tagging</label><br/>
        <label class="small"><input type="checkbox" id="ai-act-status"> Status updates</label><br/>
        <label class="small"><input type="checkbox" id="ai-act-draft"> Reply drafts</label><br/>
        <label class="small"><input type="checkbox" id="ai-act-sms"> Send SMS</label>
        <label class="small"><input type="checkbox" id="ai-act-email" style="margin-left:12px"> Send Email</label>
        <label class="small"><input type="checkbox" id="ai-act-whatsapp" style="margin-left:12px"> Send WhatsApp</label>
      </div>

      <div class="note">Tip: keep actions limited until you trust the workflow.</div>
    </div>
  </div>
</div>

  <div class="card" style="margin-top:14px">
    <div class="h2">AI Replay (Read-only)</div>
    <div class="small">Owner tool to re-run AI suggestions for a specific lead row without applying changes.</div>
    <div style="margin-top:10px;display:flex;gap:10px;flex-wrap:wrap;align-items:center">
      <input id="replay-row" class="pillselect" type="number" min="2" style="min-width:120px" placeholder="Row #" />
      <button class="btn2" type="button" onclick="replayAI()">Replay</button>
      <span id="replay-msg" class="note"></span>
    </div>
    <pre id="replayOut" style="margin-top:10px;white-space:pre-wrap;background:rgba(255,255,255,.04);border:1px solid rgba(255,255,255,.10);border-radius:12px;padding:10px;max-height:260px;overflow:auto"></pre>
  </div>

<div id="tab-aiq" class="tabpane hidden">
  <div class="card">
    <div style="display:flex;align-items:center;justify-content:space-between;gap:10px;flex-wrap:wrap">
      <div>
        <div class="h2" style="margin:0">AI Approval Queue</div>
        <div class="small">Proposed AI actions wait here for <b>Approve</b>, <b>Deny</b>, or <b>Owner Override</b>. This keeps automation powerful but controlled.</div>
      </div>
      <button class="btn2" type="button" onclick="clearAIQueue()" style="margin-left:auto">Clear queue</button>
    </div>
    <div style="margin-top:10px;display:flex;gap:10px;flex-wrap:wrap;align-items:center">
      <button class="btn2" onclick="loadAIQueue()">Refresh</button>
      <span class="note" style="opacity:.6">|</span>
      <input id="ai-run-limit" class="inp" type="number" min="1" max="25" step="1" value="5" style="max-width:90px" title="How many newest New leads to analyze" />
      <button class="btn2" onclick="runAINew()">Run AI (New)</button>
      <input id="ai-run-row" class="inp" type="number" min="2" step="1" placeholder="Row #" style="max-width:110px" title="Run AI for a specific Google Sheet row number" />
      <button class="btn2" onclick="runAIRow()">Run AI (Row)</button>
      <span class="note" style="font-size:11px">Tip: model often proposes 0 actions for Handled or non-New leads.</span>

      <select id="aiq-filter" class="inp" style="max-width:180px" onchange="loadAIQueue()">
        <option value="">All</option>
        <option value="pending">Pending</option>
        <option value="approved">Approved</option>
      </select>
      <select id="aiq-time" class="inp" style="max-width:190px" onchange="loadAIQueue()">
        <option value="">All time</option>
        <option value="30">Last 30 minutes</option>
        <option value="60">Last 1 hour</option>
        <option value="120">Last 2 hours</option>
        <option value="1440">Last 24 hours</option>
        <option value="10080">Last 7 days</option>
      </select>
      <select id="aiq-type" class="inp" style="max-width:190px" onchange="loadAIQueue()">
        <option value="">All types</option>
        <option value="reply_draft">Reply drafts</option>
        <option value="vip_tag">VIP tagging</option>
        <option value="status_update">Status updates</option>
        <option value="send_sms">Send SMS</option>
        <option value="send_email">Send Email</option>
        <option value="send_whatsapp">Send WhatsApp</option>
        <option value="send_confirmation">Send confirmation</option>
        <option value="send_reservation_received">Send reservation received</option>
        <option value="send_update">Send update</option>
        <option value="send_vip_update">Send VIP update</option>
      </select>
      <select id="aiq-conf" class="inp" style="max-width:200px" onchange="loadAIQueue()">
        <option value="">Any confidence</option>
        <option value="0.50">Min 0.50</option>
        <option value="0.70">Min 0.70</option>
        <option value="0.80">Min 0.80</option>
      </select>
      <input id="aiq-search" class="inp" style="min-width:220px" placeholder="Search: phone, reservation_id, draft…" oninput="aiqSearchDebounced()" />
      <button class="btn2" type="button" onclick="clearAIQueueFilters()" title="Reset all AI queue filters">Clear filters</button>
      <span id="aiq-msg" class="note"></span>
    </div>
  </div>

  <div class="card">
    <div class="h2" style="margin-bottom:8px">Compose message (queue outbound)</div>
    <div class="small" style="margin-bottom:10px">Add an SMS, Email, or WhatsApp message to the queue. Leave message empty to use draft template (set Row to fill placeholders from the sheet).</div>
    <div style="display:flex;flex-wrap:wrap;gap:10px;align-items:flex-end">
      <div>
        <label class="small">Channel</label>
        <select id="ob-channel" class="inp" style="min-width:100px" onchange="var w=document.getElementById('ob-subject-wrap');var v=document.getElementById('ob-channel').value;if(w)w.classList.toggle('hidden',v!=='email');">
          <option value="sms">SMS</option>
          <option value="email">Email</option>
          <option value="whatsapp">WhatsApp</option>
        </select>
      </div>
      <div>
        <label class="small">To (phone or email)</label>
        <input id="ob-to" class="inp" placeholder="+1..." style="min-width:140px" />
      </div>
      <div>
        <label class="small">Row # (optional, for draft)</label>
        <input id="ob-row" class="inp" type="number" min="2" placeholder="e.g. 38" style="max-width:80px" />
      </div>
      <div id="ob-subject-wrap" class="hidden">
        <label class="small">Subject</label>
        <input id="ob-subject" class="inp" placeholder="Subject" style="min-width:180px" />
      </div>
      <div style="flex:1;min-width:200px">
        <label class="small">Message (empty = use draft)</label>
        <input id="ob-body" class="inp" placeholder="Leave empty to use draft template" style="width:100%" />
      </div>
      <div>
        <button type="button" class="btn" onclick="composeOutbound(this)">Queue</button>
      </div>
    </div>
    <span id="ob-msg" class="note" style="display:block;margin-top:8px"></span>
  </div>

  <div class="card">
    <div id="aiq-list" class="small">Loading…</div>
  </div>
</div>

<div id="tab-rules" class="tabpane hidden">
  <div class="card">
    <div class="h2">Business Rules</div>
    <div class="small">These rules power reservation guardrails (max party size, closed dates, hours, banner). Saved rules persist on the server.</div>
  </div>

  <div class="card">
    <div class="row">
      <div>
        <label class="small">Max party size</label>
        <input id="rules-max-party" class="inp" type="number" min="1" step="1"/>
      </div>
      <div>
        <label class="small">Match-day banner</label>
        <input id="rules-banner" class="inp" placeholder="🏟️ Match-day mode..."/>
      </div>
    </div>

    <div style="margin-top:10px">
      <label class="small">Closed dates (YYYY-MM-DD, one per line)</label>
      <textarea id="rules-closed" class="inp" rows="5" placeholder="2026-06-11&#10;2026-06-12"></textarea>
    </div>

    <div style="margin-top:10px">
      <div class="small" style="font-weight:800;margin-bottom:8px">Hours (HH:MM-HH:MM)</div>
      <div class="row">
        <div><label class="small">Mon</label><input id="h-mon" class="inp" placeholder="11:00-22:00"/></div>
        <div><label class="small">Tue</label><input id="h-tue" class="inp" placeholder="11:00-22:00"/></div>
        <div><label class="small">Wed</label><input id="h-wed" class="inp" placeholder="11:00-22:00"/></div>
        <div><label class="small">Thu</label><input id="h-thu" class="inp" placeholder="11:00-23:00"/></div>
        <div><label class="small">Fri</label><input id="h-fri" class="inp" placeholder="11:00-01:00"/></div>
        <div><label class="small">Sat</label><input id="h-sat" class="inp" placeholder="10:00-01:00"/></div>
        <div><label class="small">Sun</label><input id="h-sun" class="inp" placeholder="10:00-22:00"/></div>
      </div>
    </div>

    <div style="margin-top:12px;display:flex;gap:10px;flex-wrap:wrap">
      <button type="button" class="btn" data-min-role="owner" onclick="saveRules()">Save Rules</button>
      <button class="btn2" onclick="loadRules()">Reload</button>
      <span id="rules-msg" class="note"></span>
    </div>
  </div>
  
</div>

  """)

    # Menu tab
    html.append(r"""
<div id="tab-menu" class="tabpane hidden">
  <div class="card">
    <div class="h2">Menu Manager</div>
    <div class="small">Upload a JSON menu file to update <span class="code">/menu.json</span> (fan UI stays the same). Supports en/es/pt/fr blocks.</div>
  </div>

  <div class="card">
    <div class="row">
      <div>
        <label class="small">Upload menu JSON file</label>
        <input id="menu-file" class="inp" type="file" accept="application/json"/>
        <div style="margin-top:10px;display:flex;gap:10px;flex-wrap:wrap">
          <button type="button" class="btn" data-min-role="owner" onclick="uploadMenu()">Upload</button>
          <button class="btn2" onclick="loadMenu()">Load Current</button>
          <span id="menu-msg" class="note"></span>
        </div>
      </div>
      <div>
        <label class="small">Or paste menu JSON</label>
        <textarea id="menu-json" class="inp code" rows="12" placeholder='{"en":{"title":"Menu","items":[{"category_id":"bites","name":"Nachos","price":"$16","desc":"...","tag":"Share"}]}}'></textarea>
        <div style="margin-top:10px;display:flex;gap:10px;flex-wrap:wrap">
          <button class="btn2" onclick="saveMenuJson()">Save JSON</button>
        </div>
      </div>
    </div>
  </div>
</div>
""")


    # Audit tab
    html.append(r"""

<div id="tab-policies" class="tabpane hidden">
  <div class="card" >
    <div class="h2">Partner / Venue Policies (Hard)</div>
    <div class="small">
      Policies are enforced on <b>AI suggestions</b> and again on <b>Apply</b>. They cannot be bypassed by AI.
      Defaults are safe until you set partner-specific rules.
    </div>

    <div class="grid2" style="margin-top:10px">
      <div>
        <label class="small">Partner / Venue ID</label>
        <input id="pp-partner" class="inp" placeholder="e.g., VENUE_ABC"/>
        <div class="small" style="opacity:.8;margin-top:6px">Tip: use a stable ID you also store in your leads (partner/venue field). "default" applies if no partner is detected.</div>
      </div>
      <div>
        <label class="small">VIP minimum budget (USD)</label>
        <input id="pp-vip-min" class="inp" type="number" min="0" step="50" placeholder="1500"/>
      </div>
    </div>

    <div class="grid2" style="margin-top:10px">
      <div>
        <label class="small">Status updates</label>
        <label class="small" style="display:flex;gap:8px;align-items:center;margin-top:8px">
          <input id="pp-never-status" type="checkbox" checked/>
          <span>Hard block AI status_update (recommended)</span>
        </label>
        <div class="small" style="opacity:.8;margin-top:6px">Even if status_update is allowlisted, this can still block it per partner.</div>
      </div>
      <div>
        <label class="small">Allowed statuses (comma-separated)</label>
        <input id="pp-allowed-statuses" class="inp" placeholder="New, Confirmed, Seated, No-Show, Handled"/>
      </div>
    </div>

    <div class="grid2" style="margin-top:10px">
      <div>
        <label class="small">Outbound allowed channels</label>
        <input id="pp-allowed-channels" class="inp" placeholder="email, sms, whatsapp"/>
        <div class="small" style="opacity:.8;margin-top:6px">Used by outbound sending (next phase). Human-in-the-middle is always enforced.</div>
      </div>
      <div>
        <label class="small">Outbound requires role</label>
        <select id="pp-outbound-role" class="inp">
          <option value="manager">Manager+</option>
          <option value="owner">Owner only</option>
        </select>
      </div>
    </div>

    <div style="margin-top:12px;display:flex;gap:10px;flex-wrap:wrap">
      <button type="button" class="btn" data-min-role="owner" onclick="savePartnerPolicy()">Save Policy</button>
      <button class="btn2" onclick="loadPartnerPolicy()">Load Policy</button>
      <button class="btn2" data-min-role="owner" onclick="deletePartnerPolicy()">Delete Policy</button>
      <button class="btn2" onclick="loadPartnerList()">List Partners</button>
      <span id="pp-msg" class="note"></span>
    </div>

    <div id="pp-list" class="small" style="margin-top:10px;opacity:.9"></div>
  </div>
</div>

<div id="tab-audit" class="tabpane hidden">
  <div class="card">
    <div style="display:flex;align-items:center;justify-content:space-between;gap:10px;flex-wrap:wrap">
      <div>
        <div class="h2" style="margin:0">Audit Log</div>
        <div class="small">Shows who changed ops/rules/menu and when.</div>
      </div>
      <button class="btn2" type="button" data-min-role="owner" onclick="clearAuditAll()" style="margin-left:auto">Clear all</button>
    </div>
    <div style="margin-top:10px;display:flex;gap:10px;align-items:center;flex-wrap:wrap">
      <input class="inp" id="audit-limit" type="number" min="10" max="500" value="200" style="width:120px" />
      <select class="inp" id="audit-filter" style="width:220px">
        <option value="all">All events</option>
      </select>
      <select class="inp" id="audit-time" style="width:220px">
        <option value="">All time</option>
        <option value="30">Last 30 minutes</option>
        <option value="60">Last 1 hour</option>
        <option value="120">Last 2 hours</option>
        <option value="1440">Last 24 hours</option>
        <option value="10080">Last 7 days</option>
      </select>
      <button class="btn2" onclick="loadAudit()">Refresh</button>
      <span id="audit-msg" class="note"></span>
    </div>
    <div class="tablewrap" style="margin-top:10px">
      <table>
        <thead><tr><th>Time</th><th>Actor</th><th>Role</th><th>Event</th><th>Details</th><th></th></tr></thead>
        <tbody id="audit-body"></tbody>
      </table>
    </div>
  </div>
</div>

<!-- Drafts Modal -->
<div id="drafts-modal" class="modal-overlay" onclick="if(event.target===this)closeDraftsModal()">
  <div class="modal-box">
    <div class="modal-header">
      <div class="modal-title">Message Drafts</div>
      <button type="button" class="modal-close" onclick="closeDraftsModal()">Close</button>
    </div>
    <div class="small" style="margin-bottom:12px">Edit message templates and drafts (owner-only). Stored in per-venue drafts store (file or Redis), not in venue config.</div>
    <div style="display:flex;gap:10px;align-items:center;flex-wrap:wrap;margin-bottom:12px">
      <button type="button" class="btn2" onclick="loadDrafts()">Reload</button>
      <button type="button" class="btn" data-min-role="owner" onclick="saveDrafts()">Save</button>
      <span id="drafts-modal-msg" class="note"></span>
    </div>
    <textarea id="drafts-modal-json" class="inp" style="width:100%;min-height:420px;font-family:monospace;font-size:13px;box-sizing:border-box" spellcheck="false"></textarea>
  </div>
</div>

<!-- AI Queue Template Preview Modal -->
<div id="aiq-template-modal" class="modal-overlay" onclick="if(event.target===this)closeAiqTemplateModal()">
  <div class="modal-box" style="max-width:760px">
    <div class="modal-header">
      <div class="modal-title">Notification Template Preview</div>
      <button type="button" class="modal-close" onclick="closeAiqTemplateModal()">Close</button>
    </div>
    <div class="small" style="margin-bottom:10px">Review the exact bundled SMS + WhatsApp-template payload before approval or send.</div>
    <div id="aiq-template-modal-msg" class="note" style="margin-bottom:10px"></div>
    <div id="aiq-template-modal-body" class="small" style="white-space:normal"></div>
  </div>
</div>
""")

    # Scripts
    html.append("""
<script>

/* Admin tabs bootstrap (runs even if later script has a parse error) */
(function(){
  var tab = (location.hash || '#ops').slice(1) || 'ops';

  function qsa(sel){ return document.querySelectorAll(sel); }

  function setActive(tab){
    try{
      var btn = document.querySelector('.tabbtn[data-tab="'+tab+'"]');
      var minRole = (btn && btn.getAttribute && btn.getAttribute('data-minrole')) ? btn.getAttribute('data-minrole') : "manager";
      if(ROLE_RANK && ROLE_RANK[ROLE] !== undefined && ROLE_RANK[minRole] !== undefined){
        if(ROLE_RANK[ROLE] < ROLE_RANK[minRole]){
          try{ toast("Owner-only section"); }catch(e){}
          // snap back to Ops if a hash tried to open a locked tab
          try{
            if(tab !== "ops" && document.querySelector('.tabbtn[data-tab="ops"]')) tab = "ops";
          }catch(e){}
        }
      }
    }catch(e){}

    try{
      var btns = qsa('.tabbtn');
      for(var i=0;i<btns.length;i++){
        var b = btns[i];
        if(b && b.classList){
          var dt = b.getAttribute('data-tab');
          if(dt === tab) b.classList.add('active'); else b.classList.remove('active');
        }
      }

      var panes = qsa('.tabpane');
      for(var j=0;j<panes.length;j++){
        var p = panes[j];
        if(p && p.classList) p.classList.add('hidden');
      }

      var pane = document.getElementById('tab-'+tab);
      if(pane && pane.classList) pane.classList.remove('hidden');

      try{ history.replaceState(null,'','#'+tab); }catch(e){}
    }catch(e){}
  }

window.showTab = function(tab){

    // Fan Zone tab = redirect to isolated Fan Zone page (preserve key + venue)
    if(tab === 'fanzone'){
      try{
        var qs = new URLSearchParams(window.location.search || '');
        var url = '/admin/fanzone';
        var q = qs.toString();
        if(q) url += '?' + q;
        window.location.href = url;
      }catch(e){
        window.location.href = '/admin/fanzone' + (window.location.search || '');
      }
      return false;
    }

    try{
      var b = document.querySelector('.tabbtn[data-tabbtn="'+tab+'"]') || document.querySelector('.tabbtn[data-tab="'+tab+'"]');
      var minr = (b && b.getAttribute) ? (b.getAttribute('data-minrole') || 'manager') : 'manager';
      if(ROLE_RANK && ROLE_RANK[ROLE] !== undefined && ROLE_RANK[minr] !== undefined){
        if(ROLE_RANK[ROLE] < ROLE_RANK[minr]){
          try{ toast('Owner only — redirected to Ops', 'warn'); }catch(e){}
          try{ setActive('ops'); }catch(e){}
          return false;
        }
      }
    }catch(e){}

    // switch tab panes
    try{ setActive(tab); }catch(e){}

    // Leads tab: always load table from filter API (real params, sheet-backed) so we never show stale server-rendered rows
    if(tab === 'leads'){
      try{ if(typeof applyLeadsFiltersServer === 'function') applyLeadsFiltersServer(); }catch(e){}
    }

    // If your rules tab needs loads, keep this behavior
    if(tab === 'rules'){
      try{ loadPartnerList(); loadPartnerPolicy(); }catch(e){}
      try{ loadRules(); }catch(e){}
    }

    return false;
  };

  function bind(){
    var btns = qsa('.tabbtn');

    // mark owner-only tabs for managers
    try{
      for(var j=0;j<btns.length;j++){
        var br = btns[j];
        var minr = (br && br.getAttribute) ? (br.getAttribute('data-minrole')||'manager') : 'manager';
        if(ROLE_RANK && ROLE_RANK[ROLE] !== undefined && ROLE_RANK[minr] !== undefined){
          if(ROLE_RANK[ROLE] < ROLE_RANK[minr]){
            try{ br.classList.add('locked'); br.setAttribute('title','Owner only'); }catch(e){}
          }
        }
      }
    }catch(e){}

    // click handlers
    for(var i=0;i<btns.length;i++){
      (function(b){
        try{
          b.addEventListener('click', function(ev){
            try{ ev.preventDefault(); }catch(e){}
            var t = b.getAttribute('data-tab') || b.getAttribute('data-tabbtn');
            if(t){
              try{ window.showTab(t); }catch(e){}
            }
          });
        }catch(e){}
      })(btns[i]);
    }

    // initial hash or default ops
    var h = (location.hash || '').replace('#','').trim();
    if(h && document.querySelector('.tabbtn[data-tab="'+h+'"]')) window.showTab(h);
    else window.showTab('ops');

    window.addEventListener('hashchange', function(){
      var t = (location.hash || '').replace('#','').trim();
      if(t && document.querySelector('.tabbtn[data-tab="'+t+'"]')) window.showTab(t);
    });

    // apply initial tab on load (also keeps your earlier behavior)
    try{ setActive(tab); }catch(e){}
  }

  if(document.readyState === 'loading') document.addEventListener('DOMContentLoaded', bind);
  else bind();
})();
</script>

<script>
const KEY = (new URLSearchParams(window.location.search).get('key') || '');

const m = document.cookie.match(/(?:^|;\s*)venue_id=([^;]+)/);
const VENUE =
  (new URLSearchParams(window.location.search).get('venue') || '').trim() ||
  (m ? decodeURIComponent(m[1]) : '');

(function () {
  const _origFetch = window.fetch;
  window.fetch = function (input, init = {}) {
    try {
      const url = (typeof input === 'string')
        ? input
        : (input && input.url) ? input.url : '';

      if (url && url.startsWith('/admin/api/')) {
        init.headers = init.headers || {};
        if (VENUE) init.headers['X-Venue-Id'] = VENUE;

        if (KEY && typeof input === 'string' && !url.includes('key=')) {
          input = url + (url.includes('?') ? '&' : '?') + 'key=' + encodeURIComponent(KEY);
        }
      }
    } catch (e) {}
    return _origFetch(input, init);
  };
})();

let ROLE = (__ADMIN_ROLE__ || 'manager');

// Enforce owner-only locks (tabs + buttons)
document.addEventListener('click', function(ev){
  try{
    const el = ev.target && ev.target.closest ? ev.target.closest('[data-minrole],[data-min-role]') : null;
    if(!el) return;
    const need = (el.getAttribute('data-minrole') || el.getAttribute('data-min-role') || '').toLowerCase();
    if(need === 'owner' && ROLE !== 'owner'){
      ev.preventDefault();
      ev.stopPropagation();
      try{ toast('Owner-only'); }catch(e){ alert('Owner-only'); }
      try{ if(typeof showTab === 'function') showTab('ops'); }catch(e){}
      return false;
    }
  }catch(e){}
}, true);

document.addEventListener('DOMContentLoaded', function(){
  try{ _refreshRole(); setTimeout(_refreshRole, 500); }catch(e){}
});

const ROLE_RANK = { "manager": 1, "owner": 2 };

function _elSig(el){
  if(!el) return "null";
  const id = el.id ? ("#"+el.id) : "";
  const cls = (el.className && typeof el.className==="string") ? ("."+el.className.trim().replace(/\s+/g,'.')) : "";
  return (el.tagName||"").toLowerCase()+id+cls;
}

// If an invisible overlay is intercepting clicks, this will disable pointer-events on it
// and show a tiny toast with what was unblocked.
function installClickUnblocker(){
  try{
    document.addEventListener('click', (e)=>{
      try{
        // If the click is already hitting a real control, do nothing.
        const t = e.target;
        if(t && (t.closest('button,a,input,select,textarea,label,[role="button"],.tabbtn,.pillbtn'))){
          return;
        }
        const x = e.clientX, y = e.clientY;
        if(!(Number.isFinite(x) && Number.isFinite(y))) return;

        let top = document.elementFromPoint(x,y);
        // If top is the root containers, nothing to unblock.
        if(!top || top === document.body || top === document.documentElement) return;

        // Don't ever disable the main app container.
        if(top.closest && top.closest('.wrap')) return;

        // Heuristic: large positioned layers are most commonly the culprits.
        const cs = window.getComputedStyle(top);
        const pos = (cs.position||"");
        const pe  = (cs.pointerEvents||"");
        const op  = parseFloat(cs.opacity||"1");
        const rect = top.getBoundingClientRect();
        const big = rect.width >= (window.innerWidth*0.9) && rect.height >= (window.innerHeight*0.9);

        if(pe !== 'none' && (pos === 'fixed' || pos === 'absolute') && (big || op < 0.25)){
          top.style.pointerEvents = 'none';
          toast("Unblocked overlay: " + _elSig(top));
        }
      }catch(err){}
    }, true); // capture phase so it runs even if the overlay is the target
  }catch(err){}
}

function hasRole(minRole){
  const need = ROLE_RANK[minRole||"manager"] || 1;
  const have = ROLE_RANK[ROLE||"manager"] || 1;
  return have >= need;
}

function markLockedControls(){
  document.querySelectorAll('[data-min-role]').forEach(el=>{
    const need = ((el.getAttribute('data-min-role') || el.getAttribute('data-minrole')) || 'manager');
    if(!hasRole(need)){
      el.classList.add('locked');
      el.setAttribute('title', 'Owner-only');
      // Ensure it can still be tapped/clicked to explain why
      el.style.pointerEvents = 'auto';
    }
  });
}
window.addEventListener('DOMContentLoaded', markLockedControls);

// Make "locked" controls still clickable (they show a helpful message instead of silently failing)
document.addEventListener('click', (e)=>{
  const el = e.target && e.target.closest ? e.target.closest('[data-min-role],[data-minrole]') : null;
  if(!el) return;
  const need = ((el.getAttribute('data-min-role') || el.getAttribute('data-minrole')) || 'manager');
  if(hasRole(need)) return;
  e.preventDefault();
  e.stopPropagation();
  const label = (el.textContent||'').trim() || 'This action';
  alert(label + ' is Owner-only.');
}, true);



// ===== Leads filters (simple + fast) =====
let leadTierFilter = "all";   // all | vip | regular
let leadEntryFilter = "all";  // all | <entry_point>

function norm(s){ return (s||"").toString().trim().toLowerCase(); }

function applyLeadFilters(){
  const rows = document.querySelectorAll('#leadsTable tbody tr');
  let shown = 0;
  rows.forEach(tr=>{
    const tier = norm(tr.getAttribute('data-tier')||"");
    const entry = norm(tr.getAttribute('data-entry')||"");
    const isVip = (tier === 'vip');

    let ok = true;
    if(leadTierFilter === "vip") ok = isVip;
    if(leadTierFilter === "regular") ok = !isVip;

    if(ok && leadEntryFilter !== "all"){
      ok = (entry === leadEntryFilter);
    }

    tr.style.display = ok ? "" : "none";
    if(ok) shown++;
  });

  const hint = qs('#leadsCount');
  if(hint) hint.textContent = shown + " shown";
}

function _he(s){ return (s||'').toString().replace(/[&<>"']/g, c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]||c)); }
function _tipAttr(txt){ if(!txt||String(txt).length<12) return ''; try{ return ' class=\\"leads-cell-tip\\" data-tip=\\"'+encodeURIComponent(String(txt))+'\\"'; }catch(e){ return ''; } }
function _leadRowFromItem(it){
  const row = (it.sheet_row||it.row||0);
  const ts = _he(it.timestamp||'');
  const nm = _he(it.name||'');
  const ph = _he(it.phone||'');
  const d = _he(it.date||'');
  const t = _he(it.time||'');
  const ps = _he(it.party_size||'');
  const tier = (it.tier||'').toString().toLowerCase(); const vip = (it.vip||'').toString().toLowerCase();
  const isVip = (tier.includes('vip') || /^(yes|true|1|y|vip)$/.test(vip));
  const tierKey = isVip ? 'vip' : 'regular';
  const seg = tierKey==='vip' ? '⭐ VIP' : 'Regular';
  const segCls = seg.indexOf('⭐')>=0 ? 'badge warn' : 'badge';
  const ep = _he((it.entry_point||'').replace(/_/g,' '));
  const fullEp = (it.entry_point||'').replace(/_/g,' ');
  const fullCtx = (it.business_context||it.context||'');
  const fullNotes = (it.notes||'');
  const queue = _he(it.queue||'');
  const budget = _he(it.budget||'');
  const ctx = _he((fullCtx+'').substring(0,34));
  const notes = _he((fullNotes+'').substring(0,40));
  const st = (it.status||'New').toString().trim();
  const stLow = st.toLowerCase();
  const vipVal = isVip ? 'Yes' : 'No';
  const stSel = '<select class=\\'inp\\' id=\\'status-'+row+'\\'><option value=\\"New\\"'+(stLow==='new'?' selected':'')+'>New</option><option value=\\"Confirmed\\"'+(stLow==='confirmed'?' selected':'')+'>Confirmed</option><option value=\\"Seated\\"'+(stLow==='seated'?' selected':'')+'>Seated</option><option value=\\"No-Show\\"'+(stLow==='no-show'?' selected':'')+'>No-Show</option><option value=\\"Handled\\"'+(stLow==='handled'?' selected':'')+'>Handled</option></select>';
  const vipSel = '<select class=\\'inp\\' id=\\'vip-'+row+'\\'><option value=\\"Yes\\"'+(vipVal==='Yes'?' selected':'')+'>Yes</option><option value=\\"No\\"'+(vipVal==='No'?' selected':'')+'>No</option></select>';
  const tipEp = fullEp ? _tipAttr(fullEp) : '';
  const tipCtx = fullCtx.length>=28 ? _tipAttr(fullCtx) : '';
  const tipNotes = fullNotes.length>=28 ? _tipAttr(fullNotes) : '';
  const tipNm = (it.name||'').length>=12 ? _tipAttr(it.name||'') : '';
  const tipPh = (it.phone||'').length>=12 ? _tipAttr(it.phone||'') : '';
  return '<tr data-tier="'+tierKey+'" data-entry="'+_he(it.entry_point||'')+'"><td class=\\'code\\'>'+row+'</td><td>'+ts+'</td><td'+tipNm+'>'+nm+'</td><td'+tipPh+'>'+ph+'</td><td>'+d+'</td><td>'+t+'</td><td>'+ps+'</td><td'+_tipAttr(seg)+'><span class=\\"'+segCls+'\\">'+seg+'</span></td><td'+tipEp+'><span class=\\"pill\\">'+ep+'</span></td><td'+_tipAttr(queue)+'><span class=\\"badge good\\">'+queue+'</span></td><td>'+budget+'</td><td'+tipCtx+'><span class=\\"small\\">'+ctx+(ctx.length>=34?'…':'')+'</span></td><td'+tipNotes+'><span class=\\"small\\">'+notes+(notes.length>=40?'…':'')+'</span></td><td>'+stSel+'</td><td>'+vipSel+'</td><td><button type=\\"button\\" class=\\"btn primary\\" onclick=\\"saveLead('+row+')\\">Save</button><button type=\\"button\\" class=\\"btnTiny\\" title=\\"Set status to Handled\\" onclick=\\"markHandled('+row+')\\">✅</button></td></tr>';
}
function _leadsDdLabel(panelId, allLabel){
  const panel = qs('#'+panelId); if(!panel) return allLabel;
  const chk = panel.querySelectorAll('input[type=checkbox]:checked');
  if(!chk.length) return allLabel;
  const labs = Array.from(chk).map(c=>{ const lab = c.closest('label'); return lab ? lab.textContent.replace(/\\s+/g,' ').trim() : c.value; });
  return labs.length>2 ? (labs.length+' selected') : labs.join(', ');
}
function _closeAllLeadsDd(){ qsa('.leads-dd-panel').forEach(p=>p.classList.add('hidden')); qsa('.leads-dd-btn').forEach(b=>{ b.setAttribute('aria-expanded','false'); }); }
function _toggleLeadsDd(btnId, panelId, allLabel){
  const btn = qs('#'+btnId), panel = qs('#'+panelId);
  if(!btn||!panel) return;
  const open = panel.classList.contains('hidden');
  _closeAllLeadsDd();
  if(open){ panel.classList.remove('hidden'); btn.setAttribute('aria-expanded','true'); btn.innerHTML = _leadsDdLabel(panelId, allLabel)+' <span style="opacity:.6">▾</span>'; }
  else { btn.innerHTML = allLabel+' <span style="opacity:.6">▾</span>'; }
}
function _syncDdButtons(){
  const sb = qs('#flt-status-btn'); const sp = qs('#flt-status-panel');
  if(sb&&sp) sb.innerHTML = _leadsDdLabel('flt-status-panel','All statuses')+' <span style="opacity:.6">▾</span>';
  const tb = qs('#flt-tier-btn'); const tp = qs('#flt-tier-panel');
  if(tb&&tp) tb.innerHTML = _leadsDdLabel('flt-tier-panel','All tiers')+' <span style="opacity:.6">▾</span>';
}
async function applyLeadsFiltersServer(){
  _closeAllLeadsDd();
  const timeEl = qs('#flt-time'); const entryEl = qs('#flt-entry');
  const statuses = qsa('#flt-status-panel input[type=checkbox]:checked').map(c=>c.value);
  const tiers = qsa('#flt-tier-panel input[type=checkbox]:checked').map(c=>c.value);
  const timeVal = (timeEl && timeEl.value) ? timeEl.value : '';
  const entryVal = (entryEl && entryEl.value && entryEl.value !== 'all') ? entryEl.value : '';
  const params = new URLSearchParams();
  if(typeof KEY!=='undefined') params.set('key', KEY);
  if(typeof VENUE!=='undefined' && VENUE) params.set('venue', VENUE);
  params.set('limit','500');
  statuses.forEach(s=>params.append('status', s));
  tiers.forEach(t=>params.append('tier', t));
  if(timeVal) params.set('time', timeVal);
  if(entryVal) params.set('entry', entryVal);
  const btn = qs('#btn-leads-apply'); if(btn){ btn.disabled=true; btn.textContent='Loading…'; }
  const hint = qs('#leadsCount'); if(hint) hint.textContent = 'Loading…';
  try {
    const r = await fetch('/admin/api/leads/filter?'+params.toString(), { cache: 'no-store' });
    const j = await r.json().catch(()=>null);
    if(btn){ btn.disabled=false; btn.textContent='Apply'; }
    if(!j || !j.ok){ if(hint) hint.textContent = 'Error'; if(typeof toast==='function') toast(j&&j.error ? j.error : 'Filter failed'); return; }
    const items = j.items || [];
    const tbody = qs('#leadsTableBody');
    if(tbody) tbody.innerHTML = items.map(_leadRowFromItem).join('');
    if(hint) hint.textContent = items.length + ' shown';
    _populateLeadsEntryDropdown(j.entry_point_values || [], items);
    _syncDdButtons();
  } catch(e){ if(btn){ btn.disabled=false; btn.textContent='Apply'; } if(hint) hint.textContent='Error'; }
}
function _populateLeadsEntryDropdown(entry_point_values, items){
  const entries = new Set(entry_point_values || []);
  (items||[]).forEach(it=>{ const ep = (it.entry_point||'').toString().trim(); if(ep) entries.add(ep); });
  const sel = qs('#flt-entry');
  if(!sel) return;
  const currentVal = sel.value;
  sel.innerHTML = '';
  const all = document.createElement('option'); all.value = 'all'; all.textContent = 'All sources'; sel.appendChild(all);
  Array.from(entries).sort((a,b)=>a.localeCompare(b)).forEach(ep=>{ const o = document.createElement('option'); o.value = ep; o.textContent = ep.replace(/_/g,' '); sel.appendChild(o); });
  let ok = false; Array.from(sel.options).forEach(o=>{ if(o.value===currentVal) ok=true; });
  sel.value = ok ? currentVal : 'all';
}
function resetLeadsFiltersServer(){
  qsa('#flt-status-panel input[type=checkbox]').forEach(c=>c.checked=false);
  qsa('#flt-tier-panel input[type=checkbox]').forEach(c=>c.checked=false);
  const timeEl = qs('#flt-time'); const entryEl = qs('#flt-entry');
  if(timeEl) timeEl.value = '';
  if(entryEl) entryEl.value = 'all';
  _syncDdButtons();
  applyLeadsFiltersServer();
}
function setupLeadFilters(){
  const tbl = qs('#leadsTable');
  if(!tbl) return;
  const tbody = qs('#leadsTableBody') || tbl.querySelector('tbody');
  const rowCount = tbody ? tbody.querySelectorAll('tr').length : 0;
  const hint = qs('#leadsCount'); if(hint) hint.textContent = rowCount + " shown";
  qs('#btn-leads-apply')?.addEventListener('click', applyLeadsFiltersServer);
  qs('#btn-leads-reset')?.addEventListener('click', resetLeadsFiltersServer);
  qs('#flt-status-btn')?.addEventListener('click', function(e){ e.stopPropagation(); const p = qs('#flt-status-panel'); const o = p&&p.classList.contains('hidden'); _closeAllLeadsDd(); if(o){ p.classList.remove('hidden'); this.setAttribute('aria-expanded','true'); } });
  qs('#flt-tier-btn')?.addEventListener('click', function(e){ e.stopPropagation(); const p = qs('#flt-tier-panel'); const o = p&&p.classList.contains('hidden'); _closeAllLeadsDd(); if(o){ p.classList.remove('hidden'); this.setAttribute('aria-expanded','true'); } });
  qsa('#flt-status-panel input,#flt-tier-panel input').forEach(i=>i.addEventListener('change', _syncDdButtons));
  document.addEventListener('click', function(){ _closeAllLeadsDd(); _syncDdButtons(); });
  qsa('.leads-dd-panel').forEach(p=>p.addEventListener('click', function(e){ e.stopPropagation(); }));
  const tip = document.createElement('div'); tip.id = 'leadsHoverTip'; document.body.appendChild(tip);
  const tipEl = ()=>qs('#leadsHoverTip');
  tbl.addEventListener('mousemove', function(e){
    const el = e.target.closest('.leads-cell-tip[data-tip]');
    const node = tipEl();
    if(!node) return;
    if(!el){ node.style.display='none'; return; }
    const txt = el.getAttribute('data-tip'); if(!txt) return;
    try{ node.textContent = decodeURIComponent(txt); }catch(_){ node.textContent = txt; }
    node.style.display='block';
    node.style.left = Math.min(e.clientX+14, window.innerWidth - node.offsetWidth - 12)+'px';
    node.style.top = (e.clientY+12)+'px';
  });
  tbl.addEventListener('mouseleave', function(){ const n=tipEl(); if(n) n.style.display='none'; });
}


function qs(sel){return document.querySelector(sel);}
function qsa(sel){return Array.from(document.querySelectorAll(sel));}
                
qsa('.tabbtn').forEach(btn=>{
  btn.addEventListener('click', (e)=>{
    try{ e.preventDefault(); }catch(_){}

    const t = btn.dataset.tab || btn.getAttribute('data-tab') || btn.getAttribute('data-tabbtn') || '';

    // Always route through showTab so special tabs (like fanzone) can redirect
    if(typeof window.showTab === 'function'){
      window.showTab(t);
      return;
    }

    // Fallback (should rarely be needed)
    qsa('.tabbtn').forEach(b=>b.classList.remove('active'));
    btn.classList.add('active');

    ['ops','leads','ai','aiq','rules','menu','drafts','policies','audit','monitor'].forEach(x=>{
      const pane = document.getElementById('tab-'+x);
      if(!pane) return;
      pane.classList.toggle('hidden', x!==t);
    });

    if(t==='ai') loadAI();
    if(t==='aiq'){
      // UX: default AI Queue view to Pending (if user hasn't picked a status yet)
      try{
        const f = qs('#aiq-filter');
        if(f && (!f.value || !String(f.value).trim())) f.value = 'pending';
      }catch(e){}
      loadAIQueue();
    }
    if(t==='rules') loadRules();
    if(t==='menu') loadMenu();
    if(t==='drafts') loadDrafts();
    if(t==='audit') loadAudit();
  });
});

async function saveLead(sheetRow){
  const status = qs('#status-'+sheetRow).value;
  const vip = qs('#vip-'+sheetRow).value;
  const url = '/admin/update-lead?key='+encodeURIComponent(KEY)+(typeof VENUE !== 'undefined' && VENUE ? '&venue='+encodeURIComponent(VENUE) : '');
  const res = await fetch(url, {
    method:'POST',
    headers:{
      'Content-Type':'application/json',
      ...(typeof VENUE !== 'undefined' && VENUE ? {'X-Venue-Id': String(VENUE)} : {}),
    },
    body: JSON.stringify({row: sheetRow, status, vip})
  });
  const j = await res.json().catch(()=>{});
  if(j && j.ok){
    // Refresh from server to keep Segment/VIP/status fully in sync with sheet.
    try{ await applyLeadsFiltersServer(); }catch(e){}
    if(typeof toast==='function') toast('Lead updated', 'ok');
  } else {
    if(typeof toast==='function') toast('Save failed: ' + ((j && j.error) || res.status), 'err');
    else alert('Save failed: ' + ((j && j.error) || res.status));
  }
}

async function markHandled(sheetRow){
  // Minimal: set status to Handled + write an audit entry.
  const url = '/admin/update-lead?key='+encodeURIComponent(KEY)+(typeof VENUE !== 'undefined' && VENUE ? '&venue='+encodeURIComponent(VENUE) : '');
  const res = await fetch(url, {
    method:'POST',
    headers:{
      'Content-Type':'application/json',
      ...(typeof VENUE !== 'undefined' && VENUE ? {'X-Venue-Id': String(VENUE)} : {}),
    },
    body: JSON.stringify({row: sheetRow, status: 'Handled'})
  });
  const j = await res.json().catch(()=>{});
  if(j && j.ok){
    const sel = qs('#status-'+sheetRow);
    if(sel) sel.value = 'Handled';
  } else {
    alert('Failed: ' + (j && j.error ? j.error : res.status));
  }
}

async function loadRules(){
  const msg = qs('#rules-msg'); if(msg) msg.textContent='';
  const res = await fetch('/admin/api/rules?key='+encodeURIComponent(KEY));
  const j = await res.json().catch(()=>null);
  if(!j || !j.ok){ if(msg) msg.textContent='Failed to load rules'; return; }
  const r = j.rules || {};
  qs('#rules-max-party').value = r.max_party_size || '';
  qs('#rules-banner').value = r.match_day_banner || '';
  qs('#rules-closed').value = (r.closed_dates||[]).join('\\n');
  const h = r.hours || {};
  ['mon','tue','wed','thu','fri','sat','sun'].forEach(d=>{
    const el = qs('#h-'+d);
    if(el) el.value = (h[d]||'');
  });
}

async function saveRules(){
  const msg = qs('#rules-msg'); if(msg) msg.textContent='Saving...';
  const hours={};
  ['mon','tue','wed','thu','fri','sat','sun'].forEach(d=>hours[d]=qs('#h-'+d).value);
  const payload={
    max_party_size: parseInt(qs('#rules-max-party').value || '0', 10),
    match_day_banner: qs('#rules-banner').value || '',
    closed_dates: qs('#rules-closed').value || '',
    hours: hours
  };
  const res = await fetch('/admin/api/rules?key='+encodeURIComponent(KEY), {
    method:'POST',
    headers:{'Content-Type':'application/json'},
    body: JSON.stringify(payload)
  });
  const j = await res.json().catch(()=>null);
  if(j && j.ok){
    if(msg) msg.textContent='Saved ✔';
    // Optional: normalize inputs from server (in case it clamps values)
    try{ await loadRules(); }catch(e){}
    setTimeout(()=>{ try{ if(msg) msg.textContent=''; }catch(e){} }, 1400);
  } else {
    if(msg) msg.textContent='Save failed';
    alert('Save failed: '+(j && j.error ? j.error : res.status));
  }
}


// ===============================
// Partner / Venue Policies (Hard)
// ===============================
async function loadPartnerList(){
  const msg = qs('#pp-msg'); if(msg) msg.textContent='Loading partners...';
  const box = qs('#pp-list'); if(box) box.textContent='';
  try{
    const res = await fetch('/admin/api/partner-policies/list?key='+encodeURIComponent(KEY));
    const j = await res.json().catch(()=>null);
    if(!j || !j.ok){ if(msg) msg.textContent='Failed'; return; }
    const partners = (j.partners||[]).filter(Boolean);
    if(box){
      box.innerHTML = partners.length
        ? ('<b>Known partners:</b> ' + partners.map(p=>'<code style="padding:2px 6px;border:1px solid rgba(255,255,255,.12);border-radius:10px">'+escapeHtml(p)+'</code>').join(' '))
        : 'No partner policies saved yet (only default).';
    }
    if(msg) msg.textContent='Loaded ✔';
  }catch(e){
    if(msg) msg.textContent='Error';
  }
}

function _getPartnerId(){
  const p = (qs('#pp-partner')?.value||'').trim();
  return p || 'default';
}

async function loadPartnerPolicy(){
  const msg = qs('#pp-msg'); if(msg) msg.textContent='Loading...';
  const partner = _getPartnerId();
  try{
    const res = await fetch('/admin/api/partner-policies?key='+encodeURIComponent(KEY)+'&partner='+encodeURIComponent(partner));
    const j = await res.json().catch(()=>null);
    if(!j || !j.ok){ if(msg) msg.textContent='Failed'; return; }
    const pol = j.policy || {};
    qs('#pp-vip-min').value = (pol.vip_min_budget ?? 0);
    qs('#pp-never-status').checked = !!pol.never_status_update;
    qs('#pp-allowed-statuses').value = Array.isArray(pol.allowed_statuses) ? pol.allowed_statuses.join(', ') : (pol.allowed_statuses||'');
    const oa = pol.outbound_allowed || {};
    const allowed = Object.keys(oa).filter(k=>oa[k]);
    qs('#pp-allowed-channels').value = allowed.join(', ');
    qs('#pp-outbound-role').value = (pol.outbound_require_role || 'manager');
    if(msg) msg.textContent='Loaded ✔';
  }catch(e){
    if(msg) msg.textContent='Error';
  }
}

async function savePartnerPolicy(){
  const msg = qs('#pp-msg'); if(msg) msg.textContent='Saving...';
  const partner = _getPartnerId();
  const vipMin = parseInt(qs('#pp-vip-min').value||'0',10) || 0;
  const neverStatus = !!qs('#pp-never-status').checked;
  const statusesRaw = (qs('#pp-allowed-statuses').value||'').trim();
  const statuses = statusesRaw ? statusesRaw.split(',').map(s=>s.trim()).filter(Boolean) : [];
  const chRaw = (qs('#pp-allowed-channels').value||'').trim();
  const ch = chRaw ? chRaw.split(',').map(s=>s.trim().toLowerCase()).filter(Boolean) : [];
  const outbound_allowed = { email:false, sms:false, whatsapp:false };
  ch.forEach(k=>{ if(Object.prototype.hasOwnProperty.call(outbound_allowed,k)) outbound_allowed[k]=true; });
  const outbound_role = (qs('#pp-outbound-role').value||'manager').trim() || 'manager';

  const payload = {
    partner: partner,
    policy: {
      vip_min_budget: vipMin,
      never_status_update: neverStatus,
      allowed_statuses: statuses.length ? statuses : undefined,
      outbound_allowed: outbound_allowed,
      outbound_require_role: outbound_role
    }
  };

  try{
    const res = await fetch('/admin/api/partner-policies/set?key='+encodeURIComponent(KEY), {
      method:'POST',
      headers:{'Content-Type':'application/json'},
      body: JSON.stringify(payload)
    });
    const j = await res.json().catch(()=>null);
    if(j && j.ok){
      if(msg) msg.textContent='Saved ✔';
      try{ await loadPartnerList(); }catch(e){}
    } else {
      if(msg) msg.textContent=(j && j.error) ? ('Blocked: '+j.error) : 'Failed';
    }
  }catch(e){
    if(msg) msg.textContent='Error';
  }
}

async function deletePartnerPolicy(){
  const msg = qs('#pp-msg'); if(msg) msg.textContent='Deleting...';
  const partner = _getPartnerId();
  try{
    const res = await fetch('/admin/api/partner-policies/delete?key='+encodeURIComponent(KEY), {
      method:'POST',
      headers:{'Content-Type':'application/json'},
      body: JSON.stringify({partner: partner})
    });
    const j = await res.json().catch(()=>null);
    if(j && j.ok){
      if(msg) msg.textContent='Deleted ✔';
      try{ await loadPartnerList(); }catch(e){}
      if(partner !== 'default'){ qs('#pp-partner').value=''; }
      try{ await loadPartnerPolicy(); }catch(e){}
    } else {
      if(msg) msg.textContent=(j && j.error) ? ('Blocked: '+j.error) : 'Failed';
    }
  }catch(e){
    if(msg) msg.textContent='Error';
  }
}

async function loadAlerts(){
  try{
    qs('#al-msg').textContent = 'Loading…';
    const res = await fetch('/admin/api/alerts/settings?key='+encodeURIComponent(KEY));
    const j = await res.json().catch(()=>null);
    if(!j || !j.ok){ qs('#al-msg').textContent = 'Load failed: ' + ((j&&j.error)||res.status); return; }
    const s = j.settings || {};
    qs('#al-enabled').checked = !!s.enabled;
    qs('#al-rate').value = s.rate_limit_seconds ?? 600;
    const checks = s.checks || {};
    qs('#al-fixtures-stale').value = checks.fixtures_stale_seconds ?? 86400;

    const ch = s.channels || {};
    const slack = ch.slack || {};
    const email = ch.email || {};
    const sms = ch.sms || {};

    qs('#al-slack-en').checked = !!slack.enabled;
    qs('#al-slack-url').value = slack.webhook_url || '';

    qs('#al-email-en').checked = !!email.enabled;
    qs('#al-email-to').value = email.to || '';
    qs('#al-email-from').value = email.from || '';

    qs('#al-sms-en').checked = !!sms.enabled;
    qs('#al-sms-to').value = sms.to || '';

    qs('#al-msg').textContent = 'Loaded';
  }catch(e){
    qs('#al-msg').textContent = 'Load error';
  }
}

async function saveAlerts(){
  if(!hasRole('owner')){ qs('#al-msg').textContent = 'Owner only'; return; }
  try{
    qs('#al-msg').textContent = 'Saving…';
    const payload = {
      enabled: qs('#al-enabled').checked,
      rate_limit_seconds: parseInt(qs('#al-rate').value||'600',10),
      checks: { fixtures_stale_seconds: parseInt(qs('#al-fixtures-stale').value||'86400',10) },
      channels: {
        slack: { enabled: qs('#al-slack-en').checked, webhook_url: (qs('#al-slack-url').value||'').trim() },
        email: { enabled: qs('#al-email-en').checked, to: (qs('#al-email-to').value||'').trim(), from: (qs('#al-email-from').value||'').trim() },
        sms: { enabled: qs('#al-sms-en').checked, to: (qs('#al-sms-to').value||'').trim() }
      }
    };
    const res = await fetch('/admin/api/alerts/settings?key='+encodeURIComponent(KEY), {
      method:'POST', headers:{'Content-Type':'application/json'},
      body: JSON.stringify(payload)
    });
    const j = await res.json().catch(()=>null);
    if(!j || !j.ok){ qs('#al-msg').textContent = 'Save failed: ' + ((j&&j.error)||res.status); return; }
    qs('#al-msg').textContent = 'Saved';
  }catch(e){
    qs('#al-msg').textContent = 'Save error';
  }
}

async function testAlert(){
  if(!hasRole('owner')){ qs('#al-msg').textContent = 'Owner only'; return; }
  try{
    qs('#al-msg').textContent = 'Sending test…';
    const res = await fetch('/admin/api/alerts/test?key='+encodeURIComponent(KEY), { method:'POST' });
    const j = await res.json().catch(()=>null);
    if(!j || !j.ok){ qs('#al-msg').textContent = 'Test failed: ' + ((j&&j.error)||res.status); return; }
    qs('#al-msg').textContent = 'Test sent';
  }catch(e){
    qs('#al-msg').textContent = 'Test error';
  }
}

async function loadMenu(){
  const msg = qs('#menu-msg'); if(msg) msg.textContent='';
  const res = await fetch('/admin/api/menu?key='+encodeURIComponent(KEY));
  const j = await res.json().catch(()=>null);
  if(j && j.ok){
    qs('#menu-json').value = JSON.stringify(j.menu || {}, null, 2);
    if(msg) msg.textContent='Loaded ✔';
  } else {
    if(msg) msg.textContent='Failed to load';
  }
}

async function saveMenuJson(){
  const msg = qs('#menu-msg'); if(msg) msg.textContent='Saving...';
  let payload=null;
  try { payload = JSON.parse(qs('#menu-json').value || '{}'); } catch(e) {
    alert('Invalid JSON'); if(msg) msg.textContent='Invalid JSON'; return;
  }
  const res = await fetch('/admin/api/menu?key='+encodeURIComponent(KEY), {
    method:'POST',
    headers:{'Content-Type':'application/json'},
    body: JSON.stringify(payload)
  });
  const j = await res.json().catch(()=>null);
  if(j && j.ok){ if(msg) msg.textContent='Saved ✔'; }
  else { if(msg) msg.textContent='Save failed'; alert('Save failed: '+(j && j.error ? j.error : res.status)); }
}

async function uploadMenu(){
  const msg = qs('#menu-msg'); if(msg) msg.textContent='Uploading...';
  const f = qs('#menu-file').files[0];
  if(!f){ alert('Choose a JSON file'); if(msg) msg.textContent='No file'; return; }
  const fd = new FormData();
  fd.append('file', f);
  const res = await fetch('/admin/api/menu-upload?key='+encodeURIComponent(KEY), {
    method:'POST',
    body: fd
  });
  const j = await res.json().catch(()=>null);
  if(j && j.ok){ qs('#menu-json').value = JSON.stringify(j.menu || {}, null, 2); if(msg) msg.textContent='Uploaded ✔'; }
  else { if(msg) msg.textContent='Upload failed'; alert('Upload failed: '+(j && j.error ? j.error : res.status)); }
}


function _ensureMiniState(el, idSuffix){
  try{
    if(!el) return null;
    const id = 'ops-mini-'+idSuffix;
    let s = document.getElementById(id);
    if(!s){
      s = document.createElement('span');
      s.id = id;
      s.className = 'miniState';
      // place right after the control
      const parent = el.parentElement;
      if(parent){
        parent.appendChild(s);
      } else {
        el.insertAdjacentElement('afterend', s);
      }
    }
    return s;
  }catch(e){ return null; }
}
function _setMiniState(el, idSuffix, text){
  const s = _ensureMiniState(el, idSuffix);
  if(s){ s.textContent = text || ''; s.style.opacity = text ? '1' : '0'; }
}
function _ensureOpsMeta(){
  let el = document.getElementById('ops-meta');
  if(!el){
    el = document.createElement('div');
    el.id = 'ops-meta';
    el.className = 'small';
    el.style.marginTop = '6px';
    // Prefer to place near ops message area if present
    const msg = qs('#ops-msg');
    if(msg && msg.parentElement){
      msg.parentElement.appendChild(el);
    } else {
      // fallback: add to ops card area
      (document.querySelector('#tab-leads') || document.body).appendChild(el);
    }
  }
  return el;
}
function _renderOpsMeta(meta){
  try{
    const el = _ensureOpsMeta();
    if(!meta || !meta.ts) { el.textContent = ''; return; }
    const who = (meta.role? meta.role.toUpperCase():'') + (meta.actor? ' ('+meta.actor+')':'');
    el.textContent = `Last updated: ${meta.ts} ${who}`;
  }catch(e){}
}

async function loadOps(){
  const msg = qs('#ops-msg'); if(msg) msg.textContent='Loading...';
  let url = '/admin/api/ops?key='+encodeURIComponent(KEY);
  if(VENUE) url += '&venue='+encodeURIComponent(VENUE);
  const opts = {};
  if(VENUE) opts.headers = {'X-Venue-Id': VENUE};
  const res = await fetch(url, opts);
  const j = await res.json().catch(()=>null);
  if(!j || !j.ok){ if(msg) msg.textContent='Failed to load ops'; return; }
  try{ _renderOpsMeta(j.meta); }catch(e){}
  const o = j.ops || {};
  const pause = qs('#ops-pause'); if(pause) pause.checked = (o.pause_reservations===true || o.pause_reservations==='true');
  const vip = qs('#ops-viponly'); if(vip) vip.checked = (o.vip_only===true || o.vip_only==='true');
  const wl = qs('#ops-waitlist'); if(wl) wl.checked = (o.waitlist_mode===true || o.waitlist_mode==='true');
  if(msg) msg.textContent='';
}

async function saveOps(){
  const msg = qs('#ops-msg'); if(msg) msg.textContent='Saving...';
  const payload = {
    pause_reservations: !!(qs('#ops-pause') && qs('#ops-pause').checked),
    vip_only: !!(qs('#ops-viponly') && qs('#ops-viponly').checked),
    waitlist_mode: !!(qs('#ops-waitlist') && qs('#ops-waitlist').checked),
  };
    // Per-toggle micro feedback
  const elPause = qs('#ops-pause');
  const elVip   = qs('#ops-viponly');
  const elWait  = qs('#ops-waitlist');
  _setMiniState(elPause,'pause','Saving…');
  _setMiniState(elVip,'vip','Saving…');
  _setMiniState(elWait,'wait','Saving…');
  if(elPause) elPause.disabled = true;
  if(elVip) elVip.disabled = true;
  if(elWait) elWait.disabled = true;
  let url = '/admin/api/ops?key='+encodeURIComponent(KEY);
  if(VENUE) url += '&venue='+encodeURIComponent(VENUE);
  const headers = {'Content-Type':'application/json'};
  if(VENUE) headers['X-Venue-Id'] = VENUE;
  const res = await fetch(url, {
    method:'POST',
    headers,
    body: JSON.stringify(payload)
  });
  const j = await res.json().catch(()=>null);
  if(j && j.ok){ if(msg) msg.textContent='Saved ✔'; }
  else { if(msg) msg.textContent='Save failed'; alert('Save failed: '+(j && j.error ? j.error : res.status)); }
  try{ if(elPause) elPause.disabled = false; if(elVip) elVip.disabled = false; if(elWait) elWait.disabled = false; }catch(e){}
}

// ===== AI Automation =====
async function loadAI(){
  const msg = qs('#ai-msg'); if(msg) msg.textContent = '';
  try{
    const r = await fetch(`/admin/api/ai/settings?key=${encodeURIComponent(KEY)}`, {cache:'no-store'});
    const data = await r.json();
    if(!data.ok) throw new Error(data.error || 'Failed');
    const s = data.settings || {};
    // runtime
    qs('#ai-enabled').checked = !!s.enabled;
    qs('#ai-mode').value = (s.mode || 'suggest');
    qs('#ai-approval').checked = !!s.require_approval;
    qs('#ai-minconf').value = (typeof s.min_confidence === 'number' ? s.min_confidence : 0.7);

    // owner
    const isOwner = (ROLE === 'owner');
    qs('#ai-model').value = (s.model || '');
    qs('#ai-prompt').value = (s.system_prompt || '');

    const allow = s.allow_actions || {};
    qs('#ai-act-vip').checked = !!allow.vip_tag;
    qs('#ai-act-status').checked = !!allow.status_update;
    qs('#ai-act-draft').checked = !!allow.reply_draft;
    if(qs('#ai-act-sms')) qs('#ai-act-sms').checked = !!allow.send_sms;
    if(qs('#ai-act-email')) qs('#ai-act-email').checked = !!allow.send_email;
    if(qs('#ai-act-whatsapp')) qs('#ai-act-whatsapp').checked = !!allow.send_whatsapp;

    const feat = s.features || {};
    if(qs('#ai-feat-vip')) qs('#ai-feat-vip').checked = (feat.auto_vip_tag !== false);
    if(qs('#ai-feat-status')) qs('#ai-feat-status').checked = !!feat.auto_status_update;
    if(qs('#ai-feat-draft')) qs('#ai-feat-draft').checked = (feat.auto_reply_draft !== false);

    // lock owner-only fields for managers
    ['ai-model','ai-prompt','ai-act-vip','ai-act-status','ai-act-draft','ai-act-sms','ai-act-email','ai-act-whatsapp'].forEach(id=>{
      const el = qs('#'+id); if(!el) return;
      el.disabled = !isOwner;
      el.style.opacity = isOwner ? '1' : '.55';
    });
  }catch(e){
    if(msg) msg.textContent = 'Load failed: ' + (e.message || e);
  }
}

async function saveAI(){
  const msg = qs('#ai-msg'); if(msg) msg.textContent = 'Saving…';
  const payload = {
    enabled: qs('#ai-enabled')?.checked ? true : false,
    mode: qs('#ai-mode')?.value || 'suggest',
    require_approval: qs('#ai-approval')?.checked ? true : false,
    min_confidence: parseFloat(qs('#ai-minconf')?.value || '0.7'),
  };

  // owner fields
  if(ROLE === 'owner'){
    payload.model = (qs('#ai-model')?.value || '').trim();
    payload.system_prompt = (qs('#ai-prompt')?.value || '').trim();
    payload.allow_actions = {
      vip_tag: qs('#ai-act-vip')?.checked ? true : false,
      status_update: qs('#ai-act-status')?.checked ? true : false,
      reply_draft: qs('#ai-act-draft')?.checked ? true : false,
      send_sms: qs('#ai-act-sms')?.checked ? true : false,
      send_email: qs('#ai-act-email')?.checked ? true : false,
      send_whatsapp: qs('#ai-act-whatsapp')?.checked ? true : false,
    };
  }

  try{
    const r = await fetch(`/admin/api/ai/settings?key=${encodeURIComponent(KEY)}`, {
      method:'POST',
      headers:{'Content-Type':'application/json'},
      body: JSON.stringify(payload)
    });
    const data = await r.json();
    if(!data.ok) throw new Error(data.error || 'Failed');
    if(msg) msg.textContent = 'Saved ✔';
    // refresh to reflect merged settings
    loadAI();
  }catch(e){
    if(msg) msg.textContent = 'Save failed: ' + (e.message || e);
  }
}
async function applyPreset(name){
  const msg = qs('#preset-msg'); if(msg) msg.textContent='Applying "'+name+'" ...';
  let url = '/admin/api/presets/apply?key='+encodeURIComponent(KEY);
  if(VENUE) url += '&venue='+encodeURIComponent(VENUE);
  const headers = {'Content-Type':'application/json'};
  if(VENUE) headers['X-Venue-Id'] = VENUE;
  const res = await fetch(url, {
    method:'POST',
    headers,
    body: JSON.stringify({name})
  });
  const j = await res.json().catch(()=>null);
  if(j && j.ok){
    if(msg) msg.textContent='Applied ✔ (logged)';
    // Update ops toggles immediately from response so presets visibly select the right options
    const o = j.ops || {};
    const pause = qs('#ops-pause'); if(pause) pause.checked = (o.pause_reservations===true || o.pause_reservations==='true');
    const vip = qs('#ops-viponly'); if(vip) vip.checked = (o.vip_only===true || o.vip_only==='true');
    const wl = qs('#ops-waitlist'); if(wl) wl.checked = (o.waitlist_mode===true || o.waitlist_mode==='true');
    await loadOps();
    // If you are owner and preset touched rules, refresh rules form for visibility.
    if(j.rules) await loadRules();
  } else {
    if(msg) msg.textContent='Apply failed';
    alert('Preset failed: '+(j && j.error ? j.error : res.status));
  }
}


// ===== Drafts Modal =====
function showDraftsModal(){
  const modal = qs('#drafts-modal');
  if(modal){
    modal.classList.add('show');
    loadDrafts();
    document.addEventListener('keydown', _draftModalEscHandler);
  }
}

function closeDraftsModal(){
  const modal = qs('#drafts-modal');
  if(modal) modal.classList.remove('show');
  document.removeEventListener('keydown', _draftModalEscHandler);
}

function _draftModalEscHandler(e){
  if(e.key === 'Escape') closeDraftsModal();
}

async function loadDrafts(){
  const msg = qs('#drafts-modal-msg'); if(msg) msg.textContent = 'Loading…';
  const box = qs('#drafts-modal-json');
  try{
    const url = `/admin/api/drafts?key=${encodeURIComponent(KEY)}&venue=${encodeURIComponent(VENUE)}`;
    const r = await fetch(url, {cache:'no-store'});
    const j = await r.json();
    if(!j.ok) throw new Error(j.error || 'Failed');
    if(box) box.value = JSON.stringify(j.drafts || {}, null, 2);
    if(msg) msg.textContent = j.meta?.updated_at ? `Loaded • ${j.meta.updated_at}` : 'Loaded';
  }catch(e){
    if(msg) msg.textContent = `Error: ${e.message || e}`;
  }
}

async function saveDrafts(){
  const msg = qs('#drafts-modal-msg'); if(msg) msg.textContent = 'Saving…';
  const box = qs('#drafts-modal-json');
  if(!box){ if(msg) msg.textContent = 'No textarea found'; return; }
  let obj;
  try{ obj = JSON.parse(box.value || '{}'); }catch(e){ if(msg) msg.textContent = 'Invalid JSON'; return; }
  if(typeof obj !== 'object' || obj === null){ if(msg) msg.textContent = 'JSON must be an object'; return; }
  try{
    const url = `/admin/api/drafts?key=${encodeURIComponent(KEY)}&venue=${encodeURIComponent(VENUE)}`;
    const r = await fetch(url, {
      method:'POST',
      headers:{'Content-Type':'application/json'},
      body: JSON.stringify({drafts: obj})
    });
    const j = await r.json();
    if(!j.ok) throw new Error(j.error || 'Failed');
    if(msg) msg.textContent = j.meta?.updated_at ? `Saved • ${j.meta.updated_at}` : 'Saved';
  }catch(e){
    if(msg) msg.textContent = `Error: ${e.message || e}`;
  }
}

// ===== AI Approval Queue =====
let aiqSearchT = null;
let aiqFetchSeq = 0;
let aiqItemsById = {};

function showAiqTemplateModal(){
  const m = qs('#aiq-template-modal');
  if(m) m.classList.add('show');
}
function closeAiqTemplateModal(){
  const m = qs('#aiq-template-modal');
  if(m) m.classList.remove('show');
}
async function aiqViewTemplate(id){
  const item = aiqItemsById[id];
  if(!item) return;
  const msg = qs('#aiq-template-modal-msg');
  const body = qs('#aiq-template-modal-body');
  if(msg) msg.textContent = 'Loading preview…';
  if(body) body.innerHTML = '';
  showAiqTemplateModal();
  try{
    const r = await fetch(`/admin/api/outbound/template-preview?key=${encodeURIComponent(KEY)}&venue=${encodeURIComponent(VENUE)}`, {
      method:'POST',
      headers:{'Content-Type':'application/json'},
      body: JSON.stringify({ type: item.type, payload: item.payload || {} })
    });
    const j = await r.json().catch(()=>null);
    if(!j || !j.ok) throw new Error((j && j.error) || 'Preview failed');
    const p = j.preview || {};
    if(msg) msg.textContent = p.wa_template_configured ? 'Template configured' : 'Template SID missing on server';
    const varsTxt = esc(JSON.stringify(p.wa_variables || {}, null, 2));
    if(body){
      body.innerHTML = `
        <div style="display:grid;grid-template-columns:150px 1fr;gap:8px 12px;align-items:start">
          <div class="note">Kind</div><div><b>${esc(p.kind || '')}</b></div>
          <div class="note">To (input)</div><div>${esc(p.to_input || '')}</div>
          <div class="note">To (E.164)</div><div>${esc(p.to_e164 || '')}</div>
          <div class="note">WA Template SID</div><div>${esc(p.wa_template_sid || '(missing)')}</div>
          <div class="note">SMS Body</div><div><pre style="margin:0;white-space:pre-wrap">${esc(p.sms_body || '')}</pre></div>
          <div class="note">WA Variables</div><div><pre style="margin:0;white-space:pre-wrap">${varsTxt}</pre></div>
        </div>
      `;
    }
  }catch(e){
    if(msg) msg.textContent = `Preview failed: ${e.message || e}`;
  }
}

function clearAIQueueFilters(){
  const ids = ['aiq-filter','aiq-time','aiq-type','aiq-conf','aiq-search'];
  ids.forEach(id=>{
    const el = qs('#'+id);
    if(el) el.value = '';
  });
  loadAIQueue();
}

function aiqSearchDebounced(){
  if(aiqSearchT) clearTimeout(aiqSearchT);
  aiqSearchT = setTimeout(()=>loadAIQueue(), 260);
}

async function loadAIQueue(){
  const msg = qs('#aiq-msg'); if(msg) msg.textContent = 'Loading…';
  const list = qs('#aiq-list'); if(list) list.innerHTML = 'Loading…';
  const seq = ++aiqFetchSeq;
  try{
    const filt = (qs('#aiq-filter')?.value || '').trim();
    const timeVal = (qs('#aiq-time')?.value || '').trim();
    const typeVal = (qs('#aiq-type')?.value || '').trim();
    const confVal = (qs('#aiq-conf')?.value || '').trim();
    const qVal = (qs('#aiq-search')?.value || '').trim();
    const hasAny = !!(filt || timeVal || typeVal || confVal);
    const urlBase = `/admin/api/ai/queue?key=${encodeURIComponent(KEY)}&venue=${encodeURIComponent(VENUE)}`;
    const url = urlBase
      + (filt ? `&status=${encodeURIComponent(filt)}` : '')
      + (timeVal ? `&time=${encodeURIComponent(timeVal)}` : '')
      + (typeVal ? `&type=${encodeURIComponent(typeVal)}` : '')
      + (confVal ? `&conf=${encodeURIComponent(confVal)}` : '')
      + (qVal ? `&q=${encodeURIComponent(qVal)}` : '');
    const r = await fetch(url, {cache:'no-store'});
    const data = await r.json();
    if(seq !== aiqFetchSeq) return; // ignore out-of-order responses
    if(!data.ok) throw new Error(data.error || 'Failed');
    const q = data.queue || [];
    renderAIQueue(q);
    const hasSearch = !!qVal;
    if(msg) msg.textContent = q.length ? (`${q.length} item(s)${(hasAny || hasSearch) ? ' matched' : ''}`) : 'No items';
    }catch(e){
      if(msg) msg.textContent = 'No items';
      renderAIQueue([]); // explicit empty state for CI
  }
}

async function runAINew(){
  const msg = qs('#aiq-msg'); if(msg) msg.textContent = 'Running AI…';
  const lim = parseInt(qs('#ai-run-limit')?.value || '5', 10);
  try{
    const r = await fetch(`/admin/api/ai/run?key=${encodeURIComponent(KEY)}&venue=${encodeURIComponent(VENUE)}`, {
      method:'POST',
      headers:{'Content-Type':'application/json'},
      body: JSON.stringify({mode:'new', limit: isNaN(lim)?5:lim})
    });
    const data = await r.json();
    if(!data.ok) throw new Error(data.error || 'Failed');
    if(msg) msg.textContent = `Ran ${data.ran||0}. Proposed ${data.proposed||0}.`;
    await loadAIQueue();
  }catch(e){
    if(msg) msg.textContent = 'Run failed: ' + (e.message || e);
  }
}

async function runAIRow(){
  const msg = qs('#aiq-msg'); if(msg) msg.textContent = 'Running AI…';
  const row = parseInt(qs('#ai-run-row')?.value || '0', 10);
  if(!row || row < 2){
    if(msg) msg.textContent = 'Enter a valid sheet Row # (>= 2).';
    return;
  }
  try{
    const r = await fetch(`/admin/api/ai/run?key=${encodeURIComponent(KEY)}&venue=${encodeURIComponent(VENUE)}`, {
      method:'POST',
      headers:{'Content-Type':'application/json'},
      body: JSON.stringify({row})
    });
    const data = await r.json();
    if(!data.ok) throw new Error(data.error || 'Failed');
    if(msg) msg.textContent = `Row ${row}: Proposed ${data.proposed||0}.`;
    await loadAIQueue();
  }catch(e){
    if(msg) msg.textContent = 'Run failed: ' + (e.message || e);
  }
}

async function composeOutbound(btn){
  const msg = qs('#ob-msg'); if(msg) msg.textContent = '';
  if(btn) btn.disabled = true;
  const ch = (qs('#ob-channel')?.value || 'sms').trim();
  const to = (qs('#ob-to')?.value || '').trim();
  const rowVal = qs('#ob-row')?.value;
  const row = (rowVal && parseInt(rowVal,10) >= 2) ? parseInt(rowVal,10) : null;
  const subject = (qs('#ob-subject')?.value || '').trim();
  const body = (qs('#ob-body')?.value || '').trim();
  if(!to){ if(msg) msg.textContent = 'Enter To (phone or email).'; if(btn) btn.disabled = false; return; }
  try{
    const r = await fetch(`/admin/api/outbound/propose?key=${encodeURIComponent(KEY)}&venue=${encodeURIComponent(VENUE)}`, {
      method:'POST',
      headers:{'Content-Type':'application/json'},
      body: JSON.stringify({ channel: ch, to: to, row: row || undefined, subject: subject || undefined, message: body || undefined })
    });
    const j = await r.json().catch(()=>null);
    if(j && j.ok){ if(msg) msg.textContent = 'Queued.'; await loadAIQueue(); }
    else{ if(msg) msg.textContent = (j && j.error) ? j.error : 'Queue failed'; }
  }catch(e){ if(msg) msg.textContent = 'Error: ' + (e.message || e); }
  if(btn) btn.disabled = false;
}


function esc(s){ return (s||'').toString().replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c])); }

function renderAIQueue(items){
  const list = qs('#aiq-list');
  if(!list) return;
  aiqItemsById = {};

  // Explicit empty state for CI tests + clarity
  if(!items || !items.length){
    list.innerHTML = '<div class="note">AI Queue empty — no queued actions.</div>';
    return;
  }

  const rows = (items || []).map((it)=>{
    aiqItemsById[String(it.id || '')] = it;
    const id = esc(it.id || '');
    const typ = esc(it.type || '');
    const st  = esc(it.status || '');
    const conf = (typeof it.confidence === 'number') ? it.confidence.toFixed(2) : '';
    const when = esc(it.created_at || '');
    const why  = esc(it.rationale || it.why || it.reason || '');
    const payload = esc(JSON.stringify(it.payload || {}));

    const canAct = (st === 'pending');
    const isOutbound = (
      typ === 'send_email' || typ === 'send_sms' || typ === 'send_whatsapp' ||
      typ === 'send_confirmation' || typ === 'send_reservation_received' || typ === 'send_update' || typ === 'send_vip_update'
    );
    const canSend = isOutbound && (st === 'approved') && !it.sent_at;
    const sendLabel =
      (typ === 'send_sms') ? 'Send SMS' :
      (typ === 'send_whatsapp') ? 'Send WhatsApp' :
      (typ === 'send_email') ? 'Send Email' :
      (typ === 'send_confirmation') ? 'Send Confirmation' :
      (typ === 'send_reservation_received') ? 'Send Received' :
      (typ === 'send_update') ? 'Send Update' :
      (typ === 'send_vip_update') ? 'Send VIP Update' :
      'Send';
    const sendBtn = isOutbound ? `<button type="button" class="btn" ${canSend ? '' : 'disabled'} onclick="aiqSend('${id}', this)">${sendLabel}</button>` : '';
    const isBundledTemplate = (typ === 'send_confirmation' || typ === 'send_reservation_received' || typ === 'send_update' || typ === 'send_vip_update');
    const viewTplBtn = isBundledTemplate ? `<button type="button" class="btn2" onclick="aiqViewTemplate('${id}')">View Template</button>` : '';

    const approveBtn = `<button type="button" class="btn" ${canAct ? '' : 'disabled'} onclick="aiqApprove('${id}', this)">Approve</button>`;
    const denyBtn    = `<button type="button" class="btn2" ${canAct ? '' : 'disabled'} onclick="aiqDeny('${id}', this)">Deny</button>`;
    const overrideBtn = `<button type="button" class="btn" onclick="aiqOverride('${id}', this)">Owner Override</button>`;
    const removeBtn = `<button type="button" class="btnTiny" onclick="aiqRemove('${id}', this)">Remove from queue</button>`;

    return `
      <div class="card" style="margin-bottom:10px">
        <div style="display:flex;justify-content:space-between;gap:10px;flex-wrap:wrap;align-items:center">
          <div>
            <b>${typ || 'AI item'}</b>
            <span class="chip" style="margin-left:8px">${st || 'unknown'}</span>
            ${conf ? `<span class="note" style="margin-left:8px">conf ${conf}</span>` : ``}
          </div>
          <div class="note">${when}</div>
        </div>
        ${why ? `<div class="small" style="margin-top:8px;opacity:.9">${why}</div>` : ``}
        <details style="margin-top:8px">
          <summary class="small">Payload</summary>
          <pre class="small" style="white-space:pre-wrap;opacity:.9">${payload}</pre>
        </details>
        <div style="display:flex;gap:8px;flex-wrap:wrap;margin-top:10px">
          ${approveBtn}
          ${denyBtn}
          ${viewTplBtn}
          ${sendBtn}
          ${overrideBtn}
          ${removeBtn}
        </div>
      </div>
    `;
  });

  list.innerHTML = rows.join('');
}

async function clearAIQueue(){
  const msg = qs('#aiq-msg'); if(msg) msg.textContent = 'Clearing queue…';
  try{
    const r = await fetch(`/admin/api/ai/queue/clear?key=${encodeURIComponent(KEY)}&venue=${encodeURIComponent(VENUE)}`, {
      method:'POST',
      headers:{'Content-Type':'application/json'},
      body: JSON.stringify({})
    });
    const j = await r.json().catch(()=>null);
    if(!j || !j.ok){
      throw new Error((j && j.error) || 'Failed to clear');
    }
    if(msg) msg.textContent = 'Queue cleared';
    await loadAIQueue();
  }catch(e){
    if(msg) msg.textContent = 'Clear failed';
  }
}

async function aiqRemove(id, btn){
  if(!id) return;
  if(!confirm('Remove this item from the queue?')) return;
  const _btn = btn;
  if(_btn){ _btn.disabled = true; _btn.dataset.prevText = _btn.textContent || ''; _btn.textContent = 'Removing…'; }
  const msg = qs('#aiq-msg'); if(msg) msg.textContent = 'Removing…';
  try{
    const r = await fetch(`/admin/api/ai/queue/${encodeURIComponent(id)}/delete?key=${encodeURIComponent(KEY)}&venue=${encodeURIComponent(VENUE)}`, {
      method:'POST',
      headers:{'Content-Type':'application/json'},
      body: JSON.stringify({})
    });
    const j = await r.json().catch(()=>null);
    if(!j || !j.ok){
      throw new Error((j && j.error) || 'Failed');
    }
    if(msg) msg.textContent = 'Removed';
    await loadAIQueue();
  }catch(e){
    if(msg) msg.textContent = 'Remove failed';
  }finally{
    if(_btn){ _btn.disabled = false; _btn.textContent = _btn.dataset.prevText || 'Remove from queue'; }
  }
}

async function aiqApprove(id, btn){
  if(!confirm('Approve this action?')) return;
  // Button-level loading state
  const _btn = btn;
  if(_btn){ _btn.disabled = true; _btn.dataset.prevText = _btn.textContent || ''; _btn.textContent = 'Approving…'; }

  const msg = qs('#aiq-msg'); if(msg) msg.textContent = 'Approving…';
  const r = await fetch(`/admin/api/ai/queue/${encodeURIComponent(id)}/approve?key=${encodeURIComponent(KEY)}&venue=${encodeURIComponent(VENUE)}`, {
    method:'POST',
    headers:{'Content-Type':'application/json'},
    body: JSON.stringify({})
  });
  const j = await r.json().catch(()=>null);
  if(j && j.ok){ if(msg) msg.textContent='Approved ✔'; if(_btn){ _btn.disabled = false; _btn.textContent = (_btn.dataset.prevText || 'Approve'); } await loadAIQueue(); }
  else { if(msg) msg.textContent='Approve failed'; if(_btn){ _btn.disabled = false; _btn.textContent = (_btn.dataset.prevText || 'Approve'); } alert('Approve failed: '+(j && j.error ? j.error : r.status)); }
}


async function aiqSend(id, btn){
  if(!confirm('Send this outbound message now?')) return;
  const _btn = btn;
  if(_btn){ _btn.disabled = true; _btn.dataset.prevText = _btn.textContent || ''; _btn.textContent = 'Sending…'; }
  const msg = qs('#aiq-msg'); if(msg) msg.textContent = 'Sending…';
  const r = await fetch(`/admin/api/ai/queue/${encodeURIComponent(id)}/send?key=${encodeURIComponent(KEY)}&venue=${encodeURIComponent(VENUE)}`, {
    method:'POST',
    headers:{'Content-Type':'application/json'},
    body: JSON.stringify({})
  });
  const j = await r.json().catch(()=>null);
  if(j && j.ok){
    if(msg) msg.textContent='Sent ✔';
    if(_btn){ _btn.disabled=false; _btn.textContent=(_btn.dataset.prevText || 'Send'); }
    await loadAIQueue();
  }else{
    if(msg) msg.textContent='Send failed';
    if(_btn){ _btn.disabled=false; _btn.textContent=(_btn.dataset.prevText || 'Send'); }
    var errText = (j && (j.error || (j.result && (j.result.message || j.result.error)))) ? (j.error || j.result.message || j.result.error) : r.status;
    alert('Send failed: '+errText);
  }
}

async function aiqDeny(id, btn){
  if(!confirm('Deny this action?')) return;
  const _btn = btn;
  if(_btn){ _btn.disabled = true; _btn.dataset.prevText = _btn.textContent || ''; _btn.textContent = 'Denying…'; }

  const msg = qs('#aiq-msg'); if(msg) msg.textContent = 'Denying…';
  const r = await fetch(`/admin/api/ai/queue/${encodeURIComponent(id)}/deny?key=${encodeURIComponent(KEY)}&venue=${encodeURIComponent(VENUE)}`, {
    method:'POST',
    headers:{'Content-Type':'application/json'},
    body: JSON.stringify({})
  });
  const j = await r.json().catch(()=>null);
  if(j && j.ok){ if(msg) msg.textContent='Denied ✔'; if(_btn){ _btn.disabled = false; _btn.textContent = (_btn.dataset.prevText || 'Deny'); } await loadAIQueue(); }
  else { if(msg) msg.textContent='Deny failed'; if(_btn){ _btn.disabled = false; _btn.textContent = (_btn.dataset.prevText || 'Deny'); } alert('Deny failed: '+(j && j.error ? j.error : r.status)); }
}

async function aiqOverride(id, btn){
  if(typeof hasRole === 'function' && !hasRole('owner')){
    alert('Owner-only: override is locked for managers.');
    return;
  }
  const _btn = btn;
  if(_btn){ _btn.disabled = true; _btn.dataset.prevText = _btn.textContent || ''; _btn.textContent = 'Overriding…'; }

  // Owner-only: allow quick edit of payload/type before applying
  const typ = prompt('Override action type (vip_tag, status_update, reply_draft, send_email, send_sms, send_whatsapp, send_confirmation, send_reservation_received, send_update, send_vip_update):', 'vip_tag');
  if(!typ){ if(_btn){ _btn.disabled=false; _btn.textContent=(_btn.dataset.prevText || 'Owner Override'); } return; }
  let payloadTxt = prompt('Override payload JSON (must be valid JSON object):', '{"row":2,"vip":"VIP"}');
  if(payloadTxt === null){ if(_btn){ _btn.disabled=false; _btn.textContent=(_btn.dataset.prevText || 'Owner Override'); } return; }
  payloadTxt = payloadTxt.trim();
  let payloadObj = null;
  try{ payloadObj = JSON.parse(payloadTxt); }catch(e){ if(_btn){ _btn.disabled=false; _btn.textContent=(_btn.dataset.prevText || 'Owner Override'); } alert('Invalid JSON'); return; }
  const msg = qs('#aiq-msg'); if(msg) msg.textContent = 'Applying override…';
  const r = await fetch(`/admin/api/ai/queue/${encodeURIComponent(id)}/override?key=${encodeURIComponent(KEY)}&venue=${encodeURIComponent(VENUE)}`, {
    method:'POST',
    headers:{'Content-Type':'application/json'},
    body: JSON.stringify({type: typ, payload: payloadObj})
  });
  const j = await r.json().catch(()=>null);
  if(j && j.ok){ if(msg) msg.textContent='Override applied ✔'; if(_btn){ _btn.disabled=false; _btn.textContent=(_btn.dataset.prevText || 'Owner Override'); } await loadAIQueue(); }
  else { if(msg) msg.textContent='Override failed'; if(_btn){ _btn.disabled=false; _btn.textContent=(_btn.dataset.prevText || 'Owner Override'); } alert('Override failed: '+(j && j.error ? j.error : r.status)); }
}


function esc(s){
  return (s==null?'':String(s))
    .replaceAll('&','&amp;')
    .replaceAll('<','&lt;')
    .replaceAll('>','&gt;')
    .replaceAll('"','&quot;')
    .replaceAll("'","&#39;");
}

async function loadAudit(){
  const msg = qs('#audit-msg'); if(msg) msg.textContent='Loading...';
  const lim = parseInt((qs('#audit-limit') && qs('#audit-limit').value) || '200', 10) || 200;
  const filterEl = qs('#audit-filter');
  const selected = (filterEl && filterEl.value) ? filterEl.value : 'all';
  const timeEl = qs('#audit-time');
  const timeVal = (timeEl && timeEl.value) ? timeEl.value : '';

  // ✅ Always pass venue explicitly (never rely on cookie)
  const venueVal =
    (typeof VENUE !== 'undefined' && VENUE) ? VENUE :
    (window.VENUE ? window.VENUE : '');

  const url =
    '/admin/api/audit?key=' + encodeURIComponent(KEY) +
    '&venue=' + encodeURIComponent(venueVal) +
    '&limit=' + encodeURIComponent(lim) +
    (timeVal ? '&time=' + encodeURIComponent(timeVal) : '');

  const res = await fetch(url, { cache: 'no-store' });
  const j = await res.json().catch(()=>null);
  if(!j || !j.ok){ if(msg) msg.textContent='Failed to load audit'; return; }

  let entries = (j.entries || j.items || []);

  // Normalize older shapes if any
  entries = entries.map(e=>{
    if(e && (e.event || e.ts)) return e;
    return {
      ts: e.ts || e.time || '',
      event: e.action || e.event || '',
      role: e.role || '',
      actor: e.actor || '',
      details: e.details || e.meta || {}
    };
  });

  // Newest first
  entries.sort((a,b)=> String(b.ts||'').localeCompare(String(a.ts||'')));

  // Populate filter options (event types)
  if(filterEl){
    const keep = filterEl.value || 'all';
    const events = Array.from(
      new Set(entries.map(e=>String(e.event||'').trim()).filter(Boolean))
    ).sort();

    let html = '<option value="all">All events</option>';
    events.forEach(ev=>{
      html += '<option value="'+esc(ev)+'">'+esc(ev)+'</option>';
    });
    filterEl.innerHTML = html;
    filterEl.value = (events.includes(keep) ? keep : 'all');
    filterEl.onchange = ()=>loadAudit();
  }
  if(timeEl){
    timeEl.onchange = ()=>loadAudit();
  }

  const activeFilter = (filterEl && filterEl.value) ? filterEl.value : selected;
  const body = qs('#audit-body');

  if(body){
    body.innerHTML = '';
    const shown = entries.filter(e =>
      activeFilter === 'all' ? true : String(e.event||'') === String(activeFilter)
    );

    if(!shown.length){
      body.innerHTML =
        '<tr><td colspan="6"><span class="note">No audit entries.</span></td></tr>';
    } else {
      shown.forEach(e=>{
        const details = (e.details || {});
        const copyPayload = JSON.stringify(e, null, 2);
        const ts = String(e.ts || '');
        const ev = String(e.event || '');
        const actor = String(e.actor || '');
        const tr = document.createElement('tr');
        tr.dataset.ts = ts;
        tr.dataset.event = ev;
        tr.dataset.actor = actor;
        tr.innerHTML =
          '<td>'+esc(ts)+'</td>' +
          '<td><span class="code">'+esc(actor)+'</span></td>' +
          '<td>'+esc(e.role||'')+'</td>' +
          '<td>'+esc(ev)+'</td>' +
          '<td><span class="code">'+esc(JSON.stringify(details))+'</span></td>' +
          '<td style="white-space:nowrap;display:flex;gap:6px;flex-wrap:wrap">' +
            '<button class="btn2" type="button" data-act="copy">Copy</button>' +
            '<button class="btnTiny" type="button" data-act="clear">Clear</button>' +
          '</td>';

        const copyBtn = tr.querySelector('button[data-act="copy"]');
        if(copyBtn){
          copyBtn.addEventListener('click', async ()=>{
            try{
              if(navigator?.clipboard?.writeText){
                await navigator.clipboard.writeText(copyPayload);
              } else {
                const ta = document.createElement('textarea');
                ta.value = copyPayload;
                document.body.appendChild(ta);
                ta.select();
                document.execCommand('copy');
                ta.remove();
              }
              if(msg){
                msg.textContent = 'Copied';
                setTimeout(()=>{ if(msg) msg.textContent=''; }, 800);
              }
            }catch(_){
              if(msg){
                msg.textContent = 'Copy failed';
                setTimeout(()=>{ if(msg) msg.textContent=''; }, 1200);
              }
            }
          });
        }

        const clearBtn = tr.querySelector('button[data-act="clear"]');
        if(clearBtn){
          clearBtn.addEventListener('click', ()=>{
            clearAuditOne(ts, ev, actor);
          });
        }

        body.appendChild(tr);
      });
    }
  }

  if(msg) msg.textContent='';
}

async function clearAuditAll(){
  if(typeof hasRole === 'function' && !hasRole('owner')){
    alert('Owner-only: clearing the audit log is locked for managers.');
    return;
  }
  if(!confirm('Clear all audit entries for this venue? This cannot be undone.')) return;
  const msg = qs('#audit-msg'); if(msg) msg.textContent='Clearing…';
  try{
    const venueVal =
      (typeof VENUE !== 'undefined' && VENUE) ? VENUE :
      (window.VENUE ? window.VENUE : '');
    const url =
      '/admin/api/audit/clear?key=' + encodeURIComponent(KEY) +
      '&venue=' + encodeURIComponent(venueVal);
    const res = await fetch(url, {
      method:'POST',
      headers:{'Content-Type':'application/json'},
      body: JSON.stringify({})
    });
    const j = await res.json().catch(()=>null);
    if(!j || !j.ok){
      throw new Error((j && j.error) || 'Failed');
    }
    if(msg) msg.textContent='Cleared';
    await loadAudit();
  }catch(e){
    if(msg) msg.textContent='Clear failed';
  }
}

async function clearAuditOne(ts, eventName, actor){
  if(typeof hasRole === 'function' && !hasRole('owner')){
    alert('Owner-only: clearing audit entries is locked for managers.');
    return;
  }
  if(!ts || !eventName) return;
  const msg = qs('#audit-msg'); if(msg) msg.textContent='Clearing…';
  try{
    const venueVal =
      (typeof VENUE !== 'undefined' && VENUE) ? VENUE :
      (window.VENUE ? window.VENUE : '');
    const url =
      '/admin/api/audit/clear_one?key=' + encodeURIComponent(KEY) +
      '&venue=' + encodeURIComponent(venueVal);
    const res = await fetch(url, {
      method:'POST',
      headers:{'Content-Type':'application/json'},
      body: JSON.stringify({ts, event: eventName, actor})
    });
    const j = await res.json().catch(()=>null);
    if(!j || !j.ok){
      throw new Error((j && j.error) || 'Failed');
    }
    if(msg) msg.textContent='Cleared';
    await loadAudit();
  }catch(e){
    if(msg) msg.textContent='Clear failed';
  }
}


async function loadNotifs(){
  const msg = qs('#notif-msg'); 
  if(msg) msg.textContent = 'Loading…';

  try{
    const m = document.cookie.match(/(?:^|;\s*)venue_id=([^;]+)/);
    const VENUE = ((new URLSearchParams(location.search).get('venue') || '').trim()) || (m ? decodeURIComponent(m[1]) : '');

    const timeEl = qs('#notif-time');
    const timeVal = (timeEl && timeEl.value) ? timeEl.value : '';
    const url = `/admin/api/notifications?limit=50&key=${encodeURIComponent(KEY||'')}` + (timeVal ? `&time=${encodeURIComponent(timeVal)}` : '');

    const r = await fetch(url, {
    cache: 'no-store',
    headers: { 'X-Venue-Id': VENUE }
    });
    const j = await r.json().catch(()=>null);
    const items = (j && j.items) ? (j.items || []) : [];

    // update badge
    const c = document.querySelector('#notifCount');
    if(c) c.textContent = String(items.length || 0);

    const body = document.querySelector('#notifBody');
    if(body){
      body.innerHTML = '';

      if(!items.length){
        body.innerHTML = '<div class="note">No notifications.</div>';
      } else {
        items.forEach(it=>{
          const d = it.details || {};
          const row = document.createElement('div');

          row.style.cssText =
            'padding:12px;border:1px solid rgba(255,255,255,.16);border-radius:14px;margin-bottom:10px;background:rgba(255,255,255,.06)';

          // ✅ SAFE pretty-print (strings or objects)
          let text;
          if (typeof d === 'string') {
            text = d;
          } else {
            try {
              text = JSON.stringify(d, null, 2);
            } catch (e) {
              text = String(d);
            }
          }

          const ts = String(it.ts || '');

          row.innerHTML =
            '<div style="display:flex;justify-content:space-between;gap:10px;flex-wrap:wrap;align-items:center">' +
              '<div class="note">'+esc(ts)+'</div>' +
              '<div style="display:flex;gap:8px;align-items:center;flex-wrap:wrap">' +
                '<span class="code">'+esc(it.event || '')+'</span>' +
                '<button type="button" class="btnTiny" data-ts="'+esc(ts)+'">Clear</button>' +
              '</div>' +
            '</div>' +
            '<div class="note" style="margin-top:6px">Details</div>' +
            '<pre style="margin-top:6px;padding:10px;border-radius:10px;background:rgba(255,255,255,.08);color:#eef2ff;white-space:pre-wrap;word-break:break-word;font-size:12px;line-height:1.45">' +
              esc(text) +
            '</pre>';

          const btn = row.querySelector('button[data-ts]');
          if(btn){
            btn.addEventListener('click', ()=>{
              const tsVal = btn.getAttribute('data-ts') || '';
              clearNotif(tsVal);
            });
          }

          body.appendChild(row);
        });
      }
    }

    if(msg) msg.textContent = '';
  }catch(e){
    if(msg) msg.textContent = 'Load failed';
  }
}

async function clearAllNotifs(){
  const msg = qs('#notif-msg');
  if(msg) msg.textContent = 'Clearing…';
  try{
    const r = await fetch(`/admin/api/notifications/clear?key=${encodeURIComponent(KEY||'')}`, {
      method:'POST',
      headers:{'Content-Type':'application/json'}
    });
    const j = await r.json().catch(()=>null);
    if(!j || !j.ok){
      throw new Error((j && j.error) || 'Failed to clear');
    }
    if(msg) msg.textContent = 'Cleared';
    try{
      await loadNotifs();
    }catch(e){}
  }catch(e){
    if(msg) msg.textContent = 'Clear failed';
  }
}

async function clearNotif(ts){
  if(!ts) return;
  const msg = qs('#notif-msg');
  if(msg) msg.textContent = 'Clearing…';
  try{
    const r = await fetch(`/admin/api/notifications/clear-one?key=${encodeURIComponent(KEY||'')}`, {
      method:'POST',
      headers:{'Content-Type':'application/json'},
      body: JSON.stringify({ts})
    });
    const j = await r.json().catch(()=>null);
    if(!j || !j.ok){
      throw new Error((j && j.error) || 'Failed to clear');
    }
    if(msg) msg.textContent = 'Cleared';
    try{
      await loadNotifs();
    }catch(e){}
  }catch(e){
    if(msg) msg.textContent = 'Clear failed';
  }
}

/* =========================
   Fan Zone Admin UI
   ========================= */

/* NOTE:
   These functions are INTENTIONALLY kept.
   They are used ONLY on /admin/fanzone.
   They must NOT be called from /admin tabs.
*/

async function initFanZoneAdmin(){
  const root = document.querySelector('#fanzoneAdminRoot');
  if(!root) return;

  if(!root.dataset.built){
    root.dataset.built = "1";
    root.innerHTML = `
      <div style="display:flex;gap:10px;flex-wrap:wrap;align-items:center;margin-bottom:10px">
        <button class="btn2" type="button" id="fzLoadBtn">Load</button>
        <button class="btn" type="button" id="fzSaveBtn">Save</button>
        <span class="note" id="fzMsg"></span>
      </div>
      <textarea id="fzJson" class="inp"
        style="width:100%;min-height:220px;font-family:ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;">
      </textarea>
      <div class="small" style="opacity:.7;margin-top:8px">Edit JSON then Save.</div>
    `;

    document.querySelector('#fzLoadBtn')?.addEventListener('click', loadFanZoneState);
    document.querySelector('#fzSaveBtn')?.addEventListener('click', saveFanZoneState);
  }

  await loadFanZoneState();
}

async function loadFanZoneState(){
  const msg = document.querySelector('#fzMsg');
  const ta  = document.querySelector('#fzJson');
  if(msg) msg.textContent = 'Loading…';
  try{
    const r = await fetch(`/admin/api/fanzone/state?key=${encodeURIComponent(KEY||'')}`, {cache:'no-store'});
    const j = await r.json().catch(()=>null);
    if(!j || !j.ok) throw new Error('Load failed');
    if(ta) ta.value = JSON.stringify(j.state || {}, null, 2);
    if(msg) msg.textContent = 'Loaded ✓';
  }catch(e){
    if(msg) msg.textContent = 'Load failed';
  }
}

async function saveFanZoneState(){
  const msg = document.querySelector('#fzMsg');
  const ta  = document.querySelector('#fzJson');
  if(msg) msg.textContent = 'Saving…';
  try{
    const payload = JSON.parse((ta && ta.value) ? ta.value : '{}');
    const r = await fetch(`/admin/api/fanzone/save?key=${encodeURIComponent(KEY||'')}`, {
      method:'POST',
      headers:{'Content-Type':'application/json'},
      body: JSON.stringify(payload)
    });
    const j = await r.json().catch(()=>null);
    if(!j || !j.ok) throw new Error('Save failed');
    if(msg) msg.textContent = 'Saved ✓';
  }catch(e){
    if(msg) msg.textContent = 'Save failed (bad JSON?)';
  }
}

async function clearNotifs(){
  const msg = qs('#notif-msg'); if(msg) msg.textContent='Clearing…';
  try{
    const r = await fetch(`/admin/api/notifications/clear?key=${encodeURIComponent(KEY||'')}`, {method:'POST'});
    const j = await r.json();
    if(j.ok){
      if(msg) msg.textContent='Cleared';
      loadNotifs();
    }else{
      if(msg) msg.textContent='Error';
    }
  }catch(e){
    if(msg) msg.textContent='Error';
  }
}

/* =========================
   Admin Tabs (Fan Zone REMOVED)
   ========================= */

function openNotifications(){
  try{
    showTab('ops');
    setTimeout(()=>{ document.querySelector('#notifCard')?.scrollIntoView({behavior:'smooth', block:'start'}); }, 60);
    loadNotifs();
  }catch(e){}
}
// Poll notifications lightly
setInterval(()=>{ try{ loadNotifs(); }catch(e){} }, 15000);

// Reload notifications when time-range changes (server-side).
try{
  const notifTimeEl = qs('#notif-time');
  if(notifTimeEl){
    notifTimeEl.onchange = ()=>loadNotifs();
  }
}catch(e){}


// --- Refresh controls (visual-only; calls existing loaders safely) ---
let __autoTimer = null;

function _nowHHMMSS(){
  const d=new Date();
  const p=n=>String(n).padStart(2,'0');
  return `${p(d.getHours())}:${p(d.getMinutes())}:${p(d.getSeconds())}`;
}
function updateLastRef(){
  const el=document.getElementById('lastRef');
  if(el) el.textContent = `Last refresh: ${_nowHHMMSS()}`;
}

function refreshAll(source){
  try{ loadNotifs(); }catch(e){}
  try{ loadOps(); }catch(e){}
  try{ loadAI(); }catch(e){}
  try{ loadAIQueue(); }catch(e){}
  // These are safe even if you're not on the tab; they just fetch current configs
  try{ loadRules(); }catch(e){}
  try{ loadMenu(); }catch(e){}
  try{ loadHealth(); }catch(e){}

  // ✅ Refresh audit automatically if user is currently on the Audit tab
  try{
    const ap = document.getElementById('tab-audit');
    if(ap && !ap.classList.contains('hidden')) loadAudit();
  }catch(e){}

  updateLastRef();
}

async function loadHealth(){
  const msg = qs('#health-msg'); if(msg) msg.textContent='Loading…';
  const body = qs('#health-body'); if(body) body.textContent='';
  try{
    const r = await fetch(`/admin/api/health?key=${encodeURIComponent(KEY)}`, {cache:'no-store'});
    const j = await r.json();
    if(!j.ok) throw new Error(j.error||'Failed');
    const rep = j.report || {};
    if(msg) {
      const last = (j.alerts_last_any_ts||'').trim();
      const lastTxt = last ? (' · last alert ' + last.replace('T',' ').replace('Z','')) : ' · last alert never';
      msg.textContent = (rep.status||'ok').toUpperCase() + (j.alerts_enabled ? ' · alerts ON' : ' · alerts OFF') + lastTxt;
    }
    const ts = qs('#health-ts'); if(ts) ts.textContent = rep.ts ? ('Updated '+rep.ts) : '';
    if(body){
      const lines = (rep.checks||[]).map(c=>{
        const badge = c.ok ? '✅' : (c.severity==='error' ? '🚨' : '⚠️');
        return `${badge} ${c.name}: ${c.message||''}`;
      });
      body.textContent = lines.join('\\n');
    }
  }catch(e){
    if(msg) msg.textContent='Load failed: '+(e.message||e);
    if(body) body.textContent='Unable to load health report.';
  }
}

async function runHealth(){
  const msg = qs('#health-msg'); if(msg) msg.textContent='Running…';
  const body = qs('#health-body'); if(body) body.textContent='';
  try{
    const r = await fetch(`/admin/api/health/run?key=${encodeURIComponent(KEY)}`, {method:'POST'});
    const j = await r.json();
    if(!j.ok) throw new Error(j.error||'Failed');
    const rep = j.report || {};
    if(msg){
      const last = (j.alerts_last_any_ts||'').trim();
      const lastTxt = last ? (' · last alert ' + last.replace('T',' ').replace('Z','')) : ' · last alert never';
      msg.textContent = (rep.status||'ok').toUpperCase() + ' · checked' + lastTxt;
    }
    const ts = qs('#health-ts'); if(ts) ts.textContent = rep.ts ? ('Updated '+rep.ts) : '';
    if(body){
      const lines = (rep.checks||[]).map(c=>{
        const badge = c.ok ? '✅' : (c.severity==='error' ? '🚨' : '⚠️');
        return `${badge} ${c.name}: ${c.message||''}`;
      });
      body.textContent = lines.join('\\n');
    }
    // also refresh notifications (alerts may have been emitted)
    try{ loadNotifs(); }catch(e){}
  }catch(e){
    if(msg) msg.textContent='Run failed: '+(e.message||e);
    if(body) body.textContent='Unable to run health checks.';
  }
}



function _setAutoLabel(on){
  const lab=document.getElementById('autoLabel');
  if(lab) lab.textContent = on ? "Auto: On" : "Auto: Off";
}

function _getAutoEvery(){
  const sel=document.getElementById('autoEvery');
  const v = sel ? parseInt(sel.value||"30",10) : 30;
  return (isFinite(v) && v>0) ? v : 30;
}

function startAutoRefresh(){
  stopAutoRefresh();
  const every=_getAutoEvery();
  __autoTimer = setInterval(()=>{ refreshAll('auto'); }, every*1000);
  localStorage.setItem('wc_auto_refresh','1');
  localStorage.setItem('wc_auto_refresh_every', String(every));
  _setAutoLabel(true);
}

function stopAutoRefresh(){
  if(__autoTimer){ try{ clearInterval(__autoTimer); }catch(e){} }
  __autoTimer=null;
  localStorage.setItem('wc_auto_refresh','0');
  _setAutoLabel(false);
}

function toggleAutoRefresh(){
  if(__autoTimer) stopAutoRefresh();
  else startAutoRefresh();
}

function autoEveryChanged(){
  const every=_getAutoEvery();
  localStorage.setItem('wc_auto_refresh_every', String(every));
  if(__autoTimer) startAutoRefresh(); // restart with new interval
}

function _initRefreshControls(){
  // restore interval
  const savedEvery = parseInt(localStorage.getItem('wc_auto_refresh_every')||"30",10);
  const sel=document.getElementById('autoEvery');
  if(sel && isFinite(savedEvery)) sel.value = String(savedEvery);
  // set initial last refresh time
  updateLastRef();
  // restore auto state
  const on = (localStorage.getItem('wc_auto_refresh')||"0") === "1";
  if(on) startAutoRefresh();
  else _setAutoLabel(false);
}

document.addEventListener('DOMContentLoaded', ()=>{
  
  try{ installClickUnblocker(); }catch(e){}
try{ setupTabs(); }catch(e){}
  if(((location.hash||'').replace('#','').trim()) === 'fanzone'){
  try{ initFanZoneAdmin(); }catch(e){}
}
  try{ markLockedControls(); }catch(e){}
  try{ setupLeadFilters(); }catch(e){}
  try{ loadNotifs(); }catch(e){}
  try{ _initRefreshControls(); }catch(e){}
  try{ refreshAll('boot'); }catch(e){}
});
</script>
""".replace("__ADMIN_KEY__", json.dumps(key)).replace("__ADMIN_ROLE__", json.dumps(role)))

    html.append("</div></body></html>")
    out = make_response("".join(html))
    try:
        out.set_cookie(
            "venue_id",
            _venue_id(),
            httponly=False,
            samesite="Lax",
            path="/admin"
        )
    except Exception:
        pass
    return out





@app.route("/admin/api/ai/draft-reply", methods=["POST"])
def admin_api_ai_draft_reply():
    """Owner-only: generate a reply draft for a specific lead row and enqueue it for approval.

    Body: { "row": <sheet row number> }
    """
    ok, resp = _require_admin(min_role="owner")
    if not ok:
        return resp
    ctx = _admin_ctx()
    actor = ctx.get("actor","")
    role = ctx.get("role","")
    payload = request.get_json(silent=True) or {}
    try:
        row_num = int(payload.get("row") or 0)
    except Exception:
        row_num = 0
    if row_num < 2:
        return jsonify({"ok": False, "error": "Invalid row"}), 400

    try:
        gc = get_gspread_client()
        ws = _open_default_spreadsheet(gc, venue_id=_venue_id()).sheet1
        header = ws.row_values(1) or []
        vals = ws.row_values(row_num) or []
        hmap = header_map(header)

        def g(key: str) -> str:
            col = hmap.get(key)
            if not col:
                return ""
            return (vals[col-1] if col-1 < len(vals) else "") or ""

        lead = {
            "name": g("name"),
            "phone": g("phone"),
            "date": g("date"),
            "time": g("time"),
            "party_size": g("party_size"),
            "language": g("language"),
            "budget": g("budget"),
            "notes": g("notes"),
            "entry_point": g("entry_point"),
            "tier": g("tier"),
            "business_context": g("business_context"),
        }

        settings = _get_ai_settings()
        if not settings.get("enabled"):
            return jsonify({"ok": False, "error": "AI disabled"}), 409
        if not (settings.get("allow_actions") or {}).get("reply_draft", True):
            return jsonify({"ok": False, "error": "reply_draft not allowed"}), 409
        if not _ai_feature_allows("reply_draft", settings=settings):
            return jsonify({"ok": False, "error": "reply_draft disabled by feature flag"}), 409

        draft_text = ""
        try:
            system_msg = (settings.get("system_prompt") or "").strip()
            user_msg = (
                "Draft a short, premium, action-oriented reply to this lead. "
                "Do NOT mention internal systems. Ask for any missing required details.\n\n"
                f"Name: {lead.get('name')}\nPhone: {lead.get('phone')}\nDate: {lead.get('date')}\nTime: {lead.get('time')}\n"
                f"Party: {lead.get('party_size')}\nBudget: {lead.get('budget')}\nNotes: {lead.get('notes')}\nLanguage: {lead.get('language')}\n"
                "\nReturn plain text only."
            )
            if not _OPENAI_AVAILABLE or client is None:
                raise RuntimeError("OpenAI SDK missing")
            resp2 = client.responses.create(
                model=settings.get("model") or os.environ.get("CHAT_MODEL","gpt-4o-mini"),
                input=[
                    {"role":"system","content":system_msg},
                    {"role":"user","content":user_msg},
                ],
            )
            draft_text = (resp2.output_text or "").strip()
        except Exception:
            draft_text = (
                f"Hi {lead.get('name') or 'there'}, thanks for reaching out. "
                "Confirm your party size and preferred time, and we’ll reserve the best available option for match day."
            )

        entry = {
            "id": _queue_new_id(),
            "type": "reply_draft",
            "payload": {"row": row_num, "draft": draft_text[:2000]},
            "confidence": 0.6,
            "rationale": "Owner-requested reply draft",
            "status": "pending",
            "created_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "created_by": actor,
            "created_role": role,
            "reviewed_at": None,
            "reviewed_by": None,
            "reviewed_role": None,
            "applied_result": None,
        }
        _queue_add(entry)
        _audit("ai.queue.created", {"id": entry["id"], "type": entry["type"], "source": "reply_draft", "row": row_num})
        return jsonify({"ok": True, "queued": entry})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


def _generate_reply_draft_text(lead: Dict[str, Any]) -> str:
    """Generate a plain-text reply draft (best-effort; falls back to a template)."""
    settings = _get_ai_settings()
    system_msg = (settings.get("system_prompt") or "").strip()
    user_msg = (
        "Draft a short, premium, action-oriented reply to this lead. "
        "Do NOT mention internal systems. Ask for any missing required details.\n\n"
        f"Name: {lead.get('name')}\nPhone: {lead.get('phone')}\nDate: {lead.get('date')}\nTime: {lead.get('time')}\n"
        f"Party: {lead.get('party_size')}\nBudget: {lead.get('budget')}\nNotes: {lead.get('notes')}\nLanguage: {lead.get('language')}\n"
        "\nReturn plain text only."
    )
    try:
        if not _OPENAI_AVAILABLE or client is None:
            raise RuntimeError("OpenAI SDK missing")
        resp2 = client.responses.create(
            model=settings.get("model") or os.environ.get("CHAT_MODEL","gpt-4o-mini"),
            input=[
                {"role":"system","content":system_msg},
                {"role":"user","content":user_msg},
            ],
        )
        return (resp2.output_text or "").strip()
    except Exception:
        return (
            f"Hi {lead.get('name') or 'there'}, thanks for reaching out. "
            "Confirm your party size and preferred time, and we’ll reserve the best available option for match day."
        )


def _auto_suggest_reply_draft_for_reservation(lead: Dict[str, Any]) -> None:
    """
    Proactive reply draft suggestion for staff review.
    Triggered when a new reservation is appended to local store.
    """
    try:
        settings = _get_ai_settings()
        if not settings.get("enabled"):
            return
        if not (settings.get("allow_actions") or {}).get("reply_draft", True):
            return
        if not _ai_feature_allows("reply_draft", settings=settings):
            return

        rid = str(lead.get("reservation_id") or "").strip()
        found_row = 0
        try:
            found = _find_sheet_row_by_reservation_id(rid, venue_id=_venue_id())
            if found:
                found_row = int(found[0] or 0)
        except Exception:
            found_row = 0
        # Best-effort idempotency so we don't spam queue items on retries.
        try:
            _redis_init_if_needed()
            if globals().get("_REDIS_ENABLED") and globals().get("_REDIS"):
                vid = _venue_id() if "_venue_id" in globals() else "default"
                fp_raw = "|".join([
                    vid,
                    rid,
                    str(lead.get("phone") or "").strip(),
                    str(lead.get("date") or "").strip(),
                    str(lead.get("time") or "").strip(),
                ])[:800]
                fp = hashlib.sha256(fp_raw.encode("utf-8")).hexdigest()
                dk = f"{_REDIS_NS}:{vid}:auto_reply_draft_dedupe:{fp}"
                if not _REDIS.set(dk, "1", nx=True, ex=600):
                    return
        except Exception:
            pass

        draft_lead = {
            "name": str(lead.get("name") or "").strip(),
            "phone": str(lead.get("phone") or "").strip(),
            "date": str(lead.get("date") or "").strip(),
            "time": str(lead.get("time") or "").strip(),
            "party_size": str(lead.get("party_size") or "").strip(),
            "budget": str(lead.get("budget") or "").strip(),
            "notes": str(lead.get("notes") or "").strip(),
            "language": str(lead.get("language") or lead.get("lang") or "en").strip() or "en",
        }
        draft_text = _generate_reply_draft_text(draft_lead).strip()
        if not draft_text:
            return

        entry = {
            "id": _queue_new_id(),
            "type": "reply_draft",
            "payload": {
                "row": found_row or 0,
                "sheet_row": found_row or 0,
                "draft": draft_text[:2000],
                "reservation_id": rid,
            },
            "confidence": 0.6,
            "rationale": "Auto-suggested reply draft",
            "status": "pending",
            "created_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "created_by": "system",
            "created_role": "system",
            "reviewed_at": None,
            "reviewed_by": None,
            "reviewed_role": None,
            "applied_result": None,
        }
        _queue_add(entry)
        _audit(
            "ai.queue.created",
            {"id": entry["id"], "type": entry["type"], "source": "reply_draft.auto", "row": 0, "reservation_id": rid},
        )
        _notify(
            "ai.queue.created",
            {
                "id": entry["id"],
                "type": entry["type"],
                "source": "reply_draft.auto",
                "row": found_row or None,
                "reservation_id": rid,
            },
            targets=["owner", "manager"],
        )
    except Exception:
        return


@app.route("/admin/api/analytics/load-forecast", methods=["GET"])
def admin_api_load_forecast():
    """Step 13: read-only load forecast (manager+)."""
    ok, resp = _require_admin(min_role="manager")
    if not ok:
        return resp

    try:
        gc = get_gspread_client()
        ws = _open_default_spreadsheet(gc, venue_id=_venue_id()).sheet1
        header = ws.row_values(1) or []
        hmap = header_map(header)

        all_vals = ws.get_all_values() or []
        rows = (all_vals[1:] if len(all_vals) > 1 else [])
        rows = rows[-800:]

        def col(key: str) -> int:
            return (hmap.get(key) or 0) - 1

        ts_i = col("timestamp")
        vip_i = col("vip")

        now = datetime.now(timezone.utc)

        def parse_ts(r):
            s = (r[ts_i] if ts_i >= 0 and ts_i < len(r) else "").strip()
            if not s:
                return None
            try:
                return datetime.fromisoformat(s.replace("Z","+00:00")).astimezone(timezone.utc)
            except Exception:
                try:
                    return datetime.fromisoformat(s).replace(tzinfo=timezone.utc)
                except Exception:
                    return None

        def is_vip(r):
            s = (r[vip_i] if vip_i >= 0 and vip_i < len(r) else "").strip().lower()
            return s in ("vip", "yes", "y", "true", "1")

        buckets_7 = {}
        buckets_30 = {}
        hours = {}
        vip_30 = 0
        total_30 = 0

        for r in rows:
            dt = parse_ts(r)
            if dt is None:
                continue
            age_days = (now - dt).total_seconds() / 86400.0
            day_key = dt.strftime("%Y-%m-%d")
            hour_key = dt.strftime("%H:00")

            if age_days <= 7.0:
                buckets_7[day_key] = buckets_7.get(day_key, 0) + 1
            if age_days <= 30.0:
                buckets_30[day_key] = buckets_30.get(day_key, 0) + 1
                total_30 += 1
                if is_vip(r):
                    vip_30 += 1
                hours[hour_key] = hours.get(hour_key, 0) + 1

        def top_k(dct, k=5):
            items = sorted(dct.items(), key=lambda x: x[1], reverse=True)
            return [{"key": a, "count": b} for a,b in items[:k]]

        out = {
            "ok": True,
            "generated_at": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "last_7_days_total": sum(buckets_7.values()),
            "last_30_days_total": sum(buckets_30.values()),
            "top_days_7": top_k(buckets_7, 5),
            "top_days_30": top_k(buckets_30, 7),
            "top_hours_30": top_k(hours, 6),
            "vip_ratio_30": (vip_30/total_30) if total_30 else 0.0,
        }
        return jsonify(out)
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/admin/api/ai/replay", methods=["POST"])
def admin_api_ai_replay():
    """Step 14: audit replay (owner-only, read-only)."""
    ok, resp = _require_admin(min_role="owner")
    if not ok:
        return resp

    body = request.get_json(silent=True) or {}
    try:
        row_num = int(body.get("row") or 0)
    except Exception:
        row_num = 0
    if row_num < 2:
        return jsonify({"ok": False, "error": "Invalid row"}), 400

    model_override = (body.get("model") or "").strip()
    prompt_override = (body.get("system_prompt") or "").strip()

    try:
        gc = get_gspread_client()
        ws = _open_default_spreadsheet(gc, venue_id=_venue_id()).sheet1
        header = ws.row_values(1) or []
        vals = ws.row_values(row_num) or []
        hmap = header_map(header)

        def g(key: str) -> str:
            col = hmap.get(key)
            if not col:
                return ""
            return (vals[col-1] if col-1 < len(vals) else "") or ""

        lead = {
            "intent": g("entry_point"),
            "contact": (g("name") + " " + g("phone")).strip(),
            "budget": g("budget"),
            "party_size": g("party_size"),
            "datetime": (g("date") + " " + g("time")).strip(),
            "notes": g("notes"),
            "lang": g("language"),
        }

        old = dict(AI_SETTINGS or {})
        try:
            if model_override:
                AI_SETTINGS["model"] = model_override
            if prompt_override:
                AI_SETTINGS["system_prompt"] = prompt_override
            suggestion = _ai_suggest_actions_for_lead(lead, row_num)
        finally:
            AI_SETTINGS.clear()
            AI_SETTINGS.update(old)

        q = _load_ai_queue()
        related = []
        for it in q:
            payload = it.get("payload") or {}
            prow = payload.get("row") or payload.get("sheet_row")
            if prow is not None and str(prow) == str(row_num):
                related.append(it)
            if len(related) >= 5:
                break

        return jsonify({"ok": True, "row": row_num, "lead": lead, "replay": suggestion, "recent_queue": related})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


    html.append("<title>Fan Zone Admin</title>")
    html.append(r"""<style>
:root{
  --bg:#0b1020;
  --panel:#0f1b33;
  --line:rgba(255,255,255,.10);
  --text:#eaf0ff;
  --muted:#b9c7ee;
  --gold:#d4af37;
  --good:#2ea043;
  --warn:#ffcc66;
  --bad:#ff5d5d;

  --menu-bg:#0f172a;
  --menu-hover:#1e293b;
  --menu-text:#f8fafc;
}

body{
  margin:0;
  font-family:Arial,system-ui,sans-serif;
  background:radial-gradient(1200px 700px at 20% 10%, #142a5b 0%, var(--bg) 55%);
  color:var(--text);
}

.wrap{max-width:1600px;margin:0 auto;padding:18px;}
.topbar{display:flex;align-items:center;justify-content:space-between;gap:12px;margin-bottom:14px;}
.h1{font-size:18px;font-weight:800;letter-spacing:.3px}
.sub{color:var(--muted);font-size:12px;word-break:break-word;overflow-wrap:break-word}

.pills{display:flex;gap:8px;flex-wrap:wrap}
.pill{border:1px solid var(--line);background:rgba(255,255,255,.03);padding:8px 10px;border-radius:999px;font-size:12px;color:var(--text)}
.pill b{color:var(--gold)}

.controls{
  display:grid;
  grid-template-columns:1fr;
  gap:18px;
  align-items:start;
  margin:12px 0 14px;
  width:100%;
  overflow:visible;
}
.controls > *{min-width:0;width:100%}
@media(min-width:900px){
  .controls{grid-template-columns:1fr 1fr;gap:20px}
}
.field{display:flex;flex-direction:column;gap:10px;min-width:0;overflow:visible}
.pair{display:grid;grid-template-columns:1fr 1fr;gap:14px;width:100%;min-width:0}
.pair > div{display:flex;flex-direction:column;gap:6px;min-width:0;width:100%}
.pair input{width:100%;min-width:0}
.sub + .inp, .sub + select, .sub + input{margin-top:4px}

*,*::before,*::after{box-sizing:border-box}
.inp, select, input[type="text"], input[type="number"]{
  width:100%;
  box-sizing:border-box;
  background:rgba(255,255,255,.04);
  border:1px solid var(--line);
  color:#f8fafc;
  border-radius:10px;
  padding:9px 10px;
  font-size:12px;
  outline:none;
}
.inp::placeholder,
input[type="text"]::placeholder,
input[type="number"]::placeholder{
  color:rgba(248,250,252,.45);
}

select option{
  background:#0f172a;
  color:#f8fafc;
}

.tablewrap,.card,.panel{overflow:visible;}

.btn{
  cursor:pointer;
  background:linear-gradient(180deg, rgba(212,175,55,.18), rgba(212,175,55,.06));
  border:1px solid rgba(212,175,55,.35);
  color:var(--text);
  border-radius:12px;
  padding:9px 12px;
  font-size:12px
}

.toast{
  position:fixed;
  right:14px;
  bottom:14px;
  background:rgba(0,0,0,.65);
  border:1px solid var(--line);
  padding:10px 12px;
  border-radius:12px;
  font-size:12px;
  display:none;
  color:var(--menu-text);
}
</style>""")
    html.append("</head><body><div class='wrap'>")

    html.append("<div class='topbar'>")
    html.append(f"<div><div class='h1'>Fan Zone Admin — {_hesc(SHEET_NAME or 'World Cup')}</div><div class='sub'>Poll controls (Sponsor text + Match of the Day) • Key required</div></div>")
    html.append("<div style='display:flex;gap:10px;align-items:center;flex-wrap:wrap'>")
    html.append(f"<a class='btn' href='/admin?key={key}' style='text-decoration:none;display:inline-block'>Ops</a>")
    html.append(f"<a class='btn' href='/admin/fanzone?key={key}&venue={venue_id}' "f"style='text-decoration:none;display:inline-block'>Poll Controls</a>")
    html.append("</div></div>")
    html.append(f"<div style='display:flex;gap:8px;margin:10px 0 14px 0;flex-wrap:wrap;'>"
    f"<a href='/admin?key={key}' style='text-decoration:none;color:var(--text);padding:8px 12px;border:1px solid var(--line);border-radius:999px;background:rgba(255,255,255,.04);font-weight:800'>Ops</a>"
    f"<a href='/admin/fanzone?key={key}&venue={venue_id}' style='text-decoration:none;color:var(--text);padding:8px 12px;border:1px solid rgba(212,175,55,.35);border-radius:999px;background:rgba(212,175,55,.10);font-weight:900'>Fan Zone</a>"
    f"</div>")

    html.append(r"""
<div class="panelcard" style="margin:14px 0;border:1px solid var(--line);border-radius:16px;padding:12px;background:rgba(255,255,255,.03);box-shadow:0 10px 35px rgba(0,0,0,.25)">
  <div style="display:flex;align-items:flex-end;justify-content:space-between;gap:12px;flex-wrap:wrap">
    <div>
      <div style="font-weight:800;letter-spacing:.02em">Fan Zone • Poll Controls</div>
      <div class="sub">Edit sponsor text + set Match of the Day (no redeploy). Also shows live poll status.</div>
    </div>
    <button type="button" class="btn" id="btnSaveConfig">Save settings</button>
  </div>

  <div class="controls" style="margin-top:14px">
    <div class="field">
      <div class="sub">Sponsor label (“Presented by …”)</div>
      <input class="inp" id="pollSponsorText" placeholder="Fan Pick presented by …" />
      <div class="small">Saved into config and shown in Fan Zone.</div>

      <div class="sub" style="margin-top:12px">Poll lock</div>
      <select id="pollLockMode" class="inp">
        <option value="auto">Auto (lock at kickoff)</option>
        <option value="unlocked">Force Unlocked (admin override)</option>
        <option value="locked">Force Locked</option>
      </select>
      <div class="small">If you need to reopen voting after kickoff, set <b>Force Unlocked</b>.</div>
    </div>

    <div class="field">
      <div class="sub">Match of the Day</div>
      <select id="motdSelect" class="inp"></select>

      <div class="sub" style="margin-top:6px">Manual override (optional):</div>
      <div class="pair">
        <div>
          <div class="sub">Home team</div>
          <input class="inp" id="motdHome" placeholder="Home team"/>
        </div>
        <div>
          <div class="sub">Away team</div>
          <input class="inp" id="motdAway" placeholder="Away team"/>
        </div>
      </div>

      <div class="sub" style="margin-top:6px">Kickoff (UTC ISO, e.g. 2026-06-11T19:00:00Z)</div>
      <input class="inp" id="motdKickoff" placeholder="2026-06-11T19:00:00Z"/>
    </div>
  </div>

  <div id="pollStatus" style="margin-top:12px;border-top:1px solid var(--line);padding-top:12px">
    <div class="sub">Loading poll status…</div>
  </div>

</div> <!-- end tab-menu -->




<script>
(function(){
  const qs = new URLSearchParams(location.search);

  // ✅ Venue bootstrap (scoped, no redeclare issues)
  window.VENUE = (window.VENUE || qs.get("venue") || "").trim();
  const VENUE = window.VENUE;

  const ADMIN_KEY = qs.get("key") || "";
  const ROLE = __ADMIN_ROLE__; // replaced server-side with JSON string

  const $ = (id)=>document.getElementById(id);

  function toast(msg, kind){
    const el = $("toast");
    if(!el) return;
    el.textContent = String(msg||"");
    el.setAttribute("data-kind", kind||"");
    el.classList.add("show");
    clearTimeout(toast._t);
    toast._t = setTimeout(()=>el.classList.remove("show"), 2200);
  }

  // Optional but helpful: surface missing venue immediately
  if(!VENUE){
    toast("Missing venue", "error");
  }

  function setPollStatus(html){
    const box = $("pollStatus");
    if(!box) return;
    box.innerHTML = html;
  }

  let pollStatusTimer = null;

  async function loadPollStatus(){
    try{
      setPollStatus('<div class="sub">Loading poll status…</div>');

      const res = await fetch(
        `/api/poll/state?venue=${encodeURIComponent(VENUE||"")}&_=${Date.now()}`,
        { cache: "no-store" }
      );

      const data = await res.json().catch(()=>null);
      if(!data || data.ok === false){
        setPollStatus('<div class="sub">Poll status unavailable</div>');
        return;
      }

      const locked = !!data.locked;
      const title = (data.title || "Match of the Day Poll");
      const top = (data.top && data.top.length) ? data.top : [];

      let rows = "";
      for(const r of top){
        const name = String(r.name||"");
        const votes = String(r.votes||0);
        rows += `<div class="row"><div>${escapeHtml(name)}</div><div class="mono">${escapeHtml(votes)}</div></div>`;
      }
      if(!rows) rows = '<div class="sub">No votes yet</div>';

      setPollStatus(
        `<div class="h2">${escapeHtml(title)}</div>` +
        `<div class="small">${locked ? "🔒 Locked" : "🟢 Open"}</div>` +
        `<div style="margin-top:10px;display:grid;gap:8px">${rows}</div>`
      );
    }catch(e){
      setPollStatus('<div class="sub">Poll status unavailable</div>');
    }finally{
      if(pollStatusTimer) clearTimeout(pollStatusTimer);
      pollStatusTimer = setTimeout(loadPollStatus, 5000);
    }
  }

  function escapeHtml(s){
    return String(s??"").replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  }

  async function loadMatchesForDropdown(){
    const sel = $("motdSelect");
    if(!sel) return;
    try{
      sel.disabled = true;
      const res = await fetch("/schedule.json?scope=all&q=", {cache:"no-store"});
      const data = await res.json().catch(()=>null);
      const matches = (data && Array.isArray(data.matches)) ? data.matches : [];
      // preserve current selection if possible
      const current = sel.value || "";
      sel.innerHTML = '<option value="">Select a match…</option>';
      let added = 0;
      for(const m of matches){
        if(added >= 250) break;
        const dt = String(m.datetime_utc||"");
        const home = String(m.home||"");
        const away = String(m.away||"");
        if(!dt || !home || !away) continue;

        const id = (dt + "|" + home + "|" + away).replace(/[^A-Za-z0-9|:_-]+/g,"_").slice(0,180);
        const label = `${m.date||""} ${m.time||""} • ${home} vs ${away} • ${m.venue||""}`.trim();

        const opt = document.createElement("option");
        opt.value = id;
        opt.textContent = label || (home + " vs " + away);
        opt.setAttribute("data-home", home);
        opt.setAttribute("data-away", away);
        opt.setAttribute("data-dt", dt);
        if(current && current === id) opt.selected = true;
        sel.appendChild(opt);
        added++;
      }
      sel.disabled = false;
      // If we restored selection, re-fill fields.
      if(sel.value){
        fillMatchFieldsFromOption(sel.selectedOptions[0]);
      }
    }catch(e){
      sel.disabled = false;
      toast("Couldn’t load matches", "error");
    }
  }

  function fillMatchFieldsFromOption(opt){
    if(!opt) return;
    const home = opt.getAttribute("data-home") || "";
    const away = opt.getAttribute("data-away") || "";
    const dt = opt.getAttribute("data-dt") || "";
    if($("motdHome")) $("motdHome").value = home;
    if($("motdAway")) $("motdAway").value = away;
    if($("motdKickoff")) $("motdKickoff").value = dt;
  }

               
  function safeBoot(){
    try{
      // Match dropdown
      const sel = $("motdSelect");
      if(sel){
        sel.addEventListener("change", ()=>fillMatchFieldsFromOption(sel.selectedOptions[0]));
        loadMatchesForDropdown();
      }
      // Save
      const btn = $("btnSaveConfig");
      if(btn) btn.addEventListener("click", (e)=>{ e.preventDefault(); saveFanZoneConfig(); });

      // Initial poll status
      loadPollStatus();
    }catch(e){
      // never silently die
      toast("Fan Zone UI failed to boot", "error");
      console.error(e);
    }
  }

  window.addEventListener("DOMContentLoaded", safeBoot);

  window.addEventListener("error", (e)=>{
    console.error("FanZone error:", e?.error || e?.message || e);
    toast("Fan Zone script error", "error");
  });
  window.addEventListener("unhandledrejection", (e)=>{
    console.error("FanZone rejection:", e?.reason || e);
    toast("Fan Zone script error", "error");
  });
})();


async function loadForecast(){
  const msg = qs('#forecast-msg'); if(msg) msg.textContent = 'Loading…';
  const body = qs('#forecastBody'); if(body) body.textContent = '';
  try{
    const r = await fetch(`/admin/api/analytics/load-forecast?key=${encodeURIComponent(KEY)}`, {cache:'no-store'});
    const d = await r.json();
    if(!d.ok) throw new Error(d.error || 'Failed');
    const lines = [];
    lines.push(`Last 7 days: ${d.last_7_days_total || 0} leads`);
    lines.push(`Last 30 days: ${d.last_30_days_total || 0} leads`);
    lines.push(`VIP ratio (30d): ${Math.round((d.vip_ratio_30||0)*100)}%`);
    if(Array.isArray(d.top_hours_30) && d.top_hours_30.length){
      lines.push(`Top hours (30d): ` + d.top_hours_30.map(x=>`${x.key} (${x.count})`).join(', '));
    }
    if(Array.isArray(d.top_days_7) && d.top_days_7.length){
      lines.push(`Top days (7d): ` + d.top_days_7.map(x=>`${x.key} (${x.count})`).join(', '));
    }
    if(body) body.textContent = lines.join('\\n');
    if(msg) msg.textContent = 'Updated ✔';
  }catch(e){
    if(msg) msg.textContent = 'Failed: ' + (e.message || e);
  }
}

async function replayAI(){
  const msg = qs('#replay-msg'); if(msg) msg.textContent = 'Replaying…';
  const out = qs('#replayOut'); if(out) out.textContent = '';
  const row = parseInt(qs('#replay-row')?.value || '0', 10);
  if(!row || row < 2){ if(msg) msg.textContent = 'Enter a valid sheet row # (≥2)'; return; }
  try{
    const r = await fetch(`/admin/api/ai/replay?key=${encodeURIComponent(KEY)}&venue=${encodeURIComponent(VENUE)}`, {
      method:'POST',
      headers:{'Content-Type':'application/json'},
      body: JSON.stringify({row})
    });
    const d = await r.json();
    if(!d.ok) throw new Error(d.error || 'Failed');
    if(out) out.textContent = JSON.stringify(d, null, 2);
    if(msg) msg.textContent = 'Done ✔';
  }catch(e){
    if(msg) msg.textContent = 'Failed: ' + (e.message || e);
  }
}

</script>
""".replace("__ADMIN_KEY__", json.dumps(key)).replace("__ADMIN_ROLE__", json.dumps(role)))

    html.append("</div></body></html>")
    out = make_response("".join(html))
    try:
        out.set_cookie(
            "venue_id",
            _venue_id(),
            httponly=False,
            samesite="Lax",
            path="/admin"
        )
    except Exception:
        pass
    return out



# ============================================================
# Concierge intake API (writes into Admin Leads sheet)
# ============================================================
@app.route("/api/intake", methods=["POST"])
def api_intake():
    payload = request.get_json(silent=True) or {}
    # Venue deactivation: block fan intake when the venue is inactive.
    raw_payload_venue = (payload.get("venue_id") or "").strip()
    effective_vid = _slugify_venue_id(raw_payload_venue) if raw_payload_venue else _venue_id()

    # Venue deactivation: block fan intake when the venue is inactive.
    if not _venue_is_active(effective_vid):
        return jsonify({"ok": False, "error": "Venue is inactive"}), 403



    entry_point = (payload.get("entry_point") or "").strip() or "reserve_now"
    name = (payload.get("name") or "").strip()
    contact = (payload.get("contact") or "").strip()
    business_context = (payload.get("business_context") or "").strip()
    date_s = (payload.get("date") or "").strip()
    time_s = (payload.get("time") or "").strip()
    party = (payload.get("party_size") or "").strip()
    vibe = (payload.get("vibe") or "").strip()
    budget = (payload.get("budget") or "").strip()
    notes = (payload.get("notes") or "").strip()
    lang = (payload.get("lang") or "").strip() or "en"

    # Required: name + contact + date + time + party size
    if not name or not contact or not date_s or not time_s or not party:
        return jsonify({"ok": False, "error": "Missing required fields"}), 400

    # Tier + queue (simple + reliable)
    tier = "VIP" if (entry_point == "vip_vibe" or "vip" in vibe.lower()) else "Regular"
    vip_flag = "Yes" if tier == "VIP" else "No"
    queue = "Priority" if tier == "VIP" else "Standard"

    lead = {
        "name": name,
        "phone": contact,
        "date": date_s,
        "time": time_s,
        "party_size": party,
        "language": lang,
        "status": "New",
        "vip": vip_flag,
        "entry_point": entry_point,
        "tier": tier,
        "queue": queue,
        "business_context": business_context,
        "budget": budget,
        "notes": notes,
        "vibe": vibe,
    }

    try:
        append_lead_to_sheet(lead, venue_id=effective_vid)
        _audit("intake.new", {"entry_point": entry_point, "tier": tier})
        return jsonify({"ok": True, "tier": tier})
    except Exception as e:
        return jsonify({"ok": False, "error": "Failed to store intake"}), 500


@app.route("/api/reservation/update", methods=["POST"])
def api_reservation_update():
    """Fan-facing: update an existing reservation by ID (date, time, party_size). Used by chat and optional frontend."""
    vid = _venue_id()
    if not _venue_is_active(vid):
        return jsonify({"ok": False, "error": "Venue is inactive"}), 403
    payload = request.get_json(silent=True) or {}
    rid = (payload.get("reservation_id") or "").strip()
    if not rid:
        return jsonify({"ok": False, "error": "Missing reservation_id"}), 400
    if not rid.upper().startswith("WC-"):
        rid = "WC-" + rid
    updates = {}
    if "date" in payload and str(payload.get("date") or "").strip():
        updates["date"] = str(payload["date"]).strip()
    if "time" in payload and str(payload.get("time") or "").strip():
        updates["time"] = str(payload["time"]).strip()
    if "party_size" in payload and payload.get("party_size") is not None:
        try:
            updates["party_size"] = int(payload["party_size"])
        except (TypeError, ValueError):
            pass
    if not updates:
        return jsonify({"ok": False, "error": "No valid updates (date, time, or party_size)"}), 400
    row = update_reservation_by_id(rid, updates)
    if not row:
        return jsonify({"ok": False, "error": "Reservation not found or could not be updated"}), 404
    return jsonify({"ok": True, "reservation": row, "message": "Reservation updated."})


    # ============================================================
    # Leads intake (used by the new UI)
    # - Stores locally to static/data/leads.jsonl
    # - Optionally appends to Google Sheets if configured (same creds as admin/chat)
    # ============================================================
    LEADS_STORE_PATH = os.environ.get("LEADS_STORE_PATH", "static/data/leads.jsonl")
    GOOGLE_SHEET_ID = os.environ.get("GOOGLE_SHEET_ID", "").strip()



# In-memory cache for translated greetings: (greeting_text, lang) -> translated_text. Avoids repeated LLM calls.
_greeting_translation_cache = {}

def _translate_greeting_via_llm(text: str, target_lang: str) -> str:
    """Translate greeting to target language using LLM. lang param from frontend: en, es, fr, pt.
    Use LLM to translate; return original text on failure.
    Uses OpenAI v1 API (chat.completions) when available; openai>=1.0.0 removed ChatCompletion.
    Results are cached so same (text, lang) returns instantly.
    """
    if not (text and target_lang and target_lang != "en"):
        return text or ""
    cache_key = (text, target_lang)
    if cache_key in _greeting_translation_cache:
        return _greeting_translation_cache[cache_key]
    try:
        lang_meaning = {"en": "English", "es": "Spanish", "pt": "Portuguese", "fr": "French"}
        lang_name = lang_meaning.get(target_lang, target_lang)
        messages = [
            {"role": "system", "content": (
                f"The user will send a short text. Translate it to {lang_name} only. "
                "Reply with ONLY the translation, no explanation or quotes."
            )},
            {"role": "user", "content": text},
        ]
        model = os.environ.get("CHAT_MODEL", "gpt-4o-mini")
        out = None
        try:
            from openai import OpenAI
            api_key = os.environ.get("OPENAI_API_KEY", "").strip()
            if not api_key:
                raise ValueError("OPENAI_API_KEY not set")
            v1 = OpenAI(api_key=api_key)
            r = v1.chat.completions.create(model=model, messages=messages)
            out = (r.choices[0].message.content or "").strip()
        except Exception:
            pass
        if out:
            _greeting_translation_cache[cache_key] = out
            return out
        try:
            if client is not None and hasattr(client, "responses"):
                resp = client.responses.create(model=model, input=messages)
                out = (resp.output_text or "").strip()
                if out:
                    _greeting_translation_cache[cache_key] = out
                    return out
        except Exception:
            pass
        return text
    except Exception:
        return text


@app.route("/api/venue_identity")
def api_venue_identity():
    """Fan-safe: minimal venue identity (branding-safe).

    Query params:
      - venue: venue slug (e.g. qa-sandbox)
      - lang: optional; if set and not 'en', greeting is translated to that language via LLM.

    Returns:
      - show_location_line (bool feature flag)
      - location_line (string)
      - greeting: in requested language when lang is provided
    """
    try:
        vid = _venue_id()
    except Exception:
        vid = DEFAULT_VENUE_ID

    if not _venue_is_active(vid):
        return jsonify({"ok": False, "error": "Venue is inactive"}), 404

    cfg = _venue_cfg(vid) or {}
    feat = cfg.get("features") if isinstance(cfg.get("features"), dict) else {}
    ident = cfg.get("identity") if isinstance(cfg.get("identity"), dict) else {}

    show = bool(cfg.get("show_location_line", (feat or {}).get("show_location_line", False)))
    loc = str(cfg.get("location_line") or (ident or {}).get("location_line") or "").strip()

    venue_name = str(cfg.get("name") or ident.get("venue_name") or "").strip()
    greeting = str(ident.get("greeting") or "").strip()

    req_lang = norm_lang(request.args.get("lang"))
    if req_lang != "en" and greeting:
        greeting = _translate_greeting_via_llm(greeting, req_lang)

    return jsonify({
        "ok": True,
        "venue_id": vid,
        "venue_name": venue_name,
        "greeting": greeting,
        "show_location_line": show,
        "location_line": loc,
    })

def _append_lead_local(row: dict) -> None:
    try:
        os.makedirs(os.path.dirname(LEADS_STORE_PATH), exist_ok=True)
        with open(LEADS_STORE_PATH, "a", encoding="utf-8") as f:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")
    except Exception:
        pass

def _extract_row_num_from_updated_range(updated_range: str) -> int:
    """
    Parse ranges like 'Leads!A123:K123' and return 123.
    Returns 0 if unknown.
    """
    try:
        if not updated_range:
            return 0
        if "!" in updated_range:
            updated_range = updated_range.split("!", 1)[1]
        first = updated_range.split(":", 1)[0]
        m = re.search(r"(\d+)$", first)
        return int(m.group(1)) if m else 0
    except Exception:
        return 0

def _append_lead_google_sheet(row: dict) -> tuple[bool, int]:
    """
    Append a lead to Google Sheets.
    Returns (ok, sheet_row_number).
    sheet_row_number may be 0 if it cannot be determined.
    """
    try:
        gc = get_gspread_client()
        if GOOGLE_SHEET_ID:
            sh = gc.open_by_key(GOOGLE_SHEET_ID)
        else:
            sh = _open_default_spreadsheet(gc)
        try:
            ws = sh.get_worksheet(0)
        except Exception:
            ws = sh.sheet1
        resp = ws.append_row([
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
        sheet_row = 0
        try:
            updated_range = ""
            if isinstance(resp, dict):
                updates = resp.get("updates") or {}
                updated_range = str(updates.get("updatedRange") or "")
            sheet_row = _extract_row_num_from_updated_range(updated_range)
        except Exception:
            sheet_row = 0
        # Fallback: some gspread versions return no updatedRange for append_row.
        # Resolve row by scanning recent rows for the appended lead fingerprint.
        if int(sheet_row or 0) < 2:
            try:
                rows = ws.get_all_values() or []
                ts = str(row.get("ts") or "").strip()
                contact = str(row.get("contact") or "").strip()
                dt = str(row.get("datetime") or "").strip()
                # Scan newest -> oldest; cap to last ~300 rows for speed.
                start_i = max(1, len(rows) - 300)
                for i in range(len(rows) - 1, start_i - 1, -1):
                    rr = rows[i] if i < len(rows) else []
                    if not isinstance(rr, list):
                        continue
                    r_ts = (rr[0] if len(rr) > 0 else "").strip()
                    r_contact = (rr[3] if len(rr) > 3 else "").strip()
                    r_dt = (rr[6] if len(rr) > 6 else "").strip()
                    if ts and contact and dt and r_ts == ts and r_contact == contact and r_dt == dt:
                        sheet_row = i + 1  # rows is 0-based, sheet rows are 1-based
                        break
            except Exception:
                pass
        return True, int(sheet_row or 0)
    except Exception:
        return False, 0

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

    # ------------------------------------------------------------
    # Idempotency / dedupe (prevents double submits & retries)
    # ------------------------------------------------------------
    try:
        _redis_init_if_needed()
        if globals().get("_REDIS_ENABLED") and globals().get("_REDIS"):
            vid = _venue_id() if "_venue_id" in globals() else "default"
            fp_raw = "|".join([
                vid,
                (row.get("contact") or "").lower().strip(),
                (row.get("datetime") or "").lower().strip(),
                (row.get("intent") or "").lower().strip(),
            ])[:500]
            fp = hashlib.sha256(fp_raw.encode("utf-8")).hexdigest()
            dk = f"{_REDIS_NS}:{vid}:lead_dedupe:{fp}"
            if not _REDIS.set(dk, "1", nx=True, ex=600):  # 10-minute window
                return jsonify({"ok": True, "deduped": True})
    except Exception:
        pass

    # ------------------------------------------------------------
    # Persist lead
    # ------------------------------------------------------------
    _append_lead_local(row)

    sheet_ok = False
    sheet_row = 0
    if GOOGLE_SHEET_ID or os.environ.get("GOOGLE_CREDS_JSON") or os.path.exists("google_creds.json"):
        sheet_ok, sheet_row = _append_lead_google_sheet(row)

    # ------------------------------------------------------------
    # AI triage (best-effort, non-blocking)
    # ------------------------------------------------------------
    if sheet_ok or AI_SETTINGS.get("enabled"):
        _ai_enqueue_or_apply_for_new_lead(row, int(sheet_row or 0))

    return jsonify({"ok": True, "sheet_ok": bool(sheet_ok), "sheet_row": int(sheet_row or 0)})

# ============================================================
# E2E Test Hooks (CI-safe, opt-in)
# - Disabled by default. Enable by setting E2E_TEST_MODE=1.
# - These endpoints are designed for deterministic Playwright runs (no UI flake).
# ============================================================

E2E_TEST_MODE = str(os.environ.get("E2E_TEST_MODE", "0")).strip() == "1"
E2E_TEST_TOKEN = str(os.environ.get("E2E_TEST_TOKEN", "")).strip()

def _require_e2e_test_access() -> Tuple[bool, str]:
    """Return (ok, reason). Only allows access when E2E_TEST_MODE=1 and caller is authorized."""
    if not E2E_TEST_MODE:
        return False, "E2E test mode is disabled"
    # Prefer a dedicated token for CI. Fallback: owner key can be used locally.
    token = (request.headers.get("X-E2E-Test-Token") or request.args.get("test_token") or "").strip()
    key = (request.args.get("key") or "").strip()
    if E2E_TEST_TOKEN:
        if token != E2E_TEST_TOKEN:
            return False, "Missing/invalid test token"
        return True, ""
    # If no token configured, allow OWNER key as a last resort (local-only convenience).
    if ADMIN_OWNER_KEY and key == ADMIN_OWNER_KEY:
        return True, ""
    return False, "Missing authorization (set E2E_TEST_TOKEN or pass owner ?key=)"

@app.route("/__test__/health", methods=["GET"])
def __test_health():
    """
    CI readiness probe (read-only).
    - Always returns HTTP 200 so GitHub Actions can reliably wait for the Flask server to boot.
    - Does NOT require auth/tokens; state-changing __test__ endpoints remain protected.
    """
    try:
        e2e_mode = str(os.environ.get("E2E_TEST_MODE", "0")).strip() == "1"
        token_set = bool(str(os.environ.get("E2E_TEST_TOKEN", "")).strip())
    except Exception:
        e2e_mode = False
        token_set = False

    resp = jsonify({
        "ok": True,
        "e2e_test_mode": bool(e2e_mode),
        "token_configured": bool(token_set),
        "ts": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    })
    resp.headers["Cache-Control"] = "no-store"
    return resp, 200


@app.route("/__test__/reset", methods=["POST"])
def __test_reset():
    """
    Reset minimal state for deterministic tests.
    Safe: only resets AI queue (and optionally leaves other state untouched).
    """
    ok, reason = _require_e2e_test_access()
    if not ok:
        return jsonify({"ok": False, "error": reason}), 403
    try:
        _safe_write_json_file(AI_QUEUE_FILE, [])
        return jsonify({"ok": True, "reset": ["ai_queue"]})
    except Exception as e:
        return jsonify({"ok": False, "error": f"reset failed: {e}"}), 500

@app.route("/__test__/ai_queue/seed", methods=["POST"])
def __test_ai_queue_seed():
    """
    Seed a single AI Queue item with a stable schema.
    This bypasses "AI suggestion" generation so UI tests don't depend on OpenAI/Sheets.
    """
    ok, reason = _require_e2e_test_access()
    if not ok:
        return jsonify({"ok": False, "error": reason}), 403

    body = request.get_json(silent=True) or {}
    action_type = str(body.get("type") or body.get("action_type") or "reply_draft").strip().lower()
    if action_type not in ("vip_tag", "status_update", "reply_draft"):
        return jsonify({"ok": False, "error": "Invalid type. Use vip_tag | status_update | reply_draft"}), 400

    entry = {
        "id": _queue_new_id(),
        "type": action_type,
        "title": str(body.get("title") or "CI Seed Item"),
        "details": str(body.get("details") or "Seeded by E2E test hook"),
        "status": "pending",
        "payload": body.get("payload") if isinstance(body.get("payload"), dict) else {},
        "created_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "created_by": "e2e",
    }
    try:
        _queue_add(entry)
        return jsonify({"ok": True, "id": entry["id"], "item": entry})
    except Exception as e:
        return jsonify({"ok": False, "error": f"seed failed: {e}"}), 500



# ============================================================
# Super Admin (Platform Owner) — hard isolated surface
# ============================================================

# Embedded Super Admin UI HTML (avoids template path issues in some deploys)
LEGACY_SUPER_CONSOLE_HTML = r"""<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>World Cup Concierge — Super Admin</title>
  <style>
:root{
  --bg:#070A10;
  --card:#0E1424;
  --muted:#9AA3B2;
  --text:#E9EEF7;
  --line:rgba(255,255,255,.08);

  /* dropdown / menu tokens */
  --menu-bg:#0f172a;
  --menu-hover:#1e293b;
  --menu-text:#f8fafc;
}

html,body{
  height:100%;
  background:radial-gradient(900px 700px at 20% 10%, #132752 0%, var(--bg) 55%);
  color:var(--text);
  font-family:ui-sans-serif,system-ui,-apple-system,Segoe UI,Roboto,Helvetica,Arial;
}

.wrap{max-width:1080px;margin:0 auto;padding:20px}

.top{display:flex;justify-content:space-between;align-items:center;gap:12px}
.title{font-size:18px;font-weight:700;letter-spacing:.2px}

.pill{
  font-size:12px;
  padding:6px 10px;
  border:1px solid var(--line);
  border-radius:999px;
  color:var(--muted);
  background:rgba(255,255,255,.03)
}

.grid{display:grid;grid-template-columns:repeat(3,1fr);gap:12px;margin:14px 0}

.card{
  background:linear-gradient(180deg, rgba(255,255,255,.06), rgba(255,255,255,.02));
  border:1px solid var(--line);
  border-radius:16px;
  padding:14px;
  box-shadow:0 10px 30px rgba(0,0,0,.25);
  overflow:visible;
}

.k{font-size:11px;color:var(--muted)}
.v{font-size:22px;font-weight:800;margin-top:6px}

table{width:100%;border-collapse:collapse;margin-top:12px}
th,td{
  padding:10px 8px;
  border-bottom:1px solid var(--line);
  text-align:left;
  font-size:13px
}
th{color:var(--muted);font-weight:600}

.right{text-align:right}
.err{color:#ffb4b4;font-size:12px;margin-top:10px}

/* tabs */
.tabbtn{
  background:rgba(255,255,255,.04);
  border:1px solid var(--line);
  padding:10px 12px;
  border-radius:999px;
  color:var(--text);
  cursor:pointer
}
.tabbtn.active{
  background:rgba(240,180,60,.14);
  border-color:rgba(240,180,60,.35)
}
.tabpane{display:none}
.tabpane.active{display:block}

/* scrolling tables */
.scrollbox{
  max-height:65vh;
  overflow:auto;
  border-radius:14px
}

.stickyHead thead th{
  position:sticky;
  top:0;
  background:rgba(0,0,0,.65);
  backdrop-filter: blur(10px);
  z-index:2
}

/* DROPDOWN / SELECT FIX */
select{
  background:rgba(255,255,255,.04);
  border:1px solid var(--line);
  color:#eaf0ff;
}
select option{
  background:#0f1b33 !important;
  color:#eaf0ff !important;
}
select option:hover{
  background:#1a2847 !important;
}
</style>
</head>
<body>
  <div class="wrap">
    <div class="top">
      <div>
        <div class="title">Super Admin — Global Overview</div>
        <div class="pill">Hard isolated • Read-only by default</div>
      </div>
      <div class="pill" id="ts">Loading…</div>
    </div>

    <div class="tabs" style="display:flex;gap:8px;align-items:center;margin:10px 0 6px 0;flex-wrap:wrap">
      <button class="tabbtn active" data-tabbtn="venues">Venues</button>
      <button class="tabbtn" data-tabbtn="leads">Leads</button>
    </div>

    <div class="grid">
    <div class="tabpane active" data-tab="venues">
  
    
    <div class="card" style="margin-top:12px">
      <div class="k">Venue Onboarding (creates config pack)</div>
      <div style="display:flex;gap:10px;align-items:center;margin-top:10px;flex-wrap:wrap">
        <input id="v_name" placeholder="Venue name" style="flex:1;min-width:220px;background:rgba(255,255,255,.03);border:1px solid var(--line);border-radius:12px;padding:10px;color:var(--text)">
        <input id="v_id" placeholder="Venue id (optional)" style="min-width:200px;background:rgba(255,255,255,.03);border:1px solid var(--line);border-radius:12px;padding:10px;color:var(--text)">
        <input id="v_sheet" placeholder="Google Sheet ID (optional)" style="min-width:260px;background:rgba(255,255,255,.03);border:1px solid var(--line);border-radius:12px;padding:10px;color:var(--text)">
        <select id="v_plan" style="background:rgba(255,255,255,.03);border:1px solid var(--line);border-radius:12px;padding:10px;color:var(--text)">
          <option value="standard" selected>standard</option>
          <option value="premium">premium</option>
          <option value="enterprise">enterprise</option>
        </select>
        <button class="btn" id="createVenue">Create</button>
        <button class="btn ghost" id="checkSheet">Check Sheet</button>
        <button class="btn ghost" id="refreshVenues">Refresh</button>
      </div>

      <div class="err" id="venueErr" style="display:none"></div>
      <div style="margin-top:10px;display:none" id="venueOutWrap">
        <div class="k">Generated (copy + store safely)</div>
        <div style="display:flex;gap:10px;align-items:center;flex-wrap:wrap;margin-top:10px">
          <button class="btn" id="copyPack">Copy JSON</button>
          <button class="btn ghost" id="downloadPack">Download JSON</button>
        </div>
        <pre id="venueOut" style="white-space:pre-wrap;word-break:break-word;background:rgba(0,0,0,.25);border:1px solid var(--line);border-radius:12px;padding:12px;margin-top:8px;font-size:12px;line-height:1.35"></pre>
      <div id="venueLinks" style="margin-top:10px;display:none">
          <div class="k">Quick Links</div>
          <div style="display:flex;gap:10px;flex-wrap:wrap;margin-top:8px">
            <a class="btn" id="openAdmin" target="_blank" rel="noopener">Open Admin</a>
            <a class="btn ghost" id="openManager" target="_blank" rel="noopener">Open Manager</a>
            <a class="btn ghost" id="openQR" target="_blank" rel="noopener">Open Fan Link</a>
            <button class="btn ghost" id="copyAdminUrl" type="button">Copy Admin URL</button>
            <button class="btn ghost" id="copyManagerUrl" type="button">Copy Manager URL</button>
          </div>
          <div class="note" id="venueLinksNote" style="margin-top:6px"></div>
        </div>
        </div>

      
      <div id="venueChips" style="display:flex;gap:8px;flex-wrap:wrap;margin-top:10px">
        <button class="pill" data-vchip="all">All</button>
        <button class="pill" data-vchip="active">Active</button>
        <button class="pill" data-vchip="inactive">Inactive</button>
        <button class="pill" data-vchip="needs">Needs attention</button>
        <button class="pill" data-vchip="sheetfail">Sheet fail</button>
        <button class="pill" data-vchip="notready">Not ready</button>
      </div>
<div class="scrollbox stickyHead" style="margin-top:10px"><table id="venuesTable">
        <thead>
          <tr>
            <th>Venue ID</th>
            <th>Venue</th>
            <th>Plan</th>
            <th>Sheet</th>
            <th>Status</th>
            <th class="right">Actions</th>
          </tr>
        </thead>
        <tbody id="venueRows"></tbody>
      </table>
    </div>


        </div>
    <div class="tabpane" data-tab="leads">
<div class="card" style="margin-top:12px">
      <div class="k">All Leads (cross-venue)</div>
      <div style="display:flex;gap:10px;align-items:center;margin-top:10px;flex-wrap:wrap">
        <input id="q" placeholder="Search name/phone/venue…" style="flex:1;min-width:220px;background:rgba(255,255,255,.03);border:1px solid var(--line);border-radius:12px;padding:10px;color:var(--text)">
        <select id="perPage" style="background:rgba(255,255,255,.03);border:1px solid var(--line);border-radius:12px;padding:10px;color:var(--text)">
          <option value="10" selected>10</option>
          <option value="25">25</option>
          <option value="50">50</option>
        </select>
        <button id="reload" style="background:rgba(255,255,255,.06);border:1px solid var(--line);border-radius:12px;padding:10px 12px;color:var(--text);cursor:pointer">Reload</button>
              <select id="venueFilter" class="inp" style="max-width:220px;margin-left:8px;">
                <option value="">All venues</option>
                <option value="__active__">Active venues</option>
                <option value="__inactive__">Inactive venues</option>
                <option value="__divider__" disabled>────────</option>
              </select>
              <label style="display:inline-flex;align-items:center;gap:8px;margin-left:10px;font-size:12px;color:rgba(255,255,255,0.82)"><input type="checkbox" id="demoToggle" style="transform:scale(1.1)"><span>Demo Mode</span><span id="demoBadge" style="display:none;padding:2px 8px;border-radius:999px;background:rgba(240,180,60,0.18);border:1px solid rgba(240,180,60,0.45);color:rgba(240,180,60,0.95)">ON</span></label><button id="exportCsv" class="btn" style="margin-left:8px;">Export CSV</button>
      </div>
      <table>
        <thead>
          <tr>
            <th>Venue</th>
            <th>Name</th>
            <th>Phone</th>
            <th>Date/Time</th>
            <th class="right">Party</th>
            <th>Status</th>
            <th>Tier</th>
            <th>Queue</th>
          </tr>
        </thead>
        <tbody id="leadRows"></tbody>
      </table></div>
      <div id="pager" style="display:flex;gap:8px;align-items:center;justify-content:flex-end;margin-top:10px;flex-wrap:wrap">
        <button id="prevPage" class="btn" style="opacity:.9">Prev</button>
        <div class="pill" id="pageInfo">Page 1</div>
        <button id="nextPage" class="btn" style="opacity:.9">Next</button>
      </div>
      <div class="err" id="leadErr"></div>
    </div>

        </div>
    <div class="tabpane" data-tab="venues">
<div class="card"><div class="k">Venues</div><div class="v" id="venues">—</div></div>
      <div class="card"><div class="k">AI Queue items</div><div class="v" id="aiq">—</div></div>
      <div class="card"><div class="k">Build</div><div class="v" id="build">—</div></div>
    </div>
    </div>

        <div class="tabpane active" data-tab="venues">
<div class="card">
      <div class="k">Per-venue</div>
      <div class="scrollbox stickyHead" style="margin-top:10px"><table style="width:100%;">
        <thead>
          <tr><th>Venue</th><th>Venue ID</th><th class="right">AI Queue</th></tr>
        </thead>
        <tbody id="rows"></tbody>
      </table></div>
      <div class="err" id="err"></div>
    </div>
  </div>
    </div>

<script>
// --- Demo Mode helpers (safe defaults) ---
const _demoHeaders = (enabled) => (enabled ? {"X-Demo-Mode":"1"} : {});

(async function(){
  const q = new URLSearchParams(window.location.search);
  const key = q.get("key") || "";
  const venueErr = document.getElementById("venueErr");
  const venueOutWrap = document.getElementById("venueOutWrap");
  const venueOut = document.getElementById("venueOut");

  function _slugify(s){
    s=(s||"").toLowerCase().trim();
    s=s.replace(/[^a-z0-9]+/g,'-').replace(/-{2,}/g,'-').replace(/^-|-$/g,'');
    return s||"default";
  }


  

  function esc(s){
    return String(s ?? "").replace(/[&<>"']/g, ch=>({"&":"&amp;","<":"&lt;",">":"&gt;","\"":"&quot;","'":"&#39;"}[ch]));
  }

let _venuesCache = [];
let _venueChipMode = "all";
function _venueNeedsAttention(v){
  try{
    const st = String(v.status||"").toUpperCase();
    const sheetOk = (v.sheet_ok===true);
    if(!sheetOk) return true;
    if(st.includes("FAIL") || st.includes("MISSING")) return true;
    if(st.includes("PENDING") || st.includes("NOT_READY")) return true;
    return false;
  }catch(e){ return false; }
}
function renderVenues(){
  const rows = document.getElementById("venueRows");
  rows.innerHTML = "";
  const arr = (_venuesCache||[]).slice();
  const filtered = arr.filter(v=>{
    if(_venueChipMode==="active") return !!v.active;
    if(_venueChipMode==="inactive") return !v.active;
    if(_venueChipMode==="sheetfail") return (v.sheet_ok===false);
    if(_venueChipMode==="notready") return String(v.status||"").toUpperCase().includes("PENDING") || String(v.status||"").toUpperCase().includes("NOT_READY");
    if(_venueChipMode==="needs") return _venueNeedsAttention(v);
    return true;
  });
  // issues-first sort for "needs" and default view
  filtered.sort((a,b)=>{
    const ia=_venueNeedsAttention(a)?1:0, ib=_venueNeedsAttention(b)?1:0;
    if(ia!==ib) return ib-ia;
    return String(a.venue_id||"").localeCompare(String(b.venue_id||""));
  });
  filtered.forEach(v=>{
    const tr = document.createElement("tr");
    const vid = (v.venue_id||"");
    const sheet = String(v.google_sheet_id||"").trim();
    const sheetDisp = sheet ? (sheet.length>16 ? (sheet.slice(0,8)+"…"+sheet.slice(-6)) : sheet) : "—";
    const sheetBadge = sheet
      ? `<span style="display:inline-block;padding:2px 8px;border-radius:999px;background:rgba(34,197,94,.16);border:1px solid rgba(34,197,94,.35);color:#86efac;font-size:12px;">SET</span>`
      : `<span style="display:inline-block;padding:2px 8px;border-radius:999px;background:rgba(245,158,11,.16);border:1px solid rgba(245,158,11,.35);color:#fcd34d;font-size:12px;">MISSING</span>`;
    const act = v.active ? `<span style="display:inline-block;padding:2px 8px;border-radius:999px;background:rgba(34,197,94,.16);border:1px solid rgba(34,197,94,.35);color:#86efac;font-size:12px;">ACTIVE</span>` : `<span style="display:inline-block;padding:2px 8px;border-radius:999px;background:rgba(239,68,68,.16);border:1px solid rgba(239,68,68,.35);color:#fca5a5;font-size:12px;">INACTIVE</span>`;
    const st = String(v.status||"").toUpperCase();
    const stBadge = _venueNeedsAttention(v)
      ? `<span style="display:inline-block;padding:2px 8px;border-radius:999px;background:rgba(239,68,68,.16);border:1px solid rgba(239,68,68,.35);color:#fca5a5;font-size:12px;">${esc(st||"ISSUE")}</span>`
      : `<span style="display:inline-block;padding:2px 8px;border-radius:999px;background:rgba(34,197,94,.16);border:1px solid rgba(34,197,94,.35);color:#86efac;font-size:12px;">${esc(st||"OK")}</span>`;
    const dash = `/admin?key=${encodeURIComponent(key)}&venue=${encodeURIComponent(vid)}`;
    tr.innerHTML = `
      <td style="font-family:ui-monospace,monospace">${esc(vid)}</td>
      <td>${esc(v.venue_name||vid)}</td>
      <td>${esc(v.plan||"")}</td>
      <td title="${esc(sheet)}" style="white-space:nowrap">${sheetDisp} ${sheetBadge}</td>
      <td style="white-space:nowrap">${act} ${stBadge}</td>
      <td class="right"><a class="btn ghost" href="${dash}">Open</a></td>
    `;
    rows.appendChild(tr);
  });

  // update chip labels w/ counts
  try{
    const all = (_venuesCache||[]).length;
    const active = (_venuesCache||[]).filter(v=>!!v.active).length;
    const inactive = all-active;
    const sheetfail = (_venuesCache||[]).filter(v=>v.sheet_ok===false).length;
    const notready = (_venuesCache||[]).filter(v=>String(v.status||"").toUpperCase().includes("PENDING") || String(v.status||"").toUpperCase().includes("NOT_READY")).length;
    const needs = (_venuesCache||[]).filter(v=>_venueNeedsAttention(v)).length;
    document.querySelectorAll("#venueChips .pill").forEach(btn=>{
      const mode = btn.getAttribute("data-vchip");
      let label = btn.textContent.split(" (")[0];
      if(mode==="all") label="All";
      if(mode==="active") label="Active";
      if(mode==="inactive") label="Inactive";
      if(mode==="needs") label="Needs attention";
      if(mode==="sheetfail") label="Sheet fail";
      if(mode==="notready") label="Not ready";
      const count = (mode==="all")?all:(mode==="active")?active:(mode==="inactive")?inactive:(mode==="sheetfail")?sheetfail:(mode==="notready")?notready:(mode==="needs")?needs:0;
      btn.textContent = `${label} (${count})`;
      btn.style.borderColor = (_venueChipMode===mode)? "rgba(240,180,60,.55)" : "var(--line)";
    });
  }catch(e){}
}

async function loadVenues(){
    try{
      const r = await fetch("/super/api/venues?super_key="+encodeURIComponent(super_key), {headers});
      const j = await r.json();
      const rows = document.getElementById("venueRows");
      rows.innerHTML = "";
      if(!j.ok){ throw new Error(j.error||"failed"); }

      // populate venue filter dropdown (All/Active/Inactive + per-venue)
      try{
        if(venueSel){
          // keep first 4 options (All/Active/Inactive/divider)
          const keep = Array.from(venueSel.options).slice(0,4).map(o=>({value:o.value,text:o.text,disabled:o.disabled}));
          venueSel.innerHTML = "";
          keep.forEach(o=>{
            const opt = document.createElement("option");
            opt.value = o.value; opt.textContent = o.text; opt.disabled = !!o.disabled;
            venueSel.appendChild(opt);
          });
          const active = (j.venues||[]).filter(v=>v.active);
          const inactive = (j.venues||[]).filter(v=>!v.active);
          const addGroup = (label, arr)=>{
            if(!arr.length) return;
            const og = document.createElement("optgroup");
            og.label = label;
            arr.forEach(v=>{
              const opt = document.createElement("option");
              opt.value = v.venue_id||"";
              opt.textContent = (v.venue_name||v.venue_id||"");
              og.appendChild(opt);
            });
            venueSel.appendChild(og);
          };
          addGroup("Active venues", active);
          addGroup("Inactive venues", inactive);
        }
      }catch(e){}
      _venuesCache = (j.venues||[]);
      renderVenues();
      return;
      /* legacy render removed */
      (j.venues||[]).forEach(v=>{
        const tr = document.createElement("tr");
        const vid = (v.venue_id||"");
        const sheet = String(v.google_sheet_id||"").trim();
        const sheetDisp = sheet ? (sheet.length>16 ? (sheet.slice(0,8)+"…"+sheet.slice(-6)) : sheet) : "—";
        const sheetBadge = sheet
          ? `<span style="display:inline-block;padding:2px 8px;border-radius:999px;background:rgba(34,197,94,.18);border:1px solid rgba(34,197,94,.35);color:#86efac;font-size:12px;">READY</span>`
          : `<span style="display:inline-block;padding:2px 8px;border-radius:999px;background:rgba(245,158,11,.16);border:1px solid rgba(245,158,11,.35);color:#fcd34d;font-size:12px;">MISSING SHEET</span>`;
        const sid = String(v.google_sheet_id||"").trim();
        const sidDisp = sid ? (sid.length>16 ? (sid.slice(0,8)+"…"+sid.slice(-6)) : sid) : "—";
        const st = String(v.status||"").trim();
        const statusBadge = (st==="READY")
          ? `<span style="display:inline-block;padding:2px 8px;border-radius:999px;background:rgba(34,197,94,.18);border:1px solid rgba(34,197,94,.35);color:#86efac;font-size:12px;">READY</span>`
          : (st==="SHEET_FAIL")
            ? `<span style="display:inline-block;padding:2px 8px;border-radius:999px;background:rgba(239,68,68,.16);border:1px solid rgba(239,68,68,.35);color:#fecaca;font-size:12px;">SHEET FAIL</span>`
            : (st==="MISSING_SHEET")
              ? `<span style="display:inline-block;padding:2px 8px;border-radius:999px;background:rgba(245,158,11,.16);border:1px solid rgba(245,158,11,.35);color:#fcd34d;font-size:12px;">MISSING SHEET</span>`
              : `<span style="display:inline-block;padding:2px 8px;border-radius:999px;background:rgba(148,163,184,.12);border:1px solid rgba(148,163,184,.22);color:#e2e8f0;font-size:12px;">${st||"—"}</span>`;
        tr.innerHTML = `<td>${esc(vid)}</td><td>${esc(v.venue_name||"")}</td><td>${esc(v.plan||"")}</td><td title="${esc(sid)}">${esc(sidDisp)}</td><td>${statusBadge}</td><td><button class="btn ghost" data-recheck="${esc(vid)}">Re-check</button> <button class="btn ghost" data-rotate="${esc(vid)}">Rotate Keys</button></td>`;
        rows.appendChild(tr);
      });

      // bind re-check buttons
      rows.querySelectorAll("[data-recheck]").forEach(btn=>{
        btn.addEventListener("click", async ()=>{
          const vid = btn.getAttribute("data-recheck");
          if(!vid) return;
          try{
            venueErr.style.display="none";
            const r = await fetch("/super/api/venues/check_sheet?super_key="+encodeURIComponent(super_key), {
              method:"POST",
              headers:Object.assign({"Content-Type":"application/json"}, headers),
              body: JSON.stringify({venue_id: vid})
            });
            const j2 = await r.json();
            if(!j2.ok) throw new Error(j2.error||"check failed");
            venueOut.textContent = JSON.stringify(j2, null, 2);
            venueOutWrap.style.display="block";
            await loadVenues();
          }catch(e){
            venueErr.style.display="block";
            venueErr.textContent="Re-check error: " + (e.message||e);
          }
        });
      });

      // bind rotate buttons
      rows.querySelectorAll("[data-rotate]").forEach(btn=>{
        btn.addEventListener("click", async ()=>{
          const vid = btn.getAttribute("data-rotate");
          if(!vid) return;
          if(!confirm("Rotate keys for '"+vid+"'? This will invalidate existing venue keys.")) return;
          try{
            const r = await fetch("/super/api/venues/rotate_keys?super_key="+encodeURIComponent(super_key), {
              method:"POST",
              headers:Object.assign({"Content-Type":"application/json"}, headers),
              body: JSON.stringify({venue_id: vid, rotate_admin:true, rotate_manager:true})
            });
            const j2 = await r.json();
            if(!j2.ok) throw new Error(j2.error||"rotate failed");
            venueOut.textContent = JSON.stringify(j2, null, 2);
            venueOutWrap.style.display="block";
            await loadVenues();
          }catch(e){
            venueErr.style.display="block";
            venueErr.textContent="Rotate error: " + (e.message||e);
          }
        });
      });
    }catch(e){
      venueErr.style.display="block";
      venueErr.textContent="Venue list error: " + (e.message||e);
    }
  }

  function _setVenueLinks(resp){
    try{
      const pack = (resp && (resp.pack || resp)) || {};
      const adminUrl = pack.admin_url || resp.admin_url || "";
      const managerUrl = pack.manager_url || resp.manager_url || "";
      const qrUrl = pack.qr_url || resp.qr_url || "";
      const links = document.getElementById("venueLinks");
      const note = document.getElementById("venueLinksNote");
      const aAdmin = document.getElementById("openAdmin");
      const aMgr = document.getElementById("openManager");
      const aQR = document.getElementById("openQR");
      if(aAdmin) aAdmin.href = adminUrl || "#";
      if(aMgr) aMgr.href = managerUrl || "#";
      if(aQR) aQR.href = qrUrl || "#";
      if(note) note.textContent = adminUrl ? "Admin + Manager links are ready." : "Links unavailable (older venue pack).";
      if(links) links.style.display = (adminUrl || managerUrl || qrUrl) ? "block" : "none";
    }catch(e){
      const links = document.getElementById("venueLinks");
      if(links) links.style.display="none";
    }
  }

  async function createVenue(){
    venueErr.style.display="none";
    venueOutWrap.style.display="none";
    const venue_name = (document.getElementById("v_name").value||"").trim();
    const venue_id = (document.getElementById("v_id").value||"").trim();
    const google_sheet_id = (document.getElementById("v_sheet").value||"").trim();
    const plan = (document.getElementById("v_plan").value||"standard").trim();
    if(!venue_name){
      venueErr.style.display="block";
      venueErr.textContent="Enter a venue name.";
      return;
    }
    try{
      const r = await fetch("/super/api/venues/create?super_key="+encodeURIComponent(super_key), {
        method:"POST",
        headers:Object.assign({"Content-Type":"application/json"}, headers),
        body: JSON.stringify({venue_name, venue_id, google_sheet_id, plan})
      });
      const j = await r.json();
      if(!j.ok){ throw new Error(j.error||"failed"); }
      venueOut.textContent = JSON.stringify(j, null, 2);
      venueOutWrap.style.display="block";
            _setVenueLinks(j);
      // Auto-check sheet for this venue (PASS/FAIL) when sheet id is provided
      try{
        const vid = (j.pack && j.pack.venue_id) ? j.pack.venue_id : (j.venue_id || (j.pack||{}).venue_id || "");
        const sid = (google_sheet_id || "").trim();
        if(vid && sid){
          const rc = await fetch("/super/api/venues/check_sheet?super_key="+encodeURIComponent(super_key), {
            method:"POST",
            headers:Object.assign({"Content-Type":"application/json"}, headers),
            body: JSON.stringify({venue_id: vid})
          });
          const jc = await rc.json();
          if(jc && jc.ok && jc.check){
            venueErr.style.display="block";
            venueErr.textContent = jc.check.ok ? ("Sheet PASS: " + (jc.check.title||"OK")) : ("Sheet FAIL: " + (jc.check.error||"error"));
            setTimeout(()=>{ try{ venueErr.style.display="none"; }catch(_){} }, 2500);
          }
        }
      }catch(e){}
await loadVenues();
    }catch(e){
      venueErr.style.display="block";
      venueErr.textContent="Create error: " + (e.message||e);
    }
  }

  document.getElementById("createVenue")?.addEventListener("click", createVenue);
  document.getElementById("refreshVenues")?.addEventListener("click", loadVenues);

  document.getElementById("copyPack")?.addEventListener("click", async ()=>{
    try{
      await navigator.clipboard.writeText(venueOut.textContent||"");
      venueErr.style.display="block";
      venueErr.textContent="Copied to clipboard.";
      setTimeout(()=>{ venueErr.style.display="none"; }, 1500);
    }catch(e){
      venueErr.style.display="block";
      venueErr.textContent="Copy failed: " + (e.message||e);
    }
  });

  document.getElementById("downloadPack")?.addEventListener("click", ()=>{
    try{
      const txt = venueOut.textContent||"";
      if(!txt.trim()) return;
      const blob = new Blob([txt], {type:"application/json"});
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = "venue_pack.json";
      document.body.appendChild(a);
      a.click();
      a.remove();
      setTimeout(()=>URL.revokeObjectURL(url), 1000);
    }catch(e){
      venueErr.style.display="block";
      venueErr.textContent="Download failed: " + (e.message||e);
    }
  });

  document.getElementById("checkSheet")?.addEventListener("click", async ()=>{
    venueErr.style.display="none";
    const sid = (document.getElementById("v_sheet").value||"").trim();
    if(!sid){
      venueErr.style.display="block";
      venueErr.textContent="Enter a Google Sheet ID to check.";
      return;
    }
    try{
      const r = await fetch("/super/api/sheets/check?super_key="+encodeURIComponent(super_key)+"&sheet_id="+encodeURIComponent(sid), {headers});
      const j = await r.json();
      if(!j.ok) throw new Error(j.error||"check failed");
      venueErr.style.display="block";
      venueErr.textContent="Sheet OK: " + (j.title||"opened");
      setTimeout(()=>{ ven


document.getElementById("saveSheet")?.addEventListener("click", async ()=>{
  venueErr.style.display="none";
  const vid = (document.getElementById("v_id").value||"").trim() || _slugify((document.getElementById("v_name").value||"").trim());
  const sid = (document.getElementById("v_sheet").value||"").trim();
  if(!vid){
    venueErr.style.display="block";
    venueErr.textContent="Enter a venue id (or venue name) first.";
    return;
  }
  if(!sid){
    venueErr.style.display="block";
    venueErr.textContent="Enter a Google Sheet ID first.";
    return;
  }
  try{
    const r = await fetch("/super/api/venues/set_sheet?super_key="+encodeURIComponent(super_key), {
      method:"POST",
      headers:Object.assign({"Content-Type":"application/json"}, headers),
      body: JSON.stringify({venue_id: vid, google_sheet_id: sid})
    });
    const j = await r.json();
    if(!j.ok){ throw new Error(j.error||"failed"); }
    venueErr.style.display="block";
    venueErr.textContent = "Saved sheet to venue: " + vid;
    setTimeout(()=>{ venueErr.style.display="none"; }, 1800);
    await loadVenues();
  }catch(e){
    venueErr.style.display="block";
    venueErr.textContent="Save sheet error: " + (e.message||e);
  }
});
ueErr.style.display="none"; }, 2000);
    }catch(e){
      venueErr.style.display="block";
      venueErr.textContent="Sheet check error: " + (e.message||e);
    }
  });

  const super_key = q.get("super_key") || key; // allow SUPER key as key param
  const headers = super_key ? {"X-Super-Key": super_key} : {};

  const leadRowsEl = document.getElementById("leadRows");
  const leadErrEl = document.getElementById("leadErr");
  const qEl = document.getElementById("q");
  const perPageEl = document.getElementById("perPage");
  const reloadBtn = document.getElementById("reload");

  

  const venueSel = document.getElementById("venueFilter");

  // Tabs (Venues / Leads)
  function setTab(tab){
    document.querySelectorAll(".tabbtn").forEach(b=>b.classList.toggle("active", b.getAttribute("data-tabbtn")===tab));
    document.querySelectorAll(".tabpane").forEach(p=>p.classList.toggle("active", p.getAttribute("data-tab")===tab));
    try{ localStorage.setItem("super_tab", tab);}catch(e){}
    // Ensure data loads when a tab is opened (prevents "blank Leads" on first click)
    try{
      if(tab === "leads"){
        if(typeof resetLeadsPage === "function") resetLeadsPage();
        if(typeof loadLeads === "function") loadLeads();
      }else if(tab === "venues"){
        if(typeof loadVenues === "function") loadVenues();
      }
    }catch(e){}
  }
  document.querySelectorAll(".tabbtn").forEach(b=>{
    b.addEventListener("click", ()=> setTab(b.getAttribute("data-tabbtn")));
  });
  try{
    const saved = localStorage.getItem("super_tab");
    if(saved) setTab(saved);
  }catch(e){}
  // Venue status chips
  document.querySelectorAll("#venueChips .pill").forEach(btn=>{
    btn.addEventListener("click", ()=>{
      _venueChipMode = btn.getAttribute("data-vchip")||"all";
      renderVenues();
    });
  });
  const exportBtn = document.getElementById("exportCsv");
  let __lastLeadRows = [];
  let __lastLeadMeta = {total:0,page:1,pages:1,per_page:10};
  let __leadPage = 1;

  function resetLeadsPage(){ __leadPage = 1; }

  if(reloadBtn){ reloadBtn.addEventListener("click", ()=>{ resetLeadsPage(); loadLeads(); }); }
  if(perPageEl){ perPageEl.addEventListener("change", ()=>{ resetLeadsPage(); loadLeads(); }); }
  if(venueSel){ venueSel.addEventListener("change", ()=>{ resetLeadsPage(); loadLeads(); }); }
  if(qEl){
    let __qT = null;
    qEl.addEventListener("input", ()=>{
      if(__qT) clearTimeout(__qT);
      __qT = setTimeout(()=>{ resetLeadsPage(); loadLeads(); }, 250);
    });
  }
  const prevBtn = document.getElementById("prevPage");
  const nextBtn = document.getElementById("nextPage");
  if(prevBtn){ prevBtn.addEventListener("click", ()=>{ if(__leadPage>1){ __leadPage--; loadLeads(); } }); }
  if(nextBtn){ nextBtn.addEventListener("click", ()=>{ const pages=__lastLeadMeta.pages||1; if(__leadPage < pages){ __leadPage++; loadLeads(); } }); }


  async function loadLeads(){
    if(!leadRowsEl) return;
    leadErrEl.textContent = "";
    leadRowsEl.innerHTML = "<tr><td colspan=\"8\">Loading…</td></tr>";

    const perPage = (perPageEl && perPageEl.value) ? Number(perPageEl.value) : 10;
    const query = (qEl && qEl.value || "").trim();
    const vsel = (venueSel && venueSel.value || "").trim();
    const isScope = (vsel === "__active__" || vsel === "__inactive__");
    const venue_state = isScope ? (vsel === "__active__" ? "active" : "inactive") : "all";
    const venue_id = (!isScope && vsel && vsel !== "__divider__") ? vsel : "";

    const url = "/admin/api/leads_all?key="+encodeURIComponent(super_key)+"&super_key="+encodeURIComponent(super_key)
      +"&page="+encodeURIComponent(__leadPage)
      +"&per_page="+encodeURIComponent(perPage)
      +"&venue_state="+encodeURIComponent(venue_state)
      +"&venue_id="+encodeURIComponent(venue_id)
      +"&q="+encodeURIComponent(query);

    try{
      const r = await fetch(url, {headers:_demoHeaders()});
      const j = await r.json();
      if(!j.ok) throw new Error(j.error || "forbidden");

      const rows = (j.items || j.leads || []);
      __lastLeadRows = rows;
      __lastLeadMeta = {total:j.total||rows.length, page:j.page||1, pages:j.pages||1, per_page:j.per_page||perPage};

      leadRowsEl.innerHTML = "";
      if(!rows.length){
        leadRowsEl.innerHTML = "<tr><td colspan=\"8\">No leads found.</td></tr>";
      }else{
        rows.forEach(x=>{
          const tr = document.createElement("tr");
          tr.innerHTML = `
            <td>${esc(x._venue_id||"")}</td>
            <td>${esc(x.name||"")}</td>
            <td>${esc(x.phone||"")}</td>
            <td>${esc(x.datetime||x["Date/Time"]||x.date||x.timestamp||"")}</td>
            <td class="right">${esc(x.party_size||x.party||"")}</td>
            <td>${esc(x.status||"")}</td>
            <td>${esc(x.tier||"")}</td>
            <td>${esc(x.queue||"")}</td>
          `;
          leadRowsEl.appendChild(tr);
        });
      }

      // pager
      const pg = (__lastLeadMeta.page||1);
      const pages = (__lastLeadMeta.pages||1);
      const total = (__lastLeadMeta.total||0);
      const pageInfo = document.getElementById("pageInfo");
      if(pageInfo) pageInfo.textContent = `Page ${pg} / ${pages} • ${total} total`;
      const prevBtn = document.getElementById("prevPage");
      const nextBtn = document.getElementById("nextPage");
      if(prevBtn) prevBtn.disabled = (pg<=1);
      if(nextBtn) nextBtn.disabled = (pg>=pages);

    }catch(e){
      leadRowsEl.innerHTML = "";
      leadErrEl.textContent = "Error: " + (e.message||e);
    }
  }

  
  if(exportBtn){
    exportBtn.addEventListener("click", async ()=>{
      if(__demo){ try{ leadErrEl.textContent = "Demo Mode: export disabled."; }catch(_){} return; }
      try{
        const query = (qEl && qEl.value || "").trim();
        const vsel = (venueSel && venueSel.value || "").trim();
        const isScope = (vsel === "__active__" || vsel === "__inactive__");
        const venue_state = isScope ? (vsel === "__active__" ? "active" : "inactive") : "all";
        const venue_id = (!isScope && vsel && vsel !== "__divider__") ? vsel : "";

        const url = "/admin/api/leads_all?key="+encodeURIComponent(super_key)
          +"&super_key="+encodeURIComponent(super_key)
          +"&limit=5000"
          +"&venue_state="+encodeURIComponent(venue_state)
          +"&venue_id="+encodeURIComponent(venue_id)
          +"&q="+encodeURIComponent(query);

        const r = await fetch(url, {headers:_demoHeaders()});
        const j = await r.json();
        if(!j.ok) throw new Error(j.error || "forbidden");
        const rows = (j.items || j.leads || []);

        const headersCsv = ["venue_id","name","phone","datetime","party_size","status","tier","queue"];
        const escCsv = (v)=>(""+(v??"")).replace(/\r?\n/g," ").replace(/"/g,'""');
        const lines = [];
        lines.push(headersCsv.map(h=>'"'+escCsv(h)+'"').join(","));
        (rows||[]).forEach(x=>{
          const vals = [
            x._venue_id||"",
            x.name||"",
            x.phone||"",
            x.datetime||x["Date/Time"]||x.date||x.timestamp||"",
            x.party_size||x.party||"",
            x.status||"",
            x.tier||"",
            x.queue||"",
          ];
          lines.push(vals.map(v=>'"'+escCsv(v)+'"').join(","));
        });
        const csv = lines.join("\n");
        const blob = new Blob([csv], {type:"text/csv;charset=utf-8"});
        const urlObj = URL.createObjectURL(blob);
        const a = document.createElement("a");
        a.href = urlObj;
        a.download = "super_leads_export.csv";
        document.body.appendChild(a);
        a.click();
        a.remove();
        setTimeout(()=>URL.revokeObjectURL(urlObj), 2000);
      }catch(e){
        try{ leadErrEl.textContent = "Export failed: " + (e.message||e); }catch(_){}
      }
    });
  }
if(qEl){
    qEl.addEventListener("input", ()=>loadLeads());
  }
  if(perPageEl){
    perPageEl.addEventListener("change", ()=>loadLeads());
  }

  
  if(venueSel){ venueSel.addEventListener("change", ()=>loadLeads()); }
try{
    const r = await fetch("/super/api/overview?super_key="+encodeURIComponent(super_key), {headers:_demoHeaders()});
    const j = await r.json();
    if(!j.ok) throw new Error(j.error || "forbidden");
    document.getElementById("venues").textContent = (j.total && j.total.venues) || 0;
    document.getElementById("aiq").textContent = (j.total && j.total.ai_queue) || 0;
    const tb = document.getElementById("rows");
    tb.innerHTML = "";
    (j.venues || []).forEach(v=>{
      const tr = document.createElement("tr");
      tr.innerHTML = `<td>${(v.venue_name||"")}</td><td>${(v.venue_id||"")}</td><td class="right">${(v.ai_queue||0)}</td>`;
      tb.appendChild(tr);
    });
    document.getElementById("ts").textContent = new Date().toLocaleString();
  }catch(e){
    document.getElementById("err").textContent = "Error: " + (e.message||e);
  }
  await loadVenues();
  await loadLeads();
  try{
    const b = await fetch("/super/api/diag?super_key="+encodeURIComponent(super_key), {headers:_demoHeaders()});
    const bj = await b.json();
    document.getElementById("build").textContent = (bj.app_version || bj.app_version_env || "—");
  }catch(e){}
})();
</script>
</body>
</html>
"""

# Super Admin UI (Option A command layout)
SUPER_CONSOLE_HTML_OPTIONA = r"""<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>World Cup Concierge — Super Admin</title>
  <style>
:root{
  --bg0:#05070c;
  --bg1:#0a1020;
  --card:rgba(255,255,255,.06);
  --stroke:rgba(255,255,255,.10);
  --text:#eef2ff;
  --muted:rgba(238,242,255,.65);
  --good:#36d399;
  --warn:#fbbf24;
  --bad:#fb7185;

  /* dropdown / menu tokens */
  --menu-bg:#0f172a;
  --menu-hover:#1e293b;
  --menu-text:#f8fafc;
}

html,body{
  height:100%;
  margin:0;
  background:
    radial-gradient(1200px 700px at 15% 10%, #10224a 0%, transparent 55%),
    linear-gradient(180deg,var(--bg1),var(--bg0));
  color:var(--text);
  font-family:ui-sans-serif,system-ui,-apple-system,Segoe UI,Roboto,Helvetica,Arial;
}

a{color:#a7c7ff}

.wrap{max-width:1240px;margin:0 auto;padding:18px 16px 36px;}

.topbar{
  position:sticky;
  top:0;
  z-index:20;
  backdrop-filter:blur(16px);
  background:linear-gradient(180deg, rgba(5,7,12,.92), rgba(5,7,12,.55));
  border:1px solid var(--stroke);
  border-radius:18px;
  padding:14px;
  box-shadow:0 12px 40px rgba(0,0,0,.45);
}

.toprow{display:flex;gap:12px;align-items:center;justify-content:space-between;flex-wrap:wrap;}

.title{display:flex;gap:10px;align-items:baseline;}
.title h1{font-size:18px;margin:0;}
.title small{color:var(--muted)}

.tabs{display:flex;gap:8px;}
.tabbtn{
  border:1px solid var(--stroke);
  background:rgba(255,255,255,.04);
  color:var(--text);
  padding:7px 12px;
  border-radius:999px;
  cursor:pointer;
  font-size:13px;
}
.tabbtn.active{
  background:rgba(255,255,255,.10);
  border-color:rgba(255,255,255,.18);
}

.chips{display:flex;gap:8px;flex-wrap:wrap;align-items:center;}
.chip{
  display:inline-flex;
  gap:8px;
  align-items:center;
  border:1px solid var(--stroke);
  background:rgba(255,255,255,.04);
  padding:7px 10px;
  border-radius:999px;
  cursor:pointer;
  font-size:12px;
}
.chip.active{background:rgba(255,255,255,.10);}

.dot{width:8px;height:8px;border-radius:999px;background:#94a3b8;}
.pill{
  padding:5px 9px;
  border:1px solid rgba(255,255,255,.10);
  border-radius:999px;
  font-size:12px;
  color:var(--muted);
}

.meta{display:flex;gap:10px;align-items:center;color:var(--muted);font-size:12px;}

.grid{display:grid;grid-template-columns:340px 1fr;gap:14px;margin-top:14px;}

.card{
  border:1px solid var(--stroke);
  background:var(--card);
  border-radius:18px;
  padding:12px;
  box-shadow:0 12px 40px rgba(0,0,0,.35);
  overflow:visible;
}

.card h2{margin:0 0 8px 0;font-size:13px;color:var(--muted);font-weight:600;}

.btn{
  border:1px solid var(--stroke);
  background:rgba(255,255,255,.06);
  color:var(--text);
  padding:8px 10px;
  border-radius:10px;
  cursor:pointer;
  font-size:13px;
}
.btn.primary{
  background:rgba(96,165,250,.18);
  border-color:rgba(96,165,250,.28);
}

/* INPUTS + SELECT FIX */
input,select{
  background:rgba(0,0,0,.28);
  border:1px solid var(--stroke);
  color:#eaf0ff;
  border-radius:10px;
  padding:8px 10px;
  font-size:13px;
}
input::placeholder{
  color:rgba(234,240,255,.45);
}
select option{
  background:#0f1b33 !important;
  color:#eaf0ff !important;
}
select option:hover{
  background:#1a2847 !important;
}

.rail{display:flex;flex-direction:column;gap:10px;}
.rail .actions{display:flex;gap:8px;}
.rail .list{
  border:1px solid var(--stroke);
  border-radius:14px;
  overflow:auto;
  max-height:62vh;
  background:rgba(0,0,0,.18);
}

.vrow{
  display:flex;
  gap:10px;
  align-items:center;
  padding:10px;
  border-bottom:1px solid rgba(255,255,255,.06);
  cursor:pointer;
}
.vrow:hover{background:rgba(255,255,255,.05);}
.vrow.active{background:rgba(255,255,255,.09);}

.vname{font-weight:600;font-size:13px;}
.vid{font-size:12px;color:var(--muted);}

.badges{display:flex;gap:6px;margin-left:auto;}
.badge{
  font-size:11px;
  padding:3px 7px;
  border-radius:999px;
  border:1px solid rgba(255,255,255,.10);
  background:rgba(255,255,255,.05);
  color:var(--muted);
}
.badge.good{border-color:rgba(54,211,153,.35);color:rgba(54,211,153,.95);}
.badge.warn{border-color:rgba(251,191,36,.35);color:rgba(251,191,36,.95);}
.badge.bad{border-color:rgba(251,113,133,.35);color:rgba(251,113,133,.95);}

.main{display:flex;flex-direction:column;gap:14px;}

.panel{
  border:1px solid var(--stroke);
  background:var(--card);
  border-radius:18px;
  overflow:visible;
}

.panelhead{
  display:flex;
  align-items:center;
  justify-content:space-between;
  padding:12px;
  border-bottom:1px solid rgba(255,255,255,.08);
}
.panelhead .left{display:flex;gap:10px;align-items:center;flex-wrap:wrap;}
.panelhead .right{display:flex;gap:8px;align-items:center;}

.tablewrap{max-height:52vh;overflow:auto;}

table{width:100%;border-collapse:collapse;font-size:13px;}
th,td{
  padding:10px;
  border-bottom:1px solid rgba(255,255,255,.06);
  text-align:left;
  vertical-align:top;
}
th{
  position:sticky;
  top:0;
  z-index:2;
  background:rgba(0,0,0,.35);
  color:var(--muted);
  font-weight:600;
  font-size:12px;
}

.muted{color:var(--muted)}

.section{display:none;}
.section.active{display:block;}

.diag{
  white-space:pre-wrap;
  font-family:ui-monospace,SFMono-Regular,Menlo,Monaco,Consolas,monospace;
  font-size:12px;
  color:rgba(238,242,255,.78);
  background:rgba(0,0,0,.28);
  border:1px solid rgba(255,255,255,.10);
  border-radius:12px;
  padding:10px;
  max-height:170px;
  overflow:auto;
}

.pager{
  display:flex;
  gap:8px;
  align-items:center;
  justify-content:flex-end;
  padding:10px 12px;
  border-top:1px solid rgba(255,255,255,.08);
  background:rgba(0,0,0,.18);
}
</style>
</head>
<body>
<div class="wrap">
  <div class="topbar">
    <div class="toprow">
      <div class="title">
        <h1>Super Admin</h1>
        <small>Command view • bounded panels • problems float to the top</small>
      </div>
      <div class="tabs">
        <button class="tabbtn active" id="tabVenues" type="button">Venues</button>
        <button class="tabbtn" id="tabLeads" type="button">Leads</button>
      </div>
      <div class="chips" id="healthChips">
        <div class="chip active" data-filter="all"><span class="dot"></span>All <span class="pill" id="c_all">—</span></div>
        <div class="chip" data-filter="active"><span class="dot" style="background:var(--good)"></span>Active <span class="pill" id="c_active">—</span></div>
        <div class="chip" data-filter="inactive"><span class="dot" style="background:var(--warn)"></span>Inactive <span class="pill" id="c_inactive">—</span></div>
        <div class="chip" data-filter="needs"><span class="dot" style="background:var(--bad)"></span>Needs attention <span class="pill" id="c_needs">—</span></div>
        <div class="chip" data-filter="sheetfail"><span class="dot" style="background:var(--bad)"></span>Sheet fail <span class="pill" id="c_sheetfail">—</span></div>
        <div class="chip" data-filter="notready"><span class="dot" style="background:var(--warn)"></span>Not ready <span class="pill" id="c_notready">—</span></div>
      </div>
      <div class="meta"><span id="ts">—</span><span>Build</span><strong id="build">—</strong></div>
    </div>
  </div>

  <div class="grid">
    <div class="card rail" id="venuesRailCard">
      <h2>Venue control rail</h2>
      <div class="actions">
        <button class="btn primary" id="btnCreate" type="button">+ Create venue</button>
        <button class="btn" id="btnRefresh" type="button">Refresh</button>
      </div>
      <input id="venueSearch" placeholder="Search venue name/id…" />
      <div class="list" id="venuesRail"><div class="vrow"><div class="vname">Loading venues…</div></div></div>
      <div class="muted" style="font-size:12px">Tip: click a venue → details + actions on the right.</div>
    </div>

    <div class="main">
      <div class="card">
        <h2>Venue details + quick actions</h2>

        <!-- REPLACED: static, always-present identity editor (JS will also render richer details) -->
        <div id="venueDetails">
          <div class="muted" style="font-size:12px;margin-bottom:8px">
            Select a venue from the left rail to edit identity.
          </div>

          <div class="muted" style="font-size:12px;margin-top:8px">Fan subtitle (location line)</div>
          <input id="saLocationLine" placeholder="Dallas, TX" style="width:100%;margin-top:6px" />

          <div style="margin-top:10px;display:flex;gap:8px;align-items:center;flex-wrap:wrap">
            <button class="btn primary" id="btnSaveIdentity" type="button">Save</button>
            <span class="muted" style="font-size:12px">Saves to venue config via set_identity.</span>
          </div>
        </div>
      </div>

      <div class="card" style="margin-top:12px">
        <h2>Diagnostics</h2>
        <pre id="diagBox" style="white-space:pre-wrap;word-break:break-word;min-height:44px;margin:0" class="muted">—</pre>
      </div>

      <div class="panel section active" id="sectionVenues">
        <div class="panelhead">
          <div class="left"><strong style="font-size:13px">Venues status</strong><span class="muted" style="font-size:12px">Issues first • bounded table</span></div>
          <div class="right">
            <span class="muted" style="font-size:12px">Filter:</span>
            <select id="venuesFilter">
              <option value="all">All</option><option value="active">Active</option><option value="inactive">Inactive</option>
              <option value="needs">Needs attention</option><option value="sheetfail">Sheet fail</option><option value="notready">Not ready</option>
            </select>
          </div>
        </div>
        <div class="tablewrap">
          <table>
            <thead><tr><th>Venue</th><th>Plan</th><th>Sheet</th><th>Status</th><th>Actions</th></tr></thead>
            <tbody id="venuesTbody"><tr><td colspan="5" class="muted">Loading…</td></tr></tbody>
          </table>
        </div>
      </div>

      <div class="panel section" id="sectionLeads">
        <div class="panelhead">
          <div class="left">
            <strong style="font-size:13px">All Leads (cross-venue)</strong>
            <input id="leadsSearch" placeholder="Search name/phone/venue…" style="min-width:220px" />
            <select id="leadsPerPage"><option value="10">10 / page</option><option value="25">25 / page</option><option value="50">50 / page</option></select>
            <select id="leadsVenueState"><option value="all">All venues</option><option value="active">Active venues</option><option value="inactive">Inactive venues</option></select>
            <select id="leadsVenueId"><option value="">All venues</option></select>
          </div>
          <div class="right">
            <button class="btn" id="btnReloadLeads" type="button">Reload</button>
            <button class="btn" id="btnExportCsv" type="button">Export CSV</button>
          </div>
        </div>
        <div class="tablewrap">
          <table>
            <thead><tr><th>Venue</th><th>Name</th><th>Phone</th><th>Date/Time</th><th>Party</th><th>Status</th><th>Tier</th><th>Queue</th></tr></thead>
            <tbody id="leadsTbody"><tr><td colspan="8" class="muted">Click Leads to load…</td></tr></tbody>
          </table>
        </div>
        <div class="pager">
          <span class="muted" id="leadsCount">—</span>
          <button class="btn" id="btnPrev" type="button">Prev</button>
          <button class="btn" id="btnNext" type="button">Next</button>
        </div>
        <div class="card" style="margin:12px"><h2>Diagnostics</h2><div id="leadsDiag" class="diag">—</div></div>
      </div>
    </div>
  </div>
</div>

<script>
(function(){
  const qs = new URLSearchParams(location.search);
  const super_key =
    (qs.get('super_key') || qs.get('key') || '').trim() ||
    (document.cookie.match(/(?:^|;)\s*super_key=([^;]+)/)?.[1]
      ? decodeURIComponent(document.cookie.match(/(?:^|;)\s*super_key=([^;]+)/)[1])
      : '');

  const state = {venues:[], filter:'all', selected:'', leadsPage:1, leadsTotal:0};

  let demoEnabled = (document.cookie||'').includes('demo_mode=1');

  function setDiag(s){
    const el = document.getElementById('diagBox') || document.getElementById('leadsDiag');
    if(el) el.textContent = String(s||'');
  }

  // ---- NEW: Save Identity (location_line) ----
  // Uses event delegation so it works even if #venueDetails is re-rendered dynamically.
  async function saveIdentityFromUI(){
    try{
      // Resolve currently selected venue id (state.selected is used elsewhere in this console)
      const venue_id = (state.selected || '').trim();
      if(!venue_id){
        setDiag('Select a venue first');
        return;
      }

      const inp = document.getElementById('saLocationLine');
      const location_line = (inp && inp.value ? String(inp.value) : '').trim();
      if(!location_line){
        setDiag('Enter a location line');
        return;
      }

      const payload = { venue_id, location_line, show_location_line: true };

      const r = await fetch('/super/api/venues/set_identity?super_key='+encodeURIComponent(super_key), {
        method: 'POST',
        headers: hdrs(),
        body: JSON.stringify(payload)
      });

      const j = await r.json().catch(()=>({}));
      if(!j || !j.ok){
        const msg = (j && j.error) ? j.error : ('HTTP ' + r.status);
        setDiag('Save failed: ' + msg);
        return;
      }

      setDiag('Saved identity ✔');

      // Refresh UI (these functions exist later in this script)
      try{ await loadVenues(); }catch(e){}
      try{ if(typeof renderVenueDetails === 'function') renderVenueDetails(); }catch(e){}
    }catch(e){
      setDiag('Save failed: ' + (e && e.message ? e.message : e));
    }
  }

  // Delegated click binding (works even if button is injected later)
  document.addEventListener('click', (ev)=>{
    const t = ev && ev.target;
    if(t && t.id === 'btnSaveIdentity'){
      ev.preventDefault();
      saveIdentityFromUI();
    }
  });

  async function toggleDemoMode(){
    try{
      const next = !demoEnabled;
      const r = await fetch('/super/api/demo_mode?super_key='+encodeURIComponent(super_key), {
        method:'POST',
        headers: hdrs(),
        body: JSON.stringify({enabled: next})
      });
      const j = await r.json().catch(()=>({}));
      if(!j.ok) throw new Error(j.error||('HTTP '+r.status));
      demoEnabled = !!j.enabled;
      setDiag('demo_mode='+(demoEnabled?'ON':'OFF'));
      await loadVenues();
      renderVenueDetails();
    }catch(e){
      alert('Demo mode failed: '+(e.message||e));
    }
  }

  function buildAdminUrl(v){
    const k = (v && v.admin_key) ? v.admin_key : '';
    const vid = (v && v.venue_id) ? v.venue_id : '';
    return '/admin?key='+encodeURIComponent(k)+'&venue='+encodeURIComponent(vid);
  }
  function hesc(s){s=(s===null||s===undefined)?'':String(s);return s.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;').replace(/'/g,'&#39;');}
  function hdrs(extra){const h={'Content-Type':'application/json'}; if(super_key) h['X-Super-Key']=super_key; if(demoEnabled) h['X-Demo-Mode']='1'; if(extra) Object.assign(h,extra); return h;}

  function setActiveTab(which){
    document.getElementById('tabVenues').classList.toggle('active', which==='venues');
    document.getElementById('tabLeads').classList.toggle('active', which==='leads');
    document.getElementById('sectionVenues').classList.toggle('active', which==='venues');
    document.getElementById('sectionLeads').classList.toggle('active', which==='leads');
    if(which==='leads'){ state.leadsPage=1; loadLeads(); }
  }
  function applyChipFilter(f){
    state.filter=f;
    document.querySelectorAll('#healthChips .chip').forEach(el=>el.classList.toggle('active', el.dataset.filter===f));
    document.getElementById('venuesFilter').value = (f==='needs'||f==='sheetfail'||f==='notready'||f==='active'||f==='inactive') ? f : 'all';
    renderVenues();
  }
  function venueFlags(v){
    const active = (typeof v.active==='boolean') ? v.active : true;
    const sid = (v && v.sheet && v.sheet.sheet_id) ? String(v.sheet.sheet_id) : '';
    const okRaw = (v && v.sheet) ? v.sheet.ok : null; // can be true | false | null/undefined
    const sheet_ok = (okRaw === true) ? true : (okRaw === false ? false : null);
    const ready = (typeof v.ready === 'boolean') ? v.ready : !!v.ready;
    // sheet_state: missing | unknown | ok | fail
    const sheet_state = (!sid) ? 'missing' : (sheet_ok === true ? 'ok' : (sheet_ok === false ? 'fail' : 'unknown'));
    const needs = active && (sheet_state !== 'ok' || !ready);
    return {active, sheet_ok, ready, needs, sheet_state, sid};
  }
  function computeCounts(){
    const vs=(state.venues||[]).filter(v=>!v.deleted); let all=vs.length, active=0, inactive=0, sheetfail=0, notready=0, needs=0;
    vs.forEach(v=>{const f=venueFlags(v); if(f.active) active++; else inactive++; if(!f.sheet_ok) sheetfail++; if(!f.ready) notready++; if(f.needs) needs++;});
    document.getElementById('c_all').textContent=String(all);
    document.getElementById('c_active').textContent=String(active);
    document.getElementById('c_inactive').textContent=String(inactive);
    document.getElementById('c_sheetfail').textContent=String(sheetfail);
    document.getElementById('c_notready').textContent=String(notready);
    document.getElementById('c_needs').textContent=String(needs);
  }
  function matchesFilter(v){
    const f=state.filter; const x=venueFlags(v);
    if(f==='active') return x.active;
    if(f==='inactive') return !x.active;
    if(f==='sheetfail') return !x.sheet_ok;
    if(f==='notready') return !x.ready;
    if(f==='needs') return x.needs;
    return true;
  }
  function matchesSearch(v){
    const q=(document.getElementById('venueSearch').value||'').trim().toLowerCase();
    if(!q) return true;
    return String(v.venue_id||'').toLowerCase().includes(q) || String(v.name||v.venue_name||'').toLowerCase().includes(q);
  }

  function renderVenues(){
    const list=document.getElementById('venuesRail');
    const tbody=document.getElementById('venuesTbody');
    const filtered=(state.venues||[]).filter(v=>!v.deleted && matchesFilter(v) && matchesSearch(v));

    filtered.sort((a,b)=>{
      const fa=venueFlags(a), fb=venueFlags(b);
      const sa=(fa.needs?0:1)+(!fa.sheet_ok?0:1)+(!fa.ready?0:1)+(fa.active?0:1);
      const sb=(fb.needs?0:1)+(!fb.sheet_ok?0:1)+(!fb.ready?0:1)+(fb.active?0:1);
      if(sa!==sb) return sa-sb;
      return String(a.venue_id||'').localeCompare(String(b.venue_id||''));
    });

    list.innerHTML='';
    if(!filtered.length){
      list.innerHTML='<div class="vrow"><div class="vname">No venues match.</div></div>';
    } else {
      filtered.forEach(v=>{
        const f=venueFlags(v);
        const sheetBadgeTxt = (f.sheet_state==='missing') ? 'SHEET NOT SET' : (f.sheet_state==='unknown' ? 'SHEET UNCHECKED' : (f.sheet_state==='ok' ? 'SHEET OK' : 'SHEET FAIL'));
        const sheetBadgeCls = (f.sheet_state==='ok') ? 'good' : (f.sheet_state==='missing' || f.sheet_state==='unknown') ? 'warn' : 'bad';
        const badges=[
          '<span class="badge '+(f.active?'good':'warn')+'">'+(f.active?'ACTIVE':'INACTIVE')+'</span>',
          '<span class="badge '+sheetBadgeCls+'">'+sheetBadgeTxt+'</span>',
          '<span class="badge '+(f.ready?'good':'warn')+'">'+(f.ready?'READY':'NOT READY')+'</span>'
        ];
        const el=document.createElement('div');
        el.className='vrow'+(state.selected===v.venue_id?' active':'');
        el.innerHTML='<div><div class="vname">'+hesc(v.name||v.venue_name||v.venue_id)+'</div><div class="vid">'+hesc(v.venue_id||'')+'</div></div><div class="badges">'+badges.join('')+'</div>';
        el.onclick=()=>{state.selected=v.venue_id; renderVenues(); renderVenueDetails();};
        list.appendChild(el);
      });
    }

    tbody.innerHTML='';
    if(!filtered.length){
      tbody.innerHTML='<tr><td colspan="5" class="muted">No venues to show.</td></tr>';
    } else {
      filtered.forEach(v=>{
        const f=venueFlags(v);
        const sheet=v.sheet||{};
        const sheetTxt = sheet && sheet.sheet_id ? (String(sheet.sheet_id).slice(0,8)+'…') : '—';
        const sheetBadge='<span class="badge '+((f.sheet_state==='ok')?'good':((f.sheet_state==='missing'||f.sheet_state==='unknown')?'warn':'bad'))+'">'+((f.sheet_state==='ok')?'OK':((f.sheet_state==='missing')?'NOT SET':((f.sheet_state==='unknown')?'UNCHECKED':'FAIL')))+'</span>';
        const actBadge='<span class="badge '+(f.active?'good':'warn')+'">'+(f.active?'ACTIVE':'INACTIVE')+'</span>';
        const readyBadge='<span class="badge '+(f.ready?'good':'warn')+'">'+(f.ready?'READY':'NOT READY')+'</span>';
        const actions='<button class="btn" data-act="check" data-vid="'+hesc(v.venue_id||'')+'">Re-check</button> '+
                      '<button class="btn" data-act="rotate" data-vid="'+hesc(v.venue_id||'')+'">Rotate Keys</button> '+
                      '<a href="'+buildAdminUrl(v)+'" target="_blank">Open</a> '+
                      '<button class="btnTiny" data-act="delete" data-vid="'+hesc(v.venue_id||'')+'" title="Delete venue">🗑</button>';
        const tr=document.createElement('tr');
        tr.innerHTML='<td><div><div style="font-weight:700">'+hesc(v.name||v.venue_name||v.venue_id)+'</div><div class="muted">'+hesc(v.venue_id||'')+'</div></div></td>'+
                     '<td>'+hesc(v.plan||'standard')+'</td>'+
                     '<td>'+hesc(sheetTxt)+' '+sheetBadge+'</td>'+
                     '<td>'+actBadge+' '+readyBadge+'</td>'+
                     '<td>'+actions+'</td>';
        tbody.appendChild(tr);
      });
    }

    const sel=document.getElementById('leadsVenueId');
    const cur=sel.value;
    sel.innerHTML='<option value="">All venues</option>'+(state.venues||[]).filter(v=>!v.deleted).map(v=>'<option value="'+hesc(v.venue_id||'')+'">'+hesc(v.name||v.venue_id)+'</option>').join('');
    if([].slice.call(sel.options).some(o=>o.value===cur)) sel.value=cur;

    computeCounts();
  }

  function renderVenueDetails(){
  const box = document.getElementById('venueDetails');
  const v = (state.venues || []).find(x => x.venue_id === state.selected);

  if(!v){
    box.textContent = 'Select a venue from the left rail.';
    return;
  }

  const f = venueFlags(v);
  const sheet = v.sheet || {};

  box.innerHTML =
    '<div><strong>'+hesc(v.name||v.venue_id)+'</strong> <span class="muted">('+hesc(v.venue_id||'')+')</span></div>'+

    '<div style="margin-top:8px; display:flex; gap:6px; flex-wrap:wrap">'+
      '<span class="badge '+(f.active?'good':'warn')+'">'+(f.active?'ACTIVE':'INACTIVE')+'</span>'+
      '<span class="badge '+((f.sheet_state==='ok')?'good':((f.sheet_state==='missing'||f.sheet_state==='unknown')?'warn':'bad'))+'">'+((f.sheet_state==='ok')?'SHEET OK':((f.sheet_state==='missing')?'SHEET NOT SET':((f.sheet_state==='unknown')?'SHEET UNCHECKED':'SHEET FAIL')))+'</span>'+
      '<span class="badge '+(f.ready?'good':'warn')+'">'+(f.ready?'READY':'NOT READY')+'</span>'+
    '</div>'+

    '<div class="muted" style="margin-top:8px; font-size:12px">'+
      hesc(sheet.title || sheet.error || '')+
    '</div>'+

    // 🔹 NEW: Fan-facing subtitle editor (location_line)
    '<div class="muted" style="margin-top:12px; font-size:12px">Fan subtitle (location line)</div>'+
    '<input id="saLocationLine" style="width:100%; margin-top:6px" placeholder="Dallas, TX" value="'+hesc(v.location_line||'')+'"/>'+

    '<div style="margin-top:10px; display:flex; gap:8px; flex-wrap:wrap">'+
      '<button class="btn primary" id="btnSaveIdentity">Save</button>'+
      '<button class="btn" id="vdActive">'+(f.active?'Deactivate':'Activate')+'</button>'+
      '<button class="btn" id="vdDemo">Demo Mode: '+(demoEnabled?'ON':'OFF')+'</button>'+
      '<button class="btn" id="vdCheck">Re-check Sheet</button>'+
      '<button class="btn" id="vdRotate">Rotate Keys</button>'+
      '<button class="btn" id="vdSetSheet">Set Sheet…</button>'+
      '<button class="btn" id="vdDelete" style="margin-left:auto;background:rgba(248,113,113,.12);border-color:rgba(248,113,113,.6)">Delete venue</button>'+
      '<a class="btn" style="text-decoration:none" href="'+buildAdminUrl(v)+'" target="_blank">Open Admin</a>'+
    '</div>';

  // existing actions
  document.getElementById('vdCheck').onclick = () => doVenueAction('check', v.venue_id);
  document.getElementById('vdRotate').onclick = () => doVenueAction('rotate', v.venue_id);

  const _vdA = document.getElementById('vdActive');
  if(_vdA) _vdA.onclick = () => doVenueAction('set_active', v.venue_id, {active: !f.active});

  const _vdD = document.getElementById('vdDemo');
  if(_vdD) _vdD.onclick = () => toggleDemoMode();

  const _vdDel = document.getElementById('vdDelete');
  if(_vdDel){
    _vdDel.onclick = async () => {
      const name = v.name || v.venue_id || '';
      if(!confirm('Delete venue "'+name+'" from Super Admin? This will disable intake and hide it from the console.')) return;
      await doVenueAction('delete', v.venue_id);
    };
  }

  document.getElementById('vdSetSheet').onclick = async () => {
    const sid = prompt('Paste Google Sheet ID for '+(v.venue_id||''), (sheet.sheet_id||''));
    if(sid === null) return;
    let s = sid.trim();
    const m = (s.match(/\/spreadsheets\/d\/([a-zA-Z0-9-_]+)/) || []);
    if(m[1]) s = m[1];
    await doVenueAction('set_sheet', v.venue_id, {sheet_id: s});
  };
}

  async function doVenueAction(act, venue_id, extra){
    const vid=(venue_id||'').trim(); if(!vid) return;
    let url=''; let payload=Object.assign({venue_id:vid}, extra||{});
    if(act==='check') url='/super/api/venues/check_sheet';
    if(act==='rotate') url='/super/api/venues/rotate_keys';
    if(act==='set_sheet') url='/super/api/venues/set_sheet';
    if(act==='set_active') url='/super/api/venues/set_active';
    if(act==='delete') url='/super/api/venues/delete';
    if(!url) return;
    try{
      const r=await fetch(url+'?super_key='+encodeURIComponent(super_key), {method:'POST', headers: hdrs(), body: JSON.stringify(payload)});
      const j=await r.json().catch(()=>({}));
      if(!j.ok){ setDiag(JSON.stringify(j||{}, null, 2)); alert('Action failed: '+(j.error||r.status)); }
      else { setDiag(JSON.stringify(j||{}, null, 2)); }
      await loadVenues();
      
      // Auto-verify after set_sheet to ensure state is consistent
      // (transient gspread issues may cause false negatives on first check)
      if(act==='set_sheet'){
        await new Promise(r => setTimeout(r, 800));
        await doVenueAction('check', vid);
      }
    }catch(e){
      alert('Action failed: '+(e.message||e));
    }
  }

  async function loadVenues(){
    try{
      const r=await fetch('/super/api/venues?super_key='+encodeURIComponent(super_key), {headers: hdrs()});
      const j=await r.json();
      if(!j.ok) throw new Error(j.error||'venues failed');
      state.venues=j.venues||[];
      if(!state.selected && state.venues.length) state.selected=state.venues[0].venue_id;
      renderVenues(); renderVenueDetails();
      document.getElementById('ts').textContent=new Date().toLocaleString();
    }catch(e){
      document.getElementById('venuesRail').innerHTML='<div class="vrow"><div class="vname">Failed to load venues</div><div class="vid">'+hesc(e.message||e)+'</div></div>';
      document.getElementById('venuesTbody').innerHTML='<tr><td colspan="5" class="muted">Failed to load venues: '+hesc(e.message||e)+'</td></tr>';
    }
  }

  function setLeadsDiag(s){ document.getElementById('leadsDiag').textContent=String(s||''); }

  async function loadLeads(){
    const q=(document.getElementById('leadsSearch').value||'').trim();
    const per_page=parseInt(document.getElementById('leadsPerPage').value||'10',10)||10;
    const venue_state=(document.getElementById('leadsVenueState').value||'all').trim();
    const venue_id=(document.getElementById('leadsVenueId').value||'').trim();
    const params=new URLSearchParams();
    params.set('super_key', super_key);
    params.set('page', String(state.leadsPage||1));
    params.set('per_page', String(per_page));
    if(q) params.set('q', q);
    if(venue_state && venue_state!=='all') params.set('venue_state', venue_state);
    if(venue_id) params.set('venue_id', venue_id);

    const url='/admin/api/leads_all?'+params.toString();
    document.getElementById('leadsTbody').innerHTML='<tr><td colspan="8" class="muted">Loading…</td></tr>';
    setLeadsDiag('Fetching: '+url);

    try{
      const r=await fetch(url, {headers: hdrs()});
      const raw=await r.text();
      let j={}; try{ j=JSON.parse(raw); }catch(_){ j={ok:false, error:'non-json', raw: raw.slice(0,600)}; }
      if(!r.ok || !j.ok){
        setLeadsDiag('HTTP '+r.status+'\n'+raw.slice(0,900));
        document.getElementById('leadsTbody').innerHTML='<tr><td colspan="8" class="muted">Failed to load leads.</td></tr>';
        return;
      }
      const items=j.items||[];
      const errors=j.errors||[];
      state.leadsTotal=parseInt(j.total||0,10)||0;
      document.getElementById('leadsCount').textContent='Total: '+state.leadsTotal+' • Page '+state.leadsPage;
      if(!items.length){
        setLeadsDiag(JSON.stringify({note:'No leads returned', total: state.leadsTotal, errors: errors}, null, 2));
        document.getElementById('leadsTbody').innerHTML='<tr><td colspan="8" class="muted">No leads found.</td></tr>';
        return;
      }
      setLeadsDiag(JSON.stringify({total: state.leadsTotal, returned: items.length, errors: errors.slice(0,6)}, null, 2));
      const tbody=document.getElementById('leadsTbody'); tbody.innerHTML='';
      items.forEach(it=>{
        const tr=document.createElement('tr');
        tr.innerHTML=
          '<td>'+hesc(it.venue_id||it.venue||'')+'</td>'+
          '<td>'+hesc(it.name||it.contact_name||'')+'</td>'+
          '<td>'+hesc(it.phone||it.contact||it.email||'')+'</td>'+
          '<td>'+hesc(it.datetime||it.date_time||it.time||'')+'</td>'+
          '<td>'+hesc(it.party_size||it.party||'')+'</td>'+
          '<td>'+hesc(it.status||'')+'</td>'+
          '<td>'+hesc(it.tier||it.vip||'')+'</td>'+
          '<td>'+hesc(it.queue||'')+'</td>';
        tbody.appendChild(tr);
      });
    }catch(e){
      setLeadsDiag('Error: '+(e.message||e));
      document.getElementById('leadsTbody').innerHTML='<tr><td colspan="8" class="muted">Failed to load leads.</td></tr>';
    }
  }

  async function exportCsv(){
    const q=(document.getElementById('leadsSearch').value||'').trim();
    const venue_state=(document.getElementById('leadsVenueState').value||'all').trim();
    const venue_id=(document.getElementById('leadsVenueId').value||'').trim();
    const per_page=200;
    let page=1; let all=[];
    for(let guard=0; guard<50; guard++){
      const p=new URLSearchParams();
      p.set('super_key', super_key); p.set('page', String(page)); p.set('per_page', String(per_page));
      if(q) p.set('q', q);
      if(venue_state && venue_state!=='all') p.set('venue_state', venue_state);
      if(venue_id) p.set('venue_id', venue_id);
      const r=await fetch('/admin/api/leads_all?'+p.toString(), {headers: hdrs()});
      const j=await r.json().catch(()=>({ok:false}));
      if(!j.ok) break;
      const items=j.items||[];
      all=all.concat(items);
      if(items.length<per_page) break;
      page++;
    }
    const cols=['venue_id','name','phone','datetime','party_size','status','tier','queue'];
    const lines=[cols.join(',')].concat(all.map(it=>cols.map(c=>('\"'+String(it[c]||'').replace(/\"/g,'\"\"')+'\"')).join(',')));
    const blob=new Blob([lines.join('\n')], {type:'text/csv'});
    const a=document.createElement('a'); a.href=URL.createObjectURL(blob); a.download='leads_export.csv'; document.body.appendChild(a); a.click(); a.remove();
  }

  document.getElementById('tabVenues').onclick=()=>setActiveTab('venues');
  document.getElementById('tabLeads').onclick=()=>setActiveTab('leads');
  document.getElementById('btnRefresh').onclick=()=>loadVenues();
  document.getElementById('venueSearch').oninput=()=>renderVenues();
  document.getElementById('venuesFilter').onchange=(e)=>applyChipFilter(e.target.value);
  document.querySelectorAll('#healthChips .chip').forEach(el=>{ el.onclick=()=>applyChipFilter(el.dataset.filter); });

  document.getElementById('btnReloadLeads').onclick=()=>{state.leadsPage=1; loadLeads();};
  document.getElementById('leadsSearch').onkeydown=(e)=>{ if(e.key==='Enter'){ state.leadsPage=1; loadLeads(); } };
  document.getElementById('leadsPerPage').onchange=()=>{ state.leadsPage=1; loadLeads(); };
  document.getElementById('leadsVenueState').onchange=()=>{ state.leadsPage=1; loadLeads(); };
  document.getElementById('leadsVenueId').onchange=()=>{ state.leadsPage=1; loadLeads(); };
  document.getElementById('btnPrev').onclick=()=>{ if(state.leadsPage>1){ state.leadsPage--; loadLeads(); } };
  document.getElementById('btnNext').onclick=()=>{ state.leadsPage++; loadLeads(); };
  document.getElementById('btnExportCsv').onclick=()=>exportCsv();

  document.addEventListener('click',(ev)=>{
    const t=ev.target;
    if(!t || !t.getAttribute) return;
    const act=t.getAttribute('data-act');
    const vid=t.getAttribute('data-vid');
    if(act && vid){ ev.preventDefault(); doVenueAction(act, vid); }
  });

  document.getElementById('btnCreate').onclick=async ()=>{
  const name=prompt('Venue name (display)',''); if(name===null) return;
  const vid=prompt('Venue id (optional)',''); if(vid===null) return;
  const sheet=prompt('Google Sheet ID (optional)',''); if(sheet===null) return;
  const plan=prompt('Plan (standard/premium)','standard'); if(plan===null) return;

  // ✅ FIX: use google_sheet_id (not sheet_id)
  const payload={
    venue_name:name.trim(),
    venue_id:(vid||'').trim(),
    google_sheet_id:(sheet||'').trim(),
    plan:(plan||'standard').trim()
  };

  try{
    const r=await fetch('/super/api/venues/create?super_key='+encodeURIComponent(super_key), {
      method:'POST',
      headers: hdrs(),
      body: JSON.stringify(payload)
    });

    const j=await r.json().catch(()=>({}));

    if(!j.ok){
      alert('Create failed: '+(j.error||('HTTP '+r.status)));
      return;
    }

    // ✅ SHOW PACK + make it easy to copy/send
const p = j.pack || {};
const text =
  "✅ Venue created\n\n" +
  "Admin: " + (p.admin_url||"") + "\n" +
  "Manager: " + (p.manager_url||"") + "\n" +
  "Fan / QR: " + (p.qr_url||"") + "\n";

try { await navigator.clipboard.writeText(text); } catch(e) {}
prompt("Copy & send this to the customer:", text);


    await loadVenues();
  }catch(e){
    alert('Create failed: '+(e.message||e));
  }
};

  document.getElementById('ts').textContent=new Date().toLocaleString();
  loadVenues();
  fetch('/super/api/diag?super_key='+encodeURIComponent(super_key), {headers: hdrs()}).then(r=>r.json()).then(j=>{
    document.getElementById('build').textContent=(j.app_version || j.app_version_env || '—');
  }).catch(()=>{});
})();
</script>
</body>
</html>
"""

# Active Super Admin UI
SUPER_CONSOLE_HTML = SUPER_CONSOLE_HTML_OPTIONA


@app.get("/admin/api/super")
def admin_api_super_console_redirect():
    """Back-compat redirect to /super/admin, preserving query params per spec."""
    try:
        q = request.query_string.decode("utf-8") if request.query_string else ""
        url = "/super/admin"
        if q:
            url = url + "?" + q
        return redirect(url, code=302)
    except Exception:
        return redirect("/super/admin", code=302)

@app.get("/super/admin")
def super_admin_console():
    """
    Super Admin console per spec. Sets super_key cookie for session persistence.
    Must NEVER hard-500; returns minimal diagnostic fallback HTML on failure.
    """
    try:
        if not _is_super_admin_request():
            return jsonify({"ok": False, "error": "unauthorized"}), 403
        resp = make_response(render_template_string(SUPER_CONSOLE_HTML))
        resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
        resp.headers["Pragma"] = "no-cache"
        resp.headers["Expires"] = "0"
        try:
            sk = (request.args.get("super_key") or request.headers.get("X-Super-Key") or "").strip()
            if sk:
                resp.set_cookie("super_key", sk, httponly=True, samesite="Lax")
        except Exception:
            pass
        return resp
    except Exception as e:
        tb = traceback.format_exc()
        return (
            "<h1>Super Admin</h1>"
            "<p>Dashboard failed to render. Copy the details below.</p>"
            f"<pre>{html.escape(tb)}</pre>",
            200,
        )




# ============================================================
# Super Admin: error trap (ensures /super/* never returns a generic 500)
# ============================================================
from werkzeug.exceptions import HTTPException
from flask import Response

@app.errorhandler(Exception)
def _handle_any_exception(e):
    # Let Flask/werkzeug handle HTTP errors normally (404/403/etc.)
    if isinstance(e, HTTPException):
        return e
    try:
        # Only expose diagnostics for Super Admin paths AND only with valid super key.
        if request.path.startswith("/super") and _is_super_admin_request():
            tb = traceback.format_exc()
            return Response(
                "<h3>Super Admin Error</h3><pre style='white-space:pre-wrap'>%s</pre>"
                % html.escape(tb),
                status=500,
                mimetype="text/html",
            )
    except Exception:
        pass
    # Default generic error
    return ("Internal Server Error", 500)

@app.get("/super/api/diag")
def super_api_diag():
    """Platform diagnostic endpoint per spec: redis, build, runtime, env, key presence."""
    ok, resp = _require_super_admin()
    if not ok:
        return resp
    try:
        rs = _redis_runtime_status() if callable(globals().get("_redis_runtime_status", None)) else {}
        venues = _load_venues_from_disk() or {}
        return jsonify({
            "ok": True,
            "redis": {
                "enabled": bool(_REDIS_ENABLED),
                "namespace": _REDIS_NS,
                "status": rs.get("redis_enabled", False),
                "error": rs.get("redis_error", ""),
            },
            "build": {
                "app_version": os.environ.get("APP_VERSION") or "1.4.2",
                "python_version": __import__("sys").version,
                "pid": os.getpid(),
            },
            "runtime": {
                "data_dir": DATA_DIR,
                "venues_dir": VENUES_DIR,
                "venues_count": len(venues) if isinstance(venues, dict) else 0,
                "multi_venue": bool(MULTI_VENUE),
            },
            "env": {
                "SUPER_ADMIN_KEY_present": bool(SUPER_ADMIN_KEY),
                "ADMIN_KEY_present": bool(ADMIN_KEY),
                "REDIS_URL_present": bool(os.environ.get("REDIS_URL")),
                "VENUE_LOCK": VENUE_LOCK or "",
                "DEFAULT_VENUE_ID": DEFAULT_VENUE_ID,
            },
        })
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

@app.route("/super/api/venues/set_sheet", methods=["POST", "OPTIONS"])
def super_api_venues_set_sheet():
    """Attach or update Google Sheet ID for venue.

    Deterministic behavior:
    - Persist `data.google_sheet_id`
    - Immediately validate the sheet (best-effort) and persist:
      - sheet_ok (bool)
      - ready (bool)
      - last_checked (timestamp)
    This avoids the UI looking "stochastic" (SHEET FAIL until manual re-check).
    """
    if request.method == "OPTIONS":
        return ("", 204)

    ok, resp = _require_super_admin()
    if not ok:
        return resp

    body = request.get_json(silent=True) or {}
    venue_id = _slugify_venue_id(str(body.get("venue_id") or "").strip())
    sheet_id = str(body.get("sheet_id") or body.get("google_sheet_id") or "").strip()

    if not venue_id:
        return jsonify({"ok": False, "error": "missing venue_id"}), 400
    if not sheet_id:
        return jsonify({"ok": False, "error": "missing sheet_id"}), 400

    venues = _load_venues_from_disk() or {}
    cfg = venues.get(venue_id) if isinstance(venues, dict) else None
    if not isinstance(cfg, dict):
        return jsonify({"ok": False, "error": "venue_not_found"}), 404

    path = str(cfg.get("_path") or os.path.join(VENUES_DIR, f"{venue_id}.json"))

    cfg["data"] = cfg.get("data") if isinstance(cfg.get("data"), dict) else {}
    cfg["data"]["google_sheet_id"] = sheet_id
    cfg["updated_at"] = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

    # Best-effort: validate immediately so operators see stable status.
    chk = _check_sheet_id(sheet_id)
    try:
        cfg["sheet_ok"] = bool(chk.get("ok"))
        cfg["ready"] = bool(cfg["sheet_ok"] and cfg.get("active", True))
        cfg["last_checked"] = chk.get("checked_at")
    except Exception:
        pass

    wrote, write_path, err = _write_venue_config(venue_id, cfg)
    _invalidate_venues_cache()

    return jsonify({
        "ok": True,
        "venue_id": venue_id,
        "google_sheet_id": sheet_id,
        "persisted": wrote,
        "error": err,
        "check": chk,
    })

@app.route("/super/api/venues/check_sheet", methods=["POST", "OPTIONS"])
def super_api_venues_check_sheet():
    if request.method == "OPTIONS":
        return ("", 204)

    ok, resp = _require_super_admin()
    if not ok:
        return resp

    body = request.get_json(silent=True) or {}
    venue_id = _slugify_venue_id(str(body.get("venue_id") or "").strip())
    if not venue_id:
        return jsonify({"ok": False, "error": "missing venue_id"}), 400

    cfg = _venue_cfg(venue_id)
    path = str(cfg.get("_path") or "")
    if not path:
        return jsonify({"ok": False, "error": "venue config not found"}), 404

    sheet_id = _venue_sheet_id(venue_id)  # reads data.google_sheet_id OR cfg.google_sheet_id :contentReference[oaicite:2]{index=2}
    if not sheet_id:
        return jsonify({"ok": False, "error": "No google_sheet_id configured"}), 400

    chk = _check_sheet_id(sheet_id)
    cfg["sheet_ok"] = bool(chk.get("ok"))
    cfg["ready"] = bool(cfg["sheet_ok"] and cfg.get("active", True))
    cfg["last_checked"] = chk.get("checked_at")

    _safe_write_json_file(path, cfg)
    _invalidate_venues_cache()

    return jsonify({"ok": True, "venue_id": venue_id, "sheet_id": sheet_id, "check": chk})

@app.route("/super/api/venues/create", methods=["POST", "OPTIONS"])
def super_api_venues_create():
    """Create a new venue configuration per spec."""
    if request.method == "OPTIONS":
        return ("", 204)

    ok, resp = _require_super_admin()
    if not ok:
        return resp

    body = request.get_json(silent=True) or {}

    venue_name = str(body.get("venue_name") or "").strip() or "New Venue"
    venue_id = _slugify_venue_id(str(body.get("venue_id") or venue_name))
    plan = str(body.get("plan") or "standard").strip().lower() or "standard"

    # accept either key (back-compat), but normalize to google_sheet_id
    sheet_id = str(body.get("google_sheet_id") or body.get("sheet_id") or "").strip()

    admin_key = secrets.token_hex(16)
    manager_key = secrets.token_hex(16)

    base = _public_base_url()

    pack = {
        "venue_name": venue_name,
        "venue_id": venue_id,
        "status": "active",
        "active": True,
        "plan": plan,

        # ✅ AUTO DEFAULTS (fully automated, one-click)
        # If no location_line is provided, it safely falls back to venue_name
        "show_location_line": True,
        "location_line": str(body.get("location_line") or venue_name).strip(),

        # ✅ env-safe, consistent links
        "admin_url": f"{base}/admin?key={admin_key}&venue={venue_id}",
        "manager_url": f"{base}/admin?key={manager_key}&venue={venue_id}",
        "qr_url": f"{base}/v/{venue_id}",

        "access": {
            "admin_keys": [admin_key],
            "manager_keys": [manager_key],
        },
        "data": {
            "google_sheet_id": sheet_id,
            "redis_namespace": f"{_REDIS_NS}:{venue_id}",
        },
        # Track sheet validation state: None=not checked, True=valid, False=invalid
        "sheet_ok": None if sheet_id else None,
        "ready": False,
        "features": body.get("features")
        if isinstance(body.get("features"), dict)
        else {
            "vip": True,
            "waitlist": False,
            "ai_queue": True,
        },
        "created_at": datetime.now(timezone.utc)
            .isoformat()
            .replace("+00:00", "Z"),
    }
    
    # If sheet_id provided, validate it immediately
    if sheet_id:
        chk = _check_sheet_id(sheet_id)
        pack["sheet_ok"] = bool(chk.get("ok"))
        pack["last_checked"] = chk.get("checked_at")

    wrote, write_path, err = _write_venue_config(venue_id, pack)
    if not wrote:
        return jsonify({"ok": False, "error": err or "Failed to write venue config"}), 500

    _invalidate_venues_cache()

    return jsonify({
        "ok": True,
        "venue_id": venue_id,
        "path": write_path,
        "admin_key": admin_key,
        "manager_key": manager_key,
        "pack": pack,
    })

@app.get("/super/api/overview")
def super_api_overview():
    """Platform-wide summary metrics per spec."""
    ok, resp = _require_super_admin()
    if not ok:
        return resp

    venues = _load_venues_from_disk() or {}
    total_venues = 0
    active = 0
    inactive = 0
    needs_attention = 0
    sheet_fail = 0
    not_ready = 0

    for vid, cfg in (venues or {}).items():
        if not isinstance(cfg, dict):
            continue
        total_venues += 1
        is_active = bool(_venue_is_active(vid))
        if is_active:
            active += 1
        else:
            inactive += 1

        sid = _venue_sheet_id(vid)
        s_ok = cfg.get("sheet_ok")
        ready = bool(cfg.get("ready", False))

        if s_ok is False:
            sheet_fail += 1
            needs_attention += 1
        elif not sid:
            not_ready += 1
            needs_attention += 1
        elif not ready:
            not_ready += 1

    return jsonify({
        "ok": True,
        "total_venues": total_venues,
        "active": active,
        "inactive": inactive,
        "needs_attention": needs_attention,
        "sheet_fail": sheet_fail,
        "not_ready": not_ready,
    })



# =========================
# Super Admin (Platform Owner)
# =========================
def _get_super_admin_key():
    return (os.environ.get("SUPER_ADMIN_KEY") or "").strip()


# =========================
# Demo Mode (Super Admin)
# - UI-safe demos: mask PII + disable writes/exports/AI apply
# - Activated by Super Admin via cookie + header X-Demo-Mode: 1
# =========================
def _demo_mode_enabled() -> bool:
    try:
        # Explicit header wins (used by Super Admin UI fetches)
        if str(request.headers.get("X-Demo-Mode","")).strip() in ("1","true","yes","on"):
            return True
    except Exception:
        pass
    try:
        # Cookie set by /super/api/demo_mode
        if str(request.cookies.get("demo_mode","")).strip() in ("1","true","yes","on"):
            return True
    except Exception:
        pass
    try:
        if str(request.args.get("demo","")).strip() in ("1","true","yes","on"):
            return True
    except Exception:
        pass
    return False

def _mask_phone(v: str) -> str:
    s = str(v or "").strip()
    if not s:
        return ""
    digits = re.sub(r"\D+", "", s)
    if len(digits) >= 4:
        return "•••-•••-" + digits[-4:]
    return "•••"

def _mask_email(v: str) -> str:
    s = str(v or "").strip()
    if "@" not in s:
        return "•••"
    user, dom = s.split("@", 1)
    u = (user[:1] + "•••") if user else "•••"
    # keep TLD hint
    parts = dom.split(".")
    if len(parts) >= 2:
        d = (parts[0][:1] + "•••") + "." + parts[-1]
    else:
        d = dom[:1] + "•••"
    return u + "@" + d

def _apply_demo_mask_to_lead(item: Dict[str, Any]) -> Dict[str, Any]:
    x = dict(item or {})
    # Common fields across your lead schemas
    if "phone" in x:
        x["phone"] = _mask_phone(x.get("phone"))
    if "email" in x:
        x["email"] = _mask_email(x.get("email"))
    if "contact" in x and isinstance(x.get("contact"), str):
        # if contact stores phone/email
        c = x.get("contact") or ""
        if "@" in c:
            x["contact"] = _mask_email(c)
        else:
            x["contact"] = _mask_phone(c)
    return x

@app.route("/super/api/demo_mode", methods=["POST","OPTIONS"])
def super_api_demo_mode():
    """Toggle demo mode globally for Super Admin session per spec."""
    if request.method == "OPTIONS":
        return ("", 204, {
            "Access-Control-Allow-Methods": "POST, OPTIONS",
            "Access-Control-Allow-Headers": "Content-Type, X-Super-Key",
        })

    ok, resp = _require_super_admin()
    if not ok:
        return resp

    try:
        payload = request.get_json(silent=True) or {}
        enabled = bool(payload.get("enabled"))

        # Server-authoritative persistence (Redis if enabled, else /tmp)
        demo_record = {
            "enabled": bool(enabled),
            "updated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        }

        persisted = False
        persist_where = ""

        # Redis (preferred)
        try:
            _redis_init_if_needed()
            if _REDIS_ENABLED and _REDIS:
                rkey = f"{_REDIS_NS}:demo_mode"
                persisted = bool(_redis_set_json(rkey, demo_record))
                persist_where = "redis" if persisted else ""
        except Exception:
            pass

        # Disk fallback
        if not persisted:
            try:
                demo_path = "/tmp/wc26_demo_mode.json"
                _safe_write_json_file(demo_path, demo_record)
                persisted = True
                persist_where = "disk"
            except Exception:
                persisted = False
                persist_where = ""

        # Cache bust so UI reload reflects new state
        try:
            _invalidate_venues_cache()
        except Exception:
            pass

        resp = jsonify({
            "ok": True,
            "enabled": bool(enabled),
            "persisted": bool(persisted),
            "persist_where": persist_where,
        })

        # Keep cookie for UI convenience (not source of truth)
        resp.set_cookie("demo_mode", ("1" if enabled else ""), httponly=False, samesite="Lax")
        return resp

    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

def _is_super_admin_request():
    try:
        sk = _get_super_admin_key()
        if not sk:
            return False
        for src in (
            request.headers.get("X-Super-Key"),
            request.args.get("super_key"),
            request.cookies.get("super_key"),
            request.args.get("key"),
        ):
            if src and str(src).strip() == sk:
                return True
    except Exception:
        pass
    return False

def _require_super_admin():
    if not _is_super_admin_request():
        return False, (jsonify({"ok": False, "error": "unauthorized"}), 403)
    return True, None


# SIZE_PAD_START
###########################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################

# ============================================================
# Enterprise shared state in Redis (Ops / FanZone / Polls)
# ============================================================
# ============================================================
# Enterprise shared state in Redis (Ops / FanZone / Polls)
# NOTE: Must be venue-scoped to prevent cross-venue bleed.
# ============================================================

def _ops_redis_key() -> str:
    return f"{_REDIS_NS}:{_venue_id()}:ops_state"

def _fanzone_redis_key() -> str:
    return f"{_REDIS_NS}:{_venue_id()}:fanzone_state"

def _ops_state_default():
    return {"pause": False, "viponly": False, "waitlist": False, "notify": False,
            "updated_at": None, "updated_by": None, "updated_role": None}

def _load_ops_state() -> Dict[str, Any]:
    if _REDIS_ENABLED:
        st = _redis_get_json(_ops_redis_key(), default=None)
        if isinstance(st, dict):
            return _deep_merge(_ops_state_default(), st)
    return _ops_state_default()

def _save_ops_state(patch: Dict[str, Any], actor: str, role: str) -> Dict[str, Any]:
    st = _deep_merge(_load_ops_state(), patch or {})
    st["updated_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    st["updated_by"] = actor
    st["updated_role"] = role
    if _REDIS_ENABLED:
        _redis_set_json(_ops_redis_key(), st)
    return st

def _load_fanzone_state() -> Dict[str, Any]:
    if _REDIS_ENABLED:
        st = _redis_get_json(_fanzone_redis_key(), default=None)
        if isinstance(st, dict):
            return st
    st = _safe_read_json_file(POLL_STORE_FILE, default={})
    return st if isinstance(st, dict) else {}

def _save_fanzone_state(st: Dict[str, Any], actor: str, role: str) -> Dict[str, Any]:
    st2 = st if isinstance(st, dict) else {}
    st2["_meta"] = {
        "updated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "updated_by": actor,
        "updated_role": role,
    }
    if _REDIS_ENABLED:
        _redis_set_json(_fanzone_redis_key(), st2)
    else:
        _safe_write_json_file(POLL_STORE_FILE, st2)
    return st2

# ============================================================
# Super Admin: Venue Onboarding (writes config when possible)
# ============================================================
@app.get("/super/api/venues")
def super_api_venues_list():
    """Return full list of venues with platform metadata per spec."""
    ok, resp = _require_super_admin()
    if not ok:
        return resp

    try:
        _invalidate_venues_cache()
    except Exception:
        pass

    venues = _load_venues_from_disk() or {}
    out = []
    for vid, cfg in sorted((venues or {}).items(), key=lambda kv: kv[0]):
        if not isinstance(cfg, dict):
            continue

        name = str(cfg.get("venue_name") or cfg.get("name") or vid)
        sid = _venue_sheet_id(vid)
        sheet_ok = cfg.get("sheet_ok", None)
        ready = bool(cfg.get("ready", False))
        is_active = bool(_venue_is_active(vid))

        status = str(cfg.get("status") or "").strip()
        if not sid:
            status = "MISSING_SHEET"
        elif sheet_ok is True and ready:
            status = "READY"
        elif sheet_ok is False:
            status = "SHEET_FAIL"
        else:
            status = status or "SHEET_SET"

        access = cfg.get("access") if isinstance(cfg.get("access"), dict) else {}
        a_keys = access.get("admin_keys") if isinstance(access.get("admin_keys"), list) else []
        k = cfg.get("keys") if isinstance(cfg.get("keys"), dict) else {}
        first_admin_key = ""
        if a_keys:
            first_admin_key = str(a_keys[0]).strip()
        elif k and k.get("admin_key"):
            first_admin_key = str(k["admin_key"]).strip()
        if not first_admin_key:
            first_admin_key = ADMIN_OWNER_KEY or ""

        admin_url = str(cfg.get("admin_url") or "").strip()
        manager_url = str(cfg.get("manager_url") or "").strip()
        qr_url = str(cfg.get("qr_url") or "").strip()

        out.append({
            "venue_id": vid,
            "venue_name": name,
            "name": name,
            "plan": str(cfg.get("plan") or ""),
            "google_sheet_id": sid,
            "status": status,
            "active": is_active,
            "sheet_ok": sheet_ok,
            "sheet": {
                "ok": sheet_ok,
                "last_checked": cfg.get("last_checked"),
                "sheet_id": sid,
            },
            "ready": ready,
            "last_checked": cfg.get("last_checked"),
            "last_activity": cfg.get("updated_at") or cfg.get("created_at") or "",
            "admin_url": admin_url,
            "manager_url": manager_url,
            "qr_url": qr_url,
            "admin_key": first_admin_key,
            "location_line": str(cfg.get("location_line") or cfg.get("identity", {}).get("location_line", "") if isinstance(cfg.get("identity"), dict) else cfg.get("location_line") or ""),
        })

    return jsonify({"ok": True, "total": len(out), "venues": out})


@app.post("/super/api/venues/rotate_keys")
def super_api_venues_rotate_keys():
    """Rotate admin + manager keys for venue per spec."""
    ok, resp = _require_super_admin()
    if not ok:
        return resp

    if _demo_mode_enabled():
        return jsonify({"ok": False, "error": "demo_mode: write disabled"}), 403

    body = request.get_json(silent=True) or {}
    venue_id = _slugify_venue_id(str(body.get("venue_id") or "").strip())
    if not venue_id:
        return jsonify({"ok": False, "error": "venue_id required"}), 400

    venues = _load_venues_from_disk() or {}
    cfg = venues.get(venue_id)
    if not isinstance(cfg, dict):
        return jsonify({"ok": False, "error": "unknown venue"}), 404

    new_admin = secrets.token_hex(16)
    new_manager = secrets.token_hex(16)

    cfg["access"] = cfg.get("access") if isinstance(cfg.get("access"), dict) else {}
    cfg["access"]["admin_keys"] = [new_admin]
    cfg["access"]["manager_keys"] = [new_manager]
    cfg["keys"] = {"admin_key": new_admin, "manager_key": new_manager}

    base = _public_base_url()
    cfg["admin_url"] = f"{base}/admin?key={new_admin}&venue={venue_id}"
    cfg["manager_url"] = f"{base}/admin?key={new_manager}&venue={venue_id}"
    cfg["updated_at"] = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

    wrote, write_path, err = _write_venue_config(venue_id, cfg)
    _invalidate_venues_cache()

    try:
        _audit("super.venues.rotate_keys", {"venue_id": venue_id, "persisted": wrote})
    except Exception:
        pass

    return jsonify({
        "ok": True,
        "admin_key": new_admin,
        "manager_key": new_manager,
    })

@app.get("/super/api/leads")
def super_api_leads():
    """Cross-venue leads for Super Admin (read-only)."""
    ok, resp = _require_super_admin()
    if not ok:
        return resp

    # Server-authoritative demo mode (Redis preferred, disk fallback)
    demo_enabled = False
    try:
        _redis_init_if_needed()
        if _REDIS_ENABLED and _REDIS:
            rec = _redis_get_json(f"{_REDIS_NS}:demo_mode", default={}) or {}
            demo_enabled = bool(rec.get("enabled"))
    except Exception:
        pass
    if not demo_enabled:
        try:
            rec = _safe_read_json_file("/tmp/wc26_demo_mode.json", default={}) or {}
            demo_enabled = bool(rec.get("enabled"))
        except Exception:
            demo_enabled = False

    try:
        q = (request.args.get("q") or "").strip().lower()
        venue_id = _slugify_venue_id((request.args.get("venue_id") or "").strip()) if (request.args.get("venue_id") or "").strip() else ""
        page = int(request.args.get("page") or 1)
        per_page = int(request.args.get("per_page") or 10)
        page = max(1, page)
        per_page = max(1, min(100, per_page))

        # Enforce: inactive venue => no leads unless demo mode ON
        if venue_id and (not demo_enabled) and (not _venue_is_active(venue_id)):
            return jsonify({"ok": True, "items": [], "total": 0, "page": page})

        # newest-ish first by datetime string
        try:
            all_items.sort(key=lambda x: x.get("datetime",""), reverse=True)
        except Exception:
            pass

        total = len(all_items)
        start_i = (page-1)*per_page
        end_i = start_i + per_page
        page_items = all_items[start_i:end_i]

        return jsonify({"ok": True, "items": page_items, "total": total, "page": page, "per_page": per_page})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

    body = request.get_json(silent=True) or {}
    venue_id = _slugify_venue_id(str(body.get("venue_id") or "").strip())
    if not venue_id:
        return jsonify({"ok": False, "error": "venue_id required"}), 400

    venues = _load_venues_from_disk() or {}
    cfg = venues.get(venue_id)
    if not isinstance(cfg, dict):
        return jsonify({"ok": False, "error": "unknown venue"}), 404

    sid = ""
    try:
        sid = _venue_sheet_id(venue_id)
    except Exception:
        sid = str(cfg.get("google_sheet_id") or "").strip()
    if not sid:
        return jsonify({"ok": False, "error": "venue has no google_sheet_id"}), 400

    # Run the existing sheet check logic by calling the same helpers used by /super/api/sheets/check
    ok = False
    title = ""
    err = ""
    details = {}
    try:
        # Reuse gspread client
        gc = get_gspread_client()
        sh = gc.open_by_key(sid)
        title = str(getattr(sh, "title", "") or "")
        ok = True
        # Best-effort: read header row
        try:
            ws = sh.sheet1
            header = ws.row_values(1) or []
            details["header"] = header[:64]
        except Exception:
            pass
    except Exception as e:
        ok = False
        err = str(e)

    chk = {
        "ok": bool(ok),
        "sheet_id": sid,
        "title": title,
        "error": err,
        "checked_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    }
    if details:
        chk["details"] = details

    # Persist into the venue config file
    try:
        cfg2 = dict(cfg)
        cfg2["_sheet_check"] = chk
        # write back to original path if known
        path = str(cfg.get("_path") or os.path.join(VENUES_DIR, f"{venue_id}.json"))
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(cfg2, f, indent=2, sort_keys=True)
        _MULTI_VENUE_CACHE["ts"] = 0.0
    except Exception:
        pass

    try:
        _audit("super.venues.check_sheet", {"venue_id": venue_id, "ok": bool(ok), "title": title, "error": err})
    except Exception:
        pass

    return jsonify({"ok": True, "venue_id": venue_id, "check": chk})

@app.get("/super/api/sheets/check")
def super_api_sheets_check():
    """Validate Google Sheet accessibility per spec."""
    ok_auth, resp = _require_super_admin()
    if not ok_auth:
        return resp

    sheet_id = (request.args.get("sheet_id") or "").strip()
    if not sheet_id:
        return jsonify({"ok": False, "error": "sheet_id required"}), 400

    if not _GSPREAD_AVAILABLE:
        return jsonify({"ok": False, "error": "gspread not available in this runtime"}), 400

    try:
        gc = get_gspread_client()
        sh = gc.open_by_key(sheet_id)
        title = getattr(sh, "title", "")
        try:
            ws = sh.sheet1
            _ = ws.row_values(1)
        except Exception:
            pass
        try:
            _audit("super.sheets.check", {"sheet_id": sheet_id, "title": title})
        except Exception:
            pass
        return jsonify({"ok": True, "title": title, "status": "connected"})
    except Exception as e:
        try:
            _audit("super.sheets.check_failed", {"sheet_id": sheet_id, "error": str(e)})
        except Exception:
            pass
        return jsonify({"ok": False, "error": str(e)}), 400


@app.get("/admin/api/ops/state")
def admin_api_ops_state():
    ok, resp = _require_admin(min_role="manager")
    if not ok:
        return resp
    return jsonify({"ok": True, "state": _load_ops_state()})

@app.post("/admin/api/ops/save")
def admin_api_ops_save():
    ok, resp = _require_admin(min_role="manager")
    if not ok:
        return resp
    ctx = _admin_ctx()
    actor = ctx.get("actor","")
    role = ctx.get("role","")
    data = request.get_json(silent=True) or {}
    patch = {}
    for k in ["pause","viponly","waitlist","notify"]:
        if k in data:
            patch[k] = bool(data.get(k))
    st = _save_ops_state(patch, actor=actor, role=role)
    try:
        _audit("ops.update", {"by": actor, "role": role, "patch": patch})
    except Exception:
        pass
    return jsonify({"ok": True, "state": st})

@app.get("/admin/api/fanzone/state")
def admin_api_fanzone_state():
    ok, resp = _require_admin(min_role="manager")
    if not ok:
        return resp
    return jsonify({"ok": True, "state": _load_fanzone_state()})

@app.post("/admin/api/fanzone/save")
def admin_api_fanzone_save_redis():
    ok, resp = _require_admin(min_role="manager")
    if not ok:
        return resp
    ctx = _admin_ctx()
    actor = ctx.get("actor","")
    role = ctx.get("role","")
    data = request.get_json(silent=True) or {}
    st = _save_fanzone_state(data, actor=actor, role=role)
    try:
        _audit("fanzone.save", {"by": actor, "role": role})
    except Exception:
        pass
    return jsonify({"ok": True, "state": st})

# ============================================================
# Enterprise hard-gate: WSGI / Gunicorn verification
# ============================================================
@app.get("/admin/api/_wsgi")
def admin_api_wsgi():
    ok, resp = _require_admin(min_role="manager")
    if not ok:
        return resp

    server_sw = request.environ.get("SERVER_SOFTWARE", "") or ""
    return jsonify({
        "ok": True,
        "wsgi_loaded": bool(app.config.get("WSGI_LOADED")),
        "looks_like_gunicorn": "gunicorn" in server_sw.lower(),
        "server_software": server_sw,
    })

# ============================================================
# Enterprise hard-gate: unified production readiness check
# - Single endpoint for CI and ops validation.
# ============================================================
@app.get("/admin/api/_prod_gate")
def admin_api_prod_gate():
    ok, resp = _require_admin(min_role="manager")
    if not ok:
        return resp

    # WSGI / Gunicorn proof
    server_sw = request.environ.get("SERVER_SOFTWARE", "") or ""
    looks_like_gunicorn = ("gunicorn" in server_sw.lower())
    wsgi_loaded = bool(app.config.get("WSGI_LOADED"))

    # Redis proof (no fallback allowed)
    try:
        _redis_init_if_needed()
    except Exception:
        pass

    rs = _redis_runtime_status()
    redis_enabled = bool(rs.get("redis_enabled"))
    redis_namespace = rs.get("redis_namespace") or ""

    # Run the same write/read smoke used by CI, and ensure no disk fallback
    global _REDIS_FALLBACK_USED, _REDIS_FALLBACK_LAST_PATH
    _REDIS_FALLBACK_USED = False
    _REDIS_FALLBACK_LAST_PATH = ""

    smoke_ok = False
    smoke_detail = {}
    if redis_enabled:
        payload = {
            "ts": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "pid": os.getpid(),
            "nonce": secrets.token_hex(6),
        }
        try:
            _safe_write_json_file(CI_SMOKE_FILE, payload)
            got = _safe_read_json_file(CI_SMOKE_FILE, default=None)
            match = (got == payload)
            smoke_ok = bool(match) and (not _REDIS_FALLBACK_USED)
            smoke_detail = {
                "match": bool(match),
                "fallback_used": bool(_REDIS_FALLBACK_USED),
                "fallback_last_path": _REDIS_FALLBACK_LAST_PATH,
            }
        except Exception as e:
            smoke_ok = False
            smoke_detail = {"error": str(e)}

    ok_all = bool(looks_like_gunicorn and wsgi_loaded and redis_enabled and smoke_ok)

    return jsonify({
        "ok": ok_all,
        "checks": {
            "gunicorn": {"ok": bool(looks_like_gunicorn), "server_software": server_sw},
            "wsgi_loaded": {"ok": bool(wsgi_loaded)},
            "redis_enabled": {"ok": bool(redis_enabled), "namespace": redis_namespace},
            "redis_smoke": {"ok": bool(smoke_ok), **(smoke_detail or {})},
        },
        "redis": {
            "redis_enabled": bool(rs.get("redis_enabled")),
            "redis_namespace": redis_namespace,
            "redis_url_present": bool(rs.get("redis_url_present")),
            "redis_url_effective": rs.get("redis_url_effective") or "",
            "redis_error": rs.get("redis_error") or "",
        },
    }), (200 if ok_all else 500)
@app.get("/admin/api/leads_all")
def admin_api_leads_all():
    """Owner-only: merge leads across all venues (SUPER_ADMIN_KEY or global owner key).

    Enterprise behavior:
    - Never hard-fail the entire request because one venue is misconfigured.
    - Return `errors[]` per venue so UI can show what needs fixing.
    """
    # Super Admin can query cross-venue without presenting a venue admin key.
    # Owners may also query cross-venue.
    if not _is_super_admin_request():
        ok, resp = _require_admin(min_role="owner")
        if not ok:
            return resp


    try:
        limit = int(request.args.get("limit") or 500)
    except Exception:
        limit = 500
    try:
        per_venue = int(request.args.get("per_venue") or 300)
    except Exception:
        per_venue = 300


    # v1.0: pagination + filtering (back-compat with `limit`)
    q = (request.args.get("q") or "").strip()
    venue_id = (request.args.get("venue_id") or "").strip()
    venue_state = (request.args.get("venue_state") or "").strip().lower()  # active|inactive|all
    try:
        page = int(request.args.get("page") or "1")
        if page < 1:
            page = 1
    except Exception:
        page = 1
    try:
        per_page = int(request.args.get("per_page") or "")
    except Exception:
        per_page = 0

    # If caller still uses legacy `limit` (e.g., old UI), keep working.
    if not per_page:
        try:
            per_page = int(limit) if limit else 10
        except Exception:
            per_page = 10
    if per_page <= 0:
        per_page = 10
    if per_page > 200:
        per_page = 200
    limit = max(0, min(5000, limit))
    per_venue = max(1, min(2000, per_venue))

    errors: List[Dict[str, Any]] = []
    items: List[Dict[str, Any]] = []

    # helper: map sheet rows to objects using header row 1
    def rows_to_items(rows: List[List[str]], venue_id: str) -> None:
        if not rows or len(rows) < 2:
            return
        header = rows[0] or []
        body = rows[1:] or []

        # normalize header -> index
        hmap: Dict[str, int] = {}
        for i, h in enumerate(header):
            try:
                hmap[_normalize_header(h)] = i
            except Exception:
                pass

        def get_cell(r: List[str], key: str) -> str:
            i = hmap.get(_normalize_header(key), -1)
            if i < 0 or i >= len(r):
                return ""
            v = r[i]
            return "" if v is None else str(v)

        bsr = (_LEADS_CACHE_BY_VENUE.get(_slugify_venue_id(venue_id)) or {}).get("body_sheet_rows") or []
        for off, r in enumerate(body):
            if not isinstance(r, list):
                continue
            sr = bsr[off] if isinstance(bsr, list) and off < len(bsr) else off + 2
            obj = {
                "_venue_id": venue_id,
                "sheet_row": sr,
                "timestamp": get_cell(r, "timestamp"),
                "name": get_cell(r, "name"),
                "phone": get_cell(r, "phone"),
                "date": get_cell(r, "date"),
                "time": get_cell(r, "time"),
                "party_size": get_cell(r, "party_size"),
                "language": get_cell(r, "language"),
                "status": get_cell(r, "status"),
                "vip": get_cell(r, "vip"),
                "entry_point": get_cell(r, "entry_point"),
                "tier": get_cell(r, "tier"),
                "queue": get_cell(r, "queue"),
                "business_context": get_cell(r, "business_context"),
                "budget": get_cell(r, "budget"),
                "notes": get_cell(r, "notes"),
                "vibe": get_cell(r, "vibe"),
            }

            # add any extra columns (future-proof)
            try:
                for i, h in enumerate(header):
                    k = _normalize_header(h)
                    if k and k not in set(_normalize_header(x) for x in obj.keys()):
                        if i < len(r):
                            obj[h] = r[i]
            except Exception:
                pass

            items.append(obj)

    # iterate through venue configs
    venues = _iter_venue_json_configs() or []
    if not venues:
        return jsonify({"ok": True, "count": 0, "items": [], "errors": [{"venue_id": "", "error": "No venue configs found in config/venues/"}]})

    for v in venues:
        vid = str((v or {}).get('venue_id') or '')
        try:
            rows = read_leads(limit=per_venue, venue_id=vid) or []
            rows_to_items(rows, vid)
        except Exception as e:
            errors.append({"venue_id": vid, "error": str(e)})

    # newest-first best-effort (timestamp string sort)
    def _ts(o: Dict[str, Any]) -> str:
        for k in ("timestamp", "created_at", "created", "ts", "Submitted At", "submitted_at"):
            v = o.get(k)
            if v:
                return str(v)
        return ""

    items.sort(key=_ts, reverse=True)
    if limit and len(items) > limit:
        items = items[:limit]


    # Demo Mode: mask PII for safe demos
    if _demo_mode_enabled():
        try:
            items = [_apply_demo_mask_to_lead(x) for x in (items or [])]
        except Exception:
            pass

    
    # Apply filters (defensive; never hard-fail)
    try:
        if venue_id:
            items = [o for o in items if str(o.get("venue_id") or o.get("_venue_id") or "") == venue_id]
    except Exception:
        pass
    try:
        if venue_state in ("active", "inactive"):
            want = (venue_state == "active")
            items = [o for o in items if bool(_venue_is_active(str(o.get("venue_id") or o.get("_venue_id") or ""))) == want]
    except Exception:
        pass
    try:
        if q:
            qq = q.lower()
            def _matches(o):
                hay = " ".join([
                    str(o.get("venue_name") or o.get("_venue_name") or ""),
                    str(o.get("venue_id") or o.get("_venue_id") or ""),
                    str(o.get("name") or o.get("customer_name") or ""),
                    str(o.get("phone") or o.get("phone_number") or ""),
                    str(o.get("status") or ""),
                    str(o.get("tier") or ""),
                ]).lower()
                return qq in hay
            items = [o for o in items if _matches(o)]
    except Exception:
        pass

    total = len(items)

    # Paginate
    start_i = (page - 1) * per_page
    end_i = start_i + per_page
    page_items = items[start_i:end_i]

    pages = 1
    try:
        pages = int((total + per_page - 1) / per_page) if per_page else 1
        if pages < 1:
            pages = 1
    except Exception:
        pages = 1

    return jsonify({
        "ok": True,
        "total": total,
        "count": total,
        "page": page,
        "per_page": per_page,
        "pages": pages,
        "items": page_items,
        "errors": errors
    })

# ============================================================
# LEADS FILTER API (with status + time filtering)
# ============================================================

def _parse_time_range_minutes(time_str: str) -> Optional[int]:
    """Parse time range string to minutes. Returns None if invalid."""
    if not time_str:
        return None
    time_str = time_str.lower().strip()
    
    # Map common time ranges to minutes
    time_maps = {
        "30min": 30,
        "30mins": 30,
        "30_min": 30,
        "30": 30,
        "1h": 60,
        "1hour": 60,
        "1_hour": 60,
        "60": 60,
        "2h": 120,
        "2hour": 120,
        "2_hour": 120,
        "120": 120,
        "24h": 1440,
        "24hour": 1440,
        "24_hour": 1440,
        "1day": 1440,
        "1_day": 1440,
        "1440": 1440,
        "7day": 10080,
        "7_day": 10080,
        "7days": 10080,
        "1week": 10080,
        "1_week": 10080,
        "10080": 10080,
        "604800": 10080,
    }
    
    return time_maps.get(time_str)

def _timestamp_to_datetime(ts: str) -> Optional[datetime]:
    """Convert timestamp string to datetime. Handles various formats."""
    if not ts:
        return None
    try:
        # Try ISO format first
        if 'T' in ts:
            return datetime.fromisoformat(ts.replace('Z', '+00:00'))
        # Try common datetime string formats
        for fmt in [
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d %H:%M",
            "%m/%d/%Y %H:%M:%S",
            "%m/%d/%Y %H:%M",
            "%d/%m/%Y %H:%M:%S",
            "%d/%m/%Y %H:%M",
        ]:
            try:
                return datetime.strptime(ts[:19], fmt)
            except:
                pass
    except:
        pass
    return None

def _apply_leads_filters(items: List[Dict[str, Any]], 
                        statuses: Optional[List[str]] = None,
                        tiers: Optional[List[str]] = None,
                        time_minutes: Optional[int] = None,
                        entry_points: Optional[List[str]] = None) -> List[Dict[str, Any]]:
    """Apply status, tier, time, and entry_point filters to leads. Returns filtered list."""
    result = items[:]
    now = datetime.now(timezone.utc)
    
    # Filter by entry_point (normalize so "reserve now" matches "reserve_now" in sheet)
    if entry_points:
        entry_lower = [e.lower().strip() for e in entry_points if e]
        if entry_lower:
            def _norm_ep(s):
                return (s or "").replace(" ", "_").strip()
            def entry_matches(item):
                ep_raw = (item.get("entry_point") or "").strip()
                if not ep_raw:
                    return False  # empty entry_point must not match (was matching all rows: "" in "vip_vibe" is True in Python)
                ep_lo = ep_raw.lower()
                ep_underscore = _norm_ep(ep_lo)
                ep_space = ep_lo.replace("_", " ")
                for e in entry_lower:
                    e_und = _norm_ep(e)
                    e_sp = (e or "").replace("_", " ").strip().lower()
                    if e_und == ep_underscore or e_sp == ep_space:
                        return True
                    if e_und and ep_underscore and (e_und in ep_underscore or ep_underscore in e_und):
                        return True
                return False
            result = [item for item in result if entry_matches(item)]
    
    # Filter by status (normalize hyphen/space so "no-show" matches "No Show" in sheet)
    if statuses:
        statuses_lower = [s.lower().strip() for s in statuses if s]
        if statuses_lower:
            def _norm_status(s):
                return (s or "").replace("-", " ").strip()
            def status_matches(item):
                item_status = _norm_status((item.get("status") or "").lower())
                item_vibe = (item.get("vibe") or "").lower().strip()
                for s_filter in statuses_lower:
                    s_norm = _norm_status(s_filter)
                    if s_norm in item_status or s_filter in item_status or s_norm in item_vibe or s_filter in item_vibe:
                        return True
                return False
            
            result = [item for item in result if status_matches(item)]
    
    # Filter by tier (normalize space/underscore so "vip vibe" matches "vip_vibe" in sheet)
    if tiers:
        tiers_lower = [t.lower().strip() for t in tiers if t]
        if tiers_lower:
            def _norm(s):
                return (s or "").replace("_", " ").strip()
            def tier_matches(item):
                item_tier = _norm((item.get("tier") or "").lower())
                item_entry = _norm((item.get("entry_point") or "").lower())
                for t_filter in tiers_lower:
                    t_norm = _norm(t_filter)
                    if t_norm in item_tier or t_norm in item_entry or item_tier in t_norm or item_entry in t_norm:
                        return True
                return False
            
            result = [item for item in result if tier_matches(item)]
    
    # Filter by time range
    if time_minutes and time_minutes > 0:
        cutoff = now - timedelta(minutes=time_minutes)
        def within_timerange(item):
            ts_str = item.get("timestamp") or ""
            dt = _timestamp_to_datetime(ts_str)
            if dt:
                # Handle both naive and aware datetimes
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt >= cutoff
            return True  # Keep items with missing timestamps
        
        result = [item for item in result if within_timerange(item)]
    
    return result

@app.get("/admin/api/leads/filter")
def admin_api_leads_filter():
    """
    Filter leads by status, tier, time range, and optionally by venue.
    Reads from Google Sheets (works on staging/production).
    
    IMPORTANT: Respects venue isolation - no cross-venue data bleed.
    - If venue_id param is provided, filters that specific venue
    - Otherwise, filters ONLY the current venue context (_venue_id())
    
    Query params:
    - status: comma-separated or repeated (e.g. ?status=new&status=reserved)
    - tier: comma-separated or repeated (e.g. ?tier=vip&tier=reserve now)
    - entry: entry_point / source (e.g. ?entry=reserve_now)
    - time: time range in minutes or shorthand (30, 60, 1440, 10080, 7day, etc.)
    - venue or venue_id: target venue (optional; defaults to current request context)
    - limit: max results (default 500, max 2000)
    
    All filters are applied server-side against leads read from the Google Sheet.
    
    Returns: {"ok": true, "items": [...], "count": N, "filters_applied": {...}}
    """
    ok, resp = _require_admin(min_role="manager")
    if not ok:
        return resp
    
    try:
        limit = int(request.args.get("limit") or 500)
        limit = max(1, min(2000, limit))
    except:
        limit = 500
    
    # Get status filters (handle both comma-separated and repeated params)
    status_param = request.args.get("status", "").strip()
    statuses = []
    if status_param:
        # Handle comma-separated values
        if "," in status_param:
            statuses = [s.strip() for s in status_param.split(",") if s.strip()]
        else:
            statuses = [status_param]
    # Also check for repeated params (status=value&status=value)
    statuses_list = request.args.getlist("status")
    if statuses_list:
        statuses.extend([s.strip() for s in statuses_list if s.strip()])
    # Deduplicate
    statuses = list(set(s.lower().strip() for s in statuses if s))
    
    # Get tier filters (NEW: separate tier filtering)
    tier_param = request.args.get("tier", "").strip()
    tiers = []
    if tier_param:
        if "," in tier_param:
            tiers = [t.strip() for t in tier_param.split(",") if t.strip()]
        else:
            tiers = [tier_param]
    tiers_list = request.args.getlist("tier")
    if tiers_list:
        tiers.extend([t.strip() for t in tiers_list if t.strip()])
    tiers = list(set(t.lower().strip() for t in tiers if t))
    
    # Get entry_point filter (e.g. "reserve now")
    entry_param = request.args.get("entry", "").strip()
    entry_points = []
    if entry_param:
        entry_points = [e.strip() for e in entry_param.split(",") if e.strip()] if "," in entry_param else [entry_param]
    entry_points.extend([e.strip() for e in request.args.getlist("entry") if e.strip()])
    entry_points = list(set(e.strip() for e in entry_points if e))
    
    # Get time range
    time_param = request.args.get("time", "").strip()
    time_minutes = _parse_time_range_minutes(time_param) if time_param else None
    
    # Try to parse as integer minutes if not recognized
    if time_minutes is None and time_param:
        try:
            time_minutes = int(time_param)
            if time_minutes <= 0:
                time_minutes = None
        except:
            time_minutes = None
    
    # VENUE: Use explicit venue/venue_id from request so filter always matches page (no stale cookie-only context)
    target_venue_id = (request.args.get("venue_id") or request.args.get("venue") or "").strip()
    if not target_venue_id:
        try:
            target_venue_id = _slugify_venue_id(_venue_id()).lower()
        except Exception:
            target_venue_id = DEFAULT_VENUE_ID.lower()
    else:
        target_venue_id = _slugify_venue_id(target_venue_id).lower()
    
    # Load leads ONLY from the target venue
    errors: List[Dict[str, Any]] = []
    items: List[Dict[str, Any]] = []
    
    def rows_to_items(rows: List[List[str]], vid: str, body_sheet_rows: Optional[List[int]] = None) -> None:
        if not rows or len(rows) < 2:
            return
        header = rows[0] or []
        body = rows[1:] or []
        
        hmap: Dict[str, int] = {}
        for i, h in enumerate(header):
            try:
                hmap[_normalize_header(h)] = i
            except:
                pass
        
        def get_cell(r: List[str], key: str) -> str:
            i = hmap.get(_normalize_header(key), -1)
            if i < 0 or i >= len(r):
                return ""
            v = r[i]
            return "" if v is None else str(v)
        
        for off, r in enumerate(body):
            if not isinstance(r, list):
                continue
            cell_vid = (get_cell(r, "venue_id") or "").strip()
            sr = (body_sheet_rows[off] if body_sheet_rows and off < len(body_sheet_rows) else off + 2)
            obj = {
                "_venue_id": _slugify_venue_id(cell_vid) if cell_vid else vid,
                "sheet_row": sr,
                "timestamp": get_cell(r, "timestamp"),
                "name": get_cell(r, "name"),
                "phone": get_cell(r, "phone"),
                "date": get_cell(r, "date"),
                "time": get_cell(r, "time"),
                "party_size": get_cell(r, "party_size"),
                "language": get_cell(r, "language"),
                "status": get_cell(r, "status"),
                "vip": get_cell(r, "vip"),
                "entry_point": get_cell(r, "entry_point"),
                "tier": get_cell(r, "tier"),
                "queue": get_cell(r, "queue"),
                "business_context": get_cell(r, "business_context"),
                "budget": get_cell(r, "budget"),
                "notes": get_cell(r, "notes"),
                "vibe": get_cell(r, "vibe"),
            }
            items.append(obj)
    
    # Read leads ONLY from the target venue (NO cross-venue data leakage)
    try:
        rows = read_leads(limit=limit + 100, venue_id=target_venue_id) or []
        bsr = (_LEADS_CACHE_BY_VENUE.get(_slugify_venue_id(target_venue_id)) or {}).get("body_sheet_rows") or []
        rows_to_items(rows, target_venue_id, body_sheet_rows=bsr if isinstance(bsr, list) else None)
    except Exception as e:
        errors.append({"venue_id": target_venue_id, "error": str(e)})
    
    # Sort by timestamp (newest first)
    def _ts(o: Dict[str, Any]) -> str:
        for k in ("timestamp", "created_at", "created", "ts"):
            v = o.get(k)
            if v:
                return str(v)
        return ""
    
    items.sort(key=_ts, reverse=True)
    
    # Distinct entry_point values in sheet (for Source dropdown — always full list for venue)
    entry_point_values = sorted(set(
        (it.get("entry_point") or "").strip()
        for it in items if (it.get("entry_point") or "").strip()
    ), key=lambda x: x.lower())
    
    # Apply filters (status, tier, entry_point, and time)
    filtered_items = _apply_leads_filters(items, 
                                         statuses=statuses or None, 
                                         tiers=tiers or None,
                                         time_minutes=time_minutes,
                                         entry_points=entry_points or None)
    
    # Limit results
    if len(filtered_items) > limit:
        filtered_items = filtered_items[:limit]
    
    return jsonify({
        "ok": True,
        "count": len(filtered_items),
        "items": filtered_items,
        "entry_point_values": entry_point_values,
        "filters_applied": {
            "statuses": statuses,
            "tiers": tiers,
            "entry_points": entry_points,
            "time_minutes": time_minutes,
            "venue_id": target_venue_id,
        },
        "errors": errors,
    })


@app.get("/admin/api/leads/filter-local")
def admin_api_leads_filter_local():
    """
    Filter LOCAL reservations (reservations.jsonl) by status, tier, time range, and venue.
    This endpoint reads from the JSONL file instead of Google Sheets.
    
    IMPORTANT: Respects venue isolation - no cross-venue data bleed.
    - If venue_id param is provided, filters that specific venue
    - Otherwise, filters ONLY the current venue context (_venue_id())
    
    Query params:
    - status: comma-separated list or repeated param (e.g., ?status=new&status=reserved)
    - tier: comma-separated list (regular, reserve now, vip, vip vibe, entry, premium)
    - time: time range in minutes or shorthand (30, 60, 1440, 30min, 1h, 24h, 7day, etc.)
    - venue_id: filter by specific venue (optional; defaults to current venue)
    - limit: max results (default 500, max 2000)
    
    Returns: {"ok": true, "items": [...], "count": N, "filters_applied": {...}}
    """
    ok, resp = _require_admin(min_role="manager")
    if not ok:
        return resp
    
    try:
        limit = int(request.args.get("limit") or 500)
        limit = max(1, min(2000, limit))
    except:
        limit = 500
    
    # Get status filters (handle both comma-separated and repeated params)
    status_param = request.args.get("status", "").strip()
    statuses = []
    if status_param:
        if "," in status_param:
            statuses = [s.strip() for s in status_param.split(",") if s.strip()]
        else:
            statuses = [status_param]
    # Also check for repeated params
    statuses_list = request.args.getlist("status")
    if statuses_list:
        statuses.extend([s.strip() for s in statuses_list if s.strip()])
    statuses = list(set(s.lower().strip() for s in statuses if s))
    
    # Get tier filters (new field)
    tier_param = request.args.get("tier", "").strip()
    tiers = []
    if tier_param:
        if "," in tier_param:
            tiers = [t.strip() for t in tier_param.split(",") if t.strip()]
        else:
            tiers = [tier_param]
    tiers_list = request.args.getlist("tier")
    if tiers_list:
        tiers.extend([t.strip() for t in tiers_list if t.strip()])
    tiers = list(set(t.lower().strip() for t in tiers if t))
    
    # Get time range
    time_param = request.args.get("time", "").strip()
    time_minutes = _parse_time_range_minutes(time_param) if time_param else None
    
    # Try to parse as integer minutes if not recognized
    if time_minutes is None and time_param:
        try:
            time_minutes = int(time_param)
            if time_minutes <= 0:
                time_minutes = None
        except:
            time_minutes = None
    
    # VENUE ISOLATION: Use provided venue_id or default to current venue context
    # This ensures NO cross-venue data bleed
    target_venue_id = (request.args.get("venue_id", "").strip() or "").lower()
    if not target_venue_id:
        # No explicit venue provided; use current venue context
        try:
            target_venue_id = _slugify_venue_id(_venue_id()).lower()
        except Exception:
            target_venue_id = DEFAULT_VENUE_ID.lower()
    else:
        # Explicit venue provided; normalize it
        target_venue_id = _slugify_venue_id(target_venue_id).lower()
    
    # Load from local reservations.jsonl (ONLY target venue)
    items = []
    try:
        path = os.path.join(_BASE_DIR, RESERVATIONS_LOCAL_PATH)
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                for line in f:
                    if not line.strip():
                        continue
                    try:
                        obj = json.loads(line)
                        # VENUE ISOLATION: Only include items from target venue
                        obj_venue = _slugify_venue_id(str((obj or {}).get("venue_id") or "")).lower()
                        if obj_venue != target_venue_id:
                            continue  # Skip - different venue
                        items.append(obj)
                    except json.JSONDecodeError:
                        pass
    except Exception as e:
        return jsonify({
            "ok": False,
            "error": f"Failed to read reservations: {str(e)}",
            "items": [],
            "count": 0,
        }), 400
    
    # Sort by timestamp (newest first)
    def _ts(o: Dict[str, Any]) -> str:
        return str(o.get("timestamp") or "")
    
    items.sort(key=_ts, reverse=True)
    
    # Apply filters
    now = datetime.now(timezone.utc)
    if statuses or tiers or time_minutes:
        filtered = []
        for item in items:
            # Check status filter
            if statuses:
                item_status = (item.get("status") or "").lower().strip()
                item_vibe = (item.get("vibe") or "").lower().strip()
                status_match = False
                for s_filter in statuses:
                    if s_filter in item_status or s_filter in item_vibe:
                        status_match = True
                        break
                if not status_match:
                    continue
            
            # Check tier filter
            if tiers:
                item_tier = (item.get("tier") or "regular").lower().strip()
                tier_match = False
                for t_filter in tiers:
                    if t_filter in item_tier:
                        tier_match = True
                        break
                if not tier_match:
                    continue
            
            # Check time range filter
            if time_minutes and time_minutes > 0:
                ts_str = item.get("timestamp") or ""
                dt = _timestamp_to_datetime(ts_str)
                if dt:
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    cutoff = now - timedelta(minutes=time_minutes)
                    if dt < cutoff:
                        continue
            
            filtered.append(item)
        
        items = filtered
    
    # Limit results
    if len(items) > limit:
        items = items[:limit]
    
    return jsonify({
        "ok": True,
        "count": len(items),
        "items": items,
        "filters_applied": {
            "statuses": statuses,
            "tiers": tiers,
            "time_minutes": time_minutes,
            "venue_id": target_venue_id,
        },
    })

# ============================================================
# Owner / Manager HARDENING (server-side)
# Safe shim using existing _require_admin(min_role=...)
# ============================================================

_ROLE_RANK = {"manager": 1, "owner": 2, "super": 3}

def require_role(min_role: str):
    def _wrap(fn):
        def _inner(*args, **kwargs):
            try:
                ok, resp = _require_admin(min_role=min_role)
                if not ok:
                    return resp
            except Exception:
                return jsonify({"ok": False, "error": "forbidden"}), 403
            return fn(*args, **kwargs)
        _inner.__name__ = fn.__name__
        return _inner
    return _wrap

# NOTE:
# This app already uses _require_admin(min_role=...) across admin mutation endpoints.
# This shim standardizes the decorator form without altering behavior.
# ============================================================
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
FIXTURE_CACHE_FILE = os.environ.get("FIXTURE_CACHE_FILE", "/tmp/wc26_{venue}_fixtures.json")
POLL_STORE_FILE = os.environ.get("POLL_STORE_FILE", "/tmp/wc26_{venue}_poll_votes.json")


def _safe_read_json_file(path: str, default: Any = None) -> Any:
    """Read JSON from Redis (if enabled) or disk safely."""
    # BUG FIX: Lookup Redis key BEFORE expanding {venue} placeholder
    # The _REDIS_PATH_KEY_MAP uses template paths with {venue}
    original_path = str(path)
    try:
        vid = _venue_id()
        # Check Redis using the TEMPLATE path (with {venue})
        if _REDIS_ENABLED:
            suffix = _REDIS_PATH_KEY_MAP.get(original_path)
            if suffix:
                full_key = f"{_REDIS_NS}:{vid}:{suffix}"
                return _redis_get_json(full_key, default=default)
    except Exception:
        pass

    # Expand {venue} for disk fallback
    try:
        vid = _venue_id()
        if '{venue}' in str(path):
            path = str(path).replace('{venue}', vid)
    except Exception:
        pass

    try:
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception:
        return default
    return default

def _safe_write_json_file(path: str, payload: Any) -> None:
    global _REDIS_FALLBACK_USED, _REDIS_FALLBACK_LAST_PATH
    """Write JSON to Redis (if enabled) or disk safely."""
    # BUG FIX: Lookup Redis key BEFORE expanding {venue} placeholder
    # The _REDIS_PATH_KEY_MAP uses template paths with {venue}
    original_path = str(path)
    try:
        vid = _venue_id()
        # Check Redis using the TEMPLATE path (with {venue})
        if _REDIS_ENABLED:
            suffix = _REDIS_PATH_KEY_MAP.get(original_path)
            if suffix:
                full_key = f"{_REDIS_NS}:{_venue_id()}:{suffix}"
                ok = _redis_set_json(full_key, payload)
                if ok:
                    return
                # Redis was enabled, but write failed — mark fallback for enterprise gate
                _REDIS_FALLBACK_USED = True
                _REDIS_FALLBACK_LAST_PATH = original_path
    except Exception:
        # Mark fallback on unexpected redis path errors too (best effort)
        try:
            _REDIS_FALLBACK_USED = True
            _REDIS_FALLBACK_LAST_PATH = original_path
        except Exception:
            pass


    # Expand {venue} for disk fallback
    try:
        vid = _venue_id()
        if '{venue}' in str(path):
            path = str(path).replace('{venue}', vid)
    except Exception:
        pass

    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False)
    except Exception:
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
        "ask_date": "What date would you like? (Example: June 23, 2026)\n\n(To recall a past reservation, type: recall followed by your reservation ID, e.g. recall WC-XXXX)",
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
    t = (text or "").lower().strip()
    t_clean = t.rstrip(".!? \t")
    if t_clean == "vip":
        return True
    triggers = [
        "reservation",
        "reserve",
        "book a table",
        "book table",
        "table for",
        "need a table",
        "need table",
        "reserva",
        "réservation",
        "vip reservation",
        "vip table",
        "vip reserve",
        "vip book",
        "vip hold",
    ]
    return any(k in t for k in triggers)


def extract_party_size(text: str) -> Optional[int]:
    """Extract party size from free text.

    IMPORTANT: avoid mis-reading dates like 'June 13' as a party size.
    'party size to N' wins when message also contains a long digit string (e.g. phone).
    """
    raw = (text or "").strip()
    if not raw:
        return None
    t = raw.lower()

    # Strong patterns first – "party size to N" when message also has phone digits.
    m = re.search(r"party\s*(?:size)?\s+to\s+(\d+)", t)
    if m:
        n = int(m.group(1))
        if 1 <= n <= 200:
            return n
    m = re.search(r"party\s*(?:size)?\s*(?:is|=|:)?\s*(\d+)", t)
    if m:
        return int(m.group(1))
    m = re.search(r"party\s*of\s*(\d+)", t)
    if m:
        return int(m.group(1))
    m = re.search(r"table\s*(?:for|of)?\s*(\d+)", t)
    if m:
        return int(m.group(1))
    m = re.search(r"\b(\d+)\s*(people|persons|guests|pax)\b", t)
    if m:
        return int(m.group(1))
    m = re.search(r"for\s*(\d+)\s*(people|persons|guests|pax)\b", t)
    if m:
        return int(m.group(1))

    # If the text looks like a date or time and we didn't hit any of the
    # strong patterns above, be conservative and avoid treating numbers as
    # party size.
    months = [
        "january","jan","february","feb","march","mar","april","apr","may","june","jun","july","jul",
        "august","aug","september","sep","sept","october","oct","november","nov","december","dec",
        "enero","febrero","marzo","abril","mayo","junio","julio","agosto","septiembre","octubre","noviembre","diciembre",
        "janeiro","fevereiro","março","marco","abril","maio","junho","julho","agosto","setembro","outubro","novembro","dezembro",
        "janvier","février","fevrier","mars","avril","mai","juin","juillet","août","aout","septembre","octobre","novembre","décembre","decembre",
    ]
    if any(mo in t for mo in months):
        return None
    # Also check for partial month names that might indicate a date (e.g., "fe" for feb, "ju" for jun/jul)
    # This prevents "fe 18" from being read as party size 18
    month_prefixes = ["fe", "feb", "mar", "ap", "apr", "ma", "may", "ju", "jun", "jul", "au", "aug", "se", "sep", "sept", "oc", "oct", "no", "nov", "de", "dec"]
    if any(t.startswith(pref) or f" {pref}" in t or f",{pref}" in t for pref in month_prefixes if len(pref) >= 2):
        # If there's a number right after a month prefix, it's likely a date, not party size
        for pref in month_prefixes:
            if len(pref) >= 2 and (t.startswith(pref) or f" {pref}" in t or f",{pref}" in t):
                # Check if there's a digit within 5 chars after the prefix
                idx = t.find(pref)
                if idx >= 0:
                    after = t[idx + len(pref):idx + len(pref) + 5]
                    if re.search(r"\d", after):
                        return None
    if re.search(r"\b\d{4}-\d{1,2}-\d{1,2}\b", t):
        return None
    if re.search(r"\b\d{1,2}[/-]\d{1,2}([/-]\d{2,4})?\b", t):
        return None
    # Time patterns: "4 pm", "4:30 pm", "4am", etc. - don't treat as party size
    if re.search(r"\b\d{1,2}(:\d{2})?\s*(am|pm)\b", t):
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
        pre = re.sub(r"[^A-Za-z\s\-'\.]", "", pre).strip()
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
    name_part = re.sub(r"[^A-Za-z\s\-'\.]", "", name_part).strip()
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
    
    # Fuzzy match: handle partial month names like "fe" -> "feb"
    # Only try if we didn't find a full match above, and only for unambiguous prefixes
    # Check for common typos/abbreviations: "fe" -> february, "feb" -> february
    if lower.startswith("fe") and len(lower) >= 3:
        m = re.search(r"^fe[b]?\D*(\d{1,2})", lower)
        if m:
            dd = int(m.group(1))
            y = 2026
            my = re.search(r"\b(20\d{2})\b", lower)
            if my:
                y = int(my.group(1))
            return f"{y:04d}-02-{dd:02d}"

    return None


def extract_name_candidate(text: str) -> Optional[str]:
    """Best-effort name extraction from a mixed reservation message.

    Example:
      'jeff party of 6 5pm june 18 2157779999' -> 'jeff'
    """
    s = (text or "").strip()
    if not s:
        return None

    # Explicit patterns: "name is X", "my name is X", "under name X", "for name X"
    m = re.search(
        r"\b(?:my\s+name\s+is|name\s+is|under\s+name|for\s+name)\s+([A-Za-z][A-Za-z\s']{0,40})",
        s,
        flags=re.I,
    )
    if m:
        name = m.group(1).strip()
        if name:
            return name

    lower = s.lower().strip()
    # Don't treat trigger words as names
    if lower in ["reservation", "reserva", "réservation", "reserve", "book", "book a table"]:
        return None


    # Don't treat VIP intents/buttons as names
    if (lower == 'vip' or lower.startswith('vip ') or 'vip table' in lower or 'vip hold' in lower) and ('reservation' in lower or 'reserve' in lower or 'table' in lower or 'hold' in lower or lower == 'vip'):
        return None
    # If the message clearly looks like a date/time/party-size description
    # (e.g. 'June 20, 2026 at 8 pm for 6 people'), skip heuristic name guessing
    # and let the bot explicitly ask for a name.
    if re.search(r"\b(?:am|pm)\b", lower) or re.search(r"\bparty\s*(?:size)?\s*(?:is|=|:)?\s*\d+", lower) or re.search(r"\btable\s*(?:for|of)\s*\d+", lower):
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


def recall_text(sess: Dict[str, Any], session_id: Optional[str] = None) -> str:
    # Recall is by reservation ID only (no session). Use the ID you got when you made the reservation.
    return "To recall a reservation, type **recall** followed by your reservation ID (e.g. **recall WC-XXXX**). You received this ID when you made the reservation."


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
def norm_lang(lang: str) -> str:
    lang = (lang or "en").lower().strip()
    return lang if lang in ("en","es","pt","fr") else "en"

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

def _compute_group_standings(matches: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Compute group standings from group fixtures.

    Behavior:
      - Always returns groups + team rows when group fixtures exist (even if no scores yet),
        so the Groups tab never renders empty.
      - If scores are present (home_score/away_score), it updates P/W/D/L/GF/GA/PTS.
    """
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

    # Pass 1: seed groups + team list from *all* group fixtures (scores or not)
    for m in matches:
        stage = (m.get("stage") or "").strip()
        if not stage.lower().startswith("group"):
            continue
        g = stage
        home = (m.get("home") or "").strip() or "TBD"
        away = (m.get("away") or "").strip() or "TBD"
        # Avoid polluting tables with TBD vs TBD when teams aren't known yet
        if home != "TBD":
            ensure_team(g, home)
        if away != "TBD":
            ensure_team(g, away)

    # Pass 2: apply results where scores exist
    for m in matches:
        stage = (m.get("stage") or "").strip()
        if not stage.lower().startswith("group"):
            continue
        hs = m.get("home_score")
        as_ = m.get("away_score")
        if hs is None or as_ is None:
            continue  # no result yet
        try:
            hs = int(hs); as_ = int(as_)
        except Exception:
            continue

        g = stage
        home = (m.get("home") or "").strip() or "TBD"
        away = (m.get("away") or "").strip() or "TBD"
        if home == "TBD" or away == "TBD":
            continue

        ensure_team(g, home)
        ensure_team(g, away)

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
        # If no points yet, keep a stable alphabetical order; otherwise standard sorting.
        any_points = any(int(r.get("pts", 0) or 0) > 0 or int(r.get("p", 0) or 0) > 0 for r in rows)
        if any_points:
            rows.sort(key=lambda r: (r["pts"], r["gd"], r["gf"], r["team"]), reverse=True)
        else:
            rows.sort(key=lambda r: (r["team"],))
        out[g] = rows

    return out


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
CONFIG_FILE = os.path.join(DATA_DIR, "app_config_{venue}.json")
AUDIT_LOG_FILE = os.path.join(DATA_DIR, "audit_log.jsonl")

NOTIFICATIONS_FILE = os.path.join(DATA_DIR, "notifications.jsonl")
NOTIFY_WEBHOOK_URL = os.environ.get("NOTIFY_WEBHOOK_URL", "").strip()

def _notify(event: str, details: Optional[Dict[str, Any]] = None, targets: Optional[List[str]] = None, level: str = "info") -> None:
    """
    Lightweight notifications (Step 8)
    - Stored locally in an append-only JSONL file
    - Optionally POSTs to a webhook (best-effort) if NOTIFY_WEBHOOK_URL is set
    Targets: ["owner"], ["manager"], or ["owner","manager"], or ["all"]
    """
    try:
        if not targets:
            targets = ["owner", "manager"]
        entry = {
            "ts": datetime.utcnow().isoformat(timespec="seconds") + "Z",
            "event": str(event),
            "level": str(level),
            "targets": targets,
            "details": details or {},
        }
        os.makedirs(os.path.dirname(NOTIFICATIONS_FILE), exist_ok=True)
        with open(NOTIFICATIONS_FILE, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")
        if NOTIFY_WEBHOOK_URL:
            try:
                payload = json.dumps(entry).encode("utf-8")
                req = urllib.request.Request(
                    NOTIFY_WEBHOOK_URL,
                    data=payload,
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                urllib.request.urlopen(req, timeout=3)  # nosec - best effort
            except Exception:
                pass
    except Exception:
        pass

def _audit(event: str, details: Optional[Dict[str, Any]] = None) -> None:
    """Append a single-line JSON audit entry (best-effort, non-blocking).
    Writes to Redis (per-venue) and falls back to local file.
    """
    try:
        ctx = _admin_ctx() if "_admin_ctx" in globals() else {}

        entry = {
            "ts": datetime.utcnow().isoformat(timespec="seconds") + "Z",
            "event": str(event),
            "role": ctx.get("role", ""),
            "actor": ctx.get("actor", ""),
            "ip": client_ip() if request else "",
            "path": getattr(request, "path", ""),
            "details": details or {},
        }

        # --- Resolve venue consistently (NO request/body fallback) ---
        vid = _venue_id() if "_venue_id" in globals() else "default"

        # --- 1) Redis write (per-venue) ---
        try:
            if "_redis_init_if_needed" in globals():
                _redis_init_if_needed()
            if globals().get("_REDIS_ENABLED") and globals().get("_REDIS"):
                rkey = f"{_REDIS_NS}:{vid}:audit_log"
                _REDIS.lpush(rkey, json.dumps(entry, ensure_ascii=False))
                _REDIS.ltrim(rkey, 0, 2000)  # keep last ~2000 entries
        except Exception:
            pass

        # --- 2) File fallback (legacy / dev) ---
        try:
            os.makedirs(os.path.dirname(AUDIT_LOG_FILE), exist_ok=True)
            with open(AUDIT_LOG_FILE, "a", encoding="utf-8") as f:
                f.write(json.dumps(entry, ensure_ascii=False) + "\n")
        except Exception:
            pass

    except Exception:
        pass


# -----------------------------
# Lightweight in-process caches
# (per-venue only)
# -----------------------------
_CONFIG_CACHE: Dict[str, Any] = {}   # keyed by venue_id -> {"ts":..., "cfg":...}
_LEADS_CACHE: Dict[str, Any] = {}    # keyed by venue_id -> {"ts":..., "rows":...}
_sessions: Dict[str, Dict[str, Any]] = {}  # in-memory chat/reservation sessions


def _last_audit_event(event_name: str, scan_limit: int = 800) -> Optional[Dict[str, Any]]:
    """Return the most recent audit entry for a given event (best-effort)."""
    try:
        if not os.path.exists(AUDIT_LOG_FILE):
            return None
        with open(AUDIT_LOG_FILE, "r", encoding="utf-8") as f:
            lines = f.readlines()[-max(50, min(int(scan_limit), 5000)):]
        for ln in reversed(lines):
            ln = (ln or "").strip()
            if not ln:
                continue
            try:
                e = json.loads(ln)
            except Exception:
                continue
            if e.get("event") == event_name:
                return e
    except Exception:
        return None
    return None


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

def _ensure_ws(gc, title: str, venue_id: Optional[str] = None):
    sh = _open_default_spreadsheet(gc, venue_id=venue_id)
    try:
        return sh.worksheet(title)
    except Exception:
        return sh.add_worksheet(title=title, rows=2000, cols=20)

def get_config() -> Dict[str, str]:
    vid = _venue_id()
    cache = _CONFIG_CACHE.setdefault(vid, {"ts": 0.0, "cfg": None})

    now = time.time()
    cached = cache.get("cfg")
    if isinstance(cached, dict) and (now - float(cache.get("ts", 0.0)) < 5.0):
        return dict(cached)

    cfg: Dict[str, str] = {
        "poll_sponsor_text": "",
        "match_of_day_id": "",
        "motd_home": "",
        "motd_away": "",
        "motd_datetime_utc": "",
        "poll_lock_mode": "auto",
        "ops_pause_reservations": "false",
        "ops_vip_only": "false",
        "ops_waitlist_mode": "false",
    }

    # Sponsor and fan_zone: always from venue config (what admin sets); no hardcoded fallback.
    try:
        venue_cfg_path = os.path.join(VENUES_DIR, f"{vid}.json")
        if os.path.exists(venue_cfg_path):
            with open(venue_cfg_path, "r", encoding="utf-8") as f:
                vcfg = json.load(f) or {}
            fan_zone = vcfg.get("fan_zone") or {}
            if isinstance(fan_zone, dict):
                for k, v in fan_zone.items():
                    if str(k).startswith("_"):
                        continue
                    cfg[str(k)] = "" if v is None else str(v)
    except Exception:
        pass

    # Legacy: also check the old CONFIG_FILE location (for back-compat)
    path = str(CONFIG_FILE).replace("{venue}", vid)
    local = _safe_read_json(path)
    if isinstance(local, dict):
        for k, v in local.items():
            if str(k).startswith("_"):
                continue
            # Only apply if not already set from venue config
            if str(k) not in cfg or cfg.get(str(k), "") == "":
                cfg[str(k)] = "" if v is None else str(v)

    try:
        gc = get_gspread_client()
        ws = _ensure_ws(gc, "Config", venue_id=vid)
        rows = ws.get_all_values()
        for r in rows[1:]:
            if len(r) >= 2 and r[0]:
                k = r[0]
                v = r[1]
                if (k not in cfg) or (cfg.get(k, "") == ""):
                    cfg[k] = v
    except Exception:
        pass

    cache["ts"] = now
    cache["cfg"] = dict(cfg)
    return cfg

def _cfg_bool(cfg: Dict[str, Any], key: str, default: bool = False) -> bool:
    try:
        v = cfg.get(key)
        if isinstance(v, bool):
            return v
        s = str(v or "").strip().lower()
        if s in ("1","true","yes","y","on"):
            return True
        if s in ("0","false","no","n","off",""):
            return False
    except Exception:
        pass
    return bool(default)

def get_ops(cfg: Optional[Dict[str, Any]] = None) -> Dict[str, bool]:
    cfg = cfg or get_config()
    return {
        "pause_reservations": _cfg_bool(cfg, "ops_pause_reservations", False),
        "vip_only": _cfg_bool(cfg, "ops_vip_only", False),
        "waitlist_mode": _cfg_bool(cfg, "ops_waitlist_mode", False),
    }

def set_config(pairs: Dict[str, str]) -> Dict[str, str]:
    vid = _venue_id()
    cache = _CONFIG_CACHE.setdefault(vid, {"ts": 0.0, "cfg": None})

    clean: Dict[str, str] = {}
    for k, v in (pairs or {}).items():
        if not k:
            continue
        clean[str(k)] = "" if v is None else str(v)

    path = str(CONFIG_FILE).replace("{venue}", vid)
    local = _safe_read_json(path)
    if not isinstance(local, dict):
        local = {}
    local.update(clean)
    local["_updated_at"] = datetime.utcnow().isoformat(timespec="seconds") + "Z"
    _safe_write_json(path, local)

    try:
        gc = get_gspread_client()
        ws = _ensure_ws(gc, "Config", venue_id=vid)

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

    cache["ts"] = 0.0
    cache["cfg"] = None
    return get_config()

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


def _poll_client_id(provided: str) -> str:
    """Return a stable anonymous client id for poll voting.

    - If the front-end provides a client_id, we use it.
    - Otherwise we derive a deterministic id from request metadata.
    We hash so we never store raw IP/UA in the poll store/audit.
    """
    p = (provided or "").strip()
    if p:
        return p[:120]
    try:
        ua = (request.headers.get("User-Agent") or "").strip()
        al = (request.headers.get("Accept-Language") or "").strip()
        ip = (request.headers.get("X-Forwarded-For") or request.remote_addr or "").split(",")[0].strip()
        raw = f"{ip}|{ua}|{al}"
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]
    except Exception:
        return "anon"

def _append_lead_local(row: dict) -> None:
    try:
        os.makedirs(os.path.dirname(LEADS_STORE_PATH), exist_ok=True)
        with open(LEADS_STORE_PATH, "a", encoding="utf-8") as f:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")
    except Exception:
        pass

def _extract_row_num_from_updated_range(updated_range: str) -> int:
    """
    Parse ranges like 'Leads!A123:K123' and return 123.
    Returns 0 if unknown.
    """
    try:
        if not updated_range:
            return 0
        if "!" in updated_range:
            updated_range = updated_range.split("!", 1)[1]
        first = updated_range.split(":", 1)[0]
        m = re.search(r"(\d+)$", first)
        return int(m.group(1)) if m else 0
    except Exception:
        return 0

def _append_lead_google_sheet(row: dict) -> tuple[bool, int]:
    """
    Append a lead to Google Sheets.
    Returns (ok, sheet_row_number).
    sheet_row_number may be 0 if it cannot be determined.
    """
    try:
        gc = get_gspread_client()
        if GOOGLE_SHEET_ID:
            sh = gc.open_by_key(GOOGLE_SHEET_ID)
        else:
            sh = _open_default_spreadsheet(gc)
        try:
            ws = sh.get_worksheet(0)
        except Exception:
            ws = sh.sheet1
        resp = ws.append_row([
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
        sheet_row = 0
        try:
            updated_range = ""
            if isinstance(resp, dict):
                updates = resp.get("updates") or {}
                updated_range = str(updates.get("updatedRange") or "")
            sheet_row = _extract_row_num_from_updated_range(updated_range)
        except Exception:
            sheet_row = 0
        return True, int(sheet_row or 0)
    except Exception:
        return False, 0

def _require_e2e_test_access() -> Tuple[bool, str]:
    """Return (ok, reason). Only allows access when E2E_TEST_MODE=1 and caller is authorized."""
    if not E2E_TEST_MODE:
        return False, "E2E test mode is disabled"
    # Prefer a dedicated token for CI. Fallback: owner key can be used locally.
    token = (request.headers.get("X-E2E-Test-Token") or request.args.get("test_token") or "").strip()
    key = (request.args.get("key") or "").strip()
    if E2E_TEST_TOKEN:
        if token != E2E_TEST_TOKEN:
            return False, "Missing/invalid test token"
        return True, ""
    # If no token configured, allow OWNER key as a last resort (local-only convenience).
    if ADMIN_OWNER_KEY and key == ADMIN_OWNER_KEY:
        return True, ""
    return False, "Missing authorization (set E2E_TEST_TOKEN or pass owner ?key=)"


@app.errorhandler(Exception)
def _handle_any_exception(e):
    # Let Flask/werkzeug handle HTTP errors normally (404/403/etc.)
    if isinstance(e, HTTPException):
        return e
    try:
        # Only expose diagnostics for Super Admin paths AND only with valid super key.
        if request.path.startswith("/super") and _is_super_admin_request():
            tb = traceback.format_exc()
            return Response(
                "<h3>Super Admin Error</h3><pre style='white-space:pre-wrap'>%s</pre>"
                % html.escape(tb),
                status=500,
                mimetype="text/html",
            )
    except Exception:
        pass
    # Default generic error
    return ("Internal Server Error", 500)

def _get_super_admin_key():
    return (os.environ.get("SUPER_ADMIN_KEY") or "").strip()


# =========================
# Demo Mode (Super Admin)
# - UI-safe demos: mask PII + disable writes/exports/AI apply
# - Activated by Super Admin via cookie + header X-Demo-Mode: 1
# =========================
def _demo_mode_enabled() -> bool:
    try:
        # Explicit header wins (used by Super Admin UI fetches)
        if str(request.headers.get("X-Demo-Mode","")).strip() in ("1","true","yes","on"):
            return True
    except Exception:
        pass
    try:
        # Cookie set by /super/api/demo_mode
        if str(request.cookies.get("demo_mode","")).strip() in ("1","true","yes","on"):
            return True
    except Exception:
        pass
    try:
        if str(request.args.get("demo","")).strip() in ("1","true","yes","on"):
            return True
    except Exception:
        pass
    return False

def _mask_phone(v: str) -> str:
    s = str(v or "").strip()
    if not s:
        return ""
    digits = re.sub(r"\D+", "", s)
    if len(digits) >= 4:
        return "•••-•••-" + digits[-4:]
    return "•••"

def _mask_email(v: str) -> str:
    s = str(v or "").strip()
    if "@" not in s:
        return "•••"
    user, dom = s.split("@", 1)
    u = (user[:1] + "•••") if user else "•••"
    # keep TLD hint
    parts = dom.split(".")
    if len(parts) >= 2:
        d = (parts[0][:1] + "•••") + "." + parts[-1]
    else:
        d = dom[:1] + "•••"
    return u + "@" + d

def _apply_demo_mask_to_lead(item: Dict[str, Any]) -> Dict[str, Any]:
    x = dict(item or {})
    # Common fields across your lead schemas
    if "phone" in x:
        x["phone"] = _mask_phone(x.get("phone"))
    if "email" in x:
        x["email"] = _mask_email(x.get("email"))
    if "contact" in x and isinstance(x.get("contact"), str):
        # if contact stores phone/email
        c = x.get("contact") or ""
        if "@" in c:
            x["contact"] = _mask_email(c)
        else:
            x["contact"] = _mask_phone(c)
    return x

def _is_super_admin_request():
    try:
        sk = _get_super_admin_key()
        if not sk:
            return False
        for src in (
            request.headers.get("X-Super-Key"),
            request.args.get("super_key"),
            request.cookies.get("super_key"),
            request.args.get("key"),
        ):
            if src and str(src).strip() == sk:
                return True
    except Exception:
        pass
    return False

def _require_super_admin():
    if not _is_super_admin_request():
        return False, (jsonify({"ok": False, "error": "unauthorized"}), 403)
    return True, None


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5050))
    app.run(host="0.0.0.0", port=port, debug=False)
# SIZE_PAD_START
###########################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################

# ============================================================
# Enterprise shared state in Redis (Ops / FanZone / Polls)
# ============================================================
# ============================================================
# Enterprise shared state in Redis (Ops / FanZone / Polls)
# NOTE: Must be venue-scoped to prevent cross-venue bleed.
# ============================================================

def _ops_redis_key() -> str:
    return f"{_REDIS_NS}:{_venue_id()}:ops_state"

def _fanzone_redis_key() -> str:
    return f"{_REDIS_NS}:{_venue_id()}:fanzone_state"

def _ops_state_default():
    return {"pause": False, "viponly": False, "waitlist": False, "notify": False,
            "updated_at": None, "updated_by": None, "updated_role": None}

def _load_ops_state() -> Dict[str, Any]:
    if _REDIS_ENABLED:
        st = _redis_get_json(_ops_redis_key(), default=None)
        if isinstance(st, dict):
            return _deep_merge(_ops_state_default(), st)
    return _ops_state_default()

def _save_ops_state(patch: Dict[str, Any], actor: str, role: str) -> Dict[str, Any]:
    st = _deep_merge(_load_ops_state(), patch or {})
    st["updated_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    st["updated_by"] = actor
    st["updated_role"] = role
    if _REDIS_ENABLED:
        _redis_set_json(_ops_redis_key(), st)
    return st

def _load_fanzone_state() -> Dict[str, Any]:
    if _REDIS_ENABLED:
        st = _redis_get_json(_fanzone_redis_key(), default=None)
        if isinstance(st, dict):
            return st
    st = _safe_read_json_file(POLL_STORE_FILE, default={})
    return st if isinstance(st, dict) else {}

def _save_fanzone_state(st: Dict[str, Any], actor: str, role: str) -> Dict[str, Any]:
    st2 = st if isinstance(st, dict) else {}
    st2["_meta"] = {
        "updated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "updated_by": actor,
        "updated_role": role,
    }
    if _REDIS_ENABLED:
        _redis_set_json(_fanzone_redis_key(), st2)
    else:
        _safe_write_json_file(POLL_STORE_FILE, st2)
    return st2

# ============================================================
# Super Admin: Venue Onboarding (writes config when possible)
# ============================================================
def require_role(min_role: str):
    def _wrap(fn):
        def _inner(*args, **kwargs):
            try:
                ok, resp = _require_admin(min_role=min_role)
                if not ok:
                    return resp
            except Exception:
                return jsonify({"ok": False, "error": "forbidden"}), 403
            return fn(*args, **kwargs)
        _inner.__name__ = fn.__name__
        return _inner
    return _wrap

# NOTE:
# This app already uses _require_admin(min_role=...) across admin mutation endpoints.
# This shim standardizes the decorator form without altering behavior.
# ============================================================