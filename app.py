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
import secrets
import re
import time
import datetime
from datetime import datetime, date, timezone, timedelta
from typing import Dict, Any, Optional, List, Tuple

import time
import urllib.request
import urllib.error
from flask import Flask, request, jsonify, send_from_directory, send_file, make_response, g, render_template, render_template_string

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
_REDIS_PATH_KEY_MAP = {  # values are suffixes; full key includes namespace + venue

    os.environ.get("AI_QUEUE_FILE", "/tmp/wc26_ai_queue.json"): "ai_queue",
    os.environ.get("AI_SETTINGS_FILE", "/tmp/wc26_ai_settings.json"): "ai_settings",
    os.environ.get("PARTNER_POLICIES_FILE", "/tmp/wc26_partner_policies.json"): "partner_policies",
    os.environ.get("BUSINESS_RULES_FILE", "/tmp/wc26_business_rules.json"): "business_rules",
    os.environ.get("MENU_FILE", "/tmp/wc26_menu_override.json"): "menu_override",
    os.environ.get("ALERT_SETTINGS_FILE", "/tmp/wc26_alert_settings.json"): "alert_settings",
    os.environ.get("ALERT_STATE_FILE", "/tmp/wc26_alert_state.json"): "alert_state",
    os.environ.get("POLL_STORE_FILE", "/tmp/wc26_poll_votes.json"): "poll_store",
    os.environ.get("FIXTURE_CACHE_FILE", "/tmp/wc26_fixtures.json"): "fixtures_cache",
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
    """Resolve venue_id from request (path/query/cookie/header) with optional VENUE_LOCK."""
    if VENUE_LOCK:
        return VENUE_LOCK

    # 1) /v/<venue_id>/... from path
    try:
        m = re.match(r"^/v/([^/]+)", (request.path or ""))
        if m:
            return _slugify_venue_id(m.group(1))
    except Exception:
        pass

    # 2) ?venue=<venue_id> query param
    try:
        q = (request.args.get("venue") or "").strip()
        if q:
            return _slugify_venue_id(q)
    except Exception:
        pass

    # 3) venue_id cookie
    try:
        c = (request.cookies.get("venue_id") or "").strip()
        if c:
            return _slugify_venue_id(c)
    except Exception:
        pass

    # 4) X-Venue-Id header
    try:
        h = (request.headers.get("X-Venue-Id") or "").strip()
        if h:
            return _slugify_venue_id(h)
    except Exception:
        pass

    return DEFAULT_VENUE_ID


@app.before_request
def _set_venue_ctx():
    try:
        g.venue_id = _resolve_venue_id()
    except Exception:
        g.venue_id = DEFAULT_VENUE_ID


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

    Never raises. Returns:
      { ok: bool, sheet_id, title, error, checked_at, details? }
    """
    sid = str(sheet_id or "").strip()
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
    # SendGrid (best-effort). If not configured, just return False.
    try:
        ch = (ALERT_SETTINGS.get("channels") or {}).get("email") or {}
        api_key = os.environ.get("SENDGRID_API_KEY", "")
        to_addr = str(ch.get("to") or "").strip()
        from_addr = str(ch.get("from") or "").strip()
        if not (ch.get("enabled") and api_key and to_addr and from_addr):
            return False
        payload = {
            "personalizations": [{"to": [{"email": to_addr}]}],
            "from": {"email": from_addr},
            "subject": subject,
            "content": [{"type":"text/plain","value": body}],
        }
        req = urllib.request.Request(
            "https://api.sendgrid.com/v3/mail/send",
            data=json.dumps(payload).encode("utf-8"),
            headers={"Authorization": f"Bearer {api_key}", "Content-Type":"application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=10) as r:
            r.read()
        return True
    except Exception:
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
        },
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

AI_SETTINGS: Dict[str, Any] = _default_ai_settings()

def _ai_feature_allows(action_type: str, settings: Optional[Dict[str, Any]] = None) -> bool:
    """Secondary gates for AI actions (feature flags).

    Even if allow_actions permits an action, features can disable it for safe rollout.
    """
    s = settings or AI_SETTINGS or {}
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
    global AI_SETTINGS
    payload = _safe_read_json_file(AI_SETTINGS_FILE)
    if isinstance(payload, dict) and payload:
        # merge so new defaults appear automatically
        AI_SETTINGS = _deep_merge(_default_ai_settings(), payload)

def _save_ai_settings_to_disk(patch: Dict[str, Any], actor: str, role: str) -> Dict[str, Any]:
    global AI_SETTINGS
    merged = _deep_merge(AI_SETTINGS, patch or {})
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
    if at in ("send_sms", "send_email", "send_whatsapp"):
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

def _outbound_send_twilio(channel: str, to_number_or_id: str, body_text: str) -> Tuple[bool, str]:
    """
    Send SMS/WhatsApp via Twilio.
    - SMS uses TWILIO_FROM
    - WhatsApp uses TWILIO_WHATSAPP_FROM or TWILIO_FROM prefixed with 'whatsapp:' if already provided
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

    if ch == "sms":
        frm = os.environ.get("TWILIO_FROM", "").strip()
        if not frm:
            return False, "SMS not configured (missing TWILIO_FROM)"
    elif ch == "whatsapp":
        frm = os.environ.get("TWILIO_WHATSAPP_FROM", "").strip() or os.environ.get("TWILIO_FROM", "").strip()
        if not frm:
            return False, "WhatsApp not configured (missing TWILIO_WHATSAPP_FROM or TWILIO_FROM)"
        if not frm.startswith("whatsapp:"):
            frm = "whatsapp:" + frm
        if not to.startswith("whatsapp:"):
            to = "whatsapp:" + to
    else:
        return False, "Unsupported Twilio channel"

    try:
        url = f"https://api.twilio.com/2010-04-01/Accounts/{sid}/Messages.json"
        data = {"From": frm, "To": to, "Body": body_text or ""}
        r = requests.post(url, data=data, auth=(sid, token), timeout=12)
        if 200 <= r.status_code < 300:
            return True, "Message sent"
        return False, f"Twilio error {r.status_code}"
    except Exception as e:
        return False, f"Twilio send failed: {e}"

def _outbound_send(action_type: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    """Execute outbound send (human-triggered). Never called automatically."""
    at = (action_type or "").strip().lower()
    pl = payload or {}
    if at == "send_email":
        to_email = str(pl.get("to") or pl.get("email") or "").strip()
        subject = str(pl.get("subject") or "World Cup Concierge").strip()
        body = str(pl.get("message") or pl.get("body") or "").strip()
        if not to_email:
            return {"ok": False, "error": "Missing recipient email"}
        ok, msg = _outbound_send_email(to_email, subject, body)
        return {"ok": ok, "message": msg}
    if at in ("send_sms", "send_whatsapp"):
        ch = at.replace("send_", "")
        to_num = str(pl.get("to") or pl.get("phone") or "").strip()
        body = str(pl.get("message") or pl.get("body") or "").strip()
        if not to_num:
            return {"ok": False, "error": "Missing recipient number"}
        ok, msg = _outbound_send_twilio(ch, to_num, body)
        return {"ok": ok, "message": msg}
    return {"ok": False, "error": "Unsupported outbound action"}
# ============================================================
# AI Action Queue (Approval / Deny / Override)
# - Queue stores proposed AI actions (e.g., tag VIP, update status, draft reply)
# - Managers can approve/deny
# - Owners can override payload before applying
# - Persisted to disk (/tmp) for durability
# ============================================================
AI_QUEUE_FILE = os.environ.get("AI_QUEUE_FILE", "/tmp/wc26_{venue}_ai_queue.json")

def _load_ai_queue() -> List[Dict[str, Any]]:
    q = _safe_read_json_file(AI_QUEUE_FILE, default=[])
    if isinstance(q, list):
        # newest first
        q.sort(key=lambda x: str(x.get("created_at") or ""), reverse=True)
        return q
    return []

def _save_ai_queue(queue: List[Dict[str, Any]]) -> None:
    _safe_write_json_file(AI_QUEUE_FILE, queue or [])


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
        tw_ok = bool(os.environ.get("TWILIO_ACCOUNT_SID")) and bool(os.environ.get("TWILIO_AUTH_TOKEN")) and bool(os.environ.get("TWILIO_FROM"))
        if not sg_ok and not tw_ok:
            return {"name": "outbound", "ok": True, "severity": "warn", "message": "Outbound providers not configured (send disabled until configured)."}
        parts = []
        parts.append("SendGrid OK" if sg_ok else "SendGrid missing")
        parts.append("Twilio OK" if tw_ok else "Twilio missing")
        return {"name": "outbound", "ok": True, "severity": "ok" if (sg_ok or tw_ok) else "warn", "message": ", ".join(parts)}
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
    if not _ai_feature_allows(typ):
        return {"ok": False, "error": f"{typ} disabled by feature flag"}
    allow = (AI_SETTINGS.get("allow_actions") or {})

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
        ws = _open_default_spreadsheet(gc).sheet1
        header = ws.row_values(1) or []
        hmap = header_map(header)

        if typ == "vip_tag":
            if not allow.get("vip_tag", True):
                return {"ok": False, "error": "vip_tag not allowed"}
            vip = str(payload.get("vip") or "").strip()
            if vip not in ("VIP", "Regular"):
                return {"ok": False, "error": "Invalid vip"}
            col = hmap.get("vip")
            if not col:
                return {"ok": False, "error": "VIP column not found"}
            ws.update_cell(row_num, col, vip)
            _audit("ai.vip_tag.apply", {"row": row_num, "vip": vip})
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
    settings = AI_SETTINGS or {}
    if not settings.get("enabled"):
        return {"ok": False, "error": "AI disabled"}

    # Build allowed action schema based on settings
    allow = settings.get("allow_actions") or {}
    allowed_types = [k for k,v in allow.items() if v]
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
        # Always enforce row linkage to prevent "random actions"
        if sheet_row:
            payload["row"] = int(sheet_row)
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
        settings = AI_SETTINGS or {}
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

        # Build queue entry
        entry = {
            "id": _queue_new_id(),
            "created_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "created_by": "system",
            "created_role": "system",
            "status": "pending",
            "source": "lead_intake",
            "confidence": confidence,
            "notes": sug.get("notes") or "",
            "actions": actions,
            "lead": {  # small snapshot for reviewers
                "intent": lead.get("intent",""),
                "contact": lead.get("contact",""),
                "budget": lead.get("budget",""),
                "party_size": lead.get("party_size",""),
                "datetime": lead.get("datetime",""),
            }
        }

        # Default: always queue unless explicitly safe to auto-apply
        if mode != "auto" or require_approval or confidence < min_conf:
            _queue_add(entry)
            _audit("ai.queue.created", {"id": entry["id"], "source": "lead_intake", "confidence": confidence})
            _notify("ai.queue.created", {"id": entry["id"], "source": "lead_intake", "confidence": confidence, "lead": entry.get("lead")}, targets=["owner","manager"])
            return

        # Auto-apply each action (still audited)
        ctx = {"role": "owner", "actor": "system"}  # system executes as owner for apply, but it's audited
        applied = []
        errors = []
        for a in actions:
            res = _queue_apply_action(a, ctx)
            if res.get("ok"):
                applied.append(res.get("applied") or a.get("type"))
            else:
                errors.append(res.get("error") or "unknown")
        entry["status"] = "applied" if applied else "error"
        entry["applied"] = applied
        entry["errors"] = errors
        _queue_add(entry)
        _audit("ai.queue.auto_applied", {"id": entry["id"], "applied": applied, "errors": errors, "confidence": confidence})
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

    # Auth mechanism: ?key=...
    # - Owner key (global) => role=owner (all venues)
    # - Venue-scoped keys in config/venues/<venue_id>.yaml => role=owner|manager for that venue
    # - Legacy ADMIN_MANAGER_KEYS still works (manager, current venue context)

    
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

    if key in [str(k).strip() for k in akeys if str(k).strip()]:
        actor = hashlib.sha1(key.encode("utf-8")).hexdigest()[:10]
        return {"ok": True, "role": "owner", "actor": actor, "venue_id": vid}

    if key in [str(k).strip() for k in mkeys if str(k).strip()]:
        actor = hashlib.sha1(key.encode("utf-8")).hexdigest()[:10]
        return {"ok": True, "role": "manager", "actor": actor, "venue_id": vid}

    # Legacy manager keys (fallback)
    if ADMIN_MANAGER_KEYS and key in ADMIN_MANAGER_KEYS:
        actor = hashlib.sha1(key.encode("utf-8")).hexdigest()[:10]
        return {"ok": True, "role": "manager", "actor": actor, "venue_id": vid}

    return {"ok": False, "role": "", "actor": "", "venue_id": ""}


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

@app.post("/super/api/venues/set_active")
def super_api_venues_set_active():
    # Super Admin auth
    key = request.headers.get("X-Super-Key") or request.args.get("super_key")
    if key != SUPER_ADMIN_KEY:
        return jsonify({"ok": False, "error": "unauthorized"}), 403

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
    # Super Admin auth
    key = request.headers.get("X-Super-Key") or request.args.get("super_key")
    if key != SUPER_ADMIN_KEY:
        return jsonify({"ok": False, "error": "unauthorized"}), 403

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

    Accepted:
      - super_key query param
      - X-Super-Key header
      - key query param on /super/* endpoints
    """
    try:
        sk = (request.args.get("super_key") or request.headers.get("X-Super-Key") or request.cookies.get("super_key") or "").strip()
        if SUPER_ADMIN_KEY and sk == SUPER_ADMIN_KEY:
            return True
        # Allow /super/* calls that pass SUPER key as the standard key param
        k = (request.args.get("key") or "").strip()
        if SUPER_ADMIN_KEY and k == SUPER_ADMIN_KEY:
            return True
    except Exception:
        pass
    return False

def _require_admin(min_role: str = "manager"):
    """Enforce admin access.

    min_role:
      - "manager": owner or manager
      - "owner": owner only
    """
    ctx = _admin_auth()
    if not ctx.get("ok"):
        return False, (jsonify({"ok": False, "error": "Unauthorized"}), 401)

    if (min_role or "manager") == "owner" and ctx.get("role") != "owner":
        return False, (jsonify({"ok": False, "error": "Forbidden"}), 403)

    try:
        request._admin_ctx = ctx  # type: ignore[attr-defined]
    except Exception:
        pass
    return True, None

def _admin_ctx() -> Dict[str, str]:
    try:
        ctx = getattr(request, "_admin_ctx", None)
        if isinstance(ctx, dict):
            return ctx
    except Exception:
        pass
    ctx = _admin_auth()
    if isinstance(ctx, dict) and ctx.get("ok"):
        return ctx
    return {"ok": False, "role": "", "actor": ""}


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
    # Multi-venue: expand {venue} placeholder
    try:
        vid = _venue_id()
        if '{venue}' in str(path):
            path = str(path).replace('{venue}', vid)
    except Exception:
        pass
    try:
        if _REDIS_ENABLED:
            suffix = _REDIS_PATH_KEY_MAP.get(path)
            if suffix:
                full_key = f"{_REDIS_NS}:{_venue_id()}:{suffix}"
                return _redis_get_json(full_key, default=default)
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
    # Multi-venue: expand {venue} placeholder
    try:
        vid = _venue_id()
        if '{venue}' in str(path):
            path = str(path).replace('{venue}', vid)
    except Exception:
        pass
    try:
        if _REDIS_ENABLED:
            suffix = _REDIS_PATH_KEY_MAP.get(path)
            if suffix:
                full_key = f"{_REDIS_NS}:{_venue_id()}:{suffix}"
                ok = _redis_set_json(full_key, payload)
                if ok:
                    return
                # Redis was enabled, but write failed — mark fallback for enterprise gate
                _REDIS_FALLBACK_USED = True
                _REDIS_FALLBACK_LAST_PATH = str(path)
    except Exception:
        # Mark fallback on unexpected redis path errors too (best effort)
        try:
            _REDIS_FALLBACK_USED = True
            _REDIS_FALLBACK_LAST_PATH = str(path)
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
    desired = ["timestamp", "name", "phone", "date", "time", "party_size", "language", "status", "vip", "entry_point", "tier", "queue", "business_context", "budget", "notes", "vibe"]

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
    ws = _open_default_spreadsheet(gc).sheet1

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
    setv("entry_point", lead.get("entry_point",""))
    setv("tier", lead.get("tier",""))
    setv("queue", lead.get("queue",""))
    setv("business_context", lead.get("business_context",""))
    setv("budget", lead.get("budget",""))
    setv("notes", lead.get("notes",""))
    setv("vibe", lead.get("vibe",""))

    # Append at bottom (keeps headers at the top)
    ws.append_row(row, value_input_option="USER_ENTERED")


# Small per-venue read cache to avoid Sheets 429s
_LEADS_CACHE_BY_VENUE: Dict[str, Dict[str, Any]] = {}

def read_leads(limit: int = 200, venue_id: Optional[str] = None) -> List[List[str]]:
    """Read leads from the venue's Google Sheet tab (best-effort, cached).

    Returns rows including header row (row 1) as rows[0].
    """
    vid = _slugify_venue_id(venue_id or _venue_id())
    now = time.time()

    cache = _LEADS_CACHE_BY_VENUE.get(vid) or {}
    rows_cached = cache.get("rows")
    if isinstance(rows_cached, list) and (now - float(cache.get("ts") or 0.0) < 9.0):
        return rows_cached[:limit] if limit else rows_cached

    try:
        ws = get_sheet(venue_id=vid)  # uses venue sheet_name when present
        rows = ws.get_all_values() or []
        # cache regardless; even empty is useful to avoid hammering
        _LEADS_CACHE_BY_VENUE[vid] = {"ts": now, "rows": rows}
        return rows[:limit] if limit else rows
    except Exception:
        # fallback to cached rows on error
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

@app.get("/admin/fanzone")
def admin_fanzone_page():
    ok, resp = _require_admin(min_role="manager")
    if not ok:
        return resp

    # Keep venue context via ?venue=... (your before_request already reads it)
    # This page is the Poll Controls UI (same content you removed from /admin tabs).
    html_out = """
<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Admin • Fan Zone</title>
  <style>
    :root{--line: rgba(255,255,255,.14);}
    body{margin:0; font-family: ui-sans-serif,system-ui,-apple-system,Segoe UI,Roboto,Helvetica,Arial; background:#070a12; color:#eef2ff;}
    .wrap{max-width:1240px;margin:0 auto;padding:18px 16px 36px;}
    .panelcard{margin:14px 0;border:1px solid var(--line);border-radius:16px;padding:12px;background:rgba(255,255,255,.03);box-shadow:0 10px 35px rgba(0,0,0,.25)}
    .sub{opacity:.75;font-size:13px}
    .small{opacity:.8;font-size:12px}
    .controls{display:flex;gap:12px;flex-wrap:wrap}
    .inp{width:100%;padding:10px 12px;border-radius:12px;border:1px solid rgba(255,255,255,.14);background:rgba(255,255,255,.06);color:#eef2ff}
    .btn{padding:10px 14px;border-radius:12px;border:1px solid rgba(255,255,255,.18);background:rgba(255,255,255,.12);color:#eef2ff;cursor:pointer}
  </style>
</head>
<body>
  <div class="wrap">
    <div class="panelcard">
      <div style="display:flex;align-items:flex-end;justify-content:space-between;gap:12px;flex-wrap:wrap">
        <div>
          <div style="font-weight:800;letter-spacing:.02em">Fan Zone • Poll Controls</div>
          <div class="sub">Edit sponsor text + set Match of the Day (no redeploy). Also shows live poll status.</div>
        </div>
        <button type="button" class="btn" id="btnSaveConfig">Save settings</button>
      </div>

      <div class="controls" style="margin:12px 0 0 0">
        <div style="display:flex;flex-direction:column;gap:6px;min-width:320px;flex:1">
          <div class="sub">Sponsor label (“Presented by …”)</div>
          <input class="inp" id="pollSponsorText" placeholder="Fan Pick presented by …" />
        </div>

        <div style="display:flex;flex-direction:column;gap:6px;min-width:320px;flex:1">
          <div class="sub">Match of the Day</div>
          <select id="motdSelect" class="inp"></select>
          <div class="sub" style="margin-top:8px">Manual override:</div>

          <div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-top:8px">
            <div><div class="sub">Home team</div><input class="inp" id="motdHome" placeholder="Home team"/></div>
            <div><div class="sub">Away team</div><input class="inp" id="motdAway" placeholder="Away team"/></div>
          </div>

          <div style="margin-top:10px">
            <div class="sub">Kickoff (UTC ISO 8601)</div>
            <input class="inp" id="motdKickoff" placeholder="2026-06-11T19:00:00Z"/>
          </div>

          <div style="margin-top:10px">
            <div class="sub">Poll lock</div>
            <select id="pollLockMode" class="inp">
              <option value="auto">Auto (lock at kickoff)</option>
              <option value="unlocked">Force Unlocked</option>
              <option value="locked">Force Locked</option>
            </select>
            <div class="small">To reopen voting after kickoff, set <b>Force Unlocked</b>.</div>
          </div>
        </div>
      </div>

      <div id="pollStatus" style="margin-top:12px;border-top:1px solid var(--line);padding-top:12px">
        <div class="sub">Loading poll status…</div>
      </div>
    </div>

    <script>
      (function(){
        const qs = new URLSearchParams(location.search);
        const ADMIN_KEY = qs.get("key") || "";

        const $ = (id)=>document.getElementById(id);

        function esc(s){
          return String(s??"").replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
        }

        function setPollStatus(html){
          const box = $("pollStatus");
          if(!box) return;
          box.innerHTML = html;
        }

        async function loadPollStatus(){
          try{
            setPollStatus('<div class="sub">Loading poll status…</div>');
            const res = await fetch("/api/poll/state", {cache:"no-store"});
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
              rows += `<div style="display:flex;justify-content:space-between;padding:6px 0;border-bottom:1px solid rgba(255,255,255,.10)">
                         <div>${esc(r.name||"")}</div><div>${esc(r.votes||0)}</div>
                       </div>`;
            }
            if(!rows) rows = '<div class="sub">No votes yet</div>';
            setPollStatus(`<div style="font-weight:800">${esc(title)}</div>` +
                          `<div class="small">${locked ? "🔒 Locked" : "🟢 Open"}</div>` +
                          `<div style="margin-top:10px">${rows}</div>`);
          }catch(e){
            setPollStatus('<div class="sub">Poll status unavailable</div>');
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
            sel.innerHTML = '<option value="">Select a match…</option>';
            let added = 0;
            for(const m of matches){
              if(added >= 250) break;
              const dt = String(m.datetime_utc||"");
              const home = String(m.home||"");
              const away = String(m.away||"");
              if(!dt || !home || !away) continue;
              const id = (dt + "|" + home + "|" + away).replace(/[^A-Za-z0-9|:_-]+/g,"_").slice(0,180);
              const label = `${m.date||""} ${m.time||""} • ${home} vs ${away}`.trim();
              const opt = document.createElement("option");
              opt.value = id;
              opt.textContent = label || (home + " vs " + away);
              opt.setAttribute("data-home", home);
              opt.setAttribute("data-away", away);
              opt.setAttribute("data-dt", dt);
              sel.appendChild(opt);
              added++;
            }
            sel.disabled = false;
            sel.addEventListener("change", ()=>{
              const opt = sel.selectedOptions[0];
              if(!opt) return;
              $("motdHome").value = opt.getAttribute("data-home")||"";
              $("motdAway").value = opt.getAttribute("data-away")||"";
              $("motdKickoff").value = opt.getAttribute("data-dt")||"";
            });
          }catch(e){
            sel.disabled = false;
          }
        }

        async function saveFanZoneConfig(){
          const payload = {
            section: "fanzone",
            sponsor_text: ($("pollSponsorText")?.value || "").trim(),
            motd: {
              home: ($("motdHome")?.value || "").trim(),
              away: ($("motdAway")?.value || "").trim(),
              kickoff_utc: ($("motdKickoff")?.value || "").trim(),
              match_id: ($("motdSelect")?.value || "").trim(),
            },
            poll: { lock_mode: ($("pollLockMode")?.value || "").trim() }
          };
          const res = await fetch(`/admin/update-config?key=${encodeURIComponent(ADMIN_KEY)}`, {
            method:"POST",
            headers: {"Content-Type":"application/json"},
            body: JSON.stringify(payload)
          });
          await res.json().catch(()=>null);
          loadPollStatus();
        }

        $("btnSaveConfig")?.addEventListener("click", (e)=>{ e.preventDefault(); saveFanZoneConfig(); });

        loadMatchesForDropdown();
        loadPollStatus();
      })();
    </script>
  </div>
</body>
</html>
"""
    return make_response(html_out)

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
    return jsonify({"lang": lang, "events": FANZONE_DEMO.get(lang, FANZONE_DEMO["en"])})

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
            ops = get_ops()

            # Match-day ops toggles
            if ops.get("vip_only") and not re.search(r"\bvip\b", msg.lower()):
                return jsonify({"reply": "🔒 Reservations are VIP-only right now. If you have VIP access, type **VIP** to continue. Otherwise, I can add you to the waitlist.", "rate_limit_remaining": remaining})

            if ops.get("pause_reservations") and not ops.get("waitlist_mode"):
                return jsonify({"reply": "⏸️ Reservations are temporarily paused. Please check back soon, or ask a staff member for help.", "rate_limit_remaining": remaining})

            sess["mode"] = "reserving"

            # If we are in waitlist mode, capture the same details but save as Waitlist (keeps fan UI unchanged).
            if ops.get("waitlist_mode"):
                sess["lead"]["status"] = "Waitlist"

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
                ops2 = get_ops()

                # If waitlist is enabled, tag the reservation as Waitlist (still saved to the same sheet).
                if ops2.get("waitlist_mode"):
                    lead["status"] = (lead.get("status") or "Waitlist").strip() or "Waitlist"
                    if lead["status"].lower() == "new":
                        lead["status"] = "Waitlist"

                if ops2.get("pause_reservations") and not ops2.get("waitlist_mode"):
                    sess["mode"] = "idle"
                    return jsonify({"reply": "⏸️ Reservations were just paused. Please check back soon.", "rate_limit_remaining": remaining})

                if ops2.get("vip_only") and str(lead.get("vip", "No")).strip().lower() != "yes":
                    sess["mode"] = "idle"
                    return jsonify({"reply": "🔒 VIP-only is active right now. Type VIP and start again to continue.", "rate_limit_remaining": remaining})

                try:
                    append_lead_to_sheet(lead)
                    sess["mode"] = "idle"
                    saved_msg = ("✅ Added to waitlist!" if str(lead.get("status", "")).strip().lower() == "waitlist" else LANG[lang]["saved"])
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
- If user asks about the World Cup match schedule, tell them to use the **Schedule** panel on the page, then continue booking.
- For menu/food/drink/prices/diet questions, do NOT guess: direct them to the **Menu** panel on the page, then continue booking.
- If the user wants a reservation, do NOT tell them to type "reservation". Start collecting details immediately.
- If you are unsure or missing info, do NOT dead-end. Redirect to Menu/Info panels and continue booking.
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
                    "For accurate details, please check the **Menu** or **Info** panels on this page.\n\n"
                    "I can still help with a reservation — **how many guests** and **what time**?"
                )

            return jsonify({"reply": reply, "rate_limit_remaining": remaining})
        except Exception as e:
            # Customer-safe fallback (no “chat unavailable”), still routes + continues booking
            fallback = (
                "For accurate details, please check the **Menu** or **Info** panels on this page.\n\n"
                "I can still help with a reservation — **how many guests** and **what time**?"
            )
            return jsonify({"reply": f"{fallback}\n\nDebug: {type(e).__name__}", "rate_limit_remaining": remaining}), 200

    except Exception as e:
        # Never break the UI: always return JSON.
        fallback = (
            "For accurate details, please check the **Menu** or **Info** panels on this page.\n\n"
            "I can still help with a reservation — **how many guests** and **what time**?"
        )
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

def _read_notifications(limit: int = 50, role: str = "manager") -> List[Dict[str, Any]]:
    """
    Read newest notifications first; filter by role.
    Managers see entries targeted to manager/all; Owners see everything.
    """
    try:
        if not os.path.exists(NOTIFICATIONS_FILE):
            return []
        items: List[Dict[str, Any]] = []
        with open(NOTIFICATIONS_FILE, "rb") as f:
            f.seek(0, os.SEEK_END)
            size = f.tell()
            buf = b""
            step = 4096
            while size > 0 and len(items) < limit * 3:
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
                        items.append(it)
                    except Exception:
                        continue
                    if len(items) >= limit * 3:
                        break
        out: List[Dict[str, Any]] = []
        for it in items:
            t = it.get("targets") or []
            if role == "owner":
                out.append(it)
            else:
                if "all" in t or "manager" in t:
                    out.append(it)
            if len(out) >= limit:
                break
        return out
    except Exception:
        return []

def _audit(event: str, details: Optional[Dict[str, Any]] = None) -> None:
    """Append a single-line JSON audit entry (best-effort, non-blocking)."""
    try:
        ctx = _admin_ctx()
        entry = {
            "ts": datetime.utcnow().isoformat(timespec="seconds") + "Z",
            "event": str(event),
            "role": ctx.get("role", ""),
            "actor": ctx.get("actor", ""),
            "ip": client_ip() if request else "",
            "path": getattr(request, "path", ""),
            "details": details or {},
        }
        os.makedirs(os.path.dirname(AUDIT_LOG_FILE), exist_ok=True)
        with open(AUDIT_LOG_FILE, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")
    except Exception:
        pass


# -----------------------------
# Lightweight in-process caches
# (reduces Google Sheets quota hits)
# -----------------------------
_CONFIG_CACHE: Dict[str, Any] = {"ts": 0.0, "cfg": None}   # ttl ~5s
_LEADS_CACHE: Dict[str, Any] = {"ts": 0.0, "rows": None}  # ttl ~30s
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

def _ensure_ws(gc, title: str):
    sh = _open_default_spreadsheet(gc)
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
        "ops_pause_reservations": "false",
        "ops_vip_only": "false",
        "ops_waitlist_mode": "false",
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
        client_id_raw = (request.args.get("client_id") or "").strip()
        client_id = _poll_client_id(client_id_raw) if client_id_raw else ""
        voted_for = _poll_has_voted(mid, client_id) if client_id else None
        can_vote = (not locked) and (not voted_for)
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

@app.route("/admin/update-config", methods=["POST"])
def admin_update_config():
    ok, resp = _require_admin(min_role="manager")
    if not ok:
        return resp

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

        # Ops toggles (match-day controls)
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

        cfg = set_config(pairs)
        _audit("config.update", {"keys": list(pairs.keys())})
        return jsonify({"ok": True, "config": {
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

    ctx = _admin_ctx()
    role = ctx.get("role", "")
    actor = ctx.get("actor", "")

    if request.method == "GET":
        return jsonify({"ok": True, "role": role, "settings": AI_SETTINGS})

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
        mc = as_float(data.get("min_confidence"), float(AI_SETTINGS.get("min_confidence") or 0.7))
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
            patch["model"] = str(data.get("model") or "").strip() or AI_SETTINGS.get("model")
        if "system_prompt" in data:
            sp = str(data.get("system_prompt") or "").strip()
            # Keep prompt bounded to avoid accidental huge payloads
            if len(sp) > 6000:
                return jsonify({"ok": False, "error": "system_prompt too long"}), 400
            if sp:
                patch["system_prompt"] = sp
        if "allow_actions" in data and isinstance(data.get("allow_actions"), dict):
            # Only allow known keys
            allow = {}
            for k in ("vip_tag", "status_update", "reply_draft"):
                if k in data["allow_actions"]:
                    allow[k] = bool(as_bool(data["allow_actions"].get(k)))
            patch["allow_actions"] = _deep_merge(AI_SETTINGS.get("allow_actions") or {}, allow)

        if "notify" in data and isinstance(data.get("notify"), dict):
            notify = {}
            for k in ("owner", "manager"):
                if k in data["notify"]:
                    notify[k] = bool(as_bool(data["notify"].get(k)))
            patch["notify"] = _deep_merge(AI_SETTINGS.get("notify") or {}, notify)

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

    # Load sheet (best-effort). If Sheets isn't configured, return a friendly error.
    try:
        gc = get_gspread_client()
        ws = _open_default_spreadsheet(gc).sheet1
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
        # Normalize vip to bool-ish for the AI prompt
        lead["vip"] = str(lead.get("vip") or "").strip().lower() in ["1","true","yes","y"]
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

        status_col = hmap.get("status")  # 1-based
        # Walk from bottom (newest) upward, collect "New"
        for i in range(len(rows)-1, 0, -1):
            if len(targets) >= limit:
                break
            rv = rows[i]
            st = ""
            if status_col and (status_col-1) < len(rv):
                st = (rv[status_col-1] or "").strip().lower()
            if st in ["new", ""]:
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
            payload = a.get("payload") or {}
            # Always attach sheet_row for safe apply handlers
            if isinstance(payload, dict) and "sheet_row" not in payload:
                payload["sheet_row"] = sheet_row
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
    ctx = _admin_ctx()
    role = ctx.get("role", "")
    queue = _load_ai_queue()
    # optional status filter
    status = (request.args.get("status") or "").strip().lower()
    if status:
        queue = [q for q in queue if str(q.get("status") or "").lower() == status]
    return jsonify({"ok": True, "role": role, "queue": queue[:500]})



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
      "message": "..."
      "row": 12 (optional lead row)
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
    payload = {
        "partner": data.get("partner"),
        "row": data.get("row"),
        "to": data.get("to"),
        "subject": data.get("subject"),
        "message": data.get("message"),
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
    _audit("outbound.queue", {"id": qid, "partner": partner, "type": action_type, "by": actor})
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

    it["status"] = "denied"
    it["reviewed_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    it["reviewed_by"] = actor
    it["reviewed_role"] = role
    it["applied_result"] = None
    _save_ai_queue(queue)
    _audit("ai.queue.deny", {"id": qid, "type": it.get("type")})
    _notify("ai.queue.deny", {"id": qid, "type": it.get("type"), "by": actor, "role": role}, targets=["owner","manager"])
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

    # If AI disabled, still allow approval but do not apply (acts like "reviewed")
    applied = None

    it_type = str(it.get("type") or "").strip().lower()
    # Outbound sends are NEVER executed on approval. Approval only unlocks a human "Send Now" click.
    if it_type in ("send_email", "send_sms", "send_whatsapp"):
        applied = {"ok": True, "note": "Approved — ready to send (human click required)"}
    else:
        if AI_SETTINGS.get("enabled") and (AI_SETTINGS.get("mode") in ("auto", "suggest", "off")):
            # Always require explicit approval here (this endpoint *is* the approval)
            applied = _queue_apply_action({"type": it.get("type"), "payload": it.get("payload")}, ctx)

    it["status"] = "approved"
    it["reviewed_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    it["reviewed_by"] = actor
    it["reviewed_role"] = role
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
    if it_type not in ("send_email", "send_sms", "send_whatsapp"):
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
    it["sent_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    it["sent_by"] = actor
    it["sent_role"] = role
    it["send_result"] = res
    _save_ai_queue(queue)

    _audit("outbound.send", {"id": qid, "partner": partner, "type": it_type, "ok": bool(res.get("ok")), "by": actor})
    _notify("outbound.send", {"id": qid, "partner": partner, "type": it_type, "ok": bool(res.get("ok")), "by": actor, "role": role}, targets=["owner","manager"])
    return jsonify({"ok": True, "result": res})


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
        if typ not in ("vip_tag", "status_update", "reply_draft"):
            return jsonify({"ok": False, "error": "Invalid type"}), 400
        it["type"] = typ
    if "payload" in data and isinstance(data.get("payload"), dict):
        it["payload"] = data.get("payload") or {}

    # Apply immediately
    applied = None
    if AI_SETTINGS.get("enabled"):
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
    ctx = _admin_ctx()
    role = ctx.get("role", "manager")

    # Role-based branding (visual only)
    is_owner = (role == "owner")
    page_title = ("Owner Admin Console" if is_owner else "Manager Ops Console")
    page_sub = ("Full control — Admin key" if is_owner else "Operations control — Manager key")

    # Role-based branding (visual only)
    is_owner = (role == "owner")
    page_title = ("Owner Admin Console" if is_owner else "Manager Ops Console")
    page_sub = ("Full control — Admin key" if is_owner else "Operations control — Manager key")

    try:
        limit = int(request.args.get("limit", 50) or 50)
    except Exception:
        limit = 50
    limit = max(1, min(200, limit))
    items = _read_notifications(limit=limit, role=role)
    return jsonify({"ok": True, "role": role, "items": items})

@app.route("/admin/api/notifications/clear", methods=["POST"])
def admin_api_notifications_clear():
    ok, resp = _require_admin(min_role="manager")
    if not ok:
        return resp
    try:
        with open(NOTIFICATIONS_FILE, "w", encoding="utf-8") as f:
            f.write("")
    except Exception:
        pass
    _audit("notifications.clear", {})
    return jsonify({"ok": True})


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

    # Apply Ops (manager allowed)
    ops = preset.get("ops") or {}
    def _b(v):
        return "true" if bool(v) else "false"
    pairs = {
        "ops_pause_reservations": _b(ops.get("pause_reservations", False)),
        "ops_vip_only": _b(ops.get("vip_only", False)),
        "ops_waitlist_mode": _b(ops.get("waitlist_mode", False)),
    }
    cfg = set_config(pairs)

    # Apply Rules (owner only)
    rules_patch = preset.get("rules") or {}
    if rules_patch:
        ok2, resp2 = _require_admin(min_role="owner")
        if not ok2:
            return resp2
        global BUSINESS_RULES
        BUSINESS_RULES = _deep_merge(BUSINESS_RULES, rules_patch)
        _persist_rules(rules_patch)

    _audit("preset.apply", {"name": name, "ops": get_ops(cfg), "rules_patch": rules_patch})
    return jsonify({"ok": True, "name": name, "ops": get_ops(cfg), "rules": BUSINESS_RULES})


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
    entries: List[Dict[str, Any]] = []
    try:
        if os.path.exists(AUDIT_LOG_FILE):
            with open(AUDIT_LOG_FILE, "r", encoding="utf-8") as f:
                lines = f.readlines()[-limit:]
            for ln in lines:
                ln = (ln or "").strip()
                if not ln:
                    continue
                try:
                    entries.append(json.loads(ln))
                except Exception:
                    continue
    except Exception:
        pass
    # Newest first
    try:
        entries = list(reversed(entries))
    except Exception:
        pass
    return jsonify({"ok": True, "entries": entries})



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

    gc = get_gspread_client()
    ws = _open_default_spreadsheet(gc).sheet1
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
    _audit("lead.handled", {"row": row_num}) if (status == "Handled") else _audit("lead.update", {"row": row_num})
    return jsonify({"ok": True, "updated": updates})


@app.route("/admin/export.csv")
def admin_export_csv():
    ok, resp = _require_admin(min_role="manager")
    if not ok:
        return "Unauthorized", 401

    key = (request.args.get("key","") or "").strip()

    gc = get_gspread_client()
    ws = _open_default_spreadsheet(gc).sheet1
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
        return "Unauthorized", 401
    key = (request.args.get("key", "") or "").strip()

    ctx = _admin_ctx()
    role = ctx.get("role", "manager")

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

    admin_key_q = f"?key={key}"
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

.wrap{max-width:1200px;margin:0 auto;padding:18px;}
.topbar{display:flex;align-items:flex-start;justify-content:space-between;gap:12px;margin-bottom:12px;}
.h1{font-size:18px;font-weight:800;letter-spacing:.3px}
.sub{color:var(--muted);font-size:12px;margin-top:4px}

.pills{display:flex;gap:8px;flex-wrap:wrap;margin-top:8px}
.pill{
  border:1px solid var(--line);
  background:rgba(255,255,255,.03);
  padding:8px 10px;
  border-radius:999px;
  font-size:12px
}
.pillbtn{cursor:pointer}

.pillselect,
select{
  border:1px solid var(--line);
  background:rgba(255,255,255,.03);
  color:var(--menu-text);
  padding:8px 10px;
  border-radius:999px;
  font-size:12px;
  outline:none;
  z-index:9999;
}

.pillselect option,
select option{
  background:var(--menu-bg) !important;
  color:var(--menu-text) !important;
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
  padding:12px 12px;
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

.badge{
  display:inline-block;
  padding:4px 8px;
  border-radius:999px;
  border:1px solid var(--line);
  background:rgba(255,255,255,.03);
  font-size:11px
}
.badge.good{border-color:rgba(46,160,67,.35)}
.badge.warn{border-color:rgba(255,204,102,.35)}
.badge.bad{border-color:rgba(255,93,93,.35)}

.inp,textarea,select{
  width:100%;
  box-sizing:border-box;
  background:rgba(255,255,255,.04);
  border:1px solid var(--line);
  color:var(--menu-text);
  padding:10px;
  border-radius:12px;
  font-size:13px;
  outline:none
}

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
    <div class="h2">Notifications</div>
    <div id="notifBody" class="small" style="margin-top:8px"></div>
    <div id="notif-msg" class="note" style="margin-top:8px"></div>
  </div>
</div>

<!-- FAN ZONE TAB -->

    <div class="controls" style="margin:12px 0 0 0">
      <div style="display:flex;flex-direction:column;gap:6px;min-width:320px;flex:1">
        <div class="sub">Sponsor label (“Presented by …”)</div>
        <input class="inp" id="pollSponsorText" placeholder="Fan Pick presented by …" />
      </div>

      <div style="display:flex;flex-direction:column;gap:6px;min-width:320px;flex:1">
        <div class="sub">Match of the Day</div>
        <select id="motdSelect"></select>

        <div class="sub" style="margin-top:8px">
          If schedule options don’t load (or you want to override), set Match of the Day manually:
        </div>

        <div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-top:8px">
          <div>
            <div class="sub">Home team</div>
            <input id="motdHome" placeholder="Home team"/>
          </div>
          <div>
            <div class="sub">Away team</div>
            <input id="motdAway" placeholder="Away team"/>
          </div>
        </div>

        <div style="margin-top:10px">
          <div class="sub">
            Kickoff (UTC, ISO 8601 — used to lock poll at kickoff)
          </div>
          <input id="motdKickoff" placeholder="2026-06-11T19:00:00Z"/>
        </div>

        <div style="margin-top:10px">
          <div class="sub">Poll lock</div>
          <select id="pollLockMode" class="inp">
            <option value="auto">Auto (lock at kickoff)</option>
            <option value="unlocked">Force Unlocked (admin override)</option>
            <option value="locked">Force Locked</option>
          </select>
          <div class="small">
            If you need to reopen voting after kickoff, set <b>Force Unlocked</b>.
          </div>
        </div>
      </div>
    </div>

    <div id="pollStatus" style="margin-top:12px;border-top:1px solid var(--line);padding-top:12px">
      <div class="sub">Loading poll status…</div>
    </div>
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

    <div class="grid2" style="margin-top:12px">
      <div>
        <label class="small" style="display:flex;gap:8px;align-items:center">
          <input id="al-enabled" type="checkbox"/>
          <span>Enable alerts</span>
        </label>
        <div class="small" style="opacity:.8;margin-top:6px">When enabled, <b>Run checks</b> can emit alerts on failures (rate-limited).</div>
      </div>
      <div>
        <label class="small">Rate limit (seconds)</label>
        <input id="al-rate" class="inp" type="number" min="60" step="60" placeholder="600"/>
      </div>
    </div>

    <div class="grid2" style="margin-top:12px">
      <div>
        <label class="small" style="display:flex;gap:8px;align-items:center">
          <input id="al-slack-en" type="checkbox"/>
          <span>Slack</span>
        </label>
        <input id="al-slack-url" class="inp" placeholder="Slack webhook URL"/>
      </div>
      <div>
        <label class="small" style="display:flex;gap:8px;align-items:center">
          <input id="al-email-en" type="checkbox"/>
          <span>Email</span>
        </label>
        <input id="al-email-to" class="inp" placeholder="Alert email TO"/>
        <input id="al-email-from" class="inp" placeholder="Alert email FROM (optional)"/>
      </div>
    </div>

    <div class="grid2" style="margin-top:12px">
      <div>
        <label class="small" style="display:flex;gap:8px;align-items:center">
          <input id="al-sms-en" type="checkbox"/>
          <span>SMS (critical only)</span>
        </label>
        <input id="al-sms-to" class="inp" placeholder="Alert SMS TO (E.164)"/>
        <div class="small" style="opacity:.75;margin-top:6px">SMS only sends on <b>error</b> severity alerts (to prevent spam).</div>
      </div>
      <div>
        <label class="small">Fixtures stale threshold (seconds)</label>
        <input id="al-fixtures-stale" class="inp" type="number" min="3600" step="3600" placeholder="86400"/>
      </div>
    </div>

    <div style="margin-top:12px;display:flex;gap:10px;flex-wrap:wrap;align-items:center">
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
        html.append("<div class='card'><div class='h2'>Leads</div><div class='small'>Newest first. Update Status/VIP and save.</div><div style='display:flex;flex-wrap:wrap;gap:10px;margin-top:10px;align-items:center'><div class='pills' style='margin:0'><button class='btn2' id='flt-all' type='button'>All</button><button class='btn2' id='flt-vip' type='button'>VIP</button><button class='btn2' id='flt-reg' type='button'>Regular</button></div><div style='display:flex;gap:8px;align-items:center'><span class='small' style='white-space:nowrap'>Entry:</span><select class='inp' id='flt-entry' style='min-width:180px'></select><span id='leadsCount' class='small' style='margin-left:8px'>0 shown</span></div></div></div>")
        html.append("<div class='tablewrap'><table id='leadsTable'>")
        html.append("<thead><tr>"                    "<th>Row</th><th>Timestamp</th><th>Name</th><th>Contact</th>"                    "<th>Date</th><th>Time</th><th>Party</th>"                    "<th>Segment</th><th>Entry</th><th>Queue</th><th>Budget</th>"                    "<th>Context</th><th>Notes</th>"                    "<th>Status</th><th>VIP</th><th>Save</th>"                    "</tr></thead><tbody>")
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
            # Canonical tier used for filtering (prevents VIP entries leaking into Regular when tier column is blank)
            tier_key = "vip" if (str(tier or "").strip().lower() == "vip" or str(vip or "").strip().lower() in ["yes","true","1","y","vip"]) else "regular"
            queue = colval(r, i_queue, "")
            bctx = colval(r, i_ctx, "")
            budget = colval(r, i_budget, "")
            notes = colval(r, i_notes, "")
            vibe = colval(r, i_vibe, "")

            def opt(selected, label):
                sel = "selected" if selected else ""
                return f"<option {sel}>{label}</option>"

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
            html.append(f"<td><span class='{seg_cls}'>{_hesc(seg)}</span></td>")
            html.append("<td><span class='pill'>" + _hesc(ep) + "</span></td>")
            html.append("<td><span class='badge good'>" + _hesc(queue) + "</span></td>")
            html.append(f"<td>{_hesc(budget)}</td>")
            # Context + Notes (compact)
            ctx_txt = (bctx or "").strip()
            note_txt = (notes or "").strip()
            if vibe and vibe.strip():
                # Keep vibe visible without cluttering budget/notes columns
                note_txt = (note_txt + (" | " if note_txt else "") + f"vibe: {vibe.strip()}").strip()
            def _cell_details(label, txt):
                if not txt:
                    return "<span class='small'>—</span>"
                short = txt if len(txt) <= 34 else (txt[:34] + "…")
                return "<details><summary class='small'>" + _hesc(short) + "</summary><div style='margin-top:6px;white-space:pre-wrap' class='small'>" + _hesc(txt) + "</div></details>"
            html.append("<td>" + _cell_details("context", ctx_txt) + "</td>")
            html.append("<td>" + _cell_details("notes", note_txt) + "</td>")


            html.append("<td>")
            html.append(f"<select class='inp' id='status-{sheet_row}'>"
                        f"{opt(st=='New','New')}{opt(st=='Confirmed','Confirmed')}{opt(st=='Seated','Seated')}{opt(st=='No-Show','No-Show')}{opt(st=='Handled','Handled')}"
                        "</select>")
            html.append("</td>")

            html.append("<td>")
            html.append(f"<select class='inp' id='vip-{sheet_row}'>"
                        f"{opt(vip.lower() in ['yes','true','1','y'], 'Yes')}{opt(vip.lower() in ['no','false','0','n',''], 'No')}"
                        "</select>")
            html.append("</td>")


            html.append("<td>")
            html.append(f"<button class='btn2' onclick='saveLead({sheet_row})'>Save</button><button class='btnTiny' title='Mark handled' onclick='markHandled({sheet_row})'>✅</button>")
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
        <label class="small"><input type="checkbox" id="ai-act-draft"> Reply drafts</label>
      </div>

      <div class="note">Tip: keep actions limited until you trust the workflow.</div>
    </div>
  </div>
</div>

  <div class="card" style="margin-top:14px">
    <div class="h2">AI Replay (Read-only)</div>
    <div class="small">Owner tool to re-run AI suggestions for a specific lead row without applying changes.</div>
    <div style="margin-top:10px;display:flex;gap:10px;flex-wrap:wrap;align-items:center">
      <input id="replay-row" class="pillselect" style="min-width:120px" placeholder="Row #" />
      <button class="btn2" type="button" onclick="replayAI()">Replay</button>
      <span id="replay-msg" class="note"></span>
    </div>
    <pre id="replayOut" style="margin-top:10px;white-space:pre-wrap;background:rgba(255,255,255,.04);border:1px solid rgba(255,255,255,.10);border-radius:12px;padding:10px;max-height:260px;overflow:auto"></pre>
  </div>

<div id="tab-aiq" class="tabpane hidden">
  <div class="card">
    <div class="h2">AI Approval Queue</div>
    <div class="small">Proposed AI actions wait here for <b>Approve</b>, <b>Deny</b>, or <b>Owner Override</b>. This keeps automation powerful but controlled.</div>
    <div style="margin-top:10px;display:flex;gap:10px;flex-wrap:wrap;align-items:center">
      <button class="btn2" onclick="loadAIQueue()">Refresh</button>
      <span class="note" style="opacity:.6">|</span>
      <input id="ai-run-limit" class="inp" type="number" min="1" max="25" step="1" value="5" style="max-width:90px" title="How many newest New leads to analyze" />
      <button class="btn2" onclick="runAINew()">Run AI (New)</button>
      <input id="ai-run-row" class="inp" type="number" min="2" step="1" placeholder="Row #" style="max-width:110px" title="Run AI for a specific Google Sheet row number" />
      <button class="btn2" onclick="runAIRow()">Run AI (Row)</button>

      <select id="aiq-filter" class="inp" style="max-width:180px" onchange="loadAIQueue()">
        <option value="">All</option>
        <option value="pending">Pending</option>
        <option value="approved">Approved</option>
        <option value="denied">Denied</option>
      </select>
      <span id="aiq-msg" class="note"></span>
    </div>
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
    <div class="h2">Audit Log</div>
    <div class="small">Shows who changed ops/rules/menu and when.</div>
    <div style="margin-top:10px;display:flex;gap:10px;align-items:center;flex-wrap:wrap">
      <input class="inp" id="audit-limit" type="number" min="10" max="500" value="200" style="width:120px" />
      <select class="inp" id="audit-filter" style="width:220px">
        <option value="all">All events</option>
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
    try{
      var b = document.querySelector('.tabbtn[data-tab="'+tab+'"]');
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
            var t = b.getAttribute('data-tab');
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

function setupLeadFilters(){
  const tbl = qs('#leadsTable');
  if(!tbl) return;

  // Build entry dropdown
  const entries = new Set();
  document.querySelectorAll('#leadsTable tbody tr').forEach(tr=>{
    const ep = norm(tr.getAttribute('data-entry')||"");
    if(ep) entries.add(ep);
  });

  const sel = qs('#flt-entry');
  if(sel){
    sel.innerHTML = "";
    const all = document.createElement('option');
    all.value = "all"; all.textContent = "All";
    sel.appendChild(all);
    Array.from(entries).sort().forEach(ep=>{
      const o = document.createElement('option');
      o.value = ep; o.textContent = ep.replace(/_/g,' ');
      sel.appendChild(o);
    });
    sel.addEventListener('change', ()=>{
      leadEntryFilter = norm(sel.value||"all") || "all";
      applyLeadFilters();
    });
  }

  function setBtn(activeId){
    ['flt-all','flt-vip','flt-reg'].forEach(id=>{
      const b = qs('#'+id);
      if(!b) return;
      b.style.borderColor = (id===activeId) ? 'rgba(212,175,55,.65)' : 'rgba(255,255,255,.12)';
      b.style.background = (id===activeId) ? 'rgba(212,175,55,.12)' : 'rgba(255,255,255,.03)';
      b.style.color = (id===activeId) ? '#fff' : 'rgba(234,240,255,.92)';
    });
  }

  qs('#flt-all')?.addEventListener('click', ()=>{ leadTierFilter="all"; setBtn('flt-all'); applyLeadFilters(); });
  qs('#flt-vip')?.addEventListener('click', ()=>{ leadTierFilter="vip"; setBtn('flt-vip'); applyLeadFilters(); });
  qs('#flt-reg')?.addEventListener('click', ()=>{ leadTierFilter="regular"; setBtn('flt-reg'); applyLeadFilters(); });

  setBtn('flt-all');
  applyLeadFilters();
}


function qs(sel){return document.querySelector(sel);}
function qsa(sel){return Array.from(document.querySelectorAll(sel));}

qsa('.tabbtn').forEach(btn=>{
  btn.addEventListener('click', ()=>{
    qsa('.tabbtn').forEach(b=>b.classList.remove('active'));
    btn.classList.add('active');
    const t = btn.dataset.tab;
    ['leads','ai','aiq','rules','menu','audit'].forEach(x=>{
      const pane = document.getElementById('tab-'+x);
      if(!pane) return;
      pane.classList.toggle('hidden', x!==t);
    });
if(t==='ai') loadAI();
    if(t==='aiq') loadAIQueue();
    if(t==='rules') loadRules();
    if(t==='menu') loadMenu();
    if(t==='audit') loadAudit();
  });
});

async function saveLead(sheetRow){
  const status = qs('#status-'+sheetRow).value;
  const vip = qs('#vip-'+sheetRow).value;
  const res = await fetch('/admin/update-lead?key='+encodeURIComponent(KEY), {
    method:'POST',
    headers:{'Content-Type':'application/json'},
    body: JSON.stringify({row: sheetRow, status, vip})
  });
  const j = await res.json().catch(()=>{});
  if(j && j.ok) alert('Saved');
  else alert('Save failed: ' + (j.error||res.status));
}

async function markHandled(sheetRow){
  // Minimal: set status to Handled + write an audit entry.
  const res = await fetch('/admin/update-lead?key='+encodeURIComponent(KEY), {
    method:'POST',
    headers:{'Content-Type':'application/json'},
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
  const res = await fetch('/admin/api/ops?key='+encodeURIComponent(KEY));
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
  const res = await fetch('/admin/api/ops?key='+encodeURIComponent(KEY), {
    method:'POST',
    headers:{'Content-Type':'application/json'},
    body: JSON.stringify(payload)
  });
  const j = await res.json().catch(()=>null);
  if(j && j.ok){ if(msg) msg.textContent='Saved ✔'; }
  else { if(msg) msg.textContent='Save failed'; alert('Save failed: '+(j && j.error ? j.error : res.status)); }
}



// ===== AI Automation =====
  // Re-enable toggles after save attempt
  try{
    if(elPause) elPause.disabled = false;
    if(elVip) elVip.disabled = false;
    if(elWait) elWait.disabled = false;
  }catch(e){}

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

    const feat = s.features || {};
    if(qs('#ai-feat-vip')) qs('#ai-feat-vip').checked = (feat.auto_vip_tag !== false);
    if(qs('#ai-feat-status')) qs('#ai-feat-status').checked = !!feat.auto_status_update;
    if(qs('#ai-feat-draft')) qs('#ai-feat-draft').checked = (feat.auto_reply_draft !== false);

    // lock owner-only fields for managers
    ['ai-model','ai-prompt','ai-act-vip','ai-act-status','ai-act-draft'].forEach(id=>{
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
  const res = await fetch('/admin/api/presets/apply?key='+encodeURIComponent(KEY), {
    method:'POST',
    headers:{'Content-Type':'application/json'},
    body: JSON.stringify({name})
  });
  const j = await res.json().catch(()=>null);
  if(j && j.ok){
    if(msg) msg.textContent='Applied ✔ (logged)';
    await loadOps();
    // If you are owner and preset touched rules, refresh rules form for visibility.
    if(j.rules) await loadRules();
  } else {
    if(msg) msg.textContent='Apply failed';
    alert('Preset failed: '+(j && j.error ? j.error : res.status));
  }
}


// ===== AI Approval Queue =====
async function loadAIQueue(){
  const msg = qs('#aiq-msg'); if(msg) msg.textContent = 'Loading…';
  const list = qs('#aiq-list'); if(list) list.innerHTML = 'Loading…';
  try{
    const filt = (qs('#aiq-filter')?.value || '').trim();
    const url = `/admin/api/ai/queue?key=${encodeURIComponent(KEY)}` + (filt ? `&status=${encodeURIComponent(filt)}` : '');
    const r = await fetch(url, {cache:'no-store'});
    const data = await r.json();
    if(!data.ok) throw new Error(data.error || 'Failed');
    const q = data.queue || [];
    renderAIQueue(q);
    if(msg) msg.textContent = q.length ? (`${q.length} item(s)`) : 'No items';
  }catch(e){
    if(msg) msg.textContent = 'Load failed: ' + (e.message || e);
    if(list) list.textContent = 'Failed to load queue.';
  }
}

async function runAINew(){
  const msg = qs('#aiq-msg'); if(msg) msg.textContent = 'Running AI…';
  const lim = parseInt(qs('#ai-run-limit')?.value || '5', 10);
  try{
    const r = await fetch(`/admin/api/ai/run?key=${encodeURIComponent(KEY)}`, {
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
    const r = await fetch(`/admin/api/ai/run?key=${encodeURIComponent(KEY)}`, {
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


function esc(s){ return (s||'').toString().replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c])); }

function renderAIQueue(items){
  const list = qs('#aiq-list');
  if(!list) return;

  // Explicit empty state for CI tests + clarity
  if(!items || !items.length){
    list.innerHTML = '<div class="note">AI Queue empty — no queued actions.</div>';
    return;
  }

  const rows = (items || []).map((it)=>{
    const id = esc(it.id || '');
    const typ = esc(it.type || '');
    const st  = esc(it.status || '');
    const conf = (typeof it.confidence === 'number') ? it.confidence.toFixed(2) : '';
    const when = esc(it.created_at || '');
    const why  = esc(it.why || it.reason || '');
    const payload = esc(JSON.stringify(it.payload || {}));

    const canAct = (st === 'pending');

    const approveBtn = `<button type="button" class="btn" ${canAct ? '' : 'disabled'} onclick="aiqApprove('${id}', this)">Approve</button>`;
    const denyBtn    = `<button type="button" class="btn2" ${canAct ? '' : 'disabled'} onclick="aiqDeny('${id}', this)">Deny</button>`;
    const overrideBtn = `<button type="button" class="btn" onclick="aiqOverride('${id}', this)">Owner Override</button>`;

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
          ${overrideBtn}
        </div>
      </div>
    `;
  });

  list.innerHTML = rows.join('');
}

async function aiqApprove(id, btn){
  if(!confirm('Approve this action?')) return;
  // Button-level loading state
  const _btn = btn;
  if(_btn){ _btn.disabled = true; _btn.dataset.prevText = _btn.textContent || ''; _btn.textContent = 'Approving…'; }

  const msg = qs('#aiq-msg'); if(msg) msg.textContent = 'Approving…';
  const r = await fetch(`/admin/api/ai/queue/${encodeURIComponent(id)}/approve?key=${encodeURIComponent(KEY)}`, {
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
  const r = await fetch(`/admin/api/ai/queue/${encodeURIComponent(id)}/send?key=${encodeURIComponent(KEY)}`, {
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
    alert('Send failed: '+(j && j.error ? j.error : r.status));
  }
}

async function aiqDeny(id, btn){
  if(!confirm('Deny this action?')) return;
  const _btn = btn;
  if(_btn){ _btn.disabled = true; _btn.dataset.prevText = _btn.textContent || ''; _btn.textContent = 'Denying…'; }

  const msg = qs('#aiq-msg'); if(msg) msg.textContent = 'Denying…';
  const r = await fetch(`/admin/api/ai/queue/${encodeURIComponent(id)}/deny?key=${encodeURIComponent(KEY)}`, {
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
  const typ = prompt('Override action type (vip_tag, status_update, reply_draft):', 'vip_tag');
  if(!typ){ if(_btn){ _btn.disabled=false; _btn.textContent=(_btn.dataset.prevText || 'Owner Override'); } return; }
  let payloadTxt = prompt('Override payload JSON (must be valid JSON object):', '{"row":2,"vip":"VIP"}');
  if(payloadTxt === null){ if(_btn){ _btn.disabled=false; _btn.textContent=(_btn.dataset.prevText || 'Owner Override'); } return; }
  payloadTxt = payloadTxt.trim();
  let payloadObj = null;
  try{ payloadObj = JSON.parse(payloadTxt); }catch(e){ if(_btn){ _btn.disabled=false; _btn.textContent=(_btn.dataset.prevText || 'Owner Override'); } alert('Invalid JSON'); return; }
  const msg = qs('#aiq-msg'); if(msg) msg.textContent = 'Applying override…';
  const r = await fetch(`/admin/api/ai/queue/${encodeURIComponent(id)}/override?key=${encodeURIComponent(KEY)}`, {
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

  const url = '/admin/api/audit?key='+encodeURIComponent(KEY)+'&limit='+encodeURIComponent(lim);
  const res = await fetch(url, {cache:'no-store'});
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
    const events = Array.from(new Set(entries.map(e=>String(e.event||'').trim()).filter(Boolean))).sort();
    let html = '<option value="all">All events</option>';
    events.forEach(ev=>{ html += '<option value="'+esc(ev)+'">'+esc(ev)+'</option>'; });
    filterEl.innerHTML = html;
    filterEl.value = (events.includes(keep) ? keep : 'all');
    filterEl.onchange = ()=>loadAudit();
  }

  const activeFilter = (filterEl && filterEl.value) ? filterEl.value : selected;

  const body = qs('#audit-body');
  if(body){
    body.innerHTML = '';
    const shown = entries.filter(e=> activeFilter==='all' ? true : String(e.event||'')===String(activeFilter));
    if(!shown.length){
      body.innerHTML = '<tr><td colspan="6"><span class="note">No audit entries.</span></td></tr>';
    } else {
      shown.forEach(e=>{
        const details = (e.details || {});
        const copyPayload = JSON.stringify(e, null, 2);
        const tr = document.createElement('tr');
        tr.innerHTML =
          '<td>'+esc(e.ts||'')+'</td>'
          +'<td><span class="code">'+esc(e.actor||'')+'</span></td>'
          +'<td>'+esc(e.role||'')+'</td>'
          +'<td>'+esc(e.event||'')+'</td>'
          +'<td><span class="code">'+esc(JSON.stringify(details))+'</span></td>'
          +'<td><button class="btn2" type="button">Copy</button></td>';
        const btn = tr.querySelector('button');
        if(btn){
          btn.addEventListener('click', async ()=>{
            try{
              if(navigator && navigator.clipboard && navigator.clipboard.writeText){
                await navigator.clipboard.writeText(copyPayload);
              } else {
                const ta = document.createElement('textarea');
                ta.value = copyPayload;
                document.body.appendChild(ta);
                ta.select();
                document.execCommand('copy');
                ta.remove();
              }
              if(msg){ msg.textContent = 'Copied'; setTimeout(()=>{ if(msg) msg.textContent=''; }, 800); }
            }catch(_){
              if(msg){ msg.textContent = 'Copy failed'; setTimeout(()=>{ if(msg) msg.textContent=''; }, 1200); }
            }
          });
        }
        body.appendChild(tr);
      });
    }
  }
  if(msg) msg.textContent='';
}



async function loadNotifs(){
  const msg = qs('#notif-msg'); 
  if(msg) msg.textContent = 'Loading…';

  try{
    const r = await fetch(`/admin/api/notifications?limit=50&key=${encodeURIComponent(KEY||'')}`, { cache:'no-store' });
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

          row.innerHTML =
            '<div style="display:flex;justify-content:space-between;gap:10px;flex-wrap:wrap;align-items:center">' +
              '<div class="note">'+esc(it.ts || '')+'</div>' +
              '<div><span class="code">'+esc(it.event || '')+'</span></div>' +
            '</div>' +
            '<div class="note" style="margin-top:6px">Details</div>' +
            '<pre style="margin-top:6px;padding:10px;border-radius:10px;background:rgba(255,255,255,.08);color:#eef2ff;white-space:pre-wrap;word-break:break-word;font-size:12px;line-height:1.45">' +
              esc(text) +
            '</pre>';

          body.appendChild(row);
        });
      }
    }

    if(msg) msg.textContent = '';
  }catch(e){
    if(msg) msg.textContent = 'Load failed';
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
    return "".join(html)




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
        ws = _open_default_spreadsheet(gc).sheet1
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

        if not AI_SETTINGS.get("enabled"):
            return jsonify({"ok": False, "error": "AI disabled"}), 409
        if not (AI_SETTINGS.get("allow_actions") or {}).get("reply_draft", True):
            return jsonify({"ok": False, "error": "reply_draft not allowed"}), 409
        if not _ai_feature_allows("reply_draft"):
            return jsonify({"ok": False, "error": "reply_draft disabled by feature flag"}), 409

        system_msg = (AI_SETTINGS.get("system_prompt") or "").strip()
        user_msg = (
            "Draft a short, premium, action-oriented reply to this lead. "
            "Do NOT mention internal systems. Ask for any missing required details.\n\n"
            f"Name: {lead.get('name')}\nPhone: {lead.get('phone')}\nDate: {lead.get('date')}\nTime: {lead.get('time')}\n"
            f"Party: {lead.get('party_size')}\nBudget: {lead.get('budget')}\nNotes: {lead.get('notes')}\nLanguage: {lead.get('language')}\n"
            "\nReturn plain text only."
        )

        draft_text = ""
        try:
            if not _OPENAI_AVAILABLE or client is None:
                raise RuntimeError("OpenAI SDK missing")
            resp2 = client.responses.create(
                model=AI_SETTINGS.get("model") or os.environ.get("CHAT_MODEL","gpt-4o-mini"),
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
            "created_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "created_by": actor,
            "created_role": role,
            "status": "pending",
            "source": "reply_draft",
            "confidence": 0.6,
            "notes": "Owner-requested reply draft",
            "actions": [
                {"type":"reply_draft","payload":{"row": row_num, "draft": draft_text[:2000]}, "reason":"Draft reply for staff review"}
            ],
            "lead": {
                "intent": lead.get("entry_point",""),
                "contact": (lead.get("name","") + " " + lead.get("phone","")).strip(),
                "budget": lead.get("budget",""),
                "party_size": lead.get("party_size",""),
                "datetime": (lead.get("date","") + " " + lead.get("time","")).strip(),
            }
        }
        _queue_add(entry)
        _audit("ai.queue.created", {"id": entry["id"], "source": "reply_draft", "row": row_num})
        return jsonify({"ok": True, "queued": entry})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/admin/api/analytics/load-forecast", methods=["GET"])
def admin_api_load_forecast():
    """Step 13: read-only load forecast (manager+)."""
    ok, resp = _require_admin(min_role="manager")
    if not ok:
        return resp

    try:
        gc = get_gspread_client()
        ws = _open_default_spreadsheet(gc).sheet1
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
        ws = _open_default_spreadsheet(gc).sheet1
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
            acts = it.get("actions") or []
            if isinstance(acts, list):
                for a in acts:
                    prow = (a.get("payload") or {}).get("row")
                    if str(prow) == str(row_num):
                        related.append(it)
                        break
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

.wrap{max-width:1200px;margin:0 auto;padding:18px;}
.topbar{display:flex;align-items:center;justify-content:space-between;gap:12px;margin-bottom:14px;}
.h1{font-size:18px;font-weight:800;letter-spacing:.3px}
.sub{color:var(--muted);font-size:12px}

.pills{display:flex;gap:8px;flex-wrap:wrap}
.pill{border:1px solid var(--line);background:rgba(255,255,255,.03);padding:8px 10px;border-radius:999px;font-size:12px}
.pill b{color:var(--gold)}

.controls{display:flex;gap:10px;flex-wrap:wrap;margin:12px 0 14px}

.inp, select{
  background:rgba(255,255,255,.04);
  border:1px solid var(--line);
  color:var(--menu-text);
  border-radius:10px;
  padding:9px 10px;
  font-size:12px;
  outline:none;
}

select option{
  background:var(--menu-bg);
  color:var(--menu-text);
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
    html.append("<a class='btn' href='#fanzone' style='text-decoration:none;display:inline-block' onclick=\"showTab('fanzone');return false;\">Poll Controls</a>")
    html.append("</div></div>")

    html.append(f"<div style='display:flex;gap:8px;margin:10px 0 14px 0;flex-wrap:wrap;'>"
                f"<a href='/admin?key={key}' style='text-decoration:none;color:var(--text);padding:8px 12px;border:1px solid var(--line);border-radius:999px;background:rgba(255,255,255,.04);font-weight:800'>Ops</a>"
                "<a href='#fanzone' onclick=\"showTab('fanzone');return false;\" style='text-decoration:none;color:var(--text);padding:8px 12px;border:1px solid rgba(212,175,55,.35);border-radius:999px;background:rgba(212,175,55,.10);font-weight:900'>Fan Zone</a>"
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

</div> <!-- end tab-menu -->




<script>
(function(){
  const qs = new URLSearchParams(location.search);
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

  function setPollStatus(html){
    const box = $("pollStatus");
    if(!box) return;
    box.innerHTML = html;
  }

  async function loadPollStatus(){
    try{
      setPollStatus('<div class="sub">Loading poll status…</div>');
      const res = await fetch("/api/poll/state", {cache:"no-store"});
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

  async function saveFanZoneConfig(){
    const btn = $("btnSaveConfig");
    const sel = $("motdSelect");
    const lockEl = $("pollLockMode");
    if(!btn) return;

    const payload = {
      section: "fanzone",
      motd: {
        home: ($("motdHome")?.value || "").trim(),
        away: ($("motdAway")?.value || "").trim(),
        kickoff_utc: ($("motdKickoff")?.value || "").trim(),
        match_id: (sel?.value || "").trim(),
      },
      poll: {
        lock_mode: (lockEl?.value || "").trim(), // "auto" | "force_unlocked" | "force_locked"
      }
    };

    const prev = btn.textContent;
    btn.disabled = true;
    btn.textContent = "Saving…";

    try{
      const res = await fetch(`/admin/update-config?key=${encodeURIComponent(ADMIN_KEY)}`, {
        method: "POST",
        headers: {"Content-Type":"application/json"},
        body: JSON.stringify(payload)
      });
      const data = await res.json().catch(()=>null);
      if(!res.ok || !data || data.ok === false){
        toast((data && data.error) ? data.error : "Save failed", "error");
        btn.textContent = prev;
        btn.disabled = false;
        return;
      }
      btn.textContent = "Saved ✓";
      toast("Saved", "ok");
      setTimeout(()=>{ btn.textContent = prev; btn.disabled = false; }, 900);

      // Refresh poll status after save so staff trust the click
      loadPollStatus();
    }catch(e){
      toast("Save failed", "error");
      btn.textContent = prev;
      btn.disabled = false;
    }
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
  if(!row){ if(msg) msg.textContent = 'Enter a sheet row #'; return; }
  try{
    const r = await fetch(`/admin/api/ai/replay?key=${encodeURIComponent(KEY)}`, {
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
    return "".join(html)




# ============================================================
# Concierge intake API (writes into Admin Leads sheet)
# ============================================================
@app.route("/api/intake", methods=["POST"])
def api_intake():
    payload = request.get_json(silent=True) or {}
    # Venue deactivation: block fan intake when the venue is inactive.
    vid = _venue_id()
    if not _venue_is_active(vid):
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
        append_lead_to_sheet(lead)
        _audit("intake.new", {"entry_point": entry_point, "tier": tier})
        return jsonify({"ok": True, "tier": tier})
    except Exception as e:
        return jsonify({"ok": False, "error": "Failed to store intake"}), 500

    # ============================================================
    # Leads intake (used by the new UI)
    # - Stores locally to static/data/leads.jsonl
    # - Optionally appends to Google Sheets if configured (same creds as admin/chat)
    # ============================================================
    LEADS_STORE_PATH = os.environ.get("LEADS_STORE_PATH", "static/data/leads.jsonl")
    GOOGLE_SHEET_ID = os.environ.get("GOOGLE_SHEET_ID", "").strip()



@app.route("/api/venue_identity")
def api_venue_identity():
    """Fan-safe: minimal venue identity (branding-safe).

    Returns:
      - show_location_line (bool feature flag)
      - location_line (string)
    """
    try:
        vid = _venue_id()
    except Exception:
        vid = DEFAULT_VENUE_ID

    # ✅ NEW: if venue is inactive, hide identity (prevents lingering fan views)
    if not _venue_is_active(vid):
        return jsonify({"ok": False, "error": "Venue is inactive"}), 404

    cfg = _venue_cfg(vid) or {}
    feat = cfg.get("features") if isinstance(cfg.get("features"), dict) else {}
    ident = cfg.get("identity") if isinstance(cfg.get("identity"), dict) else {}

    # Prefer top-level (new schema), fallback to legacy nested keys
    show = bool(cfg.get("show_location_line", (feat or {}).get("show_location_line", False)))
    loc = str(cfg.get("location_line") or (ident or {}).get("location_line") or "").strip()

    return jsonify({
        "ok": True,
        "venue_id": vid,
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
    _append_lead_local(row)

    sheet_ok = False
    sheet_row = 0
    if GOOGLE_SHEET_ID or os.environ.get("GOOGLE_CREDS_JSON") or os.path.exists("google_creds.json"):
        sheet_ok, sheet_row = _append_lead_google_sheet(row)

    # Step 7: AI triage on intake → queue suggestions (approval by manager/admin)
    # Best-effort; never blocks lead capture if AI fails.
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
  color:var(--menu-text);
}
select option{
  background:var(--menu-bg) !important;
  color:var(--menu-text) !important;
}
select option:hover{
  background:var(--menu-hover) !important;
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
  color:var(--menu-text);
  border-radius:10px;
  padding:8px 10px;
  font-size:13px;
}
select option{
  background:var(--menu-bg) !important;
  color:var(--menu-text) !important;
}
select option:hover{
  background:var(--menu-hover) !important;
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
    const sheet_ok = !!(v.sheet && v.sheet.ok);
    const ready = !!(v.ready);
    const needs = active && (!sheet_ok || !ready);
    return {active, sheet_ok, ready, needs};
  }
  function computeCounts(){
    const vs=state.venues||[]; let all=vs.length, active=0, inactive=0, sheetfail=0, notready=0, needs=0;
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
    const filtered=(state.venues||[]).filter(v=>matchesFilter(v)&&matchesSearch(v));

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
        const badges=[
          '<span class="badge '+(f.active?'good':'warn')+'">'+(f.active?'ACTIVE':'INACTIVE')+'</span>',
          '<span class="badge '+(f.sheet_ok?'good':'bad')+'">'+(f.sheet_ok?'SHEET OK':'SHEET FAIL')+'</span>',
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
        const sheetBadge='<span class="badge '+(f.sheet_ok?'good':'bad')+'">'+(f.sheet_ok?'OK':'FAIL')+'</span>';
        const actBadge='<span class="badge '+(f.active?'good':'warn')+'">'+(f.active?'ACTIVE':'INACTIVE')+'</span>';
        const readyBadge='<span class="badge '+(f.ready?'good':'warn')+'">'+(f.ready?'READY':'NOT READY')+'</span>';
        const actions='<button class="btn" data-act="check" data-vid="'+hesc(v.venue_id||'')+'">Re-check</button> '+
                      '<button class="btn" data-act="rotate" data-vid="'+hesc(v.venue_id||'')+'">Rotate Keys</button> '+
                      '<a href="/admin?venue='+encodeURIComponent(v.venue_id||'')+'" target="_blank">Open</a>';
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
    sel.innerHTML='<option value="">All venues</option>'+(state.venues||[]).map(v=>'<option value="'+hesc(v.venue_id||'')+'">'+hesc(v.name||v.venue_id)+'</option>').join('');
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
      '<span class="badge '+(f.sheet_ok?'good':'bad')+'">'+(f.sheet_ok?'SHEET OK':'SHEET FAIL')+'</span>'+
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
      '<a class="btn" style="text-decoration:none" href="/admin?venue='+encodeURIComponent(v.venue_id||'')+'" target="_blank">Open Admin</a>'+
    '</div>';

  // existing actions
  document.getElementById('vdCheck').onclick = () => doVenueAction('check', v.venue_id);
  document.getElementById('vdRotate').onclick = () => doVenueAction('rotate', v.venue_id);

  const _vdA = document.getElementById('vdActive');
  if(_vdA) _vdA.onclick = () => doVenueAction('set_active', v.venue_id, {active: !f.active});

  const _vdD = document.getElementById('vdDemo');
  if(_vdD) _vdD.onclick = () => toggleDemoMode();

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
    if(!url) return;
    try{
      const r=await fetch(url+'?super_key='+encodeURIComponent(super_key), {method:'POST', headers: hdrs(), body: JSON.stringify(payload)});
      const j=await r.json().catch(()=>({}));
      if(!j.ok){ setDiag(JSON.stringify(j||{}, null, 2)); alert('Action failed: '+(j.error||r.status)); }
      else { setDiag(JSON.stringify(j||{}, null, 2)); }
      await loadVenues();
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
    """Back-compat: super admin UI entrypoint.

    Historically some links used /admin/api/super. The Super Admin console UI
    actually lives at /super/admin. This route keeps old links working and
    prevents falling back to the fan UI shell.
    """
    try:
        if not _is_super_admin_request():
            return "Forbidden", 403
        # Preserve query params so the embedded console JS can call /super/api/*
        # NOTE: keep both `key` and `super_key` in the URL.
        q = request.query_string.decode("utf-8") if request.query_string else ""
        url = "/super/admin"
        if q:
            url = url + "?" + q
        return redirect(url, code=302)
    except Exception:
        return ("Forbidden", 403)

@app.get("/super/admin")
def super_admin_console():
    """
    Super Admin console. Must NEVER hard-500; if something goes wrong we return
    a minimal diagnostic page so you can see the real exception (production-safe
    because it's still protected by SUPER_ADMIN_KEY).
    """
    try:
        if not _is_super_admin_request():
            return "Forbidden", 403
        resp = make_response(render_template_string(SUPER_CONSOLE_HTML))
        try:
            sk = (request.args.get("super_key") or request.headers.get("X-Super-Key") or "").strip()
            if sk:
                resp.set_cookie("super_key", sk, httponly=True, samesite="Lax")
        except Exception:
            pass
        return resp
    except Exception as e:
        tb = traceback.format_exc()
        # still return 200 so the platform doesn't show a generic error page
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
    """Quick diagnostics for Super Admin (requires SUPER_ADMIN_KEY)."""
    if not _is_super_admin_request():
        return jsonify({"ok": False, "error": "forbidden"}), 403
    try:
        # Basic environment + template sanity
        return jsonify({
            "ok": True,
            "path": request.path,
            "has_super_key": bool(SUPER_ADMIN_KEY),
            "redis_enabled": bool(_REDIS_ENABLED),
            "redis_namespace": _REDIS_NS,
            "venues_count": (len(_load_venues_from_disk() or {}) if isinstance(_load_venues_from_disk(), dict) else 0),
            "app_version": (os.environ.get("APP_VERSION") or "1.4.2"),
        })
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

@app.route("/super/api/venues/set_sheet", methods=["POST", "OPTIONS"])
def super_api_venues_set_sheet():
    if request.method == "OPTIONS":
        return ("", 204)

    if not _is_super_admin_request():
        return jsonify({"ok": False, "error": "forbidden"}), 403

    body = request.get_json(silent=True) or {}
    venue_id = _slugify_venue_id(str(body.get("venue_id") or "").strip())
    sheet_id = str(body.get("sheet_id") or body.get("google_sheet_id") or "").strip()

    if not venue_id:
        return jsonify({"ok": False, "error": "missing venue_id"}), 400
    if not sheet_id:
        return jsonify({"ok": False, "error": "missing sheet_id"}), 400

    cfg = _venue_cfg(venue_id)
    path = str(cfg.get("_path") or "")
    if not path:
        return jsonify({"ok": False, "error": "venue config not found"}), 404

@app.route("/super/api/venues/set_location", methods=["POST","OPTIONS"])
def super_api_venues_set_location():
    if request.method == "OPTIONS":
        return ("", 204)
    key = request.headers.get("X-Super-Key") or request.args.get("super_key")
    if key != SUPER_ADMIN_KEY:
        return jsonify({"ok": False, "error": "unauthorized"}), 403

    body = request.get_json(silent=True) or {}
    venue_id = _slugify_venue_id(str(body.get("venue_id") or "").strip())
    location_line = str(body.get("location_line") or "").strip()

    if not venue_id:
        return jsonify({"ok": False, "error": "missing venue_id"}), 400
    if not location_line:
        return jsonify({"ok": False, "error": "missing location_line"}), 400

    cfg = _venue_cfg(venue_id)
    path = str(cfg.get("_path") or "")
    if not path:
        return jsonify({"ok": False, "error": "venue config not found"}), 404

    cfg["show_location_line"] = True
    cfg["location_line"] = location_line
    cfg["updated_at"] = datetime.now(timezone.utc).isoformat().replace("+00:00","Z")

    _safe_write_json_file(path, cfg)
    _invalidate_venues_cache()
    return jsonify({"ok": True, "venue_id": venue_id, "location_line": location_line})

    # persist sheet id in the canonical place
    cfg["data"] = cfg.get("data", {}) if isinstance(cfg.get("data"), dict) else {}
    cfg["data"]["google_sheet_id"] = sheet_id

    # validate + persist status flags
    chk = _check_sheet_id(sheet_id)
    cfg["sheet_ok"] = bool(chk.get("ok"))
    cfg["ready"] = bool(cfg["sheet_ok"] and cfg.get("active", True))
    cfg["last_checked"] = chk.get("checked_at")

    _safe_write_json_file(path, cfg)
    _invalidate_venues_cache()

    return jsonify({"ok": True, "venue_id": venue_id, "sheet_id": sheet_id, "check": chk})

@app.route("/super/api/venues/check_sheet", methods=["POST", "OPTIONS"])
def super_api_venues_check_sheet():
    if request.method == "OPTIONS":
        return ("", 204)

    if not _is_super_admin_request():
        return jsonify({"ok": False, "error": "forbidden"}), 403

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
    if request.method == "OPTIONS":
        return ("", 204)

    key = request.headers.get("X-Super-Key") or request.args.get("super_key")
    if key != SUPER_ADMIN_KEY:
        return jsonify({"ok": False, "error": "unauthorized"}), 403

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
    if not _is_super_admin_request():
        return jsonify({"ok": False, "error": "forbidden"}), 403

    venues = _load_venues_from_disk()
    out = []
    total = {"venues": 0, "ai_queue": 0}
    for vid, cfg in (venues or {}).items():
        try:
            # count AI queue items per venue without relying on request-scoped _venue_id()
            count = 0
            if _REDIS_ENABLED and _REDIS:
                try:
                    payload = _redis_get_json(f"{_REDIS_NS}:{vid}:ai_queue", default=[])
                    if isinstance(payload, list):
                        count = len(payload)
                except Exception:
                    count = 0
            else:
                path = str(AI_QUEUE_FILE).replace("{venue}", vid)
                payload = _safe_read_json_file(path, default=[])
                if isinstance(payload, list):
                    count = len(payload)
            out.append({
                "venue_id": vid,
                "active": bool(_venue_is_active(vid)),
                "venue_name": str((cfg or {}).get("venue_name") or (cfg or {}).get("name") or vid),
                "ai_queue": int(count),
            })
            total["venues"] += 1
            total["ai_queue"] += int(count)
        except Exception:
            # best effort — never break the dashboard
            continue

    out.sort(key=lambda x: x.get("venue_id",""))
    return jsonify({"ok": True, "total": total, "venues": out})



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
    if request.method == "OPTIONS":
        return ("", 204, {
            "Access-Control-Allow-Methods": "POST, OPTIONS",
            "Access-Control-Allow-Headers": "Content-Type, X-Super-Key",
        })

    # Normalized Super Admin auth
    key = request.headers.get("X-Super-Key") or request.args.get("super_key")
    if key != SUPER_ADMIN_KEY:
        return jsonify({"ok": False, "error": "unauthorized"}), 403

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
    k = (request.args.get("key") or request.args.get("super_key") or request.headers.get("X-Super-Key") or "").strip()
    sk = _get_super_admin_key()
    return bool(sk) and k == sk

def _require_super_admin():
    if not _is_super_admin_request():
        return False, (jsonify({"ok": False, "error": "forbidden"}), 403)
    return True, None


# SIZE_PAD_START
###########################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################

# ============================================================
# Enterprise shared state in Redis (Ops / FanZone / Polls)
# ============================================================
_OPS_KEY = "ops_state"
_FANZONE_KEY = "fanzone_state"

def _ops_state_default():
    return {"pause": False, "viponly": False, "waitlist": False, "notify": False,
            "updated_at": None, "updated_by": None, "updated_role": None}

def _load_ops_state() -> Dict[str, Any]:
    if _REDIS_ENABLED:
        st = _redis_get_json(_OPS_KEY, default=None)
        if isinstance(st, dict):
            return _deep_merge(_ops_state_default(), st)
    return _ops_state_default()

def _save_ops_state(patch: Dict[str, Any], actor: str, role: str) -> Dict[str, Any]:
    st = _deep_merge(_load_ops_state(), patch or {})
    st["updated_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    st["updated_by"] = actor
    st["updated_role"] = role
    if _REDIS_ENABLED:
        _redis_set_json(_OPS_KEY, st)
    return st

def _load_fanzone_state() -> Dict[str, Any]:
    if _REDIS_ENABLED:
        st = _redis_get_json(_FANZONE_KEY, default=None)
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
        _redis_set_json(_FANZONE_KEY, st2)
    else:
        _safe_write_json_file(POLL_STORE_FILE, st2)
    return st2

# ============================================================
# Super Admin: Venue Onboarding (writes config when possible)
# ============================================================
@app.get("/super/api/venues")
def super_api_venues_list():
    # Normalized Super Admin auth
    key = request.headers.get("X-Super-Key") or request.args.get("super_key")
    if key != SUPER_ADMIN_KEY:
        return jsonify({"ok": False, "error": "unauthorized"}), 403

    # CRITICAL: force fresh disk read (Refresh must not be stale)
    try:
        _invalidate_venues_cache()
    except Exception:
        pass

    venues = _load_venues_from_disk()
    out = []
    if isinstance(venues, dict):
        for vid, cfg in sorted(venues.items(), key=lambda kv: kv[0]):
            if not isinstance(cfg, dict):
                continue

            name = str(cfg.get("venue_name") or cfg.get("name") or vid)
            plan = str(cfg.get("plan") or "")

            # Read sheet id directly from config
            sid = _venue_sheet_id(vid)

            # Use the NEW persisted fields (not legacy _sheet_check)
            sheet_ok = cfg.get("sheet_ok", None)
            ready = bool(cfg.get("ready", False))
            active = bool(_venue_is_active(vid))

            status = str(cfg.get("status") or "").strip()
            if not sid:
                status = "MISSING_SHEET"
            elif sheet_ok is True and ready:
                status = "READY"
            elif sheet_ok is False:
                status = "SHEET_FAIL"
            else:
                status = status or "SHEET_SET"

            out.append({
                "venue_id": vid,
                "venue_name": name,
                "plan": plan,
                "google_sheet_id": sid,
                "status": status,
                "active": active,
                "sheet_ok": sheet_ok,

                # ✅ ADDED: UI expects v.sheet.ok
                "sheet": {
                    "ok": sheet_ok,
                    "last_checked": cfg.get("last_checked"),
                    "sheet_id": sid,
                },

                "ready": ready,
                "last_checked": cfg.get("last_checked"),
            })

        return jsonify({"ok": True, "venues": out})

    body = request.get_json(silent=True) or {}
    venue_id = _slugify_venue_id(str(body.get("venue_id") or ""))
    sheet_id = str(body.get("google_sheet_id") or "").strip()
    if not venue_id:
        return jsonify({"ok": False, "error": "missing_venue_id"}), 400
    if not sheet_id:
        return jsonify({"ok": False, "error": "missing_google_sheet_id"}), 400

    venues = _load_venues_from_disk() or {}
    cfg = venues.get(venue_id) if isinstance(venues, dict) else None
    if not isinstance(cfg, dict):
        return jsonify({"ok": False, "error": "venue_not_found"}), 404

    path = str(cfg.get("_path") or "")
    if not path:
        # fallback to expected json path
        path = os.path.join(VENUES_DIR, f"{venue_id}.json")

    wrote = False
    err = ""
    try:
        # load existing file as dict
        cur = {}
        try:
            cur_txt = pathlib.Path(path).read_text(encoding="utf-8")
            cur = json.loads(cur_txt) if cur_txt else {}
        except Exception:
            cur = cfg.copy()

        if not isinstance(cur, dict):
            cur = {}

        data = cur.get("data") if isinstance(cur.get("data"), dict) else {}
        data["google_sheet_id"] = sheet_id
        cur["data"] = data

        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(cur, f, indent=2, sort_keys=True)

        wrote = True
        _MULTI_VENUE_CACHE["ts"] = 0.0
    except Exception as e:
        err = str(e)

    try:
        _audit("super.venues.set_sheet", {"venue_id": venue_id, "sheet_id": sheet_id, "persisted": wrote, "path": path, "error": err})
    except Exception:
        pass

    return jsonify({"ok": True, "venue_id": venue_id, "google_sheet_id": sheet_id, "persisted": wrote, "path": path, "error": err})


@app.post("/super/api/venues/rotate_keys")
def super_api_venues_rotate_keys():
    """Super-admin: rotate a venue's keys and attempt to persist."""
    if not _is_super_admin_request():
        return jsonify({"ok": False, "error": "forbidden"}), 403


    if _demo_mode_enabled():
        return jsonify({"ok": False, "error": "demo_mode: write disabled"}), 403

    body = request.get_json(silent=True) or {}
    venue_id = _slugify_venue_id(str(body.get("venue_id") or "").strip())
    if not venue_id:
        return jsonify({"ok": False, "error": "venue_id required"}), 400

    rotate_admin = bool(body.get("rotate_admin", True))
    rotate_manager = bool(body.get("rotate_manager", True))

    venues = _load_venues_from_disk() or {}
    cfg = venues.get(venue_id)
    if not isinstance(cfg, dict):
        return jsonify({"ok": False, "error": "unknown venue"}), 404

    keys = cfg.get("keys") if isinstance(cfg.get("keys"), dict) else {}
    new_admin = secrets.token_hex(16) if rotate_admin else keys.get("admin_key")
    new_manager = secrets.token_hex(16) if rotate_manager else keys.get("manager_key")
    keys = {"admin_key": new_admin, "manager_key": new_manager}
    cfg["keys"] = keys
    cfg["updated_at"] = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

    wrote = False
    write_path = ""
    err = ""
    try:
        os.makedirs(VENUES_DIR, exist_ok=True)
        write_path = os.path.join(VENUES_DIR, f"{venue_id}.json")
        with open(write_path, "w", encoding="utf-8") as f:
            json.dump(cfg, f, indent=2, sort_keys=True)
        wrote = True
        _MULTI_VENUE_CACHE["ts"] = 0.0
    except Exception as e:
        err = str(e)

    try:
        _audit("super.venues.rotate_keys", {"venue_id": venue_id, "persisted": wrote, "path": write_path, "error": err, "rotate_admin": rotate_admin, "rotate_manager": rotate_manager})
    except Exception:
        pass

    return jsonify({"ok": True, "venue_id": venue_id, "keys": keys, "qr_url": f"https://worldcupconcierge.app/v/{venue_id}", "admin_url": f"https://admin.worldcupconcierge.app/v/{venue_id}/admin?key={keys.get('admin_key')}", "manager_url": f"https://manager.worldcupconcierge.app/v/{venue_id}/manager?key={keys.get('manager_key')}", "persisted": wrote, "path": write_path, "error": err})

@app.get("/super/api/leads")
def super_api_leads():
    """Cross-venue leads for Super Admin (read-only)."""
    # Normalized Super Admin auth
    key = request.headers.get("X-Super-Key") or request.args.get("super_key")
    if key != SUPER_ADMIN_KEY:
        return jsonify({"ok": False, "error": "unauthorized"}), 403

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
    """Best-effort validation that the service account can open the given sheet."""
    if not _is_super_admin_request():
        return jsonify({"ok": False, "error": "forbidden"}), 403

    sheet_id = (request.args.get("sheet_id") or "").strip()
    if not sheet_id:
        return jsonify({"ok": False, "error": "sheet_id required"}), 400

    if not _GSPREAD_AVAILABLE:
        return jsonify({"ok": False, "error": "gspread not available in this runtime"}), 400

    try:
        gc = _get_gspread_client()
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
        return jsonify({"ok": True, "sheet_id": sheet_id, "title": title})
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

        # keep correct sheet row numbers: header is row 1, data starts row 2
        for off, r in enumerate(body):
            if not isinstance(r, list):
                continue
            obj = {
                "_venue_id": venue_id,
                "sheet_row": off + 2,
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
        "count": total,
        "page": page,
        "per_page": per_page,
        "pages": pages,
        "items": page_items,
        "errors": errors
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
    # Multi-venue: expand {venue} placeholder
    try:
        vid = _venue_id()
        if '{venue}' in str(path):
            path = str(path).replace('{venue}', vid)
    except Exception:
        pass
    try:
        if _REDIS_ENABLED:
            suffix = _REDIS_PATH_KEY_MAP.get(path)
            if suffix:
                full_key = f"{_REDIS_NS}:{_venue_id()}:{suffix}"
                return _redis_get_json(full_key, default=default)
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
    # Multi-venue: expand {venue} placeholder
    try:
        vid = _venue_id()
        if '{venue}' in str(path):
            path = str(path).replace('{venue}', vid)
    except Exception:
        pass
    try:
        if _REDIS_ENABLED:
            suffix = _REDIS_PATH_KEY_MAP.get(path)
            if suffix:
                full_key = f"{_REDIS_NS}:{_venue_id()}:{suffix}"
                ok = _redis_set_json(full_key, payload)
                if ok:
                    return
                # Redis was enabled, but write failed — mark fallback for enterprise gate
                _REDIS_FALLBACK_USED = True
                _REDIS_FALLBACK_LAST_PATH = str(path)
    except Exception:
        # Mark fallback on unexpected redis path errors too (best effort)
        try:
            _REDIS_FALLBACK_USED = True
            _REDIS_FALLBACK_LAST_PATH = str(path)
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
    desired = ["timestamp", "name", "phone", "date", "time", "party_size", "language", "status", "vip", "entry_point", "tier", "queue", "business_context", "budget", "notes", "vibe"]

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
    ws = _open_default_spreadsheet(gc).sheet1

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
    setv("entry_point", lead.get("entry_point",""))
    setv("tier", lead.get("tier",""))
    setv("queue", lead.get("queue",""))
    setv("business_context", lead.get("business_context",""))
    setv("budget", lead.get("budget",""))
    setv("notes", lead.get("notes",""))
    setv("vibe", lead.get("vibe",""))

    # Append at bottom (keeps headers at the top)
    ws.append_row(row, value_input_option="USER_ENTERED")


# Small per-venue read cache to avoid Sheets 429s
_LEADS_CACHE_BY_VENUE: Dict[str, Dict[str, Any]] = {}

def read_leads(limit: int = 200, venue_id: Optional[str] = None) -> List[List[str]]:
    """Read leads from the venue's Google Sheet tab (best-effort, cached).

    Returns rows including header row (row 1) as rows[0].
    """
    vid = _slugify_venue_id(venue_id or _venue_id())
    now = time.time()

    cache = _LEADS_CACHE_BY_VENUE.get(vid) or {}
    rows_cached = cache.get("rows")
    if isinstance(rows_cached, list) and (now - float(cache.get("ts") or 0.0) < 9.0):
        return rows_cached[:limit] if limit else rows_cached

    try:
        ws = get_sheet(venue_id=vid)  # uses venue sheet_name when present
        rows = ws.get_all_values() or []
        # cache regardless; even empty is useful to avoid hammering
        _LEADS_CACHE_BY_VENUE[vid] = {"ts": now, "rows": rows}
        return rows[:limit] if limit else rows
    except Exception:
        # fallback to cached rows on error
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

def _read_notifications(limit: int = 50, role: str = "manager") -> List[Dict[str, Any]]:
    """
    Read newest notifications first; filter by role.
    Managers see entries targeted to manager/all; Owners see everything.
    """
    try:
        if not os.path.exists(NOTIFICATIONS_FILE):
            return []
        items: List[Dict[str, Any]] = []
        with open(NOTIFICATIONS_FILE, "rb") as f:
            f.seek(0, os.SEEK_END)
            size = f.tell()
            buf = b""
            step = 4096
            while size > 0 and len(items) < limit * 3:
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
                        items.append(it)
                    except Exception:
                        continue
                    if len(items) >= limit * 3:
                        break
        out: List[Dict[str, Any]] = []
        for it in items:
            t = it.get("targets") or []
            if role == "owner":
                out.append(it)
            else:
                if "all" in t or "manager" in t:
                    out.append(it)
            if len(out) >= limit:
                break
        return out
    except Exception:
        return []

def _audit(event: str, details: Optional[Dict[str, Any]] = None) -> None:
    """Append a single-line JSON audit entry (best-effort, non-blocking)."""
    try:
        ctx = _admin_ctx()
        entry = {
            "ts": datetime.utcnow().isoformat(timespec="seconds") + "Z",
            "event": str(event),
            "role": ctx.get("role", ""),
            "actor": ctx.get("actor", ""),
            "ip": client_ip() if request else "",
            "path": getattr(request, "path", ""),
            "details": details or {},
        }
        os.makedirs(os.path.dirname(AUDIT_LOG_FILE), exist_ok=True)
        with open(AUDIT_LOG_FILE, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")
    except Exception:
        pass


# -----------------------------
# Lightweight in-process caches
# (reduces Google Sheets quota hits)
# -----------------------------
_CONFIG_CACHE: Dict[str, Any] = {"ts": 0.0, "cfg": None}   # ttl ~5s
_LEADS_CACHE: Dict[str, Any] = {"ts": 0.0, "rows": None}  # ttl ~30s
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

def _ensure_ws(gc, title: str):
    sh = _open_default_spreadsheet(gc)
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
        "ops_pause_reservations": "false",
        "ops_vip_only": "false",
        "ops_waitlist_mode": "false",
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
    k = (request.args.get("key") or request.args.get("super_key") or request.headers.get("X-Super-Key") or "").strip()
    sk = _get_super_admin_key()
    return bool(sk) and k == sk

def _require_super_admin():
    if not _is_super_admin_request():
        return False, (jsonify({"ok": False, "error": "forbidden"}), 403)
    return True, None


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5050))
    app.run(host="0.0.0.0", port=port, debug=False)
# SIZE_PAD_START
###########################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################

# ============================================================
# Enterprise shared state in Redis (Ops / FanZone / Polls)
# ============================================================
_OPS_KEY = "ops_state"
_FANZONE_KEY = "fanzone_state"

def _ops_state_default():
    return {"pause": False, "viponly": False, "waitlist": False, "notify": False,
            "updated_at": None, "updated_by": None, "updated_role": None}

def _load_ops_state() -> Dict[str, Any]:
    if _REDIS_ENABLED:
        st = _redis_get_json(_OPS_KEY, default=None)
        if isinstance(st, dict):
            return _deep_merge(_ops_state_default(), st)
    return _ops_state_default()

def _save_ops_state(patch: Dict[str, Any], actor: str, role: str) -> Dict[str, Any]:
    st = _deep_merge(_load_ops_state(), patch or {})
    st["updated_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    st["updated_by"] = actor
    st["updated_role"] = role
    if _REDIS_ENABLED:
        _redis_set_json(_OPS_KEY, st)
    return st

def _load_fanzone_state() -> Dict[str, Any]:
    if _REDIS_ENABLED:
        st = _redis_get_json(_FANZONE_KEY, default=None)
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
        _redis_set_json(_FANZONE_KEY, st2)
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