from __future__ import annotations

import os, json, secrets
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from flask import Flask, request, jsonify, send_from_directory

# ============================================================
# PRODUCTION-SAFE APP.PY
# - Lazy-imports OpenAI + gspread so missing deps don't crash the server.
# - Leads (Google Sheet) + Admin CRM controls (Status + VIP toggle)
# - Fan Poll: persisted (sheet if available; else /tmp JSON), live %,
#   lock-at-kickoff, winner highlight, sponsor label + match override editable in /admin.
# - Qualified countries endpoint (as-of 18 Nov 2025 list from Wikipedia)
# ============================================================

APP_DIR = os.path.dirname(os.path.abspath(__file__))

ADMIN_KEY = os.environ.get("ADMIN_KEY", "").strip()

SHEET_NAME = os.environ.get("SHEET_NAME", "WorldCupLeads").strip()
LEADS_WS_TITLE = os.environ.get("LEADS_WS_TITLE", "Leads").strip()

POLL_CONFIG_WS_TITLE = os.environ.get("POLL_CONFIG_WS_TITLE", "PollConfig").strip()
POLL_VOTES_WS_TITLE  = os.environ.get("POLL_VOTES_WS_TITLE",  "PollVotes").strip()

# Fallback local stores (used only if Google Sheets is unavailable)
LEADS_LOCAL_FILE = os.environ.get("LEADS_LOCAL_FILE", "/tmp/wc26_leads.json")
POLL_LOCAL_FILE  = os.environ.get("POLL_LOCAL_FILE",  "/tmp/wc26_poll.json")

QUALIFIED_COUNTRIES = ["Canada", "Mexico", "United States", "Japan", "New Zealand", "Iran", "Argentina", "Uzbekistan", "South Korea", "Jordan", "Australia", "Brazil", "Ecuador", "Uruguay", "Colombia", "Paraguay", "Morocco", "Tunisia", "Egypt", "Algeria", "Ghana", "Cape Verde", "South Africa", "Qatar", "England", "Saudi Arabia", "Ivory Coast", "Senegal", "France", "Croatia", "Portugal", "Norway", "Germany", "Netherlands", "Belgium", "Austria", "Switzerland", "Spain", "Scotland", "Panama", "Haiti", "Curaçao"]

app = Flask(__name__, static_folder=APP_DIR, static_url_path="")

# ------------------------------------------------------------
# Helpers: safe json read/write
# ------------------------------------------------------------
def _safe_read_json(path: str, default: Any) -> Any:
    try:
        if not os.path.exists(path):
            return default
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default

def _safe_write_json(path: str, payload: Any) -> None:
    try:
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        os.replace(tmp, path)
    except Exception:
        pass

def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

# ------------------------------------------------------------
# Lazy Google Sheets
# ------------------------------------------------------------
def _get_gspread() -> Tuple[Optional[Any], Optional[str]]:
    """Returns (gspread_client, error_string)."""
    try:
        import gspread
        from google.oauth2.service_account import Credentials
    except Exception as e:
        return None, f"Missing dependency: {type(e).__name__}"

    creds_json = os.environ.get("GOOGLE_CREDENTIALS_JSON", "").strip()
    if not creds_json:
        creds_path = os.environ.get("GOOGLE_CREDENTIALS_FILE", "").strip()
        if creds_path and os.path.exists(creds_path):
            with open(creds_path, "r", encoding="utf-8") as f:
                creds_json = f.read()
    if not creds_json:
        return None, "Missing GOOGLE_CREDENTIALS_JSON (or GOOGLE_CREDENTIALS_FILE)"

    try:
        info = json.loads(creds_json)
        scopes = ["https://www.googleapis.com/auth/spreadsheets"]
        creds = Credentials.from_service_account_info(info, scopes=scopes)
        return gspread.authorize(creds), None
    except Exception as e:
        return None, f"Google auth error: {type(e).__name__}: {e}"

def _open_sheet(ws_title: str):
    gc, err = _get_gspread()
    if not gc:
        raise RuntimeError(err or "Google Sheets unavailable")
    sh = gc.open(SHEET_NAME)
    try:
        return sh.worksheet(ws_title)
    except Exception:
        return sh.add_worksheet(title=ws_title, rows=1000, cols=20)

# ------------------------------------------------------------
# Leads schema + CRUD
# ------------------------------------------------------------
LEADS_HEADER = ["timestamp","name","phone","date","time","party_size","language","status","vip"]

def _ensure_leads_schema(ws) -> List[str]:
    rows = ws.get_all_values() or []
    if not rows:
        ws.update("A1", [LEADS_HEADER])
        return LEADS_HEADER
    header = rows[0]
    normalized = [h.strip() for h in header]
    changed = False
    for col in LEADS_HEADER:
        if col not in normalized:
            normalized.append(col)
            changed = True
    if changed:
        ws.update("A1", [normalized])
    return normalized

def _read_leads(limit: int = 600) -> List[List[str]]:
    try:
        ws = _open_sheet(LEADS_WS_TITLE)
        header = _ensure_leads_schema(ws)
        rows = ws.get_all_values() or []
        if not rows:
            return [header]
        body = rows[1:]
        if limit and len(body) > limit:
            body = body[-limit:]
        return [header] + body
    except Exception:
        data = _safe_read_json(LEADS_LOCAL_FILE, {"header": LEADS_HEADER, "rows": []})
        header = data.get("header") or LEADS_HEADER
        rows = data.get("rows") or []
        if limit and len(rows) > limit:
            rows = rows[-limit:]
        return [header] + rows

def _append_lead(row: Dict[str, str]) -> None:
    r = {k: (row.get(k, "") or "").strip() for k in LEADS_HEADER}
    r["timestamp"] = r["timestamp"] or _now_iso()
    r["status"] = r["status"] or "New"
    r["vip"] = "Yes" if (r.get("vip","").lower() in ["yes","true","1","y"]) else (r["vip"] or "No")

    try:
        ws = _open_sheet(LEADS_WS_TITLE)
        header = _ensure_leads_schema(ws)
        ws.append_row([r.get(h, "") for h in header], value_input_option="USER_ENTERED")
        return
    except Exception:
        data = _safe_read_json(LEADS_LOCAL_FILE, {"header": LEADS_HEADER, "rows": []})
        header = data.get("header") or LEADS_HEADER
        rows = data.get("rows") or []
        rows.append([r.get(h, "") for h in header])
        data["header"] = header
        data["rows"] = rows
        _safe_write_json(LEADS_LOCAL_FILE, data)

def _update_lead(row_number: int, status: Optional[str], vip: Optional[bool]) -> None:
    allowed_status = ["New","Confirmed","Seated","No-Show"]
    if status is not None and status not in allowed_status:
        raise ValueError("Invalid status")

    try:
        ws = _open_sheet(LEADS_WS_TITLE)
        header = _ensure_leads_schema(ws)

        def col_idx(name: str) -> int:
            return header.index(name) + 1 if name in header else -1

        if status is not None:
            c = col_idx("status")
            if c > 0:
                ws.update_cell(row_number, c, status)

        if vip is not None:
            c = col_idx("vip")
            if c > 0:
                ws.update_cell(row_number, c, "Yes" if vip else "No")
        return
    except Exception:
        data = _safe_read_json(LEADS_LOCAL_FILE, {"header": LEADS_HEADER, "rows": []})
        header = data.get("header") or LEADS_HEADER
        rows = data.get("rows") or []
        i = row_number - 2
        if i < 0 or i >= len(rows):
            raise ValueError("Row out of range")

        def col_idx0(name: str) -> int:
            return header.index(name) if name in header else -1

        if status is not None:
            c = col_idx0("status")
            if c >= 0:
                rows[i][c] = status
        if vip is not None:
            c = col_idx0("vip")
            if c >= 0:
                rows[i][c] = "Yes" if vip else "No"
        data["rows"] = rows
        _safe_write_json(LEADS_LOCAL_FILE, data)

# ------------------------------------------------------------
# Poll config + persistence
# ------------------------------------------------------------
def _poll_defaults() -> Dict[str, str]:
    return {"sponsor": "Fan Pick", "match_id": ""}

def _poll_get_config() -> Dict[str, str]:
    cfg = _poll_defaults()
    try:
        ws = _open_sheet(POLL_CONFIG_WS_TITLE)
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
        data = _safe_read_json(POLL_LOCAL_FILE, {"config": cfg, "votes": {}, "voters": {}})
        return data.get("config") or cfg

def _poll_set_config(new_cfg: Dict[str, str]) -> Dict[str, str]:
    cfg = _poll_get_config()
    cfg["sponsor"] = (new_cfg.get("sponsor") or cfg.get("sponsor") or "Fan Pick").strip() or "Fan Pick"
    cfg["match_id"] = (new_cfg.get("match_id") or "").strip()
    try:
        ws = _open_sheet(POLL_CONFIG_WS_TITLE)
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
        data = _safe_read_json(POLL_LOCAL_FILE, {"config": _poll_defaults(), "votes": {}, "voters": {}})
        data["config"] = cfg
        _safe_write_json(POLL_LOCAL_FILE, data)
        return cfg

def _poll_ensure_votes_ws():
    ws = _open_sheet(POLL_VOTES_WS_TITLE)
    rows = ws.get_all_values() or []
    if not rows:
        ws.update("A1", [["match_id","home","away","kickoff_utc","locked","winner","home_votes","away_votes","last_updated_utc"]])
    return ws

def _poll_get_record(match_id: str) -> Dict[str, Any]:
    match_id = match_id.strip()
    ws = _poll_ensure_votes_ws()
    rows = ws.get_all_values() or []
    header = rows[0] if rows else []
    idx = {}
    for i, r in enumerate(rows[1:], start=2):
        if len(r) >= 1:
            idx[(r[0] or "").strip()] = i
    rownum = idx.get(match_id)
    if not rownum:
        ws.append_row([match_id,"","","","false","","0","0",_now_iso()], value_input_option="USER_ENTERED")
        rows = ws.get_all_values() or []
        for i, r in enumerate(rows[1:], start=2):
            if (r[0] or "").strip() == match_id:
                rownum = i
                break
    r = ws.row_values(rownum) if rownum else []

    def g(col: str, default: str = "") -> str:
        try:
            j = header.index(col)
            return (r[j] if j < len(r) else default).strip()
        except Exception:
            return default

    home_votes = int(g("home_votes","0") or 0)
    away_votes = int(g("away_votes","0") or 0)
    locked = (g("locked","false").lower() in ["true","1","yes","y"])
    return {
        "_row": rownum,
        "match_id": match_id,
        "home": g("home",""),
        "away": g("away",""),
        "kickoff_utc": g("kickoff_utc",""),
        "locked": locked,
        "winner": g("winner",""),
        "home_votes": home_votes,
        "away_votes": away_votes,
    }

def _poll_set_fields(match_id: str, fields: Dict[str, Any]) -> None:
    ws = _poll_ensure_votes_ws()
    rows = ws.get_all_values() or []
    header = rows[0]
    rec = _poll_get_record(match_id)
    rownum = rec["_row"]
    for k, v in fields.items():
        if k not in header:
            continue
        col = header.index(k) + 1
        ws.update_cell(rownum, col, str(v))
    if "last_updated_utc" in header:
        ws.update_cell(rownum, header.index("last_updated_utc")+1, _now_iso())

def _poll_locked(rec: Dict[str, Any]) -> bool:
    if rec.get("locked"):
        return True
    s = rec.get("kickoff_utc") or ""
    if not s:
        return False
    try:
        dt = datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        return datetime.now(timezone.utc) >= dt
    except Exception:
        return False

def _poll_percent(home_votes: int, away_votes: int) -> Tuple[int, int]:
    total = home_votes + away_votes
    if total <= 0:
        return 0, 0
    home_pct = int(round((home_votes/total)*100))
    away_pct = max(0, 100-home_pct)
    return home_pct, away_pct

def _poll_local_voters() -> Dict[str, Any]:
    return _safe_read_json(POLL_LOCAL_FILE, {"config": _poll_defaults(), "votes": {}, "voters": {}})

# ------------------------------------------------------------
# Routes
# ------------------------------------------------------------
@app.route("/")
def home():
    return send_from_directory(APP_DIR, "index.html")

@app.route("/countries/qualified.json")
def qualified_countries():
    return jsonify({"as_of":"2025-11-18","source":"wikipedia","countries": QUALIFIED_COUNTRIES})

@app.route("/poll/match.json")
def poll_match():
    cfg = _poll_get_config()
    match_id = (cfg.get("match_id") or "").strip()
    if not match_id:
        return jsonify({"ok": False, "error": "No match configured"}), 200
    try:
        rec = _poll_get_record(match_id)
        locked = _poll_locked(rec)
        home_pct, away_pct = _poll_percent(rec["home_votes"], rec["away_votes"])
        return jsonify({
            "ok": True,
            "match": {
                "id": rec["match_id"],
                "home": rec.get("home",""),
                "away": rec.get("away",""),
                "kickoff_utc": rec.get("kickoff_utc",""),
                "locked": locked,
                "winner": rec.get("winner",""),
            },
            "totals": {
                "home": rec["home_votes"],
                "away": rec["away_votes"],
                "home_pct": home_pct,
                "away_pct": away_pct,
                "total": rec["home_votes"] + rec["away_votes"],
            },
            "sponsor": cfg.get("sponsor") or "Fan Pick",
        })
    except Exception as e:
        return jsonify({"ok": False, "error": f"{type(e).__name__}: {e}"}), 200

@app.route("/poll/vote", methods=["POST"])
def poll_vote():
    data = request.get_json(silent=True) or {}
    match_id = (data.get("match_id") or "").strip()
    team = (data.get("team") or "").strip().lower()  # "home" or "away"
    home = (data.get("home") or "").strip()
    away = (data.get("away") or "").strip()
    kickoff_utc = (data.get("kickoff_utc") or "").strip()

    if not match_id or team not in ["home","away"]:
        return jsonify({"ok": False, "error": "Bad request"}), 400

    voter = (data.get("voter") or "").strip() or secrets.token_urlsafe(12)

    # voter de-dupe in local file (works even with sheet totals)
    local = _poll_local_voters()
    voters = local.get("voters") or {}
    prev = voters.get(voter)
    if prev and prev.get("match_id") == match_id:
        try:
            rec = _poll_get_record(match_id)
            home_pct, away_pct = _poll_percent(rec["home_votes"], rec["away_votes"])
            return jsonify({"ok": True, "already_voted": True, "voter": voter, "totals": {
                "home": rec["home_votes"], "away": rec["away_votes"], "home_pct": home_pct, "away_pct": away_pct, "total": rec["home_votes"]+rec["away_votes"]
            }, "winner": rec.get("winner","")}), 200
        except Exception:
            return jsonify({"ok": True, "already_voted": True, "voter": voter}), 200

    try:
        rec = _poll_get_record(match_id)
        fields = {}
        if home and not rec.get("home"):
            fields["home"] = home
        if away and not rec.get("away"):
            fields["away"] = away
        if kickoff_utc and not rec.get("kickoff_utc"):
            fields["kickoff_utc"] = kickoff_utc
        if fields:
            _poll_set_fields(match_id, fields)
            rec = _poll_get_record(match_id)

        if _poll_locked(rec):
            home_pct, away_pct = _poll_percent(rec["home_votes"], rec["away_votes"])
            return jsonify({"ok": True, "locked": True, "voter": voter, "totals": {
                "home": rec["home_votes"], "away": rec["away_votes"], "home_pct": home_pct, "away_pct": away_pct, "total": rec["home_votes"]+rec["away_votes"]
            }, "winner": rec.get("winner","")}), 200

        if team == "home":
            rec["home_votes"] += 1
        else:
            rec["away_votes"] += 1
        _poll_set_fields(match_id, {"home_votes": rec["home_votes"], "away_votes": rec["away_votes"]})

        voters[voter] = {"match_id": match_id, "team": team, "ts": _now_iso()}
        local["voters"] = voters
        _safe_write_json(POLL_LOCAL_FILE, local)

        home_pct, away_pct = _poll_percent(rec["home_votes"], rec["away_votes"])
        return jsonify({"ok": True, "voter": voter, "totals": {
            "home": rec["home_votes"], "away": rec["away_votes"], "home_pct": home_pct, "away_pct": away_pct, "total": rec["home_votes"]+rec["away_votes"]
        }}), 200
    except Exception:
        # Fully local fallback
        store = local
        votes = store.get("votes") or {}
        rec = votes.get(match_id) or {"home": home, "away": away, "kickoff_utc": kickoff_utc, "locked": False, "winner": "", "home_votes": 0, "away_votes": 0}
        rec["home"] = rec.get("home") or home
        rec["away"] = rec.get("away") or away
        rec["kickoff_utc"] = rec.get("kickoff_utc") or kickoff_utc

        if team == "home":
            rec["home_votes"] = int(rec["home_votes"]) + 1
        else:
            rec["away_votes"] = int(rec["away_votes"]) + 1
        votes[match_id] = rec
        voters[voter] = {"match_id": match_id, "team": team, "ts": _now_iso()}
        store["votes"] = votes
        store["voters"] = voters
        _safe_write_json(POLL_LOCAL_FILE, store)

        home_pct, away_pct = _poll_percent(int(rec["home_votes"]), int(rec["away_votes"]))
        return jsonify({"ok": True, "voter": voter, "totals": {
            "home": rec["home_votes"], "away": rec["away_votes"], "home_pct": home_pct, "away_pct": away_pct, "total": rec["home_votes"]+rec["away_votes"]
        }}), 200

@app.route("/api/leads", methods=["POST"])
def api_leads():
    data = request.get_json(silent=True) or {}
    lead = {
        "timestamp": data.get("timestamp") or _now_iso(),
        "name": data.get("name",""),
        "phone": data.get("phone",""),
        "date": data.get("date",""),
        "time": data.get("time",""),
        "party_size": str(data.get("party_size","")),
        "language": data.get("language","en"),
        "status": data.get("status","New"),
        "vip": "Yes" if str(data.get("vip","")).lower() in ["yes","true","1","y"] else "No",
    }
    _append_lead(lead)
    return jsonify({"ok": True}), 200

def _hesc(s: Any) -> str:
    s = "" if s is None else str(s)
    return (s.replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")
              .replace('"',"&quot;").replace("'","&#39;"))

@app.route("/admin/update-lead", methods=["POST"])
def admin_update_lead():
    key = request.args.get("key","")
    if not ADMIN_KEY or key != ADMIN_KEY:
        return jsonify({"ok": False, "error":"Unauthorized"}), 401
    data = request.get_json(silent=True) or {}
    row = int(data.get("row") or 0)
    status = (data.get("status") or "").strip() or None
    vip = data.get("vip")
    vip_bool = None
    if vip is not None:
        vip_bool = bool(vip)
    try:
        _update_lead(row, status=status, vip=vip_bool)
        return jsonify({"ok": True}), 200
    except Exception as e:
        return jsonify({"ok": False, "error": f"{type(e).__name__}: {e}"}), 200

@app.route("/admin/poll/config", methods=["GET","POST"])
def admin_poll_config():
    key = request.args.get("key","")
    if not ADMIN_KEY or key != ADMIN_KEY:
        return "Unauthorized", 401
    if request.method == "POST":
        sponsor = (request.form.get("sponsor") or "").strip()
        match_id = (request.form.get("match_id") or "").strip()
        _poll_set_config({"sponsor": sponsor, "match_id": match_id})
        return "Saved. <a href='/admin?key=%s'>Back to Admin</a>" % _hesc(key)
    cfg = _poll_get_config()
    sponsor_val = _hesc(cfg.get("sponsor","Fan Pick"))
    match_val = _hesc(cfg.get("match_id",""))
    back = _hesc(key)
    return f"""<!doctype html><html><head><meta charset='utf-8'/><meta name='viewport' content='width=device-width,initial-scale=1'/>
    <title>Fan Poll Config</title>
    <style>body{{font-family:Arial,system-ui;background:#0b1020;color:#eaf0ff;padding:18px}}input{{padding:10px;border-radius:10px;border:1px solid rgba(255,255,255,.2);background:#0f1b33;color:#eaf0ff;width:100%;max-width:520px}}button{{padding:10px 14px;border-radius:10px;border:0;background:#d4af37;font-weight:800}}.box{{max-width:720px;background:#0f1b33;border:1px solid rgba(255,255,255,.14);border-radius:16px;padding:14px}}</style>
    </head><body>
    <div class='box'>
      <h2 style='margin:0 0 8px 0'>Fan Pick Settings</h2>
      <form method='post'>
        <label>Presented by (Sponsor label)</label><br/>
        <input name='sponsor' value='{sponsor_val}'/><br/><br/>
        <label>Match of the Day (match_id)</label><br/>
        <input name='match_id' value='{match_val}' placeholder='Example: 2026-06-11-ATTS-ABC'/><br/>
        <div style='opacity:.75;font-size:13px;margin-top:8px'>Tip: paste the match_id your frontend uses for voting.</div><br/>
        <button type='submit'>Save</button>
      </form>
      <div style='margin-top:12px'><a style='color:#b9c7ee' href='/admin?key={back}'>← Back to Admin</a></div>
    </div>
    </body></html>"""

@app.route("/admin")
def admin():
    key = request.args.get("key","")
    if not ADMIN_KEY or key != ADMIN_KEY:
        return "Unauthorized", 401

    rows = _read_leads(limit=800)
    header = rows[0] if rows else LEADS_HEADER
    body = rows[1:] if len(rows) > 1 else []

    h = [c.strip() for c in header]
    def idx(col: str) -> int:
        return h.index(col) if col in h else -1

    i_ts, i_name, i_phone, i_date, i_time, i_party, i_lang, i_status, i_vip = [idx(c) for c in LEADS_HEADER]

    numbered = list(reversed([(i+2, r) for i, r in enumerate(body)]))
    cfg = _poll_get_config()

    html = []
    html.append("""<!doctype html><html><head><meta charset='utf-8'/><meta name='viewport' content='width=device-width,initial-scale=1'/>
    <title>Leads Admin</title>
    <style>
      :root{--bg:#0b1020;--panel:#0f1b33;--line:rgba(255,255,255,.12);--text:#eaf0ff;--muted:#b9c7ee;--gold:#d4af37;}
      body{margin:0;font-family:Arial,system-ui;background:var(--bg);color:var(--text);}
      .wrap{max-width:1200px;margin:0 auto;padding:18px;}
      .top{display:flex;gap:12px;align-items:flex-start;justify-content:space-between;flex-wrap:wrap}
      .card{background:var(--panel);border:1px solid var(--line);border-radius:16px;padding:12px;}
      table{width:100%;border-collapse:collapse;margin-top:12px;background:var(--panel);border:1px solid var(--line);border-radius:16px;overflow:hidden}
      th,td{padding:10px;border-bottom:1px solid var(--line);text-align:left;font-size:14px;vertical-align:middle}
      th{position:sticky;top:0;background:#111d37;z-index:2}
      select{background:#0b152c;color:var(--text);border:1px solid var(--line);border-radius:10px;padding:7px}
      input[type=checkbox]{transform:scale(1.2)}
      .btn{display:inline-flex;align-items:center;justify-content:center;padding:9px 12px;border-radius:12px;border:0;background:var(--gold);font-weight:900;color:#0b1020;text-decoration:none}
      .muted{color:var(--muted)}
    </style>
    </head><body>""")
    html.append("<div class='wrap'>")
    html.append("<div class='top'>")
    html.append("<div class='card' style='flex:1;min-width:280px'>")
    html.append("<h2 style='margin:0 0 6px 0'>Leads Admin — World Cup AI Reservations</h2>")
    html.append(f"<div class='muted'>Rows shown: {len(body)}</div>")
    html.append("</div>")
    html.append("<div class='card' style='min-width:280px'>")
    html.append(f"<div class='muted'><b>Fan Pick</b> — Presented by: <b>{_hesc(cfg.get('sponsor','Fan Pick'))}</b></div>")
    html.append(f"<div class='muted' style='margin-top:6px'>Match ID: <b>{_hesc(cfg.get('match_id','(not set)'))}</b></div>")
    html.append(f"<div style='margin-top:10px'><a class='btn' href='/admin/poll/config?key={_hesc(key)}'>Edit Fan Poll</a></div>")
    html.append("</div></div>")

    html.append("""<table>
      <thead><tr>
        <th>Timestamp</th><th>Name</th><th>Phone</th><th>Date</th><th>Time</th><th>Party</th><th>Lang</th><th>Status</th><th>VIP</th>
      </tr></thead><tbody>""")

    statuses = ["New","Confirmed","Seated","No-Show"]

    for rownum, r in numbered:
        def c(i, default=""):
            return (r[i] if 0 <= i < len(r) else default)
        status_val = c(i_status, "New") or "New"
        vip_val = (c(i_vip, "No") or "No").lower() in ["yes","true","1","y"]

        opts = "".join([f"<option value='{s}' {'selected' if s==status_val else ''}>{s}</option>" for s in statuses])
        html.append(f"<tr data-row='{rownum}'>")
        html.append(f"<td>{_hesc(c(i_ts))}</td><td>{_hesc(c(i_name))}</td><td>{_hesc(c(i_phone))}</td>")
        html.append(f"<td>{_hesc(c(i_date))}</td><td>{_hesc(c(i_time))}</td><td>{_hesc(c(i_party))}</td><td>{_hesc(c(i_lang))}</td>")
        html.append(f"<td><select class='status'>{opts}</select></td>")
        html.append(f"<td style='text-align:center'><input class='vip' type='checkbox' {'checked' if vip_val else ''}/></td>")
        html.append("</tr>")

    html.append("""</tbody></table>
    <div class='muted' style='margin-top:10px'>Tip: status/VIP updates save instantly.</div>
    <script>
      const ADMIN_KEY = new URLSearchParams(location.search).get('key') || '';
      async function saveRow(tr){
        const row = Number(tr.dataset.row||0);
        const status = tr.querySelector('select.status')?.value || 'New';
        const vip = !!tr.querySelector('input.vip')?.checked;
        try{
          const res = await fetch('/admin/update-lead?key='+encodeURIComponent(ADMIN_KEY),{
            method:'POST',
            headers:{'Content-Type':'application/json'},
            body: JSON.stringify({row, status, vip})
          });
          const j = await res.json().catch(()=>({ok:false}));
          if(!j.ok){ console.warn('Save failed', j); }
        }catch(e){ console.warn(e); }
      }
      document.querySelectorAll('tr[data-row]').forEach(tr=>{
        tr.querySelector('select.status')?.addEventListener('change', ()=>saveRow(tr));
        tr.querySelector('input.vip')?.addEventListener('change', ()=>saveRow(tr));
      });
    </script>""")

    html.append("</div></body></html>")
    return "".join(html)

@app.route("/chat", methods=["POST"])
def chat():
    data = request.get_json(silent=True) or {}
    msg = (data.get("message") or "").strip()
    if not msg:
        return jsonify({"reply": "Say something and I’ll help."}), 200
    if msg.lower().strip() == "reservation":
        return jsonify({"reply": "✅ Reservation started. Please tell me the date (e.g., June 8, 2026)."}), 200

    api_key = os.environ.get("OPENAI_API_KEY","").strip()
    if not api_key:
        return jsonify({"reply": "⚠️ Chat is temporarily unavailable. Type “reservation” to book a table."}), 200
    try:
        from openai import OpenAI
        client = OpenAI(api_key=api_key)
        resp = client.chat.completions.create(
            model=os.environ.get("OPENAI_MODEL","gpt-4o-mini"),
            messages=[
                {"role":"system","content":"You are the World Cup 2026 Dallas Match-Day Concierge for a fan zone venue. Be brief, helpful, and friendly."},
                {"role":"user","content": msg}
            ],
            temperature=0.5,
            max_tokens=220,
        )
        reply = resp.choices[0].message.content.strip()
        return jsonify({"reply": reply}), 200
    except Exception:
        return jsonify({"reply": "⚠️ Chat is temporarily unavailable. Type “reservation” to book a table."}), 200

if __name__ == "__main__":
    port = int(os.environ.get("PORT", "5000"))
    app.run(host="0.0.0.0", port=port)
