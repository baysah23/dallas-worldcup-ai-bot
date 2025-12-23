from __future__ import annotations

import os, json, re, hashlib
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from flask import Flask, request, jsonify, send_from_directory

APP_DIR = os.path.dirname(os.path.abspath(__file__))

ADMIN_KEY = os.environ.get("ADMIN_KEY", "").strip()

SHEET_NAME = os.environ.get("SHEET_NAME", "WorldCupLeads").strip()
LEADS_WS_TITLE = os.environ.get("LEADS_WS_TITLE", "Leads").strip()

LEADS_LOCAL_FILE = os.environ.get("LEADS_LOCAL_FILE", "/tmp/wc26_leads.json")
POLL_LOCAL_FILE  = os.environ.get("POLL_LOCAL_FILE",  "/tmp/wc26_poll.json")

QUALIFIED_COUNTRIES = ["Canada", "Mexico", "United States", "Japan", "New Zealand", "Iran", "Argentina", "Uzbekistan", "South Korea", "Jordan", "Australia", "Brazil", "Ecuador", "Uruguay", "Colombia", "Paraguay", "Morocco", "Tunisia", "Egypt", "Algeria", "Ghana", "Cape Verde", "South Africa", "Qatar", "England", "Saudi Arabia", "Ivory Coast", "Senegal", "France", "Croatia", "Portugal", "Norway", "Germany", "Netherlands", "Belgium", "Austria", "Switzerland", "Spain", "Scotland", "Panama", "Haiti", "Curaçao"]

app = Flask(__name__, static_folder=APP_DIR, static_url_path="")

def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def _hesc(s: Any) -> str:
    s = "" if s is None else str(s)
    return (s.replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")
              .replace('"',"&quot;").replace("'","&#39;"))

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

def _client_ip() -> str:
    return (request.headers.get("X-Forwarded-For","").split(",")[0].strip() or request.remote_addr or "0.0.0.0")

def _session_id() -> str:
    ua = request.headers.get("User-Agent","")
    base = f"{_client_ip()}|{ua}"
    return hashlib.sha256(base.encode("utf-8")).hexdigest()[:32]

# ===== Optional Google Sheets (lazy) =====
def _get_gspread() -> Tuple[Optional[Any], Optional[str]]:
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

def _open_ws(title: str):
    gc, err = _get_gspread()
    if not gc:
        raise RuntimeError(err or "Sheets unavailable")
    sh = gc.open(SHEET_NAME)
    try:
        return sh.worksheet(title)
    except Exception:
        return sh.add_worksheet(title=title, rows=1000, cols=30)

# ===== Leads =====
LEADS_HEADER = ["timestamp","name","phone","date","time","party_size","language","status","vip"]

def _ensure_leads_schema(ws) -> List[str]:
    rows = ws.get_all_values() or []
    if not rows:
        ws.update("A1", [LEADS_HEADER])
        return LEADS_HEADER
    header = [(h or "").strip() for h in rows[0]]
    changed = False
    for col in LEADS_HEADER:
        if col not in header:
            header.append(col)
            changed = True
    if changed:
        ws.update("A1", [header])
    return header

def _append_lead(row: Dict[str, str]) -> None:
    r = {k: (row.get(k, "") or "").strip() for k in LEADS_HEADER}
    r["timestamp"] = r["timestamp"] or _now_iso()
    r["status"] = r["status"] or "New"
    r["vip"] = "Yes" if (str(r.get("vip","")).lower() in ["yes","true","1","y"]) else (r["vip"] or "No")

    try:
        ws = _open_ws(LEADS_WS_TITLE)
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

def _read_leads(limit: int = 800) -> Tuple[List[str], List[List[str]]]:
    try:
        ws = _open_ws(LEADS_WS_TITLE)
        header = _ensure_leads_schema(ws)
        rows = ws.get_all_values() or []
        body = rows[1:] if len(rows) > 1 else []
        if limit and len(body) > limit:
            body = body[-limit:]
        return header, body
    except Exception:
        data = _safe_read_json(LEADS_LOCAL_FILE, {"header": LEADS_HEADER, "rows": []})
        header = data.get("header") or LEADS_HEADER
        body = data.get("rows") or []
        if limit and len(body) > limit:
            body = body[-limit:]
        return header, body

def _update_lead_row(row_number: int, status: Optional[str], vip: Optional[bool]) -> Tuple[bool, Optional[str]]:
    allowed = ["New","Confirmed","Seated","No-Show"]
    if status is not None and status not in allowed:
        return False, "Invalid status"

    try:
        ws = _open_ws(LEADS_WS_TITLE)
        header = _ensure_leads_schema(ws)
        def col(name: str) -> int:
            return header.index(name)+1 if name in header else -1

        if status is not None:
            c = col("status")
            if c > 0:
                ws.update_cell(row_number, c, status)

        if vip is not None:
            c = col("vip")
            if c > 0:
                ws.update_cell(row_number, c, "Yes" if vip else "No")

        return True, None
    except Exception:
        data = _safe_read_json(LEADS_LOCAL_FILE, {"header": LEADS_HEADER, "rows": []})
        header = data.get("header") or LEADS_HEADER
        rows = data.get("rows") or []
        i = row_number - 2
        if i < 0 or i >= len(rows):
            return False, "Row out of range"
        def col0(name: str) -> int:
            return header.index(name) if name in header else -1
        if status is not None:
            c = col0("status")
            if c >= 0:
                rows[i][c] = status
        if vip is not None:
            c = col0("vip")
            if c >= 0:
                rows[i][c] = "Yes" if vip else "No"
        data["rows"] = rows
        _safe_write_json(LEADS_LOCAL_FILE, data)
        return True, None

# ===== Poll =====
def _poll_defaults():
    return {
        "config": {
            "sponsor": "Fan Pick",
            "match_id": "",
            "home": "",
            "away": "",
            "kickoff_utc": "",
            "winner": "",
            "locked": False
        },
        "votes": {"home": 0, "away": 0},
        "voters": {}
    }

def _poll_read():
    return _safe_read_json(POLL_LOCAL_FILE, _poll_defaults())

def _poll_write(state):
    _safe_write_json(POLL_LOCAL_FILE, state)

def _poll_is_locked(cfg: Dict[str, Any]) -> bool:
    if cfg.get("locked"):
        return True
    k = (cfg.get("kickoff_utc") or "").strip()
    if not k:
        return False
    try:
        dt = datetime.strptime(k, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        return datetime.now(timezone.utc) >= dt
    except Exception:
        return False

def _poll_percent(home_votes: int, away_votes: int):
    total = home_votes + away_votes
    if total <= 0:
        return 0, 0, 0
    home_pct = int(round((home_votes/total)*100))
    away_pct = max(0, 100-home_pct)
    return home_pct, away_pct, total

# ===== Reservation flow (chat) =====
_sessions: Dict[str, Dict[str, Any]] = {}

def _reset_reservation(sid: str):
    _sessions[sid] = {"mode":"idle", "data":{}}

def _get_session(sid: str) -> Dict[str, Any]:
    if sid not in _sessions:
        _reset_reservation(sid)
    return _sessions[sid]

def _parse_party_size(text: str) -> Optional[int]:
    m = re.search(r"(\\d+)", text)
    if not m:
        return None
    try:
        n = int(m.group(1))
        if 1 <= n <= 40:
            return n
    except Exception:
        return None
    return None

def _parse_phone(text: str) -> Optional[str]:
    digits = re.sub(r"\\D", "", text)
    if len(digits) >= 10:
        return digits[-10:]
    return None

def _parse_date(text: str) -> Optional[str]:
    t = text.strip()
    m = re.search(r"(\\d{4}-\\d{2}-\\d{2})", t)
    if m:
        return m.group(1)
    m = re.search(r"(\\d{1,2})/(\\d{1,2})/(\\d{4})", t)
    if m:
        mm, dd, yy = m.group(1).zfill(2), m.group(2).zfill(2), m.group(3)
        return f"{yy}-{mm}-{dd}"
    return None

def _parse_time(text: str) -> Optional[str]:
    t = text.strip().lower()
    m = re.search(r"(\\d{1,2}):(\\d{2})\\s*(am|pm)?", t)
    if m:
        hh, mm, ap = int(m.group(1)), m.group(2), m.group(3)
        if ap:
            return f"{hh}:{mm} {ap}"
        return f"{hh}:{mm}"
    m = re.search(r"(\\d{1,2})\\s*(am|pm)", t)
    if m:
        return f"{int(m.group(1))}:00 {m.group(2)}"
    return None

# ===== Routes =====
@app.route("/")
def home():
    return send_from_directory(APP_DIR, "index.html")

@app.route("/countries/qualified.json")
@app.route("/worldcup/qualified.json")
def qualified_json():
    return jsonify({"as_of": "2025-12-23", "countries": QUALIFIED_COUNTRIES})

@app.route("/poll/match.json")
def poll_match():
    state = _poll_read()
    cfg = state.get("config") or {}
    votes = state.get("votes") or {"home":0,"away":0}
    match_id = (cfg.get("match_id") or "").strip()
    if not match_id:
        return jsonify({"ok": False, "error":"No match configured"}), 200
    locked = _poll_is_locked(cfg)
    hv, av = int(votes.get("home") or 0), int(votes.get("away") or 0)
    hp, ap, total = _poll_percent(hv,av)
    return jsonify({
        "ok": True,
        "sponsor": cfg.get("sponsor") or "Fan Pick",
        "match": {
            "id": match_id,
            "home": cfg.get("home",""),
            "away": cfg.get("away",""),
            "kickoff_utc": cfg.get("kickoff_utc",""),
            "locked": locked,
            "winner": cfg.get("winner",""),
        },
        "totals": {"home": hv, "away": av, "home_pct": hp, "away_pct": ap, "total": total}
    }), 200

@app.route("/poll/vote", methods=["POST"])
def poll_vote():
    data = request.get_json(silent=True) or {}
    match_id = (data.get("match_id") or "").strip()
    team = (data.get("team") or "").strip().lower()
    voter = (data.get("voter") or "").strip() or _session_id()
    if not match_id or team not in ["home","away"]:
        return jsonify({"ok": False, "error":"Bad request"}), 400

    state = _poll_read()
    cfg = state.get("config") or {}
    votes = state.get("votes") or {"home":0,"away":0}
    voters = state.get("voters") or {}

    if (cfg.get("match_id") or "").strip() != match_id:
        return jsonify({"ok": False, "error":"Match mismatch"}), 200

    for k in ["home","away","kickoff_utc"]:
        incoming = (data.get(k) or "").strip()
        if incoming and not (cfg.get(k) or "").strip():
            cfg[k] = incoming

    if _poll_is_locked(cfg):
        cfg["locked"] = True
        state["config"] = cfg
        _poll_write(state)
        hv, av = int(votes.get("home") or 0), int(votes.get("away") or 0)
        hp, ap, total = _poll_percent(hv,av)
        return jsonify({"ok": True, "locked": True, "totals": {"home": hv, "away": av, "home_pct": hp, "away_pct": ap, "total": total}, "winner": cfg.get("winner","")}), 200

    prev = voters.get(voter)
    if prev and prev.get("match_id") == match_id:
        hv, av = int(votes.get("home") or 0), int(votes.get("away") or 0)
        hp, ap, total = _poll_percent(hv,av)
        return jsonify({"ok": True, "already_voted": True, "totals": {"home": hv, "away": av, "home_pct": hp, "away_pct": ap, "total": total}, "winner": cfg.get("winner","")}), 200

    votes[team] = int(votes.get(team) or 0) + 1
    voters[voter] = {"match_id": match_id, "team": team, "ts": _now_iso()}
    state["config"] = cfg
    state["votes"] = votes
    state["voters"] = voters
    _poll_write(state)

    hv, av = int(votes.get("home") or 0), int(votes.get("away") or 0)
    hp, ap, total = _poll_percent(hv,av)
    return jsonify({"ok": True, "voter": voter, "totals": {"home": hv, "away": av, "home_pct": hp, "away_pct": ap, "total": total}, "winner": cfg.get("winner","")}), 200

@app.route("/admin/poll/config", methods=["GET","POST"])
def admin_poll_config():
    key = request.args.get("key","")
    if not ADMIN_KEY or key != ADMIN_KEY:
        return "Unauthorized", 401
    state = _poll_read()
    cfg = state.get("config") or {}
    if request.method == "POST":
        cfg["sponsor"] = (request.form.get("sponsor") or cfg.get("sponsor") or "Fan Pick").strip() or "Fan Pick"
        cfg["match_id"] = (request.form.get("match_id") or "").strip()
        cfg["home"] = (request.form.get("home") or cfg.get("home") or "").strip()
        cfg["away"] = (request.form.get("away") or cfg.get("away") or "").strip()
        cfg["kickoff_utc"] = (request.form.get("kickoff_utc") or cfg.get("kickoff_utc") or "").strip()
        cfg["winner"] = (request.form.get("winner") or cfg.get("winner") or "").strip().lower()
        if cfg["winner"] not in ["", "home", "away"]:
            cfg["winner"] = ""
        cfg["locked"] = True if (request.form.get("locked") == "on") else False
        state["config"] = cfg
        _poll_write(state)
        return f"Saved. <a href='/admin?key={_hesc(key)}'>Back to Admin</a>"
    checked = "checked" if cfg.get("locked") else ""
    return f"""<!doctype html><html><head><meta charset='utf-8'/><meta name='viewport' content='width=device-width,initial-scale=1'/>
    <title>Fan Poll Config</title>
    <style>
      body{{font-family:Arial,system-ui;background:#0b1020;color:#eaf0ff;padding:18px}}
      .box{{max-width:840px;background:#0f1b33;border:1px solid rgba(255,255,255,.14);border-radius:16px;padding:14px}}
      label{{display:block;margin-top:10px;color:#b9c7ee}}
      input{{padding:10px;border-radius:10px;border:1px solid rgba(255,255,255,.2);background:#0b152c;color:#eaf0ff;width:100%;max-width:640px}}
      button{{margin-top:14px;padding:10px 14px;border-radius:12px;border:0;background:#d4af37;font-weight:900;color:#0b1020;cursor:pointer}}
      a{{color:#b9c7ee}}
    </style></head><body>
      <div class='box'>
        <h2 style='margin:0 0 6px 0'>Fan Pick Settings</h2>
        <form method='post'>
          <label>Presented by (Sponsor label)</label>
          <input name='sponsor' value='{_hesc(cfg.get("sponsor","Fan Pick"))}'/>
          <label>Match of the Day (match_id)</label>
          <input name='match_id' value='{_hesc(cfg.get("match_id",""))}'/>
          <label>Home team (optional)</label>
          <input name='home' value='{_hesc(cfg.get("home",""))}'/>
          <label>Away team (optional)</label>
          <input name='away' value='{_hesc(cfg.get("away",""))}'/>
          <label>Kickoff UTC (optional)</label>
          <input name='kickoff_utc' value='{_hesc(cfg.get("kickoff_utc",""))}' placeholder='YYYY-MM-DDTHH:MM:SSZ'/>
          <label>Winner post-match (optional: home / away)</label>
          <input name='winner' value='{_hesc(cfg.get("winner",""))}' placeholder='home or away'/>
          <label style='display:flex;gap:10px;align-items:center;margin-top:12px;color:#b9c7ee'>
            <input type='checkbox' name='locked' {checked}/> Lock poll now
          </label>
          <button type='submit'>Save</button>
        </form>
        <div style='margin-top:12px'><a href='/admin?key={_hesc(key)}'>← Back to Admin</a></div>
      </div>
    </body></html>"""

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

@app.route("/admin/update-lead", methods=["POST"])
def admin_update_lead():
    key = request.args.get("key","")
    if not ADMIN_KEY or key != ADMIN_KEY:
        return jsonify({"ok": False, "error":"Unauthorized"}), 401
    data = request.get_json(silent=True) or {}
    try:
        row = int(data.get("row") or 0)
        status = (data.get("status") or "").strip() or None
        vip = data.get("vip")
        vip_bool = None if vip is None else bool(vip)
        ok, err = _update_lead_row(row, status=status, vip=vip_bool)
        if not ok:
            return jsonify({"ok": False, "error": err or "Update failed"}), 200
        return jsonify({"ok": True}), 200
    except Exception as e:
        return jsonify({"ok": False, "error": f"{type(e).__name__}: {e}"}), 200

@app.route("/admin")
def admin():
    key = request.args.get("key","")
    if not ADMIN_KEY or key != ADMIN_KEY:
        return "Unauthorized", 401

    header, body = _read_leads(limit=800)
    h = [(c or "").strip() for c in header]
    def idx(col: str) -> int:
        return h.index(col) if col in h else -1

    i_ts, i_name, i_phone, i_date, i_time, i_party, i_lang, i_status, i_vip = [idx(c) for c in LEADS_HEADER]
    numbered = list(reversed([(i+2, r) for i, r in enumerate(body)]))

    poll_state = _poll_read()
    cfg = (poll_state.get("config") or {})
    sponsor = _hesc(cfg.get("sponsor","Fan Pick"))
    match_id = _hesc(cfg.get("match_id","(not set)"))
    statuses = ["New","Confirmed","Seated","No-Show"]

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
      a{color:var(--muted)}
    </style></head><body>""")
    html.append("<div class='wrap'>")
    html.append("<div class='top'>")
    html.append("<div class='card' style='flex:1;min-width:280px'>")
    html.append("<h2 style='margin:0 0 6px 0'>Leads Admin — World Cup AI Reservations</h2>")
    html.append(f"<div class='muted'>Rows shown: {len(body)}</div>")
    html.append("</div>")
    html.append("<div class='card' style='min-width:280px'>")
    html.append(f"<div class='muted'><b>Fan Pick</b> — Sponsor: <b>{sponsor}</b></div>")
    html.append(f"<div class='muted' style='margin-top:6px'>Match ID: <b>{match_id}</b></div>")
    html.append(f"<div style='margin-top:10px'><a class='btn' href='/admin/poll/config?key={_hesc(key)}'>Edit Fan Poll</a></div>")
    html.append("</div></div>")

    html.append("""<table><thead><tr>
      <th>Timestamp</th><th>Name</th><th>Phone</th><th>Date</th><th>Time</th><th>Party</th><th>Lang</th><th>Status</th><th>VIP</th>
    </tr></thead><tbody>""")

    def c(r, i, default=""):
        return (r[i] if 0 <= i < len(r) else default)

    for rownum, r in numbered:
        status_val = c(r, i_status, "New") or "New"
        vip_val = (c(r, i_vip, "No") or "No").lower() in ["yes","true","1","y"]
        opts = "".join([f"<option value='{s}' {'selected' if s==status_val else ''}>{s}</option>" for s in statuses])
        html.append(f"<tr data-row='{rownum}'>")
        html.append(f"<td>{_hesc(c(r,i_ts))}</td><td>{_hesc(c(r,i_name))}</td><td>{_hesc(c(r,i_phone))}</td>")
        html.append(f"<td>{_hesc(c(r,i_date))}</td><td>{_hesc(c(r,i_time))}</td><td>{_hesc(c(r,i_party))}</td><td>{_hesc(c(r,i_lang))}</td>")
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
        return jsonify({"reply": "Type “reservation” to book a table."}), 200

    sid = _session_id()
    sess = _get_session(sid)
    text = msg.strip()

    if text.lower() in ["reservation", "book", "book table", "vip", "vip hold"]:
        sess["mode"] = "collect_name"
        sess["data"] = {}
        return jsonify({"reply": "✅ Let’s book it. What name should I put the reservation under?"}), 200

    if text.lower() in ["reset", "start over", "cancel reservation"]:
        _reset_reservation(sid)
        return jsonify({"reply":"No problem — reservation reset. Type “reservation” when ready."}), 200

    if sess.get("mode") == "collect_name":
        sess["data"]["name"] = text[:80]
        sess["mode"] = "collect_phone"
        return jsonify({"reply":"Got it. What’s the best phone number for the booking?"}), 200

    if sess.get("mode") == "collect_phone":
        phone = _parse_phone(text)
        if not phone:
            return jsonify({"reply":"Please enter a valid 10-digit phone number (digits only is fine)."}), 200
        sess["data"]["phone"] = phone
        sess["mode"] = "collect_date"
        return jsonify({"reply":"Perfect. What date? (YYYY-MM-DD or MM/DD/YYYY)"}), 200

    if sess.get("mode") == "collect_date":
        d = _parse_date(text)
        if not d:
            return jsonify({"reply":"Please enter the date as YYYY-MM-DD (example: 2026-06-08)."}), 200
        sess["data"]["date"] = d
        sess["mode"] = "collect_time"
        return jsonify({"reply":"What time? (example: 9:00 pm)"}), 200

    if sess.get("mode") == "collect_time":
        t = _parse_time(text)
        if not t:
            return jsonify({"reply":"Please enter a time like 7:00 pm."}), 200
        sess["data"]["time"] = t
        sess["mode"] = "collect_party"
        return jsonify({"reply":"How many people in your party?"}), 200

    if sess.get("mode") == "collect_party":
        n = _parse_party_size(text)
        if not n:
            return jsonify({"reply":"Please reply with a number (example: 6)."}), 200
        sess["data"]["party_size"] = str(n)
        lead = {
            "timestamp": _now_iso(),
            "name": sess["data"].get("name",""),
            "phone": sess["data"].get("phone",""),
            "date": sess["data"].get("date",""),
            "time": sess["data"].get("time",""),
            "party_size": sess["data"].get("party_size",""),
            "language": (data.get("language") or "en"),
            "status": "New",
            "vip": "No",
        }
        _append_lead(lead)
        _reset_reservation(sid)
        return jsonify({"reply": f"✅ Confirmed. {lead['name']} — party of {lead['party_size']} on {lead['date']} at {lead['time']}. We’ll text {lead['phone']} shortly."}), 200

    # AI fallback if configured
    api_key = os.environ.get("OPENAI_API_KEY","").strip()
    if not api_key:
        return jsonify({"reply":"Type “reservation” to book a table."}), 200
    try:
        from openai import OpenAI
        client = OpenAI(api_key=api_key)
        resp = client.chat.completions.create(
            model=os.environ.get("OPENAI_MODEL","gpt-4o-mini"),
            messages=[
                {"role":"system","content":"You are the World Cup 2026 Dallas Match-Day Concierge for a fan zone venue. Be brief, helpful, and friendly."},
                {"role":"user","content": text}
            ],
            temperature=0.5,
            max_tokens=220,
        )
        reply = resp.choices[0].message.content.strip()
        return jsonify({"reply": reply}), 200
    except Exception:
        return jsonify({"reply":"⚠️ Chat is temporarily unavailable. Type “reservation” to book a table."}), 200

if __name__ == "__main__":
    port = int(os.environ.get("PORT","5000"))
    app.run(host="0.0.0.0", port=port)
