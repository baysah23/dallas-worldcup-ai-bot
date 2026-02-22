# Drafts Functionality — Complete Reference (100%)

This document captures **every** aspect of the Drafts feature in this repo: backend, frontend, storage, APIs, permissions, and integration points. Use it as the baseline to restore or compare against production/staging/reference branch.

---

## 1. Backend (app.py)

### 1.1 Configuration & Constants
- **Line ~85**: `DRAFTS_FILE` is in `_REDIS_PATH_KEY_MAP` — Redis key suffix `"drafts"` when Redis is enabled.
- **Line 1020**:  
  `DRAFTS_FILE = os.environ.get("DRAFTS_FILE", "/tmp/wc26_{venue}_drafts.json")`  
  Path uses literal `{venue}` placeholder; it is **resolved at read/write time** by `_safe_read_json_file` / `_safe_write_json_file` using `_venue_id()` (so **per-venue** storage).

### 1.2 Default Structure & In-Memory State
- **Lines 1022–1035**: `_default_drafts()` returns:
  ```python
  {
    "updated_at": None,
    "updated_by": None,
    "updated_role": None,
    "drafts": {
      "sms_confirm": {
        "channel": "sms",
        "title": "SMS — Confirmation",
        "body": "Hi {name}, you're confirmed for {date} at {time} for {party_size}.",
      }
    }
  }
  ```
- **Line 1036**: Global `DRAFTS: Dict[str, Any] = _default_drafts()`.

### 1.3 Load / Save (Disk or Redis)
- **Lines 1038–1042**: `_load_drafts_from_disk()`  
  - Reads via `_safe_read_json_file(DRAFTS_FILE)`.  
  - Expands `{venue}` in path using `_venue_id()`.  
  - If Redis enabled: reads from key `{_REDIS_NS}:{_venue_id()}:drafts`.  
  - Merges payload with `_default_drafts()` and assigns to global `DRAFTS`.
- **Lines 1044–1052**: `_save_drafts_to_disk(patch, actor, role)`  
  - Merges `patch` into `DRAFTS`, sets `updated_at` (UTC ISO), `updated_by`, `updated_role`.  
  - Writes via `_safe_write_json_file(DRAFTS_FILE, DRAFTS)`.  
  - Same path/Redis resolution as above (per-venue).  
  - Returns merged `DRAFTS`.

### 1.4 Startup
- **Lines 1993–1995**: On import, `_load_drafts_from_disk()` is called in a try/except (failure is ignored).

### 1.5 Path/Redis Resolution (shared with other files)
- **Lines 2857–2880**: `_safe_read_json_file(path, default)`  
  - Replaces `{venue}` in `path` with `_venue_id()`.  
  - If Redis enabled and path in `_REDIS_PATH_KEY_MAP`, reads from Redis.  
  - Otherwise reads from disk.
- **Lines 2883–2915+**: `_safe_write_json_file(path, payload)`  
  - Same `{venue}` replacement.  
  - If Redis enabled, writes to Redis; else writes to disk (with `os.makedirs(..., exist_ok=True)` for dir).

### 1.6 Venue Context for Admin
- **Lines 570–575**: `@app.before_request _set_venue_ctx()` sets `g.venue_id = _resolve_venue_id()` (path, query, header, cookie).
- **Lines 577–610**: `_tenant_guard_admin_writes()`: for **POST/PUT/PATCH/DELETE** under `/admin`, requires **explicit** `venue` (query or `X-Venue-Id` header). If missing, returns 403 `venue_required`. Sets `g.venue_id` to that value so draft save uses correct venue.

### 1.7 REST API

#### GET `/admin/api/drafts`
- **Lines 7224–7240**.
- **Auth**: `_require_admin(min_role="manager")`.
- **Behavior**: Calls `_load_drafts_from_disk()` then returns:
  ```json
  {
    "ok": true,
    "drafts": (DRAFTS["drafts"]),
    "meta": {
      "updated_at": ...,
      "updated_by": ...,
      "updated_role": ...
    }
  }
  ```
- **Venue**: Resolved by `_set_venue_ctx()` (e.g. `?venue=qa-sandbox` or referrer path). No body.

#### POST `/admin/api/drafts`
- **Lines 7243–7276**.
- **Auth**: `_require_admin(min_role="owner")` (owner-only).
- **Body**: `{ "drafts": <object> }`. Each value must be an object with at least `body` (string). Keys: `channel`, `title`, `subject`, `body` (body truncated to 5000 chars). Empty body entries are skipped.
- **Venue**: **Required** by `_tenant_guard_admin_writes()` via `?venue=...` or `X-Venue-Id`; otherwise 403.
- **Behavior**: Merges cleaned `drafts` into storage via `_save_drafts_to_disk({"drafts": clean}, actor, role)`, audits `drafts.save`, returns `{ "ok": true, "count": ..., "meta": { "updated_at": ... } }`.

### 1.8 AI “Draft reply” (uses Drafts as templates)
- **Lines 10278–10392**: `POST /admin/api/ai/draft-reply`.
- **Auth**: Owner-only.
- **Body**: `{ "row": <sheet row number> }`.
- **Behavior**:
  - Loads lead from sheet row.
  - Reads **admin-saved drafts** from `(DRAFTS or {}).get("drafts") or {}`.
  - Picks template key `sms_more_info` or `sms_confirm` based on whether lead needs more info.
  - Uses `(d_all.get(draft_key) or {}).get("body")` as base template for OpenAI prompt.
  - Enqueues a `reply_draft` action for staff review (does not send).
- **Related**: `reply_draft` in allow_actions / feature flags (e.g. **Lines 965, 974, 995–996, 1565–1571, 6488–6489, 6508, 6775, 6998, 10327–10329**). Audit event `ai.reply_draft` and queue entry with `type: "reply_draft"`.

---

## 2. Frontend — Template Admin (templates/admin_console.html)

### 2.1 Entry Points
- **Line 49**: Tab button  
  `<button class="tabbtn" data-tab="drafts" data-minrole="owner">Drafts</button>`  
  (owner-only; `data-minrole="owner"`).
- **Lines 97–114**: Pane `#pane-drafts`:
  - Buttons: `#drafts-reload`, `#drafts-save`.
  - Status: `#drafts-status`.
  - Textarea: `#drafts-json` (full-width, min-height 320px, JSON-editable).

### 2.2 JavaScript
- **KEY / VENUE** (Lines 123–124):  
  `KEY = URLSearchParams.get('key')`, `VENUE = URLSearchParams.get('venue')`.  
  **Critical**: Save requires `VENUE`; if missing, user sees “Missing venue in URL (?venue=...).”.
- **draftsLoad()** (Lines 129–146):
  - GET `/admin/api/drafts?key=...` (no venue required for read).
  - Sets `drafts-json` to `JSON.stringify(j.drafts || {}, null, 2)`.
  - Status: “Loaded” or “Loaded • {updated_at}”.
- **draftsSave()** (Lines 148–185):
  - Owner check: `ROLE_RANK[ROLE] < 2` → “Owner only.”.
  - If `!VENUE` → “Missing venue in URL (?venue=...).”.
  - Parses textarea JSON; on failure → “Invalid JSON.”.
  - POST `/admin/api/drafts?key=...&venue=...` with body `{ drafts: draftsObj }`.
  - Status: “Saved” or “Saved • {updated_at}”.
- **Event bindings** (Lines 188–189): `drafts-reload` → `draftsLoad`, `drafts-save` → `draftsSave`.
- **Tab switch** (Line 206): When `tab === 'drafts'`, `draftsLoad()` is called (so opening Drafts tab loads current drafts).

### 2.3 Fetch Interceptor (Lines 223–246)
- For requests to `/admin`, `/api`, `/health`: URL gets `key` and `venue` if missing; headers get `X-Admin-Key` and `X-Venue-Id`. So Drafts API calls from this page automatically get `venue` when user has `?venue=...` in URL.

### 2.4 Role / Tab Visibility
- **Lines 210–214**: `showTab(tab)` checks `data-minrole` on the tab button; if role below required, shows toast “Owner only — redirected to Ops” and switches to Ops. So Drafts tab is visible but only owners can use it successfully; save path also enforces owner and venue.

---

## 3. Frontend — Legacy Admin (inline HTML in app.py)

### 3.1 Entry Points
- **Line 7895**: Tab in “Configure” group:  
  `<button type="button" class="tabbtn" data-tab="drafts" data-minrole="owner" onclick="showTab('drafts');return false;">Drafts</button>`.
- **Lines 8337–8352**: Pane `#tab-drafts`:
  - Title “Draft Templates”.
  - Buttons: Reload (`draftsLoad()`), Save (`draftsSave()`).
  - Status: `#drafts-status`.
  - Note: “Owner-only. Edit JSON and save.”
  - Textarea: `#drafts-json`.

### 3.2 JavaScript (in app.py)
- **Lines 9125–9176**: `draftsLoad()` and `draftsSave()` (same contract as template: GET with key; POST with key and venue; owner check; JSON parse).
- **Line 8487**: When switching to tab `drafts`, `draftsLoad()` is called.
- Legacy admin also has KEY/VENUE from query and same tenant guard; ensure `venue` is passed in URL for POST.

---

## 4. Storage Behavior Summary

| Aspect | Detail |
|--------|--------|
| **Where** | Per-venue file: path from `DRAFTS_FILE` with `{venue}` replaced by `_venue_id()`, e.g. `/tmp/wc26_qa-sandbox_drafts.json`. Or Redis key `{_REDIS_NS}:{venue_id}:drafts`. |
| **When read** | On GET `/admin/api/drafts` (via `_load_drafts_from_disk()`), and at startup. |
| **When written** | On POST `/admin/api/drafts` (owner, with explicit venue). |
| **Not in** | `config/venues/*.json` — drafts are **not** stored in venue config in this repo. |

---

## 5. Permissions & Guards

| Action | Permission | Venue |
|--------|------------|--------|
| GET `/admin/api/drafts` | Manager+ | From request context (query/path/header/cookie). |
| POST `/admin/api/drafts` | **Owner only** | **Required** in query or `X-Venue-Id` (403 if missing). |
| Drafts tab (UI) | Owner (enforced by `data-minrole="owner"` and role check in `draftsSave`). | Frontend requires `?venue=...` for save. |

---

## 6. Related “Draft” Concepts (not the Drafts tab)

- **reply_draft** (AI): Allowed action type; stores draft text in audit/queue; does not send. Controlled by `allow_actions.reply_draft` and feature `auto_reply_draft`. References admin-saved drafts as templates in `/admin/api/ai/draft-reply`.
- **UI labels**: “Reply drafts” checkbox (e.g. `#ai-act-draft`), “Suggest = AI drafts/recommends” note in legacy admin and in `admin_dump.html`. These refer to AI reply-draft behavior, not the Draft Templates storage.

---

## 7. Files Touched by Drafts Feature

| File | What |
|------|------|
| **app.py** | DRAFTS_FILE, _default_drafts, DRAFTS, _load_drafts_from_disk, _save_drafts_to_disk, _safe_*_json_file (venue expansion + Redis), _set_venue_ctx, _tenant_guard_admin_writes, GET/POST /admin/api/drafts, /admin/api/ai/draft-reply, startup _load_drafts_from_disk, reply_draft allow/feature and queue handling, legacy HTML pane and JS for Drafts tab. |
| **templates/admin_console.html** | Drafts tab button, #pane-drafts, draftsLoad/draftsSave, KEY/VENUE, fetch interceptor, showTab('drafts') → draftsLoad. |
| **config/venues/** | No drafts key; drafts are not stored here. |

---

## 8. Checklist for Parity / Restore

- [ ] GET `/admin/api/drafts` returns `ok`, `drafts`, `meta.updated_at/by/role`.
- [ ] POST `/admin/api/drafts` requires owner and `venue` (query or header); returns 403 if venue missing.
- [ ] Drafts tab visible in both template admin (`/admin_tpl`) and legacy admin (`/admin`); owner-only; opening tab triggers load.
- [ ] Save from UI sends `?venue=...` (and key); frontend shows “Missing venue in URL” when venue is missing on save.
- [ ] Storage: per-venue file (path with `{venue}`) or Redis key per venue; not in venue config JSON.
- [ ] Default draft entry `sms_confirm` exists when no file/Redis data.
- [ ] `/admin/api/ai/draft-reply` uses `DRAFTS["drafts"]` (e.g. `sms_confirm`, `sms_more_info`) as template body for AI.

Use this document as the single source of truth for “Drafts functionality as implemented” when comparing with production, staging, or the reference repo/branch.
