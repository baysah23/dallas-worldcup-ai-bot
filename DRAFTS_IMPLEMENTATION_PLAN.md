# Drafts Feature — Complete Implementation Plan

**Branch:** `chore/complete-draft-functionality`  
**Goal:** Store drafts only in dedicated per-venue storage; never in venue config. Support create, edit, delete, retrieve for all venues (qa-sandbox, demo, demo1). Align with Redis when enabled.

---

## 1. My Understanding

### Client requirements (summary)
- **Venue config files** = static (identity, keys, feature flags). Must **not** be modified for drafts.
- **Drafts** = operational reply templates (SMS, email, WhatsApp, AI Queue responses). Must be stored in **dedicated per-venue drafts storage** only.
- **Storage:**
  - **File:** Per-venue path, e.g. `/tmp/wc26_qa-sandbox_drafts.json`, `/tmp/wc26_demo_drafts.json` (or env override `DRAFTS_FILE` with `{venue}` placeholder).
  - **Redis:** When enabled, same per-venue namespace for drafts (e.g. `wc26:qa-sandbox:drafts`).
- **If staging currently writes drafts into** venue config (e.g. `config/venues/<venue>.json` or `/home/site/venues/...`), that is **wrong** and must be corrected.
- **Correct behavior:**
  1. Drafts persist **only** in per-venue drafts store (Redis or `/tmp/wc26_{venue}_drafts.json`).
  2. Venue configuration files remain **unchanged** after venue creation (no `drafts` key written).
  3. Existing drafts in venue config (if any) are **migrated** out into the dedicated store; then removed from venue config.
  4. Drafts tab **reads/writes correctly** for all venues (qa-sandbox, demo, demo1).
- **Confirm:** Creation, edit, delete, and retrieval work across all staging venues.

### Current state on this branch
- **Wrong:** `admin_api_drafts()` (lines 6375–6458) reads/writes drafts **from/to venue config** (`VENUES_DIR/{venue_id}.json`, key `"drafts"`). It loads the whole config, sets `vcfg["drafts"] = drafts_data`, and writes the whole file back. So venue config is being modified.
- **Missing:** No `DRAFTS_FILE` constant, no `_load_drafts_from_disk` / `_save_drafts_to_disk`, no drafts entry in `_REDIS_PATH_KEY_MAP`. So there is no dedicated drafts storage on this branch.
- **UI:** Drafts modal (legacy admin) and standalone `/admin/drafts` page and template `admin_drafts.html` exist and call `GET/POST /admin/api/drafts` with `?venue=...`. They will work unchanged once the API uses dedicated storage; only copy and storage path need updating.

### What I will do
1. **Introduce dedicated drafts storage:** Add `DRAFTS_FILE`, default structure, load/save helpers, and use them (and Redis when enabled) so drafts never touch venue config.
2. **Change the Drafts API** to read/write only the dedicated store; set `g.venue_id` from `venue` query so per-venue path/Redis key is correct.
3. **One-time migration:** For each venue that has a `drafts` key in its config file, copy drafts into the per-venue drafts store, then remove `drafts` (and optionally `updated_at` if only used for drafts) from venue config and save the config back.
4. **Keep API contract:** GET/POST `/admin/api/drafts?key=...&venue=...` and response shape unchanged so existing UI keeps working.
5. **Update copy:** Replace “Stored in venue config” with “Stored in per-venue drafts file (or Redis)” in admin_drafts.html and legacy modal.
6. **Verify:** Create, edit, delete (by removing keys in JSON and saving), and retrieve for qa-sandbox, demo, demo1; confirm venue config files are no longer modified.

---

## 2. Client Requirements (Checklist)

| # | Requirement | Status |
|---|-------------|--------|
| 1 | Drafts NOT stored in venue configuration JSON | ❌ Currently in venue config → will fix |
| 2 | Venue config = static (identity, keys, feature flags only) | ❌ Currently written for drafts → will fix |
| 3 | Drafts = operational templates (SMS, email, WhatsApp, AI Queue) | ✅ API already supports object of templates |
| 4 | Storage = dedicated per-venue file, e.g. `/tmp/wc26_{venue}_drafts.json` | ❌ Not implemented → will add |
| 5 | If Redis enabled, same per-venue namespace for drafts | ❌ Not in Redis map → will add |
| 6 | Do not write drafts to `/home/site/venues/` or venue config | ❌ Currently writing to venue config → will fix |
| 7 | Migrate existing drafts out of venue config into dedicated store | ⏳ Will implement one-time migration |
| 8 | Drafts tab read/write correct for all venues | ✅ API is venue-scoped; storage fix will complete this |
| 9 | Create, edit, delete, retrieval working across staging venues | ⏳ Verify after implementation |

---

## 3. What Is Missing (Gap List)

1. **No `DRAFTS_FILE`** — No constant for per-venue drafts path.
2. **No drafts in `_REDIS_PATH_KEY_MAP`** — Redis not used for drafts when enabled.
3. **No `_default_drafts()`** — No default structure (e.g. `sms_confirm` template).
4. **No `_load_drafts_from_disk()` / `_save_drafts_to_disk()`** — No helpers using `_safe_read_json_file` / `_safe_write_json_file` (so no file + Redis support).
5. **API uses venue config** — `admin_api_drafts()` reads/writes `VENUES_DIR/{venue_id}.json` and mutates `vcfg["drafts"]`; must switch to dedicated store.
6. **No migration** — No step to move existing `drafts` from venue config into the dedicated store and then remove from config.
7. **Copy says “venue config”** — admin_drafts.html and legacy modal say “Stored in venue config”; should say dedicated per-venue store.
8. **GET and POST must set venue context** — Before read/write, `g.venue_id` must be set from `venue` query so `_venue_id()` and path/Redis key are correct for the requested venue.

---

## 4. Complete Todo List

### Phase A: Backend — Dedicated drafts storage
- [ ] **A1** Add `DRAFTS_FILE = os.environ.get("DRAFTS_FILE", "/tmp/wc26_{venue}_drafts.json")` near other `*_FILE` constants.
- [ ] **A2** Add `DRAFTS_FILE` to `_REDIS_PATH_KEY_MAP` with value `"drafts"` so Redis is used when enabled.
- [ ] **A3** Implement `_default_drafts()` returning `{ "updated_at": None, "updated_by": None, "updated_role": None, "drafts": { "sms_confirm": { "channel": "sms", "title": "SMS — Confirmation", "body": "Hi {name}, you're confirmed for {date} at {time} for {party_size}." } } }`.
- [ ] **A4** Implement `_load_drafts_from_disk()`: use `_safe_read_json_file(DRAFTS_FILE)`, merge with `_default_drafts()`, store in a global or pass back (see below). Ensure `g.venue_id` is set to the target venue before calling (so path/Redis key are per-venue).
- [ ] **A5** Implement `_save_drafts_to_disk(patch, actor, role)`: merge patch into current drafts, set `updated_at`, `updated_by`, `updated_role`, write via `_safe_write_json_file(DRAFTS_FILE, merged)`. Require `g.venue_id` set.
- [ ] **A6** Decide in-memory model: either (1) no global `DRAFTS` and load on every GET/save on every POST, or (2) global `DRAFTS` and load once per request after setting `g.venue_id`. Option (1) is simpler and multi-venue safe; recommend load in GET and in POST (read–merge–write).

### Phase B: Backend — API and venue context
- [ ] **B1** Replace `admin_api_drafts()` implementation: require `venue` query param; set `g.venue_id = _slugify_venue_id(venue)` for the request so `_venue_id()` is correct.
- [ ] **B2** GET: do not read venue config. Call `_load_drafts_from_disk()` (or equivalent read from `_safe_read_json_file(DRAFTS_FILE)`), return `{ "ok": True, "drafts": ..., "meta": { "updated_at", "updated_by", "updated_role" } }`. If file/Redis missing, return default drafts.
- [ ] **B3** POST: validate body `drafts` (object). Optionally sanitize keys (e.g. channel, title, subject, body; body max length). Call `_save_drafts_to_disk({"drafts": clean}, actor, role)`. Return same shape as GET with updated meta. Do not read or write venue config.
- [ ] **B4** Remove all logic that reads or writes `vcfg["drafts"]` or opens `VENUES_DIR/{venue_id}.json` for drafts.

### Phase C: Migration (drafts out of venue config)
- [ ] **C1** Add a one-time migration path: e.g. on first GET (or a dedicated migration script/endpoint) per venue: if venue config file exists and has key `"drafts"` (and is a dict), read it, write to dedicated store via `_save_drafts_to_disk({"drafts": ...}, actor="migration", role="system")`, then remove `"drafts"` (and draft-only `updated_at` if desired) from venue config and write config back. Ensure `g.venue_id` is set for that venue during migration.
- [ ] **C2** Document migration in this plan or in a short MIGRATION.md (what was moved, how to run if script).

### Phase D: Frontend and copy
- [ ] **D1** In `templates/admin_drafts.html`: change “Drafts are stored in venue config.” to “Drafts are stored in the per-venue drafts store (file or Redis).”
- [ ] **D2** In legacy admin inline HTML (Drafts modal): change “Stored in venue config” to “Stored in per-venue drafts file (or Redis).”

### Phase E: Delete and validation
- [ ] **E1** “Delete” = user removes a key from the JSON and saves; no new endpoint if POST replaces the whole `drafts` object. Confirm in UI that saving with fewer keys removes those templates.
- [ ] **E2** Optional: validate each draft entry (e.g. `channel`, `body` required; `body` max 5000 chars) and return 400 with clear error if invalid.

### Phase F: Verification and docs
- [ ] **F1** Test GET/POST for qa-sandbox, demo, demo1 (with `?venue=...`). Confirm files created under `/tmp/wc26_*_drafts.json` (or Redis keys) and venue config files are not modified.
- [ ] **F2** Test create (add key in JSON, save), edit (change body, save), delete (remove key, save), retrieve (reload). Confirm across venues.
- [ ] **F3** Update DRAFTS_FUNCTIONALITY_REFERENCE.md (or this plan) to state that drafts are only in DRAFTS_FILE / Redis, never in venue config.
- [ ] **F4** Confirm AI/reply-draft code (if any) that uses drafts reads from the same store (e.g. after loading drafts for current venue); no reference to venue config for drafts.

---

## 5. Implementation Order (Recommended)

1. **A1–A5** — Add DRAFTS_FILE, Redis map entry, default, load/save (no global; load on each GET, read–merge–write on POST).
2. **B1–B4** — Rewrite `admin_api_drafts()` to set venue context and use only dedicated store.
3. **C1–C2** — Migration: on GET (or script), if venue config has `drafts`, copy to store and remove from config.
4. **D1–D2** — Update UI copy.
5. **E1–E2** — Confirm delete behavior; optional validation.
6. **F1–F4** — Manual tests and doc update.

---

## 6. Files to Touch

| File | Changes |
|------|--------|
| **app.py** | Add DRAFTS_FILE, Redis map, _default_drafts, _load_drafts_from_disk, _save_drafts_to_disk; rewrite admin_api_drafts() to use them and set g.venue_id; add migration step (read drafts from venue config if present, write to store, remove from config). |
| **templates/admin_drafts.html** | Replace “venue config” with “per-venue drafts store (file or Redis)”. |
| **app.py** (legacy modal) | Same copy change for Drafts modal. |

---

## 7. Out of Scope (No Change)

- Venue config schema (identity, keys, feature flags) — no change.
- Other admin tabs or APIs — no change.
- Fan-facing behavior — no change (drafts are admin-only templates).

---

Once these items are done, drafts will be stored only in the dedicated per-venue store, venue config will remain static, and create/edit/delete/retrieve will work for all staging venues. Use this plan as the single todo list for the feature.
