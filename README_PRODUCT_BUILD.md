# World Cup Concierge – Product Build v1 (Baseline)

This bundle is built from your latest attached files and is treated as v1.0 baseline.

## New in this build
- Fixed `/admin/api/_build` (now returns version + runtime flags)
- Added Super Admin separation:
  - `GET /super/admin?key=SUPER_ADMIN_KEY`
  - `GET /super/api/overview?key=SUPER_ADMIN_KEY` (read-only, cross-venue queue counts)
- Added missing helper `_is_super_admin_request()` used by `/admin/api/leads_all`:
  - Call: `/admin/api/leads_all?key=ADMIN_OWNER_KEY&super_key=SUPER_ADMIN_KEY`

## Environment variables
- `ADMIN_OWNER_KEY` (existing)
- `SUPER_ADMIN_KEY` (new, required for /super/* and for cross-venue leads_all)
- `APP_VERSION` (optional; default 1.0.0)
