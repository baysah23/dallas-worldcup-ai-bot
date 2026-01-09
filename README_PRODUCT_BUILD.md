# World Cup Concierge — Product Build v1.1.0

This bundle is generated from your latest baseline and is intended to be dropped in as-is.

## What's in this build
- Fixes: `/admin/api/_build` now returns build info + version.
- Adds: `SUPER_ADMIN_KEY` support and `/super/admin` hard-isolated console.
- Adds: `/super/api/overview` cross-venue AI Queue counts (best-effort, never hard-fails).

## Usage
Set env vars:
- ADMIN_OWNER_KEY=...
- ADMIN_MANAGER_KEYS=...
- SUPER_ADMIN_KEY=...   (platform-owner only)
- APP_VERSION=1.1.0

Super Admin:
`/super/admin?key=SUPER_ADMIN_KEY`

Build check:
`/admin/api/_build?key=ADMIN_OWNER_KEY`
