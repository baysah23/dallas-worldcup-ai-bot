const { test, expect } = require("@playwright/test");

function isCI() {
  return String(process.env.CI || "").toLowerCase() === "true";
}

function baseFrom(url) {
  const u = new URL(url);
  u.search = "";
  u.hash = "";
  // keep path root only
  u.pathname = "";
  return u.toString().replace(/\/+$/, "");
}

function requireUrl(name, value) {
  // In CI, URLs must be explicitly provided.
  if (isCI()) {
    expect(value, `${name} must be set in CI`).toBeTruthy();
  }
  return value;
}

async function gotoOrFail(page, url, label) {
  try {
    const r = await page.goto(url, { waitUntil: "domcontentloaded", timeout: 15000 });
    expect(r, `${label} navigation returned no response: ${url}`).toBeTruthy();
    // Fail loudly on server errors
    expect(r.status(), `${label} returned HTTP ${r.status()} for ${url}`).toBeLessThan(500);
    return r;
  } catch (e) {
    throw new Error(`${label} service not reachable: ${url}. ${e?.message || e}`);
  }
}

async function ready(page) {
  const rootSel = process.env.APP_ROOT_SELECTOR || "[data-testid='app-root']";
  const root = page.locator(rootSel);
  if (await root.count()) {
    await expect(root.first()).toBeVisible({ timeout: 15000 });
  } else {
    await expect(page.locator("body")).toContainText(/./, { timeout: 15000 });
  }
}

// Local-friendly defaults (CI must set env vars explicitly)
const DEFAULT_BASE = "http://127.0.0.1:5000";
const DEFAULT_ADMIN_URL = `${DEFAULT_BASE}/admin?key=baysah23_worldcup_admin_7f3b9a8c2d1e4f5a9c0b`;
const DEFAULT_MANAGER_URL = `${DEFAULT_BASE}/manager?key=10060f5c3997567470fc887785132a60`;
const DEFAULT_FANZONE_URL = `${DEFAULT_BASE}/admin/fanzone?key=baysah23_worldcup_admin_7f3b9a8c2d1e4f5a9c0b`;

const ADMIN_URL = requireUrl("ADMIN_URL", process.env.ADMIN_URL) || DEFAULT_ADMIN_URL;
const MANAGER_URL = requireUrl("MANAGER_URL", process.env.MANAGER_URL) || DEFAULT_MANAGER_URL;
const FANZONE_URL = requireUrl("FANZONE_URL", process.env.FANZONE_URL) || DEFAULT_FANZONE_URL;

test("Admin page loads", async ({ page }) => {
  await gotoOrFail(page, ADMIN_URL, "Admin");
  await ready(page);
});

test("Manager page loads", async ({ page }) => {
  await gotoOrFail(page, MANAGER_URL, "Manager");
  await ready(page);
});

test("Fan Zone page loads", async ({ page }) => {
  await gotoOrFail(page, FANZONE_URL, "Fan Zone");
  await ready(page);
});
