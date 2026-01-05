const { test, expect } = require("@playwright/test");

const ADMIN_URL =
  process.env.ADMIN_URL ||
  "http://127.0.0.1:5050/admin?key=REPLACE_ADMIN_KEY";

const MANAGER_URL =
  process.env.MANAGER_URL ||
  // NOTE: your original default was /admin by mistake.
  // Managers should point to /manager (or your real manager route).
  "http://127.0.0.1:5050/manager?key=REPLACE_MANAGER_KEY";

const FANZONE_URL =
  process.env.FANZONE_URL ||
  "http://127.0.0.1:5050/admin/fanzone?key=REPLACE_ADMIN_KEY";

/**
 * Collect JS crashes + console.error so CI can fail fast with context.
 */
function attachConsoleCollectors(page, errors) {
  page.on("pageerror", (err) => errors.push(`[pageerror] ${err.message}`));
  page.on("console", (msg) => {
    if (msg.type() === "error") errors.push(`[console.error] ${msg.text()}`);
  });
}

/**
 * Deterministic "page is ready" check.
 * Use ONE stable selector that exists on all your panel pages.
 *
 * ✅ Best: add <div data-testid="app-root"> ... </div>
 * If you don’t have that yet, we fall back to checking <body> has non-trivial content.
 */
async function waitForAppReady(page) {
  // If you already have a stable root selector, set it here:
  const ROOT = process.env.APP_ROOT_SELECTOR || "[data-testid='app-root']";

  const root = page.locator(ROOT);

  // If the root exists, wait for it.
  if (await root.count()) {
    await expect(root.first()).toBeVisible({ timeout: 8000 });
    return;
  }

  // Fallback: body should not be empty/placeholder.
  // This avoids passing when the page is blank due to JS dying early.
  await expect(page.locator("body")).toContainText(/./, { timeout: 8000 });
}

async function runOne(page, url, label) {
  const errors = [];
  attachConsoleCollectors(page, errors);

  const resp = await page.goto(url, { waitUntil: "domcontentloaded" });

  // Deterministic server-side check
  expect(resp, `[${label}] did not get a response from server`).toBeTruthy();
  expect(
    resp.status(),
    `[${label}] expected 2xx/3xx but got ${resp.status()}`
  ).toBeLessThan(400);

  // Deterministic app-ready check (no fixed sleeps)
  await waitForAppReady(page);

  if (errors.length) {
    console.log(`\n=== ${label.toUpperCase()} PAGE ERRORS ===\n`);
    console.log(errors.join("\n"));
    console.log("\n===========================\n");
  }

  expect(errors, errors.join("\n")).toHaveLength(0);
}

test("Admin page loads (no JS crashes)", async ({ page }) => {
  await runOne(page, ADMIN_URL, "admin");
});

test("Manager page loads (no JS crashes)", async ({ page }) => {
  await runOne(page, MANAGER_URL, "manager");
});

test("Fan Zone page loads (no JS crashes)", async ({ page }) => {
  await runOne(page, FANZONE_URL, "fanzone");
});