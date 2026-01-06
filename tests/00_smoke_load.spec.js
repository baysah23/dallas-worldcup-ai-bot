const { test, expect } = require("@playwright/test");

const ADMIN_URL =
  process.env.ADMIN_URL ||
  "http://127.0.0.1:5050/admin?key=REPLACE_ADMIN_KEY";

const MANAGER_URL =
  process.env.MANAGER_URL ||
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
 */
async function waitForAppReady(page) {
  const ROOT = process.env.APP_ROOT_SELECTOR || "[data-testid='app-root']";
  const root = page.locator(ROOT);

  if (await root.count()) {
    await expect(root.first()).toBeVisible({ timeout: 8000 });
    return;
  }

  await expect(page.locator("body")).toContainText(/./, { timeout: 8000 });
}

async function runOne(page, url, label) {
  const errors = [];
  attachConsoleCollectors(page, errors);

  const resp = await page.goto(url, { waitUntil: "domcontentloaded" });

  expect(resp, `[${label}] did not get a response from server`).toBeTruthy();
  expect(resp.status()).toBeLessThan(400);

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
