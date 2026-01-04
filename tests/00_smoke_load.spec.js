const { test, expect } = require("@playwright/test");

const ADMIN_URL = process.env.ADMIN_URL;
const MANAGER_URL = process.env.MANAGER_URL;
const FANZONE_URL = process.env.FANZONE_URL; // optional

function must(v, name) {
  if (!v) throw new Error(`Missing env var: ${name}`);
  return v;
}

async function captureConsoleErrors(page) {
  const errors = [];

  page.on("console", (msg) => {
    if (msg.type() === "error") {
      const loc = msg.location?.();
      const where =
        loc && loc.url
          ? ` @ ${loc.url}:${loc.lineNumber || 0}:${loc.columnNumber || 0}`
          : "";
      errors.push(`[console.error] ${msg.text()}${where}`);
    }
  });

  page.on("pageerror", (err) => {
    // IMPORTANT: print stack so we get file:line:col
    const stack = err?.stack || "";
    const msg = err?.message || String(err);
    errors.push(`[pageerror] ${msg}${stack ? "\n" + stack : ""}`);
  });

  return errors;
}

async function runOne(label, page, url) {
  const errors = await captureConsoleErrors(page);

  await page.goto(url, { waitUntil: "domcontentloaded" });
  await expect(page.locator("body")).toBeVisible();

  if (errors.length) {
    console.log(`\n=== ${label} PAGE ERRORS ===\n`);
    console.log(errors.join("\n\n"));
    console.log(`\n===========================\n`);
  }

  expect(errors, errors.join("\n")).toHaveLength(0);
}

test("Admin page loads with no fatal console errors", async ({ page }) => {
  await runOne("ADMIN", page, must(ADMIN_URL, "ADMIN_URL"));
});

test("Manager page loads with no fatal console errors", async ({ page }) => {
  await runOne("MANAGER", page, must(MANAGER_URL, "MANAGER_URL"));
});

test("Fan Zone page loads with no fatal console errors", async ({ page }) => {
  if (!FANZONE_URL) test.skip(true, "FANZONE_URL not set");
  await runOne("FANZONE", page, FANZONE_URL);
});