const { test, expect } = require("@playwright/test");

const ADMIN_URL =
  process.env.ADMIN_URL ||
  "http://127.0.0.1:5050/admin?key=REPLACE_ADMIN_KEY";
const MANAGER_URL =
  process.env.MANAGER_URL ||
  "http://127.0.0.1:5050/admin?key=REPLACE_MANAGER_KEY";
const FANZONE_URL =
  process.env.FANZONE_URL ||
  "http://127.0.0.1:5050/admin/fanzone?key=REPLACE_ADMIN_KEY";

function attachConsoleCollectors(page, errors) {
  page.on("pageerror", (err) => errors.push(`[pageerror] ${err.message}`));
  page.on("console", (msg) => {
    const type = msg.type();
    const text = msg.text();
    if (type === "error") errors.push(`[console.error] ${text}`);
  });
}

async function runOne(page, url, label) {
  const errors = [];
  attachConsoleCollectors(page, errors);

  await page.goto(url, { waitUntil: "domcontentloaded" });
  await page.waitForTimeout(500);

  // If your app has a known global error banner, check it here (optional):
  // const fatalBanner = page.locator(".fatal, .error-banner");
  // if (await fatalBanner.count()) errors.push(`[ui] fatal banner present`);

  if (errors.length) {
    console.log(`\n=== ${label.toUpperCase()} PAGE ERRORS ===\n`);
    console.log(errors.join("\n"));
    console.log("\n===========================\n");
  }

  expect(errors, errors.join("\n")).toHaveLength(0);
}

test("Admin page loads with no fatal console errors", async ({ page }) => {
  await runOne(page, ADMIN_URL, "admin");
});

test("Manager page loads with no fatal console errors", async ({ page }) => {
  await runOne(page, MANAGER_URL, "manager");
});

test("Fan Zone page loads with no fatal console errors", async ({ page }) => {
  await runOne(page, FANZONE_URL, "fanzone");
});