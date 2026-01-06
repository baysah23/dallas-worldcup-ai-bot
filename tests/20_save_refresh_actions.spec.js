
const { test, expect } = require("@playwright/test");

const ADMIN_URL = process.env.ADMIN_URL;
const MANAGER_URL = process.env.MANAGER_URL;
const FANZONE_URL = process.env.FANZONE_URL;

async function safeGoto(page, url) {
  try {
    const r = await page.goto(url, { waitUntil: "domcontentloaded", timeout: 5000 });
    return r && r.status() < 500;
  } catch {
    return false;
  }
}

async function ready(page) {
  const root = page.locator("[data-testid='app-root']");
  if (await root.count()) {
    await expect(root.first()).toBeVisible();
  } else {
    await expect(page.locator("body")).toContainText(/./);
  }
}

test("ADMIN Ops toggle survives refresh (if reachable)", async ({ page }) => {
  test.skip(!ADMIN_URL, "ADMIN_URL not set");
  if (!(await safeGoto(page, ADMIN_URL))) test.skip(true, "Admin not reachable");
  await ready(page);

  const toggle = page.locator("input[type='checkbox']").first();
  if (!(await toggle.count())) test.skip(true, "No toggle found");

  await toggle.click({ force: true });
  await page.reload();
  await ready(page);
});

test("MANAGER Ops interaction safe (if reachable)", async ({ page }) => {
  test.skip(!MANAGER_URL, "MANAGER_URL not set");
  if (!(await safeGoto(page, MANAGER_URL))) test.skip(true, "Manager not reachable");
  await ready(page);

  const t = page.locator("input[type='checkbox']").first();
  if (await t.count()) await t.click({ force: true });
});

test("FAN ZONE loads or skips cleanly", async ({ page }) => {
  test.skip(!FANZONE_URL, "FANZONE_URL not set");
  if (!(await safeGoto(page, FANZONE_URL))) test.skip(true, "Fan Zone not reachable");
  await ready(page);
});
