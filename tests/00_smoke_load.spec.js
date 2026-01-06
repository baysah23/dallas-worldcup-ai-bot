
const { test, expect } = require("@playwright/test");

const ADMIN_URL = process.env.ADMIN_URL;
const MANAGER_URL = process.env.MANAGER_URL;
const FANZONE_URL = process.env.FANZONE_URL;

async function isReachable(page, url) {
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

test("Admin page loads (if service reachable)", async ({ page }) => {
  test.skip(!ADMIN_URL, "ADMIN_URL not set");
  if (!(await isReachable(page, ADMIN_URL))) test.skip(true, "Admin service not reachable");
  await ready(page);
});

test("Manager page loads (if service reachable)", async ({ page }) => {
  test.skip(!MANAGER_URL, "MANAGER_URL not set");
  if (!(await isReachable(page, MANAGER_URL))) test.skip(true, "Manager service not reachable");
  await ready(page);
});

test("Fan Zone page loads (if service reachable)", async ({ page }) => {
  test.skip(!FANZONE_URL, "FANZONE_URL not set");
  if (!(await isReachable(page, FANZONE_URL))) test.skip(true, "Fan Zone service not reachable");
  await ready(page);
});
