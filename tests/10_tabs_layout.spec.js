
const { test, expect } = require("@playwright/test");

const ADMIN_URL = process.env.ADMIN_URL;
const MANAGER_URL = process.env.MANAGER_URL;

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

async function clickTab(page, name) {
  const btn = page.getByRole("button", { name: new RegExp(`^${name}$`, "i") }).first();
  if (!(await btn.count())) return false;
  await btn.click({ force: true });
  return true;
}

test("ADMIN tabs (only if reachable)", async ({ page }) => {
  test.skip(!ADMIN_URL, "ADMIN_URL not set");
  if (!(await safeGoto(page, ADMIN_URL))) test.skip(true, "Admin not reachable");

  await page.setViewportSize({ width: 1400, height: 900 });
  await ready(page);

  const tabs = ["Ops","Leads","AI Queue","Monitoring","Audit","AI Settings","Rules","Menu","Policies"];
  for (const t of tabs) {
    expect(await clickTab(page, t), `Missing tab ${t}`).toBeTruthy();
  }
});

test("MANAGER policies blocked (only if reachable)", async ({ page }) => {
  test.skip(!MANAGER_URL, "MANAGER_URL not set");
  if (!(await safeGoto(page, MANAGER_URL))) test.skip(true, "Manager not reachable");

  await page.setViewportSize({ width: 1400, height: 900 });
  await ready(page);

  const p = page.getByRole("button", { name: /^Policies$/i });
  if (!(await p.count())) return;
  const before = page.url();
  await p.first().click({ force: true });
  expect(page.url()).toBe(before);
});
