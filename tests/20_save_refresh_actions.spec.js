const { test, expect } = require("@playwright/test");

const ADMIN_URL = process.env.ADMIN_URL;
const MANAGER_URL = process.env.MANAGER_URL;
const FANZONE_URL = process.env.FANZONE_URL;

function mustUrl(name, value) {
  expect(value, `${name} must be set`).toBeTruthy();
  return value;
}

async function gotoOrFail(page, url) {
  const r = await page.goto(url, { waitUntil: "domcontentloaded", timeout: 15000 });
  expect(r).toBeTruthy();
  expect(r.status()).toBeLessThan(500);
}

async function ready(page) {
  const rootSel = process.env.APP_ROOT_SELECTOR || "[data-testid='app-root']";
  if (await page.locator(rootSel).count()) {
    await expect(page.locator(rootSel).first()).toBeVisible();
  } else {
    await expect(page.locator("body")).toContainText(/./);
  }
}

async function gotoOps(page) {
  const btn = page.locator('.tabbtn[data-tab="ops"]').first();
  if (await btn.count()) {
    await btn.click({ force: true });
    await ready(page);
    return true;
  }
  return false;
}

async function findOpsToggle(page) {
  const selectors = [
    '[data-testid^="ops-toggle"]',
    'button[aria-pressed]',
    '.ops-toggle'
  ];

  for (const sel of selectors) {
    const el = page.locator(sel).first();
    if (await el.count()) return el;
  }
  return null;
}

test("ADMIN Ops toggle survives refresh (if present)", async ({ page }) => {
  await page.setViewportSize({ width: 1400, height: 900 });

  await gotoOrFail(page, mustUrl("ADMIN_URL", ADMIN_URL));
  await ready(page);
  await gotoOps(page);

  const toggle = await findOpsToggle(page);
  if (!toggle) test.skip(true, "No Ops toggles rendered");

  const before = await toggle.getAttribute("aria-pressed");
  await toggle.click({ force: true });

  await page.reload({ waitUntil: "domcontentloaded" });
  await ready(page);
  await gotoOps(page);

  const toggle2 = await findOpsToggle(page);
  expect(toggle2).toBeTruthy();

  const after = await toggle2.getAttribute("aria-pressed");
  expect(after).toBe(before === "true" ? "false" : "true");
});

test("MANAGER Ops toggle optional (no crash)", async ({ page }) => {
  await page.setViewportSize({ width: 1400, height: 900 });

  await gotoOrFail(page, mustUrl("MANAGER_URL", MANAGER_URL));
  await ready(page);
  await gotoOps(page);

  const toggle = await findOpsToggle(page);
  if (!toggle) return; // valid by design

  await toggle.click({ force: true }).catch(() => {});
  await ready(page);
});

test("FAN ZONE loads", async ({ page }) => {
  await page.setViewportSize({ width: 1400, height: 900 });

  await gotoOrFail(page, mustUrl("FANZONE_URL", FANZONE_URL));
  await ready(page);
});