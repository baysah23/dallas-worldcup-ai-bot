// tests/30_visual_regression.spec.js
const { test, expect } = require("@playwright/test");

const ADMIN_URL = process.env.ADMIN_URL;
const MANAGER_URL = process.env.MANAGER_URL;
const FANZONE_URL = process.env.FANZONE_URL;

if (!ADMIN_URL || !MANAGER_URL || !FANZONE_URL) {
  throw new Error(
    "Missing env vars: ADMIN_URL, MANAGER_URL, FANZONE_URL. Set them in CI secrets or your shell."
  );
}

// Small stabilization helper: wait for layout + fonts to settle
async function stabilize(page) {
  await page.waitForLoadState("domcontentloaded");
  await page.waitForLoadState("networkidle").catch(() => {});
  await page.waitForTimeout(200);
}

// Mask things that change (timestamps, “last updated”, etc.)
function commonMasks(page) {
  return [
    // Common “dynamic” targets (safe to keep even if selector not found)
    page.locator("[data-testid='timestamp']"),
    page.locator("[data-testid='last-updated']"),
    page.locator(".toast"),
    page.locator("#toast"),
    page.locator(".saving"),
    page.locator(".status-pill"),
    page.locator(".audit-timestamp"),
    page.locator(".updated-at"),
  ];
}

const SNAP_OPTS = {
  fullPage: false, // viewport only (stable)
  animations: "disabled",
  // tiny rendering diffs are normal across machines; keep this small but non-zero
  maxDiffPixelRatio: 0.01,
};

async function clickTab(page, label) {
  // Prefer button by role/name if present
  const btn = page.getByRole("button", { name: label });
  if (await btn.count()) {
    await btn.first().click();
    return;
  }

  // Fallback: data-tab button if your UI uses it
  const byData = page.locator(`button[data-tab], .tabbtn`).filter({ hasText: label });
  if (await byData.count()) {
    await byData.first().click();
    return;
  }

  throw new Error(`Could not find tab button for "${label}"`);
}

test.describe("Visual - ADMIN tab panes", () => {
  test("Admin tab panes stable viewport snapshots", async ({ page }) => {
    await page.goto(ADMIN_URL, { waitUntil: "domcontentloaded" });
    await stabilize(page);

    const tabs = [
      "Ops",
      "Leads",
      "AI Queue",
      "Monitoring",
      "Audit",
      "AI Settings",
      "Rules",
      "Menu",
      "Policies",
    ];

    for (const t of tabs) {
      await clickTab(page, t);
      await stabilize(page);

      const safe = t.replace(/\s+/g, "_").toLowerCase();
      await expect(page).toHaveScreenshot(
        `admin-${safe}.png`,
        { ...SNAP_OPTS, mask: commonMasks(page) }
      );
    }
  });
});

test.describe("Visual - MANAGER core panes", () => {
  test("Manager core panes stable viewport snapshots", async ({ page }) => {
    await page.goto(MANAGER_URL, { waitUntil: "domcontentloaded" });
    await stabilize(page);

    // Keep these to the ones managers actually use
    const tabs = ["Ops", "Leads", "AI Queue", "Monitoring", "Audit"];

    for (const t of tabs) {
      await clickTab(page, t);
      await stabilize(page);

      const safe = t.replace(/\s+/g, "_").toLowerCase();
      await expect(page).toHaveScreenshot(
        `manager-${safe}.png`,
        { ...SNAP_OPTS, mask: commonMasks(page) }
      );
    }
  });
});

test.describe("Visual - FANZONE page", () => {
  test("Fan Zone stable viewport snapshot", async ({ page }) => {
    await page.goto(FANZONE_URL, { waitUntil: "domcontentloaded" });
    await stabilize(page);

    await expect(page).toHaveScreenshot(
      "fanzone.png",
      { ...SNAP_OPTS, mask: commonMasks(page) }
    );
  });
});