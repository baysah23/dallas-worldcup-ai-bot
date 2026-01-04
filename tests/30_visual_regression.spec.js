// tests/30_visual_regression.spec.js
const { test, expect } = require("@playwright/test");

const ADMIN_URL = process.env.ADMIN_URL;
const MANAGER_URL = process.env.MANAGER_URL;
const FANZONE_URL = process.env.FANZONE_URL;

if (!ADMIN_URL || !MANAGER_URL || !FANZONE_URL) {
  throw new Error(
    "Missing env vars: ADMIN_URL, MANAGER_URL, FANZONE_URL. Set them in CI/workflow."
  );
}

async function stabilize(page) {
  await page.waitForLoadState("domcontentloaded");
  await page.waitForLoadState("networkidle").catch(() => {});
  await page.waitForTimeout(250);
}

function commonMasks(page) {
  return [
    // toasts / transient status
    page.locator(".toast, #toast, .snackbar, .notice, .saving, .saved"),

    // timestamps / "last updated"
    page.locator("[data-testid='timestamp'], [data-testid='last-updated']"),
    page.locator(".last-updated, .updated-at, .audit-timestamp, .timestamp"),

    // dynamic pills/badges/counters
    page.locator(".pill, .badge, .chip, .counter, .count, .status-pill"),
  ];
}

// Extra masks specifically for audit-like feeds that change constantly
function auditFeedMasks(page) {
  return [
    // common IDs/classes for audit containers
    page.locator("#audit, #audit-log, #tab-audit, .audit, .audit-log, .activity, .activity-log"),

    // tables/lists inside audit (often the changing part)
    page.locator("#tab-audit table, #tab-audit tbody, #tab-audit ul, #tab-audit ol"),
    page.locator("#audit table, #audit tbody, #audit ul, #audit ol"),
  ];
}

const SNAP_ADMIN = {
  fullPage: false,
  animations: "disabled",
  maxDiffPixelRatio: 0.01, // strict
};

const SNAP_MANAGER = {
  fullPage: false,
  animations: "disabled",
  maxDiffPixelRatio: 0.03, // tolerant (manager reflects live state)
};

const SNAP_FANZONE = {
  fullPage: false,
  animations: "disabled",
  maxDiffPixelRatio: 0.02,
};

async function clickTab(page, label) {
  const btn = page.getByRole("button", { name: label });
  if (await btn.count()) {
    await btn.first().click();
    return;
  }

  const byText = page.locator("button, .tabbtn, .tab").filter({ hasText: label });
  if (await byText.count()) {
    await byText.first().click();
    return;
  }

  throw new Error(`Could not find tab "${label}" button`);
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
      await expect(page).toHaveScreenshot(`admin-${safe}.png`, {
        ...SNAP_ADMIN,
        mask: commonMasks(page),
      });
    }
  });
});

test.describe("Visual - MANAGER core panes", () => {
  test("Manager core panes stable viewport snapshots", async ({ page }) => {
    await page.goto(MANAGER_URL, { waitUntil: "domcontentloaded" });
    await stabilize(page);

    const tabs = ["Ops", "Leads", "AI Queue", "Monitoring", "Audit"];

    for (const t of tabs) {
      await clickTab(page, t);
      await stabilize(page);
      await page.waitForTimeout(300);

      const safe = t.replace(/\s+/g, "_").toLowerCase();

      // ✅ For Audit, mask the dynamic feed area so layout regressions still get caught
      const masks =
        t.toLowerCase() === "audit"
          ? [...commonMasks(page), ...auditFeedMasks(page)]
          : commonMasks(page);

      await expect(page).toHaveScreenshot(`manager-${safe}.png`, {
        ...SNAP_MANAGER,
        mask: masks,
      });
    }
  });
});

test.describe("Visual - FANZONE page", () => {
  test("Fan Zone stable viewport snapshot", async ({ page }) => {
    await page.goto(FANZONE_URL, { waitUntil: "domcontentloaded" });
    await stabilize(page);

    await expect(page).toHaveScreenshot("fanzone.png", {
      ...SNAP_FANZONE,
      mask: commonMasks(page),
    });
  });
});