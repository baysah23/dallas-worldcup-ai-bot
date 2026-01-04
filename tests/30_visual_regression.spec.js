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

function auditFeedMasks(page) {
  return [
    page.locator("#audit, #audit-log, #tab-audit, .audit, .audit-log, .activity, .activity-log"),
    page.locator("#tab-audit table, #tab-audit tbody, #tab-audit ul, #tab-audit ol"),
    page.locator("#audit table, #audit tbody, #audit ul, #audit ol"),
  ];
}

// ✅ NEW: Leads is highly dynamic (lists, sorting, counts, timestamps)
// Mask the main leads content region so visuals test the layout/chrome, not live data.
function leadsFeedMasks(page) {
  return [
    // common tab container IDs/classes
    page.locator("#tab-leads, #leads, .leads, .leads-pane, .leads-panel"),

    // typical table/card containers inside leads
    page.locator("#tab-leads table, #tab-leads tbody, #tab-leads .table, #tab-leads .cards"),
    page.locator("#leads table, #leads tbody, #leads .table, #leads .cards"),

    // common “rows” / “cards” patterns
    page.locator(".lead-row, .lead-card, .lead-item"),
  ];
}

const SNAP_ADMIN = {
  fullPage: false,
  animations: "disabled",
  maxDiffPixelRatio: 0.01,
};

const SNAP_MANAGER = {
  fullPage: false,
  animations: "disabled",
  maxDiffPixelRatio: 0.03,
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

function tabMasks(page, tabLabel) {
  const t = tabLabel.toLowerCase();

  if (t === "audit") return [...commonMasks(page), ...auditFeedMasks(page)];
  if (t === "leads") return [...commonMasks(page), ...leadsFeedMasks(page)];

  return commonMasks(page);
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
        mask: tabMasks(page, t),
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

      await expect(page).toHaveScreenshot(`manager-${safe}.png`, {
        ...SNAP_MANAGER,
        mask: tabMasks(page, t),
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