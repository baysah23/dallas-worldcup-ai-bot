// tests/30_visual_regression.spec.js
const { test, expect } = require("@playwright/test");

const ADMIN_URL = process.env.ADMIN_URL;
const MANAGER_URL = process.env.MANAGER_URL;
const FANZONE_URL = process.env.FANZONE_URL;

if (!ADMIN_URL || !MANAGER_URL || !FANZONE_URL) {
  throw new Error("Missing env vars: ADMIN_URL, MANAGER_URL, FANZONE_URL");
}

// Fixed, deterministic snapshot frame
const VIEWPORT = { width: 1280, height: 720 };
const CLIP = { x: 0, y: 0, width: VIEWPORT.width, height: VIEWPORT.height };

// Keep Admin strict; Manager a bit looser (but stable via masks)
const SNAP_ADMIN = { animations: "disabled", maxDiffPixelRatio: 0.02, clip: CLIP };
const SNAP_MANAGER = { animations: "disabled", maxDiffPixelRatio: 0.03, clip: CLIP };
const SNAP_FANZONE = { animations: "disabled", maxDiffPixelRatio: 0.03, clip: CLIP };

test.use({ viewport: VIEWPORT });

async function stabilize(page) {
  // Kill layout jitter
  await page.addStyleTag({
    content: `
      * { caret-color: transparent !important; }
      html { scroll-behavior: auto !important; }
      /* kill transitions/animations even if playwright misses some */
      *, *::before, *::after {
        transition: none !important;
        animation: none !important;
      }
      /* remove scrollbar rendering diffs */
      ::-webkit-scrollbar { width: 0 !important; height: 0 !important; }
      body { overflow: hidden !important; }
    `,
  });

  await page.waitForLoadState("domcontentloaded");
  await page.waitForTimeout(150);
  await page.waitForLoadState("networkidle").catch(() => {});
  await page.waitForTimeout(250);

  // Always start from top-left
  await page.evaluate(() => window.scrollTo(0, 0));
  await page.waitForTimeout(150);
}

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
  throw new Error(`Could not find tab "${label}"`);
}

function commonMasks(page) {
  return [
    // transient UI
    page.locator(".toast, #toast, .snackbar, .notice, .saving, .saved"),
    // time-ish / counters / dynamic chips
    page.locator(".last-updated, .updated-at, .timestamp, .counter, .count, .badge, .chip, .pill"),
  ];
}

// Ops is often stateful (toggle on/off, saved banners, audit line, etc.)
// We mask the *toggle region* so we regression-test layout chrome and headings.
function opsMasks(page) {
  return [
    // common containers where toggles live
    page.locator("#ops-controls, #tab-ops #ops-controls, #tab-ops .toggles, #tab-ops .toggle-grid, #tab-ops .controls"),
    // match day ops presets are dynamic too
    page.locator("#tab-ops .presets, #tab-ops .preset, #tab-ops .preset-row"),
  ];
}

// Leads is highly dynamic; mask the list/table/cards area.
function leadsMasks(page) {
  return [
    page.locator("#tab-leads, #leads, .leads, .leads-pane, .leads-panel"),
    page.locator("#tab-leads table, #tab-leads tbody, #tab-leads .cards, #tab-leads .table"),
    page.locator("#leads table, #leads tbody, #leads .cards, #leads .table"),
    page.locator(".lead-row, .lead-card, .lead-item"),
  ];
}

// Audit feeds can reorder / new lines
function auditMasks(page) {
  return [
    page.locator("#tab-audit, #audit, .audit, .audit-log, .activity, .activity-log"),
    page.locator("#tab-audit table, #tab-audit tbody, #tab-audit ul, #tab-audit ol"),
  ];
}

function masksForTab(page, tabLabel) {
  const t = (tabLabel || "").toLowerCase();

  if (t === "ops") return [...commonMasks(page), ...opsMasks(page)];
  if (t === "leads") return [...commonMasks(page), ...leadsMasks(page)];
  if (t === "audit") return [...commonMasks(page), ...auditMasks(page)];

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
        mask: masksForTab(page, t),
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

      const safe = t.replace(/\s+/g, "_").toLowerCase();
      await expect(page).toHaveScreenshot(`manager-${safe}.png`, {
        ...SNAP_MANAGER,
        mask: masksForTab(page, t),
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