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

const VIEWPORT = { width: 1280, height: 720 };

// A bit of tolerance so tiny raster differences don't fail CI
const SNAP_OPTS = {
  fullPage: false,
  maxDiffPixelRatio: 0.02,
  animations: "disabled",
  caret: "hide",
};

async function stabilize(page) {
  await page.setViewportSize(VIEWPORT);

  // Disable animations/transitions at runtime
  await page.addStyleTag({
    content: `
      *, *::before, *::after {
        transition: none !important;
        animation: none !important;
        caret-color: transparent !important;
      }
      html { scroll-behavior: auto !important; }
    `,
  });

  // Let layout settle
  await page.waitForLoadState("domcontentloaded");
  await page.waitForTimeout(250);

  // Scroll to top for consistent screenshots
  await page.evaluate(() => window.scrollTo(0, 0));
  await page.waitForTimeout(150);
}

// Common dynamic areas across pages (mask them if present)
function commonMasks(page) {
  const candidates = [
    "#toast",
    ".toast",
    ".toasts",
    "#notifications",
    ".notifications",
    ".timestamp",
    ".time",
    "[data-ts]",
    "#last-updated",
    ".last-updated",
    "#audit",
    "#tab-audit",
    ".audit",
    ".audit-log",
    ".log",
    "#leads",
    "#tab-leads",
    ".leads",
    "table",
  ];
  return candidates.map((sel) => page.locator(sel)).filter(Boolean);
}

// Extra masks per tab (when labels are known)
function tabMasks(page, tabLabel) {
  const base = commonMasks(page);

  const label = (tabLabel || "").toLowerCase();
  if (label.includes("lead")) {
    base.push(page.locator("#tab-leads table"));
    base.push(page.locator("#tab-leads .table"));
    base.push(page.locator("#tab-leads .rows"));
  }
  if (label.includes("audit")) {
    base.push(page.locator("#tab-audit"));
    base.push(page.locator("#tab-audit table"));
    base.push(page.locator("#tab-audit .log"));
  }
  if (label.includes("ops")) {
    base.push(page.locator("#tab-ops .last-updated"));
    base.push(page.locator("#tab-ops [data-ts]"));
  }
  if (label.includes("ai queue") || label.includes("aiq")) {
    base.push(page.locator("#tab-aiq table"));
    base.push(page.locator("#tab-aiq .queue"));
  }
  return base;
}

async function clickTab(page, tabName) {
  const btn = page.getByRole("button", { name: new RegExp(`^${tabName}$`, "i") });
  if (await btn.count()) {
    await btn.first().click();
    await page.waitForTimeout(200);
    return;
  }
  const byDataTab = page.locator(`[data-tab="${tabName.toLowerCase()}"]`);
  if (await byDataTab.count()) {
    await byDataTab.first().click();
    await page.waitForTimeout(200);
    return;
  }
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
        ...SNAP_OPTS,
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

      const safe = t.replace(/\s+/g, "_").toLowerCase();

      await expect(page).toHaveScreenshot(`manager-${safe}.png`, {
        ...SNAP_OPTS,
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
      ...SNAP_OPTS,
      mask: commonMasks(page),
    });
  });
});