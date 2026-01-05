const { test, expect } = require("@playwright/test");

// Quarantine visual tests: they only run when explicitly enabled.
test.skip(
  process.env.RUN_VISUAL !== "1",
  "Visual tests are quarantined; set RUN_VISUAL=1 to run."
);

const ADMIN_URL =
  process.env.ADMIN_URL ||
  "http://127.0.0.1:5050/admin?key=REPLACE_ADMIN_KEY";

const MANAGER_URL =
  process.env.MANAGER_URL ||
  "http://127.0.0.1:5050/manager?key=REPLACE_MANAGER_KEY";

const FANZONE_URL =
  process.env.FANZONE_URL ||
  "http://127.0.0.1:5050/admin/fanzone?key=REPLACE_ADMIN_KEY";

const VIEWPORT = { width: 1280, height: 900 };

const SNAP_OPTS = {
  fullPage: false,
  maxDiffPixelRatio: 0.02,
  animations: "disabled",
  caret: "hide",
  scale: "css",
};

async function waitForFonts(page) {
  await page.evaluate(async () => {
    if (document.fonts && document.fonts.ready) await document.fonts.ready;
  });
}

async function settle(page) {
  await page.evaluate(() => new Promise(requestAnimationFrame));
}

async function stabilize(page) {
  await page.setViewportSize(VIEWPORT);
  await page.emulateMedia({ reducedMotion: "reduce" });

  await page.addStyleTag({
    content: `
      *, *::before, *::after {
        transition: none !important;
        animation: none !important;
        caret-color: transparent !important;
      }

      html { scroll-behavior: auto !important; overflow-y: scroll !important; }
      body { overflow-y: scroll !important; }

      /* Freeze typical dynamic panes so content length doesn't reflow the page */
      #tab-leads, #tab-aiq, #tab-monitoring, #tab-ops, #tab-policies, #tab-menu, #tab-rules, #tab-ai-settings {
        max-height: 650px !important;
        overflow: auto !important;
      }

      /* Tables/rows jitter: constrain them */
      table, .table, .rows, .log, .audit-log {
        max-height: 520px !important;
        overflow: auto !important;
      }
    `,
  });

  await page.waitForLoadState("domcontentloaded");
  await page.waitForLoadState("networkidle").catch(() => {});
  await waitForFonts(page);

  await page.evaluate(() => window.scrollTo(0, 0));
  await settle(page);
}

async function clickTab(page, tabName) {
  const btn = page.getByRole("button", {
    name: new RegExp(`^${tabName}$`, "i"),
  });

  if (await btn.count()) {
    await btn.first().click();
    await page.waitForLoadState("networkidle").catch(() => {});
    await settle(page);
    return true;
  }

  const byDataTab = page.locator(`[data-tab="${tabName.toLowerCase()}"]`);
  if (await byDataTab.count()) {
    await byDataTab.first().click();
    await page.waitForLoadState("networkidle").catch(() => {});
    await settle(page);
    return true;
  }

  return false;
}

function commonMasks(page) {
  const selectors = [
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
    ".audit",
    ".audit-log",
    ".log",
    "time",
    "[data-time]",
    "[data-updated]",
  ];
  return selectors.map((s) => page.locator(s));
}

function tabMasks(page, tabLabel) {
  const masks = commonMasks(page);
  const label = (tabLabel || "").toLowerCase();

  if (label.includes("lead")) {
    masks.push(page.locator("#tab-leads table"));
    masks.push(page.locator("#tab-leads .rows"));
  }

  if (label.includes("ai queue") || label.includes("aiq")) {
    masks.push(page.locator("#tab-aiq table"));
    masks.push(page.locator("#tab-aiq .queue"));
  }

  if (label.includes("ops")) {
    masks.push(page.locator("#tab-ops .last-updated"));
    masks.push(page.locator("#tab-ops [data-ts]"));
  }

  return masks;
}

test.describe("Visual - ADMIN tab panes", () => {
  test("Admin tab panes stable viewport snapshots", async ({ page }) => {
    await page.goto(ADMIN_URL, { waitUntil: "domcontentloaded" });
    await stabilize(page);

    // Audit excluded (too dynamic for pixel-perfect CI)
    const tabs = [
      "Ops",
      "Leads",
      "AI Queue",
      "Monitoring",
      "AI Settings",
      "Rules",
      "Menu",
      "Policies",
    ];

    for (const t of tabs) {
      const ok = await clickTab(page, t);
      expect(ok, `Tab not found: ${t}`).toBeTruthy();

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

    // Audit excluded (too dynamic for pixel-perfect CI)
    const tabs = ["Ops", "Leads", "AI Queue", "Monitoring"];

    for (const t of tabs) {
      const ok = await clickTab(page, t);
      expect(ok, `Tab not found: ${t}`).toBeTruthy();

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