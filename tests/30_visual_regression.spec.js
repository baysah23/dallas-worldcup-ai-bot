const { test, expect } = require("@playwright/test");

const ADMIN_URL = process.env.ADMIN_URL;
const MANAGER_URL = process.env.MANAGER_URL;
const FANZONE_URL = process.env.FANZONE_URL;

function must(v, name) {
  if (!v) throw new Error(`Missing env var: ${name}`);
  return v;
}

async function stabilize(page) {
  // Freeze animations/transitions
  await page.addStyleTag({
    content: `
      *, *::before, *::after {
        transition: none !important;
        animation: none !important;
        caret-color: transparent !important;
      }

      /* ---- VISUAL SNAPSHOT STABILIZERS (IMPORTANT) ----
         These prevent dynamic content from changing screenshot size. */

      /* Fixed pane heights so AI Queue / others don’t change height with data */
      #tab-ops, #tab-leads, #tab-aiq, #tab-monitor, #tab-audit, #tab-configure,
      #tab-ai, #tab-rules, #tab-menu, #tab-policies {
        box-sizing: border-box !important;
        min-height: 650px !important;
        height: 650px !important;
        overflow: hidden !important;
      }

      /* Fan Zone: lock viewport-sized body so it doesn't grow/shrink */
      body {
        min-height: 900px !important;
        height: 900px !important;
        overflow: hidden !important;
      }
    `,
  });

  // Always start at top
  await page.evaluate(() => window.scrollTo(0, 0));
  await page.waitForTimeout(250);
}

async function clickTab(page, label) {
  const btn = page.getByRole("button", { name: label }).or(page.getByRole("link", { name: label }));
  await expect(btn).toBeVisible();
  await btn.first().click();
  await page.waitForTimeout(250);
}

function commonMasks(page) {
  return [
    // Dynamic toasts/logs/timestamps (mask if present; safe if not)
    page.locator(".toast, .toasts, #toast, #toasts").first(),
    page.locator("#tab-audit, #audit, .audit, .audit-log").first(),
    page.getByText(/last updated/i).first(),
    page.getByText(/updated by/i).first(),
  ];
}

const SNAP_OPTS = {
  maxDiffPixelRatio: 0.03, // keep strict but realistic
};

test("Visual - ADMIN tab panes", async ({ page }) => {
  await page.goto(must(ADMIN_URL, "ADMIN_URL"), { waitUntil: "domcontentloaded" });
  await stabilize(page);

  const tabs = [
    { label: "Ops", pane: "#tab-ops" },
    { label: "Leads", pane: "#tab-leads" },
    { label: "AI Queue", pane: "#tab-aiq" },
    { label: "Monitoring", pane: "#tab-monitor" },
    { label: "Audit", pane: "#tab-audit" },
    { label: "Configure", pane: "#tab-configure" },
    { label: "AI Settings", pane: "#tab-ai" },
    { label: "Rules", pane: "#tab-rules" },
    { label: "Menu", pane: "#tab-menu" },
    { label: "Policies", pane: "#tab-policies" },
  ];

  for (const t of tabs) {
    const btn = page.getByRole("button", { name: t.label }).or(page.getByRole("link", { name: t.label }));
    if (!(await btn.count())) continue;

    await clickTab(page, t.label);
    await stabilize(page);

    const pane = page.locator(t.pane).first();
    await expect(pane).toBeVisible();

    await expect(pane).toHaveScreenshot(
      `admin-${t.label.replace(/\s+/g, "_").toLowerCase()}.png`,
      { ...SNAP_OPTS, mask: commonMasks(page) }
    );
  }
});

test("Visual - MANAGER core panes", async ({ page }) => {
  await page.goto(must(MANAGER_URL, "MANAGER_URL"), { waitUntil: "domcontentloaded" });
  await stabilize(page);

  const tabs = [
    { label: "Ops", pane: "#tab-ops" },
    { label: "Leads", pane: "#tab-leads" },
    { label: "AI Queue", pane: "#tab-aiq" },
    { label: "Monitoring", pane: "#tab-monitor" },
    { label: "Audit", pane: "#tab-audit" },
  ];

  for (const t of tabs) {
    const btn = page.getByRole("button", { name: t.label }).or(page.getByRole("link", { name: t.label }));
    if (!(await btn.count())) continue;

    await clickTab(page, t.label);
    await stabilize(page);

    const pane = page.locator(t.pane).first();
    await expect(pane).toBeVisible();

    await expect(pane).toHaveScreenshot(
      `manager-${t.label.replace(/\s+/g, "_").toLowerCase()}.png`,
      { ...SNAP_OPTS, mask: commonMasks(page) }
    );
  }
});

test("Visual - FANZONE page", async ({ page }) => {
  await page.goto(must(FANZONE_URL, "FANZONE_URL"), { waitUntil: "domcontentloaded" });
  await stabilize(page);

  const target = page.locator("body").first();
  await expect(target).toBeVisible();

  await expect(target).toHaveScreenshot("fanzone.png", {
    maxDiffPixelRatio: 0.03,
    mask: commonMasks(page),
  });
});