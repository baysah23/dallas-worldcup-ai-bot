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

async function clickTab(page, tabName) {
  // Prefer button by role; fall back to data-tab attribute
  const byRole = page.getByRole("button", { name: new RegExp(`^${tabName}$`, "i") });
  if (await byRole.count()) {
    await byRole.first().click();
    return;
  }
  const byDataTab = page.locator(`[data-tab="${tabName.toLowerCase()}"]`);
  if (await byDataTab.count()) {
    await byDataTab.first().click();
    return;
  }
  throw new Error(`Tab button not found: ${tabName}`);
}

async function expectPaneVisible(page, selectors) {
  const pane = page.locator(selectors).first();
  await expect(pane).toBeVisible();
}

async function countVisiblePanes(page) {
  // Only count major panes
  const panes = page.locator(".tabpane, [id^='tab-']");
  const n = await panes.count();
  let visible = 0;
  for (let i = 0; i < n; i++) {
    const el = panes.nth(i);
    try {
      if (await el.isVisible()) visible++;
    } catch {}
  }
  return visible;
}

test("ADMIN: tabs click-through + correct pane + no duplicates", async ({ page }) => {
  await page.goto(ADMIN_URL, { waitUntil: "domcontentloaded" });

  const adminTabs = [
    { name: "Ops", pane: "#tab-ops, #tab-ops-controls, #ops, #ops-controls" },
    { name: "Leads", pane: "#tab-leads, #leads, #tab-leads-pane" },
    { name: "AI Queue", pane: "#tab-aiq, #aiq, #tab-ai-queue, #ai-queue" },
    { name: "Monitoring", pane: "#tab-monitoring, #monitoring, #health, #tab-health" },
    { name: "Audit", pane: "#tab-audit, #audit" },
    { name: "AI Settings", pane: "#tab-ai-settings, #ai-settings" },
    { name: "Rules", pane: "#tab-rules, #rules" },
    { name: "Menu", pane: "#tab-menu, #menu" },
    { name: "Policies", pane: "#tab-policies, #policies" },
  ];

  for (const t of adminTabs) {
    await clickTab(page, t.name);
    await page.waitForTimeout(150);
    await expectPaneVisible(page, t.pane);

    // No duplicate panes sanity (allow 1–2 visible because some headers/containers may show)
    const visible = await countVisiblePanes(page);
    expect(visible).toBeLessThanOrEqual(2);
  }
});

test("MANAGER: required tabs work + Policies is locked", async ({ page }) => {
  await page.goto(MANAGER_URL, { waitUntil: "domcontentloaded" });

  const managerTabs = [
    { name: "Ops", pane: "#tab-ops, #ops, #ops-controls" },
    { name: "Leads", pane: "#tab-leads, #leads" },
    { name: "AI Queue", pane: "#tab-aiq, #aiq, #tab-ai-queue, #ai-queue" },
    { name: "Monitoring", pane: "#tab-monitoring, #monitoring, #health, #tab-health" },
    { name: "Audit", pane: "#tab-audit, #audit" },
  ];

  for (const t of managerTabs) {
    await clickTab(page, t.name);
    await page.waitForTimeout(150);
    await expectPaneVisible(page, t.pane);
    const visible = await countVisiblePanes(page);
    expect(visible).toBeLessThanOrEqual(2);
  }

  // Policies should be locked/hidden for Manager (your intended behavior)
  // If the tab exists and is clickable, pane should remain hidden OR bounce back to Ops.
  const policiesBtn = page.getByRole("button", { name: /^Policies$/i });
  if (await policiesBtn.count()) {
    await policiesBtn.first().click();
    await page.waitForTimeout(250);
    const policiesPane = page.locator("#tab-policies, #policies").first();
    if (await policiesPane.count()) {
      const vis = await policiesPane.isVisible().catch(() => false);
      if (vis) {
        throw new Error('Manager "Policies" became visible but should be locked.');
      } else {
        console.log('[INFO] Manager "Policies" pane stayed hidden (may be intended lock/permission).');
      }
    }
  }
});

test("FAN ZONE: loads cleanly (route)", async ({ page }) => {
  await page.goto(FANZONE_URL, { waitUntil: "domcontentloaded" });
  await expect(page.locator("body")).toBeVisible();
});