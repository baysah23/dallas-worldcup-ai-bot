// tests/helpers/ui.js
const { expect } = require("@playwright/test");

function escapeRegex(s) {
  return s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

async function forceDesktopViewport(page) {
  await page.setViewportSize({ width: 1400, height: 900 });
}

/**
 * Deterministic "page ready" check:
 * - If APP_ROOT_SELECTOR exists, wait for it
 * - Else ensure body has some text content
 */
async function waitForReady(page) {
  const rootSel = process.env.APP_ROOT_SELECTOR || "[data-testid='app-root']";
  const root = page.locator(rootSel);

  if (await root.count()) {
    await expect(root.first()).toBeVisible({ timeout: 12000 });
    return;
  }
  await expect(page.locator("body")).toContainText(/./, { timeout: 12000 });
}

async function openNavIfCollapsed(page, tabName) {
  const tabTextVisible = await page
    .locator(`text=/^${escapeRegex(tabName)}$/i`)
    .first()
    .isVisible()
    .catch(() => false);
  if (tabTextVisible) return;

  const burgerCandidates = [
    page.getByRole("button", { name: /menu|navigation|open|tabs/i }),
    page.locator("[aria-label*='menu' i]"),
    page.locator("[data-testid*='menu' i]"),
    page.locator("button:has(svg)"),
  ];

  for (const loc of burgerCandidates) {
    const c = await loc.count().catch(() => 0);
    if (!c) continue;

    const btn = loc.first();
    const vis = await btn.isVisible().catch(() => false);
    if (!vis) continue;

    await btn.click().catch(() => {});
    const nowVisible = await page
      .locator(`text=/^${escapeRegex(tabName)}$/i`)
      .first()
      .isVisible()
      .catch(() => false);
    if (nowVisible) return;
  }
}

async function findTabControl(page, name) {
  const reExact = new RegExp(`^${escapeRegex(name)}$`, "i");
  const candidates = [
    page.getByRole("button", { name: reExact }),
    page.getByRole("link", { name: reExact }),
    page.getByRole("tab", { name: reExact }),
    page.locator("button", { hasText: reExact }),
    page.locator("a", { hasText: reExact }),
    page.locator("[role='tab']", { hasText: reExact }),
    page.locator(`[data-tab="${name.toLowerCase()}"]`),
  ];

  for (const loc of candidates) {
    const count = await loc.count().catch(() => 0);
    if (!count) continue;
    return loc.first();
  }
  return null;
}

async function clickTabAndWait(page, name) {
  await openNavIfCollapsed(page, name);

  const beforeHash = new URL(page.url()).hash;
  const tab = await findTabControl(page, name);
  if (!tab) throw new Error(`Tab control not found: ${name}`);

  await tab.click().catch(async () => {
    await tab.click({ force: true }).catch(() => {});
  });

  // Hash might not always change; we try quickly, then continue.
  await page
    .waitForFunction(
      ({ beforeHash }) => location.hash !== beforeHash,
      { beforeHash },
      { timeout: 2500 }
    )
    .catch(() => {});

  await waitForReady(page);
}

function visibleTabPanes(page) {
  return page.locator(".tabpane:not(.hidden)");
}

async function assertSinglePaneVisibleIfTabPanesExist(page) {
  const panesAll = page.locator(".tabpane");
  if ((await panesAll.count()) === 0) return;

  await expect(visibleTabPanes(page)).toHaveCount(1);
  await expect(visibleTabPanes(page).first()).toBeVisible();
}

module.exports = {
  forceDesktopViewport,
  waitForReady,
  clickTabAndWait,
  assertSinglePaneVisibleIfTabPanesExist,
};