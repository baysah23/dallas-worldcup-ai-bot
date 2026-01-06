const { expect } = require("@playwright/test");

function mustEnv(name) {
  const v = process.env[name];
  if (!v) throw new Error(`${name} is required (FILE 11)`);
  return v;
}

function buildUrl(base, path, key) {
  const b = base.replace(/\/+$/, "");
  const p = path.startsWith("/") ? path : `/${path}`;
  const u = new URL(b + p);
  if (key) u.searchParams.set("key", key);
  return u.toString();
}

function attachConsoleCollectors(page, errors) {
  page.on("pageerror", (err) => errors.push(`[pageerror] ${err.message}`));
  page.on("console", (msg) => {
    if (msg.type() === "error") errors.push(`[console.error] ${msg.text()}`);
  });
}

async function forceDesktopViewport(page) {
  await page.setViewportSize({ width: 1400, height: 900 });
}

async function waitForReady(page) {
  const rootSel = process.env.APP_ROOT_SELECTOR || "[data-testid='app-root']";
  const root = page.locator(rootSel);

  if (await root.count()) {
    await expect(root.first()).toBeVisible({ timeout: 12000 });
    return;
  }

  // Fallback: ensure page isn't blank (JS didn't die immediately)
  await expect(page.locator("body")).toContainText(/./, { timeout: 12000 });
}

function escapeRegex(s) {
  return s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
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

  // Prefer hash change when present; otherwise just ensure page stays non-blank.
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
  mustEnv,
  buildUrl,
  attachConsoleCollectors,
  forceDesktopViewport,
  waitForReady,
  clickTabAndWait,
  assertSinglePaneVisibleIfTabPanesExist,
};
