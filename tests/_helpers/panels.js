// tests/_helpers/panels.js
const { expect } = require("@playwright/test");

/**
 * Build a panel URL like:
 *   buildUrl("http://127.0.0.1:5050", "/admin", "KEY")
 * => http://127.0.0.1:5050/admin?key=KEY
 */
function buildUrl(baseUrl, path, key) {
  const base = String(baseUrl || "").replace(/\/+$/, "");
  const p = String(path || "").startsWith("/") ? String(path) : `/${path}`;
  const url = new URL(base + p);
  if (key) url.searchParams.set("key", String(key));
  return url.toString();
}

/**
 * Collect JS errors (console.error + pageerror).
 * Use: const errors=[]; attachConsoleCollectors(page, errors);
 */
function attachConsoleCollectors(page, errorsArray) {
  page.on("console", (msg) => {
    if (msg.type() === "error") errorsArray.push(`[console.error] ${msg.text()}`);
  });
  page.on("pageerror", (err) => {
    errorsArray.push(`[pageerror] ${err && err.message ? err.message : String(err)}`);
  });
}

async function forceDesktopViewport(page) {
  await page.setViewportSize({ width: 1280, height: 800 });
}

/**
 * Deterministic ready check: no sleeps.
 * Wait for DOM + basic body content.
 */
async function waitForReady(page) {
  await page.waitForLoadState("domcontentloaded");
  await page.waitForFunction(() => document && document.body && document.body.innerText.length >= 1);
}

/**
 * Click a tab button by visible text; return true if found/clicked.
 */
async function clickTabAndWait(page, tabName) {
  const tab = page.getByRole("button", { name: new RegExp(`^${tabName}$`, "i") }).first();
  if ((await tab.count().catch(() => 0)) === 0) return false;
  await tab.click({ force: true });
  await waitForReady(page);
  return true;
}

/**
 * If your UI has panes (ex: <section data-pane="ops">...</section>)
 * ensure only one is visible.
 * If none exist, do nothing (pass).
 */
async function assertSinglePaneVisibleIfTabPanesExist(page) {
  const panes = page.locator("[data-pane]");
  const count = await panes.count().catch(() => 0);
  if (count <= 1) return;

  let visible = 0;
  for (let i = 0; i < count; i++) {
    if (await panes.nth(i).isVisible().catch(() => false)) visible++;
  }
  expect(visible, "Expected exactly one [data-pane] visible").toBe(1);
}

module.exports = {
  buildUrl,
  attachConsoleCollectors,
  forceDesktopViewport,
  waitForReady,
  clickTabAndWait,
  assertSinglePaneVisibleIfTabPanesExist,
};