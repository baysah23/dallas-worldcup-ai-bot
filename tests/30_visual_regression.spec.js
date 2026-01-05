const { test, expect } = require("@playwright/test");

const ADMIN_URL =
  process.env.ADMIN_URL ||
  "http://127.0.0.1:5050/admin?key=REPLACE_ADMIN_KEY";

const MANAGER_URL =
  process.env.MANAGER_URL ||
  "http://127.0.0.1:5050/manager?key=REPLACE_MANAGER_KEY";

const FANZONE_URL =
  process.env.FANZONE_URL ||
  "http://127.0.0.1:5050/admin/fanzone?key=REPLACE_ADMIN_KEY";

// Optional: if you later add <div data-testid="app-root">,
// set APP_ROOT_SELECTOR='[data-testid="app-root"]' in env.
const APP_ROOT_SELECTOR = process.env.APP_ROOT_SELECTOR || "body";

/**
 * Deterministic page-ready check (no sleeps).
 */
async function waitForReady(page) {
  await page.waitForLoadState("domcontentloaded");
  // "networkidle" can be unreliable if the app polls; don't hard-fail on it.
  await page.waitForLoadState("networkidle").catch(() => {});
  await expect(page.locator(APP_ROOT_SELECTOR)).toBeVisible();
}

/**
 * Fail fast on obvious crash/error renderings.
 */
async function assertNoCrashText(page) {
  const bodyText = await page.locator("body").innerText();
  const forbidden = [
    "SyntaxError",
    "Unhandled",
    "Internal Server Error",
    "Traceback",
    "Invalid or unexpected token",
  ];

  for (const f of forbidden) {
    expect(bodyText).not.toContain(f);
  }
}

/**
 * Ensures page isn't blank.
 */
async function assertNotBlank(page) {
  const txt = await page.locator(APP_ROOT_SELECTOR).innerText();
  expect(txt.trim().length).toBeGreaterThan(50);
}

/**
 * Clicks a tab by ARIA role first; falls back to data-tab if needed.
 */
async function clickTab(page, tabLabel) {
  const byRole = page.getByRole("tab", { name: tabLabel });
  if (await byRole.count()) {
    await byRole.first().click();
    return true;
  }

  const byButtonRole = page.getByRole("button", { name: tabLabel });
  if (await byButtonRole.count()) {
    await byButtonRole.first().click();
    return true;
  }

  const byDataTab = page.locator(`[data-tab="${tabLabel.toLowerCase()}"]`);
  if (await byDataTab.count()) {
    await byDataTab.first().click();
    return true;
  }

  return false;
}

/**
 * Asserts that switching tabs changes content and produces non-empty output.
 * Uses #tab-content if present; otherwise falls back to body text.
 */
async function assertTabDistinct(page, tabLabel) {
  const content =
    (await page.locator("#tab-content").count())
      ? page.locator("#tab-content")
      : page.locator("body");

  const before = (await content.innerText()).trim();

  const ok = await clickTab(page, tabLabel);
  expect(ok, `Tab not found: ${tabLabel}`).toBeTruthy();

  await waitForReady(page);
  await assertNoCrashText(page);

  const after = (await content.innerText()).trim();
  expect(after.length).toBeGreaterThan(30);

  // Must change something (prevents duplicate panes / no-op clicks)
  expect(after).not.toEqual(before);
}

test.describe("FILE 10: CI-safe structure regression (no screenshots)", () => {
  test("ADMIN loads and all primary tabs render distinct content", async ({ page }) => {
    await page.goto(ADMIN_URL, { waitUntil: "domcontentloaded" });
    await waitForReady(page);
    await assertNoCrashText(page);
    await assertNotBlank(page);

    const adminTabs = [
      "Ops",
      "Leads",
      "AI Queue",
      "Monitoring",
      "Audit",
      "Configure",
      "AI Settings",
      "Rules",
      "Menu",
      "Policies",
    ];

    for (const tab of adminTabs) {
      await assertTabDistinct(page, tab);
    }
  });

  test("MANAGER loads; core tabs distinct; owner tabs locked", async ({ page }) => {
    await page.goto(MANAGER_URL, { waitUntil: "domcontentloaded" });
    await waitForReady(page);
    await assertNoCrashText(page);
    await assertNotBlank(page);

    const managerTabs = ["Ops", "Leads", "AI Queue", "Monitoring", "Audit", "Policies"];
    for (const tab of managerTabs) {
      await assertTabDistinct(page, tab);
    }

    // Owner-only tabs should be disabled/locked for managers
    const locked = ["Configure", "AI Settings", "Rules", "Menu"];
    for (const tab of locked) {
      const el =
        (await page.getByRole("tab", { name: tab }).count())
          ? page.getByRole("tab", { name: tab }).first()
          : page.getByRole("button", { name: tab }).first();

      // If element exists, assert it is disabled or aria-disabled.
      if (await el.count()) {
        const aria = await el.getAttribute("aria-disabled");
        const disabledAttr = await el.getAttribute("disabled");

        expect(
          aria === "true" || disabledAttr !== null,
          `Expected locked tab "${tab}" to be disabled for Manager`
        ).toBeTruthy();
      }
    }
  });

  test("FAN ZONE loads and shows interactive poll area (or fallback)", async ({ page }) => {
    await page.goto(FANZONE_URL, { waitUntil: "domcontentloaded" });
    await waitForReady(page);
    await assertNoCrashText(page);
    await assertNotBlank(page);

    // We don't require exact wording; just ensure poll-ish UI exists.
    await expect(page.getByText(/match|poll|vote|support/i)).toBeVisible();
  });
});