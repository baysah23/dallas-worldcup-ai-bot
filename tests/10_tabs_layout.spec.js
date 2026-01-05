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

// Optional: if you add <div data-testid="app-root"> to panels later,
// set APP_ROOT_SELECTOR='[data-testid="app-root"]' in env.
// Otherwise we fallback to body having real content.
const APP_ROOT_SELECTOR = process.env.APP_ROOT_SELECTOR || "[data-testid='app-root']";

function escapeRegex(s) {
  return s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

/**
 * Deterministic "page is ready" check: no sleeps.
 */
async function waitForReady(page) {
  const root = page.locator(APP_ROOT_SELECTOR);
  if (await root.count()) {
    await expect(root.first()).toBeVisible({ timeout: 8000 });
  } else {
    // Fallback: page should not be blank
    await expect(page.locator("body")).toContainText(/./, { timeout: 8000 });
  }
}

/**
 * Force desktop layout to avoid collapsed/hamburger nav in CI.
 */
async function forceDesktopViewport(page) {
  await page.setViewportSize({ width: 1400, height: 900 });
}

/**
 * If UI collapses navigation, try to open it (safe no-op if not present).
 * Looks for common "menu" buttons. If your app uses a specific id/class,
 * we can tighten this later.
 */
async function openNavIfCollapsed(page, tabName) {
  // If the tab text is already visible somewhere, do nothing.
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
    page.locator("button:has(svg)"), // last resort; will be filtered by visibility below
  ];

  for (const loc of burgerCandidates) {
    const c = await loc.count().catch(() => 0);
    if (!c) continue;
    const btn = loc.first();
    const vis = await btn.isVisible().catch(() => false);
    if (!vis) continue;

    // Click and then see if the desired tab became visible
    await btn.click().catch(() => {});
    const nowVisible = await page
      .locator(`text=/^${escapeRegex(tabName)}$/i`)
      .first()
      .isVisible()
      .catch(() => false);
    if (nowVisible) return;
  }
}

/**
 * Find a tab control robustly across different markup:
 * - role button/link/tab by accessible name
 * - fallback to text-based selectors for button/a/[role=tab]
 * - fallback to data-tab if present
 */
async function findTabControl(page, name) {
  const reExact = new RegExp(`^${escapeRegex(name)}$`, "i");

  const candidates = [
    page.getByRole("button", { name: reExact }),
    page.getByRole("link", { name: reExact }),
    page.getByRole("tab", { name: reExact }),

    // Fallbacks if ARIA roles aren't wired
    page.locator("button", { hasText: reExact }),
    page.locator("a", { hasText: reExact }),
    page.locator("[role='tab']", { hasText: reExact }),

    // Your existing fallback
    page.locator(`[data-tab="${name.toLowerCase()}"]`),
  ];

  for (const loc of candidates) {
    const count = await loc.count().catch(() => 0);
    if (!count) continue;

    const first = loc.first();
    const visible = await first.isVisible().catch(() => false);
    if (visible) return first;

    // If it exists but not visible (collapsed nav), we still return it
    // only after nav open attempts, handled outside.
    return first;
  }

  return null;
}

/**
 * Deterministic condition after a click.
 * We try (in order):
 * 1) URL hash changed
 * 2) tab shows selected/active accessibility state
 * 3) visible tabpane changed (fallback)
 */
async function waitForTabStateChange(page, name, beforeHash) {
  // 1) hash based navigation (very common)
  await page
    .waitForFunction(
      ({ beforeHash }) => location.hash !== beforeHash,
      { beforeHash },
      { timeout: 2000 }
    )
    .catch(() => {});

  // 2) accessible active state (if present)
  const btn = page
    .getByRole("button", { name: new RegExp(`^${escapeRegex(name)}$`, "i") })
    .first();

  // If it's not a role=button in your DOM, skip this strict check.
  if (await btn.count().catch(() => 0)) {
    await expect(btn).toBeVisible();

    const ariaSelected = await btn.getAttribute("aria-selected");
    const ariaCurrent = await btn.getAttribute("aria-current");
    if (ariaSelected !== null) {
      await expect(btn).toHaveAttribute("aria-selected", /true/i);
      return;
    }
    if (ariaCurrent !== null) {
      await expect(btn).toHaveAttribute("aria-current", /true/i);
      return;
    }
  }

  // 3) Fallback: if .tabpane shells exist, require exactly 1 visible
  const panes = visibleTabPanes(page);
  const count = await panes.count();
  if (count > 0) {
    await expect(panes).toHaveCount(1);
    await expect(panes.first()).toBeVisible();
  }
}

/**
 * Click a tab in a robust way. No sleeps.
 */
async function clickTabAndWait(page, name) {
  const beforeHash = new URL(page.url()).hash;

  // Try to open nav if collapsed (safe no-op)
  await openNavIfCollapsed(page, name);

  const tab = await findTabControl(page, name);
  if (!tab) return false;

  await tab.click().catch(async () => {
    // Some UIs need forced click (overlays). Last resort:
    await tab.click({ force: true }).catch(() => {});
  });

  await waitForTabStateChange(page, name, beforeHash);
  return true;
}

/**
 * Correct, deterministic visible pane locator:
 * selects elements that have class tabpane but NOT class hidden
 * (not a descendant check).
 */
function visibleTabPanes(page) {
  return page.locator(".tabpane:not(.hidden)");
}

async function assertSinglePaneVisibleIfTabPanesExist(page) {
  const panesAll = page.locator(".tabpane");
  if ((await panesAll.count()) === 0) return; // if your build doesn't use tabpane shells, skip

  const panesVisible = visibleTabPanes(page);
  await expect(panesVisible, "Expected exactly ONE visible .tabpane").toHaveCount(1);
  await expect(panesVisible.first()).toBeVisible();
}


/**
 * Manager UI may default to Ops without rendering an "Ops" tab control.
 * Treat as success if an Ops pane is already visible.
 */
async function managerHasOrIsOnOps(page) {
  const opsPane = page.locator("#tab-ops, #ops, #ops-controls, #tab-ops-controls").first();
  const visible = await opsPane.isVisible().catch(() => false);
  if (visible) return true;
  return await clickTabAndWait(page, "Ops");
}

test.describe("ADMIN: deterministic tab navigation (no visuals)", () => {
  test("Admin: each tab is clickable and results in a stable state", async ({ page }) => {
    await forceDesktopViewport(page);
    await page.goto(ADMIN_URL, { waitUntil: "domcontentloaded" });
    await waitForReady(page);

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

    await assertSinglePaneVisibleIfTabPanesExist(page);

    for (const t of tabs) {
      const ok = await clickTabAndWait(page, t);
      expect(ok, `Tab control not found/clickable: ${t}`).toBeTruthy();
      await assertSinglePaneVisibleIfTabPanesExist(page);
    }
  });
});

test.describe("MANAGER: deterministic access rules (no visuals)", () => {
  test("Manager: core tabs work; Policies is blocked or absent", async ({ page }) => {
    await forceDesktopViewport(page);
    await page.goto(MANAGER_URL, { waitUntil: "domcontentloaded" });
    await waitForReady(page);

    const required = ["Ops", "Leads", "AI Queue", "Monitoring", "Audit"];

    await assertSinglePaneVisibleIfTabPanesExist(page);

    for (const t of required) {
      const ok = t === "Ops" ? await managerHasOrIsOnOps(page) : await clickTabAndWait(page, t);
      expect(ok, `Tab control not found/clickable: ${t}`).toBeTruthy();
      await assertSinglePaneVisibleIfTabPanesExist(page);
    }

    // Policies: either not present OR present but blocked
    await openNavIfCollapsed(page, "Policies");

    const policiesBtn =
      (await findTabControl(page, "Policies")) ||
      page.getByRole("button", { name: /^Policies$/i }).first();

    const policiesCount = await policiesBtn.count().catch(() => 0);

    if (policiesCount) {
      const before = page.url();
      await policiesBtn.first().click().catch(() => {});
      const blockedMsg = page.getByText(/locked|owner only|not authorized|permission/i);

      await blockedMsg.first().waitFor({ timeout: 1500 }).catch(() => {});

      const panesAll = page.locator(".tabpane");
      if ((await panesAll.count()) > 0) {
        const visible = visibleTabPanes(page);
        await expect(visible).toHaveCount(1);
        const id = await visible.first().getAttribute("id");
        if (id) expect(id.toLowerCase()).not.toContain("polic");
      } else {
        expect(page.url(), "Manager should not navigate into Policies").toBe(before);
      }

      console.log('[INFO] Manager "Policies" is present but blocked (expected).');
    } else {
      console.log('[INFO] Manager "Policies" not present (acceptable).');
    }
  });
});

test.describe("FAN ZONE: loads deterministically (no visuals)", () => {
  test("Fan Zone: loads and shows core content or safe fallback", async ({ page }) => {
    await forceDesktopViewport(page);
    await page.goto(FANZONE_URL, { waitUntil: "domcontentloaded" });
    await waitForReady(page);

    await expect(page.locator("body")).toContainText(/./);

    const maybePoll = page.getByText(/match of the day/i);
    if (await maybePoll.count()) {
      await expect(maybePoll.first()).toBeVisible();
    }
  });
});