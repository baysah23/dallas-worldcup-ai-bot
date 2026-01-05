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
 * Click a tab in a robust way.
 * We do NOT sleep; instead we wait for a deterministic "state change".
 */
async function clickTabAndWait(page, name) {
  const beforeHash = new URL(page.url()).hash;

  // Primary: accessible role button with exact label (case-insensitive)
  const btn = page.getByRole("button", { name: new RegExp(`^${escapeRegex(name)}$`, "i") });
  if (await btn.count()) {
    await btn.first().click();

    // Wait for *something* to change deterministically:
    // - hash change (if your app uses it)
    // - or active state toggles (aria-selected / aria-current)
    // - or pane visibility changes
    await waitForTabStateChange(page, name, beforeHash);
    return true;
  }

  // Fallback: data-tab attribute if present
  const dt = page.locator(`[data-tab="${name.toLowerCase()}"]`);
  if (await dt.count()) {
    await dt.first().click();
    await waitForTabStateChange(page, name, beforeHash);
    return true;
  }

  return false;
}

function escapeRegex(s) {
  return s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

/**
 * Deterministic condition after a click.
 * We try (in order):
 * 1) URL hash changed OR matches tab name
 * 2) tab shows selected/active accessibility state
 * 3) visible tabpane changed (fallback)
 */
async function waitForTabStateChange(page, name, beforeHash) {
  // 1) hash based navigation (very common in your app)
  // If your app uses #menu, #leads etc, this will be stable.
  await page.waitForFunction(
    ({ beforeHash }) => location.hash !== beforeHash,
    { beforeHash },
    { timeout: 2000 }
  ).catch(() => {});

  // 2) accessible active state (if present)
  const btn = page.getByRole("button", { name: new RegExp(`^${escapeRegex(name)}$`, "i") }).first();
  await expect(btn).toBeVisible();

  // If your markup uses aria-selected/current, this becomes a strong assertion.
  // If not, these checks are skipped.
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

  // 3) Fallback: visible pane count should be exactly 1 (if you use .tabpane shells)
  // and it should be visible.
  const panes = visibleTabPanes(page);
  const count = await panes.count();
  if (count > 0) {
    await expect(panes).toHaveCount(1);
    await expect(panes.first()).toBeVisible();
  }
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
  if ((await panesAll.count()) === 0) return; // if your build doesn't use tabpane shells, skip this check

  const panesVisible = visibleTabPanes(page);
  await expect(panesVisible, "Expected exactly ONE visible .tabpane").toHaveCount(1);
  await expect(panesVisible.first()).toBeVisible();
}

test.describe("ADMIN: deterministic tab navigation (no visuals)", () => {
  test("Admin: each tab is clickable and results in a stable state", async ({ page }) => {
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

    // baseline: if your UI uses tabpanes, there should be exactly 1 visible
    await assertSinglePaneVisibleIfTabPanesExist(page);

    for (const t of tabs) {
      const ok = await clickTabAndWait(page, t);
      expect(ok, `Tab button not found: ${t}`).toBeTruthy();

      // After click: still exactly one visible pane (if panes exist)
      await assertSinglePaneVisibleIfTabPanesExist(page);
    }
  });
});

test.describe("MANAGER: deterministic access rules (no visuals)", () => {
  test("Manager: core tabs work; Policies is blocked or absent", async ({ page }) => {
    await page.goto(MANAGER_URL, { waitUntil: "domcontentloaded" });
    await waitForReady(page);

    const required = ["Ops", "Leads", "AI Queue", "Monitoring", "Audit"];

    await assertSinglePaneVisibleIfTabPanesExist(page);

    for (const t of required) {
      const ok = await clickTabAndWait(page, t);
      expect(ok, `Tab button not found: ${t}`).toBeTruthy();
      await assertSinglePaneVisibleIfTabPanesExist(page);
    }

    // Policies: either not present OR present but blocked (does not show policies pane)
    const policiesFound = await page
      .getByRole("button", { name: /^Policies$/i })
      .count();

    if (policiesFound) {
      // Click it
      const before = page.url();
      await page.getByRole("button", { name: /^Policies$/i }).first().click();

      // Deterministic block signals:
      // - a "locked/not authorized" message, OR
      // - a bounce back to Ops (URL changes or stays same but pane doesn't become policies)
      const blockedMsg = page.getByText(/locked|owner only|not authorized|permission/i);

      await blockedMsg
        .first()
        .waitFor({ timeout: 1500 })
        .catch(() => {});

      // If a message appears, that's a pass. If not, still require we didn't end up on a policies pane.
      const panesAll = page.locator(".tabpane");
      if ((await panesAll.count()) > 0) {
        const visible = visibleTabPanes(page);
        await expect(visible).toHaveCount(1);
        const id = await visible.first().getAttribute("id");
        if (id) expect(id.toLowerCase()).not.toContain("polic");
      } else {
        // No panes system: require we didn't navigate away unexpectedly
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
    await page.goto(FANZONE_URL, { waitUntil: "domcontentloaded" });
    await waitForReady(page);

    // Deterministic: page should not be blank
    await expect(page.locator("body")).toContainText(/./);

    // Optional: if the poll header exists, it should be visible.
    // This does NOT require exact layout.
    const maybePoll = page.getByText(/match of the day/i);
    if (await maybePoll.count()) {
      await expect(maybePoll.first()).toBeVisible();
    }
  });
});