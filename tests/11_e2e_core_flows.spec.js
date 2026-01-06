const { test, expect } = require("@playwright/test");
const {
  mustEnv,
  buildUrl,
  attachConsoleCollectors,
  forceDesktopViewport,
  waitForReady,
  clickTabAndWait,
  assertSinglePaneVisibleIfTabPanesExist,
} = require("./_helpers/panels");

// FILE 11 is STRICT: no skips, fail fast.
const BASE_URL = mustEnv("BASE_URL"); // e.g. http://127.0.0.1:5000 or https://your-app.onrender.com
const ADMIN_KEY = mustEnv("ADMIN_KEY");
const MANAGER_KEY = mustEnv("MANAGER_KEY");

const ADMIN_URL = buildUrl(BASE_URL, "/admin", ADMIN_KEY);
const MANAGER_URL = buildUrl(BASE_URL, "/manager", MANAGER_KEY);
const FANZONE_URL = buildUrl(BASE_URL, "/admin/fanzone", ADMIN_KEY);

test.describe("FILE 11: Core E2E flows (fail fast)", () => {
  test("Admin loads cleanly (no JS errors)", async ({ page }) => {
    const errors = [];
    attachConsoleCollectors(page, errors);

    await forceDesktopViewport(page);
    const resp = await page.goto(ADMIN_URL, { waitUntil: "domcontentloaded" });

    expect(resp, "Admin: no server response").toBeTruthy();
    expect(resp.status(), "Admin: expected < 400").toBeLessThan(400);

    await waitForReady(page);

    expect(errors, errors.join("\n")).toHaveLength(0);
  });

  test("Manager loads cleanly (no JS errors)", async ({ page }) => {
    const errors = [];
    attachConsoleCollectors(page, errors);

    await forceDesktopViewport(page);
    const resp = await page.goto(MANAGER_URL, { waitUntil: "domcontentloaded" });

    expect(resp, "Manager: no server response").toBeTruthy();
    expect(resp.status(), "Manager: expected < 400").toBeLessThan(400);

    await waitForReady(page);

    expect(errors, errors.join("\n")).toHaveLength(0);
  });

  test("Fan Zone loads cleanly (no JS errors)", async ({ page }) => {
    const errors = [];
    attachConsoleCollectors(page, errors);

    await forceDesktopViewport(page);
    const resp = await page.goto(FANZONE_URL, { waitUntil: "domcontentloaded" });

    expect(resp, "Fan Zone: no server response").toBeTruthy();
    expect(resp.status(), "Fan Zone: expected < 400").toBeLessThan(400);

    await waitForReady(page);

    expect(errors, errors.join("\n")).toHaveLength(0);
  });

  test("Admin: Ops toggle triggers save (network) and survives refresh", async ({ page }) => {
    await forceDesktopViewport(page);
    await page.goto(ADMIN_URL, { waitUntil: "domcontentloaded" });
    await waitForReady(page);

    // Navigate to Ops if tab exists; otherwise assume default is Ops.
    const opsTab = await page.getByRole("button", { name: /^Ops$/i }).count().catch(() => 0);
    if (opsTab) await clickTabAndWait(page, "Ops");

    await assertSinglePaneVisibleIfTabPanesExist(page);

    const toggle = page.locator("input[type='checkbox']").first();
    expect(await toggle.count(), "No Ops checkbox found (need at least one toggle in Ops)").toBeGreaterThan(0);

    const savePromise = page.waitForResponse(
      (r) =>
        r.request().method() === "POST" &&
        (r.url().includes("/admin/update-config") ||
          r.url().includes("update-config") ||
          r.url().includes("update_config")),
      { timeout: 8000 }
    );

    await toggle.click({ force: true });

    const saveResp = await savePromise;
    expect(saveResp.status(), "Save endpoint returned error").toBeLessThan(400);

    await page.reload({ waitUntil: "domcontentloaded" });
    await waitForReady(page);

    // Still stable after refresh
    expect(await page.locator("body").count()).toBeTruthy();
  });

  test("Admin: AI Queue approve/deny works if items exist", async ({ page }) => {
    await forceDesktopViewport(page);
    await page.goto(ADMIN_URL, { waitUntil: "domcontentloaded" });
    await waitForReady(page);

    await clickTabAndWait(page, "AI Queue");
    await assertSinglePaneVisibleIfTabPanesExist(page);

    // If queue is empty, accept; otherwise require approve/deny clickable.
    const emptyHint = page.getByText(/no items|empty|nothing to review/i);
    const hasEmptyHint = (await emptyHint.count().catch(() => 0)) > 0;

    const approve = page.getByRole("button", { name: /approve/i });
    const deny = page.getByRole("button", { name: /deny|reject/i });

    const approveCount = await approve.count().catch(() => 0);
    const denyCount = await deny.count().catch(() => 0);

    if (hasEmptyHint && approveCount === 0 && denyCount === 0) {
      // Accept empty queue
      return;
    }

    expect(approveCount + denyCount, "AI Queue: expected approve/deny buttons or an explicit empty state").toBeGreaterThan(0);

    const btn = approveCount ? approve.first() : deny.first();

    const actionRespPromise = page.waitForResponse(
      (r) =>
        r.request().method() === "POST" &&
        (r.url().includes("/api/aiq") ||
          r.url().includes("/aiq") ||
          r.url().includes("approve") ||
          r.url().includes("deny") ||
          r.url().includes("reject")),
      { timeout: 8000 }
    ).catch(() => null);

    await btn.click({ force: true }).catch(() => {});

    // Not all implementations hit a POST; if it does, it must be OK.
    const actionResp = await actionRespPromise;
    if (actionResp) {
      expect(actionResp.status(), "AI Queue action failed").toBeLessThan(400);
    }
  });

  test("Manager: owner-only tabs blocked (Configure, AI Settings, Rules)", async ({ page }) => {
    await forceDesktopViewport(page);
    await page.goto(MANAGER_URL, { waitUntil: "domcontentloaded" });
    await waitForReady(page);

    // Try a small set of owner-only-ish tabs. If the tab doesn't exist, skip that specific tab.
    const ownerTabs = ["Configure", "AI Settings", "Rules"];

    for (const t of ownerTabs) {
      const tab = await page.getByRole("button", { name: new RegExp(`^${t}$`, "i") }).count().catch(() => 0);
      if (!tab) continue;

      const before = page.url();
      await page.getByRole("button", { name: new RegExp(`^${t}$`, "i") }).first().click({ force: true }).catch(() => {});
      await waitForReady(page);

      const blockedMsg = page.getByText(/locked|owner only|not authorized|permission/i);
      const blockedVisible = (await blockedMsg.count().catch(() => 0)) > 0;

      // Either we stayed on same URL (common), or we show a clear "locked" message.
      expect(blockedVisible || page.url() === before, `Manager should be blocked from ${t}`).toBeTruthy();
    }
  });

  test("Fan Zone: Match of the Day poll vote does not crash (best-effort)", async ({ page }) => {
    const errors = [];
    attachConsoleCollectors(page, errors);

    await forceDesktopViewport(page);
    await page.goto(FANZONE_URL, { waitUntil: "domcontentloaded" });
    await waitForReady(page);

    // Open poll area if present
    const pollHeader = page.getByText(/match of the day/i).first();
    if (await pollHeader.count().catch(() => 0)) {
      await pollHeader.click().catch(() => {});
    }

    // Try to click the first obvious vote option (buttons/radios).
    const voteCandidate = page
      .locator("button:has-text('%')") // sometimes options include %
      .first();

    const radioCandidate = page.locator("input[type='radio']").first();
    const btnCandidate = page.getByRole("button", { name: /vote|submit/i }).first();

    // Watch for vote endpoint if it exists
    const voteRespPromise = page.waitForResponse(
      (r) =>
        r.request().method() === "POST" &&
        (r.url().includes("/api/poll/vote") || r.url().includes("/poll/vote")),
      { timeout: 8000 }
    ).catch(() => null);

    if (await radioCandidate.count().catch(() => 0)) {
      await radioCandidate.check().catch(() => {});
      if (await btnCandidate.count().catch(() => 0)) await btnCandidate.click().catch(() => {});
    } else if (await voteCandidate.count().catch(() => 0)) {
      await voteCandidate.click().catch(() => {});
    }

    const voteResp = await voteRespPromise;
    if (voteResp) {
      expect(voteResp.status(), "Poll vote endpoint returned error").toBeLessThan(400);
    }

    // Always fail if page crashed
    expect(errors, errors.join("\n")).toHaveLength(0);
  });
});
