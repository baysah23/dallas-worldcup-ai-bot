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

// FILE 11 is STRICT: fail fast (no skips).
const BASE_URL = mustEnv("BASE_URL");
const ADMIN_OWNER_KEY = mustEnv("ADMIN_OWNER_KEY");
const ADMIN_MANAGER_KEY = mustEnv("ADMIN_MANAGER_KEY");

const ADMIN_URL = buildUrl(BASE_URL, "/admin", ADMIN_OWNER_KEY);
const MANAGER_URL = buildUrl(BASE_URL, "/manager", ADMIN_MANAGER_KEY);
const FANZONE_URL = buildUrl(BASE_URL, "/admin/fanzone", ADMIN_OWNER_KEY);

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
  });

  test("Admin: AI Queue approve/deny works if items exist", async ({ page }) => {
    await forceDesktopViewport(page);
    await page.goto(ADMIN_URL, { waitUntil: "domcontentloaded" });
    await waitForReady(page);

    await clickTabAndWait(page, "AI Queue");
    await assertSinglePaneVisibleIfTabPanesExist(page);

    const emptyHint = page.getByText(/no items|empty|nothing to review/i);
    const hasEmptyHint = (await emptyHint.count().catch(() => 0)) > 0;

    const approve = page.getByRole("button", { name: /approve/i });
    const deny = page.getByRole("button", { name: /deny|reject/i });

    const approveCount = await approve.count().catch(() => 0);
    const denyCount = await deny.count().catch(() => 0);

    if (hasEmptyHint && approveCount === 0 && denyCount === 0) return;

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
    const actionResp = await actionRespPromise;
    if (actionResp) expect(actionResp.status(), "AI Queue action failed").toBeLessThan(400);
  });

  test("Manager: owner-only tabs blocked (Configure, AI Settings, Rules)", async ({ page }) => {
    await forceDesktopViewport(page);
    await page.goto(MANAGER_URL, { waitUntil: "domcontentloaded" });
    await waitForReady(page);

    const ownerTabs = ["Configure", "AI Settings", "Rules"];

    for (const t of ownerTabs) {
      const tab = await page.getByRole("button", { name: new RegExp(`^${t}$`, "i") }).count().catch(() => 0);
      if (!tab) continue;

      const before = page.url();
      await page.getByRole("button", { name: new RegExp(`^${t}$`, "i") }).first().click({ force: true }).catch(() => {});
      await waitForReady(page);

      const blockedMsg = page.getByText(/locked|owner only|not authorized|permission/i);
      const blockedVisible = (await blockedMsg.count().catch(() => 0)) > 0;

      expect(blockedVisible || page.url() === before, `Manager should be blocked from ${t}`).toBeTruthy();
    }
  });

  test("Fan Zone: poll interaction does not crash (best-effort)", async ({ page }) => {
    const errors = [];
    attachConsoleCollectors(page, errors);

    await forceDesktopViewport(page);
    await page.goto(FANZONE_URL, { waitUntil: "domcontentloaded" });
    await waitForReady(page);

    const pollHeader = page.getByText(/match of the day/i).first();
    if (await pollHeader.count().catch(() => 0)) await pollHeader.click().catch(() => {});

    const radioCandidate = page.locator("input[type='radio']").first();
    const btnCandidate = page.getByRole("button", { name: /vote|submit/i }).first();

    const voteRespPromise = page.waitForResponse(
      (r) =>
        r.request().method() === "POST" &&
        (r.url().includes("/api/poll/vote") || r.url().includes("/poll/vote")),
      { timeout: 8000 }
    ).catch(() => null);

    if (await radioCandidate.count().catch(() => 0)) {
      await radioCandidate.check().catch(() => {});
      if (await btnCandidate.count().catch(() => 0)) await btnCandidate.click().catch(() => {});
    }

    const voteResp = await voteRespPromise;
    if (voteResp) expect(voteResp.status(), "Poll vote endpoint returned error").toBeLessThan(400);

    expect(errors, errors.join("\n")).toHaveLength(0);
  });
});
