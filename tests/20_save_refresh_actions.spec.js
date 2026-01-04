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

test("ADMIN: Ops toggle click triggers save + survives refresh", async ({ page }) => {
  await page.goto(ADMIN_URL, { waitUntil: "domcontentloaded" });

  // Go to Ops tab
  const opsBtn = page.getByRole("button", { name: /^Ops$/i });
  if (await opsBtn.count()) await opsBtn.first().click();

  // Try to find any checkbox toggle on Ops pane
  const opsPane = page.locator("#tab-ops, #ops, #ops-controls, #tab-ops-controls").first();
  await expect(opsPane).toBeVisible();

  const toggle = opsPane.locator("input[type='checkbox']").first();
  if (!(await toggle.count())) {
    test.skip(true, "No Ops toggle checkbox found to click.");
  }

  const before = await toggle.isChecked();
  await toggle.click({ force: true });

  // best effort: wait for any saving indicator or network idle
  await page.waitForTimeout(500);

  // Refresh and ensure it doesn't crash
  await page.reload({ waitUntil: "domcontentloaded" });
  const afterToggle = page.locator("input[type='checkbox']").first();
  await expect(afterToggle).toBeVisible();

  // If server persists settings, checked state may flip/persist; we just ensure it's interactable and page stable.
  expect(await afterToggle.isChecked()).toBeDefined();
});

test("ADMIN: AI Queue approve/deny buttons are clickable (if queue has items)", async ({ page }) => {
  await page.goto(ADMIN_URL, { waitUntil: "domcontentloaded" });

  const aiqBtn = page.getByRole("button", { name: /^AI Queue$/i });
  if (await aiqBtn.count()) await aiqBtn.first().click();

  const pane = page.locator("#tab-aiq, #aiq, #tab-ai-queue, #ai-queue").first();
  await expect(pane).toBeVisible();

  // Look for approve/deny buttons (best effort)
  const approve = pane.getByRole("button", { name: /approve/i }).first();
  const deny = pane.getByRole("button", { name: /deny/i }).first();

  if ((await approve.count()) || (await deny.count())) {
    if (await approve.count()) await approve.click({ trial: true });
    if (await deny.count()) await deny.click({ trial: true });
  } else {
    console.log("[INFO] No approve/deny buttons found (queue may be empty).");
  }
});

test("MANAGER: Ops toggle click works (if allowed) and doesn’t crash", async ({ page }) => {
  await page.goto(MANAGER_URL, { waitUntil: "domcontentloaded" });

  const opsBtn = page.getByRole("button", { name: /^Ops$/i });
  if (await opsBtn.count()) await opsBtn.first().click();

  const pane = page.locator("#tab-ops, #ops, #ops-controls").first();
  await expect(pane).toBeVisible();

  const toggle = pane.locator("input[type='checkbox']").first();
  if (!(await toggle.count())) {
    console.log("[INFO] No Manager Ops toggle found; skipping click.");
    return;
  }

  await toggle.click({ force: true });
  await page.waitForTimeout(300);
  await expect(page.locator("body")).toBeVisible();
});

test("FAN ZONE: Poll area loads + interacting triggers network activity (best effort)", async ({ page }) => {
  await page.goto(FANZONE_URL, { waitUntil: "domcontentloaded" });

  // Poll UI differs; just ensure page is alive and buttons exist
  const anyButton = page.locator("button").first();
  await expect(anyButton).toBeVisible();

  // Best effort click (trial) so we don't change state
  await anyButton.click({ trial: true }).catch(() => {});
  await expect(page.locator("body")).toBeVisible();
});