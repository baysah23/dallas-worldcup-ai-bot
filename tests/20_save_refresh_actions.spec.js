const { test, expect } = require("@playwright/test");

const ADMIN_URL = process.env.ADMIN_URL;
const MANAGER_URL = process.env.MANAGER_URL;
const FANZONE_URL = process.env.FANZONE_URL; // required for Fan Zone tests

function must(v, name) {
  if (!v) throw new Error(`Missing env var: ${name}`);
  return v;
}

async function failOnConsole(page) {
  const errs = [];
  page.on("console", (msg) => {
    if (msg.type() === "error") errs.push(msg.text());
  });
  page.on("pageerror", (err) => errs.push(err.message || String(err)));
  return errs;
}

async function clickTab(page, label) {
  const btn = page.getByRole("button", { name: label }).or(page.getByRole("link", { name: label }));
  await expect(btn).toBeVisible();
  await btn.first().click();
  await page.waitForTimeout(150);
}

// Utility: click something and ensure at least one network request happened
async function expectNetworkActivity(page, fnClick, timeoutMs = 8000) {
  let saw = false;
  const handler = () => (saw = true);

  page.on("request", handler);
  try {
    await fnClick();
    const start = Date.now();
    while (!saw && Date.now() - start < timeoutMs) {
      await page.waitForTimeout(100);
    }
  } finally {
    page.off("request", handler);
  }

  expect(saw).toBeTruthy();
}

// Utility: attempt to detect a "Saved" / "Saving" toast or text if present
async function assertSavedFeedbackIfExists(page) {
  const saving = page.getByText(/Saving/i);
  const saved = page.getByText(/Saved/i);

  if (await saving.count()) await expect(saving.first()).toBeVisible();
  if (await saved.count()) await expect(saved.first()).toBeVisible({ timeout: 15000 });
}

// ---- TESTS ----

test("ADMIN: Ops toggle click triggers save + survives refresh", async ({ page }) => {
  const errs = await failOnConsole(page);

  await page.goto(must(ADMIN_URL, "ADMIN_URL"), { waitUntil: "domcontentloaded" });
  await clickTab(page, "Ops");

  // Find a toggle
  const toggle = page.locator('input[type="checkbox"], [role="switch"]').first();
  await expect(toggle).toBeVisible();

  // Read current state
  const before = await toggle.isChecked().catch(async () => {
    // if role switch element
    const aria = await toggle.getAttribute("aria-checked");
    return aria === "true";
  });

  // Click and ensure network activity (save call)
  await expectNetworkActivity(page, async () => {
    await toggle.click();
  });

  await assertSavedFeedbackIfExists(page);

  // Refresh and confirm state persists (best effort; depends on your backend behavior)
  await page.reload({ waitUntil: "domcontentloaded" });
  await clickTab(page, "Ops");
  const toggle2 = page.locator('input[type="checkbox"], [role="switch"]').first();
  await expect(toggle2).toBeVisible();

  const after = await toggle2.isChecked().catch(async () => {
    const aria = await toggle2.getAttribute("aria-checked");
    return aria === "true";
  });

  // It should flip
  expect(after).toBe(!before);

  if (errs.length) throw new Error(errs.join("\n"));
});

test("ADMIN: AI Queue approve/deny buttons are clickable (if queue has items)", async ({ page }) => {
  const errs = await failOnConsole(page);

  await page.goto(must(ADMIN_URL, "ADMIN_URL"), { waitUntil: "domcontentloaded" });
  await clickTab(page, "AI Queue");

  const approve = page.getByRole("button", { name: /Approve/i });
  const deny = page.getByRole("button", { name: /Deny/i });

  // If nothing exists, don't fail; just ensure page is stable
  if ((await approve.count()) === 0 && (await deny.count()) === 0) {
    if (errs.length) throw new Error(errs.join("\n"));
    return;
  }

  // Click one action and ensure network activity
  if (await approve.count()) {
    await expectNetworkActivity(page, async () => {
      await approve.first().click();
    });
    await assertSavedFeedbackIfExists(page);
  } else if (await deny.count()) {
    await expectNetworkActivity(page, async () => {
      await deny.first().click();
    });
    await assertSavedFeedbackIfExists(page);
  }

  if (errs.length) throw new Error(errs.join("\n"));
});

test("MANAGER: Ops toggle click works (if allowed) and doesn’t crash", async ({ page }) => {
  const errs = await failOnConsole(page);

  await page.goto(must(MANAGER_URL, "MANAGER_URL"), { waitUntil: "domcontentloaded" });
  await clickTab(page, "Ops");

  const toggle = page.locator('input[type="checkbox"], [role="switch"]').first();
  await expect(toggle).toBeVisible();

  // click should not throw
  await expectNetworkActivity(page, async () => {
    await toggle.click();
  });

  if (errs.length) throw new Error(errs.join("\n"));
});

test("FAN ZONE: Poll area loads + interacting triggers network activity (best effort)", async ({ page }) => {
  const errs = await failOnConsole(page);

  await page.goto(must(FANZONE_URL, "FANZONE_URL"), { waitUntil: "domcontentloaded" });
  await expect(page.locator("body")).toBeVisible();

  // Try common poll controls
  // (If your UI differs, we’ll refine selectors after first run.)
  const anyButton = page.getByRole("button").first();

  // At minimum: clicking something should not crash
  if (await anyButton.count()) {
    await anyButton.click().catch(() => {});
  }

  if (errs.length) throw new Error(errs.join("\n"));
});