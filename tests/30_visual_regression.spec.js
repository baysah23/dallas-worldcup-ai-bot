const { test, expect } = require("@playwright/test");

const ADMIN_URL =
  process.env.ADMIN_URL ||
  "http://127.0.0.1:5000/admin?key=REPLACE_ADMIN_KEY";

const MANAGER_URL =
  process.env.MANAGER_URL ||
  "http://127.0.0.1:5000/manager?key=REPLACE_MANAGER_KEY";

const FANZONE_URL =
  process.env.FANZONE_URL ||
  "http://127.0.0.1:5000/admin/fanzone?key=REPLACE_ADMIN_KEY";

const APP_ROOT_SELECTOR = process.env.APP_ROOT_SELECTOR || "body";

async function waitForReady(page) {
  await page.waitForLoadState("domcontentloaded");
  // networkidle can be unreliable if the app polls; don't hard-fail on it.
  await page.waitForLoadState("networkidle").catch(() => {});
  await expect(page.locator(APP_ROOT_SELECTOR)).toBeVisible();
}

async function assertNoCrashText(page) {
  const bodyText = await page.locator("body").innerText();
  const forbidden = [
    "SyntaxError",
    "Unhandled",
    "Internal Server Error",
    "Traceback",
    "Invalid or unexpected token",
  ];
  for (const f of forbidden) expect(bodyText).not.toContain(f);
}

async function assertNotAuthError(page, contextLabel) {
  const bodyText = (await page.locator("body").innerText()).trim();
  const low = bodyText.toLowerCase();

  const badKeySignals = ["unauthorized", "forbidden", "invalid key", "missing key"];
  const hasSignal = badKeySignals.some((s) => low.includes(s));

  // Many “bad key” responses are extremely short. Catch those too.
  if (hasSignal || bodyText.length < 30) {
    throw new Error(
      [
        `${contextLabel}: Not loading the real UI (likely invalid/placeholder key).`,
        `Body length=${bodyText.length}`,
        `Fix: set real env URLs with real keys: ADMIN_URL, MANAGER_URL, FANZONE_URL`,
      ].join("\n")
    );
  }
}

async function findTabControl(page, tabLabel) {
  const reExact = new RegExp(`^\\s*${tabLabel}\\s*$`, "i");
  const reLoose = new RegExp(tabLabel.replace(/\s+/g, "\\s+"), "i");

  const candidates = [
    page.getByRole("tab", { name: reExact }),
    page.getByRole("button", { name: reExact }),
    page.getByRole("tab", { name: reLoose }),
    page.getByRole("button", { name: reLoose }),
    page.locator("button").filter({ hasText: reLoose }),
    page.locator("[role='tab']").filter({ hasText: reLoose }),
  ];

  for (const loc of candidates) {
    if (await loc.count()) return loc.first();
  }
  return null;
}

async function assertTabDistinct(page, tabLabel) {
  const content =
    (await page.locator("#tab-content").count())
      ? page.locator("#tab-content")
      : page.locator("body");

  const before = (await content.innerText()).trim();

  const tab = await findTabControl(page, tabLabel);
  expect(tab, `Tab not found: ${tabLabel}`).toBeTruthy();

  await tab.click();
  await waitForReady(page);
  await assertNoCrashText(page);

  const after = (await content.innerText()).trim();
  expect(after.length).toBeGreaterThan(20);
  expect(after).not.toEqual(before);
}

async function discoverNavLabels(page) {
  // Try to catch common implementations:
  // - role=tab
  // - buttons inside nav/header/tab bars
  const controls = page.locator("[role='tab'], nav button, header button, .tabs button");
  const n = await controls.count();

  const labels = [];
  for (let i = 0; i < n; i++) {
    const el = controls.nth(i);
    const txt = (await el.innerText().catch(() => "")).trim();
    if (!txt) continue;
    if (!labels.includes(txt)) labels.push(txt);
  }
  return labels;
}

test.describe("FILE 10: CI-safe structure regression (no screenshots)", () => {
  test("ADMIN loads and all primary tabs render distinct content", async ({ page }) => {
    await page.goto(ADMIN_URL, { waitUntil: "domcontentloaded" });
    await waitForReady(page);
    await assertNoCrashText(page);
    await assertNotAuthError(page, "ADMIN");

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
    await assertNotAuthError(page, "MANAGER");

    const labels = await discoverNavLabels(page);

    // Require that the key concepts exist, even if labels differ (e.g., "Operate" vs "Ops")
    const requiredMatchers = [
      /ops|operate/i,
      /leads/i,
      /ai\s*queue|queue/i,
      /monitor/i,
      /audit/i,
      /polic/i,
    ];

    for (const re of requiredMatchers) {
      expect(labels.some((l) => re.test(l)), `Missing Manager tab matching: ${re}`).toBeTruthy();
    }

    // Click through the first match for each concept and ensure content changes
    for (const re of requiredMatchers) {
      const label = labels.find((l) => re.test(l));
      if (label) await assertTabDistinct(page, label);
    }

    // Owner-only tabs should be disabled/locked for managers (if present)
    const lockedMatchers = [/configure/i, /ai\s*settings/i, /^rules$/i, /^menu$/i];
    for (const re of lockedMatchers) {
      const label = labels.find((l) => re.test(l));
      if (!label) continue;

      const el = await findTabControl(page, label);
      if (!el) continue;

      const aria = await el.getAttribute("aria-disabled");
      const disabled = await el.getAttribute("disabled");
      expect(
        aria === "true" || disabled !== null,
        `Expected "${label}" to be locked/disabled for Manager`
      ).toBeTruthy();
    }
  });

  test("FAN ZONE loads and shows interactive poll area (or fallback)", async ({ page }) => {
    await page.goto(FANZONE_URL, { waitUntil: "domcontentloaded" });
    await waitForReady(page);
    await assertNoCrashText(page);
    await assertNotAuthError(page, "FANZONE");

    await expect(page.getByText(/match|poll|vote|support/i)).toBeVisible();
  });
});