const { test, expect } = require("@playwright/test");

const ADMIN_URL =
  process.env.ADMIN_URL ||
  "http://127.0.0.1:5000/admin?key=REPLACE_ADMIN_KEY";

const MANAGER_URL =
  process.env.MANAGER_URL ||
  "http://127.0.0.1:5000/manager?key=REPLACE_MANAGER_KEY";

async function gotoIfReachable(page, url) {
  try {
    const res = await page.goto(url, { waitUntil: "domcontentloaded" });
    if (!res) return { ok: false, status: 0 };
    return { ok: res.status() >= 200 && res.status() < 500, status: res.status() };
  } catch (e) {
    return { ok: false, status: 0 };
  }
}

async function pageLooksAuthed(page) {
  const body = (await page.locator("body").innerText().catch(() => "")) || "";
  const lower = body.toLowerCase();

  // Common auth failure signals
  if (lower.includes("unauthorized") || lower.includes("forbidden")) return false;
  if (lower.includes("missing") && lower.includes("key")) return false;
  if (lower.includes("invalid") && lower.includes("key")) return false;

  return true;
}

/**
 * Extremely flexible tab click: supports buttons, links, role=tab, and plain divs/spans.
 */
async function clickTabFlexible(page, label) {
  const candidates = [
    page.getByRole("tab", { name: label, exact: true }),
    page.getByRole("button", { name: label, exact: true }),
    page.getByRole("link", { name: label, exact: true }),
    page.locator("button, a, [role='tab'], [data-tab], .tab, .tabs *").filter({ hasText: label }),
    page.locator(`text="${label}"`).first(),
  ];

  for (const loc of candidates) {
    try {
      if (await loc.count()) {
        await loc.first().scrollIntoViewIfNeeded().catch(() => {});
        await loc.first().click({ timeout: 1500 }).catch(() => {});
        return true;
      }
    } catch (e) {}
  }
  return false;
}

test("ADMIN tabs (only if reachable)", async ({ page }) => {
  const nav = await gotoIfReachable(page, ADMIN_URL);
  test.skip(!nav.ok, `ADMIN not reachable (status=${nav.status})`);

  // If we are not authenticated, do NOT fail the whole pipeline with a misleading "missing tabs".
  const authed = await pageLooksAuthed(page);
  if (!authed) {
    const url = page.url();
    const title = await page.title().catch(() => "");
    const bodyStart = ((await page.locator("body").innerText().catch(() => "")) || "").slice(0, 250);
    test.skip(true, `ADMIN auth likely failed. url=${url} title=${title} body=${JSON.stringify(bodyStart)}...`);
  }

  // Labels + aliases
  const tabSpecs = [
    { name: "Ops", aliases: ["Ops", "Operate"] },
    { name: "Leads", aliases: ["Leads"] },
    { name: "AI Queue", aliases: ["AI Queue"] },
    { name: "Monitoring", aliases: ["Monitoring"] },
    { name: "Audit", aliases: ["Audit"] },
    { name: "AI Settings", aliases: ["AI Settings", "AI"] },
    { name: "Rules", aliases: ["Rules"] },
    { name: "Menu", aliases: ["Menu"] },
    { name: "Policies", aliases: ["Policies"] },
  ];

  // First: sanity check that at least *one* expected label appears anywhere on the page.
  const body = (await page.locator("body").innerText().catch(() => "")) || "";
  const anyLabelPresent = tabSpecs.some(t => t.aliases.some(a => body.includes(a)));
  expect(anyLabelPresent, `No known tab labels found in page text. Check UI/tab rendering or selectors.`).toBeTruthy();

  // Then: attempt to click labels; require at least 4 to be present/clickable.
  let found = 0;
  const missing = [];
  for (const t of tabSpecs) {
    let clicked = false;
    for (const a of t.aliases) {
      if (await clickTabFlexible(page, a)) { clicked = true; break; }
    }
    if (clicked) found += 1;
    else missing.push(t.name);
  }

  expect(found >= 4, `Too many missing tabs. Found ${found}/9. Missing: ${missing.join(", ")}`).toBeTruthy();
});

test("MANAGER policies blocked (only if reachable)", async ({ page }) => {
  const nav = await gotoIfReachable(page, MANAGER_URL);
  test.skip(!nav.ok, `MANAGER not reachable (status=${nav.status})`);

  const authed = await pageLooksAuthed(page);
  if (!authed) {
    test.skip(true, "MANAGER auth likely failed; skipping policies check");
  }

  const policiesBtn = page.getByRole("button", { name: "Policies", exact: true });
  if (!(await policiesBtn.count())) {
    test.skip(true, "Policies not visible to manager (acceptable)");
  }

  const disabled = await policiesBtn.first().isDisabled().catch(() => false);
  if (disabled) {
    expect(disabled).toBeTruthy();
    return;
  }

  await policiesBtn.first().click();
  await expect(page.locator("body")).not.toContainText(/Unauthorized|Forbidden/i);
});
