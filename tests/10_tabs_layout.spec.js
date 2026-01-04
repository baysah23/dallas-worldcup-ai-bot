const { test, expect } = require("@playwright/test");

const ADMIN_URL = process.env.ADMIN_URL;
const MANAGER_URL = process.env.MANAGER_URL;
const FANZONE_URL = process.env.FANZONE_URL;

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

async function assertPaneVisible(page, paneSelector) {
  const pane = page.locator(paneSelector).first();
  await expect(pane).toBeVisible();
}

async function assertNoDuplicatePanes(page) {
  const panes = page.locator('[id^="tab-"]');
  const visibleCount = await panes.evaluateAll((els) => {
    const isVisible = (el) => {
      const r = el.getBoundingClientRect();
      const s = window.getComputedStyle(el);
      return (
        s.display !== "none" &&
        s.visibility !== "hidden" &&
        !el.classList.contains("hidden") &&
        r.width > 100 &&
        r.height > 100
      );
    };
    return els.filter(isVisible).length;
  });

  if (visibleCount > 1) {
    throw new Error(`Duplicate tab panes detected (${visibleCount}).`);
  }
}

const ADMIN_TABS = [
  { label: "Ops", pane: "#tab-ops" },
  { label: "Leads", pane: "#tab-leads" },
  { label: "AI Queue", pane: "#tab-aiq" },
  { label: "Monitoring", pane: "#tab-monitor" },
  { label: "Audit", pane: "#tab-audit" },
  { label: "Configure", pane: "#tab-configure" },
  { label: "AI Settings", pane: "#tab-ai" },
  { label: "Rules", pane: "#tab-rules" },
  { label: "Menu", pane: "#tab-menu" },
  { label: "Policies", pane: "#tab-policies" },
];

const MANAGER_TABS_REQUIRED = [
  { label: "Ops", pane: "#tab-ops" },
  { label: "Leads", pane: "#tab-leads" },
  { label: "AI Queue", pane: "#tab-aiq" },
  { label: "Monitoring", pane: "#tab-monitor" },
  { label: "Audit", pane: "#tab-audit" },
];

test("ADMIN: tabs click-through + correct pane + no duplicates", async ({ page }) => {
  const errs = await failOnConsole(page);

  await page.goto(must(ADMIN_URL, "ADMIN_URL"), { waitUntil: "domcontentloaded" });
  await expect(page.locator("body")).toBeVisible();

  for (const t of ADMIN_TABS) {
    const tabBtn = page.getByRole("button", { name: t.label }).or(page.getByRole("link", { name: t.label }));
    if (await tabBtn.count()) {
      await tabBtn.first().click();
      await page.waitForTimeout(150);
      await assertPaneVisible(page, t.pane);
      await assertNoDuplicatePanes(page);
    }
  }

  if (errs.length) throw new Error(errs.join("\n"));
});

test("MANAGER: required tabs work + Policies is locked", async ({ page }) => {
  const errs = await failOnConsole(page);

  await page.goto(must(MANAGER_URL, "MANAGER_URL"), { waitUntil: "domcontentloaded" });
  await expect(page.locator("body")).toBeVisible();

  // Required tabs must work
  for (const t of MANAGER_TABS_REQUIRED) {
    const tabBtn = page.getByRole("button", { name: t.label }).or(page.getByRole("link", { name: t.label }));
    if (!(await tabBtn.count())) throw new Error(`Manager missing required tab: ${t.label}`);

    await tabBtn.first().click();
    await page.waitForTimeout(150);
    await assertPaneVisible(page, t.pane);
    await assertNoDuplicatePanes(page);
  }

  // Policies must be locked for manager (your rule)
  const policiesBtn = page.getByRole("button", { name: "Policies" }).or(page.getByRole("link", { name: "Policies" }));
  if (await policiesBtn.count()) {
    await policiesBtn.first().click();
    await page.waitForTimeout(200);

    // Must NOT show policies pane
    const policiesPane = page.locator("#tab-policies").first();
    if (await policiesPane.count()) {
      const cls = (await policiesPane.getAttribute("class")) || "";
      if (!cls.includes("hidden")) {
        throw new Error(`Manager Policies pane became visible — expected locked.`);
      }
    }

    // Must remain on Ops pane visible
    await expect(page.locator("#tab-ops").first()).toBeVisible();
  }

  if (errs.length) throw new Error(errs.join("\n"));
});

test("FAN ZONE: loads cleanly (route)", async ({ page }) => {
  await page.goto(must(FANZONE_URL, "FANZONE_URL"), { waitUntil: "domcontentloaded" });
  await expect(page.locator("body")).toBeVisible();
});