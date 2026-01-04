const { test, expect } = require("@playwright/test");

const ADMIN_URL = process.env.ADMIN_URL;

function must(v, name) {
  if (!v) throw new Error(`Missing env var: ${name}`);
  return v;
}

function redactKeys(text) {
  // redact key query param if present
  return text.replace(/(\bkey=)[^"&\s]+/g, "$1<redacted>");
}

test("Print the exact HTML line that breaks (line 1742)", async ({ page }) => {
  const url = must(ADMIN_URL, "ADMIN_URL");
  await page.goto(url, { waitUntil: "domcontentloaded" });

  // Get the raw HTML of the page
  const html = await page.content();
  const lines = html.split(/\r?\n/);

  const target = 1742; // 1-based line number from the error
  const start = Math.max(1, target - 4);
  const end = Math.min(lines.length, target + 4);

  console.log("\n===== OFFENDING HTML SNIPPET (around line 1742) =====\n");

  for (let i = start; i <= end; i++) {
    const line = lines[i - 1] ?? "";
    const safe = redactKeys(line);
    const marker = i === target ? "👉" : "  ";
    console.log(`${marker} ${String(i).padStart(5, " ")} | ${safe}`);
  }

  console.log("\n======================================================\n");

  // The test should still pass; it's diagnostic
  expect(true).toBeTruthy();
});