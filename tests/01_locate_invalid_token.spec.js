const { test, expect } = require("@playwright/test");

const ADMIN_URL = process.env.ADMIN_URL;
const MANAGER_URL = process.env.MANAGER_URL;

function must(v, name) {
  if (!v) throw new Error(`Missing env var: ${name}`);
  return v;
}

async function installEarlyErrorTrap(page) {
  // This runs BEFORE any page scripts.
  await page.addInitScript(() => {
    window.__earlyErrors = [];

    function push(kind, payload) {
      try {
        window.__earlyErrors.push({ kind, ...payload, ts: Date.now() });
      } catch (e) {}
    }

    // window.onerror catches many syntax errors with file/line/col
    window.onerror = function (message, source, lineno, colno, error) {
      push("window.onerror", {
        message: String(message),
        source: source || "",
        lineno: lineno || 0,
        colno: colno || 0,
        stack: error && error.stack ? String(error.stack) : "",
      });
      return false;
    };

    // Capture resource/script errors too
    window.addEventListener(
      "error",
      (event) => {
        const target = event && event.target;
        const isResourceError =
          target && (target.tagName === "SCRIPT" || target.tagName === "LINK" || target.tagName === "IMG");

        if (isResourceError) {
          push("resource-error", {
            tag: target.tagName,
            src: target.src || target.href || "",
          });
        } else {
          // JS runtime errors (sometimes includes filename/line)
          push("event-error", {
            message: event.message || "error event",
            filename: event.filename || "",
            lineno: event.lineno || 0,
            colno: event.colno || 0,
            stack: event.error && event.error.stack ? String(event.error.stack) : "",
          });
        }
      },
      true
    );

    // Unhandled promise rejections (often show hidden failures)
    window.addEventListener("unhandledrejection", (event) => {
      push("unhandledrejection", {
        reason: event && event.reason ? String(event.reason) : "unhandledrejection",
      });
    });
  });
}

async function runAndReport(label, page, url) {
  await installEarlyErrorTrap(page);

  const consoleErrors = [];
  page.on("console", (msg) => {
    if (msg.type() === "error") consoleErrors.push(msg.text());
  });

  await page.goto(url, { waitUntil: "domcontentloaded" });
  await expect(page.locator("body")).toBeVisible();

  // Pull early errors from inside the browser
  const early = await page.evaluate(() => window.__earlyErrors || []);

  console.log(`\n========== ${label} ERROR REPORT ==========\n`);

  if (early.length) {
    console.log("EARLY ERRORS (best signal):");
    console.log(JSON.stringify(early, null, 2));
    console.log();
  } else {
    console.log("EARLY ERRORS: none captured");
    console.log();
  }

  if (consoleErrors.length) {
    console.log("CONSOLE.ERROR:");
    console.log(consoleErrors.join("\n\n"));
    console.log();
  } else {
    console.log("CONSOLE.ERROR: none");
    console.log();
  }

  console.log(`========== END ${label} ==========\n`);

  // Don’t fail yet; this is diagnostic
}

test("Locate Invalid Token — ADMIN", async ({ page }) => {
  await runAndReport("ADMIN", page, must(ADMIN_URL, "ADMIN_URL"));
});

test("Locate Invalid Token — MANAGER", async ({ page }) => {
  await runAndReport("MANAGER", page, must(MANAGER_URL, "MANAGER_URL"));
});