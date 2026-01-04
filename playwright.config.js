// playwright.config.js
const { defineConfig } = require("@playwright/test");

const isCI = !!process.env.CI;

module.exports = defineConfig({
  testDir: "./tests",
  timeout: 60_000,
  expect: {
    timeout: 10_000,
  },

  // CI hardening
  forbidOnly: isCI,
  retries: isCI ? 1 : 0,
  workers: isCI ? 2 : undefined,

  reporter: [
    ["list"],
    ["html", { outputFolder: "playwright-report", open: "never" }],
  ],
  outputDir: "test-results",

  use: {
    // ✅ lock rendering for stable visual tests
    viewport: { width: 1280, height: 900 },
    deviceScaleFactor: 1,

    // ✅ reduce “tiny diffs”
    locale: "en-US",
    timezoneId: "America/Chicago",

    // Useful artifacts
    trace: "retain-on-failure",
    screenshot: "only-on-failure",
    video: "retain-on-failure",
  },
});