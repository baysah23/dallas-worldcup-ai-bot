// playwright.config.js
const { defineConfig } = require("@playwright/test");

module.exports = defineConfig({
  testDir: "./tests",
  timeout: 60_000,
  expect: { timeout: 10_000 },
  retries: process.env.CI ? 1 : 0,
  use: {
    headless: !!process.env.CI,
    trace: "retain-on-failure",
    screenshot: "only-on-failure",
    video: "retain-on-failure",

    // ✅ LOCKED VIEWPORT (stops 1280 vs 1265 diffs)
    viewport: { width: 1280, height: 900 },

    // ✅ stabilize font rendering
    deviceScaleFactor: 1,
  },
});