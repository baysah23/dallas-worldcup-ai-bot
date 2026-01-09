// tests/helpers/assert.js
function attachConsoleCollectors(page, errors) {
  page.on("pageerror", (err) => errors.push(`[pageerror] ${err.message}`));
  page.on("console", (msg) => {
    if (msg.type() === "error") errors.push(`[console.error] ${msg.text()}`);
  });
}

module.exports = { attachConsoleCollectors };