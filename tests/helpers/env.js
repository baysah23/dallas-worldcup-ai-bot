// tests/helpers/env.js
function mustEnv(name) {
  const v = process.env[name];
  if (!v) throw new Error(`${name} is required`);
  return v;
}

function getUrls() {
  return {
    ADMIN_URL: mustEnv("ADMIN_URL"),
    MANAGER_URL: mustEnv("MANAGER_URL"),
    // Optional: add if you have a fan-facing URL for poll/lead tests
    FAN_URL: process.env.FAN_URL || "",
  };
}

module.exports = { mustEnv, getUrls };