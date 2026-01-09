// tests/helpers/auth.js
const { mustEnv } = require("./env");

function getAdminUrl() {
  return mustEnv("ADMIN_URL");
}
function getManagerUrl() {
  return mustEnv("MANAGER_URL");
}

module.exports = { getAdminUrl, getManagerUrl };