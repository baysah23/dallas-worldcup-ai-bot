// tests/helpers/api.js
function buildUrl(base, path, key) {
  const b = base.replace(/\/+$/, "");
  const p = path.startsWith("/") ? path : `/${path}`;
  const u = new URL(b + p);
  if (key) u.searchParams.set("key", key);
  return u.toString();
}

module.exports = { buildUrl };