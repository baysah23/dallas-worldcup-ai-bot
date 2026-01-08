const { test, expect } = require("@playwright/test");

const ADMIN_URL =
  process.env.ADMIN_URL ||
  "http://127.0.0.1:5000/admin?key=REPLACE_ADMIN_KEY";

const MANAGER_URL =
  process.env.MANAGER_URL ||
  "http://127.0.0.1:5000/manager?key=REPLACE_MANAGER_KEY";

const FANZONE_URL =
  process.env.FANZONE_URL ||
  "http://127.0.0.1:5000/admin/fanzone?key=REPLACE_ADMIN_KEY";

/**
 * Extracts origin + key from a URL like:
 * http://127.0.0.1:5000/admin?key=XYZ
 */
function parseOriginAndKey(url) {
  const u = new URL(url);
  const key = u.searchParams.get("key") || "";
  return { origin: u.origin, key };
}

/**
 * Best-effort JSON parse: returns null if not JSON.
 */
async function tryJson(response) {
  try {
    return await response.json();
  } catch {
    return null;
  }
}

test.describe("FILE 11: Network contract checks (no 500s)", () => {
  test("Poll state endpoint responds (Fan Zone contract)", async ({ request }) => {
    const { origin } = parseOriginAndKey(FANZONE_URL);

    const res = await request.get(`${origin}/api/poll/state`);
    // We require: not a server crash
    expect(res.status(), "Expected /api/poll/state to not 500").not.toBe(500);

    // If it’s 200, it should be valid JSON
    if (res.status() === 200) {
      const js = await tryJson(res);
      expect(js, "Expected JSON body from /api/poll/state").toBeTruthy();
      // Light contract: should at least be an object
      expect(typeof js).toBe("object");
    }
  });

  test("Poll vote endpoint exists (Fan Zone contract)", async ({ request }) => {
    const { origin } = parseOriginAndKey(FANZONE_URL);

    // We don't know your exact payload rules; this is a "no 500" contract check.
    const res = await request.post(`${origin}/api/poll/vote`, {
      data: { choice: "TEST_CHOICE" },
      headers: { "Content-Type": "application/json" },
    });

    expect(res.status(), "Expected /api/poll/vote to not 500").not.toBe(500);

    // If it returns JSON, ensure it's parseable (don’t enforce schema here)
    const js = await tryJson(res);
    if (js !== null) {
      expect(typeof js).toBe("object");
    }
  });

  test("Admin update-config endpoint does not crash (no 500)", async ({ request }) => {
    const { origin, key } = parseOriginAndKey(ADMIN_URL);

    // Contract only: endpoint should not 500.
    // We avoid assuming your exact required fields; 200/400/401/403 are acceptable,
    // but 500 means your save path is broken.
    const res = await request.post(`${origin}/admin/update-config?key=${encodeURIComponent(key)}`, {
      data: { __ci_contract_check: true },
      headers: { "Content-Type": "application/json" },
    });

    expect(res.status(), "Expected /admin/update-config to not 500").not.toBe(500);

    // If it returns JSON, ensure it's parseable
    const js = await tryJson(res);
    if (js !== null) {
      expect(typeof js).toBe("object");
    }
  });

  test("Schedule JSON is reachable (teams/matches data contract)", async ({ request }) => {
    const { origin } = parseOriginAndKey(MANAGER_URL);

    const res = await request.get(`${origin}/schedule.json`);
    expect(res.status(), "Expected /schedule.json to return 200").toBe(200);

    const js = await tryJson(res);
    expect(js, "Expected JSON body from /schedule.json").toBeTruthy();
    expect(Array.isArray(js) || typeof js === "object").toBeTruthy();
  });
});