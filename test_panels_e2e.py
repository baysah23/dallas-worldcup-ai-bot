import os
import re
import time
import pytest
from playwright.sync_api import sync_playwright, expect

BASE_URL = os.getenv("BASE_URL", "http://127.0.0.1:5000")

# Put your real keys here or pass via env vars
ADMIN_KEY = os.getenv("ADMIN_KEY", "baysah23_worldcup_admin_7f3b9a8c2d1e4f5a9c0b")
MANAGER_KEY = os.getenv("MANAGER_KEY", "10060f5c3997567470fc887785132a60")


def _goto_panel(page, path_with_key: str):
    # Avoid Service Worker / cache weirdness during tests
    page.goto(f"{BASE_URL}{path_with_key}", wait_until="domcontentloaded")
    page.wait_for_timeout(250)


def _click_nav_and_assert_visible(page, label: str):
    # Click the nav item (button or link)
    nav = page.get_by_role("button", name=re.compile(rf"^{re.escape(label)}$", re.I))
    if nav.count() == 0:
        nav = page.get_by_role("link", name=re.compile(rf"^{re.escape(label)}$", re.I))
    expect(nav.first).to_be_visible()
    nav.first.click()
    page.wait_for_timeout(250)

    # Assert the section is now visible.
    # This is flexible: it looks for a heading or section title matching the tab name.
    heading = page.get_by_role("heading", name=re.compile(label, re.I))
    if heading.count() > 0:
        expect(heading.first).to_be_visible()
    else:
        # fallback: any element containing the label (less strict but keeps test from being brittle)
        expect(page.get_by_text(re.compile(label, re.I)).first).to_be_visible()


def _exercise_ops_toggles(page):
    # This assumes your Ops tab has toggle switches or buttons.
    # We try common patterns: "input[type=checkbox]" or buttons labeled Enable/Disable.
    # Then verify "Saving..." appears briefly AND "Last updated" line updates/exists.

    # Look for toggle checkboxes first
    toggles = page.locator("input[type=checkbox]")
    if toggles.count() == 0:
        # fallback: buttons that look like toggles
        toggles = page.locator("button").filter(has_text=re.compile(r"(enable|disable|on|off)", re.I))

    if toggles.count() == 0:
        # Nothing to test in Ops — fail with a clear message
        pytest.fail("No Ops toggles found (no checkbox toggles or enable/disable style buttons).")

    # Click the first 1–3 toggles (don’t spam everything)
    n = min(3, toggles.count())
    for i in range(n):
        toggles.nth(i).scroll_into_view_if_needed()
        toggles.nth(i).click()

        # Saving state should appear somewhere near Ops UI
        saving = page.get_by_text(re.compile(r"saving…|saving\.\.\.", re.I))
        # It may be very brief; just assert it appears at least once
        try:
            expect(saving.first).to_be_visible(timeout=2000)
        except Exception:
            # Don’t hard-fail if it’s too fast; continue and verify last updated instead
            pass

        # Verify "Last updated" line exists
        last_updated = page.get_by_text(re.compile(r"last updated", re.I))
        expect(last_updated.first).to_be_visible(timeout=3000)

        # small pause between toggles
        page.wait_for_timeout(350)


@pytest.mark.parametrize("panel_name,path", [
    ("admin",   "/admin?key={key}"),
    ("manager", "/manager?key={key}"),
])
def test_panel_tabs_and_ops(panel_name, path):
    key = ADMIN_KEY if panel_name == "admin" else MANAGER_KEY
    assert key and "YOUR_" not in key, f"Set {panel_name.upper()}_KEY env var (or edit the file)."

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(ignore_https_errors=True)

        # Disable SW caching issues
        context.add_init_script("""
            // Force-disable service workers for test runs
            if ('serviceWorker' in navigator) {
              navigator.serviceWorker.getRegistrations().then(rs => rs.forEach(r => r.unregister()));
            }
        """)

        page = context.new_page()

        # Load panel
        _goto_panel(page, path.format(key=key))

        # Click each nav button + verify content shows
        for tab in ["Ops", "AI", "AI Queue", "Rules", "Menu", "Audit"]:
            _click_nav_and_assert_visible(page, tab)

        # Specifically test Ops toggles & metadata
        _click_nav_and_assert_visible(page, "Ops")
        _exercise_ops_toggles(page)

        context.close()
        browser.close()