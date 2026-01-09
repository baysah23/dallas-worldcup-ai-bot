// tests/_helpers/panels.js

async function clickTabAndWait(page, tabName) {
  const tab = page.locator(`text=${tabName}`).first();
  if (!(await tab.count())) return false;
  await tab.click();
  await page.waitForLoadState("networkidle");
  return true;
}

async function assertSinglePaneVisibleIfTabPanesExist(page) {
  const panes = page.locator("[data-pane]");
  const count = await panes.count();
  if (count <= 1) return true;

  let visible = 0;
  for (let i = 0; i < count; i++) {
    if (await panes.nth(i).isVisible()) visible++;
  }
  expect(visible).toBe(1);
}

module.exports = {
  clickTabAndWait,
  assertSinglePaneVisibleIfTabPanesExist,
};