// Capture the real external page, without changing its content or styling.
const fs = require('node:fs');
const path = require('node:path');
const { createHash } = require('node:crypto');
const { chromium } = require(process.env.PLAYWRIGHT_MODULE || 'playwright');
const { launchChromium } = require('./playwright_browser.cjs');

async function main() {
  const [base, output, pool] = process.argv.slice(2);
  if (!base || !output || !pool) throw new Error('Usage: capture_prometheus.cjs BASE OUTPUT POOL');
  const browser = await launchChromium(chromium);
  try {
    const page = await browser.newPage({ viewport: { width: 1440, height: 620 } });
    await page.goto(new URL('/targets', base).href, { waitUntil: 'domcontentloaded', timeout: 25000 });
    const picker = page.getByPlaceholder('Select scrape pool');
    await picker.fill(pool);
    await page.getByRole('option', { name: pool, exact: true }).click();
    await page.getByRole('button').filter({ hasText: pool }).waitFor();
    await page.getByRole('link', { name: /http.*9104/ }).waitFor();
    fs.mkdirSync(path.dirname(output), { recursive: true });
    await page.screenshot({ path: output });
    const bytes = fs.readFileSync(output);
    fs.writeFileSync(output + '.json', JSON.stringify({
      source_url: page.url(), observed_at: new Date().toISOString(),
      sha256: createHash('sha256').update(bytes).digest('hex'), bytes: bytes.length,
      viewport: { width: 1440, height: 620 }, pool,
      browser: process.env.CHROME_EXECUTABLE ? 'Configured executable / Playwright' : 'Playwright Chromium',
    }, null, 2) + '\n');
    console.log(output);
  } finally { await browser.close(); }
}
main().catch(error => { console.error(error.message); process.exitCode = 1; });
