// Real Prometheus pixels only. No HTML, chart or telemetry is synthesized.
const fs = require('node:fs');
const path = require('node:path');
const { createHash } = require('node:crypto');
const { execFileSync } = require('node:child_process');

function graphUrl(base, query) {
  const url = new URL('/query', base);
  if (!['http:', 'https:'].includes(url.protocol)) throw new Error('Require HTTP(S) Prometheus');
  url.searchParams.set('g0.expr', query);
  url.searchParams.set('g0.tab', 'graph');
  url.searchParams.set('g0.range_input', '10m');
  return url.href;
}

async function waitForGraph(page) {
  const graph = page.getByRole('tabpanel', { name: 'Graph', exact: true });
  await graph.waitFor({ state: 'visible', timeout: 15000 });
  // uPlot uses <th> for legend entries, not ARIA cells. Scope to the graph
  // legend so metric names in the expression editor cannot satisfy readiness.
  const labels = graph.locator('table.u-legend .u-label');
  for (const metric of ['mysql_global_status_threads_connected', 'mysql_global_variables_max_connections']) {
    await labels.filter({ hasText: new RegExp('^' + metric + '\\{') }).first().waitFor({ state: 'visible', timeout: 15000 });
  }
  const canvas = graph.locator('canvas').first();
  await canvas.waitFor({ state: 'visible', timeout: 15000 });
  if (!await canvas.evaluate(element => element.width > 0 && element.height > 0)) {
    throw new Error('Prometheus graph canvas has no rendered dimensions');
  }
}

async function capture(base, output, spec) {
  if (fs.existsSync(output) || fs.existsSync(output + '.json')) throw new Error('Capture already exists');
  if (spec.view === 'targets') {
    execFileSync(process.execPath, [path.join(__dirname, 'capture_prometheus.cjs'), base, output, spec.pool], { stdio: 'inherit', timeout: 50000 });
    return;
  }
  if (spec.view !== 'graph' || !spec.query) throw new Error('Unknown capture specification');
  const { chromium } = require(process.env.PLAYWRIGHT_MODULE || 'playwright');
  const browser = await chromium.launch({ executablePath: process.env.CHROME_EXECUTABLE, channel: process.env.CHROME_EXECUTABLE ? undefined : 'chrome', headless: true });
  try {
    const viewport = { width: 1440, height: 980 };
    const page = await browser.newPage({ viewport });
    await page.goto(graphUrl(base, spec.query), { waitUntil: 'domcontentloaded', timeout: 25000 });
    await waitForGraph(page);
    fs.mkdirSync(path.dirname(output), { recursive: true });
    const observedAt = new Date().toISOString();
    await page.screenshot({ path: output, fullPage: true });
    const bytes = fs.readFileSync(output);
    fs.writeFileSync(output + '.json', JSON.stringify({
      source_url: page.url(), observed_at: observedAt, sha256: createHash('sha256').update(bytes).digest('hex'),
      bytes: bytes.length, viewport, query: spec.query, browser: 'Chrome / Playwright',
    }, null, 2) + '\n', { flag: 'wx' });
  } finally { await browser.close(); }
}

module.exports = { graphUrl, waitForGraph };
if (require.main === module) {
  const [base, output, encoded] = process.argv.slice(2);
  Promise.resolve().then(() => capture(base, output, JSON.parse(encoded))).catch(error => {
    console.error(error.message); process.exitCode = 1;
  });
}
