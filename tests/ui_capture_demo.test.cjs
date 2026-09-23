const { test } = require('node:test');
const assert = require('node:assert/strict');
const { waitForGraph } = require('../tools/capture_demo.cjs');

const SERIES = ['mysql_global_status_threads_connected', 'mysql_global_variables_max_connections'];

function graphPage({ series = SERIES, width = 1200, height = 550, visible = true } = {}) {
  const checked = [];
  const canvas = {
    async waitFor(options) { assert.equal(options.state, 'visible'); checked.push('canvas'); },
    async evaluate(fn) { return fn({ width, height }); },
  };
  const graph = {
    async waitFor(options) {
      assert.equal(options.timeout, 15000);
      if (!visible) throw new Error('Graph panel is not visible');
    },
    locator(selector) {
      if (selector === 'canvas') return { first: () => canvas };
      assert.equal(selector, 'table.u-legend .u-label');
      return { filter({ hasText }) {
        return { first: () => ({ async waitFor(options) {
          assert.equal(options.state, 'visible');
          // Real Prometheus labels are <th><div class="u-label">metric{...}</div></th>.
          const match = series.find(metric => hasText.test(metric + '{namespace="fcapsule-lab"}'));
          if (!match) throw new Error('Required legend series missing');
          checked.push(match);
        } }) };
      } };
    },
  };
  return { checked, getByRole(role, options) {
    // A regression to getByRole('cell') fails even though the legend exists.
    assert.equal(role, 'tabpanel');
    assert.deepEqual(options, { name: 'Graph', exact: true });
    return graph;
  } };
}

test('waits for both real graph legend entries and a rendered canvas without cell roles', async () => {
  const page = graphPage();
  await waitForGraph(page);
  assert.deepEqual(page.checked, [...SERIES, 'canvas']);
});

test('editor text alone or only one graph series cannot pass capture readiness', async () => {
  for (const series of [[], [SERIES[0]], [SERIES[1]]]) {
    await assert.rejects(waitForGraph(graphPage({ series })), /Required legend series missing/);
  }
});

test('a similarly named series does not substitute for either required gauge', async () => {
  await assert.rejects(waitForGraph(graphPage({ series: [SERIES[0] + '_extra', SERIES[1]] })), /Required legend series missing/);
});

test('hidden panels and zero-size canvases cannot pass readiness', async () => {
  await assert.rejects(waitForGraph(graphPage({ visible: false })), /not visible/);
  for (const size of [{ width: 0 }, { height: 0 }]) {
    await assert.rejects(waitForGraph(graphPage(size)), /no rendered dimensions/);
  }
});
