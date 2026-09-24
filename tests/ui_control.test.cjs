const { test } = require('node:test');
const assert = require('node:assert/strict');
const { canStart, selections, groupTracks, sourceUrl } = require('../app/control.js');
const { graphUrl } = require('../tools/capture_demo.cjs');

const healthy = () => ({ active: null, memory: { available_bytes: 2 * 1073741824, node_identity_verified: true }, worker: { reachable: true }, inventory: { reachable: true }, orders: { reachable: true } });

test('start admission covers missing data, all services, active run and host memory', () => {
  assert.equal(canStart(healthy()), true);
  for (const value of [null, {}, { ...healthy(), active: {} }, { ...healthy(), orders: {} },
    { ...healthy(), read_only_preview: true },
    { ...healthy(), external_probe: { active: true } },
    { ...healthy(), memory_error: 'unavailable' }, { ...healthy(), memory: { available_bytes: NaN } },
    { ...healthy(), memory: { available_bytes: 2 * 1073741824, node_identity_verified: false } },
    { ...healthy(), memory: { available_bytes: 100 } }]) assert.equal(canStart(value), false);
});

test('one catalog lists each failure mechanism once, including runner-only scenarios', () => {
  const state = { demos: { example: { scenario: 'mysql-connections', rounds: 1 } }, scenarios: {
    'schema-drift': { title: 'Query failures' }, 'mysql-exporter-scrape-path': { runner_only: true },
  } };
  assert.deepEqual(selections(state).map(item => item.id), ['schema-drift', 'mysql-exporter-scrape-path']);
  assert.equal(selections(state)[1].runner_only, true);
  assert.deepEqual(selections(null), []);
});

test('the demo track stays limited to qualified logs and performance cases', () => {
  const items = selections({ scenarios: {
    'poison-job': { track: 'demo' },
    'timeout-budget': { track: 'development' },
    'cpu-saturation': { track: 'demo' },
    'schema-drift': {},
  } });
  const tracks = groupTracks(items);
  assert.deepEqual(tracks.demos.map(item => item.id), ['poison-job', 'cpu-saturation']);
  assert.deepEqual(tracks.development.map(item => item.id), ['timeout-budget', 'schema-drift']);
});

test('graph links select both metric names without PromQL or dropping the ceiling', () => {
  const url = new URL(sourceUrl('http://prom:9090', 'prometheus_graph'));
  assert.equal(url.pathname, '/query');
  assert.equal(url.searchParams.get('g0.tab'), 'graph');
  assert.match(url.searchParams.get('g0.expr'), /__name__=~/);
  assert.equal(graphUrl('http://prom:9090', url.searchParams.get('g0.expr')), url.href);
  assert.equal(new URL(sourceUrl('http://prom:9090', 'prometheus_targets')).pathname, '/targets');
});

test('external source links reject executable URL schemes', () => {
  assert.equal(sourceUrl('file:///tmp/', 'prometheus_targets'), null);
  assert.equal(sourceUrl('http://prom:9090', 'unknown'), null);
  assert.throws(() => graphUrl('file:///tmp/', 'up'));
});
