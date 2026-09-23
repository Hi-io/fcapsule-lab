'use strict';

function canStart(state) {
  return Boolean(state && !state.read_only_preview && !state.active && !state.memory_error &&
    !state.external_probe?.active &&
    state.memory?.node_identity_verified === true &&
    Number.isFinite(state.memory?.available_bytes) && state.memory.available_bytes >= 1073741824 &&
    ['worker', 'inventory', 'orders'].every(name => state[name]?.reachable));
}

function selections(state) {
  return Object.entries(state?.scenarios || {}).map(([id, item]) => ({ ...item, id, scenario: id }));
}

function sourceUrl(base, sourceView) {
  if (!['prometheus_graph', 'prometheus_targets'].includes(sourceView)) return null;
  const graph = sourceView === 'prometheus_graph';
  const url = new URL(graph ? '/query' : '/targets', base);
  if (!['http:', 'https:'].includes(url.protocol)) return null;
  if (graph) {
    url.searchParams.set('g0.expr', '{__name__=~"inventory_mysql_client_sessions_active|inventory_mysql_server_max_connections|mysql_global_status_threads_connected|mysql_global_variables_max_connections",namespace="fcapsule-lab"}');
    url.searchParams.set('g0.tab', 'graph'); url.searchParams.set('g0.range_input', '10m');
  }
  return url.href;
}

if (typeof module !== 'undefined') module.exports = { canStart, selections, sourceUrl };

if (typeof document !== 'undefined') {
  const $ = selector => document.querySelector(selector);
  let state = null, busy = false, reading = false, shape = '', available = false;
  const notice = text => { $('#notice').textContent = text; };

  function render() {
    const items = selections(state);
    const nextShape = JSON.stringify(items) + state?.prometheus_url;
    // Preserve focused controls and tab order during status polling.
    if (shape !== nextShape) {
      const fragment = document.createDocumentFragment();
      for (const item of items) {
        const article = document.createElement('article'); article.className = 'scenario ' + item.class.toLowerCase();
        const title = document.createElement('h2'); title.textContent = item.title;
        const tag = document.createElement('span'); tag.className = 'tag'; tag.textContent = item.class; title.append(tag);
        const summary = document.createElement('p'); summary.textContent = item.summary;
        const evidence = document.createElement('small'); evidence.className = 'scenario-evidence';
        evidence.textContent = 'Evidence: ' + (item.evidence_domains || []).join(' · ');
        const actions = document.createElement('div'); actions.className = 'actions';
        if (item.runner_only) {
          const label = document.createElement('span'); label.textContent = 'External screenshot runner';
          const plan = document.createElement('a'); plan.href = '/api/scenarios/' + encodeURIComponent(item.id) + '/plan';
          plan.textContent = 'Open run instructions';
          actions.append(label, plan);
        } else {
          const start = document.createElement('button'); start.dataset.start = item.id;
          start.addEventListener('click', () => startScenario(item)); actions.append(start);
        }
        if (item.source_view) {
          const href = sourceUrl(state.prometheus_url, item.source_view);
          if (href) {
            const link = document.createElement('a'); link.href = href; link.target = '_blank'; link.rel = 'noopener noreferrer';
            link.textContent = item.source_view === 'prometheus_graph' ? 'Prometheus graph' : 'Prometheus targets'; actions.append(link);
          }
        }
        article.append(title, summary, evidence, actions); fragment.append(article);
      }
      $('#scenarios').replaceChildren(fragment); shape = nextShape;
    }
    for (const item of items) {
      const button = [...document.querySelectorAll('[data-start]')].find(b => b.dataset.start === item.id);
      if (!button) continue;
      const active = state.active?.scenario === item.scenario;
      button.disabled = busy || !available || !canStart(state) || item.runner_only;
      button.textContent = active ? (state.active.status === 'recovering' ? 'Recovering' : 'Running') :
        item.runner_only ? 'Recorded runner only' : item.rounds === 2 ? 'Start one occurrence' : 'Start scenario';
      button.closest('article').classList.toggle('active', active);
    }
    $('#scenarios').setAttribute('aria-busy', String(reading));
    $('#duration').disabled = busy || Boolean(state?.active);
    $('#recover').disabled = busy || !available || Boolean(state?.read_only_preview);
  }

  async function status() {
    if (reading) return;
    reading = true;
    try {
      const response = await fetch('/api/status', { cache: 'no-store' });
      if (!response.ok) throw new Error('Control unavailable');
      state = await response.json(); available = true;
      const memory = state.memory && Number.isFinite(state.memory.available_bytes) ?
        'Node available: ' + (state.memory.available_bytes / 1073741824).toFixed(2) + ' GiB' : 'Memory check unavailable';
      const identity = state.memory?.node_identity_verified ? ' | node source verified' : ' | node source unverified';
      const external = state.external_probe?.active ? ' | Prometheus screenshot run owns Lab' : '';
      const active = state.active;
      $('#run-state').textContent = memory + identity + external + (active ? ' | ' +
        (state.scenarios?.[active.scenario]?.title || active.scenario) + ' | ' + active.status + ' | ' +
        Math.max(0, Math.ceil(active.expires_at - Date.now() / 1000)) + 's remaining' : ' | No active run');
      for (const name of ['worker', 'inventory', 'orders']) {
        const up = state[name]?.reachable;
        $('#' + name + '-dot').className = 'dot' + (up ? ' up' : ' down');
        $('#' + name + '-state').textContent = up ? (state[name].mode || state[name].failure_mode || 'healthy') : 'unreachable';
      }
      if (!busy) notice(state.read_only_preview ? 'Read-only local preview' : active ? (active.status === 'recovering' ? 'Waiting for recovery' : 'Run in progress') :
        canStart(state) ? 'Ready' : 'Start unavailable');
    } catch (_) {
      available = false; $('#run-state').textContent = 'Control API unavailable'; notice('Control API unavailable');
      for (const name of ['worker', 'inventory', 'orders']) {
        $('#' + name + '-dot').className = 'dot'; $('#' + name + '-state').textContent = 'unknown';
      }
    } finally { reading = false; render(); }
  }

  async function startScenario(item) {
    if (busy || !available || !canStart(state) || item.runner_only) return;
    busy = true; render(); notice('Starting ' + item.title);
    let message;
    try {
      const response = await fetch('/api/scenarios/' + encodeURIComponent(item.scenario) + '/start', {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ duration_seconds: Number($('#duration').value) }),
      });
      const result = await response.json(); message = result.message || result.error || (response.ok ? 'Scenario started' : 'Start failed');
    } catch (_) { message = 'Start could not be confirmed; check active run before retrying'; }
    await status(); busy = false; render(); notice(message);
  }

  $('#recover').addEventListener('click', async () => {
    if (busy || !available) return;
    busy = true; render(); notice('Applying recovery');
    let message;
    try {
      const response = await fetch('/api/recover', { method: 'POST' });
      const result = await response.json(); message = result.message || result.error;
    } catch (_) { message = 'Recovery not confirmed; check active run'; }
    await status(); busy = false; render(); notice(message);
  });
  status(); setInterval(status, 5000);
}
