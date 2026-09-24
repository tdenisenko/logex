import assert from 'node:assert/strict';
import test from 'node:test';
import vm from 'node:vm';
import { html, functionSource } from './dashboard-test-support.mjs';

// Exercise the embedded production functions with finite promises and a fake
// monotonic clock. No HTTP requests, real timers or node data are involved.
function functions(...names) {
  return names.map(functionSource).join('\n');
}
function deferred() {
  let resolve;
  let reject;
  const promise = new Promise((yes, no) => { resolve = yes; reject = no; });
  return { promise, resolve, reject };
}
async function settle() {
  for (let i = 0; i < 12; i++) await Promise.resolve();
}
function statusContext() {
  let now = 100;
  const calls = [];
  const timers = new Map();
  const updates = [];
  const labels = {};
  let nextTimer = 1;
  const context = {
    statusRequest: null, STATUS_TIMEOUT_MS: 10000,
    lastStatusReceivedAtMs: null,
    performance: { now: () => now }, AbortController,
    setTimeout(fn, delay) {
      const id = nextTimer++;
      timers.set(id, { fn, delay });
      return id;
    },
    clearTimeout(id) { timers.delete(id); },
    fetch(url, options) {
      const call = { ...deferred(), url, options };
      calls.push(call);
      return call.promise;
    },
    updateStatus(data) { updates.push(data); },
    setPill(id, label) { labels[id] = label; },
    setText(id, label) { labels[id] = label; },
    setExecutionBarState() {}, refreshLastUpdated() {},
  };
  vm.createContext(context);
  // The original has no factored failure helper; execute it when present so
  // the same witnesses can run against the archived original source.
  const names = ['fetchStatus'];
  if (html.includes('function markStatusOffline(')) names.push('markStatusOffline');
  vm.runInContext(functions(...names), context);
  return { context, calls, timers, updates, labels, advance(ms) { now += ms; } };
}
const ok = data => ({ ok: true, json: () => Promise.resolve(data) });

test('the complete embedded dashboard script parses', () => {
  const script = html.match(/<script>\s*([\s\S]*?)<\/script>/);
  assert.ok(script);
  new vm.Script(script[1]);
});

test('polls have one owner until the complete response body arrives', async () => {
  const c = statusContext();
  c.context.fetchStatus(); c.context.fetchStatus();
  await settle();
  assert.equal(c.calls.length, 1);
  const body = deferred();
  c.calls[0].resolve({ ok: true, json: () => body.promise });
  await settle(); c.context.fetchStatus(); await settle();
  assert.equal(c.calls.length, 1);
  body.resolve({ node_state: 'syncing' }); await settle();
  assert.equal(c.updates.length, 1);
  assert.equal(c.timers.size, 0);
  c.context.fetchStatus(); await settle();
  assert.equal(c.calls.length, 2);
});

test('deadline marks status unavailable and allows the next poll', async () => {
  const c = statusContext(); c.context.fetchStatus(); await settle();
  assert.equal(c.timers.size, 1);
  const timer = [...c.timers.values()][0];
  assert.equal(timer.delay, 10000);
  c.advance(10000); timer.fn(); await settle();
  assert.equal(c.calls[0].options.signal.aborted, true);
  assert.equal(c.labels.nodeBadge, 'Status offline');
  c.context.fetchStatus(); await settle();
  assert.equal(c.calls.length, 2);
  c.calls[1].resolve(ok({ node_state: 'disconnected' })); await settle();
  c.calls[0].resolve(ok({ node_state: 'synced' })); await settle();
  assert.deepEqual(c.updates, [{ node_state: 'disconnected' }]);
});

test('late rejected poll cannot overwrite a recovered status', async () => {
  const c = statusContext(); c.context.fetchStatus(); await settle();
  assert.equal(c.timers.size, 1);
  c.advance(10000); [...c.timers.values()][0].fn(); await settle();
  c.context.fetchStatus(); await settle();
  c.calls[1].resolve(ok({ node_state: 'syncing' })); await settle();
  c.labels.nodeBadge = 'Syncing';
  c.calls[0].reject(new Error('old response aborted')); await settle();
  assert.equal(c.labels.nodeBadge, 'Syncing');
});

test('elapsed deadline rejects a response even when the timeout callback was delayed', async () => {
  const c = statusContext(); c.context.fetchStatus(); await settle();
  c.advance(10001);
  c.calls[0].resolve(ok({ node_state: 'synced' })); await settle();
  assert.equal(c.updates.length, 0);
  assert.equal(c.labels.nodeBadge, 'Status offline');
  assert.equal(c.timers.size, 0);
});

test('HTTP and body errors release the poll owner', async () => {
  for (const response of [
    { ok: false, status: 401, json: () => Promise.resolve({ error: 'Unauthorized' }) },
    { ok: true, json: () => Promise.reject(new Error('incomplete body')) },
  ]) {
    const c = statusContext(); c.context.fetchStatus(); await settle();
    c.calls[0].resolve(response); await settle();
    assert.equal(c.updates.length, 0);
    assert.equal(c.labels.nodeBadge, 'Status offline');
    assert.equal(c.timers.size, 0);
    c.context.fetchStatus(); await settle();
    assert.equal(c.calls.length, 2);
  }
});

test('storage failure has a distinct state and a text diagnostic', async () => {
  const c = statusContext(); c.context.fetchStatus(); await settle();
  c.calls[0].resolve({ ok: false, status: 503, json: () => Promise.resolve({
    status: 'storage_unavailable', error: 'Expected volume is unavailable.',
  }) });
  await settle();
  assert.equal(c.updates.length, 0);
  assert.equal(c.labels.nodeBadge, 'Storage unavailable');
  assert.equal(c.labels.nodeState, 'Storage unavailable');
  assert.equal(c.labels.executionSummary, 'Expected volume is unavailable.');
  assert.equal(c.timers.size, 0);
});

test('display age follows elapsed time independently of wall clock corrections', () => {
  const labels = {};
  const c = {
    lastStatusReceivedAtMs: 0, lastStatusTimestamp: 1000,
    STATUS_TIMEOUT_MS: 10000,
    performance: { now: () => 65000 },
    Date: { now: () => 900000 },
    numberOrNull: Number, setText: (id, value) => { labels[id] = value; },
    setPill: (id, value) => { labels[id] = value; }, setExecutionBarState() {},
  };
  vm.createContext(c);
  const names = ['refreshLastUpdated', 'fmtAgo'];
  if (html.includes('function fmtAge(')) names.push('fmtAge');
  if (html.includes('function markStatusOffline(')) names.push('markStatusOffline');
  vm.runInContext(functions(...names), c);
  c.refreshLastUpdated();
  assert.equal(labels.lastUpdated, '1m ago');
  assert.equal(labels.nodeBadge, 'Status offline');
});

test('explicit unavailable node status wins over proximity to a fresh consensus head', () => {
  const labels = {};
  const c = {
    Date, performance: { now: () => 100 },
    lastStatusReceivedAtMs: null, lastStatusTimestamp: null,
    numberOrNull: value => value == null ? null : Number(value),
    anchorBlock: anchor => anchor?.block_number ?? null,
    anchorTime: anchor => anchor?.timestamp ?? null,
    setPill: (id, value) => { labels[id] = value; },
    setText: (id, value) => { labels[id] = value; },
  };
  for (const name of ['bytesPerSecValue', 'payloadBytesValue']) c[name] = () => 0;
  for (const name of [
    'stateClass', 'fmtDecimal', 'fmtBlocks', 'blockWithAge', 'fmt', 'fmtLogRate',
    'fmtP2pBandwidth', 'shortHash', 'blockRange', 'fmtBytes', 'familyList',
    'warningList', 'fmtMillis', 'fmtPct', 'fmtRate', 'fmtLogCount',
  ]) c[name] = String;
  c.clamp = (n, lo, hi) => Math.min(hi, Math.max(lo, n));
  for (const name of ['refreshLastUpdated', 'setWidth', 'setExecutionBarState',
    'setParentHidden', 'updateDefaultQuery', 'maybeApplyBuilderRange',
    'addPerfSample', 'renderChart']) c[name] = () => {};
  vm.createContext(c); vm.runInContext(functions('updateStatus'), c);
  c.updateStatus({ node_state: 'disconnected', node_state_label: 'Disconnected',
    historical_sync_disabled: true, consensus_head_fresh: true,
    current_block: 100, head_block: 100, target_block: 100 });
  assert.equal(labels.nodeBadge, 'Disconnected');
  assert.equal(labels.nodeState, 'Disconnected');
});

test('repair progress and failure stay visible while polling resumes normal status', async () => {
  const c = statusContext();
  for (const [data, badge, summary] of [
    [{ status: 'repairing', phase: 'rebuilding_indexes' }, 'Repair in progress', 'Rebuilding derived indexes. Queries are paused.'],
    [{ status: 'repair_failed', phase: 'failed', diagnostic: '<script>untrusted diagnostic</script>' }, 'Repair failed', '<script>untrusted diagnostic</script>'],
  ]) {
    c.context.fetchStatus(); await settle();
    c.calls.at(-1).resolve({ ok: false, status: 503, json: () => Promise.resolve(data) });
    await settle();
    assert.equal(c.labels.nodeBadge, badge);
    assert.equal(c.labels.nodeState, badge);
    assert.equal(c.labels.executionSummary, summary);
    assert.equal(c.context.lastStatusReceivedAtMs, 100);
    assert.equal(c.timers.size, 0);
    assert.equal(c.updates.length, 0);
  }
  c.context.fetchStatus(); await settle();
  c.calls.at(-1).resolve(ok({ node_state: 'syncing' })); await settle();
  assert.deepEqual(c.updates, [{ node_state: 'syncing' }]);
  assert.equal(c.timers.size, 0);
});
