// Run with: node --test tools/dashboard-rendering.test.mjs
// DASHBOARD_SOURCE selects an archived/proposed HTML fixture. No application or network needed.
import vm from 'node:vm';
import assert from 'node:assert/strict';
import test from 'node:test';
import { html, functionSource as source, dashboard as context } from './dashboard-test-support.mjs';
const transfer = '0x' + '22'.repeat(32);
const other = '0x' + '11'.repeat(32);
const amount = '0x' + (1000000n).toString(16).padStart(64, '0');
function formatter() {
  return context(['formatCell', 'normalizeTopicHash', 'decodeUint256BigInt', 'formatTokenAmount', 'tokenForResultRow', 'resultHasTransferContext', 'displayCellValue', 'columnTitle'], {
    TRANSFER_TOPIC0: transfer, TRANSFER_EVENT_SIGNATURE: 'Transfer(address,address,uint256)', MAX_TOKEN_DECIMALS: 255,
    byId: () => ({ value: 'Transfer(address,address,uint256)' }),
    tokenForAddress: () => ({ decimals: 6, symbol: 'USDC' }),
    selectedBuilderToken: () => ({ decimals: 6, symbol: 'USDC' }),
    normalizeAddress: value => value ? value.toLowerCase() : null, queryResultCustomToken: null,
    ERC20_TOKEN_BY_ADDRESS: { known: { decimals: 6, symbol: 'USDC' } },
  });
}
test('explicit conflicting event and missing context stay raw despite editor/builder', () => {
  const c = formatter();
  for (const row of [{ topic0: other, address: 'known' }, { address: 'known' }, { topic0: transfer }, undefined]) {
    assert.equal(c.displayCellValue('data', amount, row), amount);
  }
  assert.equal(c.columnTitle('data', [{ topic0: transfer }, { topic0: other }]), 'data');
  assert.equal(c.columnTitle('data', []), 'data');
});
test('explicit Transfer formatting and maximum uint256 remain exact', () => {
  const c = formatter();
  assert.equal(c.displayCellValue('data', amount, { topic0: transfer, address: 'known' }), '1 USDC');
  assert.equal(c.formatTokenAmount('0x' + 'ff'.repeat(32), null), (2n ** 256n - 1n).toString());
  assert.equal(c.formatCell(amount), amount);
});
test('custom token address lookup compares normalized addresses', () => {
  const custom = { address: '0xAbCd', decimals: 6 };
  const c = context(['tokenForAddress'], { normalizeAddress: value => value.toLowerCase(), customBuilderToken: () => custom, ERC20_TOKEN_BY_ADDRESS: {} });
  assert.equal(c.tokenForAddress('0xabcd'), custom);
});
test('history renders a native disclosure and synchronizes expanded state', () => {
  const elements = { queryHistoryBody: {}, queryHistoryEmpty: { style: {} }, queryHistoryMeta: {} };
  const detail = { hidden: true, getAttribute: () => 'one' };
  const button = { getAttribute: () => 'one', setAttribute: (key, value) => { button[key] = value; } };
  const c = context(['renderQueryHistory', 'toggleQueryHistoryDetail'], {
    byId: id => elements[id], queryHistory: [{ id: 'one', ok: false, sql: 'sample text', error: 'sample error' }],
    escapeHtml: String, fmt: String, fmtDateTime: () => 'date', formatQueryDuration: () => 'duration', queryHistorySummary: () => 'Failed',
    document: { querySelectorAll: selector => selector === '[data-history-detail-id]' ? [detail] : [button] },
  });
  c.renderQueryHistory();
  assert.match(elements.queryHistoryBody.innerHTML, /<button[^>]*data-history-action="details"[^>]*aria-expanded="false"/);
  c.toggleQueryHistoryDetail('one');
  assert.equal(detail.hidden, false); assert.equal(button['aria-expanded'], 'true');
  c.toggleQueryHistoryDetail('one');
  assert.equal(detail.hidden, true); assert.equal(button['aria-expanded'], 'false');
});
test('actual delegated history callback preserves pointer, use and export actions', () => {
  let callback;
  const calls = [];
  const start = html.indexOf("byId('queryHistoryTable').addEventListener('click'");
  const end = html.indexOf('\n});', start) + 4;
  vm.runInNewContext(html.slice(start, end), {
    byId: () => ({ addEventListener: (_, fn) => { callback = fn; } }),
    toggleQueryHistoryDetail: id => calls.push(['details', id]), useQueryHistoryEntry: id => calls.push(['use', id]), exportQueryHistoryEntry: id => calls.push(['export', id]),
  });
  for (const action of ['details', 'use', 'export', null]) {
    const target = { getAttribute: key => key === 'data-history-action' ? action : 'one' };
    callback({ stopPropagation() {}, target: { closest: selector => selector === '[data-history-action]' && !action ? null : target } });
  }
  assert.deepEqual(calls, [['details', 'one'], ['use', 'one'], ['export', 'one'], ['details', 'one']]);
});
test('clock rollback removes future samples and accepts current sample', () => {
  const c = context(['addPerfSample'], { Date: { now: () => 100000 }, perfSamples: [{ ts: 200000, rate: 9 }], savePerfSamples() {} });
  c.addPerfSample({ ts: 100000, rate: 1 });
  assert.equal(c.perfSamples.length, 1); assert.equal(c.perfSamples[0].ts, 100000);
  c.addPerfSample({ ts: 100001, rate: 2 });
  assert.equal(c.perfSamples.length, 1);
});
test('persisted future, invalid and expired timestamps do not suppress fresh chart data', () => {
  const c = context(['loadPerfSamples'], { Date: { now: () => 50000000 }, PERF_SAMPLE_STORAGE_KEY: 'samples', localStorage: { getItem: () => JSON.stringify([{ ts: 50000001 }, { ts: 1 }, { ts: 'bad' }, { ts: 49999000 }]) } });
  assert.equal(JSON.stringify(c.loadPerfSamples()), '[{"ts":49999000}]');
});

test('actual query dispatch snapshots custom metadata through response and later pages', async () => {
  let resolveFetch;
  let finish;
  let completed = new Promise(resolve => { finish = resolve; });
  const controls = { value: 'benign inert request fixture' };
  let custom = { address: '0xAbCd', symbol: 'CUSTOM', decimals: 6 };
  const shown = [];
  const c = formatter();
  Object.assign(c, {
    queryRunning: false, queryRows: [], queryColumns: [], queryResultCustomToken: null,
    byId: () => controls, customBuilderToken: () => custom,
    tokenForAddress: address => address.toLowerCase() === custom.address.toLowerCase() ? custom : null,
    AbortController, updateQueryButtons() {}, showQueryError() {}, setText() {},
    startQueryTimer() {}, stopQueryTimer: () => 0, renderTable() {}, addQueryHistory() {},
    setQueryRunning: running => { c.queryRunning = running; if (!running) finish(); },
    fetch: () => new Promise(resolve => { resolveFetch = resolve; }),
    renderQueryPage: () => { shown.push(c.displayCellValue('data', c.queryRows[0].data, c.queryRows[0])); },
  });
  if (html.includes('\nfunction parseQueryResponse(')) vm.runInContext(source('parseQueryResponse'), c);
  vm.runInContext(source('runQuery'), c);
  c.runQuery();
  custom.decimals = 18; // Mutate the original object too: a retained reference is insufficient.
  resolveFetch({ ok: true, status: 200, text: async () => JSON.stringify({ rows: [{ address: '0xabcd', topic0: transfer, data: amount }], total_scanned: 1 }) });
  await completed;
  assert.deepEqual(shown, ['1 CUSTOM']);
  custom = { address: '0xFFFF', symbol: 'CHANGED', decimals: 0 };
  c.renderQueryPage();
  assert.deepEqual(shown, ['1 CUSTOM', '1 CUSTOM']);
  for (const row of [{ topic0: transfer }, { topic0: transfer, address: '0xffff' }, { topic0: other, address: '0xabcd' }]) {
    assert.equal(c.displayCellValue('data', amount, row), amount);
  }
  assert.equal(c.displayCellValue('data', amount, { topic0: transfer, address: 'known' }), '1 USDC');
  // A new empty input clears both backing rows and the associated metadata.
  controls.value = '';
  c.runQuery();
  assert.equal(c.queryRows.length, 0); assert.equal(c.queryResultCustomToken, null);
  // Failed and empty-success responses must not retain candidate metadata either.
  for (const ok of [false, true]) {
    completed = new Promise(resolve => { finish = resolve; });
    controls.value = 'benign inert request fixture';
    c.runQuery();
    resolveFetch({ ok, status: ok ? 200 : 400, text: async () => JSON.stringify(ok ? { rows: [], total_scanned: 0 } : { error: 'fixture failure' }) });
    // Empty result rendering has no first row.
    c.renderQueryPage = () => {};
    await completed;
    assert.equal(c.queryRows.length, 0); assert.equal(c.queryResultCustomToken, null);
  }
});

test('chart range rendering excludes future samples and derives legends from visible data', () => {
  const text = {};
  const series = [];
  const c = context(['renderChart', 'downsampleSamples'], {
    Date: { now: () => 100000 }, chartHours: 1,
    perfSamples: [{ ts: 90000, rate: 2, peers: 3, cpu: 4 }, { ts: 200000, rate: 9, peers: 10, cpu: 11 }],
    window: { Chart: true }, byId: () => ({ style: {} }), ensureCharts() {},
    updateChart: (_, samples) => series.push(Array.from(samples, sample => sample.ts)),
    setText: (key, value) => { text[key] = value; }, fmtLogRate: String, fmt: String, fmtPct: String,
  });
  c.renderChart();
  assert.deepEqual(series, [[90000], [90000], [90000]]);
  assert.equal(text.rateLegend, '2'); assert.equal(text.peerLegend, '3'); assert.equal(text.cpuLegend, '4');
  c.perfSamples = [{ ts: 200000, rate: 9 }];
  c.renderChart();
  assert.equal(text.rateLegend, '--'); assert.equal(text.peerLegend, '--'); assert.equal(text.cpuLegend, '--');
});

test('chart range selection exposes the selected toggle state', () => {
  const buttons = [1, 6, 12].map(hours => ({
    hours, attributes: {}, active: false,
    getAttribute: () => String(hours),
    setAttribute(name, value) { this.attributes[name] = value; },
    classList: { toggle(_, active) { buttons.find(button => button.hours === hours).active = active; } },
  }));
  let renders = 0;
  const c = context(['setChartWindow'], { chartHours: 1, document: { querySelectorAll: () => buttons }, renderChart: () => { renders++; } });
  for (const hours of [6, 12, 1]) {
    c.setChartWindow(hours);
    assert.equal(c.chartHours, hours);
    for (const button of buttons) {
      assert.equal(button.active, button.hours === hours);
      assert.equal(button.attributes['aria-pressed'], String(button.hours === hours));
    }
  }
  assert.equal(renders, 3);
});
