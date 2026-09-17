// Finite offline controls against the actual inline dashboard functions.
// Override DASHBOARD_SOURCE to compare an archived source or proposed patch.
import assert from 'node:assert/strict';
import { test } from 'node:test';
import { dashboard, html } from './dashboard-test-support.mjs';

function input(value = '') {
  return {
    value, type: 'number', style: {}, attributes: {},
    setAttribute(name, value) { this.attributes[name] = value; },
    removeAttribute(name) { delete this.attributes[name]; },
  };
}

// Independent, small CSV reader verifies the exported representation, not just quoting.
function parseCsv(text) {
  const rows = [[]];
  let cell = '';
  let quoted = false;
  for (let i = 0; i < text.length; i++) {
    const char = text[i];
    if (char === '"') {
      if (quoted && text[i + 1] === '"') { cell += '"'; i++; }
      else quoted = !quoted;
    } else if (!quoted && (char === ',' || char === '\n')) {
      rows.at(-1).push(cell); cell = '';
      if (char === '\n') rows.push([]);
    } else cell += char;
  }
  assert.equal(quoted, false);
  rows.at(-1).push(cell);
  return rows;
}

test('raw nested result values retain structure and explicit nulls', () => {
  const context = dashboard(['formatCell']);
  for (const value of [[{ n: '9007199254740993' }, null, [2, 3]], ['a,b', '', null], []]) {
    assert.deepEqual(JSON.parse(context.formatCell(value)), value);
  }
  assert.equal(context.formatCell('9007199254740993.0001'), '9007199254740993.0001');
  assert.equal(context.formatCell(null), '');
  assert.equal(context.formatCell(false), 'false');
});

test('CSV exports all loaded rows with nested values and exact strings', async () => {
  let blob;
  let clicked = 0;
  let revoked = 0;
  const rows = [
    { value: [{ n: 1 }, null, [2, 3]], exact: '9007199254740993.0001', text: 'quote " and, comma\r\nline' },
    { value: [], exact: '0.000000000000000001', text: 'last loaded row' },
  ];
  const context = dashboard(['formatCell', 'csvCell', 'exportQueryResults'], {
    queryRows: rows, queryColumns: Object.keys(rows[0]), queryOffset: 1, QUERY_PAGE_SIZE: 1, Blob,
    showQueryError(message) { assert.fail(message); },
    URL: { createObjectURL(value) { blob = value; return 'blob:offline'; }, revokeObjectURL() { revoked++; } },
    document: { body: { appendChild() {} }, createElement() { return { click() { clicked++; }, remove() {} }; } },
  });
  context.exportQueryResults();
  const csvRows = parseCsv(await blob.text());
  assert.equal(csvRows.length, rows.length + 1);
  assert.deepEqual(csvRows[0], Object.keys(rows[0]));
  rows.forEach((row, index) => {
    assert.deepEqual(JSON.parse(csvRows[index + 1][0]), row.value);
    assert.equal(csvRows[index + 1][1], row.exact);
    assert.equal(csvRows[index + 1][2], row.text);
  });
  assert.equal(clicked, 1);
  assert.equal(revoked, 1);
});

test('reselecting either active range mode preserves its bounds', () => {
  for (const mode of ['block', 'time']) {
    const elements = { builderFromBlock: input('100'), builderToBlock: input('200'), builderFromLabel: {}, builderToLabel: {} };
    const context = dashboard(['setBuilderRangeMode'], {
      builderRangeMode: mode, byId: id => elements[id], document: { querySelectorAll: () => [] },
    });
    context.setBuilderRangeMode(mode);
    assert.equal(elements.builderFromBlock.value, '100');
    assert.equal(elements.builderToBlock.value, '200');
  }
});

test('actual range switches still reset incompatible values and input types', () => {
  const elements = { builderFromBlock: input('100'), builderToBlock: input('200'), builderFromLabel: {}, builderToLabel: {} };
  const context = dashboard(['setBuilderRangeMode'], {
    builderRangeMode: 'block', byId: id => elements[id], document: { querySelectorAll: () => [] },
  });
  context.setBuilderRangeMode('time');
  assert.equal(elements.builderFromBlock.value, '');
  assert.equal(elements.builderToBlock.value, '');
  assert.equal(elements.builderFromBlock.type, 'datetime-local');
  elements.builderFromBlock.value = '2026-09-18T12:00';
  context.setBuilderRangeMode('block');
  assert.equal(elements.builderFromBlock.value, '');
  assert.equal(elements.builderFromBlock.type, 'number');
  assert.equal(elements.builderFromBlock.attributes.min, '0');
});

test('canonical event tuple and array signatures are accepted unchanged', () => {
  const context = dashboard(['normalizeEventSignature']);
  for (const signature of [
    'Changed((address,uint256))', 'Changed((address,uint256)[])',
    'Changed((bytes32,(bool,string)[])[2],uint8[0][])', 'Changed(())',
    'Changed(function,bytes1,bytes32,int8,int256,fixed8x1,ufixed256x80)',
  ]) assert.equal(context.normalizeEventSignature(signature), signature);
});

test('event aliases, unknown types and malformed nesting are rejected', () => {
  const context = dashboard(['normalizeEventSignature']);
  for (const signature of [
    'Changed(uint)', 'Changed(int)', 'Changed(fixed)', 'Changed(ufixed)', 'Changed(bogus)',
    'Changed(uint7)', 'Changed(uint264)', 'Changed(bytes0)', 'Changed(bytes33)',
    'Changed(uint08)', 'Changed(uint256[01])', 'Changed(fixed128x0)', 'Changed(fixed128x81)',
    'Changed((address,uint256)', 'Changed(address,,uint256)', 'Changed((address,))',
    'Changed(uint256[])extra', 'Changed(uint256[)', 'Changed(,)',
  ]) assert.throws(() => context.normalizeEventSignature(signature), undefined, signature);
});

test('existing canonical events, empty signatures and whitespace are preserved', () => {
  const context = dashboard(['normalizeEventSignature']);
  assert.equal(context.normalizeEventSignature(' Transfer( address, address, uint256 ) '), 'Transfer(address,address,uint256)');
  assert.equal(context.normalizeEventSignature('Changed()'), 'Changed()');
  assert.equal(context.normalizeEventSignature('Changed(bytes[],uint256[2])'), 'Changed(bytes[],uint256[2])');
});

test('empty query submission clears visible and backing results without sending a request', () => {
  const elements = { sql: input('  '), prevPageBtn: {}, nextPageBtn: {}, exportBtn: {}, resultCount: { textContent: '1 row loaded' }, queryTimer: {} };
  let displayedRows = [{ block_number: 123 }];
  let error = '';
  const context = dashboard(['updateQueryButtons', 'runQuery'], {
    queryRunning: false, queryRows: displayedRows, queryColumns: ['block_number'], queryOffset: 10,
    queryScannedRows: 1, QUERY_PAGE_SIZE: 10, byId: id => elements[id],
    showQueryError(message) { error = message; }, stopQueryTimer() {},
    setText(id, text) { elements[id].textContent = text; },
    renderTable(rows) { displayedRows = rows; }, fetch() { assert.fail('Unexpected request'); },
    addQueryHistory() { assert.fail('Unexpected history entry'); },
  });
  context.runQuery();
  assert.equal(error, 'Enter a SQL query.');
  assert.equal(context.queryRows.length, 0);
  assert.equal(context.queryColumns.length, 0);
  assert.equal(context.queryScannedRows, null);
  assert.equal(context.queryOffset, 0);
  assert.equal(displayedRows.length, 0);
  assert.equal(elements.resultCount.textContent, 'No query results yet.');
  assert.equal(elements.exportBtn.disabled, true);
  assert.equal(elements.prevPageBtn.disabled, true);
  assert.equal(elements.nextPageBtn.disabled, true);
});

test('generated transfer SQL preserves exact amount bounds, address topics and explicit LIMIT', () => {
  const elements = Object.fromEntries(Object.entries({
    builderEvent: 'Transfer(address,address,uint256)', builderFromBlock: '100', builderToBlock: '200',
    builderToken: 'known', builderAmountMin: '1.000000000000000001', builderAmountMax: '2',
  }).map(([id, value]) => [id, input(value)]));
  const address = '0x0000000000000000000000000000000000000001';
  const context = dashboard([
    'normalizeEventSignature', 'parseBuilderBlock', 'parseBuilderTimestamp', 'builderHasAmountFilter',
    'parseTokenDecimals', 'parseBuilderAmount', 'uint256SqlHex', 'sqlString', 'builderAddressSqlList',
    'buildQueryFromBuilder',
  ], {
    byId: id => elements[id], selectedBuilderFields: () => ['block_number', 'data'],
    selectedBuilderToken: () => ({ address, decimals: 18 }), normalizeAddress: value => value.toLowerCase(),
    builderRangeMode: 'block', builderFromAddresses: [address], builderToAddresses: [address],
    CUSTOM_TOKEN_VALUE: 'custom', DEFAULT_TOKEN_DECIMALS: 18, MAX_TOKEN_DECIMALS: 36, DEFAULT_QUERY_LIMIT: 500,
  });
  assert.equal(context.buildQueryFromBuilder(), [
    'SELECT block_number, data', 'FROM logs',
    "WHERE topic0 = event'Transfer(address,address,uint256)'",
    `  AND address = '${address}'`, `  AND topic1 IN (address'${address}')`, `  AND topic2 IN (address'${address}')`,
    '  AND block_number BETWEEN 100 AND 200', '  AND data_len = 32',
    "  AND data >= '0x0000000000000000000000000000000000000000000000000de0b6b3a7640001'",
    "  AND data <= '0x0000000000000000000000000000000000000000000000001bc16d674ec80000'",
    'ORDER BY block_number DESC, tx_index DESC, log_index DESC', 'LIMIT 500',
  ].join('\n'));
});

test('block input rejects fractional values hidden by floating-point rounding', () => {
  let value;
  const context = dashboard(['parseBuilderBlock'], { byId: () => ({ value }) });
  for (value of ['1.0000000000000001', '100.0000000000000001', '1e-324', '-1e-324', '9007199254740990.9']) {
    assert.throws(() => context.parseBuilderBlock('from'), /non-negative whole numbers/, value);
  }
});

test('block input preserves exact whole decimal and scientific values within safe bounds', () => {
  let value;
  const context = dashboard(['parseBuilderBlock'], { byId: () => ({ value }) });
  for (const [text, expected] of [
    ['', null], ['  ', null], ['0', 0], ['.0', 0], ['1.0', 1], ['1e3', 1000], ['10e-1', 1],
    ['0001.000e+2', 100], ['0e-999999999999999999999', 0], ['0e999999999999999999999', 0],
    ['9007199254740991', Number.MAX_SAFE_INTEGER], ['9007199254740991.0', Number.MAX_SAFE_INTEGER],
    ['90071992547409910e-1', Number.MAX_SAFE_INTEGER],
  ]) {
    value = text;
    assert.equal(context.parseBuilderBlock('from'), expected, text);
  }
  for (value of ['-1', '1.2', '9007199254740992', '1e999999999999999999999', 'NaN', 'Infinity']) {
    assert.throws(() => context.parseBuilderBlock('from'), /non-negative whole numbers/, value);
  }
});

test('block input distinguishes native invalid text from an intentional blank bound', () => {
  const element = { value: '', validity: { badInput: true } };
  const context = dashboard(['parseBuilderBlock'], { byId: () => element });
  assert.throws(() => context.parseBuilderBlock('from'), /non-negative whole numbers/);
  element.validity.badInput = false;
  assert.equal(context.parseBuilderBlock('from'), null);
});

test('timestamp input distinguishes native invalid text from an intentional blank bound', () => {
  const element = { value: '', validity: { badInput: true } };
  const context = dashboard(['parseBuilderTimestamp'], { byId: () => element });
  assert.throws(() => context.parseBuilderTimestamp('from', 'From time'), /From time must be a valid local time/);
  element.validity.badInput = false;
  assert.equal(context.parseBuilderTimestamp('from', 'From time'), null);
  element.value = '2026-09-18T12:34:56';
  assert.equal(context.parseBuilderTimestamp('from', 'From time'), Math.floor(new Date(2026, 8, 18, 12, 34, 56).getTime() / 1000));
});

// Exercise the actual response path; archived originals lack the new parser helper.
async function queryResponseFixture(wire, { legacy = false, ok = true, status = 200 } = {}) {
  const history = [];
  const errors = [];
  let blob;
  let context;
  const names = ['runQuery', 'formatCell', 'csvCell', 'exportQueryResults'];
  if (html.includes('\nfunction parseQueryResponse(')) names.push('parseQueryResponse');
  const globals = {
    queryRunning: false, queryRows: [], queryColumns: [], queryOffset: 0,
    byId: () => ({ value: "SELECT CAST('9007199254740993' AS BIGINT) AS n" }),
    updateQueryButtons() {}, showQueryError(message) { if (message) errors.push(message); },
    customBuilderToken: () => null, startQueryTimer() {}, stopQueryTimer: () => 7,
    setQueryRunning(value) { context.queryRunning = value; },
    setText() {}, renderTable() {}, renderQueryPage() {}, AbortController, Blob,
    addQueryHistory(entry) { history.push(entry); },
    fetch: () => Promise.resolve({ ok, status, text: () => Promise.resolve(wire) }),
    URL: { createObjectURL(value) { blob = value; return 'blob:offline'; }, revokeObjectURL() {} },
    document: { body: { appendChild() {} }, createElement() { return { click() {}, remove() {} }; } },
  };
  if (legacy) globals.JSON = {
    parse(text, reviver) {
      return JSON.parse(text, reviver && function(key, value) { return reviver.call(this, key, value); });
    },
    stringify: JSON.stringify,
  };
  context = dashboard(names, globals);
  context.runQuery();
  await new Promise(resolve => setImmediate(resolve));
  assert.equal(context.queryRunning, false);
  assert.equal(history.length, 1);
  return { context, history, errors, exportedBlob: () => blob };
}

test('query decoding preserves unsafe i64/u64 and nested integers through raw CSV and history', async () => {
  const wire = String.raw`{"rows":[{"n":9007199254740993,"negative":-9007199254740993,"maxSigned":9223372036854775807,"minSigned":-9223372036854775808,"maxUnsigned":18446744073709551615,"nested":[9007199254740993,{"n":18446744073709551615},null],"exact":"9007199254740993.0001","escaped":"digits 9007199254740993 and \"quote\""}],"total_scanned":1,"row_count":1}`;
  const result = await queryResponseFixture(wire);
  const row = result.context.queryRows[0];
  const expected = {
    n: '9007199254740993', negative: '-9007199254740993', maxSigned: '9223372036854775807',
    minSigned: '-9223372036854775808', maxUnsigned: '18446744073709551615',
    nested: ['9007199254740993', { n: '18446744073709551615' }, null],
    exact: '9007199254740993.0001', escaped: 'digits 9007199254740993 and "quote"',
  };
  assert.deepEqual(JSON.parse(JSON.stringify(row)), expected);
  assert.equal(result.context.formatCell(row.n), expected.n);
  result.context.exportQueryResults();
  const [headers, cells] = parseCsv(await result.exportedBlob().text());
  headers.forEach((name, index) => {
    assert.equal(cells[index], name === 'nested' ? JSON.stringify(expected.nested) : expected[name]);
  });
  assert.equal(result.history[0].ok, true);
  assert.equal(result.history[0].rowCount, 1);
  assert.equal(result.history[0].scannedRows, 1);
  assert.equal(result.history[0].durationMs, 7);
  assert.equal(result.errors.length, 0);
});

test('query decoding preserves safe integers, floating tokens, nulls and existing strings', async () => {
  const wire = '{"rows":[{"safe":9007199254740991,"negative":-9007199254740991,"zero":0,"float":1.25,"largeFloat":100000000000000000000.0,"scientific":1e20,"small":1e-7,"nullable":null,"bool":true,"exact":"9007199254740993"}],"total_scanned":0}';
  const result = await queryResponseFixture(wire);
  assert.deepEqual(JSON.parse(JSON.stringify(result.context.queryRows)), JSON.parse(wire).rows);
  assert.equal(result.history[0].ok, true);
});

test('query decoding on legacy engines rejects unsafe integers and ambiguous large floats explicitly', async () => {
  for (const token of ['9007199254740993', '-9223372036854775808', '1e20']) {
    const result = await queryResponseFixture(`{"rows":[{"n":${token}}],"total_scanned":0}`, { legacy: true });
    assert.equal(result.history[0].ok, false, token);
    assert.equal(result.context.queryRows.length, 0, token);
    assert.equal(result.context.queryResultCustomToken, null);
    assert.match(result.errors[0], /browser.*exact|exact.*browser/i);
  }
  const result = await queryResponseFixture('{"rows":[{"n":9007199254740991,"float":1.25,"exact":"9007199254740993","nested":[null,7]}],"total_scanned":0}', { legacy: true });
  assert.equal(result.history[0].ok, true);
  assert.equal(result.context.queryRows[0].n, Number.MAX_SAFE_INTEGER);
  assert.equal(result.context.queryRows[0].exact, '9007199254740993');
});

test('invalid successful query responses fail while non-OK text and JSON errors remain readable', async () => {
  for (const wire of ['', 'not JSON', '{"rows":', '{"message":"wrong response"}', 'null']) {
    const result = await queryResponseFixture(wire);
    assert.equal(result.history[0].ok, false, wire);
    assert.equal(result.context.queryRows.length, 0, wire);
    assert.ok(result.errors[0]);
  }
  for (const [wire, message] of [['gateway unavailable', 'gateway unavailable'], ['{"error":"query canceled"}', 'query canceled']]) {
    const result = await queryResponseFixture(wire, { ok: false, status: 503 });
    assert.equal(result.history[0].ok, false);
    assert.equal(result.errors[0], message);
  }
});
