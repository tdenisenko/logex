import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';
import crypto from 'node:crypto';
import {execFileSync} from 'node:child_process';

const file = 'crates/logex-server/src/web_ui.html';
const current = fs.readFileSync(file, 'utf8');
const baseline = execFileSync('git', ['show', 'f19f47c4:' + file], {encoding:'utf8'});
for (const [revision, html] of [['base', baseline], ['candidate', current]]) {
  const script = html.match(/<script>([\s\S]*?)<\/script>/)[1];
  new vm.Script(script); // Check syntax of the whole embedded script.
  const functions = ['buildQueryFromBuilder', 'normalizeAddress', 'sqlString', 'builderAddressSqlList']
    .map(name => {
      const match = script.match(new RegExp('^function ' + name + '\\([^]*?^}', 'm'));
      assert.ok(match, name);
      return match[0];
    }).join('\n');
  const failures = [];
  for (const address of [null, '0xdac17f958d2ee523a2206206994597c13d831ec7', '0xdAC17F958D2ee523a2206206994597C13D831ec7', '0xDAC17F958D2EE523A2206206994597C13D831EC7']) {
    const wallet = '0xE6c031F4C63e76e453d9A0aAe566D06236d11F95';
    const context = vm.createContext({
      selectedBuilderFields: () => ['block_number'],
      byId: id => ({value: id === 'builderEvent' ? 'Transfer(address,address,uint256)' : '__custom_erc20__'}),
      normalizeEventSignature: value => value,
      builderRangeMode: 'block',
      parseBuilderBlock: () => null,
      selectedBuilderToken: () => address ? {address, decimals:6} : null,
      CUSTOM_TOKEN_VALUE: '__custom_erc20__',
      DEFAULT_TOKEN_DECIMALS:18,
      DEFAULT_QUERY_LIMIT:500,
      builderHasAmountFilter: () => false,
      parseBuilderAmount: () => null,
      builderFromAddresses:[wallet],
      builderToAddresses:[],
    });
    vm.runInContext(functions, context);
    const query = vm.runInContext('buildQueryFromBuilder()', context);
    const clause = address && "address = '" + address.toLowerCase() + "'";
    if (address && !query.includes(clause)) failures.push({address, query});
    if (!address) assert.ok(!query.includes('address = '));
    assert.ok(query.includes("topic1 IN (address'" + wallet + "')"));
    assert.ok(query.includes("topic0 = event'Transfer(address,address,uint256)'"));
    assert.ok(query.endsWith('LIMIT 500'));
  }
  console.log(JSON.stringify({revision, node:process.version, platform:process.platform,
    html_sha256:crypto.createHash('sha256').update(html).digest('hex'), cases:4, failures}));
  if (revision === 'candidate') assert.deepEqual(failures, []);
  else assert.equal(failures.length, 2);
}
