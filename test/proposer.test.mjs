import test from 'node:test';
import assert from 'node:assert/strict';
import { Interface, Wallet } from 'ethers';
import {
  ADAPTER_INTERFACE,
  OO_INTERFACE,
  ERC20_INTERFACE,
  prepareProposer,
} from '../proposer.mjs';

const KEY = Wallet.createRandom().privateKey;
const ADAPTER = '0x2F5e3684cb1F318ec51b00Edba38d79Ac2c0aA9d';
const OO = '0xee3afe347d5c74317041e2618c49534daf887c24';
const TOKEN = '0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174';
const CREATOR = '0x91430CaD2d3975766499717fA0D66A78D814E5c5';
const RPCS = ['https://rpc-a.invalid', 'https://rpc-b.invalid'];
const FINALISTS = [
  { id: 'atpa', name: 'Player A' },
  { id: 'atpb', name: 'Player B' },
];

const market = (name, requestId) => ({
  question: `Will ${name} win the 2026 Men's US Open?`,
  slug: `will-${name.toLowerCase().replaceAll(' ', '-')}-win-the-2026-mens-us-open`,
  active: true,
  closed: false,
  negRisk: true,
  negRiskRequestID: requestId,
  conditionId: `0x${'a'.repeat(64)}`,
  outcomes: '["Yes","No"]',
});

function makeRpc({ balance = 100n, sendResult = '0x' + 'a'.repeat(64) } = {}) {
  const calls = [];
  const adapterInterface = new Interface(ADAPTER_INTERFACE);
  const ooInterface = new Interface(OO_INTERFACE);
  const erc20Interface = new Interface(ERC20_INTERFACE);
  const requestIds = {
    ['0x' + '1'.repeat(64)]: 101n,
    ['0x' + '2'.repeat(64)]: 102n,
  };
  const rpcCall = async (_url, method, params) => {
    calls.push({ method, params });
    if (method === 'eth_chainId') return '0x89';
    if (method === 'eth_blockNumber') return '0x100';
    if (method === 'eth_gasPrice') return '0x1';
    if (method === 'eth_getTransactionCount') return '0x2';
    if (method === 'eth_getBalance') return '0x1000000000000000000';
    if (method === 'eth_sendRawTransaction') return sendResult;
    if (method === 'eth_getTransactionReceipt') return null;
    if (method !== 'eth_call') throw new Error(`unexpected RPC method ${method}`);
    const data = params[0].data;
    const selector = data.slice(0, 10);
    if (selector === adapterInterface.getFunction('optimisticOracle').selector) {
      return adapterInterface.encodeFunctionResult('optimisticOracle', [OO]);
    }
    if (selector === adapterInterface.getFunction('getQuestion').selector) {
      const id = `0x${data.slice(10 + 24, 10 + 24 + 64)}`;
      const timestamp = requestIds[id] ?? 101n;
      return adapterInterface.encodeFunctionResult('getQuestion', [[
        timestamp, 0n, 500n, 3600n, 0n, false, false, false, false, TOKEN, CREATOR,
        '0x7469746c653a2054657374',
      ]]);
    }
    if (selector === ooInterface.getFunction('getState').selector) {
      return ooInterface.encodeFunctionResult('getState', [1]);
    }
    if (selector === ooInterface.getFunction('getRequest').selector) {
      return ooInterface.encodeFunctionResult('getRequest', [[
        '0x0000000000000000000000000000000000000000',
        '0x0000000000000000000000000000000000000000',
        TOKEN,
        false,
        [true, true, false, true, false, 500n, 0n],
        0n,
        0n,
        0n,
        0n,
        250n,
      ]]);
    }
    if (selector === erc20Interface.getFunction('balanceOf').selector) {
      return erc20Interface.encodeFunctionResult('balanceOf', [balance]);
    }
    throw new Error(`unexpected eth_call selector ${selector}`);
  };
  return { calls, rpcCall };
}

async function prepare({ balance = 100n } = {}) {
  const mock = makeRpc({ balance });
  const fetchCalls = [];
  const proposer = await prepareProposer({
    finalists: FINALISTS,
    matchId: 'mens-final-1',
    rpcUrls: RPCS,
    rpcCall: mock.rpcCall,
    fetchImpl: async (...args) => {
      fetchCalls.push(args);
      throw new Error('unexpected HTTP call');
    },
    gammaEvent: {
      slug: '2026-mens-us-open-winner-tennis',
      markets: [market('Player A', '0x' + '1'.repeat(64)), market('Player B', '0x' + '2'.repeat(64))],
    },
    privateKey: KEY,
    autoTest: true,
    gasLimit: 350000n,
    receiptMaxAttempts: 0,
    emitUi: false,
  });
  return { proposer, mock, fetchCalls };
}

const finalFor = (loserId) => ({
  type: 'MATCH_FINAL',
  matchId: 'mens-final-1',
  status: 'Completed',
  winnerId: loserId === 'atpa' ? 'atpb' : 'atpa',
  winnerName: loserId === 'atpa' ? 'Player B' : 'Player A',
  loserId,
  loserName: loserId === 'atpa' ? 'Player A' : 'Player B',
  source: 'http',
  sourceSequence: 1,
  networkReceivedMonoNs: 1n,
  detectedMonoNs: 2n,
  detectedWallTime: '2026-09-13T12:00:00.000Z',
});

test('hot path selects the loser NO template and does not read chain/market state', async () => {
  const { proposer, mock, fetchCalls } = await prepare();
  const before = mock.calls.length;
  const httpBefore = fetchCalls.length;
  const result = await proposer.handleFinal(finalFor('atpb'));
  const hotCalls = mock.calls.slice(before).map((call) => call.method);
  assert.equal(result.templateKey, 'PLAYER_B_NO_TX_TEMPLATE');
  assert.deepEqual([...new Set(hotCalls)], ['eth_sendRawTransaction']);
  assert.equal(mock.calls.filter((call) => call.method === 'eth_sendRawTransaction').length, 2);
  assert.equal(fetchCalls.length, httpBefore);
  assert.ok(result.telemetry.T4 < result.telemetry.T5);
  assert.ok(result.telemetry.T5 <= result.telemetry.T6);
  assert.ok(result.telemetry.T6 <= result.telemetry.T7);
  for (const url of RPCS) assert.ok(result.telemetry.rpc[url].dispatchMonoNs);
});

test('sufficient test collateral disables automatic broadcast', async () => {
  const { proposer, mock } = await prepare({ balance: 1000n });
  assert.equal(proposer.automaticEnabled, false);
  assert.match(proposer.readinessError, /collateral/i);
  const before = mock.calls.length;
  await proposer.handleFinal(finalFor('atpa'));
  assert.equal(mock.calls.slice(before).some((call) => call.method === 'eth_sendRawTransaction'), false);
});

test('invalid final cannot broadcast', async () => {
  const { proposer, mock } = await prepare();
  const before = mock.calls.length;
  const valid = finalFor('atpa');
  for (const invalid of [
    { ...valid, winnerId: null },
    { ...valid, winnerId: 'unknown' },
    { ...valid, loserId: 'unknown' },
    { ...valid, matchId: 'other-match' },
    { ...valid, status: 'In Progress' },
  ]) await proposer.handleFinal(invalid);
  assert.equal(mock.calls.slice(before).some((call) => call.method === 'eth_sendRawTransaction'), false);
});
