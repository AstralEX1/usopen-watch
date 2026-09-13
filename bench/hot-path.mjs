import { Wallet } from 'ethers';
import { selectNoTemplate } from '../proposer.mjs';

const finalists = [
  { playerId: 'atpa', playerName: 'Player A', templateKey: 'PLAYER_A_NO_TX_TEMPLATE' },
  { playerId: 'atpb', playerName: 'Player B', templateKey: 'PLAYER_B_NO_TX_TEMPLATE' },
];
const templates = {
  PLAYER_A_NO_TX_TEMPLATE: {
    chainId: 137,
    to: '0xee3afe347d5c74317041e2618c49534daf887c24',
    value: 0n,
    gasLimit: 350000n,
    data: `0x${'ab'.repeat(640)}`,
  },
  PLAYER_B_NO_TX_TEMPLATE: {
    chainId: 137,
    to: '0xee3afe347d5c74317041e2618c49534daf887c24',
    value: 0n,
    gasLimit: 350000n,
    data: `0x${'cd'.repeat(640)}`,
  },
};
const key = Wallet.createRandom().privateKey;
const wallet = new Wallet(key);

function percentile(samples, fraction) {
  const ordered = [...samples].sort((a, b) => a - b);
  return ordered[Math.min(ordered.length - 1, Math.floor((ordered.length - 1) * fraction))];
}

function report(name, samples) {
  return {
    name,
    samples: samples.length,
    p50Ns: percentile(samples, 0.5),
    p95Ns: percentile(samples, 0.95),
    p99Ns: percentile(samples, 0.99),
    p50Us: Number(percentile(samples, 0.5)) / 1e3,
    p95Us: Number(percentile(samples, 0.95)) / 1e3,
    p99Us: Number(percentile(samples, 0.99)) / 1e3,
  };
}

const selectionSamples = [];
for (let index = 0; index < 1_000; index += 1) selectNoTemplate(templates, finalists, 'atpb');
for (let index = 0; index < 20_000; index += 1) {
  const start = process.hrtime.bigint();
  selectNoTemplate(templates, finalists, index & 1 ? 'atpa' : 'atpb');
  selectionSamples.push(Number(process.hrtime.bigint() - start));
}

const signingTemplate = {
  ...templates.PLAYER_B_NO_TX_TEMPLATE,
  nonce: 7,
  gasPrice: 1_000_000_000n,
};
const signingSamples = [];
for (let index = 0; index < 100; index += 1) await wallet.signTransaction(signingTemplate);
for (let index = 0; index < 1_000; index += 1) {
  const start = process.hrtime.bigint();
  await wallet.signTransaction({ ...signingTemplate, nonce: index });
  signingSamples.push(Number(process.hrtime.bigint() - start));
}

console.log(JSON.stringify({
  node: process.version,
  selection: report('MATCH_FINAL -> loser -> prebuilt NO template', selectionSamples),
  signing: report('local transaction signing', signingSamples),
}, null, 2));
