import { Interface, Wallet, encodeBytes32String, getAddress } from 'ethers';
import { spawn } from 'node:child_process';
import { TERMINAL_STATUSES } from './final.mjs';

export const POLYGON_CHAIN_ID = 137;
export const GAMMA_EVENT_SLUG = '2026-mens-us-open-winner-tennis';
export const GAMMA_EVENT_URL = `https://gamma-api.polymarket.com/events?slug=${GAMMA_EVENT_SLUG}`;
export const NEG_RISK_UMA_ADAPTER = '0x2F5e3684cb1F318ec51b00Edba38d79Ac2c0aA9d';
export const YES_OR_NO_IDENTIFIER = encodeBytes32String('YES_OR_NO_QUERY');
export const NO_PRICE = 0n;
// ponytail: fixed 350k avoids eth_estimateGas in the result path; validate against a successful OOv2 receipt before production use.
export const DEFAULT_UMA_GAS_LIMIT = 350_000n;

export const ADAPTER_INTERFACE = [
  'function getQuestion(bytes32) view returns (tuple(uint256 requestTimestamp,uint256 reward,uint256 proposalBond,uint256 liveness,uint256 emergencyResolutionTimestamp,bool resolved,bool paused,bool reset,bool refund,address rewardToken,address creator,bytes ancillaryData))',
  'function optimisticOracle() view returns(address)',
];

export const OO_INTERFACE = [
  'function getState(address requester,bytes32 identifier,uint256 timestamp,bytes ancillaryData) view returns(uint8)',
  'function getRequest(address requester,bytes32 identifier,uint256 timestamp,bytes ancillaryData) view returns(tuple(address proposer,address disputer,address currency,bool settled,tuple(bool eventBased,bool refundOnDispute,bool callbackOnPriceProposed,bool callbackOnPriceDisputed,bool callbackOnPriceSettled,uint256 bond,uint256 customLiveness) requestSettings,int256 proposedPrice,int256 resolvedPrice,uint256 expirationTime,uint256 reward,uint256 finalFee))',
  'function proposePrice(address requester,bytes32 identifier,uint256 timestamp,bytes ancillaryData,int256 proposedPrice) returns(uint256)',
];

export const ERC20_INTERFACE = [
  'function balanceOf(address owner) view returns(uint256)',
];

const adapterInterface = new Interface(ADAPTER_INTERFACE);
const ooInterface = new Interface(OO_INTERFACE);
const erc20Interface = new Interface(ERC20_INTERFACE);

function monoNow() {
  return process.hrtime.bigint();
}

function wallNow() {
  return new Date().toISOString();
}

function hexNumber(value) {
  try {
    return BigInt(value);
  } catch {
    throw new Error(`Invalid hexadecimal RPC number: ${value}`);
  }
}

function isZeroAddress(value) {
  return !value || /^0x0{40}$/i.test(value);
}

function exactAddress(value, label) {
  if (isZeroAddress(value)) throw new Error(`${label} is zero`);
  try {
    return getAddress(value);
  } catch {
    throw new Error(`${label} is not an address`);
  }
}

function parseJsonField(value) {
  if (Array.isArray(value)) return value;
  if (typeof value !== 'string') return null;
  try {
    const parsed = JSON.parse(value);
    return Array.isArray(parsed) ? parsed : null;
  } catch {
    return null;
  }
}

function requireRequestId(market) {
  const requestId = market.negRiskRequestID ?? market.negRiskRequestId;
  if (typeof requestId !== 'string' || !/^0x[0-9a-f]{64}$/i.test(requestId)) {
    throw new Error(`Missing valid negRiskRequestID for ${market.question ?? 'market'}`);
  }
  return requestId;
}

function exactMarketForPlayer(markets, player) {
  const expectedQuestion = `Will ${player.name} win the 2026 Men's US Open?`;
  const matches = markets.filter((market) => market?.question === expectedQuestion);
  if (matches.length !== 1) {
    throw new Error(`Expected one exact Gamma market for ${player.name}; got ${matches.length}`);
  }
  const market = matches[0];
  const outcomes = parseJsonField(market.outcomes);
  if (!outcomes || outcomes.length !== 2 || !outcomes.includes('Yes') || !outcomes.includes('No')) {
    throw new Error(`Market for ${player.name} is not binary Yes/No`);
  }
  if (market.closed === true || market.active === false) {
    throw new Error(`Market for ${player.name} is not active`);
  }
  return market;
}

async function defaultRpcCall(url, method, params, fetchImpl) {
  const response = await fetchImpl(url, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ jsonrpc: '2.0', id: 1, method, params }),
  });
  if (response.ok === false || (response.status !== undefined && response.status >= 400)) {
    throw new Error(`RPC HTTP ${response.status ?? 'error'}`);
  }
  const body = await response.json();
  if (body.error) throw new Error(body.error.message ?? JSON.stringify(body.error));
  if (body.result === undefined) throw new Error(`RPC ${method} returned no result`);
  return body.result;
}

async function fetchGammaEvent({ gammaEvent, gammaEventUrl, fetchImpl }) {
  if (gammaEvent) {
    if (gammaEvent.slug !== GAMMA_EVENT_SLUG || !Array.isArray(gammaEvent.markets)) {
      throw new Error('Gamma event slug or markets did not verify');
    }
    return gammaEvent;
  }
  const response = await fetchImpl(gammaEventUrl, { headers: { accept: 'application/json' } });
  if (response.ok === false || (response.status !== undefined && response.status >= 400)) {
    throw new Error(`Gamma HTTP ${response.status ?? 'error'}`);
  }
  const body = await response.json();
  const event = Array.isArray(body) ? body.find((item) => item?.slug === GAMMA_EVENT_SLUG) : body;
  if (!event || event.slug !== GAMMA_EVENT_SLUG || !Array.isArray(event.markets)) {
    throw new Error('Gamma event slug or markets did not verify');
  }
  return event;
}

async function readQuestion({ rpc, rpcUrl, adapter, requestId }) {
  const data = adapterInterface.encodeFunctionData('getQuestion', [requestId]);
  const [question] = adapterInterface.decodeFunctionResult(
    'getQuestion',
    await rpc(rpcUrl, 'eth_call', [{ to: adapter, data }, 'latest']),
  );
  return question;
}

async function readRequest({ rpc, rpcUrl, adapter, oracle, requestId, question }) {
  const identifier = YES_OR_NO_IDENTIFIER;
  const args = [adapter, identifier, question.requestTimestamp, question.ancillaryData];
  const stateData = ooInterface.encodeFunctionData('getState', args);
  const requestData = ooInterface.encodeFunctionData('getRequest', args);
  const [stateRaw, requestRaw] = await Promise.all([
    rpc(rpcUrl, 'eth_call', [{ to: oracle, data: stateData }, 'latest']),
    rpc(rpcUrl, 'eth_call', [{ to: oracle, data: requestData }, 'latest']),
  ]);
  const state = Number(ooInterface.decodeFunctionResult('getState', stateRaw)[0]);
  const [request] = ooInterface.decodeFunctionResult('getRequest', requestRaw);
  if (state !== 1) throw new Error(`UMA request ${requestId} is not Requested (state ${state})`);
  if (isZeroAddress(request.currency)) throw new Error(`UMA request ${requestId} has no collateral`);
  if (request.currency.toLowerCase() !== question.rewardToken.toLowerCase()) {
    throw new Error(`UMA collateral mismatch for ${requestId}`);
  }
  if (request.requestSettings.bond !== question.proposalBond) {
    throw new Error(`UMA bond mismatch for ${requestId}`);
  }
  if (request.reward !== question.reward) {
    throw new Error(`UMA reward mismatch for ${requestId}`);
  }
  return { state, request };
}

function buildTemplate({ adapter, oracle, question, templateKey, gasLimit }) {
  const data = ooInterface.encodeFunctionData('proposePrice', [
    adapter,
    YES_OR_NO_IDENTIFIER,
    question.requestTimestamp,
    question.ancillaryData,
    NO_PRICE,
  ]);
  return {
    templateKey,
    chainId: POLYGON_CHAIN_ID,
    to: oracle,
    value: 0n,
    data,
    gasLimit,
  };
}

export function selectNoTemplate(templates, finalists, loserId) {
  const index = finalists.findIndex((player) => String(player.id ?? player.playerId) === String(loserId));
  if (index < 0) return null;
  return templates[index === 0 ? 'PLAYER_A_NO_TX_TEMPLATE' : 'PLAYER_B_NO_TX_TEMPLATE'] ?? null;
}

function assertFinalists(finalists) {
  if (!Array.isArray(finalists) || finalists.length !== 2) {
    throw new Error('Exactly two official finalists are required');
  }
  const ids = finalists.map((player) => String(player?.id ?? ''));
  const names = finalists.map((player) => String(player?.name ?? ''));
  if (ids.some((id) => !id) || names.some((name) => !name) || new Set(ids).size !== 2) {
    throw new Error('Finalist IDs and names must be exact and unique');
  }
}

async function healthCheck({ rpc, rpcUrls }) {
  const checks = await Promise.all(rpcUrls.map(async (url) => {
    try {
      const [chainId, blockNumber] = await Promise.all([
        rpc(url, 'eth_chainId', []),
        rpc(url, 'eth_blockNumber', []),
      ]);
      if (Number(hexNumber(chainId)) !== POLYGON_CHAIN_ID) throw new Error(`wrong chain ${chainId}`);
      return { url, chainId: Number(hexNumber(chainId)), blockNumber: hexNumber(blockNumber), healthy: true };
    } catch (error) {
      return { url, healthy: false, error: error instanceof Error ? error.message : String(error) };
    }
  }));
  const healthy = checks.filter((check) => check.healthy).map((check) => check.url);
  if (!healthy.length) throw new Error('No healthy Polygon RPC endpoint');
  return { checks, healthy };
}

function parsePrivateKey(value) {
  if (value === null || value === undefined || value === '') return null;
  if (typeof value !== 'string' || !/^0x[0-9a-f]{64}$/i.test(value)) {
    throw new Error('TEST_PRIVATE_KEY must be a 32-byte hex key');
  }
  return new Wallet(value);
}

function autoTestRequested(value) {
  return value === true || String(value ?? '').toLowerCase() === 'true';
}

function formatAmount(value) {
  return value === null || value === undefined ? 'unavailable' : value.toString();
}

function updateLatency(entry) {
  const latencyNs = {};
  if (entry.T1 !== null && entry.T1 !== undefined && entry.T2 !== null && entry.T2 !== undefined) {
    latencyNs.observerProcessing = entry.T2 - entry.T1;
  }
  if (entry.T2 !== null && entry.T2 !== undefined && entry.T3 !== null && entry.T3 !== undefined) {
    latencyNs.observerToProposer = entry.T3 - entry.T2;
  }
  if (entry.T4 !== undefined && entry.T5 !== undefined) latencyNs.signing = entry.T5 - entry.T4;
  if (entry.T2 !== null && entry.T2 !== undefined && entry.T6 !== undefined) {
    latencyNs.matchFinalToRpcSend = entry.T6 - entry.T2;
  }
  if (entry.T2 !== null && entry.T2 !== undefined && entry.T7 !== undefined) {
    latencyNs.matchFinalToRpcAccept = entry.T7 - entry.T2;
  }
  if (entry.T2 !== null && entry.T2 !== undefined && entry.T8 !== undefined) {
    latencyNs.matchFinalToInclusion = entry.T8 - entry.T2;
  }
  entry.latencyNs = latencyNs;
}

export function formatReadiness(prepared) {
  const mode = prepared.automaticEnabled ? 'ARMED (test wallet only)' : 'DISABLED';
  const error = prepared.readinessError ? `\nSAFETY ERROR: ${prepared.readinessError}` : '';
  return [
    'US Open 2026 Men\'s Singles final observer',
    `MATCH: ${prepared.matchId ?? 'waiting for official final snapshot'}`,
    `RPC: ${prepared.rpcUrls.length} healthy endpoint(s)`,
    `UMA REQUESTS: ${prepared.finalists?.map((candidate) => `${candidate.playerName}=${candidate.requestId}`).join(', ') ?? 'verified'}`,
    'NO TEMPLATES: ready',
    `TEST WALLET: ${prepared.walletAddress ?? 'not configured'}`,
    `AUTOMATIC TEST PROPOSAL: ${mode}`,
    `COLLATERAL: ${formatAmount(prepared.collateralBalance)} / required minimum ${formatAmount(prepared.minimumRequiredCollateral)}`,
    error,
  ].join('\n');
}

function formatManualNotice(final, candidate, template) {
  return [
    '',
    'MATCH FINAL',
    '',
    `WINNER: ${final.winnerName}`,
    `LOSER:  ${final.loserName}`,
    `STATUS: ${final.status}`,
    '',
    'MANUAL UMA ACTION:',
    `PROPOSE NO → ${final.loserName}`,
    '',
    `Question: ${candidate.question}`,
    `Requester: ${candidate.requester}`,
    `Identifier: ${candidate.identifier}`,
    `Request timestamp: ${candidate.requestTimestamp}`,
    `Ancillary data: ${candidate.ancillaryData}`,
    'Proposed price: NO',
    `OOv2 contract: ${candidate.ooV2}`,
    `Calldata: ${template.data}`,
    '',
  ].join('\n');
}

function nonBlockingAudio() {
  process.stdout.write('\x07');
  if (process.platform !== 'win32') return;
  try {
    const child = spawn('powershell.exe', [
      '-NoProfile', '-NonInteractive', '-Command', '[console]::beep(880,180)',
    ], { stdio: 'ignore', windowsHide: true });
    child.unref();
  } catch {
    // The terminal bell is the required fallback.
  }
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export async function prepareProposer({
  finalists,
  matchId = null,
  rpcUrls = [],
  fetchImpl = globalThis.fetch,
  rpcCall = null,
  gammaEvent = null,
  gammaEventUrl = GAMMA_EVENT_URL,
  adapter = process.env.POLYMARKET_UMA_ADAPTER || NEG_RISK_UMA_ADAPTER,
  privateKey = process.env.TEST_PRIVATE_KEY || null,
  autoTest = process.env.AUTO_TEST_PROPOSAL || false,
  gasLimit = process.env.UMA_GAS_LIMIT || DEFAULT_UMA_GAS_LIMIT,
  receiptPollMs = 1000,
  receiptMaxAttempts = 120,
  emitUi = true,
} = {}) {
  assertFinalists(finalists);
  if (typeof fetchImpl !== 'function') throw new Error('fetch is required for Gamma startup verification');
  const urls = [...new Set(rpcUrls.filter(Boolean))];
  if (!urls.length) throw new Error('POLYGON_RPC_1 is required');
  const normalizedAdapter = exactAddress(adapter, 'UMA adapter');
  const fixedGasLimit = BigInt(gasLimit);
  if (fixedGasLimit < 100_000n || fixedGasLimit > 2_000_000n) throw new Error('UMA_GAS_LIMIT is outside the safe range');
  const rpc = rpcCall ?? ((url, method, params) => defaultRpcCall(url, method, params, fetchImpl));

  const { checks, healthy } = await healthCheck({ rpc, rpcUrls: urls });
  const event = await fetchGammaEvent({ gammaEvent, gammaEventUrl, fetchImpl });
  const markets = event.markets;
  const selectedMarkets = finalists.map((player) => exactMarketForPlayer(markets, player));
  const requestIds = selectedMarkets.map(requireRequestId);
  if (new Set(requestIds.map((id) => id.toLowerCase())).size !== 2) throw new Error('Finalist UMA request IDs are not unique');

  const primaryRpc = healthy[0];
  const oracleData = adapterInterface.encodeFunctionData('optimisticOracle', []);
  const oracleRaw = await rpc(primaryRpc, 'eth_call', [{ to: normalizedAdapter, data: oracleData }, 'latest']);
  const oracle = exactAddress(adapterInterface.decodeFunctionResult('optimisticOracle', oracleRaw)[0], 'OOv2 contract');

  const candidates = [];
  for (let index = 0; index < finalists.length; index += 1) {
    const player = finalists[index];
    const market = selectedMarkets[index];
    const question = await readQuestion({ rpc, rpcUrl: primaryRpc, adapter: normalizedAdapter, requestId: requestIds[index] });
    if (hexNumber(question.requestTimestamp) <= 0n) throw new Error(`UMA request ${requestIds[index]} is uninitialized`);
    const rewardToken = exactAddress(question.rewardToken, 'collateral token');
    const { state, request } = await readRequest({
      rpc,
      rpcUrl: primaryRpc,
      adapter: normalizedAdapter,
      oracle,
      requestId: requestIds[index],
      question,
    });
    candidates.push({
      playerId: String(player.id),
      playerName: String(player.name),
      question: market.question,
      marketId: market.id ?? null,
      conditionId: market.conditionId ?? null,
      requester: normalizedAdapter,
      identifier: YES_OR_NO_IDENTIFIER,
      requestId: requestIds[index],
      requestTimestamp: hexNumber(question.requestTimestamp),
      ancillaryData: question.ancillaryData,
      proposedPrice: NO_PRICE,
      ooV2: oracle,
      collateralToken: rewardToken,
      proposalBond: request.requestSettings.bond,
      finalFee: request.finalFee,
      requiredCollateral: request.requestSettings.bond + request.finalFee,
      requestState: state,
      templateKey: index === 0 ? 'PLAYER_A_NO_TX_TEMPLATE' : 'PLAYER_B_NO_TX_TEMPLATE',
    });
  }

  const templates = Object.fromEntries(candidates.map((candidate) => [
    candidate.templateKey,
    buildTemplate({
      adapter: normalizedAdapter,
      oracle,
      question: {
        requestTimestamp: candidate.requestTimestamp,
        ancillaryData: candidate.ancillaryData,
      },
      templateKey: candidate.templateKey,
      gasLimit: fixedGasLimit,
    }),
  ]));

  const signer = parsePrivateKey(privateKey);
  let walletAddress = null;
  let collateralBalance = null;
  let nativeBalance = null;
  let gasPrice = null;
  let nonce = null;
  let readinessError = null;
  if (signer) {
    walletAddress = signer.address;
    [gasPrice, nonce, nativeBalance] = await Promise.all([
      rpc(primaryRpc, 'eth_gasPrice', []).then(hexNumber),
      rpc(primaryRpc, 'eth_getTransactionCount', [walletAddress, 'pending']).then(hexNumber),
      rpc(primaryRpc, 'eth_getBalance', [walletAddress, 'latest']).then(hexNumber),
    ]);
    const tokenData = erc20Interface.encodeFunctionData('balanceOf', [walletAddress]);
    const balanceRaw = await rpc(primaryRpc, 'eth_call', [{ to: candidates[0].collateralToken, data: tokenData }, 'latest']);
    collateralBalance = hexNumber(erc20Interface.decodeFunctionResult('balanceOf', balanceRaw)[0]);
  }

  const minimumRequiredCollateral = candidates.reduce(
    (minimum, candidate) => minimum === null || candidate.requiredCollateral < minimum
      ? candidate.requiredCollateral
      : minimum,
    null,
  );
  const automaticRequested = autoTestRequested(autoTest);
  let automaticEnabled = automaticRequested && Boolean(signer);
  if (signer && collateralBalance >= minimumRequiredCollateral) {
    automaticEnabled = false;
    readinessError = 'test wallet has enough proposal collateral; automatic broadcast disabled';
  } else if (automaticRequested && !signer) readinessError = 'TEST_PRIVATE_KEY is not configured';
  else if (automaticEnabled && nativeBalance < fixedGasLimit * gasPrice) {
    automaticEnabled = false;
    readinessError = 'test wallet does not have enough native Polygon gas token';
  }
  if (!automaticRequested && signer && !readinessError) readinessError = 'AUTO_TEST_PROPOSAL is false';

  let nonceState = nonce;
  const handledMatches = new Set();
  const byPlayerId = new Map(candidates.map((candidate) => [candidate.playerId, candidate]));
  const telemetry = [];

  async function sendRaw(url, rawTransaction, entry) {
    const timing = {
      endpoint: url,
      dispatchMonoNs: monoNow(),
      dispatchWallTime: wallNow(),
    };
    entry.rpc[url] = timing;
    try {
      const hash = await rpc(url, 'eth_sendRawTransaction', [rawTransaction]);
      if (typeof hash !== 'string' || !/^0x[0-9a-f]{64}$/i.test(hash)) throw new Error('invalid transaction hash');
      timing.responseMonoNs = monoNow();
      timing.responseWallTime = wallNow();
      timing.txHash = hash;
      return { url, hash, responseMonoNs: timing.responseMonoNs };
    } catch (error) {
      timing.responseMonoNs = monoNow();
      timing.responseWallTime = wallNow();
      timing.error = error instanceof Error ? error.message : String(error);
      return { url, error: timing.error };
    }
  }

  async function monitorReceipt(hash, entry) {
    for (let attempt = 0; attempt < receiptMaxAttempts; attempt += 1) {
      const receipts = await Promise.all(healthy.map(async (url) => {
        try {
          return await rpc(url, 'eth_getTransactionReceipt', [hash]);
        } catch {
          return null;
        }
      }));
      const receipt = receipts.find(Boolean);
      if (receipt) {
        entry.T8 = monoNow();
        entry.T8WallTime = wallNow();
        entry.receipt = { status: receipt.status ?? null, blockNumber: receipt.blockNumber ?? null };
        updateLatency(entry);
        return receipt;
      }
      await sleep(receiptPollMs);
    }
    return null;
  }

  async function submit(final, candidate, template, entry) {
    if (!automaticEnabled) return { status: 'automatic-disabled', templateKey: candidate.templateKey, telemetry: entry };
    entry.T4 = monoNow();
    entry.T4WallTime = wallNow();
    const rawTransaction = await signer.signTransaction({
      ...template,
      nonce: nonceState,
      gasPrice,
    });
    entry.T5 = monoNow();
    entry.T5WallTime = wallNow();
    updateLatency(entry);
    entry.nonce = nonceState;
    entry.signedBytes = (rawTransaction.length - 2) / 2;
    nonceState += 1n;
    entry.T6 = monoNow();
    entry.T6WallTime = wallNow();
    updateLatency(entry);
    const sends = healthy.map((url) => sendRaw(url, rawTransaction, entry));
    const allSends = Promise.all(sends);
    const firstAccepted = await Promise.any(sends.map((send) => send.then((result) => {
      if (result.error) throw new Error(result.error);
      return result;
    }))).catch((error) => {
      entry.broadcastError = error instanceof Error ? error.message : String(error);
      return null;
    });
    void allSends.then((results) => { entry.rpcResults = results; });
    if (!firstAccepted) return { status: 'broadcast-failed', templateKey: candidate.templateKey, telemetry: entry };
    entry.T7 = firstAccepted.responseMonoNs;
    entry.T7WallTime = wallNow();
    entry.txHash = firstAccepted.hash;
    updateLatency(entry);
    void monitorReceipt(firstAccepted.hash, entry).catch((error) => {
      entry.receiptError = error instanceof Error ? error.message : String(error);
    });
    return {
      status: 'broadcast-accepted',
      txHash: firstAccepted.hash,
      templateKey: candidate.templateKey,
      telemetry: entry,
    };
  }

  function handleFinal(final) {
    const T3 = monoNow();
    const candidate = byPlayerId.get(String(final?.loserId ?? ''));
    const winner = byPlayerId.get(String(final?.winnerId ?? ''));
    const template = selectNoTemplate(templates, candidates, final?.loserId);
    if (!candidate || !winner || winner.playerId === candidate.playerId || !template
      || final?.type !== 'MATCH_FINAL'
      || (matchId !== null && String(final.matchId) !== String(matchId))
      || !TERMINAL_STATUSES.has(String(final.status ?? '').toLowerCase())) {
      return Promise.resolve({ status: 'ignored-unknown-final' });
    }
    const matchKey = String(final.matchId);
    if (handledMatches.has(matchKey)) return Promise.resolve({ status: 'duplicate-final' });
    handledMatches.add(matchKey);
    const entry = {
      T1: final.networkReceivedMonoNs ?? null,
      T2: final.detectedMonoNs ?? null,
      T3,
      T3WallTime: wallNow(),
      matchId: final.matchId,
      winnerId: final.winnerId,
      loserId: final.loserId,
      rpc: {},
    };
    telemetry.push(entry);
    const submission = submit(final, candidate, template, entry);
    if (emitUi) setImmediate(() => {
      try {
        nonBlockingAudio();
        process.stdout.write(formatManualNotice(final, candidate, template));
      } catch (error) {
        entry.alertError = error instanceof Error ? error.message : String(error);
      }
    });
    return submission;
  }

  return {
    ready: true,
    matchId,
    adapter: normalizedAdapter,
    oracle,
    rpcUrls: healthy,
    rpcChecks: checks,
    finalists: candidates,
    templates,
    PLAYER_A_NO_TX_TEMPLATE: templates.PLAYER_A_NO_TX_TEMPLATE,
    PLAYER_B_NO_TX_TEMPLATE: templates.PLAYER_B_NO_TX_TEMPLATE,
    walletAddress,
    collateralBalance,
    nativeBalance,
    gasPrice,
    cachedPendingNonce: nonce,
    minimumRequiredCollateral,
    automaticEnabled,
    readinessError,
    telemetry,
    handleFinal,
    formatReadiness: () => formatReadiness({
      matchId,
      rpcUrls: healthy,
      finalists: candidates,
      walletAddress,
      collateralBalance,
      minimumRequiredCollateral,
      automaticEnabled,
      readinessError,
    }),
  };
}
