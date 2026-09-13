import { once } from 'node:events';
import { mkdir, writeFile } from 'node:fs/promises';
import { createWriteStream } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import {
  MqttDecoder,
  bytesToBase64,
  copyBytes,
  decodePayloadBytes,
  encodeConnect,
  encodeDisconnect,
  encodePingReq,
  encodeSubscribe,
  parseConnAck,
  parseSubAck,
} from './mqtt.mjs';
import {
  BoundedEventQueue,
  createReceiver,
  parseDerivedEvent,
  serialiseRawEvent,
  toJsonSafe,
} from './observer.mjs';
import { FinalDetector, detectMatchEndImminent, waitForMensSinglesFinalists } from './final.mjs';
import { formatReadiness, prepareProposer } from './proposer.mjs';
import { readRawEvents, writeReports } from './report.mjs';

export const MQTT_URL = 'wss://scores.usopen.org:443/mqtt2';
export const MQTT_FILTER = 'events/tennis/2026/uso/score/+';
export const HTTP_URL = 'https://www.usopen.org/en_US/scores/feeds/2026/matches/live/scores.json';
export const DEFAULT_HTTP_INTERVAL_MS = 500;
export const DEFAULT_SETTLE_MS = 10_000;
export const DEFAULT_QUEUE_SIZE = 50_000;

const moduleSequence = { value: 0 };

export function monoNow() {
  return process.hrtime.bigint();
}

function utcNow() {
  return new Date().toISOString();
}

function safeString(value) {
  return value === undefined || value === null ? null : String(value);
}

function jsonLine(value) {
  return `${JSON.stringify(toJsonSafe(value))}\n`;
}

function nextPacketId(state) {
  state.value = state.value >= 0xffff ? 1 : state.value + 1;
  return state.value;
}

export function buildMqttConnection({
  connectionId,
  phase,
  filters,
  recorder,
  send = () => {},
  sequence = moduleSequence,
  onConnAck = () => {},
  onSubAck = () => {},
  onHotEvent = null,
}) {
  const packetIds = { value: 0 };
  let sender = send;
  const subscribePacketId = nextPacketId(packetIds);
  const lifecycle = {
    connectionId,
    webSocketConnectStart: null,
    webSocketEstablished: null,
    mqttConnectSent: null,
    connackReceived: null,
    subscribeSent: null,
    subackReceived: null,
    firstPublishReceived: null,
    disconnectReceived: null,
    reconnectStarted: null,
    reconnectEstablished: null,
    requestedFilters: filters,
    requestedQos: filters.map((filter) => typeof filter === 'string' ? 0 : filter.qos),
    grantedQos: null,
    rejectedQos: [],
    connack: null,
    suback: null,
    errors: [],
  };

  const receiver = createReceiver({
    connectionId,
    phase,
    queue: recorder.queue,
    send: (...args) => sender(...args),
    sequence,
    onOverflow: recorder.onOverflow,
    onEnqueue: recorder.onEnqueue,
    onControlPacket: handleControlPacket,
    onHotEvent,
  });

  function sendConnect(clientId = `usopen-watch-${connectionId}`) {
    lifecycle.mqttConnectSent = monoNow();
    sender(encodeConnect(clientId, 30));
  }

  function sendSubscribe() {
    // The receiver was constructed before this call; PUBLISH can be handled immediately.
    lifecycle.subscribeSent = monoNow();
    sender(encodeSubscribe(subscribePacketId, filters));
  }

  function handleControlPacket(packet) {
    if (packet.type === 'malformed' || packet.type === 'malformed-publish') {
      lifecycle.errors.push({ ...packet, atMonoNs: monoNow() });
      return;
    }
    if (packet.type === 2) {
      let connack;
      try {
        connack = parseConnAck(packet.body);
      } catch (error) {
        lifecycle.errors.push({ type: 'connack-parse', error: error.message });
        return;
      }
      lifecycle.connackReceived = packet.receivedMonoNs ?? monoNow();
      lifecycle.connack = connack;
      if (connack.returnCode !== 0) {
        lifecycle.errors.push({ type: 'connack', returnCode: connack.returnCode });
      } else {
        onConnAck(connack, connection);
      }
      return;
    }
    if (packet.type === 9) {
      let suback;
      try {
        suback = parseSubAck(packet.body);
      } catch (error) {
        lifecycle.errors.push({ type: 'suback-parse', error: error.message });
        return;
      }
      lifecycle.subackReceived = packet.receivedMonoNs ?? monoNow();
      lifecycle.suback = suback;
      lifecycle.grantedQos = suback.grantedQos;
      lifecycle.rejectedQos = suback.grantedQos.filter((qos) => qos === 0x80);
      if (lifecycle.rejectedQos.length) {
        lifecycle.errors.push({
          type: 'suback-rejected',
          packetId: suback.packetId,
          grantedQos: suback.grantedQos,
        });
      }
      onSubAck(suback, connection);
      return;
    }
    recorder.onControl?.(packet, lifecycle);
  }

  function handleBytes(bytes) {
    receiver.onWebSocketMessage(bytes);
    if (lifecycle.firstPublishReceived === null && receiver.state.firstPublishRecvMonoNs !== null) {
      lifecycle.firstPublishReceived = receiver.state.firstPublishRecvMonoNs;
    }
  }

  function markWebSocketConnectStart() {
    lifecycle.webSocketConnectStart = monoNow();
  }

  function markWebSocketEstablished() {
    lifecycle.webSocketEstablished = monoNow();
  }

  function markDisconnected(code = null, reason = '') {
    lifecycle.disconnectReceived = monoNow();
    lifecycle.disconnectCode = code;
    lifecycle.disconnectReason = reason;
  }

  const connection = {
    connectionId,
    lifecycle,
    receiver,
    sendConnect,
    sendSubscribe,
    handleBytes,
    markWebSocketConnectStart,
    markWebSocketEstablished,
    markDisconnected,
    setSend: (nextSend) => { sender = nextSend; },
    decoder: new MqttDecoder(),
  };
  return connection;
}

export function recordHttpResponse({
  requestStartMonoNs,
  headersReceivedMonoNs,
  bodyCompleteMonoNs,
  parsedDetectedMonoNs = null,
  url,
  responseHeaders,
  status,
  body,
  bodyBytes = typeof body === 'string' ? new TextEncoder().encode(body) : copyBytes(body),
  recvSeq = null,
  error = null,
}) {
  const headers = responseHeaders ?? {};
  const rawBody = typeof body === 'string'
    ? { payloadRaw: body, payloadBytesBase64: null, encoding: 'utf8' }
    : decodePayloadBytes(bodyBytes);
  return {
    recvSeq,
    recvMonoNs: bodyCompleteMonoNs,
    recvUtc: new Date().toISOString(),
    source: 'http',
    phase: 'http',
    connectionId: null,
    url,
    status,
    date: headers.date ?? null,
    age: headers.age ?? null,
    etag: headers.etag ?? null,
    cacheControl: headers.cacheControl ?? null,
    contentLength: headers.contentLength ?? null,
    contentSize: bodyBytes.length,
    requestStartMonoNs,
    headersReceivedMonoNs,
    bodyCompleteMonoNs,
    parsedDetectedMonoNs,
    payloadBytes: bodyBytes,
    payloadRaw: rawBody.payloadRaw,
    payloadBytesBase64: rawBody.payloadBytesBase64,
    rawResponseBody: rawBody.payloadRaw,
    rawResponseBodyBase64: rawBody.payloadBytesBase64,
    rawResponseBodyEncoding: rawBody.encoding,
    error,
  };
}

async function pollHttp({ url, matchId, queue, sequence, onOverflow, onHotEvent }) {
  const requestStartMonoNs = monoNow();
  try {
    const response = await fetch(url, {
      headers: {
        accept: 'application/json',
        'cache-control': 'no-cache',
      },
    });
    const headersReceivedMonoNs = monoNow();
    const bodyBytes = new Uint8Array(await response.arrayBuffer());
    const bodyCompleteMonoNs = monoNow();
    const responseHeaders = {
      date: response.headers.get('date'),
      age: response.headers.get('age'),
      etag: response.headers.get('etag'),
      cacheControl: response.headers.get('cache-control'),
      contentLength: response.headers.get('content-length'),
    };
    const event = recordHttpResponse({
      requestStartMonoNs,
      headersReceivedMonoNs,
      bodyCompleteMonoNs,
      url,
      responseHeaders,
      status: response.status,
      bodyBytes,
      body: bodyBytes,
      recvSeq: ++sequence.value,
    });
    let parsed = null;
    try {
      const decoded = new TextDecoder('utf-8', { fatal: true }).decode(bodyBytes);
      parsed = JSON.parse(decoded);
      event.payloadRaw = decoded;
      event.payloadBytesBase64 = null;
      event.parsedDetectedMonoNs = monoNow();
    } catch {
      event.parsedDetectedMonoNs = monoNow();
    }
    event.matchId = matchId;
    event.httpParsed = parsed;
    event.payload = parsed;
    if (onHotEvent) {
      try {
        const derived = onHotEvent(event);
        if (derived && typeof derived === 'object') event._hotDerived = derived;
      } catch (error) {
        event.hotPathError = error instanceof Error ? error.message : String(error);
      }
    }
    if (!queue.push(event)) onOverflow?.({ source: 'http', url, recvSeq: event.recvSeq });
    return event;
  } catch (error) {
    const bodyCompleteMonoNs = monoNow();
    const event = recordHttpResponse({
      requestStartMonoNs,
      headersReceivedMonoNs: bodyCompleteMonoNs,
      bodyCompleteMonoNs,
      parsedDetectedMonoNs: bodyCompleteMonoNs,
      url,
      status: null,
      responseHeaders: {},
      body: '',
      recvSeq: ++sequence.value,
      error: error instanceof Error ? error.message : String(error),
    });
    event.matchId = matchId;
    if (!queue.push(event)) onOverflow?.({ source: 'http', url, recvSeq: event.recvSeq });
    return event;
  }
}

class AsyncEventPipeline {
  constructor({ filePath, queue, matchId, onDerived, onOverflow }) {
    this.filePath = filePath;
    this.queue = queue;
    this.matchId = String(matchId);
    this.onDerived = onDerived;
    this.onOverflow = onOverflow;
    this.stream = null;
    this.stopRequested = false;
    this.wake = null;
    this.task = null;
    this.writeError = null;
  }

  async start() {
    await mkdir(dirname(this.filePath), { recursive: true });
    this.stream = createWriteStream(this.filePath, { flags: 'a' });
    this.task = this.consume();
  }

  notify() {
    this.wake?.();
    this.wake = null;
  }

  async write(value) {
    if (!this.stream.write(jsonLine(value))) await once(this.stream, 'drain');
  }

  async consume() {
    while (!this.stopRequested || this.queue.size > 0) {
      const raw = this.queue.drainOne();
      if (!raw) {
        await new Promise((resolvePromise) => { this.wake = resolvePromise; });
        continue;
      }
      try {
        const derived = parseDerivedEvent(raw, this.matchId);
        this.onDerived?.(derived, raw);
        await this.write(serialiseRawEvent(derived));
      } catch (error) {
        this.writeError = error instanceof Error ? error.message : String(error);
        this.onOverflow?.({ source: 'writer', error: this.writeError });
      }
    }
  }

  async stop() {
    this.stopRequested = true;
    this.notify();
    await this.task;
    await new Promise((resolvePromise, reject) => {
      this.stream.end((error) => error ? reject(error) : resolvePromise());
    });
  }
}

function attachSocket({ url, connection, onClose }) {
  return new Promise((resolvePromise, reject) => {
    connection.markWebSocketConnectStart();
    const ws = new WebSocket(url, 'mqtt');
    connection.setSend((bytes) => ws.send(bytes));
    ws.binaryType = 'arraybuffer';
    let opened = false;
    ws.addEventListener('open', () => {
      opened = true;
      connection.markWebSocketEstablished();
      connection.sendConnect();
      resolvePromise(ws);
    });
    ws.addEventListener('message', (event) => {
      try {
        connection.handleBytes(event.data);
      } catch (error) {
        connection.lifecycle.errors.push({
          type: 'message-handler',
          error: error instanceof Error ? error.stack : String(error),
        });
      }
    });
    ws.addEventListener('error', (event) => {
      connection.lifecycle.errors.push({ type: 'websocket', message: event.error?.message ?? 'WebSocket error' });
      if (!opened) reject(new Error('MQTT WebSocket connection failed'));
    });
    ws.addEventListener('close', (event) => {
      connection.markDisconnected(event.code, event.reason);
      onClose?.();
    });
  });
}

function delay(ms) {
  return new Promise((resolvePromise) => setTimeout(resolvePromise, ms));
}

async function startReconnectProbe({ mqttUrl, filters, recorder, sequence, connections, sockets, control, onHotEvent }) {
  const connect = (attempt) => {
    const connection = buildMqttConnection({
      connectionId: 'reconnect-probe',
      phase: 'probe',
      filters,
      recorder,
      sequence,
      onConnAck: (_connack, current) => current.sendSubscribe(),
      onHotEvent,
    });
    connection.lifecycle.attempt = attempt;
    connections.push(connection.lifecycle);
    return connection;
  };

  let first = connect('initial');
  try {
    const firstWs = await attachSocket({ url: mqttUrl, connection: first });
    sockets.push(firstWs);
    await delay(1500);
    if (control.stopped) return;
    first.lifecycle.reconnectStarted = monoNow();
    try { firstWs.send(encodeDisconnect()); } catch {}
    try { firstWs.close(); } catch {}
    await delay(200);
    if (control.stopped) return;

    const second = connect('reconnect');
    second.lifecycle.reconnectStarted = first.lifecycle.reconnectStarted;
    const secondWs = await attachSocket({ url: mqttUrl, connection: second });
    sockets.push(secondWs);
    second.lifecycle.reconnectEstablished = second.lifecycle.webSocketEstablished;
    await delay(1000);
    try { secondWs.send(encodeDisconnect()); } catch {}
    try { secondWs.close(); } catch {}
  } catch (error) {
    first.lifecycle.errors.push({ type: 'probe', error: error instanceof Error ? error.message : String(error) });
  }
}

async function writeJson(path, value) {
  await writeFile(path, `${JSON.stringify(toJsonSafe(value), null, 2)}\n`, 'utf8');
}

function parseArgs(argv) {
  const args = [...argv];
  const command = args.shift();
  const matchId = args[0] && !String(args[0]).startsWith('--') ? args.shift() : null;
  const options = {
    outputDir: null,
    httpIntervalMs: DEFAULT_HTTP_INTERVAL_MS,
    settleMs: DEFAULT_SETTLE_MS,
    queueSize: DEFAULT_QUEUE_SIZE,
    mqttUrl: MQTT_URL,
    httpUrls: [],
    reconnectProbe: false,
    startupPollMs: 1_000,
  };
  for (let index = 0; index < args.length; index += 1) {
    const arg = args[index];
    if (arg === '--output-dir') options.outputDir = resolve(args[++index]);
    else if (arg === '--http-interval-ms') options.httpIntervalMs = Number(args[++index]);
    else if (arg === '--settle-ms') options.settleMs = Number(args[++index]);
    else if (arg === '--queue-size') options.queueSize = Number(args[++index]);
    else if (arg === '--mqtt-url') options.mqttUrl = args[++index];
    else if (arg === '--http-url') options.httpUrls.push(args[++index]);
    else if (arg === '--reconnect-probe') options.reconnectProbe = true;
    else if (arg === '--startup-poll-ms') options.startupPollMs = Number(args[++index]);
  }
  if (!options.httpUrls.length) options.httpUrls.push(HTTP_URL);
  return { command, matchId, options };
}

function defaultOutputDir(matchId) {
  const stamp = new Date().toISOString().replace(/[-:.TZ]/g, '').slice(0, 14);
  return resolve('outputs', 'usopen-watch', 'runs', `${stamp}-${matchId}`);
}

export async function runObserve({
  matchId,
  proposer = null,
  onFinal = null,
  onImminent = null,
  outputDir = null,
  httpIntervalMs = DEFAULT_HTTP_INTERVAL_MS,
  settleMs = DEFAULT_SETTLE_MS,
  queueSize = DEFAULT_QUEUE_SIZE,
  mqttUrl = MQTT_URL,
  httpUrls = [HTTP_URL],
  reconnectProbe = false,
}) {
  if (!matchId) throw new Error('matchId is required');
  if (!/^[A-Za-z0-9_-]+$/.test(String(matchId))) throw new Error('matchId contains unsafe characters');
  outputDir ??= defaultOutputDir(matchId);
  await mkdir(outputDir, { recursive: true });
  const queue = new BoundedEventQueue(queueSize);
  const sequence = { value: 0 };
  const state = {
    matchId: String(matchId),
    startedUtc: utcNow(),
    startedMonoNs: monoNow(),
    completeness: 'running',
    warmStartGap: {
      present: true,
      reason: 'observer started after the selected match was already live; no historical stream assumed',
      fromUtc: utcNow(),
    },
    overflowCount: 0,
    droppedCount: 0,
    errors: [],
    result: null,
    final: null,
    httpWinnerSeen: false,
    terminalSeen: false,
  };
  let stopResolve;
  const stopSignal = new Promise((resolvePromise) => { stopResolve = resolvePromise; });
  const analyses = [];
  const connections = [];
  const finalHandler = onFinal ?? proposer?.handleFinal?.bind(proposer) ?? null;
  let hotEvent = null;
  const deliverFinal = (final) => {
    state.result = final;
    state.final = final;
    state.terminalSeen = true;
    state.proposerTelemetry = proposer?.telemetry ?? null;
    if (hotEvent) {
      hotEvent.latency ??= { W0: hotEvent.recvMonoNs, W1: monoNow() };
      hotEvent.latency.W2 = final.detectedMonoNs;
      analyses.push({
        recvSeq: hotEvent.recvSeq ?? null,
        connectionId: hotEvent.connectionId ?? null,
        topic: hotEvent.topic ?? null,
        W0: hotEvent.latency.W0,
        W1: hotEvent.latency.W1,
        W2: hotEvent.latency.W2,
        winnerId: final.winnerId,
        loserId: final.loserId,
        sourceTimestamp: final.sourceTimestamp,
      });
    }
    try {
      const result = finalHandler?.(final);
      if (result && typeof result.then === 'function') {
        result.catch((error) => state.errors.push({
          type: 'proposer',
          error: error instanceof Error ? error.message : String(error),
        }));
      }
    } catch (error) {
      state.errors.push({ type: 'proposer', error: error instanceof Error ? error.message : String(error) });
    }
    stopResolve('match-final');
  };
  const detector = new FinalDetector({
    targetMatchId: matchId,
    onFinal: deliverFinal,
    onError: (error) => state.errors.push({ type: 'final-callback', error: error.message }),
  });
  const onHotEvent = (event) => {
    const derived = parseDerivedEvent(event, matchId);
    const imminent = detectMatchEndImminent(derived, matchId);
    if (imminent) {
      try { onImminent?.(imminent); } catch (error) {
        state.errors.push({ type: 'imminent-callback', error: error instanceof Error ? error.message : String(error) });
      }
    }
    hotEvent = event;
    detector.accept(derived);
    hotEvent = null;
    return derived;
  };
  const overflow = (details) => {
    state.overflowCount = queue.overflowCount;
    state.droppedCount = queue.droppedCount;
    state.completeness = 'incomplete';
    state.errors.push({ type: 'queue_overflow', ...details, atUtc: utcNow() });
    stopResolve('queue-overflow');
  };
  const pipeline = new AsyncEventPipeline({
    filePath: join(outputDir, 'raw.ndjson'),
    queue,
    matchId,
    onOverflow: overflow,
    onDerived: (derived, raw) => {
      if (derived.matchId !== String(matchId)) return;
      const matchStatus = derived.matchStatus ?? derived.status;
      if (matchStatus && /completed|retired|retirement|default|walkover/i.test(matchStatus)) {
        state.terminalSeen = true;
      }
      detector.accept(derived);
    },
  });
  await pipeline.start();

  const matchFilters = [
    { filter: `events/tennis/2026/uso/score/${matchId}`, qos: 0 },
    { filter: `events/tennis/2026/uso/slamtracker/${matchId}`, qos: 0 },
  ];
  const primary = buildMqttConnection({
    connectionId: 'primary',
    phase: 'target',
    filters: matchFilters,
    recorder: { queue, onOverflow: overflow, onEnqueue: () => pipeline.notify(), onControl: (packet) => {
      if (packet.type === 'malformed') state.errors.push(packet);
    } },
    sequence,
    onHotEvent,
    onConnAck: (_connack, connection) => connection.sendSubscribe(),
    onSubAck: (suback) => {
      if (suback.grantedQos.some((qos) => qos === 0x80)) {
        state.completeness = 'incomplete';
        state.errors.push({ type: 'suback-rejected', grantedQos: suback.grantedQos });
        stopResolve('suback-rejected');
      }
    },
  });
  connections.push(primary.lifecycle);
  let primaryWs = null;
  let pingTimer = null;
  const probeSockets = [];
  const probeControl = { stopped: false };
  let probePromise = null;
  try {
    primaryWs = await attachSocket({
      url: mqttUrl,
      connection: primary,
      onClose: () => {
        if (!state.result && state.completeness === 'running') {
          state.completeness = 'incomplete';
          state.errors.push({ type: 'primary_disconnect', atUtc: utcNow() });
          stopResolve('primary-disconnect');
        }
      },
    });
    pingTimer = setInterval(() => {
      if (primaryWs.readyState === WebSocket.OPEN) primaryWs.send(encodePingReq());
    }, 15_000);
    if (reconnectProbe) {
      probePromise = startReconnectProbe({
        mqttUrl,
        filters: matchFilters,
        recorder: { queue, onOverflow: overflow, onEnqueue: () => pipeline.notify() },
        sequence,
        connections,
        sockets: probeSockets,
        control: probeControl,
        onHotEvent,
      });
    }
  } catch (error) {
    state.completeness = 'incomplete';
    state.errors.push({ type: 'primary-connect', error: error.message });
    stopResolve('primary-connect-failure');
  }

  const inFlight = new Set();
  const poll = () => {
    for (const url of httpUrls) {
      if (inFlight.has(url)) continue;
      inFlight.add(url);
      pollHttp({ url, matchId, queue, sequence, onOverflow: overflow, onHotEvent })
        .then((event) => {
          pipeline.notify();
          const derived = parseDerivedEvent(event, matchId);
          if (derived.matchId === String(matchId) && derived.winner) {
            state.httpWinnerSeen = true;
          }
        })
        .catch((error) => state.errors.push({ type: 'http-poll', url, error: error.message }))
        .finally(() => inFlight.delete(url));
    }
  };
  poll();
  const pollTimer = setInterval(poll, httpIntervalMs);

  if (!primaryWs) stopResolve('no-primary');
  const observerTimeout = setTimeout(() => stopResolve('observer-timeout'), 30 * 60 * 1000);
  const reason = await Promise.race([
    stopSignal,
  ]);
  clearTimeout(observerTimeout);
  clearInterval(pollTimer);
  clearInterval(pingTimer);
  probeControl.stopped = true;
  for (const socket of probeSockets) {
    if (socket.readyState === WebSocket.OPEN) {
      try { socket.send(encodeDisconnect()); } catch {}
      try { socket.close(); } catch {}
    }
  }
  await probePromise?.catch((error) => state.errors.push({ type: 'probe', error: error.message }));
  if (primaryWs && primaryWs.readyState === WebSocket.OPEN) {
    primary.markDisconnected();
    try { primaryWs.send(encodeDisconnect()); } catch {}
    try { primaryWs.close(); } catch {}
  }
  await pipeline.stop();
  moduleSequence.value = sequence.value;
  state.finishedUtc = utcNow();
  state.finishedMonoNs = monoNow();
  state.stopReason = reason;
  state.overflowCount = queue.overflowCount;
  state.droppedCount = queue.droppedCount;
  if (queue.incomplete || state.completeness === 'incomplete') state.completeness = 'incomplete';
  else if (state.result?.type === 'MATCH_FINAL' && state.terminalSeen) state.completeness = state.warmStartGap.present ? 'warm-start-partial' : 'complete';
  else if (state.result?.type === 'MATCH_FINAL') {
    state.completeness = 'incomplete';
    state.errors.push({ type: 'terminal_not_observed' });
  }
  else state.completeness = 'terminal-without-winner';

  const manifest = {
    observer: 'usopen-watch',
    version: 1,
    matchId: state.matchId,
    outputDir,
    mqttUrl,
    httpUrls,
    mqttProtocol: '3.1.1',
    requestedQos: 0,
    primaryConnectionId: 'primary',
    collection: state,
    connections: connections.map(toJsonSafe),
    analysisCount: analyses.length,
  };
  await writeJson(join(outputDir, 'manifest.json'), manifest);
  await writeFile(join(outputDir, 'connections.ndjson'), connections.map(jsonLine).join(''), 'utf8');
  await writeFile(join(outputDir, 'analysis.ndjson'), analyses.map(jsonLine).join(''), 'utf8');
  const rawEvents = await readRawEvents(join(outputDir, 'raw.ndjson'));
  const report = await writeReports({
    outputDir,
    rawEvents,
    connections,
    manifest,
    analyses,
    targetMatchId: matchId,
  });
  manifest.report = { eventCount: report.eventCount, signalRace: report.signalRace };
  await writeJson(join(outputDir, 'manifest.json'), manifest);
  return { ...state, outputDir, connections, analyses, report };
}

export async function main(argv = process.argv.slice(2)) {
  const { command, matchId, options } = parseArgs(argv);
  if (command !== 'observe') {
    throw new Error('Usage: usopen-watch observe [matchId] [--output-dir DIR] [--http-url URL] [--reconnect-probe]');
  }
  if (matchId && !/^[A-Za-z0-9_-]+$/.test(String(matchId))) throw new Error('matchId contains unsafe characters');
  const finalists = await waitForMensSinglesFinalists({
    url: options.httpUrls[0],
    matchId: matchId ?? null,
    intervalMs: options.startupPollMs,
  });
  const proposer = await prepareProposer({
    finalists: finalists.players.map(({ id, name }) => ({ id, name })),
    matchId: finalists.matchId,
    rpcUrls: [process.env.POLYGON_RPC_1, process.env.POLYGON_RPC_2, process.env.POLYGON_RPC_3],
    privateKey: process.env.TEST_PRIVATE_KEY || null,
    autoTest: process.env.AUTO_TEST_PROPOSAL || false,
    gasLimit: process.env.UMA_GAS_LIMIT || undefined,
  });
  process.stdout.write(`${formatReadiness(proposer)}\n`);
  const result = await runObserve({ matchId: finalists.matchId, proposer, ...options });
  process.stdout.write(`${JSON.stringify(toJsonSafe(result), null, 2)}\n`);
  return result;
}

const thisFile = fileURLToPath(import.meta.url);
if (process.argv[1] && pathToFileURL(resolve(process.argv[1])).href === pathToFileURL(thisFile).href) {
  main().catch((error) => {
    process.stderr.write(`${error.stack ?? error}\n`);
    process.exitCode = 1;
  });
}
