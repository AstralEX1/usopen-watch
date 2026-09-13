import {
  MqttDecoder,
  copyBytes,
  decodePayloadBytes,
  encodeAck,
  parsePacketId,
  parsePublish,
  bytesToBase64,
} from './mqtt.mjs';
import { gunzipSync, inflateSync } from 'node:zlib';

const strictTextDecoder = new TextDecoder('utf-8', { fatal: true });

const globalReceiveSequence = { value: 0 };

export class BoundedEventQueue {
  #items = [];

  constructor(maxItems) {
    if (!Number.isInteger(maxItems) || maxItems < 1) {
      throw new RangeError('Queue size must be a positive integer');
    }
    this.maxItems = maxItems;
    this.overflowCount = 0;
    this.droppedCount = 0;
    this.incomplete = false;
  }

  get size() {
    return this.#items.length;
  }

  get events() {
    return this.#items;
  }

  push(event) {
    if (this.#items.length >= this.maxItems) {
      this.overflowCount += 1;
      this.droppedCount += 1;
      this.incomplete = true;
      return false;
    }
    this.#items.push(event);
    return true;
  }

  drainOne() {
    return this.#items.shift() ?? null;
  }
}

function normaliseKey(key) {
  return String(key).replace(/[_-]/g, '').toLowerCase();
}

function findField(value, wantedKeys) {
  const wanted = new Set(wantedKeys.map(normaliseKey));
  const stack = [value];
  const seen = new WeakSet();
  let visited = 0;
  while (stack.length && visited < 20000) {
    const current = stack.pop();
    if (!current || typeof current !== 'object') continue;
    if (seen.has(current)) continue;
    seen.add(current);
    visited += 1;
    for (const [key, child] of Object.entries(current)) {
      if (wanted.has(normaliseKey(key))) return child;
      if (child && typeof child === 'object') stack.push(child);
    }
  }
  return undefined;
}

function asString(value) {
  if (value === undefined || value === null) return null;
  if (typeof value === 'string') return value;
  if (typeof value === 'number' || typeof value === 'boolean') return String(value);
  return null;
}

function findMatchRecord(value, targetMatchId) {
  if (!targetMatchId || !value || typeof value !== 'object') return null;
  const stack = [value];
  const seen = new WeakSet();
  while (stack.length) {
    const current = stack.pop();
    if (!current || typeof current !== 'object' || seen.has(current)) continue;
    seen.add(current);
    for (const [key, child] of Object.entries(current)) {
      if (normaliseKey(key) === 'matchid' && asString(child) === String(targetMatchId)) return current;
      if (child && typeof child === 'object') stack.push(child);
    }
  }
  return null;
}

function findMatchId(payload, topic, targetMatchId) {
  const direct = asString(findField(payload, ['matchId', 'matchID', 'match_id']));
  if (direct) return direct;
  if (targetMatchId && topic.includes(String(targetMatchId))) return String(targetMatchId);
  const topicId = topic.match(/(?:^|[/_:-])match(?:id)?[/=_:-]*(\d+)/i);
  return topicId?.[1] ?? null;
}

function findEventType(payload) {
  const direct = asString(findField(payload, ['eventType', 'event', 'type', 'name']));
  if (direct) return direct;
  const matchWinner = findField(payload, ['matchWinner']);
  if (matchWinner !== undefined) return 'MatchWinner';
  return null;
}

function isValidWinner(value) {
  const normalised = asString(value)?.trim();
  return normalised === '1' || normalised === '2' ? normalised : null;
}

export function parseDerivedEvent(rawEvent, targetMatchId = null) {
  if (rawEvent._hotDerived) return rawEvent._hotDerived;
  const bytes = rawEvent.payloadBytes instanceof Uint8Array
    ? rawEvent.payloadBytes
    : rawEvent._payloadBytes instanceof Uint8Array
      ? rawEvent._payloadBytes
      : null;
  const encoded = bytes ? decodePayloadBytes(bytes) : {
    payloadRaw: rawEvent.payloadRaw ?? null,
    payloadBytesBase64: rawEvent.payloadBytesBase64 ?? null,
    encoding: rawEvent.payloadBytesBase64 ? 'base64' : 'utf8',
  };
  let payload = rawEvent.payload && typeof rawEvent.payload === 'object' ? rawEvent.payload : null;
  let parseError = null;
  let payloadDerivedRaw = null;
  let payloadDerivedEncoding = null;
  if (payload === null && encoded.payloadRaw !== null) {
    try {
      payload = JSON.parse(encoded.payloadRaw);
    } catch (error) {
      parseError = error instanceof Error ? error.message : String(error);
    }
  }
  if (payload === null && rawEvent.source === 'mqtt' && bytes) {
    try {
      let inflated;
      try {
        inflated = gunzipSync(bytes);
      } catch {
        inflated = inflateSync(bytes);
      }
      payloadDerivedRaw = strictTextDecoder.decode(inflated);
      payload = JSON.parse(payloadDerivedRaw);
      payloadDerivedEncoding = 'deflate+utf8';
      parseError = null;
    } catch {
      // The raw transport value remains authoritative when no derived JSON exists.
    }
  }

  const matchPayload = findMatchRecord(payload, targetMatchId) ?? payload;
  const discoveredMatchId = rawEvent.source === 'http'
    ? findMatchId(matchPayload, rawEvent.url ?? '', targetMatchId)
    : findMatchId(matchPayload, rawEvent.topic ?? '', targetMatchId);
  const matchId = discoveredMatchId ?? asString(rawEvent.matchId);
  const winner = isValidWinner(findField(matchPayload, ['winner', 'matchWinner']))
    ?? (isValidWinner(rawEvent.winner));
  const eventType = findEventType(matchPayload) ?? rawEvent.eventType ?? null;
  const matchStatus = asString(findField(matchPayload, ['status'])) ?? asString(rawEvent.matchStatus);
  const status = rawEvent.source === 'http' ? rawEvent.status : matchStatus;
  const statusCode = asString(findField(matchPayload, ['statusCode', 'status_code']))
    ?? asString(rawEvent.statusCode);
  const sourceTimestamp = asString(findField(matchPayload, [
    'timestamp', 'eventTimestamp', 'sourceTimestamp', 'updatedAt', 'updated', 'time',
    'epochTimeStart', 'epochTimeEnd', 'createEpoch',
  ]));
  const sequence = asString(findField(matchPayload, [
    'sequence', 'seq', 'version', 'messageId', 'messageID',
    'pointId', 'pointNumber',
  ]));
  const phase = rawEvent.source === 'http'
    ? 'http'
    : rawEvent.connectionId === 'reconnect-probe'
      ? 'probe'
      : targetMatchId && matchId === String(targetMatchId)
        ? 'target'
        : rawEvent.phase ?? 'discovery';

  return {
    ...rawEvent,
    payloadRaw: encoded.payloadRaw,
    payloadBytesBase64: encoded.payloadBytesBase64,
    payloadEncoding: encoded.encoding,
    payloadDerivedRaw,
    payloadDerivedEncoding,
    payload,
    payloadBytes: bytes ? bytes.length : rawEvent.payloadBytes ?? null,
    matchId,
    eventType,
    matchStatus,
    winner,
    status,
    statusCode,
    sourceTimestamp,
    sequence,
    phase,
    parseError,
    _payloadBytes: bytes,
  };
}

export function createReceiver({
  connectionId,
  phase,
  queue,
  send,
  sequence = globalReceiveSequence,
  onControlPacket = () => {},
  onOverflow = () => {},
  onEnqueue = () => {},
  onHotEvent = null,
}) {
  const decoder = new MqttDecoder();
  const qos2Pending = new Set();
  const state = {
    connectionId,
    phase,
    firstPublishRecvMonoNs: null,
    malformedCount: 0,
    ackPackets: [],
    incomplete: false,
  };

  function sendAck(packetType, packetId) {
    send(encodeAck(packetType, packetId));
    state.ackPackets.push({ packetType, packetId, sentMonoNs: process.hrtime.bigint() });
  }

  function onWebSocketMessage(data) {
    const callbackSeq = ++sequence.value;
    const callbackMonoNs = process.hrtime.bigint();
    const callbackUtc = new Date().toISOString();
    const copiedFrame = copyBytes(data);
    let packets;
    try {
      packets = decoder.push(copiedFrame);
    } catch (error) {
      state.malformedCount += 1;
      onControlPacket({ type: 'malformed', error: error instanceof Error ? error.message : String(error) });
      return;
    }

    for (let index = 0; index < packets.length; index += 1) {
      const packet = packets[index];
      const recvSeq = index === 0 ? callbackSeq : ++sequence.value;
      if (packet.type === 3) {
        let publish;
        try {
          publish = parsePublish(packet);
        } catch (error) {
          state.malformedCount += 1;
          onControlPacket({ type: 'malformed-publish', error: error instanceof Error ? error.message : String(error) });
          continue;
        }
        const event = {
          recvSeq,
          recvMonoNs: callbackMonoNs,
          recvUtc: callbackUtc,
          source: 'mqtt',
          phase,
          connectionId,
          topic: publish.topic,
          qos: publish.qos,
          dup: publish.dup,
          retain: publish.retain,
          payloadBytes: publish.payloadBytes,
          mqttPacketBytes: packet.rawBytes,
          messageId: publish.packetId,
          latency: { W0: callbackMonoNs, W1: null },
        };
        if (state.firstPublishRecvMonoNs === null) state.firstPublishRecvMonoNs = callbackMonoNs;
        event.latency.W1 = process.hrtime.bigint();
        if (onHotEvent) {
          try {
            const derived = onHotEvent(event);
            if (derived && typeof derived === 'object') event._hotDerived = derived;
          } catch (error) {
            state.malformedCount += 1;
            onControlPacket({
              type: 'hot-event',
              error: error instanceof Error ? error.message : String(error),
            });
          }
        }
        const queued = queue.push(event);
        if (queued) onEnqueue(event);
        if (!queued) {
          state.incomplete = true;
          onOverflow({ recvSeq, connectionId, overflowCount: queue.overflowCount });
        }
        if (publish.qos === 1 && publish.packetId !== null) sendAck(4, publish.packetId);
        if (publish.qos === 2 && publish.packetId !== null) {
          qos2Pending.add(publish.packetId);
          sendAck(5, publish.packetId);
        }
        continue;
      }

      if (packet.type === 6) {
        let packetId;
        try {
          packetId = parsePacketId(packet.body);
        } catch (error) {
          state.malformedCount += 1;
          onControlPacket({ type: 'malformed', error: error instanceof Error ? error.message : String(error) });
          continue;
        }
        qos2Pending.delete(packetId);
        sendAck(7, packetId);
        continue;
      }
      onControlPacket({ ...packet, receivedMonoNs: callbackMonoNs, receivedUtc: callbackUtc });
    }
  }

  return {
    onWebSocketMessage,
    nextRecvSeq: () => ++sequence.value,
    state,
  };
}

export class WinnerDetector {
  #emitted = false;
  #lastRecvSeq = -1;

  emitCandidate(event) {
    const recvSeq = Number(event.recvSeq);
    if (Number.isFinite(recvSeq) && recvSeq <= this.#lastRecvSeq) return null;
    if (Number.isFinite(recvSeq)) this.#lastRecvSeq = recvSeq;
    if (this.#emitted) return null;
    const winner = isValidWinner(event.winner)
      ?? isValidWinner(findField(event.payload, ['winner', 'matchWinner']));
    if (!winner) return null;
    this.#emitted = true;
    return {
      type: 'MATCH_RESULT',
      winner,
      recvSeq: event.recvSeq ?? null,
      emittedMonoNs: process.hrtime.bigint(),
    };
  }
}

export function serialiseRawEvent(event) {
  const derived = event.payloadRaw !== undefined ? event : parseDerivedEvent(event);
  const output = { ...derived };
  delete output._payloadBytes;
  delete output._hotDerived;
  delete output.latency;
  if (event.mqttPacketBytes instanceof Uint8Array) {
    output.mqttPacketBytesBase64 = bytesToBase64(event.mqttPacketBytes);
    delete output.mqttPacketBytes;
  }
  if (output.payloadRaw === undefined) output.payloadRaw = null;
  if (output.payloadBytesBase64 === undefined) output.payloadBytesBase64 = null;
  return output;
}

export function toJsonSafe(value) {
  return JSON.parse(JSON.stringify(value, (_, item) => (
    typeof item === 'bigint' ? item.toString() : item
  )));
}
