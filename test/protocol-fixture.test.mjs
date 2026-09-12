import test from 'node:test';
import assert from 'node:assert/strict';
import { BoundedEventQueue, parseDerivedEvent } from '../observer.mjs';
import { buildMqttConnection, recordHttpResponse } from '../usopen-watch.mjs';

test('records a PUBLISH before SUBACK and preserves HTTP timing markers', () => {
  const sent = [];
  const granted = [];
  const queue = new BoundedEventQueue(10);
  const connection = buildMqttConnection({
    connectionId: 'primary',
    phase: 'discovery',
    filters: [{ filter: '#', qos: 0 }],
    recorder: { queue },
    send: (bytes) => sent.push(bytes),
    onSubAck: (suback) => granted.push(...suback.grantedQos),
  });
  connection.sendSubscribe();
  connection.handleBytes(Uint8Array.from([0x30, 0x04, 0x00, 0x01, 0x61, 0x78]));
  connection.handleBytes(Uint8Array.from([0x90, 0x03, 0x00, 0x01, 0x00]));
  const http = recordHttpResponse({
    requestStartMonoNs: 1n,
    headersReceivedMonoNs: 2n,
    bodyCompleteMonoNs: 3n,
    parsedDetectedMonoNs: 4n,
    url: 'https://example.invalid',
    responseHeaders: {},
    status: 200,
    body: '{}',
  });

  assert.equal(sent.length > 0, true);
  assert.deepEqual(granted, [0]);
  assert.equal(queue.events.length, 1);
  assert.equal(connection.lifecycle.subscribeSent < queue.events[0].recvMonoNs, true);
  assert.deepEqual([
    http.requestStartMonoNs,
    http.headersReceivedMonoNs,
    http.bodyCompleteMonoNs,
    http.parsedDetectedMonoNs,
  ], [1n, 2n, 3n, 4n]);
});

test('keeps HTTP status separate from target match status in a multi-match feed', () => {
  const event = recordHttpResponse({
    requestStartMonoNs: 1n,
    headersReceivedMonoNs: 2n,
    bodyCompleteMonoNs: 3n,
    url: 'https://example.invalid/live.json',
    responseHeaders: {},
    status: 200,
    body: JSON.stringify({ matches: [
      { match_id: '3601', status: 'In Progress', winner: null },
      { match_id: '21601', status: 'Completed', winner: '2' },
    ] }),
  });
  const derived = parseDerivedEvent(event, '21601');
  assert.equal(derived.status, 200);
  assert.equal(derived.matchStatus, 'Completed');
  assert.equal(derived.matchId, '21601');
  assert.equal(derived.winner, '2');
});
