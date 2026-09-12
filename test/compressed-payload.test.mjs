import test from 'node:test';
import assert from 'node:assert/strict';
import { gzipSync } from 'node:zlib';
import { parseDerivedEvent, serialiseRawEvent } from '../observer.mjs';

test('keeps compressed transport bytes while exposing parsed JSON as a derived view', () => {
  const json = JSON.stringify({ matchId: '3601', eventType: 'MatchWinner', winner: '2' });
  const bytes = new Uint8Array(gzipSync(Buffer.from(json)));
  const event = parseDerivedEvent({
    source: 'mqtt',
    phase: 'target',
    connectionId: 'primary',
    topic: 'events/tennis/2026/uso/score/3601',
    payloadBytes: bytes,
    recvSeq: 7,
  }, '3601');

  assert.equal(event.payloadRaw, null);
  assert.equal(event.payloadBytesBase64, Buffer.from(bytes).toString('base64'));
  assert.deepEqual(event.payload, { matchId: '3601', eventType: 'MatchWinner', winner: '2' });
  assert.equal(event.winner, '2');
  assert.equal(event.payloadDerivedRaw, json);
  assert.equal(serialiseRawEvent({ ...event }).payloadBytesBase64, Buffer.from(bytes).toString('base64'));
});
