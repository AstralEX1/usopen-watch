import test from 'node:test';
import assert from 'node:assert/strict';
import {
  decodePayloadBytes,
  encodeRemainingLength,
  parsePublish,
} from '../mqtt.mjs';

test('strictly preserves invalid UTF-8 as base64', () => {
  const result = decodePayloadBytes(Uint8Array.from([0xff, 0x00, 0x61]));
  assert.equal(result.payloadRaw, null);
  assert.equal(result.payloadBytesBase64, '/wBh');
  assert.equal(result.encoding, 'base64');
});

test('encodes MQTT remaining length without truncation', () => {
  assert.deepEqual([...encodeRemainingLength(321)], [193, 2]);
});

test('parses QoS 1 PUBLISH packet metadata and payload bytes', () => {
  const packet = {
    flags: 0b0010,
    body: Uint8Array.from([
      0, 3, 97, 47, 98, 0, 7, 123, 34, 120, 34, 58, 49, 125,
    ]),
  };
  assert.deepEqual(parsePublish(packet), {
    topic: 'a/b',
    qos: 1,
    dup: false,
    retain: false,
    packetId: 7,
    payloadBytes: Uint8Array.from([123, 34, 120, 34, 58, 49, 125]),
  });
});
