import test from 'node:test';
import assert from 'node:assert/strict';
import {
  BoundedEventQueue,
  WinnerDetector,
  createReceiver,
} from '../observer.mjs';

test('receive sequence is unique even when one frame contains two packets', () => {
  const queue = new BoundedEventQueue(10);
  const receiver = createReceiver({
    connectionId: 'primary',
    phase: 'discovery',
    queue,
    send: () => {},
  });
  receiver.onWebSocketMessage(Uint8Array.from([
    0x30, 0x04, 0x00, 0x01, 0x61, 0x78,
    0x30, 0x04, 0x00, 0x01, 0x61, 0x79,
  ]));
  assert.deepEqual(queue.events.map((event) => event.recvSeq), [1, 2]);
  assert.equal(queue.events[0].recvSeq < queue.events[1].recvSeq, true);
});

test('queue overflow marks capture incomplete', () => {
  const queue = new BoundedEventQueue(1);
  queue.push({ recvSeq: 1 });
  queue.push({ recvSeq: 2 });
  assert.equal(queue.overflowCount, 1);
  assert.equal(queue.droppedCount, 1);
  assert.equal(queue.incomplete, true);
});

test('receiver acknowledges QoS 1 and completes QoS 2 flow', () => {
  const queue = new BoundedEventQueue(10);
  const sent = [];
  const receiver = createReceiver({
    connectionId: 'primary',
    phase: 'target',
    queue,
    send: (bytes) => sent.push([...bytes]),
  });

  receiver.onWebSocketMessage(Uint8Array.from([
    0x32, 0x06, 0x00, 0x01, 0x61, 0x00, 0x07, 0x78,
  ]));
  receiver.onWebSocketMessage(Uint8Array.from([
    0x34, 0x06, 0x00, 0x01, 0x61, 0x00, 0x08, 0x79,
  ]));
  receiver.onWebSocketMessage(Uint8Array.from([0x62, 0x02, 0x00, 0x08]));

  assert.deepEqual(sent, [
    [0x40, 0x02, 0x00, 0x07],
    [0x50, 0x02, 0x00, 0x08],
    [0x70, 0x02, 0x00, 0x08],
  ]);
});

test('completed without explicit winner does not emit a result', () => {
  const detector = new WinnerDetector();
  assert.equal(detector.emitCandidate({ status: 'Completed', winner: null }), null);
});

test('duplicate explicit winner emits exactly once', () => {
  const detector = new WinnerDetector();
  const event = { status: 'Completed', winner: '1', recvSeq: 8 };
  assert.equal(detector.emitCandidate(event).type, 'MATCH_RESULT');
  assert.equal(detector.emitCandidate({ ...event, recvSeq: 9 }), null);
});
