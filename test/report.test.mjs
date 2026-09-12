import test from 'node:test';
import assert from 'node:assert/strict';
import { buildTopicMap, extractSignalRace } from '../report.mjs';

test('race ignores Completed with null winner and emits one result from explicit winner', () => {
  const mqttEvent = (recvSeq, eventType, payload, topic = 'match/3601/score') => ({
    recvSeq,
    source: 'mqtt',
    topic,
    payload,
    eventType,
    matchId: '3601',
  });
  const httpEvent = (recvSeq, payload) => ({
    recvSeq,
    source: 'http',
    payload,
    matchId: '3601',
  });
  const race = extractSignalRace([
    mqttEvent(10, 'Point', { eventType: 'PointWon' }, 'match/3601/point'),
    mqttEvent(11, 'Score', { status: 'Completed', winner: null }),
    mqttEvent(12, 'Point', { eventType: 'MatchWinner', winner: '1' }, 'match/3601/point'),
    mqttEvent(13, 'Score', { status: 'Completed', winner: '1' }),
    httpEvent(14, { status: 'Completed', winner: '1' }),
  ]);
  assert.equal(race.T2, 11);
  assert.equal(race.T0, 12);
  assert.equal(race.T1, 12);
  assert.equal(race.T3, 13);
  assert.equal(race.T4, 14);
  assert.equal(race.resultCount, 1);
});

test('does not classify the namespace word events as an event payload', () => {
  const map = buildTopicMap([{
    source: 'mqtt',
    topic: 'events/tennis/2026/uso/stat/3601',
    eventType: null,
    matchId: '3601',
    qos: 0,
  }], '3601');
  assert.equal(map.topics['events/tennis/2026/uso/stat/3601'].likely, 'snapshot-or-state');
});
