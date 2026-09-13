import test from 'node:test';
import assert from 'node:assert/strict';
import { FinalDetector, extractMensSinglesFinalists, detectMatchEndImminent } from '../final.mjs';

const PLAYER_A = { id: 'atpa', firstName: 'Player', lastName: 'A', displayName: 'P. A' };
const PLAYER_B = { id: 'atpb', firstName: 'Player', lastName: 'B', displayName: 'P. B' };

function payload({
  matchId = 'mens-final-1',
  eventName = "Men's Singles",
  roundCode = 'F',
  status = 'Completed',
  winner = '1',
  team1 = PLAYER_A,
  team2 = PLAYER_B,
} = {}) {
  const team = (player) => ({
    firstNameA: player.firstName,
    lastNameA: player.lastName,
    displayNameA: player.displayName,
    idA: player.id,
    firstNameB: null,
    lastNameB: null,
    displayNameB: null,
    idB: null,
  });
  return {
    epoch: 1789235673,
    matches: [{
      match_id: matchId,
      eventName,
      roundCode,
      roundName: 'Final',
      status,
      winner,
      team1: team(team1),
      team2: team(team2),
      scores: { setsWon: [2, 1] },
    }],
  };
}

function event(options = {}) {
  return {
    source: 'http',
    recvSeq: 10,
    recvMonoNs: 100n,
    recvUtc: '2026-09-13T12:00:00.000Z',
    payload: payload(options),
  };
}

test('A wins: B is the deterministic loser', () => {
  const final = new FinalDetector({ targetMatchId: 'mens-final-1' }).accept(event({ winner: '1' }));
  assert.equal(final.type, 'MATCH_FINAL');
  assert.equal(final.winnerId, PLAYER_A.id);
  assert.equal(final.loserId, PLAYER_B.id);
  assert.equal(final.winnerName, 'Player A');
  assert.equal(final.loserName, 'Player B');
});

test('B wins: A is the deterministic loser', () => {
  const final = new FinalDetector({ targetMatchId: 'mens-final-1' }).accept(event({ winner: '2' }));
  assert.equal(final.winnerId, PLAYER_B.id);
  assert.equal(final.loserId, PLAYER_A.id);
});

test('missing winner, score inference, and non-terminal state never emit', () => {
  assert.equal(new FinalDetector({ targetMatchId: 'mens-final-1' }).accept(event({ winner: null })), null);
  assert.equal(new FinalDetector({ targetMatchId: 'mens-final-1' }).accept(event({ status: 'In Progress', winner: '1' })), null);
});

test('unknown player, wrong match, and ambiguous/cancelled state fail closed', () => {
  assert.equal(new FinalDetector({ targetMatchId: 'mens-final-1' }).accept(event({ winner: '3' })), null);
  assert.equal(new FinalDetector({ targetMatchId: 'other-match' }).accept(event()), null);
  assert.equal(new FinalDetector({ targetMatchId: 'mens-final-1' }).accept(event({ status: 'Cancelled', winner: '1' })), null);
  assert.equal(new FinalDetector({ targetMatchId: 'mens-final-1' }).accept(event({ team2: { ...PLAYER_A }, winner: '1' })), null);
});

test('only an exact Men\'s Singles final is eligible for startup finalists', () => {
  assert.deepEqual(extractMensSinglesFinalists(payload()), {
    matchId: 'mens-final-1',
    players: [
      { id: 'atpa', name: 'Player A', side: '1' },
      { id: 'atpb', name: 'Player B', side: '2' },
    ],
    status: 'Completed',
  });
  assert.equal(extractMensSinglesFinalists(payload({ eventName: "Men's Doubles" })), null);
});

test('MATCH_END_IMMINENT is non-authoritative and cannot produce MATCH_FINAL', () => {
  const signal = detectMatchEndImminent({
    source: 'mqtt',
    topic: 'events/tennis/2026/uso/slamtracker/mens-final-1',
    recvSeq: 5,
    recvMonoNs: 200n,
    payload: [{ MatchID: 'mens-final-1', Stage: 'win', Winner: '2' }],
  }, 'mens-final-1');
  assert.equal(signal.type, 'MATCH_END_IMMINENT');
  assert.equal(new FinalDetector({ targetMatchId: 'mens-final-1' }).accept({
    ...event({ status: 'In Progress', winner: null }),
    payload: { ...payload({ status: 'In Progress', winner: null }), scores: { setsWon: [2, 0] } },
  }), null);
});
