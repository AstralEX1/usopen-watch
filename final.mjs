export const MEN_SINGLES_FINAL = "Men's Singles";
export const TERMINAL_STATUSES = new Set([
  'completed',
  'retired',
  'retirement',
  'default',
  'walkover',
]);

function stringValue(value) {
  if (value === undefined || value === null) return null;
  const result = String(value).trim();
  return result || null;
}

function validSide(value) {
  const side = stringValue(value);
  return side === '1' || side === '2' ? side : null;
}

function recordsFromPayload(payload) {
  if (Array.isArray(payload)) return payload;
  if (!payload || typeof payload !== 'object') return [];
  if (Array.isArray(payload.matches)) return payload.matches;
  if (payload.match && typeof payload.match === 'object') return [payload.match];
  if (payload.data && Array.isArray(payload.data.matches)) return payload.data.matches;
  if (payload.match_id !== undefined || payload.matchId !== undefined || payload.matchID !== undefined) return [payload];
  return [];
}

function payloadFromEvent(event) {
  if (event?.payload !== undefined) return event.payload;
  if (event?.httpParsed !== undefined) return event.httpParsed;
  return event;
}

function exactMatchId(match) {
  return stringValue(match?.match_id ?? match?.matchId ?? match?.matchID);
}

function playerFromTeam(team, side) {
  if (!team || typeof team !== 'object') return null;
  if (team.idB !== undefined && team.idB !== null) return null;
  if (team.firstNameB !== undefined && team.firstNameB !== null) return null;
  if (team.lastNameB !== undefined && team.lastNameB !== null) return null;
  const id = stringValue(team.idA ?? team.id ?? team.playerId);
  const fullName = [team.firstNameA, team.lastNameA]
    .map(stringValue)
    .filter(Boolean)
    .join(' ');
  const name = fullName || stringValue(team.displayNameA ?? team.name);
  if (!id || !name) return null;
  return { id, name, side };
}

function matchForFinal(payload, targetMatchId = null) {
  const records = recordsFromPayload(payload).filter((match) => (
    match && typeof match === 'object'
      && match.eventName === MEN_SINGLES_FINAL
      && (match.roundCode === 'F' || match.roundName === 'Final')
  ));
  if (targetMatchId !== null && targetMatchId !== undefined) {
    return records.find((match) => exactMatchId(match) === String(targetMatchId)) ?? null;
  }
  return records.length === 1 ? records[0] : null;
}

function finalistsFromMatch(match) {
  if (!match) return null;
  const matchId = exactMatchId(match);
  const playerA = playerFromTeam(match.team1, '1');
  const playerB = playerFromTeam(match.team2, '2');
  if (!matchId || !playerA || !playerB || playerA.id === playerB.id) return null;
  return {
    matchId,
    players: [playerA, playerB],
    status: stringValue(match.status),
  };
}

function matchAndFinalists(payload, targetMatchId = null) {
  const match = matchForFinal(payload, targetMatchId);
  const finalists = finalistsFromMatch(match);
  return match && finalists ? { match, finalists } : null;
}

export function extractMensSinglesFinalists(payloadOrEvent, targetMatchId = null) {
  return matchAndFinalists(payloadFromEvent(payloadOrEvent), targetMatchId)?.finalists ?? null;
}

export async function waitForMensSinglesFinalists({
  url,
  matchId = null,
  fetchImpl = globalThis.fetch,
  intervalMs = 1000,
  onError = () => {},
} = {}) {
  if (typeof fetchImpl !== 'function') throw new Error('fetch is required for official US Open startup');
  for (;;) {
    try {
      const response = await fetchImpl(url, {
        headers: { accept: 'application/json', 'cache-control': 'no-cache' },
      });
      if (response.ok === false || (response.status !== undefined && response.status >= 400)) {
        throw new Error(`US Open HTTP ${response.status ?? 'error'}`);
      }
      const snapshot = extractMensSinglesFinalists(await response.json(), matchId);
      if (snapshot) return snapshot;
    } catch (error) {
      onError(error);
    }
    await new Promise((resolve) => setTimeout(resolve, intervalMs));
  }
}

function sourceTimestamp(event, payload) {
  const candidate = payload?.epoch ?? payload?.create_epoch;
  if (candidate === undefined || candidate === null) return null;
  const number = Number(candidate);
  return Number.isSafeInteger(number) && number > 0 ? number : null;
}

function sourceSequence(event) {
  return event?.sourceSequence ?? event?.recvSeq ?? event?.sequence ?? null;
}

function makeFinal(event, match, finalists) {
  const winnerSide = validSide(match.winner);
  if (!winnerSide) return null;
  const winner = finalists.players[Number(winnerSide) - 1];
  const loser = finalists.players[Number(winnerSide) === 1 ? 1 : 0];
  if (!winner || !loser || winner.id === loser.id) return null;
  const payload = payloadFromEvent(event);
  return {
    type: 'MATCH_FINAL',
    matchId: finalists.matchId,
    status: finalists.status,
    winnerId: winner.id,
    winnerName: winner.name,
    loserId: loser.id,
    loserName: loser.name,
    source: event?.source === 'http' ? 'usopen-http' : 'usopen-mqtt',
    sourceSequence: sourceSequence(event),
    sourceTimestamp: sourceTimestamp(event, payload),
    networkReceivedMonoNs: event?.networkReceivedMonoNs
      ?? event?.recvMonoNs
      ?? event?.bodyCompleteMonoNs
      ?? null,
    detectedMonoNs: process.hrtime.bigint(),
    detectedWallTime: new Date().toISOString(),
  };
}

export class FinalDetector {
  #targetMatchId;
  #onFinal;
  #onError;
  #emitted = false;
  #lastSourceSequence = -1;

  constructor({ targetMatchId = null, onFinal = null, onError = null } = {}) {
    this.#targetMatchId = targetMatchId === null ? null : String(targetMatchId);
    this.#onFinal = onFinal;
    this.#onError = onError;
  }

  onFinal(callback) {
    this.#onFinal = callback;
    return this;
  }

  accept(event) {
    const sequence = Number(sourceSequence(event));
    if (Number.isFinite(sequence)) {
      if (sequence <= this.#lastSourceSequence) return null;
      this.#lastSourceSequence = sequence;
    }
    if (this.#emitted) return null;

    const matchAndFinal = matchAndFinalists(payloadFromEvent(event), this.#targetMatchId);
    if (!matchAndFinal || !TERMINAL_STATUSES.has((matchAndFinal.finalists.status ?? '').toLowerCase())) return null;
    const final = makeFinal(event, matchAndFinal.match, matchAndFinal.finalists);
    if (!final) return null;
    this.#emitted = true;
    try {
      this.#onFinal?.(final);
    } catch (error) {
      this.#onError?.(error, final);
    }
    return final;
  }
}

export function detectMatchEndImminent(event, targetMatchId = null) {
  if (!event?.topic?.includes('/slamtracker/')) return null;
  const payload = payloadFromEvent(event);
  const points = Array.isArray(payload) ? payload : [];
  const point = points.at(-1);
  if (!point || String(point.Stage ?? '').toLowerCase() !== 'win') return null;
  const matchId = stringValue(point.MatchID ?? point.match_id ?? point.matchId);
  const winnerId = validSide(point.Winner);
  if (!matchId || (targetMatchId !== null && matchId !== String(targetMatchId)) || !winnerId) return null;
  return {
    type: 'MATCH_END_IMMINENT',
    matchId,
    winnerSide: winnerId,
    source: 'usopen-mqtt-slamtracker',
    sourceSequence: sourceSequence(event),
    networkReceivedMonoNs: event.networkReceivedMonoNs ?? event.recvMonoNs ?? null,
    detectedMonoNs: process.hrtime.bigint(),
    detectedWallTime: new Date().toISOString(),
  };
}
