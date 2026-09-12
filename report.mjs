import { readFile, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { toJsonSafe } from './observer.mjs';

function valueFrom(event, keys) {
  const wanted = new Set(keys.map((key) => String(key).replace(/[_-]/g, '').toLowerCase()));
  const stack = [event, event?.payload];
  const seen = new WeakSet();
  while (stack.length) {
    const current = stack.pop();
    if (!current || typeof current !== 'object') continue;
    if (seen.has(current)) continue;
    seen.add(current);
    for (const [key, value] of Object.entries(current)) {
      if (wanted.has(String(key).replace(/[_-]/g, '').toLowerCase())) return value;
      if (value && typeof value === 'object') stack.push(value);
    }
  }
  return undefined;
}

function winnerFrom(event) {
  const value = valueFrom(event, ['winner', 'matchWinner']);
  const winner = value === undefined || value === null ? null : String(value).trim();
  return winner === '1' || winner === '2' ? winner : null;
}

function statusFrom(event) {
  if (event?.matchStatus !== undefined && event?.matchStatus !== null) return String(event.matchStatus);
  const value = valueFrom(event, ['status']);
  return value === undefined || value === null ? null : String(value);
}

function eventTypeFrom(event) {
  const value = valueFrom(event, ['eventType', 'event', 'type', 'name']);
  if (value !== undefined && value !== null) return String(value);
  return event?.eventType ?? null;
}

function valueAt(event) {
  return event?.recvMonoNs ?? event?.recvSeq ?? null;
}

function isTarget(event, targetMatchId) {
  if (!targetMatchId) return true;
  if (event.matchId !== undefined && event.matchId !== null) {
    return String(event.matchId) === String(targetMatchId);
  }
  const haystack = `${event.topic ?? ''} ${event.url ?? ''}`;
  return haystack.includes(String(targetMatchId));
}

function isTerminal(event) {
  return /completed|retired|retirement|default|walkover/i.test(statusFrom(event) ?? '');
}

function hasMatchWinner(event) {
  const type = eventTypeFrom(event) ?? '';
  return /matchwinner/i.test(type) || valueFrom(event, ['matchWinner']) !== undefined;
}

function isScoreEvent(event) {
  return /score|status|summary/i.test(`${event.topic ?? ''} ${event.eventType ?? ''}`);
}

function isPointEvent(event) {
  return /point/i.test(eventTypeFrom(event) ?? '') || /point/i.test(event.topic ?? '');
}

function trueFlag(value) {
  return value === true || value === 1 || value === '1' || String(value).toLowerCase() === 'true';
}

function hasFinalPoint(event) {
  if (trueFlag(valueFrom(event, ['finalPoint', 'isFinalPoint', 'matchPoint']))) return true;
  return /matchwinner/i.test(eventTypeFrom(event) ?? '') && winnerFrom(event) !== null;
}

function firstValue(current, predicate) {
  for (const event of current) if (predicate(event)) return valueAt(event);
  return null;
}

function delta(later, earlier) {
  if (later === null || earlier === null || later === undefined || earlier === undefined) return null;
  try {
    return (BigInt(later) - BigInt(earlier)).toString();
  } catch {
    return null;
  }
}

function sourceTimeMs(value) {
  if (value === undefined || value === null) return NaN;
  const numeric = Number(value);
  if (Number.isFinite(numeric)) return numeric < 1e12 ? numeric * 1000 : numeric;
  return Date.parse(String(value));
}

export function extractSignalRace(rawEvents, targetMatchId = null, analyses = []) {
  const events = rawEvents.filter((event) => isTarget(event, targetMatchId));
  const t0 = firstValue(events, (event) => {
    const type = eventTypeFrom(event) ?? '';
    return /finalpoint|matchpoint/i.test(type) || hasFinalPoint(event);
  });
  const t1Event = events.find((event) => hasMatchWinner(event) && winnerFrom(event));
  const t2 = firstValue(events, isTerminal);
  const t3Event = events.find((event) => event.source === 'mqtt'
    && isScoreEvent(event)
    && winnerFrom(event));
  const t4Event = events.find((event) => event.source === 'http' && winnerFrom(event));
  const results = [];
  const seenWinners = new Set();
  for (const event of events) {
    const winner = winnerFrom(event);
    if (!winner || seenWinners.size > 0) continue;
    seenWinners.add(winner);
    results.push({ winner, recvSeq: event.recvSeq ?? null, at: valueAt(event) });
  }
  const firstAnalysis = analyses.find((analysis) => analysis.W4 !== undefined && analysis.W4 !== null);
  const t5 = firstAnalysis?.W4 ?? null;
  const t1 = t1Event ? valueAt(t1Event) : null;
  const t3 = t3Event ? valueAt(t3Event) : null;
  const t4 = t4Event ? valueAt(t4Event) : null;
  return {
    T0: t0,
    T1: t1,
    T2: t2,
    T3: t3,
    T4: t4,
    T5: t5,
    winners: {
      T1: winnerFrom(t1Event),
      T3: winnerFrom(t3Event),
      T4: winnerFrom(t4Event),
    },
    deltas: {
      T1_minus_T0: delta(t1, t0),
      T2_minus_T0: delta(t2, t0),
      T3_minus_T0: delta(t3, t0),
      T4_minus_T0: delta(t4, t0),
      T5_minus_winning_signal: firstAnalysis ? delta(t5, firstAnalysis.W3 ?? firstAnalysis.W2) : null,
    },
    resultCount: results.length,
    firstResult: results[0] ?? null,
  };
}

function display(value) {
  if (value === undefined || value === null) return '';
  if (typeof value === 'object') return JSON.stringify(value);
  return String(value);
}

export function buildTimeline(rawEvents, targetMatchId = null) {
  return rawEvents
    .filter((event) => isTarget(event, targetMatchId))
    .map((event) => {
      const type = eventTypeFrom(event) ?? event.source?.toUpperCase() ?? 'EVENT';
      const status = statusFrom(event);
      const winner = winnerFrom(event);
      const score = valueFrom(event, ['score', 'currentScore', 'scoreString']);
      const details = [
        `#${display(event.recvSeq)}`,
        event.source,
        event.phase,
        `topic=${display(event.topic ?? event.url)}`,
        status ? `status=${status}` : '',
        winner ? `winner=${winner}` : '',
        score !== undefined ? `score=${display(score)}` : '',
      ].filter(Boolean).join(' ');
      return `${display(event.recvUtc)} ${type.toUpperCase()} ${details}`;
    })
    .join('\n') + (rawEvents.length ? '\n' : '');
}

function exactEventKey(event) {
  return JSON.stringify([
    event.source,
    event.topic,
    event.url,
    event.payloadRaw,
    event.payloadBytesBase64,
    event.status,
    event.winner,
  ]);
}

function countBy(events, value) {
  const counts = {};
  for (const event of events) {
    const key = value(event) ?? 'unknown';
    counts[key] = (counts[key] ?? 0) + 1;
  }
  return counts;
}

function analyseOrdering(events) {
  const sequenceRegressions = [];
  const sourceTimestampRegressions = [];
  let previousSeq = null;
  let previousTimestamp = null;
  for (const event of events) {
    const seq = Number(event.recvSeq);
    if (Number.isFinite(seq) && previousSeq !== null && seq <= previousSeq) {
      sequenceRegressions.push({ recvSeq: event.recvSeq, previousRecvSeq: previousSeq });
    }
    if (Number.isFinite(seq)) previousSeq = seq;
    const sourceTimestamp = sourceTimeMs(event.sourceTimestamp);
    if (Number.isFinite(sourceTimestamp) && previousTimestamp !== null && sourceTimestamp < previousTimestamp) {
      sourceTimestampRegressions.push({ recvSeq: event.recvSeq, sourceTimestamp: event.sourceTimestamp });
    }
    if (Number.isFinite(sourceTimestamp)) previousTimestamp = sourceTimestamp;
  }
  return { sequenceRegressions, sourceTimestampRegressions };
}

export function buildTopicMap(rawEvents, targetMatchId = null) {
  const topics = {};
  for (const event of rawEvents) {
    if (event.source !== 'mqtt' || !event.topic) continue;
    const item = topics[event.topic] ?? {
      topic: event.topic,
      phases: {},
      eventTypes: {},
      matchIds: {},
      qos: [],
      publishCount: 0,
      likely: 'unknown',
    };
    item.publishCount += 1;
    const phase = event.phase ?? 'unknown';
    item.phases[phase] = (item.phases[phase] ?? 0) + 1;
    const type = event.eventType ?? 'unknown';
    item.eventTypes[type] = (item.eventTypes[type] ?? 0) + 1;
    if (event.matchId !== undefined && event.matchId !== null) {
      const id = String(event.matchId);
      item.matchIds[id] = (item.matchIds[id] ?? 0) + 1;
    }
    if (!item.qos.includes(event.qos)) item.qos.push(event.qos);
    if (/point|winner/i.test(`${event.topic} ${type}`)) item.likely = 'event-or-delta';
    if (/score|status|summary|stat/i.test(`${event.topic} ${type}`)) item.likely = 'snapshot-or-state';
    topics[event.topic] = item;
  }
  return {
    targetMatchId: targetMatchId ? String(targetMatchId) : null,
    topics,
    targetTopics: Object.values(topics).filter((topic) => topic.matchIds[String(targetMatchId)]),
  };
}

export function buildReport({ rawEvents, connections = [], manifest = {}, analyses = [], targetMatchId = manifest.matchId }) {
  const duplicateGroups = {};
  for (const event of rawEvents) {
    const key = exactEventKey(event);
    duplicateGroups[key] = (duplicateGroups[key] ?? 0) + 1;
  }
  const duplicateGroupSizes = Object.values(duplicateGroups).filter((count) => count > 1);
  const race = extractSignalRace(rawEvents, targetMatchId, analyses);
  const recommendation = race.winners.T1 && race.winners.T3 && race.winners.T1 === race.winners.T3
    ? 'MatchWinner matched the explicit score winner in this capture; keep a production cross-check until more matches are observed.'
    : race.winners.T3
      ? 'Use the explicit score-feed winner as the conservative production candidate; MatchWinner was not independently confirmed here.'
      : 'No verified production winner trigger was established in this capture.';
  return {
    matchId: targetMatchId ? String(targetMatchId) : null,
    eventCount: rawEvents.length,
    counts: {
      bySource: countBy(rawEvents, (event) => event.source),
      byPhase: countBy(rawEvents, (event) => event.phase),
      byTopic: countBy(rawEvents, (event) => event.topic ?? event.url),
      byType: countBy(rawEvents, (event) => event.eventType),
    },
    duplicates: {
      exactDuplicateGroups: duplicateGroupSizes.length,
      largestExactDuplicateGroup: Math.max(0, ...duplicateGroupSizes),
    },
    ordering: analyseOrdering(rawEvents),
    queue: {
      overflowCount: manifest.collection?.overflowCount ?? manifest.overflowCount ?? 0,
      droppedCount: manifest.collection?.droppedCount ?? manifest.droppedCount ?? 0,
      incomplete: Boolean(manifest.collection?.completeness === 'incomplete' || manifest.completeness === 'incomplete'),
    },
    warmStartGap: manifest.collection?.warmStartGap ?? manifest.warmStartGap ?? null,
    connections,
    signalRace: race,
    winnerAnalyses: analyses,
    recommendation,
  };
}

export async function readRawEvents(path) {
  const content = await readFile(path, 'utf8');
  return content.split(/\r?\n/).filter(Boolean).map((line) => JSON.parse(line));
}

export async function writeReports({ outputDir, rawEvents, connections, manifest, analyses, targetMatchId }) {
  const report = buildReport({ rawEvents, connections, manifest, analyses, targetMatchId });
  await writeFile(join(outputDir, 'timeline.txt'), buildTimeline(rawEvents, targetMatchId), 'utf8');
  await writeFile(join(outputDir, 'topics.json'), `${JSON.stringify(toJsonSafe(buildTopicMap(rawEvents, targetMatchId)), null, 2)}\n`, 'utf8');
  await writeFile(join(outputDir, 'report.json'), `${JSON.stringify(toJsonSafe(report), null, 2)}\n`, 'utf8');
  return report;
}
