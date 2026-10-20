// #370: shared remote-control latency histogram + aggregator, pinned to the
// same contract vectors the Rust mirror (rc_latency_telemetry.rs) asserts.
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';

import { LatencyHistogram, latencyHistogramEdges } from '@petal/shared/logic/latencyHistogram';
import {
  RC_LATENCY_COUNTERS,
  RC_LATENCY_DIMENSIONS,
  RC_LATENCY_FINAL_MIN_SAMPLES,
  RC_LATENCY_INTERVAL_MS,
  RC_LATENCY_LOG_PREFIX,
  RC_LATENCY_MAX_COUNTERS,
  RC_LATENCY_MAX_EVENTS_PER_SESSION,
  RC_LATENCY_STAGES,
  RcLatencyAggregator,
  rcLatencySummaryLogLine,
  rcLatencySummaryProperties,
  type RcLatencySummary,
} from '@petal/shared/logic/rcLatencyAggregator';
import {
  commonPropertyKeys,
  installTestSink,
  remoteControlLatencySummary,
  setUsageStatsConsentForTest,
  uninstallTestSink,
} from '../src/analytics.ts';

const contract = JSON.parse(
  readFileSync(new URL('../../contracts/petal-contracts.json', import.meta.url), 'utf8')
);
const schema = contract.remoteControlLatencySummary;
const remoteControlUiSrc = readFileSync(new URL('../src/remoteControlUi.ts', import.meta.url), 'utf8');

test('histogram matches the shared contract vectors', () => {
  assert.ok(contract.latencyHistogramVectors.length >= 5);
  assert.equal(latencyHistogramEdges().length, 176);
  for (const vector of contract.latencyHistogramVectors) {
    const histogram = new LatencyHistogram();
    for (const sample of vector.samples) histogram.record(sample === null ? Number.NaN : sample);
    assert.deepEqual(
      { n: histogram.n, rejected: histogram.rejected, p50: histogram.p50(), p95: histogram.p95(), max: histogram.max() },
      vector.expected,
      vector.name
    );
  }
});

test('stage, counter and dimension keys match the contract schema', () => {
  assert.deepEqual([...RC_LATENCY_STAGES], schema.stages);
  assert.deepEqual([...RC_LATENCY_COUNTERS], schema.counters);
  assert.deepEqual([...RC_LATENCY_MAX_COUNTERS], schema.maxCounters);
  assert.deepEqual(JSON.parse(JSON.stringify(RC_LATENCY_DIMENSIONS)), schema.dimensions);
  assert.equal(RC_LATENCY_INTERVAL_MS, schema.intervalMs);
  assert.equal(RC_LATENCY_FINAL_MIN_SAMPLES, schema.finalMinSamples);
  assert.equal(RC_LATENCY_MAX_EVENTS_PER_SESSION, schema.maxEventsPerSession);
  assert.equal(RC_LATENCY_LOG_PREFIX, schema.logPrefix);
});

test('example summary renders the contract properties and log line', () => {
  const summary = schema.example.summary as RcLatencySummary;
  const properties = rcLatencySummaryProperties(summary);
  assert.deepEqual(properties, schema.example.properties);
  assert.deepEqual(Object.keys(properties), Object.keys(schema.example.properties));
  assert.equal(rcLatencySummaryLogLine(summary), schema.example.logLine);
});

function harness() {
  const emitted: RcLatencySummary[] = [];
  let ids = 0;
  const aggregator = new RcLatencyAggregator('controller', {
    emit: (summary) => emitted.push(summary),
    newSessionId: () => `session-${(ids += 1)}`,
  });
  return { aggregator, emitted };
}

test('lifecycle: grant, 60 s tick emits, release emits a final partial', () => {
  const { aggregator, emitted } = harness();
  aggregator.recordStage('photon_ms', 10);
  aggregator.sync(false, 0);
  assert.equal(aggregator.active, false, 'nothing before the grant');
  aggregator.sync(true, 0);
  for (const ms of [20, 30, 40, 50, 60, 70]) aggregator.recordStage('photon_ms', ms);
  aggregator.sync(true, 59_999);
  assert.equal(emitted.length, 0);
  aggregator.sync(true, 60_000);
  assert.equal(emitted.length, 1);
  assert.equal(emitted[0].final, false);
  assert.equal(emitted[0].intervalS, 60);
  assert.equal(emitted[0].stages.photon_ms?.n, 6);
  assert.equal(emitted[0].rcSessionId, 'session-1');
  for (const ms of [1, 2, 3, 4, 5]) aggregator.recordStage('input_rtt_ms', ms);
  aggregator.sync(false, 75_000);
  assert.equal(emitted.length, 2);
  assert.equal(emitted[1].final, true);
  assert.equal(emitted[1].intervalS, 15);
  assert.deepEqual(Object.keys(emitted[1].stages), ['input_rtt_ms'], 'n = 0 stages are omitted');
  assert.equal(emitted[1].rcSessionId, 'session-1');
});

test('revoke mid-interval with n < 5 emits nothing; regrant mints a new session', () => {
  const { aggregator, emitted } = harness();
  aggregator.sync(true, 0);
  for (const ms of [1, 2, 3, 4]) aggregator.recordStage('photon_ms', ms);
  aggregator.sync(false, 30_000);
  assert.equal(emitted.length, 0);
  aggregator.sync(true, 31_000);
  for (let i = 0; i < 5; i += 1) aggregator.recordStage('photon_ms', 9);
  aggregator.sync(false, 32_000);
  assert.equal(emitted.length, 1);
  assert.equal(emitted[0].rcSessionId, 'session-2');
  assert.equal(emitted[0].stages.photon_ms?.n, 5);
});

test('empty intervals emit nothing and a session caps at 120 events', () => {
  const { aggregator, emitted } = harness();
  let now = 0;
  aggregator.sync(true, now);
  now += RC_LATENCY_INTERVAL_MS;
  aggregator.sync(true, now);
  assert.equal(emitted.length, 0);
  for (let i = 0; i < 130; i += 1) {
    aggregator.recordStage('decode_ms', 3);
    now += RC_LATENCY_INTERVAL_MS;
    aggregator.sync(true, now);
  }
  assert.equal(emitted.length, RC_LATENCY_MAX_EVENTS_PER_SESSION);
});

test('counters sum or keep max; dimensions are closed', () => {
  const { aggregator, emitted } = harness();
  assert.equal(aggregator.setDimension('same_edge', 'true'), false, 'no session yet');
  aggregator.sync(true, 0);
  aggregator.addCounter('input_stall_count', 2);
  aggregator.addCounter('input_stall_count', 3);
  aggregator.addCounter('offset_err_ms', 7);
  aggregator.addCounter('offset_err_ms', 4);
  assert.equal(aggregator.setDimension('same_edge', 'maybe'), false);
  assert.equal(aggregator.setDimension('lk_region', 'Room Name'), false);
  assert.equal(aggregator.setDimension('lk_region', 'us-east-1'), true);
  aggregator.sync(true, RC_LATENCY_INTERVAL_MS);
  assert.deepEqual(emitted[0].counters, { input_stall_count: 5, offset_err_ms: 7 });
  assert.deepEqual(emitted[0].dimensions, { lk_region: 'us-east-1' });
  aggregator.sync(true, RC_LATENCY_INTERVAL_MS * 2);
  assert.deepEqual(emitted[1].counters, { input_stall_count: 0, offset_err_ms: 0 });
});

test('the PostHog event carries typed numbers and only schema keys', () => {
  const events = installTestSink();
  try {
    remoteControlLatencySummary(schema.example.summary as RcLatencySummary);
    assert.equal(events.length, 1);
    const [event] = events;
    assert.equal(event.name, 'remote_control_latency_summary');
    assert.equal(event.properties.replay_complete_ms_p95, 108);
    assert.equal(event.properties.final, false);
    assert.equal(event.properties.client, 'web');
    const allowed = new Set<string>([...commonPropertyKeys(), ...schema.alwaysPresent, ...schema.counters, ...Object.keys(schema.dimensions)]);
    for (const stage of schema.stages) for (const suffix of schema.stageSuffixes) allowed.add(`${stage}${suffix}`);
    for (const key of Object.keys(event.properties)) assert.ok(allowed.has(key), `leaked property ${key}`);
    assert.equal(event.properties.replay_inject_ms_n, 400);
    assert.equal(event.properties.photon_ms_p50, undefined, 'n = 0 stages are omitted');
  } finally {
    uninstallTestSink();
  }
});

test('the consent gate blocks the send and reopens', () => {
  const events = installTestSink();
  try {
    setUsageStatsConsentForTest(false);
    remoteControlLatencySummary(schema.example.summary as RcLatencySummary);
    assert.equal(events.length, 0);
    setUsageStatsConsentForTest(true);
    remoteControlLatencySummary(schema.example.summary as RcLatencySummary);
    assert.equal(events.length, 1);
  } finally {
    uninstallTestSink();
  }
});

test('the web controller reconciles its session against the live grant', () => {
  assert.match(remoteControlUiSrc, /new RcLatencyAggregator\('controller'/);
  assert.match(remoteControlUiSrc, /rcLatency\.sync\(Boolean\(state\.activeRemoteControl\?\.grantToken\), Date\.now\(\)\)/);
  assert.match(remoteControlUiSrc, /logEvent\(rcLatencySummaryLogLine\(summary\)\)/);
  assert.match(remoteControlUiSrc, /remoteControlLatencySummary\(summary\)/);
});
