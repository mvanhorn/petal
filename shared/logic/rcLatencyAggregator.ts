// Per-control-session remote-control latency aggregator (#370, tracking #369).
// Rust mirror: apps/desktop/src-tauri/src/rc_latency_telemetry.rs. The stage,
// counter and dimension keys are pinned by contracts/petal-contracts.json
// `remoteControlLatencySummary`; add a key there and on both sides together.
//
// Producers (#371-#376) only call `recordStage`, `addCounter` and
// `setDimension`. The owner calls `sync(active, nowMs)` on a timer with the
// current "is control active" truth; the aggregator starts a session, emits
// one summary per 60 s of active control, and a final partial on stop.

import { LatencyHistogram, type LatencyHistogramStats } from './latencyHistogram';

export type RcRole = 'host' | 'controller';

export const RC_LATENCY_STAGES = [
  'input_leg_ms',
  'input_rtt_ms',
  'input_excess_delay_ms',
  'replay_inject_ms',
  'replay_complete_ms',
  'inject_to_capture_ms',
  'capture_interval_ms',
  'capture_to_encoded_ms',
  'encoded_to_packetized_ms',
  'pub_rtt_ms',
  'sub_rtt_ms',
  'video_net_ms',
  'jb_delay_ms',
  'jb_target_ms',
  'decode_ms',
  'processing_ms',
  'receive_to_present_ms',
  'photon_ms'
] as const;
export type RcLatencyStage = (typeof RC_LATENCY_STAGES)[number];

/** Summed over the interval, except the two in RC_LATENCY_MAX_COUNTERS, which keep the interval max. */
export const RC_LATENCY_COUNTERS = [
  'input_stall_count',
  'frames_dropped',
  'freeze_count',
  'nack_count',
  'pli_count',
  'packets_lost_pct_x100',
  'offset_err_ms'
] as const;
export type RcLatencyCounter = (typeof RC_LATENCY_COUNTERS)[number];
export const RC_LATENCY_MAX_COUNTERS: readonly RcLatencyCounter[] = ['packets_lost_pct_x100', 'offset_err_ms'];

/** `null` = a free-form token (lowercase [a-z0-9_-], at most 32 chars). */
export const RC_LATENCY_DIMENSIONS = {
  lk_region: null,
  same_edge: ['true', 'false', 'unknown'],
  self_hosted_sfu: ['true', 'false', 'unknown'],
  ice_candidate_type: ['host', 'srflx', 'prflx', 'relay', 'unknown'],
  ice_protocol: ['udp', 'tcp', 'unknown'],
  window_codec: ['h264', 'h265', 'vp8', 'vp9', 'av1', 'unknown'],
  content_change: ['detected', 'unknown']
} as const satisfies Record<string, readonly string[] | null>;
export type RcLatencyDimension = keyof typeof RC_LATENCY_DIMENSIONS;
export const RC_LATENCY_DIMENSION_KEYS = Object.keys(RC_LATENCY_DIMENSIONS) as RcLatencyDimension[];

export const RC_LATENCY_INTERVAL_MS = 60_000;
export const RC_LATENCY_FINAL_MIN_SAMPLES = 5;
export const RC_LATENCY_MAX_EVENTS_PER_SESSION = 120;
export const RC_LATENCY_LOG_PREFIX = 'remote-control-latency-summary:';

export type RcLatencySummary = {
  role: RcRole;
  rcSessionId: string;
  intervalS: number;
  final: boolean;
  stages: Partial<Record<RcLatencyStage, LatencyHistogramStats>>;
  counters: Partial<Record<RcLatencyCounter, number>>;
  dimensions: Partial<Record<RcLatencyDimension, string>>;
};

const TOKEN = /^[a-z0-9_-]{1,32}$/;

export function rcLatencyDimensionValueAllowed(key: RcLatencyDimension, value: string): boolean {
  const allowed = RC_LATENCY_DIMENSIONS[key] as readonly string[] | null;
  return allowed === null ? TOKEN.test(value) : allowed.includes(value);
}

/** PostHog properties for `remote_control_latency_summary` (common properties are added by the analytics pipe). */
export function rcLatencySummaryProperties(summary: RcLatencySummary): Record<string, string | number | boolean> {
  const properties: Record<string, string | number | boolean> = {
    role: summary.role,
    rc_session_id: summary.rcSessionId,
    interval_s: summary.intervalS,
    final: summary.final
  };
  for (const stage of RC_LATENCY_STAGES) {
    const stats = summary.stages[stage];
    if (!stats || stats.n === 0) continue;
    properties[`${stage}_p50`] = stats.p50;
    properties[`${stage}_p95`] = stats.p95;
    properties[`${stage}_max`] = stats.max;
    properties[`${stage}_n`] = stats.n;
  }
  for (const counter of RC_LATENCY_COUNTERS) {
    const value = summary.counters[counter];
    if (value !== undefined) properties[counter] = value;
  }
  for (const key of RC_LATENCY_DIMENSION_KEYS) {
    const value = summary.dimensions[key];
    if (value !== undefined) properties[key] = value;
  }
  return properties;
}

/** The petal.log / logEvent mirror line. Same key order as the properties; parsed by the release-gate verdict. */
export function rcLatencySummaryLogLine(summary: RcLatencySummary): string {
  const parts = Object.entries(rcLatencySummaryProperties(summary)).map(([key, value]) => `${key}=${value}`);
  return `${RC_LATENCY_LOG_PREFIX} ${parts.join(' ')}`;
}

function newRcSessionId(): string {
  const bytes = new Uint8Array(16);
  if (typeof crypto !== 'undefined' && typeof crypto.getRandomValues === 'function') {
    crypto.getRandomValues(bytes);
  } else {
    for (let i = 0; i < 16; i += 1) bytes[i] = Math.floor(Math.random() * 256);
  }
  return [...bytes].map((byte) => byte.toString(16).padStart(2, '0')).join('');
}

type Session = {
  rcSessionId: string;
  intervalStartedAt: number;
  eventsEmitted: number;
  stages: Map<RcLatencyStage, LatencyHistogram>;
  counters: Partial<Record<RcLatencyCounter, number>>;
  dimensions: Partial<Record<RcLatencyDimension, string>>;
};

export type RcLatencyAggregatorOptions = {
  emit: (summary: RcLatencySummary) => void;
  newSessionId?: () => string;
};

export class RcLatencyAggregator {
  readonly role: RcRole;
  private session: Session | null = null;
  private readonly emit: (summary: RcLatencySummary) => void;
  private readonly newSessionId: () => string;

  constructor(role: RcRole, options: RcLatencyAggregatorOptions) {
    this.role = role;
    this.emit = options.emit;
    this.newSessionId = options.newSessionId ?? newRcSessionId;
  }

  get active(): boolean {
    return this.session !== null;
  }

  get rcSessionId(): string | null {
    return this.session?.rcSessionId ?? null;
  }

  /** Reconcile with the current control state: start, emit a due interval, or stop with a final partial. */
  sync(active: boolean, nowMs: number): void {
    if (active && !this.session) {
      this.session = {
        rcSessionId: this.newSessionId(),
        intervalStartedAt: nowMs,
        eventsEmitted: 0,
        stages: new Map(),
        counters: {},
        dimensions: {}
      };
      return;
    }
    const session = this.session;
    if (!session) return;
    if (!active) {
      this.session = null;
      const hasEnough = [...session.stages.values()].some((histogram) => histogram.n >= RC_LATENCY_FINAL_MIN_SAMPLES);
      if (hasEnough) this.flush(session, nowMs, true);
      return;
    }
    if (nowMs - session.intervalStartedAt >= RC_LATENCY_INTERVAL_MS) {
      this.flush(session, nowMs, false);
    }
  }

  recordStage(stage: RcLatencyStage, ms: number): void {
    const session = this.session;
    if (!session) return;
    let histogram = session.stages.get(stage);
    if (!histogram) {
      histogram = new LatencyHistogram();
      session.stages.set(stage, histogram);
    }
    histogram.record(ms);
  }

  addCounter(counter: RcLatencyCounter, n: number): void {
    const session = this.session;
    if (!session || !Number.isFinite(n) || n < 0) return;
    const value = Math.round(n);
    const previous = session.counters[counter];
    session.counters[counter] = RC_LATENCY_MAX_COUNTERS.includes(counter)
      ? Math.max(previous ?? 0, value)
      : (previous ?? 0) + value;
  }

  /** Returns false (and keeps the old value) for a value outside the closed set. */
  setDimension(key: RcLatencyDimension, value: string): boolean {
    const session = this.session;
    if (!session || !rcLatencyDimensionValueAllowed(key, value)) return false;
    session.dimensions[key] = value;
    return true;
  }

  private flush(session: Session, nowMs: number, final: boolean): void {
    const stages: RcLatencySummary['stages'] = {};
    for (const stage of RC_LATENCY_STAGES) {
      const stats = session.stages.get(stage)?.stats();
      if (stats) stages[stage] = stats;
    }
    const counters = { ...session.counters };
    const intervalS = Math.max(0, Math.round((nowMs - session.intervalStartedAt) / 1000));
    session.intervalStartedAt = nowMs;
    for (const histogram of session.stages.values()) histogram.reset();
    for (const counter of RC_LATENCY_COUNTERS) {
      if (session.counters[counter] !== undefined) session.counters[counter] = 0;
    }
    const empty = Object.keys(stages).length === 0 && Object.keys(counters).length === 0;
    if (empty || session.eventsEmitted >= RC_LATENCY_MAX_EVENTS_PER_SESSION) return;
    session.eventsEmitted += 1;
    this.emit({
      role: this.role,
      rcSessionId: session.rcSessionId,
      intervalS,
      final,
      stages,
      counters,
      dimensions: { ...session.dimensions }
    });
  }
}
