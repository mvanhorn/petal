// Fixed log-spaced latency histogram shared by the desktop app and the web
// client (#370). Rust mirror: apps/desktop/src-tauri/src/rc_latency_telemetry.rs.
// Both sides are pinned by contracts/petal-contracts.json
// `latencyHistogramVectors`; change the bucket layout or rounding on both
// sides together, never one alone.
//
// Buckets: [0, 1), then edges 1, 1.05, 1.05^2, ... up to 5000 (about 5%
// relative resolution). Edges are built by repeated multiplication, which
// is correctly rounded IEEE arithmetic in both languages, so the layout is
// bit-identical in Rust and TS. Values above 5000 clamp to 5000; negative
// and non-finite values are rejected and counted, never recorded.
// Percentiles are nearest-rank (`ceil(n*p) - 1`), matching
// scripts/ci/remote-control-latency-verdict.mjs.

export const LATENCY_HISTOGRAM_MAX_MS = 5000;
const GROWTH = 1.05;

export type LatencyHistogramStats = {
  p50: number;
  p95: number;
  max: number;
  n: number;
};

let cachedEdges: number[] | null = null;

/** Upper edges of every bucket; bucket `j` is `[edges[j-1], edges[j])` (edges[-1] = 0). */
export function latencyHistogramEdges(): readonly number[] {
  if (cachedEdges) return cachedEdges;
  const edges: number[] = [];
  let edge = 1;
  while (edge < LATENCY_HISTOGRAM_MAX_MS) {
    edges.push(edge);
    edge *= GROWTH;
  }
  edges.push(LATENCY_HISTOGRAM_MAX_MS);
  cachedEdges = edges;
  return edges;
}

function bucketIndex(ms: number): number {
  const edges = latencyHistogramEdges();
  let lo = 0;
  let hi = edges.length;
  // First edge strictly greater than `ms`.
  while (lo < hi) {
    const mid = (lo + hi) >>> 1;
    if (edges[mid] <= ms) lo = mid + 1;
    else hi = mid;
  }
  return Math.min(lo, edges.length - 1);
}

function representative(index: number): number {
  const edges = latencyHistogramEdges();
  if (index === 0) return 0.5;
  return Math.sqrt(edges[index - 1] * edges[index]);
}

export class LatencyHistogram {
  private counts: Uint32Array;
  private total = 0;
  private rejectedCount = 0;
  private minMs = Infinity;
  private maxMs = -Infinity;

  constructor() {
    this.counts = new Uint32Array(latencyHistogramEdges().length);
  }

  /** Records one sample. Returns false (and counts it as rejected) for a negative or non-finite value. */
  record(ms: number): boolean {
    if (typeof ms !== 'number' || !Number.isFinite(ms) || ms < 0) {
      this.rejectedCount += 1;
      return false;
    }
    const clamped = Math.min(ms, LATENCY_HISTOGRAM_MAX_MS);
    this.counts[bucketIndex(clamped)] += 1;
    this.total += 1;
    if (clamped < this.minMs) this.minMs = clamped;
    if (clamped > this.maxMs) this.maxMs = clamped;
    return true;
  }

  get n(): number {
    return this.total;
  }

  get rejected(): number {
    return this.rejectedCount;
  }

  percentile(fraction: number): number | null {
    if (this.total === 0) return null;
    const rank = Math.max(0, Math.ceil(this.total * fraction) - 1);
    let cumulative = 0;
    for (let index = 0; index < this.counts.length; index += 1) {
      cumulative += this.counts[index];
      if (cumulative > rank) {
        const value = Math.min(Math.max(representative(index), this.minMs), this.maxMs);
        return Math.round(value);
      }
    }
    return Math.round(this.maxMs);
  }

  p50(): number | null {
    return this.percentile(0.5);
  }

  p95(): number | null {
    return this.percentile(0.95);
  }

  max(): number | null {
    return this.total === 0 ? null : Math.round(this.maxMs);
  }

  stats(): LatencyHistogramStats | null {
    if (this.total === 0) return null;
    return { p50: this.p50() ?? 0, p95: this.p95() ?? 0, max: this.max() ?? 0, n: this.total };
  }

  reset(): void {
    this.counts.fill(0);
    this.total = 0;
    this.rejectedCount = 0;
    this.minMs = Infinity;
    this.maxMs = -Infinity;
  }
}
