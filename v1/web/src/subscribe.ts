/**
 * The snapshot/stream handshake, client side.
 *
 * A chart is part history and part live stream, and joining them without
 * gaps or duplicates is the subtlest correctness problem in the project.
 * Both obvious orderings fail:
 *
 *   query then subscribe   -> GAP between the query's cut and the
 *                             subscription's start, which looks like a
 *                             quiet period rather than a bug
 *   subscribe then query   -> DUPLICATES, and rate calculations
 *                             double-count
 *
 * The correct procedure is:
 *
 *   1 subscribe, buffer (do not render)
 *   2 server reads the watermark BEFORE its snapshot scan
 *   3 snapshot returns data plus an explicit watermark_ts
 *   4 render the snapshot
 *   5 drain the buffer, discarding ts <= watermark
 *   6 go live
 *
 * This module implements it once. The same path serves the initial load,
 * a reconnect, adding a series, and widening the time range — four
 * hand-written variants would be four different off-by-one bugs.
 */

import { ControlKind, type Control, type DeltaBatch, type SnapshotHeader } from "./worker/decoder.ts";

export type Phase = "idle" | "buffering" | "live";

/** One buffered point, held only between subscribe and reconcile. */
interface BufferedPoint {
  seriesId: number;
  ts: number;
  value: number;
}

export interface SubscriptionSink {
  /** Apply a point to the chart's ring buffers. */
  apply(seriesId: number, tsMs: number, value: number): void;
}

export interface Transport {
  send(ctrl: Control): void;
}

/** Why a snapshot was requested. Metrics only: all four take one path. */
export type Reason = "initial_load" | "reconnect" | "add_series" | "widen_range";

export interface SubscriptionStats {
  phase: Phase;
  buffered: number;
  /** Points discarded because the snapshot already contained them. */
  discarded: number;
  applied: number;
  watermarkTsMs: number;
  lastSeenTsMs: number;
  reason: Reason | null;
}

/**
 * Drives one connection's subscription lifecycle.
 *
 * Deliberately transport-agnostic: it is handed decoded frames and it
 * emits control messages, which is what makes the ordering testable
 * without a browser or a socket.
 */
export class Subscription {
  private phase: Phase = "idle";
  private buffer: BufferedPoint[] = [];
  private watermark = -Infinity;
  private discarded = 0;
  private applied = 0;
  private lastSeen = 0;
  private reason: Reason | null = null;
  private series: string[] = [];

  constructor(
    private readonly transport: Transport,
    private readonly sink: SubscriptionSink,
  ) {}

  /**
   * Begin. Step 1: subscribe first, then buffer.
   *
   * Subscribing before requesting the snapshot is what guarantees no gap:
   * from this moment every point is captured, either by the snapshot or by
   * the buffer.
   */
  start(series: string[], fromMs: number, reason: Reason = "initial_load"): void {
    this.series = series;
    this.reason = reason;
    this.phase = "buffering";
    this.buffer.length = 0;
    this.watermark = -Infinity;

    this.transport.send({ kind: ControlKind.Subscribe, series });
    this.transport.send({ kind: ControlKind.SnapshotRequest, series, from_ms: fromMs });
  }

  /** Feed a decoded live batch. */
  onDelta(batch: DeltaBatch): void {
    for (let i = 0; i < batch.count; i++) {
      const seriesId = batch.seriesIds[i];
      const ts = batch.timestamps[i];
      const value = batch.values[i];

      if (this.phase === "buffering") {
        this.buffer.push({ seriesId, ts, value });
        continue;
      }
      this.applyPoint(seriesId, ts, value);
    }
  }

  /**
   * Feed the snapshot header. Steps 2-3: the explicit cut point.
   *
   * The server returns the watermark rather than the client inferring it.
   * "Use the timestamp of the last snapshot point" is wrong whenever a
   * series has no recent data: a series idle for ten minutes would place
   * the cut ten minutes back, and every live point since would be
   * re-applied as a duplicate.
   */
  onSnapshotHeader(header: SnapshotHeader): void {
    this.watermark = header.watermarkTsMs;
  }

  /** Feed the snapshot's data. Step 4: render it. */
  onSnapshotData(batch: DeltaBatch): void {
    for (let i = 0; i < batch.count; i++) {
      this.applyPoint(batch.seriesIds[i], batch.timestamps[i], batch.values[i]);
    }
  }

  /**
   * Steps 5-6: drain the buffer, discard at or below the watermark, go
   * live.
   *
   * The comparison is `<=`, not `<`. A point exactly at the watermark is
   * already in the snapshot, so keeping it duplicates — and an
   * off-by-one here shows up as a single doubled sample that nobody
   * notices until a rate calculation is wrong.
   */
  reconcile(): SubscriptionStats {
    if (this.watermark === -Infinity) {
      // No snapshot header arrived. Going live anyway would render
      // buffered points against no history, which is a chart that starts
      // mid-air; staying in buffering is the honest state.
      return this.stats();
    }

    for (const p of this.buffer) {
      if (p.ts <= this.watermark) {
        this.discarded++;
        continue;
      }
      this.applyPoint(p.seriesId, p.ts, p.value);
    }
    this.buffer.length = 0;
    this.phase = "live";
    return this.stats();
  }

  private applyPoint(seriesId: number, ts: number, value: number): void {
    this.sink.apply(seriesId, ts, value);
    this.applied++;
    if (ts > this.lastSeen) this.lastSeen = ts;
  }

  /**
   * Reconnect. The same path as the initial load, with `from` set to the
   * last point seen.
   */
  reconnect(): void {
    this.start(this.series, this.lastSeen + 1, "reconnect");
  }

  /** Add a series to a live chart: snapshot just that series. */
  addSeries(series: string[], fromMs: number): void {
    this.series = [...new Set([...this.series, ...series])];
    this.phase = "buffering";
    this.watermark = -Infinity;
    this.reason = "add_series";
    this.transport.send({ kind: ControlKind.Subscribe, series });
    this.transport.send({ kind: ControlKind.SnapshotRequest, series, from_ms: fromMs });
  }

  /** Widen the visible range: snapshot the newly visible portion. */
  widenRange(fromMs: number, toMs: number): void {
    this.phase = "buffering";
    this.watermark = -Infinity;
    this.reason = "widen_range";
    this.transport.send({
      kind: ControlKind.SnapshotRequest,
      series: this.series,
      from_ms: fromMs,
      to_ms: toMs,
    });
  }

  /**
   * The tab was hidden.
   *
   * Browsers throttle rAF and timers in hidden tabs, often to once per
   * second. Without pausing, a backgrounded tab accumulates a queue it
   * cannot drain, trips the server's conflation and drop logic, and looks
   * like a broken client. Pausing server-side is better for everyone.
   */
  pause(): void {
    this.transport.send({ kind: ControlKind.Pause });
  }

  /** The tab is visible again: resume and backfill the gap. */
  resume(): void {
    this.phase = "buffering";
    this.watermark = -Infinity;
    this.transport.send({
      kind: ControlKind.Resume,
      series: this.series,
      last_seen_ms: this.lastSeen,
    });
  }

  get currentPhase(): Phase {
    return this.phase;
  }

  get lastSeenTs(): number {
    return this.lastSeen;
  }

  stats(): SubscriptionStats {
    return {
      phase: this.phase,
      buffered: this.buffer.length,
      discarded: this.discarded,
      applied: this.applied,
      watermarkTsMs: this.watermark === -Infinity ? 0 : this.watermark,
      lastSeenTsMs: this.lastSeen,
      reason: this.reason,
    };
  }
}

/**
 * Reconnect backoff with full jitter.
 *
 * `Math.random() * base`, not `base/2 + random*base/2`. Without full
 * jitter a gateway restart makes every client reconnect at nearly the same
 * moment and knock it over again — the thundering herd that turns a
 * ten-second blip into an outage.
 */
export function backoffMs(attempt: number, cap = 30_000, rng: () => number = Math.random): number {
  const base = Math.min(cap, 250 * 2 ** Math.max(0, attempt));
  return rng() * base;
}
