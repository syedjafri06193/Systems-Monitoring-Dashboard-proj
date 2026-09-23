import { describe, expect, it } from "vitest";

import { ControlKind, type Control, type DeltaBatch } from "../src/worker/decoder.ts";
import { Subscription, backoffMs, type SubscriptionSink, type Transport } from "../src/subscribe.ts";

/** Records what the client sent. */
class FakeTransport implements Transport {
  readonly sent: Control[] = [];
  send(ctrl: Control): void {
    this.sent.push(ctrl);
  }
  kinds(): number[] {
    return this.sent.map((c) => c.kind);
  }
}

/** Records what reached the chart. */
class FakeSink implements SubscriptionSink {
  readonly points: Array<{ seriesId: number; ts: number; value: number }> = [];
  apply(seriesId: number, ts: number, value: number): void {
    this.points.push({ seriesId, ts, value });
  }
  timestamps(): number[] {
    return this.points.map((p) => p.ts);
  }
}

function batch(points: Array<[seriesId: number, ts: number, value: number]>): DeltaBatch {
  return {
    seriesIds: Uint32Array.from(points.map((p) => p[0])),
    timestamps: Float64Array.from(points.map((p) => p[1])),
    values: Float32Array.from(points.map((p) => p[2])),
    count: points.length,
  };
}

describe("the subscribe handshake", () => {
  it("subscribes before requesting the snapshot", () => {
    // The ordering is the whole correctness argument. Subscribing first
    // means every point from this moment is captured by either the
    // snapshot or the buffer -- there is no window in between.
    const t = new FakeTransport();
    const s = new Subscription(t, new FakeSink());
    s.start(["cpu"], 0);

    expect(t.kinds()).toEqual([ControlKind.Subscribe, ControlKind.SnapshotRequest]);
  });

  it("buffers live points instead of rendering them", () => {
    const t = new FakeTransport();
    const sink = new FakeSink();
    const s = new Subscription(t, sink);

    s.start(["cpu"], 0);
    s.onDelta(batch([[1, 1000, 1]]));

    expect(sink.points).toHaveLength(0);
    expect(s.stats().buffered).toBe(1);
    expect(s.currentPhase).toBe("buffering");
  });

  it("discards buffered points at or below the watermark", () => {
    // The inclusive boundary is the entire rule. A point exactly at the
    // watermark is already in the snapshot, so keeping it duplicates.
    const t = new FakeTransport();
    const sink = new FakeSink();
    const s = new Subscription(t, sink);

    s.start(["cpu"], 0);
    s.onDelta(batch([[1, 900, 9]]));   // below: already in the snapshot
    s.onDelta(batch([[1, 1000, 9]]));  // exactly at: already in the snapshot
    s.onDelta(batch([[1, 1200, 3]]));  // above: genuinely new

    s.onSnapshotHeader({ watermarkTsMs: 1000, seriesCount: 1 });
    s.onSnapshotData(batch([[1, 800, 1], [1, 1000, 2]]));

    const stats = s.reconcile();

    expect(stats.discarded).toBe(2);
    expect(sink.timestamps()).toEqual([800, 1000, 1200]);
  });

  it("produces no duplicate timestamps", () => {
    const t = new FakeTransport();
    const sink = new FakeSink();
    const s = new Subscription(t, sink);

    s.start(["cpu"], 0);
    // The live stream overlaps the snapshot heavily, as it does in reality.
    for (let ts = 950; ts <= 1100; ts += 10) s.onDelta(batch([[1, ts, ts]]));

    s.onSnapshotHeader({ watermarkTsMs: 1000, seriesCount: 1 });
    const snapshot: Array<[number, number, number]> = [];
    for (let ts = 900; ts <= 1000; ts += 10) snapshot.push([1, ts, ts]);
    s.onSnapshotData(batch(snapshot));

    s.reconcile();

    const seen = new Set<number>();
    for (const ts of sink.timestamps()) {
      expect(seen.has(ts), `timestamp ${ts} appeared twice`).toBe(false);
      seen.add(ts);
    }
  });

  it("produces no gaps", () => {
    const t = new FakeTransport();
    const sink = new FakeSink();
    const s = new Subscription(t, sink);

    s.start(["cpu"], 0);
    for (let ts = 1010; ts <= 1100; ts += 10) s.onDelta(batch([[1, ts, ts]]));

    s.onSnapshotHeader({ watermarkTsMs: 1000, seriesCount: 1 });
    const snapshot: Array<[number, number, number]> = [];
    for (let ts = 900; ts <= 1000; ts += 10) snapshot.push([1, ts, ts]);
    s.onSnapshotData(batch(snapshot));

    s.reconcile();

    const ts = sink.timestamps().sort((a, b) => a - b);
    for (let i = 1; i < ts.length; i++) {
      expect(ts[i] - ts[i - 1], `gap between ${ts[i - 1]} and ${ts[i]}`).toBe(10);
    }
  });

  it("goes live after reconciling", () => {
    const t = new FakeTransport();
    const sink = new FakeSink();
    const s = new Subscription(t, sink);

    s.start(["cpu"], 0);
    s.onSnapshotHeader({ watermarkTsMs: 1000, seriesCount: 1 });
    s.onSnapshotData(batch([[1, 1000, 1]]));
    s.reconcile();

    expect(s.currentPhase).toBe("live");

    s.onDelta(batch([[1, 1100, 2]]));
    expect(sink.points).toHaveLength(2);
    expect(s.stats().buffered).toBe(0);
  });

  it("stays buffering if no snapshot header arrived", () => {
    // Going live without a cut point would render buffered points against
    // no history: a chart that starts mid-air. Staying in buffering is
    // the honest state.
    const t = new FakeTransport();
    const sink = new FakeSink();
    const s = new Subscription(t, sink);

    s.start(["cpu"], 0);
    s.onDelta(batch([[1, 1000, 1]]));
    const stats = s.reconcile();

    expect(stats.phase).toBe("buffering");
    expect(sink.points).toHaveLength(0);
  });

  it("uses the server's watermark rather than the last snapshot point", () => {
    // "Use the timestamp of the last snapshot point" is wrong whenever a
    // series has no recent data: a series idle for ten minutes would put
    // the cut ten minutes back and re-apply everything since.
    const t = new FakeTransport();
    const sink = new FakeSink();
    const s = new Subscription(t, sink);

    s.start(["cpu"], 0);
    s.onDelta(batch([[1, 5500, 1]]));

    // The stream is live to 9000 but this series' last point was at 5000.
    s.onSnapshotHeader({ watermarkTsMs: 9000, seriesCount: 1 });
    s.onSnapshotData(batch([[1, 5000, 1]]));

    const stats = s.reconcile();

    // The buffered point at 5500 is below the real watermark, so it was
    // already covered by the snapshot request and must be discarded.
    expect(stats.discarded).toBe(1);
    expect(sink.timestamps()).toEqual([5000]);
  });
});

describe("the four cases take one path", () => {
  it("reconnect resumes from the last point seen", () => {
    const t = new FakeTransport();
    const s = new Subscription(t, new FakeSink());

    s.start(["cpu"], 0);
    s.onSnapshotHeader({ watermarkTsMs: 1000, seriesCount: 1 });
    s.onSnapshotData(batch([[1, 1000, 1]]));
    s.reconcile();
    s.onDelta(batch([[1, 2000, 2]]));

    t.sent.length = 0;
    s.reconnect();

    expect(t.kinds()).toEqual([ControlKind.Subscribe, ControlKind.SnapshotRequest]);
    expect(t.sent[1].from_ms).toBe(2001);
    expect(s.stats().reason).toBe("reconnect");
  });

  it("adding a series snapshots just that series", () => {
    const t = new FakeTransport();
    const s = new Subscription(t, new FakeSink());
    s.start(["cpu"], 0);

    t.sent.length = 0;
    s.addSeries(["memory"], 500);

    expect(t.sent[0].series).toEqual(["memory"]);
    expect(t.sent[1].from_ms).toBe(500);
    expect(s.currentPhase).toBe("buffering");
    expect(s.stats().reason).toBe("add_series");
  });

  it("widening the range snapshots the newly visible portion", () => {
    const t = new FakeTransport();
    const s = new Subscription(t, new FakeSink());
    s.start(["cpu"], 1000);

    t.sent.length = 0;
    s.widenRange(0, 1000);

    expect(t.kinds()).toEqual([ControlKind.SnapshotRequest]);
    expect(t.sent[0].from_ms).toBe(0);
    expect(t.sent[0].to_ms).toBe(1000);
    expect(s.stats().reason).toBe("widen_range");
  });

  it("every case returns to buffering, so reconciliation always runs", () => {
    const t = new FakeTransport();
    const s = new Subscription(t, new FakeSink());

    for (const go of [
      () => s.start(["cpu"], 0),
      () => s.reconnect(),
      () => s.addSeries(["mem"], 0),
      () => s.widenRange(0, 100),
      () => s.resume(),
    ]) {
      s.onSnapshotHeader({ watermarkTsMs: 1, seriesCount: 1 });
      s.reconcile();
      expect(s.currentPhase).toBe("live");
      go();
      expect(s.currentPhase).toBe("buffering");
    }
  });
});

describe("visibility", () => {
  it("pauses the stream when hidden", () => {
    // A backgrounded tab accumulates a queue it cannot drain, trips the
    // server's conflation and drop logic, and looks like a broken client.
    const t = new FakeTransport();
    const s = new Subscription(t, new FakeSink());
    s.pause();
    expect(t.kinds()).toEqual([ControlKind.Pause]);
  });

  it("resumes with the last-seen timestamp so the gap is backfilled", () => {
    const t = new FakeTransport();
    const s = new Subscription(t, new FakeSink());

    s.start(["cpu"], 0);
    s.onSnapshotHeader({ watermarkTsMs: 100, seriesCount: 1 });
    s.onSnapshotData(batch([[1, 100, 1]]));
    s.reconcile();
    s.onDelta(batch([[1, 5000, 2]]));

    t.sent.length = 0;
    s.resume();

    expect(t.sent[0].kind).toBe(ControlKind.Resume);
    expect(t.sent[0].last_seen_ms).toBe(5000);
  });
});

describe("backoffMs", () => {
  it("uses full jitter", () => {
    // Math.random() * base, not base/2 + random*base/2. Without full
    // jitter a gateway restart makes every client reconnect at nearly the
    // same moment and knock it over again.
    const samples = Array.from({ length: 2000 }, () => backoffMs(5));
    const max = Math.max(...samples);
    const min = Math.min(...samples);

    expect(min).toBeLessThan(max * 0.1);
    expect(max).toBeGreaterThan(0);
  });

  it("grows exponentially and caps", () => {
    const always1 = () => 1;
    expect(backoffMs(0, 30_000, always1)).toBe(250);
    expect(backoffMs(1, 30_000, always1)).toBe(500);
    expect(backoffMs(4, 30_000, always1)).toBe(4000);
    expect(backoffMs(20, 30_000, always1)).toBe(30_000);
  });

  it("never returns a negative delay", () => {
    expect(backoffMs(-5, 30_000, () => 0.5)).toBeGreaterThanOrEqual(0);
  });

  it("spreads a thundering herd", () => {
    // 1000 clients reconnecting at attempt 5 should not cluster.
    const delays = Array.from({ length: 1000 }, () => backoffMs(5));
    const buckets = new Array(10).fill(0);
    const cap = Math.min(30_000, 250 * 2 ** 5);
    for (const d of delays) buckets[Math.min(9, Math.floor((d / cap) * 10))]++;

    // Every decile should hold a meaningful share, not one spike.
    for (const n of buckets) expect(n).toBeGreaterThan(30);
  });
});
