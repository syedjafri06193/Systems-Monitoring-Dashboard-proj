import { describe, expect, it } from "vitest";

import { BYTES_PER_POINT, MAX_OFFSET_MS, RingStore, SeriesRing } from "../src/ring.ts";

const BASE = 1_700_000_000_000;

describe("SeriesRing", () => {
  it("stores and reads back points oldest first", () => {
    const r = new SeriesRing(100, BASE);
    for (let i = 0; i < 10; i++) r.push(BASE + i * 200, i);

    expect(r.length).toBe(10);
    for (let i = 0; i < 10; i++) {
      expect(r.timestampAt(i)).toBe(BASE + i * 200);
      expect(r.valueAt(i)).toBe(i);
    }
  });

  it("evicts the oldest when full", () => {
    const r = new SeriesRing(5, BASE);
    for (let i = 0; i < 12; i++) r.push(BASE + i * 100, i);

    expect(r.length).toBe(5);
    expect(r.valueAt(0)).toBe(7);
    expect(r.valueAt(4)).toBe(11);
  });

  it("bounds memory however many points arrive", () => {
    const r = new SeriesRing(50, BASE);
    const before = r.byteLength();
    for (let i = 0; i < 100_000; i++) r.push(BASE + i, i);
    expect(r.byteLength()).toBe(before);
    expect(r.length).toBe(50);
  });

  it("uses 8 bytes per point", () => {
    // The claim the whole representation rests on: Float32 values plus
    // Uint32 offsets. Objects would be ~56 B/pt and 7x the memory.
    const r = new SeriesRing(1000, BASE);
    expect(r.byteLength() / 1000).toBe(BYTES_PER_POINT.typedCompact);
  });

  it("reports the newest point", () => {
    const r = new SeriesRing(10, BASE);
    expect(r.latest()).toBeNull();
    for (let i = 0; i < 7; i++) r.push(BASE + i * 100, i);
    expect(r.latest()).toEqual({ ts: BASE + 600, value: 6 });
  });

  it("reports the newest point across the wrap", () => {
    const r = new SeriesRing(3, BASE);
    for (let i = 0; i < 10; i++) r.push(BASE + i * 100, i);
    expect(r.latest()).toEqual({ ts: BASE + 900, value: 9 });
  });

  it("drops a point older than the base rather than wrapping it", () => {
    // A Uint32 cannot hold a negative offset. Storing one silently
    // produces a timestamp ~49 days in the future, and a single late
    // point would stretch the chart's x-axis by seven weeks.
    const r = new SeriesRing(10, BASE);
    expect(r.push(BASE - 1, 99)).toBe(false);
    expect(r.length).toBe(0);

    r.push(BASE, 1);
    expect(r.push(BASE - 5000, 99)).toBe(false);
    expect(r.length).toBe(1);
    expect(r.latest()!.value).toBe(1);
  });

  it("re-bases before the offset overflows", () => {
    // Uint32 milliseconds covers ~49 days; re-basing early avoids doing
    // it under time pressure at exactly the wrong moment.
    const r = new SeriesRing(10, BASE);
    r.push(BASE, 1);
    r.push(BASE + 1000, 2);

    const far = BASE + MAX_OFFSET_MS - 1000;
    r.push(far, 3);

    // Every timestamp still reads back correctly.
    expect(r.timestampAt(0)).toBe(BASE);
    expect(r.timestampAt(1)).toBe(BASE + 1000);
    expect(r.timestampAt(2)).toBe(far);
    expect(r.baseTsMs).toBeLessThanOrEqual(BASE);
  });

  it("keeps timestamps exact across a re-base with a full buffer", () => {
    const r = new SeriesRing(4, BASE);
    const expected: number[] = [];
    for (let i = 0; i < 4; i++) {
      const ts = BASE + i * 1000;
      r.push(ts, i);
      expected.push(ts);
    }
    // Force a re-base.
    const far = BASE + 0xf0000001;
    r.push(far, 99);

    expect(r.timestampAt(r.length - 1)).toBe(far);
    for (let i = 0; i < r.length - 1; i++) {
      expect(r.timestampAt(i)).toBe(expected[i + 1]);
    }
  });

  it("copies into caller-provided arrays without allocating", () => {
    const r = new SeriesRing(100, BASE);
    for (let i = 0; i < 10; i++) r.push(BASE + i * 100, i);

    const ts = new Float64Array(20);
    const vals = new Float64Array(20);
    const n = r.copyInto(ts, vals);

    expect(n).toBe(10);
    expect(ts[0]).toBe(BASE);
    expect(vals[9]).toBe(9);
  });

  it("filters a copy by time range", () => {
    const r = new SeriesRing(100, BASE);
    for (let i = 0; i < 10; i++) r.push(BASE + i * 100, i);

    const ts = new Float64Array(20);
    const vals = new Float64Array(20);
    const n = r.copyInto(ts, vals, BASE + 300, BASE + 500);

    expect(n).toBe(3);
    expect(vals[0]).toBe(3);
    expect(vals[2]).toBe(5);
  });

  it("does not write past the destination", () => {
    const r = new SeriesRing(100, BASE);
    for (let i = 0; i < 50; i++) r.push(BASE + i, i);

    const ts = new Float64Array(5);
    const vals = new Float64Array(5);
    expect(r.copyInto(ts, vals)).toBe(5);
  });

  it("rejects a non-positive capacity", () => {
    expect(() => new SeriesRing(0, BASE)).toThrow(RangeError);
  });

  it("clears", () => {
    const r = new SeriesRing(10, BASE);
    r.push(BASE, 1);
    r.clear();
    expect(r.length).toBe(0);
    expect(r.latest()).toBeNull();
  });
});

describe("the memory argument", () => {
  it("is 7x smaller than an array of objects", () => {
    // One hour of 1 Hz data for 1000 series is 3.6 M points. As objects
    // that is ~200 MB, which will make the tab sluggish and eventually
    // kill it -- plus 3.6 million objects for the GC to trace.
    const points = 3_600_000;
    const asObjects = (points * BYTES_PER_POINT.objects) / 1e6;
    const asTyped = (points * BYTES_PER_POINT.typedCompact) / 1e6;

    expect(asObjects).toBeCloseTo(201.6, 1);
    expect(asTyped).toBeCloseTo(28.8, 1);
    expect(asObjects / asTyped).toBe(7);
  });

  it("matches the real allocation", () => {
    const store = new RingStore(3600, BASE);
    for (let s = 1; s <= 100; s++) store.ring(s);
    expect(store.byteLength()).toBe(100 * 3600 * BYTES_PER_POINT.typedCompact);
  });
});

describe("RingStore", () => {
  it("creates a ring per series on demand", () => {
    const s = new RingStore(10, BASE);
    s.push(1, BASE, 1);
    s.push(2, BASE, 2);
    s.push(1, BASE + 100, 3);

    expect(s.size).toBe(2);
    expect(s.get(1)!.length).toBe(2);
    expect(s.get(2)!.length).toBe(1);
  });

  it("reports the newest timestamp across all series", () => {
    // This is what a reconnect sends as last_seen, so the server can
    // return exactly the gap.
    const s = new RingStore(10, BASE);
    s.push(1, BASE + 500, 1);
    s.push(2, BASE + 900, 2);
    s.push(3, BASE + 100, 3);

    expect(s.latestTs()).toBe(BASE + 900);
  });

  it("reports zero when empty", () => {
    expect(new RingStore(10, BASE).latestTs()).toBe(0);
  });

  it("deletes a series", () => {
    const s = new RingStore(10, BASE);
    s.push(1, BASE, 1);
    s.delete(1);
    expect(s.size).toBe(0);
    expect(s.has(1)).toBe(false);
  });

  it("lists its series ids", () => {
    const s = new RingStore(10, BASE);
    s.push(3, BASE, 1);
    s.push(1, BASE, 1);
    expect(s.seriesIds().sort()).toEqual([1, 3]);
  });
});
