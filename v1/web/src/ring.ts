/**
 * Typed-array ring buffers.
 *
 * You will hit browser limits before server limits, and memory is the
 * first one. One hour of 1 Hz data for 1000 series is 3.6 M points:
 *
 *   {t, v} objects in an array   ~56 B/pt   ~200 MB
 *   two Float64Arrays             16 B/pt    57.6 MB
 *   Float32 values + Uint32 offs   8 B/pt    28.8 MB
 *
 * The object representation is a 7x penalty and it will make the tab
 * sluggish and eventually kill it. It also creates 3.6 million objects for
 * the garbage collector to trace, which is its own latency problem — and
 * GC pauses are a term in the very budget this project exists to minimise.
 */

/** Milliseconds a Uint32 offset can represent: about 49 days. */
export const MAX_OFFSET_MS = 0xffffffff;

/**
 * Re-base when the newest offset passes this.
 *
 * Well short of the 49-day limit, because re-basing costs a full pass over
 * the buffer and doing it under time pressure at exactly the wrong moment
 * is worse than doing it early.
 */
const REBASE_THRESHOLD = 0xf0000000;

export class SeriesRing {
  private ts: Uint32Array;
  private vals: Float32Array;
  private head = 0;
  private count = 0;
  private base: number;

  constructor(
    readonly capacity: number,
    baseTsMs: number,
  ) {
    if (capacity < 1) throw new RangeError("SeriesRing: capacity must be positive");
    this.ts = new Uint32Array(capacity);
    this.vals = new Float32Array(capacity);
    this.base = baseTsMs;
  }

  get length(): number {
    return this.count;
  }

  get baseTsMs(): number {
    return this.base;
  }

  /**
   * Append a point.
   *
   * Points older than the base are dropped rather than wrapping. A Uint32
   * cannot hold a negative offset, so storing one silently produces a
   * timestamp ~49 days in the future — a single late point would stretch
   * the chart's x-axis by seven weeks.
   */
  push(tsMs: number, value: number): boolean {
    if (tsMs < this.base) return false;

    let offset = tsMs - this.base;
    if (offset > REBASE_THRESHOLD) {
      this.rebase(tsMs);
      offset = tsMs - this.base;
    }

    this.ts[this.head] = offset;
    this.vals[this.head] = value;
    this.head = (this.head + 1) % this.capacity;
    if (this.count < this.capacity) this.count++;
    return true;
  }

  /**
   * Shift the base forward, rewriting stored offsets.
   *
   * The new base is the oldest retained timestamp, so the whole buffer
   * stays representable.
   */
  private rebase(newestTsMs: number): void {
    const oldest = this.count > 0 ? this.timestampAt(0) : newestTsMs;
    const delta = oldest - this.base;
    if (delta <= 0) return;

    for (let i = 0; i < this.count; i++) {
      const idx = this.indexOf(i);
      this.ts[idx] = this.ts[idx] - delta;
    }
    this.base = oldest;
  }

  /** Physical index of the i-th oldest point. */
  private indexOf(i: number): number {
    const start = this.count === this.capacity ? this.head : 0;
    return (start + i) % this.capacity;
  }

  timestampAt(i: number): number {
    return this.base + this.ts[this.indexOf(i)];
  }

  valueAt(i: number): number {
    return this.vals[this.indexOf(i)];
  }

  /** The newest point, or null when empty. */
  latest(): { ts: number; value: number } | null {
    if (this.count === 0) return null;
    const i = this.count - 1;
    return { ts: this.timestampAt(i), value: this.valueAt(i) };
  }

  /**
   * Copy into caller-provided arrays, oldest first.
   *
   * Takes destination arrays rather than allocating, because this runs
   * once per frame per series and allocating two arrays per series per
   * frame is exactly the garbage the typed arrays exist to avoid.
   *
   * @returns the number of points written.
   */
  copyInto(dstTs: Float64Array, dstVals: Float64Array, from = -Infinity, to = Infinity): number {
    let n = 0;
    const max = Math.min(dstTs.length, dstVals.length);
    for (let i = 0; i < this.count && n < max; i++) {
      const t = this.timestampAt(i);
      if (t < from || t > to) continue;
      dstTs[n] = t;
      dstVals[n] = this.valueAt(i);
      n++;
    }
    return n;
  }

  /** Bytes of backing storage, for the memory accounting above. */
  byteLength(): number {
    return this.ts.byteLength + this.vals.byteLength;
  }

  clear(baseTsMs?: number): void {
    this.head = 0;
    this.count = 0;
    if (baseTsMs !== undefined) this.base = baseTsMs;
  }
}

/**
 * All series' rings, keyed by interned id.
 *
 * A Map rather than an array: series ids are dense from the server's point
 * of view but a client subscribes to a subset, and an array indexed by id
 * would allocate a slot for every series in the fleet.
 */
export class RingStore {
  private rings = new Map<number, SeriesRing>();

  constructor(
    private readonly capacity: number,
    private baseTsMs: number,
  ) {}

  /** Get or create the ring for a series. */
  ring(seriesId: number): SeriesRing {
    let r = this.rings.get(seriesId);
    if (!r) {
      r = new SeriesRing(this.capacity, this.baseTsMs);
      this.rings.set(seriesId, r);
    }
    return r;
  }

  has(seriesId: number): boolean {
    return this.rings.has(seriesId);
  }

  get(seriesId: number): SeriesRing | undefined {
    return this.rings.get(seriesId);
  }

  push(seriesId: number, tsMs: number, value: number): boolean {
    return this.ring(seriesId).push(tsMs, value);
  }

  delete(seriesId: number): void {
    this.rings.delete(seriesId);
  }

  get size(): number {
    return this.rings.size;
  }

  seriesIds(): number[] {
    return [...this.rings.keys()];
  }

  /** Total backing storage, for the memory panel. */
  byteLength(): number {
    let n = 0;
    for (const r of this.rings.values()) n += r.byteLength();
    return n;
  }

  /**
   * The newest timestamp across all series.
   *
   * This is what a reconnect reports as `last_seen`, so the server can
   * send exactly the gap (§6.5, §7.3).
   */
  latestTs(): number {
    let newest = -Infinity;
    for (const r of this.rings.values()) {
      const l = r.latest();
      if (l && l.ts > newest) newest = l.ts;
    }
    return newest === -Infinity ? 0 : newest;
  }

  clear(baseTsMs?: number): void {
    if (baseTsMs !== undefined) this.baseTsMs = baseTsMs;
    for (const r of this.rings.values()) r.clear(baseTsMs);
  }
}

/**
 * Bytes per point for each representation, for the docs and the tests.
 *
 * Kept as data rather than prose so the comparison in the README can be
 * asserted rather than claimed.
 */
export const BYTES_PER_POINT = {
  /** {t: number, v: number} object plus the array's pointer to it. */
  objects: 56,
  /** Two Float64Arrays. */
  float64Pair: 16,
  /** Float32 values plus Uint32 millisecond offsets: what this uses. */
  typedCompact: 8,
} as const;
