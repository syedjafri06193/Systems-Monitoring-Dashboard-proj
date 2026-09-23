/**
 * The uPlot wrapper and the rAF loop.
 *
 * Two rules govern this file:
 *
 *   1. Never render on message arrival. At a thousand messages per second
 *      that is a thousand render attempts into a 60 Hz display. Accumulate
 *      on arrival, render at most once per frame. If 500 messages land
 *      within one frame that is one render, not 500 -- usually the single
 *      change that separates a dashboard that stutters from one that is
 *      smooth.
 *
 *   2. Never render the chart through React. Reconciliation on every frame
 *      is exactly the overhead being avoided. React is fine for layout and
 *      controls; the canvas is handed to uPlot and updated imperatively.
 */

import uPlot from "uplot";

import type { RingStore } from "./ring.ts";

export interface ChartOptions {
  element: HTMLElement;
  title?: string;
  /** Visible window in milliseconds. */
  windowMs: number;
}

export class StreamChart {
  private plot: uPlot | null = null;
  private dirty = false;
  private rafHandle = 0;
  private seriesOrder: number[] = [];
  private labels = new Map<number, string>();

  // Reused across frames. Allocating these per frame is the garbage the
  // typed-array rings exist to avoid.
  private xs: Float64Array;
  private ys: Float64Array[] = [];

  private frameTimes: number[] = [];
  private lastFrameMs = 0;

  constructor(
    private readonly opts: ChartOptions,
    private readonly rings: RingStore,
  ) {
    this.xs = new Float64Array(4096);
  }

  /** Register a series, in display order. */
  addSeries(id: number, label: string): void {
    if (this.labels.has(id)) return;
    this.labels.set(id, label);
    this.seriesOrder.push(id);
    this.ys.push(new Float64Array(this.xs.length));
    this.rebuild();
  }

  /** Mark the chart as needing a redraw. Cheap, called per message. */
  markDirty(): void {
    this.dirty = true;
  }

  start(): void {
    const frame = (now: number) => {
      this.rafHandle = requestAnimationFrame(frame);

      if (this.lastFrameMs > 0) {
        this.frameTimes.push(now - this.lastFrameMs);
        if (this.frameTimes.length > 240) this.frameTimes.shift();
      }
      this.lastFrameMs = now;

      if (!this.dirty) return;
      this.dirty = false;
      this.draw();
    };
    this.rafHandle = requestAnimationFrame(frame);
  }

  stop(): void {
    if (this.rafHandle) cancelAnimationFrame(this.rafHandle);
    this.rafHandle = 0;
  }

  private rebuild(): void {
    this.plot?.destroy();

    const series: uPlot.Series[] = [{}];
    const palette = ["#4fc3f7", "#81c784", "#ffb74d", "#e57373", "#ba68c8", "#4db6ac"];
    this.seriesOrder.forEach((id, i) => {
      series.push({
        label: this.labels.get(id) ?? String(id),
        stroke: palette[i % palette.length],
        width: 1,
        // No points: at a thousand samples per series they are invisible
        // and cost a draw call each.
        points: { show: false },
      });
    });

    this.plot = new uPlot(
      {
        title: this.opts.title,
        width: this.opts.element.clientWidth || 800,
        height: this.opts.element.clientHeight || 300,
        series,
        scales: { x: { time: true } },
        cursor: { drag: { x: true, y: false } },
      },
      [new Float64Array(0), ...this.seriesOrder.map(() => new Float64Array(0))],
      this.opts.element,
    );
  }

  /**
   * Read straight from the typed-array rings into uPlot's arrays.
   *
   * No intermediate objects, no per-frame allocation in the steady state.
   */
  private draw(): void {
    if (!this.plot || this.seriesOrder.length === 0) return;

    const now = Date.now();
    const from = now - this.opts.windowMs;

    // uPlot needs one shared x-axis, so the first series defines the
    // timestamps and the rest are sampled onto it. For a synchronous
    // collector every series shares a cadence, which is the common case.
    const first = this.rings.get(this.seriesOrder[0]);
    if (!first) return;

    const n = first.copyInto(this.xs, this.ys[0], from, now);
    for (let s = 1; s < this.seriesOrder.length; s++) {
      const r = this.rings.get(this.seriesOrder[s]);
      if (!r) {
        this.ys[s].fill(NaN, 0, n);
        continue;
      }
      // A scratch x array is needed because copyInto writes both.
      r.copyInto(this.scratchX(n), this.ys[s], from, now);
    }

    const data: uPlot.AlignedData = [
      // uPlot's x-axis is in seconds when time: true.
      Float64Array.from(this.xs.subarray(0, n), (ms) => ms / 1000),
      ...this.ys.map((y) => y.subarray(0, n)),
    ] as unknown as uPlot.AlignedData;

    this.plot.setData(data);
  }

  private scratchXBuf: Float64Array | null = null;
  private scratchX(n: number): Float64Array {
    if (!this.scratchXBuf || this.scratchXBuf.length < n) {
      this.scratchXBuf = new Float64Array(Math.max(n, 4096));
    }
    return this.scratchXBuf;
  }

  /** Frame-time distribution, for the performance panel. */
  frameStats(): { p50: number; p95: number; p99: number; count: number } {
    if (this.frameTimes.length === 0) return { p50: 0, p95: 0, p99: 0, count: 0 };
    const s = [...this.frameTimes].sort((a, b) => a - b);
    const at = (q: number) => s[Math.min(s.length - 1, Math.max(0, Math.ceil(q * s.length) - 1))];
    return { p50: at(0.5), p95: at(0.95), p99: at(0.99), count: s.length };
  }

  destroy(): void {
    this.stop();
    this.plot?.destroy();
    this.plot = null;
  }
}
