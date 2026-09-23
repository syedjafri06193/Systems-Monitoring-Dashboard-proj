/**
 * Application entry point.
 *
 * Wires the worker, the rings, the subscription handshake and the chart.
 * Deliberately thin: every decision lives in the module that owns it.
 */

import { StreamChart } from "./chart.ts";
import { RingStore } from "./ring.ts";
import { Subscription, backoffMs, type Transport } from "./subscribe.ts";
import { ControlKind, type Control } from "./worker/decoder.ts";
import type { MainMessage, WorkerMessage } from "./worker/ws.ts";

/** Hot retention, matching the gateway's ring (§8.2, tier 0). */
const WINDOW_MS = 15 * 60 * 1000;
/** 15 minutes at 5 Hz. */
const RING_CAPACITY = 4500;

class App {
  private worker: Worker;
  private rings = new RingStore(RING_CAPACITY, Date.now() - WINDOW_MS);
  private chart: StreamChart;
  private subscription: Subscription;

  private reconnectAttempt = 0;
  private latency: number[] = [];
  private clockOffsetMs = 0;
  private series: string[];

  constructor(element: HTMLElement, url: string, series: string[]) {
    this.series = series;
    this.worker = new Worker(new URL("./worker/ws.ts", import.meta.url), { type: "module" });

    this.chart = new StreamChart({ element, title: "live", windowMs: WINDOW_MS }, this.rings);

    const transport: Transport = {
      send: (control: Control) => this.post({ type: "send", control }),
    };

    this.subscription = new Subscription(transport, {
      apply: (seriesId, ts, value) => {
        this.rings.push(seriesId, ts, value);
        // Mark dirty; do NOT render here. At a thousand messages per
        // second, rendering on arrival is a thousand render attempts
        // into a 60 Hz display.
        this.chart.markDirty();
      },
    });

    this.worker.onmessage = (e: MessageEvent<WorkerMessage>) => this.onWorkerMessage(e.data);
    this.post({ type: "connect", url });
    this.chart.start();
    this.wireVisibility();
  }

  private post(msg: MainMessage): void {
    this.worker.postMessage(msg);
  }

  private onWorkerMessage(msg: WorkerMessage): void {
    switch (msg.type) {
      case "open":
        this.reconnectAttempt = 0;
        if (this.subscription.lastSeenTs > 0) {
          this.subscription.reconnect();
        } else {
          this.subscription.start(this.series, Date.now() - WINDOW_MS);
        }
        break;

      case "closed":
        this.scheduleReconnect();
        break;

      case "registry":
        for (const d of msg.defs) {
          this.chart.addSeries(d.id, labelOf(d.name, d.labels));
        }
        break;

      case "snapshotHeader":
        this.subscription.onSnapshotHeader(msg);
        break;

      case "delta": {
        const batch = {
          seriesIds: new Uint32Array(msg.seriesIds),
          timestamps: new Float64Array(msg.timestamps),
          values: new Float32Array(msg.values),
          count: msg.count,
        };
        if (msg.snapshot) {
          this.subscription.onSnapshotData(batch);
          this.subscription.reconcile();
        } else {
          this.subscription.onDelta(batch);
          this.recordLatency(batch.timestamps, batch.count);
        }
        break;
      }

      case "heartbeat":
        // The heartbeat carries the server clock, which lets the client
        // estimate the offset between two machines' clocks. The absolute
        // latency number is approximate across machines; the shape of the
        // distribution stays meaningful.
        this.clockOffsetMs = msg.clientTsMs - msg.serverTsMs;
        break;

      case "control":
        if (msg.control.kind === ControlKind.Drain) {
          // Drain: reconnect with jitter rather than immediately, or a
          // rolling deploy becomes a thundering herd.
          this.scheduleReconnect();
        }
        break;

      case "error":
        console.warn("stream error:", msg.message);
        break;
    }
  }

  /**
   * The only latency measurement that means anything: origin timestamp
   * stamped at the collector, compared at render time.
   */
  private recordLatency(timestamps: Float64Array, count: number): void {
    const now = performance.timeOrigin + performance.now();
    for (let i = 0; i < count; i++) {
      this.latency.push(now - timestamps[i] - this.clockOffsetMs);
    }
    if (this.latency.length > 10000) {
      this.latency = this.latency.slice(-10000);
    }
  }

  latencySummary(): { p50: number; p95: number; p99: number; count: number } {
    if (this.latency.length === 0) return { p50: 0, p95: 0, p99: 0, count: 0 };
    const s = [...this.latency].sort((a, b) => a - b);
    const at = (q: number) => s[Math.min(s.length - 1, Math.max(0, Math.ceil(q * s.length) - 1))];
    return { p50: at(0.5), p95: at(0.95), p99: at(0.99), count: s.length };
  }

  private scheduleReconnect(): void {
    const delay = backoffMs(this.reconnectAttempt++);
    setTimeout(() => this.post({ type: "connect", url: location.origin.replace(/^http/, "ws") + "/stream" }), delay);
  }

  private wireVisibility(): void {
    document.addEventListener("visibilitychange", () => {
      if (document.hidden) {
        this.subscription.pause();
      } else {
        this.subscription.resume();
      }
    });
  }
}

function labelOf(name: string, labels: Record<string, string>): string {
  const pairs = Object.entries(labels)
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([k, v]) => `${k}=${v}`)
    .join(",");
  return pairs ? `${name}{${pairs}}` : name;
}

const root = document.getElementById("chart");
if (root) {
  const url = location.origin.replace(/^http/, "ws") + "/stream";
  const series = new URLSearchParams(location.search).getAll("series");
  const app = new App(root, url, series.length ? series : ["node_cpu_percent"]);

  // The performance panel. Frame time and end-to-end latency are the two
  // numbers this project is actually about.
  setInterval(() => {
    const el = document.getElementById("stats");
    if (!el) return;
    const l = app.latencySummary();
    el.textContent =
      `latency p50 ${l.p50.toFixed(0)}ms p99 ${l.p99.toFixed(0)}ms (n=${l.count})`;
  }, 1000);
}

export { App };
