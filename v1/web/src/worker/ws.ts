/**
 * The WebSocket and decoder, running in a Web Worker.
 *
 * The main thread's only job is rendering. At a thousand messages per
 * second, decoding on the main thread is the difference between a
 * dashboard that stutters and one that is smooth.
 *
 * Decoded data crosses to the main thread as transferable ArrayBuffers, so
 * there is no copy. SharedArrayBuffer would give true zero-copy shared
 * access but requires cross-origin isolation (COOP: same-origin, COEP:
 * require-corp), which means every cross-origin subresource needs CORP
 * headers or it is blocked. That is a real deployment cost for a marginal
 * gain; transferables are enough.
 */

import {
  ControlKind,
  FrameType,
  encodeControl,
  parseControl,
  parseDeltaBatch,
  parseHeartbeat,
  parseSeriesRegistry,
  parseSnapshotHeader,
  readFrames,
  type Control,
  type DeltaBatch,
} from "./decoder.ts";

export interface WorkerConfig {
  url: string;
}

/** Messages the worker posts to the main thread. */
export type WorkerMessage =
  | { type: "open" }
  | { type: "closed"; code: number; reason: string }
  | { type: "registry"; defs: Array<{ id: number; name: string; labels: Record<string, string> }> }
  | { type: "snapshotHeader"; watermarkTsMs: number; seriesCount: number }
  | { type: "delta"; seriesIds: ArrayBuffer; timestamps: ArrayBuffer; values: ArrayBuffer; count: number; snapshot: boolean }
  | { type: "heartbeat"; serverTsMs: number; clientTsMs: number }
  | { type: "control"; control: Control }
  | { type: "error"; message: string };

/** Messages the main thread posts to the worker. */
export type MainMessage =
  | { type: "connect"; url: string }
  | { type: "send"; control: Control }
  | { type: "close" };

let socket: WebSocket | null = null;
let scratch: DeltaBatch | undefined;
// Tracks whether the last snapshot header has been followed by its data,
// so the main thread can tell snapshot points from live ones.
let expectingSnapshotData = false;

function post(msg: WorkerMessage, transfer: Transferable[] = []): void {
  (self as unknown as Worker).postMessage(msg, transfer);
}

function connect(url: string): void {
  socket = new WebSocket(url);
  socket.binaryType = "arraybuffer";

  socket.onopen = () => post({ type: "open" });
  socket.onclose = (e) => post({ type: "closed", code: e.code, reason: e.reason });
  socket.onerror = () => post({ type: "error", message: "websocket error" });

  socket.onmessage = (e) => {
    if (!(e.data instanceof ArrayBuffer)) return;
    try {
      handleMessage(e.data);
    } catch (err) {
      post({ type: "error", message: err instanceof Error ? err.message : String(err) });
    }
  };
}

function handleMessage(buffer: ArrayBuffer): void {
  for (const frame of readFrames(buffer)) {
    switch (frame.type) {
      case FrameType.SeriesRegistry:
        post({ type: "registry", defs: parseSeriesRegistry(frame.payload) });
        break;

      case FrameType.SnapshotHeader: {
        const h = parseSnapshotHeader(frame.payload);
        expectingSnapshotData = true;
        post({ type: "snapshotHeader", ...h });
        break;
      }

      case FrameType.DeltaBatch:
      case FrameType.SnapshotData: {
        scratch = parseDeltaBatch(frame.payload, scratch);
        const snapshot = expectingSnapshotData;
        expectingSnapshotData = false;
        postBatch(scratch, snapshot);
        break;
      }

      case FrameType.Heartbeat:
        post({
          type: "heartbeat",
          serverTsMs: parseHeartbeat(frame.payload),
          clientTsMs: Date.now(),
        });
        break;

      case FrameType.Control:
        post({ type: "control", control: parseControl(frame.payload) });
        break;

      default:
        // Unknown frame types are skipped rather than fatal: the header
        // parses independently of the type precisely so a client can talk
        // to a newer server.
        break;
    }
  }
}

/**
 * Post a batch, transferring its buffers.
 *
 * Copies out of the reusable scratch arrays first: transferring detaches
 * them, and the scratch must survive for the next message.
 */
function postBatch(batch: DeltaBatch, snapshot: boolean): void {
  const n = batch.count;
  const ids = batch.seriesIds.slice(0, n);
  const ts = batch.timestamps.slice(0, n);
  const vals = batch.values.slice(0, n);

  post(
    {
      type: "delta",
      seriesIds: ids.buffer,
      timestamps: ts.buffer,
      values: vals.buffer,
      count: n,
      snapshot,
    },
    [ids.buffer, ts.buffer, vals.buffer],
  );
}

self.onmessage = (e: MessageEvent<MainMessage>) => {
  const msg = e.data;
  switch (msg.type) {
    case "connect":
      connect(msg.url);
      break;
    case "send":
      if (socket && socket.readyState === WebSocket.OPEN) {
        socket.send(encodeControl(msg.control));
      }
      break;
    case "close":
      socket?.close();
      socket = null;
      break;
  }
};

export { ControlKind };
