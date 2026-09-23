/**
 * The binary wire decoder.
 *
 * Runs in a Web Worker. The main thread's only job is rendering, and
 * decoding a thousand messages per second on it is the difference between
 * a dashboard that stutters and one that is smooth.
 *
 * Mirrors internal/wire in Go. The two must agree byte for byte, which is
 * why both sides have tests asserting the same layouts against literal
 * bytes rather than against each other.
 */

export const FrameType = {
  SeriesRegistry: 0x01,
  SnapshotHeader: 0x02,
  DeltaBatch: 0x03,
  Heartbeat: 0x04,
  Control: 0x05,
  SnapshotData: 0x06,
} as const;

export type FrameTypeValue = (typeof FrameType)[keyof typeof FrameType];

export const HEADER_LEN = 5;

/** Mirrors wire.MaxPayloadLen. */
export const MAX_PAYLOAD_LEN = 16 << 20;

export interface Frame {
  type: number;
  payload: DataView;
}

export class DecodeError extends Error {}

/**
 * Split a message into frames.
 *
 * Several frames routinely coalesce into one WebSocket message, so the
 * reader walks them rather than assuming one frame per message.
 *
 * Returns views over the original buffer rather than copies: at 5 Hz
 * across a thousand series, a copy per frame is the allocation the binary
 * format exists to avoid.
 */
export function readFrames(buffer: ArrayBuffer): Frame[] {
  const view = new DataView(buffer);
  const out: Frame[] = [];
  let offset = 0;

  while (offset < buffer.byteLength) {
    if (buffer.byteLength - offset < HEADER_LEN) {
      throw new DecodeError(`truncated frame header at offset ${offset}`);
    }
    const type = view.getUint8(offset);
    const len = view.getUint32(offset + 1, false);

    if (len > MAX_PAYLOAD_LEN) {
      // A corrupt length prefix would otherwise make the next allocation
      // enormous. Bound on the claim, not on the read.
      throw new DecodeError(`frame claims ${len} bytes, over the maximum`);
    }
    if (offset + HEADER_LEN + len > buffer.byteLength) {
      throw new DecodeError(`frame claims ${len} bytes but only ${buffer.byteLength - offset - HEADER_LEN} remain`);
    }

    out.push({
      type,
      payload: new DataView(buffer, offset + HEADER_LEN, len),
    });
    offset += HEADER_LEN + len;
  }

  return out;
}

export interface SeriesDef {
  id: number;
  name: string;
  labels: Record<string, string>;
}

const UNIT_SEPARATOR = 0x1f;

/** Parse a SERIES_REGISTRY payload. */
export function parseSeriesRegistry(payload: DataView): SeriesDef[] {
  if (payload.byteLength < 4) throw new DecodeError("registry payload too short");

  const count = payload.getUint32(0, false);
  // Each entry is at least 6 bytes; a count claiming more than could fit
  // is corrupt, and checking before allocating stops a small hostile frame
  // reserving a huge array.
  if (count * 6 > payload.byteLength) {
    throw new DecodeError(`registry claims ${count} entries in ${payload.byteLength} bytes`);
  }

  const decoder = new TextDecoder();
  const out: SeriesDef[] = [];
  let offset = 4;

  for (let i = 0; i < count; i++) {
    if (offset + 6 > payload.byteLength) throw new DecodeError("truncated registry entry");
    const id = payload.getUint32(offset, false);
    const nbytes = payload.getUint16(offset + 4, false);
    offset += 6;

    if (offset + nbytes > payload.byteLength) throw new DecodeError("truncated label blob");
    const bytes = new Uint8Array(payload.buffer, payload.byteOffset + offset, nbytes);
    offset += nbytes;

    out.push(parseLabelKey(id, bytes, decoder));
  }

  return out;
}

function parseLabelKey(id: number, bytes: Uint8Array, decoder: TextDecoder): SeriesDef {
  const parts: string[] = [];
  let start = 0;
  for (let i = 0; i < bytes.length; i++) {
    if (bytes[i] === UNIT_SEPARATOR) {
      parts.push(decoder.decode(bytes.subarray(start, i)));
      start = i + 1;
    }
  }
  parts.push(decoder.decode(bytes.subarray(start)));

  const labels: Record<string, string> = {};
  for (let i = 1; i < parts.length; i++) {
    const eq = parts[i].indexOf("=");
    if (eq < 0) throw new DecodeError(`malformed label pair ${JSON.stringify(parts[i])}`);
    labels[parts[i].slice(0, eq)] = parts[i].slice(eq + 1);
  }

  return { id, name: parts[0], labels };
}

export interface SnapshotHeader {
  watermarkTsMs: number;
  seriesCount: number;
}

/** Parse a SNAPSHOT_HEADER payload. */
export function parseSnapshotHeader(payload: DataView): SnapshotHeader {
  if (payload.byteLength < 12) throw new DecodeError("snapshot header too short");
  // getBigInt64 then Number: timestamps are milliseconds and stay far
  // inside Number.MAX_SAFE_INTEGER (which covers to the year 287396), so
  // the conversion is exact.
  const watermark = Number(payload.getBigInt64(0, false));
  return { watermarkTsMs: watermark, seriesCount: payload.getUint32(8, false) };
}

/**
 * A decoded batch, in struct-of-arrays form.
 *
 * Not an array of point objects. At 5 Hz across a thousand series that
 * would be five thousand objects per second for the garbage collector to
 * trace, which is the exact cost the wire format was designed to avoid —
 * throwing it away at the last step would be absurd.
 */
export interface DeltaBatch {
  seriesIds: Uint32Array;
  timestamps: Float64Array;
  values: Float32Array;
  count: number;
}

/**
 * Parse a DELTA_BATCH payload into reusable arrays.
 *
 * `into` is reused across calls by the worker. Growing is handled here so
 * callers never allocate in the steady state.
 */
export function parseDeltaBatch(payload: DataView, into?: DeltaBatch): DeltaBatch {
  if (payload.byteLength === 0) {
    return into ? ((into.count = 0), into) : emptyBatch();
  }
  if (payload.byteLength < 12) throw new DecodeError("delta batch payload too short");

  const baseTs = Number(payload.getBigInt64(0, false));
  const count = payload.getUint32(8, false);

  // Minimum 9 bytes per point (4 id + 1 varint + 4 value).
  if (count * 9 > payload.byteLength) {
    throw new DecodeError(`batch claims ${count} points in ${payload.byteLength} bytes`);
  }

  const batch = ensureCapacity(into, count);
  let offset = 12;

  for (let i = 0; i < count; i++) {
    if (offset + 4 > payload.byteLength) throw new DecodeError("truncated series id");
    batch.seriesIds[i] = payload.getUint32(offset, false);
    offset += 4;

    const [delta, read] = readUvarint(payload, offset);
    offset += read;

    if (offset + 4 > payload.byteLength) throw new DecodeError("truncated value");
    batch.timestamps[i] = baseTs + delta;
    batch.values[i] = payload.getFloat32(offset, false);
    offset += 4;
  }

  batch.count = count;
  return batch;
}

function emptyBatch(): DeltaBatch {
  return {
    seriesIds: new Uint32Array(0),
    timestamps: new Float64Array(0),
    values: new Float32Array(0),
    count: 0,
  };
}

function ensureCapacity(batch: DeltaBatch | undefined, n: number): DeltaBatch {
  if (!batch || batch.seriesIds.length < n) {
    // Grow geometrically rather than exactly, so a stream whose batch size
    // creeps upward does not reallocate on every message.
    const cap = Math.max(n, (batch?.seriesIds.length ?? 0) * 2, 256);
    return {
      seriesIds: new Uint32Array(cap),
      timestamps: new Float64Array(cap),
      values: new Float32Array(cap),
      count: 0,
    };
  }
  return batch;
}

/**
 * Read a Go-compatible unsigned varint.
 *
 * Little-endian base-128, seven bits per byte, high bit as continuation.
 * Must match encoding/binary.PutUvarint exactly or every timestamp after
 * the first is wrong.
 */
export function readUvarint(view: DataView, offset: number): [value: number, bytesRead: number] {
  let result = 0;
  let shift = 0;

  for (let i = 0; i < 10; i++) {
    if (offset + i >= view.byteLength) throw new DecodeError("truncated varint");
    const byte = view.getUint8(offset + i);
    // Number, not bit operations: JavaScript's bitwise operators coerce to
    // int32, so a timestamp delta over ~2^31 ms would silently go
    // negative. Multiplying keeps full 53-bit precision.
    result += (byte & 0x7f) * Math.pow(2, shift);
    if ((byte & 0x80) === 0) return [result, i + 1];
    shift += 7;
  }
  throw new DecodeError("varint longer than 10 bytes");
}

/** Parse a HEARTBEAT payload. */
export function parseHeartbeat(payload: DataView): number {
  if (payload.byteLength < 8) throw new DecodeError("heartbeat payload too short");
  return Number(payload.getBigInt64(0, false));
}

export const ControlKind = {
  Subscribe: 1,
  Unsubscribe: 2,
  SnapshotRequest: 3,
  Pause: 4,
  Resume: 5,
  ConflationNotice: 6,
  Error: 7,
  Drain: 8,
} as const;

export interface Control {
  kind: number;
  series?: string[];
  from_ms?: number;
  to_ms?: number;
  last_seen_ms?: number;
  conflated?: number;
  dropped?: number;
  message?: string;
}

export function parseControl(payload: DataView): Control {
  const bytes = new Uint8Array(payload.buffer, payload.byteOffset, payload.byteLength);
  return JSON.parse(new TextDecoder().decode(bytes)) as Control;
}

export function encodeControl(ctrl: Control): Uint8Array {
  return new TextEncoder().encode(JSON.stringify(ctrl));
}
