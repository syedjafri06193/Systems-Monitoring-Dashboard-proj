# Wire format

Binary, length-prefixed, with series interning. The differentiator against
Grafana Live, which is JSON-only.

## Why binary at all

| encoding | bytes/point | measured |
| --- | --- | --- |
| JSON with full labels | 150–400 | 95 for a realistic label set |
| JSON with interned ids | ~40 | — |
| this format | ~9 | **9.07** |
| + Gorilla compression | 2–4 | **2.75** |

The single biggest win is **series interning**: send the label set once,
assign a `uint32`, reference the id thereafter. A label set like
`node_cpu_seconds_total{instance="node-42.eu-west-1.example.com",cpu="7",
mode="user",job="node-exporter"}` is 95 bytes. At 5 Hz that is 475 B/s per
series before any values have been sent.

Measured gain from interning alone: **9×**.

## Frames

```
Frame header (5 bytes)
  u8   type
  u32  payload length (big endian)
```

Length-prefixed rather than self-describing, because the decoder runs in a
Web Worker on every message. Anything requiring a parse to find the next
frame boundary costs more than the bytes it saves.

Several frames routinely coalesce into one WebSocket message, so a reader
must walk them rather than assuming one frame per message.

A frame claiming more than 16 MiB is rejected **on the claim**, before any
allocation. A 5-byte frame claiming 4 GB is the standard length-prefix
denial of service and costs one comparison to prevent.

### `0x01 SERIES_REGISTRY`

```
u32  count
[ u32 series_id, u16 label_bytes, <labels> ] × count
```

Labels use a canonical form: `name \x1f k=v \x1f k=v …`, with keys sorted.
Sorting is load-bearing — Go randomises map iteration order per run, so a
key built from unsorted iteration produces a *different* key for the same
label set on every process start, silently multiplying the series count.
That bug only appears under restart, so a single-process test never catches
it.

Ids are assigned densely from 1. **Zero is reserved** as "no series", which
makes a zero-valued struct detectably invalid rather than silently
referring to the first series.

The registry always precedes any point referencing a new id.

### `0x02 SNAPSHOT_HEADER`

```
u64  watermark_ts_ms
u32  series_count
```

The cut point between history and live data. See `docs/` on the snapshot
boundary, and §7 of the design document — the watermark is explicit because
inferring it from the last snapshot point is wrong whenever a series has no
recent data.

`series_count` is what was *found*, not what was requested, so a client can
tell "you have no data for 3 of my 10 series" from "your request was wrong".

### `0x03 DELTA_BATCH`

```
u64  base_ts_ms
u32  count
[ u32 series_id, uvarint ts_delta_ms, f32 value ] × count
```

The base is the **minimum** timestamp in the batch, not the first point's.
Batches arrive out of order from different agents, and a negative delta
encoded as a uvarint becomes an enormous positive one — timestamps
thousands of years in the future.

Values are `f32`. Percentages, rates and byte counts do not need 53 bits of
mantissa, and it halves the payload.

The uvarint keeps timestamps to one byte while the batch spans under
128 ms, which is what the agent's 50 ms flush deadline produces. A longer
batch window costs 2–3 bytes per timestamp instead.

### `0x04 HEARTBEAT`

```
u64  server_ts_ms
```

Sent every 15–20 s, well inside the 60 s idle timeout most load balancers
default to. The timestamp is not decoration: it lets the client estimate
clock offset and detect a stalled stream — no data frames *and* no
heartbeat means the connection is dead even though TCP has not noticed.

### `0x05 CONTROL`

JSON, deliberately, unlike everything else. Control frames are rare (a
subscribe, a pause, an error) and their schema changes far more often than
the data path's. Paying a few hundred bytes for a self-describing format on
a frame that arrives once a minute buys compatibility where it matters.

## Gorilla compression

From Pelkonen et al. (VLDB 2015). Two independent ideas:

**Timestamps: delta-of-delta.** Metrics arrive on a regular cadence, so the
second difference is almost always exactly zero, and zero costs one bit.

```
0       dod == 0             1 bit    ← the common case
10      7-bit zigzag dod     9 bits
110     9-bit               12 bits
1110    12-bit              16 bits
11110   32-bit              37 bits
11111   64-bit escape       69 bits
```

The ranges are `[-64,63]`, `[-256,255]`, `[-2048,2047]` — one less than the
paper's, because these are zigzag-encoded. Getting that wrong silently
truncates and corrupts the timestamp; see `notes-on-the-spec.md` §1.

**Values: XOR.** A metric that changes slowly XORs to very few significant
bits. The leading/trailing zero window is re-sent only when it widens.

Measured: **2.75 B/pt** on f32-precision drifting values, **2 bits/pt** on a
constant series.

## Cross-language testing

The Go encoder and the TypeScript decoder must agree byte for byte.
`cmd/genfixtures` emits real encoded bytes to `web/tests/wire-fixtures.json`
and the TypeScript tests decode those. Testing each side against its own
idea of the format would let both drift together.

Regenerate with:

```bash
go run ./cmd/genfixtures > web/tests/wire-fixtures.json
```
