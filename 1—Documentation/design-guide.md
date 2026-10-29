# Systems Monitoring Dashboard — Design & Build Guide

**Project:** Real-time infrastructure dashboard with sub-second latency, WebSocket streaming, time-series aggregation, and anomaly alerts
**Status of this document:** planning + reference

---

## Table of contents

1. [Executive summary and scope](#1-executive-summary-and-scope)
2. [Reality check](#2-reality-check)
3. [The latency budget](#3-the-latency-budget)
4. [System architecture](#4-system-architecture)
5. [Collection: push vs pull](#5-collection-push-vs-pull)
6. [Transport](#6-transport)
7. [The snapshot/stream boundary](#7-the-snapshotstream-boundary)
8. [Storage and aggregation](#8-storage-and-aggregation)
9. [The browser](#9-the-browser)
10. [Anomaly detection and alerting](#10-anomaly-detection-and-alerting)
11. [Operational concerns](#11-operational-concerns)
12. [Tech stack and setup](#12-tech-stack-and-setup)
13. [Repository layout](#13-repository-layout)
14. [Milestone ladder](#14-milestone-ladder)
15. [Reference implementations](#15-reference-implementations)
16. [Testing and benchmarking](#16-testing-and-benchmarking)
17. [Stretch goals](#17-stretch-goals)
18. [References](#18-references)

---

## 1. Executive summary and scope

### The original statement

> Real-time infra dashboard with sub-second latency, WebSocket streaming, time-series aggregation, and anomaly alerts.

Four findings reshape this before you write code:

1. **Sub-second latency is a collection-architecture problem, not a transport problem.** If you scrape metrics every 15 seconds, your median staleness is 7.5 seconds no matter how fast your WebSocket is. People build beautifully fast streaming pipelines and then wonder why the dashboard is twenty seconds behind. The collection interval dominates every other term in the budget, usually by two orders of magnitude. See section 3.
2. **Grafana Live already does WebSocket push — but with two specific gaps.** Grafana has had a WebSocket pub/sub streaming engine since v8.0. Its own documentation describes it as *soft* real-time, noting that message delivery delay "can be up to several hundred milliseconds or higher." And data sent over Grafana Live's WebSocket channels must be **JSON-encoded**. Those two facts are your differentiator, and they're the only honest one available. See section 2.1.
3. **Standard Prometheus does not support streaming.** Mimir and Thanos have experimental support. This means the entire mainstream metrics stack is pull-based and cannot get you sub-second regardless of what you put in front of it. See section 5.
4. **The browser is the bottleneck you'll hit before the server.** A thousand series at 1 Hz is a thousand messages per second to parse, buffer, and render on a single main thread with a 16.7 ms frame budget. Storing those points as JavaScript objects instead of typed arrays is a 5–7× memory difference and will kill the tab. See section 9.

### Revised project statement

> A push-first operational tail view: binary-framed WebSocket streaming of high-frequency metrics with sub-second P99 end-to-end latency, backed by a tiered time-series store for history, with robust-statistics anomaly detection and hysteresis-based alerting designed to survive the multiple-comparisons problem.

### What this is for

Be specific about the use case, because it justifies everything else. This is **the window you open during the ten minutes that matter**: a canary shifting traffic, a load test ramping, an incident in progress, a deploy rolling out, a database failing over.

Grafana at a 5-second refresh is fine for a dashboard you glance at. It is useless for watching a traffic shift happen. That narrow gap is real and worth building into.

### Explicit non-goals

- **Not a time-series database.** Writing a TSDB is a multi-year project. Use ClickHouse, VictoriaMetrics, or Prometheus. All of the interesting engineering here is in the streaming layer.
- **Not a Grafana replacement.** No dashboards-as-code, no hundred datasources, no plugin ecosystem, no long-term storage. Complement it; link out to it for history.
- **Not a logs or traces platform.** Metrics only. Adding logs triples the scope.
- **Not an incident-management system.** You emit alerts; PagerDuty and Alertmanager route them.
- **Not a lossless live stream.** The live path is best-effort and lossy by design (see section 6.4). The TSDB is the source of truth. This is a deliberate architectural choice, not a limitation to apologize for.

---

## 2. Reality check

### 2.1 What already exists

| Tool | What it does | Overlap |
|---|---|---|
| **Grafana + Grafana Live** | WebSocket pub/sub streaming, multiplexed over one connection, channel-based. Explicitly "soft real-time" — hundreds of ms or higher. **JSON-encoded payloads only.** | Very high, but see the gaps |
| **Prometheus** | Pull-based scraping, PromQL, recording rules | The collection model that makes sub-second impossible |
| **VictoriaMetrics** | Prometheus-compatible TSDB with better ingest and cardinality handling | Your storage layer — use it, don't rebuild it |
| **Perses** | CNCF Sandbox dashboarding, dashboards-as-code with Kubernetes CRDs | Visualization layer, GitOps-focused |
| **SigNoz / Uptrace** | OpenTelemetry-native unified observability | Broader scope, not real-time-focused |
| **Datadog / New Relic** | Commercial, everything | Everything |
| **ClickHouse + custom UI** | What a lot of teams actually build | The architecture you're proposing |

**The honest position.** Grafana Live covers the WebSocket push story, so "I built WebSocket streaming for metrics" is not a differentiator on its own. What Grafana Live does *not* do:

- **Binary framing.** JSON-only means you pay full label sets and floating-point-as-text on every message. A binary format with series interning and delta encoding is plausibly 10–50× smaller on the wire, which directly buys you higher series counts and lower decode cost.
- **Sub-second as a design target.** Grafana explicitly targets soft real-time at hundreds of milliseconds. An instrumented, measured end-to-end budget with a P99 SLO is a different engineering posture.
- **A collection path built for it.** Grafana Live can receive pushed data, but the ecosystem feeding it — Prometheus, most exporters — is pull-based at 15–60 s intervals.

**Lead with the measured latency distribution and the wire efficiency.** Both are numbers you can publish, and both are things nobody else is optimizing for.

### 2.2 The failure modes that separate a demo from a system

| Scenario | Naive behavior | What's required |
|---|---|---|
| Client can't keep up with the stream | Server's send buffer grows unboundedly → OOM | Bounded per-connection queue + conflation (§6.4) |
| Page loads mid-stream | Gap or duplicate data at the history/live boundary | Subscribe-then-snapshot with an explicit watermark (§7) |
| WebSocket drops | Silent stall; the chart just stops updating and nobody notices | Heartbeat + reconnect with backfill (§6.5) |
| Load balancer idle timeout | Connection killed after 60 s of quiet | Application-level ping well inside the LB timeout |
| 1000 series × 3σ threshold | ~233,000 false alerts per day from pure chance | Hysteresis (`for` duration) + robust statistics (§10.2) |
| One outlier in the baseline window | Inflates σ, raises the threshold, masks the anomaly you wanted | MAD instead of standard deviation (§10.3) |
| Averaging into 1-minute buckets | A 1-second 100% CPU spike becomes invisible | Store and render min/max per bucket, not just avg (§8.3) |
| A label with unbounded values | Series count explodes; TSDB dies | Cardinality limits enforced at ingest (§11.2) |
| Dashboard shares infra with what it monitors | It's down exactly when you need it | Meta-monitoring on separate infrastructure (§11.1) |
| Agent with a skewed clock | Points land in the wrong bucket, or poison the x-axis range | Watermarks, bounded future-timestamp rejection (§11.3) |

---

## 3. The latency budget ★

This is the centerpiece of the project. Decompose it before you optimize anything, because the dominant term is almost never where people look.

### 3.1 The full path

| # | Stage | Contribution |
|---|---|---|
| 1 | Event occurs → collector observes it | **0 to one collection interval** |
| 2 | Agent batches and flushes | 0 to flush interval |
| 3 | Network: agent → ingest | 1–50 ms |
| 4 | Ingest parse, validate, route | 1–10 ms |
| 5 | Pub/sub fanout to subscribers | 1–10 ms |
| 6 | Server send queue wait | 0 to queue drain time |
| 7 | Network: server → browser | 5–100 ms (½ RTT + queuing) |
| 8 | Client decode | 1–50 ms |
| 9 | Wait for next animation frame | 0–16.7 ms |
| 10 | Monitor refresh | 0–16.7 ms |

### 3.2 Three scenarios

| Collection model | Stage 1 P50 | Everything else | **End-to-end P50** | **P99** |
|---|---|---|---|---|
| Prometheus scrape @ 15 s | 7,500 ms | ~100 ms | **~7.6 s** | ~15 s |
| Push @ 1 Hz | 500 ms | ~100 ms | **~600 ms** | **~1.1 s** ❌ |
| Push @ 5 Hz (200 ms) | 100 ms | ~100 ms | **~200 ms** | **~400 ms** ✅ |

**The conclusion is quantitative and unambiguous: sub-second P99 requires collection at roughly 5 Hz or faster.** At 1 Hz you make P50 comfortably but miss P99, because the collection interval alone can contribute a full second in the tail.

This single table should go in your README. It's the argument for the entire architecture, and it's the kind of analysis that distinguishes a considered project from a fast demo.

### 3.3 The cost of 5 Hz

Nothing is free. Going from 15 s scrapes to 5 Hz push is a **75× increase in data volume**:

| | 15 s scrape | 5 Hz push | Ratio |
|---|---|---|---|
| Points/series/day | 5,760 | 432,000 | 75× |
| 1,000 series, points/day | 5.8 M | 432 M | 75× |
| At 16 B/point, raw/day | 92 MB | 6.9 GB | 75× |
| With Gorilla compression (~1.4 B/pt) | 8 MB | 605 MB | 75× |

This is why **the high-frequency path must be scoped**. Two rules:

1. **Only stream what someone is watching.** Subscriptions are demand-driven — the collector runs at high frequency only for series with an active subscriber. Nobody watching means fall back to the 15 s path.
2. **High-frequency data has a short retention.** Keep 5 Hz for 15 minutes in a ring buffer, roll up to 1 s for an hour, to 15 s beyond that. You need the resolution for the live view; you don't need it in September.

### 3.4 Small terms worth fixing

- **Nagle's algorithm** can add up to 40 ms waiting to coalesce small writes. Set `TCP_NODELAY` on every streaming socket. This is a one-line fix for a term that's 20% of your budget.
- **The render frame.** Even a perfectly delivered message lands at the next paint. You cannot beat one frame (16.7 ms at 60 Hz), and you shouldn't try — batch into the frame instead.
- **GC pauses.** Both Go's collector and the browser's will occasionally add tens of milliseconds. Reducing allocation in the hot path (reused buffers, typed arrays, no per-message objects) is the fix. This is also why Grafana's docs mention GC in their soft-real-time caveat.

### 3.5 Measure it, don't estimate it

**Stamp the origin timestamp at the collector and compare it at render time.** Report the full distribution.

```js
// In the render path — the only latency measurement that means anything.
const e2eMs = performance.timeOrigin + performance.now() - point.originTsMs;
latencyHistogram.observe(e2eMs);
```

Two caveats that matter:

- **This measures across two clocks.** The collector's clock and the browser's differ. For a single-machine dev setup it's fine; across machines you need NTP discipline and should treat the absolute number as approximate while the *distribution shape* stays meaningful.
- **Build this in milestone M2, before any optimization.** Optimizing without a measured budget means you'll spend a week shaving 5 ms off the decoder while the collection interval contributes 7 seconds.

---

## 4. System architecture

```
┌─────────────┐  ┌─────────────┐  ┌─────────────┐
│   Agent     │  │   Agent     │  │   Agent     │   push, 5 Hz when
│  (node 1)   │  │  (node 2)   │  │  (node N)   │   subscribed
└──────┬──────┘  └──────┬──────┘  └──────┬──────┘
       │                │                │
       └────────────────┼────────────────┘
                        │  OTLP / custom binary over gRPC
              ┌─────────▼──────────┐
              │  Ingest            │  parse · validate · cardinality
              │                    │  limit · stamp origin_ts
              └─────────┬──────────┘
                        │
         ┌──────────────┼──────────────────┐
         │              │                  │
   ┌─────▼─────┐  ┌─────▼──────┐   ┌──────▼──────────┐
   │ Hot ring  │  │  Pub/Sub   │   │  TSDB writer    │
   │ buffer    │  │ (NATS)     │   │  (batched)      │
   │ 15 min    │  └─────┬──────┘   └──────┬──────────┘
   │ in memory │        │                 │
   └─────┬─────┘        │           ┌─────▼──────────┐
         │              │           │  ClickHouse    │
         │              │           │  raw + rollups │
         │              │           └─────┬──────────┘
         │              │                 │
         │        ┌─────▼─────────────────▼──────┐
         └───────▶│  Stream gateway               │
                  │  · per-conn bounded queue     │
                  │  · conflation by series key   │
                  │  · binary framing             │
                  │  · snapshot/stream handshake  │
                  └──────────────┬────────────────┘
                                 │ WebSocket (binary)
                  ┌──────────────▼────────────────┐
                  │  Browser                      │
                  │  ┌─────────────────────────┐  │
                  │  │ Web Worker: decode      │  │
                  │  │ → ring buffers          │  │
                  │  │   (typed arrays)        │  │
                  │  └───────────┬─────────────┘  │
                  │  ┌───────────▼─────────────┐  │
                  │  │ Main: rAF → uPlot canvas│  │
                  │  └─────────────────────────┘  │
                  └───────────────────────────────┘

   ┌────────────────────────────────────────────┐
   │  Detector: reads hot ring + rollups        │
   │  → robust baselines → candidate anomalies  │
   │  → hysteresis → dedup/group → notify       │
   └────────────────────────────────────────────┘
```

### Design decisions

**Two data paths, deliberately.** The live path (hot ring → pub/sub → WebSocket) is optimized for latency and is allowed to drop data. The durable path (TSDB writer → ClickHouse) is optimized for completeness and is allowed to be slow. Conflating them means either a slow live view or a lossy history.

**Demand-driven collection.** Agents run at high frequency only for series someone is subscribed to. The gateway publishes the active subscription set back to the ingest layer, which propagates it to agents. Without this, 5 Hz across your whole fleet is unaffordable.

**The hot ring buffer is the live source of truth.** The last 15 minutes lives in memory on the gateway, indexed by series. New subscribers get their snapshot from here, not from the database, which makes subscription setup fast and takes query load off the TSDB.

**The detector reads from the hot ring, not the stream.** Alert evaluation must not depend on what any particular browser is subscribed to.

---

## 5. Collection: push vs pull

### 5.1 Why pull can't get you there

Prometheus's pull model has real virtues — service discovery, easy failure detection (a failed scrape *is* a signal), no agent configuration to push credentials around. It is also structurally incapable of sub-second delivery: the scrape interval is a hard floor on staleness, and standard Prometheus has no streaming output at all. (Mimir and Thanos have experimental streaming support, but "experimental" is doing work in that sentence.)

### 5.2 The hybrid

Don't throw out pull. Run both:

| Path | Mechanism | Interval | Retention | Used for |
|---|---|---|---|---|
| **Baseline** | Prometheus-style scrape, or OTLP push | 15 s | Long | Dashboards, capacity, SLOs, the bulk of everything |
| **Live** | Demand-driven push | 200 ms | 15 min hot | The tail view during an incident |

A series enters the live path when someone subscribes, and leaves when the last subscriber disconnects. The agent-side implementation is a subscription set it polls for and a faster timer for the matching collectors.

This keeps the steady-state cost roughly what it is today and pays the 75× only for the handful of series someone is actively staring at.

### 5.3 Agent design

- **Push over gRPC**, not HTTP/1.1 per-sample. One long-lived stream per agent.
- **Batch with a deadline**: flush when the batch reaches N points *or* T milliseconds have passed, whichever first. T is a direct line item in your latency budget — 50 ms is a reasonable starting point.
- **Stamp `origin_ts` at observation**, not at flush. This is what makes end-to-end measurement possible.
- **Bound the agent's own buffer.** If ingest is unreachable, the agent must drop and count, not grow until the node OOMs. An observability agent that takes down the host it monitors is a genuinely bad outcome and has happened to real products.
- **Never block the application.** If the agent is in-process, metric recording must be a lock-free write to a ring buffer, with a separate goroutine doing the I/O.

---

## 6. Transport

### 6.1 WebSocket or SSE?

| | **WebSocket** | **Server-Sent Events** |
|---|---|---|
| Direction | Bidirectional | Server → client only |
| Binary | Native | Text only (base64 costs ~33%) |
| Reconnect | You build it | Automatic, with `Last-Event-ID` |
| Proxy friendliness | Occasionally mangled by proxies | Plain HTTP; works everywhere |
| HTTP/2 multiplexing | No (separate connection) | Yes |
| Browser connection limit | ~255 per browser | 6 per domain on HTTP/1.1, ~100 on HTTP/2 |

**Choose WebSocket, because binary framing is the differentiator.** Base64-ing a compact binary format over SSE throws away a third of what you gained. But borrow SSE's good ideas: an event ID on every frame and a `Last-Event-ID`-style resume on reconnect (§6.5).

Multiplex all subscriptions over **one** connection per tab. Grafana Live does this, and it's correct — many connections means many TCP slow-starts, many heartbeats, and head-of-line problems at the browser's connection limit.

### 6.2 Binary framing

The single biggest win over JSON is **series interning**: send the label set once, assign a `uint32` id, and reference the id thereafter.

```
Frame header (5 bytes)
  u8   type
  u32  payload length

type 0x01  SERIES_REGISTRY
  u32  count
  [ u32 series_id, u16 label_bytes, <labels as compact kv> ] × count

type 0x02  SNAPSHOT_HEADER
  u64  watermark_ts_ms      ← the cut point; see §7
  u32  series_count

type 0x03  DELTA_BATCH
  u64  base_ts_ms
  u32  count
  [ u32 series_id, varint ts_delta_ms, f32 value ] × count

type 0x04  HEARTBEAT
  u64  server_ts_ms

type 0x05  CONTROL           ← conflation notice, backpressure signal, errors
```

Rough per-point cost:

| Encoding | Bytes/point |
|---|---|
| JSON with full labels | 150–400 |
| JSON with interned ids | ~40 |
| This binary format | **~9** |
| + delta-of-delta timestamps and XOR floats (Gorilla) | **~2–4** |

**Gorilla-style compression** (delta-of-delta on timestamps, XOR on float64s, from Facebook's TSDB paper) is worth implementing. Regularly-spaced timestamps compress to a bit or two each, and metric values that change slowly XOR to very few significant bits. It's a self-contained, satisfying piece of engineering and it's the kind of thing that makes a project memorable.

Use `f32` for values unless you know you need more. Most infrastructure metrics — percentages, rates, byte counts — don't need 53 bits of mantissa, and it halves your payload.

### 6.3 Backpressure ★

**WebSocket has no application-level flow control.** If you write faster than the client reads, the kernel send buffer fills, then your application-level buffer grows, and then the server runs out of memory. One slow client on hotel WiFi can take down the gateway for everyone.

The fix is a bounded queue per connection plus an explicit policy for what happens when it's full.

### 6.4 Conflation is the right policy

For a dashboard you care about **the latest value per series**, not every intermediate one. So when the queue is full and a new point arrives for a series that already has a pending point, **replace it** rather than queueing both.

```go
type ConflatingQueue struct {
    mu      sync.Mutex
    pending map[uint32]Point   // series_id → latest pending point
    order   []uint32           // insertion order, for fairness
    maxLen  int
    dropped atomic.Uint64
    conflated atomic.Uint64
}

func (q *ConflatingQueue) Push(p Point) {
    q.mu.Lock()
    defer q.mu.Unlock()

    if _, exists := q.pending[p.SeriesID]; exists {
        q.pending[p.SeriesID] = p      // replace — the old value is stale anyway
        q.conflated.Add(1)
        return
    }
    if len(q.pending) >= q.maxLen {
        q.dropped.Add(1)               // genuinely full of distinct series
        return
    }
    q.pending[p.SeriesID] = p
    q.order = append(q.order, p.SeriesID)
}
```

This bounds memory at `maxLen × sizeof(Point)` per connection regardless of client speed, and it degrades gracefully: a slow client sees a lower-resolution stream rather than a stalled one.

**Say this out loud in the docs: the live stream is lossy by design.** A client that conflated 40% of its updates saw a correct-but-decimated view. If you need every point, query the TSDB. Pretending the stream is lossless is how you end up with users drawing conclusions from data that silently dropped.

**Instrument it.** Expose per-connection queue depth, conflation rate, and drop count. A high conflation rate is the signal that a client is under-provisioned or the subscription is too large — and it's the first thing you'll want during a "why is my chart choppy" investigation.

**Disconnect the truly stuck.** If a connection's queue stays full for, say, 30 seconds, close it with a clear reason. A client that isn't reading isn't watching.

### 6.5 Heartbeats and reconnection

**Heartbeat well inside the shortest proxy timeout in the path.** Many load balancers default to 60 s idle; send a heartbeat every 15–20 s. The heartbeat also carries the server timestamp, which lets the client estimate clock offset and detect a stalled stream (no frames *and* no heartbeat means the connection is dead even though TCP hasn't noticed).

**Reconnect with exponential backoff and full jitter.** Without jitter, a gateway restart causes every client to reconnect simultaneously and knock it over again.

```ts
function backoffMs(attempt: number): number {
  const cap = 30_000;
  const base = Math.min(cap, 250 * 2 ** attempt);
  return Math.random() * base;     // full jitter
}
```

**Backfill the gap on reconnect.** The client knows the timestamp of the last point it received. On reconnect it re-subscribes, buffers, requests the gap `[last_seen, now]`, and merges — the same procedure as the initial load, which is section 7.

---

## 7. The snapshot/stream boundary ★

A chart is part history (from storage) and part live (from the stream). Joining them without gaps or duplicates is the subtlest correctness problem in the project, and it's one that market-data systems solved decades ago.

### 7.1 Why the obvious orderings fail

**Query first, then subscribe:**
```
  ├── query returns data up to T ──┤
                                      ├── subscription starts at T' ──▶
                                   └─ GAP ─┘
```
Everything between `T` and `T'` is lost. The chart has a hole, and nobody notices because it looks like a period with no data.

**Subscribe first, then query:**
```
  ├── subscription starts at T' ──────────────▶
              ├── query returns data up to T ──┤
              └────── DUPLICATES ──────────────┘
```
Points appear twice. Rate calculations double-count.

### 7.2 The correct procedure

Subscribe first, buffer, then snapshot with an **explicit watermark**, then reconcile.

```
Client                                    Server
  │                                          │
  ├─ SUBSCRIBE {series, resolution} ────────▶│
  │                                          ├─ register subscriber
  │◀─ SERIES_REGISTRY ───────────────────────┤
  │                                          │
  │◀─ DELTA_BATCH (live) ────────────────────┤   client BUFFERS these,
  │◀─ DELTA_BATCH (live) ────────────────────┤   does not render yet
  │                                          │
  ├─ SNAPSHOT {from, to} ───────────────────▶│
  │                                          ├─ read hot ring / TSDB
  │◀─ SNAPSHOT_HEADER {watermark_ts} ────────┤   ← the cut point
  │◀─ SNAPSHOT data ─────────────────────────┤
  │                                          │
  ├─ render snapshot                         │
  ├─ drain buffer, DISCARD ts ≤ watermark    │
  ├─ switch to live rendering                │
```

The server must return the watermark explicitly. Without it the client is guessing where history ends, and "use the timestamp of the last snapshot point" is wrong whenever a series has no recent data.

```go
type SnapshotHeader struct {
    // Every point in this snapshot has ts <= WatermarkTsMs.
    // The client discards any buffered live point at or below it.
    WatermarkTsMs int64
    SeriesCount   uint32
}

func (h *Hub) Snapshot(ctx context.Context, sub *Subscription, from, to int64) (*Snapshot, error) {
    // Take the watermark BEFORE reading, so anything written during the
    // read is strictly above it and will also arrive on the live stream.
    watermark := h.ring.CommittedWatermark()
    if to > watermark {
        to = watermark
    }
    data, err := h.ring.Range(sub.SeriesIDs, from, to)
    if err != nil {
        return nil, err
    }
    return &Snapshot{Header: SnapshotHeader{WatermarkTsMs: watermark}, Data: data}, nil
}
```

Taking the watermark **before** the read is the part that makes this correct. A point written during the read is above the watermark, so it is not in the snapshot and *will* be delivered live. Take it after, and a point written mid-read might be in neither.

### 7.3 The same procedure everywhere

This handles four cases with one mechanism:

- **Initial page load** — as above
- **Reconnect** — `from` is the last-seen timestamp
- **Adding a series to an existing chart** — snapshot just that series
- **Widening the time range** — snapshot the newly visible portion

Implement it once, in one place, and route all four through it. Four hand-written variants means four different off-by-one bugs.

---

## 8. Storage and aggregation

### 8.1 Don't build a TSDB

Writing a production time-series database is years of work — compression, compaction, indexing, out-of-order handling, retention. The interesting engineering in this project is the streaming layer. Spend it there.

| Option | Good for |
|---|---|
| **ClickHouse** | Flexible SQL aggregation, excellent compression, materialized views for rollups. **Recommended** if you want arbitrary aggregation. |
| **VictoriaMetrics** | Prometheus-compatible, great ingest rates, handles cardinality better than Prometheus. Best if you want PromQL and to drop into an existing stack. |
| Prometheus | Fine for the 15 s baseline path; not for high-frequency ingest |
| TimescaleDB | If you're already committed to Postgres |

ClickHouse's materialized views are a particularly good fit — you define the rollup once as a view and it maintains itself on insert, rather than running a separate rollup job.

### 8.2 Tiering

| Tier | Resolution | Retention | Where |
|---|---|---|---|
| 0 — hot | Native (200 ms) | 15 min | In-memory ring on the gateway |
| 1 — raw | 1 s | 24 h | TSDB |
| 2 — rollup | 1 min | 30 d | TSDB, materialized view |
| 3 — rollup | 1 h | 1 y | TSDB, materialized view |

The query router picks a tier from the requested time range and the target resolution:

```go
func selectTier(from, to time.Time, pixelWidth int) Tier {
    span := to.Sub(from)
    // Never return more points than the chart can physically draw.
    targetStep := span / time.Duration(pixelWidth)
    switch {
    case time.Since(from) < 15*time.Minute && targetStep < time.Second:
        return TierHot
    case targetStep < time.Minute:
        return TierRaw
    case targetStep < time.Hour:
        return TierMinute
    default:
        return TierHour
    }
}
```

### 8.3 Rollups must keep min and max ★

**Averaging hides spikes, and hiding spikes is a correctness failure in a monitoring tool.**

A one-second burst to 100% CPU, averaged into a one-minute bucket alongside 59 seconds at 20%, reads as 21.3%. The incident is invisible. Someone looking at the dashboard concludes nothing happened.

So every rollup stores five values per bucket, not one:

```sql
CREATE MATERIALIZED VIEW metrics_1m
ENGINE = AggregatingMergeTree()
ORDER BY (series_id, bucket)
AS SELECT
    series_id,
    toStartOfMinute(ts) AS bucket,
    minState(value)   AS min_v,
    maxState(value)   AS max_v,
    avgState(value)   AS avg_v,
    sumState(value)   AS sum_v,
    countState()      AS n
FROM metrics_raw
GROUP BY series_id, bucket;
```

And the chart renders a **band** — min-to-max shaded, average as a line. At a glance you see both the central tendency and the extremes, and a one-second spike shows up as a tall thin band rather than vanishing.

This also makes anomaly detection over rolled-up data meaningful, which it otherwise isn't.

### 8.4 Downsampling to the viewport

**A 1000-pixel-wide chart cannot display more than 1000 x-positions.** Sending 100,000 points to draw 1,000 columns wastes bandwidth, decode time, and render time for zero visible benefit.

Downsample server-side, and choose the algorithm deliberately:

| Algorithm | Behavior | Use for |
|---|---|---|
| Naive averaging | **Hides spikes.** Never use alone for monitoring. | — |
| Min/max per bucket | Preserves extremes; renders as a band | **Default for infrastructure metrics** |
| LTTB (Largest Triangle Three Buckets) | Preserves visual shape with one point per bucket | When you need a single clean line |
| Last value per bucket | Correct for gauges and state-like series | Status, enum-valued metrics |

Min/max is the right default here because the whole point of the tool is noticing that something spiked.

---

## 9. The browser

You will hit browser limits before server limits. Plan for them.

### 9.1 Memory: typed arrays, not objects

One hour of 1 Hz data for 1000 series is 3.6 M points.

| Representation | Bytes/point | Total |
|---|---|---|
| `{t: number, v: number}` objects in an array | ~56 (object header + fields + pointer) | **~200 MB** |
| Two `Float64Array`s | 16 | 57.6 MB |
| `Float32Array` values + `Uint32Array` ms-offsets from a base | **8** | **28.8 MB** |

The object representation is a 7× penalty and will make the tab sluggish and eventually kill it. It also creates 3.6 million objects for the GC to trace, which is its own latency problem.

**Use a per-series ring buffer over typed arrays:**

```ts
class SeriesRing {
  private ts: Uint32Array;      // ms offset from baseTs
  private vals: Float32Array;
  private head = 0;
  private len = 0;

  constructor(readonly capacity: number, private baseTs: number) {
    this.ts = new Uint32Array(capacity);
    this.vals = new Float32Array(capacity);
  }

  push(tsMs: number, v: number): void {
    this.ts[this.head] = tsMs - this.baseTs;
    this.vals[this.head] = v;
    this.head = (this.head + 1) % this.capacity;
    if (this.len < this.capacity) this.len++;
  }
}
```

`Uint32Array` offsets from a base timestamp give you ~49 days of millisecond range, which is plenty for a live view. Re-base when you approach the limit.

### 9.2 Decode off the main thread

Put the WebSocket and the decoder in a **Web Worker**. The main thread's only job is rendering.

Transfer decoded data to the main thread as `ArrayBuffer`s using the transferable-objects path so there's no copy. If you want true zero-copy shared access, `SharedArrayBuffer` works — but it requires cross-origin isolation (`COOP: same-origin`, `COEP: require-corp`), which has real deployment consequences: every cross-origin subresource needs `CORP` headers or it's blocked. Weigh that before committing; transferables are usually enough.

### 9.3 Decouple arrival from render

**Never render on message arrival.** At 1000 messages per second you'd attempt 1000 renders per second into a 60 Hz display.

```ts
// Worker → main: accumulate only.
onmessage = (e) => {
  for (const p of decode(e.data)) rings.get(p.seriesId)!.push(p.ts, p.v);
  dirty = true;
};

// Main: render at most once per frame.
function frame() {
  if (dirty) {
    chart.setData(buildSeries(rings));   // reads directly from typed arrays
    dirty = false;
  }
  requestAnimationFrame(frame);
}
requestAnimationFrame(frame);
```

If 500 messages arrive within one frame, that's one render, not 500. This single change is usually the difference between a dashboard that stutters and one that's smooth.

### 9.4 Charting library

| Library | Verdict |
|---|---|
| **uPlot** | **Recommended.** Canvas-based, purpose-built for large time series, handles hundreds of thousands of points, tiny bundle. |
| Chart.js | Fine for static charts, too slow for streaming |
| D3 + SVG | Dies at a few thousand points — one DOM node per element |
| Plotly | Feature-rich, heavy, not built for this |
| Custom WebGL (regl / three) | Only if you exceed uPlot, which you probably won't |

**Do not render the chart through React.** React's reconciliation on every frame is exactly the overhead you're trying to avoid. Use React for layout, controls, and chrome; hand the canvas to uPlot and update it imperatively from the rAF loop.

### 9.5 Backgrounded tabs

Browsers throttle `requestAnimationFrame` and timers in hidden tabs — often to once per second or less. Detect it and act:

```ts
document.addEventListener('visibilitychange', () => {
  if (document.hidden) {
    ws.send(encodeControl({ type: 'PAUSE' }));   // stop the firehose
  } else {
    ws.send(encodeControl({ type: 'RESUME', lastSeenTs }));  // → §7 backfill
  }
});
```

Without this, a backgrounded tab accumulates a queue it can't drain, triggers your conflation and drop logic, and looks like a broken client. Pausing server-side is better for everyone.

---

## 10. Anomaly detection and alerting

### 10.1 The hard part is not the algorithm

Detecting that a number moved is easy. Not drowning people in alerts is the entire engineering problem, and the failure mode is quantifiable.

### 10.2 The multiple-comparisons problem ★

A 3σ two-tailed threshold flags about 0.27% of samples **from a perfectly normal, perfectly healthy series**. That's the definition of 3σ, not a bug.

Now scale it:

| | Value |
|---|---|
| Series monitored | 1,000 |
| P(flag \| healthy, 3σ) | 0.0027 |
| Expected false flags per evaluation | **2.7** |
| Evaluations per day at 1 Hz | 86,400 |
| **Expected false alerts per day** | **~233,000** |

A quarter of a million false alerts per day, from a system where nothing is wrong. Anyone who's operated a naive anomaly detector recognizes this immediately.

**Hysteresis is the fix with the best ratio of value to effort.** Require the condition to hold for *k* consecutive evaluations — Prometheus's `for:` clause:

| Consecutive required | Approx. false alerts/day |
|---|---|
| 1 | ~233,000 |
| 2 | ~630 |
| 3 | **~1.7** |

A caveat worth stating honestly: this arithmetic assumes independent samples. Real metrics are autocorrelated — if CPU is high now it's probably high a second from now — so consecutive breaches are *not* independent and the true false-alert rate will be higher than this bound. The direction of the fix is still right, and it's still dramatic. Tune `k` empirically against a replay of real data (§16.3) rather than trusting the table.

### 10.3 Use robust statistics ★

Standard deviation has a **breakdown point of zero**: a single extreme outlier can inflate it arbitrarily. Which means the spike you want to detect raises your threshold and masks itself. This is precisely backwards.

**Median absolute deviation** has a 50% breakdown point — half your data can be garbage and the estimate holds.

```go
// RobustBaseline estimates center and spread in a way a spike can't corrupt.
func RobustBaseline(window []float64) (center, spread float64) {
    s := slices.Clone(window)
    slices.Sort(s)
    center = median(s)

    dev := make([]float64, len(s))
    for i, v := range s {
        dev[i] = math.Abs(v - center)
    }
    slices.Sort(dev)
    // 1.4826 rescales MAD to be a consistent estimator of σ for normal data.
    spread = 1.4826 * median(dev)
    return center, spread
}

func RobustZ(v, center, spread float64) float64 {
    if spread < 1e-9 {
        return 0   // a perfectly flat series has no anomalies by this measure
    }
    return (v - center) / spread
}
```

That `spread < 1e-9` guard matters more than it looks. A series that's been constant (a gauge sitting at zero, a counter that hasn't moved) has zero MAD, and without the guard every subsequent sample divides by ~0 and becomes infinitely anomalous. This is a real and common cause of alert storms in homegrown detectors.

### 10.4 Seasonality

CPU at 3 a.m. is not CPU at 2 p.m. A detector with a rolling one-hour baseline will alert every morning when traffic ramps.

| Approach | Cost | When |
|---|---|---|
| Rolling window | Cheap | Metrics with no daily pattern |
| **Same-time-last-week comparison** | Cheap | **Best value for the effort.** Compare against the same wall-clock slot on the previous *k* weeks and take the median. |
| STL decomposition | Moderate | Strong multi-scale seasonality |
| Holt-Winters | Moderate | Forecasting as well as detection |

Same-time-last-week handles both daily and weekly seasonality with a lookup and a median, and it degrades sensibly (falls back to the rolling window) when you don't have enough history yet. Start there.

### 10.5 Alert lifecycle

Detection is one stage of five. Skipping the last three is why homegrown alerting is unbearable.

```
 detect          robust z-score exceeds threshold
   ↓
 hysteresis      hold for k consecutive evaluations (§10.2)
   ↓
 dedup           same series + same condition → one alert, not one per evaluation
   ↓
 group           many series, same root cause → one notification
   ↓
 inhibit         suppress children of a firing parent
   ↓
 notify          Slack / PagerDuty / webhook
```

**Grouping** is what turns a rack failure from 200 pages into one. Group by a configurable label set (cluster, service, alertname) with a short wait window so related alerts arrive together.

**Inhibition** is what stops "the whole datacenter is unreachable" from also paging you about every service in it. If a parent alert is firing, suppress its children.

Both of these are Alertmanager's model and it's the right one — study it before designing your own.

**Every alert needs a resolution path.** An alert that fires and never resolves trains people to ignore alerts. Emit an explicit resolved notification when the condition clears, and require the same hysteresis on the way down so it doesn't flap.

---

## 11. Operational concerns

### 11.1 Meta-monitoring

**If your monitoring shares infrastructure with what it monitors, it fails exactly when you need it.** The cluster goes down, and so does the dashboard showing you why.

- Run the dashboard stack on separate infrastructure from the monitored fleet.
- **Dead-man's switch**: an alert rule that fires *constantly*, wired to an external service that pages when it *stops* arriving. This is the only way to detect that the alerting pipeline itself is dead — a silent alerting system looks exactly like a healthy fleet. Alertmanager calls this the Watchdog.
- Monitor the monitor: ingest rate, connection count, queue depths, conflation rate, detector evaluation lag.

### 11.2 Cardinality

**Series count is the product of label cardinalities.** One label carrying a user ID, request ID, or raw URL path makes it unbounded, and an unbounded series count kills every TSDB there is. This is the single most common way Prometheus deployments fall over.

Enforce it at ingest, where you can still refuse:

```go
func (i *Ingest) checkCardinality(m Metric) error {
    n := i.seriesCount.Load(m.Name)
    if n > i.limits.PerMetric {
        i.rejected.WithLabelValues(m.Name, "metric_cardinality").Inc()
        return ErrCardinalityExceeded
    }
    for k, v := range m.Labels {
        if i.labelValues.Count(m.Name, k) > i.limits.PerLabel {
            i.rejected.WithLabelValues(m.Name, "label_cardinality:"+k).Inc()
            return ErrCardinalityExceeded
        }
        if len(v) > i.limits.MaxLabelValueLen {
            return ErrLabelTooLong
        }
    }
    return nil
}
```

Reject loudly with a metric naming the offending label, so the person who added it can find out within minutes rather than after an outage. A silent drop is almost as bad as accepting it.

### 11.3 Clock skew and late data

Agents have imperfect clocks. Points arrive late. Decisions to make explicitly:

**Event time or ingest time?** Event time is correct for analysis; ingest time is what you can actually guarantee is monotonic. Store both. Use event time for charts and aggregation, ingest time for the latency measurement in §3.5.

**Bound how far in the future you'll accept.** An agent with a clock a day fast will otherwise blow out your chart's x-axis range and make everything else look like it's in the distant past. Reject anything more than a few seconds ahead of server time and count it.

**Bound how late you'll accept.** Pick a watermark — say 30 seconds. Data older than that is dropped from the live path and counted, though it may still be written to the TSDB for correctness. Alert evaluation must use a lag at least as large as the watermark, or you'll evaluate windows that aren't complete yet and alert on partial data.

### 11.4 Multi-node

One gateway handles a lot of connections, but you'll want more than one for availability.

- **Pub/sub backplane** (NATS) so any gateway can serve any subscription. Avoid sticky sessions — they make deploys painful and don't survive node loss.
- **The hot ring must be replicated or reconstructible.** Simplest correct approach: every gateway subscribes to everything and maintains its own ring. Costs memory, removes all coordination.
- **Drain on deploy**: send a control frame telling clients to reconnect, with jitter, before shutting down. Clients follow §7 to backfill. A rolling deploy that drops every connection simultaneously causes a reconnect thundering herd.

---

## 12. Tech stack and setup

### 12.1 Choices

| Layer | Choice | Why |
|---|---|---|
| **Gateway / ingest** | **Go** | Goroutine-per-connection scales to tens of thousands; good WebSocket libraries (`coder/websocket`, `gobwas/ws`); easy static deployment. Rust is also excellent here if you prefer it. |
| **Pub/sub** | **NATS** | Subject-based, built for fanout, trivial to run, microsecond-scale. Redis Streams works too. |
| **TSDB** | **ClickHouse** | Materialized views make rollups self-maintaining; excellent compression. VictoriaMetrics if you want PromQL. |
| **Frontend** | **TypeScript + uPlot** | Canvas rendering built for large time series |
| **Framework** | React for chrome only | Never for the chart itself (§9.4) |
| **Collection** | OpenTelemetry Collector, or a custom Go agent | OTLP for the 15 s path; custom for the 5 Hz path |
| **Alert routing** | Alertmanager | Dedup, grouping, inhibition, silences — all solved. Emit to it rather than reimplementing. |

### 12.2 Local development

```yaml
# docker-compose.yaml
services:
  clickhouse:
    image: clickhouse/clickhouse-server
    ports: ["8123:8123", "9000:9000"]
  nats:
    image: nats:latest
    ports: ["4222:4222"]
  loadgen:
    build: ./cmd/loadgen        # synthetic metrics with injected anomalies
    environment:
      SERIES: 1000
      HZ: 5
```

**Build the load generator in milestone M1, not M7.** You cannot develop this project meaningfully against three hand-typed metrics. You need a thousand realistic series with seasonality, noise, and injectable anomalies from day one — it's what you'll test every subsequent milestone against.

### 12.3 Tools

| Tool | For |
|---|---|
| `vegeta` / `k6` | Load generation against the gateway |
| Chrome DevTools Performance panel | Frame timing, main-thread blocking, GC |
| `pprof` | Go CPU and heap profiling |
| `wscat` / `websocat` | Poking at the WebSocket by hand |
| Chrome's WebSocket frame inspector | Verifying your binary framing |
| `tc netem` | Simulating latency, jitter, and loss for reconnect testing |

---

## 13. Repository layout

```
Systems-Monitoring-Dashboard/
├── README.md
├── docs/
│   ├── design.md               ← this document
│   ├── latency-budget.md       ← the measured distribution, updated per release
│   ├── wire-format.md
│   └── alerting.md
├── cmd/
│   ├── gateway/                ← WebSocket server + hot ring
│   ├── ingest/
│   ├── detector/
│   ├── agent/
│   └── loadgen/                ← synthetic series with injectable anomalies
├── internal/
│   ├── wire/                   ← binary framing + Gorilla codec
│   │   ├── frame.go
│   │   ├── gorilla.go
│   │   └── registry.go         ← series interning
│   ├── ring/                   ← hot in-memory ring buffer
│   ├── hub/
│   │   ├── hub.go
│   │   ├── conn.go
│   │   ├── conflate.go         ← §6.4
│   │   └── snapshot.go         ← §7
│   ├── store/
│   │   ├── clickhouse.go
│   │   ├── rollup.go
│   │   └── downsample.go       ← min/max, LTTB
│   ├── detect/
│   │   ├── robust.go           ← MAD baselines
│   │   ├── seasonal.go
│   │   └── hysteresis.go
│   ├── alert/
│   │   ├── dedup.go
│   │   ├── group.go
│   │   └── inhibit.go
│   └── cardinality/
├── web/
│   ├── src/
│   │   ├── worker/
│   │   │   ├── decoder.ts
│   │   │   └── ws.ts
│   │   ├── ring.ts             ← typed-array ring buffers
│   │   ├── chart.ts            ← uPlot wrapper, rAF loop
│   │   ├── subscribe.ts        ← the §7 handshake
│   │   └── app.tsx
│   └── package.json
└── test/
    ├── replay/                 ← recorded real metrics for detector tuning
    └── chaos/                  ← latency, loss, slow-client injection
```

---

## 14. Milestone ladder

### M0 — Scope and the latency budget
**Est. 3–4 days**

Write section 3 for your specific target. Decide the collection frequency from the P99 requirement, not the other way around. Decide what's in the high-frequency path and what stays on the 15 s path.

**Done when:** you can state the required collection frequency and justify it arithmetically.

---

### M1 — End-to-end skeleton + load generator
**Est. 1 week**

One synthetic metric, ingest, WebSocket, one uPlot chart. JSON is fine here. Plus the load generator producing 1000 realistic series.

**Done when:** a number moves on screen, and you have a thousand series to point at it.

---

### M2 — Instrument the latency budget ★
**Est. 4–5 days**

`origin_ts` stamped at the source, measured at render, histogram of the full distribution, per-stage breakdown.

**Do this before any optimization.** Without it you will spend a week on the decoder while the collection interval contributes seven seconds. This milestone exists to prevent exactly that.

**Done when:** you have a P50/P95/P99 number and a per-stage breakdown, and can point at the dominant term.

---

### M3 — Binary transport
**Est. 1.5 weeks**

Series registry and interning, the frame format, Gorilla compression, `TCP_NODELAY`.

**Done when:** bytes-per-point is measured and compared against the JSON baseline, and the decoder round-trips under fuzzing.

---

### M4 — Backpressure and reconnection
**Est. 1 week**

Conflating bounded queue, slow-consumer disconnect, heartbeats, backoff with full jitter, visibility-based pause/resume.

**Test with a deliberately slow client.** Attach a client that reads one message per second while the server sends a thousand, and assert that gateway memory stays flat.

**Done when:** a slow client cannot grow server memory, and killing the network mid-stream produces a clean reconnect.

---

### M5 — The snapshot/stream boundary ★
**Est. 1 week**

Subscribe-then-snapshot with explicit watermark, buffer-and-reconcile on the client, the same path for initial load, reconnect, series addition, and range change.

**Done when:** a test that continuously loads the page against a running stream produces charts with no gaps and no duplicate timestamps, over hundreds of iterations.

---

### M6 — Storage and tiered aggregation
**Est. 1.5 weeks**

ClickHouse schema, materialized-view rollups with min/max/avg/sum/count, the query router, viewport downsampling.

**Done when:** a one-second spike is still visible on a 30-day chart, rendered as a band.

---

### M7 — Browser performance
**Est. 1.5 weeks**

Web Worker decode, typed-array ring buffers, rAF batching, uPlot.

**Done when:** 1000 series at 5 Hz renders at a sustained 60 fps with main-thread blocking under 8 ms per frame, verified in the DevTools Performance panel.

---

### M8 — Scale testing
**Est. 1 week**

N series × M clients. Find the knee. Profile it. Publish the numbers, including where it falls over.

---

### M9 — Anomaly detection
**Est. 1.5 weeks**

MAD baselines, seasonal comparison, hysteresis, tuned against replayed real data.

**Done when:** on a replay of a real week, the detector finds the incidents you know about and produces fewer than a handful of false positives per day — a number you measured, not assumed.

---

### M10 — Alert lifecycle
**Est. 1 week**

Dedup, grouping, inhibition, resolution, routing to Alertmanager or Slack.

**Done when:** a simulated rack failure across 200 series produces one notification, not 200.

---

### M11 — Operational hardening
**Est. 1.5 weeks**

Meta-monitoring, dead-man's switch, cardinality limits, multi-node with NATS, graceful drain.

---

## 15. Reference implementations

### 15.1 Gorilla timestamp compression

Delta-of-delta encoding: regularly-spaced timestamps compress to a single bit.

```go
// EncodeTimestamp writes ts using delta-of-delta encoding.
// Regular intervals produce a single '0' bit per point.
func (e *Encoder) EncodeTimestamp(ts int64) {
    if e.n == 0 {
        e.bw.WriteBits(uint64(ts), 64)
        e.prevTs = ts
        e.n++
        return
    }
    delta := ts - e.prevTs
    if e.n == 1 {
        e.bw.WriteBits(zigzag(delta), 32)
        e.prevTs, e.prevDelta, e.n = ts, delta, e.n+1
        return
    }

    dod := delta - e.prevDelta
    switch {
    case dod == 0:
        e.bw.WriteBit(0)                       // 1 bit — the common case
    case dod >= -63 && dod <= 64:
        e.bw.WriteBits(0b10, 2)
        e.bw.WriteBits(zigzag(dod), 7)
    case dod >= -255 && dod <= 256:
        e.bw.WriteBits(0b110, 3)
        e.bw.WriteBits(zigzag(dod), 9)
    case dod >= -2047 && dod <= 2048:
        e.bw.WriteBits(0b1110, 4)
        e.bw.WriteBits(zigzag(dod), 12)
    default:
        e.bw.WriteBits(0b1111, 4)
        e.bw.WriteBits(zigzag(dod), 32)
    }
    e.prevTs, e.prevDelta = ts, delta
    e.n++
}
```

Values use XOR against the previous value plus leading/trailing-zero counts. A metric that changes slowly XORs to very few significant bits. **Fuzz the round-trip** — bit-level codecs fail in ways unit tests miss, and `cargo fuzz`'s Go equivalent (`go test -fuzz`) finds them quickly.

### 15.2 The hot ring buffer

```go
// Ring holds the last N points per series in memory. It is the source for
// snapshots and for the detector, and is deliberately fixed-size.
type Ring struct {
    mu     sync.RWMutex
    series map[uint32]*seriesRing
    cap    int

    // watermark is the newest timestamp fully committed and visible.
    // Snapshots read this BEFORE scanning — see §7.2.
    watermark atomic.Int64
}

func (r *Ring) Append(seriesID uint32, ts int64, v float32) {
    r.mu.RLock()
    s, ok := r.series[seriesID]
    r.mu.RUnlock()
    if !ok {
        r.mu.Lock()
        if s, ok = r.series[seriesID]; !ok {
            s = newSeriesRing(r.cap)
            r.series[seriesID] = s
        }
        r.mu.Unlock()
    }
    s.append(ts, v)

    // Monotonic advance only — a late point must not move the watermark back.
    for {
        cur := r.watermark.Load()
        if ts <= cur || r.watermark.CompareAndSwap(cur, ts) {
            break
        }
    }
}
```

### 15.3 Client subscription handshake

```ts
async function subscribe(ws: WSClient, series: string[], from: number) {
  const buffer: Point[] = [];
  let phase: 'buffering' | 'live' = 'buffering';

  ws.onDelta = (pts) => {
    if (phase === 'buffering') buffer.push(...pts);
    else applyToRings(pts);
  };

  ws.send(encodeSubscribe(series));

  // The server returns the exact cut point with the data.
  const snap = await ws.request(encodeSnapshot(series, from));
  applySnapshot(snap);

  // Anything at or below the watermark is already in the snapshot.
  for (const p of buffer) {
    if (p.ts > snap.watermarkTs) applyToRings([p]);
  }
  buffer.length = 0;
  phase = 'live';
}
```

---

## 16. Testing and benchmarking

### 16.1 The load generator is a first-class component

It must produce, for each synthetic series: a realistic baseline with daily seasonality, correlated noise, occasional gaps, and **injectable anomalies with known ground truth** — a step change at time T, a spike at time U, a gradual drift starting at V.

Ground truth is what makes §16.3 possible. Without it you're eyeballing charts and guessing.

### 16.2 Chaos tests

| Test | Assertion |
|---|---|
| Slow consumer | Gateway memory stays flat; conflation rate rises; connection eventually closed |
| Network partition mid-stream | Clean reconnect, gap backfilled, no duplicate timestamps |
| Gateway restart under load | Clients reconnect with jitter; no thundering herd; no data loss beyond the documented watermark |
| Clock-skewed agent (+1 day) | Points rejected and counted; x-axis unaffected |
| Cardinality bomb (100k label values) | Rejected at ingest with a metric naming the label; TSDB unaffected |
| `tc netem` 200 ms + 5% loss | Stream degrades gracefully; latency histogram reflects it honestly |

### 16.3 Detector evaluation

Replay a recorded real week and measure precision and recall against ground truth. Report the operating point:

| Config | Precision | Recall | False alerts/day |
|---|---|---|---|
| 3σ, no hysteresis | 0.00001 | 1.00 | ~233,000 |
| 3σ, `for=3` | ? | ? | ? |
| MAD 4σ, `for=3`, seasonal | ? | ? | ? |

**Fill this table with measurements.** It's the most credible artifact the detection work can produce, and the first row — which you can compute analytically before writing any code — is what justifies everything in the rows below it.

### 16.4 Frontend benchmarks

Automate via Playwright + the Chrome tracing API: frame time distribution, main-thread blocking per frame, heap size after an hour, dropped frames. Track them in CI. A performance regression in the render path is as much a bug as a wrong number.

---

## 17. Stretch goals

| Feature | Effort | Value |
|---|---|---|
| **Correlation view** | Medium | "What else changed at the same time?" — often the single most useful feature during an incident |
| **Deploy annotations** | Small | Overlay deploy markers on charts. Cheap, and the first question during any incident is "did we just ship something?" |
| **Shareable live URLs** | Small | Send a colleague the exact view you're looking at |
| **Alert-to-chart deep links** | Small | An alert that links to the live view of the offending series |
| **Forecasting band** | Medium | Holt-Winters prediction interval rendered as a band |
| **Distributed tracing links** | Large | Jump from a latency spike to example traces |
| **Multi-window multi-burn-rate SLO alerts** | Medium | The Google SRE workbook approach; far better than static thresholds |
| **Downsampled overview + detail brush** | Medium | Overview chart at low resolution, brush to stream the detail |
| **WASM decoder** | Medium | Move the Gorilla decoder into WASM. Ties nicely to §9.2 and is measurable. |
| **Recording/replay of a live session** | Medium | Capture a five-minute window during an incident and replay it in the postmortem. Genuinely novel. |

That last one is worth a second look. Nobody can reproduce a live dashboard view after the fact, and "here is exactly what we were watching at 03:14" is a real gap in incident review.

---

## 18. References

### Papers and primary sources

- **Gorilla: A Fast, Scalable, In-Memory Time Series Database** (Pelkonen et al., VLDB 2015) — the compression scheme in §15.1, and the paper to read before designing the wire format
- **Site Reliability Engineering** (Google), "Monitoring Distributed Systems" and "Practical Alerting" — the alerting philosophy underlying §10
- **The Site Reliability Workbook**, "Alerting on SLOs" — multi-window multi-burn-rate alerting
- **Largest Triangle Three Buckets** (Steinarsson, 2013) — downsampling that preserves visual shape
- **Rousseeuw & Croux**, "Alternatives to the Median Absolute Deviation" — robust scale estimation
- **Benjamini & Hochberg**, false discovery rate — the formal treatment of §10.2

### Documentation

| Source | For |
|---|---|
| Grafana Live docs | The channel model, and their honest soft-real-time caveat |
| Prometheus: instrumentation and naming best practices | Cardinality guidance |
| Alertmanager docs | Grouping, inhibition, silences, the Watchdog pattern |
| ClickHouse materialized views and `AggregatingMergeTree` | Self-maintaining rollups |
| uPlot docs and demos | The performance ceiling you're designing against |
| MDN: WebSockets, Web Workers, `SharedArrayBuffer`, cross-origin isolation | Browser constraints in §9 |
| RFC 6455 | WebSocket framing, close codes, ping/pong |

### Code worth reading

- **VictoriaMetrics** — ingest path and cardinality handling
- **Prometheus TSDB** (`tsdb/` package) — head block, WAL, compaction
- **uPlot source** — small enough to read, and a clinic in Canvas performance
- **Centrifugo** — the pub/sub engine behind Grafana Live; connection management and conflation patterns
- **NATS** — subject-based fanout at scale

---

## Appendix A — Decision record

| Decision | Rationale |
|---|---|
| Collection at 5 Hz, not 1 Hz | Sub-second **P99** requires it. At 1 Hz the collection interval alone contributes up to 1 s in the tail. |
| Demand-driven high-frequency collection | 5 Hz is 75× the data of 15 s scraping. Pay it only for series someone is watching. |
| Two data paths, live and durable | The live path may drop data to stay fast; the durable path may be slow to stay complete. Conflating them means failing at one. |
| Binary framing with series interning | Grafana Live is JSON-only. Interning plus Gorilla gets ~9 B/point (or 2–4 with compression) against 150–400 for JSON. |
| WebSocket over SSE | SSE is text-only; base64 would throw away a third of the binary win. Borrow SSE's event-id resume idea. |
| One multiplexed connection per tab | Avoids repeated TCP slow-start and the browser's per-domain connection limits |
| Conflating bounded queue | WebSocket has no application flow control. A slow client will otherwise OOM the gateway. Conflation is correct for dashboards — only the latest value matters. |
| The live stream is explicitly lossy | Stated in the docs rather than hidden. The TSDB is the source of truth. |
| Subscribe → buffer → snapshot-with-watermark → reconcile | Query-then-subscribe leaves a gap; subscribe-then-query duplicates. The watermark must be read before the snapshot scan. |
| One code path for load, reconnect, series-add, range-change | Four hand-written variants means four off-by-one bugs |
| Don't build a TSDB | Years of work; all the interesting engineering here is in the streaming layer |
| Rollups store min/max, not just avg | Averaging a 1 s spike into a 1 min bucket hides it. For a monitoring tool that's a correctness failure, not a fidelity trade-off. |
| Downsample to viewport width server-side | A 1000 px chart cannot show more than 1000 x-positions |
| Typed-array ring buffers in the browser | `{t,v}` objects cost ~7× the memory and create millions of GC-traced objects |
| Decode in a Web Worker, render on rAF | 1000 msg/s must become ≤60 renders/s. Never render on message arrival. |
| uPlot, not D3/SVG, and never React for the chart | SVG dies at a few thousand points; React reconciliation per frame is the exact overhead being avoided |
| MAD, not standard deviation | σ has a breakdown point of zero — one outlier inflates it and masks the anomaly you wanted to catch |
| Guard against zero spread | A flat series has zero MAD; without the guard every sample becomes infinitely anomalous. A real cause of alert storms. |
| Hysteresis (`for: k`) as the primary false-positive control | 1000 series at 3σ yields ~233,000 false alerts/day; requiring 3 consecutive brings it to single digits |
| Same-time-last-week seasonal baseline | Handles daily and weekly patterns for a lookup and a median; degrades to rolling window without history |
| Emit to Alertmanager rather than reimplement routing | Dedup, grouping, inhibition, and silences are solved problems |
| Dead-man's switch | A silent alerting pipeline is indistinguishable from a healthy fleet |
| Cardinality limits enforced at ingest, rejected loudly | Unbounded label values kill every TSDB; a silent drop is nearly as bad as accepting |
| Instrument the latency budget in M2, before optimizing | Otherwise you optimize a 5 ms term while a 7 s term dominates |
| Load generator built in M1 | You cannot develop or evaluate any of this against three hand-typed metrics |

---

## Appendix B — Quick reference card

```
LATENCY BUDGET (the whole argument)
  15 s scrape  → P50 ≈ 7.6 s     collection interval dominates
  1 Hz push    → P50 ≈ 600 ms, P99 ≈ 1.1 s   ✗ misses sub-second
  5 Hz push    → P50 ≈ 200 ms, P99 ≈ 400 ms  ✓
  Cost of 5 Hz vs 15 s: 75× the data → demand-driven only
  TCP_NODELAY (Nagle costs up to 40 ms)
  Measure origin_ts → render. Do it BEFORE optimizing.

WIRE
  JSON + labels      150–400 B/point
  JSON + interned      ~40 B/point
  binary               ~9 B/point
  + Gorilla           ~2–4 B/point
  f32 values unless you know you need f64

BACKPRESSURE
  bounded queue per connection
  conflate by series_id — only the latest value matters
  expose queue_depth, conflation_rate, dropped
  close connections stuck full for >30 s
  heartbeat every 15–20 s (inside the LB idle timeout)
  reconnect: full jitter, rand(0, min(30s, 250ms·2^n))

SNAPSHOT/STREAM (order is load-bearing)
  1 subscribe, buffer (don't render)
  2 read watermark BEFORE the snapshot scan
  3 snapshot returns data + explicit watermark_ts
  4 render snapshot
  5 drain buffer, discard ts ≤ watermark
  6 go live
  Same path for: load · reconnect · add series · widen range

AGGREGATION
  rollups store min/max/avg/sum/count — NEVER avg alone
  render min–max as a band, avg as a line
  downsample to viewport width server-side
  min/max default · LTTB for a single clean line · last for gauges

BROWSER
  {t,v} objects   ~56 B/pt → ~200 MB for 3.6 M pts  ✗
  Float32+Uint32    8 B/pt →  ~29 MB                ✓
  decode in a Worker · accumulate on message · render on rAF
  1000 msgs in a frame = 1 render
  pause the stream on visibilitychange

ANOMALY
  1000 series × 3σ × 1 Hz = ~233,000 false alerts/day
  for k=2 → ~630/day     k=3 → ~1.7/day   (independence assumed; real is worse)
  robust σ = 1.4826 × MAD   (σ has breakdown point 0)
  guard spread < 1e-9 or flat series alert forever
  baseline: same wall-clock slot, previous k weeks, median
  pipeline: detect → hysteresis → dedup → group → inhibit → notify
  every alert needs a resolve path, with hysteresis on the way down
```
