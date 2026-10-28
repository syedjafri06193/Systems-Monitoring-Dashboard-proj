# Systems Monitoring Dashboard

A push-first operational tail view: binary-framed WebSocket streaming of
high-frequency metrics with sub-second P99 end-to-end latency, backed by a
tiered store, with robust-statistics anomaly detection and hysteresis-based
alerting designed to survive the multiple-comparisons problem.

This is **the window you open during the ten minutes that matter** — a
canary shifting traffic, a load test ramping, an incident in progress, a
deploy rolling out. Grafana at a 5-second refresh is fine for a dashboard
you glance at. It is useless for watching a traffic shift happen.

```bash
go test ./... -race          # 279 Go tests
go run ./cmd/gateway -loadgen -series 1000 -hz 5
cd web && npm install && npm test && npm run dev
```

Then open http://localhost:5173.

`/stream` refuses cross-origin browser connections, so a page on another
site cannot read your metrics through a visitor's browser. If the dashboard
is served from a different origin than the gateway, allow it explicitly:
`-allowed-origins https://dash.example.com`.

## The argument, in one table

| collection model | end-to-end P50 | P99 |
| --- | --- | --- |
| Prometheus scrape @ 15 s | 7,600 ms | 15,050 ms |
| push @ 1 Hz | 600 ms | 1,190 ms ❌ |
| push @ 5 Hz | **200 ms** | **398 ms** ✅ |

**Sub-second latency is a collection-architecture problem, not a transport
problem.** If you scrape every 15 seconds, your median staleness is 7.5
seconds no matter how fast your WebSocket is. The collection interval
dominates every other term, usually by two orders of magnitude.

At 1 Hz you make P50 comfortably and miss P99, because the collection
interval alone can contribute a full second in the tail. That is why the
answer is 5 Hz and not 1 Hz — and it is arithmetic, not taste:
`internal/latency` computes it and the tests assert it.

Nothing is free: 5 Hz is **75× the data volume** of a 15-second scrape,
which is why the high-frequency path is demand-driven and has a 15-minute
retention.

## What is actually built

- **Binary wire format** with series interning and a full Gorilla codec.
  Measured: **9.07 B/pt** framed, **2.75 B/pt** compressed, against 95 B/pt
  for the same data with inline labels — a 9× gain from interning alone.
- **A conflating bounded queue** per connection. One slow client cannot
  grow gateway memory; measured 94% conflation and **zero drops** while a
  client read nothing at all.
- **The snapshot/stream boundary** with an explicit watermark, proven with
  a deterministic test that injects a write *during* the scan, plus 300
  concurrent iterations asserting no gaps and no duplicate timestamps.
- **MAD-based detection** with hysteresis, seasonality, and the full alert
  lifecycle (dedup → group → inhibit → resolve).
- **An RFC 6455 WebSocket server** written against the standard library.
- **A load generator** with autocorrelated noise, daily and weekly
  seasonality, and injectable anomalies with ground truth — which is what
  makes the detector measurable rather than eyeballed.
- **Typed-array ring buffers** in the browser: 8 B/pt against ~56 for
  objects, with decode in a Web Worker and rendering batched to rAF.

## The six things that matter

Most of the difficulty is in places where the obvious implementation
produces something that looks like it works.

### 1. The collection interval is the whole budget

See the table above. Everything else in the path is ~100 ms combined.
Optimising the decoder while a 15-second scrape sits upstream is a week
spent on 2% of the problem — which is why the instrumentation exists before
any optimisation, and why `Budget.Dominant()` names the largest term.

### 2. WebSocket has no application-level flow control

Write faster than a client reads and the kernel buffer fills, then the
application buffer grows, then the gateway runs out of memory. One client on
hotel WiFi takes down the service for everyone.

The fix is a bounded queue plus **conflation**: for a dashboard only the
latest value per series matters, so a new point replaces a pending one
rather than queueing behind it. A slow client sees a lower-resolution
stream, not a stalled one.

**The live stream is lossy by design.** The TSDB is the source of truth.
This is stated in the docs rather than hidden, because a user drawing
conclusions from silently-dropped data is worse than a user who knows the
stream is decimated.

### 3. Joining history to a live stream is the subtle part

Both obvious orderings are wrong. Query-then-subscribe leaves a **gap** that
looks like a quiet period. Subscribe-then-query **duplicates**, and rate
calculations double-count.

The correct procedure is subscribe → buffer → snapshot-with-watermark →
reconcile, and the load-bearing detail is that the server reads the
watermark **before** the scan. Read it after, and a point written mid-scan
is in neither the snapshot nor the buffer.

Implemented once. Initial load, reconnect, adding a series and widening the
range all route through it — four hand-written variants would be four
different off-by-one bugs.

### 4. Averaging is a correctness failure, not a fidelity trade-off

A one-second burst to 100% CPU, averaged into a one-minute bucket alongside
59 seconds at 20%, reads as **21.3%**. The incident is invisible and someone
looking at the dashboard concludes nothing happened.

So every rollup stores min/max/sum/count, and charts render a band. Measured:
a one-second spike is still visible on a 30-day chart, while the highest
column *average* across that same chart is 20.1%.

### 5. 1,000 series at 3σ is 233,000 false alerts per day

From a perfectly healthy fleet. That is the definition of 3σ, not a bug.

Hysteresis is the fix with the best value-to-effort ratio — requiring three
consecutive breaches takes the predicted rate to 1.7/day. But that
arithmetic assumes independent samples, and real metrics are autocorrelated:
measured against AR(1) series, the true rate was **1,008/day**, four orders
of magnitude worse than predicted.

And standard deviation makes it worse, not better: it has a **breakdown
point of zero**, so one outlier inflates the threshold and masks the next
anomaly. Measured, a baseline polluted by a single earlier incident gave
MAD a z of 7.2 on a genuine anomaly and stddev a z of −0.03.

### 6. The browser is the bottleneck you hit first

One hour of 1 Hz data for 1000 series is 3.6 M points. As `{t, v}` objects
that is ~200 MB and 3.6 million objects for the GC to trace; as
Float32 + Uint32 it is 28.8 MB. A 7× difference that decides whether the
tab survives.

And **never render on message arrival**: at 1000 messages per second that is
1000 render attempts into a 60 Hz display. Accumulate on arrival, render at
most once per frame.

## Layout

```
internal/
  wire/         framing, Gorilla codec, series interning   (+ 2 fuzz targets)
  ring/         the hot in-memory ring and its watermark
  hub/          conflating queue, snapshot boundary, connections
  ws/           RFC 6455 server, standard library only
  detect/       MAD baselines, hysteresis, seasonality
  alert/        dedup, grouping, inhibition, lifecycle
  cardinality/  ingest-time limits, rejected loudly
  store/        rollups, min/max downsampling, LTTB, tier routing
  latency/      the budget, instrumented and computed
  loadgen/      synthetic series with ground-truth anomalies
cmd/
  gateway/      the stream server (run with -loadgen)
  detector/     evaluation (try -explain)
  agent/        bounded-buffer push agent
  loadgen/      inspect the generator
  genfixtures/  emit wire fixtures for the TypeScript tests
web/
  src/ring.ts         typed-array ring buffers
  src/subscribe.ts    the §7 handshake, client side
  src/worker/         WebSocket + decoder, off the main thread
  src/chart.ts        uPlot wrapper and the rAF loop
docs/
  latency-budget.md   the measured distribution
  wire-format.md      the format and why each field is what it is
  alerting.md         the false-positive arithmetic
  notes-on-the-spec.md  where the design doc was wrong
```

## Tests

**279 Go tests** (all under `-race`) and **67 TypeScript tests**.

The Go and TypeScript wire implementations are tested against **shared
fixtures generated by the Go encoder**, so the two cannot drift together —
testing each against its own idea of the format would prove nothing.

Two fuzz targets on the Gorilla codec, over a million executions clean. The
decoder parses untrusted network input, and bit-level codecs fail in ways
unit tests miss.

Several tests exist specifically to document a surprise:

- a 1% slow tail is invisible to P99 *by definition*;
- `for=3` missed five of six injected anomalies — hysteresis trades recall
  for precision, steeply, on spikes;
- 24 hours across 1000 pixels routes to the 1-minute tier, not raw, because
  raw would return 86× more points than the chart can draw.

## Notes on the design document

Ten places where building this disagreed with the spec are recorded in
[`../1—Documentation/v1/notes-on-the-spec.md`](../1—Documentation/v1/notes-on-the-spec.md), with measurements.
The three that matter most:

- **The Gorilla encoder in §15.1 corrupts data.** It calls `zigzag()` but
  uses the paper's non-zigzag ranges, so `zigzag(64) = 128` is written into
  7 bits and truncated to 0. Silent, and only on boundary values.
- **Conflation starves series** when the queue is smaller than the
  subscription: measured 270k drops against 123k conflations, always the
  same series. The queue now grows to fit the subscription.
- **The false-alert arithmetic is optimistic by ~14,800×** once
  autocorrelation is accounted for.

## Explicit non-goals

Not a time-series database (writing one is a multi-year project — the
interesting engineering here is the streaming layer). Not a Grafana
replacement. Not a logs or traces platform. Not an incident-management
system. Not a lossless live stream.
