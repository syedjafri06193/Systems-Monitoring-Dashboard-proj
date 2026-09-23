# The latency budget

The centrepiece. Decompose before optimising anything, because the dominant
term is almost never where people look.

## The argument, in one table

| collection model | stage 1 P50 | everything else | **end-to-end P50** | **P99** |
| --- | --- | --- | --- | --- |
| Prometheus scrape @ 15 s | 7,500 ms | ~100 ms | **7,600 ms** | 15,050 ms |
| push @ 1 Hz | 500 ms | ~100 ms | **600 ms** | 1,190 ms ❌ |
| push @ 5 Hz | 100 ms | ~100 ms | **200 ms** | 398 ms ✅ |

**Sub-second P99 requires collection at roughly 5 Hz or faster.** At 1 Hz
the median is comfortable and the tail misses, because the collection
interval alone can contribute a full second.

This is arithmetic, not opinion — `internal/latency` computes it, and
`TestTheStandardScenarioTable` asserts it. Anyone proposing a different
collection interval can re-derive the consequence:

```go
latency.RequiredCollectionInterval(targetP99Ms, otherMs)
```

## The full path

| # | stage | contribution |
| --- | --- | --- |
| 1 | event → collector observes | **0 to one collection interval** |
| 2 | agent batches and flushes | 0 to the flush deadline (50 ms) |
| 3 | network: agent → ingest | 1–50 ms |
| 4 | ingest parse, validate, route | 1–10 ms |
| 5 | pub/sub fanout | 1–10 ms |
| 6 | server send queue wait | 0 to queue drain |
| 7 | network: server → browser | 5–100 ms |
| 8 | client decode | 1–50 ms |
| 9 | wait for the next animation frame | 0–16.7 ms |
| 10 | monitor refresh | 0–16.7 ms |

Stage 1 dominates by two orders of magnitude on a scraped pipeline. Every
other stage is noise until it is fixed.

## The cost of 5 Hz

Nothing is free. Measured by `TestFiveHertzCostsSeventyFiveTimesTheData`:

| | 15 s scrape | 5 Hz push | ratio |
| --- | --- | --- | --- |
| points/series/day | 5,760 | 432,000 | **75×** |
| 1000 series, points/day | 5.8 M | 432 M | 75× |
| raw at 16 B/pt | 92 MB/day | 6.9 GB/day | 75× |
| Gorilla at ~1.4 B/pt | 8 MB/day | 605 MB/day | 75× |

Which is why the high-frequency path is **demand-driven**: collection runs
at 5 Hz only for series with an active subscriber, and high-frequency data
has a 15-minute retention.

## Small terms worth fixing

- **`TCP_NODELAY`.** Nagle's algorithm can add up to 40 ms waiting to
  coalesce small writes — 20% of the budget, for a one-line fix. Set on
  every stream socket at upgrade.
- **The render frame.** A perfectly delivered message still lands at the
  next paint. You cannot beat one frame; batch into it instead.
- **GC pauses.** Both Go's collector and the browser's occasionally add
  tens of milliseconds. Reused buffers, typed arrays and no per-message
  objects are the fix, and they are why the wire format and the ring
  buffers look the way they do.

## Measuring it

Stamp `origin_ts` at observation, compare at render:

```ts
const e2eMs = performance.timeOrigin + performance.now() - point.originTsMs;
```

Two caveats that matter:

- **This crosses two clocks.** The collector's and the browser's differ.
  The heartbeat carries the server timestamp so the offset can be
  estimated; across machines treat the absolute number as approximate while
  the *distribution shape* stays meaningful.
- **Report the distribution, not a number.** A mean hides exactly the
  problem: 980 frames at 30 ms and 20 at 5 s average to 129 ms while the
  viewer sees a stutter every fiftieth frame.

And one thing worth knowing before choosing a percentile: **a tail thinner
than 1-in-100 is invisible to P99 by definition.** Exactly 1% slow samples
put the 99th percentile right on the boundary, where it reports the fast
number.

## What is actually controllable

Most of the budget is not overhead. Resolving a 40 Hz bass note — or a
40 Hz-equivalent metric feature — requires observing it, and a 4096-point
window at 48 kHz is 85 ms of audio whose result describes the *centre* of
that window.

For metrics the equivalent is: the collection interval is physics given
your sampling rate, output-device latency belongs to the device, and what
remains — the flush deadline, the queue, the decode, the frame — is the
part engineering can move.

`Budget.Dominant()` names the largest P99 term, which is the output that
actually changes behaviour.
