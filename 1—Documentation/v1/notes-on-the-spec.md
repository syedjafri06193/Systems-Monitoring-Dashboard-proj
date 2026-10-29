# Notes on the spec

Places where building this disagreed with the design document — bugs in its
reference code, claims that did not survive measurement, and gaps that only
appear once something runs. Each entry says what the document says, what
actually happens, and what this implementation does instead.

The document is unusually good; most of it survived contact with a compiler
intact. These are the exceptions.

---

## 1. The Gorilla timestamp encoder in §15.1 corrupts data

The reference implementation calls `zigzag()` on the delta-of-delta but uses
the Gorilla paper's ranges:

```go
case dod >= -63 && dod <= 64:
    e.bw.WriteBits(0b10, 2)
    e.bw.WriteBits(zigzag(dod), 7)      // ← zigzag(64) is 128
```

Those two are incompatible. The paper's ranges are for storing the value as
plain signed bits; **zigzag maps `[-64, 63]` onto `[0, 127]`**, so
`zigzag(64) = 128` does not fit in 7 bits and is silently truncated to 0.
The same off-by-one affects every band:

| field   | doc's range      | correct zigzag range |
| ------- | ---------------- | -------------------- |
| 7 bits  | `[-63, 64]`      | `[-64, 63]`          |
| 9 bits  | `[-255, 256]`    | `[-256, 255]`        |
| 12 bits | `[-2047, 2048]`  | `[-2048, 2047]`      |

The result is a corrupt timestamp, but only for delta-of-delta values
landing exactly on a boundary — which is why it survives casual testing. It
was found here by a boundary test, not by fuzzing.

**Also:** the paper's encoder stops at a 32-bit escape and silently corrupts
anything larger. This adds a 64-bit escape (`11111`), restructuring the
prefix so the 32-bit code is `11110`. It costs one bit on a case that never
occurs in practice and makes the codec total — which fuzzing finds
immediately otherwise.

Covered by `TestRoundTripDeltaOfDeltaBoundaries` and two fuzz targets (over
a million executions clean).

---

## 2. "~2–4 bytes/point" holds only for f32-precision values

§6.2's table claims Gorilla reaches 2–4 B/pt. Measured here:

| values                        | bytes/point |
| ----------------------------- | ----------- |
| f32-precision (what we store) | **2.75**    |
| full float64 random walk      | 6.38        |
| constant series               | **0.25** (2 bits) |

The claim is right for this system, because `model.Point.Value` is a
`float32` — and §6.2 itself recommends f32. But the reason is worth stating:
a float32 widened to float64 has 29 zero bits in the low mantissa, so
consecutive XORs have a long trailing-zero run and the significant window
stays narrow. Genuine float64 data has no such structure and costs roughly
2.3× more. A reader who stores f64 values and expects 3 B/pt will be
disappointed, and will not know why.

---

## 3. "~9 bytes/point" is a property of the batch window, not the format

Same section. The delta-batch figure depends on the uvarint timestamp
delta staying inside one byte, i.e. the batch spanning under 128 ms:

| batch window          | bytes/point |
| --------------------- | ----------- |
| 50 ms (the agent's flush deadline, §5.3) | **9.07** |
| 100 s                 | 10.87       |

So the number is correct for the system as designed, and the design is
self-consistent — but the two facts are in different sections and neither
references the other.

---

## 4. Conflation silently starves series when the queue is smaller than the subscription

§6.4's conflating queue bounds memory at `maxLen × sizeof(Point)`, and the
reference implementation drops when the queue is "genuinely full of distinct
series". What it does not say is what happens when the *subscription* is
larger than the queue.

Measured with 200 subscribed series and a 64-slot queue, under sustained
load:

```
conflated 123,165   dropped 269,667   conflation rate 30.8%
```

Two thirds of the points were dropped, and always for the same 136 series —
the ones that lost the race to fill the queue. Those charts simply never
update. That is not the graceful degradation conflation promises; it is a
silent partial outage that looks like a data problem.

**What this does instead.** The natural capacity of a conflating queue is
the number of series it carries, because conflation guarantees at most one
pending point per series — so a queue that size can never overflow. The
queue grows to fit the subscription, with the configured depth as a
*ceiling* against a pathological subscription rather than a target. Same
configuration after the change:

```
conflated 376,187   dropped 0   conflation rate 94.0%   max depth 200
```

Memory is still bounded — by the subscription size, which the client chose,
and hard-capped by `MaxQueueDepth`.

---

## 5. The false-alert arithmetic is optimistic by four orders of magnitude

§10.2 gives the hysteresis table and states the caveat honestly: the
arithmetic assumes independent samples, real metrics are autocorrelated, so
"the true false-alert rate will be higher than this bound."

It is much higher. Measured against the load generator's AR(1) series
(lag-1 autocorrelation 0.85, which is realistic for CPU):

| | predicted | measured |
| --- | --- | --- |
| 40 series, 3σ, MAD, `for=3` | **0.07 /day** | **1,008 /day** |

That is ~14,800× worse, not "somewhat" worse. The direction of the fix is
still right — hysteresis reduced false positives 28× in the same
measurement — but the table is a lower bound on a quantity that is four
orders of magnitude larger in practice, and treating it as a forecast would
lead someone to deploy `for=3` expecting two false alerts a day.

The document already says to tune against replayed data. This is the number
that shows why that instruction is not optional.

---

## 6. §16.3's operating-point table, filled in

The document leaves this table with question marks. Measured against the
load generator with ground truth (40 series, 1200 evaluations, 6 injected
anomalies):

| config                         | precision | recall | false alerts/day |
| ------------------------------ | --------- | ------ | ---------------- |
| 3σ, stddev, no hysteresis      | 0.057     | 1.00   | 7,200            |
| 3σ, stddev, `for=3`            | 0.200     | 0.33   | 576              |
| 3σ, MAD, no hysteresis         | 0.109     | 1.00   | 8,208            |
| 4σ, MAD, `for=3`               | 0.200     | 0.17   | 288              |

Two observations the document does not make. Precision stays low even at
the best operating point — grouping and inhibition, not detection, are
what make this bearable. And recall falls sharply with hysteresis: `for=3`
at 4σ missed five of six injected anomalies, because short spikes do not
survive three consecutive evaluations. **Hysteresis trades recall for
precision, and on spike-shaped anomalies the trade is steep.** A system
that needs to catch brief excursions needs a second, spike-specific rule
rather than a longer `for`.

---

## 7. Standard deviation's masking needs a realistic fixture to demonstrate

Not an error, but the obvious way to test §10.3's claim does not work, and
an early version of these tests asserted it wrongly.

A constant series plus one outlier does *not* demonstrate masking: a second
spike of the same magnitude still breaches even an inflated σ (measured
z = 9.95), and a constant series has zero MAD so the robust path is
degenerate too.

What masking actually costs is the ability to see a **normal-sized** anomaly
afterwards. With 200 samples at 50 ± 2 and one earlier incident at 5000:

| | spread | z for a genuine 65 (7σ) anomaly |
| --- | --- | --- |
| MAD | **2.09** | **7.2** — flags it |
| stddev | 349.2 | −0.03 — completely blind |

---

## 8. `SlotOf` needs floor semantics, not Go's `%`

§10.4 describes same-time-last-week seasonal baselines but gives no slot
arithmetic. Go's `%` keeps the sign of the dividend, so a pre-epoch
timestamp produces a negative slot index and a map lookup that never
matches anything — the detector silently falls back to the rolling window
forever. The same applies to rollup bucket alignment (`ts - ts%width` puts
pre-epoch points in the bucket *above* them).

Both use an explicit floor division here.

---

## 9. Tier routing at exactly the retention boundary

§8.2's query router uses `time.Since(from) < retention`. "Last 24 hours" is
the most common dashboard range and lands exactly on the raw tier's 24-hour
retention, so a strict comparison silently serves it from the 1-minute
tier — costing resolution on the query people run most.

The comparisons are inclusive here. At the boundary the oldest points may
be mid-expiry, which shows up as a slightly short range rather than as
wrong data.

---

## 10. The WebSocket magic GUID, and why hand-rolling needs the RFC's test vectors

Not a fault in the document — it correctly points at RFC 6455 — but worth
recording because it cost real time here. The handshake GUID is:

```
258EAFA5-E914-47DA-95CA-C5AB0DC85B11
```

The final group is `C5AB0DC85B11`. Transcribing it as `5AB0DC85B11F` (a
plausible-looking shift of the `F`) produces a syntactically valid GUID and
a handshake that **every browser silently refuses**. There is no error
message that points at it.

The fix is to test against the RFC's worked examples rather than against
your own implementation:

| key | accept |
| --- | --- |
| `dGhlIHNhbXBsZSBub25jZQ==` | `s3pPLMBiTxaQ9kYGzzhZRbK+xOo=` |
| `x3JJHMbDL1EzLkh9GBhXDw==` | `HSmrc0sMlYUkAGmm5OPpG2HaGWk=` |

---

## Smaller corrections

- **§6.2** does not say whether the delta-batch timestamp base is the first
  point or the minimum. It must be the **minimum**: batches arrive
  out of order from different agents, and a negative delta encoded as a
  uvarint becomes an enormous positive one, putting timestamps thousands of
  years in the future.

- **§9.1**'s `SeriesRing` stores `tsMs - baseTs` in a `Uint32` without
  guarding against a point older than the base. A negative offset wraps to
  ~49 days in the future, and one late point stretches the chart's x-axis by
  seven weeks. Late points are dropped here.

- **§9.1** also says to "re-base when you approach the limit" without saying
  what the new base is. It must be the *oldest retained* timestamp, not the
  newest, or the rest of the buffer becomes unrepresentable.

- **The client-side varint** must not use JavaScript's bitwise operators.
  They coerce to int32, so a timestamp delta over 2³¹ ms silently goes
  negative. Multiplying by `2**shift` keeps full 53-bit precision.

- **§10.5** lists dedup before grouping and inhibition. Inhibition must come
  **first**: a suppressed alert that consumes its dedup slot stays silent
  after the parent resolves, which is exactly when someone is checking
  whether the fix worked.

- **§11.2**'s cardinality check validates label-value length after touching
  state. A 2 MB label value should be rejected before anything records it.

- **§3.1** stops the latency path at "monitor refresh". Worth adding that a
  tail thinner than 1-in-100 is invisible to P99 *by definition* — a 1%
  stutter rate reports a clean P99 — so the percentile to alert on should be
  chosen against the expected incident rate, not by habit.
