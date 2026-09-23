# Alerting

Detecting that a number moved is easy. Not drowning people in alerts is the
entire engineering problem, and the failure mode is quantifiable.

## The multiple-comparisons problem

A 3σ two-tailed threshold flags 0.27% of samples from a **perfectly normal,
perfectly healthy** series. That is the definition of 3σ, not a bug.

| | value |
| --- | --- |
| series monitored | 1,000 |
| P(flag \| healthy, 3σ) | 0.0027 |
| expected false flags per evaluation | 2.7 |
| evaluations per day at 1 Hz | 86,400 |
| **expected false alerts per day** | **233,262** |

A quarter of a million false alerts per day from a system where nothing is
wrong. `internal/detect` computes this; `TestTheQuarterMillionFalseAlertsPerDay`
asserts it.

## Hysteresis

Require the condition to hold for *k* consecutive evaluations — Prometheus's
`for:` clause:

| consecutive | expected false alerts/day |
| --- | --- |
| 1 | 233,262 |
| 2 | 630 |
| 3 | **1.70** |

**This arithmetic assumes independent samples, and that assumption is badly
wrong.** Real metrics are autocorrelated — if CPU is high now it is probably
high a second from now — so consecutive breaches are not independent.
Measured against AR(1) series with ρ = 0.85:

| | predicted | measured |
| --- | --- | --- |
| 40 series, 3σ, `for=3` | 0.07/day | **1,008/day** |

Four orders of magnitude. The direction of the fix is still right (28×
fewer false positives in the same measurement), but the table is a lower
bound, not a forecast. **Tune against replayed data.**

Hysteresis also costs recall, and the cost is steep on spikes: at 4σ with
`for=3` the detector missed five of six injected anomalies, because a short
excursion does not survive three consecutive evaluations. A system that must
catch brief spikes needs a separate spike rule, not a longer `for`.

## Robust statistics

**Standard deviation has a breakdown point of zero.** One extreme outlier
inflates it arbitrarily — so the spike you want to detect raises your own
threshold and masks itself.

Measured: 200 samples at 50 ± 2, plus one earlier incident at 5000, then a
genuine 65 (a real 7σ event):

| | spread | z | verdict |
| --- | --- | --- | --- |
| MAD (×1.4826) | **2.09** | **7.2** | flags it |
| stddev | 349.2 | −0.03 | completely blind |

Median absolute deviation has a **50% breakdown point**: half the data can
be garbage and the estimate holds.

### The zero-spread guard

A series that has been constant — a gauge at zero, a counter that has not
moved — has a MAD of exactly zero. Without a guard, the next sample that
differs at all divides by ~0, becomes infinitely anomalous, and the detector
alerts forever.

This is a real and common cause of alert storms in homegrown detectors, and
it costs one comparison. The load generator includes a deliberately flat
gauge so the case is always exercised.

## Seasonality

CPU at 3 a.m. is not CPU at 2 p.m. A rolling one-hour baseline alerts every
morning when traffic ramps, and after a week of that nobody reads its
alerts.

**Same-time-last-week** has the best value for the effort: compare against
the same wall-clock slot in the previous *k* weeks and take the **median**
(not the mean — one bad week should not move the baseline for that hour
every subsequent week). It handles daily and weekly patterns with a lookup,
and degrades to the rolling window when history is short, which is the state
every new deployment is in for a fortnight.

Slot arithmetic uses floor division, not Go's `%`: a negative slot index is
a map lookup that never matches, so the detector silently falls back to the
rolling window forever.

Combining: take the **center** from the seasonal estimate and the **larger**
spread of the two. A seasonal spread computed from four weekly samples is a
poor scale estimate, and being more sensitive than either estimate alone
justifies is the wrong direction to err when the cost is someone's sleep.

## The lifecycle

```
detect → hysteresis → dedup → group → inhibit → notify
```

Skipping the last three is why homegrown alerting is unbearable.

**Grouping** turns a rack failure from 200 pages into one. Group by a
configurable label set with a short wait window, because related alerts do
not arrive simultaneously — each series crosses its own threshold at its own
moment.

**Inhibition** stops "the datacenter is unreachable" from also paging about
every service inside it. Scoped by equality on shared labels, or a node-down
alert in one cluster silences service alerts everywhere. Two details:

- Only **firing** alerts inhibit. A resolved parent that keeps silencing its
  children hides every downstream problem at exactly the moment someone is
  checking whether the fix worked.
- An alert must never inhibit **itself**, which it would whenever a rule's
  source and target matchers overlap — and "critical inhibits warning on the
  same host" overlaps for any alert that is both.

**Order matters: inhibit before dedup.** A suppressed alert that consumes
its dedup slot stays silent after the parent resolves.

**Every alert needs a resolution path**, with hysteresis on the way down.
An alert that fires and never resolves, or that flaps, trains people to
ignore alerts — which is worse than not alerting at all.

## Meta-monitoring

If the monitoring shares infrastructure with what it monitors, it fails
exactly when you need it.

The **dead-man's switch** (`/watchdog`) is an alert that fires *constantly*,
wired to an external service that pages when it **stops** arriving. It is
the only way to detect that the alerting pipeline itself is dead, because a
silent alerting system looks exactly like a healthy fleet.

`/stats` exposes ingest rate, connection count, per-connection queue depth,
conflation rate and drop count. A high conflation rate is the first thing
anyone wants during a "why is my chart choppy" investigation.

## Cardinality

Series count is the **product** of label cardinalities. One label carrying a
user id, request id or raw URL path makes it unbounded, and an unbounded
series count kills every TSDB there is.

Enforced at ingest, where refusing is still possible, and **rejected
loudly** with the offending label named: "cardinality exceeded" sends
someone on an hour's hunt; "cardinality exceeded on label `request_id`" is
fixed in a minute by the person who added it. `TopLabels` ranks a metric's
labels by distinct values, which answers "because of what?" rather than
just "too many".

Length is checked before any state is touched — a 2 MB label value is not
something to record and then reject.
