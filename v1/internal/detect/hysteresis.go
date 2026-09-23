package detect

import (
	"github.com/syedjafri06193/Systems-Monitoring-Dashboard-proj/internal/model"
)

// Hysteresis: require a condition to hold for k consecutive evaluations
// before it becomes an alert.
//
// This is the fix with by far the best ratio of value to effort, and the
// arithmetic is dramatic. At 1000 series, 3 sigma, 1 Hz:
//
//	k=1  ~233,000 false alerts/day
//	k=2  ~630
//	k=3  ~1.7
//
// Two caveats stated honestly. The arithmetic assumes independent samples,
// and real metrics are autocorrelated -- if CPU is high now it is probably
// high a second from now -- so consecutive breaches are not independent and
// the true rate is higher. And k trades detection latency for precision: at
// 5 Hz, k=3 costs 600 ms before an alert fires, which is usually a fine
// trade and occasionally is not.
//
// The same hysteresis applies on the way *down*. An alert that resolves the
// instant the condition clears flaps, and a flapping alert trains people to
// ignore alerts -- which is worse than not alerting at all.

// State is where one series sits in the firing lifecycle.
type State uint8

const (
	// StateOK is the steady state: nothing breaching.
	StateOK State = iota
	// StatePending is breaching but has not yet held for long enough.
	StatePending
	// StateFiring has held. This is the only state that notifies.
	StateFiring
	// StateResolving was firing and has stopped breaching, but has not yet
	// been clear for long enough to resolve.
	StateResolving
)

func (s State) String() string {
	switch s {
	case StateOK:
		return "ok"
	case StatePending:
		return "pending"
	case StateFiring:
		return "firing"
	case StateResolving:
		return "resolving"
	default:
		return "unknown"
	}
}

// Transition is what changed on one evaluation.
type Transition uint8

const (
	TransitionNone Transition = iota
	// TransitionFired is the moment to notify.
	TransitionFired
	// TransitionResolved is the moment to send the resolution.
	//
	// Every alert needs a resolution path. An alert that fires and never
	// resolves trains people to ignore alerts.
	TransitionResolved
)

// Config controls firing and resolution.
type Config struct {
	// ForCount is how many consecutive breaching evaluations are required.
	// Prometheus calls this `for:`.
	ForCount int
	// ClearCount is how many consecutive non-breaching evaluations are
	// required to resolve. Defaults to ForCount when zero: asymmetric
	// hysteresis is a deliberate choice, not an accident, so it has to be
	// asked for.
	ClearCount int
}

func (c Config) forCount() int {
	if c.ForCount < 1 {
		return 1
	}
	return c.ForCount
}

func (c Config) clearCount() int {
	if c.ClearCount < 1 {
		return c.forCount()
	}
	return c.ClearCount
}

// seriesState is the per-series counter pair.
type seriesState struct {
	state       State
	breaching   int
	clear       int
	firstBreach int64
	firedAt     int64
}

// Hysteresis tracks firing state per series.
//
// Not safe for concurrent use: the detector evaluates series in one
// goroutine by design, because alert state that can be updated from two
// places is state nobody can reason about during an incident.
type Hysteresis struct {
	cfg   Config
	state map[model.SeriesID]*seriesState
}

func NewHysteresis(cfg Config) *Hysteresis {
	return &Hysteresis{cfg: cfg, state: make(map[model.SeriesID]*seriesState)}
}

// Result is one evaluation's outcome.
type Result struct {
	Series     model.SeriesID
	State      State
	Transition Transition
	// FirstBreachMs is when the condition started, not when it fired. It
	// is what belongs in the notification: "high since 03:14" is far more
	// useful than "alerted at 03:17".
	FirstBreachMs int64
	// Consecutive is how many evaluations the current run has lasted.
	Consecutive int
}

// Observe records one evaluation for a series.
func (h *Hysteresis) Observe(id model.SeriesID, breaching bool, nowMs int64) Result {
	s, ok := h.state[id]
	if !ok {
		s = &seriesState{state: StateOK}
		h.state[id] = s
	}

	if breaching {
		s.clear = 0
		if s.breaching == 0 {
			s.firstBreach = nowMs
		}
		s.breaching++

		switch s.state {
		case StateOK:
			if s.breaching >= h.cfg.forCount() {
				s.state = StateFiring
				s.firedAt = nowMs
				return Result{id, StateFiring, TransitionFired, s.firstBreach, s.breaching}
			}
			s.state = StatePending

		case StatePending:
			if s.breaching >= h.cfg.forCount() {
				s.state = StateFiring
				s.firedAt = nowMs
				return Result{id, StateFiring, TransitionFired, s.firstBreach, s.breaching}
			}

		case StateResolving:
			// It started breaching again before the resolution completed.
			// Go straight back to firing without re-notifying: the alert
			// never actually resolved, and sending a second "fired" for
			// the same ongoing condition is exactly the flapping this
			// design prevents.
			s.state = StateFiring
		}

		return Result{id, s.state, TransitionNone, s.firstBreach, s.breaching}
	}

	// Not breaching.
	s.breaching = 0
	s.clear++

	switch s.state {
	case StatePending:
		// Never fired, so nothing to resolve. This is the case hysteresis
		// exists for: the false positive that dies quietly.
		s.state = StateOK
		s.clear = 0

	case StateFiring:
		if s.clear >= h.cfg.clearCount() {
			s.state = StateOK
			first := s.firstBreach
			s.clear = 0
			return Result{id, StateOK, TransitionResolved, first, 0}
		}
		s.state = StateResolving

	case StateResolving:
		if s.clear >= h.cfg.clearCount() {
			s.state = StateOK
			first := s.firstBreach
			s.clear = 0
			return Result{id, StateOK, TransitionResolved, first, 0}
		}
	}

	return Result{id, s.state, TransitionNone, s.firstBreach, s.clear}
}

// State returns the current state for a series.
func (h *Hysteresis) State(id model.SeriesID) State {
	if s, ok := h.state[id]; ok {
		return s.state
	}
	return StateOK
}

// Firing lists every currently firing series.
func (h *Hysteresis) Firing() []model.SeriesID {
	var out []model.SeriesID
	for id, s := range h.state {
		if s.state == StateFiring || s.state == StateResolving {
			out = append(out, id)
		}
	}
	return out
}

// Forget drops state for a series, e.g. when it stops being collected.
//
// Without this the detector accumulates one map entry per series ever seen,
// which for a fleet with any churn is an unbounded leak in the one process
// that must stay up during an incident.
func (h *Hysteresis) Forget(id model.SeriesID) { delete(h.state, id) }

// Len is the number of series being tracked.
func (h *Hysteresis) Len() int { return len(h.state) }
