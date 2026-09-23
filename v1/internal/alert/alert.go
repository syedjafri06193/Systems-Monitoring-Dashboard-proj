// Package alert is the lifecycle after detection.
//
// Detection is one stage of five, and skipping the last three is why
// homegrown alerting is unbearable:
//
//	detect -> hysteresis -> dedup -> group -> inhibit -> notify
//
// Grouping is what turns a rack failure from 200 pages into one.
// Inhibition is what stops "the datacenter is unreachable" from also
// paging about every service inside it. Both are Alertmanager's model,
// which is the right one.
package alert

import (
	"sort"
	"strings"
	"time"

	"github.com/syedjafri06193/Systems-Monitoring-Dashboard-proj/internal/model"
)

// Status is firing or resolved.
type Status uint8

const (
	StatusFiring Status = iota
	StatusResolved
)

func (s Status) String() string {
	if s == StatusResolved {
		return "resolved"
	}
	return "firing"
}

// Alert is one condition on one series.
type Alert struct {
	// Fingerprint identifies the alert across evaluations. Same series +
	// same condition must produce the same fingerprint, or dedup cannot
	// work.
	Fingerprint string
	Name        string
	Series      model.SeriesID
	Labels      model.Labels
	Status      Status
	// StartsAt is when the condition began, not when it fired.
	StartsAt time.Time
	EndsAt   time.Time
	Value    float64
	Z        float64
	// Severity drives inhibition and routing.
	Severity string
}

// Fingerprint builds a stable identity from the alert name and labels.
func Fingerprint(name string, l model.Labels) string {
	return name + "\x1e" + l.Key()
}

// --- dedup -----------------------------------------------------------------

// Deduper suppresses repeats of an alert that is already known to be firing.
//
// The hysteresis layer already fires once per condition, so this is a
// second line of defence for the cases it cannot see: a detector restart,
// two detectors evaluating the same series during a rolling deploy, or a
// re-evaluation triggered by a config reload. Without it, any of those
// re-pages everyone at 3 a.m. for a condition they already acknowledged.
type Deduper struct {
	// active maps fingerprint -> when it was last notified.
	active map[string]time.Time
	// repeatAfter re-sends a still-firing alert periodically, so a
	// long-running incident does not scroll out of a channel and get
	// forgotten. Zero disables the repeat.
	repeatAfter time.Duration
}

func NewDeduper(repeatAfter time.Duration) *Deduper {
	return &Deduper{active: make(map[string]time.Time), repeatAfter: repeatAfter}
}

// Admit reports whether an alert should be notified now.
func (d *Deduper) Admit(a Alert, now time.Time) bool {
	last, known := d.active[a.Fingerprint]

	if a.Status == StatusResolved {
		if !known {
			// A resolution for something never notified as firing would be
			// confusing noise: "RESOLVED: thing you never heard about".
			return false
		}
		delete(d.active, a.Fingerprint)
		return true
	}

	if !known {
		d.active[a.Fingerprint] = now
		return true
	}
	if d.repeatAfter > 0 && now.Sub(last) >= d.repeatAfter {
		d.active[a.Fingerprint] = now
		return true
	}
	return false
}

// ActiveCount is for meta-monitoring.
func (d *Deduper) ActiveCount() int { return len(d.active) }

// Forget drops an alert's dedup state.
func (d *Deduper) Forget(fp string) { delete(d.active, fp) }

// --- grouping --------------------------------------------------------------

// Group is a set of alerts that share a root cause.
type Group struct {
	// Key is the joined values of the grouping labels.
	Key    string
	Labels map[string]string
	Alerts []Alert
}

// Title is a one-line summary for a notification.
func (g Group) Title() string {
	if len(g.Alerts) == 1 {
		return g.Alerts[0].Name
	}
	return g.Alerts[0].Name
}

// Grouper batches alerts by a configurable label set.
//
// This is what turns a rack failure from 200 pages into one. The wait
// window matters: related alerts do not arrive simultaneously, because
// each series crosses its own threshold at its own moment, so grouping
// without a wait produces one notification per alert anyway.
type Grouper struct {
	// By is the label set to group on, e.g. {cluster, service, alertname}.
	// An empty By groups everything together, which is occasionally what
	// you want during a total outage and almost never otherwise.
	By []string
}

// Group partitions alerts.
//
// Deterministic ordering throughout: groups sorted by key, alerts within a
// group sorted by fingerprint. A notification whose contents reorder
// between runs is much harder to diff during an incident.
func (g *Grouper) Group(alerts []Alert) []Group {
	buckets := make(map[string]*Group)

	for _, a := range alerts {
		labels := make(map[string]string, len(g.By))
		parts := make([]string, 0, len(g.By))
		for _, k := range g.By {
			v := lookupLabel(a, k)
			labels[k] = v
			parts = append(parts, k+"="+v)
		}
		key := strings.Join(parts, ",")

		b, ok := buckets[key]
		if !ok {
			b = &Group{Key: key, Labels: labels}
			buckets[key] = b
		}
		b.Alerts = append(b.Alerts, a)
	}

	out := make([]Group, 0, len(buckets))
	for _, b := range buckets {
		sort.Slice(b.Alerts, func(i, j int) bool {
			return b.Alerts[i].Fingerprint < b.Alerts[j].Fingerprint
		})
		out = append(out, *b)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out
}

// lookupLabel resolves a grouping key, treating alertname specially.
func lookupLabel(a Alert, key string) string {
	if key == "alertname" {
		return a.Name
	}
	if key == "severity" {
		return a.Severity
	}
	if a.Labels.Pairs != nil {
		if v, ok := a.Labels.Pairs[key]; ok {
			return v
		}
	}
	if key == "__name__" {
		return a.Labels.Name
	}
	return ""
}

// --- inhibition ------------------------------------------------------------

// InhibitRule suppresses children of a firing parent.
//
// "The whole datacenter is unreachable" should not also page about every
// service in it. The equality constraint is what scopes the suppression:
// without it, a node-down alert in one cluster would silence service
// alerts everywhere.
type InhibitRule struct {
	// SourceMatch selects the parent alert.
	SourceMatch map[string]string
	// TargetMatch selects candidates for suppression.
	TargetMatch map[string]string
	// Equal lists labels that must match between source and target.
	Equal []string
}

// Inhibitor applies inhibition rules.
type Inhibitor struct {
	Rules []InhibitRule
}

// Apply returns the alerts that survive inhibition.
//
// Only *firing* alerts inhibit. A resolved parent must not keep silencing
// its children, or clearing the root cause makes every downstream problem
// invisible at exactly the moment someone is checking whether the fix
// worked.
func (in *Inhibitor) Apply(alerts []Alert) []Alert {
	if len(in.Rules) == 0 {
		return alerts
	}

	sources := make([]Alert, 0, len(alerts))
	for _, a := range alerts {
		if a.Status == StatusFiring {
			sources = append(sources, a)
		}
	}

	out := make([]Alert, 0, len(alerts))
	for _, a := range alerts {
		if !in.inhibited(a, sources) {
			out = append(out, a)
		}
	}
	return out
}

func (in *Inhibitor) inhibited(target Alert, sources []Alert) bool {
	for _, rule := range in.Rules {
		if !matches(target, rule.TargetMatch) {
			continue
		}
		for _, src := range sources {
			if src.Fingerprint == target.Fingerprint {
				// An alert must never inhibit itself, which it would
				// whenever a rule's source and target matchers overlap —
				// and a rule like "critical inhibits warning on the same
				// host" overlaps for any alert that is both.
				continue
			}
			if !matches(src, rule.SourceMatch) {
				continue
			}
			if equalLabels(src, target, rule.Equal) {
				return true
			}
		}
	}
	return false
}

func matches(a Alert, m map[string]string) bool {
	for k, want := range m {
		if lookupLabel(a, k) != want {
			return false
		}
	}
	return true
}

func equalLabels(a, b Alert, keys []string) bool {
	for _, k := range keys {
		av, bv := lookupLabel(a, k), lookupLabel(b, k)
		// An empty value on both sides is not a match. Otherwise a rule
		// with Equal: ["cluster"] would inhibit across every alert that
		// happens to lack a cluster label — the widest possible
		// suppression, produced by a missing label rather than a decision.
		if av == "" || av != bv {
			return false
		}
	}
	return true
}

// --- the pipeline ----------------------------------------------------------

// Pipeline wires dedup, grouping and inhibition together.
type Pipeline struct {
	Deduper   *Deduper
	Grouper   *Grouper
	Inhibitor *Inhibitor
}

// Notification is what reaches Slack, PagerDuty or Alertmanager.
type Notification struct {
	Group   Group
	Status  Status
	Summary string
}

// Process runs the full lifecycle for one evaluation's alerts.
//
// Order matters and is not arbitrary: inhibit before dedup, so that a
// suppressed alert does not consume its dedup slot and thereby stay silent
// after the parent resolves.
func (p *Pipeline) Process(alerts []Alert, now time.Time) []Notification {
	surviving := alerts
	if p.Inhibitor != nil {
		surviving = p.Inhibitor.Apply(surviving)
	}

	admitted := make([]Alert, 0, len(surviving))
	for _, a := range surviving {
		if p.Deduper == nil || p.Deduper.Admit(a, now) {
			admitted = append(admitted, a)
		}
	}
	if len(admitted) == 0 {
		return nil
	}

	var groups []Group
	if p.Grouper != nil {
		groups = p.Grouper.Group(admitted)
	} else {
		groups = []Group{{Key: "", Alerts: admitted}}
	}

	out := make([]Notification, 0, len(groups))
	for _, g := range groups {
		out = append(out, Notification{
			Group:   g,
			Status:  groupStatus(g),
			Summary: summarize(g),
		})
	}
	return out
}

func groupStatus(g Group) Status {
	for _, a := range g.Alerts {
		if a.Status == StatusFiring {
			return StatusFiring
		}
	}
	return StatusResolved
}

func summarize(g Group) string {
	if len(g.Alerts) == 1 {
		a := g.Alerts[0]
		return a.Status.String() + ": " + a.Name
	}
	return g.Alerts[0].Status.String() + ": " + g.Alerts[0].Name +
		" (" + itoa(len(g.Alerts)) + " series)"
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}
