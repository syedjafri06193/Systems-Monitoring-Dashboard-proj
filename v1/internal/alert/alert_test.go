package alert

import (
	"testing"
	"time"

	"github.com/syedjafri06193/Systems-Monitoring-Dashboard-proj/internal/model"
)

var now = time.Unix(1_700_000_000, 0)

func mk(name string, sev string, pairs map[string]string) Alert {
	l := model.Labels{Name: "metric", Pairs: pairs}
	return Alert{
		Fingerprint: Fingerprint(name, l),
		Name:        name,
		Labels:      l,
		Status:      StatusFiring,
		Severity:    sev,
		StartsAt:    now,
	}
}

// --- fingerprinting --------------------------------------------------------

func TestFingerprintIsStable(t *testing.T) {
	// Same series plus same condition must produce the same fingerprint,
	// or dedup cannot work at all.
	a := mk("HighCPU", "warning", map[string]string{"host": "a", "dc": "eu"})
	b := mk("HighCPU", "warning", map[string]string{"dc": "eu", "host": "a"})
	if a.Fingerprint != b.Fingerprint {
		t.Fatal("label order changed the fingerprint")
	}
}

func TestFingerprintDistinguishesAlertName(t *testing.T) {
	// Two conditions on the same series are two alerts.
	pairs := map[string]string{"host": "a"}
	if mk("HighCPU", "warning", pairs).Fingerprint == mk("HighMemory", "warning", pairs).Fingerprint {
		t.Fatal("different alert names collided")
	}
}

// --- dedup -----------------------------------------------------------------

func TestDedupeSuppressesRepeats(t *testing.T) {
	// The hysteresis layer fires once, but a detector restart or two
	// detectors during a rolling deploy would otherwise re-page everyone
	// for a condition already acknowledged.
	d := NewDeduper(0)
	a := mk("HighCPU", "warning", map[string]string{"host": "a"})

	if !d.Admit(a, now) {
		t.Fatal("first notification suppressed")
	}
	for i := 1; i <= 100; i++ {
		if d.Admit(a, now.Add(time.Duration(i)*time.Second)) {
			t.Fatalf("repeat %d admitted", i)
		}
	}
}

func TestDedupeAdmitsResolution(t *testing.T) {
	d := NewDeduper(0)
	a := mk("HighCPU", "warning", map[string]string{"host": "a"})
	d.Admit(a, now)

	a.Status = StatusResolved
	if !d.Admit(a, now.Add(time.Minute)) {
		t.Fatal("resolution suppressed")
	}
	if d.ActiveCount() != 0 {
		t.Fatalf("active = %d after resolution", d.ActiveCount())
	}
}

func TestDedupeSuppressesResolutionForSomethingNeverFired(t *testing.T) {
	// "RESOLVED: thing you never heard about" is confusing noise.
	d := NewDeduper(0)
	a := mk("HighCPU", "warning", map[string]string{"host": "a"})
	a.Status = StatusResolved
	if d.Admit(a, now) {
		t.Fatal("admitted a resolution for an alert that never fired")
	}
}

func TestDedupeRefiresAfterResolution(t *testing.T) {
	d := NewDeduper(0)
	a := mk("HighCPU", "warning", map[string]string{"host": "a"})
	d.Admit(a, now)

	resolved := a
	resolved.Status = StatusResolved
	d.Admit(resolved, now.Add(time.Minute))

	if !d.Admit(a, now.Add(2*time.Minute)) {
		t.Fatal("a genuinely new firing was suppressed after resolution")
	}
}

func TestDedupeRepeatsAfterInterval(t *testing.T) {
	// A long-running incident should not scroll out of a channel and be
	// forgotten.
	d := NewDeduper(4 * time.Hour)
	a := mk("HighCPU", "warning", map[string]string{"host": "a"})
	d.Admit(a, now)

	if d.Admit(a, now.Add(time.Hour)) {
		t.Fatal("repeated before the interval")
	}
	if !d.Admit(a, now.Add(5*time.Hour)) {
		t.Fatal("did not repeat after the interval")
	}
}

// --- grouping --------------------------------------------------------------

func TestGroupingTurnsARackFailureIntoOneNotification(t *testing.T) {
	// The M10 acceptance criterion: a simulated rack failure across 200
	// series produces one notification, not 200.
	g := &Grouper{By: []string{"alertname", "rack"}}

	var alerts []Alert
	for i := 0; i < 200; i++ {
		alerts = append(alerts, mk("NodeDown", "critical", map[string]string{
			"rack": "r7",
			"host": "node-" + itoa(i),
		}))
	}

	groups := g.Group(alerts)
	if len(groups) != 1 {
		t.Fatalf("200 alerts from one rack produced %d groups", len(groups))
	}
	if len(groups[0].Alerts) != 200 {
		t.Fatalf("group holds %d alerts", len(groups[0].Alerts))
	}
}

func TestGroupingSeparatesDistinctCauses(t *testing.T) {
	g := &Grouper{By: []string{"alertname", "rack"}}
	alerts := []Alert{
		mk("NodeDown", "critical", map[string]string{"rack": "r7", "host": "a"}),
		mk("NodeDown", "critical", map[string]string{"rack": "r8", "host": "b"}),
		mk("HighCPU", "warning", map[string]string{"rack": "r7", "host": "c"}),
	}
	if got := len(g.Group(alerts)); got != 3 {
		t.Fatalf("got %d groups, want 3", got)
	}
}

func TestGroupingIsDeterministic(t *testing.T) {
	// A notification whose contents reorder between runs is much harder to
	// diff during an incident.
	g := &Grouper{By: []string{"rack"}}
	alerts := []Alert{
		mk("A", "warning", map[string]string{"rack": "r2", "host": "z"}),
		mk("A", "warning", map[string]string{"rack": "r1", "host": "y"}),
		mk("A", "warning", map[string]string{"rack": "r1", "host": "x"}),
	}

	first := g.Group(alerts)
	for i := 0; i < 20; i++ {
		got := g.Group(alerts)
		if len(got) != len(first) {
			t.Fatal("group count varied")
		}
		for j := range got {
			if got[j].Key != first[j].Key {
				t.Fatalf("group order varied: %q vs %q", got[j].Key, first[j].Key)
			}
			for k := range got[j].Alerts {
				if got[j].Alerts[k].Fingerprint != first[j].Alerts[k].Fingerprint {
					t.Fatal("alert order within a group varied")
				}
			}
		}
	}
}

func TestGroupingByNothingCollapsesEverything(t *testing.T) {
	g := &Grouper{}
	alerts := []Alert{
		mk("A", "warning", map[string]string{"host": "a"}),
		mk("B", "critical", map[string]string{"host": "b"}),
	}
	if got := len(g.Group(alerts)); got != 1 {
		t.Fatalf("got %d groups", got)
	}
}

func TestGroupingHandlesAMissingLabel(t *testing.T) {
	g := &Grouper{By: []string{"rack"}}
	alerts := []Alert{
		mk("A", "warning", map[string]string{"host": "a"}), // no rack
		mk("A", "warning", map[string]string{"rack": "r1"}),
	}
	if got := len(g.Group(alerts)); got != 2 {
		t.Fatalf("got %d groups", got)
	}
}

// --- inhibition ------------------------------------------------------------

func TestInhibitionSuppressesChildrenOfAFiringParent(t *testing.T) {
	// "The datacenter is unreachable" must not also page about every
	// service inside it.
	in := &Inhibitor{Rules: []InhibitRule{{
		SourceMatch: map[string]string{"alertname": "DatacenterDown"},
		TargetMatch: map[string]string{"severity": "warning"},
		Equal:       []string{"dc"},
	}}}

	parent := mk("DatacenterDown", "critical", map[string]string{"dc": "eu"})
	child := mk("ServiceDegraded", "warning", map[string]string{"dc": "eu", "svc": "api"})
	elsewhere := mk("ServiceDegraded", "warning", map[string]string{"dc": "us", "svc": "api"})

	got := in.Apply([]Alert{parent, child, elsewhere})

	if len(got) != 2 {
		t.Fatalf("got %d alerts, want 2 (parent + the one in another dc)", len(got))
	}
	for _, a := range got {
		if a.Fingerprint == child.Fingerprint {
			t.Fatal("the child in the failing dc was not inhibited")
		}
	}
}

func TestInhibitionIsScopedByEqualLabels(t *testing.T) {
	// Without the equality constraint, a node-down alert in one cluster
	// would silence service alerts everywhere.
	in := &Inhibitor{Rules: []InhibitRule{{
		SourceMatch: map[string]string{"alertname": "NodeDown"},
		TargetMatch: map[string]string{"alertname": "HighLatency"},
		Equal:       []string{"host"},
	}}}

	parent := mk("NodeDown", "critical", map[string]string{"host": "a"})
	sameHost := mk("HighLatency", "warning", map[string]string{"host": "a"})
	otherHost := mk("HighLatency", "warning", map[string]string{"host": "b"})

	got := in.Apply([]Alert{parent, sameHost, otherHost})
	if len(got) != 2 {
		t.Fatalf("got %d, want 2", len(got))
	}
}

func TestMissingEqualLabelDoesNotInhibitEverything(t *testing.T) {
	// A rule with Equal: ["cluster"] must not inhibit across every alert
	// that happens to lack a cluster label -- the widest possible
	// suppression, produced by a missing label rather than a decision.
	in := &Inhibitor{Rules: []InhibitRule{{
		SourceMatch: map[string]string{"alertname": "Parent"},
		TargetMatch: map[string]string{"alertname": "Child"},
		Equal:       []string{"cluster"},
	}}}

	parent := mk("Parent", "critical", map[string]string{"host": "a"}) // no cluster
	child := mk("Child", "warning", map[string]string{"host": "b"})    // no cluster

	got := in.Apply([]Alert{parent, child})
	if len(got) != 2 {
		t.Fatal("a missing label on both sides caused inhibition")
	}
}

func TestResolvedParentStopsInhibiting(t *testing.T) {
	// Otherwise clearing the root cause makes every downstream problem
	// invisible at exactly the moment someone checks whether the fix
	// worked.
	in := &Inhibitor{Rules: []InhibitRule{{
		SourceMatch: map[string]string{"alertname": "Parent"},
		TargetMatch: map[string]string{"alertname": "Child"},
		Equal:       []string{"dc"},
	}}}

	parent := mk("Parent", "critical", map[string]string{"dc": "eu"})
	parent.Status = StatusResolved
	child := mk("Child", "warning", map[string]string{"dc": "eu"})

	got := in.Apply([]Alert{parent, child})
	if len(got) != 2 {
		t.Fatal("a resolved parent kept inhibiting its child")
	}
}

func TestAlertDoesNotInhibitItself(t *testing.T) {
	// A rule like "critical inhibits warning on the same host" overlaps
	// for any alert that matches both matchers.
	in := &Inhibitor{Rules: []InhibitRule{{
		SourceMatch: map[string]string{"severity": "critical"},
		TargetMatch: map[string]string{"severity": "critical"},
		Equal:       []string{"host"},
	}}}

	a := mk("X", "critical", map[string]string{"host": "a"})
	got := in.Apply([]Alert{a})
	if len(got) != 1 {
		t.Fatal("an alert inhibited itself")
	}
}

func TestNoRulesIsAPassThrough(t *testing.T) {
	in := &Inhibitor{}
	alerts := []Alert{mk("A", "warning", nil), mk("B", "critical", nil)}
	if got := in.Apply(alerts); len(got) != 2 {
		t.Fatalf("got %d", len(got))
	}
}

// --- the pipeline ----------------------------------------------------------

func TestPipelineTurnsARackFailureIntoOneNotification(t *testing.T) {
	p := &Pipeline{
		Deduper:   NewDeduper(0),
		Grouper:   &Grouper{By: []string{"alertname", "rack"}},
		Inhibitor: &Inhibitor{},
	}

	var alerts []Alert
	for i := 0; i < 200; i++ {
		alerts = append(alerts, mk("NodeDown", "critical", map[string]string{
			"rack": "r7", "host": "node-" + itoa(i),
		}))
	}

	notifications := p.Process(alerts, now)
	if len(notifications) != 1 {
		t.Fatalf("200 alerts produced %d notifications", len(notifications))
	}
	if notifications[0].Summary != "firing: NodeDown (200 series)" {
		t.Fatalf("summary = %q", notifications[0].Summary)
	}
}

func TestPipelineIsQuietOnRepeatEvaluations(t *testing.T) {
	// The detector evaluates continuously; the pipeline must not notify
	// continuously.
	p := &Pipeline{
		Deduper: NewDeduper(0),
		Grouper: &Grouper{By: []string{"alertname"}},
	}
	alerts := []Alert{mk("HighCPU", "warning", map[string]string{"host": "a"})}

	if got := p.Process(alerts, now); len(got) != 1 {
		t.Fatalf("first evaluation produced %d notifications", len(got))
	}
	for i := 1; i <= 50; i++ {
		if got := p.Process(alerts, now.Add(time.Duration(i)*time.Second)); len(got) != 0 {
			t.Fatalf("evaluation %d produced a notification", i)
		}
	}
}

func TestPipelineInhibitsBeforeDeduping(t *testing.T) {
	// Order matters: a suppressed alert must not consume its dedup slot,
	// or it stays silent after the parent resolves.
	p := &Pipeline{
		Deduper: NewDeduper(0),
		Grouper: &Grouper{By: []string{"alertname"}},
		Inhibitor: &Inhibitor{Rules: []InhibitRule{{
			SourceMatch: map[string]string{"alertname": "Parent"},
			TargetMatch: map[string]string{"alertname": "Child"},
			Equal:       []string{"dc"},
		}}},
	}

	parent := mk("Parent", "critical", map[string]string{"dc": "eu"})
	child := mk("Child", "warning", map[string]string{"dc": "eu"})

	// While the parent fires, the child is suppressed.
	got := p.Process([]Alert{parent, child}, now)
	if len(got) != 1 || got[0].Group.Alerts[0].Name != "Parent" {
		t.Fatalf("expected only the parent, got %+v", got)
	}

	// Parent resolves. The child must now be notifiable -- it never
	// consumed a dedup slot.
	parent.Status = StatusResolved
	got = p.Process([]Alert{parent, child}, now.Add(time.Minute))

	names := map[string]bool{}
	for _, n := range got {
		for _, a := range n.Group.Alerts {
			names[a.Name] = true
		}
	}
	if !names["Child"] {
		t.Fatal("the child stayed silent after its parent resolved")
	}
}

func TestPipelineEmitsResolutions(t *testing.T) {
	// Every alert needs a resolution path. An alert that fires and never
	// resolves trains people to ignore alerts.
	p := &Pipeline{Deduper: NewDeduper(0), Grouper: &Grouper{By: []string{"alertname"}}}
	a := mk("HighCPU", "warning", map[string]string{"host": "a"})

	p.Process([]Alert{a}, now)

	a.Status = StatusResolved
	got := p.Process([]Alert{a}, now.Add(time.Minute))
	if len(got) != 1 {
		t.Fatalf("got %d notifications", len(got))
	}
	if got[0].Status != StatusResolved {
		t.Fatalf("status = %v", got[0].Status)
	}
	if got[0].Summary != "resolved: HighCPU" {
		t.Fatalf("summary = %q", got[0].Summary)
	}
}

func TestPipelineWithNothingToSay(t *testing.T) {
	p := &Pipeline{Deduper: NewDeduper(0), Grouper: &Grouper{}}
	if got := p.Process(nil, now); got != nil {
		t.Fatalf("got %v", got)
	}
}

func TestStatusStrings(t *testing.T) {
	if StatusFiring.String() != "firing" || StatusResolved.String() != "resolved" {
		t.Fatal("status strings")
	}
}
