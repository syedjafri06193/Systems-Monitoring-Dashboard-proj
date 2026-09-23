package wire

import (
	"sync"

	"github.com/syedjafri06193/Systems-Monitoring-Dashboard-proj/internal/model"
)

// Registry interns label sets into uint32 ids.
//
// This is the single biggest win over JSON. A label set like
// {__name__="node_cpu_seconds_total", instance="node-42", cpu="7",
// mode="user"} is ~80 bytes; sent on every sample at 5 Hz that is 400 B/s
// per series before any values. Interning sends it once.
//
// Ids are assigned densely from 1 so they can index a slice on the client
// without a map lookup in the decode hot path. Zero is reserved as "no
// series", which makes a zero-valued struct detectably invalid rather than
// silently referring to the first series.
type Registry struct {
	mu    sync.RWMutex
	byKey map[string]model.SeriesID
	byID  []model.Labels // index 0 unused
	dirty []model.SeriesID
}

func NewRegistry() *Registry {
	return &Registry{
		byKey: make(map[string]model.SeriesID),
		byID:  make([]model.Labels, 1), // reserve id 0
	}
}

// Intern returns the id for a label set, assigning one if new.
//
// The second return reports whether the id was newly created, which is what
// tells the gateway a SERIES_REGISTRY frame must be sent before any point
// referencing it. Sending the point first would leave the client with an id
// it cannot name.
func (r *Registry) Intern(l model.Labels) (model.SeriesID, bool) {
	key := l.Key()

	r.mu.RLock()
	id, ok := r.byKey[key]
	r.mu.RUnlock()
	if ok {
		return id, false
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	// Re-check under the write lock: two goroutines can both miss the read
	// lookup for the same new series, and without this they would assign
	// two ids to one label set — a silent doubling of the series count.
	if id, ok := r.byKey[key]; ok {
		return id, false
	}

	id = model.SeriesID(len(r.byID))
	r.byID = append(r.byID, l.Clone())
	r.byKey[key] = id
	r.dirty = append(r.dirty, id)
	return id, true
}

// Lookup returns the labels for an id.
func (r *Registry) Lookup(id model.SeriesID) (model.Labels, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if int(id) <= 0 || int(id) >= len(r.byID) {
		return model.Labels{}, false
	}
	return r.byID[id], true
}

// ID returns the id for a label set without assigning one.
func (r *Registry) ID(l model.Labels) (model.SeriesID, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	id, ok := r.byKey[l.Key()]
	return id, ok
}

// Len is the number of interned series.
func (r *Registry) Len() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.byID) - 1
}

// TakeDirty returns series interned since the last call and clears the list.
//
// A per-connection registry would be simpler, but every connection would
// then re-intern the same series and hold its own copy of every label set.
// One shared registry plus a per-connection "what have I already sent"
// watermark is the same correctness with a fraction of the memory.
func (r *Registry) TakeDirty() []SeriesDef {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.dirty) == 0 {
		return nil
	}
	defs := make([]SeriesDef, 0, len(r.dirty))
	for _, id := range r.dirty {
		defs = append(defs, SeriesDef{ID: id, Labels: r.byID[id]})
	}
	r.dirty = r.dirty[:0]
	return defs
}

// DefsSince returns every series with an id greater than after.
//
// Ids are assigned monotonically, so "everything I have not sent yet" is a
// range rather than a set difference. This is what a reconnecting client
// needs: it reports the highest id it knows and gets exactly the gap.
func (r *Registry) DefsSince(after model.SeriesID) []SeriesDef {
	r.mu.RLock()
	defer r.mu.RUnlock()

	start := int(after) + 1
	if start < 1 {
		start = 1
	}
	if start >= len(r.byID) {
		return nil
	}
	defs := make([]SeriesDef, 0, len(r.byID)-start)
	for i := start; i < len(r.byID); i++ {
		defs = append(defs, SeriesDef{ID: model.SeriesID(i), Labels: r.byID[i]})
	}
	return defs
}

// MaxID is the highest assigned id.
func (r *Registry) MaxID() model.SeriesID {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return model.SeriesID(len(r.byID) - 1)
}

// ClientRegistry is the browser-side half: id -> labels, no assignment.
//
// Separate from Registry rather than shared, because the client must never
// invent an id. If it receives a point for an unknown id that is a protocol
// error worth reporting, not a series to make up a name for.
type ClientRegistry struct {
	byID map[model.SeriesID]model.Labels
}

func NewClientRegistry() *ClientRegistry {
	return &ClientRegistry{byID: make(map[model.SeriesID]model.Labels)}
}

func (c *ClientRegistry) Apply(defs []SeriesDef) {
	for _, d := range defs {
		c.byID[d.ID] = d.Labels
	}
}

func (c *ClientRegistry) Lookup(id model.SeriesID) (model.Labels, bool) {
	l, ok := c.byID[id]
	return l, ok
}

func (c *ClientRegistry) Len() int { return len(c.byID) }
