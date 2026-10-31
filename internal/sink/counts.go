package sink

import (
	"cmp"
	"slices"
	"sync"

	"github.com/Midwayne/kafka-mcp/internal/record"
)

// Group keys used when no expression applies or the group cap is reached.
const (
	GroupAll   = "(all)"
	GroupError = "(error)"
)

// KeyFunc computes a grouping key, e.g. a compiled CEL expression.
type KeyFunc func(m *record.Message) (string, error)

// Group is one aggregated key.
type Group struct {
	Key   string `json:"key"`
	Count int64  `json:"count"`
}

// CountResult summarizes grouped counts.
type CountResult struct {
	Total          int64            `json:"total"`
	DistinctGroups int              `json:"distinct_groups"`
	Groups         []Group          `json:"groups"`
	OtherGroups    int64            `json:"other_groups_count,omitempty"`
	Samples        []map[string]any `json:"samples,omitempty"`
}

// Counts aggregates matches by key. It never fills up, so the scan reads the
// whole range (subject to its budgets).
type Counts struct {
	key       KeyFunc
	maxGroups int
	samples   *Matches

	mu     sync.Mutex
	total  int64
	groups map[string]int64
	other  int64
}

// NewCounts creates a sink. key may be nil to count everything as one group.
// Up to sampleLimit matches are rendered as examples.
func NewCounts(key KeyFunc, maxGroups, sampleLimit int, renderer record.Renderer) *Counts {
	return &Counts{
		key:       key,
		maxGroups: maxGroups,
		samples:   NewMatches(sampleLimit, renderer, nil),
		groups:    map[string]int64{},
	}
}

// Offer implements scan.Sink. Counts always accepts and never fills up.
func (c *Counts) Offer(message *record.Message) (accepted, more bool) {
	group := GroupAll
	if c.key != nil {
		key, err := c.key(message)
		if err != nil {
			key = GroupError
		}
		group = key
	}

	c.samples.Offer(message)

	c.mu.Lock()
	defer c.mu.Unlock()

	c.total++

	if _, seen := c.groups[group]; seen || len(c.groups) < c.maxGroups {
		c.groups[group]++
	} else {
		c.other++
	}

	return true, true
}

// Result returns the top groups by count, ties broken by key.
func (c *Counts) Result(top int) CountResult {
	c.mu.Lock()
	groups := make([]Group, 0, len(c.groups))
	for key, count := range c.groups {
		groups = append(groups, Group{Key: key, Count: count})
	}
	result := CountResult{Total: c.total, DistinctGroups: len(c.groups), OtherGroups: c.other}
	c.mu.Unlock()

	slices.SortFunc(groups, func(a, b Group) int {
		return cmp.Or(cmp.Compare(b.Count, a.Count), cmp.Compare(a.Key, b.Key))
	})

	if top > 0 && len(groups) > top {
		for _, dropped := range groups[top:] {
			result.OtherGroups += dropped.Count
		}
		groups = groups[:top]
	}

	result.Groups = groups
	result.Samples = c.samples.Items()

	return result
}
