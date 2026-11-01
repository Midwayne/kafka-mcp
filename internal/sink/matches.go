// Package sink collects scan matches: bounded inline results, JSON Lines
// exports, and grouped counts.
package sink

import (
	"cmp"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/Midwayne/kafka-mcp/internal/record"
)

type item struct {
	ts        int64
	topic     string
	partition int32
	offset    int64
	rendered  map[string]any
}

// Matches keeps the first limit rendered matches. With an export attached it
// also streams every match to a file and stays open until the export is full.
type Matches struct {
	limit    int
	renderer record.Renderer
	export   *Export

	mu    sync.Mutex
	items []item
	full  atomic.Bool // Once full, sampling stops without contending on mu.
}

// NewMatches creates a sink. export may be nil.
func NewMatches(limit int, renderer record.Renderer, export *Export) *Matches {
	matches := &Matches{limit: limit, renderer: renderer, export: export}
	matches.full.Store(limit <= 0)

	return matches
}

// Offer implements scan.Sink. With an export, the export decides acceptance
// and capacity; inline results are just its first few entries.
func (m *Matches) Offer(message *record.Message) (accepted, more bool) {
	stored, room := m.store(message)

	if m.export != nil {
		return m.export.Write(message)
	}

	return stored, room
}

// store keeps the message if there is room and reports whether room remains.
func (m *Matches) store(message *record.Message) (stored, room bool) {
	if m.full.Load() {
		return false, false
	}

	rendered := m.renderer.Render(message)

	m.mu.Lock()
	defer m.mu.Unlock()

	if len(m.items) >= m.limit {
		return false, false
	}

	r := message.Record
	m.items = append(m.items, item{
		ts: r.Timestamp.UnixMilli(), topic: r.Topic, partition: r.Partition, offset: r.Offset, rendered: rendered,
	})
	room = len(m.items) < m.limit
	if !room {
		m.full.Store(true)
	}

	return true, room
}

// Items returns stored matches ordered by timestamp, then topic, partition, and offset.
func (m *Matches) Items() []map[string]any {
	m.mu.Lock()
	items := slices.Clone(m.items)
	m.mu.Unlock()

	slices.SortFunc(items, func(a, b item) int {
		return cmp.Or(
			cmp.Compare(a.ts, b.ts),
			cmp.Compare(a.topic, b.topic),
			cmp.Compare(a.partition, b.partition),
			cmp.Compare(a.offset, b.offset),
		)
	})

	out := make([]map[string]any, 0, len(items))
	for _, it := range items {
		out = append(out, it.rendered)
	}

	return out
}
