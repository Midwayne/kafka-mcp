package scan

import (
	"strconv"
	"sync"

	"github.com/twmb/franz-go/pkg/kgo"
)

type partitionKey struct {
	topic     string
	partition int32
}

// partitionState tracks the read position of one range. dropped is the lowest
// offset that was read but not delivered (e.g. after the sink filled up), so
// resuming never skips a record.
type partitionState struct {
	end     int64
	next    int64
	dropped int64
	done    bool
}

type tracker struct {
	mu     sync.Mutex
	states map[partitionKey]*partitionState
	open   int
}

func newTracker(ranges []Range) *tracker {
	t := &tracker{states: make(map[partitionKey]*partitionState, len(ranges))}

	for _, rng := range ranges {
		t.states[partitionKey{rng.Topic, rng.Partition}] = &partitionState{end: rng.End, next: rng.Start, dropped: -1}
		t.open++
	}

	return t
}

// clip drops records at or beyond the range end and reports whether the fetch reached it.
func (t *tracker) clip(key partitionKey, records []*kgo.Record) ([]*kgo.Record, bool) {
	t.mu.Lock()
	state := t.states[key]
	t.mu.Unlock()

	if state == nil || len(records) == 0 || state.end == OpenEnd {
		return records, false
	}

	for i, r := range records {
		if r.Offset >= state.end {
			return records[:i], true
		}
	}

	return records, records[len(records)-1].Offset >= state.end-1
}

// advance moves the position past records and returns true when the range is complete.
func (t *tracker) advance(key partitionKey, records []*kgo.Record, complete bool) bool {
	t.mu.Lock()
	defer t.mu.Unlock()

	state := t.states[key]
	if state == nil || state.done {
		return false
	}

	if len(records) > 0 {
		state.next = records[len(records)-1].Offset + 1
	}

	if complete {
		state.next, state.done = state.end, true
		t.open--
	}

	return complete
}

func (t *tracker) noteDropped(key partitionKey, offset int64) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if state := t.states[key]; state != nil && (state.dropped < 0 || offset < state.dropped) {
		state.dropped = offset
	}
}

func (t *tracker) allDone() bool {
	t.mu.Lock()
	defer t.mu.Unlock()

	return t.open == 0
}

func (t *tracker) counts() (total, done int) {
	t.mu.Lock()
	defer t.mu.Unlock()

	return len(t.states), len(t.states) - t.open
}

// nextOffsets returns resume positions for partitions with unread or undelivered records.
func (t *tracker) nextOffsets() map[string]map[string]int64 {
	t.mu.Lock()
	defer t.mu.Unlock()

	out := map[string]map[string]int64{}

	for key, state := range t.states {
		next := state.next
		if state.dropped >= 0 && state.dropped < next {
			next = state.dropped
		}

		if state.done && state.dropped < 0 {
			continue
		}

		if out[key.topic] == nil {
			out[key.topic] = map[string]int64{}
		}
		out[key.topic][strconv.Itoa(int(key.partition))] = next
	}

	if len(out) == 0 {
		return nil
	}

	return out
}

func (t *tracker) assignments() map[string]map[int32]kgo.Offset {
	t.mu.Lock()
	defer t.mu.Unlock()

	out := map[string]map[int32]kgo.Offset{}

	for key, state := range t.states {
		if out[key.topic] == nil {
			out[key.topic] = map[int32]kgo.Offset{}
		}
		out[key.topic][key.partition] = kgo.NewOffset().At(state.next)
	}

	return out
}
