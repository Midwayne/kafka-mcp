package scan

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
)

// OpenEnd marks a range that keeps reading new records (follow mode).
const OpenEnd = -1

// OffsetLister is the subset of *kadm.Client the planner needs.
type OffsetLister interface {
	ListStartOffsets(ctx context.Context, topics ...string) (kadm.ListedOffsets, error)
	ListEndOffsets(ctx context.Context, topics ...string) (kadm.ListedOffsets, error)
	ListOffsetsAfterMilli(ctx context.Context, millisecond int64, topics ...string) (kadm.ListedOffsets, error)
}

// Selection chooses which partitions to read.
type Selection struct {
	Topics      []string
	Partitions  []int32     // empty means all partitions
	Key         []byte      // with Partitioner, restricts each topic to the key's partition
	Partitioner Partitioner // nil disables key-based pruning
}

// Bounds chooses where reading starts and stops in each partition.
type Bounds struct {
	From        string                     // time point; default earliest
	To          string                     // time point; default latest (snapshot at plan time)
	StartOffset *int64                     // absolute when >= 0, relative to the end when < 0; overrides From
	EndOffset   *int64                     // exclusive; absolute when >= 0, relative to the end when < 0; overrides To
	Resume      map[string]map[int32]int64 // per-partition start positions from a previous scan's next_offsets
	Follow      bool                       // keep reading new records; ignores To and EndOffset
}

// Range is a half-open offset range [Start, End) in one partition.
type Range struct {
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	Start     int64  `json:"start"`
	End       int64  `json:"end"`
}

// Plan is the set of ranges to read plus an optional record timestamp window.
type Plan struct {
	Ranges []Range
	FromMs int64 // records with earlier timestamps are skipped; 0 disables
	ToMs   int64 // records at or after this timestamp are skipped; 0 disables
}

// BuildPlan resolves time and offset bounds into concrete ranges.
func BuildPlan(ctx context.Context, lister OffsetLister, sel Selection, bounds Bounds, now time.Time) (Plan, error) {
	from, err := ParsePoint(bounds.From, now)
	if err != nil {
		return Plan{}, fmt.Errorf("from: %w", err)
	}

	toRaw := bounds.To
	if toRaw == "" {
		toRaw = "latest"
	}

	to, err := ParsePoint(toRaw, now)
	if err != nil {
		return Plan{}, fmt.Errorf("to: %w", err)
	}

	offsets, err := listOffsets(ctx, lister, sel.Topics, from, to, bounds.Follow)
	if err != nil {
		return Plan{}, err
	}

	plan := Plan{}
	if from.Kind == AtTime && bounds.StartOffset == nil {
		plan.FromMs = from.Millis
	}
	if to.Kind == AtTime && bounds.EndOffset == nil && !bounds.Follow {
		plan.ToMs = to.Millis
	}

	resolver := rangeResolver{offsets: offsets, bounds: bounds, from: from, to: to}

	for _, topic := range sel.Topics {
		ranges, err := resolver.topicRanges(topic, sel)
		if err != nil {
			return Plan{}, err
		}

		plan.Ranges = append(plan.Ranges, ranges...)
	}

	return plan, nil
}

type rangeResolver struct {
	offsets  logOffsets
	bounds   Bounds
	from, to Point
}

func (r rangeResolver) topicRanges(topic string, sel Selection) ([]Range, error) {
	partitions := r.offsets.partitions(topic)
	if len(partitions) == 0 {
		return nil, fmt.Errorf("topic %q not found", topic)
	}

	for _, p := range sel.Partitions {
		if !slices.Contains(partitions, p) {
			return nil, fmt.Errorf("topic %q has no partition %d (it has %d)", topic, p, len(partitions))
		}
	}

	keyPartition := int32(-1)
	if sel.Partitioner != nil && sel.Key != nil {
		keyPartition = sel.Partitioner(sel.Key, len(partitions))
	}

	var ranges []Range

	for _, p := range partitions {
		if len(sel.Partitions) > 0 && !slices.Contains(sel.Partitions, p) {
			continue
		}

		if keyPartition >= 0 && p != keyPartition {
			continue
		}

		if rng, ok := r.partitionRange(topic, p); ok {
			ranges = append(ranges, rng)
		}
	}

	return ranges, nil
}

func (r rangeResolver) partitionRange(topic string, partition int32) (Range, bool) {
	lo, hi := r.offsets.bounds(topic, partition)

	start := clamp(r.startOffset(topic, partition, lo, hi), lo, hi)
	if r.bounds.Follow {
		return Range{Topic: topic, Partition: partition, Start: start, End: OpenEnd}, true
	}

	end := clamp(r.endOffset(topic, partition, lo, hi), start, hi)

	return Range{Topic: topic, Partition: partition, Start: start, End: end}, end > start
}

func (r rangeResolver) startOffset(topic string, partition int32, lo, hi int64) int64 {
	if offset, ok := r.bounds.Resume[topic][partition]; ok {
		return offset
	}

	if r.bounds.StartOffset != nil {
		return relative(*r.bounds.StartOffset, hi)
	}

	return r.offsets.at(r.from, r.offsets.fromAfter, topic, partition, lo, hi)
}

func (r rangeResolver) endOffset(topic string, partition int32, lo, hi int64) int64 {
	if r.bounds.EndOffset != nil {
		return relative(*r.bounds.EndOffset, hi)
	}

	return r.offsets.at(r.to, r.offsets.toAfter, topic, partition, lo, hi)
}

func relative(offset, end int64) int64 {
	if offset < 0 {
		return end + offset
	}

	return offset
}

func clamp(v, lo, hi int64) int64 {
	return min(max(v, lo), hi)
}
