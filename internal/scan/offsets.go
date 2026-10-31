package scan

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
)

// logOffsets holds the listed offsets the planner resolves bounds against.
type logOffsets struct {
	start, end         kadm.ListedOffsets
	fromAfter, toAfter kadm.ListedOffsets
}

func listOffsets(ctx context.Context, lister OffsetLister, topics []string, from, to Point, follow bool) (logOffsets, error) {
	var (
		out logOffsets
		err error
	)

	if out.start, err = checked(lister.ListStartOffsets(ctx, topics...)); err != nil {
		return out, fmt.Errorf("list start offsets: %w", err)
	}

	if out.end, err = checked(lister.ListEndOffsets(ctx, topics...)); err != nil {
		return out, fmt.Errorf("list end offsets: %w", err)
	}

	if from.Kind == AtTime {
		if out.fromAfter, err = checked(lister.ListOffsetsAfterMilli(ctx, from.Millis, topics...)); err != nil {
			return out, fmt.Errorf("list offsets for from: %w", err)
		}
	}

	if to.Kind == AtTime && !follow {
		if out.toAfter, err = checked(lister.ListOffsetsAfterMilli(ctx, to.Millis, topics...)); err != nil {
			return out, fmt.Errorf("list offsets for to: %w", err)
		}
	}

	return out, nil
}

// checked turns per-partition errors into a call error, ignoring unknown
// topics so the planner can report them by name.
func checked(listed kadm.ListedOffsets, err error) (kadm.ListedOffsets, error) {
	if err != nil {
		return nil, err
	}

	for topic, partitions := range listed {
		for partition, offset := range partitions {
			if offset.Err == nil {
				continue
			}

			if partition < 0 || isUnknownTopic(offset.Err) {
				delete(listed, topic)
				break
			}

			return nil, fmt.Errorf("%s[%d]: %w", topic, partition, offset.Err)
		}
	}

	return listed, nil
}

func isUnknownTopic(err error) bool {
	return errors.Is(err, kerr.UnknownTopicOrPartition) || errors.Is(err, kerr.UnknownTopicID)
}

// partitions returns the sorted partition IDs of a topic.
func (o logOffsets) partitions(topic string) []int32 {
	ids := make([]int32, 0, len(o.end[topic]))
	for id := range o.end[topic] {
		ids = append(ids, id)
	}

	slices.Sort(ids)

	return ids
}

// bounds returns the log start and end (high watermark) offsets.
func (o logOffsets) bounds(topic string, partition int32) (lo, hi int64) {
	return o.start[topic][partition].Offset, o.end[topic][partition].Offset
}

// at resolves a point. Time lookups that find no record return the end offset.
func (o logOffsets) at(point Point, afterTime kadm.ListedOffsets, topic string, partition int32, lo, hi int64) int64 {
	switch point.Kind {
	case Earliest:
		return lo
	case Latest:
		return hi
	default:
		offset, ok := afterTime[topic][partition]
		if !ok || offset.Offset < 0 {
			return hi
		}

		return offset.Offset
	}
}
