package app

import (
	"context"
	"fmt"
	"strings"

	"github.com/Midwayne/kafka-mcp/internal/record"
	"github.com/Midwayne/kafka-mcp/internal/scan"
	"github.com/Midwayne/kafka-mcp/internal/sink"
)

const (
	defaultSampleCount = 5
	maxSampleCount     = 50
	maxContext         = 50
	maxNewestRead      = 5000
)

// SampleSpec requests a few messages to look at.
type SampleSpec struct {
	Target
	Count int    `json:"count,omitempty" jsonschema:"Messages to return (default 5, max 50)."`
	From  string `json:"from,omitempty" jsonschema:"latest (default) returns the newest messages; earliest or a time reads forward from that point."`
	Output
}

// GetMessagesSpec reads messages around one offset, like grep -B/-A/-C.
type GetMessagesSpec struct {
	Cluster   string `json:"cluster" jsonschema:"Cluster name."`
	Topic     string `json:"topic" jsonschema:"Topic name."`
	Partition int32  `json:"partition" jsonschema:"Partition of the message."`
	Offset    int64  `json:"offset" jsonschema:"Offset of the message."`
	Before    int    `json:"before,omitempty" jsonschema:"Also return this many preceding messages (like grep -B, max 50)."`
	After     int    `json:"after,omitempty" jsonschema:"Also return this many following messages (like grep -A, max 50)."`
	Context   int    `json:"context,omitempty" jsonschema:"Shorthand for before and after (like grep -C)."`
	Output
}

// MessagesResult holds messages plus scan statistics.
type MessagesResult struct {
	Messages []map[string]any `json:"messages"`
	Stats    scan.Stats       `json:"stats"`
}

// Sample returns the newest messages (or the first ones after from).
func (s *Service) Sample(ctx context.Context, spec SampleSpec) (MessagesResult, error) {
	count := clampInt(spec.Count, defaultSampleCount, maxSampleCount)

	var matches *sink.Matches

	stats, err := s.readNewest(ctx, ToolSampleMessages, spec.Target, spec.From, count, spec.Output,
		func(p *prepared, capacity int) scan.Sink {
			matches = sink.NewMatches(capacity, p.view, nil)
			return matches
		})
	if err != nil {
		return MessagesResult{}, err
	}

	items := matches.Items()

	return MessagesResult{Messages: items[max(len(items)-count, 0):], Stats: stats}, nil
}

// sinkFactory builds the sink for a prepared read; capacity is the most records it can see.
type sinkFactory func(p *prepared, capacity int) scan.Sink

// readNewest reads up to perPartition records from each partition: the newest
// ones by default, or forward from a time point.
func (s *Service) readNewest(ctx context.Context, tool string, target Target, from string, perPartition int,
	output Output, newSink sinkFactory,
) (scan.Stats, error) {
	spec := ScanSpec{Target: target, Output: output}

	if from == "" || strings.EqualFold(from, "latest") {
		start := -int64(perPartition)
		spec.StartOffset = &start
	} else {
		spec.From = from
	}

	p, err := s.prepare(ctx, spec, tool)
	if err != nil {
		return scan.Stats{}, err
	}

	capacity := min(perPartition*max(len(p.plan.Ranges), 1), maxNewestRead)
	p.limits.MaxScanned = int64(capacity)

	return p.runSync(ctx, newSink(p, capacity))
}

// GetMessages returns one message with optional surrounding context in its partition.
func (s *Service) GetMessages(ctx context.Context, spec GetMessagesSpec) (MessagesResult, error) {
	before := clampInt(max(spec.Before, spec.Context), 0, maxContext)
	after := clampInt(max(spec.After, spec.Context), 0, maxContext)

	start, end := max(spec.Offset-int64(before), 0), spec.Offset+int64(after)+1

	scanSpec := ScanSpec{
		Target: Target{Cluster: spec.Cluster, Topics: []string{spec.Topic}, Partitions: []int32{spec.Partition}},
		Window: Window{StartOffset: &start, EndOffset: &end},
		Output: spec.Output,
		Budget: Budget{Limit: before + after + 1},
	}

	p, err := s.prepare(ctx, scanSpec, ToolGetMessages)
	if err != nil {
		return MessagesResult{}, err
	}

	if len(p.plan.Ranges) == 0 {
		return MessagesResult{}, fmt.Errorf("offset %d is outside the retained range of %s[%d] (see describe_topic)", spec.Offset, spec.Topic, spec.Partition)
	}

	matches := sink.NewMatches(before+after+1, p.view, nil)
	stats, err := p.runSync(ctx, matches)

	return MessagesResult{Messages: matches.Items(), Stats: stats}, err
}

// funcSink adapts a function to scan.Sink. It must be safe for concurrent calls.
type funcSink func(*record.Message)

func (f funcSink) Offer(m *record.Message) (accepted, more bool) {
	f(m)
	return true, true
}
