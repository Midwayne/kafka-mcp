package app

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/Midwayne/kafka-mcp/internal/config"
	"github.com/Midwayne/kafka-mcp/internal/record"
	"github.com/Midwayne/kafka-mcp/internal/scan"
)

// topicConfigKeys are the settings most relevant when reading a topic.
var topicConfigKeys = []string{
	"cleanup.policy", "retention.ms", "retention.bytes", "min.insync.replicas",
	"max.message.bytes", "compression.type", "message.timestamp.type",
}

// DescribeTopicSpec selects a topic.
type DescribeTopicSpec struct {
	Cluster string `json:"cluster" jsonschema:"Cluster name."`
	Topic   string `json:"topic" jsonschema:"Topic name."`
	At      string `json:"at,omitempty" jsonschema:"Also report each partition's first offset at or after this time (same formats as scan from)."`
}

// PartitionInfo describes one partition.
type PartitionInfo struct {
	Partition     int32   `json:"partition"`
	Leader        int32   `json:"leader"`
	Replicas      []int32 `json:"replicas"`
	ISR           []int32 `json:"isr"`
	Start         int64   `json:"start_offset"`
	End           int64   `json:"end_offset"`
	Messages      int64   `json:"approx_messages"`
	OffsetAt      *int64  `json:"offset_at,omitempty"`
	LastMessageAt string  `json:"last_message_at,omitempty"`
}

// TopicDescription is the describe_topic result.
type TopicDescription struct {
	Topic         string            `json:"topic"`
	Messages      int64             `json:"approx_messages"`
	LastMessageAt string            `json:"last_message_at,omitempty"`
	Partitions    []PartitionInfo   `json:"partitions"`
	Configs       map[string]string `json:"configs,omitempty"`
	ConfigsError  string            `json:"configs_error,omitempty"`
}

// DescribeTopic returns partition layout, offsets, recency, and key configs.
func (s *Service) DescribeTopic(ctx context.Context, spec DescribeTopicSpec) (TopicDescription, error) {
	h, err := s.cluster(spec.Cluster, ToolDescribeTopic)
	if err != nil {
		return TopicDescription{}, err
	}

	if err := h.checkTopic(spec.Topic); err != nil {
		return TopicDescription{}, err
	}

	description, err := s.partitionLayout(ctx, h, spec)
	if err != nil {
		return TopicDescription{}, err
	}

	description.Configs, description.ConfigsError = h.topicConfigs(ctx, spec.Topic)
	s.addLastTimestamps(ctx, spec, &description)

	return description, nil
}

func (s *Service) partitionLayout(ctx context.Context, h *handle, spec DescribeTopicSpec) (TopicDescription, error) {
	details, err := h.Admin().ListTopics(ctx, spec.Topic)
	if err != nil || !details.Has(spec.Topic) || details[spec.Topic].Err != nil {
		return TopicDescription{}, fmt.Errorf("topic %q not found on cluster %q", spec.Topic, h.Name())
	}

	starts, ends, err := h.offsets(ctx, spec.Topic)
	if err != nil {
		return TopicDescription{}, err
	}

	offsetsAt, err := s.offsetsAt(ctx, h, spec)
	if err != nil {
		return TopicDescription{}, err
	}

	description := TopicDescription{Topic: spec.Topic}

	for _, partition := range details[spec.Topic].Partitions.Sorted() {
		info := PartitionInfo{
			Partition: partition.Partition, Leader: partition.Leader, Replicas: partition.Replicas, ISR: partition.ISR,
			Start: starts[spec.Topic][partition.Partition].Offset, End: ends[spec.Topic][partition.Partition].Offset,
		}
		info.Messages = info.End - info.Start

		if at, ok := offsetsAt[partition.Partition]; ok {
			info.OffsetAt = &at
		}

		description.Messages += info.Messages
		description.Partitions = append(description.Partitions, info)
	}

	return description, nil
}

func (s *Service) offsetsAt(ctx context.Context, h *handle, spec DescribeTopicSpec) (map[int32]int64, error) {
	if spec.At == "" {
		return nil, nil
	}

	point, err := scan.ParsePoint(spec.At, s.now())
	if err != nil || point.Kind != scan.AtTime {
		return nil, fmt.Errorf("at must be a time: %w", err)
	}

	listed, err := h.Admin().ListOffsetsAfterMilli(ctx, point.Millis, spec.Topic)
	if err != nil {
		return nil, fmt.Errorf("list offsets at %s: %w", spec.At, err)
	}

	out := map[int32]int64{}
	for partition, offset := range listed[spec.Topic] {
		out[partition] = offset.Offset
	}

	return out, nil
}

func (h *handle) topicConfigs(ctx context.Context, topic string) (map[string]string, string) {
	resources, err := h.Admin().DescribeTopicConfigs(ctx, topic)
	if err == nil && len(resources) > 0 && resources[0].Err != nil {
		err = resources[0].Err
	}

	if err != nil {
		return nil, err.Error()
	}

	configs := map[string]string{}
	for _, entry := range resources[0].Configs {
		if entry.Value != nil && slices.Contains(topicConfigKeys, entry.Key) {
			configs[entry.Key] = *entry.Value
		}
	}

	return configs, ""
}

// addLastTimestamps reads the newest record of each partition (best effort).
func (s *Service) addLastTimestamps(ctx context.Context, spec DescribeTopicSpec, description *TopicDescription) {
	var mu sync.Mutex

	latest := map[int32]time.Time{}

	capture := func(m *record.Message) {
		mu.Lock()
		defer mu.Unlock()

		if m.Record.Timestamp.After(latest[m.Record.Partition]) {
			latest[m.Record.Partition] = m.Record.Timestamp
		}
	}

	target := Target{Cluster: spec.Cluster, Topics: []string{spec.Topic}}
	output := Output{KeyFormat: config.FormatNone, ValueFormat: config.FormatNone}

	_, _ = s.readNewest(ctx, ToolDescribeTopic, target, "", 1, output, func(*prepared, int) scan.Sink { return funcSink(capture) })

	var newest time.Time
	for i := range description.Partitions {
		if ts, ok := latest[description.Partitions[i].Partition]; ok {
			description.Partitions[i].LastMessageAt = ts.UTC().Format(time.RFC3339)
			if ts.After(newest) {
				newest = ts
			}
		}
	}

	if !newest.IsZero() {
		description.LastMessageAt = newest.UTC().Format(time.RFC3339)
	}
}
