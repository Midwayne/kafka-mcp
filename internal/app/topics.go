package app

import (
	"context"
	"fmt"
	"path"

	"github.com/twmb/franz-go/pkg/kadm"
)

const (
	defaultTopicListLimit = 500
	maxTopicListLimit     = 5000
)

// ListTopicsSpec filters the topic listing.
type ListTopicsSpec struct {
	Cluster         string `json:"cluster" jsonschema:"Cluster name."`
	Pattern         string `json:"pattern,omitempty" jsonschema:"Glob filter such as *orders* (default: all visible topics)."`
	IncludeInternal bool   `json:"include_internal,omitempty" jsonschema:"Include internal topics such as __consumer_offsets."`
	WithCounts      bool   `json:"with_counts,omitempty" jsonschema:"Add approximate message counts (end minus start offsets). Costs one offsets request."`
	Limit           int    `json:"limit,omitempty" jsonschema:"Maximum topics to return (default 500)."`
}

// TopicInfo summarizes one topic.
type TopicInfo struct {
	Name        string `json:"name"`
	Partitions  int    `json:"partitions"`
	Replication int    `json:"replication"`
	Messages    *int64 `json:"approx_messages,omitempty"`
	Internal    bool   `json:"internal,omitempty"`
}

// TopicList is the listing result.
type TopicList struct {
	Topics    []TopicInfo `json:"topics"`
	Total     int         `json:"total"`
	Truncated bool        `json:"truncated,omitempty"`
}

// ListTopics lists visible topics.
func (s *Service) ListTopics(ctx context.Context, spec ListTopicsSpec) (TopicList, error) {
	h, err := s.cluster(spec.Cluster, ToolListTopics)
	if err != nil {
		return TopicList{}, err
	}

	details, err := h.Admin().ListTopicsWithInternal(ctx)
	if err != nil {
		return TopicList{}, fmt.Errorf("list topics: %w", err)
	}

	limit := clampInt(spec.Limit, defaultTopicListLimit, maxTopicListLimit)
	list := TopicList{}

	for _, detail := range details.Sorted() {
		if !h.visible(detail, spec) {
			continue
		}

		list.Total++
		if len(list.Topics) >= limit {
			list.Truncated = true
			continue
		}

		list.Topics = append(list.Topics, topicInfo(detail))
	}

	if spec.WithCounts {
		err = h.addCounts(ctx, list.Topics)
	}

	return list, err
}

func (h *handle) visible(detail kadm.TopicDetail, spec ListTopicsSpec) bool {
	if detail.IsInternal && !spec.IncludeInternal {
		return false
	}

	if spec.Pattern != "" {
		if ok, _ := path.Match(spec.Pattern, detail.Topic); !ok {
			return false
		}
	}

	return h.Config().TopicAllowed(detail.Topic)
}

func topicInfo(detail kadm.TopicDetail) TopicInfo {
	info := TopicInfo{Name: detail.Topic, Partitions: len(detail.Partitions), Internal: detail.IsInternal}

	for _, partition := range detail.Partitions {
		info.Replication = max(info.Replication, len(partition.Replicas))
	}

	return info
}

func (h *handle) addCounts(ctx context.Context, topics []TopicInfo) error {
	names := make([]string, 0, len(topics))
	for _, topic := range topics {
		names = append(names, topic.Name)
	}

	if len(names) == 0 {
		return nil
	}

	starts, ends, err := h.offsets(ctx, names...)
	if err != nil {
		return err
	}

	for i := range topics {
		total := int64(0)
		for partition, end := range ends[topics[i].Name] {
			total += end.Offset - starts[topics[i].Name][partition].Offset
		}

		topics[i].Messages = &total
	}

	return nil
}

func (h *handle) offsets(ctx context.Context, topics ...string) (starts, ends kadm.ListedOffsets, err error) {
	if starts, err = h.Admin().ListStartOffsets(ctx, topics...); err != nil {
		return nil, nil, fmt.Errorf("list start offsets: %w", err)
	}

	if ends, err = h.Admin().ListEndOffsets(ctx, topics...); err != nil {
		return nil, nil, fmt.Errorf("list end offsets: %w", err)
	}

	return starts, ends, nil
}
