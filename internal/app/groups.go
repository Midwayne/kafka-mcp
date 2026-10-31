package app

import (
	"context"
	"fmt"
	"path"
	"strings"

	"github.com/twmb/franz-go/pkg/kadm"
)

// ListGroupsSpec filters consumer groups.
type ListGroupsSpec struct {
	Cluster string `json:"cluster" jsonschema:"Cluster name."`
	Pattern string `json:"pattern,omitempty" jsonschema:"Glob filter on group names, e.g. *orders*."`
}

// GroupInfo summarizes a consumer group.
type GroupInfo struct {
	Group        string `json:"group"`
	State        string `json:"state,omitempty"`
	ProtocolType string `json:"protocol_type,omitempty"`
}

// DescribeGroupSpec selects a consumer group.
type DescribeGroupSpec struct {
	Cluster string `json:"cluster" jsonschema:"Cluster name."`
	Group   string `json:"group" jsonschema:"Consumer group ID."`
}

// GroupLag is a group's state and lag per partition.
type GroupLag struct {
	Group      string              `json:"group"`
	State      string              `json:"state"`
	Members    int                 `json:"members"`
	TotalLag   int64               `json:"total_lag"`
	LagByTopic map[string]int64    `json:"lag_by_topic"`
	Partitions []PartitionLagEntry `json:"partitions"`
}

// PartitionLagEntry is one partition's committed offset and lag.
type PartitionLagEntry struct {
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	Committed int64  `json:"committed"`
	End       int64  `json:"end_offset"`
	Lag       int64  `json:"lag"`
	Member    string `json:"member,omitempty"`
	Error     string `json:"error,omitempty"`
}

// ListConsumerGroups lists groups matching an optional glob.
func (s *Service) ListConsumerGroups(ctx context.Context, spec ListGroupsSpec) ([]GroupInfo, error) {
	h, err := s.cluster(spec.Cluster, ToolListConsumerGroups)
	if err != nil {
		return nil, err
	}

	listed, err := h.Admin().ListGroups(ctx)
	if err != nil {
		return nil, fmt.Errorf("list groups: %w", err)
	}

	out := []GroupInfo{}
	for _, group := range listed.Sorted() {
		if spec.Pattern != "" {
			if ok, _ := path.Match(spec.Pattern, group.Group); !ok {
				continue
			}
		}

		out = append(out, GroupInfo{Group: group.Group, State: group.State, ProtocolType: group.ProtocolType})
	}

	return out, nil
}

// DescribeConsumerGroup reports a group's lag on visible topics. It never joins the group.
func (s *Service) DescribeConsumerGroup(ctx context.Context, spec DescribeGroupSpec) (GroupLag, error) {
	h, err := s.cluster(spec.Cluster, ToolDescribeConsumerGroup)
	if err != nil {
		return GroupLag{}, err
	}

	lags, err := h.Admin().Lag(ctx, spec.Group)
	if err != nil {
		return GroupLag{}, fmt.Errorf("describe group: %w", err)
	}

	described, ok := lags[spec.Group]
	if !ok {
		return GroupLag{}, fmt.Errorf("group %q not found", spec.Group)
	}

	if err := described.Error(); err != nil {
		return GroupLag{}, fmt.Errorf("group %q: %w", spec.Group, err)
	}

	out := GroupLag{Group: spec.Group, State: described.State, Members: len(described.Members), LagByTopic: map[string]int64{}}

	entries := described.Lag.Sorted()
	for i := range entries {
		entry := &entries[i]
		if !h.Config().TopicAllowed(entry.Topic) {
			continue
		}

		if entry.Lag > 0 {
			out.TotalLag += entry.Lag
			out.LagByTopic[entry.Topic] += entry.Lag
		}

		out.Partitions = append(out.Partitions, lagRow(entry))
	}

	return out, nil
}

func lagRow(entry *kadm.GroupMemberLag) PartitionLagEntry {
	row := PartitionLagEntry{Topic: entry.Topic, Partition: entry.Partition, Committed: entry.Commit.At, End: entry.End.Offset, Lag: entry.Lag}

	if entry.Member != nil {
		row.Member = strings.TrimSpace(entry.Member.ClientID + " " + entry.Member.ClientHost)
	}

	if entry.Err != nil {
		row.Error = entry.Err.Error()
	}

	return row
}
