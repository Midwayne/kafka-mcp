package app

import (
	"context"
	"fmt"
	"net"
	"slices"
	"strconv"

	"github.com/Midwayne/kafka-mcp/internal/config"
)

// ClusterInfo describes a configured cluster without secrets.
type ClusterInfo struct {
	Name           string             `json:"name"`
	Brokers        []string           `json:"brokers"`
	Access         string             `json:"access"`
	Auth           map[string]any     `json:"auth"`
	TopicFilter    *config.NameFilter `json:"topic_filter,omitempty"`
	RedactedFields []string           `json:"redacted_fields,omitempty"`
	DefaultFormats map[string]string  `json:"default_formats"`
}

// DescribeClusterSpec selects a cluster.
type DescribeClusterSpec struct {
	Cluster        string `json:"cluster" jsonschema:"Cluster name."`
	IncludeBrokers bool   `json:"include_brokers,omitempty" jsonschema:"List every broker address (large clusters have many)."`
}

// ClusterDetails is live metadata for one cluster.
type ClusterDetails struct {
	Name        string   `json:"name"`
	ClusterID   string   `json:"cluster_id"`
	Controller  int32    `json:"controller"`
	BrokerCount int      `json:"broker_count"`
	Racks       []string `json:"racks,omitempty"`
	Reachable   int      `json:"reachable_brokers"`
	Topics      int      `json:"visible_topics"`
	Brokers     []Broker `json:"brokers,omitempty"`
}

// Broker is one broker's address.
type Broker struct {
	ID      int32  `json:"id"`
	Address string `json:"address"`
	Rack    string `json:"rack,omitempty"`
}

// ListClusters returns configured clusters. It does not contact any broker.
func (s *Service) ListClusters() []ClusterInfo {
	clusters := s.registry.All()
	out := make([]ClusterInfo, 0, len(clusters))

	for _, cluster := range clusters {
		cfg := cluster.Config()

		info := ClusterInfo{
			Name:           cfg.Name,
			Brokers:        cfg.Brokers,
			Access:         cfg.Access,
			Auth:           cfg.AuthSummary(),
			RedactedFields: cfg.Redact,
			DefaultFormats: map[string]string{"key": cfg.Formats.Key, "value": cfg.Formats.Value},
		}

		if len(cfg.Topics.Include) > 0 || len(cfg.Topics.Exclude) > 0 {
			info.TopicFilter = &cfg.Topics
		}

		out = append(out, info)
	}

	return out
}

// DescribeCluster connects to every broker and returns cluster metadata.
func (s *Service) DescribeCluster(ctx context.Context, spec DescribeClusterSpec) (ClusterDetails, error) {
	h, err := s.cluster(spec.Cluster, ToolDescribeCluster)
	if err != nil {
		return ClusterDetails{}, err
	}

	reachable, err := h.Ping(ctx)
	if err != nil {
		return ClusterDetails{}, err
	}

	metadata, err := h.Admin().Metadata(ctx)
	if err != nil {
		return ClusterDetails{}, fmt.Errorf("cluster %q: %w", spec.Cluster, err)
	}

	details := ClusterDetails{
		Name: spec.Cluster, ClusterID: metadata.Cluster, Controller: metadata.Controller,
		BrokerCount: len(metadata.Brokers), Reachable: reachable,
	}

	for _, broker := range metadata.Brokers {
		entry := Broker{ID: broker.NodeID, Address: net.JoinHostPort(broker.Host, strconv.Itoa(int(broker.Port)))}
		if broker.Rack != nil {
			entry.Rack = *broker.Rack
			details.Racks = append(details.Racks, entry.Rack)
		}

		if spec.IncludeBrokers {
			details.Brokers = append(details.Brokers, entry)
		}
	}

	slices.Sort(details.Racks)
	details.Racks = slices.Compact(details.Racks)

	for _, topic := range metadata.Topics {
		if !topic.IsInternal && h.Config().TopicAllowed(topic.Topic) {
			details.Topics++
		}
	}

	return details, nil
}
