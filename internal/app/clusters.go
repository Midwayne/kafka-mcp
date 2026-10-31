package app

import (
	"context"
	"fmt"
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

// ClusterDetails is live metadata for one cluster.
type ClusterDetails struct {
	Name       string   `json:"name"`
	ClusterID  string   `json:"cluster_id"`
	Controller int32    `json:"controller"`
	Brokers    []Broker `json:"brokers"`
	Topics     int      `json:"visible_topics"`
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

// DescribeCluster connects and returns broker metadata.
func (s *Service) DescribeCluster(ctx context.Context, name string) (ClusterDetails, error) {
	h, err := s.cluster(name, ToolDescribeCluster)
	if err != nil {
		return ClusterDetails{}, err
	}

	metadata, err := h.Admin().Metadata(ctx)
	if err != nil {
		return ClusterDetails{}, fmt.Errorf("cluster %q: %w", name, err)
	}

	details := ClusterDetails{Name: name, ClusterID: metadata.Cluster, Controller: metadata.Controller}

	for _, broker := range metadata.Brokers {
		entry := Broker{ID: broker.NodeID, Address: broker.Host + ":" + strconv.Itoa(int(broker.Port))}
		if broker.Rack != nil {
			entry.Rack = *broker.Rack
		}

		details.Brokers = append(details.Brokers, entry)
	}

	for _, topic := range metadata.Topics {
		if !topic.IsInternal && h.Config().TopicAllowed(topic.Topic) {
			details.Topics++
		}
	}

	return details, nil
}
