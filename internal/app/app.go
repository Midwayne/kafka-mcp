// Package app implements the operations behind each MCP tool. It enforces
// access rules and turns agent requests into bounded scans.
package app

import (
	"fmt"
	"path/filepath"
	"time"

	"github.com/Midwayne/kafka-mcp/internal/codec"
	"github.com/Midwayne/kafka-mcp/internal/config"
	"github.com/Midwayne/kafka-mcp/internal/jobs"
	"github.com/Midwayne/kafka-mcp/internal/kafka"
	"github.com/Midwayne/kafka-mcp/internal/saved"
	"github.com/Midwayne/kafka-mcp/internal/schemaregistry"
)

// Service is the application core shared by all tools.
type Service struct {
	cfg      *config.Config
	registry *kafka.Registry
	handles  map[string]*handle
	jobs     *jobs.Manager
	saved    *saved.Store
	now      func() time.Time
}

// handle bundles one cluster with its decoding dependencies.
type handle struct {
	*kafka.Cluster

	decoder  *codec.Decoder
	registry *schemaregistry.Client // nil when no Schema Registry is configured
}

// New builds clients for every enabled cluster. It does not dial; use Ping to verify.
func New(cfg *config.Config) (*Service, error) {
	registry, err := kafka.NewRegistry(cfg.Clusters)
	if err != nil {
		return nil, err
	}

	handles := map[string]*handle{}
	for _, cluster := range registry.All() {
		h, err := newHandle(cluster)
		if err != nil {
			registry.Close()
			return nil, err
		}

		handles[cluster.Name()] = h
	}

	store, err := saved.Open(filepath.Join(cfg.Server.StateDir, "saved-scans.json"))
	if err != nil {
		registry.Close()
		return nil, err
	}

	jobLimits := cfg.Defaults.Jobs

	return &Service{
		cfg:      cfg,
		registry: registry,
		handles:  handles,
		jobs:     jobs.NewManager(jobLimits.MaxConcurrent, jobLimits.TTL.Std()),
		saved:    store,
		now:      time.Now,
	}, nil
}

func newHandle(cluster *kafka.Cluster) (*handle, error) {
	cfg := cluster.Config()
	h := &handle{Cluster: cluster}

	if cfg.SchemaRegistry != nil {
		client, err := schemaregistry.New(cfg.SchemaRegistry, cfg.TLS)
		if err != nil {
			return nil, fmt.Errorf("cluster %q: %w", cfg.Name, err)
		}

		h.registry = client
	}

	if h.registry != nil {
		h.decoder = codec.NewDecoder(h.registry)
	} else {
		h.decoder = codec.NewDecoder(nil)
	}

	return h, nil
}

// Close cancels running jobs and releases clients.
func (s *Service) Close() {
	s.jobs.Shutdown()
	s.registry.Close()
}

// Config returns the loaded configuration.
func (s *Service) Config() *config.Config { return s.cfg }

// AnyWritable reports whether any cluster allows write tools.
func (s *Service) AnyWritable() bool {
	for _, h := range s.handles {
		if h.Config().CanWrite() {
			return true
		}
	}

	return false
}

// cluster resolves a cluster and checks that the tool is enabled for it.
func (s *Service) cluster(name, tool string) (*handle, error) {
	cluster, err := s.registry.Get(name)
	if err != nil {
		return nil, err
	}

	if !cluster.Config().Tools.Allows(tool) {
		return nil, fmt.Errorf("tool %s is disabled for cluster %q", tool, name)
	}

	return s.handles[cluster.Name()], nil
}

// checkTopic rejects topics hidden by the cluster's topic filter.
func (h *handle) checkTopic(topic string) error {
	if !h.Config().TopicAllowed(topic) {
		return fmt.Errorf("topic %q is not exposed on cluster %q", topic, h.Name())
	}

	return nil
}

func (h *handle) checkWrite() error {
	if !h.Config().CanWrite() {
		return fmt.Errorf("cluster %q is read_only; set access: read_write to allow writes", h.Name())
	}

	return nil
}
