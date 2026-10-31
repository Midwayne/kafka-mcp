package kafka

import (
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/Midwayne/kafka-mcp/internal/config"
)

// Registry indexes clusters by name.
type Registry struct {
	clusters map[string]*Cluster
	names    []string
}

// NewRegistry builds a Cluster for every enabled config entry.
func NewRegistry(cfgs []config.Cluster) (*Registry, error) {
	registry := &Registry{clusters: make(map[string]*Cluster, len(cfgs))}

	for i := range cfgs {
		cfg := &cfgs[i]
		if !cfg.IsEnabled() {
			continue
		}

		cluster, err := NewCluster(*cfg)
		if err != nil {
			registry.Close()
			return nil, err
		}

		registry.clusters[cfg.Name] = cluster
		registry.names = append(registry.names, cfg.Name)
	}

	if len(registry.names) == 0 {
		return nil, errors.New("no enabled clusters")
	}

	sort.Strings(registry.names)
	return registry, nil
}

// Get returns a cluster by name.
func (r *Registry) Get(name string) (*Cluster, error) {
	cluster, ok := r.clusters[name]
	if !ok {
		return nil, fmt.Errorf("unknown cluster %q (available: %s)", name, strings.Join(r.names, ", "))
	}

	return cluster, nil
}

// All returns clusters sorted by name.
func (r *Registry) All() []*Cluster {
	out := make([]*Cluster, 0, len(r.names))
	for _, name := range r.names {
		out = append(out, r.clusters[name])
	}

	return out
}

// Close releases every cluster's clients.
func (r *Registry) Close() {
	for _, cluster := range r.clusters {
		cluster.Close()
	}
}
