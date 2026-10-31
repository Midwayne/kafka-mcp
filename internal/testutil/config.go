package testutil

import (
	"testing"

	"github.com/Midwayne/kafka-mcp/internal/config"
)

// Normalize runs full config normalization so tests see production defaults.
func Normalize(tb testing.TB, cfg *config.Config) *config.Config {
	tb.Helper()

	if err := cfg.Normalize(); err != nil {
		tb.Fatalf("normalize config: %v", err)
	}

	return cfg
}

// ClusterConfig returns a normalized plaintext cluster config for the brokers.
func ClusterConfig(tb testing.TB, name string, brokers []string) config.Cluster {
	tb.Helper()

	cfg := Normalize(tb, &config.Config{Clusters: []config.Cluster{{Name: name, Brokers: brokers}}})

	return cfg.Clusters[0]
}
