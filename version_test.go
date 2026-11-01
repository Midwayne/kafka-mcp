package main

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/Midwayne/kafka-mcp/internal/config"
)

func TestDistributionVersionsAgree(t *testing.T) {
	data, err := os.ReadFile("package.json")
	if err != nil {
		t.Fatal(err)
	}

	var metadata struct {
		Version string `json:"version"`
	}
	if err := json.Unmarshal(data, &metadata); err != nil {
		t.Fatal(err)
	}

	if metadata.Version != config.DefaultServerVersion {
		t.Fatalf("package version %q differs from server version %q", metadata.Version, config.DefaultServerVersion)
	}
}
