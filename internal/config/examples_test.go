package config

import (
	"os"
	"path/filepath"
	"testing"
)

// TestExamplesLoad keeps the documented examples valid as the schema evolves.
func TestExamplesLoad(t *testing.T) {
	for _, name := range []string{"KAFKA_MCP_TOKEN", "KAFKA_USER", "KAFKA_PASSWORD", "OAUTH_CLIENT_ID", "OAUTH_CLIENT_SECRET",
		"CONFLUENT_API_KEY", "CONFLUENT_API_SECRET", "SR_API_KEY", "SR_API_SECRET"} {
		t.Setenv(name, "placeholder")
	}

	// Examples reference secret files under these paths; point them at a temp home.
	home := t.TempDir()
	t.Setenv("HOME", home)

	for _, secret := range []string{"certs/truststore/pw.txt", "certs/client/pw.txt"} {
		path := filepath.Join(home, secret)
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte("changeit\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	examples, err := filepath.Glob("../../examples/*.yaml")
	if err != nil || len(examples) == 0 {
		t.Fatalf("no examples found: %v", err)
	}

	for _, path := range examples {
		t.Run(filepath.Base(path), func(t *testing.T) {
			if _, err := Load(path); err != nil {
				t.Fatalf("example does not load: %v", err)
			}
		})
	}
}
