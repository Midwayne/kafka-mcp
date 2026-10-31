package config

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v3"
)

// EnvVar names the environment variable that points at the config file.
const EnvVar = "KAFKA_MCP_CONFIG"

// Load reads, parses, expands placeholders in, and normalizes a config file.
func Load(path string) (*Config, error) {
	resolved, err := ResolvePath(path)
	if err != nil {
		return nil, err
	}

	raw, err := os.ReadFile(resolved)
	if err != nil {
		return nil, fmt.Errorf("read config %s: %w", resolved, err)
	}

	cfg, err := Decode(raw)
	if err != nil {
		return nil, fmt.Errorf("config %s: %w", resolved, err)
	}

	if err := cfg.Normalize(); err != nil {
		return nil, fmt.Errorf("config %s: %w", resolved, err)
	}

	return cfg, nil
}

// ResolvePath picks the config from an explicit path, KAFKA_MCP_CONFIG, or well-known locations.
func ResolvePath(path string) (string, error) {
	if strings.TrimSpace(path) != "" {
		return expandHome(path), nil
	}

	if env := strings.TrimSpace(os.Getenv(EnvVar)); env != "" {
		return expandHome(env), nil
	}

	candidates := []string{"kafka-mcp.yaml", "kafka-mcp.yml", "kafka-mcp.json"}
	if home, err := os.UserHomeDir(); err == nil {
		candidates = append(candidates, filepath.Join(home, ".kafka-mcp", "config.yaml"))
	}

	for _, candidate := range candidates {
		if _, err := os.Stat(candidate); err == nil {
			return candidate, nil
		}
	}

	return "", fmt.Errorf("no config found (pass -config, set %s, or create kafka-mcp.yaml)", EnvVar)
}

// Decode parses YAML or JSON (a YAML subset), expands placeholders in scalar
// values only, and rejects unknown fields. Expanding after parsing means
// comments are ignored and secrets containing YAML syntax stay literal.
func Decode(raw []byte) (*Config, error) {
	var root yaml.Node
	if err := yaml.Unmarshal(raw, &root); err != nil {
		return nil, fmt.Errorf("parse: %w", err)
	}

	if err := expandNode(&root); err != nil {
		return nil, err
	}

	expanded, err := yaml.Marshal(&root)
	if err != nil {
		return nil, fmt.Errorf("re-encode: %w", err)
	}

	decoder := yaml.NewDecoder(bytes.NewReader(expanded))
	decoder.KnownFields(true)

	var cfg Config
	if err := decoder.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("parse: %w", err)
	}

	return &cfg, nil
}

func expandHome(path string) string {
	if !strings.HasPrefix(path, "~/") {
		return path
	}

	home, err := os.UserHomeDir()
	if err != nil {
		return path
	}

	return filepath.Join(home, path[2:])
}

// ExpandHome resolves a leading "~/" against the user's home directory.
func ExpandHome(path string) string { return expandHome(path) }
