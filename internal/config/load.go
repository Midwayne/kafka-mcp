package config

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"gopkg.in/yaml.v3"
)

// EnvVar names the environment variable that points at the config file.
const EnvVar = "KAFKA_MCP_CONFIG"

var placeholder = regexp.MustCompile(`\$\$|\$\{file:([^}]+)\}|\$\{([A-Za-z_][A-Za-z0-9_]*)(:-([^}]*))?\}`)

// Load reads, expands, parses, and normalizes a config file.
func Load(path string) (*Config, error) {
	resolved, err := ResolvePath(path)
	if err != nil {
		return nil, err
	}

	raw, err := os.ReadFile(resolved)
	if err != nil {
		return nil, fmt.Errorf("read config %s: %w", resolved, err)
	}

	expanded, err := Expand(string(raw))
	if err != nil {
		return nil, fmt.Errorf("config %s: %w", resolved, err)
	}

	cfg, err := Parse(resolved, []byte(expanded))
	if err != nil {
		return nil, err
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

// Expand replaces ${VAR}, ${VAR:-default}, and ${file:/path}. "$$" yields a literal "$".
// Bare $VAR is left untouched so passwords containing "$" survive.
func Expand(s string) (string, error) {
	var firstErr error

	out := placeholder.ReplaceAllStringFunc(s, func(match string) string {
		if match == "$$" {
			return "$"
		}

		parts := placeholder.FindStringSubmatch(match)
		if parts[1] != "" {
			content, err := os.ReadFile(expandHome(strings.TrimSpace(parts[1])))
			if err != nil && firstErr == nil {
				firstErr = fmt.Errorf("expand %s: %w", match, err)
			}

			return strings.TrimSpace(string(content))
		}

		if value, ok := os.LookupEnv(parts[2]); ok {
			return value
		}

		return parts[4]
	})

	return out, firstErr
}

// Parse decodes YAML or JSON based on the file extension.
func Parse(path string, raw []byte) (*Config, error) {
	var cfg Config

	if strings.EqualFold(filepath.Ext(path), ".json") {
		if err := json.Unmarshal(raw, &cfg); err != nil {
			return nil, fmt.Errorf("parse config JSON: %w", err)
		}

		return &cfg, nil
	}

	decoder := yaml.NewDecoder(strings.NewReader(string(raw)))
	decoder.KnownFields(true)

	if err := decoder.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("parse config YAML: %w", err)
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
