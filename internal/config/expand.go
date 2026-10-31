package config

import (
	"fmt"
	"os"
	"regexp"
	"strings"

	"gopkg.in/yaml.v3"
)

var placeholder = regexp.MustCompile(`\$\$|\$\{file:([^}]+)\}|\$\{([A-Za-z_][A-Za-z0-9_]*)(:-([^}]*))?\}`)

// expandNode expands placeholders in every scalar of the tree.
func expandNode(node *yaml.Node) error {
	if node.Kind == yaml.ScalarNode {
		return expandScalar(node)
	}

	for _, child := range node.Content {
		if err := expandNode(child); err != nil {
			return err
		}
	}

	return nil
}

// expandScalar expands one value. Plain scalars lose their resolved tag so
// that, for example, port: ${PORT} decodes as a number.
func expandScalar(node *yaml.Node) error {
	expanded, err := Expand(node.Value)
	if err != nil {
		return fmt.Errorf("line %d: %w", node.Line, err)
	}

	if expanded == node.Value {
		return nil
	}

	node.Value = expanded
	if node.Style == 0 {
		node.Tag = ""
	}

	return nil
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
