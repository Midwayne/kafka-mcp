package server

import (
	"context"
	"os"
	"regexp"
	"slices"
	"testing"

	"github.com/mark3labs/mcp-go/mcp"
)

var toolHeading = regexp.MustCompile("(?m)^### `([a-z_]+)`$")

// TestToolsAreDocumented keeps docs/tools.md in sync with the registered tools.
func TestToolsAreDocumented(t *testing.T) {
	doc, err := os.ReadFile("../../docs/tools.md")
	if err != nil {
		t.Fatal(err)
	}

	matches := toolHeading.FindAllStringSubmatch(string(doc), -1)
	documented := make([]string, 0, len(matches))
	for _, match := range matches {
		documented = append(documented, match[1])
	}

	listed, err := newHarness(t).client.ListTools(context.Background(), mcp.ListToolsRequest{})
	if err != nil {
		t.Fatal(err)
	}

	registered := make([]string, 0, len(listed.Tools))
	for _, tool := range listed.Tools {
		registered = append(registered, tool.Name)

		if !slices.Contains(documented, tool.Name) {
			t.Errorf("tool %s is not documented in docs/tools.md", tool.Name)
		}
	}

	for _, name := range documented {
		if !slices.Contains(registered, name) {
			t.Errorf("docs/tools.md documents %s, which is not registered", name)
		}
	}
}
