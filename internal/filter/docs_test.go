package filter

import (
	"os"
	"regexp"
	"strings"
	"testing"
)

var celBlock = regexp.MustCompile("(?s)```cel\n(.*?)```")

// TestDocumentedExpressionsCompile keeps docs/filtering.md accurate.
func TestDocumentedExpressionsCompile(t *testing.T) {
	doc, err := os.ReadFile("../../docs/filtering.md")
	if err != nil {
		t.Fatal(err)
	}

	blocks := celBlock.FindAllStringSubmatch(string(doc), -1)
	if len(blocks) == 0 {
		t.Fatal("no cel blocks found in docs/filtering.md")
	}

	compiled := 0

	for _, block := range blocks {
		for line := range strings.SplitSeq(block[1], "\n") {
			expression, _, _ := strings.Cut(line, "//")
			if expression = strings.TrimSpace(expression); expression == "" {
				continue
			}

			if _, err := CompileValue(expression); err != nil {
				t.Errorf("documented expression %q does not compile: %v", expression, err)
			}

			compiled++
		}
	}

	if compiled < 20 {
		t.Fatalf("expected the cookbook to contain at least 20 expressions, found %d", compiled)
	}
}
