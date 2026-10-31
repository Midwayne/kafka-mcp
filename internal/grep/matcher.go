package grep

import (
	"github.com/Midwayne/kafka-mcp/internal/record"
)

// maxSnippetChars caps each reported snippet so long regex matches stay small.
const maxSnippetChars = 200

// Hit is one matched snippet and the scope it was found in.
type Hit struct {
	In    string `json:"in"`
	Match string `json:"match"`
}

// Match implements scan.Matcher. Scopes are extracted lazily and in order, so
// with "any" semantics a raw-bytes hit never pays for decoding a later scope.
func (m *Matcher) Match(message *record.Message) (bool, error) {
	return m.matches(message) != m.invert, nil
}

func (m *Matcher) matches(message *record.Message) bool {
	var satisfied uint64

	want := uint64(1)<<len(m.patterns) - 1

	for _, sc := range m.scopes {
		for _, chunk := range sc.fromMessage(message) {
			for i, p := range m.patterns {
				bit := uint64(1) << i
				if satisfied&bit != 0 || !p.match(chunk) {
					continue
				}

				if !m.all {
					return true
				}

				if satisfied |= bit; satisfied == want {
					return true
				}
			}
		}
	}

	return false
}

// Hits returns up to limit matched snippets found in the rendered (redacted)
// envelope. Inverted matchers have no snippets to report.
func (m *Matcher) Hits(envelope map[string]any, limit int) []Hit {
	if m.invert || limit <= 0 {
		return nil
	}

	var hits []Hit

	for _, sc := range m.scopes {
		for _, chunk := range sc.fromEnvelope(envelope) {
			for _, p := range m.patterns {
				for _, found := range p.findAll(chunk, limit-len(hits)) {
					hits = append(hits, Hit{In: sc.name, Match: record.TruncateString(string(found), maxSnippetChars)})
				}

				if len(hits) >= limit {
					return hits
				}
			}
		}
	}

	return hits
}

// Renderer adds grep snippets to another renderer's output. With OnlyMatching
// set, it returns only coordinates plus snippets, like grep -o.
type Renderer struct {
	Base         record.Renderer
	Matcher      *Matcher
	OnlyMatching bool
	MaxHits      int
	Redact       []string
}

// Render implements record.Renderer.
func (r Renderer) Render(message *record.Message) map[string]any {
	redacted := record.View{Redact: r.Redact, MaxItems: -1}.Render(message)
	hits := r.Matcher.Hits(redacted, r.MaxHits)

	out := redacted
	if r.OnlyMatching {
		out = record.Coordinates(message)
	} else if r.Base != nil {
		out = r.Base.Render(message)
	}

	out["grep"] = hits

	return out
}
