// Package infer summarizes the structure of decoded messages: field paths,
// types, presence, and example values. It lets an agent write filters after
// reading a compact schema instead of many raw messages.
package infer

import (
	"cmp"
	"fmt"
	"slices"

	"github.com/Midwayne/kafka-mcp/internal/record"
)

const (
	maxExamples     = 3
	maxExampleChars = 80
)

// Field describes one path, e.g. "value.items[].sku".
type Field struct {
	Path     string         `json:"path"`
	Types    map[string]int `json:"types"`
	Coverage float64        `json:"coverage"`
	Examples []any          `json:"examples,omitempty"`
}

// Schema is the inferred structure of a set of messages.
type Schema struct {
	Messages  int     `json:"messages"`
	Fields    []Field `json:"fields"`
	Truncated bool    `json:"truncated,omitempty"`
}

type fieldStats struct {
	field   Field
	present int
	seen    map[string]bool
}

// Inferrer accumulates statistics. It is not safe for concurrent use.
type Inferrer struct {
	maxFields int
	maxDepth  int

	messages  int
	fields    map[string]*fieldStats
	truncated bool
}

// New creates an inferrer that tracks at most maxFields paths up to maxDepth levels deep.
func New(maxFields, maxDepth int) *Inferrer {
	return &Inferrer{maxFields: maxFields, maxDepth: maxDepth, fields: map[string]*fieldStats{}}
}

// Add records one message's value under the given root path (e.g. "value").
func (in *Inferrer) Add(root string, value any) {
	in.messages++
	in.walk(root, value, 0, map[string]bool{})
}

// AddMessage records a message's key, value, and header names.
func (in *Inferrer) AddMessage(m *record.Message) {
	visited := map[string]bool{}
	in.messages++

	in.walk("key", m.Key().Value, 0, visited)
	in.walk("value", m.Value().Value, 0, visited)

	for name, value := range m.Headers() {
		in.walk("headers."+name, value, 0, visited)
	}
}

func (in *Inferrer) walk(path string, value any, depth int, visited map[string]bool) {
	if !in.record(path, value, visited) || depth >= in.maxDepth {
		return
	}

	switch typed := value.(type) {
	case map[string]any:
		for key, child := range typed {
			in.walk(path+"."+key, child, depth+1, visited)
		}
	case []any:
		for _, child := range typed {
			in.walk(path+"[]", child, depth+1, visited)
		}
	}
}

// record updates the stats for path and reports whether the field is tracked.
func (in *Inferrer) record(path string, value any, visited map[string]bool) bool {
	stats, ok := in.fields[path]
	if !ok {
		if len(in.fields) >= in.maxFields {
			in.truncated = true
			return false
		}

		stats = &fieldStats{field: Field{Path: path, Types: map[string]int{}}, seen: map[string]bool{}}
		in.fields[path] = stats
	}

	stats.field.Types[typeName(value)]++

	if !visited[path] {
		visited[path] = true
		stats.present++
	}

	stats.addExample(value)

	return true
}

func (s *fieldStats) addExample(value any) {
	switch value.(type) {
	case map[string]any, []any, nil:
		return
	}

	if len(s.field.Examples) >= maxExamples {
		return
	}

	if text, ok := value.(string); ok {
		value = record.TruncateString(text, maxExampleChars)
	}

	key := fmt.Sprint(value)
	if !s.seen[key] {
		s.seen[key] = true
		s.field.Examples = append(s.field.Examples, value)
	}
}

// Result returns fields sorted by path.
func (in *Inferrer) Result() Schema {
	schema := Schema{Messages: in.messages, Truncated: in.truncated, Fields: make([]Field, 0, len(in.fields))}

	for _, stats := range in.fields {
		field := stats.field
		if in.messages > 0 {
			field.Coverage = float64(stats.present) / float64(in.messages)
		}

		schema.Fields = append(schema.Fields, field)
	}

	slices.SortFunc(schema.Fields, func(a, b Field) int { return cmp.Compare(a.Path, b.Path) })

	return schema
}
