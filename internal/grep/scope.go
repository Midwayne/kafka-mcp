package grep

import (
	"bytes"
	"encoding/json"
	"fmt"
	"slices"
	"strings"

	"github.com/Midwayne/kafka-mcp/internal/record"
)

// Scope names.
const (
	ScopeKey          = "key"
	ScopeValue        = "value"
	ScopeHeaders      = "headers"
	ScopeDecodedKey   = "decoded_key"
	ScopeDecodedValue = "decoded_value"
)

// DefaultScopes searches the whole raw message, like grepping a full line.
var DefaultScopes = []string{ScopeKey, ScopeValue, ScopeHeaders}

// scope extracts searchable text. fromMessage feeds matching (raw bytes where
// possible, for speed); fromEnvelope feeds reported snippets, so snippets come
// from the same redacted view the agent is shown.
type scope struct {
	name         string
	fromMessage  func(*record.Message) [][]byte
	fromEnvelope func(map[string]any) [][]byte
}

func parseScope(name string) (scope, error) {
	switch name {
	case ScopeKey:
		return scope{name, func(m *record.Message) [][]byte { return nonEmpty(m.Record.Key) }, envelopeField(ScopeKey)}, nil
	case ScopeValue:
		return scope{name, func(m *record.Message) [][]byte { return nonEmpty(m.Record.Value) }, envelopeField(ScopeValue)}, nil
	case ScopeHeaders:
		return scope{name, func(m *record.Message) [][]byte { return headerLines(m.Headers()) }, envelopeHeaders}, nil
	case ScopeDecodedKey:
		return scope{name, func(m *record.Message) [][]byte { return text(m.Key().Value) }, envelopeField(ScopeKey)}, nil
	case ScopeDecodedValue:
		return scope{name, func(m *record.Message) [][]byte { return text(m.Value().Value) }, envelopeField(ScopeValue)}, nil
	}

	return fieldScope(name)
}

// fieldScope searches one decoded field, e.g. "value.order.status" or "headers.trace-id".
func fieldScope(name string) (scope, error) {
	root, path, ok := strings.Cut(name, ".")
	if !ok || path == "" {
		return scope{}, fmt.Errorf("unknown grep scope %q (key, value, headers, decoded_key, decoded_value, value.<path>, key.<path>, headers.<name>)", name)
	}

	fromEnvelope := func(envelope map[string]any) [][]byte {
		found, ok := record.Lookup(envelope, name)
		if !ok {
			return nil
		}

		return text(found)
	}

	switch root {
	case ScopeValue:
		return scope{name, func(m *record.Message) [][]byte { return lookupText(m.Value().Value, path) }, fromEnvelope}, nil
	case ScopeKey:
		return scope{name, func(m *record.Message) [][]byte { return lookupText(m.Key().Value, path) }, fromEnvelope}, nil
	case ScopeHeaders:
		return scope{name, func(m *record.Message) [][]byte { return headerText(m.Headers(), path) }, fromEnvelope}, nil
	default:
		return scope{}, fmt.Errorf("grep scope %q must be a value, key, or headers path such as value.status", name)
	}
}

func envelopeField(field string) func(map[string]any) [][]byte {
	return func(envelope map[string]any) [][]byte { return text(envelope[field]) }
}

func envelopeHeaders(envelope map[string]any) [][]byte {
	headers, _ := envelope[ScopeHeaders].(map[string]any)

	lines := make(map[string]string, len(headers))
	for name, value := range headers {
		lines[name] = fmt.Sprint(value)
	}

	return headerLines(lines)
}

func lookupText(root any, path string) [][]byte {
	found, ok := record.Lookup(root, path)
	if !ok {
		return nil
	}

	return text(found)
}

func headerText(headers map[string]string, name string) [][]byte {
	value, ok := headers[name]
	if !ok {
		return nil
	}

	return [][]byte{[]byte(value)}
}

// headerLines renders headers as sorted "name: value" lines.
func headerLines(headers map[string]string) [][]byte {
	lines := make([][]byte, 0, len(headers))
	for name, value := range headers {
		lines = append(lines, []byte(name+": "+value))
	}

	slices.SortFunc(lines, bytes.Compare)

	return lines
}

// text renders strings as-is and other values as compact JSON.
func text(value any) [][]byte {
	switch typed := value.(type) {
	case nil:
		return nil
	case string:
		return [][]byte{[]byte(typed)}
	default:
		encoded, err := json.Marshal(typed)
		if err != nil {
			return nil
		}

		return [][]byte{encoded}
	}
}

func nonEmpty(data []byte) [][]byte {
	if len(data) == 0 {
		return nil
	}

	return [][]byte{data}
}
