package record

import (
	"time"

	"github.com/Midwayne/kafka-mcp/internal/codec"
)

// DefaultMaxItems caps arrays in output so one huge message cannot flood the agent.
const DefaultMaxItems = 50

// View controls how a message is rendered for an agent.
type View struct {
	Fields        []string // dot paths to keep; empty keeps the full envelope
	Redact        []string // dot paths to mask; applied before projection
	MaxValueChars int      // truncate longer strings; 0 disables
	MaxItems      int      // truncate longer arrays; 0 uses DefaultMaxItems
}

// Envelope returns the full, untruncated message as JSON-compatible data.
func (m *Message) Envelope() map[string]any {
	key, value := m.Key(), m.Value()

	envelope := Coordinates(m)
	envelope["key"] = key.Value
	envelope["value"] = value.Value

	if headers := m.Headers(); len(headers) > 0 {
		asAny := make(map[string]any, len(headers))
		for name, text := range headers {
			asAny[name] = text
		}
		envelope["headers"] = asAny
	}

	if errs := decodeErrors(key, value); errs != nil {
		envelope["decode_errors"] = errs
	}

	return envelope
}

// Renderer turns a message into the JSON-compatible data returned to agents.
type Renderer interface {
	Render(m *Message) map[string]any
}

// Render applies redaction, projection, and truncation to the envelope.
func (view View) Render(m *Message) map[string]any {
	envelope := m.Envelope()

	for _, path := range view.Redact {
		Redact(envelope, path)
	}

	if len(view.Fields) > 0 {
		envelope = Project(envelope, view.Fields)
	}

	maxItems := view.MaxItems
	if maxItems == 0 {
		maxItems = DefaultMaxItems
	}

	truncated, _ := Truncate(envelope, view.MaxValueChars, maxItems).(map[string]any)

	return truncated
}

// Coordinates returns the fields that locate a message: topic, partition, offset, and timestamp.
func Coordinates(m *Message) map[string]any {
	return map[string]any{
		"topic":     m.Record.Topic,
		"partition": m.Record.Partition,
		"offset":    m.Record.Offset,
		"timestamp": m.Record.Timestamp.UTC().Format(time.RFC3339Nano),
	}
}

// Project keeps message coordinates plus the requested paths, keyed by path.
func Project(envelope map[string]any, fields []string) map[string]any {
	out := map[string]any{
		"topic":     envelope["topic"],
		"partition": envelope["partition"],
		"offset":    envelope["offset"],
		"timestamp": envelope["timestamp"],
	}

	for _, field := range fields {
		if value, ok := Lookup(envelope, field); ok {
			out[field] = value
		}
	}

	return out
}

func decodeErrors(key, value codec.Decoded) map[string]any {
	out := map[string]any{}

	if key.Err != "" {
		out["key"] = key.Err
	}

	if value.Err != "" {
		out["value"] = value.Err
	}

	if len(out) == 0 {
		return nil
	}

	return out
}
