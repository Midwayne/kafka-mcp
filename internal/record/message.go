// Package record wraps fetched Kafka records with lazy decoding and builds
// the bounded, redacted views returned to agents.
package record

import (
	"context"
	"encoding/base64"
	"unicode/utf8"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Midwayne/kafka-mcp/internal/codec"
)

// Source describes how to decode records from one topic.
type Source struct {
	Ctx         context.Context //nolint:containedctx // decoding is invoked lazily from CEL activations
	Decoder     *codec.Decoder
	KeyFormat   string
	ValueFormat string
}

// Message is a record whose key, value, and headers are decoded on first access.
// A Message is used by one goroutine at a time.
type Message struct {
	Record *kgo.Record

	source  *Source
	key     *codec.Decoded
	value   *codec.Decoded
	headers map[string]string
}

// New wraps a record. Nothing is decoded until requested.
func New(r *kgo.Record, source *Source) *Message {
	return &Message{Record: r, source: source}
}

// Key returns the decoded key.
func (m *Message) Key() codec.Decoded {
	if m.key == nil {
		decoded := m.source.Decoder.Decode(m.source.Ctx, m.Record.Key, m.source.KeyFormat)
		m.key = &decoded
	}

	return *m.key
}

// Value returns the decoded value.
func (m *Message) Value() codec.Decoded {
	if m.value == nil {
		decoded := m.source.Decoder.Decode(m.source.Ctx, m.Record.Value, m.source.ValueFormat)
		m.value = &decoded
	}

	return *m.value
}

// Headers returns headers as strings. Non-text header values are base64
// encoded; for repeated header keys the last value wins.
func (m *Message) Headers() map[string]string {
	if m.headers == nil {
		m.headers = make(map[string]string, len(m.Record.Headers))

		for _, header := range m.Record.Headers {
			m.headers[header.Key] = headerText(header.Value)
		}
	}

	return m.headers
}

// TimestampMs returns the record timestamp in Unix milliseconds.
func (m *Message) TimestampMs() int64 { return m.Record.Timestamp.UnixMilli() }

// RawKey returns the key bytes as a string without decoding.
func (m *Message) RawKey() string { return string(m.Record.Key) }

// RawValue returns the value bytes as a string without decoding.
func (m *Message) RawValue() string { return string(m.Record.Value) }

// Size returns the key plus value size in bytes.
func (m *Message) Size() int { return len(m.Record.Key) + len(m.Record.Value) }

func headerText(value []byte) string {
	if utf8.Valid(value) {
		return string(value)
	}

	return base64.StdEncoding.EncodeToString(value)
}
