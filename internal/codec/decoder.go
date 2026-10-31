// Package codec decodes Kafka keys and values into JSON-compatible Go values.
package codec

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"unicode/utf8"

	"github.com/Midwayne/kafka-mcp/internal/config"
)

// Formats reported in Decoded.Format beyond the configurable ones.
const (
	FormatNull       = "null"
	FormatAvro       = "avro"
	FormatProtobuf   = "protobuf"
	FormatJSONSchema = "json_schema"
)

// Decoded is a decoded key or value. Value holds only JSON-compatible types:
// map[string]any, []any, string, int64, float64, bool, or nil.
type Decoded struct {
	Value    any    `json:"value"`
	Format   string `json:"format"`
	SchemaID int    `json:"schema_id,omitempty"`
	Err      string `json:"error,omitempty"`
}

// SchemaResolver decodes Schema Registry framed payloads.
type SchemaResolver interface {
	Decode(ctx context.Context, schemaID int, payload []byte) (value any, format string, err error)
}

// Decoder converts raw bytes using a named format strategy.
type Decoder struct {
	registry SchemaResolver
}

// NewDecoder returns a decoder. registry may be nil when no Schema Registry is configured.
func NewDecoder(registry SchemaResolver) *Decoder {
	return &Decoder{registry: registry}
}

// Decode never fails: undecodable input falls back to a lossless representation
// and the reason is reported in Decoded.Err.
func (d *Decoder) Decode(ctx context.Context, data []byte, format string) Decoded {
	if data == nil {
		return Decoded{Format: FormatNull}
	}

	switch format {
	case config.FormatNone:
		return Decoded{Format: config.FormatNone}
	case config.FormatJSON:
		return decodeJSONOrFallback(data)
	case config.FormatString:
		return decodeText(data)
	case config.FormatBase64:
		return Decoded{Value: base64.StdEncoding.EncodeToString(data), Format: config.FormatBase64}
	case config.FormatHex:
		return Decoded{Value: hex.EncodeToString(data), Format: config.FormatHex}
	case config.FormatSchemaRegistry:
		return d.decodeRegistry(ctx, data)
	default:
		return d.decodeAuto(ctx, data)
	}
}

func (d *Decoder) decodeAuto(ctx context.Context, data []byte) Decoded {
	if d.registry != nil && IsRegistryFramed(data) {
		if decoded := d.decodeRegistry(ctx, data); decoded.Err == "" {
			return decoded
		}
	}

	if looksLikeJSON(data) {
		if value, err := DecodeJSON(data); err == nil {
			return Decoded{Value: value, Format: config.FormatJSON}
		}
	}

	return decodeText(data)
}

func (d *Decoder) decodeRegistry(ctx context.Context, data []byte) Decoded {
	fallback := decodeText(data)

	if d.registry == nil {
		fallback.Err = "schema_registry format requires a schema_registry block on the cluster"
		return fallback
	}

	schemaID, payload, err := SplitRegistryFrame(data)
	if err != nil {
		fallback.Err = err.Error()
		return fallback
	}

	value, format, err := d.registry.Decode(ctx, schemaID, payload)
	if err != nil {
		fallback.SchemaID, fallback.Err = schemaID, err.Error()
		return fallback
	}

	return Decoded{Value: value, Format: format, SchemaID: schemaID}
}

func decodeJSONOrFallback(data []byte) Decoded {
	value, err := DecodeJSON(data)
	if err == nil {
		return Decoded{Value: value, Format: config.FormatJSON}
	}

	fallback := decodeText(data)
	fallback.Err = "invalid JSON: " + err.Error()

	return fallback
}

// decodeText returns printable UTF-8 as a string and anything else as base64.
func decodeText(data []byte) Decoded {
	if isPrintableText(data) {
		return Decoded{Value: string(data), Format: config.FormatString}
	}

	return Decoded{Value: base64.StdEncoding.EncodeToString(data), Format: config.FormatBase64}
}

// isPrintableText rejects invalid UTF-8 and control characters other than
// whitespace, since binary encodings are often accidentally valid UTF-8.
func isPrintableText(data []byte) bool {
	if !utf8.Valid(data) {
		return false
	}

	for _, r := range string(data) {
		if (r < 0x20 && r != '\t' && r != '\n' && r != '\r') || r == 0x7f {
			return false
		}
	}

	return true
}
