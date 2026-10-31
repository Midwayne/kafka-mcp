package codec

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"strconv"
)

// DecodeJSON parses a JSON document. Integers that fit in int64 stay exact
// (large IDs are common in events); other numbers become float64.
func DecodeJSON(data []byte) (any, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()

	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}

	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return nil, errors.New("unexpected data after the JSON document")
	}

	return normalizeNumbers(value), nil
}

func normalizeNumbers(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		for key, item := range typed {
			typed[key] = normalizeNumbers(item)
		}
		return typed
	case []any:
		for i, item := range typed {
			typed[i] = normalizeNumbers(item)
		}
		return typed
	case json.Number:
		if n, err := strconv.ParseInt(typed.String(), 10, 64); err == nil {
			return n
		}

		f, _ := typed.Float64()
		return f
	default:
		return value
	}
}

// looksLikeJSON cheaply rejects payloads that cannot be JSON before parsing.
func looksLikeJSON(data []byte) bool {
	trimmed := bytes.TrimLeft(data, " \t\r\n")
	if len(trimmed) == 0 {
		return false
	}

	switch c := trimmed[0]; {
	case c == '{' || c == '[' || c == '"' || c == '-':
		return true
	case c >= '0' && c <= '9':
		return true
	default:
		return bytes.Equal(trimmed, []byte("true")) || bytes.Equal(trimmed, []byte("false")) ||
			bytes.Equal(trimmed, []byte("null"))
	}
}

// registryHeaderSize is the magic byte plus a big-endian 4-byte schema ID.
const registryHeaderSize = 5

// IsRegistryFramed reports whether data starts with the Confluent wire-format header.
func IsRegistryFramed(data []byte) bool {
	return len(data) >= registryHeaderSize && data[0] == 0
}

// SplitRegistryFrame returns the schema ID and the payload after the header.
func SplitRegistryFrame(data []byte) (schemaID int, payload []byte, err error) {
	if !IsRegistryFramed(data) {
		return 0, nil, errors.New("payload is not Schema Registry framed (missing magic byte 0)")
	}

	return int(binary.BigEndian.Uint32(data[1:registryHeaderSize])), data[registryHeaderSize:], nil
}
