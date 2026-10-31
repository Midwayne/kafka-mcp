package schemaregistry

import (
	"errors"
	"fmt"

	"github.com/linkedin/goavro/v2"
	"github.com/twmb/franz-go/pkg/sr"

	"github.com/Midwayne/kafka-mcp/internal/codec"
)

// avroDecoder renders Avro as standard JSON: unions appear as plain values
// rather than {"type": value} wrappers, which keeps filters natural.
type avroDecoder struct {
	codec *goavro.Codec
}

func newAvroDecoder(schema sr.Schema) (payloadDecoder, error) {
	if len(schema.References) > 0 {
		return nil, errors.New("avro schemas with references are not supported yet")
	}

	avro, err := goavro.NewCodecForStandardJSONFull(schema.Schema)
	if err != nil {
		return nil, fmt.Errorf("parse avro schema: %w", err)
	}

	return avroDecoder{codec: avro}, nil
}

func (a avroDecoder) decode(payload []byte) (any, error) {
	native, rest, err := a.codec.NativeFromBinary(payload)
	if err != nil {
		return nil, err
	}

	if len(rest) > 0 {
		return nil, fmt.Errorf("%d unexpected trailing bytes", len(rest))
	}

	textual, err := a.codec.TextualFromNative(nil, native)
	if err != nil {
		return nil, err
	}

	return codec.DecodeJSON(textual)
}

func (avroDecoder) format() string { return codec.FormatAvro }
