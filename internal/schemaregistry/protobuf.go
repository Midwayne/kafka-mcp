package schemaregistry

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/bufbuild/protocompile"
	"github.com/twmb/franz-go/pkg/sr"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"

	"github.com/Midwayne/kafka-mcp/internal/codec"
)

const (
	maxReferenceDepth = 16
	maxMessageIndexes = 64
)

// protobufDecoder decodes Confluent protobuf framing: message indexes, then the message.
// Values follow the proto3 JSON mapping, so 64-bit integers appear as strings.
type protobufDecoder struct {
	file protoreflect.FileDescriptor
}

func newProtobufDecoder(ctx context.Context, c *Client, id int, schema sr.Schema) (payloadDecoder, error) {
	root := fmt.Sprintf("kafka-mcp/schema-%d.proto", id)
	sources := map[string]string{root: schema.Schema}

	if err := c.collectReferences(ctx, schema.References, sources, 0); err != nil {
		return nil, err
	}

	compiler := protocompile.Compiler{
		Resolver: protocompile.WithStandardImports(&protocompile.SourceResolver{
			Accessor: protocompile.SourceAccessorFromMap(sources),
		}),
	}

	files, err := compiler.Compile(ctx, root)
	if err != nil {
		return nil, fmt.Errorf("compile protobuf schema: %w", err)
	}

	return protobufDecoder{file: files[0]}, nil
}

func (c *Client) collectReferences(ctx context.Context, refs []sr.SchemaReference, sources map[string]string, depth int) error {
	if depth > maxReferenceDepth {
		return errors.New("protobuf schema references are nested too deeply")
	}

	for _, ref := range refs {
		if _, seen := sources[ref.Name]; seen {
			continue
		}

		referenced, err := c.SchemaByVersion(ctx, ref.Subject, ref.Version)
		if err != nil {
			return err
		}

		sources[ref.Name] = referenced.Schema.Schema

		if err := c.collectReferences(ctx, referenced.References, sources, depth+1); err != nil {
			return err
		}
	}

	return nil
}

func (p protobufDecoder) decode(payload []byte) (any, error) {
	indexes, body, err := readMessageIndexes(payload)
	if err != nil {
		return nil, err
	}

	descriptor, err := messageAt(p.file, indexes)
	if err != nil {
		return nil, err
	}

	message := dynamicpb.NewMessage(descriptor)
	if err := proto.Unmarshal(body, message); err != nil {
		return nil, fmt.Errorf("unmarshal %s: %w", descriptor.FullName(), err)
	}

	rendered, err := protojson.MarshalOptions{UseProtoNames: true}.Marshal(message)
	if err != nil {
		return nil, err
	}

	return codec.DecodeJSON(rendered)
}

func (protobufDecoder) format() string { return codec.FormatProtobuf }

// readMessageIndexes parses the zigzag varint index path. A count of zero is
// shorthand for the first message in the file.
func readMessageIndexes(data []byte) ([]int, []byte, error) {
	count, n := binary.Varint(data)
	if n <= 0 || count < 0 || count > maxMessageIndexes {
		return nil, nil, errors.New("invalid protobuf message index header")
	}
	data = data[n:]

	if count == 0 {
		return []int{0}, data, nil
	}

	indexes := make([]int, 0, count)
	for range count {
		index, read := binary.Varint(data)
		if read <= 0 {
			return nil, nil, errors.New("truncated protobuf message index")
		}

		indexes = append(indexes, int(index))
		data = data[read:]
	}

	return indexes, data, nil
}

func messageAt(file protoreflect.FileDescriptor, indexes []int) (protoreflect.MessageDescriptor, error) {
	messages := file.Messages()

	var descriptor protoreflect.MessageDescriptor
	for _, index := range indexes {
		if index < 0 || index >= messages.Len() {
			return nil, fmt.Errorf("message index %v not found in schema", indexes)
		}

		descriptor = messages.Get(index)
		messages = descriptor.Messages()
	}

	return descriptor, nil
}
