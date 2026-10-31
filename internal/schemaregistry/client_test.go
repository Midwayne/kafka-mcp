package schemaregistry

import (
	"context"
	"reflect"
	"sync"
	"testing"

	"github.com/bufbuild/protocompile"
	"github.com/linkedin/goavro/v2"
	"github.com/twmb/franz-go/pkg/sr"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"

	"github.com/Midwayne/kafka-mcp/internal/codec"
	"github.com/Midwayne/kafka-mcp/internal/config"
)

const avroSchema = `{"type":"record","name":"Order","fields":[
	{"name":"id","type":"long"},{"name":"note","type":["null","string"],"default":null}]}`

const commonProto = `syntax = "proto3"; package shop; message Money { int64 cents = 1; string currency = 2; }`

const orderProto = `syntax = "proto3"; package shop; import "common.proto";
message Ignored {}
message Order { string id = 1; Money total = 2; message Line { string sku = 1; } }`

func TestDecodeAvroUnwrapsUnions(t *testing.T) {
	fake, client := newFakeRegistry(t)
	fake.byID[1] = sr.Schema{Schema: avroSchema, Type: sr.TypeAvro}

	avro, err := goavro.NewCodec(avroSchema)
	if err != nil {
		t.Fatal(err)
	}

	body, err := avro.BinaryFromNative(nil, map[string]any{"id": int64(9), "note": goavro.Union("string", "rush")})
	if err != nil {
		t.Fatal(err)
	}

	decoded := codec.NewDecoder(client).Decode(context.Background(), frame(1, nil, body), config.FormatSchemaRegistry)
	want := codec.Decoded{Value: map[string]any{"id": int64(9), "note": "rush"}, Format: codec.FormatAvro, SchemaID: 1}

	if !reflect.DeepEqual(decoded, want) {
		t.Fatalf("got %#v, want %#v", decoded, want)
	}
}

func TestDecodeProtobufWithReferencesAndNestedMessages(t *testing.T) {
	fake, client := newFakeRegistry(t)
	fake.byID[2] = sr.Schema{
		Schema: orderProto, Type: sr.TypeProtobuf,
		References: []sr.SchemaReference{{Name: "common.proto", Subject: "common", Version: 1}},
	}
	fake.versions["common/1"] = sr.SubjectSchema{Subject: "common", Version: 1, Schema: sr.Schema{Schema: commonProto}}

	order := newProtoMessage(t, "shop.Order")
	order.Set(order.Descriptor().Fields().ByName("id"), protoreflect.ValueOfString("o-1"))

	total := order.Mutable(order.Descriptor().Fields().ByName("total")).Message()
	total.Set(total.Descriptor().Fields().ByName("cents"), protoreflect.ValueOfInt64(1250))

	line := newProtoMessage(t, "shop.Order.Line")
	line.Set(line.Descriptor().Fields().ByName("sku"), protoreflect.ValueOfString("abc"))

	cases := map[string]struct {
		payload []byte
		want    any
	}{
		"top-level message": {frame(2, []int64{1}, marshal(t, order)), map[string]any{"id": "o-1", "total": map[string]any{"cents": "1250"}}},
		"nested message":    {frame(2, []int64{1, 0}, marshal(t, line)), map[string]any{"sku": "abc"}},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			value, format, err := client.Decode(context.Background(), 2, tc.payload[5:])
			if err != nil || format != codec.FormatProtobuf || !reflect.DeepEqual(value, tc.want) {
				t.Fatalf("Decode = %#v, %s, %v", value, format, err)
			}
		})
	}
}

func TestDecodeJSONSchema(t *testing.T) {
	fake, client := newFakeRegistry(t)
	fake.byID[3] = sr.Schema{Schema: `{"type":"object"}`, Type: sr.TypeJSON}

	value, format, err := client.Decode(context.Background(), 3, []byte(`{"ok":true}`))
	if err != nil || format != codec.FormatJSONSchema || !reflect.DeepEqual(value, map[string]any{"ok": true}) {
		t.Fatalf("Decode = %#v, %s, %v", value, format, err)
	}
}

func TestDecoderCacheFetchesEachSchemaOnce(t *testing.T) {
	fake, client := newFakeRegistry(t)
	fake.byID[3] = sr.Schema{Schema: `{}`, Type: sr.TypeJSON}

	var wg sync.WaitGroup
	for range 50 {
		wg.Go(func() {
			_, _, _ = client.Decode(context.Background(), 3, []byte(`{}`))
			_, _, _ = client.Decode(context.Background(), 404, []byte(`{}`))
		})
	}
	wg.Wait()

	if hits := fake.hitCount("/schemas/ids/3"); hits != 1 {
		t.Fatalf("schema 3 fetched %d times, want 1", hits)
	}

	if hits := fake.hitCount("/schemas/ids/404"); hits != 1 {
		t.Fatalf("missing schema fetched %d times, want 1 (failures are cached briefly)", hits)
	}
}

func TestRegistryTLSInheritsClusterTLSWithoutServerName(t *testing.T) {
	cfg, err := registryTLS(nil, &config.TLS{ServerName: "broker.internal", MinVersion: "1.3"})
	if err != nil || cfg == nil || cfg.ServerName != "" {
		t.Fatalf("inherited TLS = %+v, %v", cfg, err)
	}

	if cfg, err := registryTLS(nil, nil); cfg != nil || err != nil {
		t.Fatalf("no TLS = %+v, %v", cfg, err)
	}
}

func newProtoMessage(t *testing.T, name protoreflect.FullName) *dynamicpb.Message {
	t.Helper()

	compiler := protocompile.Compiler{Resolver: &protocompile.SourceResolver{
		Accessor: protocompile.SourceAccessorFromMap(map[string]string{"order.proto": orderProto, "common.proto": commonProto}),
	}}

	files, err := compiler.Compile(context.Background(), "order.proto")
	if err != nil {
		t.Fatal(err)
	}

	descriptor, err := files.AsResolver().FindDescriptorByName(name)
	if err != nil {
		t.Fatal(err)
	}

	return dynamicpb.NewMessage(descriptor.(protoreflect.MessageDescriptor))
}

func marshal(t *testing.T, message proto.Message) []byte {
	t.Helper()

	data, err := proto.Marshal(message)
	if err != nil {
		t.Fatal(err)
	}

	return data
}
