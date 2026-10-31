package codec

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/Midwayne/kafka-mcp/internal/config"
)

type fakeResolver struct{}

func (fakeResolver) Decode(_ context.Context, id int, payload []byte) (any, string, error) {
	if id != 7 {
		return nil, "", errors.New("unknown schema")
	}

	return map[string]any{"payload": string(payload)}, FormatAvro, nil
}

func TestDecode(t *testing.T) {
	framed := []byte{0, 0, 0, 0, 7, 'h', 'i'}
	unknownFramed := []byte{0, 0, 0, 0, 9, 'h', 'i'}
	binary := []byte{0xff, 0xfe}

	cases := []struct {
		name     string
		registry SchemaResolver
		format   string
		data     []byte
		want     Decoded
	}{
		{"nil", nil, config.FormatAuto, nil, Decoded{Format: FormatNull}},
		{"auto json keeps big ints exact", nil, config.FormatAuto, []byte(`{"id":9007199254740993,"p":1.5}`),
			Decoded{Value: map[string]any{"id": int64(9007199254740993), "p": 1.5}, Format: config.FormatJSON}},
		{"auto text", nil, config.FormatAuto, []byte("hello"), Decoded{Value: "hello", Format: config.FormatString}},
		{"auto json-ish text", nil, config.FormatAuto, []byte("{oops"), Decoded{Value: "{oops", Format: config.FormatString}},
		{"auto binary", nil, config.FormatAuto, binary, Decoded{Value: "//4=", Format: config.FormatBase64}},
		{"auto registry", fakeResolver{}, config.FormatAuto, framed,
			Decoded{Value: map[string]any{"payload": "hi"}, Format: FormatAvro, SchemaID: 7}},
		{"auto registry failure falls back", fakeResolver{}, config.FormatAuto, unknownFramed,
			Decoded{Value: "AAAAAAloaQ==", Format: config.FormatBase64}},
		{"explicit json invalid", nil, config.FormatJSON, []byte("nope"),
			Decoded{Value: "nope", Format: config.FormatString, Err: "invalid JSON: invalid character 'o' in literal null (expecting 'u')"}},
		{"json trailing data", nil, config.FormatJSON, []byte(`{} {}`),
			Decoded{Value: "{} {}", Format: config.FormatString, Err: "invalid JSON: unexpected data after the JSON document"}},
		{"hex", nil, config.FormatHex, []byte{0xab}, Decoded{Value: "ab", Format: config.FormatHex}},
		{"base64", nil, config.FormatBase64, []byte("hi"), Decoded{Value: "aGk=", Format: config.FormatBase64}},
		{"none", nil, config.FormatNone, []byte("x"), Decoded{Format: config.FormatNone}},
		{"registry without config", nil, config.FormatSchemaRegistry, framed,
			Decoded{Value: "AAAAAAdoaQ==", Format: config.FormatBase64, Err: "schema_registry format requires a schema_registry block on the cluster"}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := NewDecoder(tc.registry).Decode(context.Background(), tc.data, tc.format)

			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("Decode() = %#v\nwant %#v", got, tc.want)
			}
		})
	}
}

func TestSplitRegistryFrame(t *testing.T) {
	id, payload, err := SplitRegistryFrame([]byte{0, 0, 0, 1, 0, 'x'})
	if err != nil || id != 256 || string(payload) != "x" {
		t.Fatalf("SplitRegistryFrame = %d, %q, %v", id, payload, err)
	}

	if _, _, err := SplitRegistryFrame([]byte{1, 0, 0, 0, 0}); err == nil {
		t.Fatal("expected error for missing magic byte")
	}
}
