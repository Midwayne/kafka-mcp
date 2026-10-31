package record

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Midwayne/kafka-mcp/internal/codec"
	"github.com/Midwayne/kafka-mcp/internal/config"
)

type countingResolver struct{ calls int }

func (c *countingResolver) Decode(context.Context, int, []byte) (any, string, error) {
	c.calls++
	return map[string]any{"n": int64(1)}, codec.FormatAvro, nil
}

func newMessage(value string, source *Source) *Message {
	return New(&kgo.Record{
		Topic: "orders", Partition: 2, Offset: 41, Key: []byte("k1"), Value: []byte(value),
		Timestamp: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC),
		Headers:   []kgo.RecordHeader{{Key: "trace", Value: []byte("abc")}, {Key: "bin", Value: []byte{0xff}}},
	}, source)
}

func jsonSource() *Source {
	return &Source{Ctx: context.Background(), Decoder: codec.NewDecoder(nil), KeyFormat: config.FormatString, ValueFormat: config.FormatJSON}
}

func TestValueIsDecodedLazilyAndOnce(t *testing.T) {
	resolver := &countingResolver{}
	source := &Source{Ctx: context.Background(), Decoder: codec.NewDecoder(resolver), ValueFormat: config.FormatSchemaRegistry}
	message := newMessage("\x00\x00\x00\x00\x01x", source)

	_ = message.Headers()
	_ = message.RawValue()

	if resolver.calls != 0 {
		t.Fatal("value decoded before it was requested")
	}

	for range 3 {
		_ = message.Value()
	}

	if resolver.calls != 1 {
		t.Fatalf("value decoded %d times, want 1", resolver.calls)
	}
}

func TestRenderFullEnvelope(t *testing.T) {
	message := newMessage(`{"user":{"ssn":"123","name":"Ann"},"cards":[{"pan":"4111"},{"pan":"5500"}]}`, jsonSource())

	got := View{Redact: []string{"value.user.ssn", "value.cards.*.pan", "headers.trace"}}.Render(message)

	want := map[string]any{
		"topic": "orders", "partition": int32(2), "offset": int64(41), "timestamp": "2026-01-02T03:04:05Z",
		"key": "k1",
		"value": map[string]any{
			"user":  map[string]any{"ssn": RedactedValue, "name": "Ann"},
			"cards": []any{map[string]any{"pan": RedactedValue}, map[string]any{"pan": RedactedValue}},
		},
		"headers": map[string]any{"trace": RedactedValue, "bin": "/w=="},
	}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Render() =\n%#v\nwant\n%#v", got, want)
	}
}

func TestRenderProjectionCannotBypassRedaction(t *testing.T) {
	message := newMessage(`{"user":{"ssn":"123","name":"Ann"},"items":[1,2]}`, jsonSource())

	got := View{Fields: []string{"value.user.ssn", "value.items.1", "value.missing"}, Redact: []string{"value.user.ssn"}}.Render(message)

	if got["value.user.ssn"] != RedactedValue || got["value.items.1"] != int64(2) || got["offset"] != int64(41) {
		t.Fatalf("unexpected projection: %#v", got)
	}

	if _, ok := got["value.missing"]; ok || got["value"] != nil {
		t.Fatalf("projection should only include found fields: %#v", got)
	}
}

func TestRenderTruncatesAndReportsDecodeErrors(t *testing.T) {
	long := strings.Repeat("é", 30)
	message := newMessage(`{"s":"`+long+`","list":[1,2,3,4]}`, jsonSource())

	got := View{MaxValueChars: 10, MaxItems: 2}.Render(message)
	value := got["value"].(map[string]any)

	if value["s"] != strings.Repeat("é", 10)+"…(+20 chars)" {
		t.Fatalf("string truncation = %q", value["s"])
	}

	if !reflect.DeepEqual(value["list"], []any{int64(1), int64(2), "…(+2 items)"}) {
		t.Fatalf("list truncation = %#v", value["list"])
	}

	broken := View{}.Render(newMessage("not json", jsonSource()))
	if broken["decode_errors"] == nil || broken["value"] != "not json" {
		t.Fatalf("expected decode error with raw fallback: %#v", broken)
	}
}

func TestLookup(t *testing.T) {
	root := map[string]any{"a": []any{map[string]any{"b": "x"}}}

	if value, ok := Lookup(root, "a.0.b"); !ok || value != "x" {
		t.Fatalf("Lookup = %v, %v", value, ok)
	}

	for _, path := range []string{"a.1.b", "a.x", "missing", "a.0.b.c"} {
		if _, ok := Lookup(root, path); ok {
			t.Fatalf("Lookup(%s) should fail", path)
		}
	}
}
