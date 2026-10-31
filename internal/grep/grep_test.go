package grep

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Midwayne/kafka-mcp/internal/codec"
	"github.com/Midwayne/kafka-mcp/internal/config"
	"github.com/Midwayne/kafka-mcp/internal/record"
)

const payload = `{"status":"FAILED","reason":"Timeout talking to WMS","user":{"ssn":"123-45-6789"},"code":"ERR-504"}`

func message() *record.Message {
	return record.New(&kgo.Record{
		Topic: "trailers", Partition: 1, Offset: 7, Key: []byte("store-1375"), Value: []byte(payload),
		Timestamp: time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC),
		Headers:   []kgo.RecordHeader{{Key: "event-type", Value: []byte("TrailerClosed")}},
	}, &record.Source{Ctx: context.Background(), Decoder: codec.NewDecoder(nil), ValueFormat: config.FormatJSON})
}

func TestMatch(t *testing.T) {
	cases := []struct {
		name string
		spec Spec
		want bool
	}{
		{"regex in raw value", Spec{Patterns: []string{`ERR-\d{3}`}}, true},
		{"fixed string with regex metacharacters", Spec{Patterns: []string{`ERR-\d`}, FixedStrings: true}, false},
		{"case sensitive by default", Spec{Patterns: []string{"timeout"}}, false},
		{"ignore case", Spec{Patterns: []string{"timeout"}, IgnoreCase: true}, true},
		{"fixed + ignore case", Spec{Patterns: []string{"wms"}, FixedStrings: true, IgnoreCase: true}, true},
		{"word rejects partial", Spec{Patterns: []string{"Time"}, Word: true}, false},
		{"word accepts whole", Spec{Patterns: []string{"Timeout"}, Word: true}, true},
		{"any of several", Spec{Patterns: []string{"nope", "FAILED"}}, true},
		{"all requires each", Spec{Patterns: []string{"nope", "FAILED"}, All: true}, false},
		{"all across scopes", Spec{Patterns: []string{"store-", "TrailerClosed", "FAILED"}, All: true}, true},
		{"invert", Spec{Patterns: []string{"FAILED"}, Invert: true}, false},
		{"invert non-match", Spec{Patterns: []string{"SUCCESS"}, Invert: true}, true},
		{"key scope only", Spec{Patterns: []string{"FAILED"}, In: []string{ScopeKey}}, false},
		{"header line", Spec{Patterns: []string{"^event-type: Trailer"}, In: []string{ScopeHeaders}}, true},
		{"field scope", Spec{Patterns: []string{"^FAILED$"}, In: []string{"value.status"}}, true},
		{"field scope miss", Spec{Patterns: []string{"FAILED"}, In: []string{"value.reason"}}, false},
		{"nested object as JSON", Spec{Patterns: []string{`"ssn":"\d{3}`}, In: []string{"value.user"}}, true},
		{"single header", Spec{Patterns: []string{"Closed$"}, In: []string{"headers.event-type"}}, true},
		{"decoded value", Spec{Patterns: []string{`"code":"ERR-504"`}, In: []string{ScopeDecodedValue}}, true},
		{"missing field", Spec{Patterns: []string{"x"}, In: []string{"value.nope.deeper"}}, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			matcher, err := Compile(tc.spec)
			if err != nil {
				t.Fatal(err)
			}

			if got, _ := matcher.Match(message()); got != tc.want {
				t.Fatalf("Match = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestCompileErrors(t *testing.T) {
	cases := map[string]Spec{
		"no patterns":   {},
		"empty pattern": {Patterns: []string{""}},
		"bad regex":     {Patterns: []string{"("}},
		"bad scope":     {Patterns: []string{"x"}, In: []string{"body"}},
		"bad root":      {Patterns: []string{"x"}, In: []string{"topic.name"}},
		"too long":      {Patterns: []string{strings.Repeat("a", MaxPatternLength+1)}},
		"too many":      {Patterns: make([]string, MaxPatterns+1)},
	}

	for name, spec := range cases {
		if _, err := Compile(spec); err == nil {
			t.Fatalf("%s: expected error", name)
		}
	}
}

func TestOnlyMatchingRendersSnippetsFromRedactedView(t *testing.T) {
	matcher, err := Compile(Spec{Patterns: []string{`\d{3}-\d{2}-\d{4}`, `ERR-\d+`}, In: []string{ScopeValue}})
	if err != nil {
		t.Fatal(err)
	}

	out := Renderer{Matcher: matcher, OnlyMatching: true, MaxHits: 5, Redact: []string{"value.user.ssn"}}.Render(message())

	want := map[string]any{
		"topic": "trailers", "partition": int32(1), "offset": int64(7), "timestamp": "2026-05-01T00:00:00Z",
		"grep": []Hit{{In: ScopeValue, Match: "ERR-504"}},
	}

	if !reflect.DeepEqual(out, want) {
		t.Fatalf("Render() = %#v\nwant %#v", out, want)
	}
}

func TestRendererDecoratesBaseOutput(t *testing.T) {
	matcher, err := Compile(Spec{Patterns: []string{"FAILED"}})
	if err != nil {
		t.Fatal(err)
	}

	out := Renderer{Base: record.View{Fields: []string{"value.status"}}, Matcher: matcher, MaxHits: 1}.Render(message())

	if out["value.status"] != "FAILED" || !reflect.DeepEqual(out["grep"], []Hit{{In: ScopeValue, Match: "FAILED"}}) {
		t.Fatalf("unexpected output: %#v", out)
	}
}

func TestLiteralFindAllRespectsLimit(t *testing.T) {
	hits := literal("ab").findAll([]byte("ab-ab-ab"), 2)
	if len(hits) != 2 || string(hits[1]) != "ab" {
		t.Fatalf("findAll = %q", hits)
	}
}
