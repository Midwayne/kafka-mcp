package infer

import (
	"reflect"
	"testing"

	"github.com/Midwayne/kafka-mcp/internal/codec"
)

func decode(t *testing.T, raw string) any {
	t.Helper()

	value, err := codec.DecodeJSON([]byte(raw))
	if err != nil {
		t.Fatal(err)
	}

	return value
}

func TestInferFieldsTypesCoverageAndExamples(t *testing.T) {
	inferrer := New(100, 8)

	inferrer.Add("value", decode(t, `{"id":1,"status":"OPEN","items":[{"sku":"a"},{"sku":"b"}],"meta":"{\"x\":1}"}`))
	inferrer.Add("value", decode(t, `{"id":2.5,"status":"OPEN","items":[],"note":null}`))
	inferrer.Add("value", decode(t, `{"id":3,"status":"CLOSED","items":[{"sku":"c","qty":2}]}`))

	fields := map[string]Field{}
	for _, field := range inferrer.Result().Fields {
		fields[field.Path] = field
	}

	cases := map[string]Field{
		"value.id":          {Path: "value.id", Types: map[string]int{TypeInt: 2, TypeFloat: 1}, Coverage: 1, Examples: []any{int64(1), 2.5, int64(3)}},
		"value.status":      {Path: "value.status", Types: map[string]int{TypeString: 3}, Coverage: 1, Examples: []any{"OPEN", "CLOSED"}},
		"value.items[].sku": {Path: "value.items[].sku", Types: map[string]int{TypeString: 3}, Coverage: 2.0 / 3, Examples: []any{"a", "b", "c"}},
		"value.items[].qty": {Path: "value.items[].qty", Types: map[string]int{TypeInt: 1}, Coverage: 1.0 / 3, Examples: []any{int64(2)}},
		"value.meta":        {Path: "value.meta", Types: map[string]int{TypeJSONString: 1}, Coverage: 1.0 / 3, Examples: []any{`{"x":1}`}},
		"value.note":        {Path: "value.note", Types: map[string]int{TypeNull: 1}, Coverage: 1.0 / 3},
	}

	for path, want := range cases {
		if got := fields[path]; !reflect.DeepEqual(got, want) {
			t.Fatalf("%s:\n got %#v\nwant %#v", path, got, want)
		}
	}
}

func TestInferCapsFieldsAndDepth(t *testing.T) {
	inferrer := New(3, 1)
	inferrer.Add("value", decode(t, `{"a":{"deep":{"deeper":1}},"b":1,"c":1,"d":1}`))

	schema := inferrer.Result()
	if !schema.Truncated || len(schema.Fields) != 3 {
		t.Fatalf("expected truncation at 3 fields, got %+v", schema)
	}

	for _, field := range schema.Fields {
		if field.Path == "value.a.deep" {
			t.Fatal("depth limit should stop below value.a")
		}
	}
}
