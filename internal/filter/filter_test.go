package filter

import (
	"context"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Midwayne/kafka-mcp/internal/codec"
	"github.com/Midwayne/kafka-mcp/internal/config"
	"github.com/Midwayne/kafka-mcp/internal/record"
)

func message(value string) *record.Message {
	return record.New(&kgo.Record{
		Topic: "orders", Partition: 3, Offset: 100, Key: []byte("cust-1"), Value: []byte(value),
		Timestamp: time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC),
		Headers:   []kgo.RecordHeader{{Key: "type", Value: []byte("OrderPlaced")}},
	}, &record.Source{Ctx: context.Background(), Decoder: codec.NewDecoder(nil), ValueFormat: config.FormatAuto})
}

const order = `{"status":"FAILED","amount":1250,"price":9.5,"id":9007199254740993,"tags":["vip"],"meta":"{\"region\":\"eu\"}"}`

func TestCompilePredicateMatches(t *testing.T) {
	cases := map[string]bool{
		`value.status == "FAILED"`:                        true,
		`value.amount > 1000 && value.price < 10`:         true,
		`value.amount > 1000.5`:                           true,
		`value.id == 9007199254740993`:                    true,
		`"vip" in value.tags`:                             true,
		`has(value.missing)`:                              false,
		`value.?missing.orValue("none") == "none"`:        true,
		`headers["type"] == "OrderPlaced"`:                true,
		`key.startsWith("cust-") && partition == 3`:       true,
		`offset >= 100 && topic == "orders"`:              true,
		`timestamp > timestamp("2026-01-01T00:00:00Z")`:   true,
		`ts == 1772366400000`:                             true,
		`raw_value.contains("FAILED")`:                    true,
		`parse_json(value.meta).region == "eu"`:           true,
		`value.status.lowerAscii() == "failed"`:           true,
		`value.status.matches("^FAIL")`:                   true,
		`value.tags.exists(t, t == "vip")`:                true,
		"value.`status` == 'FAILED'":                      true,
		`headers.exists(k, k == "type") && size(key) > 0`: true,
	}

	for source, want := range cases {
		expression, err := CompilePredicate(source)
		if err != nil {
			t.Fatalf("compile %q: %v", source, err)
		}

		got, err := expression.Match(message(order))
		if err != nil || got != want {
			t.Fatalf("%q = %v, %v; want %v", source, got, err, want)
		}
	}
}

func TestCompileErrors(t *testing.T) {
	cases := map[string]string{
		"":                           "empty",
		"value.status ==":            "invalid expression",
		`"text"`:                     "must return bool",
		"unknown_var > 1":            "undeclared reference",
		strings.Repeat("x", 5000):    "exceeds",
		"partition == 'not-an-int'":  "no matching overload",
		"value.amount > 1 &&& false": "invalid expression",
	}

	for source, want := range cases {
		if _, err := CompilePredicate(source); err == nil || !strings.Contains(err.Error(), want) {
			t.Fatalf("CompilePredicate(%.20q) error = %v, want %q", source, err, want)
		}
	}
}

func TestMatchReportsMissingFieldsAsErrors(t *testing.T) {
	expression, err := CompilePredicate(`value.nope == 1`)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := expression.Match(message(order)); err == nil || !strings.Contains(err.Error(), "no such key") {
		t.Fatalf("expected missing key error, got %v", err)
	}
}

func TestVariablesAndLazyValue(t *testing.T) {
	expression, err := CompilePredicate(`headers["type"] == "Other" && value.status == "FAILED"`)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(expression.Variables(), []string{VarHeaders, VarValue}) {
		t.Fatalf("Variables() = %v", expression.Variables())
	}

	// Short-circuiting means the invalid value is never decoded, so no error surfaces.
	matched, err := expression.Match(message("{not json"))
	if err != nil || matched {
		t.Fatalf("Match = %v, %v", matched, err)
	}
}

func TestStringGroupingKeys(t *testing.T) {
	cases := map[string]string{
		`value.status`:                     "FAILED",
		`value.amount`:                     "1250",
		`value.amount > 1000`:              "true",
		`[value.status, headers["type"]]`:  "[FAILED OrderPlaced]",
		`timestamp.getHours()`:             "12",
		`value.status + "/" + string(ts)`:  "FAILED/1772366400000",
		`value.?region.orValue("unknown")`: "unknown",
	}

	for source, want := range cases {
		expression, err := CompileValue(source)
		if err != nil {
			t.Fatalf("compile %q: %v", source, err)
		}

		if got, err := expression.String(message(order)); err != nil || got != want {
			t.Fatalf("%q = %q, %v; want %q", source, got, err, want)
		}
	}
}

func TestExpressionIsSafeForConcurrentUse(t *testing.T) {
	expression, err := CompilePredicate(`value.amount > 1000`)
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	for range 32 {
		wg.Go(func() {
			if ok, err := expression.Match(message(order)); !ok || err != nil {
				t.Errorf("concurrent Match = %v, %v", ok, err)
			}
		})
	}
	wg.Wait()
}
