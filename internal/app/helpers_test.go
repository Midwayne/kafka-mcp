package app

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"

	"github.com/Midwayne/kafka-mcp/internal/infer"
	"github.com/Midwayne/kafka-mcp/internal/scan"
	"github.com/Midwayne/kafka-mcp/internal/sink"
)

func intp(v int) *int       { return &v }
func int64p(v int64) *int64 { return &v }

func TestBoundsForDefaults(t *testing.T) {
	cases := []struct {
		name   string
		window Window
		from   string
	}{
		{"scan defaults to last hour", Window{}, defaultScanFrom},
		{"follow defaults to latest", Window{Follow: true}, "latest"},
		{"explicit from wins", Window{From: "-7d"}, "-7d"},
		{"start offset leaves from empty", Window{StartOffset: int64p(-10)}, ""},
		{"resume leaves from empty", Window{ResumeOffsets: map[string]map[string]int64{"t": {"0": 5}}}, ""},
	}

	for _, tc := range cases {
		bounds, err := boundsFor(tc.window)
		if err != nil || bounds.From != tc.from {
			t.Fatalf("%s: from = %q, %v; want %q", tc.name, bounds.From, err, tc.from)
		}
	}
}

func TestParseResume(t *testing.T) {
	got, err := parseResume(map[string]map[string]int64{"orders": {"0": 10, "7": 99}})
	if err != nil || !reflect.DeepEqual(got, map[string]map[int32]int64{"orders": {0: 10, 7: 99}}) {
		t.Fatalf("parseResume = %v, %v", got, err)
	}

	for _, bad := range []string{"x", "-1", "99999999999"} {
		if _, err := parseResume(map[string]map[string]int64{"t": {bad: 1}}); err == nil {
			t.Fatalf("partition %q should be rejected", bad)
		}
	}
}

func TestSelectionRequiresKeyForPartitioner(t *testing.T) {
	if _, err := selection([]string{"t"}, ScanSpec{Match: Match{KeyPartitioner: "murmur2"}}); err == nil {
		t.Fatal("key_partitioner without key should fail")
	}

	key := "k"
	sel, err := selection([]string{"t"}, ScanSpec{Match: Match{Key: &key, KeyPartitioner: "java"}})
	if err != nil || sel.Partitioner == nil || string(sel.Key) != "k" {
		t.Fatalf("selection = %+v, %v", sel, err)
	}
}

func TestClampAndWait(t *testing.T) {
	cases := [][3]int{{0, 20, 20}, {-5, 20, 20}, {50, 20, 50}, {900, 20, 500}}
	for _, tc := range cases {
		if got := clampInt(tc[0], tc[1], 500); got != tc[2] {
			t.Fatalf("clampInt(%d, %d, 500) = %d, want %d", tc[0], tc[1], got, tc[2])
		}
	}

	waits := map[*int]time.Duration{nil: defaultWait, intp(0): 0, intp(-3): 0, intp(5): 5 * time.Second, intp(600): maxWait}
	for in, want := range waits {
		if got := waitDuration(in); got != want {
			t.Fatalf("waitDuration = %s, want %s", got, want)
		}
	}
}

func TestApplyOverrides(t *testing.T) {
	spec := CountSpec{ScanSpec: ScanSpec{Window: Window{From: "-1h", StartOffset: int64p(-5)}, Budget: Budget{Limit: 3}}}

	applyOverrides(&spec, RunSavedSpec{From: "-2d", Limit: 50, WaitSeconds: intp(0), Export: true, DryRun: true})

	if spec.From != "-2d" || spec.StartOffset != nil || spec.Limit != 50 || *spec.WaitSeconds != 0 || !spec.Export || !spec.DryRun {
		t.Fatalf("overrides not applied: %+v", spec)
	}

	untouched := CountSpec{ScanSpec: ScanSpec{Window: Window{To: "now"}, Budget: Budget{Limit: 3}}}
	applyOverrides(&untouched, RunSavedSpec{})

	if untouched.To != "now" || untouched.Limit != 3 || untouched.Export {
		t.Fatalf("empty overrides changed the spec: %+v", untouched)
	}
}

func TestHints(t *testing.T) {
	result := func(stats scan.Stats) ScanResult { return ScanResult{Stats: &stats} }

	cases := map[string]ScanResult{
		"resume_offsets": result(scan.Stats{StopReason: scan.ReasonLimit, Matched: 1, Scanned: 1}),
		"timeout":        result(scan.Stats{StopReason: scan.ReasonTimeout, Matched: 1, Scanned: 1}),
		"has(value.x)":   result(scan.Stats{StopReason: scan.ReasonCompleted, FilterErrors: 4, Matched: 1, Scanned: 9}),
		"infer_schema":   result(scan.Stats{StopReason: scan.ReasonCompleted, Scanned: 9}),
		"Widen from":     result(scan.Stats{StopReason: scan.ReasonCompleted}),
		"JSON Lines":     {Stats: &scan.Stats{StopReason: scan.ReasonCompleted, Matched: 1, Scanned: 1}, Export: &sink.ExportInfo{Path: "/x.jsonl"}},
	}

	for want, res := range cases {
		if got := strings.Join(hints(res, &prepared{}), " "); !strings.Contains(got, want) {
			t.Fatalf("hints for %q = %q", want, got)
		}
	}
}

func TestSuggestFilters(t *testing.T) {
	schema := infer.Schema{Messages: 10, Fields: []infer.Field{
		{Path: "value.status", Types: map[string]int{infer.TypeString: 10}, Coverage: 1, Examples: []any{"OPEN"}},
		{Path: "value.amount", Types: map[string]int{infer.TypeInt: 10}, Coverage: 1, Examples: []any{int64(5)}},
		{Path: "value.note", Types: map[string]int{infer.TypeString: 3}, Coverage: 0.3, Examples: []any{"x"}},
		{Path: "value.items[].sku", Types: map[string]int{infer.TypeString: 3}, Coverage: 1, Examples: []any{"a"}},
		{Path: "key", Types: map[string]int{infer.TypeString: 10}, Coverage: 1, Examples: []any{"k"}},
	}}

	want := []string{`value.status == "OPEN"`, "value.amount > 5", "has(value.note)  // present in 30% of messages"}
	if got := suggestFilters(schema); !reflect.DeepEqual(got, want) {
		t.Fatalf("suggestFilters = %q", got)
	}
}

func TestExplain(t *testing.T) {
	cases := map[string]error{
		"verify_hostname":     errors.New("tls: failed to verify certificate: x509: cannot validate certificate for 10.0.0.1 because it doesn't contain any IP SANs"),
		"truststore":          errors.New("x509: certificate signed by unknown authority"),
		"SASL authentication": fmt.Errorf("auth: %w", kerr.SaslAuthenticationFailed),
		"ACL":                 fmt.Errorf("fetch: %w", kerr.TopicAuthorizationFailed),
		"unreachable":         errors.New("dial tcp 10.0.0.1:9093: connect: connection refused"),
	}

	for want, err := range cases {
		if got := Explain(err); !strings.Contains(got, "Hint:") || !strings.Contains(got, want) {
			t.Fatalf("Explain(%v) = %q, want hint mentioning %q", err, got, want)
		}
	}

	if got := Explain(errors.New("something else")); got != "something else" {
		t.Fatalf("unknown errors should pass through, got %q", got)
	}
}

func TestSummarize(t *testing.T) {
	plan := scan.Plan{Ranges: []scan.Range{{Topic: "t", Partition: 0, Start: 10, End: 30}, {Topic: "t", Partition: 1, Start: 0, End: 5}}, FromMs: 1_700_000_000_000}

	summary := summarize([]string{"t"}, plan, false)
	if summary.RecordsInRange != 25 || summary.Partitions != 2 || summary.Window == nil || summary.Window.From != "2023-11-14T22:13:20Z" {
		t.Fatalf("summarize = %+v", summary)
	}

	if following := summarize([]string{"t"}, plan, true); following.RecordsInRange != -1 {
		t.Fatalf("follow should report an unbounded range, got %d", following.RecordsInRange)
	}
}
