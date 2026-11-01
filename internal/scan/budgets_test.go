package scan

import (
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
)

func TestTakeBudgetsAcrossBatches(t *testing.T) {
	cases := []struct {
		name   string
		limits Limits
		counts []int
		reason string
		bytes  int64
	}{
		{"unlimited", Limits{}, []int{3, 3}, "", 24},
		{"record limit", Limits{MaxScanned: 4}, []int{3, 1}, ReasonMaxScanned, 16},
		{"byte limit", Limits{MaxBytes: 16}, []int{3, 1}, ReasonMaxBytes, 16},
		{"byte overshoot", Limits{MaxBytes: 13}, []int{3, 1}, ReasonMaxBytes, 16},
		{"record limit first", Limits{MaxScanned: 4, MaxBytes: 20}, []int{3, 1}, ReasonMaxScanned, 16},
		{"byte limit first", Limits{MaxScanned: 5, MaxBytes: 16}, []int{3, 1}, ReasonMaxBytes, 16},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			scanner := New(Request{Limits: tc.limits})
			records := []*kgo.Record{
				{Key: []byte("k"), Value: []byte("123")},
				{Key: []byte("k"), Value: []byte("456")},
				{Key: []byte("k"), Value: []byte("789")},
			}
			for i, want := range tc.counts {
				taken, reason := scanner.take(records)
				wantReason := ""
				if i == len(tc.counts)-1 {
					wantReason = tc.reason
				}
				if len(taken) != want || reason != wantReason {
					t.Fatalf("batch %d: got (%d, %q), want (%d, %q)", i, len(taken), reason, want, wantReason)
				}
			}
			if got := scanner.counters.scanned.Load(); got != int64(tc.counts[0]+tc.counts[1]) {
				t.Fatalf("scanned = %d", got)
			}
			if got := scanner.counters.bytes.Load(); got != tc.bytes {
				t.Fatalf("bytes = %d, want %d", got, tc.bytes)
			}
		})
	}
}

func TestTakeSkipsControlRecords(t *testing.T) {
	scanner := New(Request{Limits: Limits{MaxScanned: 1, MaxBytes: 1}})
	control := kgo.NewRecordAttrs(kgo.RecordAttrsOpts{Control: true})
	records := []*kgo.Record{
		{Attrs: control, Value: []byte("control")},
		{Value: []byte("v")},
		{Attrs: control, Value: []byte("control")},
		{Value: []byte("next")},
	}
	taken, reason := scanner.take(records)
	if len(taken) != 3 || reason != ReasonMaxScanned {
		t.Fatalf("got (%d, %q), want (3, %q)", len(taken), reason, ReasonMaxScanned)
	}
	if scanner.counters.scanned.Load() != 1 || scanner.counters.bytes.Load() != 1 {
		t.Fatal("control records must not consume budgets")
	}
}
