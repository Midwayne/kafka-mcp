package scan

import (
	"context"
	"strconv"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Midwayne/kafka-mcp/internal/codec"
	"github.com/Midwayne/kafka-mcp/internal/config"
	"github.com/Midwayne/kafka-mcp/internal/record"
	"github.com/Midwayne/kafka-mcp/internal/sink"
)

func BenchmarkTake(b *testing.B) {
	records := make([]*kgo.Record, maxPollRecords)
	for i := range records {
		records[i] = &kgo.Record{Key: []byte("key"), Value: make([]byte, 600)}
	}

	cases := map[string]Limits{
		"unlimited": {},
		"bounded":   {MaxScanned: 1 << 60, MaxBytes: 1 << 60},
	}
	for name, limits := range cases {
		b.Run(name, func(b *testing.B) {
			scanner := New(Request{Limits: limits})
			b.ReportAllocs()

			for b.Loop() {
				taken, reason := scanner.take(records)
				if len(taken) != len(records) || reason != "" {
					b.Fatalf("take returned %d records, reason %q", len(taken), reason)
				}
			}
			b.ReportMetric(float64(len(records)), "records/op")
		})
	}
}

func BenchmarkEvaluateCount(b *testing.B) {
	for _, samples := range []int{0, 3} {
		b.Run("samples="+strconv.Itoa(samples), func(b *testing.B) {
			r := &kgo.Record{Topic: "events", Value: []byte(`{"status":"OK"}`)}
			source := &record.Source{Ctx: context.Background(), Decoder: codec.NewDecoder(nil), ValueFormat: config.FormatJSON}
			counts := sink.NewCounts(nil, 50, samples, record.View{})
			for range samples {
				counts.Offer(record.New(r, source))
			}
			scanner := New(Request{Sources: map[string]*record.Source{r.Topic: source}, Sink: counts})
			b.ReportAllocs()

			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					scanner.evaluate(r)
				}
			})

			if got := scanner.counters.matched.Load(); got != int64(b.N) {
				b.Fatalf("matched = %d, want %d", got, b.N)
			}
		})
	}
}
