package sink

import (
	"strconv"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Midwayne/kafka-mcp/internal/record"
)

func BenchmarkCountsOffer(b *testing.B) {
	for _, samples := range []int{0, 3} {
		b.Run("samples="+strconv.Itoa(samples), func(b *testing.B) {
			counts := NewCounts(nil, 50, samples, record.View{})
			for i := range samples {
				counts.Offer(message(int64(i), time.Unix(0, 0), `{}`))
			}
			b.ReportAllocs()

			b.RunParallel(func(pb *testing.PB) {
				m := record.New(&kgo.Record{}, nil)
				for pb.Next() {
					counts.Offer(m)
				}
			})

			if got := counts.Result(0).Total; got != int64(b.N+samples) {
				b.Fatalf("count = %d, want %d", got, b.N+samples)
			}
		})
	}
}
