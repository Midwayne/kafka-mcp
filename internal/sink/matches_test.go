package sink

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Midwayne/kafka-mcp/internal/record"
)

func TestMatchesConcurrentOffersKeepExactLimit(t *testing.T) {
	const limit = 10
	matches := NewMatches(limit, record.View{}, nil)
	var accepted atomic.Int64
	var wg sync.WaitGroup

	for i := range 1000 {
		wg.Go(func() {
			stored, _ := matches.Offer(message(int64(i), time.Unix(0, 0), `{}`))
			if stored {
				accepted.Add(1)
			}
		})
	}
	wg.Wait()

	if accepted.Load() != limit || len(matches.Items()) != limit {
		t.Fatalf("accepted = %d, stored = %d; want %d", accepted.Load(), len(matches.Items()), limit)
	}
	// A full sink must reject without invoking the renderer or decoding payloads.
	if stored, more := matches.Offer(nil); stored || more {
		t.Fatalf("full sink returned (%v, %v)", stored, more)
	}
}

func TestMatchesDisabledSamplingDoesNotRender(t *testing.T) {
	for _, limit := range []int{0, -1} {
		matches := NewMatches(limit, nil, nil)
		if stored, more := matches.Offer(nil); stored || more || len(matches.Items()) != 0 {
			t.Fatalf("limit %d: got (%v, %v), stored = %d", limit, stored, more, len(matches.Items()))
		}
	}
}
