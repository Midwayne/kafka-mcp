package scan

import (
	"context"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Midwayne/kafka-mcp/internal/record"
)

// work evaluates batches until the channel closes. After a halt, remaining
// records are skipped but their first offset is recorded so a resumed scan
// re-reads them instead of silently skipping.
func (s *Scanner) work(ctx context.Context, batches <-chan []*kgo.Record) {
	for batch := range batches {
		for _, r := range batch {
			if ctx.Err() != nil {
				s.tracker.noteDropped(partitionKey{r.Topic, r.Partition}, r.Offset)
				break
			}

			s.evaluate(r)
		}
	}
}

func (s *Scanner) evaluate(r *kgo.Record) {
	if r.Attrs.IsControl() || !s.req.Prefilter.Match(r) {
		return
	}

	message := record.New(r, s.req.Sources[r.Topic])

	for _, matcher := range s.req.Matchers {
		matched, err := matcher.Match(message)
		if err != nil {
			s.counters.filterError(err)
			return
		}

		if !matched {
			return
		}
	}

	accepted, more := s.req.Sink.Offer(message)

	if accepted {
		s.counters.matched.Add(1)
	} else {
		s.tracker.noteDropped(partitionKey{r.Topic, r.Partition}, r.Offset)
	}

	if !more {
		s.halt(ReasonLimit)
	}
}
