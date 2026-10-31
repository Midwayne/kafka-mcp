package scan

import (
	"context"
	"runtime"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Midwayne/kafka-mcp/internal/record"
)

// Sink receives matching messages. It must be safe for concurrent use.
// accepted reports whether the sink kept the message; more reports whether it
// wants further messages. Returning more=false halts the scan.
type Sink interface {
	Offer(m *record.Message) (accepted, more bool)
}

// Matcher decides whether a message matches. CEL expressions and grep
// patterns both implement it. It must be safe for concurrent use.
type Matcher interface {
	Match(m *record.Message) (bool, error)
}

// ConsumerFactory creates a group-less consumer for explicit partition offsets.
type ConsumerFactory interface {
	Consumer(assignments map[string]map[int32]kgo.Offset) (*kgo.Client, error)
}

// Limits bounds the resources a scan may use. Zero values disable a limit.
type Limits struct {
	MaxScanned  int64
	MaxBytes    int64
	RateBytes   int64 // bytes per second
	Workers     int
	IdleTimeout time.Duration
}

// Request is a fully resolved scan.
type Request struct {
	Plan      Plan
	Sources   map[string]*record.Source // decoding settings per topic
	Prefilter Prefilter
	Matchers  []Matcher // evaluated in order, cheapest first; empty matches every record
	Sink      Sink
	Limits    Limits
	Follow    bool
}

// Scanner executes one Request. Snapshot may be called concurrently with Run.
type Scanner struct {
	req      Request
	tracker  *tracker
	counters counters
	started  time.Time

	mu       sync.Mutex
	reason   string
	finished time.Time
	cancel   context.CancelFunc
}

// New prepares a scanner.
func New(req Request) *Scanner {
	req.Prefilter.FromMs, req.Prefilter.ToMs = req.Plan.FromMs, req.Plan.ToMs

	if req.Limits.Workers <= 0 {
		req.Limits.Workers = runtime.NumCPU()
	}

	if req.Limits.IdleTimeout <= 0 {
		req.Limits.IdleTimeout = 5 * time.Second
	}

	return &Scanner{req: req, tracker: newTracker(req.Plan.Ranges), started: time.Now()}
}

// Run reads every range until completion or a limit, then returns final stats.
func (s *Scanner) Run(ctx context.Context, factory ConsumerFactory) (Stats, error) {
	if s.tracker.allDone() {
		s.setReason(ReasonCompleted)
		return s.finish(), nil
	}

	client, err := factory.Consumer(s.tracker.assignments())
	if err != nil {
		return s.finish(), err
	}
	defer client.Close()

	scanCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	s.mu.Lock()
	s.cancel = cancel
	s.mu.Unlock()

	work := make(chan []*kgo.Record, s.req.Limits.Workers*2)

	var wg sync.WaitGroup
	for range s.req.Limits.Workers {
		wg.Go(func() { s.work(scanCtx, work) })
	}

	reason, err := s.poll(scanCtx, ctx, client, work)

	close(work)
	wg.Wait()

	// Applied after workers drain so a worker's "limit" takes precedence.
	s.setReason(reason)

	return s.finish(), err
}

// Snapshot returns current progress; after Run returns it is the final result.
func (s *Scanner) Snapshot() Stats {
	s.mu.Lock()
	reason, finished := s.reason, s.finished
	s.mu.Unlock()

	elapsed := time.Since(s.started)
	if !finished.IsZero() {
		elapsed = finished.Sub(s.started)
	}

	stats := Stats{StopReason: reason, ElapsedMs: elapsed.Milliseconds()}
	s.counters.fill(&stats)
	stats.Partitions, stats.PartitionsDone = s.tracker.counts()

	if seconds := elapsed.Seconds(); seconds > 0 {
		stats.RecordsPerSec = int64(float64(stats.Scanned) / seconds)
	}

	if !finished.IsZero() {
		stats.NextOffsets = s.tracker.nextOffsets()
	}

	return stats
}

// setReason records the first stop reason. Workers keep draining dispatched
// batches, so every record counted as scanned is evaluated.
func (s *Scanner) setReason(reason string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.reason == "" && reason != "" {
		s.reason = reason
	}
}

// halt records the reason and cancels the scan immediately. Workers skip the
// rest of their batches and record resume positions for them.
func (s *Scanner) halt(reason string) {
	s.setReason(reason)

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.cancel != nil {
		s.cancel()
	}
}

// finish freezes the elapsed time and returns the final stats.
func (s *Scanner) finish() Stats {
	s.mu.Lock()
	s.finished = time.Now()
	s.mu.Unlock()

	return s.Snapshot()
}
