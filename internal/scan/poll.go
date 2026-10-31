package scan

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	maxPollRecords = 10_000
	batchSize      = 512
)

// poll fetches and dispatches until the ranges end or a budget runs out, and
// returns why it stopped. A halt by a worker returns "" (its reason is already set).
func (s *Scanner) poll(scanCtx, parent context.Context, client *kgo.Client, work chan<- []*kgo.Record) (string, error) {
	for !s.tracker.allDone() {
		fetches, idle := s.fetch(scanCtx, client)

		select {
		case <-scanCtx.Done():
			return contextReason(parent), nil
		default:
		}

		if err := s.fetchError(fetches); err != nil {
			return "", err
		}

		if idle {
			if s.req.Follow {
				continue
			}

			return ReasonIdle, nil
		}

		if reason := s.dispatchAll(client, fetches, work); reason != "" {
			return reason, nil
		}

		s.throttle(scanCtx)
	}

	return ReasonCompleted, nil
}

// fetch polls once, reporting idle when nothing arrived within the idle timeout.
func (s *Scanner) fetch(ctx context.Context, client *kgo.Client) (kgo.Fetches, bool) {
	pollCtx, cancel := context.WithTimeout(ctx, s.req.Limits.IdleTimeout)
	defer cancel()

	fetches := client.PollRecords(pollCtx, maxPollRecords)
	idle := fetches.NumRecords() == 0 && pollCtx.Err() != nil && ctx.Err() == nil

	return fetches, idle
}

// contextReason maps a cancelled parent context to a reason. An internal halt
// leaves the parent intact, and its reason is already recorded.
func contextReason(parent context.Context) string {
	switch {
	case errors.Is(parent.Err(), context.DeadlineExceeded):
		return ReasonTimeout
	case parent.Err() != nil:
		return ReasonCancelled
	default:
		return ""
	}
}

func (s *Scanner) fetchError(fetches kgo.Fetches) error {
	for _, failure := range fetches.Errors() {
		if errors.Is(failure.Err, context.Canceled) || errors.Is(failure.Err, context.DeadlineExceeded) ||
			errors.Is(failure.Err, kgo.ErrClientClosed) {
			continue
		}

		var dataLoss *kgo.ErrDataLoss
		if errors.As(failure.Err, &dataLoss) {
			s.counters.warn(dataLoss.Error())
			continue
		}

		return fmt.Errorf("fetch %s[%d]: %w", failure.Topic, failure.Partition, failure.Err)
	}

	return nil
}

func (s *Scanner) dispatchAll(client *kgo.Client, fetches kgo.Fetches, work chan<- []*kgo.Record) string {
	reason := ""

	fetches.EachPartition(func(p kgo.FetchTopicPartition) {
		if reason == "" {
			reason = s.dispatch(client, p, work)
		}
	})

	return reason
}

// dispatch hands one partition's records to workers, enforcing budgets.
func (s *Scanner) dispatch(client *kgo.Client, p kgo.FetchTopicPartition, work chan<- []*kgo.Record) string {
	key := partitionKey{p.Topic, p.Partition}

	records, reachedEnd := s.tracker.clip(key, p.Records)
	taken, reason := s.take(records)

	if s.tracker.advance(key, taken, reachedEnd && len(taken) == len(records)) {
		client.RemoveConsumePartitions(map[string][]int32{p.Topic: {p.Partition}})
	}

	for start := 0; start < len(taken); start += batchSize {
		work <- taken[start:min(start+batchSize, len(taken))]
	}

	return reason
}

// take applies the record and byte budgets, trimming the batch at the limit.
func (s *Scanner) take(records []*kgo.Record) ([]*kgo.Record, string) {
	limits := s.req.Limits

	for i, r := range records {
		if r.Attrs.IsControl() {
			continue
		}

		if limits.MaxScanned > 0 && s.counters.scanned.Load() >= limits.MaxScanned {
			return records[:i], ReasonMaxScanned
		}

		if limits.MaxBytes > 0 && s.counters.bytes.Load() >= limits.MaxBytes {
			return records[:i], ReasonMaxBytes
		}

		s.counters.scanned.Add(1)
		s.counters.bytes.Add(int64(len(r.Key) + len(r.Value)))
	}

	return records, ""
}

// throttle sleeps until the average read rate is within the byte-rate limit.
func (s *Scanner) throttle(ctx context.Context) {
	rate := s.req.Limits.RateBytes
	if rate <= 0 {
		return
	}

	allowed := time.Duration(float64(s.counters.bytes.Load()) / float64(rate) * float64(time.Second))
	wait := allowed - time.Since(s.started)

	if wait <= 0 {
		return
	}

	timer := time.NewTimer(wait)
	defer timer.Stop()

	select {
	case <-timer.C:
	case <-ctx.Done():
	}
}
