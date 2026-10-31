package app

import (
	"context"
	"time"

	"github.com/Midwayne/kafka-mcp/internal/grep"
	"github.com/Midwayne/kafka-mcp/internal/record"
	"github.com/Midwayne/kafka-mcp/internal/scan"
)

// PlanSummary describes what a scan will read.
type PlanSummary struct {
	Topics         []string     `json:"topics"`
	Partitions     int          `json:"partitions"`
	RecordsInRange int64        `json:"records_in_range"`
	Window         *TimeWindow  `json:"time_window,omitempty"`
	Ranges         []scan.Range `json:"ranges,omitempty"`
}

// TimeWindow is the record timestamp filter derived from from/to.
type TimeWindow struct {
	From string `json:"from,omitempty"`
	To   string `json:"to,omitempty"`
}

const maxRangesShown = 50

func summarize(topics []string, plan scan.Plan, follow bool) PlanSummary {
	summary := PlanSummary{Topics: topics, Partitions: len(plan.Ranges)}

	for _, rng := range plan.Ranges {
		summary.RecordsInRange += rng.End - rng.Start
	}

	if follow {
		summary.RecordsInRange = -1
	}

	if len(plan.Ranges) <= maxRangesShown {
		summary.Ranges = plan.Ranges
	}

	if plan.FromMs > 0 || plan.ToMs > 0 {
		summary.Window = &TimeWindow{From: formatMillis(plan.FromMs), To: formatMillis(plan.ToMs)}
	}

	return summary
}

func formatMillis(ms int64) string {
	if ms <= 0 {
		return ""
	}

	return time.UnixMilli(ms).UTC().Format(time.RFC3339)
}

// request assembles the scanner input. ctx bounds lazy decoding (Schema Registry fetches).
func (p *prepared) request(ctx context.Context, sink scan.Sink) scan.Request {
	return scan.Request{
		Plan:      p.plan,
		Sources:   p.sources(ctx),
		Prefilter: p.prefilter,
		Matchers:  p.matchers,
		Sink:      sink,
		Limits:    p.limits,
		Follow:    p.follow,
	}
}

func (p *prepared) sources(ctx context.Context) map[string]*record.Source {
	sources := make(map[string]*record.Source, len(p.summary.Topics))

	for _, topic := range p.summary.Topics {
		keyFormat, valueFormat := p.handle.Config().FormatsFor(topic)

		if p.keyFormat != "" {
			keyFormat = p.keyFormat
		}

		if p.valueFormat != "" {
			valueFormat = p.valueFormat
		}

		sources[topic] = &record.Source{Ctx: ctx, Decoder: p.handle.decoder, KeyFormat: keyFormat, ValueFormat: valueFormat}
	}

	return sources
}

// renderer returns the output shape: the view, or grep snippets for only_matching.
func (p *prepared) renderer() record.Renderer {
	if p.grep == nil || !p.onlyMatch {
		return p.view
	}

	return grep.Renderer{Matcher: p.grep, OnlyMatching: true, MaxHits: maxGrepHitsReported, Redact: p.view.Redact}
}

// runSync executes a scan in the caller's goroutine, bounded by the plan's timeout.
func (p *prepared) runSync(ctx context.Context, sink scan.Sink) (scan.Stats, error) {
	ctx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()

	return scan.New(p.request(ctx, sink)).Run(ctx, p.handle)
}
