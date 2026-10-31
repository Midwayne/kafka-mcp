package app

import (
	"context"
	"fmt"
	"time"

	"github.com/Midwayne/kafka-mcp/internal/config"
	"github.com/Midwayne/kafka-mcp/internal/filter"
	"github.com/Midwayne/kafka-mcp/internal/grep"
	"github.com/Midwayne/kafka-mcp/internal/record"
	"github.com/Midwayne/kafka-mcp/internal/scan"
)

const (
	maxValueCharsLimit  = 100_000
	maxGrepHitsReported = 5
)

// prepared is a validated scan that is ready to run.
type prepared struct {
	handle      *handle
	plan        scan.Plan
	summary     PlanSummary
	matchers    []scan.Matcher
	grep        *grep.Matcher
	onlyMatch   bool
	prefilter   scan.Prefilter
	keyFormat   string
	valueFormat string
	view        record.View
	limits      scan.Limits
	limit       int
	timeout     time.Duration
	wait        time.Duration
	follow      bool
}

func (s *Service) prepare(ctx context.Context, spec ScanSpec, tool string) (*prepared, error) {
	h, err := s.cluster(spec.Cluster, tool)
	if err != nil {
		return nil, err
	}

	p := &prepared{handle: h, follow: spec.Follow, wait: waitDuration(spec.WaitSeconds), timeout: s.timeout(spec.TimeoutSeconds)}

	if err := p.compileMatch(spec.Match); err != nil {
		return nil, err
	}

	if err := p.setFormats(spec.Output); err != nil {
		return nil, err
	}

	topics, err := h.resolveTopics(ctx, spec.Topics)
	if err != nil {
		return nil, err
	}

	if err := s.plan(ctx, p, topics, spec); err != nil {
		return nil, err
	}

	s.applyBudget(p, spec.Budget, spec.Output)

	return p, nil
}

func (s *Service) plan(ctx context.Context, p *prepared, topics []string, spec ScanSpec) error {
	selection, err := selection(topics, spec)
	if err != nil {
		return err
	}

	bounds, err := boundsFor(spec.Window)
	if err != nil {
		return err
	}

	p.plan, err = scan.BuildPlan(ctx, p.handle.Admin(), selection, bounds, s.now())
	if err != nil {
		return err
	}

	p.summary = summarize(topics, p.plan, spec.Follow)

	return nil
}

func (p *prepared) compileMatch(m Match) error {
	if m.Grep != nil {
		compiled, err := grep.Compile(*m.Grep)
		if err != nil {
			return err
		}

		p.grep, p.onlyMatch = compiled, m.Grep.OnlyMatching
		p.matchers = append(p.matchers, compiled)
	}

	if m.Filter != "" {
		expression, err := filter.CompilePredicate(m.Filter)
		if err != nil {
			return fmt.Errorf("filter: %w", err)
		}

		p.matchers = append(p.matchers, expression)
	}

	p.prefilter.Headers = m.Headers
	if m.Key != nil {
		p.prefilter.Key = []byte(*m.Key)
	}

	return nil
}

func (p *prepared) setFormats(o Output) error {
	var err error

	if o.KeyFormat != "" {
		if p.keyFormat, err = config.NormalizeFormat(o.KeyFormat); err != nil {
			return fmt.Errorf("key_format: %w", err)
		}
	}

	if o.ValueFormat != "" {
		if p.valueFormat, err = config.NormalizeFormat(o.ValueFormat); err != nil {
			return fmt.Errorf("value_format: %w", err)
		}
	}

	return nil
}

func (s *Service) applyBudget(p *prepared, b Budget, o Output) {
	limits := s.cfg.Defaults.Scan

	p.limit = clampInt(b.Limit, limits.DefaultLimit, limits.MaxLimit)
	p.limits = scan.Limits{
		MaxScanned:  clampInt(b.MaxScanned, limits.MaxScanned, limits.MaxScanned),
		MaxBytes:    clampInt(b.MaxBytes, int64(limits.MaxBytes), int64(limits.MaxBytes)),
		RateBytes:   int64(limits.RateLimit),
		Workers:     limits.Workers,
		IdleTimeout: limits.IdleTimeout.Std(),
	}

	p.view = record.View{
		Fields:        o.Fields,
		Redact:        p.handle.Config().Redact,
		MaxValueChars: clampInt(o.MaxValueChars, limits.MaxValueChars, maxValueCharsLimit),
	}
}
