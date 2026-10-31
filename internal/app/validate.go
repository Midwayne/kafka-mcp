package app

import (
	"context"
	"errors"
	"sync"

	"github.com/Midwayne/kafka-mcp/internal/filter"
	"github.com/Midwayne/kafka-mcp/internal/grep"
	"github.com/Midwayne/kafka-mcp/internal/record"
	"github.com/Midwayne/kafka-mcp/internal/scan"
	"github.com/Midwayne/kafka-mcp/internal/sink"
)

const (
	defaultValidateSample = 50
	maxValidateSample     = 500
	maxValidateExamples   = 5
)

// ValidateSpec checks expressions, optionally against recent messages.
type ValidateSpec struct {
	Filter     string     `json:"filter,omitempty" jsonschema:"CEL predicate to check."`
	Grep       *grep.Spec `json:"grep,omitempty" jsonschema:"grep spec to check."`
	GroupBy    string     `json:"group_by,omitempty" jsonschema:"CEL group-by expression to check."`
	Cluster    string     `json:"cluster,omitempty" jsonschema:"With topics, evaluate against the newest messages."`
	Topics     []string   `json:"topics,omitempty" jsonschema:"Topics to sample for evaluation."`
	SampleSize int        `json:"sample_size,omitempty" jsonschema:"Newest messages per partition to evaluate (default 50, max 500)."`
}

// ValidateResult reports compile status and sample evaluation.
type ValidateResult struct {
	Valid     bool          `json:"valid"`
	Error     string        `json:"error,omitempty"`
	Variables []string      `json:"filter_variables,omitempty"`
	Sample    *SampleReport `json:"sample,omitempty"`
}

// SampleReport summarizes evaluating the expressions against real messages.
type SampleReport struct {
	Evaluated     int64            `json:"evaluated"`
	Matched       int64            `json:"matched"`
	FilterErrors  int64            `json:"filter_errors"`
	ErrorSamples  []string         `json:"filter_error_samples,omitempty"`
	GroupExamples []string         `json:"group_examples,omitempty"`
	Examples      []map[string]any `json:"matched_examples,omitempty"`
}

// ValidateFilter compiles the expressions and, when a target is given, tests them.
func (s *Service) ValidateFilter(ctx context.Context, spec ValidateSpec) (ValidateResult, error) {
	if spec.Filter == "" && spec.Grep == nil && spec.GroupBy == "" {
		return ValidateResult{}, errors.New("pass filter, grep, or group_by")
	}

	compiled, key, problem := compileForValidation(spec)
	if problem != "" {
		return ValidateResult{Error: problem}, nil
	}

	result := ValidateResult{Valid: true, Variables: variables(spec.Filter)}

	if spec.Cluster == "" || len(spec.Topics) == 0 {
		return result, nil
	}

	perPartition := clampInt(spec.SampleSize, defaultValidateSample, maxValidateSample)
	report := &sampleEvaluator{matchers: compiled.matchers, key: key}

	_, err := s.readNewest(ctx, ToolValidateFilter, Target{Cluster: spec.Cluster, Topics: spec.Topics}, "", perPartition, Output{},
		func(*prepared, int) scan.Sink { return funcSink(report.evaluate) })

	result.Sample = &report.report

	return result, err
}

// compileForValidation reports compile problems as text: for validate_filter
// they are the answer, not a failure of the tool.
func compileForValidation(spec ValidateSpec) (*prepared, sink.KeyFunc, string) {
	compiled := &prepared{}
	if err := compiled.compileMatch(Match{Filter: spec.Filter, Grep: spec.Grep}); err != nil {
		return nil, nil, err.Error()
	}

	key, err := groupKey(spec.GroupBy)
	if err != nil {
		return nil, nil, err.Error()
	}

	return compiled, key, ""
}

func variables(expression string) []string {
	if expression == "" {
		return nil
	}

	compiled, err := filter.CompilePredicate(expression)
	if err != nil {
		return nil
	}

	return compiled.Variables()
}

type sampleEvaluator struct {
	matchers []scan.Matcher
	key      sink.KeyFunc

	mu     sync.Mutex
	report SampleReport
}

func (e *sampleEvaluator) evaluate(m *record.Message) {
	matched, failure := true, ""

	for _, matcher := range e.matchers {
		ok, err := matcher.Match(m)
		if err != nil {
			matched, failure = false, err.Error()
			break
		}

		matched = matched && ok
	}

	group := ""
	if matched && e.key != nil {
		group, _ = e.key(m)
	}

	e.record(m, matched, failure, group)
}

func (e *sampleEvaluator) record(m *record.Message, matched bool, failure, group string) {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.report.Evaluated++

	switch {
	case failure != "":
		e.report.FilterErrors++
		if len(e.report.ErrorSamples) < maxValidateExamples {
			e.report.ErrorSamples = append(e.report.ErrorSamples, failure)
		}
	case matched:
		e.report.Matched++
		if len(e.report.Examples) < maxValidateExamples {
			e.report.Examples = append(e.report.Examples, record.Coordinates(m))
		}
		if group != "" && len(e.report.GroupExamples) < maxValidateExamples {
			e.report.GroupExamples = append(e.report.GroupExamples, group)
		}
	}
}
