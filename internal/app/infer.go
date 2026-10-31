package app

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/Midwayne/kafka-mcp/internal/infer"
	"github.com/Midwayne/kafka-mcp/internal/record"
	"github.com/Midwayne/kafka-mcp/internal/scan"
)

const (
	defaultInferSample = 200
	maxInferSample     = 2000
	maxInferFields     = 300
	maxInferDepth      = 8
	maxSuggestions     = 4
)

// InferSpec requests a structural summary of recent messages.
type InferSpec struct {
	Target
	SampleSize int    `json:"sample_size,omitempty" jsonschema:"Messages to inspect per partition (default 200, max 2000)."`
	From       string `json:"from,omitempty" jsonschema:"latest (default) inspects the newest messages; earliest or a time reads forward."`
	Output
}

// InferResult describes fields, decoders, and starter filters.
type InferResult struct {
	Schema      infer.Schema              `json:"schema"`
	Formats     map[string]map[string]int `json:"formats"`
	Suggestions []string                  `json:"filter_suggestions,omitempty"`
}

// InferSchema reads a sample and summarizes its structure without returning payloads.
func (s *Service) InferSchema(ctx context.Context, spec InferSpec) (InferResult, error) {
	perPartition := clampInt(spec.SampleSize, defaultInferSample, maxInferSample)

	var mu sync.Mutex

	inferrer := infer.New(maxInferFields, maxInferDepth)
	formats := map[string]map[string]int{"key": {}, "value": {}}

	newSink := func(p *prepared, _ int) scan.Sink {
		redacted := record.View{Redact: p.view.Redact, MaxItems: -1}

		return funcSink(func(m *record.Message) {
			envelope := redacted.Render(m)
			key, value := m.Key(), m.Value()

			mu.Lock()
			defer mu.Unlock()

			inferrer.AddEnvelope(envelope)
			formats["key"][key.Format]++
			formats["value"][value.Format]++
		})
	}

	_, err := s.readNewest(ctx, ToolInferSchema, spec.Target, spec.From, perPartition, spec.Output, newSink)
	if err != nil {
		return InferResult{}, err
	}

	schema := inferrer.Result()

	return InferResult{Schema: schema, Formats: formats, Suggestions: suggestFilters(schema)}, nil
}

// suggestFilters proposes starter expressions from always-present scalar fields.
func suggestFilters(schema infer.Schema) []string {
	var out []string

	for _, field := range schema.Fields {
		if len(out) >= maxSuggestions || len(field.Examples) == 0 || !strings.HasPrefix(field.Path, "value.") {
			continue
		}

		if strings.Contains(field.Path, "[]") || strings.ContainsAny(field.Path, "- ") {
			continue
		}

		switch {
		case field.Types[infer.TypeJSONString] > 0:
			out = append(out, fmt.Sprintf("parse_json(%s) != null  // string field holding JSON", field.Path))
		case field.Coverage < 1:
			out = append(out, fmt.Sprintf("has(%s)  // present in %.0f%% of messages", field.Path, field.Coverage*100))
		case field.Types[infer.TypeString] > 0:
			out = append(out, fmt.Sprintf("%s == %q", field.Path, field.Examples[0]))
		case field.Types[infer.TypeInt] > 0 || field.Types[infer.TypeFloat] > 0:
			out = append(out, fmt.Sprintf("%s > %v", field.Path, field.Examples[0]))
		}
	}

	return out
}
