package app

import (
	"fmt"
	"strings"

	"github.com/Midwayne/kafka-mcp/internal/filter"
	"github.com/Midwayne/kafka-mcp/internal/record"
	"github.com/Midwayne/kafka-mcp/internal/scan"
	"github.com/Midwayne/kafka-mcp/internal/sink"
)

// groupKey compiles a group_by expression; empty means a single group.
func groupKey(expression string) (sink.KeyFunc, error) {
	if strings.TrimSpace(expression) == "" {
		return nil, nil
	}

	compiled, err := filter.CompileValue(expression)
	if err != nil {
		return nil, fmt.Errorf("group_by: %w", err)
	}

	return func(m *record.Message) (string, error) { return compiled.String(m) }, nil
}

// describe summarizes a scan for job listings.
func describe(p *prepared) string {
	parts := []string{strings.Join(p.summary.Topics, ",")}

	if p.grep != nil {
		parts = append(parts, "grep")
	}

	if len(p.matchers) > 0 {
		parts = append(parts, fmt.Sprintf("%d matcher(s)", len(p.matchers)))
	}

	if p.follow {
		parts = append(parts, "follow")
	}

	return strings.Join(parts, " | ")
}

// hints suggest the agent's next step based on how the scan ended.
func hints(result ScanResult, p *prepared) []string {
	stats := result.Stats
	var out []string

	switch stats.StopReason {
	case scan.ReasonLimit:
		out = append(out, "Stopped at the match limit. To get more, call again with resume_offsets set to stats.next_offsets (or raise limit).")
	case scan.ReasonMaxScanned, scan.ReasonMaxBytes, scan.ReasonTimeout:
		out = append(out, fmt.Sprintf("Stopped early (%s) before reading the whole range. Resume with resume_offsets = stats.next_offsets, or narrow from/to, partitions, or key.", stats.StopReason))
	case scan.ReasonIdle:
		out = append(out, "Some partitions stopped short of their end offset because no data arrived; results may be incomplete for them.")
	}

	if stats.FilterErrors > 0 {
		out = append(out, fmt.Sprintf("%d messages made the filter error (see filter_error_samples), usually missing fields. Guard them with has(value.x) or value.?x.orValue(...).", stats.FilterErrors))
	}

	switch {
	case stats.Scanned == 0 && stats.StopReason == scan.ReasonCompleted && !p.follow:
		out = append(out, "No records in the selected range. Widen from (scans default to the last hour) or check describe_topic.")
	case stats.Matched == 0 && stats.Scanned > 0:
		out = append(out, fmt.Sprintf("No matches in %d scanned records. Check field names with infer_schema and test the filter with validate_filter.", stats.Scanned))
	}

	if result.Export != nil {
		out = append(out, fmt.Sprintf("All matches were written to %s (%d records, JSON Lines).", result.Export.Path, result.Export.Records))
	}

	return out
}
