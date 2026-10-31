package app

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/Midwayne/kafka-mcp/internal/saved"
)

// Saved scan kinds.
const (
	KindScan  = "scan"
	KindCount = "count"
)

// SaveScanSpec stores a reusable scan or count.
type SaveScanSpec struct {
	Name        string    `json:"name" jsonschema:"Name to save under (letters, digits, _ . -)."`
	Description string    `json:"description,omitempty" jsonschema:"What this scan finds and why."`
	Kind        string    `json:"kind,omitempty" jsonschema:"scan (default; runs scan_messages) or count (runs count_messages)."`
	Spec        CountSpec `json:"spec" jsonschema:"Arguments for scan_messages or count_messages. group_by, top, and samples only apply to count."`
}

// RunSavedSpec runs a saved scan with optional overrides.
type RunSavedSpec struct {
	Name           string                      `json:"name" jsonschema:"Saved scan name."`
	From           string                      `json:"from,omitempty" jsonschema:"Override the start (clears a saved start_offset)."`
	To             string                      `json:"to,omitempty" jsonschema:"Override the end (clears a saved end_offset)."`
	ResumeOffsets  map[string]map[string]int64 `json:"resume_offsets,omitempty" jsonschema:"Continue from a previous run's stats.next_offsets."`
	Limit          int                         `json:"limit,omitempty" jsonschema:"Override the match limit."`
	TimeoutSeconds int                         `json:"timeout_seconds,omitempty" jsonschema:"Override the time budget."`
	WaitSeconds    *int                        `json:"wait_seconds,omitempty" jsonschema:"How long to wait before returning a job_id."`
	Export         bool                        `json:"export,omitempty" jsonschema:"Write all matches to a JSON Lines file."`
	DryRun         bool                        `json:"dry_run,omitempty" jsonschema:"Only resolve the plan."`
}

// NameSpec identifies a saved scan.
type NameSpec struct {
	Name string `json:"name" jsonschema:"Saved scan name."`
}

// SaveScan validates and stores a definition.
func (s *Service) SaveScan(ctx context.Context, spec SaveScanSpec) (saved.Entry, error) {
	kind := spec.Kind
	if kind == "" {
		kind = KindScan
	}

	if kind != KindScan && kind != KindCount {
		return saved.Entry{}, fmt.Errorf("kind must be %s or %s", KindScan, KindCount)
	}

	if _, err := groupKey(spec.Spec.GroupBy); err != nil {
		return saved.Entry{}, err
	}

	if _, err := s.prepare(ctx, spec.Spec.ScanSpec, ToolSaveScan); err != nil {
		return saved.Entry{}, fmt.Errorf("saved scan is invalid: %w", err)
	}

	raw, err := json.Marshal(spec.Spec)
	if err != nil {
		return saved.Entry{}, err
	}

	return s.saved.Put(saved.Entry{Name: spec.Name, Description: spec.Description, Kind: kind, Spec: raw})
}

// ListSavedScans returns every saved definition.
func (s *Service) ListSavedScans() []saved.Entry { return s.saved.List() }

// DeleteSavedScan removes a definition.
func (s *Service) DeleteSavedScan(spec NameSpec) (map[string]any, error) {
	deleted, err := s.saved.Delete(spec.Name)

	return map[string]any{"name": spec.Name, "deleted": deleted}, err
}

// RunSavedScan runs a saved definition with overrides.
func (s *Service) RunSavedScan(ctx context.Context, run RunSavedSpec) (ScanResult, error) {
	entry, err := s.saved.Get(run.Name)
	if err != nil {
		return ScanResult{}, err
	}

	var spec CountSpec
	if err := json.Unmarshal(entry.Spec, &spec); err != nil {
		return ScanResult{}, fmt.Errorf("saved scan %q is corrupt: %w", run.Name, err)
	}

	applyOverrides(&spec, run)

	if entry.Kind == KindCount {
		return s.Count(ctx, spec)
	}

	return s.Scan(ctx, spec.ScanSpec)
}

func applyOverrides(spec *CountSpec, run RunSavedSpec) {
	if run.From != "" {
		spec.From, spec.StartOffset = run.From, nil
	}

	if run.To != "" {
		spec.To, spec.EndOffset = run.To, nil
	}

	if len(run.ResumeOffsets) > 0 {
		spec.ResumeOffsets = run.ResumeOffsets
	}

	if run.Limit > 0 {
		spec.Limit = run.Limit
	}

	if run.TimeoutSeconds > 0 {
		spec.TimeoutSeconds = run.TimeoutSeconds
	}

	if run.WaitSeconds != nil {
		spec.WaitSeconds = run.WaitSeconds
	}

	spec.Export = spec.Export || run.Export
	spec.DryRun = run.DryRun
}
