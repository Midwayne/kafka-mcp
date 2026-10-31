package app

import (
	"context"
	"errors"
	"path/filepath"
	"time"

	"github.com/Midwayne/kafka-mcp/internal/jobs"
	"github.com/Midwayne/kafka-mcp/internal/scan"
	"github.com/Midwayne/kafka-mcp/internal/sink"
)

// Count defaults.
const (
	defaultTopGroups = 50
	defaultSamples   = 3
	maxGroups        = 10_000
)

// ScanResult is returned by scan_messages, count_messages, and get_job.
type ScanResult struct {
	JobID    string            `json:"job_id,omitempty"`
	Status   jobs.Status       `json:"status"`
	Plan     *PlanSummary      `json:"plan,omitempty"`
	Stats    *scan.Stats       `json:"stats,omitempty"`
	Matches  []map[string]any  `json:"matches,omitempty"`
	Counts   *sink.CountResult `json:"counts,omitempty"`
	Export   *sink.ExportInfo  `json:"export,omitempty"`
	Progress any               `json:"progress,omitempty"`
	Hints    []string          `json:"hints,omitempty"`
}

// collector is a sink that also writes its results into a ScanResult.
type collector interface {
	scan.Sink
	fill(result *ScanResult)
}

type matchCollector struct {
	*sink.Matches
	export *sink.Export
}

func (c matchCollector) fill(result *ScanResult) {
	result.Matches = c.Items()

	if c.export != nil {
		info := c.export.Close()
		result.Export = &info
	}
}

type countCollector struct {
	*sink.Counts
	top int
}

func (c countCollector) fill(result *ScanResult) {
	counts := c.Result(c.top)
	result.Counts = &counts
}

// Scan finds matching messages.
func (s *Service) Scan(ctx context.Context, spec ScanSpec) (ScanResult, error) {
	p, err := s.prepare(ctx, spec, ToolScanMessages)
	if err != nil || spec.DryRun {
		return dryRun(p), err
	}

	return s.launch(ctx, p, ToolScanMessages, func(job *jobs.Job) (collector, error) {
		if !spec.Export {
			return matchCollector{Matches: sink.NewMatches(p.limit, p.renderer(), nil)}, nil
		}

		path := filepath.Join(s.cfg.Server.StateDir, "exports", job.ID+".jsonl")

		export, err := sink.NewExport(path, s.cfg.Defaults.Scan.MaxExportRecords, p.view.Redact)
		if err != nil {
			return nil, err
		}

		return matchCollector{Matches: sink.NewMatches(p.limit, p.renderer(), export), export: export}, nil
	})
}

// Count aggregates matching messages, optionally grouped by a CEL expression.
func (s *Service) Count(ctx context.Context, spec CountSpec) (ScanResult, error) {
	key, err := groupKey(spec.GroupBy)
	if err != nil {
		return ScanResult{}, err
	}

	p, err := s.prepare(ctx, spec.ScanSpec, ToolCountMessages)
	if err != nil || spec.DryRun {
		return dryRun(p), err
	}

	top := clampInt(spec.Top, defaultTopGroups, maxGroups)

	samples := defaultSamples
	if spec.Samples != nil {
		samples = min(max(*spec.Samples, 0), p.limit)
	}

	return s.launch(ctx, p, ToolCountMessages, func(*jobs.Job) (collector, error) {
		return countCollector{Counts: sink.NewCounts(key, maxGroups, samples, p.renderer()), top: top}, nil
	})
}

func dryRun(p *prepared) ScanResult {
	if p == nil {
		return ScanResult{}
	}

	return ScanResult{Status: jobs.StatusCompleted, Plan: &p.summary, Hints: []string{"dry_run: nothing was read; remove dry_run to scan"}}
}

// launch runs the scan as a job and waits up to p.wait for it to finish.
func (s *Service) launch(ctx context.Context, p *prepared, kind string, newCollector func(*jobs.Job) (collector, error)) (ScanResult, error) {
	meta := jobs.Meta{Kind: kind, Cluster: p.handle.Name(), Summary: describe(p)}

	job, err := s.jobs.Start(ctx, meta, p.timeout, func(jobCtx context.Context, job *jobs.Job) (any, error) {
		coll, err := newCollector(job)
		if err != nil {
			return nil, err
		}

		scanner := scan.New(p.request(jobCtx, coll))
		job.SetProgress(func() any { return scanner.Snapshot() })

		stats, err := scanner.Run(jobCtx, p.handle)

		result := ScanResult{Status: jobs.StatusCompleted, Plan: &p.summary, Stats: &stats}
		coll.fill(&result)
		result.Hints = hints(result, p)

		return result, err
	})
	if err != nil {
		return ScanResult{}, err
	}

	return s.await(ctx, job, p.wait)
}

// await returns the finished result, or the job ID and live progress if still running.
func (s *Service) await(ctx context.Context, job *jobs.Job, wait time.Duration) (ScanResult, error) {
	if wait > 0 && job.Wait(ctx, wait) {
		return finishedResult(job)
	}

	view := job.View()

	return ScanResult{
		JobID:    job.ID,
		Status:   view.Status,
		Progress: view.Progress,
		Hints:    []string{"Still running in the background. Call get_job with this job_id (optionally wait_seconds) to get results, or cancel_job to stop it."},
	}, nil
}

func finishedResult(job *jobs.Job) (ScanResult, error) {
	value, err := job.Result()

	result, ok := value.(ScanResult)
	if !ok && err == nil {
		err = errors.New("job finished without a scan result")
	}

	result.JobID = job.ID
	result.Status = job.View().Status

	return result, err
}
