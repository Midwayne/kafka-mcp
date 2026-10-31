package app

import (
	"context"

	"github.com/Midwayne/kafka-mcp/internal/jobs"
)

const maxJobPage = 500

// GetJobSpec fetches a job's status and a page of its results.
type GetJobSpec struct {
	JobID       string `json:"job_id" jsonschema:"ID returned by scan_messages or count_messages."`
	WaitSeconds int    `json:"wait_seconds,omitempty" jsonschema:"Wait up to this long (max 50) for a running job to finish."`
	Offset      int    `json:"offset,omitempty" jsonschema:"Skip this many matches (paging)."`
	Limit       int    `json:"limit,omitempty" jsonschema:"Return at most this many matches (default: all, max 500)."`
}

// JobIDSpec identifies a job.
type JobIDSpec struct {
	JobID string `json:"job_id" jsonschema:"Job ID."`
}

// GetJob returns a running job's progress or a finished job's results.
func (s *Service) GetJob(ctx context.Context, spec GetJobSpec) (ScanResult, error) {
	job, err := s.jobs.Get(spec.JobID)
	if err != nil {
		return ScanResult{}, err
	}

	seconds := spec.WaitSeconds
	result, err := s.await(ctx, job, waitDuration(&seconds))

	limit := clampInt(spec.Limit, maxJobPage, maxJobPage)
	start := min(max(spec.Offset, 0), len(result.Matches))
	result.Matches = result.Matches[start:min(start+limit, len(result.Matches))]

	return result, err
}

// ListJobs lists recent jobs, newest first.
func (s *Service) ListJobs() []jobs.View { return s.jobs.List() }

// CancelJob stops a running job. Partial results remain available via get_job.
func (s *Service) CancelJob(ctx context.Context, spec JobIDSpec) (jobs.View, error) {
	job, err := s.jobs.Get(spec.JobID)
	if err != nil {
		return jobs.View{}, err
	}

	job.Cancel()
	job.Wait(ctx, maxWait)

	return job.View(), nil
}
