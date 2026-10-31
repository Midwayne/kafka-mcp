// Package jobs runs long operations in the background with progress, waiting,
// cancellation, and time-based cleanup.
package jobs

import (
	"context"
	"sync"
	"time"
)

// Status of a job.
type Status string

// Job statuses.
const (
	StatusRunning   Status = "running"
	StatusCompleted Status = "completed"
	StatusFailed    Status = "failed"
	StatusCancelled Status = "cancelled"
)

// Meta describes a job for listings.
type Meta struct {
	Kind    string `json:"kind"`
	Cluster string `json:"cluster"`
	Summary string `json:"summary"`
}

// Job is one background operation.
type Job struct {
	ID      string
	Meta    Meta
	Started time.Time

	cancel context.CancelFunc
	done   chan struct{}

	mu        sync.Mutex
	status    Status
	ended     time.Time
	result    any
	err       error
	progress  func() any
	cancelled bool
}

// View is the JSON form of a job.
type View struct {
	ID string `json:"job_id"`
	Meta
	Status    Status `json:"status"`
	StartedAt string `json:"started_at"`
	EndedAt   string `json:"ended_at,omitempty"`
	ElapsedMs int64  `json:"elapsed_ms"`
	Progress  any    `json:"progress,omitempty"`
	Error     string `json:"error,omitempty"`
}

// SetProgress registers a function that reports live progress.
func (j *Job) SetProgress(progress func() any) {
	j.mu.Lock()
	defer j.mu.Unlock()

	j.progress = progress
}

// Cancel requests cancellation. The job finishes with StatusCancelled.
func (j *Job) Cancel() {
	j.mu.Lock()
	j.cancelled = true
	j.mu.Unlock()

	j.cancel()
}

// Wait blocks until the job ends, d elapses, or ctx is done. It reports whether the job ended.
func (j *Job) Wait(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-j.done:
		return true
	case <-timer.C:
		return false
	case <-ctx.Done():
		return false
	}
}

// Result returns the job's result and error once it has ended.
func (j *Job) Result() (any, error) {
	j.mu.Lock()
	defer j.mu.Unlock()

	return j.result, j.err
}

// View snapshots the job. Progress is included while running.
func (j *Job) View() View {
	j.mu.Lock()
	defer j.mu.Unlock()

	view := View{ID: j.ID, Meta: j.Meta, Status: j.status, StartedAt: j.Started.UTC().Format(time.RFC3339)}

	end := time.Now()
	if !j.ended.IsZero() {
		end = j.ended
		view.EndedAt = j.ended.UTC().Format(time.RFC3339)
	}
	view.ElapsedMs = end.Sub(j.Started).Milliseconds()

	if j.status == StatusRunning && j.progress != nil {
		view.Progress = j.progress()
	}

	if j.err != nil {
		view.Error = j.err.Error()
	}

	return view
}

func (j *Job) finish(result any, err error) {
	j.mu.Lock()
	defer j.mu.Unlock()

	j.result, j.err, j.ended = result, err, time.Now()

	switch {
	case j.cancelled:
		j.status = StatusCancelled
	case err != nil:
		j.status = StatusFailed
	default:
		j.status = StatusCompleted
	}

	close(j.done)
}

func (j *Job) expired(ttl time.Duration) bool {
	j.mu.Lock()
	defer j.mu.Unlock()

	return j.status != StatusRunning && time.Since(j.ended) > ttl
}

func (j *Job) running() bool {
	j.mu.Lock()
	defer j.mu.Unlock()

	return j.status == StatusRunning
}
