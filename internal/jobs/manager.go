package jobs

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"
)

// RunFunc performs a job. It must honour ctx cancellation.
type RunFunc func(ctx context.Context, job *Job) (any, error)

// Manager tracks jobs, caps concurrency, and forgets finished jobs after a TTL.
type Manager struct {
	maxRunning int
	ttl        time.Duration

	mu   sync.Mutex
	jobs map[string]*Job
}

// NewManager creates a manager.
func NewManager(maxRunning int, ttl time.Duration) *Manager {
	return &Manager{maxRunning: maxRunning, ttl: ttl, jobs: map[string]*Job{}}
}

// Start launches run in the background. The job outlives ctx's cancellation
// (a tool call may return while the job continues) but is bounded by timeout.
func (m *Manager) Start(ctx context.Context, meta Meta, timeout time.Duration, run RunFunc) (*Job, error) {
	jobCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), timeout)

	job := &Job{ID: newID(), Meta: meta, Started: time.Now(), status: StatusRunning, cancel: cancel, done: make(chan struct{})}

	if err := m.register(job); err != nil {
		cancel()
		return nil, err
	}

	go func() {
		defer cancel()

		result, err := run(jobCtx, job)
		job.finish(result, err)
	}()

	return job, nil
}

func (m *Manager) register(job *Job) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.sweep()

	running := 0
	for _, existing := range m.jobs {
		if existing.running() {
			running++
		}
	}

	if running >= m.maxRunning {
		return fmt.Errorf("%d jobs are already running (limit %d); wait for one or cancel it with cancel_job", running, m.maxRunning)
	}

	m.jobs[job.ID] = job

	return nil
}

// Get returns a job by ID.
func (m *Manager) Get(id string) (*Job, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.sweep()

	job, ok := m.jobs[strings.TrimSpace(id)]
	if !ok {
		return nil, fmt.Errorf("unknown or expired job %q (finished jobs are kept for %s)", id, m.ttl)
	}

	return job, nil
}

// List returns all known jobs, newest first.
func (m *Manager) List() []View {
	m.mu.Lock()
	m.sweep()

	jobs := make([]*Job, 0, len(m.jobs))
	for _, job := range m.jobs {
		jobs = append(jobs, job)
	}
	m.mu.Unlock()

	slices.SortFunc(jobs, func(a, b *Job) int { return b.Started.Compare(a.Started) })

	views := make([]View, 0, len(jobs))
	for _, job := range jobs {
		views = append(views, job.View())
	}

	return views
}

// Shutdown cancels every running job.
func (m *Manager) Shutdown() {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, job := range m.jobs {
		job.cancel()
	}
}

// sweep forgets finished jobs older than the TTL. Callers hold m.mu.
func (m *Manager) sweep() {
	for id, job := range m.jobs {
		if job.expired(m.ttl) {
			delete(m.jobs, id)
		}
	}
}

func newID() string {
	buf := make([]byte, 6)
	_, _ = rand.Read(buf)

	return "job-" + hex.EncodeToString(buf)
}
