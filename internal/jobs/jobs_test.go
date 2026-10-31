package jobs

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func block(ctx context.Context, _ *Job) (any, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func TestJobLifecycle(t *testing.T) {
	manager := NewManager(2, time.Hour)

	job, err := manager.Start(context.Background(), Meta{Kind: "scan", Cluster: "c"}, time.Minute, func(context.Context, *Job) (any, error) {
		return "done", nil
	})
	if err != nil {
		t.Fatal(err)
	}

	if !job.Wait(context.Background(), time.Second) {
		t.Fatal("job did not finish")
	}

	result, err := job.Result()
	if result != "done" || err != nil || job.View().Status != StatusCompleted {
		t.Fatalf("result = %v, %v, status %s", result, err, job.View().Status)
	}

	if got, err := manager.Get(job.ID); err != nil || got != job {
		t.Fatalf("Get = %v, %v", got, err)
	}
}

func TestJobOutlivesCallerContextButNotTimeout(t *testing.T) {
	manager := NewManager(2, time.Hour)
	caller, cancelCaller := context.WithCancel(context.Background())

	job, err := manager.Start(caller, Meta{}, 200*time.Millisecond, block)
	if err != nil {
		t.Fatal(err)
	}

	cancelCaller()

	if job.Wait(context.Background(), 50*time.Millisecond) {
		t.Fatal("job should keep running after the tool call's context ends")
	}

	if !job.Wait(context.Background(), time.Second) {
		t.Fatal("job should stop at its timeout")
	}

	if _, err := job.Result(); !errors.Is(err, context.DeadlineExceeded) || job.View().Status != StatusFailed {
		t.Fatalf("err = %v, status %s", err, job.View().Status)
	}
}

func TestCancelAndConcurrencyLimit(t *testing.T) {
	manager := NewManager(1, time.Hour)

	first, err := manager.Start(context.Background(), Meta{}, time.Minute, block)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := manager.Start(context.Background(), Meta{}, time.Minute, block); err == nil || !strings.Contains(err.Error(), "limit 1") {
		t.Fatalf("expected concurrency error, got %v", err)
	}

	first.Cancel()

	if !first.Wait(context.Background(), time.Second) || first.View().Status != StatusCancelled {
		t.Fatalf("status = %s", first.View().Status)
	}

	if _, err := manager.Start(context.Background(), Meta{}, time.Minute, block); err != nil {
		t.Fatalf("slot should be free after cancel: %v", err)
	}

	manager.Shutdown()
}

func TestProgressAndExpiry(t *testing.T) {
	manager := NewManager(2, 10*time.Millisecond)
	release := make(chan struct{})

	job, err := manager.Start(context.Background(), Meta{Kind: "scan"}, time.Minute, func(_ context.Context, j *Job) (any, error) {
		j.SetProgress(func() any { return 42 })
		<-release
		return nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(20 * time.Millisecond)

	if views := manager.List(); len(views) != 1 || views[0].Progress != 42 {
		t.Fatalf("List() = %+v", views)
	}

	close(release)
	job.Wait(context.Background(), time.Second)
	time.Sleep(20 * time.Millisecond)

	if _, err := manager.Get(job.ID); err == nil {
		t.Fatal("finished job should expire after the TTL")
	}
}
