package auth

import (
	"context"
	"sync"
)

// lazy loads a value on first use. Successful loads are cached; failures are
// returned without caching so transient errors are retried on the next call.
type lazy[T any] struct {
	load func(context.Context) (T, error)

	mu     sync.Mutex
	value  T
	loaded bool
}

func newLazy[T any](load func(context.Context) (T, error)) *lazy[T] {
	return &lazy[T]{load: load}
}

func (l *lazy[T]) get(ctx context.Context) (T, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.loaded {
		return l.value, nil
	}

	value, err := l.load(ctx)
	if err != nil {
		var zero T
		return zero, err
	}

	l.value, l.loaded = value, true
	return value, nil
}
