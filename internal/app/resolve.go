package app

import (
	"context"
	"errors"
	"fmt"
	"path"
	"slices"
	"strings"
	"time"
)

const maxScanTopics = 50

// resolveTopics expands globs against the cluster's visible topics and checks literals.
func (h *handle) resolveTopics(ctx context.Context, patterns []string) ([]string, error) {
	if len(patterns) == 0 {
		return nil, errors.New("topics is required")
	}

	var resolved, globs []string

	for _, pattern := range patterns {
		pattern = strings.TrimSpace(pattern)

		if strings.ContainsAny(pattern, "*?[") {
			globs = append(globs, pattern)
			continue
		}

		if err := h.checkTopic(pattern); err != nil {
			return nil, err
		}

		resolved = append(resolved, pattern)
	}

	if len(globs) > 0 {
		matched, err := h.matchTopics(ctx, globs)
		if err != nil {
			return nil, err
		}

		resolved = append(resolved, matched...)
	}

	slices.Sort(resolved)
	resolved = slices.Compact(resolved)

	if len(resolved) == 0 {
		return nil, fmt.Errorf("no visible topics match %v (see list_topics)", patterns)
	}

	if len(resolved) > maxScanTopics {
		return nil, fmt.Errorf("%d topics match; narrow the pattern to at most %d", len(resolved), maxScanTopics)
	}

	return resolved, nil
}

func (h *handle) matchTopics(ctx context.Context, globs []string) ([]string, error) {
	listed, err := h.Admin().ListTopics(ctx)
	if err != nil {
		return nil, fmt.Errorf("list topics: %w", err)
	}

	var matched []string

	for _, name := range listed.Names() {
		if !h.Config().TopicAllowed(name) {
			continue
		}

		for _, glob := range globs {
			if ok, _ := path.Match(glob, name); ok {
				matched = append(matched, name)
				break
			}
		}
	}

	return matched, nil
}

// Wait bounds for tool calls; MCP clients commonly time out around 60 seconds.
const (
	defaultWait = 20 * time.Second
	maxWait     = 50 * time.Second
)

// clampInt returns def for non-positive v and caps the result at limit.
func clampInt[T ~int | ~int64](v, def, limit T) T {
	if v <= 0 {
		v = def
	}

	if limit > 0 && v > limit {
		return limit
	}

	return v
}

func waitDuration(seconds *int) time.Duration {
	if seconds == nil {
		return defaultWait
	}

	return min(time.Duration(max(*seconds, 0))*time.Second, maxWait)
}

func (s *Service) timeout(seconds int) time.Duration {
	limits := s.cfg.Defaults.Scan
	requested := time.Duration(seconds) * time.Second

	return clampInt(requested, limits.DefaultTimeout.Std(), limits.MaxTimeout.Std())
}
