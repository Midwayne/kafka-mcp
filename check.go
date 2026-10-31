package main

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/Midwayne/kafka-mcp/internal/app"
	"github.com/Midwayne/kafka-mcp/internal/config"
	"github.com/Midwayne/kafka-mcp/internal/kafka"
)

const checkTimeout = 20 * time.Second

// check connects to every broker of every enabled cluster and reports per cluster.
// It returns an error when any cluster is unreachable or rejects authentication.
func check(ctx context.Context, cfg *config.Config, out io.Writer) error {
	registry, err := kafka.NewRegistry(cfg.Clusters)
	if err != nil {
		return err
	}
	defer registry.Close()

	failures := 0

	for _, cluster := range registry.All() {
		pingCtx, cancel := context.WithTimeout(ctx, checkTimeout)
		started := time.Now()
		brokers, err := cluster.Ping(pingCtx)
		cancel()

		if err != nil {
			failures++
			fmt.Fprintf(out, "FAIL %-20s %s\n", cluster.Name(), app.Explain(err))

			continue
		}

		fmt.Fprintf(out, "OK   %-20s %d brokers reachable (%s)\n", cluster.Name(), brokers, time.Since(started).Round(time.Millisecond))
	}

	if failures > 0 {
		return fmt.Errorf("%d cluster(s) failed the check", failures)
	}

	return nil
}
