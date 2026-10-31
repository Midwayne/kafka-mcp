// Command kafka-mcp is a Model Context Protocol server for exploring, sampling,
// and filtering messages across Kafka clusters.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/Midwayne/kafka-mcp/internal/config"
)

// version is overridden at build time with -ldflags "-X main.version=...".
var version = "dev"

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "kafka-mcp: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	configPath := flag.String("config", "", "path to the YAML or JSON config (defaults to $"+config.EnvVar+" or kafka-mcp.yaml)")
	checkOnly := flag.Bool("check", false, "connect to every cluster, report the result, and exit")
	showVersion := flag.Bool("version", false, "print the version and exit")
	flag.Parse()

	if *showVersion {
		fmt.Println(version)
		return nil
	}

	cfg, err := config.Load(*configPath)
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if *checkOnly {
		return check(ctx, cfg, os.Stdout)
	}

	return errors.New("serving is not implemented yet")
}
