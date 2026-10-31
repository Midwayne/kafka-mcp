// Command kafka-mcp is a Model Context Protocol server for exploring, sampling,
// and filtering messages across Kafka clusters.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/Midwayne/kafka-mcp/internal/app"
	"github.com/Midwayne/kafka-mcp/internal/config"
	"github.com/Midwayne/kafka-mcp/internal/server"
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
	configPath := flag.String("config", "", "path to the YAML or JSON config (defaults to $"+config.EnvVar+", ./kafka-mcp.yaml, or ~/.kafka-mcp/config.yaml)")
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

	return serve(ctx, cfg)
}

func serve(ctx context.Context, cfg *config.Config) error {
	svc, err := app.New(cfg)
	if err != nil {
		return err
	}
	defer svc.Close()

	serverVersion := cfg.Server.Version
	if version != "dev" {
		serverVersion = version
	}

	// Logs go to stderr: in stdio mode stdout carries the MCP protocol.
	fmt.Fprintf(os.Stderr, "kafka-mcp %s: %d cluster(s), serving over %s\n", serverVersion, len(cfg.Clusters), cfg.Server.ServeMode)

	return server.Serve(ctx, server.New(svc, cfg.Server.Name, serverVersion), cfg.Server)
}
