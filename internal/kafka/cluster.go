// Package kafka owns the franz-go clients for each configured cluster.
package kafka

import (
	"cmp"
	"context"
	"fmt"
	"math"
	"sync"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Midwayne/kafka-mcp/internal/auth"
	"github.com/Midwayne/kafka-mcp/internal/config"
)

// Cluster holds the connection settings and shared clients for one cluster.
type Cluster struct {
	cfg  config.Cluster
	opts []kgo.Opt

	admin *kadm.Client

	mu       sync.Mutex
	producer *kgo.Client
}

// NewCluster validates TLS/SASL material and prepares (but does not dial) clients.
func NewCluster(cfg config.Cluster) (*Cluster, error) {
	opts, err := connectionOptions(cfg)
	if err != nil {
		return nil, fmt.Errorf("cluster %q: %w", cfg.Name, err)
	}

	adminClient, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("cluster %q: %w", cfg.Name, err)
	}

	return &Cluster{cfg: cfg, opts: opts, admin: kadm.NewClient(adminClient)}, nil
}

func connectionOptions(cfg config.Cluster) ([]kgo.Opt, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ClientID(cfg.ClientID),
		kgo.DialTimeout(cfg.DialTimeout.Std()),
		kgo.RequestTimeoutOverhead(cfg.RequestTimeout.Std()),
	}

	tlsConfig, err := auth.TLSConfig(cfg.TLS)
	if err != nil {
		return nil, err
	}
	if tlsConfig != nil {
		opts = append(opts, kgo.DialTLSConfig(tlsConfig))
	}

	mechanism, err := auth.Mechanism(cfg.SASL)
	if err != nil {
		return nil, err
	}
	if mechanism != nil {
		opts = append(opts, kgo.SASL(mechanism))
	}

	return opts, nil
}

// Name returns the configured cluster name.
func (c *Cluster) Name() string { return c.cfg.Name }

// Config returns the normalized cluster configuration.
func (c *Cluster) Config() config.Cluster { return c.cfg }

// Admin returns the shared admin client.
func (c *Cluster) Admin() *kadm.Client { return c.admin }

// Ping verifies connectivity and authentication to every advertised broker,
// not just the bootstrap address: brokers often advertise different hosts
// (or raw IPs) whose certificates and routes must also work.
func (c *Cluster) Ping(ctx context.Context) (int, error) {
	if _, err := c.admin.BrokerMetadata(ctx); err != nil {
		return 0, fmt.Errorf("cluster %q: %w", c.cfg.Name, err)
	}

	versions, err := c.admin.ApiVersions(ctx)
	if err != nil {
		return 0, fmt.Errorf("cluster %q: %w", c.cfg.Name, err)
	}

	var firstErr error

	failed := 0
	for _, broker := range versions {
		if broker.Err != nil {
			failed++
			firstErr = cmp.Or(firstErr, broker.Err)
		}
	}

	if failed > 0 {
		return len(versions), fmt.Errorf("cluster %q: %d of %d brokers failed: %w", c.cfg.Name, failed, len(versions), firstErr)
	}

	return len(versions), nil
}

// Consumer creates a short-lived, group-less client that reads the given
// partitions directly. It never commits offsets, so it cannot disturb
// production consumer groups. Callers must Close it.
func (c *Cluster) Consumer(assignments map[string]map[int32]kgo.Offset) (*kgo.Client, error) {
	isolation := kgo.ReadUncommitted()
	if c.cfg.Isolation == "read_committed" {
		isolation = kgo.ReadCommitted()
	}

	opts := append(append([]kgo.Opt{}, c.opts...),
		kgo.ConsumePartitions(assignments),
		kgo.KeepControlRecords(),
		kgo.FetchIsolationLevel(isolation),
		kgo.FetchMaxBytes(toInt32(int64(c.cfg.Fetch.MaxBytes))),
		kgo.FetchMaxPartitionBytes(toInt32(int64(c.cfg.Fetch.MaxPartitionBytes))),
	)

	if c.cfg.Rack != "" {
		opts = append(opts, kgo.Rack(c.cfg.Rack))
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("cluster %q consumer: %w", c.cfg.Name, err)
	}

	return client, nil
}

// Producer returns a shared producer, creating it on first use. Records must
// carry their target partition; callers choose it explicitly.
func (c *Cluster) Producer() (*kgo.Client, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.producer != nil {
		return c.producer, nil
	}

	opts := append(append([]kgo.Opt{}, c.opts...), kgo.RecordPartitioner(kgo.ManualPartitioner()))

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("cluster %q producer: %w", c.cfg.Name, err)
	}

	c.producer = client
	return client, nil
}

// Close releases every client owned by the cluster.
func (c *Cluster) Close() {
	c.admin.Close()

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.producer != nil {
		c.producer.Close()
		c.producer = nil
	}
}

// toInt32 clamps a size to Kafka's int32 protocol fields.
func toInt32(v int64) int32 {
	if v > math.MaxInt32 {
		return math.MaxInt32
	}

	if v < 0 {
		return 0
	}

	return int32(v)
}
