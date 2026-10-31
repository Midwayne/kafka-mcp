package testutil

import (
	"context"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
)

// FakeCluster starts an in-memory Kafka cluster and returns its broker addresses.
func FakeCluster(tb testing.TB, opts ...kfake.Opt) []string {
	tb.Helper()

	return NewFakeCluster(tb, opts...).ListenAddrs()
}

// NewFakeCluster starts an in-memory Kafka cluster, for tests that inject behaviour.
func NewFakeCluster(tb testing.TB, opts ...kfake.Opt) *kfake.Cluster {
	tb.Helper()

	cluster, err := kfake.NewCluster(append([]kfake.Opt{kfake.NumBrokers(1)}, opts...)...)
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(cluster.Close)

	return cluster
}

// Produce synchronously writes records using a plaintext client.
func Produce(tb testing.TB, brokers []string, records ...*kgo.Record) {
	tb.Helper()

	ProduceWith(tb, []kgo.Opt{kgo.SeedBrokers(brokers...)}, records...)
}

// ProduceWith synchronously writes records using custom client options.
func ProduceWith(tb testing.TB, opts []kgo.Opt, records ...*kgo.Record) {
	tb.Helper()

	opts = append(opts, kgo.RecordPartitioner(kgo.ManualPartitioner()))

	client, err := kgo.NewClient(opts...)
	if err != nil {
		tb.Fatal(err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := client.ProduceSync(ctx, records...).FirstErr(); err != nil {
		tb.Fatalf("produce: %v", err)
	}
}

// Record builds a record for a fixed partition with an explicit timestamp.
func Record(topic string, partition int32, key, value string, ts time.Time) *kgo.Record {
	record := &kgo.Record{Topic: topic, Partition: partition, Value: []byte(value), Timestamp: ts}
	if key != "" {
		record.Key = []byte(key)
	}

	return record
}
