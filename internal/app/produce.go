package app

import (
	"context"
	"encoding/base64"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Midwayne/kafka-mcp/internal/scan"
)

// ProduceSpec writes one message. Only clusters with access: read_write accept it.
type ProduceSpec struct {
	Cluster     string            `json:"cluster" jsonschema:"Cluster name (must be read_write)."`
	Topic       string            `json:"topic" jsonschema:"Topic to write to."`
	Key         *string           `json:"key,omitempty" jsonschema:"Record key. Omit for a null key."`
	Value       *string           `json:"value,omitempty" jsonschema:"Record value. Omit for a null value (tombstone)."`
	ValueBase64 bool              `json:"value_base64,omitempty" jsonschema:"Treat value as base64-encoded binary."`
	Headers     map[string]string `json:"headers,omitempty" jsonschema:"Record headers."`
	Partition   *int32            `json:"partition,omitempty" jsonschema:"Explicit partition. Default: the producer's key-hash partitioner."`
}

// ProduceResult reports where the message landed.
type ProduceResult struct {
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	Offset    int64  `json:"offset"`
	Timestamp string `json:"timestamp"`
}

// ProduceMessage writes one record synchronously.
func (s *Service) ProduceMessage(ctx context.Context, spec ProduceSpec) (ProduceResult, error) {
	h, err := s.cluster(spec.Cluster, ToolProduceMessage)
	if err != nil {
		return ProduceResult{}, err
	}

	if err := h.checkWrite(); err != nil {
		return ProduceResult{}, err
	}

	if err := h.checkTopic(spec.Topic); err != nil {
		return ProduceResult{}, err
	}

	rec, err := buildRecord(spec)
	if err != nil {
		return ProduceResult{}, err
	}

	if rec.Partition, err = h.choosePartition(ctx, spec); err != nil {
		return ProduceResult{}, err
	}

	producer, err := h.Producer()
	if err != nil {
		return ProduceResult{}, err
	}

	if err := producer.ProduceSync(ctx, rec).FirstErr(); err != nil {
		return ProduceResult{}, fmt.Errorf("produce to %s: %w", spec.Topic, err)
	}

	return ProduceResult{Topic: rec.Topic, Partition: rec.Partition, Offset: rec.Offset, Timestamp: rec.Timestamp.UTC().Format(time.RFC3339Nano)}, nil
}

func buildRecord(spec ProduceSpec) (*kgo.Record, error) {
	rec := &kgo.Record{Topic: spec.Topic}

	if spec.Key != nil {
		rec.Key = []byte(*spec.Key)
	}

	if spec.Value != nil {
		rec.Value = []byte(*spec.Value)

		if spec.ValueBase64 {
			decoded, err := base64.StdEncoding.DecodeString(*spec.Value)
			if err != nil {
				return nil, fmt.Errorf("value is not valid base64: %w", err)
			}
			rec.Value = decoded
		}
	}

	for name, value := range spec.Headers {
		rec.Headers = append(rec.Headers, kgo.RecordHeader{Key: name, Value: []byte(value)})
	}

	return rec, nil
}

// choosePartition honours an explicit partition, otherwise hashes the key like
// the Java producer (murmur2), or picks a random partition for null keys.
func (h *handle) choosePartition(ctx context.Context, spec ProduceSpec) (int32, error) {
	details, err := h.Admin().ListTopics(ctx, spec.Topic)
	if err != nil || !details.Has(spec.Topic) || details[spec.Topic].Err != nil {
		return 0, fmt.Errorf("topic %q not found on cluster %q", spec.Topic, h.Name())
	}

	count := len(details[spec.Topic].Partitions)

	switch {
	case spec.Partition != nil:
		if *spec.Partition < 0 || int(*spec.Partition) >= count {
			return 0, fmt.Errorf("topic %q has %d partitions", spec.Topic, count)
		}
		return *spec.Partition, nil
	case spec.Key != nil:
		murmur2, _ := scan.PartitionerByName("murmur2")
		return murmur2([]byte(*spec.Key), count), nil
	default:
		return int32(rand.IntN(count)), nil //nolint:gosec // load spreading, not security
	}
}
