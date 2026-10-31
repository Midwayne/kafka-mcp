package scan

import (
	"fmt"
	"hash/crc32"
	"hash/fnv"
	"strings"
)

// Partitioner maps a record key to a partition, mirroring a producer's default
// partitioner so a key lookup can read a single partition.
type Partitioner func(key []byte, partitions int) int32

var partitioners = map[string]Partitioner{
	// Java clients, Kafka Streams, franz-go, and librdkafka "murmur2_random".
	"murmur2": func(key []byte, n int) int32 { return int32(int64(murmur2(key)&0x7fffffff) % int64(n)) },
	// librdkafka default ("consistent_random"): confluent-kafka-go/python/dotnet, node-rdkafka.
	"crc32": func(key []byte, n int) int32 { return int32(int64(crc32.ChecksumIEEE(key)) % int64(n)) },
	// Sarama's default hash partitioner.
	"fnv1a": saramaFNV,
}

// PartitionerByName looks up a partitioner. Aliases: java, librdkafka, sarama.
func PartitionerByName(name string) (Partitioner, error) {
	aliases := map[string]string{"java": "murmur2", "librdkafka": "crc32", "sarama": "fnv1a"}

	key := strings.ToLower(strings.TrimSpace(name))
	if alias, ok := aliases[key]; ok {
		key = alias
	}

	partitioner, ok := partitioners[key]
	if !ok {
		return nil, fmt.Errorf("unknown key_partitioner %q (murmur2/java, crc32/librdkafka, fnv1a/sarama)", name)
	}

	return partitioner, nil
}

func saramaFNV(key []byte, n int) int32 {
	hasher := fnv.New32a()
	_, _ = hasher.Write(key)

	partition := int32(hasher.Sum32()) % int32(n) //nolint:gosec // Sarama reinterprets the hash as signed
	if partition < 0 {
		partition = -partition
	}

	return partition
}

// murmur2 is Kafka's Utils.murmur2.
func murmur2(data []byte) int32 {
	const (
		seed uint32 = 0x9747b28c
		m    uint32 = 0x5bd1e995
		r           = 24
	)

	length := len(data)
	h := seed ^ uint32(length) //nolint:gosec // Kafka keys are far below 4 GiB

	for i := 0; i+4 <= length; i += 4 {
		k := uint32(data[i]) | uint32(data[i+1])<<8 | uint32(data[i+2])<<16 | uint32(data[i+3])<<24
		k *= m
		k ^= k >> r
		k *= m
		h *= m
		h ^= k
	}

	tail := length &^ 3
	switch length % 4 {
	case 3:
		h ^= uint32(data[tail+2]) << 16
		fallthrough
	case 2:
		h ^= uint32(data[tail+1]) << 8
		fallthrough
	case 1:
		h ^= uint32(data[tail])
		h *= m
	}

	h ^= h >> 13
	h *= m
	h ^= h >> 15

	return int32(h) //nolint:gosec // Kafka's murmur2 returns the hash bits as a signed int
}
