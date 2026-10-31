package app

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/Midwayne/kafka-mcp/internal/scan"
)

const defaultScanFrom = "-1h"

func selection(topics []string, spec ScanSpec) (scan.Selection, error) {
	sel := scan.Selection{Topics: topics, Partitions: spec.Partitions}

	if spec.Key != nil {
		sel.Key = []byte(*spec.Key)
	}

	if spec.KeyPartitioner != "" {
		if spec.Key == nil {
			return sel, errors.New("key_partitioner requires key")
		}

		partitioner, err := scan.PartitionerByName(spec.KeyPartitioner)
		if err != nil {
			return sel, err
		}

		sel.Partitioner = partitioner
	}

	return sel, nil
}

func boundsFor(w Window) (scan.Bounds, error) {
	bounds := scan.Bounds{From: w.From, To: w.To, StartOffset: w.StartOffset, EndOffset: w.EndOffset, Follow: w.Follow}

	if bounds.From == "" && bounds.StartOffset == nil && len(w.ResumeOffsets) == 0 {
		bounds.From = defaultScanFrom
		if w.Follow {
			bounds.From = "latest"
		}
	}

	resume, err := parseResume(w.ResumeOffsets)
	bounds.Resume = resume

	return bounds, err
}

func parseResume(raw map[string]map[string]int64) (map[string]map[int32]int64, error) {
	out := make(map[string]map[int32]int64, len(raw))

	for topic, partitions := range raw {
		out[topic] = make(map[int32]int64, len(partitions))

		for partition, offset := range partitions {
			id, err := strconv.ParseInt(partition, 10, 32)
			if err != nil || id < 0 {
				return nil, fmt.Errorf("resume_offsets: invalid partition %q for topic %q", partition, topic)
			}

			out[topic][int32(id)] = offset
		}
	}

	return out, nil
}
