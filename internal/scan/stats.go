package scan

import (
	"sync"
	"sync/atomic"
)

// Reasons a scan stopped.
const (
	ReasonCompleted  = "completed"   // every range was read to its end
	ReasonLimit      = "limit"       // the sink is full
	ReasonMaxScanned = "max_scanned" // the record budget was used up
	ReasonMaxBytes   = "max_bytes"   // the byte budget was used up
	ReasonTimeout    = "timeout"     // the time budget ran out
	ReasonCancelled  = "cancelled"   // the caller cancelled the scan
	ReasonIdle       = "idle"        // no records arrived within idle_timeout before the ranges ended
)

const maxSamples = 5

// Stats describes a scan's progress or final outcome.
type Stats struct {
	Scanned        int64                       `json:"scanned"`
	Matched        int64                       `json:"matched"`
	Bytes          int64                       `json:"bytes"`
	FilterErrors   int64                       `json:"filter_errors"`
	ErrorSamples   []string                    `json:"filter_error_samples,omitempty"`
	Warnings       []string                    `json:"warnings,omitempty"`
	Partitions     int                         `json:"partitions"`
	PartitionsDone int                         `json:"partitions_done"`
	ElapsedMs      int64                       `json:"elapsed_ms"`
	RecordsPerSec  int64                       `json:"records_per_sec"`
	StopReason     string                      `json:"stop_reason,omitempty"`
	NextOffsets    map[string]map[string]int64 `json:"next_offsets,omitempty"`
}

// counters are updated concurrently by the poll loop and workers.
type counters struct {
	scanned      atomic.Int64
	matched      atomic.Int64
	bytes        atomic.Int64
	filterErrors atomic.Int64

	mu       sync.Mutex
	samples  []string
	warnings []string
}

func (c *counters) filterError(err error) {
	c.filterErrors.Add(1)
	c.note(&c.samples, err.Error())
}

func (c *counters) warn(message string) {
	c.note(&c.warnings, message)
}

// note keeps the first few distinct messages.
func (c *counters) note(list *[]string, message string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(*list) >= maxSamples {
		return
	}

	for _, existing := range *list {
		if existing == message {
			return
		}
	}

	*list = append(*list, message)
}

func (c *counters) fill(stats *Stats) {
	stats.Scanned = c.scanned.Load()
	stats.Matched = c.matched.Load()
	stats.Bytes = c.bytes.Load()
	stats.FilterErrors = c.filterErrors.Load()

	c.mu.Lock()
	defer c.mu.Unlock()

	stats.ErrorSamples = append([]string(nil), c.samples...)
	stats.Warnings = append([]string(nil), c.warnings...)
}
