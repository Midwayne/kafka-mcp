package scan

import (
	"bytes"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Prefilter holds cheap checks on record metadata that run before any
// matcher, decoding, or allocation.
type Prefilter struct {
	Key     []byte            // exact key bytes; nil matches any key
	Headers map[string]string // exact header values
	FromMs  int64             // inclusive lower timestamp bound; 0 disables
	ToMs    int64             // exclusive upper timestamp bound; 0 disables
}

// Match reports whether the record passes every configured check.
func (p *Prefilter) Match(r *kgo.Record) bool {
	if p.FromMs > 0 || p.ToMs > 0 {
		ts := r.Timestamp.UnixMilli()
		if (p.FromMs > 0 && ts < p.FromMs) || (p.ToMs > 0 && ts >= p.ToMs) {
			return false
		}
	}

	if p.Key != nil && !bytes.Equal(r.Key, p.Key) {
		return false
	}

	return headersMatch(r.Headers, p.Headers)
}

func headersMatch(headers []kgo.RecordHeader, want map[string]string) bool {
	for name, value := range want {
		found := false

		for _, header := range headers {
			if header.Key == name && string(header.Value) == value {
				found = true
				break
			}
		}

		if !found {
			return false
		}
	}

	return true
}
