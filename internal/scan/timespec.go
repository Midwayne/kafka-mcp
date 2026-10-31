// Package scan reads bounded offset ranges in parallel and feeds matching
// messages to a sink.
package scan

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/Midwayne/kafka-mcp/internal/config"
)

// PointKind classifies a position in a partition's log.
type PointKind int

// Point kinds.
const (
	Earliest PointKind = iota
	Latest
	AtTime
)

// Point is a parsed "from" or "to" bound.
type Point struct {
	Kind   PointKind
	Millis int64
}

// unixMillisThreshold separates Unix seconds from milliseconds (year ~2001 in ms).
const unixMillisThreshold = 1e12

var timeLayouts = []string{time.RFC3339Nano, "2006-01-02T15:04:05", "2006-01-02 15:04:05", "2006-01-02"}

// ParsePoint accepts earliest/latest/now, RFC 3339 or date strings (UTC when no
// zone is given), Unix seconds or milliseconds, and relative times such as
// "-15m", "-2h", "-7d", or "3h ago".
func ParsePoint(raw string, now time.Time) (Point, error) {
	value := strings.ToLower(strings.TrimSpace(raw))

	switch value {
	case "", "earliest", "beginning", "start", "oldest":
		return Point{Kind: Earliest}, nil
	case "latest", "now", "end", "newest":
		return Point{Kind: Latest}, nil
	}

	if relative, ok := parseRelative(value); ok {
		d, err := config.ParseDuration(relative)
		if err != nil {
			return Point{}, fmt.Errorf("invalid relative time %q", raw)
		}

		return Point{Kind: AtTime, Millis: now.Add(-d).UnixMilli()}, nil
	}

	if n, err := strconv.ParseInt(value, 10, 64); err == nil {
		if n < unixMillisThreshold {
			n *= 1000
		}

		return Point{Kind: AtTime, Millis: n}, nil
	}

	for _, layout := range timeLayouts {
		if t, err := time.Parse(layout, strings.TrimSpace(raw)); err == nil {
			return Point{Kind: AtTime, Millis: t.UnixMilli()}, nil
		}
	}

	return Point{}, fmt.Errorf("invalid time %q (use earliest, latest, RFC 3339, Unix time, or -1h style offsets)", raw)
}

func parseRelative(value string) (string, bool) {
	if rest, ok := strings.CutPrefix(value, "-"); ok {
		return rest, true
	}

	if rest, ok := strings.CutSuffix(value, " ago"); ok {
		return strings.ReplaceAll(rest, " ", ""), true
	}

	return "", false
}
