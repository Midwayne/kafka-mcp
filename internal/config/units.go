package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Duration accepts Go duration strings ("30s", "5m") plus a "d" day suffix.
type Duration time.Duration

// ByteSize accepts plain byte counts or sizes such as "64MiB" and "1GB".
type ByteSize int64

// Std returns the value as a time.Duration.
func (d Duration) Std() time.Duration { return time.Duration(d) }

// ParseDuration parses Go durations and whole-day values such as "7d".
func ParseDuration(raw string) (time.Duration, error) {
	raw = strings.TrimSpace(raw)

	if days, ok := strings.CutSuffix(raw, "d"); ok {
		n, err := strconv.ParseFloat(days, 64)
		if err != nil {
			return 0, fmt.Errorf("invalid duration %q", raw)
		}

		return time.Duration(n * float64(24*time.Hour)), nil
	}

	d, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("invalid duration %q", raw)
	}

	return d, nil
}

func (d *Duration) set(raw string) error {
	if strings.TrimSpace(raw) == "" {
		*d = 0
		return nil
	}

	parsed, err := ParseDuration(raw)
	if err != nil {
		return err
	}

	*d = Duration(parsed)
	return nil
}

// UnmarshalYAML implements yaml.Unmarshaler.
func (d *Duration) UnmarshalYAML(node *yaml.Node) error {
	return d.set(node.Value)
}

// UnmarshalJSON implements json.Unmarshaler.
func (d *Duration) UnmarshalJSON(raw []byte) error {
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return errors.New("duration must be a string such as \"30s\"")
	}

	return d.set(s)
}

var byteUnits = []struct {
	suffix string
	scale  int64
}{
	{"kib", 1 << 10}, {"mib", 1 << 20}, {"gib", 1 << 30}, {"tib", 1 << 40},
	{"kb", 1e3}, {"mb", 1e6}, {"gb", 1e9}, {"tb", 1e12},
	{"k", 1 << 10}, {"m", 1 << 20}, {"g", 1 << 30}, {"b", 1},
}

// ParseByteSize parses "1048576", "512KiB", "64MB", or "1g".
func ParseByteSize(raw string) (int64, error) {
	s := strings.ToLower(strings.TrimSpace(raw))
	if s == "" {
		return 0, nil
	}

	scale := int64(1)
	for _, unit := range byteUnits {
		if trimmed, ok := strings.CutSuffix(s, unit.suffix); ok {
			s, scale = strings.TrimSpace(trimmed), unit.scale
			break
		}
	}

	n, err := strconv.ParseFloat(s, 64)
	if err != nil || n < 0 {
		return 0, fmt.Errorf("invalid byte size %q", raw)
	}

	return int64(n * float64(scale)), nil
}

func (b *ByteSize) set(raw string) error {
	n, err := ParseByteSize(raw)
	if err != nil {
		return err
	}

	*b = ByteSize(n)
	return nil
}

// UnmarshalYAML implements yaml.Unmarshaler.
func (b *ByteSize) UnmarshalYAML(node *yaml.Node) error {
	return b.set(node.Value)
}

// UnmarshalJSON implements json.Unmarshaler.
func (b *ByteSize) UnmarshalJSON(raw []byte) error {
	var n int64
	if err := json.Unmarshal(raw, &n); err == nil {
		*b = ByteSize(n)
		return nil
	}

	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return errors.New("byte size must be a number or string such as \"64MiB\"")
	}

	return b.set(s)
}
