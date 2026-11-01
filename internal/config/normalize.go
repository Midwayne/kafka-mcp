package config

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

// Access modes. Write tools (produce) require read_write.
const (
	AccessReadOnly  = "read_only"
	AccessReadWrite = "read_write"
)

// Defaults used when the config omits a value.
const (
	DefaultServerName     = "Kafka MCP"
	DefaultServerVersion  = "1.0.0"
	DefaultServeMode      = "stdio"
	DefaultHost           = "127.0.0.1"
	DefaultPort           = 8090
	DefaultStateDir       = "~/.kafka-mcp"
	DefaultClientID       = "kafka-mcp"
	DefaultIsolation      = "read_uncommitted"
	DefaultDialTimeout    = 10 * time.Second
	DefaultRequestTimeout = 30 * time.Second
)

// Normalize applies defaults and validates the whole document.
func (c *Config) Normalize() error {
	if err := c.Server.normalize(); err != nil {
		return err
	}

	if err := c.Defaults.normalize(); err != nil {
		return err
	}

	if len(c.Clusters) == 0 {
		return errors.New("config must declare at least one cluster")
	}

	seen := make(map[string]bool, len(c.Clusters))
	for i := range c.Clusters {
		cluster := &c.Clusters[i]

		if err := cluster.normalize(c.Defaults); err != nil {
			return err
		}

		if seen[cluster.Name] {
			return fmt.Errorf("duplicate cluster name %q", cluster.Name)
		}
		seen[cluster.Name] = true
	}

	return nil
}

func (d *Defaults) normalize() error {
	access, err := ParseAccess(orString(d.Access, AccessReadOnly))
	if err != nil {
		return fmt.Errorf("defaults.access: %w", err)
	}
	d.Access = access

	isolation, err := parseIsolation(orString(d.Isolation, DefaultIsolation))
	if err != nil {
		return fmt.Errorf("defaults.isolation: %w", err)
	}
	d.Isolation = isolation

	d.ClientID = orString(d.ClientID, DefaultClientID)
	d.DialTimeout = orDuration(d.DialTimeout, DefaultDialTimeout)
	d.RequestTimeout = orDuration(d.RequestTimeout, DefaultRequestTimeout)

	d.Fetch = d.Fetch.withDefaults(Fetch{MaxBytes: 50 << 20, MaxPartitionBytes: 1 << 20})
	d.Scan.normalize()
	d.Jobs.normalize()

	return d.Scan.validate()
}

func (s *ScanLimits) normalize() {
	s.DefaultLimit = orInt(s.DefaultLimit, 20)
	s.MaxLimit = orInt(s.MaxLimit, 500)
	s.MaxValueChars = orInt(s.MaxValueChars, 1000)

	if s.MaxScanned == 0 {
		s.MaxScanned = 5_000_000
	}
	if s.MaxBytes == 0 {
		s.MaxBytes = 2 << 30
	}
	if s.MaxExportRecords == 0 {
		s.MaxExportRecords = 1_000_000
	}

	s.DefaultTimeout = orDuration(s.DefaultTimeout, time.Minute)
	s.MaxTimeout = orDuration(s.MaxTimeout, 15*time.Minute)
	s.IdleTimeout = orDuration(s.IdleTimeout, 30*time.Second)
}

func (s *ScanLimits) validate() error {
	if s.Workers < 0 || s.MaxScanned < 0 || s.RateLimit < 0 {
		return errors.New("defaults.scan values must not be negative")
	}

	if s.DefaultLimit > s.MaxLimit {
		return errors.New("defaults.scan.default_limit exceeds max_limit")
	}

	if s.DefaultTimeout > s.MaxTimeout {
		return errors.New("defaults.scan.default_timeout exceeds max_timeout")
	}

	return nil
}

func (f Fetch) withDefaults(defaults Fetch) Fetch {
	if f.MaxBytes <= 0 {
		f.MaxBytes = defaults.MaxBytes
	}

	if f.MaxPartitionBytes <= 0 {
		f.MaxPartitionBytes = defaults.MaxPartitionBytes
	}

	return f
}

func (j *JobLimits) normalize() {
	j.MaxConcurrent = orInt(j.MaxConcurrent, 4)
	j.TTL = orDuration(j.TTL, time.Hour)
}

// ParseAccess normalizes access mode spellings.
func ParseAccess(raw string) (string, error) {
	switch strings.ToLower(strings.ReplaceAll(strings.TrimSpace(raw), "-", "_")) {
	case "read_only", "readonly", "ro", "read":
		return AccessReadOnly, nil
	case "read_write", "readwrite", "rw", "write":
		return AccessReadWrite, nil
	default:
		return "", fmt.Errorf("invalid access %q (want read_only or read_write)", raw)
	}
}

func parseIsolation(raw string) (string, error) {
	switch strings.ToLower(strings.ReplaceAll(strings.TrimSpace(raw), "-", "_")) {
	case "read_uncommitted", "uncommitted":
		return "read_uncommitted", nil
	case "read_committed", "committed":
		return "read_committed", nil
	default:
		return "", fmt.Errorf("invalid isolation %q (want read_uncommitted or read_committed)", raw)
	}
}

func orString(v, fallback string) string {
	if strings.TrimSpace(v) == "" {
		return fallback
	}

	return strings.TrimSpace(v)
}

func orInt(v, fallback int) int {
	if v == 0 {
		return fallback
	}

	return v
}

func orDuration(v Duration, fallback time.Duration) Duration {
	if v == 0 {
		return Duration(fallback)
	}

	return v
}
