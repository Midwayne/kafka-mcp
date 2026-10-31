// Package schemaregistry fetches schemas from a Confluent-compatible registry
// and decodes Avro, Protobuf, and JSON Schema payloads.
package schemaregistry

import (
	"context"
	"crypto/tls"
	"fmt"
	"sync"

	"github.com/twmb/franz-go/pkg/sr"

	"github.com/Midwayne/kafka-mcp/internal/auth"
	"github.com/Midwayne/kafka-mcp/internal/config"
)

// LatestVersion selects the newest schema version of a subject.
const LatestVersion = -1

// Client wraps a registry client with a per-schema decoder cache.
type Client struct {
	registry *sr.Client

	mu    sync.Mutex
	cache map[int]*cacheEntry
}

// New builds a registry client. When the registry has no tls block it inherits
// the cluster's TLS material (common with a shared corporate CA or keystore).
func New(cfg *config.SchemaRegistry, clusterTLS *config.TLS) (*Client, error) {
	opts := []sr.ClientOpt{sr.URLs(cfg.URLs...), sr.UserAgent("kafka-mcp")}

	switch {
	case cfg.BearerToken != "":
		opts = append(opts, sr.BearerToken(cfg.BearerToken))
	case cfg.Username != "":
		opts = append(opts, sr.BasicAuth(cfg.Username, cfg.Password))
	}

	tlsConfig, err := registryTLS(cfg.TLS, clusterTLS)
	if err != nil {
		return nil, fmt.Errorf("schema_registry tls: %w", err)
	}

	if tlsConfig != nil {
		opts = append(opts, sr.DialTLSConfig(tlsConfig))
	}

	registry, err := sr.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("schema_registry: %w", err)
	}

	return &Client{registry: registry, cache: make(map[int]*cacheEntry)}, nil
}

func registryTLS(own, cluster *config.TLS) (*tls.Config, error) {
	if own != nil {
		return auth.TLSConfig(own)
	}

	inherited, err := auth.TLSConfig(cluster)
	if err != nil || inherited == nil {
		return inherited, err
	}

	// The broker's SNI override never applies to the registry host.
	inherited.ServerName = ""

	return inherited, nil
}

// Subjects lists registered subjects.
func (c *Client) Subjects(ctx context.Context) ([]string, error) {
	subjects, err := c.registry.Subjects(ctx)
	if err != nil {
		return nil, fmt.Errorf("list subjects: %w", err)
	}

	return subjects, nil
}

// SubjectVersions lists versions registered for a subject.
func (c *Client) SubjectVersions(ctx context.Context, subject string) ([]int, error) {
	versions, err := c.registry.SubjectVersions(ctx, subject)
	if err != nil {
		return nil, fmt.Errorf("list versions of %q: %w", subject, err)
	}

	return versions, nil
}

// SchemaByVersion returns one subject version, or the latest for LatestVersion.
func (c *Client) SchemaByVersion(ctx context.Context, subject string, version int) (sr.SubjectSchema, error) {
	schema, err := c.registry.SchemaByVersion(ctx, subject, version)
	if err != nil {
		return sr.SubjectSchema{}, fmt.Errorf("fetch %q version %d: %w", subject, version, err)
	}

	return schema, nil
}

// SchemaByID returns the schema registered under an ID.
func (c *Client) SchemaByID(ctx context.Context, id int) (sr.Schema, error) {
	schema, err := c.registry.SchemaByID(ctx, id)
	if err != nil {
		return sr.Schema{}, fmt.Errorf("fetch schema id %d: %w", id, err)
	}

	return schema, nil
}

// Decode implements codec.SchemaResolver.
func (c *Client) Decode(ctx context.Context, schemaID int, payload []byte) (any, string, error) {
	decoder, err := c.decoder(ctx, schemaID)
	if err != nil {
		return nil, "", err
	}

	value, err := decoder.decode(payload)
	if err != nil {
		return nil, decoder.format(), fmt.Errorf("decode %s (schema id %d): %w", decoder.format(), schemaID, err)
	}

	return value, decoder.format(), nil
}
