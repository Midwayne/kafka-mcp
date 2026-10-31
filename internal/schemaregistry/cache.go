package schemaregistry

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/sr"

	"github.com/Midwayne/kafka-mcp/internal/codec"
)

// failureRetryAfter limits how often a failing schema ID is re-fetched, so a
// scan over millions of records does not hammer the registry.
const failureRetryAfter = 30 * time.Second

// payloadDecoder decodes the bytes that follow the wire-format header.
type payloadDecoder interface {
	decode(payload []byte) (any, error)
	format() string
}

// cacheEntry fields other than ready are written once, before ready is closed.
type cacheEntry struct {
	ready     chan struct{}
	decoder   payloadDecoder
	err       error
	fetchedAt time.Time
}

func (e *cacheEntry) expired() bool {
	select {
	case <-e.ready:
		return e.err != nil && time.Since(e.fetchedAt) > failureRetryAfter
	default:
		return false
	}
}

// decoder returns the cached decoder for an ID. Concurrent callers for the same
// new ID wait for a single fetch instead of each querying the registry.
func (c *Client) decoder(ctx context.Context, id int) (payloadDecoder, error) {
	entry, owner := c.claim(id)

	if owner {
		entry.decoder, entry.err = c.build(ctx, id)
		entry.fetchedAt = time.Now()

		// Cancellation says nothing about the schema, so retry it immediately.
		if errors.Is(entry.err, context.Canceled) || errors.Is(entry.err, context.DeadlineExceeded) {
			entry.fetchedAt = time.Time{}
		}

		close(entry.ready)
	}

	select {
	case <-entry.ready:
		return entry.decoder, entry.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// claim returns the live entry for id, or installs a new one that the caller must fill.
func (c *Client) claim(id int) (*cacheEntry, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if entry, ok := c.cache[id]; ok && !entry.expired() {
		return entry, false
	}

	entry := &cacheEntry{ready: make(chan struct{})}
	c.cache[id] = entry

	return entry, true
}

func (c *Client) build(ctx context.Context, id int) (payloadDecoder, error) {
	schema, err := c.SchemaByID(ctx, id)
	if err != nil {
		return nil, err
	}

	switch schema.Type {
	case sr.TypeAvro:
		return newAvroDecoder(schema)
	case sr.TypeProtobuf:
		return newProtobufDecoder(ctx, c, id, schema)
	case sr.TypeJSON:
		return jsonSchemaDecoder{}, nil
	default:
		return nil, fmt.Errorf("schema id %d has unsupported type %v", id, schema.Type)
	}
}

// jsonSchemaDecoder handles JSON Schema payloads, which are plain JSON after the header.
type jsonSchemaDecoder struct{}

func (jsonSchemaDecoder) decode(payload []byte) (any, error) { return codec.DecodeJSON(payload) }

func (jsonSchemaDecoder) format() string { return codec.FormatJSONSchema }
