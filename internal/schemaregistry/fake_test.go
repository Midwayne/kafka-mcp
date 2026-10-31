package schemaregistry

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/twmb/franz-go/pkg/sr"

	"github.com/Midwayne/kafka-mcp/internal/config"
)

// fakeRegistry serves the subset of the Schema Registry REST API the client uses.
type fakeRegistry struct {
	mu       sync.Mutex
	byID     map[int]sr.Schema
	versions map[string]sr.SubjectSchema
	hits     map[string]int
}

func newFakeRegistry(t *testing.T) (*fakeRegistry, *Client) {
	t.Helper()

	fake := &fakeRegistry{
		byID:     map[int]sr.Schema{},
		versions: map[string]sr.SubjectSchema{},
		hits:     map[string]int{},
	}

	server := httptest.NewServer(fake)
	t.Cleanup(server.Close)

	client, err := New(&config.SchemaRegistry{URLs: []string{server.URL}}, nil)
	if err != nil {
		t.Fatal(err)
	}

	return fake, client
}

func (f *fakeRegistry) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.hits[r.URL.Path]++

	var body any
	var id int
	var subject string
	var version int

	switch {
	case r.URL.Path == "/subjects":
		body = []string{"orders-value"}
	case scan(r.URL.Path, "/schemas/ids/%d", &id):
		schema, ok := f.byID[id]
		if !ok {
			http.Error(w, `{"error_code":40403,"message":"Schema not found"}`, http.StatusNotFound)
			return
		}
		body = schema
	case scan(strings.Replace(r.URL.Path, "/versions/", " ", 1), "/subjects/%s %d", &subject, &version):
		body = f.versions[fmt.Sprintf("%s/%d", subject, version)]
	default:
		http.NotFound(w, r)
		return
	}

	w.Header().Set("Content-Type", "application/json")

	if err := json.NewEncoder(w).Encode(body); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (f *fakeRegistry) hitCount(path string) int {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.hits[path]
}

func scan(path, format string, args ...any) bool {
	n, err := fmt.Sscanf(path, format, args...)
	return err == nil && n == len(args)
}

// frame prepends the Confluent wire header and optional protobuf message indexes.
func frame(id int, indexes []int64, body []byte) []byte {
	out := []byte{0, 0, 0, 0, 0}
	binary.BigEndian.PutUint32(out[1:], uint32(id))

	if indexes != nil {
		out = binary.AppendVarint(out, int64(len(indexes)))
		for _, index := range indexes {
			out = binary.AppendVarint(out, index)
		}
	}

	return append(out, body...)
}
