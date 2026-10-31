package saved

import (
	"encoding/json"
	"path/filepath"
	"testing"
)

func TestStorePersistsAcrossReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state", "saved-scans.json")

	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}

	first, err := store.Put(Entry{Name: "failed-trailers", Kind: "scan", Spec: json.RawMessage(`{"topics":["a"]}`)})
	if err != nil {
		t.Fatal(err)
	}

	updated, err := store.Put(Entry{Name: "failed-trailers", Kind: "count", Spec: json.RawMessage(`{"topics":["b"]}`)})
	if err != nil || !updated.CreatedAt.Equal(first.CreatedAt) {
		t.Fatalf("update should keep creation time: %v, %v", updated, err)
	}

	if _, err := store.Put(Entry{Name: "another", Kind: "scan", Spec: json.RawMessage(`{}`)}); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}

	entries := reopened.List()
	if len(entries) != 2 || entries[0].Name != "another" || entries[1].Kind != "count" {
		t.Fatalf("reopened entries = %+v", entries)
	}

	if deleted, err := reopened.Delete("another"); !deleted || err != nil {
		t.Fatalf("Delete = %v, %v", deleted, err)
	}

	if _, err := reopened.Get("another"); err == nil {
		t.Fatal("deleted entry should be gone")
	}
}

func TestStoreRejectsInvalidNames(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), "s.json"))
	if err != nil {
		t.Fatal(err)
	}

	for _, name := range []string{"", "../escape", "has space", "-leading"} {
		if _, err := store.Put(Entry{Name: name}); err == nil {
			t.Fatalf("name %q should be rejected", name)
		}
	}
}
