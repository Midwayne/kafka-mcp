// Package saved persists named scan definitions so agents can re-run them.
package saved

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"
)

var validName = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$`)

// Entry is one saved definition. Spec holds the tool arguments as JSON.
type Entry struct {
	Name        string          `json:"name"`
	Description string          `json:"description,omitempty"`
	Kind        string          `json:"kind"`
	Spec        json.RawMessage `json:"spec"`
	CreatedAt   time.Time       `json:"created_at"`
	UpdatedAt   time.Time       `json:"updated_at"`
}

// Store is a JSON file of entries, rewritten atomically on every change.
type Store struct {
	path string

	mu      sync.Mutex
	entries map[string]Entry
}

// Open loads the store, creating an empty one if the file does not exist.
func Open(path string) (*Store, error) {
	store := &Store{path: path, entries: map[string]Entry{}}

	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return store, nil
	}

	if err != nil {
		return nil, fmt.Errorf("read saved scans: %w", err)
	}

	var entries []Entry
	if err := json.Unmarshal(data, &entries); err != nil {
		return nil, fmt.Errorf("parse saved scans %s: %w", path, err)
	}

	for _, entry := range entries {
		store.entries[entry.Name] = entry
	}

	return store, nil
}

// Put creates or replaces an entry, preserving its creation time.
func (s *Store) Put(entry Entry) (Entry, error) {
	if !validName.MatchString(entry.Name) {
		return Entry{}, fmt.Errorf("invalid name %q (letters, digits, _ . -; up to 64 characters)", entry.Name)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now().UTC()
	entry.CreatedAt, entry.UpdatedAt = now, now

	if existing, ok := s.entries[entry.Name]; ok {
		entry.CreatedAt = existing.CreatedAt
	}

	s.entries[entry.Name] = entry

	return entry, s.persist()
}

// Get returns an entry by name.
func (s *Store) Get(name string) (Entry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry, ok := s.entries[strings.TrimSpace(name)]
	if !ok {
		return Entry{}, fmt.Errorf("no saved scan named %q (see list_saved_scans)", name)
	}

	return entry, nil
}

// List returns entries sorted by name.
func (s *Store) List() []Entry {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.sorted()
}

// Delete removes an entry and reports whether it existed.
func (s *Store) Delete(name string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.entries[name]; !ok {
		return false, nil
	}

	delete(s.entries, name)

	return true, s.persist()
}

func (s *Store) sorted() []Entry {
	entries := make([]Entry, 0, len(s.entries))
	for _, entry := range s.entries {
		entries = append(entries, entry)
	}

	slices.SortFunc(entries, func(a, b Entry) int { return strings.Compare(a.Name, b.Name) })

	return entries
}

// persist writes to a temp file and renames it so a crash never leaves a partial file.
func (s *Store) persist() error {
	data, err := json.MarshalIndent(s.sorted(), "", "  ")
	if err != nil {
		return fmt.Errorf("encode saved scans: %w", err)
	}

	if err := os.MkdirAll(filepath.Dir(s.path), 0o700); err != nil {
		return fmt.Errorf("create state directory: %w", err)
	}

	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return fmt.Errorf("write saved scans: %w", err)
	}

	if err := os.Rename(tmp, s.path); err != nil {
		return fmt.Errorf("replace saved scans: %w", err)
	}

	return nil
}
