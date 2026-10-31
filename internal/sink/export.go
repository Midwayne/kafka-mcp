package sink

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/Midwayne/kafka-mcp/internal/record"
)

// Export streams matches to a JSON Lines file. Values are redacted but not truncated.
type Export struct {
	path   string
	max    int64
	redact []string

	mu      sync.Mutex
	file    *os.File
	writer  *bufio.Writer
	encoder *json.Encoder
	count   int64
	err     error
}

// ExportInfo summarizes a finished export.
type ExportInfo struct {
	Path    string `json:"path"`
	Records int64  `json:"records"`
	Full    bool   `json:"full"`
	Error   string `json:"error,omitempty"`
}

// NewExport creates path (and its directory) with owner-only permissions.
func NewExport(path string, maxRecords int64, redact []string) (*Export, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, fmt.Errorf("create export directory: %w", err)
	}

	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return nil, fmt.Errorf("create export file: %w", err)
	}

	writer := bufio.NewWriterSize(file, 1<<20)

	return &Export{path: path, max: maxRecords, redact: redact, file: file, writer: writer, encoder: json.NewEncoder(writer)}, nil
}

// Write appends one message. It reports whether the message was written and
// whether the export has room for more.
func (e *Export) Write(message *record.Message) (written, more bool) {
	rendered := record.View{Redact: e.redact, MaxItems: -1}.Render(message)

	e.mu.Lock()
	defer e.mu.Unlock()

	if e.err != nil || e.count >= e.max {
		return false, false
	}

	if err := e.encoder.Encode(rendered); err != nil {
		e.err = err
		return false, false
	}

	e.count++

	return true, e.count < e.max
}

// Close flushes and closes the file.
func (e *Export) Close() ExportInfo {
	e.mu.Lock()
	defer e.mu.Unlock()

	if err := e.writer.Flush(); err != nil && e.err == nil {
		e.err = err
	}

	if err := e.file.Close(); err != nil && e.err == nil {
		e.err = err
	}

	info := ExportInfo{Path: e.path, Records: e.count, Full: e.count >= e.max}
	if e.err != nil {
		info.Error = e.err.Error()
	}

	return info
}
