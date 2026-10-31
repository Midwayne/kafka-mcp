package app

import (
	"context"
	"errors"
	"fmt"
	"path"

	"github.com/twmb/franz-go/pkg/sr"

	"github.com/Midwayne/kafka-mcp/internal/schemaregistry"
)

// SubjectsSpec lists Schema Registry subjects.
type SubjectsSpec struct {
	Cluster string `json:"cluster" jsonschema:"Cluster whose schema_registry to query."`
	Pattern string `json:"pattern,omitempty" jsonschema:"Glob filter, e.g. orders-*."`
}

// GetSchemaSpec selects a schema by subject/version or by ID.
type GetSchemaSpec struct {
	Cluster string `json:"cluster" jsonschema:"Cluster whose schema_registry to query."`
	Subject string `json:"subject,omitempty" jsonschema:"Subject name, e.g. orders-value."`
	Version int    `json:"version,omitempty" jsonschema:"Subject version (default: latest)."`
	ID      int    `json:"id,omitempty" jsonschema:"Schema ID (as seen in decoded messages' schema_id). Overrides subject."`
}

// SchemaInfo is a registered schema.
type SchemaInfo struct {
	Subject    string               `json:"subject,omitempty"`
	Version    int                  `json:"version,omitempty"`
	ID         int                  `json:"id,omitempty"`
	Type       string               `json:"type"`
	Schema     string               `json:"schema"`
	References []sr.SchemaReference `json:"references,omitempty"`
	Versions   []int                `json:"versions,omitempty"`
}

func (s *Service) schemaRegistry(cluster, tool string) (*schemaregistry.Client, error) {
	h, err := s.cluster(cluster, tool)
	if err != nil {
		return nil, err
	}

	if h.registry == nil {
		return nil, fmt.Errorf("cluster %q has no schema_registry configured", cluster)
	}

	return h.registry, nil
}

// ListSchemaSubjects lists subjects matching an optional glob.
func (s *Service) ListSchemaSubjects(ctx context.Context, spec SubjectsSpec) ([]string, error) {
	registry, err := s.schemaRegistry(spec.Cluster, ToolListSchemaSubjects)
	if err != nil {
		return nil, err
	}

	subjects, err := registry.Subjects(ctx)
	if err != nil || spec.Pattern == "" {
		return subjects, err
	}

	matched := []string{}
	for _, subject := range subjects {
		if ok, _ := path.Match(spec.Pattern, subject); ok {
			matched = append(matched, subject)
		}
	}

	return matched, nil
}

// GetSchema returns a schema by ID or by subject and version.
func (s *Service) GetSchema(ctx context.Context, spec GetSchemaSpec) (SchemaInfo, error) {
	registry, err := s.schemaRegistry(spec.Cluster, ToolGetSchema)
	if err != nil {
		return SchemaInfo{}, err
	}

	if spec.ID > 0 {
		schema, err := registry.SchemaByID(ctx, spec.ID)
		return SchemaInfo{ID: spec.ID, Type: schema.Type.String(), Schema: schema.Schema, References: schema.References}, err
	}

	if spec.Subject == "" {
		return SchemaInfo{}, errors.New("pass subject or id")
	}

	version := spec.Version
	if version <= 0 {
		version = schemaregistry.LatestVersion
	}

	schema, err := registry.SchemaByVersion(ctx, spec.Subject, version)
	if err != nil {
		return SchemaInfo{}, err
	}

	versions, _ := registry.SubjectVersions(ctx, spec.Subject)

	return SchemaInfo{
		Subject: schema.Subject, Version: schema.Version, ID: schema.ID, Type: schema.Type.String(),
		Schema: schema.Schema.Schema, References: schema.References, Versions: versions,
	}, nil
}
