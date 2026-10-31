package config

import (
	"errors"
	"fmt"
	"strings"
)

// Format names understood by the decoder.
const (
	FormatAuto           = "auto"
	FormatJSON           = "json"
	FormatString         = "string"
	FormatBase64         = "base64"
	FormatHex            = "hex"
	FormatSchemaRegistry = "schema_registry"
	FormatNone           = "none"
)

// IsEnabled reports whether the cluster should be registered.
func (c Cluster) IsEnabled() bool { return c.Enabled == nil || *c.Enabled }

// TLSEnabled reports whether connections to this cluster use TLS.
func (c Cluster) TLSEnabled() bool { return c.TLS.IsEnabled() }

// IsEnabled reports whether a TLS block is active.
func (t *TLS) IsEnabled() bool { return t != nil && (t.Enabled == nil || *t.Enabled) }

// ChecksHostname reports whether the broker hostname must match its certificate.
// Disabling it mirrors Java's ssl.endpoint.identification.algorithm="" while
// still verifying the certificate chain.
func (t *TLS) ChecksHostname() bool { return t.VerifyHostname == nil || *t.VerifyHostname }

func (c *Cluster) normalize(defaults Defaults) error {
	c.Name = strings.TrimSpace(c.Name)
	if c.Name == "" {
		return errors.New("cluster name is required")
	}

	if err := c.normalizeBasics(defaults); err != nil {
		return fmt.Errorf("cluster %q: %w", c.Name, err)
	}

	if err := c.normalizeSecurity(); err != nil {
		return fmt.Errorf("cluster %q: %w", c.Name, err)
	}

	if err := c.normalizeFormats(); err != nil {
		return fmt.Errorf("cluster %q: %w", c.Name, err)
	}

	if err := validateGlobs(c.Topics); err != nil {
		return fmt.Errorf("cluster %q topics: %w", c.Name, err)
	}

	return c.SchemaRegistry.normalize(c.Name)
}

func (c *Cluster) normalizeBasics(defaults Defaults) error {
	brokers := make([]string, 0, len(c.Brokers))
	for _, entry := range c.Brokers {
		for _, broker := range strings.Split(entry, ",") {
			if broker = strings.TrimSpace(broker); broker != "" {
				brokers = append(brokers, broker)
			}
		}
	}

	if len(brokers) == 0 {
		return errors.New("brokers is required")
	}
	c.Brokers = brokers

	access, err := ParseAccess(orString(c.Access, defaults.Access))
	if err != nil {
		return err
	}
	c.Access = access

	isolation, err := parseIsolation(orString(c.Isolation, defaults.Isolation))
	if err != nil {
		return err
	}
	c.Isolation = isolation

	c.ClientID = orString(c.ClientID, defaults.ClientID)
	c.Fetch = c.Fetch.withDefaults(defaults.Fetch)
	c.DialTimeout = orDuration(c.DialTimeout, defaults.DialTimeout.Std())
	c.RequestTimeout = orDuration(c.RequestTimeout, defaults.RequestTimeout.Std())

	return nil
}

func (c *Cluster) normalizeSecurity() error {
	protocol := strings.ToUpper(strings.TrimSpace(c.SecurityProtocol))
	c.SecurityProtocol = protocol

	switch protocol {
	case "":
		// Inferred from the tls and sasl blocks.
	case "PLAINTEXT", "SASL_PLAINTEXT":
		if c.TLS.IsEnabled() {
			return fmt.Errorf("security_protocol %s conflicts with an enabled tls block", protocol)
		}
	case "SSL", "SASL_SSL":
		if c.TLS == nil {
			c.TLS = &TLS{}
		}
		if !c.TLS.IsEnabled() {
			return fmt.Errorf("security_protocol %s requires TLS but tls.enabled is false", protocol)
		}
	default:
		return fmt.Errorf("invalid security_protocol %q", protocol)
	}

	usesSASL := strings.HasPrefix(protocol, "SASL_")
	if usesSASL && c.SASL == nil {
		return fmt.Errorf("security_protocol %s requires a sasl block", protocol)
	}

	if protocol != "" && !usesSASL && c.SASL != nil {
		return fmt.Errorf("security_protocol %s does not use SASL; remove the sasl block", protocol)
	}

	if err := c.TLS.validate(); err != nil {
		return fmt.Errorf("tls: %w", err)
	}

	return c.SASL.normalize()
}

func (c *Cluster) normalizeFormats() error {
	var err error

	if c.Formats.Key, err = NormalizeFormat(orString(c.Formats.Key, FormatAuto)); err != nil {
		return fmt.Errorf("formats.key: %w", err)
	}

	if c.Formats.Value, err = NormalizeFormat(orString(c.Formats.Value, FormatAuto)); err != nil {
		return fmt.Errorf("formats.value: %w", err)
	}

	for i := range c.Formats.Topics {
		if err := c.Formats.Topics[i].normalize(); err != nil {
			return fmt.Errorf("formats.topics[%d]: %w", i, err)
		}
	}

	return nil
}

func (t *TopicFormat) normalize() error {
	if err := validateGlobs(NameFilter{Include: []string{t.Pattern}}); err != nil || t.Pattern == "" {
		return fmt.Errorf("pattern %q is not a valid glob", t.Pattern)
	}

	var err error
	if t.Key != "" {
		if t.Key, err = NormalizeFormat(t.Key); err != nil {
			return err
		}
	}

	if t.Value != "" {
		t.Value, err = NormalizeFormat(t.Value)
	}

	return err
}

// NormalizeFormat validates a decoder name. Avro, protobuf, and json_schema are
// Schema Registry aliases because the wire format carries the schema type.
func NormalizeFormat(raw string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", FormatAuto:
		return FormatAuto, nil
	case FormatJSON:
		return FormatJSON, nil
	case FormatString, "text", "utf8":
		return FormatString, nil
	case FormatBase64, "bytes", "binary":
		return FormatBase64, nil
	case FormatHex:
		return FormatHex, nil
	case FormatSchemaRegistry, "sr", "avro", "protobuf", "proto", "json_schema", "jsonschema":
		return FormatSchemaRegistry, nil
	case FormatNone, "skip":
		return FormatNone, nil
	default:
		return "", fmt.Errorf("unknown format %q (auto, json, string, base64, hex, schema_registry, none)", raw)
	}
}
