package config

import "path"

// FormatsFor returns the configured key and value formats for a topic.
// The first matching topic override wins; unset fields fall back to cluster defaults.
func (c Cluster) FormatsFor(topic string) (key, value string) {
	key, value = c.Formats.Key, c.Formats.Value

	for _, override := range c.Formats.Topics {
		if ok, _ := path.Match(override.Pattern, topic); !ok {
			continue
		}

		if override.Key != "" {
			key = override.Key
		}

		if override.Value != "" {
			value = override.Value
		}

		break
	}

	return key, value
}

// TopicAllowed reports whether the cluster's topic filter exposes the topic.
func (c Cluster) TopicAllowed(topic string) bool { return c.Topics.Allows(topic) }

// CanWrite reports whether write tools may target the cluster.
func (c Cluster) CanWrite() bool { return c.Access == AccessReadWrite }

// AuthSummary describes the auth setup without exposing secrets.
func (c Cluster) AuthSummary() map[string]any {
	summary := map[string]any{"tls": c.TLSEnabled()}

	if c.TLS.IsEnabled() {
		summary["tls_client_cert"] = c.TLS.CertFile != "" || c.TLS.CertPEM != "" || c.TLS.Keystore != ""
		summary["tls_insecure_skip_verify"] = c.TLS.InsecureSkipVerify
	}

	if c.SASL != nil {
		summary["sasl"] = c.SASL.Mechanism
	}

	if c.SchemaRegistry != nil {
		summary["schema_registry"] = true
	}

	return summary
}
