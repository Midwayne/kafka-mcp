package app

import (
	"errors"
	"strings"

	"github.com/twmb/franz-go/pkg/kerr"
)

// remedy pairs a recognizable failure with advice for fixing it.
type remedy struct {
	matches func(err error, text string) bool
	advice  string
}

var remedies = []remedy{
	{
		matches: textContains("doesn't contain any IP SANs", "certificate is valid for", "certificate is not valid for"),
		advice:  "Brokers advertise addresses missing from their certificates. Set tls.verify_hostname: false on the cluster (the chain is still verified), as Java clients do with ssl.endpoint.identification.algorithm=''.",
	},
	{
		matches: textContains("certificate signed by unknown authority"),
		advice:  "The broker certificate is not trusted. Point tls.truststore or tls.ca_file at the CA that signed it, or set tls.include_system_cas: true.",
	},
	{
		matches: func(err error, _ string) bool {
			return errors.Is(err, kerr.SaslAuthenticationFailed) || errors.Is(err, kerr.IllegalSaslState)
		},
		advice: "SASL authentication failed. Check sasl.mechanism, username, and password (or token) for this cluster.",
	},
	{
		matches: func(err error, _ string) bool {
			return errors.Is(err, kerr.TopicAuthorizationFailed) || errors.Is(err, kerr.GroupAuthorizationFailed) ||
				errors.Is(err, kerr.ClusterAuthorizationFailed)
		},
		advice: "The credentials lack a Kafka ACL for this operation (e.g. Describe/Read on the topic or group). Ask the cluster owner to grant it.",
	},
	{
		matches: textContains("connection refused", "no such host", "i/o timeout"),
		advice:  "The broker is unreachable. Check the brokers list, network or VPN access, and the port (TLS listeners are often 9093).",
	},
	{
		matches: textContains("first record does not look like a TLS handshake", "tls: first record"),
		advice:  "TLS settings do not match the listener. Use security_protocol SSL/SASL_SSL for TLS ports and PLAINTEXT/SASL_PLAINTEXT otherwise.",
	},
}

// Explain appends advice for common connection, TLS, and authorization failures.
func Explain(err error) string {
	text := err.Error()

	for _, r := range remedies {
		if r.matches(err, text) {
			return text + "\nHint: " + r.advice
		}
	}

	return text
}

func textContains(fragments ...string) func(error, string) bool {
	return func(_ error, text string) bool {
		for _, fragment := range fragments {
			if strings.Contains(text, fragment) {
				return true
			}
		}

		return false
	}
}
