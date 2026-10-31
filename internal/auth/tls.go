// Package auth turns config blocks into TLS configs and SASL mechanisms.
package auth

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"

	"github.com/Midwayne/kafka-mcp/internal/config"
)

var tlsVersions = map[string]uint16{
	"1.0": tls.VersionTLS10, "1.1": tls.VersionTLS11,
	"1.2": tls.VersionTLS12, "1.3": tls.VersionTLS13,
}

// TLSConfig builds a client TLS config, or returns nil when TLS is disabled.
func TLSConfig(t *config.TLS) (*tls.Config, error) {
	if !t.IsEnabled() {
		return nil, nil
	}

	cfg := &tls.Config{
		MinVersion:         tls.VersionTLS12,
		ServerName:         t.ServerName,
		InsecureSkipVerify: t.InsecureSkipVerify, //nolint:gosec // explicit opt-in for test clusters
	}

	if version, ok := tlsVersions[t.MinVersion]; ok {
		cfg.MinVersion = version
	}

	roots, err := rootPool(t)
	if err != nil {
		return nil, err
	}
	cfg.RootCAs = roots

	cert, err := clientCertificate(t)
	if err != nil {
		return nil, err
	}

	if cert != nil {
		cfg.Certificates = []tls.Certificate{*cert}
	}

	if !t.ChecksHostname() && !t.InsecureSkipVerify {
		skipHostnameOnly(cfg)
	}

	return cfg, nil
}

// skipHostnameOnly replaces Go's verification with an equivalent chain check
// that omits the hostname match. Expired or untrusted certificates still fail.
func skipHostnameOnly(cfg *tls.Config) {
	roots := cfg.RootCAs

	// Chain verification is re-implemented in VerifyConnection below.
	cfg.InsecureSkipVerify = true
	cfg.VerifyConnection = func(state tls.ConnectionState) error {
		if len(state.PeerCertificates) == 0 {
			return errors.New("tls: server presented no certificate")
		}

		intermediates := x509.NewCertPool()
		for _, cert := range state.PeerCertificates[1:] {
			intermediates.AddCert(cert)
		}

		_, err := state.PeerCertificates[0].Verify(x509.VerifyOptions{
			Roots:         roots,
			Intermediates: intermediates,
			KeyUsages:     []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		})
		if err != nil {
			return fmt.Errorf("tls: verify server certificate chain: %w", err)
		}

		return nil
	}
}

func rootPool(t *config.TLS) (*x509.CertPool, error) {
	var certs []*x509.Certificate

	if t.CAFile != "" {
		data, err := os.ReadFile(config.ExpandHome(t.CAFile))
		if err != nil {
			return nil, fmt.Errorf("read ca_file: %w", err)
		}
		certs = append(certs, parsePEMCertificates(data)...)
	}

	if t.CAPEM != "" {
		certs = append(certs, parsePEMCertificates([]byte(t.CAPEM))...)
	}

	if t.Truststore != "" {
		trusted, err := LoadTrustStore(t.Truststore, t.TruststorePassword, t.TruststoreType)
		if err != nil {
			return nil, err
		}
		certs = append(certs, trusted...)
	}

	if len(certs) == 0 {
		if t.CAFile != "" || t.CAPEM != "" {
			return nil, errors.New("no PEM certificates found in the configured CA")
		}

		return nil, nil
	}

	return buildPool(certs, t.IncludeSystemCAs)
}

func buildPool(certs []*x509.Certificate, includeSystem bool) (*x509.CertPool, error) {
	pool := x509.NewCertPool()

	if includeSystem {
		system, err := x509.SystemCertPool()
		if err != nil {
			return nil, fmt.Errorf("load system CAs: %w", err)
		}
		pool = system
	}

	for _, cert := range certs {
		pool.AddCert(cert)
	}

	return pool, nil
}

func clientCertificate(t *config.TLS) (*tls.Certificate, error) {
	switch {
	case t.CertFile != "":
		cert, err := tls.LoadX509KeyPair(config.ExpandHome(t.CertFile), config.ExpandHome(t.KeyFile))
		if err != nil {
			return nil, fmt.Errorf("load cert_file/key_file: %w", err)
		}
		return &cert, nil

	case t.CertPEM != "":
		cert, err := tls.X509KeyPair([]byte(t.CertPEM), []byte(t.KeyPEM))
		if err != nil {
			return nil, fmt.Errorf("parse cert_pem/key_pem: %w", err)
		}
		return &cert, nil

	case t.Keystore != "":
		keyPassword := t.KeyPassword
		if keyPassword == "" {
			keyPassword = t.KeystorePassword
		}

		cert, err := LoadKeyStore(t.Keystore, t.KeystorePassword, keyPassword, t.KeyAlias, t.KeystoreType)
		if err != nil {
			return nil, err
		}
		return &cert, nil

	default:
		return nil, nil
	}
}
