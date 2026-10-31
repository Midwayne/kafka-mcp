package auth

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"encoding/pem"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/Midwayne/kafka-mcp/internal/config"
)

// Store formats.
const (
	storeJKS    = "jks"
	storePKCS12 = "pkcs12"
	storePEM    = "pem"
)

const (
	jksMagic     = 0xFEEDFEED
	jceksMagic   = 0xCECECECE
	asn1Sequence = 0x30
)

// LoadTrustStore returns the CA certificates in a JKS, PKCS#12, or PEM truststore.
func LoadTrustStore(path, password, declaredType string) ([]*x509.Certificate, error) {
	data, kind, err := readStore(path, declaredType)
	if err != nil {
		return nil, fmt.Errorf("truststore: %w", err)
	}

	var certs []*x509.Certificate

	switch kind {
	case storeJKS:
		certs, err = jksTrustedCerts(data, password)
	case storePKCS12:
		certs, err = pkcs12TrustedCerts(data, password)
	default:
		certs = parsePEMCertificates(data)
	}

	if err != nil {
		return nil, fmt.Errorf("truststore %s: %w", path, err)
	}

	if len(certs) == 0 {
		return nil, fmt.Errorf("truststore %s contains no certificates", path)
	}

	return certs, nil
}

// LoadKeyStore returns the client certificate chain and key from a JKS, PKCS#12, or PEM keystore.
func LoadKeyStore(path, storePassword, keyPassword, alias, declaredType string) (tls.Certificate, error) {
	data, kind, err := readStore(path, declaredType)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("keystore: %w", err)
	}

	var cert tls.Certificate

	switch kind {
	case storeJKS:
		cert, err = jksKeyPair(data, storePassword, keyPassword, alias)
	case storePKCS12:
		cert, err = pkcs12KeyPair(data, storePassword)
	default:
		cert, err = tls.X509KeyPair(data, data)
	}

	if err != nil {
		return tls.Certificate{}, fmt.Errorf("keystore %s: %w", path, err)
	}

	return cert, nil
}

func readStore(path, declaredType string) ([]byte, string, error) {
	data, err := os.ReadFile(config.ExpandHome(path))
	if err != nil {
		return nil, "", fmt.Errorf("read %s: %w", path, err)
	}

	kind, err := detectStoreType(path, data, declaredType)
	if err != nil {
		return nil, "", err
	}

	return data, kind, nil
}

// detectStoreType sniffs the content first, as Java does: keytool has written
// PKCS#12 by default since Java 9, even into files named ".jks". The declared
// type and file extension are only used when the content is inconclusive.
func detectStoreType(path string, data []byte, declared string) (string, error) {
	if sniffed, err := sniffStoreType(data); sniffed != "" || err != nil {
		return sniffed, err
	}

	switch strings.ToLower(strings.TrimSpace(declared)) {
	case "jks":
		return storeJKS, nil
	case "pkcs12", "p12", "pfx":
		return storePKCS12, nil
	case "pem":
		return storePEM, nil
	case "":
	default:
		return "", fmt.Errorf("unsupported store type %q (jks, pkcs12, pem)", declared)
	}

	switch strings.ToLower(filepath.Ext(path)) {
	case ".jks", ".keystore", ".truststore":
		return storeJKS, nil
	case ".pem", ".crt", ".cer":
		return storePEM, nil
	default:
		return storePKCS12, nil
	}
}

func sniffStoreType(data []byte) (string, error) {
	if len(data) >= 4 {
		switch binary.BigEndian.Uint32(data) {
		case jksMagic:
			return storeJKS, nil
		case jceksMagic:
			return "", errors.New("JCEKS stores are not supported; convert with keytool -importkeystore -deststoretype pkcs12")
		}
	}

	if bytes.Contains(data, []byte("-----BEGIN")) {
		return storePEM, nil
	}

	// PKCS#12 is a DER-encoded ASN.1 SEQUENCE.
	if len(data) > 0 && data[0] == asn1Sequence {
		return storePKCS12, nil
	}

	return "", nil
}

func parsePEMCertificates(data []byte) []*x509.Certificate {
	var certs []*x509.Certificate

	for {
		var block *pem.Block

		block, data = pem.Decode(data)
		if block == nil {
			return certs
		}

		if block.Type != "CERTIFICATE" {
			continue
		}

		if cert, err := x509.ParseCertificate(block.Bytes); err == nil {
			certs = append(certs, cert)
		}
	}
}
