// Package testutil provides fixtures shared by tests: a throwaway PKI and fake Kafka clusters.
package testutil

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// Identity is a certificate with its private key.
type Identity struct {
	Cert *x509.Certificate
	Key  *ecdsa.PrivateKey
}

// NewCA creates a self-signed certificate authority.
func NewCA(tb testing.TB) Identity {
	tb.Helper()

	template := baseTemplate("kafka-mcp test CA")
	template.IsCA = true
	template.BasicConstraintsValid = true
	template.KeyUsage = x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature

	return sign(tb, template, nil)
}

// Issue creates a leaf certificate for localhost signed by the CA.
func (id Identity) Issue(tb testing.TB, commonName string) Identity {
	tb.Helper()

	template := baseTemplate(commonName)
	template.KeyUsage = x509.KeyUsageDigitalSignature
	template.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}
	template.DNSNames = []string{"localhost"}
	template.IPAddresses = []net.IP{net.ParseIP("127.0.0.1")}

	return sign(tb, template, &id)
}

// TLSCertificate returns the identity as a tls.Certificate.
func (id Identity) TLSCertificate() tls.Certificate {
	return tls.Certificate{Certificate: [][]byte{id.Cert.Raw}, PrivateKey: id.Key, Leaf: id.Cert}
}

// Pool returns a cert pool trusting only this identity.
func (id Identity) Pool() *x509.CertPool {
	pool := x509.NewCertPool()
	pool.AddCert(id.Cert)

	return pool
}

// CertPEM encodes the certificate as PEM.
func (id Identity) CertPEM() []byte {
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: id.Cert.Raw})
}

// KeyPEM encodes the private key as PKCS#8 PEM.
func (id Identity) KeyPEM(tb testing.TB) []byte {
	tb.Helper()

	der, err := x509.MarshalPKCS8PrivateKey(id.Key)
	if err != nil {
		tb.Fatal(err)
	}

	return pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der})
}

// WriteFile writes data into dir/name and returns the path.
func WriteFile(tb testing.TB, dir, name string, data []byte) string {
	tb.Helper()

	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		tb.Fatal(err)
	}

	return path
}

func baseTemplate(commonName string) *x509.Certificate {
	serial, _ := rand.Int(rand.Reader, big.NewInt(1<<62))

	return &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: commonName},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
	}
}

func sign(tb testing.TB, template *x509.Certificate, issuer *Identity) Identity {
	tb.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		tb.Fatal(err)
	}

	parent, signer := template, key
	if issuer != nil {
		parent, signer = issuer.Cert, issuer.Key
	}

	der, err := x509.CreateCertificate(rand.Reader, template, parent, &key.PublicKey, signer)
	if err != nil {
		tb.Fatal(err)
	}

	cert, err := x509.ParseCertificate(der)
	if err != nil {
		tb.Fatal(err)
	}

	return Identity{Cert: cert, Key: key}
}
