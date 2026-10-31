package auth

import (
	"bytes"
	"crypto"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"

	keystore "github.com/pavlo-v-chernykh/keystore-go/v4"
	"software.sslmate.com/src/go-pkcs12"
)

func loadJKS(data []byte, password string) (keystore.KeyStore, error) {
	ks := keystore.New(keystore.WithCaseExactAliases(), keystore.WithOrderedAliases())

	err := ks.Load(bytes.NewReader(data), []byte(password))
	if err == nil {
		return ks, nil
	}

	// Like Java, a truststore opened without a password skips the integrity check.
	if password == "" && err.Error() == "got invalid digest" {
		return ks, nil
	}

	return ks, fmt.Errorf("open JKS: %w", err)
}

func jksTrustedCerts(data []byte, password string) ([]*x509.Certificate, error) {
	ks, err := loadJKS(data, password)
	if err != nil {
		return nil, err
	}

	var certs []*x509.Certificate

	for _, alias := range ks.Aliases() {
		if !ks.IsTrustedCertificateEntry(alias) {
			continue
		}

		entry, err := ks.GetTrustedCertificateEntry(alias)
		if err != nil {
			return nil, fmt.Errorf("alias %q: %w", alias, err)
		}

		cert, err := x509.ParseCertificate(entry.Certificate.Content)
		if err != nil {
			return nil, fmt.Errorf("alias %q: %w", alias, err)
		}

		certs = append(certs, cert)
	}

	return certs, nil
}

func jksKeyPair(data []byte, storePassword, keyPassword, alias string) (tls.Certificate, error) {
	ks, err := loadJKS(data, storePassword)
	if err != nil {
		return tls.Certificate{}, err
	}

	alias, err = pickKeyAlias(ks, alias)
	if err != nil {
		return tls.Certificate{}, err
	}

	entry, err := ks.GetPrivateKeyEntry(alias, []byte(keyPassword))
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("alias %q (check key_password): %w", alias, err)
	}

	key, err := parsePrivateKey(entry.PrivateKey)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("alias %q: %w", alias, err)
	}

	chain := make([][]byte, 0, len(entry.CertificateChain))
	for _, cert := range entry.CertificateChain {
		chain = append(chain, cert.Content)
	}

	return certificate(chain, key)
}

func pickKeyAlias(ks keystore.KeyStore, alias string) (string, error) {
	if alias != "" {
		if !ks.IsPrivateKeyEntry(alias) {
			return "", fmt.Errorf("alias %q is not a private key entry", alias)
		}

		return alias, nil
	}

	for _, candidate := range ks.Aliases() {
		if ks.IsPrivateKeyEntry(candidate) {
			return candidate, nil
		}
	}

	return "", errors.New("no private key entry found")
}

func pkcs12TrustedCerts(data []byte, password string) ([]*x509.Certificate, error) {
	certs, err := pkcs12.DecodeTrustStore(data, password)
	if err == nil {
		return certs, nil
	}

	// Fall back to stores that hold a key entry plus its CA chain.
	_, leaf, cas, chainErr := pkcs12.DecodeChain(data, password)
	if chainErr != nil {
		return nil, fmt.Errorf("open PKCS#12: %w", err)
	}

	return append(cas, leaf), nil
}

func pkcs12KeyPair(data []byte, password string) (tls.Certificate, error) {
	key, leaf, cas, err := pkcs12.DecodeChain(data, password)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("open PKCS#12: %w", err)
	}

	chain := [][]byte{leaf.Raw}
	for _, ca := range cas {
		chain = append(chain, ca.Raw)
	}

	signer, ok := key.(crypto.Signer)
	if !ok {
		return tls.Certificate{}, fmt.Errorf("unsupported private key type %T", key)
	}

	return certificate(chain, signer)
}

func parsePrivateKey(der []byte) (crypto.Signer, error) {
	if key, err := x509.ParsePKCS8PrivateKey(der); err == nil {
		if signer, ok := key.(crypto.Signer); ok {
			return signer, nil
		}
	}

	if key, err := x509.ParsePKCS1PrivateKey(der); err == nil {
		return key, nil
	}

	if key, err := x509.ParseECPrivateKey(der); err == nil {
		return key, nil
	}

	return nil, errors.New("unsupported private key encoding")
}

func certificate(chain [][]byte, key crypto.Signer) (tls.Certificate, error) {
	if len(chain) == 0 {
		return tls.Certificate{}, errors.New("key entry has no certificate chain")
	}

	leaf, err := x509.ParseCertificate(chain[0])
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("parse leaf certificate: %w", err)
	}

	return tls.Certificate{Certificate: chain, PrivateKey: key, Leaf: leaf}, nil
}
