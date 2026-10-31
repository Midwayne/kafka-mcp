package auth

import (
	"fmt"

	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"

	"github.com/Midwayne/kafka-mcp/internal/config"
)

type mechanismBuilder func(*config.SASL) (sasl.Mechanism, error)

var mechanismBuilders = map[string]mechanismBuilder{
	config.MechPlain:       buildPlain,
	config.MechScram256:    buildScram256,
	config.MechScram512:    buildScram512,
	config.MechOAuthBearer: buildOAuth,
	config.MechAWSMSKIAM:   buildAWS,
	config.MechGSSAPI:      buildKerberos,
}

// Mechanism builds the SASL mechanism for a normalized config block.
// It returns nil when SASL is not configured. Credentials are fetched lazily
// at authentication time, so expired tokens refresh without a restart.
func Mechanism(s *config.SASL) (sasl.Mechanism, error) {
	if s == nil {
		return nil, nil
	}

	build, ok := mechanismBuilders[s.Mechanism]
	if !ok {
		return nil, fmt.Errorf("unsupported SASL mechanism %q", s.Mechanism)
	}

	mechanism, err := build(s)
	if err != nil {
		return nil, fmt.Errorf("sasl %s: %w", s.Mechanism, err)
	}

	return mechanism, nil
}

func buildPlain(s *config.SASL) (sasl.Mechanism, error) {
	return plain.Auth{User: s.Username, Pass: s.Password}.AsMechanism(), nil
}

func buildScram256(s *config.SASL) (sasl.Mechanism, error) {
	return scram.Auth{User: s.Username, Pass: s.Password}.AsSha256Mechanism(), nil
}

func buildScram512(s *config.SASL) (sasl.Mechanism, error) {
	return scram.Auth{User: s.Username, Pass: s.Password}.AsSha512Mechanism(), nil
}
