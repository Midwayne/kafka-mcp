package auth

import (
	"context"
	"fmt"

	awssdk "github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/twmb/franz-go/pkg/sasl"
	kaws "github.com/twmb/franz-go/pkg/sasl/aws"

	"github.com/Midwayne/kafka-mcp/internal/config"
)

const userAgent = "kafka-mcp"

func buildAWS(s *config.SASL) (sasl.Mechanism, error) {
	var settings config.AWS
	if s.AWS != nil {
		settings = *s.AWS
	}

	provider := newLazy(func(ctx context.Context) (awssdk.CredentialsProvider, error) {
		return awsProvider(ctx, settings)
	})

	return kaws.ManagedStreamingIAM(func(ctx context.Context) (kaws.Auth, error) {
		resolved, err := provider.get(ctx)
		if err != nil {
			return kaws.Auth{}, err
		}

		creds, err := resolved.Retrieve(ctx)
		if err != nil {
			return kaws.Auth{}, fmt.Errorf("retrieve AWS credentials: %w", err)
		}

		return kaws.Auth{
			AccessKey:    creds.AccessKeyID,
			SecretKey:    creds.SecretAccessKey,
			SessionToken: creds.SessionToken,
			UserAgent:    userAgent,
		}, nil
	}), nil
}

// awsProvider uses static keys when configured, otherwise the default AWS chain
// (environment, shared profiles, SSO, web identity, and instance roles).
func awsProvider(ctx context.Context, settings config.AWS) (awssdk.CredentialsProvider, error) {
	if settings.AccessKeyID != "" {
		static := credentials.NewStaticCredentialsProvider(
			settings.AccessKeyID, settings.SecretAccessKey, settings.SessionToken,
		)

		return awssdk.NewCredentialsCache(static), nil
	}

	var opts []func(*awsconfig.LoadOptions) error
	if settings.Region != "" {
		opts = append(opts, awsconfig.WithRegion(settings.Region))
	}
	if settings.Profile != "" {
		opts = append(opts, awsconfig.WithSharedConfigProfile(settings.Profile))
	}

	cfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("load AWS config: %w", err)
	}

	return cfg.Credentials, nil
}
