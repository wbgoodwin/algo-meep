package main

import (
	"context"
	"errors"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/ssm"
)

type ApiKeys struct {
	ApiKey    string
	ApiSecret string
}

var cachedApiKeys *ApiKeys

func GetApiKeys() (*ApiKeys, error) {
	if cachedApiKeys != nil {
		return cachedApiKeys, nil
	}

	// Check for environment variables first (for local development)
	if apiKey := os.Getenv("ALPACA_API_KEY"); apiKey != "" {
		if apiSecret := os.Getenv("ALPACA_API_SECRET"); apiSecret != "" {
			keys := &ApiKeys{
				ApiKey:    apiKey,
				ApiSecret: apiSecret,
			}
			cachedApiKeys = keys
			return keys, nil
		}
	}

	region := "us-east-1"
	var cfg aws.Config
	var err error

	// Check if running in LocalStack (using environment variable)
	if endpoint := os.Getenv("LOCALSTACK_ENDPOINT"); endpoint != "" {
		cfg, err = config.LoadDefaultConfig(context.TODO(),
			config.WithRegion(region),
		)
		if err != nil {
			return nil, err
		}
		// Create SSM client with custom endpoint
		svc := ssm.NewFromConfig(cfg, func(o *ssm.Options) {
			o.BaseEndpoint = &endpoint
			o.Region = region
		})

		return getApiKeysFromSSM(svc)
	}

	// Production AWS configuration
	cfg, err = config.LoadDefaultConfig(context.TODO(), config.WithRegion(region))
	if err != nil {
		return nil, err
	}
	svc := ssm.NewFromConfig(cfg)

	return getApiKeysFromSSM(svc)
}

func getApiKeysFromSSM(svc *ssm.Client) (*ApiKeys, error) {
	input := &ssm.GetParametersInput{
		Names:          []string{"/dev/algo-meep/alpaca/api-key", "/dev/algo-meep/alpaca/api-secret"},
		WithDecryption: aws.Bool(true),
	}
	result, err := svc.GetParameters(context.TODO(), input)
	if err != nil {
		return nil, err
	}

	keys := &ApiKeys{}
	for _, param := range result.Parameters {
		switch *param.Name {
		case "/dev/algo-meep/alpaca/api-key":
			keys.ApiKey = *param.Value
		case "/dev/algo-meep/alpaca/api-secret":
			keys.ApiSecret = *param.Value
		}
	}

	if keys.ApiKey == "" || keys.ApiSecret == "" {
		return nil, errors.New("missing API key or secret in SSM parameters")
	}

	cachedApiKeys = keys
	return keys, nil
}
