package auth

import (
	"context"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cognitoidentityprovider"
	"github.com/aws/aws-sdk-go-v2/service/cognitoidentityprovider/types"
)

// CognitoClient defines the subset of Cognito operations used by the auth service.
type CognitoClient interface {
	SignUp(ctx context.Context, params *cognitoidentityprovider.SignUpInput, optFns ...func(*cognitoidentityprovider.Options)) (*cognitoidentityprovider.SignUpOutput, error)
	InitiateAuth(ctx context.Context, params *cognitoidentityprovider.InitiateAuthInput, optFns ...func(*cognitoidentityprovider.Options)) (*cognitoidentityprovider.InitiateAuthOutput, error)
	DeleteUser(ctx context.Context, params *cognitoidentityprovider.DeleteUserInput, optFns ...func(*cognitoidentityprovider.Options)) (*cognitoidentityprovider.DeleteUserOutput, error)
}

// Service handles Cognito authentication operations.
type Service struct {
	client     CognitoClient
	userPoolID string
	clientID   string
}

// NewService creates a new auth service from an AWS config.
func NewService(cfg aws.Config, userPoolID, clientID string) *Service {
	return &Service{
		client:     cognitoidentityprovider.NewFromConfig(cfg),
		userPoolID: userPoolID,
		clientID:   clientID,
	}
}

// NewServiceWithClient creates a new auth service with an injected client (for testing).
func NewServiceWithClient(client CognitoClient, userPoolID, clientID string) *Service {
	return &Service{
		client:     client,
		userPoolID: userPoolID,
		clientID:   clientID,
	}
}

// Tokens holds the auth tokens returned by Cognito.
type Tokens struct {
	AccessToken  string
	IDToken      string
	RefreshToken string
	ExpiresIn    int32
}

// Register creates a new user in Cognito.
func (s *Service) Register(ctx context.Context, email, password string) error {
	_, err := s.client.SignUp(ctx, &cognitoidentityprovider.SignUpInput{
		ClientId: aws.String(s.clientID),
		Username: aws.String(email),
		Password: aws.String(password),
		UserAttributes: []types.AttributeType{
			{Name: aws.String("email"), Value: aws.String(email)},
		},
	})
	if err != nil {
		return fmt.Errorf("cognito sign up: %w", err)
	}
	return nil
}

// Login authenticates a user and returns tokens.
func (s *Service) Login(ctx context.Context, email, password string) (*Tokens, error) {
	result, err := s.client.InitiateAuth(ctx, &cognitoidentityprovider.InitiateAuthInput{
		AuthFlow: types.AuthFlowTypeUserPasswordAuth,
		ClientId: aws.String(s.clientID),
		AuthParameters: map[string]string{
			"USERNAME": email,
			"PASSWORD": password,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("cognito login: %w", err)
	}

	if result.AuthenticationResult == nil {
		return nil, fmt.Errorf("cognito login: no authentication result (challenge required?)")
	}

	return &Tokens{
		AccessToken:  *result.AuthenticationResult.AccessToken,
		IDToken:      *result.AuthenticationResult.IdToken,
		RefreshToken: *result.AuthenticationResult.RefreshToken,
		ExpiresIn:    result.AuthenticationResult.ExpiresIn,
	}, nil
}

// RefreshTokens uses a refresh token to get new access/id tokens.
func (s *Service) RefreshTokens(ctx context.Context, refreshToken string) (*Tokens, error) {
	result, err := s.client.InitiateAuth(ctx, &cognitoidentityprovider.InitiateAuthInput{
		AuthFlow: types.AuthFlowTypeRefreshTokenAuth,
		ClientId: aws.String(s.clientID),
		AuthParameters: map[string]string{
			"REFRESH_TOKEN": refreshToken,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("cognito refresh: %w", err)
	}

	if result.AuthenticationResult == nil {
		return nil, fmt.Errorf("cognito refresh: no authentication result")
	}

	return &Tokens{
		AccessToken: *result.AuthenticationResult.AccessToken,
		IDToken:     *result.AuthenticationResult.IdToken,
		ExpiresIn:   result.AuthenticationResult.ExpiresIn,
		// Refresh token is not returned on refresh — client keeps the original.
	}, nil
}

// DeleteUser deletes the authenticated user's account.
func (s *Service) DeleteUser(ctx context.Context, accessToken string) error {
	_, err := s.client.DeleteUser(ctx, &cognitoidentityprovider.DeleteUserInput{
		AccessToken: aws.String(accessToken),
	})
	if err != nil {
		return fmt.Errorf("cognito delete user: %w", err)
	}
	return nil
}

// ExtractUserID extracts the user ID (sub) from the API Gateway request context.
// API Gateway JWT authorizer puts claims in the request context.
func ExtractUserID(authorizerClaims map[string]string) (string, error) {
	sub, ok := authorizerClaims["sub"]
	if !ok || sub == "" {
		return "", fmt.Errorf("missing sub claim in JWT")
	}
	return sub, nil
}

// ExtractBearerToken extracts a bearer token from an Authorization header value.
func ExtractBearerToken(authHeader string) (string, error) {
	if authHeader == "" {
		return "", fmt.Errorf("missing Authorization header")
	}
	parts := strings.SplitN(authHeader, " ", 2)
	if len(parts) != 2 || strings.ToLower(parts[0]) != "bearer" {
		return "", fmt.Errorf("invalid Authorization header format")
	}
	return parts[1], nil
}
