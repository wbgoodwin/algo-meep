package auth

import (
	"context"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cognitoidentityprovider"
	"github.com/aws/aws-sdk-go-v2/service/cognitoidentityprovider/types"
)

// --- Mock Cognito client ---

type mockCognitoClient struct {
	signUpFn       func(ctx context.Context, params *cognitoidentityprovider.SignUpInput, optFns ...func(*cognitoidentityprovider.Options)) (*cognitoidentityprovider.SignUpOutput, error)
	initiateAuthFn func(ctx context.Context, params *cognitoidentityprovider.InitiateAuthInput, optFns ...func(*cognitoidentityprovider.Options)) (*cognitoidentityprovider.InitiateAuthOutput, error)
	deleteUserFn   func(ctx context.Context, params *cognitoidentityprovider.DeleteUserInput, optFns ...func(*cognitoidentityprovider.Options)) (*cognitoidentityprovider.DeleteUserOutput, error)
	getUserFn      func(ctx context.Context, params *cognitoidentityprovider.GetUserInput, optFns ...func(*cognitoidentityprovider.Options)) (*cognitoidentityprovider.GetUserOutput, error)
}

func (m *mockCognitoClient) SignUp(ctx context.Context, params *cognitoidentityprovider.SignUpInput, optFns ...func(*cognitoidentityprovider.Options)) (*cognitoidentityprovider.SignUpOutput, error) {
	return m.signUpFn(ctx, params, optFns...)
}

func (m *mockCognitoClient) InitiateAuth(ctx context.Context, params *cognitoidentityprovider.InitiateAuthInput, optFns ...func(*cognitoidentityprovider.Options)) (*cognitoidentityprovider.InitiateAuthOutput, error) {
	return m.initiateAuthFn(ctx, params, optFns...)
}

func (m *mockCognitoClient) DeleteUser(ctx context.Context, params *cognitoidentityprovider.DeleteUserInput, optFns ...func(*cognitoidentityprovider.Options)) (*cognitoidentityprovider.DeleteUserOutput, error) {
	return m.deleteUserFn(ctx, params, optFns...)
}

func (m *mockCognitoClient) GetUser(ctx context.Context, params *cognitoidentityprovider.GetUserInput, optFns ...func(*cognitoidentityprovider.Options)) (*cognitoidentityprovider.GetUserOutput, error) {
	if m.getUserFn != nil {
		return m.getUserFn(ctx, params, optFns...)
	}
	return &cognitoidentityprovider.GetUserOutput{Username: aws.String("user-sub-123")}, nil
}

func newTestService(mock *mockCognitoClient) *Service {
	return NewServiceWithClient(mock, "us-east-1_test", "test-client-id")
}

// --- ExtractUserID tests ---

func TestExtractUserID_Valid(t *testing.T) {
	claims := map[string]string{"sub": "abc-123-def", "email": "test@example.com"}
	userID, err := ExtractUserID(claims)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if userID != "abc-123-def" {
		t.Errorf("expected 'abc-123-def', got '%s'", userID)
	}
}

func TestExtractUserID_MissingSub(t *testing.T) {
	claims := map[string]string{"email": "test@example.com"}
	_, err := ExtractUserID(claims)
	if err == nil {
		t.Error("expected error for missing sub claim")
	}
}

func TestExtractUserID_EmptySub(t *testing.T) {
	claims := map[string]string{"sub": ""}
	_, err := ExtractUserID(claims)
	if err == nil {
		t.Error("expected error for empty sub claim")
	}
}

// --- ExtractBearerToken tests ---

func TestExtractBearerToken_Valid(t *testing.T) {
	token, err := ExtractBearerToken("Bearer abc123xyz")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if token != "abc123xyz" {
		t.Errorf("expected 'abc123xyz', got '%s'", token)
	}
}

func TestExtractBearerToken_CaseInsensitive(t *testing.T) {
	token, err := ExtractBearerToken("bearer abc123xyz")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if token != "abc123xyz" {
		t.Errorf("expected 'abc123xyz', got '%s'", token)
	}
}

func TestExtractBearerToken_Empty(t *testing.T) {
	_, err := ExtractBearerToken("")
	if err == nil {
		t.Error("expected error for empty header")
	}
}

func TestExtractBearerToken_NoBearerPrefix(t *testing.T) {
	_, err := ExtractBearerToken("Basic abc123xyz")
	if err == nil {
		t.Error("expected error for non-Bearer scheme")
	}
}

func TestExtractBearerToken_NoSpace(t *testing.T) {
	_, err := ExtractBearerToken("Bearerabc123xyz")
	if err == nil {
		t.Error("expected error for missing space")
	}
}

func TestExtractBearerToken_TokenWithSpaces(t *testing.T) {
	token, err := ExtractBearerToken("Bearer abc 123 xyz")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if token != "abc 123 xyz" {
		t.Errorf("expected 'abc 123 xyz', got '%s'", token)
	}
}

// --- NewServiceWithClient test ---

func TestNewServiceWithClient(t *testing.T) {
	mock := &mockCognitoClient{}
	svc := NewServiceWithClient(mock, "pool-id", "client-id")
	if svc == nil {
		t.Fatal("expected non-nil service")
	}
	if svc.userPoolID != "pool-id" {
		t.Errorf("expected pool-id, got %s", svc.userPoolID)
	}
	if svc.clientID != "client-id" {
		t.Errorf("expected client-id, got %s", svc.clientID)
	}
}

// --- Register tests ---

func TestRegister_Success(t *testing.T) {
	mock := &mockCognitoClient{
		signUpFn: func(ctx context.Context, params *cognitoidentityprovider.SignUpInput, optFns ...func(*cognitoidentityprovider.Options)) (*cognitoidentityprovider.SignUpOutput, error) {
			if *params.Username != "test@example.com" {
				t.Errorf("expected username test@example.com, got %s", *params.Username)
			}
			if *params.ClientId != "test-client-id" {
				t.Errorf("expected client-id test-client-id, got %s", *params.ClientId)
			}
			return &cognitoidentityprovider.SignUpOutput{}, nil
		},
	}
	svc := newTestService(mock)
	err := svc.Register(context.Background(), "test@example.com", "password123")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestRegister_CognitoError(t *testing.T) {
	mock := &mockCognitoClient{
		signUpFn: func(ctx context.Context, params *cognitoidentityprovider.SignUpInput, optFns ...func(*cognitoidentityprovider.Options)) (*cognitoidentityprovider.SignUpOutput, error) {
			return nil, fmt.Errorf("UsernameExistsException: user already exists")
		},
	}
	svc := newTestService(mock)
	err := svc.Register(context.Background(), "test@example.com", "password123")
	if err == nil {
		t.Fatal("expected error")
	}
	if !contains(err.Error(), "cognito sign up") {
		t.Errorf("expected wrapped error, got: %v", err)
	}
}

func TestRegister_EmailAttribute(t *testing.T) {
	mock := &mockCognitoClient{
		signUpFn: func(ctx context.Context, params *cognitoidentityprovider.SignUpInput, optFns ...func(*cognitoidentityprovider.Options)) (*cognitoidentityprovider.SignUpOutput, error) {
			if len(params.UserAttributes) != 1 {
				t.Errorf("expected 1 attribute, got %d", len(params.UserAttributes))
			}
			if *params.UserAttributes[0].Name != "email" {
				t.Errorf("expected email attribute, got %s", *params.UserAttributes[0].Name)
			}
			if *params.UserAttributes[0].Value != "user@test.com" {
				t.Errorf("expected user@test.com, got %s", *params.UserAttributes[0].Value)
			}
			return &cognitoidentityprovider.SignUpOutput{}, nil
		},
	}
	svc := newTestService(mock)
	_ = svc.Register(context.Background(), "user@test.com", "pass")
}

// --- Login tests ---

func TestLogin_Success(t *testing.T) {
	mock := &mockCognitoClient{
		initiateAuthFn: func(ctx context.Context, params *cognitoidentityprovider.InitiateAuthInput, optFns ...func(*cognitoidentityprovider.Options)) (*cognitoidentityprovider.InitiateAuthOutput, error) {
			if params.AuthFlow != types.AuthFlowTypeUserPasswordAuth {
				t.Errorf("expected USER_PASSWORD_AUTH, got %s", params.AuthFlow)
			}
			if params.AuthParameters["USERNAME"] != "test@example.com" {
				t.Errorf("expected USERNAME=test@example.com, got %s", params.AuthParameters["USERNAME"])
			}
			return &cognitoidentityprovider.InitiateAuthOutput{
				AuthenticationResult: &types.AuthenticationResultType{
					AccessToken:  aws.String("access-token-123"),
					IdToken:      aws.String("id-token-456"),
					RefreshToken: aws.String("refresh-token-789"),
					ExpiresIn:    3600,
				},
			}, nil
		},
	}
	svc := newTestService(mock)
	tokens, err := svc.Login(context.Background(), "test@example.com", "password123")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tokens.AccessToken != "access-token-123" {
		t.Errorf("expected access-token-123, got %s", tokens.AccessToken)
	}
	if tokens.IDToken != "id-token-456" {
		t.Errorf("expected id-token-456, got %s", tokens.IDToken)
	}
	if tokens.RefreshToken != "refresh-token-789" {
		t.Errorf("expected refresh-token-789, got %s", tokens.RefreshToken)
	}
	if tokens.ExpiresIn != 3600 {
		t.Errorf("expected 3600, got %d", tokens.ExpiresIn)
	}
}

func TestLogin_CognitoError(t *testing.T) {
	mock := &mockCognitoClient{
		initiateAuthFn: func(ctx context.Context, params *cognitoidentityprovider.InitiateAuthInput, optFns ...func(*cognitoidentityprovider.Options)) (*cognitoidentityprovider.InitiateAuthOutput, error) {
			return nil, fmt.Errorf("NotAuthorizedException: incorrect username or password")
		},
	}
	svc := newTestService(mock)
	_, err := svc.Login(context.Background(), "test@example.com", "wrong")
	if err == nil {
		t.Fatal("expected error")
	}
	if !contains(err.Error(), "cognito login") {
		t.Errorf("expected wrapped error, got: %v", err)
	}
}

func TestLogin_NilAuthResult(t *testing.T) {
	mock := &mockCognitoClient{
		initiateAuthFn: func(ctx context.Context, params *cognitoidentityprovider.InitiateAuthInput, optFns ...func(*cognitoidentityprovider.Options)) (*cognitoidentityprovider.InitiateAuthOutput, error) {
			return &cognitoidentityprovider.InitiateAuthOutput{
				AuthenticationResult: nil, // challenge required
			}, nil
		},
	}
	svc := newTestService(mock)
	_, err := svc.Login(context.Background(), "test@example.com", "password123")
	if err == nil {
		t.Fatal("expected error for nil auth result")
	}
	if !contains(err.Error(), "challenge required") {
		t.Errorf("expected challenge required message, got: %v", err)
	}
}

// --- RefreshTokens tests ---

func TestRefreshTokens_Success(t *testing.T) {
	mock := &mockCognitoClient{
		initiateAuthFn: func(ctx context.Context, params *cognitoidentityprovider.InitiateAuthInput, optFns ...func(*cognitoidentityprovider.Options)) (*cognitoidentityprovider.InitiateAuthOutput, error) {
			if params.AuthFlow != types.AuthFlowTypeRefreshTokenAuth {
				t.Errorf("expected REFRESH_TOKEN_AUTH, got %s", params.AuthFlow)
			}
			if params.AuthParameters["REFRESH_TOKEN"] != "refresh-tok" {
				t.Errorf("expected REFRESH_TOKEN=refresh-tok, got %s", params.AuthParameters["REFRESH_TOKEN"])
			}
			return &cognitoidentityprovider.InitiateAuthOutput{
				AuthenticationResult: &types.AuthenticationResultType{
					AccessToken: aws.String("new-access"),
					IdToken:     aws.String("new-id"),
					ExpiresIn:   1800,
				},
			}, nil
		},
	}
	svc := newTestService(mock)
	tokens, err := svc.RefreshTokens(context.Background(), "refresh-tok")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tokens.AccessToken != "new-access" {
		t.Errorf("expected new-access, got %s", tokens.AccessToken)
	}
	if tokens.RefreshToken != "" {
		t.Errorf("expected empty refresh token, got %s", tokens.RefreshToken)
	}
}

func TestRefreshTokens_Error(t *testing.T) {
	mock := &mockCognitoClient{
		initiateAuthFn: func(ctx context.Context, params *cognitoidentityprovider.InitiateAuthInput, optFns ...func(*cognitoidentityprovider.Options)) (*cognitoidentityprovider.InitiateAuthOutput, error) {
			return nil, fmt.Errorf("token expired")
		},
	}
	svc := newTestService(mock)
	_, err := svc.RefreshTokens(context.Background(), "expired-tok")
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestRefreshTokens_NilAuthResult(t *testing.T) {
	mock := &mockCognitoClient{
		initiateAuthFn: func(ctx context.Context, params *cognitoidentityprovider.InitiateAuthInput, optFns ...func(*cognitoidentityprovider.Options)) (*cognitoidentityprovider.InitiateAuthOutput, error) {
			return &cognitoidentityprovider.InitiateAuthOutput{AuthenticationResult: nil}, nil
		},
	}
	svc := newTestService(mock)
	_, err := svc.RefreshTokens(context.Background(), "refresh-tok")
	if err == nil {
		t.Fatal("expected error for nil auth result")
	}
}

// --- DeleteUser tests ---

func TestDeleteUser_Success(t *testing.T) {
	mock := &mockCognitoClient{
		deleteUserFn: func(ctx context.Context, params *cognitoidentityprovider.DeleteUserInput, optFns ...func(*cognitoidentityprovider.Options)) (*cognitoidentityprovider.DeleteUserOutput, error) {
			if *params.AccessToken != "access-token-xyz" {
				t.Errorf("expected access-token-xyz, got %s", *params.AccessToken)
			}
			return &cognitoidentityprovider.DeleteUserOutput{}, nil
		},
	}
	svc := newTestService(mock)
	err := svc.DeleteUser(context.Background(), "access-token-xyz")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestDeleteUser_Error(t *testing.T) {
	mock := &mockCognitoClient{
		deleteUserFn: func(ctx context.Context, params *cognitoidentityprovider.DeleteUserInput, optFns ...func(*cognitoidentityprovider.Options)) (*cognitoidentityprovider.DeleteUserOutput, error) {
			return nil, fmt.Errorf("user not found")
		},
	}
	svc := newTestService(mock)
	err := svc.DeleteUser(context.Background(), "bad-token")
	if err == nil {
		t.Fatal("expected error")
	}
	if !contains(err.Error(), "cognito delete user") {
		t.Errorf("expected wrapped error, got: %v", err)
	}
}

// --- GetSubFromAccessToken tests ---

func TestGetSubFromAccessToken_Success(t *testing.T) {
	mock := &mockCognitoClient{
		getUserFn: func(ctx context.Context, params *cognitoidentityprovider.GetUserInput, optFns ...func(*cognitoidentityprovider.Options)) (*cognitoidentityprovider.GetUserOutput, error) {
			if *params.AccessToken != "valid-token" {
				t.Errorf("expected access token 'valid-token', got '%s'", *params.AccessToken)
			}
			return &cognitoidentityprovider.GetUserOutput{Username: aws.String("user-sub-abc")}, nil
		},
	}
	svc := newTestService(mock)
	sub, err := svc.GetSubFromAccessToken(context.Background(), "valid-token")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sub != "user-sub-abc" {
		t.Errorf("expected 'user-sub-abc', got '%s'", sub)
	}
}

func TestGetSubFromAccessToken_CognitoError(t *testing.T) {
	mock := &mockCognitoClient{
		getUserFn: func(ctx context.Context, params *cognitoidentityprovider.GetUserInput, optFns ...func(*cognitoidentityprovider.Options)) (*cognitoidentityprovider.GetUserOutput, error) {
			return nil, fmt.Errorf("token expired")
		},
	}
	svc := newTestService(mock)
	_, err := svc.GetSubFromAccessToken(context.Background(), "expired-token")
	if err == nil {
		t.Fatal("expected error")
	}
	if !contains(err.Error(), "cognito get user") {
		t.Errorf("expected wrapped error, got: %v", err)
	}
}

func TestGetSubFromAccessToken_EmptyUsername(t *testing.T) {
	mock := &mockCognitoClient{
		getUserFn: func(ctx context.Context, params *cognitoidentityprovider.GetUserInput, optFns ...func(*cognitoidentityprovider.Options)) (*cognitoidentityprovider.GetUserOutput, error) {
			return &cognitoidentityprovider.GetUserOutput{Username: aws.String("")}, nil
		},
	}
	svc := newTestService(mock)
	_, err := svc.GetSubFromAccessToken(context.Background(), "token")
	if err == nil {
		t.Fatal("expected error for empty username")
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsStr(s, substr))
}

func containsStr(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
