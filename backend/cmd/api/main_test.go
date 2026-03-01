package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/cognitoidentityprovider"
	cognitotypes "github.com/aws/aws-sdk-go-v2/service/cognitoidentityprovider/types"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/kms"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/wbgoodwin/algo-meep/backend/internal/auth"
	"github.com/wbgoodwin/algo-meep/backend/internal/crypto"
	"github.com/wbgoodwin/algo-meep/backend/internal/middleware"
	"github.com/wbgoodwin/algo-meep/backend/internal/provider"
	syncpkg "github.com/wbgoodwin/algo-meep/backend/internal/sync"
	"github.com/wbgoodwin/algo-meep/backend/internal/user"
	"github.com/wbgoodwin/algo-meep/backend/pkg/api"
)

// ============================================================
// Mock implementations
// ============================================================

// --- Mock Cognito ---

type mockCognito struct {
	signUpFn       func(ctx context.Context, params *cognitoidentityprovider.SignUpInput, optFns ...func(*cognitoidentityprovider.Options)) (*cognitoidentityprovider.SignUpOutput, error)
	initiateAuthFn func(ctx context.Context, params *cognitoidentityprovider.InitiateAuthInput, optFns ...func(*cognitoidentityprovider.Options)) (*cognitoidentityprovider.InitiateAuthOutput, error)
	deleteUserFn   func(ctx context.Context, params *cognitoidentityprovider.DeleteUserInput, optFns ...func(*cognitoidentityprovider.Options)) (*cognitoidentityprovider.DeleteUserOutput, error)
	getUserFn      func(ctx context.Context, params *cognitoidentityprovider.GetUserInput, optFns ...func(*cognitoidentityprovider.Options)) (*cognitoidentityprovider.GetUserOutput, error)
}

func (m *mockCognito) SignUp(ctx context.Context, params *cognitoidentityprovider.SignUpInput, optFns ...func(*cognitoidentityprovider.Options)) (*cognitoidentityprovider.SignUpOutput, error) {
	if m.signUpFn != nil {
		return m.signUpFn(ctx, params, optFns...)
	}
	return &cognitoidentityprovider.SignUpOutput{}, nil
}

func (m *mockCognito) InitiateAuth(ctx context.Context, params *cognitoidentityprovider.InitiateAuthInput, optFns ...func(*cognitoidentityprovider.Options)) (*cognitoidentityprovider.InitiateAuthOutput, error) {
	if m.initiateAuthFn != nil {
		return m.initiateAuthFn(ctx, params, optFns...)
	}
	return nil, fmt.Errorf("not implemented")
}

func (m *mockCognito) DeleteUser(ctx context.Context, params *cognitoidentityprovider.DeleteUserInput, optFns ...func(*cognitoidentityprovider.Options)) (*cognitoidentityprovider.DeleteUserOutput, error) {
	if m.deleteUserFn != nil {
		return m.deleteUserFn(ctx, params, optFns...)
	}
	return &cognitoidentityprovider.DeleteUserOutput{}, nil
}

func (m *mockCognito) GetUser(ctx context.Context, params *cognitoidentityprovider.GetUserInput, optFns ...func(*cognitoidentityprovider.Options)) (*cognitoidentityprovider.GetUserOutput, error) {
	if m.getUserFn != nil {
		return m.getUserFn(ctx, params, optFns...)
	}
	return &cognitoidentityprovider.GetUserOutput{Username: aws.String("user-sub-123")}, nil
}

// --- Mock DynamoDB ---

type mockDynamo struct {
	putItemFn    func(ctx context.Context, params *dynamodb.PutItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error)
	getItemFn    func(ctx context.Context, params *dynamodb.GetItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error)
	updateItemFn func(ctx context.Context, params *dynamodb.UpdateItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error)
	deleteItemFn func(ctx context.Context, params *dynamodb.DeleteItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error)
}

func (m *mockDynamo) PutItem(ctx context.Context, params *dynamodb.PutItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
	if m.putItemFn != nil {
		return m.putItemFn(ctx, params, optFns...)
	}
	return &dynamodb.PutItemOutput{}, nil
}

func (m *mockDynamo) GetItem(ctx context.Context, params *dynamodb.GetItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
	if m.getItemFn != nil {
		return m.getItemFn(ctx, params, optFns...)
	}
	return &dynamodb.GetItemOutput{}, nil
}

func (m *mockDynamo) UpdateItem(ctx context.Context, params *dynamodb.UpdateItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error) {
	if m.updateItemFn != nil {
		return m.updateItemFn(ctx, params, optFns...)
	}
	return &dynamodb.UpdateItemOutput{}, nil
}

func (m *mockDynamo) DeleteItem(ctx context.Context, params *dynamodb.DeleteItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error) {
	if m.deleteItemFn != nil {
		return m.deleteItemFn(ctx, params, optFns...)
	}
	return &dynamodb.DeleteItemOutput{}, nil
}

// --- Mock KMS ---

type mockKMS struct {
	encryptFn func(ctx context.Context, params *kms.EncryptInput, optFns ...func(*kms.Options)) (*kms.EncryptOutput, error)
	decryptFn func(ctx context.Context, params *kms.DecryptInput, optFns ...func(*kms.Options)) (*kms.DecryptOutput, error)
}

func (m *mockKMS) Encrypt(ctx context.Context, params *kms.EncryptInput, optFns ...func(*kms.Options)) (*kms.EncryptOutput, error) {
	if m.encryptFn != nil {
		return m.encryptFn(ctx, params, optFns...)
	}
	// Passthrough: return plaintext as "ciphertext" (tests use base64 encoding)
	return &kms.EncryptOutput{CiphertextBlob: params.Plaintext}, nil
}

func (m *mockKMS) Decrypt(ctx context.Context, params *kms.DecryptInput, optFns ...func(*kms.Options)) (*kms.DecryptOutput, error) {
	if m.decryptFn != nil {
		return m.decryptFn(ctx, params, optFns...)
	}
	// Passthrough: return ciphertext as plaintext
	return &kms.DecryptOutput{Plaintext: params.CiphertextBlob}, nil
}

// --- Mock Bank Provider ---

type mockBankProvider struct {
	name              string
	startEnrollmentFn func(userID, institutionID string) (*provider.EnrollmentSession, error)
	exchangeTokenFn   func(enrollmentToken string) (*provider.AccessCredential, error)
	getAccountsFn     func(credential provider.AccessCredential) ([]provider.Account, error)
	syncTxnsFn        func(credential provider.AccessCredential, cursor string) (*provider.TransactionSync, error)
	getInstitutionsFn func() ([]provider.Institution, error)
}

func (m *mockBankProvider) Name() string { return m.name }

func (m *mockBankProvider) StartEnrollment(userID, institutionID string) (*provider.EnrollmentSession, error) {
	if m.startEnrollmentFn != nil {
		return m.startEnrollmentFn(userID, institutionID)
	}
	return &provider.EnrollmentSession{
		SessionURL: "https://mock.test/connect",
		SessionID:  "session_123",
		Provider:   m.name,
	}, nil
}

func (m *mockBankProvider) ExchangeToken(enrollmentToken string) (*provider.AccessCredential, error) {
	if m.exchangeTokenFn != nil {
		return m.exchangeTokenFn(enrollmentToken)
	}
	return &provider.AccessCredential{
		ProviderName:  m.name,
		AccessToken:   "access_" + enrollmentToken,
		InstitutionID: "inst_123",
	}, nil
}

func (m *mockBankProvider) GetAccounts(credential provider.AccessCredential) ([]provider.Account, error) {
	if m.getAccountsFn != nil {
		return m.getAccountsFn(credential)
	}
	return []provider.Account{
		{ID: "acc_001", Name: "Checking", Type: "depository", CurrencyCode: "USD"},
	}, nil
}

func (m *mockBankProvider) SyncTransactions(credential provider.AccessCredential, cursor string) (*provider.TransactionSync, error) {
	if m.syncTxnsFn != nil {
		return m.syncTxnsFn(credential, cursor)
	}
	return &provider.TransactionSync{
		Added:  []provider.Transaction{{ID: "txn_001", Amount: -42.50}},
		Cursor: "cursor_1",
	}, nil
}

func (m *mockBankProvider) GetInstitutions() ([]provider.Institution, error) {
	if m.getInstitutionsFn != nil {
		return m.getInstitutionsFn()
	}
	return []provider.Institution{}, nil
}

// ============================================================
// Test helpers
// ============================================================

func newTestApp() *App {
	return newTestAppWithMocks(&mockCognito{}, &mockDynamo{})
}

func newTestAppWithMocks(cognito *mockCognito, dynamo *mockDynamo) *App {
	reg := provider.NewRegistry()
	reg.Register(&mockBankProvider{name: "teller"})

	return &App{
		Log:            middleware.NewLogger("TEST"),
		AuthService:    auth.NewServiceWithClient(cognito, "test-pool", "test-client"),
		UserService:    user.NewServiceWithClient(dynamo),
		ProviderReg:    reg,
		AllowedIPs:     map[string]bool{"1.2.3.4": true},
		TokenEncryptor: crypto.NewTokenEncryptor(&mockKMS{}, "test-key"),
	}
}

// testEncryptToken encrypts a token using the mock KMS passthrough for use in test requests.
func testEncryptToken(plaintext string) string {
	return base64.StdEncoding.EncodeToString([]byte(plaintext))
}

func newTestAppWithIP(allowedIPs map[string]bool) *App {
	a := newTestApp()
	a.AllowedIPs = allowedIPs
	return a
}

func makeAuthRequest(routeKey, body string) events.APIGatewayV2HTTPRequest {
	return events.APIGatewayV2HTTPRequest{
		RouteKey: routeKey,
		Body:     body,
		RequestContext: events.APIGatewayV2HTTPRequestContext{
			HTTP: events.APIGatewayV2HTTPRequestContextHTTPDescription{
				SourceIP: "1.2.3.4",
			},
			Authorizer: &events.APIGatewayV2HTTPRequestContextAuthorizerDescription{
				JWT: &events.APIGatewayV2HTTPRequestContextAuthorizerJWTDescription{
					Claims: map[string]string{"sub": "user-123"},
				},
			},
		},
		Headers: map[string]string{
			"authorization": "Bearer test-bearer-token",
		},
	}
}

func makePublicRequest(routeKey, body string) events.APIGatewayV2HTTPRequest {
	return events.APIGatewayV2HTTPRequest{
		RouteKey: routeKey,
		Body:     body,
		RequestContext: events.APIGatewayV2HTTPRequestContext{
			HTTP: events.APIGatewayV2HTTPRequestContextHTTPDescription{
				SourceIP: "1.2.3.4",
			},
		},
	}
}

func parseResponse(t *testing.T, resp events.APIGatewayV2HTTPResponse) api.APIResponse {
	t.Helper()
	var parsed api.APIResponse
	if err := json.Unmarshal([]byte(resp.Body), &parsed); err != nil {
		t.Fatalf("failed to parse response body: %v\nbody: %s", err, resp.Body)
	}
	return parsed
}

// makeJWT creates a fake JWT with a "sub" claim for testing extractSubFromJWT.
func makeJWT(sub string) string {
	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"RS256"}`))
	payload := base64.RawURLEncoding.EncodeToString([]byte(fmt.Sprintf(`{"sub":"%s"}`, sub)))
	sig := base64.RawURLEncoding.EncodeToString([]byte("signature"))
	return header + "." + payload + "." + sig
}

// ============================================================
// Unit tests for helper functions
// ============================================================

func TestIsAllowedIP_EmptyAllowlist(t *testing.T) {
	a := &App{AllowedIPs: map[string]bool{}}
	if a.isAllowedIP("1.2.3.4") {
		t.Error("empty allowlist should block all IPs (fail-closed)")
	}
}

func TestIsAllowedIP_Allowed(t *testing.T) {
	a := &App{AllowedIPs: map[string]bool{"1.2.3.4": true}}
	if !a.isAllowedIP("1.2.3.4") {
		t.Error("should allow listed IP")
	}
}

func TestIsAllowedIP_Blocked(t *testing.T) {
	a := &App{AllowedIPs: map[string]bool{"1.2.3.4": true}}
	if a.isAllowedIP("5.6.7.8") {
		t.Error("should block unlisted IP")
	}
}

func TestEmailDomain(t *testing.T) {
	tests := []struct {
		email    string
		expected string
	}{
		{"user@example.com", "example.com"},
		{"test@sub.domain.org", "sub.domain.org"},
		{"no-at-sign", "unknown"},
		{"", "unknown"},
		{"@domain.com", "domain.com"},
	}
	for _, tt := range tests {
		got := emailDomain(tt.email)
		if got != tt.expected {
			t.Errorf("emailDomain(%q) = %q, want %q", tt.email, got, tt.expected)
		}
	}
}

// ============================================================
// Component tests — full handler invocation
// ============================================================

// --- IP allowlist ---

func TestHandler_IPBlocked(t *testing.T) {
	a := newTestAppWithIP(map[string]bool{"10.0.0.1": true})
	req := makePublicRequest("GET /health", "")
	req.RequestContext.HTTP.SourceIP = "1.2.3.4"

	resp, err := a.Handler(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.StatusCode != http.StatusForbidden {
		t.Errorf("expected 403, got %d", resp.StatusCode)
	}
}

func TestHandler_IPAllowed(t *testing.T) {
	a := newTestAppWithIP(map[string]bool{"1.2.3.4": true})
	req := makePublicRequest("GET /health", "")
	req.RequestContext.HTTP.SourceIP = "1.2.3.4"

	resp, err := a.Handler(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200, got %d", resp.StatusCode)
	}
}

// --- Health ---

func TestHandler_Health(t *testing.T) {
	a := newTestApp()
	resp, err := a.Handler(context.Background(), makePublicRequest("GET /health", ""))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200, got %d", resp.StatusCode)
	}
	parsed := parseResponse(t, resp)
	if !parsed.Success {
		t.Error("expected success=true")
	}
}

// --- Unknown route ---

func TestHandler_UnknownRoute(t *testing.T) {
	a := newTestApp()
	resp, err := a.Handler(context.Background(), makePublicRequest("GET /nonexistent", ""))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("expected 404, got %d", resp.StatusCode)
	}
}

// --- Register ---

func TestHandler_Register_Success(t *testing.T) {
	cognito := &mockCognito{
		signUpFn: func(ctx context.Context, params *cognitoidentityprovider.SignUpInput, optFns ...func(*cognitoidentityprovider.Options)) (*cognitoidentityprovider.SignUpOutput, error) {
			return &cognitoidentityprovider.SignUpOutput{}, nil
		},
	}
	a := newTestAppWithMocks(cognito, &mockDynamo{})
	body := `{"email":"test@example.com","password":"Pass123!"}`
	resp, _ := a.Handler(context.Background(), makePublicRequest("POST /auth/register", body))
	if resp.StatusCode != http.StatusCreated {
		t.Errorf("expected 201, got %d: %s", resp.StatusCode, resp.Body)
	}
}

func TestHandler_Register_InvalidBody(t *testing.T) {
	a := newTestApp()
	resp, _ := a.Handler(context.Background(), makePublicRequest("POST /auth/register", "not json"))
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", resp.StatusCode)
	}
}

func TestHandler_Register_MissingFields(t *testing.T) {
	a := newTestApp()
	resp, _ := a.Handler(context.Background(), makePublicRequest("POST /auth/register", `{"email":"test@test.com"}`))
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", resp.StatusCode)
	}
}

func TestHandler_Register_UserExists(t *testing.T) {
	cognito := &mockCognito{
		signUpFn: func(ctx context.Context, params *cognitoidentityprovider.SignUpInput, optFns ...func(*cognitoidentityprovider.Options)) (*cognitoidentityprovider.SignUpOutput, error) {
			return nil, fmt.Errorf("UsernameExistsException: user exists")
		},
	}
	a := newTestAppWithMocks(cognito, &mockDynamo{})
	body := `{"email":"test@example.com","password":"Pass123!"}`
	resp, _ := a.Handler(context.Background(), makePublicRequest("POST /auth/register", body))
	if resp.StatusCode != http.StatusConflict {
		t.Errorf("expected 409, got %d: %s", resp.StatusCode, resp.Body)
	}
}

func TestHandler_Register_CognitoError(t *testing.T) {
	cognito := &mockCognito{
		signUpFn: func(ctx context.Context, params *cognitoidentityprovider.SignUpInput, optFns ...func(*cognitoidentityprovider.Options)) (*cognitoidentityprovider.SignUpOutput, error) {
			return nil, fmt.Errorf("InternalErrorException: something broke")
		},
	}
	a := newTestAppWithMocks(cognito, &mockDynamo{})
	body := `{"email":"test@example.com","password":"Pass123!"}`
	resp, _ := a.Handler(context.Background(), makePublicRequest("POST /auth/register", body))
	if resp.StatusCode != http.StatusInternalServerError {
		t.Errorf("expected 500, got %d", resp.StatusCode)
	}
}

// --- Login ---

func TestHandler_Login_Success(t *testing.T) {
	jwtToken := makeJWT("user-sub-123")
	cognito := &mockCognito{
		initiateAuthFn: func(ctx context.Context, params *cognitoidentityprovider.InitiateAuthInput, optFns ...func(*cognitoidentityprovider.Options)) (*cognitoidentityprovider.InitiateAuthOutput, error) {
			return &cognitoidentityprovider.InitiateAuthOutput{
				AuthenticationResult: &cognitotypes.AuthenticationResultType{
					AccessToken:  aws.String(jwtToken),
					IdToken:      aws.String("id-tok"),
					RefreshToken: aws.String("refresh-tok"),
					ExpiresIn:    3600,
				},
			}, nil
		},
	}
	dynamo := &mockDynamo{
		getItemFn: func(ctx context.Context, params *dynamodb.GetItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			return &dynamodb.GetItemOutput{Item: nil}, nil // user not found = first login
		},
		putItemFn: func(ctx context.Context, params *dynamodb.PutItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
			return &dynamodb.PutItemOutput{}, nil
		},
	}
	a := newTestAppWithMocks(cognito, dynamo)
	body := `{"email":"test@example.com","password":"Pass123!"}`
	resp, _ := a.Handler(context.Background(), makePublicRequest("POST /auth/login", body))
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200, got %d: %s", resp.StatusCode, resp.Body)
	}
	parsed := parseResponse(t, resp)
	if !parsed.Success {
		t.Error("expected success=true")
	}
}

func TestHandler_Login_InvalidBody(t *testing.T) {
	a := newTestApp()
	resp, _ := a.Handler(context.Background(), makePublicRequest("POST /auth/login", "bad"))
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", resp.StatusCode)
	}
}

func TestHandler_Login_MissingFields(t *testing.T) {
	a := newTestApp()
	resp, _ := a.Handler(context.Background(), makePublicRequest("POST /auth/login", `{"email":"a@b.com"}`))
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", resp.StatusCode)
	}
}

func TestHandler_Login_NotAuthorized(t *testing.T) {
	cognito := &mockCognito{
		initiateAuthFn: func(ctx context.Context, params *cognitoidentityprovider.InitiateAuthInput, optFns ...func(*cognitoidentityprovider.Options)) (*cognitoidentityprovider.InitiateAuthOutput, error) {
			return nil, fmt.Errorf("NotAuthorizedException: bad creds")
		},
	}
	a := newTestAppWithMocks(cognito, &mockDynamo{})
	body := `{"email":"test@example.com","password":"wrong"}`
	resp, _ := a.Handler(context.Background(), makePublicRequest("POST /auth/login", body))
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("expected 401, got %d", resp.StatusCode)
	}
}

func TestHandler_Login_InternalError(t *testing.T) {
	cognito := &mockCognito{
		initiateAuthFn: func(ctx context.Context, params *cognitoidentityprovider.InitiateAuthInput, optFns ...func(*cognitoidentityprovider.Options)) (*cognitoidentityprovider.InitiateAuthOutput, error) {
			return nil, fmt.Errorf("InternalErrorException: broke")
		},
	}
	a := newTestAppWithMocks(cognito, &mockDynamo{})
	body := `{"email":"test@example.com","password":"pass"}`
	resp, _ := a.Handler(context.Background(), makePublicRequest("POST /auth/login", body))
	if resp.StatusCode != http.StatusInternalServerError {
		t.Errorf("expected 500, got %d", resp.StatusCode)
	}
}

func TestHandler_Login_ExistingUser(t *testing.T) {
	jwtToken := makeJWT("user-sub-123")
	cognito := &mockCognito{
		initiateAuthFn: func(ctx context.Context, params *cognitoidentityprovider.InitiateAuthInput, optFns ...func(*cognitoidentityprovider.Options)) (*cognitoidentityprovider.InitiateAuthOutput, error) {
			return &cognitoidentityprovider.InitiateAuthOutput{
				AuthenticationResult: &cognitotypes.AuthenticationResultType{
					AccessToken:  aws.String(jwtToken),
					IdToken:      aws.String("id-tok"),
					RefreshToken: aws.String("refresh-tok"),
					ExpiresIn:    3600,
				},
			}, nil
		},
	}
	dynamo := &mockDynamo{
		getItemFn: func(ctx context.Context, params *dynamodb.GetItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			// User already exists
			return &dynamodb.GetItemOutput{
				Item: map[string]dbtypes.AttributeValue{
					"PK": &dbtypes.AttributeValueMemberS{Value: "USER#user-sub-123"},
				},
			}, nil
		},
	}
	a := newTestAppWithMocks(cognito, dynamo)
	body := `{"email":"test@example.com","password":"Pass123!"}`
	resp, _ := a.Handler(context.Background(), makePublicRequest("POST /auth/login", body))
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200, got %d", resp.StatusCode)
	}
}

// --- Refresh ---

func TestHandler_Refresh_Success(t *testing.T) {
	cognito := &mockCognito{
		initiateAuthFn: func(ctx context.Context, params *cognitoidentityprovider.InitiateAuthInput, optFns ...func(*cognitoidentityprovider.Options)) (*cognitoidentityprovider.InitiateAuthOutput, error) {
			return &cognitoidentityprovider.InitiateAuthOutput{
				AuthenticationResult: &cognitotypes.AuthenticationResultType{
					AccessToken: aws.String("new-access"),
					IdToken:     aws.String("new-id"),
					ExpiresIn:   1800,
				},
			}, nil
		},
	}
	a := newTestAppWithMocks(cognito, &mockDynamo{})
	body := `{"refresh_token":"refresh-tok"}`
	resp, _ := a.Handler(context.Background(), makePublicRequest("POST /auth/refresh", body))
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200, got %d: %s", resp.StatusCode, resp.Body)
	}
}

func TestHandler_Refresh_InvalidBody(t *testing.T) {
	a := newTestApp()
	resp, _ := a.Handler(context.Background(), makePublicRequest("POST /auth/refresh", "bad"))
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", resp.StatusCode)
	}
}

func TestHandler_Refresh_MissingToken(t *testing.T) {
	a := newTestApp()
	resp, _ := a.Handler(context.Background(), makePublicRequest("POST /auth/refresh", `{}`))
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", resp.StatusCode)
	}
}

func TestHandler_Refresh_InvalidToken(t *testing.T) {
	cognito := &mockCognito{
		initiateAuthFn: func(ctx context.Context, params *cognitoidentityprovider.InitiateAuthInput, optFns ...func(*cognitoidentityprovider.Options)) (*cognitoidentityprovider.InitiateAuthOutput, error) {
			return nil, fmt.Errorf("expired")
		},
	}
	a := newTestAppWithMocks(cognito, &mockDynamo{})
	resp, _ := a.Handler(context.Background(), makePublicRequest("POST /auth/refresh", `{"refresh_token":"bad"}`))
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("expected 401, got %d", resp.StatusCode)
	}
}

// --- Delete Account ---

func TestHandler_DeleteAccount_Success(t *testing.T) {
	a := newTestApp()
	resp, _ := a.Handler(context.Background(), makeAuthRequest("DELETE /auth/account", ""))
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200, got %d: %s", resp.StatusCode, resp.Body)
	}
}

func TestHandler_DeleteAccount_MissingAuth(t *testing.T) {
	a := newTestApp()
	req := makeAuthRequest("DELETE /auth/account", "")
	req.Headers = map[string]string{} // no authorization header
	resp, _ := a.Handler(context.Background(), req)
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("expected 401, got %d", resp.StatusCode)
	}
}

func TestHandler_DeleteAccount_CognitoError(t *testing.T) {
	cognito := &mockCognito{
		deleteUserFn: func(ctx context.Context, params *cognitoidentityprovider.DeleteUserInput, optFns ...func(*cognitoidentityprovider.Options)) (*cognitoidentityprovider.DeleteUserOutput, error) {
			return nil, fmt.Errorf("failed to delete")
		},
	}
	a := newTestAppWithMocks(cognito, &mockDynamo{})
	resp, _ := a.Handler(context.Background(), makeAuthRequest("DELETE /auth/account", ""))
	if resp.StatusCode != http.StatusInternalServerError {
		t.Errorf("expected 500, got %d", resp.StatusCode)
	}
}

func TestHandler_DeleteAccount_DynamoError(t *testing.T) {
	dynamo := &mockDynamo{
		deleteItemFn: func(ctx context.Context, params *dynamodb.DeleteItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error) {
			return nil, fmt.Errorf("dynamo error")
		},
	}
	a := newTestAppWithMocks(&mockCognito{}, dynamo)
	resp, _ := a.Handler(context.Background(), makeAuthRequest("DELETE /auth/account", ""))
	// Should still succeed — Cognito delete succeeded, DynamoDB error is non-fatal
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200 (non-fatal DynamoDB error), got %d", resp.StatusCode)
	}
}

// --- Auth middleware ---

func TestHandler_AuthRoute_MissingSub(t *testing.T) {
	a := newTestApp()
	req := makeAuthRequest("GET /user/profile", "")
	req.RequestContext.Authorizer.JWT.Claims = map[string]string{} // no sub
	resp, _ := a.Handler(context.Background(), req)
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("expected 401, got %d", resp.StatusCode)
	}
}

// --- Get Profile ---

func TestHandler_GetProfile_Found(t *testing.T) {
	dynamo := &mockDynamo{
		getItemFn: func(ctx context.Context, params *dynamodb.GetItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			return &dynamodb.GetItemOutput{
				Item: map[string]dbtypes.AttributeValue{
					"PK":                 &dbtypes.AttributeValueMemberS{Value: "USER#user-123"},
					"SK":                 &dbtypes.AttributeValueMemberS{Value: "PROFILE"},
					"email":              &dbtypes.AttributeValueMemberS{Value: "test@test.com"},
					"plan":               &dbtypes.AttributeValueMemberS{Value: "free"},
					"connected_accounts": &dbtypes.AttributeValueMemberN{Value: "2"},
					"created_at":         &dbtypes.AttributeValueMemberS{Value: "2024-01-01T00:00:00Z"},
				},
			}, nil
		},
	}
	a := newTestAppWithMocks(&mockCognito{}, dynamo)
	resp, _ := a.Handler(context.Background(), makeAuthRequest("GET /user/profile", ""))
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200, got %d: %s", resp.StatusCode, resp.Body)
	}
}

func TestHandler_GetProfile_NotFound(t *testing.T) {
	dynamo := &mockDynamo{
		getItemFn: func(ctx context.Context, params *dynamodb.GetItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			return &dynamodb.GetItemOutput{Item: nil}, nil
		},
	}
	a := newTestAppWithMocks(&mockCognito{}, dynamo)
	resp, _ := a.Handler(context.Background(), makeAuthRequest("GET /user/profile", ""))
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("expected 404, got %d", resp.StatusCode)
	}
}

func TestHandler_GetProfile_DynamoError(t *testing.T) {
	dynamo := &mockDynamo{
		getItemFn: func(ctx context.Context, params *dynamodb.GetItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			return nil, fmt.Errorf("network error")
		},
	}
	a := newTestAppWithMocks(&mockCognito{}, dynamo)
	resp, _ := a.Handler(context.Background(), makeAuthRequest("GET /user/profile", ""))
	if resp.StatusCode != http.StatusInternalServerError {
		t.Errorf("expected 500, got %d", resp.StatusCode)
	}
}

// --- Update Profile ---

func TestHandler_UpdateProfile_Success(t *testing.T) {
	a := newTestApp()
	resp, _ := a.Handler(context.Background(), makeAuthRequest("PUT /user/profile", `{"plan":"premium"}`))
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200, got %d: %s", resp.StatusCode, resp.Body)
	}
}

func TestHandler_UpdateProfile_InvalidPlan(t *testing.T) {
	a := newTestApp()
	resp, _ := a.Handler(context.Background(), makeAuthRequest("PUT /user/profile", `{"plan":"gold"}`))
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", resp.StatusCode)
	}
}

func TestHandler_UpdateProfile_InvalidBody(t *testing.T) {
	a := newTestApp()
	resp, _ := a.Handler(context.Background(), makeAuthRequest("PUT /user/profile", "bad"))
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", resp.StatusCode)
	}
}

func TestHandler_UpdateProfile_EmptyPlan(t *testing.T) {
	a := newTestApp()
	resp, _ := a.Handler(context.Background(), makeAuthRequest("PUT /user/profile", `{}`))
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200 for no-op update, got %d", resp.StatusCode)
	}
}

func TestHandler_UpdateProfile_DynamoError(t *testing.T) {
	dynamo := &mockDynamo{
		updateItemFn: func(ctx context.Context, params *dynamodb.UpdateItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error) {
			return nil, fmt.Errorf("throttled")
		},
	}
	a := newTestAppWithMocks(&mockCognito{}, dynamo)
	resp, _ := a.Handler(context.Background(), makeAuthRequest("PUT /user/profile", `{"plan":"standard"}`))
	if resp.StatusCode != http.StatusInternalServerError {
		t.Errorf("expected 500, got %d", resp.StatusCode)
	}
}

// --- Bank Enroll ---

func TestHandler_BankEnroll_Success(t *testing.T) {
	a := newTestApp()
	body := `{"institution_id":"chase","provider":"teller"}`
	resp, _ := a.Handler(context.Background(), makeAuthRequest("POST /bank/enroll", body))
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200, got %d: %s", resp.StatusCode, resp.Body)
	}
}

func TestHandler_BankEnroll_AutoSelect(t *testing.T) {
	a := newTestApp()
	body := `{"institution_id":"chase"}`
	resp, _ := a.Handler(context.Background(), makeAuthRequest("POST /bank/enroll", body))
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200, got %d: %s", resp.StatusCode, resp.Body)
	}
}

func TestHandler_BankEnroll_InvalidBody(t *testing.T) {
	a := newTestApp()
	resp, _ := a.Handler(context.Background(), makeAuthRequest("POST /bank/enroll", "bad"))
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", resp.StatusCode)
	}
}

func TestHandler_BankEnroll_UnknownProvider(t *testing.T) {
	a := newTestApp()
	body := `{"institution_id":"chase","provider":"unknown"}`
	resp, _ := a.Handler(context.Background(), makeAuthRequest("POST /bank/enroll", body))
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", resp.StatusCode)
	}
}

func TestHandler_BankEnroll_NoProviders(t *testing.T) {
	a := newTestApp()
	a.ProviderReg = provider.NewRegistry() // empty
	body := `{"institution_id":"chase"}`
	resp, _ := a.Handler(context.Background(), makeAuthRequest("POST /bank/enroll", body))
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", resp.StatusCode)
	}
}

func TestHandler_BankEnroll_ProviderError(t *testing.T) {
	reg := provider.NewRegistry()
	reg.Register(&mockBankProvider{
		name: "teller",
		startEnrollmentFn: func(userID, institutionID string) (*provider.EnrollmentSession, error) {
			return nil, fmt.Errorf("enrollment failed")
		},
	})
	a := newTestApp()
	a.ProviderReg = reg
	body := `{"institution_id":"chase","provider":"teller"}`
	resp, _ := a.Handler(context.Background(), makeAuthRequest("POST /bank/enroll", body))
	if resp.StatusCode != http.StatusBadGateway {
		t.Errorf("expected 502, got %d", resp.StatusCode)
	}
}

// --- Bank Exchange Token ---

func TestHandler_BankExchangeToken_Success(t *testing.T) {
	a := newTestApp()
	body := `{"enrollment_token":"tok_abc","provider":"teller"}`
	resp, _ := a.Handler(context.Background(), makeAuthRequest("POST /bank/exchange-token", body))
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200, got %d: %s", resp.StatusCode, resp.Body)
	}
}

func TestHandler_BankExchangeToken_InvalidBody(t *testing.T) {
	a := newTestApp()
	resp, _ := a.Handler(context.Background(), makeAuthRequest("POST /bank/exchange-token", "bad"))
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", resp.StatusCode)
	}
}

func TestHandler_BankExchangeToken_UnknownProvider(t *testing.T) {
	a := newTestApp()
	body := `{"enrollment_token":"tok","provider":"unknown"}`
	resp, _ := a.Handler(context.Background(), makeAuthRequest("POST /bank/exchange-token", body))
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", resp.StatusCode)
	}
}

func TestHandler_BankExchangeToken_ProviderError(t *testing.T) {
	reg := provider.NewRegistry()
	reg.Register(&mockBankProvider{
		name: "teller",
		exchangeTokenFn: func(enrollmentToken string) (*provider.AccessCredential, error) {
			return nil, fmt.Errorf("exchange failed")
		},
	})
	a := newTestApp()
	a.ProviderReg = reg
	body := `{"enrollment_token":"tok","provider":"teller"}`
	resp, _ := a.Handler(context.Background(), makeAuthRequest("POST /bank/exchange-token", body))
	if resp.StatusCode != http.StatusBadGateway {
		t.Errorf("expected 502, got %d", resp.StatusCode)
	}
}

// --- Bank Accounts ---

func TestHandler_BankAccounts_Success(t *testing.T) {
	a := newTestApp()
	body := fmt.Sprintf(`{"encrypted_access_token":"%s","provider":"teller"}`, testEncryptToken("tok"))
	resp, _ := a.Handler(context.Background(), makeAuthRequest("POST /bank/accounts", body))
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200, got %d: %s", resp.StatusCode, resp.Body)
	}
}

func TestHandler_BankAccounts_UnknownProvider(t *testing.T) {
	a := newTestApp()
	body := fmt.Sprintf(`{"encrypted_access_token":"%s","provider":"unknown"}`, testEncryptToken("tok"))
	resp, _ := a.Handler(context.Background(), makeAuthRequest("POST /bank/accounts", body))
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", resp.StatusCode)
	}
}

func TestHandler_BankAccounts_ProviderError(t *testing.T) {
	reg := provider.NewRegistry()
	reg.Register(&mockBankProvider{
		name: "teller",
		getAccountsFn: func(credential provider.AccessCredential) ([]provider.Account, error) {
			return nil, fmt.Errorf("API error")
		},
	})
	a := newTestApp()
	a.ProviderReg = reg
	body := fmt.Sprintf(`{"encrypted_access_token":"%s","provider":"teller"}`, testEncryptToken("tok"))
	resp, _ := a.Handler(context.Background(), makeAuthRequest("POST /bank/accounts", body))
	if resp.StatusCode != http.StatusBadGateway {
		t.Errorf("expected 502, got %d", resp.StatusCode)
	}
}

func TestHandler_BankAccounts_InvalidBody(t *testing.T) {
	a := newTestApp()
	resp, _ := a.Handler(context.Background(), makeAuthRequest("POST /bank/accounts", "bad"))
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("expected 400, got %d: %s", resp.StatusCode, resp.Body)
	}
}

// --- Bank Sync Transactions ---

func TestHandler_BankSyncTransactions_Success(t *testing.T) {
	a := newTestApp()
	body := fmt.Sprintf(`{"encrypted_access_token":"%s","provider":"teller"}`, testEncryptToken("tok"))
	resp, _ := a.Handler(context.Background(), makeAuthRequest("POST /bank/sync-transactions", body))
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200, got %d: %s", resp.StatusCode, resp.Body)
	}
}

func TestHandler_BankSyncTransactions_InvalidBody(t *testing.T) {
	a := newTestApp()
	resp, _ := a.Handler(context.Background(), makeAuthRequest("POST /bank/sync-transactions", "bad"))
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", resp.StatusCode)
	}
}

func TestHandler_BankSyncTransactions_UnknownProvider(t *testing.T) {
	a := newTestApp()
	body := fmt.Sprintf(`{"encrypted_access_token":"%s","provider":"unknown"}`, testEncryptToken("tok"))
	resp, _ := a.Handler(context.Background(), makeAuthRequest("POST /bank/sync-transactions", body))
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", resp.StatusCode)
	}
}

func TestHandler_BankSyncTransactions_ProviderError(t *testing.T) {
	reg := provider.NewRegistry()
	reg.Register(&mockBankProvider{
		name: "teller",
		syncTxnsFn: func(credential provider.AccessCredential, cursor string) (*provider.TransactionSync, error) {
			return nil, fmt.Errorf("sync error")
		},
	})
	a := newTestApp()
	a.ProviderReg = reg
	body := fmt.Sprintf(`{"encrypted_access_token":"%s","provider":"teller"}`, testEncryptToken("tok"))
	resp, _ := a.Handler(context.Background(), makeAuthRequest("POST /bank/sync-transactions", body))
	if resp.StatusCode != http.StatusBadGateway {
		t.Errorf("expected 502, got %d", resp.StatusCode)
	}
}

func TestHandler_BankSyncTransactions_WithUpdated(t *testing.T) {
	reg := provider.NewRegistry()
	reg.Register(&mockBankProvider{
		name: "teller",
		syncTxnsFn: func(credential provider.AccessCredential, cursor string) (*provider.TransactionSync, error) {
			return &provider.TransactionSync{
				Added:   []provider.Transaction{{ID: "txn_001", Amount: -10}},
				Updated: []provider.Transaction{{ID: "txn_002", Amount: -20}},
				Removed: []string{"txn_003"},
				Cursor:  "cursor_2",
			}, nil
		},
	})
	a := newTestApp()
	a.ProviderReg = reg
	body := fmt.Sprintf(`{"encrypted_access_token":"%s","provider":"teller","cursor":"cursor_1"}`, testEncryptToken("tok"))
	resp, _ := a.Handler(context.Background(), makeAuthRequest("POST /bank/sync-transactions", body))
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200, got %d: %s", resp.StatusCode, resp.Body)
	}
}

// --- Bank Providers ---

func TestHandler_BankProviders_WithProviders(t *testing.T) {
	a := newTestApp()
	resp, _ := a.Handler(context.Background(), makeAuthRequest("GET /bank/providers", ""))
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200, got %d", resp.StatusCode)
	}
	parsed := parseResponse(t, resp)
	if !parsed.Success {
		t.Error("expected success=true")
	}
}

func TestHandler_BankProviders_Empty(t *testing.T) {
	a := newTestApp()
	a.ProviderReg = provider.NewRegistry()
	resp, _ := a.Handler(context.Background(), makeAuthRequest("GET /bank/providers", ""))
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200, got %d", resp.StatusCode)
	}
}

// --- Login edge cases ---

func TestHandler_Login_CreateUserFails(t *testing.T) {
	jwtToken := makeJWT("user-sub-123")
	cognito := &mockCognito{
		initiateAuthFn: func(ctx context.Context, params *cognitoidentityprovider.InitiateAuthInput, optFns ...func(*cognitoidentityprovider.Options)) (*cognitoidentityprovider.InitiateAuthOutput, error) {
			return &cognitoidentityprovider.InitiateAuthOutput{
				AuthenticationResult: &cognitotypes.AuthenticationResultType{
					AccessToken:  aws.String(jwtToken),
					IdToken:      aws.String("id-tok"),
					RefreshToken: aws.String("refresh-tok"),
					ExpiresIn:    3600,
				},
			}, nil
		},
	}
	dynamo := &mockDynamo{
		getItemFn: func(ctx context.Context, params *dynamodb.GetItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			return &dynamodb.GetItemOutput{Item: nil}, nil // user not found
		},
		putItemFn: func(ctx context.Context, params *dynamodb.PutItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
			return nil, fmt.Errorf("DynamoDB write error") // create fails
		},
	}
	a := newTestAppWithMocks(cognito, dynamo)
	body := `{"email":"test@example.com","password":"Pass123!"}`
	resp, _ := a.Handler(context.Background(), makePublicRequest("POST /auth/login", body))
	// Login should still succeed — profile creation failure is non-fatal
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200 even when profile creation fails, got %d", resp.StatusCode)
	}
}

func TestHandler_Login_GetSubFails_StillReturnsTokens(t *testing.T) {
	// Cognito GetUser fails — login should still succeed, just skip profile creation
	jwtToken := makeJWT("user-sub-123")
	cognito := &mockCognito{
		initiateAuthFn: func(ctx context.Context, params *cognitoidentityprovider.InitiateAuthInput, optFns ...func(*cognitoidentityprovider.Options)) (*cognitoidentityprovider.InitiateAuthOutput, error) {
			return &cognitoidentityprovider.InitiateAuthOutput{
				AuthenticationResult: &cognitotypes.AuthenticationResultType{
					AccessToken:  aws.String(jwtToken),
					IdToken:      aws.String("id-tok"),
					RefreshToken: aws.String("refresh-tok"),
					ExpiresIn:    3600,
				},
			}, nil
		},
		getUserFn: func(ctx context.Context, params *cognitoidentityprovider.GetUserInput, optFns ...func(*cognitoidentityprovider.Options)) (*cognitoidentityprovider.GetUserOutput, error) {
			return nil, fmt.Errorf("cognito unavailable")
		},
	}
	a := newTestAppWithMocks(cognito, &mockDynamo{})
	body := `{"email":"test@example.com","password":"Pass123!"}`
	resp, _ := a.Handler(context.Background(), makePublicRequest("POST /auth/login", body))
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200, got %d", resp.StatusCode)
	}
}

func TestHandler_Login_DynamoGetUserError_SkipsCreate(t *testing.T) {
	jwtToken := makeJWT("user-sub-123")
	var createCalled bool
	cognito := &mockCognito{
		initiateAuthFn: func(ctx context.Context, params *cognitoidentityprovider.InitiateAuthInput, optFns ...func(*cognitoidentityprovider.Options)) (*cognitoidentityprovider.InitiateAuthOutput, error) {
			return &cognitoidentityprovider.InitiateAuthOutput{
				AuthenticationResult: &cognitotypes.AuthenticationResultType{
					AccessToken:  aws.String(jwtToken),
					IdToken:      aws.String("id-tok"),
					RefreshToken: aws.String("refresh-tok"),
					ExpiresIn:    3600,
				},
			}, nil
		},
	}
	dynamo := &mockDynamo{
		getItemFn: func(ctx context.Context, params *dynamodb.GetItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			return nil, fmt.Errorf("dynamo error")
		},
		putItemFn: func(ctx context.Context, params *dynamodb.PutItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
			createCalled = true
			return &dynamodb.PutItemOutput{}, nil
		},
	}
	a := newTestAppWithMocks(cognito, dynamo)
	body := `{"email":"test@example.com","password":"Pass123!"}`
	resp, _ := a.Handler(context.Background(), makePublicRequest("POST /auth/login", body))
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200, got %d", resp.StatusCode)
	}
	if createCalled {
		t.Error("CreateUser should NOT be called when GetUser returns an error")
	}
}

// --- Content-Type header ---

// --- KMS Encryption/Decryption Error Paths ---

func TestHandler_BankExchangeToken_KMSEncryptError(t *testing.T) {
	a := newTestApp()
	a.TokenEncryptor = crypto.NewTokenEncryptor(&mockKMS{
		encryptFn: func(ctx context.Context, params *kms.EncryptInput, optFns ...func(*kms.Options)) (*kms.EncryptOutput, error) {
			return nil, fmt.Errorf("kms unavailable")
		},
	}, "test-key")
	body := `{"enrollment_token":"tok_abc","provider":"teller"}`
	resp, _ := a.Handler(context.Background(), makeAuthRequest("POST /bank/exchange-token", body))
	if resp.StatusCode != http.StatusInternalServerError {
		t.Errorf("expected 500 on KMS encrypt failure, got %d: %s", resp.StatusCode, resp.Body)
	}
}

func TestHandler_BankAccounts_KMSDecryptError(t *testing.T) {
	a := newTestApp()
	a.TokenEncryptor = crypto.NewTokenEncryptor(&mockKMS{
		decryptFn: func(ctx context.Context, params *kms.DecryptInput, optFns ...func(*kms.Options)) (*kms.DecryptOutput, error) {
			return nil, fmt.Errorf("access denied")
		},
	}, "test-key")
	body := fmt.Sprintf(`{"encrypted_access_token":"%s","provider":"teller"}`, testEncryptToken("tok"))
	resp, _ := a.Handler(context.Background(), makeAuthRequest("POST /bank/accounts", body))
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("expected 400 on KMS decrypt failure, got %d: %s", resp.StatusCode, resp.Body)
	}
}

func TestHandler_BankSyncTransactions_KMSDecryptError(t *testing.T) {
	a := newTestApp()
	a.TokenEncryptor = crypto.NewTokenEncryptor(&mockKMS{
		decryptFn: func(ctx context.Context, params *kms.DecryptInput, optFns ...func(*kms.Options)) (*kms.DecryptOutput, error) {
			return nil, fmt.Errorf("access denied")
		},
	}, "test-key")
	body := fmt.Sprintf(`{"encrypted_access_token":"%s","provider":"teller"}`, testEncryptToken("tok"))
	resp, _ := a.Handler(context.Background(), makeAuthRequest("POST /bank/sync-transactions", body))
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("expected 400 on KMS decrypt failure, got %d: %s", resp.StatusCode, resp.Body)
	}
}

func TestHandler_BankExchangeToken_VerifiesEncryption(t *testing.T) {
	a := newTestApp()
	body := `{"enrollment_token":"tok_abc","provider":"teller"}`
	resp, _ := a.Handler(context.Background(), makeAuthRequest("POST /bank/exchange-token", body))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", resp.StatusCode, resp.Body)
	}

	// Parse response and verify the token is base64-encoded (encrypted)
	parsed := parseResponse(t, resp)
	if !parsed.Success {
		t.Fatal("expected success=true")
	}
	var exchangeResp api.ExchangeTokenResponse
	dataBytes, _ := json.Marshal(parsed.Data)
	if err := json.Unmarshal(dataBytes, &exchangeResp); err != nil {
		t.Fatalf("failed to parse exchange response: %v", err)
	}

	// The encrypted token should be base64-encoded, not the raw access token
	if exchangeResp.EncryptedAccessToken == "access_tok_abc" {
		t.Error("access token should be encrypted (base64-encoded), not plaintext")
	}
	// Verify we can decode it as base64
	_, err := base64.StdEncoding.DecodeString(exchangeResp.EncryptedAccessToken)
	if err != nil {
		t.Errorf("encrypted token should be valid base64: %v", err)
	}
}

func TestHandler_ResponseContentType(t *testing.T) {
	a := newTestApp()
	resp, _ := a.Handler(context.Background(), makePublicRequest("GET /health", ""))
	if resp.Headers["Content-Type"] != "application/json" {
		t.Errorf("expected Content-Type=application/json, got %s", resp.Headers["Content-Type"])
	}
}

// ============================================================
// Sync mock clients (for sync service injection into App)
// ============================================================

type mockSyncDynamo struct {
	putItemFn func(ctx context.Context, params *dynamodb.PutItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error)
	getItemFn func(ctx context.Context, params *dynamodb.GetItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error)
}

func (m *mockSyncDynamo) PutItem(ctx context.Context, params *dynamodb.PutItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
	if m.putItemFn != nil {
		return m.putItemFn(ctx, params, optFns...)
	}
	return &dynamodb.PutItemOutput{}, nil
}

func (m *mockSyncDynamo) GetItem(ctx context.Context, params *dynamodb.GetItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
	if m.getItemFn != nil {
		return m.getItemFn(ctx, params, optFns...)
	}
	return &dynamodb.GetItemOutput{}, nil
}

type mockSyncS3 struct {
	putObjectFn func(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	getObjectFn func(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error)
}

func (m *mockSyncS3) PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	if m.putObjectFn != nil {
		return m.putObjectFn(ctx, params, optFns...)
	}
	return &s3.PutObjectOutput{}, nil
}

func (m *mockSyncS3) GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	if m.getObjectFn != nil {
		return m.getObjectFn(ctx, params, optFns...)
	}
	return &s3.GetObjectOutput{}, nil
}

type mockSyncPresigner struct {
	url string
	err error
}

func (m *mockSyncPresigner) PresignGetObject(_ context.Context, _, _, _ string, _ time.Duration) (string, error) {
	if m.err != nil {
		return "", m.err
	}
	if m.url != "" {
		return m.url, nil
	}
	return "https://s3.example.com/presigned", nil
}

func makeSyncItem(userID string, version int, sizeBytes int64, checksum, deviceID string) map[string]dbtypes.AttributeValue {
	type syncRecord struct {
		PK        string `dynamodbav:"PK"`
		SK        string `dynamodbav:"SK"`
		Version   int    `dynamodbav:"version"`
		SizeBytes int64  `dynamodbav:"size_bytes"`
		Checksum  string `dynamodbav:"checksum"`
		DeviceID  string `dynamodbav:"device_id"`
		UpdatedAt string `dynamodbav:"updated_at"`
	}
	record := syncRecord{
		PK:        fmt.Sprintf("USER#%s", userID),
		SK:        "SYNC",
		Version:   version,
		SizeBytes: sizeBytes,
		Checksum:  checksum,
		DeviceID:  deviceID,
		UpdatedAt: "2025-01-01T00:00:00Z",
	}
	item, _ := attributevalue.MarshalMap(record)
	return item
}

func newTestAppWithSync(syncDB *mockSyncDynamo, syncS3 *mockSyncS3, presigner *mockSyncPresigner) *App {
	a := newTestApp()
	a.SyncService = syncpkg.NewServiceWithClients(syncDB, syncS3, presigner, "test-sync-bucket")
	return a
}

// ============================================================
// Sync endpoint tests
// ============================================================

func TestHandler_SyncPush_Success(t *testing.T) {
	syncDB := &mockSyncDynamo{
		getItemFn: func(_ context.Context, _ *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			return &dynamodb.GetItemOutput{Item: nil}, nil
		},
	}
	a := newTestAppWithSync(syncDB, &mockSyncS3{}, &mockSyncPresigner{})

	body := `{"data":"ZW5jcnlwdGVkLWJsb2I=","checksum":"sha256:abc123","device_id":"device-a"}`
	resp, _ := a.Handler(context.Background(), makeAuthRequest("POST /sync/push", body))

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", resp.StatusCode, resp.Body)
	}
	parsed := parseResponse(t, resp)
	if !parsed.Success {
		t.Fatal("expected success=true")
	}

	var pushResp api.SyncPushResponse
	dataBytes, _ := json.Marshal(parsed.Data)
	if err := json.Unmarshal(dataBytes, &pushResp); err != nil {
		t.Fatalf("failed to parse push response: %v", err)
	}
	if pushResp.Version != 1 {
		t.Errorf("expected version 1, got %d", pushResp.Version)
	}
}

func TestHandler_SyncPush_MissingData(t *testing.T) {
	a := newTestAppWithSync(&mockSyncDynamo{}, &mockSyncS3{}, &mockSyncPresigner{})

	body := `{"checksum":"sha256:abc","device_id":"d"}`
	resp, _ := a.Handler(context.Background(), makeAuthRequest("POST /sync/push", body))
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", resp.StatusCode, resp.Body)
	}
}

func TestHandler_SyncPush_MissingChecksum(t *testing.T) {
	a := newTestAppWithSync(&mockSyncDynamo{}, &mockSyncS3{}, &mockSyncPresigner{})

	body := `{"data":"blob","device_id":"d"}`
	resp, _ := a.Handler(context.Background(), makeAuthRequest("POST /sync/push", body))
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", resp.StatusCode, resp.Body)
	}
}

func TestHandler_SyncPush_MissingDeviceID(t *testing.T) {
	a := newTestAppWithSync(&mockSyncDynamo{}, &mockSyncS3{}, &mockSyncPresigner{})

	body := `{"data":"blob","checksum":"sha256:abc"}`
	resp, _ := a.Handler(context.Background(), makeAuthRequest("POST /sync/push", body))
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", resp.StatusCode, resp.Body)
	}
}

func TestHandler_SyncPush_InvalidBody(t *testing.T) {
	a := newTestAppWithSync(&mockSyncDynamo{}, &mockSyncS3{}, &mockSyncPresigner{})

	resp, _ := a.Handler(context.Background(), makeAuthRequest("POST /sync/push", "not json"))
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", resp.StatusCode, resp.Body)
	}
}

func TestHandler_SyncPush_S3Error(t *testing.T) {
	syncDB := &mockSyncDynamo{
		getItemFn: func(_ context.Context, _ *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			return &dynamodb.GetItemOutput{Item: nil}, nil
		},
	}
	syncS3 := &mockSyncS3{
		putObjectFn: func(_ context.Context, _ *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
			return nil, fmt.Errorf("s3 unavailable")
		},
	}
	a := newTestAppWithSync(syncDB, syncS3, &mockSyncPresigner{})

	body := `{"data":"blob","checksum":"sha256:abc","device_id":"d"}`
	resp, _ := a.Handler(context.Background(), makeAuthRequest("POST /sync/push", body))
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d: %s", resp.StatusCode, resp.Body)
	}
}

func TestHandler_SyncPush_NoSyncService(t *testing.T) {
	a := newTestApp() // no sync service
	body := `{"data":"blob","checksum":"sha256:abc","device_id":"d"}`
	resp, _ := a.Handler(context.Background(), makeAuthRequest("POST /sync/push", body))
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d: %s", resp.StatusCode, resp.Body)
	}
}

func TestHandler_SyncPull_Success(t *testing.T) {
	syncDB := &mockSyncDynamo{
		getItemFn: func(_ context.Context, _ *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			return &dynamodb.GetItemOutput{
				Item: makeSyncItem("user-123", 5, 2048, "sha256:xyz", "device-a"),
			}, nil
		},
	}
	presigner := &mockSyncPresigner{url: "https://s3.example.com/sync/user-123/db.enc?signed"}
	a := newTestAppWithSync(syncDB, &mockSyncS3{}, presigner)

	resp, _ := a.Handler(context.Background(), makeAuthRequest("GET /sync/pull", ""))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", resp.StatusCode, resp.Body)
	}

	parsed := parseResponse(t, resp)
	if !parsed.Success {
		t.Fatal("expected success=true")
	}

	var pullResp api.SyncPullResponse
	dataBytes, _ := json.Marshal(parsed.Data)
	if err := json.Unmarshal(dataBytes, &pullResp); err != nil {
		t.Fatalf("failed to parse pull response: %v", err)
	}
	if pullResp.Version != 5 {
		t.Errorf("expected version 5, got %d", pullResp.Version)
	}
	if pullResp.URL == "" {
		t.Error("expected presigned URL")
	}
	if pullResp.Checksum != "sha256:xyz" {
		t.Errorf("expected checksum sha256:xyz, got %s", pullResp.Checksum)
	}
}

func TestHandler_SyncPull_NoData(t *testing.T) {
	syncDB := &mockSyncDynamo{
		getItemFn: func(_ context.Context, _ *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			return &dynamodb.GetItemOutput{Item: nil}, nil
		},
	}
	a := newTestAppWithSync(syncDB, &mockSyncS3{}, &mockSyncPresigner{})

	resp, _ := a.Handler(context.Background(), makeAuthRequest("GET /sync/pull", ""))
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404, got %d: %s", resp.StatusCode, resp.Body)
	}
}

func TestHandler_SyncPull_NoSyncService(t *testing.T) {
	a := newTestApp()
	resp, _ := a.Handler(context.Background(), makeAuthRequest("GET /sync/pull", ""))
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d: %s", resp.StatusCode, resp.Body)
	}
}

func TestHandler_SyncPull_PresignError(t *testing.T) {
	syncDB := &mockSyncDynamo{
		getItemFn: func(_ context.Context, _ *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			return &dynamodb.GetItemOutput{
				Item: makeSyncItem("user-123", 1, 100, "x", "d"),
			}, nil
		},
	}
	presigner := &mockSyncPresigner{err: fmt.Errorf("presign failed")}
	a := newTestAppWithSync(syncDB, &mockSyncS3{}, presigner)

	resp, _ := a.Handler(context.Background(), makeAuthRequest("GET /sync/pull", ""))
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d: %s", resp.StatusCode, resp.Body)
	}
}

func TestHandler_SyncStatus_HasData(t *testing.T) {
	syncDB := &mockSyncDynamo{
		getItemFn: func(_ context.Context, _ *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			return &dynamodb.GetItemOutput{
				Item: makeSyncItem("user-123", 3, 1024, "sha256:abc", "device-a"),
			}, nil
		},
	}
	a := newTestAppWithSync(syncDB, &mockSyncS3{}, &mockSyncPresigner{})

	resp, _ := a.Handler(context.Background(), makeAuthRequest("GET /sync/status", ""))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", resp.StatusCode, resp.Body)
	}

	parsed := parseResponse(t, resp)
	if !parsed.Success {
		t.Fatal("expected success=true")
	}

	var statusResp api.SyncStatusResponse
	dataBytes, _ := json.Marshal(parsed.Data)
	if err := json.Unmarshal(dataBytes, &statusResp); err != nil {
		t.Fatalf("failed to parse status response: %v", err)
	}
	if !statusResp.HasData {
		t.Error("expected has_data=true")
	}
	if statusResp.Version != 3 {
		t.Errorf("expected version 3, got %d", statusResp.Version)
	}
	if statusResp.DeviceID != "device-a" {
		t.Errorf("expected device-a, got %s", statusResp.DeviceID)
	}
}

func TestHandler_SyncStatus_NoData(t *testing.T) {
	syncDB := &mockSyncDynamo{
		getItemFn: func(_ context.Context, _ *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			return &dynamodb.GetItemOutput{Item: nil}, nil
		},
	}
	a := newTestAppWithSync(syncDB, &mockSyncS3{}, &mockSyncPresigner{})

	resp, _ := a.Handler(context.Background(), makeAuthRequest("GET /sync/status", ""))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", resp.StatusCode, resp.Body)
	}

	parsed := parseResponse(t, resp)
	var statusResp api.SyncStatusResponse
	dataBytes, _ := json.Marshal(parsed.Data)
	if err := json.Unmarshal(dataBytes, &statusResp); err != nil {
		t.Fatalf("failed to parse status response: %v", err)
	}
	if statusResp.HasData {
		t.Error("expected has_data=false")
	}
}

func TestHandler_SyncStatus_NoSyncService(t *testing.T) {
	a := newTestApp()
	resp, _ := a.Handler(context.Background(), makeAuthRequest("GET /sync/status", ""))
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d: %s", resp.StatusCode, resp.Body)
	}
}

func TestHandler_SyncStatus_DynamoError(t *testing.T) {
	syncDB := &mockSyncDynamo{
		getItemFn: func(_ context.Context, _ *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			return nil, fmt.Errorf("dynamo error")
		},
	}
	a := newTestAppWithSync(syncDB, &mockSyncS3{}, &mockSyncPresigner{})

	resp, _ := a.Handler(context.Background(), makeAuthRequest("GET /sync/status", ""))
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d: %s", resp.StatusCode, resp.Body)
	}
}

func TestHandler_SyncPush_RequiresAuth(t *testing.T) {
	a := newTestAppWithSync(&mockSyncDynamo{}, &mockSyncS3{}, &mockSyncPresigner{})
	// Request with JWT but empty claims (no "sub")
	req := makePublicRequest("POST /sync/push", `{"data":"x","checksum":"y","device_id":"z"}`)
	req.RequestContext.Authorizer = &events.APIGatewayV2HTTPRequestContextAuthorizerDescription{
		JWT: &events.APIGatewayV2HTTPRequestContextAuthorizerJWTDescription{
			Claims: map[string]string{},
		},
	}
	resp, _ := a.Handler(context.Background(), req)
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d: %s", resp.StatusCode, resp.Body)
	}
}
