package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/kms"

	"github.com/wbgoodwin/algo-meep/backend/internal/auth"
	"github.com/wbgoodwin/algo-meep/backend/internal/crypto"
	"github.com/wbgoodwin/algo-meep/backend/internal/middleware"
	"github.com/wbgoodwin/algo-meep/backend/internal/provider"
	"github.com/wbgoodwin/algo-meep/backend/internal/user"
	"github.com/wbgoodwin/algo-meep/backend/pkg/api"
)

// App holds all injectable dependencies for the API handler.
type App struct {
	Log            *middleware.Logger
	AuthService    *auth.Service
	UserService    *user.Service
	ProviderReg    *provider.Registry
	AllowedIPs     map[string]bool
	TokenEncryptor *crypto.TokenEncryptor
}

var app *App

func init() {
	logger := middleware.NewLogger("ALGOFLOW_API")

	// Load IP allowlist from env (comma-separated). Empty = allow all.
	allowedIPs := make(map[string]bool)
	if ipList := os.Getenv("ALLOWED_IPS"); ipList != "" {
		for _, ip := range strings.Split(ipList, ",") {
			allowedIPs[strings.TrimSpace(ip)] = true
		}
	}

	awsCfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		logger.Error("Failed to load AWS config", err)
		os.Exit(1)
	}

	// Auth service (Cognito)
	userPoolID := os.Getenv("COGNITO_USER_POOL_ID")
	clientID := os.Getenv("COGNITO_CLIENT_ID")
	authSvc := auth.NewService(awsCfg, userPoolID, clientID)

	// User service (DynamoDB)
	userSvc := user.NewService(awsCfg)

	// Provider registry
	providerReg := provider.NewRegistry()

	// Initialize Teller provider if configured
	tellerAPIKey := os.Getenv("TELLER_API_KEY")
	tellerCert := os.Getenv("TELLER_CERT_PEM")
	tellerKey := os.Getenv("TELLER_KEY_PEM")
	tellerEnv := os.Getenv("TELLER_ENV")

	if tellerAPIKey != "" {
		baseURL := "https://api.teller.io"
		if tellerEnv == "sandbox" {
			baseURL = "https://api.teller.io"
		}
		tp, err := provider.NewTellerProvider(provider.TellerConfig{
			APIKey:  tellerAPIKey,
			CertPEM: []byte(tellerCert),
			KeyPEM:  []byte(tellerKey),
			BaseURL: baseURL,
		})
		if err != nil {
			logger.Error("Failed to initialize Teller provider", err)
		} else {
			providerReg.Register(tp)
			logger.Info("Teller provider initialized")
		}
	}

	// Token encryption (KMS)
	var tokenEnc *crypto.TokenEncryptor
	if kmsKeyARN := os.Getenv("KMS_KEY_ARN"); kmsKeyARN != "" {
		tokenEnc = crypto.NewTokenEncryptor(kms.NewFromConfig(awsCfg), kmsKeyARN)
		logger.Info("KMS token encryption initialized")
	} else {
		logger.Warn("KMS_KEY_ARN not set — token encryption disabled (must be set in production)")
	}

	app = &App{
		Log:            logger,
		AuthService:    authSvc,
		UserService:    userSvc,
		ProviderReg:    providerReg,
		AllowedIPs:     allowedIPs,
		TokenEncryptor: tokenEnc,
	}

	logger.Info("API Lambda initialized",
		middleware.WithField("user_pool_id", userPoolID),
		middleware.WithField("providers", strings.Join(providerReg.List(), ",")),
	)
}

func (a *App) Handler(ctx context.Context, req events.APIGatewayV2HTTPRequest) (events.APIGatewayV2HTTPResponse, error) {
	start := time.Now()
	routeKey := req.RouteKey

	// IP allowlist — reject requests not from allowed IPs
	sourceIP := req.RequestContext.HTTP.SourceIP
	if !a.isAllowedIP(sourceIP) {
		a.Log.Warn("Blocked request from unauthorized IP",
			middleware.WithField("source_ip", sourceIP),
			middleware.WithRoute(routeKey),
		)
		return events.APIGatewayV2HTTPResponse{
			StatusCode: http.StatusForbidden,
			Headers:    map[string]string{"Content-Type": "application/json"},
			Body:       `{"success":false,"error":{"code":"FORBIDDEN","message":"access denied"}}`,
		}, nil
	}

	a.Log.Info("Request received", middleware.WithRoute(routeKey))

	var (
		statusCode int
		body       []byte
	)

	switch routeKey {
	// --- Public routes ---
	case "GET /health":
		statusCode, body = a.handleHealth()
	case "POST /auth/register":
		statusCode, body = a.handleRegister(ctx, req)
	case "POST /auth/login":
		statusCode, body = a.handleLogin(ctx, req)
	case "POST /auth/refresh":
		statusCode, body = a.handleRefresh(ctx, req)

	// --- Authenticated routes ---
	case "DELETE /auth/account":
		statusCode, body = a.withAuth(req, func(userID string) (int, []byte) {
			return a.handleDeleteAccount(ctx, req, userID)
		})
	case "GET /user/profile":
		statusCode, body = a.withAuth(req, func(userID string) (int, []byte) {
			return a.handleGetProfile(ctx, userID)
		})
	case "PUT /user/profile":
		statusCode, body = a.withAuth(req, func(userID string) (int, []byte) {
			return a.handleUpdateProfile(ctx, req, userID)
		})
	case "POST /bank/enroll":
		statusCode, body = a.withAuth(req, func(userID string) (int, []byte) {
			return a.handleBankEnroll(ctx, req, userID)
		})
	case "POST /bank/exchange-token":
		statusCode, body = a.withAuth(req, func(userID string) (int, []byte) {
			return a.handleBankExchangeToken(ctx, req, userID)
		})
	case "GET /bank/accounts":
		statusCode, body = a.withAuth(req, func(userID string) (int, []byte) {
			return a.handleBankAccounts(ctx, req, userID)
		})
	case "POST /bank/sync-transactions":
		statusCode, body = a.withAuth(req, func(userID string) (int, []byte) {
			return a.handleBankSyncTransactions(ctx, req, userID)
		})
	case "GET /bank/providers":
		statusCode, body = a.withAuth(req, func(_ string) (int, []byte) {
			return a.handleBankProviders()
		})

	default:
		statusCode, body = api.NewNotFound("route not found").ToAPIResponse()
	}

	a.Log.Info("Request completed",
		middleware.WithRoute(routeKey),
		middleware.WithDuration(time.Since(start)),
		middleware.WithField("status", fmt.Sprintf("%d", statusCode)),
	)

	return events.APIGatewayV2HTTPResponse{
		StatusCode: statusCode,
		Headers:    map[string]string{"Content-Type": "application/json"},
		Body:       string(body),
	}, nil
}

// withAuth extracts the user ID from the JWT authorizer context and calls the handler.
func (a *App) withAuth(req events.APIGatewayV2HTTPRequest, fn func(userID string) (int, []byte)) (int, []byte) {
	claims := req.RequestContext.Authorizer.JWT.Claims
	userID, err := auth.ExtractUserID(claims)
	if err != nil {
		return api.NewUnauthorized("invalid token").ToAPIResponse()
	}
	return fn(userID)
}

// --- Health ---

func (a *App) handleHealth() (int, []byte) {
	resp, _ := api.SuccessJSON(api.HealthResponse{
		Status:    "ok",
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		Version:   "0.1.0",
	})
	return http.StatusOK, resp
}

// --- Auth handlers ---

func (a *App) handleRegister(ctx context.Context, req events.APIGatewayV2HTTPRequest) (int, []byte) {
	var input api.RegisterRequest
	if err := json.Unmarshal([]byte(req.Body), &input); err != nil {
		return api.NewBadRequest("invalid request body").ToAPIResponse()
	}
	if input.Email == "" || input.Password == "" {
		return api.NewBadRequest("email and password are required").ToAPIResponse()
	}

	if err := a.AuthService.Register(ctx, input.Email, input.Password); err != nil {
		a.Log.Error("Registration failed", err, middleware.WithField("email_domain", emailDomain(input.Email)))
		if strings.Contains(err.Error(), "UsernameExistsException") {
			return api.NewConflict("user already exists").ToAPIResponse()
		}
		return api.NewInternal("registration failed", err).ToAPIResponse()
	}

	resp, _ := api.SuccessJSON(map[string]string{"message": "user registered, check email for verification"})
	return http.StatusCreated, resp
}

func (a *App) handleLogin(ctx context.Context, req events.APIGatewayV2HTTPRequest) (int, []byte) {
	var input api.LoginRequest
	if err := json.Unmarshal([]byte(req.Body), &input); err != nil {
		return api.NewBadRequest("invalid request body").ToAPIResponse()
	}
	if input.Email == "" || input.Password == "" {
		return api.NewBadRequest("email and password are required").ToAPIResponse()
	}

	tokens, err := a.AuthService.Login(ctx, input.Email, input.Password)
	if err != nil {
		a.Log.Error("Login failed", err, middleware.WithField("email_domain", emailDomain(input.Email)))
		if strings.Contains(err.Error(), "NotAuthorizedException") {
			return api.NewUnauthorized("invalid credentials").ToAPIResponse()
		}
		return api.NewInternal("login failed", err).ToAPIResponse()
	}

	// Ensure DynamoDB user profile exists (created on first login)
	if sub := extractSubFromJWT(tokens.AccessToken); sub != "" {
		existing, _ := a.UserService.GetUser(ctx, sub)
		if existing == nil {
			if createErr := a.UserService.CreateUser(ctx, sub, input.Email); createErr != nil {
				a.Log.Error("Failed to create user profile on first login", createErr,
					middleware.WithField("email_domain", emailDomain(input.Email)))
			} else {
				a.Log.Info("Created user profile on first login", middleware.WithUserID(sub))
			}
		}
	}

	resp, _ := api.SuccessJSON(api.AuthTokens{
		AccessToken:  tokens.AccessToken,
		IDToken:      tokens.IDToken,
		RefreshToken: tokens.RefreshToken,
		ExpiresIn:    int(tokens.ExpiresIn),
	})
	return http.StatusOK, resp
}

func (a *App) handleRefresh(ctx context.Context, req events.APIGatewayV2HTTPRequest) (int, []byte) {
	var input api.RefreshRequest
	if err := json.Unmarshal([]byte(req.Body), &input); err != nil {
		return api.NewBadRequest("invalid request body").ToAPIResponse()
	}
	if input.RefreshToken == "" {
		return api.NewBadRequest("refresh_token is required").ToAPIResponse()
	}

	tokens, err := a.AuthService.RefreshTokens(ctx, input.RefreshToken)
	if err != nil {
		a.Log.Error("Token refresh failed", err)
		return api.NewUnauthorized("invalid refresh token").ToAPIResponse()
	}

	resp, _ := api.SuccessJSON(api.AuthTokens{
		AccessToken: tokens.AccessToken,
		IDToken:     tokens.IDToken,
		ExpiresIn:   int(tokens.ExpiresIn),
	})
	return http.StatusOK, resp
}

func (a *App) handleDeleteAccount(ctx context.Context, req events.APIGatewayV2HTTPRequest, userID string) (int, []byte) {
	// Extract the bearer token to delete the Cognito user
	token, err := auth.ExtractBearerToken(req.Headers["authorization"])
	if err != nil {
		return api.NewUnauthorized("missing authorization header").ToAPIResponse()
	}

	// Delete from Cognito
	if err := a.AuthService.DeleteUser(ctx, token); err != nil {
		a.Log.Error("Failed to delete Cognito user", err, middleware.WithUserID(userID))
		return api.NewInternal("failed to delete account", err).ToAPIResponse()
	}

	// Delete from DynamoDB
	if err := a.UserService.DeleteUser(ctx, userID); err != nil {
		a.Log.Error("Failed to delete DynamoDB user record", err, middleware.WithUserID(userID))
		// Don't fail — Cognito user is already deleted
	}

	a.Log.Info("User account deleted", middleware.WithUserID(userID))
	resp, _ := api.SuccessJSON(map[string]string{"message": "account deleted"})
	return http.StatusOK, resp
}

// --- User handlers ---

func (a *App) handleGetProfile(ctx context.Context, userID string) (int, []byte) {
	record, err := a.UserService.GetUser(ctx, userID)
	if err != nil {
		a.Log.Error("Failed to get user profile", err, middleware.WithUserID(userID))
		return api.NewInternal("failed to get profile", err).ToAPIResponse()
	}
	if record == nil {
		return api.NewNotFound("user not found").ToAPIResponse()
	}

	resp, _ := api.SuccessJSON(api.UserProfile{
		UserID:            userID,
		Email:             record.Email,
		Plan:              record.Plan,
		ConnectedAccounts: record.ConnectedAccounts,
	})
	return http.StatusOK, resp
}

func (a *App) handleUpdateProfile(ctx context.Context, req events.APIGatewayV2HTTPRequest, userID string) (int, []byte) {
	var input api.UpdateProfileRequest
	if err := json.Unmarshal([]byte(req.Body), &input); err != nil {
		return api.NewBadRequest("invalid request body").ToAPIResponse()
	}

	if input.Plan != "" {
		validPlans := map[string]bool{"free": true, "standard": true, "premium": true}
		if !validPlans[input.Plan] {
			return api.NewBadRequest("invalid plan: must be free, standard, or premium").ToAPIResponse()
		}
		if err := a.UserService.UpdatePlan(ctx, userID, input.Plan); err != nil {
			a.Log.Error("Failed to update plan", err, middleware.WithUserID(userID))
			return api.NewInternal("failed to update profile", err).ToAPIResponse()
		}
	}

	resp, _ := api.SuccessJSON(map[string]string{"message": "profile updated"})
	return http.StatusOK, resp
}

// --- Bank handlers ---

func (a *App) handleBankEnroll(ctx context.Context, req events.APIGatewayV2HTTPRequest, userID string) (int, []byte) {
	var input api.EnrollRequest
	if err := json.Unmarshal([]byte(req.Body), &input); err != nil {
		return api.NewBadRequest("invalid request body").ToAPIResponse()
	}

	var p provider.BankProvider
	if input.Provider != "" {
		p = a.ProviderReg.Get(input.Provider)
	} else {
		p = a.ProviderReg.SelectProvider(input.InstitutionID)
	}
	if p == nil {
		return api.NewBadRequest("no provider available for this institution").ToAPIResponse()
	}

	session, err := p.StartEnrollment(userID, input.InstitutionID)
	if err != nil {
		a.Log.Error("Bank enrollment failed", err, middleware.WithUserID(userID))
		return api.NewProviderError("enrollment failed", err).ToAPIResponse()
	}

	resp, _ := api.SuccessJSON(api.EnrollResponse{
		SessionURL: session.SessionURL,
		SessionID:  session.SessionID,
		Provider:   session.Provider,
	})
	return http.StatusOK, resp
}

func (a *App) handleBankExchangeToken(ctx context.Context, req events.APIGatewayV2HTTPRequest, userID string) (int, []byte) {
	var input api.ExchangeTokenRequest
	if err := json.Unmarshal([]byte(req.Body), &input); err != nil {
		return api.NewBadRequest("invalid request body").ToAPIResponse()
	}

	p := a.ProviderReg.Get(input.Provider)
	if p == nil {
		return api.NewBadRequest("unknown provider: " + input.Provider).ToAPIResponse()
	}

	credential, err := p.ExchangeToken(input.EnrollmentToken)
	if err != nil {
		a.Log.Error("Token exchange failed", err, middleware.WithUserID(userID))
		return api.NewProviderError("token exchange failed", err).ToAPIResponse()
	}

	if a.TokenEncryptor == nil {
		a.Log.Error("Token encryption not configured", fmt.Errorf("KMS_KEY_ARN not set"), middleware.WithUserID(userID))
		return api.NewInternal("token encryption not configured", fmt.Errorf("KMS_KEY_ARN not set")).ToAPIResponse()
	}

	encryptedToken, err := a.TokenEncryptor.Encrypt(ctx, credential.AccessToken)
	if err != nil {
		a.Log.Error("Failed to encrypt access token", err, middleware.WithUserID(userID))
		return api.NewInternal("failed to secure access token", err).ToAPIResponse()
	}

	_ = a.UserService.IncrementConnectedAccounts(ctx, userID, 1)

	resp, _ := api.SuccessJSON(api.ExchangeTokenResponse{
		EncryptedAccessToken: encryptedToken,
		InstitutionID:        credential.InstitutionID,
		Provider:             credential.ProviderName,
	})
	return http.StatusOK, resp
}

func (a *App) handleBankAccounts(ctx context.Context, req events.APIGatewayV2HTTPRequest, userID string) (int, []byte) {
	var input api.AccountsRequest
	if err := json.Unmarshal([]byte(req.Body), &input); err != nil {
		// Try query params for GET
		input.EncryptedAccessToken = req.QueryStringParameters["access_token"]
		input.Provider = req.QueryStringParameters["provider"]
	}

	p := a.ProviderReg.Get(input.Provider)
	if p == nil {
		return api.NewBadRequest("unknown provider").ToAPIResponse()
	}

	if a.TokenEncryptor == nil {
		a.Log.Error("Token encryption not configured", fmt.Errorf("KMS_KEY_ARN not set"), middleware.WithUserID(userID))
		return api.NewInternal("token encryption not configured", fmt.Errorf("KMS_KEY_ARN not set")).ToAPIResponse()
	}

	plainToken, err := a.TokenEncryptor.Decrypt(ctx, input.EncryptedAccessToken)
	if err != nil {
		a.Log.Error("Failed to decrypt access token", err, middleware.WithUserID(userID))
		return api.NewBadRequest("invalid or corrupted access token").ToAPIResponse()
	}

	credential := provider.AccessCredential{
		ProviderName: input.Provider,
		AccessToken:  plainToken,
	}

	accounts, err := p.GetAccounts(credential)
	if err != nil {
		a.Log.Error("Get accounts failed", err, middleware.WithUserID(userID))
		return api.NewProviderError("failed to fetch accounts", err).ToAPIResponse()
	}

	// Convert provider accounts to API accounts
	apiAccounts := make([]api.Account, len(accounts))
	for i, acc := range accounts {
		apiAccounts[i] = api.Account{
			ID:               acc.ID,
			InstitutionID:    acc.InstitutionID,
			Name:             acc.Name,
			Type:             acc.Type,
			Subtype:          acc.Subtype,
			CurrentBalance:   acc.CurrentBalance,
			AvailableBalance: acc.AvailableBalance,
			CurrencyCode:     acc.CurrencyCode,
			LastUpdated:      acc.LastUpdated,
		}
	}

	resp, _ := api.SuccessJSON(apiAccounts)
	return http.StatusOK, resp
}

func (a *App) handleBankSyncTransactions(ctx context.Context, req events.APIGatewayV2HTTPRequest, userID string) (int, []byte) {
	var input api.SyncTransactionsRequest
	if err := json.Unmarshal([]byte(req.Body), &input); err != nil {
		return api.NewBadRequest("invalid request body").ToAPIResponse()
	}

	p := a.ProviderReg.Get(input.Provider)
	if p == nil {
		return api.NewBadRequest("unknown provider").ToAPIResponse()
	}

	if a.TokenEncryptor == nil {
		a.Log.Error("Token encryption not configured", fmt.Errorf("KMS_KEY_ARN not set"), middleware.WithUserID(userID))
		return api.NewInternal("token encryption not configured", fmt.Errorf("KMS_KEY_ARN not set")).ToAPIResponse()
	}

	plainToken, err := a.TokenEncryptor.Decrypt(ctx, input.EncryptedAccessToken)
	if err != nil {
		a.Log.Error("Failed to decrypt access token", err, middleware.WithUserID(userID))
		return api.NewBadRequest("invalid or corrupted access token").ToAPIResponse()
	}

	credential := provider.AccessCredential{
		ProviderName: input.Provider,
		AccessToken:  plainToken,
	}

	sync, err := p.SyncTransactions(credential, input.Cursor)
	if err != nil {
		a.Log.Error("Sync transactions failed", err, middleware.WithUserID(userID))
		return api.NewProviderError("failed to sync transactions", err).ToAPIResponse()
	}

	// Convert provider transactions to API transactions
	added := make([]api.Transaction, len(sync.Added))
	for i, t := range sync.Added {
		added[i] = api.Transaction{
			ID:           t.ID,
			AccountID:    t.AccountID,
			Amount:       t.Amount,
			Date:         t.Date,
			Description:  t.Description,
			MerchantName: t.MerchantName,
			Category:     t.Category,
			Pending:      t.Pending,
			CurrencyCode: t.CurrencyCode,
		}
	}

	updated := make([]api.Transaction, len(sync.Updated))
	for i, t := range sync.Updated {
		updated[i] = api.Transaction{
			ID:           t.ID,
			AccountID:    t.AccountID,
			Amount:       t.Amount,
			Date:         t.Date,
			Description:  t.Description,
			MerchantName: t.MerchantName,
			Category:     t.Category,
			Pending:      t.Pending,
			CurrencyCode: t.CurrencyCode,
		}
	}

	resp, _ := api.SuccessJSON(api.SyncTransactionsResponse{
		Added:   added,
		Updated: updated,
		Removed: sync.Removed,
		Cursor:  sync.Cursor,
	})
	return http.StatusOK, resp
}

func (a *App) handleBankProviders() (int, []byte) {
	names := a.ProviderReg.List()
	providers := make([]api.ProviderInfo, len(names))
	for i, name := range names {
		providers[i] = api.ProviderInfo{
			Name:             name,
			InstitutionCount: 0, // TODO: cache institution counts
		}
	}
	resp, _ := api.SuccessJSON(api.ProvidersResponse{Providers: providers})
	return http.StatusOK, resp
}

// extractSubFromJWT decodes the JWT payload (middle segment) to extract the "sub" claim.
func extractSubFromJWT(token string) string {
	parts := strings.SplitN(token, ".", 3)
	if len(parts) != 3 {
		return ""
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return ""
	}
	var claims struct {
		Sub string `json:"sub"`
	}
	if err := json.Unmarshal(payload, &claims); err != nil {
		return ""
	}
	return claims.Sub
}

// isAllowedIP checks if the source IP is in the allowlist.
// If the allowlist is empty (ALLOWED_IPS not set), all IPs are allowed.
func (a *App) isAllowedIP(ip string) bool {
	if len(a.AllowedIPs) == 0 {
		return true
	}
	return a.AllowedIPs[ip]
}

// emailDomain extracts the domain from an email for safe logging (no PII).
func emailDomain(email string) string {
	parts := strings.SplitN(email, "@", 2)
	if len(parts) == 2 {
		return parts[1]
	}
	return "unknown"
}

func main() {
	lambda.Start(app.Handler)
}
