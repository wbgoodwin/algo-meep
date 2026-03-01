package api

import "time"

// APIResponse is the standard JSON envelope for all API responses.
type APIResponse struct {
	Success bool        `json:"success"`
	Data    interface{} `json:"data,omitempty"`
	Error   *APIError   `json:"error,omitempty"`
}

// APIError represents a structured error in the response.
type APIError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// HealthResponse is returned by GET /health.
type HealthResponse struct {
	Status    string `json:"status"`
	Timestamp string `json:"timestamp"`
	Version   string `json:"version"`
}

// --- Auth types ---

type RegisterRequest struct {
	Email    string `json:"email"`
	Password string `json:"password"`
}

type LoginRequest struct {
	Email    string `json:"email"`
	Password string `json:"password"`
}

type RefreshRequest struct {
	RefreshToken string `json:"refresh_token"`
}

type AuthTokens struct {
	AccessToken  string `json:"access_token"`
	IDToken      string `json:"id_token"`
	RefreshToken string `json:"refresh_token"`
	ExpiresIn    int    `json:"expires_in"`
}

// --- Bank provider types ---

type EnrollRequest struct {
	InstitutionID string `json:"institution_id"`
	Provider      string `json:"provider,omitempty"` // "teller" or "fdx"; empty = auto-select
}

type EnrollResponse struct {
	SessionURL string `json:"session_url"`
	SessionID  string `json:"session_id"`
	Provider   string `json:"provider"`
}

type ExchangeTokenRequest struct {
	EnrollmentToken string `json:"enrollment_token"`
	Provider        string `json:"provider"`
}

type ExchangeTokenResponse struct {
	EncryptedAccessToken string `json:"encrypted_access_token"`
	InstitutionID        string `json:"institution_id"`
	Provider             string `json:"provider"`
}

type SyncTransactionsRequest struct {
	EncryptedAccessToken string `json:"encrypted_access_token"`
	Provider             string `json:"provider"`
	Cursor               string `json:"cursor,omitempty"`
}

type SyncTransactionsResponse struct {
	Added   []Transaction `json:"added"`
	Updated []Transaction `json:"updated"`
	Removed []string      `json:"removed"`
	Cursor  string        `json:"cursor"`
}

type AccountsRequest struct {
	EncryptedAccessToken string `json:"encrypted_access_token"`
	Provider             string `json:"provider"`
}

type Account struct {
	ID               string  `json:"id"`
	InstitutionID    string  `json:"institution_id"`
	Name             string  `json:"name"`
	Type             string  `json:"type"`
	Subtype          string  `json:"subtype"`
	CurrentBalance   float64 `json:"current_balance"`
	AvailableBalance float64 `json:"available_balance,omitempty"`
	CurrencyCode     string  `json:"currency_code"`
	LastUpdated      string  `json:"last_updated"`
}

type Transaction struct {
	ID           string  `json:"id"`
	AccountID    string  `json:"account_id"`
	Amount       float64 `json:"amount"`
	Date         string  `json:"date"`
	Description  string  `json:"description"`
	MerchantName string  `json:"merchant_name,omitempty"`
	Category     string  `json:"category,omitempty"`
	Pending      bool    `json:"pending"`
	CurrencyCode string  `json:"currency_code"`
}

type Institution struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	Provider string `json:"provider"` // "teller" or "fdx"
}

type ProvidersResponse struct {
	Providers []ProviderInfo `json:"providers"`
}

type ProviderInfo struct {
	Name             string `json:"name"`
	InstitutionCount int    `json:"institution_count"`
}

// --- User types ---

type UserProfile struct {
	UserID            string    `json:"user_id"`
	Email             string    `json:"email"`
	Plan              string    `json:"plan"` // "free", "standard", "premium"
	ConnectedAccounts int       `json:"connected_accounts"`
	CreatedAt         time.Time `json:"created_at"`
}

type UpdateProfileRequest struct {
	Plan string `json:"plan,omitempty"`
}

type UsageResponse struct {
	Month            string  `json:"month"`
	ProviderSyncs    int     `json:"provider_syncs"`
	SyncStorageBytes int64   `json:"sync_storage_bytes"`
	APICalls         int     `json:"api_calls"`
	EstimatedCost    float64 `json:"estimated_cost"`
}

// --- Sync types ---

type SyncPushRequest struct {
	Data     string `json:"data"`      // base64-encoded encrypted blob
	Checksum string `json:"checksum"`  // client-computed checksum (e.g. "sha256:<hex>")
	DeviceID string `json:"device_id"` // originating device identifier
}

type SyncPushResponse struct {
	Version   int    `json:"version"`
	SizeBytes int64  `json:"size_bytes"`
	UpdatedAt string `json:"updated_at"`
}

type SyncPullResponse struct {
	URL       string `json:"url"` // presigned S3 download URL (expires in 15min)
	Version   int    `json:"version"`
	SizeBytes int64  `json:"size_bytes"`
	Checksum  string `json:"checksum"`
	UpdatedAt string `json:"updated_at"`
}

type SyncStatusResponse struct {
	HasData   bool   `json:"has_data"`
	Version   int    `json:"version,omitempty"`
	SizeBytes int64  `json:"size_bytes,omitempty"`
	Checksum  string `json:"checksum,omitempty"`
	DeviceID  string `json:"device_id,omitempty"`
	UpdatedAt string `json:"updated_at,omitempty"`
}
