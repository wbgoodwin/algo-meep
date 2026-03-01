package provider

type EnrollmentSession struct {
	SessionURL string // URL to open in webview (Teller Connect / FDX OAuth)
	SessionID  string // Server-side session tracking
	Provider   string // "teller" or "fdx"
}

type AccessCredential struct {
	ProviderName  string // "teller" or "fdx"
	AccessToken   string // Plaintext — Lambda encrypts via KMS before returning to client
	InstitutionID string
	EnrolledAt    string // ISO 8601
}

type Account struct {
	ID               string
	InstitutionID    string
	Name             string
	Type             string  // "depository", "credit", "investment"
	Subtype          string  // "checking", "savings", "credit_card", "401k", etc.
	CurrentBalance   float64
	AvailableBalance float64
	CurrencyCode     string
	LastUpdated      string
}

type Transaction struct {
	ID           string
	AccountID    string
	Amount       float64
	Date         string
	Description  string
	MerchantName string
	Category     string
	Pending      bool
	CurrencyCode string
}

type TransactionSync struct {
	Added   []Transaction
	Updated []Transaction
	Removed []string // Transaction IDs
	Cursor  string   // For next incremental sync
}

type Institution struct {
	ID   string
	Name string
}
