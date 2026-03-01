package provider

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// TellerProvider implements BankProvider using the Teller API.
// Teller uses mutual TLS (client certificate) for authentication.
// https://teller.io/docs
type TellerProvider struct {
	apiKey     string
	baseURL    string
	httpClient *http.Client
}

// TellerConfig holds configuration needed to initialize TellerProvider.
type TellerConfig struct {
	APIKey  string
	CertPEM []byte // Client certificate PEM bytes
	KeyPEM  []byte // Client private key PEM bytes
	BaseURL string // "https://api.teller.io" for prod, "https://api.teller.io/sandbox" for sandbox
}

// NewTellerProvider creates a new Teller provider with mTLS.
func NewTellerProvider(cfg TellerConfig) (*TellerProvider, error) {
	cert, err := tls.X509KeyPair(cfg.CertPEM, cfg.KeyPEM)
	if err != nil {
		return nil, fmt.Errorf("failed to load teller certificate: %w", err)
	}

	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{cert},
	}

	baseURL := cfg.BaseURL
	if baseURL == "" {
		baseURL = "https://api.teller.io"
	}

	return &TellerProvider{
		apiKey:  cfg.APIKey,
		baseURL: baseURL,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
			Transport: &http.Transport{
				TLSClientConfig: tlsConfig,
			},
		},
	}, nil
}

func (t *TellerProvider) Name() string {
	return "teller"
}

func (t *TellerProvider) StartEnrollment(userID string, institutionID string) (*EnrollmentSession, error) {
	// Teller Connect is a client-side widget. The backend provides the
	// application ID; the client opens Teller Connect in a webview.
	// The enrollment session URL points to Teller Connect with the app ID.
	return &EnrollmentSession{
		SessionURL: fmt.Sprintf("https://teller.io/connect?application_id=%s&institution=%s", t.apiKey, institutionID),
		SessionID:  fmt.Sprintf("teller_%s_%d", userID, time.Now().UnixMilli()),
		Provider:   "teller",
	}, nil
}

func (t *TellerProvider) ExchangeToken(enrollmentToken string) (*AccessCredential, error) {
	// After Teller Connect completes, it returns an access token directly.
	// The enrollmentToken IS the access token from Teller Connect.
	return &AccessCredential{
		ProviderName:  "teller",
		AccessToken:   enrollmentToken,
		InstitutionID: "", // resolved on first GetAccounts call
		EnrolledAt:    time.Now().UTC().Format(time.RFC3339),
	}, nil
}

func (t *TellerProvider) GetAccounts(credential AccessCredential) ([]Account, error) {
	req, err := http.NewRequest("GET", t.baseURL+"/accounts", nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}
	req.SetBasicAuth(credential.AccessToken, "")

	resp, err := t.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("teller accounts request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("teller accounts returned %d: %s", resp.StatusCode, string(body))
	}

	var tellerAccounts []tellerAccount
	if err := json.NewDecoder(resp.Body).Decode(&tellerAccounts); err != nil {
		return nil, fmt.Errorf("failed to decode teller accounts: %w", err)
	}

	accounts := make([]Account, len(tellerAccounts))
	for i, ta := range tellerAccounts {
		accounts[i] = Account{
			ID:              ta.ID,
			InstitutionID:   ta.Institution.ID,
			Name:            ta.Name,
			Type:            ta.Type,
			Subtype:         ta.Subtype,
			CurrencyCode:    ta.Currency,
			LastUpdated:      ta.LastFour, // placeholder — Teller doesn't have a direct "last_updated"
		}
	}
	return accounts, nil
}

func (t *TellerProvider) SyncTransactions(credential AccessCredential, cursor string) (*TransactionSync, error) {
	// Teller uses pagination, not cursors. We fetch all transactions for each account.
	// The cursor in our system tracks the last-seen transaction ID for incremental sync.
	url := t.baseURL + "/accounts/" + credential.InstitutionID + "/transactions"
	if cursor != "" {
		url += "?from_id=" + cursor
	}

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}
	req.SetBasicAuth(credential.AccessToken, "")

	resp, err := t.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("teller transactions request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("teller transactions returned %d: %s", resp.StatusCode, string(body))
	}

	var tellerTxns []tellerTransaction
	if err := json.NewDecoder(resp.Body).Decode(&tellerTxns); err != nil {
		return nil, fmt.Errorf("failed to decode teller transactions: %w", err)
	}

	txns := make([]Transaction, len(tellerTxns))
	newCursor := cursor
	for i, tt := range tellerTxns {
		txns[i] = Transaction{
			ID:           tt.ID,
			AccountID:    tt.AccountID,
			Amount:       tt.Amount,
			Date:         tt.Date,
			Description:  tt.Description,
			MerchantName: tt.Details.Counterparty.Name,
			Category:     tt.Details.Category,
			Pending:      tt.Status == "pending",
			CurrencyCode: "USD",
		}
		if tt.ID > newCursor {
			newCursor = tt.ID
		}
	}

	return &TransactionSync{
		Added:   txns,
		Updated: nil,
		Removed: nil,
		Cursor:  newCursor,
	}, nil
}

func (t *TellerProvider) GetInstitutions() ([]Institution, error) {
	req, err := http.NewRequest("GET", t.baseURL+"/institutions", nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}
	req.SetBasicAuth(t.apiKey, "")

	resp, err := t.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("teller institutions request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("teller institutions returned %d: %s", resp.StatusCode, string(body))
	}

	var tellerInsts []tellerInstitution
	if err := json.NewDecoder(resp.Body).Decode(&tellerInsts); err != nil {
		return nil, fmt.Errorf("failed to decode teller institutions: %w", err)
	}

	institutions := make([]Institution, len(tellerInsts))
	for i, ti := range tellerInsts {
		institutions[i] = Institution{
			ID:   ti.ID,
			Name: ti.Name,
		}
	}
	return institutions, nil
}

// Teller API response types (internal).

type tellerAccount struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Type        string `json:"type"`
	Subtype     string `json:"subtype"`
	Currency    string `json:"currency"`
	LastFour    string `json:"last_four"`
	Institution struct {
		ID   string `json:"id"`
		Name string `json:"name"`
	} `json:"institution"`
	Balances struct {
		Available string `json:"available"`
		Ledger    string `json:"ledger"`
	} `json:"balances"`
}

type tellerTransaction struct {
	ID          string  `json:"id"`
	AccountID   string  `json:"account_id"`
	Amount      float64 `json:"amount,string"`
	Date        string  `json:"date"`
	Description string  `json:"description"`
	Status      string  `json:"status"`
	Details     struct {
		Category     string `json:"category"`
		Counterparty struct {
			Name string `json:"name"`
		} `json:"counterparty"`
	} `json:"details"`
}

type tellerInstitution struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}
