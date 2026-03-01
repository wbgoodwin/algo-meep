package provider

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

// testCertAndKey returns a valid self-signed EC cert/key pair for testing NewTellerProvider.
func testCertAndKey() ([]byte, []byte) {
	certPEM := []byte(`-----BEGIN CERTIFICATE-----
MIIBczCCARmgAwIBAgIUdMo9TcRXpyN3x4gO5OGAyJa5AIYwCgYIKoZIzj0EAwIw
DzENMAsGA1UEAwwEdGVzdDAeFw0yNjAzMDEwMjA4MDdaFw0yNjAzMDIwMjA4MDda
MA8xDTALBgNVBAMMBHRlc3QwWTATBgcqhkjOPQIBBggqhkjOPQMBBwNCAATMTqtl
sTSMCpurPNQuGcHtBsABrQ0MaUYdfpjYdmtNurcXeulLKNdpw2xW4kWpiz1U3jbT
Zz8tjKlfGU2oXKhLo1MwUTAdBgNVHQ4EFgQUOZQ77kfIvJl1Knt56WRx2arzPWkw
HwYDVR0jBBgwFoAUOZQ77kfIvJl1Knt56WRx2arzPWkwDwYDVR0TAQH/BAUwAwEB
/zAKBggqhkjOPQQDAgNIADBFAiEA4Fvyhs8Ox8AoSwY0ge4A1mPoUYtuLKkWlgJR
Ex4IOFkCIGDKGwatwgjiGgdMWhGZOjV93rzz+X+GJspQB4RSMF4Y
-----END CERTIFICATE-----`)
	keyPEM := []byte(`-----BEGIN PRIVATE KEY-----
MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgcjp/p+P6QLxYHsEN
ddDANYAQVOlkowOJs8AlM4+de9ShRANCAATMTqtlsTSMCpurPNQuGcHtBsABrQ0M
aUYdfpjYdmtNurcXeulLKNdpw2xW4kWpiz1U3jbTZz8tjKlfGU2oXKhL
-----END PRIVATE KEY-----`)
	return certPEM, keyPEM
}

// newTestTellerProvider creates a TellerProvider pointing at a test server.
// Uses a plain HTTP client (no mTLS) since httptest doesn't need it.
func newTestTellerProvider(baseURL string) *TellerProvider {
	return &TellerProvider{
		apiKey:     "test-api-key",
		baseURL:    baseURL,
		httpClient: &http.Client{},
	}
}

// --- Name ---

func TestTellerProvider_Name(t *testing.T) {
	tp := newTestTellerProvider("http://localhost")
	if tp.Name() != "teller" {
		t.Errorf("expected 'teller', got '%s'", tp.Name())
	}
}

// --- NewTellerProvider ---

func TestNewTellerProvider_ValidCert(t *testing.T) {
	certPEM, keyPEM := testCertAndKey()
	_, err := NewTellerProvider(TellerConfig{
		APIKey:  "test-key",
		CertPEM: certPEM,
		KeyPEM:  keyPEM,
		BaseURL: "https://api.teller.io",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestNewTellerProvider_InvalidCert(t *testing.T) {
	_, err := NewTellerProvider(TellerConfig{
		APIKey:  "test-key",
		CertPEM: []byte("not a cert"),
		KeyPEM:  []byte("not a key"),
	})
	if err == nil {
		t.Fatal("expected error for invalid cert")
	}
}

func TestNewTellerProvider_DefaultBaseURL(t *testing.T) {
	certPEM, keyPEM := testCertAndKey()
	tp, err := NewTellerProvider(TellerConfig{
		APIKey:  "test-key",
		CertPEM: certPEM,
		KeyPEM:  keyPEM,
		BaseURL: "", // should default
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tp.baseURL != "https://api.teller.io" {
		t.Errorf("expected default base URL, got %s", tp.baseURL)
	}
}

// --- StartEnrollment ---

func TestTellerProvider_StartEnrollment(t *testing.T) {
	tp := newTestTellerProvider("http://localhost")
	session, err := tp.StartEnrollment("user-123", "chase")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if session.Provider != "teller" {
		t.Errorf("expected provider 'teller', got '%s'", session.Provider)
	}
	if session.SessionURL == "" {
		t.Error("expected non-empty session URL")
	}
	if session.SessionID == "" {
		t.Error("expected non-empty session ID")
	}
}

// --- ExchangeToken ---

func TestTellerProvider_ExchangeToken(t *testing.T) {
	tp := newTestTellerProvider("http://localhost")
	cred, err := tp.ExchangeToken("enrollment-token-abc")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cred.ProviderName != "teller" {
		t.Errorf("expected 'teller', got '%s'", cred.ProviderName)
	}
	if cred.AccessToken != "enrollment-token-abc" {
		t.Errorf("expected access token to equal enrollment token, got '%s'", cred.AccessToken)
	}
	if cred.EnrolledAt == "" {
		t.Error("expected non-empty enrolled_at")
	}
}

// --- GetAccounts ---

func TestTellerProvider_GetAccounts_Success(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/accounts" {
			t.Errorf("expected /accounts, got %s", r.URL.Path)
		}
		if r.Method != "GET" {
			t.Errorf("expected GET, got %s", r.Method)
		}
		// Verify basic auth
		user, _, ok := r.BasicAuth()
		if !ok || user != "test-access-token" {
			t.Errorf("expected basic auth with test-access-token, got %s", user)
		}
		accounts := []tellerAccount{
			{
				ID:       "acc_001",
				Name:     "Checking",
				Type:     "depository",
				Subtype:  "checking",
				Currency: "USD",
				LastFour: "1234",
				Institution: struct {
					ID   string `json:"id"`
					Name string `json:"name"`
				}{ID: "chase", Name: "Chase"},
			},
			{
				ID:       "acc_002",
				Name:     "Savings",
				Type:     "depository",
				Subtype:  "savings",
				Currency: "USD",
				LastFour: "5678",
				Institution: struct {
					ID   string `json:"id"`
					Name string `json:"name"`
				}{ID: "chase", Name: "Chase"},
			},
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(accounts)
	}))
	defer server.Close()

	tp := newTestTellerProvider(server.URL)
	accounts, err := tp.GetAccounts(AccessCredential{AccessToken: "test-access-token"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(accounts) != 2 {
		t.Fatalf("expected 2 accounts, got %d", len(accounts))
	}
	if accounts[0].ID != "acc_001" {
		t.Errorf("expected acc_001, got %s", accounts[0].ID)
	}
	if accounts[0].InstitutionID != "chase" {
		t.Errorf("expected institution_id=chase, got %s", accounts[0].InstitutionID)
	}
	if accounts[0].Type != "depository" {
		t.Errorf("expected type=depository, got %s", accounts[0].Type)
	}
	if accounts[1].Name != "Savings" {
		t.Errorf("expected Savings, got %s", accounts[1].Name)
	}
}

func TestTellerProvider_GetAccounts_ServerError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(`{"error":"internal server error"}`))
	}))
	defer server.Close()

	tp := newTestTellerProvider(server.URL)
	_, err := tp.GetAccounts(AccessCredential{AccessToken: "tok"})
	if err == nil {
		t.Fatal("expected error for 500 response")
	}
}

func TestTellerProvider_GetAccounts_InvalidJSON(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`not json`))
	}))
	defer server.Close()

	tp := newTestTellerProvider(server.URL)
	_, err := tp.GetAccounts(AccessCredential{AccessToken: "tok"})
	if err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}

func TestTellerProvider_GetAccounts_ConnectionError(t *testing.T) {
	tp := newTestTellerProvider("http://127.0.0.1:1") // bad port
	_, err := tp.GetAccounts(AccessCredential{AccessToken: "tok"})
	if err == nil {
		t.Fatal("expected error for connection failure")
	}
}

// --- SyncTransactions ---

func TestTellerProvider_SyncTransactions_Success(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			t.Errorf("expected GET, got %s", r.Method)
		}
		txns := []tellerTransaction{
			{
				ID:          "txn_001",
				AccountID:   "acc_001",
				Amount:      42.50,
				Date:        "2025-01-15",
				Description: "Coffee Shop",
				Status:      "posted",
				Details: struct {
					Category     string `json:"category"`
					Counterparty struct {
						Name string `json:"name"`
					} `json:"counterparty"`
				}{
					Category: "food",
					Counterparty: struct {
						Name string `json:"name"`
					}{Name: "Starbucks"},
				},
			},
			{
				ID:          "txn_002",
				AccountID:   "acc_001",
				Amount:      15.00,
				Date:        "2025-01-16",
				Description: "Uber Ride",
				Status:      "pending",
				Details: struct {
					Category     string `json:"category"`
					Counterparty struct {
						Name string `json:"name"`
					} `json:"counterparty"`
				}{
					Category: "transportation",
					Counterparty: struct {
						Name string `json:"name"`
					}{Name: "Uber"},
				},
			},
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(txns)
	}))
	defer server.Close()

	tp := newTestTellerProvider(server.URL)
	sync, err := tp.SyncTransactions(AccessCredential{
		AccessToken:   "tok",
		InstitutionID: "acc_001",
	}, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(sync.Added) != 2 {
		t.Fatalf("expected 2 transactions, got %d", len(sync.Added))
	}
	if sync.Added[0].ID != "txn_001" {
		t.Errorf("expected txn_001, got %s", sync.Added[0].ID)
	}
	if sync.Added[0].MerchantName != "Starbucks" {
		t.Errorf("expected Starbucks, got %s", sync.Added[0].MerchantName)
	}
	if sync.Added[0].Pending != false {
		t.Error("expected posted transaction to not be pending")
	}
	if sync.Added[1].Pending != true {
		t.Error("expected pending transaction to be pending")
	}
	if sync.Cursor == "" {
		t.Error("expected non-empty cursor")
	}
}

func TestTellerProvider_SyncTransactions_WithCursor(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fromID := r.URL.Query().Get("from_id")
		if fromID != "cursor_abc" {
			t.Errorf("expected from_id=cursor_abc, got %s", fromID)
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode([]tellerTransaction{})
	}))
	defer server.Close()

	tp := newTestTellerProvider(server.URL)
	sync, err := tp.SyncTransactions(AccessCredential{
		AccessToken:   "tok",
		InstitutionID: "acc_001",
	}, "cursor_abc")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(sync.Added) != 0 {
		t.Errorf("expected 0 transactions, got %d", len(sync.Added))
	}
}

func TestTellerProvider_SyncTransactions_ServerError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		w.Write([]byte(`{"error":"forbidden"}`))
	}))
	defer server.Close()

	tp := newTestTellerProvider(server.URL)
	_, err := tp.SyncTransactions(AccessCredential{
		AccessToken:   "tok",
		InstitutionID: "acc_001",
	}, "")
	if err == nil {
		t.Fatal("expected error for 403 response")
	}
}

func TestTellerProvider_SyncTransactions_InvalidJSON(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{broken`))
	}))
	defer server.Close()

	tp := newTestTellerProvider(server.URL)
	_, err := tp.SyncTransactions(AccessCredential{
		AccessToken:   "tok",
		InstitutionID: "acc_001",
	}, "")
	if err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}

func TestTellerProvider_SyncTransactions_ConnectionError(t *testing.T) {
	tp := newTestTellerProvider("http://127.0.0.1:1")
	_, err := tp.SyncTransactions(AccessCredential{
		AccessToken:   "tok",
		InstitutionID: "acc_001",
	}, "")
	if err == nil {
		t.Fatal("expected error for connection failure")
	}
}

// --- GetInstitutions ---

func TestTellerProvider_GetInstitutions_Success(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/institutions" {
			t.Errorf("expected /institutions, got %s", r.URL.Path)
		}
		user, _, ok := r.BasicAuth()
		if !ok || user != "test-api-key" {
			t.Errorf("expected API key auth, got %s", user)
		}
		insts := []tellerInstitution{
			{ID: "chase", Name: "Chase"},
			{ID: "bofa", Name: "Bank of America"},
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(insts)
	}))
	defer server.Close()

	tp := newTestTellerProvider(server.URL)
	institutions, err := tp.GetInstitutions()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(institutions) != 2 {
		t.Fatalf("expected 2 institutions, got %d", len(institutions))
	}
	if institutions[0].ID != "chase" {
		t.Errorf("expected chase, got %s", institutions[0].ID)
	}
	if institutions[1].Name != "Bank of America" {
		t.Errorf("expected Bank of America, got %s", institutions[1].Name)
	}
}

func TestTellerProvider_GetInstitutions_ServerError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		w.Write([]byte(`service down`))
	}))
	defer server.Close()

	tp := newTestTellerProvider(server.URL)
	_, err := tp.GetInstitutions()
	if err == nil {
		t.Fatal("expected error for 503 response")
	}
}

func TestTellerProvider_GetInstitutions_InvalidJSON(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`not json at all`))
	}))
	defer server.Close()

	tp := newTestTellerProvider(server.URL)
	_, err := tp.GetInstitutions()
	if err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}

func TestTellerProvider_GetInstitutions_ConnectionError(t *testing.T) {
	tp := newTestTellerProvider("http://127.0.0.1:1")
	_, err := tp.GetInstitutions()
	if err == nil {
		t.Fatal("expected error for connection failure")
	}
}

// --- GetAccounts empty ---

func TestTellerProvider_GetAccounts_Empty(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode([]tellerAccount{})
	}))
	defer server.Close()

	tp := newTestTellerProvider(server.URL)
	accounts, err := tp.GetAccounts(AccessCredential{AccessToken: "tok"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(accounts) != 0 {
		t.Errorf("expected 0 accounts, got %d", len(accounts))
	}
}
