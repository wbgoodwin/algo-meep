package provider

import (
	"fmt"
	"testing"
)

// mockProvider implements BankProvider for testing.
type mockProvider struct {
	name         string
	institutions []Institution
}

func (m *mockProvider) Name() string { return m.name }

func (m *mockProvider) StartEnrollment(userID, institutionID string) (*EnrollmentSession, error) {
	return &EnrollmentSession{
		SessionURL: fmt.Sprintf("https://%s.test/connect?inst=%s", m.name, institutionID),
		SessionID:  fmt.Sprintf("%s_%s_session", m.name, userID),
		Provider:   m.name,
	}, nil
}

func (m *mockProvider) ExchangeToken(enrollmentToken string) (*AccessCredential, error) {
	return &AccessCredential{
		ProviderName:  m.name,
		AccessToken:   "test_access_token_" + enrollmentToken,
		InstitutionID: "inst_123",
		EnrolledAt:    "2025-01-01T00:00:00Z",
	}, nil
}

func (m *mockProvider) GetAccounts(credential AccessCredential) ([]Account, error) {
	return []Account{
		{
			ID:             "acc_001",
			InstitutionID:  "inst_123",
			Name:           "Checking",
			Type:           "depository",
			Subtype:        "checking",
			CurrentBalance: 1500.00,
			CurrencyCode:   "USD",
		},
	}, nil
}

func (m *mockProvider) SyncTransactions(credential AccessCredential, cursor string) (*TransactionSync, error) {
	return &TransactionSync{
		Added: []Transaction{
			{
				ID:          "txn_001",
				AccountID:   "acc_001",
				Amount:      -42.50,
				Date:        "2025-01-15",
				Description: "Coffee Shop",
			},
		},
		Cursor: "cursor_after_txn_001",
	}, nil
}

func (m *mockProvider) GetInstitutions() ([]Institution, error) {
	return m.institutions, nil
}

func TestRegistryRegisterAndGet(t *testing.T) {
	reg := NewRegistry()
	mock := &mockProvider{name: "teller", institutions: nil}

	reg.Register(mock)

	got := reg.Get("teller")
	if got == nil {
		t.Fatal("expected to get 'teller' provider")
	}
	if got.Name() != "teller" {
		t.Errorf("expected name 'teller', got '%s'", got.Name())
	}
}

func TestRegistryGetMissing(t *testing.T) {
	reg := NewRegistry()
	got := reg.Get("nonexistent")
	if got != nil {
		t.Error("expected nil for missing provider")
	}
}

func TestRegistryList(t *testing.T) {
	reg := NewRegistry()
	reg.Register(&mockProvider{name: "teller"})
	reg.Register(&mockProvider{name: "fdx"})

	names := reg.List()
	if len(names) != 2 {
		t.Fatalf("expected 2 providers, got %d", len(names))
	}

	nameSet := make(map[string]bool)
	for _, n := range names {
		nameSet[n] = true
	}
	if !nameSet["teller"] || !nameSet["fdx"] {
		t.Errorf("expected teller and fdx in list, got %v", names)
	}
}

func TestSelectProvider_PrefersFDX(t *testing.T) {
	reg := NewRegistry()

	teller := &mockProvider{
		name:         "teller",
		institutions: []Institution{{ID: "chase", Name: "Chase"}},
	}
	fdx := &mockProvider{
		name:         "fdx",
		institutions: []Institution{{ID: "chase", Name: "Chase"}},
	}

	reg.Register(teller)
	reg.Register(fdx)

	selected := reg.SelectProvider("chase")
	if selected == nil {
		t.Fatal("expected a provider to be selected")
	}
	if selected.Name() != "fdx" {
		t.Errorf("expected FDX to be preferred, got '%s'", selected.Name())
	}
}

func TestSelectProvider_FallsBackToTeller(t *testing.T) {
	reg := NewRegistry()

	teller := &mockProvider{
		name:         "teller",
		institutions: []Institution{{ID: "chase", Name: "Chase"}},
	}
	fdx := &mockProvider{
		name:         "fdx",
		institutions: []Institution{}, // FDX doesn't support Chase
	}

	reg.Register(teller)
	reg.Register(fdx)

	selected := reg.SelectProvider("chase")
	if selected == nil {
		t.Fatal("expected a provider to be selected")
	}
	if selected.Name() != "teller" {
		t.Errorf("expected Teller fallback, got '%s'", selected.Name())
	}
}

func TestSelectProvider_NoProviders(t *testing.T) {
	reg := NewRegistry()
	selected := reg.SelectProvider("chase")
	if selected != nil {
		t.Error("expected nil when no providers registered")
	}
}

func TestMockProvider_StartEnrollment(t *testing.T) {
	mock := &mockProvider{name: "teller"}
	session, err := mock.StartEnrollment("user_1", "chase")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if session.Provider != "teller" {
		t.Errorf("expected provider 'teller', got '%s'", session.Provider)
	}
	if session.SessionURL == "" {
		t.Error("expected non-empty session URL")
	}
}

func TestMockProvider_ExchangeToken(t *testing.T) {
	mock := &mockProvider{name: "teller"}
	cred, err := mock.ExchangeToken("enrollment_abc")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cred.ProviderName != "teller" {
		t.Errorf("expected provider 'teller', got '%s'", cred.ProviderName)
	}
	if cred.AccessToken == "" {
		t.Error("expected non-empty access token")
	}
}

func TestMockProvider_GetAccounts(t *testing.T) {
	mock := &mockProvider{name: "teller"}
	accounts, err := mock.GetAccounts(AccessCredential{AccessToken: "tok"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(accounts) != 1 {
		t.Fatalf("expected 1 account, got %d", len(accounts))
	}
	if accounts[0].Type != "depository" {
		t.Errorf("expected type 'depository', got '%s'", accounts[0].Type)
	}
}

func TestMockProvider_SyncTransactions(t *testing.T) {
	mock := &mockProvider{name: "teller"}
	sync, err := mock.SyncTransactions(AccessCredential{AccessToken: "tok"}, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(sync.Added) != 1 {
		t.Fatalf("expected 1 transaction, got %d", len(sync.Added))
	}
	if sync.Cursor == "" {
		t.Error("expected non-empty cursor")
	}
}
