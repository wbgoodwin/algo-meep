package provider

// BankProvider defines the interface for bank data aggregation providers.
// Implementations: Teller (MVP), FDX (future).
type BankProvider interface {
	// Name returns the provider identifier ("teller", "fdx").
	Name() string

	// StartEnrollment initiates a bank connection flow.
	// Returns an enrollment session URL/token for the client to present to the user.
	StartEnrollment(userID string, institutionID string) (*EnrollmentSession, error)

	// ExchangeToken takes the enrollment result and returns an access credential.
	ExchangeToken(enrollmentToken string) (*AccessCredential, error)

	// GetAccounts returns all accounts for the given credential.
	GetAccounts(credential AccessCredential) ([]Account, error)

	// SyncTransactions returns transactions since the given cursor.
	// Returns new transactions and an updated cursor for incremental sync.
	SyncTransactions(credential AccessCredential, cursor string) (*TransactionSync, error)

	// GetInstitutions returns institutions supported by this provider.
	GetInstitutions() ([]Institution, error)
}

// Registry holds the available bank providers keyed by name.
type Registry struct {
	providers map[string]BankProvider
}

// NewRegistry creates a provider registry.
func NewRegistry() *Registry {
	return &Registry{providers: make(map[string]BankProvider)}
}

// Register adds a provider to the registry.
func (r *Registry) Register(p BankProvider) {
	r.providers[p.Name()] = p
}

// Get returns a provider by name or nil if not found.
func (r *Registry) Get(name string) BankProvider {
	return r.providers[name]
}

// List returns all registered provider names.
func (r *Registry) List() []string {
	names := make([]string, 0, len(r.providers))
	for name := range r.providers {
		names = append(names, name)
	}
	return names
}

// SelectProvider picks the best provider for a given institution.
// Strategy: prefer FDX (free) over Teller when both support the institution.
func (r *Registry) SelectProvider(institutionID string) BankProvider {
	// Try FDX first (free, bank-funded)
	if fdx, ok := r.providers["fdx"]; ok {
		institutions, err := fdx.GetInstitutions()
		if err == nil {
			for _, inst := range institutions {
				if inst.ID == institutionID {
					return fdx
				}
			}
		}
	}
	// Fall back to Teller
	if teller, ok := r.providers["teller"]; ok {
		return teller
	}
	return nil
}
