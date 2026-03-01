# AlgoFlow — Technical Requirements Document

## 1. Architecture Overview

```
┌──────────────────────────────────────────────────────────────────────┐
│                         CLIENT (User's Device)                       │
│                                                                      │
│  ┌────────────────────────────────────────────────────────────────┐  │
│  │  Tauri 2 Desktop App                                          │  │
│  │  ┌──────────────┐  ┌──────────────┐  ┌─────────────────────┐  │  │
│  │  │ React + TS   │  │  Rust Core   │  │  SQLite (encrypted) │  │  │
│  │  │ Frontend     │←→│  Commands    │←→│  Local Database     │  │  │
│  │  └──────────────┘  └──────┬───────┘  └─────────────────────┘  │  │
│  └───────────────────────────┼────────────────────────────────────┘  │
│                              │ HTTPS                                 │
└──────────────────────────────┼───────────────────────────────────────┘
                               │
┌──────────────────────────────┼───────────────────────────────────────┐
│                         AWS CLOUD                                    │
│                              │                                       │
│  ┌───────────────────────────▼───────────────────────────────────┐  │
│  │  API Gateway (HTTP API)                                       │  │
│  │  JWT Authorizer (Cognito)                                     │  │
│  └───────────┬──────────────┬──────────────┬─────────────────────┘  │
│              │              │              │                         │
│  ┌───────────▼──┐ ┌────────▼───┐ ┌────────▼──────┐                 │
│  │  Go Lambda   │ │ Go Lambda  │ │  Go Lambda    │                 │
│  │  Bank Proxy  │ │ Sync Svc   │ │  User Mgmt    │                 │
│  └───────┬──────┘ └─────┬──────┘ └───────┬───────┘                 │
│          │              │                │                          │
│  ┌───────▼──────┐ ┌─────▼──────┐ ┌──────▼───────┐                 │
│  │  Teller API  │ │ DynamoDB   │ │  Cognito     │                 │
│  │  / FDX APIs  │ │ Sync Store │ │  User Pool   │                 │
│  └──────────────┘ └────────────┘ └──────────────┘                 │
│                                                                     │
│  ┌──────────────────────────────────────────────────────────────┐   │
│  │  Existing: Market Data Collector (Go Lambda + S3 + Parquet)  │   │
│  └──────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────┘
```

### Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Backend language | **Go** | Already used for market data collector. Fast cold starts on Lambda. Single binary deploys. Strong stdlib for HTTP/crypto. |
| Compute | **Lambda (ARM64)** | Pay-per-request. Zero cost at zero traffic. Go cold starts < 50ms. Already proven in existing pipeline. |
| API layer | **API Gateway HTTP API** | ~70% cheaper than REST API. Native JWT auth. Sufficient for this use case. |
| Auth | **Cognito** | Cheapest managed auth. 50k MAU free tier. JWT tokens work natively with API Gateway. |
| Database | **DynamoDB** | On-demand pricing = pay-per-request. No idle costs. Scales to zero. 25GB free tier. |
| IaC | **AWS CDK (Python)** | Already in use. Extend existing pipeline. |
| Secrets | **SSM Parameter Store** | Free for standard parameters. Already used for Alpaca keys. |
| Bank aggregation | **Teller (MVP) + FDX (future)** | Teller: free for first 1,000 enrollments, direct bank APIs. FDX: free, bank-funded, growing coverage under CFPB Section 1033. |
| Client DB | **SQLite (encrypted)** | Already in use via Tauri/rusqlite. Add sqlcipher for encryption at rest. |
| CI/CD | **CodePipeline** | Already configured. Extend with new Lambda targets. |

---

## 2. Technology Stack

### 2.1 Client Application

| Component | Technology | Version |
|-----------|-----------|---------|
| Framework | Tauri | 2.x |
| Core language | Rust | 2021 edition |
| Frontend | React + TypeScript | 18.x / 5.x |
| Styling | Tailwind CSS | 3.x |
| Charts | Recharts | 2.x |
| Icons | Lucide React | latest |
| UI primitives | Radix UI | latest |
| Local database | rusqlite (bundled) | 0.31 |
| HTTP client | reqwest (rustls-tls) | 0.12 |

### 2.2 Backend Services (Go)

| Component | Technology | Notes |
|-----------|-----------|-------|
| Language | Go | 1.25+ (match existing module) |
| HTTP framework | stdlib `net/http` | Lambda adapter via `aws-lambda-go`. No framework needed — keep deps minimal. |
| AWS SDK | `aws-sdk-go-v2` | Already in use |
| Teller client | `net/http` + custom client | Teller has a simple REST API — no SDK needed, just HTTP calls with cert-based auth |
| FDX client | `net/http` + custom client | FDX is a REST standard — implement per-bank as coverage grows |
| JSON | `encoding/json` (stdlib) | Zero deps |
| Crypto | `golang.org/x/crypto` | For client-side encryption key derivation |
| Testing | stdlib `testing` + `testify` | Already a dependency |

### 2.3 AWS Infrastructure

| Service | Purpose | Cost Model |
|---------|---------|------------|
| **Lambda (ARM64)** | All backend compute | $0.0000133/GB-s. First 1M requests free/month. |
| **API Gateway HTTP API** | Request routing + auth | $1.00/million requests. First 1M free for 12 months. |
| **Cognito** | User auth + JWT | Free up to 50k MAU (with some caveats on advanced features) |
| **DynamoDB (on-demand)** | Sync data, user metadata, billing | $1.25/million writes, $0.25/million reads. 25GB free. |
| **S3** | Encrypted sync blobs, market data (existing) | $0.023/GB/month. Existing bucket reuse. |
| **SSM Parameter Store** | Secrets (Teller certs, FDX keys, config) | Free (standard tier) |
| **KMS** | Encryption key management | $1/key/month + $0.03/10k requests |
| **CloudWatch** | Logging + metrics | First 5GB ingest free |
| **CodePipeline** | CI/CD | $1/pipeline/month (existing) |
| **SES** | Transactional email (verification, alerts) | $0.10/1000 emails |

#### Estimated Monthly Cost at Scale

| Users | Lambda | API GW | DynamoDB | S3 | Cognito | Total |
|-------|--------|--------|----------|-----|---------|-------|
| 100 | ~$0 | ~$0 | ~$0 | ~$0.10 | $0 | **< $1** |
| 1,000 | ~$0.50 | ~$0.50 | ~$2 | ~$1 | $0 | **~$5** |
| 10,000 | ~$5 | ~$5 | ~$20 | ~$10 | $0 | **~$40** |
| 50,000 | ~$25 | ~$25 | ~$100 | ~$50 | $0 | **~$200** |

*Plus Teller costs: free for first 1,000 enrollments, then usage-based (significantly cheaper than Plaid). FDX connections are free (bank-funded). As FDX coverage grows, per-user costs approach zero.*

---

## 3. Backend Service Design

### 3.1 Go Project Structure

```
backend/
├── cmd/
│   ├── api/                    # Main API Lambda handler
│   │   └── main.go
│   └── sync-worker/            # Async sync processing Lambda
│       └── main.go
├── internal/
│   ├── auth/                   # Cognito JWT validation, user context
│   │   └── auth.go
│   ├── provider/               # Bank provider abstraction
│   │   ├── provider.go         # BankProvider interface
│   │   ├── teller.go           # Teller API implementation (MVP)
│   │   ├── fdx.go              # FDX standard implementation (future)
│   │   └── types.go            # Shared account/transaction types
│   ├── sync/                   # Encrypted sync logic
│   │   ├── encrypt.go          # Client-side encryption helpers
│   │   ├── store.go            # DynamoDB sync operations
│   │   └── handler.go
│   ├── billing/                # Usage tracking + cost passthrough
│   │   ├── usage.go
│   │   └── handler.go
│   ├── user/                   # User profile, preferences
│   │   └── handler.go
│   └── middleware/              # Logging, error handling, rate limiting
│       ├── logging.go
│       └── ratelimit.go
├── pkg/
│   └── api/                    # Shared request/response types
│       ├── types.go
│       └── errors.go
├── go.mod
├── go.sum
├── Makefile
└── README.md
```

### 3.2 API Routes

All routes require JWT authorization header unless marked public.

```
POST   /auth/register              # Public — Cognito sign-up
POST   /auth/login                 # Public — Cognito sign-in
POST   /auth/refresh               # Public — Token refresh
DELETE /auth/account               # Delete account + all data

POST   /bank/enroll                # Start bank enrollment (Teller Connect or FDX OAuth)
POST   /bank/exchange-token        # Exchange enrollment token → access token (encrypted, stored on device)
POST   /bank/sync-transactions     # Proxy transaction sync from Teller/FDX
GET    /bank/accounts              # Proxy account list from provider
GET    /bank/providers             # List available providers + supported institutions

POST   /sync/push                  # Upload encrypted sync blob
GET    /sync/pull                  # Download latest sync blob
GET    /sync/status                # Last sync timestamp per device

GET    /user/profile               # User preferences, settings
PUT    /user/profile               # Update preferences
GET    /user/usage                 # Current billing period usage + cost breakdown

GET    /health                     # Public — health check
```

### 3.3 Lambda Handler Pattern

Single binary, route dispatch via API Gateway path:

```go
// cmd/api/main.go
package main

import (
    "context"
    "github.com/aws/aws-lambda-go/events"
    "github.com/aws/aws-lambda-go/lambda"
)

func handler(ctx context.Context, req events.APIGatewayV2HTTPRequest) (events.APIGatewayV2HTTPResponse, error) {
    // Route based on req.RouteKey e.g. "POST /bank/enroll"
    // Each route calls into internal/ packages
    // Shared middleware: logging, error wrapping, rate limiting
}

func main() {
    lambda.Start(handler)
}
```

**Why single Lambda?** Cost. One function = one cold start pool = better warm hit rate. At scale (>10k users), split into per-domain Lambdas (bank, sync, user) for independent scaling.

### 3.4 BankProvider Interface

The core abstraction that allows swapping/adding bank data providers without changing the rest of the backend:

```go
// internal/provider/provider.go
package provider

type BankProvider interface {
    // Name returns the provider identifier ("teller", "fdx")
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

type EnrollmentSession struct {
    SessionURL string  // URL to open in webview (Teller Connect / FDX OAuth)
    SessionID  string  // Server-side session tracking
    Provider   string  // "teller" or "fdx"
}

type AccessCredential struct {
    ProviderName  string // "teller" or "fdx"
    AccessToken   string // Encrypted — only Lambda decrypts via KMS
    InstitutionID string
    EnrolledAt    string // ISO 8601
}

type TransactionSync struct {
    Added   []Transaction
    Updated []Transaction
    Removed []string // Transaction IDs
    Cursor  string   // For next incremental sync
}
```

**Teller implementation** (`teller.go`): Uses Teller's REST API with mutual TLS (client certificate). Teller Connect handles the user-facing enrollment UI. Covers ~5,000 US institutions.

**FDX implementation** (`fdx.go`): Implements the FDX 6.0 REST standard. Each bank that supports FDX gets registered as an endpoint. Uses OAuth 2.0 + PKCE. Coverage growing under CFPB Section 1033 mandate (major banks required by 2026–2027).

**Provider selection logic**: When a user searches for their bank, the backend checks FDX availability first (free), then falls back to Teller. This minimizes cost automatically as FDX coverage expands.

---

## 4. Security Architecture

### 4.1 Authentication Flow

```
Client                    API Gateway               Cognito
  │                           │                        │
  │  POST /auth/register      │                        │
  │──────────────────────────►│───────────────────────►│
  │                           │  Create user           │
  │◄──────────────────────────│◄───────────────────────│
  │  { id_token, access_token, refresh_token }         │
  │                           │                        │
  │  GET /bank/accounts       │                        │
  │  Authorization: Bearer <access_token>              │
  │──────────────────────────►│                        │
  │                           │  JWT Authorizer        │
  │                           │  validates token       │
  │                           │  extracts user_id      │
  │                           │──► Go Lambda           │
  │◄──────────────────────────│◄── response            │
```

- **API Gateway JWT Authorizer** validates tokens at the edge — Lambda code never sees invalid requests
- `user_id` extracted from JWT `sub` claim, passed to handler via request context
- Refresh tokens stored securely on device (Tauri secure storage / Keychain)

### 4.2 Bank Provider Credential Security

**Current problem**: Plaid `client_id` and `secret` are in the client `.env` file. This is being replaced with Teller + FDX, and credentials **must live server-side**.

**Target architecture**:
1. Teller application certificate + API key stored in **SSM Parameter Store** (SecureString, encrypted by KMS)
2. FDX OAuth client credentials stored similarly per-bank as integrations are added
3. Client never sees provider credentials
4. Client calls `POST /bank/enroll` → Go Lambda uses provider credentials → initiates Teller Connect or FDX OAuth → returns enrollment session to client
5. After enrollment completes, client sends enrollment token to `POST /bank/exchange-token` → Lambda exchanges it with provider → returns encrypted `access_token` to client for local storage
6. Provider `access_token` is **encrypted client-side** before storage (see §4.3) and re-encrypted when syncing

**Teller-specific**: Teller uses mutual TLS (client certificate auth). The certificate is stored in SSM and loaded by the Lambda at startup. This is more secure than API key auth — the cert never leaves the server.

**FDX-specific**: FDX uses standard OAuth 2.0 with PKCE. The client initiates the OAuth flow in a browser, and the callback is handled by the Go backend which exchanges the auth code for tokens.

### 4.3 Data Encryption

#### On-Device (SQLite)

| Layer | Method | Details |
|-------|--------|---------|
| Database encryption | SQLCipher (via `rusqlite` feature) | AES-256-CBC, full database encryption at rest |
| Key derivation | PBKDF2 or Argon2id | Derived from user password or device-specific key |
| Key storage | OS Keychain (macOS Keychain, Windows DPAPI) | Tauri plugin `tauri-plugin-stronghold` or native keychain |

#### In Transit
- All API calls over **TLS 1.3** (enforced by API Gateway)
- Certificate pinning optional but recommended for bank proxy calls

#### Cloud Sync (Double Encryption)

```
Client Device                          AWS
    │                                    │
    │  1. SQLite → JSON export           │
    │  2. Filter to last 30 days         │
    │  3. Encrypt with user key          │
    │     (AES-256-GCM, key from        │
    │      user password via Argon2id)   │
    │  4. POST /sync/push                │
    │     { encrypted_blob, device_id,   │
    │       key_check_hash }             │
    │──────────────────────────────────►│
    │                                    │  5. Server encrypts again
    │                                    │     (S3 SSE-KMS)
    │                                    │  6. Store in S3:
    │                                    │     s3://sync/{user_id}/{device_id}/
    │                                    │     latest.enc
    │                                    │
    │  GET /sync/pull                    │
    │◄──────────────────────────────────│  7. Return encrypted blob
    │  8. Decrypt with user key          │
    │  9. Merge into local SQLite        │
    │                                    │
```

**Key properties**:
- Server **cannot** read user financial data (client-side encryption with user-held key)
- If AWS is breached, attacker gets double-encrypted blobs — useless without user passwords
- Sync only the delta since last sync or last 30 days (whichever is smaller) to minimize blob size
- `key_check_hash` = hash of a known constant encrypted with user key — allows client to verify correct password before attempting full decryption

### 4.4 Provider Access Token Storage

```
Provider access_token flow:
1. Lambda receives access_token from Teller/FDX provider
2. Lambda encrypts with user-specific KMS data key
3. Returns encrypted_access_token to client
4. Client stores encrypted_access_token in local SQLite
5. When syncing, client sends encrypted_access_token to Lambda
6. Lambda decrypts with KMS → calls provider API → returns data
```

The client **never has the plaintext provider access token**. This means:
- If the device is compromised, the attacker can't call Teller/FDX directly
- Access token rotation is managed server-side
- Revocation is instant (delete the KMS key alias)

---

## 5. Database Design

### 5.1 Local SQLite Schema (Client)

Extends the existing schema. New/modified tables in **bold**.

```sql
-- Existing tables (unchanged)
CREATE TABLE institutions ( ... );
CREATE TABLE credentials ( ... );  -- access_token now encrypted
CREATE TABLE accounts ( ... );
CREATE TABLE transactions ( ... );
CREATE TABLE categories ( ... );
CREATE TABLE recurring_transactions ( ... );
CREATE TABLE net_worth_snapshots ( ... );

-- New tables
CREATE TABLE budgets (
    id TEXT PRIMARY KEY,
    category TEXT NOT NULL,
    monthly_limit REAL NOT NULL,
    rollover INTEGER DEFAULT 0,        -- 0 = no rollover, 1 = rollover
    created_at TEXT DEFAULT (datetime('now')),
    updated_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE budget_periods (
    id TEXT PRIMARY KEY,
    budget_id TEXT NOT NULL REFERENCES budgets(id),
    year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    spent REAL DEFAULT 0,
    rollover_amount REAL DEFAULT 0,     -- carried from previous month
    UNIQUE(budget_id, year, month)
);

CREATE TABLE retirement_scenarios (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    retirement_age INTEGER NOT NULL,
    monthly_contribution REAL NOT NULL,
    growth_rate REAL NOT NULL,
    inflation_rate REAL NOT NULL,
    is_primary INTEGER DEFAULT 0,
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE goals (
    id TEXT PRIMARY KEY,
    type TEXT NOT NULL,                 -- 'retirement', 'emergency_fund', 'debt_payoff', 'custom'
    target_amount REAL,
    target_date TEXT,
    current_amount REAL DEFAULT 0,
    created_at TEXT DEFAULT (datetime('now')),
    updated_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE sync_metadata (
    id TEXT PRIMARY KEY,
    last_sync_at TEXT,
    last_sync_device TEXT,
    sync_version INTEGER DEFAULT 0
);

CREATE TABLE user_preferences (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
```

### 5.2 DynamoDB Tables (Cloud)

**Table: `algoflow-users`**

| Key | Attribute | Type | Notes |
|-----|-----------|------|-------|
| PK | `USER#{user_id}` | S | Cognito sub |
| SK | `PROFILE` | S | |
| | `email` | S | |
| | `created_at` | S | ISO 8601 |
| | `plan` | S | `free`, `premium` |
| | `connected_accounts` | N | Count for billing |
| | `provider_access_tokens` | M | Map of `{institution_id: {provider, encrypted_token}}` |

**Table: `algoflow-sync`**

| Key | Attribute | Type | Notes |
|-----|-----------|------|-------|
| PK | `USER#{user_id}` | S | |
| SK | `DEVICE#{device_id}` | S | |
| | `s3_key` | S | Path to encrypted blob in S3 |
| | `sync_version` | N | Monotonic counter |
| | `synced_at` | S | ISO 8601 |
| | `blob_size_bytes` | N | For usage tracking |

**Table: `algoflow-usage`**

| Key | Attribute | Type | Notes |
|-----|-----------|------|-------|
| PK | `USER#{user_id}` | S | |
| SK | `MONTH#{YYYY-MM}` | S | |
| | `provider_syncs` | N | Number of Teller/FDX API calls |
| | `sync_storage_bytes` | N | Total S3 usage |
| | `api_calls` | N | Total API calls |
| | `estimated_cost` | N | Running cost in cents |

**Single-table design** is an option but separate tables are clearer for this scale and easier to reason about for billing/deletion.

---

## 6. Client-Side Changes

### 6.1 Migrate Bank Calls to Backend

Replace direct Plaid API calls in Rust with calls to Go backend via provider abstraction.

**Current** (`src-tauri/src/plaid.rs`): Client calls Plaid directly with embedded secrets.

**Target**: Client calls AlgoFlow API → Go Lambda calls Teller/FDX via `BankProvider` interface.

```
// Current Tauri commands to modify:
create_link_token       → POST /bank/enroll
exchange_token_and_sync → POST /bank/exchange-token + POST /bank/sync-transactions
sync_all_accounts       → POST /bank/sync-transactions (for each credential)
```

The Rust `PlaidClient` struct becomes an `ApiClient` struct that talks to the Go backend:

```rust
pub struct ApiClient {
    base_url: String,
    http: reqwest::Client,
    // JWT tokens managed here
    access_token: Option<String>,
    refresh_token: Option<String>,
}
```

### 6.2 Add SQLCipher Encryption

```toml
# Cargo.toml change
rusqlite = { version = "0.31", features = ["bundled-sqlcipher"] }
```

On first launch or login, derive encryption key and open DB with:
```rust
conn.execute_batch("PRAGMA key = 'derived_key_here';")?;
```

### 6.3 Auth Flow in Client

```
App Launch
    │
    ├─ Has stored refresh_token?
    │   ├─ Yes → Call /auth/refresh → Get new access_token → Continue
    │   └─ No  → Show login/register screen
    │
    ├─ User registers → POST /auth/register → Store tokens → Continue
    └─ User logs in   → POST /auth/login    → Store tokens → Continue
```

Tokens stored in OS Keychain via Tauri secure storage (not in SQLite).

---

## 7. Sync Protocol

### 7.1 Delta Sync Strategy

To keep sync lightweight (per product requirements):

1. **On sync push**: Export transactions + accounts modified since `last_sync_at` (or last 30 days max)
2. **Format**: JSON → gzip → AES-256-GCM encrypt → upload
3. **On sync pull**: Download → decrypt → decompress → merge
4. **Conflict resolution**: Last-write-wins per record (using `updated_at` timestamp)
5. **Sync version**: Monotonic counter per device. Server rejects pushes with stale version (optimistic locking).

### 7.2 Sync Blob Size Estimates

| Records | JSON | Gzipped | Encrypted |
|---------|------|---------|-----------|
| 100 transactions | ~50 KB | ~8 KB | ~8.1 KB |
| 500 transactions | ~250 KB | ~35 KB | ~35.1 KB |
| 1000 transactions (30 days heavy use) | ~500 KB | ~65 KB | ~65.1 KB |

Well within DynamoDB item limits (store blob in S3, pointer in DynamoDB).

---

## 8. Infrastructure as Code

### 8.1 New CDK Stacks

Extend the existing `infrastructure/` directory:

```
infrastructure/
├── stacks/
│   ├── market_data_collector_stack.py   # Existing
│   ├── api_stack.py                     # NEW — API Gateway + Lambda
│   ├── auth_stack.py                    # NEW — Cognito User Pool
│   ├── data_stack.py                    # NEW — DynamoDB tables + S3 sync bucket
│   └── monitoring_stack.py              # NEW — CloudWatch dashboards + alarms
├── app.py                               # Extend with new stages
└── constants.py
```

### 8.2 CI/CD Pipeline Extension

```
CodePipeline
    │
    ├── Source (GitHub: main branch)
    │
    ├── Build
    │   ├── Go build: market_data_collector (existing)
    │   ├── Go build: backend/cmd/api → bootstrap
    │   ├── Go build: backend/cmd/sync-worker → bootstrap
    │   └── CDK synth
    │
    ├── Deploy: Dev
    │   ├── Auth stack (Cognito)
    │   ├── Data stack (DynamoDB + S3)
    │   ├── API stack (API Gateway + Lambdas)
    │   ├── Market Data stack (existing)
    │   └── Integration tests
    │
    └── Deploy: Prod (manual approval gate)
        └── Same stacks, prod config
```

### 8.3 Environment Configuration

```python
# constants.py additions
ENVIRONMENTS = {
    "dev": {
        "teller_env": "sandbox",
        "cognito_domain_prefix": "algoflow-dev",
        "api_throttle_rate": 100,      # requests/sec
        "api_throttle_burst": 50,
    },
    "prod": {
        "teller_env": "production",
        "cognito_domain_prefix": "algoflow",
        "api_throttle_rate": 1000,
        "api_throttle_burst": 500,
    },
}
```

---

## 9. Billing & Cost Passthrough

### 9.1 Pricing Model

Per the product requirements: charge a small premium on actual costs, transparent breakdown.

| Tier | Price | Includes |
|------|-------|----------|
| **Free** | $0/month | 1 connected institution, manual refresh, no sync |
| **Standard** | $3.99/month | 3 institutions, auto-sync daily, cross-device sync |
| **Premium** | $7.99/month | Unlimited institutions, hourly sync, priority support |

### 9.2 Cost Breakdown Display

The app should show users what they're paying for:

```
Your Monthly Costs                    $3.99
├── Base subscription                 $1.50
├── Teller: 2 institutions            $0.50
├── Cloud sync (45 KB)               $0.01
└── Platform fee                      $0.98
```

### 9.3 Implementation

- Track usage in `algoflow-usage` DynamoDB table
- Lambda middleware increments counters per API call
- Monthly billing aggregation via scheduled Lambda (EventBridge cron)
- Payment processing: **Stripe** (handles App Store requirements, invoicing, PCI compliance)
- Apple takes 15–30% cut on App Store subscriptions — factor into pricing

---

## 10. Testing Strategy

### 10.1 Backend (Go)

| Type | Tool | Coverage Target |
|------|------|----------------|
| Unit tests | `testing` + `testify` | > 80% for `internal/` packages |
| Integration tests | `testing` + Docker (DynamoDB Local) | All API routes |
| Teller mock | Teller Sandbox environment | Full enrollment + sync flow |
| Load tests | `k6` or `vegeta` | Verify Lambda concurrency under 100 concurrent users |

### 10.2 Client (Tauri + React)

| Type | Tool | Scope |
|------|------|-------|
| React components | Vitest + React Testing Library | UI logic, hooks |
| Tauri commands | Rust `#[cfg(test)]` | DB operations, encryption |
| E2E | Playwright or WebDriverIO | Full user flows |

### 10.3 Security

| Test | Method |
|------|--------|
| Dependency audit | `govulncheck` (Go), `cargo audit` (Rust), `npm audit` (Node) |
| Secret scanning | GitHub secret scanning + pre-commit hooks |
| Penetration testing | OWASP ZAP against API Gateway endpoints |
| Encryption validation | Unit tests verifying encrypt → decrypt roundtrip, key derivation determinism |

---

## 11. Monitoring & Observability

### 11.1 CloudWatch Metrics

| Metric | Alarm Threshold |
|--------|----------------|
| Lambda errors (5xx) | > 1% of invocations |
| Lambda duration P99 | > 5s |
| API Gateway 4xx rate | > 10% (potential abuse) |
| DynamoDB throttled reads/writes | > 0 |
| Bank provider API errors | > 5% failure rate |

### 11.2 Structured Logging

Reuse the pattern from `market_data_collector/logger.go`:

```go
logger.Info("Transaction sync completed",
    WithUserID(userID),
    WithFunction("syncTransactions"),
    WithField("institution_count", len(institutions)),
    WithField("transaction_count", count),
    WithDuration(elapsed),
)
```

**Never log**: access tokens, financial data, PII. Log only IDs, counts, and durations.

### 11.3 Cost Monitoring

- AWS Budgets alert at $10, $50, $100/month
- Per-user cost tracking via usage table
- Monthly cost report Lambda (EventBridge → Lambda → SES email to admin)

---

## 12. Migration Plan

### Phase 1: Backend Foundation (Weeks 1–3)

| Task | Details |
|------|---------|
| Set up Go backend project | `backend/` directory, `go.mod`, Makefile |
| Cognito User Pool + CDK stack | Registration, login, JWT |
| API Gateway + single Lambda | Route dispatch, health check |
| Teller proxy endpoints | Implement `POST /bank/enroll` + `POST /bank/exchange-token` via Teller API |
| BankProvider interface | Define Go interface with Teller as first implementation |
| Modify Tauri client | Replace `PlaidClient` with `ApiClient`, add auth flow |
| DynamoDB tables + CDK stack | User table, basic CRUD |

### Phase 2: Sync & Security (Weeks 4–6)

| Task | Details |
|------|---------|
| SQLCipher integration | Encrypt local SQLite |
| Client-side encryption module | AES-256-GCM, key derivation |
| Sync push/pull endpoints | S3 blob storage, DynamoDB metadata |
| Sync merge logic in Rust | Delta export, conflict resolution |
| Provider access token encryption | KMS-based server-side encryption |

### Phase 3: Billing & Polish (Weeks 7–9)

| Task | Details |
|------|---------|
| Usage tracking middleware | Per-request counting |
| Stripe integration | Subscription management |
| Cost breakdown endpoint | Real-time usage → cost calculation |
| Monitoring stack | CloudWatch dashboards + alarms |
| Security audit | Dependency scan, penetration testing |

### Phase 4: App Store Preparation (Weeks 10–12)

| Task | Details |
|------|---------|
| macOS App Sandbox | Entitlements, code signing |
| Apple Developer Program | Certificates, provisioning |
| Notarization | Tauri build pipeline |
| Privacy policy + ToS | Legal review |
| Teller Production access | Application + compliance review |
| FDX registration | Register with FDX for production API access |
| App Store submission | Screenshots, description, review |

---

## 13. Repository Structure (Target)

```
algo-meep/
├── backend/                    # Go backend services (NEW)
│   ├── cmd/
│   ├── internal/
│   ├── pkg/
│   ├── go.mod
│   ├── go.sum
│   └── Makefile
├── market_data_collector/      # Existing Go Lambda
├── ui/                         # Existing Tauri + React app
│   ├── src/                    # React frontend
│   ├── src-tauri/              # Rust backend
│   └── ...
├── infrastructure/             # AWS CDK (Python)
│   ├── stacks/
│   │   ├── market_data_collector_stack.py
│   │   ├── api_stack.py        # NEW
│   │   ├── auth_stack.py       # NEW
│   │   ├── data_stack.py       # NEW
│   │   └── monitoring_stack.py # NEW
│   └── app.py
├── docs/                       # Documentation
│   ├── REQUIREMENTS.md
│   └── TECHNICAL_REQUIREMENTS.md
├── .github/                    # GitHub Actions (optional, supplement CodePipeline)
└── ...
```

---

## 14. Open Technical Decisions

| # | Question | Options | Recommendation |
|---|----------|---------|----------------|
| 1 | Single Lambda vs. multi-Lambda? | Single for MVP, split later | Single — cheaper, simpler, split at ~10k users |
| 2 | SQLCipher vs. application-level encryption? | SQLCipher = transparent; app-level = selective | SQLCipher — simpler, encrypts everything including indexes |
| 3 | Cognito vs. custom auth? | Cognito = managed; custom = flexible | Cognito — free tier is huge, JWT works natively with API GW |
| 4 | Stripe vs. Apple IAP only? | Stripe = web + desktop; IAP = required for iOS | Both — Stripe for desktop, IAP for future iOS. Share entitlements via backend. |
| 5 | Sync frequency limit? | Real-time vs. daily vs. manual | Free = manual, Standard = daily, Premium = hourly |
| 6 | DynamoDB single-table vs. multi-table? | Single = fewer tables; multi = simpler queries | Multi-table for now — clearer separation, easier to reason about, optimize later if needed |
