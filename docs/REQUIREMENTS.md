# AlgoFlow — Product Requirements Document

## Vision

A personal finance app that gives users complete visibility into their money, automates budgeting decisions, and builds an actionable, optimized path to early retirement. All sensitive financial data stays on-device; cloud services handle authentication, sync, and intelligence that benefits from scale.

---

## 1. Core Principles

| Principle | Detail |
|-----------|--------|
| **Privacy-first** | Financial data (transactions, balances, credentials) stored locally on-device in encrypted SQLite. Cloud only holds anonymized analytics and user auth. |
| **Actionable over informational** | Every screen should answer "what should I do?" not just "what happened?" |
| **Speed to retirement** | The app's north star metric: reduce the user's projected retirement date. Every feature ties back to this. |
| **Automation** | Minimize manual input. Auto-categorize, auto-detect recurring bills, auto-suggest optimizations. |

---

## 2. User Personas

### 2.1 Early Career Professional (22–35)
- Has income but no clear savings strategy
- Wants to understand where money goes
- Needs guidance on how much to save and where

### 2.2 Mid-Career Optimizer (35–50)
- Has multiple accounts, investments, and possibly a mortgage
- Wants to maximize retirement contributions and minimize tax drag
- Needs scenario planning (e.g., "what if I max my 401k?")

### 2.3 Pre-Retiree (50–65)
- Focused on drawdown strategy and risk reduction
- Needs confidence that savings will last
- Wants to model Social Security timing, healthcare costs, etc.

---

## 3. Feature Requirements

### 3.1 Account Aggregation

| ID | Requirement | Priority |
|----|-------------|----------|
| AG-1 | Connect bank accounts, credit cards, and investment accounts via Teller (MVP) and FDX direct bank APIs (future) | P0 |
| AG-2 | Auto-sync transactions and balances on a configurable schedule | P0 |
| AG-3 | Support manual account entry for assets providers can't reach (property, crypto, etc.) | P1 |
| AG-4 | Display unified net worth across all accounts | P0 |
| AG-5 | Track net worth over time with historical snapshots | P0 |

### 3.2 Transaction Management

| ID | Requirement | Priority |
|----|-------------|----------|
| TX-1 | Auto-categorize transactions using provider categories + ML refinement | P0 |
| TX-2 | Allow user to re-categorize; learn from corrections | P1 |
| TX-3 | Flag unreviewed transactions for user attention | P0 |
| TX-4 | Detect and surface recurring transactions (subscriptions, bills, income) | P0 |
| TX-5 | Split transactions across categories | P2 |
| TX-6 | Search and filter transactions by date, amount, category, merchant | P1 |

### 3.3 Budgeting

| ID | Requirement | Priority |
|----|-------------|----------|
| BG-1 | Auto-generate a recommended budget based on income and spending history | P0 |
| BG-2 | Allow user to set custom budget limits per category | P0 |
| BG-3 | Real-time budget progress tracking (spent vs. remaining) | P0 |
| BG-4 | Alerts when approaching or exceeding budget limits | P1 |
| BG-5 | Rollover unused budget to next month (optional per category) | P2 |
| BG-6 | "Pay yourself first" mode: treat savings/investment contributions as a fixed budget line, budget remaining income | P1 |
| BG-7 | Compare actual spending vs. budget month-over-month | P1 |

### 3.4 Savings & Debt Optimization

| ID | Requirement | Priority |
|----|-------------|----------|
| SD-1 | Identify subscriptions and recurring charges; flag candidates for cancellation | P1 |
| SD-2 | Detect spending categories where user is above median (anonymized benchmarks) | P2 |
| SD-3 | Debt payoff planner: avalanche vs. snowball strategy comparison | P1 |
| SD-4 | Show interest saved by accelerating debt payments | P1 |
| SD-5 | Suggest optimal allocation of surplus cash (emergency fund → high-interest debt → investments) | P1 |
| SD-6 | Emergency fund tracker: target 3–6 months of expenses, show progress | P1 |

### 3.5 Retirement Projection & Planning

| ID | Requirement | Priority |
|----|-------------|----------|
| RT-1 | Project retirement balance based on current net worth, monthly contributions, and growth assumptions | P0 |
| RT-2 | Monte Carlo simulation (1,000+ runs) to produce confidence rating | P0 |
| RT-3 | Multi-scenario comparison: vary retirement age, contribution amount, growth rate, inflation rate | P0 |
| RT-4 | **Calculate earliest possible retirement date** given current trajectory | P0 |
| RT-5 | Show "what if" impact of changes: "If you save $200 more/month, you retire 3 years earlier" | P0 |
| RT-6 | Model Social Security income at different claiming ages (62, 67, 70) | P1 |
| RT-7 | Account for employer 401k match in contribution calculations | P1 |
| RT-8 | Model tax-advantaged vs. taxable account growth differently | P2 |
| RT-9 | Inflation-adjusted projections (show values in today's dollars) | P0 |
| RT-10 | Target retirement spending input (or auto-derive from current spending minus work-related costs) | P1 |
| RT-11 | Safe withdrawal rate modeling (4% rule, variable percentage, guardrails strategy) | P1 |
| RT-12 | Track progress toward retirement goal over time — is the user ahead or behind? | P1 |

### 3.6 Actionable Recommendations Engine

| ID | Requirement | Priority |
|----|-------------|----------|
| RE-1 | Generate prioritized list of actions to accelerate retirement | P0 |
| RE-2 | Quantify each recommendation's impact in dollars and retirement-date reduction | P0 |
| RE-3 | Example recommendations: increase 401k to match limit, refinance debt, cut specific subscription, shift allocation | P1 |
| RE-4 | Track which recommendations the user has acted on | P2 |
| RE-5 | Refresh recommendations as financial situation changes | P1 |

### 3.7 Investment Tracking

| ID | Requirement | Priority |
|----|-------------|----------|
| IV-1 | Display investment accounts with holdings and current values | P0 |
| IV-2 | Show asset allocation (stocks, bonds, cash, real estate, etc.) | P0 |
| IV-3 | Portfolio growth chart over time | P0 |
| IV-4 | Compare portfolio performance to benchmark indices | P2 |
| IV-5 | Flag high-fee funds and suggest lower-cost alternatives | P2 |
| IV-6 | Rebalancing alerts when allocation drifts from target | P2 |

### 3.8 Dashboard & Reporting

| ID | Requirement | Priority |
|----|-------------|----------|
| DA-1 | At-a-glance dashboard: net worth, monthly cash flow, budget status, retirement countdown | P0 |
| DA-2 | Monthly financial summary (income, spending by category, savings rate) | P0 |
| DA-3 | Year-over-year comparisons | P1 |
| DA-4 | Savings rate trend (% of income saved over time) | P1 |
| DA-5 | Exportable reports (PDF/CSV) | P2 |

---

## 4. Retirement Acceleration — The Core Loop

The app's differentiator is a tight feedback loop:

```
┌─────────────────────────────────────────────────┐
│                                                 │
│   1. MEASURE                                    │
│   Current net worth, income, spending,          │
│   savings rate, investment returns              │
│                     │                           │
│                     ▼                           │
│   2. PROJECT                                    │
│   Earliest retirement date given current        │
│   trajectory (Monte Carlo confidence)           │
│                     │                           │
│                     ▼                           │
│   3. OPTIMIZE                                   │
│   Ranked actions to pull retirement closer:     │
│   • Increase contributions                      │
│   • Cut low-value spending                      │
│   • Optimize debt payoff                        │
│   • Improve investment allocation               │
│                     │                           │
│                     ▼                           │
│   4. TRACK                                      │
│   Did retirement date improve? Show progress    │
│   over weeks/months. Celebrate wins.            │
│                     │                           │
│                     └──────── loop ─────────────┘
```

**Key metric displayed prominently**: "Projected Retirement Date" — this number should update as the user makes changes and as new financial data comes in.

---

## 5. Data Architecture

### 5.1 Local (On-Device)
| Data | Storage | Rationale |
|------|---------|-----------|
| Bank provider access tokens | Encrypted SQLite | Sensitive credential — never leaves device |
| Transactions | SQLite | Bulk financial data, fast local queries |
| Account balances | SQLite | Real-time display without network |
| Net worth snapshots | SQLite | Historical tracking |
| Budget configuration | SQLite | User preferences |
| Scenario inputs | SQLite | Retirement planner state |
| Categories & rules | SQLite | Categorization customizations |

### 5.2 Cloud (AWS)
| Data | Service | Rationale |
|------|---------|-----------|
| User authentication | Cognito | Secure identity management |
| Bank API proxy | API Gateway + Lambda | Keep Teller/FDX secrets server-side |
| Anonymized benchmarks | DynamoDB | Aggregate spending comparisons |
| Push notification triggers | SNS / Lambda | Budget alerts, sync reminders |
| App configuration | SSM Parameter Store | Feature flags, rate limits |
| Backup encryption keys | KMS | Optional encrypted cloud backup |

### 5.3 What Does NOT Go to Cloud
- Raw transaction data
- Account numbers or balances
- Bank provider access tokens
- Any PII unless user explicitly opts into cloud backup (encrypted, user-held key)

---

## 6. Non-Functional Requirements

| Area | Requirement |
|------|-------------|
| **Performance** | Dashboard loads in < 1s with 10k+ transactions locally |
| **Offline** | Full functionality without network (sync when available) |
| **Security** | SQLite encryption at rest, TLS for all network calls, no secrets in client binary |
| **Privacy** | GDPR and CCPA compliant. Clear privacy policy. User can export and delete all data. |
| **Platforms** | macOS (App Store) initially. Windows and Linux via Tauri. iOS/Android as future phase. |
| **Accessibility** | WCAG 2.1 AA compliance for all UI |
| **Updates** | Auto-update mechanism (Tauri updater or App Store) |

---

## 7. Monetization Considerations

| Model | Notes |
|-------|-------|
| **Freemium** | Free: 1 institution, basic budgeting, simple retirement projection. Paid: unlimited institutions, multi-scenario planning, recommendations engine, advanced analytics. |
| **Subscription** | $5–10/month or $50–80/year — aligned with financial apps market |
| **No ads, no data selling** | Core trust proposition for a financial app |

---

## 8. MVP Scope (Phase 1)

The minimum viable product to validate the concept:

- [ ] Account connection via Teller (bank + credit cards)
- [ ] Transaction list with auto-categorization
- [ ] Monthly budget creation and tracking
- [ ] Net worth dashboard with historical chart
- [ ] Retirement projector: net worth + contributions → projected balance + confidence
- [ ] Single "earliest retirement date" calculation
- [ ] 1–3 auto-generated recommendations to improve retirement timeline
- [ ] Local-only data storage
- [ ] Teller API calls proxied through AWS backend
- [ ] Provider abstraction layer (BankProvider interface) to support future FDX integration

### Phase 2
- Investment account support and allocation tracking
- Multi-scenario retirement comparison
- Debt payoff planner
- Recurring transaction detection and subscription management
- Social Security modeling

### Phase 3
- Spending benchmarks (anonymized)
- Tax-aware projections
- Cloud backup (encrypted)
- iOS / Android via Tauri mobile or React Native
- Recommendation tracking and progress gamification

---

## 9. Success Metrics

| Metric | Target |
|--------|--------|
| User connects at least 1 account | > 80% of signups |
| User returns weekly | > 60% WAU/MAU |
| User sets a retirement goal | > 50% within first week |
| Projected retirement date improves | > 70% of active users within 3 months |
| App Store rating | ≥ 4.5 stars |
| Churn (monthly) | < 5% for paid subscribers |

---

## 10. Open Questions

1. **Bank provider pricing**: Teller is free for the first 1,000 enrollments, then usage-based (significantly cheaper than Plaid). FDX direct connections are free (banks pay, not developers). How does this factor into pricing?
I want to keep the app as cheap as possible. maybe we charge the customer a small premium on the actual costs of Teller and aws services. The app should include a small amount of free accounts to get users started. It should also include a cost breakdown so users know exactly what they are paying for. As FDX coverage grows, costs per user drop further.
2. **Retirement spending model**: Should MVP require user to input target retirement spending, or auto-derive from current spending?
I want to keep the app as simple as possible. so I think we should auto-derive from current spending. I think the user should be able to say when they want to retire and the app should calculate the amount of money they need to save each month to reach that goal, or if that age is reasonable for their current financial situation, the app should calculate the amount of money they need to save each month to reach that goal.
3. **Multi-device sync**: If a user has a Mac and iPhone, how do we sync local data without putting raw financials in the cloud? Options: iCloud container, encrypted cloud backup, or device-to-device sync.
I want to make sure that the app is secure and that the user's data is protected. I also want syncing to be easy and seamless. Can a encrypted cloud backup be used to sync data between devices? client side encryption? and server side encryption on top of that? Maybe only sync the most recent 30 days of data to keep the app light weight. Or the data from the last sync.
4. **Regulatory**: Does the recommendations engine constitute "financial advice" requiring registration? (Likely no if framed as educational, but needs legal review.)
I do not want to give financial advice, but I do want to help users make better financial decisions.
5. **Competitor differentiation**: Apps like Mint, YNAB, and Monarch exist. Our angle is **retirement acceleration as the organizing principle** — every feature serves that goal. Is that positioning clear enough?
I also want to make sure that the app is secure and that the user's data is protected. and cheaper than mint and others.
