use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::env;

pub struct PlaidClient {
    client: Client,
    client_id: String,
    secret: String,
    base_url: String,
}

#[derive(Serialize)]
struct LinkTokenCreateRequest {
    client_id: String,
    secret: String,
    user: PlaidUser,
    client_name: String,
    products: Vec<String>,
    country_codes: Vec<String>,
    language: String,
}

#[derive(Serialize)]
struct PlaidUser {
    client_user_id: String,
}

#[derive(Deserialize)]
pub struct LinkTokenResponse {
    pub link_token: String,
    pub expiration: String,
}

#[derive(Serialize)]
struct PublicTokenExchangeRequest {
    client_id: String,
    secret: String,
    public_token: String,
}

#[derive(Deserialize)]
pub struct AccessTokenResponse {
    pub access_token: String,
    pub item_id: String,
}

#[derive(Serialize)]
struct TransactionsSyncRequest {
    client_id: String,
    secret: String,
    access_token: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    cursor: Option<String>,
    count: u32,
}

#[derive(Deserialize, Debug, Clone)]
pub struct TransactionsSyncResponse {
    pub added: Vec<PlaidTransaction>,
    pub modified: Vec<PlaidTransaction>,
    pub removed: Vec<RemovedTransaction>,
    pub next_cursor: String,
    pub has_more: bool,
}

#[derive(Deserialize, Debug, Clone)]
pub struct PlaidTransaction {
    pub transaction_id: String,
    pub account_id: String,
    pub amount: f64,
    pub date: String,
    pub name: String,
    pub merchant_name: Option<String>,
    pub personal_finance_category: Option<PersonalFinanceCategory>,
    pub pending: bool,
    pub payment_channel: Option<String>,
    pub iso_currency_code: Option<String>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct PersonalFinanceCategory {
    pub primary: String,
    pub detailed: String,
}

#[derive(Deserialize, Debug, Clone)]
pub struct RemovedTransaction {
    pub transaction_id: String,
}

#[derive(Serialize)]
struct AccountsGetRequest {
    client_id: String,
    secret: String,
    access_token: String,
}

#[derive(Deserialize, Debug)]
pub struct AccountsGetResponse {
    pub accounts: Vec<PlaidAccount>,
    pub item: PlaidItem,
}

#[derive(Deserialize, Debug)]
pub struct PlaidAccount {
    pub account_id: String,
    pub name: String,
    pub official_name: Option<String>,
    #[serde(rename = "type")]
    pub account_type: String,
    pub subtype: Option<String>,
    pub mask: Option<String>,
    pub balances: PlaidBalances,
}

#[derive(Deserialize, Debug)]
pub struct PlaidBalances {
    pub current: Option<f64>,
    pub available: Option<f64>,
    pub limit: Option<f64>,
    pub iso_currency_code: Option<String>,
}

#[derive(Deserialize, Debug)]
pub struct PlaidItem {
    pub item_id: String,
    pub institution_id: Option<String>,
}

#[derive(Serialize)]
struct InstitutionGetRequest {
    client_id: String,
    secret: String,
    institution_id: String,
    country_codes: Vec<String>,
}

#[derive(Deserialize, Debug)]
pub struct InstitutionGetResponse {
    pub institution: PlaidInstitution,
}

#[derive(Deserialize, Debug)]
pub struct PlaidInstitution {
    pub institution_id: String,
    pub name: String,
    pub logo: Option<String>,
    pub primary_color: Option<String>,
}

impl PlaidClient {
    pub fn new() -> Self {
        let environment = env::var("PLAID_ENV").unwrap_or_else(|_| "sandbox".to_string());
        let base_url = match environment.as_str() {
            "production" => "https://production.plaid.com",
            "development" => "https://development.plaid.com",
            _ => "https://sandbox.plaid.com",
        };

        PlaidClient {
            client: Client::new(),
            client_id: env::var("PLAID_CLIENT_ID").unwrap_or_default(),
            secret: env::var("PLAID_SECRET").unwrap_or_default(),
            base_url: base_url.to_string(),
        }
    }

    pub async fn create_link_token(&self, user_id: &str) -> Result<LinkTokenResponse, String> {
        let request = LinkTokenCreateRequest {
            client_id: self.client_id.clone(),
            secret: self.secret.clone(),
            user: PlaidUser {
                client_user_id: user_id.to_string(),
            },
            client_name: "AlgoFlow".to_string(),
            products: vec!["transactions".to_string()],
            country_codes: vec!["US".to_string()],
            language: "en".to_string(),
        };

        let response = self
            .client
            .post(format!("{}/link/token/create", self.base_url))
            .json(&request)
            .send()
            .await
            .map_err(|e| format!("Failed to create link token: {}", e))?;

        response
            .json::<LinkTokenResponse>()
            .await
            .map_err(|e| format!("Failed to parse link token response: {}", e))
    }

    pub async fn exchange_public_token(
        &self,
        public_token: &str,
    ) -> Result<AccessTokenResponse, String> {
        let request = PublicTokenExchangeRequest {
            client_id: self.client_id.clone(),
            secret: self.secret.clone(),
            public_token: public_token.to_string(),
        };

        let response = self
            .client
            .post(format!("{}/item/public_token/exchange", self.base_url))
            .json(&request)
            .send()
            .await
            .map_err(|e| format!("Failed to exchange public token: {}", e))?;

        response
            .json::<AccessTokenResponse>()
            .await
            .map_err(|e| format!("Failed to parse access token response: {}", e))
    }

    pub async fn get_accounts(
        &self,
        access_token: &str,
    ) -> Result<AccountsGetResponse, String> {
        let request = AccountsGetRequest {
            client_id: self.client_id.clone(),
            secret: self.secret.clone(),
            access_token: access_token.to_string(),
        };

        let response = self
            .client
            .post(format!("{}/accounts/get", self.base_url))
            .json(&request)
            .send()
            .await
            .map_err(|e| format!("Failed to get accounts: {}", e))?;

        response
            .json::<AccountsGetResponse>()
            .await
            .map_err(|e| format!("Failed to parse accounts response: {}", e))
    }

    pub async fn get_institution(
        &self,
        institution_id: &str,
    ) -> Result<InstitutionGetResponse, String> {
        let request = InstitutionGetRequest {
            client_id: self.client_id.clone(),
            secret: self.secret.clone(),
            institution_id: institution_id.to_string(),
            country_codes: vec!["US".to_string()],
        };

        let response = self
            .client
            .post(format!("{}/institutions/get_by_id", self.base_url))
            .json(&request)
            .send()
            .await
            .map_err(|e| format!("Failed to get institution: {}", e))?;

        response
            .json::<InstitutionGetResponse>()
            .await
            .map_err(|e| format!("Failed to parse institution response: {}", e))
    }

    pub async fn sync_transactions(
        &self,
        access_token: &str,
        cursor: Option<String>,
    ) -> Result<TransactionsSyncResponse, String> {
        let request = TransactionsSyncRequest {
            client_id: self.client_id.clone(),
            secret: self.secret.clone(),
            access_token: access_token.to_string(),
            cursor,
            count: 500,
        };

        let response = self
            .client
            .post(format!("{}/transactions/sync", self.base_url))
            .json(&request)
            .send()
            .await
            .map_err(|e| format!("Failed to sync transactions: {}", e))?;

        response
            .json::<TransactionsSyncResponse>()
            .await
            .map_err(|e| format!("Failed to parse transactions sync response: {}", e))
    }
}
