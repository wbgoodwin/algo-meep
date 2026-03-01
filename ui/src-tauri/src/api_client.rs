use keyring::Entry;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::Mutex;

const KEYCHAIN_SERVICE: &str = "algoflow";
const KEYCHAIN_ACCESS_TOKEN: &str = "access_token";
const KEYCHAIN_REFRESH_TOKEN: &str = "refresh_token";
const CONFIG_FILE: &str = "config.json";

pub struct ApiClient {
    data_dir: PathBuf,
    base_url: Mutex<String>,
    http: Client,
    tokens: Mutex<TokenState>,
}

#[derive(Default)]
struct TokenState {
    access_token: Option<String>,
    refresh_token: Option<String>,
}

#[derive(Serialize, Deserialize, Default)]
struct AppConfig {
    api_base_url: String,
}

// --- Response envelope ---

#[derive(Deserialize)]
struct ApiResponse<T> {
    pub success: bool,
    pub data: Option<T>,
    pub error: Option<ApiErrorBody>,
}

#[derive(Deserialize)]
struct ApiErrorBody {
    pub message: String,
}

// --- Auth types ---

#[derive(Deserialize, Debug, Clone, Serialize)]
pub struct AuthTokens {
    pub access_token: String,
    pub id_token: String,
    pub refresh_token: Option<String>,
    pub expires_in: i64,
}

#[derive(Serialize)]
struct LoginRequest<'a> {
    email: &'a str,
    password: &'a str,
}

#[derive(Serialize)]
struct RegisterRequest<'a> {
    email: &'a str,
    password: &'a str,
}

#[derive(Serialize)]
struct RefreshRequest<'a> {
    refresh_token: &'a str,
}

// --- Bank types ---

#[derive(Serialize)]
struct EnrollRequest<'a> {
    institution_id: &'a str,
    #[serde(skip_serializing_if = "str::is_empty")]
    provider: &'a str,
}

#[derive(Deserialize, Debug, Clone)]
pub struct EnrollResponse {
    pub session_url: String,
    pub session_id: String,
    pub provider: String,
}

#[derive(Serialize)]
struct ExchangeTokenRequest<'a> {
    enrollment_token: &'a str,
    provider: &'a str,
}

#[derive(Deserialize, Debug, Clone)]
pub struct ExchangeTokenResponse {
    pub encrypted_access_token: String,
    pub institution_id: String,
    pub provider: String,
}

#[derive(Serialize)]
struct SyncTransactionsRequest<'a> {
    encrypted_access_token: &'a str,
    provider: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    cursor: Option<&'a str>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct SyncTransactionsResponse {
    pub added: Vec<ApiTransaction>,
    pub updated: Vec<ApiTransaction>,
    pub removed: Vec<String>,
    pub cursor: String,
}

#[derive(Deserialize, Debug, Clone)]
pub struct ApiTransaction {
    pub id: String,
    pub account_id: String,
    pub amount: f64,
    pub date: String,
    pub description: String,
    pub merchant_name: Option<String>,
    pub category: Option<String>,
    pub pending: bool,
    pub currency_code: String,
}

#[derive(Serialize)]
struct AccountsRequest<'a> {
    encrypted_access_token: &'a str,
    provider: &'a str,
}

#[derive(Deserialize, Debug, Clone)]
pub struct ApiAccount {
    pub id: String,
    pub institution_id: String,
    pub name: String,
    #[serde(rename = "type")]
    pub account_type: String,
    pub subtype: String,
    pub current_balance: f64,
    pub available_balance: Option<f64>,
    pub currency_code: String,
}

impl ApiClient {
    /// Create a new ApiClient. Loads the API URL from config file, falling back to
    /// the `API_BASE_URL` env var, then an empty string (user must configure via Settings).
    pub fn new(data_dir: PathBuf) -> Self {
        let base_url = Self::read_config_url(&data_dir)
            .or_else(|| std::env::var("API_BASE_URL").ok().filter(|s| !s.is_empty()))
            .unwrap_or_default();

        ApiClient {
            data_dir,
            base_url: Mutex::new(base_url),
            http: Client::new(),
            tokens: Mutex::new(TokenState::default()),
        }
    }

    // --- URL management ---

    pub fn get_url(&self) -> String {
        self.base_url.lock().unwrap().clone()
    }

    pub fn set_url(&self, url: String) -> Result<(), String> {
        *self.base_url.lock().unwrap() = url.clone();
        self.write_config_url(&url)
    }

    fn url(&self, path: &str) -> Result<String, String> {
        let base = self.base_url.lock().unwrap().clone();
        if base.is_empty() {
            return Err("API URL not configured. Open Settings to set it.".to_string());
        }
        Ok(format!("{}{}", base.trim_end_matches('/'), path))
    }

    fn read_config_url(data_dir: &PathBuf) -> Option<String> {
        let path = data_dir.join(CONFIG_FILE);
        let text = std::fs::read_to_string(&path).ok()?;
        let config: AppConfig = serde_json::from_str(&text).ok()?;
        if config.api_base_url.is_empty() {
            None
        } else {
            Some(config.api_base_url)
        }
    }

    fn write_config_url(&self, url: &str) -> Result<(), String> {
        let config = AppConfig {
            api_base_url: url.to_string(),
        };
        let json = serde_json::to_string_pretty(&config)
            .map_err(|e| format!("Failed to serialize config: {}", e))?;
        let path = self.data_dir.join(CONFIG_FILE);
        std::fs::write(&path, json)
            .map_err(|e| format!("Failed to write config: {}", e))
    }

    // --- Keychain helpers ---

    pub fn load_from_keychain(&self) -> Result<bool, String> {
        let access = load_keychain_entry(KEYCHAIN_ACCESS_TOKEN)?;
        let refresh = load_keychain_entry(KEYCHAIN_REFRESH_TOKEN)?;

        if access.is_none() {
            return Ok(false);
        }

        let mut state = self.tokens.lock().unwrap();
        state.access_token = access;
        state.refresh_token = refresh;
        Ok(true)
    }

    pub fn store_tokens(&self, access: &str, refresh: Option<&str>) -> Result<(), String> {
        store_keychain_entry(KEYCHAIN_ACCESS_TOKEN, access)?;
        if let Some(r) = refresh {
            store_keychain_entry(KEYCHAIN_REFRESH_TOKEN, r)?;
        }
        let mut state = self.tokens.lock().unwrap();
        state.access_token = Some(access.to_string());
        state.refresh_token = refresh.map(|s| s.to_string());
        Ok(())
    }

    pub fn clear_tokens(&self) -> Result<(), String> {
        delete_keychain_entry(KEYCHAIN_ACCESS_TOKEN);
        delete_keychain_entry(KEYCHAIN_REFRESH_TOKEN);
        let mut state = self.tokens.lock().unwrap();
        state.access_token = None;
        state.refresh_token = None;
        Ok(())
    }

    pub fn is_authenticated(&self) -> bool {
        self.tokens.lock().unwrap().access_token.is_some()
    }

    fn get_access_token(&self) -> Option<String> {
        self.tokens.lock().unwrap().access_token.clone()
    }

    fn get_refresh_token(&self) -> Option<String> {
        self.tokens.lock().unwrap().refresh_token.clone()
    }

    // --- Auth API ---

    pub async fn register(&self, email: &str, password: &str) -> Result<String, String> {
        let url = self.url("/auth/register")?;
        let req = RegisterRequest { email, password };

        let resp = self
            .http
            .post(&url)
            .json(&req)
            .send()
            .await
            .map_err(|e| format!("Request failed: {}", e))?;

        let envelope: ApiResponse<serde_json::Value> = resp
            .json()
            .await
            .map_err(|e| format!("Failed to parse response: {}", e))?;

        if !envelope.success {
            return Err(envelope
                .error
                .map(|e| e.message)
                .unwrap_or_else(|| "Registration failed".to_string()));
        }

        Ok("Registration successful. Check your email for verification.".to_string())
    }

    pub async fn login(&self, email: &str, password: &str) -> Result<AuthTokens, String> {
        let url = self.url("/auth/login")?;
        let req = LoginRequest { email, password };

        let resp = self
            .http
            .post(&url)
            .json(&req)
            .send()
            .await
            .map_err(|e| format!("Request failed: {}", e))?;

        let envelope: ApiResponse<AuthTokens> = resp
            .json()
            .await
            .map_err(|e| format!("Failed to parse response: {}", e))?;

        if !envelope.success {
            return Err(envelope
                .error
                .map(|e| e.message)
                .unwrap_or_else(|| "Login failed".to_string()));
        }

        let tokens = envelope
            .data
            .ok_or_else(|| "No token data in response".to_string())?;

        self.store_tokens(&tokens.access_token, tokens.refresh_token.as_deref())?;
        Ok(tokens)
    }

    pub async fn try_refresh(&self) -> Result<AuthTokens, String> {
        let refresh_token = self
            .get_refresh_token()
            .ok_or_else(|| "No refresh token available".to_string())?;

        let url = self.url("/auth/refresh")?;
        let req = RefreshRequest {
            refresh_token: &refresh_token,
        };

        let resp = self
            .http
            .post(&url)
            .json(&req)
            .send()
            .await
            .map_err(|e| format!("Request failed: {}", e))?;

        let envelope: ApiResponse<AuthTokens> = resp
            .json()
            .await
            .map_err(|e| format!("Failed to parse response: {}", e))?;

        if !envelope.success {
            return Err(envelope
                .error
                .map(|e| e.message)
                .unwrap_or_else(|| "Token refresh failed".to_string()));
        }

        let tokens = envelope
            .data
            .ok_or_else(|| "No token data in response".to_string())?;

        self.store_tokens(&tokens.access_token, tokens.refresh_token.as_deref())?;
        Ok(tokens)
    }

    // --- Authenticated request header ---

    fn auth_header(&self) -> Result<String, String> {
        let token = self
            .get_access_token()
            .ok_or_else(|| "Not authenticated".to_string())?;
        Ok(format!("Bearer {}", token))
    }

    // --- Bank API ---

    pub async fn bank_enroll(
        &self,
        institution_id: &str,
        provider: &str,
    ) -> Result<EnrollResponse, String> {
        let url = self.url("/bank/enroll")?;
        let auth = self.auth_header()?;
        let req = EnrollRequest {
            institution_id,
            provider,
        };

        let resp = self
            .http
            .post(&url)
            .header("Authorization", auth)
            .json(&req)
            .send()
            .await
            .map_err(|e| format!("Request failed: {}", e))?;

        let envelope: ApiResponse<EnrollResponse> = resp
            .json()
            .await
            .map_err(|e| format!("Failed to parse response: {}", e))?;

        if !envelope.success {
            return Err(envelope
                .error
                .map(|e| e.message)
                .unwrap_or_else(|| "Enrollment failed".to_string()));
        }

        envelope
            .data
            .ok_or_else(|| "No enrollment data in response".to_string())
    }

    pub async fn bank_exchange_token(
        &self,
        provider: &str,
        enrollment_token: &str,
    ) -> Result<ExchangeTokenResponse, String> {
        let url = self.url("/bank/exchange-token")?;
        let auth = self.auth_header()?;
        let req = ExchangeTokenRequest {
            enrollment_token,
            provider,
        };

        let resp = self
            .http
            .post(&url)
            .header("Authorization", auth)
            .json(&req)
            .send()
            .await
            .map_err(|e| format!("Request failed: {}", e))?;

        let envelope: ApiResponse<ExchangeTokenResponse> = resp
            .json()
            .await
            .map_err(|e| format!("Failed to parse response: {}", e))?;

        if !envelope.success {
            return Err(envelope
                .error
                .map(|e| e.message)
                .unwrap_or_else(|| "Token exchange failed".to_string()));
        }

        envelope
            .data
            .ok_or_else(|| "No exchange data in response".to_string())
    }

    pub async fn bank_get_accounts(
        &self,
        provider: &str,
        encrypted_access_token: &str,
    ) -> Result<Vec<ApiAccount>, String> {
        let url = self.url("/bank/accounts")?;
        let auth = self.auth_header()?;
        let req = AccountsRequest {
            encrypted_access_token,
            provider,
        };

        let resp = self
            .http
            .post(&url)
            .header("Authorization", auth)
            .json(&req)
            .send()
            .await
            .map_err(|e| format!("Request failed: {}", e))?;

        let envelope: ApiResponse<Vec<ApiAccount>> = resp
            .json()
            .await
            .map_err(|e| format!("Failed to parse response: {}", e))?;

        if !envelope.success {
            return Err(envelope
                .error
                .map(|e| e.message)
                .unwrap_or_else(|| "Failed to fetch accounts".to_string()));
        }

        envelope
            .data
            .ok_or_else(|| "No accounts data in response".to_string())
    }

    pub async fn bank_sync_transactions(
        &self,
        provider: &str,
        encrypted_access_token: &str,
        cursor: Option<&str>,
    ) -> Result<SyncTransactionsResponse, String> {
        let url = self.url("/bank/sync-transactions")?;
        let auth = self.auth_header()?;
        let req = SyncTransactionsRequest {
            encrypted_access_token,
            provider,
            cursor,
        };

        let resp = self
            .http
            .post(&url)
            .header("Authorization", auth)
            .json(&req)
            .send()
            .await
            .map_err(|e| format!("Request failed: {}", e))?;

        let envelope: ApiResponse<SyncTransactionsResponse> = resp
            .json()
            .await
            .map_err(|e| format!("Failed to parse response: {}", e))?;

        if !envelope.success {
            return Err(envelope
                .error
                .map(|e| e.message)
                .unwrap_or_else(|| "Transaction sync failed".to_string()));
        }

        envelope
            .data
            .ok_or_else(|| "No sync data in response".to_string())
    }
}

// --- Keychain helpers ---

fn load_keychain_entry(key: &str) -> Result<Option<String>, String> {
    let entry = Entry::new(KEYCHAIN_SERVICE, key)
        .map_err(|e| format!("Keychain error: {}", e))?;
    match entry.get_password() {
        Ok(val) => Ok(Some(val)),
        Err(keyring::Error::NoEntry) => Ok(None),
        Err(e) => Err(format!("Failed to read keychain '{}': {}", key, e)),
    }
}

fn store_keychain_entry(key: &str, value: &str) -> Result<(), String> {
    let entry = Entry::new(KEYCHAIN_SERVICE, key)
        .map_err(|e| format!("Keychain error: {}", e))?;
    entry
        .set_password(value)
        .map_err(|e| format!("Failed to store keychain '{}': {}", key, e))
}

fn delete_keychain_entry(key: &str) {
    if let Ok(entry) = Entry::new(KEYCHAIN_SERVICE, key) {
        let _ = entry.delete_password();
    }
}
