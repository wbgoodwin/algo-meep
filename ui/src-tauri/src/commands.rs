use crate::db::Database;
use crate::plaid::PlaidClient;
use rusqlite::params;
use serde::{Deserialize, Serialize};
use tauri::State;
use uuid::Uuid;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Account {
    pub id: String,
    pub institution_id: String,
    pub plaid_account_id: String,
    pub name: String,
    pub official_name: Option<String>,
    pub account_type: String,
    pub account_subtype: Option<String>,
    pub mask: Option<String>,
    pub current_balance: Option<f64>,
    pub available_balance: Option<f64>,
    pub credit_limit: Option<f64>,
    pub iso_currency_code: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Transaction {
    pub id: String,
    pub account_id: String,
    pub plaid_transaction_id: Option<String>,
    pub amount: f64,
    pub date: String,
    pub name: String,
    pub merchant_name: Option<String>,
    pub category_primary: Option<String>,
    pub category_detailed: Option<String>,
    pub pending: bool,
    pub payment_channel: Option<String>,
    pub iso_currency_code: Option<String>,
    pub reviewed: bool,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Institution {
    pub id: String,
    pub name: String,
    pub plaid_institution_id: Option<String>,
    pub logo: Option<String>,
    pub primary_color: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Category {
    pub id: String,
    pub name: String,
    pub budget_amount: Option<f64>,
    pub color: Option<String>,
    pub icon: Option<String>,
}

#[derive(Serialize, Debug)]
pub struct DashboardData {
    pub accounts: Vec<Account>,
    pub recent_transactions: Vec<Transaction>,
    pub institutions: Vec<Institution>,
    pub total_balance: f64,
    pub total_credit_debt: f64,
    pub categories: Vec<CategorySpending>,
}

#[derive(Serialize, Debug)]
pub struct CategorySpending {
    pub name: String,
    pub amount: f64,
    pub budget: Option<f64>,
    pub color: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct NetWorthSnapshot {
    pub id: String,
    pub snapshot_date: String,
    pub total_assets: f64,
    pub total_liabilities: f64,
    pub net_worth: f64,
}

// --- Plaid Commands ---

#[tauri::command]
pub async fn create_link_token(plaid: State<'_, PlaidClient>) -> Result<String, String> {
    let user_id = Uuid::new_v4().to_string();
    let response = plaid.create_link_token(&user_id).await?;
    Ok(response.link_token)
}

#[tauri::command]
pub async fn exchange_token_and_sync(
    public_token: String,
    plaid: State<'_, PlaidClient>,
    db: State<'_, Database>,
) -> Result<String, String> {
    let token_response = plaid.exchange_public_token(&public_token).await?;
    let access_token = &token_response.access_token;
    let item_id = &token_response.item_id;

    let accounts_response = plaid.get_accounts(access_token).await?;

    let institution_id_str = accounts_response
        .item
        .institution_id
        .as_deref()
        .unwrap_or("unknown");

    let inst_info = plaid.get_institution(institution_id_str).await.ok();

    let local_institution_id = Uuid::new_v4().to_string();
    let inst_name = inst_info
        .as_ref()
        .map(|i| i.institution.name.clone())
        .unwrap_or_else(|| "Unknown Institution".to_string());
    let inst_logo = inst_info.as_ref().and_then(|i| i.institution.logo.clone());
    let inst_color = inst_info
        .as_ref()
        .and_then(|i| i.institution.primary_color.clone());

    {
        let conn = db.conn.lock().unwrap();

        conn.execute(
            "INSERT OR REPLACE INTO institutions (id, name, plaid_institution_id, logo, primary_color) VALUES (?1, ?2, ?3, ?4, ?5)",
            params![local_institution_id, inst_name, institution_id_str, inst_logo, inst_color],
        ).map_err(|e| format!("Failed to insert institution: {}", e))?;

        conn.execute(
            "INSERT INTO credentials (id, institution_id, access_token, item_id) VALUES (?1, ?2, ?3, ?4)",
            params![Uuid::new_v4().to_string(), local_institution_id, access_token, item_id],
        ).map_err(|e| format!("Failed to insert credentials: {}", e))?;

        for account in &accounts_response.accounts {
            let account_id = Uuid::new_v4().to_string();
            conn.execute(
                "INSERT OR REPLACE INTO accounts (id, institution_id, plaid_account_id, name, official_name, account_type, account_subtype, mask, current_balance, available_balance, credit_limit, iso_currency_code) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
                params![
                    account_id,
                    local_institution_id,
                    account.account_id,
                    account.name,
                    account.official_name,
                    account.account_type,
                    account.subtype,
                    account.mask,
                    account.balances.current,
                    account.balances.available,
                    account.balances.limit,
                    account.balances.iso_currency_code,
                ],
            ).map_err(|e| format!("Failed to insert account: {}", e))?;
        }
    }

    sync_transactions_for_item(access_token, None, &plaid, &db).await?;

    Ok(local_institution_id)
}

async fn sync_transactions_for_item(
    access_token: &str,
    cursor: Option<String>,
    plaid: &PlaidClient,
    db: &Database,
) -> Result<(), String> {
    let mut current_cursor = cursor;

    loop {
        let response = plaid
            .sync_transactions(access_token, current_cursor.clone())
            .await?;

        let conn = db.conn.lock().unwrap();

        for txn in &response.added {
            let account_id: Option<String> = conn
                .query_row(
                    "SELECT id FROM accounts WHERE plaid_account_id = ?1",
                    params![txn.account_id],
                    |row| row.get(0),
                )
                .ok();

            if let Some(account_id) = account_id {
                let cat_primary = txn
                    .personal_finance_category
                    .as_ref()
                    .map(|c| c.primary.clone());
                let cat_detailed = txn
                    .personal_finance_category
                    .as_ref()
                    .map(|c| c.detailed.clone());

                conn.execute(
                    "INSERT OR REPLACE INTO transactions (id, account_id, plaid_transaction_id, amount, date, name, merchant_name, category_primary, category_detailed, pending, payment_channel, iso_currency_code) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
                    params![
                        Uuid::new_v4().to_string(),
                        account_id,
                        txn.transaction_id,
                        txn.amount,
                        txn.date,
                        txn.name,
                        txn.merchant_name,
                        cat_primary,
                        cat_detailed,
                        txn.pending as i32,
                        txn.payment_channel,
                        txn.iso_currency_code,
                    ],
                ).map_err(|e| format!("Failed to insert transaction: {}", e))?;
            }
        }

        for txn in &response.modified {
            conn.execute(
                "UPDATE transactions SET amount = ?1, name = ?2, merchant_name = ?3, pending = ?4, updated_at = datetime('now') WHERE plaid_transaction_id = ?5",
                params![txn.amount, txn.name, txn.merchant_name, txn.pending as i32, txn.transaction_id],
            ).map_err(|e| format!("Failed to update transaction: {}", e))?;
        }

        for txn in &response.removed {
            conn.execute(
                "DELETE FROM transactions WHERE plaid_transaction_id = ?1",
                params![txn.transaction_id],
            ).map_err(|e| format!("Failed to remove transaction: {}", e))?;
        }

        drop(conn);

        current_cursor = Some(response.next_cursor.clone());

        {
            let conn = db.conn.lock().unwrap();
            conn.execute(
                "UPDATE credentials SET cursor = ?1, last_synced_at = datetime('now'), updated_at = datetime('now') WHERE access_token = ?2",
                params![current_cursor, access_token],
            ).map_err(|e| format!("Failed to update cursor: {}", e))?;
        }

        if !response.has_more {
            break;
        }
    }

    Ok(())
}

// --- Sync All Command ---

#[tauri::command]
pub async fn sync_all_accounts(
    plaid: State<'_, PlaidClient>,
    db: State<'_, Database>,
) -> Result<String, String> {
    let credentials: Vec<(String, Option<String>)> = {
        let conn = db.conn.lock().unwrap();
        let mut stmt = conn
            .prepare("SELECT access_token, cursor FROM credentials")
            .map_err(|e| format!("Failed to query credentials: {}", e))?;
        let results: Vec<(String, Option<String>)> = stmt
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .map_err(|e| format!("Failed to read credentials: {}", e))?
            .filter_map(|r| r.ok())
            .collect();
        results
    };

    for (access_token, cursor) in credentials {
        sync_transactions_for_item(&access_token, cursor, &plaid, &db).await?;
    }

    snapshot_net_worth(&db)?;

    Ok("Sync complete".to_string())
}

fn snapshot_net_worth(db: &Database) -> Result<(), String> {
    let conn = db.conn.lock().unwrap();

    let total_assets: f64 = conn
        .query_row(
            "SELECT COALESCE(SUM(current_balance), 0) FROM accounts WHERE account_type != 'credit'",
            [],
            |row| row.get(0),
        )
        .map_err(|e| format!("Failed to compute assets: {}", e))?;

    let total_liabilities: f64 = conn
        .query_row(
            "SELECT COALESCE(SUM(ABS(current_balance)), 0) FROM accounts WHERE account_type = 'credit'",
            [],
            |row| row.get(0),
        )
        .map_err(|e| format!("Failed to compute liabilities: {}", e))?;

    let net_worth = total_assets - total_liabilities;
    let today: String = conn
        .query_row("SELECT date('now')", [], |row| row.get(0))
        .map_err(|e| format!("Failed to get date: {}", e))?;

    conn.execute(
        "DELETE FROM net_worth_snapshots WHERE snapshot_date = ?1",
        params![today],
    )
    .map_err(|e| format!("Failed to clear old snapshot: {}", e))?;

    conn.execute(
        "INSERT INTO net_worth_snapshots (id, snapshot_date, total_assets, total_liabilities, net_worth) VALUES (?1, ?2, ?3, ?4, ?5)",
        params![Uuid::new_v4().to_string(), today, total_assets, total_liabilities, net_worth],
    )
    .map_err(|e| format!("Failed to insert snapshot: {}", e))?;

    Ok(())
}

// --- Read Commands ---

#[tauri::command]
pub fn get_dashboard_data(db: State<'_, Database>) -> Result<DashboardData, String> {
    let conn = db.conn.lock().unwrap();

    let mut stmt = conn
        .prepare("SELECT id, institution_id, plaid_account_id, name, official_name, account_type, account_subtype, mask, current_balance, available_balance, credit_limit, iso_currency_code FROM accounts")
        .map_err(|e| format!("Failed to query accounts: {}", e))?;

    let accounts: Vec<Account> = stmt
        .query_map([], |row| {
            Ok(Account {
                id: row.get(0)?,
                institution_id: row.get(1)?,
                plaid_account_id: row.get(2)?,
                name: row.get(3)?,
                official_name: row.get(4)?,
                account_type: row.get(5)?,
                account_subtype: row.get(6)?,
                mask: row.get(7)?,
                current_balance: row.get(8)?,
                available_balance: row.get(9)?,
                credit_limit: row.get(10)?,
                iso_currency_code: row.get(11)?,
            })
        })
        .map_err(|e| format!("Failed to read accounts: {}", e))?
        .filter_map(|r| r.ok())
        .collect();

    let mut stmt = conn
        .prepare("SELECT id, account_id, plaid_transaction_id, amount, date, name, merchant_name, category_primary, category_detailed, pending, payment_channel, iso_currency_code, reviewed FROM transactions ORDER BY date DESC LIMIT 50")
        .map_err(|e| format!("Failed to query transactions: {}", e))?;

    let recent_transactions: Vec<Transaction> = stmt
        .query_map([], |row| {
            Ok(Transaction {
                id: row.get(0)?,
                account_id: row.get(1)?,
                plaid_transaction_id: row.get(2)?,
                amount: row.get(3)?,
                date: row.get(4)?,
                name: row.get(5)?,
                merchant_name: row.get(6)?,
                category_primary: row.get(7)?,
                category_detailed: row.get(8)?,
                pending: row.get::<_, i32>(9)? != 0,
                payment_channel: row.get(10)?,
                iso_currency_code: row.get(11)?,
                reviewed: row.get::<_, i32>(12)? != 0,
            })
        })
        .map_err(|e| format!("Failed to read transactions: {}", e))?
        .filter_map(|r| r.ok())
        .collect();

    let mut stmt = conn
        .prepare("SELECT id, name, plaid_institution_id, logo, primary_color FROM institutions")
        .map_err(|e| format!("Failed to query institutions: {}", e))?;

    let institutions: Vec<Institution> = stmt
        .query_map([], |row| {
            Ok(Institution {
                id: row.get(0)?,
                name: row.get(1)?,
                plaid_institution_id: row.get(2)?,
                logo: row.get(3)?,
                primary_color: row.get(4)?,
            })
        })
        .map_err(|e| format!("Failed to read institutions: {}", e))?
        .filter_map(|r| r.ok())
        .collect();

    let total_balance: f64 = accounts
        .iter()
        .filter(|a| a.account_type == "depository")
        .filter_map(|a| a.current_balance)
        .sum();

    let total_credit_debt: f64 = accounts
        .iter()
        .filter(|a| a.account_type == "credit")
        .filter_map(|a| a.current_balance)
        .sum();

    let mut stmt = conn
        .prepare("SELECT category_primary, SUM(amount) FROM transactions WHERE date >= date('now', '-30 days') AND amount > 0 GROUP BY category_primary ORDER BY SUM(amount) DESC LIMIT 10")
        .map_err(|e| format!("Failed to query category spending: {}", e))?;

    let categories: Vec<CategorySpending> = stmt
        .query_map([], |row| {
            let name: Option<String> = row.get(0)?;
            let amount: f64 = row.get(1)?;
            Ok(CategorySpending {
                name: name.unwrap_or_else(|| "Uncategorized".to_string()),
                amount,
                budget: None,
                color: None,
            })
        })
        .map_err(|e| format!("Failed to read category spending: {}", e))?
        .filter_map(|r| r.ok())
        .collect();

    Ok(DashboardData {
        accounts,
        recent_transactions,
        institutions,
        total_balance,
        total_credit_debt,
        categories,
    })
}

#[tauri::command]
pub fn get_transactions(
    db: State<'_, Database>,
    limit: Option<u32>,
    offset: Option<u32>,
) -> Result<Vec<Transaction>, String> {
    let conn = db.conn.lock().unwrap();
    let limit = limit.unwrap_or(100);
    let offset = offset.unwrap_or(0);

    let mut stmt = conn
        .prepare("SELECT id, account_id, plaid_transaction_id, amount, date, name, merchant_name, category_primary, category_detailed, pending, payment_channel, iso_currency_code, reviewed FROM transactions ORDER BY date DESC LIMIT ?1 OFFSET ?2")
        .map_err(|e| format!("Failed to query transactions: {}", e))?;

    let transactions: Vec<Transaction> = stmt
        .query_map(params![limit, offset], |row| {
            Ok(Transaction {
                id: row.get(0)?,
                account_id: row.get(1)?,
                plaid_transaction_id: row.get(2)?,
                amount: row.get(3)?,
                date: row.get(4)?,
                name: row.get(5)?,
                merchant_name: row.get(6)?,
                category_primary: row.get(7)?,
                category_detailed: row.get(8)?,
                pending: row.get::<_, i32>(9)? != 0,
                payment_channel: row.get(10)?,
                iso_currency_code: row.get(11)?,
                reviewed: row.get::<_, i32>(12)? != 0,
            })
        })
        .map_err(|e| format!("Failed to read transactions: {}", e))?
        .filter_map(|r| r.ok())
        .collect();

    Ok(transactions)
}

#[tauri::command]
pub fn review_transaction(db: State<'_, Database>, transaction_id: String) -> Result<(), String> {
    let conn = db.conn.lock().unwrap();
    conn.execute(
        "UPDATE transactions SET reviewed = 1, updated_at = datetime('now') WHERE id = ?1",
        params![transaction_id],
    )
    .map_err(|e| format!("Failed to review transaction: {}", e))?;
    Ok(())
}

#[tauri::command]
pub fn get_investment_transactions(db: State<'_, Database>) -> Result<Vec<Transaction>, String> {
    let conn = db.conn.lock().unwrap();

    let mut stmt = conn
        .prepare(
            "SELECT t.id, t.account_id, t.plaid_transaction_id, t.amount, t.date, t.name, t.merchant_name, t.category_primary, t.category_detailed, t.pending, t.payment_channel, t.iso_currency_code, t.reviewed
             FROM transactions t
             INNER JOIN accounts a ON t.account_id = a.id
             WHERE a.account_type = 'investment'
             ORDER BY t.date ASC"
        )
        .map_err(|e| format!("Failed to query investment transactions: {}", e))?;

    let transactions: Vec<Transaction> = stmt
        .query_map([], |row| {
            Ok(Transaction {
                id: row.get(0)?,
                account_id: row.get(1)?,
                plaid_transaction_id: row.get(2)?,
                amount: row.get(3)?,
                date: row.get(4)?,
                name: row.get(5)?,
                merchant_name: row.get(6)?,
                category_primary: row.get(7)?,
                category_detailed: row.get(8)?,
                pending: row.get::<_, i32>(9)? != 0,
                payment_channel: row.get(10)?,
                iso_currency_code: row.get(11)?,
                reviewed: row.get::<_, i32>(12)? != 0,
            })
        })
        .map_err(|e| format!("Failed to read investment transactions: {}", e))?
        .filter_map(|r| r.ok())
        .collect();

    Ok(transactions)
}

#[tauri::command]
pub fn get_net_worth_history(db: State<'_, Database>) -> Result<Vec<NetWorthSnapshot>, String> {
    let conn = db.conn.lock().unwrap();

    let mut stmt = conn
        .prepare("SELECT id, snapshot_date, total_assets, total_liabilities, net_worth FROM net_worth_snapshots ORDER BY snapshot_date ASC")
        .map_err(|e| format!("Failed to query net worth history: {}", e))?;

    let snapshots: Vec<NetWorthSnapshot> = stmt
        .query_map([], |row| {
            Ok(NetWorthSnapshot {
                id: row.get(0)?,
                snapshot_date: row.get(1)?,
                total_assets: row.get(2)?,
                total_liabilities: row.get(3)?,
                net_worth: row.get(4)?,
            })
        })
        .map_err(|e| format!("Failed to read net worth history: {}", e))?
        .filter_map(|r| r.ok())
        .collect();

    Ok(snapshots)
}

#[tauri::command]
pub fn get_accounts(db: State<'_, Database>) -> Result<Vec<Account>, String> {
    let conn = db.conn.lock().unwrap();

    let mut stmt = conn
        .prepare("SELECT id, institution_id, plaid_account_id, name, official_name, account_type, account_subtype, mask, current_balance, available_balance, credit_limit, iso_currency_code FROM accounts")
        .map_err(|e| format!("Failed to query accounts: {}", e))?;

    let accounts: Vec<Account> = stmt
        .query_map([], |row| {
            Ok(Account {
                id: row.get(0)?,
                institution_id: row.get(1)?,
                plaid_account_id: row.get(2)?,
                name: row.get(3)?,
                official_name: row.get(4)?,
                account_type: row.get(5)?,
                account_subtype: row.get(6)?,
                mask: row.get(7)?,
                current_balance: row.get(8)?,
                available_balance: row.get(9)?,
                credit_limit: row.get(10)?,
                iso_currency_code: row.get(11)?,
            })
        })
        .map_err(|e| format!("Failed to read accounts: {}", e))?
        .filter_map(|r| r.ok())
        .collect();

    Ok(accounts)
}
