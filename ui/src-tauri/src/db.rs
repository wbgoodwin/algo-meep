use rusqlite::{Connection, Result};
use std::path::PathBuf;
use std::sync::Mutex;

pub struct Database {
    pub conn: Mutex<Connection>,
}

impl Database {
    pub fn new(app_data_dir: PathBuf) -> Result<Self> {
        std::fs::create_dir_all(&app_data_dir).ok();
        let db_path = app_data_dir.join("algoflow.db");
        let conn = Connection::open(db_path)?;
        let db = Database {
            conn: Mutex::new(conn),
        };
        db.initialize_tables()?;
        Ok(db)
    }

    fn initialize_tables(&self) -> Result<()> {
        let conn = self.conn.lock().unwrap();

        conn.execute_batch(
            "
            CREATE TABLE IF NOT EXISTS institutions (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                plaid_institution_id TEXT,
                logo TEXT,
                primary_color TEXT,
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                updated_at TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS credentials (
                id TEXT PRIMARY KEY,
                institution_id TEXT NOT NULL,
                access_token TEXT NOT NULL,
                item_id TEXT NOT NULL,
                cursor TEXT,
                last_synced_at TEXT,
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                updated_at TEXT NOT NULL DEFAULT (datetime('now')),
                FOREIGN KEY (institution_id) REFERENCES institutions(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS accounts (
                id TEXT PRIMARY KEY,
                institution_id TEXT NOT NULL,
                plaid_account_id TEXT NOT NULL UNIQUE,
                name TEXT NOT NULL,
                official_name TEXT,
                account_type TEXT NOT NULL,
                account_subtype TEXT,
                mask TEXT,
                current_balance REAL,
                available_balance REAL,
                credit_limit REAL,
                iso_currency_code TEXT DEFAULT 'USD',
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                updated_at TEXT NOT NULL DEFAULT (datetime('now')),
                FOREIGN KEY (institution_id) REFERENCES institutions(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS transactions (
                id TEXT PRIMARY KEY,
                account_id TEXT NOT NULL,
                plaid_transaction_id TEXT UNIQUE,
                amount REAL NOT NULL,
                date TEXT NOT NULL,
                name TEXT NOT NULL,
                merchant_name TEXT,
                category_primary TEXT,
                category_detailed TEXT,
                pending INTEGER NOT NULL DEFAULT 0,
                payment_channel TEXT,
                iso_currency_code TEXT DEFAULT 'USD',
                reviewed INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                updated_at TEXT NOT NULL DEFAULT (datetime('now')),
                FOREIGN KEY (account_id) REFERENCES accounts(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS categories (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL UNIQUE,
                budget_amount REAL,
                color TEXT,
                icon TEXT,
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                updated_at TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS recurring_transactions (
                id TEXT PRIMARY KEY,
                account_id TEXT NOT NULL,
                plaid_stream_id TEXT UNIQUE,
                description TEXT NOT NULL,
                amount REAL NOT NULL,
                frequency TEXT NOT NULL,
                category_primary TEXT,
                is_active INTEGER NOT NULL DEFAULT 1,
                first_date TEXT,
                last_date TEXT,
                next_expected_date TEXT,
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                updated_at TEXT NOT NULL DEFAULT (datetime('now')),
                FOREIGN KEY (account_id) REFERENCES accounts(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS net_worth_snapshots (
                id TEXT PRIMARY KEY,
                snapshot_date TEXT NOT NULL,
                total_assets REAL NOT NULL DEFAULT 0,
                total_liabilities REAL NOT NULL DEFAULT 0,
                net_worth REAL NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE INDEX IF NOT EXISTS idx_transactions_account_id ON transactions(account_id);
            CREATE INDEX IF NOT EXISTS idx_transactions_date ON transactions(date);
            CREATE INDEX IF NOT EXISTS idx_transactions_category ON transactions(category_primary);
            CREATE INDEX IF NOT EXISTS idx_accounts_institution_id ON accounts(institution_id);
            CREATE INDEX IF NOT EXISTS idx_net_worth_snapshots_date ON net_worth_snapshots(snapshot_date);
            "
        )?;

        Ok(())
    }
}
