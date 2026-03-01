mod commands;
mod db;
mod plaid;

use db::Database;
use plaid::PlaidClient;

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    let app_data_dir = dirs_next::data_dir()
        .unwrap_or_else(|| std::path::PathBuf::from("."))
        .join("algoflow");

    let database = Database::new(app_data_dir).expect("Failed to initialize database");
    let plaid_client = PlaidClient::new();

    tauri::Builder::default()
        .plugin(tauri_plugin_opener::init())
        .manage(database)
        .manage(plaid_client)
        .invoke_handler(tauri::generate_handler![
            commands::create_link_token,
            commands::exchange_token_and_sync,
            commands::sync_all_accounts,
            commands::get_dashboard_data,
            commands::get_transactions,
            commands::review_transaction,
            commands::get_accounts,
            commands::get_investment_transactions,
            commands::get_net_worth_history,
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
