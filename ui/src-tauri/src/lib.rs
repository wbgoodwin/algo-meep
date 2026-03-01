mod api_client;
mod commands;
mod db;

use api_client::ApiClient;
use db::Database;

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    let app_data_dir = dirs_next::data_dir()
        .unwrap_or_else(|| std::path::PathBuf::from("."))
        .join("algoflow");

    // Ensure the data directory exists before opening the DB or config file
    std::fs::create_dir_all(&app_data_dir).expect("Failed to create app data directory");

    let database = Database::new(app_data_dir.clone()).expect("Failed to initialize database");
    let api_client = ApiClient::new(app_data_dir);

    tauri::Builder::default()
        .plugin(tauri_plugin_opener::init())
        .manage(database)
        .manage(api_client)
        .invoke_handler(tauri::generate_handler![
            // Config
            commands::get_api_url,
            commands::set_api_url,
            // Auth
            commands::register,
            commands::login,
            commands::logout,
            commands::check_auth,
            // Bank enrollment
            commands::bank_enroll,
            commands::bank_exchange_token,
            // Sync
            commands::sync_all_accounts,
            // Read
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
