#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use std::net::SocketAddr;
use tauri::Manager;

struct EngineState {
    origin: String,
}

#[tauri::command]
fn engine_origin(state: tauri::State<'_, EngineState>) -> String {
    state.origin.clone()
}

fn main() {
    tauri::Builder::default()
        .invoke_handler(tauri::generate_handler![engine_origin])
        .setup(|app| {
            let data_dir = app.path().app_data_dir()?;
            std::fs::create_dir_all(&data_dir)?;
            let database = data_dir.join("rivet.db");
            let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
            listener.set_nonblocking(true)?;
            let bind: SocketAddr = listener.local_addr()?;
            let origin = format!("http://{bind}");
            app.manage(EngineState { origin });
            tauri::async_runtime::spawn(async move {
                let listener = match tokio::net::TcpListener::from_std(listener) {
                    Ok(listener) => listener,
                    Err(error) => {
                        eprintln!("Rivet local engine listener failed: {error}");
                        return;
                    }
                };
                if let Err(error) = rivet_server::serve_with_listener(
                    database,
                    rivet_server::ServerConfig {
                        bind,
                        auth_token: None,
                        auth_policy_file: None,
                        webhook_secret: None,
                        github_webhook_secret: None,
                        gitlab_webhook_secret: None,
                        github_webhook_credential_id: None,
                        gitlab_webhook_credential_id: None,
                        credentials_file: None,
                        credentials_passphrase: None,
                        extension_manifest_dir: None,
                        allowed_origins: Vec::new(),
                    },
                    listener,
                )
                .await
                {
                    eprintln!("Rivet local engine stopped: {error}");
                }
            });
            Ok(())
        })
        .run(tauri::generate_context!())
        .expect("error while running Rivet desktop");
}
