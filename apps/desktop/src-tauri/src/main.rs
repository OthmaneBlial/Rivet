#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use std::net::SocketAddr;
use tauri::Manager;

const ENGINE_BIND: &str = "127.0.0.1:7878";

fn main() {
    tauri::Builder::default()
        .setup(|app| {
            let data_dir = app.path().app_data_dir()?;
            std::fs::create_dir_all(&data_dir)?;
            let database = data_dir.join("rivet.db");
            let bind: SocketAddr = ENGINE_BIND.parse().expect("static engine address");
            tauri::async_runtime::spawn(async move {
                if let Err(error) = rivet_server::serve(database, bind).await {
                    eprintln!("Rivet local engine stopped: {error}");
                }
            });
            Ok(())
        })
        .run(tauri::generate_context!())
        .expect("error while running Rivet desktop");
}
