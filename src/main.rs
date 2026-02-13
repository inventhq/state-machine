use std::sync::Arc;

use axum::routing::{delete, get, post, put};
use axum::Router;
use tokio::signal;
use tracing::info;
use tracing_subscriber::EnvFilter;

mod actions;
mod auth;
mod db;
mod engine;
mod errors;
mod models;
mod routes;
mod scheduler;
mod transition_core;

use routes::AppState;

#[tokio::main]
async fn main() {
    dotenvy::dotenv().ok();

    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let listen_addr =
        std::env::var("LISTEN_ADDR").unwrap_or_else(|_| "0.0.0.0:3050".to_string());
    let turso_url =
        std::env::var("TURSO_DATABASE_URL").unwrap_or_else(|_| "file:statemachine.db".to_string());
    let turso_token = std::env::var("TURSO_AUTH_TOKEN").unwrap_or_default();
    let event_core_ingest_url =
        std::env::var("EVENT_CORE_INGEST_URL").unwrap_or_else(|_| "http://localhost:3030/ingest".to_string());
    let api_key = std::env::var("API_KEY").unwrap_or_default();
    let timeout_interval_secs: u64 = std::env::var("TIMEOUT_INTERVAL_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(10);

    info!("Starting state-machine on {}", listen_addr);

    let database = db::init(&turso_url, &turso_token)
        .await
        .expect("Failed to initialize database");

    let http_client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .build()
        .expect("Failed to create HTTP client");

    let db = Arc::new(database);

    // Start timeout scheduler
    scheduler::start(
        db.clone(),
        http_client.clone(),
        event_core_ingest_url.clone(),
        timeout_interval_secs,
    );

    let machine_cache = std::sync::Arc::new(dashmap::DashMap::new());

    let state = AppState {
        db,
        http_client,
        event_core_ingest_url,
        machine_cache,
    };

    // API routes (auth-protected)
    let api_routes = Router::new()
        // Machine CRUD
        .route("/machines", post(routes::machines::create_machine))
        .route("/machines", get(routes::machines::list_machines))
        .route("/machines/{machine_id}", get(routes::machines::get_machine))
        .route("/machines/{machine_id}", put(routes::machines::update_machine))
        .route("/machines/{machine_id}", delete(routes::machines::delete_machine))
        // Entity CRUD
        .route("/machines/{machine_id}/entities", post(routes::entities::create_entity))
        .route("/machines/{machine_id}/entities", get(routes::entities::list_entities))
        .route("/machines/{machine_id}/entities/{entity_id}", get(routes::entities::get_entity))
        .route("/machines/{machine_id}/entities/{entity_id}", delete(routes::entities::delete_entity))
        // Transitions
        .route("/machines/{machine_id}/entities/{entity_id}/transition", post(routes::transitions::transition_entity))
        .route("/machines/{machine_id}/entities/{entity_id}/history", get(routes::transitions::get_history))
        // Evaluate
        .route("/machines/{machine_id}/evaluate", post(routes::evaluate::evaluate))
        .route("/machines/{machine_id}/evaluate/batch", post(routes::evaluate::evaluate_batch))
        .layer(axum::middleware::from_fn(auth::require_auth))
        .layer(axum::Extension(auth::ApiKey(api_key)));

    let app = Router::new()
        .route("/health", get(handle_health))
        .nest("/api", api_routes)
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(&listen_addr)
        .await
        .expect("Failed to bind to address");

    info!("state-machine listening on {}", listen_addr);

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .expect("Server error");

    info!("state-machine shut down gracefully");
}

async fn handle_health() -> axum::response::Response {
    use axum::response::IntoResponse;
    let body = serde_json::json!({
        "status": "ok",
        "version": env!("CARGO_PKG_VERSION"),
        "service": "state-machine"
    });
    (
        axum::http::StatusCode::OK,
        [("content-type", "application/json")],
        body.to_string(),
    )
        .into_response()
}

async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("Failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("Failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }

    info!("Shutdown signal received, draining connections...");
}
