pub mod entities;
pub mod evaluate;
pub mod machines;
pub mod transitions;

use dashmap::DashMap;
use libsql::Database;
use reqwest::Client;
use std::sync::Arc;

use crate::models::MachineDefinition;

/// Shared application state injected into every Axum handler.
#[derive(Clone)]
pub struct AppState {
    pub db: Arc<Database>,
    pub http_client: Client,
    pub event_core_ingest_url: String,
    /// In-memory cache: (tenant_id, machine_id) → MachineDefinition
    /// Eliminates DB reads on the hot path for machine lookups.
    pub machine_cache: Arc<DashMap<(String, String), MachineDefinition>>,
}

/// Extract tenant_id from the X-Tenant-Id header.
pub fn extract_tenant_id(headers: &axum::http::HeaderMap) -> Result<String, crate::errors::AppError> {
    headers
        .get("x-tenant-id")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
        .ok_or_else(|| crate::errors::AppError::BadRequest("Missing X-Tenant-Id header".into()))
}

/// Get current time as unix millis.
pub fn now_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}
