use serde::Deserialize;
use tracing::{info, warn};

use crate::routes::AppState;

#[derive(Deserialize)]
struct CreateTokenResponse {
    token: String,
}

/// Get or create an ingest token for a tenant.
/// 1. Check in-memory cache
/// 2. Check DB
/// 3. Auto-provision via Platform API, store in DB + cache
/// Returns None if Platform API is not configured or provisioning fails.
pub async fn get_or_create(state: &AppState, tenant_id: &str) -> Option<String> {
    // 1. Check cache
    if let Some(token) = state.ingest_token_cache.get(tenant_id) {
        return Some(token.clone());
    }

    // 2. Check DB
    if let Ok(conn) = state.db.connect() {
        if let Ok(mut rows) = conn
            .query(
                "SELECT token FROM ingest_tokens WHERE tenant_id = ?1",
                libsql::params![tenant_id.to_string()],
            )
            .await
        {
            if let Ok(Some(row)) = rows.next().await {
                if let Ok(token) = row.get::<String>(0) {
                    state
                        .ingest_token_cache
                        .insert(tenant_id.to_string(), token.clone());
                    return Some(token);
                }
            }
        }
    }

    // 3. Auto-provision via Platform API
    if state.platform_api_key.is_empty() || state.platform_api_url.is_empty() {
        warn!(
            "No PLATFORM_API_KEY configured — cannot auto-provision ingest token for tenant '{}'",
            tenant_id
        );
        return None;
    }

    let url = format!("{}/internal/ingest-tokens", state.platform_api_url);
    let resp = state
        .http_client
        .post(&url)
        .header("Authorization", format!("Bearer {}", state.platform_api_key))
        .json(&serde_json::json!({ "key_prefix": tenant_id }))
        .send()
        .await;

    match resp {
        Ok(r) if r.status().is_success() => match r.json::<CreateTokenResponse>().await {
            Ok(body) => {
                let token = body.token;
                info!(
                    "Auto-provisioned ingest token for tenant '{}': pt_{}...",
                    tenant_id,
                    &tenant_id[..tenant_id.len().min(8)]
                );

                // Store in DB
                if let Ok(conn) = state.db.connect() {
                    let now = crate::routes::now_millis();
                    let _ = conn
                        .execute(
                            "INSERT OR REPLACE INTO ingest_tokens (tenant_id, token, created_at) VALUES (?1, ?2, ?3)",
                            libsql::params![tenant_id.to_string(), token.clone(), now],
                        )
                        .await;
                }

                // Store in cache
                state
                    .ingest_token_cache
                    .insert(tenant_id.to_string(), token.clone());

                Some(token)
            }
            Err(e) => {
                warn!(
                    "Failed to parse ingest token response for tenant '{}': {}",
                    tenant_id, e
                );
                None
            }
        },
        Ok(r) => {
            warn!(
                "Platform API returned {} when provisioning ingest token for tenant '{}'",
                r.status(),
                tenant_id
            );
            None
        }
        Err(e) => {
            warn!(
                "Failed to call Platform API for ingest token (tenant '{}'): {}",
                tenant_id, e
            );
            None
        }
    }
}
