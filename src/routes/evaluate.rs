use axum::extract::{Path, State};
use axum::http::HeaderMap;
use axum::Json;
use serde::{Deserialize, Serialize};

use crate::errors::AppError;
use crate::models::*;
use crate::routes::entities::load_entity;
use crate::routes::machines::load_machine;
use crate::routes::{extract_tenant_id, now_millis, AppState};
use crate::transition_core;

/// POST /api/machines/:machine_id/evaluate
///
/// Single-event evaluate. Resolves entity from params via entity_key.
pub async fn evaluate(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(machine_id): Path<String>,
    Json(req): Json<EvaluateRequest>,
) -> Result<Json<TransitionResponse>, AppError> {
    let tenant_id = resolve_tenant(&headers, &req.params)?;
    let entity_id = resolve_entity_id(&req.entity_key, &req.params)?;

    let machine = load_machine(&state, &tenant_id, &machine_id).await?;
    let entity = load_or_create_entity(&state, &tenant_id, &machine_id, &entity_id, &machine).await?;
    let timestamp = req.timestamp.unwrap_or_else(now_millis);

    let resp = transition_core::execute_transition(
        &state, &tenant_id, &machine, &entity, &req.event_type, &req.params, timestamp, req.dispatch,
    )
    .await?;

    Ok(Json(resp))
}

/// POST /api/machines/:machine_id/evaluate/batch
///
/// Process multiple events in parallel. Each event is resolved and evaluated
/// independently. Returns results for all events, including individual errors.
pub async fn evaluate_batch(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(machine_id): Path<String>,
    Json(req): Json<BatchEvaluateRequest>,
) -> Result<Json<BatchEvaluateResponse>, AppError> {
    if req.events.is_empty() {
        return Err(AppError::BadRequest("events array is empty".into()));
    }
    if req.events.len() > 1000 {
        return Err(AppError::BadRequest("batch size exceeds maximum of 1000".into()));
    }

    // Load machine once (from cache — ~0ns after first call)
    let header_tenant = extract_tenant_id(&headers).ok();
    let machine = {
        let tenant_id = header_tenant.as_deref().or_else(|| {
            req.events.first().and_then(|e| e.params.get("key_prefix").map(|s| s.as_str()))
        }).ok_or_else(|| AppError::BadRequest("Missing tenant: provide X-Tenant-Id header or key_prefix in params".into()))?;
        load_machine(&state, tenant_id, &machine_id).await?
    };

    // Spawn parallel tasks for each event
    let mut handles = Vec::with_capacity(req.events.len());

    for event in req.events {
        let state = state.clone();
        let machine = machine.clone();
        let machine_id = machine_id.clone();
        let header_tenant = header_tenant.clone();

        handles.push(tokio::spawn(async move {
            let tenant_id = match header_tenant {
                Some(t) => t,
                None => match event.params.get("key_prefix") {
                    Some(t) => t.clone(),
                    None => return BatchEventResult {
                        entity_id: None,
                        result: None,
                        error: Some("Missing tenant: provide X-Tenant-Id header or key_prefix in params".into()),
                    },
                },
            };

            let entity_id = match event.params.get(&event.entity_key) {
                Some(id) => id.clone(),
                None => return BatchEventResult {
                    entity_id: None,
                    result: None,
                    error: Some(format!("entity_key '{}' not found in params", event.entity_key)),
                },
            };

            let entity = match load_or_create_entity(&state, &tenant_id, &machine_id, &entity_id, &machine).await {
                Ok(e) => e,
                Err(e) => return BatchEventResult {
                    entity_id: Some(entity_id),
                    result: None,
                    error: Some(format!("{}", e)),
                },
            };

            let timestamp = event.timestamp.unwrap_or_else(now_millis);

            match transition_core::execute_transition(
                &state, &tenant_id, &machine, &entity, &event.event_type, &event.params, timestamp, event.dispatch,
            ).await {
                Ok(resp) => BatchEventResult {
                    entity_id: Some(resp.entity_id.clone()),
                    result: Some(resp),
                    error: None,
                },
                Err(e) => BatchEventResult {
                    entity_id: Some(entity_id),
                    result: None,
                    error: Some(format!("{}", e)),
                },
            }
        }));
    }

    // Await all results
    let mut results = Vec::with_capacity(handles.len());
    for handle in handles {
        match handle.await {
            Ok(r) => results.push(r),
            Err(e) => results.push(BatchEventResult {
                entity_id: None,
                result: None,
                error: Some(format!("Task panicked: {}", e)),
            }),
        }
    }

    let total = results.len();
    let succeeded = results.iter().filter(|r| r.result.is_some()).count();
    let failed = total - succeeded;

    Ok(Json(BatchEvaluateResponse {
        total,
        succeeded,
        failed,
        results,
    }))
}

fn resolve_tenant(
    headers: &HeaderMap,
    params: &std::collections::HashMap<String, String>,
) -> Result<String, AppError> {
    extract_tenant_id(headers).or_else(|_| {
        params
            .get("key_prefix")
            .cloned()
            .ok_or_else(|| AppError::BadRequest("Missing tenant: provide X-Tenant-Id header or key_prefix in params".into()))
    })
}

fn resolve_entity_id(
    entity_key: &str,
    params: &std::collections::HashMap<String, String>,
) -> Result<String, AppError> {
    params
        .get(entity_key)
        .cloned()
        .ok_or_else(|| AppError::BadRequest(format!("entity_key '{}' not found in params", entity_key)))
}

/// Load entity or auto-create it in the machine's initial state (upsert).
/// This lets the plugin-runtime call evaluate without a prior create call.
async fn load_or_create_entity(
    state: &AppState,
    tenant_id: &str,
    machine_id: &str,
    entity_id: &str,
    machine: &MachineDefinition,
) -> Result<Entity, AppError> {
    match load_entity(state, tenant_id, machine_id, entity_id).await {
        Ok(entity) => Ok(entity),
        Err(AppError::NotFound(_)) => {
            let now = now_millis();
            let initial_state_map = machine.initial_state_map();
            let initial_state_encoded = Entity::encode_state(&initial_state_map);
            let context_json = "{}";

            let conn = state.db.connect()?;
            // INSERT OR IGNORE handles race conditions — two concurrent creates for the same entity
            conn.execute(
                "INSERT OR IGNORE INTO entities (machine_id, tenant_id, entity_id, current_state, context, state_version, created_at, updated_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                libsql::params![
                    machine_id.to_string(),
                    tenant_id.to_string(),
                    entity_id.to_string(),
                    initial_state_encoded,
                    context_json.to_string(),
                    1_i64,
                    now,
                    now
                ],
            ).await?;

            // Always re-load to get the canonical row (handles race with INSERT OR IGNORE)
            load_entity(state, tenant_id, machine_id, entity_id).await
        }
        Err(e) => Err(e),
    }
}

// ── Batch types ─────────────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct BatchEvaluateRequest {
    pub events: Vec<EvaluateRequest>,
}

#[derive(Debug, Serialize)]
pub struct BatchEvaluateResponse {
    pub total: usize,
    pub succeeded: usize,
    pub failed: usize,
    pub results: Vec<BatchEventResult>,
}

#[derive(Debug, Serialize)]
pub struct BatchEventResult {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub entity_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<TransitionResponse>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}
