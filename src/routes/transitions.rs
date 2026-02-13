use axum::extract::{Path, State};
use axum::http::HeaderMap;
use axum::Json;
use std::collections::HashMap;

use crate::errors::AppError;
use crate::models::*;
use crate::routes::entities::load_entity;
use crate::routes::machines::load_machine;
use crate::routes::{extract_tenant_id, now_millis, AppState};
use crate::transition_core;

/// POST /api/machines/:machine_id/entities/:entity_id/transition
pub async fn transition_entity(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((machine_id, entity_id)): Path<(String, String)>,
    Json(req): Json<TransitionRequest>,
) -> Result<Json<TransitionResponse>, AppError> {
    let tenant_id = extract_tenant_id(&headers)?;
    let machine = load_machine(&state, &tenant_id, &machine_id).await?;
    let entity = load_entity(&state, &tenant_id, &machine_id, &entity_id).await?;
    let timestamp = req.timestamp.unwrap_or_else(now_millis);

    let resp = transition_core::execute_transition(
        &state, &tenant_id, &machine, &entity, &req.event_type, &req.params, timestamp, true,
    )
    .await?;

    Ok(Json(resp))
}

/// GET /api/machines/:machine_id/entities/:entity_id/history
pub async fn get_history(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((machine_id, entity_id)): Path<(String, String)>,
) -> Result<Json<Vec<TransitionRecord>>, AppError> {
    let tenant_id = extract_tenant_id(&headers)?;
    let conn = state.db.connect()?;

    let mut rows = conn
        .query(
            "SELECT id, tenant_id, machine_id, entity_id, from_state, to_state, event_type, event_params, actions_dispatched, region, timestamp, created_at FROM transitions WHERE tenant_id = ?1 AND machine_id = ?2 AND entity_id = ?3 ORDER BY timestamp ASC",
            libsql::params![tenant_id, machine_id, entity_id],
        )
        .await?;

    let mut records = Vec::new();
    while let Some(row) = rows.next().await? {
        let params_str: String = row.get::<String>(7).unwrap_or_else(|_| "null".to_string());
        let actions_str: String = row.get::<String>(8).unwrap_or_else(|_| "[]".to_string());

        let event_params: Option<HashMap<String, String>> =
            serde_json::from_str(&params_str).ok();
        let actions_dispatched: Vec<String> =
            serde_json::from_str(&actions_str).unwrap_or_default();

        let region_str: String = row.get::<String>(9).unwrap_or_default();
        let region = if region_str.is_empty() { None } else { Some(region_str) };

        records.push(TransitionRecord {
            id: row.get(0)?,
            tenant_id: row.get(1)?,
            machine_id: row.get(2)?,
            entity_id: row.get(3)?,
            from_state: row.get(4)?,
            to_state: row.get(5)?,
            event_type: row.get(6)?,
            event_params,
            actions_dispatched,
            region,
            timestamp: row.get(10)?,
            created_at: row.get(11)?,
        });
    }

    Ok(Json(records))
}
