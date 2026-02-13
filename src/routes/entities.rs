use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::Json;
use std::collections::HashMap;

use crate::errors::AppError;
use crate::models::*;
use crate::routes::machines::load_machine;
use crate::routes::{extract_tenant_id, now_millis, AppState};

/// POST /api/machines/:machine_id/entities — create entity in initial state
pub async fn create_entity(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(machine_id): Path<String>,
    Json(req): Json<CreateEntityRequest>,
) -> Result<impl IntoResponse, AppError> {
    let tenant_id = extract_tenant_id(&headers)?;
    let machine = load_machine(&state, &tenant_id, &machine_id).await?;

    if req.entity_id.is_empty() {
        return Err(AppError::BadRequest("entity_id is required".into()));
    }

    let context = req.context.unwrap_or_default();
    let context_json = serde_json::to_string(&context)?;
    let now = now_millis();

    // Build initial state from machine definition (flat string or JSON map)
    let initial_state_map = machine.initial_state_map();
    let initial_state_encoded = Entity::encode_state(&initial_state_map);

    let conn = state.db.connect()?;
    conn.execute(
        "INSERT INTO entities (machine_id, tenant_id, entity_id, current_state, context, state_version, created_at, updated_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        libsql::params![
            machine_id.clone(),
            tenant_id.clone(),
            req.entity_id.clone(),
            initial_state_encoded.clone(),
            context_json,
            1_i64,
            now,
            now
        ],
    ).await.map_err(|e| {
        if e.to_string().contains("UNIQUE constraint") || e.to_string().contains("PRIMARY KEY") {
            AppError::Conflict(format!("Entity '{}' already exists in machine '{}'", req.entity_id, machine_id))
        } else {
            AppError::from(e)
        }
    })?;

    let entity = Entity {
        machine_id,
        tenant_id,
        entity_id: req.entity_id,
        current_state: initial_state_encoded,
        context,
        state_version: 1,
        created_at: now,
        updated_at: now,
    };

    Ok((StatusCode::CREATED, Json(entity.to_response())))
}

/// GET /api/machines/:machine_id/entities — list entities with optional filters
pub async fn list_entities(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(machine_id): Path<String>,
    Query(query): Query<ListEntitiesQuery>,
) -> Result<Json<Vec<EntityResponse>>, AppError> {
    let tenant_id = extract_tenant_id(&headers)?;
    let conn = state.db.connect()?;

    let mut sql = String::from(
        "SELECT machine_id, tenant_id, entity_id, current_state, context, state_version, created_at, updated_at FROM entities WHERE tenant_id = ?1 AND machine_id = ?2",
    );
    let mut param_idx = 3;

    // For parallel machines with region filter: json_extract(current_state, '$.region') = 'state'
    if let (Some(ref region), Some(ref state_filter)) = (&query.region, &query.state) {
        sql.push_str(&format!(
            " AND json_extract(current_state, '$.{}') = ?{}",
            region, param_idx
        ));
        param_idx += 1;
        // state_filter will be pushed as param below
        let _ = state_filter; // used below
    } else if query.state.is_some() {
        sql.push_str(&format!(" AND current_state = ?{}", param_idx));
        param_idx += 1;
    }
    if query.updated_since.is_some() {
        sql.push_str(&format!(" AND updated_at >= ?{}", param_idx));
    }
    sql.push_str(" ORDER BY created_at");

    // Build params dynamically
    let mut params: Vec<libsql::Value> = vec![
        tenant_id.into(),
        machine_id.into(),
    ];
    if let Some(ref state_filter) = query.state {
        params.push(state_filter.clone().into());
    }
    if let Some(updated_since) = query.updated_since {
        params.push(updated_since.into());
    }

    let mut rows = conn.query(&sql, libsql::params_from_iter(params)).await?;

    let mut entities = Vec::new();
    while let Some(row) = rows.next().await? {
        let entity = row_to_entity(&row)?;
        entities.push(entity.to_response());
    }

    Ok(Json(entities))
}

/// GET /api/machines/:machine_id/entities/:entity_id — get entity
pub async fn get_entity(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((machine_id, entity_id)): Path<(String, String)>,
) -> Result<Json<EntityResponse>, AppError> {
    let tenant_id = extract_tenant_id(&headers)?;
    let entity = load_entity(&state, &tenant_id, &machine_id, &entity_id).await?;
    Ok(Json(entity.to_response()))
}

/// DELETE /api/machines/:machine_id/entities/:entity_id — delete entity
pub async fn delete_entity(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((machine_id, entity_id)): Path<(String, String)>,
) -> Result<StatusCode, AppError> {
    let tenant_id = extract_tenant_id(&headers)?;
    let conn = state.db.connect()?;

    let affected = conn
        .execute(
            "DELETE FROM entities WHERE tenant_id = ?1 AND machine_id = ?2 AND entity_id = ?3",
            libsql::params![tenant_id.clone(), machine_id.clone(), entity_id.clone()],
        )
        .await?;

    if affected == 0 {
        return Err(AppError::NotFound(format!(
            "Entity '{}' not found in machine '{}'",
            entity_id, machine_id
        )));
    }

    // Also clean up transition history
    conn.execute(
        "DELETE FROM transitions WHERE tenant_id = ?1 AND machine_id = ?2 AND entity_id = ?3",
        libsql::params![tenant_id, machine_id, entity_id],
    )
    .await?;

    Ok(StatusCode::NO_CONTENT)
}

/// Load an entity from the database.
pub async fn load_entity(
    state: &AppState,
    tenant_id: &str,
    machine_id: &str,
    entity_id: &str,
) -> Result<Entity, AppError> {
    let conn = state.db.connect()?;

    let mut rows = conn
        .query(
            "SELECT machine_id, tenant_id, entity_id, current_state, context, state_version, created_at, updated_at FROM entities WHERE tenant_id = ?1 AND machine_id = ?2 AND entity_id = ?3",
            libsql::params![tenant_id.to_string(), machine_id.to_string(), entity_id.to_string()],
        )
        .await?;

    match rows.next().await? {
        Some(row) => row_to_entity(&row),
        None => Err(AppError::NotFound(format!(
            "Entity '{}' not found in machine '{}'",
            entity_id, machine_id
        ))),
    }
}

fn row_to_entity(row: &libsql::Row) -> Result<Entity, AppError> {
    let context_str: String = row.get::<String>(4).unwrap_or_else(|_| "{}".to_string());
    let context: HashMap<String, serde_json::Value> =
        serde_json::from_str(&context_str).unwrap_or_default();

    Ok(Entity {
        machine_id: row.get(0)?,
        tenant_id: row.get(1)?,
        entity_id: row.get(2)?,
        current_state: row.get(3)?,
        context,
        state_version: row.get(5)?,
        created_at: row.get(6)?,
        updated_at: row.get(7)?,
    })
}
