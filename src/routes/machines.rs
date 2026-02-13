use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::Json;

use crate::errors::AppError;
use crate::models::*;
use crate::routes::{extract_tenant_id, now_millis, AppState};

/// POST /api/machines — create a machine definition
pub async fn create_machine(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<CreateMachineRequest>,
) -> Result<impl IntoResponse, AppError> {
    let tenant_id = extract_tenant_id(&headers)?;

    let machine = MachineDefinition {
        machine_id: req.machine_id,
        tenant_id: tenant_id.clone(),
        states: req.states,
        initial_state: req.initial_state,
        regions: req.regions,
        joins: req.joins,
        transitions: req.transitions,
        actions: req.actions,
    };

    machine.validate().map_err(AppError::BadRequest)?;

    let definition_json = serde_json::to_string(&machine)?;
    let now = now_millis();

    let conn = state.db.connect()?;
    conn.execute(
        "INSERT INTO machines (machine_id, tenant_id, definition, created_at, updated_at) VALUES (?1, ?2, ?3, ?4, ?5)",
        libsql::params![machine.machine_id.clone(), tenant_id, definition_json, now, now],
    ).await.map_err(|e| {
        if e.to_string().contains("UNIQUE constraint") || e.to_string().contains("PRIMARY KEY") {
            AppError::Conflict(format!("Machine '{}' already exists", machine.machine_id))
        } else {
            AppError::from(e)
        }
    })?;

    // Populate cache
    state.machine_cache.insert(
        (machine.tenant_id.clone(), machine.machine_id.clone()),
        machine.clone(),
    );

    Ok((StatusCode::CREATED, Json(machine)))
}

/// GET /api/machines — list all machines for tenant
pub async fn list_machines(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Vec<MachineDefinition>>, AppError> {
    let tenant_id = extract_tenant_id(&headers)?;
    let conn = state.db.connect()?;

    let mut rows = conn
        .query(
            "SELECT definition FROM machines WHERE tenant_id = ?1 ORDER BY created_at",
            libsql::params![tenant_id],
        )
        .await?;

    let mut machines = Vec::new();
    while let Some(row) = rows.next().await? {
        let def_str: String = row.get(0)?;
        let machine: MachineDefinition = serde_json::from_str(&def_str)?;
        machines.push(machine);
    }

    Ok(Json(machines))
}

/// GET /api/machines/:machine_id — get a machine definition
pub async fn get_machine(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(machine_id): Path<String>,
) -> Result<Json<MachineDefinition>, AppError> {
    let tenant_id = extract_tenant_id(&headers)?;
    let machine = load_machine(&state, &tenant_id, &machine_id).await?;
    Ok(Json(machine))
}

/// PUT /api/machines/:machine_id — update a machine definition
pub async fn update_machine(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(machine_id): Path<String>,
    Json(req): Json<UpdateMachineRequest>,
) -> Result<Json<MachineDefinition>, AppError> {
    let tenant_id = extract_tenant_id(&headers)?;

    // Verify it exists
    let mut machine = load_machine(&state, &tenant_id, &machine_id).await?;

    machine.states = req.states;
    machine.initial_state = req.initial_state;
    machine.regions = req.regions;
    machine.joins = req.joins;
    machine.transitions = req.transitions;
    machine.actions = req.actions;

    machine.validate().map_err(AppError::BadRequest)?;

    let definition_json = serde_json::to_string(&machine)?;
    let now = now_millis();

    let conn = state.db.connect()?;
    conn.execute(
        "UPDATE machines SET definition = ?1, updated_at = ?2 WHERE tenant_id = ?3 AND machine_id = ?4",
        libsql::params![definition_json, now, tenant_id, machine_id],
    ).await?;

    // Update cache
    state.machine_cache.insert(
        (machine.tenant_id.clone(), machine.machine_id.clone()),
        machine.clone(),
    );

    Ok(Json(machine))
}

/// DELETE /api/machines/:machine_id — delete a machine definition
pub async fn delete_machine(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(machine_id): Path<String>,
) -> Result<StatusCode, AppError> {
    let tenant_id = extract_tenant_id(&headers)?;
    let conn = state.db.connect()?;

    // Check for existing entities
    let mut rows = conn
        .query(
            "SELECT COUNT(*) FROM entities WHERE tenant_id = ?1 AND machine_id = ?2",
            libsql::params![tenant_id.clone(), machine_id.clone()],
        )
        .await?;

    if let Some(row) = rows.next().await? {
        let count: i64 = row.get(0)?;
        if count > 0 {
            return Err(AppError::Conflict(format!(
                "Cannot delete machine '{}': {} entities exist. Delete entities first.",
                machine_id, count
            )));
        }
    }

    let affected = conn
        .execute(
            "DELETE FROM machines WHERE tenant_id = ?1 AND machine_id = ?2",
            libsql::params![tenant_id.clone(), machine_id.clone()],
        )
        .await?;

    if affected == 0 {
        return Err(AppError::NotFound(format!("Machine '{}' not found", machine_id)));
    }

    // Invalidate cache
    state.machine_cache.remove(&(tenant_id, machine_id));

    Ok(StatusCode::NO_CONTENT)
}

/// Load a machine definition — cache-first, DB fallback.
/// Cache hit: ~0ns. Cache miss: one DB read + cache populate.
pub async fn load_machine(
    state: &AppState,
    tenant_id: &str,
    machine_id: &str,
) -> Result<MachineDefinition, AppError> {
    let key = (tenant_id.to_string(), machine_id.to_string());

    // Fast path: cache hit (lock-free read via DashMap)
    if let Some(cached) = state.machine_cache.get(&key) {
        return Ok(cached.value().clone());
    }

    // Slow path: DB read + cache populate
    let conn = state.db.connect()?;
    let mut rows = conn
        .query(
            "SELECT definition FROM machines WHERE tenant_id = ?1 AND machine_id = ?2",
            libsql::params![tenant_id.to_string(), machine_id.to_string()],
        )
        .await?;

    match rows.next().await? {
        Some(row) => {
            let def_str: String = row.get(0)?;
            let machine: MachineDefinition = serde_json::from_str(&def_str)?;
            state.machine_cache.insert(key, machine.clone());
            Ok(machine)
        }
        None => Err(AppError::NotFound(format!("Machine '{}' not found", machine_id))),
    }
}
