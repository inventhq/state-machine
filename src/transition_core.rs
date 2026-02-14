use std::collections::HashMap;

use crate::actions;
use crate::engine;
use crate::errors::AppError;
use crate::models::*;
use crate::routes::entities::load_entity;
use crate::routes::machines::load_machine;
use crate::routes::{now_millis, AppState};

/// Shared transition execution logic. Region-aware with join support.
/// When `dispatch` is true, actions fire server-side (webhooks, events).
/// When `dispatch` is false, actions are returned in the response only (plugin-runtime executes them).
pub async fn execute_transition(
    state: &AppState,
    tenant_id: &str,
    machine: &MachineDefinition,
    entity: &Entity,
    event_type: &str,
    params: &HashMap<String, String>,
    timestamp: i64,
    dispatch: bool,
) -> Result<TransitionResponse, AppError> {
    let entity_id = &entity.entity_id;
    let machine_id = &machine.machine_id;
    let prev_state_value = entity.to_response().current_state;

    // Dedup check — single connection reused for all subsequent ops
    let conn = state.db.connect()?;
    let mut rows = conn
        .query(
            "SELECT 1 FROM transitions WHERE tenant_id = ?1 AND machine_id = ?2 AND entity_id = ?3 AND event_type = ?4 AND timestamp = ?5 LIMIT 1",
            libsql::params![tenant_id.to_string(), machine_id.clone(), entity_id.clone(), event_type.to_string(), timestamp],
        )
        .await?;

    if rows.next().await?.is_some() {
        return Ok(TransitionResponse {
            entity_id: entity_id.clone(),
            previous_state: prev_state_value.clone(),
            current_state: prev_state_value,
            transition: None,
            region: None,
            triggered_by: event_type.to_string(),
            actions_dispatched: vec![],
            joins_fired: vec![],
            timestamp,
            reason: Some("duplicate event (already processed)".into()),
            sub_machine: None,
        });
    }

    // Evaluate transition — pure CPU, region-aware
    let result = engine::evaluate(machine, entity, event_type, params);

    match result {
        Some(tr) => {
            // Build new state map: update only the affected region
            let mut new_state_map = entity.state_map();
            new_state_map.insert(tr.region.clone(), tr.to_state.clone());

            // Check and apply join conditions (cascading, max 10 depth)
            let joins_fired = engine::apply_joins(machine, &mut new_state_map, 10);

            // Encode new state
            let new_state_encoded = Entity::encode_state(&new_state_map);

            // Merge event params into entity context
            let mut new_context = entity.context.clone();
            for (k, v) in params {
                new_context.insert(k.clone(), serde_json::Value::String(v.clone()));
            }
            let context_json = serde_json::to_string(&new_context)?;
            let now = now_millis();

            // Determine region for display (None for flat FSMs)
            let display_region = if machine.is_parallel() {
                Some(tr.region.clone())
            } else {
                None
            };

            // Collect actions for the direct transition
            let action_configs =
                actions::collect_actions_for_state(&machine.actions, &tr.to_state, display_region.as_deref());

            // Also collect actions for any joins that fired
            let mut join_action_configs = Vec::new();
            for jf in &joins_fired {
                let ja = actions::collect_actions_for_state(
                    &machine.actions,
                    &jf.target_state,
                    Some(&jf.target_region),
                );
                join_action_configs.extend(ja);
            }

            // Collect join-specific actions from the JoinDef itself
            for jf in &joins_fired {
                for join_def in &machine.joins {
                    if join_def.target_region == jf.target_region
                        && join_def.target_state == jf.target_state
                    {
                        for a in &join_def.actions {
                            join_action_configs.push((a.on_enter.clone(), a.action.clone(), Some(jf.target_region.clone())));
                        }
                    }
                }
            }

            let mut all_action_configs = action_configs;
            all_action_configs.extend(join_action_configs);

            // Build structured action list (always returned in response).
            // Only fire server-side if dispatch=true (default for non-plugin-runtime callers).
            let dispatched = if dispatch {
                actions::dispatch_actions(
                    &state.http_client,
                    &state.event_core_ingest_url,
                    all_action_configs,
                    tenant_id,
                    machine_id,
                    entity_id,
                    &tr.from_state,
                    &tr.to_state,
                )
            } else {
                actions::build_action_list(all_action_configs)
            };

            let dispatched_json = serde_json::to_string(&dispatched)?;
            let params_json = serde_json::to_string(params)?;

            // Optimistic locking: only update if state_version matches
            let affected = conn.execute(
                "UPDATE entities SET current_state = ?1, context = ?2, state_version = state_version + 1, updated_at = ?3 WHERE tenant_id = ?4 AND machine_id = ?5 AND entity_id = ?6 AND state_version = ?7",
                libsql::params![new_state_encoded, context_json, now, tenant_id.to_string(), machine_id.clone(), entity_id.clone(), entity.state_version],
            ).await?;

            if affected == 0 {
                return Err(AppError::Conflict(
                    "Concurrent modification detected — retry the transition".into(),
                ));
            }

            // Build transition label
            let transition_label = if machine.is_parallel() {
                format!("{}: {} → {}", tr.region, tr.from_state, tr.to_state)
            } else {
                format!("{} → {}", tr.from_state, tr.to_state)
            };

            // Record transition history
            let region_str = display_region.as_deref().unwrap_or("");
            conn.execute(
                "INSERT INTO transitions (tenant_id, machine_id, entity_id, from_state, to_state, event_type, event_params, actions_dispatched, region, timestamp, created_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
                libsql::params![
                    tenant_id.to_string(),
                    machine_id.clone(),
                    entity_id.clone(),
                    tr.from_state.clone(),
                    tr.to_state.clone(),
                    event_type.to_string(),
                    params_json,
                    dispatched_json,
                    region_str.to_string(),
                    timestamp,
                    now
                ],
            ).await?;

            // Record join transitions in history too
            for jf in &joins_fired {
                let _ = conn.execute(
                    "INSERT INTO transitions (tenant_id, machine_id, entity_id, from_state, to_state, event_type, event_params, actions_dispatched, region, timestamp, created_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
                    libsql::params![
                        tenant_id.to_string(),
                        machine_id.clone(),
                        entity_id.clone(),
                        jf.from_state.clone(),
                        jf.target_state.clone(),
                        "$join".to_string(),
                        "{}".to_string(),
                        "[]".to_string(),
                        jf.target_region.clone(),
                        timestamp,
                        now
                    ],
                ).await;
            }

            let new_state_value = state_map_to_value(&new_state_map);

            Ok(TransitionResponse {
                entity_id: entity_id.clone(),
                previous_state: prev_state_value,
                current_state: new_state_value,
                transition: Some(transition_label),
                region: display_region,
                triggered_by: event_type.to_string(),
                actions_dispatched: dispatched,
                joins_fired,
                timestamp,
                reason: None,
                sub_machine: None,
            })
        }
        None => {
            // Try sub-machine forwarding before giving up
            if let Some(resp) = try_sub_machine_forward(
                state, tenant_id, machine, entity, event_type, params, timestamp, dispatch,
            ).await? {
                return Ok(resp);
            }

            let state_desc = if machine.is_parallel() {
                format!("{:?}", entity.state_map())
            } else {
                entity.current_state.clone()
            };
            Ok(TransitionResponse {
                entity_id: entity_id.clone(),
                previous_state: prev_state_value.clone(),
                current_state: prev_state_value,
                transition: None,
                region: None,
                triggered_by: event_type.to_string(),
                actions_dispatched: vec![],
                joins_fired: vec![],
                timestamp,
                reason: Some(format!(
                    "no transition from '{}' on '{}'",
                    state_desc, event_type
                )),
                sub_machine: None,
            })
        }
    }
}

// ── Sub-machine runtime ─────────────────────────────────────────────────────

/// Try to forward an event to an active sub-machine.
/// Returns Some(response) if a sub-machine handled the event, None otherwise.
async fn try_sub_machine_forward(
    state: &AppState,
    tenant_id: &str,
    machine: &MachineDefinition,
    entity: &Entity,
    event_type: &str,
    params: &HashMap<String, String>,
    timestamp: i64,
    dispatch: bool,
) -> Result<Option<TransitionResponse>, AppError> {
    let state_map = entity.state_map();

    // Collect active compound states: (parent_state, region, sub_def)
    let compound_states: Vec<(&str, &str, &SubMachineDef)> = if !machine.is_parallel() {
        let current = state_map.get(DEFAULT_REGION).map(|s| s.as_str()).unwrap_or("");
        machine.sub_machines.get(current)
            .map(|sd| vec![(current, DEFAULT_REGION, sd)])
            .unwrap_or_default()
    } else {
        machine.regions.iter()
            .filter_map(|r| {
                let current = state_map.get(&r.id)?;
                r.sub_machines.get(current.as_str())
                    .map(|sd| (current.as_str(), r.id.as_str(), sd))
            })
            .collect()
    };

    for (parent_state, region, sub_def) in compound_states {
        match handle_sub_machine_event(
            state, tenant_id, machine, entity, sub_def,
            parent_state, region, event_type, params, timestamp, dispatch,
        ).await {
            Ok(Some(resp)) => return Ok(Some(resp)),
            Ok(None) => continue,
            Err(e) => return Err(e),
        }
    }

    Ok(None)
}

/// Handle a single sub-machine event forwarding.
/// Returns Some(response) if the child handled the event, None if child had no matching transition.
async fn handle_sub_machine_event(
    state: &AppState,
    tenant_id: &str,
    parent_machine: &MachineDefinition,
    parent_entity: &Entity,
    sub_def: &SubMachineDef,
    parent_state: &str,
    parent_region: &str,
    event_type: &str,
    params: &HashMap<String, String>,
    timestamp: i64,
    dispatch: bool,
) -> Result<Option<TransitionResponse>, AppError> {
    let child_machine = load_machine(state, tenant_id, &sub_def.machine_id).await?;
    let child_entity_id = format!("{}::sub::{}", parent_entity.entity_id, parent_state);

    // Load or create child entity (auto-create on first access)
    let child_entity = load_or_create_child_entity(
        state, tenant_id, &sub_def.machine_id, &child_entity_id, &child_machine,
    ).await?;

    // Check if child is already in a final state (recovery from previous failed auto-advance)
    let child_current = flat_state(&child_entity);
    if let Some(parent_target) = sub_def.on_final.get(&child_current) {
        let resp = auto_advance_parent(
            state, tenant_id, parent_machine, parent_entity,
            parent_state, parent_region, parent_target,
            &child_entity, sub_def, event_type, timestamp, dispatch,
        ).await?;
        return Ok(Some(resp));
    }

    // Forward event to child machine (Box::pin for async recursion)
    let child_resp = Box::pin(execute_transition(
        state, tenant_id, &child_machine, &child_entity,
        event_type, params, timestamp, dispatch,
    )).await?;

    // If child had no matching transition, this sub-machine didn't handle the event
    if child_resp.transition.is_none() && child_resp.sub_machine.is_none() {
        return Ok(None);
    }

    // Child transitioned — check if it reached a final state
    let child_new_state = child_resp.current_state.as_str().unwrap_or("").to_string();
    let auto_completed = sub_def.on_final.contains_key(&child_new_state);

    let sub_info = SubMachineTransition {
        machine_id: sub_def.machine_id.clone(),
        entity_id: child_entity_id,
        previous_state: child_resp.previous_state.clone(),
        current_state: child_resp.current_state.clone(),
        transition: child_resp.transition.clone(),
        auto_completed,
    };

    if auto_completed {
        let parent_target = sub_def.on_final.get(&child_new_state).unwrap();

        // Auto-advance parent
        let parent_resp = advance_parent_state(
            state, tenant_id, parent_machine, parent_entity,
            parent_state, parent_region, parent_target,
            event_type, timestamp, dispatch,
        ).await?;

        Ok(Some(TransitionResponse {
            entity_id: parent_entity.entity_id.clone(),
            previous_state: parent_resp.0,
            current_state: parent_resp.1,
            transition: parent_resp.2,
            region: parent_resp.3,
            triggered_by: event_type.to_string(),
            actions_dispatched: parent_resp.4,
            joins_fired: parent_resp.5,
            timestamp,
            reason: None,
            sub_machine: Some(sub_info),
        }))
    } else {
        // Child transitioned but didn't complete — parent stays in compound state
        let parent_state_value = parent_entity.to_response().current_state;
        Ok(Some(TransitionResponse {
            entity_id: parent_entity.entity_id.clone(),
            previous_state: parent_state_value.clone(),
            current_state: parent_state_value,
            transition: None,
            region: if parent_machine.is_parallel() { Some(parent_region.to_string()) } else { None },
            triggered_by: event_type.to_string(),
            actions_dispatched: child_resp.actions_dispatched,
            joins_fired: vec![],
            timestamp,
            reason: None,
            sub_machine: Some(sub_info),
        }))
    }
}

/// Auto-advance the parent when child is already in a final state (recovery path).
async fn auto_advance_parent(
    state: &AppState,
    tenant_id: &str,
    parent_machine: &MachineDefinition,
    parent_entity: &Entity,
    parent_state: &str,
    parent_region: &str,
    parent_target: &str,
    child_entity: &Entity,
    sub_def: &SubMachineDef,
    event_type: &str,
    timestamp: i64,
    dispatch: bool,
) -> Result<TransitionResponse, AppError> {
    let child_state_value = child_entity.to_response().current_state;

    let sub_info = SubMachineTransition {
        machine_id: sub_def.machine_id.clone(),
        entity_id: child_entity.entity_id.clone(),
        previous_state: child_state_value.clone(),
        current_state: child_state_value,
        transition: None,
        auto_completed: true,
    };

    let parent_resp = advance_parent_state(
        state, tenant_id, parent_machine, parent_entity,
        parent_state, parent_region, parent_target,
        event_type, timestamp, dispatch,
    ).await?;

    Ok(TransitionResponse {
        entity_id: parent_entity.entity_id.clone(),
        previous_state: parent_resp.0,
        current_state: parent_resp.1,
        transition: parent_resp.2,
        region: parent_resp.3,
        triggered_by: event_type.to_string(),
        actions_dispatched: parent_resp.4,
        joins_fired: parent_resp.5,
        timestamp,
        reason: None,
        sub_machine: Some(sub_info),
    })
}

/// Advance parent entity state (used when child completes).
/// Returns (prev_state, new_state, transition_label, region, dispatched_actions, joins_fired).
#[allow(clippy::type_complexity)]
async fn advance_parent_state(
    state: &AppState,
    tenant_id: &str,
    machine: &MachineDefinition,
    entity: &Entity,
    from_state: &str,
    region: &str,
    to_state: &str,
    _triggered_by: &str,
    timestamp: i64,
    dispatch: bool,
) -> Result<(
    serde_json::Value,
    serde_json::Value,
    Option<String>,
    Option<String>,
    Vec<DispatchedAction>,
    Vec<JoinFired>,
), AppError> {
    let prev_state_value = entity.to_response().current_state;

    let mut new_state_map = entity.state_map();
    new_state_map.insert(region.to_string(), to_state.to_string());

    let joins_fired = engine::apply_joins(machine, &mut new_state_map, 10);
    let new_state_encoded = Entity::encode_state(&new_state_map);
    let context_json = serde_json::to_string(&entity.context)?;
    let now = now_millis();

    let display_region = if machine.is_parallel() {
        Some(region.to_string())
    } else {
        None
    };

    // Collect actions for entering new parent state
    let mut all_action_configs =
        actions::collect_actions_for_state(&machine.actions, to_state, display_region.as_deref());

    // Collect actions for joins
    for jf in &joins_fired {
        let ja = actions::collect_actions_for_state(
            &machine.actions, &jf.target_state, Some(&jf.target_region),
        );
        all_action_configs.extend(ja);
    }
    for jf in &joins_fired {
        for join_def in &machine.joins {
            if join_def.target_region == jf.target_region && join_def.target_state == jf.target_state {
                for a in &join_def.actions {
                    all_action_configs.push((a.on_enter.clone(), a.action.clone(), Some(jf.target_region.clone())));
                }
            }
        }
    }

    let dispatched = if dispatch {
        actions::dispatch_actions(
            &state.http_client, &state.event_core_ingest_url,
            all_action_configs, tenant_id, &machine.machine_id,
            &entity.entity_id, from_state, to_state,
        )
    } else {
        actions::build_action_list(all_action_configs)
    };

    let dispatched_json = serde_json::to_string(&dispatched)?;

    // Optimistic locking
    let conn = state.db.connect()?;
    let affected = conn.execute(
        "UPDATE entities SET current_state = ?1, context = ?2, state_version = state_version + 1, updated_at = ?3 WHERE tenant_id = ?4 AND machine_id = ?5 AND entity_id = ?6 AND state_version = ?7",
        libsql::params![new_state_encoded, context_json, now, tenant_id.to_string(), machine.machine_id.clone(), entity.entity_id.clone(), entity.state_version],
    ).await?;

    if affected == 0 {
        return Err(AppError::Conflict(
            "Concurrent modification detected — retry the transition".into(),
        ));
    }

    let transition_label = if machine.is_parallel() {
        format!("{}: {} → {}", region, from_state, to_state)
    } else {
        format!("{} → {}", from_state, to_state)
    };

    // Record parent auto-advance in audit log
    let region_str = display_region.as_deref().unwrap_or("");
    conn.execute(
        "INSERT INTO transitions (tenant_id, machine_id, entity_id, from_state, to_state, event_type, event_params, actions_dispatched, region, timestamp, created_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
        libsql::params![
            tenant_id.to_string(),
            machine.machine_id.clone(),
            entity.entity_id.clone(),
            from_state.to_string(),
            to_state.to_string(),
            "$sub_complete".to_string(),
            "{}".to_string(),
            dispatched_json,
            region_str.to_string(),
            timestamp,
            now
        ],
    ).await?;

    // Record join transitions
    for jf in &joins_fired {
        let _ = conn.execute(
            "INSERT INTO transitions (tenant_id, machine_id, entity_id, from_state, to_state, event_type, event_params, actions_dispatched, region, timestamp, created_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
            libsql::params![
                tenant_id.to_string(),
                machine.machine_id.clone(),
                entity.entity_id.clone(),
                jf.from_state.clone(),
                jf.target_state.clone(),
                "$join".to_string(),
                "{}".to_string(),
                "[]".to_string(),
                jf.target_region.clone(),
                timestamp,
                now
            ],
        ).await;
    }

    let new_state_value = state_map_to_value(&new_state_map);

    Ok((
        prev_state_value,
        new_state_value,
        Some(transition_label),
        display_region,
        dispatched,
        joins_fired,
    ))
}

// ── Child entity helpers ────────────────────────────────────────────────────

/// Load or create a child entity for a sub-machine.
async fn load_or_create_child_entity(
    state: &AppState,
    tenant_id: &str,
    child_machine_id: &str,
    child_entity_id: &str,
    child_machine: &MachineDefinition,
) -> Result<Entity, AppError> {
    match load_entity(state, tenant_id, child_machine_id, child_entity_id).await {
        Ok(entity) => Ok(entity),
        Err(AppError::NotFound(_)) => {
            create_child_entity(state, tenant_id, child_machine_id, child_entity_id, child_machine).await?;
            load_entity(state, tenant_id, child_machine_id, child_entity_id).await
        }
        Err(e) => Err(e),
    }
}

/// Create (or reset) a child entity in a sub-machine.
async fn create_child_entity(
    state: &AppState,
    tenant_id: &str,
    child_machine_id: &str,
    child_entity_id: &str,
    child_machine: &MachineDefinition,
) -> Result<(), AppError> {
    let initial_state_map = child_machine.initial_state_map();
    let initial_state_encoded = Entity::encode_state(&initial_state_map);
    let now = now_millis();

    let conn = state.db.connect()?;
    conn.execute(
        "INSERT OR REPLACE INTO entities (machine_id, tenant_id, entity_id, current_state, context, state_version, created_at, updated_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        libsql::params![
            child_machine_id.to_string(),
            tenant_id.to_string(),
            child_entity_id.to_string(),
            initial_state_encoded,
            "{}".to_string(),
            1_i64,
            now,
            now
        ],
    ).await?;

    Ok(())
}

/// Get the flat state of an entity (for sub-machine final state checks).
fn flat_state(entity: &Entity) -> String {
    if entity.is_parallel() {
        entity.current_state.clone()
    } else {
        entity.current_state.clone()
    }
}
