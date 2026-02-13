use std::collections::HashMap;

use crate::actions;
use crate::engine;
use crate::errors::AppError;
use crate::models::*;
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
            })
        }
        None => {
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
            })
        }
    }
}
