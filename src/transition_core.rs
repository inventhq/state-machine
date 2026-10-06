use std::collections::HashMap;

use libsql::{Connection, TransactionBehavior};

use crate::actions;
use crate::engine::{self, RegionEntered};
use crate::errors::{AppError, EVENT_IDENTITY_CONFLICT, UNCORRELATED_CHILD_FINAL};
use crate::ingest_tokens;
use crate::models::*;
use crate::routes::entities::load_entity_on;
use crate::routes::machines::load_machine;
use crate::routes::{now_millis, AppState};

/// Actions of one committed write. They are fired only after the transaction commits.
pub struct PendingDispatch {
    pub machine_id: String,
    pub entity_id: String,
    pub from_state: String,
    pub to_state: String,
    pub actions: Vec<(String, ActionConfig, Option<String>)>,
}

/// Shared transition execution logic. Region-aware with join support.
/// When `dispatch` is true, actions fire server-side (webhooks, events).
/// When `dispatch` is false, actions are returned in the response only (plugin-runtime executes them).
///
/// Atomicity (E1): the entity is re-read, deduplicated, evaluated and written in one IMMEDIATE
/// transaction: state, context, version, region entries, the history row and its `$join` rows,
/// plus any forwarded child write and the parent's `$sub_complete`. Any failure rolls all of it
/// back. Network effects (ingest-token provisioning, action dispatch) run only after commit.
#[allow(clippy::too_many_arguments)]
pub async fn execute_transition(
    state: &AppState,
    tenant_id: &str,
    machine: &MachineDefinition,
    entity_id: &str,
    event_type: &str,
    params: &HashMap<String, String>,
    timestamp: i64,
    dispatch: bool,
) -> Result<TransitionResponse, AppError> {
    let conn = state.db.connect()?;
    let tx = conn
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .await?;
    let mut pending = Vec::new();
    // On error the transaction is dropped, which rolls it back.
    let resp = transition_in_tx(
        &tx, state, tenant_id, machine, entity_id, event_type, params, timestamp, &mut pending,
    )
    .await?;
    tx.commit().await?;

    if dispatch && !pending.is_empty() {
        dispatch_committed(state, tenant_id, pending).await;
    }
    Ok(resp)
}

/// Fire the actions of committed writes (never called for a rolled-back transaction).
async fn dispatch_committed(state: &AppState, tenant_id: &str, pending: Vec<PendingDispatch>) {
    let token = ingest_tokens::get_or_create(state, tenant_id).await;
    for p in pending {
        actions::dispatch_actions(
            &state.http_client,
            &state.event_core_ingest_url,
            p.actions,
            tenant_id,
            &p.machine_id,
            &p.entity_id,
            &p.from_state,
            &p.to_state,
            token.as_deref(),
        );
    }
}

#[allow(clippy::too_many_arguments)]
async fn transition_in_tx(
    tx: &Connection,
    state: &AppState,
    tenant_id: &str,
    machine: &MachineDefinition,
    entity_id: &str,
    event_type: &str,
    params: &HashMap<String, String>,
    timestamp: i64,
    pending: &mut Vec<PendingDispatch>,
) -> Result<TransitionResponse, AppError> {
    let machine_id = &machine.machine_id;
    let entity = load_entity_on(tx, tenant_id, machine_id, entity_id).await?;
    let prev_state_value = entity.to_response().current_state;

    // Dedup over every committed row with this (event_type, timestamp), legacy rows included.
    // Equal params is a duplicate; different params under the same key is a conflict.
    let mut rows = tx
        .query(
            "SELECT event_params FROM transitions WHERE tenant_id = ?1 AND machine_id = ?2 AND entity_id = ?3 AND event_type = ?4 AND timestamp = ?5",
            libsql::params![tenant_id.to_string(), machine_id.clone(), entity_id.to_string(), event_type.to_string(), timestamp],
        )
        .await?;
    let (mut seen, mut same) = (false, false);
    while let Some(row) = rows.next().await? {
        seen = true;
        let stored = row.get::<Option<String>>(0).ok().flatten();
        if engine::same_event_params(stored.as_deref(), params) {
            same = true;
            break;
        }
    }
    drop(rows);

    if same {
        return Ok(TransitionResponse {
            entity_id: entity_id.to_string(),
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
    if seen {
        return Err(AppError::Rejected {
            code: EVENT_IDENTITY_CONFLICT,
            message: format!(
                "event '{}' at timestamp {} was already committed for entity '{}' with different params",
                event_type, timestamp, entity_id
            ),
        });
    }

    // Evaluate transition — pure CPU, region-aware
    let result = engine::evaluate(machine, &entity, event_type, params);

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
            let version = entity.state_version + 1;

            // Determine region for display (None for flat FSMs)
            let display_region = if machine.is_parallel() {
                Some(tr.region.clone())
            } else {
                None
            };

            let all_action_configs =
                collect_write_actions(machine, &tr.to_state, display_region.as_deref(), &joins_fired);
            // Built now, fired after commit (dispatch_committed).
            let dispatched = actions::build_action_list(all_action_configs.clone());

            // Record transition history (and joins) before the guarded UPDATE, in the same transaction
            let key = engine::event_identity(event_type, timestamp);
            let row = insert_history(
                tx,
                tenant_id,
                machine_id,
                entity_id,
                HistoryRow {
                    from_state: &tr.from_state,
                    to_state: &tr.to_state,
                    event_type,
                    event_params: serde_json::to_string(params)?,
                    actions_dispatched: serde_json::to_string(&dispatched)?,
                    region: display_region.as_deref().unwrap_or(""),
                    timestamp,
                    now,
                    identity_key: &key,
                    cause: None,
                },
            )
            .await?;
            let mut entered = vec![RegionEntered {
                region: tr.region.clone(),
                state: tr.to_state.clone(),
                row,
                key: key.clone(),
            }];
            entered.extend(
                insert_join_rows(tx, tenant_id, machine_id, entity_id, &joins_fired, version, &key, timestamp, now)
                    .await?,
            );

            // Optimistic locking: only update if state_version matches
            let entries = engine::advance_region_entries(
                &engine::effective_region_entries(&entity),
                &new_state_map,
                &entered,
                version,
                now,
            );
            update_entity(
                tx,
                tenant_id,
                machine_id,
                entity_id,
                &new_state_encoded,
                Some(&context_json),
                &entries,
                entity.state_version,
                now,
            )
            .await?;

            pending.push(PendingDispatch {
                machine_id: machine_id.clone(),
                entity_id: entity_id.to_string(),
                from_state: tr.from_state.clone(),
                to_state: tr.to_state.clone(),
                actions: all_action_configs,
            });

            // Build transition label
            let transition_label = if machine.is_parallel() {
                format!("{}: {} → {}", tr.region, tr.from_state, tr.to_state)
            } else {
                format!("{} → {}", tr.from_state, tr.to_state)
            };

            let new_state_value = state_map_to_value(&new_state_map);

            Ok(TransitionResponse {
                entity_id: entity_id.to_string(),
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
                tx, state, tenant_id, machine, &entity, event_type, params, timestamp, pending,
            ).await? {
                return Ok(resp);
            }

            let state_desc = if machine.is_parallel() {
                format!("{:?}", entity.state_map())
            } else {
                entity.current_state.clone()
            };
            Ok(TransitionResponse {
                entity_id: entity_id.to_string(),
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

/// Actions for entering `to_state` plus every fired join's target state and join actions.
fn collect_write_actions(
    machine: &MachineDefinition,
    to_state: &str,
    display_region: Option<&str>,
    joins_fired: &[JoinFired],
) -> Vec<(String, ActionConfig, Option<String>)> {
    let mut all_action_configs =
        actions::collect_actions_for_state(&machine.actions, to_state, display_region);

    // Also collect actions for any joins that fired
    for jf in joins_fired {
        all_action_configs.extend(actions::collect_actions_for_state(
            &machine.actions,
            &jf.target_state,
            Some(&jf.target_region),
        ));
    }

    // Collect join-specific actions from the JoinDef itself
    for jf in joins_fired {
        for join_def in &machine.joins {
            if join_def.target_region == jf.target_region && join_def.target_state == jf.target_state {
                for a in &join_def.actions {
                    all_action_configs.push((a.on_enter.clone(), a.action.clone(), Some(jf.target_region.clone())));
                }
            }
        }
    }
    all_action_configs
}

// ── Transactional write helpers (also used by the scheduler) ────────────────

pub(crate) struct HistoryRow<'a> {
    pub from_state: &'a str,
    pub to_state: &'a str,
    pub event_type: &'a str,
    pub event_params: String,
    pub actions_dispatched: String,
    pub region: &'a str,
    pub timestamp: i64,
    pub now: i64,
    pub identity_key: &'a str,
    pub cause: Option<serde_json::Value>,
}

/// Insert one history row and return its id. A unique-identity violation means another writer
/// committed this identity first; it is reported as a retryable conflict, never as success.
pub(crate) async fn insert_history(
    tx: &Connection,
    tenant_id: &str,
    machine_id: &str,
    entity_id: &str,
    row: HistoryRow<'_>,
) -> Result<i64, AppError> {
    let cause_json = row.cause.as_ref().map(|c| c.to_string());
    tx.execute(
        "INSERT INTO transitions (tenant_id, machine_id, entity_id, from_state, to_state, event_type, event_params, actions_dispatched, region, timestamp, created_at, identity_key, cause) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
        libsql::params![
            tenant_id.to_string(),
            machine_id.to_string(),
            entity_id.to_string(),
            row.from_state.to_string(),
            row.to_state.to_string(),
            row.event_type.to_string(),
            row.event_params,
            row.actions_dispatched,
            row.region.to_string(),
            row.timestamp,
            row.now,
            row.identity_key.to_string(),
            cause_json
        ],
    )
    .await
    .map_err(|e| {
        if e.to_string().contains("UNIQUE constraint failed") {
            AppError::Conflict(format!(
                "history identity {} already committed by a concurrent writer — retry the transition",
                row.identity_key
            ))
        } else {
            AppError::from(e)
        }
    })?;
    Ok(tx.last_insert_rowid())
}

/// Record every fired join (in cascade order) and return the region instances they entered.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn insert_join_rows(
    tx: &Connection,
    tenant_id: &str,
    machine_id: &str,
    entity_id: &str,
    joins_fired: &[JoinFired],
    version: i64,
    cause_key: &str,
    timestamp: i64,
    now: i64,
) -> Result<Vec<RegionEntered>, AppError> {
    let mut entered = Vec::with_capacity(joins_fired.len());
    for (step, jf) in joins_fired.iter().enumerate() {
        let key = engine::join_identity(&jf.target_region, version, step);
        let row = insert_history(
            tx,
            tenant_id,
            machine_id,
            entity_id,
            HistoryRow {
                from_state: &jf.from_state,
                to_state: &jf.target_state,
                event_type: "$join",
                event_params: "{}".to_string(),
                actions_dispatched: "[]".to_string(),
                region: &jf.target_region,
                timestamp,
                now,
                identity_key: &key,
                cause: Some(serde_json::json!({"kind": "join", "cause_key": cause_key, "step": step})),
            },
        )
        .await?;
        entered.push(RegionEntered {
            region: jf.target_region.clone(),
            state: jf.target_state.clone(),
            row,
            key,
        });
    }
    Ok(entered)
}

/// Guarded write of state, version, region entries and (optionally) context.
/// Fails with `Conflict` when `expected_version` no longer matches.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn update_entity(
    tx: &Connection,
    tenant_id: &str,
    machine_id: &str,
    entity_id: &str,
    state_encoded: &str,
    context_json: Option<&str>,
    entries: &StoredRegionEntries,
    expected_version: i64,
    now: i64,
) -> Result<(), AppError> {
    let entries_json = serde_json::to_string(entries)?;
    let affected = match context_json {
        Some(ctx) => tx.execute(
            "UPDATE entities SET current_state = ?1, context = ?2, state_version = ?3, updated_at = ?4, region_entries = ?5 WHERE tenant_id = ?6 AND machine_id = ?7 AND entity_id = ?8 AND state_version = ?9",
            libsql::params![state_encoded.to_string(), ctx.to_string(), entries.version, now, entries_json, tenant_id.to_string(), machine_id.to_string(), entity_id.to_string(), expected_version],
        ).await?,
        None => tx.execute(
            "UPDATE entities SET current_state = ?1, state_version = ?2, updated_at = ?3, region_entries = ?4 WHERE tenant_id = ?5 AND machine_id = ?6 AND entity_id = ?7 AND state_version = ?8",
            libsql::params![state_encoded.to_string(), entries.version, now, entries_json, tenant_id.to_string(), machine_id.to_string(), entity_id.to_string(), expected_version],
        ).await?,
    };

    if affected == 0 {
        return Err(AppError::Conflict(
            "Concurrent modification detected — retry the transition".into(),
        ));
    }
    Ok(())
}

// ── Sub-machine runtime ─────────────────────────────────────────────────────

/// Try to forward an event to an active sub-machine.
/// Returns Some(response) if a sub-machine handled the event, None otherwise.
#[allow(clippy::too_many_arguments)]
async fn try_sub_machine_forward(
    tx: &Connection,
    state: &AppState,
    tenant_id: &str,
    machine: &MachineDefinition,
    entity: &Entity,
    event_type: &str,
    params: &HashMap<String, String>,
    timestamp: i64,
    pending: &mut Vec<PendingDispatch>,
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
            tx, state, tenant_id, machine, entity, sub_def,
            parent_state, region, event_type, params, timestamp, pending,
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
#[allow(clippy::too_many_arguments)]
async fn handle_sub_machine_event(
    tx: &Connection,
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
    pending: &mut Vec<PendingDispatch>,
) -> Result<Option<TransitionResponse>, AppError> {
    let child_machine = load_machine(state, tenant_id, &sub_def.machine_id).await?;
    let child_entity_id = format!("{}::sub::{}", parent_entity.entity_id, parent_state);

    // Load or create child entity (auto-create on first access), inside the transaction
    let child_entity = load_or_create_child_entity(
        tx, tenant_id, &sub_def.machine_id, &child_entity_id, &child_machine,
    ).await?;

    // Check if child is already in a final state (recovery from previous failed auto-advance,
    // or a child made final by its own $timeout). Advance only on exact correlation (E3).
    let child_current = flat_state(&child_entity);
    if let Some(parent_target) = sub_def.on_final.get(&child_current) {
        let evidence = correlate_child_final(
            tx, tenant_id, parent_machine, parent_entity, parent_region, parent_state,
            &sub_def.machine_id, &child_entity,
        ).await?;
        let cause = sub_complete_cause(
            "recovery", event_type, timestamp, &evidence.parent_entry, &sub_def.machine_id,
            &child_entity_id, &child_current, &evidence.child_row,
        );
        let resp = auto_advance_parent(
            tx, parent_machine, parent_entity,
            parent_state, parent_region, parent_target,
            &child_entity, sub_def, event_type, timestamp, cause, pending,
        ).await?;
        return Ok(Some(resp));
    }

    // Forward event to child machine (Box::pin for async recursion), same transaction
    let child_resp = Box::pin(transition_in_tx(
        tx, state, tenant_id, &child_machine, &child_entity_id,
        event_type, params, timestamp, pending,
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
        entity_id: child_entity_id.clone(),
        previous_state: child_resp.previous_state.clone(),
        current_state: child_resp.current_state.clone(),
        transition: child_resp.transition.clone(),
        auto_completed,
    };

    if auto_completed {
        let parent_target = sub_def.on_final.get(&child_new_state).unwrap();

        // The child's row was just written in this transaction.
        let child_row = latest_history_row(tx, tenant_id, &sub_def.machine_id, &child_entity_id)
            .await?
            .ok_or_else(|| AppError::Internal("child transition left no history row".into()))?;
        let parent_entry = parent_entry_ref(parent_entity, parent_region)?;
        let cause = sub_complete_cause(
            "forward", event_type, timestamp, &parent_entry, &sub_def.machine_id,
            &child_entity_id, &child_new_state, &child_row,
        );

        // Auto-advance parent (same transaction as the child's write)
        let parent_resp = advance_parent_state(
            tx, parent_machine, parent_entity,
            parent_state, parent_region, parent_target,
            timestamp, cause, pending,
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
#[allow(clippy::too_many_arguments)]
async fn auto_advance_parent(
    tx: &Connection,
    parent_machine: &MachineDefinition,
    parent_entity: &Entity,
    parent_state: &str,
    parent_region: &str,
    parent_target: &str,
    child_entity: &Entity,
    sub_def: &SubMachineDef,
    event_type: &str,
    timestamp: i64,
    cause: serde_json::Value,
    pending: &mut Vec<PendingDispatch>,
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
        tx, parent_machine, parent_entity,
        parent_state, parent_region, parent_target,
        timestamp, cause, pending,
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

/// Advance parent entity state (used when child completes), recording `$sub_complete` with its
/// instance identity and provenance in the caller's transaction.
/// Returns (prev_state, new_state, transition_label, region, dispatched_actions, joins_fired).
#[allow(clippy::type_complexity, clippy::too_many_arguments)]
async fn advance_parent_state(
    tx: &Connection,
    machine: &MachineDefinition,
    entity: &Entity,
    from_state: &str,
    region: &str,
    to_state: &str,
    timestamp: i64,
    cause: serde_json::Value,
    pending: &mut Vec<PendingDispatch>,
) -> Result<(
    serde_json::Value,
    serde_json::Value,
    Option<String>,
    Option<String>,
    Vec<DispatchedAction>,
    Vec<JoinFired>,
), AppError> {
    let prev_state_value = entity.to_response().current_state;
    let entries = engine::effective_region_entries(entity);
    let instance = entries.get(region).ok_or_else(|| {
        AppError::Internal(format!("parent region '{}' has no entry instance", region))
    })?;
    let key = engine::sub_complete_identity(region, from_state, instance.seq);

    let mut new_state_map = entity.state_map();
    new_state_map.insert(region.to_string(), to_state.to_string());

    let joins_fired = engine::apply_joins(machine, &mut new_state_map, 10);
    let new_state_encoded = Entity::encode_state(&new_state_map);
    let context_json = serde_json::to_string(&entity.context)?;
    let now = now_millis();
    let version = entity.state_version + 1;

    let display_region = if machine.is_parallel() {
        Some(region.to_string())
    } else {
        None
    };

    // Collect actions for entering new parent state and for joins
    let all_action_configs =
        collect_write_actions(machine, to_state, display_region.as_deref(), &joins_fired);
    let dispatched = actions::build_action_list(all_action_configs.clone());

    // Record parent auto-advance in audit log (same transaction as the state)
    let row = insert_history(
        tx,
        &machine.tenant_id,
        &machine.machine_id,
        &entity.entity_id,
        HistoryRow {
            from_state,
            to_state,
            event_type: "$sub_complete",
            event_params: "{}".to_string(),
            actions_dispatched: serde_json::to_string(&dispatched)?,
            region: display_region.as_deref().unwrap_or(""),
            timestamp,
            now,
            identity_key: &key,
            cause: Some(cause),
        },
    )
    .await?;
    let mut entered = vec![RegionEntered {
        region: region.to_string(),
        state: to_state.to_string(),
        row,
        key: key.clone(),
    }];
    entered.extend(
        insert_join_rows(
            tx, &machine.tenant_id, &machine.machine_id, &entity.entity_id,
            &joins_fired, version, &key, timestamp, now,
        )
        .await?,
    );

    // Optimistic locking
    let new_entries = engine::advance_region_entries(&entries, &new_state_map, &entered, version, now);
    update_entity(
        tx,
        &machine.tenant_id,
        &machine.machine_id,
        &entity.entity_id,
        &new_state_encoded,
        Some(&context_json),
        &new_entries,
        entity.state_version,
        now,
    )
    .await?;

    pending.push(PendingDispatch {
        machine_id: machine.machine_id.clone(),
        entity_id: entity.entity_id.clone(),
        from_state: from_state.to_string(),
        to_state: to_state.to_string(),
        actions: all_action_configs,
    });

    let transition_label = if machine.is_parallel() {
        format!("{}: {} → {}", region, from_state, to_state)
    } else {
        format!("{} → {}", from_state, to_state)
    };

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

// ── Parent/child provenance (E3) ────────────────────────────────────────────

/// A committed history row, by id.
struct HistoryRef {
    id: i64,
    to_state: String,
    identity_key: Option<String>,
}

/// The parent's current instance of the compound state.
struct ParentEntryRef {
    region: String,
    state: String,
    seq: i64,
    basis: EntryBasis,
    /// History row that entered it (`None` = since creation or unknown).
    row: Option<i64>,
}

struct ChildFinalEvidence {
    parent_entry: ParentEntryRef,
    child_row: HistoryRef,
}

fn parent_entry_ref(parent_entity: &Entity, region: &str) -> Result<ParentEntryRef, AppError> {
    let entry = engine::effective_region_entries(parent_entity)
        .remove(region)
        .ok_or_else(|| AppError::Internal(format!("parent region '{}' has no entry instance", region)))?;
    Ok(ParentEntryRef {
        region: region.to_string(),
        state: entry.state,
        seq: entry.seq,
        basis: entry.basis,
        row: entry.row,
    })
}

fn uncorrelated(message: String) -> AppError {
    AppError::Rejected { code: UNCORRELATED_CHILD_FINAL, message }
}

/// A final child may complete the parent only when (a) its final state was entered by its
/// latest committed history row and (b) that row is newer than the row that entered the
/// parent's current instance of the compound state. Anything else stays unknown: nothing is
/// written and the caller gets `UNCORRELATED_CHILD_FINAL`.
#[allow(clippy::too_many_arguments)]
async fn correlate_child_final(
    tx: &Connection,
    tenant_id: &str,
    parent_machine: &MachineDefinition,
    parent_entity: &Entity,
    parent_region: &str,
    compound_state: &str,
    child_machine_id: &str,
    child_entity: &Entity,
) -> Result<ChildFinalEvidence, AppError> {
    let child_state = flat_state(child_entity);
    let child_row = match latest_history_row(tx, tenant_id, child_machine_id, &child_entity.entity_id).await? {
        Some(r) if r.to_state == child_state => r,
        _ => {
            return Err(uncorrelated(format!(
                "child '{}' is final in '{}' but no committed history row entered that state; parent '{}' not advanced",
                child_entity.entity_id, child_state, parent_entity.entity_id
            )))
        }
    };

    let mut parent_entry = parent_entry_ref(parent_entity, parent_region)?;
    let entered_row = match (parent_entry.row, parent_entry.basis) {
        (Some(row), _) => Some(row),
        (None, EntryBasis::Created) => Some(0),
        (None, _) => {
            // Legacy parent: the latest committed row that entered the compound state.
            let region_col = if parent_machine.is_parallel() { parent_region } else { "" };
            let found = latest_row_entering(
                tx, tenant_id, &parent_machine.machine_id, &parent_entity.entity_id,
                region_col, parent_region, compound_state,
            ).await?;
            found.or(if parent_entity.state_version <= 1 { Some(0) } else { None })
        }
    };
    let entered_row = entered_row.ok_or_else(|| {
        uncorrelated(format!(
            "no committed history row entered parent '{}' state '{}'; child final cannot be correlated",
            parent_entity.entity_id, compound_state
        ))
    })?;
    if child_row.id <= entered_row {
        return Err(uncorrelated(format!(
            "child '{}' reached '{}' (row {}) before parent '{}' entered its current '{}' instance (row {})",
            child_entity.entity_id, child_state, child_row.id, parent_entity.entity_id, compound_state, entered_row
        )));
    }
    if entered_row > 0 {
        parent_entry.row = Some(entered_row);
    }
    Ok(ChildFinalEvidence { parent_entry, child_row })
}

#[allow(clippy::too_many_arguments)]
fn sub_complete_cause(
    via: &str,
    event_type: &str,
    timestamp: i64,
    parent_entry: &ParentEntryRef,
    child_machine_id: &str,
    child_entity_id: &str,
    child_state: &str,
    child_row: &HistoryRef,
) -> serde_json::Value {
    serde_json::json!({
        "kind": "sub_complete",
        "via": via,
        "trigger": {"event_type": event_type, "timestamp": timestamp},
        "parent_entry": {
            "region": parent_entry.region,
            "state": parent_entry.state,
            "seq": parent_entry.seq,
            "basis": parent_entry.basis,
            "row": parent_entry.row,
        },
        "child": {
            "machine_id": child_machine_id,
            "entity_id": child_entity_id,
            "state": child_state,
            "row": child_row.id,
            "key": child_row.identity_key,
        },
    })
}

async fn latest_history_row(
    tx: &Connection,
    tenant_id: &str,
    machine_id: &str,
    entity_id: &str,
) -> Result<Option<HistoryRef>, AppError> {
    let mut rows = tx
        .query(
            "SELECT id, to_state, identity_key FROM transitions WHERE tenant_id = ?1 AND machine_id = ?2 AND entity_id = ?3 ORDER BY id DESC LIMIT 1",
            libsql::params![tenant_id.to_string(), machine_id.to_string(), entity_id.to_string()],
        )
        .await?;
    match rows.next().await? {
        Some(row) => Ok(Some(HistoryRef {
            id: row.get(0)?,
            to_state: row.get(1)?,
            identity_key: row.get::<Option<String>>(2).ok().flatten(),
        })),
        None => Ok(None),
    }
}

#[allow(clippy::too_many_arguments)]
async fn latest_row_entering(
    tx: &Connection,
    tenant_id: &str,
    machine_id: &str,
    entity_id: &str,
    region_col: &str,
    region: &str,
    state: &str,
) -> Result<Option<i64>, AppError> {
    let mut rows = tx
        .query(
            "SELECT id FROM transitions WHERE tenant_id = ?1 AND machine_id = ?2 AND entity_id = ?3 AND to_state = ?4 AND region IN (?5, ?6) ORDER BY id DESC LIMIT 1",
            libsql::params![tenant_id.to_string(), machine_id.to_string(), entity_id.to_string(), state.to_string(), region_col.to_string(), region.to_string()],
        )
        .await?;
    match rows.next().await? {
        Some(row) => Ok(Some(row.get(0)?)),
        None => Ok(None),
    }
}

// ── Child entity helpers ────────────────────────────────────────────────────

/// Load or create a child entity for a sub-machine (inside the caller's transaction).
async fn load_or_create_child_entity(
    tx: &Connection,
    tenant_id: &str,
    child_machine_id: &str,
    child_entity_id: &str,
    child_machine: &MachineDefinition,
) -> Result<Entity, AppError> {
    match load_entity_on(tx, tenant_id, child_machine_id, child_entity_id).await {
        Ok(entity) => Ok(entity),
        Err(AppError::NotFound(_)) => {
            create_child_entity(tx, tenant_id, child_machine_id, child_entity_id, child_machine).await?;
            load_entity_on(tx, tenant_id, child_machine_id, child_entity_id).await
        }
        Err(e) => Err(e),
    }
}

/// Create a child entity in a sub-machine. The caller's transaction has just seen it absent,
/// so this is a plain INSERT (it never resets an existing child).
async fn create_child_entity(
    tx: &Connection,
    tenant_id: &str,
    child_machine_id: &str,
    child_entity_id: &str,
    child_machine: &MachineDefinition,
) -> Result<(), AppError> {
    let initial_state_map = child_machine.initial_state_map();
    let initial_state_encoded = Entity::encode_state(&initial_state_map);
    let now = now_millis();
    let region_entries = engine::initial_region_entries(&initial_state_map, now);

    tx.execute(
        "INSERT INTO entities (machine_id, tenant_id, entity_id, current_state, context, state_version, created_at, updated_at, region_entries) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
        libsql::params![
            child_machine_id.to_string(),
            tenant_id.to_string(),
            child_entity_id.to_string(),
            initial_state_encoded,
            "{}".to_string(),
            1_i64,
            now,
            now,
            serde_json::to_string(&region_entries)?
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
