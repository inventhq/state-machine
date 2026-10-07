use std::collections::{BTreeMap, HashMap};

use libsql::{Connection, TransactionBehavior};

use crate::actions;
use crate::engine::{self, RegionEntered};
use crate::errors::{
    AppError, EVENT_IDENTITY_CONFLICT, INSTANCE_CONFLICT, INSTANCE_NOT_ACTIVE,
    NESTING_DEPTH_EXCEEDED, TARGET_NOT_ACTIVE, UNCORRELATED_CHILD_FINAL,
};
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

/// Where code inside a transaction reads machine definitions.
pub(crate) enum Defs<'a> {
    /// API requests: the process cache (`load_machine`).
    App(&'a AppState),
    /// The scheduler: read through the transaction.
    Tx,
}

/// Everything one transaction accumulates: its writes' actions (fired after commit) and the
/// managed lifecycle events of the commit.
pub(crate) struct Ctx<'a> {
    pub tx: &'a Connection,
    pub defs: Defs<'a>,
    pub tenant: &'a str,
    /// The event (or `$timeout`) that started this transaction, recorded in managed causes.
    pub trigger: (String, i64),
    pub pending: Vec<PendingDispatch>,
    pub instances: Vec<InstanceEvent>,
}

impl<'a> Ctx<'a> {
    pub(crate) fn new(tx: &'a Connection, defs: Defs<'a>, tenant: &'a str, trigger: &str, timestamp: i64) -> Self {
        Ctx {
            tx,
            defs,
            tenant,
            trigger: (trigger.to_string(), timestamp),
            pending: Vec::new(),
            instances: Vec::new(),
        }
    }

    async fn machine(&self, machine_id: &str) -> Result<MachineDefinition, AppError> {
        match self.defs {
            Defs::App(state) => load_machine(state, self.tenant, machine_id).await,
            Defs::Tx => {
                let mut rows = self
                    .tx
                    .query(
                        "SELECT definition FROM machines WHERE tenant_id = ?1 AND machine_id = ?2",
                        libsql::params![self.tenant.to_string(), machine_id.to_string()],
                    )
                    .await?;
                match rows.next().await? {
                    Some(row) => Ok(serde_json::from_str(&row.get::<String>(0)?)?),
                    None => Err(AppError::NotFound(format!("Machine '{}' not found", machine_id))),
                }
            }
        }
    }
}

/// Shared transition execution logic. Region-aware with join support.
/// When `dispatch` is true, actions fire server-side (webhooks, events).
/// When `dispatch` is false, actions are returned in the response only (plugin-runtime executes them).
///
/// Atomicity (E1): the entity is re-read, deduplicated, evaluated and written in one IMMEDIATE
/// transaction: state, context, version, region entries, the history row and its `$join` rows,
/// plus any forwarded child write, the parent's `$sub_complete` and every managed start,
/// cancellation and completion cascade. Any failure rolls all of it back. Network effects
/// (ingest-token provisioning, action dispatch) run only after commit.
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
    let mut ctx = Ctx::new(&tx, Defs::App(state), tenant_id, event_type, timestamp);
    // On error the transaction is dropped, which rolls it back.
    let mut resp =
        transition_in_tx(&mut ctx, machine, entity_id, event_type, params, timestamp, true).await?;
    let (pending, instances) = (std::mem::take(&mut ctx.pending), std::mem::take(&mut ctx.instances));
    drop(ctx);
    tx.commit().await?;

    resp.instances = instances;
    if dispatch && !pending.is_empty() {
        dispatch_committed(state, tenant_id, pending).await;
    }
    Ok(resp)
}

/// Targeted delivery (statemachine-nested.v1): apply the event to exactly the managed instance
/// that `target` names below the root, using only that entity's own transitions.
///
/// Order: resolve the target from the definitions; deduplicate at the target (a replay is a
/// duplicate even after the instance ended); require every step of the path to be the current
/// active instance; evaluate the target without forwarding. Completion cascades upward in the
/// same transaction.
#[allow(clippy::too_many_arguments)]
pub async fn execute_targeted(
    state: &AppState,
    tenant_id: &str,
    root_machine: &MachineDefinition,
    root_entity_id: &str,
    target: &[TargetStep],
    event_type: &str,
    params: &HashMap<String, String>,
    timestamp: i64,
    dispatch: bool,
) -> Result<TransitionResponse, AppError> {
    let conn = state.db.connect()?;
    let tx = conn
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .await?;
    let mut ctx = Ctx::new(&tx, Defs::App(state), tenant_id, event_type, timestamp);

    // 1. Resolve the target entity from the definitions alone.
    struct Hop {
        parent_machine_id: String,
        parent_entity_id: String,
        region: String,
        state: String,
        seq: Option<i64>,
        child_machine_id: String,
        child_entity_id: String,
    }
    let mut machine = root_machine.clone();
    let mut entity_id = root_entity_id.to_string();
    let mut hops: Vec<Hop> = Vec::with_capacity(target.len());
    for (i, step) in target.iter().enumerate() {
        let sub = machine.managed_sub(&step.region, &step.state).ok_or_else(|| {
            AppError::BadRequest(format!(
                "target step {} ({}:{}) is not a managed compound state of machine '{}'",
                i, step.region, step.state, machine.machine_id
            ))
        })?;
        let child_machine_id = sub.machine_id.clone();
        let child_entity_id = engine::child_entity_id(&entity_id, &step.state);
        hops.push(Hop {
            parent_machine_id: machine.machine_id.clone(),
            parent_entity_id: entity_id.clone(),
            region: step.region.clone(),
            state: step.state.clone(),
            seq: step.seq,
            child_machine_id: child_machine_id.clone(),
            child_entity_id: child_entity_id.clone(),
        });
        machine = ctx.machine(&child_machine_id).await?;
        entity_id = child_entity_id;
    }
    let target_info = TargetInfo {
        machine_id: machine.machine_id.clone(),
        entity_id: entity_id.clone(),
        depth: hops.len(),
    };

    // 2. Deduplicate at the target before any activity check.
    match dedup(ctx.tx, tenant_id, &machine.machine_id, &entity_id, event_type, timestamp, params).await? {
        Seen::Duplicate => {
            let current = match load_entity_on(ctx.tx, tenant_id, &machine.machine_id, &entity_id).await {
                Ok(e) => e.to_response().current_state,
                Err(AppError::NotFound(_)) => serde_json::Value::Null,
                Err(e) => return Err(e),
            };
            drop(ctx);
            tx.commit().await?;
            let mut resp = duplicate_response(&entity_id, current, event_type, timestamp);
            resp.target = Some(target_info);
            return Ok(resp);
        }
        Seen::Conflict => return Err(identity_conflict(event_type, timestamp, &entity_id)),
        Seen::New => {}
    }

    // 3. Every step must be the current active instance.
    let not_active = |what: String| AppError::Rejected { code: TARGET_NOT_ACTIVE, message: what };
    let root = load_entity_on(ctx.tx, tenant_id, &root_machine.machine_id, root_entity_id).await?;
    if root.instance.as_ref().is_some_and(|i| !i.is_active()) {
        return Err(not_active(format!("entity '{}' is not an active instance", root_entity_id)));
    }
    for (i, hop) in hops.iter().enumerate() {
        let parent = match load_entity_on(ctx.tx, tenant_id, &hop.parent_machine_id, &hop.parent_entity_id).await {
            Ok(p) => p,
            Err(AppError::NotFound(_)) => {
                return Err(not_active(format!("target step {}: entity '{}' does not exist", i, hop.parent_entity_id)))
            }
            Err(e) => return Err(e),
        };
        if parent.instance.as_ref().is_some_and(|inst| !inst.is_active()) {
            return Err(not_active(format!("target step {}: entity '{}' is not active", i, hop.parent_entity_id)));
        }
        let entry = engine::effective_region_entries(&parent).remove(&hop.region);
        let entry = match entry {
            Some(e) if e.state == hop.state && hop.seq.is_none_or(|s| s == e.seq) => e,
            Some(e) => {
                return Err(not_active(format!(
                    "target step {}: '{}' region '{}' is in '{}' (seq {}), not '{}'{}",
                    i,
                    hop.parent_entity_id,
                    hop.region,
                    e.state,
                    e.seq,
                    hop.state,
                    hop.seq.map(|s| format!(" (seq {})", s)).unwrap_or_default()
                )))
            }
            None => return Err(not_active(format!("target step {}: no region '{}'", i, hop.region))),
        };
        let child = match load_entity_on(ctx.tx, tenant_id, &hop.child_machine_id, &hop.child_entity_id).await {
            Ok(c) => c,
            Err(AppError::NotFound(_)) => {
                return Err(not_active(format!("target step {}: no instance '{}'", i, hop.child_entity_id)))
            }
            Err(e) => return Err(e),
        };
        let belongs = child.instance.as_ref().is_some_and(|inst| {
            inst.is_active() && inst.parent().entity_id == hop.parent_entity_id && inst.parent().seq == entry.seq
        });
        if !belongs {
            return Err(not_active(format!(
                "target step {}: instance '{}' is not the active child of that entry",
                i, hop.child_entity_id
            )));
        }
    }

    // 4. The target's own transitions only.
    let mut resp =
        transition_in_tx(&mut ctx, &machine, &entity_id, event_type, params, timestamp, false).await?;
    let (pending, instances) = (std::mem::take(&mut ctx.pending), std::mem::take(&mut ctx.instances));
    drop(ctx);
    tx.commit().await?;

    resp.target = Some(target_info);
    resp.instances = instances;
    if dispatch && !pending.is_empty() {
        dispatch_committed(state, tenant_id, pending).await;
    }
    Ok(resp)
}

/// Create a root entity whose initial state includes managed compound states, starting those
/// children in the same transaction. With `or_ignore`, an existing entity is left unchanged
/// (returns false).
pub async fn create_root_with_children(
    state: &AppState,
    tenant_id: &str,
    machine: &MachineDefinition,
    entity_id: &str,
    context_json: &str,
    or_ignore: bool,
) -> Result<bool, AppError> {
    let conn = state.db.connect()?;
    let tx = conn
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .await?;
    let initial = machine.initial_state_map();
    let now = now_millis();
    let entries = engine::initial_region_entries(&initial, now);
    let sql = if or_ignore {
        "INSERT OR IGNORE INTO entities (machine_id, tenant_id, entity_id, current_state, context, state_version, created_at, updated_at, region_entries) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)"
    } else {
        "INSERT INTO entities (machine_id, tenant_id, entity_id, current_state, context, state_version, created_at, updated_at, region_entries) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)"
    };
    let inserted = tx
        .execute(
            sql,
            libsql::params![
                machine.machine_id.clone(),
                tenant_id.to_string(),
                entity_id.to_string(),
                Entity::encode_state(&initial),
                context_json.to_string(),
                1_i64,
                now,
                now,
                serde_json::to_string(&entries)?
            ],
        )
        .await
        .map_err(|e| {
            if e.to_string().contains("UNIQUE constraint") || e.to_string().contains("PRIMARY KEY") {
                AppError::Conflict(format!("Entity '{}' already exists in machine '{}'", entity_id, machine.machine_id))
            } else {
                AppError::from(e)
            }
        })?;
    if inserted == 0 {
        tx.commit().await?;
        return Ok(false);
    }
    let mut ctx = Ctx::new(&tx, Defs::App(state), tenant_id, "$create", now);
    let sorted: BTreeMap<String, String> = initial.into_iter().collect();
    for (region, state_name) in &sorted {
        if machine.managed_sub(region, state_name).is_some() {
            start_child(&mut ctx, machine, entity_id, &[], region, state_name, 1).await?;
        }
    }
    drop(ctx);
    tx.commit().await?;
    Ok(true)
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

enum Seen {
    New,
    Duplicate,
    Conflict,
}

/// Dedup over every committed row with this (event_type, timestamp), legacy rows included.
/// Equal params is a duplicate; different params under the same key is a conflict.
async fn dedup(
    tx: &Connection,
    tenant_id: &str,
    machine_id: &str,
    entity_id: &str,
    event_type: &str,
    timestamp: i64,
    params: &HashMap<String, String>,
) -> Result<Seen, AppError> {
    let mut rows = tx
        .query(
            "SELECT event_params FROM transitions WHERE tenant_id = ?1 AND machine_id = ?2 AND entity_id = ?3 AND event_type = ?4 AND timestamp = ?5",
            libsql::params![tenant_id.to_string(), machine_id.to_string(), entity_id.to_string(), event_type.to_string(), timestamp],
        )
        .await?;
    let mut seen = Seen::New;
    while let Some(row) = rows.next().await? {
        seen = Seen::Conflict;
        let stored = row.get::<Option<String>>(0).ok().flatten();
        if engine::same_event_params(stored.as_deref(), params) {
            seen = Seen::Duplicate;
            break;
        }
    }
    Ok(seen)
}

fn duplicate_response(
    entity_id: &str,
    current: serde_json::Value,
    event_type: &str,
    timestamp: i64,
) -> TransitionResponse {
    TransitionResponse {
        entity_id: entity_id.to_string(),
        previous_state: current.clone(),
        current_state: current,
        triggered_by: event_type.to_string(),
        timestamp,
        reason: Some("duplicate event (already processed)".into()),
        ..Default::default()
    }
}

fn identity_conflict(event_type: &str, timestamp: i64, entity_id: &str) -> AppError {
    AppError::Rejected {
        code: EVENT_IDENTITY_CONFLICT,
        message: format!(
            "event '{}' at timestamp {} was already committed for entity '{}' with different params",
            event_type, timestamp, entity_id
        ),
    }
}

/// Evaluate one event on one entity inside the caller's transaction. `forward` allows the
/// legacy fallback into active children when the entity has no matching transition.
#[allow(clippy::too_many_arguments)]
async fn transition_in_tx(
    ctx: &mut Ctx<'_>,
    machine: &MachineDefinition,
    entity_id: &str,
    event_type: &str,
    params: &HashMap<String, String>,
    timestamp: i64,
    forward: bool,
) -> Result<TransitionResponse, AppError> {
    let machine_id = &machine.machine_id;
    let entity = load_entity_on(ctx.tx, ctx.tenant, machine_id, entity_id).await?;
    let prev_state_value = entity.to_response().current_state;

    match dedup(ctx.tx, ctx.tenant, machine_id, entity_id, event_type, timestamp, params).await? {
        Seen::Duplicate => return Ok(duplicate_response(entity_id, prev_state_value, event_type, timestamp)),
        Seen::Conflict => return Err(identity_conflict(event_type, timestamp, entity_id)),
        Seen::New => {}
    }
    if entity.instance.as_ref().is_some_and(|i| !i.is_active()) {
        return Err(AppError::Rejected {
            code: INSTANCE_NOT_ACTIVE,
            message: format!("managed instance '{}' has ended", entity_id),
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

            // Merge event params into entity context
            let mut new_context = entity.context.clone();
            for (k, v) in params {
                new_context.insert(k.clone(), serde_json::Value::String(v.clone()));
            }
            let context_json = serde_json::to_string(&new_context)?;

            // Determine region for display (None for flat FSMs)
            let display_region = if machine.is_parallel() {
                Some(tr.region.clone())
            } else {
                None
            };

            let all_action_configs =
                collect_write_actions(machine, &tr.to_state, display_region.as_deref(), &joins_fired);

            let written = write_entity(
                ctx,
                machine,
                &entity,
                WriteSpec {
                    event_type,
                    from_state: &tr.from_state,
                    to_state: &tr.to_state,
                    region: &tr.region,
                    row_region: display_region.as_deref().unwrap_or(""),
                    event_params: serde_json::to_string(params)?,
                    timestamp,
                    identity_key: engine::event_identity(event_type, timestamp),
                    cause: None,
                    new_state_map: &new_state_map,
                    joins_fired: &joins_fired,
                    context_json: Some(&context_json),
                    actions: all_action_configs,
                },
            )
            .await?;

            // Build transition label
            let transition_label = if machine.is_parallel() {
                format!("{}: {} → {}", tr.region, tr.from_state, tr.to_state)
            } else {
                format!("{} → {}", tr.from_state, tr.to_state)
            };

            Ok(TransitionResponse {
                entity_id: entity_id.to_string(),
                previous_state: prev_state_value,
                current_state: state_map_to_value(&new_state_map),
                transition: Some(transition_label),
                region: display_region,
                triggered_by: event_type.to_string(),
                actions_dispatched: written.dispatched,
                joins_fired,
                timestamp,
                ..Default::default()
            })
        }
        None => {
            // Try sub-machine forwarding before giving up
            if forward {
                if let Some(resp) = try_sub_machine_forward(
                    ctx, machine, &entity, event_type, params, timestamp,
                ).await? {
                    return Ok(resp);
                }
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
                triggered_by: event_type.to_string(),
                timestamp,
                reason: Some(format!(
                    "no transition from '{}' on '{}'",
                    state_desc, event_type
                )),
                ..Default::default()
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

// ── The one write path (transitions, $sub_complete, $timeout) ───────────────

/// One state-changing write of one entity.
pub(crate) struct WriteSpec<'a> {
    pub event_type: &'a str,
    pub from_state: &'a str,
    pub to_state: &'a str,
    /// State-map region the main row entered.
    pub region: &'a str,
    /// History `region` column ("" for flat machines).
    pub row_region: &'a str,
    pub event_params: String,
    pub timestamp: i64,
    pub identity_key: String,
    pub cause: Option<serde_json::Value>,
    /// Complete state map after the write, joins included.
    pub new_state_map: &'a HashMap<String, String>,
    pub joins_fired: &'a [JoinFired],
    /// `None` leaves the context column unchanged.
    pub context_json: Option<&'a str>,
    pub actions: Vec<(String, ActionConfig, Option<String>)>,
}

pub(crate) struct Written {
    pub row: i64,
    pub key: String,
    pub dispatched: Vec<DispatchedAction>,
}

/// Write the history row(s), region entries, state and managed instance of one entity, then run
/// its managed lifecycle: children of regions it left are cancelled, children of managed compound
/// states it entered are started, and if it is an active managed child that now satisfies its
/// completion rule, it is marked completed and its parent advances (cascading upward).
/// Legacy entities with legacy compound states do nothing beyond the write.
pub(crate) async fn write_entity(
    ctx: &mut Ctx<'_>,
    machine: &MachineDefinition,
    entity: &Entity,
    spec: WriteSpec<'_>,
) -> Result<Written, AppError> {
    let now = now_millis();
    let version = entity.state_version + 1;
    let dispatched = actions::build_action_list(spec.actions.clone());
    let key = spec.identity_key.clone();

    let row = insert_history(
        ctx.tx,
        ctx.tenant,
        &machine.machine_id,
        &entity.entity_id,
        HistoryRow {
            from_state: spec.from_state,
            to_state: spec.to_state,
            event_type: spec.event_type,
            event_params: spec.event_params,
            actions_dispatched: serde_json::to_string(&dispatched)?,
            region: spec.row_region,
            timestamp: spec.timestamp,
            now,
            identity_key: &key,
            cause: spec.cause,
        },
    )
    .await?;
    let mut entered = vec![RegionEntered {
        region: spec.region.to_string(),
        state: spec.to_state.to_string(),
        row,
        key: key.clone(),
    }];
    entered.extend(
        insert_join_rows(
            ctx.tx, ctx.tenant, &machine.machine_id, &entity.entity_id,
            spec.joins_fired, version, &key, spec.timestamp, now,
        )
        .await?,
    );

    let old_entries = engine::effective_region_entries(entity);
    let entries = engine::advance_region_entries(&old_entries, spec.new_state_map, &entered, version, now);

    // A managed child completes in the same UPDATE that satisfies its rule.
    let mut instance = entity.instance.clone();
    let mut completion: Option<(MachineDefinition, String)> = None;
    if let Some(inst) = instance.as_mut() {
        if !inst.is_active() {
            return Err(AppError::Rejected {
                code: INSTANCE_NOT_ACTIVE,
                message: format!("managed instance '{}' has ended", entity.entity_id),
            });
        }
        let link = inst.parent().clone();
        let parent_machine = ctx.machine(&link.machine_id).await?;
        let target = parent_machine
            .managed_sub(&link.region, &link.state)
            .and_then(|sub| sub.completion_target(spec.new_state_map))
            .map(|t| t.to_string());
        if let Some(target) = target {
            inst.status = InstanceStatus::Completed;
            inst.ended = Some(InstanceEnd { row, key: key.clone() });
            completion = Some((parent_machine, target));
        }
    }

    let encoded = Entity::encode_state(spec.new_state_map);
    update_entity(
        ctx.tx,
        ctx.tenant,
        &machine.machine_id,
        &entity.entity_id,
        &encoded,
        spec.context_json,
        &entries,
        entity.state_version,
        now,
        instance.as_ref(),
    )
    .await?;

    ctx.pending.push(PendingDispatch {
        machine_id: machine.machine_id.clone(),
        entity_id: entity.entity_id.clone(),
        from_state: spec.from_state.to_string(),
        to_state: spec.to_state.to_string(),
        actions: spec.actions,
    });

    let own_path: Vec<InstanceStep> = entity.instance.as_ref().map(|i| i.path.clone()).unwrap_or_default();
    Box::pin(apply_child_lifecycle(
        ctx,
        machine,
        &entity.entity_id,
        &own_path,
        &old_entries,
        &entries.regions,
        completion.is_some(),
        row,
        &key,
    ))
    .await?;

    if let (Some((parent_machine, target)), Some(inst)) = (completion, instance.as_ref()) {
        Box::pin(complete_parent(
            ctx,
            &parent_machine,
            &machine.machine_id,
            &entity.entity_id,
            &encoded,
            inst,
            &target,
            row,
            &key,
        ))
        .await?;
    }

    Ok(Written { row, key, dispatched })
}

/// Managed children of `entity_id` after one of its writes: a region whose entry instance changed
/// cancels the child of the state it left and starts the child of the managed state it entered.
/// When the entity itself just ended (`ended`), it starts nothing and cancels every active child.
#[allow(clippy::too_many_arguments)]
async fn apply_child_lifecycle(
    ctx: &mut Ctx<'_>,
    machine: &MachineDefinition,
    entity_id: &str,
    own_path: &[InstanceStep],
    old: &BTreeMap<String, RegionEntry>,
    new: &BTreeMap<String, RegionEntry>,
    ended: bool,
    by_row: i64,
    by_key: &str,
) -> Result<(), AppError> {
    for (region, new_entry) in new {
        let old_entry = old.get(region);
        let changed = old_entry.is_none_or(|o| o.seq != new_entry.seq);
        if changed {
            if let Some(o) = old_entry {
                if let Some(sub) = machine.managed_sub(region, &o.state) {
                    let reason = if ended { "completed" } else { "parent_exit" };
                    Box::pin(cancel_subtree(
                        ctx,
                        &sub.machine_id,
                        &engine::child_entity_id(entity_id, &o.state),
                        reason,
                        by_row,
                        by_key,
                    ))
                    .await?;
                }
            }
            if !ended && machine.managed_sub(region, &new_entry.state).is_some() {
                Box::pin(start_child(ctx, machine, entity_id, own_path, region, &new_entry.state, new_entry.seq)).await?;
            }
        } else if ended {
            if let Some(sub) = machine.managed_sub(region, &new_entry.state) {
                Box::pin(cancel_subtree(
                    ctx,
                    &sub.machine_id,
                    &engine::child_entity_id(entity_id, &new_entry.state),
                    "completed",
                    by_row,
                    by_key,
                ))
                .await?;
            }
        }
    }
    Ok(())
}

/// Create the managed child of `state` (entered in `region` with entry `parent_seq`) and,
/// recursively, the children of its own initial managed compound states. Create-only: an existing
/// entity at that id (re-entry, a pre-created entity or a collision) refuses the whole write.
#[allow(clippy::too_many_arguments)]
async fn start_child(
    ctx: &mut Ctx<'_>,
    parent_machine: &MachineDefinition,
    parent_entity_id: &str,
    parent_path: &[InstanceStep],
    region: &str,
    state: &str,
    parent_seq: i64,
) -> Result<(), AppError> {
    let sub = parent_machine
        .managed_sub(region, state)
        .ok_or_else(|| AppError::Internal(format!("'{}' is not a managed compound state", state)))?
        .clone();
    let depth = parent_path.len() + 1;
    if depth > MAX_MANAGED_DEPTH {
        return Err(AppError::Rejected {
            code: NESTING_DEPTH_EXCEEDED,
            message: format!(
                "starting '{}' under '{}' would exceed {} managed levels",
                sub.machine_id, parent_entity_id, MAX_MANAGED_DEPTH
            ),
        });
    }
    let child_machine = ctx.machine(&sub.machine_id).await?;
    sub.check_rules_against(&child_machine).map_err(AppError::BadRequest)?;
    let child_id = engine::child_entity_id(parent_entity_id, state);
    match load_entity_on(ctx.tx, ctx.tenant, &sub.machine_id, &child_id).await {
        Ok(_) => {
            return Err(AppError::Rejected {
                code: INSTANCE_CONFLICT,
                message: format!(
                    "entity '{}' already exists in '{}': a managed compound state cannot be re-entered",
                    child_id, sub.machine_id
                ),
            })
        }
        Err(AppError::NotFound(_)) => {}
        Err(e) => return Err(e),
    }

    let mut path = parent_path.to_vec();
    path.push(InstanceStep {
        machine_id: parent_machine.machine_id.clone(),
        entity_id: parent_entity_id.to_string(),
        region: region.to_string(),
        state: state.to_string(),
        seq: parent_seq,
    });
    let instance = ChildInstance {
        status: InstanceStatus::Active,
        depth,
        path: path.clone(),
        started: 1,
        ended: None,
    };
    let initial = child_machine.initial_state_map();
    let now = now_millis();
    let entries = engine::initial_region_entries(&initial, now);
    ctx.tx
        .execute(
            "INSERT INTO entities (machine_id, tenant_id, entity_id, current_state, context, state_version, created_at, updated_at, region_entries, instance) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
            libsql::params![
                sub.machine_id.clone(),
                ctx.tenant.to_string(),
                child_id.clone(),
                Entity::encode_state(&initial),
                "{}".to_string(),
                1_i64,
                now,
                now,
                serde_json::to_string(&entries)?,
                serde_json::to_string(&instance)?
            ],
        )
        .await?;
    ctx.instances.push(InstanceEvent {
        kind: "started".into(),
        machine_id: sub.machine_id.clone(),
        entity_id: child_id.clone(),
        depth,
        parent: path.last().cloned().expect("path has the parent step"),
        reason: None,
        row: None,
        key: None,
        parent_to: None,
        parent_row: None,
        parent_key: None,
    });

    let sorted: BTreeMap<String, String> = initial.into_iter().collect();
    for (child_region, child_state) in &sorted {
        if child_machine.managed_sub(child_region, child_state).is_some() {
            Box::pin(start_child(ctx, &child_machine, &child_id, &path, child_region, child_state, 1)).await?;
        }
    }
    Ok(())
}

/// Cancel an active managed child and, deepest first, its active descendants: version+1, a
/// `$sub_cancel` row `["cancel", parent seq]`, status `cancelled`. State, timers' entries and
/// history stay readable; nothing fires or completes from it afterwards.
async fn cancel_subtree(
    ctx: &mut Ctx<'_>,
    child_machine_id: &str,
    child_id: &str,
    reason: &str,
    by_row: i64,
    by_key: &str,
) -> Result<(), AppError> {
    let child = match load_entity_on(ctx.tx, ctx.tenant, child_machine_id, child_id).await {
        Ok(c) => c,
        Err(AppError::NotFound(_)) => return Ok(()),
        Err(e) => return Err(e),
    };
    let mut instance = match child.instance.clone() {
        Some(i) if i.is_active() => i,
        _ => return Ok(()),
    };
    let child_machine = ctx.machine(child_machine_id).await?;
    let sorted: BTreeMap<String, String> = child.state_map().into_iter().collect();
    for (region, state) in &sorted {
        if let Some(sub) = child_machine.managed_sub(region, state) {
            Box::pin(cancel_subtree(
                ctx,
                &sub.machine_id,
                &engine::child_entity_id(child_id, state),
                "ancestor_cancelled",
                by_row,
                by_key,
            ))
            .await?;
        }
    }

    let now = now_millis();
    let version = child.state_version + 1;
    let parent = instance.parent().clone();
    let key = engine::cancel_identity(parent.seq);
    let row = insert_history(
        ctx.tx,
        ctx.tenant,
        child_machine_id,
        child_id,
        HistoryRow {
            from_state: &child.current_state,
            to_state: &child.current_state,
            event_type: "$sub_cancel",
            event_params: "{}".to_string(),
            actions_dispatched: "[]".to_string(),
            region: "",
            timestamp: ctx.trigger.1,
            now,
            identity_key: &key,
            cause: Some(serde_json::json!({
                "kind": "cancel",
                "reason": reason,
                "parent": parent,
                "by": {"row": by_row, "key": by_key},
            })),
        },
    )
    .await?;
    instance.status = InstanceStatus::Cancelled;
    instance.ended = Some(InstanceEnd { row, key: key.clone() });
    let entries = StoredRegionEntries {
        version,
        regions: engine::effective_region_entries(&child),
    };
    update_entity(
        ctx.tx,
        ctx.tenant,
        child_machine_id,
        child_id,
        &child.current_state,
        None,
        &entries,
        child.state_version,
        now,
        Some(&instance),
    )
    .await?;
    ctx.instances.push(InstanceEvent {
        kind: "cancelled".into(),
        machine_id: child_machine_id.to_string(),
        entity_id: child_id.to_string(),
        depth: instance.depth,
        parent,
        reason: Some(reason.to_string()),
        row: Some(row),
        key: Some(key),
        parent_to: None,
        parent_row: None,
        parent_key: None,
    });
    Ok(())
}

/// A managed child just completed (`child_row`/`child_key`): advance its parent from the compound
/// state to `target` with `$sub_complete` in the same transaction. The parent must still be in the
/// child's entry instance; anything else is an internal inconsistency and rolls back.
#[allow(clippy::too_many_arguments)]
async fn complete_parent(
    ctx: &mut Ctx<'_>,
    parent_machine: &MachineDefinition,
    child_machine_id: &str,
    child_entity_id: &str,
    child_state: &str,
    instance: &ChildInstance,
    target: &str,
    child_row: i64,
    child_key: &str,
) -> Result<(), AppError> {
    let link = instance.parent().clone();
    let parent = load_entity_on(ctx.tx, ctx.tenant, &link.machine_id, &link.entity_id).await?;
    let entry = engine::effective_region_entries(&parent)
        .remove(&link.region)
        .filter(|e| e.state == link.state && e.seq == link.seq)
        .ok_or_else(|| {
            AppError::Internal(format!(
                "managed parent '{}' is no longer in '{}' (seq {}) of region '{}'",
                link.entity_id, link.state, link.seq, link.region
            ))
        })?;
    if parent.instance.as_ref().is_some_and(|i| !i.is_active()) {
        return Err(AppError::Internal(format!(
            "managed parent '{}' ended without cancelling its child '{}'",
            link.entity_id, child_entity_id
        )));
    }
    let cause = serde_json::json!({
        "kind": "sub_complete",
        "via": "managed",
        "trigger": {"event_type": ctx.trigger.0, "timestamp": ctx.trigger.1},
        "parent_entry": {
            "region": link.region,
            "state": link.state,
            "seq": entry.seq,
            "basis": entry.basis,
            "row": entry.row,
        },
        "child": {
            "machine_id": child_machine_id,
            "entity_id": child_entity_id,
            "state": child_state,
            "row": child_row,
            "key": child_key,
        },
    });

    // Recorded before the parent's write so the events read bottom-up.
    let index = ctx.instances.len();
    ctx.instances.push(InstanceEvent {
        kind: "completed".into(),
        machine_id: child_machine_id.to_string(),
        entity_id: child_entity_id.to_string(),
        depth: instance.depth,
        parent: link.clone(),
        reason: None,
        row: Some(child_row),
        key: Some(child_key.to_string()),
        parent_to: Some(target.to_string()),
        parent_row: None,
        parent_key: None,
    });
    let timestamp = ctx.trigger.1;
    let (_, written) = Box::pin(advance_parent_state(
        ctx, parent_machine, &parent, &link.state, &link.region, target, timestamp, cause,
    ))
    .await?;
    ctx.instances[index].parent_row = Some(written.row);
    ctx.instances[index].parent_key = Some(written.key);
    Ok(())
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

/// Guarded write of state, version, region entries, managed instance and (optionally) context.
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
    instance: Option<&ChildInstance>,
) -> Result<(), AppError> {
    let entries_json = serde_json::to_string(entries)?;
    let instance_json = instance.map(serde_json::to_string).transpose()?;
    let affected = match context_json {
        Some(ctx) => tx.execute(
            "UPDATE entities SET current_state = ?1, context = ?2, state_version = ?3, updated_at = ?4, region_entries = ?5, instance = ?6 WHERE tenant_id = ?7 AND machine_id = ?8 AND entity_id = ?9 AND state_version = ?10",
            libsql::params![state_encoded.to_string(), ctx.to_string(), entries.version, now, entries_json, instance_json, tenant_id.to_string(), machine_id.to_string(), entity_id.to_string(), expected_version],
        ).await?,
        None => tx.execute(
            "UPDATE entities SET current_state = ?1, state_version = ?2, updated_at = ?3, region_entries = ?4, instance = ?5 WHERE tenant_id = ?6 AND machine_id = ?7 AND entity_id = ?8 AND state_version = ?9",
            libsql::params![state_encoded.to_string(), entries.version, now, entries_json, instance_json, tenant_id.to_string(), machine_id.to_string(), entity_id.to_string(), expected_version],
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
    ctx: &mut Ctx<'_>,
    machine: &MachineDefinition,
    entity: &Entity,
    event_type: &str,
    params: &HashMap<String, String>,
    timestamp: i64,
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
        let handled = if sub_def.is_managed() {
            Box::pin(forward_to_managed_child(
                ctx, machine, entity, sub_def, parent_state, region, event_type, params, timestamp,
            )).await
        } else {
            Box::pin(handle_sub_machine_event(
                ctx, machine, entity, sub_def,
                parent_state, region, event_type, params, timestamp,
            )).await
        };
        match handled {
            Ok(Some(resp)) => return Ok(Some(resp)),
            Ok(None) => continue,
            Err(e) => return Err(e),
        }
    }

    Ok(None)
}

/// Untargeted delivery into an active managed child: the child evaluates (and may forward
/// further); any completion has already cascaded within the child's write.
#[allow(clippy::too_many_arguments)]
async fn forward_to_managed_child(
    ctx: &mut Ctx<'_>,
    parent_machine: &MachineDefinition,
    parent_entity: &Entity,
    sub_def: &SubMachineDef,
    parent_state: &str,
    parent_region: &str,
    event_type: &str,
    params: &HashMap<String, String>,
    timestamp: i64,
) -> Result<Option<TransitionResponse>, AppError> {
    let child_id = engine::child_entity_id(&parent_entity.entity_id, parent_state);
    let child = match load_entity_on(ctx.tx, ctx.tenant, &sub_def.machine_id, &child_id).await {
        Ok(c) => c,
        Err(AppError::NotFound(_)) => return Ok(None),
        Err(e) => return Err(e),
    };
    if !child.instance.as_ref().is_some_and(|i| i.is_active()) {
        return Ok(None);
    }
    let child_machine = ctx.machine(&sub_def.machine_id).await?;
    let child_resp = Box::pin(transition_in_tx(
        ctx, &child_machine, &child_id, event_type, params, timestamp, true,
    ))
    .await?;
    if child_resp.transition.is_none() && child_resp.sub_machine.is_none() {
        return Ok(None);
    }

    let child_after = load_entity_on(ctx.tx, ctx.tenant, &sub_def.machine_id, &child_id).await?;
    let auto_completed = child_after
        .instance
        .as_ref()
        .is_some_and(|i| i.status == InstanceStatus::Completed);
    let parent_after =
        load_entity_on(ctx.tx, ctx.tenant, &parent_machine.machine_id, &parent_entity.entity_id).await?;
    let transition = (parent_after.state_version != parent_entity.state_version).then(|| {
        let to = parent_after.state_map().get(parent_region).cloned().unwrap_or_default();
        if parent_machine.is_parallel() {
            format!("{}: {} → {}", parent_region, parent_state, to)
        } else {
            format!("{} → {}", parent_state, to)
        }
    });

    Ok(Some(TransitionResponse {
        entity_id: parent_entity.entity_id.clone(),
        previous_state: parent_entity.to_response().current_state,
        current_state: parent_after.to_response().current_state,
        transition,
        region: if parent_machine.is_parallel() { Some(parent_region.to_string()) } else { None },
        triggered_by: event_type.to_string(),
        actions_dispatched: child_resp.actions_dispatched,
        timestamp,
        sub_machine: Some(SubMachineTransition {
            machine_id: sub_def.machine_id.clone(),
            entity_id: child_id,
            previous_state: child_resp.previous_state,
            current_state: child_resp.current_state,
            transition: child_resp.transition,
            auto_completed,
        }),
        ..Default::default()
    }))
}

/// Handle a single legacy sub-machine event forwarding.
/// Returns Some(response) if the child handled the event, None if child had no matching transition.
#[allow(clippy::too_many_arguments)]
async fn handle_sub_machine_event(
    ctx: &mut Ctx<'_>,
    parent_machine: &MachineDefinition,
    parent_entity: &Entity,
    sub_def: &SubMachineDef,
    parent_state: &str,
    parent_region: &str,
    event_type: &str,
    params: &HashMap<String, String>,
    timestamp: i64,
) -> Result<Option<TransitionResponse>, AppError> {
    let child_machine = ctx.machine(&sub_def.machine_id).await?;
    let child_entity_id = engine::child_entity_id(&parent_entity.entity_id, parent_state);

    // Load or create child entity (auto-create on first access), inside the transaction
    let child_entity = load_or_create_child_entity(
        ctx.tx, ctx.tenant, &sub_def.machine_id, &child_entity_id, &child_machine,
    ).await?;

    // Check if child is already in a final state (recovery from previous failed auto-advance,
    // or a child made final by its own $timeout). Advance only on exact correlation (E3).
    let child_current = flat_state(&child_entity);
    if let Some(parent_target) = sub_def.on_final.get(&child_current) {
        let evidence = correlate_child_final(
            ctx.tx, ctx.tenant, parent_machine, parent_entity, parent_region, parent_state,
            &sub_def.machine_id, &child_entity,
        ).await?;
        let cause = sub_complete_cause(
            "recovery", event_type, timestamp, &evidence.parent_entry, &sub_def.machine_id,
            &child_entity_id, &child_current, &evidence.child_row,
        );
        let resp = auto_advance_parent(
            ctx, parent_machine, parent_entity,
            parent_state, parent_region, parent_target,
            &child_entity, sub_def, event_type, timestamp, cause,
        ).await?;
        return Ok(Some(resp));
    }

    // Forward event to child machine (Box::pin for async recursion), same transaction
    let child_resp = Box::pin(transition_in_tx(
        ctx, &child_machine, &child_entity_id,
        event_type, params, timestamp, true,
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
        let child_row = latest_history_row(ctx.tx, ctx.tenant, &sub_def.machine_id, &child_entity_id)
            .await?
            .ok_or_else(|| AppError::Internal("child transition left no history row".into()))?;
        let parent_entry = parent_entry_ref(parent_entity, parent_region)?;
        let cause = sub_complete_cause(
            "forward", event_type, timestamp, &parent_entry, &sub_def.machine_id,
            &child_entity_id, &child_new_state, &child_row,
        );

        // Auto-advance parent (same transaction as the child's write)
        let (parent_resp, _) = Box::pin(advance_parent_state(
            ctx, parent_machine, parent_entity,
            parent_state, parent_region, parent_target,
            timestamp, cause,
        )).await?;

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
            sub_machine: Some(sub_info),
            ..Default::default()
        }))
    } else {
        // Child transitioned but didn't complete — parent stays in compound state
        let parent_state_value = parent_entity.to_response().current_state;
        Ok(Some(TransitionResponse {
            entity_id: parent_entity.entity_id.clone(),
            previous_state: parent_state_value.clone(),
            current_state: parent_state_value,
            region: if parent_machine.is_parallel() { Some(parent_region.to_string()) } else { None },
            triggered_by: event_type.to_string(),
            actions_dispatched: child_resp.actions_dispatched,
            timestamp,
            sub_machine: Some(sub_info),
            ..Default::default()
        }))
    }
}

/// Auto-advance the parent when child is already in a final state (recovery path).
#[allow(clippy::too_many_arguments)]
async fn auto_advance_parent(
    ctx: &mut Ctx<'_>,
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

    let (parent_resp, _) = Box::pin(advance_parent_state(
        ctx, parent_machine, parent_entity,
        parent_state, parent_region, parent_target,
        timestamp, cause,
    )).await?;

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
        sub_machine: Some(sub_info),
        ..Default::default()
    })
}

/// Advance parent entity state (used when child completes), recording `$sub_complete` with its
/// instance identity and provenance in the caller's transaction.
/// Returns (prev_state, new_state, transition_label, region, dispatched_actions, joins_fired).
#[allow(clippy::type_complexity, clippy::too_many_arguments)]
async fn advance_parent_state(
    ctx: &mut Ctx<'_>,
    machine: &MachineDefinition,
    entity: &Entity,
    from_state: &str,
    region: &str,
    to_state: &str,
    timestamp: i64,
    cause: serde_json::Value,
) -> Result<((
    serde_json::Value,
    serde_json::Value,
    Option<String>,
    Option<String>,
    Vec<DispatchedAction>,
    Vec<JoinFired>,
), Written), AppError> {
    let prev_state_value = entity.to_response().current_state;
    let instance = engine::effective_region_entries(entity).remove(region).ok_or_else(|| {
        AppError::Internal(format!("parent region '{}' has no entry instance", region))
    })?;

    let mut new_state_map = entity.state_map();
    new_state_map.insert(region.to_string(), to_state.to_string());

    let joins_fired = engine::apply_joins(machine, &mut new_state_map, 10);
    let context_json = serde_json::to_string(&entity.context)?;

    let display_region = if machine.is_parallel() {
        Some(region.to_string())
    } else {
        None
    };

    // Collect actions for entering new parent state and for joins
    let all_action_configs =
        collect_write_actions(machine, to_state, display_region.as_deref(), &joins_fired);

    // Record parent auto-advance in audit log (same transaction as the state)
    let written = write_entity(
        ctx,
        machine,
        entity,
        WriteSpec {
            event_type: "$sub_complete",
            from_state,
            to_state,
            region,
            row_region: display_region.as_deref().unwrap_or(""),
            event_params: "{}".to_string(),
            timestamp,
            identity_key: engine::sub_complete_identity(region, from_state, instance.seq),
            cause: Some(cause),
            new_state_map: &new_state_map,
            joins_fired: &joins_fired,
            context_json: Some(&context_json),
            actions: all_action_configs,
        },
    )
    .await?;

    let transition_label = if machine.is_parallel() {
        format!("{}: {} → {}", region, from_state, to_state)
    } else {
        format!("{} → {}", from_state, to_state)
    };

    let new_state_value = state_map_to_value(&new_state_map);
    let dispatched = written.dispatched.clone();

    Ok((
        (
            prev_state_value,
            new_state_value,
            Some(transition_label),
            display_region,
            dispatched,
            joins_fired,
        ),
        written,
    ))
}

// ── Parent/child provenance (E3, legacy children) ───────────────────────────

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

// ── Child entity helpers (legacy children) ──────────────────────────────────

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
