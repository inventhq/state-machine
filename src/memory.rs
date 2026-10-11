//! The no-database store (`STATE_STORE=memory`): machines, entities and history live in this process's memory.
//!
//! The API, its request and response shapes, and every decision are the service's own: transitions, guards and joins
//! (`engine::evaluate`, `engine::apply_joins`), event identities and entry instances (`engine::*_identity`,
//! `engine::advance_region_entries`), timer due-checks (`engine::timeout_due`) and actions (`actions::*`). Only the
//! storage differs: one write per event under one lock, so concurrent writers queue instead of being refused as
//! busy. Events are deduplicated by `(event_type, timestamp)` with the params compared, as in `statemachine-e1e4.v1`.
//!
//! Not in this mode: child machines (refused at registration) and rows from older binaries. A restart loses
//! everything.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tracing::info;

use crate::engine::{self, RegionEntered};
use crate::errors::{AppError, EVENT_IDENTITY_CONFLICT};
use crate::models::*;
use crate::routes::{extract_tenant_id, now_millis};
use crate::{actions, auth, transition_core};

type MachineKey = (String, String);
type EntityKey = (String, String, String);

#[derive(Default)]
struct Store {
    machines: HashMap<MachineKey, MachineDefinition>,
    entities: HashMap<EntityKey, Entity>,
    history: HashMap<EntityKey, Vec<TransitionRecord>>,
    next_row: i64,
}

/// The memory store and what dispatching actions needs.
#[derive(Clone)]
pub struct MemState {
    store: Arc<Mutex<Store>>,
    http: Client,
    ingest_url: String,
}

/// Actions to dispatch after a write.
struct Pending {
    machine_id: String,
    entity_id: String,
    from_state: String,
    to_state: String,
    actions: Vec<(String, ActionConfig, Option<String>)>,
}

/// One write of one entity (a transition or a timeout).
struct Write<'a> {
    event_type: &'a str,
    from_state: &'a str,
    to_state: &'a str,
    region: &'a str,
    row_region: Option<String>,
    event_params: Option<HashMap<String, String>>,
    timestamp: i64,
    identity_key: String,
    cause: Option<serde_json::Value>,
    new_state_map: &'a HashMap<String, String>,
    joins_fired: &'a [JoinFired],
    context: Option<HashMap<String, serde_json::Value>>,
    actions: Vec<(String, ActionConfig, Option<String>)>,
}

fn has_children(m: &MachineDefinition) -> bool {
    !m.sub_machines.is_empty() || m.regions.iter().any(|r| !r.sub_machines.is_empty())
}

fn entity_missing(machine_id: &str, entity_id: &str) -> AppError {
    AppError::NotFound(format!("Entity '{}' not found in machine '{}'", entity_id, machine_id))
}

fn machine_missing(machine_id: &str) -> AppError {
    AppError::NotFound(format!("Machine '{}' not found", machine_id))
}

impl Store {
    fn machine(&self, tenant: &str, machine_id: &str) -> Result<MachineDefinition, AppError> {
        self.machines.get(&(tenant.to_string(), machine_id.to_string())).cloned().ok_or_else(|| machine_missing(machine_id))
    }

    fn create_entity(&mut self, tenant: &str, machine: &MachineDefinition, entity_id: &str, context: HashMap<String, serde_json::Value>) -> Result<Entity, AppError> {
        let key = (tenant.to_string(), machine.machine_id.clone(), entity_id.to_string());
        if self.entities.contains_key(&key) {
            return Err(AppError::Conflict(format!("Entity '{}' already exists in machine '{}'", entity_id, machine.machine_id)));
        }
        let now = now_millis();
        let map = machine.initial_state_map();
        let entity = Entity {
            machine_id: machine.machine_id.clone(),
            tenant_id: tenant.to_string(),
            entity_id: entity_id.to_string(),
            current_state: Entity::encode_state(&map),
            context,
            state_version: 1,
            created_at: now,
            updated_at: now,
            region_entries: Some(engine::initial_region_entries(&map, now)),
            instance: None,
        };
        self.entities.insert(key, entity.clone());
        Ok(entity)
    }

    /// The one write path: history row(s), entry instances, state and context, as `transition_core::write_entity`.
    fn write(&mut self, tenant: &str, machine: &MachineDefinition, entity: &Entity, w: Write) -> (Vec<DispatchedAction>, Pending) {
        let now = now_millis();
        let version = entity.state_version + 1;
        let dispatched = actions::build_action_list(w.actions.clone());
        let key = (tenant.to_string(), machine.machine_id.clone(), entity.entity_id.clone());
        let history = self.history.entry(key.clone()).or_default();
        self.next_row += 1;
        let row = self.next_row;
        history.push(TransitionRecord {
            id: row,
            tenant_id: tenant.to_string(),
            machine_id: machine.machine_id.clone(),
            entity_id: entity.entity_id.clone(),
            from_state: w.from_state.to_string(),
            to_state: w.to_state.to_string(),
            event_type: w.event_type.to_string(),
            event_params: w.event_params,
            actions_dispatched: dispatched.iter().filter_map(|a| serde_json::to_string(a).ok()).collect(),
            region: w.row_region,
            timestamp: w.timestamp,
            created_at: now,
            identity_key: Some(w.identity_key.clone()),
            cause: w.cause,
        });
        let mut entered = vec![RegionEntered { region: w.region.to_string(), state: w.to_state.to_string(), row, key: w.identity_key.clone() }];
        for (step, jf) in w.joins_fired.iter().enumerate() {
            self.next_row += 1;
            let join_key = engine::join_identity(&jf.target_region, version, step);
            history.push(TransitionRecord {
                id: self.next_row,
                tenant_id: tenant.to_string(),
                machine_id: machine.machine_id.clone(),
                entity_id: entity.entity_id.clone(),
                from_state: jf.from_state.clone(),
                to_state: jf.target_state.clone(),
                event_type: "$join".into(),
                event_params: Some(HashMap::new()),
                actions_dispatched: Vec::new(),
                region: Some(jf.target_region.clone()),
                timestamp: w.timestamp,
                created_at: now,
                identity_key: Some(join_key.clone()),
                cause: Some(serde_json::json!({"kind": "join", "cause_key": w.identity_key, "step": step})),
            });
            entered.push(RegionEntered { region: jf.target_region.clone(), state: jf.target_state.clone(), row: self.next_row, key: join_key });
        }
        let entries = engine::advance_region_entries(&engine::effective_region_entries(entity), w.new_state_map, &entered, version, now);
        let stored = self.entities.get_mut(&key).expect("the entity being written");
        stored.current_state = Entity::encode_state(w.new_state_map);
        if let Some(context) = w.context {
            stored.context = context;
        }
        stored.state_version = version;
        stored.updated_at = now;
        stored.region_entries = Some(entries);
        let pending = Pending {
            machine_id: machine.machine_id.clone(),
            entity_id: entity.entity_id.clone(),
            from_state: w.from_state.to_string(),
            to_state: w.to_state.to_string(),
            actions: w.actions,
        };
        (dispatched, pending)
    }

    /// One event on one entity, as `transition_core::transition_in_tx` (without child forwarding).
    fn transition(
        &mut self,
        tenant: &str,
        machine: &MachineDefinition,
        entity_id: &str,
        event_type: &str,
        params: &HashMap<String, String>,
        timestamp: i64,
    ) -> Result<(TransitionResponse, Option<Pending>), AppError> {
        let key = (tenant.to_string(), machine.machine_id.clone(), entity_id.to_string());
        let entity = self.entities.get(&key).cloned().ok_or_else(|| entity_missing(&machine.machine_id, entity_id))?;
        let previous = entity.to_response().current_state;
        let mut seen = None;
        for r in self.history.get(&key).into_iter().flatten().filter(|r| r.event_type == event_type && r.timestamp == timestamp) {
            seen = Some(r.event_params.as_ref() == Some(params));
            if seen == Some(true) {
                break;
            }
        }
        match seen {
            Some(true) => {
                return Ok((
                    TransitionResponse {
                        entity_id: entity_id.to_string(),
                        previous_state: previous.clone(),
                        current_state: previous,
                        triggered_by: event_type.to_string(),
                        timestamp,
                        reason: Some("duplicate event (already processed)".into()),
                        ..Default::default()
                    },
                    None,
                ));
            }
            Some(false) => {
                return Err(AppError::Rejected {
                    code: EVENT_IDENTITY_CONFLICT,
                    message: format!("event '{}' at timestamp {} was already committed for entity '{}' with different params", event_type, timestamp, entity_id),
                });
            }
            None => {}
        }
        let Some(tr) = engine::evaluate(machine, &entity, event_type, params) else {
            let desc = if machine.is_parallel() { format!("{:?}", entity.state_map()) } else { entity.current_state.clone() };
            return Ok((
                TransitionResponse {
                    entity_id: entity_id.to_string(),
                    previous_state: previous.clone(),
                    current_state: previous,
                    triggered_by: event_type.to_string(),
                    timestamp,
                    reason: Some(format!("no transition from '{}' on '{}'", desc, event_type)),
                    ..Default::default()
                },
                None,
            ));
        };
        let mut new_state_map = entity.state_map();
        new_state_map.insert(tr.region.clone(), tr.to_state.clone());
        let joins_fired = engine::apply_joins(machine, &mut new_state_map, 10);
        let mut context = entity.context.clone();
        for (k, v) in params {
            context.insert(k.clone(), serde_json::Value::String(v.clone()));
        }
        let display_region = machine.is_parallel().then(|| tr.region.clone());
        let actions = transition_core::collect_write_actions(machine, &tr.to_state, display_region.as_deref(), &joins_fired);
        let (dispatched, pending) = self.write(
            tenant,
            machine,
            &entity,
            Write {
                event_type,
                from_state: &tr.from_state,
                to_state: &tr.to_state,
                region: &tr.region,
                row_region: display_region.clone(),
                event_params: Some(params.clone()),
                timestamp,
                identity_key: engine::event_identity(event_type, timestamp),
                cause: None,
                new_state_map: &new_state_map,
                joins_fired: &joins_fired,
                context: Some(context),
                actions,
            },
        );
        let label = if machine.is_parallel() { format!("{}: {} → {}", tr.region, tr.from_state, tr.to_state) } else { format!("{} → {}", tr.from_state, tr.to_state) };
        Ok((
            TransitionResponse {
                entity_id: entity_id.to_string(),
                previous_state: previous,
                current_state: state_map_to_value(&new_state_map),
                transition: Some(label),
                region: display_region,
                triggered_by: event_type.to_string(),
                actions_dispatched: dispatched,
                joins_fired,
                timestamp,
                ..Default::default()
            },
            Some(pending),
        ))
    }

    /// Every due `$timeout`, once per entry instance, as `scheduler::fire_timeout`.
    fn due_timeouts(&mut self, now: i64) -> Vec<(String, Pending)> {
        let mut fired = Vec::new();
        let machines: Vec<MachineDefinition> = self.machines.values().cloned().collect();
        for machine in &machines {
            for tt in machine.transitions.iter().filter(|t| t.on == "$timeout") {
                let Some(timeout_ms) = tt.guard.as_ref().and_then(|g| g.get("timeout_seconds")).and_then(|s| s.as_i64()).map(|s| s * 1000) else {
                    continue;
                };
                let region = tt.region.as_deref().unwrap_or(DEFAULT_REGION).to_string();
                let candidates: Vec<Entity> = self
                    .entities
                    .values()
                    .filter(|e| e.tenant_id == machine.tenant_id && e.machine_id == machine.machine_id)
                    .filter(|e| e.state_map().get(&region) == Some(&tt.from))
                    .cloned()
                    .collect();
                for entity in candidates {
                    let entries = engine::effective_region_entries(&entity);
                    let Some(entry) = entries.get(&region) else { continue };
                    if !engine::timeout_due(entry, timeout_ms, now) {
                        continue;
                    }
                    let key = engine::timeout_identity(&region, entry.seq);
                    let history_key = (machine.tenant_id.clone(), machine.machine_id.clone(), entity.entity_id.clone());
                    if self.history.get(&history_key).into_iter().flatten().any(|r| r.identity_key.as_deref() == Some(key.as_str())) {
                        continue;
                    }
                    let mut map = entity.state_map();
                    map.insert(region.clone(), tt.to.clone());
                    let joins_fired = if machine.is_parallel() { engine::apply_joins(machine, &mut map, 10) } else { Vec::new() };
                    let region_opt = machine.is_parallel().then_some(region.as_str());
                    let actions = actions::collect_actions_for_state(&machine.actions, &tt.to, region_opt);
                    let cause = serde_json::json!({
                        "kind": "timeout",
                        "region": region,
                        "from": tt.from,
                        "to": tt.to,
                        "entry": {"seq": entry.seq, "entered_at": entry.entered_at, "basis": entry.basis},
                        "timeout_ms": timeout_ms,
                        "due_at": entry.entered_at.saturating_add(timeout_ms),
                    });
                    let snapshot: HashMap<String, String> = entity.context.iter().map(|(k, v)| (k.clone(), v.as_str().map_or_else(|| v.to_string(), str::to_string))).collect();
                    let (_, pending) = self.write(
                        &machine.tenant_id,
                        machine,
                        &entity,
                        Write {
                            event_type: "$timeout",
                            from_state: &tt.from,
                            to_state: &tt.to,
                            region: &region,
                            row_region: machine.is_parallel().then(|| region.clone()),
                            event_params: Some(snapshot),
                            timestamp: now,
                            identity_key: key,
                            cause: Some(cause),
                            new_state_map: &map,
                            joins_fired: &joins_fired,
                            context: None,
                            actions,
                        },
                    );
                    fired.push((machine.tenant_id.clone(), pending));
                }
            }
        }
        fired
    }
}

impl MemState {
    fn lock(&self) -> std::sync::MutexGuard<'_, Store> {
        self.store.lock().unwrap_or_else(|p| p.into_inner())
    }

    fn dispatch(&self, tenant: &str, p: Pending) {
        if !p.actions.is_empty() {
            actions::dispatch_actions(&self.http, &self.ingest_url, p.actions, tenant, &p.machine_id, &p.entity_id, &p.from_state, &p.to_state, None);
        }
    }

    /// An event, then its actions after the write.
    #[allow(clippy::too_many_arguments)]
    fn apply(&self, tenant: &str, machine_id: &str, entity_id: &str, event_type: &str, params: &HashMap<String, String>, timestamp: i64, dispatch: bool) -> Result<TransitionResponse, AppError> {
        let (resp, pending) = {
            let mut store = self.lock();
            let machine = store.machine(tenant, machine_id)?;
            store.transition(tenant, &machine, entity_id, event_type, params, timestamp)?
        };
        if let (true, Some(p)) = (dispatch, pending) {
            self.dispatch(tenant, p);
        }
        Ok(resp)
    }
}

// ── Routes: the same paths, bodies and answers as the database store ─────────

async fn create_machine(State(s): State<MemState>, headers: HeaderMap, Json(req): Json<CreateMachineRequest>) -> Result<impl IntoResponse, AppError> {
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
        sub_machines: req.sub_machines,
    };
    machine.validate().map_err(AppError::BadRequest)?;
    if has_children(&machine) {
        return Err(AppError::BadRequest("child machines are not supported by the memory store".into()));
    }
    let mut store = s.lock();
    let key = (tenant_id, machine.machine_id.clone());
    if store.machines.contains_key(&key) {
        return Err(AppError::Conflict(format!("Machine '{}' already exists", machine.machine_id)));
    }
    store.machines.insert(key, machine.clone());
    Ok((StatusCode::CREATED, Json(machine)))
}

async fn list_machines(State(s): State<MemState>, headers: HeaderMap) -> Result<Json<Vec<MachineDefinition>>, AppError> {
    let tenant_id = extract_tenant_id(&headers)?;
    Ok(Json(s.lock().machines.values().filter(|m| m.tenant_id == tenant_id).cloned().collect()))
}

async fn get_machine(State(s): State<MemState>, headers: HeaderMap, Path(machine_id): Path<String>) -> Result<Json<MachineDefinition>, AppError> {
    let tenant_id = extract_tenant_id(&headers)?;
    Ok(Json(s.lock().machine(&tenant_id, &machine_id)?))
}

async fn update_machine(State(s): State<MemState>, headers: HeaderMap, Path(machine_id): Path<String>, Json(req): Json<UpdateMachineRequest>) -> Result<Json<MachineDefinition>, AppError> {
    let tenant_id = extract_tenant_id(&headers)?;
    let mut store = s.lock();
    let mut machine = store.machine(&tenant_id, &machine_id)?;
    machine.states = req.states;
    machine.initial_state = req.initial_state;
    machine.regions = req.regions;
    machine.joins = req.joins;
    machine.transitions = req.transitions;
    machine.actions = req.actions;
    machine.sub_machines = req.sub_machines;
    machine.validate().map_err(AppError::BadRequest)?;
    if has_children(&machine) {
        return Err(AppError::BadRequest("child machines are not supported by the memory store".into()));
    }
    store.machines.insert((tenant_id, machine_id), machine.clone());
    Ok(Json(machine))
}

async fn delete_machine(State(s): State<MemState>, headers: HeaderMap, Path(machine_id): Path<String>) -> Result<StatusCode, AppError> {
    let tenant_id = extract_tenant_id(&headers)?;
    let mut store = s.lock();
    let count = store.entities.keys().filter(|(t, m, _)| *t == tenant_id && *m == machine_id).count();
    if count > 0 {
        return Err(AppError::Conflict(format!("Cannot delete machine '{}': {} entities exist. Delete entities first.", machine_id, count)));
    }
    store.machines.remove(&(tenant_id, machine_id.clone())).map(|_| StatusCode::NO_CONTENT).ok_or_else(|| machine_missing(&machine_id))
}

async fn create_entity(State(s): State<MemState>, headers: HeaderMap, Path(machine_id): Path<String>, Json(req): Json<CreateEntityRequest>) -> Result<impl IntoResponse, AppError> {
    let tenant_id = extract_tenant_id(&headers)?;
    if req.entity_id.is_empty() {
        return Err(AppError::BadRequest("entity_id is required".into()));
    }
    let mut store = s.lock();
    let machine = store.machine(&tenant_id, &machine_id)?;
    let entity = store.create_entity(&tenant_id, &machine, &req.entity_id, req.context.unwrap_or_default())?;
    Ok((StatusCode::CREATED, Json(entity.to_response())))
}

async fn list_entities(State(s): State<MemState>, headers: HeaderMap, Path(machine_id): Path<String>, Query(q): Query<ListEntitiesQuery>) -> Result<Json<Vec<EntityResponse>>, AppError> {
    let tenant_id = extract_tenant_id(&headers)?;
    let store = s.lock();
    let mut out: Vec<&Entity> = store
        .entities
        .values()
        .filter(|e| e.tenant_id == tenant_id && e.machine_id == machine_id)
        .filter(|e| match (&q.region, &q.state) {
            (Some(region), Some(state)) => e.is_parallel() && e.state_map().get(region) == Some(state),
            (None, Some(state)) => &e.current_state == state,
            _ => true,
        })
        .filter(|e| q.updated_since.is_none_or(|t| e.updated_at >= t))
        .collect();
    out.sort_by_key(|e| e.created_at);
    Ok(Json(out.into_iter().map(Entity::to_response).collect()))
}

async fn get_entity(State(s): State<MemState>, headers: HeaderMap, Path((machine_id, entity_id)): Path<(String, String)>) -> Result<Json<EntityResponse>, AppError> {
    let tenant_id = extract_tenant_id(&headers)?;
    let store = s.lock();
    store.entities.get(&(tenant_id, machine_id.clone(), entity_id.clone())).map(|e| Json(e.to_response())).ok_or_else(|| entity_missing(&machine_id, &entity_id))
}

async fn delete_entity(State(s): State<MemState>, headers: HeaderMap, Path((machine_id, entity_id)): Path<(String, String)>) -> Result<StatusCode, AppError> {
    let tenant_id = extract_tenant_id(&headers)?;
    let mut store = s.lock();
    let key = (tenant_id, machine_id.clone(), entity_id.clone());
    store.entities.remove(&key).ok_or_else(|| entity_missing(&machine_id, &entity_id))?;
    store.history.remove(&key);
    Ok(StatusCode::NO_CONTENT)
}

async fn transition_entity(State(s): State<MemState>, headers: HeaderMap, Path((machine_id, entity_id)): Path<(String, String)>, Json(req): Json<TransitionRequest>) -> Result<Json<TransitionResponse>, AppError> {
    let tenant_id = extract_tenant_id(&headers)?;
    let timestamp = req.timestamp.unwrap_or_else(now_millis);
    Ok(Json(s.apply(&tenant_id, &machine_id, &entity_id, &req.event_type, &req.params, timestamp, true)?))
}

async fn get_history(State(s): State<MemState>, headers: HeaderMap, Path((machine_id, entity_id)): Path<(String, String)>) -> Result<Json<Vec<TransitionRecord>>, AppError> {
    let tenant_id = extract_tenant_id(&headers)?;
    let mut rows = s.lock().history.get(&(tenant_id, machine_id, entity_id)).cloned().unwrap_or_default();
    rows.sort_by_key(|r| (r.timestamp, r.id));
    Ok(Json(rows))
}

fn resolve(headers: &HeaderMap, req: &EvaluateRequest) -> Result<(String, String), AppError> {
    let tenant = extract_tenant_id(headers).or_else(|_| {
        req.params.get("key_prefix").cloned().ok_or_else(|| AppError::BadRequest("Missing tenant: provide X-Tenant-Id header or key_prefix in params".into()))
    })?;
    let entity = req.params.get(&req.entity_key).cloned().ok_or_else(|| AppError::BadRequest(format!("entity_key '{}' not found in params", req.entity_key)))?;
    Ok((tenant, entity))
}

fn evaluate_one(s: &MemState, tenant: &str, entity_id: &str, machine_id: &str, req: &EvaluateRequest) -> Result<TransitionResponse, AppError> {
    if req.target.is_some() {
        return Err(AppError::BadRequest("targeted delivery needs child machines, not supported by the memory store".into()));
    }
    {
        let mut store = s.lock();
        let machine = store.machine(tenant, machine_id)?;
        if !store.entities.contains_key(&(tenant.to_string(), machine_id.to_string(), entity_id.to_string())) {
            store.create_entity(tenant, &machine, entity_id, HashMap::new())?;
        }
    }
    let timestamp = req.timestamp.unwrap_or_else(now_millis);
    s.apply(tenant, machine_id, entity_id, &req.event_type, &req.params, timestamp, req.dispatch)
}

async fn evaluate(State(s): State<MemState>, headers: HeaderMap, Path(machine_id): Path<String>, Json(req): Json<EvaluateRequest>) -> Result<Json<TransitionResponse>, AppError> {
    let (tenant, entity_id) = resolve(&headers, &req)?;
    Ok(Json(evaluate_one(&s, &tenant, &entity_id, &machine_id, &req)?))
}

#[derive(Deserialize)]
struct BatchRequest {
    events: Vec<EvaluateRequest>,
}

#[derive(Serialize)]
struct BatchResponse {
    total: usize,
    succeeded: usize,
    failed: usize,
    results: Vec<BatchResult>,
}

#[derive(Serialize)]
struct BatchResult {
    #[serde(skip_serializing_if = "Option::is_none")]
    entity_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    result: Option<TransitionResponse>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

async fn evaluate_batch(State(s): State<MemState>, headers: HeaderMap, Path(machine_id): Path<String>, Json(req): Json<BatchRequest>) -> Result<Json<BatchResponse>, AppError> {
    if req.events.is_empty() {
        return Err(AppError::BadRequest("events array is empty".into()));
    }
    if req.events.len() > 1000 {
        return Err(AppError::BadRequest("batch size exceeds maximum of 1000".into()));
    }
    let results: Vec<BatchResult> = req
        .events
        .iter()
        .map(|e| match resolve(&headers, e).and_then(|(t, id)| evaluate_one(&s, &t, &id, &machine_id, e).map(|r| (id, r))) {
            Ok((id, r)) => BatchResult { entity_id: Some(id), result: Some(r), error: None },
            Err(err) => BatchResult { entity_id: None, result: None, error: Some(err.to_string()) },
        })
        .collect();
    let succeeded = results.iter().filter(|r| r.error.is_none()).count();
    Ok(Json(BatchResponse { total: results.len(), succeeded, failed: results.len() - succeeded, results }))
}

/// The API on the memory store, with its timer (every `timeout_interval_secs`).
pub fn router(api_key: String, http: Client, ingest_url: String, timeout_interval_secs: u64) -> Router {
    let state = MemState { store: Arc::new(Mutex::new(Store::default())), http, ingest_url };
    let ticker = state.clone();
    tokio::spawn(async move {
        let mut every = tokio::time::interval(Duration::from_secs(timeout_interval_secs.max(1)));
        loop {
            every.tick().await;
            let fired = ticker.lock().due_timeouts(now_millis());
            for (tenant, p) in fired {
                ticker.dispatch(&tenant, p);
            }
        }
    });
    info!("store: memory (no database); a restart loses every machine and entity");
    let api = Router::new()
        .route("/machines", post(create_machine).get(list_machines))
        .route("/machines/{machine_id}", get(get_machine).put(update_machine).delete(delete_machine))
        .route("/machines/{machine_id}/entities", post(create_entity).get(list_entities))
        .route("/machines/{machine_id}/entities/{entity_id}", get(get_entity).delete(delete_entity))
        .route("/machines/{machine_id}/entities/{entity_id}/transition", post(transition_entity))
        .route("/machines/{machine_id}/entities/{entity_id}/history", get(get_history))
        .route("/machines/{machine_id}/evaluate", post(evaluate))
        .route("/machines/{machine_id}/evaluate/batch", post(evaluate_batch))
        .layer(axum::middleware::from_fn(auth::require_auth))
        .layer(axum::Extension(auth::ApiKey(api_key)));
    Router::new().nest("/api", api).with_state(state)
}
