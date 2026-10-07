use std::collections::{BTreeMap, HashMap};

use crate::models::{
    Entity, EntryBasis, JoinDef, JoinFired, MachineDefinition, RegionEntry, StoredRegionEntries,
    TransitionDef, DEFAULT_REGION,
};

/// Result of evaluating a transition.
pub struct TransitionResult {
    pub region: String,
    pub from_state: String,
    pub to_state: String,
    pub _matched_transition: TransitionDef,
}

/// Evaluate whether a valid transition exists for the given event.
/// Region-aware: checks entity's state in the transition's target region.
pub fn evaluate(
    machine: &MachineDefinition,
    entity: &Entity,
    event_type: &str,
    params: &HashMap<String, String>,
) -> Option<TransitionResult> {
    let state_map = entity.state_map();

    for t in &machine.transitions {
        let region = t.region.as_deref().unwrap_or(DEFAULT_REGION);
        let current = match state_map.get(region) {
            Some(s) => s.as_str(),
            None => continue,
        };

        if current == t.from && t.on == event_type {
            if check_guard(&t.guard, params, &entity.context) {
                return Some(TransitionResult {
                    region: region.to_string(),
                    from_state: current.to_string(),
                    to_state: t.to.clone(),
                    _matched_transition: t.clone(),
                });
            }
        }
    }
    None
}

/// Check all join conditions against a state map.
/// Returns info about every join that is fully satisfied.
pub fn check_joins<'a>(
    machine: &'a MachineDefinition,
    state_map: &HashMap<String, String>,
) -> Vec<(&'a JoinDef, JoinFired)> {
    machine
        .joins
        .iter()
        .filter_map(|j| {
            let all_met = j.when.iter().all(|(region, required)| {
                state_map.get(region).map_or(false, |s| s == required)
            });
            if all_met {
                let from_state = state_map
                    .get(&j.target_region)
                    .cloned()
                    .unwrap_or_default();
                // Don't fire if already in target state (prevent re-fire)
                if from_state == j.target_state {
                    return None;
                }
                Some((
                    j,
                    JoinFired {
                        target_region: j.target_region.clone(),
                        from_state,
                        target_state: j.target_state.clone(),
                    },
                ))
            } else {
                None
            }
        })
        .collect()
}

/// Apply join transitions to a state map, cascading up to `max_depth` times.
/// Returns all joins that fired.
pub fn apply_joins(
    machine: &MachineDefinition,
    state_map: &mut HashMap<String, String>,
    max_depth: usize,
) -> Vec<JoinFired> {
    let mut all_fired = Vec::new();

    for _ in 0..max_depth {
        let satisfied = check_joins(machine, state_map);
        if satisfied.is_empty() {
            break;
        }
        for (_join_def, fired) in satisfied {
            state_map.insert(fired.target_region.clone(), fired.target_state.clone());
            all_fired.push(fired);
        }
    }

    all_fired
}

// ── Identity keys (E1/E3) ───────────────────────────────────────────────────
// Canonical compact JSON arrays, unique per (tenant, machine, entity). None of them uses
// wall-clock time or a from→to string.

/// Row recording an externally submitted event (the existing (event_type, timestamp) identity).
pub fn event_identity(event_type: &str, timestamp: i64) -> String {
    serde_json::json!(["evt", event_type, timestamp]).to_string()
}

/// `$timeout` of region instance `seq`.
pub fn timeout_identity(region: &str, seq: i64) -> String {
    serde_json::json!(["timeout", region, seq]).to_string()
}

/// `$sub_complete` of the compound-state instance `seq` in `region`.
pub fn sub_complete_identity(region: &str, compound_state: &str, seq: i64) -> String {
    serde_json::json!(["sub", region, compound_state, seq]).to_string()
}

/// `$join` that entered instance `seq` of `target_region`, at `step` of the write's cascade.
pub fn join_identity(target_region: &str, seq: i64, step: usize) -> String {
    serde_json::json!(["join", target_region, seq, step]).to_string()
}

/// `$sub_cancel` of a managed child instance whose parent entry was `seq`.
pub fn cancel_identity(parent_seq: i64) -> String {
    serde_json::json!(["cancel", parent_seq]).to_string()
}

/// Entity id of the child of compound `state` (legacy and managed children alike).
pub fn child_entity_id(parent_entity_id: &str, state: &str) -> String {
    format!("{}::sub::{}", parent_entity_id, state)
}

/// True when a stored history `event_params` value equals the request params (as maps).
/// Unparseable or absent stored params never match.
pub fn same_event_params(stored: Option<&str>, params: &HashMap<String, String>) -> bool {
    stored
        .and_then(|s| serde_json::from_str::<HashMap<String, String>>(s).ok())
        .is_some_and(|m| &m == params)
}

// ── Region entry instances (E2) ─────────────────────────────────────────────

/// Entries for a newly created entity: every region enters its initial state at `now`.
pub fn initial_region_entries(state_map: &HashMap<String, String>, now: i64) -> StoredRegionEntries {
    StoredRegionEntries {
        version: 1,
        regions: state_map
            .iter()
            .map(|(region, state)| {
                (
                    region.clone(),
                    RegionEntry {
                        state: state.clone(),
                        seq: 1,
                        entered_at: now,
                        basis: EntryBasis::Created,
                        row: None,
                        key: None,
                    },
                )
            })
            .collect(),
    }
}

/// The current entry instance of every region of `entity`.
///
/// Uses the persisted document when it belongs to the row's `state_version` and agrees with
/// the region's state. Otherwise (rows written before E2, or by a writer without entry
/// tracking) it derives a truthful legacy entry and never claims more than the row proves.
pub fn effective_region_entries(entity: &Entity) -> BTreeMap<String, RegionEntry> {
    let stored = entity
        .region_entries
        .as_ref()
        .filter(|s| s.version == entity.state_version);
    entity
        .state_map()
        .into_iter()
        .map(|(region, state)| {
            let entry = stored
                .and_then(|s| s.regions.get(&region))
                .filter(|e| e.state == state)
                .cloned()
                .unwrap_or_else(|| legacy_region_entry(entity, &state));
            (region, entry)
        })
        .collect()
}

fn legacy_region_entry(entity: &Entity, state: &str) -> RegionEntry {
    let (entered_at, basis) = if entity.state_version <= 1 && entity.updated_at == entity.created_at {
        // Never written since creation.
        (entity.created_at, EntryBasis::Created)
    } else if entity.is_parallel() {
        // Some region changed at updated_at; this one entered its state no later than that.
        (entity.updated_at, EntryBasis::LegacyUpperBound)
    } else {
        // Every write to a flat entity re-enters its only region.
        (entity.updated_at, EntryBasis::LegacyExact)
    };
    RegionEntry {
        state: state.to_string(),
        seq: entity.state_version,
        entered_at,
        basis,
        row: None,
        key: None,
    }
}

/// A region (re-)entered by one write, in the order the write applied it.
pub struct RegionEntered {
    pub region: String,
    pub state: String,
    pub row: i64,
    pub key: String,
}

/// Entries after a write that produced `version` at `now`. Every region in `entered` gets a
/// new instance (later items win, so a join cascade keeps its last step); every other region
/// keeps its previous instance.
pub fn advance_region_entries(
    previous: &BTreeMap<String, RegionEntry>,
    new_state_map: &HashMap<String, String>,
    entered: &[RegionEntered],
    version: i64,
    now: i64,
) -> StoredRegionEntries {
    let mut regions = BTreeMap::new();
    for (region, state) in new_state_map {
        let entry = match entered.iter().rev().find(|e| &e.region == region) {
            Some(e) => RegionEntry {
                state: e.state.clone(),
                seq: version,
                entered_at: now,
                basis: EntryBasis::Transition,
                row: Some(e.row),
                key: Some(e.key.clone()),
            },
            None => match previous.get(region).filter(|p| &p.state == state) {
                Some(p) => p.clone(),
                // Unreachable for consistent rows: a region whose state changed is always entered.
                None => RegionEntry {
                    state: state.clone(),
                    seq: version,
                    entered_at: now,
                    basis: EntryBasis::Transition,
                    row: None,
                    key: None,
                },
            },
        };
        regions.insert(region.clone(), entry);
    }
    StoredRegionEntries { version, regions }
}

/// True when a `$timeout` of `timeout_ms` is due for this region instance at `now`.
pub fn timeout_due(entry: &RegionEntry, timeout_ms: i64, now: i64) -> bool {
    entry.entered_at.saturating_add(timeout_ms) <= now
}

/// Evaluate a guard condition against event params and entity context.
///
/// Guard format (JSON object):
/// ```json
/// { "field": "amount_cents", "op": "gt", "value": 1000 }
/// ```
/// Or compound:
/// ```json
/// { "all": [ { "field": "...", "op": "...", "value": ... }, ... ] }
/// ```
/// Or timeout (handled separately):
/// ```json
/// { "timeout_seconds": 2592000 }
/// ```
///
/// If guard is `None`, the transition is unconditional.
fn check_guard(
    guard: &Option<serde_json::Value>,
    params: &HashMap<String, String>,
    context: &HashMap<String, serde_json::Value>,
) -> bool {
    let guard = match guard {
        Some(g) => g,
        None => return true,
    };

    let obj = match guard.as_object() {
        Some(o) => o,
        None => return true,
    };

    // Timeout guards are evaluated separately (not here — they need a scheduler)
    if obj.contains_key("timeout_seconds") {
        return false;
    }

    // Compound "all" guard
    if let Some(all) = obj.get("all") {
        if let Some(conditions) = all.as_array() {
            return conditions
                .iter()
                .all(|c| check_guard(&Some(c.clone()), params, context));
        }
    }

    // Compound "any" guard
    if let Some(any) = obj.get("any") {
        if let Some(conditions) = any.as_array() {
            return conditions
                .iter()
                .any(|c| check_guard(&Some(c.clone()), params, context));
        }
    }

    // Single condition: { "field": "...", "op": "...", "value": ... }
    let field = match obj.get("field").and_then(|f| f.as_str()) {
        Some(f) => f,
        None => return true,
    };
    let op = match obj.get("op").and_then(|o| o.as_str()) {
        Some(o) => o,
        None => return true,
    };
    let expected = match obj.get("value") {
        Some(v) => v,
        None => return true,
    };

    // Resolve field value: check params first, then entity context
    let actual = if let Some(param_val) = params.get(field) {
        // Try to parse as number for numeric comparisons
        if let Ok(n) = param_val.parse::<f64>() {
            serde_json::Value::from(n)
        } else {
            serde_json::Value::String(param_val.clone())
        }
    } else if let Some(ctx_val) = context.get(field) {
        ctx_val.clone()
    } else {
        return false; // Field not found — guard fails
    };

    compare_values(op, &actual, expected)
}

fn compare_values(op: &str, actual: &serde_json::Value, expected: &serde_json::Value) -> bool {
    match op {
        "eq" => actual == expected,
        "neq" | "ne" => actual != expected,
        "gt" | "gte" | "lt" | "lte" => {
            let a = as_f64(actual);
            let b = as_f64(expected);
            match (a, b) {
                (Some(a), Some(b)) => match op {
                    "gt" => a > b,
                    "gte" => a >= b,
                    "lt" => a < b,
                    "lte" => a <= b,
                    _ => false,
                },
                _ => false,
            }
        }
        "contains" => {
            if let (Some(haystack), Some(needle)) = (actual.as_str(), expected.as_str()) {
                haystack.contains(needle)
            } else {
                false
            }
        }
        _ => {
            tracing::warn!("Unknown guard operator: {}", op);
            false
        }
    }
}

fn as_f64(v: &serde_json::Value) -> Option<f64> {
    v.as_f64().or_else(|| v.as_str().and_then(|s| s.parse().ok()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::*;

    fn test_machine() -> MachineDefinition {
        MachineDefinition {
            machine_id: "test".into(),
            tenant_id: "t1".into(),
            states: vec![
                "new".into(),
                "trial".into(),
                "active".into(),
                "past_due".into(),
                "churned".into(),
            ],
            initial_state: "new".into(),
            regions: vec![],
            joins: vec![],
            transitions: vec![
                TransitionDef {
                    from: "new".into(),
                    to: "trial".into(),
                    on: "trial.started".into(),
                    region: None,
                    guard: None,
                },
                TransitionDef {
                    from: "trial".into(),
                    to: "active".into(),
                    on: "charge.succeeded".into(),
                    region: None,
                    guard: None,
                },
                TransitionDef {
                    from: "active".into(),
                    to: "past_due".into(),
                    on: "invoice.payment_failed".into(),
                    region: None,
                    guard: None,
                },
                TransitionDef {
                    from: "past_due".into(),
                    to: "active".into(),
                    on: "charge.succeeded".into(),
                    region: None,
                    guard: Some(serde_json::json!({
                        "field": "amount_cents",
                        "op": "gt",
                        "value": 0
                    })),
                },
            ],
            actions: vec![],
            sub_machines: HashMap::new(),
        }
    }

    fn test_entity(state: &str) -> Entity {
        Entity {
            machine_id: "test".into(),
            tenant_id: "t1".into(),
            entity_id: "e1".into(),
            current_state: state.into(),
            context: HashMap::new(),
            state_version: 1,
            created_at: 0,
            updated_at: 0,
            region_entries: None,
            instance: None,
        }
    }

    fn test_parallel_machine() -> MachineDefinition {
        MachineDefinition {
            machine_id: "order".into(),
            tenant_id: "t1".into(),
            states: vec![],
            initial_state: String::new(),
            regions: vec![
                RegionDef {
                    id: "payment".into(),
                    states: vec!["pending".into(), "confirmed".into(), "refunded".into()],
                    initial_state: "pending".into(),
                    sub_machines: HashMap::new(),
                },
                RegionDef {
                    id: "inventory".into(),
                    states: vec!["pending".into(), "reserved".into(), "released".into()],
                    initial_state: "pending".into(),
                    sub_machines: HashMap::new(),
                },
                RegionDef {
                    id: "fulfillment".into(),
                    states: vec!["waiting".into(), "ready".into(), "shipped".into()],
                    initial_state: "waiting".into(),
                    sub_machines: HashMap::new(),
                },
            ],
            joins: vec![JoinDef {
                when: HashMap::from([
                    ("payment".into(), "confirmed".into()),
                    ("inventory".into(), "reserved".into()),
                ]),
                target_region: "fulfillment".into(),
                target_state: "ready".into(),
                actions: vec![],
            }],
            transitions: vec![
                TransitionDef {
                    from: "pending".into(),
                    to: "confirmed".into(),
                    on: "payment.confirmed".into(),
                    region: Some("payment".into()),
                    guard: None,
                },
                TransitionDef {
                    from: "pending".into(),
                    to: "reserved".into(),
                    on: "inventory.reserved".into(),
                    region: Some("inventory".into()),
                    guard: None,
                },
                TransitionDef {
                    from: "ready".into(),
                    to: "shipped".into(),
                    on: "order.shipped".into(),
                    region: Some("fulfillment".into()),
                    guard: None,
                },
            ],
            actions: vec![],
            sub_machines: HashMap::new(),
        }
    }

    fn test_parallel_entity(state_json: &str) -> Entity {
        Entity {
            machine_id: "order".into(),
            tenant_id: "t1".into(),
            entity_id: "order_1".into(),
            current_state: state_json.into(),
            context: HashMap::new(),
            state_version: 1,
            created_at: 0,
            updated_at: 0,
            region_entries: None,
            instance: None,
        }
    }

    // ── Flat FSM tests ──────────────────────────────────────────────────────

    #[test]
    fn test_simple_transition() {
        let m = test_machine();
        let e = test_entity("new");
        let result = evaluate(&m, &e, "trial.started", &HashMap::new());
        assert!(result.is_some());
        let r = result.unwrap();
        assert_eq!(r.region, DEFAULT_REGION);
        assert_eq!(r.from_state, "new");
        assert_eq!(r.to_state, "trial");
    }

    #[test]
    fn test_no_valid_transition() {
        let m = test_machine();
        let e = test_entity("active");
        let result = evaluate(&m, &e, "trial.started", &HashMap::new());
        assert!(result.is_none());
    }

    #[test]
    fn test_guard_passes() {
        let m = test_machine();
        let e = test_entity("past_due");
        let mut params = HashMap::new();
        params.insert("amount_cents".into(), "4999".into());
        let result = evaluate(&m, &e, "charge.succeeded", &params);
        assert!(result.is_some());
        assert_eq!(result.unwrap().to_state, "active");
    }

    #[test]
    fn test_guard_fails() {
        let m = test_machine();
        let e = test_entity("past_due");
        let mut params = HashMap::new();
        params.insert("amount_cents".into(), "0".into());
        let result = evaluate(&m, &e, "charge.succeeded", &params);
        assert!(result.is_none());
    }

    // ── Parallel / Statechart tests ─────────────────────────────────────────

    #[test]
    fn test_parallel_region_transition() {
        let m = test_parallel_machine();
        let e = test_parallel_entity(
            r#"{"payment":"pending","inventory":"pending","fulfillment":"waiting"}"#,
        );
        let result = evaluate(&m, &e, "payment.confirmed", &HashMap::new());
        assert!(result.is_some());
        let r = result.unwrap();
        assert_eq!(r.region, "payment");
        assert_eq!(r.from_state, "pending");
        assert_eq!(r.to_state, "confirmed");
    }

    #[test]
    fn test_parallel_wrong_region_no_match() {
        let m = test_parallel_machine();
        // payment already confirmed — should not match payment.confirmed again
        let e = test_parallel_entity(
            r#"{"payment":"confirmed","inventory":"pending","fulfillment":"waiting"}"#,
        );
        let result = evaluate(&m, &e, "payment.confirmed", &HashMap::new());
        assert!(result.is_none());
    }

    #[test]
    fn test_join_not_satisfied() {
        let m = test_parallel_machine();
        // Only payment confirmed, inventory still pending
        let mut state_map = HashMap::from([
            ("payment".into(), "confirmed".into()),
            ("inventory".into(), "pending".into()),
            ("fulfillment".into(), "waiting".into()),
        ]);
        let joins = apply_joins(&m, &mut state_map, 5);
        assert!(joins.is_empty());
        assert_eq!(state_map.get("fulfillment").unwrap(), "waiting");
    }

    #[test]
    fn test_join_fires_when_all_satisfied() {
        let m = test_parallel_machine();
        // Both payment and inventory are in required states
        let mut state_map = HashMap::from([
            ("payment".into(), "confirmed".into()),
            ("inventory".into(), "reserved".into()),
            ("fulfillment".into(), "waiting".into()),
        ]);
        let joins = apply_joins(&m, &mut state_map, 5);
        assert_eq!(joins.len(), 1);
        assert_eq!(joins[0].target_region, "fulfillment");
        assert_eq!(joins[0].target_state, "ready");
        assert_eq!(state_map.get("fulfillment").unwrap(), "ready");
    }

    #[test]
    fn test_join_does_not_refire() {
        let m = test_parallel_machine();
        // Already in target state — join should not fire again
        let mut state_map = HashMap::from([
            ("payment".into(), "confirmed".into()),
            ("inventory".into(), "reserved".into()),
            ("fulfillment".into(), "ready".into()),
        ]);
        let joins = apply_joins(&m, &mut state_map, 5);
        assert!(joins.is_empty());
    }

    // ── E1/E2/E3 pure helpers ───────────────────────────────────────────────

    fn entity_with(state: &str, version: i64, created: i64, updated: i64) -> Entity {
        Entity {
            machine_id: "m".into(),
            tenant_id: "t1".into(),
            entity_id: "e".into(),
            current_state: state.into(),
            context: HashMap::new(),
            state_version: version,
            created_at: created,
            updated_at: updated,
            region_entries: None,
            instance: None,
        }
    }

    #[test]
    fn test_identity_keys_are_canonical_and_distinct() {
        assert_eq!(event_identity("a.b", 7), r#"["evt","a.b",7]"#);
        assert_eq!(timeout_identity("deadline", 3), r#"["timeout","deadline",3]"#);
        assert_eq!(sub_complete_identity("_", "seg", 4), r#"["sub","_","seg",4]"#);
        assert_eq!(join_identity("r'x", 5, 1), r#"["join","r'x",5,1]"#);
        // An external "$timeout" event never collides with a scheduler identity.
        assert_ne!(event_identity("$timeout", 3), timeout_identity("_", 3));
        // Quotes in names cannot forge another key.
        assert_ne!(event_identity("a\",\"b", 1), event_identity("a", 1));
    }

    #[test]
    fn test_same_event_params_compares_maps() {
        let p = HashMap::from([("k".to_string(), "v".to_string()), ("x".to_string(), "1".to_string())]);
        assert!(same_event_params(Some(r#"{"x":"1","k":"v"}"#), &p));
        assert!(!same_event_params(Some(r#"{"x":"2","k":"v"}"#), &p));
        assert!(!same_event_params(Some(r#"{"k":"v"}"#), &p));
        assert!(!same_event_params(None, &p));
        assert!(!same_event_params(Some("not json"), &p));
        assert!(same_event_params(Some("{}"), &HashMap::new()));
    }

    #[test]
    fn test_initial_entries_and_created_basis() {
        let map = HashMap::from([("A".to_string(), "a1".to_string()), ("B".to_string(), "b1".to_string())]);
        let stored = initial_region_entries(&map, 1000);
        assert_eq!(stored.version, 1);
        let a = &stored.regions["A"];
        assert_eq!((a.state.as_str(), a.seq, a.entered_at, a.basis, a.row), ("a1", 1, 1000, EntryBasis::Created, None));
    }

    #[test]
    fn test_unrelated_region_keeps_its_instance() {
        let mut e = entity_with(r#"{"A":"a1","B":"b1"}"#, 1, 1000, 1000);
        e.region_entries = Some(initial_region_entries(&e.state_map(), 1000));
        let before = effective_region_entries(&e);
        // B flips at t=5000 (version 2): A keeps seq 1 / entered_at 1000.
        let new_map = HashMap::from([("A".to_string(), "a1".to_string()), ("B".to_string(), "b2".to_string())]);
        let entered = [RegionEntered { region: "B".into(), state: "b2".into(), row: 7, key: event_identity("flip", 1) }];
        let after = advance_region_entries(&before, &new_map, &entered, 2, 5000);
        assert_eq!(after.version, 2);
        assert_eq!(after.regions["A"], before["A"]);
        let b = &after.regions["B"];
        assert_eq!((b.seq, b.entered_at, b.basis, b.row), (2, 5000, EntryBasis::Transition, Some(7)));
        assert_eq!(b.key.as_deref(), Some(r#"["evt","flip",1]"#));
        assert!(!timeout_due(&after.regions["A"], 5000, 5999));
        assert!(timeout_due(&after.regions["A"], 5000, 6000));
    }

    #[test]
    fn test_reentry_and_self_transition_create_new_instances() {
        let mut e = entity_with("a", 1, 1000, 1000);
        e.region_entries = Some(initial_region_entries(&e.state_map(), 1000));
        let prev = effective_region_entries(&e);
        let map = HashMap::from([("_".to_string(), "a".to_string())]);
        let entered = [RegionEntered { region: "_".into(), state: "a".into(), row: 3, key: event_identity("again", 9) }];
        let after = advance_region_entries(&prev, &map, &entered, 2, 4000);
        assert_eq!(after.regions["_"].seq, 2);
        assert_eq!(after.regions["_"].entered_at, 4000);
        assert_ne!(timeout_identity("_", prev["_"].seq), timeout_identity("_", after.regions["_"].seq));
    }

    #[test]
    fn test_join_cascade_last_step_wins() {
        let prev = BTreeMap::from([("F".to_string(), RegionEntry {
            state: "w".into(), seq: 1, entered_at: 0, basis: EntryBasis::Created, row: None, key: None,
        })]);
        let map = HashMap::from([("F".to_string(), "y".to_string())]);
        let entered = [
            RegionEntered { region: "F".into(), state: "x".into(), row: 10, key: join_identity("F", 2, 0) },
            RegionEntered { region: "F".into(), state: "y".into(), row: 11, key: join_identity("F", 2, 1) },
        ];
        let after = advance_region_entries(&prev, &map, &entered, 2, 50);
        assert_eq!(after.regions["F"].state, "y");
        assert_eq!(after.regions["F"].row, Some(11));
    }

    #[test]
    fn test_legacy_entries_are_truthful() {
        // Flat row written before E2: updated_at is exact.
        let flat = effective_region_entries(&entity_with("b", 3, 100, 900));
        assert_eq!((flat["_"].basis, flat["_"].entered_at, flat["_"].seq), (EntryBasis::LegacyExact, 900, 3));
        // Parallel row: only an upper bound.
        let par = effective_region_entries(&entity_with(r#"{"A":"a1","B":"b2"}"#, 4, 100, 900));
        assert_eq!((par["A"].basis, par["A"].entered_at), (EntryBasis::LegacyUpperBound, 900));
        // Never written since creation: exact creation time.
        let fresh = effective_region_entries(&entity_with(r#"{"A":"a1"}"#, 1, 100, 100));
        assert_eq!((fresh["A"].basis, fresh["A"].entered_at), (EntryBasis::Created, 100));
        // Old pre-version rows (version defaulted to 1 but written later) are not "created".
        let old = effective_region_entries(&entity_with("b", 1, 100, 900));
        assert_eq!(old["_"].basis, EntryBasis::LegacyExact);
    }

    #[test]
    fn test_stale_stored_entries_fall_back_to_legacy() {
        // Stored for version 2, but a non-E2 writer moved the row to version 3 at t=900.
        let mut e = entity_with(r#"{"A":"a1","B":"b1"}"#, 3, 100, 900);
        let mut stored = initial_region_entries(&e.state_map(), 100);
        stored.version = 2;
        e.region_entries = Some(stored);
        let eff = effective_region_entries(&e);
        assert_eq!((eff["A"].basis, eff["A"].entered_at), (EntryBasis::LegacyUpperBound, 900));
        // A stored entry whose state disagrees with the row is not trusted either.
        let mut e2 = entity_with(r#"{"A":"a2"}"#, 2, 100, 900);
        let mut s2 = initial_region_entries(&HashMap::from([("A".to_string(), "a1".to_string())]), 100);
        s2.version = 2;
        e2.region_entries = Some(s2);
        assert_eq!(effective_region_entries(&e2)["A"].basis, EntryBasis::LegacyUpperBound);
    }

    #[test]
    fn test_region_validation_shapes() {
        let mut m = test_parallel_machine();
        assert!(m.validate().is_ok());
        // Previously valid unusual names stay valid (no charset rule).
        m.regions[0].id = "pay'ment \"x\".y".into();
        for t in m.transitions.iter_mut().filter(|t| t.region.as_deref() == Some("payment")) {
            t.region = Some("pay'ment \"x\".y".into());
        }
        m.joins[0].when = HashMap::from([
            ("pay'ment \"x\".y".into(), "confirmed".into()),
            ("inventory".into(), "reserved".into()),
        ]);
        assert!(m.validate().is_ok());

        let mut dup = test_parallel_machine();
        dup.regions[1].id = "payment".into();
        assert!(dup.validate().unwrap_err().contains("duplicate region id"));

        let mut bad_sub = test_parallel_machine();
        bad_sub.regions[2].sub_machines.insert(
            "nope".into(),
            SubMachineDef { machine_id: "child".into(), on_final: HashMap::new(), ..Default::default() },
        );
        assert!(bad_sub.validate().unwrap_err().contains("sub_machine state 'nope'"));

        let mut bad_target = test_parallel_machine();
        bad_target.regions[2].sub_machines.insert(
            "ready".into(),
            SubMachineDef { machine_id: "child".into(), on_final: HashMap::from([("done".into(), "elsewhere".into())]), ..Default::default() },
        );
        assert!(bad_target.validate().unwrap_err().contains("on_final target 'elsewhere'"));
    }
}
