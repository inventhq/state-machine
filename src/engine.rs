use std::collections::HashMap;

use crate::models::{Entity, JoinDef, JoinFired, MachineDefinition, TransitionDef, DEFAULT_REGION};

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
}
