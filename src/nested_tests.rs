//! DB-backed tests for managed nested children (statemachine-nested.v1): eager start, atomic
//! completion cascade (events and timers), selected-branch cancellation, exact targeting,
//! re-entry refusal, depth/cycle bounds, rollback and restart.
//!
//! Shape used throughout (depth 3 below a parallel root; one definition reused twice):
//! `main` (parallel: b1, b2, batch) → `ret` (parallel child: case, deadline) → `settle` (flat)
//! → `owned` (flat leaf with a timer).

use std::sync::Arc;

use axum::extract::{Path, State};
use axum::Json;
use serde_json::json;

use crate::e1e4_tests::{entity, entries, eval, fixture, fixture_at, headers, history, is_rejected, mk_entity, mk_machine, sql, tick, Fixture, T};
use crate::errors::{AppError, INSTANCE_CONFLICT, INSTANCE_NOT_ACTIVE, NESTING_DEPTH_EXCEEDED, TARGET_NOT_ACTIVE, EVENT_IDENTITY_CONFLICT};
use crate::models::*;
use crate::routes;

const SETTLE: &str = "::sub::settling";
const OWNED: &str = "::sub::reversing";

async fn eval_t(
    fx: &Fixture,
    machine: &str,
    entity: &str,
    event: &str,
    timestamp: i64,
    target: serde_json::Value,
) -> Result<TransitionResponse, AppError> {
    eval_td(fx, machine, entity, event, timestamp, target, false).await
}

async fn eval_td(
    fx: &Fixture,
    machine: &str,
    entity: &str,
    event: &str,
    timestamp: i64,
    target: serde_json::Value,
    dispatch: bool,
) -> Result<TransitionResponse, AppError> {
    let req: EvaluateRequest = serde_json::from_value(json!({
        "event_type": event, "entity_key": "eid", "params": {"eid": entity},
        "timestamp": timestamp, "dispatch": dispatch, "target": target,
    }))
    .unwrap();
    routes::evaluate::evaluate(State(fx.state.clone()), headers(T), Path(machine.to_string()), Json(req))
        .await
        .map(|j| j.0)
}

fn ret_ref() -> serde_json::Value {
    json!({
        "machine_id": "ret", "lifecycle": "managed",
        "complete_when": [
            {"when": {"case": "returned", "deadline": "closed"}, "target": "returned"},
            {"when": {"case": "kept", "deadline": "closed"}, "target": "kept"},
            {"when": {"case": "requesting", "deadline": "lapsed"}, "target": "lapsed"}
        ]
    })
}

async fn register_shape(fx: &Fixture) {
    mk_machine(fx, T, json!({
        "machine_id": "owned", "states": ["working","done","timed_out"], "initial_state": "working",
        "transitions": [
            {"from":"working","to":"done","on":"approve"},
            {"from":"working","to":"timed_out","on":"$timeout","guard":{"timeout_seconds":2}}
        ],
        "actions": [{"on_enter":"done","action":{"type":"webhook","url": fx.sink}}]
    })).await.unwrap();
    mk_machine(fx, T, json!({
        "machine_id": "settle", "states": ["idle","reversing","settled","needs_owner"], "initial_state": "idle",
        "transitions": [{"from":"idle","to":"reversing","on":"begin"}],
        "sub_machines": {"reversing": {"machine_id":"owned","lifecycle":"managed","on_final":{"done":"settled","timed_out":"needs_owner"}}},
        "actions": []
    })).await.unwrap();
    mk_machine(fx, T, json!({
        "machine_id": "ret",
        "regions": [
            {"id":"case","states":["requesting","settling","returned","kept"],"initial_state":"requesting",
             "sub_machines": {"settling": {"machine_id":"settle","lifecycle":"managed","on_final":{"settled":"returned","needs_owner":"kept"}}}},
            {"id":"deadline","states":["watching","closed","lapsed"],"initial_state":"watching"}
        ],
        "joins": [
            {"when":{"case":"returned","deadline":"watching"},"target_region":"deadline","target_state":"closed"},
            {"when":{"case":"kept","deadline":"watching"},"target_region":"deadline","target_state":"closed"}
        ],
        "transitions": [
            {"region":"case","from":"requesting","to":"settling","on":"decide.return"},
            {"region":"case","from":"requesting","to":"kept","on":"decide.keep"},
            {"region":"deadline","from":"watching","to":"lapsed","on":"$timeout","guard":{"timeout_seconds":60}}
        ],
        "actions": []
    })).await.unwrap();
    mk_machine(fx, T, json!({
        "machine_id": "main",
        "regions": [
            {"id":"b1","states":["ret_b1","returned","kept","lapsed","cancelled"],"initial_state":"ret_b1",
             "sub_machines": {"ret_b1": ret_ref()}},
            {"id":"b2","states":["idle","ret_b2","returned","kept","lapsed","cancelled"],"initial_state":"idle",
             "sub_machines": {"ret_b2": ret_ref()}},
            {"id":"batch","states":["open","done"],"initial_state":"open"}
        ],
        "joins": [{"when":{"b1":"returned","b2":"returned","batch":"open"},"target_region":"batch","target_state":"done"}],
        "transitions": [
            {"region":"b2","from":"idle","to":"ret_b2","on":"open.b2"},
            {"region":"b1","from":"ret_b1","to":"cancelled","on":"cancel.b1"},
            {"region":"b2","from":"ret_b2","to":"cancelled","on":"cancel.b2"}
        ],
        "actions": []
    })).await.unwrap();
}

fn path_b(n: u8, depth: usize) -> serde_json::Value {
    let steps = [
        json!({"region": format!("b{}", n), "state": format!("ret_b{}", n)}),
        json!({"region": "case", "state": "settling"}),
        json!({"state": "reversing"}),
    ];
    json!(steps[..depth].to_vec())
}

fn ids(root: &str, n: u8) -> (String, String, String) {
    let ret = format!("{}::sub::ret_b{}", root, n);
    let settle = format!("{}{}", ret, SETTLE);
    let owned = format!("{}{}", settle, OWNED);
    (ret, settle, owned)
}

/// Drive branch `n` of `root` down to an active `owned` leaf (depth 3).
async fn to_leaf(fx: &Fixture, root: &str, n: u8, t: i64) {
    eval_t(fx, "main", root, "decide.return", t, path_b(n, 1)).await.unwrap();
    eval_t(fx, "main", root, "begin", t + 1, path_b(n, 2)).await.unwrap();
}

fn status(e: &Entity) -> Option<InstanceStatus> {
    e.instance.as_ref().map(|i| i.status)
}

#[tokio::test]
async fn nested_depth3_parallel_child_completes_atomically_and_siblings_stay_independent() {
    let fx = fixture().await;
    register_shape(&fx).await;

    // Root creation starts b1's child (compound initial state) in the same commit.
    mk_entity(&fx, T, "main", "r").await;
    let (ret1, settle1, owned1) = ids("r", 1);
    let r1 = entity(&fx, T, "ret", &ret1).await.unwrap();
    let inst = r1.instance.as_ref().unwrap();
    assert_eq!((inst.status, inst.depth), (InstanceStatus::Active, 1));
    assert_eq!(inst.path[0].entity_id, "r");
    assert_eq!((inst.path[0].region.as_str(), inst.path[0].state.as_str(), inst.path[0].seq), ("b1", "ret_b1", 1));
    assert!(matches!(entity(&fx, T, "ret", &format!("r{}", "::sub::ret_b2")).await, Err(AppError::NotFound(_))));

    // Untargeted root event enters b2's compound state: its (same-definition) child starts.
    let open = eval(&fx, T, "main", "r", "open.b2", 1, &[], false).await.unwrap();
    assert_eq!(open.instances.len(), 1);
    assert_eq!((open.instances[0].kind.as_str(), open.instances[0].entity_id.as_str()), ("started", "r::sub::ret_b2"));
    let (ret2, _, owned2) = ids("r", 2);
    assert_eq!(entity(&fx, T, "ret", &ret2).await.unwrap().instance.unwrap().path[0].seq, 2);

    // Down to depth 3 on b1 only.
    let d = eval_t(&fx, "main", "r", "decide.return", 2, path_b(1, 1)).await.unwrap();
    assert_eq!(d.target.as_ref().unwrap().entity_id, ret1);
    assert_eq!(d.instances[0].entity_id, settle1);
    let b = eval_t(&fx, "main", "r", "begin", 3, path_b(1, 2)).await.unwrap();
    assert_eq!((b.instances[0].entity_id.as_str(), b.instances[0].depth), (owned1.as_str(), 3));
    let leaf = entity(&fx, T, "owned", &owned1).await.unwrap();
    let path: Vec<_> = leaf.instance.as_ref().unwrap().path.iter().map(|s| s.state.clone()).collect();
    assert_eq!(path, vec!["ret_b1", "settling", "reversing"]);

    // The same event aimed at the sibling branch's (inactive) path changes nothing.
    let wrong = eval_t(&fx, "main", "r", "approve", 4, path_b(2, 3)).await;
    assert!(is_rejected(&wrong, TARGET_NOT_ACTIVE), "{:?}", wrong.err());
    let r2 = entity(&fx, T, "ret", &ret2).await.unwrap();
    assert_eq!((r2.state_map()["case"].as_str(), status(&r2)), ("requesting", Some(InstanceStatus::Active)));
    assert!(matches!(entity(&fx, T, "owned", &owned2).await, Err(AppError::NotFound(_))));

    // One targeted event at depth 3 completes owned → settle → ret_b1 → root b1 in one commit.
    let root_version = entity(&fx, T, "main", "r").await.unwrap().state_version;
    let done = eval_t(&fx, "main", "r", "approve", 4, path_b(1, 3)).await.unwrap();
    assert_eq!(done.transition.as_deref(), Some("working → done"));
    let kinds: Vec<_> = done.instances.iter().map(|e| (e.kind.as_str(), e.entity_id.as_str(), e.parent_to.as_deref())).collect();
    assert_eq!(kinds, vec![
        ("completed", owned1.as_str(), Some("settled")),
        ("completed", settle1.as_str(), Some("returned")),
        ("completed", ret1.as_str(), Some("returned")),
    ]);
    let root = entity(&fx, T, "main", "r").await.unwrap();
    assert_eq!(root.state_version, root_version + 1);
    assert_eq!(root.state_map()["b1"], "returned");
    assert_eq!(root.state_map()["b2"], "ret_b2", "sibling untouched");
    assert_eq!(root.state_map()["batch"], "open", "join waits for b2");
    for (m, id) in [("owned", &owned1), ("settle", &settle1), ("ret", &ret1)] {
        assert_eq!(status(&entity(&fx, T, m, id).await.unwrap()), Some(InstanceStatus::Completed), "{}", id);
    }
    let ret_after = entity(&fx, T, "ret", &ret1).await.unwrap();
    assert_eq!(ret_after.state_map()["deadline"], "closed", "join in the completing write");

    // Provenance chain: each parent's $sub_complete names the level below's completing row.
    let root_sub = history(&fx, T, "main", "r").await.into_iter().find(|h| h.event_type == "$sub_complete").unwrap();
    assert_eq!(root_sub.identity_key.as_deref(), Some(r#"["sub","b1","ret_b1",1]"#));
    let cause = root_sub.cause.unwrap();
    assert_eq!(cause["via"], json!("managed"));
    assert_eq!(cause["trigger"], json!({"event_type": "approve", "timestamp": 4}));
    assert_eq!(cause["child"]["key"], json!(r#"["sub","case","settling",2]"#));
    let settle_sub = history(&fx, T, "settle", &settle1).await.into_iter().find(|h| h.event_type == "$sub_complete").unwrap();
    assert_eq!(settle_sub.cause.unwrap()["child"]["key"], json!(r#"["evt","approve",4]"#));

    // Replay is a duplicate even though the instance ended; a new event there is refused.
    let replay = eval_t(&fx, "main", "r", "approve", 4, path_b(1, 3)).await.unwrap();
    assert_eq!(replay.reason.as_deref(), Some("duplicate event (already processed)"));
    let conflict = eval_t(&fx, "main", "r", "approve", 4, json!([{"region":"b1","state":"ret_b1"},{"region":"case","state":"settling"},{"state":"reversing","seq":2}])).await;
    assert!(conflict.is_ok(), "same identity at the same target: still the duplicate");
    let late = eval_t(&fx, "main", "r", "approve", 5, path_b(1, 3)).await;
    assert!(is_rejected(&late, TARGET_NOT_ACTIVE));

    // Branch b2 completes on its own (keep → join closes deadline → kept), batch stays open.
    let keep = eval_t(&fx, "main", "r", "decide.keep", 6, path_b(2, 1)).await.unwrap();
    assert_eq!(keep.instances.iter().filter(|e| e.kind == "completed").count(), 1);
    let root = entity(&fx, T, "main", "r").await.unwrap();
    assert_eq!((root.state_map()["b2"].as_str(), root.state_map()["batch"].as_str()), ("kept", "open"));
    assert_eq!(history(&fx, T, "main", "r").await.iter().filter(|h| h.event_type == "$sub_complete").count(), 2);
}

#[tokio::test]
async fn nested_timeout_at_depth3_cascades_to_root_and_survives_restart() {
    let fx = fixture().await;
    register_shape(&fx).await;
    mk_entity(&fx, T, "main", "t").await;
    to_leaf(&fx, "t", 1, 10).await;
    let (ret1, settle1, owned1) = ids("t", 1);
    let leaf_entry = entries(&entity(&fx, T, "owned", &owned1).await.unwrap())["_"].clone();

    // Restart at the child-active boundary: a new process on the same file.
    let fx2 = fixture_at(fx.db_path.clone()).await;
    assert_eq!(tick(&fx2, leaf_entry.entered_at + 1999).await.fired, 0, "not early");
    let rep = tick(&fx2, leaf_entry.entered_at + 2000).await;
    assert_eq!((rep.fired, rep.errors), (1, 0));
    let root = entity(&fx2, T, "main", "t").await.unwrap();
    assert_eq!(root.state_map()["b1"], "kept", "timeout cascaded through settle (needs_owner) and ret (kept)");
    for (m, id) in [("owned", &owned1), ("settle", &settle1), ("ret", &ret1)] {
        assert_eq!(status(&entity(&fx2, T, m, id).await.unwrap()), Some(InstanceStatus::Completed));
    }
    let root_sub = history(&fx2, T, "main", "t").await.into_iter().find(|h| h.event_type == "$sub_complete").unwrap();
    assert_eq!(root_sub.cause.unwrap()["trigger"]["event_type"], json!("$timeout"));
    let leaf_to = history(&fx2, T, "owned", &owned1).await.into_iter().find(|h| h.event_type == "$timeout").unwrap();
    assert_eq!(leaf_to.identity_key.as_deref(), Some(r#"["timeout","_",1]"#));

    // Nothing in the ended subtree fires again (ret's 60 s deadline included), before or after restart.
    let far = leaf_entry.entered_at + 3_600_000;
    assert_eq!(tick(&fx2, far).await.fired, 0);
    let fx3 = fixture_at(fx.db_path.clone()).await;
    assert_eq!(tick(&fx3, far).await.fired, 0);
    assert_eq!(history(&fx3, T, "main", "t").await.iter().filter(|h| h.event_type == "$sub_complete").count(), 1);
    assert_eq!(crate::e1e4_tests::sink_hits(&fx, 0).await, 0, "the leaf's 'done' action never ran: it timed out");
}

#[tokio::test]
async fn nested_selected_cancel_is_atomic_durable_and_spares_siblings() {
    let fx = fixture().await;
    register_shape(&fx).await;
    mk_entity(&fx, T, "main", "c").await;
    eval(&fx, T, "main", "c", "open.b2", 1, &[], false).await.unwrap();
    to_leaf(&fx, "c", 1, 10).await;
    to_leaf(&fx, "c", 2, 20).await;
    let (ret1, _, owned1) = ids("c", 1);
    let (ret2, settle2, owned2) = ids("c", 2);

    // Cancel b2: the root's own edge (target = root), the whole subtree in one commit.
    let cancel = eval_t(&fx, "main", "c", "cancel.b2", 30, json!([])).await.unwrap();
    assert_eq!(cancel.transition.as_deref(), Some("b2: ret_b2 → cancelled"));
    let kinds: Vec<_> = cancel.instances.iter().map(|e| (e.kind.as_str(), e.entity_id.as_str(), e.reason.as_deref())).collect();
    assert_eq!(kinds, vec![
        ("cancelled", owned2.as_str(), Some("ancestor_cancelled")),
        ("cancelled", settle2.as_str(), Some("ancestor_cancelled")),
        ("cancelled", ret2.as_str(), Some("parent_exit")),
    ]);
    for (m, id) in [("owned", &owned2), ("settle", &settle2), ("ret", &ret2)] {
        let e = entity(&fx, T, m, id).await.unwrap();
        assert_eq!(status(&e), Some(InstanceStatus::Cancelled), "{}", id);
        let h = history(&fx, T, m, id).await;
        let c = h.iter().find(|r| r.event_type == "$sub_cancel").unwrap();
        assert_eq!(c.cause.as_ref().unwrap()["by"]["key"], json!(r#"["evt","cancel.b2",30]"#));
    }
    assert_eq!(entity(&fx, T, "owned", &owned2).await.unwrap().state_map()["_"], "working", "state kept readable");
    for (m, id) in [("owned", &owned1), ("ret", &ret1)] {
        assert_eq!(status(&entity(&fx, T, m, id).await.unwrap()), Some(InstanceStatus::Active), "sibling {}", id);
    }

    // Late events into the cancelled subtree: by path and directly.
    assert!(is_rejected(&eval_t(&fx, "main", "c", "approve", 31, path_b(2, 3)).await, TARGET_NOT_ACTIVE));
    assert!(is_rejected(&eval(&fx, T, "owned", &owned2, "approve", 31, &[], false).await, INSTANCE_NOT_ACTIVE));

    // Timers: the cancelled leaf never fires; the sibling's leaf does and completes b1.
    // Both leaves are due; the 60 s `ret` deadlines are not.
    let due = entries(&entity(&fx, T, "owned", &owned2).await.unwrap())["_"].entered_at + 2000;
    assert_eq!(tick(&fx, due).await.fired, 1);
    assert!(history(&fx, T, "owned", &owned2).await.iter().all(|h| h.event_type != "$timeout"));
    let root = entity(&fx, T, "main", "c").await.unwrap();
    assert_eq!((root.state_map()["b1"].as_str(), root.state_map()["b2"].as_str()), ("kept", "cancelled"));

    // Repeat cancel: same identity is the duplicate; a new one finds no edge.
    let again = eval_t(&fx, "main", "c", "cancel.b2", 30, json!([])).await.unwrap();
    assert_eq!(again.reason.as_deref(), Some("duplicate event (already processed)"));
    let fresh = eval_t(&fx, "main", "c", "cancel.b2", 32, json!([])).await.unwrap();
    assert!(fresh.transition.is_none() && fresh.instances.is_empty());
    assert_eq!(history(&fx, T, "owned", &owned2).await.iter().filter(|h| h.event_type == "$sub_cancel").count(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn nested_completion_race_has_one_durable_winner() {
    let fx = Arc::new(fixture().await);
    register_shape(&fx).await;

    // (a) Event first: the timer finds nothing.
    mk_entity(&fx, T, "main", "ra").await;
    to_leaf(&fx, "ra", 1, 10).await;
    let (_, _, owned_a) = ids("ra", 1);
    let due_a = entries(&entity(&fx, T, "owned", &owned_a).await.unwrap())["_"].entered_at + 2000;
    eval_t(&fx, "main", "ra", "approve", 12, path_b(1, 3)).await.unwrap();
    assert_eq!(tick(&fx, due_a).await.fired, 0);
    assert_eq!(entity(&fx, T, "main", "ra").await.unwrap().state_map()["b1"], "returned");

    // (b) Timer first: the event is refused.
    mk_entity(&fx, T, "main", "rb").await;
    to_leaf(&fx, "rb", 1, 10).await;
    let (_, _, owned_b) = ids("rb", 1);
    let due_b = entries(&entity(&fx, T, "owned", &owned_b).await.unwrap())["_"].entered_at + 2000;
    assert_eq!(tick(&fx, due_b).await.fired, 1);
    assert!(is_rejected(&eval_t(&fx, "main", "rb", "approve", 12, path_b(1, 3)).await, TARGET_NOT_ACTIVE));
    assert_eq!(entity(&fx, T, "main", "rb").await.unwrap().state_map()["b1"], "kept");

    // (c) Truly concurrent: event and timer race; exactly one completion commits.
    for i in 0..5 {
        let root = format!("rc{}", i);
        mk_entity(&fx, T, "main", &root).await;
        to_leaf(&fx, &root, 1, 10).await;
        let (_, settle_c, owned_c) = ids(&root, 1);
        let due = entries(&entity(&fx, T, "owned", &owned_c).await.unwrap())["_"].entered_at + 2000;
        let (f1, f2) = (fx.clone(), fx.clone());
        let r1 = root.clone();
        let ev = tokio::spawn(async move { eval_t(&f1, "main", &r1, "approve", 12, path_b(1, 3)).await });
        // A tick that loses the lock on its machine read is skipped as a whole (no busy timeout).
        let tk = tokio::spawn(async move {
            crate::scheduler::tick(&f2.state.db, &f2.state.http_client, &f2.sink, due).await.map_err(|e| e.to_string())
        });
        let (ev, tk) = (ev.await.unwrap(), tk.await.unwrap());
        // Finish whichever lost to a lock (never a second completion).
        tick(&fx, due).await;
        let _ = eval_t(&fx, "main", &root, "approve", 12, path_b(1, 3)).await;
        let completes = history(&fx, T, "settle", &settle_c).await.iter().filter(|h| h.event_type == "$sub_complete").count();
        let root_e = entity(&fx, T, "main", &root).await.unwrap();
        eprintln!("race {}: event={:?} tick={:?} -> b1={} settle completions={}", i,
            ev.as_ref().map(|r| r.transition.clone()).map_err(|e| e.to_string()), tk, root_e.state_map()["b1"], completes);
        assert_eq!(completes, 1);
        assert!(["returned", "kept"].contains(&root_e.state_map()["b1"].as_str()));
        assert_eq!(history(&fx, T, "main", &root).await.iter().filter(|h| h.event_type == "$sub_complete").count(), 1);
    }
}

#[tokio::test]
async fn managed_reentry_and_self_transition_are_refused_with_nothing_written() {
    let fx = fixture().await;
    register_shape(&fx).await;
    mk_machine(&fx, T, json!({
        "machine_id": "loop", "states": ["idle","work","done"], "initial_state": "idle",
        "transitions": [
            {"from":"idle","to":"work","on":"go"},
            {"from":"work","to":"idle","on":"back"},
            {"from":"work","to":"work","on":"poke"}
        ],
        "sub_machines": {"work": {"machine_id":"owned","lifecycle":"managed","on_final":{"done":"done"}}},
        "actions": []
    })).await.unwrap();
    eval(&fx, T, "loop", "l", "go", 1, &[], false).await.unwrap();
    let poke = eval(&fx, T, "loop", "l", "poke", 2, &[], false).await;
    assert!(is_rejected(&poke, INSTANCE_CONFLICT), "{:?}", poke.err());
    let back = eval(&fx, T, "loop", "l", "back", 3, &[], false).await.unwrap();
    assert_eq!(back.instances[0].kind, "cancelled");
    let v = entity(&fx, T, "loop", "l").await.unwrap().state_version;
    let again = eval(&fx, T, "loop", "l", "go", 4, &[], false).await;
    assert!(is_rejected(&again, INSTANCE_CONFLICT));
    let l = entity(&fx, T, "loop", "l").await.unwrap();
    assert_eq!((l.current_state.as_str(), l.state_version), ("idle", v), "rolled back");
    assert_eq!(history(&fx, T, "loop", "l").await.len(), 2);
}

#[tokio::test]
async fn nesting_depth_and_reference_cycles_are_bounded() {
    let fx = fixture().await;
    // Registration: direct and two-step cycles.
    let selfref = mk_machine(&fx, T, json!({
        "machine_id": "selfref", "regions": [{"id":"r","states":["s"],"initial_state":"s",
            "sub_machines": {"s": {"machine_id":"selfref","lifecycle":"managed"}}}], "transitions": []
    })).await;
    assert!(matches!(selfref, Err(AppError::BadRequest(m)) if m.contains("cycle")));
    let level = |id: &str, child: &str| json!({
        "machine_id": id, "regions": [{"id":"r","states":["s"],"initial_state":"s",
            "sub_machines": {"s": {"machine_id": child, "lifecycle":"managed"}}}], "transitions": []
    });
    mk_machine(&fx, T, level("cy1", "cy2")).await.unwrap();
    let cyc = mk_machine(&fx, T, level("cy2", "cy1")).await;
    assert!(matches!(cyc, Err(AppError::BadRequest(m)) if m.contains("cycle")));

    // Runtime: nine managed levels under d0 (registered root-first, so registration cannot see it).
    mk_machine(&fx, T, level("d0", "d1")).await.unwrap();
    mk_machine(&fx, T, json!({"machine_id":"d9","states":["leaf"],"initial_state":"leaf","transitions":[]})).await.unwrap();
    for i in (1..9).rev() {
        mk_machine(&fx, T, level(&format!("d{}", i), &format!("d{}", i + 1))).await.unwrap();
    }
    let req: CreateEntityRequest = serde_json::from_value(json!({"entity_id": "deep"})).unwrap();
    let r = routes::entities::create_entity(State(fx.state.clone()), headers(T), Path("d0".into()), Json(req)).await;
    assert!(matches!(r, Err(AppError::Rejected { code, .. }) if code == NESTING_DEPTH_EXCEEDED));
    assert!(matches!(entity(&fx, T, "d0", "deep").await, Err(AppError::NotFound(_))), "root creation rolled back");
    assert!(matches!(entity(&fx, T, "d1", "deep::sub::s").await, Err(AppError::NotFound(_))));

    // Eight levels are fine.
    mk_machine(&fx, T, level("e0", "d2")).await.unwrap();
    mk_entity(&fx, T, "e0", "ok8").await;
    let leaf_id = format!("ok8{}", "::sub::s".repeat(8));
    assert_eq!(entity(&fx, T, "d9", &leaf_id).await.unwrap().instance.unwrap().depth, 8);
}

#[tokio::test]
async fn managed_cascade_fault_rolls_back_every_level() {
    let fx = fixture().await;
    register_shape(&fx).await;
    mk_entity(&fx, T, "main", "f").await;
    to_leaf(&fx, "f", 1, 10).await;
    let (ret1, settle1, owned1) = ids("f", 1);
    let before: Vec<i64> = vec![
        entity(&fx, T, "owned", &owned1).await.unwrap().state_version,
        entity(&fx, T, "settle", &settle1).await.unwrap().state_version,
        entity(&fx, T, "ret", &ret1).await.unwrap().state_version,
        entity(&fx, T, "main", "f").await.unwrap().state_version,
    ];
    // Fail only the root's $sub_complete: the top of the cascade.
    sql(&fx, "CREATE TRIGGER inject_root_sub BEFORE INSERT ON transitions WHEN NEW.event_type='$sub_complete' AND NEW.machine_id='main' BEGIN SELECT RAISE(ABORT,'injected root completion fault'); END;").await;
    let r = eval_td(&fx, "main", "f", "approve", 12, path_b(1, 3), true).await;
    assert!(matches!(&r, Err(AppError::Internal(m)) if m.contains("injected root completion fault")));
    let after: Vec<i64> = vec![
        entity(&fx, T, "owned", &owned1).await.unwrap().state_version,
        entity(&fx, T, "settle", &settle1).await.unwrap().state_version,
        entity(&fx, T, "ret", &ret1).await.unwrap().state_version,
        entity(&fx, T, "main", "f").await.unwrap().state_version,
    ];
    assert_eq!(before, after, "nothing committed at any level");
    assert_eq!(status(&entity(&fx, T, "owned", &owned1).await.unwrap()), Some(InstanceStatus::Active));
    assert_eq!(crate::e1e4_tests::sink_hits(&fx, 1).await, 0, "the leaf's action did not run");

    sql(&fx, "DROP TRIGGER inject_root_sub;").await;
    eval_td(&fx, "main", "f", "approve", 12, path_b(1, 3), true).await.unwrap();
    assert_eq!(entity(&fx, T, "main", "f").await.unwrap().state_map()["b1"], "returned");
    assert_eq!(crate::e1e4_tests::sink_hits(&fx, 1).await, 1, "one action batch after the commit");
}

#[tokio::test]
async fn managed_definitions_and_targets_are_validated() {
    let fx = fixture().await;
    register_shape(&fx).await;
    // complete_when needs managed; rules must name every child region; managed names unique.
    let legacy_rules = mk_machine(&fx, T, json!({
        "machine_id": "bad1", "states": ["a","b"], "initial_state": "a", "transitions": [],
        "sub_machines": {"b": {"machine_id":"owned","complete_when":[{"when":{"_":"done"},"target":"a"}]}}
    })).await;
    assert!(matches!(legacy_rules, Err(AppError::BadRequest(m)) if m.contains("requires lifecycle")));
    let partial = mk_machine(&fx, T, json!({
        "machine_id": "bad2", "states": ["a","b"], "initial_state": "a", "transitions": [],
        "sub_machines": {"b": {"machine_id":"ret","lifecycle":"managed","complete_when":[{"when":{"case":"returned"},"target":"a"}]}}
    })).await;
    assert!(matches!(partial, Err(AppError::BadRequest(m)) if m.contains("every child region")));
    let dup = mk_machine(&fx, T, json!({
        "machine_id": "bad3",
        "regions": [{"id":"x","states":["s","t"],"initial_state":"t","sub_machines":{"s":{"machine_id":"owned","lifecycle":"managed"}}},
                    {"id":"y","states":["s","u"],"initial_state":"u"}],
        "transitions": []
    })).await;
    assert!(matches!(dup, Err(AppError::BadRequest(m)) if m.contains("only one region")));

    // A legacy definition serializes exactly as before (no new keys).
    let legacy = SubMachineDef { machine_id: "c".into(), on_final: [("x".to_string(), "y".to_string())].into(), ..Default::default() };
    assert_eq!(serde_json::to_value(&legacy).unwrap(), json!({"machine_id":"c","on_final":{"x":"y"}}));

    // Targets: unknown/legacy compound states are bad requests; conflicting params at the target.
    mk_entity(&fx, T, "main", "v").await;
    let bad = eval_t(&fx, "main", "v", "x", 1, json!([{"region":"b1","state":"returned"}])).await;
    assert!(matches!(bad, Err(AppError::BadRequest(_))));
    eval_t(&fx, "main", "v", "decide.return", 2, path_b(1, 1)).await.unwrap();
    let mut req: EvaluateRequest = serde_json::from_value(json!({
        "event_type": "decide.return", "entity_key": "eid", "params": {"eid": "v", "k": "other"},
        "timestamp": 2, "dispatch": false, "target": path_b(1, 1)})).unwrap();
    req.dispatch = false;
    let c = routes::evaluate::evaluate(State(fx.state.clone()), headers(T), Path("main".into()), Json(req)).await.map(|j| j.0);
    assert!(is_rejected(&c, EVENT_IDENTITY_CONFLICT));
    // A stale seq in the path is refused.
    let stale = eval_t(&fx, "main", "v", "begin", 3, json!([{"region":"b1","state":"ret_b1","seq":99},{"region":"case","state":"settling"}])).await;
    assert!(is_rejected(&stale, TARGET_NOT_ACTIVE));
}
