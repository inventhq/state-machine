//! DB-backed tests for E1 (atomic writes, dedup identity), E2 (per-region entry instances),
//! E3 (timeout identity, parent/child provenance) and E4 (bound region queries).
//! Each test uses its own disposable SQLite file and an in-process loopback HTTP sink that
//! counts every outbound request (webhooks and events).

use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use axum::extract::{Path, Query, State};
use axum::http::HeaderMap;
use axum::Json;
use serde_json::json;

use crate::db;
use crate::engine;
use crate::errors::{AppError, EVENT_IDENTITY_CONFLICT, UNCORRELATED_CHILD_FINAL};
use crate::models::*;
use crate::routes::{self, AppState};
use crate::scheduler;

struct Fixture {
    state: AppState,
    db_path: PathBuf,
    hits: Arc<AtomicUsize>,
    sink: String,
}

async fn fixture() -> Fixture {
    let dir = std::env::temp_dir().join(format!("sm-e1e4-{}", uuid::Uuid::now_v7()));
    std::fs::create_dir_all(&dir).unwrap();
    let db_path = dir.join("sm.db");
    fixture_at(db_path).await
}

async fn fixture_at(db_path: PathBuf) -> Fixture {
    let database = db::init(db_path.to_str().unwrap(), "").await.unwrap();

    let hits = Arc::new(AtomicUsize::new(0));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let counter = hits.clone();
    let app = axum::Router::new().fallback(move || {
        let counter = counter.clone();
        async move {
            counter.fetch_add(1, Ordering::SeqCst);
            "ok"
        }
    });
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    let sink = format!("http://{}/sink", addr);

    let state = AppState {
        db: Arc::new(database),
        http_client: reqwest::Client::new(),
        event_core_ingest_url: sink.clone(),
        machine_cache: Arc::new(dashmap::DashMap::new()),
        // Empty: ingest-token provisioning makes no network call.
        platform_api_url: String::new(),
        platform_api_key: String::new(),
        ingest_token_cache: Arc::new(dashmap::DashMap::new()),
    };
    Fixture { state, db_path, hits, sink }
}

fn headers(tenant: &str) -> HeaderMap {
    let mut h = HeaderMap::new();
    h.insert("x-tenant-id", tenant.parse().unwrap());
    h
}

async fn mk_machine(fx: &Fixture, tenant: &str, def: serde_json::Value) -> Result<(), AppError> {
    let req: CreateMachineRequest = serde_json::from_value(def).unwrap();
    routes::machines::create_machine(State(fx.state.clone()), headers(tenant), Json(req))
        .await
        .map(|_| ())
}

async fn mk_entity(fx: &Fixture, tenant: &str, machine: &str, entity: &str) {
    let req: CreateEntityRequest = serde_json::from_value(json!({ "entity_id": entity })).unwrap();
    routes::entities::create_entity(State(fx.state.clone()), headers(tenant), Path(machine.to_string()), Json(req))
        .await
        .map(|_| ())
        .unwrap();
}

#[allow(clippy::too_many_arguments)]
async fn eval(
    fx: &Fixture,
    tenant: &str,
    machine: &str,
    entity: &str,
    event: &str,
    timestamp: i64,
    extra: &[(&str, &str)],
    dispatch: bool,
) -> Result<TransitionResponse, AppError> {
    let mut params = serde_json::Map::new();
    params.insert("eid".into(), json!(entity));
    for (k, v) in extra {
        params.insert((*k).into(), json!(v));
    }
    let req: EvaluateRequest = serde_json::from_value(json!({
        "event_type": event, "entity_key": "eid", "params": params,
        "timestamp": timestamp, "dispatch": dispatch,
    }))
    .unwrap();
    routes::evaluate::evaluate(State(fx.state.clone()), headers(tenant), Path(machine.to_string()), Json(req))
        .await
        .map(|j| j.0)
}

async fn entity(fx: &Fixture, tenant: &str, machine: &str, entity: &str) -> Result<Entity, AppError> {
    routes::entities::load_entity(&fx.state, tenant, machine, entity).await
}

async fn history(fx: &Fixture, tenant: &str, machine: &str, entity: &str) -> Vec<TransitionRecord> {
    routes::transitions::get_history(
        State(fx.state.clone()),
        headers(tenant),
        Path((machine.to_string(), entity.to_string())),
    )
    .await
    .unwrap()
    .0
}

async fn sql(fx: &Fixture, statement: &str) {
    fx.state.db.connect().unwrap().execute_batch(statement).await.unwrap();
}

async fn tick(fx: &Fixture, now: i64) -> scheduler::TickReport {
    scheduler::tick(&fx.state.db, &fx.state.http_client, &fx.sink, now).await.unwrap()
}

/// Wait up to 2 s for the sink to reach `n` hits; return the final count after a settle delay.
async fn sink_hits(fx: &Fixture, n: usize) -> usize {
    for _ in 0..40 {
        if fx.hits.load(Ordering::SeqCst) >= n {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    tokio::time::sleep(Duration::from_millis(200)).await;
    fx.hits.load(Ordering::SeqCst)
}

fn entries(e: &Entity) -> std::collections::BTreeMap<String, RegionEntry> {
    engine::effective_region_entries(e)
}

fn is_rejected(r: &Result<TransitionResponse, AppError>, code: &str) -> bool {
    matches!(r, Err(AppError::Rejected { code: c, .. }) if *c == code)
}

const T: &str = "e1e4-test";

// ── E1: atomic writes, no effect before a failed commit ─────────────────────

#[tokio::test]
async fn e1_history_fault_rolls_back_state_and_sends_nothing() {
    let fx = fixture().await;
    mk_machine(&fx, T, json!({
        "machine_id": "chain", "states": ["a","b","c"], "initial_state": "a",
        "transitions": [{"from":"a","to":"b","on":"fault.e"},{"from":"b","to":"c","on":"fault.e"}],
        "actions": [{"on_enter":"b","action":{"type":"webhook","url": fx.sink}},
                    {"on_enter":"c","action":{"type":"webhook","url": fx.sink}}]
    })).await.unwrap();
    sql(&fx, "CREATE TRIGGER inject_hist_fault BEFORE INSERT ON transitions WHEN NEW.event_type='fault.e' BEGIN SELECT RAISE(ABORT,'injected history fault'); END;").await;

    let r = eval(&fx, T, "chain", "g1", "fault.e", 1001, &[("rx_event_id", "evt-g1")], true).await;
    assert!(matches!(&r, Err(AppError::Internal(m)) if m.contains("injected history fault")), "{:?}", r.err());
    let e = entity(&fx, T, "chain", "g1").await.unwrap();
    assert_eq!((e.current_state.as_str(), e.state_version), ("a", 1), "state rolled back");
    assert!(!e.context.contains_key("rx_event_id"), "context rolled back");
    assert!(history(&fx, T, "chain", "g1").await.is_empty());
    assert_eq!(sink_hits(&fx, 1).await, 0, "no network effect before a failed commit");

    // R1 replay: applied exactly once (a→b), not a second edge.
    sql(&fx, "DROP TRIGGER inject_hist_fault;").await;
    let r = eval(&fx, T, "chain", "g1", "fault.e", 1001, &[("rx_event_id", "evt-g1")], true).await.unwrap();
    assert_eq!(r.transition.as_deref(), Some("a → b"));
    let again = eval(&fx, T, "chain", "g1", "fault.e", 1001, &[("rx_event_id", "evt-g1")], true).await.unwrap();
    assert_eq!(again.reason.as_deref(), Some("duplicate event (already processed)"));
    assert_eq!(again.current_state, json!("b"));
    let e = entity(&fx, T, "chain", "g1").await.unwrap();
    assert_eq!((e.current_state.as_str(), e.state_version), ("b", 2));
    let h = history(&fx, T, "chain", "g1").await;
    assert_eq!(h.len(), 1);
    assert_eq!(h[0].identity_key.as_deref(), Some(r#"["evt","fault.e",1001]"#));
    assert_eq!(entries(&e)["_"].key, h[0].identity_key);
    assert_eq!(entries(&e)["_"].row, Some(h[0].id));
    assert_eq!(sink_hits(&fx, 1).await, 1, "exactly one action batch after commit");
}

#[tokio::test]
async fn e1_entity_update_fault_rolls_back_history_rows() {
    let fx = fixture().await;
    mk_machine(&fx, T, json!({
        "machine_id": "flat", "states": ["a","b"], "initial_state": "a",
        "transitions": [{"from":"a","to":"b","on":"go"}],
        "actions": [{"on_enter":"b","action":{"type":"event","event_type":"x.entered"}}]
    })).await.unwrap();
    mk_entity(&fx, T, "flat", "u1").await;
    sql(&fx, "CREATE TRIGGER inject_upd_fault BEFORE UPDATE ON entities WHEN NEW.current_state='b' BEGIN SELECT RAISE(ABORT,'injected update fault'); END;").await;
    let r = eval(&fx, T, "flat", "u1", "go", 5, &[], true).await;
    assert!(matches!(&r, Err(AppError::Internal(m)) if m.contains("injected update fault")));
    assert!(history(&fx, T, "flat", "u1").await.is_empty(), "history row inserted before the UPDATE is rolled back");
    assert_eq!(entity(&fx, T, "flat", "u1").await.unwrap().state_version, 1);
    assert_eq!(sink_hits(&fx, 1).await, 0);
}

fn join_machine(id: &str) -> serde_json::Value {
    json!({
        "machine_id": id,
        "regions": [
            {"id":"payment","states":["pending","confirmed"],"initial_state":"pending"},
            {"id":"inventory","states":["pending","reserved"],"initial_state":"pending"},
            {"id":"fulfillment","states":["waiting","ready"],"initial_state":"waiting"}
        ],
        "joins": [{"when":{"payment":"confirmed","inventory":"reserved","fulfillment":"waiting"},
                   "target_region":"fulfillment","target_state":"ready"}],
        "transitions": [
            {"region":"payment","from":"pending","to":"confirmed","on":"pay"},
            {"region":"inventory","from":"pending","to":"reserved","on":"reserve"}
        ],
        "actions": []
    })
}

#[tokio::test]
async fn e1_join_row_fault_rolls_back_whole_transition() {
    let fx = fixture().await;
    mk_machine(&fx, T, join_machine("order")).await.unwrap();
    eval(&fx, T, "order", "o1", "pay", 1, &[], false).await.unwrap();
    sql(&fx, "CREATE TRIGGER inject_join_fault BEFORE INSERT ON transitions WHEN NEW.event_type='$join' BEGIN SELECT RAISE(ABORT,'injected join fault'); END;").await;
    assert!(eval(&fx, T, "order", "o1", "reserve", 2, &[], false).await.is_err());
    let e = entity(&fx, T, "order", "o1").await.unwrap();
    assert_eq!(e.state_version, 2);
    assert_eq!(e.state_map()["inventory"], "pending");
    assert_eq!(history(&fx, T, "order", "o1").await.len(), 1, "only the earlier pay row");

    sql(&fx, "DROP TRIGGER inject_join_fault;").await;
    let r = eval(&fx, T, "order", "o1", "reserve", 2, &[], false).await.unwrap();
    assert_eq!(r.joins_fired.len(), 1);
    let h = history(&fx, T, "order", "o1").await;
    let join = h.iter().find(|r| r.event_type == "$join").unwrap();
    assert_eq!(join.identity_key.as_deref(), Some(r#"["join","fulfillment",3,0]"#));
    assert_eq!(join.cause.as_ref().unwrap()["cause_key"], json!(r#"["evt","reserve",2]"#));
    let e = entity(&fx, T, "order", "o1").await.unwrap();
    assert_eq!(entries(&e)["fulfillment"].key.as_deref(), Some(r#"["join","fulfillment",3,0]"#));
    assert_eq!(entries(&e)["payment"].seq, 2, "unrelated region keeps its instance");
}

fn child_machine(id: &str, timeout_s: Option<i64>) -> serde_json::Value {
    let mut transitions = vec![json!({"from":"c1","to":"c2","on":"seg.step"})];
    if let Some(s) = timeout_s {
        transitions.push(json!({"from":"c1","to":"timed_out","on":"$timeout","guard":{"timeout_seconds": s}}));
    }
    json!({"machine_id": id, "states":["c1","c2","timed_out"], "initial_state":"c1", "transitions": transitions, "actions": []})
}

fn parent_machine(id: &str, child: &str) -> serde_json::Value {
    json!({
        "machine_id": id, "states": ["start","seg","done","escalated","closed"], "initial_state": "start",
        "transitions": [
            {"from":"start","to":"seg","on":"open"},
            {"from":"seg","to":"start","on":"escape"},
            {"from":"done","to":"closed","on":"parent.next"}
        ],
        "sub_machines": {"seg": {"machine_id": child, "on_final": {"c2": "done", "timed_out": "escalated"}}},
        "actions": []
    })
}

#[tokio::test]
async fn e1_parent_completion_fault_rolls_back_child_write() {
    let fx = fixture().await;
    mk_machine(&fx, T, child_machine("child", None)).await.unwrap();
    mk_machine(&fx, T, parent_machine("parent", "child")).await.unwrap();
    eval(&fx, T, "parent", "p1", "open", 1, &[], false).await.unwrap();
    sql(&fx, "CREATE TRIGGER inject_sub_fault BEFORE INSERT ON transitions WHEN NEW.event_type='$sub_complete' BEGIN SELECT RAISE(ABORT,'injected sub fault'); END;").await;
    assert!(eval(&fx, T, "parent", "p1", "seg.step", 2, &[("rx", "s2")], false).await.is_err());
    // The lazily created child, its transition and the parent advance are all rolled back.
    assert!(matches!(entity(&fx, T, "child", "p1::sub::seg").await, Err(AppError::NotFound(_))));
    assert!(history(&fx, T, "child", "p1::sub::seg").await.is_empty());
    assert_eq!(entity(&fx, T, "parent", "p1").await.unwrap().current_state, "seg");

    sql(&fx, "DROP TRIGGER inject_sub_fault;").await;
    let r = eval(&fx, T, "parent", "p1", "seg.step", 2, &[("rx", "s2")], false).await.unwrap();
    assert_eq!(r.current_state, json!("done"));
    assert!(r.sub_machine.as_ref().unwrap().auto_completed);
    let ph = history(&fx, T, "parent", "p1").await;
    let sub = ph.iter().find(|r| r.event_type == "$sub_complete").unwrap();
    assert_eq!(sub.identity_key.as_deref(), Some(r#"["sub","_","seg",2]"#));
    let cause = sub.cause.as_ref().unwrap();
    assert_eq!(cause["via"], json!("forward"));
    assert_eq!(cause["child"]["key"], json!(r#"["evt","seg.step",2]"#));
    assert_eq!(cause["trigger"], json!({"event_type": "seg.step", "timestamp": 2}));
    // Unchanged (E7 not in scope): a replay at the parent is not a parent duplicate.
    let replay = eval(&fx, T, "parent", "p1", "seg.step", 2, &[("rx", "s2")], false).await.unwrap();
    assert!(replay.reason.unwrap().starts_with("no transition"));
    assert_eq!(history(&fx, T, "parent", "p1").await.len(), 2);
}

#[tokio::test]
async fn e1_scheduler_fault_rolls_back_and_fires_once_later() {
    let fx = fixture().await;
    mk_machine(&fx, T, json!({
        "machine_id": "timer", "states": ["a","b"], "initial_state": "a",
        "transitions": [{"from":"a","to":"b","on":"$timeout","guard":{"timeout_seconds":1}}],
        "actions": [{"on_enter":"b","action":{"type":"webhook","url": fx.sink}}]
    })).await.unwrap();
    mk_entity(&fx, T, "timer", "t1").await;
    let due = entries(&entity(&fx, T, "timer", "t1").await.unwrap())["_"].entered_at + 1000;
    sql(&fx, "CREATE TRIGGER inject_to_fault BEFORE INSERT ON transitions WHEN NEW.event_type='$timeout' BEGIN SELECT RAISE(ABORT,'injected timeout fault'); END;").await;
    let rep = tick(&fx, due).await;
    assert_eq!((rep.fired, rep.errors), (0, 1));
    let e = entity(&fx, T, "timer", "t1").await.unwrap();
    assert_eq!((e.current_state.as_str(), e.state_version), ("a", 1));
    assert_eq!(sink_hits(&fx, 1).await, 0);

    sql(&fx, "DROP TRIGGER inject_to_fault;").await;
    assert_eq!(tick(&fx, due).await.fired, 1);
    assert_eq!(tick(&fx, due + 5000).await.fired, 0, "repeated tick fires nothing");
    assert_eq!(history(&fx, T, "timer", "t1").await.len(), 1);
    assert_eq!(sink_hits(&fx, 1).await, 1);
}

// ── E1: identity ────────────────────────────────────────────────────────────

#[tokio::test]
async fn e1_exact_replay_is_duplicate_and_different_payload_conflicts() {
    let fx = fixture().await;
    mk_machine(&fx, T, json!({
        "machine_id": "m", "states": ["a","b","c"], "initial_state": "a",
        "transitions": [{"from":"a","to":"b","on":"go"},{"from":"b","to":"c","on":"go"}], "actions": []
    })).await.unwrap();
    eval(&fx, T, "m", "x", "go", 5, &[("k", "1")], false).await.unwrap();
    let dup = eval(&fx, T, "m", "x", "go", 5, &[("k", "1")], false).await.unwrap();
    assert!(dup.transition.is_none());
    assert_eq!(dup.reason.as_deref(), Some("duplicate event (already processed)"));
    let conflict = eval(&fx, T, "m", "x", "go", 5, &[("k", "2")], false).await;
    assert!(is_rejected(&conflict, EVENT_IDENTITY_CONFLICT), "{:?}", conflict.err());
    let e = entity(&fx, T, "m", "x").await.unwrap();
    assert_eq!((e.current_state.as_str(), e.state_version), ("b", 2));
    assert_eq!(e.context["k"], json!("1"));
    assert_eq!(history(&fx, T, "m", "x").await.len(), 1);
    // A different timestamp is a different event.
    assert_eq!(eval(&fx, T, "m", "x", "go", 6, &[("k", "2")], false).await.unwrap().transition.as_deref(), Some("b → c"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e1_concurrent_duplicates_apply_once() {
    let fx = Arc::new(fixture().await);
    mk_machine(&fx, T, json!({
        "machine_id": "m", "states": ["a","b","c"], "initial_state": "a",
        "transitions": [{"from":"a","to":"b","on":"go"},{"from":"b","to":"c","on":"go"}], "actions": []
    })).await.unwrap();
    mk_entity(&fx, T, "m", "cc").await;
    let mut handles = Vec::new();
    for _ in 0..16 {
        let fx = fx.clone();
        handles.push(tokio::spawn(async move { eval(&fx, T, "m", "cc", "go", 77, &[("k", "v")], false).await }));
    }
    let (mut applied, mut dups, mut locked, mut other) = (0, 0, 0, 0);
    for h in handles {
        match h.await.unwrap() {
            Ok(r) if r.transition.is_some() => applied += 1,
            Ok(r) if r.reason.as_deref() == Some("duplicate event (already processed)") => dups += 1,
            Err(AppError::Internal(m)) if m.contains("locked") => locked += 1,
            _ => other += 1,
        }
    }
    eprintln!("concurrent duplicates: applied={} duplicate={} locked={} other={}", applied, dups, locked, other);
    assert_eq!(applied, 1);
    assert_eq!(other, 0);
    let e = entity(&fx, T, "m", "cc").await.unwrap();
    assert_eq!((e.current_state.as_str(), e.state_version), ("b", 2));
    assert_eq!(history(&fx, T, "m", "cc").await.len(), 1);

    // Concurrent replays after the commit: each is a duplicate or a lock refusal, never a second edge.
    let mut handles = Vec::new();
    for _ in 0..16 {
        let fx = fx.clone();
        handles.push(tokio::spawn(async move { eval(&fx, T, "m", "cc", "go", 77, &[("k", "v")], false).await }));
    }
    let (mut dups, mut locked, mut other) = (0, 0, 0);
    for h in handles {
        match h.await.unwrap() {
            Ok(r) if r.reason.as_deref() == Some("duplicate event (already processed)") => dups += 1,
            Err(AppError::Internal(m)) if m.contains("locked") => locked += 1,
            _ => other += 1,
        }
    }
    eprintln!("concurrent replays after commit: duplicate={} locked={} other={}", dups, locked, other);
    assert_eq!(other, 0);
    assert!(dups >= 1);
    let e = entity(&fx, T, "m", "cc").await.unwrap();
    assert_eq!((e.current_state.as_str(), e.state_version), ("b", 2));
    assert_eq!(history(&fx, T, "m", "cc").await.len(), 1);
}

#[tokio::test]
async fn e1_old_schema_startup_keeps_and_consults_legacy_duplicates() {
    let dir = std::env::temp_dir().join(format!("sm-e1e4-old-{}", uuid::Uuid::now_v7()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("sm.db");
    {
        // The 226279ae schema, written without any E1–E4 column.
        let old = libsql::Builder::new_local(path.to_str().unwrap()).build().await.unwrap();
        let c = old.connect().unwrap();
        c.execute_batch(r#"
            CREATE TABLE machines (machine_id TEXT NOT NULL, tenant_id TEXT NOT NULL, definition TEXT NOT NULL, created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL, PRIMARY KEY (tenant_id, machine_id));
            CREATE TABLE entities (machine_id TEXT NOT NULL, tenant_id TEXT NOT NULL, entity_id TEXT NOT NULL, current_state TEXT NOT NULL, context TEXT, state_version INTEGER NOT NULL DEFAULT 1, created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL, PRIMARY KEY (tenant_id, machine_id, entity_id));
            CREATE TABLE transitions (id INTEGER PRIMARY KEY AUTOINCREMENT, tenant_id TEXT NOT NULL, machine_id TEXT NOT NULL, entity_id TEXT NOT NULL, from_state TEXT NOT NULL, to_state TEXT NOT NULL, event_type TEXT NOT NULL, event_params TEXT, actions_dispatched TEXT, timestamp INTEGER NOT NULL, created_at INTEGER NOT NULL);
            CREATE INDEX idx_transitions_entity ON transitions(tenant_id, machine_id, entity_id);
            CREATE INDEX idx_transitions_time ON transitions(tenant_id, timestamp);
            CREATE INDEX idx_transitions_dedup ON transitions(tenant_id, machine_id, entity_id, event_type, timestamp);
            CREATE INDEX idx_entities_state ON entities(tenant_id, machine_id, current_state);
            ALTER TABLE transitions ADD COLUMN region TEXT DEFAULT '';
            CREATE TABLE ingest_tokens (tenant_id TEXT PRIMARY KEY, token TEXT NOT NULL, created_at INTEGER NOT NULL);
            INSERT INTO machines VALUES ('legacy', 'e1e4-test', '{"machine_id":"legacy","tenant_id":"e1e4-test","states":["a","b","c"],"initial_state":"a","transitions":[{"from":"a","to":"b","on":"go"},{"from":"b","to":"c","on":"go"}],"actions":[]}', 1, 1);
            INSERT INTO entities VALUES ('legacy', 'e1e4-test', 'L', 'b', '{"eid":"L","n":"1"}', 3, 100, 900);
            INSERT INTO transitions (tenant_id, machine_id, entity_id, from_state, to_state, event_type, event_params, actions_dispatched, timestamp, created_at, region) VALUES
              ('e1e4-test','legacy','L','a','b','go','{"eid":"L","n":"1"}','[]',10,500,''),
              ('e1e4-test','legacy','L','b','b','go','{"eid":"L","n":"1"}','[]',10,900,'');
        "#).await.unwrap();
    }
    let fx = fixture_at(path).await;
    let conn = fx.state.db.connect().unwrap();
    assert_eq!(db::report_legacy_duplicates(&conn).await.unwrap(), 1, "legacy duplicate group reported");
    let h = history(&fx, T, "legacy", "L").await;
    assert_eq!(h.len(), 2, "legacy duplicates retained, not deleted");
    assert!(h.iter().all(|r| r.identity_key.is_none() && r.cause.is_none()));

    let e = entity(&fx, T, "legacy", "L").await.unwrap();
    assert!(e.region_entries.is_none());
    let en = entries(&e);
    assert_eq!((en["_"].basis, en["_"].entered_at, en["_"].seq), (EntryBasis::LegacyExact, 900, 3));

    let dup = eval(&fx, T, "legacy", "L", "go", 10, &[("n", "1")], false).await.unwrap();
    assert_eq!(dup.reason.as_deref(), Some("duplicate event (already processed)"));
    assert!(is_rejected(&eval(&fx, T, "legacy", "L", "go", 10, &[("n", "2")], false).await, EVENT_IDENTITY_CONFLICT));
    // A new event persists entries on the legacy row.
    eval(&fx, T, "legacy", "L", "go", 11, &[("n", "1")], false).await.unwrap();
    let e = entity(&fx, T, "legacy", "L").await.unwrap();
    assert_eq!(e.region_entries.as_ref().unwrap().version, 4);
    assert_eq!(entries(&e)["_"].basis, EntryBasis::Transition);

    // A second startup on the migrated file is a no-op.
    drop(conn);
    let again = db::init(fx.db_path.to_str().unwrap(), "").await.unwrap();
    let c2 = again.connect().unwrap();
    assert_eq!(db::report_legacy_duplicates(&c2).await.unwrap(), 1);
}

// ── E2: per-region entry instances ──────────────────────────────────────────

fn two_region_timer(id: &str) -> serde_json::Value {
    json!({
        "machine_id": id,
        "regions": [
            {"id":"A","states":["a1","a2"],"initial_state":"a1"},
            {"id":"B","states":["b1","b2"],"initial_state":"b1"}
        ],
        "transitions": [
            {"region":"A","from":"a1","to":"a2","on":"$timeout","guard":{"timeout_seconds":2}},
            {"region":"A","from":"a2","to":"a1","on":"rearm"},
            {"region":"A","from":"a1","to":"a1","on":"poke"},
            {"region":"B","from":"b1","to":"b2","on":"flip"},
            {"region":"B","from":"b2","to":"b1","on":"flip"}
        ],
        "actions": []
    })
}

#[tokio::test]
async fn e2_unrelated_region_does_not_postpone_timeout() {
    let fx = fixture().await;
    mk_machine(&fx, T, two_region_timer("par")).await.unwrap();
    mk_entity(&fx, T, "par", "p1").await;
    let a0 = entries(&entity(&fx, T, "par", "p1").await.unwrap())["A"].clone();
    for i in 0..3 {
        tokio::time::sleep(Duration::from_millis(5)).await;
        eval(&fx, T, "par", "p1", "flip", 100 + i, &[], false).await.unwrap();
    }
    let e = entity(&fx, T, "par", "p1").await.unwrap();
    assert_eq!(entries(&e)["A"], a0, "B events leave A's instance untouched");
    assert!(e.updated_at > a0.entered_at);
    let b = entries(&e)["B"].clone();

    assert_eq!(tick(&fx, a0.entered_at + 1999).await.fired, 0, "not early");
    assert_eq!(tick(&fx, a0.entered_at + 2000).await.fired, 1, "due from A's own entry");
    let e = entity(&fx, T, "par", "p1").await.unwrap();
    assert_eq!(e.state_map()["A"], "a2");
    let en = entries(&e);
    assert_eq!(en["B"], b, "the timeout leaves B's instance untouched");
    let h = history(&fx, T, "par", "p1").await;
    let to = h.iter().find(|r| r.event_type == "$timeout").unwrap();
    assert_eq!(to.identity_key.as_deref(), Some(r#"["timeout","A",1]"#));
    assert_eq!(en["A"].key, to.identity_key);
    assert_eq!(en["A"].row, Some(to.id));
    let cause = to.cause.as_ref().unwrap();
    assert_eq!(cause["entry"]["seq"], json!(1));
    assert_eq!(cause["due_at"], json!(a0.entered_at + 2000));
}

#[tokio::test]
async fn e2_reentry_and_self_transition_start_new_instances() {
    let fx = fixture().await;
    mk_machine(&fx, T, two_region_timer("par")).await.unwrap();
    mk_entity(&fx, T, "par", "p1").await;
    let first = entries(&entity(&fx, T, "par", "p1").await.unwrap())["A"].clone();
    tokio::time::sleep(Duration::from_millis(5)).await;
    eval(&fx, T, "par", "p1", "poke", 1, &[], false).await.unwrap();
    let poked = entries(&entity(&fx, T, "par", "p1").await.unwrap())["A"].clone();
    assert_eq!(poked.seq, 2, "self-transition re-enters");
    assert!(poked.entered_at > first.entered_at);
    assert_eq!(tick(&fx, first.entered_at + 2000).await.fired, 0, "timer restarted by the self-transition");
    assert_eq!(tick(&fx, poked.entered_at + 2000).await.fired, 1);
    eval(&fx, T, "par", "p1", "rearm", 2, &[], false).await.unwrap();
    let rearmed = entries(&entity(&fx, T, "par", "p1").await.unwrap())["A"].clone();
    assert_eq!((rearmed.state.as_str(), rearmed.seq), ("a1", 4), "re-entry is a new instance");
    assert_eq!(tick(&fx, rearmed.entered_at + 2000).await.fired, 1);
    let keys: Vec<_> = history(&fx, T, "par", "p1").await.into_iter()
        .filter(|r| r.event_type == "$timeout").map(|r| r.identity_key.unwrap()).collect();
    assert_eq!(keys, vec![r#"["timeout","A",2]"#.to_string(), r#"["timeout","A",4]"#.to_string()]);
}

#[tokio::test]
async fn e2_legacy_rows_migrate_truthfully() {
    let fx = fixture().await;
    mk_machine(&fx, T, two_region_timer("par")).await.unwrap();
    // Parallel row last written at t=900 by a pre-E2 binary (version 4, no region_entries).
    sql(&fx, r#"INSERT INTO entities (machine_id, tenant_id, entity_id, current_state, context, state_version, created_at, updated_at)
                VALUES ('par','e1e4-test','old','{"A":"a1","B":"b2"}','{}',4,100,900);"#).await;
    let e = entity(&fx, T, "par", "old").await.unwrap();
    let a = entries(&e)["A"].clone();
    assert_eq!((a.basis, a.entered_at, a.seq), (EntryBasis::LegacyUpperBound, 900, 4));
    assert_eq!(tick(&fx, 900 + 1999).await.fired, 0, "upper bound: never early");

    // A B event now persists A's legacy entry unchanged instead of resetting it to now.
    eval(&fx, T, "par", "old", "flip", 1, &[], false).await.unwrap();
    let e = entity(&fx, T, "par", "old").await.unwrap();
    assert_eq!(e.region_entries.as_ref().unwrap().regions["A"], a);
    assert!(e.updated_at > 900 + 2000);
    assert_eq!(tick(&fx, 900 + 2000).await.fired, 1, "not postponed by the B event");
}

#[tokio::test]
async fn e2_stale_entries_from_non_e2_writer_never_fire_early() {
    let fx = fixture().await;
    mk_machine(&fx, T, two_region_timer("par")).await.unwrap();
    mk_entity(&fx, T, "par", "s").await;
    let created = entries(&entity(&fx, T, "par", "s").await.unwrap())["A"].entered_at;
    // An older binary re-enters A (a1→a2→a1 collapsed) at created+60000 without maintaining entries.
    let later = created + 60_000;
    sql(&fx, &format!("UPDATE entities SET state_version = 3, updated_at = {} WHERE entity_id = 's';", later)).await;
    let a = entries(&entity(&fx, T, "par", "s").await.unwrap())["A"].clone();
    assert_eq!((a.basis, a.entered_at), (EntryBasis::LegacyUpperBound, later));
    assert_eq!(tick(&fx, created + 2000).await.fired, 0);
    assert_eq!(tick(&fx, later + 2000).await.fired, 1);
}

// ── E3: timeout identity, joins, parent/child provenance ────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e3_timeout_identity_survives_restart_and_concurrent_ticks() {
    let fx = fixture().await;
    mk_machine(&fx, T, json!({
        "machine_id": "timer", "states": ["a","b"], "initial_state": "a",
        "transitions": [{"from":"a","to":"b","on":"$timeout","guard":{"timeout_seconds":1}}],
        "actions": [{"on_enter":"b","action":{"type":"event","event_type":"timer.b"}}]
    })).await.unwrap();
    for i in 0..5 {
        mk_entity(&fx, T, "timer", &format!("t{}", i)).await;
    }
    let due = entries(&entity(&fx, T, "timer", "t4").await.unwrap())["_"].entered_at + 1000;
    // Three ticks on separate worker threads (as if from three processes or overlapping intervals).
    let mut handles = Vec::new();
    for _ in 0..3 {
        let (db, client, sink) = (fx.state.db.clone(), fx.state.http_client.clone(), fx.sink.clone());
        handles.push(tokio::spawn(async move { scheduler::tick(&db, &client, &sink, due).await.unwrap() }));
    }
    let mut reports = Vec::new();
    for h in handles {
        reports.push(h.await.unwrap());
    }
    eprintln!("concurrent ticks: {:?}", reports);
    let mut fired: usize = reports.iter().map(|r| r.fired).sum();
    // A tick that lost the write lock retries on the next pass.
    fired += tick(&fx, due).await.fired;
    assert_eq!(fired, 5, "each instance fires exactly once");
    for i in 0..5 {
        let h = history(&fx, T, "timer", &format!("t{}", i)).await;
        assert_eq!(h.len(), 1);
        assert_eq!(h[0].identity_key.as_deref(), Some(r#"["timeout","_",1]"#));
    }

    // Restart: a new process on the same DB file finds nothing due.
    let restarted = fixture_at(fx.db_path.clone()).await;
    assert_eq!(tick(&restarted, due + 60_000).await.fired, 0);
    assert_eq!(sink_hits(&fx, 5).await, 5, "one action batch per instance");
}

#[tokio::test]
async fn e3_timeout_join_receipt_in_same_commit() {
    let fx = fixture().await;
    mk_machine(&fx, T, json!({
        "machine_id": "case",
        "regions": [
            {"id":"case","states":["open","escalated"],"initial_state":"open"},
            {"id":"deadline","states":["watching","lapsed"],"initial_state":"watching"}
        ],
        "joins": [{"when":{"case":"open","deadline":"lapsed"},"target_region":"case","target_state":"escalated"}],
        "transitions": [{"region":"deadline","from":"watching","to":"lapsed","on":"$timeout","guard":{"timeout_seconds":1}}],
        "actions": []
    })).await.unwrap();
    mk_entity(&fx, T, "case", "k1").await;
    let due = entries(&entity(&fx, T, "case", "k1").await.unwrap())["deadline"].entered_at + 1000;
    assert_eq!(tick(&fx, due).await.fired, 1);
    let e = entity(&fx, T, "case", "k1").await.unwrap();
    assert_eq!(e.state_version, 2, "timeout and join are one write");
    assert_eq!(e.state_map()["case"], "escalated");
    let h = history(&fx, T, "case", "k1").await;
    assert_eq!(h.len(), 2);
    let to = h.iter().find(|r| r.event_type == "$timeout").unwrap();
    let join = h.iter().find(|r| r.event_type == "$join").unwrap();
    assert_eq!(to.identity_key.as_deref(), Some(r#"["timeout","deadline",1]"#));
    assert_eq!(join.identity_key.as_deref(), Some(r#"["join","case",2,0]"#));
    assert_eq!(join.cause.as_ref().unwrap()["cause_key"], json!(r#"["timeout","deadline",1]"#));
    let en = entries(&e);
    assert_eq!(en["case"].key, join.identity_key);
    assert_eq!(en["deadline"].key, to.identity_key);
}

#[tokio::test]
async fn e3_child_timeout_completes_parent_once_with_provenance() {
    let fx = fixture().await;
    mk_machine(&fx, T, child_machine("child", Some(1))).await.unwrap();
    mk_machine(&fx, T, parent_machine("parent", "child")).await.unwrap();
    eval(&fx, T, "parent", "p1", "open", 1, &[], false).await.unwrap();
    // The adapter pre-creates the child at entry (starts its timer).
    mk_entity(&fx, T, "child", "p1::sub::seg").await;
    let due = entries(&entity(&fx, T, "child", "p1::sub::seg").await.unwrap())["_"].entered_at + 1000;
    assert_eq!(tick(&fx, due).await.fired, 1);
    assert_eq!(entity(&fx, T, "parent", "p1").await.unwrap().current_state, "seg", "scheduler never advances a parent");
    let child = entity(&fx, T, "child", "p1::sub::seg").await.unwrap();
    let child_entry = entries(&child)["_"].clone();
    assert_eq!(child_entry.key.as_deref(), Some(r#"["timeout","_",1]"#));

    // Nudge: no parent edge and no child edge, so the parent advances through recovery.
    let r = eval(&fx, T, "parent", "p1", "seg.nudge", 50, &[], false).await.unwrap();
    assert_eq!(r.current_state, json!("escalated"));
    assert!(r.sub_machine.as_ref().unwrap().auto_completed);
    let ph = history(&fx, T, "parent", "p1").await;
    let subs: Vec<_> = ph.iter().filter(|r| r.event_type == "$sub_complete").collect();
    assert_eq!(subs.len(), 1);
    assert_eq!(subs[0].identity_key.as_deref(), Some(r#"["sub","_","seg",2]"#));
    let cause = subs[0].cause.as_ref().unwrap();
    assert_eq!(cause["via"], json!("recovery"));
    assert_eq!(cause["child"]["key"], json!(r#"["timeout","_",1]"#));
    assert_eq!(cause["child"]["row"], json!(child_entry.row.unwrap()));
    assert_eq!(cause["trigger"]["event_type"], json!("seg.nudge"));
    assert_eq!(cause["parent_entry"]["seq"], json!(2));

    // Replaying the nudge after the parent moved on creates no second completion.
    let replay = eval(&fx, T, "parent", "p1", "seg.nudge", 50, &[], false).await.unwrap();
    assert!(replay.transition.is_none());
    assert_eq!(history(&fx, T, "parent", "p1").await.iter().filter(|r| r.event_type == "$sub_complete").count(), 1);
}

#[tokio::test]
async fn e3_uncorrelated_or_stale_child_final_stays_unknown() {
    let fx = fixture().await;
    mk_machine(&fx, T, child_machine("child", Some(1))).await.unwrap();
    mk_machine(&fx, T, parent_machine("parent", "child")).await.unwrap();

    // (a) Child made final out of band, with no committed history row.
    eval(&fx, T, "parent", "p1", "open", 1, &[], false).await.unwrap();
    mk_entity(&fx, T, "child", "p1::sub::seg").await;
    sql(&fx, "UPDATE entities SET current_state = 'timed_out', state_version = 2 WHERE entity_id = 'p1::sub::seg';").await;
    let r = eval(&fx, T, "parent", "p1", "seg.nudge", 2, &[], false).await;
    assert!(is_rejected(&r, UNCORRELATED_CHILD_FINAL), "{:?}", r.err());
    let p = entity(&fx, T, "parent", "p1").await.unwrap();
    assert_eq!((p.current_state.as_str(), p.state_version), ("seg", 2));
    assert!(history(&fx, T, "parent", "p1").await.iter().all(|r| r.event_type != "$sub_complete"));

    // (b) Stale child: it timed out during an earlier instance of the compound state.
    eval(&fx, T, "parent", "p2", "open", 1, &[], false).await.unwrap();
    mk_entity(&fx, T, "child", "p2::sub::seg").await;
    let due = entries(&entity(&fx, T, "child", "p2::sub::seg").await.unwrap())["_"].entered_at + 1000;
    assert_eq!(tick(&fx, due).await.fired, 1);
    eval(&fx, T, "parent", "p2", "escape", 2, &[], false).await.unwrap();
    eval(&fx, T, "parent", "p2", "open", 3, &[], false).await.unwrap();
    let r = eval(&fx, T, "parent", "p2", "seg.nudge", 4, &[], false).await;
    assert!(is_rejected(&r, UNCORRELATED_CHILD_FINAL), "{:?}", r.err());
    assert_eq!(entity(&fx, T, "parent", "p2").await.unwrap().current_state, "seg");
}

// ── E4: bound region queries ────────────────────────────────────────────────

#[tokio::test]
async fn e4_list_region_filter_is_bound_and_tenant_scoped() {
    let fx = fixture().await;
    let odd = "r'1 \"x\".y) OR 1=1 --";
    for tenant in ["e4-a", "e4-b"] {
        mk_machine(&fx, tenant, json!({
            "machine_id": "par",
            "regions": [{"id":"r1","states":["x","y"],"initial_state":"x"},
                        {"id": odd, "states":["p","q"],"initial_state":"p"}],
            "transitions": [{"region":"r1","from":"x","to":"y","on":"go"}],
            "actions": []
        })).await.unwrap();
        mk_entity(&fx, tenant, "par", &format!("{}-ent", tenant)).await;
    }
    mk_machine(&fx, "e4-a", json!({"machine_id":"flat","states":["x"],"initial_state":"x","transitions":[],"actions":[]})).await.unwrap();
    mk_entity(&fx, "e4-a", "flat", "f").await;

    let list = |machine: &'static str, region: &str, state: &str| {
        let q = ListEntitiesQuery { state: Some(state.to_string()), region: Some(region.to_string()), updated_since: None };
        let st = fx.state.clone();
        async move {
            routes::entities::list_entities(State(st), headers("e4-a"), Path(machine.to_string()), Query(q))
                .await
                .map(|j| j.0.into_iter().map(|e| e.entity_id).collect::<Vec<_>>())
        }
    };
    assert_eq!(list("par", "r1", "x").await.unwrap(), vec!["e4-a-ent"]);
    assert_eq!(list("par", odd, "p").await.unwrap(), vec!["e4-a-ent"], "unusual names are matched literally");
    for crafted in [
        "r1') = ?3 OR 1=1 OR json_extract(current_state, '$.r1",
        "r'1",
        "$.r1",
        "r1\"",
        "",
    ] {
        assert_eq!(list("par", crafted, "x").await.unwrap(), Vec::<String>::new(), "crafted {:?}", crafted);
    }
    assert_eq!(list("flat", "_", "x").await.unwrap(), Vec::<String>::new(), "flat + region: empty, not an error");
}

#[tokio::test]
async fn e4_scheduler_handles_unusual_region_names_without_blocking_others() {
    let fx = fixture().await;
    mk_machine(&fx, T, json!({
        "machine_id": "odd",
        "regions": [{"id":"q'z","states":["s1","s2"],"initial_state":"s1"},
                    {"id":"a.b\"c","states":["u1","u2"],"initial_state":"u1"}],
        "transitions": [
            {"region":"q'z","from":"s1","to":"s2","on":"$timeout","guard":{"timeout_seconds":1}},
            {"region":"a.b\"c","from":"u1","to":"u2","on":"$timeout","guard":{"timeout_seconds":1}}
        ],
        "actions": []
    })).await.unwrap();
    mk_machine(&fx, T, json!({
        "machine_id": "victim", "states": ["v1","v2"], "initial_state": "v1",
        "transitions": [{"from":"v1","to":"v2","on":"$timeout","guard":{"timeout_seconds":1}}], "actions": []
    })).await.unwrap();
    mk_entity(&fx, T, "odd", "o").await;
    mk_entity(&fx, T, "victim", "v").await;
    let rep = tick(&fx, crate::routes::now_millis() + 5000).await;
    assert_eq!((rep.fired, rep.errors), (3, 0));
    let o = entity(&fx, T, "odd", "o").await.unwrap();
    assert_eq!(o.state_map()["q'z"], "s2");
    assert_eq!(o.state_map()["a.b\"c"], "u2");
    assert_eq!(entity(&fx, T, "victim", "v").await.unwrap().current_state, "v2");
}

#[tokio::test]
async fn e4_definition_validation_rejects_ambiguous_region_shapes() {
    let fx = fixture().await;
    let dup = mk_machine(&fx, T, json!({
        "machine_id": "dup",
        "regions": [{"id":"r","states":["a"],"initial_state":"a"},{"id":"r","states":["b"],"initial_state":"b"}],
        "transitions": [], "actions": []
    })).await;
    assert!(matches!(dup, Err(AppError::BadRequest(m)) if m.contains("duplicate region id")));
    let bad_child = mk_machine(&fx, T, json!({
        "machine_id": "badchild",
        "regions": [{"id":"r","states":["a","b"],"initial_state":"a",
                     "sub_machines": {"b": {"machine_id":"c","on_final":{"done":"zzz"}}}}],
        "transitions": [], "actions": []
    })).await;
    assert!(matches!(bad_child, Err(AppError::BadRequest(m)) if m.contains("on_final target 'zzz'")));
}
