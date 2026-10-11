//! The memory store (`STATE_STORE=memory`) over HTTP: the same API, no database.

use std::sync::{Arc, LazyLock, Mutex};
use std::time::Duration;

use axum::routing::post;
use axum::{Json, Router};
use serde_json::{json, Value};

const KEY: &str = "k";
static CLIENT: LazyLock<reqwest::Client> = LazyLock::new(reqwest::Client::new);

async fn serve(router: Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    format!("http://{addr}")
}

async fn store(timeout_secs: u64) -> String {
    serve(crate::memory::router(KEY.into(), reqwest::Client::new(), "http://127.0.0.1:9/ingest".into(), timeout_secs)).await
}

async fn call(base: &str, method: reqwest::Method, path: &str, body: Option<Value>) -> (u16, Value) {
    let mut req = CLIENT.request(method, format!("{base}/api{path}")).bearer_auth(KEY).header("X-Tenant-Id", "t1");
    if let Some(b) = body {
        req = req.json(&b);
    }
    let res = req.send().await.unwrap();
    let status = res.status().as_u16();
    (status, res.json().await.unwrap_or(Value::Null))
}

async fn post_json(base: &str, path: &str, body: Value) -> (u16, Value) {
    call(base, reqwest::Method::POST, path, Some(body)).await
}

async fn get_json(base: &str, path: &str) -> Value {
    call(base, reqwest::Method::GET, path, None).await.1
}

fn order_machine(url: Option<&str>) -> Value {
    let mut m = json!({
        "machine_id": "order",
        "states": ["placed", "paid", "shipped", "expired"],
        "initial_state": "placed",
        "transitions": [
            {"from": "placed", "to": "paid", "on": "pay", "guard": {"field": "method", "op": "eq", "value": "card"}},
            {"from": "paid", "to": "shipped", "on": "ship"},
            {"from": "placed", "to": "expired", "on": "$timeout", "guard": {"timeout_seconds": 1}}
        ]
    });
    if let Some(url) = url {
        m["actions"] = json!([{"on_enter": "paid", "action": {"type": "webhook", "url": url}}]);
    }
    m
}

#[tokio::test(flavor = "multi_thread")]
async fn memory_store_runs_transitions_with_guards_context_and_entries() {
    let base = store(3600).await;
    assert_eq!(post_json(&base, "/machines", order_machine(None)).await.0, 201);
    assert_eq!(post_json(&base, "/machines", order_machine(None)).await.0, 409, "create-only");
    // A guard that does not hold: no transition; evaluate created the entity in its initial state.
    let (s, r) = post_json(&base, "/machines/order/evaluate", json!({"event_type": "pay", "entity_key": "id", "params": {"id": "o1", "method": "cash"}, "timestamp": 1})).await;
    assert_eq!((s, r["current_state"].clone(), r["reason"].is_string()), (200, json!("placed"), true));
    let (_, r) = post_json(&base, "/machines/order/entities/o1/transition", json!({"event_type": "pay", "params": {"method": "card"}, "timestamp": 2})).await;
    assert_eq!((r["previous_state"].clone(), r["current_state"].clone()), (json!("placed"), json!("paid")));
    let e = get_json(&base, "/machines/order/entities/o1").await;
    assert_eq!((e["current_state"].clone(), e["context"]["method"].clone(), e["state_version"].clone()), (json!("paid"), json!("card"), json!(2)));
    assert_eq!((e["region_entries"]["_"]["state"].clone(), e["region_entries"]["_"]["seq"].clone()), (json!("paid"), json!(2)));
    post_json(&base, "/machines/order/entities/o1/transition", json!({"event_type": "ship", "timestamp": 3})).await;
    let e = get_json(&base, "/machines/order/entities/o1").await;
    assert_eq!((e["current_state"].clone(), e["region_entries"]["_"]["seq"].clone()), (json!("shipped"), json!(3)));
    let history = get_json(&base, "/machines/order/entities/o1/history").await;
    let keys: Vec<Value> = history.as_array().unwrap().iter().map(|r| r["identity_key"].clone()).collect();
    assert_eq!(keys, vec![json!("[\"evt\",\"pay\",2]"), json!("[\"evt\",\"ship\",3]")]);
    assert_eq!(get_json(&base, "/machines/order/entities?state=shipped").await.as_array().unwrap().len(), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn memory_store_deduplicates_events_by_identity() {
    let base = store(3600).await;
    post_json(&base, "/machines", order_machine(None)).await;
    post_json(&base, "/machines/order/entities", json!({"entity_id": "o1"})).await;
    let pay = json!({"event_type": "pay", "params": {"method": "card"}, "timestamp": 7});
    assert_eq!(post_json(&base, "/machines/order/entities/o1/transition", pay.clone()).await.1["current_state"], "paid");
    let (s, again) = post_json(&base, "/machines/order/entities/o1/transition", pay).await;
    assert_eq!((s, again["reason"].clone()), (200, json!("duplicate event (already processed)")));
    let (s, conflict) = post_json(&base, "/machines/order/entities/o1/transition", json!({"event_type": "pay", "params": {"method": "other"}, "timestamp": 7})).await;
    assert_eq!((s, conflict["error"]["code"].clone(), conflict["error"]["retry"].clone()), (409, json!("EVENT_IDENTITY_CONFLICT"), json!(false)));
    assert_eq!(get_json(&base, "/machines/order/entities/o1").await["state_version"], 2, "neither wrote");
}

#[tokio::test(flavor = "multi_thread")]
async fn memory_store_sends_webhooks_after_the_write() {
    let seen: Arc<Mutex<Vec<Value>>> = Arc::default();
    let sink_seen = seen.clone();
    let sink = serve(Router::new().route(
        "/hook",
        post(move |Json(v): Json<Value>| {
            let s = sink_seen.clone();
            async move {
                s.lock().unwrap().push(v);
                "ok"
            }
        }),
    ))
    .await;
    let base = store(3600).await;
    post_json(&base, "/machines", order_machine(Some(&format!("{sink}/hook")))).await;
    post_json(&base, "/machines/order/evaluate", json!({"event_type": "pay", "entity_key": "id", "params": {"id": "o1", "method": "card"}, "timestamp": 1})).await;
    for _ in 0..200 {
        if !seen.lock().unwrap().is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let got = seen.lock().unwrap().clone();
    assert_eq!(got.len(), 1);
    assert_eq!((got[0]["entity_id"].clone(), got[0]["from_state"].clone(), got[0]["to_state"].clone()), (json!("o1"), json!("placed"), json!("paid")));
}

#[tokio::test(flavor = "multi_thread")]
async fn memory_store_fires_a_timeout_once_per_entry() {
    let base = store(1).await;
    post_json(&base, "/machines", order_machine(None)).await;
    post_json(&base, "/machines/order/entities", json!({"entity_id": "late"})).await;
    post_json(&base, "/machines/order/entities", json!({"entity_id": "quick"})).await;
    post_json(&base, "/machines/order/entities/quick/transition", json!({"event_type": "pay", "params": {"method": "card"}})).await;
    tokio::time::sleep(Duration::from_millis(3200)).await;
    assert_eq!(get_json(&base, "/machines/order/entities/late").await["current_state"], "expired");
    assert_eq!(get_json(&base, "/machines/order/entities/quick").await["current_state"], "paid", "it left placed in time");
    let history = get_json(&base, "/machines/order/entities/late/history").await;
    let timeouts: Vec<&Value> = history.as_array().unwrap().iter().filter(|r| r["event_type"] == "$timeout").collect();
    assert_eq!(timeouts.len(), 1);
    assert_eq!((timeouts[0]["identity_key"].clone(), timeouts[0]["cause"]["kind"].clone()), (json!("[\"timeout\",\"_\",1]"), json!("timeout")));
}

#[tokio::test(flavor = "multi_thread")]
async fn memory_store_applies_joins_and_refuses_child_machines() {
    let base = store(3600).await;
    let parallel = json!({
        "machine_id": "fulfil",
        "regions": [
            {"id": "payment", "states": ["pending", "captured"], "initial_state": "pending"},
            {"id": "goods", "states": ["picking", "packed", "complete"], "initial_state": "picking"}
        ],
        "transitions": [
            {"from": "pending", "to": "captured", "on": "pay", "region": "payment"},
            {"from": "picking", "to": "packed", "on": "pack", "region": "goods"}
        ],
        "joins": [{"when": {"payment": "captured", "goods": "packed"}, "target_region": "goods", "target_state": "complete"}]
    });
    assert_eq!(post_json(&base, "/machines", parallel).await.0, 201);
    post_json(&base, "/machines/fulfil/evaluate", json!({"event_type": "pay", "entity_key": "id", "params": {"id": "f1"}, "timestamp": 1})).await;
    let (_, r) = post_json(&base, "/machines/fulfil/entities/f1/transition", json!({"event_type": "pack", "timestamp": 2})).await;
    assert_eq!(r["current_state"], json!({"payment": "captured", "goods": "complete"}));
    assert_eq!(r["joins_fired"][0]["target_state"], "complete");
    let with_child = json!({
        "machine_id": "parent", "states": ["a", "b"], "initial_state": "a",
        "transitions": [{"from": "a", "to": "b", "on": "go"}],
        "sub_machines": {"b": {"machine_id": "child", "on_final": {"done": "a"}}}
    });
    let (s, r) = post_json(&base, "/machines", with_child).await;
    assert_eq!((s, r["error"]["code"].clone()), (400, json!("BAD_REQUEST")));
}

#[tokio::test(flavor = "multi_thread")]
async fn memory_store_queues_concurrent_writers_instead_of_refusing_them() {
    let base = store(3600).await;
    post_json(&base, "/machines", order_machine(None)).await;
    let mut tasks = Vec::new();
    for i in 0..100 {
        let base = base.clone();
        tasks.push(tokio::spawn(async move {
            post_json(&base, "/machines/order/evaluate", json!({"event_type": "pay", "entity_key": "id", "params": {"id": format!("o{i}"), "method": "card"}, "timestamp": 1})).await
        }));
    }
    for t in tasks {
        let (s, r) = t.await.unwrap();
        assert_eq!((s, r["current_state"].clone()), (200, json!("paid")));
    }
    assert_eq!(get_json(&base, "/machines/order/entities?state=paid").await.as_array().unwrap().len(), 100);
}
