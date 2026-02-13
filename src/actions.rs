use std::time::Duration;

use reqwest::Client;
use serde_json::json;
use tracing::{error, info, warn};

use crate::models::{ActionConfig, DispatchedAction};

/// Dispatch actions triggered by entering a state.
/// Fire-and-forget: spawns async tasks with retry. Does NOT block the caller.
/// Returns structured `DispatchedAction` objects for the API response.
pub fn dispatch_actions(
    client: &Client,
    event_core_url: &str,
    actions: Vec<(String, ActionConfig, Option<String>)>,
    tenant_id: &str,
    machine_id: &str,
    entity_id: &str,
    from_state: &str,
    to_state: &str,
) -> Vec<DispatchedAction> {
    let mut dispatched = Vec::new();

    for (state_label, action, region) in actions {
        match &action {
            ActionConfig::Webhook { url } => {
                dispatched.push(DispatchedAction::Webhook {
                    url: url.clone(),
                    state: state_label,
                    region: region.clone(),
                });
                let client = client.clone();
                let url = url.clone();
                let payload = json!({
                    "tenant_id": tenant_id,
                    "machine_id": machine_id,
                    "entity_id": entity_id,
                    "from_state": from_state,
                    "to_state": to_state,
                });
                tokio::spawn(async move {
                    dispatch_webhook_with_retry(&client, &url, &payload, 3).await;
                });
            }
            ActionConfig::Event { event_type } => {
                dispatched.push(DispatchedAction::Event {
                    event_type: event_type.clone(),
                    state: state_label,
                    region: region.clone(),
                });
                let client = client.clone();
                let ingest_url = event_core_url.to_string();
                let payload = json!({
                    "event_type": event_type,
                    "params": {
                        "key_prefix": tenant_id,
                        "machine_id": machine_id,
                        "entity_id": entity_id,
                        "from_state": from_state,
                        "to_state": to_state,
                    }
                });
                tokio::spawn(async move {
                    dispatch_webhook_with_retry(&client, &ingest_url, &payload, 3).await;
                });
            }
        }
    }

    dispatched
}

/// Build structured action list WITHOUT dispatching server-side.
/// Used when plugin-runtime handles execution (dispatch=false).
pub fn build_action_list(
    actions: Vec<(String, ActionConfig, Option<String>)>,
) -> Vec<DispatchedAction> {
    actions
        .into_iter()
        .map(|(state_label, action, region)| match &action {
            ActionConfig::Webhook { url } => DispatchedAction::Webhook {
                url: url.clone(),
                state: state_label,
                region,
            },
            ActionConfig::Event { event_type } => DispatchedAction::Event {
                event_type: event_type.clone(),
                state: state_label,
                region,
            },
        })
        .collect()
}

/// Collect actions that should fire for entering `to_state` in an optional region.
/// For flat machines, pass region=None. For parallel, pass the region name.
pub fn collect_actions_for_state(
    actions: &[crate::models::ActionDef],
    to_state: &str,
    region: Option<&str>,
) -> Vec<(String, ActionConfig, Option<String>)> {
    actions
        .iter()
        .filter(|a| {
            a.on_enter == to_state
                && match (&a.region, region) {
                    (None, _) => true,            // action has no region scope — always matches
                    (Some(ar), Some(r)) => ar == r, // both specified — must match
                    (Some(_), None) => false,       // action scoped but flat machine
                }
        })
        .map(|a| (a.on_enter.clone(), a.action.clone(), a.region.clone()))
        .collect()
}

async fn dispatch_webhook_with_retry(
    client: &Client,
    url: &str,
    payload: &serde_json::Value,
    max_retries: u32,
) {
    for attempt in 0..=max_retries {
        match client.post(url).json(payload).send().await {
            Ok(resp) => {
                if resp.status().is_success() {
                    info!("Action dispatched to {} (attempt {})", url, attempt + 1);
                    return;
                }
                warn!(
                    "Action dispatch to {} returned {}, attempt {}/{}",
                    url,
                    resp.status(),
                    attempt + 1,
                    max_retries + 1
                );
            }
            Err(e) => {
                warn!(
                    "Action dispatch to {} failed: {}, attempt {}/{}",
                    url,
                    e,
                    attempt + 1,
                    max_retries + 1
                );
            }
        }
        if attempt < max_retries {
            tokio::time::sleep(Duration::from_millis(500 * 2u64.pow(attempt))).await;
        }
    }
    error!("Action dispatch to {} exhausted all retries", url);
}
