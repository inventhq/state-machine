use std::sync::Arc;
use std::time::Duration;

use libsql::Database;
use reqwest::Client;
use tracing::{error, info, warn};

use crate::actions;
use crate::models::{ActionConfig, MachineDefinition, TransitionDef, DEFAULT_REGION};
use crate::routes::now_millis;

/// Start the background timeout scheduler.
/// Polls every `interval` seconds for entities that have exceeded their timeout guard
/// and auto-fires `$timeout` transitions.
pub fn start(
    db: Arc<Database>,
    http_client: Client,
    event_core_ingest_url: String,
    interval_secs: u64,
) {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(Duration::from_secs(interval_secs));
        loop {
            ticker.tick().await;
            if let Err(e) = tick(&db, &http_client, &event_core_ingest_url).await {
                error!("Timeout scheduler error: {}", e);
            }
        }
    });
    info!(
        "Timeout scheduler started (interval: {}s)",
        interval_secs
    );
}

async fn tick(
    db: &Database,
    http_client: &Client,
    event_core_ingest_url: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let conn = db.connect()?;

    // Load all machine definitions
    let mut rows = conn
        .query("SELECT definition FROM machines", ())
        .await?;

    let mut machines: Vec<MachineDefinition> = Vec::new();
    while let Some(row) = rows.next().await? {
        let def_str: String = row.get(0)?;
        if let Ok(m) = serde_json::from_str::<MachineDefinition>(&def_str) {
            machines.push(m);
        }
    }

    let now = now_millis();

    for machine in &machines {
        // Find transitions with $timeout trigger + timeout_seconds guard
        let timeout_transitions: Vec<&TransitionDef> = machine
            .transitions
            .iter()
            .filter(|t| t.on == "$timeout")
            .collect();

        if timeout_transitions.is_empty() {
            continue;
        }

        for tt in &timeout_transitions {
            let timeout_ms = match extract_timeout_ms(&tt.guard) {
                Some(ms) => ms,
                None => {
                    warn!(
                        "Machine '{}': $timeout transition from '{}' missing timeout_seconds guard",
                        machine.machine_id, tt.from
                    );
                    continue;
                }
            };

            let cutoff = now - timeout_ms;
            let region = tt.region.as_deref().unwrap_or(DEFAULT_REGION);
            let is_parallel = machine.is_parallel();

            // Find entities in the `from` state whose updated_at is older than cutoff
            // For flat: current_state = from. For parallel: json_extract(current_state, '$.region') = from
            let mut entity_rows = if is_parallel {
                conn.query(
                    &format!(
                        "SELECT entity_id, current_state, context, state_version, updated_at FROM entities WHERE tenant_id = ?1 AND machine_id = ?2 AND json_extract(current_state, '$.{}') = ?3 AND updated_at <= ?4",
                        region
                    ),
                    libsql::params![
                        machine.tenant_id.clone(),
                        machine.machine_id.clone(),
                        tt.from.clone(),
                        cutoff
                    ],
                )
                .await?
            } else {
                conn.query(
                    "SELECT entity_id, current_state, context, state_version, updated_at FROM entities WHERE tenant_id = ?1 AND machine_id = ?2 AND current_state = ?3 AND updated_at <= ?4",
                    libsql::params![
                        machine.tenant_id.clone(),
                        machine.machine_id.clone(),
                        tt.from.clone(),
                        cutoff
                    ],
                )
                .await?
            };

            while let Some(row) = entity_rows.next().await? {
                let entity_id: String = row.get(0)?;
                let current_state_raw: String = row.get(1)?;
                let context_str: String = row.get::<String>(2).unwrap_or_else(|_| "{}".to_string());
                let state_version: i64 = row.get(3)?;

                let to_state = &tt.to;

                // Build new state
                let new_state = if is_parallel {
                    let mut state_map: std::collections::HashMap<String, String> =
                        serde_json::from_str(&current_state_raw).unwrap_or_default();
                    let from_state = state_map.get(region).cloned().unwrap_or_default();
                    state_map.insert(region.to_string(), to_state.clone());

                    // Check joins after timeout transition
                    let _joins = crate::engine::apply_joins(&machine, &mut state_map, 10);

                    let new_encoded = crate::models::Entity::encode_state(&state_map);
                    (new_encoded, from_state)
                } else {
                    (to_state.clone(), current_state_raw.clone())
                };

                let (new_state_encoded, from_state) = new_state;

                // Optimistic locking: only transition if version matches
                let affected = conn
                    .execute(
                        "UPDATE entities SET current_state = ?1, state_version = state_version + 1, updated_at = ?2 WHERE tenant_id = ?3 AND machine_id = ?4 AND entity_id = ?5 AND state_version = ?6",
                        libsql::params![
                            new_state_encoded,
                            now,
                            machine.tenant_id.clone(),
                            machine.machine_id.clone(),
                            entity_id.clone(),
                            state_version
                        ],
                    )
                    .await?;

                if affected == 0 {
                    continue; // Another transition won the race
                }

                // Collect and dispatch actions
                let region_opt = if is_parallel { Some(region) } else { None };
                let action_configs: Vec<(String, ActionConfig, Option<String>)> =
                    actions::collect_actions_for_state(&machine.actions, to_state, region_opt);

                // Look up ingest token for this tenant
                let ingest_token = lookup_ingest_token(&conn, &machine.tenant_id).await;

                let dispatched = actions::dispatch_actions(
                    http_client,
                    event_core_ingest_url,
                    action_configs,
                    &machine.tenant_id,
                    &machine.machine_id,
                    &entity_id,
                    &from_state,
                    to_state,
                    ingest_token.as_deref(),
                );

                let dispatched_json = serde_json::to_string(&dispatched).unwrap_or_default();
                let region_str = if is_parallel { region } else { "" };

                // Record transition history
                conn.execute(
                    "INSERT INTO transitions (tenant_id, machine_id, entity_id, from_state, to_state, event_type, event_params, actions_dispatched, region, timestamp, created_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
                    libsql::params![
                        machine.tenant_id.clone(),
                        machine.machine_id.clone(),
                        entity_id.clone(),
                        from_state.clone(),
                        to_state.clone(),
                        "$timeout".to_string(),
                        context_str,
                        dispatched_json,
                        region_str.to_string(),
                        now,
                        now
                    ],
                )
                .await?;

                info!(
                    "Timeout transition: {}/{} {}:{} → {} (after {}s)",
                    machine.machine_id,
                    entity_id,
                    region,
                    from_state,
                    to_state,
                    timeout_ms / 1000
                );
            }
        }
    }

    Ok(())
}

/// Look up an ingest token for a tenant from the DB.
async fn lookup_ingest_token(conn: &libsql::Connection, tenant_id: &str) -> Option<String> {
    let mut rows = conn
        .query(
            "SELECT token FROM ingest_tokens WHERE tenant_id = ?1",
            libsql::params![tenant_id.to_string()],
        )
        .await
        .ok()?;
    let row = rows.next().await.ok()??;
    row.get::<String>(0).ok()
}

fn extract_timeout_ms(guard: &Option<serde_json::Value>) -> Option<i64> {
    guard
        .as_ref()?
        .as_object()?
        .get("timeout_seconds")?
        .as_i64()
        .map(|s| s * 1000)
}
