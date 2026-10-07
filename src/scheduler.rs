use std::sync::Arc;
use std::time::Duration;

use libsql::{Database, TransactionBehavior};
use reqwest::Client;
use tracing::{error, info, warn};

use crate::actions;
use crate::engine;
use crate::errors::AppError;
use crate::models::{ActionConfig, MachineDefinition, TransitionDef, DEFAULT_REGION};
use crate::routes::entities::{load_entity_on, region_state_predicate};
use crate::routes::now_millis;
use crate::transition_core::{write_entity, Ctx, Defs, PendingDispatch, WriteSpec};

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
            if let Err(e) = tick(&db, &http_client, &event_core_ingest_url, now_millis()).await {
                error!("Timeout scheduler error: {}", e);
            }
        }
    });
    info!(
        "Timeout scheduler started (interval: {}s)",
        interval_secs
    );
}

/// Outcome counts of one tick.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct TickReport {
    pub fired: usize,
    pub errors: usize,
}

/// One scheduler pass at `now`.
///
/// Eligibility (E2): a region's `$timeout` from `from` is due when that region is in `from` and
/// its own entry instance satisfies `entered_at + timeout_ms <= now`; other regions' writes do
/// not move it. Each candidate fires in its own IMMEDIATE transaction (E1) that re-reads and
/// re-checks the entity and writes state, region entries, the `$timeout` row (identity
/// `["timeout", region, seq]`, E3) and any `$join` rows. A failing candidate is logged and
/// skipped. Actions are dispatched only after commit.
pub(crate) async fn tick(
    db: &Database,
    http_client: &Client,
    event_core_ingest_url: &str,
    now: i64,
) -> Result<TickReport, Box<dyn std::error::Error + Send + Sync>> {
    let machines = load_machines(db).await?;
    let mut report = TickReport::default();

    for machine in &machines {
        // Find transitions with $timeout trigger + timeout_seconds guard
        let timeout_transitions: Vec<&TransitionDef> = machine
            .transitions
            .iter()
            .filter(|t| t.on == "$timeout")
            .collect();

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
            let region = tt.region.as_deref().unwrap_or(DEFAULT_REGION);

            let candidates =
                match timeout_candidates(db, machine, region, &tt.from, now - timeout_ms).await {
                    Ok(c) => c,
                    Err(e) => {
                        error!(
                            "Timeout scheduler: candidate query failed for {}/{} region '{}': {}",
                            machine.tenant_id, machine.machine_id, region, e
                        );
                        report.errors += 1;
                        continue;
                    }
                };

            for entity_id in candidates {
                match fire_timeout(db, machine, tt, region, &entity_id, timeout_ms, now).await {
                    Ok(Some(fired)) => {
                        report.fired += 1;
                        // Network only after the commit above.
                        let ingest_token = lookup_ingest_token(db, &machine.tenant_id).await;
                        for p in fired.pending {
                            actions::dispatch_actions(
                                http_client,
                                event_core_ingest_url,
                                p.actions,
                                &machine.tenant_id,
                                &p.machine_id,
                                &p.entity_id,
                                &p.from_state,
                                &p.to_state,
                                ingest_token.as_deref(),
                            );
                        }
                        info!(
                            "Timeout transition: {}/{} {}:{} → {} (after {}s)",
                            machine.machine_id,
                            entity_id,
                            region,
                            fired.from_state,
                            tt.to,
                            timeout_ms / 1000
                        );
                    }
                    Ok(None) => {}
                    Err(e) => {
                        error!(
                            "Timeout scheduler: {}/{} region '{}' not fired: {}",
                            machine.machine_id, entity_id, region, e
                        );
                        report.errors += 1;
                    }
                }
            }
        }
    }

    Ok(report)
}

async fn load_machines(
    db: &Database,
) -> Result<Vec<MachineDefinition>, Box<dyn std::error::Error + Send + Sync>> {
    let conn = db.connect()?;
    let mut rows = conn.query("SELECT definition FROM machines", ()).await?;
    let mut machines = Vec::new();
    while let Some(row) = rows.next().await? {
        let def_str: String = row.get(0)?;
        if let Ok(m) = serde_json::from_str::<MachineDefinition>(&def_str) {
            machines.push(m);
        }
    }
    Ok(machines)
}

/// Prefilter (re-checked in `fire_timeout`): entities whose `region` is in `from` and whose
/// entry for it is at or before `cutoff`. Region and state are bound parameters; no SQL text
/// comes from the definition. Entries are read from `region_entries` when it belongs to the
/// row's `state_version`, else from `updated_at` (exact for flat rows, an upper bound for
/// parallel rows), exactly as `engine::effective_region_entries` does.
async fn timeout_candidates(
    db: &Database,
    machine: &MachineDefinition,
    region: &str,
    from: &str,
    cutoff: i64,
) -> Result<Vec<String>, libsql::Error> {
    let state_predicate = if machine.is_parallel() {
        region_state_predicate("entities.current_state", 3, 4)
    } else {
        "current_state = ?4".to_string()
    };
    let sql = format!(
        "SELECT entity_id FROM entities WHERE tenant_id = ?1 AND machine_id = ?2 AND {} \
            AND (CASE WHEN json_valid(entities.instance) THEN json_extract(entities.instance, '$.status') = 'active' ELSE 1 END) \
            AND COALESCE(\
            CASE WHEN json_valid(entities.region_entries) THEN \
              CASE WHEN json_extract(entities.region_entries, '$.version') = entities.state_version \
              THEN (SELECT json_extract(re.value, '$.entered_at') FROM json_each(entities.region_entries, '$.regions') AS re \
                    WHERE re.key = ?3 AND json_extract(re.value, '$.state') = ?4) END \
            END, \
            updated_at) <= ?5",
        state_predicate
    );

    let conn = db.connect()?;
    let mut rows = conn
        .query(
            &sql,
            libsql::params![
                machine.tenant_id.clone(),
                machine.machine_id.clone(),
                region.to_string(),
                from.to_string(),
                cutoff
            ],
        )
        .await?;
    // Drain before any write transaction starts.
    let mut ids = Vec::new();
    while let Some(row) = rows.next().await? {
        ids.push(row.get::<String>(0)?);
    }
    Ok(ids)
}

struct FiredTimeout {
    from_state: String,
    /// Every committed write of the timeout's transaction (the timeout, then any cascade).
    pending: Vec<PendingDispatch>,
}

/// Fire one `$timeout` atomically, or return `None` when it is no longer due (state changed,
/// instance not yet due, or another writer won).
async fn fire_timeout(
    db: &Database,
    machine: &MachineDefinition,
    tt: &TransitionDef,
    region: &str,
    entity_id: &str,
    timeout_ms: i64,
    now: i64,
) -> Result<Option<FiredTimeout>, AppError> {
    let conn = db.connect()?;
    let tx = conn
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .await?;

    let entity = match load_entity_on(&tx, &machine.tenant_id, &machine.machine_id, entity_id).await {
        Ok(e) => e,
        Err(AppError::NotFound(_)) => return Ok(None),
        Err(e) => return Err(e),
    };
    // An ended managed instance (completed or cancelled) never fires.
    if entity.instance.as_ref().is_some_and(|i| !i.is_active()) {
        return Ok(None);
    }

    let mut state_map = entity.state_map();
    let from_state = match state_map.get(region) {
        Some(s) if *s == tt.from => s.clone(),
        _ => return Ok(None),
    };
    let entries = engine::effective_region_entries(&entity);
    let entry = match entries.get(region) {
        Some(e) => e.clone(),
        None => return Ok(None),
    };
    if !engine::timeout_due(&entry, timeout_ms, now) {
        return Ok(None);
    }
    let key = engine::timeout_identity(region, entry.seq);

    let is_parallel = machine.is_parallel();
    state_map.insert(region.to_string(), tt.to.clone());
    // Check joins after timeout transition
    let joins_fired = if is_parallel {
        engine::apply_joins(machine, &mut state_map, 10)
    } else {
        Vec::new()
    };

    // Collect actions (fired by the caller after commit; join actions stay undispatched on this path)
    let region_opt = if is_parallel { Some(region) } else { None };
    let action_configs: Vec<(String, ActionConfig, Option<String>)> =
        actions::collect_actions_for_state(&machine.actions, &tt.to, region_opt);

    let cause = serde_json::json!({
        "kind": "timeout",
        "region": region,
        "from": from_state,
        "to": tt.to,
        "entry": {"seq": entry.seq, "entered_at": entry.entered_at, "basis": entry.basis},
        "timeout_ms": timeout_ms,
        "due_at": entry.entered_at.saturating_add(timeout_ms),
    });

    // One write (history, joins, state, region entries) plus, for a managed child, its
    // completion cascade to the parents, all in this transaction.
    let mut ctx = Ctx::new(&tx, Defs::Tx, &machine.tenant_id, "$timeout", now);
    let written = write_entity(
        &mut ctx,
        machine,
        &entity,
        WriteSpec {
            event_type: "$timeout",
            from_state: &from_state,
            to_state: &tt.to,
            region,
            row_region: if is_parallel { region } else { "" },
            // event_params keeps the legacy context snapshot
            event_params: serde_json::to_string(&entity.context)?,
            timestamp: now,
            identity_key: key,
            cause: Some(cause),
            new_state_map: &state_map,
            joins_fired: &joins_fired,
            context_json: None,
            actions: action_configs,
        },
    )
    .await;
    match written {
        Ok(_) => {}
        // Another tick, process or transition won this instance.
        Err(AppError::Conflict(_)) => return Ok(None),
        Err(e) => return Err(e),
    }
    let pending = std::mem::take(&mut ctx.pending);
    drop(ctx);

    tx.commit().await?;
    Ok(Some(FiredTimeout { from_state, pending }))
}

/// Look up an ingest token for a tenant from the DB.
async fn lookup_ingest_token(db: &Database, tenant_id: &str) -> Option<String> {
    let conn = db.connect().ok()?;
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
