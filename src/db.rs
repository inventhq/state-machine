use libsql::{Connection, Database};
use std::time::Duration;
use tracing::{info, warn};

/// Initialize the Turso/libsql database and run migrations.
/// For remote Turso: attempts local replica first (sub-ms reads), falls back to pure remote.
pub async fn init(url: &str, token: &str) -> Result<Database, libsql::Error> {
    let db = if url.starts_with("file:") || url.starts_with("/") || url == ":memory:" {
        info!("Using local database: {}", url);
        libsql::Builder::new_local(url).build().await?
    } else {
        // Try embedded replica first for sub-ms reads
        match try_replica(url, token).await {
            Ok(db) => {
                info!("Using embedded replica (sub-ms reads, remote writes)");
                db
            }
            Err(e) => {
                warn!("Embedded replica failed ({}), falling back to pure remote", e);
                libsql::Builder::new_remote(url.to_string(), token.to_string())
                    .build()
                    .await?
            }
        }
    };

    let conn = db.connect()?;
    migrate(&conn).await?;
    info!("Database initialized and migrations applied");
    Ok(db)
}

async fn try_replica(url: &str, token: &str) -> Result<Database, libsql::Error> {
    // Ensure .data directory exists for the local replica file
    let _ = std::fs::create_dir_all(".data");
    let replica_path = ".data/replica.db";

    // Remove stale replica if it exists (clean start)
    let _ = std::fs::remove_file(replica_path);
    let _ = std::fs::remove_file(format!("{}-wal", replica_path));
    let _ = std::fs::remove_file(format!("{}-shm", replica_path));

    let db = libsql::Builder::new_remote_replica(
        replica_path,
        url.to_string(),
        token.to_string(),
    )
    .sync_interval(Duration::from_millis(200))
    .build()
    .await?;

    // Force initial sync
    db.sync().await?;

    Ok(db)
}

async fn migrate(conn: &Connection) -> Result<(), libsql::Error> {
    conn.execute_batch(
        "
        CREATE TABLE IF NOT EXISTS machines (
            machine_id TEXT NOT NULL,
            tenant_id TEXT NOT NULL,
            definition TEXT NOT NULL,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL,
            PRIMARY KEY (tenant_id, machine_id)
        );

        CREATE TABLE IF NOT EXISTS entities (
            machine_id TEXT NOT NULL,
            tenant_id TEXT NOT NULL,
            entity_id TEXT NOT NULL,
            current_state TEXT NOT NULL,
            context TEXT,
            state_version INTEGER NOT NULL DEFAULT 1,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL,
            PRIMARY KEY (tenant_id, machine_id, entity_id)
        );

        CREATE TABLE IF NOT EXISTS transitions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tenant_id TEXT NOT NULL,
            machine_id TEXT NOT NULL,
            entity_id TEXT NOT NULL,
            from_state TEXT NOT NULL,
            to_state TEXT NOT NULL,
            event_type TEXT NOT NULL,
            event_params TEXT,
            actions_dispatched TEXT,
            timestamp INTEGER NOT NULL,
            created_at INTEGER NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_transitions_entity
            ON transitions(tenant_id, machine_id, entity_id);
        CREATE INDEX IF NOT EXISTS idx_transitions_time
            ON transitions(tenant_id, timestamp);
        CREATE INDEX IF NOT EXISTS idx_transitions_dedup
            ON transitions(tenant_id, machine_id, entity_id, event_type, timestamp);
        CREATE INDEX IF NOT EXISTS idx_entities_state
            ON entities(tenant_id, machine_id, current_state);
        ",
    )
    .await?;

    // Migration: add state_version column if missing (for existing databases)
    let _ = conn
        .execute(
            "ALTER TABLE entities ADD COLUMN state_version INTEGER NOT NULL DEFAULT 1",
            (),
        )
        .await;

    // Migration: add region column to transitions (for statechart audit trail)
    let _ = conn
        .execute(
            "ALTER TABLE transitions ADD COLUMN region TEXT DEFAULT ''",
            (),
        )
        .await;

    // Migration: ingest tokens table (per-tenant tracker auth)
    conn.execute(
        "CREATE TABLE IF NOT EXISTS ingest_tokens (
            tenant_id TEXT PRIMARY KEY,
            token TEXT NOT NULL,
            created_at INTEGER NOT NULL
        )",
        (),
    )
    .await?;

    // Migration (E2): per-region entry instances. NULL for existing rows, which are read
    // through engine::effective_region_entries and persisted on their next engine write.
    let _ = conn
        .execute("ALTER TABLE entities ADD COLUMN region_entries TEXT", ())
        .await;

    // Migration (statemachine-nested.v1): managed child instance (status, path). NULL for roots,
    // legacy children and every existing row.
    let _ = conn
        .execute("ALTER TABLE entities ADD COLUMN instance TEXT", ())
        .await;

    // Migration (E1/E3): semantic identity and provenance of history rows. NULL for existing
    // rows, so the partial unique index below cannot conflict with legacy data.
    let _ = conn
        .execute("ALTER TABLE transitions ADD COLUMN identity_key TEXT", ())
        .await;
    let _ = conn
        .execute("ALTER TABLE transitions ADD COLUMN cause TEXT", ())
        .await;
    conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_transitions_identity
            ON transitions(tenant_id, machine_id, entity_id, identity_key)
            WHERE identity_key IS NOT NULL",
        (),
    )
    .await?;

    report_legacy_duplicates(conn).await?;

    Ok(())
}

/// Report (never modify) legacy history rows that share one dedup key. Dedup keeps consulting
/// every such row, comparing params, so they are evidence rather than a migration blocker.
/// Returns the number of duplicate groups.
pub(crate) async fn report_legacy_duplicates(conn: &Connection) -> Result<usize, libsql::Error> {
    let mut rows = conn
        .query(
            "SELECT tenant_id, machine_id, entity_id, event_type, timestamp, COUNT(*) FROM transitions
             WHERE identity_key IS NULL AND event_type <> '$join'
             GROUP BY tenant_id, machine_id, entity_id, event_type, timestamp HAVING COUNT(*) > 1",
            (),
        )
        .await?;
    let mut groups = 0usize;
    while let Some(row) = rows.next().await? {
        groups += 1;
        if groups <= 20 {
            warn!(
                "Legacy duplicate history key (kept, not deleted): tenant={} machine={} entity={} event_type={} timestamp={} rows={}",
                row.get::<String>(0)?,
                row.get::<String>(1)?,
                row.get::<String>(2)?,
                row.get::<String>(3)?,
                row.get::<i64>(4)?,
                row.get::<i64>(5)?
            );
        }
    }
    if groups > 0 {
        warn!("Legacy duplicate history keys: {} group(s) retained unchanged", groups);
    } else {
        info!("Legacy duplicate history keys: none");
    }
    Ok(groups)
}
