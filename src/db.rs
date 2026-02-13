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

    Ok(())
}
