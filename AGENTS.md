# AI Agent Guide — State Machine Service

This document is for AI coding agents (Copilot, Cascade, Cursor, etc.) working on this codebase. It describes the architecture, key abstractions, data flow, and common patterns you need to understand before making changes.

## Stack

- **Language:** Rust (edition 2021, requires 1.88+)
- **Web framework:** Axum 0.8 (async, tower-based)
- **Runtime:** Tokio (full features)
- **Database:** Turso (libsql) — SQLite-compatible, with embedded replica for sub-ms reads
- **HTTP client:** reqwest 0.12 (rustls-tls)
- **Caching:** DashMap (concurrent HashMap for machine definitions)
- **Serialization:** serde + serde_json

## Codebase Map

```
src/
├── main.rs              — App entry: env loading, DB init, router, middleware, graceful shutdown
├── models.rs            — ALL data types live here. MachineDefinition, Entity, TransitionResponse,
│                          SubMachineDef, guards, actions, API request/response structs.
│                          Also contains validation logic for flat and parallel machines.
├── engine.rs            — Pure functions (no DB, no async). evaluate() finds matching transitions,
│                          check_guard() evaluates guard conditions, check_joins()/apply_joins()
│                          handle barrier-style join conditions. Unit tests live here.
├── transition_core.rs   — The heart of the system. execute_transition() is called by all routes.
│                          Handles: dedup check → evaluate → state update → optimistic lock →
│                          audit log → action dispatch. Sub-machine runtime lives here too:
│                          try_sub_machine_forward(), handle_sub_machine_event(),
│                          advance_parent_state(), create_child_entity().
├── actions.rs           — dispatch_actions() fires webhooks/events server-side with retry.
│                          build_action_list() returns structured actions without firing.
│                          collect_actions_for_state() filters actions by state + optional region.
├── scheduler.rs         — Background task: polls for $timeout transitions every N seconds.
│                          Region-aware (uses json_extract for parallel machines).
├── auth.rs              — Axum middleware: validates Authorization: Bearer <token>.
│                          Empty API_KEY = no auth (dev mode).
├── db.rs                — Database init with replica fallback. Migrations are idempotent
│                          (CREATE TABLE IF NOT EXISTS + ALTER TABLE ADD COLUMN).
├── errors.rs            — AppError enum maps to HTTP status codes (400, 401, 404, 409, 500).
└── routes/
    ├── mod.rs           — AppState struct (db, http_client, event_core_ingest_url, machine_cache).
    │                      Helper: extract_tenant_id(), now_millis().
    ├── machines.rs      — CRUD for machine definitions. load_machine() checks DashMap cache first.
    │                      Cache is invalidated on update/delete.
    ├── entities.rs      — CRUD for entities. load_entity() used by transition_core and evaluate.
    │                      list_entities supports ?state=, ?region=, ?updated_since= filters.
    ├── evaluate.rs      — POST /evaluate (single) and /evaluate/batch (up to 1000 parallel).
    │                      Auto-creates entities via load_or_create_entity().
    └── transitions.rs   — POST /transition (direct entity transition) and GET /history.
```

## Data Flow

### Evaluate Request (most common path)

```
HTTP POST /api/machines/{id}/evaluate
  → auth middleware (auth.rs)
  → evaluate handler (routes/evaluate.rs)
    → load_machine() from DashMap cache or DB
    → load_or_create_entity() — auto-creates in initial state if missing
    → transition_core::execute_transition()
      → dedup check (SELECT from transitions by event+timestamp)
      → engine::evaluate() — pure CPU, finds matching transition + checks guards
      → IF match found:
        → update state_map, apply joins, encode new state
        → collect + dispatch actions
        → optimistic lock UPDATE (state_version check)
        → record transition history
      → IF no match:
        → try_sub_machine_forward() — check if current state is a compound state
          → load child machine, load/create child entity
          → recursively call execute_transition on child
          → if child completed (final state) → advance_parent_state()
        → if still no match → return "no transition" response
    ← TransitionResponse JSON
```

## Key Abstractions

### State Map

Internally, every entity's state is a `HashMap<String, String>` (region → state):

- **Flat FSM:** `{"_": "active"}` — single region named `"_"` (`DEFAULT_REGION`)
- **Parallel:** `{"payment": "captured", "fulfillment": "picking"}` — one entry per region

The `Entity.current_state` column stores either a plain string (flat) or a JSON object (parallel). `Entity.state_map()` parses it. `Entity.encode_state()` serializes it back.

### Transitions Are Region-Scoped

Every `TransitionDef` operates within a region. For flat machines, the region is implicitly `"_"`. For parallel machines, `transition.region` must be specified. `engine::evaluate()` checks the entity's state in the transition's region.

### Optimistic Locking

All state updates use `state_version`:

```sql
UPDATE entities SET ... WHERE state_version = ?
```

If `affected == 0`, another transition won the race → return `409 Conflict`.

### Sub-Machine Convention

- Child entity ID: `{parent_entity_id}::sub::{compound_state_name}`
- Child entities live in the child machine's namespace (separate `machine_id`)
- Parent auto-advance is recorded as `event_type = "$sub_complete"` in audit log
- Escape transitions (parent-level) always take priority over sub-machine forwarding
- Sub-machine forwarding uses `Box::pin()` for async recursion

### Machine Definition Caching

`load_machine()` in `routes/machines.rs` uses a `DashMap<(tenant_id, machine_id), MachineDefinition>`. Cache is populated on first read, invalidated on update/delete. This means machine lookups on the hot path are ~0ns after the first call.

## How to Add a New Feature

### Adding a new field to MachineDefinition

1. Add the field to `MachineDefinition` in `models.rs` (with `#[serde(default)]` for backward compat)
2. Add it to `CreateMachineRequest` and `UpdateMachineRequest` in `models.rs`
3. Set it in `create_machine()` and `update_machine()` in `routes/machines.rs`
4. Add validation in `validate_flat()` and/or `validate_parallel()` in `models.rs`
5. Update test fixtures in `engine.rs` (add the field to `test_machine()` and `test_parallel_machine()`)

### Adding a new API endpoint

1. Add the handler function in the appropriate `routes/*.rs` file
2. Register the route in `main.rs` under `api_routes`
3. Define request/response types in `models.rs`

### Adding a new transition feature

1. If it's a pure evaluation concern → modify `engine.rs` (`evaluate()` or `check_guard()`)
2. If it involves DB/actions/side effects → modify `transition_core.rs` (`execute_transition()`)
3. If it's a background process → add to `scheduler.rs`

### Modifying the database schema

1. Add migration in `db.rs` `migrate()` function using idempotent statements:
   ```rust
   let _ = conn.execute("ALTER TABLE x ADD COLUMN y TEXT DEFAULT ''", ()).await;
   ```
2. The `let _ =` pattern ignores "duplicate column" errors on re-runs

## Testing

### Unit Tests

All unit tests are in `src/engine.rs` under `#[cfg(test)] mod tests`. They test the pure evaluation engine (no DB required).

```bash
cargo test
```

Current tests (9):
- `test_simple_transition` — basic flat FSM transition
- `test_no_valid_transition` — event with no matching transition
- `test_guard_passes` / `test_guard_fails` — guard condition evaluation
- `test_parallel_region_transition` — parallel machine region update
- `test_parallel_wrong_region_no_match` — event in wrong region
- `test_join_fires_when_all_satisfied` — join barrier fires
- `test_join_not_satisfied` — join doesn't fire when conditions not met
- `test_join_does_not_refire` — join idempotency

### E2E Testing

Start the server locally and use curl:

```bash
# Set up
export API_KEY=dev_secret
cargo run &

# Create machine
curl -X POST http://localhost:3051/api/machines \
  -H "Authorization: Bearer dev_secret" \
  -H "X-Tenant-Id: test" \
  -H "Content-Type: application/json" \
  -d '{"machine_id":"demo","states":["a","b"],"initial_state":"a","transitions":[{"from":"a","to":"b","on":"go"}]}'

# Evaluate (auto-creates entity)
curl -X POST http://localhost:3051/api/machines/demo/evaluate \
  -H "Authorization: Bearer dev_secret" \
  -H "X-Tenant-Id: test" \
  -H "Content-Type: application/json" \
  -d '{"event_type":"go","entity_key":"id","params":{"id":"e1"}}'

# Check entity state
curl http://localhost:3051/api/machines/demo/entities/e1 \
  -H "Authorization: Bearer dev_secret" \
  -H "X-Tenant-Id: test"
```

## Common Pitfalls

### 1. Forgetting `sub_machines: HashMap::new()` in test fixtures

`MachineDefinition` has a `sub_machines` field. All test machine constructors in `engine.rs` must include it.

### 2. Async recursion requires boxing

Sub-machine forwarding creates recursive async calls (`execute_transition` → `try_sub_machine_forward` → `handle_sub_machine_event` → `execute_transition`). Rust async fns can't be recursive without `Box::pin()` at the recursive call site.

### 3. Flat vs parallel state encoding

`current_state` is a plain string for flat machines but a JSON object for parallel machines. Always use `entity.state_map()` to read and `Entity::encode_state()` to write. Never compare `current_state` directly.

### 4. TransitionResponse needs all fields

When constructing a `TransitionResponse`, all fields must be present including `sub_machine: None` for non-sub-machine transitions. Missing this causes compile errors.

### 5. Machine cache invalidation

If you change how machine definitions are stored or loaded, remember that `routes/machines.rs` uses a `DashMap` cache. Updates and deletes invalidate the cache entry. New fields in `MachineDefinition` are automatically cached since the whole struct is cached.

### 6. Migrations must be idempotent

`db.rs` migrations run on every startup. Use `CREATE TABLE IF NOT EXISTS` and wrap `ALTER TABLE` in `let _ =` to ignore "column already exists" errors.

### 7. `DEFAULT_REGION` is `"_"`

Flat machines use region `"_"` internally. This constant is defined in `models.rs`. When checking regions, use `DEFAULT_REGION` rather than hardcoding `"_"`.

## Environment Setup

```bash
# Install Rust 1.88+
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
rustup update

# Build
cargo build

# Run tests
cargo test

# Run with hot reload (optional, install cargo-watch)
cargo install cargo-watch
cargo watch -x run
```

## Deployment

- **Docker:** Multi-stage build using `debian:bookworm-slim` + rustup 1.88. See `Dockerfile`.
- **K8s:** Manifests in `k8s/`. Secrets managed via `kubectl create secret`, not checked into YAML.
- **CI/CD:** `.github/workflows/deploy.yml` — push to `main` triggers build → push to GHCR → deploy to K3s.
- **Branching:** Feature branches → PR → merge to `main` → auto-deploy.
