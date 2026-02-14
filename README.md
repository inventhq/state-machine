# State Machine Service

A multi-tenant, event-driven state machine engine built in Rust. Supports flat FSMs, parallel statecharts (Harel-style), sub-machines (hierarchical nesting), guard conditions, timeout transitions, join barriers, and webhook/event actions.

## Quick Start

### Prerequisites

- Rust 1.88+
- (Optional) [Turso](https://turso.tech) account for remote database

### Local Development

```bash
# Clone
git clone https://github.com/inventhq/state-machine.git
cd state-machine

# Create .env
cat > .env <<EOF
TURSO_DATABASE_URL=file:statemachine.db
TURSO_AUTH_TOKEN=
API_KEY=dev_secret
LISTEN_ADDR=0.0.0.0:3051
TIMEOUT_INTERVAL_SECS=10
EVENT_CORE_INGEST_URL=http://localhost:3030/ingest
EOF

# Run
cargo run

# Health check
curl http://localhost:3051/health
```

### Environment Variables

| Variable | Description | Default |
|---|---|---|
| `TURSO_DATABASE_URL` | Turso DB URL or `file:local.db` for local SQLite | `file:statemachine.db` |
| `TURSO_AUTH_TOKEN` | Turso auth token (empty for local) | — |
| `API_KEY` | Bearer token for API auth (empty = no auth) | — |
| `LISTEN_ADDR` | Bind address | `0.0.0.0:3050` |
| `TIMEOUT_INTERVAL_SECS` | Timeout scheduler poll interval | `10` |
| `EVENT_CORE_INGEST_URL` | URL to forward event actions to | `http://localhost:3030/ingest` |
| `RUST_LOG` | Log level filter | `info` |

---

## Authentication

All `/api/*` endpoints require a Bearer token:

```
Authorization: Bearer <API_KEY>
```

The `/health` endpoint is unauthenticated.

Multi-tenancy is header-based:

```
X-Tenant-Id: my_tenant
```

---

## Machine Types

### 1. Flat FSM

A simple finite state machine with a list of states and transitions between them.

```json
{
  "machine_id": "order",
  "states": ["placed", "processing", "shipped", "delivered"],
  "initial_state": "placed",
  "transitions": [
    { "from": "placed", "to": "processing", "on": "start" },
    { "from": "processing", "to": "shipped", "on": "ship" },
    { "from": "shipped", "to": "delivered", "on": "deliver" }
  ]
}
```

Entity `current_state` is a plain string: `"processing"`.

### 2. Parallel / Statechart Machine

Multiple independent regions that advance in parallel. Uses Harel statechart semantics.

```json
{
  "machine_id": "order",
  "regions": [
    {
      "id": "payment",
      "states": ["pending", "captured", "refunded"],
      "initial_state": "pending"
    },
    {
      "id": "fulfillment",
      "states": ["picking", "packed", "shipped"],
      "initial_state": "picking"
    }
  ],
  "transitions": [
    { "from": "pending", "to": "captured", "on": "pay", "region": "payment" },
    { "from": "picking", "to": "packed", "on": "pack", "region": "fulfillment" },
    { "from": "packed", "to": "shipped", "on": "ship", "region": "fulfillment" }
  ],
  "joins": [
    {
      "when": { "payment": "captured", "fulfillment": "shipped" },
      "target_region": "fulfillment",
      "target_state": "complete"
    }
  ]
}
```

Entity `current_state` is a JSON object: `{"payment": "captured", "fulfillment": "picking"}`.

**Joins** are barrier-style conditions: when all `when` conditions are simultaneously met, the `target_region` is set to `target_state`. Joins cascade (up to depth 10).

### 3. Sub-Machine (Hierarchical / Nested)

A state in the parent machine can contain its own child state machine. Events are forwarded to the child; when the child reaches a final state, the parent auto-advances.

**Step 1: Define the child machine**

```json
{
  "machine_id": "processing_flow",
  "states": ["picking", "packing", "labeling"],
  "initial_state": "picking",
  "transitions": [
    { "from": "picking", "to": "packing", "on": "picked" },
    { "from": "packing", "to": "labeling", "on": "packed" }
  ]
}
```

**Step 2: Define the parent machine with `sub_machines`**

```json
{
  "machine_id": "order",
  "states": ["placed", "processing", "shipped", "delivered", "cancelled"],
  "initial_state": "placed",
  "transitions": [
    { "from": "placed", "to": "processing", "on": "start_processing" },
    { "from": "processing", "to": "cancelled", "on": "cancel" },
    { "from": "shipped", "to": "delivered", "on": "deliver" }
  ],
  "sub_machines": {
    "processing": {
      "machine_id": "processing_flow",
      "on_final": {
        "labeling": "shipped"
      }
    }
  }
}
```

**How it works:**

1. When the parent enters `"processing"`, a child entity is auto-created in `processing_flow`
2. Events sent to the parent that don't match a parent transition are forwarded to the active child
3. When the child reaches `"labeling"` (a key in `on_final`), the parent auto-advances to `"shipped"`
4. **Escape transitions** (e.g. `cancel`) are evaluated at the parent level first and always take priority

**Response when child handles an event:**

```json
{
  "entity_id": "order_1",
  "previous_state": "processing",
  "current_state": "processing",
  "transition": null,
  "triggered_by": "picked",
  "sub_machine": {
    "machine_id": "processing_flow",
    "entity_id": "order_1::sub::processing",
    "previous_state": "picking",
    "current_state": "packing",
    "transition": "picking → packing",
    "auto_completed": false
  }
}
```

**Response when child completes and parent auto-advances:**

```json
{
  "entity_id": "order_1",
  "previous_state": "processing",
  "current_state": "shipped",
  "transition": "processing → shipped",
  "triggered_by": "packed",
  "sub_machine": {
    "machine_id": "processing_flow",
    "entity_id": "order_1::sub::processing",
    "previous_state": "packing",
    "current_state": "labeling",
    "transition": "packing → labeling",
    "auto_completed": true
  }
}
```

Child entity IDs follow the pattern `{parent_entity_id}::sub::{compound_state}`.

---

## Features

### Guard Conditions

Transitions can have guard conditions that must be satisfied for the transition to fire.

```json
{
  "from": "pending",
  "to": "confirmed",
  "on": "pay",
  "guard": {
    "field": "amount_cents",
    "op": "gt",
    "value": 0
  }
}
```

**Supported operators:** `eq`, `neq`, `gt`, `gte`, `lt`, `lte`, `in`, `not_in`, `exists`, `not_exists`

**Compound guards:**

```json
{
  "guard": {
    "all": [
      { "field": "amount", "op": "gt", "value": 0 },
      { "field": "currency", "op": "eq", "value": "USD" }
    ]
  }
}
```

Also supports `"any"` for OR logic.

Guard field values are resolved from event `params` first, then entity `context`.

### Timeout Transitions

Automatic transitions after a duration. The scheduler polls every `TIMEOUT_INTERVAL_SECS`.

```json
{
  "from": "pending",
  "to": "expired",
  "on": "$timeout",
  "guard": { "timeout_seconds": 3600 }
}
```

### Actions

Trigger webhooks or emit events when entering a state.

```json
{
  "actions": [
    {
      "on_enter": "shipped",
      "action": { "type": "webhook", "url": "https://example.com/notify" }
    },
    {
      "on_enter": "shipped",
      "action": { "type": "event", "event_type": "order.shipped" }
    }
  ]
}
```

Actions can be scoped to a specific region for parallel machines:

```json
{ "on_enter": "captured", "region": "payment", "action": { "type": "webhook", "url": "..." } }
```

When `dispatch: true` (default), actions fire server-side with retries. When `dispatch: false`, actions are returned in the response for client-side execution.

### Batch Evaluate

Process up to 1000 events in parallel:

```bash
POST /api/machines/{machine_id}/evaluate/batch
```

```json
{
  "events": [
    { "event_type": "pay", "entity_key": "id", "params": { "id": "e1" } },
    { "event_type": "pay", "entity_key": "id", "params": { "id": "e2" } }
  ]
}
```

### Optimistic Locking

All state updates use `state_version` for optimistic concurrency control. Concurrent modifications return `409 Conflict`.

### Entity Auto-Creation

The `/evaluate` endpoint auto-creates entities in the machine's initial state if they don't exist. No need for a separate create call.

---

## API Reference

All endpoints are prefixed with `/api` and require `Authorization: Bearer <token>` and `X-Tenant-Id` headers.

### Machines

| Method | Path | Description |
|---|---|---|
| `POST` | `/api/machines` | Create a machine |
| `GET` | `/api/machines` | List all machines |
| `GET` | `/api/machines/{machine_id}` | Get a machine |
| `PUT` | `/api/machines/{machine_id}` | Update a machine |
| `DELETE` | `/api/machines/{machine_id}` | Delete a machine (fails if entities exist) |

### Entities

| Method | Path | Description |
|---|---|---|
| `POST` | `/api/machines/{machine_id}/entities` | Create an entity |
| `GET` | `/api/machines/{machine_id}/entities` | List entities (filterable by `?state=`, `?region=`, `?updated_since=`) |
| `GET` | `/api/machines/{machine_id}/entities/{entity_id}` | Get an entity |
| `DELETE` | `/api/machines/{machine_id}/entities/{entity_id}` | Delete an entity |

### Evaluate (Recommended)

| Method | Path | Description |
|---|---|---|
| `POST` | `/api/machines/{machine_id}/evaluate` | Evaluate a single event (auto-creates entity) |
| `POST` | `/api/machines/{machine_id}/evaluate/batch` | Evaluate up to 1000 events in parallel |

**Evaluate request:**

```json
{
  "event_type": "pay",
  "entity_key": "order_id",
  "params": { "order_id": "abc123", "amount": "100" },
  "dispatch": true,
  "timestamp": 1700000000000
}
```

- `entity_key` — which field in `params` holds the entity ID
- `dispatch` — `true` (default): server fires actions; `false`: actions returned in response only
- `timestamp` — optional, defaults to current time (used for dedup)

### Transitions

| Method | Path | Description |
|---|---|---|
| `POST` | `/api/machines/{machine_id}/entities/{entity_id}/transition` | Transition a specific entity |
| `GET` | `/api/machines/{machine_id}/entities/{entity_id}/history` | Get transition history |

### Health

| Method | Path | Description |
|---|---|---|
| `GET` | `/health` | Health check (no auth required) |

---

## Database

Uses [Turso](https://turso.tech) (libsql) with embedded replica support for sub-millisecond reads.

**Tables:**

- `machines` — machine definitions (PK: `tenant_id, machine_id`)
- `entities` — entity state + context (PK: `tenant_id, machine_id, entity_id`)
- `transitions` — audit log of all state transitions

Migrations run automatically on startup. Schema changes use idempotent `ALTER TABLE ... ADD COLUMN` statements.

For local development, set `TURSO_DATABASE_URL=file:statemachine.db` to use a local SQLite file.

---

## Deployment

### Docker

```bash
docker build -t state-machine .
docker run -p 3051:3051 \
  -e TURSO_DATABASE_URL=... \
  -e TURSO_AUTH_TOKEN=... \
  -e API_KEY=... \
  state-machine
```

### Kubernetes

Manifests are in `k8s/`:

```
k8s/
├── namespace.yaml
├── deployment.yaml    # 2 replicas, resource limits, env from secrets
├── service.yaml       # ClusterIP on port 3051
├── ingress.yaml       # External access
├── hpa.yaml           # Auto-scale 2-6 replicas at 70% CPU
└── secrets.yaml       # Template (real secrets managed via kubectl)
```

### CI/CD

GitHub Actions workflow (`.github/workflows/deploy.yml`):

1. Build Docker image
2. Push to GHCR (`ghcr.io/inventhq/state-machine:<sha>`)
3. Apply K8s manifests
4. Update image tag + restart rollout

Triggered on push to `main`.

---

## Testing

```bash
# Unit tests (9 tests: flat FSM, parallel, joins, guards)
cargo test

# Run locally and test via curl
cargo run &
curl -X POST http://localhost:3051/api/machines \
  -H "Authorization: Bearer dev_secret" \
  -H "X-Tenant-Id: test" \
  -H "Content-Type: application/json" \
  -d '{"machine_id":"demo","states":["a","b"],"initial_state":"a","transitions":[{"from":"a","to":"b","on":"go"}]}'
```

---

## Project Structure

```
src/
├── main.rs              # Entry point, router setup, middleware
├── models.rs            # All data types: MachineDefinition, Entity, TransitionResponse, etc.
├── engine.rs            # Pure transition evaluation, guard checking, join logic
├── transition_core.rs   # Shared transition execution: DB update, actions, sub-machine runtime
├── actions.rs           # Webhook/event action dispatch with retry
├── scheduler.rs         # Background $timeout transition poller
├── auth.rs              # Bearer token middleware
├── db.rs                # Database init, migrations, replica fallback
├── errors.rs            # AppError enum → HTTP status codes
└── routes/
    ├── mod.rs           # AppState, helpers
    ├── machines.rs      # Machine CRUD + DashMap cache
    ├── entities.rs      # Entity CRUD + list filters
    ├── evaluate.rs      # Single + batch evaluate endpoints
    └── transitions.rs   # Direct transition + history endpoints
```

---

## License

Private — InventHQ
