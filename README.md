# State Machine Service

A multi-tenant, event-driven state machine engine in Rust (Axum, Tokio, libsql/Turso). It supports:
- flat machines and parallel regions, with joins;
- guards, and persisted `$timeout` transitions;
- child machines (sub-machines);
- webhook and event actions.

It implements the bounded semantics described here. It is **not** a general Harel statechart implementation; see
[Known limitations](#known-limitations).

**Contracts.** This README summarizes them; the contract documents are authoritative:

| Contract | Document | Covers |
|---|---|---|
| `statemachine-e1e4.v1` | [docs/E1_E4_CONTRACT.md](docs/E1_E4_CONTRACT.md) | atomic writes, event identity and dedup, per-region entry instances and timers, timeout identity, legacy child provenance, bound region queries |
| `statemachine-nested.v1` | [docs/NESTED_CONTRACT.md](docs/NESTED_CONTRACT.md) | opt-in **managed** child machines: eager start, atomic completion cascade, subtree cancellation, exact targeted delivery, bounds |

Contributor and agent guide: [AGENTS.md](AGENTS.md).

## Quick Start

### Prerequisites

- Rust 1.88+
- (Optional) a [Turso](https://turso.tech) database; a local SQLite file works for development.

### Local Development

```bash
git clone https://github.com/inventhq/state-machine.git
cd state-machine

cat > .env <<EOF
TURSO_DATABASE_URL=file:statemachine.db
TURSO_AUTH_TOKEN=
API_KEY=dev_secret
LISTEN_ADDR=0.0.0.0:3051
TIMEOUT_INTERVAL_SECS=10
EVENT_CORE_INGEST_URL=http://localhost:3030/ingest
EOF

cargo run
curl http://localhost:3051/health
```

`.env` is read by `dotenvy` from the working directory or any parent directory. Set variables explicitly when that is
not wanted.

### Environment Variables

| Variable | Description | Default |
|---|---|---|
| `TURSO_DATABASE_URL` | Turso URL, or a local file (`file:…`, an absolute path or `:memory:`) | `file:statemachine.db` |
| `TURSO_AUTH_TOKEN` | Turso auth token (empty for local) | empty |
| `API_KEY` | Bearer token for `/api/*` (empty = **no auth**, dev only) | empty |
| `LISTEN_ADDR` | Bind address | `0.0.0.0:3050` |
| `TIMEOUT_INTERVAL_SECS` | `$timeout` scheduler poll interval | `10` |
| `EVENT_CORE_INGEST_URL` | Where `event` actions are posted | `http://localhost:3030/ingest` |
| `PLATFORM_API_URL` | Platform API used to provision per-tenant ingest tokens | `https://api.juicyapi.com` |
| `PLATFORM_API_KEY` | Key for that provisioning call (empty = never provision) | empty |
| `RUST_LOG` | Log filter | `info` |

---

## Authentication and Tenancy

All `/api/*` routes require `Authorization: Bearer <API_KEY>`. A wrong or missing key gets `401`. `/health` is
unauthenticated.

`X-Tenant-Id: <tenant>` selects the tenant namespace. `evaluate` and `evaluate/batch` fall back to `params.key_prefix`
when the header is absent.

- The tenant is a **namespace asserted by the caller**, not an authorization boundary.
- There is one shared API key.

---

## Core Model

- A **machine** is a definition, keyed by `(tenant, machine_id)`.
- An **entity** is one instance of a machine, keyed by `(tenant, machine_id, entity_id)`.
- An entity's state is a **region map**:
  - **flat machine:** one region `"_"`. `current_state` is a string, for example `"processing"`;
  - **parallel machine:** one entry per region. `current_state` is an object, for example
    `{"payment": "captured", "fulfillment": "picking"}`.
- Each event moves **at most one region**: the first matching transition in definition order wins. No event is
  multicast.
- Event `params` (strings) are merged into the entity's `context` on every applied transition.

## Machine Definitions

### Flat machine

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

### Parallel regions and joins

```json
{
  "machine_id": "order_parallel",
  "regions": [
    { "id": "payment", "states": ["pending", "captured"], "initial_state": "pending" },
    { "id": "fulfillment", "states": ["picking", "packed", "complete"], "initial_state": "picking" }
  ],
  "transitions": [
    { "from": "pending", "to": "captured", "on": "pay", "region": "payment" },
    { "from": "picking", "to": "packed", "on": "pack", "region": "fulfillment" }
  ],
  "joins": [
    {
      "when": { "payment": "captured", "fulfillment": "packed" },
      "target_region": "fulfillment",
      "target_state": "complete"
    }
  ]
}
```

- Every parallel transition names its `region`.
- **Joins:** when all `when` conditions hold after a write, `target_region` is set to `target_state` in the same write,
  with a `$join` history row. Joins cascade up to 10 steps.
- Joins are **level-triggered**: a join whose `when` does not include its target region can fire again after later
  transitions. Two joins aimed at one region can alternate. Write each join's `when` so it names the target region
  in a state other than the target.
- Region ids are matched literally everywhere (they are never interpolated into SQL). Duplicate region ids are
  rejected.

### Guards

```json
{
  "from": "pending",
  "to": "confirmed",
  "on": "pay",
  "guard": { "all": [
    { "field": "amount_cents", "op": "gt", "value": 0 },
    { "field": "currency", "op": "eq", "value": "USD" }
  ] }
}
```

- **Operators that exist:** `eq`, `neq` (also `ne`), `gt`, `gte`, `lt`, `lte`, `contains` (substring). Compound guards
  use `all` and `any`. Any other operator evaluates **false**.
- **Value lookup:** a field is read from the event `params` first, then from the entity `context`. A param that parses
  as a number is compared as a float, so `eq` against an integer literal does not match; use `gte`/`lte`, or a float
  literal such as `5.0`.
- **Malformed guards pass.** A guard that is not an object, lacks `field`/`op`/`value`, or uses a non-array
  `all`/`any` evaluates **true**. Validate guards before registering.

### Timeouts

```json
{ "from": "pending", "to": "expired", "on": "$timeout", "guard": { "timeout_seconds": 3600 } }
```

- `timeout_seconds` must be a top-level integer. The scheduler checks every `TIMEOUT_INTERVAL_SECS`.
- Region `r` times out from `f` when `r` is in `f` and **`r`'s own entry** is at least `timeout_seconds` old.
  Transitions in other regions do not postpone it.
- Entry instances are kept in `region_entries`. Every write that sets a region's state starts a new entry, including
  an explicit self-transition, which re-enters and restarts that region's timer.
- Each timeout commits once per entry instance, with identity `["timeout", region, seq]`. Restarts and concurrent ticks
  do not repeat it.

### Actions

```json
{
  "actions": [
    { "on_enter": "shipped", "action": { "type": "webhook", "url": "https://example.com/notify" } },
    { "on_enter": "captured", "region": "payment", "action": { "type": "event", "event_type": "order.captured" } }
  ]
}
```

- Actions fire on entering a state. Join definitions may carry their own `actions`. On the `$timeout` path, join
  actions are not dispatched.
- With `dispatch: true` (the default for `evaluate`; always for `/transition` and the scheduler), actions are sent
  **after the write commits**. Each makes up to 4 attempts with exponential backoff.
- With `dispatch: false`, they are only returned in the response.
- **Dispatch is best effort:** a crash between commit and dispatch loses that batch. There is no outbox.
- A `dispatch: true` write also asks the platform for an ingest token when `PLATFORM_API_KEY` is set and none is
  cached, even if the write has no actions.

---

## Execution Guarantees (`statemachine-e1e4.v1`)

- **One transaction per write.** Each request, and each timer candidate, runs in one `BEGIN IMMEDIATE` transaction. It
  re-reads the entity, deduplicates, evaluates, and then writes history rows (`$join` included), state, context,
  `state_version` and `region_entries` together. Any failure rolls back all of it.
- **Event identity.** An external event is identified by `(event_type, timestamp)` on the entity it applies to;
  `timestamp` defaults to the current time.
  - Replaying it with **equal params** returns the unchanged duplicate response (`200`,
    `reason: "duplicate event (already processed)"`).
  - The same key with **different params** returns `409 EVENT_IDENTITY_CONFLICT` (`retry:false`), and nothing is
    written.
- **History** rows carry `identity_key`, for example `["evt","pay",1700000000000]`, `["timeout","_",3]`,
  `["sub","b1","ret",2]` or `["join","fulfillment",4,0]`, and `cause` (provenance). Rows written before E1 have
  `null`.
- **Contention.** There is no busy timeout. A request or tick that cannot take the write lock gets
  `500 … database is locked` (`retry:true`) with **nothing applied**. Replaying the same identity is safe.
- **Legacy rows.** Rows written by a pre-E2 binary report truthful entry times: exact for flat rows, an upper bound for
  parallel rows (a timer may fire late, never early).

---

## Child Machines

A state can own a child machine through `sub_machines` (`regions[].sub_machines` for a parallel parent). The child
entity id is `{parent_entity_id}::sub::{state}`, in the child machine's namespace.

### Legacy children (default)

```json
{
  "machine_id": "order",
  "states": ["placed", "processing", "shipped", "cancelled"],
  "initial_state": "placed",
  "transitions": [
    { "from": "placed", "to": "processing", "on": "start_processing" },
    { "from": "processing", "to": "cancelled", "on": "cancel" }
  ],
  "sub_machines": {
    "processing": { "machine_id": "processing_flow", "on_final": { "labeling": "shipped" } }
  }
}
```

- **Forwarding.** An event with no parent transition is forwarded to the active child: in region definition order, and
  the first child that handles it wins. Parent transitions (escapes) always win.
- **Lazy creation.** The child is created on the first forwarded event, not when the parent enters the state. It is not
  reset if the state is entered again.
- **Completion.** When the child's state is an `on_final` key, the parent advances in the same transaction, with a
  `$sub_complete` row.
- **Recovery.** An event that reaches an already-final child advances the parent (it consumes that event), but only if
  that final state is backed by a committed child history row newer than the parent's entry. Otherwise the result is
  `409 UNCORRELATED_CHILD_FINAL`.
- Only flat children can complete. A legacy compound state cannot be a flat machine's `initial_state`. Replaying a
  child-handled event at the parent answers "no transition".

### Managed children (opt-in, `statemachine-nested.v1`)

```json
{
  "machine_id": "bundle",
  "regions": [
    { "id": "b1", "states": ["idle", "ret_b1", "returned", "kept", "cancelled"], "initial_state": "idle",
      "sub_machines": { "ret_b1": {
        "machine_id": "return_case",
        "lifecycle": "managed",
        "complete_when": [
          { "when": { "case": "returned", "deadline": "closed" }, "target": "returned" },
          { "when": { "case": "kept", "deadline": "closed" }, "target": "kept" }
        ]
      } } }
  ],
  "transitions": [
    { "region": "b1", "from": "idle", "to": "ret_b1", "on": "bundle.open" },
    { "region": "b1", "from": "ret_b1", "to": "cancelled", "on": "bundle.cancel_b1" }
  ]
}
```

With `"lifecycle": "managed"` on a compound state:

- **Start.** The child is created eagerly and create-only, in the same commit that enters the state. That includes
  entity creation and a flat or region `initial_state` that is a managed compound. The child's own initial managed
  states start recursively.
- **Completion.**
  - A flat child completes on `on_final[state]`.
  - Any child completes on the first `complete_when` rule. A rule's `when` must name **every** child region; this is
    how a parallel child completes.
  - Completion is set in the same write. The child's remaining active descendants are cancelled, and the parent
    advances (`$sub_complete`, `cause.via: "managed"`), cascading up to the root **in one commit**. This applies on
    every path, scheduler timeouts included. There is no nudge or resume.
- **Cancellation.** Any write that moves the parent region out of the compound state cancels the child subtree,
  deepest first, in that commit: an explicit cancel edge, a join or a timeout. Each cancelled child gets a
  `$sub_cancel` row and `instance.status: "cancelled"`. A cancelled child accepts no event and its timers never fire.
  Siblings are untouched.
- **Identity.** The child id is still `{parent}::sub::{state}`.
  - Entity reads add `instance`: `{status, depth, path, started, ended}`. `path` runs root first, with each
    ancestor's `{machine_id, entity_id, region, state, seq}`.
  - Re-entering a managed compound state, or any existing entity at the child id, is refused with
    `409 INSTANCE_CONFLICT`.
  - Depth is at most 8 managed levels.
- **Registration checks.**
  - Registration always rejects `complete_when` on a legacy child, and a managed compound state name that appears in
    more than one region.
  - Managed cycles, depth over 8, and `complete_when` rules that do not name every child region are checked only
    against children that are **already registered**. A child that is not registered yet is skipped at registration.
  - The same rules apply when that child starts:
    - a rule gap returns `400`;
    - a child machine still missing returns `404`;
    - depth over 8 returns `409 NESTING_DEPTH_EXCEEDED`. That bound also stops a cycle among definitions registered
      out of order.

#### Targeted delivery

`evaluate` accepts `target`: the managed compound states from the root to the instance. `target: []` is the root
itself.

```json
{
  "event_type": "decision.approve",
  "entity_key": "id",
  "params": { "id": "run-7" },
  "timestamp": 9001,
  "dispatch": false,
  "target": [ { "region": "b1", "state": "ret_b1", "seq": 2 }, { "region": "case", "state": "settling" } ]
}
```

- A step's `region` defaults to `"_"`. `seq` is optional; when given, it must equal the ancestor's current entry seq.
- **Order:**
  1. the engine deduplicates at the target, so a replay is a duplicate even after the instance ended;
  2. it checks that every step is the current active instance, or answers `409 TARGET_NOT_ACTIVE`;
  3. it evaluates **only the target's own transitions**, never forwarded down or up. Completion still cascades
     upward.
- A cancel edge on a parent is sent with `target` = the **parent's** path.
- Direct `evaluate` on an ended managed child's machine returns `409 INSTANCE_NOT_ACTIVE`.

**Response additions.** On targeted calls `target` is `{machine_id, entity_id, depth}`. `instances` lists the commit's
lifecycle events in order, for example:

```json
{
  "entity_id": "run-7::sub::ret_b1::sub::settling",
  "previous_state": "observing",
  "current_state": "settled",
  "transition": "observing → settled",
  "triggered_by": "return.buyer.observed",
  "timestamp": 9002,
  "target": { "machine_id": "settle", "entity_id": "run-7::sub::ret_b1::sub::settling", "depth": 2 },
  "instances": [
    { "kind": "completed", "machine_id": "settle", "entity_id": "run-7::sub::ret_b1::sub::settling", "depth": 2,
      "parent": { "machine_id": "return_case", "entity_id": "run-7::sub::ret_b1", "region": "case", "state": "settling", "seq": 3 },
      "row": 41, "key": "[\"evt\",\"return.buyer.observed\",9002]", "parent_to": "returned", "parent_row": 42,
      "parent_key": "[\"sub\",\"case\",\"settling\",3]" }
  ]
}
```

---

## API Reference

All `/api` routes need `Authorization: Bearer <token>` and `X-Tenant-Id`. `evaluate` and `evaluate/batch` accept
`params.key_prefix` instead when the header is absent (legacy fallback; see
[Authentication and Tenancy](#authentication-and-tenancy)).

| Method | Path | Description |
|---|---|---|
| `POST` | `/api/machines` | Create a machine (create-only; `409` if it exists) |
| `GET` | `/api/machines` | List the tenant's machines |
| `GET` | `/api/machines/{machine_id}` | Get a machine |
| `PUT` | `/api/machines/{machine_id}` | Replace a machine definition in place (affects in-flight entities; per-process cache) |
| `DELETE` | `/api/machines/{machine_id}` | Delete a machine (`409` while entities exist) |
| `POST` | `/api/machines/{machine_id}/entities` | Create an entity (`{"entity_id", "context"?}`) |
| `GET` | `/api/machines/{machine_id}/entities` | List entities; filters `?state=`, `?region=&state=` (parallel), `?updated_since=` |
| `GET` | `/api/machines/{machine_id}/entities/{entity_id}` | Get an entity |
| `DELETE` | `/api/machines/{machine_id}/entities/{entity_id}` | Delete an entity **and its history** (children are not deleted) |
| `POST` | `/api/machines/{machine_id}/evaluate` | Evaluate one event; auto-creates the entity |
| `POST` | `/api/machines/{machine_id}/evaluate/batch` | Up to 1000 events, each as its own `evaluate` |
| `POST` | `/api/machines/{machine_id}/entities/{entity_id}/transition` | Transition an existing entity (`{event_type, params, timestamp?}`; always dispatches) |
| `GET` | `/api/machines/{machine_id}/entities/{entity_id}/history` | History rows, ordered by `timestamp`, then `id` |
| `GET` | `/health` | Health check (no auth) |

**Evaluate request:**

| Field | Meaning |
|---|---|
| `event_type` | event name |
| `entity_key` | which `params` field holds the entity id |
| `params` | string map: merged into context, read by guards |
| `timestamp` | identity of the event (default: now). Use a stable value per event to make replays safe |
| `dispatch` | default `true` |
| `target` | optional, managed targeted delivery (above) |

**Responses:**
- **Transition response:** `entity_id`, `previous_state`, `current_state`, `transition` (label or `null`), `region`,
  `triggered_by`, `actions_dispatched`, `joins_fired`, `timestamp`, `reason` (no transition or duplicate),
  `sub_machine` (one level), `target`, `instances`. Empty optional fields are omitted.
- **Entity:** `machine_id`, `tenant_id`, `entity_id`, `current_state`, `context`, `state_version`, `created_at`,
  `updated_at`, `region_entries` (per region: `{state, seq, entered_at, basis, row, key}`), and `instance` (managed
  children only).
- **History row:** `id`, `from_state`, `to_state`, `event_type` (`$join`, `$timeout`, `$sub_complete` and
  `$sub_cancel` are engine rows), `event_params`, `actions_dispatched`, `region`, `timestamp`, `created_at`,
  `identity_key`, `cause`.
- **Batch:** `{total, succeeded, failed, results: [{entity_id?, result?, error?}]}`. A failed event's `error` is a
  string.

### Errors

Errors are `{"error": {"code", "message", "retry"}}`.

| Code | Status | `retry` | When |
|---|---|---|---|
| `BAD_REQUEST` | 400 | false | invalid request or definition; missing tenant; a `target` naming a non-managed state |
| `UNAUTHORIZED` | 401 | false | missing or wrong bearer token |
| `NOT_FOUND` | 404 | false | unknown machine or entity |
| `CONFLICT` | 409 | true | machine/entity already exists, delete with entities, or the version/identity guard (not expected while writers use the transaction) |
| `EVENT_IDENTITY_CONFLICT` | 409 | false | same `(event_type, timestamp)` with different params |
| `UNCORRELATED_CHILD_FINAL` | 409 | false | legacy recovery refused (child final not provable) |
| `TARGET_NOT_ACTIVE` | 409 | false | targeted path or entry is not the current active instance |
| `INSTANCE_NOT_ACTIVE` | 409 | false | event for an ended managed child |
| `INSTANCE_CONFLICT` | 409 | false | managed start would reuse an existing entity (re-entry or collision) |
| `NESTING_DEPTH_EXCEEDED` | 409 | false | managed start beyond 8 levels |
| `INTERNAL_ERROR` | 500 | true | database error, including `database is locked`; nothing was committed |

---

## Database

libsql/Turso. A local file or `:memory:` URL uses a local database. Remote URLs try an embedded replica first and fall
back to a pure remote connection.

| Table | Contents |
|---|---|
| `machines` | definitions (PK `tenant_id, machine_id`) |
| `entities` | state, context, `state_version`, `region_entries` (E2), `instance` (managed children) |
| `transitions` | history, with `region`, `identity_key` (unique per entity when set) and `cause` |
| `ingest_tokens` | per-tenant tracker tokens |

Migrations run at every startup and are idempotent (`CREATE … IF NOT EXISTS`, `ALTER TABLE … ADD COLUMN`). Startup
also logs, and never modifies, legacy history rows that share one `(event_type, timestamp)` key.

---

## Known Limitations

- **Contention (E6):** there is no busy timeout or WAL. A write that loses the lock fails immediately with `500
  database is locked` (nothing applied). Under concurrency most losers are refused rather than queued.
- **Not a general statechart:**
  - no history states;
  - no re-entry of a managed compound state;
  - one region per event (no multicast);
  - level-triggered joins (E5);
  - no unrestricted cycles.
- **Managed child already final at start (review EF-1):** a managed child whose initial state already satisfies its
  completion rule is never completed, so its parent stays in the compound state. Loaders must refuse such definitions.
  Likewise, `complete_when` must cover every terminal combination the child can reach, or the parent can be stranded.
- **Corrupt instance metadata (review EF-2):** an undecodable `entities.instance` value is read as "not managed", which
  fails open: direct evaluate and timers treat it like a root. It is reachable only through database corruption or a
  foreign writer.
- **Legacy children:** a replay of a child-handled event at the parent is "no transition", not duplicate (E7). The
  recovery advance consumes its event. Children are never reset. A legacy (lazy) child does not start managed initial
  states inside it.
- **Effects:** dispatch after commit is best effort, with no outbox. Ingest-token provisioning can run for writes
  without actions (E8).
- **Operations:**
  - `PUT` replaces definitions in place, and the definition cache is per process;
  - deleting an entity deletes its history (its dedup memory) but not its children; re-creating the root then hits
    `INSTANCE_CONFLICT`;
  - run no pre-E2 binary against the same database;
  - the startup duplicate audit scans legacy history.
- **Remote Turso and embedded-replica transactions** are not covered by the test suite, which uses local files.

---

## Testing

```bash
cargo test           # all suites
cargo test nested    # managed nested children only
cargo test e1e4      # E1–E4 DB-backed tests only
```

The accepted suite at engine `ebe04589` is **46 tests** (author-run; this documentation change did not rerun them):

| File | Tests | Scope |
|---|---|---|
| `src/engine.rs` | 18 | pure evaluation, guards, joins (9 original), identity keys, entry instances, legacy derivation, region validation |
| `src/e1e4_tests.rs` | 19 | DB-backed: fault rollback with zero outbound requests, replay and conflict, concurrency, old-schema startup, region timers, timeout and join receipts, legacy child provenance, bound region queries |
| `src/nested_tests.rs` | 9 | DB-backed managed children: depth-3 cascade through a reused parallel child, timeout cascade across restart, selected cancellation, event-vs-timer race, re-entry refusal, depth and cycle bounds, whole-cascade rollback, validation, managed initial compound states |

DB-backed tests use a disposable SQLite file and an in-process loopback HTTP sink; failures are injected with SQLite
triggers on that file.

---

## Deployment

- **Docker:** a multi-stage build on `debian:bookworm-slim` (exposes 3051).
- **Kubernetes:** manifests in `k8s/`:
  - `namespace.yaml`;
  - `deployment.yaml` (2 replicas);
  - `service.yaml`;
  - `ingress.yaml`, `certificate.yaml`, `cluster-issuer.yaml`, `traefik-lb.yaml`;
  - `hpa.yaml` (2–6 replicas at 70% CPU);
  - `secrets.yaml` (template; real secrets are created by the workflow).
- **CI/CD:** `.github/workflows/deploy.yml` builds and pushes `ghcr.io/inventhq/state-machine` on every push to `main`,
  then deploys.

## Project Structure

```
src/
├── main.rs              # Entry point, router, auth layer, scheduler start
├── models.rs            # Definitions, entities, region entries, managed instances, API types, validation
├── engine.rs            # Pure evaluation, guards, joins, identity keys, entry-instance helpers
├── transition_core.rs   # Transactional write path, dedup, targeted delivery, child lifecycle
├── scheduler.rs         # $timeout poller (per-candidate transaction)
├── actions.rs           # Webhook/event dispatch with retry
├── ingest_tokens.rs     # Per-tenant ingest token provisioning
├── auth.rs              # Bearer token middleware
├── db.rs                # Database init, migrations, legacy duplicate audit
├── errors.rs            # AppError → HTTP status and codes
├── e1e4_tests.rs        # DB-backed E1–E4 tests
├── nested_tests.rs      # DB-backed managed-children tests
└── routes/              # machines, entities, evaluate, transitions
docs/
├── E1_E4_CONTRACT.md    # statemachine-e1e4.v1
└── NESTED_CONTRACT.md   # statemachine-nested.v1
```

## License

Private — InventHQ
