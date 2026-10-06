# Engine contract `statemachine-e1e4.v1`

Additive changes on top of `226279ae`. Every existing route, field and default is unchanged. This file is the
versioned schema/API note for E1 (atomic writes and dedup identity), E2 (per-region entry instances), E3 (timeout
identity and parent/child provenance) and E4 (bound region queries).

## Schema (idempotent startup migration)

| Change | Existing rows |
|---|---|
| `entities.region_entries TEXT NULL` | NULL; read through the legacy rules below; persisted on the row's next engine write |
| `transitions.identity_key TEXT NULL` | NULL |
| `transitions.cause TEXT NULL` (JSON) | NULL |
| `UNIQUE INDEX idx_transitions_identity ON transitions(tenant_id, machine_id, entity_id, identity_key) WHERE identity_key IS NOT NULL` | cannot conflict (legacy rows are NULL) |

At startup the engine logs (WARN) every group of legacy rows sharing `(tenant, machine, entity, event_type,
timestamp)`, `$join` excluded. It never modifies or deletes them; dedup keeps consulting them.

## Identity keys

Canonical compact JSON arrays stored as text, unique per `(tenant, machine, entity)`. Flat machines use region `"_"`.

| `event_type` | `identity_key` |
|---|---|
| an external event (evaluate, batch, transition route; also on a child entity reached by forwarding) | `["evt","<event_type>",<timestamp>]` |
| `$timeout` | `["timeout","<region>",<seq>]` (`seq` of the instance that timed out) |
| `$sub_complete` | `["sub","<region>","<compound_state>",<seq>]` (`seq` of the compound-state instance that completed) |
| `$join` | `["join","<target_region>",<seq>,<step>]` (`seq` of the instance the join entered; `step` in the write's cascade) |

No identity uses tick wall-clock time or a `from→to` string.

## Region entry instances

Entity reads (`GET …/entities/{id}`, `GET …/entities`, `POST …/entities`) carry `region_entries`:

```json
"region_entries": {
  "deadline": {"state": "watching", "seq": 4, "entered_at": 1791293401018,
               "basis": "transition", "row": 17, "key": "[\"evt\",\"case.opened\",7001]"}
}
```

- `seq`: the `state_version` produced by the write that entered the region's current state (1 at creation).
- `entered_at`: server time (ms) of that write. `row`/`key`: id and `identity_key` of the history row that entered it,
  `null` when none did.
- Every write that sets a region's state starts a new instance: transition, explicit self-transition (external: it
  re-enters and restarts the region's timer), join, `$sub_complete`, `$timeout` (a `$timeout` self-loop restarts its
  timer). Regions the write does not set keep their instance.
- The persisted document is `{"version": <state_version>, "regions": {…}}`, written by the same `UPDATE` as the state.
  It is trusted only while `version` equals the row's `state_version`.
- `basis` and legacy rules (no document, or a stale one because a writer without entry tracking changed the row):
  - `created`: entered at creation (a row still at version 1 with `updated_at == created_at`).
  - `transition`: entered by a committed engine write.
  - `legacy_exact`: flat row. Every write re-enters a flat entity's only region, so `updated_at` is exact.
  - `legacy_upper_bound`: parallel row. `updated_at` is an upper bound of the true entry: the timer never fires early
    and may fire late. It is persisted unchanged at the next engine write, so later writes do not postpone it.

A `$timeout` for region `r` from `f` is due when `r` is in `f` and `region_entries[r].entered_at + timeout_ms <= now`.
`engine::evaluate` remains pure; `now` is supplied by the caller.

## Atomicity, dedup and responses

- Every writer (direct transition, forwarded child transition plus the parent's `$sub_complete`, joins, scheduler) runs
  in one `BEGIN IMMEDIATE` transaction that re-reads the entity, deduplicates, evaluates and writes state, context,
  version, `region_entries` and its history rows. Any failure rolls the whole transition back.
- Network effects (ingest-token provisioning, webhook/event actions) run only after commit, never after a rollback,
  conflict or duplicate. No outbox or new retry owner was added, so post-commit dispatch is best effort: a crash
  between commit and dispatch loses that batch (policy definitions keep `actions: []` and `dispatch: false`; domain
  owners remain the effect authorities).
- Dedup runs inside the transaction on the entity the event was submitted to, over every row with the same
  `(event_type, timestamp)` (legacy rows included), comparing stored `event_params` with the request `params` as maps.

| Outcome | Response |
|---|---|
| Exact duplicate | unchanged `200`: `transition: null`, `reason: "duplicate event (already processed)"`; states read in the transaction |
| Same `(event_type, timestamp)`, different params | `409 {"error":{"code":"EVENT_IDENTITY_CONFLICT","message":…,"retry":false}}`, nothing written |
| Failure inside the transaction | unchanged `500 INTERNAL_ERROR retry:true`, nothing committed; a same-identity replay is applied once or answered as duplicate |
| Lock contention | unchanged `500 … database is locked`, `retry:true`, nothing applied (no busy timeout: out of scope) |
| Version mismatch | unchanged `409 CONFLICT retry:true`; kept as a guard, not expected while writers hold the transaction's write lock |
| Uncorrelated child final | `409 {"error":{"code":"UNCORRELATED_CHILD_FINAL",…,"retry":false}}`, nothing written |

History records carry `identity_key` (string or null) and `cause` (object or null), ordered by `timestamp`, then `id`.

## Timeouts, joins and parent/child provenance

- No engine-reserved context key is written. A `$timeout` commits, with the state: a history row
  (`identity_key ["timeout",r,seq]`; `timestamp` = tick time; `event_params` = context snapshot, unchanged;
  `cause {"kind":"timeout","region","from","to","entry":{"seq","entered_at","basis"},"timeout_ms","due_at"}`), and
  `region_entries[r]` with `key`/`row` of that row.
- Each candidate fires in its own transaction after a re-check, guarded by the version and the unique identity: a
  restart, repeated tick or concurrent tick creates at most one `$timeout` row and one action batch per instance. A
  failing candidate is logged and skipped; it does not abort the tick.
- `$join` rows on every path (including the scheduler) are in the same commit, with
  `cause {"kind":"join","cause_key":<key of the write's main row>,"step":i}`. Join actions on the timeout path stay
  undispatched, as before.
- The scheduler never advances a parent. A child made final by `$timeout` reaches its parent only through the existing
  recovery advance (a parent event with no parent edge, forwarded to the final child).
- `$sub_complete` rows: `event_params {}` and the trigger's `timestamp` (unchanged), `identity_key ["sub",r,S,seq]`,
  `cause {"kind":"sub_complete","via":"forward"|"recovery","trigger":{"event_type","timestamp"},"parent_entry":{"region","state","seq","basis","row"},"child":{"machine_id","entity_id","state","row","key"}}`.
- Recovery refuses with `UNCORRELATED_CHILD_FINAL` unless the child's latest committed history row entered its final
  state and that row's id is greater than the row that entered the parent's current instance of the compound state
  (legacy parents: their latest row entering it; none and `state_version > 1` refuses).

## Region-dependent queries

- `GET …/entities?region=&state=` and the scheduler bind region and state as parameters and match them as a JSON key
  and value (`json_each`). No SQL text is built from a definition or query string. Every previously valid region name
  keeps working, including quotes, dots and non-ASCII; no charset rule was added.
- A `region` filter on a flat machine returns `[]` (it was a `500` malformed-JSON error).
- Create/update rejects duplicate region ids, a region `sub_machines` state outside that region's states, and an
  `on_final` target outside that region's states. Stored definitions still load and run without validation.

## Unchanged and not claimed

One region per event (first match); level-triggered joins and same-region join alternation; no busy timeout or WAL;
a replay of a child-handled event at the parent still answers "no transition"; the recovery advance still consumes its
trigger; child entity ids carry no generation and a compound re-entry does not reset the child; parallel children
cannot complete a parent; orphaned child timers still fire into the orphaned child; ingest tokens are still provisioned
for `dispatch=true` writes without actions. Embedded-replica and remote Turso transactions are not covered by the local
tests. This is not a claim of general statechart correctness.
