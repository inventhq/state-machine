# Engine contract `statemachine-nested.v1`

Additive to `statemachine-e1e4.v1` (`docs/E1_E4_CONTRACT.md`). Managed nested children are opt-in per compound state;
every existing definition, route, field and default is unchanged. Tests: `src/nested_tests.rs` (DB-backed) on top of
the existing engine and E1–E4 suites.

## 0. Compatibility

Everything is **opt-in per compound state** with `"lifecycle": "managed"`. Without it a `SubMachineDef` is `legacy` and
behaves exactly as at `93358dbd`:
- lazy child creation;
- forward-path completion;
- recovery advance and `UNCORRELATED_CHILD_FINAL`;
- EV-1 unchanged.

Definitions that do not use the new fields serialize byte-identically, so registration and digest compares are
unchanged. v1/v2 templates and all 37 existing tests must stay exact.

## 1. Definition fields (on `SubMachineDef`)

```json
"sub_machines": {
  "ret_b1": {
    "machine_id": "<registered recipient-return id>",
    "lifecycle": "managed",
    "complete_when": [
      {"when": {"case": "returned", "deadline": "closed"}, "target": "returned"},
      {"when": {"case": "kept",     "deadline": "closed"}, "target": "kept"}
    ]
  },
  "settling": {"machine_id": "<reverse-and-settle id>", "lifecycle": "managed", "on_final": {"settled": "done"}}
}
```

| Field | Default | Meaning |
|---|---|---|
| `lifecycle` | `"legacy"` (not serialized) | `"managed"` enables sections 2–6 for this compound state |
| `on_final` | `{}` (was required; now defaults) | flat child: child state → parent target |
| `complete_when` | `[]` (not serialized; managed only) | ordered rules; the first fully satisfied rule completes the child to `target` |

**Rules for `complete_when`:**
- Every rule's `when` must name **every** region of the child machine. A flat child uses region `"_"`.
- Rules are checked at registration when the child machine is already registered, and again at child start, where a
  violation is refused. The engine does not judge "terminal": stability of those states is a template/loader rule.
- Completion is checked against the full region map after the write (joins included).

**Registration validation (400; stored definitions still load):**
- `complete_when` requires `managed`.
- Every rule's `when` must be non-empty and its `target` must be a state of the parent (region).
- `on_final` targets as before.
- A managed compound state name must appear in **only one** region of its machine (child ids are keyed by state).
- A flat machine's `initial_state` (like a region's) may be a **managed** compound state: its child starts with the
  entity (section 3.1). A **legacy** compound `initial_state` is still refused, because legacy children never start on
  entry.
- Managed references must be acyclic and at most 8 levels deep over the definitions registered at that moment.
  Unregistered children are skipped there; the runtime bound in section 5 always applies.

## 2. Instance identity

- **Child entity id**, as for legacy children: `{parent_entity_id}::sub::{state}`, in the child machine's namespace,
  for example `run-7::sub::ret_b1::sub::settling`. The app's slash path `ret_b1/settling` maps to it 1:1.
- **Create-only.** A managed child is created by the engine at start. If any entity already exists at that id, the
  entering write is refused with `INSTANCE_CONFLICT` and nothing is written. That covers re-entry, a self-transition
  on a managed compound state, a pre-created entity and a cross-machine id collision. **Re-entry is not supported.**
- **Entity reads** of a managed child add `instance`; it is absent on roots and legacy children:

```json
"instance": {"status": "active", "depth": 2,
  "path": [{"machine_id":"<root id>","entity_id":"run-7","region":"b1","state":"ret_b1","seq":2},
           {"machine_id":"<ret id>","entity_id":"run-7::sub::ret_b1","region":"case","state":"settling","seq":5}],
  "started": 1, "ended": null}
```

- `path` runs root first. Each step gives an ancestor, its region and compound state, and the `seq` of that
  compound-state entry instance (`region_entries`, `statemachine-e1e4.v1`). `depth = path.len()`.
- `status` is `active`, `completed` or `cancelled`. `ended` is `{row, key}` of the write that ended it.
- Context starts `{}`. The engine stores **no per-instance args**: the adapter resolves `arg.*` and sends adapter-built
  params with each event, as it does today.

## 3. Lifecycle (always inside the triggering request's or timer's single transaction)

1. **Start (eager).** A write that enters managed compound `S` in region `r` creates the child in the same commit:
   version 1, no history row, initial state, `region_entries` starting now, status `active`. Its own initial managed
   compound states start recursively. Entering writes include:
   - a transition;
   - a join;
   - a `$sub_complete` target;
   - a `$timeout` target;
   - creation of a root whose region (or flat `initial_state`) starts in `S`;
   - the start of a managed child whose own initial state (flat or a region's) is managed compound, recursively.
2. **Cancellation is any exit.** A write that moves `r` away from the `S` instance cancels that child and, deepest
   first, its active descendants, in the same commit. Each cancelled entity gets:
   - version+1;
   - a `$sub_cancel` row: identity `["cancel",<parent seq>]`, `cause {kind:"cancel",reason,parent:<step>,by:{row,key}}`;
     `reason` is `parent_exit`, `completed` or `ancestor_cancelled`;
   - status `cancelled`.

   The entity's state and history remain readable. **Selected-branch cancellation** is the parent's explicit cancel
   edge (`S --cancelEvent--> parentState`), delivered with `target` = the **parent's** path. Siblings are untouched.
   The cancel commit is the durable intent and the linearization point, with no multi-transaction cascade and no crash
   window. A cancelled descendant never fires a timer, never accepts an event and never completes anything.
3. **Completion.** After any write to an active managed child, the engine checks `on_final[state]` (flat) or the first
   satisfied `complete_when` rule. If one matches:
   - the same `UPDATE` sets status `completed` and `ended` = that write's row/key;
   - the child's still-active descendants are cancelled (`reason:"completed"`);
   - the parent region advances `S → target` with `$sub_complete`, identity `["sub",r,S,<parent seq>]`;
   - this cascades upward.

   This happens on every path: untargeted, targeted, direct child evaluate, and **the scheduler**.
   **There is no nudge and no resume for managed children.** They are never final while their parent is still in `S`.
4. **One winner.** A cancel, a child completion and a child timer touching one instance are serialized by the
   transaction (`BEGIN IMMEDIATE`). The first commit wins.
   - The loser of an event race gets `TARGET_NOT_ACTIVE` or `INSTANCE_NOT_ACTIVE`, or "no transition" at a parent
     already out of `S`.
   - A losing timer simply does not fire.
   - Owner effects are outside the engine and are never undone.

## 4. Delivery

**Targeted (exact, no multicast).** `POST /machines/{root}/evaluate` (and each batch event) accepts `target`, a list of
steps `{"region": <default "_">, "state": <managed compound state>, "seq": <optional entry seq>}`.

```json
{"event_type":"decision.approve","entity_key":"id","params":{"id":"run-7"},"timestamp":9001,"dispatch":false,
 "target":[{"region":"b2","state":"ret_b2","seq":2}]}
```

1. The engine resolves the target entity id from the definitions. `target: []` is the root itself.
2. It deduplicates **at the target** over `["evt",E,T]`. Equal params return the unchanged `duplicate` 200, even after
   the instance ended. Different params return `EVENT_IDENTITY_CONFLICT`.
3. It checks the path: each ancestor is active; region `r` is in `state`; the entry `seq` equals the given `seq` when
   one is given; the child exists, is `active` and belongs to that entry. Otherwise it returns
   `409 TARGET_NOT_ACTIVE` and nothing is written.
4. It evaluates **only the target's own transitions**: never forwarded to children, never to ancestors. With no match
   the answer is "no transition". At a compound state the target's own edges (for example a cancel edge) are those
   transitions.

**Untargeted** delivery is unchanged legacy routing:
- parent edges first;
- then active children in region definition order;
- first handler wins.

A managed child is entered only while `active`, and its completion cascades atomically.

**Direct evaluate on a managed child entity** (child machine route) works while `active` and runs the cascade. On an
ended child it returns `409 INSTANCE_NOT_ACTIVE`; dedup still answers replays first.

## 5. Timers and bounds

- A managed child's timers count from its start, which is the parent's entry into `S`.
- The scheduler re-checks status inside its transaction and fires only for roots, legacy children and `active` managed
  children.
- A managed timer completion cascades to the parent and above **in the same commit**. Restart is safe: there is no
  in-memory state, and the E3 identities hold.
- **Engine bound:** 8 managed levels (`instance.depth ≤ 8`), enforced at start with `409 NESTING_DEPTH_EXCEEDED`.
  There is no engine branch or instance-count limit beyond regions. The app/template limits (depth 4 with root = 1,
  32 instances, 16 machines, 4 branches) are stricter and stay loader rules.

## 6. Responses, rows and errors (all additive)

**Responses:**
- `TransitionResponse.target`: `{machine_id, entity_id, depth}`, on targeted calls; the other fields describe the
  target.
- `TransitionResponse.instances` (omitted when empty) lists the commit's lifecycle events in order:
  - `{kind:"started",machine_id,entity_id,depth,parent}`;
  - `{kind:"completed",…,row,key,parent_to,parent_row,parent_key}`;
  - `{kind:"cancelled",…,reason,row,key}`.
- An untargeted response's `current_state` is read back after any cascade.

**New row kinds:**
- `$sub_cancel` (above).
- `$sub_complete` with `cause.via:"managed"` and `trigger:{event_type,timestamp}`. The trigger is the bottom event or
  `$timeout`, the same at every level of one cascade. `parent_entry` and `child:{machine_id,entity_id,state,row,key}`:
  the child's row is the write that satisfied completion, which for a cascade is the child's own `$sub_complete`.

**Errors (`retry:false`, nothing written):**

| Code | Status | When |
|---|---|---|
| `TARGET_NOT_ACTIVE` | 409 | the targeted path or entry is not the current active one |
| `INSTANCE_NOT_ACTIVE` | 409 | direct evaluate on an ended managed child |
| `INSTANCE_CONFLICT` | 409 | a start would reuse an existing entity id (re-entry or collision) |
| `NESTING_DEPTH_EXCEEDED` | 409 | a start beyond 8 levels |
| `BAD_REQUEST` | 400 | `target` names a non-managed or unknown compound state; definition validation; an invalid `complete_when` at start |

## 7. Not supported, and known caveats

Not supported: history states; re-entry of a managed compound state; unrestricted cycles; multicast or multi-region
firing; edge-triggered joins (E5); busy timeout (E6); legacy nested-duplicate propagation (E7); engine-held
per-instance args; a cancel API separate from edges; cascade delete with the root. This is not a general statechart
claim.

Known caveats (documented, not fixed; from the independent engine review):

- **Already final at start (EF-1).** Completion is checked only after a write to the child. A managed child whose
  initial state already satisfies its `on_final`/`complete_when` predicate is therefore never completed, and its
  parent stays in `S`. Loaders must refuse such definitions. Likewise, `complete_when` must cover every terminal
  combination the child can reach. An uncovered one leaves the child active, and the parent waits.
- **Undecodable instance metadata (EF-2).** An `entities.instance` value that cannot be parsed is read as "not
  managed". Direct evaluate and the scheduler then treat that entity like a root; targeted delivery still refuses it.
  It is reachable only through storage corruption or a foreign writer.
- **Legacy child containing managed states.** A legacy (lazily created) child whose machine starts in a managed
  compound state does not start that grandchild; managed starts happen only at root creation, at entering writes and
  at a managed child's start. Use managed edges throughout a nested template.
- **Deleted roots.** Deleting a root entity leaves its child entities. Re-creating the same root id then hits
  `INSTANCE_CONFLICT` at its first managed entry.
- **Contention (E6).** A request or tick that loses the immediate write lock gets `500 database is locked` with
  nothing applied. Concurrent races are therefore mostly decided by lock order.
