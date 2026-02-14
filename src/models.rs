use serde::{Deserialize, Serialize};
use std::collections::HashMap;

pub const DEFAULT_REGION: &str = "_";

// ── Machine Definition ──────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MachineDefinition {
    pub machine_id: String,
    pub tenant_id: String,
    /// Flat FSM states (used when `regions` is empty)
    #[serde(default)]
    pub states: Vec<String>,
    /// Flat FSM initial state (used when `regions` is empty)
    #[serde(default)]
    pub initial_state: String,
    /// Parallel regions — if non-empty, this is a statechart machine
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub regions: Vec<RegionDef>,
    /// Join conditions across regions (barrier-style transitions)
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub joins: Vec<JoinDef>,
    pub transitions: Vec<TransitionDef>,
    #[serde(default)]
    pub actions: Vec<ActionDef>,
    /// Sub-machines for compound states (flat FSMs). Parallel machines use RegionDef.sub_machines.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub sub_machines: HashMap<String, SubMachineDef>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegionDef {
    pub id: String,
    pub states: Vec<String>,
    pub initial_state: String,
    /// Sub-machines: state_name → SubMachineDef (for compound states)
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub sub_machines: HashMap<String, SubMachineDef>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubMachineDef {
    /// Machine ID of the sub-machine (must exist in same tenant)
    pub machine_id: String,
    /// Maps sub-machine final state → parent region target state
    pub on_final: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JoinDef {
    /// Map of region_id → required state. All must be satisfied to fire.
    pub when: HashMap<String, String>,
    /// Region to transition when join fires
    pub target_region: String,
    /// State to set in target region
    pub target_state: String,
    /// Actions to dispatch when join fires
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub actions: Vec<ActionDef>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransitionDef {
    pub from: String,
    pub to: String,
    pub on: String,
    /// Region this transition operates in (None = default "_" for flat FSMs)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub region: Option<String>,
    #[serde(default)]
    pub guard: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActionDef {
    pub on_enter: String,
    /// Region scope for this action (None = matches any/default region)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub region: Option<String>,
    pub action: ActionConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum ActionConfig {
    #[serde(rename = "webhook")]
    Webhook { url: String },
    #[serde(rename = "event")]
    Event { event_type: String },
}

impl MachineDefinition {
    /// Returns true if this machine uses parallel regions (statechart).
    pub fn is_parallel(&self) -> bool {
        !self.regions.is_empty()
    }

    /// Build the initial state_map for a new entity.
    pub fn initial_state_map(&self) -> HashMap<String, String> {
        if self.is_parallel() {
            self.regions
                .iter()
                .map(|r| (r.id.clone(), r.initial_state.clone()))
                .collect()
        } else {
            let mut m = HashMap::new();
            m.insert(DEFAULT_REGION.to_string(), self.initial_state.clone());
            m
        }
    }

    /// Validate the machine definition for consistency.
    pub fn validate(&self) -> Result<(), String> {
        if self.machine_id.is_empty() {
            return Err("machine_id is required".into());
        }

        if self.is_parallel() {
            self.validate_parallel()
        } else {
            self.validate_flat()
        }
    }

    /// Look up a sub-machine definition for a state in a given region.
    pub fn sub_machine_for_state(&self, state: &str, region: &str) -> Option<&SubMachineDef> {
        if self.is_parallel() {
            self.regions
                .iter()
                .find(|r| r.id == region)
                .and_then(|r| r.sub_machines.get(state))
        } else {
            self.sub_machines.get(state)
        }
    }

    fn validate_flat(&self) -> Result<(), String> {
        if self.states.is_empty() {
            return Err("at least one state is required".into());
        }
        if !self.states.contains(&self.initial_state) {
            return Err(format!(
                "initial_state '{}' is not in states list",
                self.initial_state
            ));
        }
        for t in &self.transitions {
            if !self.states.contains(&t.from) {
                return Err(format!(
                    "transition 'from' state '{}' not in states",
                    t.from
                ));
            }
            if !self.states.contains(&t.to) {
                return Err(format!("transition 'to' state '{}' not in states", t.to));
            }
        }
        for a in &self.actions {
            if !self.states.contains(&a.on_enter) {
                return Err(format!(
                    "action on_enter state '{}' not in states",
                    a.on_enter
                ));
            }
        }
        // Validate sub-machine references
        for (state, sub) in &self.sub_machines {
            if !self.states.contains(state) {
                return Err(format!("sub_machine state '{}' not in states list", state));
            }
            if state == &self.initial_state {
                return Err(format!(
                    "initial_state '{}' cannot be a compound state (sub-machine)",
                    state
                ));
            }
            for (_final_state, target) in &sub.on_final {
                if !self.states.contains(target) {
                    return Err(format!(
                        "sub_machine on_final target '{}' not in states list",
                        target
                    ));
                }
            }
        }
        Ok(())
    }

    fn validate_parallel(&self) -> Result<(), String> {
        let region_map: HashMap<&str, &RegionDef> =
            self.regions.iter().map(|r| (r.id.as_str(), r)).collect();

        for r in &self.regions {
            if r.id.is_empty() {
                return Err("region id is required".into());
            }
            if r.states.is_empty() {
                return Err(format!("region '{}' must have at least one state", r.id));
            }
            if !r.states.contains(&r.initial_state) {
                return Err(format!(
                    "region '{}' initial_state '{}' not in its states",
                    r.id, r.initial_state
                ));
            }
        }

        for t in &self.transitions {
            let region_id = t.region.as_deref().ok_or_else(|| {
                format!(
                    "transition on '{}' must specify a region in parallel machine",
                    t.on
                )
            })?;
            let region = region_map.get(region_id).ok_or_else(|| {
                format!("transition references unknown region '{}'", region_id)
            })?;
            if !region.states.contains(&t.from) {
                return Err(format!(
                    "transition from '{}' not in region '{}'",
                    t.from, region_id
                ));
            }
            if !region.states.contains(&t.to) {
                return Err(format!(
                    "transition to '{}' not in region '{}'",
                    t.to, region_id
                ));
            }
        }

        for j in &self.joins {
            for (region_id, state) in &j.when {
                let region = region_map.get(region_id.as_str()).ok_or_else(|| {
                    format!("join references unknown region '{}'", region_id)
                })?;
                if !region.states.contains(state) {
                    return Err(format!(
                        "join references state '{}' not in region '{}'",
                        state, region_id
                    ));
                }
            }
            let target = region_map.get(j.target_region.as_str()).ok_or_else(|| {
                format!("join target region '{}' not defined", j.target_region)
            })?;
            if !target.states.contains(&j.target_state) {
                return Err(format!(
                    "join target state '{}' not in region '{}'",
                    j.target_state, j.target_region
                ));
            }
        }

        Ok(())
    }
}

// ── Entity ──────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Deserialize)]
pub struct Entity {
    pub machine_id: String,
    pub tenant_id: String,
    pub entity_id: String,
    /// For flat: plain state string. For parallel: JSON-encoded state map.
    pub current_state: String,
    #[serde(default)]
    pub context: HashMap<String, serde_json::Value>,
    pub state_version: i64,
    pub created_at: i64,
    pub updated_at: i64,
}

impl Entity {
    /// Parse current_state into a region → state map.
    /// Flat: {"_": "active"}. Parallel: {"payment": "captured", ...}.
    pub fn state_map(&self) -> HashMap<String, String> {
        if self.current_state.starts_with('{') {
            serde_json::from_str(&self.current_state).unwrap_or_else(|_| {
                let mut m = HashMap::new();
                m.insert(DEFAULT_REGION.to_string(), self.current_state.clone());
                m
            })
        } else {
            let mut m = HashMap::new();
            m.insert(DEFAULT_REGION.to_string(), self.current_state.clone());
            m
        }
    }

    /// Get state for a specific region.
    pub fn region_state(&self, region: &str) -> Option<String> {
        self.state_map().get(region).cloned()
    }

    /// Check if this entity uses parallel state.
    pub fn is_parallel(&self) -> bool {
        self.current_state.starts_with('{')
    }

    /// Encode a state map back to a current_state string.
    /// Flat (single "_" region): returns plain string.
    /// Parallel: returns JSON object.
    pub fn encode_state(map: &HashMap<String, String>) -> String {
        if map.len() == 1 {
            if let Some(state) = map.get(DEFAULT_REGION) {
                return state.clone();
            }
        }
        serde_json::to_string(map).unwrap_or_default()
    }

    /// Convert to API response with proper JSON typing for current_state.
    pub fn to_response(&self) -> EntityResponse {
        let current_state = if self.is_parallel() {
            serde_json::from_str(&self.current_state)
                .unwrap_or(serde_json::Value::String(self.current_state.clone()))
        } else {
            serde_json::Value::String(self.current_state.clone())
        };
        EntityResponse {
            machine_id: self.machine_id.clone(),
            tenant_id: self.tenant_id.clone(),
            entity_id: self.entity_id.clone(),
            current_state,
            context: self.context.clone(),
            state_version: self.state_version,
            created_at: self.created_at,
            updated_at: self.updated_at,
        }
    }
}

/// Convert a state_map to a serde_json::Value for API responses.
pub fn state_map_to_value(map: &HashMap<String, String>) -> serde_json::Value {
    if map.len() == 1 {
        if let Some(state) = map.get(DEFAULT_REGION) {
            return serde_json::Value::String(state.clone());
        }
    }
    serde_json::to_value(map).unwrap_or(serde_json::Value::Null)
}

// ── Entity Response (API serialization) ─────────────────────────────────────

#[derive(Debug, Clone, Serialize)]
pub struct EntityResponse {
    pub machine_id: String,
    pub tenant_id: String,
    pub entity_id: String,
    /// String for flat, Object for parallel
    pub current_state: serde_json::Value,
    #[serde(default)]
    pub context: HashMap<String, serde_json::Value>,
    pub state_version: i64,
    pub created_at: i64,
    pub updated_at: i64,
}

// ── Transition Record (audit log) ───────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransitionRecord {
    pub id: i64,
    pub tenant_id: String,
    pub machine_id: String,
    pub entity_id: String,
    pub from_state: String,
    pub to_state: String,
    pub event_type: String,
    #[serde(default)]
    pub event_params: Option<HashMap<String, String>>,
    #[serde(default)]
    pub actions_dispatched: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub region: Option<String>,
    pub timestamp: i64,
    pub created_at: i64,
}

// ── API Request / Response Types ────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct CreateEntityRequest {
    pub entity_id: String,
    #[serde(default)]
    pub context: Option<HashMap<String, serde_json::Value>>,
}

#[derive(Debug, Deserialize)]
pub struct TransitionRequest {
    pub event_type: String,
    #[serde(default)]
    pub params: HashMap<String, String>,
    pub timestamp: Option<i64>,
}

#[derive(Debug, Serialize)]
pub struct TransitionResponse {
    pub entity_id: String,
    pub previous_state: serde_json::Value,
    pub current_state: serde_json::Value,
    pub transition: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub region: Option<String>,
    pub triggered_by: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub actions_dispatched: Vec<DispatchedAction>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub joins_fired: Vec<JoinFired>,
    pub timestamp: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sub_machine: Option<SubMachineTransition>,
}

/// Sub-machine transition detail returned when an event is forwarded to a child machine.
#[derive(Debug, Clone, Serialize)]
pub struct SubMachineTransition {
    pub machine_id: String,
    pub entity_id: String,
    pub previous_state: serde_json::Value,
    pub current_state: serde_json::Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transition: Option<String>,
    /// True when the child reached a final state and the parent auto-advanced.
    pub auto_completed: bool,
}

/// Structured action object returned in transition responses.
/// Plugin-runtime reads `action_type` to dispatch without string parsing.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "action_type")]
pub enum DispatchedAction {
    #[serde(rename = "webhook")]
    Webhook {
        url: String,
        state: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        region: Option<String>,
    },
    #[serde(rename = "event")]
    Event {
        event_type: String,
        state: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        region: Option<String>,
    },
}

#[derive(Debug, Clone, Serialize)]
pub struct JoinFired {
    pub target_region: String,
    pub from_state: String,
    pub target_state: String,
}

#[derive(Debug, Deserialize)]
pub struct EvaluateRequest {
    pub event_type: String,
    pub entity_key: String,
    #[serde(default)]
    pub params: HashMap<String, String>,
    pub timestamp: Option<i64>,
    /// When false, actions are returned in the response but NOT dispatched server-side.
    /// Plugin-runtime sets this to false and executes actions itself.
    /// Default: true (backward compatible — server dispatches actions).
    #[serde(default = "default_true")]
    pub dispatch: bool,
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Deserialize)]
pub struct ListEntitiesQuery {
    pub state: Option<String>,
    /// Filter by region state (for parallel machines)
    pub region: Option<String>,
    pub updated_since: Option<i64>,
}

// ── Machine CRUD request (without tenant_id — comes from header) ────────────

#[derive(Debug, Deserialize)]
pub struct CreateMachineRequest {
    pub machine_id: String,
    #[serde(default)]
    pub states: Vec<String>,
    #[serde(default)]
    pub initial_state: String,
    #[serde(default)]
    pub regions: Vec<RegionDef>,
    #[serde(default)]
    pub joins: Vec<JoinDef>,
    pub transitions: Vec<TransitionDef>,
    #[serde(default)]
    pub actions: Vec<ActionDef>,
    #[serde(default)]
    pub sub_machines: HashMap<String, SubMachineDef>,
}

#[derive(Debug, Deserialize)]
pub struct UpdateMachineRequest {
    #[serde(default)]
    pub states: Vec<String>,
    #[serde(default)]
    pub initial_state: String,
    #[serde(default)]
    pub regions: Vec<RegionDef>,
    #[serde(default)]
    pub joins: Vec<JoinDef>,
    pub transitions: Vec<TransitionDef>,
    #[serde(default)]
    pub actions: Vec<ActionDef>,
    #[serde(default)]
    pub sub_machines: HashMap<String, SubMachineDef>,
}
