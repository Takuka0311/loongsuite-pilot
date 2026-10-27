//! Three-tier agent admission control. Port of `agent-control-manager.ts`.
//!
//! Mode precedence: `"on"` force-enables, `"off"` force-disables, `"auto"`
//! (the default) returns `default_when_auto`.

use std::collections::HashMap;

use crate::types::{AgentControlConfig, AgentControlMode};
use crate::utils::{read_json_file, resolve_home, write_json_file};
use serde_json::Value;

use super::{fields, logger};

const DEFAULT_AGENT_CONTROL_PATH: &str = "~/.loongsuite-pilot/agent-control.json";

pub struct AgentControlManager {
    config: AgentControlConfig,
    file_path: String,
}

impl AgentControlManager {
    pub fn new(file_path: Option<String>) -> Self {
        Self {
            config: AgentControlConfig {
                version: 3,
                tools: HashMap::new(),
            },
            file_path: file_path.unwrap_or_else(|| resolve_home(DEFAULT_AGENT_CONTROL_PATH)),
        }
    }

    pub async fn load(&mut self) {
        let data = read_json_file::<Value>(&self.file_path).await;
        if let Some(Value::Object(map)) = data {
            if let Some(tools) = map.get("tools").and_then(Value::as_object) {
                let mut parsed = HashMap::new();
                for (id, mode) in tools {
                    if let Some(text) = mode.as_str() {
                        if let Ok(mode) = text.parse::<AgentControlMode>() {
                            parsed.insert(id.clone(), mode);
                        }
                    }
                }
                let version = map.get("version").and_then(Value::as_u64).unwrap_or(3) as u32;
                self.config = AgentControlConfig {
                    version,
                    tools: parsed,
                };
            }
        }
        let log = logger("AgentControlManager");
        log.info(
            "loaded agent-control config",
            Some(&fields(
                serde_json::json!({ "tools": self.config.tools.len() }),
            )),
        );
    }

    pub async fn save(&self) -> Result<(), crate::utils::AtomicWriteError> {
        write_json_file(&self.file_path, &self.config).await
    }

    pub fn resolve_enabled(&self, agent_id: &str, default_when_auto: bool) -> bool {
        match self.get_mode(agent_id) {
            AgentControlMode::On => true,
            AgentControlMode::Off => false,
            AgentControlMode::Auto => default_when_auto,
        }
    }

    pub fn get_mode(&self, agent_id: &str) -> AgentControlMode {
        self.config
            .tools
            .get(agent_id)
            .copied()
            .unwrap_or(AgentControlMode::Auto)
    }

    pub fn set_mode(&mut self, agent_id: impl Into<String>, mode: AgentControlMode) {
        self.config.tools.insert(agent_id.into(), mode);
    }

    pub fn get_all_modes(&self) -> HashMap<String, AgentControlMode> {
        self.config.tools.clone()
    }

    pub fn file_path(&self) -> &str {
        &self.file_path
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn auto_uses_the_default_and_on_off_override_it() {
        let mut manager = AgentControlManager::new(Some("/tmp/agent-control.json".into()));
        assert!(manager.resolve_enabled("qoder", true));
        assert!(!manager.resolve_enabled("qoder", false));
        manager.set_mode("qoder", AgentControlMode::Off);
        assert!(!manager.resolve_enabled("qoder", true));
        manager.set_mode("cursor", AgentControlMode::On);
        assert!(manager.resolve_enabled("cursor", false));
        assert_eq!(manager.get_all_modes().len(), 2);
    }

    #[tokio::test]
    async fn load_ignores_a_missing_file_and_round_trips_modes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("agent-control.json");
        let mut manager = AgentControlManager::new(Some(path.to_string_lossy().into_owned()));
        manager.load().await;
        assert_eq!(manager.get_mode("qoder"), AgentControlMode::Auto);
        manager.set_mode("qoder", AgentControlMode::Off);
        manager.save().await.unwrap();
        let mut again = AgentControlManager::new(Some(path.to_string_lossy().into_owned()));
        again.load().await;
        assert_eq!(again.get_mode("qoder"), AgentControlMode::Off);
    }
}
