//! Native uninstall coordination. Reuse each deployment strategy's ownership,
//! locking and rollback rules before an installer removes runtime assets.
use anyhow::{ensure, Context, Result};
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::HashSet;
use std::path::{Path, PathBuf};

use super::{DeploymentManager, DeploymentManagerOptions};
use crate::types::{AgentDefinition, DeployMode};
use crate::utils::runtime_paths::{
    build_edition, build_target, resolve_resource_root, validate_runtime,
};

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Options {
    data_dir: String,
    pilot_dir: Option<String>,
    #[serde(default)]
    check_only: bool,
}

fn present(path: &Path) -> Result<bool> {
    match std::fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(_) => {
            anyhow::bail!("Cannot inspect an uninstall target; runtime assets were preserved")
        }
    }
}

fn configured_path(path: &str) -> PathBuf {
    PathBuf::from(crate::utils::resolve_home(path))
}

async fn selected(manager: &DeploymentManager, def: &mut AgentDefinition) -> Result<bool> {
    let recorded = manager.has_record(&def.id);
    match def.deploy_mode {
        DeployMode::DetectionOnly => Ok(false),
        DeployMode::Hook => {
            let hook = def.hook.as_ref().context("Hook definition is incomplete")?;
            let mut paths = manager.recorded_settings_paths(&def.id);
            paths.push(hook.settings_path.clone());
            if let Some(trust) = &hook.trust_toml {
                paths.push(trust.config_path.clone());
            }
            for path in paths {
                if present(&configured_path(&path))? {
                    return Ok(true);
                }
            }
            Ok(recorded)
        }
        DeployMode::PluginInject => Ok(recorded
            || manager
                .plugin_config_path(def)
                .await
                .map_err(|_| {
                    anyhow::anyhow!(
                        "Cannot inspect plugin configuration; runtime assets were preserved"
                    )
                })?
                .is_some()),
        DeployMode::DirectoryPlugin => {
            let config = def
                .directory_plugin
                .as_ref()
                .context("Directory plugin definition is incomplete")?;
            Ok(recorded || present(&configured_path(&config.target_dir))?)
        }
        DeployMode::PluginProbe => {
            let config = def
                .plugin_probe
                .as_ref()
                .context("Plugin probe definition is incomplete")?;
            Ok(recorded || present(&configured_path(&config.source.dest_dir))?)
        }
        DeployMode::DshYamlPatch => {
            let config = def
                .dsh_yaml_patch
                .as_mut()
                .context("DSH definition is incomplete")?;
            // Match the old uninstall fallback, but let persisted deployment
            // state take priority inside undeploy_agent/undeploy_recorded.
            let home = std::env::var("DSH_HOME")
                .ok()
                .filter(|value| !value.trim().is_empty())
                .unwrap_or_else(|| "~/.dsh".into());
            let fallback = configured_path(&home).join("cordis.patch.yml");
            let patch = config
                .patch_path
                .as_ref()
                .map(|path| configured_path(path))
                .unwrap_or(fallback);
            let enabled = configured_path(&config.plugin_source)
                .parent()
                .map(|parent| parent.join(".collection-enabled"));
            let selected = recorded
                || present(&patch)?
                || enabled
                    .as_ref()
                    .map(|path| present(path))
                    .transpose()?
                    .unwrap_or(false);
            if config.patch_path.is_none() {
                config.patch_path = Some(patch.to_string_lossy().into_owned());
            }
            Ok(selected)
        }
    }
}

pub(super) async fn run(value: Value) -> Result<String> {
    let options: Options = serde_json::from_value(value)
        .map_err(|_| anyhow::anyhow!("Invalid native uninstall options"))?;
    ensure!(
        Path::new(&options.data_dir).is_absolute(),
        "dataDir must be absolute"
    );
    if let Some(root) = &options.pilot_dir {
        ensure!(Path::new(root).is_absolute(), "pilotDir must be absolute");
    }
    let root = resolve_resource_root(
        &options.data_dir,
        options.pilot_dir.as_deref(),
        &[env!("CARGO_MANIFEST_DIR")],
    )?;
    if !root.join("Cargo.toml").is_file() || !root.join("src/main.rs").is_file() {
        validate_runtime(
            &root,
            env!("CARGO_PKG_VERSION"),
            build_target(),
            build_edition(),
        )
        .context("Uninstall runtime integrity check failed")?;
    }
    let manager = DeploymentManager::new(DeploymentManagerOptions {
        data_dir: options.data_dir,
        pilot_dir: root.to_string_lossy().into_owned(),
        builtin_agents_dir: None,
        installed_by: None,
    })
    .preserving_runtime_assets();
    manager.validate_state().await.map_err(|_| {
        anyhow::anyhow!(
            "Deployment state is invalid; configuration and runtime assets were preserved"
        )
    })?;
    let definitions = manager.load_definitions().await;
    ensure!(
        !definitions.is_empty(),
        "No valid Agent definitions; runtime assets were preserved"
    );
    let known: HashSet<_> = definitions.iter().map(|def| def.id.as_str()).collect();
    ensure!(
        manager
            .recorded_agent_ids()
            .iter()
            .all(|id| known.contains(id.as_str())),
        "Deployment state contains an unknown Agent; runtime assets were preserved"
    );
    let mut targets = Vec::new();
    for mut def in definitions {
        if selected(&manager, &mut def).await? {
            targets.push(def);
        }
    }
    // Refuse conflicting DSH YAML before removing other Agent configuration.
    targets.sort_by_key(|def| (def.deploy_mode != DeployMode::DshYamlPatch, def.id.clone()));
    let agents: Vec<_> = targets.iter().map(|def| def.id.clone()).collect();
    if !options.check_only {
        for target in &targets {
            ensure!(
                manager.undeploy_agent(target).await,
                "Native cleanup failed for Agent {}; runtime assets were preserved",
                target.id
            );
        }
    }
    Ok(json!({"agents":agents,"checkOnly":options.check_only,"ok":true}).to_string())
}
