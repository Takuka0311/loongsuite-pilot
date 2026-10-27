//! Marked YAML block in `$DSH_HOME/cordis.patch.yml`.

use crate::types::{
    AgentDefinition, DeployMode, DeployResult, DeployStrategy, DeployedAgentRecord,
    DshYamlPatchConfig,
};
use crate::utils::{create_logger, file_exists, resolve_home};
use async_trait::async_trait;
use sha2::{Digest, Sha256};
use std::path::Path;
use std::time::{Duration, Instant, SystemTime};

use super::dsh_runtime_locator::{DshRuntimeLocator, DshRuntimeTarget};
use super::paths::node_resolve;

pub const DSH_ENABLED_MARKER: &str = ".collection-enabled";
const BEGIN_PREFIX: &str = "# BEGIN ";
const END_PREFIX: &str = "# END ";
const LOCK_SUFFIX: &str = ".loongsuite-pilot.lock";
const LOCK_GATE_SUFFIX: &str = ".reclaim";
const LOCK_TIMEOUT: Duration = Duration::from_millis(2_000);
const LOCK_RETRY: Duration = Duration::from_millis(25);
const STALE_LOCK: Duration = Duration::from_secs(30);

fn logger() -> crate::utils::BoundLogger {
    create_logger("DshYamlPatchStrategy")
}

pub struct DshYamlPatchStrategy {
    data_dir: String,
    locator: DshRuntimeLocator,
}

impl DshYamlPatchStrategy {
    pub fn new(data_dir: String) -> Self {
        Self {
            data_dir,
            locator: DshRuntimeLocator::new(),
        }
    }

    pub fn with_locator(data_dir: String, locator: DshRuntimeLocator) -> Self {
        Self { data_dir, locator }
    }

    pub async fn resolve_target(
        &self,
        def: &AgentDefinition,
        record: Option<&DeployedAgentRecord>,
    ) -> Option<DshRuntimeTarget> {
        self.locator.locate(def, record).await.ok().flatten()
    }

    pub async fn detect_recorded(
        &self,
        def: &AgentDefinition,
        record: Option<&DeployedAgentRecord>,
    ) -> bool {
        self.resolve_target(def, record).await.is_some()
    }

    pub async fn needs_deploy_at(&self, def: &AgentDefinition, target: &DshRuntimeTarget) -> bool {
        let Some(cfg) = &def.dsh_yaml_patch else {
            return false;
        };
        let plugin_path = resolve_home(&cfg.plugin_source);
        let Some(plugin_hash) = sha256_file(&plugin_path).await else {
            return true;
        };
        if !file_exists(&enabled_marker(cfg)).await {
            return true;
        }
        let plugin_url = file_url(&plugin_path);
        let bytes = read_bytes(&target.patch_path).await.unwrap_or_default();
        let parsed = split_on_pilot_block(&bytes, &cfg.marker);
        let Some(block) = parsed.existing_block else {
            return true;
        };
        if parsed.conflict
            || !has_line(&block, &format!("# entryId={}", cfg.entry_id))
            || !has_line(&block, &format!("# pluginSource={plugin_url}"))
            || !has_line(&block, &format!("# pluginHash={plugin_hash}"))
        {
            return true;
        }
        false
    }

    pub async fn deploy_at(
        &self,
        def: &AgentDefinition,
        target: &DshRuntimeTarget,
    ) -> DeployResult {
        let Some(cfg) = &def.dsh_yaml_patch else {
            return fail(def, "missing dshYamlPatch config");
        };
        let plugin_path = resolve_home(&cfg.plugin_source);
        let Some(plugin_hash) = sha256_file(&plugin_path).await else {
            return fail(
                def,
                &format!("plugin file not found or unreadable: {plugin_path}"),
            );
        };
        let Some(token) = acquire_patch_lock(&target.patch_path).await else {
            return fail(def, "timed out waiting for DSH patch lock");
        };
        let result = async {
            for _attempt in 0..2 {
                let original = read_bytes(&target.patch_path).await?;
                let existed = !original.is_empty() || file_exists(&target.patch_path).await;
                let h0 = sha256_bytes(&original);
                let mode = resolve_mode(&target.patch_path, existed).await;
                let parsed = split_on_pilot_block(&original, &cfg.marker);
                if parsed.conflict || parsed.existing_block.as_ref().is_some_and(|block| !has_line(block, &format!("# entryId={}", cfg.entry_id))) {
                    return Err(format!("conflict: existing block with marker {} is not Pilot-managed (path/id mismatch)", cfg.marker));
                }
                let created = match parsed.existing_block.as_ref().and_then(|block| extract_created_flag(block)) {
                    Some(flag) => flag,
                    None => !existed,
                };
                let new_block = compose_block(cfg, &plugin_hash, created);
                let next = if parsed.existing_block.is_some() {
                    let mut bytes = parsed.before.clone();
                    bytes.extend(new_block.as_bytes());
                    bytes.extend(&parsed.after);
                    bytes
                } else {
                    let mut bytes = original.clone();
                    if !bytes.is_empty() && !bytes.ends_with(b"\n") {
                        bytes.push(b'\n');
                    }
                    bytes.extend(new_block.as_bytes());
                    bytes
                };
                if atomic_write_if_unchanged(&target.patch_path, &next, &h0, mode).await? {
                    create_enabled_marker(cfg).await?;
                    return Ok(());
                }
            }
            Err("concurrent modification detected; aborting after retry".into())
        }
        .await;
        release_patch_lock(&target.patch_path, &token).await;
        let _ = self.data_dir;
        match result {
            Ok(()) => DeployResult {
                success: true,
                agent_id: def.id.clone(),
                deploy_mode: DeployMode::DshYamlPatch,
                skipped: None,
                reason: None,
                error: None,
                plugin_inject_config_path: None,
            },
            Err(err) => fail(def, &err),
        }
    }

    pub async fn undeploy_recorded(
        &self,
        def: &AgentDefinition,
        record: Option<&DeployedAgentRecord>,
    ) -> bool {
        let Some(cfg) = &def.dsh_yaml_patch else {
            return false;
        };
        let Some(target) = self.resolve_target(def, record).await else {
            return false;
        };
        if remove_enabled_marker(cfg).await.is_err() {
            return false;
        }
        let initial = read_bytes(&target.patch_path).await.unwrap_or_default();
        if initial.is_empty() && !file_exists(&target.patch_path).await {
            return true;
        }
        let Some(token) = acquire_patch_lock(&target.patch_path).await else {
            return false;
        };
        let result: Result<bool, String> = async {
            for _ in 0..2 {
                let original = read_bytes(&target.patch_path).await?;
                if original.is_empty() && !file_exists(&target.patch_path).await {
                    return Ok(true);
                }
                let h0 = sha256_bytes(&original);
                let mode = resolve_mode(&target.patch_path, true).await;
                let parsed = split_on_pilot_block(&original, &cfg.marker);
                if parsed.conflict
                    || parsed.existing_block.as_ref().is_some_and(|block| {
                        !has_line(block, &format!("# entryId={}", cfg.entry_id))
                    })
                {
                    return Ok(false);
                }
                let Some(block) = parsed.existing_block else {
                    return Ok(true);
                };
                let mut next = parsed.before.clone();
                next.extend(&parsed.after);
                if extract_created_flag(&block) == Some(true)
                    && String::from_utf8_lossy(&next).trim().is_empty()
                {
                    if delete_if_unchanged(&target.patch_path, &h0).await? {
                        return Ok(true);
                    }
                    continue;
                }
                if atomic_write_if_unchanged(&target.patch_path, &next, &h0, mode).await? {
                    return Ok(true);
                }
            }
            Ok(false)
        }
        .await;
        release_patch_lock(&target.patch_path, &token).await;
        result.unwrap_or(false)
    }
}

struct Split {
    before: Vec<u8>,
    existing_block: Option<String>,
    after: Vec<u8>,
    conflict: bool,
}

fn compose_block(cfg: &DshYamlPatchConfig, plugin_hash: &str, created_file: bool) -> String {
    let plugin_url = file_url(&resolve_home(&cfg.plugin_source));
    let flag = format!(
        " (created-file: {})",
        if created_file { "true" } else { "false" }
    );
    [
        format!("{BEGIN_PREFIX}{}{flag}", cfg.marker),
        format!("# entryId={}", cfg.entry_id),
        format!("# pluginSource={plugin_url}"),
        format!("# pluginHash={plugin_hash}"),
        "- insert:".into(),
        "  - id: 'loongsuite-pilot-observability'".into(),
        format!("    name: {plugin_url}"),
        format!("{END_PREFIX}{}", cfg.marker),
        String::new(),
    ]
    .join("\n")
}

fn split_on_pilot_block(bytes: &[u8], marker: &str) -> Split {
    let begin_marker = format!("{BEGIN_PREFIX}{marker}");
    let end_marker = format!("{END_PREFIX}{marker}");
    let begins = find_line_markers(bytes, &begin_marker);
    let ends = find_line_markers(bytes, &end_marker);
    if begins.is_empty() && ends.is_empty() {
        return Split {
            before: bytes.to_vec(),
            existing_block: None,
            after: Vec::new(),
            conflict: false,
        };
    }
    if begins.len() != 1 || ends.len() != 1 || ends[0] <= begins[0] {
        return Split {
            before: bytes.to_vec(),
            existing_block: None,
            after: Vec::new(),
            conflict: true,
        };
    }
    let begin_idx = begins[0];
    let end_idx = ends[0];
    let begin_line_end = line_end(bytes, begin_idx);
    let end_line_end = line_end(bytes, end_idx);
    let begin_line = String::from_utf8_lossy(&bytes[begin_idx..begin_line_end])
        .trim_end_matches('\r')
        .to_string();
    let end_line = String::from_utf8_lossy(&bytes[end_idx..end_line_end])
        .trim_end_matches('\r')
        .to_string();
    let mut block_end = end_line_end;
    if bytes.get(block_end) == Some(&b'\n') {
        block_end += 1;
    }
    let block = String::from_utf8_lossy(&bytes[begin_idx..block_end]).into_owned();
    let valid_begin = begin_line == begin_marker
        || begin_line == format!("{begin_marker} (created-file: true)")
        || begin_line == format!("{begin_marker} (created-file: false)");
    let conflict = !valid_begin
        || end_line != end_marker
        || !has_prefix(&block, "# entryId=")
        || !has_prefix(&block, "# pluginSource=")
        || !has_prefix(&block, "# pluginHash=");
    Split {
        before: bytes[..begin_idx].to_vec(),
        existing_block: Some(block),
        after: bytes[block_end..].to_vec(),
        conflict,
    }
}

fn find_line_markers(bytes: &[u8], marker: &str) -> Vec<usize> {
    let needle = marker.as_bytes();
    let mut offsets = Vec::new();
    let mut from = 0;
    while from < bytes.len() {
        let Some(rel) = bytes[from..]
            .windows(needle.len())
            .position(|window| window == needle)
        else {
            break;
        };
        let index = from + rel;
        let at_line = index == 0 || bytes[index - 1] == b'\n';
        let next = index + needle.len();
        let boundary = next == bytes.len() || matches!(bytes[next], b' ' | b'\t' | b'\r' | b'\n');
        if at_line && boundary {
            offsets.push(index);
        }
        from = index + needle.len();
    }
    offsets
}

fn line_end(bytes: &[u8], start: usize) -> usize {
    bytes[start..]
        .iter()
        .position(|byte| *byte == b'\n')
        .map(|index| start + index)
        .unwrap_or(bytes.len())
}

fn has_prefix(block: &str, prefix: &str) -> bool {
    block
        .split(['\n', '\r'])
        .any(|line| line.starts_with(prefix) && line.len() > prefix.len())
}

fn has_line(block: &str, expected: &str) -> bool {
    block.split(['\n', '\r']).any(|line| line == expected)
}

fn extract_created_flag(block: &str) -> Option<bool> {
    if block.contains("created-file: true") {
        Some(true)
    } else if block.contains("created-file: false") {
        Some(false)
    } else {
        None
    }
}

fn file_url(path: &str) -> String {
    let absolute = node_resolve(path);
    let mut url = String::from("file://");
    for byte in absolute.as_bytes() {
        match *byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' | b'/' => {
                url.push(*byte as char)
            }
            other => url.push_str(&format!("%{other:02X}")),
        }
    }
    url
}

fn enabled_marker(cfg: &DshYamlPatchConfig) -> String {
    Path::new(&resolve_home(&cfg.plugin_source))
        .parent()
        .unwrap_or(Path::new("."))
        .join(DSH_ENABLED_MARKER)
        .to_string_lossy()
        .into_owned()
}

async fn read_bytes(path: &str) -> Result<Vec<u8>, String> {
    match tokio::fs::read(path).await {
        Ok(bytes) => Ok(bytes),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
        Err(err) => Err(err.to_string()),
    }
}

async fn resolve_mode(path: &str, existed: bool) -> u32 {
    if !existed {
        return 0o644;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if let Ok(meta) = tokio::fs::metadata(path).await {
            return meta.permissions().mode() & 0o777;
        }
    }
    0o644
}

async fn sha256_file(path: &str) -> Option<String> {
    let bytes = tokio::fs::read(path).await.ok()?;
    Some(sha256_bytes(&bytes))
}

fn sha256_bytes(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    format!("{:x}", hasher.finalize())
}

async fn atomic_write_if_unchanged(
    target: &str,
    next: &[u8],
    h0: &str,
    mode: u32,
) -> Result<bool, String> {
    if let Some(parent) = Path::new(target).parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .map_err(|err| err.to_string())?;
    }
    let current = read_bytes(target).await?;
    if sha256_bytes(&current) != h0 {
        return Ok(false);
    }
    let tmp = format!(
        "{target}.{}.{}.tmp",
        std::process::id(),
        uuid::Uuid::new_v4().simple()
    );
    {
        let mut options = std::fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(mode);
        }
        let mut file = options.open(&tmp).map_err(|err| err.to_string())?;
        use std::io::Write;
        file.write_all(next).map_err(|err| err.to_string())?;
        file.sync_all().map_err(|err| err.to_string())?;
    }
    let final_bytes = read_bytes(target).await?;
    if sha256_bytes(&final_bytes) != h0 {
        let _ = tokio::fs::remove_file(&tmp).await;
        return Ok(false);
    }
    tokio::fs::rename(&tmp, target)
        .await
        .map_err(|err| err.to_string())?;
    sync_dir(Path::new(target).parent().unwrap_or(Path::new("."))).await;
    let _ = tokio::fs::remove_file(&tmp).await;
    Ok(true)
}

async fn delete_if_unchanged(target: &str, h0: &str) -> Result<bool, String> {
    let current = read_bytes(target).await?;
    if sha256_bytes(&current) != h0 {
        return Ok(false);
    }
    let final_bytes = read_bytes(target).await?;
    if sha256_bytes(&final_bytes) != h0 {
        return Ok(false);
    }
    tokio::fs::remove_file(target)
        .await
        .map_err(|err| err.to_string())?;
    sync_dir(Path::new(target).parent().unwrap_or(Path::new("."))).await;
    Ok(true)
}

async fn create_enabled_marker(cfg: &DshYamlPatchConfig) -> Result<(), String> {
    let marker = enabled_marker(cfg);
    if let Some(parent) = Path::new(&marker).parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .map_err(|err| err.to_string())?;
    }
    let tmp = format!("{marker}.{}.tmp", std::process::id());
    tokio::fs::write(&tmp, "enabled\n")
        .await
        .map_err(|err| err.to_string())?;
    tokio::fs::rename(&tmp, &marker)
        .await
        .map_err(|err| err.to_string())?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(&marker, std::fs::Permissions::from_mode(0o600));
    }
    Ok(())
}

async fn remove_enabled_marker(cfg: &DshYamlPatchConfig) -> Result<(), String> {
    match tokio::fs::remove_file(enabled_marker(cfg)).await {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err.to_string()),
    }
}

async fn acquire_patch_lock(target: &str) -> Option<String> {
    let lock_path = format!("{target}{LOCK_SUFFIX}");
    let gate_path = format!("{lock_path}{LOCK_GATE_SUFFIX}");
    if let Some(parent) = Path::new(&lock_path).parent() {
        let _ = tokio::fs::create_dir_all(parent).await;
    }
    let token = format!("{}:{}", std::process::id(), uuid::Uuid::new_v4().simple());
    let deadline = Instant::now() + LOCK_TIMEOUT;
    while Instant::now() <= deadline {
        let gate = acquire_owner_file(&gate_path, deadline).await?;
        let acquired = match exclusive_token(&lock_path, &token).await {
            Ok(()) => true,
            Err(err) if err == "exists" => {
                if let Ok(meta) = tokio::fs::metadata(&lock_path).await {
                    if meta
                        .modified()
                        .ok()
                        .and_then(|time| SystemTime::now().duration_since(time).ok())
                        .is_some_and(|age| age > STALE_LOCK)
                    {
                        let _ = tokio::fs::remove_file(&lock_path).await;
                    }
                }
                false
            }
            Err(_) => false,
        };
        release_owner_file(&gate_path, &gate).await;
        if acquired {
            return Some(token);
        }
        tokio::time::sleep(LOCK_RETRY).await;
    }
    None
}

async fn acquire_owner_file(path: &str, deadline: Instant) -> Option<String> {
    let token = format!("{}:{}", std::process::id(), uuid::Uuid::new_v4().simple());
    while Instant::now() <= deadline {
        match exclusive_token(path, &token).await {
            Ok(()) => return Some(token),
            Err(err) if err == "exists" => tokio::time::sleep(LOCK_RETRY).await,
            Err(_) => return None,
        }
    }
    None
}

async fn exclusive_token(path: &str, token: &str) -> Result<(), String> {
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    match options.open(path) {
        Ok(mut file) => {
            use std::io::Write;
            file.write_all(token.as_bytes())
                .map_err(|err| err.to_string())?;
            file.sync_all().map_err(|err| err.to_string())
        }
        Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => Err("exists".into()),
        Err(err) => Err(err.to_string()),
    }
}

async fn release_patch_lock(target: &str, token: &str) {
    let lock_path = format!("{target}{LOCK_SUFFIX}");
    let gate_path = format!("{lock_path}{LOCK_GATE_SUFFIX}");
    let Some(gate) = acquire_owner_file(&gate_path, Instant::now() + LOCK_TIMEOUT).await else {
        logger().warn("failed to acquire DSH lock release gate", None);
        return;
    };
    if tokio::fs::read_to_string(&lock_path).await.ok().as_deref() == Some(token) {
        let _ = tokio::fs::remove_file(&lock_path).await;
    }
    release_owner_file(&gate_path, &gate).await;
}

async fn release_owner_file(path: &str, token: &str) {
    if tokio::fs::read_to_string(path).await.ok().as_deref() == Some(token) {
        let _ = tokio::fs::remove_file(path).await;
    }
}

async fn sync_dir(dir: &Path) {
    let _ = std::fs::File::open(dir).and_then(|file| file.sync_all());
}

fn fail(def: &AgentDefinition, error: &str) -> DeployResult {
    DeployResult {
        success: false,
        agent_id: def.id.clone(),
        deploy_mode: DeployMode::DshYamlPatch,
        skipped: None,
        reason: None,
        error: Some(error.to_string()),
        plugin_inject_config_path: None,
    }
}

#[async_trait]
impl DeployStrategy for DshYamlPatchStrategy {
    async fn detect(&self, def: &AgentDefinition) -> bool {
        self.detect_recorded(def, None).await
    }
    async fn needs_deploy(
        &self,
        def: &AgentDefinition,
        record: Option<&DeployedAgentRecord>,
    ) -> bool {
        if def.dsh_yaml_patch.is_none() {
            return false;
        }
        match self.resolve_target(def, record).await {
            Some(target) => self.needs_deploy_at(def, &target).await,
            None => true,
        }
    }
    async fn deploy(&self, def: &AgentDefinition) -> DeployResult {
        let Some(target) = self.resolve_target(def, None).await else {
            return fail(def, "DSH runtime target is unavailable");
        };
        self.deploy_at(def, &target).await
    }
    async fn undeploy(&self, def: &AgentDefinition) -> bool {
        self.undeploy_recorded(def, None).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{AgentDetectionConfig, DshYamlPatchConfig};

    #[tokio::test]
    async fn writes_and_removes_a_created_patch_file() {
        let dir = tempfile::tempdir().unwrap();
        let plugin = dir.path().join("plugin.mjs");
        std::fs::write(&plugin, "export {}\n").unwrap();
        let home = dir.path().join("dsh");
        std::fs::create_dir_all(&home).unwrap();
        let mut locator = DshRuntimeLocator::new();
        locator
            .env
            .insert("DSH_HOME".into(), home.to_string_lossy().into_owned());
        let strategy =
            DshYamlPatchStrategy::with_locator(dir.path().to_string_lossy().into_owned(), locator);
        let def = AgentDefinition {
            id: "dsh".into(),
            display_name: "DSH".into(),
            deploy_mode: DeployMode::DshYamlPatch,
            detection: AgentDetectionConfig {
                paths: vec![home.to_string_lossy().into_owned()],
                commands: vec![],
            },
            local_worker_runtime: None,
            hook: None,
            plugin_probe: None,
            plugin_inject: None,
            pi_sdk: None,
            directory_plugin: None,
            dsh_yaml_patch: Some(DshYamlPatchConfig {
                plugin_source: plugin.to_string_lossy().into_owned(),
                patch_path: None,
                entry_id: "loongsuite-pilot-observability".into(),
                marker: "PILOT-OBSERVABILITY-MANAGED".into(),
            }),
            input: None,
            runtime: None,
        };
        let result = strategy.deploy(&def).await;
        assert!(result.success, "{:?}", result.error);
        let patch = std::fs::read_to_string(home.join("cordis.patch.yml")).unwrap();
        assert!(patch.contains("# BEGIN PILOT-OBSERVABILITY-MANAGED (created-file: true)"));
        assert!(patch.contains("# entryId=loongsuite-pilot-observability"));
        assert!(patch.contains("- insert:"));
        assert!(
            home.join("..").join(".collection-enabled").exists()
                || plugin.parent().unwrap().join(DSH_ENABLED_MARKER).exists()
        );
        assert!(strategy.undeploy(&def).await);
        assert!(!home.join("cordis.patch.yml").exists());
    }
}
