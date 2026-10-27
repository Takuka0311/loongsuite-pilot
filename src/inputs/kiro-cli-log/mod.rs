//! Kiro CLI hook JSONL input. Port of `kiro-cli-log-input.ts`.
//!
//! The JSONL is written by the daemon's own delayed-collect subprocess, so a
//! cold start keeps only the last turn.

use std::sync::Arc;

use crate::checkpoints::StateStore;
use crate::inputs::rest_support::{HookInputOptions, HookJsonlInput};
use crate::types::ClientType;
use crate::utils::{directory_exists, resolve_home};

const DEFAULT_DIR: &str = "~/.loongsuite-pilot/logs/kiro-cli";

pub struct KiroCliLogInput {
    inner: HookJsonlInput,
}

impl KiroCliLogInput {
    pub fn new(opts: HookInputOptions) -> Self {
        Self {
            inner: HookJsonlInput::shared(
                "KiroCliLogInput",
                "kiro-cli-log",
                ClientType::KiroCli,
                DEFAULT_DIR,
                "kiro-cli",
                "kiro-cli",
                true,
                opts,
            ),
        }
    }

    pub fn with_store(state_store: Arc<StateStore>) -> Self {
        Self::new(HookInputOptions {
            state_store,
            log_dir: None,
            log_prefix: None,
            poll_interval_ms: None,
        })
    }

    pub async fn check_availability() -> bool {
        directory_exists(&resolve_home(DEFAULT_DIR)).await
    }

    pub fn watch_paths() -> Vec<String> {
        vec![resolve_home(DEFAULT_DIR)]
    }
}

impl std::ops::Deref for KiroCliLogInput {
    type Target = HookJsonlInput;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}
