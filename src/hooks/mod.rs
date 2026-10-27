//! Hook installation and removal.

mod claude_settings;
mod hook_manager;

pub use claude_settings::{
    legacy_claude_hook_paths, quote_claude_hook_path, with_claude_settings_lock,
};
pub use hook_manager::{HookDefinition, HookManager};
