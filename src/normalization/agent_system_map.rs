/// Collector agent type → product system. Unmapped types resolve to `unknown`.
pub const AGENT_SYSTEM_MAP: &[(&str, &str)] = &[
    ("claude-code", "claude"),
    ("codex", "codex"),
    ("codex-session", "codex"),
    ("qoder", "qoder"),
    ("qoder-idea", "qoder"),
    ("qoder-work", "qoder"),
    ("qoder-work-cn", "qoder"),
    ("qwen-work-cn", "qwen"),
    ("qoder-cli", "qoder"),
    ("qoder-cli-hook", "qoder"),
    ("cursor", "cursor"),
    ("cursor-hook", "cursor"),
    ("qwen-code-cli", "qwen-code"),
    ("opencode", "opencode"),
    ("mimo-code", "mimo-code"),
    ("pi-coding-agent", "pi"),
    ("grok-build", "grok"),
    ("hermes", "hermes"),
    ("wukong", "wukong"),
    ("workbuddy", "workbuddy"),
    ("dsh", "dsh"),
];

pub fn resolve_agent_system(agent_type: &str) -> &'static str {
    AGENT_SYSTEM_MAP
        .iter()
        .find(|(key, _)| *key == agent_type)
        .map(|(_, system)| *system)
        .unwrap_or("unknown")
}

#[cfg(test)]
fn agent_system_defined(agent_type: &str) -> bool {
    AGENT_SYSTEM_MAP.iter().any(|(key, _)| *key == agent_type)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_claude_code_to_claude() {
        assert_eq!(resolve_agent_system("claude-code"), "claude");
    }

    #[test]
    fn maps_codex_variants_to_codex() {
        assert_eq!(resolve_agent_system("codex"), "codex");
        assert_eq!(resolve_agent_system("codex-session"), "codex");
    }

    #[test]
    fn maps_qoder_variants_to_qoder() {
        assert_eq!(resolve_agent_system("qoder"), "qoder");
        assert_eq!(resolve_agent_system("qoder-idea"), "qoder");
        assert_eq!(resolve_agent_system("qoder-work"), "qoder");
        assert_eq!(resolve_agent_system("qoder-work-cn"), "qoder");
        assert_eq!(resolve_agent_system("qoder-cli"), "qoder");
        assert_eq!(resolve_agent_system("qoder-cli-hook"), "qoder");
    }

    #[test]
    fn maps_cursor_variants_to_cursor() {
        assert_eq!(resolve_agent_system("cursor"), "cursor");
        assert_eq!(resolve_agent_system("cursor-hook"), "cursor");
    }

    #[test]
    fn maps_grok_build_to_grok() {
        assert_eq!(resolve_agent_system("grok-build"), "grok");
    }

    #[test]
    fn maps_qwen_variants() {
        assert_eq!(resolve_agent_system("qwen-code-cli"), "qwen-code");
        assert_eq!(resolve_agent_system("qwen-work-cn"), "qwen");
    }

    #[test]
    fn maps_mimo_code_to_mimo_code() {
        assert_eq!(resolve_agent_system("mimo-code"), "mimo-code");
    }

    #[test]
    fn maps_pi_coding_agent_to_pi() {
        assert_eq!(resolve_agent_system("pi-coding-agent"), "pi");
    }

    #[test]
    fn maps_workbuddy_to_workbuddy() {
        assert_eq!(resolve_agent_system("workbuddy"), "workbuddy");
    }

    #[test]
    fn maps_dsh_to_dsh() {
        assert_eq!(resolve_agent_system("dsh"), "dsh");
    }

    #[test]
    fn maps_hermes_to_hermes() {
        assert_eq!(resolve_agent_system("hermes"), "hermes");
    }

    #[test]
    fn returns_unknown_for_unmapped_types() {
        assert_eq!(resolve_agent_system("some-future-agent"), "unknown");
        assert_eq!(resolve_agent_system(""), "unknown");
    }

    #[test]
    fn has_entries_for_all_expected_agent_types() {
        let expected_keys = [
            "claude-code",
            "codex",
            "codex-session",
            "qoder",
            "qoder-idea",
            "qoder-work",
            "qoder-work-cn",
            "qoder-cli",
            "qoder-cli-hook",
            "cursor",
            "cursor-hook",
            "grok-build",
            "qwen-code-cli",
            "qwen-work-cn",
            "mimo-code",
            "pi-coding-agent",
            "workbuddy",
            "hermes",
            "dsh",
        ];
        for key in expected_keys {
            assert!(
                agent_system_defined(key),
                "{key} missing from AGENT_SYSTEM_MAP"
            );
        }
    }
}
