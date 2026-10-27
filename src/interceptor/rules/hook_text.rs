//! Flatten a hook request into the text the sensitive-data rules scan.

use crate::interceptor::types::HookRequest;
use serde_json::Value;
use std::collections::HashSet;

const MAX_LEAF_DEPTH: usize = 32;

pub fn collect_hook_text(request: &HookRequest) -> String {
    let mut parts = Vec::new();
    if let Some(prompt) = request.prompt.as_deref() {
        if !prompt.is_empty() {
            parts.push(prompt.to_string());
        }
    }
    if let Some(tool_name) = request.tool_name.as_deref() {
        if !tool_name.is_empty() {
            parts.push(tool_name.to_string());
        }
    }
    if let Some(input) = &request.tool_input {
        parts.extend(collect_text_leaves(input, &mut HashSet::new(), 0));
    }
    if let Some(response) = &request.tool_response {
        parts.extend(collect_text_leaves(response, &mut HashSet::new(), 0));
    }
    parts.join("\n")
}

fn collect_text_leaves(value: &Value, seen: &mut HashSet<usize>, depth: usize) -> Vec<String> {
    if value.is_null() || depth > MAX_LEAF_DEPTH {
        return Vec::new();
    }
    match value {
        Value::String(text) => {
            if text.is_empty() {
                Vec::new()
            } else {
                vec![text.clone()]
            }
        }
        Value::Number(number) => vec![number.to_string()],
        Value::Bool(flag) => vec![flag.to_string()],
        Value::Array(items) => {
            let id = items.as_ptr() as usize;
            if !seen.insert(id) {
                return Vec::new();
            }
            items
                .iter()
                .flat_map(|item| collect_text_leaves(item, seen, depth + 1))
                .collect()
        }
        Value::Object(map) => {
            let id = map as *const _ as usize;
            if !seen.insert(id) {
                return Vec::new();
            }
            map.values()
                .flat_map(|item| collect_text_leaves(item, seen, depth + 1))
                .collect()
        }
        Value::Null => Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::interceptor::types::{HookEventName, HookRequest, InterceptorAgent};
    use serde_json::{json, Map, Value};

    fn request(
        prompt: Option<&str>,
        tool_name: Option<&str>,
        input: Option<Value>,
        response: Option<Value>,
    ) -> HookRequest {
        HookRequest {
            agent: InterceptorAgent::Qoder,
            event: HookEventName::UserPromptSubmit,
            session_id: None,
            transcript_path: None,
            cwd: None,
            tool_name: tool_name.map(str::to_string),
            tool_input: input,
            tool_response: response,
            tool_use_id: None,
            prompt: prompt.map(str::to_string),
            raw: Map::new(),
        }
    }

    #[test]
    fn joins_prompt_tool_name_and_nested_string_leaves() {
        assert_eq!(
            collect_hook_text(&request(
                Some("hello"),
                Some("Bash"),
                Some(json!({"command": "ls"})),
                Some(json!({"stdout": "ok"}))
            )),
            "hello\nBash\nls\nok"
        );
    }

    #[test]
    fn keeps_real_newlines_inside_nested_file_content() {
        assert_eq!(
            collect_hook_text(&request(
                None,
                Some("Read"),
                None,
                Some(
                    json!({"type": "text", "file": {"content": "intro\nmysql://agent:eMCyjl4XWcVzpXFb@127.0.0.1:3306/pilot"}})
                )
            )),
            "Read\ntext\nintro\nmysql://agent:eMCyjl4XWcVzpXFb@127.0.0.1:3306/pilot"
        );
    }
}
