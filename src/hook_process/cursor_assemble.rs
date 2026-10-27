//! Cursor turn assembly. Port of `assets/hooks/cursor/react-assembler.mjs`,
//! `transcript-assembler.mjs`, and `skill-detector.mjs`, plus the processor's
//! skill-read injection.
//!
//! Windows transcript recovery runs when the host is Windows, when the hook
//! environment names Windows, or when the transcript path is a Windows path.
//! The deferred `afterAgentResponse` path still omits the explicit transcript
//! argument, matching the JavaScript processor.

use super::super::common::{finite_f64, json_number, sanitize_record, Env, Obj};
use chrono::{DateTime, SecondsFormat, Utc};
use regex::Regex;
use serde_json::{json, Map, Value};
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::LazyLock;

static SKILL_DOCUMENT_PATH_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r#"(?i)(?:^|[/\\])skills(?:-[A-Za-z0-9][A-Za-z0-9_-]*)?[/\\](?:\.system[/\\])?([^/\\"'\s]+)[/\\]SKILL\.md$"#,
    )
    .expect("skill document path")
});
static MANUALLY_ATTACHED_SKILLS_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?is)<manually_attached_skills>(.*?)</manually_attached_skills>")
        .expect("manual skills")
});
static ATTACHED_SKILL_HEADER_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"(?:^|\r?\n)Skill Name:\s*([^\r\n]+)\r?\nPath:\s*([^\r\n]+)\r?\nSKILL\.md content:\s*(?:\r?\n|$)",
    )
    .expect("skill header")
});
static USER_QUERY_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?is)<user_query>\s*(.*?)\s*</user_query>").expect("user query"));
static USER_QUERY_TAG_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"</?user_query>\n?").expect("user query tag"));
static USER_QUERY_CONTAINER_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?is)<user_query\b[^>]*>.*?</user_query>").expect("user query container")
});
static MANUALLY_ATTACHED_CONTAINER_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?is)<manually_attached_skills\b[^>]*>.*?</manually_attached_skills>")
        .expect("manual container")
});
static AGENT_SKILLS_CONTAINER_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?is)<agent_skills\b[^>]*>(.*?)</agent_skills>").expect("agent skills")
});
static AVAILABLE_SKILLS_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?is)<available_skills\b[^>]*>(.*?)</available_skills>").expect("available skills")
});
static AGENT_SKILL_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?is)<agent_skill\b([^>]*)>.*?</agent_skill>").expect("agent skill")
});
static AGENT_SKILL_FULL_PATH_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r#"(?i)\bfullPath\s*=\s*(?:"([^"]+)"|'([^']+)')"#).expect("skill full path")
});
static TAG_RE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"<[^>]+>").expect("tag"));
static WHITESPACE_RE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"\s+").expect("whitespace"));
static THOUGHT_SUFFIX_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)^(.+)-(\d+)-([a-z0-9]{4,})$").expect("thought suffix"));
static WINDOWS_JSON_COMMA_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r#""?\?,"#).expect("windows comma"));
static WINDOWS_JSON_OBJECT_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r#""?\?}"#).expect("windows object"));
static WINDOWS_JSON_ARRAY_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r#""?\?]"#).expect("windows array"));

pub(super) struct TurnOptions<'a> {
    pub user_id: &'a str,
    pub variant: &'a str,
    pub stop_conversation_id: &'a str,
    pub transcript_path: Option<&'a str>,
    pub windows: bool,
}

#[derive(Clone)]
struct SkillHit {
    skill_name: String,
    skill_path: String,
    detection_source: String,
    detection_sources: Vec<String>,
}

#[derive(Clone)]
struct SkillPath {
    skill_name: String,
    skill_path: String,
}

#[derive(Clone)]
struct SubagentResult {
    result_text: String,
    duration_ms: i64,
    end_ts: String,
    tool_name: String,
}

#[derive(Clone)]
struct ToolCallMeta {
    tool_name: String,
    tool_use_id: Option<String>,
    tool_input: Option<Value>,
    observed_at: String,
}

#[derive(Clone)]
struct ToolResultMeta {
    tool_use_id: Option<String>,
    result: Option<Value>,
    error: Option<String>,
    end_ts: String,
}

#[derive(Clone)]
enum Stamp {
    Iso(String),
    EpochMs(f64),
}

struct Timed {
    stamp: Stamp,
    source: Option<String>,
}

struct TranscriptStep {
    text: Option<String>,
    tool_calls: Vec<Value>,
    tool_results: Vec<Value>,
    thought_event: Option<Value>,
    response_event: Option<Value>,
}

struct StepBuilder {
    events: Vec<Value>,
    turn_id: String,
    model: String,
    base: Obj,
    user_prompt: Option<String>,
    step_prefix: Option<String>,
    subagent_results: HashMap<String, SubagentResult>,
    records: Vec<Obj>,
    cumulative: Vec<Value>,
    step_round: u32,
    current_step_id: Option<String>,
    current_step_has_tools: bool,
    current_response: Option<Obj>,
    previous_assistant: Option<Value>,
    previous_tool_results: Vec<ToolResultMeta>,
    last_step_end_ts: Option<String>,
    pending_records: Vec<Obj>,
    pending_calls: Vec<ToolCallMeta>,
    pending_results: Vec<ToolResultMeta>,
    step_tool_calls: HashMap<String, Vec<ToolCallMeta>>,
    active_sync: HashMap<String, usize>,
    synthesized: HashSet<String>,
}

pub(super) fn windows_context(env: &Env, path_hint: &str) -> bool {
    cfg!(windows) || env_says_windows(env) || path_says_windows(path_hint)
}

pub(super) fn repaired_windows_object(raw: &str, env: &Env) -> Option<Obj> {
    if !windows_context(env, raw) {
        return None;
    }
    let repaired = repair_windows_json(raw);
    if repaired == raw {
        return None;
    }
    match serde_json::from_str::<Value>(repaired.trim()) {
        Ok(Value::Object(map)) => Some(map),
        _ => None,
    }
}

pub(super) fn assemble_parent_turn(
    events: &[Value],
    options: TurnOptions<'_>,
) -> (Vec<Obj>, HashSet<String>) {
    let explicit_transcript = options
        .transcript_path
        .map(str::trim)
        .filter(|path| !path.is_empty() && *path != "None");
    let prompt = if options.stop_conversation_id.is_empty() {
        events
            .iter()
            .find(|event| hook_name(event) == "beforeSubmitPrompt")
    } else {
        events.iter().find(|event| {
            hook_name(event) == "beforeSubmitPrompt"
                && conversation_of(event) == options.stop_conversation_id
        })
    };
    let Some(prompt) = prompt else {
        return (Vec::new(), HashSet::new());
    };

    let mut transcript_user_prompt = None;
    if options.windows {
        if let Some(path) = explicit_transcript {
            transcript_user_prompt = recover_windows_user_prompt(path);
        }
    }

    let parent_conv_id = conversation_of(prompt).to_string();
    let turn_id = event_str(prompt, "generation_id")
        .unwrap_or(parent_conv_id.as_str())
        .to_string();
    let trace_id = derive_trace_id(&turn_id);
    let stop_event = events
        .iter()
        .find(|event| hook_name(event) == "stop" && conversation_of(event) == parent_conv_id);
    let response_event = events.iter().find(|event| {
        hook_name(event) == "afterAgentResponse" && conversation_of(event) == parent_conv_id
    });
    let model = resolve_model(
        response_event
            .and_then(|event| event_str(event, "model"))
            .or_else(|| event_str(prompt, "model")),
    );
    let mut parent_events: Vec<&Value> = events
        .iter()
        .filter(|event| conversation_of(event) == parent_conv_id)
        .filter(|event| hook_name(event) != "sessionStart")
        .collect();
    parent_events.sort_by_key(|event| ts_ms_sort(event));
    let workspace = resolve_workspace_path_refs(&parent_events);
    let base = base_fields(
        &trace_id,
        &parent_conv_id,
        &turn_id,
        options.variant,
        options.user_id,
        workspace.as_deref(),
        false,
    );
    let user_prompt =
        transcript_user_prompt.or_else(|| event_str(prompt, "prompt").map(str::to_owned));

    let mut records = Vec::new();
    if let Some(prompt_text) = user_prompt.as_deref().filter(|text| !text.is_empty()) {
        let nanos = event_ts_nanos(prompt);
        let mut fields = timed_identity(&nanos, "other");
        fields.insert(
            "gen_ai.input.messages_delta".into(),
            json!([user_text_message(prompt_text)]),
        );
        fields.insert(
            "agent.cursor.hook_event_name".into(),
            Value::String("beforeSubmitPrompt".into()),
        );
        if let Some(mode) = event_str(prompt, "composer_mode") {
            fields.insert(
                "agent.cursor.composer_mode".into(),
                Value::String(mode.to_string()),
            );
        }
        records.push(finish_record(&base, fields));
    }

    let scan_path = explicit_transcript.or_else(|| {
        stop_event
            .and_then(|event| event_str(event, "transcript_path").filter(|path| *path != "None"))
    });
    let child_conv_ids = scan_path.map(scan_subagent_dir).unwrap_or_default();
    let mut parent_subagent_calls: Vec<&Value> = parent_events
        .iter()
        .copied()
        .filter(|event| {
            hook_name(event) == "preToolUse" && is_subagent_tool(event_str(event, "tool_name"))
        })
        .collect();
    parent_subagent_calls.sort_by_key(|event| ts_ms_sort(event));

    let mut child_sessions = Vec::new();
    for child_conv_id in &child_conv_ids {
        let mut child_events: Vec<&Value> = events
            .iter()
            .filter(|event| conversation_of(event) == child_conv_id)
            .filter(|event| hook_name(event) != "sessionStart")
            .collect();
        if child_events.is_empty() {
            continue;
        }
        child_events.sort_by_key(|event| ts_ms_sort(event));
        child_sessions.push((child_conv_id.clone(), child_events));
    }
    child_sessions.sort_by_key(|(_, child_events)| ts_ms_sort(child_events[0]));

    let mut assigned = HashSet::new();
    let mut child_links = Vec::new();
    for (child_conv_id, child_events) in child_sessions {
        let child_first = ts_ms_sort(child_events[0]);
        let mut matched = None;
        for call in &parent_subagent_calls {
            let call_id = event_str(call, "tool_use_id").unwrap_or("");
            if assigned.contains(call_id) {
                continue;
            }
            if ts_ms_sort(call) <= child_first {
                matched = Some(*call);
            }
        }
        if let Some(call) = matched {
            let call_id = event_str(call, "tool_use_id").unwrap_or("").to_string();
            assigned.insert(call_id);
            child_links.push((child_conv_id, child_events, Some(call)));
        } else {
            child_links.push((child_conv_id, child_events, None));
        }
    }

    let mut subagent_results = HashMap::new();
    for (_, child_events, parent_call) in &child_links {
        let Some(parent_call) = parent_call.as_ref() else {
            continue;
        };
        let Some(parent_tool_call_id) = event_str(parent_call, "tool_use_id") else {
            continue;
        };
        let child_last = child_events[child_events.len() - 1];
        let child_last_response = child_events
            .iter()
            .rev()
            .find(|event| matches!(hook_name(event), "afterAgentResponse" | "afterAgentThought"));
        let result_text = child_last_response
            .and_then(|event| event_str(event, "text"))
            .unwrap_or("")
            .to_string();
        let duration_ms = ts_ms_sort(child_last).saturating_sub(ts_ms_sort(child_events[0]));
        subagent_results.insert(
            parent_tool_call_id.to_string(),
            SubagentResult {
                result_text,
                duration_ms,
                end_ts: event_str(child_last, "_journal_ts")
                    .unwrap_or("")
                    .to_string(),
                tool_name: event_str(parent_call, "tool_name")
                    .unwrap_or("Subagent")
                    .to_string(),
            },
        );
    }

    let parent_owned: Vec<Value> = parent_events.iter().map(|event| (*event).clone()).collect();
    records.extend(build_parent_steps(
        &parent_owned,
        &turn_id,
        &model,
        &base,
        ParentStepOptions {
            user_prompt: user_prompt.clone(),
            prompt_event_ts: event_str(prompt, "_journal_ts").map(str::to_owned),
            step_prefix: None,
            subagent_results,
        },
    ));

    for (child_conv_id, child_events, parent_call) in &child_links {
        let child_short: String = child_conv_id.chars().take(8).collect();
        let child_owned: Vec<Value> = child_events.iter().map(|event| (*event).clone()).collect();
        let child_workspace = resolve_workspace_path(&child_owned).or(workspace.clone());
        let parent_tool_call_id = parent_call
            .as_ref()
            .and_then(|call| event_str(call, "tool_use_id"));
        let mut child_base = base.clone();
        if let Some(cwd) = child_workspace.as_deref() {
            child_base.insert(
                format!("agent.{}.cwd", options.variant),
                Value::String(cwd.to_string()),
            );
        }
        child_base.insert(
            "gen_ai.agent.scope".into(),
            Value::String("subagent".into()),
        );
        child_base.insert("gen_ai.agent.depth".into(), Value::from(1));
        child_base.insert(
            "gen_ai.agent.id".into(),
            Value::String(child_conv_id.clone()),
        );
        child_base.insert(
            "gen_ai.agent.parent.id".into(),
            Value::String(parent_conv_id.clone()),
        );
        if let Some(parent_tool_call_id) = parent_tool_call_id {
            child_base.insert(
                "gen_ai.subagent.parent_tool_call.id".into(),
                Value::String(parent_tool_call_id.to_string()),
            );
        }
        child_base.insert(
            "agent.cursor.subagent.link_confidence".into(),
            Value::String(
                if parent_tool_call_id.is_some() {
                    "transcript_dir"
                } else {
                    "orphan"
                }
                .into(),
            ),
        );
        let child_model = event_str(&child_owned[0], "model").unwrap_or(&model);
        let child_model = if event_str(&child_owned[0], "model").is_some() {
            child_model.to_string()
        } else {
            model.clone()
        };
        let prompt_ts = event_str(&child_owned[0], "_journal_ts").map(str::to_owned);
        records.extend(build_parent_steps(
            &child_owned,
            &turn_id,
            &child_model,
            &child_base,
            ParentStepOptions {
                user_prompt: None,
                prompt_event_ts: prompt_ts,
                step_prefix: Some(format!("{turn_id}:sub:{child_short}")),
                subagent_results: HashMap::new(),
            },
        ));
    }

    let mut consumed = HashSet::new();
    consumed.insert(parent_conv_id);
    for child_id in child_conv_ids {
        consumed.insert(child_id);
    }
    let prompts = events
        .iter()
        .filter(|event| hook_name(event) == "beforeSubmitPrompt")
        .map(|event| conversation_of(event).to_string())
        .collect::<HashSet<_>>();
    let parent_prompt_ts = ts_ms_value(prompt);
    let parent_stop_ts = match stop_event {
        None => Some(i64::MAX),
        Some(event) => ts_ms_value(event),
    };
    if let (Some(start), Some(end)) = (parent_prompt_ts, parent_stop_ts) {
        for event in events {
            let conversation = conversation_of(event);
            if conversation.is_empty() || consumed.contains(conversation) {
                continue;
            }
            let Some(event_ms) = ts_ms_value(event) else {
                continue;
            };
            if event_ms >= start && event_ms <= end && !prompts.contains(conversation) {
                consumed.insert(conversation.to_string());
            }
        }
    }
    (records, consumed)
}

pub(super) fn build_cursor_records_from_transcript(
    transcript_path: &str,
    events: &[Value],
    stop_conversation_id: &str,
    variant: &str,
    user_id: &str,
) -> Option<Vec<Obj>> {
    if transcript_path.is_empty() || !Path::new(transcript_path).exists() {
        return None;
    }
    let turn = parse_cursor_transcript(transcript_path)?;
    let prompt = if stop_conversation_id.is_empty() {
        events
            .iter()
            .find(|event| hook_name(event) == "beforeSubmitPrompt")
    } else {
        events.iter().find(|event| {
            hook_name(event) == "beforeSubmitPrompt"
                && conversation_of(event) == stop_conversation_id
        })
    }?;
    let parent_conv_id = conversation_of(prompt).to_string();
    let turn_id = event_str(prompt, "generation_id")
        .unwrap_or(parent_conv_id.as_str())
        .to_string();
    let trace_id = derive_trace_id(&turn_id);
    let mut parent_events: Vec<Value> = events
        .iter()
        .filter(|event| conversation_of(event) == parent_conv_id)
        .filter(|event| hook_name(event) != "sessionStart")
        .cloned()
        .collect();
    parent_events.sort_by_key(ts_ms_sort);
    let workspace = resolve_workspace_path(&parent_events);
    let model = parent_events
        .iter()
        .find_map(|event| {
            event_str(event, "model").filter(|model| *model != "unknown" && !model.is_empty())
        })
        .or_else(|| event_str(prompt, "model"))
        .unwrap_or("unknown")
        .to_string();
    let base = base_fields(
        &trace_id,
        &parent_conv_id,
        &turn_id,
        variant,
        user_id,
        workspace.as_deref(),
        true,
    );
    let user_text = turn
        .user_text
        .clone()
        .or_else(|| event_str(prompt, "prompt").map(str::to_owned));
    let mut records = Vec::new();
    if let Some(user_text) = user_text.as_deref().filter(|text| !text.is_empty()) {
        let nanos = event_ts_nanos(prompt);
        let mut fields = timed_identity(&nanos, "other");
        fields.insert(
            "gen_ai.provider.name".into(),
            Value::String(infer_provider(&model)),
        );
        fields.insert(
            "gen_ai.input.messages_delta".into(),
            json!([user_text_message(user_text)]),
        );
        fields.insert(
            "agent.cursor.hook_event_name".into(),
            Value::String("beforeSubmitPrompt".into()),
        );
        if let Some(mode) = event_str(prompt, "composer_mode") {
            fields.insert(
                "agent.cursor.composer_mode".into(),
                Value::String(mode.to_string()),
            );
        }
        records.push(finish_record(&base, fields));
    }

    let steps = align_steps(&turn.assistant_entries, &parent_events, &turn_id);
    let stop_event = parent_events
        .iter()
        .find(|event| hook_name(event) == "stop");
    let mut cumulative =
        if let Some(user_text) = user_text.as_deref().filter(|text| !text.is_empty()) {
            vec![user_text_message(user_text)]
        } else {
            Vec::new()
        };
    let mut previous_assistant = None;
    let mut previous_results: Vec<Value> = Vec::new();

    for (index, step) in steps.iter().enumerate() {
        let step_id = format!("{turn_id}:s{}", index + 1);
        let is_last = index + 1 == steps.len();
        let response_id = uuid::Uuid::new_v4().to_string();
        let req_nanos = if index == 0 {
            if step
                .thought_event
                .as_ref()
                .and_then(|event| event.get("duration_ms"))
                .and_then(|value| finite_f64(Some(value)))
                .is_some()
            {
                nanos_from_ms(duration_start_ms(step.thought_event.as_ref().unwrap()))
            } else {
                event_ts_nanos(prompt)
            }
        } else {
            let previous = &steps[index - 1];
            if let Some(last_result) = previous.tool_results.last() {
                event_ts_nanos(last_result)
            } else if step
                .thought_event
                .as_ref()
                .and_then(|event| event.get("duration_ms"))
                .and_then(|value| finite_f64(Some(value)))
                .is_some()
            {
                nanos_from_ms(duration_start_ms(step.thought_event.as_ref().unwrap()))
            } else if let Some(thought) = step.thought_event.as_ref() {
                event_ts_nanos(thought)
            } else {
                event_ts_nanos(prompt)
            }
        };
        let step_model = step
            .thought_event
            .as_ref()
            .and_then(|event| event_str(event, "model"))
            .or_else(|| {
                step.response_event
                    .as_ref()
                    .and_then(|event| event_str(event, "model"))
            })
            .unwrap_or(model.as_str());
        let mut delta = Vec::new();
        if index == 0 {
            if let Some(user_text) = user_text.as_deref().filter(|text| !text.is_empty()) {
                delta.push(user_text_message(user_text));
            }
        }
        if index > 0 && !previous_results.is_empty() {
            let exchange = transcript_tool_exchange(previous_assistant.as_ref(), &previous_results);
            delta.extend(transcript_delta_from_exchange(&exchange));
            cumulative.extend(exchange);
        }
        let req_source = step
            .thought_event
            .as_ref()
            .or(step.response_event.as_ref())
            .unwrap_or(prompt);
        let time_source = if index == 0 {
            Some("prompt_submit")
        } else if !steps[index - 1].tool_results.is_empty() {
            Some("previous_step_end")
        } else {
            None
        };
        records.push(build_llm_request(
            &req_nanos,
            req_source,
            &base,
            &delta,
            &cumulative,
            RequestOptions {
                model_fallback: step_model,
                step_id: &step_id,
                time_source,
                response_id: Some(response_id.as_str()),
                resolve_request_model: false,
            },
        ));

        for call in &step.tool_calls {
            let call_nanos = if call.get("_synthetic").and_then(Value::as_bool) == Some(true) {
                req_nanos.clone()
            } else {
                event_ts_nanos(call)
            };
            records.push(build_tool_call_at(&call_nanos, call, &base, &step_id));
        }
        for result in &step.tool_results {
            records.push(build_tool_result(result, &base, &step_id));
        }

        let finish_reason = if step.tool_calls.is_empty() {
            "stop"
        } else {
            "tool_calls"
        };
        let resp_source = if is_last {
            step.response_event.as_ref().or(stop_event)
        } else {
            step.thought_event.as_ref()
        };
        let resp_nanos = resp_source
            .map(event_ts_nanos)
            .unwrap_or_else(|| req_nanos.clone());
        let text_part_type = if is_last { "text" } else { "reasoning" };
        let mut output_parts = Vec::new();
        if let Some(text) = step.text.as_deref().filter(|text| !text.is_empty()) {
            output_parts.push(json!({ "type": text_part_type, "content": text }));
        }
        for call in &step.tool_calls {
            output_parts.push(tool_call_part(
                event_str(call, "tool_use_id"),
                event_str(call, "tool_name").unwrap_or(""),
                call.get("tool_input").map(parse_maybe_json).unwrap_or(None),
            ));
        }
        let resp_model = resp_source
            .and_then(|event| event_str(event, "model"))
            .unwrap_or(step_model);
        let hook_event = resp_source
            .map(hook_name)
            .filter(|hook| !hook.is_empty())
            .unwrap_or(if is_last {
                "afterAgentResponse"
            } else {
                "afterAgentThought"
            });
        let mut fields = timed_identity(&resp_nanos, "llm.response");
        fields.insert("gen_ai.step.id".into(), Value::String(step_id));
        fields.insert("gen_ai.response.id".into(), Value::String(response_id));
        fields.insert(
            "gen_ai.provider.name".into(),
            Value::String(infer_provider(resp_model)),
        );
        fields.insert(
            "gen_ai.request.model".into(),
            Value::String(resp_model.to_string()),
        );
        fields.insert(
            "gen_ai.response.model".into(),
            Value::String(resp_model.to_string()),
        );
        fields.insert(
            "gen_ai.output.messages".into(),
            json!([{
                "role": "assistant",
                "parts": output_parts,
                "finish_reason": finish_reason,
            }]),
        );
        fields.insert(
            "gen_ai.response.finish_reasons".into(),
            json!([finish_reason]),
        );
        fields.insert(
            "agent.cursor.hook_event_name".into(),
            Value::String(hook_event.to_string()),
        );
        if let Some(source_name) = resp_source.map(hook_name) {
            let source = match source_name {
                "afterAgentThought" => Some("after_agent_thought"),
                "afterAgentResponse" => Some("after_agent_response"),
                _ => None,
            };
            if let Some(source) = source {
                fields.insert(
                    "agent.cursor.llm_response_time_source".into(),
                    Value::String(source.into()),
                );
            }
        }
        let mut response = finish_record(&base, fields);
        if is_last {
            if let Some(token_event) = step.response_event.as_ref().or(stop_event) {
                merge_tokens(&mut response, token_event);
            }
        } else {
            for key in [
                "gen_ai.usage.input_tokens",
                "gen_ai.usage.output_tokens",
                "gen_ai.usage.cache_read.input_tokens",
                "gen_ai.usage.cache_creation.input_tokens",
                "gen_ai.usage.total_tokens",
            ] {
                response.insert(key.into(), Value::from(0));
            }
        }
        records.push(response);
        let assistant_has_call = output_parts
            .iter()
            .any(|part| part.get("type").and_then(Value::as_str) == Some("tool_call"));
        previous_assistant = if assistant_has_call {
            Some(json!({ "role": "assistant", "parts": output_parts }))
        } else {
            None
        };
        previous_results = step.tool_results.clone();
    }

    if records.is_empty() {
        None
    } else {
        Some(records)
    }
}

pub(super) fn maybe_inject_skills(
    records: &mut Vec<Obj>,
    events: &[Value],
    conversation_id: &str,
    transcript_path: &str,
    assembled_from_transcript: bool,
) {
    if transcript_path.is_empty() || records.is_empty() {
        return;
    }
    let Some(prompt) = events.iter().find_map(|event| {
        if hook_name(event) == "beforeSubmitPrompt" && conversation_of(event) == conversation_id {
            event_str(event, "prompt")
        } else {
            None
        }
    }) else {
        return;
    };
    let Some(detected) = detect_skill_from_transcript(transcript_path, prompt) else {
        return;
    };
    let skills = filter_skills_for_read_injection(&detected, assembled_from_transcript);
    if skills.is_empty() {
        return;
    }
    inject_skill_records(records, &skills);
}

struct ParentStepOptions {
    user_prompt: Option<String>,
    prompt_event_ts: Option<String>,
    step_prefix: Option<String>,
    subagent_results: HashMap<String, SubagentResult>,
}

fn build_parent_steps(
    events: &[Value],
    turn_id: &str,
    model: &str,
    base: &Obj,
    options: ParentStepOptions,
) -> Vec<Obj> {
    let ParentStepOptions {
        user_prompt,
        prompt_event_ts,
        step_prefix,
        subagent_results,
    } = options;
    let step_events = dedupe_after_agent_thought(events)
        .into_iter()
        .filter(|event| {
            matches!(
                hook_name(event),
                "afterAgentThought"
                    | "afterAgentResponse"
                    | "preToolUse"
                    | "postToolUse"
                    | "postToolUseFailure"
            )
        })
        .collect();
    let mut builder = StepBuilder {
        events: step_events,
        turn_id: turn_id.to_string(),
        model: model.to_string(),
        base: base.clone(),
        user_prompt: user_prompt.clone(),
        step_prefix,
        subagent_results,
        records: Vec::new(),
        cumulative: Vec::new(),
        step_round: 0,
        current_step_id: None,
        current_step_has_tools: false,
        current_response: None,
        previous_assistant: None,
        previous_tool_results: Vec::new(),
        last_step_end_ts: prompt_event_ts,
        pending_records: Vec::new(),
        pending_calls: Vec::new(),
        pending_results: Vec::new(),
        step_tool_calls: HashMap::new(),
        active_sync: HashMap::new(),
        synthesized: HashSet::new(),
    };
    if let Some(prompt) = user_prompt.filter(|prompt| !prompt.is_empty()) {
        builder.cumulative.push(user_text_message(&prompt));
    }
    builder.drive();
    builder.records
}

impl StepBuilder {
    fn drive(&mut self) {
        let total = self.events.len();
        for index in 0..total {
            let event = self.events[index].clone();
            match hook_name(&event) {
                "afterAgentThought" => self.on_thought(index, &event),
                "afterAgentResponse" => self.on_response(index, &event),
                "preToolUse" => self.on_pre_tool(&event),
                "postToolUse" | "postToolUseFailure" => self.on_post_tool(&event),
                _ => {}
            }
        }
        if !self.pending_records.is_empty() {
            let stamp = self
                .last_step_end_ts
                .clone()
                .map(Stamp::Iso)
                .unwrap_or_else(|| Stamp::Iso(String::new()));
            self.open_implicit_step(Timed {
                stamp,
                source: None,
            });
        }
        self.finalize_current_response();
        assign_finish_reasons(&mut self.records);
    }

    fn on_thought(&mut self, index: usize, event: &Value) {
        let no_step = self.current_step_id.is_none();
        let open = no_step || self.can_open_step_after_tools(event, index);
        if open {
            let is_first = self.step_round == 0;
            let prompt = self.user_prompt.clone();
            self.open_new_step(event, is_first, prompt.as_deref());
            let step_id = self.current_step_id.clone().unwrap_or_default();
            self.current_response = Some(build_llm_response(
                event,
                &self.base,
                &self.model,
                &step_id,
                "reasoning",
                false,
            ));
            if self.flush_pending(&step_id) {
                self.current_step_has_tools = true;
            }
        } else if self.current_response.is_some() {
            append_part(
                self.current_response.as_mut().unwrap(),
                "reasoning",
                event_str(event, "text").unwrap_or(""),
            );
        } else {
            let step_id = self.current_step_id.clone().unwrap_or_default();
            self.current_response = Some(build_llm_response(
                event,
                &self.base,
                &self.model,
                &step_id,
                "reasoning",
                false,
            ));
        }
    }

    fn on_response(&mut self, index: usize, event: &Value) {
        if self.current_step_id.is_none() && !self.pending_records.is_empty() {
            let timed = llm_request_start_time(event, self.last_step_end_ts.as_deref());
            self.open_implicit_step(timed);
        }
        let has_step = self.current_step_id.is_some();
        if has_step && self.can_open_step_after_tools(event, index) {
            self.open_new_step(event, false, None);
            let step_id = self.current_step_id.clone().unwrap_or_default();
            self.current_response = Some(build_llm_response(
                event,
                &self.base,
                &self.model,
                &step_id,
                "text",
                true,
            ));
            if self.flush_pending(&step_id) {
                self.current_step_has_tools = true;
            }
        } else if has_step {
            if self.current_response.is_some() {
                append_part(
                    self.current_response.as_mut().unwrap(),
                    "text",
                    event_str(event, "text").unwrap_or(""),
                );
                merge_tokens(self.current_response.as_mut().unwrap(), event);
            } else {
                let step_id = self.current_step_id.clone().unwrap_or_default();
                self.current_response = Some(build_llm_response(
                    event,
                    &self.base,
                    &self.model,
                    &step_id,
                    "text",
                    true,
                ));
            }
        } else {
            let is_first = self.step_round == 0;
            let prompt = self.user_prompt.clone();
            self.open_new_step(event, is_first, prompt.as_deref());
            let step_id = self.current_step_id.clone().unwrap_or_default();
            self.current_response = Some(build_llm_response(
                event,
                &self.base,
                &self.model,
                &step_id,
                "text",
                true,
            ));
            if self.flush_pending(&step_id) {
                self.current_step_has_tools = true;
            }
        }
    }

    fn on_pre_tool(&mut self, event: &Value) {
        mark_sync_started(&mut self.active_sync, event);
        let tool_name = event_str(event, "tool_name").unwrap_or("").to_string();
        let tool_use_id = event_str(event, "tool_use_id").map(str::to_owned);
        let tool_input = event
            .get("tool_input")
            .filter(|value| !value.is_null())
            .cloned();
        let observed = event_str(event, "_journal_ts").unwrap_or("").to_string();
        if self.current_step_id.is_none() {
            self.pending_records
                .push(build_tool_call(event, &self.base, "__pending__"));
            self.pending_calls.push(ToolCallMeta {
                tool_name: tool_name.clone(),
                tool_use_id: tool_use_id.clone(),
                tool_input: tool_input.clone(),
                observed_at: observed,
            });
            if is_subagent_tool(Some(tool_name.as_str())) {
                if let Some(id) = tool_use_id.clone().filter(|id| !id.is_empty()) {
                    if let Some(result) = self.subagent_results.get(&id).cloned() {
                        self.pending_records.push(build_subagent_result(
                            event,
                            &result,
                            &self.base,
                            "__pending__",
                        ));
                        self.pending_results.push(ToolResultMeta {
                            tool_use_id,
                            result: Some(Value::String(result.result_text)),
                            error: None,
                            end_ts: result.end_ts,
                        });
                        self.synthesized.insert(id);
                    }
                }
            }
            return;
        }

        let step_id = self.current_step_id.clone().unwrap_or_default();
        if is_subagent_tool(Some(tool_name.as_str())) {
            if let Some(id) = tool_use_id.clone().filter(|id| !id.is_empty()) {
                if let Some(result) = self.subagent_results.get(&id).cloned() {
                    let child_start = if result.end_ts.is_empty() {
                        observed.clone()
                    } else {
                        iso_minus_millis(&result.end_ts, result.duration_ms)
                            .unwrap_or_else(|| observed.clone())
                    };
                    self.records.push(build_tool_call_at(
                        &stamp_to_nanos(&Stamp::Iso(child_start.clone())),
                        event,
                        &self.base,
                        &step_id,
                    ));
                    self.records
                        .push(build_subagent_result(event, &result, &self.base, &step_id));
                    self.previous_tool_results.push(ToolResultMeta {
                        tool_use_id: tool_use_id.clone(),
                        result: Some(Value::String(result.result_text)),
                        error: None,
                        end_ts: result.end_ts.clone(),
                    });
                    self.last_step_end_ts = Some(result.end_ts);
                    self.synthesized.insert(id);
                    self.current_step_has_tools = true;
                    self.step_tool_calls
                        .entry(step_id)
                        .or_default()
                        .push(ToolCallMeta {
                            tool_name,
                            tool_use_id,
                            tool_input,
                            observed_at: child_start,
                        });
                    return;
                }
            }
        }
        self.records
            .push(build_tool_call(event, &self.base, &step_id));
        self.current_step_has_tools = true;
        self.step_tool_calls
            .entry(step_id)
            .or_default()
            .push(ToolCallMeta {
                tool_name,
                tool_use_id,
                tool_input,
                observed_at: observed,
            });
    }

    fn on_post_tool(&mut self, event: &Value) {
        let tool_name = event_str(event, "tool_name").unwrap_or("");
        let tool_use_id = event_str(event, "tool_use_id").unwrap_or("");
        if is_subagent_tool(Some(tool_name)) && self.synthesized.contains(tool_use_id) {
            return;
        }
        mark_sync_finished(&mut self.active_sync, event);
        let meta = ToolResultMeta {
            tool_use_id: event_str(event, "tool_use_id").map(str::to_owned),
            result: event
                .get("tool_output")
                .filter(|value| !value.is_null())
                .cloned(),
            error: event_str(event, "error_message").map(str::to_owned),
            end_ts: event_str(event, "_journal_ts").unwrap_or("").to_string(),
        };
        if self.current_step_id.is_none() {
            self.pending_records
                .push(build_tool_result(event, &self.base, "__pending__"));
            apply_tool_duration(&mut self.pending_records, "__pending__", event);
            self.pending_results.push(meta);
        } else {
            let step_id = self.current_step_id.clone().unwrap_or_default();
            apply_tool_duration(&mut self.records, &step_id, event);
            self.records
                .push(build_tool_result(event, &self.base, &step_id));
            self.previous_tool_results.push(meta);
            if let Some(ts) = event_str(event, "_journal_ts") {
                self.last_step_end_ts = Some(ts.to_string());
            }
        }
    }

    fn can_open_step_after_tools(&mut self, event: &Value, event_index: usize) -> bool {
        if !self.current_step_has_tools {
            return false;
        }
        let recovered = recover_leaked_sync_tools(self, event_index);
        if !self.active_sync.is_empty() {
            return false;
        }
        if self.previous_tool_results.is_empty() {
            return recovered;
        }
        match latest_tool_result_end(&self.previous_tool_results) {
            None => true,
            Some((_, end_ms)) => ts_ms_sort(event) >= end_ms,
        }
    }

    fn open_new_step(&mut self, event: &Value, is_first: bool, user_prompt: Option<&str>) {
        self.finalize_current_response();
        self.step_round += 1;
        let prefix = self
            .step_prefix
            .clone()
            .unwrap_or_else(|| self.turn_id.clone());
        let step_id = format!("{prefix}:s{}", self.step_round);
        self.current_step_id = Some(step_id.clone());
        self.current_step_has_tools = false;
        self.step_tool_calls.insert(step_id.clone(), Vec::new());
        let latest = latest_tool_result_end(&self.previous_tool_results);
        let delta = build_delta_messages(
            is_first,
            user_prompt,
            self.previous_assistant.as_ref(),
            &self.previous_tool_results,
            &mut self.cumulative,
        );
        let timed = clamp_request_start(
            llm_request_start_time(event, self.last_step_end_ts.as_deref()),
            latest.as_ref(),
        );
        let nanos = stamp_to_nanos(&timed.stamp);
        let full = self.cumulative.clone();
        let record = build_llm_request(
            &nanos,
            event,
            &self.base,
            &delta,
            &full,
            RequestOptions {
                model_fallback: &self.model,
                step_id: &step_id,
                time_source: timed.source.as_deref(),
                response_id: None,
                resolve_request_model: true,
            },
        );
        self.records.push(record);
        self.previous_assistant = None;
        self.previous_tool_results.clear();
    }

    fn open_implicit_step(&mut self, timed: Timed) {
        let is_first = self.step_round == 0;
        self.step_round += 1;
        let prefix = self
            .step_prefix
            .clone()
            .unwrap_or_else(|| self.turn_id.clone());
        let step_id = format!("{prefix}:s{}", self.step_round);
        self.current_step_id = Some(step_id.clone());
        let prompt = self.user_prompt.clone();
        let assistant = self.previous_assistant.clone();
        let results = self.previous_tool_results.clone();
        let delta = build_delta_messages(
            is_first,
            prompt.as_deref(),
            assistant.as_ref(),
            &results,
            &mut self.cumulative,
        );
        self.previous_assistant = None;
        self.previous_tool_results.clear();
        let nanos = stamp_to_nanos(&timed.stamp);
        let implicit = json!({ "hook_event": "implicit" });
        let full = self.cumulative.clone();
        let source = timed.source.clone();
        self.records.push(build_llm_request(
            &nanos,
            &implicit,
            &self.base,
            &delta,
            &full,
            RequestOptions {
                model_fallback: &self.model,
                step_id: &step_id,
                time_source: source.as_deref(),
                response_id: None,
                resolve_request_model: true,
            },
        ));
        self.current_response = Some(build_empty_llm_response(
            &nanos,
            &self.base,
            &self.model,
            &step_id,
        ));
        self.flush_pending(&step_id);
        self.current_step_has_tools = true;
    }

    fn flush_pending(&mut self, step_id: &str) -> bool {
        let had_tools = !self.pending_records.is_empty();
        for mut record in self.pending_records.drain(..) {
            record.insert("gen_ai.step.id".into(), Value::String(step_id.to_string()));
            self.records.push(record);
        }
        self.step_tool_calls
            .entry(step_id.to_string())
            .or_default()
            .append(&mut self.pending_calls);
        self.previous_tool_results.append(&mut self.pending_results);
        had_tools
    }

    fn finalize_current_response(&mut self) {
        let Some(mut response) = self.current_response.take() else {
            return;
        };
        let step_id = self.current_step_id.clone().unwrap_or_default();
        let tool_calls = self
            .step_tool_calls
            .get(&step_id)
            .cloned()
            .unwrap_or_default();
        append_tool_call_parts(&mut response, &tool_calls);
        self.previous_assistant = llm_response_to_input_assistant(&response);
        apply_tool_call_response_timing(&mut response, &tool_calls);
        guard_request_time(&mut self.records, &step_id, &response);
        self.records.push(response);
    }
}

fn recover_leaked_sync_tools(builder: &mut StepBuilder, event_index: usize) -> bool {
    let active: Vec<(String, usize)> = builder
        .active_sync
        .iter()
        .map(|(key, count)| (key.clone(), *count))
        .collect();
    let mut recovered = false;
    for (key, active_count) in active {
        let mut remaining = 0usize;
        for future in builder.events.iter().skip(event_index + 1) {
            let hook = hook_name(future);
            if matches!(hook, "postToolUse" | "postToolUseFailure")
                && !is_subagent_tool(event_str(future, "tool_name"))
                && sync_completion_matches(&key, future)
            {
                remaining += 1;
            }
        }
        if remaining == 0 {
            builder.active_sync.remove(&key);
            recovered = true;
        } else if remaining < active_count {
            builder.active_sync.insert(key, remaining);
            recovered = true;
        }
    }
    recovered
}

fn mark_sync_started(active: &mut HashMap<String, usize>, event: &Value) {
    if is_subagent_tool(event_str(event, "tool_name")) {
        return;
    }
    let key = sync_key(event);
    *active.entry(key).or_insert(0) += 1;
}

fn mark_sync_finished(active: &mut HashMap<String, usize>, event: &Value) {
    if is_subagent_tool(event_str(event, "tool_name")) {
        return;
    }
    let key = sync_key(event);
    if active.contains_key(&key) {
        drain_active(active, &key);
        return;
    }
    let fallback: Vec<String> = active
        .keys()
        .filter(|active_key| sync_completion_matches(active_key, event))
        .cloned()
        .collect();
    if fallback.len() == 1 {
        drain_active(active, &fallback[0]);
    }
}

fn drain_active(active: &mut HashMap<String, usize>, key: &str) {
    let count = active.get(key).copied().unwrap_or(0);
    if count <= 1 {
        active.remove(key);
    } else {
        active.insert(key.to_string(), count - 1);
    }
}

fn sync_key(event: &Value) -> String {
    format!(
        "{}\0{}",
        event_str(event, "tool_use_id").unwrap_or(""),
        event_str(event, "tool_name").unwrap_or("")
    )
}

fn sync_completion_matches(key: &str, event: &Value) -> bool {
    let (active_id, active_name) = key.split_once('\0').unwrap_or((key, ""));
    let result_id = event_str(event, "tool_use_id").unwrap_or("");
    let result_name = event_str(event, "tool_name").unwrap_or("");
    if active_name != result_name {
        return false;
    }
    if !active_id.is_empty() && !result_id.is_empty() {
        return active_id == result_id;
    }
    true
}

fn build_delta_messages(
    is_first: bool,
    user_prompt: Option<&str>,
    previous_assistant: Option<&Value>,
    previous_results: &[ToolResultMeta],
    cumulative: &mut Vec<Value>,
) -> Vec<Value> {
    let mut delta = Vec::new();
    if is_first {
        if let Some(prompt) = user_prompt.filter(|prompt| !prompt.is_empty()) {
            delta.push(user_text_message(prompt));
        }
    }
    if !previous_results.is_empty() {
        let exchange = react_tool_exchange(previous_assistant, previous_results);
        delta.extend(exchange.iter().cloned());
        cumulative.extend(exchange);
    }
    delta
}

fn react_tool_exchange(assistant: Option<&Value>, results: &[ToolResultMeta]) -> Vec<Value> {
    let calls = assistant
        .and_then(|message| message.get("parts"))
        .and_then(Value::as_array)
        .map(|parts| {
            parts
                .iter()
                .filter(|part| part.get("type").and_then(Value::as_str) == Some("tool_call"))
                .cloned()
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let mut messages = Vec::new();
    if let Some(assistant) = assistant {
        messages.push(assistant.clone());
    }
    let mut used = HashSet::new();
    for call in &calls {
        let Some(call_id) = call
            .get("id")
            .and_then(Value::as_str)
            .filter(|id| !id.is_empty())
        else {
            continue;
        };
        if let Some(index) = results.iter().enumerate().find_map(|(index, result)| {
            if used.contains(&index) {
                return None;
            }
            let result_id = result.tool_use_id.as_deref().filter(|id| !id.is_empty())?;
            (result_id == call_id).then_some(index)
        }) {
            used.insert(index);
            messages.push(json!({
                "role": "tool",
                "parts": [tool_result_part(&results[index])],
            }));
        }
    }
    for (index, result) in results.iter().enumerate() {
        if used.contains(&index) {
            continue;
        }
        messages.push(json!({
            "role": "tool",
            "parts": [tool_result_part(result)],
        }));
    }
    messages
}

fn tool_result_part(result: &ToolResultMeta) -> Value {
    let response = match result.error.as_deref().filter(|error| !error.is_empty()) {
        Some(error) => error.to_string(),
        None => result
            .result
            .as_ref()
            .map(stringify_value)
            .unwrap_or_default(),
    };
    json!({
        "type": "tool_call_response",
        "id": match result.tool_use_id.as_deref().filter(|id| !id.is_empty()) {
            Some(id) => Value::String(id.to_string()),
            None => Value::Null,
        },
        "response": response,
    })
}

struct RequestOptions<'a> {
    model_fallback: &'a str,
    step_id: &'a str,
    time_source: Option<&'a str>,
    response_id: Option<&'a str>,
    resolve_request_model: bool,
}

fn build_llm_request(
    nanos: &str,
    event: &Value,
    base: &Obj,
    delta: &[Value],
    full: &[Value],
    options: RequestOptions<'_>,
) -> Obj {
    let RequestOptions {
        model_fallback,
        step_id,
        time_source,
        response_id,
        resolve_request_model,
    } = options;
    let raw_model = if resolve_request_model {
        event_str(event, "model").unwrap_or(model_fallback)
    } else {
        model_fallback
    };
    let request_model = if resolve_request_model {
        resolve_model(Some(raw_model))
    } else {
        raw_model.to_string()
    };
    let mut fields = timed_identity(nanos, "llm.request");
    fields.insert("gen_ai.step.id".into(), Value::String(step_id.to_string()));
    if let Some(response_id) = response_id {
        fields.insert(
            "gen_ai.response.id".into(),
            Value::String(response_id.to_string()),
        );
    }
    fields.insert(
        "gen_ai.provider.name".into(),
        Value::String(infer_provider(raw_model)),
    );
    fields.insert("gen_ai.request.model".into(), Value::String(request_model));
    if !delta.is_empty() {
        fields.insert(
            "gen_ai.input.messages_delta".into(),
            Value::Array(delta.to_vec()),
        );
    }
    if !full.is_empty() {
        fields.insert("gen_ai.input.messages".into(), Value::Array(full.to_vec()));
    }
    let hook = hook_name(event);
    if !hook.is_empty() {
        fields.insert(
            "agent.cursor.hook_event_name".into(),
            Value::String(hook.to_string()),
        );
    }
    if let Some(time_source) = time_source {
        fields.insert(
            "agent.cursor.llm_request_time_source".into(),
            Value::String(time_source.to_string()),
        );
    }
    finish_record(base, fields)
}

fn build_llm_response(
    event: &Value,
    base: &Obj,
    model_fallback: &str,
    step_id: &str,
    part_type: &str,
    with_tokens: bool,
) -> Obj {
    let model = event_str(event, "model").unwrap_or(model_fallback);
    let nanos = event_ts_nanos(event);
    let text = event_str(event, "text").unwrap_or("");
    let parts = if text.is_empty() {
        Vec::new()
    } else {
        vec![json!({ "type": part_type, "content": text })]
    };
    let mut fields = timed_identity(&nanos, "llm.response");
    fields.insert("gen_ai.step.id".into(), Value::String(step_id.to_string()));
    fields.insert(
        "gen_ai.response.id".into(),
        Value::String(uuid::Uuid::new_v4().to_string()),
    );
    fields.insert(
        "gen_ai.provider.name".into(),
        Value::String(infer_provider(model)),
    );
    fields.insert(
        "gen_ai.request.model".into(),
        Value::String(resolve_model(Some(model))),
    );
    fields.insert(
        "gen_ai.response.model".into(),
        Value::String(resolve_model(Some(model))),
    );
    fields.insert(
        "gen_ai.output.messages".into(),
        json!([{ "role": "assistant", "parts": parts }]),
    );
    let hook = hook_name(event);
    if !hook.is_empty() {
        fields.insert(
            "agent.cursor.hook_event_name".into(),
            Value::String(hook.to_string()),
        );
    }
    if part_type == "reasoning" {
        if let Some(observed) = event_str(event, "_journal_ts") {
            fields.insert(
                "agent.cursor.reasoning_observed_at".into(),
                Value::String(observed.to_string()),
            );
        }
    }
    let source = match hook {
        "afterAgentThought" => Some("after_agent_thought"),
        "afterAgentResponse" => Some("after_agent_response"),
        _ => None,
    };
    if let Some(source) = source {
        fields.insert(
            "agent.cursor.llm_response_time_source".into(),
            Value::String(source.into()),
        );
    }
    let mut record = finish_record(base, fields);
    if with_tokens {
        merge_tokens(&mut record, event);
    }
    record
}

fn build_empty_llm_response(nanos: &str, base: &Obj, model_fallback: &str, step_id: &str) -> Obj {
    let mut fields = timed_identity(nanos, "llm.response");
    fields.insert("gen_ai.step.id".into(), Value::String(step_id.to_string()));
    fields.insert(
        "gen_ai.response.id".into(),
        Value::String(uuid::Uuid::new_v4().to_string()),
    );
    fields.insert(
        "gen_ai.provider.name".into(),
        Value::String(infer_provider(model_fallback)),
    );
    fields.insert(
        "gen_ai.request.model".into(),
        Value::String(resolve_model(Some(model_fallback))),
    );
    fields.insert(
        "gen_ai.response.model".into(),
        Value::String(resolve_model(Some(model_fallback))),
    );
    fields.insert(
        "gen_ai.output.messages".into(),
        json!([{ "role": "assistant", "parts": [] }]),
    );
    fields.insert(
        "agent.cursor.hook_event_name".into(),
        Value::String("implicit".into()),
    );
    finish_record(base, fields)
}

fn build_tool_call(event: &Value, base: &Obj, step_id: &str) -> Obj {
    build_tool_call_at(&event_ts_nanos(event), event, base, step_id)
}

fn build_tool_call_at(nanos: &str, event: &Value, base: &Obj, step_id: &str) -> Obj {
    let mut fields = timed_identity(nanos, "tool.call");
    fields.insert("gen_ai.step.id".into(), Value::String(step_id.to_string()));
    if let Some(name) = event_str(event, "tool_name") {
        fields.insert("gen_ai.tool.name".into(), Value::String(name.to_string()));
    }
    if let Some(id) = event_str(event, "tool_use_id") {
        fields.insert("gen_ai.tool.call.id".into(), Value::String(id.to_string()));
    }
    if let Some(input) = event
        .get("tool_input")
        .map(parse_maybe_json)
        .unwrap_or(None)
    {
        if let Some(json_value) = to_json_value(&input) {
            fields.insert("gen_ai.tool.call.arguments".into(), json_value);
        }
    }
    if let Some(hook) = event_str(event, "hook_event") {
        fields.insert(
            "agent.cursor.hook_event_name".into(),
            Value::String(hook.to_string()),
        );
    }
    finish_record(base, fields)
}

fn build_tool_result(event: &Value, base: &Obj, step_id: &str) -> Obj {
    let failure = hook_name(event) == "postToolUseFailure";
    let nanos = event_ts_nanos(event);
    let mut fields = timed_identity(&nanos, "tool.result");
    fields.insert("gen_ai.step.id".into(), Value::String(step_id.to_string()));
    if let Some(name) = event_str(event, "tool_name") {
        fields.insert("gen_ai.tool.name".into(), Value::String(name.to_string()));
    }
    if let Some(id) = event_str(event, "tool_use_id") {
        fields.insert("gen_ai.tool.call.id".into(), Value::String(id.to_string()));
    }
    if !failure {
        if let Some(output) = event
            .get("tool_output")
            .map(parse_maybe_json)
            .unwrap_or(None)
        {
            if let Some(json_value) = to_json_value(&output) {
                fields.insert("gen_ai.tool.call.result".into(), json_value);
            }
        }
    }
    if let Some(duration) = event.get("duration_ms") {
        if finite_f64(Some(duration)).is_some() {
            fields.insert("gen_ai.tool.call.duration".into(), duration.clone());
        }
    }
    if failure {
        fields.insert("tool.result.status".into(), Value::String("failure".into()));
        fields.insert(
            "error.type".into(),
            Value::String(
                event_str(event, "failure_type")
                    .unwrap_or("tool_use_failure")
                    .to_string(),
            ),
        );
        if let Some(message) = event_str(event, "error_message") {
            fields.insert("error.message".into(), Value::String(message.to_string()));
        }
    }
    if let Some(hook) = event_str(event, "hook_event") {
        fields.insert(
            "agent.cursor.hook_event_name".into(),
            Value::String(hook.to_string()),
        );
    }
    finish_record(base, fields)
}

fn build_subagent_result(event: &Value, result: &SubagentResult, base: &Obj, step_id: &str) -> Obj {
    let nanos = if result.end_ts.is_empty() {
        event_ts_nanos(event)
    } else {
        timestamp_from_string(&result.end_ts)
    };
    let mut fields = timed_identity(&nanos, "tool.result");
    fields.insert("gen_ai.step.id".into(), Value::String(step_id.to_string()));
    fields.insert(
        "gen_ai.tool.name".into(),
        Value::String(if result.tool_name.is_empty() {
            event_str(event, "tool_name")
                .unwrap_or("Subagent")
                .to_string()
        } else {
            result.tool_name.clone()
        }),
    );
    if let Some(id) = event_str(event, "tool_use_id") {
        fields.insert("gen_ai.tool.call.id".into(), Value::String(id.to_string()));
    }
    fields.insert(
        "gen_ai.tool.call.duration".into(),
        Value::from(result.duration_ms),
    );
    fields.insert(
        "gen_ai.tool.call.result".into(),
        if result.result_text.is_empty() {
            json!({ "status": "completed" })
        } else {
            json!({ "summary": result.result_text.chars().take(500).collect::<String>() })
        },
    );
    fields.insert(
        "tool.result.status".into(),
        Value::String("completed".into()),
    );
    fields.insert(
        "agent.cursor.hook_event_name".into(),
        Value::String("subagent_result_synthesized".into()),
    );
    finish_record(base, fields)
}

fn append_tool_call_parts(response: &mut Obj, tool_calls: &[ToolCallMeta]) {
    if tool_calls.is_empty() {
        return;
    }
    let Some(messages) = response
        .get_mut("gen_ai.output.messages")
        .and_then(Value::as_array_mut)
    else {
        return;
    };
    if messages.is_empty() {
        return;
    }
    let Some(first) = messages[0].as_object_mut() else {
        return;
    };
    let parts = first
        .entry("parts")
        .or_insert_with(|| Value::Array(Vec::new()));
    let Some(parts) = parts.as_array_mut() else {
        return;
    };
    for call in tool_calls {
        parts.push(tool_call_part(
            call.tool_use_id.as_deref(),
            &call.tool_name,
            call.tool_input
                .as_ref()
                .map(parse_maybe_json)
                .unwrap_or(None),
        ));
    }
}

fn tool_call_part(id: Option<&str>, name: &str, arguments: Option<Value>) -> Value {
    let mut part = Obj::new();
    part.insert("type".into(), Value::String("tool_call".into()));
    part.insert(
        "id".into(),
        match id.filter(|id| !id.is_empty()) {
            Some(id) => Value::String(id.to_string()),
            None => Value::Null,
        },
    );
    part.insert("name".into(), Value::String(name.to_string()));
    if let Some(arguments) = arguments {
        part.insert("arguments".into(), arguments);
    }
    Value::Object(part)
}

fn llm_response_to_input_assistant(response: &Obj) -> Option<Value> {
    let messages = response.get("gen_ai.output.messages")?.as_array()?;
    let assistant = messages
        .iter()
        .find(|message| message.get("role").and_then(Value::as_str) == Some("assistant"))?;
    let parts = assistant.get("parts")?.as_array()?;
    if !parts
        .iter()
        .any(|part| part.get("type").and_then(Value::as_str) == Some("tool_call"))
    {
        return None;
    }
    Some(json!({ "role": "assistant", "parts": parts.clone() }))
}

fn apply_tool_call_response_timing(response: &mut Obj, tool_calls: &[ToolCallMeta]) {
    if tool_calls.is_empty() {
        return;
    }
    let mut observed = tool_calls
        .iter()
        .filter_map(|call| parse_iso_ms(&call.observed_at).map(|ms| (ms, call)))
        .collect::<Vec<_>>();
    if observed.is_empty() {
        return;
    }
    observed.sort_by_key(|(ms, _)| *ms);
    let (first_ms, first) = observed[0];
    let (last_ms, last) = observed[observed.len() - 1];
    let reasoning_ms = response
        .get("agent.cursor.reasoning_observed_at")
        .and_then(Value::as_str)
        .and_then(parse_iso_ms);
    let buffered = reasoning_ms.map(|ms| first_ms < ms).unwrap_or(false);
    if !buffered {
        let nanos = nanos_from_unix_millis(first_ms);
        response.insert("time_unix_nano".into(), Value::String(nanos));
    }
    response.insert(
        "agent.cursor.first_tool_call_observed_at".into(),
        Value::String(first.observed_at.clone()),
    );
    response.insert(
        "agent.cursor.last_tool_call_observed_at".into(),
        Value::String(last.observed_at.clone()),
    );
    response.insert(
        "agent.cursor.tool_call_count".into(),
        Value::from(tool_calls.len() as i64),
    );
    if let Some(reasoning_ms) = reasoning_ms {
        response.insert(
            "agent.cursor.thought_to_first_tool_ms".into(),
            json_number((first_ms - reasoning_ms) as f64),
        );
        response.insert(
            "agent.cursor.tool_call_emission_ms".into(),
            json_number((last_ms - reasoning_ms) as f64),
        );
        if first_ms < reasoning_ms {
            response.insert(
                "agent.cursor.reasoning_observed_late".into(),
                Value::Bool(true),
            );
        }
    }
}

fn apply_tool_duration(records: &mut [Obj], step_id: &str, event: &Value) {
    let Some(tool_use_id) = event_str(event, "tool_use_id") else {
        return;
    };
    let Some(index) = records.iter().rposition(|record| {
        field_str(record, "event.name") == "tool.call"
            && field_str(record, "gen_ai.step.id") == step_id
            && field_str(record, "gen_ai.tool.call.id") == tool_use_id
    }) else {
        return;
    };
    if let Some(duration) = event.get("duration_ms") {
        if finite_f64(Some(duration)).is_some() {
            records[index].insert("gen_ai.tool.call.duration".into(), duration.clone());
        }
    }
}

fn guard_request_time(records: &mut [Obj], step_id: &str, response: &Obj) {
    let Some(req_index) = records.iter().rposition(|record| {
        field_str(record, "event.name") == "llm.request"
            && field_str(record, "gen_ai.step.id") == step_id
    }) else {
        return;
    };
    let req_nano = parse_i128(records[req_index].get("time_unix_nano"));
    let resp_nano = parse_i128(response.get("time_unix_nano"));
    let mut earliest: Option<i128> = None;
    for record in records.iter() {
        if field_str(record, "event.name") == "tool.call"
            && field_str(record, "gen_ai.step.id") == step_id
        {
            if let Some(nano) = record
                .get("time_unix_nano")
                .and_then(Value::as_str)
                .and_then(|text| text.parse().ok())
            {
                earliest = Some(earliest.map(|current| current.min(nano)).unwrap_or(nano));
            }
        }
    }
    let earliest_positive = earliest.filter(|nano| *nano > 0);
    if !(req_nano >= resp_nano
        || earliest_positive
            .map(|nano| req_nano >= nano)
            .unwrap_or(false))
    {
        return;
    }
    let base = earliest_positive.unwrap_or(resp_nano);
    let fixed = if base > 1_000_000 {
        base - 1_000_000
    } else {
        0
    };
    let label = if earliest_positive.is_some() {
        "earliest_tool_call_start_minus_1ms"
    } else {
        "response_end_minus_1ms"
    };
    let fixed = fixed.to_string();
    records[req_index].insert("time_unix_nano".into(), Value::String(fixed.clone()));
    records[req_index].insert("observed_time_unix_nano".into(), Value::String(fixed));
    records[req_index].insert(
        "agent.cursor.llm_request_time_guard".into(),
        Value::String(label.into()),
    );
}

fn assign_finish_reasons(records: &mut [Obj]) {
    let mut steps_with_tools = HashSet::new();
    for record in records.iter() {
        if matches!(field_str(record, "event.name"), "tool.call" | "tool.result") {
            let step = field_str(record, "gen_ai.step.id");
            if !step.is_empty() {
                steps_with_tools.insert(step.to_string());
            }
        }
    }
    let final_index = records
        .iter()
        .rposition(|record| field_str(record, "event.name") == "llm.response");
    for (index, record) in records.iter_mut().enumerate() {
        if field_str(record, "event.name") != "llm.response" {
            continue;
        }
        let step = field_str(record, "gen_ai.step.id").to_string();
        let reason = if Some(index) == final_index {
            "stop"
        } else if steps_with_tools.contains(&step) {
            "tool_calls"
        } else {
            "stop"
        };
        record.insert("gen_ai.response.finish_reasons".into(), json!([reason]));
        if let Some(messages) = record
            .get_mut("gen_ai.output.messages")
            .and_then(Value::as_array_mut)
        {
            if let Some(first) = messages.first_mut().and_then(Value::as_object_mut) {
                first.insert("finish_reason".into(), Value::String(reason.into()));
            }
        }
    }
}

fn append_part(response: &mut Obj, part_type: &str, text: &str) {
    if text.is_empty() {
        return;
    }
    let Some(messages) = response
        .get_mut("gen_ai.output.messages")
        .and_then(Value::as_array_mut)
    else {
        return;
    };
    let Some(first) = messages.first_mut().and_then(Value::as_object_mut) else {
        return;
    };
    let parts = first
        .entry("parts")
        .or_insert_with(|| Value::Array(Vec::new()));
    if let Some(parts) = parts.as_array_mut() {
        parts.push(json!({ "type": part_type, "content": text }));
    }
}

fn merge_tokens(record: &mut Obj, event: &Value) {
    copy_number(
        record,
        "gen_ai.usage.input_tokens",
        event.get("input_tokens"),
    );
    copy_number(
        record,
        "gen_ai.usage.output_tokens",
        event.get("output_tokens"),
    );
    copy_number(
        record,
        "gen_ai.usage.cache_read.input_tokens",
        event.get("cache_read_tokens"),
    );
    copy_number(
        record,
        "gen_ai.usage.cache_creation.input_tokens",
        event.get("cache_write_tokens"),
    );
    if let (Some(input), Some(output)) = (
        finite_f64(event.get("input_tokens")),
        finite_f64(event.get("output_tokens")),
    ) {
        record.insert(
            "gen_ai.usage.total_tokens".into(),
            json_number(input + output),
        );
    }
}

fn llm_request_start_time(event: &Value, fallback_ts: Option<&str>) -> Timed {
    if hook_name(event) == "afterAgentThought" && finite_f64(event.get("duration_ms")).is_some() {
        return Timed {
            stamp: Stamp::EpochMs(duration_start_ms(event)),
            source: Some("thought_duration".into()),
        };
    }
    if let Some(fallback) = fallback_ts.filter(|ts| !ts.is_empty()) {
        Timed {
            stamp: Stamp::Iso(fallback.to_string()),
            source: Some("previous_step_end".into()),
        }
    } else if let Some(ts) = event_str(event, "_journal_ts") {
        Timed {
            stamp: Stamp::Iso(ts.to_string()),
            source: None,
        }
    } else {
        Timed {
            stamp: Stamp::EpochMs(Utc::now().timestamp_millis() as f64),
            source: None,
        }
    }
}

fn clamp_request_start(start: Timed, latest: Option<&(String, i64)>) -> Timed {
    let Some((end_ts, end_ms)) = latest else {
        return start;
    };
    let Some(start_ms) = stamp_to_ms(&start.stamp) else {
        return start;
    };
    if start_ms >= *end_ms as f64 {
        return start;
    }
    Timed {
        stamp: Stamp::Iso(end_ts.clone()),
        source: Some("previous_tool_result_end".into()),
    }
}

fn duration_start_ms(event: &Value) -> f64 {
    let end_ms = ts_ms_sort(event) as f64;
    match finite_f64(event.get("duration_ms")) {
        Some(duration) if duration >= 0.0 => end_ms - duration,
        _ => end_ms,
    }
}

fn latest_tool_result_end(results: &[ToolResultMeta]) -> Option<(String, i64)> {
    let mut best: Option<(String, i64)> = None;
    for result in results {
        let Some(ms) = parse_iso_ms(&result.end_ts) else {
            continue;
        };
        if best
            .as_ref()
            .map(|(_, current)| ms > *current)
            .unwrap_or(true)
        {
            best = Some((result.end_ts.clone(), ms));
        }
    }
    best
}

fn dedupe_after_agent_thought(events: &[Value]) -> Vec<Value> {
    let mut generation_ids = HashSet::new();
    for event in events {
        if hook_name(event) == "afterAgentThought" {
            if let Some(id) = event.get("generation_id").and_then(Value::as_str) {
                generation_ids.insert(id.to_string());
            }
        }
    }
    struct Annotated {
        event: Value,
        duplicate_key: Option<String>,
        suffixed: bool,
    }
    let mut annotated = Vec::new();
    for event in events {
        let mut duplicate_key = None;
        let mut suffixed = false;
        if hook_name(event) == "afterAgentThought" {
            if let (Some(conversation), Some(generation), Some(text), Some(duration)) = (
                event.get("conversation_id").and_then(Value::as_str),
                event.get("generation_id").and_then(Value::as_str),
                event.get("text").and_then(Value::as_str),
                finite_f64(event.get("duration_ms")),
            ) {
                let suffix_base = THOUGHT_SUFFIX_RE
                    .captures(generation)
                    .and_then(|caps| caps.get(1))
                    .map(|matched| matched.as_str().to_string());
                suffixed = suffix_base
                    .as_ref()
                    .is_some_and(|base| generation_ids.contains(base));
                let canonical = if suffixed {
                    suffix_base.unwrap_or_else(|| generation.to_string())
                } else {
                    generation.to_string()
                };
                duplicate_key = serde_json::to_string(&json!([
                    conversation,
                    canonical,
                    text,
                    json_number(duration)
                ]))
                .ok();
            }
        }
        annotated.push(Annotated {
            event: event.clone(),
            duplicate_key,
            suffixed,
        });
    }
    let mut available: HashMap<String, usize> = HashMap::new();
    for item in &annotated {
        if let Some(key) = item.duplicate_key.as_ref().filter(|_| !item.suffixed) {
            *available.entry(key.clone()).or_insert(0) += 1;
        }
    }
    let mut consumed: HashMap<String, usize> = HashMap::new();
    annotated
        .into_iter()
        .filter(|item| {
            let Some(key) = item.duplicate_key.as_ref() else {
                return true;
            };
            if !item.suffixed {
                return true;
            }
            let available = available.get(key).copied().unwrap_or(0);
            let used = consumed.get(key).copied().unwrap_or(0);
            if used >= available {
                return true;
            }
            consumed.insert(key.clone(), used + 1);
            false
        })
        .map(|item| item.event)
        .collect()
}

struct TranscriptTurn {
    user_text: Option<String>,
    assistant_entries: Vec<AssistantEntry>,
}

struct AssistantEntry {
    text: Option<String>,
    tool_uses: Vec<Value>,
}

fn parse_cursor_transcript(path: &str) -> Option<TranscriptTurn> {
    let content = fs::read_to_string(path).ok()?;
    let lines: Vec<&str> = content
        .lines()
        .filter(|line| !line.trim().is_empty())
        .collect();
    let mut turn_ended = Vec::new();
    for (index, line) in lines.iter().enumerate() {
        if let Ok(entry) = serde_json::from_str::<Value>(line) {
            if entry.get("type").and_then(Value::as_str) == Some("turn_ended") {
                turn_ended.push(index);
            }
        }
    }
    let mut last_entry = None;
    for line in lines.iter().rev() {
        if let Ok(entry) = serde_json::from_str::<Value>(line) {
            last_entry = Some(entry);
            break;
        }
    }
    let ends_with_turn = last_entry
        .as_ref()
        .and_then(|entry| entry.get("type"))
        .and_then(Value::as_str)
        == Some("turn_ended");
    let (start, end) = if ends_with_turn && !turn_ended.is_empty() {
        let last = *turn_ended.last().unwrap();
        let previous = if turn_ended.len() >= 2 {
            turn_ended[turn_ended.len() - 2]
        } else {
            usize::MAX
        };
        let start = previous.wrapping_add(1);
        (start, last)
    } else {
        let last = turn_ended.last().copied();
        let start = last.map(|index| index + 1).unwrap_or(0);
        (start, lines.len())
    };
    let mut user_text = None;
    let mut assistant_entries = Vec::new();
    for line in lines.iter().take(end).skip(start) {
        let Ok(entry) = serde_json::from_str::<Value>(line) else {
            continue;
        };
        if entry.get("role").and_then(Value::as_str) == Some("user") {
            if let Some(text) = user_message_text(&entry) {
                if !text.is_empty() {
                    user_text = Some(text);
                }
            }
        }
        if entry.get("role").and_then(Value::as_str) == Some("assistant") {
            let Some(content) = entry.pointer("/message/content").and_then(Value::as_array) else {
                continue;
            };
            let raw_text = content
                .iter()
                .filter(|part| part.get("type").and_then(Value::as_str) == Some("text"))
                .filter_map(|part| part.get("text").and_then(Value::as_str))
                .collect::<String>();
            let text = if is_usable_text(&raw_text) {
                Some(raw_text)
            } else {
                None
            };
            let tool_uses = content
                .iter()
                .filter(|part| part.get("type").and_then(Value::as_str) == Some("tool_use"))
                .cloned()
                .collect::<Vec<_>>();
            assistant_entries.push(AssistantEntry { text, tool_uses });
        }
    }
    if user_text.is_none() && assistant_entries.is_empty() {
        return None;
    }
    Some(TranscriptTurn {
        user_text,
        assistant_entries,
    })
}

fn user_message_text(entry: &Value) -> Option<String> {
    let content = entry.pointer("/message/content")?.as_array()?;
    let mut pieces = Vec::new();
    for part in content {
        if part.get("type").and_then(Value::as_str) != Some("text") {
            continue;
        }
        let Some(text) = part
            .get("text")
            .and_then(Value::as_str)
            .filter(|text| !text.is_empty())
        else {
            continue;
        };
        let cleaned = USER_QUERY_TAG_RE.replace_all(text, "");
        let trimmed = cleaned.trim();
        if !trimmed.is_empty() {
            pieces.push(trimmed.to_string());
        }
    }
    if pieces.is_empty() {
        None
    } else {
        Some(pieces.concat())
    }
}

fn is_usable_text(text: &str) -> bool {
    !text.replace("[REDACTED]", "").trim().is_empty()
}

fn align_steps(
    entries: &[AssistantEntry],
    parent_events: &[Value],
    turn_id: &str,
) -> Vec<TranscriptStep> {
    let mut calls: Vec<Value> = parent_events
        .iter()
        .filter(|event| hook_name(event) == "preToolUse")
        .cloned()
        .collect();
    calls.sort_by_key(ts_ms_sort);
    let mut results: Vec<Value> = parent_events
        .iter()
        .filter(|event| matches!(hook_name(event), "postToolUse" | "postToolUseFailure"))
        .cloned()
        .collect();
    results.sort_by_key(ts_ms_sort);
    let mut thoughts: Vec<Value> = parent_events
        .iter()
        .filter(|event| hook_name(event) == "afterAgentThought")
        .cloned()
        .collect();
    thoughts.sort_by_key(ts_ms_sort);
    let mut responses: Vec<Value> = parent_events
        .iter()
        .filter(|event| hook_name(event) == "afterAgentResponse")
        .cloned()
        .collect();
    responses.sort_by_key(ts_ms_sort);
    if entries.is_empty() {
        return vec![TranscriptStep {
            text: None,
            tool_calls: calls,
            tool_results: results,
            thought_event: thoughts.into_iter().next(),
            response_event: responses.into_iter().next(),
        }];
    }
    let mut call_index = 0usize;
    let mut result_index = 0usize;
    let mut steps = Vec::new();
    for (index, entry) in entries.iter().enumerate() {
        let count = entry.tool_uses.len();
        let is_final = index + 1 == entries.len();
        let mut step_calls = Vec::new();
        for (offset, transcript_tool) in entry.tool_uses.iter().enumerate() {
            let journal_event = calls.get(call_index + offset);
            let input = transcript_tool
                .get("input")
                .cloned()
                .unwrap_or_else(|| json!({}));
            let input_text = serde_json::to_string(&input).unwrap_or_else(|_| "{}".into());
            if let Some(journal_event) = journal_event {
                let mut copy = journal_event.clone();
                if let Some(object) = copy.as_object_mut() {
                    object.insert("tool_input".into(), Value::String(input_text));
                }
                step_calls.push(copy);
            } else {
                let name = transcript_tool
                    .get("name")
                    .and_then(Value::as_str)
                    .unwrap_or("");
                step_calls.push(json!({
                    "_journal_ts": Value::Null,
                    "hook_event": "preToolUse",
                    "tool_name": name,
                    "tool_use_id": format!("{turn_id}:s{}:t{}", index + 1, offset + 1),
                    "tool_input": input_text,
                    "_synthetic": true,
                }));
            }
        }
        call_index += count;
        let step_results = if result_index < results.len() {
            let end = (result_index + count).min(results.len());
            let slice = results[result_index..end].to_vec();
            result_index = end;
            slice
        } else {
            Vec::new()
        };
        steps.push(TranscriptStep {
            text: entry.text.clone(),
            tool_calls: step_calls,
            tool_results: step_results,
            thought_event: if is_final {
                None
            } else {
                thoughts.get(index).cloned()
            },
            response_event: if is_final {
                responses.first().cloned()
            } else {
                None
            },
        });
    }
    steps
}

fn transcript_tool_exchange(assistant: Option<&Value>, results: &[Value]) -> Vec<Value> {
    let calls = assistant
        .and_then(|message| message.get("parts"))
        .and_then(Value::as_array)
        .map(|parts| {
            parts
                .iter()
                .filter(|part| part.get("type").and_then(Value::as_str) == Some("tool_call"))
                .cloned()
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let mut messages = Vec::new();
    if let Some(assistant) = assistant {
        messages.push(assistant.clone());
    }
    let mut used = HashSet::new();
    for call in &calls {
        let Some(call_id) = call
            .get("id")
            .and_then(Value::as_str)
            .filter(|id| !id.is_empty())
        else {
            continue;
        };
        if let Some(index) = results.iter().enumerate().find_map(|(index, result)| {
            if used.contains(&index) {
                return None;
            }
            let result_id = event_str(result, "tool_use_id")?;
            (result_id == call_id).then_some(index)
        }) {
            used.insert(index);
            messages.push(json!({
                "role": "tool",
                "parts": [{
                    "type": "tool_call_response",
                    "id": event_str(&results[index], "tool_use_id").map(|id| Value::String(id.to_string())).unwrap_or(Value::Null),
                    "response": "",
                }],
            }));
        }
    }
    for (index, result) in results.iter().enumerate() {
        if used.contains(&index) {
            continue;
        }
        messages.push(json!({
            "role": "tool",
            "parts": [{
                "type": "tool_call_response",
                "id": event_str(result, "tool_use_id").map(|id| Value::String(id.to_string())).unwrap_or(Value::Null),
                "response": "",
            }],
        }));
    }
    messages
}

fn transcript_delta_from_exchange(exchange: &[Value]) -> Vec<Value> {
    exchange
        .iter()
        .map(|message| {
            if message.get("role").and_then(Value::as_str) != Some("assistant") {
                return message.clone();
            }
            let calls = message
                .get("parts")
                .and_then(Value::as_array)
                .map(|parts| {
                    parts
                        .iter()
                        .filter(|part| {
                            part.get("type").and_then(Value::as_str) == Some("tool_call")
                        })
                        .cloned()
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            let mut copy = message.clone();
            if let Some(object) = copy.as_object_mut() {
                object.insert("parts".into(), Value::Array(calls));
            }
            copy
        })
        .collect()
}

fn recover_windows_user_prompt(path: &str) -> Option<String> {
    if !Path::new(path).exists() {
        return None;
    }
    let content = fs::read_to_string(path).ok()?;
    for line in content.lines().rev() {
        if line.trim().is_empty() {
            continue;
        }
        let Ok(entry) = serde_json::from_str::<Value>(line) else {
            continue;
        };
        if entry.get("role").and_then(Value::as_str) != Some("user") {
            continue;
        }
        if let Some(text) = user_message_text(&entry) {
            if !text.is_empty() {
                return Some(text);
            }
        }
    }
    None
}

fn scan_subagent_dir(transcript_path: &str) -> Vec<String> {
    if transcript_path.is_empty() || transcript_path == "None" {
        return Vec::new();
    }
    let directory = subagent_directory(transcript_path);
    let Ok(entries) = fs::read_dir(directory) else {
        return Vec::new();
    };
    let mut ids = Vec::new();
    for entry in entries.flatten() {
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if let Some(id) = name.strip_suffix(".jsonl").filter(|id| !id.is_empty()) {
            ids.push(id.to_string());
        }
    }
    ids
}

fn subagent_directory(transcript_path: &str) -> PathBuf {
    let path = Path::new(transcript_path);
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            return parent.join("subagents");
        }
    }
    if let Some(index) = transcript_path.rfind(['\\', '/']) {
        let mut directory = PathBuf::from(&transcript_path[..index]);
        directory.push("subagents");
        return directory;
    }
    path.join("subagents")
}

fn detect_skill_from_transcript(path: &str, user_prompt: &str) -> Option<Vec<SkillHit>> {
    if path.is_empty() || user_prompt.is_empty() {
        return None;
    }
    let content = fs::read_to_string(path).ok()?;
    let mut entries = Vec::new();
    for line in content.lines() {
        if line.trim().is_empty() {
            continue;
        }
        if let Ok(entry) = serde_json::from_str::<Value>(line) {
            entries.push(entry);
        }
    }
    if entries.is_empty() {
        return None;
    }
    let normalized_prompt = normalize_for_match(user_prompt);
    let mut matched_index = None;
    let mut matched_text = String::new();
    for (index, entry) in entries.iter().enumerate() {
        if entry.get("role").and_then(Value::as_str) != Some("user") {
            continue;
        }
        let user_text = extract_user_text(entry);
        let user_query = extract_user_query(&user_text);
        let candidate = normalize_for_match(if user_query.is_empty() {
            user_text.as_str()
        } else {
            user_query.as_str()
        });
        if candidate == normalized_prompt
            || (user_query.is_empty() && candidate.contains(&normalized_prompt))
        {
            matched_index = Some(index);
            matched_text = user_text;
        }
    }
    let matched_index = matched_index?;
    let mut detected: HashMap<String, SkillHit> = HashMap::new();
    for skill in extract_manually_attached_skills(&matched_text) {
        add_detected_skill(&mut detected, skill, "manual_attachment");
    }
    for skill in extract_agent_skill_usage(&matched_text) {
        add_detected_skill(&mut detected, skill, "agent_skill");
    }
    for entry in entries.iter().skip(matched_index + 1) {
        if entry.get("type").and_then(Value::as_str) == Some("turn_ended")
            || entry.get("role").and_then(Value::as_str) == Some("user")
        {
            break;
        }
        if entry.get("role").and_then(Value::as_str) != Some("assistant") {
            continue;
        }
        let Some(content) = entry.pointer("/message/content").and_then(Value::as_array) else {
            continue;
        };
        for block in content {
            if block.get("type").and_then(Value::as_str) != Some("tool_use") {
                continue;
            }
            let name = block.get("name").and_then(Value::as_str).unwrap_or("");
            if name != "Read" && name != "ReadFile" {
                continue;
            }
            let file_path = block
                .pointer("/input/path")
                .or_else(|| block.pointer("/input/file_path"))
                .and_then(Value::as_str)
                .unwrap_or("");
            if let Some(skill) = parse_skill_document_path(file_path) {
                add_detected_skill(&mut detected, skill, "transcript_read");
            }
        }
    }
    if detected.is_empty() {
        None
    } else {
        Some(detected.into_values().collect())
    }
}

fn extract_user_text(entry: &Value) -> String {
    entry
        .pointer("/message/content")
        .and_then(Value::as_array)
        .map(|content| {
            content
                .iter()
                .filter(|block| block.get("type").and_then(Value::as_str) == Some("text"))
                .filter_map(|block| block.get("text").and_then(Value::as_str))
                .collect::<Vec<_>>()
                .join("\n")
        })
        .unwrap_or_default()
}

fn extract_user_query(user_text: &str) -> String {
    USER_QUERY_RE
        .captures(user_text)
        .and_then(|caps| caps.get(1))
        .map(|matched| matched.as_str().trim().to_string())
        .unwrap_or_default()
}

fn extract_manually_attached_skills(user_text: &str) -> Vec<SkillPath> {
    let Some(container) = MANUALLY_ATTACHED_SKILLS_RE
        .captures(user_text)
        .and_then(|caps| caps.get(1))
    else {
        return Vec::new();
    };
    let mut skills = Vec::new();
    for caps in ATTACHED_SKILL_HEADER_RE.captures_iter(container.as_str()) {
        let declared = caps.get(1).map(|m| m.as_str().trim()).unwrap_or("");
        let skill_path = caps.get(2).map(|m| m.as_str().trim()).unwrap_or("");
        if declared.is_empty() || parse_skill_document_path(skill_path).is_none() {
            continue;
        }
        skills.push(SkillPath {
            skill_name: declared.to_string(),
            skill_path: skill_path.to_string(),
        });
    }
    skills
}

fn extract_agent_skill_usage(user_text: &str) -> Vec<SkillPath> {
    if user_text.is_empty() {
        return Vec::new();
    }
    let without_manual = MANUALLY_ATTACHED_CONTAINER_RE.replace_all(user_text, "");
    let metadata = USER_QUERY_CONTAINER_RE.replace_all(&without_manual, "");
    let mut used = Vec::new();
    for caps in AGENT_SKILLS_CONTAINER_RE.captures_iter(&metadata) {
        let body = caps.get(1).map(|matched| matched.as_str()).unwrap_or("");
        let usage_body = strip_available_skills(body);
        used.extend(extract_agent_skill_elements(&usage_body));
    }
    used
}

fn strip_available_skills(body: &str) -> String {
    let mut out = String::new();
    let mut last = 0;
    for caps in AVAILABLE_SKILLS_RE.captures_iter(body) {
        let Some(matched) = caps.get(0) else {
            continue;
        };
        out.push_str(&body[last..matched.start()]);
        last = matched.end();
    }
    out.push_str(&body[last..]);
    out
}

fn extract_agent_skill_elements(text: &str) -> Vec<SkillPath> {
    let mut skills = Vec::new();
    for caps in AGENT_SKILL_RE.captures_iter(text) {
        let attributes = caps.get(1).map(|matched| matched.as_str()).unwrap_or("");
        let Some(path_match) = AGENT_SKILL_FULL_PATH_RE.captures(attributes) else {
            continue;
        };
        let skill_path = path_match
            .get(1)
            .or_else(|| path_match.get(2))
            .map(|matched| matched.as_str().trim())
            .unwrap_or("");
        if let Some(skill) = parse_skill_document_path(skill_path) {
            skills.push(skill);
        }
    }
    skills
}

fn parse_skill_document_path(file_path: &str) -> Option<SkillPath> {
    let skill_path = file_path.trim();
    let caps = SKILL_DOCUMENT_PATH_RE.captures(skill_path)?;
    let name = caps.get(1)?.as_str();
    if name.is_empty() {
        return None;
    }
    Some(SkillPath {
        skill_name: name.to_string(),
        skill_path: skill_path.to_string(),
    })
}

fn add_detected_skill(detected: &mut HashMap<String, SkillHit>, skill: SkillPath, source: &str) {
    let key = format!(
        "{}\0{}",
        skill.skill_name.to_lowercase(),
        skill.skill_path.to_lowercase()
    );
    if let Some(existing) = detected.get_mut(&key) {
        if !existing.detection_sources.iter().any(|item| item == source) {
            existing.detection_sources.push(source.to_string());
        }
        if source == "manual_attachment" {
            existing.detection_source = source.to_string();
        }
        return;
    }
    detected.insert(
        key,
        SkillHit {
            skill_name: skill.skill_name,
            skill_path: skill.skill_path,
            detection_source: source.to_string(),
            detection_sources: vec![source.to_string()],
        },
    );
}

fn normalize_for_match(text: &str) -> String {
    let stripped = TAG_RE.replace_all(text, "");
    WHITESPACE_RE
        .replace_all(&stripped, " ")
        .trim()
        .to_lowercase()
}

fn filter_skills_for_read_injection(
    skills: &[SkillHit],
    assembled_from_transcript: bool,
) -> Vec<SkillHit> {
    skills
        .iter()
        .filter(|skill| {
            let explicit = skill
                .detection_sources
                .iter()
                .any(|source| source == "manual_attachment" || source == "agent_skill");
            let transcript_read = skill
                .detection_sources
                .iter()
                .any(|source| source == "transcript_read");
            if !assembled_from_transcript {
                explicit || transcript_read
            } else {
                explicit && !transcript_read
            }
        })
        .cloned()
        .collect()
}

fn inject_skill_records(records: &mut Vec<Obj>, skills: &[SkillHit]) {
    let Some(target) = records
        .iter()
        .position(|record| field_str(record, "event.name") == "llm.response")
    else {
        return;
    };
    let skill_entries = skills
        .iter()
        .map(|skill| (skill.clone(), uuid::Uuid::new_v4().to_string()))
        .collect::<Vec<_>>();
    let output = records[target]
        .get("gen_ai.output.messages")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let source_ids = tool_call_ids(&output);
    let source_had_tool_calls = output.iter().any(|message| {
        message.get("role").and_then(Value::as_str) == Some("assistant")
            && message
                .get("parts")
                .and_then(Value::as_array)
                .is_some_and(|parts| {
                    parts
                        .iter()
                        .any(|part| part.get("type").and_then(Value::as_str) == Some("tool_call"))
                })
    });
    let mut output = output;
    if !output
        .iter()
        .any(|message| message.get("role").and_then(Value::as_str) == Some("assistant"))
    {
        output.push(json!({ "role": "assistant", "parts": [] }));
    }
    if let Some(assistant) = output
        .iter_mut()
        .find(|message| message.get("role").and_then(Value::as_str) == Some("assistant"))
    {
        let Some(object) = assistant.as_object_mut() else {
            return;
        };
        let parts = object
            .entry("parts")
            .or_insert_with(|| Value::Array(Vec::new()));
        let Some(parts) = parts.as_array_mut() else {
            return;
        };
        for (skill, tool_call_id) in &skill_entries {
            parts.push(json!({
                "type": "tool_call",
                "id": tool_call_id,
                "name": "Read",
                "arguments": { "path": skill.skill_path },
            }));
        }
    }
    records[target].insert("gen_ai.output.messages".into(), Value::Array(output));
    records[target] = sanitize_record(records[target].clone());

    let call_parts = skill_entries
        .iter()
        .map(|(skill, tool_call_id)| {
            json!({
                "type": "tool_call",
                "id": tool_call_id,
                "name": "Read",
                "arguments": { "path": skill.skill_path },
            })
        })
        .collect::<Vec<_>>();
    let response_parts = skill_entries
        .iter()
        .map(|(_, tool_call_id)| {
            json!({
                "type": "tool_call_response",
                "id": tool_call_id,
                "response": "",
            })
        })
        .collect::<Vec<_>>();
    let target_trace = field_str(&records[target], "trace_id").to_string();
    let target_turn = field_str(&records[target], "gen_ai.turn.id").to_string();
    let later_requests = records
        .iter()
        .enumerate()
        .skip(target + 1)
        .filter_map(|(index, record)| {
            if field_str(record, "event.name") != "llm.request" {
                return None;
            }
            if field_str(record, "gen_ai.agent.scope") == "subagent" {
                return None;
            }
            if !target_trace.is_empty() && field_str(record, "trace_id") != target_trace {
                return None;
            }
            if !target_turn.is_empty() && field_str(record, "gen_ai.turn.id") != target_turn {
                return None;
            }
            Some(index)
        })
        .collect::<Vec<_>>();
    for (offset, index) in later_requests.iter().copied().enumerate() {
        if offset == 0 {
            let merged = merge_skill_exchange(
                records[index].get("gen_ai.input.messages_delta"),
                &source_ids,
                source_had_tool_calls,
                &call_parts,
                &response_parts,
            );
            records[index].insert("gen_ai.input.messages_delta".into(), merged);
        }
        let merged = merge_skill_exchange(
            records[index].get("gen_ai.input.messages"),
            &source_ids,
            source_had_tool_calls,
            &call_parts,
            &response_parts,
        );
        records[index].insert("gen_ai.input.messages".into(), merged);
        records[index] = sanitize_record(records[index].clone());
    }

    let llm = &records[target];
    let base_time = parse_i128(llm.get("time_unix_nano"));
    let base_observed = llm
        .get("observed_time_unix_nano")
        .map(|value| parse_i128(Some(value)))
        .unwrap_or(base_time);
    let agent_type = field_str(llm, "gen_ai.agent.type").to_string();
    let cwd_key = format!("agent.{agent_type}.cwd");
    let cwd = llm.get(&cwd_key).cloned();
    let trace_id = field_str(llm, "trace_id").to_string();
    let session_id = field_str(llm, "gen_ai.session.id").to_string();
    let turn_id = field_str(llm, "gen_ai.turn.id").to_string();
    let step_id = {
        let step = field_str(llm, "gen_ai.step.id");
        if step.is_empty() {
            "step_1".to_string()
        } else {
            step.to_string()
        }
    };
    let user_id = field_str(llm, "user.id").to_string();
    let identity = SkillIdentity {
        trace_id: &trace_id,
        session_id: &session_id,
        turn_id: &turn_id,
        step_id: &step_id,
        agent_type: &agent_type,
        user_id: &user_id,
        cwd: cwd.as_ref(),
        cwd_key: &cwd_key,
    };
    let mut inserted = Vec::new();
    for (index, (skill, tool_call_id)) in skill_entries.iter().enumerate() {
        let call_offset = (index as i128) * 2 + 1;
        let result_offset = call_offset + 1;
        inserted.push(skill_tool_record(
            &identity,
            base_time + call_offset,
            base_observed + call_offset,
            "tool.call",
            tool_call_id,
            skill,
            true,
        ));
        inserted.push(skill_tool_record(
            &identity,
            base_time + result_offset,
            base_observed + result_offset,
            "tool.result",
            tool_call_id,
            skill,
            false,
        ));
    }
    let tail = records.split_off(target + 1);
    records.extend(inserted);
    records.extend(tail);
}

struct SkillIdentity<'a> {
    trace_id: &'a str,
    session_id: &'a str,
    turn_id: &'a str,
    step_id: &'a str,
    agent_type: &'a str,
    user_id: &'a str,
    cwd: Option<&'a Value>,
    cwd_key: &'a str,
}

fn skill_tool_record(
    identity: &SkillIdentity<'_>,
    time_nano: i128,
    observed_nano: i128,
    event_name: &str,
    tool_call_id: &str,
    skill: &SkillHit,
    include_arguments: bool,
) -> Obj {
    let SkillIdentity {
        trace_id,
        session_id,
        turn_id,
        step_id,
        agent_type,
        user_id,
        cwd,
        cwd_key,
    } = *identity;
    let mut record = Obj::new();
    record.insert("trace_id".into(), Value::String(trace_id.to_string()));
    record.insert(
        "gen_ai.session.id".into(),
        Value::String(session_id.to_string()),
    );
    record.insert("gen_ai.turn.id".into(), Value::String(turn_id.to_string()));
    record.insert("gen_ai.step.id".into(), Value::String(step_id.to_string()));
    record.insert(
        "gen_ai.agent.type".into(),
        Value::String(agent_type.to_string()),
    );
    record.insert("user.id".into(), Value::String(user_id.to_string()));
    if let Some(cwd) = cwd {
        record.insert(cwd_key.to_string(), cwd.clone());
    }
    record.insert(
        "time_unix_nano".into(),
        Value::String(time_nano.to_string()),
    );
    record.insert(
        "observed_time_unix_nano".into(),
        Value::String(observed_nano.to_string()),
    );
    record.insert(
        "event.id".into(),
        Value::String(uuid::Uuid::new_v4().to_string()),
    );
    record.insert("event.name".into(), Value::String(event_name.to_string()));
    record.insert("gen_ai.tool.name".into(), Value::String("Read".into()));
    record.insert(
        "gen_ai.tool.call.id".into(),
        Value::String(tool_call_id.to_string()),
    );
    if include_arguments {
        record.insert(
            "gen_ai.tool.call.arguments".into(),
            json!({ "path": skill.skill_path }),
        );
    }
    record.insert(
        "gen_ai.skill.name".into(),
        Value::String(skill.skill_name.clone()),
    );
    record.insert(
        "gen_ai.skill.id".into(),
        Value::String(skill.skill_name.clone()),
    );
    record.insert(
        "agent.cursor.skill_detection_source".into(),
        Value::String(
            if skill.detection_source.is_empty() {
                "transcript_post_assembly"
            } else {
                skill.detection_source.as_str()
            }
            .to_string(),
        ),
    );
    sanitize_record(record)
}

fn tool_call_ids(messages: &[Value]) -> HashSet<String> {
    let mut ids = HashSet::new();
    for message in messages {
        if message.get("role").and_then(Value::as_str) != Some("assistant") {
            continue;
        }
        let Some(parts) = message.get("parts").and_then(Value::as_array) else {
            continue;
        };
        for part in parts {
            if part.get("type").and_then(Value::as_str) == Some("tool_call") {
                if let Some(id) = part
                    .get("id")
                    .and_then(Value::as_str)
                    .filter(|id| !id.is_empty())
                {
                    ids.insert(id.to_string());
                }
            }
        }
    }
    ids
}

fn merge_skill_exchange(
    messages: Option<&Value>,
    source_ids: &HashSet<String>,
    source_had_tool_calls: bool,
    call_parts: &[Value],
    response_parts: &[Value],
) -> Value {
    let mut merged = messages
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let existing_ids = {
        let mut ids = HashSet::new();
        for message in &merged {
            if let Some(parts) = message.get("parts").and_then(Value::as_array) {
                for part in parts {
                    if let Some(id) = part
                        .get("id")
                        .and_then(Value::as_str)
                        .filter(|id| !id.is_empty())
                    {
                        ids.insert(id.to_string());
                    }
                }
            }
        }
        ids
    };
    let pending = call_parts
        .iter()
        .filter(|part| {
            part.get("id")
                .and_then(Value::as_str)
                .map(|id| !existing_ids.contains(id))
                .unwrap_or(true)
        })
        .cloned()
        .collect::<Vec<_>>();
    if pending.is_empty() {
        return Value::Array(merged);
    }
    let responses = response_parts
        .iter()
        .filter_map(|part| {
            part.get("id")
                .and_then(Value::as_str)
                .map(|id| (id.to_string(), part.clone()))
        })
        .collect::<HashMap<_, _>>();
    let response_messages = pending
        .iter()
        .filter_map(|call| {
            let id = call.get("id").and_then(Value::as_str)?;
            let response = responses.get(id)?.clone();
            Some(json!({ "role": "tool", "parts": [response] }))
        })
        .collect::<Vec<_>>();
    let mut assistant_index = merged.iter().position(|message| {
        message.get("role").and_then(Value::as_str) == Some("assistant")
            && message
                .get("parts")
                .and_then(Value::as_array)
                .is_some_and(|parts| {
                    parts.iter().any(|part| {
                        part.get("id")
                            .and_then(Value::as_str)
                            .is_some_and(|id| source_ids.contains(id))
                    })
                })
    });
    if assistant_index.is_none() && source_had_tool_calls {
        assistant_index = merged
            .iter()
            .position(|message| message.get("role").and_then(Value::as_str) == Some("assistant"));
    }
    if let Some(assistant_index) = assistant_index {
        if let Some(parts) = merged[assistant_index]
            .get_mut("parts")
            .and_then(Value::as_array_mut)
        {
            parts.extend(pending);
        } else if let Some(object) = merged[assistant_index].as_object_mut() {
            object.insert("parts".into(), Value::Array(pending));
        }
        let mut insert_at = assistant_index + 1;
        while insert_at < merged.len()
            && merged[insert_at].get("role").and_then(Value::as_str) == Some("tool")
        {
            insert_at += 1;
        }
        let tail = merged.split_off(insert_at);
        merged.extend(response_messages);
        merged.extend(tail);
    } else {
        let first_history = merged.iter().position(|message| {
            matches!(
                message.get("role").and_then(Value::as_str),
                Some("assistant" | "tool")
            )
        });
        let insert_at = first_history.unwrap_or(merged.len());
        let tail = merged.split_off(insert_at);
        merged.push(json!({ "role": "assistant", "parts": pending }));
        merged.extend(response_messages);
        merged.extend(tail);
    }
    Value::Array(merged)
}

fn repair_windows_json(raw: &str) -> String {
    let step = WINDOWS_JSON_COMMA_RE.replace_all(raw, "\",");
    let step = WINDOWS_JSON_OBJECT_RE.replace_all(&step, "\"}");
    WINDOWS_JSON_ARRAY_RE.replace_all(&step, "\"]").into_owned()
}

fn env_says_windows(env: &Env) -> bool {
    for key in ["LOONGSUITE_CURSOR_PLATFORM", "LOONGSUITE_PLATFORM", "OS"] {
        if let Some(value) = env.get(key).map(|value| value.trim()) {
            if value.eq_ignore_ascii_case("win32")
                || value.eq_ignore_ascii_case("windows")
                || value.eq_ignore_ascii_case("Windows_NT")
            {
                return true;
            }
        }
    }
    false
}

fn path_says_windows(path: &str) -> bool {
    let bytes = path.as_bytes();
    if bytes.starts_with(br"\\") {
        return true;
    }
    for index in 0..bytes.len().saturating_sub(2) {
        if bytes[index].is_ascii_alphabetic()
            && bytes[index + 1] == b':'
            && matches!(bytes.get(index + 2), Some(b'\\' | b'/'))
            && (index == 0 || !bytes[index - 1].is_ascii_alphanumeric())
        {
            return true;
        }
    }
    false
}

fn base_fields(
    trace_id: &str,
    conversation_id: &str,
    turn_id: &str,
    variant: &str,
    user_id: &str,
    workspace: Option<&str>,
    include_agent_id: bool,
) -> Obj {
    let mut base = Obj::new();
    base.insert("trace_id".into(), Value::String(trace_id.to_string()));
    base.insert(
        "gen_ai.session.id".into(),
        Value::String(conversation_id.to_string()),
    );
    base.insert("gen_ai.turn.id".into(), Value::String(turn_id.to_string()));
    base.insert(
        "gen_ai.agent.type".into(),
        Value::String(variant.to_string()),
    );
    if include_agent_id {
        base.insert(
            "gen_ai.agent.id".into(),
            Value::String(conversation_id.to_string()),
        );
    }
    base.insert("user.id".into(), Value::String(user_id.to_string()));
    if let Some(workspace) = workspace.filter(|path| !path.is_empty()) {
        base.insert(
            format!("agent.{variant}.cwd"),
            Value::String(workspace.to_string()),
        );
    }
    base
}

fn resolve_workspace_path(events: &[Value]) -> Option<String> {
    resolve_workspace_path_refs(&events.iter().collect::<Vec<_>>())
}

fn resolve_workspace_path_refs(events: &[&Value]) -> Option<String> {
    for event in events {
        if let Some(cwd) = event_str(event, "cwd") {
            return Some(cwd.to_string());
        }
    }
    for event in events {
        if let Some(root) = normalize_roots(event.get("workspace_roots"))
            .into_iter()
            .next()
        {
            return Some(root);
        }
    }
    None
}

fn normalize_roots(value: Option<&Value>) -> Vec<String> {
    match value {
        Some(Value::Array(items)) => items
            .iter()
            .filter_map(|item| item.as_str())
            .map(str::trim)
            .filter(|item| !item.is_empty())
            .map(str::to_owned)
            .collect(),
        Some(Value::String(text)) => {
            if let Ok(Value::Array(items)) = serde_json::from_str::<Value>(text) {
                return normalize_roots(Some(&Value::Array(items)));
            }
            let root = text.trim();
            if root.is_empty() {
                Vec::new()
            } else {
                vec![root.to_string()]
            }
        }
        _ => Vec::new(),
    }
}

fn timed_identity(nanos: &str, event_name: &str) -> Obj {
    let mut fields = Obj::new();
    fields.insert("time_unix_nano".into(), Value::String(nanos.to_string()));
    fields.insert(
        "observed_time_unix_nano".into(),
        Value::String(nanos.to_string()),
    );
    fields.insert(
        "event.id".into(),
        Value::String(uuid::Uuid::new_v4().to_string()),
    );
    fields.insert("event.name".into(), Value::String(event_name.to_string()));
    fields
}

fn finish_record(base: &Obj, fields: Obj) -> Obj {
    let mut record = base.clone();
    for (key, value) in fields {
        record.insert(key, value);
    }
    sanitize_record(record)
}

fn user_text_message(text: &str) -> Value {
    json!({
        "role": "user",
        "parts": [{ "type": "text", "content": text }],
    })
}

fn derive_trace_id(turn_id: &str) -> String {
    if turn_id.is_empty() {
        return uuid::Uuid::new_v4().simple().to_string();
    }
    super::super::common::sha256_text_prefix(&format!("cursor:{turn_id}"), 32)
}

fn resolve_model(raw: Option<&str>) -> String {
    match raw.map(str::trim).filter(|model| !model.is_empty()) {
        None | Some("default") | Some("unknown") => "composer-2.5".to_string(),
        Some(model) => model.to_string(),
    }
}

fn infer_provider(model: &str) -> String {
    let model_lower = model.to_lowercase();
    let provider = if model_lower.contains("claude") || model_lower.contains("anthropic") {
        "anthropic"
    } else if model_lower.contains("gpt")
        || model_lower.contains("openai")
        || model_lower.contains("codex")
    {
        "openai"
    } else if model_lower.contains("qwen") || model_lower.contains("tongyi") {
        "qwen"
    } else if model_lower.contains("deepseek") {
        "deepseek"
    } else if model_lower.contains("gemini") {
        "gcp.gemini"
    } else if model_lower.contains("grok")
        || model_lower.contains("xai")
        || model_lower.contains("x_ai")
    {
        "x_ai"
    } else {
        "unknown"
    };
    if provider == "unknown" && model_lower.contains("composer") {
        "openai".to_string()
    } else {
        provider.to_string()
    }
}

fn is_subagent_tool(name: Option<&str>) -> bool {
    matches!(name, Some("Subagent" | "Task"))
}

fn hook_name(event: &Value) -> &str {
    event
        .get("hook_event")
        .and_then(Value::as_str)
        .unwrap_or("")
}

fn conversation_of(event: &Value) -> &str {
    event
        .get("conversation_id")
        .and_then(Value::as_str)
        .unwrap_or("")
}

fn event_str<'a>(event: &'a Value, key: &str) -> Option<&'a str> {
    event
        .get(key)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
}

fn field_str<'a>(record: &'a Obj, key: &str) -> &'a str {
    record.get(key).and_then(Value::as_str).unwrap_or("")
}

fn copy_number(record: &mut Obj, key: &str, value: Option<&Value>) {
    if finite_f64(value).is_some() {
        if let Some(value) = value {
            record.insert(key.to_string(), value.clone());
        }
    }
}

fn parse_maybe_json(value: &Value) -> Option<Value> {
    match value {
        Value::Null => None,
        Value::String(text) => {
            let trimmed = text.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(serde_json::from_str(trimmed).unwrap_or_else(|_| Value::String(text.clone())))
            }
        }
        other => Some(other.clone()),
    }
}

fn to_json_value(value: &Value) -> Option<Value> {
    match value {
        Value::Null => None,
        Value::String(_) | Value::Number(_) | Value::Bool(_) => Some(value.clone()),
        Value::Array(items) => {
            let list = items.iter().filter_map(to_json_value).collect::<Vec<_>>();
            if list.is_empty() {
                None
            } else {
                Some(Value::Array(list))
            }
        }
        Value::Object(map) => {
            let mut out = Map::new();
            for (key, item) in map {
                if let Some(json) = to_json_value(item) {
                    out.insert(key.clone(), json);
                }
            }
            if out.is_empty() {
                None
            } else {
                Some(Value::Object(out))
            }
        }
    }
}

fn stringify_value(value: &Value) -> String {
    match value {
        Value::Null => String::new(),
        Value::String(text) => text.clone(),
        other => serde_json::to_string(other).unwrap_or_default(),
    }
}

fn event_ts_nanos(event: &Value) -> String {
    match event.get("_journal_ts") {
        Some(Value::String(text)) if !text.is_empty() => timestamp_from_string(text),
        Some(Value::Number(number)) => number
            .as_i64()
            .map(nanos_from_unix_millis)
            .or_else(|| number.as_f64().map(nanos_from_ms))
            .unwrap_or_else(now_nanos),
        _ => now_nanos(),
    }
}

fn timestamp_from_string(text: &str) -> String {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return now_nanos();
    }
    if trimmed.len() >= 16 && trimmed.chars().all(|ch| ch.is_ascii_digit()) {
        return trimmed.to_string();
    }
    if let Ok(integer) = trimmed.parse::<i64>() {
        return nanos_from_unix_millis(integer);
    }
    if let Ok(number) = trimmed.parse::<f64>() {
        if number.is_finite() {
            return nanos_from_ms(number);
        }
    }
    if let Some(ms) = parse_iso_ms(trimmed) {
        return nanos_from_unix_millis(ms);
    }
    now_nanos()
}

fn stamp_to_nanos(stamp: &Stamp) -> String {
    match stamp {
        Stamp::Iso(text) => {
            if text.is_empty() {
                now_nanos()
            } else {
                timestamp_from_string(text)
            }
        }
        Stamp::EpochMs(ms) => nanos_from_ms(*ms),
    }
}

fn stamp_to_ms(stamp: &Stamp) -> Option<f64> {
    match stamp {
        Stamp::EpochMs(ms) if ms.is_finite() => Some(*ms),
        Stamp::Iso(text) => parse_iso_ms(text).map(|ms| ms as f64),
        _ => None,
    }
}

fn nanos_from_unix_millis(ms: i64) -> String {
    if ms >= 10_000_000_000_000_000 {
        ms.to_string()
    } else if ms >= 1_000_000_000_000 {
        format!("{ms}000000")
    } else {
        format!("{}000000", ms.saturating_mul(1000))
    }
}

fn nanos_from_ms(ms: f64) -> String {
    if !ms.is_finite() {
        return now_nanos();
    }
    if ms >= 1e16 {
        return trunc_digits(ms);
    }
    if ms >= 1e12 {
        return format!("{}000000", trunc_digits(ms));
    }
    format!("{}000000", trunc_digits(ms * 1000.0))
}

fn trunc_digits(value: f64) -> String {
    format!("{:.0}", value.trunc())
}

fn now_nanos() -> String {
    nanos_from_unix_millis(Utc::now().timestamp_millis())
}

fn parse_iso_ms(text: &str) -> Option<i64> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return None;
    }
    DateTime::parse_from_rfc3339(trimmed)
        .ok()
        .map(|parsed| parsed.timestamp_millis())
}

fn ts_ms_value(event: &Value) -> Option<i64> {
    match event.get("_journal_ts") {
        Some(Value::String(text)) => parse_iso_ms(text),
        Some(Value::Number(number)) => number
            .as_i64()
            .or_else(|| number.as_f64().map(|ms| ms as i64)),
        _ => None,
    }
}

fn ts_ms_sort(event: &Value) -> i64 {
    ts_ms_value(event).unwrap_or(0)
}

fn parse_i128(value: Option<&Value>) -> i128 {
    value
        .and_then(Value::as_str)
        .and_then(|text| text.parse().ok())
        .unwrap_or(0)
}

fn iso_minus_millis(iso: &str, duration_ms: i64) -> Option<String> {
    let parsed = DateTime::parse_from_rfc3339(iso).ok()?;
    let start = parsed.timestamp_millis().checked_sub(duration_ms)?;
    DateTime::<Utc>::from_timestamp_millis(start)
        .map(|time| time.to_rfc3339_opts(SecondsFormat::Millis, true))
}

#[cfg(test)]
mod tests {
    use super::super::super::common::{Env, HookRequest};
    use super::super::run;
    use super::*;
    use std::io::Write;
    use std::path::Path;

    fn invoke(root: &Path, payload: &Value, extra: &[(&str, &str)]) {
        let mut env = Env::new();
        env.insert(
            "LOONGSUITE_PILOT_DATA_DIR".into(),
            root.to_string_lossy().into_owned(),
        );
        for (key, value) in extra {
            env.insert((*key).to_string(), (*value).to_string());
        }
        let stdin = serde_json::to_vec(payload).unwrap();
        let mut stdout = Vec::new();
        let mut request = HookRequest {
            args: &[],
            stdin: &stdin,
            env: &env,
            stdout: &mut stdout,
        };
        assert_eq!(run(&mut request), 0);
        assert_eq!(String::from_utf8(stdout).unwrap().trim(), "{}");
    }

    fn history(root: &Path) -> Vec<Value> {
        let dir = root.join("logs").join("cursor").join("history");
        let Ok(entries) = fs::read_dir(dir) else {
            return Vec::new();
        };
        let mut records = Vec::new();
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().and_then(|ext| ext.to_str()) != Some("jsonl") {
                continue;
            }
            let content = fs::read_to_string(path).unwrap_or_default();
            for line in content.lines() {
                if let Ok(value) = serde_json::from_str::<Value>(line.trim()) {
                    records.push(value);
                }
            }
        }
        records
    }

    fn write_jsonl(path: &Path, rows: &[Value]) {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).unwrap();
        }
        let mut body = String::new();
        for row in rows {
            body.push_str(&serde_json::to_string(row).unwrap());
            body.push('\n');
        }
        let mut file = fs::File::create(path).unwrap();
        file.write_all(body.as_bytes()).unwrap();
    }

    #[test]
    fn subagent_transcript_directory_nests_child_session() {
        let root = tempfile::tempdir().unwrap();
        let transcripts = root.path().join("transcripts");
        let parent_id = "parent-conv-1111";
        let child_id = "child-conv-2222";
        let parent_transcript = transcripts.join(format!("{parent_id}.jsonl"));
        write_jsonl(&parent_transcript, &[json!({ "role": "user" })]);
        write_jsonl(
            &transcripts
                .join("subagents")
                .join(format!("{child_id}.jsonl")),
            &[json!({ "role": "assistant" })],
        );
        let transcript = parent_transcript.to_string_lossy().into_owned();
        let data = root.path().join("data");
        invoke(
            &data,
            &json!({
                "hook_event_name": "beforeSubmitPrompt",
                "conversation_id": parent_id,
                "generation_id": "turn-parent",
                "prompt": "delegate the search",
                "model": "gpt-5",
                "cursor_version": "1.7.0",
                "transcript_path": transcript,
            }),
            &[],
        );
        invoke(
            &data,
            &json!({
                "hook_event_name": "preToolUse",
                "conversation_id": parent_id,
                "generation_id": "turn-parent",
                "tool_name": "Subagent",
                "tool_use_id": "tool-sub-1",
                "tool_input": { "task": "search" },
                "cursor_version": "1.7.0",
            }),
            &[],
        );
        invoke(
            &data,
            &json!({
                "hook_event_name": "afterAgentResponse",
                "conversation_id": child_id,
                "generation_id": "turn-child",
                "text": "found it",
                "model": "gpt-5",
                "cursor_version": "1.7.0",
            }),
            &[],
        );
        invoke(
            &data,
            &json!({
                "hook_event_name": "afterAgentResponse",
                "conversation_id": parent_id,
                "generation_id": "turn-parent",
                "text": "parent finished",
                "model": "gpt-5",
                "cursor_version": "1.7.0",
            }),
            &[],
        );
        invoke(
            &data,
            &json!({
                "hook_event_name": "stop",
                "conversation_id": parent_id,
                "generation_id": "turn-parent",
                "status": "completed",
                "cursor_version": "1.7.0",
                "transcript_path": transcript,
            }),
            &[],
        );

        let records = history(&data);
        let child = records
            .iter()
            .find(|record| {
                record.get("gen_ai.agent.scope").and_then(Value::as_str) == Some("subagent")
            })
            .expect("nested subagent record");
        assert_eq!(child["gen_ai.agent.id"], child_id);
        assert_eq!(child["gen_ai.agent.parent.id"], parent_id);
        assert_eq!(child["gen_ai.subagent.parent_tool_call.id"], "tool-sub-1");
        assert_eq!(
            child["agent.cursor.subagent.link_confidence"],
            "transcript_dir"
        );
        assert!(records.iter().any(|record| {
            record["event.name"] == "tool.call"
                && record["gen_ai.tool.name"] == "Subagent"
                && record["gen_ai.tool.call.id"] == "tool-sub-1"
        }));
        assert!(records.iter().any(|record| {
            record.get("gen_ai.agent.scope").and_then(Value::as_str) == Some("subagent")
                && record["event.name"] == "llm.response"
                && record.to_string().contains("found it")
        }));
    }

    #[test]
    fn skill_synthesis_injects_manual_attachment_read() {
        let root = tempfile::tempdir().unwrap();
        let transcript = root.path().join("skill-transcript.jsonl");
        let prompt = "please use demo-skill";
        let skill_path = "/Users/test/.cursor/skills/demo-skill/SKILL.md";
        let user_text = format!(
            "<manually_attached_skills>\nSkill Name: demo-skill\nPath: {skill_path}\nSKILL.md content:\nbody\n</manually_attached_skills>\n<user_query>\n{prompt}\n</user_query>"
        );
        write_jsonl(
            &transcript,
            &[json!({
                "role": "user",
                "message": { "content": [{ "type": "text", "text": user_text }] }
            })],
        );
        let transcript_path = transcript.to_string_lossy().into_owned();
        let data = root.path().join("data");
        invoke(
            &data,
            &json!({
                "hook_event_name": "beforeSubmitPrompt",
                "conversation_id": "skill-conv",
                "generation_id": "skill-turn",
                "prompt": prompt,
                "model": "gpt-5",
                "cursor_version": "1.7.0",
                "transcript_path": transcript_path,
            }),
            &[],
        );
        invoke(
            &data,
            &json!({
                "hook_event_name": "afterAgentResponse",
                "conversation_id": "skill-conv",
                "generation_id": "skill-turn",
                "text": "done",
                "model": "gpt-5",
                "cursor_version": "1.7.0",
            }),
            &[],
        );
        invoke(
            &data,
            &json!({
                "hook_event_name": "stop",
                "conversation_id": "skill-conv",
                "generation_id": "skill-turn",
                "status": "completed",
                "cursor_version": "1.7.0",
                "transcript_path": transcript_path,
            }),
            &[],
        );

        let records = history(&data);
        let tool_call = records
            .iter()
            .find(|record| {
                record["event.name"] == "tool.call" && record["gen_ai.tool.name"] == "Read"
            })
            .expect("synthetic skill read");
        assert_eq!(tool_call["gen_ai.skill.name"], "demo-skill");
        assert_eq!(tool_call["gen_ai.skill.id"], "demo-skill");
        assert_eq!(tool_call["gen_ai.tool.call.arguments"]["path"], skill_path);
        assert_eq!(
            tool_call["agent.cursor.skill_detection_source"],
            "manual_attachment"
        );
        let tool_result = records
            .iter()
            .find(|record| {
                record["event.name"] == "tool.result" && record["gen_ai.tool.name"] == "Read"
            })
            .expect("synthetic skill result");
        assert_eq!(
            tool_result["gen_ai.tool.call.id"],
            tool_call["gen_ai.tool.call.id"]
        );
        let response = records
            .iter()
            .find(|record| record["event.name"] == "llm.response")
            .unwrap();
        let parts = response["gen_ai.output.messages"][0]["parts"]
            .as_array()
            .unwrap();
        assert!(parts.iter().any(|part| {
            part["type"] == "tool_call"
                && part["name"] == "Read"
                && part["id"] == tool_call["gen_ai.tool.call.id"]
        }));
    }

    #[test]
    fn windows_transcript_recovery_uses_utf8_turn_text() {
        let root = tempfile::tempdir().unwrap();
        let transcript = root.path().join("win-transcript.jsonl");
        write_jsonl(
            &transcript,
            &[
                json!({
                    "role": "user",
                    "message": { "content": [{ "type": "text", "text": "<user_query>\nold turn\n</user_query>" }] }
                }),
                json!({
                    "role": "assistant",
                    "message": { "content": [{ "type": "text", "text": "old answer" }] }
                }),
                json!({ "type": "turn_ended" }),
                json!({
                    "role": "user",
                    "message": { "content": [{ "type": "text", "text": "<user_query>\n正确的中文\n</user_query>" }] }
                }),
                json!({
                    "role": "assistant",
                    "message": { "content": [{ "type": "text", "text": "回复正文" }] }
                }),
                json!({ "type": "turn_ended" }),
            ],
        );
        let transcript_path = transcript.to_string_lossy().into_owned();
        let data = root.path().join("data");
        let windows = [("LOONGSUITE_CURSOR_PLATFORM", "win32")];
        invoke(
            &data,
            &json!({
                "hook_event_name": "beforeSubmitPrompt",
                "conversation_id": "win-conv",
                "generation_id": "win-turn",
                "prompt": "????",
                "model": "gpt-5",
                "cursor_version": "1.7.0",
                "transcript_path": transcript_path,
            }),
            &windows,
        );
        invoke(
            &data,
            &json!({
                "hook_event_name": "afterAgentResponse",
                "conversation_id": "win-conv",
                "generation_id": "win-turn",
                "text": "???",
                "model": "gpt-5",
                "cursor_version": "1.7.0",
            }),
            &windows,
        );
        invoke(
            &data,
            &json!({
                "hook_event_name": "stop",
                "conversation_id": "win-conv",
                "generation_id": "win-turn",
                "status": "completed",
                "cursor_version": "1.7.0",
                "transcript_path": transcript_path,
            }),
            &windows,
        );

        let records = history(&data);
        let other = records
            .iter()
            .find(|record| record["event.name"] == "other")
            .expect("user record");
        let delta = other["gen_ai.input.messages_delta"].to_string();
        assert!(delta.contains("正确的中文"));
        assert!(!delta.contains("????"));
        assert!(!delta.contains("old turn"));
        let response = records
            .iter()
            .find(|record| record["event.name"] == "llm.response")
            .unwrap();
        let output = response["gen_ai.output.messages"].to_string();
        assert!(output.contains("回复正文"));
        assert!(!output.contains("old answer"));
        assert_eq!(response["gen_ai.agent.id"], "win-conv");
    }

    #[test]
    fn repairs_windows_question_mark_json_only_for_windows_context() {
        let raw = r#"{"prompt":"中文"?, "ok":true}"#;
        let mut env = Env::new();
        env.insert("LOONGSUITE_CURSOR_PLATFORM".into(), "win32".into());
        let repaired = repaired_windows_object(raw, &env).expect("repaired object");
        assert_eq!(repaired.get("prompt").and_then(Value::as_str), Some("中文"));
        assert_eq!(repaired.get("ok").and_then(Value::as_bool), Some(true));
        #[cfg(not(windows))]
        {
            assert!(repaired_windows_object(raw, &Env::new()).is_none());
            assert!(windows_context(&Env::new(), r"C:\Users\a\transcript.jsonl"));
            assert!(!windows_context(&Env::new(), "/tmp/transcript.jsonl"));
        }
    }
}
