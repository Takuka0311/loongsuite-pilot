use super::*;
use crate::inputs::codex_transcript::{CodexTranscriptInput, CodexTranscriptInputOptions};
use std::sync::{Arc, Mutex};

struct Fixture {
    root: Arc<tempfile::TempDir>,
    input: CodexTranscriptInput,
    entries: Arc<Mutex<Vec<AgentActivityEntry>>>,
    batches: Arc<Mutex<Vec<usize>>>,
}
impl Fixture {
    fn new() -> Self {
        Self::with_root(Arc::new(tempfile::tempdir().expect("tempdir")))
    }
    fn with_root(root: Arc<tempfile::TempDir>) -> Self {
        let input = CodexTranscriptInput::new(CodexTranscriptInputOptions {
            state_store: Arc::new(crate::checkpoints::StateStore::new(
                root.path().join("state.json"),
            )),
            session_dir: Some(root.path().join("sessions").to_string_lossy().into_owned()),
            wakeup_dir: Some(root.path().join("wakeups").to_string_lossy().into_owned()),
            span_context_dir: Some(root.path().join("spans").to_string_lossy().into_owned()),
            poll_interval_ms: Some(60_000),
            multimodal: None,
        });
        let entries = Arc::new(Mutex::new(Vec::new()));
        let batches = Arc::new(Mutex::new(Vec::new()));
        let out = entries.clone();
        let sizes = batches.clone();
        input.on_entries(move |batch| {
            out.lock().expect("entries").extend_from_slice(batch);
            sizes.lock().expect("batches").push(batch.len());
        });
        Self {
            root,
            input,
            entries,
            batches,
        }
    }
    async fn append(&self, path: &str, text: &str) {
        use tokio::io::AsyncWriteExt;
        let mut file = tokio::fs::OpenOptions::new()
            .append(true)
            .open(path)
            .await
            .expect("append open");
        file.write_all(text.as_bytes()).await.expect("append bytes");
        file.flush().await.expect("append durable visibility");
    }
    async fn restart(&self) -> Self {
        self.rt().state_store.save().await.expect("durable save");
        let next = Self::with_root(self.root.clone());
        next.rt().state_store.load().await;
        next
    }
    fn checkpoint(&self, path: &str) -> CodexTranscriptCheckpoint {
        self.engine()
            .read_checkpoint(self.rt(), &self.engine().state_key(path))
            .expect("native checkpoint")
    }
    fn engine(&self) -> CodexTranscriptEngine<'_> {
        CodexTranscriptEngine {
            body: &self.input.inner.body,
        }
    }
    fn rt(&self) -> &InputRuntime {
        &self.input.inner.runtime
    }
    async fn write(&self, name: &str, text: &str) -> String {
        let p = self.root.path().join("sessions").join(name);
        tokio::fs::create_dir_all(p.parent().expect("parent"))
            .await
            .expect("dir");
        tokio::fs::write(&p, text).await.expect("fixture");
        p.to_string_lossy().into_owned()
    }
    async fn process(&self, path: &str) -> usize {
        self.engine()
            .process_file(self.rt(), path)
            .await
            .expect("production process")
    }
    fn records(&self) -> Vec<Value> {
        self.entries
            .lock()
            .expect("entries")
            .iter()
            .map(|e| serde_json::to_value(e).expect("entry"))
            .collect()
    }
    async fn marker(&self, folder: &str, name: &str, payload: Value) {
        let dir = self.root.path().join(folder);
        tokio::fs::create_dir_all(&dir).await.expect("marker dir");
        tokio::fs::write(dir.join(name), payload.to_string())
            .await
            .expect("marker");
    }
}
fn record(at: &str, kind: &str, payload: Value) -> String {
    json!({"timestamp":at,"type":kind,"payload":payload}).to_string()
}
fn header() -> Vec<String> {
    vec![
        record(
            "2026-06-24T06:00:00.000Z",
            "session_meta",
            json!({"id":"session-1","model_provider":"openai"}),
        ),
        record(
            "2026-06-24T06:00:01.000Z",
            "turn_context",
            json!({"turn_id":"turn-1","model":"gpt-5.5","cwd":"/tmp/project"}),
        ),
        record(
            "2026-06-24T06:00:02.000Z",
            "event_msg",
            json!({"type":"task_started","turn_id":"turn-1"}),
        ),
        record(
            "2026-06-24T06:00:03.000Z",
            "response_item",
            json!({"type":"message","role":"user","content":[{"type":"input_text","text":"fix it"}]}),
        ),
    ]
}
fn usage(input: u64, output: u64) -> Value {
    json!({"type":"token_count","info":{"last_token_usage":{"input_tokens":input,"output_tokens":output,"cached_input_tokens":0,"reasoning_output_tokens":output-1,"total_tokens":input+output}}})
}
fn completed() -> String {
    let mut lines = header();
    lines.extend([
 record("2026-06-24T06:00:04.000Z","event_msg",json!({"type":"agent_message","message":"inspect the script first","phase":"commentary"})),
 record("2026-06-24T06:00:05.000Z","response_item",json!({"type":"function_call","id":"fc-1","call_id":"call-1","name":"exec_command","arguments":"{\"cmd\":\"pwd\"}"})),
 record("2026-06-24T06:00:06.000Z","response_item",json!({"type":"function_call_output","call_id":"call-1","output":"\"/tmp/project\""})),
 record("2026-06-24T06:00:06.000Z","event_msg",usage(100,10)),
 record("2026-06-24T06:00:07.000Z","event_msg",json!({"type":"agent_message","message":"apply the focused patch","phase":"commentary"})),
 record("2026-06-24T06:00:08.000Z","response_item",json!({"type":"custom_tool_call","call_id":"call-2","name":"apply_patch","input":"*** Begin Patch\n*** End Patch"})),
 record("2026-06-24T06:00:09.000Z","response_item",json!({"type":"custom_tool_call_output","call_id":"call-2","output":"Done"})),
 record("2026-06-24T06:00:09.000Z","event_msg",usage(120,12)),
 record("2026-06-24T06:00:10.000Z","event_msg",json!({"type":"agent_message","message":"fixed","phase":"final"})),
 record("2026-06-24T06:00:10.000Z","event_msg",usage(130,13)),
 record("2026-06-24T06:00:11.000Z","event_msg",json!({"type":"task_complete","turn_id":"turn-1","last_agent_message":"fixed"})),
 ]);
    lines.join("\n") + "\n"
}
fn events(records: &[Value], name: &str) -> Vec<Value> {
    records
        .iter()
        .filter(|e| e["event.name"] == name)
        .cloned()
        .collect()
}
fn timestamp(entry: &Value) -> u64 {
    entry["time_unix_nano"]
        .as_str()
        .expect("nanos")
        .parse::<u64>()
        .expect("nanos")
        / 1_000_000
}
fn meta(id: &str) -> CodexTranscriptMeta {
    let r = json!({"type":"session_meta","timestamp":"2026-06-24T06:00:00.000Z","payload":{"id":id,"model_provider":"openai"}});
    extract_codex_transcript_meta(r.as_object().expect("record")).expect("metadata")
}

#[tokio::test]
async fn wakeup_marker_makes_absent_default_available_and_preserves_both_watch_paths() {
    let f = Fixture::new();
    let home = f.root.path().join("home");
    let isolated = home.join("u/fixture/sessions");
    tokio::fs::create_dir_all(&isolated)
        .await
        .expect("isolated");
    let wakeup = f.input.inner.body.wakeup_dir.as_str();
    let sessions = f.input.inner.body.session_dir.as_str();
    assert!(!CodexTranscriptInput::check_availability(Some(wakeup), Some(sessions)).await);
    f.marker("wakeups","session-1.json",json!({"session_id":"session-1","session_dir":isolated,"received_at":chrono::Utc::now().to_rfc3339()})).await;
    assert!(CodexTranscriptInput::check_availability(Some(wakeup), Some(sessions)).await);
    assert_eq!(
        CodexTranscriptInput::watch_paths(Some(wakeup), Some(sessions)),
        vec![sessions, wakeup]
    );
}
#[tokio::test]
async fn invalid_dynamic_wakeup_markers_never_activate_real_input() {
    for kind in [
        "expired",
        "malformed",
        "missing-timestamp",
        "unsupported-layout",
        "missing-root",
    ] {
        let f = Fixture::new();
        let home = f.root.path().join("home");
        let isolated = home.join(if kind == "unsupported-layout" {
            "other/fixture/sessions"
        } else {
            "u/fixture/sessions"
        });
        if kind != "missing-root" {
            tokio::fs::create_dir_all(&isolated)
                .await
                .expect("isolated");
        }
        let mut payload = json!({"codex_home":home,"session_dir":isolated,"received_at":chrono::Utc::now().to_rfc3339()});
        if kind == "expired" {
            payload["received_at"] =
                json!((chrono::Utc::now() - chrono::Duration::hours(49)).to_rfc3339());
        }
        if kind == "missing-timestamp" {
            payload
                .as_object_mut()
                .expect("object")
                .remove("received_at");
        }
        f.marker("wakeups", "invalid.json", payload).await;
        if kind == "malformed" {
            tokio::fs::write(f.root.path().join("wakeups/invalid.json"), "{")
                .await
                .expect("malformed");
        }
        assert!(
            !CodexTranscriptInput::check_availability(
                Some(&f.input.inner.body.wakeup_dir),
                Some(&f.input.inner.body.session_dir)
            )
            .await,
            "{kind}"
        );
        assert!(!f.input.collector_running());
        assert!(f
            .input
            .inner
            .body
            .watcher_stop
            .lock()
            .expect("watcher")
            .is_none());
    }
}
#[tokio::test]
async fn absent_metadata_survives_discovery_until_exact_absence_ttl_then_reclaims_bytes() {
    let f = Fixture::new();
    let path = f
        .write(
            "rollout-absence.jsonl",
            &(record(
                "2026-06-24T06:00:00.000Z",
                "session_meta",
                json!({"id":"absence-session","model_provider":"openai"}),
            ) + "\n"),
        )
        .await;
    let stat = tokio::fs::metadata(&path).await.expect("stat");
    let (ino, size, _) = file_meta_snapshot(&stat);
    f.engine()
        .register_subagent_owner_meta(f.rt(), &path, ino, size, Some(0), true)
        .await
        .expect("register");
    let seen = f
        .input
        .inner
        .body
        .with_shared(|s| s.meta_cache[&path].last_seen_at_ms);
    tokio::fs::remove_file(&path).await.expect("unlink");
    f.engine()
        .index_discovered_transcript_owners(f.rt(), &[], None)
        .await
        .expect("discovery");
    f.engine().prune_absent_meta_cache(
        &HashSet::new(),
        seen + CACHED_TRANSCRIPT_META_ABSENCE_TTL_MS - 1,
    );
    assert!(f
        .input
        .inner
        .body
        .with_shared(|s| s.meta_cache.contains_key(&path)));
    f.engine().prune_absent_meta_cache(
        &HashSet::new(),
        seen + CACHED_TRANSCRIPT_META_ABSENCE_TTL_MS + 1,
    );
    assert!(f
        .input
        .inner
        .body
        .with_shared(|s| s.meta_cache.is_empty() && s.meta_cache_bytes == 0));
}
#[tokio::test]
async fn discovery_cache_hits_do_not_refresh_or_evict_the_active_metadata_working_set() {
    let f = Fixture::new();
    let cold = f
        .write(
            "rollout-cold.jsonl",
            &(record(
                "2026-06-24T06:00:00.000Z",
                "session_meta",
                json!({"id":"cold","model_provider":"openai"}),
            ) + "\n"),
        )
        .await;
    let stat = tokio::fs::metadata(&cold).await.expect("stat");
    let (ino, size, _) = file_meta_snapshot(&stat);
    f.engine()
        .register_subagent_owner_meta(f.rt(), &cold, ino, size, Some(0), true)
        .await
        .expect("cold");
    for n in 1..MAX_LINK_DESCRIPTORS {
        f.engine().remember_transcript_owner_meta(
            &format!("synthetic-hot-{n}"),
            n as u64,
            100,
            0,
            meta(&format!("hot-{n}")),
            true,
        );
    }
    let before = f
        .input
        .inner
        .body
        .with_shared(|s| s.meta_cache.keys().cloned().collect::<Vec<_>>());
    let result = f
        .engine()
        .load_transcript_owner_meta(f.rt(), &cold, ino, size, 0, false)
        .await
        .expect("discovery hit")
        .expect("meta");
    assert_eq!(result.thread_id, "cold");
    let candidate = f
        .write(
            "rollout-cache-candidate.jsonl",
            &(record(
                "2026-06-24T06:00:00.000Z",
                "session_meta",
                json!({"id":"candidate","model_provider":"openai"}),
            ) + "\n"),
        )
        .await;
    f.engine()
        .index_discovered_transcript_owners(
            f.rt(),
            &[
                DiscoveredSessionFile {
                    file_path: cold.clone(),
                    baseline_on_start: true,
                },
                DiscoveredSessionFile {
                    file_path: candidate.clone(),
                    baseline_on_start: true,
                },
            ],
            None,
        )
        .await
        .expect("real discovery");
    assert!(
        !f.input
            .inner
            .body
            .with_shared(|s| s.meta_cache.contains_key(&candidate)),
        "discovery cache misses cannot evict active metadata"
    );
    assert!(
        f.input
            .inner
            .body
            .with_shared(|s| s.meta_cache.keys().cloned().collect::<Vec<_>>())
            == before,
        "discovery must not refresh active LRU order"
    );
    f.engine()
        .remember_transcript_owner_meta("new-active", 999, 100, 0, meta("new-active"), true);
    assert!(
        !f.input
            .inner
            .body
            .with_shared(|s| s.meta_cache.contains_key(&cold)),
        "discovery must not promote cold metadata over active working set"
    );
    assert!(f
        .input
        .inner
        .body
        .with_shared(|s| s.meta_cache.contains_key("synthetic-hot-1")
            && s.meta_cache.contains_key("new-active")));
}
#[test]
fn real_cache_estimated_byte_budget_evicts_oldest_and_rejects_oversized_metadata() {
    let f = Fixture::new();
    let mut big = meta("first");
    big.base_instructions = Some("x".repeat(MAX_CACHED_TRANSCRIPT_META_BYTES / 2));
    f.engine()
        .remember_transcript_owner_meta("first", 1, 100, 0, big.clone(), true);
    big.thread_id = "second".into();
    f.engine()
        .remember_transcript_owner_meta("second", 2, 100, 0, big, true);
    f.input.inner.body.with_shared(|s| {
        assert!(!s.meta_cache.contains_key("first"));
        assert!(s.meta_cache.contains_key("second"));
        assert!(s.meta_cache_bytes <= MAX_CACHED_TRANSCRIPT_META_BYTES);
    });
    let mut enormous = meta("oversized");
    enormous.base_instructions = Some("y".repeat(MAX_CACHED_TRANSCRIPT_META_BYTES + 1));
    f.engine()
        .remember_transcript_owner_meta("oversized", 3, 100, 0, enormous, true);
    assert!(!f
        .input
        .inner
        .body
        .with_shared(|s| s.meta_cache.contains_key("oversized")));
}
#[tokio::test]
async fn recovery_watermark_is_actual_file_size_so_same_inode_shrink_reloads_owner() {
    let f = Fixture::new();
    let turn = completed();
    let original = format!(
        "{}{}\n",
        turn,
        record(
            "2026-06-24T06:01:00.000Z",
            "event_msg",
            json!({"type":"diagnostic","padding":"x".repeat(32768)})
        )
    );
    let path = f.write("rollout-watermark.jsonl", &original).await;
    let stat = tokio::fs::metadata(&path).await.expect("stat");
    let (ino, size, _) = file_meta_snapshot(&stat);
    let mut cp = empty_checkpoint(ino);
    cp.owner_session_meta_offset = Some(0);
    cp.scan_offset = turn.len() as i64;
    cp.active_turn = Some(create_active_turn(
        "turn-1".into(),
        (turn.lines().next().expect("line").len() + 1) as i64,
        1_782_280_801_000.0,
        false,
    ));
    f.engine()
        .recover_turn_segment(
            f.rt(),
            &path,
            &mut cp,
            RecoveryBoundary {
                end_offset: turn.len() as i64,
                terminal: true,
                inferred: None,
            },
            None,
        )
        .await
        .expect("recover");
    assert_eq!(
        f.input
            .inner
            .body
            .with_shared(|s| s.meta_cache[&path].file_size),
        size
    );
    let changed = turn.replacen("\"id\":\"session-1\"", "\"id\":\"replacement-session\"", 1);
    let rewritten = format!(
        "{}{}\n",
        changed,
        record(
            "2026-06-24T06:00:12.000Z",
            "event_msg",
            json!({"type":"diagnostic","padding":"y".repeat(8192)})
        )
    );
    tokio::fs::write(&path, &rewritten).await.expect("rewrite");
    let after = tokio::fs::metadata(&path).await.expect("stat");
    let (ino2, size2, _) = file_meta_snapshot(&after);
    assert_eq!(ino2, ino);
    assert!(size2 < size && size2 >= turn.len() as i64);
    let replacement = f
        .engine()
        .load_transcript_owner_meta(f.rt(), &path, ino2, size2, 0, true)
        .await
        .expect("reload")
        .expect("meta");
    assert_eq!(replacement.thread_id, "replacement-session");
}
#[tokio::test]
async fn empty_completed_turn_emits_one_terminal_zero_usage_llm_pair() {
    let f = Fixture::new();
    let mut lines = header();
    lines.push(record(
        "2026-06-24T06:00:04.000Z",
        "event_msg",
        json!({"type":"task_complete","turn_id":"turn-1"}),
    ));
    let p = f
        .write("rollout-empty.jsonl", &(lines.join("\n") + "\n"))
        .await;
    f.process(&p).await;
    let all = f.records();
    assert_eq!(events(&all, "llm.request").len(), 1);
    let r = events(&all, "llm.response");
    assert_eq!(r.len(), 1);
    assert_eq!(r[0]["gen_ai.response.finish_reasons"], json!(["stop"]));
    for key in [
        "gen_ai.usage.input_tokens",
        "gen_ai.usage.output_tokens",
        "gen_ai.usage.cache_read.input_tokens",
        "gen_ai.usage.cache_creation.input_tokens",
        "gen_ai.usage.total_tokens",
    ] {
        assert_eq!(r[0][key], json!(0), "{key}");
    }
}
#[tokio::test]
async fn completed_three_waves_preserve_exact_tokens_reasoning_messages_ids_and_step_links() {
    let f = Fixture::new();
    let p = f.write("rollout-waves.jsonl", &completed()).await;
    f.process(&p).await;
    let all = f.records();
    let r = events(&all, "llm.response");
    assert_eq!(r.len(), 3);
    assert_eq!(
        r.iter()
            .map(|e| e["gen_ai.usage.total_tokens"].clone())
            .collect::<Vec<_>>(),
        vec![json!(110), json!(132), json!(143)]
    );
    assert_eq!(
        r.iter()
            .map(|e| e["gen_ai.response.finish_reasons"].clone())
            .collect::<Vec<_>>(),
        vec![json!(["tool_call"]), json!(["tool_call"]), json!(["stop"])]
    );
    assert_eq!(timestamp(&r[0]), 1_782_280_805_000);
    assert_eq!(timestamp(&r[1]), 1_782_280_808_000);
    assert_eq!(r[0]["gen_ai.response.id"], "fc-1");
    assert!(r[0]["gen_ai.output.messages"][0]["parts"]
        .as_array()
        .expect("parts")
        .contains(&json!({"type":"reasoning","content":"inspect the script first"})));
    assert!(r[1]["gen_ai.output.messages"][0]["parts"]
        .as_array()
        .expect("parts")
        .contains(&json!({"type":"reasoning","content":"apply the focused patch"})));
    assert_eq!(
        r[2]["gen_ai.output.messages"],
        json!([{"role":"assistant","parts":[{"type":"text","content":"fixed"}],"finish_reason":"stop"}])
    );
    let requests = events(&all, "llm.request");
    assert_eq!(requests.len(), 3);
    assert_eq!(requests[0]["gen_ai.response.id"], "fc-1");
    let mut expected = json!([{"role":"user","parts":[{"type":"text","content":"fix it"}]},{"role":"assistant","parts":[{"type":"tool_call","id":"call-1","name":"exec_command","arguments":{"command":"pwd"}}]},{"role":"tool","parts":[{"type":"tool_call_response","id":"call-1","response":"/tmp/project"}]}]);
    assert_eq!(requests[1]["gen_ai.input.messages"], expected);
    expected.as_array_mut().expect("messages").extend([json!({"role":"assistant","parts":[{"type":"tool_call","id":"call-2","name":"apply_patch","arguments":{"command":"*** Begin Patch\n*** End Patch"}}]}),json!({"role":"tool","parts":[{"type":"tool_call_response","id":"call-2","response":"Done"}]})]);
    assert_eq!(requests[2]["gen_ai.input.messages"], expected);
    assert!(requests
        .iter()
        .all(|e| e["gen_ai.input.messages_hash"].is_string()));
    assert_ne!(
        requests[0]["gen_ai.input.messages_hash"],
        requests[1]["gen_ai.input.messages_hash"]
    );
    assert_eq!(
        events(&all, "tool.call")
            .iter()
            .map(|e| e["gen_ai.step.id"].clone())
            .collect::<Vec<_>>(),
        vec![json!("session-1:turn-1:s1"), json!("session-1:turn-1:s2")]
    );
    let before = all.len();
    assert_eq!(f.process(&p).await, 0);
    assert_eq!(f.records().len(), before);
}
async fn web_search(with_reasoning: bool) -> Vec<Value> {
    let f = Fixture::new();
    let mut lines = header();
    if with_reasoning {
        lines.push(record(
            "2026-06-24T06:00:04.000Z",
            "response_item",
            json!({"type":"reasoning","id":"reasoning-1"}),
        ));
    }
    lines.extend([
 record("2026-06-24T06:00:08.000Z","event_msg",json!({"type":"web_search_end","call_id":"ws-1","action":{"type":"search","query":"test"}})),
 record("2026-06-24T06:00:08.010Z","response_item",json!({"type":"web_search_call","id":"ws-1","status":"completed","action":{"type":"search","query":"test"}})),
 record("2026-06-24T06:00:09.000Z","event_msg",json!({"type":"agent_message","message":"found it","phase":"final"})),
 record("2026-06-24T06:00:09.000Z","event_msg",usage(100,10)),
 record("2026-06-24T06:00:10.000Z","event_msg",json!({"type":"task_complete","turn_id":"turn-1","last_agent_message":"found it"}))]);
    let p = f
        .write("rollout-web.jsonl", &(lines.join("\n") + "\n"))
        .await;
    f.process(&p).await;
    f.records()
}
#[tokio::test]
async fn web_search_uses_activity_start_and_end_event_for_nonzero_tool_and_llm_timing() {
    let all = web_search(true).await;
    let q = events(&all, "llm.request");
    let r = events(&all, "llm.response");
    let c = events(&all, "tool.call");
    let t = events(&all, "tool.result");
    assert_eq!(timestamp(&q[0]), 1_782_280_803_000);
    assert_eq!(timestamp(&r[0]), 1_782_280_804_000);
    assert_eq!(timestamp(&c[0]), 1_782_280_804_000);
    assert_eq!(timestamp(&t[0]), 1_782_280_808_000);
    assert_eq!(t[0]["gen_ai.tool.call.duration"].as_f64(), Some(4000.0));
}
#[tokio::test]
async fn web_search_end_closes_llm_when_pretool_reasoning_is_absent() {
    let all = web_search(false).await;
    assert_eq!(
        timestamp(&events(&all, "llm.request")[0]),
        1_782_280_803_000
    );
    assert_eq!(
        timestamp(&events(&all, "llm.response")[0]),
        1_782_280_808_000
    );
}
#[tokio::test]
async fn provider_response_id_wins_over_reasoning_message_and_function_item_ids() {
    let f = Fixture::new();
    let mut lines = header();
    lines.extend([
 record("2026-06-24T06:00:04.000Z","response_item",json!({"type":"reasoning","id":"rs-provider-item","summary":[],"content":null,"encrypted_content":null})),
 record("2026-06-24T06:00:05.000Z","response_item",json!({"type":"message","id":"msg-provider-item","role":"assistant","content":[{"type":"output_text","text":"done"}]})),
 record("2026-06-24T06:00:06.000Z","token_usage_record",json!({"thread_id":"session-1","turn_id":"turn-1","session_id":"session-1","response_id":"resp-provider-response","usage":{"input_tokens":100,"output_tokens":10,"total_tokens":110}})),
 record("2026-06-24T06:00:07.000Z","event_msg",usage(100,10)),
 record("2026-06-24T06:00:08.000Z","event_msg",json!({"type":"task_complete","turn_id":"turn-1","last_agent_message":"done"}))]);
    let p = f
        .write("rollout-response.jsonl", &(lines.join("\n") + "\n"))
        .await;
    f.process(&p).await;
    let all = f.records();
    assert_eq!(
        events(&all, "llm.request")[0]["gen_ai.response.id"],
        "resp-provider-response"
    );
    assert_eq!(
        events(&all, "llm.response")[0]["gen_ai.response.id"],
        "resp-provider-response"
    );
}
#[tokio::test]
async fn malformed_terminal_timestamp_never_produces_unix_epoch_records() {
    let f = Fixture::new();
    let mut lines = header();
    lines.push(record(
        "not-a-date",
        "event_msg",
        json!({"type":"task_complete","turn_id":"turn-1"}),
    ));
    let p = f
        .write("rollout-time.jsonl", &(lines.join("\n") + "\n"))
        .await;
    f.process(&p).await;
    let all = f.records();
    assert!(!events(&all, "llm.response").is_empty());
    assert!(all.iter().all(|e| timestamp(e) > 1_767_225_600_000));
}
#[tokio::test]
async fn multiple_user_messages_form_one_entry_prompt_but_keep_raw_llm_input_parts() {
    let f = Fixture::new();
    let mut lines: Vec<String> = completed().lines().map(str::to_owned).collect();
    lines.insert(4,record("2026-06-24T06:00:03.500Z","response_item",json!({"type":"message","role":"user","content":[{"type":"input_text","text":"and run the tests"}]})));
    let p = f
        .write("rollout-prompt.jsonl", &(lines.join("\n") + "\n"))
        .await;
    f.process(&p).await;
    let all = f.records();
    assert_eq!(
        events(&all, "other")[0]["gen_ai.input.messages_delta"],
        json!([{"role":"user","parts":[{"type":"text","content":"fix it\nand run the tests"}]}])
    );
    assert_eq!(
        events(&all, "llm.request")[0]["gen_ai.input.messages_delta"],
        json!([{"role":"user","parts":[{"type":"text","content":"fix it"}]},{"role":"user","parts":[{"type":"text","content":"and run the tests"}]}])
    );
}
#[tokio::test]
async fn every_record_receives_trimmed_allowed_wakeup_resource_context_without_sensitive_keys() {
    let f = Fixture::new();
    f.marker("wakeups","session-1.json",json!({"session_id":"session-1","resourceAttributes":{"agentteams.worker.name":" codex-worker ","agentteams.instance.id":" lw-codex ","agentteams.token":"should-not-leak","custom.key":"ignored"}})).await;
    let p = f.write("rollout-resource.jsonl", &completed()).await;
    f.process(&p).await;
    let all = f.records();
    assert_eq!(events(&all, "llm.response").len(), 3);
    for e in &all {
        assert_eq!(e["gen_ai.agent.name"], "codex-worker");
        assert_eq!(
            e["resourceAttributes"],
            json!({"agentteams.worker.name":"codex-worker","agentteams.instance.id":"lw-codex"})
        );
        assert!(e.get("agentteams.worker.name").is_none());
        assert!(e.get("agentteams.instance.id").is_none());
    }
    let encoded = serde_json::to_string(&all).expect("json");
    assert!(!encoded.contains("should-not-leak") && !encoded.contains("ignored"));
}
#[tokio::test]
async fn overlong_wakeup_worker_names_are_never_attached_or_promoted_to_agent_name() {
    let f = Fixture::new();
    let long = "x".repeat(513);
    f.marker("wakeups","session-1.json",json!({"session_id":"session-1","resourceAttributes":{"agentteams.worker.name":long,"agentteams.instance.id":"lw-codex"}})).await;
    let p = f.write("rollout-long-resource.jsonl", &completed()).await;
    f.process(&p).await;
    let all = f.records();
    assert!(!all.is_empty());
    for e in &all {
        assert!(e.get("gen_ai.agent.name").is_none());
        assert_eq!(
            e["resourceAttributes"],
            json!({"agentteams.instance.id":"lw-codex"})
        );
    }
    assert!(!serde_json::to_string(&all).expect("json").contains(&long));
}
#[tokio::test]
async fn invocation_attributes_reach_all_five_event_types_and_filter_reserved_sensitive_long_nonstring(
) {
    let f = Fixture::new();
    let long = "x".repeat(513);
    f.marker("spans","session-1--turn-1.json",json!({"session_id":"session-1","turn_id":"turn-1","spanAttributes":{"multica.issue.id":" issue-1 ","multica.agent.name":"codex-agent","multica.runtime.name":"codex","gen_ai.agent.name":"must-not-overwrite","multica.api_token":"must-not-leak","multica.long":long,"multica.number":42},"received_at":chrono::Utc::now().to_rfc3339()})).await;
    let p = f.write("rollout-span.jsonl", &completed()).await;
    f.process(&p).await;
    let all = f.records();
    let kinds: std::collections::BTreeSet<String> = all
        .iter()
        .map(|e| e["event.name"].as_str().expect("event").into())
        .collect();
    assert_eq!(
        kinds,
        [
            "other",
            "llm.request",
            "llm.response",
            "tool.call",
            "tool.result"
        ]
        .into_iter()
        .map(str::to_owned)
        .collect()
    );
    for e in &all {
        assert_eq!(e["multica.issue.id"], "issue-1");
        assert_eq!(e["multica.agent.name"], "codex-agent");
        assert_eq!(e["multica.runtime.name"], "codex");
        assert_ne!(e["gen_ai.agent.name"], "must-not-overwrite");
        assert!(
            e.get("multica.api_token").is_none()
                && e.get("multica.long").is_none()
                && e.get("multica.number").is_none()
        );
    }
    let encoded = serde_json::to_string(&all).expect("json");
    assert!(!encoded.contains("must-not-leak") && !encoded.contains(&long));
}
#[tokio::test]
async fn mismatched_span_context_payload_identifiers_do_not_attribute_any_record() {
    let f = Fixture::new();
    f.marker("spans","session-1--turn-1.json",json!({"session_id":"different-session","turn_id":"turn-1","spanAttributes":{"multica.issue.id":"wrong-issue"},"received_at":chrono::Utc::now().to_rfc3339()})).await;
    let p = f.write("rollout-mismatch.jsonl", &completed()).await;
    f.process(&p).await;
    let all = f.records();
    assert!(!all.is_empty());
    assert!(all.iter().all(|e| e.get("multica.issue.id").is_none()));
}
#[tokio::test]
async fn expired_span_contexts_are_removed_by_production_cleanup_while_fresh_context_survives() {
    let f = Fixture::new();
    f.marker("spans","session-1--turn-1.json",json!({"session_id":"session-1","turn_id":"turn-1","spanAttributes":{"multica.issue.id":"expired"},"received_at":(chrono::Utc::now()-chrono::Duration::hours(49)).to_rfc3339()})).await;
    f.marker("spans","session-1--turn-2.json",json!({"session_id":"session-1","turn_id":"turn-2","spanAttributes":{"multica.issue.id":"fresh"},"received_at":chrono::Utc::now().to_rfc3339()})).await;
    f.engine()
        .cleanup_expired_span_contexts(f.rt(), now_ms() as i64)
        .await;
    assert!(!f.root.path().join("spans/session-1--turn-1.json").exists());
    assert!(f.root.path().join("spans/session-1--turn-2.json").exists());
}
#[tokio::test]
async fn invocation_context_isolation_is_per_turn_within_one_native_rollout() {
    let f = Fixture::new();
    for n in [1, 2] {
        f.marker("spans",&format!("session-1--turn-{n}.json"),json!({"session_id":"session-1","turn_id":format!("turn-{n}"),"spanAttributes":{"multica.issue.id":format!("issue-{n}")},"received_at":chrono::Utc::now().to_rfc3339()})).await;
    }
    let text = completed()
        + &completed()
            .replace("turn-1", "turn-2")
            .lines()
            .skip(1)
            .collect::<Vec<_>>()
            .join("\n")
        + "\n";
    let p = f.write("rollout-isolation.jsonl", &text).await;
    f.process(&p).await;
    let all = f.records();
    for n in [1, 2] {
        let t: Vec<_> = all
            .iter()
            .filter(|e| e["agent.codex.transcript_turn_id"] == format!("turn-{n}"))
            .collect();
        assert!(!t.is_empty());
        assert!(t
            .iter()
            .all(|e| e["multica.issue.id"] == format!("issue-{n}")));
    }
}
#[tokio::test]
async fn long_native_turns_emit_bounded_batches_without_dropping_observed_tool_results() {
    let f = Fixture::new();
    let mut lines = header();
    let at = chrono::DateTime::parse_from_rfc3339("2026-06-24T06:00:00Z").expect("time");
    for n in 0..80 {
        let ts = |s: i64| (at + chrono::Duration::seconds(s)).to_rfc3339();
        lines.extend([
 record(&ts(4+n*3),"event_msg",json!({"type":"agent_message","message":format!("checking {n}"),"phase":"commentary"})),
 record(&ts(5+n*3),"response_item",json!({"type":"function_call","id":format!("fc-{n}"),"call_id":format!("call-{n}"),"name":"exec_command","arguments":json!({"cmd":format!("echo {n}")}).to_string()})),
 record(&ts(6+n*3),"response_item",json!({"type":"function_call_output","call_id":format!("call-{n}"),"output":json!(n.to_string()).to_string()})),
 record(&(at+chrono::Duration::milliseconds((6+n*3)*1000+100)).to_rfc3339(),"event_msg",usage((100+n) as u64,10))]);
    }
    lines.extend([
        record(
            "2026-06-24T06:10:00.000Z",
            "event_msg",
            json!({"type":"agent_message","message":"done","phase":"final"}),
        ),
        record(
            "2026-06-24T06:10:01.000Z",
            "event_msg",
            json!({"type":"task_complete","turn_id":"turn-1","last_agent_message":"done"}),
        ),
    ]);
    let p = f
        .write("rollout-many.jsonl", &(lines.join("\n") + "\n"))
        .await;
    f.process(&p).await;
    let all = f.records();
    assert_eq!(events(&all, "tool.result").len(), 80);
    assert!(all.len() > 256);
    let batches = f.batches.lock().expect("batches");
    assert!(batches.len() > 1);
    assert!(batches.iter().all(|n| *n <= 256));
}
#[tokio::test]
async fn waves_emit_before_terminal_but_stop_finish_and_end_marker_wait_for_task_complete() {
    let f = Fixture::new();
    let mut lines: Vec<_> = completed().lines().map(str::to_owned).collect();
    let terminal = lines.pop().expect("terminal");
    let p = f
        .write("rollout-before-terminal.jsonl", &(lines.join("\n") + "\n"))
        .await;
    f.process(&p).await;
    let before = f.records();
    assert!(!events(&before, "tool.result").is_empty());
    assert!(!before
        .iter()
        .any(|e| e["gen_ai.response.finish_reasons"] == json!(["stop"])
            || e["gen_ai.turn.end"] == true));
    f.append(&p, &(terminal + "\n")).await;
    f.process(&p).await;
    let all = f.records();
    assert!(all
        .iter()
        .any(|e| e["gen_ai.response.finish_reasons"] == json!(["stop"])));
    assert_eq!(
        all.iter().filter(|e| e["gen_ai.turn.end"] == true).count(),
        1
    );
}
#[tokio::test]
async fn equal_token_counts_in_distinct_waves_remain_independent_steps_across_native_cycles() {
    let f = Fixture::new();
    let mut lines = header();
    lines.extend([
        record(
            "2026-06-24T06:00:04.000Z",
            "event_msg",
            json!({"type":"agent_message","message":"A","phase":"commentary"}),
        ),
        record("2026-06-24T06:00:05.000Z", "event_msg", usage(10, 2)),
    ]);
    let text = lines.join("\n") + "\n";
    let p = f.write("rollout-distinct-waves.jsonl", &text).await;
    f.process(&p).await;
    assert!(events(&f.records(), "llm.response").is_empty());
    assert!(f.checkpoint(&p).active_turn.expect("active").start_offset < text.len() as i64);
    let second = [
        record(
            "2026-06-24T06:00:06.000Z",
            "event_msg",
            json!({"type":"agent_message","message":"B","phase":"commentary"}),
        ),
        record(
            "2026-06-24T06:00:07.000Z",
            "response_item",
            json!({"type":"function_call","id":"fc-1","call_id":"call-1","name":"exec_command","arguments":"{\"cmd\":\"pwd\"}"}),
        ),
        record(
            "2026-06-24T06:00:08.000Z",
            "response_item",
            json!({"type":"function_call_output","call_id":"call-1","output":"\"/tmp\""}),
        ),
        record("2026-06-24T06:00:09.000Z", "event_msg", usage(10, 2)),
    ];
    f.append(&p, &(second.join("\n") + "\n")).await;
    f.process(&p).await;
    let all = f.records();
    let responses = events(&all, "llm.response");
    assert_eq!(responses.len(), 2);
    for (n, r) in responses.iter().enumerate() {
        assert_eq!(r["gen_ai.step.id"], format!("session-1:turn-1:s{}", n + 1));
        assert_eq!(r["gen_ai.usage.total_tokens"], 12);
    }
    assert_eq!(
        responses[0]["gen_ai.response.finish_reasons"],
        json!(["stop"])
    );
    assert_eq!(
        responses[1]["gen_ai.response.finish_reasons"],
        json!(["tool_call"])
    );
    assert_eq!(events(&all, "tool.call").len(), 1);
    assert_eq!(events(&all, "tool.result").len(), 1);
    f.append(
        &p,
        &([
            record(
                "2026-06-24T06:00:10.000Z",
                "event_msg",
                json!({"type":"agent_message","message":"C","phase":"final"}),
            ),
            record("2026-06-24T06:00:11.000Z", "event_msg", usage(20, 3)),
        ]
        .join("\n")
            + "\n"),
    )
    .await;
    f.process(&p).await;
    assert_eq!(events(&f.records(), "llm.response").len(), 2);
    f.append(
        &p,
        &(record(
            "2026-06-24T06:00:12.000Z",
            "event_msg",
            json!({"type":"task_complete","turn_id":"turn-1","last_agent_message":"C"}),
        ) + "\n"),
    )
    .await;
    f.process(&p).await;
    let all = f.records();
    let responses = events(&all, "llm.response");
    assert_eq!(responses.len(), 3);
    assert_eq!(responses[2]["gen_ai.step.id"], "session-1:turn-1:s3");
    assert_eq!(responses[2]["gen_ai.usage.total_tokens"], 23);
    assert_eq!(
        responses[2]["gen_ai.output.messages"],
        json!([{"role":"assistant","parts":[{"type":"text","content":"C"}],"finish_reason":"stop"}])
    );
    assert!(responses[0].get("agent.codex.turn_status").is_none());
    assert_eq!(
        all.iter()
            .filter(|e| e["gen_ai.turn.end"] == true && e["agent.codex.turn_status"] == "completed")
            .count(),
        1
    );
    let q = events(&all, "llm.request");
    let delta = json!([{"role":"assistant","parts":[{"type":"tool_call","id":"call-1","name":"exec_command","arguments":{"command":"pwd"}}]},{"role":"tool","parts":[{"type":"tool_call_response","id":"call-1","response":"/tmp"}]}]);
    assert_eq!(q[2]["gen_ai.input.messages_delta"], delta);
    let mut full = json!([{"role":"user","parts":[{"type":"text","content":"fix it"}]}]);
    full.as_array_mut()
        .expect("messages")
        .extend(delta.as_array().expect("delta").clone());
    assert_eq!(q[2]["gen_ai.input.messages"], full);
}
fn cumulative(input: u64, output: u64, total: u64) -> Value {
    let mut u = usage(input, output);
    u["info"]["total_token_usage"] = json!({"total_tokens":total});
    u
}
fn repeated_snapshot_head() -> String {
    let mut lines = header();
    lines.extend([
        record(
            "2026-06-24T06:00:04.000Z",
            "event_msg",
            json!({"type":"agent_message","message":"A","phase":"commentary"}),
        ),
        record(
            "2026-06-24T06:00:05.000Z",
            "event_msg",
            cumulative(10, 2, 1000),
        ),
        record(
            "2026-06-24T06:00:06.000Z",
            "event_msg",
            json!({"type":"agent_message","message":"B","phase":"final"}),
        ),
        record(
            "2026-06-24T06:00:07.000Z",
            "event_msg",
            cumulative(10, 2, 1000),
        ),
    ]);
    lines.join("\n") + "\n"
}
fn fresh_snapshot_tail() -> String {
    [
        record(
            "2026-06-24T06:00:10.000Z",
            "event_msg",
            cumulative(20, 2, 1100),
        ),
        record(
            "2026-06-24T06:00:11.000Z",
            "event_msg",
            json!({"type":"task_complete","turn_id":"turn-1","last_agent_message":"B"}),
        ),
    ]
    .join("\n")
        + "\n"
}
fn assert_two_snapshot_steps(all: &[Value]) {
    let r = events(all, "llm.response");
    assert_eq!(r.len(), 2);
    assert_eq!(
        r.iter()
            .map(|e| e["gen_ai.step.id"].clone())
            .collect::<Vec<_>>(),
        vec![json!("session-1:turn-1:s1"), json!("session-1:turn-1:s2")]
    );
    assert_eq!(
        r.iter()
            .map(|e| e["gen_ai.usage.total_tokens"].clone())
            .collect::<Vec<_>>(),
        vec![json!(12), json!(22)]
    );
}
#[tokio::test]
async fn fresh_accounting_after_repeated_snapshot_closes_the_still_open_wave() {
    let f = Fixture::new();
    let p = f
        .write(
            "rollout-stale-fresh.jsonl",
            &(repeated_snapshot_head() + &fresh_snapshot_tail()),
        )
        .await;
    f.process(&p).await;
    assert_two_snapshot_steps(&f.records());
}
#[tokio::test]
async fn repeated_snapshot_keeps_next_wave_collectable_after_durable_checkpoint_reload() {
    let first = Fixture::new();
    let p = first
        .write("rollout-snapshot-restart.jsonl", &repeated_snapshot_head())
        .await;
    first.process(&p).await;
    assert_eq!(events(&first.records(), "llm.response").len(), 1);
    assert_eq!(
        first
            .checkpoint(&p)
            .active_turn
            .expect("active")
            .last_cumulative_token_total,
        Some(1000.0)
    );
    let second = first.restart().await;
    second.append(&p, &fresh_snapshot_tail()).await;
    second.process(&p).await;
    let mut all = first.records();
    all.extend(second.records());
    assert_two_snapshot_steps(&all);
    assert_eq!(second.process(&p).await, 0);
}
#[tokio::test]
async fn token_count_before_tool_output_never_commits_an_unfinished_tool_wave() {
    let f = Fixture::new();
    let mut lines = header();
    lines.extend([record("2026-06-24T06:00:04.000Z","response_item",json!({"type":"function_call","id":"fc-1","call_id":"call-1","name":"exec_command","arguments":"{\"cmd\":\"pwd\"}"})),record("2026-06-24T06:00:05.000Z","event_msg",usage(30,4))]);
    let p = f
        .write("rollout-late-output.jsonl", &(lines.join("\n") + "\n"))
        .await;
    f.process(&p).await;
    let before = f.records();
    assert!(events(&before, "llm.response").is_empty() && events(&before, "tool.call").is_empty());
    f.append(
        &p,
        &(record(
            "2026-06-24T06:00:06.000Z",
            "response_item",
            json!({"type":"function_call_output","call_id":"call-1","output":"\"/tmp\""}),
        ) + "\n"),
    )
    .await;
    f.process(&p).await;
    let all = f.records();
    assert_eq!(events(&all, "llm.response").len(), 1);
    assert_eq!(events(&all, "tool.call").len(), 1);
    let r = events(&all, "tool.result");
    assert_eq!(r.len(), 1);
    assert_eq!(r[0]["gen_ai.tool.call.id"], "call-1");
    assert_eq!(r[0]["gen_ai.tool.call.result"], "/tmp");
}
#[tokio::test]
async fn independent_transcript_files_with_equal_session_turn_ids_are_both_collected() {
    let f = Fixture::new();
    let one = f.write("rollout-independent-one.jsonl", &completed()).await;
    let two = f.write("rollout-independent-two.jsonl", &completed()).await;
    f.process(&one).await;
    f.process(&two).await;
    let all = f.records();
    assert_eq!(events(&all, "llm.response").len(), 6);
    assert_eq!(f.checkpoint(&one).scan_offset, completed().len() as i64);
    assert_eq!(f.checkpoint(&two).scan_offset, completed().len() as i64);
}
#[tokio::test]
async fn durable_reload_does_not_restore_cross_transcript_turn_id_suppression() {
    let first = Fixture::new();
    let one = first
        .write("rollout-before-restart.jsonl", &completed())
        .await;
    first.process(&one).await;
    let second = first.restart().await;
    let two = second
        .write("rollout-after-restart.jsonl", &completed())
        .await;
    second.process(&two).await;
    assert_eq!(events(&second.records(), "llm.response").len(), 3);
    assert_eq!(second.process(&one).await, 0);
}
#[tokio::test]
async fn prompt_waits_for_submitted_user_text_instead_of_emitting_environment_only_prompt() {
    let f = Fixture::new();
    let environment = "<environment_context>\n  <cwd>C:\\project</cwd>\n</environment_context>";
    let lines = [
        record(
            "2026-06-24T06:00:00.000Z",
            "session_meta",
            json!({"id":"session-1","model_provider":"openai"}),
        ),
        record(
            "2026-06-24T06:00:01.000Z",
            "event_msg",
            json!({"type":"task_started","turn_id":"turn-1"}),
        ),
        record(
            "2026-06-24T06:00:01.100Z",
            "response_item",
            json!({"type":"message","role":"user","content":[{"type":"input_text","text":environment}]}),
        ),
        record(
            "2026-06-24T06:00:01.200Z",
            "turn_context",
            json!({"turn_id":"turn-1","model":"gpt-5.5","cwd":"C:\\project"}),
        ),
    ];
    let p = f
        .write("rollout-prompt-ready.jsonl", &(lines.join("\n") + "\n"))
        .await;
    f.process(&p).await;
    assert!(f.records().is_empty());
    assert!(!f.checkpoint(&p).active_turn.expect("active").emitted_prompt);
    f.append(&p,&([record("2026-06-24T06:00:01.700Z","response_item",json!({"type":"message","role":"user","content":[{"type":"input_text","text":"修复 Windows 采集"}]})),record("2026-06-24T06:00:01.710Z","event_msg",json!({"type":"user_message","message":"修复 Windows 采集"}))].join("\n")+"\n")).await;
    f.process(&p).await;
    f.process(&p).await;
    let prompts = events(&f.records(), "other");
    assert_eq!(prompts.len(), 1);
    assert_eq!(
        prompts[0]["gen_ai.input.messages_delta"],
        json!([{"role":"user","parts":[{"type":"text","content":format!("{environment}\n修复 Windows 采集")}]}])
    );
    assert!(f.checkpoint(&p).active_turn.expect("active").emitted_prompt);
}
fn media_records(content: Value, tool_output: Option<Value>) -> Vec<Map<String, Value>> {
    let mut lines = header();
    lines.truncate(3);
    lines.push(record(
        "2026-06-24T06:00:03.000Z",
        "response_item",
        json!({"type":"message","role":"user","content":content}),
    ));
    if let Some(output) = tool_output {
        lines.extend([
            record(
                "2026-06-24T06:00:04.000Z",
                "response_item",
                json!({"type":"function_call","call_id":"c1","name":"ReadImage","arguments":"{}"}),
            ),
            record(
                "2026-06-24T06:00:05.000Z",
                "response_item",
                json!({"type":"function_call_output","call_id":"c1","output":output}),
            ),
        ]);
    }
    lines.extend([
        record(
            "2026-06-24T06:00:06.000Z",
            "event_msg",
            json!({"type":"user_message","message":"look"}),
        ),
        record(
            "2026-06-24T06:00:07.000Z",
            "event_msg",
            json!({"type":"agent_message","message":"ok","phase":"final"}),
        ),
        record("2026-06-24T06:00:08.000Z", "event_msg", usage(1, 1)),
        record(
            "2026-06-24T06:00:09.000Z",
            "event_msg",
            json!({"type":"task_complete","turn_id":"turn-1","last_agent_message":"ok"}),
        ),
    ]);
    lines
        .into_iter()
        .map(|s| {
            serde_json::from_str::<Value>(&s)
                .expect("record")
                .as_object()
                .expect("map")
                .clone()
        })
        .collect()
}
fn fake_blob(params: &BlobToUriParams) -> Option<UriResult> {
    use sha2::{Digest, Sha256};
    let digest = format!("{:x}", Sha256::digest(params.content.as_bytes()));
    Some(UriResult {
        uri: format!("oss://test/{digest}"),
        mime_type: params
            .mime_type
            .clone()
            .unwrap_or_else(|| "image/png".into()),
        modality: Some("image".into()),
        size: params.content.len(),
        sha256: digest,
    })
}
type TestBlobResolver<'a> = dyn Fn(&BlobToUriParams) -> Option<UriResult> + 'a;
fn extract_media(
    records: &[Map<String, Value>],
    blob: Option<&TestBlobResolver<'_>>,
    mode: crate::types::MultimodalUploadMode,
) -> super::super::super::types::CodexExtractedTranscriptTurn {
    extract_codex_partial_turn(
        records,
        Some(&meta("session-1")),
        "session-1",
        "turn-1",
        &ExtractOptions {
            blob_to_uri: blob,
            upload_mode: Some(mode),
            ..Default::default()
        },
    )
    .expect("extract media")
}
fn user_parts(turn: &super::super::super::types::CodexExtractedTranscriptTurn) -> Vec<Value> {
    turn.input_messages[0]["parts"]
        .as_array()
        .expect("parts")
        .clone()
}
#[test]
fn image_data_url_converts_to_uri_collapses_wrappers_and_survives_real_entry_building() {
    let text="\n# Files mentioned by the user:\n\n## pipeline.jpg: /tmp/pipeline.jpg\n\n## My request for Codex:\n这个图像是什么意思？\n";
    let records = media_records(
        json!([{"type":"input_text","text":text},{"type":"input_text","text":"<image name=[Image #1] path=\"/tmp/pipeline.jpg\">"},{"type":"input_image","image_url":"data:image/png;base64,ZmFrZS1wbmc=","detail":"high"},{"type":"input_text","text":"</image>"}]),
        None,
    );
    let turn = extract_media(
        &records,
        Some(&fake_blob),
        crate::types::MultimodalUploadMode::All,
    );
    assert!(
        turn.prompt
            .as_deref()
            .expect("prompt")
            .contains("这个图像是什么意思？")
            && turn
                .prompt
                .as_deref()
                .expect("prompt")
                .contains("/tmp/pipeline.jpg")
    );
    assert!(
        !turn
            .prompt
            .as_deref()
            .expect("prompt")
            .contains("ZmFrZS1wbmc=")
            && !turn
                .prompt
                .as_deref()
                .expect("prompt")
                .contains("data:image")
    );
    let parts = user_parts(&turn);
    assert_eq!(parts.len(), 2);
    assert_eq!(parts[0], json!({"type":"text","content":text}));
    assert_eq!(parts[1]["type"], "uri");
    assert_eq!(parts[1]["mime_type"], "image/png");
    assert_eq!(parts[1]["modality"], "image");
    assert!(parts[1]["uri"]
        .as_str()
        .expect("uri")
        .starts_with("oss://test/"));
    let out = serde_json::to_string(&parts).expect("parts");
    assert!(!out.contains("ZmFrZS1wbmc=") && !out.contains("<image") && !out.contains("</image>"));
    let built = build_codex_transcript_segment(&turn, &CodexTranscriptBuildOptions::default());
    let values: Vec<Value> = built
        .entries
        .iter()
        .map(|e| serde_json::to_value(e).expect("entry"))
        .collect();
    let q = events(&values, "llm.request");
    assert!(!q.is_empty());
    let msgs = &q[0]["gen_ai.input.messages"];
    assert!(serde_json::to_string(msgs)
        .expect("messages")
        .contains("\"type\":\"uri\""));
    assert!(!serde_json::to_string(msgs)
        .expect("messages")
        .contains("\"type\":\"blob\""));
}
#[test]
fn multiple_image_wrappers_collapse_only_around_converted_uris_and_keep_adjacent_text() {
    let files = "\n# Files mentioned by the user:\n\n## a.png: /tmp/a.png\n";
    let records = media_records(
        json!([{"type":"input_text","text":files},{"type":"input_text","text":"<image name=[Image #1] path=\"/tmp/a.png\">"},{"type":"input_image","image_url":"data:image/png;base64,ZmFrZS1wbmctMQ=="},{"type":"input_text","text":"</image>"},{"type":"input_text","text":"<image name=[Image #2] path=\"/tmp/b.png\">"},{"type":"input_image","image_url":"data:image/png;base64,ZmFrZS1wbmctMg=="},{"type":"input_text","text":"</image>"},{"type":"input_text","text":"compare these"}]),
        None,
    );
    let turn = extract_media(
        &records,
        Some(&fake_blob),
        crate::types::MultimodalUploadMode::All,
    );
    let parts = user_parts(&turn);
    assert_eq!(parts.len(), 4);
    assert_eq!(parts[0], json!({"type":"text","content":files}));
    assert_eq!(parts[1]["type"], "uri");
    assert_eq!(parts[2]["type"], "uri");
    assert_ne!(parts[1]["uri"], parts[2]["uri"]);
    assert_eq!(parts[3], json!({"type":"text","content":"compare these"}));
}
#[test]
fn image_tags_with_unconvertible_path_only_image_remain_literal_text() {
    let records = media_records(
        json!([{"type":"input_text","text":"look"},{"type":"input_text","text":"<image name=[Image #1] path=\"/tmp/a.png\">"},{"type":"input_image","image_url":"/tmp/a.png"},{"type":"input_text","text":"</image>"}]),
        None,
    );
    let turn = extract_media(
        &records,
        Some(&fake_blob),
        crate::types::MultimodalUploadMode::All,
    );
    assert_eq!(
        user_parts(&turn),
        vec![
            json!({"type":"text","content":"look"}),
            json!({"type":"text","content":"<image name=[Image #1] path=\"/tmp/a.png\">"}),
            json!({"type":"text","content":"</image>"})
        ]
    );
}
#[test]
fn function_output_image_array_converts_to_uri_using_the_actual_extractor() {
    let records = media_records(
        json!([{"type":"input_text","text":"/tmp/a.jpg what is this?"}]),
        Some(
            json!([{"type":"input_image","image_url":"data:image/jpeg;base64,ZmFrZS1qcGVn","detail":"high"}]),
        ),
    );
    let turn = extract_media(
        &records,
        Some(&fake_blob),
        crate::types::MultimodalUploadMode::All,
    );
    let tools: Vec<_> = turn.steps.iter().flat_map(|s| s.tools.iter()).collect();
    let tool = tools.iter().find(|t| t.call_id == "c1").expect("tool");
    let output = tool
        .output
        .as_ref()
        .expect("output")
        .as_array()
        .expect("parts");
    assert_eq!(output.len(), 1);
    assert_eq!(output[0]["type"], "uri");
    assert_eq!(output[0]["mime_type"], "image/jpeg");
    assert_eq!(output[0]["modality"], "image");
    assert!(output[0]["uri"]
        .as_str()
        .expect("uri")
        .starts_with("oss://test/"));
}
#[test]
fn upload_modes_gate_user_and_tool_result_images_as_input_without_base64_leaks() {
    let records = media_records(
        json!([{"type":"input_text","text":"look"},{"type":"input_image","image_url":"data:image/png;base64,ZmFrZS1wbmctbW9kZQ=="}]),
        Some(
            json!([{"type":"input_image","image_url":"data:image/png;base64,ZmFrZS1wbmctbW9kZQ=="}]),
        ),
    );
    use crate::types::MultimodalUploadMode::{All, Input, None, Output};
    for mode in [None, Input, Output, All] {
        let turn = extract_media(&records, Some(&fake_blob), mode);
        let enabled = matches!(mode, Input | All);
        let parts = user_parts(&turn);
        assert_eq!(
            parts.iter().any(|p| p["type"] == "uri"),
            enabled,
            "{mode:?}"
        );
        let tool = turn
            .steps
            .iter()
            .flat_map(|s| s.tools.iter())
            .find(|t| t.call_id == "c1")
            .expect("tool");
        let encoded = serde_json::to_string(&tool.output).expect("tool");
        assert_eq!(encoded.contains("\"type\":\"uri\""), enabled, "{mode:?}");
        assert!(!format!("{turn:?}").contains("ZmFrZS1wbmctbW9kZQ=="));
    }
}
#[test]
fn absent_blob_resolver_drops_embedded_image_payload_instead_of_attaching_raw_media() {
    let records = media_records(
        json!([{"type":"input_text","text":"look"},{"type":"input_image","image_url":"data:image/png;base64,ZmFrZS1wbmc="}]),
        None,
    );
    let turn = extract_media(&records, None, crate::types::MultimodalUploadMode::All);
    assert_eq!(
        user_parts(&turn),
        vec![json!({"type":"text","content":"look"})]
    );
    assert!(!serde_json::to_string(&turn.input_messages)
        .expect("messages")
        .contains("ZmFrZS1wbmc="));
}
#[test]
fn converted_input_images_stop_at_real_multimodal_part_limit_and_keep_prompt_text() {
    use crate::multimodal::types::MAX_MULTIMODAL_PARTS;
    let mut content = vec![json!({"type":"input_text","text":"many"})];
    for n in 0..MAX_MULTIMODAL_PARTS + 2 {
        use base64::Engine;
        content.push(json!({"type":"input_image","image_url":format!("data:image/png;base64,{}",base64::engine::general_purpose::STANDARD.encode(format!("img-{n}")))}));
    }
    let turn = extract_media(
        &media_records(json!(content), None),
        Some(&fake_blob),
        crate::types::MultimodalUploadMode::All,
    );
    let parts = user_parts(&turn);
    assert_eq!(
        parts.iter().filter(|p| p["type"] == "uri").count(),
        MAX_MULTIMODAL_PARTS
    );
    assert!(parts.contains(&json!({"type":"text","content":"many"})));
}
#[test]
fn path_only_input_image_does_not_invoke_blob_resolver_or_leak_path_into_input_messages() {
    let called = std::cell::Cell::new(0);
    let resolver = |p: &BlobToUriParams| {
        called.set(called.get() + 1);
        fake_blob(p)
    };
    let records = media_records(
        json!([{"type":"input_text","text":"look"},{"type":"input_image","image_url":"/tmp/pipeline.jpg"}]),
        None,
    );
    let turn = extract_media(
        &records,
        Some(&resolver),
        crate::types::MultimodalUploadMode::All,
    );
    assert_eq!(called.get(), 0);
    assert_eq!(
        user_parts(&turn),
        vec![json!({"type":"text","content":"look"})]
    );
    assert!(!serde_json::to_string(&turn.input_messages)
        .expect("messages")
        .contains("/tmp/pipeline.jpg"));
}
#[test]
fn stable_blob_reuse_key_prevents_repeat_decode_across_partial_extractor_replay() {
    let records = media_records(
        json!([{"type":"input_text","text":"look"},{"type":"input_image","image_url":"data:image/png;base64,ZmFrZS1wbmctcmV1c2U="}]),
        None,
    );
    let cache = std::cell::RefCell::new(HashMap::new());
    let decoded = std::cell::Cell::new(0);
    let resolver = |p: &BlobToUriParams| {
        let key = p.reuse_key.as_ref().expect("reuse key").clone();
        if let Some(hit) = cache.borrow().get(&key).cloned() {
            return Some(hit);
        }
        decoded.set(decoded.get() + 1);
        let out = fake_blob(p).expect("uri");
        cache.borrow_mut().insert(key, out.clone());
        Some(out)
    };
    let first = extract_media(
        &records,
        Some(&resolver),
        crate::types::MultimodalUploadMode::All,
    );
    let second = extract_media(
        &records,
        Some(&resolver),
        crate::types::MultimodalUploadMode::All,
    );
    assert_eq!(user_parts(&first)[1]["type"], "uri");
    assert_eq!(user_parts(&first)[1], user_parts(&second)[1]);
    assert_eq!(decoded.get(), 1);
    let cache = cache.borrow();
    assert_eq!(cache.len(), 1);
    let key = cache.keys().next().expect("key");
    assert_eq!(key.split(':').count(), 2);
    assert!(key
        .split(':')
        .all(|s| !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit())));
}
fn child_record() -> Value {
    json!({"timestamp":"2026-08-07T02:00:04.100Z","type":"session_meta","payload":{"id":"bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb","session_id":"aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa","timestamp":"2026-08-07T02:00:04.100Z","cwd":"/tmp/codex-subagent-fixture","originator":"Codex Desktop","source":{"subagent":{"thread_spawn":{"parent_thread_id":"aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa","depth":1,"agent_path":"/root/fixture_child","agent_nickname":"FixtureChild","agent_role":null}}},"thread_source":"subagent","model_provider":"openai","multi_agent_version":"v2"}})
}
#[test]
fn owning_session_metadata_preserves_every_single_level_subagent_relationship_field() {
    let record = child_record();
    let child = extract_codex_transcript_meta(record.as_object().expect("record")).expect("meta");
    let got = serde_json::to_value(child).expect("metadata");
    for (key, value) in [
        ("threadId", json!("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb")),
        (
            "rootSessionId",
            json!("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"),
        ),
        ("threadSource", json!("subagent")),
        (
            "parentThreadId",
            json!("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"),
        ),
        ("depth", json!(1)),
        ("createdAtMs", json!(1_786_068_004_100.0)),
        ("agentPath", json!("/root/fixture_child")),
        ("agentNickname", json!("FixtureChild")),
        ("provider", json!("openai")),
    ] {
        assert_eq!(got[key], value, "{key}");
    }
}
#[tokio::test]
async fn cached_owner_metadata_reregisters_child_after_linker_eviction_without_reading_transcript()
{
    let f = Fixture::new();
    let p = f
        .write(
            "rollout-child-cache.jsonl",
            &(child_record().to_string() + "\n"),
        )
        .await;
    let stat = tokio::fs::metadata(&p).await.expect("stat");
    let (ino, size, _) = file_meta_snapshot(&stat);
    f.engine()
        .register_subagent_owner_meta(f.rt(), &p, ino, size, Some(0), true)
        .await
        .expect("first registration");
    assert_eq!(f.input.subagent_link_snapshot().detected_children, 1);
    f.input
        .inner
        .body
        .with_shared(|s| s.linker = crate::inputs::codex_transcript::CodexSubagentLinker::new());
    assert_eq!(f.input.subagent_link_snapshot().detected_children, 0);
    // If cache lookup regresses to physical I/O this fails rather than merely
    // comparing an instrumentation counter written by the same test.
    tokio::fs::remove_file(&p)
        .await
        .expect("remove physical source");
    f.engine()
        .register_subagent_owner_meta(f.rt(), &p, ino, size, Some(0), true)
        .await
        .expect("cached registration");
    assert_eq!(f.input.subagent_link_snapshot().detected_children, 1);
}
#[test]
fn reported_child_fingerprints_evict_oldest_at_the_real_bounded_cap() {
    let f = Fixture::new();
    f.input.inner.body.with_shared(|s| {
        for n in 0..MAX_LINK_DESCRIPTORS {
            s.reported
                .insert(format!("old-child-{n}"), "orphan::parent_not_found".into());
        }
        let mut child = meta("new-child");
        child.thread_source = CodexThreadSource::Subagent;
        child.parent_thread_id = Some("parent-thread".into());
        child.root_session_id = "parent-thread".into();
        child.depth = 1;
        s.linker.register_child(&child);
    });
    f.engine().report_subagent_links(f.rt());
    f.input.inner.body.with_shared(|s| {
        assert_eq!(s.reported.len(), MAX_LINK_DESCRIPTORS);
        assert!(!s.reported.contains_key("old-child-0"));
        assert!(s.reported.contains_key("new-child"));
    });
}
async fn run_real_lifecycle(f: &Fixture) {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        f.input.start().await.expect("native start");
        f.input.run_cycle().await;
        f.input.stop().await.expect("native stop");
    })
    .await
    .expect("bounded native lifecycle");
}
#[tokio::test]
async fn task_codex_home_is_discovered_before_stop_hook_and_aborted_work_is_ingested() {
    let f = Fixture::new();
    let text = completed().replace("\"type\":\"task_complete\"", "\"type\":\"turn_aborted\"");
    let p = f
        .write("../task/home/sessions/2026/06/24/rollout-task.jsonl", &text)
        .await;
    let home = f.root.path().join("task/home");
    f.marker("wakeups","session-1.json",json!({"session_id":"session-1","turn_id":"turn-1","codex_home":home,"received_at":chrono::Utc::now().to_rfc3339()})).await;
    run_real_lifecycle(&f).await;
    let all = f.records();
    assert!(!events(&all, "tool.result").is_empty());
    assert!(!events(&all, "llm.response").is_empty());
    assert!(all
        .iter()
        .any(|e| e["agent.codex.transcript_turn_id"] == "turn-1"));
    let canonical = tokio::fs::canonicalize(&p)
        .await
        .expect("canonical")
        .to_string_lossy()
        .into_owned();
    assert_eq!(f.checkpoint(&canonical).scan_offset, text.len() as i64);
}
#[tokio::test]
async fn transcript_path_discovers_user_isolated_home_even_when_marker_has_legacy_root() {
    let f = Fixture::new();
    let p = f
        .write(
            "../home/u/fixture-user/sessions/2026/06/24/rollout-isolated.jsonl",
            &completed(),
        )
        .await;
    let home = f.root.path().join("home");
    let canonical = tokio::fs::canonicalize(&p)
        .await
        .expect("canonical")
        .to_string_lossy()
        .into_owned();
    f.marker("wakeups","session-1.json",json!({"session_id":"session-1","turn_id":"turn-1","codex_home":home,"session_dir":home.join("sessions"),"transcript_path":canonical,"received_at":chrono::Utc::now().to_rfc3339()})).await;
    run_real_lifecycle(&f).await;
    let all = f.records();
    assert!(all.iter().any(|e| e["gen_ai.session.id"] == "session-1"));
    assert_eq!(
        f.checkpoint(&canonical).scan_offset,
        completed().len() as i64
    );
}
#[tokio::test]
async fn sixty_four_default_rollouts_do_not_consume_dynamic_session_file_budget() {
    let f = Fixture::new();
    for n in 0..64 {
        f.write(&format!("2026/06/24/rollout-default-{n}.jsonl"), "")
            .await;
    }
    let p = f
        .write(
            "../home/sessions/2026/06/24/rollout-dynamic.jsonl",
            &completed(),
        )
        .await;
    let home = f.root.path().join("home");
    f.marker("wakeups","zzzz-default.json",json!({"session_id":"default-session","session_dir":f.input.inner.body.session_dir,"received_at":chrono::Utc::now().to_rfc3339()})).await;
    f.marker("wakeups","aaaa-dynamic.json",json!({"session_id":"session-1","codex_home":home,"session_dir":home.join("sessions"),"received_at":chrono::Utc::now().to_rfc3339()})).await;
    run_real_lifecycle(&f).await;
    assert!(f
        .records()
        .iter()
        .any(|e| e["gen_ai.session.id"] == "session-1"));
    let p = tokio::fs::canonicalize(&p).await.expect("canonical");
    assert_eq!(
        f.checkpoint(p.to_str().expect("path")).scan_offset,
        completed().len() as i64
    );
}
#[tokio::test]
async fn bounded_dynamic_discovery_ignores_rollout_beneath_unrelated_deep_tree() {
    let f = Fixture::new();
    let p = f
        .write(
            "../home/sessions/unrelated/deep/tree/rollout-outside-layout.jsonl",
            &completed(),
        )
        .await;
    f.marker("wakeups","session-1.json",json!({"session_id":"session-1","session_dir":f.root.path().join("home/sessions"),"received_at":chrono::Utc::now().to_rfc3339()})).await;
    run_real_lifecycle(&f).await;
    assert!(f.records().is_empty());
    assert!(f
        .rt()
        .state_store
        .get(&f.engine().state_key(&p))
        .last_offset()
        .is_none());
    let canonical = tokio::fs::canonicalize(p).await.expect("canonical");
    assert!(f
        .rt()
        .state_store
        .get(&f.engine().state_key(canonical.to_str().expect("path")))
        .last_offset()
        .is_none());
}
#[tokio::test]
async fn marker_root_mismatch_cannot_admit_an_unrelated_codex_session_directory() {
    let f = Fixture::new();
    let p = f
        .write(
            "../unexpected/sessions/2026/06/24/rollout-mismatch-home.jsonl",
            &completed(),
        )
        .await;
    f.marker("wakeups","session-1.json",json!({"session_id":"session-1","codex_home":f.root.path().join("expected"),"session_dir":f.root.path().join("unexpected/sessions"),"received_at":chrono::Utc::now().to_rfc3339()})).await;
    run_real_lifecycle(&f).await;
    assert!(f.records().is_empty());
    let canonical = tokio::fs::canonicalize(p).await.expect("canonical");
    assert!(f
        .rt()
        .state_store
        .get(&f.engine().state_key(canonical.to_str().expect("path")))
        .last_offset()
        .is_none());
}
#[tokio::test]
async fn newest_valid_marker_is_considered_after_more_than_two_hundred_fifty_six_older_markers() {
    let f = Fixture::new();
    f.write(
        "../newest/sessions/2026/06/24/rollout-newest.jsonl",
        &completed(),
    )
    .await;
    for n in 0..256 {
        f.marker(
            "wakeups",
            &format!("old-{n:03}.json"),
            json!({"session_id":format!("old-{n}"),"received_at":chrono::Utc::now().to_rfc3339()}),
        )
        .await;
    }
    f.marker("wakeups","zzzz-newest.json",json!({"session_id":"session-1","session_dir":f.root.path().join("newest/sessions"),"received_at":chrono::Utc::now().to_rfc3339()})).await;
    run_real_lifecycle(&f).await;
    assert!(f
        .records()
        .iter()
        .any(|e| e["gen_ai.session.id"] == "session-1"));
}
#[tokio::test]
async fn expired_dynamic_home_marker_is_removed_without_checkpointing_its_rollout() {
    let f = Fixture::new();
    let p = f
        .write(
            "../stale/sessions/2026/06/24/rollout-stale.jsonl",
            &completed(),
        )
        .await;
    f.marker("wakeups","session-1.json",json!({"session_id":"session-1","session_dir":f.root.path().join("stale/sessions"),"received_at":(chrono::Utc::now()-chrono::Duration::hours(49)).to_rfc3339()})).await;
    run_real_lifecycle(&f).await;
    assert!(f.records().is_empty());
    assert!(!f.root.path().join("wakeups/session-1.json").exists());
    let canonical = tokio::fs::canonicalize(p).await.expect("canonical");
    assert!(f
        .rt()
        .state_store
        .get(&f.engine().state_key(canonical.to_str().expect("path")))
        .last_offset()
        .is_none());
}
#[tokio::test]
async fn newly_discovered_dynamic_home_ignores_rollouts_inactive_for_sixteen_minutes() {
    let f = Fixture::new();
    let p = f
        .write(
            "../inactive/sessions/2026/06/24/rollout-inactive.jsonl",
            &completed(),
        )
        .await;
    let file = std::fs::OpenOptions::new()
        .write(true)
        .open(&p)
        .expect("fixture file");
    file.set_times(
        std::fs::FileTimes::new()
            .set_modified(std::time::SystemTime::now() - std::time::Duration::from_secs(16 * 60)),
    )
    .expect("set synthetic age");
    f.marker("wakeups","session-1.json",json!({"session_id":"session-1","session_dir":f.root.path().join("inactive/sessions"),"received_at":chrono::Utc::now().to_rfc3339()})).await;
    run_real_lifecycle(&f).await;
    assert!(f.records().is_empty());
    let canonical = tokio::fs::canonicalize(p).await.expect("canonical");
    assert!(f
        .rt()
        .state_store
        .get(&f.engine().state_key(canonical.to_str().expect("path")))
        .last_offset()
        .is_none());
}
#[tokio::test]
async fn real_start_baselines_preexisting_default_rollout_without_exporting_history() {
    let f = Fixture::new();
    let text = completed();
    let p = f
        .write("2026/06/24/rollout-default-history.jsonl", &text)
        .await;
    run_real_lifecycle(&f).await;
    assert!(f.records().is_empty());
    let canonical = tokio::fs::canonicalize(p).await.expect("canonical");
    assert_eq!(
        f.checkpoint(canonical.to_str().expect("path")).scan_offset,
        text.len() as i64
    );
}
struct NativeDetection {
    input: Arc<CodexTranscriptInput>,
    paths: Vec<String>,
    starts: std::sync::atomic::AtomicUsize,
}
#[async_trait::async_trait]
impl crate::types::AgentDetection for NativeDetection {
    fn id(&self) -> &str {
        self.input.id()
    }
    fn type_name(&self) -> &str {
        "codex-transcript"
    }
    fn watch_paths(&self) -> &[String] {
        &self.paths
    }
    fn enabled(&self) -> bool {
        true
    }
    fn poll_interval_ms(&self) -> u64 {
        60_000
    }
    async fn is_available(&self) -> bool {
        CodexTranscriptInput::check_availability(
            Some(&self.input.inner.body.wakeup_dir),
            Some(&self.input.inner.body.session_dir),
        )
        .await
    }
    async fn start(&self) -> anyhow::Result<()> {
        self.starts
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.input.start().await.map_err(anyhow::Error::msg)
    }
    async fn stop(&self) -> anyhow::Result<()> {
        self.input.stop().await.map_err(anyhow::Error::msg)
    }
}
#[tokio::test]
async fn all_invalid_marker_variants_keep_real_discovery_idle_without_starting_codex() {
    for kind in [
        "expired",
        "malformed",
        "missing-timestamp",
        "unsupported-layout",
        "missing-root",
    ] {
        let f = Fixture::new();
        let home = f.root.path().join("home");
        let isolated = home.join(if kind == "unsupported-layout" {
            "other/fixture/sessions"
        } else {
            "u/fixture/sessions"
        });
        if kind != "missing-root" {
            tokio::fs::create_dir_all(&isolated)
                .await
                .expect("isolated");
        }
        let mut payload = json!({"codex_home":home,"session_dir":isolated,"received_at":chrono::Utc::now().to_rfc3339()});
        if kind == "expired" {
            payload["received_at"] =
                json!((chrono::Utc::now() - chrono::Duration::hours(49)).to_rfc3339());
        }
        if kind == "missing-timestamp" {
            payload
                .as_object_mut()
                .expect("object")
                .remove("received_at");
        }
        f.marker("wakeups", "invalid.json", payload).await;
        if kind == "malformed" {
            tokio::fs::write(f.root.path().join("wakeups/invalid.json"), "{")
                .await
                .expect("invalid");
        }
        let native = Arc::new(NativeDetection {
            input: Arc::new(CodexTranscriptInput {
                inner: f.input.inner.clone(),
            }),
            paths: CodexTranscriptInput::watch_paths(
                Some(&f.input.inner.body.wakeup_dir),
                Some(&f.input.inner.body.session_dir),
            ),
            starts: std::sync::atomic::AtomicUsize::new(0),
        });
        let entry: Arc<dyn crate::types::AgentDetection> = native.clone();
        let service = Arc::new(crate::core::AgentDiscoveryService::new(vec![entry]));
        tokio::time::timeout(std::time::Duration::from_secs(3), service.start())
            .await
            .expect("discovery start");
        assert_eq!(
            service.get_states().get(f.input.id()),
            Some(&crate::types::EntryState::Idle),
            "{kind}"
        );
        assert_eq!(
            native.starts.load(std::sync::atomic::Ordering::SeqCst),
            0,
            "{kind}"
        );
        assert!(!f.input.collector_running());
        assert!(f
            .input
            .inner
            .body
            .watcher_stop
            .lock()
            .expect("watcher")
            .is_none());
        assert!(
            !f.root.path().join("sessions").exists(),
            "native on_start must not create a baseline directory"
        );
        tokio::time::timeout(std::time::Duration::from_secs(3), service.stop())
            .await
            .expect("discovery stop");
    }
}
fn simple_turn(turn: &str, input: u64, output: u64, seconds: i64) -> Vec<String> {
    let base = chrono::DateTime::parse_from_rfc3339("2026-06-24T06:00:00Z").expect("time")
        + chrono::Duration::seconds(seconds);
    let at = |s: i64| (base + chrono::Duration::seconds(s)).to_rfc3339();
    vec![
        record(
            &at(0),
            "session_meta",
            json!({"id":"session-1","model_provider":"openai"}),
        ),
        record(
            &at(1),
            "turn_context",
            json!({"turn_id":turn,"model":"gpt-5.5","cwd":"/tmp/project"}),
        ),
        record(
            &at(2),
            "event_msg",
            json!({"type":"task_started","turn_id":turn}),
        ),
        record(
            &at(3),
            "response_item",
            json!({"type":"message","role":"user","content":[{"type":"input_text","text":format!("prompt {turn}")}]}),
        ),
        record(
            &at(4),
            "event_msg",
            json!({"type":"agent_message","message":format!("response {turn}"),"phase":"final"}),
        ),
        record(&at(4), "event_msg", usage(input, output)),
        record(
            &at(5),
            "event_msg",
            json!({"type":"task_complete","turn_id":turn,"last_agent_message":format!("response {turn}")}),
        ),
    ]
}
fn control_abort() -> Vec<String> {
    vec![
        record(
            "2026-06-24T06:00:00.000Z",
            "event_msg",
            json!({"type":"task_started","turn_id":"turn-control"}),
        ),
        record(
            "2026-06-24T06:00:01.000Z",
            "turn_context",
            json!({"turn_id":"turn-control","model":"gpt-5.5"}),
        ),
        record(
            "2026-06-24T06:00:02.000Z",
            "session_meta",
            json!({"id":"session-1","model_provider":"openai"}),
        ),
        record(
            "2026-06-24T06:00:03.000Z",
            "response_item",
            json!({"type":"message","role":"developer","content":[{"type":"input_text","text":"<turn_aborted> previous turn interrupted"}]}),
        ),
        record(
            "2026-06-24T06:00:04.000Z",
            "event_msg",
            json!({"type":"turn_aborted","turn_id":"turn-control"}),
        ),
    ]
}
#[tokio::test]
async fn hundred_terminal_budget_resumes_at_durable_offset_without_losing_the_hundred_and_first_turn(
) {
    let f = Fixture::new();
    let mut lines = Vec::new();
    for n in 0..101 {
        let turn = simple_turn(&format!("turn-{n}"), 100 + n, 10, n as i64 * 10);
        lines.extend(if n == 0 {
            turn
        } else {
            turn.into_iter().skip(1).collect()
        });
    }
    let text = lines.join("\n") + "\n";
    let p = f.write("rollout-terminal-budget.jsonl", &text).await;
    f.process(&p).await;
    assert_eq!(events(&f.records(), "llm.response").len(), 100);
    assert!(f.checkpoint(&p).scan_offset < text.len() as i64);
    f.process(&p).await;
    assert_eq!(events(&f.records(), "llm.response").len(), 101);
    assert_eq!(f.checkpoint(&p).scan_offset, text.len() as i64);
}
#[tokio::test]
async fn control_only_abort_consumes_its_range_and_collects_following_normal_turn_in_the_same_cycle(
) {
    let f = Fixture::new();
    let mut lines = control_abort();
    lines.extend(simple_turn("turn-normal", 120, 12, 60).into_iter().skip(1));
    let p = f
        .write("rollout-control-abort.jsonl", &(lines.join("\n") + "\n"))
        .await;
    f.process(&p).await;
    let all = f.records();
    assert!(all
        .iter()
        .all(|e| e["agent.codex.transcript_turn_id"] != "turn-control"));
    let normal = events(&all, "llm.response");
    assert_eq!(normal.len(), 1);
    assert_eq!(normal[0]["agent.codex.transcript_turn_id"], "turn-normal");
    let cp = f.checkpoint(&p);
    assert!(
        cp.active_turn.is_none() && cp.pending_terminal.is_none() && cp.fork_bootstrap.is_none()
    );
}
#[tokio::test]
async fn truly_unparseable_legacy_pending_range_survives_durable_restart_and_blocks_later_work() {
    let first = Fixture::new();
    let pending = record(
        "2026-06-24T06:00:00.000Z",
        "event_msg",
        json!({"type":"task_started","turn_id":"turn-unparseable"}),
    ) + "\n";
    let p = first
        .write(
            "rollout-unparseable-pending.jsonl",
            &(pending.clone() + &simple_turn("turn-normal", 120, 12, 60).join("\n") + "\n"),
        )
        .await;
    let (ino, _, _) = file_meta_snapshot(&tokio::fs::metadata(&p).await.expect("stat"));
    first.rt().state_store.update(&first.engine().state_key(&p),InputStatePatch{last_offset:Some(pending.len() as f64),extra:Some(json!({"codexTranscript":{"inode":ino,"scanOffset":pending.len(),"activeTurn":{"turnId":"turn-unparseable","startOffset":0,"startedAtMs":1_782_280_800_000.0},"pendingTerminal":{"turnId":"turn-unparseable","terminalEndOffset":pending.len()},"latestSessionMetaOffset":null,"emittedTerminalTurnIds":[]}})),..Default::default()});
    let second = first.restart().await;
    second.process(&p).await;
    assert!(second.records().is_empty());
    let cp = second.checkpoint(&p);
    assert_eq!(cp.scan_offset, pending.len() as i64);
    let still = cp.pending_terminal.expect("pending retained");
    assert_eq!(still.turn_id, "turn-unparseable");
    assert_eq!(still.retry_count, Some(1));
    assert_eq!(still.source_record_count, Some(1));
}
fn turn_block(turn: &str, base: &str) -> Vec<String> {
    let at = chrono::DateTime::parse_from_rfc3339(base).expect("time");
    let ts = |s: i64| (at + chrono::Duration::seconds(s)).to_rfc3339();
    vec![
        record(
            &ts(0),
            "turn_context",
            json!({"turn_id":turn,"model":"gpt-5.5","cwd":"/tmp/p"}),
        ),
        record(
            &ts(1),
            "event_msg",
            json!({"type":"task_started","turn_id":turn}),
        ),
        record(
            &ts(2),
            "response_item",
            json!({"type":"message","role":"user","content":[{"type":"input_text","text":"hi"}]}),
        ),
        record(
            &ts(3),
            "event_msg",
            json!({"type":"agent_message","message":format!("done-{turn}"),"phase":"final"}),
        ),
        record(&ts(4), "event_msg", usage(100, 10)),
        record(
            &ts(5),
            "event_msg",
            json!({"type":"task_complete","turn_id":turn,"last_agent_message":format!("done-{turn}")}),
        ),
    ]
}
fn uuid_at(at: &str, tail: u64) -> String {
    let hex = format!(
        "{:012x}",
        chrono::DateTime::parse_from_rfc3339(at)
            .expect("time")
            .timestamp_millis()
    );
    format!("{}-{}-7000-8000-{tail:012x}", &hex[..8], &hex[8..])
}
fn owner_meta(owner: &str, parent: &str, subagent: bool) -> String {
    let mut payload = json!({"id":owner,"forked_from_id":parent,"thread_source":if subagent{"subagent"}else{"user"},"model_provider":"openai"});
    if subagent {
        payload["source"] = json!({"subagent":{"thread_spawn":{"parent_thread_id":parent,"depth":1,"agent_path":"/root/child"}}});
    }
    record("2026-08-05T10:00:00.000Z", "session_meta", payload)
}
fn responses_for(records: &[Value], turn: &str) -> Vec<Value> {
    events(records, "llm.response")
        .into_iter()
        .filter(|e| e["agent.codex.transcript_turn_id"] == turn)
        .collect()
}
async fn prefix_is_dropped(subagent: bool) {
    let f = Fixture::new();
    let owner = "cccccccc-2222-4222-8222-222222222222";
    let parent = "aaaaaaaa-8888-4888-8888-888888888888";
    let mut lines = vec![
        owner_meta(owner, parent, subagent),
        record(
            "2026-06-01T00:00:00.000Z",
            "session_meta",
            json!({"id":parent,"thread_source":"user","model_provider":"openai"}),
        ),
    ];
    lines.extend(turn_block("copied-turn", "2026-06-01T00:00:10Z"));
    lines.push(record(
        "2026-08-05T10:00:05.000Z",
        "event_msg",
        json!({"type":"thread_settings_applied","thread_settings":{"model":"gpt-5.5"}}),
    ));
    lines.extend(turn_block("child-turn", "2026-08-05T10:00:10Z"));
    let p = f
        .write(
            &format!("2026/08/05/rollout-2026-08-05T10-00-00-{owner}.jsonl"),
            &(lines.join("\n") + "\n"),
        )
        .await;
    f.marker("wakeups",&format!("{owner}.json"),json!({"session_id":owner,"turn_id":"child-turn","initial_turn_id":"child-turn","transcript_path":p,"hook_event":if subagent{"subagent-stop"}else{"stop"},"received_at":chrono::Utc::now().to_rfc3339()})).await;
    f.process(&p).await;
    let all = f.records();
    assert_eq!(responses_for(&all, "child-turn").len(), 1);
    assert!(all
        .iter()
        .all(|e| e["agent.codex.transcript_turn_id"] != "copied-turn"));
}
#[tokio::test]
async fn fork_rollout_drops_copied_ancestor_prefix_and_emits_only_owned_turn() {
    prefix_is_dropped(false).await;
}
#[tokio::test]
async fn subagent_rollout_drops_copied_parent_prefix_and_emits_only_owned_turn() {
    prefix_is_dropped(true).await;
}
#[tokio::test]
async fn normal_user_rollout_without_copied_prefix_is_never_skipped() {
    let f = Fixture::new();
    let owner = "dddddddd-3333-4333-8333-333333333333";
    let mut lines = vec![record(
        "2026-08-05T10:00:00.000Z",
        "session_meta",
        json!({"id":owner,"thread_source":"user","model_provider":"openai"}),
    )];
    lines.extend(turn_block("normal-turn", "2026-08-05T10:00:10Z"));
    let p = f
        .write(
            &format!("2026/08/05/rollout-2026-08-05T10-00-00-{owner}.jsonl"),
            &(lines.join("\n") + "\n"),
        )
        .await;
    f.process(&p).await;
    assert_eq!(responses_for(&f.records(), "normal-turn").len(), 1);
}
async fn startup_copied_baseline_does_not_poison_parent(subagent: bool) {
    let f = Fixture::new();
    let parent = uuid_at("2026-08-05T09:00:00Z", 41);
    let owner = uuid_at("2026-08-05T10:00:00Z", 42);
    let parent_turn = uuid_at("2026-08-05T09:30:00Z", 43);
    let child_turn = uuid_at("2026-08-05T10:00:00.010Z", 44);
    let parent_meta = record(
        "2026-08-05T09:00:00Z",
        "session_meta",
        json!({"id":parent,"thread_source":"user","model_provider":"openai"}),
    );
    let p = f
        .write(
            &format!("2026/08/05/rollout-2026-08-05T09-00-00-{parent}.jsonl"),
            &(parent_meta.clone()
                + "\n"
                + &turn_block(&parent_turn, "2026-08-05T09:30:00Z").join("\n")
                + "\n"),
        )
        .await;
    let (ino, _, _) = file_meta_snapshot(&tokio::fs::metadata(&p).await.expect("stat"));
    let mut cp = empty_checkpoint(ino);
    cp.scan_offset = (parent_meta.len() + 1) as i64;
    cp.owner_session_meta_offset = Some(0);
    f.engine()
        .save_checkpoint(f.rt(), &f.engine().state_key(&p), &cp);
    let mut lines = vec![owner_meta(&owner, &parent, subagent)];
    lines.extend(turn_block(&parent_turn, "2026-08-05T09:30:00Z"));
    lines.extend(turn_block(&child_turn, "2026-08-05T10:00:00.010Z"));
    f.write(
        &format!("2026/08/05/rollout-2026-08-05T10-00-00-{owner}.jsonl"),
        &(lines.join("\n") + "\n"),
    )
    .await;
    run_real_lifecycle(&f).await;
    let all = f.records();
    assert_eq!(responses_for(&all, &parent_turn).len(), 1);
    assert!(responses_for(&all, &child_turn).is_empty());
}
#[tokio::test]
async fn startup_baselined_user_fork_never_suppresses_the_uncollected_parent_turn() {
    startup_copied_baseline_does_not_poison_parent(false).await;
}
#[tokio::test]
async fn startup_baselined_subagent_never_suppresses_the_uncollected_parent_turn() {
    startup_copied_baseline_does_not_poison_parent(true).await;
}
#[tokio::test]
async fn same_millisecond_uuidv7_causality_collects_first_owned_turn_without_any_hook() {
    let f = Fixture::new();
    let parent = uuid_at("2026-08-05T09:00:00Z", 21);
    let owner = uuid_at("2026-08-05T10:00:00Z", 22);
    let old = uuid_at("2026-08-05T09:30:00Z", 23);
    let owned = uuid_at("2026-08-05T10:00:00Z", 24);
    let mut lines = vec![owner_meta(&owner, &parent, false)];
    lines.extend(turn_block(&old, "2026-08-05T09:30:00Z"));
    lines.extend(turn_block(&owned, "2026-08-05T10:00:00Z"));
    let p = f
        .write(
            &format!("2026/08/05/rollout-2026-08-05T10-00-00-{owner}.jsonl"),
            &(lines.join("\n") + "\n"),
        )
        .await;
    f.process(&p).await;
    let all = f.records();
    assert!(responses_for(&all, &old).is_empty());
    assert_eq!(responses_for(&all, &owned).len(), 1);
    assert!(f.checkpoint(&p).fork_bootstrap.is_none());
}
#[tokio::test]
async fn hook_anchor_uses_turn_context_when_task_started_is_absent() {
    let f = Fixture::new();
    let owner = "cccccccc-0005-4005-8005-000000000005";
    let owned = "turn-context-only-owned-turn";
    let mut lines = vec![owner_meta(owner, "parent", false)];
    lines.extend(turn_block(
        "copied-parent-turn-with-start",
        "2026-08-04T10:00:00Z",
    ));
    lines.extend(
        turn_block(owned, "2026-08-05T10:00:00.010Z")
            .into_iter()
            .filter(|l| {
                serde_json::from_str::<Value>(l).expect("record")["payload"]["type"]
                    != "task_started"
            }),
    );
    let p = f
        .write(
            &format!("2026/08/05/rollout-2026-08-05T10-00-00-{owner}.jsonl"),
            &(lines.join("\n") + "\n"),
        )
        .await;
    f.marker(
        "wakeups",
        &format!("{owner}.json"),
        json!({"session_id":owner,"initial_turn_id":owned,"transcript_path":p,"hook_event":"stop"}),
    )
    .await;
    f.process(&p).await;
    let all = f.records();
    assert!(responses_for(&all, "copied-parent-turn-with-start").is_empty());
    let r = responses_for(&all, owned);
    assert_eq!(r.len(), 1);
    assert_eq!(r[0]["gen_ai.response.model"], "gpt-5.5");
    assert_eq!(r[0]["agent.codex.cwd"], "/tmp/p");
}
