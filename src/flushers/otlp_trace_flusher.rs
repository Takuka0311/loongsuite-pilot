//! 按轮次缓冲事件并执行真实 GenAI 转换、资源投影和 OTLP HTTP/protobuf 导出。
//! 保留终止信号、批量上限、运行指标及失败落盘；依赖注入仅用于测试和扩展。

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use chrono::Local;
use indexmap::IndexMap;
use regex::Regex;
use serde_json::{Map, Value};
use std::sync::LazyLock;
use uuid::Uuid;

use super::base_flusher::{BaseFlusher, FlushError, TraceRuntimeCounters, TraceRuntimeSnapshot};
use super::delegated_turn_converter::convert_delegated_turn;
use super::otlp_json_serializer::{
    create_readable_span_to_otlp_span_json_array, estimate_span_size, hr_time_to_nano,
    nano_to_hr_time, ConvertOptions, ConvertResult, EventLogConverter, ReadableSpan,
};
use super::qwen_subagent_converter::convert_qwen_subagents;
use super::span_enricher::{SpanEnricherContext, SpanEnricherRunner};
use crate::normalization::{
    entry_from_map, entry_to_object, is_openclaw_session_key, is_reserved_key,
    resolve_agent_system, GlobalAttributesProvider, DEFAULT_GIT_PASSTHROUGH_KEYS,
    OPENCLAW_SESSION_KEY, OPENCLAW_SESSION_KEY_AMBIGUOUS,
};
use crate::types::{AgentActivityEntry, OtlpCompression, OtlpTraceFlusherConfig};
use crate::utils::{
    append_line, create_logger, ensure_dir, format_time, get_today_date_string,
    normalize_agent_type, read_installed_version,
};

struct ConvertScope<'a> {
    agent_type: &'a str,
    service_name: &'a str,
    system: &'a str,
    framework: &'a str,
    convert_key: &'a str,
}

const VALID_TRACE_ID: &str = "^[0-9a-f]{32}$";
const MAX_TURN_BUFFERS: usize = 64;
const MAX_RUNTIME_AGENTS: usize = 64;
const MAX_CONVERT_STATES: usize = 64;
const DEFAULT_MAX_EXPORT_BATCH_BYTES: u64 = 10 * 1024 * 1024;
const ARMS_TRACE_LOGSTORE: &str = "logstore-tracing";

static TRACE_ID_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(VALID_TRACE_ID).expect("trace id"));
static STEP_ROUND_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?:^|[_:s])(\d+)$").expect("step round"));
static SENSITIVE_RESOURCE_KEY: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"(?i)(^|[_.-])(TOKEN|SECRET|PASSWORD|CREDENTIAL|COOKIE)([_.-]|$)|^(API_KEY|API_HEADER)$",
    )
    .expect("sensitive")
});

const TERMINAL_FINISH: &[&str] = &["stop", "end_turn", "cancelled", "error"];
const CLAUDE_TERMINAL_FINISH: &[&str] = &["stop", "end_turn", "cancelled"];
const GROK_TERMINAL_FINISH: &[&str] = &["length", "content_filter"];
const USER_IDENTITY_KEYS: &[&str] = &["user.name", "user.email"];
const GROK_PASSTHROUGH: &[&str] = &[
    "loongsuite.grok.match.strategy",
    "loongsuite.grok.timing.source",
];
const OPENCLAW_COMPAT_KEYS: &[&str] = &[
    "agent.openclaw.compatibility",
    "agent.openclaw.timing.inferred",
    "agent.openclaw.timing.source",
    "agent.openclaw.timing.quantized_ms",
    "agent.openclaw.collection.incomplete",
    "agent.openclaw.collection.end_reason",
    "agent.openclaw.correlation.ambiguous",
];
const HIERARCHY_KEYS: &[&str] = &[
    "gen_ai.turn.id",
    "gen_ai.agent.scope",
    "gen_ai.agent.depth",
    "gen_ai.agent.parent.id",
    "gen_ai.subagent.parent_tool_call.id",
    "agent.pilot.parent.turn.id",
    "agent.qwen-code-cli.subagent.collection",
    "agent.qwen-code-cli.subagent.status",
    "agent.qwen-code-cli.timing.source",
];
const SKILL_KEYS: &[&str] = &[
    "gen_ai.skill.name",
    "gen_ai.skill.id",
    "gen_ai.skill.description",
    "gen_ai.skill.version",
];
const RESERVED_RESOURCE_KEYS: &[&str] = &[
    "service.name",
    "service.version",
    "service.instance.id",
    "service.namespace",
    "host.name",
    "gen_ai.agent.type",
    "gen_ai.agent.system",
    "gen_ai.framework",
];
const SPAN_KINDS: &[&str] = &["ENTRY", "AGENT", "STEP", "LLM", "TOOL"];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum KeySource {
    TurnId,
    TraceId,
    SessionId,
    Ephemeral,
}

struct TurnBuffer {
    key: String,
    key_source: KeySource,
    key_value: String,
    agent_type: String,
    session_id: Option<String>,
    records: Vec<AgentActivityEntry>,
    completed: bool,
    last_activity_ms: i64,
    logical_bytes: f64,
    unmeasured_records: u64,
    opened_at: Instant,
    has_counters: bool,
}

#[derive(Clone, Debug)]
pub struct ExporterOpts {
    pub url: String,
    pub headers: HashMap<String, String>,
    pub compression: String,
    pub name: String,
}

#[async_trait]
pub trait TraceExporter: Send + Sync {
    async fn export(&self, spans: Vec<ReadableSpan>) -> Result<(), String>;
    async fn shutdown(&self);
}

pub type ExporterFactory = Arc<dyn Fn(ExporterOpts) -> Arc<dyn TraceExporter> + Send + Sync>;

#[derive(Clone, Debug, Default)]
pub struct OtlpEndpointCounter {
    pub in_spans: u64,
    pub in_bytes: u64,
    pub out_spans: u64,
    pub out_bytes: u64,
    pub out_failed: u64,
    pub total_delay_ms: u64,
    pub last_flush_time: String,
    pub start_time: String,
    pub is_cms: bool,
    pub project: String,
    pub logstore: String,
}

#[derive(Clone, Debug)]
struct ResolvedEndpoint {
    name: String,
    url: String,
    headers: HashMap<String, String>,
    compression: String,
    service_name: String,
    append_agent_type: bool,
}

struct ExportState {
    exporters: Vec<(String, Arc<dyn TraceExporter>)>,
}

struct ConvertState {
    active: usize,
}

struct OtlpState {
    buffers: IndexMap<String, TurnBuffer>,
    flushed: HashSet<String>,
    defer_signal_a: bool,
    counters: IndexMap<String, TraceRuntimeCounters>,
    endpoint_counters: HashMap<String, OtlpEndpointCounter>,
    export_states: HashMap<String, ExportState>,
    convert_states: IndexMap<String, ConvertState>,
    created_exporters: Vec<ExporterOpts>,
}

pub struct OtlpTraceFlusher {
    inner: Arc<OtlpInner>,
}

impl std::fmt::Debug for OtlpTraceFlusher {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("OtlpTraceFlusher")
            .field("service_name", &self.inner.cfg.service_name)
            .finish()
    }
}

struct OtlpInner {
    cfg: OtlpTraceFlusherConfig,
    endpoints: Vec<ResolvedEndpoint>,
    state: Mutex<OtlpState>,
    enrichers: SpanEnricherRunner,
    converter: Arc<dyn EventLogConverter>,
    exporter_factory: ExporterFactory,
    global_attributes: Option<Arc<Mutex<GlobalAttributesProvider>>>,
    instance_id: String,
    pilot_version: String,
    debug_dir: PathBuf,
    failed_dir: PathBuf,
    resource_attribute_keys: Vec<String>,
    passthrough_prefixes: Vec<String>,
    idle: Mutex<Option<tokio::task::JoinHandle<()>>>,
    convert_locks: Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
}

impl OtlpTraceFlusher {
    pub fn new(cfg: OtlpTraceFlusherConfig) -> Result<Self, String> {
        Self::with_parts(cfg, None, None, None)
    }

    pub fn with_parts(
        cfg: OtlpTraceFlusherConfig,
        global_attributes: Option<Arc<Mutex<GlobalAttributesProvider>>>,
        exporter_factory: Option<ExporterFactory>,
        converter: Option<Arc<dyn EventLogConverter>>,
    ) -> Result<Self, String> {
        if cfg.endpoints.is_empty() {
            return Err(
                "[otlp-trace-flusher] config.endpoints must be non-empty when enabled".to_string(),
            );
        }
        if cfg.service_name.is_empty() {
            return Err(
                "[otlp-trace-flusher] config.serviceName is required when enabled".to_string(),
            );
        }
        enable_content_capture();
        let append_default = cfg.append_agent_type_to_service_name != Some(false);
        let endpoints: Vec<ResolvedEndpoint> = cfg
            .endpoints
            .iter()
            .enumerate()
            .map(|(index, endpoint)| {
                let headers = endpoint.headers.clone().unwrap_or_default();
                ResolvedEndpoint {
                    name: if endpoint.name.is_empty() {
                        format!("otlp-{index}")
                    } else {
                        endpoint.name.clone()
                    },
                    url: resolve_endpoint_url(&endpoint.endpoint),
                    compression: if endpoint.compression == Some(OtlpCompression::None) {
                        "none".to_string()
                    } else {
                        "gzip".to_string()
                    },
                    service_name: endpoint
                        .service_name
                        .clone()
                        .filter(|value| !value.is_empty())
                        .unwrap_or_else(|| cfg.service_name.clone()),
                    append_agent_type: append_default,
                    headers,
                }
            })
            .collect();
        let mut endpoint_counters = HashMap::new();
        for endpoint in &endpoints {
            let is_cms = is_cms_endpoint(&endpoint.headers);
            endpoint_counters.insert(
                endpoint.name.clone(),
                OtlpEndpointCounter {
                    is_cms,
                    project: if is_cms {
                        cms_project_of(&endpoint.headers, &endpoint.url)
                    } else {
                        String::new()
                    },
                    logstore: if is_cms {
                        ARMS_TRACE_LOGSTORE.to_string()
                    } else {
                        String::new()
                    },
                    ..OtlpEndpointCounter::default()
                },
            );
        }
        let data_dir = cfg.data_dir.clone().unwrap_or_else(default_data_dir);
        let inner = Arc::new(OtlpInner {
            enrichers: SpanEnricherRunner::new(cfg.span_enricher_paths.as_deref().unwrap_or(&[]))?,
            converter: converter
                .unwrap_or_else(|| Arc::new(super::event_log_converter::GenAiEventLogConverter)),
            exporter_factory: exporter_factory.unwrap_or_else(|| {
                Arc::new(|opts| {
                    Arc::new(super::otlp_http_exporter::OtlpHttpExporter::new(opts))
                        as Arc<dyn TraceExporter>
                })
            }),
            global_attributes,
            instance_id: Uuid::new_v4().to_string(),
            pilot_version: read_installed_version(&data_dir),
            debug_dir: PathBuf::from(&data_dir).join("logs").join("otlp-debug"),
            failed_dir: PathBuf::from(&data_dir).join("logs").join("otlp-failed"),
            resource_attribute_keys: cfg
                .resource_attribute_keys
                .clone()
                .unwrap_or_default()
                .into_iter()
                .map(|key| key.trim().to_string())
                .filter(|key| !key.is_empty())
                .collect(),
            passthrough_prefixes: cfg
                .span_attribute_passthrough_prefixes
                .clone()
                .unwrap_or_default()
                .into_iter()
                .map(|prefix| prefix.trim().to_string())
                .filter(|prefix| !prefix.is_empty())
                .collect(),
            idle: Mutex::new(None),
            convert_locks: Mutex::new(HashMap::new()),
            endpoints,
            state: Mutex::new(OtlpState {
                buffers: IndexMap::new(),
                flushed: HashSet::new(),
                defer_signal_a: false,
                counters: IndexMap::new(),
                endpoint_counters,
                export_states: HashMap::new(),
                convert_states: IndexMap::new(),
                created_exporters: Vec::new(),
            }),
            cfg,
        });
        if let Some(timeout) = inner
            .cfg
            .turn_idle_timeout_ms
            .filter(|timeout| *timeout > 0)
        {
            let idle_inner = Arc::clone(&inner);
            let handle = tokio::spawn(async move {
                let mut ticker = tokio::time::interval(Duration::from_secs(1));
                ticker.tick().await;
                loop {
                    ticker.tick().await;
                    idle_inner.tick_idle(timeout).await;
                }
            });
            *inner.idle.lock().unwrap_or_else(|err| err.into_inner()) = Some(handle);
        }
        let names = inner
            .endpoints
            .iter()
            .map(|endpoint| format!("{}({})", endpoint.name, endpoint.url))
            .collect::<Vec<_>>()
            .join(", ");
        create_logger("otlp-trace-flusher")
            .info(&format!("OTLP trace flusher initialized → {names}"), None);
        Ok(Self { inner })
    }

    pub fn endpoint_counters(&self) -> HashMap<String, OtlpEndpointCounter> {
        self.inner
            .state
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .endpoint_counters
            .clone()
    }

    pub fn created_exporters(&self) -> Vec<ExporterOpts> {
        self.inner
            .state
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .created_exporters
            .clone()
    }

    pub fn endpoint_service_name(&self, index: usize, agent_type: &str) -> String {
        self.inner
            .resolve_endpoint_service_name(&self.inner.endpoints[index], agent_type)
    }

    pub fn resource_attributes(&self, agent_type: &str, service_name: &str) -> Map<String, Value> {
        self.inner.build_resource(
            agent_type,
            service_name,
            &Map::new(),
            agent_type,
            resolve_agent_system(agent_type),
        )
    }

    pub async fn export_spans_for_agent(&self, agent_type: &str, spans: Vec<ReadableSpan>) {
        Arc::clone(&self.inner)
            .export_spans_for_agent(agent_type, spans)
            .await;
    }

    pub async fn tick_idle(&self) {
        let timeout = self.inner.cfg.turn_idle_timeout_ms.unwrap_or(0);
        self.inner.tick_idle(timeout).await;
    }
}

#[async_trait]
impl BaseFlusher for OtlpTraceFlusher {
    fn name(&self) -> &str {
        "otlp-trace"
    }

    fn trace_runtime_snapshot(&self) -> Vec<TraceRuntimeSnapshot> {
        self.inner.snapshot()
    }

    async fn send(
        &self,
        entry: AgentActivityEntry,
        logical_bytes: Option<f64>,
    ) -> Result<(), FlushError> {
        Arc::clone(&self.inner)
            .send_entry(entry, logical_bytes)
            .await;
        Ok(())
    }

    async fn send_batch(
        &self,
        entries: Vec<AgentActivityEntry>,
        logical_bytes: Option<Vec<f64>>,
    ) -> Result<(), FlushError> {
        let sizes = logical_bytes.filter(|sizes| sizes.len() == entries.len());
        self.inner
            .state
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .defer_signal_a = true;
        for (index, entry) in entries.into_iter().enumerate() {
            let bytes = sizes.as_ref().map(|sizes| sizes[index]);
            Arc::clone(&self.inner).send_entry(entry, bytes).await;
        }
        self.inner
            .state
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .defer_signal_a = false;
        Arc::clone(&self.inner).flush_completed().await;
        Ok(())
    }

    async fn flush(&self) -> Result<(), FlushError> {
        {
            let mut state = self
                .inner
                .state
                .lock()
                .unwrap_or_else(|err| err.into_inner());
            for buffer in state.buffers.values_mut() {
                buffer.completed = true;
            }
        }
        Arc::clone(&self.inner).flush_completed().await;
        Ok(())
    }

    async fn shutdown(&self) -> Result<(), FlushError> {
        if let Some(handle) = self
            .inner
            .idle
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .take()
        {
            handle.abort();
        }
        self.flush().await?;
        let exporters: Vec<Arc<dyn TraceExporter>> = {
            let state = self
                .inner
                .state
                .lock()
                .unwrap_or_else(|err| err.into_inner());
            state
                .export_states
                .values()
                .flat_map(|export| {
                    export
                        .exporters
                        .iter()
                        .map(|(_, exporter)| Arc::clone(exporter))
                })
                .collect()
        };
        for exporter in exporters {
            exporter.shutdown().await;
        }
        self.inner
            .state
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .export_states
            .clear();
        self.inner
            .state
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .convert_states
            .clear();
        create_logger("otlp-trace-flusher").info("OTLP trace flusher shut down", None);
        Ok(())
    }
}

impl OtlpInner {
    async fn send_entry(self: &Arc<Self>, entry: AgentActivityEntry, logical_bytes: Option<f64>) {
        let (source, value, key) = resolve_group_key(&entry);
        let agent_type = normalize_agent_type(&entry.agent_type);
        if source == KeySource::Ephemeral {
            if is_metadata_only_other(&entry) {
                return;
            }
            Arc::clone(self)
                .convert_and_export(&agent_type, vec![entry])
                .await;
            return;
        }
        let incoming_session = nonempty(entry_str(&entry, "gen_ai.session.id"));
        let preempted = {
            let mut state = self.state.lock().unwrap_or_else(|err| err.into_inner());
            if state.flushed.contains(&key) {
                return;
            }
            let mut preempt = Vec::new();
            for (buf_key, buffer) in &state.buffers {
                if buffer.agent_type != agent_type || buf_key == &key || buffer.completed {
                    continue;
                }
                if agent_type == "hermes"
                    && buffer.records.iter().any(|record| {
                        entry_str(record, "agent.hermes.collection").as_deref()
                            == Some("realtime-v1")
                    })
                {
                    continue;
                }
                if incoming_session.is_none()
                    || buffer.session_id.is_none()
                    || incoming_session != buffer.session_id
                {
                    continue;
                }
                preempt.push(buf_key.clone());
            }
            let mut buffers = Vec::new();
            for buf_key in preempt {
                if let Some(mut buffer) = state.buffers.shift_remove(&buf_key) {
                    buffer.completed = true;
                    buffers.push(buffer);
                }
            }
            buffers
        };
        for buffer in preempted {
            self.finish_buffer(buffer, false).await;
        }
        let evicted = {
            let mut state = self.state.lock().unwrap_or_else(|err| err.into_inner());
            let mut buffers = Vec::new();
            if state.buffers.len() > MAX_TURN_BUFFERS {
                let overflow = state.buffers.len() - MAX_TURN_BUFFERS;
                let mut candidates: Vec<(String, i64)> = state
                    .buffers
                    .iter()
                    .filter(|(_, buffer)| !buffer.completed)
                    .map(|(key, buffer)| (key.clone(), buffer.last_activity_ms))
                    .collect();
                candidates.sort_by_key(|(_, activity)| *activity);
                let old: Vec<String> = candidates
                    .into_iter()
                    .take(overflow)
                    .map(|(key, _)| key)
                    .collect();
                for buf_key in old {
                    if let Some(mut buffer) = state.buffers.shift_remove(&buf_key) {
                        buffer.completed = true;
                        buffers.push(buffer);
                    }
                }
            }
            buffers
        };
        for buffer in evicted {
            self.finish_buffer(buffer, false).await;
        }
        let ready = {
            let mut state = self.state.lock().unwrap_or_else(|err| err.into_inner());
            let now = now_ms();
            let has_counters = state.counters.contains_key(&agent_type)
                || state.counters.len() < MAX_RUNTIME_AGENTS;
            if has_counters && !state.counters.contains_key(&agent_type) {
                state
                    .counters
                    .insert(agent_type.clone(), TraceRuntimeCounters::default());
            }
            {
                let buffer = state
                    .buffers
                    .entry(key.clone())
                    .or_insert_with(|| TurnBuffer {
                        key: key.clone(),
                        key_source: source,
                        key_value: value,
                        agent_type: agent_type.clone(),
                        session_id: incoming_session.clone(),
                        records: Vec::new(),
                        completed: false,
                        last_activity_ms: now,
                        logical_bytes: 0.0,
                        unmeasured_records: 0,
                        opened_at: Instant::now(),
                        has_counters,
                    });
                if buffer.session_id.is_none() {
                    buffer.session_id = incoming_session;
                }
                buffer.records.push(entry.clone());
                buffer.last_activity_ms = now;
                if let Some(bytes) =
                    logical_bytes.filter(|bytes| bytes.is_finite() && *bytes >= 0.0)
                {
                    buffer.logical_bytes += bytes;
                } else {
                    buffer.unmeasured_records += 1;
                }
                if is_terminal_event(&entry) {
                    buffer.completed = true;
                }
            }
            let defer = state.defer_signal_a;
            let should_flush = state
                .buffers
                .get(&key)
                .is_some_and(|buffer| buffer.completed)
                && !defer;
            if should_flush {
                state.buffers.shift_remove(&key)
            } else {
                None
            }
        };
        if let Some(buffer) = ready {
            self.finish_buffer(buffer, true).await;
        }
    }

    async fn finish_buffer(self: &Arc<Self>, mut buffer: TurnBuffer, mark_flushed: bool) {
        if mark_flushed {
            self.state
                .lock()
                .unwrap_or_else(|err| err.into_inner())
                .flushed
                .insert(buffer.key.clone());
        }
        self.record_removal(&buffer);
        if buffer.key_source != KeySource::TurnId {
            for record in &mut buffer.records {
                if entry_str(record, "gen_ai.turn.id").is_none() {
                    set_string(record, "gen_ai.turn.id", &buffer.key_value);
                }
            }
        }
        self.convert_and_export(&buffer.agent_type, buffer.records)
            .await;
    }

    fn record_removal(&self, buffer: &TurnBuffer) {
        if !buffer.has_counters {
            return;
        }
        let mut state = self.state.lock().unwrap_or_else(|err| err.into_inner());
        if let Some(counters) = state.counters.get_mut(&buffer.agent_type) {
            counters.removed_buffers_total += 1;
            counters.removed_logical_bytes_total += buffer.logical_bytes;
            counters.removed_unmeasured_records_total += buffer.unmeasured_records;
        }
    }

    async fn flush_completed(self: &Arc<Self>) {
        let completed: Vec<TurnBuffer> = {
            let mut state = self.state.lock().unwrap_or_else(|err| err.into_inner());
            let keys: Vec<String> = state
                .buffers
                .iter()
                .filter(|(_, buffer)| buffer.completed)
                .map(|(key, _)| key.clone())
                .collect();
            let mut out = Vec::new();
            for key in keys {
                if let Some(buffer) = state.buffers.shift_remove(&key) {
                    state.flushed.insert(key);
                    out.push(buffer);
                }
            }
            out
        };
        for buffer in &completed {
            self.record_removal(buffer);
        }
        for mut buffer in completed {
            if buffer.key_source != KeySource::TurnId {
                for record in &mut buffer.records {
                    if entry_str(record, "gen_ai.turn.id").is_none() {
                        set_string(record, "gen_ai.turn.id", &buffer.key_value);
                    }
                }
            }
            Arc::clone(self)
                .convert_and_export(&buffer.agent_type, buffer.records)
                .await;
        }
    }

    async fn tick_idle(self: &Arc<Self>, timeout: u64) {
        if timeout == 0 {
            return;
        }
        let now = now_ms();
        let stale: Vec<String> = {
            let state = self.state.lock().unwrap_or_else(|err| err.into_inner());
            state
                .buffers
                .iter()
                .filter(|(_, buffer)| {
                    !buffer.completed
                        && now.saturating_sub(buffer.last_activity_ms) > timeout as i64
                })
                .map(|(key, _)| key.clone())
                .collect()
        };
        for key in stale {
            let buffer = {
                let mut state = self.state.lock().unwrap_or_else(|err| err.into_inner());
                state.buffers.shift_remove(&key)
            };
            if let Some(mut buffer) = buffer {
                buffer.completed = true;
                self.finish_buffer(buffer, true).await;
            }
        }
    }

    async fn convert_and_export(
        self: &Arc<Self>,
        agent_type: &str,
        records: Vec<AgentActivityEntry>,
    ) {
        if records.is_empty() {
            return;
        }
        let projected = collect_resource_attributes(self, &records);
        let (system, framework) = resource_identity(agent_type, &records);
        let service_names = unique_service_names(self, agent_type);
        let mut set = tokio::task::JoinSet::new();
        for service_name in service_names {
            let records = records.clone();
            let projected = projected.clone();
            let agent_type = agent_type.to_string();
            let system = system.clone();
            let framework = framework.clone();
            let key = convert_state_key(&agent_type, &service_name, &projected);
            let gate = self.convert_lock(&key);
            let inner = Arc::clone(self);
            set.spawn(async move {
                let _guard = gate.lock().await;
                let converter = Arc::clone(&inner.converter);
                inner
                    .do_convert(
                        ConvertScope {
                            agent_type: &agent_type,
                            service_name: &service_name,
                            system: &system,
                            framework: &framework,
                            convert_key: &key,
                        },
                        records,
                        projected,
                        converter.as_ref(),
                    )
                    .await;
            });
        }
        while set.join_next().await.is_some() {}
        self.evict_convert_states();
    }

    fn convert_lock(&self, key: &str) -> Arc<tokio::sync::Mutex<()>> {
        let mut locks = self
            .convert_locks
            .lock()
            .unwrap_or_else(|err| err.into_inner());
        locks
            .entry(key.to_string())
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone()
    }

    async fn do_convert(
        self: &Arc<Self>,
        scope: ConvertScope<'_>,
        records: Vec<AgentActivityEntry>,
        projected: Map<String, Value>,
        converter: &dyn EventLogConverter,
    ) {
        let ConvertScope {
            agent_type,
            service_name,
            system,
            framework,
            convert_key,
        } = scope;
        {
            let mut state = self.state.lock().unwrap_or_else(|err| err.into_inner());
            let entry = state
                .convert_states
                .entry(convert_key.to_string())
                .or_insert(ConvertState { active: 0 });
            entry.active += 1;
        }
        let prepared = prepare_records(self, agent_type, &records);
        let trace_records: Vec<AgentActivityEntry> = prepared
            .records
            .into_iter()
            .filter(|record| entry_str(record, "event.name").as_deref() != Some("agent.input"))
            .collect();
        let sanitized = drop_orphan_pairs(&trace_records);
        let started = Instant::now();
        let result = route_convert(agent_type, &sanitized, &prepared.passthrough, converter);
        let succeeded = result.is_ok();
        {
            let mut state = self.state.lock().unwrap_or_else(|err| err.into_inner());
            if let Some(counters) = state.counters.get_mut(agent_type) {
                counters.converter_calls_total += 1;
                counters.converter_duration_ms_total += started.elapsed().as_secs_f64() * 1000.0;
                if !succeeded {
                    counters.converter_failed_total += 1;
                }
            }
        }
        let Ok(mut result) = result else {
            self.note_convert_done(convert_key);
            return;
        };
        if !result.warnings.is_empty() {
            create_logger("otlp-trace-flusher").warn(
                &format!("Conversion warnings for {agent_type}"),
                Some(&meta(&[(
                    "warnings",
                    Value::String(result.warnings.join("; ")),
                )])),
            );
        }
        if result.spans.is_empty() {
            self.note_convert_done(convert_key);
            return;
        }
        let resource = self.build_resource(agent_type, service_name, &projected, framework, system);
        for span in &mut result.spans {
            span.resource_attributes = Some(resource.clone());
        }
        apply_qoder_work_step_timing(&records, &mut result.spans);
        enrich_tool_skills(&records, &mut result.spans);
        if agent_type == "openclaw" {
            enrich_openclaw(&records, &prepared.identity, &mut result.spans);
        }
        if agent_type == "claude-code" {
            enrich_claude(&records, &mut result.spans);
        }
        if agent_type == "grok-build" {
            enrich_grok(&records, &prepared.grok, &mut result.spans);
        }
        let spans = self
            .enrichers
            .enrich(
                result.spans,
                SpanEnricherContext {
                    agent_type: agent_type.to_string(),
                    service_name: service_name.to_string(),
                },
            )
            .await;
        if self.cfg.debug == Some(true) {
            self.write_debug_log(agent_type, &spans).await;
        }
        self.export_in_batches(agent_type, service_name, spans)
            .await;
        self.note_convert_done(convert_key);
    }

    fn note_convert_done(&self, key: &str) {
        let mut state = self.state.lock().unwrap_or_else(|err| err.into_inner());
        if let Some(convert) = state.convert_states.get_mut(key) {
            convert.active = convert.active.saturating_sub(1);
        }
    }

    fn evict_convert_states(&self) {
        let mut state = self.state.lock().unwrap_or_else(|err| err.into_inner());
        while state.convert_states.len() > MAX_CONVERT_STATES {
            let Some(key) = state
                .convert_states
                .iter()
                .find(|(_, convert)| convert.active == 0)
                .map(|(key, _)| key.clone())
            else {
                return;
            };
            state.convert_states.shift_remove(&key);
            self.convert_locks
                .lock()
                .unwrap_or_else(|err| err.into_inner())
                .remove(&key);
        }
    }

    async fn export_spans_for_agent(self: &Arc<Self>, agent_type: &str, spans: Vec<ReadableSpan>) {
        if self.cfg.debug == Some(true) {
            self.write_debug_log(agent_type, &spans).await;
        }
        let names = unique_service_names(self, agent_type);
        for service_name in names {
            Arc::clone(self)
                .export_in_batches(agent_type, &service_name, spans.clone())
                .await;
        }
    }

    async fn export_in_batches(
        self: &Arc<Self>,
        agent_type: &str,
        service_name: &str,
        spans: Vec<ReadableSpan>,
    ) {
        let max_bytes = self
            .cfg
            .max_export_batch_bytes
            .unwrap_or(DEFAULT_MAX_EXPORT_BATCH_BYTES) as usize;
        let mut batches = Vec::new();
        let mut current = Vec::new();
        let mut current_size = 0_usize;
        for span in spans {
            let size = estimate_span_size(&span);
            if !current.is_empty() && current_size + size > max_bytes {
                batches.push(std::mem::take(&mut current));
                current_size = 0;
            }
            current_size += size;
            current.push(span);
        }
        if !current.is_empty() {
            batches.push(current);
        }
        let exporters = self.exporters_for(agent_type, service_name);
        let mut set = tokio::task::JoinSet::new();
        for (name, exporter) in exporters {
            let batches = batches.clone();
            let agent_type = agent_type.to_string();
            let inner = Arc::clone(self);
            set.spawn(async move {
                for batch in batches {
                    inner
                        .do_export(exporter.as_ref(), &name, &agent_type, batch)
                        .await;
                }
            });
        }
        while set.join_next().await.is_some() {}
    }

    async fn do_export(
        &self,
        exporter: &dyn TraceExporter,
        endpoint_name: &str,
        agent_type: &str,
        spans: Vec<ReadableSpan>,
    ) {
        let batch_bytes: u64 = spans
            .iter()
            .map(|span| estimate_span_size(span) as u64)
            .sum();
        let count = spans.len() as u64;
        {
            let mut state = self.state.lock().unwrap_or_else(|err| err.into_inner());
            if let Some(counter) = state.endpoint_counters.get_mut(endpoint_name) {
                counter.in_spans += count;
                counter.in_bytes += batch_bytes;
                if counter.start_time.is_empty() {
                    counter.start_time = format_time(Local::now());
                }
            }
        }
        let started = Instant::now();
        let result = exporter.export(spans.clone()).await;
        {
            let mut state = self.state.lock().unwrap_or_else(|err| err.into_inner());
            if let Some(counter) = state.endpoint_counters.get_mut(endpoint_name) {
                counter.total_delay_ms += started.elapsed().as_millis() as u64;
                match &result {
                    Ok(()) => {
                        counter.out_spans += count;
                        counter.out_bytes += batch_bytes;
                        counter.last_flush_time = format_time(Local::now());
                    }
                    Err(_) => counter.out_failed += count,
                }
            }
        }
        if let Err(error) = result {
            create_logger("otlp-trace-flusher").warn(
                &format!("Export failed for {agent_type} → {endpoint_name}: {error}"),
                None,
            );
            self.write_failed_log(agent_type, endpoint_name, &spans, &error)
                .await;
        }
    }

    fn exporters_for(
        &self,
        agent_type: &str,
        service_name: &str,
    ) -> Vec<(String, Arc<dyn TraceExporter>)> {
        let key = format!("{agent_type}|{service_name}");
        let mut state = self.state.lock().unwrap_or_else(|err| err.into_inner());
        if let Some(existing) = state.export_states.get(&key) {
            return existing.exporters.clone();
        }
        let mut exporters = Vec::new();
        for endpoint in &self.endpoints {
            if self.resolve_endpoint_service_name(endpoint, agent_type) != service_name {
                continue;
            }
            let opts = ExporterOpts {
                url: endpoint.url.clone(),
                headers: endpoint.headers.clone(),
                compression: endpoint.compression.clone(),
                name: endpoint.name.clone(),
            };
            state.created_exporters.push(opts.clone());
            exporters.push((endpoint.name.clone(), (self.exporter_factory)(opts)));
        }
        state.export_states.insert(
            key,
            ExportState {
                exporters: exporters.clone(),
            },
        );
        exporters
    }

    fn resolve_endpoint_service_name(
        &self,
        endpoint: &ResolvedEndpoint,
        agent_type: &str,
    ) -> String {
        if endpoint.append_agent_type {
            format!("{}-{agent_type}", endpoint.service_name)
        } else {
            endpoint.service_name.clone()
        }
    }

    fn build_resource(
        &self,
        agent_type: &str,
        service_name: &str,
        projected: &Map<String, Value>,
        framework: &str,
        system: &str,
    ) -> Map<String, Value> {
        let mut attributes = Map::new();
        attributes.insert(
            "service.name".to_string(),
            Value::String(service_name.to_string()),
        );
        attributes.insert(
            "service.version".to_string(),
            Value::String(self.pilot_version.clone()),
        );
        attributes.insert(
            "service.instance.id".to_string(),
            Value::String(self.instance_id.clone()),
        );
        attributes.insert(
            "service.namespace".to_string(),
            Value::String("loongsuite-pilot".to_string()),
        );
        attributes.insert("host.name".to_string(), Value::String(host_name()));
        attributes.insert(
            "gen_ai.agent.type".to_string(),
            Value::String(agent_type.to_string()),
        );
        attributes.insert(
            "gen_ai.agent.system".to_string(),
            Value::String(system.to_string()),
        );
        attributes.insert(
            "gen_ai.framework".to_string(),
            Value::String(framework.to_string()),
        );
        if let Some(configured) = &self.cfg.resource_attributes {
            for (key, value) in configured {
                if RESERVED_RESOURCE_KEYS.contains(&key.as_str()) {
                    continue;
                }
                attributes.insert(key.clone(), Value::String(value.clone()));
            }
        }
        for (key, value) in projected {
            if RESERVED_RESOURCE_KEYS.contains(&key.as_str())
                || SENSITIVE_RESOURCE_KEY.is_match(key)
            {
                continue;
            }
            attributes.insert(key.clone(), value.clone());
        }
        attributes
    }

    fn snapshot(&self) -> Vec<TraceRuntimeSnapshot> {
        let state = self.state.lock().unwrap_or_else(|err| err.into_inner());
        let mut rows: IndexMap<String, TraceRuntimeSnapshot> = IndexMap::new();
        for (agent_type, counters) in &state.counters {
            rows.insert(
                agent_type.clone(),
                TraceRuntimeSnapshot::from_counters(agent_type, counters),
            );
        }
        for buffer in state.buffers.values() {
            let Some(row) = rows.get_mut(&buffer.agent_type) else {
                continue;
            };
            let age = buffer.opened_at.elapsed().as_millis() as u64;
            row.pending_buffers += 1;
            row.pending_records += buffer.records.len() as u64;
            row.pending_logical_bytes += buffer.logical_bytes;
            row.pending_unmeasured_records += buffer.unmeasured_records;
            row.oldest_buffer_age_ms = row.oldest_buffer_age_ms.max(age);
            if row.pending_buffers == 1 || buffer.logical_bytes > row.largest_buffer_logical_bytes {
                row.largest_buffer_logical_bytes = buffer.logical_bytes;
                row.largest_buffer_records = buffer.records.len() as u64;
                row.largest_buffer_age_ms = age;
                row.largest_buffer_turn_id = if buffer.key_source == KeySource::TurnId {
                    Some(buffer.key_value.clone())
                } else {
                    None
                };
                row.largest_buffer_session_id = buffer.session_id.clone();
            }
        }
        rows.into_values().collect()
    }

    async fn write_debug_log(&self, agent_type: &str, spans: &[ReadableSpan]) {
        let dir = self.debug_dir.to_string_lossy().to_string();
        ensure_dir(&dir).await;
        let path = self.debug_dir.join(format!(
            "{}-{agent_type}-{}.jsonl",
            self.cfg.service_name,
            get_today_date_string()
        ));
        for line in create_readable_span_to_otlp_span_json_array(spans) {
            append_line(&path.to_string_lossy(), &line).await;
        }
    }

    async fn write_failed_log(
        &self,
        agent_type: &str,
        endpoint_name: &str,
        spans: &[ReadableSpan],
        message: &str,
    ) {
        let safe: String = endpoint_name
            .chars()
            .map(|ch| {
                if ch.is_ascii_alphanumeric() || matches!(ch, '.' | '_' | '-') {
                    ch
                } else {
                    '_'
                }
            })
            .collect();
        let dir = self.failed_dir.to_string_lossy().to_string();
        ensure_dir(&dir).await;
        let path = self.failed_dir.join(format!(
            "{}-{agent_type}__{safe}-{}.jsonl",
            self.cfg.service_name,
            get_today_date_string()
        ));
        for line in create_readable_span_to_otlp_span_json_array(spans) {
            let mut obj: Value = serde_json::from_str(&line).unwrap_or(Value::Object(Map::new()));
            if let Value::Object(map) = &mut obj {
                map.insert(
                    "_error".to_string(),
                    serde_json::json!({"code": 1, "message": message}),
                );
            }
            if let Ok(text) = serde_json::to_string(&obj) {
                append_line(&path.to_string_lossy(), &text).await;
            }
        }
    }
}

struct PreparedRecords {
    records: Vec<AgentActivityEntry>,
    passthrough: Vec<String>,
    identity: Map<String, Value>,
    grok: GrokMeta,
}

#[derive(Clone, Debug, Default)]
struct GrokMeta {
    system_instructions: Vec<Value>,
    agent_description: Option<String>,
    data_source_id: Option<String>,
}

fn prepare_records(
    inner: &OtlpInner,
    agent_type: &str,
    records: &[AgentActivityEntry],
) -> PreparedRecords {
    let mut custom = Map::new();
    if let Some(provider) = &inner.global_attributes {
        let resolved = provider
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .resolve();
        for key in resolved.keys() {
            if let Some(value) = resolved.get(&key) {
                custom.insert(key, Value::String(value.to_string()));
            }
        }
    }
    let mut prefix_keys = Vec::new();
    for record in records {
        for key in entry_to_object(record).keys() {
            if is_reserved_key(key) {
                continue;
            }
            if inner
                .passthrough_prefixes
                .iter()
                .any(|prefix| key.starts_with(prefix))
                && !prefix_keys.contains(key)
            {
                prefix_keys.push(key.clone());
            }
        }
    }
    let mut passthrough = Vec::new();
    for key in DEFAULT_GIT_PASSTHROUGH_KEYS
        .iter()
        .chain(USER_IDENTITY_KEYS)
        .chain(HIERARCHY_KEYS)
    {
        passthrough.push((*key).to_string());
    }
    if agent_type == "grok-build" {
        passthrough.extend(GROK_PASSTHROUGH.iter().map(|key| (*key).to_string()));
    }
    if agent_type == "openclaw" {
        passthrough.extend(OPENCLAW_COMPAT_KEYS.iter().map(|key| (*key).to_string()));
    }
    for key in custom.keys() {
        if !passthrough.contains(key) {
            passthrough.push(key.clone());
        }
    }
    passthrough.extend(prefix_keys);
    let mut copied: Vec<AgentActivityEntry> = records.to_vec();
    if !custom.is_empty() {
        for record in &mut copied {
            let mut map = entry_to_object(record);
            for (key, value) in &custom {
                if !map.contains_key(key) {
                    map.insert(key.clone(), value.clone());
                }
            }
            *record = entry_from_map(map);
        }
    }
    let mut identity = Map::new();
    if agent_type == "openclaw" {
        copied = prepare_openclaw_collection(&copied);
        let prepared = prepare_openclaw_identity(&copied);
        copied = prepared.0;
        identity = prepared.1;
    }
    let mut grok = GrokMeta::default();
    if agent_type == "grok-build" {
        let prepared = prepare_grok(&copied);
        copied = prepared.0;
        grok = prepared.1;
    }
    PreparedRecords {
        records: copied,
        passthrough,
        identity,
        grok,
    }
}

fn route_convert(
    agent_type: &str,
    records: &[AgentActivityEntry],
    passthrough: &[String],
    converter: &dyn EventLogConverter,
) -> Result<ConvertResult, String> {
    let options = ConvertOptions {
        strict: false,
        passthrough_keys: passthrough.to_vec(),
    };
    if agent_type == "qwen-code-cli" {
        convert_qwen_subagents(records, &options, converter)
    } else if agent_type == "hermes" || agent_type == "openclaw" {
        convert_delegated_turn(records, &options, converter)
    } else {
        converter.convert(records, &options)
    }
}

fn unique_service_names(inner: &OtlpInner, agent_type: &str) -> Vec<String> {
    let mut names = Vec::new();
    for endpoint in &inner.endpoints {
        let name = inner.resolve_endpoint_service_name(endpoint, agent_type);
        if !names.contains(&name) {
            names.push(name);
        }
    }
    names
}

fn convert_state_key(
    agent_type: &str,
    service_name: &str,
    projected: &Map<String, Value>,
) -> String {
    let mut keys: Vec<&String> = projected.keys().collect();
    keys.sort();
    let mut stable = Map::new();
    for key in keys {
        if let Some(value) = projected.get(key) {
            stable.insert(key.clone(), value.clone());
        }
    }
    format!(
        "{agent_type}|{service_name}|{}",
        serde_json::to_string(&Value::Object(stable)).unwrap_or_else(|_| "{}".to_string())
    )
}

fn resolve_group_key(entry: &AgentActivityEntry) -> (KeySource, String, String) {
    if let Some(turn_id) = entry_str(entry, "gen_ai.turn.id") {
        return (
            KeySource::TurnId,
            turn_id.clone(),
            format!("turn:{turn_id}"),
        );
    }
    if let Some(trace_id) = entry_str(entry, "trace_id") {
        if TRACE_ID_RE.is_match(&trace_id) {
            return (
                KeySource::TraceId,
                trace_id.clone(),
                format!("trace:{trace_id}"),
            );
        }
    }
    if let Some(session_id) = entry_str(entry, "gen_ai.session.id") {
        return (
            KeySource::SessionId,
            session_id.clone(),
            format!("session:{session_id}"),
        );
    }
    let ephemeral = entry_str(entry, "event.id").unwrap_or_else(|| Uuid::new_v4().to_string());
    (
        KeySource::Ephemeral,
        ephemeral.clone(),
        format!("ephemeral:{ephemeral}"),
    )
}

fn is_terminal_event(entry: &AgentActivityEntry) -> bool {
    let agent_type = normalize_agent_type(&entry.agent_type);
    if agent_type == "opencode"
        && entry_str(entry, "agent.opencode.collection").as_deref() == Some("v2")
    {
        return entry_bool(entry, "gen_ai.turn.end") == Some(true);
    }
    if agent_type == "hermes"
        && entry_str(entry, "agent.hermes.collection").as_deref() == Some("realtime-v1")
    {
        return entry_bool(entry, "gen_ai.turn.end") == Some(true);
    }
    if agent_type == "qwen-code-cli"
        && entry_str(entry, "agent.qwen-code-cli.collection").as_deref() == Some("foreground-v1")
    {
        return entry_str(entry, "gen_ai.agent.scope").as_deref() != Some("subagent")
            && entry_bool(entry, "gen_ai.turn.end") == Some(true);
    }
    if agent_type == "codex" {
        if entry_str(entry, "gen_ai.agent.scope").as_deref() == Some("subagent") {
            return false;
        }
        let status = entry_str(entry, "agent.codex.turn_status");
        if status.as_deref() != Some("completed") && status.as_deref() != Some("interrupted") {
            return false;
        }
        return entry_bool(entry, "gen_ai.turn.end") == Some(true);
    }
    if agent_type == "openclaw" {
        return entry_str(entry, "agent.openclaw.hook").as_deref() == Some("llm_output")
            || (entry_str(entry, "agent.openclaw.compatibility").as_deref() == Some("legacy")
                && matches!(
                    entry_str(entry, "agent.openclaw.hook").as_deref(),
                    Some("agent_end") | Some("legacy_cleanup")
                )
                && entry_bool(entry, "gen_ai.turn.end") == Some(true));
    }
    if agent_type == "grok-build" {
        return entry_str(entry, "event.name").as_deref() == Some("other")
            && (has_finish(entry, TERMINAL_FINISH) || has_finish(entry, GROK_TERMINAL_FINISH));
    }
    if agent_type == "claude-code" {
        return entry_bool(entry, "gen_ai.turn.end") == Some(true)
            || has_finish(entry, CLAUDE_TERMINAL_FINISH);
    }
    has_finish(entry, TERMINAL_FINISH)
}

fn has_finish(entry: &AgentActivityEntry, expected: &[&str]) -> bool {
    entry
        .finish_reasons
        .as_ref()
        .map(|reasons| {
            reasons
                .iter()
                .any(|reason| expected.contains(&reason.as_str()))
        })
        .unwrap_or(false)
}

fn is_metadata_only_other(entry: &AgentActivityEntry) -> bool {
    if entry_str(entry, "event.name").as_deref() != Some("other") {
        return false;
    }
    entry.input_messages.is_none() && entry.input_messages_delta.is_none()
}

fn drop_orphan_pairs(records: &[AgentActivityEntry]) -> Vec<AgentActivityEntry> {
    let mut steps_with_response = HashSet::new();
    let mut completed_tools = HashSet::new();
    for record in records {
        if entry_str(record, "event.name").as_deref() == Some("llm.response") {
            steps_with_response.insert(
                entry_str(record, "gen_ai.step.id").unwrap_or_else(|| "__no_step__".to_string()),
            );
        }
        if entry_str(record, "event.name").as_deref() == Some("tool.result") {
            if let Some(call_id) = entry_str(record, "gen_ai.tool.call.id") {
                completed_tools.insert(call_id);
            }
        }
    }
    records
        .iter()
        .filter(|record| match entry_str(record, "event.name").as_deref() {
            Some("llm.request") => steps_with_response.contains(
                &entry_str(record, "gen_ai.step.id").unwrap_or_else(|| "__no_step__".to_string()),
            ),
            Some("tool.call") => match entry_str(record, "gen_ai.tool.call.id") {
                Some(call_id) => completed_tools.contains(&call_id),
                None => true,
            },
            _ => true,
        })
        .cloned()
        .collect()
}

pub fn apply_qoder_work_step_timing(records: &[AgentActivityEntry], spans: &mut [ReadableSpan]) {
    let mut boundaries: HashMap<String, (Option<String>, Option<String>)> = HashMap::new();
    for record in records {
        let Some(session_id) = entry_str(record, "gen_ai.session.id") else {
            continue;
        };
        let Some(step_id) = entry_str(record, "gen_ai.step.id") else {
            continue;
        };
        let start = entry_str(record, "agent.qoderwork.step.start_time_unix_nano");
        let end = entry_str(record, "agent.qoderwork.step.end_time_unix_nano");
        if start.is_none() && end.is_none() {
            continue;
        }
        let Some(round) = step_round(&step_id) else {
            continue;
        };
        boundaries.insert(format!("{session_id}\0{round}"), (start, end));
    }
    if boundaries.is_empty() {
        return;
    }
    for span in spans {
        if span
            .attributes
            .get("gen_ai.span.kind")
            .and_then(|value| value.as_str())
            != Some("STEP")
        {
            continue;
        }
        let Some(session_id) = span
            .attributes
            .get("gen_ai.session.id")
            .and_then(|value| value.as_str())
        else {
            continue;
        };
        let Some(round) = span
            .attributes
            .get("gen_ai.react.round")
            .and_then(|value| value.as_i64())
        else {
            continue;
        };
        let Some((start, end)) = boundaries.get(&format!("{session_id}\0{round}")) else {
            continue;
        };
        let current_start = hr_time_to_nano(span.start_time)
            .parse::<i128>()
            .unwrap_or(0);
        let current_duration = hr_time_to_nano(span.duration).parse::<i128>().unwrap_or(0);
        let current_end = current_start + current_duration;
        let desired_start = start
            .as_deref()
            .and_then(|value| value.parse().ok())
            .unwrap_or(current_start);
        let desired_end = end
            .as_deref()
            .and_then(|value| value.parse().ok())
            .unwrap_or(current_end);
        if desired_end < desired_start {
            continue;
        }
        span.start_time = nano_to_hr_time(desired_start);
        span.end_time = nano_to_hr_time(desired_end);
        span.duration = nano_to_hr_time(desired_end - desired_start);
    }
}

fn step_round(step_id: &str) -> Option<i64> {
    STEP_ROUND_RE
        .captures(step_id)
        .and_then(|caps| caps.get(1))
        .and_then(|value| value.as_str().parse().ok())
}

fn enrich_tool_skills(records: &[AgentActivityEntry], spans: &mut [ReadableSpan]) {
    let mut by_call: HashMap<String, Map<String, Value>> = HashMap::new();
    for record in records {
        let name = entry_str(record, "event.name");
        if name.as_deref() != Some("tool.call") && name.as_deref() != Some("tool.result") {
            continue;
        }
        let Some(call_id) = entry_str(record, "gen_ai.tool.call.id") else {
            continue;
        };
        let slot = by_call.entry(call_id).or_default();
        for key in SKILL_KEYS {
            if let Some(value) = entry_str(record, key) {
                slot.insert((*key).to_string(), Value::String(value));
            }
        }
    }
    for span in spans {
        if span
            .attributes
            .get("gen_ai.span.kind")
            .and_then(|v| v.as_str())
            != Some("TOOL")
        {
            continue;
        }
        let Some(call_id) = span
            .attributes
            .get("gen_ai.tool.call.id")
            .and_then(|v| v.as_str())
        else {
            continue;
        };
        if let Some(attributes) = by_call.get(call_id) {
            for (key, value) in attributes {
                span.attributes.insert(key.clone(), value.clone());
            }
        }
    }
}

fn enrich_openclaw(
    records: &[AgentActivityEntry],
    identity: &Map<String, Value>,
    spans: &mut [ReadableSpan],
) {
    for span in spans.iter_mut() {
        for (key, value) in identity {
            span.attributes.insert(key.clone(), value.clone());
        }
    }
    let mut keys = HashSet::new();
    let mut scopes = HashSet::new();
    let mut valid = true;
    for record in records {
        if entry_str(record, "gen_ai.agent.type").as_deref() != Some("openclaw")
            || entry_bool(record, OPENCLAW_SESSION_KEY_AMBIGUOUS) == Some(true)
        {
            valid = false;
            break;
        }
        scopes.insert(format!(
            "{:?}|{:?}|{:?}|{:?}|{:?}|{:?}",
            record.trace_id,
            entry_str(record, "gen_ai.session.id"),
            entry_str(record, "gen_ai.turn.id"),
            record.agent_id,
            record.agent_name,
            entry_str(record, "gen_ai.agent.scope")
        ));
        if let Some(value) = record.get(OPENCLAW_SESSION_KEY) {
            if !is_openclaw_session_key(value) {
                valid = false;
                break;
            }
            if let Some(text) = value.as_str() {
                keys.insert(text.to_string());
            }
        }
    }
    if valid && scopes.len() == 1 && keys.len() == 1 {
        let session_key = keys.into_iter().next().unwrap_or_default();
        let trace_id = records.first().and_then(|record| record.trace_id.clone());
        for span in spans.iter_mut() {
            let kind = span
                .attributes
                .get("gen_ai.span.kind")
                .and_then(|value| value.as_str())
                .unwrap_or("");
            if Some(span.trace_id.as_str()) == trace_id.as_deref() && SPAN_KINDS.contains(&kind) {
                span.attributes.insert(
                    OPENCLAW_SESSION_KEY.to_string(),
                    Value::String(session_key.clone()),
                );
            }
        }
    }
}

#[derive(Default)]
struct ClaudeResponseMetadata {
    error: Option<String>,
    status: Option<i64>,
    request: Option<String>,
}

fn enrich_claude(records: &[AgentActivityEntry], spans: &mut [ReadableSpan]) {
    let mut by_response: HashMap<String, ClaudeResponseMetadata> = HashMap::new();
    for record in records {
        let name = entry_str(record, "event.name");
        if name.as_deref() != Some("llm.request") && name.as_deref() != Some("llm.response") {
            continue;
        }
        let Some(response_id) = entry_str(record, "gen_ai.response.id") else {
            continue;
        };
        let slot = by_response.entry(response_id).or_default();
        if let Some(error) = entry_str(record, "error.type") {
            slot.error = Some(error);
        }
        if let Some(status) = record.http_response_status_code {
            slot.status = Some(status);
        }
        if let Some(request_id) = record.request_id.clone() {
            slot.request = Some(request_id);
        }
    }
    for span in spans {
        if span
            .attributes
            .get("gen_ai.span.kind")
            .and_then(|v| v.as_str())
            != Some("LLM")
        {
            continue;
        }
        let Some(response_id) = span
            .attributes
            .get("gen_ai.response.id")
            .and_then(|v| v.as_str())
        else {
            continue;
        };
        let Some(ClaudeResponseMetadata {
            error,
            status,
            request,
        }) = by_response.get(response_id)
        else {
            continue;
        };
        if let Some(request) = request {
            span.attributes.insert(
                "gen_ai.request.id".to_string(),
                Value::String(request.clone()),
            );
        }
        if let Some(status) = status {
            span.attributes.insert(
                "http.response.status_code".to_string(),
                Value::from(*status),
            );
        }
        if let Some(error) = error {
            span.attributes
                .insert("error.type".to_string(), Value::String(error.clone()));
            span.status.insert("code".to_string(), Value::from(2));
            span.status.insert(
                "message".to_string(),
                Value::String("model request failed".to_string()),
            );
        }
    }
}

fn enrich_grok(records: &[AgentActivityEntry], meta: &GrokMeta, spans: &mut [ReadableSpan]) {
    if let Some(description) = &meta.agent_description {
        for span in spans.iter_mut() {
            if span
                .attributes
                .get("gen_ai.span.kind")
                .and_then(|v| v.as_str())
                == Some("AGENT")
            {
                span.attributes.insert(
                    "gen_ai.agent.description".to_string(),
                    Value::String(description.clone()),
                );
            }
        }
    }
    if let Some(data_source) = &meta.data_source_id {
        for span in spans.iter_mut() {
            if span
                .attributes
                .get("gen_ai.span.kind")
                .and_then(|v| v.as_str())
                == Some("AGENT")
            {
                span.attributes.insert(
                    "gen_ai.data_source.id".to_string(),
                    Value::String(data_source.clone()),
                );
            }
        }
    }
    if !meta.system_instructions.is_empty() {
        if let Some(first) = spans
            .iter_mut()
            .filter(|span| {
                span.attributes
                    .get("gen_ai.span.kind")
                    .and_then(|v| v.as_str())
                    == Some("LLM")
            })
            .min_by_key(|span| (span.start_time[0], span.start_time[1]))
        {
            if let Ok(text) = serde_json::to_string(&meta.system_instructions) {
                first.attributes.insert(
                    "gen_ai.system_instructions".to_string(),
                    Value::String(text),
                );
            }
        }
    }
    let _ = records;
}

fn prepare_openclaw_collection(records: &[AgentActivityEntry]) -> Vec<AgentActivityEntry> {
    let mut endings: HashMap<String, Map<String, Value>> = HashMap::new();
    for record in records {
        if entry_str(record, "agent.openclaw.compatibility").as_deref() == Some("legacy")
            && entry_str(record, "agent.openclaw.hook").as_deref() == Some("legacy_cleanup")
        {
            let key = format!(
                "{:?}|{:?}",
                record.trace_id,
                entry_str(record, "gen_ai.turn.id")
            );
            let mut fields = Map::new();
            fields.insert(
                "agent.openclaw.collection.incomplete".to_string(),
                Value::Bool(true),
            );
            if let Some(reason) = entry_str(record, "agent.openclaw.collection.end_reason") {
                fields.insert(
                    "agent.openclaw.collection.end_reason".to_string(),
                    Value::String(reason),
                );
            }
            if entry_bool(record, "agent.openclaw.correlation.ambiguous") == Some(true) {
                fields.insert(
                    "agent.openclaw.correlation.ambiguous".to_string(),
                    Value::Bool(true),
                );
            }
            endings.insert(key, fields);
        }
    }
    if endings.is_empty() {
        return records.to_vec();
    }
    records
        .iter()
        .map(|record| {
            let key = format!(
                "{:?}|{:?}",
                record.trace_id,
                entry_str(record, "gen_ai.turn.id")
            );
            let Some(extra) = endings.get(&key) else {
                return record.clone();
            };
            let mut map = entry_to_object(record);
            for (key, value) in extra {
                map.insert(key.clone(), value.clone());
            }
            entry_from_map(map)
        })
        .collect()
}

fn prepare_openclaw_identity(
    records: &[AgentActivityEntry],
) -> (Vec<AgentActivityEntry>, Map<String, Value>) {
    let mut metadata = Map::new();
    let mut sender: Option<Map<String, Value>> = None;
    for record in records {
        insert_first(
            &mut metadata,
            "agent.openclaw.sender.id",
            entry_str(record, "agent.openclaw.sender.id"),
        );
        insert_first(
            &mut metadata,
            "agent.openclaw.channel",
            entry_str(record, "agent.openclaw.channel"),
        );
        insert_first(
            &mut metadata,
            "agent.openclaw.account.id",
            entry_str(record, "agent.openclaw.account.id"),
        );
        insert_first(
            &mut metadata,
            "agent.openclaw.channel.id",
            entry_str(record, "agent.openclaw.channel.id"),
        );
        insert_first(
            &mut metadata,
            "agent.openclaw.user.id.source",
            entry_str(record, "agent.openclaw.user.id.source"),
        );
        if entry_str(record, "agent.openclaw.user.id.source").as_deref() == Some("sender") {
            if let Some(sender_id) = entry_str(record, "agent.openclaw.sender.id") {
                let mut chosen = Map::new();
                chosen.insert(
                    "agent.openclaw.sender.id".to_string(),
                    Value::String(sender_id),
                );
                if let Some(value) = entry_str(record, "agent.openclaw.channel") {
                    chosen.insert("agent.openclaw.channel".to_string(), Value::String(value));
                }
                if let Some(value) = entry_str(record, "agent.openclaw.account.id") {
                    chosen.insert(
                        "agent.openclaw.account.id".to_string(),
                        Value::String(value),
                    );
                }
                if let Some(value) = entry_str(record, "agent.openclaw.channel.id") {
                    chosen.insert(
                        "agent.openclaw.channel.id".to_string(),
                        Value::String(value),
                    );
                }
                chosen.insert(
                    "agent.openclaw.user.id.source".to_string(),
                    Value::String("sender".to_string()),
                );
                sender = Some(chosen);
            }
        }
    }
    let Some(sender) = sender else {
        return (records.to_vec(), metadata);
    };
    let sender_id = sender
        .get("agent.openclaw.sender.id")
        .cloned()
        .unwrap_or(Value::Null);
    let records = records
        .iter()
        .map(|record| {
            let mut map = entry_to_object(record);
            map.insert("user.id".to_string(), sender_id.clone());
            for (key, value) in &sender {
                map.insert(key.clone(), value.clone());
            }
            entry_from_map(map)
        })
        .collect();
    (records, sender)
}

fn prepare_grok(records: &[AgentActivityEntry]) -> (Vec<AgentActivityEntry>, GrokMeta) {
    let mut meta = GrokMeta::default();
    let records = records
        .iter()
        .map(|record| {
            if meta.system_instructions.is_empty() {
                if let Some(value) = record.system_instructions.clone() {
                    meta.system_instructions = parse_system_instructions(&value);
                }
            }
            if meta.agent_description.is_none() {
                meta.agent_description = record.agent_description.clone();
            }
            if meta.data_source_id.is_none() {
                meta.data_source_id = record.data_source_id.clone();
            }
            let mut map = entry_to_object(record);
            for key in [
                "gen_ai.system_instructions",
                "gen_ai.agent.description",
                "gen_ai.data_source.id",
                "gen_ai.tool.call.duration",
            ] {
                map.remove(key);
            }
            for key in ["gen_ai.input.messages", "gen_ai.input.messages_delta"] {
                if let Some(value) = map.get(key).cloned() {
                    map.insert(key.to_string(), strip_system_messages(value));
                }
            }
            if map.get("event.name").and_then(|value| value.as_str()) == Some("other")
                && !map.contains_key("gen_ai.input.messages")
                && !map.contains_key("gen_ai.input.messages_delta")
            {
                map.insert(
                    "gen_ai.input.messages_delta".to_string(),
                    Value::Array(Vec::new()),
                );
            }
            if map.get("event.name").and_then(|value| value.as_str()) == Some("other") {
                map.remove("error.type");
                map.remove("error.message");
            }
            entry_from_map(map)
        })
        .collect();
    (records, meta)
}

fn parse_system_instructions(value: &Value) -> Vec<Value> {
    if let Value::Array(items) = value {
        return items.clone();
    }
    if let Value::String(text) = value {
        if text.is_empty() {
            return Vec::new();
        }
        if let Ok(Value::Array(items)) = serde_json::from_str(text) {
            return items;
        }
        return vec![serde_json::json!({"type": "text", "content": text})];
    }
    Vec::new()
}

fn strip_system_messages(value: Value) -> Value {
    let strip_array = |items: Vec<Value>| {
        items
            .into_iter()
            .filter(|message| message.get("role").and_then(|role| role.as_str()) != Some("system"))
            .collect::<Vec<_>>()
    };
    match value {
        Value::Array(items) => Value::Array(strip_array(items)),
        Value::String(text) if !text.is_empty() => {
            if let Ok(Value::Array(items)) = serde_json::from_str::<Value>(&text) {
                Value::String(serde_json::to_string(&strip_array(items)).unwrap_or(text))
            } else {
                Value::String(text)
            }
        }
        other => other,
    }
}

fn collect_resource_attributes(
    inner: &OtlpInner,
    records: &[AgentActivityEntry],
) -> Map<String, Value> {
    let allowed: HashSet<&str> = inner
        .resource_attribute_keys
        .iter()
        .map(String::as_str)
        .collect();
    let mut attributes = Map::new();
    for record in records {
        if let Some(resource) = &record.resource_attributes {
            for (key, value) in resource {
                insert_resource(&mut attributes, key, value);
            }
        }
        if allowed.is_empty() {
            continue;
        }
        for (key, value) in entry_to_object(record) {
            if allowed.contains(key.as_str()) {
                insert_resource(&mut attributes, &key, &value);
            }
        }
    }
    attributes
}

fn insert_resource(attributes: &mut Map<String, Value>, key: &str, raw: &Value) {
    if SENSITIVE_RESOURCE_KEY.is_match(key) {
        return;
    }
    let Some(value) = normalize_resource_value(raw) else {
        return;
    };
    if let Some(existing) = attributes.get(key) {
        if existing != &value {
            return;
        }
    }
    attributes.insert(key.to_string(), value);
}

fn normalize_resource_value(value: &Value) -> Option<Value> {
    match value {
        Value::String(text) => {
            let trimmed = text.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(Value::String(trimmed.to_string()))
            }
        }
        Value::Bool(flag) => Some(Value::Bool(*flag)),
        Value::Number(number) if number.as_f64().is_some_and(|number| number.is_finite()) => {
            Some(Value::Number(number.clone()))
        }
        _ => None,
    }
}

fn resource_identity(agent_type: &str, records: &[AgentActivityEntry]) -> (String, String) {
    let mut system = None;
    let mut framework = None;
    for record in records {
        if system.is_none() {
            system = entry_str(record, "gen_ai.agent.system")
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty());
        }
        if framework.is_none() {
            framework = entry_str(record, "gen_ai.framework")
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty());
        }
    }
    (
        system.unwrap_or_else(|| resolve_agent_system(agent_type).to_string()),
        framework.unwrap_or_else(|| agent_type.to_string()),
    )
}

fn resolve_endpoint_url(raw: &str) -> String {
    let url = raw.trim_end_matches('/').to_string();
    if url.ends_with("/v1/traces") {
        url
    } else {
        format!("{url}/v1/traces")
    }
}

fn is_cms_endpoint(headers: &HashMap<String, String>) -> bool {
    headers.keys().any(|key| {
        let key = key.to_ascii_lowercase();
        key == "x-cms-workspace" || key == "x-arms-license-key" || key == "x-arms-project"
    })
}

fn cms_project_of(headers: &HashMap<String, String>, url: &str) -> String {
    for (key, value) in headers {
        if key.eq_ignore_ascii_case("x-arms-project") && !value.is_empty() {
            return value.clone();
        }
    }
    url.split("://")
        .nth(1)
        .and_then(|rest| rest.split('/').next())
        .and_then(|host| host.split('.').next())
        .unwrap_or("")
        .to_string()
}

fn enable_content_capture() {
    let current = std::env::var("OTEL_SEMCONV_STABILITY_OPT_IN").unwrap_or_default();
    let mut parts: Vec<String> = current
        .split(',')
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .collect();
    if !parts
        .iter()
        .any(|value| value.eq_ignore_ascii_case("gen_ai_latest_experimental"))
    {
        parts.push("gen_ai_latest_experimental".to_string());
    }
    unsafe {
        std::env::set_var("OTEL_SEMCONV_STABILITY_OPT_IN", parts.join(","));
        std::env::set_var(
            "OTEL_INSTRUMENTATION_GENAI_CAPTURE_MESSAGE_CONTENT",
            "SPAN_ONLY",
        );
    }
}

fn default_data_dir() -> String {
    let home = std::env::var("HOME").unwrap_or_default();
    format!("{home}/.loongsuite-pilot")
}

fn host_name() -> String {
    hostname::get()
        .ok()
        .and_then(|value| value.into_string().ok())
        .unwrap_or_default()
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or(0)
}

fn entry_map(entry: &AgentActivityEntry) -> Map<String, Value> {
    entry_to_object(entry)
}

fn entry_str(entry: &AgentActivityEntry, key: &str) -> Option<String> {
    match key {
        "event.name" => entry.event_name.map(|name| name.as_str().to_string()),
        "event.id" => nonempty(Some(entry.event_id.clone())),
        "gen_ai.agent.type" => nonempty(Some(entry.agent_type.clone())),
        "gen_ai.turn.id" => entry.turn_id.clone().filter(|value| !value.is_empty()),
        "gen_ai.session.id" => nonempty(Some(entry.session_id.clone())),
        "gen_ai.step.id" => entry.step_id.clone().filter(|value| !value.is_empty()),
        "trace_id" => entry.trace_id.clone().filter(|value| !value.is_empty()),
        "gen_ai.tool.call.id" => entry.tool_call_id.clone().filter(|value| !value.is_empty()),
        "gen_ai.response.id" => entry.response_id.clone().filter(|value| !value.is_empty()),
        "gen_ai.request.id" => entry.request_id.clone().filter(|value| !value.is_empty()),
        "error.type" => entry.error_type.clone().filter(|value| !value.is_empty()),
        _ => entry_map(entry)
            .get(key)
            .and_then(|value| value.as_str())
            .filter(|value| !value.is_empty())
            .map(str::to_string),
    }
}

fn entry_bool(entry: &AgentActivityEntry, key: &str) -> Option<bool> {
    match key {
        "gen_ai.turn.end" => entry.turn_end,
        _ => entry_map(entry).get(key).and_then(|value| value.as_bool()),
    }
}

fn nonempty(value: Option<String>) -> Option<String> {
    value.filter(|text| !text.is_empty())
}

fn set_string(entry: &mut AgentActivityEntry, key: &str, value: &str) {
    let mut map = entry_map(entry);
    map.insert(key.to_string(), Value::String(value.to_string()));
    *entry = entry_from_map(map);
}

fn insert_first(map: &mut Map<String, Value>, key: &str, value: Option<String>) {
    if map.contains_key(key) {
        return;
    }
    if let Some(value) = value {
        map.insert(key.to_string(), Value::String(value));
    }
}

fn meta(pairs: &[(&str, Value)]) -> Map<String, Value> {
    pairs
        .iter()
        .map(|(key, value)| ((*key).to_string(), value.clone()))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::super::base_flusher::BaseFlusher;
    use super::super::otlp_json_serializer::{
        ConvertOptions, ConvertResult, EventLogConverter, ReadableSpan,
    };
    use super::{entry_str, ExporterFactory, OtlpTraceFlusher, TraceExporter};
    use crate::normalization::entry_from_map;
    use crate::types::{
        AgentActivityEntry, OtlpCompression, OtlpEndpoint, OtlpProtocol, OtlpTraceFlusherConfig,
    };
    use async_trait::async_trait;
    use serde_json::{json, Value};
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    fn cfg(endpoints: Vec<OtlpEndpoint>) -> OtlpTraceFlusherConfig {
        OtlpTraceFlusherConfig {
            span_enricher_paths: None,
            enabled: true,
            endpoints,
            protocol: OtlpProtocol::HttpProtobuf,
            service_name: "test-pilot".to_string(),
            append_agent_type_to_service_name: None,
            resource_attributes: None,
            capture_message_content: None,
            debug: None,
            turn_idle_timeout_ms: None,
            resource_attribute_keys: None,
            span_attribute_passthrough_prefixes: None,
            max_export_batch_bytes: None,
            data_dir: None,
        }
    }

    fn ep(name: &str, endpoint: &str) -> OtlpEndpoint {
        crate::types::OtlpEndpoint {
            name: name.to_string(),
            endpoint: endpoint.to_string(),
            headers: None,
            compression: None,
            service_name: None,
        }
    }

    fn entry(fields: Value) -> AgentActivityEntry {
        entry_from_map(fields.as_object().cloned().unwrap_or_default())
    }

    struct RecConv {
        calls: Mutex<Vec<Vec<AgentActivityEntry>>>,
    }

    impl EventLogConverter for RecConv {
        fn convert(
            &self,
            records: &[AgentActivityEntry],
            _options: &ConvertOptions,
        ) -> Result<ConvertResult, String> {
            self.calls.lock().unwrap().push(records.to_vec());
            Ok(ConvertResult {
                span_count: records.len(),
                ..ConvertResult::default()
            })
        }
    }

    fn make_flusher() -> (OtlpTraceFlusher, Arc<RecConv>) {
        let converter = Arc::new(RecConv {
            calls: Mutex::new(Vec::new()),
        });
        let flusher = OtlpTraceFlusher::with_parts(
            cfg(vec![ep("primary", "http://localhost:4318")]),
            None,
            None,
            Some(converter.clone()),
        )
        .unwrap();
        (flusher, converter)
    }

    #[test]
    fn rejects_empty_endpoints_and_service_name() {
        let error = OtlpTraceFlusher::new(cfg(Vec::new())).unwrap_err();
        assert!(error.contains("endpoints must be non-empty"));
        let mut config = cfg(vec![ep("primary", "http://localhost:4318")]);
        config.service_name.clear();
        let error = OtlpTraceFlusher::new(config).unwrap_err();
        assert!(error.contains("serviceName is required"));
        let flusher =
            OtlpTraceFlusher::new(cfg(vec![ep("primary", "http://localhost:4318")])).unwrap();
        assert_eq!(flusher.name(), "otlp-trace");
        assert_eq!(
            std::env::var("OTEL_INSTRUMENTATION_GENAI_CAPTURE_MESSAGE_CONTENT").unwrap(),
            "SPAN_ONLY"
        );
    }

    #[tokio::test]
    async fn normalizes_endpoint_urls_and_cms_identity() {
        let flusher = OtlpTraceFlusher::new(cfg(vec![ep(
            "primary",
            "https://example.com/apm/trace/opentelemetry",
        )]))
        .unwrap();
        flusher
            .export_spans_for_agent("claude-code", Vec::new())
            .await;
        assert_eq!(
            flusher.created_exporters()[0].url,
            "https://example.com/apm/trace/opentelemetry/v1/traces"
        );
        assert_eq!(flusher.created_exporters()[0].compression, "gzip");

        let mut already = ep("primary", "https://example.com/v1/traces");
        already.compression = Some(OtlpCompression::None);
        let flusher = OtlpTraceFlusher::new(cfg(vec![already])).unwrap();
        flusher.export_spans_for_agent("codex", Vec::new()).await;
        assert_eq!(
            flusher.created_exporters()[0].url,
            "https://example.com/v1/traces"
        );
        assert_eq!(flusher.created_exporters()[0].compression, "none");

        let slashed =
            OtlpTraceFlusher::new(cfg(vec![ep("primary", "https://example.com/otlp/")])).unwrap();
        slashed.export_spans_for_agent("codex", Vec::new()).await;
        assert_eq!(
            slashed.created_exporters()[0].url,
            "https://example.com/otlp/v1/traces"
        );

        let cms_url = "https://proj-xtrace-1d3dc285e44fcb12fa8cbcb1dd13551-cn-hongkong.cn-hongkong.log.aliyuncs.com/apm/trace/opentelemetry";
        let mut cms = ep("user-cms", cms_url);
        cms.headers = Some(HashMap::from([
            ("x-arms-license-key".to_string(), "lk".to_string()),
            (
                "x-arms-project".to_string(),
                "proj-xtrace-1d3dc285e44fcb12fa8cbcb1dd13551-cn-hongkong".to_string(),
            ),
        ]));
        let counters = OtlpTraceFlusher::new(cfg(vec![cms]))
            .unwrap()
            .endpoint_counters();
        let counter = counters.get("user-cms").unwrap();
        assert!(counter.is_cms);
        assert_eq!(
            counter.project,
            "proj-xtrace-1d3dc285e44fcb12fa8cbcb1dd13551-cn-hongkong"
        );
        assert_eq!(counter.logstore, "logstore-tracing");

        let mut workspace = ep("inner-cms-0", cms_url);
        workspace.headers = Some(HashMap::from([(
            "X-CMS-Workspace".to_string(),
            "ws-1".to_string(),
        )]));
        let counter = OtlpTraceFlusher::new(cfg(vec![workspace]))
            .unwrap()
            .endpoint_counters();
        let counter = counter.get("inner-cms-0").unwrap();
        assert!(counter.is_cms);
        assert_eq!(
            counter.project,
            "proj-xtrace-1d3dc285e44fcb12fa8cbcb1dd13551-cn-hongkong"
        );

        let mut plain = ep("user-otlp", "https://collector.example.com/v1/traces");
        plain.headers = Some(HashMap::from([(
            "authorization".to_string(),
            "Bearer t".to_string(),
        )]));
        let counter = OtlpTraceFlusher::new(cfg(vec![plain]))
            .unwrap()
            .endpoint_counters()
            .remove("user-otlp")
            .unwrap();
        assert!(!counter.is_cms);
        assert!(counter.project.is_empty());
        assert!(counter.logstore.is_empty());
    }

    #[tokio::test]
    async fn groups_by_turn_then_trace_then_session() {
        let (flusher, converter) = make_flusher();
        let trace = "4bf92f3577b34da6a3ce929d0e0e4736";
        flusher.send(entry(json!({"event.name":"llm.response","gen_ai.agent.type":"claude-code","gen_ai.turn.id":"turn-abc","trace_id":trace,"gen_ai.session.id":"session-1"})), None).await.unwrap();
        flusher.send(entry(json!({"event.name":"llm.response","gen_ai.agent.type":"claude-code","gen_ai.turn.id":"turn-abc","trace_id":"aaaa2f3577b34da6a3ce929d0e0e4736","gen_ai.session.id":"session-1"})), None).await.unwrap();
        flusher.shutdown().await.unwrap();
        assert_eq!(converter.calls.lock().unwrap().len(), 1);
        assert_eq!(converter.calls.lock().unwrap()[0].len(), 2);

        let (flusher, converter) = make_flusher();
        flusher.send(entry(json!({"event.name":"llm.request","gen_ai.agent.type":"claude-code","trace_id":"aaaa2f3577b34da6a3ce929d0e0e4736","gen_ai.session.id":"session-1"})), None).await.unwrap();
        flusher.send(entry(json!({"event.name":"llm.request","gen_ai.agent.type":"claude-code","trace_id":"bbbb2f3577b34da6a3ce929d0e0e4736","gen_ai.session.id":"session-1"})), None).await.unwrap();
        assert_eq!(converter.calls.lock().unwrap().len(), 1);
        flusher.shutdown().await.unwrap();
        assert_eq!(converter.calls.lock().unwrap().len(), 2);

        let (flusher, converter) = make_flusher();
        flusher.send(entry(json!({"event.name":"llm.response","gen_ai.agent.type":"claude-code","trace_id":"invalid-not-32-hex","gen_ai.session.id":"session-1"})), None).await.unwrap();
        flusher.send(entry(json!({"event.name":"llm.response","gen_ai.agent.type":"claude-code","trace_id":"still-invalid","gen_ai.session.id":"session-1"})), None).await.unwrap();
        flusher.shutdown().await.unwrap();
        assert_eq!(converter.calls.lock().unwrap().len(), 1);
        assert_eq!(converter.calls.lock().unwrap()[0].len(), 2);
    }

    #[tokio::test]
    async fn opencode_v2_flushes_only_execution_completion_and_v1_keeps_finish_reason_behavior() {
        for v2 in [false, true] {
            let (flusher, converter) = make_flusher();
            let mut request = json!({"event.name":"llm.request","gen_ai.agent.type":"opencode",
                "gen_ai.session.id":"session-1","gen_ai.turn.id":"turn",
                "trace_id":"4bf92f3577b34da6a3ce929d0e0e4736"});
            if v2 {
                request["agent.opencode.collection"] = json!("v2");
            }
            flusher.send(entry(request.clone()), None).await.unwrap();
            let mut response = request.clone();
            response["event.name"] = json!("llm.response");
            response["gen_ai.response.finish_reasons"] = json!(["stop"]);
            flusher.send(entry(response.clone()), None).await.unwrap();
            if !v2 {
                assert_eq!(converter.calls.lock().unwrap()[0].len(), 2);
                flusher.shutdown().await.unwrap();
                continue;
            }
            assert!(converter.calls.lock().unwrap().is_empty());
            for name in ["tool.call", "tool.result", "llm.request", "llm.response"] {
                let mut record = request.clone();
                record["event.name"] = json!(name);
                record["gen_ai.response.finish_reasons"] = json!(["error"]);
                record["error.type"] = json!("RetryableModelError");
                flusher.send(entry(record), None).await.unwrap();
                assert!(converter.calls.lock().unwrap().is_empty());
            }
            response["event.name"] = json!("other");
            response["gen_ai.turn.end"] = json!(true);
            flusher.send(entry(response), None).await.unwrap();
            assert_eq!(converter.calls.lock().unwrap().len(), 1);
            assert_eq!(converter.calls.lock().unwrap()[0].len(), 7);
            flusher.shutdown().await.unwrap();
            assert_eq!(converter.calls.lock().unwrap().len(), 1);
        }
    }

    #[tokio::test]
    async fn terminal_rules_match_the_typescript_flusher() {
        let (flusher, converter) = make_flusher();
        flusher.send(entry(json!({"event.name":"llm.request","gen_ai.agent.type":"claude-code","trace_id":"4bf92f3577b34da6a3ce929d0e0e4736"})), None).await.unwrap();
        assert!(converter.calls.lock().unwrap().is_empty());
        flusher.send(entry(json!({"event.name":"llm.response","gen_ai.agent.type":"claude-code","trace_id":"4bf92f3577b34da6a3ce929d0e0e4736","gen_ai.response.finish_reasons":["stop"]})), None).await.unwrap();
        assert_eq!(converter.calls.lock().unwrap()[0].len(), 2);

        let (flusher, converter) = make_flusher();
        flusher.send(entry(json!({"event.name":"llm.response","gen_ai.agent.type":"claude-code","trace_id":"4bf92f3577b34da6a3ce929d0e0e4736","gen_ai.response.finish_reasons":["tool_calls"]})), None).await.unwrap();
        assert!(converter.calls.lock().unwrap().is_empty());

        let (flusher, converter) = make_flusher();
        flusher.send(entry(json!({"event.name":"llm.request","gen_ai.agent.type":"claude-code","trace_id":"4bf92f3577b34da6a3ce929d0e0e4736"})), None).await.unwrap();
        flusher.send(entry(json!({"event.name":"llm.response","gen_ai.agent.type":"claude-code","trace_id":"4bf92f3577b34da6a3ce929d0e0e4736","gen_ai.response.finish_reasons":["error"],"error.type":"RateLimitError"})), None).await.unwrap();
        assert!(converter.calls.lock().unwrap().is_empty());
        flusher.send(entry(json!({"event.name":"llm.response","gen_ai.agent.type":"claude-code","trace_id":"4bf92f3577b34da6a3ce929d0e0e4736","gen_ai.response.finish_reasons":["error"],"gen_ai.turn.end":true})), None).await.unwrap();
        assert_eq!(converter.calls.lock().unwrap()[0].len(), 3);

        let (flusher, converter) = make_flusher();
        let turn = "parent:turn-1";
        for extra in [
            json!({"event.name":"llm.request"}),
            json!({"gen_ai.response.finish_reasons":["stop"]}),
            json!({"gen_ai.agent.scope":"subagent","gen_ai.response.finish_reasons":["stop"]}),
            json!({"gen_ai.response.finish_reasons":["stop"]}),
        ] {
            let mut value = json!({"event.name":"llm.response", "gen_ai.turn.id": turn, "gen_ai.agent.type":"codex"});
            if let (Value::Object(base), Value::Object(extra)) = (&mut value, extra) {
                for (key, item) in extra {
                    base.insert(key, item);
                }
            }
            flusher.send(entry(value), None).await.unwrap();
        }
        assert!(converter.calls.lock().unwrap().is_empty());
        flusher.send(entry(json!({"gen_ai.turn.id": turn, "gen_ai.agent.type":"codex","event.name":"other","agent.codex.turn_status":"completed","gen_ai.turn.end":true})), None).await.unwrap();
        assert_eq!(converter.calls.lock().unwrap()[0].len(), 5);

        let (flusher, converter) = make_flusher();
        flusher.send(entry(json!({"event.name":"llm.response","gen_ai.agent.type":"openclaw","gen_ai.turn.id":"oc","trace_id":"4bf92f3577b34da6a3ce929d0e0e4736","agent.openclaw.hook":"before_message_write","gen_ai.response.finish_reasons":["stop"]})), None).await.unwrap();
        assert!(converter.calls.lock().unwrap().is_empty());
        flusher.send(entry(json!({"event.name":"other","gen_ai.agent.type":"openclaw","gen_ai.turn.id":"oc","trace_id":"4bf92f3577b34da6a3ce929d0e0e4736","agent.openclaw.hook":"llm_output"})), None).await.unwrap();
        assert_eq!(converter.calls.lock().unwrap()[0].len(), 2);
    }

    #[tokio::test]
    async fn signal_b_requires_the_same_session_and_shutdown_backfills_turn_id() {
        let (flusher, converter) = make_flusher();
        flusher.send(entry(json!({"event.name":"llm.request","gen_ai.agent.type":"claude-code","trace_id":"aaaa2f3577b34da6a3ce929d0e0e4736"})), None).await.unwrap();
        flusher.send(entry(json!({"event.name":"llm.request","gen_ai.agent.type":"claude-code","trace_id":"bbbb2f3577b34da6a3ce929d0e0e4736"})), None).await.unwrap();
        assert!(converter.calls.lock().unwrap().is_empty());
        flusher.send(entry(json!({"event.name":"llm.response","gen_ai.agent.type":"claude-code","trace_id":"aaaa2f3577b34da6a3ce929d0e0e4736","gen_ai.response.finish_reasons":["stop"]})), None).await.unwrap();
        assert_eq!(converter.calls.lock().unwrap()[0].len(), 2);

        let (flusher, converter) = make_flusher();
        let trace = "aaaa2f3577b34da6a3ce929d0e0e4736";
        flusher.send(entry(json!({"event.name":"llm.request","gen_ai.agent.type":"claude-code","trace_id":trace})), None).await.unwrap();
        flusher.send(entry(json!({"event.name":"llm.response","gen_ai.agent.type":"claude-code","trace_id":trace,"gen_ai.response.finish_reasons":["stop"]})), None).await.unwrap();
        for record in &converter.calls.lock().unwrap()[0] {
            assert_eq!(entry_str(record, "gen_ai.turn.id").as_deref(), Some(trace));
        }
    }

    #[tokio::test]
    async fn send_batch_defers_signal_a_until_the_batch_is_appended() {
        let (flusher, converter) = make_flusher();
        let trace = "4bf92f3577b34da6a3ce929d0e0e4736";
        flusher.send_batch(vec![
            entry(json!({"event.name":"llm.request","gen_ai.agent.type":"claude-code","trace_id":trace})),
            entry(json!({"event.name":"llm.response","gen_ai.agent.type":"claude-code","trace_id":trace,"gen_ai.response.finish_reasons":["stop"]})),
            entry(json!({"event.name":"tool.result","gen_ai.agent.type":"claude-code","trace_id":trace})),
        ], None).await.unwrap();
        assert_eq!(converter.calls.lock().unwrap().len(), 1);
        assert_eq!(converter.calls.lock().unwrap()[0].len(), 3);
    }

    #[tokio::test]
    async fn orphan_starts_are_removed_at_the_conversion_boundary() {
        let (flusher, converter) = make_flusher();
        flusher.send_batch(vec![
            entry(json!({"event.name":"other", "gen_ai.agent.type":"mimo-code", "gen_ai.turn.id":"orphan", "gen_ai.input.messages_delta":[]})),
            entry(json!({"event.name":"llm.request", "gen_ai.agent.type":"mimo-code", "gen_ai.turn.id":"orphan", "gen_ai.step.id":"s1"})),
            entry(json!({"event.name":"tool.call", "gen_ai.agent.type":"mimo-code", "gen_ai.turn.id":"orphan", "gen_ai.tool.call.id":"call-1"})),
            entry(json!({"event.name":"tool.call", "gen_ai.agent.type":"mimo-code", "gen_ai.turn.id":"orphan", "gen_ai.tool.call.id":"call-2"})),
        ], None).await.unwrap();
        flusher.shutdown().await.unwrap();
        let calls = converter.calls.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].len(), 1);
        assert_eq!(
            calls[0][0].event_name,
            Some(crate::types::AgentEventName::Other)
        );
    }

    #[tokio::test]
    async fn exports_batches_and_sanitizes_failed_log_names() {
        let calls = Arc::new(AtomicUsize::new(0));
        let seen = Arc::clone(&calls);
        let factory: ExporterFactory = Arc::new(move |_opts| {
            let seen = Arc::clone(&seen);
            Arc::new(CountExporter { seen }) as Arc<dyn TraceExporter>
        });
        struct CountExporter {
            seen: Arc<AtomicUsize>,
        }
        #[async_trait]
        impl TraceExporter for CountExporter {
            async fn export(&self, spans: Vec<ReadableSpan>) -> Result<(), String> {
                self.seen.fetch_add(spans.len(), Ordering::SeqCst);
                Ok(())
            }
            async fn shutdown(&self) {}
        }
        let mut config = cfg(vec![ep("primary", "http://localhost:4318")]);
        config.max_export_batch_bytes = Some(10 * 1024);
        let flusher = OtlpTraceFlusher::with_parts(config, None, Some(factory), None).unwrap();
        let mut spans = Vec::new();
        for _ in 0..5 {
            let mut span = ReadableSpan::minimal("s", &"a".repeat(32), &"b".repeat(16));
            span.attributes.insert(
                "gen_ai.agent.type".to_string(),
                Value::String("claude-code".to_string()),
            );
            span.attributes.insert(
                "gen_ai.input.messages".to_string(),
                Value::String("x".repeat(4096)),
            );
            spans.push(span);
        }
        flusher.export_spans_for_agent("claude-code", spans).await;
        assert_eq!(calls.load(Ordering::SeqCst), 5);

        let dir = tempfile::tempdir().unwrap();
        let bad = ep("../../../../tmp/pwn", "http://evil:4318");
        let mut config = cfg(vec![bad.clone()]);
        config.data_dir = Some(dir.path().to_string_lossy().to_string());
        config.service_name = "test-pilot".to_string();
        struct FailExport;
        #[async_trait]
        impl TraceExporter for FailExport {
            async fn export(&self, _spans: Vec<ReadableSpan>) -> Result<(), String> {
                Err("boom".to_string())
            }
            async fn shutdown(&self) {}
        }
        let flusher = OtlpTraceFlusher::with_parts(
            config,
            None,
            Some(Arc::new(|_| Arc::new(FailExport) as Arc<dyn TraceExporter>)),
            None,
        )
        .unwrap();
        flusher
            .export_spans_for_agent("claude-code", vec![ReadableSpan::minimal("s", "a", "b")])
            .await;
        let failed = std::fs::read_dir(dir.path().join("logs").join("otlp-failed"))
            .unwrap()
            .next()
            .unwrap()
            .unwrap();
        let name = failed.file_name().to_string_lossy().to_string();
        assert!(name.contains(".._.._.._.._tmp_pwn"));
        assert!(!name.contains("/tmp/pwn"));
        let text = std::fs::read_to_string(failed.path()).unwrap();
        let parsed: Value = serde_json::from_str(text.lines().next().unwrap()).unwrap();
        assert!(parsed["_error"]["message"]
            .as_str()
            .unwrap()
            .contains("boom"));
        let _ = bad;
    }

    #[test]
    fn service_names_can_disable_the_agent_suffix() {
        let user = ep("user", "http://a:4318");
        let mut inner = ep("inner", "http://b:4318");
        inner.service_name = Some("managed-svc".to_string());
        let mut config = cfg(vec![user, inner]);
        config.append_agent_type_to_service_name = Some(false);
        config.service_name = "shared-service".to_string();
        let flusher = OtlpTraceFlusher::new(config).unwrap();
        assert_eq!(
            flusher.endpoint_service_name(0, "opencode"),
            "shared-service"
        );
        assert_eq!(flusher.endpoint_service_name(1, "codex"), "managed-svc");
        let resource = flusher.resource_attributes("codex", "shared-service");
        assert_eq!(resource["service.name"], "shared-service");
        assert_eq!(resource["gen_ai.agent.type"], "codex");
        assert_eq!(resource["service.namespace"], "loongsuite-pilot");
    }

    #[tokio::test]
    async fn runtime_snapshot_counts_pending_buffers() {
        let (flusher, _) = make_flusher();
        flusher.send(entry(json!({"event.name":"llm.request","gen_ai.agent.type":"claude-code","gen_ai.turn.id":"t1"})), Some(10.0)).await.unwrap();
        let snapshot = flusher.trace_runtime_snapshot();
        assert_eq!(snapshot.len(), 1);
        assert_eq!(snapshot[0].agent_type, "claude-code");
        assert_eq!(snapshot[0].pending_buffers, 1);
        assert_eq!(snapshot[0].pending_records, 1);
        assert_eq!(snapshot[0].pending_logical_bytes, 10.0);
        assert_eq!(snapshot[0].largest_buffer_turn_id.as_deref(), Some("t1"));
    }
}
