//! Per-input offset checkpoint.
//!
//! The on-disk document is a JSON object keyed by input id. `save` coalesces
//! concurrent callers onto one flush loop: each iteration snapshots the map,
//! writes it, then repeats if `set` / `update` / `delete` ran during the write.

use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use tokio::sync::oneshot;

use super::json_file::{self, JVal};
use super::OrderedMap;

const LAST_OFFSET: &str = "lastOffset";
const LAST_FILE: &str = "lastFile";
const LAST_ROW_ID: &str = "lastRowId";
const LAST_TIMESTAMP: &str = "lastTimestamp";
const HIGH_WATERMARK: &str = "highWatermark";
const EXTRA: &str = "extra";

type WriteHook = Arc<
    dyn Fn(PathBuf, String) -> Pin<Box<dyn Future<Output = Result<(), anyhow::Error>> + Send>>
        + Send
        + Sync,
>;

/// One input's persisted checkpoint.
///
/// Field order follows insertion. Unknown JSON keys are kept and written back.
/// `get` returns an owned copy.
#[derive(Clone, Debug, Default)]
pub struct InputState {
    fields: Vec<(String, JVal)>,
}

impl PartialEq for InputState {
    fn eq(&self, other: &Self) -> bool {
        if self.fields.len() != other.fields.len() {
            return false;
        }
        self.fields
            .iter()
            .all(|(key, value)| other.field(key) == Some(value))
    }
}

impl InputState {
    pub fn new() -> Self {
        Self { fields: Vec::new() }
    }

    pub fn last_offset(&self) -> Option<f64> {
        self.field_f64(LAST_OFFSET)
    }

    pub fn last_file(&self) -> Option<&str> {
        self.field_str(LAST_FILE)
    }

    pub fn last_row_id(&self) -> Option<f64> {
        self.field_f64(LAST_ROW_ID)
    }

    pub fn last_timestamp(&self) -> Option<f64> {
        self.field_f64(LAST_TIMESTAMP)
    }

    pub fn high_watermark(&self) -> Option<f64> {
        self.field_f64(HIGH_WATERMARK)
    }

    /// `None` when the key is absent. JSON `null` is `Some(Null)`.
    pub fn extra(&self) -> Option<serde_json::Value> {
        self.get_field(EXTRA)
    }

    pub fn get_field(&self, key: &str) -> Option<serde_json::Value> {
        self.field(key).map(json_file::to_value)
    }

    pub fn with_last_offset(mut self, value: f64) -> Self {
        self.insert_number(LAST_OFFSET, value);
        self
    }

    pub fn with_last_file(mut self, value: impl Into<String>) -> Self {
        self.insert(LAST_FILE, JVal::String(value.into()));
        self
    }

    pub fn with_last_row_id(mut self, value: f64) -> Self {
        self.insert_number(LAST_ROW_ID, value);
        self
    }

    pub fn with_last_timestamp(mut self, value: f64) -> Self {
        self.insert_number(LAST_TIMESTAMP, value);
        self
    }

    pub fn with_high_watermark(mut self, value: f64) -> Self {
        self.insert_number(HIGH_WATERMARK, value);
        self
    }

    pub fn with_extra(mut self, extra: serde_json::Value) -> Self {
        self.insert(EXTRA, normalize_extra(json_file::from_value(extra)));
        self
    }

    pub fn set_last_offset(&mut self, value: f64) {
        self.insert_number(LAST_OFFSET, value);
    }

    fn insert_number(&mut self, key: &str, value: f64) {
        self.insert(key, json_file::number_from_f64(value));
    }

    fn field_f64(&self, key: &str) -> Option<f64> {
        match self.field(key)? {
            JVal::Number(number) => number.as_f64(),
            _ => None,
        }
    }

    fn field_str(&self, key: &str) -> Option<&str> {
        match self.field(key)? {
            JVal::String(value) => Some(value.as_str()),
            _ => None,
        }
    }

    fn field(&self, key: &str) -> Option<&JVal> {
        self.fields
            .iter()
            .find(|(candidate, _)| candidate == key)
            .map(|(_, value)| value)
    }

    fn insert(&mut self, key: &str, value: JVal) {
        if let Some((_, slot)) = self
            .fields
            .iter_mut()
            .find(|(candidate, _)| candidate == key)
        {
            *slot = value;
            return;
        }
        self.fields.push((key.to_owned(), value));
    }

    fn to_jval(&self) -> JVal {
        JVal::Object(self.fields.clone())
    }

    /// `cloneState`: array `extra` becomes an object with decimal index keys.
    fn normalize(&mut self) {
        let Some(index) = self.fields.iter().position(|(key, _)| key == EXTRA) else {
            return;
        };
        let current = std::mem::replace(&mut self.fields[index].1, JVal::Null);
        self.fields[index].1 = normalize_extra(current);
    }

    fn from_jval(value: JVal) -> Option<Self> {
        let fields = match value {
            JVal::Object(fields) => fields,
            JVal::Array(items) => items
                .into_iter()
                .enumerate()
                .map(|(index, value)| (index.to_string(), value))
                .collect(),
            JVal::Null | JVal::Bool(_) | JVal::Number(_) | JVal::String(_) => return None,
        };
        let mut state = Self { fields };
        state.normalize();
        Some(state)
    }
}

fn normalize_extra(value: JVal) -> JVal {
    match value {
        JVal::Array(items) => JVal::Object(
            items
                .into_iter()
                .enumerate()
                .map(|(index, value)| (index.to_string(), value))
                .collect(),
        ),
        JVal::Null | JVal::Bool(_) | JVal::Number(_) | JVal::String(_) | JVal::Object(_) => value,
    }
}

/// Fields left as `None` are unchanged. `Some` overwrites that field.
///
/// `extra` uses the TypeScript `update` rule: two object-like values are
/// shallow-merged; anything else replaces `extra`. Arrays count as object-like
/// and are stored as index-keyed objects.
#[derive(Clone, Debug, Default)]
pub struct InputStatePatch {
    pub last_offset: Option<f64>,
    pub last_file: Option<String>,
    pub last_row_id: Option<f64>,
    pub last_timestamp: Option<f64>,
    pub high_watermark: Option<f64>,
    pub extra: Option<serde_json::Value>,
}

fn apply_patch(state: &mut InputState, patch: InputStatePatch) {
    if let Some(value) = patch.last_offset {
        state.insert_number(LAST_OFFSET, value);
    }
    if let Some(value) = patch.last_file {
        state.insert(LAST_FILE, JVal::String(value));
    }
    if let Some(value) = patch.last_row_id {
        state.insert_number(LAST_ROW_ID, value);
    }
    if let Some(value) = patch.last_timestamp {
        state.insert_number(LAST_TIMESTAMP, value);
    }
    if let Some(value) = patch.high_watermark {
        state.insert_number(HIGH_WATERMARK, value);
    }
    if let Some(extra) = patch.extra {
        apply_extra(state, json_file::from_value(extra));
    }
    state.normalize();
}

fn apply_extra(state: &mut InputState, partial_extra: JVal) {
    let partial_is_object_like = is_object_like(&partial_extra);
    let current_is_object_like = state.field(EXTRA).is_some_and(is_object_like);
    if partial_is_object_like && current_is_object_like {
        let base = object_like_fields(state.field(EXTRA).cloned().unwrap_or(JVal::Null));
        let overlay = object_like_fields(partial_extra);
        state.insert(EXTRA, JVal::Object(merge_fields(base, overlay)));
        return;
    }
    state.insert(EXTRA, partial_extra);
}

fn is_object_like(value: &JVal) -> bool {
    match value {
        JVal::Object(_) | JVal::Array(_) => true,
        JVal::Null | JVal::Bool(_) | JVal::Number(_) | JVal::String(_) => false,
    }
}

fn object_like_fields(value: JVal) -> Vec<(String, JVal)> {
    match value {
        JVal::Object(fields) => fields,
        JVal::Array(items) => items
            .into_iter()
            .enumerate()
            .map(|(index, value)| (index.to_string(), value))
            .collect(),
        JVal::Null | JVal::Bool(_) | JVal::Number(_) | JVal::String(_) => Vec::new(),
    }
}

fn merge_fields(
    mut base: Vec<(String, JVal)>,
    overlay: Vec<(String, JVal)>,
) -> Vec<(String, JVal)> {
    for (key, value) in overlay {
        if let Some((_, slot)) = base.iter_mut().find(|(existing, _)| existing == &key) {
            *slot = value;
        } else {
            base.push((key, value));
        }
    }
    base
}

struct Waiter {
    tx: oneshot::Sender<Result<(), String>>,
}

#[derive(Default)]
struct StoreData {
    states: OrderedMap<InputState>,
    revision: u64,
    persisted_revision: u64,
    flushing: bool,
    waiters: Vec<Waiter>,
}

/// In-memory input checkpoints backed by one JSON file.
pub struct StateStore {
    file_path: PathBuf,
    mu: Mutex<StoreData>,
    write_hook: Mutex<Option<WriteHook>>,
}

impl std::fmt::Debug for StateStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("StateStore")
            .field("file_path", &self.file_path)
            .finish()
    }
}

impl StateStore {
    pub fn new(file_path: impl AsRef<Path>) -> Self {
        Self {
            file_path: file_path.as_ref().to_path_buf(),
            mu: Mutex::new(StoreData::default()),
            write_hook: Mutex::new(None),
        }
    }

    /// Loads the file. Missing or corrupt JSON becomes an empty map.
    /// Revisions reset to zero, so a following `save` is a no-op until a mutation.
    pub async fn load(&self) {
        let parsed = json_file::read_json(&self.file_path).await;
        let mut data = self.mu.lock().unwrap();
        data.states.clear();
        data.revision = 0;
        data.persisted_revision = 0;
        let entries = match parsed {
            None => return,
            Some(JVal::Object(fields)) => fields,
            Some(JVal::Array(items)) => items
                .into_iter()
                .enumerate()
                .map(|(index, value)| (index.to_string(), value))
                .collect::<Vec<_>>(),
            Some(JVal::Null | JVal::Bool(_) | JVal::Number(_) | JVal::String(_)) => return,
        };
        for (id, value) in entries {
            if let Some(state) = InputState::from_jval(value) {
                data.states.insert(id, state);
            }
        }
    }

    pub async fn save(&self) -> Result<(), anyhow::Error> {
        let (tx, rx) = oneshot::channel();
        let lead = {
            let mut data = self.mu.lock().unwrap();
            if data.persisted_revision == data.revision {
                return Ok(());
            }
            data.waiters.push(Waiter { tx });
            if data.flushing {
                false
            } else {
                data.flushing = true;
                true
            }
        };

        if lead {
            let mut guard = LeaderGuard {
                store: self,
                armed: true,
            };
            self.flush_pending().await;
            guard.armed = false;
        }

        match rx.await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(message)) => Err(anyhow::Error::msg(message)),
            Err(_) => Err(anyhow::Error::msg("state store save waiter dropped")),
        }
    }

    pub fn get(&self, input_id: &str) -> InputState {
        self.mu
            .lock()
            .unwrap()
            .states
            .get(input_id)
            .cloned()
            .unwrap_or_default()
    }

    pub fn set(&self, input_id: &str, mut state: InputState) {
        state.normalize();
        let mut data = self.mu.lock().unwrap();
        data.states.insert(input_id.to_owned(), state);
        data.revision += 1;
    }

    pub fn update(&self, input_id: &str, patch: InputStatePatch) {
        let mut data = self.mu.lock().unwrap();
        let mut state = data.states.get(input_id).cloned().unwrap_or_default();
        apply_patch(&mut state, patch);
        data.states.insert(input_id.to_owned(), state);
        data.revision += 1;
    }

    pub fn delete(&self, input_id: &str) -> bool {
        let mut data = self.mu.lock().unwrap();
        let removed = data.states.remove(input_id).is_some();
        if removed {
            data.revision += 1;
        }
        removed
    }

    pub fn keys(&self) -> Vec<String> {
        self.mu
            .lock()
            .unwrap()
            .states
            .keys()
            .map(str::to_owned)
            .collect()
    }

    pub fn get_offset(&self, input_id: &str) -> f64 {
        self.get(input_id).last_offset().unwrap_or(0.0)
    }

    pub fn set_offset(&self, input_id: &str, offset: f64) {
        self.update(
            input_id,
            InputStatePatch {
                last_offset: Some(offset),
                ..InputStatePatch::default()
            },
        );
    }

    pub fn get_row_id(&self, input_id: &str) -> f64 {
        self.get(input_id).last_row_id().unwrap_or(0.0)
    }

    pub fn set_row_id(&self, input_id: &str, row_id: f64) {
        self.update(
            input_id,
            InputStatePatch {
                last_row_id: Some(row_id),
                ..InputStatePatch::default()
            },
        );
    }

    async fn flush_pending(&self) {
        loop {
            let step = {
                let mut data = self.mu.lock().unwrap();
                if data.persisted_revision >= data.revision {
                    data.flushing = false;
                    let waiters = std::mem::take(&mut data.waiters);
                    drop(data);
                    notify(waiters, Ok(()));
                    return;
                }
                let target = data.revision;
                let text = render_states(&data.states);
                (target, text)
            };

            match self.persist_text(&step.1).await {
                Ok(()) => {
                    self.mu.lock().unwrap().persisted_revision = step.0;
                }
                Err(err) => {
                    let message = err.to_string();
                    let mut data = self.mu.lock().unwrap();
                    data.flushing = false;
                    let waiters = std::mem::take(&mut data.waiters);
                    drop(data);
                    notify(waiters, Err(message));
                    return;
                }
            }
        }
    }

    fn abort_flush(&self) {
        let mut data = self.mu.lock().unwrap();
        if !data.flushing {
            return;
        }
        data.flushing = false;
        let waiters = std::mem::take(&mut data.waiters);
        drop(data);
        notify(waiters, Err("state store save cancelled".to_owned()));
    }

    async fn persist_text(&self, text: &str) -> Result<(), anyhow::Error> {
        let hook = self.write_hook.lock().unwrap().clone();
        if let Some(hook) = hook {
            return hook(self.file_path.clone(), text.to_owned()).await;
        }
        json_file::write_text_atomic(&self.file_path, text).await
    }

    #[cfg(test)]
    fn set_write_hook<F, Fut>(&self, hook: F)
    where
        F: Fn(PathBuf, String) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), anyhow::Error>> + Send + 'static,
    {
        let boxed: WriteHook = Arc::new(
            move |path, text| -> Pin<Box<dyn Future<Output = Result<(), anyhow::Error>> + Send>> {
                Box::pin(hook(path, text))
            },
        );
        *self.write_hook.lock().unwrap() = Some(boxed);
    }

    #[cfg(test)]
    fn inflight_waiters(&self) -> usize {
        self.mu.lock().unwrap().waiters.len()
    }
}

fn notify(waiters: Vec<Waiter>, result: Result<(), String>) {
    for waiter in waiters {
        let _ = waiter.tx.send(result.clone());
    }
}

fn render_states(states: &OrderedMap<InputState>) -> String {
    let fields: Vec<(String, JVal)> = states
        .iter()
        .map(|(key, state)| (key.to_owned(), state.to_jval()))
        .collect();
    format!("{}\n", json_file::to_pretty(&JVal::Object(fields)))
}

struct LeaderGuard<'a> {
    store: &'a StateStore,
    armed: bool,
}

impl Drop for LeaderGuard<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.store.abort_flush();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    use tokio::sync::oneshot;

    use super::super::json_file::write_text_atomic;
    use super::*;

    fn setup(name: &str) -> (tempfile::TempDir, std::path::PathBuf, StateStore) {
        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join(name);
        let store = StateStore::new(&file_path);
        (dir, file_path, store)
    }

    async fn read_text(path: &Path) -> String {
        tokio::fs::read_to_string(path).await.unwrap()
    }

    #[tokio::test]
    async fn starts_with_empty_state() {
        let (_dir, _path, store) = setup("state.json");
        store.load().await;
        assert_eq!(store.get("unknown"), InputState::new());
    }

    #[tokio::test]
    async fn persists_state_across_save_load() {
        let (_dir, file_path, store) = setup("state.json");
        store.load().await;
        store.set(
            "input-a",
            InputState::new()
                .with_last_offset(100.0)
                .with_last_row_id(5.0),
        );
        store.save().await.unwrap();

        let body = read_text(&file_path).await;
        assert_eq!(
            body,
            "{\n  \"input-a\": {\n    \"lastOffset\": 100,\n    \"lastRowId\": 5\n  }\n}\n"
        );

        let store2 = StateStore::new(&file_path);
        store2.load().await;
        assert_eq!(
            store2.get("input-a"),
            InputState::new()
                .with_last_offset(100.0)
                .with_last_row_id(5.0)
        );
    }

    #[tokio::test]
    async fn handles_missing_file() {
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("nonexistent.json");
        let store = StateStore::new(&missing);
        store.load().await;
        assert_eq!(store.get("any"), InputState::new());
    }

    #[tokio::test]
    async fn handles_corrupt_json() {
        let (_dir, file_path, store) = setup("state.json");
        tokio::fs::write(&file_path, "not json!!").await.unwrap();
        store.load().await;
        assert_eq!(store.get("any"), InputState::new());
    }

    #[tokio::test]
    async fn strips_utf8_bom() {
        let (_dir, file_path, store) = setup("state.json");
        tokio::fs::write(&file_path, "\u{feff}{\"a\":{\"lastOffset\":7}}")
            .await
            .unwrap();
        store.load().await;
        assert_eq!(store.get_offset("a"), 7.0);
    }

    #[tokio::test]
    async fn get_returns_empty_state_for_unknown_id() {
        let (_dir, _path, store) = setup("state.json");
        store.load().await;
        assert_eq!(store.get("nonexistent"), InputState::new());
    }

    #[tokio::test]
    async fn sets_and_retrieves_state() {
        let (_dir, _path, store) = setup("state.json");
        store.load().await;
        store.set("x", InputState::new().with_last_offset(42.0));
        assert_eq!(store.get("x").last_offset(), Some(42.0));
    }

    #[tokio::test]
    async fn updates_partial_state_without_losing_fields() {
        let (_dir, _path, store) = setup("state.json");
        store.load().await;
        store.set(
            "x",
            InputState::new()
                .with_last_offset(10.0)
                .with_last_row_id(20.0),
        );
        store.update(
            "x",
            InputStatePatch {
                last_offset: Some(30.0),
                ..InputStatePatch::default()
            },
        );
        let state = store.get("x");
        assert_eq!(state.last_offset(), Some(30.0));
        assert_eq!(state.last_row_id(), Some(20.0));
    }

    #[tokio::test]
    async fn set_stores_an_independent_copy() {
        let (_dir, _path, store) = setup("state.json");
        store.load().await;
        let mut original = InputState::new()
            .with_last_offset(10.0)
            .with_extra(serde_json::json!({"foo": "bar"}));
        store.set("x", original.clone());
        original.set_last_offset(999.0);
        assert_eq!(store.get("x").last_offset(), Some(10.0));
        assert_eq!(
            store.get("x").extra(),
            Some(serde_json::json!({"foo": "bar"}))
        );
    }

    #[tokio::test]
    async fn get_offset_returns_zero_for_unknown_input() {
        let (_dir, _path, store) = setup("state.json");
        store.load().await;
        assert_eq!(store.get_offset("unknown"), 0.0);
    }

    #[tokio::test]
    async fn sets_and_gets_offset() {
        let (_dir, _path, store) = setup("state.json");
        store.load().await;
        store.set_offset("input-a", 256.0);
        assert_eq!(store.get_offset("input-a"), 256.0);
    }

    #[tokio::test]
    async fn get_row_id_returns_zero_for_unknown_input() {
        let (_dir, _path, store) = setup("state.json");
        store.load().await;
        assert_eq!(store.get_row_id("unknown"), 0.0);
    }

    #[tokio::test]
    async fn sets_and_gets_row_id() {
        let (_dir, _path, store) = setup("state.json");
        store.load().await;
        store.set_row_id("input-b", 42.0);
        assert_eq!(store.get_row_id("input-b"), 42.0);
    }

    #[tokio::test]
    async fn does_not_write_without_a_new_revision() {
        let (_dir, file_path, store) = setup("state.json");
        store.load().await;
        store.save().await.unwrap();
        assert!(tokio::fs::metadata(&file_path).await.is_err());
    }

    #[tokio::test]
    async fn writes_when_the_revision_changes() {
        let (_dir, file_path, store) = setup("state.json");
        store.load().await;
        store.set("x", InputState::new().with_last_offset(1.0));
        store.save().await.unwrap();
        assert!(tokio::fs::metadata(&file_path).await.is_ok());
    }

    #[tokio::test]
    async fn does_not_rewrite_an_already_persisted_revision() {
        let (_dir, _path, store) = setup("state.json");
        let writes = Arc::new(AtomicUsize::new(0));
        let writes_hook = Arc::clone(&writes);
        store.set_write_hook(move |path, text| {
            let writes_hook = Arc::clone(&writes_hook);
            async move {
                writes_hook.fetch_add(1, Ordering::SeqCst);
                write_text_atomic(&path, &text).await
            }
        });
        store.load().await;
        store.set("x", InputState::new().with_last_offset(1.0));
        store.save().await.unwrap();
        store.save().await.unwrap();
        assert_eq!(writes.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn serializes_saves_and_persists_mutations_during_write() {
        let (_dir, file_path, store) = setup("state.json");
        let store = Arc::new(store);
        store.load().await;
        store.set("input-a", InputState::new().with_last_offset(1.0));

        let write_count = Arc::new(AtomicUsize::new(0));
        let active = Arc::new(AtomicUsize::new(0));
        let max_active = Arc::new(AtomicUsize::new(0));
        let payloads = Arc::new(Mutex::new(Vec::new()));
        let (started_tx, started_rx) = oneshot::channel::<()>();
        let (release_tx, release_rx) = oneshot::channel::<()>();
        let started_tx = Arc::new(Mutex::new(Some(started_tx)));
        let release_rx = Arc::new(Mutex::new(Some(release_rx)));

        let write_count_hook = Arc::clone(&write_count);
        let active_hook = Arc::clone(&active);
        let max_active_hook = Arc::clone(&max_active);
        let payloads_hook = Arc::clone(&payloads);
        store.set_write_hook(move |path, text| {
            let write_count_hook = Arc::clone(&write_count_hook);
            let active_hook = Arc::clone(&active_hook);
            let max_active_hook = Arc::clone(&max_active_hook);
            let payloads_hook = Arc::clone(&payloads_hook);
            let started_tx = Arc::clone(&started_tx);
            let release_rx = Arc::clone(&release_rx);
            async move {
                let current = active_hook.fetch_add(1, Ordering::SeqCst) + 1;
                max_active_hook.fetch_max(current, Ordering::SeqCst);
                let write_index = write_count_hook.fetch_add(1, Ordering::SeqCst) + 1;
                payloads_hook.lock().unwrap().push(text.clone());
                if write_index == 1 {
                    if let Some(tx) = started_tx.lock().unwrap().take() {
                        let _ = tx.send(());
                    }
                    let pending = release_rx.lock().unwrap().take();
                    if let Some(rx) = pending {
                        let _ = rx.await;
                    }
                }
                let result = write_text_atomic(&path, &text).await;
                active_hook.fetch_sub(1, Ordering::SeqCst);
                result
            }
        });

        let first_store = Arc::clone(&store);
        let first = tokio::spawn(async move { first_store.save().await });
        started_rx.await.unwrap();
        store.update(
            "input-a",
            InputStatePatch {
                last_offset: Some(2.0),
                ..InputStatePatch::default()
            },
        );
        store.set("input-b", InputState::new().with_last_row_id(3.0));
        let second_store = Arc::clone(&store);
        let second = tokio::spawn(async move { second_store.save().await });
        let mut joined = false;
        for _ in 0..50 {
            if store.inflight_waiters() >= 2 {
                joined = true;
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(joined, "concurrent save did not join the in-flight flush");
        release_tx.send(()).unwrap();
        first.await.unwrap().unwrap();
        second.await.unwrap().unwrap();

        assert_eq!(write_count.load(Ordering::SeqCst), 2);
        assert_eq!(max_active.load(Ordering::SeqCst), 1);
        let payloads = payloads.lock().unwrap().clone();
        assert_eq!(
            payloads[0],
            "{\n  \"input-a\": {\n    \"lastOffset\": 1\n  }\n}\n"
        );
        assert_eq!(
            payloads[1],
            "{\n  \"input-a\": {\n    \"lastOffset\": 2\n  },\n  \"input-b\": {\n    \"lastRowId\": 3\n  }\n}\n"
        );
        let persisted: serde_json::Value =
            serde_json::from_str(&read_text(&file_path).await).unwrap();
        assert_eq!(
            persisted,
            serde_json::json!({
                "input-a": { "lastOffset": 2 },
                "input-b": { "lastRowId": 3 }
            })
        );
    }

    #[tokio::test]
    async fn retains_unpersisted_revision_after_failed_write() {
        let (_dir, file_path, store) = setup("state.json");
        store.load().await;
        store.set("input-a", InputState::new().with_last_offset(10.0));

        let calls = Arc::new(AtomicUsize::new(0));
        let calls_hook = Arc::clone(&calls);
        store.set_write_hook(move |path, text| {
            let calls_hook = Arc::clone(&calls_hook);
            async move {
                let call = calls_hook.fetch_add(1, Ordering::SeqCst);
                if call == 0 {
                    Err(anyhow::anyhow!("synthetic write failure"))
                } else {
                    write_text_atomic(&path, &text).await
                }
            }
        });

        let err = store.save().await.unwrap_err();
        assert!(err.to_string().contains("synthetic write failure"), "{err}");
        store.save().await.unwrap();

        let persisted: serde_json::Value =
            serde_json::from_str(&read_text(&file_path).await).unwrap();
        assert_eq!(
            persisted,
            serde_json::json!({ "input-a": { "lastOffset": 10 } })
        );
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn preserves_insertion_order_unknown_fields_and_extra_merge() {
        let (_dir, file_path, store) = setup("state.json");
        let raw = "{\n  \"b\": { \"lastOffset\": 1 },\n  \"agent\": {\n    \"lastOffset\": 1,\n    \"custom\": true,\n    \"extra\": {\n      \"a\": 1,\n      \"b\": 2\n    }\n  }\n}\n";
        tokio::fs::write(&file_path, raw).await.unwrap();
        store.load().await;
        store.set_offset("a", 2.0);
        store.update(
            "agent",
            InputStatePatch {
                last_offset: Some(5.0),
                extra: Some(serde_json::json!({"b": 3, "c": 4})),
                ..InputStatePatch::default()
            },
        );
        store.delete("b");
        assert!(!store.delete("b"));
        assert_eq!(store.keys(), vec!["agent".to_owned(), "a".to_owned()]);
        store.save().await.unwrap();
        let body = read_text(&file_path).await;
        assert_eq!(
            body,
            "{\n  \"agent\": {\n    \"lastOffset\": 5,\n    \"custom\": true,\n    \"extra\": {\n      \"a\": 1,\n      \"b\": 3,\n      \"c\": 4\n    }\n  },\n  \"a\": {\n    \"lastOffset\": 2\n  }\n}\n"
        );
    }

    #[tokio::test]
    async fn spreads_array_extra_into_an_object() {
        let (_dir, file_path, store) = setup("state.json");
        tokio::fs::write(&file_path, r#"{"id":{"extra":[10,20]}}"#)
            .await
            .unwrap();
        store.load().await;
        assert_eq!(
            store.get("id").extra(),
            Some(serde_json::json!({"0": 10, "1": 20}))
        );
    }

    #[tokio::test]
    async fn delete_persists_on_the_next_save() {
        let (_dir, file_path, store) = setup("state.json");
        store.load().await;
        store.set_offset("a", 1.0);
        store.set_offset("b", 2.0);
        store.save().await.unwrap();
        assert!(store.delete("a"));
        store.save().await.unwrap();
        assert_eq!(
            read_text(&file_path).await,
            "{\n  \"b\": {\n    \"lastOffset\": 2\n  }\n}\n"
        );
        assert_eq!(store.keys(), vec!["b".to_owned()]);
    }
}
