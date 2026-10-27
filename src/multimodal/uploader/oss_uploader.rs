//! OSS uploader. Fail-open: a put error returns false and does not raise.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::types::{MultimodalAkAuth, MultimodalOssTarget};
use crate::utils::create_logger;

use super::super::http::log_meta;
use super::super::types::{UploadItem, UploadOptions, Uploader};
use super::lru_set::{LruMap, MULTIMODAL_LRU_LIMIT};
use super::oss_client::{
    oss_put_object, parse_oss_storage_base_path, OssPutObjectParams, OssPutObjectResult,
};
use super::retry::{
    with_retries, AttemptError, RetryOutcome, DEFAULT_MULTIMODAL_RETRY,
    MULTIMODAL_UPLOAD_TIMEOUT_MS,
};

const UPLOAD_FAILURE_WARN_INTERVAL_MS: i64 = 60_000;

fn logger() -> crate::utils::BoundLogger {
    create_logger("OssUploader")
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

type PutFn =
    Arc<dyn Fn(OssPutObjectParams) -> super::super::http::BoxFut<OssPutObjectResult> + Send + Sync>;

pub struct OssUploader {
    endpoint: String,
    auth: MultimodalAkAuth,
    bucket: String,
    prefix: String,
    success_keys: Mutex<LruMap<bool>>,
    failure_warn_at: Mutex<LruMap<i64>>,
    abort: super::super::types::AbortFlag,
    closed: AtomicBool,
    put_override: Mutex<Option<PutFn>>,
}

impl OssUploader {
    pub fn new(target: MultimodalOssTarget, auth: MultimodalAkAuth) -> Result<Self, String> {
        let (bucket, prefix) = parse_oss_storage_base_path(&target.storage_base_path)?;
        Ok(Self {
            endpoint: target.endpoint,
            auth,
            bucket,
            prefix,
            success_keys: Mutex::new(LruMap::new(MULTIMODAL_LRU_LIMIT).expect("lru")),
            failure_warn_at: Mutex::new(LruMap::new(MULTIMODAL_LRU_LIMIT).expect("lru")),
            abort: super::super::types::new_abort_flag(),
            closed: AtomicBool::new(false),
            put_override: Mutex::new(None),
        })
    }

    #[cfg(test)]
    pub fn set_put_override(&self, put: PutFn) {
        *self.put_override.lock().unwrap() = Some(put);
    }
}

#[async_trait::async_trait]
impl Uploader for OssUploader {
    async fn upload(&self, item: UploadItem, opts: UploadOptions) -> bool {
        if self.closed.load(Ordering::SeqCst) {
            logger().warn("oss upload skipped: uploader closed", None);
            return false;
        }
        let Some(data) = item.data.clone() else {
            logger().warn(
                "oss upload skipped: invalid payload size",
                Some(&log_meta(&[
                    ("expectedSize", serde_json::json!(item.expected_size)),
                    ("actualSize", serde_json::Value::Null),
                ])),
            );
            return false;
        };
        if data.len() != item.expected_size {
            logger().warn(
                "oss upload skipped: invalid payload size",
                Some(&log_meta(&[
                    ("expectedSize", serde_json::json!(item.expected_size)),
                    ("actualSize", serde_json::json!(data.len())),
                ])),
            );
            return false;
        }
        let object_key = if self.prefix.is_empty() {
            item.target_path.clone()
        } else {
            format!("{}/{}", self.prefix, item.target_path)
        };
        let skip = opts.skip_if_exists.unwrap_or(true);
        if skip && self.success_keys.lock().unwrap().has(&object_key) {
            logger().debug("oss upload skipped (idempotent)", None);
            return true;
        }
        let outcome = with_retries(
            DEFAULT_MULTIMODAL_RETRY,
            |_| {
                let closed =
                    self.closed.load(Ordering::SeqCst) || self.abort.load(Ordering::SeqCst);
                let params = OssPutObjectParams {
                    endpoint: self.endpoint.clone(),
                    bucket: self.bucket.clone(),
                    object_key: object_key.clone(),
                    access_key_id: self.auth.access_key_id.clone(),
                    access_key_secret: self.auth.access_key_secret.clone(),
                    security_token: self.auth.security_token.clone(),
                    body: data.clone(),
                    content_type: item.content_type.clone(),
                    timeout_ms: MULTIMODAL_UPLOAD_TIMEOUT_MS,
                    meta: item
                        .meta
                        .iter()
                        .map(|(key, value)| (key.clone(), value.clone()))
                        .collect(),
                    signal: Some(Arc::clone(&self.abort)),
                };
                let override_put = self.put_override.lock().unwrap().clone();
                async move {
                    if closed {
                        return Err(AttemptError {
                            retryable: false,
                            error: Some("uploader closed".into()),
                            status_code: None,
                            request_id: None,
                        });
                    }
                    let put = if let Some(put) = override_put {
                        put(params).await
                    } else {
                        oss_put_object(params).await
                    };
                    if put.ok {
                        Ok(())
                    } else {
                        Err(AttemptError {
                            retryable: put.retryable.unwrap_or(true),
                            error: put.error,
                            status_code: put.status_code,
                            request_id: put.request_id,
                        })
                    }
                }
            },
            Some(Arc::clone(&self.abort)),
        )
        .await;
        match outcome {
            RetryOutcome::Err {
                error,
                status_code,
                request_id,
            } => {
                self.warn_upload_failure(
                    status_code,
                    error.as_deref().unwrap_or(""),
                    item.expected_size,
                    request_id.as_deref(),
                );
                false
            }
            RetryOutcome::Ok(()) => {
                if self.closed.load(Ordering::SeqCst) {
                    logger().debug("oss upload completed after close", None);
                    return true;
                }
                self.success_keys.lock().unwrap().set(object_key, true);
                true
            }
        }
    }

    async fn shutdown(&self) {
        if self.closed.swap(true, Ordering::SeqCst) {
            logger().debug("oss uploader shutdown skipped (already closed)", None);
            return;
        }
        self.abort.store(true, Ordering::SeqCst);
        self.success_keys.lock().unwrap().clear();
    }
}

impl OssUploader {
    fn warn_upload_failure(
        &self,
        status_code: Option<u16>,
        reason: &str,
        size: usize,
        request_id: Option<&str>,
    ) {
        let key = format!(
            "{}\0{reason}",
            status_code
                .map(|code| code.to_string())
                .unwrap_or_else(|| "none".into())
        );
        let now = now_ms();
        let mut map = self.failure_warn_at.lock().unwrap();
        let previous = map.get_copied(&key).unwrap_or(0);
        if now - previous < UPLOAD_FAILURE_WARN_INTERVAL_MS {
            return;
        }
        map.set(key, now);
        drop(map);
        logger().warn(
            "oss upload failed",
            Some(&log_meta(&[
                ("statusCode", serde_json::json!(status_code)),
                ("error", serde_json::json!(reason)),
                ("requestId", serde_json::json!(request_id)),
                ("size", serde_json::json!(size)),
            ])),
        );
    }
}
