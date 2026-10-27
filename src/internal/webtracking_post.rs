//! SLS web tracking POST. Network I/O runs only with the `proprietary` feature.

#[cfg(feature = "proprietary")]
use crate::utils::create_logger;
use serde_json::Value;
#[cfg(feature = "proprietary")]
use std::time::Duration;

#[cfg(feature = "proprietary")]
const TIMEOUT: Duration = Duration::from_secs(10);
#[cfg(feature = "proprietary")]
const RETRY_MAX_ATTEMPTS: u32 = 3;
#[cfg(feature = "proprietary")]
const RETRY_BASE_DELAY_MS: u64 = 1000;
#[cfg(feature = "proprietary")]
const RETRYABLE_STATUS: &[u16] = &[408, 429, 500, 502, 503, 504];

pub fn build_web_tracking_url(endpoint: &str, project: &str, logstore: &str) -> String {
    let base = if let Some(rest) = endpoint.strip_prefix("https://") {
        format!("https://{project}.{rest}")
    } else if let Some(rest) = endpoint.strip_prefix("http://") {
        format!("http://{project}.{rest}")
    } else {
        format!("{project}.{endpoint}")
    };
    format!("{base}/logstores/{logstore}/track")
}

pub async fn post_web_tracking(url: &str, body: &Value, label: Option<&str>) {
    #[cfg(feature = "proprietary")]
    {
        post_web_tracking_enabled(url, body, label).await;
    }
    #[cfg(not(feature = "proprietary"))]
    {
        let _ = (url, body, label);
    }
}

#[cfg(feature = "proprietary")]
async fn post_web_tracking_enabled(url: &str, body: &Value, label: Option<&str>) {
    let logger = create_logger("WebTrackingPost");
    let raw = serde_json::to_string(body).unwrap_or_else(|_| "null".to_string());
    let tag = label.unwrap_or("webtracking");
    let mut last_err = String::new();
    for attempt in 0..RETRY_MAX_ATTEMPTS {
        match send_once(url, &raw).await {
            Ok(()) => {
                let mut meta = serde_json::Map::new();
                meta.insert(
                    "url".to_string(),
                    serde_json::Value::String(url.to_string()),
                );
                logger.debug(&format!("{tag} sent"), Some(&meta));
                return;
            }
            Err(err) => {
                let retryable = err.retryable;
                last_err = err.message;
                if !retryable {
                    break;
                }
                if attempt + 1 < RETRY_MAX_ATTEMPTS {
                    let delay = RETRY_BASE_DELAY_MS * 2u64.pow(attempt);
                    let mut meta = serde_json::Map::new();
                    meta.insert("attempt".to_string(), serde_json::json!(attempt + 1));
                    meta.insert("delayMs".to_string(), serde_json::json!(delay));
                    meta.insert(
                        "error".to_string(),
                        serde_json::Value::String(last_err.clone()),
                    );
                    logger.warn(&format!("{tag} retrying"), Some(&meta));
                    tokio::time::sleep(Duration::from_millis(delay)).await;
                }
            }
        }
    }
    let mut meta = serde_json::Map::new();
    meta.insert(
        "url".to_string(),
        serde_json::Value::String(url.to_string()),
    );
    meta.insert("error".to_string(), serde_json::Value::String(last_err));
    logger.error(&format!("{tag} failed after retries"), Some(&meta));
}

#[cfg(feature = "proprietary")]
struct SendError {
    message: String,
    retryable: bool,
}

#[cfg(feature = "proprietary")]
async fn send_once(url: &str, raw: &str) -> Result<(), SendError> {
    let client = reqwest::Client::builder()
        .timeout(TIMEOUT)
        .build()
        .map_err(|err| SendError {
            message: err.to_string(),
            retryable: is_retryable_message(&err.to_string()),
        })?;
    let response = client
        .post(url)
        .header("x-log-apiversion", "0.6.0")
        .header("x-log-bodyrawsize", raw.len().to_string())
        .header("Content-Type", "application/json")
        .body(raw.to_string())
        .send()
        .await
        .map_err(|err| SendError {
            message: err.to_string(),
            retryable: is_retryable_message(&err.to_string()) || err.is_timeout(),
        })?;
    if response.status().is_success() {
        return Ok(());
    }
    let status = response.status().as_u16();
    let text = response.text().await.unwrap_or_default();
    Err(SendError {
        message: format!("{status} {text}"),
        retryable: RETRYABLE_STATUS.contains(&status),
    })
}

#[cfg(feature = "proprietary")]
fn is_retryable_message(message: &str) -> bool {
    message.contains("ECONNRESET")
        || message.contains("ETIMEDOUT")
        || message.contains("ECONNREFUSED")
        || message.contains("TimeoutError")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inserts_the_project_into_the_endpoint_host() {
        assert_eq!(
            build_web_tracking_url(
                "https://cn-shanghai.log.aliyuncs.com",
                "loongsuite-community-edition",
                "loongsuite-online"
            ),
            "https://loongsuite-community-edition.cn-shanghai.log.aliyuncs.com/logstores/loongsuite-online/track"
        );
    }
}
