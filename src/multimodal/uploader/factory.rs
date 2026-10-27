//! Build the uploader for the configured storage.

use std::sync::Arc;

use crate::types::{
    MultimodalCredentialAuth, MultimodalRuntimeConfig, MultimodalSlsTarget, MultimodalStorage,
};

use super::super::types::{StorageBaseResult, Uploader};
use super::oss_uploader::OssUploader;
use super::sls_uploader::{BackingAuthInput, SlsUploader};

pub fn create_uploader(
    config: &MultimodalRuntimeConfig,
    expected_presign_origin: Option<String>,
) -> Result<Arc<dyn Uploader>, String> {
    match &config.storage {
        MultimodalStorage::Oss { target, auth } => {
            let uploader = OssUploader::new(target.clone(), auth.clone())?;
            Ok(Arc::new(uploader))
        }
        MultimodalStorage::Sls { target, auth } => {
            let uploader = SlsUploader::new(
                target.clone(),
                BackingAuthInput::Sls(auth.clone()),
                &config.storage_base_path,
                None,
            )?;
            Ok(Arc::new(uploader))
        }
        MultimodalStorage::DelegatedOss { target, auth } => {
            let uploader = SlsUploader::new(
                target.clone(),
                BackingAuthInput::Delegated(auth.clone()),
                &config.storage_base_path,
                expected_presign_origin,
            )?;
            Ok(Arc::new(uploader))
        }
    }
}

pub struct DelegatedOssRuntime {
    pub uploader: Arc<SlsUploader>,
}

impl DelegatedOssRuntime {
    pub async fn resolve_storage_base(&self) -> StorageBaseResult {
        self.uploader.discover_delegated_oss_bucket().await
    }
}

pub fn create_delegated_oss_uploader(
    target: MultimodalSlsTarget,
    auth: MultimodalCredentialAuth,
    storage_base_path: &str,
) -> Result<DelegatedOssRuntime, String> {
    let uploader = SlsUploader::new(
        target,
        BackingAuthInput::Delegated(auth),
        storage_base_path,
        None,
    )?;
    Ok(DelegatedOssRuntime {
        uploader: Arc::new(uploader),
    })
}
