use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use aws_config::BehaviorVersion;
use aws_sdk_s3::{
    Client as S3ControlClient,
    config::{Builder, Region},
    presigning::PresigningConfig,
    types::ObjectLockRetentionMode,
};
use chrono::Utc;
use futures_util::StreamExt;
use object_store::{
    Error, GetOptions, ObjectMeta, ObjectStore, ObjectStoreExt, Result, WriteMultipart,
    aws::{AmazonS3, AmazonS3Builder},
    path::Path,
};
use quant_pivot_error::{QuantResult, research::ResearchError};
use quant_pivot_models::{config::ArtifactStoreDeployConfig, types::ArtifactUri};
use tokio::sync::OnceCell;
use url::Url;

use super::{
    ArtifactByteStream, ArtifactDurability, ArtifactKey, ArtifactObjectMetadata, ArtifactStore,
};

/// S3-compatible streaming artifact store using standard AWS credential sources.
pub struct S3ArtifactStore {
    store: Arc<AmazonS3>,
    bucket: String,
    prefix: String,
    require_object_lock: bool,
    require_versioning: bool,
    region: String,
    endpoint: Option<String>,
    path_style: bool,
    control_client: OnceCell<S3ControlClient>,
}

struct S3ObjectRef {
    path: Path,
    version_id: Option<String>,
}

impl S3ArtifactStore {
    pub fn new(config: &ArtifactStoreDeployConfig) -> QuantResult<Self> {
        if config.bucket.trim().is_empty() {
            return Err(ResearchError::ArtifactIo {
                uri: "s3://".to_owned(),
                detail: "S3 artifact store requires a non-empty bucket".to_owned(),
            }
            .into());
        }
        if !config.require_object_lock || !config.require_versioning {
            return Err(ResearchError::ArtifactIo {
                uri: format!("s3://{}", config.bucket),
                detail: "production S3 artifact store must require Object Lock and versioning"
                    .to_owned(),
            }
            .into());
        }
        let mut builder = AmazonS3Builder::from_env()
            .with_bucket_name(&config.bucket)
            .with_region(&config.region)
            .with_virtual_hosted_style_request(!config.path_style);
        if let Some(endpoint) = &config.endpoint {
            builder = builder
                .with_endpoint(endpoint)
                .with_allow_http(endpoint.starts_with("http://"));
        }
        let store = builder.build().map_err(|error| ResearchError::ArtifactIo {
            uri: format!("s3://{}", config.bucket),
            detail: format!("failed to configure S3 artifact store: {error}"),
        })?;
        Ok(Self {
            store: Arc::new(store),
            bucket: config.bucket.clone(),
            prefix: config.prefix.trim_matches('/').to_owned(),
            require_object_lock: config.require_object_lock,
            require_versioning: config.require_versioning,
            region: config.region.clone(),
            endpoint: config.endpoint.clone(),
            path_style: config.path_style,
            control_client: OnceCell::const_new(),
        })
    }

    fn object_path(&self, key: &ArtifactKey) -> Path {
        let relative = key.relative_path();
        if self.prefix.is_empty() {
            Path::from(relative)
        } else {
            Path::from(format!("{}/{relative}", self.prefix))
        }
    }

    fn object_ref_from_uri(&self, uri: &ArtifactUri) -> QuantResult<S3ObjectRef> {
        let parsed = Url::parse(uri.as_str()).map_err(|error| ResearchError::ArtifactIo {
            uri: uri.as_str().to_owned(),
            detail: format!("invalid S3 artifact URI: {error}"),
        })?;
        if parsed.scheme() != "s3"
            || parsed.host_str() != Some(self.bucket.as_str())
            || parsed.port().is_some()
            || !parsed.username().is_empty()
            || parsed.password().is_some()
            || parsed.fragment().is_some()
        {
            return Err(ResearchError::ArtifactIo {
                uri: uri.as_str().to_owned(),
                detail: format!("URI is outside configured bucket `{}`", self.bucket),
            }
            .into());
        }
        let raw = parsed.path().trim_start_matches('/');
        let in_prefix = self.prefix.is_empty()
            || raw
                .strip_prefix(&self.prefix)
                .is_some_and(|suffix| suffix.starts_with('/'));
        if raw.is_empty()
            || raw.contains('%')
            || raw
                .split('/')
                .any(|segment| segment.is_empty() || segment == "..")
            || !in_prefix
        {
            return Err(ResearchError::ArtifactIo {
                uri: uri.as_str().to_owned(),
                detail: "invalid object path".to_owned(),
            }
            .into());
        }
        let query = parsed.query_pairs().collect::<Vec<_>>();
        let version_id = match query.as_slice() {
            [] => None,
            [(key, value)] if key == "versionId" && !value.is_empty() => {
                Some(value.as_ref().to_owned())
            }
            _ => {
                return Err(ResearchError::ArtifactIo {
                    uri: uri.as_str().to_owned(),
                    detail: "S3 artifact URI has an invalid version query".to_owned(),
                }
                .into());
            }
        };
        Ok(S3ObjectRef {
            path: Path::from(raw),
            version_id,
        })
    }

    fn uri_for_path(&self, path: &Path, version_id: Option<&str>) -> QuantResult<ArtifactUri> {
        let mut uri = Url::parse(&format!("s3://{}/{}", self.bucket, path)).map_err(|error| {
            ResearchError::ArtifactIo {
                uri: format!("s3://{}/{}", self.bucket, path),
                detail: format!("failed to build S3 artifact URI: {error}"),
            }
        })?;
        if let Some(version_id) = version_id {
            uri.query_pairs_mut().append_pair("versionId", version_id);
        }
        ArtifactUri::parse(uri.to_string()).map_err(Into::into)
    }

    fn io_error(&self, path: &Path, error: &Error) -> ResearchError {
        ResearchError::ArtifactIo {
            uri: format!("s3://{}/{}", self.bucket, path),
            detail: error.to_string(),
        }
    }

    async fn control_client(&self) -> &S3ControlClient {
        self.control_client
            .get_or_init(|| async {
                let shared = aws_config::defaults(BehaviorVersion::latest())
                    .region(Region::new(self.region.clone()))
                    .load()
                    .await;
                let mut builder = Builder::from(&shared).force_path_style(self.path_style);
                if let Some(endpoint) = &self.endpoint {
                    builder = builder.endpoint_url(endpoint);
                }
                S3ControlClient::from_conf(builder.build())
            })
            .await
    }

    async fn object_lock_is_active(&self, path: &Path, version_id: &str) -> QuantResult<bool> {
        if !self.require_object_lock {
            return Ok(false);
        }
        let output = self
            .control_client()
            .await
            .get_object_retention()
            .bucket(&self.bucket)
            .key(path.to_string())
            .version_id(version_id)
            .send()
            .await
            .map_err(|error| ResearchError::ArtifactIo {
                uri: format!("s3://{}/{}?versionId={version_id}", self.bucket, path),
                detail: format!("failed to prove Object Lock retention: {error}"),
            })?;
        let Some(retention) = output.retention() else {
            return Ok(false);
        };
        let recognized_mode = matches!(
            retention.mode(),
            Some(ObjectLockRetentionMode::Compliance | ObjectLockRetentionMode::Governance)
        );
        let active_until = retention
            .retain_until_date()
            .is_some_and(|date| date.secs() > Utc::now().timestamp());
        Ok(recognized_mode && active_until)
    }

    async fn head_object(&self, object: &S3ObjectRef) -> Result<ObjectMeta> {
        let options = GetOptions {
            version: object.version_id.clone(),
            head: true,
            ..GetOptions::default()
        };
        self.store
            .get_opts(&object.path, options)
            .await
            .map(|result| result.meta)
    }
}

#[async_trait]
impl ArtifactStore for S3ArtifactStore {
    async fn put_stream(
        &self,
        key: ArtifactKey,
        mut stream: ArtifactByteStream,
    ) -> QuantResult<ArtifactUri> {
        let path = self.object_path(&key);
        let upload = self
            .store
            .put_multipart(&path)
            .await
            .map_err(|error| self.io_error(&path, &error))?;
        let mut writer = WriteMultipart::new(upload);
        while let Some(chunk) = stream.next().await {
            writer
                .wait_for_capacity(4)
                .await
                .map_err(|error| self.io_error(&path, &error))?;
            writer.put(chunk?);
        }
        let completed = writer
            .finish()
            .await
            .map_err(|error| self.io_error(&path, &error))?;
        if self.require_versioning && completed.version.is_none() {
            return Err(ResearchError::ArtifactIo {
                uri: format!("s3://{}/{}", self.bucket, path),
                detail: "object write returned no version id; bucket versioning is not proven"
                    .to_owned(),
            }
            .into());
        }
        let Some(version_id) = completed.version.as_deref() else {
            return Err(ResearchError::ArtifactIo {
                uri: format!("s3://{}/{}", self.bucket, path),
                detail: "object write has no immutable version identity".to_owned(),
            }
            .into());
        };
        if !self.object_lock_is_active(&path, version_id).await? {
            return Err(ResearchError::ArtifactIo {
                uri: format!("s3://{}/{}?versionId={version_id}", self.bucket, path),
                detail: "object version has no active Object Lock retention".to_owned(),
            }
            .into());
        }
        self.uri_for_path(&path, Some(version_id))
    }

    async fn get_stream(&self, uri: &ArtifactUri) -> QuantResult<ArtifactByteStream> {
        let object = self.object_ref_from_uri(uri)?;
        let options = GetOptions {
            version: object.version_id,
            ..GetOptions::default()
        };
        let result =
            self.store
                .get_opts(&object.path, options)
                .await
                .map_err(|error| match error {
                    Error::NotFound { .. } => ResearchError::ArtifactNotFound {
                        uri: uri.as_str().to_owned(),
                    },
                    error => self.io_error(&object.path, &error),
                })?;
        let display = uri.as_str().to_owned();
        let stream = result.into_stream().map(move |result| {
            result.map_err(|error| {
                ResearchError::ArtifactIo {
                    uri: display.clone(),
                    detail: error.to_string(),
                }
                .into()
            })
        });
        Ok(Box::pin(stream))
    }

    async fn durability(&self, uri: &ArtifactUri) -> QuantResult<ArtifactDurability> {
        Ok(self.metadata(uri).await?.durability)
    }

    async fn metadata(&self, uri: &ArtifactUri) -> QuantResult<ArtifactObjectMetadata> {
        let object = self.object_ref_from_uri(uri)?;
        let meta = self
            .head_object(&object)
            .await
            .map_err(|error| self.io_error(&object.path, &error))?;
        let object_locked = match meta.version.as_deref() {
            Some(version_id) => self.object_lock_is_active(&object.path, version_id).await?,
            None => false,
        };
        Ok(ArtifactObjectMetadata {
            byte_size: meta.size,
            etag: meta.e_tag,
            version_id: meta.version.clone(),
            durability: ArtifactDurability {
                remote: true,
                versioned: meta.version.is_some(),
                object_locked,
            },
        })
    }

    async fn signed_download_url(
        &self,
        uri: &ArtifactUri,
        valid_for: Duration,
    ) -> QuantResult<String> {
        let object = self.object_ref_from_uri(uri)?;
        let signing =
            PresigningConfig::expires_in(valid_for).map_err(|error| ResearchError::ArtifactIo {
                uri: uri.as_str().to_owned(),
                detail: format!("invalid signed-download lifetime: {error}"),
            })?;
        self.control_client()
            .await
            .get_object()
            .bucket(&self.bucket)
            .key(object.path.to_string())
            .set_version_id(object.version_id)
            .presigned(signing)
            .await
            .map(|request| request.uri().to_owned())
            .map_err(|error| {
                ResearchError::ArtifactIo {
                    uri: uri.as_str().to_owned(),
                    detail: format!("failed to sign evidence download: {error}"),
                }
                .into()
            })
    }

    async fn exists(&self, uri: &ArtifactUri) -> QuantResult<bool> {
        let object = self.object_ref_from_uri(uri)?;
        match self.head_object(&object).await {
            Ok(_) => Ok(true),
            Err(Error::NotFound { .. }) => Ok(false),
            Err(error) => Err(self.io_error(&object.path, &error).into()),
        }
    }

    async fn get_by_key(&self, key: &ArtifactKey) -> QuantResult<Vec<u8>> {
        self.get(&self.uri_for_path(&self.object_path(key), None)?)
            .await
    }

    async fn exists_by_key(&self, key: &ArtifactKey) -> QuantResult<bool> {
        self.exists(&self.uri_for_path(&self.object_path(key), None)?)
            .await
    }
}
