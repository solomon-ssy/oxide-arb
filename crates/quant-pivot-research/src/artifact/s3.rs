use std::{io::Error as IoError, mem, sync::Arc, time::Duration};

use async_trait::async_trait;
use aws_config::BehaviorVersion;
use aws_sdk_s3::{
    Client as S3ControlClient,
    config::{Builder, Credentials, Region},
    presigning::PresigningConfig,
    types::ObjectLockRetentionMode,
};
use bytes::Bytes;
use chrono::Utc;
use futures_util::StreamExt;
use object_store::{
    Error, GetOptions, MultipartUpload, ObjectMeta, ObjectStore, ObjectStoreExt, PutPayload,
    PutPayloadMut, PutResult, Result,
    aws::{AmazonS3, AmazonS3Builder},
    path::Path,
};
use quant_pivot_error::{QuantError, QuantResult, research::ResearchError};
use quant_pivot_models::{config::ArtifactStoreDeployConfig, types::ArtifactUri};
use tokio::{sync::OnceCell, task::JoinSet};
use tracing::error;
use url::Url;

use super::{
    ArtifactByteStream, ArtifactDurability, ArtifactKey, ArtifactObjectMetadata, ArtifactStore,
    S3StaticCredentials,
};

// WORM artifacts are control-plane evidence, not a latency hot path. Keep one
// fixed-size multipart PUT in flight and explicitly abort failed uploads. The
// bound applies to S3 parts, independently of upstream stream chunk sizes.
const MULTIPART_PART_SIZE: usize = 5 * 1024 * 1024;
const MULTIPART_WRITE_CONCURRENCY: usize = 1;

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
    credentials: Option<S3StaticCredentials>,
    control_client: OnceCell<S3ControlClient>,
}

struct S3ObjectRef {
    path: Path,
    version_id: Option<String>,
}

struct BoundedMultipartWriter {
    upload: Box<dyn MultipartUpload>,
    buffer: PutPayloadMut,
    in_flight: JoinSet<Result<()>>,
}

impl BoundedMultipartWriter {
    fn new(upload: Box<dyn MultipartUpload>) -> Self {
        Self {
            upload,
            buffer: PutPayloadMut::new(),
            in_flight: JoinSet::new(),
        }
    }

    async fn write(&mut self, mut bytes: Bytes) -> Result<()> {
        while !bytes.is_empty() {
            let remaining = MULTIPART_PART_SIZE - self.buffer.content_length();
            let take = remaining.min(bytes.len());
            self.buffer.push(bytes.split_to(take));
            if self.buffer.content_length() == MULTIPART_PART_SIZE {
                let payload = mem::take(&mut self.buffer).into();
                self.enqueue(payload).await?;
            }
        }
        Ok(())
    }

    async fn enqueue(&mut self, payload: PutPayload) -> Result<()> {
        while self.in_flight.len() >= MULTIPART_WRITE_CONCURRENCY {
            self.await_one().await?;
        }
        self.in_flight.spawn(self.upload.put_part(payload));
        Ok(())
    }

    async fn await_one(&mut self) -> Result<()> {
        let joined = self
            .in_flight
            .join_next()
            .await
            .ok_or_else(|| Error::NotSupported {
                source: Box::new(IoError::other(
                    "multipart task set became empty while awaiting capacity",
                )),
            })?;
        joined.map_err(|source| Error::JoinError { source })?
    }

    async fn finish(&mut self) -> Result<PutResult> {
        if !self.buffer.is_empty() {
            let payload = mem::take(&mut self.buffer).into();
            self.enqueue(payload).await?;
        }
        while !self.in_flight.is_empty() {
            self.await_one().await?;
        }
        self.upload.complete().await
    }

    async fn abort(&mut self) -> Result<()> {
        self.in_flight.shutdown().await;
        self.upload.abort().await
    }
}

impl S3ArtifactStore {
    pub fn new(config: &ArtifactStoreDeployConfig) -> QuantResult<Self> {
        Self::build(config, None)
    }

    /// Build an S3 store with caller-owned static credentials.
    ///
    /// This constructor does not read or mutate process-global AWS credential
    /// state. Both the object data client and the Object Lock control client
    /// receive the exact same credential identity.
    pub fn new_with_credentials(
        config: &ArtifactStoreDeployConfig,
        credentials: S3StaticCredentials,
    ) -> QuantResult<Self> {
        Self::build(config, Some(credentials))
    }

    fn build(
        config: &ArtifactStoreDeployConfig,
        credentials: Option<S3StaticCredentials>,
    ) -> QuantResult<Self> {
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
        let mut builder = credentials
            .as_ref()
            .map_or_else(AmazonS3Builder::from_env, |credentials| {
                AmazonS3Builder::new()
                    .with_access_key_id(credentials.access_key_id.expose_secret())
                    .with_secret_access_key(credentials.secret_access_key.expose_secret())
            })
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
            credentials,
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
            uri: uri.to_string(),
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
                uri: uri.to_string(),
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
                uri: uri.to_string(),
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
                    uri: uri.to_string(),
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

    fn store_error(&self, path: &Path, error: &Error) -> ResearchError {
        let uri = format!("s3://{}/{}", self.bucket, path);
        match error {
            // `object_store` maps exhausted HTTP transport/server retries to
            // `Generic`; authentication, authorization, path, precondition,
            // and capability failures have dedicated non-retryable variants.
            Error::Generic { .. } => ResearchError::ArtifactTransport {
                uri,
                detail: error.to_string(),
            },
            _ => ResearchError::ArtifactIo {
                uri,
                detail: error.to_string(),
            },
        }
    }

    fn with_abort_detail(&self, path: &Path, error: QuantError, abort_error: &Error) -> QuantError {
        match error {
            QuantError::Research(ResearchError::ArtifactTransport { uri, detail }) => {
                ResearchError::ArtifactTransport {
                    uri,
                    detail: format!(
                        "{detail}; aborting the incomplete multipart upload also failed: {abort_error}"
                    ),
                }
                .into()
            }
            QuantError::Research(ResearchError::ArtifactIo { uri, detail }) => {
                ResearchError::ArtifactIo {
                    uri,
                    detail: format!(
                        "{detail}; aborting the incomplete multipart upload also failed: {abort_error}"
                    ),
                }
                .into()
            }
            error => {
                error!(
                    uri = %format_args!("s3://{}/{}", self.bucket, path),
                    primary_code = error.code(),
                    abort_error = %abort_error,
                    "incomplete artifact multipart abort failed after a non-storage error"
                );
                error
            }
        }
    }

    async fn control_client(&self) -> &S3ControlClient {
        self.control_client
            .get_or_init(|| async {
                let mut loader = aws_config::defaults(BehaviorVersion::latest())
                    .region(Region::new(self.region.clone()));
                if let Some(credentials) = &self.credentials {
                    loader = loader.credentials_provider(Credentials::new(
                        credentials.access_key_id.expose_secret(),
                        credentials.secret_access_key.expose_secret(),
                        None,
                        None,
                        "quant-pivot-explicit-s3-credentials",
                    ));
                }
                let shared = loader.load().await;
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

    async fn write_multipart(
        &self,
        path: &Path,
        upload: Box<dyn MultipartUpload>,
        mut stream: ArtifactByteStream,
    ) -> QuantResult<PutResult> {
        let mut writer = BoundedMultipartWriter::new(upload);
        let result: QuantResult<PutResult> = async {
            while let Some(chunk) = stream.next().await {
                writer
                    .write(chunk?)
                    .await
                    .map_err(|error| self.store_error(path, &error))?;
            }
            let completed = writer
                .finish()
                .await
                .map_err(|error| self.store_error(path, &error))?;
            Ok(completed)
        }
        .await;
        match result {
            Ok(completed) => Ok(completed),
            Err(error) => match writer.abort().await {
                Ok(()) => Err(error),
                Err(abort_error) => Err(self.with_abort_detail(path, error, &abort_error)),
            },
        }
    }
}

#[async_trait]
impl ArtifactStore for S3ArtifactStore {
    async fn put_stream(
        &self,
        key: ArtifactKey,
        stream: ArtifactByteStream,
    ) -> QuantResult<ArtifactUri> {
        let path = self.object_path(&key);
        let upload = self
            .store
            .put_multipart(&path)
            .await
            .map_err(|error| self.store_error(&path, &error))?;
        let completed = self.write_multipart(&path, upload, stream).await?;
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
                        uri: uri.to_string(),
                    },
                    error => self.store_error(&object.path, &error),
                })?;
        let display = uri.to_string();
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
            .map_err(|error| self.store_error(&object.path, &error))?;
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
                uri: uri.to_string(),
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
                    uri: uri.to_string(),
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
            Err(error) => Err(self.store_error(&object.path, &error).into()),
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

#[cfg(test)]
mod tests {
    use std::{
        io::Error as IoError,
        sync::{
            Arc, Mutex,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        },
    };

    use async_trait::async_trait;
    use bytes::Bytes;
    use futures_util::stream;
    use object_store::{
        Error, Extensions, MultipartUpload, PutPayload, PutResult, Result as ObjectStoreResult,
        UploadPart, path::Path,
    };
    use quant_pivot_error::{QuantError, QuantResult, research::ResearchError};
    use quant_pivot_models::config::{ArtifactStoreDeployConfig, ArtifactStoreKind};

    use super::{ArtifactByteStream, MULTIPART_PART_SIZE, S3ArtifactStore, S3StaticCredentials};

    #[derive(Debug, Default)]
    struct UploadProbe {
        aborted: AtomicBool,
        active: AtomicUsize,
        completed: AtomicBool,
        max_active: AtomicUsize,
        parts: Mutex<Vec<(usize, Bytes)>>,
    }

    #[derive(Debug)]
    struct ProbedUpload {
        abort_fails: bool,
        fail_part: Option<usize>,
        next_part: usize,
        probe: Arc<UploadProbe>,
    }

    impl ProbedUpload {
        fn new(probe: Arc<UploadProbe>, fail_part: Option<usize>, abort_fails: bool) -> Self {
            Self {
                abort_fails,
                fail_part,
                next_part: 0,
                probe,
            }
        }
    }

    #[async_trait]
    impl MultipartUpload for ProbedUpload {
        fn put_part(&mut self, data: PutPayload) -> UploadPart {
            let part = self.next_part;
            self.next_part += 1;
            let fails = self.fail_part == Some(part);
            let probe = Arc::clone(&self.probe);
            Box::pin(async move {
                let active = probe.active.fetch_add(1, Ordering::AcqRel) + 1;
                probe.max_active.fetch_max(active, Ordering::AcqRel);
                tokio::task::yield_now().await;
                probe.active.fetch_sub(1, Ordering::AcqRel);
                if fails {
                    return Err(Error::Generic {
                        store: "probe",
                        source: Box::new(IoError::other("injected multipart failure")),
                    });
                }
                probe
                    .parts
                    .lock()
                    .expect("upload probe lock")
                    .push((part, Bytes::from(data)));
                Ok(())
            })
        }

        async fn complete(&mut self) -> ObjectStoreResult<PutResult> {
            self.probe.completed.store(true, Ordering::Release);
            Ok(PutResult {
                e_tag: Some("probe-etag".to_owned()),
                version: Some("probe-version".to_owned()),
                extensions: Extensions::default(),
            })
        }

        async fn abort(&mut self) -> ObjectStoreResult<()> {
            self.probe.aborted.store(true, Ordering::Release);
            if self.abort_fails {
                Err(Error::Generic {
                    store: "probe",
                    source: Box::new(IoError::other("injected multipart abort failure")),
                })
            } else {
                Ok(())
            }
        }
    }

    impl S3ArtifactStore {
        fn probe() -> QuantResult<Self> {
            let config = ArtifactStoreDeployConfig {
                kind: ArtifactStoreKind::S3,
                bucket: "multipart-probe".to_owned(),
                prefix: "artifacts".to_owned(),
                region: "us-east-1".to_owned(),
                endpoint: Some("http://127.0.0.1:1".to_owned()),
                path_style: true,
                require_object_lock: true,
                require_versioning: true,
            };
            Self::new_with_credentials(
                &config,
                S3StaticCredentials::new("probe-access", "probe-secret")?,
            )
        }
    }

    #[tokio::test]
    async fn large_chunk_stays_bounded() -> QuantResult<()> {
        let store = S3ArtifactStore::probe()?;
        let probe = Arc::new(UploadProbe::default());
        let payload = Bytes::from(vec![0x5a; MULTIPART_PART_SIZE * 2 + 17]);
        let stream: ArtifactByteStream = Box::pin(stream::iter([Ok(payload.clone())]));
        let path = Path::from("artifacts/large.json");

        store
            .write_multipart(
                &path,
                Box::new(ProbedUpload::new(Arc::clone(&probe), None, false)),
                stream,
            )
            .await?;

        let mut parts = probe.parts.lock().expect("upload probe lock").clone();
        parts.sort_by_key(|(part, _)| *part);
        assert_eq!(probe.max_active.load(Ordering::Acquire), 1);
        assert!(probe.completed.load(Ordering::Acquire));
        assert!(!probe.aborted.load(Ordering::Acquire));
        assert_eq!(
            parts
                .iter()
                .map(|(_, bytes)| bytes.len())
                .collect::<Vec<_>>(),
            [MULTIPART_PART_SIZE, MULTIPART_PART_SIZE, 17]
        );
        let rebuilt = parts
            .into_iter()
            .flat_map(|(_, bytes)| bytes)
            .collect::<Vec<_>>();
        assert_eq!(rebuilt, payload);
        Ok(())
    }

    #[tokio::test]
    async fn failed_part_is_aborted() -> QuantResult<()> {
        let store = S3ArtifactStore::probe()?;
        let probe = Arc::new(UploadProbe::default());
        let payload = Bytes::from(vec![0x7b; MULTIPART_PART_SIZE * 2 + 17]);
        let stream: ArtifactByteStream = Box::pin(stream::iter([Ok(payload)]));
        let path = Path::from("artifacts/failure.json");

        let result = store
            .write_multipart(
                &path,
                Box::new(ProbedUpload::new(Arc::clone(&probe), Some(1), false)),
                stream,
            )
            .await;

        assert!(result.is_err());
        assert_eq!(probe.max_active.load(Ordering::Acquire), 1);
        assert!(probe.aborted.load(Ordering::Acquire));
        assert!(!probe.completed.load(Ordering::Acquire));
        Ok(())
    }

    #[tokio::test]
    async fn failed_abort_keeps_transport() -> QuantResult<()> {
        let store = S3ArtifactStore::probe()?;
        let probe = Arc::new(UploadProbe::default());
        let payload = Bytes::from(vec![0x4c; MULTIPART_PART_SIZE * 2 + 17]);
        let stream: ArtifactByteStream = Box::pin(stream::iter([Ok(payload)]));
        let path = Path::from("artifacts/abort-failure.json");

        let error = store
            .write_multipart(
                &path,
                Box::new(ProbedUpload::new(Arc::clone(&probe), Some(1), true)),
                stream,
            )
            .await
            .expect_err("injected multipart transport failure");

        let QuantError::Research(ResearchError::ArtifactTransport { detail, .. }) = &error else {
            panic!("multipart transport classification changed: {error}")
        };
        assert!(detail.contains("injected multipart failure"));
        assert!(detail.contains("injected multipart abort failure"));
        assert!(probe.aborted.load(Ordering::Acquire));
        assert!(!probe.completed.load(Ordering::Acquire));
        Ok(())
    }
}
