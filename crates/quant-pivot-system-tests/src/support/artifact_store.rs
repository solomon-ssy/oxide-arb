//! Artifact-store adapters owned by cross-crate system fixtures.

use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use quant_pivot_error::QuantResult;
use quant_pivot_models::types::ArtifactUri;
use quant_pivot_research::artifact::{
    ArtifactByteStream, ArtifactDurability, ArtifactKey, ArtifactObjectMetadata, ArtifactStore,
};

const SYSTEM_TEST_VERSION_ID: &str = "system-test-object-version-v1";

/// In-process versioned Object-Lock boundary for deterministic system tests.
///
/// The wrapped store remains the sole byte owner. This adapter supplies the
/// immutable object-version metadata that production obtains from S3, allowing
/// serving tests to exercise the exact production durability gate without a
/// network service or a second artifact implementation.
pub struct VersionedArtifactStoreFixture {
    inner: Arc<dyn ArtifactStore>,
}

impl VersionedArtifactStoreFixture {
    #[must_use]
    pub const fn new(inner: Arc<dyn ArtifactStore>) -> Self {
        Self { inner }
    }

    const fn durability_contract() -> ArtifactDurability {
        ArtifactDurability {
            remote: true,
            versioned: true,
            object_locked: true,
        }
    }
}

/// Read-fault adapter for proving content verification without mutating the
/// underlying immutable object.
///
/// Only bounded [`ArtifactStore::get`] reads for the exact target URI are
/// replaced. Streaming reads, writes, metadata, and every other URI continue
/// through the original store.
pub struct ReadTamperArtifactStoreFixture {
    inner: Arc<dyn ArtifactStore>,
    target: ArtifactUri,
    replacement: Vec<u8>,
}

/// Targeted byte-read counter for proving shared immutable input reuse.
pub struct ReadCountingArtifactStoreFixture {
    inner: Arc<dyn ArtifactStore>,
    reads: BTreeMap<String, AtomicUsize>,
}

impl ReadCountingArtifactStoreFixture {
    #[must_use]
    pub fn new(inner: Arc<dyn ArtifactStore>, targets: Vec<ArtifactUri>) -> Self {
        Self {
            inner,
            reads: targets
                .into_iter()
                .map(|uri| (uri.as_str().to_owned(), AtomicUsize::new(0)))
                .collect(),
        }
    }

    #[must_use]
    pub fn reads(&self, uri: &ArtifactUri) -> usize {
        self.reads
            .get(uri.as_str())
            .map_or(0, |count| count.load(Ordering::SeqCst))
    }

    pub fn reset(&self) {
        for count in self.reads.values() {
            count.store(0, Ordering::SeqCst);
        }
    }

    fn count(&self, uri: &ArtifactUri) {
        if let Some(count) = self.reads.get(uri.as_str()) {
            count.fetch_add(1, Ordering::SeqCst);
        }
    }
}

impl ReadTamperArtifactStoreFixture {
    #[must_use]
    pub const fn new(
        inner: Arc<dyn ArtifactStore>,
        target: ArtifactUri,
        replacement: Vec<u8>,
    ) -> Self {
        Self {
            inner,
            target,
            replacement,
        }
    }
}

#[async_trait]
impl ArtifactStore for ReadTamperArtifactStoreFixture {
    async fn put_stream(
        &self,
        key: ArtifactKey,
        stream: ArtifactByteStream,
    ) -> QuantResult<ArtifactUri> {
        self.inner.put_stream(key, stream).await
    }

    async fn get_stream(&self, uri: &ArtifactUri) -> QuantResult<ArtifactByteStream> {
        self.inner.get_stream(uri).await
    }

    async fn durability(&self, uri: &ArtifactUri) -> QuantResult<ArtifactDurability> {
        self.inner.durability(uri).await
    }

    async fn metadata(&self, uri: &ArtifactUri) -> QuantResult<ArtifactObjectMetadata> {
        self.inner.metadata(uri).await
    }

    async fn signed_download_url(
        &self,
        uri: &ArtifactUri,
        valid_for: Duration,
    ) -> QuantResult<String> {
        self.inner.signed_download_url(uri, valid_for).await
    }

    async fn get(&self, uri: &ArtifactUri) -> QuantResult<Vec<u8>> {
        if uri == &self.target {
            return Ok(self.replacement.clone());
        }
        self.inner.get(uri).await
    }

    async fn exists(&self, uri: &ArtifactUri) -> QuantResult<bool> {
        self.inner.exists(uri).await
    }

    async fn get_by_key(&self, key: &ArtifactKey) -> QuantResult<Vec<u8>> {
        self.inner.get_by_key(key).await
    }

    async fn exists_by_key(&self, key: &ArtifactKey) -> QuantResult<bool> {
        self.inner.exists_by_key(key).await
    }
}

#[async_trait]
impl ArtifactStore for ReadCountingArtifactStoreFixture {
    async fn put_stream(
        &self,
        key: ArtifactKey,
        stream: ArtifactByteStream,
    ) -> QuantResult<ArtifactUri> {
        self.inner.put_stream(key, stream).await
    }

    async fn get_stream(&self, uri: &ArtifactUri) -> QuantResult<ArtifactByteStream> {
        self.inner.get_stream(uri).await
    }

    async fn durability(&self, uri: &ArtifactUri) -> QuantResult<ArtifactDurability> {
        self.inner.durability(uri).await
    }

    async fn metadata(&self, uri: &ArtifactUri) -> QuantResult<ArtifactObjectMetadata> {
        self.inner.metadata(uri).await
    }

    async fn signed_download_url(
        &self,
        uri: &ArtifactUri,
        valid_for: Duration,
    ) -> QuantResult<String> {
        self.inner.signed_download_url(uri, valid_for).await
    }

    async fn get(&self, uri: &ArtifactUri) -> QuantResult<Vec<u8>> {
        self.count(uri);
        self.inner.get(uri).await
    }

    async fn exists(&self, uri: &ArtifactUri) -> QuantResult<bool> {
        self.inner.exists(uri).await
    }

    async fn get_by_key(&self, key: &ArtifactKey) -> QuantResult<Vec<u8>> {
        self.inner.get_by_key(key).await
    }

    async fn exists_by_key(&self, key: &ArtifactKey) -> QuantResult<bool> {
        self.inner.exists_by_key(key).await
    }
}

#[async_trait]
impl ArtifactStore for VersionedArtifactStoreFixture {
    async fn put_stream(
        &self,
        key: ArtifactKey,
        stream: ArtifactByteStream,
    ) -> QuantResult<ArtifactUri> {
        self.inner.put_stream(key, stream).await
    }

    async fn get_stream(&self, uri: &ArtifactUri) -> QuantResult<ArtifactByteStream> {
        self.inner.get_stream(uri).await
    }

    async fn durability(&self, _uri: &ArtifactUri) -> QuantResult<ArtifactDurability> {
        Ok(Self::durability_contract())
    }

    async fn metadata(&self, uri: &ArtifactUri) -> QuantResult<ArtifactObjectMetadata> {
        let mut metadata = self.inner.metadata(uri).await?;
        metadata.version_id = Some(SYSTEM_TEST_VERSION_ID.to_owned());
        metadata.durability = Self::durability_contract();
        Ok(metadata)
    }

    async fn signed_download_url(
        &self,
        uri: &ArtifactUri,
        valid_for: Duration,
    ) -> QuantResult<String> {
        self.inner.signed_download_url(uri, valid_for).await
    }

    async fn exists(&self, uri: &ArtifactUri) -> QuantResult<bool> {
        self.inner.exists(uri).await
    }

    async fn get_by_key(&self, key: &ArtifactKey) -> QuantResult<Vec<u8>> {
        self.inner.get_by_key(key).await
    }

    async fn exists_by_key(&self, key: &ArtifactKey) -> QuantResult<bool> {
        self.inner.exists_by_key(key).await
    }
}
