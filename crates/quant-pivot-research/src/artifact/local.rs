//! Local filesystem [`ArtifactStore`] backend (`file://` URIs).

use std::{
    io::{Error, ErrorKind},
    path::{self, Path, PathBuf},
    process,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use futures_util::StreamExt;
use quant_pivot_error::{QuantResult, research::ResearchError};
use quant_pivot_models::types::ArtifactUri;
use tokio::{fs, io::AsyncWriteExt};
use tokio_util::io::ReaderStream;

use super::{
    ArtifactByteStream, ArtifactDurability, ArtifactKey, ArtifactObjectMetadata, ArtifactStore,
};

/// URI scheme served by this backend.
const FILE_SCHEME: &str = "file://";

/// Artifact store backed by a local directory tree rooted at `root`.
///
/// Bytes are written atomically (temp file + rename) so a crashed write never
/// leaves a torn artifact at its final path. Reads validate that the resolved
/// path stays within `root`, so a crafted URI cannot read outside the store.
pub struct LocalArtifactStore {
    root: PathBuf,
}

impl LocalArtifactStore {
    /// Build a store rooted at the Local artifact-store `prefix`.
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    /// Absolute path for a key's final location.
    fn absolute_path(&self, key: &ArtifactKey) -> QuantResult<PathBuf> {
        let relative = self.root.join(key.relative_path());
        absolutize(&relative)
    }

    /// Resolve a `file://` URI to a local path, rejecting anything outside the
    /// store root.
    fn path_from_uri(&self, uri: &ArtifactUri) -> QuantResult<PathBuf> {
        let raw =
            uri.as_str()
                .strip_prefix(FILE_SCHEME)
                .ok_or_else(|| ResearchError::ArtifactIo {
                    uri: uri.as_str().to_owned(),
                    detail: format!("unsupported scheme (expected `{FILE_SCHEME}`)"),
                })?;
        let candidate = absolutize(Path::new(raw))?;
        let root = absolutize(&self.root)?;
        if !candidate.starts_with(&root) {
            return Err(ResearchError::ArtifactIo {
                uri: uri.as_str().to_owned(),
                detail: "resolved path escapes the artifact store root".to_owned(),
            }
            .into());
        }
        Ok(candidate)
    }
}

#[async_trait]
impl ArtifactStore for LocalArtifactStore {
    async fn put_stream(
        &self,
        key: ArtifactKey,
        mut stream: ArtifactByteStream,
    ) -> QuantResult<ArtifactUri> {
        let path = self.absolute_path(&key)?;
        let parent = path.parent().ok_or_else(|| ResearchError::ArtifactIo {
            uri: path.display().to_string(),
            detail: "artifact path has no parent directory".to_owned(),
        })?;
        fs::create_dir_all(parent)
            .await
            .map_err(|error| io_error(&path, &error))?;

        // Atomic publish: write a uniquely-named temp file, then rename onto the
        // final path so readers never observe a partial write.
        let temp = parent.join(temp_file_name(&key));
        let mut file = fs::File::create(&temp)
            .await
            .map_err(|error| io_error(&temp, &error))?;
        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            file.write_all(&chunk)
                .await
                .map_err(|error| io_error(&temp, &error))?;
        }
        file.sync_all()
            .await
            .map_err(|error| io_error(&temp, &error))?;
        drop(file);
        if let Err(error) = fs::rename(&temp, &path).await {
            let _ = fs::remove_file(&temp).await;
            return Err(io_error(&path, &error).into());
        }

        ArtifactUri::parse(format!("{FILE_SCHEME}{}", path.display())).map_err(Into::into)
    }

    async fn get_stream(&self, uri: &ArtifactUri) -> QuantResult<ArtifactByteStream> {
        let path = self.path_from_uri(uri)?;
        match fs::File::open(&path).await {
            Ok(file) => {
                let display = path.display().to_string();
                let stream = ReaderStream::new(file).map(move |result| {
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
            Err(error) if error.kind() == ErrorKind::NotFound => {
                Err(ResearchError::ArtifactNotFound {
                    uri: uri.as_str().to_owned(),
                }
                .into())
            }
            Err(error) => Err(io_error(&path, &error).into()),
        }
    }

    async fn durability(&self, _uri: &ArtifactUri) -> QuantResult<ArtifactDurability> {
        Ok(ArtifactDurability {
            remote: false,
            versioned: false,
            object_locked: false,
        })
    }

    async fn metadata(&self, uri: &ArtifactUri) -> QuantResult<ArtifactObjectMetadata> {
        let path = self.path_from_uri(uri)?;
        let metadata = fs::metadata(&path)
            .await
            .map_err(|error| io_error(&path, &error))?;
        Ok(ArtifactObjectMetadata {
            byte_size: metadata.len(),
            etag: None,
            version_id: None,
            durability: ArtifactDurability {
                remote: false,
                versioned: false,
                object_locked: false,
            },
        })
    }

    async fn signed_download_url(
        &self,
        uri: &ArtifactUri,
        _valid_for: Duration,
    ) -> QuantResult<String> {
        Err(ResearchError::ArtifactIo {
            uri: uri.as_str().to_owned(),
            detail: "local artifact storage cannot issue signed download URLs".to_owned(),
        }
        .into())
    }

    async fn exists(&self, uri: &ArtifactUri) -> QuantResult<bool> {
        let path = self.path_from_uri(uri)?;
        fs::try_exists(&path)
            .await
            .map_err(|error| io_error(&path, &error).into())
    }

    async fn get_by_key(&self, key: &ArtifactKey) -> QuantResult<Vec<u8>> {
        let path = self.absolute_path(key)?;
        match fs::read(&path).await {
            Ok(bytes) => Ok(bytes),
            Err(error) if error.kind() == ErrorKind::NotFound => {
                Err(ResearchError::ArtifactNotFound {
                    uri: path.display().to_string(),
                }
                .into())
            }
            Err(error) => Err(io_error(&path, &error).into()),
        }
    }

    async fn exists_by_key(&self, key: &ArtifactKey) -> QuantResult<bool> {
        let path = self.absolute_path(key)?;
        fs::try_exists(&path)
            .await
            .map_err(|error| io_error(&path, &error).into())
    }
}

/// Make `path` absolute without requiring it to exist (no symlink resolution).
fn absolutize(path: &Path) -> QuantResult<PathBuf> {
    path::absolute(path).map_err(|error| {
        ResearchError::ArtifactIo {
            uri: path.display().to_string(),
            detail: format!("failed to absolutize path: {error}"),
        }
        .into()
    })
}

/// Build an [`ResearchError::ArtifactIo`] from a filesystem error at `path`.
fn io_error(path: &Path, error: &Error) -> ResearchError {
    ResearchError::ArtifactIo {
        uri: path.display().to_string(),
        detail: error.to_string(),
    }
}

/// Process- and time-unique temp file name for an atomic write.
fn temp_file_name(key: &ArtifactKey) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos());
    format!(
        ".{}.{}.{nanos}.tmp",
        key.namespace().as_str(),
        process::id()
    )
}

#[cfg(test)]
mod tests {
    use super::{ArtifactKey, ArtifactStore, LocalArtifactStore};
    use quant_pivot_models::types::ArtifactUri;

    use crate::artifact::ArtifactNamespace;
    use std::{
        env, fs,
        path::PathBuf,
        process,
        sync::atomic::{AtomicU64, Ordering},
        time::{SystemTime, UNIX_EPOCH},
    };

    fn temp_root() -> PathBuf {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        env::temp_dir().join(format!(
            "qp_artifact_test_{}_{}_{}",
            process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_or(0, |d| d.as_nanos()),
            COUNTER.fetch_add(1, Ordering::Relaxed)
        ))
    }

    #[tokio::test]
    async fn put_then_get_roundtrips() {
        let root = temp_root();
        let store = LocalArtifactStore::new(&root);
        let key =
            ArtifactKey::new(ArtifactNamespace::Dataset, "abc123", "parquet").expect("valid key");
        let uri = store.put(key, b"hello bytes").await.expect("put");
        assert!(uri.as_str().starts_with("file://"));
        assert!(store.exists(&uri).await.expect("exists"));
        assert_eq!(store.get(&uri).await.expect("get"), b"hello bytes");
        let _ = fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn get_missing_uri_returns_typed_error() {
        let root = temp_root();
        let store = LocalArtifactStore::new(&root);
        let uri = ArtifactUri::parse(format!(
            "file://{}/datasets/missing.parquet",
            root.display()
        ))
        .expect("uri");
        let err = store.get(&uri).await.expect_err("missing must error");
        assert!(err.to_string().contains("artifact not found"));
        let _ = fs::remove_dir_all(&root);
    }
}
