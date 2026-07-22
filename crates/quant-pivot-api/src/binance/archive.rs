//! Verified Binance public-data archive transport.

use std::{
    fmt::Display,
    io::{Cursor, Read},
    str::{self, FromStr},
};

use quant_pivot_compute::ComputeTask;
use quant_pivot_error::{QuantResult, api::ApiError};
use reqwest::Client;
use sha2::{Digest, Sha256};
use tokio::sync::mpsc::{Receiver, Sender};
use zip::ZipArchive;

use crate::infra::{http::get_optional_bytes_with_retry, retry::RetryPolicy};

/// Bounded bridge from blocking ZIP/CSV decoding into async persistence.
///
/// The compressed archive has already passed its official SHA-256 sidecar
/// check before this stream is constructed. At most the configured channel
/// depth plus one decoded batch is resident; decoder errors and task panics are
/// surfaced after the final successfully decoded batch.
pub struct BinanceArchiveBatchStream<T> {
    receiver: Receiver<Vec<T>>,
    decoder: Option<ComputeTask<()>>,
}

impl<T> BinanceArchiveBatchStream<T> {
    pub(super) const fn new(receiver: Receiver<Vec<T>>, decoder: ComputeTask<()>) -> Self {
        Self {
            receiver,
            decoder: Some(decoder),
        }
    }

    pub async fn next_batch(&mut self) -> QuantResult<Option<Vec<T>>> {
        if let Some(batch) = self.receiver.recv().await {
            return Ok(Some(batch));
        }
        let Some(decoder) = self.decoder.take() else {
            return Ok(None);
        };
        decoder.join().await?;
        Ok(None)
    }
}

pub(super) fn send_batch<T>(sender: &Sender<Vec<T>>, batch: Vec<T>) -> bool {
    batch.is_empty() || sender.blocking_send(batch).is_ok()
}

pub(super) async fn download_verified_archive(
    http: &Client,
    retry_policy: &RetryPolicy,
    url: &str,
    filename: &str,
) -> QuantResult<Option<Vec<u8>>> {
    let checksum_url = format!("{url}.CHECKSUM");
    let (archive, checksum) = tokio::try_join!(
        get_optional_bytes_with_retry(http, retry_policy, url),
        get_optional_bytes_with_retry(http, retry_policy, &checksum_url),
    )?;
    let Some(archive) = archive else {
        if checksum.is_some() {
            return Err(archive_error("checksum exists but archive is absent").into());
        }
        return Ok(None);
    };
    let checksum =
        checksum.ok_or_else(|| archive_error("archive exists without its required checksum"))?;
    verify_archive_checksum(filename, &archive, &checksum)?;
    Ok(Some(archive))
}

pub(super) fn decode_single_csv_archive<T>(
    archive: Vec<u8>,
    expected_member: &str,
    decode: impl FnOnce(&mut dyn Read) -> QuantResult<T>,
) -> QuantResult<T> {
    let mut zip = ZipArchive::new(Cursor::new(archive))
        .map_err(|error| archive_error(format!("invalid ZIP: {error}")))?;
    if zip.len() != 1 {
        return Err(archive_error("archive must contain exactly one CSV member").into());
    }
    let mut member = zip
        .by_index(0)
        .map_err(|error| archive_error(format!("cannot open ZIP member: {error}")))?;
    if member.is_dir() || member.name() != expected_member {
        return Err(archive_error(format!(
            "archive member `{}` does not match `{expected_member}`",
            member.name()
        ))
        .into());
    }
    decode(&mut member)
}

pub(super) fn verify_archive_checksum(
    filename: &str,
    archive: &[u8],
    checksum: &[u8],
) -> QuantResult<()> {
    let text = str::from_utf8(checksum)
        .map_err(|error| archive_error(format!("checksum is not UTF-8: {error}")))?;
    let mut fields = text.split_whitespace();
    let expected = fields
        .next()
        .filter(|value| value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit()))
        .ok_or_else(|| archive_error("checksum does not start with a SHA-256 digest"))?;
    let named_file = fields
        .next()
        .map(|value| value.trim_start_matches('*'))
        .ok_or_else(|| archive_error("checksum does not name its archive"))?;
    if named_file != filename || fields.next().is_some() {
        return Err(
            archive_error("checksum does not exclusively name the requested archive").into(),
        );
    }
    let actual = hex::encode(Sha256::digest(archive));
    if !actual.eq_ignore_ascii_case(expected) {
        return Err(archive_error("archive SHA-256 checksum mismatch").into());
    }
    Ok(())
}

pub(super) fn parse_archive_field<T>(raw: &str, name: &str) -> QuantResult<T>
where
    T: FromStr,
    T::Err: Display,
{
    raw.parse()
        .map_err(|error| archive_error(format!("invalid {name} `{raw}`: {error}")).into())
}

pub(super) fn archive_error(detail: impl Into<String>) -> ApiError {
    ApiError::Deserialize {
        context: "Binance public-data archive".to_owned(),
        detail: detail.into(),
    }
}
