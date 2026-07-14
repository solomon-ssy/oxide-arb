//! Canonical book snapshot replay handle for report evidence.
//!
//! Reports and replay both resolve the durable `ClickHouse` fact ledger and
//! therefore share the same exact tie-breaker coordinates.

use std::{
    fmt::{self, Display, Formatter},
    str::FromStr,
};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::types::{ContentHash, TokenId};

/// Where the captured book snapshot was read from.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BookSnapshotSource {
    /// Durable canonical L2 state reconstructed from a checkpoint and its
    /// contiguous event stream, anchored to the latest applied book event.
    CanonicalL2 {
        stream_session_id: Uuid,
        token_sequence: u64,
        source_event_hash: ContentHash,
        event_time_ms: i64,
        ingestion_time_ms: i64,
    },
}

/// Replay handle for the order book frozen at decision time.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BookSnapshotRef {
    pub token_id: TokenId,
    pub source: BookSnapshotSource,
    /// Blake3 digest of bid/ask levels at capture — integrity check on replay.
    pub content_hash: ContentHash,
}

impl BookSnapshotRef {
    /// Stable wire form shared by feature provenance and recommendation evidence.
    #[must_use]
    pub fn canonical_string(&self) -> String {
        format!("{self}")
    }
}

impl Display for BookSnapshotRef {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match &self.source {
            BookSnapshotSource::CanonicalL2 {
                stream_session_id,
                token_sequence,
                source_event_hash,
                event_time_ms,
                ingestion_time_ms,
            } => write!(
                f,
                "book:l2|{}|{}|{}|{}|{}|{}@{}",
                self.token_id,
                stream_session_id,
                token_sequence,
                source_event_hash,
                event_time_ms,
                ingestion_time_ms,
                self.content_hash
            ),
        }
    }
}

impl FromStr for BookSnapshotRef {
    type Err = BookSnapshotRefParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let (body, hash) = value.split_once('@').ok_or(BookSnapshotRefParseError)?;
        let content_hash = ContentHash::parse(hash).map_err(|_| BookSnapshotRefParseError)?;
        let rest = body
            .strip_prefix("book:")
            .ok_or(BookSnapshotRefParseError)?;
        if let Some(l2) = rest.strip_prefix("l2|") {
            let mut parts = l2.split('|');
            let token_id = TokenId::new(parts.next().ok_or(BookSnapshotRefParseError)?);
            let stream_session_id = parts
                .next()
                .ok_or(BookSnapshotRefParseError)?
                .parse::<Uuid>()
                .map_err(|_| BookSnapshotRefParseError)?;
            let token_sequence: u64 = parts
                .next()
                .ok_or(BookSnapshotRefParseError)?
                .parse()
                .map_err(|_| BookSnapshotRefParseError)?;
            let source_event_hash =
                ContentHash::parse(parts.next().ok_or(BookSnapshotRefParseError)?)
                    .map_err(|_| BookSnapshotRefParseError)?;
            let event_time_ms: i64 = parts
                .next()
                .ok_or(BookSnapshotRefParseError)?
                .parse()
                .map_err(|_| BookSnapshotRefParseError)?;
            let ingestion_time_ms: i64 = parts
                .next()
                .ok_or(BookSnapshotRefParseError)?
                .parse()
                .map_err(|_| BookSnapshotRefParseError)?;
            if parts.next().is_some() {
                return Err(BookSnapshotRefParseError);
            }
            return Ok(Self {
                token_id,
                source: BookSnapshotSource::CanonicalL2 {
                    stream_session_id,
                    token_sequence,
                    source_event_hash,
                    event_time_ms,
                    ingestion_time_ms,
                },
                content_hash,
            });
        }
        Err(BookSnapshotRefParseError)
    }
}

/// Parse errors for [`BookSnapshotRef`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BookSnapshotRefParseError;
