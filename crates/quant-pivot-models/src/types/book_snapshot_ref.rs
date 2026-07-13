//! Canonical book snapshot replay handle for report evidence.
//!
//! Reports and replay both resolve the durable `ClickHouse` fact ledger and
//! therefore share the same exact tie-breaker coordinates.

use std::fmt::{self, Display, Formatter};
use std::str::FromStr;

use serde::{Deserialize, Serialize};

use crate::types::{ContentHash, TokenId};

/// Where the captured book snapshot was read from.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BookSnapshotSource {
    /// Durable `ClickHouse` `book_snapshots` fact.
    ClickHouse {
        event_time_ms: i64,
        ingestion_time_ms: i64,
        sequence: u64,
        book_version: u64,
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
            BookSnapshotSource::ClickHouse {
                event_time_ms,
                ingestion_time_ms,
                sequence,
                book_version,
            } => write!(
                f,
                "book:ch:{}:{}:{}:{}:{}@{}",
                self.token_id,
                event_time_ms,
                ingestion_time_ms,
                sequence,
                book_version,
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
        if let Some(ch) = rest.strip_prefix("ch:") {
            let mut parts = ch.split(':');
            let token_id = TokenId::new(parts.next().ok_or(BookSnapshotRefParseError)?);
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
            let sequence: u64 = parts
                .next()
                .ok_or(BookSnapshotRefParseError)?
                .parse()
                .map_err(|_| BookSnapshotRefParseError)?;
            let book_version: u64 = parts
                .next()
                .ok_or(BookSnapshotRefParseError)?
                .parse()
                .map_err(|_| BookSnapshotRefParseError)?;
            if parts.next().is_some() {
                return Err(BookSnapshotRefParseError);
            }
            return Ok(Self {
                token_id,
                source: BookSnapshotSource::ClickHouse {
                    event_time_ms,
                    ingestion_time_ms,
                    sequence,
                    book_version,
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
