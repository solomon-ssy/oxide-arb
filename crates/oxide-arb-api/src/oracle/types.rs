//! Oracle domain types.

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

/// A vote from a single oracle source.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceVote {
    pub source_id: String,
    pub actual_yes: bool,
    pub confidence: Decimal,
    pub reported_at: DateTime<Utc>,
}

/// The final resolution verdict from the voting oracle.
#[derive(Debug, Clone)]
pub enum ResolutionVerdict {
    Resolved {
        actual_yes: bool,
        votes: Vec<SourceVote>,
    },
    Disputed {
        votes: Vec<SourceVote>,
    },
    Unresolved {
        reason: String,
    },
}
