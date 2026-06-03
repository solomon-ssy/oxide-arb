//! Evidence coverage vocabulary.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MissingEvidenceField {
    TokenYes,
    TokenNo,
    FillProbability,
    Score,
    BookContext,
    AppliedFactors,
    ScoredSnapshot,
    ResolutionProb,
    Confidence,
    PriceZone,
    DurationBucket,
    DepthUsedPct,
    Staleness,
    Category,
}
