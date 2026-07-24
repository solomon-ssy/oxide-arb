//! Immutable recommendation-resolution outcome persistence contracts.

use std::cmp::Ordering;

use chrono::{DateTime, Utc};
use quant_pivot_error::hashing::CanonicalDigestError;
use sea_orm::{DeriveIntoActiveModel, DerivePartialModel};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    entities::quant_recommendation_resolution_outcome,
    enums::quant::RecommendationResolutionKind,
    hashing::CanonicalDigest,
    types::{ContentHash, MarketId, PayoutRatio, RecommendationId, SchemaVersion, TokenId},
};

/// Maximum bounded page size for outcome-ledger keyset scans.
pub const RECOMMENDATION_RESOLUTION_OUTCOME_PAGE_LIMIT: u32 = 1_000;

/// Insert payload for the WORM recommendation-resolution outcome ledger.
#[derive(Debug, Clone, Serialize, Deserialize, DeriveIntoActiveModel)]
#[sea_orm(active_model = "crate::entities::quant_recommendation_resolution_outcome::ActiveModel")]
pub struct NewRecommendationResolutionOutcome {
    pub recommendation_id: RecommendationId,
    pub market_id: MarketId,
    pub token_id: TokenId,
    pub resolution_kind: RecommendationResolutionKind,
    pub token_payout_ratio: PayoutRatio,
    /// Venue economic resolution time.
    pub resolved_at: DateTime<Utc>,
    /// Time the canonical source fact first became visible to the platform.
    pub source_observed_at: DateTime<Utc>,
    /// Frozen source frontier used by reconciliation.
    pub source_checkpoint_hash: ContentHash,
    /// Hash of the exact canonical `market_resolution_event` row.
    pub resolution_fact_hash: ContentHash,
    /// Canonical EVM log index within the source transaction.
    pub resolution_fact_log_index: i64,
    pub resolution_fact_schema_version: SchemaVersion,
}

impl NewRecommendationResolutionOutcome {
    /// Compute the canonical hash after the repository assigns system availability.
    pub fn expected_outcome_hash(
        &self,
        available_at: DateTime<Utc>,
    ) -> Result<ContentHash, RecommendationResolutionOutcomeContractError> {
        let input = ResolutionOutcomeHashInput::from_new(self, available_at);
        validate_fields(&input)?;
        Ok(ContentHash::try_from(&input)?)
    }

    /// Validate source timeline, identity, payout shape, and log identity.
    pub fn validate(&self) -> Result<(), RecommendationResolutionOutcomeContractError> {
        validate_derivation_fields(&ResolutionOutcomeHashInput::from_new(
            self,
            self.source_observed_at,
        ))
    }
}

/// Immutable recommendation-resolution outcome row.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, DerivePartialModel)]
#[sea_orm(entity = "crate::entities::quant_recommendation_resolution_outcome::Entity")]
pub struct RecommendationResolutionOutcomeInfo {
    pub recommendation_id: RecommendationId,
    pub market_id: MarketId,
    pub token_id: TokenId,
    pub resolution_kind: RecommendationResolutionKind,
    pub token_payout_ratio: PayoutRatio,
    pub resolved_at: DateTime<Utc>,
    pub source_observed_at: DateTime<Utc>,
    pub available_at: DateTime<Utc>,
    pub source_checkpoint_hash: ContentHash,
    pub resolution_fact_hash: ContentHash,
    pub resolution_fact_log_index: i64,
    pub resolution_fact_schema_version: SchemaVersion,
    pub outcome_hash: ContentHash,
    pub created_at: DateTime<Utc>,
}

impl RecommendationResolutionOutcomeInfo {
    /// Compute the canonical hash of the source derivation and database-owned availability.
    pub fn expected_outcome_hash(
        &self,
    ) -> Result<ContentHash, RecommendationResolutionOutcomeContractError> {
        let input = ResolutionOutcomeHashInput::from_info(self);
        validate_fields(&input)?;
        Ok(ContentHash::try_from(&input)?)
    }

    /// Recompute and verify the stored immutable content.
    pub fn validate(&self) -> Result<(), RecommendationResolutionOutcomeContractError> {
        let expected = self.expected_outcome_hash()?;
        if expected != self.outcome_hash {
            return Err(
                RecommendationResolutionOutcomeContractError::OutcomeHashMismatch {
                    expected,
                    actual: self.outcome_hash,
                },
            );
        }
        Ok(())
    }

    /// Whether an idempotent retry carries the exact stored source derivation.
    #[must_use]
    pub fn has_same_derivation(&self, candidate: &NewRecommendationResolutionOutcome) -> bool {
        self.recommendation_id == candidate.recommendation_id
            && self.market_id == candidate.market_id
            && self.token_id == candidate.token_id
            && self.resolution_kind == candidate.resolution_kind
            && self.token_payout_ratio == candidate.token_payout_ratio
            && self.resolved_at == candidate.resolved_at
            && self.source_observed_at == candidate.source_observed_at
            && self.source_checkpoint_hash == candidate.source_checkpoint_hash
            && self.resolution_fact_hash == candidate.resolution_fact_hash
            && self.resolution_fact_log_index == candidate.resolution_fact_log_index
            && self.resolution_fact_schema_version == candidate.resolution_fact_schema_version
    }

    /// Stable keyset position for this row.
    #[must_use]
    pub const fn cursor(&self) -> RecommendationResolutionOutcomeCursor {
        RecommendationResolutionOutcomeCursor {
            available_at: self.available_at,
            recommendation_id: self.recommendation_id,
        }
    }
}

info_from_model!(
    RecommendationResolutionOutcomeInfo,
    quant_recommendation_resolution_outcome::Model,
    {
        recommendation_id,
        market_id,
        token_id,
        resolution_kind,
        token_payout_ratio,
        resolved_at,
        source_observed_at,
        available_at,
        source_checkpoint_hash,
        resolution_fact_hash,
        resolution_fact_log_index,
        resolution_fact_schema_version,
        outcome_hash,
        created_at,
    }
);

/// Outcome of an idempotent WORM append.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InsertResolutionOutcomeResult {
    /// This call inserted the canonical row.
    Inserted(RecommendationResolutionOutcomeInfo),
    /// The exact immutable row was already present.
    AlreadyPresent(RecommendationResolutionOutcomeInfo),
}

/// Total-order key for deterministic `(available_at, recommendation_id)` scans.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecommendationResolutionOutcomeCursor {
    pub available_at: DateTime<Utc>,
    pub recommendation_id: RecommendationId,
}

impl PartialOrd for RecommendationResolutionOutcomeCursor {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for RecommendationResolutionOutcomeCursor {
    fn cmp(&self, other: &Self) -> Ordering {
        self.available_at.cmp(&other.available_at).then_with(|| {
            self.recommendation_id
                .as_uuid()
                .cmp(&other.recommendation_id.as_uuid())
        })
    }
}

/// One bounded, frozen-window keyset request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecommendationResolutionOutcomePageQuery {
    /// Inclusive lower availability bound.
    pub available_from: DateTime<Utc>,
    /// Inclusive frozen availability cutoff.
    pub available_through: DateTime<Utc>,
    /// Exclusive keyset cursor returned by the preceding page.
    pub after: Option<RecommendationResolutionOutcomeCursor>,
    pub limit: u32,
}

impl RecommendationResolutionOutcomePageQuery {
    pub fn validate(&self) -> Result<(), RecommendationResolutionOutcomePageQueryError> {
        if self.available_from > self.available_through {
            return Err(
                RecommendationResolutionOutcomePageQueryError::InvalidWindow {
                    available_from: self.available_from,
                    available_through: self.available_through,
                },
            );
        }
        if !(1..=RECOMMENDATION_RESOLUTION_OUTCOME_PAGE_LIMIT).contains(&self.limit) {
            return Err(
                RecommendationResolutionOutcomePageQueryError::InvalidLimit {
                    actual: self.limit,
                    maximum: RECOMMENDATION_RESOLUTION_OUTCOME_PAGE_LIMIT,
                },
            );
        }
        if let Some(cursor) = self.after
            && (cursor.available_at < self.available_from
                || cursor.available_at > self.available_through)
        {
            return Err(
                RecommendationResolutionOutcomePageQueryError::CursorOutsideWindow {
                    cursor_available_at: cursor.available_at,
                    available_from: self.available_from,
                    available_through: self.available_through,
                },
            );
        }
        Ok(())
    }
}

/// One bounded outcome page and its exclusive continuation cursor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecommendationResolutionOutcomePage {
    pub outcomes: Vec<RecommendationResolutionOutcomeInfo>,
    pub next_cursor: Option<RecommendationResolutionOutcomeCursor>,
}

impl RecommendationResolutionOutcomePage {
    #[must_use]
    pub fn new(outcomes: Vec<RecommendationResolutionOutcomeInfo>) -> Self {
        let next_cursor = outcomes
            .last()
            .map(RecommendationResolutionOutcomeInfo::cursor);
        Self {
            outcomes,
            next_cursor,
        }
    }
}

/// Invalid immutable resolution-outcome content.
#[derive(Debug, Error)]
pub enum RecommendationResolutionOutcomeContractError {
    #[error("market_id and token_id must both be non-empty")]
    EmptyIdentity,
    #[error("resolution timeline must satisfy resolved_at <= source_observed_at <= available_at")]
    InvalidTimeline,
    #[error(
        "resolution kind {resolution_kind:?} is incompatible with token payout {token_payout_ratio}"
    )]
    ResolutionKindMismatch {
        resolution_kind: RecommendationResolutionKind,
        token_payout_ratio: PayoutRatio,
    },
    #[error("resolution fact log index must be non-negative, got {actual}")]
    NegativeLogIndex { actual: i64 },
    #[error("resolution outcome hash mismatch: expected {expected}, got {actual}")]
    OutcomeHashMismatch {
        expected: ContentHash,
        actual: ContentHash,
    },
    #[error(transparent)]
    CanonicalDigest(#[from] CanonicalDigestError),
}

/// Invalid bounded keyset request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum RecommendationResolutionOutcomePageQueryError {
    #[error(
        "available_from {available_from} must be no later than available_through {available_through}"
    )]
    InvalidWindow {
        available_from: DateTime<Utc>,
        available_through: DateTime<Utc>,
    },
    #[error("resolution outcome page limit must be within 1..={maximum}, got {actual}")]
    InvalidLimit { actual: u32, maximum: u32 },
    #[error(
        "cursor availability {cursor_available_at} is outside [{available_from}, {available_through}]"
    )]
    CursorOutsideWindow {
        cursor_available_at: DateTime<Utc>,
        available_from: DateTime<Utc>,
        available_through: DateTime<Utc>,
    },
}

#[derive(Serialize)]
struct ResolutionOutcomeHashInput<'a> {
    contract: &'static str,
    recommendation_id: &'a RecommendationId,
    market_id: &'a MarketId,
    token_id: &'a TokenId,
    resolution_kind: RecommendationResolutionKind,
    token_payout_ratio: PayoutRatio,
    resolved_at: DateTime<Utc>,
    source_observed_at: DateTime<Utc>,
    available_at: DateTime<Utc>,
    source_checkpoint_hash: &'a ContentHash,
    resolution_fact_hash: &'a ContentHash,
    resolution_fact_log_index: i64,
    resolution_fact_schema_version: SchemaVersion,
}

impl<'a> ResolutionOutcomeHashInput<'a> {
    const fn from_new(
        outcome: &'a NewRecommendationResolutionOutcome,
        available_at: DateTime<Utc>,
    ) -> Self {
        Self {
            contract: "recommendation_resolution_outcome_v1",
            recommendation_id: &outcome.recommendation_id,
            market_id: &outcome.market_id,
            token_id: &outcome.token_id,
            resolution_kind: outcome.resolution_kind,
            token_payout_ratio: outcome.token_payout_ratio,
            resolved_at: outcome.resolved_at,
            source_observed_at: outcome.source_observed_at,
            available_at,
            source_checkpoint_hash: &outcome.source_checkpoint_hash,
            resolution_fact_hash: &outcome.resolution_fact_hash,
            resolution_fact_log_index: outcome.resolution_fact_log_index,
            resolution_fact_schema_version: outcome.resolution_fact_schema_version,
        }
    }

    const fn from_info(outcome: &'a RecommendationResolutionOutcomeInfo) -> Self {
        Self {
            contract: "recommendation_resolution_outcome_v1",
            recommendation_id: &outcome.recommendation_id,
            market_id: &outcome.market_id,
            token_id: &outcome.token_id,
            resolution_kind: outcome.resolution_kind,
            token_payout_ratio: outcome.token_payout_ratio,
            resolved_at: outcome.resolved_at,
            source_observed_at: outcome.source_observed_at,
            available_at: outcome.available_at,
            source_checkpoint_hash: &outcome.source_checkpoint_hash,
            resolution_fact_hash: &outcome.resolution_fact_hash,
            resolution_fact_log_index: outcome.resolution_fact_log_index,
            resolution_fact_schema_version: outcome.resolution_fact_schema_version,
        }
    }
}

impl TryFrom<&ResolutionOutcomeHashInput<'_>> for ContentHash {
    type Error = CanonicalDigestError;

    fn try_from(input: &ResolutionOutcomeHashInput<'_>) -> Result<Self, Self::Error> {
        CanonicalDigest::content_hash_json(input)
    }
}

fn validate_fields(
    input: &ResolutionOutcomeHashInput<'_>,
) -> Result<(), RecommendationResolutionOutcomeContractError> {
    validate_derivation_fields(input)?;
    if input.source_observed_at > input.available_at {
        return Err(RecommendationResolutionOutcomeContractError::InvalidTimeline);
    }
    Ok(())
}

fn validate_derivation_fields(
    input: &ResolutionOutcomeHashInput<'_>,
) -> Result<(), RecommendationResolutionOutcomeContractError> {
    if input.market_id.as_str().is_empty() || input.token_id.as_str().is_empty() {
        return Err(RecommendationResolutionOutcomeContractError::EmptyIdentity);
    }
    if input.resolved_at > input.source_observed_at {
        return Err(RecommendationResolutionOutcomeContractError::InvalidTimeline);
    }
    let valid_payout_shape = match input.resolution_kind {
        RecommendationResolutionKind::WinnerTakeAll => {
            input.token_payout_ratio == PayoutRatio::ZERO
                || input.token_payout_ratio == PayoutRatio::ONE
        }
        RecommendationResolutionKind::SplitPayout => {
            input.token_payout_ratio > PayoutRatio::ZERO
                && input.token_payout_ratio < PayoutRatio::ONE
        }
    };
    if !valid_payout_shape {
        return Err(
            RecommendationResolutionOutcomeContractError::ResolutionKindMismatch {
                resolution_kind: input.resolution_kind,
                token_payout_ratio: input.token_payout_ratio,
            },
        );
    }
    if input.resolution_fact_log_index < 0 {
        return Err(
            RecommendationResolutionOutcomeContractError::NegativeLogIndex {
                actual: input.resolution_fact_log_index,
            },
        );
    }
    SchemaVersion::try_new(input.resolution_fact_schema_version.get())?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use chrono::{Duration, TimeZone, Utc};
    use rust_decimal_macros::dec;

    use super::{
        NewRecommendationResolutionOutcome, RecommendationResolutionOutcomeCursor,
        RecommendationResolutionOutcomePageQuery, RecommendationResolutionOutcomePageQueryError,
    };
    use crate::{
        enums::quant::RecommendationResolutionKind,
        types::{ContentHash, MarketId, PayoutRatio, RecommendationId, SchemaVersion, TokenId},
    };

    fn hash(seed: char) -> ContentHash {
        ContentHash::parse(&format!("blake3:{}", seed.to_string().repeat(64))).expect("valid hash")
    }

    impl NewRecommendationResolutionOutcome {
        fn test_fixture() -> Self {
            let resolved_at = Utc
                .with_ymd_and_hms(2026, 7, 1, 0, 0, 0)
                .single()
                .expect("timestamp");
            Self {
                recommendation_id: RecommendationId::from_v7(),
                market_id: MarketId::new("0xmarket"),
                token_id: TokenId::new("123"),
                resolution_kind: RecommendationResolutionKind::SplitPayout,
                token_payout_ratio: PayoutRatio::try_new(dec!(0.5)).expect("split payout"),
                resolved_at,
                source_observed_at: resolved_at + Duration::seconds(1),
                source_checkpoint_hash: hash('a'),
                resolution_fact_hash: hash('b'),
                resolution_fact_log_index: 1,
                resolution_fact_schema_version: SchemaVersion::FIRST,
            }
        }
    }

    #[test]
    fn database_availability_hash_validated() {
        let outcome = NewRecommendationResolutionOutcome::test_fixture();
        assert!(outcome.validate().is_ok());
        let available_at = outcome.source_observed_at + Duration::seconds(1);
        let outcome_hash = outcome
            .expected_outcome_hash(available_at)
            .expect("outcome hash");
        let later_hash = outcome
            .expected_outcome_hash(available_at + Duration::milliseconds(1))
            .expect("later outcome hash");
        assert_ne!(outcome_hash, later_hash);

        assert!(
            outcome
                .expected_outcome_hash(outcome.source_observed_at - Duration::milliseconds(1))
                .is_err()
        );

        let mut invalid_split = outcome;
        invalid_split.token_payout_ratio = PayoutRatio::ONE;
        assert!(invalid_split.validate().is_err());

        invalid_split.token_payout_ratio =
            PayoutRatio::try_new(dec!(0.5)).expect("restore split payout");
        invalid_split.resolution_fact_schema_version = SchemaVersion::new(0);
        assert!(invalid_split.validate().is_err());
    }

    #[test]
    fn page_query_bounded_window() {
        let outcome = NewRecommendationResolutionOutcome::test_fixture();
        let available_at = outcome.source_observed_at + Duration::seconds(1);
        let invalid_limit = RecommendationResolutionOutcomePageQuery {
            available_from: outcome.resolved_at,
            available_through: available_at,
            after: None,
            limit: 0,
        };
        assert!(matches!(
            invalid_limit.validate(),
            Err(RecommendationResolutionOutcomePageQueryError::InvalidLimit { .. })
        ));

        let outside_cursor = RecommendationResolutionOutcomePageQuery {
            available_from: outcome.resolved_at,
            available_through: available_at,
            after: Some(RecommendationResolutionOutcomeCursor {
                available_at: available_at + Duration::seconds(1),
                recommendation_id: outcome.recommendation_id,
            }),
            limit: 10,
        };
        assert!(matches!(
            outside_cursor.validate(),
            Err(RecommendationResolutionOutcomePageQueryError::CursorOutsideWindow { .. })
        ));
    }
}
