use quant_pivot_error::hashing::CanonicalDigestError;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    clickhouse::{ChPayoutRatio, ChSchemaVersion},
    enums::{clickhouse::ChFactSource, quant::RecommendationResolutionKind},
    hashing::CanonicalDigest,
    types::{ContentHash, EvmBlockHash, EvmTransactionHash, MarketId, PayoutRatio, TokenId},
};

/// Validated source fields used to seal one canonical resolution fact.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MarketResolutionFactInput {
    pub market_id: MarketId,
    pub token_ids: [TokenId; MarketResolutionRow::TOKEN_COUNT],
    pub payout_ratios: [PayoutRatio; MarketResolutionRow::TOKEN_COUNT],
    /// Economic resolution block time, epoch milliseconds.
    pub resolved_at: i64,
    /// First platform observation time, epoch milliseconds.
    pub observed_at: i64,
    pub source_block_number: u64,
    pub source_block_hash: EvmBlockHash,
    pub source_transaction_hash: EvmTransactionHash,
    pub source_log_index: u64,
    pub source_checkpoint_hash: ContentHash,
}

/// `ClickHouse` row for the `market_resolution_event` table — the single,
/// append-only, point-in-time settlement truth source.
///
/// `token_ids` and `payout_ratios` are positionally aligned and always contain
/// the complete binary payout vector. This preserves both winner-take-all
/// (`[1, 0]`) and split (`[0.5, 0.5]`) resolution without inventing a winner.
/// `resolved_at` is the economic settlement time; `observed_at` is when the
/// resolution was ingested (the PIT maturity anchor). Both are epoch
/// milliseconds bound to `DateTime64(3, 'UTC')` columns.
#[derive(Debug, Clone, PartialEq, Eq, clickhouse::Row, Serialize, Deserialize)]
pub struct MarketResolutionRow {
    pub market_id: MarketId,
    pub token_ids: Vec<TokenId>,
    pub payout_ratios: Vec<ChPayoutRatio>,
    /// Economic settlement (close) time, epoch milliseconds.
    pub resolved_at: i64,
    /// Writer ingestion time, epoch milliseconds (PIT maturity anchor).
    pub observed_at: i64,
    pub source: ChFactSource,
    pub source_block_number: u64,
    pub source_block_hash: EvmBlockHash,
    pub source_transaction_hash: EvmTransactionHash,
    pub source_log_index: u64,
    pub source_checkpoint_hash: ContentHash,
    pub resolution_fact_hash: ContentHash,
    pub schema_version: ChSchemaVersion,
}

impl MarketResolutionRow {
    pub const TOKEN_COUNT: usize = 2;

    /// Seal exact finalized source lineage into a content-addressed row.
    pub fn seal(input: MarketResolutionFactInput) -> Result<Self, MarketResolutionContractError> {
        let mut row = Self {
            market_id: input.market_id,
            token_ids: input.token_ids.into_iter().collect(),
            payout_ratios: input
                .payout_ratios
                .into_iter()
                .map(ChPayoutRatio::from)
                .collect(),
            resolved_at: input.resolved_at,
            observed_at: input.observed_at,
            source: ChFactSource::ResolutionReconciliation,
            source_block_number: input.source_block_number,
            source_block_hash: input.source_block_hash,
            source_transaction_hash: input.source_transaction_hash,
            source_log_index: input.source_log_index,
            source_checkpoint_hash: input.source_checkpoint_hash,
            resolution_fact_hash: ContentHash::from_bytes([0; 32]),
            schema_version: ChSchemaVersion::FIRST,
        };
        validate_source_fields(&row)?;
        row.resolution_fact_hash = row.expected_resolution_fact_hash()?;
        row.validate()?;
        Ok(row)
    }

    /// Recompute the content address over every source and payout field.
    pub fn expected_resolution_fact_hash(
        &self,
    ) -> Result<ContentHash, MarketResolutionContractError> {
        let input = MarketResolutionFactHashInput::from(self);
        Ok(CanonicalDigest::content_hash_typed(
            "quant-pivot/market-resolution-fact",
            1,
            &input,
        )?)
    }

    /// Validate the complete persisted vector before a write or after a read.
    pub fn validate(&self) -> Result<(), MarketResolutionContractError> {
        validate_source_fields(self)?;
        if self.token_ids.len() != Self::TOKEN_COUNT {
            return Err(MarketResolutionContractError::UnsupportedTokenCount {
                actual: self.token_ids.len(),
            });
        }
        if self.token_ids.len() != self.payout_ratios.len() {
            return Err(MarketResolutionContractError::CardinalityMismatch {
                token_count: self.token_ids.len(),
                payout_count: self.payout_ratios.len(),
            });
        }

        for (index, token_id) in self.token_ids.iter().enumerate() {
            if !is_canonical_token_id(token_id.as_str()) {
                return Err(MarketResolutionContractError::InvalidTokenId { index });
            }
            if let Some(first_index) = self.token_ids[..index]
                .iter()
                .position(|existing| existing == token_id)
            {
                return Err(MarketResolutionContractError::DuplicateTokenId {
                    first_index,
                    duplicate_index: index,
                });
            }
        }

        let mut total = Decimal::ZERO;
        for (index, payout) in self.payout_ratios.iter().copied().enumerate() {
            let payout = payout
                .try_to_payout_ratio()
                .map_err(|_| MarketResolutionContractError::InvalidPayoutRatio { index })?;
            total += payout.inner();
        }
        total = total.normalize();
        if total != Decimal::ONE {
            return Err(MarketResolutionContractError::PayoutTotalNotOne { total });
        }
        let expected = self.expected_resolution_fact_hash()?;
        if self.resolution_fact_hash != expected {
            return Err(MarketResolutionContractError::ResolutionFactHashMismatch {
                expected,
                actual: self.resolution_fact_hash,
            });
        }
        Ok(())
    }

    /// Derive the resolution shape from the canonical payout vector.
    pub fn resolution_kind(
        &self,
    ) -> Result<RecommendationResolutionKind, MarketResolutionContractError> {
        self.validate()?;
        if self
            .payout_ratios
            .iter()
            .copied()
            .any(ChPayoutRatio::is_one)
        {
            Ok(RecommendationResolutionKind::WinnerTakeAll)
        } else {
            Ok(RecommendationResolutionKind::SplitPayout)
        }
    }

    /// Return one token's exact payout, rejecting corrupt or foreign vectors.
    pub fn payout_for(
        &self,
        token_id: &TokenId,
    ) -> Result<PayoutRatio, MarketResolutionContractError> {
        self.validate()?;
        let index = self
            .token_ids
            .iter()
            .position(|candidate| candidate == token_id)
            .ok_or(MarketResolutionContractError::TokenNotPresent)?;
        self.payout_ratios[index]
            .try_to_payout_ratio()
            .map_err(|_| MarketResolutionContractError::InvalidPayoutRatio { index })
    }
}

#[derive(Serialize)]
struct MarketResolutionFactHashInput<'a> {
    market_id: &'a MarketId,
    token_ids: &'a [TokenId],
    payout_ratios: &'a [ChPayoutRatio],
    resolved_at: i64,
    observed_at: i64,
    source: ChFactSource,
    source_block_number: u64,
    source_block_hash: &'a EvmBlockHash,
    source_transaction_hash: &'a EvmTransactionHash,
    source_log_index: u64,
    source_checkpoint_hash: ContentHash,
    schema_version: ChSchemaVersion,
}

impl<'a> From<&'a MarketResolutionRow> for MarketResolutionFactHashInput<'a> {
    fn from(row: &'a MarketResolutionRow) -> Self {
        Self {
            market_id: &row.market_id,
            token_ids: &row.token_ids,
            payout_ratios: &row.payout_ratios,
            resolved_at: row.resolved_at,
            observed_at: row.observed_at,
            source: row.source,
            source_block_number: row.source_block_number,
            source_block_hash: &row.source_block_hash,
            source_transaction_hash: &row.source_transaction_hash,
            source_log_index: row.source_log_index,
            source_checkpoint_hash: row.source_checkpoint_hash,
            schema_version: row.schema_version,
        }
    }
}

fn validate_source_fields(row: &MarketResolutionRow) -> Result<(), MarketResolutionContractError> {
    if row.source != ChFactSource::ResolutionReconciliation {
        return Err(MarketResolutionContractError::InvalidSource { actual: row.source });
    }
    if row.schema_version != ChSchemaVersion::FIRST {
        return Err(MarketResolutionContractError::UnsupportedSchemaVersion {
            actual: row.schema_version.0,
        });
    }
    if row.resolved_at <= 0 || row.observed_at <= 0 || row.resolved_at > row.observed_at {
        return Err(MarketResolutionContractError::InvalidTimeline {
            resolved_at: row.resolved_at,
            observed_at: row.observed_at,
        });
    }
    if row.source_block_number == 0 {
        return Err(MarketResolutionContractError::InvalidSourceBlock);
    }
    if row
        .source_checkpoint_hash
        .as_bytes()
        .iter()
        .all(|byte| *byte == 0)
    {
        return Err(MarketResolutionContractError::EmptySourceCheckpointHash);
    }
    Ok(())
}

const U256_MAX_DECIMAL: &str =
    "115792089237316195423570985008687907853269984665640564039457584007913129639935";

fn is_canonical_token_id(value: &str) -> bool {
    let bytes = value.as_bytes();
    if bytes.is_empty() || bytes[0] == b'0' || !bytes.iter().all(u8::is_ascii_digit) {
        return false;
    }
    value.len() < U256_MAX_DECIMAL.len()
        || (value.len() == U256_MAX_DECIMAL.len() && value <= U256_MAX_DECIMAL)
}

/// Corruption or source-contract violation in a market-resolution vector.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum MarketResolutionContractError {
    #[error("market resolution source must be ResolutionReconciliation, got {actual:?}")]
    InvalidSource { actual: ChFactSource },
    #[error("market resolution schema version {actual} is unsupported")]
    UnsupportedSchemaVersion { actual: u32 },
    #[error(
        "market resolution timeline must satisfy 0 < resolved_at <= observed_at, got {resolved_at}/{observed_at}"
    )]
    InvalidTimeline { resolved_at: i64, observed_at: i64 },
    #[error("market resolution source block must be positive")]
    InvalidSourceBlock,
    #[error("market resolution source checkpoint hash cannot be zero")]
    EmptySourceCheckpointHash,
    #[error("market resolution requires exactly two token ids, got {actual}")]
    UnsupportedTokenCount { actual: usize },
    #[error(
        "market resolution token/payout cardinality mismatch: {token_count} tokens, {payout_count} payouts"
    )]
    CardinalityMismatch {
        token_count: usize,
        payout_count: usize,
    },
    #[error("market resolution token id at index {index} is not a canonical decimal U256")]
    InvalidTokenId { index: usize },
    #[error("market resolution token id at index {duplicate_index} duplicates index {first_index}")]
    DuplicateTokenId {
        first_index: usize,
        duplicate_index: usize,
    },
    #[error("market resolution payout ratio at index {index} is invalid")]
    InvalidPayoutRatio { index: usize },
    #[error("market resolution payout ratios must sum to 1, got {total}")]
    PayoutTotalNotOne { total: Decimal },
    #[error("requested token is not present in the market resolution vector")]
    TokenNotPresent,
    #[error("market resolution fact hash mismatch: expected {expected}, got {actual}")]
    ResolutionFactHashMismatch {
        expected: ContentHash,
        actual: ContentHash,
    },
    #[error(transparent)]
    CanonicalDigest(#[from] CanonicalDigestError),
}

#[cfg(test)]
mod tests {
    use rust_decimal_macros::dec;

    use super::{MarketResolutionContractError, MarketResolutionFactInput, MarketResolutionRow};
    use crate::{
        clickhouse::ChPayoutRatio,
        enums::quant::RecommendationResolutionKind,
        types::{ContentHash, EvmBlockHash, EvmTransactionHash, MarketId, PayoutRatio, TokenId},
    };

    fn row(token_ids: [&str; 2], payout_ratios: [PayoutRatio; 2]) -> MarketResolutionRow {
        MarketResolutionRow::seal(MarketResolutionFactInput {
            market_id: MarketId::new("0xresolution"),
            token_ids: token_ids.map(TokenId::new),
            payout_ratios,
            resolved_at: 100,
            observed_at: 110,
            source_block_number: 42,
            source_block_hash: EvmBlockHash::parse(format!("0x{}", "11".repeat(32)))
                .expect("block hash"),
            source_transaction_hash: EvmTransactionHash::parse(format!("0x{}", "22".repeat(32)))
                .expect("transaction hash"),
            source_log_index: 3,
            source_checkpoint_hash: ContentHash::from_bytes([0x33; 32]),
        })
        .expect("sealed resolution fact")
    }

    #[test]
    fn winner_take_all_and_split_vectors_preserve_token_payouts() {
        let winner = row(["101", "202"], [PayoutRatio::ONE, PayoutRatio::ZERO]);
        winner.validate().expect("valid winner-take-all vector");
        assert_eq!(
            winner.resolution_kind().expect("resolution kind"),
            RecommendationResolutionKind::WinnerTakeAll
        );
        assert_eq!(
            winner
                .payout_for(&TokenId::new("101"))
                .expect("winner payout"),
            PayoutRatio::ONE
        );

        let half = PayoutRatio::try_new(dec!(0.5)).expect("half payout");
        let split = row(["101", "202"], [half, half]);
        split.validate().expect("valid split vector");
        assert_eq!(
            split.resolution_kind().expect("resolution kind"),
            RecommendationResolutionKind::SplitPayout
        );
        assert_eq!(
            split
                .payout_for(&TokenId::new("202"))
                .expect("split payout"),
            half
        );
    }

    #[test]
    fn vector_contract_rejects_cardinality_total_and_token_corruption() {
        let half = PayoutRatio::try_new(dec!(0.5)).expect("half payout");

        let mut mismatched = row(["101", "202"], [half, half]);
        let _ = mismatched.payout_ratios.pop();
        assert_eq!(
            mismatched.validate(),
            Err(MarketResolutionContractError::CardinalityMismatch {
                token_count: 2,
                payout_count: 1,
            })
        );

        let one_quarter = PayoutRatio::try_new(dec!(0.25)).expect("quarter payout");
        let mut wrong_total = row(["101", "202"], [half, half]);
        wrong_total.payout_ratios[0] = ChPayoutRatio::from(one_quarter);
        assert_eq!(
            wrong_total.validate(),
            Err(MarketResolutionContractError::PayoutTotalNotOne { total: dec!(0.75) })
        );

        let mut invalid_token = row(["101", "202"], [half, half]);
        invalid_token.token_ids[0] = TokenId::new("not-a-decimal-u256");
        assert_eq!(
            invalid_token.validate(),
            Err(MarketResolutionContractError::InvalidTokenId { index: 0 })
        );

        let mut duplicate = row(["101", "202"], [half, half]);
        duplicate.token_ids[1] = duplicate.token_ids[0].clone();
        assert_eq!(
            duplicate.validate(),
            Err(MarketResolutionContractError::DuplicateTokenId {
                first_index: 0,
                duplicate_index: 1,
            })
        );
    }

    #[test]
    fn finalized_source_lineage_and_fact_hash_are_tamper_evident() {
        let sealed = MarketResolutionRow::seal(MarketResolutionFactInput {
            market_id: MarketId::new("0xresolution"),
            token_ids: [TokenId::new("101"), TokenId::new("202")],
            payout_ratios: [PayoutRatio::ONE, PayoutRatio::ZERO],
            resolved_at: 100,
            observed_at: 110,
            source_block_number: 42,
            source_block_hash: EvmBlockHash::parse(format!("0x{}", "11".repeat(32)))
                .expect("block hash"),
            source_transaction_hash: EvmTransactionHash::parse(format!("0x{}", "22".repeat(32)))
                .expect("transaction hash"),
            source_log_index: 3,
            source_checkpoint_hash: ContentHash::from_bytes([0x33; 32]),
        })
        .expect("sealed resolution fact");
        sealed.validate().expect("valid sealed fact");

        let mut tampered = sealed;
        tampered.source_log_index += 1;
        assert!(matches!(
            tampered.validate(),
            Err(MarketResolutionContractError::ResolutionFactHashMismatch { .. })
        ));
    }
}
