//! Canonical temporal grid shared by scenario artifacts and capital constraints.

use quant_pivot_error::hashing::CanonicalDigestError;
use quant_pivot_models::{
    domain::quant::DiscountCurvePoint, hashing::CanonicalDigest,
    runtime_config::CapitalTimeBucketLimit, types::ContentHash,
};
use serde::Serialize;
use thiserror::Error;

const CONTRACT_DIGEST_DOMAIN: &str = "quant-pivot/capital-time-bucket-contract";

/// Invalid ordered capital-time grid.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum CapitalTimeBucketContractError {
    #[error("capital-time bucket contract must contain at least one boundary")]
    Empty,
    #[error("capital-time bucket boundary {index} must be positive")]
    ZeroBoundary { index: usize },
    #[error(
        "capital-time bucket boundary {index} must be strictly greater than {previous}, got {current}"
    )]
    NotIncreasing {
        index: usize,
        previous: u64,
        current: u64,
    },
}

/// Ordered elapsed-time boundaries shared by scenario discounting and capital occupancy.
///
/// USD caps are intentionally absent. They are decision-policy constraints and can
/// change without refitting the statistical scenario model, while any boundary
/// change alters the temporal grid and therefore requires an atomically compatible
/// scenario-model binding.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CapitalTimeBucketContract {
    end_secs: Vec<u64>,
}

impl CapitalTimeBucketContract {
    pub fn try_new(
        end_secs: impl IntoIterator<Item = u64>,
    ) -> Result<Self, CapitalTimeBucketContractError> {
        let end_secs = end_secs.into_iter().collect::<Vec<_>>();
        if end_secs.is_empty() {
            return Err(CapitalTimeBucketContractError::Empty);
        }
        for (index, current) in end_secs.iter().copied().enumerate() {
            if current == 0 {
                return Err(CapitalTimeBucketContractError::ZeroBoundary { index });
            }
            if let Some(previous) = index.checked_sub(1).map(|previous| end_secs[previous])
                && current <= previous
            {
                return Err(CapitalTimeBucketContractError::NotIncreasing {
                    index,
                    previous,
                    current,
                });
            }
        }
        Ok(Self { end_secs })
    }

    #[must_use]
    pub fn end_secs(&self) -> &[u64] {
        &self.end_secs
    }

    pub fn content_hash(&self) -> Result<ContentHash, CanonicalDigestError> {
        CanonicalDigest::content_hash_typed(CONTRACT_DIGEST_DOMAIN, 1, self)
    }
}

impl TryFrom<&[CapitalTimeBucketLimit]> for CapitalTimeBucketContract {
    type Error = CapitalTimeBucketContractError;

    fn try_from(buckets: &[CapitalTimeBucketLimit]) -> Result<Self, Self::Error> {
        Self::try_new(buckets.iter().map(|bucket| bucket.end_secs))
    }
}

impl TryFrom<&[DiscountCurvePoint]> for CapitalTimeBucketContract {
    type Error = CapitalTimeBucketContractError;

    fn try_from(curve: &[DiscountCurvePoint]) -> Result<Self, Self::Error> {
        Self::try_new(curve.iter().map(|point| point.end_secs))
    }
}

#[cfg(test)]
mod tests {
    use quant_pivot_models::runtime_config::{CapitalTimeBucketLimit, DecimalValue};
    use rust_decimal_macros::dec;

    use super::{CapitalTimeBucketContract, CapitalTimeBucketContractError};

    #[test]
    fn caps_do_not_rekey() {
        let first = [
            CapitalTimeBucketLimit {
                end_secs: 3_600,
                max_capital_usd: DecimalValue::new(dec!(100)),
            },
            CapitalTimeBucketLimit {
                end_secs: 86_400,
                max_capital_usd: DecimalValue::new(dec!(200)),
            },
        ];
        let mut second = first.clone();
        second[0].max_capital_usd = DecimalValue::new(dec!(10));

        let first = CapitalTimeBucketContract::try_from(first.as_slice()).expect("first grid");
        let second = CapitalTimeBucketContract::try_from(second.as_slice()).expect("second grid");

        assert_eq!(first, second);
        assert_eq!(
            first.content_hash().expect("first hash"),
            second.content_hash().expect("second hash")
        );
    }

    #[test]
    fn boundaries_rekey_contract() {
        let first = CapitalTimeBucketContract::try_new([3_600, 86_400]).expect("first grid");
        let second = CapitalTimeBucketContract::try_new([7_200, 86_400]).expect("second grid");

        assert_ne!(first, second);
        assert_ne!(
            first.content_hash().expect("first hash"),
            second.content_hash().expect("second hash")
        );
    }

    #[test]
    fn unordered_grid_rejected() {
        let error = CapitalTimeBucketContract::try_new([3_600, 3_600])
            .expect_err("duplicate boundary must fail");

        assert_eq!(
            error,
            CapitalTimeBucketContractError::NotIncreasing {
                index: 1,
                previous: 3_600,
                current: 3_600,
            }
        );
    }
}
