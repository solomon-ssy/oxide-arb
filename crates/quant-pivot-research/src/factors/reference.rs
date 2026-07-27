//! Frozen training-reference distributions for small-cross-section factors.
//!
//! The reference CDF is fitted from raw factor values in a training partition,
//! serialized into the weighted-model artifact, and applied unchanged by
//! serving. It is deliberately independent of online factor history: an online
//! write can never change the transform a published model applies.

use std::collections::BTreeMap;

use quant_pivot_error::{QuantError, QuantResult, research::ResearchError};
use quant_pivot_models::types::Probability;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::{
    factors::{FactorName, normalize::interpolated_percentile},
    precision::RESEARCH_DECIMAL_SCALE,
};

/// One factor's empirical training-reference CDF.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "FrozenReferenceCdfDocument")]
pub struct FrozenReferenceCdf {
    /// Governed factor name this reference transforms.
    factor: FactorName,
    /// Every observed raw training value, sorted ascending. Duplicates are
    /// retained because they are probability mass in the empirical CDF.
    sorted_values: Vec<Decimal>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FrozenReferenceCdfDocument {
    factor: FactorName,
    sorted_values: Vec<Decimal>,
}

impl FrozenReferenceCdf {
    /// Construct and validate a deterministic empirical CDF.
    ///
    /// # Errors
    ///
    /// Returns an invalid-artifact error when fewer than two observations are
    /// present or the reference has no dispersion.
    pub fn fit(factor: FactorName, mut values: Vec<Decimal>) -> QuantResult<Self> {
        values.sort();
        let reference = Self {
            factor,
            sorted_values: values,
        };
        reference.validate()?;
        Ok(reference)
    }

    /// Validate ordering, sample count, and dispersion.
    ///
    /// # Errors
    ///
    /// Returns an invalid-artifact error for malformed reference bytes.
    pub fn validate(&self) -> QuantResult<()> {
        if self.sorted_values.len() < 2 {
            return Err(ResearchError::InvalidModelArtifact {
                detail: format!(
                    "frozen reference CDF `{}` requires at least two observed raw values",
                    self.factor
                ),
            }
            .into());
        }
        if self.sorted_values.windows(2).any(|pair| pair[0] > pair[1]) {
            return Err(ResearchError::InvalidModelArtifact {
                detail: format!(
                    "frozen reference CDF `{}` values are not sorted ascending",
                    self.factor
                ),
            }
            .into());
        }
        if self.sorted_values.first() == self.sorted_values.last() {
            return Err(ResearchError::InvalidModelArtifact {
                detail: format!("frozen reference CDF `{}` has zero variance", self.factor),
            }
            .into());
        }
        Ok(())
    }

    /// Number of observations retained by the frozen reference.
    #[must_use]
    pub const fn sample_count(&self) -> usize {
        self.sorted_values.len()
    }

    /// Map a raw value onto the piecewise-linear reference CDF using average
    /// ranks for ties and exact `Decimal` interpolation for unseen values.
    pub fn percentile(&self, raw: Decimal) -> QuantResult<Probability> {
        Ok(Probability::new(
            interpolated_percentile(&self.sorted_values, raw)?.round_dp(RESEARCH_DECIMAL_SCALE),
        ))
    }
}

impl TryFrom<FrozenReferenceCdfDocument> for FrozenReferenceCdf {
    type Error = QuantError;

    fn try_from(document: FrozenReferenceCdfDocument) -> Result<Self, Self::Error> {
        let reference = Self {
            factor: document.factor,
            sorted_values: document.sorted_values,
        };
        reference.validate()?;
        Ok(reference)
    }
}

/// Frozen per-factor CDFs carried by one weighted-model artifact.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "FrozenReferenceQuantilesDocument")]
pub struct FrozenReferenceQuantiles {
    /// Stable factor-name-ordered CDFs.
    references: Vec<FrozenReferenceCdf>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FrozenReferenceQuantilesDocument {
    references: Vec<FrozenReferenceCdf>,
}

impl FrozenReferenceQuantiles {
    /// No reference distributions (valid only with the `Indeterminate`
    /// small-cross-section policy).
    #[must_use]
    pub const fn empty() -> Self {
        Self {
            references: Vec::new(),
        }
    }

    /// Build a factor-name-ordered collection and reject duplicates.
    ///
    /// # Errors
    ///
    /// Returns an invalid-artifact error for a duplicate or malformed CDF.
    pub fn new(mut references: Vec<FrozenReferenceCdf>) -> QuantResult<Self> {
        references.sort_by(|left, right| left.factor.cmp(&right.factor));
        let frozen = Self { references };
        frozen.validate()?;
        Ok(frozen)
    }

    /// Validate every CDF and collection-level uniqueness.
    ///
    /// # Errors
    ///
    /// Returns an invalid-artifact error for malformed artifact content.
    pub fn validate(&self) -> QuantResult<()> {
        if self
            .references
            .windows(2)
            .any(|pair| pair[0].factor >= pair[1].factor)
        {
            return Err(ResearchError::InvalidModelArtifact {
                detail: "frozen reference CDFs must be unique and ordered by factor name"
                    .to_owned(),
            }
            .into());
        }
        for reference in &self.references {
            reference.validate()?;
        }
        Ok(())
    }

    /// Find a factor's frozen training reference.
    #[must_use]
    pub fn get(&self, factor: &FactorName) -> Option<&FrozenReferenceCdf> {
        self.references
            .binary_search_by(|reference| reference.factor.cmp(factor))
            .ok()
            .map(|index| &self.references[index])
    }

    /// Build an owned index for callers that need repeated joins.
    #[must_use]
    pub fn index(&self) -> BTreeMap<FactorName, FrozenReferenceCdf> {
        self.references
            .iter()
            .cloned()
            .map(|reference| (reference.factor.clone(), reference))
            .collect()
    }

    /// Whether the artifact carries no reference distribution.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.references.is_empty()
    }
}

impl TryFrom<FrozenReferenceQuantilesDocument> for FrozenReferenceQuantiles {
    type Error = QuantError;

    fn try_from(document: FrozenReferenceQuantilesDocument) -> Result<Self, Self::Error> {
        let references = Self {
            references: document.references,
        };
        references.validate()?;
        Ok(references)
    }
}

#[cfg(test)]
mod tests {
    use rust_decimal_macros::dec;

    use super::{FrozenReferenceCdf, FrozenReferenceQuantiles};
    use crate::factors::FactorName;

    #[test]
    fn cdf_preserves_tie_values() {
        let reference = FrozenReferenceCdf::fit(
            FactorName::new("momentum"),
            vec![dec!(3), dec!(1), dec!(2), dec!(2)],
        )
        .expect("reference");

        assert_eq!(
            reference.sorted_values,
            vec![dec!(1), dec!(2), dec!(2), dec!(3)]
        );
        assert_eq!(
            reference.percentile(dec!(0)).expect("below").inner(),
            dec!(0)
        );
        assert_eq!(
            reference.percentile(dec!(2)).expect("tie").inner(),
            dec!(0.5)
        );
        assert_eq!(
            reference
                .percentile(dec!(2.5))
                .expect("interpolated")
                .inner(),
            dec!(0.75)
        );
        assert_eq!(
            reference.percentile(dec!(4)).expect("above").inner(),
            dec!(1)
        );
    }

    #[test]
    fn malformed_duplicate_references_rejects() {
        assert!(FrozenReferenceCdf::fit(FactorName::new("flat"), vec![dec!(1), dec!(1)]).is_err());
        let one = FrozenReferenceCdf::fit(FactorName::new("momentum"), vec![dec!(1), dec!(2)])
            .expect("one");
        assert!(FrozenReferenceQuantiles::new(vec![one.clone(), one]).is_err());
    }
}
