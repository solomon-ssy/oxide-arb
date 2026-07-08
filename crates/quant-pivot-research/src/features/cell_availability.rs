//! Shared train/serve availability-cell semantics (11.2.2 remediation R2).
//!
//! [`training::matrix`](crate::training::matrix) and the online/backtest
//! classical-inference matrix assembly
//! (`quant-pivot-core::projection::inference_batch::build_feature_matrix`)
//! must classify a feature's structural state — genuinely present, never
//! applicable to this row, or applicable-but-missing right now — **using the
//! identical rule**, or a classical model's learned availability signal is
//! trained on real distinctions and served a fabricated constant. This
//! module is the one shared primitive both call sites use, so that mismatch
//! is structurally impossible rather than merely undocumented.
//!
//! Not to be confused with [`crate::features::FeatureAvailabilityOracle`],
//! which answers a different question at a different pipeline stage
//! (pre-selection: "can this candidate market supply this feature at all?",
//! from [`quant_pivot_models::domain::MarketCandidate`] facts alone). This
//! module answers "what state is this key in an already-computed
//! [`FeatureVector`]?", consumed after computation, at matrix/row-assembly
//! time.

use crate::features::value::{FeatureName, FeatureValue, FeatureVector, NullReason};

/// Availability-column value: the cell was a genuine observation.
pub const AVAILABILITY_PRESENT: f64 = 1.0;
/// Availability-column value: the feature structurally does not apply to this row.
pub const AVAILABILITY_NOT_APPLICABLE: f64 = 0.0;
/// Availability-column value: the feature applies but is missing this instance.
pub const AVAILABILITY_MISSING: f64 = -1.0;
/// Suffix appended to a feature's name for its availability companion column.
pub const AVAILABILITY_SUFFIX: &str = ".__available";

/// The three structurally distinct states one [`FeatureVector`] key can be in.
///
/// [`Self::NotApplicable`] and [`Self::MissingApplicable`] must never
/// collapse into the same fabricated placeholder value: a model must be able
/// to tell "this market's vertical never has this signal" apart from "this
/// market's vertical had a data outage right now".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CellAvailability {
    /// A genuine observed, present value.
    Present,
    /// The feature structurally never applies to this row: the vector's
    /// domain slice is entirely absent (a `domain.*` column on a market
    /// whose category maps to no vertical), or the value is explicitly a
    /// [`NullReason::NotApplicable`] [`FeatureValue::Missing`].
    NotApplicable,
    /// The feature applies in principle but this instance has a data or
    /// quality gap (any other [`NullReason`]).
    MissingApplicable,
}

impl CellAvailability {
    /// This state's numeric encoding for an availability companion column.
    #[must_use]
    pub const fn as_f64(self) -> f64 {
        match self {
            Self::Present => AVAILABILITY_PRESENT,
            Self::NotApplicable => AVAILABILITY_NOT_APPLICABLE,
            Self::MissingApplicable => AVAILABILITY_MISSING,
        }
    }
}

/// Classify `feature`'s state in `vector` — the single rule both the
/// training-matrix assembler and the online/backtest inference-row assembler
/// must apply identically.
///
/// The key being entirely absent (`None`) is folded into the same
/// [`CellAvailability::NotApplicable`] arm as an explicit
/// [`NullReason::NotApplicable`]: for a `domain.*` name this is the
/// `vector.domain: None` case (structurally distinct from a present-but-missing
/// domain value, never a data gap); for a generic name this should not occur
/// under the fixed-width schema, and failing soft to "not applicable" is the
/// conservative choice (never a fabricated data gap).
#[must_use]
pub fn availability_of(vector: &FeatureVector, feature: &FeatureName) -> CellAvailability {
    match vector.value(feature) {
        Some(FeatureValue::Missing(NullReason::NotApplicable)) | None => {
            CellAvailability::NotApplicable
        }
        Some(FeatureValue::Missing(_)) => CellAvailability::MissingApplicable,
        Some(_) => CellAvailability::Present,
    }
}

/// The availability companion [`FeatureName`] for a feature column.
#[must_use]
pub fn availability_column_name(feature: &FeatureName) -> FeatureName {
    FeatureName::new(format!("{}{AVAILABILITY_SUFFIX}", feature.as_str()))
}

/// The base feature name shadowed by an availability companion column, if any.
///
/// The inverse of [`availability_column_name`]. Centralizing the suffix
/// convention here means the training-side column-name synthesis and the
/// serving-side column-name recognition can never drift out of sync.
#[must_use]
pub fn base_name_if_availability_column(name: &FeatureName) -> Option<FeatureName> {
    name.as_str()
        .strip_suffix(AVAILABILITY_SUFFIX)
        .map(FeatureName::new)
}

#[cfg(test)]
mod tests {
    use super::{
        AVAILABILITY_MISSING, AVAILABILITY_NOT_APPLICABLE, AVAILABILITY_PRESENT, CellAvailability,
        availability_column_name, availability_of, base_name_if_availability_column,
    };
    use crate::features::value::{
        DomainFeatureSlice, FeatureName, FeatureValue, FeatureVector, NullReason,
    };
    use chrono::{TimeZone, Utc};
    use quant_pivot_models::{
        enums::{domain::DomainFamily, quant::DataQualityStatus},
        types::{MarketId, SchemaVersion, TokenId},
    };
    use rust_decimal_macros::dec;
    use std::collections::BTreeMap;

    fn base_vector(
        generic: BTreeMap<FeatureName, FeatureValue>,
        domain: Option<DomainFeatureSlice>,
    ) -> FeatureVector {
        FeatureVector {
            market_id: MarketId::new("m1"),
            token_id: Some(TokenId::new("t1")),
            as_of: Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap(),
            generic_schema_version: SchemaVersion::FIRST,
            generic,
            domain,
            substitutions: Vec::new(),
            data_quality: DataQualityStatus::Fresh,
            staleness_ms: 0,
            source_refs: Vec::new(),
        }
    }

    #[test]
    fn present_value_is_present() {
        let mut generic = BTreeMap::new();
        generic.insert(
            FeatureName::from_static("book.mid"),
            FeatureValue::Decimal(dec!(0.5)),
        );
        let vector = base_vector(generic, None);
        assert_eq!(
            availability_of(&vector, &FeatureName::from_static("book.mid")),
            CellAvailability::Present
        );
    }

    #[test]
    fn absent_domain_slice_is_not_applicable() {
        let vector = base_vector(BTreeMap::new(), None);
        assert_eq!(
            availability_of(
                &vector,
                &FeatureName::from_static("domain.crypto.distance_to_strike")
            ),
            CellAvailability::NotApplicable
        );
    }

    #[test]
    fn explicit_not_applicable_is_not_applicable() {
        let mut generic = BTreeMap::new();
        generic.insert(
            FeatureName::from_static("spread"),
            FeatureValue::Missing(NullReason::NotApplicable),
        );
        let vector = base_vector(generic, None);
        assert_eq!(
            availability_of(&vector, &FeatureName::from_static("spread")),
            CellAvailability::NotApplicable
        );
    }

    #[test]
    fn other_missing_reason_is_missing_applicable() {
        let mut values = BTreeMap::new();
        values.insert(
            FeatureName::from_static("domain.crypto.distance_to_strike"),
            FeatureValue::Missing(NullReason::DomainSourceUnavailable),
        );
        let vector = base_vector(
            BTreeMap::new(),
            Some(DomainFeatureSlice {
                family: DomainFamily::Crypto,
                schema_version: SchemaVersion::FIRST,
                values,
            }),
        );
        assert_eq!(
            availability_of(
                &vector,
                &FeatureName::from_static("domain.crypto.distance_to_strike")
            ),
            CellAvailability::MissingApplicable
        );
    }

    /// Exact-value assertion helper: these constants are fixed sentinel
    /// literals (never the result of arithmetic), so byte-exact comparison
    /// via bit representation is the correct check — not an epsilon
    /// tolerance, which would be the wrong tool for a sentinel encoding.
    fn same_bits(a: f64, b: f64) -> bool {
        a.to_bits() == b.to_bits()
    }

    #[test]
    fn encodings_are_distinct() {
        assert!(!same_bits(
            AVAILABILITY_PRESENT,
            AVAILABILITY_NOT_APPLICABLE
        ));
        assert!(!same_bits(
            AVAILABILITY_NOT_APPLICABLE,
            AVAILABILITY_MISSING
        ));
        assert!(!same_bits(AVAILABILITY_PRESENT, AVAILABILITY_MISSING));
        assert!(same_bits(
            CellAvailability::Present.as_f64(),
            AVAILABILITY_PRESENT
        ));
        assert!(same_bits(
            CellAvailability::NotApplicable.as_f64(),
            AVAILABILITY_NOT_APPLICABLE
        ));
        assert!(same_bits(
            CellAvailability::MissingApplicable.as_f64(),
            AVAILABILITY_MISSING
        ));
    }

    #[test]
    fn column_name_roundtrips() {
        let feature = FeatureName::from_static("domain.crypto.distance_to_strike");
        let column = availability_column_name(&feature);
        assert_eq!(
            column.as_str(),
            "domain.crypto.distance_to_strike.__available"
        );
        assert_eq!(base_name_if_availability_column(&column), Some(feature));
        assert_eq!(
            base_name_if_availability_column(&FeatureName::from_static("book.mid")),
            None
        );
    }
}
