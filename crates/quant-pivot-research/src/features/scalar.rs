//! Shared `FeatureValue` → numeric scalar projection (11.2.2 remediation R2).
//!
//! The training-matrix assembler ([`crate::training::matrix`]) and the
//! classical-model runtime ([`crate::model::classical_runtime`]) both need to
//! reduce a [`FeatureValue`] to a plain number — training to build a scaled
//! `f64` matrix column, serving to standardize one inference row — and had
//! each hand-rolled an identical match over the value's variants. Centralized
//! here so the set of "numeric" `FeatureValue` variants can never silently
//! drift between the two call sites.

use rust_decimal::{Decimal, prelude::ToPrimitive};

use crate::features::value::FeatureValue;

/// Project a [`FeatureValue`] to a scalar decimal.
///
/// `None` when the value is missing or not a numeric/boolean scalar
/// (categoricals are intentionally rejected — they must be one-hot encoded
/// upstream, never fed as ordinals).
#[must_use]
pub fn feature_scalar(value: &FeatureValue) -> Option<Decimal> {
    match value {
        FeatureValue::Decimal(d) | FeatureValue::Bps(d) => Some(*d),
        FeatureValue::Probability(p) => Some(p.inner()),
        FeatureValue::Usd(u) => Some(u.inner()),
        FeatureValue::Count(c) => Some(Decimal::from(*c)),
        FeatureValue::Bool(b) => Some(if *b { Decimal::ONE } else { Decimal::ZERO }),
        FeatureValue::Category(_) | FeatureValue::Missing(_) => None,
    }
}

/// Cast a decimal to a finite `f64`, rejecting non-finite and unrepresentable values.
#[must_use]
pub fn finite_f64(decimal: Decimal) -> Option<f64> {
    decimal
        .to_f64()
        .and_then(|value| value.is_finite().then_some(value))
}

#[cfg(test)]
mod tests {
    use super::{feature_scalar, finite_f64};
    use crate::features::value::FeatureValue;
    use quant_pivot_models::types::{Probability, Usd};
    use rust_decimal_macros::dec;

    #[test]
    fn numeric_variants_project_to_decimal() {
        assert_eq!(
            feature_scalar(&FeatureValue::Decimal(dec!(1.5))),
            Some(dec!(1.5))
        );
        assert_eq!(feature_scalar(&FeatureValue::Bps(dec!(50))), Some(dec!(50)));
        assert_eq!(
            feature_scalar(&FeatureValue::Probability(Probability::new(dec!(0.5)))),
            Some(dec!(0.5))
        );
        assert_eq!(
            feature_scalar(&FeatureValue::Usd(Usd::new(dec!(100)))),
            Some(dec!(100))
        );
        assert_eq!(feature_scalar(&FeatureValue::Count(3)), Some(dec!(3)));
        assert_eq!(feature_scalar(&FeatureValue::Bool(true)), Some(dec!(1)));
        assert_eq!(feature_scalar(&FeatureValue::Bool(false)), Some(dec!(0)));
    }

    #[test]
    fn non_numeric_variants_reject() {
        assert_eq!(
            feature_scalar(&FeatureValue::Missing(
                crate::features::value::NullReason::NotApplicable
            )),
            None
        );
    }

    #[test]
    fn finite_f64_converts_ordinary_values() {
        assert_eq!(finite_f64(dec!(1.25)), Some(1.25));
        assert_eq!(finite_f64(dec!(0)), Some(0.0));
        assert_eq!(finite_f64(dec!(-3.5)), Some(-3.5));
    }
}
