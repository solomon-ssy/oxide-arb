//! Market-metadata feature builder: resolution timing, age, outcome structure,
//! and lifecycle flags derived from Gamma catalog context.

use chrono::{DateTime, Utc};
use quant_pivot_models::{enums::market::MarketStatus, runtime_config::FeatureFamily};

use crate::features::{
    builder::{FeatureComputeCtx, FeatureGroupBuilder, RawFeature},
    resolved::ResolvedMarketContext,
    value::{EvidenceSourceKind, EvidenceSourceRef, FeatureName, FeatureValue, NullReason},
};

/// Builds the [`FeatureFamily::MarketMetadata`] features.
pub struct MarketMetadataFeatureBuilder;

impl FeatureGroupBuilder for MarketMetadataFeatureBuilder {
    fn family(&self) -> FeatureFamily {
        FeatureFamily::MarketMetadata
    }

    fn compute(&self, ctx: &FeatureComputeCtx<'_>) -> Vec<RawFeature> {
        // No metadata ⇒ produce nothing; critical metadata specs then reject the
        // market via the null policy (never silently defaulted).
        let Some(market) = ctx.market else {
            return Vec::new();
        };
        let evidence = EvidenceSourceRef {
            source_kind: EvidenceSourceKind::GammaMetadata,
            reference: market.market_id.as_str().to_owned(),
            observed_at: market.observed_at,
        };

        vec![
            // Category is carried faithfully as the enum; it comes from the frozen
            // selection snapshot (identical online and offline), so the build is
            // parity-stable. Numeric encoding is a downstream normalization
            // concern (3.3) — never an ordinal value fed to a model.
            RawFeature::present(
                FeatureName::from_static("market.category"),
                FeatureValue::Category(ctx.category),
                evidence.clone(),
            ),
            time_to_resolution(ctx, market, &evidence),
            RawFeature::present(
                FeatureName::from_static("market.event_age_secs"),
                FeatureValue::Count(secs_since(market.created_at, ctx)),
                evidence.clone(),
            ),
            RawFeature::present(
                FeatureName::from_static("market.outcome_count"),
                FeatureValue::Count(u64::from(market.outcome_count)),
                evidence.clone(),
            ),
            RawFeature::present(
                FeatureName::from_static("market.neg_risk"),
                FeatureValue::Bool(market.neg_risk),
                evidence.clone(),
            ),
            RawFeature::present(
                FeatureName::from_static("market.is_active"),
                FeatureValue::Bool(market.status == MarketStatus::Active),
                evidence,
            ),
        ]
    }
}

/// Time to resolution in whole seconds, or missing when no end date is known.
fn time_to_resolution(
    ctx: &FeatureComputeCtx<'_>,
    market: &ResolvedMarketContext,
    evidence: &EvidenceSourceRef,
) -> RawFeature {
    let name = FeatureName::from_static("market.time_to_resolution_secs");
    match market.end_date {
        Some(end_date) => {
            let secs = (end_date - ctx.as_of).num_seconds();
            let value = FeatureValue::Count(u64::try_from(secs).unwrap_or(0));
            RawFeature::present(name, value, evidence.clone())
        }
        None => RawFeature::missing(name, NullReason::SourceUnavailable),
    }
}

/// Whole seconds elapsed since `since` at the decision time (clamped at zero).
fn secs_since(since: DateTime<Utc>, ctx: &FeatureComputeCtx<'_>) -> u64 {
    let secs = (ctx.as_of - since).num_seconds();
    u64::try_from(secs).unwrap_or(0)
}
