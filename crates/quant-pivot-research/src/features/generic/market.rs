//! Market-metadata feature builder: resolution timing, age,
//! and lifecycle flags derived from Gamma catalog context.

use quant_pivot_error::QuantResult;
use quant_pivot_models::{enums::market::MarketStatus, runtime_config::FeatureFamily};

use crate::features::{
    builder::{FeatureComputeCtx, FeatureGroupBuilder, RawFeature},
    names::{
        market,
        market::{EVENT_AGE_SECS, TIME_TO_RESOLUTION_SECS},
    },
    resolved::ResolvedMarketContext,
    value::{EvidenceSourceKind, EvidenceSourceRef, FeatureValue, NullReason},
};

/// Builds the [`FeatureFamily::MarketMetadata`] features.
pub struct MarketMetadataFeatureBuilder;

impl FeatureGroupBuilder for MarketMetadataFeatureBuilder {
    fn family(&self) -> FeatureFamily {
        FeatureFamily::MarketMetadata
    }

    fn compute(&self, ctx: &FeatureComputeCtx<'_>) -> QuantResult<Vec<RawFeature>> {
        // No metadata ⇒ produce nothing; model-required metadata inputs then reject the
        // market via the null policy (never silently defaulted).
        let Some(market) = ctx.market else {
            return Ok(Vec::new());
        };
        let evidence = EvidenceSourceRef {
            source_kind: EvidenceSourceKind::GammaMetadata,
            reference: market.market_id.to_string(),
            effective_at: market.effective_at,
            available_at: Some(market.available_at),
        };

        Ok(vec![
            // Category is carried faithfully as the enum; it comes from the frozen
            // selection snapshot (identical online and offline), so the build is
            // parity-stable. Numeric encoding is a downstream normalization
            // concern — never an ordinal value fed to a model.
            RawFeature::present(
                market::CATEGORY,
                FeatureValue::Category(ctx.category),
                evidence.clone(),
            ),
            time_to_resolution(ctx, market, &evidence),
            event_age(ctx, market, &evidence),
            RawFeature::present(
                market::NEG_RISK,
                FeatureValue::Bool(market.neg_risk),
                evidence.clone(),
            ),
            RawFeature::present(
                market::IS_ACTIVE,
                FeatureValue::Bool(market.status == MarketStatus::Active),
                evidence,
            ),
        ])
    }
}

/// Time to resolution in whole seconds, or missing when no end date is known.
fn time_to_resolution(
    ctx: &FeatureComputeCtx<'_>,
    market: &ResolvedMarketContext,
    evidence: &EvidenceSourceRef,
) -> RawFeature {
    market.end_date.map_or_else(
        || RawFeature::missing(TIME_TO_RESOLUTION_SECS, NullReason::SourceUnavailable),
        |end_date| {
            let secs = (end_date - ctx.decision_at).num_seconds();
            u64::try_from(secs).map_or_else(
                |_| RawFeature::missing(TIME_TO_RESOLUTION_SECS, NullReason::OutOfValidRange),
                |secs| {
                    RawFeature::present(
                        TIME_TO_RESOLUTION_SECS,
                        FeatureValue::Count(secs),
                        evidence.clone(),
                    )
                },
            )
        },
    )
}

fn event_age(
    ctx: &FeatureComputeCtx<'_>,
    market: &ResolvedMarketContext,
    evidence: &EvidenceSourceRef,
) -> RawFeature {
    let Some(created_at) = market.created_at else {
        return RawFeature::missing(EVENT_AGE_SECS, NullReason::SourceUnavailable);
    };
    let seconds = (ctx.decision_at - created_at).num_seconds();
    u64::try_from(seconds).map_or_else(
        |_| RawFeature::missing(EVENT_AGE_SECS, NullReason::OutOfValidRange),
        |seconds| {
            RawFeature::present(
                EVENT_AGE_SECS,
                FeatureValue::Count(seconds),
                evidence.clone(),
            )
        },
    )
}
