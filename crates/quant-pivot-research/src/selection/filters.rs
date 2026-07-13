//! The 7-stage market-selection filter pipeline.
//!
//! Each [`SelectionFilter`] is a pure predicate over a [`MarketCandidateCtx`]:
//! the borrowed candidate facts plus the once-resolved [`SelectionThresholds`].
//! [`FilterChain`] runs the stages in a **fixed order** and short-circuits on
//! the first exclusion, so every excluded market carries exactly one deciding
//! [`ExclusionReason`] (the highest-priority rule that rejected it).
//!
//! # Fail-closed
//!
//! Missing facts never pass silently. Absent liquidity/volume, a one-sided book,
//! or a never-published book all reject the candidate rather than admit it on
//! incomplete evidence — money decisions must be made on data we actually have.

use std::collections::HashSet;
use std::str::FromStr;

use chrono::{DateTime, Utc};
use quant_pivot_error::{QuantError, QuantResult};
use quant_pivot_models::{
    domain::{MarketCandidate, MarketDataHealth},
    enums::{common::MarketCategory, market::MarketStatus},
    runtime_config::{DataQualityConfig, DecimalString, SelectionConfig},
    types::{Bps, SelectionExclusionSummary, Usd},
};
use rust_decimal::Decimal;

use crate::{
    features::{FeatureAvailabilityOracle, FeatureSchema},
    selection::{ExclusionReason, ModelFeatureRequirements},
};

/// Once-per-round resolution of every config threshold the filters compare
/// against, parsed out of the string/`DecimalString` config wire forms so the
/// hot filter loop performs no parsing or allocation.
#[derive(Debug, Clone)]
pub struct SelectionThresholds {
    /// Categories eligible for selection.
    pub enabled_categories: HashSet<MarketCategory>,
    /// Minimum Gamma liquidity in USD.
    pub min_liquidity_usd: Usd,
    /// Minimum trailing 24h volume in USD.
    pub min_volume_24h_usd: Usd,
    /// Maximum allowed top-of-book spread, in basis points.
    pub max_spread_bps: Decimal,
    /// Whether near-resolution markets may enter the selection.
    pub allow_near_resolution: bool,
    /// Minimum seconds until resolution.
    pub min_time_to_resolution_secs: i64,
    /// Maximum seconds until resolution.
    pub max_time_to_resolution_secs: i64,
    /// Maximum allowed published-book age, in milliseconds.
    pub max_book_age_ms: u64,
    /// Maximum allowed worst-case ingest pipeline lag (enqueue→flush), in ms.
    pub max_ingest_lag_ms: u64,
    /// Reject crossed books.
    pub reject_crossed_books: bool,
    /// Reject empty (one-sided) books.
    pub reject_empty_books: bool,
}

impl SelectionThresholds {
    /// Resolve thresholds from the frozen selection and data-quality configs.
    ///
    /// Fails with a config error when a decimal threshold string cannot be
    /// parsed — a malformed governance value must never be silently coerced.
    pub fn resolve(
        selection: &SelectionConfig,
        data_quality: &DataQualityConfig,
    ) -> QuantResult<Self> {
        Ok(Self {
            enabled_categories: selection.enabled_categories.iter().copied().collect(),
            min_liquidity_usd: parse_usd(&selection.min_liquidity_usd)?,
            min_volume_24h_usd: parse_usd(&selection.min_volume_24h_usd)?,
            max_spread_bps: Decimal::from(selection.max_spread_bps),
            allow_near_resolution: selection.allow_near_resolution,
            min_time_to_resolution_secs: i64::try_from(selection.min_time_to_resolution_secs)
                .map_err(|error| {
                    QuantError::config(format!(
                        "selection.min_time_to_resolution_secs is outside chrono range: {error}"
                    ))
                })?,
            max_time_to_resolution_secs: i64::try_from(selection.max_time_to_resolution_secs)
                .map_err(|error| {
                    QuantError::config(format!(
                        "selection.max_time_to_resolution_secs is outside chrono range: {error}"
                    ))
                })?,
            max_book_age_ms: data_quality.max_book_age_ms,
            max_ingest_lag_ms: data_quality.max_ingest_lag_ms,
            reject_crossed_books: data_quality.reject_crossed_books,
            reject_empty_books: data_quality.reject_empty_books,
        })
    }
}

/// Parse a `DecimalString` threshold into a `Usd`, failing closed on garbage.
fn parse_usd(value: &DecimalString) -> QuantResult<Usd> {
    let raw = value.value.trim();
    let decimal = Decimal::from_str(raw)
        .map_err(|err| QuantError::config(format!("invalid decimal threshold `{raw}`: {err}")))?;
    Ok(Usd::new(decimal))
}

/// Everything one filter needs to judge a single market, all borrowed.
pub struct MarketCandidateCtx<'a> {
    /// The frozen candidate facts under evaluation.
    pub candidate: &'a MarketCandidate,
    /// Pre-resolved config thresholds shared across the round.
    pub thresholds: &'a SelectionThresholds,
    /// Decision time for the round.
    pub decision_at: DateTime<Utc>,
    /// Feature availability the active model requires.
    pub model_requirements: &'a ModelFeatureRequirements,
    /// Governed feature schema backing the availability oracle.
    pub feature_schema: &'a FeatureSchema,
}

/// The verdict of a single filter for a single candidate.
pub enum FilterOutcome {
    /// The candidate survives this stage.
    Keep,
    /// The candidate is rejected with the deciding reason.
    Exclude(ExclusionReason),
}

/// A single, order-significant selection predicate.
pub trait SelectionFilter: Send + Sync {
    /// Stable name for diagnostics and metrics partitioning.
    fn name(&self) -> &'static str;

    /// Judge one candidate.
    fn evaluate(&self, ctx: &MarketCandidateCtx<'_>) -> FilterOutcome;
}

/// Stage 1 — keep only live (`Active`) markets.
pub struct MarketStatusFilter;

impl SelectionFilter for MarketStatusFilter {
    fn name(&self) -> &'static str {
        "market_status"
    }

    fn evaluate(&self, ctx: &MarketCandidateCtx<'_>) -> FilterOutcome {
        match ctx.candidate.status {
            MarketStatus::Active => FilterOutcome::Keep,
            MarketStatus::ManuallyBlocked => {
                FilterOutcome::Exclude(ExclusionReason::ManuallyBlocked)
            }
            MarketStatus::Discovered
            | MarketStatus::Filtered
            | MarketStatus::Paused
            | MarketStatus::Settled
            | MarketStatus::Delisted => FilterOutcome::Exclude(ExclusionReason::NotOpen),
        }
    }
}

/// Stage 2 — category gate.
pub struct CategoryFilter;

impl SelectionFilter for CategoryFilter {
    fn name(&self) -> &'static str {
        "category"
    }

    fn evaluate(&self, ctx: &MarketCandidateCtx<'_>) -> FilterOutcome {
        if ctx
            .thresholds
            .enabled_categories
            .contains(&ctx.candidate.category)
        {
            FilterOutcome::Keep
        } else {
            FilterOutcome::Exclude(ExclusionReason::CategoryDisabled)
        }
    }
}

/// Stage 3 — liquidity, volume, and top-of-book spread floors.
pub struct LiquidityFilter;

impl SelectionFilter for LiquidityFilter {
    fn name(&self) -> &'static str {
        "liquidity"
    }

    fn evaluate(&self, ctx: &MarketCandidateCtx<'_>) -> FilterOutcome {
        let candidate = ctx.candidate;
        let thresholds = ctx.thresholds;

        let Some(liquidity) = candidate.liquidity_usd else {
            return FilterOutcome::Exclude(ExclusionReason::InsufficientLiquidity);
        };
        if liquidity < thresholds.min_liquidity_usd {
            return FilterOutcome::Exclude(ExclusionReason::InsufficientLiquidity);
        }
        let Some(volume) = candidate.volume_24h_usd else {
            return FilterOutcome::Exclude(ExclusionReason::InsufficientLiquidity);
        };
        if volume < thresholds.min_volume_24h_usd {
            return FilterOutcome::Exclude(ExclusionReason::InsufficientLiquidity);
        }

        match spread_bps(candidate) {
            Some(spread) if spread <= thresholds.max_spread_bps => FilterOutcome::Keep,
            _ => FilterOutcome::Exclude(ExclusionReason::SpreadTooWide),
        }
    }
}

/// Top-of-book spread in basis points: `(ask - bid) / mid × 10_000`.
///
/// Returns `None` when the book is one-sided or the mid is non-positive, so the
/// caller fails closed on an unquotable market.
fn spread_bps(candidate: &MarketCandidate) -> Option<Decimal> {
    let bid = candidate.best_bid?.inner();
    let ask = candidate.best_ask?.inner();
    let mid = (bid + ask) / Decimal::from(2);
    Bps::relative(ask - bid, mid).map(Bps::inner)
}

/// Stage 4 — data-quality gate over book structure, freshness, and ingest lag.
///
/// Freshness is connection-aware, mirroring the live data-quality plane: on
/// Polymarket a quiet book is not resent, so an aged-but-valid book only counts
/// as stale when the market-data connection is unhealthy (we might be missing
/// updates). While the connection is healthy the book is the current venue
/// truth and is admitted; precise point-in-time freshness is then enforced
/// deterministically at feature materialization (`MaxBookAge`), keeping the
/// online/offline feature computation identical.
pub struct DataQualityFilter;

impl SelectionFilter for DataQualityFilter {
    fn name(&self) -> &'static str {
        "data_quality"
    }

    fn evaluate(&self, ctx: &MarketCandidateCtx<'_>) -> FilterOutcome {
        let candidate = ctx.candidate;
        let thresholds = ctx.thresholds;

        // Fail closed: a token that never published a book is unusable.
        let Some(age) = candidate.book_age_ms else {
            return FilterOutcome::Exclude(ExclusionReason::StaleBook);
        };
        // A healthy live connection can legitimately carry a quiet book. A
        // degraded connection or durable replay has no such live liveness
        // proof, so the configured age ceiling applies.
        if candidate.market_data_health != MarketDataHealth::Healthy
            && age > thresholds.max_book_age_ms
        {
            return FilterOutcome::Exclude(ExclusionReason::StaleBook);
        }
        if thresholds.reject_crossed_books && candidate.crossed == Some(true) {
            return FilterOutcome::Exclude(ExclusionReason::StaleBook);
        }
        if thresholds.reject_empty_books && candidate.empty != Some(false) {
            return FilterOutcome::Exclude(ExclusionReason::StaleBook);
        }
        match (candidate.market_data_health, candidate.ingest_lag_ms) {
            (MarketDataHealth::NotApplicable, Some(_))
            | (MarketDataHealth::Healthy | MarketDataHealth::Unhealthy, None) => {
                return FilterOutcome::Exclude(ExclusionReason::IngestLagExceeded);
            }
            (_, Some(lag)) if lag > thresholds.max_ingest_lag_ms => {
                return FilterOutcome::Exclude(ExclusionReason::IngestLagExceeded);
            }
            (MarketDataHealth::NotApplicable, None) | (_, Some(_)) => {}
        }
        FilterOutcome::Keep
    }
}

/// Stage 5 — resolution-window gate.
pub struct ResolutionAmbiguityFilter;

impl SelectionFilter for ResolutionAmbiguityFilter {
    fn name(&self) -> &'static str {
        "resolution_ambiguity"
    }

    fn evaluate(&self, ctx: &MarketCandidateCtx<'_>) -> FilterOutcome {
        let thresholds = ctx.thresholds;
        let Some(end_date) = ctx.candidate.end_date else {
            return if thresholds.allow_near_resolution {
                FilterOutcome::Keep
            } else {
                FilterOutcome::Exclude(ExclusionReason::ResolutionAmbiguous)
            };
        };

        let secs = (end_date - ctx.decision_at).num_seconds();
        if secs < thresholds.min_time_to_resolution_secs && !thresholds.allow_near_resolution {
            return FilterOutcome::Exclude(ExclusionReason::ResolutionAmbiguous);
        }
        if secs > thresholds.max_time_to_resolution_secs {
            return FilterOutcome::Exclude(ExclusionReason::ResolutionAmbiguous);
        }
        FilterOutcome::Keep
    }
}

/// Stage 6 — model feature-availability gate.
///
/// With no required features the stage keeps every market. Otherwise the
/// [`FeatureAvailabilityOracle`] checks each required feature's source against
/// the candidate's facts; the market is excluded only for the features it
/// genuinely cannot supply (no more blanket fail-closed).
pub struct ModelEligibilityFilter;

impl SelectionFilter for ModelEligibilityFilter {
    fn name(&self) -> &'static str {
        "model_eligibility"
    }

    fn evaluate(&self, ctx: &MarketCandidateCtx<'_>) -> FilterOutcome {
        // Resolved per-candidate: `generic` ∪ this candidate's OWN category's
        // specific requirements — never a different category's requirement
        // (a crypto-only domain requirement must never gate a Sports market).
        let required = ctx.model_requirements.for_category(ctx.candidate.category);
        if required.is_empty() {
            return FilterOutcome::Keep;
        }
        let oracle = FeatureAvailabilityOracle::new(ctx.feature_schema);
        let missing = oracle.missing_required(ctx.candidate, &required);
        if missing.is_empty() {
            FilterOutcome::Keep
        } else {
            FilterOutcome::Exclude(ExclusionReason::ModelFeatureUnavailable { missing })
        }
    }
}

/// The fixed, short-circuiting selection pipeline.
pub struct FilterChain {
    filters: Vec<Box<dyn SelectionFilter>>,
}

impl FilterChain {
    /// The canonical 6-stage pipeline in evaluation order.
    #[must_use]
    pub fn standard() -> Self {
        Self {
            filters: vec![
                Box::new(MarketStatusFilter),
                Box::new(CategoryFilter),
                Box::new(LiquidityFilter),
                Box::new(DataQualityFilter),
                Box::new(ResolutionAmbiguityFilter),
                Box::new(ModelEligibilityFilter),
            ],
        }
    }

    /// Run the pipeline, returning the first exclusion or [`FilterOutcome::Keep`].
    #[must_use]
    pub fn evaluate(&self, ctx: &MarketCandidateCtx<'_>) -> FilterOutcome {
        for filter in &self.filters {
            if let FilterOutcome::Exclude(reason) = filter.evaluate(ctx) {
                return FilterOutcome::Exclude(reason);
            }
        }
        FilterOutcome::Keep
    }
}

impl Default for FilterChain {
    fn default() -> Self {
        Self::standard()
    }
}

/// Fold one exclusion reason into the running aggregate summary.
pub const fn accumulate_exclusion(
    summary: &mut SelectionExclusionSummary,
    reason: &ExclusionReason,
) {
    match reason {
        ExclusionReason::StaleBook => summary.stale_book_count += 1,
        ExclusionReason::InsufficientLiquidity => summary.insufficient_liquidity_count += 1,
        ExclusionReason::ManuallyBlocked => summary.excluded_by_operator_count += 1,
        ExclusionReason::NotOpen
        | ExclusionReason::CategoryDisabled
        | ExclusionReason::SpreadTooWide
        | ExclusionReason::IngestLagExceeded
        | ExclusionReason::ResolutionAmbiguous
        | ExclusionReason::SelectionCapExceeded
        | ExclusionReason::ModelFeatureUnavailable { .. } => summary.other_count += 1,
    }
}

#[cfg(test)]
mod tests {
    use quant_pivot_models::runtime_config::{DataQualityConfig, SelectionConfig};

    use super::SelectionThresholds;

    #[test]
    fn resolution_thresholds_reject_values_outside_chrono_range() {
        let selection = SelectionConfig {
            max_time_to_resolution_secs: u64::MAX,
            ..SelectionConfig::default()
        };
        assert!(SelectionThresholds::resolve(&selection, &DataQualityConfig::default()).is_err());
    }
}
