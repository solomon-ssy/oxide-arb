//! Offline point-in-time market selection.
//!
//! Reconstructs, per historical `as_of`, the same market-selection funnel the
//! online report pipeline runs — by projecting a [`MarketCandidate`] from
//! point-in-time facts and evaluating it through the **identical**
//! [`ConfiguredMarketSelector`] / `FilterChain::standard()` code. Only markets
//! that survive the funnel at that instant enter the training spine, so the
//! offline dataset carries no train/serve selection skew.
//!
//! # Honest approximations (data availability)
//!
//! Gamma `liquidity_usd` / `volume_24h` are not historized in the offline plane,
//! so the funnel is replayed with principled substitutions:
//!
//! - **liquidity** → the book's combined visible USD depth ([`ResolvedBook::visible_liquidity_usd`]),
//!   gated by the frozen `training.min_selection_depth_usd` floor (a book-depth
//!   quantity, distinct from the Gamma-calibrated online `min_liquidity_usd`);
//! - **24h volume** → the volume floor is skipped offline (`min_volume_24h_usd = 0`),
//!   as trade-print volume is not the same measure as the Gamma figure;
//! - **feed health** (`connection_healthy` / `ingest_lag_ms`) → treated as
//!   healthy / zero: a stored historical book is the venue truth by construction,
//!   with no live-feed staleness to guard against.
//!
//! Every other gate (status, category, spread, book freshness, resolution window,
//! model eligibility) runs against exact point-in-time facts with the frozen
//! config's own thresholds.

use chrono::{DateTime, Utc};

use quant_pivot_error::QuantResult;
use quant_pivot_models::{
    domain::{DomainAvailability, MarketCandidate, MarketInfo},
    enums::domain::DomainFamily,
    runtime_config::{DataQualityConfig, DecimalString, FeaturesConfig, SelectionConfig},
    types::{RuntimeConfigVersionId, Usd},
};
use quant_pivot_research::{
    features::ResolvedBook,
    pit::{MarketContextAt, PitQueryEngine},
    selection::{
        ConfiguredMarketSelector, MarketSelectionBuildRequest, ModelFeatureRequirements,
        SelectionResult,
    },
};

/// Replays the online selection funnel over point-in-time facts, per `as_of`.
pub struct OfflinePitSelector {
    selector: ConfiguredMarketSelector,
    /// Frozen selection policy, with the two non-historized floors overridden for
    /// offline replay (`min_liquidity_usd` → book-depth floor, `min_volume` → 0).
    selection: SelectionConfig,
    data_quality: DataQualityConfig,
    features: FeaturesConfig,
    runtime_config_version_id: RuntimeConfigVersionId,
    source_delay_secs: u64,
}

impl OfflinePitSelector {
    /// Wire the selector from a build's frozen config snapshot.
    #[must_use]
    pub fn new(
        selection: &SelectionConfig,
        data_quality: &DataQualityConfig,
        features: &FeaturesConfig,
        min_selection_depth_usd: &DecimalString,
        runtime_config_version_id: RuntimeConfigVersionId,
        source_delay_secs: u64,
    ) -> Self {
        // Override only the two Gamma-sourced floors the offline plane cannot
        // reproduce; every other threshold stays at its frozen value.
        let mut offline_selection = selection.clone();
        offline_selection.min_liquidity_usd = min_selection_depth_usd.clone();
        offline_selection.min_volume_24h_usd = DecimalString::new("0");
        Self {
            selector: ConfiguredMarketSelector::new(),
            selection: offline_selection,
            data_quality: data_quality.clone(),
            features: features.clone(),
            runtime_config_version_id,
            source_delay_secs,
        }
    }

    /// Run the funnel over `markets` at `as_of`, returning the kept/excluded
    /// partition. Book + market context are resolved point-in-time from `pit`.
    pub async fn select_at(
        &self,
        as_of: DateTime<Utc>,
        markets: &[&MarketInfo],
        pit: &dyn PitQueryEngine,
    ) -> QuantResult<SelectionResult> {
        let mut candidates = Vec::with_capacity(markets.len());
        for market in markets {
            let context = pit.market_at(&market.market_id, as_of).await?;
            let book = pit
                .book_at(&market.yes_token_id, as_of)
                .await?
                .map(ResolvedBook::from);
            candidates.push(project_candidate(
                market,
                context.as_ref(),
                book.as_ref(),
                as_of,
            ));
        }
        let request = self.request(as_of);
        self.selector.select_markets(&request, &candidates)
    }

    fn request(&self, as_of: DateTime<Utc>) -> MarketSelectionBuildRequest {
        MarketSelectionBuildRequest {
            as_of,
            runtime_config_version_id: self.runtime_config_version_id.clone(),
            selection: self.selection.clone(),
            data_quality: self.data_quality.clone(),
            features: self.features.clone(),
            // A dataset is built before its model is trained, so selection does
            // not gate on a specific model's feature requirements.
            model_requirements: ModelFeatureRequirements::default(),
            source_delay_secs: self.source_delay_secs,
        }
    }
}

/// Project a point-in-time [`MarketCandidate`] from registry metadata + PIT book.
fn project_candidate(
    market: &MarketInfo,
    context: Option<&MarketContextAt>,
    book: Option<&ResolvedBook>,
    as_of: DateTime<Utc>,
) -> MarketCandidate {
    let depth_usd = book.map(ResolvedBook::visible_liquidity_usd);
    MarketCandidate {
        market_id: market.market_id.clone(),
        event_id: market.event_id.clone(),
        category: market.fee_category(),
        // Prefer the point-in-time lifecycle status (resolution-aware) over the
        // current registry status, so a since-resolved market is `Active` at an
        // `as_of` that predates its resolution.
        status: context.map_or(market.status, |ctx| ctx.status),
        primary_token_id: market.yes_token_id.clone(),
        secondary_token_id: Some(market.no_token_id.clone()),
        end_date: context.and_then(|ctx| ctx.end_date).or(market.end_date),
        // Gamma liquidity/volume are not historized: book depth is the liquidity
        // proxy; the volume floor is skipped via the offline threshold override.
        liquidity_usd: depth_usd,
        volume_24h_usd: Some(Usd::ZERO),
        best_bid: book.and_then(ResolvedBook::best_bid),
        best_ask: book.and_then(ResolvedBook::best_ask),
        depth_usd,
        book_age_ms: book.map(|resolved| book_age_ms(resolved, as_of)),
        crossed: book.is_some_and(ResolvedBook::is_crossed),
        // No book at `as_of` ⇒ treat as empty (fail-closed, like the online plane).
        empty: book.is_none_or(ResolvedBook::is_empty),
        // Offline replay: a stored book is the venue truth (no live-feed staleness).
        connection_healthy: true,
        ingest_lag_ms: 0,
        // Offline selection never gates on model feature requirements (see
        // `request`), so availability is conservative truth only: a mapped
        // category without a consulted linkage is `Unresolved` (fail-closed);
        // the domain slice itself is assembled later from the frozen ledger.
        domain_availability: DomainFamily::for_category(market.fee_category())
            .map_or(DomainAvailability::NotMapped, |_| {
                DomainAvailability::Unresolved
            }),
        observed_at: as_of,
    }
}

/// Book age in milliseconds at `as_of` (clamped non-negative).
fn book_age_ms(book: &ResolvedBook, as_of: DateTime<Utc>) -> u64 {
    let published = i64::try_from(book.timestamp_ms).unwrap_or(i64::MAX);
    u64::try_from((as_of.timestamp_millis() - published).max(0)).unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::project_candidate;
    use chrono::{TimeZone, Utc};
    use quant_pivot_models::{
        domain::{MarketInfo, market::book::BookLevel},
        enums::{
            common::{MarketCategory, TickSize},
            market::MarketStatus,
        },
        types::{EventId, MarketId, Price, Shares, TokenId, Usd},
    };
    use quant_pivot_research::{features::ResolvedBook, pit::BookSnapshotAt};
    use rust_decimal::Decimal;
    use std::sync::Arc;

    fn market() -> MarketInfo {
        let now = Utc.timestamp_millis_opt(1_000_000).single().expect("ts");
        MarketInfo {
            market_id: MarketId::new("m"),
            event_id: EventId::new("e"),
            question: "q".to_owned(),
            slug: "s".to_owned(),
            description: None,
            categories: vec![MarketCategory::Sports],
            status: MarketStatus::Active,
            outcome: None,
            yes_token_id: TokenId::new("yes"),
            no_token_id: TokenId::new("no"),
            tick_size: TickSize::Hundredth,
            neg_risk: false,
            end_date: Some(now),
            resolved_at: None,
            fees_enabled: false,
            fee_rate: None,
            fee_exponent: None,
            fee_taker_only: None,
            fee_rebate_rate: None,
            fee_source: None,
            fee_observed_at: None,
            created_at: now,
            updated_at: now,
        }
    }

    fn level(price: &str, size: u64) -> BookLevel {
        BookLevel::from_decimal_unchecked(
            Price::new(Decimal::from_str_exact(price).expect("price")),
            Shares::new(Decimal::from(size)),
        )
    }

    #[test]
    fn projects_book_depth_as_liquidity_with_volume_and_feed_sentinels() {
        let as_of = Utc.timestamp_millis_opt(2_000_000).single().expect("ts");
        let book = ResolvedBook::from(BookSnapshotAt {
            token_id: TokenId::new("yes"),
            as_of,
            bids: Arc::from([level("0.48", 100)]),
            asks: Arc::from([level("0.52", 100)]),
            timestamp_ms: 1_995_000,
            version: 1,
        });
        let candidate = project_candidate(&market(), None, Some(&book), as_of);

        // Book depth is the liquidity proxy; volume floor is skipped (sentinel 0).
        assert_eq!(candidate.liquidity_usd, Some(book.visible_liquidity_usd()));
        assert_eq!(candidate.depth_usd, Some(book.visible_liquidity_usd()));
        assert_eq!(candidate.volume_24h_usd, Some(Usd::ZERO));
        // Offline replay: the stored book is venue truth (no live-feed staleness).
        assert!(candidate.connection_healthy);
        assert_eq!(candidate.ingest_lag_ms, 0);
        assert_eq!(candidate.book_age_ms, Some(5_000));
        assert!(!candidate.crossed);
        assert!(!candidate.empty);
        assert_eq!(candidate.category, MarketCategory::Sports);
    }

    #[test]
    fn missing_book_projects_empty_fail_closed() {
        let as_of = Utc.timestamp_millis_opt(2_000_000).single().expect("ts");
        let candidate = project_candidate(&market(), None, None, as_of);
        assert!(candidate.empty, "no book at as_of ⇒ empty (fail-closed)");
        assert_eq!(candidate.liquidity_usd, None);
        assert_eq!(candidate.best_bid, None);
    }
}
