//! Market selection orchestration wiring: research snapshot → persistence DTOs.
//!
//! The research plane owns [`MarketSelectionSnapshot`]; Postgres owns
//! [`MarketSelectionModel`]. This module is the core-side adapter between them.

use std::collections::HashMap;

use quant_pivot_error::{QuantResult, report::ReportError};
use quant_pivot_models::{
    domain::{MarketCandidate, MarketSelectionModel, NewMarketSelection, NewMarketSelectionMember},
    enums::market::MarketStatus,
    types::MarketId,
};
use quant_pivot_research::selection::MarketSelectionSnapshot;

/// Map a research snapshot plus the frozen candidate slice into persistence DTOs.
///
/// Member `status` is resolved from the candidate freeze (not carried on
/// [`quant_pivot_research::selection::SelectedMarket`]). Every included market
/// must have a matching candidate row; otherwise mapping fails closed.
pub fn map_snapshot_to_model(
    snapshot: &MarketSelectionSnapshot,
    candidates: &[MarketCandidate],
) -> QuantResult<MarketSelectionModel> {
    let status_by_market = candidates
        .iter()
        .map(|candidate| (candidate.market_id.clone(), candidate.status))
        .collect::<HashMap<MarketId, MarketStatus>>();

    let snapshot_row = NewMarketSelection {
        market_selection_id: snapshot.market_selection_id.clone(),
        as_of: snapshot.as_of,
        runtime_config_version_id: snapshot.runtime_config_version_id.clone(),
        selector_hash: snapshot.selector_hash.clone(),
        market_count: i32::try_from(snapshot.included.len()).map_err(|err| {
            ReportError::NumericOverflow {
                field: "market_selection.market_count",
                detail: format!(
                    "included market count {} exceeds i32::MAX: {err}",
                    snapshot.included.len()
                ),
            }
        })?,
        exclusion_summary: snapshot.exclusion_summary,
    };

    let members = snapshot
        .included
        .iter()
        .map(|selected| {
            let status = status_by_market
                .get(&selected.market_id)
                .copied()
                .ok_or_else(|| ReportError::InvariantViolation {
                    stage: "market_selection",
                    detail: format!(
                        "missing candidate status for included market {}",
                        selected.market_id
                    ),
                })?;
            Ok(NewMarketSelectionMember {
                market_selection_id: snapshot.market_selection_id.clone(),
                market_id: selected.market_id.clone(),
                event_id: selected.event_id.clone(),
                category: selected.category,
                status,
                primary_token_id: selected.primary_token_id.clone(),
                secondary_token_id: selected.secondary_token_id.clone(),
                liquidity_usd: selected.liquidity_usd,
                volume_24h_usd: selected.volume_24h_usd,
            })
        })
        .collect::<QuantResult<Vec<_>>>()?;

    Ok(MarketSelectionModel {
        snapshot: snapshot_row,
        members,
    })
}

#[cfg(test)]
mod tests {
    use super::map_snapshot_to_model;
    use chrono::{TimeZone, Utc};
    use quant_pivot_models::{
        domain::MarketCandidate,
        enums::{common::MarketCategory, market::MarketStatus},
        types::{
            ContentHash, EventId, MarketId, MarketSelectionId, Price, RuntimeConfigVersionId,
            SelectionExclusionSummary, TokenId, Usd,
        },
    };
    use quant_pivot_research::selection::{
        ExcludedMarket, ExclusionReason, MarketSelectionSnapshot, SelectedMarket,
    };
    use rust_decimal::Decimal;

    fn as_of() -> chrono::DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 6, 1, 0, 0, 0).unwrap()
    }

    fn candidate(id: &str) -> MarketCandidate {
        MarketCandidate {
            market_id: MarketId::new(id),
            event_id: EventId::new("evt-1"),
            category: MarketCategory::Sports,
            status: MarketStatus::Active,
            primary_token_id: TokenId::new("yes"),
            secondary_token_id: Some(TokenId::new("no")),
            end_date: Some(as_of() + chrono::Duration::days(7)),
            liquidity_usd: Some(Usd::new(Decimal::from(10_000))),
            volume_24h_usd: Some(Usd::new(Decimal::from(5_000))),
            best_bid: Some(Price::new(Decimal::new(49, 2))),
            best_ask: Some(Price::new(Decimal::new(51, 2))),
            depth_usd: Some(Usd::new(Decimal::from(2_000))),
            book_age_ms: Some(500),
            crossed: false,
            empty: false,
            connection_healthy: true,
            ingest_lag_ms: 1_000,
            observed_at: as_of(),
        }
    }

    #[test]
    fn map_snapshot_to_model_projects_members_and_summary() {
        let snapshot = MarketSelectionSnapshot {
            market_selection_id: MarketSelectionId::from_v7(),
            as_of: as_of(),
            runtime_config_version_id: RuntimeConfigVersionId::from_v7(),
            selector_hash: ContentHash::parse(format!("blake3:{}", "b".repeat(64)))
                .expect("valid hash"),
            included: vec![SelectedMarket {
                market_id: MarketId::new("0xok"),
                event_id: EventId::new("evt-1"),
                category: MarketCategory::Sports,
                primary_token_id: TokenId::new("yes"),
                secondary_token_id: Some(TokenId::new("no")),
                liquidity_usd: Some(Usd::new(Decimal::from(10_000))),
                volume_24h_usd: Some(Usd::new(Decimal::from(5_000))),
                source_refs: Vec::new(),
            }],
            excluded: vec![ExcludedMarket {
                market_id: MarketId::new("0xblocked"),
                reason: ExclusionReason::SelectionCapExceeded,
            }],
            exclusion_summary: SelectionExclusionSummary::default(),
        };
        let candidates = vec![candidate("0xok"), candidate("0xblocked")];

        let model = map_snapshot_to_model(&snapshot, &candidates).expect("map snapshot");

        assert_eq!(
            model.snapshot.market_selection_id,
            snapshot.market_selection_id
        );
        assert_eq!(model.snapshot.market_count, 1);
        assert_eq!(model.members.len(), 1);
        assert_eq!(model.members[0].market_id.as_str(), "0xok");
        assert_eq!(model.members[0].status, MarketStatus::Active);
        assert_eq!(model.members[0].category, MarketCategory::Sports);
    }
}
