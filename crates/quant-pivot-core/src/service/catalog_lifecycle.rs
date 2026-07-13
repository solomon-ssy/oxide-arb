//! Catalog lifecycle rules applied during Gamma sync before registry registration.
//!
//! Complements wire-driven status mapping (`market_status_from_wire`) and
//! stale-catalog deactivation with a local rule: wire-`Active` markets whose
//! settlement deadline has passed are downgraded to [`MarketStatus::Paused`].

use chrono::{DateTime, Utc};
use quant_pivot_models::{
    domain::market::{MarketRegistryInfo, UpsertMarket},
    enums::market::MarketStatus,
    types::MarketId,
};
use std::collections::HashMap;

/// Reason a market was transitioned to [`MarketStatus::Paused`] during sync.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PauseReason {
    /// Wire `Active` but `end_date <= now` (local lifecycle).
    PastDeadline,
}

impl PauseReason {
    /// Prometheus label for [`MetricsHub::gamma_markets_paused`].
    #[must_use]
    pub const fn metric_label(self) -> &'static str {
        match self {
            Self::PastDeadline => "past_deadline",
        }
    }
}

/// Downgrade wire-`Active` markets whose settlement deadline has passed.
///
/// Idempotent: never mutates `Paused` (including `deactivate_stale`) or
/// `Settled`. Markets without `end_date` are left unchanged (subscription excludes
/// them via predicate filtering).
#[must_use]
pub fn apply_past_deadline_lifecycle(
    entry: &mut MarketRegistryInfo,
    now: DateTime<Utc>,
) -> Option<PauseReason> {
    if entry.status != MarketStatus::Active {
        return None;
    }
    let end_date = entry.end_date?;
    if end_date > now {
        return None;
    }
    entry.status = MarketStatus::Paused;
    Some(PauseReason::PastDeadline)
}

/// Apply past-deadline lifecycle to registry rows and mirror `status` into upserts.
#[must_use]
pub fn apply_past_deadline_to_sync_batch(
    registry_markets: &mut [MarketRegistryInfo],
    upsert_markets: &mut [UpsertMarket],
    now: DateTime<Utc>,
) -> u64 {
    let mut paused = 0_u64;
    for entry in registry_markets.iter_mut() {
        if apply_past_deadline_lifecycle(entry, now).is_some() {
            paused += 1;
        }
    }
    if paused == 0 {
        return 0;
    }

    let status_by_id: HashMap<&MarketId, MarketStatus> = registry_markets
        .iter()
        .map(|entry| (&entry.market_id, entry.status))
        .collect();
    for upsert in upsert_markets.iter_mut() {
        if let Some(status) = status_by_id.get(&upsert.market_id) {
            upsert.status = *status;
        }
    }
    paused
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;
    use quant_pivot_models::{
        domain::market::TokenInfo,
        enums::common::{CategorySet, MarketCategory, TickSize},
        types::{EventId, TokenId},
    };
    use rust_decimal_macros::dec;

    fn sample_market(status: MarketStatus, end_date: Option<DateTime<Utc>>) -> MarketRegistryInfo {
        MarketRegistryInfo {
            market_id: MarketId::new("0xtest"),
            event_id: EventId::new("evt-1"),
            token_yes: TokenId::new("yes"),
            token_no: TokenId::new("no"),
            question: "Q?".into(),
            slug: "q".into(),
            description: None,
            categories: CategorySet::from(MarketCategory::Politics),
            status,
            outcome: None,
            neg_risk: false,
            tick_size: TickSize::Hundredth,
            tokens: vec![
                TokenInfo {
                    token_id: TokenId::new("yes"),
                    outcome: "Yes".into(),
                    neg_risk: false,
                },
                TokenInfo {
                    token_id: TokenId::new("no"),
                    outcome: "No".into(),
                    neg_risk: false,
                },
            ],
            best_bid: None,
            best_ask: None,
            depth_usd: None,
            min_order_size: dec!(5),
            liquidity_usd: None,
            volume_24h: None,
            fee_schedule: None,
            end_date,
            resolved_at: None,
            created_at: Some(Utc::now()),
            updated_at: Utc::now(),
        }
    }

    #[test]
    fn past_deadline_active_becomes_paused() {
        let now = Utc::now();
        let mut entry = sample_market(MarketStatus::Active, Some(now - Duration::hours(1)));
        assert_eq!(
            apply_past_deadline_lifecycle(&mut entry, now),
            Some(PauseReason::PastDeadline)
        );
        assert_eq!(entry.status, MarketStatus::Paused);
    }

    #[test]
    fn future_deadline_stays_active() {
        let now = Utc::now();
        let mut entry = sample_market(MarketStatus::Active, Some(now + Duration::hours(1)));
        assert_eq!(apply_past_deadline_lifecycle(&mut entry, now), None);
        assert_eq!(entry.status, MarketStatus::Active);
    }

    #[test]
    fn settled_and_paused_are_not_overwritten() {
        let now = Utc::now();
        let past = Some(now - Duration::hours(1));
        let mut settled = sample_market(MarketStatus::Settled, past);
        let mut paused = sample_market(MarketStatus::Paused, past);
        assert_eq!(apply_past_deadline_lifecycle(&mut settled, now), None);
        assert_eq!(apply_past_deadline_lifecycle(&mut paused, now), None);
    }

    #[test]
    fn batch_sync_mirrors_status_into_upsert() {
        let now = Utc::now();
        let mut registry = sample_market(MarketStatus::Active, Some(now - Duration::hours(2)));
        let mut upsert = UpsertMarket::try_from(&registry).expect("valid upsert");
        let paused = apply_past_deadline_to_sync_batch(
            std::slice::from_mut(&mut registry),
            std::slice::from_mut(&mut upsert),
            now,
        );
        assert_eq!(paused, 1);
        assert_eq!(registry.status, MarketStatus::Paused);
        assert_eq!(upsert.status, MarketStatus::Paused);
    }
}
