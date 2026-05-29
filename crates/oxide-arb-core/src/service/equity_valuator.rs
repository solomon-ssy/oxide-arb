use crate::pipeline::{book_store::BookStore, market_registry::MarketRegistry};
use chrono::{DateTime, Utc};
use oxide_arb_algorithm::calibration::ResolutionCalibrator;
use oxide_arb_models::{
    domain::{calibration::BucketKey, position::PositionInfo},
    enums::calibration::{DurationBucket, PriceZone},
    types::{Price, Usd},
};
use rust_decimal::Decimal;
use std::sync::Arc;

const MAX_MARK_BOOK_STALENESS_MS: u64 = 30_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MarkSource {
    Calibrated,
    BookBid,
    CostBasis,
}

#[derive(Debug, Clone)]
pub struct PerPositionMark {
    pub position: PositionInfo,
    pub mark: Price,
    pub value: Usd,
    pub stale: bool,
    pub source: MarkSource,
}

pub struct EquityValuator {
    registry: Arc<MarketRegistry>,
    book_store: Arc<BookStore>,
    calibrator: Arc<ResolutionCalibrator>,
}

impl EquityValuator {
    pub const fn new(
        registry: Arc<MarketRegistry>,
        book_store: Arc<BookStore>,
        calibrator: Arc<ResolutionCalibrator>,
    ) -> Self {
        Self {
            registry,
            book_store,
            calibrator,
        }
    }

    #[must_use]
    pub fn value(
        &self,
        positions: &[PositionInfo],
        now: DateTime<Utc>,
    ) -> (Usd, Vec<PerPositionMark>) {
        let now_ms = u64::try_from(now.timestamp_millis().max(0)).unwrap_or(0);
        let mut total = Usd::ZERO;
        let mut marks = Vec::with_capacity(positions.len());

        for position in positions {
            let (mark, stale, source) = self.mark_position(position, now, now_ms);
            let value = position.shares * mark;
            total += value;
            marks.push(PerPositionMark {
                position: position.clone(),
                mark,
                value,
                stale,
                source,
            });
        }

        (total, marks)
    }

    fn mark_position(
        &self,
        position: &PositionInfo,
        now: DateTime<Utc>,
        now_ms: u64,
    ) -> (Price, bool, MarkSource) {
        let Some(bid) = self.fresh_best_bid(position, now_ms) else {
            return (position.avg_entry_price, true, MarkSource::CostBasis);
        };

        let Some(market) = self.registry.get_market(&position.market_id) else {
            return (bid, false, MarkSource::BookBid);
        };
        let Some(end_date) = market.end_date else {
            return (bid, false, MarkSource::BookBid);
        };

        let seconds_to_end = end_date.signed_duration_since(now).num_seconds().max(0);
        let key = BucketKey {
            category: market.category,
            price_zone: PriceZone::from_price(bid),
            duration_bucket: DurationBucket::from_secs(u64::try_from(seconds_to_end).unwrap_or(0)),
        };
        let calibrated = self.calibrator.lookup(&key).posterior_mean();
        (
            Price::new(calibrated.clamp(Decimal::ZERO, Decimal::ONE)),
            false,
            MarkSource::Calibrated,
        )
    }

    fn fresh_best_bid(&self, position: &PositionInfo, now_ms: u64) -> Option<Price> {
        let snapshot = self.book_store.load(&position.token_id)?;
        let age_ms = now_ms.saturating_sub(snapshot.timestamp_ms);
        if snapshot.timestamp_ms == 0 || age_ms > MAX_MARK_BOOK_STALENESS_MS {
            return None;
        }
        snapshot.best_bid()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        observability::metrics_hub::MetricsHub,
        pipeline::{book_store::BookStore, market_registry::MarketRegistry},
    };
    use oxide_arb_algorithm::calibration::CalibrationEntry;
    use oxide_arb_models::{
        config::CalibrationConfig,
        domain::{
            BookLevel,
            market::{MarketRegistryInfo, TokenInfo},
        },
        enums::{
            calibration::{DurationBucket, PriceZone},
            common::{
                MarketCategory, PositionStatus, RedeemStatus, SettlementAccountingStatus,
                SettlementTrigger, Side, TickSize,
            },
            market::MarketStatus,
        },
        types::{EventId, MarketId, PositionId, Shares, TokenId, TradeId},
    };
    use rust_decimal_macros::dec;

    #[test]
    fn marks_with_calibrated_probability_when_market_context_is_complete() {
        let now = Utc::now();
        let yes = TokenId::new("yes");
        let no = TokenId::new("no");
        let market = MarketId::new("market");
        let valuator = valuator(now, &market, &yes, &no, true);

        let position = position(&market, &yes, dec!(100), dec!(0.90));
        let (total, marks) = valuator.value(&[position], now);

        assert_eq!(marks[0].source, MarkSource::Calibrated);
        assert_eq!(marks[0].mark.inner().round_dp(2), dec!(0.80));
        assert_eq!(total.inner().round_dp(2), dec!(80.00));
    }

    #[test]
    fn falls_back_to_book_bid_without_end_date() {
        let now = Utc::now();
        let yes = TokenId::new("yes");
        let no = TokenId::new("no");
        let market = MarketId::new("market");
        let valuator = valuator(now, &market, &yes, &no, false);

        let position = position(&market, &yes, dec!(100), dec!(0.90));
        let (total, marks) = valuator.value(&[position], now);

        assert_eq!(marks[0].source, MarkSource::BookBid);
        assert_eq!(marks[0].mark.inner(), dec!(0.97));
        assert_eq!(total.inner().round_dp(2), dec!(97.00));
    }

    #[test]
    fn falls_back_to_cost_basis_when_book_is_stale() {
        let now = Utc::now();
        let yes = TokenId::new("yes");
        let no = TokenId::new("no");
        let market = MarketId::new("market");
        let registry = Arc::new(MarketRegistry::new());
        registry.register_market(market_entry(now, &market, &yes, &no, true));
        let book_store = Arc::new(BookStore::new(Arc::new(MetricsHub::new())));
        book_store.apply_snapshot(&yes, vec![level(dec!(0.97))], Vec::new(), 1, None);
        let calibrator = Arc::new(ResolutionCalibrator::from_entries(
            vec![calibration_entry()],
            CalibrationConfig::default(),
        ));
        let valuator = EquityValuator::new(registry, book_store, calibrator);

        let position = position(&market, &yes, dec!(100), dec!(0.90));
        let (total, marks) = valuator.value(&[position], now);

        assert_eq!(marks[0].source, MarkSource::CostBasis);
        assert!(marks[0].stale);
        assert_eq!(total.inner().round_dp(2), dec!(90.00));
    }

    fn valuator(
        now: DateTime<Utc>,
        market: &MarketId,
        yes: &TokenId,
        no: &TokenId,
        with_end_date: bool,
    ) -> EquityValuator {
        let registry = Arc::new(MarketRegistry::new());
        registry.register_market(market_entry(now, market, yes, no, with_end_date));
        let book_store = Arc::new(BookStore::new(Arc::new(MetricsHub::new())));
        let timestamp_ms = u64::try_from(now.timestamp_millis().max(0)).unwrap_or(0);
        book_store.apply_snapshot(yes, vec![level(dec!(0.97))], Vec::new(), timestamp_ms, None);
        let calibrator = Arc::new(ResolutionCalibrator::from_entries(
            vec![calibration_entry()],
            CalibrationConfig::default(),
        ));
        EquityValuator::new(registry, book_store, calibrator)
    }

    fn market_entry(
        now: DateTime<Utc>,
        market_id: &MarketId,
        yes: &TokenId,
        no: &TokenId,
        with_end_date: bool,
    ) -> MarketRegistryInfo {
        MarketRegistryInfo {
            market_id: market_id.clone(),
            event_id: EventId::new("event"),
            token_yes: yes.clone(),
            token_no: no.clone(),
            question: "question".into(),
            slug: "slug".into(),
            category: MarketCategory::Politics,
            status: MarketStatus::Active,
            outcome: None,
            neg_risk: false,
            tick_size: TickSize::Hundredth,
            tokens: vec![
                TokenInfo {
                    token_id: yes.clone(),
                    outcome: "Yes".into(),
                    neg_risk: false,
                },
                TokenInfo {
                    token_id: no.clone(),
                    outcome: "No".into(),
                    neg_risk: false,
                },
            ],
            best_bid: None,
            best_ask: None,
            depth_usd: None,
            min_order_size: Decimal::ZERO,
            volume_24h: Usd::ZERO,
            fee_schedule: None,
            end_date: with_end_date.then_some(now + chrono::Duration::hours(2)),
            resolved_at: None,
            created_at: now,
            updated_at: now,
        }
    }

    fn position(
        market_id: &MarketId,
        token_id: &TokenId,
        shares: Decimal,
        entry: Decimal,
    ) -> PositionInfo {
        PositionInfo {
            position_id: PositionId::generate(),
            trade_id: TradeId::generate(),
            market_id: market_id.clone(),
            token_id: token_id.clone(),
            side: Side::Buy,
            shares: Shares::new(shares),
            avg_entry_price: Price::new(entry),
            total_cost_usd: Usd::new(shares * entry),
            total_fees_usd: Usd::ZERO,
            unrealized_pnl: Usd::ZERO,
            realized_pnl: Usd::ZERO,
            status: PositionStatus::Open,
            opened_at: Utc::now(),
            closed_at: None,
            settled_at: None,
            winning_token_id: None,
            settlement_payout_usd: None,
            redeem_tx_hash: None,
            redeem_status: RedeemStatus::Pending,
            redeem_attempts: 0,
            oracle_verdict: None,
            settlement_trigger: Some(SettlementTrigger::Ws),
            settlement_accounting_status: SettlementAccountingStatus::Pending,
            settlement_accounting_error: None,
            settlement_accounted_at: None,
            redeem_terminal_reason: None,
        }
    }

    fn level(price: Decimal) -> BookLevel {
        BookLevel::from_decimal_unchecked(Price::new(price), Shares::new(dec!(100)))
    }

    fn calibration_entry() -> CalibrationEntry {
        CalibrationEntry {
            bucket_key: BucketKey {
                category: MarketCategory::Politics,
                price_zone: PriceZone::Z97,
                duration_bucket: DurationBucket::Medium,
            },
            total_count: 100,
            correct_count: 80,
            alpha_prior: dec!(0),
            beta_prior: dec!(0),
            fallback_tier: 1,
        }
    }
}
