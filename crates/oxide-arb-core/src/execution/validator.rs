use std::sync::Arc;

use chrono::Utc;
use num_traits::ToPrimitive;
use oxide_arb_error::trading::TradingError;
use oxide_arb_models::domain::execution::ValidationResult;
use oxide_arb_models::domain::opportunity::Opportunity;
use oxide_arb_models::enums::common::{Side, StalenessLevel};
use oxide_arb_models::types::Bps;
use rust_decimal_macros::dec;

use crate::observability::metrics_hub::MetricsHub;
use crate::pipeline::book_store::BookStore;
use crate::pipeline::market_registry::MarketRegistry;
use crate::pipeline::staleness_classifier::StalenessClassifier;

pub struct Validator {
    book_store: Arc<BookStore>,
    market_registry: Arc<MarketRegistry>,
    staleness_classifier: StalenessClassifier,
    max_slippage_bps: rust_decimal::Decimal,
    metrics: Arc<MetricsHub>,
}

impl Validator {
    pub const fn new(
        book_store: Arc<BookStore>,
        market_registry: Arc<MarketRegistry>,
        staleness_classifier: StalenessClassifier,
        max_slippage_bps: rust_decimal::Decimal,
        metrics: Arc<MetricsHub>,
    ) -> Self {
        Self {
            book_store,
            market_registry,
            staleness_classifier,
            max_slippage_bps,
            metrics,
        }
    }

    pub fn validate(&self, opp: &Opportunity) -> Result<ValidationResult, TradingError> {
        let now_ms = ToPrimitive::to_u64(&Utc::now().timestamp_millis().max(0)).unwrap_or(0);
        let top = self
            .book_store
            .top_of_book(&self.market_registry, &opp.market_id, now_ms)
            .ok_or_else(|| TradingError::Validation("book not available".into()))?;

        let staleness = self.staleness_classifier.classify(top.max_staleness_ms);
        if staleness > StalenessLevel::Acceptable {
            self.metrics.validation_failures.inc();
            return Err(TradingError::Validation(format!(
                "book staleness {staleness:?}"
            )));
        }

        let current_price = match opp.side {
            Side::Buy => top.yes_best_ask,
            Side::Sell => top.yes_best_bid,
        }
        .ok_or_else(|| TradingError::Validation("no price on relevant side".into()))?;

        let slippage_bps = if opp.entry_price.inner() > rust_decimal::Decimal::ZERO {
            ((current_price.inner() - opp.entry_price.inner()).abs() / opp.entry_price.inner()
                * dec!(10000))
            .round()
        } else {
            rust_decimal::Decimal::ZERO
        };

        if slippage_bps > self.max_slippage_bps {
            self.metrics.validation_failures.inc();
            return Err(TradingError::Validation(format!(
                "slippage {slippage_bps}bps exceeds max {}bps",
                self.max_slippage_bps
            )));
        }

        Ok(ValidationResult {
            current_price,
            staleness,
            slippage_bps: Bps::new(slippage_bps),
            validated_at: Utc::now(),
        })
    }
}
