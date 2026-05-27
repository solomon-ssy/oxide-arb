use std::sync::Arc;

use chrono::Utc;
use num_traits::ToPrimitive;
use oxide_arb_error::trading::TradingError;
use oxide_arb_models::domain::execution::ValidationResult;
use oxide_arb_models::domain::opportunity::Opportunity;
use oxide_arb_models::enums::common::{Side, StalenessLevel};
use oxide_arb_models::types::{Bps, TokenId};
use rust_decimal_macros::dec;

use crate::observability::metrics_hub::MetricsHub;
use crate::pipeline::book_store::BookStore;
use crate::pipeline::staleness_classifier::StalenessClassifier;

pub struct Validator {
    book_store: Arc<BookStore>,
    staleness_classifier: StalenessClassifier,
    max_slippage_bps: rust_decimal::Decimal,
    max_book_to_order_ms: u64,
    metrics: Arc<MetricsHub>,
}

impl Validator {
    pub const fn new(
        book_store: Arc<BookStore>,
        staleness_classifier: StalenessClassifier,
        max_slippage_bps: rust_decimal::Decimal,
        max_book_to_order_ms: u64,
        metrics: Arc<MetricsHub>,
    ) -> Self {
        Self {
            book_store,
            staleness_classifier,
            max_slippage_bps,
            max_book_to_order_ms,
            metrics,
        }
    }

    #[inline]
    pub fn validate(
        &self,
        opp: &Opportunity,
        token_yes: &TokenId,
        token_no: &TokenId,
        book_yes_version: u64,
        book_no_version: u64,
    ) -> Result<ValidationResult, TradingError> {
        let now_ms = ToPrimitive::to_u64(&Utc::now().timestamp_millis().max(0)).unwrap_or(0);
        let top = self
            .book_store
            .top_of_book_tokens(token_yes, token_no, now_ms)
            .ok_or_else(|| TradingError::Validation("book not available".into()))?;

        if top.yes_version < book_yes_version || top.no_version < book_no_version {
            self.metrics.book_freshness_rejected.inc();
            return Err(TradingError::Validation(
                "book version regressed since detection".into(),
            ));
        }

        if top.max_staleness_ms > self.max_book_to_order_ms {
            self.metrics.book_freshness_rejected.inc();
            return Err(TradingError::Validation(format!(
                "book age {0}ms exceeds SLO-2 budget {1}ms",
                top.max_staleness_ms, self.max_book_to_order_ms
            )));
        }

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
