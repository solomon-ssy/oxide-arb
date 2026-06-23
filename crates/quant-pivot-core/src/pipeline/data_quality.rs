//! Live book-plane data-quality classification.
//!
//! Maps each token's current book to a [`DataQualityStatus`] from the staleness
//! ladder plus structural checks (empty / crossed) and ingest-side fact lag.
//! The aggregate [`DataQualitySnapshot`] feeds the operator API and Prometheus.
//!
//! TODO(phase-3): gate on `min_book_depth_usd` once feature builders consume
//! per-level notionals; depth-in-USD is intentionally out of scope here.

use std::sync::Arc;

use arc_swap::ArcSwap;
use chrono::Utc;
use quant_pivot_models::{
    domain::{DataQualityInput, DataQualityPort, DataQualityReport, DataQualitySnapshot},
    enums::{common::StalenessLevel, quant::DataQualityStatus},
    runtime_config::DataQualityConfig,
};

use crate::{
    observability::fact_lag::FactLagTracker,
    pipeline::{book_store::BookStore, staleness_classifier::StalenessClassifier},
};

/// Classifies live book freshness and structural validity.
pub trait DataQualityService: Send + Sync {
    /// Classify one token's book observation.
    fn classify(&self, input: DataQualityInput) -> DataQualityReport;
    /// Aggregate classification across the live book plane at call time.
    fn snapshot(&self) -> DataQualitySnapshot;
}

/// Hot-swappable gating policy derived from `DataQualityConfig`.
struct QualityPolicy {
    max_book_age_ms: u64,
    max_fact_lag_ms: u64,
    reject_crossed_books: bool,
    reject_empty_books: bool,
}

impl QualityPolicy {
    const fn from_config(config: &DataQualityConfig) -> Self {
        Self {
            max_book_age_ms: config.max_book_age_ms,
            max_fact_lag_ms: config.max_fact_lag_secs.saturating_mul(1_000),
            reject_crossed_books: config.reject_crossed_books,
            reject_empty_books: config.reject_empty_books,
        }
    }
}

/// `BookStore`-backed data-quality service. Clones of the staleness classifier
/// and the policy snapshot are hot-reloaded together on config activation.
pub struct BookDataQualityService {
    book_store: Arc<BookStore>,
    staleness: StalenessClassifier,
    policy: ArcSwap<QualityPolicy>,
    fact_lag: Arc<FactLagTracker>,
}

impl BookDataQualityService {
    #[must_use]
    pub fn new(
        book_store: Arc<BookStore>,
        config: &DataQualityConfig,
        fact_lag: Arc<FactLagTracker>,
    ) -> Self {
        Self {
            book_store,
            staleness: StalenessClassifier::new(config),
            policy: ArcSwap::from_pointee(QualityPolicy::from_config(config)),
            fact_lag,
        }
    }

    /// Hot-reload thresholds on runtime-config activation.
    pub fn reload(&self, config: &DataQualityConfig) {
        self.staleness.reload(config);
        self.policy
            .store(Arc::new(QualityPolicy::from_config(config)));
    }

    /// Reset and return the peak fact lag for the elapsed metrics window.
    #[must_use]
    pub fn take_worst_fact_lag_ms(&self) -> u64 {
        self.fact_lag.take_worst_ms()
    }

    fn status_for(
        staleness: StalenessLevel,
        crossed: bool,
        empty: bool,
        fact_lag_ms: Option<u64>,
        policy: &QualityPolicy,
    ) -> DataQualityStatus {
        if empty && policy.reject_empty_books {
            return DataQualityStatus::Insufficient;
        }
        if crossed && policy.reject_crossed_books {
            return DataQualityStatus::Degraded;
        }
        if fact_lag_ms.is_some_and(|lag| lag > policy.max_fact_lag_ms) {
            return DataQualityStatus::Degraded;
        }
        match staleness {
            StalenessLevel::Fresh => DataQualityStatus::Fresh,
            StalenessLevel::Acceptable => DataQualityStatus::Acceptable,
            StalenessLevel::Stale | StalenessLevel::Expired => DataQualityStatus::Stale,
        }
    }

    fn now_ms() -> u64 {
        u64::try_from(Utc::now().timestamp_millis()).unwrap_or(0)
    }
}

impl DataQualityService for BookDataQualityService {
    fn classify(&self, input: DataQualityInput) -> DataQualityReport {
        let staleness = self.staleness.classify(input.book_age_ms);
        let policy = self.policy.load();
        let status = Self::status_for(
            staleness,
            input.crossed,
            input.empty,
            input.fact_lag_ms,
            &policy,
        );
        DataQualityReport {
            token_id: input.token_id,
            status,
            staleness,
            book_age_ms: input.book_age_ms,
            crossed: input.crossed,
            empty: input.empty,
        }
    }

    fn snapshot(&self) -> DataQualitySnapshot {
        let now_ms = Self::now_ms();
        let policy = self.policy.load();
        let worst_fact_lag_ms = self.fact_lag.peek_worst_ms();
        let fact_lag_exceeded = worst_fact_lag_ms > policy.max_fact_lag_ms;
        let mut snapshot =
            DataQualitySnapshot::empty(Utc::now(), policy.max_book_age_ms, policy.max_fact_lag_ms);
        snapshot.worst_fact_lag_ms = worst_fact_lag_ms;
        snapshot.fact_lag_exceeded = fact_lag_exceeded;

        let aggregate_lag = if fact_lag_exceeded {
            Some(worst_fact_lag_ms)
        } else {
            None
        };

        for (token_id, book) in self.book_store.published_snapshots() {
            let book_age_ms = now_ms.saturating_sub(book.timestamp_ms);
            let empty = book.bids.is_empty() || book.asks.is_empty();
            let crossed = match (book.best_bid(), book.best_ask()) {
                (Some(bid), Some(ask)) => bid >= ask,
                _ => false,
            };
            let report = self.classify(DataQualityInput {
                token_id,
                book_age_ms,
                crossed,
                empty,
                fact_lag_ms: aggregate_lag,
            });
            snapshot.tally(report.status);
        }
        snapshot
    }
}

impl DataQualityPort for BookDataQualityService {
    fn snapshot(&self) -> DataQualitySnapshot {
        DataQualityService::snapshot(self)
    }
}
