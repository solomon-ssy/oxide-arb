//! Live book-plane data-quality classification.
//!
//! Maps each token's current book to a [`DataQualityStatus`] from the staleness
//! ladder, structural checks (empty / crossed), and market-data connection
//! liveness. Classification is a function of **per-token** signals only (local
//! book age via the WS receipt clock, empty/crossed); ingest pipeline lag
//! (`enqueue`→`ClickHouse` flush-ack) is a **plane-level** field on
//! [`DataQualitySnapshot`] and never downgrades individual tokens. On
//! Polymarket a quiet but valid book stays usable while the connection is
//! healthy; only connection failure plus an aged book yields [`DataQualityStatus::Stale`].
//! The aggregate snapshot feeds the operator API and Prometheus.
//!
//! TODO(phase-3): gate on `min_book_depth_usd` once feature builders consume
//! per-level notionals; depth-in-USD is intentionally out of scope here.

use std::sync::Arc;

use arc_swap::ArcSwap;
use chrono::Utc;
use quant_pivot_api::ws::WsShardHealthPort;
use quant_pivot_models::{
    domain::{DataQualityInput, DataQualityPort, DataQualityReport, DataQualitySnapshot},
    enums::{common::StalenessLevel, quant::DataQualityStatus},
    runtime_config::DataQualityConfig,
};

use crate::{
    observability::fact_lag::IngestPipelineLagTracker,
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
    max_ingest_lag_ms: u64,
    reject_crossed_books: bool,
    reject_empty_books: bool,
}

impl QualityPolicy {
    const fn from_config(config: &DataQualityConfig) -> Self {
        Self {
            max_book_age_ms: config.max_book_age_ms,
            max_ingest_lag_ms: config.max_ingest_lag_ms,
            reject_crossed_books: config.reject_crossed_books,
            reject_empty_books: config.reject_empty_books,
        }
    }
}

/// `BookStore`-backed data-quality service. Clones of the staleness classifier
/// and the policy snapshot are hot-reloaded together on config activation.
///
/// Per-token classification is a function of **per-token** signals only
/// (structure + local book age + connection liveness). The `ClickHouse` ingest
/// pipeline lag is a **plane-level** health field on the snapshot, never a
/// per-token downgrade — a slow persistence pipeline must not poison the live
/// freshness of every token.
pub struct BookDataQualityService {
    book_store: Arc<BookStore>,
    ws_health: Arc<dyn WsShardHealthPort>,
    staleness: StalenessClassifier,
    policy: ArcSwap<QualityPolicy>,
    ingest_lag: Arc<IngestPipelineLagTracker>,
}

impl BookDataQualityService {
    #[must_use]
    pub fn new(
        book_store: Arc<BookStore>,
        ws_health: Arc<dyn WsShardHealthPort>,
        config: &DataQualityConfig,
        ingest_lag: Arc<IngestPipelineLagTracker>,
    ) -> Self {
        Self {
            book_store,
            ws_health,
            staleness: StalenessClassifier::new(config),
            policy: ArcSwap::from_pointee(QualityPolicy::from_config(config)),
            ingest_lag,
        }
    }

    /// Hot-reload thresholds on runtime-config activation.
    pub fn reload(&self, config: &DataQualityConfig) {
        self.staleness.reload(config);
        self.policy
            .store(Arc::new(QualityPolicy::from_config(config)));
    }

    /// Reset and return the peak ingest pipeline lag for the elapsed window.
    #[must_use]
    pub fn take_worst_ingest_lag_ms(&self) -> u64 {
        self.ingest_lag.take_worst_ms()
    }

    const fn status_for(
        staleness: StalenessLevel,
        crossed: bool,
        empty: bool,
        connection_healthy: bool,
        policy: &QualityPolicy,
    ) -> DataQualityStatus {
        if empty && policy.reject_empty_books {
            return DataQualityStatus::Insufficient;
        }
        if crossed && policy.reject_crossed_books {
            return DataQualityStatus::Degraded;
        }
        match staleness {
            StalenessLevel::Fresh => DataQualityStatus::Fresh,
            StalenessLevel::Acceptable => DataQualityStatus::Acceptable,
            // Aged beyond the acceptable window: on Polymarket a quiet book is
            // not resent, so this is only a problem when the connection itself
            // is unhealthy (we may be missing updates). Otherwise the book is
            // still the current venue truth → Acceptable.
            StalenessLevel::Stale | StalenessLevel::Expired => {
                if connection_healthy {
                    DataQualityStatus::Acceptable
                } else {
                    DataQualityStatus::Stale
                }
            }
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
            input.connection_healthy,
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
        let worst_ingest_lag_ms = self.ingest_lag.peek_worst_ms();
        let ingest_lag_exceeded = worst_ingest_lag_ms > policy.max_ingest_lag_ms;
        let connection_healthy = self.ws_health.market_data_healthy();
        let mut snapshot = DataQualitySnapshot::empty(
            Utc::now(),
            policy.max_book_age_ms,
            policy.max_ingest_lag_ms,
        );
        snapshot.worst_ingest_lag_ms = worst_ingest_lag_ms;
        snapshot.ingest_lag_exceeded = ingest_lag_exceeded;

        let mut worst_book_age_ms = 0_u64;
        for (token_id, book) in self.book_store.published_snapshots() {
            // Prefer the local WS receipt clock (no venue clock skew / reconnect
            // re-write artifacts); fall back to the venue timestamp age.
            let book_age_ms = self
                .ws_health
                .token_message_age_ms(&token_id)
                .unwrap_or_else(|| now_ms.saturating_sub(book.timestamp_ms));
            worst_book_age_ms = worst_book_age_ms.max(book_age_ms);
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
                connection_healthy,
            });
            snapshot.tally(report.status);
        }
        snapshot.worst_book_age_ms = worst_book_age_ms;
        snapshot
    }
}

impl DataQualityPort for BookDataQualityService {
    fn snapshot(&self) -> DataQualitySnapshot {
        DataQualityService::snapshot(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policy() -> QualityPolicy {
        QualityPolicy::from_config(&DataQualityConfig::default())
    }

    #[test]
    fn empty_book_is_insufficient_regardless_of_connection() {
        let status =
            BookDataQualityService::status_for(StalenessLevel::Fresh, false, true, true, &policy());
        assert_eq!(status, DataQualityStatus::Insufficient);
    }

    #[test]
    fn crossed_book_is_degraded() {
        let status =
            BookDataQualityService::status_for(StalenessLevel::Fresh, true, false, true, &policy());
        assert_eq!(status, DataQualityStatus::Degraded);
    }

    #[test]
    fn fresh_and_acceptable_pass_through() {
        assert_eq!(
            BookDataQualityService::status_for(
                StalenessLevel::Fresh,
                false,
                false,
                true,
                &policy()
            ),
            DataQualityStatus::Fresh,
        );
        assert_eq!(
            BookDataQualityService::status_for(
                StalenessLevel::Acceptable,
                false,
                false,
                true,
                &policy(),
            ),
            DataQualityStatus::Acceptable,
        );
    }

    #[test]
    fn quiet_but_valid_book_stays_acceptable_when_connection_healthy() {
        // On Polymarket a quiet book is not resent; an aged-but-valid book is
        // still the venue truth while the connection is healthy.
        for level in [StalenessLevel::Stale, StalenessLevel::Expired] {
            assert_eq!(
                BookDataQualityService::status_for(level, false, false, true, &policy()),
                DataQualityStatus::Acceptable,
            );
        }
    }

    #[test]
    fn aged_book_is_stale_only_when_connection_unhealthy() {
        for level in [StalenessLevel::Stale, StalenessLevel::Expired] {
            assert_eq!(
                BookDataQualityService::status_for(level, false, false, false, &policy()),
                DataQualityStatus::Stale,
            );
        }
    }
}
