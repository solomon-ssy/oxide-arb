//! Detection funnel reject reasons for metrics and sampled diagnostics.
//!
//! Every `process_ref` call that does not emit a [`ScoredOpportunity`] returns
//! exactly one [`DetectionRejectReason`] so operators can see which gate dominates.

use crate::scorer::ScoredOpportunity;
use std::{fmt, sync::Arc};

/// Why a single market scan did not emit a scored opportunity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DetectionRejectReason {
    /// Catalog warmup gate (scanner only).
    CatalogNotReady,
    /// Operational lifecycle gate (scanner only).
    LifecycleGated,
    /// YES/NO pair missing from `BookStore` (scanner only).
    MissingBookPair,
    /// `BookGate` quality check failed (scanner only).
    BookGate,
    /// Active control-factor market/event/category anomaly gate.
    MarketAnomaly,
    /// Per-market emission cooldown still active.
    EmissionCooldown,
    /// Book staleness is `Expired` (not tradeable).
    StalenessExpired,
    /// Neither YES nor NO best ask reached `high_threshold`.
    NoConvergenceDirection,
    /// Settlement deadline outside `settlement_window_hours`.
    OutsideSettlementWindow,
    /// Convergence direction held but duration below `min_convergence_duration_secs`.
    ConvergenceInsufficient {
        elapsed_secs: u64,
        required_secs: u64,
    },
    /// `OrderbookWalker` could not fill under caps/threshold.
    WalkFailed,
    /// Per-share edge `1 - entry_vwap` below `min_profit_per_share`.
    MinProfitPerShare,
    /// Published bucket-risk factor blocked new entries.
    BucketRiskBlocked,
    /// Published bucket-risk factor edge floor not met.
    BucketRiskEdgeFloor,
    /// `expected_net_profit` below `min_profit_threshold_usd`.
    MinProfitThreshold,
    /// Walk depth usage above `max_depth_usage_pct`.
    MaxDepthUsage,
    /// Composite score below `min_score`.
    MinScore,
}

impl DetectionRejectReason {
    /// Stable Prometheus label (`reason` on `detection_scan_rejects_total`).
    #[must_use]
    pub const fn metric_label(self) -> &'static str {
        match self {
            Self::CatalogNotReady => "catalog_not_ready",
            Self::LifecycleGated => "lifecycle_gated",
            Self::MissingBookPair => "missing_book_pair",
            Self::BookGate => "book_gate",
            Self::MarketAnomaly => "market_anomaly",
            Self::EmissionCooldown => "emission_cooldown",
            Self::StalenessExpired => "staleness_expired",
            Self::NoConvergenceDirection => "no_convergence_direction",
            Self::OutsideSettlementWindow => "outside_settlement_window",
            Self::ConvergenceInsufficient { .. } => "convergence_insufficient",
            Self::WalkFailed => "walk_failed",
            Self::MinProfitPerShare => "min_profit_per_share",
            Self::BucketRiskBlocked => "bucket_risk_blocked",
            Self::BucketRiskEdgeFloor => "bucket_risk_edge_floor",
            Self::MinProfitThreshold => "min_profit_threshold",
            Self::MaxDepthUsage => "max_depth_usage",
            Self::MinScore => "min_score",
        }
    }
}

impl fmt::Display for DetectionRejectReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ConvergenceInsufficient {
                elapsed_secs,
                required_secs,
            } => write!(
                f,
                "convergence_insufficient(elapsed={elapsed_secs}s, required={required_secs}s)"
            ),
            other => f.write_str(other.metric_label()),
        }
    }
}

/// Result of one detection pipeline pass.
#[derive(Debug, Clone)]
pub struct DetectionProcessOutcome {
    pub opportunity: Option<Arc<ScoredOpportunity>>,
    pub reject: Option<DetectionRejectReason>,
}

impl DetectionProcessOutcome {
    /// Rejected at a specific gate.
    #[must_use]
    pub const fn rejected(reason: DetectionRejectReason) -> Self {
        Self {
            opportunity: None,
            reject: Some(reason),
        }
    }

    #[must_use]
    pub const fn is_emitted(&self) -> bool {
        self.opportunity.is_some()
    }
}
