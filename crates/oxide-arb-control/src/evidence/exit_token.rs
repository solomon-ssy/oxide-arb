use chrono::{TimeZone, Utc};
use oxide_arb_models::{
    clickhouse::OpportunityAuditRow,
    domain::{
        control_factor::{EvidenceSourceBundle, QueryFingerprint, SimulationConfig},
        evidence::EvidenceMetric,
    },
    enums::clickhouse::{ChAuditOutcome, ChOpportunityAuditStage, ChSettlementOutcome, ChSide},
    types::Shares,
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::evidence::book::BookReconstructionArtifact;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExitTokenEvidenceArtifact {
    pub report: ExitTokenEvidenceReport,
    pub report_only: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExitTokenEvidenceReport {
    pub historical_filled_position_count: u64,
    pub sell_side_book_coverage_bps: u64,
    pub executable_exit_rate_bps: EvidenceMetric<u64>,
    pub false_exit_count: EvidenceMetric<u64>,
    pub avoided_tail_loss_count: EvidenceMetric<u64>,
    pub token_inventory_consistency_bps: EvidenceMetric<u64>,
    pub insufficient_reasons: Vec<String>,
    pub query_fingerprints: Vec<QueryFingerprint>,
}

#[must_use]
pub fn build(
    book: &BookReconstructionArtifact,
    audits: &[OpportunityAuditRow],
    query_fingerprints: Vec<QueryFingerprint>,
    source_bundle: &EvidenceSourceBundle,
    simulation_config: &SimulationConfig,
) -> ExitTokenEvidenceArtifact {
    let filled = audits
        .iter()
        .filter(|row| matches!(row.outcome, Some(ChAuditOutcome::Success)))
        .collect::<Vec<_>>();
    let executable_exits = filled
        .iter()
        .filter(|row| executable_exit(book, row, source_bundle, simulation_config))
        .count()
        .try_into()
        .unwrap_or(u64::MAX);
    let filled_count = u64::try_from(filled.len()).unwrap_or(u64::MAX);
    let settlement_rows = audits
        .iter()
        .filter(|row| row.stage == ChOpportunityAuditStage::Settled)
        .collect::<Vec<_>>();
    let false_exit_count = filled
        .iter()
        .filter(|filled| {
            executable_exit(book, filled, source_bundle, simulation_config)
                && settlement_rows.iter().any(|settled| {
                    settled.opportunity_id == filled.opportunity_id
                        && settled.settlement_status == Some(ChSettlementOutcome::Won)
                })
        })
        .count()
        .try_into()
        .unwrap_or(u64::MAX);
    let avoided_tail_loss_count = filled
        .iter()
        .filter(|filled| {
            executable_exit(book, filled, source_bundle, simulation_config)
                && settlement_rows.iter().any(|settled| {
                    settled.opportunity_id == filled.opportunity_id
                        && settled.settlement_status == Some(ChSettlementOutcome::Lost)
                })
        })
        .count()
        .try_into()
        .unwrap_or(u64::MAX);
    let mut insufficient_reasons = Vec::new();
    if filled_count > 0 && executable_exits < filled_count {
        insufficient_reasons
            .push("exit.book_coverage_insufficient: sell-side bid books are incomplete".to_owned());
    }
    insufficient_reasons.push(
        "exit.report_only: auto-exit requires token-level reconciliation and exit accounting shadow evidence".to_owned(),
    );
    ExitTokenEvidenceArtifact {
        report: ExitTokenEvidenceReport {
            historical_filled_position_count: filled_count,
            sell_side_book_coverage_bps: executable_exits
                .saturating_mul(10_000)
                .checked_div(filled_count)
                .unwrap_or(0),
            executable_exit_rate_bps: if filled_count > 0
                && !source_bundle.token_balance_snapshots.is_empty()
            {
                EvidenceMetric::Available {
                    value: executable_exits.saturating_mul(10_000) / filled_count,
                }
            } else {
                EvidenceMetric::Unavailable {
                    code: "exit.accounting_model_missing".to_owned(),
                    reason:
                        "executable exit rate requires filled positions and token balance snapshots"
                            .to_owned(),
                }
            },
            false_exit_count: if settlement_rows.is_empty() {
                EvidenceMetric::Unavailable {
                    code: "exit.shadow_outcomes_missing".to_owned(),
                    reason: "false exit count requires settlement labels".to_owned(),
                }
            } else {
                EvidenceMetric::Available {
                    value: false_exit_count,
                }
            },
            avoided_tail_loss_count: if settlement_rows.is_empty() {
                EvidenceMetric::Unavailable {
                    code: "exit.tail_loss_labels_missing".to_owned(),
                    reason: "avoided tail loss requires hold-to-resolution outcome comparison"
                        .to_owned(),
                }
            } else {
                EvidenceMetric::Available {
                    value: avoided_tail_loss_count,
                }
            },
            token_inventory_consistency_bps: inventory_consistency_bps(source_bundle),
            insufficient_reasons,
            query_fingerprints,
        },
        report_only: true,
    }
}

fn inventory_consistency_bps(source_bundle: &EvidenceSourceBundle) -> EvidenceMetric<u64> {
    if source_bundle.token_balance_snapshots.is_empty() {
        return EvidenceMetric::Unavailable {
            code: "token.inventory_reconciliation_missing".to_owned(),
            reason: "token inventory consistency requires token-level balance reconciliation"
                .to_owned(),
        };
    }
    let consistent_count = source_bundle
        .token_balance_snapshots
        .iter()
        .filter(|snapshot| {
            snapshot
                .drift_shares
                .is_some_and(|drift| drift.inner() == Decimal::ZERO)
        })
        .count()
        .try_into()
        .unwrap_or(u64::MAX);
    let total_count =
        u64::try_from(source_bundle.token_balance_snapshots.len()).unwrap_or(u64::MAX);
    EvidenceMetric::Available {
        value: consistent_count
            .saturating_mul(10_000)
            .checked_div(total_count)
            .unwrap_or(0),
    }
}

fn executable_exit(
    book: &BookReconstructionArtifact,
    audit: &OpportunityAuditRow,
    source_bundle: &EvidenceSourceBundle,
    simulation_config: &SimulationConfig,
) -> bool {
    if !simulation_config.exit_policy.enabled {
        return false;
    }
    let Some(inventory) = source_bundle
        .token_balance_snapshots
        .iter()
        .find(|snapshot| snapshot.token_id == audit.token_id)
        .map(|snapshot| snapshot.internal_shares)
    else {
        return false;
    };
    let min_depth = simulation_config
        .exit_policy
        .min_bid_depth_shares
        .map_or(inventory, Shares::new);
    exit_candidate_times(audit, simulation_config)
        .into_iter()
        .any(|decision_time| {
            let Some(token_book) = book.token_book_at(&audit.token_id, decision_time) else {
                return false;
            };
            match audit.side {
                ChSide::Buy => {
                    token_book
                        .bids
                        .iter()
                        .map(|level| level.size_decimal().inner())
                        .sum::<Decimal>()
                        >= min_depth.inner()
                }
                ChSide::Sell => {
                    token_book
                        .asks
                        .iter()
                        .map(|level| level.size_decimal().inner())
                        .sum::<Decimal>()
                        >= min_depth.inner()
                }
            }
        })
}

fn exit_candidate_times(
    audit: &OpportunityAuditRow,
    simulation_config: &SimulationConfig,
) -> Vec<chrono::DateTime<Utc>> {
    let Some(entry_time) = Utc.timestamp_millis_opt(audit.stage_at).single() else {
        return Vec::new();
    };
    let mut times = Vec::new();
    if simulation_config.exit_policy.fixed_stop_bps.is_some() {
        times.push(entry_time);
    }
    if simulation_config.exit_policy.trailing_stop_bps.is_some() {
        times.push(entry_time);
    }
    if let Some(stop_secs) = simulation_config.exit_policy.time_stop_secs {
        times.push(
            entry_time + chrono::Duration::seconds(i64::try_from(stop_secs).unwrap_or(i64::MAX)),
        );
    }
    if let Some(grace_secs) = simulation_config.exit_policy.zone_invalidation_grace_secs {
        times.push(
            entry_time + chrono::Duration::seconds(i64::try_from(grace_secs).unwrap_or(i64::MAX)),
        );
    }
    times.sort_unstable();
    times.dedup();
    times
}
