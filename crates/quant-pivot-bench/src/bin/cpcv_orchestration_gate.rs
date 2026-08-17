//! Single-shot time/RSS gate for the 1M-row CPCV orchestration kernel.

use std::{env, error::Error, time::Instant};

use chrono::{DateTime, Utc};
use quant_pivot_bench::{self as _, enforce_linux_peak_rss, peak_rss_bytes};
use quant_pivot_error::{QuantResult, research::ResearchError};
use quant_pivot_models::types::BacktestPathSetId;
use quant_pivot_research::validation::{
    CombinatorialPurgedBacktester, CpcvConfig, CpcvRequest, DefaultCombinatorialPurgedBacktester,
    FoldModelSource, FoldRuntime, FoldTrainingIdentity, FoldTrainingRequest, GroupEvaluation,
    GroupRowFilter, PolicyFoldRuntime, PurgeConfig, ReplayEngine, TimelineGroup,
};
use rayon::ThreadPoolBuilder;
use rust_decimal::Decimal;

const DEFAULT_ROWS: usize = 1_000_000;
const MAX_SECONDS: f64 = 300.0;
const MAX_RSS_BYTES: u64 = 10 * 1_024 * 1_024 * 1_024;
const PARTITIONS: u32 = 10;
const TEST_PARTITIONS: u32 = 2;
const OFFLINE_THREADS: usize = 2;

struct GateFoldSource;

impl FoldModelSource for GateFoldSource {
    fn train_fold(&self, request: FoldTrainingRequest<'_>) -> QuantResult<FoldRuntime> {
        let FoldTrainingIdentity::Validation {
            combination_index, ..
        } = request.identity
        else {
            return Err(ResearchError::ValidationMethodology {
                detail: "orchestration gate does not execute trial-grid estimators".to_owned(),
            }
            .into());
        };
        let filter = request.filter;
        let training_group_count = u64::try_from(filter.group_indices.len()).map_err(|error| {
            ResearchError::ValidationMethodology {
                detail: format!("gate training group count does not fit u64: {error}"),
            }
        })?;
        Ok(FoldRuntime::Policy(PolicyFoldRuntime {
            validation_fold_index: combination_index,
            candidate_index: 0,
            candidate_id: "cpcv-orchestration-gate".to_owned(),
            training_group_count,
            training_utility_bps: Decimal::ZERO,
            training_risk_utility_bps: Decimal::ZERO,
        }))
    }
}

struct GateReplay;

impl ReplayEngine for GateReplay {
    fn evaluate(
        &self,
        _model: &FoldRuntime,
        filter: &GroupRowFilter,
    ) -> QuantResult<Vec<GroupEvaluation>> {
        filter
            .group_indices
            .iter()
            .map(|&group_index| {
                let bucket = i64::try_from(group_index % 101).map_err(|error| {
                    ResearchError::ValidationMethodology {
                        detail: format!("gate return bucket does not fit i64: {error}"),
                    }
                })?;
                Ok(GroupEvaluation {
                    group_index,
                    return_value: Decimal::new(bucket - 50, 6),
                    risk_return_value: Decimal::new(bucket - 50, 6),
                    scenario_residual: None,
                    rank_observations: Vec::new(),
                    executed_turnover: None,
                    portfolio_replay: None,
                })
            })
            .collect()
    }
}

fn groups(rows: usize) -> Result<Vec<TimelineGroup>, Box<dyn Error>> {
    (0..rows)
        .map(|row| {
            let seconds = i64::try_from(row)?;
            let decision_at = DateTime::<Utc>::from_timestamp(seconds, 0)
                .ok_or("CPCV orchestration gate timestamp exceeds chrono range")?;
            Ok(TimelineGroup {
                decision_at,
                label_horizon_end: decision_at,
            })
        })
        .collect()
}

fn main() -> Result<(), Box<dyn Error>> {
    let rows = env::args()
        .nth(1)
        .map(|value| value.parse())
        .transpose()?
        .unwrap_or(DEFAULT_ROWS);
    let groups = groups(rows)?;
    let pool = ThreadPoolBuilder::new()
        .num_threads(OFFLINE_THREADS)
        .build()?;
    let started = Instant::now();
    let result = pool.install(|| {
        DefaultCombinatorialPurgedBacktester::new().run(CpcvRequest {
            path_set_id: BacktestPathSetId::from_v7(),
            groups: &groups,
            cpcv: CpcvConfig {
                n_groups: PARTITIONS,
                k_test: TEST_PARTITIONS,
            },
            purge: PurgeConfig::pct_only(Decimal::ZERO),
            fold_source: &GateFoldSource,
            replay: &GateReplay,
        })
    })?;
    let elapsed = started.elapsed();
    let exact_shape = result.combination_count == 45
        && result.paths.len() == 9
        && result
            .paths
            .iter()
            .all(|path| path.group_returns.len() == rows);
    if !exact_shape {
        return Err("CPCV orchestration gate result shape mismatch".into());
    }
    let peak_rss = peak_rss_bytes()?;
    let peak_rss_label = peak_rss.map_or_else(|| "unavailable".to_owned(), |rss| rss.to_string());
    println!(
        "cpcv_orchestration_gate rows={rows} partitions={PARTITIONS} combinations={} paths={} elapsed_seconds={:.3} peak_rss_bytes={peak_rss_label}",
        result.combination_count,
        result.paths.len(),
        elapsed.as_secs_f64()
    );
    if rows == DEFAULT_ROWS {
        if elapsed.as_secs_f64() > MAX_SECONDS {
            return Err(format!(
                "CPCV hard gate exceeded {MAX_SECONDS}s: {:.3}s",
                elapsed.as_secs_f64()
            )
            .into());
        }
        enforce_linux_peak_rss(peak_rss, MAX_RSS_BYTES, "CPCV")?;
    }
    Ok(())
}
