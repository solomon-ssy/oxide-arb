//! Single-shot time/RSS gate for 1M-row, ten-partition CPCV orchestration.

use std::{env, error::Error, time::Instant};

use chrono::{DateTime, Utc};
use quant_pivot_bench as _;
use quant_pivot_error::{QuantResult, research::ResearchError};
use quant_pivot_models::types::BacktestPathSetId;
use quant_pivot_research::validation::{
    CombinatorialPurgedBacktester, CpcvConfig, CpcvRequest, DefaultCombinatorialPurgedBacktester,
    FoldModelSource, FoldRuntime, GroupEvaluation, GroupRowFilter, PolicyFoldRuntime, PurgeConfig,
    ReplayEngine, TimelineGroup,
};
use rayon::ThreadPoolBuilder;
use rust_decimal::Decimal;

const DEFAULT_ROWS: usize = 1_000_000;
const MAX_SECONDS: f64 = 300.0;
const PARTITIONS: u32 = 10;
const TEST_PARTITIONS: u32 = 2;
const OFFLINE_THREADS: usize = 2;

struct GateFoldSource;

impl FoldModelSource for GateFoldSource {
    fn train_fold(&self, filter: &GroupRowFilter) -> QuantResult<FoldRuntime> {
        let training_group_count = u64::try_from(filter.group_indices.len()).map_err(|error| {
            ResearchError::ValidationMethodology {
                detail: format!("gate training group count does not fit u64: {error}"),
            }
        })?;
        Ok(FoldRuntime::Policy(PolicyFoldRuntime {
            candidate_index: 0,
            candidate_id: "cpcv-gate".to_owned(),
            training_group_count,
            training_utility_bps: Decimal::ZERO,
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
                    rank_observations: Vec::new(),
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
                .ok_or("CPCV gate timestamp exceeds chrono range")?;
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
        return Err("CPCV gate result shape mismatch".into());
    }
    println!(
        "cpcv_gate rows={rows} partitions={PARTITIONS} combinations={} paths={} elapsed_seconds={:.3}",
        result.combination_count,
        result.paths.len(),
        elapsed.as_secs_f64()
    );
    if rows == DEFAULT_ROWS && elapsed.as_secs_f64() > MAX_SECONDS {
        return Err(format!(
            "CPCV hard gate exceeded {MAX_SECONDS}s: {:.3}s",
            elapsed.as_secs_f64()
        )
        .into());
    }
    Ok(())
}
