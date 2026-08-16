//! Single-shot 1M-row classical model train plus frozen-transform replay gate.

use std::{env, error::Error, time::Instant};

use quant_pivot_bench::{enforce_linux_peak_rss, peak_rss_bytes, training_matrix_fixture};
use quant_pivot_compute::{ComputeExecutor, OfflineMemory};
use quant_pivot_error::{QuantResult, research::ResearchError};
use quant_pivot_models::enums::model::ClassicalKind;
use quant_pivot_research::model::{ClassicalAdapterRegistry, replay_training_matrix};

const DEFAULT_ROWS: usize = 1_000_000;
const MAX_SECONDS: f64 = 300.0;
const MAX_RSS_BYTES: u64 = 10 * 1_024 * 1_024 * 1_024;

struct GateResult {
    train_seconds: f64,
    replay_seconds: f64,
    prediction_count: usize,
    prediction_checksum: f64,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    let rows = env::args()
        .nth(1)
        .map(|value| value.parse())
        .transpose()?
        .unwrap_or(DEFAULT_ROWS);
    let executor = ComputeExecutor::new()?;
    let total_started = Instant::now();
    let result = executor
        .run_offline(OfflineMemory::try_gib(10)?, move || run_gate(rows))
        .await?;
    let total_seconds = total_started.elapsed().as_secs_f64();
    let peak_rss = peak_rss_bytes()?;
    let peak_rss_label = peak_rss.map_or_else(|| "unavailable".to_owned(), |rss| rss.to_string());

    println!(
        "model_train_replay_gate rows={rows} kind=logistic_regression train_seconds={:.3} replay_seconds={:.3} total_seconds={total_seconds:.3} prediction_count={} prediction_checksum={:.9} peak_rss_bytes={peak_rss_label}",
        result.train_seconds,
        result.replay_seconds,
        result.prediction_count,
        result.prediction_checksum,
    );
    if rows == DEFAULT_ROWS {
        if total_seconds > MAX_SECONDS {
            return Err(format!(
                "model train/replay hard gate exceeded {MAX_SECONDS}s: {total_seconds:.3}s"
            )
            .into());
        }
        enforce_linux_peak_rss(peak_rss, MAX_RSS_BYTES, "model train/replay")?;
    }
    Ok(())
}

fn run_gate(rows: usize) -> QuantResult<GateResult> {
    let matrix = training_matrix_fixture(rows)?;
    let adapter = ClassicalAdapterRegistry::adapter_for(ClassicalKind::LogisticRegression);
    let train_started = Instant::now();
    let output = adapter.train(&matrix)?;
    let train_seconds = train_started.elapsed().as_secs_f64();

    let replay_started = Instant::now();
    let predictions = replay_training_matrix(&output, &matrix)?;
    let replay_seconds = replay_started.elapsed().as_secs_f64();
    if predictions.len() != rows {
        return Err(ResearchError::MatrixBuild {
            detail: format!(
                "model train/replay gate produced {} predictions for {rows} rows",
                predictions.len()
            ),
        }
        .into());
    }
    let prediction_checksum: f64 = predictions.iter().sum();
    if !prediction_checksum.is_finite() {
        return Err(ResearchError::MatrixBuild {
            detail: "model train/replay gate checksum is non-finite".to_owned(),
        }
        .into());
    }
    Ok(GateResult {
        train_seconds,
        replay_seconds,
        prediction_count: predictions.len(),
        prediction_checksum,
    })
}
