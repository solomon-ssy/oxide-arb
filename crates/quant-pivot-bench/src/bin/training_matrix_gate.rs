//! Single-shot time/RSS gate for the governed 1M×128 training transform.

use std::{env, error::Error, time::Instant};

use quant_pivot_bench::{
    TRAINING_MATRIX_COLUMNS, enforce_linux_peak_rss, peak_rss_bytes, training_matrix_fixture,
};
use quant_pivot_research::model::artifact::FittedInputTransform;

const DEFAULT_ROWS: usize = 1_000_000;
const MAX_SECONDS: f64 = 60.0;
const MAX_RSS_BYTES: u64 = 8 * 1_024 * 1_024 * 1_024;

fn main() -> Result<(), Box<dyn Error>> {
    let rows = env::args()
        .nth(1)
        .map(|value| value.parse())
        .transpose()?
        .unwrap_or(DEFAULT_ROWS);
    let matrix = training_matrix_fixture(rows)?;
    let started = Instant::now();
    let (_transform, dense) = FittedInputTransform::fit(&matrix)?;
    let elapsed = started.elapsed();
    if dense.row_count() != rows {
        return Err("dense transform row count mismatch".into());
    }
    let peak_rss = peak_rss_bytes()?;
    let peak_rss_label = peak_rss.map_or_else(|| "unavailable".to_owned(), |rss| rss.to_string());
    println!(
        "training_matrix_gate rows={rows} columns={TRAINING_MATRIX_COLUMNS} elapsed_seconds={:.3} peak_rss_bytes={peak_rss_label}",
        elapsed.as_secs_f64()
    );
    if rows == DEFAULT_ROWS {
        if elapsed.as_secs_f64() > MAX_SECONDS {
            return Err(format!(
                "training matrix hard gate exceeded {MAX_SECONDS}s: {:.3}s",
                elapsed.as_secs_f64()
            )
            .into());
        }
        enforce_linux_peak_rss(peak_rss, MAX_RSS_BYTES, "training matrix")?;
    }
    Ok(())
}
