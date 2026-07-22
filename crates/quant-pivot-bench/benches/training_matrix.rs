//! Classical training-matrix transform benchmark.

use std::{env, time::Duration};

use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};
use quant_pivot_bench::{TRAINING_MATRIX_COLUMNS, training_matrix_fixture};
use quant_pivot_research::model::artifact::FittedInputTransform;

const DEFAULT_ROWS: usize = 10_000;

fn configured_rows() -> usize {
    env::var("QP_TRAINING_MATRIX_ROWS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(DEFAULT_ROWS)
}

fn bench_training_matrix(c: &mut Criterion) {
    let rows = configured_rows();
    let elements = u64::try_from(
        rows.checked_mul(TRAINING_MATRIX_COLUMNS)
            .expect("fixture shape fits usize"),
    )
    .expect("fixture element count fits u64");
    let mut group = c.benchmark_group("training_matrix");
    group.throughput(Throughput::Elements(elements));
    group.sample_size(10);
    group.warm_up_time(Duration::from_secs(2));
    group.measurement_time(Duration::from_secs(5));
    group.bench_function(format!("fit_{rows}x{TRAINING_MATRIX_COLUMNS}"), |bencher| {
        bencher.iter_batched(
            || training_matrix_fixture(rows).expect("training matrix fixture"),
            |matrix| FittedInputTransform::fit(&matrix),
            BatchSize::LargeInput,
        );
    });
    group.finish();
}

criterion_group!(benches, bench_training_matrix);
criterion_main!(benches);
