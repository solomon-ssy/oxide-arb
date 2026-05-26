use criterion::{Criterion, black_box, criterion_group, criterion_main};
use oxide_arb_algorithm::walker::OrderbookWalker;
use oxide_arb_core::observability::metrics_hub::MetricsHub;
use oxide_arb_core::pipeline::book_store::BookStore;
use oxide_arb_core::pipeline::dual_book_assembler::DualBookAssembler;
use oxide_arb_core::pipeline::order_book::OrderBook;
use oxide_arb_models::domain::book::{BookLevel, total_depth_usd};
use oxide_arb_models::types::{MicroPrice, MicroUsd, Price, Shares, TokenId};
use rust_decimal_macros::dec;
use std::sync::Arc;

fn sample_levels(n: usize) -> Vec<BookLevel> {
    (0..n)
        .map(|i| {
            BookLevel::from_decimal_unchecked(
                Price::new(dec!(0.95) + dec!(0.0001) * rust_decimal::Decimal::from(i)),
                Shares::new(dec!(100)),
            )
        })
        .collect()
}

fn bench_dual_book_assemble(c: &mut Criterion) {
    let metrics = Arc::new(MetricsHub::new());
    let store = BookStore::new(Arc::clone(&metrics));
    let yes = TokenId::new("yes");
    let no = TokenId::new("no");
    let levels = sample_levels(50);
    store.apply_snapshot(&yes, levels.clone(), levels, 1);
    let levels = sample_levels(50);
    store.apply_snapshot(&no, levels.clone(), levels, 1);

    c.bench_function("dual_book_assemble_50_levels", |b| {
        b.iter(|| DualBookAssembler::assemble(black_box(&store), &yes, &no));
    });
}

fn bench_walk_asks_by_cost(c: &mut Criterion) {
    let levels = sample_levels(50);
    let depth = total_depth_usd(&levels);
    let budget = MicroUsd::try_from_decimal(dec!(500)).unwrap();
    let floor = MicroPrice::try_from_decimal(dec!(0.95)).unwrap();
    c.bench_function("walk_asks_by_cost_50", |b| {
        b.iter(|| OrderbookWalker::walk_asks_by_cost(black_box(&levels), budget, floor, depth));
    });
}

fn bench_book_apply_snapshot(c: &mut Criterion) {
    let levels = sample_levels(50);
    c.bench_function("book_apply_snapshot_50", |b| {
        b.iter(|| {
            let mut ob = OrderBook::new(TokenId::new("t"));
            ob.apply_snapshot(levels.clone(), levels.clone(), 1);
            black_box(ob.publish());
        });
    });
}

criterion_group!(
    benches,
    bench_dual_book_assemble,
    bench_walk_asks_by_cost,
    bench_book_apply_snapshot
);
criterion_main!(benches);
