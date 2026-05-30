#!/usr/bin/env bash
# Production promotion gate. Expensive by design; run before enabling Live.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
bash scripts/lint-architecture.sh
cargo test --workspace
cargo test-network
cargo test-docker
bash scripts/check-bench-slo.sh
bash scripts/check-bench-regression.sh
cargo bench -p oxide-arb-bench --bench e2e_paths -- --output-format bencher
cargo test -p oxide-arb-core --test production_soak \
  five_hundred_markets_thousand_tokens_ingest_without_book_drops \
  -- --ignored --exact --nocapture
