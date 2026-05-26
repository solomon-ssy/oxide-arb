#!/usr/bin/env bash
# Profile-guided optimization workflow for release builds.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

PGO_DIR="${PGO_DIR:-target/pgo-profiles}"
mkdir -p "$PGO_DIR"

echo "== PGO instrument (release-with-debug) =="
RUSTFLAGS="-Cprofile-generate=$PGO_DIR" \
  cargo build --profile release-with-debug -p oxide-arb-bin

echo "== Train: hot_paths benchmarks =="
cargo bench -p oxide-arb-bench --bench hot_paths

echo "== Train: integration tests =="
cargo test -p oxide-arb-core --test hot_path_integration --test execution_integration

echo "== Merge profiles =="
llvm-profdata merge -o "$PGO_DIR/merged.profdata" "$PGO_DIR"/*.profraw

echo "== PGO use (release) =="
RUSTFLAGS="-Cprofile-use=$PGO_DIR/merged.profdata" \
  cargo build --profile release -p oxide-arb-bin

echo "Done. Compare release binary latency against non-PGO baseline."
