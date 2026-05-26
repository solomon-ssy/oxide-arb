#!/usr/bin/env bash
# Optional PGO workflow for release builds.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

echo "== PGO generate =="
RUSTFLAGS="-Cprofile-generate=/tmp/oxide-arb-pgo" cargo build --profile release

echo "== Run benchmarks (warm profile) =="
cargo bench -p oxide-arb-bench --bench hot_paths || true
cargo bench -p oxide-arb-core --bench pipeline_bench || true

echo "== PGO use =="
RUSTFLAGS="-Cprofile-use=/tmp/oxide-arb-pgo" cargo build --profile release

echo "Done. Compare release binary size/latency against non-PGO baseline."
