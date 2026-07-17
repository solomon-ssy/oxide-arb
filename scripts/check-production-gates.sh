#!/usr/bin/env bash
# Production promotion gate. Expensive by design; run before enabling any
# risk-increasing runtime capability.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
bash scripts/lint-architecture.sh
bash scripts/lint-import-style.sh
bash scripts/lint-quant-pivot-boundary.sh
bash scripts/lint-quant-pivot-errors.sh
bash scripts/lint-dead-semantics.sh
bash scripts/lint-clickhouse-correctness.sh
cargo test --workspace
cargo test-network
cargo test-docker

(
  cd ui
  pnpm lint
  pnpm check:circular
  pnpm check:dep
  pnpm check:type
  pnpm test:unit
  pnpm build:antdv-next

  DIST=apps/web-antdv-next/dist
  if rg -n -i 'mock-napi|[?&]token=|Bearer%20|VITE_APP_STORE_SECURE_KEY|REPLACE_WITH|hm\.baidu\.com|@vbenjs/static-source|Oxide Arb|Vben Admin|www\.vben\.pro|ann\.vben|open\.dingtalk' "$DIST"; then
    echo "ERROR: forbidden production bundle pattern found"
    exit 1
  fi
)

bash scripts/check-bench-slo.sh
bash scripts/check-bench-regression.sh
cargo bench -p quant-pivot-bench --bench e2e_paths -- --output-format bencher
cargo test -p quant-pivot-core --test production_soak \
  five_hundred_markets_thousand_tokens_ingest_without_book_drops \
  -- --ignored --exact --nocapture
