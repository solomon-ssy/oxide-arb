#!/usr/bin/env bash
# Canonical production promotion gates. CI invokes the same named groups.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
GROUP="${1:-all}"
cd "$ROOT"

rust_static() {
  cargo fmt --all -- --check
  cargo clippy --workspace --all-targets -- -D warnings
  bash scripts/lint-architecture.sh
  bash scripts/lint-import-style.sh
  bash scripts/lint-quant-pivot-boundary.sh
  bash scripts/lint-quant-pivot-errors.sh
  bash scripts/lint-dead-semantics.sh
  bash scripts/lint-clickhouse-correctness.sh
  bash scripts/lint-training-serving-parity.sh
  bash scripts/lint-phase-lifecycle.sh
  bash scripts/lint-config-inventory.sh
  bash scripts/lint-seaorm-persistence.sh
  bash scripts/lint-secret-boundaries.sh
  cargo machete --with-metadata
  cargo +nightly udeps --workspace --all-targets
  cargo test --workspace
  cargo build -p quant-pivot-bin
  cargo build -p quant-pivot-bin --features lp-solver,optimize,ml-classical
  cargo clippy -p quant-pivot-research --features ml-classical,optimize,dataframe --all-targets -- -D warnings
  cargo clippy -p quant-pivot-core --features ml-classical,optimize --all-targets -- -D warnings
  cargo test -p quant-pivot-research --features ml-classical,optimize,dataframe
  cargo test -p quant-pivot-core --features ml-classical,optimize
  cargo bench -p quant-pivot-bench --no-run
  cargo bench -p quant-pivot-bench --bench e2e_paths -- --output-format bencher
}

ui_gate() {
  bash scripts/lint-ui-semantic-colors.sh
  (
    cd ui
    pnpm check:config-api
    pnpm lint
    pnpm check:circular
    pnpm check:dep
    pnpm check:type
    pnpm test:unit
    pnpm build:antdv-next
  )
  bash scripts/check-ui-production-bundle.sh
}

network_gate() {
  cargo test-network
}

docker_gate() {
  cargo test-docker
  cargo test-docker
}

protected_e2e() {
  (
    cd ui
    pnpm exec playwright test apps/web-antdv-next/tests/e2e/phase-11-7-protected-flow.spec.ts
  )
}

case "$GROUP" in
  rust-static)
    rust_static
    ;;
  ui)
    ui_gate
    ;;
  network)
    network_gate
    ;;
  docker)
    docker_gate
    ;;
  protected-e2e)
    protected_e2e
    ;;
  all)
    rust_static
    ui_gate
    network_gate
    docker_gate
    protected_e2e
    ;;
  *)
    echo "usage: $0 {rust-static|ui|network|docker|protected-e2e|all}" >&2
    exit 2
    ;;
esac
