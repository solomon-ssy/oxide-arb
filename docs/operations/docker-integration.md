# Docker integration tests

Tests that spin up real Postgres, Redis, or ClickHouse via [testcontainers](https://github.com/testcontainers/testcontainers-rs) are **ignored by default** so `cargo test --workspace` stays fast and does not require Docker.

## Prerequisites

- Docker **daemon** running (Docker Desktop, Colima, etc.). Installing the CLI alone is not enough.
- Sufficient disk/RAM for container images on first run.

## Run all Docker integration tests

From the repo root:

```bash
cargo test-docker
```

Tests run **serially** (`--test-threads=1`) to avoid port and resource contention between containers.

## Run a single suite

```bash
cargo test -p quant-pivot-core --test gamma_service_sync -- --ignored --test-threads=1
```

## What each suite covers

| Crate | Test binary | Services |
|-------|-------------|----------|
| `quant-pivot-repository` | `pg_repository` | Postgres |
| `quant-pivot-repository` | `pg_rbac` | Postgres |
| `quant-pivot-repository` | `ch_timeseries` | ClickHouse |
| `quant-pivot-storage` | `migration_pg` | Postgres |
| `quant-pivot-storage` | `redis_integration` | Redis |
| `quant-pivot-storage` | `clickhouse_integration` | ClickHouse |
| `quant-pivot-storage` | `cache_tiered_integration` | Redis |
| `quant-pivot-core` | `gamma_service_sync` | Postgres + Redis (wiremock Gamma) |
| `quant-pivot-web` | `web` | Postgres + Redis (full HTTP/RBAC/WS E2E) |

Implementation lives in `crates/quant-pivot-xtask` (`cargo xtask test-docker`).

## Do not use workspace-wide `--ignored`

```bash
# Avoid — also runs live Polymarket / RPC tests in quant-pivot-api
cargo test --workspace -- --ignored
```

Network and credential-dependent tests live under `quant-pivot-api` with different ignore reasons. See [network-integration.md](./network-integration.md).

## CI

The `integration-docker` job in `.github/workflows/ci.yml` runs `cargo test-docker` on every push and pull request to `main`.

## Troubleshooting

**`unauthorized: incorrect username or password` when pulling images**

Docker Hub rate limits or stale credentials in Docker Desktop. Fix: `docker logout` then pull again, or log in with `docker login`. First run also needs network access to pull `postgres`, `redis`, and `clickhouse/clickhouse-server` images.

**`Docker daemon is not running`**

Start Docker Desktop / Colima before running `cargo test-docker`.

**`quant-pivot-web` auth tests: login 200 but refresh/logout 503**

Usually Redis testcontainer cold-start or pool pressure under parallel runs. The web harness connects the shared Redis pool (`connect_pool`) and waits for a successful blacklist `health_check` before serving requests. Prefer `cargo test-docker` (serial) over parallel `cargo test -p quant-pivot-web --test web -- --ignored` without `--test-threads=1`.

## Test tiers (summary)

| Tier | Command | Requires |
|------|---------|----------|
| Unit (default) | `cargo test --workspace` | nothing |
| Docker | `cargo test-docker` | Docker daemon |
| Network / live | `cargo test -p quant-pivot-api --features integration -- --ignored --test-threads=1` | outbound network + secrets |
