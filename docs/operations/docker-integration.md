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
cargo test -p oxide-arb-core --test gamma_service_sync -- --ignored --test-threads=1
```

## What each suite covers

| Crate | Test binary | Services |
|-------|-------------|----------|
| `oxide-arb-repository` | `pg_repository` | Postgres |
| `oxide-arb-repository` | `ch_timeseries` | ClickHouse |
| `oxide-arb-storage` | `migration_pg` | Postgres |
| `oxide-arb-storage` | `redis_integration` | Redis |
| `oxide-arb-storage` | `clickhouse_integration` | ClickHouse |
| `oxide-arb-storage` | `cache_tiered_integration` | Redis |
| `oxide-arb-core` | `gamma_service_sync` | Postgres + Redis (wiremock Gamma) |

Implementation lives in `crates/oxide-arb-xtask` (`cargo xtask test-docker`).

## Do not use workspace-wide `--ignored`

```bash
# Avoid — also runs live Polymarket / RPC tests in oxide-arb-api
cargo test --workspace -- --ignored
```

Network and credential-dependent tests live under `oxide-arb-api` with different ignore reasons. See [network-integration.md](./network-integration.md).

## CI

The `integration-docker` job in `.github/workflows/ci.yml` runs `cargo test-docker` on every push and pull request to `main`.

## Troubleshooting

**`unauthorized: incorrect username or password` when pulling images**

Docker Hub rate limits or stale credentials in Docker Desktop. Fix: `docker logout` then pull again, or log in with `docker login`. First run also needs network access to pull `postgres`, `redis`, and `clickhouse/clickhouse-server` images.

**`Docker daemon is not running`**

Start Docker Desktop / Colima before running `cargo test-docker`.

## Test tiers (summary)

| Tier | Command | Requires |
|------|---------|----------|
| Unit (default) | `cargo test --workspace` | nothing |
| Docker | `cargo test-docker` | Docker daemon |
| Network / live | `cargo test -p oxide-arb-api --features integration -- --ignored --test-threads=1` | outbound network + secrets |
