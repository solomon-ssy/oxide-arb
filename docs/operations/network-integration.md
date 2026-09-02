# Network integration verification

The current `quant-pivot-api` tests are deterministic client, protocol, retry, and
credential-file contracts. The crate has no ignored live-authentication, FOK, or
WebSocket probe. Do not interpret `--ignored` as a command for live-venue acceptance.

## Deterministic contracts

```bash
cargo test -p quant-pivot-api
cargo test --workspace
```

Provider contracts use owned loopback fixtures. The workspace also runs system
tests with disposable PostgreSQL, Redis, ClickHouse, object storage, and the real
production binary. Their success proves the exercised implementation contracts,
not connectivity to a current production deployment or permission to move money.

The two ignored `vertical_readiness_evidence` tests in `quant-pivot-system-tests`
require explicitly pinned evidence artifacts and current deployment inputs.
They are readiness evidence generators, not live-money canaries; running them
does not grant Operational Activation.

## Implementation Closure and Operational Activation

The [Phase 12 acceptance contract](../plans/quant-pivot/phase-12/12.0-execution-authority-account-recovery-fast-feedback.md#9-验收)
separately requires the retained disposable feedback-closure rehearsal, two fresh
UI runs, and the documented static/test gates. Default workspace test success does
not replace those independent results. Recover current status only from the
[implementation ledger](../plans/quant-pivot/phase-12/12.1-implementation-ledger.md).

Real venue orders, chain approval/redeem, relayer requests, and governed canaries
belong to the independent [Operational Activation checklist](./runbook.md).
They require authorization for the exact account, route, action, amount, and
deployment digest, together with the specified preflight and recovery evidence.
Neither a local test result nor a generic ignored-test command authorizes them.

## CI

The current [`.github/workflows/ci.yml`](../../.github/workflows/ci.yml) has no
credentialed `network-integration` job. Deterministic provider contracts run in
the Rust unit/contract partition, and disposable infrastructure contracts run in
the system partition. Claim only the exact commands and evidence that actually
completed; do not report these partitions as production or real-money validation.

## Related

Postgres / Redis / ClickHouse tests that use testcontainers are a separate tier — see [docker-integration.md](./docker-integration.md).
