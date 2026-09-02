# Disposable infrastructure system tests

The current Docker-backed integration owner is `quant-pivot-system-tests`. Its suites share owned
disposable PostgreSQL, Redis, and ClickHouse infrastructure through the repository test harness.
There is no `cargo test-docker` alias and no `cargo xtask test-docker` command.

## Prerequisites

- A running Docker daemon (Docker Desktop, Colima, OrbStack, or equivalent).
- Enough disk/RAM to start the pinned PostgreSQL and ClickHouse images.
- `cargo-nextest` when reproducing the exact CI partition.

## Canonical full system command

From the repository root:

```bash
cargo nextest run -p quant-pivot-system-tests --profile system
```

This is the exact command owned by the `system` job in
[`.github/workflows/ci.yml`](../../.github/workflows/ci.yml). The profile and test harness own
serialization, shared-stack lifetime, evidence capture, and failure cleanup; do not emulate them by
running every workspace `#[ignore]` test.

## Targeted reproduction

Use the concrete system-test binary or filter that failed, for example:

```bash
cargo test -p quant-pivot-system-tests --test infrastructure_contracts \
  infrastructure_contracts_share_stack -- --nocapture
```

The disposable ClickHouse copies the exact
`docker/clickhouse/config.d/quant-pivot-governance.xml` file mounted by the production Compose
service. Startup verifies the bounded background pools and rejects high-churn system-log tables;
tests must not launch a default 26.5 image that bypasses this contract.

The Phase 12 governed feedback rehearsal has a separate production-composed command:

```bash
cargo xtask production-stack feedback-closure --runs 1 --retain-artifacts
```

That rehearsal must remain inside owned disposable infrastructure and loopback rejectors. Its
manifest must prove unchanged runtime authority/money-path counts and zero venue, chain, capital,
and relayer writes; it is not a live canary or Operational Activation.

The system partition also owns two deliberately separate artifact-store contracts:

- the pinned MinIO fixture proves versioned Object Lock writes and its provider-native global
  stale-multipart sweep, including an uploaded part becoming `NoSuchUpload` under a shortened test
  policy;
- the AWS SDK protocol test proves the production S3 rule is exactly prefix-scoped
  `AbortIncompleteMultipartUpload(days_after_initiation = 1)` on PUT and survives an independent GET
  readback without expiration, transition, legacy-prefix, tag, or size-filter fields.

MinIO does not persist the standard S3 abort action. Do not add a dummy object-expiration action to
make its lifecycle PUT succeed: MinIO drops the abort member on readback and the extra action changes
object-retention semantics.

Target either contract without running the production rehearsal:

```bash
cargo test -p quant-pivot-system-tests --lib s3_lifecycle_roundtrips -- --nocapture
cargo test -p quant-pivot-system-tests --lib minio_sweeps_stale_upload -- --nocapture
```

## Do not use workspace-wide ignored tests

```bash
# Avoid: mixes unrelated external-network/credential probes with system tests.
cargo test --workspace -- --ignored
```

External read-only and explicitly credentialed probes have different authority and are documented
in [network-integration.md](network-integration.md).

## Troubleshooting

- Verify Docker first with `docker info`.
- Preserve the first failing system-test log and `target/production-stack/*/backend.log` when present.
- Retry a named failing scenario only after recording the original root cause; a retry does not erase
  failed evidence.
- Docker image pull/authentication failures are infrastructure failures, not passing test evidence.
