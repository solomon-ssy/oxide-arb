# Network integration tests

Live tests for `oxide-arb-api` require outbound HTTPS/WSS to Polymarket and (optionally) Polygon mainnet RPC. They are **ignored by default** so `cargo test` stays deterministic in CI.

## Run all live tests

```bash
export OXIDE_ARB__POLYMARKET__ONCHAIN__RPC_URL="https://polygon-mainnet.g.alchemy.com/v2/YOUR_KEY"
export OXIDE_ARB_TEST_RESOLVED_CONDITION_ID="0x..."   # see CTF section below

cargo test -p oxide-arb-api --features integration -- --ignored --test-threads=1
```

Optional overrides:

| Variable | Purpose |
|----------|---------|
| `OXIDE_ARB_TEST_TOKEN_ID` | CLOB decimal token id for WS book test (skips Gamma discovery) |
| `OXIDE_ARB_TEST_POLYGON_RPC_URL` | Alias for Polygon RPC if you prefer not to use `OXIDE_ARB__*` |
| `OXIDE_ARB_TEST_PRIVATE_KEY` | CLOB auth / FOK probe (`integration/clob_auth.rs`) |

## Polygon RPC (Alchemy)

**Yes — Alchemy is supported.** Use a Polygon **Mainnet** HTTPS endpoint:

```text
https://polygon-mainnet.g.alchemy.com/v2/<API_KEY>
```

1. [Alchemy Dashboard](https://dashboard.alchemy.com/) → Create app → Chain: **Polygon** → Network: **Mainnet**
2. Copy **HTTPS** URL into config or env:

```bash
export OXIDE_ARB__POLYMARKET__ONCHAIN__RPC_URL="https://polygon-mainnet.g.alchemy.com/v2/xxxx"
```

No contract allowlisting is required for the view calls used by `CtfOracleSource` (`payoutNumerators`, `payoutDenominator` on `CTF_ADDRESS`).

## CTF oracle fixture (`OXIDE_ARB_TEST_RESOLVED_CONDITION_ID`)

This must be a **condition_id** (66-char `0x` + 32 bytes), **not** a CLOB decimal `token_id`.

How to obtain one:

1. **Gamma API** — fetch a closed market: `GET https://gamma-api.polymarket.com/markets?closed=true&limit=1` and read `conditionId`.
2. **Polymarket UI** — open a settled market; condition id appears in network payloads to Gamma/CLOB.
3. **Polygonscan** — search interactions with [CTF contract](https://polygonscan.com/address/0x4D97DCd97eC945f40cF65F87097ACe5EA0476045) after resolution.

Verify on-chain: `payoutDenominator(conditionId) > 0`.

## WebSocket book test

`tests/integration/ws_book.rs` subscribes via `ClobWsManager` and waits for a `BookSnapshot` with depth. Token id is discovered from Gamma unless `OXIDE_ARB_TEST_TOKEN_ID` is set.

## CI

The `network-integration` job in `.github/workflows/ci.yml` runs ignored tests on `main` when repository secrets are configured (`POLYGON_RPC_URL`, `RESOLVED_CONDITION_ID`).

## Related

Postgres / Redis / ClickHouse tests that use testcontainers are a separate tier — see [docker-integration.md](./docker-integration.md).
