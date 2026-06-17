# Live Trading SOP (Manual Treasury)

This runbook covers **manual** fund movement and go-live safety. There is no Treasury API
or on-chain deposit automation in this release — operators move USDC via wallet/Proxy
outside the bot, then align runtime config.

## 1. Pre-flight (all modes)

1. Confirm Postgres/Redis/ClickHouse healthy (`GET /api/system/health`).
2. Drain or reconcile blocking trades (`GET /api/system/balance` → `blocking_trade_count = 0`).
3. Publish a control-factor snapshot before Live, or accept `control_factor_live_warn` on status.

## 2. Manual funding (Polygon USDC)

1. Send USDC to the configured **holder/proxy** address (see keystore / deploy config).
2. Approve Polymarket CTF exchange if required by your wallet setup.
3. Wait for CLOB collateral balance to reflect deposit (`GET /api/system/balance` →
   `cash_balance_usd`, source `authoritative_clob` in Live).

**Proxy trap:** deposits to the EOA while the bot trades via Proxy will not increase CLOB
collateral — funds must land on the trading proxy path the keystore uses.

## 3. Config alignment

1. Set `risk.bankroll_usd` ≤ strategy capital ≤ wallet USDC available for strategy.
2. Set `risk.reserve_balance_usd` for operational buffer (default $100).
3. Activate runtime config via governed `/api/runtime-config/versions/{id}/activate`.

## 4. Enter Live

1. `POST /api/system/mode` with reason + `X-Acting-Role`.
2. Verify `GET /api/system/status`: `execution_mode = live`, metrics authoritative.
3. Verify `GET /api/system/balance`:
   - `available_for_sizing_usd` — Kelly bankroll after reserve/reservations/potential loss
   - `blocking_trade_count = 0`
   - `binding_exposure_limit` — which cap binds next sizing

## 5. Halt → reconcile → withdraw

1. `POST /api/system/halt` before manual wallet operations.
2. Wait for in-flight trades to terminalize; resolve `needs_reconcile_count`.
3. `POST /api/system/resume` only when `blocking_trade_count = 0` (409 otherwise).
4. Withdraw USDC from CLOB/wallet **after** halt and reconciliation — never during open
   reservations or unknown venue outcomes.

## 6. Boot with blocking queue

If the process restarts with durable `Submitted` / `Orphaned` / `Intent` rows:

- Boot runs `TradeIntegrityStore::boot_rehydrate()` and may enter **planned halt**.
- Restore in-memory reservations from durable `reservation_id` rows before trading.
- Resolve reconciliation queue, then operator resume.

See also: [`runbook.md`](runbook.md), [`bankroll-and-risk-metrics.md`](bankroll-and-risk-metrics.md).
