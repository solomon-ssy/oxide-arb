//! Live-network integration tests (ignored by default).
//!
//! Run: `cargo test -p quant-pivot-api -- --ignored --test-threads=1`

#[path = "integration/binance_klines.rs"]
mod binance_klines;
#[path = "integration/clob_auth.rs"]
mod clob_auth;
#[path = "integration/gamma_sync.rs"]
mod gamma_sync;
#[path = "integration/rtds_live.rs"]
mod rtds_live;
#[path = "integration/weather_public.rs"]
mod weather_public;
#[path = "integration/ws_book.rs"]
mod ws_book;
