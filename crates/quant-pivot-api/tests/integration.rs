//! Live-network integration tests (ignored by default).
//!
//! Run: `cargo test -p quant-pivot-api --features integration -- --ignored`
#![cfg(feature = "integration")]

#[path = "integration/clob_auth.rs"]
mod clob_auth;
#[path = "integration/gamma_sync.rs"]
mod gamma_sync;
#[path = "integration/ws_book.rs"]
mod ws_book;
