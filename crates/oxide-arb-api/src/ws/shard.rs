//! Single WebSocket shard: one SDK connection, multiplexed market streams.

use futures_util::StreamExt;
use oxide_arb_models::domain::pipeline::{PipelineEvent, ShardConnectionStatus};
use oxide_arb_models::types::TokenId;
use polymarket_client_sdk_v2::clob::ws::Client as SdkWsClient;
use polymarket_client_sdk_v2::clob::ws::types::response::WsMessage;
use polymarket_client_sdk_v2::types::U256;
use polymarket_client_sdk_v2::ws::config::Config as SdkWsConfig;
use std::collections::HashSet;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Instant;
use tokio_util::sync::CancellationToken;

use super::drop_hook::WsEventDropHook;
use super::ingest_hooks::BookLevelRejectHook;
use super::normalize::normalize_ws_message;
use super::reconnect::{ReconnectPolicy, ReconnectState};

/// A single shard managing one SDK WebSocket connection.
pub struct WsShard {
    pub shard_id: usize,
    pub subscribed_tokens: HashSet<TokenId>,
    pub reconnect_state: ReconnectState,
    pub output_tx: flume::Sender<PipelineEvent>,
    pub shutdown: CancellationToken,
    pub ws_url: String,
    last_message_at: Arc<parking_lot::Mutex<Option<Instant>>>,
    on_events_dropped: Option<WsEventDropHook>,
    on_book_level_rejected: Option<BookLevelRejectHook>,
}

impl WsShard {
    pub fn new(
        shard_id: usize,
        ws_url: String,
        output_tx: flume::Sender<PipelineEvent>,
        shutdown: CancellationToken,
        last_message_at: Arc<parking_lot::Mutex<Option<Instant>>>,
        on_events_dropped: Option<WsEventDropHook>,
        on_book_level_rejected: Option<BookLevelRejectHook>,
    ) -> Self {
        Self {
            shard_id,
            subscribed_tokens: HashSet::new(),
            reconnect_state: ReconnectState::new(shard_id, &ReconnectPolicy::default()),
            output_tx,
            shutdown,
            ws_url,
            last_message_at,
            on_events_dropped,
            on_book_level_rejected,
        }
    }

    pub async fn run_loop(mut self) {
        loop {
            if self.shutdown.is_cancelled() {
                tracing::info!(shard_id = self.shard_id, "Shard shutting down");
                break;
            }

            self.emit_status(ShardConnectionStatus::Reconnecting {
                attempt: self.reconnect_state.retries_used(),
            });

            match self.connect_and_stream().await {
                Ok(()) => tracing::info!(shard_id = self.shard_id, "Stream ended cleanly"),
                Err(e) => {
                    tracing::warn!(shard_id = self.shard_id, error = %e, "Shard connection error");
                }
            }

            self.emit_status(ShardConnectionStatus::Disconnected);

            if self.shutdown.is_cancelled() {
                break;
            }

            if let Some(delay) = self.reconnect_state.next_delay() {
                tokio::select! {
                    () = tokio::time::sleep(delay) => {}
                    () = self.shutdown.cancelled() => break,
                }
            } else {
                tracing::error!(shard_id = self.shard_id, "Reconnection budget exhausted");
                break;
            }
        }
    }

    async fn connect_and_stream(&mut self) -> Result<(), String> {
        let asset_ids: Vec<U256> = self
            .subscribed_tokens
            .iter()
            .filter_map(|t| U256::from_str(t.as_str()).ok())
            .collect();

        if asset_ids.is_empty() {
            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(5)) => {}
                () = self.shutdown.cancelled() => {}
            }
            return Ok(());
        }

        let ws_config = SdkWsConfig::default();
        let client = SdkWsClient::new(&self.ws_url, ws_config)
            .map_err(|e| format!("WS client creation failed: {e}"))?;

        // `subscribe_market_resolutions` first — enables SDK `custom_features` on the channel.
        let mut resolution_stream = Box::pin(
            client
                .subscribe_market_resolutions(asset_ids.clone())
                .map_err(|e| format!("subscribe_market_resolutions: {e}"))?,
        );
        let mut book_stream = Box::pin(
            client
                .subscribe_orderbook(asset_ids.clone())
                .map_err(|e| format!("subscribe_orderbook: {e}"))?,
        );
        let mut price_stream = Box::pin(
            client
                .subscribe_prices(asset_ids)
                .map_err(|e| format!("subscribe_prices: {e}"))?,
        );

        self.reconnect_state.reset();
        self.emit_status(ShardConnectionStatus::Connected);

        loop {
            tokio::select! {
                () = self.shutdown.cancelled() => return Ok(()),
                book = book_stream.next() => {
                    match book {
                        Some(Ok(update)) => {
                            let ws_ingress = Instant::now();
                            self.dispatch_events(normalize_ws_message(
                                WsMessage::Book(update),
                                ws_ingress,
                                self.on_book_level_rejected.as_ref(),
                            ));
                        }
                        Some(Err(e)) => tracing::warn!(shard_id = self.shard_id, error = %e, "book stream error"),
                        None => return Err("book stream closed".into()),
                    }
                }
                price = price_stream.next() => {
                    match price {
                        Some(Ok(pc)) => {
                            let ws_ingress = Instant::now();
                            self.dispatch_events(normalize_ws_message(
                                WsMessage::PriceChange(pc),
                                ws_ingress,
                                self.on_book_level_rejected.as_ref(),
                            ));
                        }
                        Some(Err(e)) => tracing::warn!(shard_id = self.shard_id, error = %e, "price stream error"),
                        None => return Err("price stream closed".into()),
                    }
                }
                res = resolution_stream.next() => {
                    match res {
                        Some(Ok(mr)) => {
                            let ws_ingress = Instant::now();
                            self.dispatch_events(normalize_ws_message(
                                WsMessage::MarketResolved(mr),
                                ws_ingress,
                                self.on_book_level_rejected.as_ref(),
                            ));
                        }
                        Some(Err(e)) => tracing::warn!(shard_id = self.shard_id, error = %e, "resolution stream error"),
                        None => return Err("resolution stream closed".into()),
                    }
                }
            }
        }
    }

    fn dispatch_events(&self, events: Vec<PipelineEvent>) {
        if !events.is_empty() {
            *self.last_message_at.lock() = Some(Instant::now());
        }
        let mut dropped = 0u64;
        for event in events {
            if self.output_tx.try_send(event).is_err() {
                dropped += 1;
            }
        }
        if dropped > 0 {
            tracing::error!(
                shard_id = self.shard_id,
                dropped,
                "WS output channel full — events dropped"
            );
            if let Some(hook) = &self.on_events_dropped {
                hook(dropped);
            }
        }
    }

    fn emit_status(&self, status: ShardConnectionStatus) {
        let _ = self.output_tx.try_send(PipelineEvent::ShardStatus {
            shard_id: self.shard_id,
            status,
        });
    }
}

#[cfg(test)]
mod dispatch_tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::*;
    use oxide_arb_models::domain::pipeline::ShardConnectionStatus;

    #[test]
    fn continues_dispatch_on_full_channel_and_invokes_drop_hook() {
        let (tx, rx) = flume::bounded(1);
        let dropped = Arc::new(AtomicU64::new(0));
        let hook: WsEventDropHook = {
            let dropped = Arc::clone(&dropped);
            Arc::new(move |n| {
                dropped.fetch_add(n, Ordering::Relaxed);
            })
        };
        let shard = WsShard::new(
            0,
            "ws://test".into(),
            tx,
            CancellationToken::new(),
            Arc::new(parking_lot::Mutex::new(None)),
            Some(hook),
            None,
        );

        let status = |_n| PipelineEvent::ShardStatus {
            shard_id: 0,
            status: ShardConnectionStatus::Connected,
        };

        shard.dispatch_events(vec![status(1), status(2), status(3)]);

        assert_eq!(rx.len(), 1);
        assert_eq!(dropped.load(Ordering::Relaxed), 2);
    }
}
