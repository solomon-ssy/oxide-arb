//! Single WebSocket shard: owns one SDK WS client connection.
//!
//! Each shard subscribes to a set of token IDs and forwards parsed
//! events to the shared output channel.

use futures_util::StreamExt;
use oxide_arb_models::types::{Price, Shares, TokenId};
use polymarket_client_sdk_v2::clob::ws::Client as SdkWsClient;
use polymarket_client_sdk_v2::types::U256;
use std::collections::HashSet;
use std::str::FromStr;
use tokio_util::sync::CancellationToken;

use super::event::{PriceLevel, ShardConnectionStatus, WsEvent};
use super::reconnect::{ReconnectPolicy, ReconnectState};

/// A single shard managing one SDK WebSocket connection.
pub struct WsShard {
    pub shard_id: usize,
    pub subscribed_tokens: HashSet<TokenId>,
    pub reconnect_state: ReconnectState,
    pub output_tx: flume::Sender<WsEvent>,
    pub shutdown: CancellationToken,
    pub ws_url: String,
}

impl WsShard {
    pub fn new(
        shard_id: usize,
        ws_url: String,
        output_tx: flume::Sender<WsEvent>,
        shutdown: CancellationToken,
    ) -> Self {
        Self {
            shard_id,
            subscribed_tokens: HashSet::new(),
            reconnect_state: ReconnectState::new(shard_id, &ReconnectPolicy::default()),
            output_tx,
            shutdown,
            ws_url,
        }
    }

    /// Run the shard's main loop: connect, subscribe, forward events, reconnect.
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
                Ok(()) => {
                    tracing::info!(shard_id = self.shard_id, "Stream ended cleanly");
                }
                Err(e) => {
                    tracing::warn!(shard_id = self.shard_id, error = %e, "Shard connection error");
                }
            }

            self.emit_status(ShardConnectionStatus::Disconnected);

            if self.shutdown.is_cancelled() {
                break;
            }

            if let Some(delay) = self.reconnect_state.next_delay() {
                tracing::debug!(
                    shard_id = self.shard_id,
                    delay_ms = delay.as_millis(),
                    "Waiting before reconnect"
                );
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

        let ws_config = polymarket_client_sdk_v2::ws::config::Config::default();
        let client = SdkWsClient::new(&self.ws_url, ws_config)
            .map_err(|e| format!("WS client creation failed: {e}"))?;

        let stream = client
            .subscribe_orderbook(asset_ids)
            .map_err(|e| format!("subscribe_orderbook failed: {e}"))?;

        self.reconnect_state.reset();
        self.emit_status(ShardConnectionStatus::Connected);

        let mut stream = Box::pin(stream);

        loop {
            tokio::select! {
                () = self.shutdown.cancelled() => {
                    return Ok(());
                }
                msg = stream.next() => {
                    match msg {
                        Some(Ok(book)) => {
                            let bids: Vec<PriceLevel> = book.bids.iter().map(|l| PriceLevel {
                                price: Price::new(l.price),
                                size: Shares::new(l.size),
                            }).collect();
                            let asks: Vec<PriceLevel> = book.asks.iter().map(|l| PriceLevel {
                                price: Price::new(l.price),
                                size: Shares::new(l.size),
                            }).collect();

                            let event = WsEvent::BookSnapshot {
                                asset_id: TokenId::new(book.asset_id.to_string()),
                                bids,
                                asks,
                                timestamp_ms: u64::try_from(book.timestamp).unwrap_or(0),
                                hash: book.hash.unwrap_or_default(),
                            };

                            if self.output_tx.send(event).is_err() {
                                tracing::error!(shard_id = self.shard_id, "Output channel closed");
                                return Ok(());
                            }
                        }
                        Some(Err(e)) => {
                            tracing::warn!(shard_id = self.shard_id, error = %e, "WS message error");
                        }
                        None => {
                            return Err("Stream ended (connection closed)".into());
                        }
                    }
                }
            }
        }
    }

    fn emit_status(&self, status: ShardConnectionStatus) {
        let _ = self.output_tx.try_send(WsEvent::ShardStatus {
            shard_id: self.shard_id,
            status,
        });
    }
}
