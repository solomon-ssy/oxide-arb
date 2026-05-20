//! Token-to-shard routing and dynamic shard spawning.

use oxide_arb_models::types::TokenId;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

use super::event::WsEvent;
use super::shard::WsShard;

/// Routes token subscriptions across shards and spawns shard tasks.
pub struct ShardRouter {
    max_per_shard: usize,
    assignments: Arc<RwLock<HashMap<TokenId, usize>>>,
    shard_loads: Arc<RwLock<Vec<usize>>>,
    output_tx: flume::Sender<WsEvent>,
    ws_url: String,
    shutdown: CancellationToken,
}

impl ShardRouter {
    pub fn new(
        max_per_shard: usize,
        output_tx: flume::Sender<WsEvent>,
        ws_url: String,
        shutdown: CancellationToken,
    ) -> Self {
        Self {
            max_per_shard,
            assignments: Arc::new(RwLock::new(HashMap::new())),
            shard_loads: Arc::new(RwLock::new(Vec::new())),
            output_tx,
            ws_url,
            shutdown,
        }
    }

    pub fn assign_tokens(&self, tokens: &[TokenId]) {
        let mut new_shards: Vec<(usize, Vec<TokenId>)> = Vec::new();

        for token in tokens {
            let shard_id = {
                let mut assignments = self.assignments.write();
                if assignments.contains_key(token) {
                    continue;
                }

                let mut loads = self.shard_loads.write();
                let shard_id = self.find_or_create_shard(&mut loads);
                assignments.insert(token.clone(), shard_id);
                drop(assignments);
                loads[shard_id] += 1;
                shard_id
            };

            if let Some((_, toks)) = new_shards.iter_mut().find(|(id, _)| *id == shard_id) {
                toks.push(token.clone());
            } else {
                new_shards.push((shard_id, vec![token.clone()]));
            }
        }

        for (shard_id, shard_tokens) in new_shards {
            self.ensure_shard_running(shard_id, shard_tokens);
        }
    }

    pub fn remove_tokens(&self, tokens: &[TokenId]) {
        for token in tokens {
            let shard_id = {
                let mut assignments = self.assignments.write();
                assignments.remove(token)
            };

            if let Some(shard_id) = shard_id {
                let mut loads = self.shard_loads.write();
                if shard_id < loads.len() && loads[shard_id] > 0 {
                    loads[shard_id] -= 1;
                }
            }
        }
    }

    pub fn shard_count(&self) -> usize {
        self.shard_loads.read().len()
    }

    fn find_or_create_shard(&self, loads: &mut Vec<usize>) -> usize {
        for (i, load) in loads.iter().enumerate() {
            if *load < self.max_per_shard {
                return i;
            }
        }
        loads.push(0);
        loads.len() - 1
    }

    fn ensure_shard_running(&self, shard_id: usize, tokens: Vec<TokenId>) {
        let mut shard = WsShard::new(
            shard_id,
            self.ws_url.clone(),
            self.output_tx.clone(),
            self.shutdown.clone(),
        );
        for token in tokens {
            shard.subscribed_tokens.insert(token);
        }
        tokio::spawn(shard.run_loop());
    }
}
