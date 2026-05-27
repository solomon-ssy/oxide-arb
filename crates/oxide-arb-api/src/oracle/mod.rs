//! Settlement oracle with 2-of-3 multi-source voting (Gamma + CTF + UMA).

mod builder;
mod ctf_source;
mod gamma_source;
pub mod source;
pub mod types;
mod uma_source;

pub use builder::{build_voting_oracle, build_voting_oracle_from_urls};
pub use ctf_source::CtfOracleSource;
pub use gamma_source::GammaOracleSource;
pub use source::OracleSource;
pub use types::{ResolutionVerdict, SourceVote};
pub use uma_source::UmaOracleSource;

use futures_util::future::join_all;
use oxide_arb_error::rpc::RpcError;
use oxide_arb_models::{config::AllSourcesDownStrategy, types::MarketId};
use std::{sync::Arc, time::Duration};

/// Multi-source voting oracle for settlement verification.
///
/// Production default: 2-of-3 quorum (Gamma + CTF on-chain + UMA).
/// Disagreement yields [`ResolutionVerdict::Disputed`] for manual review.
pub struct VotingOracle {
    sources: Vec<Arc<dyn OracleSource>>,
    quorum: usize,
    cross_check_delay: Duration,
    all_sources_down: AllSourcesDownStrategy,
}

impl VotingOracle {
    pub fn new(
        sources: Vec<Arc<dyn OracleSource>>,
        quorum: usize,
        cross_check_delay: Duration,
        all_sources_down: AllSourcesDownStrategy,
    ) -> Self {
        Self {
            sources,
            quorum,
            cross_check_delay,
            all_sources_down,
        }
    }

    /// Query all sources in parallel; require `quorum` agreeing votes.
    ///
    /// When Gamma reports a resolution hint, waits [`VotingOracle::cross_check_delay`] before
    /// querying all sources so slower on-chain / UMA sources can catch up.
    pub async fn resolve(
        &self,
        market_id: &MarketId,
        condition_id: &str,
    ) -> Result<ResolutionVerdict, RpcError> {
        self.maybe_wait_for_gamma_hint(market_id, condition_id)
            .await;

        let query_futures = self.sources.iter().map(|source| {
            let market_id = market_id.clone();
            let condition_id = condition_id.to_owned();
            async move {
                let source_id = source.source_id();
                let result = source.query_resolution(&market_id, &condition_id).await;
                (source_id, result)
            }
        });

        let outcomes = join_all(query_futures).await;
        let mut votes = Vec::new();
        let mut source_errors = 0u32;

        for (source_id, outcome) in outcomes {
            match outcome {
                Ok(Some(vote)) => votes.push(vote),
                Ok(None) => {}
                Err(e) => {
                    source_errors += 1;
                    tracing::warn!(
                        source = source_id,
                        error = %e,
                        "Oracle source query failed"
                    );
                }
            }
        }

        let verdict = Self::tally_votes(&votes, self.quorum);

        tracing::info!(
            event = "oracle.resolve",
            market_id = %market_id,
            condition_id,
            yes_votes = votes.iter().filter(|v| v.actual_yes).count(),
            no_votes = votes.iter().filter(|v| !v.actual_yes).count(),
            total_votes = votes.len(),
            quorum = self.quorum,
            source_errors,
            verdict = ?verdict,
        );

        if matches!(verdict, ResolutionVerdict::Unresolved { .. }) && votes.is_empty() {
            return Ok(self.verdict_all_sources_down(source_errors));
        }

        Ok(verdict)
    }

    async fn maybe_wait_for_gamma_hint(&self, market_id: &MarketId, condition_id: &str) {
        if self.cross_check_delay.is_zero() {
            return;
        }

        let gamma = self.sources.iter().find(|s| s.source_id() == "gamma");
        let Some(gamma) = gamma else {
            return;
        };

        if let Ok(Some(_)) = gamma.query_resolution(market_id, condition_id).await {
            tracing::debug!(
                delay_secs = self.cross_check_delay.as_secs(),
                "Gamma resolution hint; delaying cross-check for on-chain sources"
            );
            tokio::time::sleep(self.cross_check_delay).await;
        }
    }

    fn tally_votes(votes: &[SourceVote], quorum: usize) -> ResolutionVerdict {
        let yes_votes = votes.iter().filter(|v| v.actual_yes).count();
        let no_votes = votes.len().saturating_sub(yes_votes);

        if yes_votes >= quorum {
            return ResolutionVerdict::Resolved {
                actual_yes: true,
                votes: votes.to_vec(),
            };
        }

        if no_votes >= quorum {
            return ResolutionVerdict::Resolved {
                actual_yes: false,
                votes: votes.to_vec(),
            };
        }

        if votes.len() >= quorum {
            ResolutionVerdict::Disputed {
                votes: votes.to_vec(),
            }
        } else {
            ResolutionVerdict::Unresolved {
                reason: format!(
                    "Insufficient agreeing votes: yes={yes_votes}, no={no_votes}, need {quorum}"
                ),
            }
        }
    }

    fn verdict_all_sources_down(&self, source_errors: u32) -> ResolutionVerdict {
        let reason = match self.all_sources_down {
            AllSourcesDownStrategy::ConservativeReject => {
                format!("conservative_reject: no oracle votes ({source_errors} source errors)")
            }
            AllSourcesDownStrategy::ManualAck => {
                format!("manual_ack_required: no oracle votes ({source_errors} source errors)")
            }
        };
        ResolutionVerdict::Unresolved { reason }
    }
}
