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

use arc_swap::ArcSwap;
use futures_util::future::join_all;
use quant_pivot_error::rpc::RpcError;
use quant_pivot_models::{
    runtime_config::{AllSourcesDownStrategy, SettlementOracleConfig},
    types::MarketId,
};
use std::{sync::Arc, time::Duration};

/// Hot-swappable oracle voting policy + source set.
///
/// Sources are part of the snapshot because the UMA source is built from the
/// runtime `settlement.oracle` section (endpoint + timeout) and must be
/// rebuilt atomically with the policy on reload.
struct OracleState {
    sources: Vec<Arc<dyn OracleSource>>,
    quorum: usize,
    cross_check_delay: Duration,
    all_sources_down: AllSourcesDownStrategy,
}

/// Multi-source voting oracle for settlement verification.
///
/// Production default: 2-of-3 quorum (Gamma + CTF on-chain + UMA).
/// Disagreement yields [`ResolutionVerdict::Disputed`] for manual review.
/// Policy and the UMA source are hot-reloadable via
/// [`VotingOracle::stage_reload`] + [`StagedOracleReload::commit`].
pub struct VotingOracle {
    state: ArcSwap<OracleState>,
}

/// A fully built next oracle state, staged but not yet visible.
///
/// Produced by [`VotingOracle::stage_reload`]. Splitting the fallible build
/// from the infallible publish lets the runtime-config applicator stage every
/// fallible subscriber first and commit only when all of them succeeded — an
/// aborted activation never leaves the oracle partially reloaded.
#[must_use = "a staged reload has no effect until committed"]
pub struct StagedOracleReload<'a> {
    oracle: &'a VotingOracle,
    state: Arc<OracleState>,
}

impl StagedOracleReload<'_> {
    /// Publish the staged policy + source set (infallible).
    pub fn commit(self) {
        self.oracle.state.store(self.state);
    }
}

impl VotingOracle {
    pub fn new(
        sources: Vec<Arc<dyn OracleSource>>,
        quorum: usize,
        cross_check_delay: Duration,
        all_sources_down: AllSourcesDownStrategy,
    ) -> Self {
        Self {
            state: ArcSwap::from_pointee(OracleState {
                sources,
                quorum,
                cross_check_delay,
                all_sources_down,
            }),
        }
    }

    /// Stage a hot-reload of the voting policy (runtime-config activation).
    ///
    /// Quorum, cross-check delay, and the all-sources-down strategy apply to
    /// the next resolution after commit. The UMA source is rebuilt from the
    /// new endpoint / timeout; Gamma and CTF sources are connection-level
    /// (deploy) and are carried over unchanged. Nothing becomes visible until
    /// [`StagedOracleReload::commit`].
    pub fn stage_reload(
        &self,
        config: &SettlementOracleConfig,
    ) -> Result<StagedOracleReload<'_>, RpcError> {
        let current = self.state.load();
        let mut sources: Vec<Arc<dyn OracleSource>> = current
            .sources
            .iter()
            .filter(|source| source.source_id() != "uma")
            .map(Arc::clone)
            .collect();
        sources.push(Arc::new(UmaOracleSource::new(config)?));
        Ok(StagedOracleReload {
            oracle: self,
            state: Arc::new(OracleState {
                sources,
                quorum: usize::from(config.voting_quorum.max(1)),
                cross_check_delay: Duration::from_secs(config.cross_check_delay_secs),
                all_sources_down: config.all_sources_down_strategy.clone(),
            }),
        })
    }

    /// Query all sources in parallel; require `quorum` agreeing votes.
    ///
    /// When Gamma reports a resolution hint, waits the configured cross-check
    /// delay before querying all sources so slower on-chain / UMA sources can
    /// catch up.
    pub async fn resolve(
        &self,
        market_id: &MarketId,
        condition_id: &str,
    ) -> Result<ResolutionVerdict, RpcError> {
        let state = self.state.load_full();
        Self::maybe_wait_for_gamma_hint(&state, market_id, condition_id).await;

        let query_futures = state.sources.iter().map(|source| {
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

        let verdict = Self::tally_votes(&votes, state.quorum);

        tracing::info!(
            event = "oracle.resolve",
            market_id = %market_id,
            condition_id,
            yes_votes = votes.iter().filter(|v| v.actual_yes).count(),
            no_votes = votes.iter().filter(|v| !v.actual_yes).count(),
            total_votes = votes.len(),
            quorum = state.quorum,
            source_errors,
            verdict = ?verdict,
        );

        if matches!(verdict, ResolutionVerdict::Unresolved { .. }) && votes.is_empty() {
            return Ok(Self::verdict_all_sources_down(&state, source_errors));
        }

        Ok(verdict)
    }

    async fn maybe_wait_for_gamma_hint(
        state: &OracleState,
        market_id: &MarketId,
        condition_id: &str,
    ) {
        if state.cross_check_delay.is_zero() {
            return;
        }

        let gamma = state.sources.iter().find(|s| s.source_id() == "gamma");
        let Some(gamma) = gamma else {
            return;
        };

        if let Ok(Some(_)) = gamma.query_resolution(market_id, condition_id).await {
            tracing::debug!(
                delay_secs = state.cross_check_delay.as_secs(),
                "Gamma resolution hint; delaying cross-check for on-chain sources"
            );
            tokio::time::sleep(state.cross_check_delay).await;
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

    fn verdict_all_sources_down(state: &OracleState, source_errors: u32) -> ResolutionVerdict {
        let reason = match state.all_sources_down {
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
