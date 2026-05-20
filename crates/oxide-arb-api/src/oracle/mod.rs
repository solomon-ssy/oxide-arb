//! Settlement oracle with 2-of-2 multi-source voting.

mod ctf_source;
mod gamma_source;
pub mod source;
pub mod types;

pub use ctf_source::CtfOracleSource;
pub use gamma_source::GammaOracleSource;
pub use source::OracleSource;
pub use types::{ResolutionVerdict, SourceVote};

use oxide_arb_error::rpc::RpcError;
use oxide_arb_models::types::MarketId;
use std::sync::Arc;

/// Multi-source voting oracle for settlement verification.
///
/// Phase 1: 2-of-2 (Gamma + CTF must agree). If they disagree,
/// returns `Disputed` for manual intervention.
pub struct VotingOracle {
    sources: Vec<Arc<dyn OracleSource>>,
    quorum: usize,
}

impl VotingOracle {
    pub fn new(sources: Vec<Arc<dyn OracleSource>>, quorum: usize) -> Self {
        Self { sources, quorum }
    }

    /// Query all sources and require quorum agreement.
    pub async fn resolve(
        &self,
        market_id: &MarketId,
        condition_id: &str,
    ) -> Result<ResolutionVerdict, RpcError> {
        let mut votes = Vec::new();

        for source in &self.sources {
            match source.query_resolution(market_id, condition_id).await {
                Ok(Some(vote)) => votes.push(vote),
                Ok(None) => {}
                Err(e) => {
                    tracing::warn!(
                        source = source.source_id(),
                        error = %e,
                        "Oracle source query failed"
                    );
                }
            }
        }

        if votes.len() < self.quorum {
            return Ok(ResolutionVerdict::Unresolved {
                reason: format!(
                    "Insufficient votes: got {}, need {}",
                    votes.len(),
                    self.quorum
                ),
            });
        }

        let first_yes = votes[0].actual_yes;
        let all_agree = votes.iter().all(|v| v.actual_yes == first_yes);

        if all_agree {
            Ok(ResolutionVerdict::Resolved {
                actual_yes: first_yes,
                votes,
            })
        } else {
            Ok(ResolutionVerdict::Disputed { votes })
        }
    }
}
