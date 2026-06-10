//! Oracle voting logic tests.

use async_trait::async_trait;
use oxide_arb_api::oracle::{OracleSource, ResolutionVerdict, SourceVote, VotingOracle};
use oxide_arb_error::rpc::RpcError;
use oxide_arb_models::{runtime_config::AllSourcesDownStrategy, types::MarketId};
use rust_decimal::Decimal;
use std::{sync::Arc, time::Duration};

struct MockSource {
    id: &'static str,
    vote: Option<bool>,
    should_fail: bool,
}

#[async_trait]
impl OracleSource for MockSource {
    fn source_id(&self) -> &'static str {
        self.id
    }

    async fn query_resolution(
        &self,
        _market_id: &MarketId,
        _condition_id: &str,
    ) -> Result<Option<SourceVote>, RpcError> {
        if self.should_fail {
            return Err(RpcError::CallFailed {
                method: "mock".into(),
                reason: "mock failure".into(),
            });
        }
        Ok(self.vote.map(|actual_yes| SourceVote {
            source_id: self.id.into(),
            actual_yes,
            confidence: Decimal::ONE,
            reported_at: chrono::Utc::now(),
        }))
    }
}

fn test_oracle(sources: Vec<Arc<dyn OracleSource>>, quorum: usize) -> VotingOracle {
    VotingOracle::new(
        sources,
        quorum,
        Duration::ZERO,
        AllSourcesDownStrategy::ConservativeReject,
    )
}

#[tokio::test]
async fn both_agree_yes_resolves() {
    let oracle = test_oracle(
        vec![
            Arc::new(MockSource {
                id: "a",
                vote: Some(true),
                should_fail: false,
            }),
            Arc::new(MockSource {
                id: "b",
                vote: Some(true),
                should_fail: false,
            }),
        ],
        2,
    );

    let result = oracle
        .resolve(&MarketId::new("0xtest"), "condition123")
        .await
        .unwrap();

    match result {
        ResolutionVerdict::Resolved { actual_yes, votes } => {
            assert!(actual_yes);
            assert_eq!(votes.len(), 2);
        }
        other => panic!("Expected Resolved, got {other:?}"),
    }
}

#[tokio::test]
async fn both_agree_no_resolves() {
    let oracle = test_oracle(
        vec![
            Arc::new(MockSource {
                id: "a",
                vote: Some(false),
                should_fail: false,
            }),
            Arc::new(MockSource {
                id: "b",
                vote: Some(false),
                should_fail: false,
            }),
        ],
        2,
    );

    let result = oracle
        .resolve(&MarketId::new("0xtest"), "condition123")
        .await
        .unwrap();

    match result {
        ResolutionVerdict::Resolved { actual_yes, .. } => assert!(!actual_yes),
        other => panic!("Expected Resolved, got {other:?}"),
    }
}

#[tokio::test]
async fn disagreement_returns_disputed() {
    let oracle = test_oracle(
        vec![
            Arc::new(MockSource {
                id: "a",
                vote: Some(true),
                should_fail: false,
            }),
            Arc::new(MockSource {
                id: "b",
                vote: Some(false),
                should_fail: false,
            }),
        ],
        2,
    );

    let result = oracle
        .resolve(&MarketId::new("0xtest"), "condition123")
        .await
        .unwrap();

    assert!(matches!(result, ResolutionVerdict::Disputed { .. }));
}

#[tokio::test]
async fn insufficient_votes_returns_unresolved() {
    let oracle = test_oracle(
        vec![
            Arc::new(MockSource {
                id: "a",
                vote: Some(true),
                should_fail: false,
            }),
            Arc::new(MockSource {
                id: "b",
                vote: None,
                should_fail: false,
            }),
        ],
        2,
    );

    let result = oracle
        .resolve(&MarketId::new("0xtest"), "condition123")
        .await
        .unwrap();

    assert!(matches!(result, ResolutionVerdict::Unresolved { .. }));
}

#[tokio::test]
async fn two_of_three_majority_resolves_despite_dissent() {
    let oracle = test_oracle(
        vec![
            Arc::new(MockSource {
                id: "gamma",
                vote: Some(true),
                should_fail: false,
            }),
            Arc::new(MockSource {
                id: "ctf",
                vote: Some(true),
                should_fail: false,
            }),
            Arc::new(MockSource {
                id: "uma",
                vote: Some(false),
                should_fail: false,
            }),
        ],
        2,
    );

    let result = oracle
        .resolve(&MarketId::new("0xtest"), "condition123")
        .await
        .unwrap();

    match result {
        ResolutionVerdict::Resolved { actual_yes, votes } => {
            assert!(actual_yes);
            assert_eq!(votes.len(), 3);
        }
        other => panic!("Expected Resolved (2-of-3), got {other:?}"),
    }
}

#[tokio::test]
async fn source_failure_degrades_gracefully() {
    let oracle = test_oracle(
        vec![
            Arc::new(MockSource {
                id: "a",
                vote: Some(true),
                should_fail: false,
            }),
            Arc::new(MockSource {
                id: "b",
                vote: None,
                should_fail: true,
            }),
        ],
        2,
    );

    let result = oracle
        .resolve(&MarketId::new("0xtest"), "condition123")
        .await
        .unwrap();

    assert!(matches!(result, ResolutionVerdict::Unresolved { .. }));
}

#[tokio::test]
async fn all_sources_down_uses_conservative_reject_reason() {
    let oracle = test_oracle(
        vec![
            Arc::new(MockSource {
                id: "a",
                vote: None,
                should_fail: true,
            }),
            Arc::new(MockSource {
                id: "b",
                vote: None,
                should_fail: true,
            }),
        ],
        2,
    );

    let result = oracle
        .resolve(&MarketId::new("0xtest"), "condition123")
        .await
        .unwrap();

    match result {
        ResolutionVerdict::Unresolved { reason } => {
            assert!(reason.contains("conservative_reject"));
        }
        other => panic!("Expected Unresolved, got {other:?}"),
    }
}
