//! Expected domain-source binding lifecycle persistence system contracts.

use chrono::Utc;
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::data_plane::{
        DomainSourceExpectationDefinition, DomainSourceExpectationTransition,
        UpsertDomainSourceExpectation,
    },
    enums::domain::{DomainFamily, DomainSourceExpectationStatus},
    types::{ContentHash, DomainInstrumentKey, DomainSourceId, ResearchProfileId},
};
use quant_pivot_repository::{
    postgres::PgDomainSourceExpectationRepository, traits::DomainSourceExpectationRepository,
};
use quant_pivot_system_tests::postgres::setup_pg;

fn hash(fill: char) -> ContentHash {
    ContentHash::parse(&format!("blake3:{}", fill.to_string().repeat(64))).expect("hash")
}

fn expectation() -> UpsertDomainSourceExpectation {
    expectation_with_required(true)
}

fn expectation_with_required(required: bool) -> UpsertDomainSourceExpectation {
    UpsertDomainSourceExpectation::new(
        DomainSourceExpectationDefinition {
            family: DomainFamily::Weather,
            source_id: DomainSourceId::aviation_weather(),
            instrument_key: DomainInstrumentKey::new("METAR:KLGA"),
            capability_registry_hash: hash('a'),
            required,
            credential_required: false,
            freshness_secs: 900,
            affected_market_ids: Vec::new(),
            affected_profile_ids: vec![ResearchProfileId::new("weather_forecast_24h")],
        },
        DomainSourceExpectationStatus::NotStarted,
        None,
        Utc::now(),
    )
    .expect("expectation")
}

pub async fn expected_source_exists_before_cursor_and_transitions_optimistically() {
    let (pool, _container) = setup_pg().await;
    let repo = PgDomainSourceExpectationRepository::new(pool.connection().clone());
    let inserted = repo
        .upsert(expectation())
        .await
        .expect("upsert expectation");
    assert_eq!(inserted.status, DomainSourceExpectationStatus::NotStarted);
    assert_eq!(repo.list_all().await.expect("list").len(), 1);

    let live = repo
        .transition(DomainSourceExpectationTransition {
            expectation_id: inserted.expectation_id,
            from: DomainSourceExpectationStatus::NotStarted,
            to: DomainSourceExpectationStatus::Live,
            reason: None,
        })
        .await
        .expect("transition live");
    assert_eq!(live.status, DomainSourceExpectationStatus::Live);

    let stale_writer = repo
        .transition(DomainSourceExpectationTransition {
            expectation_id: inserted.expectation_id,
            from: DomainSourceExpectationStatus::NotStarted,
            to: DomainSourceExpectationStatus::Failed,
            reason: Some("source_timeout".to_owned()),
        })
        .await
        .expect_err("stale status writer must fail");
    assert!(matches!(
        stale_writer,
        StorageError::IllegalTransition { .. }
    ));
}

pub async fn natural_key_upsert_updates_one_stable_expectation() {
    let (pool, _container) = setup_pg().await;
    let repo = PgDomainSourceExpectationRepository::new(pool.connection().clone());
    let initial = repo
        .upsert(expectation())
        .await
        .expect("insert expectation");
    let replacement = expectation_with_required(false);
    let replaced = repo
        .upsert(replacement)
        .await
        .expect("upsert expectation by natural key");

    assert_eq!(replaced.expectation_id, initial.expectation_id);
    assert!(!replaced.required);
    assert_eq!(repo.list_all().await.expect("list").len(), 1);
}
