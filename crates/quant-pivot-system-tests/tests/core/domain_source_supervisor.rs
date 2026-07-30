//! Credential-gated source expectation contracts against disposable `PostgreSQL`.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use quant_pivot_core::app::domain_source_supervisor::DomainSourceSupervisor;
use quant_pivot_models::{
    config::WeatherVerticalBindingsConfig,
    enums::domain::DomainSourceExpectationStatus,
    types::{DomainInstrumentKey, DomainSourceId},
};
use quant_pivot_repository::{
    postgres::{PgDomainSourceExpectationRepository, PgMarketLinkageRepository},
    traits::{DomainSourceExpectationRepository, MarketLinkageRepository},
};
use quant_pivot_system_tests::postgres::setup_pg;

use super::weather_linkage::station_profiles;

pub async fn credential_blocked_recovers() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let expectations = Arc::new(PgDomainSourceExpectationRepository::new(db.clone()));
    let linkages = Arc::new(PgMarketLinkageRepository::new(db));
    let expectation_port: Arc<dyn DomainSourceExpectationRepository> =
        Arc::<PgDomainSourceExpectationRepository>::clone(&expectations);
    let linkage_port: Arc<dyn MarketLinkageRepository> =
        Arc::<PgMarketLinkageRepository>::clone(&linkages);
    let weather_stations: BTreeMap<_, _> = station_profiles().into_iter().collect();
    let vertical_bindings = WeatherVerticalBindingsConfig::default();

    DomainSourceSupervisor::new(
        Arc::clone(&expectation_port),
        Arc::clone(&linkage_port),
        weather_stations.clone(),
        vertical_bindings.clone(),
        BTreeSet::new(),
    )
    .expect("build credential-blocked source supervisor")
    .reconcile()
    .await
    .expect("materialize credential-blocked expectations");

    let blocked = expectations
        .list_all()
        .await
        .expect("list credential-blocked expectations")
        .into_iter()
        .filter(|row| row.credential_required)
        .collect::<Vec<_>>();
    let expected_instruments = BTreeSet::from([
        DomainInstrumentKey::new("CHAINLINK_DATA_STREAMS:BNB-USD"),
        DomainInstrumentKey::new("CHAINLINK_DATA_STREAMS:DOGE-USD"),
        DomainInstrumentKey::new("CHAINLINK_DATA_STREAMS:HYPE-USD"),
    ]);
    assert_eq!(blocked.len(), expected_instruments.len());
    assert_eq!(
        blocked
            .iter()
            .map(|row| row.instrument_key.clone())
            .collect::<BTreeSet<_>>(),
        expected_instruments
    );
    assert!(blocked.iter().all(|row| {
        row.source_id == DomainSourceId::chainlink_data_streams()
            && row.status == DomainSourceExpectationStatus::CredentialBlocked
            && row.status_reason.as_deref() == Some("source_credentials_unavailable")
    }));

    DomainSourceSupervisor::new(
        expectation_port,
        linkage_port,
        weather_stations,
        vertical_bindings,
        BTreeSet::from([DomainSourceId::chainlink_data_streams()]),
    )
    .expect("build credential-ready source supervisor")
    .reconcile()
    .await
    .expect("reconcile credential readiness");

    let recovered = expectations
        .list_all()
        .await
        .expect("list credential-ready expectations")
        .into_iter()
        .filter(|row| row.credential_required)
        .collect::<Vec<_>>();
    assert_eq!(recovered.len(), blocked.len());
    assert!(recovered.iter().all(|row| {
        row.status == DomainSourceExpectationStatus::NotStarted && row.status_reason.is_none()
    }));
}
