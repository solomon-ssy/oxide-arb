//! Basis-cross-check exceedance detection (11.2.2 remediation R6).
//!
//! Closes the loop the design promised but never built: `domain.crypto.
//! basis_vs_resolution_source` was computed and persisted as a feature value,
//! but exceeding `domain.crypto.cross_check.max_basis_bps` never produced a
//! durable, operator-actionable artifact — this module is that artifact.
//!
//! The review queue is the `quant_basis_alert` ledger itself, filtered to
//! unacknowledged rows (`BasisAlertListQuery::open_only`) and triaged through
//! the single governed `BasisAlertRepository::acknowledge` mutation — this is
//! deliberately **not** a `MarketLinkage` state change: a basis divergence is
//! a live feature-vs-oracle observation, not evidence the market's subject
//! binding itself is wrong, so it never mutates the linkage ledger.
//!
//! The feature itself is only ever `Present` for a Chainlink-oracle-settled
//! subject (a Binance-settled market's basis feature is
//! `Missing(NotApplicable)` — see `CryptoDomainFeatureBuilder`), so presence
//! alone is sufficient evidence the check applies; no separate oracle lookup
//! is needed here.

use std::{collections::HashMap, hash::BuildHasher};

use quant_pivot_models::{
    domain::NewBasisAlert,
    runtime_config::DomainConfig,
    types::{BasisAlertId, Bps, MarketId},
};
use quant_pivot_research::{
    domain::oracle_instrument,
    features::{DomainSliceInputs, FeatureValue, FeatureVector, names::domain_crypto},
};
use rust_decimal::Decimal;

/// Scan `accepted` vectors for a basis exceedance and build the alert rows to
/// persist. Pure and side-effect-free — the caller persists + alerts.
#[must_use]
pub fn detect_basis_alerts<S: BuildHasher>(
    accepted: &[FeatureVector],
    domain_inputs: &HashMap<MarketId, DomainSliceInputs, S>,
    domain: &DomainConfig,
) -> Vec<NewBasisAlert> {
    let threshold_bps = Bps::new(Decimal::from(domain.crypto.cross_check.max_basis_bps));
    accepted
        .iter()
        .filter_map(|vector| {
            let FeatureValue::Bps(basis) =
                vector.value(&domain_crypto::BASIS_VS_RESOLUTION_SOURCE)?
            else {
                return None;
            };
            if basis.abs() <= threshold_bps.inner() {
                return None;
            }
            let inputs = domain_inputs.get(&vector.market_id)?;
            let oracle_key = oracle_instrument(&inputs.binding)?;
            Some(NewBasisAlert {
                alert_id: BasisAlertId::from_v7(),
                market_id: vector.market_id.clone(),
                instrument_key: inputs.binding.instrument_key.to_string(),
                oracle_instrument_key: oracle_key.to_string(),
                basis_bps: Bps::new(*basis),
                threshold_bps,
                as_of: vector.as_of,
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::detect_basis_alerts;
    use chrono::{TimeZone, Utc};
    use quant_pivot_models::{
        domain::{
            CryptoSubject, GroundingProof, MarketSubject, NewBasisAlert, PriceComparator,
            ResolutionOracle, ResolvedBinding,
        },
        enums::{
            domain::{DomainFamily, KlineInterval},
            quant::DataQualityStatus,
        },
        runtime_config::DomainConfig,
        types::{
            BinanceSymbol, ChainlinkFeedKey, CryptoAsset, CryptoQuote, DomainInstrumentKey,
            MarketId, SchemaVersion, TokenId,
        },
    };
    use quant_pivot_research::{
        domain::DomainObservationWindow,
        features::{DomainSliceInputs, FeatureValue, FeatureVector, names::domain_crypto},
    };
    use rust_decimal_macros::dec;
    use std::collections::{BTreeMap, HashMap};

    fn instrument() -> DomainInstrumentKey {
        DomainInstrumentKey::binance_kline(
            &BinanceSymbol::parse("BTCUSDT").expect("symbol"),
            KlineInterval::OneMinute,
        )
    }

    fn binding() -> ResolvedBinding {
        let now = Utc.with_ymd_and_hms(2026, 7, 1, 12, 0, 0).unwrap();
        ResolvedBinding {
            subject: MarketSubject::Crypto(CryptoSubject {
                asset: CryptoAsset::parse("BTC").expect("asset"),
                quote: CryptoQuote::parse("USD").expect("quote"),
                comparator: PriceComparator::UpVsReference,
                strike: None,
                reference_at: Some(now),
                observation_at: now,
                resolution_oracle: ResolutionOracle::ChainlinkDataStreams {
                    feed: ChainlinkFeedKey::parse("BTC-USD").expect("feed"),
                },
            }),
            instrument_key: instrument(),
            grounding: GroundingProof { spans: Vec::new() },
            override_context: None,
        }
    }

    fn vector(basis_bps: Option<FeatureValue>) -> FeatureVector {
        let market_id = MarketId::new("m1");
        let mut generic = BTreeMap::new();
        if let Some(value) = basis_bps {
            generic.insert(domain_crypto::BASIS_VS_RESOLUTION_SOURCE, value);
        }
        FeatureVector {
            market_id,
            token_id: Some(TokenId::new("t1")),
            as_of: Utc.with_ymd_and_hms(2026, 7, 1, 12, 0, 0).unwrap(),
            generic_schema_version: SchemaVersion::FIRST,
            generic,
            domain: None,
            substitutions: Vec::new(),
            data_quality: DataQualityStatus::Fresh,
            staleness_ms: 0,
            source_refs: Vec::new(),
        }
    }

    fn domain_inputs_for(market_id: &MarketId) -> HashMap<MarketId, DomainSliceInputs> {
        HashMap::from([(
            market_id.clone(),
            DomainSliceInputs {
                family: DomainFamily::Crypto,
                binding: binding(),
                primary: DomainObservationWindow::default(),
                oracle: None,
            },
        )])
    }

    #[test]
    fn exceedance_produces_an_alert_with_the_governed_threshold() {
        let domain = DomainConfig::default(); // default max_basis_bps = 50
        let vec = vector(Some(FeatureValue::Bps(dec!(75))));
        let alerts = detect_basis_alerts(
            std::slice::from_ref(&vec),
            &domain_inputs_for(&vec.market_id),
            &domain,
        );
        assert_eq!(alerts.len(), 1);
        assert_eq!(alerts[0].basis_bps.inner(), dec!(75));
        assert_eq!(alerts[0].threshold_bps.inner(), dec!(50));
        assert_eq!(alerts[0].market_id, vec.market_id);
        assert_eq!(alerts[0].oracle_instrument_key, "CHAINLINK:BTC-USD");
    }

    #[test]
    fn negative_exceedance_also_alerts_on_magnitude() {
        let domain = DomainConfig::default(); // default max_basis_bps = 50
        let vec = vector(Some(FeatureValue::Bps(dec!(-80))));
        let alerts = detect_basis_alerts(
            std::slice::from_ref(&vec),
            &domain_inputs_for(&vec.market_id),
            &domain,
        );
        assert_eq!(alerts.len(), 1, "magnitude, not sign, drives the threshold");
    }

    #[test]
    fn within_threshold_never_alerts() {
        let domain = DomainConfig::default();
        let vec = vector(Some(FeatureValue::Bps(dec!(10))));
        let alerts = detect_basis_alerts(
            std::slice::from_ref(&vec),
            &domain_inputs_for(&vec.market_id),
            &domain,
        );
        assert!(alerts.is_empty());
    }

    #[test]
    fn missing_basis_feature_never_alerts() {
        // Binance-settled markets carry no basis feature at all (NotApplicable).
        let domain = DomainConfig::default();
        let vec = vector(None);
        let alerts = detect_basis_alerts(
            std::slice::from_ref(&vec),
            &domain_inputs_for(&vec.market_id),
            &domain,
        );
        assert!(alerts.is_empty());
    }

    #[test]
    fn missing_domain_inputs_fails_closed_not_panics() {
        let domain = DomainConfig::default();
        let vec = vector(Some(FeatureValue::Bps(dec!(999))));
        let alerts = detect_basis_alerts(&[vec], &HashMap::new(), &domain);
        assert!(alerts.is_empty());
    }

    // ── R6: detect → record → acknowledge closed loop ───────────────────────

    use quant_pivot_error::storage::StorageError;
    use quant_pivot_models::{
        domain::{BasisAlertInfo, BasisAlertListQuery, Paginated, pagination::PageRequest},
        types::BasisAlertId,
    };
    use quant_pivot_repository::traits::BasisAlertRepository;
    use std::sync::Mutex;

    /// A minimal in-memory `BasisAlertRepository`, real enough to prove the
    /// `detect_basis_alerts` → `record` → `acknowledge` wiring end to end
    /// without a database (the Postgres-backed SQL semantics for the same
    /// contract are covered separately by `pg_basis_alert.rs`, which requires
    /// Docker).
    #[derive(Default)]
    struct InMemoryBasisAlertRepo {
        rows: Mutex<Vec<BasisAlertInfo>>,
    }

    #[async_trait::async_trait]
    impl BasisAlertRepository for InMemoryBasisAlertRepo {
        async fn record(&self, alert: NewBasisAlert) -> Result<BasisAlertInfo, StorageError> {
            let row = BasisAlertInfo {
                alert_id: alert.alert_id,
                market_id: alert.market_id,
                instrument_key: alert.instrument_key,
                oracle_instrument_key: alert.oracle_instrument_key,
                basis_bps: alert.basis_bps,
                threshold_bps: alert.threshold_bps,
                as_of: alert.as_of,
                acknowledged: false,
                acknowledged_at: None,
                acknowledged_by: None,
                created_at: Utc::now(),
            };
            self.rows.lock().expect("lock").push(row.clone());
            Ok(row)
        }

        async fn latest_for_market(
            &self,
            market_id: &MarketId,
        ) -> Result<Option<BasisAlertInfo>, StorageError> {
            Ok(self
                .rows
                .lock()
                .expect("lock")
                .iter()
                .filter(|row| &row.market_id == market_id)
                .max_by_key(|row| row.as_of)
                .cloned())
        }

        async fn page(
            &self,
            query: BasisAlertListQuery,
        ) -> Result<Paginated<BasisAlertInfo>, StorageError> {
            let items: Vec<_> = self
                .rows
                .lock()
                .expect("lock")
                .iter()
                .filter(|row| !query.open_only || !row.acknowledged)
                .cloned()
                .collect();
            Ok(Paginated {
                total: items.len() as u64,
                items,
                page: 1,
                size: 0,
                has_next: false,
            })
        }

        async fn acknowledge(
            &self,
            alert_id: &BasisAlertId,
            actor: String,
        ) -> Result<BasisAlertInfo, StorageError> {
            let mut rows = self.rows.lock().expect("lock");
            let row = rows
                .iter_mut()
                .find(|row| &row.alert_id == alert_id)
                .ok_or_else(|| StorageError::NotFound {
                    entity: "quant_basis_alert",
                    id: alert_id.to_string(),
                })?;
            if !row.acknowledged {
                row.acknowledged = true;
                row.acknowledged_at = Some(Utc::now());
                row.acknowledged_by = Some(actor);
            }
            let result = row.clone();
            drop(rows);
            Ok(result)
        }
    }

    #[tokio::test]
    async fn detect_record_acknowledge_closes_the_review_loop() {
        let domain = DomainConfig::default(); // default max_basis_bps = 50
        let vec = vector(Some(FeatureValue::Bps(dec!(75))));
        let alerts = detect_basis_alerts(
            std::slice::from_ref(&vec),
            &domain_inputs_for(&vec.market_id),
            &domain,
        );
        assert_eq!(alerts.len(), 1, "exceedance produces exactly one alert");

        let repo = InMemoryBasisAlertRepo::default();
        let recorded = repo
            .record(alerts.into_iter().next().unwrap())
            .await
            .expect("record");
        assert!(!recorded.acknowledged, "newly recorded alert is open");

        // It shows up in the open-only review queue.
        let open = repo
            .page(BasisAlertListQuery {
                market_id: None,
                from: None,
                to: None,
                open_only: true,
                page: PageRequest::default(),
            })
            .await
            .expect("page");
        assert_eq!(open.items.len(), 1);

        // Acknowledging it removes it from the open queue and records who/when.
        let acked = repo
            .acknowledge(&recorded.alert_id, "alice".to_owned())
            .await
            .expect("acknowledge");
        assert!(acked.acknowledged);
        assert_eq!(acked.acknowledged_by.as_deref(), Some("alice"));

        let open_after = repo
            .page(BasisAlertListQuery {
                market_id: None,
                from: None,
                to: None,
                open_only: true,
                page: PageRequest::default(),
            })
            .await
            .expect("page");
        assert!(
            open_after.items.is_empty(),
            "acknowledged alert leaves the open review queue"
        );
    }
}
