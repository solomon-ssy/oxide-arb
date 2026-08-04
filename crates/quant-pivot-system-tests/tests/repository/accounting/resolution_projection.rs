//! Resolution projection retry, blocking, remediation, and exclusion contracts.

use chrono::{DateTime, Duration, Utc};
use quant_pivot_error::{QuantError, storage::StorageError};
use quant_pivot_models::{
    domain::{
        data_plane::{
            DomainCursorStatus, DomainSourceCheckpoint, DomainSourceCursorCasOutcome,
            UpsertDomainSourceCursor,
        },
        quant::{
            NewResolutionObservationInbox, RemediateResolutionProjection,
            ResolutionObservationProjectionInfo, ResolutionProjectionSettlement,
            ResolutionRemediationCommit, ResolutionScanCommitOutcome,
        },
    },
    entities::user::{Column as UserColumn, Entity as UserEntity},
    enums::quant::{
        ResolutionProjectionErrorCode, ResolutionProjectionStatus, ResolutionRemediationAction,
    },
    hashing::CanonicalDigest,
    types::{
        ArtifactUri, ContentHash, DomainInstrumentKey, DomainSourceId, EvmAddress, EvmBlockHash,
        EvmTransactionHash, EvmUint256, MarketId, PayoutRatio, PolicyIdempotencyKey,
        ResolutionObservationId, RoleCode, UserId, WorkerId,
    },
};
use quant_pivot_repository::{
    postgres::{PgDomainSourceCursorRepository, PgResolutionObservationRepository},
    traits::{DomainSourceCursorRepository, ResolutionObservationRepository},
};
use quant_pivot_system_tests::postgres::{PostgresClock, setup_pg};
use rust_decimal_macros::dec;
use sea_orm::{
    ColumnTrait, ConnectionTrait, DatabaseConnection, DbBackend, EntityTrait, QueryFilter,
    Statement,
};

fn hash(seed: char) -> ContentHash {
    ContentHash::parse(&format!("blake3:{}", seed.to_string().repeat(64)))
        .expect("valid content hash")
}

fn block_hash(seed: char) -> EvmBlockHash {
    EvmBlockHash::parse(format!("0x{}", seed.to_string().repeat(64))).expect("valid block hash")
}

fn cursor(finalized_block: u64, seed: char, block_time: DateTime<Utc>) -> UpsertDomainSourceCursor {
    let checkpoint_json = DomainSourceCheckpoint::PolymarketCtfResolution {
        finalized_block,
        block_hash: block_hash(seed),
        block_time,
    };
    let checkpoint_hash =
        CanonicalDigest::content_hash_json(&checkpoint_json).expect("hash resolution cursor");
    UpsertDomainSourceCursor {
        source_id: DomainSourceId::polymarket_ctf_resolution(),
        instrument_key: DomainInstrumentKey::polymarket_ctf_resolution(),
        checkpoint_json,
        checkpoint_hash,
        status: DomainCursorStatus::Live,
        last_error: None,
        updated_at: block_time,
    }
}

fn observation(resolved_at: DateTime<Utc>) -> NewResolutionObservationInbox {
    let block_hash = block_hash('b');
    let mut observation = NewResolutionObservationInbox {
        source_checkpoint_hash: hash('c'),
        source_id: DomainSourceId::polymarket_ctf_resolution(),
        instrument_key: DomainInstrumentKey::polymarket_ctf_resolution(),
        market_id: MarketId::new("resolution-remediation-market"),
        denominator: EvmUint256::parse("2").expect("resolution denominator"),
        yes_numerator: EvmUint256::parse("1").expect("YES numerator"),
        no_numerator: EvmUint256::parse("1").expect("NO numerator"),
        yes_payout_ratio: PayoutRatio::try_new(dec!(0.5)).expect("YES payout"),
        no_payout_ratio: PayoutRatio::try_new(dec!(0.5)).expect("NO payout"),
        oracle: EvmAddress::parse("0x1111111111111111111111111111111111111111")
            .expect("resolution oracle"),
        question_id: format!("0x{}", "2".repeat(64)),
        transaction_hash: EvmTransactionHash::parse(format!("0x{}", "3".repeat(64)))
            .expect("resolution transaction hash"),
        block_number: 101,
        block_hash: block_hash.clone(),
        log_index: 7,
        resolved_at,
        raw_payload_hash: ContentHash::from_bytes([0; 32]),
        raw_uri: ArtifactUri::parse(format!("polygon://resolution/101/{}/7", "3".repeat(64)))
            .expect("resolution raw URI"),
        provider_revision: block_hash,
    };
    observation.raw_payload_hash = observation
        .expected_raw_payload_hash()
        .expect("hash resolution observation");
    observation
        .validate()
        .expect("valid resolution observation");
    observation
}

async fn seed_projection(
    db: &DatabaseConnection,
    now: DateTime<Utc>,
) -> (PgResolutionObservationRepository, ResolutionObservationId) {
    let cursors = PgDomainSourceCursorRepository::new(db.clone());
    let initial = cursor(100, 'a', now - Duration::minutes(2));
    assert!(matches!(
        cursors
            .compare_and_set(None, initial.clone())
            .await
            .expect("initialize resolution cursor"),
        DomainSourceCursorCasOutcome::Advanced(_)
    ));

    let repository = PgResolutionObservationRepository::new(db.clone());
    let advanced = cursor(101, 'b', now - Duration::minutes(1));
    let observation = observation(now - Duration::minutes(1));
    let observation_id =
        ResolutionObservationId::from_checkpoint_hash(&observation.source_checkpoint_hash);
    assert!(matches!(
        repository
            .commit_scan(initial.checkpoint_hash, advanced, vec![observation])
            .await
            .expect("commit resolution scan"),
        ResolutionScanCommitOutcome::Committed {
            inserted: 1,
            existing: 0,
            ..
        }
    ));
    (repository, observation_id)
}

async fn block_projection(
    db: &DatabaseConnection,
    repository: &PgResolutionObservationRepository,
    observation_id: ResolutionObservationId,
) -> ResolutionObservationProjectionInfo {
    let first_worker = WorkerId::from_v7();
    let first_claim = repository
        .claim_pending(first_worker, 60, 10)
        .await
        .expect("claim resolution projection")
        .pop()
        .expect("one resolution projection");
    assert_eq!(
        first_claim.projection.status,
        ResolutionProjectionStatus::Delivering
    );
    let retried = repository
        .settle(
            observation_id,
            first_worker,
            ResolutionProjectionSettlement::RetryScheduled {
                retry_delay_secs: 60,
                error_code: ResolutionProjectionErrorCode::ExternalDependencyUnavailable,
                error: "canonical fact store is temporarily unavailable".to_owned(),
            },
        )
        .await
        .expect("schedule projection retry");
    assert_eq!(retried.status, ResolutionProjectionStatus::RetryScheduled);
    assert!(
        retried
            .next_attempt_at
            .is_some_and(|retry_at| retry_at > retried.updated_at)
    );
    assert!(
        repository
            .claim_pending(WorkerId::from_v7(), 60, 10)
            .await
            .expect("scan before retry is due")
            .is_empty(),
        "a scheduled retry must not be claimed early"
    );
    db.execute_raw(Statement::from_sql_and_values(
        DbBackend::Postgres,
        "UPDATE quant_resolution_observation_projection
         SET next_attempt_at = statement_timestamp() - INTERVAL '1 millisecond'
         WHERE resolution_observation_id = $1",
        [observation_id.as_uuid().into()],
    ))
    .await
    .expect("release retry using database time");

    let mapping_worker = WorkerId::from_v7();
    let mapping_claim = repository
        .claim_pending(mapping_worker, 60, 10)
        .await
        .expect("claim released retry")
        .pop()
        .expect("released retry is claimable");
    assert_eq!(
        mapping_claim.projection.status,
        ResolutionProjectionStatus::Delivering
    );
    assert!(mapping_claim.projection.last_error_code.is_none());
    let mapping = repository
        .settle(
            observation_id,
            mapping_worker,
            ResolutionProjectionSettlement::MappingBlocked {
                error_code: ResolutionProjectionErrorCode::CatalogMappingUnavailable,
                error: "catalog market mapping is not available".to_owned(),
            },
        )
        .await
        .expect("block missing catalog mapping");
    assert_eq!(mapping.status, ResolutionProjectionStatus::MappingBlocked);
    assert!(
        repository
            .claim_pending(WorkerId::from_v7(), 60, 10)
            .await
            .expect("scan mapping-blocked queue")
            .is_empty(),
        "MappingBlocked work requires governed remediation"
    );
    mapping
}

async fn requeue_projection(
    db: &DatabaseConnection,
    repository: &PgResolutionObservationRepository,
    observation_id: ResolutionObservationId,
    mapping: &ResolutionObservationProjectionInfo,
) -> UserId {
    let admin = UserEntity::find()
        .filter(UserColumn::Username.eq("admin"))
        .one(db)
        .await
        .expect("load bootstrap admin")
        .expect("bootstrap admin exists");
    let requeue = RemediateResolutionProjection {
        resolution_observation_id: observation_id,
        expected_revision: mapping.revision,
        action: ResolutionRemediationAction::Requeue,
        idempotency_key: PolicyIdempotencyKey::parse("resolution-requeue-101")
            .expect("requeue idempotency key"),
        reason_code: "catalog_mapping_repaired".to_owned(),
        operator_note: "The catalog mapping was verified and the immutable source can be replayed."
            .to_owned(),
        actor_user_id: admin.id,
        actor_role: RoleCode::new("super_admin"),
    };
    let requeued = repository
        .remediate(requeue.clone())
        .await
        .expect("requeue blocked projection");
    assert_eq!(
        requeued.projection.status,
        ResolutionProjectionStatus::Pending
    );
    assert!(!requeued.replayed);
    let replayed = repository
        .remediate(requeue.clone())
        .await
        .expect("replay exact remediation request");
    assert!(replayed.replayed);
    assert_eq!(replayed.remediation, requeued.remediation);
    let mut conflicting_replay = requeue;
    "Different content under the same key.".clone_into(&mut conflicting_replay.operator_note);
    assert!(matches!(
        repository.remediate(conflicting_replay).await,
        Err(QuantError::Storage(StorageError::StateConflict { .. }))
    ));
    admin.id
}

async fn exclude_projection(
    repository: &PgResolutionObservationRepository,
    observation_id: ResolutionObservationId,
    admin_id: UserId,
) -> ResolutionRemediationCommit {
    let quarantine_worker = WorkerId::from_v7();
    repository
        .claim_pending(quarantine_worker, 60, 10)
        .await
        .expect("claim remediated projection")
        .pop()
        .expect("requeued projection is claimable");
    let quarantined = repository
        .settle(
            observation_id,
            quarantine_worker,
            ResolutionProjectionSettlement::Quarantined {
                error_code: ResolutionProjectionErrorCode::InvalidObservation,
                error: "the payout vector contradicts canonical market metadata".to_owned(),
            },
        )
        .await
        .expect("quarantine deterministic invalid data");
    assert_eq!(quarantined.status, ResolutionProjectionStatus::Quarantined);
    let excluded = repository
        .remediate(RemediateResolutionProjection {
            resolution_observation_id: observation_id,
            expected_revision: quarantined.revision,
            action: ResolutionRemediationAction::Exclude,
            idempotency_key: PolicyIdempotencyKey::parse("resolution-exclude-101")
                .expect("exclude idempotency key"),
            reason_code: "invalid_source_excluded".to_owned(),
            operator_note:
                "The immutable source remains in the inbox and is excluded as missing-label evidence."
                    .to_owned(),
            actor_user_id: admin_id,
            actor_role: RoleCode::new("super_admin"),
        })
        .await
        .expect("exclude quarantined projection");
    assert_eq!(
        excluded.projection.status,
        ResolutionProjectionStatus::Excluded
    );
    excluded
}

async fn assert_remediation_evidence(
    db: &DatabaseConnection,
    repository: &PgResolutionObservationRepository,
    observation_id: ResolutionObservationId,
    excluded: &ResolutionRemediationCommit,
) {
    let attention = repository
        .list_attention(10)
        .await
        .expect("list Truth Ops attention items");
    assert_eq!(attention.len(), 1);
    assert_eq!(
        attention[0].observation.resolution_observation_id,
        observation_id
    );
    assert_eq!(attention[0].remediations.len(), 2);
    assert_eq!(
        attention[0]
            .remediations
            .iter()
            .map(|entry| entry.action)
            .collect::<Vec<_>>(),
        vec![
            ResolutionRemediationAction::Requeue,
            ResolutionRemediationAction::Exclude
        ]
    );
    let worm_update = db
        .execute_raw(Statement::from_sql_and_values(
            DbBackend::Postgres,
            "UPDATE quant_resolution_projection_remediation
             SET operator_note = 'tampered'
             WHERE remediation_id = $1",
            [excluded.remediation.remediation_id.as_uuid().into()],
        ))
        .await;
    assert!(worm_update.is_err(), "remediation evidence must be WORM");

    let cutoff = db.statement_time().await;
    let barrier = repository
        .barrier(cutoff)
        .await
        .expect("read terminal resolution barrier");
    assert_eq!(barrier.unresolved_count, 0);
    assert_eq!(barrier.excluded_count, 1);
    assert!(barrier.is_complete());
    assert!(
        repository
            .find_by_checkpoint(hash('c'))
            .await
            .expect("reload immutable observation")
            .is_some(),
        "Exclude must never delete the immutable source observation"
    );
}

pub async fn remediation_lifecycle_contracts() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let now = db.statement_time().await;
    let (repository, observation_id) = seed_projection(&db, now).await;
    let mapping = block_projection(&db, &repository, observation_id).await;
    let admin_id = requeue_projection(&db, &repository, observation_id, &mapping).await;
    let excluded = exclude_projection(&repository, observation_id, admin_id).await;
    assert_remediation_evidence(&db, &repository, observation_id, &excluded).await;
}
