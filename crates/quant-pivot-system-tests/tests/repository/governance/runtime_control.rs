//! Atomic runtime-control persistence contracts.

use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::governance::RuntimeControlUpdate,
    entities::system_runtime_control_transition::{
        Column as RuntimeControlTransitionColumn, Entity as RuntimeControlTransitionEntity,
    },
    enums::{
        execution::KillSwitchState, quant::EntryAuthorizationPolicy,
        settlement::SettlementWritePolicy,
    },
};
use quant_pivot_repository::{
    postgres::PgRuntimeControlRepository, traits::RuntimeControlRepository,
};
use quant_pivot_system_tests::postgres::setup_pg;
use sea_orm::{ColumnTrait, EntityTrait, QueryFilter, QueryOrder};

pub async fn singleton_cas_atomic_rejects() {
    let (pool, _database) = setup_pg().await;
    let db = pool.connection().clone();
    let repository = PgRuntimeControlRepository::new(db.clone());

    let initial = repository.load().await.expect("load fresh-boot controls");
    assert_eq!(initial.revision, 0);
    assert_eq!(
        initial.entry_authorization_policy,
        EntryAuthorizationPolicy::OperatorApprovalRequired
    );
    assert_eq!(
        initial.settlement_write_policy,
        SettlementWritePolicy::Disabled
    );
    assert_eq!(initial.kill_switch_state, KillSwitchState::Closed);
    assert!(!initial.kill_switch_requires_ack);

    let automatic_policy = repository
        .compare_and_set(RuntimeControlUpdate {
            expected_revision: initial.revision,
            entry_authorization_policy: Some(EntryAuthorizationPolicy::PolicyAutomatic),
            settlement_write_policy: None,
            kill_switch_state: None,
            kill_switch_requires_ack: None,
            actor: "runtime-admin".to_owned(),
            reason: "authorization preflight approved policy automatic".to_owned(),
        })
        .await
        .expect("transition entry authorization policy");
    assert_eq!(automatic_policy.revision, 1);
    assert_eq!(
        automatic_policy.entry_authorization_policy,
        EntryAuthorizationPolicy::PolicyAutomatic
    );

    let idempotent = repository
        .compare_and_set(RuntimeControlUpdate {
            expected_revision: automatic_policy.revision,
            entry_authorization_policy: Some(EntryAuthorizationPolicy::PolicyAutomatic),
            settlement_write_policy: None,
            kill_switch_state: None,
            kill_switch_requires_ack: None,
            actor: "runtime-admin".to_owned(),
            reason: "idempotent retry".to_owned(),
        })
        .await
        .expect("idempotent retry");
    assert_eq!(idempotent.revision, automatic_policy.revision);

    let malformed = repository
        .compare_and_set(RuntimeControlUpdate {
            expected_revision: automatic_policy.revision,
            entry_authorization_policy: Some(EntryAuthorizationPolicy::PolicyAutomatic),
            settlement_write_policy: Some(SettlementWritePolicy::PolicyAutomatic),
            kill_switch_state: None,
            kill_switch_requires_ack: None,
            actor: "runtime-admin".to_owned(),
            reason: "invalid multi-domain update".to_owned(),
        })
        .await;
    assert!(matches!(
        malformed,
        Err(StorageError::InvariantViolation { .. })
    ));

    let stale = repository
        .compare_and_set(RuntimeControlUpdate {
            expected_revision: initial.revision,
            entry_authorization_policy: None,
            settlement_write_policy: Some(SettlementWritePolicy::GovernedCanary),
            kill_switch_state: None,
            kill_switch_requires_ack: None,
            actor: "runtime-admin".to_owned(),
            reason: "stale browser state".to_owned(),
        })
        .await;
    assert!(matches!(stale, Err(StorageError::StateConflict { .. })));

    let left = PgRuntimeControlRepository::new(db.clone());
    let right = PgRuntimeControlRepository::new(db.clone());
    let (policy_result, halt_result) = tokio::join!(
        left.compare_and_set(RuntimeControlUpdate {
            expected_revision: automatic_policy.revision,
            entry_authorization_policy: None,
            settlement_write_policy: Some(SettlementWritePolicy::GovernedCanary),
            kill_switch_state: None,
            kill_switch_requires_ack: None,
            actor: "settlement-admin".to_owned(),
            reason: "enable governed canary".to_owned(),
        }),
        right.compare_and_set(RuntimeControlUpdate {
            expected_revision: automatic_policy.revision,
            entry_authorization_policy: None,
            settlement_write_policy: None,
            kill_switch_state: Some(KillSwitchState::ExecutionHalted),
            kill_switch_requires_ack: Some(true),
            actor: "incident-admin".to_owned(),
            reason: "halt execution".to_owned(),
        }),
    );
    assert_eq!(
        usize::from(policy_result.is_ok()) + usize::from(halt_result.is_ok()),
        1,
        "one expected-revision writer must win"
    );
    assert_eq!(
        usize::from(matches!(
            policy_result,
            Err(StorageError::StateConflict { .. })
        )) + usize::from(matches!(
            halt_result,
            Err(StorageError::StateConflict { .. })
        )),
        1,
        "the losing writer must receive a typed CAS conflict"
    );

    let final_state = repository.load().await.expect("load winning state");
    assert_eq!(final_state.revision, 2);
    let transitions = RuntimeControlTransitionEntity::find()
        .filter(RuntimeControlTransitionColumn::ToRevision.lte(final_state.revision))
        .order_by_asc(RuntimeControlTransitionColumn::ToRevision)
        .all(&db)
        .await
        .expect("load transition audit");
    assert_eq!(
        transitions.len(),
        2,
        "idempotent retries and rejected writes must not append audit rows"
    );
    assert_eq!(
        transitions
            .iter()
            .map(|transition| transition.to_revision)
            .collect::<Vec<_>>(),
        vec![1, 2]
    );
}
