//! Durable cold-start FSM and governed activation integration tests.

use chrono::Utc;
use quant_pivot_models::{
    domain::{ActivateBootstrapState, NewRuntimeConfigApproval, NewRuntimeConfigVersion},
    entities::system_bootstrap_transition,
    enums::{
        runtime_config::{RuntimeConfigApprovalDecision, RuntimeConfigVersionSource},
        system::BootstrapPhase,
    },
    hashing::CanonicalDigest,
    runtime_config::{RUNTIME_CONFIG_SCHEMA_VERSION, RuntimeConfig},
    types::{RuntimeConfigApprovalId, RuntimeConfigVersionId},
};
use quant_pivot_repository::{
    postgres::{PgRuntimeConfigVersionRepository, PgSystemRuntimeStateRepository},
    traits::{RuntimeConfigVersionRepository, SystemRuntimeStateRepository},
};
use quant_pivot_test_support::pg::setup_pg;
use sea_orm::{ColumnTrait, EntityTrait, QueryFilter, QueryOrder};

#[tokio::test]
#[ignore = "requires Docker"]
async fn bootstrap_transitions_are_monotonic_restart_safe_and_approval_bound() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let state = PgSystemRuntimeStateRepository::new(db.clone());
    let configs = PgRuntimeConfigVersionRepository::new(db.clone());

    let collecting = state
        .begin_baseline_collection()
        .await
        .expect("begin baseline collection");
    assert_eq!(
        collecting.bootstrap_phase,
        BootstrapPhase::CollectingBaseline
    );
    assert_eq!(collecting.state_revision, 1);
    let collecting_restart = state
        .begin_baseline_collection()
        .await
        .expect("collecting restart");
    assert_eq!(collecting_restart.state_revision, 1);

    let awaiting = state
        .mark_catalog_baseline_ready()
        .await
        .expect("catalog baseline ready");
    assert_eq!(awaiting.bootstrap_phase, BootstrapPhase::AwaitingActivation);
    assert_eq!(awaiting.state_revision, 2);
    let awaiting_restart = state
        .begin_baseline_collection()
        .await
        .expect("awaiting restart");
    assert_eq!(
        awaiting_restart.bootstrap_phase,
        BootstrapPhase::AwaitingActivation
    );
    assert_eq!(awaiting_restart.state_revision, 2);

    let config_json = RuntimeConfig::default().to_json();
    let config_hash = CanonicalDigest::content_hash_json(&config_json).expect("config hash");
    let version_id = RuntimeConfigVersionId::from_v7();
    configs
        .create_version(NewRuntimeConfigVersion {
            runtime_config_version_id: version_id.clone(),
            config_hash: config_hash.clone(),
            schema_version: RUNTIME_CONFIG_SCHEMA_VERSION,
            config_json,
            source: RuntimeConfigVersionSource::Operator,
            created_by: "config-author".to_owned(),
            reason: "bootstrap integration fixture".to_owned(),
        })
        .await
        .expect("create runtime config");
    let approval_id = RuntimeConfigApprovalId::from_v7();
    configs
        .record_approval(NewRuntimeConfigApproval {
            runtime_config_approval_id: approval_id.clone(),
            runtime_config_version_id: version_id.clone(),
            config_hash,
            decision: RuntimeConfigApprovalDecision::Approved,
            decided_by: "approver".to_owned(),
            reason: "reviewed exact bootstrap config".to_owned(),
            decided_at: Utc::now(),
            expires_at: None,
        })
        .await
        .expect("record config approval");

    let stale_revision = state
        .activate_bootstrap(ActivateBootstrapState {
            runtime_config_version_id: version_id.clone(),
            runtime_config_approval_id: approval_id.clone(),
            bootstrap_contract_version: awaiting.bootstrap_contract_version,
            expected_state_revision: awaiting.state_revision - 1,
            actor: "activator".to_owned(),
            acting_role: "system-operator".to_owned(),
            reason: "stale activation attempt".to_owned(),
            report_only_forced_ack: true,
            require_approver_activator_separation: true,
        })
        .await;
    assert!(stale_revision.is_err());
    assert_eq!(
        state
            .load()
            .await
            .expect("load state after stale activation")
            .expect("runtime state")
            .state_revision,
        2
    );

    let same_operator = state
        .activate_bootstrap(ActivateBootstrapState {
            runtime_config_version_id: version_id.clone(),
            runtime_config_approval_id: approval_id.clone(),
            bootstrap_contract_version: awaiting.bootstrap_contract_version,
            expected_state_revision: awaiting.state_revision,
            actor: "approver".to_owned(),
            acting_role: "system-operator".to_owned(),
            reason: "separation violation".to_owned(),
            report_only_forced_ack: true,
            require_approver_activator_separation: true,
        })
        .await;
    assert!(same_operator.is_err());

    let expected_version_id = version_id.clone();
    let activated = state
        .activate_bootstrap(ActivateBootstrapState {
            runtime_config_version_id: version_id,
            runtime_config_approval_id: approval_id.clone(),
            bootstrap_contract_version: awaiting.bootstrap_contract_version,
            expected_state_revision: awaiting.state_revision,
            actor: "activator".to_owned(),
            acting_role: "system-operator".to_owned(),
            reason: "explicit cold-start activation".to_owned(),
            report_only_forced_ack: true,
            require_approver_activator_separation: true,
        })
        .await
        .expect("activate bootstrap");
    assert_eq!(activated.state.bootstrap_phase, BootstrapPhase::Active);
    assert_eq!(activated.state.state_revision, 3);
    let active_config = configs
        .load_active_at(Utc::now())
        .await
        .expect("load activated config by decision time")
        .expect("activated config must be visible");
    assert_eq!(active_config.runtime_config_version_id, expected_version_id);

    let active_restart = state
        .begin_baseline_collection()
        .await
        .expect("active restart");
    assert_eq!(active_restart.bootstrap_phase, BootstrapPhase::Active);
    assert_eq!(active_restart.state_revision, 3);

    let transitions = system_bootstrap_transition::Entity::find()
        .filter(
            system_bootstrap_transition::Column::BootstrapContractVersion
                .eq(active_restart.bootstrap_contract_version),
        )
        .order_by_asc(system_bootstrap_transition::Column::StateRevision)
        .all(&db)
        .await
        .expect("load bootstrap transition audit");
    assert_eq!(
        transitions
            .iter()
            .map(|row| row.state_revision)
            .collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
    assert_eq!(
        transitions[2].runtime_config_approval_id.as_ref(),
        Some(&approval_id)
    );
}
