//! Durable cold-start FSM integration tests for the typed policy boot bundle.

use chrono::Duration;
use quant_pivot_models::{
    domain::ActivateBootstrapState, entities::system_bootstrap_transition,
    enums::system::BootstrapPhase,
};
use quant_pivot_repository::{
    postgres::{PgPolicyRepository, PgSystemRuntimeStateRepository},
    traits::{PolicyRepository, SystemRuntimeStateRepository},
};
use quant_pivot_test_support::{pg::setup_pg, policy_fixtures::bootstrap_default_policy_bundle};
use sea_orm::{ColumnTrait, EntityTrait, QueryFilter, QueryOrder};

#[tokio::test]
#[ignore = "requires Docker"]
async fn bootstrap_transitions_are_monotonic_restart_safe_and_policy_bundle_bound() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let state = PgSystemRuntimeStateRepository::new(db.clone());
    let policies = PgPolicyRepository::new(db.clone());

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

    let snapshot_id = bootstrap_default_policy_bundle(
        &db,
        "bootstrap-policy-author",
        "typed boot policy fixture",
    )
    .await;
    let latest_activation = policies
        .load_current_activation(None)
        .await
        .expect("load latest policy activation")
        .expect("six-resource bundle has activations");
    let first_activation_at = policies
        .list_activations(None, 10)
        .await
        .expect("list policy activations")
        .into_iter()
        .map(|activation| activation.activated_at)
        .min()
        .expect("six-resource bundle has activations");

    let stale_revision = state
        .activate_bootstrap(ActivateBootstrapState {
            bootstrap_contract_version: awaiting.bootstrap_contract_version,
            expected_state_revision: awaiting.state_revision - 1,
            actor: "bootstrap-operator".to_owned(),
            acting_role: "system-operator".to_owned(),
            reason: "stale activation attempt".to_owned(),
            report_only_forced_ack: true,
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

    let activated = state
        .activate_bootstrap(ActivateBootstrapState {
            bootstrap_contract_version: awaiting.bootstrap_contract_version,
            expected_state_revision: awaiting.state_revision,
            actor: "bootstrap-operator".to_owned(),
            acting_role: "system-operator".to_owned(),
            reason: "explicit cold-start activation".to_owned(),
            report_only_forced_ack: true,
        })
        .await
        .expect("activate bootstrap");
    assert_eq!(activated.state.bootstrap_phase, BootstrapPhase::Active);
    assert_eq!(activated.state.state_revision, 3);

    let current = policies
        .load_current()
        .await
        .expect("load active policy bundle")
        .expect("active policy bundle");
    assert_eq!(current.decision_policy_snapshot_id, snapshot_id);
    assert!(
        policies
            .load_active_at(first_activation_at - Duration::microseconds(1))
            .await
            .expect("load policy before final resource activation")
            .is_none()
    );
    assert_eq!(
        policies
            .load_active_at(latest_activation.activated_at)
            .await
            .expect("load policy at activation boundary")
            .expect("policy is active at boundary")
            .decision_policy_snapshot_id,
        snapshot_id
    );

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
    assert!(transitions[2].report_only_forced_ack);
}
