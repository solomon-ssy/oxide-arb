//! Entry-authorization preflight engine.
//!
//! A read-only, side-effect-free aggregate run before enabling
//! policy-automatic authorization. Every check with a real data source is
//! evaluated against live state and failure denies the upgrade.

use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use quant_pivot_error::{QuantError, QuantResult};
use quant_pivot_models::{
    config::DeployConfig,
    domain::{
        governance::{AuthorizationPreflightCheck, AuthorizationPreflightReport},
        ports::DataQualityPort,
        quant::RepresentedRouteSet,
    },
    enums::{
        execution::KillSwitchState,
        quant::{EntryAuthorizationPolicy, ExecutionWalletKind},
    },
    runtime_config::DecisionPolicySnapshot,
    types::ModelVersionId,
};
use quant_pivot_repository::traits::{
    CapitalAllocationRepository, ModelRegistryRepository, ReconciliationRepository,
    ShadowComparisonRepository,
};
use rust_decimal::Decimal;

use crate::{
    execution::ExitMonitorHealthHandle,
    governance::{
        RuntimeControlsHandle,
        quality_gate_load::{active_load_ok, shadow_load_ok},
    },
    runtime_config::DecisionPolicyStore,
};

/// Entry-authorization upgrade preflight boundary.
#[async_trait]
pub trait AuthorizationPreflight: Send + Sync {
    /// Aggregate the read-only check list for a target upgrade mode.
    ///
    /// Genuine infrastructure failures (DB down) propagate as typed errors
    /// (mapped to 5xx). Business denials are captured as failed hard checks in
    /// the returned [`AuthorizationPreflightReport`] (`passed == false`).
    async fn run(
        &self,
        target: EntryAuthorizationPolicy,
    ) -> QuantResult<AuthorizationPreflightReport>;
}

/// Read-only dependency bundle for [`DefaultAuthorizationPreflight`].
pub struct AuthorizationPreflightDeps {
    pub deploy: Arc<DeployConfig>,
    pub config_store: Arc<DecisionPolicyStore>,
    pub data_quality: Arc<dyn DataQualityPort>,
    pub model_registry: Arc<dyn ModelRegistryRepository>,
    pub shadow_comparison: Arc<dyn ShadowComparisonRepository>,
    pub reconciliation: Arc<dyn ReconciliationRepository>,
    pub capital: Arc<dyn CapitalAllocationRepository>,
    pub runtime_controls: RuntimeControlsHandle,
    /// Exit-monitor health: policy-automatic authorization requires a live worker.
    pub exit_monitor_health: ExitMonitorHealthHandle,
}

/// Spec preflight engine.
pub struct DefaultAuthorizationPreflight {
    deps: AuthorizationPreflightDeps,
}

impl DefaultAuthorizationPreflight {
    #[must_use]
    pub const fn new(deps: AuthorizationPreflightDeps) -> Self {
        Self { deps }
    }
}

#[async_trait]
impl AuthorizationPreflight for DefaultAuthorizationPreflight {
    async fn run(
        &self,
        target: EntryAuthorizationPolicy,
    ) -> QuantResult<AuthorizationPreflightReport> {
        let config = self.deps.config_store.current();
        let now = Utc::now();
        let mut checks = vec![
            self.check_credentials(),
            self.check_jwt(target),
            self.check_order_client_ready(),
            self.check_route_champions(&config, "model_available")
                .await?,
            self.check_data_quality(&config),
            self.check_no_unresolvable().await?,
            self.check_no_impaired_capital().await?,
            Self::check_runtime_config_valid(&config),
        ];

        if target == EntryAuthorizationPolicy::PolicyAutomatic {
            checks.push(
                self.check_route_champions(&config, "active_route_models")
                    .await?,
            );
            checks.push(self.check_shadow_period(&config, now).await?);
            checks.push(Self::check_auto_policy(&config));
            checks.push(self.check_kill_switch_closed());
            checks.push(Self::check_capital_budget(&config));
            checks.push(self.check_exit_monitor(now));
        }

        Ok(AuthorizationPreflightReport::new(target, checks))
    }
}

impl DefaultAuthorizationPreflight {
    fn check_credentials(&self) -> AuthorizationPreflightCheck {
        let key = self.deps.deploy.keys.private_key_present();
        let funder = self
            .deps
            .deploy
            .quant
            .account
            .funder
            .as_deref()
            .is_some_and(|funder| !funder.trim().is_empty());
        // Proxy / Gnosis Safe topologies move money (e.g. settlement redeem)
        // through the gasless relayer, so the relayer API credentials are a hard
        // requirement before policy-automatic authorization. EOA settles
        // on-chain directly and needs none.
        let wallet_kind = self.deps.deploy.quant.account.wallet_kind;
        let relayer_required = matches!(
            wallet_kind,
            ExecutionWalletKind::Proxy | ExecutionWalletKind::GnosisSafe
        );
        let relayer_ok = !relayer_required || self.deps.deploy.polymarket.relayer.is_ready();

        let mut missing = Vec::new();
        if !key {
            missing.push("keys.private_key");
        }
        if !funder {
            missing.push("quant.account.funder");
        }
        if !relayer_ok {
            missing.push("polymarket.relayer.api_key + api_key_address");
        }
        let passed = key && funder && relayer_ok;
        let detail = if passed {
            format!(
                "private_key + quant.account.funder present (wallet_kind={})",
                wallet_kind.as_str()
            )
        } else {
            format!("missing {}", missing.join(", "))
        };
        AuthorizationPreflightCheck::hard("credentials_loaded", passed, detail)
    }

    fn check_jwt(&self, target: EntryAuthorizationPolicy) -> AuthorizationPreflightCheck {
        let configured = self.deps.deploy.web.has_jwt_signing_key();
        let detail = if configured {
            "web.jwt HS256 signing key is configured".to_owned()
        } else {
            "web.jwt signing key is missing or invalid".to_owned()
        };
        if target == EntryAuthorizationPolicy::PolicyAutomatic {
            AuthorizationPreflightCheck::hard("jwt_signing_key_configured", configured, detail)
        } else {
            AuthorizationPreflightCheck::soft("jwt_signing_key_configured", configured, detail)
        }
    }

    fn check_order_client_ready(&self) -> AuthorizationPreflightCheck {
        let keys_ok = self.deps.deploy.keys.private_key_present();
        let url_ok = !self.deps.deploy.polymarket.clob_base_url.trim().is_empty();
        AuthorizationPreflightCheck::hard(
            "order_client_ready",
            keys_ok && url_ok,
            "boot-level keystore and CLOB endpoint are configured; live connectivity is checked by the execution bundle",
        )
    }

    fn check_data_quality(&self, config: &DecisionPolicySnapshot) -> AuthorizationPreflightCheck {
        let snapshot = self.deps.data_quality.snapshot();
        let stale_bps = if snapshot.total_tokens == 0 {
            u64::MAX
        } else {
            snapshot
                .stale
                .checked_mul(10_000)
                .map_or(u64::MAX, |scaled| scaled / snapshot.total_tokens)
        };
        let green = snapshot.total_tokens > 0
            && !snapshot.ingest_lag_exceeded
            && snapshot.insufficient == 0
            && stale_bps <= config.recommendation.data_quality.max_stale_book_ratio_bps;
        let detail = format!(
            "tokens={}, insufficient={}, stale={}, ingest_lag_exceeded={}",
            snapshot.total_tokens,
            snapshot.insufficient,
            snapshot.stale,
            snapshot.ingest_lag_exceeded
        );
        AuthorizationPreflightCheck::hard("data_quality_green", green, detail)
    }

    async fn check_no_unresolvable(&self) -> QuantResult<AuthorizationPreflightCheck> {
        let blocked = self.deps.reconciliation.has_unresolvable().await?;
        Ok(AuthorizationPreflightCheck::hard(
            "no_unresolvable_reconciliation",
            !blocked,
            if blocked {
                "unresolved `unresolvable` reconciliations require operator action"
            } else {
                "no unresolvable reconciliations"
            },
        ))
    }

    async fn check_no_impaired_capital(&self) -> QuantResult<AuthorizationPreflightCheck> {
        let impaired = self.deps.capital.has_impaired().await?;
        Ok(AuthorizationPreflightCheck::hard(
            "no_impaired_capital",
            !impaired,
            if impaired {
                "impaired capital allocations must be resolved before trading"
            } else {
                "no impaired capital allocations"
            },
        ))
    }

    fn check_runtime_config_valid(config: &DecisionPolicySnapshot) -> AuthorizationPreflightCheck {
        let report = config.validate_runtime_config();
        let passed = !report.has_errors();
        let detail = if passed {
            "portfolio / execution / risk envelope config valid".to_owned()
        } else {
            report.to_string()
        };
        AuthorizationPreflightCheck::hard("runtime_config_valid", passed, detail)
    }

    async fn check_route_champions(
        &self,
        config: &DecisionPolicySnapshot,
        check_name: &'static str,
    ) -> QuantResult<AuthorizationPreflightCheck> {
        let routes = configured_routes(config).map_err(QuantError::config)?;
        let mut failures = Vec::new();
        for route in &routes.routes {
            let binding = match config.model_routing.model.champion(*route) {
                Ok(binding) => binding,
                Err(error) => {
                    failures.push(format!("{}: {error}", route.as_str()));
                    continue;
                }
            };
            if let Err(detail) = self.active_status(binding.model_version_id).await? {
                failures.push(format!("{}: {detail}", route.as_str()));
            }
        }
        let passed = failures.is_empty();
        let detail = if passed {
            format!(
                "all configured Route champions are loadable: {}",
                route_names(&routes)
            )
        } else {
            failures.join("; ")
        };
        Ok(AuthorizationPreflightCheck::hard(
            check_name, passed, detail,
        ))
    }

    async fn check_shadow_period(
        &self,
        config: &DecisionPolicySnapshot,
        now: DateTime<Utc>,
    ) -> QuantResult<AuthorizationPreflightCheck> {
        let routes = configured_routes(config).map_err(QuantError::config)?;
        let required = config
            .profile_artifacts
            .research_method
            .model_promotion
            .required_shadow_window_secs;
        let required_secs = i64::try_from(required)
            .map_err(|error| QuantError::config(format!("shadow window exceeds i64: {error}")))?;
        let mut checked = 0_u32;
        let mut failures = Vec::new();
        for route in &routes.routes {
            let binding = match config.model_routing.model.route_binding(*route) {
                Ok(binding) => binding,
                Err(error) => {
                    failures.push(format!("{}: {error}", route.as_str()));
                    continue;
                }
            };
            let Some(shadow) = &binding.shadow else {
                continue;
            };
            checked = checked.saturating_add(1);
            if let Err(detail) = self.shadow_status(shadow.model_version_id).await? {
                failures.push(format!("{}: {detail}", route.as_str()));
                continue;
            }
            let summary = self
                .deps
                .shadow_comparison
                .summary(&shadow.model_version_id, shadow.bound_at)
                .await?;
            let covered = match (summary.window_start, summary.window_end) {
                (Some(start), Some(end)) => {
                    (end - start).num_seconds() >= required_secs
                        && now - shadow.bound_at >= Duration::seconds(required_secs)
                }
                _ => false,
            };
            if summary.sample_count == 0 || summary.any_hard_divergence || !covered {
                failures.push(format!(
                    "{}: samples={}, hard_divergence={}, window_covered={covered}",
                    route.as_str(),
                    summary.sample_count,
                    summary.any_hard_divergence
                ));
            }
        }
        let passed = failures.is_empty();
        let detail = if passed {
            format!("{checked} configured Route shadows satisfy the {required}s comparison window")
        } else {
            failures.join("; ")
        };
        Ok(AuthorizationPreflightCheck::hard(
            "shadow_period_complete",
            passed,
            detail,
        ))
    }

    fn check_auto_policy(config: &DecisionPolicySnapshot) -> AuthorizationPreflightCheck {
        let auto = &config
            .execution_authorization_policy
            .policy_automatic_limits;
        let ok = auto.max_orders_per_report > 0
            && auto.max_total_usd_per_report.value > Decimal::ZERO
            && config
                .execution_risk
                .portfolio
                .budget
                .max_open_capital_usd
                .value
                > Decimal::ZERO
            && config
                .execution_risk
                .portfolio
                .exposure_limits
                .max_open_recommendations
                > 0;
        AuthorizationPreflightCheck::hard(
            "auto_policy_valid",
            ok,
            "execution automation caps and governed portfolio capital thresholds are positive",
        )
    }

    fn check_kill_switch_closed(&self) -> AuthorizationPreflightCheck {
        let state = self.deps.runtime_controls.kill_switch_state();
        AuthorizationPreflightCheck::hard(
            "kill_switch_closed",
            state == KillSwitchState::Closed,
            format!("kill switch state = {}", state.as_str()),
        )
    }

    fn check_capital_budget(config: &DecisionPolicySnapshot) -> AuthorizationPreflightCheck {
        let budget = config
            .execution_risk
            .portfolio
            .budget
            .total_budget_usd
            .value;
        let ok = budget > Decimal::ZERO;
        AuthorizationPreflightCheck::hard(
            "max_capital_budget_set",
            ok,
            "portfolio.budget.total_budget_usd must be > 0 for policy-automatic authorization",
        )
    }

    fn check_exit_monitor(&self, now: DateTime<Utc>) -> AuthorizationPreflightCheck {
        AuthorizationPreflightCheck::hard(
            "exit_monitor_healthy",
            self.deps.exit_monitor_health.is_ready(now),
            "exit monitor worker must be healthy for policy-automatic authorization",
        )
    }

    async fn active_status(&self, id: ModelVersionId) -> QuantResult<Result<(), String>> {
        let Some(version) = self.deps.model_registry.find_model_version(&id).await? else {
            return Ok(Err(format!("active model {id} not found")));
        };
        Ok(active_load_ok(&version))
    }

    async fn shadow_status(&self, id: ModelVersionId) -> QuantResult<Result<(), String>> {
        let Some(version) = self.deps.model_registry.find_model_version(&id).await? else {
            return Ok(Err(format!("shadow model {id} not found")));
        };
        Ok(shadow_load_ok(&version))
    }
}

fn configured_routes(config: &DecisionPolicySnapshot) -> Result<RepresentedRouteSet, String> {
    RepresentedRouteSet::from_enabled_categories(
        &config.recommendation.selection.enabled_categories,
    )
    .map_err(|error| format!("configured Route-set digest failed: {error}"))
}

fn route_names(routes: &RepresentedRouteSet) -> String {
    routes
        .routes
        .iter()
        .map(|route| route.as_str())
        .collect::<Vec<_>>()
        .join(",")
}
