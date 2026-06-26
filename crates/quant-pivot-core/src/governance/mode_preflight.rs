//! Mode-upgrade preflight engine.
//!
//! A read-only, side-effect-free aggregate run before an *upgrade* transition
//! (`report_only -> semi_auto`, `semi_auto -> auto_execution`). Every check with
//! a real data source is evaluated against live state; the report is fail-closed
//! (the transition proceeds only when every hard check passes). Downgrades skip
//! this engine entirely (tightening is always permitted).

use crate::{
    governance::{
        kill_switch::KillSwitchHandle,
        quality_gate_load::{active_load_ok, active_publication_status_ok, shadow_load_ok},
    },
    runtime_config::RuntimeConfigStore,
};
use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use quant_pivot_error::QuantResult;
use quant_pivot_models::{
    config::DeployConfig,
    domain::{DataQualityPort, PreflightCheck, PreflightReport},
    enums::{execution::KillSwitchState, quant::QuantRuntimeMode},
    runtime_config::{ModelVersionRef, RuntimeConfig, validate_runtime_config},
    types::ModelVersionId,
};
use quant_pivot_repository::traits::{
    CapitalAllocationRepository, ModelRegistryRepository, ReconciliationRepository,
    ShadowComparisonRepository,
};
use rust_decimal::Decimal;
use std::sync::Arc;

/// Maximum tolerated share of stale tokens (basis points) for "data quality green".
const MAX_STALE_RATIO_BPS: u64 = 2_000;

/// Mode-upgrade preflight boundary.
#[async_trait]
pub trait ModePreflight: Send + Sync {
    /// Aggregate the read-only check list for a target upgrade mode.
    ///
    /// Genuine infrastructure failures (DB down) propagate as [`QuantError`]
    /// (mapped to 5xx). Business denials are captured as failed hard checks in
    /// the returned [`PreflightReport`] (`passed == false`).
    async fn run(&self, target: QuantRuntimeMode) -> QuantResult<PreflightReport>;
}

/// Read-only dependency bundle for [`DefaultModePreflight`].
pub struct ModePreflightDeps {
    pub deploy: Arc<DeployConfig>,
    pub config_store: Arc<RuntimeConfigStore>,
    pub data_quality: Arc<dyn DataQualityPort>,
    pub model_registry: Arc<dyn ModelRegistryRepository>,
    pub shadow_comparison: Arc<dyn ShadowComparisonRepository>,
    pub reconciliation: Arc<dyn ReconciliationRepository>,
    pub capital: Arc<dyn CapitalAllocationRepository>,
    pub kill_switch: KillSwitchHandle,
}

/// Spec preflight engine.
pub struct DefaultModePreflight {
    deps: ModePreflightDeps,
}

impl DefaultModePreflight {
    #[must_use]
    pub const fn new(deps: ModePreflightDeps) -> Self {
        Self { deps }
    }
}

#[async_trait]
impl ModePreflight for DefaultModePreflight {
    async fn run(&self, target: QuantRuntimeMode) -> QuantResult<PreflightReport> {
        let config = self.deps.config_store.current();
        let now = Utc::now();
        let mut checks = vec![
            self.check_credentials(),
            self.check_jwt(target),
            self.check_order_client_ready(),
            self.check_model_available(&config, now).await?,
            self.check_data_quality(),
            self.check_no_unresolvable().await?,
            self.check_no_impaired_capital().await?,
            Self::check_runtime_config_valid(&config),
        ];

        if target == QuantRuntimeMode::AutoExecution {
            checks.push(self.check_published_model(&config).await?);
            checks.push(self.check_shadow_period(&config, now).await?);
            checks.push(Self::check_admission_policy(&config));
            checks.push(self.check_kill_switch_closed());
            checks.push(Self::check_capital_budget(&config));
            checks.push(Self::check_exit_monitor());
        }

        Ok(PreflightReport::new(target, checks))
    }
}

impl DefaultModePreflight {
    fn check_credentials(&self) -> PreflightCheck {
        let key = self.deps.deploy.keys.private_key_present();
        let funder = self
            .deps
            .deploy
            .quant
            .account
            .funder
            .as_deref()
            .is_some_and(|funder| !funder.trim().is_empty());
        let detail = match (key, funder) {
            (true, true) => "private_key and quant.account.funder present".to_owned(),
            (false, true) => "missing keys.private_key".to_owned(),
            (true, false) => "missing quant.account.funder".to_owned(),
            (false, false) => "missing keys.private_key and quant.account.funder".to_owned(),
        };
        PreflightCheck::hard("credentials_loaded", key && funder, detail)
    }

    fn check_jwt(&self, target: QuantRuntimeMode) -> PreflightCheck {
        let weak = self.deps.deploy.web.jwt_secret_is_weak();
        let detail = if weak {
            "web.jwt.secret is empty or the insecure placeholder".to_owned()
        } else {
            "web.jwt.secret is strong".to_owned()
        };
        // Strong JWT is mandatory before unattended execution; for semi_auto it is
        // an informational warning (mirrors `validate_for_quant_mode`).
        if target == QuantRuntimeMode::AutoExecution {
            PreflightCheck::hard("jwt_secret_strong", !weak, detail)
        } else {
            PreflightCheck::soft("jwt_secret_strong", !weak, detail)
        }
    }

    fn check_order_client_ready(&self) -> PreflightCheck {
        let keys_ok = self.deps.deploy.keys.private_key_present();
        let url_ok = !self.deps.deploy.polymarket.clob_base_url.trim().is_empty();
        PreflightCheck::hard(
            "order_client_ready",
            keys_ok && url_ok,
            "boot-level keystore + CLOB endpoint present (live connectivity probe \
             lands with the ExecutionBundle in 05.4)",
        )
    }

    fn check_data_quality(&self) -> PreflightCheck {
        let snapshot = self.deps.data_quality.snapshot();
        let stale_bps = if snapshot.total_tokens == 0 {
            u64::MAX
        } else {
            snapshot
                .stale
                .saturating_mul(10_000)
                .saturating_div(snapshot.total_tokens)
        };
        let green = snapshot.total_tokens > 0
            && !snapshot.fact_lag_exceeded
            && snapshot.insufficient == 0
            && stale_bps <= MAX_STALE_RATIO_BPS;
        let detail = format!(
            "tokens={}, insufficient={}, stale={}, fact_lag_exceeded={}",
            snapshot.total_tokens,
            snapshot.insufficient,
            snapshot.stale,
            snapshot.fact_lag_exceeded
        );
        PreflightCheck::hard("data_quality_green", green, detail)
    }

    async fn check_no_unresolvable(&self) -> QuantResult<PreflightCheck> {
        let blocked = self.deps.reconciliation.has_unresolvable().await?;
        Ok(PreflightCheck::hard(
            "no_unresolvable_reconciliation",
            !blocked,
            if blocked {
                "unresolved `unresolvable` reconciliations require operator action"
            } else {
                "no unresolvable reconciliations"
            },
        ))
    }

    async fn check_no_impaired_capital(&self) -> QuantResult<PreflightCheck> {
        let impaired = self.deps.capital.has_impaired().await?;
        Ok(PreflightCheck::hard(
            "no_impaired_capital",
            !impaired,
            if impaired {
                "impaired capital allocations must be resolved before trading"
            } else {
                "no impaired capital allocations"
            },
        ))
    }

    fn check_runtime_config_valid(config: &RuntimeConfig) -> PreflightCheck {
        let report = validate_runtime_config(config);
        let passed = !report.has_errors();
        let detail = if passed {
            "portfolio / execution / risk envelope config valid".to_owned()
        } else {
            report.to_string()
        };
        PreflightCheck::hard("runtime_config_valid", passed, detail)
    }

    async fn check_model_available(
        &self,
        config: &RuntimeConfig,
        now: DateTime<Utc>,
    ) -> QuantResult<PreflightCheck> {
        let min_age = config.model.min_quality_gate_age_secs;
        let active = match &config.model.active_model_version_id {
            Some(reference) => self.active_status(reference, min_age, now).await?,
            None => Err("no active model configured".to_owned()),
        };
        if active.is_ok() {
            return Ok(PreflightCheck::hard(
                "model_available",
                true,
                "active published model is loadable",
            ));
        }
        let shadow = match &config.model.shadow_model_version_id {
            Some(reference) => self.shadow_status(reference, min_age, now).await?,
            None => Err("no shadow model configured".to_owned()),
        };
        match shadow {
            Ok(()) => Ok(PreflightCheck::hard(
                "model_available",
                true,
                "approved candidate/shadow model is loadable",
            )),
            Err(shadow_detail) => Ok(PreflightCheck::hard(
                "model_available",
                false,
                format!(
                    "active: {}; shadow: {shadow_detail}",
                    active.err().unwrap_or_default()
                ),
            )),
        }
    }

    async fn check_published_model(&self, config: &RuntimeConfig) -> QuantResult<PreflightCheck> {
        let Some(reference) = &config.model.active_model_version_id else {
            return Ok(PreflightCheck::hard(
                "published_model",
                false,
                "auto_execution requires an active published model",
            ));
        };
        let id = match ModelVersionId::try_from(reference) {
            Ok(id) => id,
            Err(error) => {
                return Ok(PreflightCheck::hard(
                    "published_model",
                    false,
                    format!("invalid active model ref: {error}"),
                ));
            }
        };
        let Some(version) = self
            .deps
            .model_registry
            .find_model_version_by_id(&id)
            .await?
        else {
            return Ok(PreflightCheck::hard(
                "published_model",
                false,
                format!("active model {id} not found in registry"),
            ));
        };
        Ok(match active_publication_status_ok(&version) {
            Ok(()) => PreflightCheck::hard("published_model", true, "active model is published"),
            Err(detail) => PreflightCheck::hard("published_model", false, detail),
        })
    }

    async fn check_shadow_period(
        &self,
        config: &RuntimeConfig,
        now: DateTime<Utc>,
    ) -> QuantResult<PreflightCheck> {
        let Some(reference) = &config.model.shadow_model_version_id else {
            return Ok(PreflightCheck::hard(
                "shadow_period_complete",
                true,
                "no shadow configured; active model cleared its publish-time shadow gate",
            ));
        };
        let id = match ModelVersionId::try_from(reference) {
            Ok(id) => id,
            Err(error) => {
                return Ok(PreflightCheck::hard(
                    "shadow_period_complete",
                    false,
                    format!("invalid shadow model ref: {error}"),
                ));
            }
        };
        let required = config.quality_gate.required_shadow_window_secs;
        let since = now - Duration::seconds(i64::try_from(required).unwrap_or(i64::MAX));
        let summary = self.deps.shadow_comparison.summary(&id, since).await?;
        let covered = match (summary.window_start, summary.window_end) {
            (Some(start), Some(end)) => {
                (end - start).num_seconds() >= i64::try_from(required).unwrap_or(i64::MAX)
            }
            _ => false,
        };
        let stable = summary.sample_count > 0 && !summary.any_hard_divergence && covered;
        let detail = format!(
            "samples={}, hard_divergence={}, window_covered={covered} (required {required}s)",
            summary.sample_count, summary.any_hard_divergence
        );
        Ok(PreflightCheck::hard(
            "shadow_period_complete",
            stable,
            detail,
        ))
    }

    fn check_admission_policy(config: &RuntimeConfig) -> PreflightCheck {
        let admission = &config.execution.admission;
        let ok = admission.min_score.value.parse::<Decimal>().is_ok()
            && admission.min_confidence.value.parse::<Decimal>().is_ok();
        PreflightCheck::hard(
            "admission_policy_valid",
            ok,
            "execution.admission min_score / min_confidence thresholds parseable",
        )
    }

    fn check_kill_switch_closed(&self) -> PreflightCheck {
        let state = self.deps.kill_switch.current();
        PreflightCheck::hard(
            "kill_switch_closed",
            state == KillSwitchState::Closed,
            format!("kill switch state = {}", state.as_str()),
        )
    }

    fn check_capital_budget(config: &RuntimeConfig) -> PreflightCheck {
        let budget = config
            .portfolio
            .budget
            .total_budget_usd
            .value
            .parse::<Decimal>();
        let ok = matches!(budget, Ok(value) if value > Decimal::ZERO);
        PreflightCheck::hard(
            "max_capital_budget_set",
            ok,
            "portfolio.budget.total_budget_usd must be > 0 for auto_execution",
        )
    }

    fn check_exit_monitor() -> PreflightCheck {
        PreflightCheck::soft(
            "exit_monitor_healthy",
            true,
            "deferred: exit monitor worker health lands with 05.6",
        )
    }

    async fn active_status(
        &self,
        reference: &ModelVersionRef,
        min_age: u64,
        now: DateTime<Utc>,
    ) -> QuantResult<Result<(), String>> {
        let id = match ModelVersionId::try_from(reference) {
            Ok(id) => id,
            Err(error) => return Ok(Err(format!("invalid active model ref: {error}"))),
        };
        let Some(version) = self
            .deps
            .model_registry
            .find_model_version_by_id(&id)
            .await?
        else {
            return Ok(Err(format!("active model {id} not found")));
        };
        Ok(active_load_ok(&version, min_age, now))
    }

    async fn shadow_status(
        &self,
        reference: &ModelVersionRef,
        min_age: u64,
        now: DateTime<Utc>,
    ) -> QuantResult<Result<(), String>> {
        let id = match ModelVersionId::try_from(reference) {
            Ok(id) => id,
            Err(error) => return Ok(Err(format!("invalid shadow model ref: {error}"))),
        };
        let Some(version) = self
            .deps
            .model_registry
            .find_model_version_by_id(&id)
            .await?
        else {
            return Ok(Err(format!("shadow model {id} not found")));
        };
        Ok(shadow_load_ok(&version, min_age, now))
    }
}
