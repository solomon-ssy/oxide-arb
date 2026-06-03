use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use chrono::Utc;
use oxide_arb_models::{
    domain::{
        MarketInfo, MarketPitSnapshotInfo,
        control_factor::{
            InputFallbackPolicy, InputResolutionReport, MarketReplayContext,
            MaterializationRunManifest, MissingPointInTimeInput, PointInTimeInputManifest,
            PointInTimeInputSource, QueryFingerprint, RequiredInputDomain, RuntimeConfigRef,
            StageCoverageReport, StageWarning,
        },
    },
    enums::control_factor::MaterializationErrorCode,
    types::{EventId, MarketId, TokenId},
};
use oxide_arb_repository::traits::{
    BalanceSnapshotRepository, EventRepository, EvidenceTimeseriesRepository, MarketRepository,
    PositionRepository, ReconciliationRepository, ResolutionEventRepository,
    RuntimeConfigVersionRepository, TimeWindow, TradeRepository,
};

use crate::materialization::{ArtifactHasher, MaterializationResult};

#[derive(Clone, Default)]
pub struct ResolverRepositories {
    pub runtime_config: Option<Arc<dyn RuntimeConfigVersionRepository>>,
    pub timeseries: Option<Arc<dyn EvidenceTimeseriesRepository>>,
    pub markets: Option<Arc<dyn MarketRepository>>,
    pub events: Option<Arc<dyn EventRepository>>,
    pub balances: Option<Arc<dyn BalanceSnapshotRepository>>,
    pub trades: Option<Arc<dyn TradeRepository>>,
    pub positions: Option<Arc<dyn PositionRepository>>,
    pub reconciliation: Option<Arc<dyn ReconciliationRepository>>,
    pub resolution_events: Option<Arc<dyn ResolutionEventRepository>>,
}

pub struct PointInTimeResolver {
    repos: ResolverRepositories,
}

impl PointInTimeResolver {
    #[must_use]
    pub const fn new(repos: ResolverRepositories) -> Self {
        Self { repos }
    }

    pub async fn resolve(
        &self,
        manifest: &MaterializationRunManifest,
    ) -> MaterializationResult<InputResolutionReport> {
        let mut state = ResolutionState::new(manifest);
        self.resolve_runtime_config(manifest, &mut state).await?;
        self.resolve_markets(manifest, &mut state).await?;
        self.resolve_events(manifest, &mut state).await?;
        Self::resolve_fee_schedule(manifest, &mut state);
        self.resolve_calibration(manifest, &mut state).await?;
        Self::resolve_l2_coverage_requirement(manifest, &mut state);
        self.resolve_trades(manifest, &mut state).await?;
        self.resolve_positions(manifest, &mut state).await?;
        self.resolve_reconciliation(manifest, &mut state).await?;
        self.resolve_settlement_truth(manifest, &mut state).await?;
        self.resolve_balance_domains(manifest, &mut state).await?;
        Self::resolve_risk_state(manifest, &mut state);
        let mut pit_manifest = PointInTimeInputManifest {
            inputs: state.inputs,
            production_eligible: state.fatal_errors.is_empty(),
            missing_inputs: state.missing_inputs,
            fatal_errors: state.fatal_errors,
            warnings: state.warnings,
            manifest_hash: String::new(),
        };
        pit_manifest.manifest_hash = ArtifactHasher::compute(&pit_manifest)?.0;
        Ok(InputResolutionReport {
            run_id: manifest.run_id.clone(),
            window: manifest.window,
            manifest: pit_manifest,
            market_contexts: state.market_contexts,
        })
    }

    async fn resolve_runtime_config(
        &self,
        manifest: &MaterializationRunManifest,
        state: &mut ResolutionState<'_>,
    ) -> MaterializationResult<()> {
        let Some(repo) = &self.repos.runtime_config else {
            state.missing(
                RequiredInputDomain::RuntimeConfig,
                MaterializationErrorCode::InputPitConfigMissing,
                "runtime config repository is not configured",
            );
            return Ok(());
        };
        let row = match &manifest.runtime_config_ref {
            RuntimeConfigRef::ActiveAt { at } => repo.load_active_at(*at).await?,
            RuntimeConfigRef::Version { version_id, .. } => repo.load_version(version_id).await?,
            RuntimeConfigRef::Hash { config_hash } => repo.load_by_hash(config_hash).await?,
        };
        match row {
            Some(version) => state.source(
                RequiredInputDomain::RuntimeConfig,
                "runtime_config_version",
                "RuntimeConfigVersionRepository",
                1,
                Some(version.config_hash),
            ),
            None => state.missing(
                RequiredInputDomain::RuntimeConfig,
                MaterializationErrorCode::InputPitConfigMissing,
                "runtime config version is not available point-in-time",
            ),
        }
        Ok(())
    }

    async fn resolve_markets(
        &self,
        manifest: &MaterializationRunManifest,
        state: &mut ResolutionState<'_>,
    ) -> MaterializationResult<()> {
        let required = state.is_required(RequiredInputDomain::TokenMapping);
        let Some(repo) = &self.repos.markets else {
            if required {
                state.missing(
                    RequiredInputDomain::TokenMapping,
                    MaterializationErrorCode::InputMarketMappingMissing,
                    "market repository is not configured",
                );
            }
            return Ok(());
        };
        if manifest.markets.market_ids.is_empty() {
            state.source(
                RequiredInputDomain::MarketMetadata,
                "market",
                "MarketRepository",
                0,
                None,
            );
            return Ok(());
        }
        let snapshots = repo
            .latest_pit_snapshots_before(&manifest.markets.market_ids, state.as_of)
            .await?;
        let snapshot_ids = snapshots
            .iter()
            .map(|snapshot| snapshot.market_id.clone())
            .collect::<HashSet<MarketId>>();
        let missing_snapshot_ids = manifest
            .markets
            .market_ids
            .iter()
            .filter(|market_id| !snapshot_ids.contains(*market_id))
            .cloned()
            .collect::<Vec<_>>();
        let current_rows = if missing_snapshot_ids.is_empty() {
            Vec::new()
        } else {
            repo.find_by_ids(&missing_snapshot_ids).await?
        };
        let current_by_market = current_rows
            .iter()
            .map(|market| (market.market_id.clone(), market.as_ref()))
            .collect::<HashMap<_, _>>();
        record_missing_market_snapshots(state, manifest, &snapshot_ids, &current_by_market);
        let usable_current_rows = usable_current_market_rows(current_rows, &snapshot_ids, state);
        let source_hash = ArtifactHasher::compute(&(&snapshots, &usable_current_rows))?.0;
        let row_count = snapshots.len() + usable_current_rows.len();
        state.source(
            RequiredInputDomain::TokenMapping,
            "market_pit_snapshot",
            "MarketRepository",
            u64::try_from(row_count).unwrap_or(u64::MAX),
            Some(source_hash),
        );
        for snapshot in snapshots {
            state
                .market_contexts
                .push(market_context_from_snapshot(snapshot, state.as_of));
        }
        for market in usable_current_rows {
            state
                .market_contexts
                .push(market_context_from_current(&market, state.as_of)?);
        }
        Ok(())
    }

    async fn resolve_events(
        &self,
        manifest: &MaterializationRunManifest,
        state: &mut ResolutionState<'_>,
    ) -> MaterializationResult<()> {
        let Some(repo) = &self.repos.events else {
            return Ok(());
        };
        if manifest.markets.event_ids.is_empty() {
            return Ok(());
        }
        let rows = repo.find_by_ids(&manifest.markets.event_ids).await?;
        state.source(
            RequiredInputDomain::MarketMetadata,
            "event",
            "EventRepository",
            u64::try_from(rows.len()).unwrap_or(u64::MAX),
            None,
        );
        Ok(())
    }

    fn resolve_fee_schedule(
        manifest: &MaterializationRunManifest,
        state: &mut ResolutionState<'_>,
    ) {
        if !state.is_required(RequiredInputDomain::FeeSchedule) {
            return;
        }
        if !state.has_resolved_source(RequiredInputDomain::RuntimeConfig) {
            state.missing(
                RequiredInputDomain::FeeSchedule,
                MaterializationErrorCode::InputFeeScheduleMissing,
                "fee schedule is pinned through runtime config, but the runtime config PIT source is unresolved",
            );
            return;
        }
        state.source(
            RequiredInputDomain::FeeSchedule,
            "runtime_config_fee_source",
            "RuntimeConfigVersionRepository",
            1,
            Self::runtime_config_snapshot_hash(manifest),
        );
    }

    fn runtime_config_snapshot_hash(manifest: &MaterializationRunManifest) -> Option<String> {
        match &manifest.runtime_config_ref {
            RuntimeConfigRef::Version { config_hash, .. }
            | RuntimeConfigRef::Hash { config_hash } => Some(config_hash.clone()),
            RuntimeConfigRef::ActiveAt { .. } => None,
        }
    }

    async fn resolve_calibration(
        &self,
        manifest: &MaterializationRunManifest,
        state: &mut ResolutionState<'_>,
    ) -> MaterializationResult<()> {
        if !state.is_required(RequiredInputDomain::CalibrationSnapshots) {
            return Ok(());
        }
        let Some(repo) = &self.repos.timeseries else {
            state.missing(
                RequiredInputDomain::CalibrationSnapshots,
                MaterializationErrorCode::InputCalibrationSnapshotMissing,
                "evidence timeseries repository is not configured",
            );
            return Ok(());
        };
        let rows = repo
            .calibration_snapshots(TimeWindow::new(manifest.window.from, manifest.window.to))
            .await?;
        if rows.is_empty() {
            state.missing(
                RequiredInputDomain::CalibrationSnapshots,
                MaterializationErrorCode::InputCalibrationSnapshotMissing,
                "no calibration snapshots in materialization window",
            );
        } else {
            state.source(
                RequiredInputDomain::CalibrationSnapshots,
                "calibration_snapshots",
                "EvidenceTimeseriesRepository",
                u64::try_from(rows.len()).unwrap_or(u64::MAX),
                None,
            );
        }
        Ok(())
    }

    async fn resolve_trades(
        &self,
        manifest: &MaterializationRunManifest,
        state: &mut ResolutionState<'_>,
    ) -> MaterializationResult<()> {
        let Some(repo) = &self.repos.trades else {
            return Ok(());
        };
        let rows = repo
            .find_between(manifest.window.from, manifest.window.to)
            .await?;
        state.source(
            RequiredInputDomain::Trades,
            "trade",
            "TradeRepository",
            u64::try_from(rows.len()).unwrap_or(u64::MAX),
            None,
        );
        Ok(())
    }

    fn resolve_l2_coverage_requirement(
        manifest: &MaterializationRunManifest,
        state: &mut ResolutionState<'_>,
    ) {
        if manifest.data_requirements.requires_l2_coverage() {
            state.warnings.push(StageWarning {
                code: MaterializationErrorCode::ChCoverageL2Insufficient
                    .as_str()
                    .to_owned(),
                message:
                    "L2 coverage is declared in DataRequirements and is evaluated by Phase 5.3 book reconstruction"
                        .to_owned(),
            });
        }
    }

    async fn resolve_positions(
        &self,
        manifest: &MaterializationRunManifest,
        state: &mut ResolutionState<'_>,
    ) -> MaterializationResult<()> {
        if !state.is_required(RequiredInputDomain::Positions) {
            return Ok(());
        }
        let Some(repo) = &self.repos.positions else {
            return Ok(());
        };
        let rows = repo.open_as_of(manifest.window.to).await?;
        state.source(
            RequiredInputDomain::Positions,
            "position",
            "PositionRepository",
            u64::try_from(rows.len()).unwrap_or(u64::MAX),
            None,
        );
        Ok(())
    }

    async fn resolve_reconciliation(
        &self,
        manifest: &MaterializationRunManifest,
        state: &mut ResolutionState<'_>,
    ) -> MaterializationResult<()> {
        if !state.is_required(RequiredInputDomain::ReconciliationStatus) {
            return Ok(());
        }
        let Some(repo) = &self.repos.reconciliation else {
            state.missing(
                RequiredInputDomain::ReconciliationStatus,
                MaterializationErrorCode::InputReconciliationStatusMissing,
                "reconciliation repository is not configured",
            );
            return Ok(());
        };
        let rows = repo
            .find_between(manifest.window.from, manifest.window.to)
            .await?;
        state.source(
            RequiredInputDomain::ReconciliationStatus,
            "reconciliation_report",
            "ReconciliationRepository",
            u64::try_from(rows.len()).unwrap_or(u64::MAX),
            None,
        );
        Ok(())
    }

    async fn resolve_settlement_truth(
        &self,
        manifest: &MaterializationRunManifest,
        state: &mut ResolutionState<'_>,
    ) -> MaterializationResult<()> {
        if !manifest.data_requirements.requires_settlement_truth() {
            return Ok(());
        }
        let Some(repo) = &self.repos.resolution_events else {
            state.missing(
                RequiredInputDomain::SettlementTruth,
                MaterializationErrorCode::InputSettlementTruthMissing,
                "resolution event repository is not configured",
            );
            return Ok(());
        };
        let mut row_count = 0u64;
        for market_id in &manifest.markets.market_ids {
            if repo
                .latest_before(market_id, manifest.window.to)
                .await?
                .is_some()
            {
                row_count += 1;
            }
        }
        if row_count == 0 {
            state.missing(
                RequiredInputDomain::SettlementTruth,
                MaterializationErrorCode::InputSettlementTruthMissing,
                "no settlement truth resolved before materialization window end",
            );
        } else {
            state.source(
                RequiredInputDomain::SettlementTruth,
                "resolution_event",
                "ResolutionEventRepository",
                row_count,
                None,
            );
        }
        Ok(())
    }

    async fn resolve_balance_domains(
        &self,
        manifest: &MaterializationRunManifest,
        state: &mut ResolutionState<'_>,
    ) -> MaterializationResult<()> {
        let balance_required = state.is_required(RequiredInputDomain::BalanceSnapshot);
        let token_balance_required = manifest.data_requirements.requires_token_balances();
        if !balance_required && !token_balance_required {
            return Ok(());
        }
        let Some(scope) = manifest.replay_account_scope.as_ref() else {
            Self::missing_balance_scope(state, balance_required, token_balance_required);
            return Ok(());
        };
        let Some(repo) = &self.repos.balances else {
            if balance_required {
                state.missing(
                    RequiredInputDomain::BalanceSnapshot,
                    MaterializationErrorCode::InputBalanceSnapshotMissing,
                    "balance snapshot repository is not configured",
                );
            }
            if token_balance_required {
                state.missing(
                    RequiredInputDomain::TokenBalanceSnapshot,
                    MaterializationErrorCode::InputTokenBalanceSnapshotMissing,
                    "token balance snapshot repository is not configured",
                );
            }
            return Ok(());
        };
        if balance_required {
            match repo
                .latest_balance_before(&scope.holder_address, manifest.window.to)
                .await?
            {
                Some(snapshot) => state.source(
                    RequiredInputDomain::BalanceSnapshot,
                    "balance_snapshot",
                    "BalanceSnapshotRepository",
                    1,
                    Some(ArtifactHasher::compute(&snapshot)?.0),
                ),
                None => state.missing(
                    RequiredInputDomain::BalanceSnapshot,
                    MaterializationErrorCode::InputBalanceSnapshotMissing,
                    "no balance snapshot resolved before materialization window end",
                ),
            }
        }
        if token_balance_required {
            let (market_ids, token_ids) = token_balance_scope(&state.market_contexts);
            if market_ids.is_empty() || token_ids.is_empty() {
                state.missing(
                    RequiredInputDomain::TokenBalanceSnapshot,
                    MaterializationErrorCode::InputTokenBalanceSnapshotMissing,
                    "token balance PIT lookup has no resolved market token scope",
                );
                return Ok(());
            }
            let rows = repo
                .latest_token_balances_before(
                    &scope.holder_address,
                    &market_ids,
                    &token_ids,
                    manifest.window.to,
                )
                .await?;
            if rows.len() < token_ids.len() {
                state.missing(
                    RequiredInputDomain::TokenBalanceSnapshot,
                    MaterializationErrorCode::InputTokenBalanceSnapshotMissing,
                    format!(
                        "resolved {} token balance snapshots for {} required tokens",
                        rows.len(),
                        token_ids.len()
                    ),
                );
            }
            if !rows.is_empty() {
                state.source(
                    RequiredInputDomain::TokenBalanceSnapshot,
                    "token_balance_snapshot",
                    "BalanceSnapshotRepository",
                    u64::try_from(rows.len()).unwrap_or(u64::MAX),
                    Some(ArtifactHasher::compute(&rows)?.0),
                );
            }
        }
        Ok(())
    }

    fn missing_balance_scope(
        state: &mut ResolutionState<'_>,
        balance_required: bool,
        token_balance_required: bool,
    ) {
        if balance_required {
            state.warnings.push(StageWarning {
                code: "input.balance_account_scope_absent".to_owned(),
                message:
                    "balance snapshot PIT lookup requires account scope, which is not present in the Phase 5.2 manifest"
                        .to_owned(),
            });
            state.missing(
                RequiredInputDomain::BalanceSnapshot,
                MaterializationErrorCode::InputBalanceSnapshotMissing,
                "balance snapshot account scope is absent from the materialization manifest",
            );
        }
        if token_balance_required {
            state.warnings.push(StageWarning {
                code: "input.token_balance_account_scope_absent".to_owned(),
                message:
                    "token balance PIT lookup requires account scope, which is not present in the Phase 5.2 manifest"
                        .to_owned(),
            });
            state.missing(
                RequiredInputDomain::TokenBalanceSnapshot,
                MaterializationErrorCode::InputTokenBalanceSnapshotMissing,
                "token balance account scope is absent from the materialization manifest",
            );
        }
    }

    fn resolve_risk_state(_manifest: &MaterializationRunManifest, state: &mut ResolutionState<'_>) {
        if state.is_required(RequiredInputDomain::RiskState) {
            if state.is_production_required(RequiredInputDomain::RiskState) {
                state.missing(
                    RequiredInputDomain::RiskState,
                    MaterializationErrorCode::InputCurrentStateFallbackForbidden,
                    "risk state repository is current-state only; production-required PIT fallback is forbidden",
                );
                return;
            }
            state.warnings.push(StageWarning {
                code: "input.risk_state_pit_source_missing".to_owned(),
                message:
                    "risk state repository is current-state only; resolver refuses PIT fallback"
                        .to_owned(),
            });
        }
    }
}

fn token_balance_scope(contexts: &[MarketReplayContext]) -> (Vec<MarketId>, Vec<TokenId>) {
    let mut market_seen = HashSet::new();
    let mut token_seen = HashSet::new();
    let mut market_ids = Vec::new();
    let mut token_ids = Vec::new();
    for context in contexts {
        let market_id = context.market_id.clone();
        if market_seen.insert(market_id.clone()) {
            market_ids.push(market_id);
        }
        let yes_token_id = context.yes_token_id.clone();
        if token_seen.insert(yes_token_id.clone()) {
            token_ids.push(yes_token_id);
        }
        let no_token_id = context.no_token_id.clone();
        if token_seen.insert(no_token_id.clone()) {
            token_ids.push(no_token_id);
        }
    }
    (market_ids, token_ids)
}

fn record_missing_market_snapshots(
    state: &mut ResolutionState<'_>,
    manifest: &MaterializationRunManifest,
    snapshot_ids: &HashSet<MarketId>,
    current_by_market: &HashMap<MarketId, &MarketInfo>,
) {
    for market_id in &manifest.markets.market_ids {
        if snapshot_ids.contains(market_id) {
            continue;
        }
        let Some(current) = current_by_market.get(market_id) else {
            state.missing(
                RequiredInputDomain::TokenMapping,
                MaterializationErrorCode::InputMarketMappingMissing,
                format!("market {market_id} is missing"),
            );
            continue;
        };
        record_current_market_staleness(state, market_id, current);
    }
}

fn record_current_market_staleness(
    state: &mut ResolutionState<'_>,
    market_id: &MarketId,
    current: &MarketInfo,
) {
    if current.updated_at > state.as_of {
        state.missing(
            RequiredInputDomain::TokenMapping,
            MaterializationErrorCode::InputMarketMappingMissing,
            format!(
                "market {market_id} current row was updated after PIT as_of {}",
                state.as_of
            ),
        );
        return;
    }
    if state.is_required(RequiredInputDomain::FeeSchedule)
        && current
            .fee_observed_at
            .is_some_and(|observed_at| observed_at > state.as_of)
    {
        state.missing(
            RequiredInputDomain::FeeSchedule,
            MaterializationErrorCode::InputFeeScheduleMissing,
            format!(
                "market {market_id} fee row was observed after PIT as_of {}",
                state.as_of
            ),
        );
    }
}

fn usable_current_market_rows(
    current_rows: Vec<Arc<MarketInfo>>,
    snapshot_ids: &HashSet<MarketId>,
    state: &ResolutionState<'_>,
) -> Vec<Arc<MarketInfo>> {
    current_rows
        .into_iter()
        .filter(|market| {
            !snapshot_ids.contains(&market.market_id) && market.updated_at <= state.as_of
        })
        .collect()
}

fn market_context_from_snapshot(
    snapshot: MarketPitSnapshotInfo,
    as_of: chrono::DateTime<Utc>,
) -> MarketReplayContext {
    MarketReplayContext {
        market_id: snapshot.market_id,
        event_id: Some(snapshot.event_id),
        yes_token_id: snapshot.yes_token_id,
        no_token_id: snapshot.no_token_id,
        category: Some(snapshot.category),
        resolved_as_of: as_of,
        source_hash: snapshot.payload_hash,
    }
}

fn market_context_from_current(
    market: &MarketInfo,
    as_of: chrono::DateTime<Utc>,
) -> MaterializationResult<MarketReplayContext> {
    let source_hash = ArtifactHasher::compute(market)?.0;
    Ok(MarketReplayContext {
        market_id: market.market_id.clone(),
        event_id: Some(market.event_id.clone()),
        yes_token_id: market.yes_token_id.clone(),
        no_token_id: market.no_token_id.clone(),
        category: Some(market.category),
        resolved_as_of: as_of,
        source_hash,
    })
}

struct ResolutionState<'a> {
    as_of: chrono::DateTime<Utc>,
    manifest: &'a MaterializationRunManifest,
    inputs: Vec<PointInTimeInputSource>,
    missing_inputs: Vec<MissingPointInTimeInput>,
    fatal_errors: Vec<MaterializationErrorCode>,
    warnings: Vec<StageWarning>,
    market_contexts: Vec<MarketReplayContext>,
}

impl<'a> ResolutionState<'a> {
    const fn new(manifest: &'a MaterializationRunManifest) -> Self {
        Self {
            as_of: manifest.window.to,
            manifest,
            inputs: Vec::new(),
            missing_inputs: Vec::new(),
            fatal_errors: Vec::new(),
            warnings: Vec::new(),
            market_contexts: Vec::new(),
        }
    }

    fn is_required(&self, domain: RequiredInputDomain) -> bool {
        self.manifest.data_requirements.requires(domain)
    }

    fn is_production_required(&self, domain: RequiredInputDomain) -> bool {
        self.manifest.data_requirements.production_requires(domain)
    }

    fn has_resolved_source(&self, domain: RequiredInputDomain) -> bool {
        self.inputs
            .iter()
            .any(|input| input.domain == domain && input.resolved)
    }

    fn source(
        &mut self,
        domain: RequiredInputDomain,
        source_table: &str,
        source_repository: &str,
        row_count: u64,
        snapshot_hash: Option<String>,
    ) {
        self.inputs.push(PointInTimeInputSource {
            domain,
            source_table: source_table.to_owned(),
            source_repository: source_repository.to_owned(),
            query_window: Some(self.manifest.window),
            as_of: self.as_of,
            query_fingerprint: self.query_fingerprint(domain, source_repository, source_table),
            row_count,
            coverage: StageCoverageReport::complete(row_count),
            snapshot_hash,
            fallback_policy: InputFallbackPolicy::None,
            production_required: self.is_production_required(domain),
            resolved: true,
        });
    }

    fn missing(
        &mut self,
        domain: RequiredInputDomain,
        code: MaterializationErrorCode,
        detail: impl Into<String>,
    ) {
        let production_required = self.is_production_required(domain);
        if production_required || code.is_fatal_for_production() {
            self.fatal_errors.push(code);
        }
        self.missing_inputs.push(MissingPointInTimeInput {
            domain,
            code,
            detail: detail.into(),
            production_required,
        });
    }

    fn query_fingerprint(
        &self,
        domain: RequiredInputDomain,
        source_repository: &str,
        source_table: &str,
    ) -> QueryFingerprint {
        let mut market_ids = self
            .manifest
            .markets
            .market_ids
            .iter()
            .map(MarketId::as_str)
            .collect::<Vec<_>>();
        let mut event_ids = self
            .manifest
            .markets
            .event_ids
            .iter()
            .map(EventId::as_str)
            .collect::<Vec<_>>();
        let mut token_ids = self
            .manifest
            .markets
            .token_ids
            .iter()
            .map(TokenId::as_str)
            .collect::<Vec<_>>();
        market_ids.sort_unstable();
        event_ids.sort_unstable();
        token_ids.sort_unstable();
        let payload = serde_json::json!({
            "version": 2,
            "domain": domain,
            "source_repository": source_repository,
            "source_table": source_table,
            "window_from": self.manifest.window.from,
            "window_to": self.manifest.window.to,
            "as_of": self.as_of,
            "market_ids": market_ids,
            "event_ids": event_ids,
            "token_ids": token_ids,
            "categories": self.manifest.markets.categories,
        });
        let digest = serde_json::to_vec(&payload).map_or_else(
            |_| format!("{source_repository}.{source_table}:v2:codec_error"),
            |bytes| {
                format!(
                    "{source_repository}.{source_table}:blake3:{}",
                    hex::encode(blake3::hash(&bytes).as_bytes())
                )
            },
        );
        QueryFingerprint(digest)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::materialization::{PointInTimeResolver, ResolverRepositories};
    use oxide_arb_models::{
        domain::control_factor::RequiredInputDomain,
        types::{MarketId, TokenId},
    };
    use oxide_arb_test_support::materialization::{
        FakeMarketRepository, fixed_time, market_info, market_snapshot, materialization_manifest,
        runtime_config_repo,
    };

    #[tokio::test]
    async fn market_snapshot_resolves_point_in_time_mapping() {
        let as_of = fixed_time(10);
        let market_id = MarketId::new("0xpit");
        let repo = FakeMarketRepository {
            snapshots: vec![
                market_snapshot(&market_id, "yes_old", "no_old", fixed_time(5)),
                market_snapshot(&market_id, "yes_new", "no_new", fixed_time(15)),
            ],
            current: Vec::new(),
        };
        let report = resolver(repo)
            .resolve(&materialization_manifest(as_of, vec![market_id]))
            .await
            .expect("resolve PIT inputs");
        assert_eq!(
            report.market_contexts[0].yes_token_id,
            TokenId::new("yes_old")
        );
        assert!(report.manifest.is_production_eligible());
    }

    #[tokio::test]
    async fn current_market_row_after_as_of_fails_closed_without_snapshot() {
        let as_of = fixed_time(10);
        let market_id = MarketId::new("0xstale");
        let repo = FakeMarketRepository {
            snapshots: Vec::new(),
            current: vec![market_info(
                &market_id,
                "yes_current",
                "no_current",
                fixed_time(20),
            )],
        };
        let report = resolver(repo)
            .resolve(&materialization_manifest(as_of, vec![market_id]))
            .await
            .expect("resolve report-only inputs");
        assert!(!report.manifest.is_production_eligible());
        assert!(
            report
                .manifest
                .missing_inputs
                .iter()
                .any(|missing| missing.domain == RequiredInputDomain::TokenMapping)
        );
    }

    #[tokio::test]
    async fn query_fingerprint_changes_when_window_changes() {
        let market_id = MarketId::new("0xfingerprint");
        let repo = FakeMarketRepository {
            snapshots: vec![market_snapshot(&market_id, "yes", "no", fixed_time(5))],
            current: Vec::new(),
        };
        let resolver = resolver(repo);
        let first = resolver
            .resolve(&materialization_manifest(
                fixed_time(10),
                vec![market_id.clone()],
            ))
            .await
            .expect("first resolve");
        let second = resolver
            .resolve(&materialization_manifest(fixed_time(11), vec![market_id]))
            .await
            .expect("second resolve");
        assert_ne!(
            first.manifest.inputs[0].query_fingerprint,
            second.manifest.inputs[0].query_fingerprint
        );
    }

    fn resolver(repo: FakeMarketRepository) -> PointInTimeResolver {
        PointInTimeResolver::new(ResolverRepositories {
            runtime_config: Some(runtime_config_repo()),
            markets: Some(Arc::new(repo)),
            ..ResolverRepositories::default()
        })
    }
}
