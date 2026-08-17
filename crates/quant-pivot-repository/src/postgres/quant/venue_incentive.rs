//! PostgreSQL-backed append-only venue incentive ledger.

use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, NaiveDate, Utc};
use quant_pivot_error::storage::{
    StorageError,
    entity::{QUANT_VENUE_INCENTIVE_EVENT, QUANT_VENUE_INCENTIVE_RECONCILIATION_SCAN},
};
use quant_pivot_models::{
    domain::{
        api::quant_incentive::VenueIncentiveEventListQuery,
        pagination::{PageWindow, Paginated},
        quant::venue_incentive::{
            NewVenueIncentiveEvent, NewVenueIncentiveReconciliationScan,
            NewVenueIncentiveReportedAccrualSnapshot, VenueIncentiveEventInfo,
            VenueIncentiveReconciliation, VenueIncentiveReconciliationScanInfo,
        },
    },
    entities::{
        quant_venue_incentive_event::{
            Column as EventColumn, Entity as EventEntity, Model as EventModel,
        },
        quant_venue_incentive_reconciliation_scan::{
            Column as ScanColumn, Entity as ScanEntity, Model as ScanModel,
        },
    },
    enums::fee::{VenueIncentiveKind, VenueIncentiveReconciliationScanStatus, VenueIncentiveStage},
    types::{ExecutionAccountId, Usd, VenueIncentiveEventId},
};
use rust_decimal::Decimal;
use sea_orm::{
    ColumnTrait, Condition, ConnectionTrait, DatabaseBackend, DatabaseConnection, EntityTrait,
    ExprTrait, FromQueryResult, IntoActiveModel, QueryFilter, QueryOrder, Statement,
    TransactionTrait, TryInsertResult,
    sea_query::{Alias, Expr, OnConflict, Order, Query},
};

use crate::{postgres::query::paginate_mapped, traits::VenueIncentiveRepository};

/// PostgreSQL-backed venue incentive repository.
pub struct PgVenueIncentiveRepository {
    db: DatabaseConnection,
}

#[derive(Debug, FromQueryResult)]
struct CreditSum {
    total: Option<Decimal>,
}

fn event_condition(
    execution_account_id: &ExecutionAccountId,
    query: &VenueIncentiveEventListQuery,
) -> Condition {
    let mut condition =
        Condition::all().add(EventColumn::ExecutionAccountId.eq(*execution_account_id));
    if let Some(kind) = query.kind {
        condition = condition.add(EventColumn::Kind.eq(kind));
    }
    if let Some(stage) = query.stage {
        condition = condition.add(EventColumn::Stage.eq(stage));
    }
    if let Some(program_date) = query.program_date {
        condition = condition.add(EventColumn::ProgramDate.eq(program_date));
    }
    condition
}

impl PgVenueIncentiveRepository {
    #[must_use]
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }

    fn event_matches(existing: &EventModel, event: &NewVenueIncentiveEvent) -> bool {
        let timing_matches = match event.stage {
            VenueIncentiveStage::EstimatedAccrual => {
                existing.observed_at == event.observed_at
                    && existing.available_at == event.available_at
            }
            VenueIncentiveStage::VenueReportedAccrual => true,
            VenueIncentiveStage::WalletCredited => existing.observed_at == event.observed_at,
        };
        timing_matches
            && existing.execution_account_id == event.execution_account_id
            && existing.execution_fill_id == event.execution_fill_id
            && existing.market_id == event.market_id
            && existing.kind == event.kind
            && existing.stage == event.stage
            && existing.program_date == event.program_date
            && existing.amount_usd == event.amount_usd
            && existing.source_terms_hash == event.source_terms_hash
            && existing.source_partition == event.source_partition
            && existing.source_identity == event.source_identity
            && existing.transaction_hash == event.transaction_hash
            && existing.evidence_hash == event.evidence_hash
    }

    pub(crate) async fn persist_on(
        db: &impl ConnectionTrait,
        event: NewVenueIncentiveEvent,
    ) -> Result<(), StorageError> {
        let source_identity = event.source_identity.clone();
        let outcome = EventEntity::insert(event.clone().into_active_model())
            .on_conflict(OnConflict::new().do_nothing().to_owned())
            .try_insert()
            .exec_without_returning(db)
            .await
            .map_err(StorageError::from)?;
        match outcome {
            TryInsertResult::Inserted(1 | 0) | TryInsertResult::Conflicted => {}
            TryInsertResult::Inserted(rows) => {
                return Err(StorageError::invariant_violation(
                    Some(QUANT_VENUE_INCENTIVE_EVENT),
                    format!("single incentive event insert affected {rows} rows"),
                ));
            }
            TryInsertResult::Empty => {
                return Err(StorageError::invariant_violation(
                    Some(QUANT_VENUE_INCENTIVE_EVENT),
                    "single incentive event insert unexpectedly had no input",
                ));
            }
        }
        let existing = EventEntity::find()
            .filter(EventColumn::SourceIdentity.eq(source_identity.clone()))
            .one(db)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| {
                StorageError::state_conflict(
                    QUANT_VENUE_INCENTIVE_EVENT,
                    Some(&source_identity),
                    "incentive event disappeared after idempotent insert",
                )
            })?;
        if !Self::event_matches(&existing, &event) {
            return Err(StorageError::state_conflict(
                QUANT_VENUE_INCENTIVE_EVENT,
                Some(&source_identity),
                "venue incentive identity was replayed with different economics or lineage",
            ));
        }
        Ok(())
    }

    fn validate_scan(
        scan: &NewVenueIncentiveReconciliationScan,
        events: &[NewVenueIncentiveEvent],
    ) -> Result<(), StorageError> {
        if scan.completed_at < scan.started_at {
            return Err(StorageError::invariant_violation(
                Some(QUANT_VENUE_INCENTIVE_RECONCILIATION_SCAN),
                "completed_at must not precede started_at",
            ));
        }
        let event_count = i32::try_from(events.len()).map_err(|_| {
            StorageError::invariant_violation(
                Some(QUANT_VENUE_INCENTIVE_RECONCILIATION_SCAN),
                "response event count exceeds i32",
            )
        })?;
        match scan.status {
            VenueIncentiveReconciliationScanStatus::Succeeded => {
                if scan.response_digest.is_none()
                    || scan.error_code.is_some()
                    || scan.response_count != event_count
                {
                    return Err(StorageError::invariant_violation(
                        Some(QUANT_VENUE_INCENTIVE_RECONCILIATION_SCAN),
                        "successful scan requires digest, exact response count, and no error",
                    ));
                }
            }
            VenueIncentiveReconciliationScanStatus::Failed => {
                if scan.response_digest.is_some()
                    || scan.error_code.as_deref().is_none_or(str::is_empty)
                    || scan.response_count != 0
                    || !events.is_empty()
                {
                    return Err(StorageError::invariant_violation(
                        Some(QUANT_VENUE_INCENTIVE_RECONCILIATION_SCAN),
                        "failed scan requires an error and cannot carry response evidence",
                    ));
                }
            }
        }
        for event in events {
            if event.execution_account_id != scan.execution_account_id
                || event.kind != scan.kind
                || event.stage != scan.stage
                || event.program_date != scan.program_date
            {
                return Err(StorageError::invariant_violation(
                    Some(QUANT_VENUE_INCENTIVE_RECONCILIATION_SCAN),
                    "scan event lies outside its account/kind/stage/program-date partition",
                ));
            }
        }
        let mut source_identities = BTreeSet::new();
        if events
            .iter()
            .any(|event| !source_identities.insert(&event.source_identity))
        {
            return Err(StorageError::state_conflict(
                QUANT_VENUE_INCENTIVE_EVENT,
                None::<&str>,
                "complete incentive response contains a duplicate source identity",
            ));
        }
        Ok(())
    }

    async fn persist_scan_on(
        db: &impl ConnectionTrait,
        scan: NewVenueIncentiveReconciliationScan,
    ) -> Result<(), StorageError> {
        let scan_id = scan.venue_incentive_reconciliation_scan_id;
        let outcome = ScanEntity::insert(scan.clone().into_active_model())
            .on_conflict(OnConflict::new().do_nothing().to_owned())
            .try_insert()
            .exec_without_returning(db)
            .await
            .map_err(StorageError::from)?;
        match outcome {
            TryInsertResult::Inserted(1 | 0) | TryInsertResult::Conflicted => {}
            TryInsertResult::Inserted(rows) => {
                return Err(StorageError::invariant_violation(
                    Some(QUANT_VENUE_INCENTIVE_RECONCILIATION_SCAN),
                    format!("single reconciliation scan insert affected {rows} rows"),
                ));
            }
            TryInsertResult::Empty => {
                return Err(StorageError::invariant_violation(
                    Some(QUANT_VENUE_INCENTIVE_RECONCILIATION_SCAN),
                    "single reconciliation scan insert unexpectedly had no input",
                ));
            }
        }
        let existing = ScanEntity::find_by_id(scan_id)
            .one(db)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| {
                StorageError::state_conflict(
                    QUANT_VENUE_INCENTIVE_RECONCILIATION_SCAN,
                    Some(&scan_id),
                    "reconciliation scan disappeared after idempotent insert",
                )
            })?;
        if !Self::scan_matches(&existing, &scan) {
            return Err(StorageError::state_conflict(
                QUANT_VENUE_INCENTIVE_RECONCILIATION_SCAN,
                Some(&scan_id),
                "reconciliation scan identity was replayed with different evidence",
            ));
        }
        Ok(())
    }

    fn scan_matches(existing: &ScanModel, scan: &NewVenueIncentiveReconciliationScan) -> bool {
        existing.execution_account_id == scan.execution_account_id
            && existing.venue_incentive_reconciliation_scan_id
                == scan.venue_incentive_reconciliation_scan_id
            && existing.kind == scan.kind
            && existing.stage == scan.stage
            && existing.program_date == scan.program_date
            && existing.started_at == scan.started_at
            && existing.completed_at == scan.completed_at
            && existing.status == scan.status
            && existing.response_digest == scan.response_digest
            && existing.response_count == scan.response_count
            && existing.error_code == scan.error_code
    }

    async fn validate_response_retry(
        db: &impl ConnectionTrait,
        scan: &NewVenueIncentiveReconciliationScan,
        events: &[NewVenueIncentiveEvent],
    ) -> Result<(), StorageError> {
        let response_digest = scan.response_digest.ok_or_else(|| {
            StorageError::invariant_violation(
                Some(QUANT_VENUE_INCENTIVE_RECONCILIATION_SCAN),
                "response retry validation requires a successful scan digest",
            )
        })?;
        let response_identity = response_digest.to_string();
        let existing = ScanEntity::find()
            .filter(ScanColumn::ExecutionAccountId.eq(scan.execution_account_id))
            .filter(ScanColumn::Kind.eq(scan.kind))
            .filter(ScanColumn::Stage.eq(scan.stage))
            .filter(ScanColumn::ProgramDate.eq(scan.program_date))
            .filter(ScanColumn::ResponseDigest.eq(response_digest))
            .order_by_asc(ScanColumn::CompletedAt)
            .order_by_asc(ScanColumn::CreatedAt)
            .one(db)
            .await
            .map_err(StorageError::from)?;
        let Some(existing) = existing else {
            return Ok(());
        };
        if existing.status != VenueIncentiveReconciliationScanStatus::Succeeded
            || existing.response_count != scan.response_count
        {
            return Err(StorageError::state_conflict(
                QUANT_VENUE_INCENTIVE_RECONCILIATION_SCAN,
                Some(&response_identity),
                "response digest was replayed with a different response count",
            ));
        }
        for event in events {
            let existing_event = EventEntity::find()
                .filter(EventColumn::SourceIdentity.eq(event.source_identity.clone()))
                .one(db)
                .await
                .map_err(StorageError::from)?
                .ok_or_else(|| {
                    StorageError::state_conflict(
                        QUANT_VENUE_INCENTIVE_EVENT,
                        Some(&event.source_identity),
                        "response digest retry omitted the previously durable event identity",
                    )
                })?;
            if !Self::event_matches(&existing_event, event) {
                return Err(StorageError::state_conflict(
                    QUANT_VENUE_INCENTIVE_EVENT,
                    Some(&event.source_identity),
                    "response digest was replayed with different economics or lineage",
                ));
            }
        }
        Ok(())
    }

    async fn lock_reported_accrual_partition(
        db: &impl ConnectionTrait,
        snapshot: &NewVenueIncentiveReportedAccrualSnapshot,
    ) -> Result<(), StorageError> {
        let lock_key = format!(
            "venue-incentive-award:{}:{}",
            snapshot.scan.execution_account_id, snapshot.scan.program_date
        );
        db.execute_raw(Statement::from_sql_and_values(
            DatabaseBackend::Postgres,
            "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
            [lock_key.into()],
        ))
        .await
        .map_err(StorageError::from)?;
        Ok(())
    }

    async fn latest_reported_accrual_partitions(
        db: &impl ConnectionTrait,
        snapshot: &NewVenueIncentiveReportedAccrualSnapshot,
    ) -> Result<BTreeMap<String, EventModel>, StorageError> {
        let rows = EventEntity::find()
            .filter(EventColumn::ExecutionAccountId.eq(snapshot.scan.execution_account_id))
            .filter(EventColumn::Kind.eq(snapshot.scan.kind))
            .filter(EventColumn::Stage.eq(snapshot.scan.stage))
            .filter(EventColumn::ProgramDate.eq(snapshot.scan.program_date))
            .order_by_asc(EventColumn::SourcePartition)
            .order_by_desc(EventColumn::AvailableAt)
            .order_by_desc(EventColumn::CreatedAt)
            .all(db)
            .await
            .map_err(StorageError::from)?;
        let mut latest = BTreeMap::new();
        for row in rows {
            latest.entry(row.source_partition.clone()).or_insert(row);
        }
        Ok(latest)
    }

    async fn latest_total(
        &self,
        execution_account_id: &ExecutionAccountId,
        kind: VenueIncentiveKind,
        stage: VenueIncentiveStage,
        as_of: DateTime<Utc>,
    ) -> Result<Usd, StorageError> {
        let latest_alias = Alias::new("latest_incentive_partition");
        let latest = Query::select()
            .distinct_on([EventColumn::SourcePartition])
            .columns([EventColumn::SourcePartition, EventColumn::AmountUsd])
            .from(EventEntity)
            .and_where(EventColumn::ExecutionAccountId.eq(*execution_account_id))
            .and_where(EventColumn::Kind.eq(kind))
            .and_where(EventColumn::Stage.eq(stage))
            .and_where(EventColumn::AvailableAt.lte(as_of))
            .order_by(EventColumn::SourcePartition, Order::Asc)
            .order_by(EventColumn::AvailableAt, Order::Desc)
            .order_by(EventColumn::CreatedAt, Order::Desc)
            .to_owned();
        let statement = Query::select()
            .expr_as(
                Expr::col((latest_alias.clone(), EventColumn::AmountUsd)).sum(),
                Alias::new("total"),
            )
            .from_subquery(latest, latest_alias)
            .to_owned();
        let row = CreditSum::find_by_statement(self.db.get_database_backend().build(&statement))
            .one(&self.db)
            .await
            .map_err(StorageError::from)?;
        Ok(Usd::new(
            row.and_then(|value| value.total).unwrap_or(Decimal::ZERO),
        ))
    }
}

#[async_trait::async_trait]
impl VenueIncentiveRepository for PgVenueIncentiveRepository {
    async fn record(&self, events: Vec<NewVenueIncentiveEvent>) -> Result<(), StorageError> {
        if events
            .iter()
            .any(|event| event.stage == VenueIncentiveStage::VenueReportedAccrual)
        {
            return Err(StorageError::invariant_violation(
                Some(QUANT_VENUE_INCENTIVE_EVENT),
                "venue-awarded maker facts require a complete apply_reported_accrual_snapshot transaction",
            ));
        }
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        for event in events {
            Self::persist_on(&txn, event).await?;
        }
        txn.commit().await.map_err(StorageError::from)
    }

    async fn record_scan(
        &self,
        scan: NewVenueIncentiveReconciliationScan,
        events: Vec<NewVenueIncentiveEvent>,
    ) -> Result<(), StorageError> {
        Self::validate_scan(&scan, &events)?;
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        Self::validate_response_retry(&txn, &scan, &events).await?;
        Self::persist_scan_on(&txn, scan).await?;
        for event in events {
            Self::persist_on(&txn, event).await?;
        }
        txn.commit().await.map_err(StorageError::from)
    }

    async fn apply_reported_accrual_snapshot(
        &self,
        snapshot: NewVenueIncentiveReportedAccrualSnapshot,
    ) -> Result<(), StorageError> {
        Self::validate_scan(&snapshot.scan, &snapshot.awards)?;
        if snapshot.scan.kind != VenueIncentiveKind::MakerRebate
            || snapshot.scan.stage != VenueIncentiveStage::VenueReportedAccrual
            || snapshot.scan.status != VenueIncentiveReconciliationScanStatus::Succeeded
        {
            return Err(StorageError::invariant_violation(
                Some(QUANT_VENUE_INCENTIVE_RECONCILIATION_SCAN),
                "award snapshot must be a successful maker venue-award scan",
            ));
        }
        let mut response_partitions = BTreeSet::new();
        for award in &snapshot.awards {
            if !response_partitions.insert(award.source_partition.clone()) {
                return Err(StorageError::state_conflict(
                    QUANT_VENUE_INCENTIVE_EVENT,
                    Some(&award.source_partition),
                    "complete award response contains a duplicate partition",
                ));
            }
        }
        let response_digest = snapshot.scan.response_digest.ok_or_else(|| {
            StorageError::invariant_violation(
                Some(QUANT_VENUE_INCENTIVE_RECONCILIATION_SCAN),
                "successful award scan is missing its response digest",
            )
        })?;
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        Self::lock_reported_accrual_partition(&txn, &snapshot).await?;
        Self::validate_response_retry(&txn, &snapshot.scan, &snapshot.awards).await?;
        Self::persist_scan_on(&txn, snapshot.scan.clone()).await?;
        let previous = Self::latest_reported_accrual_partitions(&txn, &snapshot).await?;
        for award in snapshot.awards {
            Self::persist_on(&txn, award).await?;
        }
        for (source_partition, prior) in previous {
            if response_partitions.contains(&source_partition) {
                continue;
            }
            Self::persist_on(
                &txn,
                NewVenueIncentiveEvent {
                    venue_incentive_event_id: VenueIncentiveEventId::from_v7(),
                    execution_account_id: snapshot.scan.execution_account_id,
                    execution_fill_id: None,
                    market_id: prior.market_id,
                    kind: VenueIncentiveKind::MakerRebate,
                    stage: VenueIncentiveStage::VenueReportedAccrual,
                    program_date: snapshot.scan.program_date,
                    amount_usd: Usd::ZERO,
                    source_terms_hash: None,
                    source_identity: format!("{source_partition}:retracted:{response_digest}"),
                    source_partition,
                    transaction_hash: None,
                    observed_at: snapshot.scan.completed_at,
                    available_at: snapshot.scan.completed_at,
                    evidence_hash: response_digest,
                },
            )
            .await?;
        }
        txn.commit().await.map_err(StorageError::from)
    }

    async fn credited_cumulative(
        &self,
        execution_account_id: &ExecutionAccountId,
        as_of: DateTime<Utc>,
    ) -> Result<Usd, StorageError> {
        let maker = self
            .latest_total(
                execution_account_id,
                VenueIncentiveKind::MakerRebate,
                VenueIncentiveStage::WalletCredited,
                as_of,
            )
            .await?;
        let taker = self
            .latest_total(
                execution_account_id,
                VenueIncentiveKind::TakerRebate,
                VenueIncentiveStage::WalletCredited,
                as_of,
            )
            .await?;
        Ok(maker + taker)
    }

    async fn reconciliation_cumulative(
        &self,
        execution_account_id: &ExecutionAccountId,
        as_of: DateTime<Utc>,
    ) -> Result<VenueIncentiveReconciliation, StorageError> {
        let maker_accrual = self
            .latest_total(
                execution_account_id,
                VenueIncentiveKind::MakerRebate,
                VenueIncentiveStage::EstimatedAccrual,
                as_of,
            )
            .await?;
        let maker_award = self
            .latest_total(
                execution_account_id,
                VenueIncentiveKind::MakerRebate,
                VenueIncentiveStage::VenueReportedAccrual,
                as_of,
            )
            .await?;
        let maker_cash = self
            .latest_total(
                execution_account_id,
                VenueIncentiveKind::MakerRebate,
                VenueIncentiveStage::WalletCredited,
                as_of,
            )
            .await?;
        let taker_cash = self
            .latest_total(
                execution_account_id,
                VenueIncentiveKind::TakerRebate,
                VenueIncentiveStage::WalletCredited,
                as_of,
            )
            .await?;
        Ok(VenueIncentiveReconciliation {
            as_of,
            estimated_maker_accrual_usd: maker_accrual,
            venue_reported_maker_accrual_usd: maker_award,
            wallet_credited_maker_usd: maker_cash,
            wallet_credited_taker_usd: taker_cash,
        })
    }

    async fn maker_credit_pending_since(
        &self,
        execution_account_id: &ExecutionAccountId,
        as_of: DateTime<Utc>,
    ) -> Result<Option<DateTime<Utc>>, StorageError> {
        let rows = EventEntity::find()
            .filter(EventColumn::ExecutionAccountId.eq(*execution_account_id))
            .filter(EventColumn::Kind.eq(VenueIncentiveKind::MakerRebate))
            .filter(EventColumn::Stage.eq(VenueIncentiveStage::VenueReportedAccrual))
            .filter(EventColumn::AvailableAt.lte(as_of))
            .order_by_asc(EventColumn::SourcePartition)
            .order_by_desc(EventColumn::AvailableAt)
            .order_by_desc(EventColumn::CreatedAt)
            .all(&self.db)
            .await
            .map_err(StorageError::from)?;
        let mut latest = BTreeMap::new();
        for row in rows {
            latest.entry(row.source_partition.clone()).or_insert(row);
        }
        let mut awards = latest
            .into_values()
            .filter(|row| row.amount_usd.is_positive())
            .collect::<Vec<_>>();
        awards.sort_by_key(|row| (row.available_at, row.created_at));
        let mut credited = self
            .latest_total(
                execution_account_id,
                VenueIncentiveKind::MakerRebate,
                VenueIncentiveStage::WalletCredited,
                as_of,
            )
            .await?;
        for award in awards {
            if credited >= award.amount_usd {
                credited -= award.amount_usd;
            } else {
                return Ok(Some(award.available_at));
            }
        }
        Ok(None)
    }

    async fn scans(
        &self,
        execution_account_id: &ExecutionAccountId,
        from: NaiveDate,
        to: NaiveDate,
    ) -> Result<Vec<VenueIncentiveReconciliationScanInfo>, StorageError> {
        ScanEntity::find()
            .filter(ScanColumn::ExecutionAccountId.eq(*execution_account_id))
            .filter(ScanColumn::ProgramDate.gte(from))
            .filter(ScanColumn::ProgramDate.lte(to))
            .order_by_asc(ScanColumn::ProgramDate)
            .order_by_asc(ScanColumn::Kind)
            .order_by_asc(ScanColumn::Stage)
            .order_by_desc(ScanColumn::CompletedAt)
            .order_by_desc(ScanColumn::CreatedAt)
            .all(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|rows| rows.into_iter().map(Into::into).collect())
    }

    async fn maker_valuation_events(
        &self,
        execution_account_id: &ExecutionAccountId,
        as_of: DateTime<Utc>,
    ) -> Result<Vec<VenueIncentiveEventInfo>, StorageError> {
        EventEntity::find()
            .filter(EventColumn::ExecutionAccountId.eq(*execution_account_id))
            .filter(EventColumn::Kind.eq(VenueIncentiveKind::MakerRebate))
            .filter(
                Condition::any()
                    .add(EventColumn::Stage.eq(VenueIncentiveStage::EstimatedAccrual))
                    .add(EventColumn::Stage.eq(VenueIncentiveStage::VenueReportedAccrual))
                    .add(EventColumn::Stage.eq(VenueIncentiveStage::WalletCredited)),
            )
            .filter(EventColumn::AvailableAt.lte(as_of))
            .order_by_asc(EventColumn::ProgramDate)
            .order_by_asc(EventColumn::ObservedAt)
            .order_by_asc(EventColumn::AvailableAt)
            .order_by_asc(EventColumn::CreatedAt)
            .all(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|rows| rows.into_iter().map(Into::into).collect())
    }

    async fn page_events(
        &self,
        execution_account_id: &ExecutionAccountId,
        query: VenueIncentiveEventListQuery,
    ) -> Result<Paginated<VenueIncentiveEventInfo>, StorageError> {
        paginate_mapped(
            EventEntity::find()
                .filter(event_condition(execution_account_id, &query))
                .order_by_desc(EventColumn::ProgramDate)
                .order_by_desc(EventColumn::AvailableAt)
                .order_by_desc(EventColumn::CreatedAt),
            &self.db,
            PageWindow::from_query(&query),
            Into::into,
        )
        .await
    }
}
