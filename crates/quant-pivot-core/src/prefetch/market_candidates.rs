//! Core-side projector that freezes [`MarketCandidate`] slices for the research
//! market selector.
//!
//! [`MarketCandidateProvider`] loads the complete durable catalog candidate set and
//! primary books once per decision boundary. Selection, feature computation,
//! and decision capture then read the same immutable [`DecisionSnapshotSource`]
//! so concurrent ingest/catalog updates cannot tear a report round.

use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use quant_pivot_error::{QuantResult, research::ResearchError};
use quant_pivot_models::{
    domain::{
        DecisionBoundary, DomainAvailability, MarketCandidate, MarketDataHealth, MarketRegistryInfo,
    },
    enums::common::MarketCategory,
    runtime_config::DomainConfig,
    types::{MarketId, TokenId},
};
use quant_pivot_repository::traits::{MarketLinkageRepository, QuantFactReadRepository};
use quant_pivot_research::{
    features::ResolvedBook,
    pit::{BookSnapshotAt, PointInTimeSnapshotSource, ResolvedMarketSnapshot},
};

use crate::prefetch::domain_availability::resolve_domain_availability;

/// One report round's immutable PIT world and its selection candidates.
pub struct MarketCandidateBatch {
    pub candidates: Vec<MarketCandidate>,
    pub snapshot_source: Arc<DecisionSnapshotSource>,
}

/// Boundary-bound in-memory projection returned by the durable batch resolver.
pub struct DecisionSnapshotSource {
    boundary: DecisionBoundary,
    books: HashMap<TokenId, BookSnapshotAt>,
    markets: HashMap<MarketId, ResolvedMarketSnapshot>,
}

impl DecisionSnapshotSource {
    fn new(
        boundary: DecisionBoundary,
        books: HashMap<TokenId, BookSnapshotAt>,
        snapshots: Vec<ResolvedMarketSnapshot>,
    ) -> Self {
        let markets = snapshots
            .into_iter()
            .map(|snapshot| (snapshot.market.market_id.clone(), snapshot))
            .collect();
        Self {
            boundary,
            books,
            markets,
        }
    }

    fn validate_boundary(&self, boundary: &DecisionBoundary) -> QuantResult<()> {
        if boundary != &self.boundary {
            return Err(ResearchError::PitResolution {
                detail: "decision snapshot was queried with a different boundary".to_owned(),
            }
            .into());
        }
        Ok(())
    }
}

#[async_trait]
impl PointInTimeSnapshotSource for DecisionSnapshotSource {
    async fn book_at_boundary(
        &self,
        token_id: &TokenId,
        boundary: &DecisionBoundary,
    ) -> QuantResult<Option<BookSnapshotAt>> {
        self.validate_boundary(boundary)?;
        Ok(self.books.get(token_id).cloned())
    }

    async fn books_at_boundary(
        &self,
        token_ids: &[TokenId],
        boundary: &DecisionBoundary,
    ) -> QuantResult<HashMap<TokenId, BookSnapshotAt>> {
        self.validate_boundary(boundary)?;
        Ok(token_ids
            .iter()
            .filter_map(|token_id| {
                self.books
                    .get(token_id)
                    .cloned()
                    .map(|book| (token_id.clone(), book))
            })
            .collect())
    }

    async fn market_snapshot_at(
        &self,
        market_id: &MarketId,
        boundary: &DecisionBoundary,
    ) -> QuantResult<Option<ResolvedMarketSnapshot>> {
        self.validate_boundary(boundary)?;
        Ok(self.markets.get(market_id).cloned())
    }

    async fn market_snapshots_at_boundary(
        &self,
        boundary: &DecisionBoundary,
    ) -> QuantResult<Vec<ResolvedMarketSnapshot>> {
        self.validate_boundary(boundary)?;
        let mut snapshots = self.markets.values().cloned().collect::<Vec<_>>();
        snapshots.sort_by(|left, right| {
            left.market
                .market_id
                .as_str()
                .cmp(right.market.market_id.as_str())
        });
        Ok(snapshots)
    }
}

/// Projects the decision-time market world into frozen candidate facts.
pub struct MarketCandidateProvider {
    pit_source: Arc<dyn PointInTimeSnapshotSource>,
    linkage_repo: Arc<dyn MarketLinkageRepository>,
    fact_read: Arc<dyn QuantFactReadRepository>,
}

impl MarketCandidateProvider {
    /// Build the provider over the durable PIT resolver and domain evidence.
    #[must_use]
    pub const fn new(
        pit_source: Arc<dyn PointInTimeSnapshotSource>,
        linkage_repo: Arc<dyn MarketLinkageRepository>,
        fact_read: Arc<dyn QuantFactReadRepository>,
    ) -> Self {
        Self {
            pit_source,
            linkage_repo,
            fact_read,
        }
    }

    /// Freeze every visible market and required book at one boundary.
    ///
    /// # Errors
    ///
    /// Propagates linkage-ledger / domain-fact read failures (the domain plane
    /// fails closed as a whole rather than serving guessed availability).
    pub async fn candidates(
        &self,
        boundary: &DecisionBoundary,
        domain: &DomainConfig,
    ) -> QuantResult<MarketCandidateBatch> {
        let decision_at = boundary.decision_at();
        let snapshots = self
            .pit_source
            .market_snapshots_at_boundary(boundary)
            .await?;
        let mut token_ids = snapshots
            .iter()
            .flat_map(|snapshot| {
                [
                    snapshot.market.token_yes.clone(),
                    snapshot.market.token_no.clone(),
                ]
            })
            .collect::<Vec<_>>();
        token_ids.sort_by(|left, right| left.as_str().cmp(right.as_str()));
        token_ids.dedup();
        let books = self
            .pit_source
            .books_at_boundary(&token_ids, boundary)
            .await?;
        let infos = snapshots
            .iter()
            .map(|snapshot| Arc::clone(&snapshot.market))
            .collect::<Vec<_>>();
        let availability = self
            .project_domain_availability(&infos, boundary, domain)
            .await?;
        let mut candidates = Vec::with_capacity(snapshots.len());
        for snapshot in &snapshots {
            let info = snapshot.market.as_ref();
            let domain_availability =
                availability.get(&info.market_id).copied().ok_or_else(|| {
                    ResearchError::Determinism {
                        detail: format!(
                            "domain availability batch omitted market {}",
                            info.market_id
                        ),
                    }
                })?;
            let book = books
                .get(&info.token_yes)
                .cloned()
                .map(ResolvedBook::try_from)
                .transpose()?;
            candidates.push(project_candidate(
                info,
                book.as_ref(),
                decision_at,
                domain_availability,
            )?);
        }
        Ok(MarketCandidateBatch {
            candidates,
            snapshot_source: Arc::new(DecisionSnapshotSource::new(
                boundary.clone(),
                books,
                snapshots,
            )),
        })
    }

    /// One batched domain-availability reading for the category-mapped subset
    /// (Phase 11.2.2 §3.8): mapped ∧ enabled ∧ `Resolved` linkage ∧ the linked
    /// instrument has an observation effective by its source cutoff and
    /// available by the decision time.
    ///
    /// Thin wrapper over [`resolve_domain_availability`] — the same batched
    /// projector the offline keep-rate estimator uses, so the live and
    /// offline planes can never drift on this decision (Phase 11.2.2 §3.8
    /// train-serve parity).
    async fn project_domain_availability(
        &self,
        infos: &[Arc<MarketRegistryInfo>],
        boundary: &DecisionBoundary,
        domain: &DomainConfig,
    ) -> QuantResult<HashMap<MarketId, DomainAvailability>> {
        let markets: Vec<(MarketId, MarketCategory)> = infos
            .iter()
            .map(|info| (info.market_id.clone(), info.fee_category()))
            .collect();
        resolve_domain_availability(
            self.linkage_repo.as_ref(),
            self.fact_read.as_ref(),
            domain,
            boundary,
            &markets,
        )
        .await
    }
}

fn project_candidate(
    info: &MarketRegistryInfo,
    book: Option<&ResolvedBook>,
    decision_at: DateTime<Utc>,
    domain_availability: DomainAvailability,
) -> QuantResult<MarketCandidate> {
    let book_age_ms = book
        .map(|book| {
            u64::try_from((decision_at - book.effective_at).num_milliseconds()).map_err(|_| {
                ResearchError::PitResolution {
                    detail: format!(
                        "book {} observation {} is after decision {decision_at}",
                        book.token_id, book.effective_at
                    ),
                }
            })
        })
        .transpose()?;
    Ok(MarketCandidate {
        market_id: info.market_id.clone(),
        event_id: info.event_id.clone(),
        category: info.fee_category(),
        status: info.status,
        primary_token_id: info.token_yes.clone(),
        secondary_token_id: Some(info.token_no.clone()),
        end_date: info.end_date,
        liquidity_usd: info.liquidity_usd,
        volume_24h_usd: info.volume_24h,
        best_bid: book.and_then(ResolvedBook::best_bid),
        best_ask: book.and_then(ResolvedBook::best_ask),
        depth_usd: book.map(ResolvedBook::visible_liquidity_usd),
        book_age_ms,
        crossed: book.map(ResolvedBook::is_crossed),
        empty: book.map(ResolvedBook::is_empty),
        market_data_health: MarketDataHealth::NotApplicable,
        ingest_lag_ms: None,
        domain_availability,
        decision_at,
    })
}
