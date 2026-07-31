//! Bounded, zero-I/O replay pages over one independently verified Source Slice.

use std::{
    cmp::Ordering,
    collections::{BTreeSet, HashMap, HashSet},
};

use chrono::{DateTime, Utc};
use quant_pivot_error::{QuantResult, research::ResearchError};
use quant_pivot_models::{
    clickhouse::{
        BookL2LedgerRow, BookMicrostructureRow, BookStreamSessionRow, MarketResolutionRow,
        TradeTapeRow,
    },
    domain::{
        data_plane::{
            CryptoPriceReport, DecisionBoundary, DecisionSource, DomainObservation,
            WeatherForecastPoint, WeatherObservationFact,
        },
        market::{CatalogEventChangeInfo, CatalogMarketChangeInfo},
        quant::{LinkageOutcome, MarketLinkage},
    },
    enums::clickhouse::ChCanonicalBookEventType,
    types::{
        ClobMarketInfoVersion, DomainInstrumentKey, MarketId, SourceSliceInvalidSession, TokenId,
    },
};
use quant_pivot_research::{domain::weather_history_start, pit::BookSnapshotAt};
use uuid::Uuid;

use super::source_slice::FrozenSourceSlice;
use crate::pit::platform::ch_historical::{reconstruct_snapshot, reconstruct_snapshot_series};

/// Maximum candidate markets in a single replay page. Callers page above this
/// boundary; token and candidate cardinality never create per-row repository I/O.
pub const MAX_REPLAY_PAGE_MARKETS: usize = 100;

/// One bounded market page and half-open event-time interval.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplayPageRequest {
    pub market_ids: Vec<MarketId>,
    pub token_ids: Vec<TokenId>,
    pub window_start: DateTime<Utc>,
    pub window_end: DateTime<Utc>,
    pub available_by: DateTime<Utc>,
}

impl ReplayPageRequest {
    fn validate(&self) -> QuantResult<()> {
        if self.market_ids.is_empty() || self.token_ids.is_empty() {
            return Err(ResearchError::DatasetPlan {
                detail: "ReplayPage requires at least one market and token".to_owned(),
            }
            .into());
        }
        if self.market_ids.len() > MAX_REPLAY_PAGE_MARKETS {
            return Err(ResearchError::DatasetPlan {
                detail: format!(
                    "ReplayPage market count {} exceeds page limit {MAX_REPLAY_PAGE_MARKETS}",
                    self.market_ids.len()
                ),
            }
            .into());
        }
        if self.window_start >= self.window_end || self.available_by < self.window_start {
            return Err(ResearchError::DatasetPlan {
                detail: "ReplayPage requires a non-empty window and available_by at or after window_start"
                    .to_owned(),
            }
            .into());
        }
        if self.market_ids.iter().collect::<HashSet<_>>().len() != self.market_ids.len()
            || self.token_ids.iter().collect::<HashSet<_>>().len() != self.token_ids.len()
        {
            return Err(ResearchError::DatasetPlan {
                detail: "ReplayPage market_ids and token_ids must be unique".to_owned(),
            }
            .into());
        }
        Ok(())
    }
}

/// All immutable facts needed to replay one candidate page.
///
/// This is a data contract, not a repository facade: Fit, Dataset, and Validate
/// receive this owned page and cannot query live storage inside candidate loops.
#[derive(Debug, Clone)]
pub struct ReplayPage {
    pub market_ids: Vec<MarketId>,
    pub token_ids: Vec<TokenId>,
    pub catalog_markets: Vec<CatalogMarketChangeInfo>,
    pub catalog_events: Vec<CatalogEventChangeInfo>,
    pub clob_market_info: Vec<ClobMarketInfoVersion>,
    pub snapshots: Vec<BookL2LedgerRow>,
    pub sessions: Vec<BookStreamSessionRow>,
    pub gaps: Vec<SourceSliceInvalidSession>,
    pub l2_ledger: Vec<BookL2LedgerRow>,
    pub microstructure: Vec<BookMicrostructureRow>,
    pub trade_tape: Vec<TradeTapeRow>,
    pub resolutions: Vec<MarketResolutionRow>,
    pub linkages: Vec<MarketLinkage>,
    pub domain_observations: Vec<DomainObservation>,
    pub crypto_reports: Vec<CryptoPriceReport>,
    pub weather_observations: Vec<WeatherObservationFact>,
    pub weather_forecasts: Vec<WeatherForecastPoint>,
}

impl ReplayPage {
    /// Resolve the latest market-info version visible for this token at the
    /// decision boundary. Fee consumers must use this bitemporal lookup and
    /// may never consult a current cache while replaying historical fills.
    #[must_use]
    pub fn market_info_at(
        &self,
        market_id: &MarketId,
        token_id: &TokenId,
        boundary: &DecisionBoundary,
    ) -> Option<&ClobMarketInfoVersion> {
        self.clob_market_info
            .iter()
            .filter(|version| {
                &version.market_id == market_id
                    && version
                        .tokens
                        .iter()
                        .any(|token| &token.token_id == token_id)
                    && version.effective_at <= boundary.knowledge_cutoff()
                    && version.available_at <= boundary.decision_at()
            })
            .max_by(|left, right| {
                (left.effective_at, left.available_at, &left.payload_hash).cmp(&(
                    right.effective_at,
                    right.available_at,
                    &right.payload_hash,
                ))
            })
    }

    /// Reconstruct the exact snapshot visible at one decision boundary without
    /// touching a repository. The page must carry the pre-window snapshot,
    /// all intervening canonical events/trades, and its session ledger.
    pub fn book_at_boundary(
        &self,
        token_id: &TokenId,
        boundary: &DecisionBoundary,
    ) -> QuantResult<Option<BookSnapshotAt>> {
        let source_cutoff = boundary.cutoff_for(DecisionSource::Book);
        let decision_at = boundary.decision_at();
        let snapshot = self.snapshot_at(token_id, boundary).cloned();
        let Some(snapshot) = snapshot else {
            return Ok(None);
        };
        self.validate_session(token_id, snapshot.stream_session_id, decision_at)?;
        let session = self
            .session_at(snapshot.stream_session_id, decision_at)
            .ok_or_else(|| ResearchError::PitResolution {
                detail: format!(
                    "Source Slice session ledger {} for token {} is unavailable",
                    snapshot.stream_session_id, token_id
                ),
            })?;
        let mut events = self
            .l2_ledger
            .iter()
            .filter(|row| {
                &row.token_id == token_id
                    && row.stream_session_id == snapshot.stream_session_id
                    && row.token_sequence >= snapshot.token_sequence
                    && row.venue_event_time <= source_cutoff.timestamp_millis()
                    && row.persisted_time <= decision_at.timestamp_millis()
            })
            .cloned()
            .collect::<Vec<_>>();
        events.sort_by_key(|row| row.token_sequence);
        reconstruct_snapshot(snapshot, session, &events, source_cutoff, decision_at)
    }

    /// Reconstruct a strictly monotonic timeline with one event-sequence walk
    /// per stream session. Periodic snapshots do not restart replay; a later
    /// session starts a new independently verified chain.
    pub fn books_at_boundaries(
        &self,
        token_id: &TokenId,
        boundaries: &[DecisionBoundary],
    ) -> QuantResult<Vec<Option<BookSnapshotAt>>> {
        if boundaries.windows(2).any(|pair| {
            pair[0].decision_at() >= pair[1].decision_at()
                || pair[0].cutoff_for(DecisionSource::Book)
                    >= pair[1].cutoff_for(DecisionSource::Book)
        }) {
            return Err(ResearchError::PitResolution {
                detail: format!(
                    "Source Slice boundaries for token {token_id} are not strictly monotonic"
                ),
            }
            .into());
        }
        let selected = self.snapshots_at_boundaries(token_id, boundaries);
        let mut books = vec![None; boundaries.len()];
        let mut start = 0_usize;
        while start < boundaries.len() {
            let Some(anchor) = selected[start] else {
                start = start
                    .checked_add(1)
                    .ok_or_else(|| ResearchError::PitResolution {
                        detail: "ReplayPage boundary cursor overflow".to_owned(),
                    })?;
                continue;
            };
            let session_id = anchor.stream_session_id;
            let mut end = start
                .checked_add(1)
                .ok_or_else(|| ResearchError::PitResolution {
                    detail: "ReplayPage session cursor overflow".to_owned(),
                })?;
            while end < boundaries.len()
                && selected[end].is_some_and(|snapshot| snapshot.stream_session_id == session_id)
            {
                end = end
                    .checked_add(1)
                    .ok_or_else(|| ResearchError::PitResolution {
                        detail: "ReplayPage session cursor overflow".to_owned(),
                    })?;
            }
            let final_boundary =
                boundaries
                    .get(end - 1)
                    .ok_or_else(|| ResearchError::PitResolution {
                        detail: "ReplayPage session has no final boundary".to_owned(),
                    })?;
            self.validate_session(token_id, session_id, final_boundary.decision_at())?;
            let session = self
                .session_at(session_id, final_boundary.decision_at())
                .ok_or_else(|| ResearchError::PitResolution {
                    detail: format!(
                        "Source Slice session ledger {session_id} for token {token_id} is unavailable"
                    ),
                })?;
            let final_cutoff = final_boundary.cutoff_for(DecisionSource::Book);
            let mut events = self
                .l2_ledger
                .iter()
                .filter(|row| {
                    &row.token_id == token_id
                        && row.stream_session_id == session_id
                        && row.token_sequence >= anchor.token_sequence
                        && row.venue_event_time <= final_cutoff.timestamp_millis()
                        && row.persisted_time <= final_boundary.decision_at().timestamp_millis()
                })
                .cloned()
                .collect::<Vec<_>>();
            events.sort_by_key(|row| row.token_sequence);
            let snapshots = reconstruct_snapshot_series(
                anchor.clone(),
                session,
                &events,
                &boundaries[start..end],
            )?;
            for (slot, snapshot) in books[start..end].iter_mut().zip(snapshots) {
                *slot = Some(snapshot);
            }
            start = end;
        }
        Ok(books)
    }

    fn snapshot_at(
        &self,
        token_id: &TokenId,
        boundary: &DecisionBoundary,
    ) -> Option<&BookL2LedgerRow> {
        let source_cutoff = boundary.cutoff_for(DecisionSource::Book);
        let decision_at = boundary.decision_at();
        self.snapshots
            .iter()
            .filter(|row| {
                &row.token_id == token_id
                    && row.event_type == ChCanonicalBookEventType::Snapshot
                    && row.venue_event_time <= source_cutoff.timestamp_millis()
                    && row.persisted_time <= decision_at.timestamp_millis()
            })
            .max_by(|left, right| snapshot_order(left, right))
    }

    fn snapshots_at_boundaries<'a>(
        &'a self,
        token_id: &TokenId,
        boundaries: &[DecisionBoundary],
    ) -> Vec<Option<&'a BookL2LedgerRow>> {
        let mut arrivals = vec![Vec::<&BookL2LedgerRow>::new(); boundaries.len()];
        for snapshot in self.snapshots.iter().filter(|row| {
            &row.token_id == token_id && row.event_type == ChCanonicalBookEventType::Snapshot
        }) {
            let mut low = 0_usize;
            let mut high = boundaries.len();
            while low < high {
                let middle = low + (high - low) / 2;
                let boundary = &boundaries[middle];
                let eligible = snapshot.venue_event_time
                    <= boundary.cutoff_for(DecisionSource::Book).timestamp_millis()
                    && snapshot.persisted_time <= boundary.decision_at().timestamp_millis();
                if eligible {
                    high = middle;
                } else {
                    low = middle + 1;
                }
            }
            if let Some(bucket) = arrivals.get_mut(low) {
                bucket.push(snapshot);
            }
        }
        let mut latest = None;
        arrivals
            .into_iter()
            .map(|arrived| {
                for snapshot in arrived {
                    if latest.is_none_or(|current| snapshot_order(current, snapshot).is_lt()) {
                        latest = Some(snapshot);
                    }
                }
                latest
            })
            .collect()
    }

    fn session_at(
        &self,
        session_id: Uuid,
        decision_at: DateTime<Utc>,
    ) -> Option<&BookStreamSessionRow> {
        self.sessions
            .iter()
            .filter(|row| {
                row.stream_session_id == session_id
                    && row.recorded_at <= decision_at.timestamp_millis()
            })
            .max_by_key(|row| row.ledger_sequence)
    }

    fn validate_session(
        &self,
        token_id: &TokenId,
        session_id: Uuid,
        decision_at: DateTime<Utc>,
    ) -> QuantResult<()> {
        if self.gaps.iter().any(|gap| {
            gap.token_id == token_id.as_str()
                && gap.session_id == session_id.to_string()
                && gap.invalidated_at <= decision_at
        }) {
            return Err(ResearchError::PitResolution {
                detail: format!(
                    "Source Slice session {session_id} for token {token_id} is invalid at decision {decision_at}"
                ),
            }
            .into());
        }
        Ok(())
    }
}

fn snapshot_order(left: &BookL2LedgerRow, right: &BookL2LedgerRow) -> Ordering {
    (left.venue_event_time, left.token_sequence, left.event_hash).cmp(&(
        right.venue_event_time,
        right.token_sequence,
        right.event_hash,
    ))
}

impl FrozenSourceSlice {
    /// Read one bounded candidate page entirely from verified in-memory facts.
    pub fn replay_page(&self, request: &ReplayPageRequest) -> QuantResult<ReplayPage> {
        request.validate()?;
        let scope = ReplayPageScope::new(request);
        let catalog = page_catalog(self, &scope);
        let books = page_books(self, &scope);
        let market_facts = page_market_facts(self, &scope, &books.replay_start_by_token);
        let domain = page_domain_facts(self, &scope, &market_facts.linkages)?;
        Ok(ReplayPage {
            market_ids: request.market_ids.clone(),
            token_ids: request.token_ids.clone(),
            catalog_markets: catalog.markets,
            catalog_events: catalog.events,
            clob_market_info: catalog.clob_market_info,
            snapshots: books.snapshots,
            sessions: books.sessions,
            gaps: books.gaps,
            l2_ledger: books.l2_ledger,
            microstructure: market_facts.microstructure,
            trade_tape: market_facts.trade_tape,
            resolutions: market_facts.resolutions,
            linkages: market_facts.linkages,
            domain_observations: domain.observations,
            crypto_reports: domain.crypto_reports,
            weather_observations: domain.weather_observations,
            weather_forecasts: domain.weather_forecasts,
        })
    }
}

struct ReplayPageScope<'a> {
    request: &'a ReplayPageRequest,
    markets: HashSet<MarketId>,
    tokens: HashSet<TokenId>,
    start_ms: i64,
    end_ms: i64,
    available_ms: i64,
}

impl<'a> ReplayPageScope<'a> {
    fn new(request: &'a ReplayPageRequest) -> Self {
        Self {
            markets: request.market_ids.iter().cloned().collect(),
            tokens: request.token_ids.iter().cloned().collect(),
            start_ms: request.window_start.timestamp_millis(),
            end_ms: request.window_end.timestamp_millis(),
            available_ms: request.available_by.timestamp_millis(),
            request,
        }
    }
}

struct CatalogPage {
    markets: Vec<CatalogMarketChangeInfo>,
    events: Vec<CatalogEventChangeInfo>,
    clob_market_info: Vec<ClobMarketInfoVersion>,
}

fn page_catalog(source: &FrozenSourceSlice, scope: &ReplayPageScope<'_>) -> CatalogPage {
    let markets = source
        .prefetched
        .catalog
        .market_changes
        .iter()
        .filter(|row| {
            scope.markets.contains(&row.market_id)
                && row.source_effective_at < scope.request.window_end
                && row.available_at <= scope.request.available_by
        })
        .cloned()
        .collect::<Vec<_>>();
    let event_ids = markets
        .iter()
        .map(|row| row.event_id.clone())
        .collect::<HashSet<_>>();
    let events = source
        .prefetched
        .catalog
        .event_changes
        .iter()
        .filter(|row| {
            event_ids.contains(&row.event_id)
                && row.source_effective_at < scope.request.window_end
                && row.available_at <= scope.request.available_by
        })
        .cloned()
        .collect();
    let clob_market_info = source
        .clob_market_info
        .iter()
        .filter(|row| {
            scope.markets.contains(&row.market_id)
                && row.effective_at < scope.request.window_end
                && row.available_at <= scope.request.available_by
        })
        .cloned()
        .collect();
    CatalogPage {
        markets,
        events,
        clob_market_info,
    }
}

struct BookPage {
    replay_start_by_token: HashMap<TokenId, i64>,
    snapshots: Vec<BookL2LedgerRow>,
    sessions: Vec<BookStreamSessionRow>,
    gaps: Vec<SourceSliceInvalidSession>,
    l2_ledger: Vec<BookL2LedgerRow>,
}

fn page_books(source: &FrozenSourceSlice, scope: &ReplayPageScope<'_>) -> BookPage {
    let mut replay_start_by_token = HashMap::<TokenId, i64>::new();
    let mut snapshots = Vec::new();
    for token in &scope.request.token_ids {
        let Some(rows) = source.prefetched.books.get(token) else {
            replay_start_by_token.insert(token.clone(), scope.start_ms);
            continue;
        };
        let anchor = rows
            .iter()
            .filter(|row| {
                row.event_type == ChCanonicalBookEventType::Snapshot
                    && row.venue_event_time <= scope.start_ms
                    && row.persisted_time <= scope.available_ms
            })
            .max_by(|left, right| {
                (left.venue_event_time, left.token_sequence, left.event_hash).cmp(&(
                    right.venue_event_time,
                    right.token_sequence,
                    right.event_hash,
                ))
            });
        replay_start_by_token.insert(
            token.clone(),
            anchor.map_or(scope.start_ms, |row| row.venue_event_time),
        );
        if let Some(anchor) = anchor {
            snapshots.push(anchor.clone());
        }
        snapshots.extend(
            rows.iter()
                .filter(|row| {
                    row.event_type == ChCanonicalBookEventType::Snapshot
                        && row.venue_event_time > scope.start_ms
                        && row.venue_event_time < scope.end_ms
                        && row.persisted_time <= scope.available_ms
                })
                .cloned(),
        );
    }
    snapshots.sort_by(|left, right| {
        (
            &left.token_id,
            left.venue_event_time,
            left.token_sequence,
            left.event_hash,
        )
            .cmp(&(
                &right.token_id,
                right.venue_event_time,
                right.token_sequence,
                right.event_hash,
            ))
    });
    let l2_ledger = source
        .l2_ledger
        .iter()
        .filter(|row| {
            scope.tokens.contains(&row.token_id)
                && row.venue_event_time
                    >= replay_start_by_token
                        .get(&row.token_id)
                        .copied()
                        .unwrap_or(scope.start_ms)
                && row.venue_event_time < scope.end_ms
                && row.persisted_time <= scope.available_ms
        })
        .cloned()
        .collect::<Vec<_>>();
    let session_ids = snapshots
        .iter()
        .map(|row| row.stream_session_id)
        .chain(l2_ledger.iter().map(|row| row.stream_session_id))
        .collect::<HashSet<_>>();
    let sessions = source
        .sessions
        .iter()
        .filter(|row| {
            session_ids.contains(&row.stream_session_id) && row.recorded_at <= scope.available_ms
        })
        .cloned()
        .collect();
    let starts = scope
        .tokens
        .iter()
        .map(|token| {
            (
                token.as_str(),
                replay_start_by_token
                    .get(token)
                    .copied()
                    .unwrap_or(scope.start_ms),
            )
        })
        .collect::<HashMap<_, _>>();
    let gaps = source
        .invalid_sessions
        .iter()
        .filter(|gap| {
            starts
                .get(gap.token_id.as_str())
                .and_then(|start| DateTime::from_timestamp_millis(*start))
                .is_some_and(|start| gap.invalidated_at >= start)
                && gap.invalidated_at < scope.request.window_end
        })
        .cloned()
        .collect();
    BookPage {
        replay_start_by_token,
        snapshots,
        sessions,
        gaps,
        l2_ledger,
    }
}

struct MarketFactPage {
    microstructure: Vec<BookMicrostructureRow>,
    trade_tape: Vec<TradeTapeRow>,
    resolutions: Vec<MarketResolutionRow>,
    linkages: Vec<MarketLinkage>,
}

fn page_market_facts(
    source: &FrozenSourceSlice,
    scope: &ReplayPageScope<'_>,
    replay_starts: &HashMap<TokenId, i64>,
) -> MarketFactPage {
    let microstructure = scope
        .request
        .token_ids
        .iter()
        .flat_map(|token| source.prefetched.micro.get(token).into_iter().flatten())
        .filter(|row| {
            row.bucket_time >= scope.start_ms
                && row.bucket_time < scope.end_ms
                && row.available_at <= scope.available_ms
        })
        .cloned()
        .collect();
    let trade_tape = scope
        .request
        .market_ids
        .iter()
        .flat_map(|market| {
            source
                .prefetched
                .trade_tape
                .get(market)
                .into_iter()
                .flatten()
        })
        .filter(|row| {
            scope.tokens.contains(&row.token_id)
                && row.event_time
                    >= replay_starts
                        .get(&row.token_id)
                        .copied()
                        .unwrap_or(scope.start_ms)
                && row.event_time < scope.end_ms
                && row.ingestion_time <= scope.available_ms
        })
        .cloned()
        .collect();
    let resolutions = scope
        .request
        .market_ids
        .iter()
        .flat_map(|market| {
            source
                .prefetched
                .resolutions
                .get(market)
                .into_iter()
                .flatten()
        })
        .filter(|row| row.resolved_at < scope.end_ms && row.observed_at <= scope.available_ms)
        .cloned()
        .collect();
    let linkages = scope
        .request
        .market_ids
        .iter()
        .flat_map(|market| source.prefetched.linkages.get(market).into_iter().flatten())
        .filter(|row| {
            row.effective_at < scope.request.window_end
                && row.available_at <= scope.request.available_by
        })
        .cloned()
        .collect();
    MarketFactPage {
        microstructure,
        trade_tape,
        resolutions,
        linkages,
    }
}

struct DomainFactPage {
    observations: Vec<DomainObservation>,
    crypto_reports: Vec<CryptoPriceReport>,
    weather_observations: Vec<WeatherObservationFact>,
    weather_forecasts: Vec<WeatherForecastPoint>,
}

fn page_domain_facts(
    source: &FrozenSourceSlice,
    scope: &ReplayPageScope<'_>,
    linkages: &[MarketLinkage],
) -> QuantResult<DomainFactPage> {
    let (instrument_keys, weather_history) = page_domain_bindings(linkages)?;
    let observations = instrument_keys
        .iter()
        .flat_map(|key| {
            source
                .prefetched
                .domain_observations
                .get(key)
                .into_iter()
                .flatten()
        })
        .filter(|row| {
            row.observed_at >= scope.request.window_start
                && row.observed_at < scope.request.window_end
                && row
                    .available_at
                    .is_some_and(|at| at <= scope.request.available_by)
        })
        .cloned()
        .collect();
    let crypto_reports = instrument_keys
        .iter()
        .flat_map(|key| {
            source
                .prefetched
                .crypto_reports
                .get(key)
                .into_iter()
                .flatten()
        })
        .filter(|row| {
            row.event_time >= scope.request.window_start
                && row.event_time < scope.request.window_end
                && row.available_at <= scope.request.available_by
        })
        .cloned()
        .collect();
    let weather_observations = weather_history
        .keys()
        .flat_map(|subject_key| {
            source
                .prefetched
                .weather_observations
                .get(subject_key)
                .into_iter()
                .flatten()
        })
        .filter(|row| {
            row.observed_at
                >= weather_history
                    .get(&row.subject_key)
                    .copied()
                    .unwrap_or(scope.request.window_start)
                && row.observed_at < scope.request.window_end
                && row.available_at <= scope.request.available_by
        })
        .cloned()
        .collect();
    let weather_forecasts = weather_history
        .keys()
        .flat_map(|subject_key| {
            source
                .prefetched
                .weather_forecasts
                .get(subject_key)
                .into_iter()
                .flatten()
        })
        .filter(|row| {
            row.valid_time >= scope.request.window_start
                && row.valid_time < scope.request.window_end
                && row.available_at <= scope.request.available_by
        })
        .cloned()
        .collect();
    Ok(DomainFactPage {
        observations,
        crypto_reports,
        weather_observations,
        weather_forecasts,
    })
}

type PageDomainBindings = (
    BTreeSet<DomainInstrumentKey>,
    HashMap<String, DateTime<Utc>>,
);

fn page_domain_bindings(linkages: &[MarketLinkage]) -> QuantResult<PageDomainBindings> {
    let mut instrument_keys = BTreeSet::new();
    let mut weather_history = HashMap::new();
    for linkage in linkages {
        let LinkageOutcome::Resolved(binding) = &linkage.outcome else {
            continue;
        };
        instrument_keys.extend(
            binding
                .source_bindings
                .iter()
                .map(|source| source.instrument_key.clone()),
        );
        if let Some(subject_key) = binding.subject.weather_subject_key() {
            let history_start = weather_history_start(&binding.subject)?;
            weather_history
                .entry(subject_key)
                .and_modify(|current: &mut DateTime<Utc>| {
                    *current = (*current).min(history_start);
                })
                .or_insert(history_start);
        }
    }
    Ok((instrument_keys, weather_history))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use chrono::{TimeZone, Utc};
    use quant_pivot_models::{
        clickhouse::{BookL2LedgerRow, ChDigest},
        domain::market::CatalogWindowInfo,
        enums::clickhouse::ChCanonicalBookEventType,
        types::{
            ArtifactUri, CapabilityRegistryHashes, CatalogSyncBatchId, ContentHash,
            DATASET_ARTIFACT_FORMAT_VERSION, DecisionPolicySnapshotId, MarketId,
            ReaderContractVersion, ResearchEvaluationTrack, SOURCE_SLICE_MANIFEST_FORMAT_VERSION,
            SchemaContractVersion, SourceSliceCatalogProof, SourceSliceInvalidSession,
            SourceSliceManifest, SourceSliceObjectKind, SourceSliceObjectRef,
            SourceSlicePitCutoffs, SourceSliceSessionInvalidationReason, TokenId,
            builtin_research_profiles,
        },
    };
    use uuid::Uuid;

    use super::ReplayPageRequest;
    use crate::prefetch::{historical_window::Prefetched, source_slice::FrozenSourceSlice};

    fn hash(byte: char) -> ContentHash {
        ContentHash::parse(&format!("blake3:{}", byte.to_string().repeat(64))).expect("hash")
    }

    fn manifest(invalid_sessions: Vec<SourceSliceInvalidSession>) -> SourceSliceManifest {
        let window_start = Utc.timestamp_opt(100, 0).single().expect("source start");
        let window_end = Utc.timestamp_opt(120, 0).single().expect("source end");
        let pit_cutoff = Utc.timestamp_opt(130, 0).single().expect("source cutoff");
        let materialized_at = Utc.timestamp_opt(140, 0).single().expect("materialized at");
        SourceSliceManifest {
            format_version: SOURCE_SLICE_MANIFEST_FORMAT_VERSION,
            profile_ref: builtin_research_profiles()
                .expect("built-in profiles")
                .remove(0)
                .profile_ref,
            evaluation_track: ResearchEvaluationTrack::ResearchOnly,
            research_program_hash: hash('1'),
            window_start,
            window_end,
            pit_cutoff,
            materialized_at,
            catalog_proof: SourceSliceCatalogProof {
                base_complete_batch_id: CatalogSyncBatchId::new(Uuid::from_u128(1)),
                terminal_batch_id: CatalogSyncBatchId::new(Uuid::from_u128(2)),
                committed_through: pit_cutoff,
                ordered_batch_chain_hash: hash('2'),
                market_count: 1,
                event_count: 1,
                snapshot_hash: hash('3'),
            },
            reader_contract_version: ReaderContractVersion::v1(),
            schema_contract_version: SchemaContractVersion::v1(),
            decision_policy_snapshot_id: DecisionPolicySnapshotId::from_v7(),
            runtime_config_hash: hash('4'),
            dataset_format_version: DATASET_ARTIFACT_FORMAT_VERSION,
            capability_registry_hashes: CapabilityRegistryHashes::try_new(vec![hash('5')])
                .expect("canonical capabilities"),
            pit_cutoffs: SourceSlicePitCutoffs {
                catalog_available_at: pit_cutoff,
                clob_market_info_available_at: pit_cutoff,
                l2_available_at: pit_cutoff,
                trade_tape_available_at: pit_cutoff,
                weather_available_at: None,
                calibration_available_at: None,
                resolution_available_at: pit_cutoff,
            },
            invalid_sessions,
            objects: vec![SourceSliceObjectRef {
                kind: SourceSliceObjectKind::L2Ledger,
                uri: ArtifactUri::parse("s3://fixture/source/l2.parquet")
                    .expect("source object URI"),
                object_version: "fixture-v1".to_owned(),
                byte_hash: hash('6'),
                schema_hash: hash('7'),
                row_count: 3,
                min_event_at: Some(window_start),
                max_event_at: Some(window_end),
                min_available_at: Some(window_start),
                max_available_at: Some(pit_cutoff),
            }],
        }
    }

    fn event(token_id: &TokenId, session: Uuid, sequence: u64, at_ms: i64) -> BookL2LedgerRow {
        BookL2LedgerRow {
            stream_session_id: session,
            shard_id: 0,
            token_id: token_id.clone(),
            market_id: Some(MarketId::new("market")),
            token_sequence: sequence,
            event_type: if sequence == 1 {
                ChCanonicalBookEventType::Snapshot
            } else {
                ChCanonicalBookEventType::Delta
            },
            bid_prices: Vec::new(),
            bid_sizes: Vec::new(),
            ask_prices: Vec::new(),
            ask_sizes: Vec::new(),
            old_tick_size: None,
            new_tick_size: None,
            trade_price: None,
            trade_side: None,
            trade_size: None,
            fee_rate_bps: None,
            venue_event_time: at_ms,
            ingress_time: at_ms + 1,
            persisted_time: at_ms + 2,
            event_hash: ChDigest::from(hash(
                char::from_digit(u32::try_from(sequence).expect("sequence"), 16).expect("hex"),
            )),
            schema_version: BookL2LedgerRow::SCHEMA_VERSION,
        }
    }

    #[test]
    fn replay_page_keeps_gap() {
        let token_id = TokenId::new("token");
        let market_id = MarketId::new("market");
        let session = Uuid::now_v7();
        let snapshot = BookL2LedgerRow {
            stream_session_id: session,
            shard_id: 0,
            token_id: token_id.clone(),
            market_id: Some(market_id.clone()),
            token_sequence: 1,
            event_type: ChCanonicalBookEventType::Snapshot,
            bid_prices: Vec::new(),
            bid_sizes: Vec::new(),
            ask_prices: Vec::new(),
            ask_sizes: Vec::new(),
            old_tick_size: None,
            new_tick_size: None,
            trade_price: None,
            trade_side: None,
            trade_size: None,
            fee_rate_bps: None,
            venue_event_time: 100_000,
            ingress_time: 100_001,
            persisted_time: 100_002,
            event_hash: ChDigest::from(hash('a')),
            schema_version: BookL2LedgerRow::SCHEMA_VERSION,
        };
        let prefetched = Prefetched {
            books: HashMap::from([(token_id.clone(), vec![snapshot])]),
            micro: HashMap::new(),
            trade_tape: HashMap::new(),
            resolutions: HashMap::new(),
            catalog: CatalogWindowInfo {
                market_changes: Vec::new(),
                event_changes: Vec::new(),
            },
            domain_observations: HashMap::new(),
            crypto_reports: HashMap::new(),
            weather_observations: HashMap::new(),
            weather_forecasts: HashMap::new(),
            weather_calibrations: Vec::new(),
            linkages: HashMap::new(),
        };
        let invalid_sessions = vec![SourceSliceInvalidSession {
            token_id: token_id.to_string(),
            session_id: session.to_string(),
            invalidated_at: Utc.timestamp_opt(109, 0).single().expect("gap time"),
            first_failure_sequence: Some(3),
            reason: SourceSliceSessionInvalidationReason::SequenceGap,
            diagnostic_hash: hash('b'),
        }];
        let source = FrozenSourceSlice {
            manifest: manifest(invalid_sessions.clone()),
            window_start: Utc.timestamp_opt(100, 0).single().expect("source start"),
            window_end: Utc.timestamp_opt(120, 0).single().expect("source end"),
            pit_cutoff: Utc.timestamp_opt(130, 0).single().expect("source cutoff"),
            prefetched,
            clob_market_info: Vec::new(),
            l2_ledger: vec![
                event(&token_id, session, 1, 100_000),
                event(&token_id, session, 2, 108_000),
                event(&token_id, session, 3, 115_000),
            ],
            sessions: Vec::new(),
            invalid_sessions,
        };
        let page = source
            .replay_page(&ReplayPageRequest {
                market_ids: vec![market_id],
                token_ids: vec![token_id],
                window_start: Utc.timestamp_opt(110, 0).single().expect("start"),
                window_end: Utc.timestamp_opt(120, 0).single().expect("end"),
                available_by: Utc.timestamp_opt(130, 0).single().expect("cutoff"),
            })
            .expect("replay page");

        assert_eq!(page.snapshots.len(), 1);
        assert_eq!(page.l2_ledger.len(), 3);
        assert_eq!(page.l2_ledger[1].venue_event_time, 108_000);
        assert_eq!(page.gaps.len(), 1);
    }
}
