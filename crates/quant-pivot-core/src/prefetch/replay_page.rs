//! Bounded, zero-I/O replay pages over one independently verified Source Slice.

use std::{
    cmp::Ordering,
    collections::{BTreeSet, HashMap, HashSet},
};

use chrono::{DateTime, Utc};
use quant_pivot_error::{QuantResult, research::ResearchError};
use quant_pivot_models::{
    clickhouse::{
        BookL2CheckpointRow, BookL2EventRow, BookMicrostructureRow, BookStreamSessionRow,
        MarketResolutionRow, TradeTapeRow,
    },
    domain::{
        CryptoPriceReport, DecisionBoundary, DecisionSource, DomainObservation,
        EventCatalogVersionInfo, LinkageOutcome, MarketCatalogVersionInfo, MarketLinkage,
        MarketSubject, WeatherForecastPoint, WeatherObservationFact,
    },
    types::{
        ClobMarketInfoVersion, DomainInstrumentKey, IcaoStation, MarketId,
        SourceSliceInvalidSession, TokenId,
    },
};
use quant_pivot_research::pit::BookSnapshotAt;
use uuid::Uuid;

use crate::pit::platform::ch_historical::{reconstruct_checkpoint, reconstruct_checkpoint_series};

use super::source_slice::FrozenSourceSlice;

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
    pub catalog_markets: Vec<MarketCatalogVersionInfo>,
    pub catalog_events: Vec<EventCatalogVersionInfo>,
    pub clob_market_info: Vec<ClobMarketInfoVersion>,
    pub checkpoints: Vec<BookL2CheckpointRow>,
    pub sessions: Vec<BookStreamSessionRow>,
    pub gaps: Vec<SourceSliceInvalidSession>,
    pub l2_events: Vec<BookL2EventRow>,
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
    /// Reconstruct the exact snapshot visible at one decision boundary without
    /// touching a repository. The page must carry the pre-window checkpoint,
    /// all intervening canonical events/trades, and its session ledger.
    pub fn book_at_boundary(
        &self,
        token_id: &TokenId,
        boundary: &DecisionBoundary,
    ) -> QuantResult<Option<BookSnapshotAt>> {
        let source_cutoff = boundary.cutoff_for(DecisionSource::Book);
        let decision_at = boundary.decision_at();
        let checkpoint = self.checkpoint_at(token_id, boundary).cloned();
        let Some(checkpoint) = checkpoint else {
            return Ok(None);
        };
        self.validate_session(token_id, checkpoint.stream_session_id, decision_at)?;
        let session = self
            .session_at(checkpoint.stream_session_id, decision_at)
            .ok_or_else(|| ResearchError::PitResolution {
                detail: format!(
                    "Source Slice session ledger {} for token {} is unavailable",
                    checkpoint.stream_session_id, token_id
                ),
            })?;
        let mut events = self
            .l2_events
            .iter()
            .filter(|row| {
                &row.token_id == token_id
                    && row.stream_session_id == checkpoint.stream_session_id
                    && row.token_sequence >= checkpoint.token_sequence
                    && row.venue_event_time <= source_cutoff.timestamp_millis()
                    && row.persisted_time <= decision_at.timestamp_millis()
            })
            .cloned()
            .collect::<Vec<_>>();
        events.sort_by_key(|row| row.token_sequence);
        let mut trades = self
            .trade_tape
            .iter()
            .filter(|row| {
                &row.token_id == token_id
                    && row.stream_session_id == Some(checkpoint.stream_session_id)
                    && row
                        .token_sequence
                        .is_some_and(|sequence| sequence >= checkpoint.token_sequence)
                    && row.event_time <= source_cutoff.timestamp_millis()
                    && row.ingestion_time <= decision_at.timestamp_millis()
            })
            .cloned()
            .collect::<Vec<_>>();
        trades.sort_by_key(|row| row.token_sequence);
        reconstruct_checkpoint(
            checkpoint,
            session,
            &events,
            &trades,
            source_cutoff,
            decision_at,
        )
    }

    /// Reconstruct a strictly monotonic timeline with one event-sequence walk
    /// per stream session. Periodic checkpoints do not restart replay; a later
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
        let selected = self.checkpoints_at_boundaries(token_id, boundaries);
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
                && selected[end]
                    .is_some_and(|checkpoint| checkpoint.stream_session_id == session_id)
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
                .l2_events
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
            let mut trades = self
                .trade_tape
                .iter()
                .filter(|row| {
                    &row.token_id == token_id
                        && row.stream_session_id == Some(session_id)
                        && row
                            .token_sequence
                            .is_some_and(|sequence| sequence >= anchor.token_sequence)
                        && row.event_time <= final_cutoff.timestamp_millis()
                        && row.ingestion_time <= final_boundary.decision_at().timestamp_millis()
                })
                .cloned()
                .collect::<Vec<_>>();
            trades.sort_by_key(|row| row.token_sequence);
            let snapshots = reconstruct_checkpoint_series(
                anchor.clone(),
                session,
                &events,
                &trades,
                &boundaries[start..end],
            )?;
            for (slot, snapshot) in books[start..end].iter_mut().zip(snapshots) {
                *slot = Some(snapshot);
            }
            start = end;
        }
        Ok(books)
    }

    fn checkpoint_at(
        &self,
        token_id: &TokenId,
        boundary: &DecisionBoundary,
    ) -> Option<&BookL2CheckpointRow> {
        let source_cutoff = boundary.cutoff_for(DecisionSource::Book);
        let decision_at = boundary.decision_at();
        self.checkpoints
            .iter()
            .filter(|row| {
                &row.token_id == token_id
                    && row.event_time <= source_cutoff.timestamp_millis()
                    && row.created_at <= decision_at.timestamp_millis()
            })
            .max_by(|left, right| checkpoint_order(left, right))
    }

    fn checkpoints_at_boundaries<'a>(
        &'a self,
        token_id: &TokenId,
        boundaries: &[DecisionBoundary],
    ) -> Vec<Option<&'a BookL2CheckpointRow>> {
        let mut arrivals = vec![Vec::<&BookL2CheckpointRow>::new(); boundaries.len()];
        for checkpoint in self
            .checkpoints
            .iter()
            .filter(|row| &row.token_id == token_id)
        {
            let mut low = 0_usize;
            let mut high = boundaries.len();
            while low < high {
                let middle = low + (high - low) / 2;
                let boundary = &boundaries[middle];
                let eligible = checkpoint.event_time
                    <= boundary.cutoff_for(DecisionSource::Book).timestamp_millis()
                    && checkpoint.created_at <= boundary.decision_at().timestamp_millis();
                if eligible {
                    high = middle;
                } else {
                    low = middle + 1;
                }
            }
            if let Some(bucket) = arrivals.get_mut(low) {
                bucket.push(checkpoint);
            }
        }
        let mut latest = None;
        arrivals
            .into_iter()
            .map(|arrived| {
                for checkpoint in arrived {
                    if latest.is_none_or(|current| checkpoint_order(current, checkpoint).is_lt()) {
                        latest = Some(checkpoint);
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

fn checkpoint_order(left: &BookL2CheckpointRow, right: &BookL2CheckpointRow) -> Ordering {
    (left.event_time, left.token_sequence, &left.checkpoint_hash).cmp(&(
        right.event_time,
        right.token_sequence,
        &right.checkpoint_hash,
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
        let domain = page_domain_facts(self, &scope, &market_facts.linkages);
        Ok(ReplayPage {
            market_ids: request.market_ids.clone(),
            token_ids: request.token_ids.clone(),
            catalog_markets: catalog.markets,
            catalog_events: catalog.events,
            clob_market_info: catalog.clob_market_info,
            checkpoints: books.checkpoints,
            sessions: books.sessions,
            gaps: books.gaps,
            l2_events: books.l2_events,
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
    markets: Vec<MarketCatalogVersionInfo>,
    events: Vec<EventCatalogVersionInfo>,
    clob_market_info: Vec<ClobMarketInfoVersion>,
}

fn page_catalog(source: &FrozenSourceSlice, scope: &ReplayPageScope<'_>) -> CatalogPage {
    let markets = source
        .prefetched
        .catalog
        .market_versions
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
        .event_versions
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
    checkpoints: Vec<BookL2CheckpointRow>,
    sessions: Vec<BookStreamSessionRow>,
    gaps: Vec<SourceSliceInvalidSession>,
    l2_events: Vec<BookL2EventRow>,
}

fn page_books(source: &FrozenSourceSlice, scope: &ReplayPageScope<'_>) -> BookPage {
    let mut replay_start_by_token = HashMap::<TokenId, i64>::new();
    let mut checkpoints = Vec::new();
    for token in &scope.request.token_ids {
        let Some(rows) = source.prefetched.books.get(token) else {
            replay_start_by_token.insert(token.clone(), scope.start_ms);
            continue;
        };
        let anchor = rows
            .iter()
            .filter(|row| row.event_time <= scope.start_ms && row.created_at <= scope.available_ms)
            .max_by(|left, right| {
                (left.event_time, left.token_sequence, &left.checkpoint_hash).cmp(&(
                    right.event_time,
                    right.token_sequence,
                    &right.checkpoint_hash,
                ))
            });
        replay_start_by_token.insert(
            token.clone(),
            anchor.map_or(scope.start_ms, |row| row.event_time),
        );
        if let Some(anchor) = anchor {
            checkpoints.push(anchor.clone());
        }
        checkpoints.extend(
            rows.iter()
                .filter(|row| {
                    row.event_time > scope.start_ms
                        && row.event_time < scope.end_ms
                        && row.created_at <= scope.available_ms
                })
                .cloned(),
        );
    }
    checkpoints.sort_by(|left, right| {
        (
            &left.token_id,
            left.event_time,
            left.token_sequence,
            &left.checkpoint_hash,
        )
            .cmp(&(
                &right.token_id,
                right.event_time,
                right.token_sequence,
                &right.checkpoint_hash,
            ))
    });
    let l2_events = source
        .l2_events
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
    let session_ids = checkpoints
        .iter()
        .map(|row| row.stream_session_id)
        .chain(l2_events.iter().map(|row| row.stream_session_id))
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
        checkpoints,
        sessions,
        gaps,
        l2_events,
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
) -> DomainFactPage {
    let (instrument_keys, stations) = page_domain_bindings(linkages);
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
    let weather_observations = stations
        .iter()
        .flat_map(|station| {
            source
                .prefetched
                .weather_observations
                .get(station)
                .into_iter()
                .flatten()
        })
        .filter(|row| {
            row.observation_time >= scope.request.window_start
                && row.observation_time < scope.request.window_end
                && row.available_at <= scope.request.available_by
        })
        .cloned()
        .collect();
    let weather_forecasts = stations
        .iter()
        .flat_map(|station| {
            source
                .prefetched
                .weather_forecasts
                .get(station)
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
    DomainFactPage {
        observations,
        crypto_reports,
        weather_observations,
        weather_forecasts,
    }
}

fn page_domain_bindings(
    linkages: &[MarketLinkage],
) -> (BTreeSet<DomainInstrumentKey>, BTreeSet<IcaoStation>) {
    let mut instrument_keys = BTreeSet::new();
    let mut stations = BTreeSet::new();
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
        if let MarketSubject::Weather(subject) = &binding.subject {
            stations.insert(subject.station.clone());
        }
    }
    (instrument_keys, stations)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use chrono::{TimeZone, Utc};
    use quant_pivot_models::{
        clickhouse::{BookL2CheckpointRow, BookL2EventRow, ChSchemaVersion},
        domain::CatalogWindowInfo,
        enums::clickhouse::ChCanonicalBookEventType,
        types::{
            ContentHash, MarketId, SourceSliceInvalidSession, SourceSliceSessionInvalidationReason,
            TokenId,
        },
    };
    use uuid::Uuid;

    use crate::prefetch::{historical_window::Prefetched, source_slice::FrozenSourceSlice};

    use super::ReplayPageRequest;

    fn hash(byte: char) -> ContentHash {
        ContentHash::parse(format!("blake3:{}", byte.to_string().repeat(64))).expect("hash")
    }

    fn event(token_id: &TokenId, session: Uuid, sequence: u64, at_ms: i64) -> BookL2EventRow {
        BookL2EventRow {
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
            book_version: sequence,
            old_tick_size: None,
            new_tick_size: None,
            venue_event_time: at_ms,
            ingress_time: at_ms + 1,
            persisted_time: at_ms + 2,
            payload_hash: hash(
                char::from_digit(u32::try_from(sequence).expect("sequence"), 16).expect("hex"),
            ),
            schema_version: ChSchemaVersion(2),
        }
    }

    #[test]
    fn replay_page_keeps_pre_window_anchor_deltas_and_gap() {
        let token_id = TokenId::new("token");
        let market_id = MarketId::new("market");
        let session = Uuid::now_v7();
        let checkpoint = BookL2CheckpointRow {
            token_id: token_id.clone(),
            market_id: Some(market_id.clone()),
            stream_session_id: session,
            token_sequence: 1,
            bids_json: "[]".to_owned(),
            asks_json: "[]".to_owned(),
            book_version: 1,
            source_event_hash: hash('1'),
            checkpoint_hash: hash('a'),
            event_time: 100_000,
            created_at: 100_002,
            schema_version: ChSchemaVersion(2),
        };
        let prefetched = Prefetched {
            books: HashMap::from([(token_id.clone(), vec![checkpoint])]),
            micro: HashMap::new(),
            trade_tape: HashMap::new(),
            resolutions: HashMap::new(),
            catalog: CatalogWindowInfo {
                market_versions: Vec::new(),
                event_versions: Vec::new(),
            },
            domain_observations: HashMap::new(),
            crypto_reports: HashMap::new(),
            weather_observations: HashMap::new(),
            weather_forecasts: HashMap::new(),
            weather_calibrations: Vec::new(),
            linkages: HashMap::new(),
        };
        let source = FrozenSourceSlice {
            prefetched,
            clob_market_info: Vec::new(),
            l2_events: vec![
                event(&token_id, session, 1, 100_000),
                event(&token_id, session, 2, 108_000),
                event(&token_id, session, 3, 115_000),
            ],
            sessions: Vec::new(),
            invalid_sessions: vec![SourceSliceInvalidSession {
                token_id: token_id.to_string(),
                session_id: session.to_string(),
                invalidated_at: Utc.timestamp_opt(109, 0).single().expect("gap time"),
                first_failure_sequence: Some(3),
                reason: SourceSliceSessionInvalidationReason::SequenceGap,
                diagnostic_hash: hash('b'),
            }],
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

        assert_eq!(page.checkpoints.len(), 1);
        assert_eq!(page.l2_events.len(), 3);
        assert_eq!(page.l2_events[1].venue_event_time, 108_000);
        assert_eq!(page.gaps.len(), 1);
    }
}
