//! Ordered durable handoff from Crypto source streams to `ClickHouse` and cursors.

use std::{cmp::Ordering, collections::VecDeque, sync::Arc, time::Duration};

use async_trait::async_trait;
use quant_pivot_error::{QuantError, QuantResult, storage::StorageError};
use quant_pivot_models::{
    clickhouse::CryptoPriceReportRow,
    config::{CLICKHOUSE_DURABLE_ACK_TIMEOUT_MS, CLICKHOUSE_DURABLE_SCHEDULING_MARGIN_MS},
    domain::data_plane::CryptoPriceReport,
    types::{DomainInstrumentKey, DomainSourceId},
};
use quant_pivot_repository::traits::DomainProjectionRepository;
use quant_pivot_storage::write::{
    DurableWriteAcknowledgement, DurableWriteError, DurableWriteReceipt, DurableWriteTimeouts,
    DurableWriter,
};

use super::domain_source_supervisor::DomainSourceSupervisor;

#[async_trait]
pub(super) trait CryptoRecoveryPort: Send + Sync {
    async fn mark_recovered(
        &self,
        source_id: &DomainSourceId,
        instrument_key: &DomainInstrumentKey,
    ) -> QuantResult<()>;
}

#[async_trait]
impl CryptoRecoveryPort for DomainSourceSupervisor {
    async fn mark_recovered(
        &self,
        source_id: &DomainSourceId,
        instrument_key: &DomainInstrumentKey,
    ) -> QuantResult<()> {
        self.mark_source_recovered(source_id, instrument_key).await
    }
}

pub(super) struct CryptoFactPersistence {
    source_supervisor: Arc<dyn CryptoRecoveryPort>,
    projections: Arc<dyn DomainProjectionRepository>,
    writer: Arc<DurableWriter<CryptoPriceReportRow>>,
    timeouts: DurableWriteTimeouts,
}

pub(super) struct PendingCryptoFact {
    report: CryptoPriceReport,
    gap_generation: u64,
    receipt: DurableWriteReceipt,
}

impl CryptoFactPersistence {
    pub(super) fn new<R>(
        source_supervisor: Arc<R>,
        projections: Arc<dyn DomainProjectionRepository>,
        writer: Arc<DurableWriter<CryptoPriceReportRow>>,
        timeouts: DurableWriteTimeouts,
    ) -> Self
    where
        R: CryptoRecoveryPort + 'static,
    {
        Self {
            source_supervisor,
            projections,
            writer,
            timeouts,
        }
    }

    /// Admit a live report while concurrently consuming any earlier source ACK.
    /// This prevents a full byte budget from deadlocking behind receipts whose
    /// `ClickHouse` batch has already completed.
    pub(super) async fn enqueue_ordered(
        &self,
        report: CryptoPriceReport,
        gap_generation: u64,
        pending: &mut VecDeque<PendingCryptoFact>,
    ) -> QuantResult<()> {
        let row = CryptoPriceReportRow::from_report(&report, gap_generation)
            .map_err(|error| QuantError::config(error.to_string()))?;
        let admission = self.writer.enqueue_batch(vec![row], self.timeouts);
        tokio::pin!(admission);
        loop {
            if pending.is_empty() {
                let receipt = admission.await.map_err(|error| self.map_error(error))?;
                pending.push_back(PendingCryptoFact {
                    report,
                    gap_generation,
                    receipt,
                });
                return Ok(());
            }
            let acknowledgement = {
                let front = pending.front_mut().ok_or_else(|| {
                    QuantError::Storage(StorageError::invariant_violation(
                        Some("quant_crypto_price_report"),
                        "pending Crypto receipt disappeared before ordered acknowledgement",
                    ))
                })?;
                tokio::select! {
                    biased;
                    acknowledgement = front.receipt.acknowledge() => acknowledgement,
                    receipt = &mut admission => {
                        let receipt = receipt.map_err(|error| self.map_error(error))?;
                        pending.push_back(PendingCryptoFact {
                            report,
                            gap_generation,
                            receipt,
                        });
                        return Ok(());
                    }
                }
            };
            let acknowledgement = acknowledgement.map_err(|error| self.map_error(error))?;
            self.commit_front(pending, &acknowledgement).await?;
            drop(acknowledgement);
        }
    }

    async fn commit_front(
        &self,
        pending: &mut VecDeque<PendingCryptoFact>,
        _acknowledgement: &DurableWriteAcknowledgement,
    ) -> QuantResult<()> {
        let PendingCryptoFact {
            report,
            gap_generation,
            receipt,
        } = pending.pop_front().ok_or_else(|| {
            QuantError::Storage(StorageError::invariant_violation(
                Some("quant_crypto_price_report"),
                "durable Crypto acknowledgement has no pending source report",
            ))
        })?;
        drop(receipt);
        self.commit(report, gap_generation).await
    }

    pub(super) async fn acknowledge_front(
        &self,
        pending: &mut VecDeque<PendingCryptoFact>,
    ) -> QuantResult<()> {
        let acknowledgement = pending
            .front_mut()
            .ok_or_else(|| {
                QuantError::Storage(StorageError::invariant_violation(
                    Some("quant_crypto_price_report"),
                    "attempted to acknowledge an empty Crypto source queue",
                ))
            })?
            .receipt
            .acknowledge()
            .await
            .map_err(|error| self.map_error(error))?;
        let result = self.commit_front(pending, &acknowledgement).await;
        drop(acknowledgement);
        result
    }

    pub(super) async fn drain(&self, pending: &mut VecDeque<PendingCryptoFact>) -> QuantResult<()> {
        while !pending.is_empty() {
            self.acknowledge_front(pending).await?;
        }
        Ok(())
    }

    /// Force all writes admitted before source shutdown through the shared
    /// FIFO writer, then commit their receipts in source order.
    pub(super) async fn shutdown(
        &self,
        pending: &mut VecDeque<PendingCryptoFact>,
    ) -> QuantResult<()> {
        let deadline = Duration::from_millis(
            CLICKHOUSE_DURABLE_ACK_TIMEOUT_MS + CLICKHOUSE_DURABLE_SCHEDULING_MARGIN_MS,
        );
        self.writer
            .flush(deadline)
            .await
            .map_err(|error| Self::map_flush_error(error, deadline))?;
        self.drain(pending).await
    }

    pub(super) async fn persist_batch(
        &self,
        reports: Vec<CryptoPriceReport>,
        gap_generation: u64,
    ) -> QuantResult<()> {
        validate_batch(&reports)?;
        if reports.is_empty() {
            return Ok(());
        }
        let rows = reports
            .iter()
            .map(|report| {
                CryptoPriceReportRow::from_report(report, gap_generation)
                    .map_err(|error| QuantError::config(error.to_string()))
            })
            .collect::<QuantResult<Vec<_>>>()?;
        let mut receipt = self
            .writer
            .enqueue_batch(rows, self.timeouts)
            .await
            .map_err(|error| self.map_error(error))?;
        let acknowledgement = receipt
            .acknowledge()
            .await
            .map_err(|error| self.map_error(error))?;
        for report in reports {
            self.commit(report, gap_generation).await?;
        }
        drop(acknowledgement);
        Ok(())
    }

    async fn commit(&self, report: CryptoPriceReport, gap_generation: u64) -> QuantResult<()> {
        let source_id = report.source_id.clone();
        let instrument_key = report.instrument_key.clone();
        let checkpoint = report
            .checkpoint()
            .map_err(|error| QuantError::config(error.to_string()))?;
        self.projections
            .apply_crypto_report(report, checkpoint, gap_generation, true)
            .await?;
        self.source_supervisor
            .mark_recovered(&source_id, &instrument_key)
            .await?;
        Ok(())
    }

    fn map_error(&self, error: DurableWriteError) -> QuantError {
        let storage = match error {
            DurableWriteError::QueueTimeout => StorageError::ClickHouseTimeout {
                operation: "quant_crypto_price_report.enqueue",
                duration: self.timeouts.enqueue(),
            },
            DurableWriteError::AcknowledgementTimeout => StorageError::ClickHouseTimeout {
                operation: "quant_crypto_price_report.acknowledgement",
                duration: self.timeouts.acknowledgement(),
            },
            DurableWriteError::QueueClosed => {
                StorageError::ChannelClosed("quant_crypto_price_report".to_owned())
            }
            DurableWriteError::CapacityExceeded => StorageError::CapacityExceeded {
                entity: "quant_crypto_price_report",
                limit: u64::try_from(self.writer.item_limit()).unwrap_or(u64::MAX),
            },
            DurableWriteError::PayloadTooLarge => StorageError::CapacityExceeded {
                entity: "quant_crypto_price_report",
                limit: u64::try_from(self.writer.byte_limit().unwrap_or_default())
                    .unwrap_or(u64::MAX),
            },
            DurableWriteError::PersistenceFailed => StorageError::Connection(
                "quant_crypto_price_report durable batch persistence failed".to_owned(),
            ),
            DurableWriteError::AlreadyAcknowledged => StorageError::invariant_violation(
                Some("quant_crypto_price_report"),
                "durable Crypto receipt was acknowledged more than once",
            ),
        };
        QuantError::Storage(storage)
    }

    fn map_flush_error(error: DurableWriteError, duration: Duration) -> QuantError {
        let storage = match error {
            DurableWriteError::QueueTimeout | DurableWriteError::AcknowledgementTimeout => {
                StorageError::ClickHouseTimeout {
                    operation: "quant_crypto_price_report.shutdown_flush",
                    duration,
                }
            }
            DurableWriteError::QueueClosed => {
                StorageError::ChannelClosed("quant_crypto_price_report".to_owned())
            }
            DurableWriteError::CapacityExceeded | DurableWriteError::PayloadTooLarge => {
                StorageError::invariant_violation(
                    Some("quant_crypto_price_report"),
                    "shutdown flush command unexpectedly carried a payload",
                )
            }
            DurableWriteError::PersistenceFailed => StorageError::Connection(
                "quant_crypto_price_report shutdown flush failed".to_owned(),
            ),
            DurableWriteError::AlreadyAcknowledged => StorageError::invariant_violation(
                Some("quant_crypto_price_report"),
                "shutdown flush acknowledgement was consumed twice",
            ),
        };
        QuantError::Storage(storage)
    }
}

fn validate_batch(reports: &[CryptoPriceReport]) -> QuantResult<()> {
    let Some(first) = reports.first() else {
        return Ok(());
    };
    first
        .checkpoint()
        .map_err(|error| QuantError::config(error.to_string()))?;
    for pair in reports.windows(2) {
        let previous = &pair[0];
        let current = &pair[1];
        if current.source_id != first.source_id || current.instrument_key != first.instrument_key {
            return Err(QuantError::config(
                "Crypto persistence batch mixed source or instrument identities",
            ));
        }
        let previous_checkpoint = previous
            .checkpoint()
            .map_err(|error| QuantError::config(error.to_string()))?;
        let current_checkpoint = current
            .checkpoint()
            .map_err(|error| QuantError::config(error.to_string()))?;
        match previous_checkpoint
            .compare_crypto(&current_checkpoint)
            .map_err(|error| QuantError::config(error.to_string()))?
        {
            Ordering::Less => {
                return Err(QuantError::config(
                    "Crypto persistence batch regressed in source-native order",
                ));
            }
            Ordering::Equal if previous.report_hash != current.report_hash => {
                return Err(QuantError::config(
                    "Crypto source equivocated within one persistence batch",
                ));
            }
            Ordering::Equal => {
                return Err(QuantError::config(
                    "Crypto persistence batch contains a duplicate source checkpoint",
                ));
            }
            Ordering::Greater => {}
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{
        collections::VecDeque,
        sync::{Arc, Mutex},
        time::Duration,
    };

    use async_trait::async_trait;
    use chrono::{DateTime, NaiveDate, Utc};
    use quant_pivot_error::{QuantResult, storage::StorageError};
    use quant_pivot_models::{
        clickhouse::CryptoPriceReportRow,
        domain::{
            data_plane::{
                CryptoPriceReport, DomainEventEnvelope, DomainSourceCheckpoint,
                WeatherObservationReport,
            },
            quant::{CryptoPriceProjectionInfo, WeatherDailyTemperatureProjectionInfo},
        },
        hashing::CanonicalDigest,
        types::{
            BinanceSymbol, ContentHash, DomainEventId, DomainInstrumentKey, DomainSourceId,
            IcaoStation, Usd, WorkerId,
        },
    };
    use quant_pivot_repository::traits::DomainProjectionRepository;
    use quant_pivot_storage::write::{
        AsyncWriterObservability, DurableWriteTimeouts, DurableWriter, DurableWriterConfig,
    };
    use rust_decimal_macros::dec;
    use tokio::{sync::Notify, time::timeout};
    use tokio_util::sync::CancellationToken;

    use super::{CryptoFactPersistence, CryptoRecoveryPort, PendingCryptoFact, validate_batch};

    #[derive(Default)]
    struct RecordingProjection {
        sequences: Mutex<Vec<u64>>,
    }

    #[async_trait]
    impl DomainProjectionRepository for RecordingProjection {
        async fn apply_crypto_report(
            &self,
            report: CryptoPriceReport,
            checkpoint: DomainSourceCheckpoint,
            gap_generation: u64,
            source_healthy: bool,
        ) -> Result<CryptoPriceProjectionInfo, StorageError> {
            self.sequences
                .lock()
                .map_err(|_| StorageError::Connection("recording projection poisoned".to_owned()))?
                .push(report.source_sequence);
            Ok(CryptoPriceProjectionInfo {
                source_id: report.source_id,
                instrument_key: report.instrument_key,
                previous_price: None,
                current_price: report.price,
                source_sequence: report.source_sequence,
                event_time: report.event_time,
                available_at: report.available_at,
                report_hash: report.report_hash,
                gap_generation: i64::try_from(gap_generation).map_err(|error| {
                    StorageError::invariant_violation(
                        None,
                        format!("recording gap generation overflow: {error}"),
                    )
                })?,
                source_healthy,
                committed_checkpoint_hash: CanonicalDigest::content_hash_json(&checkpoint)
                    .map_err(|error| StorageError::invariant_violation(None, error.to_string()))?,
                committed_checkpoint: checkpoint,
            })
        }

        async fn apply_weather_report(
            &self,
            _report: WeatherObservationReport,
            _timezone: String,
            _local_date: NaiveDate,
            _checkpoint: DomainSourceCheckpoint,
            _gap_generation: u64,
            _source_healthy: bool,
        ) -> Result<Vec<WeatherDailyTemperatureProjectionInfo>, StorageError> {
            Err(unused_method("apply_weather_report"))
        }

        async fn close_weather_day(
            &self,
            _station: &IcaoStation,
            _local_date: NaiveDate,
            _closed_at: DateTime<Utc>,
        ) -> Result<Vec<WeatherDailyTemperatureProjectionInfo>, StorageError> {
            Err(unused_method("close_weather_day"))
        }

        async fn mark_crypto_source_gap(
            &self,
            _source_id: &DomainSourceId,
            _instrument_key: &DomainInstrumentKey,
            _observed_at: DateTime<Utc>,
        ) -> Result<u64, StorageError> {
            Err(unused_method("mark_crypto_source_gap"))
        }

        async fn mark_weather_source_gap(
            &self,
            _station: &IcaoStation,
            _local_date: NaiveDate,
            _observed_at: DateTime<Utc>,
        ) -> Result<u64, StorageError> {
            Err(unused_method("mark_weather_source_gap"))
        }

        async fn claim_pending_events(
            &self,
            _worker_id: WorkerId,
            _now: DateTime<Utc>,
            _lease_expires_at: DateTime<Utc>,
            _limit: u64,
        ) -> Result<Vec<DomainEventEnvelope>, StorageError> {
            Err(unused_method("claim_pending_events"))
        }

        async fn mark_event_published(
            &self,
            _event_id: &DomainEventId,
            _worker_id: WorkerId,
            _published_at: DateTime<Utc>,
        ) -> Result<(), StorageError> {
            Err(unused_method("mark_event_published"))
        }

        async fn mark_event_failed(
            &self,
            _event_id: &DomainEventId,
            _worker_id: WorkerId,
            _detail: String,
        ) -> Result<(), StorageError> {
            Err(unused_method("mark_event_failed"))
        }
    }

    struct RecordingRecovery;

    #[async_trait]
    impl CryptoRecoveryPort for RecordingRecovery {
        async fn mark_recovered(
            &self,
            _source_id: &DomainSourceId,
            _instrument_key: &DomainInstrumentKey,
        ) -> QuantResult<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn cursor_waits_for_ack() {
        let gate = Arc::new(Notify::new());
        let sink_gate = Arc::clone(&gate);
        let (writer, worker) = DurableWriter::new(
            DurableWriterConfig::new("crypto-cursor-test")
                .capacity(4)
                .max_batch_size(1)
                .max_batch_delay(Duration::from_millis(1)),
            move |_rows| {
                let sink_gate = Arc::clone(&sink_gate);
                Box::pin(async move {
                    sink_gate.notified().await;
                    Ok(())
                })
            },
            AsyncWriterObservability::default(),
        );
        let shutdown = CancellationToken::new();
        let worker_task = tokio::spawn(worker.run(shutdown.clone()));
        let projection = Arc::new(RecordingProjection::default());
        let persistence = CryptoFactPersistence::new(
            Arc::new(RecordingRecovery),
            Arc::clone(&projection) as Arc<dyn DomainProjectionRepository>,
            Arc::new(writer),
            DurableWriteTimeouts::new(Duration::from_millis(50), Duration::from_secs(1)),
        );
        let mut pending = VecDeque::<PendingCryptoFact>::new();
        persistence
            .enqueue_ordered(report(7), 3, &mut pending)
            .await
            .expect("admit Crypto report");

        assert!(
            timeout(
                Duration::from_millis(10),
                persistence.acknowledge_front(&mut pending),
            )
            .await
            .is_err()
        );
        assert!(projection.sequences.lock().expect("sequences").is_empty());
        gate.notify_one();
        persistence
            .acknowledge_front(&mut pending)
            .await
            .expect("commit acknowledged report");
        assert_eq!(*projection.sequences.lock().expect("sequences"), vec![7]);
        drop(persistence);
        shutdown.cancel();
        worker_task.await.expect("durable worker shutdown");
    }

    #[tokio::test]
    async fn failure_blocks_cursor() {
        let (writer, worker) = DurableWriter::new(
            DurableWriterConfig::new("crypto-failure-test")
                .capacity(2)
                .max_batch_size(1)
                .max_batch_delay(Duration::from_millis(1)),
            |_rows| {
                Box::pin(async {
                    Err(StorageError::Connection(
                        "injected Crypto fact failure".to_owned(),
                    ))
                })
            },
            AsyncWriterObservability::default(),
        );
        let shutdown = CancellationToken::new();
        let worker_task = tokio::spawn(worker.run(shutdown.clone()));
        let projection = Arc::new(RecordingProjection::default());
        let persistence = CryptoFactPersistence::new(
            Arc::new(RecordingRecovery),
            Arc::clone(&projection) as Arc<dyn DomainProjectionRepository>,
            Arc::new(writer),
            DurableWriteTimeouts::new(Duration::from_millis(50), Duration::from_secs(1)),
        );
        let mut pending = VecDeque::<PendingCryptoFact>::new();
        persistence
            .enqueue_ordered(report(8), 3, &mut pending)
            .await
            .expect("admit failing Crypto report");

        assert!(persistence.acknowledge_front(&mut pending).await.is_err());
        assert!(projection.sequences.lock().expect("sequences").is_empty());
        drop(persistence);
        shutdown.cancel();
        worker_task.await.expect("durable worker shutdown");
    }

    #[test]
    fn batch_equivocation_fails() {
        let first = report(9);
        let equivocation = CryptoPriceReport {
            report_hash: ContentHash::parse(&format!("blake3:{:064x}", 10)).expect("content hash"),
            ..first.clone()
        };
        assert!(validate_batch(&[first, equivocation]).is_err());
    }

    #[test]
    fn row_requires_generation() {
        let report = report(11);
        let row = CryptoPriceReportRow::from_report(&report, 7).expect("valid report");
        assert_eq!(row.gap_generation, 7);
    }

    fn report(sequence: u64) -> CryptoPriceReport {
        let symbol = BinanceSymbol::parse("BTCUSDT").expect("symbol");
        let now = Utc::now();
        CryptoPriceReport {
            source_id: DomainSourceId::binance_agg_trade(),
            instrument_key: DomainInstrumentKey::binance_agg_trade(&symbol),
            source_sequence: sequence,
            price: Usd::new(dec!(50000)),
            quantity: None,
            event_time: now,
            published_at: now,
            available_at: now,
            valid_from: None,
            observations_timestamp: None,
            expires_at: None,
            report_hash: ContentHash::parse(&format!("blake3:{sequence:064x}"))
                .expect("content hash"),
            raw_report: format!("report-{sequence}"),
        }
    }

    fn unused_method(name: &str) -> StorageError {
        StorageError::Connection(format!("unexpected mock method {name}"))
    }
}
