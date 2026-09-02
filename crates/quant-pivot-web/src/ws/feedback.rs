//! Durable `research.feedback` outbox publication.

use std::{sync::Arc, time::Duration};

use bytestring::ByteString;
use prometheus::IntCounterVec;
use quant_pivot_error::{
    QuantResult,
    feedback::FeedbackError,
    storage::{StorageError, entity::QUANT_FEEDBACK_EVENT_OUTBOX},
};
use quant_pivot_models::{
    config::ResearchJobsConfig,
    domain::{
        quant::FeedbackOutboxEntry,
        ws::{ResearchFeedbackEvent, SubscriptionKey, WsChannel, WsEnvelope},
    },
    types::WorkerId,
};
use quant_pivot_repository::traits::FeedbackOutboxRepository;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;

use super::{DeliveryClass, SessionRegistry};

const OUTBOX_BATCH_SIZE: u64 = 128;

/// Publishes durable feedback revisions through the bounded session hub.
pub struct FeedbackOutboxWorker {
    worker_id: WorkerId,
    outbox: Arc<dyn FeedbackOutboxRepository>,
    registry: SessionRegistry,
    lease_secs: u64,
    poll_interval: Duration,
    recovery_total: IntCounterVec,
}

pub(super) struct ResearchFeedbackFrame(ByteString);

impl From<ResearchFeedbackFrame> for ByteString {
    fn from(frame: ResearchFeedbackFrame) -> Self {
        frame.0
    }
}

impl TryFrom<&FeedbackOutboxEntry> for ResearchFeedbackFrame {
    type Error = StorageError;

    fn try_from(entry: &FeedbackOutboxEntry) -> Result<Self, Self::Error> {
        let payload = ResearchFeedbackEvent::try_from(entry).map_err(|error| {
            StorageError::invariant_violation(
                Some(QUANT_FEEDBACK_EVENT_OUTBOX),
                format!("feedback outbox payload failed validation: {error}"),
            )
        })?;
        let data = serde_json::to_value(payload).map_err(|error| {
            StorageError::invariant_violation(
                Some(QUANT_FEEDBACK_EVENT_OUTBOX),
                format!("feedback outbox payload serialization failed: {error}"),
            )
        })?;
        serde_json::to_string(&WsEnvelope::channel(WsChannel::ResearchFeedback, data))
            .map(ByteString::from)
            .map(Self)
            .map_err(|error| {
                StorageError::invariant_violation(
                    Some(QUANT_FEEDBACK_EVENT_OUTBOX),
                    format!("feedback WebSocket envelope serialization failed: {error}"),
                )
            })
    }
}

impl FeedbackOutboxWorker {
    pub fn try_new(
        outbox: Arc<dyn FeedbackOutboxRepository>,
        registry: SessionRegistry,
        config: ResearchJobsConfig,
        recovery_total: IntCounterVec,
    ) -> QuantResult<Self> {
        let lease_secs = u64::try_from(config.lease_ttl_secs).map_err(|error| {
            FeedbackError::InvalidCoordinatorConfig {
                detail: format!("feedback outbox lease is invalid: {error}"),
            }
        })?;
        if lease_secs == 0 || config.poll_secs == 0 {
            return Err(FeedbackError::InvalidCoordinatorConfig {
                detail: "feedback outbox lease and poll cadence must be greater than zero"
                    .to_owned(),
            }
            .into());
        }
        Ok(Self {
            worker_id: WorkerId::from_v7(),
            outbox,
            registry,
            lease_secs,
            poll_interval: Duration::from_secs(config.poll_secs),
            recovery_total,
        })
    }

    /// Stop claiming immediately on shutdown; any interrupted claim is
    /// recovered from its durable lease after restart.
    pub async fn run(self, shutdown: CancellationToken) -> QuantResult<()> {
        loop {
            let result = tokio::select! {
                biased;
                () = shutdown.cancelled() => return Ok(()),
                result = self.run_once() => result,
            };
            if let Err(error) = result {
                tracing::warn!(%error, "feedback WebSocket outbox iteration failed");
            }
            tokio::select! {
                biased;
                () = shutdown.cancelled() => return Ok(()),
                () = sleep(self.poll_interval) => {}
            }
        }
    }

    async fn run_once(&self) -> QuantResult<()> {
        let entries = self
            .outbox
            .claim_outbox(self.worker_id, self.lease_secs, OUTBOX_BATCH_SIZE)
            .await?;
        for (index, entry) in entries.iter().enumerate() {
            let result = match self.dispatch(entry).await {
                Ok(()) => {
                    let result = self
                        .outbox
                        .publish_outbox(entry.revision, self.worker_id)
                        .await;
                    if result.is_ok() && entry.publish_attempts > 1 {
                        self.recovery_total.with_label_values(&["recovered"]).inc();
                    }
                    result
                }
                Err(error) => Err(error),
            };
            if let Err(error) = result {
                self.recovery_total.with_label_values(&["retry"]).inc();
                let detail = format!("feedback websocket delivery failed: {error}")
                    .chars()
                    .take(500)
                    .collect::<String>();
                self.fail_claims(&entries[index..], &detail).await;
                return Err(error.into());
            }
        }
        Ok(())
    }

    async fn dispatch(&self, entry: &FeedbackOutboxEntry) -> Result<(), StorageError> {
        let frame = ResearchFeedbackFrame::try_from(entry)?.into();
        if self
            .registry
            .fanout(
                SubscriptionKey::global(WsChannel::ResearchFeedback),
                frame,
                DeliveryClass::Reliable,
            )
            .await
        {
            Ok(())
        } else {
            Err(StorageError::state_conflict(
                QUANT_FEEDBACK_EVENT_OUTBOX,
                Some(entry.revision),
                "feedback session hub rejected durable publication",
            ))
        }
    }

    async fn fail_claims(&self, entries: &[FeedbackOutboxEntry], detail: &str) {
        for entry in entries {
            if let Err(error) = self
                .outbox
                .fail_outbox(entry.revision, self.worker_id, detail.to_owned())
                .await
            {
                tracing::error!(
                    revision = entry.revision,
                    %error,
                    "failed to release feedback WebSocket outbox claim"
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::VecDeque,
        sync::{
            Arc, Mutex,
            atomic::{AtomicU64, Ordering},
        },
    };

    use async_trait::async_trait;
    use chrono::{TimeZone, Utc};
    use prometheus::{
        GaugeVec, Histogram, HistogramOpts, IntCounter, IntCounterVec, IntGauge, IntGaugeVec, Opts,
    };
    use quant_pivot_error::storage::StorageError;
    use quant_pivot_models::{
        config::ResearchJobsConfig,
        domain::{
            quant::{
                FeedbackOutboxEntry, FeedbackOutboxSource, FeedbackStageEventInfo,
                FeedbackStageEventInput, NewFeedbackStageEvent,
            },
            ws::{SubscriptionKey, WsChannel},
        },
        enums::quant::{FeedbackStage, FeedbackStageEventKind, FeedbackTriggerFamily},
        types::{FeedbackCycleId, ResearchProfileId, UserId, WorkerId},
    };
    use quant_pivot_repository::traits::FeedbackOutboxRepository;
    use serde_json::{Value, from_value, to_value};
    use tokio::{
        sync::mpsc,
        time::{Duration, timeout},
    };
    use tokio_util::sync::CancellationToken;
    use uuid::Uuid;

    use super::FeedbackOutboxWorker;
    use crate::ws::{SessionHub, SessionHubMetrics, SessionRegistration, SessionRegistry};

    static METRIC_ID: AtomicU64 = AtomicU64::new(1);

    struct FakeOutbox {
        history: Vec<FeedbackOutboxEntry>,
        pending: Mutex<VecDeque<FeedbackOutboxEntry>>,
        published: Mutex<Vec<i64>>,
        failed: Mutex<Vec<i64>>,
    }

    impl FakeOutbox {
        fn new(entries: Vec<FeedbackOutboxEntry>) -> Self {
            Self {
                history: entries.clone(),
                pending: Mutex::new(entries.into()),
                published: Mutex::new(Vec::new()),
                failed: Mutex::new(Vec::new()),
            }
        }
    }

    #[async_trait]
    impl FeedbackOutboxRepository for FakeOutbox {
        async fn latest_outbox_revision(&self) -> Result<i64, StorageError> {
            Ok(self.history.last().map_or(0, |entry| entry.revision))
        }

        async fn claim_outbox(
            &self,
            _worker_id: WorkerId,
            _lease_secs: u64,
            limit: u64,
        ) -> Result<Vec<FeedbackOutboxEntry>, StorageError> {
            let mut pending = self.pending.lock().expect("pending outbox lock");
            let count = usize::try_from(limit)
                .unwrap_or(usize::MAX)
                .min(pending.len());
            Ok(pending.drain(..count).collect())
        }

        async fn publish_outbox(
            &self,
            revision: i64,
            _worker_id: WorkerId,
        ) -> Result<(), StorageError> {
            self.published
                .lock()
                .expect("published outbox lock")
                .push(revision);
            Ok(())
        }

        async fn fail_outbox(
            &self,
            revision: i64,
            _worker_id: WorkerId,
            _detail: String,
        ) -> Result<(), StorageError> {
            self.failed
                .lock()
                .expect("failed outbox lock")
                .push(revision);
            Ok(())
        }

        async fn list_outbox(
            &self,
            after_revision: i64,
            limit: u64,
        ) -> Result<Vec<FeedbackOutboxEntry>, StorageError> {
            let limit = usize::try_from(limit).unwrap_or(usize::MAX);
            Ok(self
                .history
                .iter()
                .filter(|entry| entry.revision > after_revision)
                .take(limit)
                .cloned()
                .collect())
        }
    }

    fn outbox_entry(revision: i64, profile_id: &str) -> FeedbackOutboxEntry {
        let occurred_at = Utc
            .with_ymd_and_hms(2026, 7, 29, 0, 0, 0)
            .single()
            .expect("valid timestamp");
        let sealed = NewFeedbackStageEvent::try_seal(FeedbackStageEventInput {
            feedback_cycle_id: FeedbackCycleId::from_v7(),
            event_sequence: 1,
            stage: FeedbackStage::Trigger,
            event_kind: FeedbackStageEventKind::Triggered,
            trigger_family: Some(FeedbackTriggerFamily::Scheduled),
            research_job_id: None,
            actor: Some("scheduler".to_owned()),
            reason_code: Some("scheduled_cadence".to_owned()),
            evidence_uri: None,
            evidence_hash: None,
            occurred_at,
        })
        .expect("seal feedback stage event");
        let mut value = to_value(sealed).expect("serialize sealed stage event");
        value.as_object_mut().expect("stage event object").insert(
            "created_at".to_owned(),
            to_value(occurred_at).expect("serialize created at"),
        );
        FeedbackOutboxEntry {
            revision,
            publish_attempts: 1,
            profile_id: ResearchProfileId::new(profile_id),
            source: FeedbackOutboxSource::Stage(
                from_value::<FeedbackStageEventInfo>(value).expect("decode stage event info"),
            ),
        }
    }

    fn test_registry() -> (SessionRegistry, SessionHub, CancellationToken) {
        let id = METRIC_ID.fetch_add(1, Ordering::Relaxed);
        let metrics = SessionHubMetrics {
            best_effort_dropped: IntCounter::new(
                format!("feedback_ws_best_effort_dropped_{id}"),
                "test",
            )
            .expect("best-effort metric"),
            best_effort_coalesced: IntCounter::new(
                format!("feedback_ws_best_effort_coalesced_{id}"),
                "test",
            )
            .expect("coalesced metric"),
            reliable_disconnects: IntCounter::new(
                format!("feedback_ws_reliable_disconnects_{id}"),
                "test",
            )
            .expect("reliable metric"),
            control_timeouts: IntCounter::new(format!("feedback_ws_control_timeouts_{id}"), "test")
                .expect("timeout metric"),
            control_latency_seconds: Histogram::with_opts(HistogramOpts::new(
                format!("feedback_ws_control_latency_{id}"),
                "test",
            ))
            .expect("latency metric"),
            queue_depth: IntGaugeVec::new(
                Opts::new(format!("feedback_ws_queue_depth_{id}"), "test"),
                &["lane"],
            )
            .expect("depth metric"),
            queue_oldest_age_seconds: GaugeVec::new(
                Opts::new(format!("feedback_ws_queue_age_{id}"), "test"),
                &["lane"],
            )
            .expect("age metric"),
            frame_bytes: IntGauge::new(format!("feedback_ws_frame_bytes_{id}"), "test")
                .expect("bytes metric"),
        };
        let shutdown = CancellationToken::new();
        let (registry, hub) = SessionRegistry::new(metrics);
        (registry, hub, shutdown)
    }

    fn recovery_metric() -> IntCounterVec {
        let id = METRIC_ID.fetch_add(1, Ordering::Relaxed);
        IntCounterVec::new(
            Opts::new(format!("feedback_ws_recovery_{id}"), "test"),
            &["outcome"],
        )
        .expect("recovery metric")
    }

    #[tokio::test]
    async fn worker_publishes_durable_revision() {
        let mut entry = outbox_entry(7, "crypto_price_15m");
        entry.publish_attempts = 2;
        let outbox = Arc::new(FakeOutbox::new(vec![entry]));
        let (registry, hub, shutdown) = test_registry();
        let recovery = recovery_metric();
        let hub_task = tokio::spawn(hub.run(shutdown.clone(), shutdown.clone()));
        let (outbound, mut receiver) = mpsc::channel(8);
        let session_id = registry
            .register(SessionRegistration {
                outbound,
                subject: UserId::new(Uuid::from_u128(1)),
                family_id: "feedback-family".to_owned(),
                can_read_system: false,
                cancellation: CancellationToken::new(),
            })
            .await
            .expect("register feedback subscriber");
        assert!(
            registry
                .subscribe(
                    session_id,
                    SubscriptionKey::global(WsChannel::ResearchFeedback),
                )
                .await
        );
        FeedbackOutboxWorker::try_new(
            Arc::clone(&outbox) as Arc<dyn FeedbackOutboxRepository>,
            registry.clone(),
            ResearchJobsConfig::default(),
            recovery.clone(),
        )
        .expect("build feedback outbox worker")
        .run_once()
        .await
        .expect("publish feedback outbox");
        let body: Value = {
            let frame = timeout(Duration::from_secs(1), receiver.recv())
                .await
                .expect("feedback frame timeout")
                .expect("feedback frame");
            serde_json::from_str(frame.text()).expect("decode feedback frame")
        };
        assert_eq!(body["type"], "research.feedback");
        assert_eq!(body["data"]["revision"], 7);
        assert_eq!(body["data"]["subject_kind"], "feedback_cycle");
        assert_eq!(body["data"]["profile_id"], "crypto_price_15m");
        assert_eq!(
            outbox.published.lock().expect("published lock").as_slice(),
            [7]
        );
        assert!(outbox.failed.lock().expect("failed lock").is_empty());
        assert_eq!(recovery.with_label_values(&["recovered"]).get(), 1);
        shutdown.cancel();
        hub_task.await.expect("hub task");
    }

    #[tokio::test]
    async fn invalid_payload_nacks_claim() {
        let outbox = Arc::new(FakeOutbox::new(vec![outbox_entry(9, "")]));
        let (registry, hub, shutdown) = test_registry();
        let recovery = recovery_metric();
        let hub_task = tokio::spawn(hub.run(shutdown.clone(), shutdown.clone()));
        assert!(
            FeedbackOutboxWorker::try_new(
                Arc::clone(&outbox) as Arc<dyn FeedbackOutboxRepository>,
                registry.clone(),
                ResearchJobsConfig::default(),
                recovery.clone(),
            )
            .expect("build feedback outbox worker")
            .run_once()
            .await
            .is_err()
        );
        assert!(outbox.published.lock().expect("published lock").is_empty());
        assert_eq!(outbox.failed.lock().expect("failed lock").as_slice(), [9]);
        assert_eq!(recovery.with_label_values(&["retry"]).get(), 1);
        shutdown.cancel();
        hub_task.await.expect("hub task");
    }
}
