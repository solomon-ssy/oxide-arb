use chrono::{DateTime, Utc};
use oxide_arb_models::{
    domain::control_factor::{
        ArtifactHash, QueryFingerprint, StageArtifactRef, StageCoverageReport, StageError,
        StageReportBody, StageWarning,
    },
    enums::control_factor::{EvidenceStageStatus, MaterializationStageName},
    types::{MaterializationRunId, StageReportId},
};
use serde::Serialize;

use crate::materialization::{ArtifactHasher, MaterializationResult};

pub struct StageReportBuilder {
    stage_report_id: StageReportId,
    run_id: MaterializationRunId,
    stage_name: MaterializationStageName,
    status: EvidenceStageStatus,
    started_at: DateTime<Utc>,
    finished_at: Option<DateTime<Utc>>,
    input_artifact_hashes: Vec<StageArtifactRef>,
    output_artifact_hash: Option<ArtifactHash>,
    coverage: StageCoverageReport,
    metrics: serde_json::Value,
    records_read: u64,
    records_written: u64,
    warnings: Vec<StageWarning>,
    errors: Vec<StageError>,
    query_fingerprints: Vec<QueryFingerprint>,
}

impl StageReportBuilder {
    #[must_use]
    pub fn new(
        run_id: MaterializationRunId,
        stage_name: MaterializationStageName,
        started_at: DateTime<Utc>,
    ) -> Self {
        Self {
            stage_report_id: StageReportId::from_v7(),
            run_id,
            stage_name,
            status: EvidenceStageStatus::Running,
            started_at,
            finished_at: None,
            input_artifact_hashes: Vec::new(),
            output_artifact_hash: None,
            coverage: StageCoverageReport::complete(0),
            metrics: serde_json::json!({}),
            records_read: 0,
            records_written: 0,
            warnings: Vec::new(),
            errors: Vec::new(),
            query_fingerprints: Vec::new(),
        }
    }

    #[must_use]
    pub const fn status(mut self, status: EvidenceStageStatus) -> Self {
        self.status = status;
        self
    }

    #[must_use]
    pub const fn finished_at(mut self, finished_at: DateTime<Utc>) -> Self {
        self.finished_at = Some(finished_at);
        self
    }

    #[must_use]
    pub fn coverage(mut self, coverage: StageCoverageReport) -> Self {
        self.coverage = coverage;
        self
    }

    #[must_use]
    pub fn metrics(mut self, metrics: serde_json::Value) -> Self {
        self.metrics = metrics;
        self
    }

    #[must_use]
    pub const fn records_read(mut self, records_read: u64) -> Self {
        self.records_read = records_read;
        self
    }

    #[must_use]
    pub const fn records_written(mut self, records_written: u64) -> Self {
        self.records_written = records_written;
        self
    }

    #[must_use]
    pub fn warning(mut self, warning: StageWarning) -> Self {
        self.warnings.push(warning);
        self
    }

    #[must_use]
    pub fn error(mut self, error: StageError) -> Self {
        self.errors.push(error);
        self
    }

    #[must_use]
    pub fn input_artifact(mut self, artifact: StageArtifactRef) -> Self {
        self.input_artifact_hashes.push(artifact);
        self
    }

    #[must_use]
    pub fn query_fingerprint(mut self, fingerprint: QueryFingerprint) -> Self {
        self.query_fingerprints.push(fingerprint);
        self
    }

    pub fn output_artifact<T: Serialize>(mut self, artifact: &T) -> MaterializationResult<Self> {
        self.output_artifact_hash = Some(ArtifactHasher::compute(artifact)?);
        Ok(self)
    }

    #[must_use]
    pub fn build(self) -> StageReportBody {
        StageReportBody {
            stage_report_id: self.stage_report_id,
            run_id: self.run_id,
            stage_name: self.stage_name,
            status: self.status,
            started_at: self.started_at,
            finished_at: self.finished_at,
            input_artifact_hashes: self.input_artifact_hashes,
            output_artifact_hash: self.output_artifact_hash,
            coverage: self.coverage,
            metrics: self.metrics,
            records_read: self.records_read,
            records_written: self.records_written,
            warnings: self.warnings,
            errors: self.errors,
            query_fingerprints: self.query_fingerprints,
        }
    }
}
