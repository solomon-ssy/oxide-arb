use serde::{Deserialize, Serialize};

use crate::{
    domain::control_factor::{ArtifactHash, QueryFingerprint},
    enums::control_factor::{MaterializationErrorCode, MaterializationStageName},
    hashing::CanonicalDigest,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EvidenceStageOutcome {
    Usable,
    EvidenceOnly,
    Insufficient,
    Fatal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EvidenceIssueSeverity {
    Info,
    Warning,
    ProductionBlocking,
    Fatal,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvidenceSourceRef {
    pub source_domain: String,
    pub source_repository: String,
    pub source_table: Option<String>,
    pub query_fingerprint: QueryFingerprint,
    pub row_ref: Option<String>,
    pub artifact_hash: Option<ArtifactHash>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvidenceIssue {
    pub code: MaterializationErrorCode,
    pub severity: EvidenceIssueSeverity,
    pub message: String,
    pub stage_name: Option<MaterializationStageName>,
    pub source_refs: Vec<EvidenceSourceRef>,
}

impl EvidenceIssue {
    #[must_use]
    pub fn production_blocking(
        code: MaterializationErrorCode,
        message: impl Into<String>,
        stage_name: MaterializationStageName,
    ) -> Self {
        Self {
            code,
            severity: EvidenceIssueSeverity::ProductionBlocking,
            message: message.into(),
            stage_name: Some(stage_name),
            source_refs: Vec::new(),
        }
    }

    #[must_use]
    pub const fn is_production_blocking(&self) -> bool {
        matches!(
            self.severity,
            EvidenceIssueSeverity::ProductionBlocking | EvidenceIssueSeverity::Fatal
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryContract {
    pub version: u32,
    pub repository: String,
    pub method: String,
    pub params_hash: String,
    pub ordering: Vec<String>,
    pub schema_version: Option<u32>,
}

impl QueryContract {
    #[must_use]
    pub fn new(
        repository: impl Into<String>,
        method: impl Into<String>,
        params_hash: impl Into<String>,
        ordering: Vec<String>,
        schema_version: Option<u32>,
    ) -> Self {
        Self {
            version: 1,
            repository: repository.into(),
            method: method.into(),
            params_hash: params_hash.into(),
            ordering,
            schema_version,
        }
    }

    #[must_use]
    pub fn fingerprint(&self) -> QueryFingerprint {
        let bytes = serde_json::to_vec(self).unwrap_or_else(|_| self.fallback_bytes());
        QueryFingerprint(format!(
            "{}.{}:v{}:blake3:{}",
            self.repository,
            self.method,
            self.version,
            CanonicalDigest::raw_hex(&bytes)
        ))
    }

    fn fallback_bytes(&self) -> Vec<u8> {
        format!(
            "{}:{}:{}:{}",
            self.repository, self.method, self.version, self.params_hash
        )
        .into_bytes()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvidenceQueryResult<T> {
    pub rows: Vec<T>,
    pub contract: QueryContract,
    pub fingerprint: QueryFingerprint,
    pub source_refs: Vec<EvidenceSourceRef>,
}

impl<T> EvidenceQueryResult<T> {
    #[must_use]
    pub fn new(rows: Vec<T>, contract: QueryContract, source_refs: Vec<EvidenceSourceRef>) -> Self {
        let fingerprint = contract.fingerprint();
        Self {
            rows,
            contract,
            fingerprint,
            source_refs,
        }
    }

    #[must_use]
    pub fn from_rows(rows: Vec<T>, contract: QueryContract) -> Self {
        Self::new(rows, contract, Vec::new())
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.rows.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    #[must_use]
    pub fn into_rows(self) -> Vec<T> {
        self.rows
    }
}
