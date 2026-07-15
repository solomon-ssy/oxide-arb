//! Strong-typed `quant_reconciliation.evidence_json` JSONB content.

use chrono::{DateTime, Utc};
use sea_orm::FromJsonQueryResult;
use serde::{Deserialize, Serialize};

use crate::{
    enums::execution::ReconciliationEvidenceKind,
    jsonb_active,
    types::{FeeEvidence, Price, Shares},
};

/// One reconciliation observation used to explain an execution-order result.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReconciliationEvidence {
    pub kind: ReconciliationEvidenceKind,
    pub observed_at: DateTime<Utc>,
    pub detail: String,
    pub venue_ref: Option<String>,
    pub shares: Option<Shares>,
    pub price: Option<Price>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fee_evidence: Option<FeeEvidence>,
}

/// Ordered evidence chain for one reconciliation summary row.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
#[serde(transparent)]
pub struct ReconciliationEvidenceChain(pub Vec<ReconciliationEvidence>);

impl ReconciliationEvidenceChain {
    #[must_use]
    pub fn into_inner(self) -> Vec<ReconciliationEvidence> {
        self.0
    }

    pub fn push(&mut self, evidence: ReconciliationEvidence) {
        self.0.push(evidence);
    }
}

jsonb_active!(ReconciliationEvidenceChain);

#[cfg(test)]
mod tests {
    use super::{ReconciliationEvidence, ReconciliationEvidenceChain};
    use crate::{enums::execution::ReconciliationEvidenceKind, types::Shares};
    use chrono::TimeZone;
    use rust_decimal_macros::dec;

    #[test]
    fn evidence_chain_round_trips_as_json_array() {
        let chain = ReconciliationEvidenceChain(vec![ReconciliationEvidence {
            kind: ReconciliationEvidenceKind::ClobOrderStatus,
            observed_at: chrono::Utc
                .with_ymd_and_hms(2026, 1, 2, 3, 4, 5)
                .single()
                .expect("valid timestamp"),
            detail: "open on venue".to_owned(),
            venue_ref: Some("0xorder".to_owned()),
            shares: Some(Shares::new(dec!(12.5))),
            price: None,
            fee_evidence: None,
        }]);

        let encoded = serde_json::to_value(&chain).expect("serialize");
        assert!(encoded.is_array());
        let decoded: ReconciliationEvidenceChain =
            serde_json::from_value(encoded).expect("deserialize");
        assert_eq!(decoded, chain);
    }
}
