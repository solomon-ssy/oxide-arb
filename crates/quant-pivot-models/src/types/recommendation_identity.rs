//! Human-readable recommendation identity (parent doc §5).

use sea_orm::FromJsonQueryResult;
use serde::{Deserialize, Serialize};

use crate::enums::common::MarketCategory;

/// Display identity frozen at decision time.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
#[serde(deny_unknown_fields)]
pub struct RecommendationIdentity {
    pub category: MarketCategory,
    pub question: String,
    pub outcome_name: String,
}
