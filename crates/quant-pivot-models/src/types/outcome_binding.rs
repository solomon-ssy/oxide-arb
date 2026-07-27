//! Exact binary-outcome token orientation for one model input row.

use serde::{Deserialize, Deserializer, Serialize, de::Error as SerdeError};
use thiserror::Error;

use crate::{
    enums::quant::OutcomeSide,
    types::{MarketId, TokenId},
};

/// A validated catalog token pair plus the token whose evidence fed the row.
///
/// This is the sole orientation contract used to project token-relative alpha
/// into canonical-YES space. Keeping both the token and its declared side makes
/// a stale or mismatched position/catalog binding fail closed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct OutcomeTokenBinding {
    market_id: MarketId,
    yes_token_id: TokenId,
    no_token_id: TokenId,
    feature_token_id: TokenId,
    feature_side: OutcomeSide,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct OutcomeTokenBindingDocument {
    market_id: MarketId,
    yes_token_id: TokenId,
    no_token_id: TokenId,
    feature_token_id: TokenId,
    feature_side: OutcomeSide,
}

/// Stable validation failures for [`OutcomeTokenBinding`].
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum OutcomeTokenBindingError {
    #[error("market {market_id} has the same YES and NO token {token_id}")]
    DuplicateTokenPair {
        market_id: MarketId,
        token_id: TokenId,
    },
    #[error(
        "market {market_id} declares feature token {actual} as {side:?}, but the catalog token is {expected}"
    )]
    TokenSideMismatch {
        market_id: MarketId,
        side: OutcomeSide,
        expected: TokenId,
        actual: TokenId,
    },
}

impl OutcomeTokenBinding {
    /// Validate and construct an exact feature-token orientation.
    pub fn try_new(
        market_id: MarketId,
        yes_token_id: TokenId,
        no_token_id: TokenId,
        feature_token_id: TokenId,
        feature_side: OutcomeSide,
    ) -> Result<Self, OutcomeTokenBindingError> {
        let binding = Self {
            market_id,
            yes_token_id,
            no_token_id,
            feature_token_id,
            feature_side,
        };
        binding.validate()?;
        Ok(binding)
    }

    /// Validate the catalog pair and explicit side declaration.
    pub fn validate(&self) -> Result<(), OutcomeTokenBindingError> {
        if self.yes_token_id == self.no_token_id {
            return Err(OutcomeTokenBindingError::DuplicateTokenPair {
                market_id: self.market_id.clone(),
                token_id: self.yes_token_id.clone(),
            });
        }
        let expected = match self.feature_side {
            OutcomeSide::Yes => &self.yes_token_id,
            OutcomeSide::No => &self.no_token_id,
        };
        if &self.feature_token_id != expected {
            return Err(OutcomeTokenBindingError::TokenSideMismatch {
                market_id: self.market_id.clone(),
                side: self.feature_side,
                expected: expected.clone(),
                actual: self.feature_token_id.clone(),
            });
        }
        Ok(())
    }

    #[must_use]
    pub const fn market_id(&self) -> &MarketId {
        &self.market_id
    }

    #[must_use]
    pub const fn yes_token_id(&self) -> &TokenId {
        &self.yes_token_id
    }

    #[must_use]
    pub const fn no_token_id(&self) -> &TokenId {
        &self.no_token_id
    }

    #[must_use]
    pub const fn feature_token_id(&self) -> &TokenId {
        &self.feature_token_id
    }

    #[must_use]
    pub const fn feature_side(&self) -> OutcomeSide {
        self.feature_side
    }

    /// Sign that projects feature-token support into canonical-YES support.
    #[must_use]
    pub const fn feature_to_yes_sign(&self) -> i8 {
        match self.feature_side {
            OutcomeSide::Yes => 1,
            OutcomeSide::No => -1,
        }
    }
}

impl TryFrom<OutcomeTokenBindingDocument> for OutcomeTokenBinding {
    type Error = OutcomeTokenBindingError;

    fn try_from(document: OutcomeTokenBindingDocument) -> Result<Self, Self::Error> {
        Self::try_new(
            document.market_id,
            document.yes_token_id,
            document.no_token_id,
            document.feature_token_id,
            document.feature_side,
        )
    }
}

impl<'de> Deserialize<'de> for OutcomeTokenBinding {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let document = OutcomeTokenBindingDocument::deserialize(deserializer)?;
        Self::try_from(document).map_err(SerdeError::custom)
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::OutcomeTokenBinding;
    use crate::{
        enums::quant::OutcomeSide,
        types::{MarketId, TokenId},
    };

    #[test]
    fn yes_no_signs_mirror() {
        let market_id = MarketId::from("market");
        let yes_token = TokenId::from("yes");
        let no_token = TokenId::from("no");
        let yes = OutcomeTokenBinding::try_new(
            market_id.clone(),
            yes_token.clone(),
            no_token.clone(),
            yes_token,
            OutcomeSide::Yes,
        )
        .expect("YES binding");
        let no = OutcomeTokenBinding::try_new(
            market_id,
            TokenId::from("yes"),
            no_token.clone(),
            no_token,
            OutcomeSide::No,
        )
        .expect("NO binding");

        assert_eq!(yes.feature_to_yes_sign(), 1);
        assert_eq!(no.feature_to_yes_sign(), -1);
    }

    #[test]
    fn token_mismatch_fails_closed() {
        let result = OutcomeTokenBinding::try_new(
            MarketId::from("market"),
            TokenId::from("yes"),
            TokenId::from("no"),
            TokenId::from("no"),
            OutcomeSide::Yes,
        );

        assert!(result.is_err());
    }

    #[test]
    fn deserialize_revalidates_binding() {
        let malformed = json!({
            "market_id": "market",
            "yes_token_id": "yes",
            "no_token_id": "no",
            "feature_token_id": "yes",
            "feature_side": "no"
        });

        assert!(serde_json::from_value::<OutcomeTokenBinding>(malformed).is_err());
    }
}
