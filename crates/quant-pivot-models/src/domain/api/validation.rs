//! Shared [`validator`] helpers for inbound API bodies.
//!
//! Mutation bodies with explicit half-open `[window_start, window_end)` bounds
//! register a schema validator via [`half_open_window_request!`] (naming convention:
//! `validate_{snake_case_type}`). Optional catalog `from` / `to` filters use
//! [`validate_optional_inclusive_range`].

use chrono::{DateTime, Utc};
use validator::ValidationError;

use crate::domain::{TimeWindow, WindowBoundsError};

/// Reject half-open windows where `end <= start` (empty or inverted span).
pub fn validate_half_open_window(
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) -> Result<(), ValidationError> {
    TimeWindow::try_half_open(start, end)
        .map_err(|_| {
            let mut error = ValidationError::new("half_open_window");
            error.message = Some(WindowBoundsError::MESSAGE.into());
            error
        })
        .map(|_| ())
}

/// Registers `validate_{snake_case_type}` for a mutation body carrying
/// `window_start` / `window_end`.
///
/// Pair with:
/// ```ignore
/// #[derive(Validate)]
/// #[validate(schema(function = "validate_fit_bias_table_request"))]
/// pub struct FitBiasTableRequest { window_start, window_end, .. }
/// half_open_window_request!(FitBiasTableRequest);
/// ```
#[macro_export]
macro_rules! half_open_window_request {
    ($ty:ident) => {
        ::paste::paste! {
            fn [<validate_ $ty:snake>](
                value: &$ty,
            ) -> ::std::result::Result<(), ::validator::ValidationError> {
                $crate::domain::api::validation::validate_half_open_window(
                    value.window_start,
                    value.window_end,
                )
            }
        }
    };
}

/// When both catalog-filter bounds are present, require `from <= to`.
///
/// Zero-width inclusive ranges are allowed (unlike half-open mutation windows).
pub fn validate_optional_inclusive_range(
    from: Option<DateTime<Utc>>,
    to: Option<DateTime<Utc>>,
) -> Result<(), ValidationError> {
    if let (Some(from), Some(to)) = (from, to)
        && to < from
    {
        let mut error = ValidationError::new("inclusive_range");
        error.message = Some("`to` must be >= `from`".into());
        return Err(error);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;

    use super::*;

    #[test]
    fn half_open_window_rejects_inverted_and_empty() {
        let start = Utc.timestamp_opt(100, 0).unwrap();
        let end = Utc.timestamp_opt(50, 0).unwrap();
        assert!(validate_half_open_window(start, end).is_err());

        let t = Utc.timestamp_opt(100, 0).unwrap();
        assert!(validate_half_open_window(t, t).is_err());
    }

    #[test]
    fn optional_inclusive_range_allows_zero_width() {
        let t = Utc.timestamp_opt(100, 0).unwrap();
        assert!(validate_optional_inclusive_range(Some(t), Some(t)).is_ok());
    }

    #[test]
    fn optional_inclusive_range_rejects_inverted() {
        let from = Utc.timestamp_opt(200, 0).unwrap();
        let to = Utc.timestamp_opt(100, 0).unwrap();
        assert!(validate_optional_inclusive_range(Some(from), Some(to)).is_err());
    }
}
