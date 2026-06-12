//! Generic pagination envelope shared by the repository layer and the web API.

use serde::{Deserialize, Serialize};
use serde_with::{DisplayFromStr, PickFirst, serde_as};

/// Reusable pagination request parameters embedded by every list query DTO.
///
/// `page` is 1-based; `size` is the requested page size. Both fields default
/// (via `serde`) when absent from a query string, and [`PageRequest::normalized`]
/// hardens any caller-supplied values into a safe window before they reach SQL:
/// `page` is forced to at least `1`, and `size` is clamped to at most
/// [`PageRequest::MAX_SIZE`] so a single request can never demand an unbounded
/// result set. List DTOs compose this with `#[serde(flatten)]` so the
/// query string stays flat (`?page=&size=`), which the web `Query` extractor
/// consumes directly.
///
/// `PickFirst<(_, DisplayFromStr)>`: `#[serde(flatten)]` buffers all fields
/// through serde's internal `Content` tree, which preserves query-string values
/// as strings and performs no string→number coercion, so a plain `u64` field
/// would reject `?page=1`. The adapter accepts a native number first (JSON) and
/// falls back to `FromStr` (query string), while serializing as a number.
#[serde_as]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct PageRequest {
    /// 1-based page index (defaults to [`PageRequest::DEFAULT_PAGE`]).
    #[serde_as(as = "PickFirst<(_, DisplayFromStr)>")]
    #[serde(default = "PageRequest::default_page")]
    pub page: u64,
    /// Requested page size (defaults to [`PageRequest::DEFAULT_SIZE`]).
    #[serde_as(as = "PickFirst<(_, DisplayFromStr)>")]
    #[serde(default = "PageRequest::default_size")]
    pub size: u64,
}

impl PageRequest {
    /// Default page index when omitted by the caller.
    pub const DEFAULT_PAGE: u64 = 1;
    /// Default page size when omitted by the caller.
    pub const DEFAULT_SIZE: u64 = 20;
    /// Hard upper bound on `size`, the fail-safe against unbounded queries.
    pub const MAX_SIZE: u64 = 200;

    /// `serde` default for [`PageRequest::page`].
    const fn default_page() -> u64 {
        Self::DEFAULT_PAGE
    }

    /// `serde` default for [`PageRequest::size`].
    const fn default_size() -> u64 {
        Self::DEFAULT_SIZE
    }

    /// Construct a request for an explicit window (unnormalized).
    #[must_use]
    pub const fn new(page: u64, size: u64) -> Self {
        Self { page, size }
    }

    /// Return a safe copy: `page` forced to at least `1`, and `size` mapped to
    /// [`PageRequest::DEFAULT_SIZE`] when `0` or clamped to
    /// [`PageRequest::MAX_SIZE`] when too large.
    #[must_use]
    pub const fn normalized(&self) -> Self {
        let page = if self.page == 0 { 1 } else { self.page };
        let size = if self.size == 0 {
            Self::DEFAULT_SIZE
        } else if self.size > Self::MAX_SIZE {
            Self::MAX_SIZE
        } else {
            self.size
        };
        Self { page, size }
    }

    /// SQL `OFFSET` for the normalized window: `(page - 1) * size`.
    #[must_use]
    pub const fn offset(&self) -> u64 {
        let normalized = self.normalized();
        (normalized.page - 1).saturating_mul(normalized.size)
    }

    /// SQL `LIMIT` for the normalized window (the effective page size).
    #[must_use]
    pub const fn limit(&self) -> u64 {
        self.normalized().size
    }
}

impl Default for PageRequest {
    fn default() -> Self {
        Self {
            page: Self::DEFAULT_PAGE,
            size: Self::DEFAULT_SIZE,
        }
    }
}

/// A single page of results plus the metadata needed to drive client paging.
///
/// `page` is 1-based; `size` is the requested page size. `has_next` is derived
/// from `total` and the current window so callers never recompute it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Paginated<T> {
    /// The rows for the requested page.
    pub items: Vec<T>,
    /// Total number of rows matching the query across all pages.
    pub total: u64,
    /// 1-based page index that produced `items`.
    pub page: u64,
    /// Requested page size (the maximum length of `items`).
    pub size: u64,
    /// Whether at least one more page exists after this one.
    pub has_next: bool,
}

impl<T> Paginated<T> {
    /// Build a page, deriving `has_next` from the window `(page, size)` against
    /// `total`. A `size` of `0` is treated as "no further pages".
    #[must_use]
    pub const fn new(items: Vec<T>, total: u64, page: u64, size: u64) -> Self {
        let consumed = page.saturating_mul(size);
        let has_next = size != 0 && consumed < total;
        Self {
            items,
            total,
            page,
            size,
            has_next,
        }
    }

    /// Build a page from a [`PageRequest`], reporting the normalized window so
    /// `page`/`size`/`has_next` reflect what was actually queried.
    #[must_use]
    pub const fn from_request(items: Vec<T>, total: u64, request: &PageRequest) -> Self {
        let window = request.normalized();
        Self::new(items, total, window.page, window.size)
    }

    /// An empty page for the given window (no rows, `total = 0`).
    #[must_use]
    pub const fn empty(page: u64, size: u64) -> Self {
        Self {
            items: Vec::new(),
            total: 0,
            page,
            size,
            has_next: false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{PageRequest, Paginated};

    #[test]
    fn deserializes_string_query_params() {
        let request: PageRequest =
            serde_json::from_value(serde_json::json!({ "page": "1", "size": "10" }))
                .expect("string page/size from query string");
        assert_eq!(request.page, 1);
        assert_eq!(request.size, 10);
    }

    #[test]
    fn deserializes_numeric_json_params() {
        let request: PageRequest =
            serde_json::from_value(serde_json::json!({ "page": 2, "size": 50 }))
                .expect("numeric page/size from JSON");
        assert_eq!(request.page, 2);
        assert_eq!(request.size, 50);
    }

    #[test]
    fn rejects_non_numeric_and_negative_params() {
        assert!(
            serde_json::from_value::<PageRequest>(serde_json::json!({ "page": "abc" })).is_err()
        );
        assert!(serde_json::from_value::<PageRequest>(serde_json::json!({ "page": -1 })).is_err());
    }

    #[test]
    fn serializes_as_native_numbers() {
        let json = serde_json::to_value(PageRequest::new(2, 50)).expect("serialize");
        assert_eq!(json, serde_json::json!({ "page": 2, "size": 50 }));
    }

    #[test]
    fn default_is_first_page_default_size() {
        let request = PageRequest::default();
        assert_eq!(request.page, PageRequest::DEFAULT_PAGE);
        assert_eq!(request.size, PageRequest::DEFAULT_SIZE);
    }

    #[test]
    fn normalize_forces_page_to_at_least_one() {
        assert_eq!(PageRequest::new(0, 10).normalized().page, 1);
        assert_eq!(PageRequest::new(3, 10).normalized().page, 3);
    }

    #[test]
    fn normalize_maps_zero_size_to_default() {
        assert_eq!(
            PageRequest::new(1, 0).normalized().size,
            PageRequest::DEFAULT_SIZE
        );
    }

    #[test]
    fn normalize_clamps_oversized_size_to_max() {
        assert_eq!(
            PageRequest::new(1, PageRequest::MAX_SIZE + 1)
                .normalized()
                .size,
            PageRequest::MAX_SIZE
        );
        assert_eq!(PageRequest::new(1, 50).normalized().size, 50);
    }

    #[test]
    fn offset_and_limit_use_the_normalized_window() {
        let request = PageRequest::new(3, 25);
        assert_eq!(request.offset(), 50);
        assert_eq!(request.limit(), 25);
        // Page 0 normalizes to 1, so a first-page offset of 0.
        assert_eq!(PageRequest::new(0, 25).offset(), 0);
        // Oversized size is clamped before computing the offset.
        assert_eq!(
            PageRequest::new(2, PageRequest::MAX_SIZE + 100).offset(),
            PageRequest::MAX_SIZE
        );
    }

    #[test]
    fn from_request_reports_the_normalized_window_and_has_next() {
        let page = Paginated::from_request(vec![1, 2], 5, &PageRequest::new(0, 0));
        assert_eq!(page.page, 1);
        assert_eq!(page.size, PageRequest::DEFAULT_SIZE);
        // total (5) <= consumed (1 * 20) -> no further page.
        assert!(!page.has_next);

        let windowed = Paginated::from_request(vec![1, 2], 5, &PageRequest::new(1, 2));
        assert!(windowed.has_next);
    }
}
