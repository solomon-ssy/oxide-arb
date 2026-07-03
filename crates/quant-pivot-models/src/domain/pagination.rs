//! Generic pagination envelope shared by the repository layer and the web API.
//!
//! Three layers:
//! 1. **Wire** — [`PageRequest`] embedded in list query DTOs (untrusted).
//! 2. **Contract** — [`NormalizePageQuery`] hardens the full query; domain enrich
//!    hooks like `MarketPageQuery::prepare()` live on specific query types.
//! 3. **SQL boundary** — [`PageWindow`] is the only type accepted by
//!    `paginate_mapped`; it is always hardened via [`PageWindow::harden`].

use serde::{Deserialize, Serialize};
use serde_with::{DisplayFromStr, PickFirst, serde_as};

pub(crate) mod sealed {
    /// Marks list queries registered by `#[derive(NormalizePageQuery)]`.
    pub trait Sealed {}
}

/// Inbound list-query contract for DTOs embedding a `#[normalize_page]` field.
pub trait NormalizePageQuery: sealed::Sealed + Sized {
    /// Embedded pagination parameters (may be caller-supplied / unnormalized).
    fn page(&self) -> &PageRequest;

    /// Return a copy whose embedded page has been hardened.
    #[must_use]
    fn normalized(self) -> Self;
}

/// Hardened pagination window — safe for SQL `LIMIT`/`OFFSET` and outbound
/// [`Paginated`] metadata.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PageWindow {
    page: u64,
    size: u64,
}

impl PageWindow {
    /// Harden raw request parameters.
    #[must_use]
    pub const fn harden(raw: PageRequest) -> Self {
        let raw = raw.normalized();
        Self {
            page: raw.page,
            size: raw.size,
        }
    }

    /// Harden the embedded page of any list query.
    #[must_use]
    pub fn from_query<Q: NormalizePageQuery>(query: &Q) -> Self {
        Self::harden(*query.page())
    }

    /// 1-based page index (always >= 1).
    #[must_use]
    pub const fn page(&self) -> u64 {
        self.page
    }

    /// Effective page size (always 1..=[`PageRequest::MAX_SIZE`]).
    #[must_use]
    pub const fn size(&self) -> u64 {
        self.size
    }

    /// SQL `OFFSET`: `(page - 1) * size`.
    #[must_use]
    pub const fn offset(&self) -> u64 {
        (self.page - 1).saturating_mul(self.size)
    }

    /// 0-based page index for `SeaORM` `fetch_page`.
    #[must_use]
    pub const fn seaorm_index(&self) -> u64 {
        self.page.saturating_sub(1)
    }
}

/// Empty catalog page for mock/stub ports.
#[must_use]
pub fn empty_catalog_page<Q, I>(query: &Q) -> Paginated<I>
where
    Q: NormalizePageQuery,
{
    Paginated::empty_for(query)
}

/// Reusable pagination request parameters embedded by every list query DTO.
///
/// `page` is 1-based; `size` is the requested page size. Both fields default
/// (via `serde`) when absent from a query string, and [`PageRequest::normalized`]
/// hardens any caller-supplied values into a safe window before they reach SQL:
/// `page` is forced to at least `1`, and `size` is clamped to at most
/// [`PageRequest::MAX_SIZE`] so a single request can never demand an unbounded
/// result set. List DTOs compose this with `#[serde(flatten)]` so the
/// query string stays flat (`?page=&size=`), which the web `Query` extractor
/// consumes directly. Pair with `#[normalize_page]` and
/// [`NormalizePageQuery`](quant_pivot_macros::NormalizePageQuery) on list query
/// DTOs.
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
        PageWindow::harden(*self).offset()
    }

    /// SQL `LIMIT` for the normalized window (the effective page size).
    #[must_use]
    pub const fn limit(&self) -> u64 {
        PageWindow::harden(*self).size()
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

    /// Build a page from a hardened [`PageWindow`].
    #[must_use]
    pub const fn from_window(items: Vec<T>, total: u64, window: PageWindow) -> Self {
        Self::new(items, total, window.page(), window.size())
    }

    /// Empty page using the hardened window implied by `query`.
    #[must_use]
    pub fn empty_for<Q: NormalizePageQuery>(query: &Q) -> Self {
        let window = PageWindow::from_query(query);
        Self::empty(window.page(), window.size())
    }

    /// Project every item through `f`, preserving the paging metadata.
    ///
    /// The canonical bridge from a repository page (`Paginated<XRow>` /
    /// `Paginated<XInfo>`) to its outbound contract (`Paginated<XView>`).
    #[must_use]
    pub fn map<U>(self, f: impl FnMut(T) -> U) -> Paginated<U> {
        Paginated {
            items: self.items.into_iter().map(f).collect(),
            total: self.total,
            page: self.page,
            size: self.size,
            has_next: self.has_next,
        }
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
    use quant_pivot_macros::NormalizePageQuery;

    use super::{NormalizePageQuery as _, PageRequest, PageWindow, Paginated};

    #[derive(NormalizePageQuery)]
    struct SmokeQuery {
        filter: Option<u32>,
        #[normalize_page]
        page: PageRequest,
    }

    #[test]
    fn normalize_page_query_derive_hardens_page() {
        let query = SmokeQuery {
            filter: Some(1),
            page: PageRequest::new(0, 9999),
        };
        let normalized = query.normalized();
        assert_eq!(normalized.page.page, 1);
        assert_eq!(normalized.page.size, PageRequest::MAX_SIZE);
        assert_eq!(normalized.filter, Some(1));
    }

    #[test]
    fn page_window_hardens_raw_request() {
        let window = PageWindow::harden(PageRequest::new(0, 9999));
        assert_eq!(window.page(), 1);
        assert_eq!(window.size(), PageRequest::MAX_SIZE);
        assert_eq!(window.seaorm_index(), 0);
    }

    #[test]
    fn empty_for_uses_query_window() {
        let query = SmokeQuery {
            filter: None,
            page: PageRequest::new(2, 50),
        };
        let page = Paginated::<i32>::empty_for(&query);
        assert_eq!(page.page, 2);
        assert_eq!(page.size, 50);
        assert!(page.items.is_empty());
    }

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
    fn from_window_reports_the_hardened_window_and_has_next() {
        let window = PageWindow::harden(PageRequest::new(0, 0));
        let page = Paginated::from_window(vec![1, 2], 5, window);
        assert_eq!(page.page, 1);
        assert_eq!(page.size, PageRequest::DEFAULT_SIZE);
        // total (5) <= consumed (1 * 20) -> no further page.
        assert!(!page.has_next);

        let windowed = PageWindow::harden(PageRequest::new(1, 2));
        let windowed_page = Paginated::from_window(vec![1, 2], 5, windowed);
        assert!(windowed_page.has_next);
    }
}
