//! Small data-parallel helpers built on `rayon`.
//!
//! These wrap `rayon`'s parallel iterators with **source-order collection**, so
//! a parallel map over a slice yields exactly the ordered `Vec` a serial `map`
//! would. They exist so the research compute plane (e.g. the factor engine's
//! batch grid build) can parallelize pure, deterministic per-element work
//! without re-deriving the same ordered `collect` plumbing at every call site.
//!
//! Determinism is the caller's contract: pass only pure closures. Parallel
//! scheduling then changes evaluation order alone, never the indexed output.

use rayon::prelude::*;

/// Map `f` over `items` in parallel, collecting in source order and
/// short-circuiting on the first error.
///
/// Equivalent to `items.iter.map(f).collect::<Result<Vec<_>, _>>` but spread
/// across the `rayon` pool; the returned `Vec` is index-aligned with `items`.
pub fn par_try_map<T, R, E, F>(items: &[T], f: F) -> Result<Vec<R>, E>
where
    T: Sync,
    R: Send,
    E: Send,
    F: Fn(&T) -> Result<R, E> + Sync + Send,
{
    items.par_iter().map(f).collect()
}

/// Map the infallible `f` over `items` in parallel, passing each element's index
/// alongside it and collecting in source order.
///
/// The index lets a closure read a pre-built parallel structure (e.g. a column
/// at `index`, or a per-market slot) while staying index-aligned with `items`.
pub fn par_map_with_index<T, R, F>(items: &[T], f: F) -> Vec<R>
where
    T: Sync,
    R: Send,
    F: Fn(usize, &T) -> R + Sync + Send,
{
    items
        .par_iter()
        .enumerate()
        .map(|(index, item)| f(index, item))
        .collect()
}

/// Fallible indexed parallel map, preserving source order and propagating the
/// first computation error without manufacturing a result for that element.
pub fn par_try_map_index<T, R, E, F>(items: &[T], f: F) -> Result<Vec<R>, E>
where
    T: Sync,
    R: Send,
    E: Send,
    F: Fn(usize, &T) -> Result<R, E> + Sync + Send,
{
    items
        .par_iter()
        .enumerate()
        .map(|(index, item)| f(index, item))
        .collect()
}
