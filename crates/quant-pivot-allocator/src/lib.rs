//! Process-wide allocator policy.
//!
//! Every runtime crate links this crate so production binaries, tools,
//! benchmarks, unit tests, and integration-test harnesses all exercise the
//! same allocator behavior. The proc-macro crate is a compiler plugin and is
//! intentionally outside the target process allocator graph.

use tikv_jemallocator::Jemalloc;

#[global_allocator]
static GLOBAL_ALLOCATOR: Jemalloc = Jemalloc;

/// Canonical allocator name exposed to diagnostics and architecture checks.
pub const NAME: &str = "tikv-jemalloc";
