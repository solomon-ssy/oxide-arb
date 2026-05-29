//! Domain models and DTOs — the canonical contract between business and persistence layers.
//!
//! # Paradigm
//!
//! Every domain file follows a fixed three-section layout:
//!
//! ## Read models (`*Info`)
//!
//! - **`{Entity}Info`** — DB row projection returned by Repository `find_*` methods.
//!   Prefer `DerivePartialModel` + `FromQueryResult` when fields align 1:1 with
//!   entity columns; fall back to `impl From<entity::Model>` otherwise.
//! - **`{Entity}RegistryInfo`** — In-memory enriched view (e.g. runtime book data).
//!   Not persisted directly; converted to write DTOs for persistence.
//!
//! ## Write DTOs (`New*`, `Update*`, `Upsert*`)
//!
//! - **`New{Entity}`** — Insert payload. Derives `DeriveIntoActiveModel`. Entity
//!   defaults may fill generated IDs and insert-only values; DB defaults/triggers
//!   own database-managed write timestamps.
//! - **`New{Entity}WithId`** — Insert payload where the caller assigns the PK.
//! - **`Update{Entity}`** — Partial update. Fields are `Option<T>` for selective
//!   patching; the repository fetches, patches, and persists internally.
//! - **`Upsert{Entity}`** — `ON CONFLICT DO UPDATE` payload. Derives
//!   `DeriveIntoActiveModel`. Contains the conflict key and all updateable columns.
//!
//! ## Runtime types (not persisted)
//!
//! - **`*Snapshot`** — Audit-time freeze of transient state (e.g. calibration
//!   params at detection time). No backing table.
//! - **`*State`** — Engine/runtime aggregate (e.g. `RiskEngineState`). Converted
//!   to `Upsert*` for persistence via `From`.
//! - **`PostTradeInput`** — Cross-crate input for domain operations.
//!
//! ## Conversions
//!
//! - `State → Upsert*` via `From` (complete, no silent defaults).
//! - `RegistryInfo → Upsert*` via `TryFrom` (shape transformation).
//! - `Info → RegistryInfo` via `From` (enrichment with runtime defaults).
//! - `StateInfo → State` via `From` (recovery from DB).
//!
//! # Rules
//!
//! - **No `ActiveModel` / `ActiveValue`** in any public signature or field.
//! - **No `to_active_model()`** methods — `DeriveIntoActiveModel` handles this.
//! - Repository traits use only these 5 write verbs: `create`, `create_batch`,
//!   `update`, `upsert`, `upsert_batch`.
//! - Port traits (e.g. `RiskPersistence`) use domain verbs: `upsert_state`,
//!   `create_audit`, `load_blacklist`, etc. Write methods accept only `New*` /
//!   `Upsert*` DTOs — never `*Info`.
//! - All DB read models end in `Info`; runtime aggregates end in `State`; frozen
//!   audit captures end in `Snapshot`.
/// Generate `impl From<$model> for $info` by copying all named fields.
///
/// Use when `DerivePartialModel` is present (provides `FromQueryResult` +
/// partial-select) but `From<Model>` is still needed for insert-return paths.
macro_rules! info_from_model {
    ($info:ty, $model:ty, { $($field:ident),* $(,)? }) => {
        impl From<$model> for $info {
            fn from(m: $model) -> Self {
                Self { $($field: m.$field),* }
            }
        }
    };
}

pub mod blacklist;
pub mod book;
pub mod calibration;
pub mod execution;
pub mod fee;
pub mod latency;
pub mod market;
pub mod opportunity;
pub mod order;
pub mod pipeline;
pub mod pnl;
pub mod position;
pub mod potential_loss;
pub mod report;
pub mod risk;
pub mod scored_snapshot;
pub mod settlement;
pub mod system;
pub mod trade;

pub use blacklist::*;
pub use book::*;
pub use calibration::*;
pub use execution::*;
pub use latency::*;
pub use market::*;
pub use opportunity::*;
pub use order::*;
pub use pipeline::*;
pub use pnl::*;
pub use position::*;
pub use potential_loss::*;
pub use report::*;
pub use risk::*;
pub use scored_snapshot::*;
pub use system::*;
pub use trade::*;
