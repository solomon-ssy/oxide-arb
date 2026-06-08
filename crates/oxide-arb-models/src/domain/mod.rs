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
//! ## Write DTOs (`New*`, `Patch*`, `Upsert*`)
//!
//! - **`New{Entity}`** — Insert payload. Derives `DeriveIntoActiveModel`.
//!   Database-managed write timestamps are omitted so Postgres defaults/triggers
//!   remain the single source of truth.
//! - **`New{Entity}WithId`** — Insert payload where the caller assigns the PK.
//! - **`{Entity}Patch`** — Partial update. Uses `Patch<T>` for non-nullable
//!   columns and `NullablePatch<T>` for nullable columns, so write intent is
//!   explicit: keep, set, or clear.
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

// Bounded-context groups.
pub mod accounting;
pub mod control_factor;
pub mod evidence;
pub mod governance;
pub mod market;
pub mod rbac;
pub mod risk;
pub mod trading;

// Cross-cutting helpers shared by every context.
pub mod pagination;
pub mod patch;

// Flattened facade: every domain type is reachable directly under `domain::`.
pub use accounting::*;
pub use control_factor::*;
pub use evidence::*;
pub use governance::*;
pub use market::*;
pub use pagination::*;
pub use patch::*;
pub use rbac::*;
pub use risk::*;
pub use trading::*;
