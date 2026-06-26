//! Canonical column-definition builders for schema iden modules.

use sea_orm::{
    entity::ActiveEnum,
    sea_query::{Alias, ColumnDef, ColumnType, Expr, IntoIden, SimpleExpr},
};

use crate::types::{Bps, Price, Probability, Shares, Usd};

// ── UUID identifier columns ──────────────────────────────────────────────

/// `uuid NOT NULL PRIMARY KEY` for an internal, system-generated identifier.
pub fn uuid_pk(column: impl IntoIden) -> ColumnDef {
    let mut col = ColumnDef::new(column);
    col.uuid().not_null().primary_key();
    col
}

/// `uuid NOT NULL` for a foreign key (or required reference) to an internal id.
pub fn uuid_fk(column: impl IntoIden) -> ColumnDef {
    let mut col = ColumnDef::new(column);
    col.uuid().not_null();
    col
}

/// Nullable `uuid` for an optional reference to an internal id.
pub fn uuid_null(column: impl IntoIden) -> ColumnDef {
    let mut col = ColumnDef::new(column);
    col.uuid().null();
    col
}

// ── External string identifier columns ───────────────────────────────────

/// Maximum length of a Polymarket `condition_id` (`0x` + 64 hex chars).
const MARKET_ID_LEN: u32 = 66;

/// `varchar(66) NOT NULL PRIMARY KEY` for a Polymarket `condition_id`.
///
/// The length bound matches the canonical on-chain `condition_id`. A regex
/// `CHECK` is intentionally omitted: dry-run and paper trading modes persist
/// synthetic market ids that do not match the `0x…` format, and a DB-level
/// regex would reject otherwise valid non-live rows.
pub fn market_id_pk(column: impl IntoIden) -> ColumnDef {
    let mut col = ColumnDef::new(column);
    col.string_len(MARKET_ID_LEN).not_null().primary_key();
    col
}

/// `varchar(66) NOT NULL` for a `MarketId` foreign key or required reference.
pub fn market_id(column: impl IntoIden) -> ColumnDef {
    let mut col = ColumnDef::new(column);
    col.string_len(MARKET_ID_LEN).not_null();
    col
}

/// `text NOT NULL` for a CLOB decimal `TokenId`.
///
/// Token ids are arbitrary-length decimal U256 strings, so no length bound or
/// format `CHECK` applies. Namespace confusion with `MarketId` is guarded at
/// the type layer (`TokenId::debug_validate`).
pub fn token_id(column: impl IntoIden) -> ColumnDef {
    let mut col = ColumnDef::new(column);
    col.text().not_null();
    col
}

/// Nullable `text` for an optional `TokenId`.
pub fn token_id_null(column: impl IntoIden) -> ColumnDef {
    let mut col = ColumnDef::new(column);
    col.text().null();
    col
}

/// `text NOT NULL PRIMARY KEY` for an external/semantic string id.
///
/// Used by `StrId`-backed ids whose value is an unbounded external string
/// (Polymarket `EventId`) or a human-readable natural key (`ReportId`, e.g.
/// `"daily_2025-06-01"`), neither of which is a UUID.
pub fn text_id_pk(column: impl IntoIden) -> ColumnDef {
    let mut col = ColumnDef::new(column);
    col.text().not_null().primary_key();
    col
}

/// `text NOT NULL` for an external/semantic string id reference (e.g. an
/// `EventId` foreign key or a seed-ledger natural key).
pub fn text_id(column: impl IntoIden) -> ColumnDef {
    let mut col = ColumnDef::new(column);
    col.text().not_null();
    col
}

/// Nullable `text` for an optional external/semantic string id (e.g. a
/// venue-assigned `OrderId` that is absent until submission).
pub fn text_id_null(column: impl IntoIden) -> ColumnDef {
    let mut col = ColumnDef::new(column);
    col.text().null();
    col
}

// ── Surrogate / singleton primary keys ───────────────────────────────────

/// `bigint NOT NULL GENERATED … PRIMARY KEY` surrogate key.
///
/// For high-insert rows that are not addressed by a domain id: adapter tables
/// (`casbin_rule`), append-only audit/report rows, and join/outcome rows whose
/// identity is purely a row counter.
pub fn bigserial_pk(column: impl IntoIden) -> ColumnDef {
    let mut col = ColumnDef::new(column);
    col.big_integer().not_null().auto_increment().primary_key();
    col
}

/// `integer NOT NULL GENERATED … PRIMARY KEY` surrogate key for low-cardinality
/// enumeration rows (e.g. calibration buckets).
pub fn int_identity_pk(column: impl IntoIden) -> ColumnDef {
    let mut col = ColumnDef::new(column);
    col.integer().not_null().auto_increment().primary_key();
    col
}

/// `integer NOT NULL PRIMARY KEY` for a singleton row whose id is a fixed
/// application-chosen constant (e.g. the single `risk_engine_state` row, id 1).
pub fn singleton_pk(column: impl IntoIden) -> ColumnDef {
    let mut col = ColumnDef::new(column);
    col.integer().not_null().primary_key();
    col
}

// ── Money columns (native NUMERIC) ───────────────────────────────────────

fn numeric(column: impl IntoIden, precision: (u32, u32)) -> ColumnDef {
    let mut col = ColumnDef::new(column);
    col.decimal_len(precision.0, precision.1).not_null();
    col
}

fn numeric_null(column: impl IntoIden, precision: (u32, u32)) -> ColumnDef {
    let mut col = ColumnDef::new(column);
    col.decimal_len(precision.0, precision.1).null();
    col
}

/// `NUMERIC(28, 8) NOT NULL` for a USD amount.
pub fn usd(column: impl IntoIden) -> ColumnDef {
    numeric(column, Usd::PRECISION)
}

/// Nullable `NUMERIC(28, 8)` for an optional USD amount.
pub fn usd_null(column: impl IntoIden) -> ColumnDef {
    numeric_null(column, Usd::PRECISION)
}

/// `NUMERIC(28, 8) NOT NULL DEFAULT 0` for a USD accumulator.
pub fn usd_default_zero(column: impl IntoIden) -> ColumnDef {
    let mut col = numeric(column, Usd::PRECISION);
    col.default(Usd::ZERO);
    col
}

/// `NUMERIC(20, 18) NOT NULL` for a price in `[0, 1]`.
pub fn price(column: impl IntoIden) -> ColumnDef {
    numeric(column, Price::PRECISION)
}

/// `NUMERIC(38, 18) NOT NULL` for a share quantity.
pub fn shares(column: impl IntoIden) -> ColumnDef {
    numeric(column, Shares::PRECISION)
}

/// `NUMERIC(10, 4) NOT NULL` for a basis-point value.
pub fn bps(column: impl IntoIden) -> ColumnDef {
    numeric(column, Bps::PRECISION)
}

/// `qp_* NOT NULL` for a Postgres native enum column ([`pg_enum!`]).
pub fn pg_enum<E: ActiveEnum>(column: impl IntoIden) -> ColumnDef {
    let mut col = ColumnDef::new(column);
    col.custom(E::name()).not_null();
    col
}

/// Nullable Postgres native enum column.
pub fn pg_enum_null<E: ActiveEnum>(column: impl IntoIden) -> ColumnDef {
    let mut col = ColumnDef::new(column);
    col.custom(E::name()).null();
    col
}

/// Postgres native enum with a typed default.
pub fn pg_enum_default<E: ActiveEnum>(column: impl IntoIden, default: &E) -> ColumnDef {
    let mut col = ColumnDef::new(column);
    col.custom(E::name()).not_null().default(default.to_value());
    col
}

/// Postgres native enum array (`qp_*[]`) with empty-array default.
pub fn pg_enum_array<E: ActiveEnum>(column: impl IntoIden) -> ColumnDef {
    let type_name = E::name().to_string();
    let mut col = ColumnDef::new(column);
    col.array(ColumnType::Custom(E::name().into_iden()))
        .not_null()
        .default(Expr::cust(format!("'{{}}'::{type_name}[]")));
    col
}

/// Cast a Rust pg enum for [`SeaORM`] bulk `UPDATE` expressions.
pub fn pg_enum_value<E: ActiveEnum>(value: &E) -> SimpleExpr {
    let type_name = E::name().to_string();
    Expr::value(value.to_value()).cast_as(Alias::new(type_name))
}

/// Nullable `NUMERIC(10, 4)` for an optional basis-point value.
pub fn bps_null(column: impl IntoIden) -> ColumnDef {
    numeric_null(column, Bps::PRECISION)
}

/// Precision of a signed, unitless ratio / analytical metric column.
///
/// Used by backtest metrics (rank IC, drawdown, turnover, tail loss, coverage)
/// that may be negative or exceed `1`, so neither [`probability`] nor [`bps_null`]
/// applies.
const RATIO_PRECISION: (u32, u32) = (28, 12);

/// `NUMERIC(28, 12) NOT NULL` for a signed, unitless ratio / analytical metric.
pub fn ratio(column: impl IntoIden) -> ColumnDef {
    numeric(column, RATIO_PRECISION)
}

/// `NUMERIC(20, 18) NOT NULL` for a probability / model weight.
pub fn probability(column: impl IntoIden) -> ColumnDef {
    numeric(column, Probability::PRECISION)
}

/// Nullable `NUMERIC(20, 18)` for an optional probability.
pub fn probability_null(column: impl IntoIden) -> ColumnDef {
    numeric_null(column, Probability::PRECISION)
}

/// `NUMERIC(20, 18) NOT NULL DEFAULT 1` for a Beta-prior parameter.
pub fn probability_default_one(column: impl IntoIden) -> ColumnDef {
    let mut col = numeric(column, Probability::PRECISION);
    col.default(Probability::ONE);
    col
}

// ── Casbin policy columns ────────────────────────────────────────────────

/// A Casbin policy value column (`v0..v5`).
///
/// `NOT NULL DEFAULT ''` (rather than nullable) so the `uq_casbin_rule` unique
/// index de-duplicates exactly under standard semantics — Postgres treats
/// NULLs as distinct, which would silently defeat de-duplication on unused
/// fields.
pub fn casbin_policy_text(column: impl IntoIden) -> ColumnDef {
    let mut col = ColumnDef::new(column);
    col.text().not_null().default("");
    col
}
