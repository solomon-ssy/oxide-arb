//! Declarative helpers for Postgres `JSONB` newtypes used in `SeaORM` entities.
//!
//! [`jsonb_newtype!`] defines a JSONB-backed struct or tuple newtype with
//! `FromJsonQueryResult`. `SeaORM` 2 derives the active-value binding together
//! with the JSON query-result binding.

/// Define a JSONB-backed newtype with `SeaORM` read/write bindings.
#[macro_export]
macro_rules! jsonb_newtype {
    (
        $(#[$meta:meta])*
        $vis:vis struct $name:ident ( $inner:ty );
    ) => {
        $(#[$meta])*
        #[derive(
            Clone,
            Debug,
            PartialEq,
            Eq,
            serde::Serialize,
            serde::Deserialize,
            sea_orm::FromJsonQueryResult,
        )]
        $vis struct $name(pub $inner);
    };

    (
        $(#[$meta:meta])*
        $vis:vis struct $name:ident {
            $($field:ident: $ty:ty),* $(,)?
        }
    ) => {
        $(#[$meta])*
        #[derive(
            Clone,
            Debug,
            PartialEq,
            Eq,
            serde::Serialize,
            serde::Deserialize,
            sea_orm::FromJsonQueryResult,
        )]
        $vis struct $name {
            $(pub $field: $ty,)*
        }
    };
}
