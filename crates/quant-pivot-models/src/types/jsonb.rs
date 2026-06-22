//! Declarative helpers for Postgres `JSONB` newtypes used in `SeaORM` entities.
//!
//! `FromJsonQueryResult` covers read/query binding; the macro also emits the
//! trivial `IntoActiveValue` impl required for `ActiveModel` writes.

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

        impl sea_orm::IntoActiveValue<Self> for $name {
            fn into_active_value(self) -> sea_orm::ActiveValue<Self> {
                sea_orm::ActiveValue::Set(self)
            }
        }
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

        impl sea_orm::IntoActiveValue<Self> for $name {
            fn into_active_value(self) -> sea_orm::ActiveValue<Self> {
                sea_orm::ActiveValue::Set(self)
            }
        }
    };
}

pub use jsonb_newtype;
