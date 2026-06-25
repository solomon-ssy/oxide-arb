//! Declarative helpers for Postgres `JSONB` newtypes used in `SeaORM` entities.
//!
//! [`jsonb_active!`] emits the trivial `IntoActiveValue` impl required for
//! `ActiveModel` writes. [`jsonb_newtype!`] defines a JSONB-backed struct or
//! tuple newtype with `FromJsonQueryResult` and delegates the active binding to
//! [`jsonb_active!`].

/// Emit the trivial `IntoActiveValue` impl required to `Set` a JSONB column type
/// on an `ActiveModel` (read binding comes from `FromJsonQueryResult`).
#[macro_export]
macro_rules! jsonb_active {
    ($($name:ty),+ $(,)?) => {
        $(
            impl sea_orm::IntoActiveValue<Self> for $name {
                fn into_active_value(self) -> sea_orm::ActiveValue<Self> {
                    sea_orm::ActiveValue::Set(self)
                }
            }
        )+
    };
}

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

        $crate::jsonb_active!($name);
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

        $crate::jsonb_active!($name);
    };
}

pub use jsonb_active;
pub use jsonb_newtype;
