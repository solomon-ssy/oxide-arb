//! Stable, compile-time-known name newtypes for the research plane.
//!
//! Feature / factor / label names are *stable identifiers*: known at compile
//! time for built-in computations, yet round-trippable through serde (model
//! artifacts persist factor-keyed weights, datasets persist label names).
//! Backing them with `Cow<'static, str>` delivers both — zero-allocation
//! [`from_static`](crate::features::FeatureName::from_static) for built-ins and
//! owned values when deserialized.

/// Generate a `Cow<'static, str>`-backed stable name newtype with the standard
/// constructors, accessor, and `Display`.
macro_rules! stable_name {
    ($(#[$meta:meta])* $name:ident) => {
        $(#[$meta])*
        #[derive(
            Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash,
            ::serde::Serialize, ::serde::Deserialize,
        )]
        #[serde(transparent)]
        pub struct $name(::std::borrow::Cow<'static, str>);

        impl $name {
            /// Construct from a compile-time-known static name (zero allocation).
            #[must_use]
            pub const fn from_static(name: &'static str) -> Self {
                Self(::std::borrow::Cow::Borrowed(name))
            }

            /// Construct from an owned or borrowed runtime string.
            #[must_use]
            pub fn new(name: impl Into<String>) -> Self {
                Self(::std::borrow::Cow::Owned(name.into()))
            }

            /// The name as a string slice.
            #[must_use]
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl ::std::fmt::Display for $name {
            fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
                f.write_str(&self.0)
            }
        }
    };
}

pub(crate) use stable_name;
