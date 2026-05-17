//! Newtype identifiers for the `resolved` module.
//!
//! Each ID is a thin wrapper over its underlying primitive (`String` or `i64`)
//! so the type system rejects accidental mixups between, say, a `StageId` and
//! a `SqlExecId`. Conversion methods are explicit (`into_inner` /
//! `From<primitive>`) to keep the boundary visible.

use serde::{Deserialize, Serialize};
use std::fmt;

macro_rules! string_newtype {
    ($name:ident, $doc:literal) => {
        #[doc = $doc]
        #[derive(Debug, Clone, Hash, PartialEq, Eq, Serialize, Deserialize)]
        pub struct $name(String);

        impl $name {
            #[inline]
            pub fn new(value: impl Into<String>) -> Self {
                Self(value.into())
            }

            #[inline]
            pub fn into_inner(self) -> String {
                self.0
            }

            #[inline]
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl AsRef<str> for $name {
            #[inline]
            fn as_ref(&self) -> &str {
                &self.0
            }
        }

        impl From<&str> for $name {
            #[inline]
            fn from(value: &str) -> Self {
                Self(value.to_string())
            }
        }

        impl From<String> for $name {
            #[inline]
            fn from(value: String) -> Self {
                Self(value)
            }
        }

        impl fmt::Display for $name {
            #[inline]
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                fmt::Display::fmt(&self.0, f)
            }
        }
    };
}

macro_rules! i64_newtype {
    ($name:ident, $doc:literal) => {
        #[doc = $doc]
        #[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, Serialize, Deserialize)]
        pub struct $name(i64);

        impl $name {
            #[inline]
            pub fn new(value: i64) -> Self {
                Self(value)
            }

            #[inline]
            pub fn into_inner(self) -> i64 {
                self.0
            }
        }

        impl From<i64> for $name {
            #[inline]
            fn from(value: i64) -> Self {
                Self(value)
            }
        }

        impl fmt::Display for $name {
            #[inline]
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                fmt::Display::fmt(&self.0, f)
            }
        }
    };
}

string_newtype!(
    StaticNodeId,
    "Identifier for a node in the canonical static `Graph`."
);
i64_newtype!(
    StageId,
    "Spark REST stage identifier (`/api/v1/applications/.../stages/{id}`)."
);
i64_newtype!(
    SqlExecId,
    "Spark REST SQL execution identifier (`/api/v1/applications/.../sql/{id}`)."
);
i64_newtype!(
    PlanNodeId,
    "Identifier of a single node inside a Catalyst physical plan tree."
);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn string_newtype_roundtrip_and_as_ref() {
        let id = StaticNodeId::new("sql_node_1");
        assert_eq!(id.as_str(), "sql_node_1");
        assert_eq!(<StaticNodeId as AsRef<str>>::as_ref(&id), "sql_node_1");
        let back: String = id.clone().into_inner();
        assert_eq!(back, "sql_node_1");
    }

    #[test]
    fn i64_newtype_roundtrip() {
        let s = StageId::from(42i64);
        assert_eq!(s.into_inner(), 42);
        let exec = SqlExecId::new(7);
        assert_eq!(format!("{exec}"), "7");
    }

    #[test]
    fn distinct_id_types_do_not_unify() {
        // Compile-time only: ensure StageId and SqlExecId are different types
        // by requiring an explicit conversion.
        let stage: StageId = 1.into();
        let exec: SqlExecId = 1.into();
        assert_eq!(stage.into_inner(), exec.into_inner());
    }
}
