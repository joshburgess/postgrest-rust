//! Newtype wrappers for PostgreSQL types that don't map to native Rust types.

/// PostgreSQL `numeric` / `decimal` type, stored as its string representation.
/// Use this when you need exact decimal values without adding a decimal crate.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PgNumeric(pub String);

impl std::fmt::Display for PgNumeric {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<String> for PgNumeric {
    fn from(s: String) -> Self { Self(s) }
}
impl From<&str> for PgNumeric {
    fn from(s: &str) -> Self { Self(s.to_string()) }
}

/// PostgreSQL `inet` type, stored as its string representation (e.g. "192.168.1.1/24").
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PgInet(pub String);

impl std::fmt::Display for PgInet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<String> for PgInet {
    fn from(s: String) -> Self { Self(s) }
}
impl From<&str> for PgInet {
    fn from(s: &str) -> Self { Self(s.to_string()) }
}
