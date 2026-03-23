//! Typed PostgreSQL query layer built on pg-wire.
//!
//! Provides binary-format Encode/Decode traits for zero-overhead type mapping,
//! a typed Row abstraction, FromRow derivation, and an ergonomic query API.
//!
//! # Performance over sqlx
//!
//! - Binary format: PG sends raw bytes, no text→number parsing
//! - Message coalescing: multiple queries batched into one write() syscall
//! - Multiplexed connections: many queries share one TCP connection
//! - Statement caching: Parse once, Bind+Execute on reuse

mod checked;
mod decode;
mod encode;
mod error;
mod oid;
mod query;
mod row;
mod types;

pub use checked::CheckedQuery;
pub use decode::Decode;
pub use encode::{Encode, SqlParam};
pub use error::TypedError;
pub use oid::TypeOid;
pub use query::{Client, Transaction};
pub use row::{Row, FromRow};
/// Derive macro for `FromRow`. Use `#[derive(pg_typed::FromRow)]` on structs.
pub use pg_typed_derive::FromRow;
/// Compile-time checked query macro. Requires `DATABASE_URL` env var.
pub use pg_typed_macros::query;
pub use types::TypeInfo;
