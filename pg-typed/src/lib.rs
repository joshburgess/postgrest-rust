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

mod decode;
mod encode;
mod error;
mod oid;
mod query;
mod row;
mod types;

pub use decode::Decode;
pub use encode::Encode;
pub use error::TypedError;
pub use oid::TypeOid;
pub use query::{Client, Transaction};
pub use row::{Row, FromRow};
pub use types::TypeInfo;
