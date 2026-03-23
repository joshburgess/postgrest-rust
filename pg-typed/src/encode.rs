//! Encode Rust types into PostgreSQL binary wire format.
//!
//! Binary format is big-endian for all numeric types.
//! The wire protocol sends: 4-byte length prefix + raw bytes.
//! Encode only produces the raw bytes; the length prefix is handled by the caller.

use bytes::{BufMut, BytesMut};

use crate::oid::TypeOid;

/// Trait for types that can be encoded into PostgreSQL binary format.
pub trait Encode {
    /// The PostgreSQL type OID for this Rust type.
    fn type_oid(&self) -> TypeOid;

    /// Encode the value into binary format, appending to `buf`.
    fn encode(&self, buf: &mut BytesMut);

    /// Encode as a binary parameter: 4-byte length + data.
    /// Returns None for NULL values (handled by Option<T>).
    fn encode_param(&self, buf: &mut BytesMut) {
        let start = buf.len();
        buf.put_i32(0); // placeholder for length
        self.encode(buf);
        let len = (buf.len() - start - 4) as i32;
        let start_bytes = &mut buf[start..start + 4];
        start_bytes.copy_from_slice(&len.to_be_bytes());
    }
}

// ---------------------------------------------------------------------------
// Primitive implementations
// ---------------------------------------------------------------------------

impl Encode for bool {
    fn type_oid(&self) -> TypeOid { TypeOid::Bool }
    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u8(if *self { 1 } else { 0 });
    }
}

impl Encode for i16 {
    fn type_oid(&self) -> TypeOid { TypeOid::Int2 }
    fn encode(&self, buf: &mut BytesMut) {
        buf.put_i16(*self);
    }
}

impl Encode for i32 {
    fn type_oid(&self) -> TypeOid { TypeOid::Int4 }
    fn encode(&self, buf: &mut BytesMut) {
        buf.put_i32(*self);
    }
}

impl Encode for i64 {
    fn type_oid(&self) -> TypeOid { TypeOid::Int8 }
    fn encode(&self, buf: &mut BytesMut) {
        buf.put_i64(*self);
    }
}

impl Encode for f32 {
    fn type_oid(&self) -> TypeOid { TypeOid::Float4 }
    fn encode(&self, buf: &mut BytesMut) {
        buf.put_f32(*self);
    }
}

impl Encode for f64 {
    fn type_oid(&self) -> TypeOid { TypeOid::Float8 }
    fn encode(&self, buf: &mut BytesMut) {
        buf.put_f64(*self);
    }
}

impl Encode for str {
    fn type_oid(&self) -> TypeOid { TypeOid::Text }
    fn encode(&self, buf: &mut BytesMut) {
        buf.put_slice(self.as_bytes());
    }
}

impl Encode for String {
    fn type_oid(&self) -> TypeOid { TypeOid::Text }
    fn encode(&self, buf: &mut BytesMut) {
        buf.put_slice(self.as_bytes());
    }
}

impl Encode for &str {
    fn type_oid(&self) -> TypeOid { TypeOid::Text }
    fn encode(&self, buf: &mut BytesMut) {
        buf.put_slice(self.as_bytes());
    }
}

impl Encode for [u8] {
    fn type_oid(&self) -> TypeOid { TypeOid::Bytea }
    fn encode(&self, buf: &mut BytesMut) {
        buf.put_slice(self);
    }
}

impl Encode for Vec<u8> {
    fn type_oid(&self) -> TypeOid { TypeOid::Bytea }
    fn encode(&self, buf: &mut BytesMut) {
        buf.put_slice(self);
    }
}

// ---------------------------------------------------------------------------
// Option<T>: NULL encoding
// ---------------------------------------------------------------------------

/// Encode helper that wraps a value as a binary parameter.
/// Returns the OID and the encoded bytes (or None for NULL).
pub fn encode_value<T: Encode>(val: &T) -> (u32, BytesMut) {
    let mut buf = BytesMut::new();
    val.encode(&mut buf);
    (val.type_oid().as_u32(), buf)
}

/// Encode a possibly-NULL value.
pub fn encode_option<T: Encode>(val: &Option<T>) -> (u32, Option<BytesMut>) {
    match val {
        Some(v) => {
            let mut buf = BytesMut::new();
            v.encode(&mut buf);
            (v.type_oid().as_u32(), Some(buf))
        }
        None => (0, None),
    }
}
