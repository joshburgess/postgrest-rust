//! Decode PostgreSQL binary wire format into Rust types.
//!
//! Binary format is big-endian for all numeric types.
//! The wire protocol provides raw bytes (length already stripped by parser).

use crate::error::TypedError;

/// Trait for types that can be decoded from PostgreSQL binary format.
pub trait Decode: Sized {
    /// Decode from binary bytes. Returns an error if the bytes are malformed.
    fn decode(buf: &[u8]) -> Result<Self, TypedError>;

    /// Decode from a possibly-NULL column.
    fn decode_option(buf: Option<&[u8]>) -> Result<Option<Self>, TypedError> {
        match buf {
            Some(b) => Ok(Some(Self::decode(b)?)),
            None => Ok(None),
        }
    }
}

// ---------------------------------------------------------------------------
// Primitive implementations
// ---------------------------------------------------------------------------

impl Decode for bool {
    fn decode(buf: &[u8]) -> Result<Self, TypedError> {
        if buf.len() != 1 {
            return Err(TypedError::Decode {
                column: 0,
                message: format!("bool: expected 1 byte, got {}", buf.len()),
            });
        }
        Ok(buf[0] != 0)
    }
}

impl Decode for i16 {
    fn decode(buf: &[u8]) -> Result<Self, TypedError> {
        if buf.len() != 2 {
            return Err(TypedError::Decode {
                column: 0,
                message: format!("i16: expected 2 bytes, got {}", buf.len()),
            });
        }
        Ok(i16::from_be_bytes([buf[0], buf[1]]))
    }
}

impl Decode for i32 {
    fn decode(buf: &[u8]) -> Result<Self, TypedError> {
        if buf.len() != 4 {
            return Err(TypedError::Decode {
                column: 0,
                message: format!("i32: expected 4 bytes, got {}", buf.len()),
            });
        }
        Ok(i32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]))
    }
}

impl Decode for i64 {
    fn decode(buf: &[u8]) -> Result<Self, TypedError> {
        if buf.len() != 8 {
            return Err(TypedError::Decode {
                column: 0,
                message: format!("i64: expected 8 bytes, got {}", buf.len()),
            });
        }
        Ok(i64::from_be_bytes([
            buf[0], buf[1], buf[2], buf[3], buf[4], buf[5], buf[6], buf[7],
        ]))
    }
}

impl Decode for f32 {
    fn decode(buf: &[u8]) -> Result<Self, TypedError> {
        if buf.len() != 4 {
            return Err(TypedError::Decode {
                column: 0,
                message: format!("f32: expected 4 bytes, got {}", buf.len()),
            });
        }
        Ok(f32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]))
    }
}

impl Decode for f64 {
    fn decode(buf: &[u8]) -> Result<Self, TypedError> {
        if buf.len() != 8 {
            return Err(TypedError::Decode {
                column: 0,
                message: format!("f64: expected 8 bytes, got {}", buf.len()),
            });
        }
        Ok(f64::from_be_bytes([
            buf[0], buf[1], buf[2], buf[3], buf[4], buf[5], buf[6], buf[7],
        ]))
    }
}

impl Decode for String {
    fn decode(buf: &[u8]) -> Result<Self, TypedError> {
        String::from_utf8(buf.to_vec()).map_err(|e| TypedError::Decode {
            column: 0,
            message: format!("String: invalid UTF-8: {e}"),
        })
    }
}

impl Decode for Vec<u8> {
    fn decode(buf: &[u8]) -> Result<Self, TypedError> {
        Ok(buf.to_vec())
    }
}

// ---------------------------------------------------------------------------
// Text-format fallback decode
// ---------------------------------------------------------------------------

/// Decode from PostgreSQL text format (for backwards compat and mixed-mode queries).
pub trait DecodeText: Sized {
    fn decode_text(s: &str) -> Result<Self, TypedError>;
}

impl DecodeText for bool {
    fn decode_text(s: &str) -> Result<Self, TypedError> {
        match s {
            "t" | "true" | "1" => Ok(true),
            "f" | "false" | "0" => Ok(false),
            _ => Err(TypedError::Decode { column: 0, message: format!("bool: {s:?}") }),
        }
    }
}

impl DecodeText for i16 {
    fn decode_text(s: &str) -> Result<Self, TypedError> {
        s.parse().map_err(|_| TypedError::Decode { column: 0, message: format!("i16: {s:?}") })
    }
}

impl DecodeText for i32 {
    fn decode_text(s: &str) -> Result<Self, TypedError> {
        s.parse().map_err(|_| TypedError::Decode { column: 0, message: format!("i32: {s:?}") })
    }
}

impl DecodeText for i64 {
    fn decode_text(s: &str) -> Result<Self, TypedError> {
        s.parse().map_err(|_| TypedError::Decode { column: 0, message: format!("i64: {s:?}") })
    }
}

impl DecodeText for f32 {
    fn decode_text(s: &str) -> Result<Self, TypedError> {
        s.parse().map_err(|_| TypedError::Decode { column: 0, message: format!("f32: {s:?}") })
    }
}

impl DecodeText for f64 {
    fn decode_text(s: &str) -> Result<Self, TypedError> {
        s.parse().map_err(|_| TypedError::Decode { column: 0, message: format!("f64: {s:?}") })
    }
}

impl DecodeText for String {
    fn decode_text(s: &str) -> Result<Self, TypedError> {
        Ok(s.to_string())
    }
}
