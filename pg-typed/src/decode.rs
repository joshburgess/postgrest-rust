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
// Chrono types (behind "chrono" feature)
// ---------------------------------------------------------------------------

#[cfg(feature = "chrono")]
const PG_EPOCH_OFFSET_US: i64 = 946_684_800_000_000;

#[cfg(feature = "chrono")]
impl Decode for chrono::NaiveDateTime {
    fn decode(buf: &[u8]) -> Result<Self, TypedError> {
        let us = i64::decode(buf)?;
        let unix_us = us + PG_EPOCH_OFFSET_US;
        chrono::DateTime::from_timestamp_micros(unix_us)
            .map(|dt| dt.naive_utc())
            .ok_or_else(|| TypedError::Decode {
                column: 0,
                message: format!("NaiveDateTime: invalid microseconds {us}"),
            })
    }
}

#[cfg(feature = "chrono")]
impl Decode for chrono::DateTime<chrono::Utc> {
    fn decode(buf: &[u8]) -> Result<Self, TypedError> {
        let us = i64::decode(buf)?;
        let unix_us = us + PG_EPOCH_OFFSET_US;
        chrono::DateTime::from_timestamp_micros(unix_us).ok_or_else(|| TypedError::Decode {
            column: 0,
            message: format!("DateTime<Utc>: invalid microseconds {us}"),
        })
    }
}

#[cfg(feature = "chrono")]
impl Decode for chrono::NaiveDate {
    fn decode(buf: &[u8]) -> Result<Self, TypedError> {
        let days = i32::decode(buf)?;
        let pg_epoch = chrono::NaiveDate::from_ymd_opt(2000, 1, 1).unwrap();
        pg_epoch
            .checked_add_signed(chrono::Duration::days(days as i64))
            .ok_or_else(|| TypedError::Decode {
                column: 0,
                message: format!("NaiveDate: invalid days offset {days}"),
            })
    }
}

#[cfg(feature = "chrono")]
impl Decode for chrono::NaiveTime {
    fn decode(buf: &[u8]) -> Result<Self, TypedError> {
        let us = i64::decode(buf)?;
        let secs = (us / 1_000_000) as u32;
        let nano = ((us % 1_000_000) * 1000) as u32;
        chrono::NaiveTime::from_num_seconds_from_midnight_opt(secs, nano).ok_or_else(|| {
            TypedError::Decode {
                column: 0,
                message: format!("NaiveTime: invalid microseconds {us}"),
            }
        })
    }
}

// ---------------------------------------------------------------------------
// JSON types (behind "json" feature)
// ---------------------------------------------------------------------------

#[cfg(feature = "json")]
impl Decode for serde_json::Value {
    fn decode(buf: &[u8]) -> Result<Self, TypedError> {
        // JSONB binary: first byte is version (1), rest is JSON text.
        // JSON binary: just JSON text.
        let text = if !buf.is_empty() && buf[0] == 1 {
            &buf[1..] // Skip JSONB version byte.
        } else {
            buf
        };
        serde_json::from_slice(text).map_err(|e| TypedError::Decode {
            column: 0,
            message: format!("JSON: {e}"),
        })
    }
}

// ---------------------------------------------------------------------------
// UUID (behind "uuid" feature)
// ---------------------------------------------------------------------------

#[cfg(feature = "uuid")]
impl Decode for uuid::Uuid {
    fn decode(buf: &[u8]) -> Result<Self, TypedError> {
        if buf.len() != 16 {
            return Err(TypedError::Decode {
                column: 0,
                message: format!("UUID: expected 16 bytes, got {}", buf.len()),
            });
        }
        Ok(uuid::Uuid::from_bytes(buf.try_into().unwrap()))
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

impl DecodeText for Vec<u8> {
    fn decode_text(s: &str) -> Result<Self, TypedError> {
        // PG text format for bytea: hex encoding (\x...) or escape format.
        if let Some(hex) = s.strip_prefix("\\x") {
            (0..hex.len())
                .step_by(2)
                .map(|i| {
                    u8::from_str_radix(&hex[i..i + 2], 16).map_err(|_| TypedError::Decode {
                        column: 0,
                        message: format!("bytea: invalid hex at offset {i}"),
                    })
                })
                .collect()
        } else {
            Ok(s.as_bytes().to_vec())
        }
    }
}

#[cfg(feature = "chrono")]
impl DecodeText for chrono::NaiveDateTime {
    fn decode_text(s: &str) -> Result<Self, TypedError> {
        s.parse().map_err(|e| TypedError::Decode {
            column: 0,
            message: format!("NaiveDateTime: {e}"),
        })
    }
}

#[cfg(feature = "chrono")]
impl DecodeText for chrono::DateTime<chrono::Utc> {
    fn decode_text(s: &str) -> Result<Self, TypedError> {
        s.parse().map_err(|e| TypedError::Decode {
            column: 0,
            message: format!("DateTime<Utc>: {e}"),
        })
    }
}

#[cfg(feature = "chrono")]
impl DecodeText for chrono::NaiveDate {
    fn decode_text(s: &str) -> Result<Self, TypedError> {
        s.parse().map_err(|e| TypedError::Decode {
            column: 0,
            message: format!("NaiveDate: {e}"),
        })
    }
}

#[cfg(feature = "chrono")]
impl DecodeText for chrono::NaiveTime {
    fn decode_text(s: &str) -> Result<Self, TypedError> {
        s.parse().map_err(|e| TypedError::Decode {
            column: 0,
            message: format!("NaiveTime: {e}"),
        })
    }
}

#[cfg(feature = "json")]
impl DecodeText for serde_json::Value {
    fn decode_text(s: &str) -> Result<Self, TypedError> {
        serde_json::from_str(s).map_err(|e| TypedError::Decode {
            column: 0,
            message: format!("JSON: {e}"),
        })
    }
}

#[cfg(feature = "uuid")]
impl DecodeText for uuid::Uuid {
    fn decode_text(s: &str) -> Result<Self, TypedError> {
        s.parse().map_err(|e| TypedError::Decode {
            column: 0,
            message: format!("UUID: {e}"),
        })
    }
}
