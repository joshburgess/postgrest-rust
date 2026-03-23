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
// Newtype wrappers (numeric, inet)
// ---------------------------------------------------------------------------

impl Decode for crate::newtypes::PgNumeric {
    fn decode(buf: &[u8]) -> Result<Self, TypedError> {
        // PG binary numeric: ndigits(i16) weight(i16) sign(i16) dscale(i16) digits[](i16)
        if buf.len() < 8 {
            return Err(TypedError::Decode {
                column: 0,
                message: format!("PgNumeric: expected >= 8 bytes, got {}", buf.len()),
            });
        }
        let ndigits = i16::from_be_bytes([buf[0], buf[1]]) as usize;
        let weight = i16::from_be_bytes([buf[2], buf[3]]);
        let sign = i16::from_be_bytes([buf[4], buf[5]]);
        let dscale = i16::from_be_bytes([buf[6], buf[7]]) as usize;

        if buf.len() < 8 + ndigits * 2 {
            return Err(TypedError::Decode {
                column: 0,
                message: "PgNumeric: truncated digit data".into(),
            });
        }

        // Special cases.
        if ndigits == 0 {
            return if dscale > 0 {
                let zeros: String = std::iter::repeat('0').take(dscale).collect();
                Ok(crate::newtypes::PgNumeric(format!("0.{zeros}")))
            } else {
                Ok(crate::newtypes::PgNumeric("0".into()))
            };
        }

        let mut digits = Vec::with_capacity(ndigits);
        for i in 0..ndigits {
            let off = 8 + i * 2;
            digits.push(i16::from_be_bytes([buf[off], buf[off + 1]]));
        }

        // Build the string. Each digit is 0-9999 (base-10000).
        // weight = position of first digit group (0 = units, 1 = ten-thousands, etc.).
        let mut result = String::new();
        if sign == 0x4000 {
            result.push('-');
        }

        // Integer part: digit groups at positions weight down to 0.
        let int_groups = (weight + 1).max(0) as usize;
        for i in 0..int_groups {
            let d = if i < ndigits { digits[i] } else { 0 };
            if i == 0 {
                result.push_str(&d.to_string());
            } else {
                result.push_str(&format!("{d:04}"));
            }
        }
        if int_groups == 0 {
            result.push('0');
        }

        // Fractional part.
        if dscale > 0 {
            result.push('.');
            let mut frac_chars = 0;
            for i in int_groups..ndigits {
                let d = digits[i];
                let s = format!("{d:04}");
                for ch in s.chars() {
                    if frac_chars >= dscale {
                        break;
                    }
                    result.push(ch);
                    frac_chars += 1;
                }
            }
            // Pad with zeros if needed.
            while frac_chars < dscale {
                result.push('0');
                frac_chars += 1;
            }
        }

        Ok(crate::newtypes::PgNumeric(result))
    }
}

impl Decode for crate::newtypes::PgInet {
    fn decode(buf: &[u8]) -> Result<Self, TypedError> {
        // PG inet binary: family(u8) netmask(u8) is_cidr(u8) addr_len(u8) addr[addr_len]
        if buf.len() < 4 {
            return Err(TypedError::Decode {
                column: 0,
                message: "PgInet: too short".into(),
            });
        }
        let family = buf[0];
        let netmask = buf[1];
        let addr_len = buf[3] as usize;
        if buf.len() < 4 + addr_len {
            return Err(TypedError::Decode {
                column: 0,
                message: "PgInet: truncated address".into(),
            });
        }
        let addr = &buf[4..4 + addr_len];
        let s = match family {
            2 if addr_len == 4 => {
                format!("{}.{}.{}.{}/{netmask}", addr[0], addr[1], addr[2], addr[3])
            }
            3 if addr_len == 16 => {
                let mut parts = Vec::new();
                for i in (0..16).step_by(2) {
                    parts.push(format!("{:x}", u16::from_be_bytes([addr[i], addr[i + 1]])));
                }
                format!("{}/{netmask}", parts.join(":"))
            }
            _ => {
                return Err(TypedError::Decode {
                    column: 0,
                    message: format!("PgInet: unknown family {family}"),
                });
            }
        };
        Ok(crate::newtypes::PgInet(s))
    }
}

// ---------------------------------------------------------------------------
// Array types
// ---------------------------------------------------------------------------

/// Parse a PG array header, returns (element_oid, num_elements).
fn parse_array_header(buf: &[u8]) -> Result<(u32, usize, usize), TypedError> {
    if buf.len() < 12 {
        return Err(TypedError::Decode {
            column: 0,
            message: "array: header too short".into(),
        });
    }
    let ndim = i32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
    // let _has_null = i32::from_be_bytes([buf[4], buf[5], buf[6], buf[7]]);
    let element_oid = u32::from_be_bytes([buf[8], buf[9], buf[10], buf[11]]);
    if ndim == 0 {
        return Ok((element_oid, 0, 12)); // Empty array: just the 12-byte header.
    }
    if ndim != 1 {
        return Err(TypedError::Decode {
            column: 0,
            message: format!("array: only 1D arrays supported, got {ndim}D"),
        });
    }
    if buf.len() < 20 {
        return Err(TypedError::Decode {
            column: 0,
            message: "array: 1D header too short".into(),
        });
    }
    let dim_len = i32::from_be_bytes([buf[12], buf[13], buf[14], buf[15]]) as usize;
    // skip lower_bound at bytes 16..20
    Ok((element_oid, dim_len, 20))
}

impl Decode for Vec<i32> {
    fn decode(buf: &[u8]) -> Result<Self, TypedError> {
        let (_, count, mut offset) = parse_array_header(buf)?;
        let mut result = Vec::with_capacity(count);
        for _ in 0..count {
            let len = i32::from_be_bytes([buf[offset], buf[offset+1], buf[offset+2], buf[offset+3]]);
            offset += 4;
            if len == -1 {
                return Err(TypedError::Decode { column: 0, message: "array: unexpected NULL in Vec<i32>".into() });
            }
            result.push(i32::from_be_bytes([buf[offset], buf[offset+1], buf[offset+2], buf[offset+3]]));
            offset += 4;
        }
        Ok(result)
    }
}

impl Decode for Vec<i64> {
    fn decode(buf: &[u8]) -> Result<Self, TypedError> {
        let (_, count, mut offset) = parse_array_header(buf)?;
        let mut result = Vec::with_capacity(count);
        for _ in 0..count {
            let len = i32::from_be_bytes([buf[offset], buf[offset+1], buf[offset+2], buf[offset+3]]);
            offset += 4;
            if len == -1 {
                return Err(TypedError::Decode { column: 0, message: "array: unexpected NULL in Vec<i64>".into() });
            }
            result.push(i64::from_be_bytes([
                buf[offset], buf[offset+1], buf[offset+2], buf[offset+3],
                buf[offset+4], buf[offset+5], buf[offset+6], buf[offset+7],
            ]));
            offset += 8;
        }
        Ok(result)
    }
}

impl Decode for Vec<String> {
    fn decode(buf: &[u8]) -> Result<Self, TypedError> {
        let (_, count, mut offset) = parse_array_header(buf)?;
        let mut result = Vec::with_capacity(count);
        for _ in 0..count {
            let len = i32::from_be_bytes([buf[offset], buf[offset+1], buf[offset+2], buf[offset+3]]);
            offset += 4;
            if len == -1 {
                return Err(TypedError::Decode { column: 0, message: "array: unexpected NULL in Vec<String>".into() });
            }
            let len = len as usize;
            let s = String::from_utf8(buf[offset..offset+len].to_vec())
                .map_err(|e| TypedError::Decode { column: 0, message: format!("array: {e}") })?;
            result.push(s);
            offset += len;
        }
        Ok(result)
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

impl DecodeText for crate::newtypes::PgNumeric {
    fn decode_text(s: &str) -> Result<Self, TypedError> {
        Ok(crate::newtypes::PgNumeric(s.to_string()))
    }
}

impl DecodeText for crate::newtypes::PgInet {
    fn decode_text(s: &str) -> Result<Self, TypedError> {
        Ok(crate::newtypes::PgInet(s.to_string()))
    }
}

impl DecodeText for Vec<i32> {
    fn decode_text(s: &str) -> Result<Self, TypedError> {
        // PG text array: {1,2,3}
        let inner = s.trim_start_matches('{').trim_end_matches('}');
        if inner.is_empty() { return Ok(Vec::new()); }
        inner.split(',')
            .map(|v| v.trim().parse::<i32>().map_err(|_| TypedError::Decode {
                column: 0, message: format!("array element: {v:?}"),
            }))
            .collect()
    }
}

impl DecodeText for Vec<i64> {
    fn decode_text(s: &str) -> Result<Self, TypedError> {
        let inner = s.trim_start_matches('{').trim_end_matches('}');
        if inner.is_empty() { return Ok(Vec::new()); }
        inner.split(',')
            .map(|v| v.trim().parse::<i64>().map_err(|_| TypedError::Decode {
                column: 0, message: format!("array element: {v:?}"),
            }))
            .collect()
    }
}

impl DecodeText for Vec<String> {
    fn decode_text(s: &str) -> Result<Self, TypedError> {
        let inner = s.trim_start_matches('{').trim_end_matches('}');
        if inner.is_empty() { return Ok(Vec::new()); }
        // Simple parse — doesn't handle quoted strings with commas inside.
        Ok(inner.split(',').map(|v| {
            let v = v.trim();
            // Remove surrounding quotes if present.
            if v.starts_with('"') && v.ends_with('"') {
                v[1..v.len()-1].replace("\\\"", "\"").to_string()
            } else {
                v.to_string()
            }
        }).collect())
    }
}
