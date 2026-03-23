//! PostgreSQL type OID constants.
//!
//! These match the OIDs in pg_type. When sending Bind with binary params,
//! the OID tells PostgreSQL how to interpret the bytes.

/// Known PostgreSQL type OIDs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u32)]
pub enum TypeOid {
    Bool = 16,
    Bytea = 17,
    Int8 = 20,
    Int2 = 21,
    Int4 = 23,
    Text = 25,
    Oid = 26,
    Float4 = 700,
    Float8 = 701,
    Varchar = 1043,
    Char = 18,
    Name = 19,
    Timestamp = 1114,
    Timestamptz = 1184,
    Date = 1082,
    Time = 1083,
    Interval = 1186,
    Uuid = 2950,
    Json = 114,
    Jsonb = 3802,
    BoolArray = 1000,
    Int2Array = 1005,
    Int4Array = 1007,
    Int8Array = 1016,
    Float4Array = 1021,
    Float8Array = 1022,
    TextArray = 1009,
    VarcharArray = 1015,
}

impl TypeOid {
    pub fn as_u32(self) -> u32 {
        self as u32
    }
}

impl From<TypeOid> for u32 {
    fn from(oid: TypeOid) -> u32 {
        oid as u32
    }
}
