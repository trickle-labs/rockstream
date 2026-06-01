//! Schema types: column definitions, data types, and versioned schemas.
//!
//! A `SchemaVersion` is the canonical representation of a source or view
//! schema stored in the catalog. Every schema carries a monotone `version`
//! counter that is incremented on every compatible change.

use serde::{Deserialize, Serialize};

/// Supported column data types in the RockStream catalog.
///
/// This is a closed enum — new types require a catalog format version bump.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DataType {
    /// Boolean (true / false).
    Boolean,
    /// 32-bit signed integer.
    Int32,
    /// 64-bit signed integer.
    Int64,
    /// 32-bit IEEE 754 float.
    Float32,
    /// 64-bit IEEE 754 float.
    Float64,
    /// UTF-8 string.
    Utf8,
    /// Opaque byte array.
    Binary,
    /// Milliseconds since Unix epoch (UTC).
    TimestampMs,
    /// PNCounter (COUNTER)
    Counter,
    /// MaxRegister (MAX_REGISTER)
    MaxRegister,
    /// MinRegister (MIN_REGISTER)
    MinRegister,
    /// LWW (LWW)
    Lww,
}

impl DataType {
    /// Returns the canonical wire name for this data type.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Boolean => "Boolean",
            Self::Int32 => "Int32",
            Self::Int64 => "Int64",
            Self::Float32 => "Float32",
            Self::Float64 => "Float64",
            Self::Utf8 => "Utf8",
            Self::Binary => "Binary",
            Self::TimestampMs => "TimestampMs",
            Self::Counter => "Counter",
            Self::MaxRegister => "MaxRegister",
            Self::MinRegister => "MinRegister",
            Self::Lww => "Lww",
        }
    }
}

impl std::fmt::Display for DataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A single column definition within a schema.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnDef {
    /// Column name (unique within the schema).
    pub name: String,
    /// Column data type.
    pub data_type: DataType,
    /// Whether the column allows NULL values.
    pub nullable: bool,
}

impl ColumnDef {
    /// Create a non-nullable column.
    pub fn required(name: impl Into<String>, data_type: DataType) -> Self {
        Self {
            name: name.into(),
            data_type,
            nullable: false,
        }
    }

    /// Create a nullable column.
    pub fn nullable(name: impl Into<String>, data_type: DataType) -> Self {
        Self {
            name: name.into(),
            data_type,
            nullable: true,
        }
    }
}

/// A versioned schema snapshot.
///
/// The `version` counter starts at 1 and is incremented on every compatible
/// change. Incompatible changes produce a new catalog entry (view rebuild).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaVersion {
    /// Monotone version counter.
    pub version: u32,
    /// Ordered list of column definitions.
    pub columns: Vec<ColumnDef>,
}

impl SchemaVersion {
    /// Create a version-1 schema.
    pub fn new(columns: Vec<ColumnDef>) -> Self {
        Self {
            version: 1,
            columns,
        }
    }

    /// Returns the column with the given name, if any.
    pub fn column(&self, name: &str) -> Option<&ColumnDef> {
        self.columns.iter().find(|c| c.name == name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn data_type_display() {
        assert_eq!(DataType::Int64.to_string(), "Int64");
        assert_eq!(DataType::Utf8.to_string(), "Utf8");
        assert_eq!(DataType::TimestampMs.to_string(), "TimestampMs");
    }

    #[test]
    fn schema_version_column_lookup() {
        let schema = SchemaVersion::new(vec![
            ColumnDef::required("id", DataType::Int64),
            ColumnDef::nullable("name", DataType::Utf8),
        ]);
        assert_eq!(schema.version, 1);
        assert!(schema.column("id").is_some());
        assert!(schema.column("missing").is_none());
    }

    #[test]
    fn column_def_required_not_nullable() {
        let col = ColumnDef::required("amount", DataType::Float64);
        assert!(!col.nullable);
    }

    #[test]
    fn column_def_nullable_is_nullable() {
        let col = ColumnDef::nullable("notes", DataType::Utf8);
        assert!(col.nullable);
    }
}
