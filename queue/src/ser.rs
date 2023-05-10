use bincode::Options;
use crate::{
    error::{Error, Result},
};
use serde::{
    de::Deserialize,
    ser::Serialize,
};

/// Serialize a value into a byte vector
pub(crate) fn serialize<T: Serialize>(val: &T) -> Result<Vec<u8>> {
    bincode::DefaultOptions::new()
        // big endian makes u64 values properly sortable by sled
        .with_big_endian()
        // fixed length encoding makes u64 values propertly sortable
        .with_fixint_encoding()
        .serialize(val)
        .map_err(|e| Error::Serde(e))
}

/// Deserialize a value from a byte vector
pub(crate) fn deserialize<'a, T: Deserialize<'a>>(bytes: &'a [u8]) -> Result<T> {
    bincode::DefaultOptions::new()
        // big endian makes u64 values properly sortable by sled
        .with_big_endian()
        // fixed length encoding makes u64 values propertly sortable
        .with_fixint_encoding()
        .deserialize(bytes)
        .map_err(|e| Error::Serde(e))
}

