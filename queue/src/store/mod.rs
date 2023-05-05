pub mod memory;
pub mod disk;

use crate::{
    error::{Result},
};

/// Allows iterating over our store's values.
pub trait Iter: std::iter::Iterator { }

/// The `Store` trait defines an interface for our storage systems.
pub trait Store: Send + Sync {
    /// Generate a new unique sequential ID.
    fn gen_id(&self) -> Result<u64>;

    /// Put a value into this `Store`
    fn insert<K: Into<Vec<u8>>>(&self, key: K, value: Vec<u8>) -> Result<()>;

    /// Get a value from this `Store`
    fn get<K: Into<Vec<u8>>>(&self, key: K) -> Result<Option<Vec<u8>>>;

    /// Get an iterator over all the data in this `Store`
    fn iter(&self) -> dyn Iter<Item = (Vec<u8>, Vec<u8>)>;
}

