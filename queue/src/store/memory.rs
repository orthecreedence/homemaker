//! This module holds our in-memory storage interface.

use crate::{
    error::{Result},
    store::{Iter, Store},
};
use dashmap::{DashMap, iter::Iter as DashIter};
use std::iterator::Iterator;
use std::sync::atomic::{AtomicU64, Ordering};

pub struct MemIter(DashIter<Vec<u8>, Vec<u8>>);

impl Iterator for MemIter {
    type Item = (Vec<u8>, Vec<u8>);

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next()
    }
}

impl Iter for MemIter {}

/// A storage layer that lives entirely in-memory.
pub struct MemoryStore {
    next_id: AtomicU64,
    data: DashMap<Vec<u8>, Vec<u8>>,
}

impl MemoryStore {
    /// Create a new store at the given location.
    pub fn new() -> Self {
        Self {
            next_id: AtomicU64::new(1),
            data: DashMap::new(),
        }
    }
}

impl Store for MemoryStore {
    fn gen_id(&self) -> Result<u64> {
        Ok(self.next_id.fetch_add(1, Ordering::SeqCst))
    }

    fn insert<K: Into<Vec<u8>>>(&self, key: K, value: Vec<u8>) -> Result<()> {
        self.data.insert(key.into(), value);
        Ok(())
    }

    fn get<K: Into<Vec<u8>>>(&self, key: K) -> Result<Option<Vec<u8>>> {
        let key: Vec<u8> = key.into();
        Ok(self.data.get(&key).map(|x| x.clone()))
    }

    fn iter(&self) -> dyn Iter<Item = Vec<u8>> {
        MemIter(self.data.iter())
    }

    /*
    fn load<F>(&self, mut loader: F) -> Result<()>
        where F: FnMut(Job) -> Result<()>
    {
        for dashref in self.job_data.iter() {
            let (job_id, store_ser) = dashref.pair();
            let store = JobStore::deserialize_binary(store_ser)?;
            let meta_maybe = {
                self.job_meta.get(job_id)
                    .map(|x| JobMeta::deserialize_binary(x.value()))
                    .transpose()
                    .unwrap_or(None)
            };
            let job = Job::create_from_parts(job_id.clone(), store, meta_maybe);
            loader(job)?;
        }
        Ok(())
    }
    */
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;
    use std::sync::{Arc, RwLock};

    #[test]
    fn generates_ids() {
        let store = Arc::new(MemoryStore::new());
        let id_dumpster = Arc::new(RwLock::new(BTreeSet::new()));
        let mut handles = Vec::new();

        for _i in 0..8 {
            let local_store = store.clone();
            let ids = id_dumpster.clone();
            handles.push(std::thread::spawn(move || {
                for _x in 0..5 {
                    let new_id = local_store.gen_id().unwrap();
                    let mut handle = ids.write().unwrap();
                    (*handle).insert(new_id);
                }
            }));
        }

        for handle in handles { handle.join().unwrap(); }
        let id_list = {
            let handle = id_dumpster.read().unwrap();
            (*handle).iter().map(|x| x.clone()).collect::<Vec<_>>()
        };
        assert_eq!(id_list, vec![
            1, 2, 3, 4, 5,
            6, 7, 8, 9, 10,
            11, 12, 13, 14, 15,
            16, 17, 18, 19, 20,
            21, 22, 23, 24, 25,
            26, 27, 28, 29, 30,
            31, 32, 33, 34, 35,
            36, 37, 38, 39, 40
        ]);
    }
}

