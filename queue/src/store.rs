//! This module allows interfacing with the storage system.

use crate::{
    error::Result,
    job::{JobID, JobMeta, JobStatus, JobStore},
    ser,
};
use getset::{Getters};
use serde::{Serialize, Deserialize};
use sled::Db;

/// Convert serializable items into a key
pub(crate) fn to_key<T: Serialize>(item: &T) -> Result<Vec<u8>> {
    ser::serialize(item)
}

/// Defines all the operations that can modify our storage system (writes).
#[derive(Debug, Serialize, Deserialize)]
pub(crate) enum Mod {
    /// Clear a job's delay value from the meta store
    ClearDelayMeta(JobID),
    /// Clear a job's delay value from the primary store
    ClearDelayPrimary(JobID),
    /// Increment a job's release count
    JobMetricsIncReleases(JobID),
    /// Increment a job's reserve count
    JobMetricsIncReserves(JobID),
    /// Sets job.state.status
    SetStatus(JobID, JobStatus),
}

/// Defines all the operations that can modify our storage system (writes).
#[derive(Debug)]
pub enum ModResult {
    Meta(Option<JobMeta>),
    Store(Option<JobStore>),
}

impl ModResult {
    pub(crate) fn take_meta(self) -> Option<JobMeta> {
        match self {
            Self::Meta(x) => x,
            _ => None,
        }
    }
}

#[derive(Debug, Getters)]
#[getset(get = "pub(crate)")]
pub struct Store {
    primary: Db,
    meta: Db,
}

impl Store {
    /// Create a new `Store` from primary and meta [`sled::Db`] objects.
    pub fn new(primary: Db, meta: Db) -> Self {
        Self { primary, meta }
    }

    /// This is the central function for modifying the underlying storage.
    ///
    /// This method of clearly defining all storage write operations simplifies the interface
    /// for the [`Queue`][crate::queue::Queue] system BUT also allows expansion later on for
    /// things like replication across instances for HA.
    pub(crate) fn push_mod(&self, mod_op: Mod) -> Result<ModResult> {
        let res = match mod_op {
            Mod::ClearDelayMeta(job_id) => {
                self.update_and_save_meta(to_key(&job_id)?, |meta| {
                    *meta.delay_mut() = None;
                })?
            }
            Mod::ClearDelayPrimary(job_id) => {
                self.update_and_save_store(to_key(&job_id)?, |store| {
                    *store.state_mut().delay_mut() = None;
                })?
            }
            Mod::JobMetricsIncReleases(job_id) => {
                self.update_and_save_meta(to_key(&job_id)?, |meta| {
                    *meta.metrics_mut().releases_mut() += 1;
                })?
            }
            Mod::JobMetricsIncReserves(job_id) => {
                self.update_and_save_meta(to_key(&job_id)?, |meta| {
                    *meta.metrics_mut().reserves_mut() += 1;
                })?
            }
            Mod::SetStatus(job_id, status) => {
                self.update_and_save_store(to_key(&job_id)?, |store| {
                    *store.state_mut().status_mut() = status.clone();
                })?
            }
        };
        Ok(res)
    }

    /// Runs an update function on the state of the given job.
    fn update_and_save_store<T, F>(&self, job_id_ser: T, op: F) -> Result<ModResult>
        where T: AsRef<[u8]>,
              F: Fn(&mut JobStore),
    {
        self.primary()
            .update_and_fetch(job_id_ser, |old| {
                let store = match old {
                    Some(bytes) => {
                        match ser::deserialize::<JobStore>(bytes) {
                            Ok(mut store) => {
                                op(&mut store);
                                Some(store)
                            }
                            // can't deal with errors here so we just ignore.
                            // it's only stats, after all...
                            Err(_) => None,
                        }
                    }
                    None => None,
                };
                store
                    .map(|x| ser::serialize(&x))
                    .transpose()
                    .unwrap_or_else(|_| old.map(|x| Vec::from(x)))
            })?
            .map(|x| ser::deserialize::<JobStore>(x.as_ref()))
            .transpose()
            .map(|x| ModResult::Store(x))
    }

    /// Runs an update function on the meta of the given job.
    fn update_and_save_meta<T, F>(&self, job_id_ser: T, op: F) -> Result<ModResult>
        where T: AsRef<[u8]>,
              F: Fn(&mut JobMeta),
    {
        self.meta()
            .update_and_fetch(job_id_ser, |old| {
                let meta = match old {
                    Some(bytes) => {
                        let mut meta = match ser::deserialize::<JobMeta>(bytes) {
                            Ok(meta) => meta,
                            // can't deal with errors here so we just ignore.
                            // it's only stats, after all...
                            Err(_) => return Some(Vec::from(bytes)),
                        };
                        op(&mut meta);
                        Some(meta)
                    }
                    None => None,
                };
                meta
                    .map(|x| ser::serialize(&x))
                    .transpose()
                    .unwrap_or_else(|_| old.map(|x| Vec::from(x)))
            })?
            .map(|x| ser::deserialize::<JobMeta>(x.as_ref()))
            .transpose()
            .map(|x| ModResult::Meta(x))
    }
}

