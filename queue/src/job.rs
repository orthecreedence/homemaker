//! The job module stores all structures relating to jobs that are stored in the queue.
//!
//! There are three main containers here: [`Job`], [`JobStore`], and [`JobMeta`]. Because
//! this system is about balancing high-throughput and high-safety, it makes sense to store
//! jobs to disk as quickly as the user defines. However, some other pieces of data about
//! jobs are likely to be deemed less important (such as metrics), and consequently these
//! pieces of data are constantly changing.
//!
//! So the idea is that a `Job` represents a complete job, the `JobStore` represents data
//! *we absolutely do not want to lose aobut the job*, and `JobMeta` represents data that's
//! rapidly in flux and nice to have but not necessarily critical.
//!
//! This gives us a sort of tiered storage approach: one storage layer for important stuff,
//! and a less important one for bullshit. The data types here are in support of this model.

use getset::{CopyGetters, Getters, MutGetters};
use serde::{Serialize, Deserialize};
use std::ops::Deref;

macro_rules! wrapper_primitive {
    ($(#[$attr:meta])* $name:ident, $ty:ty) => {
        $(#[$attr])*
        #[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
        pub struct $name($ty);

        impl From<$ty> for $name {
            fn from(val: $ty) -> Self {
                Self(val)
            }
        }

        impl From<$name> for Vec<u8> {
            fn from(val: $name) -> Vec<u8> {
                Vec::from(val.0.to_be_bytes().as_slice())
            }
        }

        impl Deref for $name {
            type Target = $ty;

            fn deref(&self) -> &Self::Target {
                &self.0
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                self.0.fmt(f)
            }
        }
    }
}

wrapper_primitive! {
    /// Holds a priority value
    Priority, u32
}

wrapper_primitive! {
    /// Holds a unix timestamp in ms, used for delaying jobs.
    Timestamp, i64
}

wrapper_primitive! {
    /// Represents a job's unique ID
    JobID, u64
}

wrapper_primitive! {
    /// A unique ID for failed jobs
    FailID, u64
}

/// The status a job can have at any given point
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub enum JobStatus {
    /// The job is ready to be processed
    #[default]
    Ready,
    /// The job is failed (with a binary error message) and unavailable for
    /// processing until kicked
    Failed(FailID, Option<Vec<u8>>),
}

/// A job's state within the queue.
#[derive(Clone, Debug, Default, Getters, MutGetters, Serialize, Deserialize)]
#[getset(get = "pub", get_mut = "pub(crate)")]
pub struct JobState {
    /// The channel this job lives in
    channel: String,
    /// The job's priority (0 is highest priority)
    priority: Priority,
    /// The job's current status
    status: JobStatus,
    /// How many seconds this job can be reserved before it is released
    /// automatically
    ttr: u32,
    /// When the job was created (in ms)
    created: Timestamp,
    /// An optional delay. This is stored as a timestamp (ms) after which the job will be
    /// ready for processing again.
    delay: Option<Timestamp>,
}

impl JobState {
    /// Create a new `JobState`
    pub fn new<P, T>(channel: String, priority: P, status: JobStatus, ttr: u32, created: T, delay: Option<T>) -> Self
        where P: Into<Priority>,
              T: Into<Timestamp>,
    {
        Self {
            channel,
            priority: priority.into(),
            status,
            ttr,
            created: created.into(),
            delay: delay.map(|x| x.into()),
        }
    }
}

/// Metrics and stats about a single job
#[derive(Clone, Debug, Default, CopyGetters, MutGetters, Serialize, Deserialize)]
#[getset(get_copy = "pub", get_mut = "pub(crate)")]
pub struct JobMetrics {
    /// How many times this job has been reserved
    reserves: u32,
    /// How many times this job has been released
    releases: u32,
    /// How many times this job has timed out when processing
    timeouts: u32,
    /// How many times this job has failed
    fails: u32,
    /// How many times this job has been kicked
    kicks: u32,
}

/// Represents a job.
///
/// This is any set of data you want to process later.
#[derive(Clone, Debug, Default, Getters, MutGetters, Serialize, Deserialize)]
#[getset(get = "pub", get_mut = "pub(crate)")]
pub struct Job {
    /// The job's unique ID
    id: JobID,
    /// The job's data payload
    data: Vec<u8>,
    /// The job's current state
    state: JobState,
    /// The job's metrics
    metrics: JobMetrics,
}

impl Job {
    /// Create a new job. 
    pub fn new(id: JobID, data: Vec<u8>, state: JobState) -> Self {
        Self {
            id,
            data,
            state,
            metrics: JobMetrics::default(),
        }
    }

    /// Take a job id, store, and an optional meta and return a Job.
    ///
    /// Heyyyy, man. You got a job!
    pub fn create_from_parts(id: JobID, store: JobStore, meta: Option<JobMeta>, delay_in_meta: bool) -> Self {
        let JobStore { data, state } = store;
        let mut job = Self::new(id, data, state);
        if let Some(meta) = meta {
            job.metrics = meta.metrics;
            if delay_in_meta {
                if let Some(delay) = meta.delay {
                    job.state.delay = Some(delay);
                }
            }
        }
        job
    }

    /// Consume this job and its id, data, and state as a tuple.
    pub fn take(self) -> (JobID, Vec<u8>, JobState) {
        let Self { id, data, state, .. } = self;
        (id, data, state)
    }
}

/// A struct made specifically for storing jobs in our storage layer
#[derive(Clone, Debug, Getters, MutGetters, Serialize, Deserialize)]
#[getset(get = "pub", get_mut = "pub(crate)")]
pub struct JobStore {
    data: Vec<u8>,
    state: JobState,
}

impl From<&Job> for JobStore {
    fn from(job: &Job) -> Self {
        let Job { data, state, .. } = job;
        Self {
            data: data.clone(),
            state: state.clone(),
        }
    }
}

/// A struct made specifically for storing job meta in our storage layer
#[derive(Clone, Debug, Getters, MutGetters, Serialize, Deserialize)]
#[getset(get = "pub", get_mut = "pub(crate)")]
pub struct JobMeta {
    /// Stores the job's metrics, which can generally be thought of as ancillary and can be stored
    /// with looser requirements than primary job data.
    metrics: JobMetrics,
    /// A job's delay can be thought of as either primary or secondary data, so instead of forcing
    /// one or the other we allow the delay to be stored in either [`JobState`] as primary data or
    /// here in `JobMeta` as secondary data depending on what the user wants.
    delay: Option<Timestamp>,
}

impl JobMeta {
    /// Consumes this `JobMeta` and returns job metrics and the delay value
    pub fn take(self) -> (JobMetrics, Option<Timestamp>) {
        let Self { metrics, delay } = self;
        (metrics, delay)
    }
}

impl From<&Job> for JobMeta {
    fn from(job: &Job) -> Self {
        let Job { metrics, state, .. } = job;
        Self {
            metrics: metrics.clone(),
            delay: state.delay.clone(),
        }
    }
}

