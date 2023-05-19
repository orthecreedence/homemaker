//! The main error enum for the project lives here, and documents the various
//! conditions that can arise while interacting with the system.

use crate::{
    job::JobID,
};
use thiserror::Error;

/// This is our error enum. It contains an entry for any part of the system in
/// which an expectation is not met or a problem occurs.
#[derive(Error, Debug)]
pub enum Error {
    /// An empty channel list was sent. Shame.
    #[error("Channel list cannot be empty")]
    ChannelListEmpty,

    /// Error locking a channel
    #[error("Error locking a channel: {0}")]
    ChannelLockError(String),

    /// Channel not found
    #[error("That channel wasn't found: {0}")]
    ChannelNotFound(String),

    /// That job wasn't found
    #[error("Job not found: {0}")]
    JobNotFound(JobID),

    /// Error serializing an object
    #[error("Error serializing")]
    Serde(#[from] bincode::Error),

    /// Error in storage layer
    #[error("Error in storage layer {0}")]
    StoreError(#[from] sled::Error),

    /// Error generating an id
    #[error("Error generating unique ID: {0}")]
    StoreIDError(String),

    /// Error storing job data
    #[error("Error handling job storage: {0}")]
    StoreJobError(String),
}

/// Wraps `std::result::Result` around our `Error` enum
pub type Result<T> = std::result::Result<T, Error>;


