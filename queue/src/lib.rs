//! The queue library is responsible for all queue operations: enqueuing, dequeuing,
//! pausing, etc etc. It is also responsible for persistence of queue data.
//!
//! The queue is safe to be used across multiple threads due to its internal data
//! structures. All operations within the queue are synchronous.
//!
//! This library does not track clients or reservations. It is up to the layer above
//! to make sure certain operations are allowed to be performed (such as releasing a
//! reserved job).

pub mod channel;
pub mod error;
pub mod job;
pub mod queue;
pub mod store;
mod ser;

pub use crossbeam_channel;
pub use sled;

