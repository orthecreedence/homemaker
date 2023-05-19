//! Channels are effectively what would be "tables" in an RDMS. A channel holds a set of
//! jobs and manages the state of those jobs (ready, reserved, failed, etc). Much of this
//! state is meant to be persisted, but not at this level...channels are all in-memory.
//!
//! The channel module should really most likely be private since it's not meant to be
//! interacted with directly (use [`Queue`](crate::queue::Queue) instead) but having it
//! private make some integration/benchmarking tests more difficult.

use ahash::AHashMap;
use crate::{
    job::{Delay, FailID, JobID, Priority},
};
use crossbeam_channel::{self, Sender, Receiver};
use getset::{CopyGetters, Getters, MutGetters};
use std::collections::{BTreeMap, BTreeSet};
use tracing::{info};

#[derive(Clone, Debug, Default, CopyGetters, MutGetters)]
#[getset(get_copy = "pub", get_mut)]
pub struct ChannelMetrics {
    /// Number of jobs urgent (priority < 1024)
    urgent: u64,
    /// Number of jobs ready
    ready: u64,
    /// Number of jobs reserved
    reserved: u64,
    /// Number of jobs delayed
    delayed: u64,
    /// Number of jobs deleted
    deleted: u64,
    /// Number of jobs failed
    failed: u64,
    /// Number of total jobs entered
    total: u64,
    /// Number of active subscribers
    subscribers: u64,
}

/// A collection of modifications that can happen to a job on a channel, mainly used when
/// signaling changes.
#[derive(Debug, PartialEq)]
pub enum ChannelMod {
    Ready,
    Reserved,
    Delayed,
    Deleted,
    Failed,
}

#[derive(Clone, Debug, Getters, MutGetters)]
#[getset(get = "pub(crate)", get_mut)]
pub struct Channel {
    /// Allows interested parties to receive signals on changes in channel state
    signal: Receiver<ChannelMod>,
    /// Allows the channel to send signals when things change.
    sender: Sender<ChannelMod>,
    /// Ready job queue, segmented by sorted priority, then `JobId` (aka FIFO)
    ready: BTreeSet<(Priority, JobID)>,
    /// Reserved jobs, indexed by id
    reserved: AHashMap<JobID, Priority>,
    /// Delayed jobs, ordered by their delay value. Note that we use an `Option` here because
    /// if we kick a delayed job, if using a Vec we'd have to splice the job out of the vec
    /// which could potentially be expensive. Instead, we'll just update it in place to be
    /// `None` and when we expire the delayed jobs we'll just ignore any missing values.
    ///
    /// We'll still have to iterate over the Vec to *find* the job, but that's not a huge deal,
    /// unless we have millions of jobs expiring at the exact same timestamp-ms (possible, but
    /// not super likely), and also only matters when kicking jobs. Also who kicks delayed jobs?
    delayed: BTreeMap<Delay, Vec<Option<(JobID, Priority)>>>,
    /// Failed jobs, stored FIFO
    failed: BTreeMap<FailID, (JobID, Priority)>,
    /// Our heroic channel metrics
    metrics: ChannelMetrics,
}

impl Channel {
    /// Create a new empty `Channel`.
    pub fn new() -> Self {
        let (sender, signal) = crossbeam_channel::bounded(24);
        Self {
            signal,
            sender,
            ready: Default::default(),
            reserved: Default::default(),
            delayed: Default::default(),
            failed: Default::default(),
            metrics: Default::default(),
        }
    }

    /// Send a signal
    #[tracing::instrument(skip(self))]
    fn event(&self, ev: ChannelMod) {
        match self.sender().try_send(ev) {
            Err(e) => info!("Channel::event() -- problem sending event, channel possibly full {:?}", e),
            _ => {}
        }
    }

    fn push_ready_impl(&mut self, job_id: JobID, priority: Priority) {
        self.ready_mut().insert((priority, job_id));
        *self.metrics_mut().ready_mut() = self.ready().len() as u64;
        if priority < 1024.into() {
            *self.metrics_mut().urgent_mut() += 1;
        }
    }

    fn push_delayed_impl(&mut self, job_id: JobID, priority: Priority, delay: Delay) {
        let entry = self.delayed.entry(delay).or_insert_with(|| Vec::with_capacity(1));
        (*entry).push(Some((job_id, priority)));
        *self.metrics_mut().delayed_mut() += 1;
    }

    fn push_failed_impl(&mut self, job_id: JobID, fail_id: FailID, priority: Priority) {
        self.failed_mut().insert(fail_id, (job_id, priority));
        *self.metrics_mut().failed_mut() = self.failed().len() as u64;
    }

    fn reserve_impl(&mut self, job_id: JobID, priority: Priority) {
        self.reserved_mut().insert(job_id.clone(), priority);
        *self.metrics_mut().ready_mut() = self.ready().len() as u64;
        *self.metrics_mut().reserved_mut() = self.reserved().len() as u64;
        if priority < 1024.into() {
            *self.metrics_mut().urgent_mut() -= 1;
        }
        self.event(ChannelMod::Reserved);
    }

    /// Takes a few arguments about a job and puts it into the correct location (ready,
    /// failed, delayed) based on its state. This is mainly useful for restoring from the
    /// storage system.
    pub(crate) fn restore(&mut self, job_id: JobID, priority: Priority, delay: Option<Delay>, fail_id: Option<FailID>) {
        match (fail_id, delay) {
            (Some(fail_id), _) => {
                self.push_failed_impl(job_id, fail_id, priority);
            }
            (_, Some(delay)) => {
                self.push_delayed_impl(job_id, priority, delay);
            }
            (_, None) => {
                self.push_ready_impl(job_id, priority);
            }
        }
    }

    /// Push a job into this channel's queue. It can be given an optional delay value (a ms
    /// timestamp) that will delay processing of that job until the delay passes.
    #[tracing::instrument(skip(self))]
    pub fn push<P, D>(&mut self, id: JobID, priority: P, delay: Option<D>)
        where P: Into<Priority> + std::fmt::Debug,
              D: Into<Delay> + std::fmt::Debug,
    {
        let priority = priority.into();
        let delay = delay.map(|x| x.into());
        if let Some(delay) = delay {
            self.push_delayed_impl(id, priority, delay);
            self.event(ChannelMod::Delayed);
        } else {
            self.push_ready_impl(id, priority);
            self.event(ChannelMod::Ready);
        }
        *self.metrics_mut().total_mut() += 1;
    }

    /// Reserve the next available job
    #[tracing::instrument(skip(self))]
    pub fn reserve_next(&mut self) -> Option<JobID> {
        let next = self.ready.pop_first();
        let (priority, job_id) = next?;
        self.reserve_impl(job_id.clone(), priority);
        Some(job_id)
    }

    /// Reserve the next available job
    #[tracing::instrument(skip(self))]
    pub fn reserve_job<P>(&mut self, priority: P, job_id: JobID) -> Option<JobID>
        where P: Into<Priority> + std::fmt::Debug,
    {
        let priority = priority.into();
        let removed = self.ready.remove(&(priority, job_id));
        if removed {
            self.reserve_impl(job_id.clone(), priority);
            Some(job_id)
        } else {
            None
        }
    }

    /// Release a reserved job.
    #[tracing::instrument(skip(self))]
    pub fn release<P, D>(&mut self, id: JobID, priority: Option<P>, delay: Option<D>) -> Option<JobID>
        where P: Into<Priority> + std::fmt::Debug,
              D: Into<Delay> + std::fmt::Debug,
    {
        let existing = self.reserved_mut().remove(&id)?;
        let priority = priority
            .map(|x| x.into())
            .unwrap_or_else(|| existing);
        let delay = delay.map(|x| x.into());
        if let Some(delay) = delay {
            self.push_delayed_impl(id, priority, delay);
            self.event(ChannelMod::Delayed);
        } else {
            self.push_ready_impl(id, priority);
            self.event(ChannelMod::Ready);
        }
        *self.metrics_mut().reserved_mut() = self.reserved().len() as u64;
        Some(id)
    }

    /// Delete a reserved job.
    #[tracing::instrument(skip(self))]
    pub fn delete_reserved(&mut self, id: JobID) -> Option<JobID> {
        self.reserved_mut().remove(&id)?;
        *self.metrics_mut().reserved_mut() = self.reserved().len() as u64;
        *self.metrics_mut().deleted_mut() += 1;
        self.event(ChannelMod::Deleted);
        Some(id)
    }

    /// Delete a ready job.
    #[tracing::instrument(skip(self))]
    pub fn delete_ready<P>(&mut self, priority: P, id: JobID) -> Option<JobID>
        where P: Into<Priority> + std::fmt::Debug,
    {
        let priority = priority.into();
        let exists = self.ready_mut().remove(&(priority, id));
        if exists {
            *self.metrics_mut().ready_mut() = self.ready().len() as u64;
            if priority < 1024.into() {
                *self.metrics_mut().urgent_mut() -= 1;
            }
            *self.metrics_mut().deleted_mut() += 1;
            Some(id)
        } else {
            None
        }
    }

    /// Delete a ready job.
    #[tracing::instrument(skip(self))]
    pub fn delete_delayed<D>(&mut self, delay: D, job_id: JobID) -> Option<JobID>
        where D: Into<Delay> + std::fmt::Debug,
    {
        let delay = delay.into();
        let delqueue = self.delayed_mut().get_mut(&delay)?;
        if let Some(entry) = delqueue.iter_mut().find(|x| x.map(|x| &x.0 == &job_id).unwrap_or(false)) {
            // NOTE: look, i hate unwrap more than anybody. but...
            let (job_id, _priority) = entry.take().unwrap();
            *self.metrics_mut().delayed_mut() -= 1;
            *self.metrics_mut().deleted_mut() += 1;
            return Some(job_id);
        }
        None
    }

    /// Delete a failed job.
    #[tracing::instrument(skip(self))]
    pub fn delete_failed(&mut self, fail_id: FailID) -> Option<JobID> {
        match self.failed_mut().remove(&fail_id) {
            Some((job_id, _priority)) => {
                *self.metrics_mut().failed_mut() = self.failed().len() as u64;
                *self.metrics_mut().deleted_mut() += 1;
                Some(job_id)
            }
            None => None,
        }
    }

    /// Fail a reserved job.
    #[tracing::instrument(skip(self))]
    pub fn fail_reserved(&mut self, fail_id: FailID, job_id: JobID) -> Option<JobID> {
        let priority = self.reserved_mut().remove(&job_id)?;
        self.push_failed_impl(job_id, fail_id, priority);
        *self.metrics_mut().reserved_mut() = self.reserved().len() as u64;
        self.event(ChannelMod::Failed);
        Some(job_id)
    }

    /// Kick a specific failed job in the failed queue.
    #[tracing::instrument(skip(self))]
    pub fn kick_job_failed(&mut self, fail_id: &FailID) -> Option<JobID> {
        self.failed_mut().remove(fail_id)
            .map(|(id, priority)| {
                self.push_ready_impl(id, priority);
                *self.metrics_mut().failed_mut() = self.failed().len() as u64;
                self.event(ChannelMod::Ready);
                id
            })
    }

    /// Kick a specific job in the delayed queue.
    #[tracing::instrument(skip(self))]
    pub fn kick_job_delayed(&mut self, job_id: &JobID) -> Option<JobID> {
        for (_, delqueue) in self.delayed_mut().iter_mut() {
            if let Some(entry) = delqueue.iter_mut().find(|x| x.map(|x| &x.0 == job_id).unwrap_or(false)) {
                // NOTE: i hate unwrap(). normally. but we do a check above for None so we should
                // be fine
                let (job_id, priority) = entry.take().unwrap();
                *self.metrics_mut().delayed_mut() -= 1;
                self.push_ready_impl(job_id, priority);
                return Some(job_id.clone());
            }
        }
        None
    }

    /// Kick N failed jobs into the ready queue, returning the number of jobs kicked.
    #[tracing::instrument(skip(self))]
    pub fn kick_jobs_failed(&mut self, num_jobs: usize) -> Vec<JobID> {
        let mut kicked = Vec::with_capacity(num_jobs);
        for _ in 0..num_jobs {
            match self.failed_mut().first_entry() {
                Some(entry) => {
                    let (id, priority) = entry.remove();
                    self.push_ready_impl(id.clone(), priority);
                    kicked.push(id);
                }
                None => break,
            }
        }
        if kicked.len() > 0 {
            self.event(ChannelMod::Ready);
        }
        *self.metrics_mut().failed_mut() = self.failed().len() as u64;
        kicked
    }

    /// Kick N delayed jobs into the ready queue, returning the amount we actually kicked (could be
    /// lower than `num_jobs`).
    #[tracing::instrument(skip(self))]
    pub fn kick_jobs_delayed(&mut self, num_jobs: usize) -> Vec<JobID> {
        let mut kicked = Vec::with_capacity(num_jobs);
        // tracks any delay queues we can get rid of
        let mut rm_list = Vec::with_capacity(self.delayed.len());
        let mut ready_list = Vec::new();
        for (delay, delqueue) in self.delayed_mut().iter_mut() {
            let mut queue_processed = 0;
            for entry in delqueue.iter_mut() {
                match entry.take() {
                    Some((job_id, priority)) => {
                        // can't borrow mutably again, so we save these for later
                        ready_list.push((job_id.clone(), priority));
                        kicked.push(job_id);
                    }
                    None => {}
                }
                // mark as processed, even if None. this allows us to do a little
                // housekeeping
                queue_processed += 1;
                if kicked.len() >= num_jobs {
                    break;
                }
            }

            // if we processed all the items in this delayed queue, mark it for removal
            if queue_processed >= delqueue.len() {
                rm_list.push(delay.clone());
            }
            if kicked.len() >= num_jobs {
                break;
            }
        }

        // move these jobs to ready!
        for (job_id, priority) in ready_list {
            self.push_ready_impl(job_id, priority);
        }

        // remove any marked delay queues (they should now be empty)
        for delay_key in rm_list {
            self.delayed_mut().remove(&delay_key);
        }
        *self.metrics_mut().delayed_mut() -= kicked.len() as u64;

        if kicked.len() > 0 {
            self.event(ChannelMod::Ready);
        }
        kicked
    }

    /// Find all delayed jobs that are ready to move to the ready queue and move them. Returns the
    /// number of jobs we moved from delayed into ready.
    #[tracing::instrument(skip(self))]
    pub fn expire_delayed(&mut self, now: &Delay) -> Vec<JobID> {
        let mut expired = Vec::new();
        let buckets: Vec<Delay> = self.delayed().keys()
            .filter(|x| x <= &now)
            .map(|x| x.clone())
            .collect::<Vec<_>>();
        for key in buckets {
            match self.delayed_mut().remove(&key) {
                Some(jobs) => {
                    for val in jobs {
                        if let Some((id, priority)) = val {
                            self.push_ready_impl(id.clone(), priority);
                            expired.push(id);
                        }
                    }
                }
                None => {}
            }
        }
        *self.metrics_mut().delayed_mut() -= expired.len() as u64;
        if expired.len() > 0 {
            self.event(ChannelMod::Ready);
        }
        expired
    }

    /// Look at the next ready job
    #[tracing::instrument(skip(self))]
    pub fn peek_ready(&self) -> Option<(Priority, JobID)> {
        self.ready().first().map(|x| x.clone())
    }

    /// Look at the next delayed job
    #[tracing::instrument(skip(self))]
    pub fn peek_delayed(&self) -> Option<JobID> {
        for (_, delqueue) in self.delayed().iter() {
            if let Some(Some(job)) = delqueue.iter().find(|x| x.is_some()) {
                return Some(job.0.clone());
            }
        }
        None
    }

    /// Look at the next failed job
    #[tracing::instrument(skip(self))]
    pub fn peek_failed(&self) -> Option<(FailID, JobID)> {
        self.failed().first_key_value().map(|x| (x.0.clone(), x.1.0.clone()))
    }

    /// Subscribe to this channel. This returns a crossbeam channel that notifies
    /// listeners when things happen.
    #[tracing::instrument(skip(self))]
    pub fn subscribe(&mut self) -> Receiver<ChannelMod> {
        *self.metrics_mut().subscribers_mut() += 1;
        self.signal.clone()
    }

    /// Unsubscribe from this channel.
    #[tracing::instrument(skip(self))]
    pub fn unsubscribe(&mut self, signal: Receiver<ChannelMod>) {
        *self.metrics_mut().subscribers_mut() -= 1;
        drop(signal)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ops::Deref;

    macro_rules! assert_counts {
        ($channel:expr, $num_urgent:expr, $num_ready:expr, $num_reserved:expr, $num_delayed:expr, $num_deleted:expr, $num_failed:expr, $num_total:expr) => {
            assert_eq!($channel.metrics().urgent(), $num_urgent);
            assert_eq!($channel.metrics().ready(), $num_ready);
            assert_eq!($channel.metrics().reserved(), $num_reserved);
            assert_eq!($channel.metrics().delayed(), $num_delayed);
            assert_eq!($channel.metrics().deleted(), $num_deleted);
            assert_eq!($channel.metrics().failed(), $num_failed);
            assert_eq!($channel.metrics().total(), $num_total);
        }
    }

    #[test]
    fn restore() {
        let mut channel = Channel::new();
        assert_counts!(&channel, 0, 0, 0, 0, 0, 0, 0);

        channel.restore(JobID::from(1000), 1024.into(), None, None);
        assert_counts!(&channel, 0, 1, 0, 0, 0, 0, 0);
        channel.restore(JobID::from(1001), 1000.into(), None, None);
        assert_counts!(&channel, 1, 2, 0, 0, 0, 0, 0);
        channel.restore(JobID::from(1002), 1024.into(), Some(5000.into()), Some(FailID::from(1)));
        assert_counts!(&channel, 1, 2, 0, 0, 0, 1, 0);
        channel.restore(JobID::from(1003), 1024.into(), Some(5000.into()), None);
        assert_counts!(&channel, 1, 2, 0, 1, 0, 1, 0);

        let res1 = channel.reserve_next().unwrap();
        assert_eq!(res1, JobID::from(1001));
        assert_counts!(&channel, 0, 1, 1, 1, 0, 1, 0);
        let res2 = channel.reserve_next().unwrap();
        assert_eq!(res2, JobID::from(1000));
        assert_counts!(&channel, 0, 0, 2, 1, 0, 1, 0);
        let res3 = channel.reserve_next();
        assert_eq!(res3, None);
        assert_counts!(&channel, 0, 0, 2, 1, 0, 1, 0);
        assert_eq!(channel.expire_delayed(&5000.into()), vec![JobID::from(1003)]);
        assert_counts!(&channel, 0, 1, 2, 0, 0, 1, 0);
        let res4 = channel.reserve_next().unwrap();
        assert_eq!(res4, JobID::from(1003));
        assert_counts!(&channel, 0, 0, 3, 0, 0, 1, 0);
        assert_eq!(channel.kick_jobs_failed(10), vec![JobID::from(1002)]);
        assert_counts!(&channel, 0, 1, 3, 0, 0, 0, 0);
        let res5 = channel.reserve_next().unwrap();
        assert_eq!(res5, JobID::from(1002));
        assert_counts!(&channel, 0, 0, 4, 0, 0, 0, 0);
        assert_eq!(channel.reserve_next(), None);
        assert_counts!(&channel, 0, 0, 4, 0, 0, 0, 0);
    }

    #[test]
    fn restore_dupe() {
        let mut channel2 = Channel::new();
        assert_counts!(&channel2, 0, 0, 0, 0, 0, 0, 0);
        // duplicate ready jobs should NOT update counts
        channel2.restore(JobID::from(1000), 1024.into(), None, None);
        channel2.restore(JobID::from(1000), 1024.into(), None, None);
        channel2.restore(JobID::from(1000), 1024.into(), None, None);
        channel2.restore(JobID::from(1000), 1024.into(), None, None);
        assert_counts!(&channel2, 0, 1, 0, 0, 0, 0, 0);
        // duplicate delay jobs will update counts. this is expected.
        channel2.restore(JobID::from(1001), 1024.into(), Some(5000.into()), None);
        channel2.restore(JobID::from(1001), 1024.into(), Some(5000.into()), None);
        channel2.restore(JobID::from(1001), 1024.into(), Some(5000.into()), None);
        channel2.restore(JobID::from(1001), 1024.into(), Some(5000.into()), None);
        channel2.restore(JobID::from(1001), 1024.into(), Some(5000.into()), None);
        assert_counts!(&channel2, 0, 1, 0, 5, 0, 0, 0);
        // we really don't care about deduping here
        assert_eq!(channel2.expire_delayed(&5000.into()), vec![
            JobID::from(1001),
            JobID::from(1001),
            JobID::from(1001),
            JobID::from(1001),
            JobID::from(1001),
        ]);
        assert_counts!(&channel2, 0, 2, 0, 0, 0, 0, 0);
        // duplicate failed will not increase counts
        channel2.restore(JobID::from(1002), 1024.into(), None, Some(FailID::from(2001)));
        channel2.restore(JobID::from(1002), 1024.into(), None, Some(FailID::from(2001)));
        channel2.restore(JobID::from(1002), 1024.into(), None, Some(FailID::from(2001)));
        channel2.restore(JobID::from(1002), 1024.into(), None, Some(FailID::from(2001)));
        assert_counts!(&channel2, 0, 2, 0, 0, 0, 1, 0);
        assert_eq!(channel2.kick_jobs_failed(100), vec![JobID::from(1002)]);
        assert_counts!(&channel2, 0, 3, 0, 0, 0, 0, 0);
    }

    #[test]
    fn push_ready_reserve() {
        let mut channel = Channel::new();
        assert_counts!(&channel, 0, 0, 0, 0, 0, 0, 0);

        channel.push(JobID::from(45), 1024, None::<Delay>);
        channel.push(JobID::from(32), 1024, None::<Delay>);
        channel.push(JobID::from(69), 1000, None::<Delay>);
        assert_counts!(&channel, 1, 3, 0, 0, 0, 0, 3);

        let job1 = channel.reserve_next();
        assert_counts!(&channel, 0, 2, 1, 0, 0, 0, 3);
        let job2 = channel.reserve_next();
        assert_counts!(&channel, 0, 1, 2, 0, 0, 0, 3);
        let job3 = channel.reserve_next();
        assert_counts!(&channel, 0, 0, 3, 0, 0, 0, 3);
        let job4 = channel.reserve_next();
        assert_counts!(&channel, 0, 0, 3, 0, 0, 0, 3);

        assert_eq!(job1, Some(JobID::from(69)));
        assert_eq!(job2, Some(JobID::from(32)));
        assert_eq!(job3, Some(JobID::from(45)));
        assert_eq!(job4, None);
    }

    #[test]
    fn job_ordering() {
        let mut channel = Channel::new();
        channel.push(JobID::from(1102), 1024, None::<Delay>);
        channel.push(JobID::from(1100), 1024, None::<Delay>);
        channel.push(JobID::from(1101), 1024, None::<Delay>);
        channel.push(JobID::from(2001), 999, None::<Delay>);
        channel.push(JobID::from(2000), 1000, None::<Delay>);
        channel.push(JobID::from(2002), 500, None::<Delay>);

        let job1 = channel.reserve_next().unwrap();
        let job2 = channel.reserve_next().unwrap();
        let job3 = channel.reserve_next().unwrap();
        let job4 = channel.reserve_next().unwrap();
        let job5 = channel.reserve_next().unwrap();
        let job6 = channel.reserve_next().unwrap();

        assert_eq!(job1, JobID::from(2002));
        assert_eq!(job2, JobID::from(2001));
        assert_eq!(job3, JobID::from(2000));
        assert_eq!(job4, JobID::from(1100));
        assert_eq!(job5, JobID::from(1101));
        assert_eq!(job6, JobID::from(1102));
    }

    #[test]
    fn push_reserve_release() {
        let mut channel = Channel::new();
        channel.push(JobID::from(1111), 1024, None::<Delay>);
        channel.push(JobID::from(1112), 1024, None::<Delay>);
        assert_counts!(&channel, 0, 2, 0, 0, 0, 0, 2);

        let job1 = channel.reserve_next().unwrap();
        assert_eq!(job1, JobID::from(1111));
        assert_counts!(&channel, 0, 1, 1, 0, 0, 0, 2);
        assert_eq!(channel.peek_ready(), Some((1024.into(), JobID::from(1112))));

        channel.release(job1, None::<Priority>, None::<Delay>).unwrap();
        assert_counts!(&channel, 0, 2, 0, 0, 0, 0, 2);

        // job2 should be the same job as before because of ordering
        let job2 = channel.reserve_next().unwrap();
        assert_eq!(channel.peek_ready(), Some((1024.into(), JobID::from(1112))));
        let job3 = channel.reserve_next().unwrap();
        assert_eq!(channel.peek_ready(), None);
        assert_eq!(job2, JobID::from(1111));
        assert_eq!(job3, JobID::from(1112));
        assert_counts!(&channel, 0, 0, 2, 0, 0, 0, 2);

        channel.release(JobID::from(1112), None::<Priority>, None::<Delay>).unwrap();
        assert_counts!(&channel, 0, 1, 1, 0, 0, 0, 2);
        channel.release(JobID::from(1111), None::<Priority>, None::<Delay>).unwrap();
        assert_counts!(&channel, 0, 2, 0, 0, 0, 0, 2);
        assert_eq!(channel.peek_ready(), Some((1024.into(), JobID::from(1111))));

        let job4 = channel.reserve_next().unwrap();
        assert_counts!(&channel, 0, 1, 1, 0, 0, 0, 2);
        channel.release(job4, Some(1000), None::<Delay>).unwrap();
        assert_counts!(&channel, 1, 2, 0, 0, 0, 0, 2);
        let job5 = channel.reserve_next().unwrap();
        assert_counts!(&channel, 0, 1, 1, 0, 0, 0, 2);
        channel.release(job5, Some(2000), None::<Delay>).unwrap();
        assert_counts!(&channel, 0, 2, 0, 0, 0, 0, 2);

        let job6 = channel.reserve_next().unwrap();
        assert_counts!(&channel, 0, 1, 1, 0, 0, 0, 2);
        channel.release(job6, None::<Priority>, Some(5000)).unwrap();
        assert_counts!(&channel, 0, 1, 0, 1, 0, 0, 2);
        let job7 = channel.reserve_next().unwrap();
        assert_counts!(&channel, 0, 0, 1, 1, 0, 0, 2);
        channel.release(job7, None::<Priority>, Some(4000)).unwrap();
        assert_counts!(&channel, 0, 0, 0, 2, 0, 0, 2);

        // won't "release" non-reserved jobs
        let job8 = channel.release(JobID::from(69696969), Some(1024), None::<Delay>);
        assert_eq!(job8, None);
        assert_counts!(&channel, 0, 0, 0, 2, 0, 0, 2);
    }

    #[test]
    fn reserve_job() {
        let mut channel = Channel::new();
        for i in 0..10 {
            channel.push(JobID::from(i), 1024, None::<Delay>);
        }
        assert_counts!(&channel, 0, 10, 0, 0, 0, 0, 10);
        assert_eq!(channel.reserve_job(1024, JobID::from(999)), None);
        assert_counts!(&channel, 0, 10, 0, 0, 0, 0, 10);
        assert_eq!(channel.reserve_job(1000, JobID::from(5)), None);
        assert_counts!(&channel, 0, 10, 0, 0, 0, 0, 10);
        let job1 = channel.reserve_job(1024, JobID::from(5)).unwrap();
        assert_eq!(job1, JobID::from(5));
        assert_counts!(&channel, 0, 9, 1, 0, 0, 0, 10);
        channel.delete_reserved(job1).unwrap();
        assert_counts!(&channel, 0, 9, 0, 0, 1, 0, 10);
        let job2 = channel.reserve_job(1024, JobID::from(5));
        assert_eq!(job2, None);
        assert_counts!(&channel, 0, 9, 0, 0, 1, 0, 10);
    }

    #[test]
    fn push_delayed_peek_delayed_expire() {
        let mut channel = Channel::new();
        assert_eq!(channel.peek_delayed(), None);
        channel.push(JobID::from(3), 1024, Some(1678169565578));
        assert_eq!(channel.peek_delayed(), Some(JobID::from(3)));
        assert_counts!(&channel, 0, 0, 0, 1, 0, 0, 1);
        channel.push(JobID::from(4), 1024, Some(1678169569578));
        assert_eq!(channel.peek_delayed(), Some(JobID::from(3)));
        assert_counts!(&channel, 0, 0, 0, 2, 0, 0, 2);
        channel.push(JobID::from(1), 1024, Some(1678169565578));
        assert_eq!(channel.peek_delayed(), Some(JobID::from(3)));
        assert_counts!(&channel, 0, 0, 0, 3, 0, 0, 3);
        channel.push(JobID::from(2), 1000, Some(1678169566578));
        assert_eq!(channel.peek_delayed(), Some(JobID::from(3)));
        assert_counts!(&channel, 0, 0, 0, 4, 0, 0, 4);

        assert_eq!(channel.delayed().len(), 3); // cheating, but oh well.
        assert_eq!(channel.reserve_next(), None);
        assert_eq!(channel.reserve_next(), None);

        assert_eq!(channel.expire_delayed(&Delay::from(1678169565577)), vec![]);
        assert_counts!(&channel, 0, 0, 0, 4, 0, 0, 4);
        assert_eq!(channel.reserve_next(), None);
        assert_eq!(channel.delayed().len(), 3); // cheating, but oh well.

        assert_eq!(channel.expire_delayed(&Delay::from(1678169565578)), vec![JobID::from(3), JobID::from(1)]);
        assert_counts!(&channel, 0, 2, 0, 2, 0, 0, 4);
        assert_eq!(channel.peek_delayed(), Some(JobID::from(2)));
        assert_eq!(channel.reserve_next(), Some(JobID::from(1)));
        assert_eq!(channel.reserve_next(), Some(JobID::from(3)));
        assert_counts!(&channel, 0, 0, 2, 2, 0, 0, 4);
        assert_eq!(channel.delayed().len(), 2); // cheating, but oh well.

        assert_eq!(channel.expire_delayed(&Delay::from(1678169565578)), vec![]);
        assert_counts!(&channel, 0, 0, 2, 2, 0, 0, 4);
        assert_eq!(channel.peek_delayed(), Some(JobID::from(2)));
        assert_eq!(channel.reserve_next(), None);
        assert_counts!(&channel, 0, 0, 2, 2, 0, 0, 4);
        assert_eq!(channel.delayed().len(), 2); // cheating, but oh well.

        assert_eq!(channel.expire_delayed(&Delay::from(1778169565578)), vec![JobID::from(2), JobID::from(4)]);
        assert_eq!(channel.peek_delayed(), None);
        assert_counts!(&channel, 1, 2, 2, 0, 0, 0, 4);
        assert_eq!(channel.delayed().len(), 0); // cheating, but oh well.
    }

    #[test]
    fn push_reserve_delete_reserved() {
        let mut channel = Channel::new();
        channel.push(JobID::from(123), 1024, None::<Delay>);
        channel.push(JobID::from(345), 1024, None::<Delay>);
        channel.push(JobID::from(456), 1024, None::<Delay>);
        assert_counts!(&channel, 0, 3, 0, 0, 0, 0, 3);

        // can't delete a non-reserved job
        channel.delete_reserved(JobID::from(123));
        assert_counts!(&channel, 0, 3, 0, 0, 0, 0, 3);

        let job1 = channel.reserve_next().unwrap();
        assert_eq!(job1, JobID::from(123));
        assert_counts!(&channel, 0, 2, 1, 0, 0, 0, 3);
        let job1_2 = channel.delete_reserved(job1).unwrap();
        assert_eq!(job1_2, JobID::from(123));
        assert_counts!(&channel, 0, 2, 0, 0, 1, 0, 3);

        let job2 = channel.reserve_next().unwrap();
        assert!(channel.delete_reserved(job1_2).is_none());
        channel.delete_reserved(job2).unwrap();
        assert_counts!(&channel, 0, 1, 0, 0, 2, 0, 3);

        let job3 = channel.reserve_next().unwrap();
        channel.delete_reserved(job3).unwrap();
        assert_counts!(&channel, 0, 0, 0, 0, 3, 0, 3);
    }

    #[test]
    fn delete_ready() {
        let mut channel = Channel::new();
        channel.push(JobID::from(123), 1024, None::<Delay>);
        channel.push(JobID::from(345), 1000, None::<Delay>);
        channel.push(JobID::from(456), 1024, None::<Delay>);
        channel.push(JobID::from(789), 1024, Some(1111));
        assert_counts!(&channel, 1, 3, 0, 1, 0, 0, 4);
        channel.delete_ready(1024, JobID::from(123)).unwrap();
        assert_counts!(&channel, 1, 2, 0, 1, 1, 0, 4);
        assert!(channel.delete_ready(1024, JobID::from(123)).is_none());
        channel.delete_ready(1000, JobID::from(345)).unwrap();
        assert_counts!(&channel, 0, 1, 0, 1, 2, 0, 4);
        assert!(channel.delete_ready(1025, JobID::from(456)).is_none());
        assert_counts!(&channel, 0, 1, 0, 1, 2, 0, 4);
        assert!(channel.delete_ready(1024, JobID::from(789)).is_none());
        assert_counts!(&channel, 0, 1, 0, 1, 2, 0, 4);
        let job1 = channel.reserve_next().unwrap();
        assert_eq!(job1, JobID::from(456));
        let job2 = channel.reserve_next();
        assert_eq!(job2, None);
    }

    #[test]
    fn delete_delayed() {
        let mut channel = Channel::new();
        channel.push(JobID::from(123), 1024, Some(5000));
        channel.push(JobID::from(345), 1000, Some(4000));
        channel.push(JobID::from(456), 1024, Some(3000));
        channel.push(JobID::from(789), 1024, None::<Delay>);
        assert_counts!(&channel, 0, 1, 0, 3, 0, 0, 4);
        assert_eq!(channel.delete_delayed(5000, JobID::from(123)).unwrap(), JobID::from(123));
        assert_counts!(&channel, 0, 1, 0, 2, 1, 0, 4);
        // delay/id must match OR ELSE
        assert_eq!(channel.delete_delayed(4001, JobID::from(345)), None);
        assert_counts!(&channel, 0, 1, 0, 2, 1, 0, 4);
        assert_eq!(channel.delete_delayed(4000, JobID::from(344)), None);
        assert_counts!(&channel, 0, 1, 0, 2, 1, 0, 4);
        assert_eq!(channel.delete_delayed(4000, JobID::from(345)).unwrap(), JobID::from(345));
        assert_counts!(&channel, 0, 1, 0, 1, 2, 0, 4);
        assert_eq!(channel.delete_delayed(1000, JobID::from(789)), None);
        assert_counts!(&channel, 0, 1, 0, 1, 2, 0, 4);
    }

    #[test]
    fn delete_failed() {
        let mut channel = Channel::new();
        channel.push(JobID::from(123), 1024, None::<Delay>);
        channel.push(JobID::from(345), 1000, None::<Delay>);
        channel.push(JobID::from(456), 1024, None::<Delay>);
        channel.push(JobID::from(789), 1024, None::<Delay>);
        assert_counts!(&channel, 1, 4, 0, 0, 0, 0, 4);

        let job1 = channel.reserve_next().unwrap();
        assert_counts!(&channel, 0, 3, 1, 0, 0, 0, 4);
        let job2 = channel.reserve_next().unwrap();
        assert_counts!(&channel, 0, 2, 2, 0, 0, 0, 4);
        let job3 = channel.reserve_next().unwrap();
        assert_counts!(&channel, 0, 1, 3, 0, 0, 0, 4);
        let job4 = channel.reserve_next().unwrap();
        assert_counts!(&channel, 0, 0, 4, 0, 0, 0, 4);
        channel.release(job3, None::<Priority>, None::<Delay>).unwrap();
        channel.release(job4, None::<Priority>, Some(5000)).unwrap();
        assert_counts!(&channel, 0, 1, 2, 1, 0, 0, 4);

        channel.fail_reserved(FailID::from(job1.deref().clone()), job1).unwrap();
        assert_counts!(&channel, 0, 1, 1, 1, 0, 1, 4);
        channel.fail_reserved(FailID::from(job2.deref().clone()), job2).unwrap();
        assert_counts!(&channel, 0, 1, 0, 1, 0, 2, 4);

        assert_eq!(channel.delete_failed(FailID::from(456)), None);
        assert_counts!(&channel, 0, 1, 0, 1, 0, 2, 4);
        assert_eq!(channel.delete_failed(FailID::from(789)), None);
        assert_counts!(&channel, 0, 1, 0, 1, 0, 2, 4);
        assert_eq!(channel.delete_failed(FailID::from(123)), Some(JobID::from(123)));
        assert_counts!(&channel, 0, 1, 0, 1, 1, 1, 4);
        assert_eq!(channel.delete_failed(FailID::from(345)), Some(JobID::from(345)));
        assert_counts!(&channel, 0, 1, 0, 1, 2, 0, 4);
    }

    #[test]
    fn push_reserve_failed_kick_peek_failed() {
        let mut channel = Channel::new();
        for i in 0..100 {
            channel.push(JobID::from(i), 1024, None::<Delay>);
        }
        assert_counts!(&channel, 0, 100, 0, 0, 0, 0, 100);

        assert!(channel.fail_reserved(FailID::from(1), JobID::from(0)).is_none());
        assert_counts!(&channel, 0, 100, 0, 0, 0, 0, 100);

        let mut fail_id = 0;
        while let Some(job) = channel.reserve_next() {
            if job.deref() % 3 == 0 {
                channel.fail_reserved(FailID::from(fail_id), job).unwrap();
                fail_id += 1;
            } else {
                channel.delete_reserved(job).unwrap();
            }
        }

        assert_counts!(&channel, 0, 0, 0, 0, 66, 34, 100);
        let (fail_id, job_id) = channel.peek_failed().unwrap();
        assert_eq!(channel.kick_job_failed(&fail_id), Some(job_id));
        assert_eq!(job_id.deref(), &0);
        assert_eq!(fail_id.deref(), &0);
        assert_counts!(&channel, 0, 1, 0, 0, 66, 33, 100);

        assert_eq!(channel.kick_jobs_failed(10), [
            3, 6, 9, 12, 15, 18, 21, 24, 27, 30
        ].iter().map(|x| JobID::from(x.clone())).collect::<Vec<_>>());
        assert_counts!(&channel, 0, 11, 0, 0, 66, 23, 100);

        let mut reserve_counter = 0;
        while let Some(job) = channel.reserve_next() {
            reserve_counter += 1;
            assert_eq!(job.deref() % 3, 0);
        }
        assert_eq!(reserve_counter, 11);
        assert_eq!(channel.kick_jobs_failed(9999999), [
            33, 36, 39, 42, 45, 48, 51, 54, 57, 60, 63, 66,
            69, 72, 75, 78, 81, 84, 87, 90, 93, 96, 99
        ].iter().map(|x| JobID::from(x.clone())).collect::<Vec<_>>());
        assert_counts!(&channel, 0, 23, 11, 0, 66, 0, 100);
    }

    #[test]
    fn push_delayed_kick_delayed() {
        let mut channel = Channel::new();
        for i in 0..100 {
            channel.push(JobID::from(i), 1024, Some(1000));
        }
        assert_counts!(&channel, 0, 0, 0, 100, 0, 0, 100);
        channel.kick_job_delayed(&JobID::from(1)).unwrap();
        channel.kick_job_delayed(&JobID::from(22)).unwrap();
        channel.kick_job_delayed(&JobID::from(5)).unwrap();
        assert_counts!(&channel, 0, 3, 0, 97, 0, 0, 100);
        let job1 = channel.reserve_next().unwrap();
        let job2 = channel.reserve_next().unwrap();
        let job3 = channel.reserve_next().unwrap();
        assert_eq!(channel.reserve_next(), None);
        assert_eq!(job1, JobID::from(1));
        assert_eq!(job2, JobID::from(5));
        assert_eq!(job3, JobID::from(22));
        assert_counts!(&channel, 0, 0, 3, 97, 0, 0, 100);

        assert_eq!(
            channel.kick_jobs_delayed(23),
            (0..26).filter(|x| x != &1 && x != &5 && x != &22).map(|x| JobID::from(x)).collect::<Vec<_>>()
        );
        assert_counts!(&channel, 0, 23, 3, 74, 0, 0, 100);
        let job4 = channel.reserve_next().unwrap();
        let job5 = channel.reserve_next().unwrap();
        let job6 = channel.reserve_next().unwrap();
        let job7 = channel.reserve_next().unwrap();
        let job8 = channel.reserve_next().unwrap();
        let job9 = channel.reserve_next().unwrap();
        assert_eq!(job4, JobID::from(0));
        assert_eq!(job5, JobID::from(2));
        assert_eq!(job6, JobID::from(3));
        assert_eq!(job7, JobID::from(4));
        assert_eq!(job8, JobID::from(6));
        assert_eq!(job9, JobID::from(7));

        // test that kick_jobs_delayed will clean up
        let mut channel2 = Channel::new();
        for i in 0..100 {
            channel2.push(JobID::from(i), 1024, Some(((i % 5) + 1) as i64));
        }
        // kind of cheating here reading len() on delayed, but this is a good test
        assert_eq!(channel2.delayed().len(), 5);
        assert_eq!(channel2.kick_jobs_delayed(33), [
            0, 5, 10, 15, 20, 25, 30, 35, 40, 45,
            50, 55, 60, 65, 70, 75, 80, 85, 90,
            95, 1, 6, 11, 16, 21, 26, 31, 36, 41,
            46, 51, 56, 61
        ].iter().map(|x| JobID::from(x.clone())).collect::<Vec<_>>());
        assert_eq!(channel2.delayed().len(), 4);
        assert_eq!(channel2.kick_jobs_delayed(6), [
            66, 71, 76, 81, 86, 91
        ].iter().map(|x| JobID::from(x.clone())).collect::<Vec<_>>());
        assert_eq!(channel2.delayed().len(), 4);
        assert_eq!(channel2.kick_jobs_delayed(2), vec![JobID::from(96), JobID::from(2)]);
        assert_eq!(channel2.delayed().len(), 3);
    }

    #[test]
    fn un_subscribe() {
        let mut channel = Channel::new();
        assert_eq!(channel.metrics().subscribers(), 0);
        let sig = channel.subscribe();
        let sig2 = sig.clone();

        let handle = std::thread::spawn(move || {
            assert_eq!(channel.metrics.subscribers, 1);
            channel.push(JobID::from(1), 1024, None::<Delay>);
            channel.push(JobID::from(2), 1024, None::<Delay>);
            channel.push(JobID::from(3), 1024, Some(167000000));
            channel.push(JobID::from(4), 1024, Some(167000000));
            let res1 = channel.reserve_next().unwrap();
            let res2 = channel.reserve_next().unwrap();
            channel.delete_reserved(res1);
            channel.fail_reserved(FailID::from(1), res2);
            channel.expire_delayed(&Delay::from(167000001));
            let res3 = channel.reserve_next().unwrap();
            channel.release(res3, None::<Priority>, None::<Delay>);
            channel.unsubscribe(sig2);
            assert_eq!(channel.metrics.subscribers, 0);
        });
        let mut recv = Vec::new();
        loop {
            match sig.recv() {
                Ok(x) => recv.push(x),
                Err(_) => break,
            }
        }
        handle.join().unwrap();
        assert_eq!(recv, vec![
            ChannelMod::Ready,
            ChannelMod::Ready,
            ChannelMod::Delayed,
            ChannelMod::Delayed,
            ChannelMod::Reserved,
            ChannelMod::Reserved,
            ChannelMod::Deleted,
            ChannelMod::Failed,
            ChannelMod::Ready,
            ChannelMod::Reserved,
            ChannelMod::Ready,
        ]);
    }
}

