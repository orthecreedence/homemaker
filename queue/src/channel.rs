use crate::{
    job::{Delay, FailID, JobID, JobRef, Priority},
};
use crossbeam_channel::{self, Sender, Receiver};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use tracing::{info};

#[derive(Debug, Default)]
pub(crate) struct ChannelMetrics {
    /// Number of jobs urgent (priority < 1024)
    pub(crate) urgent: u64,
    /// Number of jobs ready
    pub(crate) ready: u64,
    /// Number of jobs reserved
    pub(crate) reserved: u64,
    /// Number of jobs delayed
    pub(crate) delayed: u64,
    /// Number of jobs deleted
    pub(crate) deleted: u64,
    /// Number of jobs failed
    pub(crate) failed: u64,
    /// Number of total jobs entered
    pub(crate) total: u64,
    /// Number of active subscribers
    pub(crate) subscribers: u64,
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

#[derive(Debug)]
pub(crate) struct Channel {
    /// Allows interested parties to receive signals on changes in channel state
    pub(crate) signal: Receiver<ChannelMod>,
    /// Allows the channel to send signals when things change.
    sender: Sender<ChannelMod>,
    /// Ready job queues, segmented by sorted priority, then FIFO
    ready: BTreeSet<(Priority, JobID)>,
    /// Reserved jobs, indexed by id
    reserved: HashMap<JobID, Priority>,
    /// Delayed jobs, ordered by their delay value
    delayed: BTreeMap<Delay, Vec<JobRef>>,
    /// Failed jobs, stored FIFO
    failed: BTreeMap<FailID, JobRef>,
    /// Our heroic channel metrics
    pub(crate) metrics: ChannelMetrics,
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
    fn event(&self, ev: ChannelMod) {
        match self.sender.try_send(ev) {
            Err(e) => info!("Channel::event() -- problem sending event, channel possibly full {:?}", e),
            _ => {}
        }
    }

    fn push_ready_impl<P: Into<Priority>>(&mut self, id: JobID, priority: P) {
        let priority = priority.into();
        self.ready.insert((priority, id));
        self.metrics.ready += 1;
        if priority < 1024.into() {
            self.metrics.urgent += 1;
        }
    }

    /// Push a new job into this channel's ready queue
    pub fn push_ready<P: Into<Priority>>(&mut self, id: JobID, priority: P) {
        self.push_ready_impl(id, priority.into());
        self.metrics.total += 1;
        self.event(ChannelMod::Ready);
    }

    /// Push a job into this channel's delay queue for processing later.
    pub fn push_delayed<P, D>(&mut self, id: JobID, priority: P, delay: D)
        where P: Into<Priority>,
              D: Into<Delay>,
    {
        let entry = self.delayed.entry(delay.into()).or_insert_with(|| Vec::with_capacity(1));
        (*entry).push(JobRef::new(id, priority.into()));
        self.metrics.delayed += 1;
        self.metrics.total += 1;
        self.event(ChannelMod::Delayed);
    }

    /// Reserve the next available job
    pub fn reserve_next(&mut self) -> Option<JobID> {
        let (priority, job_id) = self.ready.pop_first()?;
        self.reserved.insert(job_id.clone(), priority);
        self.metrics.ready -= 1;
        if priority < 1024.into() {
            self.metrics.urgent -= 1;
        }
        self.metrics.reserved += 1;
        self.event(ChannelMod::Reserved);
        Some(job_id)
    }

    /// Release a reserved job.
    pub fn release(&mut self, id: JobID) -> Option<JobID> {
        let priority = self.reserved.remove(&id)?;
        self.push_ready_impl(id, priority);
        self.metrics.reserved -= 1;
        self.event(ChannelMod::Ready);
        Some(id)
    }

    /// Delete a reserved job.
    pub fn delete_reserved(&mut self, id: JobID) -> Option<JobID> {
        self.reserved.remove(&id)?;
        self.metrics.reserved -= 1;
        self.metrics.deleted += 1;
        self.event(ChannelMod::Deleted);
        Some(id)
    }

    /// Fail a reserved job.
    pub fn fail_reserved(&mut self, fail_id: FailID, id: JobID) -> Option<JobID> {
        let priority = self.reserved.remove(&id)?;
        let job_ref = JobRef::new(id, priority);
        let job_id = job_ref.id.clone();
        self.failed.insert(fail_id, job_ref);
        self.metrics.reserved -= 1;
        self.metrics.failed += 1;
        self.event(ChannelMod::Failed);
        Some(job_id)
    }

    /// Kick a specific job in the failed queue.
    pub fn kick_job(&mut self, fail_id: &FailID) -> bool {
        match self.failed.remove(fail_id) {
            Some(JobRef { id, priority }) => {
                self.push_ready_impl(id, priority);
                self.metrics.failed -= 1;
                self.event(ChannelMod::Ready);
                true
            }
            None => false,
        }
    }

    /// Kick N jobs into the ready queue
    pub fn kick_jobs(&mut self, num_jobs: usize) {
        let mut sent_ready = false;
        for _ in 0..num_jobs {
            match self.failed.first_entry() {
                Some(entry) => {
                    let JobRef { id, priority } = entry.remove();
                    self.push_ready_impl(id, priority);
                    self.metrics.failed -= 1;
                    sent_ready = true;
                }
                None => break,
            }
        }
        if sent_ready {
            self.event(ChannelMod::Ready);
        }
    }

    /// Find all delayed jobs that are ready to move to the ready queue and move them.
    pub fn expire_delayed(&mut self, now: &Delay) {
        let mut sent_ready = false;
        let buckets: Vec<Delay> = self.delayed.keys()
            .filter(|x| x <= &now)
            .map(|x| x.clone())
            .collect::<Vec<_>>();
        for key in buckets {
            match self.delayed.remove(&key) {
                Some(jobs) => {
                    for JobRef { id, priority } in jobs {
                        self.metrics.delayed -= 1;
                        self.push_ready_impl(id, priority);
                        sent_ready = true;
                    }
                }
                None => {}
            }
        }
        if sent_ready {
            self.event(ChannelMod::Ready);
        }
    }

    /// Look at the next ready job
    pub fn peek_ready(&self) -> Option<(Priority, JobID)> {
        self.ready.first().map(|x| x.clone())
    }

    /// Look at the next delayed job
    pub fn peek_delayed(&self) -> Option<JobID> {
        let (_, next) = self.delayed.first_key_value()?;
        next.get(0).map(|x| x.id.clone())
    }

    /// Look at the next failed job
    pub fn peek_failed(&self) -> Option<(FailID, JobID)> {
        self.failed.first_key_value().map(|x| (x.0.clone(), x.1.id.clone()))
    }

    /// Subscribe to this channel. This returns a crossbeam channel that notifies
    /// listeners when things happen.
    pub fn subscribe(&mut self) -> Receiver<ChannelMod> {
        self.metrics.subscribers += 1;
        self.signal.clone()
    }

    /// Unsubscribe from this channel.
    pub fn unsubscribe(&mut self, signal: Receiver<ChannelMod>) {
        self.metrics.subscribers -= 1;
        drop(signal)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ops::Deref;

    macro_rules! assert_counts {
        ($channel:expr, $num_urgent:expr, $num_ready:expr, $num_reserved:expr, $num_delayed:expr, $num_deleted:expr, $num_failed:expr, $num_total:expr) => {
            assert_eq!($channel.metrics.urgent, $num_urgent);
            assert_eq!($channel.metrics.ready, $num_ready);
            assert_eq!($channel.metrics.reserved, $num_reserved);
            assert_eq!($channel.metrics.delayed, $num_delayed);
            assert_eq!($channel.metrics.deleted, $num_deleted);
            assert_eq!($channel.metrics.failed, $num_failed);
            assert_eq!($channel.metrics.total, $num_total);
        }
    }

    #[test]
    fn push_ready_reserve() {
        let mut channel = Channel::new();
        assert_counts!(&channel, 0, 0, 0, 0, 0, 0, 0);

        channel.push_ready(JobID::from(45), 1024);
        channel.push_ready(JobID::from(32), 1024);
        channel.push_ready(JobID::from(69), 1000);
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
        channel.push_ready(JobID::from(1102), 1024);
        channel.push_ready(JobID::from(1100), 1024);
        channel.push_ready(JobID::from(1101), 1024);
        channel.push_ready(JobID::from(2001), 999);
        channel.push_ready(JobID::from(2000), 1000);
        channel.push_ready(JobID::from(2002), 500);

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
    fn push_release() {
        let mut channel = Channel::new();
        channel.push_ready(JobID::from(1111), 1024);
        channel.push_ready(JobID::from(1112), 1024);
        assert_counts!(&channel, 0, 2, 0, 0, 0, 0, 2);

        let job1 = channel.reserve_next().unwrap();
        assert_eq!(job1, JobID::from(1111));
        assert_counts!(&channel, 0, 1, 1, 0, 0, 0, 2);
        assert_eq!(channel.peek_ready(), Some((1024.into(), JobID::from(1112))));

        channel.release(job1).unwrap();
        assert_counts!(&channel, 0, 2, 0, 0, 0, 0, 2);

        // job2 should be the same job as before because of ordering
        let job2 = channel.reserve_next().unwrap();
        assert_eq!(channel.peek_ready(), Some((1024.into(), JobID::from(1112))));
        let job3 = channel.reserve_next().unwrap();
        assert_eq!(channel.peek_ready(), None);
        assert_eq!(job2, JobID::from(1111));
        assert_eq!(job3, JobID::from(1112));
        assert_counts!(&channel, 0, 0, 2, 0, 0, 0, 2);

        channel.release(JobID::from(1112));
        assert_counts!(&channel, 0, 1, 1, 0, 0, 0, 2);
        channel.release(JobID::from(1111));
        assert_counts!(&channel, 0, 2, 0, 0, 0, 0, 2);
        assert_eq!(channel.peek_ready(), Some((1024.into(), JobID::from(1111))));
    }

    #[test]
    fn push_delayed_expire() {
        let mut channel = Channel::new();
        assert_eq!(channel.peek_delayed(), None);
        channel.push_delayed(JobID::from(3), 1024, 1678169565578);
        assert_eq!(channel.peek_delayed(), Some(JobID::from(3)));
        assert_counts!(&channel, 0, 0, 0, 1, 0, 0, 1);
        channel.push_delayed(JobID::from(4), 1024, 1678169569578);
        assert_eq!(channel.peek_delayed(), Some(JobID::from(3)));
        assert_counts!(&channel, 0, 0, 0, 2, 0, 0, 2);
        channel.push_delayed(JobID::from(1), 1024, 1678169565578);
        assert_eq!(channel.peek_delayed(), Some(JobID::from(3)));
        assert_counts!(&channel, 0, 0, 0, 3, 0, 0, 3);
        channel.push_delayed(JobID::from(2), 1000, 1678169566578);
        assert_eq!(channel.peek_delayed(), Some(JobID::from(3)));
        assert_counts!(&channel, 0, 0, 0, 4, 0, 0, 4);

        assert_eq!(channel.reserve_next(), None);
        assert_eq!(channel.reserve_next(), None);

        channel.expire_delayed(&Delay::from(1678169565577));
        assert_counts!(&channel, 0, 0, 0, 4, 0, 0, 4);
        assert_eq!(channel.reserve_next(), None);

        channel.expire_delayed(&Delay::from(1678169565578));
        assert_counts!(&channel, 0, 2, 0, 2, 0, 0, 4);
        assert_eq!(channel.peek_delayed(), Some(JobID::from(2)));
        assert_eq!(channel.reserve_next(), Some(JobID::from(1)));
        assert_eq!(channel.reserve_next(), Some(JobID::from(3)));
        assert_counts!(&channel, 0, 0, 2, 2, 0, 0, 4);

        channel.expire_delayed(&Delay::from(1678169565578));
        assert_counts!(&channel, 0, 0, 2, 2, 0, 0, 4);
        assert_eq!(channel.peek_delayed(), Some(JobID::from(2)));
        assert_eq!(channel.reserve_next(), None);
        assert_counts!(&channel, 0, 0, 2, 2, 0, 0, 4);

        channel.expire_delayed(&Delay::from(1778169565578));
        assert_eq!(channel.peek_delayed(), None);
        assert_counts!(&channel, 1, 2, 2, 0, 0, 0, 4);
    }

    #[test]
    fn push_reserve_delete() {
        let mut channel = Channel::new();
        channel.push_ready(JobID::from(123), 1024);
        channel.push_ready(JobID::from(345), 1024);
        channel.push_ready(JobID::from(456), 1024);
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
    fn push_reserve_failed_kick_peek_failed() {
        let mut channel = Channel::new();
        for i in 0..100 {
            channel.push_ready(JobID::from(i), 1024);
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
        assert!(channel.kick_job(&fail_id));
        assert_eq!(job_id.deref(), &0);
        assert_eq!(fail_id.deref(), &0);
        assert_counts!(&channel, 0, 1, 0, 0, 66, 33, 100);

        channel.kick_jobs(10);
        assert_counts!(&channel, 0, 11, 0, 0, 66, 23, 100);

        let mut reserve_counter = 0;
        while let Some(job) = channel.reserve_next() {
            reserve_counter += 1;
            assert_eq!(job.deref() % 3, 0);
        }
        assert_eq!(reserve_counter, 11);
    }

    #[test]
    fn un_subscribe() {
        let mut channel = Channel::new();
        assert_eq!(channel.metrics.subscribers, 0);
        let sig = channel.subscribe();
        let sig2 = sig.clone();

        let handle = std::thread::spawn(move || {
            assert_eq!(channel.metrics.subscribers, 1);
            channel.push_ready(JobID::from(1), 1024);
            channel.push_ready(JobID::from(2), 1024);
            channel.push_delayed(JobID::from(3), 1024, 167000000);
            channel.push_delayed(JobID::from(4), 1024, 167000000);
            let res1 = channel.reserve_next().unwrap();
            let res2 = channel.reserve_next().unwrap();
            channel.delete_reserved(res1);
            channel.fail_reserved(FailID::from(1), res2);
            channel.expire_delayed(&Delay::from(167000001));
            let res3 = channel.reserve_next().unwrap();
            channel.release(res3);
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

