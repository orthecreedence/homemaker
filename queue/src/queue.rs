//! The queue module is the main interface to the queue system. It manages creation and cleanup
//! of channels, enqueuing/dequeuing jobs, persistence, etc. It's great.

use ahash::{RandomState};
use crate::{
    channel::{Channel, ChannelMod},
    error::{Error, Result},
    job::{Delay, FailID, Job, JobID, JobMeta, JobState, JobStatus, JobStore, Priority},
    ser,
    store::{Mod, Store, to_key},
};
use crossbeam_channel::{Receiver};
use dashmap::DashMap;
use derive_builder::Builder;
use getset::{Getters, MutGetters};
use std::sync::{Mutex};
use tracing::{warn};

/// Defines guarantees for [dequeuing][Queue::dequeue] jobs *from multiple channels*.
/// This does not matter if only grabbing jobs from one channel.
pub enum Ordering {
    /// Loose ordering is fast but won't necessarily return the jobs in order of
    /// priority/id across multiple channels. It's faster because it it does not
    /// hold the locks for channels when looking for the next job, it looks then
    /// releases, allowing other threads to dequeue from those channels. Using this
    /// mode, jobs are still ordered by priority/id *over time* but not strictly.
    Loose,
    /// Strict ordering, when dequeuing from multiple channels at once, will hold
    /// the locks to those channels open until a job has been dequeued. This
    /// guarantees strict priority/id ordering, but blocks other threads from
    /// grabbing jobs.
    Strict,
    /// Exactly like `Strict` ordering, but lets you run a function while the channel
    /// lock is being held open (just before unlocking). Probably only good for testing.
    /// But wow, is it great for testing.
    StrictOp(Box<dyn FnMut()>),
}

impl std::fmt::Debug for Ordering {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Loose => write!(f, "Loose"),
            Self::Strict => write!(f, "Strict"),
            Self::StrictOp(_) => write!(f, "StrictOp(FnMut)"),
        }
    }
}

/// Configured some options our queue might have
#[derive(Builder, Debug, Default, Getters)]
#[getset(get = "pub")]
#[builder(pattern = "owned")]
pub struct QueueConfig {
    /// If false (the default), stores job delays in the primary storage, making delays and
    /// expirations more resilient (but less performant). If you don't care about your delay
    /// values as much, and you use delays quite often (for example for rate limiting), you
    /// can get a great performance boost setting this to `true`.
    #[builder(default = "false")]
    delay_in_meta: bool,
}

/// The main queue datastructure...we create one of these for the process and it
/// manages all of our jobs and channels for us.
#[derive(Debug, Getters, MutGetters)]
#[getset(get = "pub(crate)", get_mut)]
pub struct Queue {
    /// Configuration for the queue
    config: QueueConfig,
    /// Holds our queue channels
    channels: DashMap<String, Channel, RandomState>,
    /// Our queue's storage system, used for persisting data.
    store: Store,
    /// Locks the queue when using [Ordering::Strict] with [dequeue][Queue::dequeue] such that
    /// only one dequeue op can run at a time. Kind of a crappy way to do it, but more granular
    /// locking on the channels is next to impossible, so if you want strict ordering you will
    /// pay for it.
    dequeue_lock: Mutex<()>,
}

impl Queue {
    /// Create a new `Queue` object. This will read the underlying storage object and recreate
    /// the jobs and channels within that storage layer in-memory.
    pub fn new(config: QueueConfig, store: Store, estimated_num_channels: usize) -> Result<Self> {
        let channels = DashMap::with_capacity_and_hasher_and_shard_amount(estimated_num_channels, RandomState::new(), estimated_num_channels);
        Ok(Self {
            config,
            channels,
            store,
            dequeue_lock: Mutex::new(()),
        })
    }

    /// Loads the jobs inside the primary data store and puts them into their corresponding
    /// channels. You probably want to call this every time you create a `Queue` object.
    ///
    /// Returns the number of jobs loaded.
    pub fn restore(&self) -> Result<u64> {
        let mut counter = 0;        // TODO: use metrics when implemented
        for res in self.store().primary().iter() {
            let (job_id_ser, job_store_ser) = res?;
            let job_id: JobID = ser::deserialize(&job_id_ser)?;
            let store: JobStore = ser::deserialize(&job_store_ser)?;
            let meta: Option<JobMeta> = self.store().meta().get(&job_id_ser)?
                .map(|x| ser::deserialize(&x))
                .transpose()?;
            let job = Job::create_from_parts(job_id, store, meta, *self.config().delay_in_meta());
            let (job_id, _job_data, job_state) = job.take();
            let mut channel = self.channels().entry(job_state.channel().clone()).or_insert_with(|| Channel::new());
            let fail_id_maybe = match job_state.status() {
                JobStatus::Failed(ref fail_id, ..) => Some(fail_id.clone()),
                _ => None,
            };
            channel.restore(job_id, job_state.priority().clone(), job_state.delay().clone(), fail_id_maybe);
            counter += 1;
        }
        Ok(counter)
    }

    /// Loop over our channels and perform any needed maintenance (pausing, expiring delayed
    /// jobs, etc).
    pub fn tick<D>(&self, now: D) -> Result<()>
        where D: Into<Delay> + Copy + std::fmt::Debug,
    {
        let now = now.into();
        let mut expired = Vec::new();
        for mut chan in self.channels().iter_mut() {
            // unpause the channel if its pause expiration is nigh
            chan.update_paused(&now);

            // expire any delayed jobs, moving them into ready, returning their ids.
            let mut expired_loc = chan.value_mut().expire_delayed(&now);
            // instead of handling the updating of the jobs in storage here, we push whatever
            // jobs we got onto the big todo list and keep going. want to keep these channel
            // locks open for as short as possible.
            expired.append(&mut expired_loc);
        }
        for job_id in expired {
            let mod_op = if *self.config().delay_in_meta() {
                Mod::ClearDelayMeta(job_id)
            } else {
                Mod::ClearDelayPrimary(job_id)
            };
            self.store().push_mod(mod_op)?;
        }
        Ok(())
    }

    /// Push a new job into the queue.
    ///
    /// This puts the jobs into the given channel with the given priority/ttr. Optionally, a delay
    /// can be specified, which is a unix timestamp in milliseconds (ie, the time the job becomes
    /// ready).
    #[tracing::instrument(skip(self))]
    pub fn enqueue<C, P, D>(&self, channel_name: C, job_data: Vec<u8>, priority: P, ttr: u64, delay: Option<D>) -> Result<JobID>
        where C: Into<String> + std::fmt::Debug,
              P: Into<Priority> + Copy + std::fmt::Debug,
              D: Into<Delay> + Copy + std::fmt::Debug,
    {
        let job_id_val = self.store().primary().generate_id()?;
        let job_id = JobID::from(job_id_val);
        let state = JobState::new(channel_name.into(), priority, JobStatus::Ready, ttr, delay);
        let job = Job::new(job_id.clone(), job_data, state);

        self.store().primary().insert(ser::serialize(&job_id)?, ser::serialize(&JobStore::from(&job))?)?;
        self.store().meta().insert(ser::serialize(&job_id)?, ser::serialize(&JobMeta::from(&job))?)?;
        let mut channel = self.channels().entry(job.state().channel().clone()).or_insert_with(|| Channel::new());
        channel.push(job_id.clone(), priority, delay);

        Ok(job.id().clone())
    }

    /// Pull a single job off the given list of channels, marking it as reserved so others cannot
    /// use it until we release, delete, or fail it.
    ///
    /// This takes an [`Ordering`] value, which determines how the dequeue should work. If using
    /// `Ordering::Loose`, jobs are returned loosely in priority/id order over time, however
    /// concurrent performance is much higher. If using `Ordering::Strict`, jobs are guaranteed
    /// to be returned in exact priority/id order, but performance will be lower. Note that
    /// specifying `Ordering` is only relevant when dequeuing from multiple channels.
    #[tracing::instrument(skip(self))]
    pub fn dequeue(&self, channels: &Vec<String>, ordering: Ordering) -> Result<Option<Job>> {
        let reserve_from_channel = |channel: &mut Channel| -> Result<Option<Job>> {
            let maybe_job = channel.reserve_next();
            match maybe_job {
                Some(job_id) => {
                    let job_id_ser = to_key(&job_id)?;
                    match self.store().primary().get(&job_id_ser)? {
                        Some(store_bytes) => {
                            let store = ser::deserialize(store_bytes.as_ref())?;
                            let meta = self.store()
                                .push_mod(Mod::JobMetricsIncReserves(job_id))?
                                .take_meta();
                            let job = Job::create_from_parts(job_id, store, meta, *self.config().delay_in_meta());
                            Ok(Some(job))
                        }
                        None => Ok(None),
                    }
                }
                None => Ok(None),
            }
        };

        let chanlen = channels.len();
        if chanlen == 0 {
            Err(Error::ChannelListEmpty)
        } else {
            self.select_channel(channels, ordering, |chan| {
                reserve_from_channel(chan)
            })
        }
    }

    /// Dequeue/reserve a job by its [`JobID`]. If the job isn't in the "ready" state, we return
    /// `Ok(None)`.
    #[tracing::instrument(skip(self))]
    pub fn dequeue_job(&self, job_id: JobID) -> Result<Option<Job>> {
        let job_id_ser = ser::serialize(&job_id)?;
        let stored_bytes = self.store().primary().get(&job_id_ser)?
            .ok_or(Error::JobNotFound(job_id.clone()))?;
        let job_store: JobStore = ser::deserialize(&stored_bytes)?;
        match self.channels.get_mut(job_store.state().channel()) {
            Some(mut chan) => {
                match chan.reserve_job(job_store.state().priority().clone(), job_id.clone()) {
                    Some(_) => {
                        let job_meta = self.store()
                            .push_mod(Mod::JobMetricsIncReserves(job_id))?
                            .take_meta();
                        let job = Job::create_from_parts(job_id, job_store, job_meta, *self.config().delay_in_meta());
                        Ok(Some(job))
                    }
                    None => Ok(None),
                }
            }
            None => Ok(None),
        }
    }

    /// Release a job we previously reserved back into the queue, allowing others to process it.
    ///
    /// It is the responsibility of the caller to ensure that this release command is happening
    /// on the same connection the job is reserved on.
    #[tracing::instrument(skip(self))]
    pub fn release<P, D>(&self, job_id: &JobID, priority: P, delay: Option<D>) -> Result<JobID>
        where P: Into<Priority> + std::fmt::Debug,
              D: Into<Delay> + std::fmt::Debug,
    {
        let store = self.store().primary().get(ser::serialize(job_id)?)?
            .map(|x| ser::deserialize::<JobStore>(x.as_ref()))
            .transpose()?
            .ok_or(Error::JobNotFound(job_id.clone()))?;
        let channel_name = store.state().channel();
        let mut channel = self.channels().get_mut(channel_name)
            .ok_or_else(|| {
                warn!("Queue.release() -- job {0} found but channel {1} (found in {0}'s data) is missing. curious.", job_id, channel_name);
                Error::ChannelNotFound(channel_name.clone())
            })?;
        let released = channel.release(job_id.clone(), Some(priority), delay)
            .ok_or_else(|| Error::JobNotFound(job_id.clone()))?;
        self.store()
            .push_mod(Mod::JobMetricsIncReleases(job_id.clone()))?;
        Ok(released)
    }

    /// Delete a job.
    ///
    /// If the job is in the reserved state, the caller *must ensure* that the delete command
    /// is originating from a connection where the job was originally reserved from. This logic
    /// is not enforced at this level.
    #[tracing::instrument(skip(self))]
    pub fn delete(&self, job_id: JobID) -> Result<Option<JobID>> {
        let job_id_ser = ser::serialize(&job_id)?;
        let job_ser = self.store().primary().get(&job_id_ser)?
            .ok_or_else(|| Error::JobNotFound(job_id))?;
        let mut job: JobStore = ser::deserialize(&job_ser)?;
        let delay: Option<Delay> = if *self.config().delay_in_meta() {
            self.store().meta().get(&job_id_ser)?
                .map(|meta_ser| {
                    ser::deserialize(&meta_ser)
                })
                .transpose()?
                .and_then(|x: JobMeta| x.take().1)
        } else {
            job.state_mut().delay_mut().take()
        };
        let res = {
            let mut channel = self.channels.get_mut(job.state().channel())
                .ok_or_else(|| {
                    warn!("Queue.delete() -- job {0} found but channel {1} (found in {0}'s data) is missing. curious.", job_id, job.state().channel());
                    Error::ChannelNotFound(job.state().channel().clone())
                })?;
            match (job.state().status(), delay) {
                (JobStatus::Failed(fail_id, ..), _) => {
                    channel.delete_failed(fail_id.clone())
                }
                (_, Some(delay)) => {
                    channel.delete_delayed(delay, job_id)
                }
                (_, None) => {
                    if channel.reserved().contains_key(&job_id) {
                        channel.delete_reserved(job_id)
                    } else {
                        channel.delete_ready(job.state().priority().clone(), job_id)
                    }
                }
            }
        };
        if res.is_some() {
            self.store().primary().remove(&job_id_ser)?;
            self.store().meta().remove(&job_id_ser)?;
        }
        self.clean_channels()?;
        Ok(res)
    }

    /// Fail a reserved job.
    ///
    /// The caller *must ensure* that the delete command is originating from a connection
    /// where the job was originally reserved from. This logic is not enforced at this level.
    #[tracing::instrument(skip(self))]
    pub fn fail_reserved(&self, job_id: JobID, faildata: Option<Vec<u8>>) -> Result<Option<JobID>> {
        let job_id_ser = ser::serialize(&job_id)?;
        let job_ser = self.store().primary().get(&job_id_ser)?
            .ok_or_else(|| Error::JobNotFound(job_id))?;
        let job: JobStore = ser::deserialize(&job_ser)?;
        let res = {
            let mut channel = self.channels.get_mut(job.state().channel())
                .ok_or_else(|| Error::JobNotFound(job_id))?;
            let fail_id = FailID::from(self.store().primary().generate_id()?);
            match channel.fail_reserved(fail_id.clone(), job_id) {
                Some(job_id) => {
                    let new_status = JobStatus::Failed(fail_id, faildata);
                    self.store.push_mod(Mod::SetStatus(job_id.clone(), new_status))?;
                    Some(job_id)
                }
                None => None,
            }
        };
        Ok(res)
    }

    /// Kick N jobs on the given channel, returning the job ids of the kicked jobs.
    ///
    /// If the channel has ANY failed jobs, we kick N failed and return. If the channel has NO
    /// failed jobs, then we kick N delayed jobs and return.
    #[tracing::instrument(skip(self))]
    pub fn kick(&self, channel: &str, num: usize) -> Result<Vec<JobID>> {
        match self.channels().get_mut(channel) {
            Some(mut chan) => {
                let kicked = if chan.metrics().failed() > 0 {
                    chan.kick_jobs_failed(num)
                } else {
                    chan.kick_jobs_delayed(num)
                };
                Ok(kicked)
            }
            None => Ok(vec![]),
        }
    }

    /// Kick a specific job, returning the kicked job id.
    ///
    /// If the job is failed or delayed, we kick it, otherwise we do nussing, Lebowski. Nussing.
    #[tracing::instrument(skip(self))]
    pub fn kick_job(&self, job_id: &JobID) -> Result<Option<JobID>> {
        let job_id_ser = to_key(job_id)?;
        let job_ser = self.store().primary().get(&job_id_ser)?
            .ok_or_else(|| Error::JobNotFound(job_id.clone()))?;
        let store: JobStore = ser::deserialize(job_ser.as_ref())?;
        let mut chan = self.channels().get_mut(store.state().channel())
            .ok_or_else(|| Error::ChannelNotFound(store.state().channel().clone()))?;
        let res = match store.state().status() {
            JobStatus::Failed(ref fail_id, _) => {
                chan.kick_job_failed(fail_id)
            }
            _ => {
                chan.kick_job_delayed(job_id)
            }
        };
        Ok(res)
    }

    /// Pause a channel.
    #[tracing::instrument(skip(self))]
    pub fn pause(&self, channel: &str, now: Delay, unpause_at: Delay) -> Result<()> {
        let mut channel = self.channels().get_mut(channel)
            .ok_or_else(|| Error::ChannelNotFound(channel.to_string()))?;
        channel.pause(now, unpause_at);
        Ok(())
    }

    /// UNPause a channel.
    #[tracing::instrument(skip(self))]
    pub fn unpause(&self, channel: &str) -> Result<()> {
        let mut channel = self.channels().get_mut(channel)
            .ok_or_else(|| Error::ChannelNotFound(channel.to_string()))?;
        channel.unpause();
        Ok(())
    }

    /// Look for and return a specific job by ID
    #[tracing::instrument(skip(self))]
    pub fn peek_job(&self, job_id: &JobID) -> Result<Job> {
        self.load_full_job(job_id)
    }

    /// Look for and return a the next available job on a particular channel.
    #[tracing::instrument(skip(self))]
    pub fn peek_ready(&self, channel: &str) -> Result<Option<Job>> {
        let channel = self.channels().get(channel)
            .ok_or_else(|| Error::ChannelNotFound(channel.to_string()))?;
        let job_id = match channel.peek_ready() {
            Some((_, job_id)) => job_id,
            None => return Ok(None),
        };
        self.load_full_job(&job_id)
            .map(|x| Some(x))
    }

    /// Look for and return a specific job by ID
    #[tracing::instrument(skip(self))]
    pub fn peek_delayed(&self, channel: &str) -> Result<Option<Job>> {
        let channel = self.channels().get(channel)
            .ok_or_else(|| Error::ChannelNotFound(channel.to_string()))?;
        let job_id = match channel.peek_delayed() {
            Some(job_id) => job_id,
            None => return Ok(None),
        };
        self.load_full_job(&job_id)
            .map(|x| Some(x))
    }

    /// Look for and return a specific job by ID
    #[tracing::instrument(skip(self))]
    pub fn peek_failed(&self, channel: &str) -> Result<Option<Job>> {
        let channel = self.channels().get(channel)
            .ok_or_else(|| Error::ChannelNotFound(channel.to_string()))?;
        let job_id = match channel.peek_failed() {
            Some((_, job_id)) => job_id,
            None => return Ok(None),
        };
        self.load_full_job(&job_id)
            .map(|x| Some(x))
    }

    /// Look for and return a specific job by ID
    #[tracing::instrument(skip(self))]
    pub fn list_tubes(&self) -> Result<Vec<String>> {
        let mut channels = Vec::with_capacity(self.channels().len());
        for cref in self.channels().iter() {
            channels.push(cref.key().clone());
        }
        Ok(channels)
    }

    /// Subscribe to a channel, returning its, well, um, channel (sigh) which allows the channel
    /// to send events on its...channel...which subscribers can use to know to "wake up" and
    /// reserve jobs that are then sent to their dumb `reserve` clients.
    #[tracing::instrument(skip(self))]
    pub fn subscribe(&self, channel: &str) -> Result<Receiver<ChannelMod>> {
        let mut chan = self.channels().entry(channel.to_string()).or_insert_with(|| Channel::new());
        Ok(chan.subscribe())
    }

    /// Unsubscribe from a channel. Generally, a client calls this when its connection is
    /// terminated.
    #[tracing::instrument(skip(self))]
    pub fn unsubscribe(&self, channel: &str, signal: Receiver<ChannelMod>) -> Result<()> {
        // put this in a block so `chan` drops before we run clean_channels() (or we get
        // deadlocks LOL XD XD AHAHAHHA)
        {
            let mut chan = self.channels().get_mut(channel)
                .ok_or_else(|| Error::ChannelNotFound(channel.to_string()))?;
            chan.unsubscribe(signal);
        }
        self.clean_channels()?;
        Ok(())
    }

    /// Load a job.
    fn load_full_job(&self, job_id: &JobID) -> Result<Job> {
        let job_id_ser = to_key(job_id)?;
        let job_ser = self.store().primary().get(&job_id_ser)?
            .ok_or_else(|| Error::JobNotFound(job_id.clone()))?;
        let store: JobStore = ser::deserialize(job_ser.as_ref())?;
        let meta: Option<JobMeta> = match self.store().meta().get(&job_id_ser)? {
            Some(ser) => ser::deserialize(ser.as_ref()).unwrap_or(None),
            None => None,
        };
        let job = Job::create_from_parts(job_id.clone(), store, meta, *self.config().delay_in_meta());
        Ok(job)
    }

    /// Given a list of channels and an ordering value, finds the channel with the
    /// next job having the highest priority and passes it as a &mut Channel into
    /// `op`.
    fn select_channel<T, F>(&self, channels: &Vec<String>, ordering: Ordering, op: F) -> Result<Option<T>>
        where F: FnOnce(&mut Channel) -> Result<Option<T>>,
    {
        if channels.len() == 1 {
            // no need to deal with dumb locking or scanning or w/e if our channel list is len 1
            return match self.channels().get_mut(&channels[0]) {
                Some(mut chan) => {
                    op(chan.value_mut())
                }
                None => Ok(None),
            };
        }

        let lock = match ordering {
            Ordering::Strict | Ordering::StrictOp(_) => {
                let handle = match self.dequeue_lock().lock() {
                    Ok(x) => x,
                    Err(e) => Err(Error::ChannelLockError(format!("{:?}", e)))?,
                };
                Some(handle)
            }
            _ => None,
        };
        // loop over our channels one by one, peeking jobs and at the end we'll reserve from
        // the chan with the lowest priority/job_id. not perfect because there's a chance
        // the channel we pick will have gotten that job reserved by the time we finish.
        let mut lowest = None;
        for chan in channels {
            let maybe_job = self.channels().get_mut(chan).as_ref()
                .and_then(|c| c.peek_ready().clone());
            match (lowest, maybe_job) {
                (None, Some((pri, id))) => {
                    lowest = Some((pri, id, chan));
                }
                (Some(pair1), Some((pri, id))) => {
                    let comp = (pri, id, chan);
                    if comp < pair1 {
                        lowest = Some(comp);
                    }
                }
                _ => {}
            }
        }
        let ret = if let Some(chan) = lowest.map(|x| x.2) {
            match self.channels().get_mut(chan) {
                Some(mut chan) => {
                    op(chan.value_mut())
                }
                None => Ok(None),
            }
        } else {
            Ok(None)
        };
        if let Ordering::StrictOp(mut op) = ordering {
            op();
        }
        drop(lock);
        ret
    }

    /// Remove any channels that are empty and have no subscribers.
    fn clean_channels(&self) -> Result<()> {
        self.channels().retain(|_, chan| !chan.empty());
        Ok(())
    }

    #[cfg(test)]
    /// Consume the queue and return the internal databases. For testing mainly.
    fn take_store(self) -> Store {
        let Self { store, .. } = self;
        store
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ops::Deref;
    use std::sync::{Arc, RwLock, atomic::{AtomicU64, Ordering as AtomicOrdering}};

    fn dbs() -> (sled::Db, sled::Db) {
        let db_primary = sled::Config::default()
            .mode(sled::Mode::HighThroughput)
            .temporary(true)
            .open().unwrap();
        let db_meta = sled::Config::default()
            .mode(sled::Mode::HighThroughput)
            .temporary(true)
            .open().unwrap();
        (db_primary, db_meta)
    }

    fn make_queue(num_chan: usize) -> Queue {
        let (dbp1, dbm1) = dbs();
        let config = QueueConfigBuilder::default().build().unwrap();
        let store = Store::new(dbp1, dbm1);
        Queue::new(config, store, num_chan).unwrap()
    }

    #[test]
    fn enqueue() {
        let queue1 = make_queue(32);
        assert_eq!(queue1.channels.len(), 0);
        assert_eq!(queue1.channels.contains_key("blixtopher"), false);
        queue1.enqueue("blixtopher", Vec::from("get a job".as_bytes()), 1024, 300, None::<Delay>).unwrap();
        assert_eq!(queue1.channels.len(), 1);
        assert_eq!(queue1.channels.contains_key("blixtopher"), true);
        assert_eq!(queue1.channels.get("blixtopher").unwrap().metrics().ready(), 1);
        assert_eq!(queue1.channels.get("blixtopher").unwrap().metrics().delayed(), 0);
        queue1.enqueue("blixtopher", Vec::from("say hello".as_bytes()), 1024, 300, None::<Delay>).unwrap();
        assert_eq!(queue1.channels.len(), 1);
        assert_eq!(queue1.channels.contains_key("blixtopher"), true);
        assert_eq!(queue1.channels.get("blixtopher").unwrap().metrics().ready(), 2);
        assert_eq!(queue1.channels.get("blixtopher").unwrap().metrics().delayed(), 0);
        queue1.enqueue("WORK", Vec::from("get a job".as_bytes()), 1024, 300, None::<Delay>).unwrap();
        assert_eq!(queue1.channels.len(), 2);
        assert_eq!(queue1.channels.contains_key("WORK"), true);
        assert_eq!(queue1.channels.get("WORK").unwrap().metrics().ready(), 1);
        assert_eq!(queue1.channels.get("WORK").unwrap().metrics().delayed(), 0);

        let queue2 = make_queue(128);
        assert_eq!(queue2.channels.len(), 0);
        assert_eq!(queue2.channels.contains_key("blixtopher"), false);
        queue2.enqueue("blixtopher", Vec::from("get a job".as_bytes()), 1024, 300, Some(696969696)).unwrap();
        assert_eq!(queue2.channels.len(), 1);
        assert_eq!(queue2.channels.contains_key("blixtopher"), true);
        assert_eq!(queue2.channels.get("blixtopher").unwrap().metrics().ready(), 0);
        assert_eq!(queue2.channels.get("blixtopher").unwrap().metrics().delayed(), 1);
    }

    #[test]
    fn dequeue() {
        let queue1  = make_queue(32);
        queue1.enqueue("videos", Vec::from("process-vid1".as_bytes()), 1024, 300, None::<Delay>).unwrap();
        queue1.enqueue("videos", Vec::from("process-vid2".as_bytes()), 1000, 300, None::<Delay>).unwrap();
        queue1.enqueue("videos", Vec::from("process-vid3".as_bytes()), 1000, 300, None::<Delay>).unwrap();
        queue1.enqueue("downloads", Vec::from("https://post.vidz.ru/videos/YOUR-WIFE-WITH-THE-NEIGHBOR.rar.mp4.avi.exe".as_bytes()), 1001, 300, None::<Delay>).unwrap();

        let job1 = queue1.dequeue(&vec!["videos".into()], Ordering::Strict).unwrap().unwrap();
        assert_eq!(job1.data(), &Vec::from("process-vid2".as_bytes()));
        assert_eq!(job1.state().channel(), "videos");
        assert_eq!(job1.metrics().reserves(), 1);

        let job2 = queue1.dequeue(&vec!["videos".into(), "downloads".into()], Ordering::Strict).unwrap().unwrap();
        assert_eq!(job2.data(), &Vec::from("process-vid3".as_bytes()));
        assert_eq!(job2.state().channel(), "videos");
        assert_eq!(job2.metrics().reserves(), 1);

        let job3 = queue1.dequeue(&vec!["videos".into(), "downloads".into()], Ordering::Strict).unwrap().unwrap();
        assert_eq!(job3.data(), &Vec::from("https://post.vidz.ru/videos/YOUR-WIFE-WITH-THE-NEIGHBOR.rar.mp4.avi.exe".as_bytes()));
        assert_eq!(job3.state().channel(), "downloads");
        assert_eq!(job3.metrics().reserves(), 1);

        let job4 = queue1.dequeue(&vec!["videos".into(), "downloads".into()], Ordering::Strict).unwrap().unwrap();
        assert_eq!(job4.data(), &Vec::from("process-vid1".as_bytes()));
        assert_eq!(job4.state().channel(), "videos");
        assert_eq!(job4.metrics().reserves(), 1);

        let job5 = queue1.dequeue(&vec!["videos".into(), "downloads".into()], Ordering::Strict).unwrap();
        assert_eq!(job5.as_ref().map(|x| x.id()), None);

        let job6 = queue1.dequeue(&vec!["i don't exist".into(), "me neither".into()], Ordering::Strict).unwrap();
        assert_eq!(job6.as_ref().map(|x| x.id()), None);
    }

    #[test]
    fn restore() {
        let queue1 = make_queue(32);
        let job1 = queue1.enqueue("jobs", Vec::from("do stuff".as_bytes()), 1024, 300, None::<Delay>).unwrap();
        let job2 = queue1.enqueue("jobs", Vec::from("do something".as_bytes()), 0, 300, None::<Delay>).unwrap();
        let job3 = queue1.enqueue("jobs", Vec::from("do something else".as_bytes()), 0, 300, None::<Delay>).unwrap();
        let job4 = queue1.enqueue("jobs", Vec::from("do later".as_bytes()), 1024, 300, Some(5000)).unwrap();
        let job5 = queue1.enqueue("todo", Vec::from("make todo list".as_bytes()), 1024, 300, None::<Delay>).unwrap();
        // "i have done that," said toad
        let job6 = queue1.enqueue("todo", Vec::from("wake up".as_bytes()), 1024, 300, Some(4000)).unwrap();
        for i in 0..9000 {
            queue1.enqueue("chain", Vec::from(format!("send this queue to {} people or else", i).as_bytes()), 2000, 300, None::<Delay>).unwrap();
        }
        let deq1 = queue1.dequeue(&vec!["jobs".into()], Ordering::Strict).unwrap().unwrap();
        assert_eq!(deq1.id(), &job2);
        queue1.fail_reserved(deq1.id().clone(), Some(vec![1, 2, 3])).unwrap().unwrap();

        let store = queue1.take_store();
        let queue2 = Queue::new(QueueConfigBuilder::default().build().unwrap(), store, 32).unwrap();
        let deq0 = queue2.dequeue(&vec!["jobs".into(), "todo".into(), "chain".into()], Ordering::Strict).unwrap();
        assert_eq!(deq0.as_ref().map(|x| x.id()), None);
        queue2.restore().unwrap();

        let deq1 = queue2.dequeue(&vec!["jobs".into(), "todo".into()], Ordering::Strict).unwrap();
        let deq2 = queue2.dequeue(&vec!["jobs".into(), "todo".into()], Ordering::Strict).unwrap();
        let deq3 = queue2.dequeue(&vec!["jobs".into(), "todo".into()], Ordering::Strict).unwrap();
        let deq4 = queue2.dequeue(&vec!["jobs".into(), "todo".into()], Ordering::Strict).unwrap();
        let deq5 = queue2.dequeue(&vec!["jobs".into(), "todo".into()], Ordering::Strict).unwrap();
        assert_eq!(deq1.unwrap().id(), &job3);
        assert_eq!(deq2.unwrap().id(), &job1);
        assert_eq!(deq3.unwrap().id(), &job5);
        assert_eq!(deq4.as_ref().map(|x| x.id()), None);
        assert_eq!(deq5.as_ref().map(|x| x.id()), None);
        queue2.tick(5000).unwrap();
        let deq6 = queue2.dequeue(&vec!["jobs".into(), "todo".into()], Ordering::Strict).unwrap();
        let deq7 = queue2.dequeue(&vec!["jobs".into(), "todo".into()], Ordering::Strict).unwrap();
        assert_eq!(deq6.unwrap().id(), &job4);
        assert_eq!(deq7.unwrap().id(), &job6);
        let deq8 = queue2.dequeue(&vec!["jobs".into(), "todo".into()], Ordering::Strict).unwrap();
        assert_eq!(deq8.as_ref().map(|x| x.id()), None);

        let mut counter = 0;
        while let Some(deq) = queue2.dequeue(&vec!["chain".into()], Ordering::Strict).unwrap() {
            assert_eq!(String::from_utf8(deq.data().clone()).unwrap(), format!("send this queue to {} people or else", counter));
            counter += 1;
        }
        assert_eq!(counter, 9000);
    }

    #[test]
    fn dequeue_job() {
        let queue = make_queue(32);
        let job1 = queue.enqueue("jobs", Vec::from("jerry".as_bytes()), 1024, 300, None::<Delay>).unwrap();
        let job2 = queue.enqueue("jobs", Vec::from("larry".as_bytes()), 1024, 300, None::<Delay>).unwrap();
        let job3 = queue.enqueue("jobs", Vec::from("barry".as_bytes()), 1024, 300, None::<Delay>).unwrap();
        let job4 = queue.enqueue("jobs", Vec::from("mary".as_bytes()), 1024, 300, None::<Delay>).unwrap();
        let job5 = queue.enqueue("jobs", Vec::from("harry".as_bytes()), 1024, 300, None::<Delay>).unwrap();
        let job6 = queue.enqueue("jobs", Vec::from("dary".as_bytes()), 1024, 300, None::<Delay>).unwrap();

        let deq1 = queue.dequeue(&vec!["jobs".into()], Ordering::Strict).unwrap();
        assert_eq!(deq1.as_ref().map(|x| x.id()), Some(&job1));
        assert_eq!(queue.release(&job1, 1024, None::<Delay>).unwrap(), job1);

        let deq_id = queue.dequeue_job(job4.clone()).unwrap();
        assert_eq!(deq_id.as_ref().map(|x| x.id()), Some(&job4));

        let deq2 = queue.dequeue(&vec!["jobs".into()], Ordering::Strict).unwrap().unwrap();
        let deq3 = queue.dequeue(&vec!["jobs".into()], Ordering::Strict).unwrap().unwrap();
        let deq4 = queue.dequeue(&vec!["jobs".into()], Ordering::Strict).unwrap().unwrap();
        let deq5 = queue.dequeue(&vec!["jobs".into()], Ordering::Strict).unwrap().unwrap();
        let deq6 = queue.dequeue(&vec!["jobs".into()], Ordering::Strict).unwrap().unwrap();
        let deq7 = queue.dequeue(&vec!["jobs".into()], Ordering::Strict).unwrap();

        assert_eq!(deq2.id(), &job1);
        assert_eq!(deq3.id(), &job2);
        assert_eq!(deq4.id(), &job3);
        assert_eq!(deq5.id(), &job5);
        assert_eq!(deq6.id(), &job6);
        assert_eq!(deq7.as_ref().map(|x| x.id()), None);
    }

    #[test]
    fn dequeue_parallel_strict() {
        let num_threads = 128;
        let queue1 = make_queue(32);
        let job1_id = queue1.enqueue("vids1", Vec::from("threw rocks at my car".as_bytes()), 1024, 300, None::<Delay>).unwrap();
        let job2_id = queue1.enqueue("vids2", Vec::from("we don't have any kids".as_bytes()), 1024, 300, None::<Delay>).unwrap();
        let job3_id = queue1.enqueue("vids3", Vec::from("went to the house of the kids".as_bytes()), 1000, 300, None::<Delay>).unwrap();

        let deq1 = queue1.dequeue(&vec!["vids1".into(), "vids2".into(), "vids3".into()], Ordering::Strict).unwrap().unwrap();
        let deq2 = queue1.dequeue(&vec!["vids2".into(), "vids3".into(), "vids1".into()], Ordering::Strict).unwrap().unwrap();
        let deq3 = queue1.dequeue(&vec!["vids3".into(), "vids1".into(), "vids2".into()], Ordering::Strict).unwrap().unwrap();
        assert_eq!(deq1.id(), &job3_id);
        assert_eq!(deq2.id(), &job1_id);
        assert_eq!(deq3.id(), &job2_id);

        // ok, let's create a parallel version of the ordering stuff.
        let queue2  = Arc::new(make_queue(32));
        for pri in [1024, 1000, 100, 2000].iter() {
            for i in 0..5 {
                for chan in ["vids1", "vids2", "vids3"].iter() {
                    queue2.enqueue(*chan, Vec::from(format!("{}/{}", pri, i).as_bytes()), *pri, 300, None::<Delay>).unwrap();
                }
            }
        }
        let counter = Arc::new(AtomicU64::new(0));
        let results = Arc::new(RwLock::new(Vec::new()));
        let mut handles = Vec::new();
        for _ in 0..num_threads {
            let q = queue2.clone();
            let res = results.clone();
            let counterc = counter.clone();
            let handle = std::thread::spawn(move || {
                let mut jobs = Vec::new();
                // do some freaky shit with locks and SrictOp to verify dequeue() is returning
                // jobs in the correct order across our threads. you might be tempted to increment
                // a  counter just after the dequeue op, but that will lead to ordering errors.
                // it must be done *with the channel lock held open* which is why we use StrictOp.
                let get = || {
                    let counter_arc = Arc::new(RwLock::new(0));
                    let counter_res = counter_arc.clone();
                    let counterc_2 = counterc.clone();
                    let order = Ordering::StrictOp(Box::new(move || {
                        let mut guard = counter_arc.write().unwrap();
                        (*guard) = counterc_2.fetch_add(1, AtomicOrdering::SeqCst);
                    }));
                    let job = q.dequeue(&vec!["vids1".into(), "vids2".into(), "vids3".into()], order).unwrap();
                    job.map(|j| ((*counter_res.read().unwrap()), j))
                };
                // do all our dequeues before doing any locking.
                while let Some((counter_val, job)) = get() {
                    jobs.push((counter_val, job));
                }
                // now lock the result set and push our stupid jobs onto the end
                for j in jobs {
                    let mut handle = res.write().unwrap();
                    (*handle).push(j);
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        // sort by our logged Instant values...should give us a general ordering.
        let mut jobs = (*results.read().unwrap()).clone();
        jobs.sort_by(|a, b| a.0.cmp(&b.0));
        // format results...
        let jobdata = jobs.into_iter()
            .map(|(_, job)| job)
            .map(|x| (
                x.id().clone(),
                x.state().channel().clone(),
                String::from_utf8(x.data().clone()).unwrap()
                    .split("/")
                    .map(|v| v.parse::<u64>().unwrap())
                    .collect::<Vec<_>>(),
            ))
            .collect::<Vec<_>>();

        // loop and look for anomolies
        let mut last_jobid: Option<JobID> = None;
        let mut last_pri: Option<u64> = None;
        for (job_id, _chan, stats) in jobdata {
            let pri = stats[0];
            match (last_jobid, last_pri) {
                (None, None) => {
                    last_jobid = Some(job_id);
                    last_pri = Some(pri);
                }
                (Some(lj), Some(lpri)) => {
                    if pri < lpri {
                        panic!("Jobs out of order: {:?}", ((last_pri, last_jobid), (pri, job_id)));
                    } else if lpri == pri && job_id.deref() < lj.deref() {
                        panic!("Jobs out of order: {:?}", ((last_pri, last_jobid), (pri, job_id)));
                    }
                    last_jobid = Some(job_id);
                    last_pri = Some(pri);
                }
                _ => panic!("that's not true. THAT'S IMPOSSIBLE"),
            }
        }
    }

    #[test]
    fn release() {
        let queue1 = make_queue(32);
        queue1.enqueue("videos", Vec::from("process-vid1".as_bytes()), 1024, 300, None::<Delay>).unwrap();
        queue1.enqueue("videos", Vec::from("process-vid2".as_bytes()), 1000, 300, None::<Delay>).unwrap();
        queue1.enqueue("videos", Vec::from("process-vid3".as_bytes()), 1000, 300, None::<Delay>).unwrap();
        queue1.enqueue("downloads", Vec::from("https://big-files.com/document".as_bytes()), 1001, 300, None::<Delay>).unwrap();
        queue1.enqueue("downloads", Vec::from("https://big-files.com/BIG-document".as_bytes()), 1001, 300, None::<Delay>).unwrap();

        let job1 = queue1.dequeue(&vec!["videos".into(), "downloads".into(), "flabbywabby".into()], Ordering::Strict).unwrap().unwrap();
        assert_eq!(job1.data(), &Vec::from("process-vid2".as_bytes()));
        let job1_id = queue1.release(job1.id(), 1001, None::<Delay>).unwrap();
        assert_eq!(&job1_id, job1.id());

        let job2 = queue1.dequeue(&vec!["videos".into(), "downloads".into(), "flabbywabby".into()], Ordering::Strict).unwrap().unwrap();
        assert_eq!(job2.data(), &Vec::from("process-vid3".as_bytes()));
        assert_eq!(job2.metrics().reserves(), 1);
        assert_eq!(job2.metrics().releases(), 0);

        let job3 = queue1.dequeue(&vec!["videos".into(), "downloads".into(), "flabbywabby".into()], Ordering::Strict).unwrap().unwrap();
        assert_eq!(job3.data(), &Vec::from("process-vid2".as_bytes()));
        assert_eq!(job3.metrics().reserves(), 2);
        assert_eq!(job3.metrics().releases(), 1);
    }

    #[test]
    fn delete() {
        let queue1 = make_queue(32);
        let job1 = queue1.enqueue("vidz", Vec::from("record".as_bytes()), 1024, 300, None::<Delay>).unwrap();
        let job2 = queue1.enqueue("vidz", Vec::from("edit".as_bytes()), 1024, 300, Some(5000)).unwrap();
        let job3 = queue1.enqueue("vidz", Vec::from("show".as_bytes()), 1024, 300, None::<Delay>).unwrap();
        let job4 = queue1.enqueue("vidz", Vec::from("critique".as_bytes()), 1024, 300, None::<Delay>).unwrap();
        let job5 = queue1.enqueue("vidz", Vec::from("sob-uncontrollably".as_bytes()), 1024, 300, None::<Delay>).unwrap();

        queue1.dequeue(&vec!["vidz".into()], Ordering::Strict).unwrap().unwrap();
        let deq2 = queue1.dequeue(&vec!["vidz".into()], Ordering::Strict).unwrap().unwrap();
        queue1.fail_reserved(deq2.id().clone(), Some(vec![6, 9, 6, 9])).unwrap().unwrap();
        queue1.delete(job1).unwrap().unwrap();
        queue1.delete(job2).unwrap().unwrap();
        queue1.delete(job3).unwrap().unwrap();
        queue1.delete(job4).unwrap().unwrap();
        queue1.delete(job5).unwrap().unwrap();
        assert!(matches!(queue1.delete(job1), Err(Error::JobNotFound(_))));
        assert!(matches!(queue1.delete(job2), Err(Error::JobNotFound(_))));
        assert!(matches!(queue1.delete(job3), Err(Error::JobNotFound(_))));
        assert!(matches!(queue1.delete(job4), Err(Error::JobNotFound(_))));
        assert!(matches!(queue1.delete(job5), Err(Error::JobNotFound(_))));
        assert_eq!(queue1.dequeue(&vec!["vidz".into()], Ordering::Strict).unwrap().as_ref().map(|x| x.id()), None);
    }

    #[test]
    fn fail_reserved() {
        let queue1 = make_queue(32);
        let job1 = queue1.enqueue("vidz", Vec::from("record".as_bytes()), 1024, 300, None::<Delay>).unwrap();
        let job2 = queue1.enqueue("vidz", Vec::from("edit".as_bytes()), 1024, 300, Some(5000)).unwrap();
        let job3 = queue1.enqueue("vidz", Vec::from("watch".as_bytes()), 1024, 300, None::<Delay>).unwrap();
        let job4 = queue1.enqueue("vidz", Vec::from("critique".as_bytes()), 1024, 300, None::<Delay>).unwrap();
        let job5 = queue1.enqueue("vidz", Vec::from("sob-uncontrollably".as_bytes()), 1024, 300, None::<Delay>).unwrap();

        assert_eq!(queue1.fail_reserved(job1, None).unwrap(), None);
        assert_eq!(queue1.fail_reserved(job2, None).unwrap(), None);
        assert_eq!(queue1.fail_reserved(job3, None).unwrap(), None);
        assert_eq!(queue1.fail_reserved(job4, None).unwrap(), None);
        assert_eq!(queue1.fail_reserved(job5, None).unwrap(), None);
        {
            let chan = queue1.channels().get("vidz").unwrap();
            assert_eq!(chan.failed().len(), 0);
        }

        queue1.dequeue(&vec!["vidz".into()], Ordering::Strict).unwrap().unwrap();
        queue1.dequeue(&vec!["vidz".into()], Ordering::Strict).unwrap().unwrap();
        queue1.dequeue(&vec!["vidz".into()], Ordering::Strict).unwrap().unwrap();

        assert_eq!(queue1.fail_reserved(job1, None).unwrap(), Some(job1.clone()));
        assert_eq!(queue1.fail_reserved(job2, None).unwrap(), None);
        assert_eq!(queue1.fail_reserved(job3, None).unwrap(), Some(job3.clone()));
        assert_eq!(queue1.fail_reserved(job4, None).unwrap(), Some(job4.clone()));
        assert_eq!(queue1.fail_reserved(job5, None).unwrap(), None);
        {
            let chan = queue1.channels().get("vidz").unwrap();
            assert_eq!(chan.failed().len(), 3);
        }

        let store = queue1.take_store();
        let queue2 = Queue::new(QueueConfigBuilder::default().build().unwrap(), store, 32).unwrap();
        queue2.restore().unwrap();
        {
            let chan = queue2.channels().get("vidz").unwrap();
            assert_eq!(chan.failed().len(), 3);
        }
    }

    #[test]
    fn kick_failed_delayed() {
        let queue1 = make_queue(32);
        let _job1 = queue1.enqueue("prank-calls", Vec::from("i'm detective john kimble".as_bytes()), 1024, 300, None::<Delay>).unwrap();
        let _job2 = queue1.enqueue("prank-calls", Vec::from("yeah sure you are".as_bytes()), 1024, 300, None::<Delay>).unwrap();
        let job3 = queue1.enqueue("prank-calls", Vec::from("i'm a cop you idiot".as_bytes()), 1024, 300, Some(5000)).unwrap();
        let job4 = queue1.enqueue("prank-calls", Vec::from("well how 'bout i come over there and i whoop your god damn ass??".as_bytes()), 1024, 300, Some(5000)).unwrap();
        let job1_2 = queue1.dequeue(&vec!["prank-calls".into()], Ordering::Strict).unwrap().unwrap();
        let job2_2 = queue1.dequeue(&vec!["prank-calls".into()], Ordering::Strict).unwrap().unwrap();
        let deq1 = queue1.dequeue(&vec!["prank-calls".into()], Ordering::Strict).unwrap();
        assert_eq!(deq1.as_ref().map(|x| x.id()), None);
        queue1.fail_reserved(job1_2.id().clone(), None).unwrap().unwrap();
        queue1.fail_reserved(job2_2.id().clone(), None).unwrap().unwrap();
        // should kick the two failed, not the two delayed
        assert_eq!(queue1.kick("prank-calls", 10).unwrap(), vec![
            job1_2.id().clone(),
            job2_2.id().clone(),
        ]);
        // should kick the two delayed now that we have no failed left LOL AHAHAH XD
        assert_eq!(queue1.kick("prank-calls", 10).unwrap(), vec![
            job3.clone(),
            job4.clone(),
        ]);
        assert_eq!(queue1.kick("prank-calls", 10).unwrap(), vec![]);
    }

    #[test]
    fn kick_job() {
        let queue1 = make_queue(32);
        let job1 = queue1.enqueue("prank-calls", Vec::from("i'm detective john kimble".as_bytes()), 1024, 300, None::<Delay>).unwrap();
        let job2 = queue1.enqueue("prank-calls", Vec::from("yeah sure you are".as_bytes()), 1024, 300, None::<Delay>).unwrap();
        let job3 = queue1.enqueue("prank-calls", Vec::from("i'm a cop you idiot".as_bytes()), 1024, 300, Some(5000)).unwrap();
        let job4 = queue1.enqueue("prank-calls", Vec::from("well how 'bout i come over there and i whoop your god damn ass??".as_bytes()), 1024, 300, Some(5000)).unwrap();
        let job1_2 = queue1.dequeue(&vec!["prank-calls".into()], Ordering::Strict).unwrap().unwrap();
        let job2_2 = queue1.dequeue(&vec!["prank-calls".into()], Ordering::Strict).unwrap().unwrap();
        let deq1 = queue1.dequeue(&vec!["prank-calls".into()], Ordering::Strict).unwrap();
        assert_eq!(deq1.as_ref().map(|x| x.id()), None);
        queue1.fail_reserved(job1_2.id().clone(), None).unwrap().unwrap();
        queue1.fail_reserved(job2_2.id().clone(), None).unwrap().unwrap();
        // kick buried
        assert_eq!(queue1.kick_job(&job1).unwrap(), Some(job1.clone()));
        assert_eq!(queue1.kick_job(&job1).unwrap(), None);
        assert_eq!(queue1.kick_job(&job2).unwrap(), Some(job2.clone()));
        assert_eq!(queue1.kick_job(&job2).unwrap(), None);
        // kick delayed
        assert_eq!(queue1.kick_job(&job3).unwrap(), Some(job3.clone()));
        assert_eq!(queue1.kick_job(&job3).unwrap(), None);
        assert_eq!(queue1.kick_job(&job4).unwrap(), Some(job4.clone()));
        assert_eq!(queue1.kick_job(&job4).unwrap(), None);
        // that should be it...LOL XD XD AHAHAHA OMG
        assert_eq!(queue1.kick("prank-calls", 42).unwrap(), vec![]);
    }

    #[test]
    fn tick() {
        let queue1 = make_queue(2);
        let job1 = queue1.enqueue("jobs", Vec::from("hello".as_bytes()), 1024, 300, None::<Delay>).unwrap();
        let job2 = queue1.enqueue("jobs", Vec::from("hello".as_bytes()), 1024, 300, None::<Delay>).unwrap();
        queue1.enqueue("jobs", Vec::from("hello".as_bytes()), 1024, 300, Some(5000)).unwrap();
        queue1.enqueue("jobs", Vec::from("hello".as_bytes()), 1024, 300, Some(5000)).unwrap();
        queue1.enqueue("jobs", Vec::from("hello".as_bytes()), 1024, 300, Some(5000)).unwrap();
        queue1.enqueue("jobs", Vec::from("hello".as_bytes()), 1024, 300, Some(6000)).unwrap();
        queue1.enqueue("jobs", Vec::from("hello".as_bytes()), 1024, 300, Some(6000)).unwrap();
        queue1.enqueue("jobs", Vec::from("hello".as_bytes()), 1024, 300, Some(6000)).unwrap();
        queue1.pause("jobs", 0.into(), 2500.into()).unwrap();

        assert!(queue1.dequeue(&vec!["jobs".into()], Ordering::Strict).unwrap().is_none());
        queue1.tick(1000).unwrap();
        assert!(queue1.dequeue(&vec!["jobs".into()], Ordering::Strict).unwrap().is_none());
        queue1.tick(2000).unwrap();
        assert!(queue1.dequeue(&vec!["jobs".into()], Ordering::Strict).unwrap().is_none());
        queue1.tick(3000).unwrap();
        let deq1 = queue1.dequeue(&vec!["jobs".into()], Ordering::Strict).unwrap().unwrap();
        assert_eq!(deq1.id(), &job1);
        let deq2 = queue1.dequeue(&vec!["jobs".into()], Ordering::Strict).unwrap().unwrap();
        assert_eq!(deq2.id(), &job2);
        assert!(queue1.dequeue(&vec!["jobs".into()], Ordering::Strict).unwrap().is_none());
        assert!(queue1.dequeue(&vec!["jobs".into()], Ordering::Strict).unwrap().is_none());
        assert!(queue1.dequeue(&vec!["jobs".into()], Ordering::Strict).unwrap().is_none());
        queue1.tick(4000).unwrap();
        assert!(queue1.dequeue(&vec!["jobs".into()], Ordering::Strict).unwrap().is_none());
        queue1.tick(5000).unwrap();
        // some
        assert!(queue1.dequeue(&vec!["jobs".into()], Ordering::Strict).unwrap().is_some());
        assert!(queue1.dequeue(&vec!["jobs".into()], Ordering::Strict).unwrap().is_some());
        assert!(queue1.dequeue(&vec!["jobs".into()], Ordering::Strict).unwrap().is_some());
        // none
        assert!(queue1.dequeue(&vec!["jobs".into()], Ordering::Strict).unwrap().is_none());
        assert!(queue1.dequeue(&vec!["jobs".into()], Ordering::Strict).unwrap().is_none());
        queue1.tick(6000).unwrap();
        // some
        assert!(queue1.dequeue(&vec!["jobs".into()], Ordering::Strict).unwrap().is_some());
        assert!(queue1.dequeue(&vec!["jobs".into()], Ordering::Strict).unwrap().is_some());
        assert!(queue1.dequeue(&vec!["jobs".into()], Ordering::Strict).unwrap().is_some());
        // none
        assert!(queue1.dequeue(&vec!["jobs".into()], Ordering::Strict).unwrap().is_none());
        assert!(queue1.dequeue(&vec!["jobs".into()], Ordering::Strict).unwrap().is_none());
    }

    #[test]
    fn peek_job() {
        let queue1 = make_queue(2);
        let job1 = queue1.enqueue("prank-calls", Vec::from("i'm detective john kimble".as_bytes()), 1024, 300, None::<Delay>).unwrap();
        let job2 = queue1.enqueue("prank-calls", Vec::from("yeah sure you are".as_bytes()), 1024, 300, None::<Delay>).unwrap();
        let job3 = queue1.enqueue("prank-calls", Vec::from("i'm a cop you idiot".as_bytes()), 1024, 300, Some(5000)).unwrap();
        let job4 = queue1.enqueue("prank-calls", Vec::from("well how 'bout i come over there and i whoop your god damn ass??".as_bytes()), 1024, 300, Some(5000)).unwrap();

        assert_eq!(queue1.peek_job(&job1).unwrap().data(), &Vec::from("i'm detective john kimble".as_bytes()));
        assert_eq!(queue1.peek_job(&job2).unwrap().data(), &Vec::from("yeah sure you are".as_bytes()));
        assert_eq!(queue1.peek_job(&job3).unwrap().data(), &Vec::from("i'm a cop you idiot".as_bytes()));
        assert_eq!(queue1.peek_job(&job4).unwrap().data(), &Vec::from("well how 'bout i come over there and i whoop your god damn ass??".as_bytes()));
        let neq = JobID::from(job4.deref() + 1);
        assert!(matches!(queue1.peek_job(&neq).unwrap_err(), Error::JobNotFound(_)));
    }

    #[test]
    fn peek_ready() {
        let queue1 = make_queue(4);
        queue1.enqueue("prank-calls1", Vec::from("i'm detective john kimble".as_bytes()), 1024, 300, Some(5000)).unwrap();
        queue1.enqueue("prank-calls1", Vec::from("yeah sure you are".as_bytes()), 1024, 300, None::<Delay>).unwrap();
        queue1.enqueue("prank-calls2", Vec::from("i'm a cop you idiot".as_bytes()), 1024, 300, None::<Delay>).unwrap();
        queue1.enqueue("prank-calls2", Vec::from("well how 'bout i come over there and i whoop your god damn ass??".as_bytes()), 1024, 300, None::<Delay>).unwrap();
        let _sub = queue1.subscribe("get-a-job").unwrap();

        let job1 = queue1.peek_ready("prank-calls1").unwrap().unwrap();
        assert_eq!(job1.data(), &Vec::from("yeah sure you are".as_bytes()));
        let job2 = queue1.peek_ready("prank-calls2").unwrap().unwrap();
        assert_eq!(job2.data(), &Vec::from("i'm a cop you idiot".as_bytes()));
        assert!(queue1.peek_ready("get-a-job").unwrap().is_none());
        assert!(matches!(queue1.peek_ready("poopy-butt").unwrap_err(), Error::ChannelNotFound(_)));
        assert!(queue1.peek_delayed("prank-calls2").unwrap().is_none());
        assert!(queue1.peek_failed("prank-calls1").unwrap().is_none());
        assert!(queue1.peek_failed("prank-calls2").unwrap().is_none());
    }

    #[test]
    fn peek_delayed() {
        let queue1 = make_queue(4);
        queue1.enqueue("prank-calls1", Vec::from("i'm detective john kimble".as_bytes()), 1024, 300, Some(5000)).unwrap();
        queue1.enqueue("prank-calls1", Vec::from("yeah sure you are".as_bytes()), 1024, 300, None::<Delay>).unwrap();
        queue1.enqueue("prank-calls2", Vec::from("i'm a cop you idiot".as_bytes()), 1024, 300, None::<Delay>).unwrap();
        queue1.enqueue("prank-calls2", Vec::from("well how 'bout i come over there and i whoop your god damn ass??".as_bytes()), 1024, 300, None::<Delay>).unwrap();
        let _sub = queue1.subscribe("get-a-job").unwrap();

        let job1 = queue1.peek_delayed("prank-calls1").unwrap().unwrap();
        assert_eq!(job1.data(), &Vec::from("i'm detective john kimble".as_bytes()));
        assert!(queue1.peek_delayed("prank-calls2").unwrap().is_none());
        assert!(queue1.peek_delayed("get-a-job").unwrap().is_none());
        assert!(matches!(queue1.peek_delayed("poopy-butt").unwrap_err(), Error::ChannelNotFound(_)));
        assert!(queue1.peek_failed("prank-calls1").unwrap().is_none());
        assert!(queue1.peek_failed("prank-calls2").unwrap().is_none());
    }

    #[test]
    fn peek_failed() {
        let queue1 = make_queue(4);
        let _job1 = queue1.enqueue("prank-calls1", Vec::from("i'm detective john kimble".as_bytes()), 1024, 300, Some(5000)).unwrap();
        let job2 = queue1.enqueue("prank-calls1", Vec::from("yeah sure you are".as_bytes()), 1024, 300, None::<Delay>).unwrap();
        let _job3 = queue1.enqueue("prank-calls2", Vec::from("i'm a cop you idiot".as_bytes()), 1024, 300, None::<Delay>).unwrap();
        let _job4 = queue1.enqueue("prank-calls2", Vec::from("well how 'bout i come over there and i whoop your god damn ass??".as_bytes()), 1024, 300, None::<Delay>).unwrap();
        let _sub = queue1.subscribe("get-a-job").unwrap();

        assert!(queue1.peek_failed("prank-calls1").unwrap().is_none());
        assert!(queue1.peek_failed("prank-calls2").unwrap().is_none());
        assert!(queue1.peek_failed("get-a-job").unwrap().is_none());
        assert!(matches!(queue1.peek_failed("poopy-butt").unwrap_err(), Error::ChannelNotFound(_)));

        let job2_d = queue1.dequeue(&vec!["prank-calls1".into(), "prank-calls2".into()], Ordering::Strict).unwrap().unwrap();
        assert_eq!(job2_d.id(), &job2);
        queue1.fail_reserved(job2_d.id().clone(), Some(Vec::from("i will. bye.".as_bytes()))).unwrap();
        let peeked1 = queue1.peek_failed("prank-calls1").unwrap().unwrap();
        assert_eq!(peeked1.id(), &job2);
        match peeked1.state().status() {
            JobStatus::Failed(_, faildata) => {
                assert_eq!(faildata, &Some(Vec::from("i will. bye.".as_bytes())));
            }
            _ => panic!("this was unexpected"),
        }
    }

    #[test]
    fn list_tubes() {
        let queue1 = make_queue(16);
        let _job1 = queue1.enqueue("prank-calls1", Vec::from("i'm detective john kimble".as_bytes()), 1024, 300, None::<Delay>).unwrap();
        let job2 = queue1.enqueue("prank-calls2", Vec::from("yeah sure you are".as_bytes()), 1024, 300, None::<Delay>).unwrap();
        let _job3 = queue1.enqueue("prank-calls3", Vec::from("i'm a cop you idiot".as_bytes()), 1024, 300, None::<Delay>).unwrap();
        let job4 = queue1.enqueue("prank-calls4", Vec::from("well how 'bout i come over there and i whoop your god damn ass??".as_bytes()), 1024, 300, None::<Delay>).unwrap();

        let mut chans = queue1.list_tubes().unwrap();
        chans.sort();
        assert_eq!(chans, vec![
            "prank-calls1".to_string(),
            "prank-calls2".to_string(),
            "prank-calls3".to_string(),
            "prank-calls4".to_string(),
        ]);

        queue1.delete(job4).unwrap().unwrap();
        queue1.delete(job2).unwrap().unwrap();

        let mut chans = queue1.list_tubes().unwrap();
        chans.sort();
        assert_eq!(chans, vec![
            "prank-calls1".to_string(),
            "prank-calls3".to_string(),
        ]);
    }

    #[test]
    fn clean_channels() {
        // channels should clean if they are empty AND have no subscribers
        let queue1 = make_queue(16);
        let _job1 = queue1.enqueue("prank-calls1", Vec::from("i'm detective john kimble".as_bytes()), 1024, 300, None::<Delay>).unwrap();
        let job2 = queue1.enqueue("prank-calls2", Vec::from("yeah sure you are".as_bytes()), 1024, 300, None::<Delay>).unwrap();
        let _job3 = queue1.enqueue("prank-calls3", Vec::from("i'm a cop you idiot".as_bytes()), 1024, 300, None::<Delay>).unwrap();
        let job4 = queue1.enqueue("prank-calls4", Vec::from("well how 'bout i come over there and i whoop your god damn ass??".as_bytes()), 1024, 300, None::<Delay>).unwrap();

        let mut chans = queue1.list_tubes().unwrap();
        chans.sort();
        assert_eq!(chans, vec![
            "prank-calls1".to_string(),
            "prank-calls2".to_string(),
            "prank-calls3".to_string(),
            "prank-calls4".to_string(),
        ]);

        let sub2 = queue1.subscribe("prank-calls2").unwrap();
        let sub4 = queue1.subscribe("prank-calls4").unwrap();
        queue1.delete(job4).unwrap().unwrap();
        queue1.delete(job2).unwrap().unwrap();

        let mut chans = queue1.list_tubes().unwrap();
        chans.sort();
        assert_eq!(chans, vec![
            "prank-calls1".to_string(),
            "prank-calls2".to_string(),
            "prank-calls3".to_string(),
            "prank-calls4".to_string(),
        ]);

        queue1.unsubscribe("prank-calls2", sub2).unwrap();
        let mut chans = queue1.list_tubes().unwrap();
        chans.sort();
        assert_eq!(chans, vec![
            "prank-calls1".to_string(),
            "prank-calls3".to_string(),
            "prank-calls4".to_string(),
        ]);

        queue1.unsubscribe("prank-calls4", sub4).unwrap();
        let mut chans = queue1.list_tubes().unwrap();
        chans.sort();
        assert_eq!(chans, vec![
            "prank-calls1".to_string(),
            "prank-calls3".to_string(),
        ]);
    }
}

