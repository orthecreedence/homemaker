//! The queue module is the main interface to the queue system. It manages creation and cleanup
//! of channels, enqueuing/dequeuing jobs, persistence, etc. It's great.

use ahash::{RandomState};
use crate::{
    channel::Channel,
    error::{Error, Result},
    job::{Delay, Job, JobID, JobMeta, JobState, JobStatus, JobStore, Priority},
    ser,
};
use dashmap::DashMap;
use getset::{Getters, MutGetters};
use sled::Db;
use std::sync::Mutex;
use tracing::{warn};

/// A handy macro to wrap some gibberish around updating inline stats in our db.
macro_rules! update_meta {
    ($db:expr, $job_id:expr, $meta:ident, $op:block) => {
        $db
            .update_and_fetch(ser::serialize($job_id)?, |old| {
                let meta = match old {
                    Some(bytes) => {
                        let mut $meta = match ser::deserialize::<JobMeta>(bytes) {
                            Ok(meta) => meta,
                            // can't deal with errors here so we just ignore.
                            // it's only stats, after all...
                            Err(_) => return Some(Vec::from(bytes)),
                        };
                        $op
                        Some($meta)
                    }
                    None => None,
                };
                meta
                    .map(|x| ser::serialize(&x))
                    .transpose()
                    .unwrap_or_else(|_| old.map(|x| Vec::from(x)))
            })?
            .map(|x| ser::deserialize::<JobMeta>(x.as_ref()))
            .transpose()?
    }
}

/// Defines guarantees for [dequeuing][Queue::dequeue] jobs *from multiple channels*.
/// This does not matter if only grabbing jobs from one channel.
#[derive(Debug)]
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
}

/// The main queue datastructure...we create one of these for the process and it
/// manages all of our jobs and channels for us.
#[derive(Debug, Getters, MutGetters)]
#[getset(get = "pub", get_mut)]
pub struct Queue {
    /// Holds our queue channels
    channels: DashMap<String, Channel, RandomState>,
    /// Our queue's primary backing store. Stores critical job data that we simply cannot afford
    /// to lose.
    db_primary: Db,
    /// Our queue's secondary store, for storing dumb bullshit we don't care that much about like
    /// job stats and stuff.
    db_meta: Db,
    /// Locks the queue when using [Ordering::Strict] with [dequeue][Queue::dequeue] such that
    /// only one dequeue op can run at a time. Kind of a crappy way to do it, but more granular
    /// locking on the channels is next to impossible, so if you want strict ordering you will
    /// pay for it.
    dequeue_lock: Mutex<()>,
}

impl Queue {
    /// Create a new `Queue` object. This will read the underlying storage object and recreate
    /// the jobs and channels within that storage layer in-memory.
    pub fn new(db_primary: Db, db_meta: Db, estimated_num_channels: usize) -> Result<Self> {
        let channels = DashMap::with_capacity_and_hasher_and_shard_amount(estimated_num_channels, RandomState::new(), estimated_num_channels);
        Ok(Self {
            channels,
            db_primary,
            db_meta,
            dequeue_lock: Mutex::new(()),
        })
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
        let channel_name = channel_name.into();
        let job_id_val = self.db_primary().generate_id()?;
        let job_id = JobID::from(job_id_val);
        let state = JobState::new(channel_name.clone(), priority, JobStatus::Ready, ttr, delay);
        let job = Job::new(job_id.clone(), job_data, state);
        self.db_primary().insert(ser::serialize(&job_id)?, ser::serialize(&JobStore::from(&job))?)?;
        self.db_meta().insert(ser::serialize(&job_id)?, ser::serialize(&JobMeta::from(&job))?)?;

        let mut channel = self.channels().entry(channel_name).or_insert_with(|| Channel::new());
        channel.push(job_id.clone(), priority, delay);
        Ok(job_id)
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
            let maybe_job = channel.reserve();
            match maybe_job {
                Some(job_id) => {
                    match self.db_primary().get(ser::serialize(&job_id)?)? {
                        Some(store_bytes) => {
                            let store = ser::deserialize(store_bytes.as_ref())?;
                            let meta = update_meta! {
                                self.db_meta(),
                                &job_id,
                                meta,
                                { *meta.metrics_mut().reserves_mut() += 1 }
                            };
                            let job = Job::create_from_parts(job_id, store, meta);
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
        } else if chanlen == 1 {
            // no need to deal with dumb locking or scanning or w/e if our channel list is len 1
            match self.channels().get_mut(&channels[0]) {
                Some(mut chan) => {
                    reserve_from_channel(chan.value_mut())
                }
                None => Ok(None),
            }
        } else {
            let lock = match ordering {
                Ordering::Strict => {
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
                let lock = self.channels().get_mut(chan);
                let maybe_job = lock.as_ref()
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
                        reserve_from_channel(chan.value_mut())
                    }
                    None => Ok(None),
                }
            } else {
                Ok(None)
            };
            drop(lock);
            ret
        }
    }

    /// Release a job we previously reserved back into the queue, allowing others to process it.
    #[tracing::instrument(skip(self))]
    pub fn release<P, D>(&self, job_id: &JobID, priority: P, delay: Option<D>) -> Result<JobID>
        where P: Into<Priority> + std::fmt::Debug,
              D: Into<Delay> + std::fmt::Debug,
    {
        let store = self.db_primary().get(ser::serialize(job_id)?)?
            .map(|x| ser::deserialize::<JobStore>(x.as_ref()))
            .transpose()?
            .ok_or(Error::JobNotFound(job_id.clone()))?;
        let channel_name = store.state().channel();
        let mut channel = self.channels().get_mut(channel_name)
            .ok_or_else(|| {
                warn!("Queue.release() -- job {0} found but channel {1} (found in {0}'s data) is missing. curious.", job_id, channel_name);
                Error::JobNotFound(job_id.clone())
            })?;
        let released = channel.release(job_id.clone(), Some(priority), delay)
            .ok_or_else(|| Error::JobNotFound(job_id.clone()))?;
        update_meta! {
            self.db_meta(),
            &released,
            meta,
            { *meta.metrics_mut().releases_mut() += 1 }
        };
        Ok(released)
    }

    /*
    /// Delete a job.
    #[tracing::instrument(skip(self))]
    pub fn delete(&self, job_id: &JobID) -> Result<JobID> {
        let job = self.db_primary().get(
    }
    */
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ops::Deref;
    use std::sync::{Arc, RwLock};
    use std::time::{Instant};

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

    #[test]
    fn enqueue() {
        let (dbp1, dbm1) = dbs();
        let queue1 = Queue::new(dbp1, dbm1, 32).unwrap();
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

        let (dbp2, dbm2) = dbs();
        let queue2 = Queue::new(dbp2, dbm2, 128).unwrap();
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
        let (dbp1, dbm1) = dbs();
        let queue1  = Queue::new(dbp1, dbm1, 32).unwrap();
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
    fn dequeue_parallel_strict() {
        let num_threads = 3;
        let (dbp1, dbm1) = dbs();
        let queue1  = Queue::new(dbp1, dbm1, 32).unwrap();
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
        let (dbp2, dbm2) = dbs();
        let queue2  = Arc::new(Queue::new(dbp2, dbm2, 32).unwrap());
        for pri in [1024, 1000, 100, 2000].iter() {
            for i in 0..5 {
                for chan in ["vids1", "vids2", "vids3"].iter() {
                    queue2.enqueue(*chan, Vec::from(format!("{}/{}", pri, i).as_bytes()), *pri, 300, None::<Delay>).unwrap();
                }
            }
        }
        let results = Arc::new(RwLock::new(Vec::new()));
        let mut handles = Vec::new();
        for _ in 0..num_threads {
            let q = queue2.clone();
            let res = results.clone();
            let handle = std::thread::spawn(move || {
                let mut jobs = Vec::new();
                // do all our dequeues before doing any locking.
                while let Some(job) = q.dequeue(&vec!["vids1".into(), "vids2".into(), "vids3".into()], Ordering::Strict).unwrap() {
                    jobs.push((Instant::now(), job));
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
        let (dbp1, dbm1) = dbs();
        let queue1  = Queue::new(dbp1, dbm1, 32).unwrap();
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

    /*
    #[test]
    fn bench_hash() {
        let chan_map: std::collections::HashMap<String, u64> = std::collections::HashMap::new();
        let channels = std::sync::Arc::new(std::sync::RwLock::new(chan_map));
        let mut handles = Vec::new();
        for _ in 0..8 {
            let map = channels.clone();
            handles.push(std::thread::spawn(move || {
                for i in 0..3000000 {
                    let mut handle = map.write().unwrap();
                    let key = format!("chan-{}", i % 200);
                    (*handle).entry(key)
                        .and_modify(|e| (*e) += 1)
                        .or_insert(1);
                }
            }));
        }
        for handle in handles { handle.join().unwrap(); }
        println!("map {:?}", channels.read().unwrap());
    }

    #[test]
    fn bench_dash() {
        let chan_map: dashmap::DashMap<String, u64> = dashmap::DashMap::with_shard_amount(256);
        let channels = std::sync::Arc::new(chan_map);
        let mut handles = Vec::new();
        for _ in 0..8 {
            let map = channels.clone();
            handles.push(std::thread::spawn(move || {
                for i in 0..3000000 {
                    let key = format!("chan-{}", i % 200);
                    map.entry(key)
                        .and_modify(|e| (*e) += 1)
                        .or_insert(1);
                }
            }));
        }
        for handle in handles { handle.join().unwrap(); }
        println!("map {:?}", channels);
    }
    */
}

