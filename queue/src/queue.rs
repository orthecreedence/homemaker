//! The queue module is the main interface to the queue system. It manages creation and cleanup
//! of channels, enqueuing/dequeuing jobs, persistence, etc. It's great.

use crate::{
    channel::Channel,
    error::{Error, Result},
    job::{Delay, Job, JobID, JobMeta, JobState, JobStatus, JobStore, Priority},
    ser,
};
use dashmap::DashMap;
use getset::{Getters, MutGetters};
use sled::Db;
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

/// The main queue datastructure...we create one of these for the process and it
/// manages all of our jobs and channels for us.
#[derive(Debug, Getters, MutGetters)]
#[getset(get = "pub", get_mut)]
pub struct Queue {
    /// Holds our queue channels
    channels: DashMap<String, Channel>,
    /// Our queue's primary backing store. Stores critical job data that we simply cannot afford
    /// to lose.
    db_primary: Db,
    /// Our queue's secondary store, for storing dumb bullshit we don't care that much about like
    /// job stats and stuff.
    db_meta: Db,
}

impl Queue {
    /// Create a new `Queue` object. This will read the underlying storage object and recreate
    /// the jobs and channels within that storage layer in-memory.
    pub fn new(db_primary: Db, db_meta: Db, estimated_num_channels: usize) -> Result<Self> {
        let channels = DashMap::with_capacity_and_shard_amount(estimated_num_channels, estimated_num_channels);
        Ok(Self {
            channels,
            db_primary,
            db_meta,
        })
    }

    /// Push a new job into the queue.
    ///
    /// This puts the jobs into the given channel with the given priority/ttr. Optionally, a delay
    /// can be specified, which is a unix timestamp in milliseconds (ie, the time the job becomes
    /// ready).
    pub fn enqueue<C, P, D>(&self, channel_name: C, job_data: Vec<u8>, priority: P, ttr: u64, delay: Option<D>) -> Result<JobID>
        where C: Into<String>,
              P: Into<Priority> + Copy,
              D: Into<Delay> + Copy,
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
    /// This works by reading the channels' ready jobs ([`Channel::peek_ready`]) and picking the
    /// one with the lowest priority/[`JobID`]. This is obviously imperfect because if the most
    /// available job is on the firest channel, and we go reading five other channels to see if
    /// there's a better job, by the time we conclude the first was the best it may have already
    /// been reserved by some other jerk (the wonders of parallelism).
    pub fn dequeue(&self, channels: &Vec<String>) -> Result<Option<Job>> {
        let reserve_from_channel = |channel: &str| -> Result<Option<Job>> {
            let maybe_job = self.channels().get_mut(channel)
                .and_then(|mut c| c.reserve());
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
            reserve_from_channel(&channels[0])
        } else {
            // loop over our channels one by one, peeking jobs and at the end we'll reserve from
            // the chan with the lowest priority/job_id. not perfect because there's a chance
            // the channel we pick will have gotten that job reserved by the time we finish.
            let mut lowest = None;
            for chan in channels {
                let maybe_job = self.channels().get_mut(chan)
                    .and_then(|c| c.peek_ready());
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
            if let Some(chan) = lowest.map(|x| x.2) {
                reserve_from_channel(chan)
            } else {
                Ok(None)
            }
        }
    }

    /// Release a job we previously reserved back into the queue, allowing others to process it.
    pub fn release<P, D>(&self, job_id: &JobID, priority: P, delay: Option<D>) -> Result<()>
        where P: Into<Priority>,
              D: Into<Delay>,
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
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        queue1.enqueue("downloads", Vec::from("https://post.vidz.ru/vidoes/YOUR-WIFE-WITH-THE-NEIGHBOR.rar.mp4.avi.exe".as_bytes()), 1001, 300, None::<Delay>).unwrap();

        let job1 = queue1.dequeue(&vec!["videos".into()]).unwrap().unwrap();
        assert_eq!(job1.data(), &Vec::from("process-vid2".as_bytes()));
        assert_eq!(job1.state().channel(), "videos");
        assert_eq!(job1.metrics().reserves(), 1);
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

