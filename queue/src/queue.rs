use crate::{
    channel::Channel,
    error::{Error, Result},
    job::{Delay, Job, JobID, JobState, JobStatus, Priority},
    store::{Store},
};
use dashmap::DashMap;

/// The main queue datastructure...we create one of these for the process and it
/// manages all of our jobs and channels for us.
#[derive(Debug, Default)]
pub struct Queue<S> {
    /// Holds our queue channels
    pub(crate) channels: DashMap<String, Channel>,
    /// Our queue's primary backing store. Stores critical job data that we simply cannot afford
    /// to lose.
    db_primary: S,
    /// Our queue's secondary store, for storing dumb bullshit we don't care that much about like
    /// job stats and stuff.
    db_meta: S,
}

impl<S: Store> Queue<S> {
    /// Create a new `Queue` object. This will read the underlying storage object and recreate
    /// the jobs and channels within that storage layer in-memory.
    pub fn new(db_primary: S, db_meta: S, estimated_num_channels: usize) -> Result<Self> {
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
        /*
        let channel_name = channel_name.into();
        let job_id_val = self.db.gen_job_id()?;
        let job_id = JobID::from(job_id_val);
        let state = JobState::new(channel_name.clone(), priority, JobStatus::Ready, ttr, delay);
        let job = Job::new(job_id.clone(), job_data, state);
        //self.db_primary.insert(
        self.db.store_job_data(&job)?;
        self.db.store_job_meta(&job)?;

        let mut channel = self.channels.entry(channel_name).or_insert_with(|| Channel::new());
        if let Some(delay_ts) = delay {
            channel.push_delayed(job_id.clone(), priority, delay_ts);
        } else {
            channel.push_ready(job_id.clone(), priority);
        }
        Ok(job_id)
        */
        Ok(JobID::from(self.db_primary.gen_id()?))
    }

    /// Pull a single job off the given list of channels.
    ///
    /// This works by reading the channels' ready jobs ([`Channel::peek_ready`]) and picking the
    /// one with the lowest priority/[`JobID`]. This is obviously imperfect because if the most
    /// available job is on the firest channel, and we go reading five other channels to see if
    /// there's a better job, by the time we conclude the first was the best it may have already
    /// been reserved by some other jerk (the wonders of parallelism).
    pub fn dequeue(&self, channels: &Vec<String>, grab_meta: bool) -> Result<Option<Job>> {
        /*
        let reserve_from_channel = |channel: &str, grab_meta: bool| -> Result<Option<Job>> {
            let maybe_job = self.channels.get_mut(channel)
                .and_then(|mut c| c.reserve_next());
            match maybe_job {
                Some(job_id) => {
                    match self.db.get_job_data(&job_id)? {
                        Some(store) => {
                            let meta = if grab_meta {
                                self.db.get_job_meta(&job_id)?
                            } else {
                                None
                            };

                            let mut job = Job::create_from_parts(job_id, store, meta);
                            job.metrics.reserves += 1;
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
            reserve_from_channel(&channels[0], grab_meta)
        } else {
            // loop over our channels one by one, peeking jobs and at the end we'll reserve from
            // the chan with the lowest priority/job_id. not perfect because there's a chance
            // the channel we pick will have gotten that job reserved by the time we finish.
            let mut lowest = None;
            for chan in channels {
                let maybe_job = self.channels.get_mut(chan)
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
                reserve_from_channel(chan, grab_meta)
            } else {
                Ok(None)
            }
        }
        */
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        store::memory::MemoryStore,
    };

    /*
    #[test]
    fn enqueue() {
        let queue1 = Queue::new(MemoryStore::new(), 32).unwrap();
        assert_eq!(queue1.channels.len(), 0);
        assert_eq!(queue1.channels.contains_key("blixtopher"), false);
        queue1.enqueue("blixtopher", Vec::from("get a job".as_bytes()), 1024, 300, None::<Delay>).unwrap();
        assert_eq!(queue1.channels.len(), 1);
        assert_eq!(queue1.channels.contains_key("blixtopher"), true);
        assert_eq!(queue1.channels.get("blixtopher").unwrap().metrics.ready, 1);
        assert_eq!(queue1.channels.get("blixtopher").unwrap().metrics.delayed, 0);
        queue1.enqueue("blixtopher", Vec::from("say hello".as_bytes()), 1024, 300, None::<Delay>).unwrap();
        assert_eq!(queue1.channels.len(), 1);
        assert_eq!(queue1.channels.contains_key("blixtopher"), true);
        assert_eq!(queue1.channels.get("blixtopher").unwrap().metrics.ready, 2);
        assert_eq!(queue1.channels.get("blixtopher").unwrap().metrics.delayed, 0);

        let queue2 = Queue::new(MemoryStore::new(), 128).unwrap();
        assert_eq!(queue2.channels.len(), 0);
        assert_eq!(queue2.channels.contains_key("blixtopher"), false);
        queue2.enqueue("blixtopher", Vec::from("get a job".as_bytes()), 1024, 300, Some(696969696)).unwrap();
        assert_eq!(queue2.channels.len(), 1);
        assert_eq!(queue2.channels.contains_key("blixtopher"), true);
        assert_eq!(queue2.channels.get("blixtopher").unwrap().metrics.ready, 0);
        assert_eq!(queue2.channels.get("blixtopher").unwrap().metrics.delayed, 1);
    }

    #[test]
    fn dequeue() {
        let queue1  = Queue::new(MemoryStore::new(), 32).unwrap();
        queue1.enqueue("videos", Vec::from("process-vid1".as_bytes()), 1024, 300, None::<Delay>).unwrap();
        queue1.enqueue("videos", Vec::from("process-vid2".as_bytes()), 1000, 300, None::<Delay>).unwrap();
        queue1.enqueue("videos", Vec::from("process-vid3".as_bytes()), 1000, 300, None::<Delay>).unwrap();
        queue1.enqueue("downloads", Vec::from("https://post.vidz.ru/vidoes/YOUR-WIFE-WITH-THE-NEIGHBOR.rar.mp4.avi.exe".as_bytes()), 1001, 300, None::<Delay>).unwrap();

        let job1 = queue1.dequeue()
    }
    */

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

