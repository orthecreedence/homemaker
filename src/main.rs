use ahash::{RandomState};
use anyhow::{Context, Result};
use bean_protocol::{Error as ProtoError, ParseOptionsBuilder, Request, Response, parse_request};
use bytes::{BytesMut};
use chrono::offset::Utc;
use clap::Parser;
use homemaker_queue::{
    channel::ChannelMod,
    crossbeam_channel::Receiver,
    error::{Error as QueueError},
    job::{Job, JobID, JobStatus, Priority, Timestamp},
    queue::{Ordering, Queue, QueueConfigBuilder},
    sled,
    store::Store,
};
use serde::Serialize;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::net::SocketAddr;
use std::ops::Deref;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufStream};
use tokio::sync::oneshot::error::TryRecvError;
use tracing::log::{trace, debug, info, warn, error};
use tracing_subscriber::{EnvFilter, fmt, prelude::*};

fn setup_logger() -> Result<()> {
    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(
            EnvFilter::try_from_default_env()
                .or_else(|_| EnvFilter::try_new("info"))
                .context("error parsing RUST_LOG env")?
        )
        .init();
    Ok(())
}

async fn handler(socket: TcpStream, client: SocketAddr, queue: Arc<Queue>, loose_ordering: bool) -> Result<(), Box<dyn std::error::Error>> {
    let bean_options = ParseOptionsBuilder::default().build()?;
    let mut reserved_jobs: HashSet<(JobID, Priority), RandomState> = HashSet::default();
    let mut ttr_tracker: BTreeMap<Timestamp, Vec<JobID>> = BTreeMap::new();
    let mut channel_using = String::from("default");
    let mut subscriptions: HashMap<String, Receiver<ChannelMod>, RandomState> = HashMap::default();
    let mut watching: Vec<String> = Vec::new();

    let mark_reserved = |reserved_jobs: &mut HashSet<(JobID, Priority), RandomState>, ttr_tracker: &mut BTreeMap<Timestamp, Vec<JobID>>, job: &Job| {
        reserved_jobs.insert((job.id().clone(), job.state().priority().clone()));
        let mut ttr_entry = ttr_tracker
            .entry(Timestamp::from(Utc::now().timestamp() + *job.state().ttr() as i64))
            .or_insert(Vec::new());
        if ttr_entry.iter().find(|id| *id == job.id()).is_none() {
            ttr_entry.push(job.id().clone());
        }
    };

    let release_reserved = |reserved_jobs: &mut HashSet<(JobID, Priority), RandomState>, ttr_tracker: &mut BTreeMap<Timestamp, Vec<JobID>>, job: &Job| {
        reserved_jobs.remove(&(job.id().clone(), job.state().priority().clone()));
    };

    let release_all_reserved = |reserved_jobs: &mut HashSet<(JobID, Priority), RandomState>, ttr_tracker: &mut BTreeMap<Timestamp, Vec<JobID>>| {
        //let mut guard = reserved_jobs.lock().expect("handler() -- Error locking reserved_jobs");
        for (job_id, priority) in reserved_jobs.iter() {
            match queue.release(&job_id, priority.clone(), None::<Timestamp>) {
                Err(e) => error!("{} -- error releasing reserved job: {}", client, e),
                _ => {}
            }
        }
        reserved_jobs.clear();
        ttr_tracker.clear();
    };

    let tick = |reserved_jobs: &mut HashSet<(JobID, Priority), RandomState>, ttr_tracker: &mut BTreeMap<Timestamp, Vec<JobID>>| {
    };

    let tick_task = tokio::task::spawn(async move {
        loop {

            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    });

    let mut stream = BufStream::new(socket);
    let mut buf = BytesMut::with_capacity(1024);
    'main: loop {
        trace!("{} -- read wait", client);
        let n = match stream.read_buf(&mut buf).await {
            Ok(n) if n == 0 => {
                info!("{} -- read 0 bytes (connection closed) -- exiting", client);
                release_all_reserved(&mut reserved_jobs, &mut ttr_tracker);
                break;
            }
            Ok(n) => n,
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                continue;
            }
            Err(e) => {
                error!("{} -- error reading dumb socket: {:?}", client, e);
                release_all_reserved(&mut reserved_jobs, &mut ttr_tracker);
                break;
            }
        };
        trace!("{} -- read {} bytes", client, n);

        'read: loop {
            macro_rules! next_command {
                () => {
                    match buf.windows(2).position(|window| window == b"\r\n") {
                        Some(pos) => {
                            // throw out until the next CRLF
                            buf = buf.split_off(pos + 2);
                        }
                        None => {
                            // need more data i guess
                            break 'read;
                        }
                    }
                }
            }

            macro_rules! send {
                ($res:expr) => {
                    if let Err(e) = $res.serialize_into(&mut stream).await {
                        error!("{} -- error writing to socket: {:?}", client, e);
                        break 'read;
                    }
                    if let Err(e) = stream.flush().await {
                        error!("{} -- error flushing socket: {:?}", client, e);
                        break 'read;
                    }
                }
            }

            async fn reserve_loop(queue: &Arc<Queue>, watching: &Vec<String>, subscriptions: &HashMap<String, Receiver<ChannelMod>, RandomState>, loose_ordering: bool) -> Result<Job> {
                let job = 'outer: loop {
                    // try to grab a job from our watched channels. if we fail, wait on the
                    // subscription list until we get a hit, then rinse and repeat.
                    let ordering = if loose_ordering { Ordering::Loose } else { Ordering::Strict };
                    let job_maybe = queue.dequeue(&watching, ordering)?;
                    match job_maybe {
                        Some(job) => break job,
                        None => {
                            loop {
                                let mut set = tokio::task::JoinSet::new();
                                for sub in subscriptions.values() {
                                    let sub_clone = sub.clone();
                                    set.spawn(tokio::task::spawn_blocking(move || {
                                        sub_clone.recv()
                                    }));
                                }
                                while let Some(res) = set.join_next().await {
                                    if res.map(|x| x.is_ok()).unwrap_or(false) {
                                        continue 'outer;
                                    }
                                }
                            }
                        }
                    }
                };
                Ok(job)
            }

            let pos = {
                let (rest, cmd) = match parse_request(&buf[..], &bean_options) {
                    Ok(res) => res,
                    Err(ProtoError::UnknownRequest(cmd)) => {
                        let res = Response::UnknownCommand;
                        warn!("{} -- unknown command: {}", client, cmd);
                        next_command! {}
                        send!(res);
                        continue;
                    }
                    Err(ProtoError::Parse(parse_error)) => {
                        let res = Response::BadFormat;
                        warn!("{} -- parse error: {:?}", client, parse_error);
                        next_command! {}
                        send!(res);
                        continue;
                    }
                    Err(ProtoError::ParseIncomplete(_)) => {
                        // we need more data
                        break 'read;
                    }
                    _ => panic!("TODO: finish ProtoError impl"),
                };
                debug!("{} -- cmd: {:?}", client, cmd);
                'cmd: { match cmd {
                    Request::Bury(job_id, priority) => {
                    }
                    Request::Delete(job_id) => {
                    }
                    Request::Ignore(channel) => {
                    }
                    Request::Kick(num) => {
                    }
                    Request::KickJob(job_id) => {
                    }
                    Request::ListTubes => {
                    }
                    Request::ListTubeUsed => {
                    }
                    Request::PauseTube(channel, secs) => {
                    }
                    Request::Peek(job_id) => {
                        let job = match queue.peek_job(&job_id.into()) {
                            Ok(job) => job,
                            Err(QueueError::JobNotFound(_)) => {
                                send!(Response::NotFound);
                                break 'cmd;
                            }
                            Err(e) => {
                                error!("{} -- peek: error: {:?}", client, e);
                                send!(Response::InternalError);
                                break 'cmd;
                            }
                        };
                        send!(Response::Found(job.id().deref().clone(), &job.data()[..]));
                    }
                    Request::PeekBuried => {
                    }
                    Request::PeekDelayed => {
                    }
                    Request::PeekReady => {
                    }
                    Request::Put(priority, delay, ttr, body) => {
                        let delay = match delay {
                            0 => None,
                            _ => Some(Utc::now().timestamp_millis() + ((delay * 1000) as i64)),
                        };
                        let job_id = match queue.enqueue(&channel_using, Vec::from(body), priority, ttr, delay, Utc::now().timestamp_millis()) {
                            Ok(job_id) => job_id,
                            Err(e) => {
                                error!("{} -- put: error: {:?}", client, e);
                                send!(Response::InternalError);
                                break 'cmd;
                            }
                        };
                        send!(Response::Inserted(*job_id.deref()));
                    }
                    Request::Quit => {
                        break 'main;
                    }
                    Request::Release(job_id, priority, delay) => {

                    }
                    Request::Reserve => {
                        let job = match reserve_loop(&queue, &watching, &subscriptions, loose_ordering).await {
                            Ok(job) => {
                                mark_reserved(&mut reserved_jobs, &mut ttr_tracker, &job);
                                job
                            }
                            Err(e) => {
                                error!("{} -- reserve: error: {:?}", client, e);
                                send!(Response::InternalError);
                                break 'cmd;
                            }
                        };
                        send!(Response::Reserved(job.id().deref().clone(), job.data()));
                    }
                    Request::ReserveJob(job_id) => {
                        let job = match queue.dequeue_job(job_id.into()) {
                            Ok(Some(job)) => {
                                mark_reserved(&mut reserved_jobs, &mut ttr_tracker, &job);
                                job
                            }
                            Ok(None) => {
                                send!(Response::NotFound);
                                break 'cmd;
                            }
                            Err(e) => {
                                error!("{} -- reserve-job: error: {:?}", client, e);
                                send!(Response::InternalError);
                                break 'cmd;
                            }
                        };
                    }
                    Request::ReserveWithTimeout(timeout) => {
                        let task_reserve = reserve_loop(&queue, &watching, &subscriptions, loose_ordering);
                        let task_timeout = tokio::task::spawn(async move {
                            tokio::time::sleep(Duration::from_millis(timeout * 1000)).await;
                        });
                        let job = tokio::select! {
                            j = task_reserve => Some(j),
                            _ = task_timeout => None,
                        };
                        match job {
                            Some(Ok(job)) => {
                                mark_reserved(&mut reserved_jobs, &mut ttr_tracker, &job);
                                send!(Response::Reserved(job.id().deref().clone(), job.data()));
                            }
                            Some(Err(e)) => {
                                error!("{} -- reserve: error: {:?}", client, e);
                                send!(Response::InternalError);
                            }
                            None => {
                                send!(Response::TimedOut);
                            }
                        }
                    }
                    Request::Stats => {
                    }
                    Request::StatsJob(job_id) => {
                        let job = match queue.peek_job(&job_id.into()) {
                            Ok(job) => job,
                            Err(QueueError::JobNotFound(_)) => {
                                send!(Response::NotFound);
                                break 'cmd;
                            }
                            Err(e) => {
                                error!("{} -- stats-job: error: {:?}", client, e);
                                send!(Response::InternalError);
                                break 'cmd;
                            }
                        };
                        #[derive(Debug, Serialize)]
                        struct Stats<'a> {
                            id: u64,
                            tube: &'a String,
                            state: &'static str,
                            pri: u32,
                            age: u64,
                            delay: i64,
                            ttr: u32,
                            time_left: u32,
                            file: u32,
                            reserves: u32,
                            timeouts: u32,
                            releases: u32,
                            buries: u32,
                            kicks: u32,
                        }
                        let job_reserved = match queue.job_is_reserved(job.id(), job.state().channel()) {
                            Ok(x) => x,
                            Err(e) => {
                                error!("{} -- stats-job: error: {:?}", client, e);
                                send!(Response::InternalError);
                                break 'cmd;
                            }
                        };
                        let stats = Stats {
                            id: job.id().deref().clone(),
                            tube: job.state().channel(),
                            state: match (job.state().status(), job.state().delay()) {
                                // "delayed" or "reserved" or "buried"
                                (JobStatus::Failed(..), _) => "buried",
                                (_, Some(_)) => "delayed",
                                _ => match job_reserved {
                                    true => "reserved",
                                    false => "ready",
                                },
                            },
                            pri: job.state().priority().deref().clone(),
                            age: ((Utc::now().timestamp_millis() - job.state().created().deref().clone()) / 1000) as u64,
                            // TODO: match beanstalkd here. it uses the actual delay value in
                            // seconds instead of a timestamp or time left
                            delay: job.state().delay().map(|x| x.deref().clone()).unwrap_or(0),
                            ttr: job.state().ttr().clone(),
                            // TODO: match beanstalkd here. time-left is an indicator of TTR
                            // seconds left in the case of reserved, or delay time left in the case
                            // of delayed.
                            time_left: match job_reserved {
                                true => 0,
                                false => 0,
                            },
                            file: 0,
                            reserves: job.metrics().reserves(),
                            timeouts: job.metrics().timeouts(),
                            releases: job.metrics().releases(),
                            buries: job.metrics().fails(),
                            kicks: job.metrics().kicks(),
                        };
                        match serde_yaml::to_string(&stats) {
                            Ok(yaml) => {
                                send!(Response::Ok(yaml.as_str()));
                            }
                            Err(e) => {
                                error!("{} -- stats-job: error: {:?}", client, e);
                                send!(Response::InternalError);
                            }
                        }
                    }
                    Request::StatsTube(channel) => {
                    }
                    Request::Touch(job_id) => {
                    }
                    Request::Use(channel) => {
                        channel_using = String::from(channel);
                        send!(Response::Using(channel));
                    }
                    Request::Watch(channel) => {
                        let subscription = match queue.subscribe(channel) {
                            Ok(sub) => sub,
                            Err(e) => {
                                error!("{} -- watch: error: {:?}", client, e);
                                send!(Response::InternalError);
                                break 'cmd;
                            }
                        };
                        subscriptions.insert(String::from(channel), subscription);
                        watching.push(String::from(channel));
                    }
                } }
                buf.len() - rest.len()
            };
            buf = buf.split_off(pos);
        }
    }
    info!("{} -- exiting", client);
    Ok(())
}

/// A multi-threaded work queue in the beanstalkd family.
///
/// Homemaker closely follows the beanstalkd protocol
/// (https://github.com/beanstalkd/beanstalkd/blob/master/doc/protocol.txt)
/// but adds a number of performance improvements and tradeoffs, making it
/// a great successor to an already amazing queue system.
#[derive(Debug, Parser)]
#[command(max_term_width=100)]
struct Args {
    /// Enable loose ordering (off by default).
    ///
    /// Strict ordering locks all channels whenever a reserve happens in order to ensure
    /// that the next job pulled off the watch list is the highest priority job across all
    /// channels. If loose ordering is enabled, the channels are scanned but they are not locked,
    /// meaning another client could reserve/put in jobs while the scan is happening,
    /// affecting the "correctness" of the ordering.
    ///
    /// Because of the global queue lock, strict ordering is much slower and effectively
    /// negates a lot of the gains one might get from multithreaded design. However, it's
    /// a great option if strict job ordering is more important to you than performance.
    ///
    /// It's important to note that strict vs loose ordering only matters when multiple
    /// channels are being watched. If you only ever watch one channel in your workers,
    /// it would make sense to enable loose ordering.
    #[arg(short = 'o', long, default_value = "false")]
    loose_ordering: bool,

    /// Enable storing delay values in the meta storage layer (off by default).
    ///
    /// Storing delay values in meta makes things *much faster* if you make heavy use of
    /// delays (for instance if using delays for rate limiting jobs) and you tune your
    /// meta datastore to have looser resilience.
    ///
    /// If delay values on jobs are extremely important to you, leave this option unset.
    /// This will store delay values in primary storage which likely has stricter
    /// guarantees.
    #[arg(short = None, long, default_value = "false")]
    delay_in_meta: bool,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    setup_logger()
        .context("Failed to setup logging")?;
    let args = Args::parse();
    let listener = TcpListener::bind("127.0.0.1:9999").await?;
    let queue_config = QueueConfigBuilder::default()
        .delay_in_meta(args.delay_in_meta)
        .build()?;
    let db_primary = sled::Config::default()
        .mode(sled::Mode::HighThroughput)
        .temporary(true)
        .open()?;
    let db_meta = sled::Config::default()
        .mode(sled::Mode::HighThroughput)
        .temporary(true)
        .open()?;
    let store = Store::new(db_primary, db_meta);
    let queue = Arc::new(Queue::new(queue_config, store, 64)?);
    info!("Listening on {}:{}", "127.0.0.1", 9999);

    // start a task that runs queue.tick() every few ms. the tick function will move expired
    // delayed jobs to the ready queue, remove empty channels, etc etc: basic maintenance.
    // we create a signal that we use to tell it t exit when the main listener quits.
    let (tick_quit_tx, mut tick_quit_rx) = tokio::sync::oneshot::channel::<()>();
    let tick_queue = queue.clone();
    let tick_task = tokio::task::spawn(async move {
        loop {
            match tick_quit_rx.try_recv() {
                Err(TryRecvError::Empty) => {}
                _ => break,
            }
            match tick_queue.tick(Utc::now().timestamp_millis()) {
                Err(e) => error!("main()::tick() -- {}", e),
                _ => {}
            }
            tokio::time::sleep(std::time::Duration::from_millis(250)).await;
        }
    });
    loop {
        let (socket, addr) = listener.accept().await?;
        let queue_copy = queue.clone();
        tokio::spawn(async move {
            match handler(socket, addr, queue_copy, args.loose_ordering).await {
                Err(e) => {
                    error!("{} -- hander error: {:?}", addr, e);
                }
                _ => {}
            }
        });
    }
    // tell the tick task to exit
    let _ = tick_quit_tx.send(());
}

