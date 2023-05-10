use criterion::{black_box, criterion_group, criterion_main, Criterion, BatchSize, BenchmarkId};
use homemaker_queue::{
    channel::Channel,
    job::{Delay, FailID, JobID, Priority},
};
use std::ops::Deref;

fn create_channel<D>(num: usize, delay: Option<Vec<D>>) -> Channel
    where D: Into<Delay> + Clone + std::fmt::Debug
{
    let mut channel = Channel::new();
    for i in 0..num {
        let delayval = delay.as_ref().map(|x| x[i % x.len()].clone());
        channel.push(JobID::from(i as u64), 1024, delayval);
    }
    channel
}

fn create_channel_varying_pri<D>(num: usize, priority_range: (usize, usize), delay: Option<Vec<D>>) -> Channel
    where D: Into<Delay> + Clone + std::fmt::Debug
{
    let mut channel = Channel::new();
    let diff = priority_range.1 - priority_range.0;
    for i in 0..num {
        let priority = (i % diff) + priority_range.0;
        let delayval = delay.as_ref().map(|x| x[i % x.len()].clone());
        channel.push(JobID::from(i as u64), priority as u16, delayval);
    }
    channel
}

fn reserve_delete(mut channel: Channel) {
    while let Some(job) = channel.reserve() {
        channel.delete_reserved(job);
    }
}

fn reserve_release_delay(mut channel: Channel, delay_buckets: usize) {
    let mut d = 0;
    while let Some(job) = channel.reserve() {
        channel.release(job, None::<Priority>, Some(((d % delay_buckets) + 1) as i64)).unwrap();
        d += 1;
    }
}

fn reserve_fail(mut channel: Channel) {
    while let Some(job) = channel.reserve() {
        channel.fail_reserved(FailID::from(job.deref().clone()), job).unwrap();
    }
}

fn kick_failed(mut channel: Channel, num: usize) {
    channel.kick_jobs_failed(num);
}

fn kick_delayed(mut channel: Channel, num: usize) {
    channel.kick_jobs_delayed(num);
}

fn expire_delayed(mut channel: Channel, now: &Delay) {
    channel.expire_delayed(now);
}

fn benchmarks(c: &mut Criterion) {
    let num_jobs = 100_000;

    let mut group = c.benchmark_group("channel::push");
    group.bench_function("pri-s", |b| b.iter(|| create_channel(black_box(num_jobs), black_box(None::<Vec<Delay>>))));
    group.bench_function("pri-d", |b| b.iter(|| create_channel_varying_pri(black_box(num_jobs), (black_box(1000), black_box(5000)), black_box(None::<Vec<Delay>>))));
    group.finish();

    let mut group = c.benchmark_group("channel::reserve_delete");
    group.bench_function("pri-s", |b| {
        let channel = create_channel(num_jobs, None::<Vec<Delay>>);
        b.iter_batched(|| channel.clone(), |channel| reserve_delete(channel), BatchSize::SmallInput)
    });
    group.bench_function("pri-d", |b| {
        let channel = create_channel_varying_pri(num_jobs, (1000, 5000), None::<Vec<Delay>>);
        b.iter_batched(|| channel.clone(), |channel| reserve_delete(channel), BatchSize::SmallInput)
    });
    group.finish();

    let mut group = c.benchmark_group("channel::reserve_release_delay");
    for delay_bucket in [1, 1000, 100_000].iter() {
        group.bench_with_input(BenchmarkId::new("pri-s", delay_bucket), delay_bucket, |b, &delay| {
            let channel = create_channel(num_jobs, None::<Vec<Delay>>);
            b.iter_batched(|| channel.clone(), |channel| reserve_release_delay(channel, delay), BatchSize::SmallInput)
        });
        group.bench_with_input(BenchmarkId::new("pri-d", delay_bucket), delay_bucket, |b, &delay| {
            let channel = create_channel_varying_pri(num_jobs, (1000, 5000), None::<Vec<Delay>>);
            b.iter_batched(|| channel.clone(), |channel| reserve_release_delay(channel, delay), BatchSize::SmallInput)
        });
    }
    group.finish();

    let mut group = c.benchmark_group("channel::reserve_fail");
    group.bench_function("pri-s", |b| {
        let channel = create_channel(num_jobs, None::<Vec<Delay>>);
        b.iter_batched(|| channel.clone(), |channel| reserve_fail(channel), BatchSize::SmallInput)
    });
    group.bench_function("pri-d", |b| {
        let channel = create_channel_varying_pri(num_jobs, (1000, 5000), None::<Vec<Delay>>);
        b.iter_batched(|| channel.clone(), |channel| reserve_fail(channel), BatchSize::SmallInput)
    });
    group.finish();

    let mut group = c.benchmark_group("channel::kick_failed");
    for kick_num in [1, 1000, 100_000].iter() {
        group.bench_with_input(BenchmarkId::new("pri-s", kick_num), kick_num, |b, &num| {
            let mut channel = create_channel(num_jobs, None::<Vec<Delay>>);
            while let Some(job) = channel.reserve() {
                channel.fail_reserved(FailID::from(job.deref().clone()), job);
            }
            b.iter_batched(|| channel.clone(), |channel| kick_failed(channel, num), BatchSize::SmallInput)
        });
        group.bench_with_input(BenchmarkId::new("pri-d", kick_num), kick_num, |b, &num| {
            let mut channel = create_channel_varying_pri(num_jobs, (1000, 5000), None::<Vec<Delay>>);
            while let Some(job) = channel.reserve() {
                channel.fail_reserved(FailID::from(job.deref().clone()), job);
            }
            b.iter_batched(|| channel.clone(), |channel| kick_failed(channel, num), BatchSize::SmallInput)
        });
    }
    group.finish();

    let mut group = c.benchmark_group("channel::kick_delayed");
    for delay_bucket in [1, 1000, 100_000].iter() {
        let kick_num = 100_000; // kick it all
        group.bench_with_input(BenchmarkId::new("pri-s", format!("{}/{}", kick_num, delay_bucket)), delay_bucket, |b, &delay| {
            let buckets = (0..delay.clone()).collect::<Vec<_>>();
            let channel = create_channel(num_jobs, Some(buckets));
            b.iter_batched(|| channel.clone(), |channel| kick_delayed(channel, kick_num), BatchSize::SmallInput)
        });
        group.bench_with_input(BenchmarkId::new("pri-d", format!("{}/{}", kick_num, delay_bucket)), delay_bucket, |b, &delay| {
            let buckets = (0..delay.clone()).collect::<Vec<_>>();
            let channel = create_channel_varying_pri(num_jobs, (1000, 5000), Some(buckets));
            b.iter_batched(|| channel.clone(), |channel| kick_delayed(channel, kick_num), BatchSize::SmallInput)
        });
    }
    group.finish();

    let mut group = c.benchmark_group("channel::expire_delayed");
    for delay_bucket in [1, 1000, 100_000].iter() {
        let now = 100_001;  // expire everything
        group.bench_with_input(BenchmarkId::new("pri-s", format!("{}/{}", now, delay_bucket)), delay_bucket, |b, &delay| {
            let buckets = (0..delay.clone()).collect::<Vec<_>>();
            let channel = create_channel(num_jobs, Some(buckets));
            b.iter_batched(|| channel.clone(), |channel| expire_delayed(channel, &Delay::from(now.clone())), BatchSize::SmallInput)
        });
        group.bench_with_input(BenchmarkId::new("pri-d", format!("{}/{}", now, delay_bucket)), delay_bucket, |b, &delay| {
            let buckets = (0..delay.clone()).collect::<Vec<_>>();
            let channel = create_channel_varying_pri(num_jobs, (1000, 5000), Some(buckets));
            b.iter_batched(|| channel.clone(), |channel| expire_delayed(channel, &Delay::from(now.clone())), BatchSize::SmallInput)
        });
    }
}

criterion_group!(benches, benchmarks);
criterion_main!(benches);

