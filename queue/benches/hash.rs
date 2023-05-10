use ahash::{RandomState};
use criterion::{criterion_group, criterion_main, Criterion, BatchSize, BenchmarkId};
use dashmap::DashMap;
use homemaker_queue::{
    channel::Channel,
};
use std::collections::HashMap;

fn benchmarks(c: &mut Criterion) {
    let mut group = c.benchmark_group("hash::fill");
    for hash_size in [10, 100, 1000].iter() {
        group.bench_with_input(BenchmarkId::new("HashMap", hash_size), hash_size, |b, &size| {
            b.iter(|| {
                let mut map = HashMap::new();
                for i in 0..size {
                    map.insert(format!("chan-{}", i), Channel::new());
                }
            });
        });
        group.bench_with_input(BenchmarkId::new("DashMap::stock", hash_size), hash_size, |b, &size| {
            b.iter(|| {
                let map = DashMap::new();
                for i in 0..size {
                    map.insert(format!("chan-{}", i), Channel::new());
                }
            });
        });
        group.bench_with_input(BenchmarkId::new("DashMap::ahash", hash_size), hash_size, |b, &size| {
            b.iter(|| {
                let map = DashMap::with_hasher(RandomState::new());
                for i in 0..size {
                    map.insert(i, Channel::new());
                }
            });
        });
    }
    group.finish();

    let mut group = c.benchmark_group("hash::get_seq");
    for hash_size in [10, 100, 1000].iter() {
        group.bench_with_input(BenchmarkId::new("HashMap", hash_size), hash_size, |b, &size| {
            let mut map = HashMap::new();
            for i in 0..size {
                map.insert(i, Channel::new());
            }
            b.iter_batched(|| map.clone(), |map| {
                for i in 0..size {
                    let val = map.get(&i).unwrap();
                    drop(val);
                }
            }, BatchSize::SmallInput);
        });
        group.bench_with_input(BenchmarkId::new("DashMap::stock", hash_size), hash_size, |b, &size| {
            let map = DashMap::new();
            for i in 0..size {
                map.insert(i, Channel::new());
            }
            b.iter_batched(|| map.clone(), |map| {
                for i in 0..size {
                    let val = map.get(&i).unwrap();
                    drop(val);
                }
            }, BatchSize::SmallInput);
        });
        group.bench_with_input(BenchmarkId::new("DashMap::ahash", hash_size), hash_size, |b, &size| {
            let map = DashMap::with_hasher(RandomState::new());
            for i in 0..size {
                map.insert(i, Channel::new());
            }
            b.iter_batched(|| map.clone(), |map| {
                for i in 0..size {
                    let val = map.get(&i).unwrap();
                    drop(val);
                }
            }, BatchSize::SmallInput);
        });
    }
    group.finish();
}

criterion_group!(benches, benchmarks);
criterion_main!(benches);


