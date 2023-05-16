# Homemaker

A high-throughput, multithreaded work queue in the lineage of (and protocol-compatible with)
[beanstalkd](https://beanstalkd.github.io/).

This project is dedicated to all the people who work tirelessly in the background, doing essential
and thankless work that goes unnoticed but keeps society running.

## Work in progress

This system cannot be used for anything yet. Unless you are helping to build it, you probably
want to hold off on using it until further notice.

## Work queue?

Any time you need to defer work on something, you might reach for a work queue. It could be
processing a video file, grabbing data from a third-party, performing system maintenance, etc.
Often you'll implement this as a redis list, or maybe a table in postgres. These are fine
solutions. For a while. Then your redis instance needs to scale up, your postgres instance starts
to get thrashed. You hit an upper threshold and can't keep up with the incoming jobs.

You start looking at general messaging systems like RabbitMQ or Kafka...but these are not queuing
systems, they are messaging and logging systems. Can they be used as a queue? Sure, but not
with any elegance.

This is where things like beanstalkd, and now Homemaker, come into play. They are dedicated systems
for running high-throughput queues. Think along the lines of millions of jobs per second. They
have features dedicated to ensuring at-least-once job processing, prioritization and
segmentation of jobs, retrying and failing jobs, and overall data structures that support running
a buttload of jobs/s, while using pull semantics so your worker processes can control how much they
get at once.

## How is Homemaker different?

Beanstalkd is an exceptionally great queue. Its protocol is simple to implement and understand and
its performance is top notch. So why build a successor to it?

Homemaker tries to solve a few problems that would be nearly impossible to build into beanstalkd
without a full rewrite.

### Multithreading

Beanstalkd is single-threaded. This means you can stick it on an 8-core machine, but it will only
ever use one core. Homemaker makes use of the available cores in your system, allowing you scale up
throughput with server size.

It partitions thread work on a channel-based ("tube" in beanstalkd) level, so the more channels you
have the more cores you use.

### Storage tiers

Beanstalkd can save all writes to a binlog which allows for recovery when the process crashes.
However, all writes are put into the binlog equally, and in high-throughput situations the
bottleneck often becomes the disk. This forces a trade-off between slow performance and durability
or high-throughput and volatililty.

Homemaker also stores to disk but adds another dimension: it has two storage tiers. The primary
tier stores essential and static job data. This tier can be tuned to have high durability to
protect from failure. But it also has a second tier for storing more rapidly-changing data like
delay values or job statistics. This allows for dynamic but less important data to be saved to
disk much less frequently (or not at all). This architecture opens more possibilities for
durability *without sacrificing throughput.*

### Error messages

Beanstalkd's `bury` command allows for putting failed jobs to the side for inspection, but doesn't
allow storing any indication of *why the job failed*. You can run the job again, but often the
failure conditions no longer exist and it becomes difficult to debug why something broke.

Homemaker allows storing failure data with failed jobs to make debugging easier.

### Metrics

Homemaker exposes global and per-channel statistics as prometheus metrics so you can monitor
it directly.

## License

