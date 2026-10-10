//! Generating load: enqueueing jobs and draining them with workers.

use std::convert::Infallible;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use serde::{Deserialize, Serialize};
use tokio::sync::watch;
use tokio::task::JoinSet;
use zizq::{Client, JobKind, Router, Worker};

use crate::Error;

/// Progress shared with the sampler.
#[derive(Default)]
pub struct Counters {
    pub enqueued: AtomicU64,
    pub completed: AtomicU64,
}

/// The job processed by the benchmark. The handler does nothing, so the
/// measurement is of the queue rather than of the work.
#[derive(Serialize, Deserialize)]
struct Bench {
    n: u64,
}

impl JobKind for Bench {
    const NAME: &'static str = "bench";
    const QUEUE: &'static str = "bench";
}

/// Enqueue `jobs` jobs in bulk batches of `batch_size`, with up to
/// `concurrency` batches in flight.
pub async fn enqueue(
    client: Client,
    jobs: u64,
    batch_size: u64,
    concurrency: usize,
    counters: Arc<Counters>,
) -> Result<(), Error> {
    let cursor = Arc::new(AtomicU64::new(0));
    let mut tasks = JoinSet::new();

    for _ in 0..concurrency {
        let client = client.clone();
        let cursor = cursor.clone();
        let counters = counters.clone();

        tasks.spawn(async move {
            loop {
                let start = cursor.fetch_add(batch_size, Ordering::Relaxed);
                if start >= jobs {
                    return Ok::<(), Error>(());
                }
                let end = (start + batch_size).min(jobs);

                let mut batch = client.enqueue_bulk();
                for n in start..end {
                    batch.push(client.enqueue(Bench { n }));
                }
                batch.await?;

                counters.enqueued.fetch_add(end - start, Ordering::Relaxed);
            }
        });
    }

    while let Some(result) = tasks.join_next().await {
        result??;
    }

    Ok(())
}

/// Run `workers` workers until `jobs` jobs have been processed, and their
/// acknowledgements sent.
pub async fn drain(
    client: Client,
    jobs: u64,
    workers: usize,
    concurrency: usize,
    prefetch: usize,
    counters: Arc<Counters>,
) -> Result<(), Error> {
    let (done_tx, done_rx) = watch::channel(false);
    let done_tx = Arc::new(done_tx);
    let mut tasks = JoinSet::new();

    for _ in 0..workers {
        let counters = counters.clone();
        let done_tx = done_tx.clone();
        let mut done_rx = done_rx.clone();

        let worker = Worker::builder()
            .client(client.clone())
            .concurrency(concurrency)
            .prefetch(prefetch)
            .queues(vec![Bench::QUEUE])
            .handler(Router::new().route(move |_job: Bench| {
                let counters = counters.clone();
                let done_tx = done_tx.clone();
                async move {
                    if counters.completed.fetch_add(1, Ordering::Relaxed) + 1 == jobs {
                        done_tx.send_replace(true);
                    }
                    Ok::<(), Infallible>(())
                }
            }))
            .build()?;

        tasks.spawn(async move {
            worker
                .run(async move {
                    let _ = done_rx.wait_for(|done| *done).await;
                })
                .await
        });
    }

    while let Some(result) = tasks.join_next().await {
        result??;
    }

    Ok(())
}
