// Copyright (c) 2026 Chris Corbyn <chris@zizq.io>
// Licensed under the Business Source License 1.1. See LICENSE file for details.

//! Where the writer's and finisher's time goes.
//!
//! Wall-clock time per stage, not CPU time: a thread blocked on a lock,
//! on the journal, or on the other thread uses no CPU, and would look idle
//! when it is the bottleneck. Logged at debug level, as one line per
//! thread every few seconds, e.g. with
//! `ZIZQ_LOG_FILTER=warn,zizq=info,zizq::store::writer=debug`.

use std::time::{Duration, Instant};

/// How often each thread logs its breakdown.
const LOG_EVERY: Duration = Duration::from_secs(5);

/// Time spent in each of a thread's stages since it last logged.
pub(super) struct Stages<const N: usize> {
    thread: &'static str,
    names: [&'static str; N],
    spent: [Duration; N],
    passes: u64,
    since: Instant,
}

impl<const N: usize> Stages<N> {
    pub(super) fn new(thread: &'static str, names: [&'static str; N]) -> Self {
        Self {
            thread,
            names,
            spent: [Duration::ZERO; N],
            passes: 0,
            since: Instant::now(),
        }
    }

    /// Time `f` as `stage`.
    pub(super) fn time<T>(&mut self, stage: usize, f: impl FnOnce() -> T) -> T {
        let started = Instant::now();
        let out = f();
        self.spent[stage] += started.elapsed();
        out
    }

    pub(super) fn add(&mut self, stage: usize, spent: Duration) {
        self.spent[stage] += spent;
    }

    /// Count a pass, and log the breakdown if it is time to.
    pub(super) fn pass(&mut self) {
        self.passes += 1;
        let window = self.since.elapsed();
        if window < LOG_EVERY {
            return;
        }

        if tracing::enabled!(tracing::Level::DEBUG) {
            let total = window.as_secs_f64();
            let breakdown: Vec<String> = self
                .names
                .iter()
                .zip(&self.spent)
                .map(|(name, spent)| format!("{name}={:.0}%", spent.as_secs_f64() / total * 100.0))
                .collect();
            tracing::debug!(
                thread = self.thread,
                passes = self.passes,
                window_ms = window.as_millis() as u64,
                "{}",
                breakdown.join(" "),
            );
        }

        self.spent = [Duration::ZERO; N];
        self.passes = 0;
        self.since = Instant::now();
    }
}
