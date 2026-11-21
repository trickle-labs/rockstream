//! Preemptive fair chunk scheduler for resumable operators (Issue #402 R4).
//!
//! Coordinates expensive, chunked operators ([`ResumableChunkedOp`], such as
//! high-fanout joins) alongside protected low-latency views:
//! - Heavy operators advance in bounded [`ChunkBudget`] quanta (`step_chunk`)
//!   and yield between chunks.
//! - [`WorkloadPriority::Protected`] tasks preempt lower-priority work at every
//!   chunk boundary so interactive/protected views meet freshness SLOs even
//!   under heavy background fanout.
//! - [`WorkloadPriority::Standard`] and [`WorkloadPriority::Background`] share
//!   remaining quanta with weighted round-robin fairness (3:1) so neither
//!   starves.
//! - Every chunked task honors [`CancellationToken`] cleanly.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use rockstream_ops::{ArrowZSet, ChunkBudget, ChunkStepResult, OpError, ResumableChunkedOp};
use tokio::sync::{oneshot, Notify};
use tokio_util::sync::CancellationToken;

/// Scheduling priority class for worker execution quanta.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum WorkloadPriority {
    /// Latency-sensitive views preempting heavy work between chunks.
    Protected = 0,
    /// Normal streaming maintenance work.
    Standard = 1,
    /// Heavy high-fanout joins, backfills, or catch-up jobs.
    Background = 2,
}

/// Summary of latency and scheduling metrics for protected workloads.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct WorkloadFreshnessStats {
    pub samples: usize,
    pub min_latency: Duration,
    pub max_latency: Duration,
    pub p99_latency: Duration,
    pub mean_latency: Duration,
    pub total_chunks: u64,
    pub preemptions: u64,
    pub cancelled_tasks: u64,
}

/// Completion outcome of a scheduled [`ResumableChunkedOp`].
#[derive(Debug)]
pub enum ChunkedTaskOutcome {
    Completed {
        outputs: Vec<ArrowZSet>,
        chunks: u64,
        total_rows: usize,
        total_probes: usize,
    },
    Cancelled {
        chunks_before_cancel: u64,
    },
}

struct ProtectedJob {
    submitted_at: Instant,
    work: Box<dyn FnOnce() + Send + 'static>,
}

struct ChunkedTask {
    op: Box<dyn ResumableChunkedOp + Send>,
    budget: ChunkBudget,
    cancel: CancellationToken,
    outputs: Vec<ArrowZSet>,
    chunks: u64,
    total_rows: usize,
    total_probes: usize,
    reply: Option<oneshot::Sender<Result<ChunkedTaskOutcome, OpError>>>,
}

#[derive(Default)]
struct SchedulerQueues {
    protected: VecDeque<ProtectedJob>,
    standard: VecDeque<ChunkedTask>,
    background: VecDeque<ChunkedTask>,
    protected_latencies: Vec<Duration>,
    standard_burst: u8,
}

struct SchedulerState {
    queues: Mutex<SchedulerQueues>,
    notify: Notify,
    shutdown: AtomicBool,
    total_chunks: AtomicU64,
    preemptions: AtomicU64,
    cancelled_tasks: AtomicU64,
}

/// Preemptive chunk-level scheduler for fair multi-tenant worker execution.
#[derive(Clone)]
pub struct FairChunkScheduler {
    state: Arc<SchedulerState>,
}

/// Handle to a running background [`FairChunkScheduler`] task.
pub struct FairSchedulerHandle {
    scheduler: FairChunkScheduler,
    join: Option<tokio::task::JoinHandle<()>>,
}

impl FairSchedulerHandle {
    pub fn scheduler(&self) -> &FairChunkScheduler {
        &self.scheduler
    }

    /// Gracefully stop the scheduler loop once queued work drains or immediately on shutdown.
    pub async fn shutdown(mut self) {
        self.scheduler.shutdown();
        if let Some(handle) = self.join.take() {
            let _ = handle.await;
        }
    }
}

impl Drop for FairSchedulerHandle {
    fn drop(&mut self) {
        self.scheduler.shutdown();
        if let Some(handle) = self.join.take() {
            handle.abort();
        }
    }
}

impl Default for FairChunkScheduler {
    fn default() -> Self {
        Self::new()
    }
}

impl FairChunkScheduler {
    pub fn new() -> Self {
        Self {
            state: Arc::new(SchedulerState {
                queues: Mutex::new(SchedulerQueues::default()),
                notify: Notify::new(),
                shutdown: AtomicBool::new(false),
                total_chunks: AtomicU64::new(0),
                preemptions: AtomicU64::new(0),
                cancelled_tasks: AtomicU64::new(0),
            }),
        }
    }

    /// Spawn a background Tokio worker task driving this scheduler.
    pub fn spawn(self) -> FairSchedulerHandle {
        let sched = self.clone();
        let join = tokio::spawn(async move {
            sched.run_loop().await;
        });
        FairSchedulerHandle {
            scheduler: self,
            join: Some(join),
        }
    }

    /// Signal the scheduler loop to exit once idle.
    pub fn shutdown(&self) {
        self.state.shutdown.store(true, Ordering::Release);
        self.state.notify.notify_waiters();
    }

    /// Submit a protected low-latency task that preempts chunked operators at
    /// the next chunk boundary and records its end-to-end freshness latency.
    pub fn submit_protected<F, R>(&self, work: F) -> oneshot::Receiver<R>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        let (tx, rx) = oneshot::channel();
        let job = ProtectedJob {
            submitted_at: Instant::now(),
            work: Box::new(move || {
                let res = work();
                let _ = tx.send(res);
            }),
        };
        self.state.queues.lock().protected.push_back(job);
        self.state.notify.notify_one();
        rx
    }

    /// Submit a resumable chunked operator to run under `priority` with `budget`
    /// and cancellation support.
    pub fn submit_chunked<O>(
        &self,
        priority: WorkloadPriority,
        op: O,
        budget: ChunkBudget,
        cancel: CancellationToken,
    ) -> oneshot::Receiver<Result<ChunkedTaskOutcome, OpError>>
    where
        O: ResumableChunkedOp + Send + 'static,
    {
        let (tx, rx) = oneshot::channel();
        let task = ChunkedTask {
            op: Box::new(op),
            budget,
            cancel,
            outputs: Vec::new(),
            chunks: 0,
            total_rows: 0,
            total_probes: 0,
            reply: Some(tx),
        };
        {
            let mut queues = self.state.queues.lock();
            match priority {
                WorkloadPriority::Protected | WorkloadPriority::Standard => {
                    queues.standard.push_back(task);
                }
                WorkloadPriority::Background => {
                    queues.background.push_back(task);
                }
            }
        }
        self.state.notify.notify_one();
        rx
    }

    /// Snapshot latency and preemption statistics for protected workloads.
    pub fn freshness_stats(&self) -> WorkloadFreshnessStats {
        let mut latencies = self.state.queues.lock().protected_latencies.clone();
        let total_chunks = self.state.total_chunks.load(Ordering::Relaxed);
        let preemptions = self.state.preemptions.load(Ordering::Relaxed);
        let cancelled_tasks = self.state.cancelled_tasks.load(Ordering::Relaxed);
        if latencies.is_empty() {
            return WorkloadFreshnessStats {
                total_chunks,
                preemptions,
                cancelled_tasks,
                ..Default::default()
            };
        }
        latencies.sort_unstable();
        let samples = latencies.len();
        let min_latency = latencies[0];
        let max_latency = latencies[samples - 1];
        let p99_idx = ((samples * 99).saturating_sub(1)) / 100;
        let p99_latency = latencies[p99_idx.min(samples - 1)];
        let sum_nanos: u128 = latencies.iter().map(|d| d.as_nanos()).sum();
        let mean_latency = Duration::from_nanos((sum_nanos / samples as u128) as u64);
        WorkloadFreshnessStats {
            samples,
            min_latency,
            max_latency,
            p99_latency,
            mean_latency,
            total_chunks,
            preemptions,
            cancelled_tasks,
        }
    }

    /// Execute one scheduling step: drain protected work first, then run at most
    /// one chunk of a standard or background resumable operator.
    ///
    /// Returns `true` if any work ran.
    pub fn step_once(&self) -> bool {
        // 1. Always prioritize Protected tasks.
        let protected_job = {
            let mut queues = self.state.queues.lock();
            let job = queues.protected.pop_front();
            if job.is_some() && (!queues.standard.is_empty() || !queues.background.is_empty()) {
                self.state.preemptions.fetch_add(1, Ordering::Relaxed);
            }
            job
        };
        if let Some(job) = protected_job {
            (job.work)();
            let elapsed = job.submitted_at.elapsed();
            self.state.queues.lock().protected_latencies.push(elapsed);
            return true;
        }

        // 2. Pick next chunked task using 3:1 weighted fairness between Standard and Background.
        let picked = {
            let mut queues = self.state.queues.lock();
            let take_standard = if queues.standard.is_empty() {
                false
            } else if queues.background.is_empty() {
                true
            } else if queues.standard_burst < 3 {
                queues.standard_burst += 1;
                true
            } else {
                queues.standard_burst = 0;
                false
            };
            if take_standard {
                queues
                    .standard
                    .pop_front()
                    .map(|t| (WorkloadPriority::Standard, t))
            } else {
                queues
                    .background
                    .pop_front()
                    .map(|t| (WorkloadPriority::Background, t))
            }
        };

        let Some((priority, mut task)) = picked else {
            return false;
        };

        let step_result = task.op.step_chunk(&task.budget, &task.cancel);
        task.chunks += 1;
        self.state.total_chunks.fetch_add(1, Ordering::Relaxed);

        match step_result {
            Ok(ChunkStepResult::Yielded {
                partial_output,
                stats,
            }) => {
                task.total_rows += stats.output_rows;
                task.total_probes += stats.probes;
                if !partial_output.is_empty() {
                    task.outputs.push(partial_output);
                }
                let mut queues = self.state.queues.lock();
                match priority {
                    WorkloadPriority::Protected | WorkloadPriority::Standard => {
                        queues.standard.push_back(task);
                    }
                    WorkloadPriority::Background => {
                        queues.background.push_back(task);
                    }
                }
            }
            Ok(ChunkStepResult::Completed {
                final_output,
                stats,
            }) => {
                task.total_rows += stats.output_rows;
                task.total_probes += stats.probes;
                if !final_output.is_empty() {
                    task.outputs.push(final_output);
                }
                if let Some(reply) = task.reply.take() {
                    let _ = reply.send(Ok(ChunkedTaskOutcome::Completed {
                        outputs: task.outputs,
                        chunks: task.chunks,
                        total_rows: task.total_rows,
                        total_probes: task.total_probes,
                    }));
                }
            }
            Ok(ChunkStepResult::Cancelled { .. }) => {
                self.state.cancelled_tasks.fetch_add(1, Ordering::Relaxed);
                if let Some(reply) = task.reply.take() {
                    let _ = reply.send(Ok(ChunkedTaskOutcome::Cancelled {
                        chunks_before_cancel: task.chunks,
                    }));
                }
            }
            Err(err) => {
                if let Some(reply) = task.reply.take() {
                    let _ = reply.send(Err(err));
                }
            }
        }
        true
    }

    async fn run_loop(&self) {
        loop {
            if self.step_once() {
                tokio::task::yield_now().await;
                continue;
            }
            if self.state.shutdown.load(Ordering::Acquire) {
                break;
            }
            self.state.notify.notified().await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rockstream_ops::ChunkStats;

    struct CountingOp {
        remaining_chunks: usize,
    }

    impl ResumableChunkedOp for CountingOp {
        fn step_chunk(
            &mut self,
            _budget: &ChunkBudget,
            cancel: &CancellationToken,
        ) -> Result<ChunkStepResult<ArrowZSet>, OpError> {
            if cancel.is_cancelled() {
                return Ok(ChunkStepResult::Cancelled {
                    stats: ChunkStats::default(),
                });
            }
            self.remaining_chunks = self.remaining_chunks.saturating_sub(1);
            let out = ArrowZSet::from_ab_rows(&[(self.remaining_chunks as i64, 1)], 1);
            let stats = ChunkStats {
                output_rows: 1,
                output_bytes: 24,
                probes: 1,
                cpu_time: Duration::from_micros(50),
            };
            if self.remaining_chunks == 0 {
                Ok(ChunkStepResult::Completed {
                    final_output: out,
                    stats,
                })
            } else {
                Ok(ChunkStepResult::Yielded {
                    partial_output: out,
                    stats,
                })
            }
        }
    }

    #[tokio::test]
    async fn protected_task_preempts_background_chunked_work() {
        let sched = FairChunkScheduler::new();
        let cancel = CancellationToken::new();
        let bg_rx = sched.submit_chunked(
            WorkloadPriority::Background,
            CountingOp {
                remaining_chunks: 10,
            },
            ChunkBudget::default(),
            cancel,
        );

        // Advance one background chunk, then submit a protected task.
        assert!(sched.step_once());
        let prot_rx = sched.submit_protected(|| 42i64);
        assert!(sched.step_once());
        assert_eq!(prot_rx.await.unwrap(), 42);

        // Drain the remaining background chunks.
        while sched.step_once() {}
        let outcome = bg_rx.await.unwrap().unwrap();
        match outcome {
            ChunkedTaskOutcome::Completed {
                chunks, total_rows, ..
            } => {
                assert_eq!(chunks, 10);
                assert_eq!(total_rows, 10);
            }
            other => panic!("expected Completed, got {other:?}"),
        }
        let stats = sched.freshness_stats();
        assert_eq!(stats.samples, 1);
        assert_eq!(stats.preemptions, 1);
        assert_eq!(stats.total_chunks, 10);
    }
}
