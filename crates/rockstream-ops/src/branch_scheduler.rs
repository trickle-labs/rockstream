//! View dependency graph and concurrent branch scheduler (v0.65.1).
//!
//! Schedules independent branches of the view dependency graph concurrently
//! while preserving topological dependency order and bounding task concurrency
//! (`MAX_CONCURRENT_BRANCH_TASKS = 16`).

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use tokio::sync::{mpsc, Mutex, Semaphore};

use crate::error::OpError;
use crate::zset::ArrowZSet;

/// Maximum number of branch maintenance tasks that may execute concurrently.
///
/// Named upper bound: **`MAX_CONCURRENT_BRANCH_TASKS`**.
pub const MAX_CONCURRENT_BRANCH_TASKS: usize = 16;

/// Directed acyclic graph of view dependencies.
#[derive(Debug, Clone, Default)]
pub struct ViewDependencyGraph {
    /// Maps each view name to its direct dependencies (source tables or parent views).
    views: HashMap<String, Vec<String>>,
}

impl ViewDependencyGraph {
    /// Create a new empty view dependency graph.
    pub fn new() -> Self {
        Self {
            views: HashMap::new(),
        }
    }

    /// Register a view with its input dependencies.
    pub fn add_view(&mut self, view_name: impl Into<String>, deps: Vec<String>) {
        self.views.insert(view_name.into(), deps);
    }

    /// Return direct dependencies for a view.
    pub fn get_view_deps(&self, view_name: &str) -> Option<&[String]> {
        self.views.get(view_name).map(|v| v.as_slice())
    }

    /// Return all registered views and their dependencies.
    pub fn views(&self) -> &HashMap<String, Vec<String>> {
        &self.views
    }

    /// Check if a view exists in the graph.
    pub fn contains_view(&self, view_name: &str) -> bool {
        self.views.contains_key(view_name)
    }

    /// Compute the subset of views affected by the given changed sources (tables or views),
    /// returned in topological order (upstream views before downstream views).
    pub fn reachable_views(&self, changed_sources: &HashSet<String>) -> Vec<String> {
        let mut reachable = changed_sources.clone();
        loop {
            let mut progressed = false;
            for (view_name, deps) in &self.views {
                if deps.iter().any(|dep| reachable.contains(dep))
                    && reachable.insert(view_name.clone())
                {
                    progressed = true;
                }
            }
            if !progressed {
                break;
            }
        }

        let candidates = reachable
            .iter()
            .filter(|name| self.views.contains_key(*name))
            .cloned()
            .collect::<HashSet<_>>();

        let mut indegree = HashMap::new();
        let mut dependents = HashMap::<String, Vec<String>>::new();

        for view in &candidates {
            let deps = self.views.get(view).cloned().unwrap_or_default();
            indegree.insert(
                view.clone(),
                deps.iter().filter(|dep| candidates.contains(*dep)).count(),
            );
            for dep in deps {
                if candidates.contains(&dep) {
                    dependents.entry(dep).or_default().push(view.clone());
                }
            }
        }

        let mut ready = indegree
            .iter()
            .filter(|(_, deg)| **deg == 0)
            .map(|(v, _)| v.clone())
            .collect::<std::collections::BTreeSet<_>>();

        let mut ordered = Vec::with_capacity(candidates.len());
        while let Some(view) = ready.iter().next().cloned() {
            ready.remove(&view);
            ordered.push(view.clone());
            for dep in dependents.get(&view).into_iter().flatten() {
                if let Some(deg) = indegree.get_mut(dep) {
                    *deg -= 1;
                    if *deg == 0 {
                        ready.insert(dep.clone());
                    }
                }
            }
        }
        ordered
    }
}

/// Trait implemented by view delta executors.
#[async_trait::async_trait]
pub trait ViewBranchExecutor: Send + Sync {
    /// Execute maintenance on a view given the deltas of its dependencies.
    async fn execute(
        &self,
        view_name: &str,
        inputs: HashMap<String, ArrowZSet>,
    ) -> Result<ArrowZSet, OpError>;
}

/// Helper wrapper to adapt an async closure into a `ViewBranchExecutor`.
pub struct FnExecutor<F>(pub F);

#[async_trait::async_trait]
impl<F, Fut> ViewBranchExecutor for FnExecutor<F>
where
    F: Fn(&str, HashMap<String, ArrowZSet>) -> Fut + Send + Sync,
    Fut: std::future::Future<Output = Result<ArrowZSet, OpError>> + Send,
{
    async fn execute(
        &self,
        view_name: &str,
        inputs: HashMap<String, ArrowZSet>,
    ) -> Result<ArrowZSet, OpError> {
        (self.0)(view_name, inputs).await
    }
}

/// Bounded concurrent branch scheduler.
pub struct BranchScheduler {
    max_concurrency: usize,
    active_tasks: Arc<AtomicUsize>,
    peak_active_tasks: Arc<AtomicUsize>,
}

impl Default for BranchScheduler {
    fn default() -> Self {
        Self::new()
    }
}

impl BranchScheduler {
    /// Create a branch scheduler with default bounded concurrency (`MAX_CONCURRENT_BRANCH_TASKS = 16`).
    pub fn new() -> Self {
        Self::with_concurrency(MAX_CONCURRENT_BRANCH_TASKS)
    }

    /// Create a branch scheduler with explicit concurrency bound.
    pub fn with_concurrency(max_concurrency: usize) -> Self {
        Self {
            max_concurrency: max_concurrency.max(1),
            active_tasks: Arc::new(AtomicUsize::new(0)),
            peak_active_tasks: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Return the maximum concurrent branch tasks allowed.
    pub fn max_concurrency(&self) -> usize {
        self.max_concurrency
    }

    /// Return the current number of concurrently running branch tasks (`concurrent_branch_active_tasks`).
    pub fn active_tasks(&self) -> usize {
        self.active_tasks.load(Ordering::Relaxed)
    }

    /// Return the peak number of concurrently active tasks observed during execution.
    pub fn peak_active_tasks(&self) -> usize {
        self.peak_active_tasks.load(Ordering::Relaxed)
    }

    /// Reset the peak active tasks counter.
    pub fn reset_peak(&self) {
        self.peak_active_tasks.store(0, Ordering::Relaxed);
    }

    /// Schedule and execute independent view branches concurrently while strictly enforcing
    /// topological dependency order and bounded concurrency.
    ///
    /// If any branch fails, an epoch-wide cancellation is triggered, halting pending tasks
    /// and discarding uncommitted state.
    pub async fn execute_epoch<E: ViewBranchExecutor + 'static>(
        &self,
        graph: &ViewDependencyGraph,
        source_deltas: HashMap<String, ArrowZSet>,
        executor: Arc<E>,
    ) -> Result<HashMap<String, ArrowZSet>, OpError> {
        let changed_sources = source_deltas.keys().cloned().collect::<HashSet<_>>();
        let affected = graph.reachable_views(&changed_sources);
        if affected.is_empty() {
            return Ok(HashMap::new());
        }

        let affected_set: HashSet<String> = affected.iter().cloned().collect();

        // Calculate indegrees among affected views:
        // A view awaits another view only if that view is also in the affected set.
        let mut indegrees = HashMap::new();
        let mut dependents = HashMap::<String, Vec<String>>::new();

        for view in &affected {
            let deps = graph.get_view_deps(view).unwrap_or(&[]);
            let deg = deps.iter().filter(|d| affected_set.contains(*d)).count();
            indegrees.insert(view.clone(), deg);
            for dep in deps {
                if affected_set.contains(dep) {
                    dependents
                        .entry(dep.clone())
                        .or_default()
                        .push(view.clone());
                }
            }
        }

        let total_views = affected.len();
        let semaphore = Arc::new(Semaphore::new(self.max_concurrency));
        let completed_outputs = Arc::new(Mutex::new(HashMap::<String, ArrowZSet>::new()));
        let cancelled = Arc::new(AtomicBool::new(false));
        let error_cell = Arc::new(Mutex::new(None::<OpError>));
        let indegrees = Arc::new(std::sync::Mutex::new(indegrees));
        let dependents = Arc::new(dependents);

        let (ready_tx, mut ready_rx) = mpsc::channel::<String>(total_views + 1);

        // Seed ready queue with nodes whose indegree is 0
        {
            let lock = indegrees.lock().unwrap();
            for (view, &deg) in lock.iter() {
                if deg == 0 {
                    let _ = ready_tx.try_send(view.clone());
                }
            }
        }

        let mut completed_count = 0;
        let (task_done_tx, mut task_done_rx) =
            mpsc::channel::<Result<(String, ArrowZSet), OpError>>(total_views + 1);

        while completed_count < total_views {
            tokio::select! {
                Some(ready_view) = ready_rx.recv() => {
                    if cancelled.load(Ordering::Acquire) {
                        continue;
                    }
                    let permit = semaphore.clone().acquire_owned().await.map_err(|_| {
                        OpError::internal("BranchScheduler semaphore closed")
                    })?;

                    let active_counter = self.active_tasks.clone();
                    let peak_counter = self.peak_active_tasks.clone();
                    let completed_outputs = completed_outputs.clone();
                    let source_deltas_ref = source_deltas.clone();
                    let cancelled_ref = cancelled.clone();
                    let error_cell_ref = error_cell.clone();
                    let task_done_tx = task_done_tx.clone();
                    let graph_deps = graph.get_view_deps(&ready_view).unwrap_or(&[]).to_vec();
                    let executor_ref = executor.clone();

                    let view_name = ready_view;

                    tokio::spawn(async move {
                        let _permit = permit;
                        let active = active_counter.fetch_add(1, Ordering::SeqCst) + 1;
                        let _ = peak_counter.fetch_max(active, Ordering::SeqCst);

                        if cancelled_ref.load(Ordering::Acquire) {
                            active_counter.fetch_sub(1, Ordering::SeqCst);
                            return;
                        }

                        // Assemble inputs for this view from completed outputs and source deltas
                        let mut inputs = HashMap::new();
                        {
                            let outputs = completed_outputs.lock().await;
                            for dep in &graph_deps {
                                if let Some(delta) = outputs.get(dep) {
                                    inputs.insert(dep.clone(), delta.clone());
                                } else if let Some(delta) = source_deltas_ref.get(dep) {
                                    inputs.insert(dep.clone(), delta.clone());
                                }
                            }
                        }

                        let result = executor_ref.execute(&view_name, inputs).await;
                        active_counter.fetch_sub(1, Ordering::SeqCst);

                        match result {
                            Ok(output) => {
                                let _ = task_done_tx.send(Ok((view_name, output))).await;
                            }
                            Err(err) => {
                                cancelled_ref.store(true, Ordering::Release);
                                {
                                    let mut err_guard = error_cell_ref.lock().await;
                                    if err_guard.is_none() {
                                        *err_guard = Some(err);
                                    }
                                }
                                let _ = task_done_tx.send(Err(OpError::internal("Branch failure"))).await;
                            }
                        }
                    });
                }
                Some(task_result) = task_done_rx.recv() => {
                    match task_result {
                        Ok((view_name, output)) => {
                            completed_count += 1;
                            {
                                let mut outputs = completed_outputs.lock().await;
                                outputs.insert(view_name.clone(), output);
                            }

                            // Decrement indegrees of dependents
                            if let Some(deps) = dependents.get(&view_name) {
                                let newly_ready = {
                                    let mut lock = indegrees.lock().unwrap();
                                    let mut ready = Vec::new();
                                    for dep_view in deps {
                                        if let Some(deg) = lock.get_mut(dep_view) {
                                            *deg -= 1;
                                            if *deg == 0 {
                                                ready.push(dep_view.clone());
                                            }
                                        }
                                    }
                                    ready
                                };
                                for dep_view in newly_ready {
                                    let _ = ready_tx.send(dep_view).await;
                                }
                            }
                        }
                        Err(_) => {
                            // Cancellation triggered
                            cancelled.store(true, Ordering::Release);
                            let err = error_cell.lock().await.take().unwrap_or_else(|| {
                                OpError::internal("branch task failed")
                            });
                            return Err(err);
                        }
                    }
                }
                else => {
                    break;
                }
            }
        }

        if cancelled.load(Ordering::Acquire) {
            let err = error_cell
                .lock()
                .await
                .take()
                .unwrap_or_else(|| OpError::internal("branch execution cancelled"));
            return Err(err);
        }

        let res = completed_outputs.lock().await.clone();
        Ok(res)
    }
}
