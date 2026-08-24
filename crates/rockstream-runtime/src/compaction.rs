//! Foreground-independent compaction budget.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

#[derive(Clone)]
pub struct CompactionBudget {
    max_in_flight: usize,
    in_flight: Arc<AtomicUsize>,
}

pub struct CompactionPermit {
    in_flight: Arc<AtomicUsize>,
}

#[derive(Clone)]
pub struct CompactionWorker {
    budget: CompactionBudget,
}

impl CompactionBudget {
    pub fn new(max_in_flight: usize) -> Self {
        Self {
            max_in_flight: max_in_flight.max(1),
            in_flight: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn max_in_flight(&self) -> usize {
        self.max_in_flight
    }

    pub fn fill_level(&self) -> usize {
        self.in_flight.load(Ordering::Acquire)
    }

    pub fn try_acquire(&self) -> Option<CompactionPermit> {
        let mut current = self.in_flight.load(Ordering::Acquire);
        loop {
            if current >= self.max_in_flight {
                return None;
            }
            match self.in_flight.compare_exchange_weak(
                current,
                current + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    return Some(CompactionPermit {
                        in_flight: self.in_flight.clone(),
                    });
                }
                Err(actual) => current = actual,
            }
        }
    }
}

impl CompactionWorker {
    pub fn new(max_in_flight: usize) -> Self {
        Self {
            budget: CompactionBudget::new(max_in_flight),
        }
    }

    pub fn budget(&self) -> &CompactionBudget {
        &self.budget
    }

    pub fn try_spawn<F, Fut, T>(&self, job: F) -> Option<tokio::task::JoinHandle<T>>
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: std::future::Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        let permit = self.budget.try_acquire()?;
        Some(tokio::spawn(async move {
            let _permit = permit;
            job().await
        }))
    }
}

impl Drop for CompactionPermit {
    fn drop(&mut self) {
        self.in_flight.fetch_sub(1, Ordering::AcqRel);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compaction_budget_is_separate_and_bounded() {
        let budget = CompactionBudget::new(1);
        let permit = budget.try_acquire().unwrap();
        assert_eq!(budget.fill_level(), 1);
        assert!(budget.try_acquire().is_none());
        drop(permit);
        assert_eq!(budget.fill_level(), 0);
        assert!(budget.try_acquire().is_some());
    }

    #[tokio::test]
    async fn compaction_worker_holds_budget_until_job_finishes() {
        let worker = CompactionWorker::new(1);
        let handle = worker.try_spawn(|| async { 7 }).unwrap();
        assert!(worker.try_spawn(|| async { 8 }).is_none());
        assert_eq!(handle.await.unwrap(), 7);
        assert_eq!(worker.budget().fill_level(), 0);
    }
}
