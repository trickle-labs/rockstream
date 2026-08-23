use std::collections::{BTreeMap, HashMap};

use rockstream_types::SharedWindowSpec;

pub const MAX_SHARED_WINDOW_SLICES: usize = 100_000;
pub const MAX_SHARED_WINDOW_QUERY_SLICES: usize = 1_024;
pub const MAX_SHARED_WINDOW_CONSUMERS: usize = 1_024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SharedWindowFillLevel {
    pub used: usize,
    pub capacity: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SharedWindowError {
    NotRegistered,
    InvalidWindowWidth { width_ms: u64, slice_width_ms: u64 },
    ConsumerCapacityExceeded { max: usize },
    SliceCapacityExceeded { max: usize },
    QueryTooWide { requested: usize, max: usize },
    InvalidRange,
    ArithmeticOverflow,
}

#[derive(Debug, Default)]
struct SliceState {
    slices: BTreeMap<u64, (i64, i64)>,
    consumers: BTreeMap<String, u64>,
}

/// Bounded shared physical slices for correlated logical windows.
#[derive(Debug)]
pub struct SharedWindowFabric {
    windows: HashMap<SharedWindowSpec, SliceState>,
    max_slices: usize,
    max_query_slices: usize,
}

impl Default for SharedWindowFabric {
    fn default() -> Self {
        Self::new()
    }
}

impl SharedWindowFabric {
    pub fn new() -> Self {
        Self::with_limits(MAX_SHARED_WINDOW_SLICES, MAX_SHARED_WINDOW_QUERY_SLICES)
    }

    pub fn with_limits(max_slices: usize, max_query_slices: usize) -> Self {
        Self {
            windows: HashMap::new(),
            max_slices,
            max_query_slices,
        }
    }

    pub fn register(&mut self, spec: SharedWindowSpec) -> Result<(), SharedWindowError> {
        self.windows.entry(spec).or_default();
        Ok(())
    }

    pub fn attach(
        &mut self,
        spec: &SharedWindowSpec,
        consumer: impl Into<String>,
        window_width_ms: u64,
    ) -> Result<(), SharedWindowError> {
        let state = self
            .windows
            .get_mut(spec)
            .ok_or(SharedWindowError::NotRegistered)?;
        if window_width_ms == 0 || !window_width_ms.is_multiple_of(spec.slice_width_ms) {
            return Err(SharedWindowError::InvalidWindowWidth {
                width_ms: window_width_ms,
                slice_width_ms: spec.slice_width_ms,
            });
        }
        let consumer = consumer.into();
        if !state.consumers.contains_key(&consumer)
            && state.consumers.len() >= MAX_SHARED_WINDOW_CONSUMERS
        {
            return Err(SharedWindowError::ConsumerCapacityExceeded {
                max: MAX_SHARED_WINDOW_CONSUMERS,
            });
        }
        state.consumers.insert(consumer, window_width_ms);
        Ok(())
    }

    pub fn detach(
        &mut self,
        spec: &SharedWindowSpec,
        consumer: &str,
    ) -> Result<bool, SharedWindowError> {
        let state = self
            .windows
            .get_mut(spec)
            .ok_or(SharedWindowError::NotRegistered)?;
        Ok(state.consumers.remove(consumer).is_some())
    }

    pub fn apply(
        &mut self,
        spec: &SharedWindowSpec,
        event_time_ms: u64,
        value: i64,
        weight: i64,
    ) -> Result<(), SharedWindowError> {
        let state = self
            .windows
            .get_mut(spec)
            .ok_or(SharedWindowError::NotRegistered)?;
        let slice_start = event_time_ms / spec.slice_width_ms * spec.slice_width_ms;
        if !state.slices.contains_key(&slice_start) && state.slices.len() >= self.max_slices {
            return Err(SharedWindowError::SliceCapacityExceeded {
                max: self.max_slices,
            });
        }
        let entry = state.slices.entry(slice_start).or_default();
        entry.0 = entry
            .0
            .checked_add(
                value
                    .checked_mul(weight)
                    .ok_or(SharedWindowError::ArithmeticOverflow)?,
            )
            .ok_or(SharedWindowError::ArithmeticOverflow)?;
        entry.1 = entry
            .1
            .checked_add(weight)
            .ok_or(SharedWindowError::ArithmeticOverflow)?;
        if entry.0 == 0 && entry.1 == 0 {
            state.slices.remove(&slice_start);
        }
        Ok(())
    }

    pub fn window_sum(
        &self,
        spec: &SharedWindowSpec,
        start_ms: u64,
        end_ms: u64,
    ) -> Result<(i64, i64), SharedWindowError> {
        let state = self
            .windows
            .get(spec)
            .ok_or(SharedWindowError::NotRegistered)?;
        if end_ms <= start_ms {
            return Err(SharedWindowError::InvalidRange);
        }
        let width = spec.slice_width_ms;
        if !start_ms.is_multiple_of(width) || !end_ms.is_multiple_of(width) {
            return Err(SharedWindowError::InvalidRange);
        }
        let first = start_ms / width * width;
        let last = end_ms
            .div_ceil(width)
            .checked_mul(width)
            .ok_or(SharedWindowError::ArithmeticOverflow)?;
        let requested = ((last - first) / width) as usize;
        if requested > self.max_query_slices {
            return Err(SharedWindowError::QueryTooWide {
                requested,
                max: self.max_query_slices,
            });
        }
        state.slices.range(first..last).try_fold(
            (0_i64, 0_i64),
            |(sum, count), (_, (slice_sum, slice_count))| {
                Ok((
                    sum.checked_add(*slice_sum)
                        .ok_or(SharedWindowError::ArithmeticOverflow)?,
                    count
                        .checked_add(*slice_count)
                        .ok_or(SharedWindowError::ArithmeticOverflow)?,
                ))
            },
        )
    }

    pub fn slice_count(&self, spec: &SharedWindowSpec) -> Option<usize> {
        self.windows.get(spec).map(|state| state.slices.len())
    }

    pub fn consumer_count(&self, spec: &SharedWindowSpec) -> Option<usize> {
        self.windows.get(spec).map(|state| state.consumers.len())
    }

    pub fn fill_level(&self, spec: &SharedWindowSpec) -> Option<SharedWindowFillLevel> {
        self.windows.get(spec).map(|state| SharedWindowFillLevel {
            used: state.slices.len(),
            capacity: self.max_slices,
        })
    }
}
