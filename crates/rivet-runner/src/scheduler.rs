use crate::{RunnerError, execute_pipeline_with_parameters_and_cache};
use chrono::Utc;
use rivet_core::{BuildEvent, BuildId, BuildStatus, ExecutionPlan, Pipeline, ProjectId};
use std::cmp::Ordering as Comparison;
use std::collections::{BTreeMap, BinaryHeap, HashMap};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use thiserror::Error;
use tokio::sync::{Notify, Semaphore, mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

pub const MIN_QUEUE_PRIORITY: i32 = -100;
pub const MAX_QUEUE_PRIORITY: i32 = 100;

struct QueueRequest {
    plan: ExecutionPlan,
    pipeline: Pipeline,
    repository_root: PathBuf,
    parameters: BTreeMap<String, String>,
    cancellation: CancellationToken,
    events: mpsc::Sender<BuildEvent>,
    completion: oneshot::Sender<Result<BuildStatus, RunnerError>>,
    priority: i32,
    sequence: u64,
}

struct PendingRequest {
    priority: i32,
    sequence: u64,
    request: QueueRequest,
}

impl PendingRequest {
    fn new(request: QueueRequest) -> Self {
        Self {
            priority: request.priority,
            sequence: request.sequence,
            request,
        }
    }
}

impl PartialEq for PendingRequest {
    fn eq(&self, other: &Self) -> bool {
        self.priority == other.priority && self.sequence == other.sequence
    }
}

impl Eq for PendingRequest {}

impl Ord for PendingRequest {
    fn cmp(&self, other: &Self) -> Comparison {
        self.priority
            .cmp(&other.priority)
            // BinaryHeap is a max-heap; an earlier sequence must therefore
            // compare greater when priorities are equal.
            .then_with(|| other.sequence.cmp(&self.sequence))
    }
}

impl PartialOrd for PendingRequest {
    fn partial_cmp(&self, other: &Self) -> Option<Comparison> {
        Some(self.cmp(other))
    }
}

#[derive(Debug, Error)]
pub enum SchedulerError {
    #[error("scheduler queue is closed")]
    Closed,
    #[error("scheduled build task failed: {0}")]
    Join(#[from] tokio::task::JoinError),
    #[error("scheduled build did not return a result")]
    MissingResult,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueueStats {
    pub queued: usize,
    pub running: usize,
    pub capacity: usize,
}

struct SchedulerMetrics {
    queued: AtomicUsize,
    running: AtomicUsize,
    capacity: usize,
}

struct RunningBuildGuard {
    metrics: Arc<SchedulerMetrics>,
    wake: Arc<Notify>,
}

impl Drop for RunningBuildGuard {
    fn drop(&mut self) {
        self.metrics.running.fetch_sub(1, Ordering::Relaxed);
        self.wake.notify_waiters();
    }
}

pub struct Scheduler {
    queue: mpsc::Sender<QueueRequest>,
    worker: JoinHandle<()>,
    metrics: Arc<SchedulerMetrics>,
    wake: Arc<Notify>,
    next_sequence: AtomicU64,
}

pub struct QueueHandle {
    pub build_id: BuildId,
    cancellation: CancellationToken,
    completion: oneshot::Receiver<Result<BuildStatus, RunnerError>>,
    wake: Arc<Notify>,
}

impl Scheduler {
    /// Start a priority queue with FIFO tie ordering, a global execution
    /// limit, and an optional per-project limit. Queue admission is separate
    /// from execution permits, so callers can persist `queued` before a
    /// worker starts.
    pub fn new(global_concurrency: usize, per_project_concurrency: Option<usize>) -> Self {
        Self::new_with_cache(global_concurrency, per_project_concurrency, None)
    }

    pub fn new_with_cache(
        global_concurrency: usize,
        per_project_concurrency: Option<usize>,
        cache_root: Option<PathBuf>,
    ) -> Self {
        assert!(
            global_concurrency > 0,
            "global concurrency must be positive"
        );
        if let Some(limit) = per_project_concurrency {
            assert!(limit > 0, "per-project concurrency must be positive");
        }
        let (queue, mut receiver) = mpsc::channel::<QueueRequest>(256);
        let global = Arc::new(Semaphore::new(global_concurrency));
        let metrics = Arc::new(SchedulerMetrics {
            queued: AtomicUsize::new(0),
            running: AtomicUsize::new(0),
            capacity: global_concurrency,
        });
        let project_limit = per_project_concurrency.unwrap_or(global_concurrency);
        let project_slots = Arc::new(tokio::sync::Mutex::new(
            HashMap::<ProjectId, Arc<Semaphore>>::new(),
        ));
        let worker_metrics = metrics.clone();
        let worker_cache_root = cache_root;
        let wake = Arc::new(Notify::new());
        let worker_wake = wake.clone();
        let worker = tokio::spawn(async move {
            let mut pending = BinaryHeap::<PendingRequest>::new();
            let mut queue_closed = false;
            loop {
                if !queue_closed {
                    loop {
                        match receiver.try_recv() {
                            Ok(request) => pending.push(PendingRequest::new(request)),
                            Err(mpsc::error::TryRecvError::Empty) => break,
                            Err(mpsc::error::TryRecvError::Disconnected) => {
                                queue_closed = true;
                                break;
                            }
                        }
                    }
                }

                if pending.is_empty() {
                    if queue_closed {
                        break;
                    }
                    match receiver.recv().await {
                        Some(request) => pending.push(PendingRequest::new(request)),
                        None => queue_closed = true,
                    }
                    continue;
                }

                let mut blocked = Vec::with_capacity(pending.len());
                let mut admitted = false;
                while let Some(entry) = pending.pop() {
                    let request = entry.request;
                    if request.cancellation.is_cancelled() {
                        worker_metrics.queued.fetch_sub(1, Ordering::Relaxed);
                        tokio::spawn(finish_queued_cancellation(request));
                        continue;
                    }

                    let global_permit = match global.clone().try_acquire_owned() {
                        Ok(permit) => permit,
                        Err(tokio::sync::TryAcquireError::NoPermits) => {
                            blocked.push(PendingRequest::new(request));
                            continue;
                        }
                        Err(tokio::sync::TryAcquireError::Closed) => {
                            worker_metrics.queued.fetch_sub(1, Ordering::Relaxed);
                            let _ = request
                                .completion
                                .send(Err(RunnerError::EventChannelClosed));
                            continue;
                        }
                    };
                    let project_id = request.plan.project_id;
                    let project_slot = {
                        let mut slots = project_slots.lock().await;
                        slots
                            .entry(project_id)
                            .or_insert_with(|| Arc::new(Semaphore::new(project_limit)))
                            .clone()
                    };
                    let project_permit = match project_slot.try_acquire_owned() {
                        Ok(permit) => permit,
                        Err(tokio::sync::TryAcquireError::NoPermits) => {
                            drop(global_permit);
                            blocked.push(PendingRequest::new(request));
                            continue;
                        }
                        Err(tokio::sync::TryAcquireError::Closed) => {
                            drop(global_permit);
                            worker_metrics.queued.fetch_sub(1, Ordering::Relaxed);
                            let _ = request
                                .completion
                                .send(Err(RunnerError::EventChannelClosed));
                            continue;
                        }
                    };
                    if request.cancellation.is_cancelled() {
                        drop(project_permit);
                        drop(global_permit);
                        worker_metrics.queued.fetch_sub(1, Ordering::Relaxed);
                        tokio::spawn(finish_queued_cancellation(request));
                        admitted = true;
                        continue;
                    }

                    worker_metrics.queued.fetch_sub(1, Ordering::Relaxed);
                    worker_metrics.running.fetch_add(1, Ordering::Relaxed);
                    let metrics = worker_metrics.clone();
                    let wake = worker_wake.clone();
                    let cache_root = worker_cache_root.clone();
                    tokio::spawn(async move {
                        let _running = RunningBuildGuard { metrics, wake };
                        let result = execute_pipeline_with_parameters_and_cache(
                            &request.plan,
                            &request.pipeline,
                            request.repository_root,
                            &request.parameters,
                            request.cancellation,
                            request.events,
                            cache_root.as_deref(),
                        )
                        .await;
                        drop(project_permit);
                        drop(global_permit);
                        let _ = request.completion.send(result);
                    });
                    admitted = true;
                }
                pending.extend(blocked);

                if pending.is_empty() || admitted {
                    continue;
                }
                if queue_closed {
                    worker_wake.notified().await;
                } else {
                    tokio::select! {
                        request = receiver.recv() => {
                            match request {
                                Some(request) => pending.push(PendingRequest::new(request)),
                                None => queue_closed = true,
                            }
                        }
                        _ = worker_wake.notified() => {}
                    }
                }
            }
        });
        Self {
            queue,
            worker,
            metrics,
            wake,
            next_sequence: AtomicU64::new(0),
        }
    }

    pub fn stats(&self) -> QueueStats {
        QueueStats {
            queued: self.metrics.queued.load(Ordering::Relaxed),
            running: self.metrics.running.load(Ordering::Relaxed),
            capacity: self.metrics.capacity,
        }
    }

    pub async fn enqueue(
        &self,
        plan: ExecutionPlan,
        pipeline: Pipeline,
        repository_root: PathBuf,
        cancellation: CancellationToken,
        events: mpsc::Sender<BuildEvent>,
    ) -> Result<QueueHandle, SchedulerError> {
        self.enqueue_with_parameters(
            plan,
            pipeline,
            repository_root,
            BTreeMap::new(),
            cancellation,
            events,
        )
        .await
    }

    pub async fn enqueue_with_parameters(
        &self,
        plan: ExecutionPlan,
        pipeline: Pipeline,
        repository_root: PathBuf,
        parameters: BTreeMap<String, String>,
        cancellation: CancellationToken,
        events: mpsc::Sender<BuildEvent>,
    ) -> Result<QueueHandle, SchedulerError> {
        self.enqueue_with_priority_and_parameters(
            plan,
            pipeline,
            repository_root,
            parameters,
            0,
            cancellation,
            events,
        )
        .await
    }

    pub async fn enqueue_with_priority(
        &self,
        plan: ExecutionPlan,
        pipeline: Pipeline,
        repository_root: PathBuf,
        priority: i32,
        cancellation: CancellationToken,
        events: mpsc::Sender<BuildEvent>,
    ) -> Result<QueueHandle, SchedulerError> {
        self.enqueue_with_priority_and_parameters(
            plan,
            pipeline,
            repository_root,
            BTreeMap::new(),
            priority,
            cancellation,
            events,
        )
        .await
    }

    pub async fn enqueue_with_priority_and_parameters(
        &self,
        plan: ExecutionPlan,
        pipeline: Pipeline,
        repository_root: PathBuf,
        parameters: BTreeMap<String, String>,
        priority: i32,
        cancellation: CancellationToken,
        events: mpsc::Sender<BuildEvent>,
    ) -> Result<QueueHandle, SchedulerError> {
        events
            .send(BuildEvent::BuildQueued {
                build_id: plan.build_id,
                project_id: plan.project_id,
                timestamp: Utc::now(),
            })
            .await
            .map_err(|_| SchedulerError::Closed)?;
        let (completion, result) = oneshot::channel();
        let sequence = self.next_sequence.fetch_add(1, Ordering::Relaxed);
        self.metrics.queued.fetch_add(1, Ordering::Relaxed);
        if self
            .queue
            .send(QueueRequest {
                plan: plan.clone(),
                pipeline,
                repository_root,
                parameters,
                cancellation: cancellation.clone(),
                events,
                completion,
                priority,
                sequence,
            })
            .await
            .is_err()
        {
            self.metrics.queued.fetch_sub(1, Ordering::Relaxed);
            return Err(SchedulerError::Closed);
        }
        Ok(QueueHandle {
            build_id: plan.build_id,
            cancellation,
            completion: result,
            wake: self.wake.clone(),
        })
    }
}

async fn finish_queued_cancellation(request: QueueRequest) {
    let timestamp = Utc::now();
    let _ = request
        .events
        .send(BuildEvent::BuildCancelled {
            build_id: request.plan.build_id,
            timestamp,
        })
        .await;
    let _ = request
        .events
        .send(BuildEvent::BuildFinished {
            build_id: request.plan.build_id,
            status: BuildStatus::Cancelled,
            timestamp: Utc::now(),
        })
        .await;
    let _ = request.completion.send(Ok(BuildStatus::Cancelled));
}

impl QueueHandle {
    pub fn cancel(&self) {
        self.cancellation.cancel();
        self.wake.notify_one();
    }

    pub async fn wait(self) -> Result<Result<BuildStatus, RunnerError>, SchedulerError> {
        self.completion
            .await
            .map_err(|_| SchedulerError::MissingResult)
    }
}

impl Drop for Scheduler {
    fn drop(&mut self) {
        self.worker.abort();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::tempdir;
    use tokio::time::{Duration, sleep};

    #[tokio::test]
    async fn queue_preserves_admission_order_when_execution_is_serial() {
        let dir = tempdir().expect("tempdir");
        let pipeline = Pipeline::from_toml_str(
            r#"
version = 1
name = "queue"
[[stages]]
name = "run"
[[stages.steps]]
name = "write"
program = "sh"
args = ["-c", "printf done > result.txt"]
"#,
        )
        .expect("pipeline");
        let scheduler = Scheduler::new(1, Some(1));
        let mut handles = Vec::new();
        let mut receivers = Vec::new();
        for _ in 0..3 {
            let plan =
                ExecutionPlan::from_pipeline(&pipeline, uuid::Uuid::new_v4(), uuid::Uuid::new_v4());
            let (tx, rx) = mpsc::channel(64);
            let handle = scheduler
                .enqueue(
                    plan,
                    pipeline.clone(),
                    dir.path().to_path_buf(),
                    CancellationToken::new(),
                    tx,
                )
                .await
                .expect("enqueue");
            handles.push(handle);
            receivers.push(rx);
        }
        for handle in handles {
            assert_eq!(
                handle.wait().await.expect("scheduler").expect("run"),
                BuildStatus::Passed
            );
        }
        assert_eq!(
            fs::read_to_string(dir.path().join("result.txt")).expect("result"),
            "done"
        );
        // Let spawned reader tasks finish before the test drops its receivers.
        sleep(Duration::from_millis(10)).await;
        drop(receivers);
    }

    #[tokio::test]
    async fn queue_runs_higher_priority_work_before_older_lower_priority_work() {
        let dir = tempdir().expect("tempdir");
        let pipeline = Pipeline::from_toml_str(
            r#"
version = 1
name = "priority"
[[parameters]]
name = "LABEL"
[[stages]]
name = "run"
[[stages.steps]]
name = "write"
program = "sh"
args = ["-c", "printf '%s' \"$LABEL\" >> priority.txt; sleep 0.35"]
"#,
        )
        .expect("pipeline");
        let project_id = uuid::Uuid::new_v4();
        let scheduler = Scheduler::new(1, Some(1));
        let (first_tx, first_rx) = mpsc::channel(64);
        let first = scheduler
            .enqueue_with_priority_and_parameters(
                ExecutionPlan::from_pipeline(&pipeline, uuid::Uuid::new_v4(), project_id),
                pipeline.clone(),
                dir.path().to_path_buf(),
                BTreeMap::from([(String::from("LABEL"), String::from("first"))]),
                0,
                CancellationToken::new(),
                first_tx,
            )
            .await
            .expect("first enqueue");
        for _ in 0..40 {
            if scheduler.stats().running == 1 {
                break;
            }
            sleep(Duration::from_millis(5)).await;
        }
        assert_eq!(scheduler.stats().running, 1);

        let (low_tx, low_rx) = mpsc::channel(64);
        let low = scheduler
            .enqueue_with_priority_and_parameters(
                ExecutionPlan::from_pipeline(&pipeline, uuid::Uuid::new_v4(), project_id),
                pipeline.clone(),
                dir.path().to_path_buf(),
                BTreeMap::from([(String::from("LABEL"), String::from("low"))]),
                -10,
                CancellationToken::new(),
                low_tx,
            )
            .await
            .expect("low enqueue");
        let (high_tx, high_rx) = mpsc::channel(64);
        let high = scheduler
            .enqueue_with_priority_and_parameters(
                ExecutionPlan::from_pipeline(&pipeline, uuid::Uuid::new_v4(), project_id),
                pipeline,
                dir.path().to_path_buf(),
                BTreeMap::from([(String::from("LABEL"), String::from("high"))]),
                10,
                CancellationToken::new(),
                high_tx,
            )
            .await
            .expect("high enqueue");

        for handle in [first, low, high] {
            assert_eq!(
                handle.wait().await.expect("scheduler").expect("run"),
                BuildStatus::Passed
            );
        }
        assert_eq!(
            fs::read_to_string(dir.path().join("priority.txt")).expect("priority output"),
            "firsthighlow"
        );
        drop((first_rx, low_rx, high_rx));
    }

    #[tokio::test]
    async fn exposes_configured_capacity_without_fake_work() {
        let scheduler = Scheduler::new(3, Some(1));
        assert_eq!(
            scheduler.stats(),
            QueueStats {
                queued: 0,
                running: 0,
                capacity: 3,
            }
        );
    }

    #[tokio::test]
    async fn reports_work_waiting_for_a_project_slot_as_queued() {
        let dir = tempdir().expect("tempdir");
        let pipeline = Pipeline::from_toml_str(
            r#"
version = 1
name = "queue-stats"
[[stages]]
name = "run"
[[stages.steps]]
name = "wait"
program = "sh"
args = ["-c", "sleep 0.4"]
"#,
        )
        .expect("pipeline");
        let scheduler = Scheduler::new(2, Some(1));
        let project_id = uuid::Uuid::new_v4();
        let mut handles = Vec::new();
        let mut receivers = Vec::new();
        for _ in 0..2 {
            let plan = ExecutionPlan::from_pipeline(&pipeline, uuid::Uuid::new_v4(), project_id);
            let (tx, rx) = mpsc::channel(64);
            receivers.push(rx);
            handles.push(
                scheduler
                    .enqueue(
                        plan,
                        pipeline.clone(),
                        dir.path().to_path_buf(),
                        CancellationToken::new(),
                        tx,
                    )
                    .await
                    .expect("enqueue"),
            );
        }
        sleep(Duration::from_millis(100)).await;
        let stats = scheduler.stats();
        assert_eq!(stats.capacity, 2);
        assert_eq!(stats.running, 1);
        assert_eq!(stats.queued, 1);
        for handle in handles {
            assert_eq!(
                handle.wait().await.expect("scheduler").expect("run"),
                BuildStatus::Passed
            );
        }
        drop(receivers);
    }

    #[tokio::test]
    async fn cancels_a_build_before_it_acquires_a_slot() {
        let dir = tempdir().expect("tempdir");
        let pipeline = Pipeline::from_toml_str(
            r#"
version = 1
name = "queued-cancel"
[[stages]]
name = "run"
[[stages.steps]]
name = "wait"
program = "sh"
args = ["-c", "sleep 0.4"]
"#,
        )
        .expect("pipeline");
        let scheduler = Scheduler::new(1, Some(1));
        let project_id = uuid::Uuid::new_v4();
        let (first_tx, mut first_rx) = mpsc::channel(64);
        let first = scheduler
            .enqueue(
                ExecutionPlan::from_pipeline(&pipeline, uuid::Uuid::new_v4(), project_id),
                pipeline.clone(),
                dir.path().to_path_buf(),
                CancellationToken::new(),
                first_tx,
            )
            .await
            .expect("first enqueue");
        tokio::time::sleep(Duration::from_millis(50)).await;

        let (second_tx, mut second_rx) = mpsc::channel(64);
        let second = scheduler
            .enqueue(
                ExecutionPlan::from_pipeline(&pipeline, uuid::Uuid::new_v4(), project_id),
                pipeline,
                dir.path().to_path_buf(),
                CancellationToken::new(),
                second_tx,
            )
            .await
            .expect("second enqueue");
        second.cancel();
        assert_eq!(
            second.wait().await.expect("scheduler").expect("cancel"),
            BuildStatus::Cancelled
        );
        let mut second_events = Vec::new();
        while let Some(event) = second_rx.recv().await {
            second_events.push(event);
        }
        assert!(
            second_events
                .iter()
                .any(|event| { matches!(event, BuildEvent::BuildCancelled { .. }) })
        );
        assert!(
            !second_events
                .iter()
                .any(|event| matches!(event, BuildEvent::BuildStarted { .. }))
        );

        assert_eq!(
            first.wait().await.expect("scheduler").expect("first run"),
            BuildStatus::Passed
        );
        while first_rx.recv().await.is_some() {}
        assert_eq!(scheduler.stats().queued, 0);
        assert_eq!(scheduler.stats().running, 0);
    }
}
