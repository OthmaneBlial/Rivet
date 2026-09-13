use crate::{RunnerError, execute_pipeline};
use chrono::Utc;
use rivet_core::{BuildEvent, BuildId, BuildStatus, ExecutionPlan, Pipeline};
use std::path::PathBuf;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::{Semaphore, mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

struct QueueRequest {
    plan: ExecutionPlan,
    pipeline: Pipeline,
    repository_root: PathBuf,
    cancellation: CancellationToken,
    events: mpsc::Sender<BuildEvent>,
    completion: oneshot::Sender<Result<BuildStatus, RunnerError>>,
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

pub struct Scheduler {
    queue: mpsc::Sender<QueueRequest>,
    worker: JoinHandle<()>,
}

pub struct QueueHandle {
    pub build_id: BuildId,
    cancellation: CancellationToken,
    completion: oneshot::Receiver<Result<BuildStatus, RunnerError>>,
}

impl Scheduler {
    /// Start a FIFO queue with a global execution limit and optional per-
    /// project limit. Queue admission is separate from execution permits, so
    /// callers can persist `queued` before a worker starts.
    pub fn new(global_concurrency: usize, per_project_concurrency: Option<usize>) -> Self {
        assert!(
            global_concurrency > 0,
            "global concurrency must be positive"
        );
        if let Some(limit) = per_project_concurrency {
            assert!(limit > 0, "per-project concurrency must be positive");
        }
        let (queue, mut receiver) = mpsc::channel::<QueueRequest>(256);
        let global = Arc::new(Semaphore::new(global_concurrency));
        let project_limit = per_project_concurrency.unwrap_or(global_concurrency);
        let project_slots = Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::<
            BuildId,
            Arc<Semaphore>,
        >::new()));
        let worker = tokio::spawn(async move {
            while let Some(request) = receiver.recv().await {
                let global = global.clone();
                let project_slots = project_slots.clone();
                let project_id = request.plan.project_id;
                let project_slot = {
                    let mut slots = project_slots.lock().await;
                    slots
                        .entry(project_id)
                        .or_insert_with(|| Arc::new(Semaphore::new(project_limit)))
                        .clone()
                };
                tokio::spawn(async move {
                    let global_permit = match global.acquire_owned().await {
                        Ok(permit) => permit,
                        Err(_) => {
                            let _ = request
                                .completion
                                .send(Err(RunnerError::EventChannelClosed));
                            return;
                        }
                    };
                    let project_permit = match project_slot.acquire_owned().await {
                        Ok(permit) => permit,
                        Err(_) => {
                            drop(global_permit);
                            let _ = request
                                .completion
                                .send(Err(RunnerError::EventChannelClosed));
                            return;
                        }
                    };
                    let result = execute_pipeline(
                        &request.plan,
                        &request.pipeline,
                        request.repository_root,
                        request.cancellation,
                        request.events,
                    )
                    .await;
                    drop(project_permit);
                    drop(global_permit);
                    let _ = request.completion.send(result);
                });
            }
        });
        Self { queue, worker }
    }

    pub async fn enqueue(
        &self,
        plan: ExecutionPlan,
        pipeline: Pipeline,
        repository_root: PathBuf,
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
        self.queue
            .send(QueueRequest {
                plan: plan.clone(),
                pipeline,
                repository_root,
                cancellation: cancellation.clone(),
                events,
                completion,
            })
            .await
            .map_err(|_| SchedulerError::Closed)?;
        Ok(QueueHandle {
            build_id: plan.build_id,
            cancellation,
            completion: result,
        })
    }
}

impl QueueHandle {
    pub fn cancel(&self) {
        self.cancellation.cancel();
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
}
