//! Process execution and sequential pipeline orchestration.
//!
//! The runner has no knowledge of SQLite or the desktop UI. It emits typed
//! events and can therefore be hosted by a CLI, server, or Tauri process.

mod cache;
mod pipeline;
mod process;
mod scheduler;

pub use cache::{CacheError, CacheStore};
pub use pipeline::{
    RunnerError, execute_pipeline, execute_pipeline_with_parameters,
    execute_pipeline_with_parameters_and_cache,
};
pub use process::{LogLine, ProcessError, ProcessOutcome, ProcessResult, ProcessSpec, run_process};
pub use scheduler::{QueueHandle, QueueStats, Scheduler, SchedulerError};
