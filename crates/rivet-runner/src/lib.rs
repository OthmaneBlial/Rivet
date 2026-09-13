//! Process execution and sequential pipeline orchestration.
//!
//! The runner has no knowledge of SQLite or the desktop UI. It emits typed
//! events and can therefore be hosted by a CLI, server, or Tauri process.

mod pipeline;
mod process;
mod scheduler;

pub use pipeline::{RunnerError, execute_pipeline};
pub use process::{LogLine, ProcessError, ProcessOutcome, ProcessResult, ProcessSpec, run_process};
pub use scheduler::{QueueHandle, QueueStats, Scheduler, SchedulerError};
