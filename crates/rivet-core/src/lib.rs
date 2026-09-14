//! Domain types shared by Rivet's server, runner, CLI, and desktop clients.
//!
//! This crate deliberately contains no persistence, process, or UI code.  It
//! is the stable vocabulary at the centre of the platform.

mod events;
mod execution;
mod model;
mod pipeline;
mod schedule;

pub use events::{BuildEvent, EventKind, LogStream};
pub use execution::{ExecutionPlan, ExecutionStage, ExecutionStep};
pub use model::{
    BuildId, BuildStatus, ModelError, Project, ProjectId, ScheduleId, SourceSnapshot, StageId,
    StageStatus, StepId, StepStatus,
};
pub use pipeline::{
    AgentRequirement, ArtifactSpec, CacheSpec, ContainerPullPolicy, ContainerRuntime,
    ContainerSpec, ContainerVolume, ParameterKind, ParameterSpec, Pipeline, PipelineError,
    REDACTED_PARAMETER_VALUE, Stage, StageCondition, Step,
};
pub use schedule::{CronExpression, CronExpressionError};
