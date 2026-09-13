use crate::{BuildId, BuildStatus, StageId, StageStatus, StepId, StepStatus};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum LogStream {
    Stdout,
    Stderr,
    System,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum EventKind {
    BuildQueued,
    BuildStarted,
    BuildFinished,
    BuildCancelled,
    StageStarted,
    StageFinished,
    StepStarted,
    StepOutput,
    StepFinished,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum BuildEvent {
    BuildQueued {
        build_id: BuildId,
        project_id: uuid::Uuid,
        timestamp: DateTime<Utc>,
    },
    BuildStarted {
        build_id: BuildId,
        timestamp: DateTime<Utc>,
    },
    StageStarted {
        build_id: BuildId,
        stage_id: StageId,
        stage_name: String,
        timestamp: DateTime<Utc>,
    },
    StepStarted {
        build_id: BuildId,
        stage_id: StageId,
        step_id: StepId,
        step_name: String,
        timestamp: DateTime<Utc>,
    },
    StepOutput {
        build_id: BuildId,
        stage_id: StageId,
        step_id: StepId,
        stream: LogStream,
        line: String,
        timestamp: DateTime<Utc>,
    },
    StepFinished {
        build_id: BuildId,
        stage_id: StageId,
        step_id: StepId,
        step_name: String,
        status: StepStatus,
        exit_code: Option<i32>,
        timestamp: DateTime<Utc>,
    },
    StageFinished {
        build_id: BuildId,
        stage_id: StageId,
        stage_name: String,
        status: StageStatus,
        timestamp: DateTime<Utc>,
    },
    BuildFinished {
        build_id: BuildId,
        status: BuildStatus,
        timestamp: DateTime<Utc>,
    },
    BuildCancelled {
        build_id: BuildId,
        timestamp: DateTime<Utc>,
    },
}

impl BuildEvent {
    pub fn kind(&self) -> EventKind {
        match self {
            Self::BuildQueued { .. } => EventKind::BuildQueued,
            Self::BuildStarted { .. } => EventKind::BuildStarted,
            Self::BuildFinished { .. } => EventKind::BuildFinished,
            Self::BuildCancelled { .. } => EventKind::BuildCancelled,
            Self::StageStarted { .. } => EventKind::StageStarted,
            Self::StageFinished { .. } => EventKind::StageFinished,
            Self::StepStarted { .. } => EventKind::StepStarted,
            Self::StepOutput { .. } => EventKind::StepOutput,
            Self::StepFinished { .. } => EventKind::StepFinished,
        }
    }

    pub fn timestamp(&self) -> DateTime<Utc> {
        match self {
            Self::BuildQueued { timestamp, .. }
            | Self::BuildStarted { timestamp, .. }
            | Self::BuildFinished { timestamp, .. }
            | Self::BuildCancelled { timestamp, .. }
            | Self::StageStarted { timestamp, .. }
            | Self::StageFinished { timestamp, .. }
            | Self::StepStarted { timestamp, .. }
            | Self::StepOutput { timestamp, .. }
            | Self::StepFinished { timestamp, .. } => *timestamp,
        }
    }
}
