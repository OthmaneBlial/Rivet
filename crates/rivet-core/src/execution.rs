use crate::{BuildId, ProjectId, StageId, StepId};
use crate::{Pipeline, Stage, Step};
use serde::{Deserialize, Serialize};

/// A validated pipeline with stable identifiers for one build attempt.
///
/// Configuration remains reusable and ID-free. IDs are introduced only when a
/// build is created, which gives persistence and live events a common identity
/// without leaking runtime state into a `Rivetfile`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExecutionPlan {
    pub build_id: BuildId,
    pub project_id: ProjectId,
    pub stages: Vec<ExecutionStage>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExecutionStage {
    pub id: StageId,
    pub position: u32,
    pub name: String,
    pub steps: Vec<ExecutionStep>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExecutionStep {
    pub id: StepId,
    pub position: u32,
    pub definition: Step,
}

impl ExecutionPlan {
    pub fn from_pipeline(pipeline: &Pipeline, build_id: BuildId, project_id: ProjectId) -> Self {
        Self {
            build_id,
            project_id,
            stages: pipeline
                .stages
                .iter()
                .enumerate()
                .map(|(stage_index, stage)| ExecutionStage::from_stage(stage, stage_index as u32))
                .collect(),
        }
    }
}

impl ExecutionStage {
    fn from_stage(stage: &Stage, position: u32) -> Self {
        Self {
            id: uuid::Uuid::new_v4(),
            position,
            name: stage.name.clone(),
            steps: stage
                .steps
                .iter()
                .enumerate()
                .map(|(step_index, definition)| ExecutionStep {
                    id: uuid::Uuid::new_v4(),
                    position: step_index as u32,
                    definition: definition.clone(),
                })
                .collect(),
        }
    }
}
