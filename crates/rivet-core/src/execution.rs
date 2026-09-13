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
    /// Stable stage IDs for the dependencies declared in the pipeline.
    /// `position` remains the deterministic topological execution order.
    pub depends_on: Vec<StageId>,
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
        let order = pipeline
            .stage_order()
            .expect("execution plans require a validated pipeline");
        let stage_ids = pipeline
            .stages
            .iter()
            .map(|_| uuid::Uuid::new_v4())
            .collect::<Vec<_>>();
        Self {
            build_id,
            project_id,
            stages: order
                .into_iter()
                .enumerate()
                .map(|(position, stage_index)| {
                    ExecutionStage::from_stage(
                        &pipeline.stages[stage_index],
                        position as u32,
                        stage_ids[stage_index],
                        &stage_ids,
                        pipeline,
                    )
                })
                .collect(),
        }
    }
}

impl ExecutionStage {
    fn from_stage(
        stage: &Stage,
        position: u32,
        id: StageId,
        stage_ids: &[StageId],
        pipeline: &Pipeline,
    ) -> Self {
        Self {
            id,
            position,
            name: stage.name.clone(),
            depends_on: stage
                .depends_on
                .iter()
                .filter_map(|dependency| {
                    pipeline
                        .stages
                        .iter()
                        .position(|candidate| candidate.name == *dependency)
                        .map(|index| stage_ids[index])
                })
                .collect(),
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
