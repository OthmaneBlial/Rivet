use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

pub type ProjectId = Uuid;
pub type BuildId = Uuid;
pub type StageId = Uuid;
pub type StepId = Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum BuildStatus {
    Pending,
    Queued,
    Running,
    Passed,
    Failed,
    Cancelled,
}

impl BuildStatus {
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Passed | Self::Failed | Self::Cancelled)
    }

    pub fn can_transition_to(&self, next: &Self) -> bool {
        use BuildStatus::*;

        matches!(
            (self, next),
            (Pending, Queued | Cancelled)
                | (Queued, Running | Cancelled)
                | (Running, Passed | Failed | Cancelled)
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum StageStatus {
    Pending,
    Running,
    Passed,
    Failed,
    Cancelled,
    Skipped,
}

impl StageStatus {
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::Passed | Self::Failed | Self::Cancelled | Self::Skipped
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum StepStatus {
    Pending,
    Running,
    Passed,
    Failed,
    Cancelled,
    Skipped,
}

impl StepStatus {
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::Passed | Self::Failed | Self::Cancelled | Self::Skipped
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Project {
    pub id: ProjectId,
    pub name: String,
    pub repository_path: String,
    pub pipeline_path: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Error)]
pub enum ModelError {
    #[error("project name cannot be empty")]
    EmptyProjectName,
    #[error("project name contains a path separator")]
    InvalidProjectName,
}

impl Project {
    pub fn new(
        name: impl Into<String>,
        repository_path: impl Into<String>,
        pipeline_path: impl Into<String>,
    ) -> Result<Self, ModelError> {
        let name = name.into();
        if name.trim().is_empty() {
            return Err(ModelError::EmptyProjectName);
        }
        if name.contains('/') || name.contains('\\') {
            return Err(ModelError::InvalidProjectName);
        }

        Ok(Self {
            id: Uuid::new_v4(),
            name,
            repository_path: repository_path.into(),
            pipeline_path: pipeline_path.into(),
            created_at: Utc::now(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_transitions_are_explicit() {
        assert!(BuildStatus::Pending.can_transition_to(&BuildStatus::Queued));
        assert!(BuildStatus::Queued.can_transition_to(&BuildStatus::Running));
        assert!(BuildStatus::Running.can_transition_to(&BuildStatus::Failed));
        assert!(!BuildStatus::Pending.can_transition_to(&BuildStatus::Passed));
        assert!(!BuildStatus::Passed.can_transition_to(&BuildStatus::Running));
    }

    #[test]
    fn project_names_are_safe_for_local_identifiers() {
        assert!(Project::new("api", ".", "Rivetfile.toml").is_ok());
        assert!(matches!(
            Project::new("", ".", "Rivetfile.toml"),
            Err(ModelError::EmptyProjectName)
        ));
        assert!(matches!(
            Project::new("team/api", ".", "Rivetfile.toml"),
            Err(ModelError::InvalidProjectName)
        ));
    }
}
