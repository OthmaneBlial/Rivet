use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use thiserror::Error;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Pipeline {
    pub version: u32,
    pub name: String,
    #[serde(default)]
    pub workspace: Option<PathBuf>,
    pub stages: Vec<Stage>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Stage {
    pub name: String,
    pub steps: Vec<Step>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Step {
    pub name: String,
    /// The executable to launch. Arguments are passed separately so a
    /// pipeline cannot accidentally reinterpret an argument as shell syntax.
    pub program: String,
    #[serde(default)]
    pub args: Vec<String>,
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    #[serde(default)]
    pub working_dir: Option<PathBuf>,
    #[serde(default)]
    pub timeout_seconds: Option<u64>,
}

#[derive(Debug, Error)]
pub enum PipelineError {
    #[error("could not read pipeline file {path}: {source}")]
    Read {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("invalid pipeline TOML: {0}")]
    Parse(#[from] toml::de::Error),
    #[error("pipeline version {0} is not supported")]
    UnsupportedVersion(u32),
    #[error("pipeline name cannot be empty")]
    EmptyName,
    #[error("pipeline must contain at least one stage")]
    EmptyStages,
    #[error("stage {0:?} cannot be empty")]
    EmptyStage(String),
    #[error("duplicate stage name {0:?}")]
    DuplicateStage(String),
    #[error("step {step:?} in stage {stage:?} cannot be empty")]
    EmptyStep { stage: String, step: String },
    #[error("duplicate step name {step:?} in stage {stage:?}")]
    DuplicateStep { stage: String, step: String },
    #[error("step {step:?} in stage {stage:?} has no executable")]
    EmptyProgram { stage: String, step: String },
    #[error("step {step:?} in stage {stage:?} has an invalid timeout")]
    InvalidTimeout { stage: String, step: String },
    #[error("workspace escapes the repository root: {0}")]
    WorkspaceOutsideRepository(PathBuf),
    #[error("workspace does not exist: {0}")]
    MissingWorkspace(PathBuf),
    #[error("workspace path has no repository root: {0}")]
    InvalidRepositoryRoot(PathBuf),
}

impl Pipeline {
    pub fn from_toml_str(source: &str) -> Result<Self, PipelineError> {
        let pipeline: Self = toml::from_str(source)?;
        pipeline.validate()?;
        Ok(pipeline)
    }

    pub fn load(path: impl AsRef<Path>) -> Result<Self, PipelineError> {
        let path = path.as_ref().to_path_buf();
        let source = fs::read_to_string(&path).map_err(|source| PipelineError::Read {
            path: path.clone(),
            source,
        })?;
        Self::from_toml_str(&source)
    }

    pub fn validate(&self) -> Result<(), PipelineError> {
        if self.version != 1 {
            return Err(PipelineError::UnsupportedVersion(self.version));
        }
        if self.name.trim().is_empty() {
            return Err(PipelineError::EmptyName);
        }
        if self.stages.is_empty() {
            return Err(PipelineError::EmptyStages);
        }

        let mut stage_names = std::collections::HashSet::new();
        for stage in &self.stages {
            if stage.name.trim().is_empty() || stage.steps.is_empty() {
                return Err(PipelineError::EmptyStage(stage.name.clone()));
            }
            if !stage_names.insert(stage.name.as_str()) {
                return Err(PipelineError::DuplicateStage(stage.name.clone()));
            }

            let mut step_names = std::collections::HashSet::new();
            for step in &stage.steps {
                if step.name.trim().is_empty() {
                    return Err(PipelineError::EmptyStep {
                        stage: stage.name.clone(),
                        step: step.name.clone(),
                    });
                }
                if !step_names.insert(step.name.as_str()) {
                    return Err(PipelineError::DuplicateStep {
                        stage: stage.name.clone(),
                        step: step.name.clone(),
                    });
                }
                if step.program.trim().is_empty() {
                    return Err(PipelineError::EmptyProgram {
                        stage: stage.name.clone(),
                        step: step.name.clone(),
                    });
                }
                if step.timeout_seconds == Some(0) {
                    return Err(PipelineError::InvalidTimeout {
                        stage: stage.name.clone(),
                        step: step.name.clone(),
                    });
                }
            }
        }

        Ok(())
    }

    /// Resolve a pipeline workspace without allowing it to escape the
    /// associated repository.  The repository root must already exist; the
    /// selected workspace may be created by the runner.
    pub fn resolve_workspace(
        &self,
        repository_root: impl AsRef<Path>,
    ) -> Result<PathBuf, PipelineError> {
        let repository_root = repository_root.as_ref();
        let root = fs::canonicalize(repository_root)
            .map_err(|_| PipelineError::InvalidRepositoryRoot(repository_root.to_path_buf()))?;
        let relative = self.workspace.as_deref().unwrap_or_else(|| Path::new("."));
        let candidate = if relative.is_absolute() {
            relative.to_path_buf()
        } else {
            root.join(relative)
        };
        let normalized = normalize_path(&candidate);
        if !normalized.starts_with(&root) {
            return Err(PipelineError::WorkspaceOutsideRepository(normalized));
        }
        Ok(normalized)
    }
}

fn normalize_path(path: &Path) -> PathBuf {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            std::path::Component::CurDir => {}
            std::path::Component::ParentDir => {
                normalized.pop();
            }
            component => normalized.push(component.as_os_str()),
        }
    }
    normalized
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn valid_pipeline() -> Pipeline {
        Pipeline::from_toml_str(
            r#"
version = 1
name = "example"

[[stages]]
name = "Test"

[[stages.steps]]
name = "unit"
program = "cargo"
args = ["test"]
timeout_seconds = 60
"#,
        )
        .expect("fixture is valid")
    }

    #[test]
    fn parses_explicit_commands_and_defaults_workspace() {
        let pipeline = valid_pipeline();
        assert_eq!(pipeline.stages[0].steps[0].program, "cargo");
        assert_eq!(pipeline.stages[0].steps[0].args, ["test"]);
        assert_eq!(pipeline.workspace, None);
    }

    #[test]
    fn rejects_duplicate_stage_names() {
        let result = Pipeline::from_toml_str(
            r#"
version = 1
name = "broken"
[[stages]]
name = "Test"
[[stages.steps]]
name = "one"
program = "true"
[[stages]]
name = "Test"
[[stages.steps]]
name = "two"
program = "true"
"#,
        );

        assert!(matches!(result, Err(PipelineError::DuplicateStage(name)) if name == "Test"));
    }

    #[test]
    fn workspace_cannot_escape_repository() {
        let dir = tempdir().expect("tempdir");
        let pipeline = Pipeline {
            workspace: Some(PathBuf::from("../outside")),
            ..valid_pipeline()
        };

        assert!(matches!(
            pipeline.resolve_workspace(dir.path()),
            Err(PipelineError::WorkspaceOutsideRepository(_))
        ));
    }
}
