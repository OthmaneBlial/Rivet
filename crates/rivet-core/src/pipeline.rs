use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use thiserror::Error;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Pipeline {
    pub version: u32,
    pub name: String,
    #[serde(default)]
    pub workspace: Option<PathBuf>,
    #[serde(default)]
    pub parameters: Vec<ParameterSpec>,
    #[serde(default)]
    pub artifacts: Vec<ArtifactSpec>,
    pub stages: Vec<Stage>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ParameterSpec {
    pub name: String,
    #[serde(default)]
    pub default: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ArtifactSpec {
    pub name: String,
    pub paths: Vec<String>,
    #[serde(default)]
    pub allow_empty: bool,
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
    #[error(
        "parameter name {0:?} is invalid; use a letter or underscore followed by letters, digits, or underscores"
    )]
    InvalidParameterName(String),
    #[error("parameter name {0:?} is reserved")]
    ReservedParameterName(String),
    #[error("duplicate parameter name {0:?}")]
    DuplicateParameter(String),
    #[error("parameter {0:?} is not declared by the pipeline")]
    UnknownParameter(String),
    #[error("required parameter {0:?} was not provided")]
    MissingParameter(String),
    #[error("artifact name cannot be empty")]
    EmptyArtifactName,
    #[error("artifact name {0:?} contains a path separator")]
    InvalidArtifactName(String),
    #[error("artifact {0:?} must declare at least one path")]
    EmptyArtifactPaths(String),
    #[error("artifact {artifact:?} has an invalid path {path:?}")]
    InvalidArtifactPath { artifact: String, path: String },
    #[error("duplicate artifact name {0:?}")]
    DuplicateArtifact(String),
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

        let mut parameter_names = HashSet::new();
        for parameter in &self.parameters {
            if !valid_parameter_name(&parameter.name) {
                return Err(PipelineError::InvalidParameterName(parameter.name.clone()));
            }
            if parameter.name == "CI"
                || parameter.name == "RIVET_BUILD_ID"
                || parameter.name == "RIVET_PROJECT_ID"
                || parameter.name.starts_with("RIVET_")
            {
                return Err(PipelineError::ReservedParameterName(parameter.name.clone()));
            }
            if !parameter_names.insert(parameter.name.as_str()) {
                return Err(PipelineError::DuplicateParameter(parameter.name.clone()));
            }
        }

        let mut artifact_names = HashSet::new();
        for artifact in &self.artifacts {
            if artifact.name.trim().is_empty() {
                return Err(PipelineError::EmptyArtifactName);
            }
            if artifact.name.contains('/') || artifact.name.contains('\\') {
                return Err(PipelineError::InvalidArtifactName(artifact.name.clone()));
            }
            if !artifact_names.insert(artifact.name.as_str()) {
                return Err(PipelineError::DuplicateArtifact(artifact.name.clone()));
            }
            if artifact.paths.is_empty() {
                return Err(PipelineError::EmptyArtifactPaths(artifact.name.clone()));
            }
            for path in &artifact.paths {
                let path_value = Path::new(path);
                if path.trim().is_empty()
                    || path.contains('\0')
                    || path_value.is_absolute()
                    || path_value
                        .components()
                        .any(|component| matches!(component, std::path::Component::ParentDir))
                {
                    return Err(PipelineError::InvalidArtifactPath {
                        artifact: artifact.name.clone(),
                        path: path.clone(),
                    });
                }
            }
        }

        let mut stage_names = HashSet::new();
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

    pub fn resolve_parameters(
        &self,
        supplied: &BTreeMap<String, String>,
    ) -> Result<BTreeMap<String, String>, PipelineError> {
        let declared = self
            .parameters
            .iter()
            .map(|parameter| parameter.name.as_str())
            .collect::<HashSet<_>>();
        if let Some(unknown) = supplied
            .keys()
            .find(|name| !declared.contains(name.as_str()))
        {
            return Err(PipelineError::UnknownParameter(unknown.clone()));
        }

        self.parameters
            .iter()
            .map(|parameter| {
                supplied
                    .get(&parameter.name)
                    .or(parameter.default.as_ref())
                    .cloned()
                    .map(|value| (parameter.name.clone(), value))
                    .ok_or_else(|| PipelineError::MissingParameter(parameter.name.clone()))
            })
            .collect()
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

fn valid_parameter_name(name: &str) -> bool {
    let mut characters = name.chars();
    matches!(characters.next(), Some(first) if first == '_' || first.is_ascii_alphabetic())
        && characters.all(|character| character == '_' || character.is_ascii_alphanumeric())
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
    fn resolves_defaults_and_rejects_unknown_or_missing_parameters() {
        let pipeline = Pipeline::from_toml_str(
            r#"
version = 1
name = "parameterized"

[[parameters]]
name = "target"
default = "debug"

[[parameters]]
name = "release_channel"

[[stages]]
name = "Test"
[[stages.steps]]
name = "unit"
program = "true"
"#,
        )
        .expect("pipeline");

        let resolved = pipeline
            .resolve_parameters(&BTreeMap::from([(
                "release_channel".to_owned(),
                "stable".to_owned(),
            )]))
            .expect("parameters resolve");
        assert_eq!(resolved["target"], "debug");
        assert_eq!(resolved["release_channel"], "stable");

        assert!(matches!(
            pipeline.resolve_parameters(&BTreeMap::from([(
                "unknown".to_owned(),
                "value".to_owned(),
            ),])),
            Err(PipelineError::UnknownParameter(name)) if name == "unknown"
        ));
        assert!(matches!(
            pipeline.resolve_parameters(&BTreeMap::new()),
            Err(PipelineError::MissingParameter(name)) if name == "release_channel"
        ));
    }

    #[test]
    fn rejects_parameter_names_that_could_override_engine_environment() {
        let result = Pipeline::from_toml_str(
            r#"
version = 1
name = "reserved"
[[parameters]]
name = "RIVET_BUILD_ID"
[[stages]]
name = "Test"
[[stages.steps]]
name = "unit"
program = "true"
"#,
        );

        assert!(matches!(
            result,
            Err(PipelineError::ReservedParameterName(name)) if name == "RIVET_BUILD_ID"
        ));
    }

    #[test]
    fn validates_workspace_scoped_artifact_patterns() {
        let pipeline = Pipeline::from_toml_str(
            r#"
version = 1
name = "artifacts"

[[artifacts]]
name = "bundle"
paths = ["dist/**", "manifest.json"]

[[stages]]
name = "Build"
[[stages.steps]]
name = "compile"
program = "true"
"#,
        )
        .expect("pipeline");
        assert_eq!(pipeline.artifacts[0].paths, ["dist/**", "manifest.json"]);

        let invalid = Pipeline::from_toml_str(
            r#"
version = 1
name = "unsafe-artifacts"
[[artifacts]]
name = "bundle"
paths = ["../outside/**"]
[[stages]]
name = "Build"
[[stages.steps]]
name = "compile"
program = "true"
"#,
        );
        assert!(matches!(
            invalid,
            Err(PipelineError::InvalidArtifactPath { artifact, path })
                if artifact == "bundle" && path == "../outside/**"
        ));
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
