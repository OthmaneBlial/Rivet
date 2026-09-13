use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use thiserror::Error;

const MAX_AGENT_REQUIREMENT_VALUE_BYTES: usize = 64;
const MAX_AGENT_REQUIREMENT_LABELS: usize = 64;
const MAX_AGENT_CPU_CORES: u16 = 4096;
const MAX_AGENT_MEMORY_MB: u64 = 4 * 1024 * 1024;
const MAX_ENVIRONMENT_VARIABLES: usize = 128;
const MAX_ENVIRONMENT_NAME_BYTES: usize = 256;
const MAX_ENVIRONMENT_VALUE_BYTES: usize = 16 * 1024;
const MAX_STEP_RETRIES: u8 = 5;
const MAX_STEP_RETRY_DELAY_SECONDS: u64 = 300;
const MAX_CONTAINER_IMAGE_BYTES: usize = 512;
const MAX_CONTAINER_NETWORK_BYTES: usize = 128;
const MAX_CONTAINER_VOLUMES: usize = 16;
const MAX_STAGE_CONDITION_VALUE_BYTES: usize = 256;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Pipeline {
    pub version: u32,
    pub name: String,
    #[serde(default)]
    pub workspace: Option<PathBuf>,
    /// Non-secret defaults inherited by every process in the pipeline.
    /// Secret values belong in secret parameters or the credential vault.
    #[serde(default)]
    pub environment: BTreeMap<String, String>,
    #[serde(default)]
    pub parameters: Vec<ParameterSpec>,
    #[serde(default)]
    pub artifacts: Vec<ArtifactSpec>,
    #[serde(default)]
    pub caches: Vec<CacheSpec>,
    pub stages: Vec<Stage>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ParameterSpec {
    pub name: String,
    #[serde(default)]
    pub default: Option<String>,
    #[serde(default)]
    pub secret: bool,
}

pub const REDACTED_PARAMETER_VALUE: &str = "[redacted]";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ArtifactSpec {
    pub name: String,
    pub paths: Vec<String>,
    #[serde(default)]
    pub allow_empty: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CacheSpec {
    pub name: String,
    pub key: String,
    pub paths: Vec<String>,
    #[serde(default)]
    pub fallback_keys: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Stage {
    pub name: String,
    /// Stage names that must complete successfully before this stage starts.
    /// An empty list preserves the declaration-order behavior for independent
    /// stages while keeping the execution graph explicit.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub depends_on: Vec<String>,
    /// Optional deterministic gate evaluated against resolved build
    /// parameters before the stage is admitted to the runner.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub condition: Option<StageCondition>,
    pub steps: Vec<Step>,
}

/// A deliberately small declarative stage gate. Arbitrary expressions and
/// process execution are intentionally not part of the pipeline contract.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StageCondition {
    pub parameter: String,
    #[serde(default)]
    pub equals: Option<String>,
    #[serde(default)]
    pub not_equals: Option<String>,
}

impl StageCondition {
    pub fn evaluate(&self, parameters: &BTreeMap<String, String>) -> bool {
        let actual = parameters.get(&self.parameter).map(String::as_str);
        if let Some(expected) = self.equals.as_deref() {
            return actual == Some(expected);
        }
        if let Some(expected) = self.not_equals.as_deref() {
            return actual != Some(expected);
        }
        false
    }
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
    /// Number of additional attempts after a failed or timed-out process.
    #[serde(default)]
    pub retries: u8,
    /// Delay between attempts. Cancellation interrupts the delay.
    #[serde(default)]
    pub retry_delay_seconds: u64,
    #[serde(default)]
    pub container: Option<ContainerSpec>,
    /// Optional remote-agent requirements. A local runner must reject this
    /// explicitly until a server assigns the build to a matching worker.
    #[serde(default)]
    pub agent: Option<AgentRequirement>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ContainerSpec {
    pub image: String,
    /// Ask the runtime to refresh the image before every container start.
    #[serde(default)]
    pub pull: ContainerPullPolicy,
    /// Optional Docker network name or one of Docker's built-in network names.
    #[serde(default)]
    pub network: Option<String>,
    /// Additional bind mounts. Sources are always relative to the workspace and
    /// targets are confined to the container workspace.
    #[serde(default)]
    pub volumes: Vec<ContainerVolume>,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ContainerPullPolicy {
    #[default]
    IfNotPresent,
    Always,
    Never,
}

impl ContainerPullPolicy {
    pub fn docker_value(self) -> &'static str {
        match self {
            Self::IfNotPresent => "missing",
            Self::Always => "always",
            Self::Never => "never",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ContainerVolume {
    pub source: PathBuf,
    pub target: PathBuf,
    #[serde(default)]
    pub read_only: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct AgentRequirement {
    #[serde(default)]
    pub os: Option<String>,
    #[serde(default)]
    pub arch: Option<String>,
    #[serde(default)]
    pub docker: bool,
    #[serde(default)]
    pub labels: Vec<String>,
    #[serde(default)]
    pub executors: Option<u16>,
    /// Minimum CPU capacity that the assigned agent must advertise. An
    /// omitted value means the pipeline has no CPU-specific requirement.
    #[serde(default)]
    pub cpu_cores: Option<u16>,
    /// Minimum memory capacity in MiB that the assigned agent must advertise.
    /// An omitted value means the pipeline has no memory-specific requirement.
    #[serde(default)]
    pub memory_mb: Option<u64>,
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
    #[error("stage {stage:?} depends on unknown stage {dependency:?}")]
    UnknownStageDependency { stage: String, dependency: String },
    #[error("stage {stage:?} depends on itself")]
    StageDependsOnSelf { stage: String },
    #[error("stage {stage:?} declares duplicate dependency {dependency:?}")]
    DuplicateStageDependency { stage: String, dependency: String },
    #[error("stage {stage:?} declares an invalid condition")]
    InvalidStageCondition { stage: String },
    #[error("stage {stage:?} condition references unknown parameter {parameter:?}")]
    UnknownStageConditionParameter { stage: String, parameter: String },
    #[error("stage {stage:?} condition cannot inspect secret parameter {parameter:?}")]
    SecretStageConditionParameter { stage: String, parameter: String },
    #[error("stage dependencies contain a cycle")]
    StageDependencyCycle,
    #[error("step {step:?} in stage {stage:?} cannot be empty")]
    EmptyStep { stage: String, step: String },
    #[error("duplicate step name {step:?} in stage {stage:?}")]
    DuplicateStep { stage: String, step: String },
    #[error("step {step:?} in stage {stage:?} has no executable")]
    EmptyProgram { stage: String, step: String },
    #[error("step {step:?} in stage {stage:?} has an invalid timeout")]
    InvalidTimeout { stage: String, step: String },
    #[error("step {step:?} in stage {stage:?} declares too many retries")]
    TooManyRetries { stage: String, step: String },
    #[error("step {step:?} in stage {stage:?} declares an invalid retry delay")]
    InvalidRetryDelay { stage: String, step: String },
    #[error("step {step:?} in stage {stage:?} has no container image")]
    EmptyContainerImage { stage: String, step: String },
    #[error("container image for step {step:?} in stage {stage:?} is invalid")]
    InvalidContainerImage { stage: String, step: String },
    #[error("container network for step {step:?} in stage {stage:?} is invalid")]
    InvalidContainerNetwork { stage: String, step: String },
    #[error("container volumes for step {step:?} in stage {stage:?} exceed the limit")]
    TooManyContainerVolumes { stage: String, step: String },
    #[error("container volume for step {step:?} in stage {stage:?} is invalid")]
    InvalidContainerVolume { stage: String, step: String },
    #[error("agent requirement {field} for step {step:?} in stage {stage:?} cannot be empty")]
    EmptyAgentRequirement {
        stage: String,
        step: String,
        field: &'static str,
    },
    #[error("agent requirement {field} for step {step:?} in stage {stage:?} is invalid")]
    InvalidAgentRequirement {
        stage: String,
        step: String,
        field: &'static str,
    },
    #[error("agent requirement labels for step {step:?} in stage {stage:?} are invalid")]
    InvalidAgentLabels { stage: String, step: String },
    #[error(
        "agent requirement for step {step:?} in stage {stage:?} must request at least one executor"
    )]
    ZeroAgentExecutors { stage: String, step: String },
    #[error(
        "agent requirement cpu_cores for step {step:?} in stage {stage:?} must request at least one core"
    )]
    ZeroAgentCpuCores { stage: String, step: String },
    #[error(
        "agent requirement memory_mb for step {step:?} in stage {stage:?} must request at least one MiB"
    )]
    ZeroAgentMemory { stage: String, step: String },
    #[error("agent requirement cpu_cores for step {step:?} in stage {stage:?} is too large")]
    InvalidAgentCpuCores { stage: String, step: String },
    #[error("agent requirement memory_mb for step {step:?} in stage {stage:?} is too large")]
    InvalidAgentMemory { stage: String, step: String },
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
    #[error("secret parameter {0:?} cannot define a default value")]
    SecretParameterDefault(String),
    #[error("pipeline environment declares too many variables")]
    TooManyEnvironmentVariables,
    #[error("pipeline environment variable name {0:?} is invalid")]
    InvalidEnvironmentVariableName(String),
    #[error("pipeline environment variable {name:?} has an invalid value")]
    InvalidEnvironmentVariableValue { name: String },
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
    #[error("cache name cannot be empty")]
    EmptyCacheName,
    #[error("cache name {0:?} is invalid")]
    InvalidCacheName(String),
    #[error("cache key for {0:?} cannot be empty")]
    EmptyCacheKey(String),
    #[error("cache key for {name:?} is too long")]
    CacheKeyTooLong { name: String },
    #[error("cache key for {name:?} contains a control character")]
    InvalidCacheKey { name: String },
    #[error("cache {name:?} declares too many fallback keys")]
    TooManyCacheFallbackKeys { name: String },
    #[error("cache fallback key for {name:?} cannot be empty")]
    EmptyCacheFallbackKey { name: String },
    #[error("cache fallback key for {name:?} is too long")]
    CacheFallbackKeyTooLong { name: String },
    #[error("cache fallback key for {name:?} contains a control character")]
    InvalidCacheFallbackKey { name: String },
    #[error("cache {name:?} repeats a fallback key")]
    DuplicateCacheFallbackKey { name: String },
    #[error("cache {name:?} repeats its primary key as a fallback")]
    PrimaryCacheFallbackKey { name: String },
    #[error("cache {0:?} must declare at least one path")]
    EmptyCachePaths(String),
    #[error("cache {cache:?} has an invalid path {path:?}")]
    InvalidCachePath { cache: String, path: String },
    #[error("duplicate cache name {0:?}")]
    DuplicateCache(String),
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

        if self.environment.len() > MAX_ENVIRONMENT_VARIABLES {
            return Err(PipelineError::TooManyEnvironmentVariables);
        }
        for (name, value) in &self.environment {
            if name.len() > MAX_ENVIRONMENT_NAME_BYTES
                || !valid_parameter_name(name)
                || name == "CI"
                || name == "RIVET_BUILD_ID"
                || name == "RIVET_PROJECT_ID"
                || name.starts_with("RIVET_")
            {
                return Err(PipelineError::InvalidEnvironmentVariableName(name.clone()));
            }
            if value.len() > MAX_ENVIRONMENT_VALUE_BYTES
                || value.contains('\0')
                || value.chars().any(char::is_control)
            {
                return Err(PipelineError::InvalidEnvironmentVariableValue { name: name.clone() });
            }
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
            if parameter.secret && parameter.default.is_some() {
                return Err(PipelineError::SecretParameterDefault(
                    parameter.name.clone(),
                ));
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

        let mut cache_names = HashSet::new();
        for cache in &self.caches {
            if cache.name.trim().is_empty() {
                return Err(PipelineError::EmptyCacheName);
            }
            if !valid_cache_name(&cache.name) {
                return Err(PipelineError::InvalidCacheName(cache.name.clone()));
            }
            if !cache_names.insert(cache.name.as_str()) {
                return Err(PipelineError::DuplicateCache(cache.name.clone()));
            }
            if cache.key.trim().is_empty() {
                return Err(PipelineError::EmptyCacheKey(cache.name.clone()));
            }
            if cache.key.len() > 256 {
                return Err(PipelineError::CacheKeyTooLong {
                    name: cache.name.clone(),
                });
            }
            if cache.key.chars().any(char::is_control) {
                return Err(PipelineError::InvalidCacheKey {
                    name: cache.name.clone(),
                });
            }
            if cache.fallback_keys.len() > 16 {
                return Err(PipelineError::TooManyCacheFallbackKeys {
                    name: cache.name.clone(),
                });
            }
            let mut fallback_keys = HashSet::new();
            for fallback_key in &cache.fallback_keys {
                if fallback_key.trim().is_empty() {
                    return Err(PipelineError::EmptyCacheFallbackKey {
                        name: cache.name.clone(),
                    });
                }
                if fallback_key.len() > 256 {
                    return Err(PipelineError::CacheFallbackKeyTooLong {
                        name: cache.name.clone(),
                    });
                }
                if fallback_key.chars().any(char::is_control) {
                    return Err(PipelineError::InvalidCacheFallbackKey {
                        name: cache.name.clone(),
                    });
                }
                if fallback_key == &cache.key {
                    return Err(PipelineError::PrimaryCacheFallbackKey {
                        name: cache.name.clone(),
                    });
                }
                if !fallback_keys.insert(fallback_key.as_str()) {
                    return Err(PipelineError::DuplicateCacheFallbackKey {
                        name: cache.name.clone(),
                    });
                }
            }
            if cache.paths.is_empty() {
                return Err(PipelineError::EmptyCachePaths(cache.name.clone()));
            }
            for path in &cache.paths {
                let path_value = Path::new(path);
                if path.trim().is_empty()
                    || path.contains('\0')
                    || path.chars().any(char::is_control)
                    || path_value.is_absolute()
                    || path_value == Path::new(".")
                    || path
                        .chars()
                        .any(|character| matches!(character, '*' | '?' | '[' | ']'))
                    || path_value
                        .components()
                        .any(|component| matches!(component, std::path::Component::ParentDir))
                {
                    return Err(PipelineError::InvalidCachePath {
                        cache: cache.name.clone(),
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
            if let Some(condition) = &stage.condition {
                if !valid_parameter_name(&condition.parameter) {
                    return Err(PipelineError::InvalidStageCondition {
                        stage: stage.name.clone(),
                    });
                }
                if !parameter_names.contains(condition.parameter.as_str()) {
                    return Err(PipelineError::UnknownStageConditionParameter {
                        stage: stage.name.clone(),
                        parameter: condition.parameter.clone(),
                    });
                }
                if self
                    .parameters
                    .iter()
                    .any(|parameter| parameter.name == condition.parameter && parameter.secret)
                {
                    return Err(PipelineError::SecretStageConditionParameter {
                        stage: stage.name.clone(),
                        parameter: condition.parameter.clone(),
                    });
                }
                let condition_value = condition
                    .equals
                    .as_deref()
                    .or(condition.not_equals.as_deref());
                if condition.equals.is_some() == condition.not_equals.is_some()
                    || condition_value.is_some_and(|value| {
                        value.len() > MAX_STAGE_CONDITION_VALUE_BYTES
                            || value.chars().any(char::is_control)
                    })
                {
                    return Err(PipelineError::InvalidStageCondition {
                        stage: stage.name.clone(),
                    });
                }
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
                if step.retries > MAX_STEP_RETRIES {
                    return Err(PipelineError::TooManyRetries {
                        stage: stage.name.clone(),
                        step: step.name.clone(),
                    });
                }
                if step.retry_delay_seconds > MAX_STEP_RETRY_DELAY_SECONDS {
                    return Err(PipelineError::InvalidRetryDelay {
                        stage: stage.name.clone(),
                        step: step.name.clone(),
                    });
                }
                if let Some(container) = &step.container {
                    if container.image.trim().is_empty() {
                        return Err(PipelineError::EmptyContainerImage {
                            stage: stage.name.clone(),
                            step: step.name.clone(),
                        });
                    }
                    if container
                        .image
                        .chars()
                        .any(|character| character.is_whitespace() || character.is_control())
                        || container.image.len() > MAX_CONTAINER_IMAGE_BYTES
                        || container.image.starts_with('-')
                    {
                        return Err(PipelineError::InvalidContainerImage {
                            stage: stage.name.clone(),
                            step: step.name.clone(),
                        });
                    }
                    if let Some(network) = &container.network {
                        if network.trim().is_empty()
                            || network.len() > MAX_CONTAINER_NETWORK_BYTES
                            || network.starts_with('-')
                            || network.chars().any(|character| {
                                !(character.is_ascii_alphanumeric()
                                    || matches!(character, '.' | '_' | '-'))
                            })
                        {
                            return Err(PipelineError::InvalidContainerNetwork {
                                stage: stage.name.clone(),
                                step: step.name.clone(),
                            });
                        }
                    }
                    if container.volumes.len() > MAX_CONTAINER_VOLUMES {
                        return Err(PipelineError::TooManyContainerVolumes {
                            stage: stage.name.clone(),
                            step: step.name.clone(),
                        });
                    }
                    for volume in &container.volumes {
                        let source_invalid = volume.source.as_os_str().is_empty()
                            || volume.source.is_absolute()
                            || volume.source == Path::new(".")
                            || volume.source.components().any(|component| {
                                matches!(component, std::path::Component::ParentDir)
                            });
                        let target_invalid = volume.target.is_relative()
                            || !volume.target.starts_with(Path::new("/rivet/workspace"))
                            || volume.target.components().any(|component| {
                                matches!(component, std::path::Component::ParentDir)
                            });
                        let source = volume.source.to_string_lossy();
                        let target = volume.target.to_string_lossy();
                        let contains_control =
                            source.chars().chain(target.chars()).any(char::is_control);
                        if source_invalid || target_invalid || contains_control {
                            return Err(PipelineError::InvalidContainerVolume {
                                stage: stage.name.clone(),
                                step: step.name.clone(),
                            });
                        }
                    }
                }
                if let Some(agent) = &step.agent {
                    for (field, value) in
                        [("os", agent.os.as_deref()), ("arch", agent.arch.as_deref())]
                    {
                        let Some(value) = value else {
                            continue;
                        };
                        if value.trim().is_empty() {
                            return Err(PipelineError::EmptyAgentRequirement {
                                stage: stage.name.clone(),
                                step: step.name.clone(),
                                field,
                            });
                        }
                        if value.len() > MAX_AGENT_REQUIREMENT_VALUE_BYTES
                            || value.chars().any(char::is_control)
                        {
                            return Err(PipelineError::InvalidAgentRequirement {
                                stage: stage.name.clone(),
                                step: step.name.clone(),
                                field,
                            });
                        }
                    }
                    if agent.labels.len() > MAX_AGENT_REQUIREMENT_LABELS
                        || agent.labels.iter().any(|label| {
                            label.trim().is_empty()
                                || label.len() > MAX_AGENT_REQUIREMENT_VALUE_BYTES
                                || label.chars().any(char::is_control)
                        })
                    {
                        return Err(PipelineError::InvalidAgentLabels {
                            stage: stage.name.clone(),
                            step: step.name.clone(),
                        });
                    }
                    if agent.executors == Some(0) {
                        return Err(PipelineError::ZeroAgentExecutors {
                            stage: stage.name.clone(),
                            step: step.name.clone(),
                        });
                    }
                    if agent.cpu_cores == Some(0) {
                        return Err(PipelineError::ZeroAgentCpuCores {
                            stage: stage.name.clone(),
                            step: step.name.clone(),
                        });
                    }
                    if agent
                        .cpu_cores
                        .is_some_and(|cores| cores > MAX_AGENT_CPU_CORES)
                    {
                        return Err(PipelineError::InvalidAgentCpuCores {
                            stage: stage.name.clone(),
                            step: step.name.clone(),
                        });
                    }
                    if agent.memory_mb == Some(0) {
                        return Err(PipelineError::ZeroAgentMemory {
                            stage: stage.name.clone(),
                            step: step.name.clone(),
                        });
                    }
                    if agent
                        .memory_mb
                        .is_some_and(|memory| memory > MAX_AGENT_MEMORY_MB)
                    {
                        return Err(PipelineError::InvalidAgentMemory {
                            stage: stage.name.clone(),
                            step: step.name.clone(),
                        });
                    }
                }
            }
        }

        self.stage_order_unchecked()?;

        Ok(())
    }

    /// Return a stable topological order for a validated pipeline. Independent
    /// stages retain their declaration order, while a dependency may refer to
    /// a stage declared later in the TOML file.
    pub fn stage_order(&self) -> Result<Vec<usize>, PipelineError> {
        self.validate()?;
        self.stage_order_unchecked()
    }

    fn stage_order_unchecked(&self) -> Result<Vec<usize>, PipelineError> {
        let stage_indexes = self
            .stages
            .iter()
            .enumerate()
            .map(|(index, stage)| (stage.name.as_str(), index))
            .collect::<BTreeMap<_, _>>();
        let mut indegrees = vec![0_usize; self.stages.len()];
        let mut dependents = vec![Vec::<usize>::new(); self.stages.len()];
        for (index, stage) in self.stages.iter().enumerate() {
            let mut dependencies = BTreeSet::new();
            for dependency in &stage.depends_on {
                if dependency == &stage.name {
                    return Err(PipelineError::StageDependsOnSelf {
                        stage: stage.name.clone(),
                    });
                }
                let Some(&dependency_index) = stage_indexes.get(dependency.as_str()) else {
                    return Err(PipelineError::UnknownStageDependency {
                        stage: stage.name.clone(),
                        dependency: dependency.clone(),
                    });
                };
                if !dependencies.insert(dependency_index) {
                    return Err(PipelineError::DuplicateStageDependency {
                        stage: stage.name.clone(),
                        dependency: dependency.clone(),
                    });
                }
                indegrees[index] += 1;
                dependents[dependency_index].push(index);
            }
        }

        let mut ready = (0..self.stages.len())
            .filter(|index| indegrees[*index] == 0)
            .collect::<BTreeSet<_>>();
        let mut order = Vec::with_capacity(self.stages.len());
        while let Some(&index) = ready.first() {
            ready.remove(&index);
            order.push(index);
            for dependent in &dependents[index] {
                indegrees[*dependent] -= 1;
                if indegrees[*dependent] == 0 {
                    ready.insert(*dependent);
                }
            }
        }
        if order.len() != self.stages.len() {
            return Err(PipelineError::StageDependencyCycle);
        }
        Ok(order)
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
                let supplied_value = supplied.get(&parameter.name).filter(|value| {
                    !(parameter.secret && value.as_str() == REDACTED_PARAMETER_VALUE)
                });
                supplied_value
                    .or(parameter.default.as_ref())
                    .cloned()
                    .map(|value| (parameter.name.clone(), value))
                    .ok_or_else(|| PipelineError::MissingParameter(parameter.name.clone()))
            })
            .collect()
    }

    pub fn has_secret_parameters(&self) -> bool {
        self.parameters.iter().any(|parameter| parameter.secret)
    }

    pub fn redact_parameters(
        &self,
        parameters: &BTreeMap<String, String>,
    ) -> BTreeMap<String, String> {
        parameters
            .iter()
            .map(|(name, value)| {
                let value = if self
                    .parameters
                    .iter()
                    .any(|parameter| parameter.secret && parameter.name == *name)
                {
                    REDACTED_PARAMETER_VALUE.to_owned()
                } else {
                    value.clone()
                };
                (name.clone(), value)
            })
            .collect()
    }

    pub fn secret_values(&self, parameters: &BTreeMap<String, String>) -> Vec<String> {
        self.parameters
            .iter()
            .filter(|parameter| parameter.secret)
            .filter_map(|parameter| parameters.get(&parameter.name))
            .filter(|value| !value.is_empty())
            .cloned()
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

fn valid_cache_name(name: &str) -> bool {
    name.chars().enumerate().all(|(index, character)| {
        if index == 0 {
            character == '_' || character.is_ascii_alphabetic()
        } else {
            character == '_' || character == '-' || character.is_ascii_alphanumeric()
        }
    })
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
        assert!(pipeline.environment.is_empty());
    }

    #[test]
    fn validates_and_orders_stage_dependencies_as_a_stable_dag() {
        let pipeline = Pipeline::from_toml_str(
            r#"
version = 1
name = "dag"
[[stages]]
name = "Deploy"
depends_on = ["Build"]
[[stages.steps]]
name = "deploy"
program = "true"
[[stages]]
name = "Build"
depends_on = ["Test"]
[[stages.steps]]
name = "build"
program = "true"
[[stages]]
name = "Test"
[[stages.steps]]
name = "test"
program = "true"
[[stages]]
name = "Independent"
[[stages.steps]]
name = "lint"
program = "true"
"#,
        )
        .expect("DAG pipeline");
        assert_eq!(
            pipeline.stage_order().expect("topological order"),
            [2, 1, 0, 3]
        );

        let cycle = Pipeline::from_toml_str(
            r#"
version = 1
name = "cycle"
[[stages]]
name = "A"
depends_on = ["B"]
[[stages.steps]]
name = "a"
program = "true"
[[stages]]
name = "B"
depends_on = ["A"]
[[stages.steps]]
name = "b"
program = "true"
"#,
        )
        .expect_err("cycles must be rejected");
        assert!(matches!(cycle, PipelineError::StageDependencyCycle));

        let unknown = Pipeline::from_toml_str(
            r#"
version = 1
name = "unknown-dependency"
[[stages]]
name = "Build"
depends_on = ["Missing"]
[[stages.steps]]
name = "build"
program = "true"
"#,
        )
        .expect_err("unknown dependencies must be rejected");
        assert!(matches!(
            unknown,
            PipelineError::UnknownStageDependency { .. }
        ));
    }

    #[test]
    fn validates_and_evaluates_safe_stage_conditions() {
        let pipeline = Pipeline::from_toml_str(
            r#"
version = 1
name = "conditional"
[[parameters]]
name = "DEPLOY"
default = "false"
[[stages]]
name = "Deploy"
[stages.condition]
parameter = "DEPLOY"
equals = "true"
[[stages.steps]]
name = "deploy"
program = "true"
"#,
        )
        .expect("conditional pipeline");
        let condition = pipeline.stages[0]
            .condition
            .as_ref()
            .expect("stage condition");
        assert!(!condition.evaluate(&BTreeMap::from([
            ("DEPLOY".to_owned(), "false".to_owned(),)
        ])));
        assert!(condition.evaluate(&BTreeMap::from([("DEPLOY".to_owned(), "true".to_owned(),)])));

        let both_operators = Pipeline::from_toml_str(
            r#"
version = 1
name = "invalid-condition"
[[parameters]]
name = "DEPLOY"
[[stages]]
name = "Deploy"
[stages.condition]
parameter = "DEPLOY"
equals = "true"
not_equals = "false"
[[stages.steps]]
name = "deploy"
program = "true"
"#,
        )
        .expect_err("conditions must use one operator");
        assert!(matches!(
            both_operators,
            PipelineError::InvalidStageCondition { stage } if stage == "Deploy"
        ));

        let unknown_parameter = Pipeline::from_toml_str(
            r#"
version = 1
name = "unknown-condition"
[[stages]]
name = "Deploy"
[stages.condition]
parameter = "MISSING"
equals = "true"
[[stages.steps]]
name = "deploy"
program = "true"
"#,
        )
        .expect_err("conditions must name a declared parameter");
        assert!(matches!(
            unknown_parameter,
            PipelineError::UnknownStageConditionParameter { parameter, .. }
                if parameter == "MISSING"
        ));

        let secret_parameter = Pipeline::from_toml_str(
            r#"
version = 1
name = "secret-condition"
[[parameters]]
name = "TOKEN"
secret = true
[[stages]]
name = "Deploy"
[stages.condition]
parameter = "TOKEN"
equals = "true"
[[stages.steps]]
name = "deploy"
program = "true"
"#,
        )
        .expect_err("conditions must not inspect secret parameters");
        assert!(matches!(
            secret_parameter,
            PipelineError::SecretStageConditionParameter { parameter, .. }
                if parameter == "TOKEN"
        ));
    }

    #[test]
    fn validates_pipeline_environment_defaults_and_reserved_names() {
        let pipeline = Pipeline::from_toml_str(
            r#"
version = 1
name = "environment"
[environment]
RUST_BACKTRACE = "1"
BUILD_CHANNEL = "stable"
[[stages]]
name = "Test"
[[stages.steps]]
name = "unit"
program = "true"
"#,
        )
        .expect("pipeline environment is valid");
        assert_eq!(pipeline.environment["RUST_BACKTRACE"], "1");
        assert_eq!(pipeline.environment["BUILD_CHANNEL"], "stable");

        let invalid = Pipeline::from_toml_str(
            r#"
version = 1
name = "reserved-environment"
[environment]
RIVET_BUILD_ID = "override"
[[stages]]
name = "Test"
[[stages.steps]]
name = "unit"
program = "true"
"#,
        )
        .expect_err("engine environment must be reserved");
        assert!(matches!(
            invalid,
            PipelineError::InvalidEnvironmentVariableName(name) if name == "RIVET_BUILD_ID"
        ));
    }

    #[test]
    fn validates_container_network_and_workspace_volume_boundaries() {
        let invalid_network = Pipeline::from_toml_str(
            r#"
version = 1
name = "invalid-container-network"
[[stages]]
name = "Build"
[[stages.steps]]
name = "unit"
program = "true"
[stages.steps.container]
image = "rust:1.85"
network = "unsafe/network"
"#,
        )
        .expect_err("network names must be bounded");
        assert!(matches!(
            invalid_network,
            PipelineError::InvalidContainerNetwork { .. }
        ));

        let invalid_volume = Pipeline::from_toml_str(
            r#"
version = 1
name = "invalid-container-volume"
[[stages]]
name = "Build"
[[stages.steps]]
name = "unit"
program = "true"
[stages.steps.container]
image = "rust:1.85"
[[stages.steps.container.volumes]]
source = "../secrets"
target = "/rivet/workspace/secrets"
"#,
        )
        .expect_err("volume sources must remain in the workspace");
        assert!(matches!(
            invalid_volume,
            PipelineError::InvalidContainerVolume { .. }
        ));
    }

    #[test]
    fn validates_explicit_remote_agent_requirements() {
        let pipeline = Pipeline::from_toml_str(
            r#"
version = 1
name = "remote"
[[stages]]
name = "Build"
[[stages.steps]]
name = "compile"
program = "cargo"
args = ["build"]
[stages.steps.agent]
os = "linux"
arch = "x86_64"
docker = true
labels = ["large-memory"]
executors = 2
cpu_cores = 4
memory_mb = 8192
"#,
        )
        .expect("remote requirement is valid");
        let requirement = pipeline.stages[0].steps[0]
            .agent
            .as_ref()
            .expect("agent requirement");
        assert_eq!(requirement.os.as_deref(), Some("linux"));
        assert_eq!(requirement.executors, Some(2));
        assert_eq!(requirement.cpu_cores, Some(4));
        assert_eq!(requirement.memory_mb, Some(8192));

        let invalid = Pipeline::from_toml_str(
            r#"
version = 1
name = "invalid-remote"
[[stages]]
name = "Build"
[[stages.steps]]
name = "compile"
program = "true"
[stages.steps.agent]
executors = 0
"#,
        )
        .expect_err("zero remote executors must be rejected");
        assert!(matches!(invalid, PipelineError::ZeroAgentExecutors { .. }));

        let invalid_cpu = Pipeline::from_toml_str(
            r#"
version = 1
name = "invalid-remote-cpu"
[[stages]]
name = "Build"
[[stages.steps]]
name = "compile"
program = "true"
[stages.steps.agent]
cpu_cores = 0
"#,
        )
        .expect_err("zero remote CPU must be rejected");
        assert!(matches!(
            invalid_cpu,
            PipelineError::ZeroAgentCpuCores { .. }
        ));

        let invalid_memory = Pipeline::from_toml_str(
            r#"
version = 1
name = "invalid-remote-memory"
[[stages]]
name = "Build"
[[stages.steps]]
name = "compile"
program = "true"
[stages.steps.agent]
memory_mb = 5_000_000
"#,
        )
        .expect_err("excessive remote memory must be rejected");
        assert!(matches!(
            invalid_memory,
            PipelineError::InvalidAgentMemory { .. }
        ));
    }

    #[test]
    fn bounds_step_retry_policy() {
        let too_many = Pipeline::from_toml_str(
            r#"
version = 1
name = "too-many-retries"
[[stages]]
name = "Test"
[[stages.steps]]
name = "unit"
program = "true"
retries = 6
"#,
        )
        .expect_err("retry count must be bounded");
        assert!(matches!(
            too_many,
            PipelineError::TooManyRetries { stage, step }
                if stage == "Test" && step == "unit"
        ));

        let too_slow = Pipeline::from_toml_str(
            r#"
version = 1
name = "slow-retries"
[[stages]]
name = "Test"
[[stages.steps]]
name = "unit"
program = "true"
retry_delay_seconds = 301
"#,
        )
        .expect_err("retry delay must be bounded");
        assert!(matches!(
            too_slow,
            PipelineError::InvalidRetryDelay { stage, step }
                if stage == "Test" && step == "unit"
        ));
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
    fn secret_parameters_require_runtime_values_and_redact_persisted_values() {
        let pipeline = Pipeline::from_toml_str(
            r#"
version = 1
name = "secrets"
[[parameters]]
name = "TOKEN"
secret = true
[[stages]]
name = "Test"
[[stages.steps]]
name = "unit"
program = "true"
"#,
        )
        .expect("pipeline");
        let parameters = pipeline
            .resolve_parameters(&BTreeMap::from([(
                "TOKEN".to_owned(),
                "runtime-secret".to_owned(),
            )]))
            .expect("secret value");
        assert_eq!(
            pipeline.redact_parameters(&parameters)["TOKEN"],
            REDACTED_PARAMETER_VALUE
        );
        assert_eq!(pipeline.secret_values(&parameters), ["runtime-secret"]);
        assert!(matches!(
            pipeline.resolve_parameters(&BTreeMap::new()),
            Err(PipelineError::MissingParameter(name)) if name == "TOKEN"
        ));
        assert!(matches!(
            pipeline.resolve_parameters(&BTreeMap::from([(
                "TOKEN".to_owned(),
                REDACTED_PARAMETER_VALUE.to_owned(),
            )])),
            Err(PipelineError::MissingParameter(name)) if name == "TOKEN"
        ));

        let invalid = Pipeline::from_toml_str(
            r#"
version = 1
name = "invalid-secret"
[[parameters]]
name = "TOKEN"
secret = true
default = "do-not-commit"
[[stages]]
name = "Test"
[[stages.steps]]
name = "unit"
program = "true"
"#,
        );
        assert!(matches!(
            invalid,
            Err(PipelineError::SecretParameterDefault(name)) if name == "TOKEN"
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

    #[test]
    fn validates_project_scoped_cache_declarations() {
        let pipeline = Pipeline::from_toml_str(
            r#"
version = 1
name = "cache"
[[caches]]
name = "dependencies-v1"
key = "deps-v1"
fallback_keys = ["deps-default"]
paths = ["target", "node_modules"]
[[stages]]
name = "Test"
[[stages.steps]]
name = "unit"
program = "true"
"#,
        )
        .expect("cache pipeline");
        assert_eq!(pipeline.caches[0].name, "dependencies-v1");
        assert_eq!(pipeline.caches[0].paths, ["target", "node_modules"]);
        assert_eq!(pipeline.caches[0].fallback_keys, ["deps-default"]);

        let duplicate_fallback = Pipeline::from_toml_str(
            r#"
version = 1
name = "duplicate-fallback"
[[caches]]
name = "dependencies"
key = "deps-v1"
fallback_keys = ["deps-default", "deps-default"]
paths = ["target"]
[[stages]]
name = "Test"
[[stages.steps]]
name = "unit"
program = "true"
"#,
        );
        assert!(matches!(
            duplicate_fallback,
            Err(PipelineError::DuplicateCacheFallbackKey { name }) if name == "dependencies"
        ));

        let invalid = Pipeline::from_toml_str(
            r#"
version = 1
name = "invalid-cache"
[[caches]]
name = "dependencies"
key = "deps-v1"
paths = ["../outside"]
[[stages]]
name = "Test"
[[stages.steps]]
name = "unit"
program = "true"
"#,
        );
        assert!(matches!(
            invalid,
            Err(PipelineError::InvalidCachePath { cache, path })
                if cache == "dependencies" && path == "../outside"
        ));
    }
}
