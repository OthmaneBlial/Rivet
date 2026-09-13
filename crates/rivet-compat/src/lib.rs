//! Local semantic comparison for recorded Jenkins/Rivet behavior snapshots.
//!
//! The harness intentionally compares exported observations rather than
//! pretending to embed Jenkins. It is safe to run locally and keeps the
//! normalization rules explicit so a future live adapter can feed the same
//! comparator without changing its semantics.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use thiserror::Error;

pub const FIXTURE_SCHEMA_VERSION: u16 = 1;
const MAX_SCENARIO_BYTES: usize = 128;
const MAX_STAGES: usize = 128;
const MAX_STEPS_PER_STAGE: usize = 256;
const MAX_PARAMETERS: usize = 256;
const MAX_ARTIFACTS: usize = 256;
const MAX_LOG_LINES: usize = 4096;
const MAX_TEXT_BYTES: usize = 512;
const MAX_LOG_LINE_BYTES: usize = 8192;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BehaviorFixture {
    pub schema_version: u16,
    pub scenario: String,
    pub jenkins: BehaviorSnapshot,
    pub rivet: BehaviorSnapshot,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BehaviorSnapshot {
    pub status: String,
    pub stages: Vec<StageSnapshot>,
    #[serde(default)]
    pub parameters: BTreeMap<String, String>,
    #[serde(default)]
    pub artifacts: Vec<ArtifactSnapshot>,
    #[serde(default)]
    pub logs: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StageSnapshot {
    pub name: String,
    pub status: String,
    pub steps: Vec<StepSnapshot>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StepSnapshot {
    pub name: String,
    pub status: String,
    #[serde(default)]
    pub exit_code: Option<i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ArtifactSnapshot {
    pub name: String,
    #[serde(default)]
    pub checksum: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CompatibilityReport {
    pub scenario: String,
    pub matches: bool,
    pub differences: Vec<SemanticDifference>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SemanticDifference {
    pub field: String,
    pub jenkins: Option<String>,
    pub rivet: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct NormalizedSnapshot {
    status: String,
    stages: Vec<NormalizedStage>,
    parameters: BTreeMap<String, String>,
    artifacts: Vec<NormalizedArtifact>,
    logs: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct NormalizedStage {
    name: String,
    status: String,
    steps: Vec<NormalizedStep>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct NormalizedStep {
    name: String,
    status: String,
    exit_code: Option<i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct NormalizedArtifact {
    name: String,
    checksum: Option<String>,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum CompatError {
    #[error("compatibility fixture JSON is invalid: {0}")]
    InvalidJson(String),
    #[error(
        "compatibility fixture schema {found} is unsupported; expected {FIXTURE_SCHEMA_VERSION}"
    )]
    UnsupportedSchema { found: u16 },
    #[error("compatibility scenario cannot be empty or contain control characters")]
    InvalidScenario,
    #[error("compatibility fixture contains too many {0}")]
    TooMany(&'static str),
    #[error("compatibility {kind} name is empty, too long, or contains control characters")]
    InvalidName { kind: &'static str },
    #[error("compatibility {kind} status is unsupported: {value}")]
    InvalidStatus { kind: &'static str, value: String },
    #[error("compatibility parameter {0:?} is invalid")]
    InvalidParameter(String),
    #[error("compatibility log line is too long or contains control characters")]
    InvalidLogLine,
}

impl BehaviorFixture {
    pub fn from_json(bytes: &[u8]) -> Result<Self, CompatError> {
        let fixture: Self = serde_json::from_slice(bytes)
            .map_err(|error| CompatError::InvalidJson(error.to_string()))?;
        fixture.validate()?;
        Ok(fixture)
    }

    pub fn validate(&self) -> Result<(), CompatError> {
        if self.schema_version != FIXTURE_SCHEMA_VERSION {
            return Err(CompatError::UnsupportedSchema {
                found: self.schema_version,
            });
        }
        validate_text(
            &self.scenario,
            MAX_SCENARIO_BYTES,
            CompatError::InvalidScenario,
        )?;
        validate_snapshot(&self.jenkins)?;
        validate_snapshot(&self.rivet)?;
        Ok(())
    }
}

pub fn compare_fixture(fixture: &BehaviorFixture) -> Result<CompatibilityReport, CompatError> {
    fixture.validate()?;
    let jenkins = normalize_snapshot(&fixture.jenkins)?;
    let rivet = normalize_snapshot(&fixture.rivet)?;
    let mut differences = Vec::new();
    compare_value("status", &jenkins.status, &rivet.status, &mut differences);
    compare_value("stages", &jenkins.stages, &rivet.stages, &mut differences);
    compare_value(
        "parameters",
        &jenkins.parameters,
        &rivet.parameters,
        &mut differences,
    );
    compare_value(
        "artifacts",
        &jenkins.artifacts,
        &rivet.artifacts,
        &mut differences,
    );
    compare_value("logs", &jenkins.logs, &rivet.logs, &mut differences);
    Ok(CompatibilityReport {
        scenario: fixture.scenario.clone(),
        matches: differences.is_empty(),
        differences,
    })
}

fn validate_snapshot(snapshot: &BehaviorSnapshot) -> Result<(), CompatError> {
    normalize_status(&snapshot.status, "build")?;
    if snapshot.stages.len() > MAX_STAGES {
        return Err(CompatError::TooMany("stages"));
    }
    if snapshot.parameters.len() > MAX_PARAMETERS {
        return Err(CompatError::TooMany("parameters"));
    }
    for (name, value) in &snapshot.parameters {
        validate_text(
            name,
            MAX_TEXT_BYTES,
            CompatError::InvalidParameter(name.clone()),
        )?;
        validate_text(
            value,
            MAX_LOG_LINE_BYTES,
            CompatError::InvalidParameter(name.clone()),
        )?;
    }
    if snapshot.artifacts.len() > MAX_ARTIFACTS {
        return Err(CompatError::TooMany("artifacts"));
    }
    if snapshot.logs.len() > MAX_LOG_LINES {
        return Err(CompatError::TooMany("logs"));
    }
    for stage in &snapshot.stages {
        validate_name(&stage.name, "stage")?;
        normalize_status(&stage.status, "stage")?;
        if stage.steps.len() > MAX_STEPS_PER_STAGE {
            return Err(CompatError::TooMany("steps in a stage"));
        }
        for step in &stage.steps {
            validate_name(&step.name, "step")?;
            normalize_status(&step.status, "step")?;
        }
    }
    for artifact in &snapshot.artifacts {
        validate_name(&artifact.name, "artifact")?;
        if let Some(checksum) = &artifact.checksum {
            validate_text(
                checksum,
                MAX_TEXT_BYTES,
                CompatError::InvalidName { kind: "checksum" },
            )?;
        }
    }
    for line in &snapshot.logs {
        if line.len() > MAX_LOG_LINE_BYTES
            || line
                .chars()
                .any(|character| character.is_control() && character != '\r' && character != '\n')
        {
            return Err(CompatError::InvalidLogLine);
        }
    }
    Ok(())
}

fn normalize_snapshot(snapshot: &BehaviorSnapshot) -> Result<NormalizedSnapshot, CompatError> {
    Ok(NormalizedSnapshot {
        status: normalize_status(&snapshot.status, "build")?.to_owned(),
        stages: snapshot
            .stages
            .iter()
            .map(|stage| {
                Ok(NormalizedStage {
                    name: stage.name.trim().to_owned(),
                    status: normalize_status(&stage.status, "stage")?.to_owned(),
                    steps: stage
                        .steps
                        .iter()
                        .map(|step| {
                            let status = normalize_status(&step.status, "step")?;
                            Ok(NormalizedStep {
                                name: step.name.trim().to_owned(),
                                status: status.to_owned(),
                                exit_code: (status != "passed" || step.exit_code != Some(0))
                                    .then_some(step.exit_code)
                                    .flatten(),
                            })
                        })
                        .collect::<Result<Vec<_>, CompatError>>()?,
                })
            })
            .collect::<Result<Vec<_>, CompatError>>()?,
        parameters: snapshot
            .parameters
            .iter()
            .map(|(name, value)| (name.trim().to_owned(), normalize_secret(value)))
            .collect(),
        artifacts: snapshot
            .artifacts
            .iter()
            .map(|artifact| NormalizedArtifact {
                name: artifact.name.trim().to_owned(),
                checksum: artifact.checksum.as_deref().map(normalize_checksum),
            })
            .collect(),
        logs: snapshot
            .logs
            .iter()
            .map(|line| line.replace("\r\n", "\n").replace('\r', "\n"))
            .collect(),
    })
}

fn normalize_status(value: &str, kind: &'static str) -> Result<&'static str, CompatError> {
    match value.trim().to_ascii_lowercase().as_str() {
        "success" | "passed" | "pass" => Ok("passed"),
        "failure" | "failed" | "error" => Ok("failed"),
        "aborted" | "cancelled" | "canceled" => Ok("cancelled"),
        "unstable" => Ok("unstable"),
        "queued" => Ok("queued"),
        "running" => Ok("running"),
        "pending" => Ok("pending"),
        "skipped" => Ok("skipped"),
        _ => Err(CompatError::InvalidStatus {
            kind,
            value: value.to_owned(),
        }),
    }
}

fn normalize_secret(value: &str) -> String {
    match value {
        "[redacted]" | "***" | "****" | "<redacted>" => "<redacted>".into(),
        _ => value.to_owned(),
    }
}

fn normalize_checksum(value: &str) -> String {
    value
        .trim()
        .strip_prefix("sha256:")
        .unwrap_or(value.trim())
        .to_ascii_lowercase()
}

fn validate_name(value: &str, kind: &'static str) -> Result<(), CompatError> {
    validate_text(value, MAX_TEXT_BYTES, CompatError::InvalidName { kind })
}

fn validate_text(value: &str, max_bytes: usize, error: CompatError) -> Result<(), CompatError> {
    if value.trim().is_empty() || value.len() > max_bytes || value.chars().any(char::is_control) {
        return Err(error);
    }
    Ok(())
}

fn compare_value<T: Serialize + PartialEq>(
    field: &str,
    jenkins: &T,
    rivet: &T,
    differences: &mut Vec<SemanticDifference>,
) {
    if jenkins != rivet {
        differences.push(SemanticDifference {
            field: field.to_owned(),
            jenkins: serde_json::to_string(jenkins).ok(),
            rivet: serde_json::to_string(rivet).ok(),
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn snapshot(status: &str) -> BehaviorSnapshot {
        BehaviorSnapshot {
            status: status.into(),
            stages: vec![StageSnapshot {
                name: "Test".into(),
                status: status.into(),
                steps: vec![StepSnapshot {
                    name: "unit".into(),
                    status: status.into(),
                    exit_code: Some(0),
                }],
            }],
            parameters: BTreeMap::from([(String::from("TOKEN"), String::from("[redacted]"))]),
            artifacts: vec![ArtifactSnapshot {
                name: "bundle".into(),
                checksum: Some("sha256:ABCDEF".into()),
            }],
            logs: vec!["done\r\n".into()],
        }
    }

    fn fixture() -> BehaviorFixture {
        BehaviorFixture {
            schema_version: FIXTURE_SCHEMA_VERSION,
            scenario: "sequential-build".into(),
            jenkins: snapshot("SUCCESS"),
            rivet: snapshot("passed"),
        }
    }

    #[test]
    fn normalizes_provider_spelling_without_hiding_semantic_differences() {
        let report = compare_fixture(&fixture()).expect("comparison");
        assert!(report.matches);
        assert!(report.differences.is_empty());

        let mut changed = fixture();
        changed.rivet.stages[0].steps[0].status = "failed".into();
        let report = compare_fixture(&changed).expect("comparison");
        assert!(!report.matches);
        assert_eq!(report.differences[0].field, "stages");
    }

    #[test]
    fn rejects_unknown_status_and_oversized_fixture_sections() {
        let mut invalid = fixture();
        invalid.rivet.status = "green-ish".into();
        assert!(matches!(
            invalid.validate(),
            Err(CompatError::InvalidStatus { kind: "build", .. })
        ));

        invalid = fixture();
        invalid.rivet.logs = vec!["line".into(); MAX_LOG_LINES + 1];
        assert_eq!(invalid.validate(), Err(CompatError::TooMany("logs")));
    }

    #[test]
    fn fixture_json_round_trips() {
        let fixture = fixture();
        let bytes = serde_json::to_vec(&fixture).expect("fixture JSON");
        let decoded = BehaviorFixture::from_json(&bytes).expect("decoded fixture");
        assert_eq!(decoded, fixture);
    }
}
