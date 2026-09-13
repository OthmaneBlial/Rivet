//! SQLite persistence for Rivet's local-first execution model.
//!
//! The storage layer consumes domain events instead of knowing how processes
//! are run. This keeps the same persistence contract usable from the CLI,
//! server, and embedded desktop engine.

use chrono::{DateTime, Utc};
use rivet_core::{
    BuildEvent, BuildId, BuildStatus, ExecutionPlan, LogStream, Pipeline, Project, ProjectId,
    StageId, StageStatus, StepId, StepStatus,
};
use rusqlite::{Connection, OptionalExtension, Transaction, params};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;
use std::sync::{Arc, Mutex};
use thiserror::Error;
use uuid::Uuid;

#[derive(Clone)]
pub struct Storage {
    connection: Arc<Mutex<Connection>>,
}

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("database error: {0}")]
    Database(#[from] rusqlite::Error),
    #[error("filesystem error: {0}")]
    Filesystem(#[from] std::io::Error),
    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("database mutex was poisoned")]
    Poisoned,
    #[error("invalid UUID in database: {0}")]
    InvalidUuid(String),
    #[error("invalid timestamp in database: {0}")]
    InvalidTimestamp(String),
    #[error("invalid {kind} status in database: {value}")]
    InvalidStatus { kind: &'static str, value: String },
    #[error("build {0} does not exist")]
    MissingBuild(BuildId),
    #[error("stage {0} does not exist")]
    MissingStage(StageId),
    #[error("step {0} does not exist")]
    MissingStep(StepId),
    #[error("invalid build transition from {from:?} to {to:?}")]
    InvalidBuildTransition { from: BuildStatus, to: BuildStatus },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BuildRecord {
    pub id: BuildId,
    pub project_id: ProjectId,
    pub number: i64,
    pub status: BuildStatus,
    pub queued_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StageRecord {
    pub id: StageId,
    pub build_id: BuildId,
    pub position: u32,
    pub name: String,
    pub status: StageStatus,
    pub started_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StepRecord {
    pub id: StepId,
    pub stage_id: StageId,
    pub position: u32,
    pub name: String,
    pub status: StepStatus,
    pub exit_code: Option<i32>,
    pub started_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StageDetails {
    pub stage: StageRecord,
    pub steps: Vec<StepRecord>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BuildDetails {
    pub build: BuildRecord,
    pub stages: Vec<StageDetails>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LogRecord {
    pub sequence: i64,
    pub build_id: BuildId,
    pub timestamp: DateTime<Utc>,
    pub stream: LogStream,
    pub line: String,
}

impl Storage {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, StorageError> {
        let path = path.as_ref();
        if path != Path::new(":memory:") {
            if let Some(parent) = path
                .parent()
                .filter(|parent| !parent.as_os_str().is_empty())
            {
                fs::create_dir_all(parent)?;
            }
        }
        let connection = Connection::open(path)?;
        connection.execute_batch("PRAGMA foreign_keys = ON;")?;
        connection.execute_batch(include_str!("../migrations/001_initial.sql"))?;
        connection.execute(
            "INSERT OR IGNORE INTO schema_migrations(version, applied_at) VALUES (?1, ?2)",
            params![1_i64, Utc::now().to_rfc3339()],
        )?;
        Ok(Self {
            connection: Arc::new(Mutex::new(connection)),
        })
    }

    pub fn open_in_memory() -> Result<Self, StorageError> {
        Self::open(":memory:")
    }

    pub fn create_project(
        &self,
        project: &Project,
        pipeline: &Pipeline,
    ) -> Result<(), StorageError> {
        let pipeline_json = serde_json::to_string(pipeline)?;
        let connection = self.connection.lock().map_err(|_| StorageError::Poisoned)?;
        connection.execute(
            "INSERT INTO projects(id, name, repository_path, pipeline_path, pipeline_json, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                project.id.to_string(),
                project.name,
                project.repository_path,
                project.pipeline_path,
                pipeline_json,
                project.created_at.to_rfc3339(),
            ],
        )?;
        Ok(())
    }

    pub fn get_project_by_name(&self, name: &str) -> Result<Option<Project>, StorageError> {
        let connection = self.connection.lock().map_err(|_| StorageError::Poisoned)?;
        let row = connection
            .query_row(
                "SELECT id, name, repository_path, pipeline_path, created_at
                 FROM projects WHERE name = ?1",
                params![name],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, String>(4)?,
                    ))
                },
            )
            .optional()?;
        row.map(parse_project).transpose()
    }

    pub fn list_projects(&self) -> Result<Vec<Project>, StorageError> {
        let connection = self.connection.lock().map_err(|_| StorageError::Poisoned)?;
        let mut statement = connection.prepare(
            "SELECT id, name, repository_path, pipeline_path, created_at
             FROM projects ORDER BY name ASC",
        )?;
        let rows = statement.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
            ))
        })?;
        rows.map(|row| row.map_err(StorageError::from).and_then(parse_project))
            .collect()
    }

    pub fn create_build(
        &self,
        project: &Project,
        plan: &ExecutionPlan,
        pipeline: &Pipeline,
    ) -> Result<BuildRecord, StorageError> {
        let mut connection = self.connection.lock().map_err(|_| StorageError::Poisoned)?;
        let transaction = connection.transaction()?;
        let number: i64 = transaction.query_row(
            "SELECT COALESCE(MAX(number), 0) + 1 FROM builds WHERE project_id = ?1",
            params![project.id.to_string()],
            |row| row.get(0),
        )?;
        let id = plan.build_id;
        let queued_at = Utc::now();
        transaction.execute(
            "INSERT INTO builds(id, project_id, number, status, queued_at)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                id.to_string(),
                project.id.to_string(),
                number,
                status_string(&BuildStatus::Pending)?,
                queued_at.to_rfc3339(),
            ],
        )?;
        for stage in &plan.stages {
            transaction.execute(
                "INSERT INTO build_stages(id, build_id, position, name, status)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                params![
                    stage.id.to_string(),
                    id.to_string(),
                    i64::from(stage.position),
                    stage.name,
                    status_string(&StageStatus::Pending)?,
                ],
            )?;
            for step in &stage.steps {
                transaction.execute(
                    "INSERT INTO build_steps(id, stage_id, position, name, status)
                     VALUES (?1, ?2, ?3, ?4, ?5)",
                    params![
                        step.id.to_string(),
                        stage.id.to_string(),
                        i64::from(step.position),
                        step.definition.name,
                        status_string(&StepStatus::Pending)?,
                    ],
                )?;
            }
        }
        transaction.execute(
            "UPDATE projects SET pipeline_json = ?1 WHERE id = ?2",
            params![serde_json::to_string(pipeline)?, project.id.to_string()],
        )?;
        transaction.commit()?;
        Ok(BuildRecord {
            id,
            project_id: project.id,
            number,
            status: BuildStatus::Pending,
            queued_at,
            started_at: None,
            finished_at: None,
        })
    }

    pub fn apply_event(&self, event: &BuildEvent) -> Result<(), StorageError> {
        let mut connection = self.connection.lock().map_err(|_| StorageError::Poisoned)?;
        let transaction = connection.transaction()?;
        match event {
            BuildEvent::BuildQueued {
                build_id,
                timestamp,
                ..
            } => transition_build(&transaction, *build_id, BuildStatus::Queued, *timestamp)?,
            BuildEvent::BuildStarted {
                build_id,
                timestamp,
            } => transition_build(&transaction, *build_id, BuildStatus::Running, *timestamp)?,
            BuildEvent::BuildFinished {
                build_id,
                status,
                timestamp,
            } => transition_build(&transaction, *build_id, status.clone(), *timestamp)?,
            BuildEvent::BuildCancelled {
                build_id,
                timestamp,
            } => transition_build(&transaction, *build_id, BuildStatus::Cancelled, *timestamp)?,
            BuildEvent::StageStarted {
                stage_id,
                timestamp,
                ..
            } => {
                let changed = transaction.execute(
                    "UPDATE build_stages SET status = ?1, started_at = ?2 WHERE id = ?3",
                    params![
                        status_string(&StageStatus::Running)?,
                        timestamp.to_rfc3339(),
                        stage_id.to_string(),
                    ],
                )?;
                if changed == 0 {
                    return Err(StorageError::MissingStage(*stage_id));
                }
            }
            BuildEvent::StageFinished {
                stage_id,
                status,
                timestamp,
                ..
            } => {
                let changed = transaction.execute(
                    "UPDATE build_stages SET status = ?1, finished_at = ?2 WHERE id = ?3",
                    params![
                        status_string(status)?,
                        timestamp.to_rfc3339(),
                        stage_id.to_string(),
                    ],
                )?;
                if changed == 0 {
                    return Err(StorageError::MissingStage(*stage_id));
                }
            }
            BuildEvent::StepStarted {
                step_id, timestamp, ..
            } => {
                let changed = transaction.execute(
                    "UPDATE build_steps SET status = ?1, started_at = ?2 WHERE id = ?3",
                    params![
                        status_string(&StepStatus::Running)?,
                        timestamp.to_rfc3339(),
                        step_id.to_string(),
                    ],
                )?;
                if changed == 0 {
                    return Err(StorageError::MissingStep(*step_id));
                }
            }
            BuildEvent::StepFinished {
                step_id,
                status,
                exit_code,
                timestamp,
                ..
            } => {
                let changed = transaction.execute(
                    "UPDATE build_steps SET status = ?1, exit_code = ?2, finished_at = ?3 WHERE id = ?4",
                    params![
                        status_string(status)?,
                        exit_code,
                        timestamp.to_rfc3339(),
                        step_id.to_string(),
                    ],
                )?;
                if changed == 0 {
                    return Err(StorageError::MissingStep(*step_id));
                }
            }
            BuildEvent::StepOutput {
                build_id,
                stream,
                line,
                timestamp,
                ..
            } => {
                transaction.execute(
                    "INSERT INTO build_logs(build_id, timestamp, stream, line)
                     VALUES (?1, ?2, ?3, ?4)",
                    params![
                        build_id.to_string(),
                        timestamp.to_rfc3339(),
                        status_string(stream)?,
                        line,
                    ],
                )?;
            }
        }
        transaction.commit()?;
        Ok(())
    }

    pub fn list_builds(&self, project_id: ProjectId) -> Result<Vec<BuildRecord>, StorageError> {
        let connection = self.connection.lock().map_err(|_| StorageError::Poisoned)?;
        let mut statement = connection.prepare(
            "SELECT id, project_id, number, status, queued_at, started_at, finished_at
             FROM builds WHERE project_id = ?1 ORDER BY number DESC",
        )?;
        let rows = statement.query_map(params![project_id.to_string()], raw_build)?;
        rows.map(|row| row.map_err(StorageError::from).and_then(parse_build))
            .collect()
    }

    pub fn get_build_details(
        &self,
        build_id: BuildId,
    ) -> Result<Option<BuildDetails>, StorageError> {
        let connection = self.connection.lock().map_err(|_| StorageError::Poisoned)?;
        let raw = connection
            .query_row(
                "SELECT id, project_id, number, status, queued_at, started_at, finished_at
                 FROM builds WHERE id = ?1",
                params![build_id.to_string()],
                raw_build,
            )
            .optional()?;
        let Some(raw_build) = raw else {
            return Ok(None);
        };
        let build = parse_build(raw_build)?;
        let mut stage_statement = connection.prepare(
            "SELECT id, build_id, position, name, status, started_at, finished_at
             FROM build_stages WHERE build_id = ?1 ORDER BY position ASC",
        )?;
        let raw_stages = stage_statement
            .query_map(params![build_id.to_string()], raw_stage)?
            .collect::<Result<Vec<_>, _>>()?;
        let mut stages = Vec::with_capacity(raw_stages.len());
        let mut step_statement = connection.prepare(
            "SELECT id, stage_id, position, name, status, exit_code, started_at, finished_at
             FROM build_steps WHERE stage_id = ?1 ORDER BY position ASC",
        )?;
        for raw_stage in raw_stages {
            let stage = parse_stage(raw_stage)?;
            let raw_steps = step_statement
                .query_map(params![stage.id.to_string()], raw_step)?
                .collect::<Result<Vec<_>, _>>()?;
            stages.push(StageDetails {
                stage,
                steps: raw_steps
                    .into_iter()
                    .map(parse_step)
                    .collect::<Result<Vec<_>, _>>()?,
            });
        }
        Ok(Some(BuildDetails { build, stages }))
    }

    pub fn logs(&self, build_id: BuildId) -> Result<Vec<LogRecord>, StorageError> {
        let connection = self.connection.lock().map_err(|_| StorageError::Poisoned)?;
        let mut statement = connection.prepare(
            "SELECT sequence, build_id, timestamp, stream, line
             FROM build_logs WHERE build_id = ?1 ORDER BY sequence ASC",
        )?;
        let rows = statement.query_map(params![build_id.to_string()], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
            ))
        })?;
        rows.map(|row| row.map_err(StorageError::from).and_then(parse_log))
            .collect()
    }
}

type RawProject = (String, String, String, String, String);
type RawBuild = (
    String,
    String,
    i64,
    String,
    String,
    Option<String>,
    Option<String>,
);
type RawStage = (
    String,
    String,
    i64,
    String,
    String,
    Option<String>,
    Option<String>,
);
type RawStep = (
    String,
    String,
    i64,
    String,
    String,
    Option<i32>,
    Option<String>,
    Option<String>,
);

fn parse_project(raw: RawProject) -> Result<Project, StorageError> {
    Ok(Project {
        id: parse_uuid(&raw.0)?,
        name: raw.1,
        repository_path: raw.2,
        pipeline_path: raw.3,
        created_at: parse_timestamp(&raw.4)?,
    })
}

fn raw_build(row: &rusqlite::Row<'_>) -> rusqlite::Result<RawBuild> {
    Ok((
        row.get(0)?,
        row.get(1)?,
        row.get(2)?,
        row.get(3)?,
        row.get(4)?,
        row.get(5)?,
        row.get(6)?,
    ))
}

fn parse_build(raw: RawBuild) -> Result<BuildRecord, StorageError> {
    Ok(BuildRecord {
        id: parse_uuid(&raw.0)?,
        project_id: parse_uuid(&raw.1)?,
        number: raw.2,
        status: parse_status(&raw.3, "build")?,
        queued_at: parse_timestamp(&raw.4)?,
        started_at: raw.5.as_deref().map(parse_timestamp).transpose()?,
        finished_at: raw.6.as_deref().map(parse_timestamp).transpose()?,
    })
}

fn raw_stage(row: &rusqlite::Row<'_>) -> rusqlite::Result<RawStage> {
    Ok((
        row.get(0)?,
        row.get(1)?,
        row.get(2)?,
        row.get(3)?,
        row.get(4)?,
        row.get(5)?,
        row.get(6)?,
    ))
}

fn parse_stage(raw: RawStage) -> Result<StageRecord, StorageError> {
    Ok(StageRecord {
        id: parse_uuid(&raw.0)?,
        build_id: parse_uuid(&raw.1)?,
        position: u32::try_from(raw.2).unwrap_or_default(),
        name: raw.3,
        status: parse_status(&raw.4, "stage")?,
        started_at: raw.5.as_deref().map(parse_timestamp).transpose()?,
        finished_at: raw.6.as_deref().map(parse_timestamp).transpose()?,
    })
}

fn raw_step(row: &rusqlite::Row<'_>) -> rusqlite::Result<RawStep> {
    Ok((
        row.get(0)?,
        row.get(1)?,
        row.get(2)?,
        row.get(3)?,
        row.get(4)?,
        row.get(5)?,
        row.get(6)?,
        row.get(7)?,
    ))
}

fn parse_step(raw: RawStep) -> Result<StepRecord, StorageError> {
    Ok(StepRecord {
        id: parse_uuid(&raw.0)?,
        stage_id: parse_uuid(&raw.1)?,
        position: u32::try_from(raw.2).unwrap_or_default(),
        name: raw.3,
        status: parse_status(&raw.4, "step")?,
        exit_code: raw.5,
        started_at: raw.6.as_deref().map(parse_timestamp).transpose()?,
        finished_at: raw.7.as_deref().map(parse_timestamp).transpose()?,
    })
}

fn parse_log(raw: (i64, String, String, String, String)) -> Result<LogRecord, StorageError> {
    Ok(LogRecord {
        sequence: raw.0,
        build_id: parse_uuid(&raw.1)?,
        timestamp: parse_timestamp(&raw.2)?,
        stream: parse_status(&raw.3, "log stream")?,
        line: raw.4,
    })
}

fn parse_uuid(value: &str) -> Result<Uuid, StorageError> {
    Uuid::parse_str(value).map_err(|_| StorageError::InvalidUuid(value.to_owned()))
}

fn parse_timestamp(value: &str) -> Result<DateTime<Utc>, StorageError> {
    DateTime::parse_from_rfc3339(value)
        .map(|timestamp| timestamp.with_timezone(&Utc))
        .map_err(|_| StorageError::InvalidTimestamp(value.to_owned()))
}

fn status_string<T: Serialize>(value: &T) -> Result<String, StorageError> {
    Ok(serde_json::to_value(value)?
        .as_str()
        .unwrap_or_default()
        .to_owned())
}

fn parse_status<T: for<'de> Deserialize<'de>>(
    value: &str,
    kind: &'static str,
) -> Result<T, StorageError> {
    serde_json::from_value(serde_json::Value::String(value.to_owned())).map_err(|_| {
        StorageError::InvalidStatus {
            kind,
            value: value.to_owned(),
        }
    })
}

fn transition_build(
    transaction: &Transaction<'_>,
    build_id: BuildId,
    next: BuildStatus,
    timestamp: DateTime<Utc>,
) -> Result<(), StorageError> {
    let raw: Option<String> = transaction
        .query_row(
            "SELECT status FROM builds WHERE id = ?1",
            params![build_id.to_string()],
            |row| row.get(0),
        )
        .optional()?;
    let Some(raw) = raw else {
        return Err(StorageError::MissingBuild(build_id));
    };
    let current: BuildStatus = parse_status(&raw, "build")?;
    if current != next && !current.can_transition_to(&next) {
        return Err(StorageError::InvalidBuildTransition {
            from: current,
            to: next,
        });
    }
    let next_raw = status_string(&next)?;
    transaction.execute(
        "UPDATE builds
         SET status = ?1,
             started_at = CASE WHEN ?2 = 'running' AND started_at IS NULL THEN ?3 ELSE started_at END,
             finished_at = CASE WHEN ?4 = 1 THEN COALESCE(finished_at, ?3) ELSE finished_at END
         WHERE id = ?5",
        params![
            next_raw,
            status_string(&BuildStatus::Running)?,
            timestamp.to_rfc3339(),
            if next.is_terminal() { 1_i64 } else { 0_i64 },
            build_id.to_string(),
        ],
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rivet_core::{BuildEvent, ExecutionPlan, Pipeline};
    use tempfile::tempdir;

    fn fixture() -> (Project, Pipeline, ExecutionPlan) {
        let project = Project::new("demo", ".", "Rivetfile.toml").expect("project");
        let pipeline = Pipeline::from_toml_str(
            r#"
version = 1
name = "demo"
[[stages]]
name = "Test"
[[stages.steps]]
name = "unit"
program = "true"
"#,
        )
        .expect("pipeline");
        let plan = ExecutionPlan::from_pipeline(&pipeline, Uuid::new_v4(), project.id);
        (project, pipeline, plan)
    }

    #[test]
    fn build_events_are_replayable_and_survive_reopen() {
        let directory = tempdir().expect("tempdir");
        let database = directory.path().join("rivet.db");
        let (project, pipeline, plan) = fixture();
        let storage = Storage::open(&database).expect("open");
        storage
            .create_project(&project, &pipeline)
            .expect("project");
        let build = storage
            .create_build(&project, &plan, &pipeline)
            .expect("build");
        storage
            .apply_event(&BuildEvent::BuildQueued {
                build_id: build.id,
                project_id: project.id,
                timestamp: Utc::now(),
            })
            .expect("queued");
        storage
            .apply_event(&BuildEvent::BuildStarted {
                build_id: build.id,
                timestamp: Utc::now(),
            })
            .expect("started");
        let stage = &plan.stages[0];
        let step = &stage.steps[0];
        storage
            .apply_event(&BuildEvent::StageStarted {
                build_id: build.id,
                stage_id: stage.id,
                stage_name: stage.name.clone(),
                timestamp: Utc::now(),
            })
            .expect("stage started");
        storage
            .apply_event(&BuildEvent::StepStarted {
                build_id: build.id,
                stage_id: stage.id,
                step_id: step.id,
                step_name: step.definition.name.clone(),
                timestamp: Utc::now(),
            })
            .expect("step started");
        storage
            .apply_event(&BuildEvent::StepOutput {
                build_id: build.id,
                stage_id: stage.id,
                step_id: step.id,
                stream: LogStream::Stdout,
                line: "hello".into(),
                timestamp: Utc::now(),
            })
            .expect("log");
        storage
            .apply_event(&BuildEvent::StepFinished {
                build_id: build.id,
                stage_id: stage.id,
                step_id: step.id,
                step_name: step.definition.name.clone(),
                status: StepStatus::Passed,
                exit_code: Some(0),
                timestamp: Utc::now(),
            })
            .expect("step finished");
        storage
            .apply_event(&BuildEvent::StageFinished {
                build_id: build.id,
                stage_id: stage.id,
                stage_name: stage.name.clone(),
                status: StageStatus::Passed,
                timestamp: Utc::now(),
            })
            .expect("stage finished");
        storage
            .apply_event(&BuildEvent::BuildFinished {
                build_id: build.id,
                status: BuildStatus::Passed,
                timestamp: Utc::now(),
            })
            .expect("finished");
        drop(storage);

        let reopened = Storage::open(&database).expect("reopen");
        let details = reopened
            .get_build_details(build.id)
            .expect("details")
            .expect("build exists");
        assert_eq!(details.build.status, BuildStatus::Passed);
        assert_eq!(details.stages[0].steps[0].status, StepStatus::Passed);
        assert_eq!(reopened.logs(build.id).expect("logs")[0].line, "hello");
    }
}
