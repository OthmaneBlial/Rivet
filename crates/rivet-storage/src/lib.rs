//! SQLite persistence for Rivet's local-first execution model.
//!
//! The storage layer consumes domain events instead of knowing how processes
//! are run. This keeps the same persistence contract usable from the CLI,
//! server, and embedded desktop engine.

use chrono::{DateTime, Utc};
use globset::{Glob, GlobSetBuilder};
use rivet_core::{
    BuildEvent, BuildId, BuildStatus, ExecutionPlan, LogStream, Pipeline, Project, ProjectId,
    ScheduleId, SourceSnapshot, StageId, StageStatus, StepId, StepStatus,
};
use rusqlite::{Connection, OptionalExtension, Transaction, params};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use thiserror::Error;
use uuid::Uuid;
use walkdir::WalkDir;

const MAX_AUDIT_PAGE_SIZE: usize = 1000;
const MAX_LOG_PAGE_SIZE: usize = 10_000;
const MAX_AUDIT_FIELD_BYTES: usize = 256;
const SESSION_DIGEST_BYTES: usize = 32;
const MAX_ANNOTATION_KIND_BYTES: usize = 64;
const MAX_ANNOTATION_MESSAGE_BYTES: usize = 4096;

#[derive(Clone)]
pub struct Storage {
    connection: Arc<Mutex<Connection>>,
    database_path: Option<Arc<PathBuf>>,
    artifact_root: Arc<std::path::PathBuf>,
    cache_root: Arc<std::path::PathBuf>,
}

/// Counts produced by a consistent local storage backup.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StorageBackupReport {
    pub database_bytes: u64,
    pub artifact_files: usize,
    pub artifact_bytes: u64,
}

fn apply_migration(
    connection: &Connection,
    version: i64,
    script: Option<&str>,
) -> Result<(), StorageError> {
    let applied: Option<i64> = connection
        .query_row(
            "SELECT version FROM schema_migrations WHERE version = ?1",
            params![version],
            |row| row.get(0),
        )
        .optional()?;
    if applied.is_none() {
        if let Some(script) = script {
            connection.execute_batch(script)?;
        }
        connection.execute(
            "INSERT INTO schema_migrations(version, applied_at) VALUES (?1, ?2)",
            params![version, Utc::now().to_rfc3339()],
        )?;
    }
    Ok(())
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
    #[error("annotation {field} is empty, too long, or contains control characters")]
    InvalidAnnotationField { field: &'static str },
    #[error("build {0} does not exist")]
    MissingBuild(BuildId),
    #[error("stage {0} does not exist")]
    MissingStage(StageId),
    #[error("step {0} does not exist")]
    MissingStep(StepId),
    #[error("invalid build transition from {from:?} to {to:?}")]
    InvalidBuildTransition { from: BuildStatus, to: BuildStatus },
    #[error("invalid source snapshot in database: {0}")]
    InvalidSource(String),
    #[error("artifact walk failed: {0}")]
    ArtifactWalk(String),
    #[error("artifact pattern {path:?} for {artifact:?} is invalid: {message}")]
    ArtifactPattern {
        artifact: String,
        path: String,
        message: String,
    },
    #[error("artifact {artifact:?} matched no files for pattern {path:?}")]
    ArtifactPatternNoMatch { artifact: String, path: String },
    #[error("artifact path escapes the workspace: {0}")]
    ArtifactPathOutsideWorkspace(std::path::PathBuf),
    #[error("artifact file is too large to persist: {0} bytes")]
    ArtifactTooLarge(u64),
    #[error("invalid artifact size in database: {0}")]
    InvalidArtifactSize(i64),
    #[error("invalid schedule enabled flag in database: {0}")]
    InvalidScheduleEnabled(i64),
    #[error("invalid schedule trigger in database: {0}")]
    InvalidScheduleTrigger(String),
    #[error("invalid schedule poll remote in database: {0}")]
    InvalidSchedulePollRemote(String),
    #[error("invalid schedule poll fetch flag in database: {0}")]
    InvalidSchedulePollFetch(i64),
    #[error("invalid schedule poll credential ID in database: {0}")]
    InvalidSchedulePollCredential(String),
    #[error("webhook delivery {0} does not exist")]
    MissingWebhookDelivery(String),
    #[error("repository poll event {0} does not exist")]
    MissingRepositoryPollEvent(String),
    #[error("pipeline trigger cannot reference the same project")]
    PipelineTriggerSelfReference,
    #[error("pipeline trigger {0} does not exist")]
    MissingPipelineTrigger(Uuid),
    #[error("pipeline trigger delivery does not exist")]
    MissingPipelineTriggerDelivery,
    #[error("invalid pipeline trigger enabled flag in database: {0}")]
    InvalidPipelineTriggerEnabled(i64),
    #[error("provider trigger {field} is empty, too long, or contains control characters")]
    InvalidProviderTriggerSelector { field: &'static str },
    #[error("invalid provider trigger enabled flag in database: {0}")]
    InvalidProviderTriggerEnabled(i64),
    #[error("provider trigger delivery does not exist")]
    MissingProviderTriggerDelivery,
    #[error("audit {field} is empty, too long, or contains control characters")]
    InvalidAuditField { field: &'static str },
    #[error("authentication session token digest is invalid")]
    InvalidSessionDigest,
    #[error("authentication session principal is invalid")]
    InvalidSessionPrincipal,
    #[error("log page is invalid")]
    InvalidLogPage,
    #[error("authentication session role is invalid")]
    InvalidSessionRole,
    #[error("invalid remote recovery attempt count in database: {0}")]
    InvalidRemoteRecoveryAttempts(i64),
    #[error("remote event build ID does not match the persisted build")]
    RemoteEventBuildMismatch,
    #[error("remote event identity is invalid")]
    InvalidRemoteEventIdentity,
    #[error("remote event sequence {sequence} was already recorded with a different payload")]
    RemoteEventSequenceConflict { sequence: u64 },
    #[error("backups are unavailable for an in-memory database")]
    BackupUnavailable,
    #[error("backup destination already exists: {0}")]
    BackupDestinationExists(PathBuf),
    #[error("backup destination is inside the artifact store: {0}")]
    BackupDestinationInsideArtifactStore(PathBuf),
    #[error("artifact store contains an unsupported path: {0}")]
    BackupUnsupportedArtifactPath(PathBuf),
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
    pub source: Option<SourceSnapshot>,
    pub parameters: BTreeMap<String, String>,
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ArtifactRecord {
    pub id: Uuid,
    pub build_id: BuildId,
    pub name: String,
    pub relative_path: String,
    pub size_bytes: u64,
    pub checksum: String,
    pub mime_type: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AnnotationRecord {
    pub id: Uuid,
    pub build_id: BuildId,
    pub stage_id: Option<StageId>,
    pub kind: String,
    pub message: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ArtifactPruneResult {
    pub removed_entries: usize,
    pub removed_bytes: u64,
    pub remaining_bytes: u64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ScheduleTrigger {
    Build,
    RepositoryPoll,
}

impl Default for ScheduleTrigger {
    fn default() -> Self {
        Self::Build
    }
}

impl ScheduleTrigger {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Build => "build",
            Self::RepositoryPoll => "repository_poll",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SchedulePollConfig {
    pub remote: String,
    pub fetch: bool,
    pub credential_id: Option<String>,
}

impl Default for SchedulePollConfig {
    fn default() -> Self {
        Self {
            remote: "origin".to_owned(),
            fetch: false,
            credential_id: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ScheduleRecord {
    pub id: ScheduleId,
    pub project_id: ProjectId,
    pub name: String,
    pub expression: String,
    pub trigger: ScheduleTrigger,
    /// Repository polling configuration. Build schedules intentionally omit
    /// this field so the API cannot imply that every schedule mutates Git.
    pub poll: Option<SchedulePollConfig>,
    pub enabled: bool,
    pub next_run_at: DateTime<Utc>,
    pub last_run_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WebhookDeliveryRecord {
    pub event_id: String,
    pub project_id: ProjectId,
    pub received_at: DateTime<Utc>,
    pub build_id: Option<BuildId>,
    pub build_number: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RepositoryPollEventRecord {
    pub event_id: String,
    pub project_id: ProjectId,
    pub revision: String,
    pub checked_at: DateTime<Utc>,
    pub build_id: Option<BuildId>,
    pub build_number: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PipelineTriggerRecord {
    pub id: Uuid,
    pub upstream_project_id: ProjectId,
    pub downstream_project_id: ProjectId,
    pub enabled: bool,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProviderTriggerRecord {
    pub id: Uuid,
    pub downstream_project_id: ProjectId,
    pub provider: String,
    pub source_repository: String,
    /// `None` matches any supported pipeline/workflow name for the provider.
    pub source_pipeline: Option<String>,
    pub enabled: bool,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AuditEventRecord {
    pub sequence: i64,
    pub timestamp: DateTime<Utc>,
    pub actor_id: Option<String>,
    pub action: String,
    pub resource: String,
    pub outcome: String,
}

/// Persisted session metadata. The raw session token is never stored; callers
/// pass its SHA-256 digest when creating, looking up, or revoking a session.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AuthSessionRecord {
    pub id: Uuid,
    pub token_sha256: String,
    pub principal_id: String,
    pub role: String,
    pub projects: Vec<String>,
    pub created_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub revoked_at: Option<DateTime<Utc>>,
}

/// Durable metadata needed to re-dispatch a remote build after the server
/// process is recreated. Parameter values are redacted according to the
/// pipeline before they reach this record; a build containing secret
/// parameters is therefore never resumed from this table.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RemoteAttemptRecord {
    pub build_id: BuildId,
    pub attempt_id: Uuid,
    pub project_id: ProjectId,
    pub agent_id: Uuid,
    pub requirements_json: String,
    pub plan: ExecutionPlan,
    pub pipeline: Pipeline,
    pub parameters: BTreeMap<String, String>,
    pub created_at: DateTime<Utc>,
    pub recovery_attempts: usize,
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
        let artifact_root = if path == Path::new(":memory:") {
            std::env::temp_dir().join(format!("rivet-artifacts-{}", Uuid::new_v4()))
        } else {
            path.parent()
                .filter(|parent| !parent.as_os_str().is_empty())
                .unwrap_or_else(|| Path::new("."))
                .join("artifacts")
        };
        let cache_root = if path == Path::new(":memory:") {
            std::env::temp_dir().join(format!("rivet-cache-{}", Uuid::new_v4()))
        } else {
            path.parent()
                .filter(|parent| !parent.as_os_str().is_empty())
                .unwrap_or_else(|| Path::new("."))
                .join("cache")
        };
        connection.execute_batch(include_str!("../migrations/001_initial.sql"))?;
        apply_migration(&connection, 1, None)?;
        apply_migration(
            &connection,
            2,
            Some(include_str!("../migrations/002_build_source.sql")),
        )?;
        apply_migration(
            &connection,
            3,
            Some(include_str!("../migrations/003_event_log.sql")),
        )?;
        apply_migration(
            &connection,
            4,
            Some(include_str!("../migrations/004_build_parameters.sql")),
        )?;
        apply_migration(
            &connection,
            5,
            Some(include_str!("../migrations/005_artifacts.sql")),
        )?;
        apply_migration(
            &connection,
            6,
            Some(include_str!("../migrations/006_schedules.sql")),
        )?;
        apply_migration(
            &connection,
            7,
            Some(include_str!("../migrations/007_webhook_deliveries.sql")),
        )?;
        apply_migration(
            &connection,
            8,
            Some(include_str!("../migrations/008_audit_events.sql")),
        )?;
        apply_migration(
            &connection,
            9,
            Some(include_str!("../migrations/009_auth_sessions.sql")),
        )?;
        apply_migration(
            &connection,
            10,
            Some(include_str!("../migrations/010_event_idempotency.sql")),
        )?;
        apply_migration(
            &connection,
            11,
            Some(include_str!("../migrations/011_remote_attempts.sql")),
        )?;
        apply_migration(
            &connection,
            12,
            Some(include_str!("../migrations/012_build_annotations.sql")),
        )?;
        apply_migration(
            &connection,
            13,
            Some(include_str!(
                "../migrations/013_remote_attempt_recovery.sql"
            )),
        )?;
        apply_migration(
            &connection,
            14,
            Some(include_str!(
                "../migrations/014_remote_attempt_identity.sql"
            )),
        )?;
        apply_migration(
            &connection,
            15,
            Some(include_str!(
                "../migrations/015_remote_event_deliveries.sql"
            )),
        )?;
        apply_migration(
            &connection,
            16,
            Some(include_str!("../migrations/016_repository_poll_events.sql")),
        )?;
        apply_migration(
            &connection,
            17,
            Some(include_str!("../migrations/017_schedule_triggers.sql")),
        )?;
        apply_migration(
            &connection,
            18,
            Some(include_str!("../migrations/018_schedule_poll_config.sql")),
        )?;
        apply_migration(
            &connection,
            19,
            Some(include_str!("../migrations/019_pipeline_triggers.sql")),
        )?;
        apply_migration(
            &connection,
            20,
            Some(include_str!("../migrations/020_provider_triggers.sql")),
        )?;
        apply_migration(
            &connection,
            21,
            Some(include_str!("../migrations/021_artifact_mime.sql")),
        )?;
        backfill_event_hashes(&connection)?;
        Ok(Self {
            connection: Arc::new(Mutex::new(connection)),
            database_path: (path != Path::new(":memory:")).then(|| Arc::new(path.to_path_buf())),
            artifact_root: Arc::new(artifact_root),
            cache_root: Arc::new(cache_root),
        })
    }

    pub fn open_in_memory() -> Result<Self, StorageError> {
        Self::open(":memory:")
    }

    pub fn cache_root(&self) -> std::path::PathBuf {
        self.cache_root.as_ref().clone()
    }

    pub fn artifact_root(&self) -> PathBuf {
        self.artifact_root.as_ref().clone()
    }

    /// Create a consistent, portable snapshot of the database and stored
    /// artifacts. The destination must not already exist. Derived cache data
    /// and external credential vaults are intentionally not included.
    pub fn backup_to(&self, destination: &Path) -> Result<StorageBackupReport, StorageError> {
        if self.database_path.is_none() {
            return Err(StorageError::BackupUnavailable);
        }
        if destination.exists() {
            return Err(StorageError::BackupDestinationExists(
                destination.to_path_buf(),
            ));
        }

        let artifact_root = fs::canonicalize(self.artifact_root.as_ref()).ok();
        if let Some(artifact_root) = artifact_root.as_ref() {
            let parent = destination.parent().unwrap_or_else(|| Path::new("."));
            fs::create_dir_all(parent)?;
            let destination_name = destination
                .file_name()
                .ok_or_else(|| StorageError::BackupDestinationExists(destination.to_path_buf()))?;
            let canonical_destination = fs::canonicalize(parent)?.join(destination_name);
            if canonical_destination.starts_with(artifact_root) {
                return Err(StorageError::BackupDestinationInsideArtifactStore(
                    destination.to_path_buf(),
                ));
            }
        } else if let Some(parent) = destination.parent() {
            fs::create_dir_all(parent)?;
        }

        fs::create_dir(destination)?;
        let result = (|| {
            let database_destination = destination.join("rivet.db");
            let database_destination = database_destination.to_string_lossy().into_owned();
            let connection = self.connection.lock().map_err(|_| StorageError::Poisoned)?;
            connection.execute("VACUUM INTO ?1", [&database_destination])?;
            drop(connection);

            let database_bytes = fs::metadata(destination.join("rivet.db"))?.len();
            let mut artifact_files = 0;
            let mut artifact_bytes = 0_u64;
            if let Some(artifact_root) = artifact_root {
                let destination_root = destination.join("artifacts");
                copy_artifact_tree(
                    &artifact_root,
                    &destination_root,
                    &mut artifact_files,
                    &mut artifact_bytes,
                )?;
            }
            Ok(StorageBackupReport {
                database_bytes,
                artifact_files,
                artifact_bytes,
            })
        })();

        if result.is_err() {
            let _ = fs::remove_dir_all(destination);
        }
        result
    }

    /// Run a read-only probe used by deployment readiness checks.
    pub fn health_check(&self) -> Result<(), StorageError> {
        let connection = self.connection.lock().map_err(|_| StorageError::Poisoned)?;
        connection.query_row("SELECT 1", [], |_| Ok(()))?;
        Ok(())
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

    pub fn get_project_by_id(
        &self,
        project_id: ProjectId,
    ) -> Result<Option<Project>, StorageError> {
        let connection = self.connection.lock().map_err(|_| StorageError::Poisoned)?;
        let row = connection
            .query_row(
                "SELECT id, name, repository_path, pipeline_path, created_at
                 FROM projects WHERE id = ?1",
                params![project_id.to_string()],
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

    pub fn create_schedule(
        &self,
        project_id: ProjectId,
        name: impl Into<String>,
        expression: impl Into<String>,
        enabled: bool,
        next_run_at: DateTime<Utc>,
    ) -> Result<ScheduleRecord, StorageError> {
        self.create_schedule_with_trigger(
            project_id,
            name,
            expression,
            ScheduleTrigger::Build,
            enabled,
            next_run_at,
        )
    }

    pub fn create_schedule_with_trigger(
        &self,
        project_id: ProjectId,
        name: impl Into<String>,
        expression: impl Into<String>,
        trigger: ScheduleTrigger,
        enabled: bool,
        next_run_at: DateTime<Utc>,
    ) -> Result<ScheduleRecord, StorageError> {
        self.create_schedule_with_trigger_and_poll(
            project_id,
            name,
            expression,
            trigger,
            None,
            enabled,
            next_run_at,
        )
    }

    pub fn create_schedule_with_trigger_and_poll(
        &self,
        project_id: ProjectId,
        name: impl Into<String>,
        expression: impl Into<String>,
        trigger: ScheduleTrigger,
        poll: Option<SchedulePollConfig>,
        enabled: bool,
        next_run_at: DateTime<Utc>,
    ) -> Result<ScheduleRecord, StorageError> {
        let poll = match trigger {
            ScheduleTrigger::Build => None,
            ScheduleTrigger::RepositoryPoll => {
                Some(normalize_schedule_poll_config(poll.unwrap_or_default())?)
            }
        };
        if let Some(poll) = poll.as_ref() {
            validate_schedule_poll_config(poll)?;
        }
        let schedule = ScheduleRecord {
            id: Uuid::new_v4(),
            project_id,
            name: name.into(),
            expression: expression.into(),
            trigger,
            poll: poll.clone(),
            enabled,
            next_run_at,
            last_run_at: None,
            created_at: Utc::now(),
        };
        let connection = self.connection.lock().map_err(|_| StorageError::Poisoned)?;
        connection.execute(
            "INSERT INTO schedules(
                id, project_id, name, expression, trigger, enabled, next_run_at, last_run_at,
                created_at, poll_remote, poll_fetch, poll_credential_id
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
            params![
                schedule.id.to_string(),
                schedule.project_id.to_string(),
                schedule.name,
                schedule.expression,
                schedule.trigger.as_str(),
                i64::from(schedule.enabled),
                schedule.next_run_at.to_rfc3339(),
                Option::<String>::None,
                schedule.created_at.to_rfc3339(),
                poll.as_ref()
                    .map(|poll| poll.remote.as_str())
                    .unwrap_or("origin"),
                i64::from(poll.as_ref().is_some_and(|poll| poll.fetch)),
                poll.as_ref().and_then(|poll| poll.credential_id.as_deref()),
            ],
        )?;
        Ok(schedule)
    }

    pub fn list_schedules(
        &self,
        project_id: ProjectId,
    ) -> Result<Vec<ScheduleRecord>, StorageError> {
        let connection = self.connection.lock().map_err(|_| StorageError::Poisoned)?;
        let mut statement = connection.prepare(
            "SELECT id, project_id, name, expression, trigger, enabled, next_run_at, last_run_at,
                    created_at, poll_remote, poll_fetch, poll_credential_id
             FROM schedules WHERE project_id = ?1 ORDER BY name ASC",
        )?;
        let rows = statement.query_map(params![project_id.to_string()], raw_schedule)?;
        rows.map(|row| row.map_err(StorageError::from).and_then(parse_schedule))
            .collect()
    }

    pub fn get_schedule(
        &self,
        project_id: ProjectId,
        schedule_id: ScheduleId,
    ) -> Result<Option<ScheduleRecord>, StorageError> {
        let connection = self.connection.lock().map_err(|_| StorageError::Poisoned)?;
        let row = connection
            .query_row(
                "SELECT id, project_id, name, expression, trigger, enabled, next_run_at, last_run_at,
                        created_at, poll_remote, poll_fetch, poll_credential_id
                 FROM schedules WHERE project_id = ?1 AND id = ?2",
                params![project_id.to_string(), schedule_id.to_string()],
                raw_schedule,
            )
            .optional()?;
        row.map(parse_schedule).transpose()
    }

    pub fn due_schedules(&self, now: DateTime<Utc>) -> Result<Vec<ScheduleRecord>, StorageError> {
        let connection = self.connection.lock().map_err(|_| StorageError::Poisoned)?;
        let mut statement = connection.prepare(
            "SELECT id, project_id, name, expression, trigger, enabled, next_run_at, last_run_at,
                    created_at, poll_remote, poll_fetch, poll_credential_id
             FROM schedules
             WHERE enabled = 1 AND next_run_at <= ?1
             ORDER BY next_run_at ASC, created_at ASC, id ASC",
        )?;
        let rows = statement.query_map(params![now.to_rfc3339()], raw_schedule)?;
        rows.map(|row| row.map_err(StorageError::from).and_then(parse_schedule))
            .collect()
    }

    /// Atomically claims one due occurrence. The expected timestamp is a
    /// compare-and-swap guard so two dispatcher ticks cannot enqueue it twice.
    pub fn claim_schedule(
        &self,
        schedule_id: ScheduleId,
        expected_next_run_at: DateTime<Utc>,
        fired_at: DateTime<Utc>,
        next_run_at: DateTime<Utc>,
    ) -> Result<bool, StorageError> {
        let connection = self.connection.lock().map_err(|_| StorageError::Poisoned)?;
        let changed = connection.execute(
            "UPDATE schedules
             SET last_run_at = ?1, next_run_at = ?2
             WHERE id = ?3 AND enabled = 1 AND next_run_at = ?4",
            params![
                fired_at.to_rfc3339(),
                next_run_at.to_rfc3339(),
                schedule_id.to_string(),
                expected_next_run_at.to_rfc3339(),
            ],
        )?;
        Ok(changed == 1)
    }

    pub fn set_schedule_enabled(
        &self,
        project_id: ProjectId,
        schedule_id: ScheduleId,
        enabled: bool,
    ) -> Result<Option<ScheduleRecord>, StorageError> {
        let connection = self.connection.lock().map_err(|_| StorageError::Poisoned)?;
        let changed = connection.execute(
            "UPDATE schedules SET enabled = ?1 WHERE project_id = ?2 AND id = ?3",
            params![
                i64::from(enabled),
                project_id.to_string(),
                schedule_id.to_string()
            ],
        )?;
        if changed == 0 {
            return Ok(None);
        }
        let row = connection.query_row(
            "SELECT id, project_id, name, expression, trigger, enabled, next_run_at, last_run_at,
                    created_at, poll_remote, poll_fetch, poll_credential_id
             FROM schedules WHERE project_id = ?1 AND id = ?2",
            params![project_id.to_string(), schedule_id.to_string()],
            raw_schedule,
        )?;
        parse_schedule(row).map(Some)
    }

    pub fn delete_schedule(
        &self,
        project_id: ProjectId,
        schedule_id: ScheduleId,
    ) -> Result<bool, StorageError> {
        let connection = self.connection.lock().map_err(|_| StorageError::Poisoned)?;
        let changed = connection.execute(
            "DELETE FROM schedules WHERE project_id = ?1 AND id = ?2",
            params![project_id.to_string(), schedule_id.to_string()],
        )?;
        Ok(changed == 1)
    }

    /// Reserve a webhook event ID. The primary key makes this safe across
    /// concurrent deliveries and server processes sharing the database.
    pub fn claim_webhook_delivery(
        &self,
        event_id: &str,
        project_id: ProjectId,
        received_at: DateTime<Utc>,
    ) -> Result<bool, StorageError> {
        let connection = self.connection.lock().map_err(|_| StorageError::Poisoned)?;
        let changed = connection.execute(
            "INSERT OR IGNORE INTO webhook_deliveries(event_id, project_id, received_at)
             VALUES (?1, ?2, ?3)",
            params![event_id, project_id.to_string(), received_at.to_rfc3339()],
        )?;
        Ok(changed == 1)
    }

    pub fn webhook_delivery(
        &self,
        event_id: &str,
    ) -> Result<Option<WebhookDeliveryRecord>, StorageError> {
        let connection = self.connection.lock().map_err(|_| StorageError::Poisoned)?;
        let row = connection
            .query_row(
                "SELECT event_id, project_id, received_at, build_id, build_number
                 FROM webhook_deliveries WHERE event_id = ?1",
                params![event_id],
                raw_webhook_delivery,
            )
            .optional()?;
        row.map(parse_webhook_delivery).transpose()
    }

    pub fn complete_webhook_delivery(
        &self,
        event_id: &str,
        build_id: BuildId,
        build_number: i64,
    ) -> Result<(), StorageError> {
        let connection = self.connection.lock().map_err(|_| StorageError::Poisoned)?;
        let changed = connection.execute(
            "UPDATE webhook_deliveries
             SET build_id = ?1, build_number = ?2
             WHERE event_id = ?3",
            params![build_id.to_string(), build_number, event_id],
        )?;
        if changed == 0 {
            return Err(StorageError::MissingWebhookDelivery(event_id.to_owned()));
        }
        Ok(())
    }

    pub fn release_webhook_delivery(&self, event_id: &str) -> Result<(), StorageError> {
        let connection = self.connection.lock().map_err(|_| StorageError::Poisoned)?;
        connection.execute(
            "DELETE FROM webhook_deliveries WHERE event_id = ?1 AND build_id IS NULL",
            params![event_id],
        )?;
        Ok(())
    }

    /// Reserve one repository revision transition. The caller derives the
    /// event ID from the project, observed revision, and previous baseline so
    /// concurrent pollers cannot queue the same transition twice.
    pub fn claim_repository_poll_event(
        &self,
        event_id: &str,
        project_id: ProjectId,
        revision: &str,
        checked_at: DateTime<Utc>,
    ) -> Result<bool, StorageError> {
        let connection = self.connection.lock().map_err(|_| StorageError::Poisoned)?;
        let changed = connection.execute(
            "INSERT OR IGNORE INTO repository_poll_events(
                event_id, project_id, revision, checked_at
             ) VALUES (?1, ?2, ?3, ?4)",
            params![
                event_id,
                project_id.to_string(),
                revision,
                checked_at.to_rfc3339()
            ],
        )?;
        Ok(changed == 1)
    }

    pub fn repository_poll_event(
        &self,
        event_id: &str,
    ) -> Result<Option<RepositoryPollEventRecord>, StorageError> {
        let connection = self.connection.lock().map_err(|_| StorageError::Poisoned)?;
        let row = connection
            .query_row(
                "SELECT event_id, project_id, revision, checked_at, build_id, build_number
                 FROM repository_poll_events WHERE event_id = ?1",
                params![event_id],
                raw_repository_poll_event,
            )
            .optional()?;
        row.map(parse_repository_poll_event).transpose()
    }

    pub fn complete_repository_poll_event(
        &self,
        event_id: &str,
        build_id: BuildId,
        build_number: i64,
    ) -> Result<(), StorageError> {
        let connection = self.connection.lock().map_err(|_| StorageError::Poisoned)?;
        let changed = connection.execute(
            "UPDATE repository_poll_events
             SET build_id = ?1, build_number = ?2
             WHERE event_id = ?3",
            params![build_id.to_string(), build_number, event_id],
        )?;
        if changed == 0 {
            return Err(StorageError::MissingRepositoryPollEvent(
                event_id.to_owned(),
            ));
        }
        Ok(())
    }

    pub fn release_repository_poll_event(&self, event_id: &str) -> Result<(), StorageError> {
        let connection = self.connection.lock().map_err(|_| StorageError::Poisoned)?;
        connection.execute(
            "DELETE FROM repository_poll_events
             WHERE event_id = ?1 AND build_id IS NULL",
            params![event_id],
        )?;
        Ok(())
    }

    pub fn create_pipeline_trigger(
        &self,
        upstream_project_id: ProjectId,
        downstream_project_id: ProjectId,
    ) -> Result<Option<PipelineTriggerRecord>, StorageError> {
        if upstream_project_id == downstream_project_id {
            return Err(StorageError::PipelineTriggerSelfReference);
        }
        let trigger = PipelineTriggerRecord {
            id: Uuid::new_v4(),
            upstream_project_id,
            downstream_project_id,
            enabled: true,
            created_at: Utc::now(),
        };
        let connection = self.connection.lock().map_err(|_| StorageError::Poisoned)?;
        let changed = connection.execute(
            "INSERT OR IGNORE INTO pipeline_triggers(
                id, upstream_project_id, downstream_project_id, enabled, created_at
             ) VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                trigger.id.to_string(),
                trigger.upstream_project_id.to_string(),
                trigger.downstream_project_id.to_string(),
                1_i64,
                trigger.created_at.to_rfc3339(),
            ],
        )?;
        Ok((changed == 1).then_some(trigger))
    }

    pub fn list_pipeline_triggers(&self) -> Result<Vec<PipelineTriggerRecord>, StorageError> {
        let connection = self.connection.lock().map_err(|_| StorageError::Poisoned)?;
        let mut statement = connection.prepare(
            "SELECT id, upstream_project_id, downstream_project_id, enabled, created_at
             FROM pipeline_triggers ORDER BY created_at ASC, id ASC",
        )?;
        let rows = statement.query_map([], raw_pipeline_trigger)?;
        rows.map(|row| {
            row.map_err(StorageError::from)
                .and_then(parse_pipeline_trigger)
        })
        .collect()
    }

    pub fn list_pipeline_triggers_from(
        &self,
        upstream_project_id: ProjectId,
    ) -> Result<Vec<PipelineTriggerRecord>, StorageError> {
        Ok(self
            .list_pipeline_triggers()?
            .into_iter()
            .filter(|trigger| trigger.enabled && trigger.upstream_project_id == upstream_project_id)
            .collect())
    }

    pub fn list_pipeline_triggers_to(
        &self,
        downstream_project_id: ProjectId,
    ) -> Result<Vec<PipelineTriggerRecord>, StorageError> {
        Ok(self
            .list_pipeline_triggers()?
            .into_iter()
            .filter(|trigger| trigger.downstream_project_id == downstream_project_id)
            .collect())
    }

    /// Return whether an enabled trigger path already reaches `target`.
    /// Callers can use this to reject a new edge that would create a cycle.
    pub fn pipeline_trigger_reaches(
        &self,
        start: ProjectId,
        target: ProjectId,
    ) -> Result<bool, StorageError> {
        if start == target {
            return Ok(true);
        }
        let triggers = self.list_pipeline_triggers()?;
        let mut visited = BTreeSet::new();
        let mut pending = vec![start];
        while let Some(project_id) = pending.pop() {
            if !visited.insert(project_id) {
                continue;
            }
            for trigger in triggers
                .iter()
                .filter(|trigger| trigger.enabled && trigger.upstream_project_id == project_id)
            {
                if trigger.downstream_project_id == target {
                    return Ok(true);
                }
                pending.push(trigger.downstream_project_id);
            }
        }
        Ok(false)
    }

    pub fn delete_pipeline_trigger(
        &self,
        downstream_project_id: ProjectId,
        trigger_id: Uuid,
    ) -> Result<bool, StorageError> {
        let connection = self.connection.lock().map_err(|_| StorageError::Poisoned)?;
        Ok(connection.execute(
            "DELETE FROM pipeline_triggers WHERE id = ?1 AND downstream_project_id = ?2",
            params![trigger_id.to_string(), downstream_project_id.to_string()],
        )? == 1)
    }

    /// Claim one upstream build for one trigger. The primary key makes this
    /// idempotent across duplicate build-finished events and server restarts.
    pub fn claim_pipeline_trigger_delivery(
        &self,
        trigger_id: Uuid,
        upstream_build_id: BuildId,
        triggered_at: DateTime<Utc>,
    ) -> Result<bool, StorageError> {
        let connection = self.connection.lock().map_err(|_| StorageError::Poisoned)?;
        let changed = connection.execute(
            "INSERT OR IGNORE INTO pipeline_trigger_deliveries(
                trigger_id, upstream_build_id, downstream_build_id, triggered_at
             )
             SELECT ?1, ?2, NULL, ?3
             WHERE EXISTS (
                 SELECT 1 FROM pipeline_triggers
                 WHERE id = ?1 AND enabled = 1
                   AND upstream_project_id = (
                       SELECT project_id FROM builds WHERE id = ?2
                   )
             )",
            params![
                trigger_id.to_string(),
                upstream_build_id.to_string(),
                triggered_at.to_rfc3339()
            ],
        )?;
        Ok(changed == 1)
    }

    pub fn complete_pipeline_trigger_delivery(
        &self,
        trigger_id: Uuid,
        upstream_build_id: BuildId,
        downstream_build_id: BuildId,
    ) -> Result<(), StorageError> {
        let connection = self.connection.lock().map_err(|_| StorageError::Poisoned)?;
        let changed = connection.execute(
            "UPDATE pipeline_trigger_deliveries
             SET downstream_build_id = ?1
             WHERE trigger_id = ?2 AND upstream_build_id = ?3
               AND downstream_build_id IS NULL
               AND EXISTS (
                   SELECT 1
                   FROM pipeline_triggers t
                   JOIN builds b ON b.id = ?1
                   WHERE t.id = ?2
                     AND t.downstream_project_id = b.project_id
               )",
            params![
                downstream_build_id.to_string(),
                trigger_id.to_string(),
                upstream_build_id.to_string()
            ],
        )?;
        if changed == 0 {
            return Err(StorageError::MissingPipelineTriggerDelivery);
        }
        Ok(())
    }

    pub fn release_pipeline_trigger_delivery(
        &self,
        trigger_id: Uuid,
        upstream_build_id: BuildId,
    ) -> Result<(), StorageError> {
        let connection = self.connection.lock().map_err(|_| StorageError::Poisoned)?;
        connection.execute(
            "DELETE FROM pipeline_trigger_deliveries
             WHERE trigger_id = ?1 AND upstream_build_id = ?2
               AND downstream_build_id IS NULL",
            params![trigger_id.to_string(), upstream_build_id.to_string()],
        )?;
        Ok(())
    }

    /// Find passed builds that became eligible after a trigger was created but
    /// have not yet been dispatched. This makes a restart safe without
    /// replaying builds that predate the trigger relation.
    pub fn pending_pipeline_trigger_deliveries(
        &self,
    ) -> Result<Vec<(PipelineTriggerRecord, BuildId)>, StorageError> {
        let connection = self.connection.lock().map_err(|_| StorageError::Poisoned)?;
        let mut statement = connection.prepare(
            "SELECT t.id, t.upstream_project_id, t.downstream_project_id, t.enabled,
                    t.created_at, b.id
             FROM pipeline_triggers t
             JOIN builds b ON b.project_id = t.upstream_project_id
             LEFT JOIN pipeline_trigger_deliveries d
               ON d.trigger_id = t.id AND d.upstream_build_id = b.id
             WHERE t.enabled = 1 AND b.status = 'passed'
               AND b.finished_at IS NOT NULL
               AND b.finished_at >= t.created_at
               AND d.trigger_id IS NULL
             ORDER BY b.finished_at ASC, b.id ASC",
        )?;
        let rows = statement.query_map([], |row| {
            let raw: RawPipelineTrigger = (
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
            );
            let build_id: String = row.get(5)?;
            Ok((raw, build_id))
        })?;
        rows.map(|row| {
            let (raw, build_id) = row?;
            Ok((parse_pipeline_trigger(raw)?, parse_uuid(&build_id)?))
        })
        .collect()
    }

    pub fn create_provider_trigger(
        &self,
        downstream_project_id: ProjectId,
        provider: &str,
        source_repository: &str,
        source_pipeline: Option<&str>,
    ) -> Result<Option<ProviderTriggerRecord>, StorageError> {
        let provider = normalize_provider_trigger_selector(provider, "provider")?;
        let source_repository =
            normalize_provider_trigger_selector(source_repository, "source_repository")?;
        let source_pipeline = source_pipeline
            .map(|value| normalize_provider_trigger_selector(value, "source_pipeline"))
            .transpose()?
            .unwrap_or_default();
        let trigger = ProviderTriggerRecord {
            id: Uuid::new_v4(),
            downstream_project_id,
            provider,
            source_repository,
            source_pipeline: (!source_pipeline.is_empty()).then_some(source_pipeline.clone()),
            enabled: true,
            created_at: Utc::now(),
        };
        let connection = self.connection.lock().map_err(|_| StorageError::Poisoned)?;
        let changed = connection.execute(
            "INSERT OR IGNORE INTO provider_triggers(
                id, downstream_project_id, provider, source_repository,
                source_pipeline, enabled, created_at
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                trigger.id.to_string(),
                trigger.downstream_project_id.to_string(),
                &trigger.provider,
                &trigger.source_repository,
                trigger.source_pipeline.as_deref().unwrap_or_default(),
                1_i64,
                trigger.created_at.to_rfc3339(),
            ],
        )?;
        Ok((changed == 1).then_some(trigger))
    }

    pub fn list_provider_triggers_to(
        &self,
        downstream_project_id: ProjectId,
    ) -> Result<Vec<ProviderTriggerRecord>, StorageError> {
        let connection = self.connection.lock().map_err(|_| StorageError::Poisoned)?;
        let mut statement = connection.prepare(
            "SELECT id, downstream_project_id, provider, source_repository,
                    source_pipeline, enabled, created_at
             FROM provider_triggers
             WHERE downstream_project_id = ?1
             ORDER BY created_at ASC, id ASC",
        )?;
        let rows = statement.query_map(
            params![downstream_project_id.to_string()],
            raw_provider_trigger,
        )?;
        rows.map(|row| {
            row.map_err(StorageError::from)
                .and_then(parse_provider_trigger)
        })
        .collect()
    }

    pub fn matching_provider_triggers(
        &self,
        downstream_project_id: ProjectId,
        provider: &str,
        source_repository: &str,
        source_pipeline: Option<&str>,
    ) -> Result<Vec<ProviderTriggerRecord>, StorageError> {
        let provider = normalize_provider_trigger_selector(provider, "provider")?;
        let source_repository =
            normalize_provider_trigger_selector(source_repository, "source_repository")?;
        let source_pipeline = source_pipeline
            .map(|value| normalize_provider_trigger_selector(value, "source_pipeline"))
            .transpose()?
            .unwrap_or_default();
        let connection = self.connection.lock().map_err(|_| StorageError::Poisoned)?;
        let mut statement = connection.prepare(
            "SELECT id, downstream_project_id, provider, source_repository,
                    source_pipeline, enabled, created_at
             FROM provider_triggers
             WHERE downstream_project_id = ?1
               AND provider = ?2
               AND source_repository = ?3
               AND enabled = 1
               AND (source_pipeline = '' OR source_pipeline = ?4)
             ORDER BY created_at ASC, id ASC",
        )?;
        let rows = statement.query_map(
            params![
                downstream_project_id.to_string(),
                provider,
                source_repository,
                source_pipeline,
            ],
            raw_provider_trigger,
        )?;
        rows.map(|row| {
            row.map_err(StorageError::from)
                .and_then(parse_provider_trigger)
        })
        .collect()
    }

    pub fn delete_provider_trigger(
        &self,
        downstream_project_id: ProjectId,
        trigger_id: Uuid,
    ) -> Result<bool, StorageError> {
        let connection = self.connection.lock().map_err(|_| StorageError::Poisoned)?;
        Ok(connection.execute(
            "DELETE FROM provider_triggers
             WHERE id = ?1 AND downstream_project_id = ?2",
            params![trigger_id.to_string(), downstream_project_id.to_string()],
        )? == 1)
    }

    /// Claim one provider event for one configured mapping. The composite
    /// primary key makes retries and restart reconciliation idempotent.
    pub fn claim_provider_trigger_delivery(
        &self,
        trigger_id: Uuid,
        event_id: &str,
        triggered_at: DateTime<Utc>,
    ) -> Result<bool, StorageError> {
        let connection = self.connection.lock().map_err(|_| StorageError::Poisoned)?;
        let changed = connection.execute(
            "INSERT OR IGNORE INTO provider_trigger_deliveries(
                trigger_id, event_id, downstream_build_id, triggered_at
             )
             SELECT ?1, ?2, NULL, ?3
             WHERE EXISTS (
                 SELECT 1 FROM provider_triggers
                 WHERE id = ?1 AND enabled = 1
             )",
            params![trigger_id.to_string(), event_id, triggered_at.to_rfc3339()],
        )?;
        Ok(changed == 1)
    }

    pub fn complete_provider_trigger_delivery(
        &self,
        trigger_id: Uuid,
        event_id: &str,
        downstream_build_id: BuildId,
    ) -> Result<(), StorageError> {
        let connection = self.connection.lock().map_err(|_| StorageError::Poisoned)?;
        let changed = connection.execute(
            "UPDATE provider_trigger_deliveries
             SET downstream_build_id = ?1
             WHERE trigger_id = ?2 AND event_id = ?3
               AND downstream_build_id IS NULL
               AND EXISTS (
                   SELECT 1
                   FROM provider_triggers t
                   JOIN builds b ON b.id = ?1
                   WHERE t.id = ?2
                     AND t.downstream_project_id = b.project_id
               )",
            params![
                downstream_build_id.to_string(),
                trigger_id.to_string(),
                event_id,
            ],
        )?;
        if changed == 0 {
            return Err(StorageError::MissingProviderTriggerDelivery);
        }
        Ok(())
    }

    pub fn release_provider_trigger_delivery(
        &self,
        trigger_id: Uuid,
        event_id: &str,
    ) -> Result<(), StorageError> {
        let connection = self.connection.lock().map_err(|_| StorageError::Poisoned)?;
        connection.execute(
            "DELETE FROM provider_trigger_deliveries
             WHERE trigger_id = ?1 AND event_id = ?2
               AND downstream_build_id IS NULL",
            params![trigger_id.to_string(), event_id],
        )?;
        Ok(())
    }

    /// Append a bounded audit record. Authentication material and request
    /// bodies are intentionally not accepted by this API; callers provide a
    /// short actor, action, resource, and outcome only.
    pub fn append_audit_event(
        &self,
        timestamp: DateTime<Utc>,
        actor_id: Option<&str>,
        action: &str,
        resource: &str,
        outcome: &str,
    ) -> Result<i64, StorageError> {
        if let Some(actor_id) = actor_id {
            validate_audit_field(actor_id, "actor_id")?;
        }
        validate_audit_field(action, "action")?;
        validate_audit_field(resource, "resource")?;
        validate_audit_field(outcome, "outcome")?;
        let connection = self.connection.lock().map_err(|_| StorageError::Poisoned)?;
        connection.execute(
            "INSERT INTO audit_events(timestamp, actor_id, action, resource, outcome)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![timestamp.to_rfc3339(), actor_id, action, resource, outcome,],
        )?;
        Ok(connection.last_insert_rowid())
    }

    pub fn list_audit_events(&self, limit: usize) -> Result<Vec<AuditEventRecord>, StorageError> {
        let limit = limit.min(MAX_AUDIT_PAGE_SIZE);
        let connection = self.connection.lock().map_err(|_| StorageError::Poisoned)?;
        let mut statement = connection.prepare(
            "SELECT sequence, timestamp, actor_id, action, resource, outcome
             FROM audit_events ORDER BY sequence DESC LIMIT ?1",
        )?;
        let rows = statement.query_map(params![limit as i64], raw_audit_event)?;
        rows.map(|row| row.map_err(StorageError::from).and_then(parse_audit_event))
            .collect()
    }

    pub fn create_auth_session(
        &self,
        id: Uuid,
        token_sha256: &str,
        principal_id: &str,
        role: &str,
        projects: &[String],
        created_at: DateTime<Utc>,
        expires_at: DateTime<Utc>,
    ) -> Result<AuthSessionRecord, StorageError> {
        validate_session_digest(token_sha256)?;
        validate_session_field(principal_id, "principal")?;
        validate_session_field(role, "role")?;
        let projects_json = serde_json::to_string(projects)?;
        let connection = self.connection.lock().map_err(|_| StorageError::Poisoned)?;
        connection.execute(
            "INSERT INTO auth_sessions(
                id, token_sha256, principal_id, role, projects_json, created_at, expires_at
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                id.to_string(),
                token_sha256,
                principal_id,
                role,
                projects_json,
                created_at.to_rfc3339(),
                expires_at.to_rfc3339(),
            ],
        )?;
        Ok(AuthSessionRecord {
            id,
            token_sha256: token_sha256.to_owned(),
            principal_id: principal_id.to_owned(),
            role: role.to_owned(),
            projects: projects.to_vec(),
            created_at,
            expires_at,
            revoked_at: None,
        })
    }

    pub fn auth_session(
        &self,
        token_sha256: &str,
        now: DateTime<Utc>,
    ) -> Result<Option<AuthSessionRecord>, StorageError> {
        validate_session_digest(token_sha256)?;
        let connection = self.connection.lock().map_err(|_| StorageError::Poisoned)?;
        let raw = connection
            .query_row(
                "SELECT id, token_sha256, principal_id, role, projects_json,
                        created_at, expires_at, revoked_at
                 FROM auth_sessions
                 WHERE token_sha256 = ?1 AND revoked_at IS NULL AND expires_at > ?2",
                params![token_sha256, now.to_rfc3339()],
                raw_auth_session,
            )
            .optional()?;
        raw.map(parse_auth_session).transpose()
    }

    pub fn revoke_auth_session(
        &self,
        token_sha256: &str,
        revoked_at: DateTime<Utc>,
    ) -> Result<bool, StorageError> {
        validate_session_digest(token_sha256)?;
        let connection = self.connection.lock().map_err(|_| StorageError::Poisoned)?;
        let changed = connection.execute(
            "UPDATE auth_sessions SET revoked_at = ?1
             WHERE token_sha256 = ?2 AND revoked_at IS NULL",
            params![revoked_at.to_rfc3339(), token_sha256],
        )?;
        Ok(changed == 1)
    }

    pub fn prune_auth_sessions(&self, now: DateTime<Utc>) -> Result<usize, StorageError> {
        let connection = self.connection.lock().map_err(|_| StorageError::Poisoned)?;
        Ok(connection.execute(
            "DELETE FROM auth_sessions WHERE expires_at <= ?1 OR revoked_at IS NOT NULL",
            params![now.to_rfc3339()],
        )?)
    }

    pub fn create_build(
        &self,
        project: &Project,
        plan: &ExecutionPlan,
        pipeline: &Pipeline,
        source: Option<&SourceSnapshot>,
    ) -> Result<BuildRecord, StorageError> {
        self.create_build_with_parameters(project, plan, pipeline, source, &BTreeMap::new())
    }

    pub fn create_build_with_parameters(
        &self,
        project: &Project,
        plan: &ExecutionPlan,
        pipeline: &Pipeline,
        source: Option<&SourceSnapshot>,
        parameters: &BTreeMap<String, String>,
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
        let persisted_parameters = pipeline.redact_parameters(parameters);
        transaction.execute(
            "INSERT INTO builds(
                id, project_id, number, status, queued_at,
                source_provider, source_revision, source_reference,
                source_remote, source_dirty, parameters_json
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
            params![
                id.to_string(),
                project.id.to_string(),
                number,
                status_string(&BuildStatus::Pending)?,
                queued_at.to_rfc3339(),
                source.map(|snapshot| snapshot.provider.as_str()),
                source.map(|snapshot| snapshot.revision.as_str()),
                source.and_then(|snapshot| snapshot.reference.as_deref()),
                source.and_then(|snapshot| snapshot.remote.as_deref()),
                source.map(|snapshot| i64::from(snapshot.dirty)),
                serde_json::to_string(&persisted_parameters)?,
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
            source: source.cloned(),
            parameters: persisted_parameters,
        })
    }

    /// Record the non-secret payload required to re-dispatch a remote build.
    /// The build row must already exist; the foreign key makes a crashed
    /// cleanup unable to leave an orphaned assignment record.
    pub fn create_remote_attempt(
        &self,
        build_id: BuildId,
        project_id: ProjectId,
        agent_id: Uuid,
        requirements_json: &str,
        plan: &ExecutionPlan,
        pipeline: &Pipeline,
        parameters: &BTreeMap<String, String>,
    ) -> Result<RemoteAttemptRecord, StorageError> {
        let record = RemoteAttemptRecord {
            build_id,
            attempt_id: Uuid::new_v4(),
            project_id,
            agent_id,
            requirements_json: requirements_json.to_owned(),
            plan: plan.clone(),
            pipeline: pipeline.clone(),
            parameters: pipeline.redact_parameters(parameters),
            created_at: Utc::now(),
            recovery_attempts: 0,
        };
        let plan_json = serde_json::to_string(&record.plan)?;
        let pipeline_json = serde_json::to_string(&record.pipeline)?;
        let parameters_json = serde_json::to_string(&record.parameters)?;
        let connection = self.connection.lock().map_err(|_| StorageError::Poisoned)?;
        connection.execute(
            "INSERT INTO remote_attempts(
                build_id, attempt_id, project_id, agent_id, requirements_json,
                plan_json, pipeline_json, parameters_json, created_at
                , recovery_attempts
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
            params![
                record.build_id.to_string(),
                record.attempt_id.to_string(),
                record.project_id.to_string(),
                record.agent_id.to_string(),
                &record.requirements_json,
                plan_json,
                pipeline_json,
                parameters_json,
                record.created_at.to_rfc3339(),
                record.recovery_attempts as i64,
            ],
        )?;
        Ok(record)
    }

    pub fn list_remote_attempts(&self) -> Result<Vec<RemoteAttemptRecord>, StorageError> {
        let connection = self.connection.lock().map_err(|_| StorageError::Poisoned)?;
        let mut statement = connection.prepare(
            "SELECT build_id, attempt_id, project_id, agent_id,
                    requirements_json, plan_json, pipeline_json,
                    parameters_json, created_at, recovery_attempts
             FROM remote_attempts ORDER BY created_at ASC, build_id ASC",
        )?;
        let rows = statement.query_map([], raw_remote_attempt)?;
        rows.map(|row| {
            row.map_err(StorageError::from)
                .and_then(parse_remote_attempt)
        })
        .collect()
    }

    pub fn delete_remote_attempt(&self, build_id: BuildId) -> Result<bool, StorageError> {
        let connection = self.connection.lock().map_err(|_| StorageError::Poisoned)?;
        Ok(connection.execute(
            "DELETE FROM remote_attempts WHERE build_id = ?1",
            params![build_id.to_string()],
        )? == 1)
    }

    /// Persist the agent selected for the current remote attempt without
    /// consuming a recovery slot. This is used when the first post-restart
    /// dispatch has to choose a compatible replacement before execution starts.
    pub fn set_remote_attempt_agent(
        &self,
        build_id: BuildId,
        agent_id: Uuid,
    ) -> Result<bool, StorageError> {
        let connection = self.connection.lock().map_err(|_| StorageError::Poisoned)?;
        Ok(connection.execute(
            "UPDATE remote_attempts SET agent_id = ?1 WHERE build_id = ?2",
            params![agent_id.to_string(), build_id.to_string()],
        )? == 1)
    }

    /// Persist the agent selected for the next bounded recovery attempt.
    ///
    /// The update is conditional so two recovery dispatchers sharing the same
    /// database cannot both consume the same retry budget. The returned count
    /// is the durable number of replacements already committed.
    pub fn advance_remote_attempt(
        &self,
        build_id: BuildId,
        agent_id: Uuid,
        attempt_id: Uuid,
        max_recovery_attempts: usize,
    ) -> Result<Option<usize>, StorageError> {
        if attempt_id.is_nil() {
            return Err(StorageError::InvalidRemoteEventIdentity);
        }
        let max_recovery_attempts = i64::try_from(max_recovery_attempts)
            .map_err(|_| StorageError::InvalidRemoteRecoveryAttempts(i64::MAX))?;
        let connection = self.connection.lock().map_err(|_| StorageError::Poisoned)?;
        let changed = connection.execute(
            "UPDATE remote_attempts
             SET agent_id = ?1, attempt_id = ?2,
                 recovery_attempts = recovery_attempts + 1
             WHERE build_id = ?3 AND recovery_attempts < ?4",
            params![
                agent_id.to_string(),
                attempt_id.to_string(),
                build_id.to_string(),
                max_recovery_attempts
            ],
        )?;
        if changed == 0 {
            return Ok(None);
        }
        let attempts: i64 = connection.query_row(
            "SELECT recovery_attempts FROM remote_attempts WHERE build_id = ?1",
            params![build_id.to_string()],
            |row| row.get(0),
        )?;
        let attempts = usize::try_from(attempts)
            .map_err(|_| StorageError::InvalidRemoteRecoveryAttempts(attempts))?;
        Ok(Some(attempts))
    }

    pub fn apply_event(&self, event: &BuildEvent) -> Result<(), StorageError> {
        let mut connection = self.connection.lock().map_err(|_| StorageError::Poisoned)?;
        let transaction = connection.transaction()?;
        let event_json = serde_json::to_string(event)?;
        let digest = event_hash(&event_json);
        let inserted = transaction.execute(
            "INSERT OR IGNORE INTO build_events(build_id, timestamp, event_json, event_hash)
             VALUES (?1, ?2, ?3, ?4)",
            params![
                event_build_id(event).to_string(),
                event.timestamp().to_rfc3339(),
                event_json,
                digest,
            ],
        )?;
        if inserted == 0 {
            transaction.commit()?;
            return Ok(());
        }
        Self::project_event(&transaction, event)?;
        transaction.commit()?;
        Ok(())
    }

    /// Atomically record and project an event received from a remote agent.
    /// The durable identity is separate from the timestamp-bearing domain
    /// event, so retransmission after a server restart cannot duplicate logs
    /// or state transitions.
    pub fn apply_remote_event(
        &self,
        build_id: BuildId,
        attempt_id: Uuid,
        sequence: u64,
        event: &BuildEvent,
    ) -> Result<bool, StorageError> {
        if attempt_id.is_nil() || sequence == 0 {
            return Err(StorageError::InvalidRemoteEventIdentity);
        }
        if event_build_id(event) != build_id {
            return Err(StorageError::RemoteEventBuildMismatch);
        }
        let database_sequence =
            i64::try_from(sequence).map_err(|_| StorageError::InvalidRemoteEventIdentity)?;
        let mut connection = self.connection.lock().map_err(|_| StorageError::Poisoned)?;
        let transaction = connection.transaction()?;
        let event_json = serde_json::to_string(event)?;
        let digest = event_hash(&event_json);
        let existing_hash: Option<String> = transaction
            .query_row(
                "SELECT event_hash FROM remote_event_deliveries
                 WHERE build_id = ?1 AND attempt_id = ?2 AND sequence = ?3",
                params![
                    build_id.to_string(),
                    attempt_id.to_string(),
                    database_sequence
                ],
                |row| row.get(0),
            )
            .optional()?;
        if let Some(existing_hash) = existing_hash {
            if existing_hash != digest {
                return Err(StorageError::RemoteEventSequenceConflict { sequence });
            }
            transaction.commit()?;
            return Ok(false);
        }
        transaction.execute(
            "INSERT INTO remote_event_deliveries(
                build_id, attempt_id, sequence, event_hash, created_at
             ) VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                build_id.to_string(),
                attempt_id.to_string(),
                database_sequence,
                digest,
                Utc::now().to_rfc3339(),
            ],
        )?;
        let inserted = transaction.execute(
            "INSERT OR IGNORE INTO build_events(build_id, timestamp, event_json, event_hash)
             VALUES (?1, ?2, ?3, ?4)",
            params![
                build_id.to_string(),
                event.timestamp().to_rfc3339(),
                event_json,
                digest,
            ],
        )?;
        if inserted == 1 {
            Self::project_event(&transaction, event)?;
        }
        transaction.commit()?;
        Ok(inserted == 1)
    }

    fn project_event(
        transaction: &Transaction<'_>,
        event: &BuildEvent,
    ) -> Result<(), StorageError> {
        match event {
            BuildEvent::BuildQueued {
                build_id,
                timestamp,
                ..
            } => transition_build(transaction, *build_id, BuildStatus::Queued, *timestamp)?,
            BuildEvent::BuildStarted {
                build_id,
                timestamp,
            } => transition_build(transaction, *build_id, BuildStatus::Running, *timestamp)?,
            BuildEvent::BuildFinished {
                build_id,
                status,
                timestamp,
            } => transition_build(transaction, *build_id, status.clone(), *timestamp)?,
            BuildEvent::BuildCancelled {
                build_id,
                timestamp,
            } => transition_build(transaction, *build_id, BuildStatus::Cancelled, *timestamp)?,
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
        Ok(())
    }

    /// Reconcile work that was in flight when the owning process stopped.
    ///
    /// The event log remains the source of truth: unfinished steps and stages
    /// receive terminal cancellation events first, then the build receives a
    /// terminal event that is legal for its last persisted state. Running work
    /// is marked failed because no process supervisor survived the restart;
    /// pending and queued work is marked cancelled. Re-running this method is
    /// safe because terminal builds are ignored.
    pub fn recover_incomplete_builds(
        &self,
        timestamp: DateTime<Utc>,
    ) -> Result<Vec<BuildId>, StorageError> {
        self.recover_incomplete_builds_except(timestamp, &BTreeSet::new())
    }

    /// Reconcile incomplete work while preserving explicitly durable remote
    /// attempts for a server-level redispatcher.
    pub fn recover_incomplete_builds_except(
        &self,
        timestamp: DateTime<Utc>,
        preserved: &BTreeSet<BuildId>,
    ) -> Result<Vec<BuildId>, StorageError> {
        let builds = self.list_incomplete_builds()?;
        let mut recovered = Vec::new();
        for build in builds {
            if preserved.contains(&build.id) {
                continue;
            }
            let Some(details) = self.get_build_details(build.id)? else {
                continue;
            };
            for stage in &details.stages {
                for step in &stage.steps {
                    if !step.status.is_terminal() {
                        self.apply_event(&BuildEvent::StepFinished {
                            build_id: build.id,
                            stage_id: stage.stage.id,
                            step_id: step.id,
                            step_name: step.name.clone(),
                            status: StepStatus::Cancelled,
                            exit_code: None,
                            timestamp,
                        })?;
                    }
                }
                if !stage.stage.status.is_terminal() {
                    self.apply_event(&BuildEvent::StageFinished {
                        build_id: build.id,
                        stage_id: stage.stage.id,
                        stage_name: stage.stage.name.clone(),
                        status: StageStatus::Cancelled,
                        timestamp,
                    })?;
                }
            }
            let terminal = match build.status {
                BuildStatus::Pending | BuildStatus::Queued => BuildEvent::BuildCancelled {
                    build_id: build.id,
                    timestamp,
                },
                BuildStatus::Running => BuildEvent::BuildFinished {
                    build_id: build.id,
                    status: BuildStatus::Failed,
                    timestamp,
                },
                BuildStatus::Passed | BuildStatus::Failed | BuildStatus::Cancelled => continue,
            };
            self.apply_event(&terminal)?;
            recovered.push(build.id);
        }
        Ok(recovered)
    }

    pub fn list_builds(&self, project_id: ProjectId) -> Result<Vec<BuildRecord>, StorageError> {
        let connection = self.connection.lock().map_err(|_| StorageError::Poisoned)?;
        let mut statement = connection.prepare(
            "SELECT id, project_id, number, status, queued_at, started_at, finished_at,
                    source_provider, source_revision, source_reference, source_remote, source_dirty,
                    parameters_json
             FROM builds WHERE project_id = ?1 ORDER BY number DESC",
        )?;
        let rows = statement.query_map(params![project_id.to_string()], raw_build)?;
        rows.map(|row| row.map_err(StorageError::from).and_then(parse_build))
            .collect()
    }

    pub fn list_incomplete_builds(&self) -> Result<Vec<BuildRecord>, StorageError> {
        let connection = self.connection.lock().map_err(|_| StorageError::Poisoned)?;
        let mut statement = connection.prepare(
            "SELECT id, project_id, number, status, queued_at, started_at, finished_at,
                    source_provider, source_revision, source_reference, source_remote, source_dirty,
                    parameters_json
             FROM builds
             WHERE status IN ('pending', 'queued', 'running')
             ORDER BY queued_at ASC, number ASC",
        )?;
        let rows = statement.query_map([], raw_build)?;
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
                "SELECT id, project_id, number, status, queued_at, started_at, finished_at,
                        source_provider, source_revision, source_reference, source_remote, source_dirty,
                        parameters_json
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

    /// Read a bounded slice of build output after an inclusive sequence
    /// cursor. The API layer can use this without loading an unbounded log
    /// history into one response.
    pub fn logs_page(
        &self,
        build_id: BuildId,
        after_sequence: i64,
        limit: usize,
    ) -> Result<Vec<LogRecord>, StorageError> {
        if after_sequence < -1 || !(1..=MAX_LOG_PAGE_SIZE).contains(&limit) {
            return Err(StorageError::InvalidLogPage);
        }
        let connection = self.connection.lock().map_err(|_| StorageError::Poisoned)?;
        let mut statement = connection.prepare(
            "SELECT sequence, build_id, timestamp, stream, line
             FROM build_logs
             WHERE build_id = ?1 AND sequence > ?2
             ORDER BY sequence ASC LIMIT ?3",
        )?;
        let rows = statement.query_map(
            params![build_id.to_string(), after_sequence, limit as i64],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                ))
            },
        )?;
        rows.map(|row| row.map_err(StorageError::from).and_then(parse_log))
            .collect()
    }

    pub fn events(&self, build_id: BuildId) -> Result<Vec<BuildEvent>, StorageError> {
        let connection = self.connection.lock().map_err(|_| StorageError::Poisoned)?;
        let mut statement = connection.prepare(
            "SELECT event_json FROM build_events
             WHERE build_id = ?1 ORDER BY sequence ASC",
        )?;
        let rows =
            statement.query_map(params![build_id.to_string()], |row| row.get::<_, String>(0))?;
        rows.map(|row| {
            let event_json = row?;
            Ok(serde_json::from_str(&event_json)?)
        })
        .collect()
    }

    pub fn collect_artifacts(
        &self,
        build_id: BuildId,
        pipeline: &Pipeline,
        workspace: &Path,
    ) -> Result<Vec<ArtifactRecord>, StorageError> {
        let workspace = fs::canonicalize(workspace)?;
        if !workspace.is_dir() {
            return Err(StorageError::ArtifactPathOutsideWorkspace(workspace));
        }
        let existing = self.artifacts(build_id)?;
        let existing_keys = existing
            .iter()
            .map(|artifact| (artifact.name.clone(), artifact.relative_path.clone()))
            .collect::<BTreeSet<_>>();
        let mut collected = Vec::new();

        for specification in &pipeline.artifacts {
            let mut builder = GlobSetBuilder::new();
            for path in &specification.paths {
                let glob = Glob::new(path).map_err(|error| StorageError::ArtifactPattern {
                    artifact: specification.name.clone(),
                    path: path.clone(),
                    message: error.to_string(),
                })?;
                builder.add(glob);
            }
            let matcher = builder
                .build()
                .map_err(|error| StorageError::ArtifactPattern {
                    artifact: specification.name.clone(),
                    path: specification.paths.join(", "),
                    message: error.to_string(),
                })?;
            let mut matches = BTreeMap::new();
            for entry in WalkDir::new(&workspace).follow_links(false) {
                let entry = entry.map_err(|error| StorageError::ArtifactWalk(error.to_string()))?;
                if !entry.file_type().is_file() {
                    continue;
                }
                let relative = entry.path().strip_prefix(&workspace).map_err(|_| {
                    StorageError::ArtifactPathOutsideWorkspace(entry.path().to_path_buf())
                })?;
                let relative = relative
                    .to_string_lossy()
                    .replace(std::path::MAIN_SEPARATOR, "/");
                if matcher.is_match(&relative) {
                    matches.insert(relative, entry.path().to_path_buf());
                }
            }
            if matches.is_empty() && !specification.allow_empty {
                return Err(StorageError::ArtifactPatternNoMatch {
                    artifact: specification.name.clone(),
                    path: specification.paths.join(", "),
                });
            }

            for (relative_path, source_path) in matches {
                if existing_keys.contains(&(specification.name.clone(), relative_path.clone())) {
                    continue;
                }
                let artifact_id = Uuid::new_v4();
                let size_bytes = fs::metadata(&source_path)?.len();
                i64::try_from(size_bytes)
                    .map_err(|_| StorageError::ArtifactTooLarge(size_bytes))?;
                let checksum = sha256_file(&source_path)?;
                let mime_type = artifact_mime_type(&relative_path);
                let destination_dir = self.artifact_root.join(build_id.to_string());
                fs::create_dir_all(&destination_dir)?;
                fs::copy(&source_path, destination_dir.join(artifact_id.to_string()))?;
                collected.push(ArtifactRecord {
                    id: artifact_id,
                    build_id,
                    name: specification.name.clone(),
                    relative_path,
                    size_bytes,
                    checksum,
                    mime_type,
                    created_at: Utc::now(),
                });
            }
        }

        if !collected.is_empty() {
            let mut connection = self.connection.lock().map_err(|_| StorageError::Poisoned)?;
            let transaction = connection.transaction()?;
            for artifact in &collected {
                let size_bytes = i64::try_from(artifact.size_bytes)
                    .map_err(|_| StorageError::ArtifactTooLarge(artifact.size_bytes))?;
                transaction.execute(
                    "INSERT INTO build_artifacts(
                        id, build_id, name, relative_path, size_bytes, checksum, mime_type, created_at
                     ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                    params![
                        artifact.id.to_string(),
                        artifact.build_id.to_string(),
                        artifact.name,
                        artifact.relative_path,
                        size_bytes,
                        artifact.checksum,
                        artifact.mime_type,
                        artifact.created_at.to_rfc3339(),
                    ],
                )?;
            }
            transaction.commit()?;
        }

        self.artifacts(build_id)
    }

    pub fn artifacts(&self, build_id: BuildId) -> Result<Vec<ArtifactRecord>, StorageError> {
        let connection = self.connection.lock().map_err(|_| StorageError::Poisoned)?;
        let mut statement = connection.prepare(
            "SELECT id, build_id, name, relative_path, size_bytes, checksum, mime_type, created_at
             FROM build_artifacts
             WHERE build_id = ?1 ORDER BY name ASC, relative_path ASC",
        )?;
        let rows = statement.query_map(params![build_id.to_string()], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, i64>(4)?,
                row.get::<_, String>(5)?,
                row.get::<_, String>(6)?,
                row.get::<_, String>(7)?,
            ))
        })?;
        rows.map(|row| row.map_err(StorageError::from).and_then(parse_artifact))
            .collect()
    }

    /// Persist a bounded, human-readable build annotation. An optional stage
    /// must belong to the same build so consumers cannot attach metadata to a
    /// different execution graph by accident.
    pub fn add_annotation(
        &self,
        build_id: BuildId,
        stage_id: Option<StageId>,
        kind: &str,
        message: &str,
    ) -> Result<AnnotationRecord, StorageError> {
        validate_annotation_field(kind, "kind", MAX_ANNOTATION_KIND_BYTES)?;
        validate_annotation_field(message, "message", MAX_ANNOTATION_MESSAGE_BYTES)?;
        let mut connection = self.connection.lock().map_err(|_| StorageError::Poisoned)?;
        let transaction = connection.transaction()?;
        let build_exists: bool = transaction.query_row(
            "SELECT EXISTS(SELECT 1 FROM builds WHERE id = ?1)",
            params![build_id.to_string()],
            |row| row.get(0),
        )?;
        if !build_exists {
            return Err(StorageError::MissingBuild(build_id));
        }
        if let Some(stage_id) = stage_id {
            let stage_build_id: Option<String> = transaction
                .query_row(
                    "SELECT build_id FROM build_stages WHERE id = ?1",
                    params![stage_id.to_string()],
                    |row| row.get(0),
                )
                .optional()?;
            if stage_build_id.as_deref() != Some(build_id.to_string().as_str()) {
                return Err(StorageError::MissingStage(stage_id));
            }
        }
        let annotation = AnnotationRecord {
            id: Uuid::new_v4(),
            build_id,
            stage_id,
            kind: kind.to_owned(),
            message: message.to_owned(),
            created_at: Utc::now(),
        };
        transaction.execute(
            "INSERT INTO build_annotations(
                id, build_id, stage_id, kind, message, created_at
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                annotation.id.to_string(),
                annotation.build_id.to_string(),
                annotation.stage_id.map(|id| id.to_string()),
                annotation.kind,
                annotation.message,
                annotation.created_at.to_rfc3339(),
            ],
        )?;
        transaction.commit()?;
        Ok(annotation)
    }

    pub fn annotations(&self, build_id: BuildId) -> Result<Vec<AnnotationRecord>, StorageError> {
        let connection = self.connection.lock().map_err(|_| StorageError::Poisoned)?;
        let mut statement = connection.prepare(
            "SELECT id, build_id, stage_id, kind, message, created_at
             FROM build_annotations
             WHERE build_id = ?1 ORDER BY created_at ASC, id ASC",
        )?;
        let rows = statement.query_map(params![build_id.to_string()], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Option<String>>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, String>(5)?,
            ))
        })?;
        rows.map(|row| row.map_err(StorageError::from).and_then(parse_annotation))
            .collect()
    }

    pub fn artifact_file(
        &self,
        artifact_id: Uuid,
    ) -> Result<Option<(ArtifactRecord, std::path::PathBuf)>, StorageError> {
        let connection = self.connection.lock().map_err(|_| StorageError::Poisoned)?;
        let row = connection
            .query_row(
                "SELECT id, build_id, name, relative_path, size_bytes, checksum, mime_type, created_at
                 FROM build_artifacts WHERE id = ?1",
                params![artifact_id.to_string()],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, i64>(4)?,
                        row.get::<_, String>(5)?,
                        row.get::<_, String>(6)?,
                        row.get::<_, String>(7)?,
                    ))
                },
            )
            .optional()?;
        row.map(parse_artifact).transpose().map(|artifact| {
            artifact.map(|record| {
                let path = self
                    .artifact_root
                    .join(record.build_id.to_string())
                    .join(record.id.to_string());
                (record, path)
            })
        })
    }

    /// Remove oldest artifacts from completed builds until their recorded
    /// total fits the byte budget. Active-build artifacts, symlinks, and
    /// non-regular paths are preserved.
    pub fn prune_artifacts(&self, max_bytes: u64) -> Result<ArtifactPruneResult, StorageError> {
        let candidates = {
            let connection = self.connection.lock().map_err(|_| StorageError::Poisoned)?;
            let mut statement = connection.prepare(
                "SELECT a.id, a.build_id, a.name, a.relative_path, a.size_bytes,
                        a.checksum, a.mime_type, a.created_at
                 FROM build_artifacts a
                 JOIN builds b ON b.id = a.build_id
                 WHERE b.status IN ('passed', 'failed', 'cancelled')
                 ORDER BY a.created_at ASC, a.id ASC",
            )?;
            let rows = statement.query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, i64>(4)?,
                    row.get::<_, String>(5)?,
                    row.get::<_, String>(6)?,
                    row.get::<_, String>(7)?,
                ))
            })?;
            rows.map(|row| {
                let artifact = parse_artifact(row?)?;
                let path = self
                    .artifact_root
                    .join(artifact.build_id.to_string())
                    .join(artifact.id.to_string());
                Ok::<_, StorageError>((artifact, path))
            })
            .collect::<Result<Vec<_>, _>>()?
        };

        let mut remaining_bytes = candidates.iter().fold(0_u64, |total, (artifact, _)| {
            total.saturating_add(artifact.size_bytes)
        });
        let mut removed_entries = 0;
        let mut removed_bytes = 0_u64;
        for (artifact, path) in candidates {
            if remaining_bytes <= max_bytes {
                break;
            }
            match fs::symlink_metadata(&path) {
                Ok(metadata) if metadata.file_type().is_symlink() => continue,
                Ok(metadata) if metadata.is_file() => fs::remove_file(&path)?,
                Ok(_) => continue,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(StorageError::Filesystem(error)),
            }

            let connection = self.connection.lock().map_err(|_| StorageError::Poisoned)?;
            let deleted = connection.execute(
                "DELETE FROM build_artifacts WHERE id = ?1",
                params![artifact.id.to_string()],
            )?;
            if deleted == 0 {
                continue;
            }
            remaining_bytes = remaining_bytes.saturating_sub(artifact.size_bytes);
            removed_entries += 1;
            removed_bytes = removed_bytes.saturating_add(artifact.size_bytes);
        }

        Ok(ArtifactPruneResult {
            removed_entries,
            removed_bytes,
            remaining_bytes,
        })
    }
}

fn copy_artifact_tree(
    source: &Path,
    destination: &Path,
    file_count: &mut usize,
    byte_count: &mut u64,
) -> Result<(), StorageError> {
    if !source.is_dir() {
        return Ok(());
    }
    for entry in WalkDir::new(source).follow_links(false) {
        let entry = entry.map_err(|error| StorageError::ArtifactWalk(error.to_string()))?;
        let relative = entry
            .path()
            .strip_prefix(source)
            .map_err(|_| StorageError::BackupUnsupportedArtifactPath(entry.path().to_path_buf()))?;
        if relative.as_os_str().is_empty() {
            continue;
        }
        let target = destination.join(relative);
        let file_type = entry.file_type();
        if file_type.is_symlink() || (!file_type.is_dir() && !file_type.is_file()) {
            return Err(StorageError::BackupUnsupportedArtifactPath(
                relative.to_path_buf(),
            ));
        }
        if file_type.is_dir() {
            fs::create_dir_all(target)?;
            continue;
        }
        if let Some(parent) = target.parent() {
            fs::create_dir_all(parent)?;
        }
        let bytes = fs::metadata(entry.path())?.len();
        fs::copy(entry.path(), &target)?;
        *file_count = (*file_count).saturating_add(1);
        *byte_count = byte_count.saturating_add(bytes);
    }
    Ok(())
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
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<i64>,
    String,
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
type RawArtifact = (String, String, String, String, i64, String, String, String);
type RawAnnotation = (String, String, Option<String>, String, String, String);
type RawSchedule = (
    String,
    String,
    String,
    String,
    String,
    i64,
    String,
    Option<String>,
    String,
    String,
    i64,
    Option<String>,
);
type RawWebhookDelivery = (String, String, String, Option<String>, Option<i64>);
type RawRepositoryPollEvent = (String, String, String, String, Option<String>, Option<i64>);
type RawPipelineTrigger = (String, String, String, i64, String);
type RawProviderTrigger = (String, String, String, String, String, i64, String);
type RawAuditEvent = (i64, String, Option<String>, String, String, String);
type RawAuthSession = (
    String,
    String,
    String,
    String,
    String,
    String,
    String,
    Option<String>,
);
type RawRemoteAttempt = (
    String,
    String,
    String,
    String,
    String,
    String,
    String,
    String,
    String,
    i64,
);

fn validate_audit_field(value: &str, field: &'static str) -> Result<(), StorageError> {
    if value.trim().is_empty()
        || value.len() > MAX_AUDIT_FIELD_BYTES
        || value.chars().any(char::is_control)
    {
        return Err(StorageError::InvalidAuditField { field });
    }
    Ok(())
}

fn validate_schedule_poll_config(config: &SchedulePollConfig) -> Result<(), StorageError> {
    if config.remote.trim().is_empty()
        || config.remote.starts_with('-')
        || config.remote.chars().any(char::is_control)
    {
        return Err(StorageError::InvalidSchedulePollRemote(
            config.remote.clone(),
        ));
    }
    if let Some(credential_id) = config.credential_id.as_deref() {
        if credential_id.trim().is_empty()
            || credential_id.len() > 256
            || credential_id.chars().any(char::is_control)
        {
            return Err(StorageError::InvalidSchedulePollCredential(
                credential_id.to_owned(),
            ));
        }
    }
    if config.credential_id.is_some() && !config.fetch {
        return Err(StorageError::InvalidSchedulePollRemote(
            "a poll credential requires fetch=true".to_owned(),
        ));
    }
    Ok(())
}

fn normalize_schedule_poll_config(
    mut config: SchedulePollConfig,
) -> Result<SchedulePollConfig, StorageError> {
    validate_schedule_poll_config(&config)?;
    config.remote = config.remote.trim().to_owned();
    if let Some(credential_id) = config.credential_id.as_mut() {
        *credential_id = credential_id.trim().to_owned();
    }
    Ok(config)
}

fn validate_annotation_field(
    value: &str,
    field: &'static str,
    max_bytes: usize,
) -> Result<(), StorageError> {
    if value.trim().is_empty() || value.len() > max_bytes || value.chars().any(char::is_control) {
        return Err(StorageError::InvalidAnnotationField { field });
    }
    Ok(())
}

fn validate_session_digest(value: &str) -> Result<(), StorageError> {
    if value.len() != SESSION_DIGEST_BYTES * 2
        || value.bytes().any(|byte| !byte.is_ascii_hexdigit())
    {
        return Err(StorageError::InvalidSessionDigest);
    }
    Ok(())
}

fn validate_session_field(value: &str, field: &'static str) -> Result<(), StorageError> {
    if value.trim().is_empty()
        || value.len() > MAX_AUDIT_FIELD_BYTES
        || value.chars().any(char::is_control)
    {
        return Err(StorageError::InvalidSessionPrincipal);
    }
    if field == "role" && !matches!(value, "admin" | "operator" | "viewer" | "agent") {
        return Err(StorageError::InvalidSessionRole);
    }
    Ok(())
}

fn event_hash(event_json: &str) -> String {
    hex::encode(Sha256::digest(event_json.as_bytes()))
}

fn backfill_event_hashes(connection: &Connection) -> Result<(), StorageError> {
    let rows = {
        let mut statement =
            connection.prepare("SELECT sequence, event_json FROM build_events WHERE event_hash IS NULL ORDER BY sequence ASC")?;
        statement
            .query_map([], |row| {
                Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
            })?
            .collect::<Result<Vec<_>, _>>()?
    };
    let mut seen = BTreeSet::new();
    for (sequence, event_json) in rows {
        let digest = event_hash(&event_json);
        // Keep pre-existing duplicate rows readable during the one-time
        // migration; future writes will still converge on the canonical hash.
        if seen.insert(digest.clone()) {
            connection.execute(
                "UPDATE build_events SET event_hash = ?1 WHERE sequence = ?2",
                params![digest, sequence],
            )?;
        }
    }
    Ok(())
}

fn raw_audit_event(row: &rusqlite::Row<'_>) -> rusqlite::Result<RawAuditEvent> {
    Ok((
        row.get(0)?,
        row.get(1)?,
        row.get(2)?,
        row.get(3)?,
        row.get(4)?,
        row.get(5)?,
    ))
}

fn parse_audit_event(raw: RawAuditEvent) -> Result<AuditEventRecord, StorageError> {
    Ok(AuditEventRecord {
        sequence: raw.0,
        timestamp: parse_timestamp(&raw.1)?,
        actor_id: raw.2,
        action: raw.3,
        resource: raw.4,
        outcome: raw.5,
    })
}

fn raw_auth_session(row: &rusqlite::Row<'_>) -> rusqlite::Result<RawAuthSession> {
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

fn parse_auth_session(raw: RawAuthSession) -> Result<AuthSessionRecord, StorageError> {
    validate_session_digest(&raw.1)?;
    validate_session_field(&raw.2, "principal")?;
    validate_session_field(&raw.3, "role")?;
    Ok(AuthSessionRecord {
        id: parse_uuid(&raw.0)?,
        token_sha256: raw.1,
        principal_id: raw.2,
        role: raw.3,
        projects: serde_json::from_str(&raw.4)?,
        created_at: parse_timestamp(&raw.5)?,
        expires_at: parse_timestamp(&raw.6)?,
        revoked_at: raw.7.as_deref().map(parse_timestamp).transpose()?,
    })
}

fn raw_remote_attempt(row: &rusqlite::Row<'_>) -> rusqlite::Result<RawRemoteAttempt> {
    Ok((
        row.get(0)?,
        row.get(1)?,
        row.get(2)?,
        row.get(3)?,
        row.get(4)?,
        row.get(5)?,
        row.get(6)?,
        row.get(7)?,
        row.get(8)?,
        row.get(9)?,
    ))
}

fn parse_remote_attempt(raw: RawRemoteAttempt) -> Result<RemoteAttemptRecord, StorageError> {
    let recovery_attempts =
        usize::try_from(raw.9).map_err(|_| StorageError::InvalidRemoteRecoveryAttempts(raw.9))?;
    Ok(RemoteAttemptRecord {
        build_id: parse_uuid(&raw.0)?,
        attempt_id: parse_uuid(&raw.1)?,
        project_id: parse_uuid(&raw.2)?,
        agent_id: parse_uuid(&raw.3)?,
        requirements_json: raw.4,
        plan: serde_json::from_str(&raw.5)?,
        pipeline: serde_json::from_str(&raw.6)?,
        parameters: serde_json::from_str(&raw.7)?,
        created_at: parse_timestamp(&raw.8)?,
        recovery_attempts,
    })
}

fn parse_project(raw: RawProject) -> Result<Project, StorageError> {
    Ok(Project {
        id: parse_uuid(&raw.0)?,
        name: raw.1,
        repository_path: raw.2,
        pipeline_path: raw.3,
        created_at: parse_timestamp(&raw.4)?,
    })
}

fn event_build_id(event: &BuildEvent) -> BuildId {
    match event {
        BuildEvent::BuildQueued { build_id, .. }
        | BuildEvent::BuildStarted { build_id, .. }
        | BuildEvent::BuildFinished { build_id, .. }
        | BuildEvent::BuildCancelled { build_id, .. }
        | BuildEvent::StageStarted { build_id, .. }
        | BuildEvent::StageFinished { build_id, .. }
        | BuildEvent::StepStarted { build_id, .. }
        | BuildEvent::StepOutput { build_id, .. }
        | BuildEvent::StepFinished { build_id, .. } => *build_id,
    }
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
        row.get(7)?,
        row.get(8)?,
        row.get(9)?,
        row.get(10)?,
        row.get(11)?,
        row.get(12)?,
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
        source: parse_source(&raw)?,
        parameters: serde_json::from_str(&raw.12)?,
    })
}

fn parse_source(raw: &RawBuild) -> Result<Option<SourceSnapshot>, StorageError> {
    match (raw.7.as_deref(), raw.8.as_deref()) {
        (None, None) => Ok(None),
        (Some(provider), Some(revision)) => {
            let dirty = match raw.11 {
                None | Some(0) => false,
                Some(1) => true,
                Some(value) => return Err(StorageError::InvalidSource(value.to_string())),
            };
            Ok(Some(SourceSnapshot {
                provider: provider.to_owned(),
                revision: revision.to_owned(),
                reference: raw.9.clone(),
                remote: raw.10.clone(),
                dirty,
            }))
        }
        _ => Err(StorageError::InvalidSource(
            "provider and revision must be stored together".to_owned(),
        )),
    }
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

fn parse_artifact(raw: RawArtifact) -> Result<ArtifactRecord, StorageError> {
    Ok(ArtifactRecord {
        id: parse_uuid(&raw.0)?,
        build_id: parse_uuid(&raw.1)?,
        name: raw.2,
        relative_path: raw.3,
        size_bytes: u64::try_from(raw.4).map_err(|_| StorageError::InvalidArtifactSize(raw.4))?,
        checksum: raw.5,
        mime_type: raw.6,
        created_at: parse_timestamp(&raw.7)?,
    })
}

fn artifact_mime_type(path: &str) -> String {
    let extension = path
        .rsplit('.')
        .next()
        .unwrap_or_default()
        .to_ascii_lowercase();
    match extension.as_str() {
        "css" => "text/css",
        "csv" => "text/csv",
        "html" | "htm" => "text/html",
        "js" | "mjs" => "text/javascript",
        "json" | "map" => "application/json",
        "pdf" => "application/pdf",
        "png" => "image/png",
        "jpg" | "jpeg" => "image/jpeg",
        "gif" => "image/gif",
        "svg" => "image/svg+xml",
        "txt" | "log" => "text/plain",
        "toml" => "application/toml",
        "xml" => "application/xml",
        "zip" => "application/zip",
        "gz" => "application/gzip",
        "wasm" => "application/wasm",
        _ => "application/octet-stream",
    }
    .to_string()
}

fn parse_annotation(raw: RawAnnotation) -> Result<AnnotationRecord, StorageError> {
    validate_annotation_field(&raw.3, "kind", MAX_ANNOTATION_KIND_BYTES)?;
    validate_annotation_field(&raw.4, "message", MAX_ANNOTATION_MESSAGE_BYTES)?;
    Ok(AnnotationRecord {
        id: parse_uuid(&raw.0)?,
        build_id: parse_uuid(&raw.1)?,
        stage_id: raw.2.as_deref().map(parse_uuid).transpose()?,
        kind: raw.3,
        message: raw.4,
        created_at: parse_timestamp(&raw.5)?,
    })
}

fn raw_schedule(row: &rusqlite::Row<'_>) -> rusqlite::Result<RawSchedule> {
    Ok((
        row.get(0)?,
        row.get(1)?,
        row.get(2)?,
        row.get(3)?,
        row.get(4)?,
        row.get(5)?,
        row.get(6)?,
        row.get(7)?,
        row.get(8)?,
        row.get(9)?,
        row.get(10)?,
        row.get(11)?,
    ))
}

fn parse_schedule(raw: RawSchedule) -> Result<ScheduleRecord, StorageError> {
    let trigger = match raw.4.as_str() {
        "build" => ScheduleTrigger::Build,
        "repository_poll" => ScheduleTrigger::RepositoryPoll,
        value => return Err(StorageError::InvalidScheduleTrigger(value.to_owned())),
    };
    let enabled = match raw.5 {
        0 => false,
        1 => true,
        value => return Err(StorageError::InvalidScheduleEnabled(value)),
    };
    let poll = if matches!(trigger, ScheduleTrigger::RepositoryPoll) {
        let fetch = match raw.10 {
            0 => false,
            1 => true,
            value => return Err(StorageError::InvalidSchedulePollFetch(value)),
        };
        let poll = SchedulePollConfig {
            remote: raw.9,
            fetch,
            credential_id: raw.11,
        };
        validate_schedule_poll_config(&poll)?;
        Some(poll)
    } else {
        None
    };
    Ok(ScheduleRecord {
        id: parse_uuid(&raw.0)?,
        project_id: parse_uuid(&raw.1)?,
        name: raw.2,
        expression: raw.3,
        trigger,
        poll,
        enabled,
        next_run_at: parse_timestamp(&raw.6)?,
        last_run_at: raw.7.as_deref().map(parse_timestamp).transpose()?,
        created_at: parse_timestamp(&raw.8)?,
    })
}

fn raw_webhook_delivery(row: &rusqlite::Row<'_>) -> rusqlite::Result<RawWebhookDelivery> {
    Ok((
        row.get(0)?,
        row.get(1)?,
        row.get(2)?,
        row.get(3)?,
        row.get(4)?,
    ))
}

fn parse_webhook_delivery(raw: RawWebhookDelivery) -> Result<WebhookDeliveryRecord, StorageError> {
    Ok(WebhookDeliveryRecord {
        event_id: raw.0,
        project_id: parse_uuid(&raw.1)?,
        received_at: parse_timestamp(&raw.2)?,
        build_id: raw.3.as_deref().map(parse_uuid).transpose()?,
        build_number: raw.4,
    })
}

fn raw_repository_poll_event(row: &rusqlite::Row<'_>) -> rusqlite::Result<RawRepositoryPollEvent> {
    Ok((
        row.get(0)?,
        row.get(1)?,
        row.get(2)?,
        row.get(3)?,
        row.get(4)?,
        row.get(5)?,
    ))
}

fn parse_repository_poll_event(
    raw: RawRepositoryPollEvent,
) -> Result<RepositoryPollEventRecord, StorageError> {
    Ok(RepositoryPollEventRecord {
        event_id: raw.0,
        project_id: parse_uuid(&raw.1)?,
        revision: raw.2,
        checked_at: parse_timestamp(&raw.3)?,
        build_id: raw.4.as_deref().map(parse_uuid).transpose()?,
        build_number: raw.5,
    })
}

fn raw_pipeline_trigger(row: &rusqlite::Row<'_>) -> rusqlite::Result<RawPipelineTrigger> {
    Ok((
        row.get(0)?,
        row.get(1)?,
        row.get(2)?,
        row.get(3)?,
        row.get(4)?,
    ))
}

fn parse_pipeline_trigger(raw: RawPipelineTrigger) -> Result<PipelineTriggerRecord, StorageError> {
    let enabled = match raw.3 {
        0 => false,
        1 => true,
        value => return Err(StorageError::InvalidPipelineTriggerEnabled(value)),
    };
    Ok(PipelineTriggerRecord {
        id: parse_uuid(&raw.0)?,
        upstream_project_id: parse_uuid(&raw.1)?,
        downstream_project_id: parse_uuid(&raw.2)?,
        enabled,
        created_at: parse_timestamp(&raw.4)?,
    })
}

fn raw_provider_trigger(row: &rusqlite::Row<'_>) -> rusqlite::Result<RawProviderTrigger> {
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

fn parse_provider_trigger(raw: RawProviderTrigger) -> Result<ProviderTriggerRecord, StorageError> {
    let enabled = match raw.5 {
        0 => false,
        1 => true,
        value => return Err(StorageError::InvalidProviderTriggerEnabled(value)),
    };
    Ok(ProviderTriggerRecord {
        id: parse_uuid(&raw.0)?,
        downstream_project_id: parse_uuid(&raw.1)?,
        provider: raw.2,
        source_repository: raw.3,
        source_pipeline: (!raw.4.is_empty()).then_some(raw.4),
        enabled,
        created_at: parse_timestamp(&raw.6)?,
    })
}

fn normalize_provider_trigger_selector(
    value: &str,
    field: &'static str,
) -> Result<String, StorageError> {
    let value = value.trim();
    if value.is_empty() || value.len() > 512 || value.chars().any(char::is_control) {
        return Err(StorageError::InvalidProviderTriggerSelector { field });
    }
    Ok(value.to_owned())
}

fn sha256_file(path: &Path) -> Result<String, StorageError> {
    let mut file = fs::File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(format!("sha256:{:x}", hasher.finalize()))
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
    use chrono::TimeZone;
    use rivet_core::{BuildEvent, ExecutionPlan, Pipeline, SourceSnapshot};
    use std::collections::BTreeMap;
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
        let source = SourceSnapshot {
            provider: "git".to_owned(),
            revision: "0123456789012345678901234567890123456789".to_owned(),
            reference: Some("main".to_owned()),
            remote: Some("origin".to_owned()),
            dirty: false,
        };
        let parameters = BTreeMap::from([("target".to_owned(), "release".to_owned())]);
        let build = storage
            .create_build_with_parameters(&project, &plan, &pipeline, Some(&source), &parameters)
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
        assert_eq!(details.build.source, Some(source));
        assert_eq!(details.build.parameters, parameters);
        assert_eq!(details.stages[0].steps[0].status, StepStatus::Passed);
        assert_eq!(reopened.logs(build.id).expect("logs")[0].line, "hello");
        let events = reopened.events(build.id).expect("events");
        assert_eq!(events.len(), 8);
        assert!(matches!(events[0], BuildEvent::BuildQueued { .. }));
        assert!(matches!(events[7], BuildEvent::BuildFinished { .. }));
    }

    #[test]
    fn logs_page_uses_a_sequence_cursor_and_rejects_unbounded_limits() {
        let (project, pipeline, plan) = fixture();
        let storage = Storage::open_in_memory().expect("storage");
        storage
            .create_project(&project, &pipeline)
            .expect("project");
        let build = storage
            .create_build(&project, &plan, &pipeline, None)
            .expect("build");
        let stage = &plan.stages[0];
        let step = &stage.steps[0];
        for line in ["one", "two", "three"] {
            storage
                .apply_event(&BuildEvent::StepOutput {
                    build_id: build.id,
                    stage_id: stage.id,
                    step_id: step.id,
                    stream: LogStream::Stdout,
                    line: line.into(),
                    timestamp: Utc::now(),
                })
                .expect("log");
        }

        let first = storage.logs_page(build.id, -1, 2).expect("first page");
        assert_eq!(
            first
                .iter()
                .map(|log| log.line.as_str())
                .collect::<Vec<_>>(),
            ["one", "two"]
        );
        let second = storage
            .logs_page(build.id, first[1].sequence, 2)
            .expect("second page");
        assert_eq!(second[0].line, "three");
        assert!(matches!(
            storage.logs_page(build.id, -2, 2),
            Err(StorageError::InvalidLogPage)
        ));
        assert!(matches!(
            storage.logs_page(build.id, -1, 10_001),
            Err(StorageError::InvalidLogPage)
        ));
    }

    #[test]
    fn applying_the_same_event_twice_is_idempotent() {
        let (project, pipeline, plan) = fixture();
        let storage = Storage::open_in_memory().expect("storage");
        storage
            .create_project(&project, &pipeline)
            .expect("project");
        let build = storage
            .create_build(&project, &plan, &pipeline, None)
            .expect("build");
        let timestamp = Utc
            .with_ymd_and_hms(2026, 9, 13, 12, 0, 0)
            .single()
            .expect("timestamp");
        let event = BuildEvent::BuildQueued {
            build_id: build.id,
            project_id: project.id,
            timestamp,
        };
        storage.apply_event(&event).expect("first event");
        storage.apply_event(&event).expect("duplicate event");
        assert_eq!(storage.events(build.id).expect("events").len(), 1);
        assert_eq!(
            storage
                .get_build_details(build.id)
                .expect("details")
                .expect("build")
                .build
                .status,
            BuildStatus::Queued
        );
    }

    #[test]
    fn build_annotations_are_scoped_validated_and_reopenable() {
        let directory = tempdir().expect("tempdir");
        let database = directory.path().join("rivet.db");
        let (project, pipeline, plan) = fixture();
        let storage = Storage::open(&database).expect("open");
        storage
            .create_project(&project, &pipeline)
            .expect("project");
        let build = storage
            .create_build(&project, &plan, &pipeline, None)
            .expect("build");
        let stage_id = plan.stages[0].id;
        let annotation = storage
            .add_annotation(build.id, Some(stage_id), "warning", "lint issue")
            .expect("annotation");
        assert_eq!(annotation.build_id, build.id);
        assert_eq!(annotation.stage_id, Some(stage_id));
        assert_eq!(
            storage.annotations(build.id).expect("annotations"),
            vec![annotation.clone()]
        );
        assert!(matches!(
            storage.add_annotation(build.id, None, "", "message"),
            Err(StorageError::InvalidAnnotationField { field: "kind" })
        ));
        assert!(matches!(
            storage.add_annotation(build.id, Some(Uuid::new_v4()), "warning", "message"),
            Err(StorageError::MissingStage(_))
        ));
        drop(storage);

        let reopened = Storage::open(&database).expect("reopen");
        assert_eq!(
            reopened.annotations(build.id).expect("annotations"),
            vec![annotation]
        );
    }

    #[test]
    fn secret_parameters_are_redacted_in_build_history() {
        let directory = tempdir().expect("tempdir");
        let database = directory.path().join("rivet.db");
        let pipeline = Pipeline::from_toml_str(
            r#"
version = 1
name = "secret-history"
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
        let project = Project::new("secret-history", ".", "Rivetfile.toml").expect("project");
        let plan = ExecutionPlan::from_pipeline(&pipeline, Uuid::new_v4(), project.id);
        let storage = Storage::open(&database).expect("storage");
        storage
            .create_project(&project, &pipeline)
            .expect("project");
        let build = storage
            .create_build_with_parameters(
                &project,
                &plan,
                &pipeline,
                None,
                &BTreeMap::from([(String::from("TOKEN"), String::from("runtime-secret"))]),
            )
            .expect("build");
        assert_eq!(
            build.parameters["TOKEN"],
            rivet_core::REDACTED_PARAMETER_VALUE
        );
        assert!(
            !serde_json::to_string(&build)
                .expect("build JSON")
                .contains("runtime-secret")
        );
        drop(storage);
        let reopened = Storage::open(&database).expect("reopen");
        let persisted = reopened
            .get_build_details(build.id)
            .expect("details")
            .expect("build");
        assert_eq!(
            persisted.build.parameters["TOKEN"],
            rivet_core::REDACTED_PARAMETER_VALUE
        );
    }

    #[test]
    fn collects_workspace_artifacts_with_checksum_and_reopenable_storage() {
        let directory = tempdir().expect("tempdir");
        let workspace = directory.path().join("workspace");
        fs::create_dir_all(workspace.join("dist")).expect("dist");
        fs::write(workspace.join("dist/app.js"), "console.log('rivet');\n").expect("artifact");
        fs::write(workspace.join("notes.txt"), "not selected\n").expect("other file");
        let database = directory.path().join("rivet.db");
        let project = Project::new(
            "artifacts",
            workspace.to_string_lossy().into_owned(),
            workspace
                .join("Rivetfile.toml")
                .to_string_lossy()
                .into_owned(),
        )
        .expect("project");
        let pipeline = Pipeline::from_toml_str(
            r#"
version = 1
name = "artifacts"

[[artifacts]]
name = "bundle"
paths = ["dist/**"]

[[stages]]
name = "Build"
[[stages.steps]]
name = "compile"
program = "true"
"#,
        )
        .expect("pipeline");
        let plan = ExecutionPlan::from_pipeline(&pipeline, Uuid::new_v4(), project.id);
        let storage = Storage::open(&database).expect("open");
        storage
            .create_project(&project, &pipeline)
            .expect("project");
        let build = storage
            .create_build(&project, &plan, &pipeline, None)
            .expect("build");

        let collected = storage
            .collect_artifacts(build.id, &pipeline, &workspace)
            .expect("collect");
        assert_eq!(collected.len(), 1);
        assert_eq!(collected[0].relative_path, "dist/app.js");
        assert_eq!(collected[0].name, "bundle");
        assert_eq!(collected[0].mime_type, "text/javascript");
        assert!(collected[0].checksum.starts_with("sha256:"));

        let again = storage
            .collect_artifacts(build.id, &pipeline, &workspace)
            .expect("idempotent collect");
        assert_eq!(again, collected);

        let (record, stored_path) = storage
            .artifact_file(collected[0].id)
            .expect("artifact lookup")
            .expect("artifact exists");
        assert_eq!(record, collected[0]);
        assert_eq!(
            fs::read_to_string(stored_path).expect("stored file"),
            "console.log('rivet');\n"
        );

        drop(storage);
        let reopened = Storage::open(&database).expect("reopen");
        assert_eq!(reopened.artifacts(build.id).expect("artifacts"), collected);
    }

    #[test]
    fn backup_is_consistent_and_copies_artifact_store() {
        let directory = tempdir().expect("tempdir");
        let workspace = directory.path().join("workspace");
        fs::create_dir_all(&workspace).expect("workspace");
        fs::write(workspace.join("result.txt"), "backup me\n").expect("artifact");
        let database = directory.path().join("rivet.db");
        let project = Project::new(
            "backup",
            workspace.to_string_lossy().into_owned(),
            workspace
                .join("Rivetfile.toml")
                .to_string_lossy()
                .into_owned(),
        )
        .expect("project");
        let pipeline = Pipeline::from_toml_str(
            r#"
version = 1
name = "backup"
[[artifacts]]
name = "result"
paths = ["result.txt"]
[[stages]]
name = "Build"
[[stages.steps]]
name = "unit"
program = "true"
"#,
        )
        .expect("pipeline");
        let plan = ExecutionPlan::from_pipeline(&pipeline, Uuid::new_v4(), project.id);
        let storage = Storage::open(&database).expect("open");
        storage
            .create_project(&project, &pipeline)
            .expect("project");
        let build = storage
            .create_build(&project, &plan, &pipeline, None)
            .expect("build");
        let artifacts = storage
            .collect_artifacts(build.id, &pipeline, &workspace)
            .expect("collect artifacts");
        let backup = directory.path().join("backup");
        let report = storage.backup_to(&backup).expect("backup");
        assert_eq!(report.artifact_files, 1);
        assert_eq!(report.artifact_bytes, b"backup me\n".len() as u64);
        assert_eq!(
            fs::read_to_string(
                backup
                    .join("artifacts")
                    .join(build.id.to_string())
                    .join(artifacts[0].id.to_string())
            )
            .expect("backed up artifact"),
            "backup me\n"
        );

        let reopened = Storage::open(backup.join("rivet.db")).expect("reopen backup");
        assert_eq!(reopened.artifacts(build.id).expect("artifacts"), artifacts);
        assert!(matches!(
            storage.backup_to(&storage.artifact_root().join("nested")),
            Err(StorageError::BackupDestinationInsideArtifactStore(_))
        ));
    }

    #[test]
    fn prunes_oldest_completed_artifacts_without_touching_active_builds() {
        let directory = tempdir().expect("tempdir");
        let workspace = directory.path().join("workspace");
        fs::create_dir_all(&workspace).expect("workspace");
        let pipeline = Pipeline::from_toml_str(
            r#"
version = 1
name = "artifact-retention"
[[artifacts]]
name = "bundle"
paths = ["artifact.txt"]
[[stages]]
name = "Build"
[[stages.steps]]
name = "unit"
program = "true"
"#,
        )
        .expect("pipeline");
        let project = Project::new(
            "artifact-retention",
            workspace.to_string_lossy().into_owned(),
            workspace
                .join("Rivetfile.toml")
                .to_string_lossy()
                .into_owned(),
        )
        .expect("project");
        let database = directory.path().join("rivet.db");
        let storage = Storage::open(&database).expect("storage");
        storage
            .create_project(&project, &pipeline)
            .expect("project");

        let mut completed = Vec::new();
        for contents in ["a", "bb", "ccc"] {
            fs::write(workspace.join("artifact.txt"), contents).expect("artifact");
            let plan = ExecutionPlan::from_pipeline(&pipeline, Uuid::new_v4(), project.id);
            let build = storage
                .create_build(&project, &plan, &pipeline, None)
                .expect("build");
            let artifacts = storage
                .collect_artifacts(build.id, &pipeline, &workspace)
                .expect("collect");
            completed.push(artifacts[0].clone());
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
            storage
                .apply_event(&BuildEvent::BuildFinished {
                    build_id: build.id,
                    status: BuildStatus::Passed,
                    timestamp: Utc::now(),
                })
                .expect("finished");
            std::thread::sleep(std::time::Duration::from_millis(3));
        }

        fs::write(workspace.join("artifact.txt"), "live").expect("active artifact");
        let active_plan = ExecutionPlan::from_pipeline(&pipeline, Uuid::new_v4(), project.id);
        let active_build = storage
            .create_build(&project, &active_plan, &pipeline, None)
            .expect("active build");
        let active_artifact = storage
            .collect_artifacts(active_build.id, &pipeline, &workspace)
            .expect("active collect")[0]
            .clone();

        let first_prune = storage.prune_artifacts(3).expect("first prune");
        assert_eq!(first_prune.removed_entries, 2);
        assert_eq!(first_prune.removed_bytes, 3);
        assert_eq!(first_prune.remaining_bytes, 3);
        assert!(
            storage
                .artifact_file(completed[0].id)
                .expect("first lookup")
                .is_none()
        );
        assert!(
            storage
                .artifact_file(completed[1].id)
                .expect("second lookup")
                .is_none()
        );
        assert!(
            storage
                .artifact_file(completed[2].id)
                .expect("third lookup")
                .is_some()
        );
        assert!(
            storage
                .artifact_file(active_artifact.id)
                .expect("active lookup")
                .is_some()
        );

        let second_prune = storage.prune_artifacts(0).expect("second prune");
        assert_eq!(second_prune.removed_entries, 1);
        assert_eq!(second_prune.removed_bytes, 3);
        assert_eq!(second_prune.remaining_bytes, 0);
        assert_eq!(
            storage
                .artifacts(active_build.id)
                .expect("active artifacts")
                .len(),
            1
        );
    }

    #[test]
    fn schedules_are_persisted_and_claimed_once() {
        let directory = tempdir().expect("tempdir");
        let database = directory.path().join("rivet.db");
        let (project, pipeline, _) = fixture();
        let storage = Storage::open(&database).expect("open");
        storage
            .create_project(&project, &pipeline)
            .expect("project");
        let due_at = Utc
            .with_ymd_and_hms(2026, 9, 13, 12, 0, 0)
            .single()
            .expect("due timestamp");
        let next_at = Utc
            .with_ymd_and_hms(2026, 9, 13, 12, 5, 0)
            .single()
            .expect("next timestamp");
        let schedule = storage
            .create_schedule(project.id, "nightly", "*/5 * * * *", true, due_at)
            .expect("schedule");
        assert_eq!(schedule.trigger, ScheduleTrigger::Build);

        assert_eq!(
            storage.list_schedules(project.id).expect("list"),
            vec![schedule.clone()]
        );
        assert_eq!(storage.due_schedules(due_at).expect("due").len(), 1);
        assert!(
            storage
                .claim_schedule(schedule.id, due_at, due_at, next_at)
                .expect("claim")
        );
        assert!(storage.due_schedules(due_at).expect("claimed").is_empty());
        assert!(
            !storage
                .claim_schedule(schedule.id, due_at, due_at, next_at)
                .expect("duplicate claim")
        );

        drop(storage);
        let reopened = Storage::open(&database).expect("reopen");
        let persisted = reopened
            .list_schedules(project.id)
            .expect("persisted")
            .pop()
            .expect("schedule exists");
        assert_eq!(persisted.trigger, ScheduleTrigger::Build);
        assert_eq!(persisted.next_run_at, next_at);
        assert_eq!(persisted.last_run_at, Some(due_at));
    }

    #[test]
    fn repository_poll_schedule_options_survive_reopen() {
        let directory = tempdir().expect("tempdir");
        let database = directory.path().join("rivet.db");
        let (project, pipeline, _) = fixture();
        let storage = Storage::open(&database).expect("open");
        storage
            .create_project(&project, &pipeline)
            .expect("project");
        let due_at = Utc
            .with_ymd_and_hms(2026, 9, 13, 12, 0, 0)
            .single()
            .expect("due timestamp");
        let poll = SchedulePollConfig {
            remote: "upstream".into(),
            fetch: true,
            credential_id: Some("scm-prod".into()),
        };
        let schedule = storage
            .create_schedule_with_trigger_and_poll(
                project.id,
                "poll-upstream",
                "*/5 * * * *",
                ScheduleTrigger::RepositoryPoll,
                Some(poll.clone()),
                true,
                due_at,
            )
            .expect("schedule");
        assert_eq!(schedule.poll, Some(poll.clone()));
        drop(storage);

        let reopened = Storage::open(&database).expect("reopen");
        let persisted = reopened
            .list_schedules(project.id)
            .expect("persisted")
            .pop()
            .expect("schedule exists");
        assert_eq!(persisted.poll, Some(poll));
    }

    #[test]
    fn repository_poll_schedule_rejects_credentials_without_fetch() {
        let (project, pipeline, _) = fixture();
        let storage = Storage::open_in_memory().expect("storage");
        storage
            .create_project(&project, &pipeline)
            .expect("project");
        let result = storage.create_schedule_with_trigger_and_poll(
            project.id,
            "invalid-poll",
            "* * * * *",
            ScheduleTrigger::RepositoryPoll,
            Some(SchedulePollConfig {
                remote: "origin".into(),
                fetch: false,
                credential_id: Some("scm-prod".into()),
            }),
            true,
            Utc::now(),
        );
        assert!(matches!(
            result,
            Err(StorageError::InvalidSchedulePollRemote(message))
                if message.contains("fetch=true")
        ));
    }

    #[test]
    fn pipeline_triggers_are_durable_idempotent_and_cycle_checked() {
        let directory = tempdir().expect("tempdir");
        let database = directory.path().join("rivet.db");
        let (_, pipeline, _) = fixture();
        let upstream = Project::new("upstream", ".", "Rivetfile.toml").expect("upstream");
        let downstream = Project::new("downstream", ".", "Rivetfile.toml").expect("downstream");
        let storage = Storage::open(&database).expect("open");
        storage
            .create_project(&upstream, &pipeline)
            .expect("upstream project");
        storage
            .create_project(&downstream, &pipeline)
            .expect("downstream project");

        let trigger = storage
            .create_pipeline_trigger(upstream.id, downstream.id)
            .expect("trigger")
            .expect("new trigger");
        assert!(
            storage
                .create_pipeline_trigger(upstream.id, downstream.id)
                .expect("duplicate trigger")
                .is_none()
        );
        assert_eq!(
            storage
                .list_pipeline_triggers_to(downstream.id)
                .expect("downstream triggers"),
            vec![trigger.clone()]
        );
        assert!(
            storage
                .pipeline_trigger_reaches(upstream.id, downstream.id)
                .expect("reachable")
        );
        assert!(
            !storage
                .pipeline_trigger_reaches(downstream.id, upstream.id)
                .expect("not reachable")
        );
        assert!(matches!(
            storage.create_pipeline_trigger(upstream.id, upstream.id),
            Err(StorageError::PipelineTriggerSelfReference)
        ));

        let upstream_plan = ExecutionPlan::from_pipeline(&pipeline, Uuid::new_v4(), upstream.id);
        let upstream_build = storage
            .create_build(&upstream, &upstream_plan, &pipeline, None)
            .expect("upstream build");
        let finished_at = Utc::now();
        storage
            .apply_event(&BuildEvent::BuildQueued {
                build_id: upstream_build.id,
                project_id: upstream.id,
                timestamp: finished_at,
            })
            .expect("queue upstream");
        storage
            .apply_event(&BuildEvent::BuildStarted {
                build_id: upstream_build.id,
                timestamp: finished_at,
            })
            .expect("start upstream");
        storage
            .apply_event(&BuildEvent::BuildFinished {
                build_id: upstream_build.id,
                status: BuildStatus::Passed,
                timestamp: finished_at,
            })
            .expect("finish upstream");

        let pending = storage
            .pending_pipeline_trigger_deliveries()
            .expect("pending trigger deliveries");
        assert_eq!(pending, vec![(trigger.clone(), upstream_build.id)]);
        assert!(
            storage
                .claim_pipeline_trigger_delivery(trigger.id, upstream_build.id, finished_at)
                .expect("claim trigger delivery")
        );
        assert!(
            !storage
                .claim_pipeline_trigger_delivery(trigger.id, upstream_build.id, finished_at)
                .expect("duplicate trigger delivery")
        );

        let downstream_plan =
            ExecutionPlan::from_pipeline(&pipeline, Uuid::new_v4(), downstream.id);
        let downstream_build = storage
            .create_build(&downstream, &downstream_plan, &pipeline, None)
            .expect("downstream build");
        storage
            .complete_pipeline_trigger_delivery(trigger.id, upstream_build.id, downstream_build.id)
            .expect("complete trigger delivery");
        assert!(
            storage
                .pending_pipeline_trigger_deliveries()
                .expect("no pending trigger deliveries")
                .is_empty()
        );

        drop(storage);
        let reopened = Storage::open(&database).expect("reopen");
        assert_eq!(
            reopened
                .list_pipeline_triggers_from(upstream.id)
                .expect("reopened trigger")
                .len(),
            1
        );
        assert!(
            reopened
                .delete_pipeline_trigger(downstream.id, trigger.id)
                .expect("delete trigger")
        );
        assert!(
            reopened
                .list_pipeline_triggers()
                .expect("deleted trigger list")
                .is_empty()
        );
    }

    #[test]
    fn provider_triggers_match_workflows_and_deduplicate_deliveries() {
        let directory = tempdir().expect("tempdir");
        let database = directory.path().join("rivet.db");
        let (project, pipeline, plan) = fixture();
        let storage = Storage::open(&database).expect("open");
        storage
            .create_project(&project, &pipeline)
            .expect("project");

        let trigger = storage
            .create_provider_trigger(project.id, "github", "Acme/widgets", None)
            .expect("provider trigger")
            .expect("new provider trigger");
        assert_eq!(trigger.source_pipeline, None);
        assert!(
            storage
                .create_provider_trigger(project.id, "github", "Acme/widgets", None)
                .expect("duplicate provider trigger")
                .is_none()
        );
        assert_eq!(
            storage
                .matching_provider_triggers(
                    project.id,
                    "github",
                    "Acme/widgets",
                    Some("release.yml")
                )
                .expect("matching trigger"),
            vec![trigger.clone()]
        );
        assert!(
            storage
                .matching_provider_triggers(
                    project.id,
                    "gitlab",
                    "Acme/widgets",
                    Some("release.yml")
                )
                .expect("non-matching provider")
                .is_empty()
        );

        let event_id = "workflow-run-1";
        let timestamp = Utc::now();
        assert!(
            storage
                .claim_provider_trigger_delivery(trigger.id, event_id, timestamp)
                .expect("claim provider event")
        );
        assert!(
            !storage
                .claim_provider_trigger_delivery(trigger.id, event_id, timestamp)
                .expect("duplicate provider event")
        );
        let build = storage
            .create_build(&project, &plan, &pipeline, None)
            .expect("downstream build");
        storage
            .complete_provider_trigger_delivery(trigger.id, event_id, build.id)
            .expect("complete provider event");
        assert!(
            storage
                .claim_provider_trigger_delivery(trigger.id, event_id, timestamp)
                .expect("completed provider event")
                == false
        );

        drop(storage);
        let reopened = Storage::open(&database).expect("reopen");
        assert_eq!(
            reopened
                .list_provider_triggers_to(project.id)
                .expect("reopened provider trigger"),
            vec![trigger.clone()]
        );
        assert!(
            reopened
                .delete_provider_trigger(project.id, trigger.id)
                .expect("delete provider trigger")
        );
        assert!(
            reopened
                .list_provider_triggers_to(project.id)
                .expect("deleted provider trigger")
                .is_empty()
        );
    }

    #[test]
    fn webhook_deliveries_are_idempotent_and_reopenable() {
        let directory = tempdir().expect("tempdir");
        let database = directory.path().join("rivet.db");
        let (project, pipeline, plan) = fixture();
        let storage = Storage::open(&database).expect("open");
        storage
            .create_project(&project, &pipeline)
            .expect("project");
        let received_at = Utc::now();
        assert!(
            storage
                .claim_webhook_delivery("delivery-1", project.id, received_at)
                .expect("claim")
        );
        assert!(
            !storage
                .claim_webhook_delivery("delivery-1", project.id, received_at)
                .expect("duplicate claim")
        );
        let build = storage
            .create_build(&project, &plan, &pipeline, None)
            .expect("build");
        storage
            .complete_webhook_delivery("delivery-1", build.id, build.number)
            .expect("complete");
        let delivery = storage
            .webhook_delivery("delivery-1")
            .expect("lookup")
            .expect("delivery");
        assert_eq!(delivery.project_id, project.id);
        assert_eq!(delivery.build_id, Some(build.id));
        assert_eq!(delivery.build_number, Some(build.number));

        assert!(
            storage
                .claim_webhook_delivery("delivery-2", project.id, received_at)
                .expect("second claim")
        );
        storage
            .release_webhook_delivery("delivery-2")
            .expect("release");
        assert!(
            storage
                .webhook_delivery("delivery-2")
                .expect("released lookup")
                .is_none()
        );

        drop(storage);
        let reopened = Storage::open(&database).expect("reopen");
        assert_eq!(
            reopened
                .webhook_delivery("delivery-1")
                .expect("reopened lookup")
                .expect("reopened delivery")
                .build_number,
            Some(build.number)
        );
    }

    #[test]
    fn repository_poll_transitions_are_idempotent_and_reopenable() {
        let directory = tempdir().expect("tempdir");
        let database = directory.path().join("rivet.db");
        let (project, pipeline, plan) = fixture();
        let storage = Storage::open(&database).expect("open");
        storage
            .create_project(&project, &pipeline)
            .expect("project");
        let build = storage
            .create_build(&project, &plan, &pipeline, None)
            .expect("build");
        let checked_at = Utc::now();

        assert!(
            storage
                .claim_repository_poll_event(
                    "poll-transition-1",
                    project.id,
                    "revision-1",
                    checked_at,
                )
                .expect("claim")
        );
        assert!(
            !storage
                .claim_repository_poll_event(
                    "poll-transition-1",
                    project.id,
                    "revision-1",
                    checked_at,
                )
                .expect("duplicate claim")
        );
        storage
            .complete_repository_poll_event("poll-transition-1", build.id, build.number)
            .expect("complete");

        drop(storage);
        let reopened = Storage::open(&database).expect("reopen");
        assert!(
            !reopened
                .claim_repository_poll_event(
                    "poll-transition-1",
                    project.id,
                    "revision-1",
                    checked_at,
                )
                .expect("reopened duplicate claim")
        );
        assert!(
            reopened
                .claim_repository_poll_event(
                    "poll-transition-2",
                    project.id,
                    "revision-2",
                    Utc::now(),
                )
                .expect("next transition")
        );
        reopened
            .release_repository_poll_event("poll-transition-2")
            .expect("release");
        assert!(
            reopened
                .claim_repository_poll_event(
                    "poll-transition-2",
                    project.id,
                    "revision-2",
                    Utc::now(),
                )
                .expect("reclaimed transition")
        );
    }

    #[test]
    fn audit_events_are_bounded_ordered_and_reopenable() {
        let directory = tempdir().expect("tempdir");
        let database = directory.path().join("rivet.db");
        let storage = Storage::open(&database).expect("open");
        let first_at = Utc
            .with_ymd_and_hms(2026, 9, 13, 12, 0, 0)
            .single()
            .expect("first timestamp");
        let second_at = first_at + chrono::Duration::seconds(1);
        assert_eq!(
            storage
                .append_audit_event(
                    first_at,
                    Some("operator"),
                    "auth.authenticate",
                    "/api/v1/queue",
                    "success",
                )
                .expect("first event"),
            1
        );
        storage
            .append_audit_event(
                second_at,
                None,
                "auth.authenticate",
                "/api/v1/auth/me",
                "failure",
            )
            .expect("second event");
        assert!(matches!(
            storage.append_audit_event(
                second_at,
                None,
                "auth\n.authenticate",
                "/api/v1/auth/me",
                "failure",
            ),
            Err(StorageError::InvalidAuditField { field: "action" })
        ));

        let events = storage.list_audit_events(1).expect("latest events");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].sequence, 2);
        assert_eq!(events[0].actor_id, None);
        assert_eq!(events[0].outcome, "failure");

        drop(storage);
        let reopened = Storage::open(&database).expect("reopen");
        let events = reopened.list_audit_events(10).expect("reopened events");
        assert_eq!(events.len(), 2);
        assert_eq!(events[1].actor_id.as_deref(), Some("operator"));
        assert_eq!(events[1].timestamp, first_at);
    }

    #[test]
    fn auth_sessions_store_only_digests_and_support_expiry_revocation_and_reopen() {
        let directory = tempdir().expect("tempdir");
        let database = directory.path().join("rivet.db");
        let storage = Storage::open(&database).expect("open");
        let created_at = Utc
            .with_ymd_and_hms(2026, 9, 13, 12, 0, 0)
            .single()
            .expect("created timestamp");
        let expires_at = created_at + chrono::Duration::hours(1);
        let digest = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
        let session_id = Uuid::new_v4();
        let projects = vec!["release".to_owned()];
        let record = storage
            .create_auth_session(
                session_id, digest, "operator", "operator", &projects, created_at, expires_at,
            )
            .expect("session");
        assert_eq!(record.id, session_id);
        assert_eq!(
            storage
                .auth_session(digest, created_at + chrono::Duration::minutes(1))
                .expect("lookup")
                .expect("active session")
                .projects,
            projects
        );
        assert!(
            storage
                .auth_session(digest, expires_at)
                .expect("expired lookup")
                .is_none()
        );
        assert!(
            storage
                .revoke_auth_session(digest, created_at + chrono::Duration::minutes(2))
                .expect("revoke")
        );
        assert!(
            storage
                .auth_session(digest, created_at + chrono::Duration::minutes(3))
                .expect("revoked lookup")
                .is_none()
        );

        drop(storage);
        let reopened = Storage::open(&database).expect("reopen");
        assert!(
            reopened
                .auth_session(digest, created_at + chrono::Duration::minutes(3))
                .expect("reopened lookup")
                .is_none()
        );
    }

    #[test]
    fn auth_sessions_reject_invalid_digest_and_role() {
        let storage = Storage::open_in_memory().expect("storage");
        let now = Utc::now();
        let projects = Vec::new();
        assert!(matches!(
            storage.create_auth_session(
                Uuid::new_v4(),
                "not-a-digest",
                "viewer",
                "viewer",
                &projects,
                now,
                now + chrono::Duration::hours(1),
            ),
            Err(StorageError::InvalidSessionDigest)
        ));
        assert!(matches!(
            storage.create_auth_session(
                Uuid::new_v4(),
                "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
                "viewer",
                "root",
                &projects,
                now,
                now + chrono::Duration::hours(1),
            ),
            Err(StorageError::InvalidSessionRole)
        ));
    }

    #[test]
    fn remote_attempts_survive_reopen_and_preserve_incomplete_builds() {
        let directory = tempdir().expect("tempdir");
        let database = directory.path().join("rivet.db");
        let (project, pipeline, _) = fixture();
        let storage = Storage::open(&database).expect("open");
        storage
            .create_project(&project, &pipeline)
            .expect("project");
        let plan = ExecutionPlan::from_pipeline(&pipeline, Uuid::new_v4(), project.id);
        let build = storage
            .create_build(&project, &plan, &pipeline, None)
            .expect("build");
        storage
            .apply_event(&BuildEvent::BuildQueued {
                build_id: build.id,
                project_id: project.id,
                timestamp: Utc::now(),
            })
            .expect("queue build");
        let agent_id = Uuid::new_v4();
        let parameters = BTreeMap::from([("PROFILE".to_owned(), "release".to_owned())]);
        let created = storage
            .create_remote_attempt(
                build.id,
                project.id,
                agent_id,
                r#"{"os":"linux","executors":1}"#,
                &plan,
                &pipeline,
                &parameters,
            )
            .expect("remote attempt");
        assert_eq!(created.plan.build_id, plan.build_id);

        drop(storage);
        let reopened = Storage::open(&database).expect("reopen");
        let attempts = reopened.list_remote_attempts().expect("attempts");
        assert_eq!(attempts.len(), 1);
        assert_eq!(attempts[0].build_id, build.id);
        assert_eq!(attempts[0].attempt_id, created.attempt_id);
        assert_eq!(attempts[0].agent_id, agent_id);
        assert_eq!(attempts[0].recovery_attempts, 0);
        assert_eq!(attempts[0].parameters, parameters);
        let remote_event = BuildEvent::BuildStarted {
            build_id: build.id,
            timestamp: Utc::now(),
        };
        assert!(
            reopened
                .apply_remote_event(build.id, created.attempt_id, 1, &remote_event)
                .expect("apply remote event")
        );
        assert!(
            !reopened
                .apply_remote_event(build.id, created.attempt_id, 1, &remote_event)
                .expect("deduplicate remote event")
        );
        let conflicting_event = BuildEvent::BuildStarted {
            build_id: build.id,
            timestamp: Utc::now() + chrono::Duration::seconds(1),
        };
        assert!(matches!(
            reopened.apply_remote_event(build.id, created.attempt_id, 1, &conflicting_event),
            Err(StorageError::RemoteEventSequenceConflict { sequence: 1 })
        ));
        assert_eq!(
            reopened
                .get_build_details(build.id)
                .expect("details")
                .expect("build")
                .build
                .status,
            BuildStatus::Running
        );
        let replacement_agent = Uuid::new_v4();
        let replacement_attempt = Uuid::new_v4();
        assert_eq!(
            reopened
                .advance_remote_attempt(build.id, replacement_agent, replacement_attempt, 1)
                .expect("advance remote attempt"),
            Some(1)
        );
        assert_eq!(
            reopened
                .advance_remote_attempt(build.id, Uuid::new_v4(), Uuid::new_v4(), 1)
                .expect("bounded remote attempt"),
            None
        );
        let advanced = reopened.list_remote_attempts().expect("advanced attempt");
        assert_eq!(advanced[0].agent_id, replacement_agent);
        assert_eq!(advanced[0].attempt_id, replacement_attempt);
        assert_eq!(advanced[0].recovery_attempts, 1);
        assert!(
            reopened
                .recover_incomplete_builds_except(Utc::now(), &BTreeSet::from([build.id]),)
                .expect("preserve remote build")
                .is_empty()
        );
        assert_eq!(
            reopened
                .get_build_details(build.id)
                .expect("details")
                .expect("build")
                .build
                .status,
            BuildStatus::Running
        );
        assert!(reopened.delete_remote_attempt(build.id).expect("delete"));
        assert!(
            reopened
                .list_remote_attempts()
                .expect("empty attempts")
                .is_empty()
        );
    }

    #[test]
    fn remote_attempts_never_persist_secret_parameter_values() {
        let storage = Storage::open_in_memory().expect("storage");
        let project = Project::new("secret-remote", ".", "Rivetfile.toml").expect("project");
        let pipeline = Pipeline::from_toml_str(
            r#"
version = 1
name = "secret-remote"
parameters = [{ name = "TOKEN", secret = true }]
[[stages]]
name = "Test"
[[stages.steps]]
name = "unit"
program = "true"
"#,
        )
        .expect("pipeline");
        storage
            .create_project(&project, &pipeline)
            .expect("project");
        let plan = ExecutionPlan::from_pipeline(&pipeline, Uuid::new_v4(), project.id);
        let build = storage
            .create_build_with_parameters(
                &project,
                &plan,
                &pipeline,
                None,
                &BTreeMap::from([("TOKEN".to_owned(), "do-not-store".to_owned())]),
            )
            .expect("build");
        let record = storage
            .create_remote_attempt(
                build.id,
                project.id,
                Uuid::new_v4(),
                r#"{"os":"linux"}"#,
                &plan,
                &pipeline,
                &BTreeMap::from([("TOKEN".to_owned(), "do-not-store".to_owned())]),
            )
            .expect("remote attempt");
        let serialized = serde_json::to_string(&record).expect("record JSON");
        assert!(!serialized.contains("do-not-store"));
        assert_eq!(
            record.parameters.get("TOKEN").map(String::as_str),
            Some(rivet_core::REDACTED_PARAMETER_VALUE)
        );
    }

    #[test]
    fn restart_recovery_closes_incomplete_builds_and_is_idempotent() {
        let directory = tempdir().expect("tempdir");
        let database = directory.path().join("rivet.db");
        let (project, pipeline, _) = fixture();
        let storage = Storage::open(&database).expect("open");
        storage
            .create_project(&project, &pipeline)
            .expect("project");

        let queued_plan = ExecutionPlan::from_pipeline(&pipeline, Uuid::new_v4(), project.id);
        let queued = storage
            .create_build(&project, &queued_plan, &pipeline, None)
            .expect("queued build");
        storage
            .apply_event(&BuildEvent::BuildQueued {
                build_id: queued.id,
                project_id: project.id,
                timestamp: Utc::now(),
            })
            .expect("queue build");

        let running_plan = ExecutionPlan::from_pipeline(&pipeline, Uuid::new_v4(), project.id);
        let running = storage
            .create_build(&project, &running_plan, &pipeline, None)
            .expect("running build");
        let stage = &running_plan.stages[0];
        let step = &stage.steps[0];
        storage
            .apply_event(&BuildEvent::BuildQueued {
                build_id: running.id,
                project_id: project.id,
                timestamp: Utc::now(),
            })
            .expect("queue running build");
        storage
            .apply_event(&BuildEvent::BuildStarted {
                build_id: running.id,
                timestamp: Utc::now(),
            })
            .expect("start running build");
        storage
            .apply_event(&BuildEvent::StageStarted {
                build_id: running.id,
                stage_id: stage.id,
                stage_name: stage.name.clone(),
                timestamp: Utc::now(),
            })
            .expect("start stage");
        storage
            .apply_event(&BuildEvent::StepStarted {
                build_id: running.id,
                stage_id: stage.id,
                step_id: step.id,
                step_name: step.definition.name.clone(),
                timestamp: Utc::now(),
            })
            .expect("start step");

        let recovered_at = Utc::now();
        assert_eq!(
            storage
                .recover_incomplete_builds(recovered_at)
                .expect("recover")
                .len(),
            2
        );
        assert!(
            storage
                .recover_incomplete_builds(recovered_at)
                .expect("idempotent recovery")
                .is_empty()
        );

        let queued_details = storage
            .get_build_details(queued.id)
            .expect("queued details")
            .expect("queued build exists");
        assert_eq!(queued_details.build.status, BuildStatus::Cancelled);
        assert!(queued_details.stages[0].stage.status.is_terminal());
        assert!(queued_details.stages[0].steps[0].status.is_terminal());

        let running_details = storage
            .get_build_details(running.id)
            .expect("running details")
            .expect("running build exists");
        assert_eq!(running_details.build.status, BuildStatus::Failed);
        assert_eq!(
            running_details.stages[0].steps[0].status,
            StepStatus::Cancelled
        );
        assert_eq!(
            running_details.stages[0].stage.status,
            StageStatus::Cancelled
        );

        drop(storage);
        let reopened = Storage::open(&database).expect("reopen");
        assert!(
            reopened
                .list_incomplete_builds()
                .expect("incomplete")
                .is_empty()
        );
        assert!(
            reopened
                .events(running.id)
                .expect("replayed events")
                .iter()
                .any(|event| matches!(
                    event,
                    BuildEvent::BuildFinished {
                        status: BuildStatus::Failed,
                        ..
                    }
                ))
        );
    }
}
