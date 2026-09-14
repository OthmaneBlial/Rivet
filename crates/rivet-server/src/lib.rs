//! Headless Rivet HTTP/WebSocket service.
//!
//! The service is deliberately a thin transport layer. Pipeline validation,
//! queueing, process supervision, and state projection remain in the shared
//! crates so desktop and server modes execute the same code.

use axum::body::{Body, Bytes};
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{DefaultBodyLimit, Extension, Path as AxumPath, Query, State};
use axum::http::{HeaderMap, HeaderName, HeaderValue, Method, StatusCode, header};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, patch, post, put};
use axum::{Json, Router};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use chrono::{DateTime, Utc};
use hmac::{Hmac, Mac};
use rivet_agent_protocol::{
    AgentMessage, AgentRequirements, AgentTransportMessage, MAX_WORKSPACE_CHUNK_BYTES,
    PROTOCOL_VERSION, WorkspaceTransfer,
};
use rivet_auth::{
    AuthError, AuthPolicy, AuthUsers, Permission, Principal, Role, generate_token, token_digest,
};
use rivet_core::{
    BuildEvent, BuildId, BuildStatus, CronExpression, ExecutionPlan, ParameterKind, Pipeline,
    Project, ScheduleId, SourceSnapshot,
};
use rivet_credentials::{
    CredentialError, CredentialKeychain, CredentialKind, CredentialSummary, CredentialVault,
};
use rivet_extension_protocol::{
    ExtensionCatalog, ExtensionCatalogError, ExtensionManager, ExtensionManagerError,
    ExtensionManifest, ExtensionPermission,
};
use rivet_runner::{MAX_QUEUE_PRIORITY, MIN_QUEUE_PRIORITY, QueueHandle, QueueStats, Scheduler};
use rivet_scm::{
    GitCloneOptions, GitCredential, GitHttpCredential, GitPrepareOptions, GitRepository,
    GitSnapshot, GitSshCredential, ScmError, validate_known_hosts_file,
};
use rivet_storage::{
    AnnotationRecord, ArtifactRecord, AuditEventRecord, BuildDetails, BuildRecord,
    RemoteAttemptRecord, SchedulePollConfig, ScheduleRecord, ScheduleTrigger, Storage,
    StorageError,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::io::{Read, Seek, SeekFrom};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;
use subtle::ConstantTimeEq;
use thiserror::Error;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio::sync::{Mutex, broadcast, mpsc};
use tokio::time::{Duration, MissedTickBehavior};
use tokio_util::sync::CancellationToken;
use tower_http::cors::{AllowOrigin, CorsLayer};
use uuid::Uuid;

mod agent_registry;
mod workspace_archive;

use agent_registry::{
    AgentLease, AgentRegistry, AgentRegistryError, AgentReservation, AgentSummary,
};
use workspace_archive::archive_workspace;

const DEFAULT_ALLOWED_ORIGINS: [&str; 7] = [
    "http://127.0.0.1:1420",
    "http://localhost:1420",
    "http://127.0.0.1:1421",
    "http://localhost:1421",
    "tauri://localhost",
    "http://tauri.localhost",
    "https://tauri.localhost",
];

#[derive(Clone)]
pub struct AppState {
    pub storage: Storage,
    scheduler: Arc<Scheduler>,
    active_builds: Arc<Mutex<HashMap<BuildId, CancellationToken>>>,
    events: broadcast::Sender<BuildEvent>,
    shutdown: CancellationToken,
    auth_digest: Option<[u8; 32]>,
    auth_policy: Option<Arc<AuthPolicy>>,
    auth_users: Option<Arc<AuthUsers>>,
    webhook_secret: Option<Vec<u8>>,
    github_webhook_secret: Option<Vec<u8>>,
    gitlab_webhook_secret: Option<Vec<u8>>,
    bitbucket_webhook_secret: Option<Vec<u8>>,
    github_webhook_credential_id: Option<String>,
    gitlab_webhook_credential_id: Option<String>,
    bitbucket_webhook_credential_id: Option<String>,
    credentials: Option<Arc<Mutex<CredentialVault>>>,
    ssh_known_hosts_file: Option<PathBuf>,
    extensions: Arc<ExtensionCatalog>,
    extension_manager: Option<Arc<ExtensionManager>>,
    agents: AgentRegistry,
    remote_messages: Arc<Mutex<HashMap<BuildId, RemoteBuildRoute>>>,
}

#[derive(Clone)]
struct RemoteBuildRoute {
    agent_id: rivet_agent_protocol::AgentId,
    session_id: Uuid,
    messages: mpsc::Sender<AgentMessage>,
}

const MAX_AGENT_PENDING_DELIVERIES: usize = 8192;
const MAX_AGENT_RECEIVED_DELIVERIES: usize = 8192;
const AGENT_RETRANSMIT_AFTER: Duration = Duration::from_secs(2);
const MAX_AGENT_RETRANSMITS: u8 = 5;
const MAX_EXTENSION_BUILD_RECORDS: usize = 100;
const MAX_EXTENSION_STAGE_RECORDS: usize = 100;
const MAX_EXTENSION_STEP_RECORDS: usize = 500;
const MAX_EXTENSION_LOG_RECORDS: usize = 500;
const MAX_EXTENSION_ARTIFACT_RECORDS: usize = 100;
const MAX_EXTENSION_ANNOTATION_RECORDS: usize = 100;
const MAX_EXTENSION_TRIGGER_PARAMETERS: usize = 64;
const DEFAULT_LOG_PAGE_SIZE: usize = 2_000;
const MAX_LOG_PAGE_SIZE: usize = 10_000;

struct PendingAgentDelivery {
    envelope: AgentTransportMessage,
    last_sent: Instant,
    retransmits: u8,
}

#[derive(Default)]
struct AgentWireState {
    pending: HashMap<Uuid, PendingAgentDelivery>,
    received: HashSet<Uuid>,
}

impl AgentWireState {
    fn accept_incoming(&mut self, delivery_id: Uuid) -> Result<bool, ()> {
        if self.received.contains(&delivery_id) {
            return Ok(false);
        }
        if self.received.len() >= MAX_AGENT_RECEIVED_DELIVERIES {
            return Err(());
        }
        self.received.insert(delivery_id);
        Ok(true)
    }

    fn acknowledge(&mut self, delivery_id: Uuid) {
        self.pending.remove(&delivery_id);
    }

    async fn send_message(&mut self, socket: &mut WebSocket, payload: AgentMessage) -> bool {
        self.send_pending(socket, AgentTransportMessage::message(payload))
            .await
    }

    async fn send_pending(
        &mut self,
        socket: &mut WebSocket,
        envelope: AgentTransportMessage,
    ) -> bool {
        if self.pending.len() >= MAX_AGENT_PENDING_DELIVERIES {
            return false;
        }
        let delivery_id = envelope.delivery_id();
        if send_agent_transport(socket, &envelope).await.is_err() {
            return false;
        }
        self.pending.insert(
            delivery_id,
            PendingAgentDelivery {
                envelope,
                last_sent: Instant::now(),
                retransmits: 0,
            },
        );
        true
    }

    async fn send_ack(&self, socket: &mut WebSocket, delivery_id: Uuid) -> bool {
        send_agent_transport(socket, &AgentTransportMessage::ack(delivery_id))
            .await
            .is_ok()
    }

    async fn retransmit_due(&mut self, socket: &mut WebSocket) -> bool {
        let due = self
            .pending
            .iter()
            .filter(|(_, delivery)| delivery.last_sent.elapsed() >= AGENT_RETRANSMIT_AFTER)
            .map(|(delivery_id, _)| *delivery_id)
            .collect::<Vec<_>>();
        for delivery_id in due {
            let Some(delivery) = self.pending.get_mut(&delivery_id) else {
                continue;
            };
            if delivery.retransmits >= MAX_AGENT_RETRANSMITS {
                return false;
            }
            if send_agent_transport(socket, &delivery.envelope)
                .await
                .is_err()
            {
                return false;
            }
            delivery.last_sent = Instant::now();
            delivery.retransmits += 1;
        }
        true
    }
}

#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub bind: SocketAddr,
    pub auth_token: Option<String>,
    pub auth_policy_file: Option<PathBuf>,
    /// Optional private local-account policy with Argon2id password hashes.
    pub auth_users_file: Option<PathBuf>,
    pub webhook_secret: Option<String>,
    pub github_webhook_secret: Option<String>,
    pub gitlab_webhook_secret: Option<String>,
    pub bitbucket_webhook_secret: Option<String>,
    pub github_webhook_credential_id: Option<String>,
    pub gitlab_webhook_credential_id: Option<String>,
    pub bitbucket_webhook_credential_id: Option<String>,
    pub credentials_file: Option<PathBuf>,
    pub credentials_passphrase: Option<String>,
    pub credentials_keychain_account: Option<String>,
    /// Optional deployment-specific OS keychain service for the vault
    /// passphrase. The default service remains `Rivet` for compatibility.
    pub credentials_keychain_service: Option<String>,
    /// Optional deployment-controlled OpenSSH trust root for SSH SCM fetches.
    /// When absent, OpenSSH's normal system/user known-hosts files apply.
    pub ssh_known_hosts_file: Option<PathBuf>,
    pub extension_manifest_dir: Option<PathBuf>,
    pub allowed_origins: Vec<String>,
}

#[derive(Debug, Error)]
pub enum ServerError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error("could not bind Rivet server: {0}")]
    Bind(#[from] std::io::Error),
    #[error("binding Rivet outside loopback requires an authentication token or policy file: {0}")]
    AuthRequired(SocketAddr),
    #[error("Rivet authentication token cannot be empty")]
    EmptyAuthToken,
    #[error("could not read authentication policy file {path}: {message}")]
    AuthPolicyFile { path: PathBuf, message: String },
    #[error("authentication policy is invalid: {0}")]
    AuthPolicy(#[from] AuthError),
    #[error("could not read authentication users file {path}: {message}")]
    AuthUsersFile { path: PathBuf, message: String },
    #[error("authentication users policy is invalid: {0}")]
    AuthUsers(AuthError),
    #[error("Rivet webhook secret cannot be empty")]
    EmptyWebhookSecret,
    #[error("{provider} webhook secret cannot be empty")]
    EmptyProviderWebhookSecret { provider: &'static str },
    #[error("{provider} webhook credential ID cannot be empty")]
    EmptyProviderWebhookCredential { provider: &'static str },
    #[error("credential vault configuration requires both a file and a passphrase")]
    IncompleteCredentialVaultConfig,
    #[error("invalid SSH known-hosts file: {0}")]
    InvalidSshKnownHostsFile(String),
    #[error("could not open credential vault: {0}")]
    Credentials(#[from] CredentialError),
    #[error("could not load extension catalog: {0}")]
    Extensions(#[from] ExtensionCatalogError),
    #[error("could not initialize extension manager: {0}")]
    ExtensionManager(#[from] ExtensionManagerError),
    #[error("invalid allowed origin: {0}")]
    InvalidAllowedOrigin(String),
}

#[derive(Debug, Error)]
enum ApiError {
    #[error("project not found: {0}")]
    ProjectNotFound(String),
    #[error("build not found: {project} #{number}")]
    BuildNotFound { project: String, number: i64 },
    #[error("schedule not found: {project} {schedule_id}")]
    ScheduleNotFound {
        project: String,
        schedule_id: ScheduleId,
    },
    #[error("repository path is not a directory: {0}")]
    InvalidRepository(PathBuf),
    #[error("pipeline path is not a file: {0}")]
    InvalidPipeline(PathBuf),
    #[error("artifact {0} was not found")]
    ArtifactNotFound(uuid::Uuid),
    #[error("could not read artifact: {0}")]
    ArtifactRead(#[source] std::io::Error),
    #[error("artifact contents failed integrity verification")]
    ArtifactIntegrity,
    #[error("credential vault is not configured")]
    CredentialsUnavailable,
    #[error("local user authentication is not configured")]
    AuthUsersUnavailable,
    #[error("invalid username or password")]
    InvalidCredentials,
    #[error("extension runtime is not configured")]
    ExtensionsUnavailable,
    #[error(transparent)]
    ExtensionManager(#[from] ExtensionManagerError),
    #[error(transparent)]
    Credentials(#[from] CredentialError),
    #[error("{0}")]
    BadRequest(String),
    #[error("permission denied: {0}")]
    Forbidden(String),
    #[error("webhook delivery signatures are not configured")]
    WebhookNotConfigured,
    #[error("GitHub webhook delivery signatures are not configured")]
    GitHubWebhookNotConfigured,
    #[error("GitLab webhook delivery signatures are not configured")]
    GitLabWebhookNotConfigured,
    #[error("Bitbucket webhook delivery signatures are not configured")]
    BitbucketWebhookNotConfigured,
    #[error("invalid webhook signature")]
    InvalidWebhookSignature,
    #[error("webhook event ID cannot be empty")]
    EmptyWebhookEventId,
    #[error("webhook event ID is too long")]
    WebhookEventIdTooLong,
    #[error("webhook event ID contains control characters")]
    InvalidWebhookEventId,
    #[error("webhook event ID already belongs to another project")]
    WebhookEventConflict,
    #[error(transparent)]
    Migration(#[from] rivet_migration::MigrationError),
    #[error(transparent)]
    Auth(#[from] AuthError),
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Pipeline(#[from] rivet_core::PipelineError),
    #[error(transparent)]
    Model(#[from] rivet_core::ModelError),
    #[error(transparent)]
    Scheduler(#[from] rivet_runner::SchedulerError),
    #[error(transparent)]
    Scm(#[from] rivet_scm::ScmError),
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = match &self {
            Self::ProjectNotFound(_)
            | Self::BuildNotFound { .. }
            | Self::ScheduleNotFound { .. } => StatusCode::NOT_FOUND,
            Self::InvalidRepository(_) | Self::InvalidPipeline(_) | Self::BadRequest(_) => {
                StatusCode::BAD_REQUEST
            }
            Self::Forbidden(_) => StatusCode::FORBIDDEN,
            Self::WebhookNotConfigured
            | Self::GitHubWebhookNotConfigured
            | Self::GitLabWebhookNotConfigured
            | Self::BitbucketWebhookNotConfigured => StatusCode::SERVICE_UNAVAILABLE,
            Self::InvalidWebhookSignature => StatusCode::UNAUTHORIZED,
            Self::EmptyWebhookEventId
            | Self::WebhookEventIdTooLong
            | Self::InvalidWebhookEventId => StatusCode::BAD_REQUEST,
            Self::WebhookEventConflict => StatusCode::CONFLICT,
            Self::ArtifactNotFound(_) => StatusCode::NOT_FOUND,
            Self::ArtifactRead(_) | Self::ArtifactIntegrity => StatusCode::INTERNAL_SERVER_ERROR,
            Self::CredentialsUnavailable => StatusCode::SERVICE_UNAVAILABLE,
            Self::AuthUsersUnavailable => StatusCode::SERVICE_UNAVAILABLE,
            Self::InvalidCredentials => StatusCode::UNAUTHORIZED,
            Self::ExtensionsUnavailable => StatusCode::SERVICE_UNAVAILABLE,
            Self::ExtensionManager(_) => StatusCode::BAD_REQUEST,
            Self::Credentials(error) => match error {
                CredentialError::CredentialNotFound(_) => StatusCode::NOT_FOUND,
                CredentialError::InvalidId(_)
                | CredentialError::InvalidUsername
                | CredentialError::InvalidProject(_)
                | CredentialError::TooManyProjects
                | CredentialError::InvalidKeychainLabel(_)
                | CredentialError::EmptySecret
                | CredentialError::InvalidSecret
                | CredentialError::WeakPassphrase => StatusCode::BAD_REQUEST,
                CredentialError::SymlinkPath(_)
                | CredentialError::VaultNotFound(_)
                | CredentialError::VaultNotAFile(_)
                | CredentialError::InvalidFormat(_)
                | CredentialError::Cryptography
                | CredentialError::Randomness(_)
                | CredentialError::Serialization(_)
                | CredentialError::Filesystem(_)
                | CredentialError::Keychain(_) => StatusCode::INTERNAL_SERVER_ERROR,
            },
            Self::Scm(error) => match error {
                rivet_scm::ScmError::InvalidRepository(_)
                | rivet_scm::ScmError::NotGitRepository(_)
                | rivet_scm::ScmError::InvalidCloneDestination(_)
                | rivet_scm::ScmError::InvalidCredential(_)
                | rivet_scm::ScmError::InvalidHostKeyPolicy(_) => StatusCode::BAD_REQUEST,
                rivet_scm::ScmError::Command { .. }
                | rivet_scm::ScmError::InvalidOutput { .. }
                | rivet_scm::ScmError::Filesystem(_) => StatusCode::INTERNAL_SERVER_ERROR,
            },
            Self::Migration(_)
            | Self::Auth(_)
            | Self::Storage(_)
            | Self::Model(_)
            | Self::Scheduler(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Self::Pipeline(error) => match error {
                rivet_core::PipelineError::UnknownParameter(_)
                | rivet_core::PipelineError::MissingParameter(_)
                | rivet_core::PipelineError::InvalidParameterValue { .. } => {
                    StatusCode::BAD_REQUEST
                }
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            },
        };
        (status, Json(json!({ "error": self.to_string() }))).into_response()
    }
}

#[derive(Debug, Serialize)]
struct HealthResponse {
    status: &'static str,
    service: &'static str,
    timestamp: chrono::DateTime<Utc>,
}

#[derive(Debug, Serialize)]
struct ReadinessResponse {
    status: &'static str,
    service: &'static str,
    storage: &'static str,
    timestamp: chrono::DateTime<Utc>,
}

#[derive(Debug, Serialize)]
struct AuthMeResponse {
    id: String,
    role: rivet_auth::Role,
    projects: Vec<String>,
    local_mode: bool,
}

#[derive(Debug, Serialize)]
struct AuthSessionResponse {
    id: uuid::Uuid,
    session_token: String,
    expires_at: DateTime<Utc>,
}

#[derive(Debug, Deserialize)]
struct AuthLoginRequest {
    username: String,
    password: String,
}

const AUTH_SESSION_TTL_SECONDS: i64 = 12 * 60 * 60;

#[derive(Debug, Deserialize)]
pub struct CreateProjectRequest {
    pub name: String,
    #[serde(default)]
    pub repository_path: PathBuf,
    pub pipeline_path: Option<PathBuf>,
    /// Optional remote repository to clone before registering the project.
    /// The clone destination must be supplied explicitly; Rivet never chooses
    /// a server-side path implicitly.
    #[serde(default)]
    pub repository_url: Option<String>,
    #[serde(default)]
    pub clone_destination: Option<PathBuf>,
    #[serde(default)]
    pub branch: Option<String>,
    #[serde(default)]
    pub depth: Option<u32>,
    #[serde(default)]
    pub revision: Option<String>,
    #[serde(default)]
    pub submodules: bool,
    /// Non-secret vault reference resolved only for the duration of Git.
    #[serde(default)]
    pub credential_id: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
pub struct QueueBuildRequest {
    #[serde(default)]
    pub scm: Option<PrepareScmRequest>,
    #[serde(default)]
    pub parameters: BTreeMap<String, String>,
    /// Higher values are admitted before older lower-priority builds.
    #[serde(default)]
    pub priority: i32,
}

#[derive(Debug, Deserialize, Default, Clone)]
pub struct RepositoryPollRequest {
    /// Remote to fetch when `fetch` is enabled.
    #[serde(default = "default_remote")]
    pub remote: String,
    /// Fetch the selected remote before inspecting the checkout.
    #[serde(default)]
    pub fetch: bool,
    /// Non-secret ID resolved from the server's encrypted credential vault.
    #[serde(default)]
    pub credential_id: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct CreateAnnotationRequest {
    pub kind: String,
    pub message: String,
    #[serde(default)]
    pub stage_id: Option<Uuid>,
}

#[derive(Debug, Deserialize)]
pub struct CreateScheduleRequest {
    pub name: String,
    pub expression: String,
    #[serde(default)]
    pub trigger: ScheduleTrigger,
    /// Poll options are only accepted for repository_poll schedules. The
    /// credential is an opaque vault ID; its secret never enters the record.
    #[serde(default)]
    pub poll: Option<RepositoryPollRequest>,
    #[serde(default = "default_schedule_enabled")]
    pub enabled: bool,
}

#[derive(Debug, Deserialize)]
pub struct UpdateScheduleRequest {
    pub enabled: bool,
}

#[derive(Deserialize)]
struct CredentialWriteRequest {
    #[serde(default)]
    kind: CredentialKind,
    username: String,
    secret: String,
    /// Empty keeps the credential globally usable for backwards-compatible
    /// vaults. Non-empty values restrict SCM resolution to these projects.
    #[serde(default)]
    projects: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct WebhookBuildRequest {
    /// A provider delivery ID. Re-deliveries with the same ID are ignored.
    pub event_id: String,
    /// The Rivet project receiving the build.
    pub project: String,
    #[serde(default)]
    pub revision: Option<String>,
    /// Optional provider refspec fetched before checking out the revision.
    #[serde(default)]
    pub fetch_ref: Option<String>,
    #[serde(default)]
    pub remote: Option<String>,
    #[serde(default)]
    pub fetch: bool,
    #[serde(default)]
    pub credential_id: Option<String>,
    /// Initialize and recursively update Git submodules after preparation.
    #[serde(default)]
    pub submodules: bool,
    #[serde(default)]
    pub parameters: BTreeMap<String, String>,
    /// Optional upstream build proof used by the signed generic webhook
    /// adapter. Downstream builds are admitted only after an explicit
    /// `passed` upstream status.
    #[serde(default)]
    pub upstream: Option<UpstreamBuildReference>,
}

#[derive(Debug, Deserialize)]
pub struct UpstreamBuildReference {
    pub project: String,
    pub build: i64,
    pub status: String,
}

#[derive(Debug, Deserialize)]
pub struct JenkinsfileAnalysisRequest {
    pub source: String,
    #[serde(default)]
    pub draft: bool,
}

#[derive(Debug, Deserialize, Default)]
pub struct AgentMatchRequest {
    #[serde(flatten)]
    pub requirements: AgentRequirements,
}

#[derive(Debug, Serialize)]
struct JenkinsfileAnalysisResponse {
    analysis: rivet_migration::JenkinsfileAnalysis,
    #[serde(skip_serializing_if = "Option::is_none")]
    draft: Option<rivet_migration::RivetfileDraft>,
}

fn default_schedule_enabled() -> bool {
    true
}

#[derive(Debug, Serialize)]
struct QueueBuildResponse {
    build: BuildRecord,
    status: BuildStatus,
}

#[derive(Debug, Serialize)]
struct WebhookBuildResponse {
    status: &'static str,
    deduplicated: bool,
    build: Option<BuildRecord>,
}

#[derive(Debug, Serialize)]
struct RepositoryPollResponse {
    status: &'static str,
    changed: bool,
    deduplicated: bool,
    revision: String,
    reference: Option<String>,
    build: Option<BuildRecord>,
}

#[derive(Debug, Serialize)]
struct QueueStatusResponse {
    queued: usize,
    running: usize,
    capacity: usize,
    paused: bool,
}

#[derive(Debug, Serialize)]
struct ShutdownResponse {
    status: &'static str,
}

#[derive(Debug, Serialize)]
struct QueueItemResponse {
    build_id: BuildId,
    project_id: rivet_core::ProjectId,
    project: String,
    priority: i32,
    position: usize,
}

#[derive(Debug, Serialize)]
struct PipelineParameterResponse {
    name: String,
    kind: ParameterKind,
    secret: bool,
    default: Option<String>,
    required: bool,
    choices: Vec<String>,
}

#[derive(Debug, Deserialize, Default)]
struct LogQuery {
    #[serde(default)]
    after: Option<i64>,
    #[serde(default)]
    limit: Option<usize>,
}

#[derive(Debug, Serialize)]
struct ExtensionRuntimeStatusResponse {
    id: String,
    active: bool,
    runtime_available: bool,
}

#[derive(Debug, Deserialize)]
struct ExtensionRequestInput {
    permission: ExtensionPermission,
    method: String,
    payload: Value,
}

pub fn router(state: AppState) -> Router {
    router_with_origins(state, &default_allowed_origins())
        .expect("default Rivet origins must be valid")
}

fn router_with_origins(state: AppState, allowed_origins: &[String]) -> Result<Router, ServerError> {
    let cors = cors_layer(allowed_origins)?;
    Ok(Router::new()
        .route("/api/v1/health", get(health))
        .route("/api/v1/ready", get(readiness))
        .route("/api/v1/metrics", get(metrics))
        .route("/api/v1/admin/shutdown", post(request_shutdown))
        .route("/api/v1/auth/login", post(login))
        .route("/api/v1/auth/me", get(auth_me))
        .route("/api/v1/auth/sessions", post(create_session))
        .route(
            "/api/v1/auth/sessions/current",
            axum::routing::delete(revoke_session),
        )
        .route("/api/v1/audit", get(list_audit))
        .route("/api/v1/credentials", get(list_credentials))
        .route(
            "/api/v1/credentials/{id}",
            put(set_credential).delete(delete_credential),
        )
        .route("/api/v1/extensions", get(list_extensions))
        .route("/api/v1/extensions/status", get(extension_status))
        .route("/api/v1/extensions/{id}/start", post(start_extension))
        .route("/api/v1/extensions/{id}/stop", post(stop_extension))
        .route("/api/v1/extensions/{id}/request", post(request_extension))
        .route("/api/v1/agents", get(list_agents))
        .route("/api/v1/agents/match", post(match_agents))
        .route("/api/v1/agents/connect", get(connect_agent))
        .route("/api/v1/migration/jenkinsfile", post(analyze_jenkinsfile))
        .route("/api/v1/webhooks/generic", post(webhook_build))
        .route("/api/v1/webhooks/github/{project}", post(github_webhook))
        .route("/api/v1/webhooks/gitlab/{project}", post(gitlab_webhook))
        .route(
            "/api/v1/webhooks/bitbucket/{project}",
            post(bitbucket_webhook),
        )
        .route("/api/v1/queue", get(queue_status))
        .route("/api/v1/queue/items", get(queue_items))
        .route("/api/v1/queue/pause", post(pause_queue))
        .route("/api/v1/queue/resume", post(resume_queue))
        .route("/api/v1/projects", get(list_projects).post(create_project))
        .route(
            "/api/v1/projects/{name}/parameters",
            get(list_pipeline_parameters),
        )
        .route(
            "/api/v1/projects/{name}/builds",
            get(list_builds).post(queue_build),
        )
        .route(
            "/api/v1/projects/{name}/repository-changes",
            post(poll_repository_changes),
        )
        .route("/api/v1/projects/{name}/builds/{number}", get(get_build))
        .route(
            "/api/v1/projects/{name}/schedules",
            get(list_schedules).post(create_schedule),
        )
        .route(
            "/api/v1/projects/{name}/schedules/{schedule_id}",
            patch(update_schedule).delete(delete_schedule),
        )
        .route(
            "/api/v1/projects/{name}/builds/{number}/logs",
            get(get_logs),
        )
        .route(
            "/api/v1/projects/{name}/builds/{number}/annotations",
            get(list_annotations).post(create_annotation),
        )
        .route(
            "/api/v1/projects/{name}/builds/{number}/artifacts",
            get(get_artifacts),
        )
        .route(
            "/api/v1/projects/{name}/builds/{number}/artifacts/{artifact_id}",
            get(download_artifact),
        )
        .route(
            "/api/v1/projects/{name}/builds/{number}/events",
            get(build_events),
        )
        .route(
            "/api/v1/projects/{name}/builds/{number}/cancel",
            post(cancel_build),
        )
        .route(
            "/api/v1/projects/{name}/builds/{number}/retry",
            post(retry_build),
        )
        .route(
            "/api/v1/projects/{name}/scm",
            get(get_scm).post(prepare_scm),
        )
        // The default server binds only to loopback and is consumed by the
        // local Tauri webview. Remote deployments should put an explicit
        // authenticated reverse proxy in front of this transport before
        // widening the bind address.
        .layer(DefaultBodyLimit::max(256 * 1024))
        .layer(cors)
        .layer(middleware::from_fn(security_headers))
        .layer(middleware::from_fn_with_state(state.clone(), authenticate))
        .layer(middleware::from_fn(request_id))
        .with_state(state))
}

fn default_allowed_origins() -> Vec<String> {
    DEFAULT_ALLOWED_ORIGINS
        .iter()
        .map(|origin| (*origin).to_owned())
        .collect()
}

fn cors_layer(allowed_origins: &[String]) -> Result<CorsLayer, ServerError> {
    let origins = if allowed_origins.is_empty() {
        default_allowed_origins()
    } else {
        allowed_origins.to_vec()
    };
    let mut values = Vec::with_capacity(origins.len());
    for origin in origins {
        let origin = origin.trim();
        if origin.is_empty() || origin == "*" {
            return Err(ServerError::InvalidAllowedOrigin(origin.to_owned()));
        }
        values.push(
            HeaderValue::from_str(origin)
                .map_err(|_| ServerError::InvalidAllowedOrigin(origin.to_owned()))?,
        );
    }
    Ok(CorsLayer::new()
        .allow_origin(AllowOrigin::list(values))
        .allow_methods([
            Method::GET,
            Method::POST,
            Method::PATCH,
            Method::DELETE,
            Method::OPTIONS,
        ])
        .allow_headers([
            header::AUTHORIZATION,
            header::CONTENT_TYPE,
            HeaderName::from_static("x-rivet-signature"),
            HeaderName::from_static("x-request-id"),
        ]))
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct PrepareScmRequest {
    #[serde(default = "default_remote")]
    pub remote: String,
    #[serde(default)]
    pub fetch: bool,
    pub revision: Option<String>,
    /// Optional bounded provider refspec fetched before checking out revision.
    #[serde(default)]
    pub fetch_ref: Option<String>,
    #[serde(default)]
    pub clean: bool,
    #[serde(default)]
    pub clean_ignored: bool,
    /// Initialize and recursively update Git submodules after preparation.
    #[serde(default)]
    pub submodules: bool,
    /// Non-secret ID resolved from the server's encrypted credential vault.
    #[serde(default)]
    pub credential_id: Option<String>,
}

fn default_remote() -> String {
    "origin".to_owned()
}

async fn get_scm(
    State(state): State<AppState>,
    AxumPath(name): AxumPath<String>,
    Extension(principal): Extension<Principal>,
) -> Result<Json<GitSnapshot>, ApiError> {
    require_project(&principal, Permission::Read, &name)?;
    let project = project_by_name(&state.storage, &name)?;
    let repository = GitRepository::open(project.repository_path).await?;
    Ok(Json(repository.inspect().await?))
}

async fn prepare_scm(
    State(state): State<AppState>,
    AxumPath(name): AxumPath<String>,
    Extension(principal): Extension<Principal>,
    Json(request): Json<PrepareScmRequest>,
) -> Result<Json<GitSnapshot>, ApiError> {
    require_project(&principal, Permission::Build, &name)?;
    let project = project_by_name(&state.storage, &name)?;
    let repository = GitRepository::open(project.repository_path).await?;
    if request.credential_id.is_some() && !request.fetch {
        return Err(ApiError::BadRequest(
            "an SCM credential requires fetch=true".into(),
        ));
    }
    if request.fetch_ref.is_some() && !request.fetch {
        return Err(ApiError::BadRequest(
            "an SCM fetch_ref requires fetch=true".into(),
        ));
    }
    let credential = resolve_git_credential(
        state.credentials.as_ref(),
        request.credential_id.as_deref(),
        &name,
    )
    .await?;
    Ok(Json(
        repository
            .prepare_with_auth(
                &GitPrepareOptions {
                    remote: request.remote,
                    fetch: request.fetch,
                    revision: request.revision,
                    fetch_ref: request.fetch_ref,
                    clean: request.clean,
                    clean_ignored: request.clean_ignored,
                    submodules: request.submodules,
                    credential_id: request.credential_id,
                    known_hosts_file: state.ssh_known_hosts_file.clone(),
                },
                credential.as_ref(),
            )
            .await?,
    ))
}

pub async fn serve(storage_path: impl AsRef<Path>, bind: SocketAddr) -> Result<(), ServerError> {
    serve_with_config(
        storage_path,
        ServerConfig {
            bind,
            auth_token: None,
            auth_policy_file: None,
            auth_users_file: None,
            webhook_secret: None,
            github_webhook_secret: None,
            gitlab_webhook_secret: None,
            bitbucket_webhook_secret: None,
            github_webhook_credential_id: None,
            gitlab_webhook_credential_id: None,
            bitbucket_webhook_credential_id: None,
            credentials_file: None,
            credentials_passphrase: None,
            credentials_keychain_account: None,
            credentials_keychain_service: None,
            ssh_known_hosts_file: None,
            extension_manifest_dir: None,
            allowed_origins: default_allowed_origins(),
        },
    )
    .await
}

pub async fn serve_with_config(
    storage_path: impl AsRef<Path>,
    config: ServerConfig,
) -> Result<(), ServerError> {
    validate_config(&config)?;
    let listener = TcpListener::bind(config.bind).await?;
    serve_with_listener(storage_path, config, listener).await
}

/// Serve Rivet on a listener that has already been bound by the caller.
///
/// Desktop clients use an ephemeral loopback listener so another local
/// process cannot make the embedded engine fail merely by occupying port
/// 7878. The actual origin is communicated through the desktop command
/// bridge before the webview starts its API requests.
pub async fn serve_with_listener(
    storage_path: impl AsRef<Path>,
    config: ServerConfig,
    listener: TcpListener,
) -> Result<(), ServerError> {
    let bind = listener.local_addr()?;
    let config = ServerConfig { bind, ..config };
    validate_config(&config)?;
    let auth_policy = config
        .auth_policy_file
        .as_deref()
        .map(load_auth_policy)
        .transpose()?
        .map(Arc::new);
    let auth_users = config
        .auth_users_file
        .as_deref()
        .map(load_auth_users)
        .transpose()?
        .map(Arc::new);
    let credentials = match (
        config.credentials_file.as_ref(),
        config.credentials_passphrase.as_deref(),
        config.credentials_keychain_account.as_deref(),
    ) {
        (Some(path), Some(passphrase), None) => Some(Arc::new(Mutex::new(CredentialVault::open(
            path, passphrase,
        )?))),
        (Some(path), None, Some(account)) => {
            let keychain = config
                .credentials_keychain_service
                .as_deref()
                .map(CredentialKeychain::new)
                .transpose()?
                .unwrap_or_else(CredentialKeychain::rivet);
            let passphrase = keychain.get_passphrase(account)?;
            Some(Arc::new(Mutex::new(CredentialVault::open(
                path,
                passphrase.as_bytes(),
            )?)))
        }
        (None, None, None) => None,
        _ => return Err(ServerError::IncompleteCredentialVaultConfig),
    };
    let extensions = ExtensionCatalog::from_directory(config.extension_manifest_dir.as_deref())?;
    let extension_manager = config
        .extension_manifest_dir
        .as_deref()
        .map(|root| ExtensionManager::new(root, &extensions))
        .transpose()?
        .map(Arc::new);
    let storage = Storage::open(storage_path)?;
    let remote_attempts = storage.list_remote_attempts()?;
    let mut resumable_remote_attempts = Vec::new();
    let mut preserved_remote_builds = std::collections::BTreeSet::new();
    for attempt in remote_attempts {
        let details = storage.get_build_details(attempt.build_id)?;
        let Some(details) = details else {
            let _ = storage.delete_remote_attempt(attempt.build_id)?;
            continue;
        };
        if details.build.status.is_terminal() || attempt.pipeline.has_secret_parameters() {
            let _ = storage.delete_remote_attempt(attempt.build_id)?;
            continue;
        }
        preserved_remote_builds.insert(attempt.build_id);
        resumable_remote_attempts.push(attempt);
    }
    let recovered_builds =
        storage.recover_incomplete_builds_except(Utc::now(), &preserved_remote_builds)?;
    for build_id in &recovered_builds {
        let _ = storage.delete_remote_attempt(*build_id)?;
        tracing::warn!(
            %build_id,
            "reconciled an incomplete build left by a previous server process"
        );
    }
    let mut state = AppState::with_security(
        storage,
        config.auth_token.as_deref(),
        config.webhook_secret.as_deref(),
        credentials,
    );
    let shutdown = CancellationToken::new();
    state.shutdown = shutdown.clone();
    state.auth_policy = auth_policy;
    state.auth_users = auth_users;
    state.extensions = Arc::new(extensions);
    state.extension_manager = extension_manager;
    state.github_webhook_secret = config
        .github_webhook_secret
        .as_deref()
        .map(str::as_bytes)
        .map(ToOwned::to_owned);
    state.gitlab_webhook_secret = config
        .gitlab_webhook_secret
        .as_deref()
        .map(str::as_bytes)
        .map(ToOwned::to_owned);
    state.bitbucket_webhook_secret = config
        .bitbucket_webhook_secret
        .as_deref()
        .map(str::as_bytes)
        .map(ToOwned::to_owned);
    state.github_webhook_credential_id = config.github_webhook_credential_id;
    state.gitlab_webhook_credential_id = config.gitlab_webhook_credential_id;
    state.bitbucket_webhook_credential_id = config.bitbucket_webhook_credential_id;
    state.ssh_known_hosts_file = config.ssh_known_hosts_file;
    let allowed_origins = if config.allowed_origins.is_empty() {
        default_allowed_origins()
    } else {
        config.allowed_origins
    };
    tracing::info!(bind = %bind, "Rivet server listening");
    spawn_schedule_dispatcher(state.clone(), shutdown.clone());
    spawn_remote_recovery_dispatcher(state.clone(), resumable_remote_attempts, shutdown.clone());
    let result = axum::serve(listener, router_with_origins(state, &allowed_origins)?)
        .with_graceful_shutdown(wait_for_shutdown_signal(shutdown.clone()))
        .await;
    shutdown.cancel();
    result?;
    Ok(())
}

fn validate_config(config: &ServerConfig) -> Result<(), ServerError> {
    if !config.bind.ip().is_loopback()
        && config.auth_token.is_none()
        && config.auth_policy_file.is_none()
        && config.auth_users_file.is_none()
    {
        return Err(ServerError::AuthRequired(config.bind));
    }
    if config
        .auth_token
        .as_deref()
        .is_some_and(|token| token.trim().is_empty())
    {
        return Err(ServerError::EmptyAuthToken);
    }
    if config
        .webhook_secret
        .as_deref()
        .is_some_and(|secret| secret.trim().is_empty())
    {
        return Err(ServerError::EmptyWebhookSecret);
    }
    if config
        .github_webhook_secret
        .as_deref()
        .is_some_and(|secret| secret.trim().is_empty())
    {
        return Err(ServerError::EmptyProviderWebhookSecret { provider: "GitHub" });
    }
    if config
        .gitlab_webhook_secret
        .as_deref()
        .is_some_and(|secret| secret.trim().is_empty())
    {
        return Err(ServerError::EmptyProviderWebhookSecret { provider: "GitLab" });
    }
    if config
        .bitbucket_webhook_secret
        .as_deref()
        .is_some_and(|secret| secret.trim().is_empty())
    {
        return Err(ServerError::EmptyProviderWebhookSecret {
            provider: "Bitbucket",
        });
    }
    if config
        .github_webhook_credential_id
        .as_deref()
        .is_some_and(|credential| credential.trim().is_empty())
    {
        return Err(ServerError::EmptyProviderWebhookCredential { provider: "GitHub" });
    }
    if config
        .gitlab_webhook_credential_id
        .as_deref()
        .is_some_and(|credential| credential.trim().is_empty())
    {
        return Err(ServerError::EmptyProviderWebhookCredential { provider: "GitLab" });
    }
    if config
        .bitbucket_webhook_credential_id
        .as_deref()
        .is_some_and(|credential| credential.trim().is_empty())
    {
        return Err(ServerError::EmptyProviderWebhookCredential {
            provider: "Bitbucket",
        });
    }
    match (
        config.credentials_file.is_some(),
        config.credentials_passphrase.is_some(),
        config.credentials_keychain_account.is_some(),
        config.credentials_keychain_service.is_some(),
    ) {
        (false, false, false, false)
        | (true, true, false, false)
        | (true, false, true, false)
        | (true, false, true, true) => {}
        _ => return Err(ServerError::IncompleteCredentialVaultConfig),
    }
    if config
        .credentials_passphrase
        .as_deref()
        .is_some_and(|passphrase| passphrase.trim().is_empty())
    {
        return Err(ServerError::IncompleteCredentialVaultConfig);
    }
    if config
        .credentials_keychain_account
        .as_deref()
        .is_some_and(|account| account.trim().is_empty())
    {
        return Err(ServerError::IncompleteCredentialVaultConfig);
    }
    if config
        .credentials_keychain_service
        .as_deref()
        .is_some_and(|service| service.trim().is_empty())
    {
        return Err(ServerError::IncompleteCredentialVaultConfig);
    }
    if let Some(service) = config.credentials_keychain_service.as_deref() {
        CredentialKeychain::new(service.to_owned())?;
    }
    if let Some(path) = config.ssh_known_hosts_file.as_deref() {
        validate_known_hosts_file(path)
            .map(|_| ())
            .map_err(|error| ServerError::InvalidSshKnownHostsFile(error.to_string()))?;
    }
    Ok(())
}

fn load_auth_policy(path: &Path) -> Result<AuthPolicy, ServerError> {
    let metadata =
        std::fs::symlink_metadata(path).map_err(|error| ServerError::AuthPolicyFile {
            path: path.to_path_buf(),
            message: error.to_string(),
        })?;
    if metadata.file_type().is_symlink() {
        return Err(ServerError::AuthPolicyFile {
            path: path.to_path_buf(),
            message: "symbolic links are not accepted".into(),
        });
    }
    if !metadata.is_file() {
        return Err(ServerError::AuthPolicyFile {
            path: path.to_path_buf(),
            message: "path is not a regular file".into(),
        });
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if metadata.permissions().mode() & 0o077 != 0 {
            return Err(ServerError::AuthPolicyFile {
                path: path.to_path_buf(),
                message: "file must not be group- or world-readable".into(),
            });
        }
    }
    let bytes = std::fs::read(path).map_err(|error| ServerError::AuthPolicyFile {
        path: path.to_path_buf(),
        message: error.to_string(),
    })?;
    AuthPolicy::from_json(&bytes).map_err(ServerError::AuthPolicy)
}

fn load_auth_users(path: &Path) -> Result<AuthUsers, ServerError> {
    let metadata = std::fs::symlink_metadata(path).map_err(|error| ServerError::AuthUsersFile {
        path: path.to_path_buf(),
        message: error.to_string(),
    })?;
    if metadata.file_type().is_symlink() {
        return Err(ServerError::AuthUsersFile {
            path: path.to_path_buf(),
            message: "symbolic links are not accepted".into(),
        });
    }
    if !metadata.is_file() {
        return Err(ServerError::AuthUsersFile {
            path: path.to_path_buf(),
            message: "path is not a regular file".into(),
        });
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if metadata.permissions().mode() & 0o077 != 0 {
            return Err(ServerError::AuthUsersFile {
                path: path.to_path_buf(),
                message: "file must not be group- or world-readable".into(),
            });
        }
    }
    let bytes = std::fs::read(path).map_err(|error| ServerError::AuthUsersFile {
        path: path.to_path_buf(),
        message: error.to_string(),
    })?;
    AuthUsers::from_json(&bytes).map_err(ServerError::AuthUsers)
}

impl AppState {
    pub fn new(storage: Storage) -> Self {
        let (events, _) = broadcast::channel(1024);
        let cache_root = storage.cache_root();
        Self {
            storage,
            scheduler: Arc::new(Scheduler::new_with_cache(2, Some(1), Some(cache_root))),
            active_builds: Arc::new(Mutex::new(HashMap::new())),
            events,
            shutdown: CancellationToken::new(),
            auth_digest: None,
            auth_policy: None,
            auth_users: None,
            webhook_secret: None,
            github_webhook_secret: None,
            gitlab_webhook_secret: None,
            bitbucket_webhook_secret: None,
            github_webhook_credential_id: None,
            gitlab_webhook_credential_id: None,
            bitbucket_webhook_credential_id: None,
            credentials: None,
            ssh_known_hosts_file: None,
            extensions: Arc::new(ExtensionCatalog::empty()),
            extension_manager: None,
            agents: AgentRegistry::default(),
            remote_messages: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    #[cfg(test)]
    fn with_token(storage: Storage, token: &str) -> Self {
        Self::with_security(storage, Some(token), None, None)
    }

    #[cfg(test)]
    fn with_webhook_secret(storage: Storage, secret: &str) -> Self {
        Self::with_security(storage, None, Some(secret), None)
    }

    fn with_security(
        storage: Storage,
        token: Option<&str>,
        webhook_secret: Option<&str>,
        credentials: Option<Arc<Mutex<CredentialVault>>>,
    ) -> Self {
        let mut state = Self::new(storage);
        state.auth_digest = token.map(|token| hash_token(token.as_bytes()));
        state.webhook_secret = webhook_secret.map(|secret| secret.as_bytes().to_vec());
        state.credentials = credentials;
        state
    }
}

async fn auth_me(Extension(principal): Extension<Principal>) -> Json<AuthMeResponse> {
    Json(AuthMeResponse {
        local_mode: principal.id() == "local",
        id: principal.id().to_owned(),
        role: principal.role(),
        projects: principal.projects().map(str::to_owned).collect(),
    })
}

async fn login(
    State(state): State<AppState>,
    axum::extract::Json(request): axum::extract::Json<AuthLoginRequest>,
) -> Result<Json<AuthSessionResponse>, ApiError> {
    let users = state
        .auth_users
        .as_deref()
        .ok_or(ApiError::AuthUsersUnavailable)?;
    let principal = users
        .authenticate(&request.username, &request.password)
        .ok_or_else(|| {
            record_auth_audit(&state.storage, None, "/api/v1/auth/login", "failure");
            ApiError::InvalidCredentials
        })?;
    record_auth_audit(
        &state.storage,
        Some(principal.id()),
        "/api/v1/auth/login",
        "success",
    );
    issue_session(&state, &principal).await
}

async fn create_session(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
) -> Result<Json<AuthSessionResponse>, ApiError> {
    require_global(&principal, Permission::Read)?;
    if principal.id() == "local" {
        return Err(ApiError::BadRequest(
            "sessions are only needed when server authentication is enabled".into(),
        ));
    }

    issue_session(&state, &principal).await
}

async fn issue_session(
    state: &AppState,
    principal: &Principal,
) -> Result<Json<AuthSessionResponse>, ApiError> {
    let created_at = Utc::now();
    let expires_at = created_at + chrono::Duration::seconds(AUTH_SESSION_TTL_SECONDS);
    let raw_token = generate_token()?;
    let digest = token_digest(&raw_token);
    let session_id = uuid::Uuid::new_v4();
    let role = role_label(principal.role());
    let projects = principal.projects().map(str::to_owned).collect::<Vec<_>>();
    state.storage.create_auth_session(
        session_id,
        &digest,
        principal.id(),
        role,
        &projects,
        created_at,
        expires_at,
    )?;
    if let Err(error) = state.storage.prune_auth_sessions(created_at) {
        tracing::warn!(?error, "could not prune expired authentication sessions");
    }
    record_auth_session_audit(
        &state.storage,
        principal.id(),
        &session_id.to_string(),
        "created",
    );
    Ok(Json(AuthSessionResponse {
        id: session_id,
        session_token: raw_token.as_str().to_owned(),
        expires_at,
    }))
}

async fn revoke_session(
    State(state): State<AppState>,
    headers: HeaderMap,
    Extension(principal): Extension<Principal>,
) -> Result<StatusCode, ApiError> {
    require_global(&principal, Permission::Read)?;
    let Some(token) = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
    else {
        return Err(ApiError::BadRequest(
            "Bearer session token is required".into(),
        ));
    };
    let digest = token_digest(token);
    if state.storage.revoke_auth_session(&digest, Utc::now())? {
        record_auth_session_audit(&state.storage, principal.id(), "current", "revoked");
    }
    Ok(StatusCode::NO_CONTENT)
}

fn role_label(role: Role) -> &'static str {
    match role {
        Role::Admin => "admin",
        Role::Operator => "operator",
        Role::Viewer => "viewer",
        Role::Agent => "agent",
    }
}

fn parse_role_label(value: &str) -> Option<Role> {
    match value {
        "admin" => Some(Role::Admin),
        "operator" => Some(Role::Operator),
        "viewer" => Some(Role::Viewer),
        "agent" => Some(Role::Agent),
        _ => None,
    }
}

fn authenticate_session(state: &AppState, token: &str) -> Option<Principal> {
    let digest = token_digest(token);
    let session = match state.storage.auth_session(&digest, Utc::now()) {
        Ok(session) => session,
        Err(error) => {
            tracing::warn!(?error, "could not read authentication session");
            return None;
        }
    }?;
    let role = parse_role_label(&session.role)?;
    Some(Principal::from_parts(
        session.principal_id,
        role,
        session.projects,
    ))
}

fn record_auth_session_audit(
    storage: &Storage,
    actor_id: &str,
    session_resource: &str,
    operation: &str,
) {
    if let Err(error) = storage.append_audit_event(
        Utc::now(),
        Some(actor_id),
        "auth.session",
        session_resource,
        operation,
    ) {
        tracing::warn!(
            ?error,
            "could not persist authentication session audit event"
        );
    }
}

fn configured_credentials(state: &AppState) -> Result<&Arc<Mutex<CredentialVault>>, ApiError> {
    state
        .credentials
        .as_ref()
        .ok_or(ApiError::CredentialsUnavailable)
}

async fn list_credentials(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
) -> Result<Json<Vec<CredentialSummary>>, ApiError> {
    require_global(&principal, Permission::Administer)?;
    let vault = configured_credentials(&state)?;
    Ok(Json(vault.lock().await.list()))
}

async fn set_credential(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
    Extension(principal): Extension<Principal>,
    Json(request): Json<CredentialWriteRequest>,
) -> Result<Json<CredentialSummary>, ApiError> {
    require_global(&principal, Permission::Administer)?;
    let vault = configured_credentials(&state)?;
    let mut vault = vault.lock().await;
    let CredentialWriteRequest {
        kind,
        username,
        secret,
        projects,
    } = request;
    match kind {
        CredentialKind::HttpBasic => {
            vault.set_http_basic_for_projects(id.clone(), username, secret, projects)?;
        }
        CredentialKind::SshKey => {
            vault.set_ssh_key_for_projects(id.clone(), username, secret, projects)?;
        }
    }
    let summary = vault
        .list()
        .into_iter()
        .find(|credential| credential.id == id)
        .ok_or_else(|| ApiError::BadRequest("credential was not persisted".into()))?;
    record_credential_audit(&state.storage, principal.id(), &id, "set");
    Ok(Json(summary))
}

async fn delete_credential(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
    Extension(principal): Extension<Principal>,
) -> Result<StatusCode, ApiError> {
    require_global(&principal, Permission::Administer)?;
    let vault = configured_credentials(&state)?;
    vault.lock().await.remove(&id)?;
    record_credential_audit(&state.storage, principal.id(), &id, "remove");
    Ok(StatusCode::NO_CONTENT)
}

fn record_credential_audit(
    storage: &Storage,
    actor_id: &str,
    credential_id: &str,
    operation: &str,
) {
    if let Err(error) = storage.append_audit_event(
        Utc::now(),
        Some(actor_id),
        "credentials.manage",
        credential_id,
        operation,
    ) {
        tracing::warn!(
            ?error,
            "could not persist credential management audit event"
        );
    }
}

async fn list_audit(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
) -> Result<Json<Vec<AuditEventRecord>>, ApiError> {
    require_global(&principal, Permission::Administer)?;
    Ok(Json(state.storage.list_audit_events(100)?))
}

async fn authenticate(
    State(state): State<AppState>,
    mut request: axum::http::Request<Body>,
    next: Next,
) -> Response {
    let auth_enabled =
        state.auth_digest.is_some() || state.auth_policy.is_some() || state.auth_users.is_some();
    if !auth_enabled {
        request.extensions_mut().insert(Principal::local_admin());
        return next.run(request).await;
    }
    if matches!(
        request.uri().path(),
        "/api/v1/health" | "/api/v1/ready" | "/api/v1/auth/login"
    ) {
        return next.run(request).await;
    }
    let principal = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .and_then(|token| {
            state
                .auth_policy
                .as_deref()
                .and_then(|policy| policy.authenticate(token))
                .or_else(|| {
                    state.auth_digest.as_ref().and_then(|digest| {
                        let candidate = hash_token(token.as_bytes());
                        bool::from(candidate.ct_eq(digest)).then(Principal::legacy_admin)
                    })
                })
                .or_else(|| authenticate_session(&state, token))
        });
    if let Some(principal) = principal {
        record_auth_audit(
            &state.storage,
            Some(principal.id()),
            request.uri().path(),
            "success",
        );
        request.extensions_mut().insert(principal);
        next.run(request).await
    } else {
        record_auth_audit(&state.storage, None, request.uri().path(), "failure");
        let mut response = (
            StatusCode::UNAUTHORIZED,
            Json(json!({ "error": "authentication required" })),
        )
            .into_response();
        response
            .headers_mut()
            .insert(header::WWW_AUTHENTICATE, HeaderValue::from_static("Bearer"));
        response
    }
}

fn record_auth_audit(storage: &Storage, actor_id: Option<&str>, resource: &str, outcome: &str) {
    if let Err(error) =
        storage.append_audit_event(Utc::now(), actor_id, "auth.authenticate", resource, outcome)
    {
        tracing::warn!(?error, "could not persist authentication audit event");
    }
}

async fn security_headers(request: axum::http::Request<Body>, next: Next) -> Response {
    let mut response = next.run(request).await;
    let headers = response.headers_mut();
    headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    headers.insert(
        HeaderName::from_static("x-content-type-options"),
        HeaderValue::from_static("nosniff"),
    );
    headers.insert(
        HeaderName::from_static("referrer-policy"),
        HeaderValue::from_static("no-referrer"),
    );
    headers.insert(
        HeaderName::from_static("x-frame-options"),
        HeaderValue::from_static("DENY"),
    );
    response
}

async fn request_id(request: axum::http::Request<Body>, next: Next) -> Response {
    let request_id = request
        .headers()
        .get("x-request-id")
        .and_then(|value| value.to_str().ok())
        .filter(|value| valid_request_id(value))
        .map(str::to_owned)
        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
    let method = request.method().clone();
    let path = request.uri().path().to_owned();
    let mut response = next.run(request).await;
    if let Ok(value) = HeaderValue::from_str(&request_id) {
        response.headers_mut().insert("x-request-id", value);
    }
    tracing::info!(
        request_id = %request_id,
        method = %method,
        path = %path,
        status = response.status().as_u16(),
        "Rivet HTTP request"
    );
    response
}

fn valid_request_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b':' | b'-'))
}

fn hash_token(token: &[u8]) -> [u8; 32] {
    Sha256::digest(token).into()
}

fn require_global(principal: &Principal, permission: Permission) -> Result<(), ApiError> {
    if principal.can_global(permission) {
        Ok(())
    } else {
        Err(ApiError::Forbidden(format!(
            "{} permission is required",
            permission_label(permission)
        )))
    }
}

fn require_project(
    principal: &Principal,
    permission: Permission,
    project: &str,
) -> Result<(), ApiError> {
    if principal.can_project(permission, project) {
        Ok(())
    } else {
        Err(ApiError::Forbidden(format!(
            "{} permission is required for this project",
            permission_label(permission)
        )))
    }
}

fn permission_label(permission: Permission) -> &'static str {
    match permission {
        Permission::Read => "read",
        Permission::Build => "build",
        Permission::Administer => "administer",
        Permission::ConnectAgent => "agent-connect",
    }
}

async fn health() -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok",
        service: "rivet-server",
        timestamp: Utc::now(),
    })
}

async fn readiness(State(state): State<AppState>) -> Response {
    match state.storage.health_check() {
        Ok(()) => (
            StatusCode::OK,
            Json(ReadinessResponse {
                status: "ready",
                service: "rivet-server",
                storage: "ok",
                timestamp: Utc::now(),
            }),
        )
            .into_response(),
        Err(error) => {
            tracing::error!(?error, "Rivet readiness storage probe failed");
            (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(ReadinessResponse {
                    status: "not_ready",
                    service: "rivet-server",
                    storage: "unavailable",
                    timestamp: Utc::now(),
                }),
            )
                .into_response()
        }
    }
}

async fn metrics(State(state): State<AppState>) -> Result<Response, ApiError> {
    let queue = state.scheduler.stats();
    let project_count = state.storage.list_projects()?.len();
    let active_builds = state.active_builds.lock().await.len();
    let body = format!(
        "# HELP rivet_info Static metadata for the Rivet server.\n\
# TYPE rivet_info gauge\n\
rivet_info{{service=\"rivet-server\"}} 1\n\
# HELP rivet_projects_total Number of projects known to this server.\n\
# TYPE rivet_projects_total gauge\n\
rivet_projects_total {project_count}\n\
# HELP rivet_builds_active Builds currently owned by the execution service.\n\
# TYPE rivet_builds_active gauge\n\
rivet_builds_active {active_builds}\n\
# HELP rivet_queue_queued Builds waiting for scheduler admission.\n\
# TYPE rivet_queue_queued gauge\n\
rivet_queue_queued {}\n\
# HELP rivet_queue_running Builds currently occupying scheduler capacity.\n\
# TYPE rivet_queue_running gauge\n\
rivet_queue_running {}\n\
# HELP rivet_queue_capacity Configured scheduler capacity.\n\
# TYPE rivet_queue_capacity gauge\n\
rivet_queue_capacity {}\n\
# HELP rivet_queue_paused Whether new scheduler admissions are paused.\n\
# TYPE rivet_queue_paused gauge\n\
rivet_queue_paused {}\n",
        queue.queued,
        queue.running,
        queue.capacity,
        usize::from(queue.paused),
    );
    let mut response = (StatusCode::OK, body).into_response();
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/plain; version=0.0.4; charset=utf-8"),
    );
    Ok(response)
}

async fn request_shutdown(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
) -> Result<(StatusCode, Json<ShutdownResponse>), ApiError> {
    require_global(&principal, Permission::Administer)?;
    state.shutdown.cancel();
    tracing::info!(
        actor = %principal.id(),
        "Rivet server shutdown requested through the admin API"
    );
    Ok((
        StatusCode::ACCEPTED,
        Json(ShutdownResponse {
            status: "shutdown_requested",
        }),
    ))
}

async fn analyze_jenkinsfile(
    Extension(principal): Extension<Principal>,
    Json(request): Json<JenkinsfileAnalysisRequest>,
) -> Result<Json<JenkinsfileAnalysisResponse>, ApiError> {
    require_global(&principal, Permission::Read)?;
    const MAX_JENKINSFILE_BYTES: usize = 200 * 1024;
    if request.source.len() > MAX_JENKINSFILE_BYTES {
        return Err(ApiError::BadRequest(format!(
            "Jenkinsfile source exceeds {MAX_JENKINSFILE_BYTES} bytes"
        )));
    }
    let analysis = rivet_migration::analyze_jenkinsfile(&request.source);
    let draft = request
        .draft
        .then(|| rivet_migration::generate_rivetfile_draft(&request.source))
        .transpose()?;
    Ok(Json(JenkinsfileAnalysisResponse { analysis, draft }))
}

async fn queue_status(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
) -> Result<Json<QueueStatusResponse>, ApiError> {
    require_global(&principal, Permission::Read)?;
    Ok(Json(queue_status_response(&state)))
}

async fn queue_items(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
) -> Result<Json<Vec<QueueItemResponse>>, ApiError> {
    require_global(&principal, Permission::Read)?;
    let project_names = state
        .storage
        .list_projects()?
        .into_iter()
        .map(|project| (project.id, project.name))
        .collect::<HashMap<_, _>>();
    let items = state
        .scheduler
        .queue_entries()
        .await
        .into_iter()
        .enumerate()
        .map(|(index, entry)| QueueItemResponse {
            build_id: entry.build_id,
            project_id: entry.project_id,
            project: project_names
                .get(&entry.project_id)
                .cloned()
                .unwrap_or_else(|| entry.project_id.to_string()),
            priority: entry.priority,
            position: index + 1,
        })
        .collect();
    Ok(Json(items))
}

async fn pause_queue(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
) -> Result<Json<QueueStatusResponse>, ApiError> {
    require_global(&principal, Permission::Administer)?;
    state.scheduler.pause();
    Ok(Json(queue_status_response(&state)))
}

async fn resume_queue(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
) -> Result<Json<QueueStatusResponse>, ApiError> {
    require_global(&principal, Permission::Administer)?;
    state.scheduler.resume();
    Ok(Json(queue_status_response(&state)))
}

fn queue_status_response(state: &AppState) -> QueueStatusResponse {
    let QueueStats {
        queued,
        running,
        capacity,
        paused,
    } = state.scheduler.stats();
    QueueStatusResponse {
        queued,
        running,
        capacity,
        paused,
    }
}

async fn list_projects(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
) -> Result<Json<Vec<Project>>, ApiError> {
    require_global(&principal, Permission::Read)?;
    let projects = state
        .storage
        .list_projects()?
        .into_iter()
        .filter(|project| principal.can_project(Permission::Read, &project.name))
        .collect();
    Ok(Json(projects))
}

async fn list_pipeline_parameters(
    State(state): State<AppState>,
    AxumPath(name): AxumPath<String>,
    Extension(principal): Extension<Principal>,
) -> Result<Json<Vec<PipelineParameterResponse>>, ApiError> {
    require_project(&principal, Permission::Read, &name)?;
    let project = project_by_name(&state.storage, &name)?;
    let pipeline = Pipeline::load(&project.pipeline_path)?;
    Ok(Json(
        pipeline
            .parameters
            .into_iter()
            .map(|parameter| PipelineParameterResponse {
                required: parameter.default.is_none(),
                name: parameter.name,
                kind: parameter.kind,
                secret: parameter.secret,
                default: parameter.default,
                choices: parameter.choices,
            })
            .collect(),
    ))
}

async fn list_extensions(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
) -> Result<Json<Vec<ExtensionManifest>>, ApiError> {
    require_global(&principal, Permission::Read)?;
    Ok(Json(state.extensions.manifests().to_vec()))
}

async fn extension_status(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
) -> Result<Json<Vec<ExtensionRuntimeStatusResponse>>, ApiError> {
    require_global(&principal, Permission::Read)?;
    let active = match state.extension_manager.as_ref() {
        Some(manager) => manager
            .active_extensions()
            .await
            .into_iter()
            .collect::<HashSet<_>>(),
        None => HashSet::new(),
    };
    Ok(Json(
        state
            .extensions
            .manifests()
            .iter()
            .map(|manifest| ExtensionRuntimeStatusResponse {
                id: manifest.id.clone(),
                active: active.contains(&manifest.id),
                runtime_available: state.extension_manager.is_some(),
            })
            .collect(),
    ))
}

async fn start_extension(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
    Extension(principal): Extension<Principal>,
) -> Result<Json<ExtensionRuntimeStatusResponse>, ApiError> {
    require_global(&principal, Permission::Administer)?;
    let manager = state
        .extension_manager
        .as_ref()
        .ok_or(ApiError::ExtensionsUnavailable)?;
    manager
        .launch(&id, std::iter::empty::<String>())
        .await
        .map_err(ApiError::ExtensionManager)?;
    extension_status_for(&state, &id).await
}

async fn stop_extension(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
    Extension(principal): Extension<Principal>,
) -> Result<Json<ExtensionRuntimeStatusResponse>, ApiError> {
    require_global(&principal, Permission::Administer)?;
    let manager = state
        .extension_manager
        .as_ref()
        .ok_or(ApiError::ExtensionsUnavailable)?;
    manager
        .terminate(&id)
        .await
        .map_err(ApiError::ExtensionManager)?;
    extension_status_for(&state, &id).await
}

async fn request_extension(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
    Extension(principal): Extension<Principal>,
    Json(request): Json<ExtensionRequestInput>,
) -> Result<Json<Value>, ApiError> {
    require_global(&principal, Permission::Administer)?;
    let manager = state
        .extension_manager
        .as_ref()
        .ok_or(ApiError::ExtensionsUnavailable)?;
    let ExtensionRequestInput {
        permission,
        method,
        payload: input,
    } = request;
    manager
        .validate_request(&id, permission)
        .await
        .map_err(ApiError::ExtensionManager)?;
    let payload = extension_request_payload(&state, permission, &method, input).await?;
    let result = manager
        .request(&id, permission, method, payload)
        .await
        .map_err(ApiError::ExtensionManager)?;
    Ok(Json(result))
}

async fn extension_request_payload(
    state: &AppState,
    permission: ExtensionPermission,
    method: &str,
    input: Value,
) -> Result<Value, ApiError> {
    if method == "build.trigger" {
        extension_trigger_payload(state, permission, method, input).await
    } else {
        extension_host_payload(state, permission, method, input)
    }
}

async fn extension_trigger_payload(
    state: &AppState,
    permission: ExtensionPermission,
    method: &str,
    input: Value,
) -> Result<Value, ApiError> {
    if permission != ExtensionPermission::TriggerBuilds {
        return Err(ApiError::BadRequest(
            "extension host method \"build.trigger\" requires permission TriggerBuilds".into(),
        ));
    }
    let project_id = extension_uuid(&input, "project_id")?;
    let project = state
        .storage
        .get_project_by_id(project_id)?
        .ok_or_else(|| ApiError::BadRequest("extension project_id was not found".into()))?;
    let parameters = match input.get("parameters") {
        Some(value) => {
            serde_json::from_value::<BTreeMap<String, String>>(value.clone()).map_err(|error| {
                ApiError::BadRequest(format!("extension trigger parameters are invalid: {error}"))
            })?
        }
        None => BTreeMap::new(),
    };
    if parameters.len() > MAX_EXTENSION_TRIGGER_PARAMETERS {
        return Err(ApiError::BadRequest(format!(
            "extension trigger accepts at most {MAX_EXTENSION_TRIGGER_PARAMETERS} parameters"
        )));
    }
    let priority = match input.get("priority") {
        Some(value) => value
            .as_i64()
            .and_then(|value| i32::try_from(value).ok())
            .ok_or_else(|| ApiError::BadRequest("extension trigger priority is invalid".into()))?,
        None => 0,
    };
    let queued = enqueue_project_build(
        state,
        project,
        QueueBuildRequest {
            scm: None,
            parameters,
            priority,
        },
    )
    .await?;
    Ok(json!({
        "host_protocol_version": 1,
        "method": method,
        "input": input,
        "data": {
            "build": queued.build,
            "status": queued.status,
        },
    }))
}

/// Add bounded, read-only host data to the small set of versioned capability
/// methods exposed by the server. Unknown methods keep their original payload
/// so extensions can define private application-level messages without being
/// granted implicit access to Rivet state.
fn extension_host_payload(
    state: &AppState,
    permission: ExtensionPermission,
    method: &str,
    input: Value,
) -> Result<Value, ApiError> {
    let Some(required_permission) = extension_host_permission(method) else {
        return Ok(input);
    };
    if permission != required_permission {
        return Err(ApiError::BadRequest(format!(
            "extension host method {method:?} requires permission {required_permission:?}"
        )));
    }
    let data = match method {
        "builds.list" => {
            let project_id = extension_uuid(&input, "project_id")?;
            if state.storage.get_project_by_id(project_id)?.is_none() {
                return Err(ApiError::BadRequest(
                    "extension project_id was not found".into(),
                ));
            }
            let builds = state.storage.list_builds(project_id)?;
            json!({
                "builds": builds.iter().take(MAX_EXTENSION_BUILD_RECORDS).collect::<Vec<_>>(),
                "truncated": builds.len() > MAX_EXTENSION_BUILD_RECORDS,
            })
        }
        "build.details" => {
            let build_id = extension_uuid(&input, "build_id")?;
            let details = state
                .storage
                .get_build_details(build_id)?
                .ok_or_else(|| ApiError::BadRequest("extension build_id was not found".into()))?;
            let stages = details
                .stages
                .iter()
                .take(MAX_EXTENSION_STAGE_RECORDS)
                .map(|stage| {
                    json!({
                        "stage": stage.stage,
                        "steps": stage.steps.iter().take(MAX_EXTENSION_STEP_RECORDS).collect::<Vec<_>>(),
                        "steps_truncated": stage.steps.len() > MAX_EXTENSION_STEP_RECORDS,
                    })
                })
                .collect::<Vec<_>>();
            json!({
                "build": details.build,
                "stages": stages,
                "stages_truncated": details.stages.len() > MAX_EXTENSION_STAGE_RECORDS,
            })
        }
        "build.logs" => {
            let build_id = extension_uuid(&input, "build_id")?;
            ensure_extension_build_exists(&state.storage, build_id)?;
            let after_sequence = input
                .get("after_sequence")
                .and_then(Value::as_i64)
                .unwrap_or(-1);
            let logs = state
                .storage
                .logs(build_id)?
                .into_iter()
                .filter(|log| log.sequence > after_sequence)
                .collect::<Vec<_>>();
            let truncated = logs.len() > MAX_EXTENSION_LOG_RECORDS;
            json!({
                "logs": logs.into_iter().take(MAX_EXTENSION_LOG_RECORDS).collect::<Vec<_>>(),
                "after_sequence": after_sequence,
                "truncated": truncated,
            })
        }
        "build.annotations" => {
            let build_id = extension_uuid(&input, "build_id")?;
            ensure_extension_build_exists(&state.storage, build_id)?;
            let annotations = state.storage.annotations(build_id)?;
            let truncated = annotations.len() > MAX_EXTENSION_ANNOTATION_RECORDS;
            json!({
                "annotations": annotations.into_iter().take(MAX_EXTENSION_ANNOTATION_RECORDS).collect::<Vec<_>>(),
                "truncated": truncated,
            })
        }
        "build.artifacts" => {
            let build_id = extension_uuid(&input, "build_id")?;
            ensure_extension_build_exists(&state.storage, build_id)?;
            let artifacts = state.storage.artifacts(build_id)?;
            let truncated = artifacts.len() > MAX_EXTENSION_ARTIFACT_RECORDS;
            json!({
                "artifacts": artifacts.into_iter().take(MAX_EXTENSION_ARTIFACT_RECORDS).collect::<Vec<_>>(),
                "truncated": truncated,
            })
        }
        "build.annotate" => {
            let build_id = extension_uuid(&input, "build_id")?;
            ensure_extension_build_exists(&state.storage, build_id)?;
            let stage_id = match input.get("stage_id") {
                None | Some(Value::Null) => None,
                Some(_) => Some(extension_uuid(&input, "stage_id")?),
            };
            let kind = input.get("kind").and_then(Value::as_str).ok_or_else(|| {
                ApiError::BadRequest("extension payload requires \"kind\"".into())
            })?;
            let message = input
                .get("message")
                .and_then(Value::as_str)
                .ok_or_else(|| {
                    ApiError::BadRequest("extension payload requires \"message\"".into())
                })?;
            let annotation = state
                .storage
                .add_annotation(build_id, stage_id, kind, message)?;
            json!({ "annotation": annotation })
        }
        _ => unreachable!("extension_host_permission only returns known methods"),
    };
    Ok(json!({
        "host_protocol_version": 1,
        "method": method,
        "input": input,
        "data": data,
    }))
}

fn extension_host_permission(method: &str) -> Option<ExtensionPermission> {
    match method {
        "builds.list" | "build.details" => Some(ExtensionPermission::ReadBuilds),
        "build.logs" => Some(ExtensionPermission::ReadLogs),
        "build.annotations" => Some(ExtensionPermission::ReadBuilds),
        "build.artifacts" => Some(ExtensionPermission::ReadArtifacts),
        "build.annotate" => Some(ExtensionPermission::WriteAnnotations),
        _ => None,
    }
}

fn extension_uuid(input: &Value, field: &str) -> Result<Uuid, ApiError> {
    let value = input
        .get(field)
        .and_then(Value::as_str)
        .ok_or_else(|| ApiError::BadRequest(format!("extension payload requires {field:?}")))?;
    Uuid::parse_str(value)
        .map_err(|_| ApiError::BadRequest(format!("extension payload {field:?} is invalid")))
}

fn ensure_extension_build_exists(storage: &Storage, build_id: BuildId) -> Result<(), ApiError> {
    if storage.get_build_details(build_id)?.is_none() {
        return Err(ApiError::BadRequest(
            "extension build_id was not found".into(),
        ));
    }
    Ok(())
}

async fn extension_status_for(
    state: &AppState,
    id: &str,
) -> Result<Json<ExtensionRuntimeStatusResponse>, ApiError> {
    if !state
        .extensions
        .manifests()
        .iter()
        .any(|manifest| manifest.id == id)
    {
        return Err(ApiError::ExtensionManager(
            ExtensionManagerError::UnknownExtension(id.into()),
        ));
    }
    let active = match state.extension_manager.as_ref() {
        Some(manager) => manager
            .active_extensions()
            .await
            .iter()
            .any(|active| active == id),
        None => false,
    };
    Ok(Json(ExtensionRuntimeStatusResponse {
        id: id.to_owned(),
        active,
        runtime_available: state.extension_manager.is_some(),
    }))
}

async fn list_agents(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
) -> Result<Json<Vec<AgentSummary>>, ApiError> {
    require_global(&principal, Permission::Read)?;
    Ok(Json(state.agents.list(Utc::now()).await))
}

async fn match_agents(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Json(request): Json<AgentMatchRequest>,
) -> Result<Json<Vec<AgentSummary>>, ApiError> {
    require_global(&principal, Permission::Read)?;
    state
        .agents
        .matching(&request.requirements, Utc::now())
        .await
        .map(Json)
        .map_err(|error| ApiError::BadRequest(error.to_string()))
}

async fn connect_agent(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    websocket: WebSocketUpgrade,
) -> Result<impl IntoResponse, ApiError> {
    require_global(&principal, Permission::ConnectAgent)?;
    Ok(websocket.on_upgrade(move |socket| handle_agent_socket(socket, state)))
}

async fn handle_agent_socket(mut socket: WebSocket, state: AppState) {
    let first_message = match tokio::time::timeout(Duration::from_secs(10), socket.recv()).await {
        Ok(Some(Ok(message))) => message,
        Ok(Some(Err(error))) => {
            tracing::debug!(?error, "agent websocket failed before registration");
            return;
        }
        Ok(None) | Err(_) => {
            tracing::debug!("agent websocket did not register before timeout");
            return;
        }
    };
    let registration_delivery_id;
    let registration = match decode_agent_transport_message(first_message) {
        Ok(AgentTransportMessage::Message {
            delivery_id,
            payload: AgentMessage::Register(registration),
            ..
        }) => {
            registration_delivery_id = delivery_id;
            registration
        }
        Ok(_) => {
            let _ = send_agent_error(
                &mut socket,
                "registration_required",
                "first agent message must be register",
            )
            .await;
            return;
        }
        Err(error) => {
            let _ = send_agent_error(&mut socket, "invalid_message", &error).await;
            return;
        }
    };
    let (outbound, mut outbound_rx) = mpsc::channel(256);
    let lease = match state
        .agents
        .register_with_sender(registration, Utc::now(), outbound)
        .await
    {
        Ok(lease) => lease,
        Err(error) => {
            let _ =
                send_agent_error(&mut socket, "registration_rejected", &error.to_string()).await;
            return;
        }
    };
    let mut wire = AgentWireState::default();
    wire.received.insert(registration_delivery_id);
    let registered = AgentMessage::Registered {
        protocol_version: PROTOCOL_VERSION,
        agent_id: lease.agent_id,
        session_id: lease.session_id,
    };
    if !wire.send_message(&mut socket, registered).await {
        state
            .agents
            .unregister(lease.agent_id, lease.session_id)
            .await;
        return;
    }
    if !wire.send_ack(&mut socket, registration_delivery_id).await {
        state
            .agents
            .unregister(lease.agent_id, lease.session_id)
            .await;
        return;
    }

    let mut retransmit = tokio::time::interval(Duration::from_secs(1));
    retransmit.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = retransmit.tick() => {
                if !wire.retransmit_due(&mut socket).await {
                    break;
                }
            }
            incoming = socket.recv() => {
                let Some(result) = incoming else { break; };
                let message = match result {
                    Ok(message) => message,
                    Err(error) => {
                        tracing::debug!(agent_id = %lease.agent_id, ?error, "agent websocket failed");
                        break;
                    }
                };
                match message {
                    Message::Ping(payload) => {
                        if socket.send(Message::Pong(payload)).await.is_err() {
                            break;
                        }
                    }
                    Message::Close(_) => break,
                    Message::Pong(_) => continue,
                    message => match decode_agent_transport_message(message) {
                        Ok(AgentTransportMessage::Ack { delivery_id, .. }) => {
                            wire.acknowledge(delivery_id);
                        }
                        Ok(AgentTransportMessage::Message { delivery_id, payload, .. }) => {
                            let is_new = match wire.accept_incoming(delivery_id) {
                                Ok(is_new) => is_new,
                                Err(()) => {
                                    let _ = send_agent_error(
                                        &mut socket,
                                        "delivery_window_exhausted",
                                        "agent delivery deduplication window is full",
                                    ).await;
                                    break;
                                }
                            };
                            if !is_new {
                                if !wire.send_ack(&mut socket, delivery_id).await {
                                    break;
                                }
                                continue;
                            }
                            if !wire.send_ack(&mut socket, delivery_id).await {
                                break;
                            }
                            match dispatch_agent_message(&state, &lease, payload).await {
                            Ok(Some(response)) => {
                                if !wire.send_message(&mut socket, response).await {
                                    break;
                                }
                            }
                            Ok(None) => {}
                            Err((code, message)) => {
                                let _ = send_agent_error(&mut socket, &code, &message).await;
                                break;
                            }
                            }
                        }
                        Err(error) => {
                            let _ = send_agent_error(&mut socket, "invalid_message", &error).await;
                            break;
                        }
                    },
                }
            }
            outgoing = outbound_rx.recv() => {
                let Some(message) = outgoing else { break; };
                if !wire.send_pending(&mut socket, message).await {
                    break;
                }
            }
        }
    }
    notify_agent_disconnect(&state, lease.agent_id, lease.session_id).await;
    state
        .agents
        .unregister(lease.agent_id, lease.session_id)
        .await;
}

async fn notify_agent_disconnect(
    state: &AppState,
    agent_id: rivet_agent_protocol::AgentId,
    session_id: Uuid,
) {
    let routes = state
        .remote_messages
        .lock()
        .await
        .iter()
        .filter(|(_, route)| route.agent_id == agent_id && route.session_id == session_id)
        .map(|(build_id, route)| (*build_id, route.messages.clone()))
        .collect::<Vec<_>>();
    for (build_id, messages) in routes {
        let _ = messages
            .send(AgentMessage::Error {
                protocol_version: PROTOCOL_VERSION,
                build_id: Some(build_id),
                code: "agent_disconnected".into(),
                message: "the assigned agent websocket disconnected".into(),
            })
            .await;
    }
}

async fn dispatch_agent_message(
    state: &AppState,
    lease: &AgentLease,
    message: AgentMessage,
) -> Result<Option<AgentMessage>, (String, String)> {
    match message {
        AgentMessage::Heartbeat(heartbeat) => {
            if heartbeat.agent_id != lease.agent_id {
                return Err((
                    "agent_identity_mismatch".into(),
                    "heartbeat agent_id does not match the registered session".into(),
                ));
            }
            let sequence = heartbeat.sequence;
            state
                .agents
                .heartbeat(heartbeat, Utc::now())
                .await
                .map_err(|error| ("heartbeat_rejected".into(), error.to_string()))?;
            Ok(Some(AgentMessage::HeartbeatAck {
                protocol_version: PROTOCOL_VERSION,
                sequence,
                server_time: Utc::now(),
            }))
        }
        AgentMessage::Register(_) => Err((
            "already_registered".into(),
            "an agent can register only once per connection".into(),
        )),
        AgentMessage::Event {
            protocol_version,
            attempt_id,
            sequence,
            event,
        } => {
            route_agent_build_message(
                state,
                lease.agent_id,
                lease.session_id,
                build_id_from_event(&event),
                AgentMessage::Event {
                    protocol_version,
                    attempt_id,
                    sequence,
                    event,
                },
            )
            .await?;
            Ok(None)
        }
        AgentMessage::AssignmentAccepted { build_id, .. }
        | AgentMessage::WorkspaceReady { build_id, .. }
        | AgentMessage::ArtifactsReady { build_id, .. }
        | AgentMessage::ArtifactChunk { build_id, .. }
        | AgentMessage::Log { build_id, .. }
        | AgentMessage::Finished { build_id, .. } => {
            route_agent_build_message(state, lease.agent_id, lease.session_id, build_id, message)
                .await?;
            Ok(None)
        }
        AgentMessage::Error {
            build_id: Some(build_id),
            code,
            message,
            ..
        } => {
            route_agent_build_message(
                state,
                lease.agent_id,
                lease.session_id,
                build_id,
                AgentMessage::Error {
                    protocol_version: PROTOCOL_VERSION,
                    build_id: Some(build_id),
                    code,
                    message,
                },
            )
            .await?;
            Ok(None)
        }
        AgentMessage::Error { code, message, .. } => Err((format!("agent_{code}"), message)),
        AgentMessage::Cancel { .. }
        | AgentMessage::Assign { .. }
        | AgentMessage::WorkspaceChunk { .. } => Err((
            "unsupported_message".into(),
            "this message is server-originated and cannot be sent by an agent".into(),
        )),
        AgentMessage::Registered { .. } | AgentMessage::HeartbeatAck { .. } => Err((
            "unsupported_message".into(),
            "this message is server-originated and cannot be sent by an agent".into(),
        )),
    }
}

async fn route_agent_build_message(
    state: &AppState,
    agent_id: rivet_agent_protocol::AgentId,
    session_id: Uuid,
    build_id: BuildId,
    message: AgentMessage,
) -> Result<(), (String, String)> {
    let route = state
        .remote_messages
        .lock()
        .await
        .get(&build_id)
        .cloned()
        .ok_or_else(|| {
            (
                "unknown_build".into(),
                format!("agent message refers to unassigned build {build_id}"),
            )
        })?;
    if route.agent_id != agent_id || route.session_id != session_id {
        return Err((
            "agent_session_fenced".into(),
            format!("agent session is not assigned to build {build_id}"),
        ));
    }
    route.messages.send(message).await.map_err(|_| {
        (
            "build_channel_closed".into(),
            format!("server is no longer listening for build {build_id}"),
        )
    })
}

fn build_id_from_event(event: &BuildEvent) -> BuildId {
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

fn decode_agent_transport_message(message: Message) -> Result<AgentTransportMessage, String> {
    let payload = match message {
        Message::Text(text) => text.to_string(),
        Message::Binary(bytes) => String::from_utf8(bytes.to_vec())
            .map_err(|_| "agent messages must contain valid UTF-8 JSON".to_owned())?,
        Message::Ping(_) | Message::Pong(_) => {
            return Err("control frame is not an agent message".into());
        }
        Message::Close(_) => return Err("agent websocket closed".into()),
    };
    let message: AgentTransportMessage = serde_json::from_str(&payload)
        .map_err(|error| format!("invalid agent transport JSON: {error}"))?;
    message
        .validate()
        .map_err(|error| format!("invalid agent transport message: {error}"))?;
    Ok(message)
}

async fn send_agent_transport(
    socket: &mut WebSocket,
    message: &AgentTransportMessage,
) -> Result<(), ()> {
    let Ok(payload) = serde_json::to_string(message) else {
        return Err(());
    };
    socket
        .send(Message::Text(payload.into()))
        .await
        .map_err(|_| ())
}

async fn send_agent_error(socket: &mut WebSocket, code: &str, message: &str) -> bool {
    let message = AgentTransportMessage::message(AgentMessage::Error {
        protocol_version: PROTOCOL_VERSION,
        build_id: None,
        code: code.to_owned(),
        message: message.to_owned(),
    });
    send_agent_transport(socket, &message).await.is_ok()
}

async fn list_schedules(
    State(state): State<AppState>,
    AxumPath(name): AxumPath<String>,
    Extension(principal): Extension<Principal>,
) -> Result<Json<Vec<ScheduleRecord>>, ApiError> {
    require_project(&principal, Permission::Read, &name)?;
    let project = project_by_name(&state.storage, &name)?;
    Ok(Json(state.storage.list_schedules(project.id)?))
}

async fn create_schedule(
    State(state): State<AppState>,
    AxumPath(name): AxumPath<String>,
    Extension(principal): Extension<Principal>,
    Json(request): Json<CreateScheduleRequest>,
) -> Result<(StatusCode, Json<ScheduleRecord>), ApiError> {
    require_project(&principal, Permission::Build, &name)?;
    let project = project_by_name(&state.storage, &name)?;
    let schedule_name = validate_schedule_name(request.name)?;
    let expression = CronExpression::parse(&request.expression)
        .map_err(|error| ApiError::BadRequest(error.to_string()))?;
    let now = Utc::now();
    let next_run_at = expression
        .next_after(now)
        .map_err(|error| ApiError::BadRequest(error.to_string()))?;
    let poll = match request.trigger {
        ScheduleTrigger::Build => {
            if request.poll.is_some() {
                return Err(ApiError::BadRequest(
                    "poll options require trigger=repository_poll".into(),
                ));
            }
            None
        }
        ScheduleTrigger::RepositoryPoll => {
            let request = normalize_repository_poll_request(request.poll.unwrap_or_default())?;
            Some(SchedulePollConfig {
                remote: request.remote,
                fetch: request.fetch,
                credential_id: request.credential_id,
            })
        }
    };
    let schedule = state.storage.create_schedule_with_trigger_and_poll(
        project.id,
        schedule_name,
        expression.expression(),
        request.trigger,
        poll,
        request.enabled,
        next_run_at,
    )?;
    Ok((StatusCode::CREATED, Json(schedule)))
}

async fn update_schedule(
    State(state): State<AppState>,
    AxumPath((name, schedule_id)): AxumPath<(String, ScheduleId)>,
    Extension(principal): Extension<Principal>,
    Json(request): Json<UpdateScheduleRequest>,
) -> Result<Json<ScheduleRecord>, ApiError> {
    require_project(&principal, Permission::Build, &name)?;
    let project = project_by_name(&state.storage, &name)?;
    state
        .storage
        .set_schedule_enabled(project.id, schedule_id, request.enabled)?
        .map(Json)
        .ok_or(ApiError::ScheduleNotFound {
            project: name,
            schedule_id,
        })
}

async fn delete_schedule(
    State(state): State<AppState>,
    AxumPath((name, schedule_id)): AxumPath<(String, ScheduleId)>,
    Extension(principal): Extension<Principal>,
) -> Result<StatusCode, ApiError> {
    require_project(&principal, Permission::Build, &name)?;
    let project = project_by_name(&state.storage, &name)?;
    if state.storage.delete_schedule(project.id, schedule_id)? {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(ApiError::ScheduleNotFound {
            project: name,
            schedule_id,
        })
    }
}

async fn webhook_build(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<(StatusCode, Json<WebhookBuildResponse>), ApiError> {
    let secret = state
        .webhook_secret
        .as_deref()
        .ok_or(ApiError::WebhookNotConfigured)?;
    verify_webhook_signature(secret, &headers, &body)?;
    let request: WebhookBuildRequest = serde_json::from_slice(&body)
        .map_err(|error| ApiError::BadRequest(format!("invalid webhook JSON: {error}")))?;
    enqueue_webhook_build(&state, &principal, request).await
}

async fn github_webhook(
    State(state): State<AppState>,
    AxumPath(project): AxumPath<String>,
    Extension(principal): Extension<Principal>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<(StatusCode, Json<WebhookBuildResponse>), ApiError> {
    let secret = state
        .github_webhook_secret
        .as_deref()
        .ok_or(ApiError::GitHubWebhookNotConfigured)?;
    let Some(request) = normalize_github_webhook(
        secret,
        &headers,
        &body,
        project,
        state.github_webhook_credential_id.clone(),
    )?
    else {
        return Ok((
            StatusCode::OK,
            Json(WebhookBuildResponse {
                status: "ignored",
                deduplicated: false,
                build: None,
            }),
        ));
    };
    enqueue_webhook_build(&state, &principal, request).await
}

async fn gitlab_webhook(
    State(state): State<AppState>,
    AxumPath(project): AxumPath<String>,
    Extension(principal): Extension<Principal>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<(StatusCode, Json<WebhookBuildResponse>), ApiError> {
    let secret = state
        .gitlab_webhook_secret
        .as_deref()
        .ok_or(ApiError::GitLabWebhookNotConfigured)?;
    let Some(request) = normalize_gitlab_webhook(
        secret,
        &headers,
        &body,
        project,
        state.gitlab_webhook_credential_id.clone(),
    )?
    else {
        return Ok((
            StatusCode::OK,
            Json(WebhookBuildResponse {
                status: "ignored",
                deduplicated: false,
                build: None,
            }),
        ));
    };
    enqueue_webhook_build(&state, &principal, request).await
}

async fn bitbucket_webhook(
    State(state): State<AppState>,
    AxumPath(project): AxumPath<String>,
    Extension(principal): Extension<Principal>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<(StatusCode, Json<WebhookBuildResponse>), ApiError> {
    let secret = state
        .bitbucket_webhook_secret
        .as_deref()
        .ok_or(ApiError::BitbucketWebhookNotConfigured)?;
    let Some(request) = normalize_bitbucket_webhook(
        secret,
        &headers,
        &body,
        project,
        state.bitbucket_webhook_credential_id.clone(),
    )?
    else {
        return Ok((
            StatusCode::OK,
            Json(WebhookBuildResponse {
                status: "ignored",
                deduplicated: false,
                build: None,
            }),
        ));
    };
    enqueue_webhook_build(&state, &principal, request).await
}

async fn enqueue_webhook_build(
    state: &AppState,
    principal: &Principal,
    request: WebhookBuildRequest,
) -> Result<(StatusCode, Json<WebhookBuildResponse>), ApiError> {
    let event_id = validate_webhook_event_id(request.event_id)?;
    if let Some(upstream) = request.upstream.as_ref() {
        if upstream.project.trim().is_empty() {
            return Err(ApiError::BadRequest(
                "upstream project cannot be empty".into(),
            ));
        }
        if upstream.build <= 0 {
            return Err(ApiError::BadRequest(
                "upstream build must be a positive number".into(),
            ));
        }
        let upstream_status = upstream.status.trim();
        if !matches!(upstream_status, "passed" | "failed" | "cancelled") {
            return Err(ApiError::BadRequest(
                "upstream status must be passed, failed, or cancelled".into(),
            ));
        }
        if upstream_status != "passed" {
            return Ok((
                StatusCode::OK,
                Json(WebhookBuildResponse {
                    status: "ignored",
                    deduplicated: false,
                    build: None,
                }),
            ));
        }
        let upstream_project = project_by_name(&state.storage, upstream.project.trim())?;
        let upstream_build = build_by_number(
            &state.storage,
            upstream_project.id,
            upstream.project.trim(),
            upstream.build,
        )?;
        if upstream_build.status != BuildStatus::Passed {
            return Err(ApiError::BadRequest(format!(
                "upstream build {} #{} is not recorded as passed",
                upstream.project.trim(),
                upstream.build
            )));
        }
    }
    let project_name = request.project.trim();
    require_project(principal, Permission::Build, project_name)?;
    let project = project_by_name(&state.storage, project_name)?;

    if !state
        .storage
        .claim_webhook_delivery(&event_id, project.id, Utc::now())?
    {
        let delivery = state
            .storage
            .webhook_delivery(&event_id)?
            .ok_or_else(|| ApiError::BadRequest("webhook delivery disappeared".into()))?;
        if delivery.project_id != project.id {
            return Err(ApiError::WebhookEventConflict);
        }
        let build = delivery.build_id.and_then(|build_id| {
            state
                .storage
                .list_builds(project.id)
                .ok()?
                .into_iter()
                .find(|build| build.id == build_id)
        });
        return Ok((
            StatusCode::OK,
            Json(WebhookBuildResponse {
                status: if build.is_some() {
                    "already_queued"
                } else {
                    "already_received"
                },
                deduplicated: true,
                build,
            }),
        ));
    }

    let scm = if request.fetch
        || request.revision.is_some()
        || request.fetch_ref.is_some()
        || request.remote.is_some()
    {
        Some(PrepareScmRequest {
            remote: request.remote.unwrap_or_else(default_remote),
            fetch: request.fetch,
            revision: request.revision,
            fetch_ref: request.fetch_ref,
            clean: false,
            clean_ignored: false,
            submodules: request.submodules,
            credential_id: request.credential_id,
        })
    } else {
        None
    };
    let queued = match enqueue_project_build(
        state,
        project,
        QueueBuildRequest {
            scm,
            parameters: request.parameters,
            priority: 0,
        },
    )
    .await
    {
        Ok(queued) => queued,
        Err(error) => {
            state.storage.release_webhook_delivery(&event_id)?;
            return Err(error);
        }
    };
    state
        .storage
        .complete_webhook_delivery(&event_id, queued.build.id, queued.build.number)?;
    Ok((
        StatusCode::ACCEPTED,
        Json(WebhookBuildResponse {
            status: "queued",
            deduplicated: false,
            build: Some(queued.build),
        }),
    ))
}

fn normalize_github_webhook(
    secret: &[u8],
    headers: &HeaderMap,
    body: &[u8],
    project: String,
    credential_id: Option<String>,
) -> Result<Option<WebhookBuildRequest>, ApiError> {
    verify_hmac_hex_signature(secret, headers, "x-hub-signature-256", body)?;
    let event = required_header(headers, "x-github-event")?;
    if event == "ping" {
        return Ok(None);
    }
    let event_id = required_header(headers, "x-github-delivery")?;
    let payload: serde_json::Value = serde_json::from_slice(body)
        .map_err(|error| ApiError::BadRequest(format!("invalid GitHub webhook JSON: {error}")))?;
    match event.as_str() {
        "push" => {
            if payload
                .get("deleted")
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(false)
            {
                return Ok(None);
            }
            let Some(revision) = normalize_commit_revision(payload.get("after"), "GitHub after")?
            else {
                return Ok(None);
            };
            Ok(Some(WebhookBuildRequest {
                event_id,
                project,
                revision: Some(revision),
                fetch_ref: None,
                remote: Some("origin".into()),
                fetch: true,
                credential_id,
                submodules: false,
                parameters: BTreeMap::new(),
                upstream: None,
            }))
        }
        "pull_request" | "pull_request_target" => {
            let action = payload
                .get("action")
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| {
                    ApiError::BadRequest("GitHub pull_request action is missing".into())
                })?;
            if !matches!(action, "opened" | "reopened" | "synchronize") {
                return Ok(None);
            }
            let number = payload
                .get("number")
                .and_then(serde_json::Value::as_u64)
                .ok_or_else(|| {
                    ApiError::BadRequest("GitHub pull_request number is missing".into())
                })?;
            let Some(revision) = normalize_commit_revision(
                payload
                    .get("pull_request")
                    .and_then(|value| value.get("head"))
                    .and_then(|value| value.get("sha")),
                "GitHub pull_request head.sha",
            )?
            else {
                return Ok(None);
            };
            Ok(Some(WebhookBuildRequest {
                event_id,
                project,
                revision: Some(revision),
                fetch_ref: Some(github_pull_request_refspec(number)?),
                remote: Some("origin".into()),
                fetch: true,
                credential_id,
                submodules: false,
                parameters: BTreeMap::new(),
                upstream: None,
            }))
        }
        "repository_dispatch" => {
            let client_payload = payload
                .get("client_payload")
                .and_then(serde_json::Value::as_object)
                .ok_or_else(|| {
                    ApiError::BadRequest(
                        "GitHub repository_dispatch client_payload is missing".into(),
                    )
                })?;
            let Some(revision) = normalize_commit_revision(
                client_payload.get("rivet_revision"),
                "GitHub repository_dispatch client_payload.rivet_revision",
            )?
            else {
                return Ok(None);
            };
            Ok(Some(WebhookBuildRequest {
                event_id,
                project,
                revision: Some(revision),
                fetch_ref: None,
                remote: Some("origin".into()),
                fetch: true,
                credential_id,
                submodules: false,
                parameters: webhook_parameters(
                    client_payload.get("rivet_parameters"),
                    "GitHub repository_dispatch",
                )?,
                upstream: None,
            }))
        }
        _ => Err(ApiError::BadRequest(format!(
            "unsupported GitHub webhook event: {event}"
        ))),
    }
}

fn normalize_gitlab_webhook(
    secret: &[u8],
    headers: &HeaderMap,
    body: &[u8],
    project: String,
    credential_id: Option<String>,
) -> Result<Option<WebhookBuildRequest>, ApiError> {
    verify_gitlab_webhook_signature(secret, headers, body)?;
    let event = required_header(headers, "x-gitlab-event")?;
    let event_id = first_header(
        headers,
        &["webhook-id", "x-gitlab-event-uuid", "idempotency-key"],
    )
    .ok_or(ApiError::EmptyWebhookEventId)?;
    let payload: serde_json::Value = serde_json::from_slice(body)
        .map_err(|error| ApiError::BadRequest(format!("invalid GitLab webhook JSON: {error}")))?;
    match event.as_str() {
        "Push Hook" | "Tag Push Hook" => {
            let Some(revision) = normalize_commit_revision(payload.get("after"), "GitLab after")?
            else {
                return Ok(None);
            };
            Ok(Some(WebhookBuildRequest {
                event_id,
                project,
                revision: Some(revision),
                fetch_ref: None,
                remote: Some("origin".into()),
                fetch: true,
                credential_id,
                submodules: false,
                parameters: BTreeMap::new(),
                upstream: None,
            }))
        }
        "Merge Request Hook" => {
            let attributes = payload.get("object_attributes").ok_or_else(|| {
                ApiError::BadRequest("GitLab merge request attributes are missing".into())
            })?;
            let action = attributes
                .get("action")
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| {
                    ApiError::BadRequest("GitLab merge request action is missing".into())
                })?;
            if !matches!(action, "open" | "reopen" | "update") {
                return Ok(None);
            }
            let iid = attributes
                .get("iid")
                .and_then(serde_json::Value::as_u64)
                .ok_or_else(|| {
                    ApiError::BadRequest("GitLab merge request IID is missing".into())
                })?;
            let Some(revision) = normalize_commit_revision(
                attributes
                    .get("last_commit")
                    .and_then(|value| value.get("id")),
                "GitLab merge request last_commit.id",
            )?
            else {
                return Ok(None);
            };
            Ok(Some(WebhookBuildRequest {
                event_id,
                project,
                revision: Some(revision),
                fetch_ref: Some(gitlab_merge_request_refspec(iid)?),
                remote: Some("origin".into()),
                fetch: true,
                credential_id,
                submodules: false,
                parameters: BTreeMap::new(),
                upstream: None,
            }))
        }
        _ => Err(ApiError::BadRequest(format!(
            "unsupported GitLab webhook event: {event}"
        ))),
    }
}

fn normalize_bitbucket_webhook(
    secret: &[u8],
    headers: &HeaderMap,
    body: &[u8],
    project: String,
    credential_id: Option<String>,
) -> Result<Option<WebhookBuildRequest>, ApiError> {
    verify_hmac_hex_signature(secret, headers, "x-hub-signature", body)?;
    let event = required_header(headers, "x-event-key")?;
    let event_id = required_header(headers, "x-request-uuid")?;
    let payload: serde_json::Value = serde_json::from_slice(body).map_err(|error| {
        ApiError::BadRequest(format!("invalid Bitbucket webhook JSON: {error}"))
    })?;

    match event.as_str() {
        "repo:push" => {
            let changes = payload
                .get("push")
                .and_then(|value| value.get("changes"))
                .and_then(serde_json::Value::as_array)
                .ok_or_else(|| ApiError::BadRequest("Bitbucket push changes are missing".into()))?;
            let active_changes = changes
                .iter()
                .filter(|change| change.get("new").is_some_and(|value| !value.is_null()))
                .collect::<Vec<_>>();
            if active_changes.is_empty() {
                return Ok(None);
            }
            if active_changes.len() > 1 {
                return Err(ApiError::BadRequest(
                    "Bitbucket push contains multiple updated refs; use one webhook delivery per ref"
                        .into(),
                ));
            }
            let Some(revision) = normalize_commit_revision(
                active_changes[0]
                    .get("new")
                    .and_then(|value| value.get("target"))
                    .and_then(|value| value.get("hash")),
                "Bitbucket push new.target.hash",
            )?
            else {
                return Ok(None);
            };
            Ok(Some(WebhookBuildRequest {
                event_id,
                project,
                revision: Some(revision),
                fetch_ref: None,
                remote: Some("origin".into()),
                fetch: true,
                credential_id,
                submodules: false,
                parameters: BTreeMap::new(),
                upstream: None,
            }))
        }
        "pullrequest:created" | "pullrequest:updated" => {
            let pull_request = payload.get("pullrequest").ok_or_else(|| {
                ApiError::BadRequest("Bitbucket pullrequest payload is missing".into())
            })?;
            let number = pull_request
                .get("id")
                .and_then(serde_json::Value::as_u64)
                .ok_or_else(|| {
                    ApiError::BadRequest("Bitbucket pullrequest id is missing".into())
                })?;
            let Some(revision) = normalize_commit_revision(
                pull_request
                    .get("source")
                    .and_then(|value| value.get("commit"))
                    .and_then(|value| value.get("hash")),
                "Bitbucket pullrequest source.commit.hash",
            )?
            else {
                return Ok(None);
            };
            Ok(Some(WebhookBuildRequest {
                event_id,
                project,
                revision: Some(revision),
                fetch_ref: Some(bitbucket_pull_request_refspec(number)?),
                remote: Some("origin".into()),
                fetch: true,
                credential_id,
                submodules: false,
                parameters: BTreeMap::new(),
                upstream: None,
            }))
        }
        // Bitbucket sends these signed lifecycle/review events to the same
        // webhook endpoint when a repository webhook subscribes to them.
        // They are valid deliveries, but they do not represent a new source
        // revision for a Rivet build. Acknowledge them after signature and
        // delivery-ID validation so Bitbucket does not retry them as failures.
        "repo:fork"
        | "repo:updated"
        | "repo:transfer"
        | "repo:commit_comment_created"
        | "repo:commit_status_created"
        | "repo:commit_status_updated"
        | "repo:deleted"
        | "pullrequest:changes_request_created"
        | "pullrequest:changes_request_removed"
        | "pullrequest:approved"
        | "pullrequest:unapproved"
        | "pullrequest:fulfilled"
        | "pullrequest:rejected"
        | "pullrequest:comment_created"
        | "pullrequest:comment_updated"
        | "pullrequest:comment_deleted"
        | "pullrequest:comment_resolved"
        | "pullrequest:comment_reopened"
        | "pipeline:span_created" => Ok(None),
        _ => Err(ApiError::BadRequest(format!(
            "unsupported Bitbucket webhook event: {event}"
        ))),
    }
}

fn github_pull_request_refspec(number: u64) -> Result<String, ApiError> {
    provider_pull_request_refspec("refs/pull", number)
}

fn gitlab_merge_request_refspec(iid: u64) -> Result<String, ApiError> {
    provider_pull_request_refspec("refs/merge-requests", iid)
}

fn bitbucket_pull_request_refspec(number: u64) -> Result<String, ApiError> {
    validate_pull_request_number(number)?;
    Ok(format!(
        "+refs/pull-requests/{number}/from:refs/remotes/origin/pull-requests/{number}"
    ))
}

fn provider_pull_request_refspec(prefix: &str, number: u64) -> Result<String, ApiError> {
    validate_pull_request_number(number)?;
    let destination = prefix.strip_prefix("refs/").unwrap_or(prefix);
    Ok(format!(
        "+{prefix}/{number}/head:refs/remotes/origin/{destination}/{number}"
    ))
}

fn validate_pull_request_number(number: u64) -> Result<(), ApiError> {
    if number == 0 || number > 9_999_999_999 {
        return Err(ApiError::BadRequest(
            "pull request number is outside the supported range".into(),
        ));
    }
    Ok(())
}

fn normalize_commit_revision(
    value: Option<&serde_json::Value>,
    field: &str,
) -> Result<Option<String>, ApiError> {
    let revision = value
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| ApiError::BadRequest(format!("{field} is missing from webhook payload")))?;
    if revision.chars().all(|character| character == '0') {
        return Ok(None);
    }
    if (revision.len() != 40 && revision.len() != 64)
        || revision.bytes().any(|byte| !byte.is_ascii_hexdigit())
    {
        return Err(ApiError::BadRequest(format!(
            "{field} must be a 40- or 64-character commit SHA"
        )));
    }
    Ok(Some(revision.to_owned()))
}

fn webhook_parameters(
    value: Option<&serde_json::Value>,
    provider_event: &str,
) -> Result<BTreeMap<String, String>, ApiError> {
    let Some(value) = value else {
        return Ok(BTreeMap::new());
    };
    let parameters =
        serde_json::from_value::<BTreeMap<String, String>>(value.clone()).map_err(|error| {
            ApiError::BadRequest(format!("{provider_event} parameters are invalid: {error}"))
        })?;
    if parameters.len() > 64 {
        return Err(ApiError::BadRequest(format!(
            "{provider_event} accepts at most 64 parameters"
        )));
    }
    Ok(parameters)
}

fn required_header(headers: &HeaderMap, name: &'static str) -> Result<String, ApiError> {
    first_header(headers, &[name])
        .ok_or_else(|| ApiError::BadRequest(format!("required webhook header is missing: {name}")))
}

fn first_header(headers: &HeaderMap, names: &[&str]) -> Option<String> {
    names.iter().find_map(|name| {
        headers
            .get(*name)
            .and_then(|value| value.to_str().ok())
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_owned)
    })
}

fn verify_hmac_hex_signature(
    secret: &[u8],
    headers: &HeaderMap,
    header_name: &'static str,
    body: &[u8],
) -> Result<(), ApiError> {
    let value = headers
        .get(header_name)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("sha256="))
        .ok_or(ApiError::InvalidWebhookSignature)?;
    let signature = hex::decode(value).map_err(|_| ApiError::InvalidWebhookSignature)?;
    let mut mac =
        Hmac::<Sha256>::new_from_slice(secret).map_err(|_| ApiError::InvalidWebhookSignature)?;
    mac.update(body);
    mac.verify_slice(&signature)
        .map_err(|_| ApiError::InvalidWebhookSignature)
}

fn verify_gitlab_webhook_signature(
    secret: &[u8],
    headers: &HeaderMap,
    body: &[u8],
) -> Result<(), ApiError> {
    if let Some(signature_header) = first_header(headers, &["webhook-signature"]) {
        let message_id = required_header(headers, "webhook-id")?;
        let timestamp = required_header(headers, "webhook-timestamp")?;
        let timestamp = timestamp
            .parse::<i64>()
            .map_err(|_| ApiError::InvalidWebhookSignature)?;
        if Utc::now().timestamp().abs_diff(timestamp) > 300 {
            return Err(ApiError::InvalidWebhookSignature);
        }
        let encoded_key = secret
            .strip_prefix(b"whsec_")
            .ok_or(ApiError::InvalidWebhookSignature)?;
        let key = STANDARD
            .decode(encoded_key)
            .map_err(|_| ApiError::InvalidWebhookSignature)?;
        let mut message =
            Vec::with_capacity(message_id.len() + timestamp.to_string().len() + body.len() + 2);
        message.extend_from_slice(message_id.as_bytes());
        message.push(b'.');
        message.extend_from_slice(timestamp.to_string().as_bytes());
        message.push(b'.');
        message.extend_from_slice(body);
        let mut mac =
            Hmac::<Sha256>::new_from_slice(&key).map_err(|_| ApiError::InvalidWebhookSignature)?;
        mac.update(&message);
        let expected = format!("v1,{}", STANDARD.encode(mac.finalize().into_bytes()));
        let valid = signature_header
            .split_whitespace()
            .any(|received| bool::from(expected.as_bytes().ct_eq(received.as_bytes())));
        if !valid {
            return Err(ApiError::InvalidWebhookSignature);
        }
        return Ok(());
    }

    let supplied = required_header(headers, "x-gitlab-token")?;
    if bool::from(supplied.as_bytes().ct_eq(secret)) {
        Ok(())
    } else {
        Err(ApiError::InvalidWebhookSignature)
    }
}

fn verify_webhook_signature(
    secret: &[u8],
    headers: &HeaderMap,
    body: &[u8],
) -> Result<(), ApiError> {
    verify_hmac_hex_signature(secret, headers, "x-rivet-signature", body)
}

fn validate_webhook_event_id(event_id: String) -> Result<String, ApiError> {
    let event_id = event_id.trim();
    if event_id.is_empty() {
        return Err(ApiError::EmptyWebhookEventId);
    }
    if event_id.len() > 256 {
        return Err(ApiError::WebhookEventIdTooLong);
    }
    if event_id.chars().any(char::is_control) {
        return Err(ApiError::InvalidWebhookEventId);
    }
    Ok(event_id.to_owned())
}

fn repository_poll_event_id(
    project_id: rivet_core::ProjectId,
    source: &SourceSnapshot,
    previous_build: Option<&BuildRecord>,
) -> String {
    let previous_build = previous_build
        .map(|build| build.id.to_string())
        .unwrap_or_else(|| "initial".to_owned());
    let mut digest = Sha256::new();
    digest.update(project_id.as_bytes());
    digest.update([0]);
    digest.update(source.provider.as_bytes());
    digest.update([0]);
    digest.update(source.revision.as_bytes());
    digest.update([0]);
    digest.update(source.reference.as_deref().unwrap_or_default().as_bytes());
    digest.update([0]);
    digest.update(previous_build.as_bytes());
    format!("rivet-poll-{}", hex::encode(digest.finalize()))
}

fn spawn_schedule_dispatcher(state: AppState, shutdown: CancellationToken) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => break,
                _ = interval.tick() => {
                    if let Err(error) = dispatch_due_schedules(&state, Utc::now()).await {
                        tracing::error!(?error, "Rivet schedule dispatcher failed");
                    }
                }
            }
        }
    });
}

fn spawn_remote_recovery_dispatcher(
    state: AppState,
    attempts: Vec<RemoteAttemptRecord>,
    shutdown: CancellationToken,
) {
    tokio::spawn(async move {
        let mut pending = attempts
            .into_iter()
            .map(|attempt| (attempt.build_id, attempt))
            .collect::<BTreeMap<_, _>>();
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => break,
                _ = interval.tick() => {
                    let build_ids = pending.keys().copied().collect::<Vec<_>>();
                    for build_id in build_ids {
                        let Some(attempt) = pending.get(&build_id).cloned() else {
                            continue;
                        };
                        let details = match state.storage.get_build_details(build_id) {
                            Ok(Some(details)) => details,
                            Ok(None) => {
                                pending.remove(&build_id);
                                continue;
                            }
                            Err(error) => {
                                tracing::error!(?error, %build_id, "could not inspect remote attempt during recovery");
                                continue;
                            }
                        };
                        if details.build.status.is_terminal() {
                            if let Err(error) = state.storage.delete_remote_attempt(build_id) {
                                tracing::warn!(?error, %build_id, "could not remove terminal remote attempt");
                            }
                            pending.remove(&build_id);
                            continue;
                        }
                        if state.active_builds.lock().await.contains_key(&build_id) {
                            continue;
                        }
                        let requirements = match serde_json::from_str::<AgentRequirements>(&attempt.requirements_json) {
                            Ok(requirements) => requirements,
                            Err(error) => {
                                tracing::error!(?error, %build_id, "remote attempt requirements are invalid");
                                continue;
                            }
                        };
                        let reservation = match state
                            .agents
                            .reserve_for_agent(&requirements, build_id, attempt.agent_id, Utc::now())
                            .await
                        {
                            Ok(reservation) => reservation,
                            Err(AgentRegistryError::NoMatchingAgent)
                                if attempt.recovery_attempts == 0 => match state
                                    .agents
                                    .reserve(&requirements, build_id, Utc::now())
                                    .await
                                {
                                    Ok(reservation) => reservation,
                                    Err(AgentRegistryError::NoMatchingAgent) => continue,
                                    Err(error) => {
                                        tracing::debug!(?error, %build_id, "no replacement agent is ready for remote recovery");
                                        continue;
                                    }
                                },
                            Err(AgentRegistryError::NoMatchingAgent) => {
                                // Once a replacement has been committed, a
                                // restart must resume that exact durable
                                // attempt instead of silently consuming a
                                // second replacement slot.
                                continue;
                            }
                            Err(AgentRegistryError::ReservationConflict(_)) => continue,
                            Err(error) => {
                                tracing::debug!(?error, %build_id, "original agent is not ready for remote recovery");
                                continue;
                            }
                        };
                        if reservation.agent_id != attempt.agent_id {
                            match state
                                .storage
                                .set_remote_attempt_agent(build_id, reservation.agent_id)
                            {
                                Ok(true) => {}
                                Ok(false) => {
                                    state.agents.release(&reservation).await;
                                    tracing::debug!(%build_id, "remote recovery attempt disappeared before agent handoff");
                                    continue;
                                }
                                Err(error) => {
                                    state.agents.release(&reservation).await;
                                    tracing::error!(?error, %build_id, "could not persist remote recovery agent handoff");
                                    continue;
                                }
                            }
                        }
                        let Some(project) = (match state.storage.get_project_by_id(attempt.project_id) {
                            Ok(project) => project,
                            Err(error) => {
                                tracing::error!(?error, %build_id, "could not load project for remote recovery");
                                None
                            }
                        }) else {
                            state.agents.release(&reservation).await;
                            continue;
                        };
                        let workspace = PathBuf::from(project.repository_path);
                        if let Err(error) = attempt.pipeline.resolve_workspace(&workspace) {
                            tracing::error!(?error, %build_id, "remote recovery workspace is no longer valid");
                            state.agents.release(&reservation).await;
                            continue;
                        }
                        if details.build.status == BuildStatus::Pending {
                            if let Err(error) = state.storage.apply_event(&BuildEvent::BuildQueued {
                                build_id,
                                project_id: attempt.project_id,
                                timestamp: Utc::now(),
                            }) {
                                tracing::error!(?error, %build_id, "could not queue recovered remote build");
                                state.agents.release(&reservation).await;
                                continue;
                            }
                        }
                        let cancellation = CancellationToken::new();
                        let already_active = state
                            .active_builds
                            .lock()
                            .await
                            .insert(build_id, cancellation.clone())
                            .is_some();
                        if already_active {
                            state.agents.release(&reservation).await;
                            continue;
                        }
                        let (events, received_events) = mpsc::channel(512);
                        spawn_event_projector(
                            state.storage.clone(),
                            state.events.clone(),
                            attempt.pipeline.clone(),
                            workspace.clone(),
                            false,
                            received_events,
                        );
                        spawn_remote_build_task(
                            state.clone(),
                            reservation,
                            requirements,
                            attempt.plan.clone(),
                            attempt.pipeline,
                            workspace,
                            attempt.parameters,
                            cancellation,
                            events,
                            attempt.attempt_id,
                            Some(details),
                        );
                        pending.remove(&build_id);
                    }
                }
            }
        }
    });
}

async fn wait_for_shutdown_signal(shutdown: CancellationToken) {
    let source = {
        #[cfg(unix)]
        {
            let mut terminate =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                    .expect("install SIGTERM handler");
            tokio::select! {
                _ = shutdown.cancelled() => "admin_api",
                _ = tokio::signal::ctrl_c() => "signal",
                _ = terminate.recv() => "signal",
            }
        }
        #[cfg(not(unix))]
        {
            tokio::select! {
                _ = shutdown.cancelled() => "admin_api",
                _ = tokio::signal::ctrl_c() => "signal",
            }
        }
    };
    tracing::info!(source, "Rivet server shutdown requested");
}

async fn dispatch_due_schedules(state: &AppState, now: DateTime<Utc>) -> Result<usize, ApiError> {
    let due = state.storage.due_schedules(now)?;
    let mut dispatched = 0;
    for schedule in due {
        let expression = match CronExpression::parse(&schedule.expression) {
            Ok(expression) => expression,
            Err(error) => {
                tracing::error!(
                    schedule = %schedule.id,
                    ?error,
                    "persisted schedule expression is invalid"
                );
                continue;
            }
        };
        let next_run_at = match expression.next_after(now) {
            Ok(next_run_at) => next_run_at,
            Err(error) => {
                tracing::error!(
                    schedule = %schedule.id,
                    ?error,
                    "could not calculate next schedule occurrence"
                );
                continue;
            }
        };
        if !state.storage.claim_schedule(
            schedule.id,
            schedule.next_run_at,
            schedule.next_run_at,
            next_run_at,
        )? {
            continue;
        }
        let Some(project) = state.storage.get_project_by_id(schedule.project_id)? else {
            tracing::error!(schedule = %schedule.id, "schedule project disappeared");
            continue;
        };
        let queued = match schedule.trigger {
            ScheduleTrigger::Build => {
                enqueue_project_build(state, project, QueueBuildRequest::default())
                    .await
                    .map(|_| true)
            }
            ScheduleTrigger::RepositoryPoll => {
                let poll = schedule
                    .poll
                    .map(|poll| RepositoryPollRequest {
                        remote: poll.remote,
                        fetch: poll.fetch,
                        credential_id: poll.credential_id,
                    })
                    .unwrap_or_default();
                poll_repository_changes_for_project(state, project, poll)
                    .await
                    .map(|response| response.status == "queued")
            }
        };
        match queued {
            Ok(true) => dispatched += 1,
            Ok(false) => {
                tracing::debug!(schedule = %schedule.id, "repository poll found no new revision")
            }
            Err(error) => {
                tracing::error!(schedule = %schedule.id, ?error, "scheduled trigger could not be dispatched");
            }
        }
    }
    Ok(dispatched)
}

fn validate_schedule_name(name: String) -> Result<String, ApiError> {
    let name = name.trim();
    if name.is_empty() {
        return Err(ApiError::BadRequest("schedule name cannot be empty".into()));
    }
    if name.contains('/') || name.contains('\\') || name.chars().any(char::is_control) {
        return Err(ApiError::BadRequest(
            "schedule name cannot contain path separators or control characters".into(),
        ));
    }
    Ok(name.to_owned())
}

fn normalize_repository_poll_request(
    mut request: RepositoryPollRequest,
) -> Result<RepositoryPollRequest, ApiError> {
    let remote = request.remote.trim();
    if remote.is_empty() {
        request.remote = default_remote();
    } else if remote.len() > 256 || remote.starts_with('-') || remote.chars().any(char::is_control)
    {
        return Err(ApiError::BadRequest(
            "repository poll remote must be a bounded Git remote name".into(),
        ));
    } else {
        request.remote = remote.to_owned();
    }
    if let Some(credential_id) = request.credential_id.as_mut() {
        let trimmed = credential_id.trim();
        if trimmed.is_empty() || trimmed.len() > 256 || trimmed.chars().any(char::is_control) {
            return Err(ApiError::BadRequest(
                "repository poll credential_id must be a bounded opaque ID".into(),
            ));
        }
        *credential_id = trimmed.to_owned();
    }
    if request.credential_id.is_some() && !request.fetch {
        return Err(ApiError::BadRequest(
            "a repository poll credential requires fetch=true".into(),
        ));
    }
    Ok(request)
}

async fn create_project(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Json(request): Json<CreateProjectRequest>,
) -> Result<(StatusCode, Json<Project>), ApiError> {
    require_global(&principal, Permission::Administer)?;
    let has_repository_path = !request.repository_path.as_os_str().is_empty();
    let repository_url = request
        .repository_url
        .as_deref()
        .map(str::trim)
        .filter(|url| !url.is_empty());
    if has_repository_path == repository_url.is_some() {
        return Err(ApiError::BadRequest(
            "provide exactly one of repository_path or repository_url".into(),
        ));
    }

    let repository = if let Some(repository_url) = repository_url {
        let destination = request.clone_destination.clone().ok_or_else(|| {
            ApiError::BadRequest("repository_url requires an explicit clone_destination".into())
        })?;
        let credential = resolve_git_credential(
            state.credentials.as_ref(),
            request.credential_id.as_deref(),
            &request.name,
        )
        .await?;
        let snapshot = GitRepository::clone_repository_with_auth(
            &GitCloneOptions {
                remote: repository_url.to_owned(),
                destination,
                branch: request.branch.clone(),
                depth: request.depth,
                revision: request.revision.clone(),
                submodules: request.submodules,
                credential_id: request.credential_id.clone(),
                known_hosts_file: state.ssh_known_hosts_file.clone(),
            },
            credential.as_ref(),
        )
        .await?;
        snapshot.root
    } else {
        if request.clone_destination.is_some()
            || request.branch.is_some()
            || request.depth.is_some()
            || request.revision.is_some()
            || request.submodules
            || request.credential_id.is_some()
        {
            return Err(ApiError::BadRequest(
                "clone options require repository_url".into(),
            ));
        }
        canonical_directory(&request.repository_path)?
    };
    let pipeline_path = request
        .pipeline_path
        .unwrap_or_else(|| repository.join("Rivetfile.toml"));
    if !pipeline_path.is_file() {
        return Err(ApiError::InvalidPipeline(pipeline_path));
    }
    let pipeline_path = std::fs::canonicalize(&pipeline_path)
        .map_err(|_| ApiError::InvalidPipeline(pipeline_path.clone()))?;
    let pipeline = Pipeline::load(&pipeline_path)?;
    let project = Project::new(
        request.name,
        repository.to_string_lossy().into_owned(),
        pipeline_path.to_string_lossy().into_owned(),
    )?;
    state.storage.create_project(&project, &pipeline)?;
    Ok((StatusCode::CREATED, Json(project)))
}

async fn list_builds(
    State(state): State<AppState>,
    AxumPath(name): AxumPath<String>,
    Extension(principal): Extension<Principal>,
) -> Result<Json<Vec<BuildRecord>>, ApiError> {
    require_project(&principal, Permission::Read, &name)?;
    let project = project_by_name(&state.storage, &name)?;
    Ok(Json(state.storage.list_builds(project.id)?))
}

async fn get_build(
    State(state): State<AppState>,
    AxumPath((name, number)): AxumPath<(String, i64)>,
    Extension(principal): Extension<Principal>,
) -> Result<Json<BuildDetails>, ApiError> {
    require_project(&principal, Permission::Read, &name)?;
    let project = project_by_name(&state.storage, &name)?;
    let build = build_by_number(&state.storage, project.id, &name, number)?;
    state
        .storage
        .get_build_details(build.id)?
        .map(Json)
        .ok_or(ApiError::BuildNotFound {
            project: name,
            number,
        })
}

async fn get_logs(
    State(state): State<AppState>,
    AxumPath((name, number)): AxumPath<(String, i64)>,
    Extension(principal): Extension<Principal>,
    Query(query): Query<LogQuery>,
) -> Result<Response, ApiError> {
    require_project(&principal, Permission::Read, &name)?;
    let project = project_by_name(&state.storage, &name)?;
    let build = build_by_number(&state.storage, project.id, &name, number)?;
    let after = query.after.unwrap_or(-1);
    if after < -1 {
        return Err(ApiError::BadRequest(
            "log cursor must be -1 or a non-negative sequence".into(),
        ));
    }
    let limit = query.limit.unwrap_or(DEFAULT_LOG_PAGE_SIZE);
    if !(1..=MAX_LOG_PAGE_SIZE).contains(&limit) {
        return Err(ApiError::BadRequest(format!(
            "log limit must be between 1 and {MAX_LOG_PAGE_SIZE}"
        )));
    }
    let logs = state.storage.logs_page(build.id, after, limit)?;
    let next_after = (logs.len() == limit)
        .then(|| logs.last().map(|log| log.sequence))
        .flatten();
    let mut response = Json(logs).into_response();
    if let Some(next_after) = next_after
        && let Ok(value) = HeaderValue::from_str(&next_after.to_string())
    {
        response
            .headers_mut()
            .insert(HeaderName::from_static("x-rivet-log-next-after"), value);
    }
    Ok(response)
}

async fn get_artifacts(
    State(state): State<AppState>,
    AxumPath((name, number)): AxumPath<(String, i64)>,
    Extension(principal): Extension<Principal>,
) -> Result<Json<Vec<ArtifactRecord>>, ApiError> {
    require_project(&principal, Permission::Read, &name)?;
    let project = project_by_name(&state.storage, &name)?;
    let build = build_by_number(&state.storage, project.id, &name, number)?;
    Ok(Json(state.storage.artifacts(build.id)?))
}

async fn list_annotations(
    State(state): State<AppState>,
    AxumPath((name, number)): AxumPath<(String, i64)>,
    Extension(principal): Extension<Principal>,
) -> Result<Json<Vec<AnnotationRecord>>, ApiError> {
    require_project(&principal, Permission::Read, &name)?;
    let project = project_by_name(&state.storage, &name)?;
    let build = build_by_number(&state.storage, project.id, &name, number)?;
    Ok(Json(state.storage.annotations(build.id)?))
}

async fn create_annotation(
    State(state): State<AppState>,
    AxumPath((name, number)): AxumPath<(String, i64)>,
    Extension(principal): Extension<Principal>,
    Json(request): Json<CreateAnnotationRequest>,
) -> Result<Json<AnnotationRecord>, ApiError> {
    require_project(&principal, Permission::Build, &name)?;
    let project = project_by_name(&state.storage, &name)?;
    let build = build_by_number(&state.storage, project.id, &name, number)?;
    Ok(Json(state.storage.add_annotation(
        build.id,
        request.stage_id,
        &request.kind,
        &request.message,
    )?))
}

async fn download_artifact(
    State(state): State<AppState>,
    AxumPath((name, number, artifact_id)): AxumPath<(String, i64, uuid::Uuid)>,
    Extension(principal): Extension<Principal>,
) -> Result<Response, ApiError> {
    require_project(&principal, Permission::Read, &name)?;
    let project = project_by_name(&state.storage, &name)?;
    let build = build_by_number(&state.storage, project.id, &name, number)?;
    let Some((artifact, path)) = state.storage.artifact_file(artifact_id)? else {
        return Err(ApiError::ArtifactNotFound(artifact_id));
    };
    if artifact.build_id != build.id {
        return Err(ApiError::ArtifactNotFound(artifact_id));
    }
    let (file, content_length) = verified_artifact_file(&path, &artifact)?;
    let filename = artifact
        .relative_path
        .rsplit('/')
        .next()
        .filter(|value| !value.is_empty())
        .unwrap_or("artifact")
        .replace(['"', '\r', '\n'], "_");
    let file = tokio::fs::File::from_std(file);
    let mut response = Response::new(Body::from_stream(tokio_util::io::ReaderStream::new(file)));
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/octet-stream"),
    );
    if let Ok(value) = HeaderValue::from_str(&content_length.to_string()) {
        response.headers_mut().insert(header::CONTENT_LENGTH, value);
    }
    if let Ok(value) = HeaderValue::from_str(&format!("\"{}\"", artifact.checksum)) {
        response.headers_mut().insert(header::ETAG, value);
    }
    if let Ok(value) = HeaderValue::from_str(&artifact.checksum) {
        response
            .headers_mut()
            .insert(HeaderName::from_static("x-rivet-artifact-sha256"), value);
    }
    if let Ok(value) = HeaderValue::from_str(&format!("attachment; filename=\"{filename}\"")) {
        response
            .headers_mut()
            .insert(header::CONTENT_DISPOSITION, value);
    }
    Ok(response)
}

fn verified_artifact_file(
    path: &Path,
    artifact: &ArtifactRecord,
) -> Result<(std::fs::File, u64), ApiError> {
    let metadata = std::fs::symlink_metadata(path).map_err(ApiError::ArtifactRead)?;
    if !metadata.file_type().is_file() || metadata.len() != artifact.size_bytes {
        return Err(ApiError::ArtifactIntegrity);
    }

    let mut file = std::fs::File::open(path).map_err(ApiError::ArtifactRead)?;
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer).map_err(ApiError::ArtifactRead)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    let checksum = format!("sha256:{:x}", hasher.finalize());
    if checksum != artifact.checksum {
        return Err(ApiError::ArtifactIntegrity);
    }
    file.seek(SeekFrom::Start(0))
        .map_err(ApiError::ArtifactRead)?;
    Ok((file, metadata.len()))
}

async fn queue_build(
    State(state): State<AppState>,
    AxumPath(name): AxumPath<String>,
    Extension(principal): Extension<Principal>,
    request: Option<Json<QueueBuildRequest>>,
) -> Result<(StatusCode, Json<QueueBuildResponse>), ApiError> {
    require_project(&principal, Permission::Build, &name)?;
    let request = request.map(|Json(request)| request).unwrap_or_default();
    let project = project_by_name(&state.storage, &name)?;
    Ok((
        StatusCode::ACCEPTED,
        Json(enqueue_project_build(&state, project, request).await?),
    ))
}

async fn poll_repository_changes(
    State(state): State<AppState>,
    AxumPath(name): AxumPath<String>,
    Extension(principal): Extension<Principal>,
    request: Option<Json<RepositoryPollRequest>>,
) -> Result<(StatusCode, Json<RepositoryPollResponse>), ApiError> {
    require_project(&principal, Permission::Build, &name)?;
    let project = project_by_name(&state.storage, &name)?;
    let request = request.map(|Json(request)| request).unwrap_or_default();
    let response = poll_repository_changes_for_project(&state, project, request).await?;
    let status = if response.status == "queued" {
        StatusCode::ACCEPTED
    } else {
        StatusCode::OK
    };
    Ok((status, Json(response)))
}

async fn poll_repository_changes_for_project(
    state: &AppState,
    project: Project,
    request: RepositoryPollRequest,
) -> Result<RepositoryPollResponse, ApiError> {
    let request = normalize_repository_poll_request(request)?;

    let prepare = PrepareScmRequest {
        remote: if request.remote.trim().is_empty() {
            default_remote()
        } else {
            request.remote
        },
        fetch: request.fetch,
        revision: None,
        fetch_ref: None,
        clean: false,
        clean_ignored: false,
        submodules: false,
        credential_id: request.credential_id,
    };
    let source = capture_source_snapshot(
        Path::new(&project.repository_path),
        &project.name,
        Some(&prepare),
        state.credentials.as_ref(),
        state.ssh_known_hosts_file.as_deref(),
    )
    .await?
    .ok_or_else(|| ApiError::InvalidRepository(PathBuf::from(&project.repository_path)))?;

    let latest = state.storage.list_builds(project.id)?.into_iter().next();
    let unchanged = latest
        .as_ref()
        .and_then(|build| build.source.as_ref())
        .is_some_and(|known| {
            known.provider == source.provider && known.revision == source.revision
        });
    if unchanged {
        return Ok(RepositoryPollResponse {
            status: "unchanged",
            changed: false,
            deduplicated: false,
            revision: source.revision,
            reference: source.reference,
            build: None,
        });
    }

    let event_id = repository_poll_event_id(project.id, &source, latest.as_ref());
    if !state.storage.claim_repository_poll_event(
        &event_id,
        project.id,
        &source.revision,
        Utc::now(),
    )? {
        let event = state
            .storage
            .repository_poll_event(&event_id)?
            .ok_or_else(|| ApiError::BadRequest("repository poll event disappeared".into()))?;
        let build = event.build_id.and_then(|build_id| {
            state
                .storage
                .list_builds(project.id)
                .ok()?
                .into_iter()
                .find(|build| build.id == build_id)
        });
        return Ok(RepositoryPollResponse {
            status: if build.is_some() {
                "already_queued"
            } else {
                "already_checking"
            },
            changed: true,
            deduplicated: true,
            revision: source.revision,
            reference: source.reference,
            build,
        });
    }

    // Build the exact revision observed by this poll. The initial fetch has
    // already happened, so admission does not repeat the network operation.
    let queued = match enqueue_project_build(
        &state,
        project,
        QueueBuildRequest {
            scm: Some(PrepareScmRequest {
                remote: source.remote.clone().unwrap_or_else(default_remote),
                fetch: false,
                revision: Some(source.revision.clone()),
                fetch_ref: None,
                clean: false,
                clean_ignored: false,
                submodules: false,
                credential_id: None,
            }),
            parameters: BTreeMap::new(),
            priority: 0,
        },
    )
    .await
    {
        Ok(queued) => queued,
        Err(error) => {
            state.storage.release_repository_poll_event(&event_id)?;
            return Err(error);
        }
    };
    state.storage.complete_repository_poll_event(
        &event_id,
        queued.build.id,
        queued.build.number,
    )?;
    Ok(RepositoryPollResponse {
        status: "queued",
        changed: true,
        deduplicated: false,
        revision: source.revision,
        reference: source.reference,
        build: Some(queued.build),
    })
}

async fn retry_build(
    State(state): State<AppState>,
    AxumPath((name, number)): AxumPath<(String, i64)>,
    Extension(principal): Extension<Principal>,
    request: Option<Json<QueueBuildRequest>>,
) -> Result<(StatusCode, Json<QueueBuildResponse>), ApiError> {
    require_project(&principal, Permission::Build, &name)?;
    let project = project_by_name(&state.storage, &name)?;
    let pipeline = Pipeline::load(&project.pipeline_path)?;
    let original = build_by_number(&state.storage, project.id, &name, number)?;
    if !original.status.is_terminal() {
        return Err(ApiError::BadRequest(format!(
            "build {name} #{number} is not finished and cannot be retried"
        )));
    }
    let mut request = request.map(|Json(request)| request).unwrap_or_default();
    if request.parameters.is_empty() {
        if pipeline.has_secret_parameters() {
            return Err(ApiError::BadRequest(
                "retry requires explicit values for secret parameters".into(),
            ));
        }
        request.parameters = original.parameters;
    }
    Ok((
        StatusCode::ACCEPTED,
        Json(enqueue_project_build(&state, project, request).await?),
    ))
}

async fn enqueue_project_build(
    state: &AppState,
    project: Project,
    request: QueueBuildRequest,
) -> Result<QueueBuildResponse, ApiError> {
    if !(MIN_QUEUE_PRIORITY..=MAX_QUEUE_PRIORITY).contains(&request.priority) {
        return Err(ApiError::BadRequest(format!(
            "build priority must be between {MIN_QUEUE_PRIORITY} and {MAX_QUEUE_PRIORITY}"
        )));
    }
    let repository_root = PathBuf::from(&project.repository_path);
    let source = capture_source_snapshot(
        &repository_root,
        &project.name,
        request.scm.as_ref(),
        state.credentials.as_ref(),
        state.ssh_known_hosts_file.as_deref(),
    )
    .await?;
    let pipeline = Pipeline::load(&project.pipeline_path)?;
    let parameters = pipeline.resolve_parameters(&request.parameters)?;
    let plan = ExecutionPlan::from_pipeline(&pipeline, uuid::Uuid::new_v4(), project.id);
    let remote_requirements = remote_agent_requirements(&pipeline)?;
    let remote_requirements_json = remote_requirements
        .as_ref()
        .map(serde_json::to_string)
        .transpose()
        .map_err(|error| ApiError::BadRequest(format!("invalid remote requirements: {error}")))?;
    let remote_workspace = remote_requirements
        .as_ref()
        .map(|_| {
            pipeline
                .resolve_workspace(&repository_root)
                .map(|_| repository_root.clone())
        })
        .transpose()?;
    let reservation = match remote_requirements.as_ref() {
        Some(requirements) => Some(
            state
                .agents
                .reserve(requirements, plan.build_id, Utc::now())
                .await
                .map_err(|error| {
                    ApiError::BadRequest(format!("remote agent unavailable: {error}"))
                })?,
        ),
        None => None,
    };
    let build = match state.storage.create_build_with_parameters(
        &project,
        &plan,
        &pipeline,
        source.as_ref(),
        &parameters,
    ) {
        Ok(build) => build,
        Err(error) => {
            if let Some(reservation) = reservation.as_ref() {
                state.agents.release(reservation).await;
            }
            return Err(error.into());
        }
    };
    let remote_attempt = if let Some(reservation) = reservation.as_ref() {
        match state.storage.create_remote_attempt(
            plan.build_id,
            plan.project_id,
            reservation.agent_id,
            remote_requirements_json
                .as_deref()
                .expect("serialized remote requirements"),
            &plan,
            &pipeline,
            &parameters,
        ) {
            Ok(record) => Some(record),
            Err(error) => {
                state.agents.release(reservation).await;
                let _ = state.storage.apply_event(&BuildEvent::BuildCancelled {
                    build_id: plan.build_id,
                    timestamp: Utc::now(),
                });
                return Err(error.into());
            }
        }
    } else {
        None
    };
    let cancellation = CancellationToken::new();
    state
        .active_builds
        .lock()
        .await
        .insert(build.id, cancellation.clone());

    let (events, received_events) = mpsc::channel(512);
    spawn_event_projector(
        state.storage.clone(),
        state.events.clone(),
        pipeline.clone(),
        repository_root.clone(),
        reservation.is_none(),
        received_events,
    );

    if let Some(reservation) = reservation {
        events
            .send(BuildEvent::BuildQueued {
                build_id: plan.build_id,
                project_id: plan.project_id,
                timestamp: Utc::now(),
            })
            .await
            .map_err(|_| ApiError::BadRequest("remote build event channel closed".into()))?;
        let requirements =
            remote_requirements.expect("a reservation exists only for remote requirements");
        spawn_remote_build_task(
            state.clone(),
            reservation,
            requirements,
            plan,
            pipeline,
            remote_workspace.expect("remote workspace was resolved"),
            parameters,
            cancellation,
            events,
            remote_attempt
                .as_ref()
                .expect("remote attempt exists for a remote reservation")
                .attempt_id,
            None,
        );
    } else {
        let handle = state
            .scheduler
            .enqueue_with_priority_and_parameters(
                plan,
                pipeline,
                repository_root,
                parameters,
                request.priority,
                cancellation.clone(),
                events,
            )
            .await?;
        spawn_build_reaper(state.active_builds.clone(), handle);
    }
    let mut response_build = build;
    response_build.status = BuildStatus::Queued;
    Ok(QueueBuildResponse {
        build: response_build,
        status: BuildStatus::Queued,
    })
}

fn spawn_event_projector(
    storage: Storage,
    event_bus: broadcast::Sender<BuildEvent>,
    pipeline: Pipeline,
    workspace: PathBuf,
    collect_local_artifacts: bool,
    mut received_events: mpsc::Receiver<BuildEvent>,
) {
    tokio::spawn(async move {
        while let Some(event) = received_events.recv().await {
            let event = if collect_local_artifacts {
                finalize_artifacts(&storage, &pipeline, &workspace, event)
            } else {
                event
            };
            if let Err(error) = storage.apply_event(&event) {
                tracing::error!(?error, "could not project Rivet build event");
                continue;
            }
            let _ = event_bus.send(event);
        }
    });
}

fn spawn_remote_build_task(
    state: AppState,
    reservation: AgentReservation,
    requirements: AgentRequirements,
    plan: ExecutionPlan,
    pipeline: Pipeline,
    workspace: PathBuf,
    parameters: BTreeMap<String, String>,
    cancellation: CancellationToken,
    events: mpsc::Sender<BuildEvent>,
    attempt_id: Uuid,
    resume_details: Option<BuildDetails>,
) {
    let build_id = plan.build_id;
    tokio::spawn(async move {
        let result = run_remote_build_with_recovery(
            state.clone(),
            reservation,
            requirements,
            plan,
            pipeline,
            workspace,
            parameters,
            cancellation,
            events,
            attempt_id,
            resume_details,
        )
        .await;
        if let Err(error) = result {
            tracing::error!(
                ?error,
                build_id = %build_id,
                "remote Rivet build failed before completion"
            );
        }
        if let Err(error) = state.storage.delete_remote_attempt(build_id) {
            tracing::warn!(?error, build_id = %build_id, "could not delete remote attempt record");
        }
        state.active_builds.lock().await.remove(&build_id);
    });
}

fn remote_agent_requirements(pipeline: &Pipeline) -> Result<Option<AgentRequirements>, ApiError> {
    let mut requirements = None;
    for stage in &pipeline.stages {
        for step in &stage.steps {
            let Some(agent) = step.agent.as_ref() else {
                continue;
            };
            let candidate = AgentRequirements::from(agent);
            if requirements
                .as_ref()
                .is_some_and(|current| current != &candidate)
            {
                return Err(ApiError::BadRequest(
                    "all remote steps in one build must use identical agent requirements".into(),
                ));
            }
            requirements = Some(candidate);
        }
    }
    Ok(requirements)
}

#[derive(Debug, Error)]
enum RemoteBuildError {
    #[error("remote agent transport failed: {0}")]
    Agent(#[from] AgentRegistryError),
    #[error("remote recovery state could not be persisted: {0}")]
    Storage(#[from] StorageError),
    #[error("workspace archive failed: {0}")]
    WorkspaceArchive(String),
    #[error("remote build event channel closed")]
    EventChannelClosed,
    #[error("remote agent disconnected while build {0} was running")]
    AgentDisconnected(BuildId),
    #[error("remote agent rejected build {build_id}: {code}: {message}")]
    AgentRejected {
        build_id: BuildId,
        code: String,
        message: String,
    },
    #[error("invalid event from remote agent for build {build_id}: {reason}")]
    InvalidEvent { build_id: BuildId, reason: String },
    #[error("remote agent sent an unsupported legacy message for build {0}")]
    UnsupportedMessage(BuildId),
    #[error("remote operation cancelled")]
    Cancelled,
    #[error("remote artifact transfer failed: {0}")]
    Artifact(String),
    #[error("remote build {0} exhausted its durable recovery budget")]
    RecoveryBudgetExhausted(BuildId),
}

struct RemoteArtifactReceiver {
    transfer: WorkspaceTransfer,
    archive_path: PathBuf,
    extraction_root: PathBuf,
    file: tokio::fs::File,
    digest: Sha256,
    received_bytes: u64,
    expected_sequence: u32,
}

const MAX_REMOTE_RECOVERY_ATTEMPTS: usize = 1;

async fn run_remote_build_with_recovery(
    state: AppState,
    initial_reservation: AgentReservation,
    requirements: AgentRequirements,
    plan: ExecutionPlan,
    pipeline: Pipeline,
    workspace: PathBuf,
    parameters: BTreeMap<String, String>,
    cancellation: CancellationToken,
    events: mpsc::Sender<BuildEvent>,
    mut attempt_id: Uuid,
    resume_details: Option<BuildDetails>,
) -> Result<(), RemoteBuildError> {
    let mut reservation = initial_reservation;
    let mut suppress_build_started = resume_details
        .as_ref()
        .is_some_and(|details| details.build.status == BuildStatus::Running);
    let mut active_stages = resume_details
        .as_ref()
        .map(|details| {
            details
                .stages
                .iter()
                .filter(|stage| stage.stage.status == rivet_core::StageStatus::Running)
                .map(|stage| stage.stage.id)
                .collect()
        })
        .unwrap_or_default();
    let mut active_steps = resume_details
        .as_ref()
        .map(|details| {
            details
                .stages
                .iter()
                .flat_map(|stage| stage.steps.iter())
                .filter(|step| step.status == rivet_core::StepStatus::Running)
                .map(|step| step.id)
                .collect()
        })
        .unwrap_or_default();
    let mut build_started = suppress_build_started;
    for attempt in 0..=MAX_REMOTE_RECOVERY_ATTEMPTS {
        let (route_sender, received_messages) = mpsc::channel(1024);
        state.remote_messages.lock().await.insert(
            plan.build_id,
            RemoteBuildRoute {
                agent_id: reservation.agent_id,
                session_id: reservation.session_id,
                messages: route_sender,
            },
        );
        let result = run_remote_build(
            state.clone(),
            reservation.clone(),
            plan.clone(),
            pipeline.clone(),
            workspace.clone(),
            parameters.clone(),
            cancellation.clone(),
            events.clone(),
            received_messages,
            attempt_id,
            suppress_build_started,
            &mut active_stages,
            &mut active_steps,
            &mut build_started,
        )
        .await;
        state.remote_messages.lock().await.remove(&plan.build_id);
        if let Err(error) = cleanup_remote_artifact_root(plan.build_id) {
            tracing::warn!(?error, build_id = %plan.build_id, "could not clean remote artifact staging");
        }

        match result {
            Ok(()) => {
                state.agents.release(&reservation).await;
                return Ok(());
            }
            Err(error)
                if attempt < MAX_REMOTE_RECOVERY_ATTEMPTS
                    && !cancellation.is_cancelled()
                    && remote_error_is_recoverable(&error) =>
            {
                tracing::warn!(
                    build_id = %plan.build_id,
                    failed_agent = %reservation.agent_id,
                    attempt = attempt + 1,
                    error = %error,
                    "remote agent lost; trying one replacement agent"
                );
                state.agents.release(&reservation).await;
                let replacement = state
                    .agents
                    .reserve(&requirements, plan.build_id, Utc::now())
                    .await;
                reservation = match replacement {
                    Ok(reservation) => {
                        let replacement_attempt_id = Uuid::new_v4();
                        match state.storage.advance_remote_attempt(
                            plan.build_id,
                            reservation.agent_id,
                            replacement_attempt_id,
                            MAX_REMOTE_RECOVERY_ATTEMPTS,
                        )? {
                            Some(_) => {
                                attempt_id = replacement_attempt_id;
                                reservation
                            }
                            None => {
                                state.agents.release(&reservation).await;
                                finish_remote_failed(
                                    &plan,
                                    &events,
                                    &active_stages,
                                    &active_steps,
                                    build_started,
                                )
                                .await?;
                                return Err(RemoteBuildError::RecoveryBudgetExhausted(
                                    plan.build_id,
                                ));
                            }
                        }
                    }
                    Err(error) => {
                        finish_remote_failed(
                            &plan,
                            &events,
                            &active_stages,
                            &active_steps,
                            build_started,
                        )
                        .await?;
                        return Err(RemoteBuildError::Agent(error));
                    }
                };
                suppress_build_started = build_started;
            }
            Err(error) => {
                state.agents.release(&reservation).await;
                finish_remote_failed(&plan, &events, &active_stages, &active_steps, build_started)
                    .await?;
                return Err(error);
            }
        }
    }
    unreachable!("remote recovery loop always returns after its bounded attempts")
}

fn remote_error_is_recoverable(error: &RemoteBuildError) -> bool {
    match error {
        RemoteBuildError::AgentDisconnected(_) => true,
        RemoteBuildError::Agent(
            AgentRegistryError::AgentNotConnected(_)
            | AgentRegistryError::AgentChannelClosed(_)
            | AgentRegistryError::StaleReservation { .. },
        ) => true,
        RemoteBuildError::AgentRejected { code, .. } => code == "agent_disconnected",
        _ => false,
    }
}

async fn run_remote_build(
    state: AppState,
    reservation: AgentReservation,
    plan: ExecutionPlan,
    pipeline: Pipeline,
    workspace: PathBuf,
    parameters: BTreeMap<String, String>,
    cancellation: CancellationToken,
    events: mpsc::Sender<BuildEvent>,
    messages: mpsc::Receiver<AgentMessage>,
    attempt_id: Uuid,
    suppress_build_started: bool,
    active_stages: &mut HashSet<rivet_core::StageId>,
    active_steps: &mut HashSet<rivet_core::StepId>,
    build_started: &mut bool,
) -> Result<(), RemoteBuildError> {
    run_remote_build_inner(
        state,
        reservation,
        plan,
        pipeline,
        workspace,
        parameters,
        cancellation,
        events,
        messages,
        attempt_id,
        suppress_build_started,
        active_stages,
        active_steps,
        build_started,
    )
    .await
}

async fn run_remote_build_inner(
    state: AppState,
    reservation: AgentReservation,
    plan: ExecutionPlan,
    pipeline: Pipeline,
    workspace: PathBuf,
    parameters: BTreeMap<String, String>,
    cancellation: CancellationToken,
    events: mpsc::Sender<BuildEvent>,
    mut messages: mpsc::Receiver<AgentMessage>,
    attempt_id: Uuid,
    suppress_build_started: bool,
    active_stages: &mut HashSet<rivet_core::StageId>,
    active_steps: &mut HashSet<rivet_core::StepId>,
    build_started: &mut bool,
) -> Result<(), RemoteBuildError> {
    let mut accepted = false;
    let mut ready = false;
    let artifact_pipeline = pipeline.clone();
    let mut artifact_receiver = None;
    let mut artifacts_complete = artifact_pipeline.artifacts.is_empty();
    let archive_task = tokio::task::spawn_blocking(move || archive_workspace(&workspace));
    let archive = tokio::select! {
        result = archive_task => match result {
            Ok(Ok(archive)) => archive,
            Ok(Err(error)) => return Err(RemoteBuildError::WorkspaceArchive(error.to_string())),
            Err(error) => return Err(RemoteBuildError::WorkspaceArchive(format!("archive task failed: {error}"))),
        },
        _ = cancellation.cancelled() => {
            return finish_remote_cancelled(
                &state,
                &reservation,
                &plan,
                attempt_id,
                &events,
                &mut messages,
                &mut accepted,
                &mut ready,
                    active_stages,
                    active_steps,
            )
            .await;
        }
    };

    let assignment = AgentMessage::Assign {
        protocol_version: PROTOCOL_VERSION,
        build_id: plan.build_id,
        attempt_id,
        project_id: plan.project_id,
        plan: plan.clone(),
        pipeline: pipeline.clone(),
        parameters,
        workspace: archive.transfer,
    };
    match send_remote_message(&state, &reservation, assignment, &cancellation).await {
        Ok(()) => {}
        Err(RemoteBuildError::Cancelled) => {
            return finish_remote_cancelled(
                &state,
                &reservation,
                &plan,
                attempt_id,
                &events,
                &mut messages,
                &mut accepted,
                &mut ready,
                active_stages,
                active_steps,
            )
            .await;
        }
        Err(error) => return Err(error),
    }

    for (sequence, data) in archive.bytes.chunks(MAX_WORKSPACE_CHUNK_BYTES).enumerate() {
        let sequence = u32::try_from(sequence).map_err(|_| {
            RemoteBuildError::WorkspaceArchive(
                "workspace archive contains too many transfer chunks".into(),
            )
        })?;
        match send_remote_message(
            &state,
            &reservation,
            AgentMessage::WorkspaceChunk {
                protocol_version: PROTOCOL_VERSION,
                build_id: plan.build_id,
                sequence,
                data: data.to_vec(),
            },
            &cancellation,
        )
        .await
        {
            Ok(()) => {}
            Err(RemoteBuildError::Cancelled) => {
                return finish_remote_cancelled(
                    &state,
                    &reservation,
                    &plan,
                    attempt_id,
                    &events,
                    &mut messages,
                    &mut accepted,
                    &mut ready,
                    active_stages,
                    active_steps,
                )
                .await;
            }
            Err(error) => return Err(error),
        }
    }

    let mut health_check = tokio::time::interval(Duration::from_secs(5));
    health_check.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = cancellation.cancelled() => {
                return finish_remote_cancelled(
                    &state,
                    &reservation,
                    &plan,
                    attempt_id,
                    &events,
                    &mut messages,
                    &mut accepted,
                    &mut ready,
                    active_stages,
                    active_steps,
                )
                .await;
            }
            _ = health_check.tick() => {
                if !state.agents.reservation_online(&reservation, Utc::now()).await {
                    return Err(RemoteBuildError::AgentDisconnected(plan.build_id));
                }
            }
            message = messages.recv() => {
                let Some(message) = message else {
                    return Err(RemoteBuildError::AgentDisconnected(plan.build_id));
                };
                match message {
                    AgentMessage::AssignmentAccepted { build_id, .. } if build_id == plan.build_id => {
                        accepted = true;
                    }
                    AgentMessage::WorkspaceReady { build_id, .. } if build_id == plan.build_id => {
                        if !accepted {
                            return Err(RemoteBuildError::InvalidEvent {
                                build_id: plan.build_id,
                                reason: "workspace ready arrived before assignment acceptance".into(),
                            });
                        }
                        ready = true;
                    }
                    AgentMessage::ArtifactsReady { build_id, transfer, .. }
                        if build_id == plan.build_id =>
                    {
                        if !ready
                            || artifact_pipeline.artifacts.is_empty()
                            || artifact_receiver.is_some()
                            || artifacts_complete
                        {
                            return Err(RemoteBuildError::InvalidEvent {
                                build_id: plan.build_id,
                                reason: "artifact transfer arrived in an invalid state".into(),
                            });
                        }
                        let receiver = start_remote_artifact_receiver(build_id, transfer).await?;
                        if receiver.transfer.total_bytes == 0 {
                            complete_remote_artifacts(
                                &state,
                                receiver,
                                &artifact_pipeline,
                                build_id,
                            )
                            .await?;
                            artifacts_complete = true;
                        } else {
                            artifact_receiver = Some(receiver);
                        }
                    }
                    AgentMessage::ArtifactChunk {
                        build_id,
                        sequence,
                        data,
                        ..
                    } if build_id == plan.build_id => {
                        let Some(receiver) = artifact_receiver.as_mut() else {
                            return Err(RemoteBuildError::InvalidEvent {
                                build_id: plan.build_id,
                                reason: "artifact chunk arrived without an artifact transfer"
                                    .into(),
                            });
                        };
                        if sequence != receiver.expected_sequence {
                            return Err(RemoteBuildError::InvalidEvent {
                                build_id: plan.build_id,
                                reason: format!(
                                    "artifact chunk sequence {sequence} arrived; expected {}",
                                    receiver.expected_sequence
                                ),
                            });
                        }
                        receiver.received_bytes = receiver
                            .received_bytes
                            .checked_add(data.len() as u64)
                            .ok_or_else(|| {
                                RemoteBuildError::Artifact("artifact transfer size overflow".into())
                            })?;
                        if receiver.received_bytes > receiver.transfer.total_bytes {
                            return Err(RemoteBuildError::Artifact(
                                "artifact transfer exceeded its declared size".into(),
                            ));
                        }
                        receiver.digest.update(&data);
                        receiver
                            .file
                            .write_all(&data)
                            .await
                            .map_err(|error| RemoteBuildError::Artifact(error.to_string()))?;
                        receiver.expected_sequence = receiver
                            .expected_sequence
                            .checked_add(1)
                            .ok_or_else(|| {
                                RemoteBuildError::Artifact("artifact chunk sequence overflow".into())
                            })?;
                        if receiver.received_bytes == receiver.transfer.total_bytes {
                            let receiver = artifact_receiver
                                .take()
                                .expect("artifact receiver exists while completing transfer");
                            complete_remote_artifacts(
                                &state,
                                receiver,
                                &artifact_pipeline,
                                build_id,
                            )
                            .await?;
                            artifacts_complete = true;
                        }
                    }
                    AgentMessage::Event {
                        attempt_id: event_attempt_id,
                        sequence,
                        event,
                        ..
                    } => {
                        if event_attempt_id != attempt_id {
                            return Err(RemoteBuildError::InvalidEvent {
                                build_id: plan.build_id,
                                reason: "event belongs to a different remote attempt".into(),
                            });
                        }
                        validate_remote_event(&event, &plan, accepted, ready)?;
                        if matches!(event, BuildEvent::BuildStarted { .. }) {
                            *build_started = true;
                        }
                        let terminal = matches!(event, BuildEvent::BuildFinished { .. });
                        if matches!(
                            &event,
                            BuildEvent::BuildFinished {
                                status: BuildStatus::Passed,
                                ..
                            }
                        ) && !artifacts_complete
                        {
                            return Err(RemoteBuildError::InvalidEvent {
                                build_id: plan.build_id,
                                reason: "passed build arrived before artifact transfer completed"
                                    .into(),
                            });
                        }
                        let applied = state.storage.apply_remote_event(
                            plan.build_id,
                            attempt_id,
                            sequence,
                            &event,
                        )?;
                        if applied {
                            track_remote_activity(&event, active_stages, active_steps);
                            if suppress_build_started
                                && matches!(event, BuildEvent::BuildStarted { .. })
                            {
                                continue;
                            }
                            events.send(event).await.map_err(|_| RemoteBuildError::EventChannelClosed)?;
                        }
                        if terminal {
                            return Ok(());
                        }
                    }
                    AgentMessage::Finished { build_id, status, timestamp, .. } if build_id == plan.build_id => {
                        if !ready {
                            return Err(RemoteBuildError::InvalidEvent {
                                build_id: plan.build_id,
                                reason: "legacy finished message arrived before workspace readiness".into(),
                            });
                        }
                        events.send(BuildEvent::BuildFinished {
                            build_id,
                            status,
                            timestamp,
                        }).await.map_err(|_| RemoteBuildError::EventChannelClosed)?;
                        return Ok(());
                    }
                    AgentMessage::Error { build_id: Some(build_id), code, message, .. } if build_id == plan.build_id => {
                        return Err(RemoteBuildError::AgentRejected {
                            build_id,
                            code,
                            message,
                        });
                    }
                    AgentMessage::Log { build_id, .. } if build_id == plan.build_id => {
                        return Err(RemoteBuildError::UnsupportedMessage(build_id));
                    }
                    AgentMessage::AssignmentAccepted { build_id, .. }
                    | AgentMessage::WorkspaceReady { build_id, .. }
                    | AgentMessage::ArtifactsReady { build_id, .. }
                    | AgentMessage::ArtifactChunk { build_id, .. }
                    | AgentMessage::Finished { build_id, .. }
                    | AgentMessage::Error { build_id: Some(build_id), .. }
                    | AgentMessage::Log { build_id, .. } => {
                        return Err(RemoteBuildError::InvalidEvent {
                            build_id: plan.build_id,
                            reason: format!("message refers to unexpected build {build_id}"),
                        });
                    }
                    AgentMessage::Error { build_id: None, code, message, .. } => {
                        return Err(RemoteBuildError::AgentRejected {
                            build_id: plan.build_id,
                            code,
                            message,
                        });
                    }
                    _ => {
                        return Err(RemoteBuildError::InvalidEvent {
                            build_id: plan.build_id,
                            reason: "agent sent a server-originated or unrelated message".into(),
                        });
                    }
                }
            }
        }
    }
}

async fn start_remote_artifact_receiver(
    build_id: BuildId,
    transfer: WorkspaceTransfer,
) -> Result<RemoteArtifactReceiver, RemoteBuildError> {
    transfer
        .validate()
        .map_err(|error| RemoteBuildError::Artifact(error.to_string()))?;
    let staging_root = remote_artifact_root(build_id);
    if std::fs::symlink_metadata(&staging_root).is_ok() {
        return Err(RemoteBuildError::Artifact(format!(
            "artifact staging path already exists: {}",
            staging_root.display()
        )));
    }
    tokio::fs::create_dir_all(&staging_root)
        .await
        .map_err(|error| RemoteBuildError::Artifact(error.to_string()))?;
    let archive_path = staging_root.join("artifacts.tar");
    let file = tokio::fs::File::create(&archive_path)
        .await
        .map_err(|error| RemoteBuildError::Artifact(error.to_string()))?;
    Ok(RemoteArtifactReceiver {
        transfer,
        archive_path,
        extraction_root: staging_root.join("workspace"),
        file,
        digest: Sha256::new(),
        received_bytes: 0,
        expected_sequence: 0,
    })
}

async fn complete_remote_artifacts(
    state: &AppState,
    mut receiver: RemoteArtifactReceiver,
    pipeline: &Pipeline,
    build_id: BuildId,
) -> Result<(), RemoteBuildError> {
    receiver
        .file
        .flush()
        .await
        .map_err(|error| RemoteBuildError::Artifact(error.to_string()))?;
    let actual = hex::encode(receiver.digest.finalize());
    if actual != receiver.transfer.sha256 {
        return Err(RemoteBuildError::Artifact(format!(
            "artifact checksum mismatch: received {actual}, expected {}",
            receiver.transfer.sha256
        )));
    }
    let archive_path = receiver.archive_path;
    let extraction_root = receiver.extraction_root;
    let expected_entries = receiver.transfer.file_count;
    drop(receiver.file);

    let storage = state.storage.clone();
    let mut pipeline = pipeline.clone();
    pipeline.workspace = None;
    let task = tokio::task::spawn_blocking(move || {
        workspace_archive::extract_archive_file(&archive_path, &extraction_root, expected_entries)
            .map_err(|error| error.to_string())?;
        storage
            .collect_artifacts(build_id, &pipeline, &extraction_root)
            .map_err(|error| error.to_string())?;
        Ok::<(), String>(())
    });
    let result = match task.await {
        Ok(result) => result,
        Err(error) => Err(format!("artifact staging task failed: {error}")),
    };
    if let Err(error) = cleanup_remote_artifact_root(build_id) {
        tracing::warn!(?error, build_id = %build_id, "could not clean remote artifact staging");
    }
    result.map_err(RemoteBuildError::Artifact)
}

fn remote_artifact_root(build_id: BuildId) -> PathBuf {
    std::env::temp_dir().join(format!("rivet-remote-artifacts-{build_id}"))
}

fn cleanup_remote_artifact_root(build_id: BuildId) -> Result<(), String> {
    let path = remote_artifact_root(build_id);
    let Ok(metadata) = std::fs::symlink_metadata(&path) else {
        return Ok(());
    };
    if metadata.file_type().is_symlink() {
        return Err(format!(
            "refusing to remove symlink artifact staging path: {}",
            path.display()
        ));
    }
    if metadata.is_dir() {
        std::fs::remove_dir_all(&path).map_err(|error| error.to_string())?;
    } else {
        std::fs::remove_file(&path).map_err(|error| error.to_string())?;
    }
    Ok(())
}

async fn send_remote_message(
    state: &AppState,
    reservation: &AgentReservation,
    message: AgentMessage,
    cancellation: &CancellationToken,
) -> Result<(), RemoteBuildError> {
    tokio::select! {
        result = state.agents.send(reservation, message) => result.map_err(RemoteBuildError::Agent),
        _ = cancellation.cancelled() => Err(RemoteBuildError::Cancelled),
    }
}

async fn finish_remote_cancelled(
    state: &AppState,
    reservation: &AgentReservation,
    plan: &ExecutionPlan,
    attempt_id: Uuid,
    events: &mpsc::Sender<BuildEvent>,
    messages: &mut mpsc::Receiver<AgentMessage>,
    accepted: &mut bool,
    ready: &mut bool,
    active_stages: &mut HashSet<rivet_core::StageId>,
    active_steps: &mut HashSet<rivet_core::StepId>,
) -> Result<(), RemoteBuildError> {
    let _ = tokio::time::timeout(
        Duration::from_secs(2),
        state.agents.send(
            reservation,
            AgentMessage::Cancel {
                protocol_version: PROTOCOL_VERSION,
                build_id: reservation.build_id,
            },
        ),
    )
    .await;

    let grace = tokio::time::sleep(Duration::from_secs(5));
    tokio::pin!(grace);
    loop {
        tokio::select! {
            _ = &mut grace => break,
            message = messages.recv() => {
                let Some(message) = message else { break; };
                match message {
                    AgentMessage::AssignmentAccepted { build_id, .. } if build_id == plan.build_id => {
                        *accepted = true;
                    }
                    AgentMessage::WorkspaceReady { build_id, .. } if build_id == plan.build_id => {
                        *ready = *accepted;
                    }
                    AgentMessage::Event {
                        attempt_id: event_attempt_id,
                        sequence,
                        event,
                        ..
                    } => {
                        if event_attempt_id != attempt_id {
                            continue;
                        }
                        if validate_remote_event(&event, plan, *accepted, *ready).is_err() {
                            continue;
                        }
                        let terminal = matches!(event, BuildEvent::BuildFinished { .. });
                        if state.storage.apply_remote_event(
                            plan.build_id,
                            attempt_id,
                            sequence,
                            &event,
                        )? {
                            track_remote_activity(&event, active_stages, active_steps);
                            events.send(event).await.map_err(|_| RemoteBuildError::EventChannelClosed)?;
                        }
                        if terminal {
                            return Ok(());
                        }
                    }
                    AgentMessage::Finished { build_id, status, timestamp, .. } if build_id == plan.build_id => {
                        events.send(BuildEvent::BuildFinished {
                            build_id,
                            status,
                            timestamp,
                        }).await.map_err(|_| RemoteBuildError::EventChannelClosed)?;
                        return Ok(());
                    }
                    AgentMessage::Error { .. } => break,
                    _ => {}
                }
            }
        }
    }

    for stage in &plan.stages {
        for step in &stage.steps {
            if active_steps.remove(&step.id) {
                events
                    .send(BuildEvent::StepFinished {
                        build_id: plan.build_id,
                        stage_id: stage.id,
                        step_id: step.id,
                        step_name: step.definition.name.clone(),
                        status: rivet_core::StepStatus::Cancelled,
                        exit_code: None,
                        timestamp: Utc::now(),
                    })
                    .await
                    .map_err(|_| RemoteBuildError::EventChannelClosed)?;
            }
        }
        if active_stages.remove(&stage.id) {
            events
                .send(BuildEvent::StageFinished {
                    build_id: plan.build_id,
                    stage_id: stage.id,
                    stage_name: stage.name.clone(),
                    status: rivet_core::StageStatus::Cancelled,
                    timestamp: Utc::now(),
                })
                .await
                .map_err(|_| RemoteBuildError::EventChannelClosed)?;
        }
    }
    events
        .send(BuildEvent::BuildCancelled {
            build_id: plan.build_id,
            timestamp: Utc::now(),
        })
        .await
        .map_err(|_| RemoteBuildError::EventChannelClosed)?;
    events
        .send(BuildEvent::BuildFinished {
            build_id: plan.build_id,
            status: BuildStatus::Cancelled,
            timestamp: Utc::now(),
        })
        .await
        .map_err(|_| RemoteBuildError::EventChannelClosed)?;
    Ok(())
}

async fn finish_remote_failed(
    plan: &ExecutionPlan,
    events: &mpsc::Sender<BuildEvent>,
    active_stages: &HashSet<rivet_core::StageId>,
    active_steps: &HashSet<rivet_core::StepId>,
    build_started: bool,
) -> Result<(), RemoteBuildError> {
    if !build_started {
        events
            .send(BuildEvent::BuildStarted {
                build_id: plan.build_id,
                timestamp: Utc::now(),
            })
            .await
            .map_err(|_| RemoteBuildError::EventChannelClosed)?;
    }
    for stage in &plan.stages {
        for step in &stage.steps {
            if active_steps.contains(&step.id) {
                events
                    .send(BuildEvent::StepFinished {
                        build_id: plan.build_id,
                        stage_id: stage.id,
                        step_id: step.id,
                        step_name: step.definition.name.clone(),
                        status: rivet_core::StepStatus::Failed,
                        exit_code: None,
                        timestamp: Utc::now(),
                    })
                    .await
                    .map_err(|_| RemoteBuildError::EventChannelClosed)?;
            }
        }
        if active_stages.contains(&stage.id) {
            events
                .send(BuildEvent::StageFinished {
                    build_id: plan.build_id,
                    stage_id: stage.id,
                    stage_name: stage.name.clone(),
                    status: rivet_core::StageStatus::Failed,
                    timestamp: Utc::now(),
                })
                .await
                .map_err(|_| RemoteBuildError::EventChannelClosed)?;
        }
    }
    events
        .send(BuildEvent::BuildFinished {
            build_id: plan.build_id,
            status: BuildStatus::Failed,
            timestamp: Utc::now(),
        })
        .await
        .map_err(|_| RemoteBuildError::EventChannelClosed)?;
    Ok(())
}

fn track_remote_activity(
    event: &BuildEvent,
    active_stages: &mut HashSet<rivet_core::StageId>,
    active_steps: &mut HashSet<rivet_core::StepId>,
) {
    match event {
        BuildEvent::StageStarted { stage_id, .. } => {
            active_stages.insert(*stage_id);
        }
        BuildEvent::StageFinished { stage_id, .. } => {
            active_stages.remove(stage_id);
        }
        BuildEvent::StepStarted { step_id, .. } => {
            active_steps.insert(*step_id);
        }
        BuildEvent::StepFinished { step_id, .. } => {
            active_steps.remove(step_id);
        }
        _ => {}
    }
}

fn validate_remote_event(
    event: &BuildEvent,
    plan: &ExecutionPlan,
    accepted: bool,
    ready: bool,
) -> Result<(), RemoteBuildError> {
    let build_id = build_id_from_event(event);
    if build_id != plan.build_id {
        return Err(RemoteBuildError::InvalidEvent {
            build_id: plan.build_id,
            reason: format!("event refers to unexpected build {build_id}"),
        });
    }
    if !accepted {
        return Err(RemoteBuildError::InvalidEvent {
            build_id: plan.build_id,
            reason: "event arrived before assignment acceptance".into(),
        });
    }
    if matches!(event, BuildEvent::BuildQueued { .. }) {
        return Err(RemoteBuildError::InvalidEvent {
            build_id: plan.build_id,
            reason: "build queued is server-owned".into(),
        });
    }
    if !ready {
        return Err(RemoteBuildError::InvalidEvent {
            build_id: plan.build_id,
            reason: "event arrived before workspace readiness".into(),
        });
    }
    match event {
        BuildEvent::StageStarted { stage_id, .. } | BuildEvent::StageFinished { stage_id, .. } => {
            if !plan.stages.iter().any(|stage| stage.id == *stage_id) {
                return Err(RemoteBuildError::InvalidEvent {
                    build_id: plan.build_id,
                    reason: format!("unknown stage {stage_id}"),
                });
            }
        }
        BuildEvent::StepStarted {
            stage_id, step_id, ..
        }
        | BuildEvent::StepOutput {
            stage_id, step_id, ..
        }
        | BuildEvent::StepFinished {
            stage_id, step_id, ..
        } => {
            let valid_step = plan
                .stages
                .iter()
                .find(|stage| stage.id == *stage_id)
                .is_some_and(|stage| stage.steps.iter().any(|step| step.id == *step_id));
            if !valid_step {
                return Err(RemoteBuildError::InvalidEvent {
                    build_id: plan.build_id,
                    reason: format!("unknown step {step_id} in stage {stage_id}"),
                });
            }
        }
        BuildEvent::BuildStarted { .. }
        | BuildEvent::BuildFinished { .. }
        | BuildEvent::BuildCancelled { .. } => {}
        BuildEvent::BuildQueued { .. } => unreachable!("queued events are rejected above"),
    }
    Ok(())
}

fn finalize_artifacts(
    storage: &Storage,
    pipeline: &Pipeline,
    workspace: &Path,
    event: BuildEvent,
) -> BuildEvent {
    let BuildEvent::BuildFinished {
        build_id,
        status: BuildStatus::Passed,
        timestamp,
    } = &event
    else {
        return event;
    };
    if let Err(error) = storage.collect_artifacts(*build_id, pipeline, workspace) {
        tracing::error!(?error, %build_id, "artifact collection failed");
        return BuildEvent::BuildFinished {
            build_id: *build_id,
            status: BuildStatus::Failed,
            timestamp: *timestamp,
        };
    }
    event
}

async fn capture_source_snapshot(
    path: &Path,
    project_name: &str,
    prepare: Option<&PrepareScmRequest>,
    credentials: Option<&Arc<Mutex<CredentialVault>>>,
    ssh_known_hosts_file: Option<&Path>,
) -> Result<Option<SourceSnapshot>, ApiError> {
    match GitRepository::open(path).await {
        Ok(repository) => {
            let snapshot = match prepare {
                Some(request) => {
                    if request.credential_id.is_some() && !request.fetch {
                        return Err(ApiError::BadRequest(
                            "an SCM credential requires fetch=true".into(),
                        ));
                    }
                    if request.fetch_ref.is_some() && !request.fetch {
                        return Err(ApiError::BadRequest(
                            "an SCM fetch_ref requires fetch=true".into(),
                        ));
                    }
                    let credential = resolve_git_credential(
                        credentials,
                        request.credential_id.as_deref(),
                        project_name,
                    )
                    .await?;
                    repository
                        .prepare_with_auth(
                            &GitPrepareOptions {
                                remote: request.remote.clone(),
                                fetch: request.fetch,
                                revision: request.revision.clone(),
                                fetch_ref: request.fetch_ref.clone(),
                                clean: request.clean,
                                clean_ignored: request.clean_ignored,
                                submodules: request.submodules,
                                credential_id: request.credential_id.clone(),
                                known_hosts_file: ssh_known_hosts_file.map(Path::to_path_buf),
                            },
                            credential.as_ref(),
                        )
                        .await?
                }
                None => repository.inspect().await?,
            };
            Ok(Some(snapshot.source_snapshot()))
        }
        Err(ScmError::NotGitRepository(_)) if prepare.is_none() => Ok(None),
        Err(error) => {
            if prepare.is_some() {
                Err(error.into())
            } else {
                tracing::warn!(?error, "source snapshot unavailable");
                Ok(None)
            }
        }
    }
}

async fn resolve_git_credential(
    credentials: Option<&Arc<Mutex<CredentialVault>>>,
    credential_id: Option<&str>,
    project_name: &str,
) -> Result<Option<GitCredential>, ApiError> {
    let Some(credential_id) = credential_id else {
        return Ok(None);
    };
    if credential_id.trim().is_empty() {
        return Err(ApiError::BadRequest(
            "SCM credential ID cannot be empty".into(),
        ));
    }
    let vault = credentials.ok_or_else(|| {
        ApiError::BadRequest("this server has no configured SCM credential vault".into())
    })?;
    let vault = vault.lock().await;
    let credential = vault
        .get_for_project(credential_id, project_name)
        .map_err(|error| {
            ApiError::BadRequest(format!("could not resolve SCM credential: {error}"))
        })?;
    let resolved = match credential.kind() {
        CredentialKind::HttpBasic => GitCredential::HttpBasic(
            GitHttpCredential::new(credential.username(), credential.secret())
                .map_err(|error| ApiError::BadRequest(error.to_string()))?,
        ),
        CredentialKind::SshKey => GitCredential::SshKey(
            GitSshCredential::new(credential.username(), credential.secret())
                .map_err(|error| ApiError::BadRequest(error.to_string()))?,
        ),
    };
    Ok(Some(resolved))
}

async fn cancel_build(
    State(state): State<AppState>,
    AxumPath((name, number)): AxumPath<(String, i64)>,
    Extension(principal): Extension<Principal>,
) -> Result<StatusCode, ApiError> {
    require_project(&principal, Permission::Build, &name)?;
    let project = project_by_name(&state.storage, &name)?;
    let build = build_by_number(&state.storage, project.id, &name, number)?;
    let cancellation = state.active_builds.lock().await.get(&build.id).cloned();
    match cancellation {
        Some(cancellation) => {
            cancellation.cancel();
            Ok(StatusCode::ACCEPTED)
        }
        None if build.status.is_terminal() => Ok(StatusCode::CONFLICT),
        None => Err(ApiError::BadRequest(
            "build is not currently controllable by this server".into(),
        )),
    }
}

async fn build_events(
    State(state): State<AppState>,
    AxumPath((name, number)): AxumPath<(String, i64)>,
    Extension(principal): Extension<Principal>,
    websocket: WebSocketUpgrade,
) -> Result<impl IntoResponse, ApiError> {
    require_project(&principal, Permission::Read, &name)?;
    let project = project_by_name(&state.storage, &name)?;
    let build = build_by_number(&state.storage, project.id, &name, number)?;
    let receiver = state.events.subscribe();
    let replay = state.storage.events(build.id)?;
    Ok(websocket.on_upgrade(move |socket| stream_build_events(socket, receiver, build.id, replay)))
}

async fn stream_build_events(
    mut socket: WebSocket,
    mut receiver: broadcast::Receiver<BuildEvent>,
    build_id: BuildId,
    replay: Vec<BuildEvent>,
) {
    let mut replayed = HashSet::new();
    for event in replay {
        let Some(payload) = serialize_event(&event) else {
            return;
        };
        replayed.insert(payload.clone());
        if socket.send(Message::Text(payload.into())).await.is_err() {
            return;
        }
        if matches!(event, BuildEvent::BuildFinished { .. }) {
            return;
        }
    }

    while let Ok(event) = receiver.recv().await {
        if event_build_id(&event) != build_id {
            continue;
        }
        let Some(payload) = serialize_event(&event) else {
            break;
        };
        if replayed.remove(&payload) {
            continue;
        }
        if socket.send(Message::Text(payload.into())).await.is_err() {
            break;
        }
        if matches!(event, BuildEvent::BuildFinished { .. }) {
            break;
        }
    }
}

fn serialize_event(event: &BuildEvent) -> Option<String> {
    serde_json::to_string(event)
        .map_err(|error| {
            tracing::error!(?error, "could not serialize Rivet event");
        })
        .ok()
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

fn spawn_build_reaper(
    active: Arc<Mutex<HashMap<BuildId, CancellationToken>>>,
    handle: QueueHandle,
) {
    tokio::spawn(async move {
        let build_id = handle.build_id;
        if let Err(error) = handle.wait().await {
            tracing::error!(?error, "Rivet build worker failed");
        }
        active.lock().await.remove(&build_id);
    });
}

fn canonical_directory(path: &Path) -> Result<PathBuf, ApiError> {
    let canonical =
        std::fs::canonicalize(path).map_err(|_| ApiError::InvalidRepository(path.to_path_buf()))?;
    if !canonical.is_dir() {
        return Err(ApiError::InvalidRepository(canonical));
    }
    Ok(canonical)
}

fn project_by_name(storage: &Storage, name: &str) -> Result<Project, ApiError> {
    storage
        .get_project_by_name(name)?
        .ok_or_else(|| ApiError::ProjectNotFound(name.to_owned()))
}

fn build_by_number(
    storage: &Storage,
    project_id: uuid::Uuid,
    project: &str,
    number: i64,
) -> Result<BuildRecord, ApiError> {
    storage
        .list_builds(project_id)?
        .into_iter()
        .find(|build| build.number == number)
        .ok_or_else(|| ApiError::BuildNotFound {
            project: project.to_owned(),
            number,
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::{Body, to_bytes};
    use axum::http::Request;
    use chrono::TimeZone;
    use futures_util::{SinkExt, StreamExt};
    use rivet_agent_protocol::{
        AgentCapabilities, AgentHeartbeat, AgentMessage, AgentRegistration, AgentRequirements,
        AgentTransportMessage, PROTOCOL_VERSION,
    };
    use rivet_auth::{
        AUTH_USERS_VERSION, ApiTokenRecord, AuthPolicyDocument, AuthUserRecord, AuthUsers,
        AuthUsersDocument, Role, hash_password,
    };
    use rivet_extension_protocol::{
        ExtensionKind, ExtensionMessage, ExtensionPermission, encode_message,
    };
    use std::fs;
    use tempfile::tempdir;
    use tokio::time::sleep;
    use tokio_tungstenite::tungstenite::Message as ClientMessage;
    use tower::ServiceExt;

    #[test]
    fn artifact_download_verification_rejects_tampering_and_non_files() {
        let directory = tempdir().expect("tempdir");
        let path = directory.path().join("artifact.bin");
        let original = b"verified artifact\n";
        fs::write(&path, original).expect("artifact");
        let artifact = ArtifactRecord {
            id: Uuid::new_v4(),
            build_id: Uuid::new_v4(),
            name: "bundle".into(),
            relative_path: "dist/app.bin".into(),
            size_bytes: original.len() as u64,
            checksum: format!("sha256:{:x}", Sha256::digest(original)),
            created_at: Utc::now(),
        };

        let (mut verified, size) = verified_artifact_file(&path, &artifact).expect("verified");
        let mut bytes = Vec::new();
        verified
            .read_to_end(&mut bytes)
            .expect("read verified file");
        assert_eq!(size, original.len() as u64);
        assert_eq!(bytes, original);

        fs::write(&path, b"tampered").expect("tamper artifact");
        assert!(matches!(
            verified_artifact_file(&path, &artifact),
            Err(ApiError::ArtifactIntegrity)
        ));
        fs::remove_file(&path).expect("remove artifact");
        fs::create_dir(&path).expect("directory artifact");
        assert!(matches!(
            verified_artifact_file(&path, &artifact),
            Err(ApiError::ArtifactIntegrity)
        ));
    }

    #[tokio::test]
    async fn health_route_is_available_without_a_project() {
        let app = router(AppState::new(Storage::open_in_memory().expect("storage")));
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/health")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn admin_shutdown_route_requires_admin_and_requests_graceful_shutdown() {
        let state = AppState::with_token(
            Storage::open_in_memory().expect("storage"),
            "shutdown-secret",
        );
        let shutdown = state.shutdown.clone();

        let unauthorized = router(state.clone())
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/admin/shutdown")
                    .body(Body::empty())
                    .expect("unauthorized request"),
            )
            .await
            .expect("unauthorized response");
        assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);
        assert!(!shutdown.is_cancelled());

        let response = router(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/admin/shutdown")
                    .header(header::AUTHORIZATION, "Bearer shutdown-secret")
                    .body(Body::empty())
                    .expect("shutdown request"),
            )
            .await
            .expect("shutdown response");
        assert_eq!(response.status(), StatusCode::ACCEPTED);
        assert!(shutdown.is_cancelled());
        let body: serde_json::Value = serde_json::from_slice(
            &to_bytes(response.into_body(), 16 * 1024)
                .await
                .expect("shutdown body"),
        )
        .expect("shutdown JSON");
        assert_eq!(body["status"], "shutdown_requested");
    }

    #[tokio::test]
    async fn logs_route_returns_bounded_pages_with_a_next_cursor() {
        let pipeline = Pipeline::from_toml_str(
            "version = 1\nname = \"logs-page\"\n[[stages]]\nname = \"Test\"\n[[stages.steps]]\nname = \"unit\"\nprogram = \"true\"\n",
        )
        .expect("pipeline");
        let project = Project::new("logs-page", ".", "Rivetfile.toml").expect("project");
        let storage = Storage::open_in_memory().expect("storage");
        storage
            .create_project(&project, &pipeline)
            .expect("project");
        let plan = ExecutionPlan::from_pipeline(&pipeline, Uuid::new_v4(), project.id);
        let build = storage
            .create_build(&project, &plan, &pipeline, None)
            .expect("build");
        let stage = &plan.stages[0];
        let step = &stage.steps[0];
        for line in ["first", "second"] {
            storage
                .apply_event(&BuildEvent::StepOutput {
                    build_id: build.id,
                    stage_id: stage.id,
                    step_id: step.id,
                    stream: rivet_core::LogStream::Stdout,
                    line: line.into(),
                    timestamp: Utc::now(),
                })
                .expect("log");
        }

        let response = router(AppState::new(storage.clone()))
            .oneshot(
                Request::builder()
                    .uri("/api/v1/projects/logs-page/builds/1/logs?limit=1")
                    .body(Body::empty())
                    .expect("logs request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response
                .headers()
                .get("x-rivet-log-next-after")
                .and_then(|value| value.to_str().ok()),
            Some("1")
        );
        let body: serde_json::Value = serde_json::from_slice(
            &to_bytes(response.into_body(), 64 * 1024)
                .await
                .expect("body"),
        )
        .expect("JSON");
        assert_eq!(body[0]["line"], "first");

        let response = router(AppState::new(storage))
            .oneshot(
                Request::builder()
                    .uri("/api/v1/projects/logs-page/builds/1/logs?after=1&limit=10001")
                    .body(Body::empty())
                    .expect("invalid logs request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn metrics_route_is_authenticated_and_reports_bounded_gauges() {
        let pipeline = Pipeline::from_toml_str(
            "version = 1\nname = \"metrics\"\n[[stages]]\nname = \"Test\"\n[[stages.steps]]\nname = \"noop\"\nprogram = \"true\"\n",
        )
        .expect("pipeline");
        let project = Project::new("metrics", ".", "Rivetfile.toml").expect("project");
        let storage = Storage::open_in_memory().expect("storage");
        storage
            .create_project(&project, &pipeline)
            .expect("project");
        let raw_token = "metrics-secret";
        let mut state = AppState::new(storage);
        state.auth_digest = Some(Sha256::digest(raw_token.as_bytes()).into());

        let response = router(state.clone())
            .oneshot(
                Request::builder()
                    .uri("/api/v1/metrics")
                    .body(Body::empty())
                    .expect("unauthenticated request"),
            )
            .await
            .expect("unauthenticated response");
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

        let response = router(state)
            .oneshot(
                Request::builder()
                    .uri("/api/v1/metrics")
                    .header(header::AUTHORIZATION, format!("Bearer {raw_token}"))
                    .body(Body::empty())
                    .expect("metrics request"),
            )
            .await
            .expect("metrics response");
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers()[header::CONTENT_TYPE],
            "text/plain; version=0.0.4; charset=utf-8"
        );
        let body = String::from_utf8(
            to_bytes(response.into_body(), 16 * 1024)
                .await
                .expect("metrics body")
                .to_vec(),
        )
        .expect("metrics UTF-8");
        assert!(body.contains("rivet_projects_total 1\n"));
        assert!(body.contains("rivet_builds_active 0\n"));
        assert!(body.contains("rivet_queue_capacity 2\n"));
        assert!(body.contains("rivet_queue_paused 0\n"));
        assert!(!body.contains(raw_token));
    }

    #[tokio::test]
    async fn create_project_can_clone_a_remote_repository_to_an_explicit_destination() {
        let directory = tempdir().expect("tempdir");
        let source = directory.path().join("source");
        let destination = directory.path().join("checkout");
        fs::create_dir_all(&source).expect("source");
        fs::write(
            source.join("Rivetfile.toml"),
            "version = 1\nname = \"remote-project\"\n[[stages]]\nname = \"Test\"\n[[stages.steps]]\nname = \"noop\"\nprogram = \"true\"\n",
        )
        .expect("pipeline");

        let git = |args: &[&str]| {
            let output = std::process::Command::new("git")
                .args(args)
                .current_dir(&source)
                .output()
                .expect("git available");
            assert!(
                output.status.success(),
                "git {:?}: {}",
                args,
                String::from_utf8_lossy(&output.stderr)
            );
        };
        git(&["init", "-q"]);
        git(&["config", "user.email", "rivet@example.test"]);
        git(&["config", "user.name", "Rivet Tests"]);
        git(&["add", "Rivetfile.toml"]);
        git(&["commit", "-qm", "remote fixture"]);

        let storage = Storage::open_in_memory().expect("storage");
        let app = router(AppState::new(storage.clone()));
        let body = serde_json::to_vec(&serde_json::json!({
            "name": "remote-project",
            "repository_url": source,
            "clone_destination": destination,
        }))
        .expect("request body");
        let response = app
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/api/v1/projects")
                    .header("content-type", "application/json")
                    .body(Body::from(body))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::CREATED);
        let project: Project = serde_json::from_slice(
            &to_bytes(response.into_body(), 16 * 1024)
                .await
                .expect("response body"),
        )
        .expect("project JSON");
        assert_eq!(project.name, "remote-project");
        assert_eq!(
            PathBuf::from(&project.repository_path),
            fs::canonicalize(&destination).expect("canonical destination")
        );
        assert!(Path::new(&project.pipeline_path).is_file());
        assert!(
            storage
                .get_project_by_name("remote-project")
                .expect("project lookup")
                .is_some()
        );
    }

    #[tokio::test]
    async fn create_project_requires_an_explicit_destination_for_remote_clone() {
        let directory = tempdir().expect("tempdir");
        let source = directory.path().join("source");
        let destination = directory.path().join("checkout");
        fs::create_dir_all(&source).expect("source");

        let storage = Storage::open_in_memory().expect("storage");
        let app = router(AppState::new(storage));
        let body = serde_json::to_vec(&serde_json::json!({
            "name": "remote-project",
            "repository_url": source,
        }))
        .expect("request body");
        let response = app
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/api/v1/projects")
                    .header("content-type", "application/json")
                    .body(Body::from(body))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert!(!destination.exists());
    }

    #[tokio::test]
    async fn readiness_route_probes_storage_without_exposing_details() {
        let app = router(AppState::new(Storage::open_in_memory().expect("storage")));
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/ready")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);
        let payload: serde_json::Value = serde_json::from_slice(
            &to_bytes(response.into_body(), 8 * 1024)
                .await
                .expect("body"),
        )
        .expect("JSON");
        assert_eq!(payload["status"], "ready");
        assert_eq!(payload["service"], "rivet-server");
        assert_eq!(payload["storage"], "ok");
        assert!(payload.get("error").is_none());
    }

    #[tokio::test]
    async fn local_user_login_issues_a_session_and_protects_routes() {
        let storage = Storage::open_in_memory().expect("storage");
        let password_hash = hash_password("a-correct-local-password").expect("password hash");
        let mut state = AppState::new(storage);
        state.auth_users = Some(Arc::new(
            AuthUsers::from_document(AuthUsersDocument {
                version: AUTH_USERS_VERSION,
                users: vec![AuthUserRecord {
                    id: "operator-1".into(),
                    username: "operator@example.test".into(),
                    password_hash,
                    role: Role::Operator,
                    projects: vec!["demo".into()],
                    disabled: false,
                }],
            })
            .expect("users"),
        ));
        let app = router(state);
        let login_body = serde_json::to_vec(&serde_json::json!({
            "username": "OPERATOR@example.test",
            "password": "a-correct-local-password"
        }))
        .expect("login body");
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/api/v1/auth/login")
                    .header("content-type", "application/json")
                    .body(Body::from(login_body))
                    .expect("login request"),
            )
            .await
            .expect("login response");
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), 16 * 1024)
            .await
            .expect("login response body");
        let payload: serde_json::Value = serde_json::from_slice(&body).expect("login JSON");
        let session_token = payload["session_token"].as_str().expect("session token");
        assert!(!session_token.is_empty());
        assert!(
            !body
                .windows(b"a-correct-local-password".len())
                .any(|window| { window == b"a-correct-local-password" })
        );

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/v1/auth/me")
                    .header("authorization", format!("Bearer {session_token}"))
                    .body(Body::empty())
                    .expect("me request"),
            )
            .await
            .expect("me response");
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), 16 * 1024)
            .await
            .expect("me body");
        let payload: serde_json::Value = serde_json::from_slice(&body).expect("me JSON");
        assert_eq!(payload["id"], "operator-1");
        assert_eq!(payload["role"], "operator");

        let wrong_body = serde_json::to_vec(&serde_json::json!({
            "username": "operator@example.test",
            "password": "wrong-password"
        }))
        .expect("wrong login body");
        let response = app
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/api/v1/auth/login")
                    .header("content-type", "application/json")
                    .body(Body::from(wrong_body))
                    .expect("wrong login request"),
            )
            .await
            .expect("wrong login response");
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[test]
    fn keychain_service_requires_an_explicit_account() {
        let config = ServerConfig {
            bind: "127.0.0.1:7878".parse().expect("bind"),
            auth_token: None,
            auth_policy_file: None,
            auth_users_file: None,
            webhook_secret: None,
            github_webhook_secret: None,
            gitlab_webhook_secret: None,
            bitbucket_webhook_secret: None,
            github_webhook_credential_id: None,
            gitlab_webhook_credential_id: None,
            bitbucket_webhook_credential_id: None,
            credentials_file: None,
            credentials_passphrase: None,
            credentials_keychain_account: None,
            credentials_keychain_service: Some("rivet-production".into()),
            ssh_known_hosts_file: None,
            extension_manifest_dir: None,
            allowed_origins: Vec::new(),
        };

        assert!(matches!(
            validate_config(&config),
            Err(ServerError::IncompleteCredentialVaultConfig)
        ));
    }

    #[test]
    fn invalid_keychain_service_is_rejected_before_startup() {
        let config = ServerConfig {
            bind: "127.0.0.1:7878".parse().expect("bind"),
            auth_token: None,
            auth_policy_file: None,
            auth_users_file: None,
            webhook_secret: None,
            github_webhook_secret: None,
            gitlab_webhook_secret: None,
            bitbucket_webhook_secret: None,
            github_webhook_credential_id: None,
            gitlab_webhook_credential_id: None,
            bitbucket_webhook_credential_id: None,
            credentials_file: Some(PathBuf::from("credentials.vault")),
            credentials_passphrase: None,
            credentials_keychain_account: Some("rivet-production".into()),
            credentials_keychain_service: Some("rivet\0production".into()),
            ssh_known_hosts_file: None,
            extension_manifest_dir: None,
            allowed_origins: Vec::new(),
        };

        assert!(matches!(
            validate_config(&config),
            Err(ServerError::Credentials(
                CredentialError::InvalidKeychainLabel("service")
            ))
        ));
    }

    #[tokio::test]
    async fn request_id_is_propagated_or_safely_regenerated() {
        let app = router(AppState::new(Storage::open_in_memory().expect("storage")));
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/v1/health")
                    .header("x-request-id", "desktop-startup-01")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(
            response
                .headers()
                .get("x-request-id")
                .and_then(|value| value.to_str().ok()),
            Some("desktop-startup-01")
        );

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/health")
                    .header("x-request-id", "not a safe id")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        let generated = response
            .headers()
            .get("x-request-id")
            .and_then(|value| value.to_str().ok())
            .expect("generated request ID");
        assert_ne!(generated, "not a safe id");
        assert!(uuid::Uuid::parse_str(generated).is_ok());
    }

    #[tokio::test]
    async fn migration_route_reports_findings_and_a_valid_draft() {
        let app = router(AppState::new(Storage::open_in_memory().expect("storage")));
        let body = serde_json::to_vec(&serde_json::json!({
            "source": "pipeline {\n  agent any\n  stages {\n    stage('Test') {\n      steps {\n        sh 'cargo test'\n      }\n    }\n  }\n}\n",
            "draft": true,
        }))
        .expect("request body");
        let response = app
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/api/v1/migration/jenkinsfile")
                    .header("content-type", "application/json")
                    .body(Body::from(body))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), 64 * 1024)
            .await
            .expect("response body");
        let payload: serde_json::Value = serde_json::from_slice(&body).expect("JSON response");
        assert_eq!(payload["analysis"]["status"], "partial");
        assert_eq!(payload["analysis"]["constructs"][0]["line"], 1);
        assert_eq!(payload["draft"]["converted_steps"], 1);
        assert!(
            payload["draft"]["rivetfile_toml"]
                .as_str()
                .is_some_and(|draft| draft.contains("migrated-jenkinsfile"))
        );
    }

    #[tokio::test]
    async fn queue_route_reports_scheduler_capacity() {
        let app = router(AppState::new(Storage::open_in_memory().expect("storage")));
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/queue")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), 4096).await.expect("body");
        assert_eq!(
            &body[..],
            br#"{"queued":0,"running":0,"capacity":2,"paused":false}"#
        );
    }

    #[tokio::test]
    async fn pipeline_parameters_expose_definitions_without_secret_values() {
        let directory = tempdir().expect("tempdir");
        let pipeline_path = directory.path().join("Rivetfile.toml");
        fs::write(
            &pipeline_path,
            "version = 1\nname = \"parameters\"\n\n[[parameters]]\nname = \"TARGET\"\ndefault = \"release\"\n\n[[parameters]]\nname = \"CHANNEL\"\nkind = \"choice\"\nchoices = [\"staging\", \"production\"]\ndefault = \"staging\"\n\n[[parameters]]\nname = \"PUBLISH\"\nkind = \"boolean\"\ndefault = \"false\"\n\n[[parameters]]\nname = \"TOKEN\"\nkind = \"password\"\nsecret = true\n\n[[stages]]\nname = \"Test\"\n[[stages.steps]]\nname = \"noop\"\nprogram = \"true\"\n",
        )
        .expect("pipeline");
        let pipeline = Pipeline::load(&pipeline_path).expect("pipeline");
        let storage = Storage::open_in_memory().expect("storage");
        let project = Project::new(
            "parameters",
            directory.path().to_string_lossy().into_owned(),
            pipeline_path.to_string_lossy().into_owned(),
        )
        .expect("project");
        storage
            .create_project(&project, &pipeline)
            .expect("create project");

        let response = router(AppState::new(storage.clone()))
            .oneshot(
                Request::builder()
                    .uri("/api/v1/projects/parameters/parameters")
                    .body(Body::empty())
                    .expect("parameters request"),
            )
            .await
            .expect("parameters response");
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), 16 * 1024)
            .await
            .expect("parameters body");
        let payload: serde_json::Value = serde_json::from_slice(&body).expect("parameters JSON");
        assert_eq!(payload[0]["name"], "TARGET");
        assert_eq!(payload[0]["kind"], "string");
        assert_eq!(payload[0]["default"], "release");
        assert_eq!(payload[0]["required"], false);
        assert_eq!(payload[1]["name"], "CHANNEL");
        assert_eq!(payload[1]["kind"], "choice");
        assert_eq!(
            payload[1]["choices"],
            serde_json::json!(["staging", "production"])
        );
        assert_eq!(payload[2]["name"], "PUBLISH");
        assert_eq!(payload[2]["kind"], "boolean");
        assert_eq!(payload[2]["default"], "false");
        assert_eq!(payload[3]["name"], "TOKEN");
        assert_eq!(payload[3]["kind"], "password");
        assert_eq!(payload[3]["secret"], true);
        assert!(payload[3]["default"].is_null());

        let response = router(AppState::new(storage))
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/api/v1/projects/parameters/builds")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"parameters":{"PUBLISH":"not-a-boolean"}}"#))
                    .expect("invalid parameter request"),
            )
            .await
            .expect("invalid parameter response");
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn build_annotations_api_persists_and_reads_stage_scoped_metadata() {
        let directory = tempdir().expect("workspace");
        let pipeline = Pipeline::from_toml_str(
            r#"
version = 1
name = "annotation-api"
[[stages]]
name = "Test"
[[stages.steps]]
name = "unit"
program = "true"
"#,
        )
        .expect("pipeline");
        let project = Project::new(
            "annotation-api",
            directory.path().to_string_lossy().into_owned(),
            "Rivetfile.toml",
        )
        .expect("project");
        let storage = Storage::open_in_memory().expect("storage");
        storage
            .create_project(&project, &pipeline)
            .expect("project persistence");
        let plan = ExecutionPlan::from_pipeline(&pipeline, Uuid::new_v4(), project.id);
        let build = storage
            .create_build(&project, &plan, &pipeline, None)
            .expect("build persistence");
        let state = AppState::new(storage);
        let app = router(state);
        let uri = "/api/v1/projects/annotation-api/builds/1/annotations";
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri(uri)
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "kind": "warning",
                            "message": "lint issue",
                            "stage_id": plan.stages[0].id,
                        })
                        .to_string(),
                    ))
                    .expect("annotation request"),
            )
            .await
            .expect("annotation response");
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), 16 * 1024)
            .await
            .expect("annotation body");
        let created: AnnotationRecord = serde_json::from_slice(&body).expect("annotation JSON");
        assert_eq!(created.build_id, build.id);
        assert_eq!(created.stage_id, Some(plan.stages[0].id));

        let response = app
            .oneshot(
                Request::builder()
                    .uri(uri)
                    .body(Body::empty())
                    .expect("annotation list request"),
            )
            .await
            .expect("annotation list response");
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), 16 * 1024)
            .await
            .expect("annotation list body");
        let listed: Vec<AnnotationRecord> =
            serde_json::from_slice(&body).expect("annotation list JSON");
        assert_eq!(listed, vec![created]);
    }

    #[tokio::test]
    async fn queue_items_report_priority_order_without_build_parameters() {
        let directory = tempdir().expect("tempdir");
        let pipeline_path = directory.path().join("Rivetfile.toml");
        fs::write(
            &pipeline_path,
            "version = 1\nname = \"queue-items\"\n[[stages]]\nname = \"Test\"\n[[stages.steps]]\nname = \"noop\"\nprogram = \"true\"\n",
        )
        .expect("pipeline");
        let pipeline = Pipeline::load(&pipeline_path).expect("pipeline");
        let storage = Storage::open_in_memory().expect("storage");
        let project = Project::new(
            "queue-items",
            directory.path().to_string_lossy().into_owned(),
            pipeline_path.to_string_lossy().into_owned(),
        )
        .expect("project");
        storage
            .create_project(&project, &pipeline)
            .expect("create project");
        let state = AppState::new(storage);
        state.scheduler.pause();

        let response = router(state.clone())
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/api/v1/projects/queue-items/builds")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"priority":10}"#))
                    .expect("queue request"),
            )
            .await
            .expect("queue response");
        assert_eq!(response.status(), StatusCode::ACCEPTED);

        let response = router(state)
            .oneshot(
                Request::builder()
                    .uri("/api/v1/queue/items")
                    .body(Body::empty())
                    .expect("items request"),
            )
            .await
            .expect("items response");
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), 16 * 1024)
            .await
            .expect("items body");
        let payload: serde_json::Value = serde_json::from_slice(&body).expect("items JSON");
        assert_eq!(payload[0]["project"], "queue-items");
        assert_eq!(payload[0]["priority"], 10);
        assert_eq!(payload[0]["position"], 1);
        assert!(payload[0].get("parameters").is_none());
    }

    #[tokio::test]
    async fn queue_admin_controls_pause_and_resume_state() {
        let state = AppState::new(Storage::open_in_memory().expect("storage"));
        let response = router(state.clone())
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/api/v1/queue/pause")
                    .body(Body::empty())
                    .expect("pause request"),
            )
            .await
            .expect("pause response");
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), 4096)
            .await
            .expect("pause body");
        assert_eq!(
            &body[..],
            br#"{"queued":0,"running":0,"capacity":2,"paused":true}"#
        );

        let response = router(state.clone())
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/api/v1/queue/resume")
                    .body(Body::empty())
                    .expect("resume request"),
            )
            .await
            .expect("resume response");
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), 4096)
            .await
            .expect("resume body");
        assert_eq!(
            &body[..],
            br#"{"queued":0,"running":0,"capacity":2,"paused":false}"#
        );
    }

    #[tokio::test]
    async fn agents_route_starts_with_an_empty_ephemeral_registry() {
        let app = router(AppState::new(Storage::open_in_memory().expect("storage")));
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/agents")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), 4096).await.expect("body");
        assert_eq!(&body[..], b"[]");
    }

    #[tokio::test]
    async fn extensions_route_returns_only_validated_catalog_manifests() {
        let directory = tempdir().expect("catalog directory");
        let manifest = ExtensionManifest {
            protocol_version: rivet_extension_protocol::PROTOCOL_VERSION,
            id: "coverage.reporter".into(),
            name: "Coverage reporter".into(),
            version: "1.0.0".into(),
            kind: ExtensionKind::Wasm,
            entrypoint: "coverage.wasm".into(),
            permissions: vec![ExtensionPermission::ReadBuilds],
        };
        fs::write(
            directory.path().join("coverage.json"),
            serde_json::to_vec(&manifest).expect("manifest JSON"),
        )
        .expect("manifest");
        let catalog = ExtensionCatalog::from_directory(Some(directory.path())).expect("catalog");
        let mut state = AppState::new(Storage::open_in_memory().expect("storage"));
        state.extensions = Arc::new(catalog);

        let response = router(state)
            .oneshot(
                Request::builder()
                    .uri("/api/v1/extensions")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), 16 * 1024)
            .await
            .expect("body");
        let manifests: Vec<ExtensionManifest> =
            serde_json::from_slice(&body).expect("manifests JSON");
        assert_eq!(manifests, vec![manifest]);
    }

    #[tokio::test]
    async fn extension_status_reports_runtime_capability_without_launching_code() {
        let directory = tempdir().expect("catalog directory");
        let manifest = ExtensionManifest {
            protocol_version: rivet_extension_protocol::PROTOCOL_VERSION,
            id: "coverage.reporter".into(),
            name: "Coverage reporter".into(),
            version: "1.0.0".into(),
            kind: ExtensionKind::Wasm,
            entrypoint: "coverage.wasm".into(),
            permissions: vec![ExtensionPermission::ReadBuilds],
        };
        fs::write(
            directory.path().join("coverage.json"),
            serde_json::to_vec(&manifest).expect("manifest JSON"),
        )
        .expect("manifest");
        let catalog = ExtensionCatalog::from_directory(Some(directory.path())).expect("catalog");
        let manager = ExtensionManager::new(directory.path(), &catalog).expect("manager");
        let mut state = AppState::new(Storage::open_in_memory().expect("storage"));
        state.extensions = Arc::new(catalog);
        state.extension_manager = Some(Arc::new(manager));

        let response = router(state)
            .oneshot(
                Request::builder()
                    .uri("/api/v1/extensions/status")
                    .body(Body::empty())
                    .expect("status request"),
            )
            .await
            .expect("status response");
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), 16 * 1024)
            .await
            .expect("status body");
        assert_eq!(
            &body[..],
            br#"[{"id":"coverage.reporter","active":false,"runtime_available":true}]"#
        );
    }

    #[tokio::test]
    async fn wasm_extension_routes_start_through_the_bounded_runtime() {
        let directory = tempdir().expect("catalog directory");
        let manifest = ExtensionManifest {
            protocol_version: rivet_extension_protocol::PROTOCOL_VERSION,
            id: "coverage.reporter".into(),
            name: "Coverage reporter".into(),
            version: "1.0.0".into(),
            kind: ExtensionKind::Wasm,
            entrypoint: "coverage.wasm".into(),
            permissions: vec![
                ExtensionPermission::ReadBuilds,
                ExtensionPermission::WriteAnnotations,
                ExtensionPermission::TriggerBuilds,
            ],
        };
        fs::write(
            directory.path().join("coverage.json"),
            serde_json::to_vec(&manifest).expect("manifest JSON"),
        )
        .expect("manifest");
        let response = br#"{"abi_version":1,"result":{"ok":true}}"#;
        let response_data = response
            .iter()
            .map(|byte| format!("\\{byte:02x}"))
            .collect::<String>();
        let packed = (u64::try_from(response.len()).expect("response length") << 32) | 1024;
        let module = wat::parse_str(format!(
            r#"(module
                (memory (export "memory") 1 1)
                (data (i32.const 1024) "{response_data}")
                (func (export "rivet_alloc") (param i32) (result i32) i32.const 0)
                (func (export "rivet_handle") (param i32 i32) (result i64) i64.const {packed}))"#
        ))
        .expect("WASM module");
        fs::write(directory.path().join("coverage.wasm"), module).expect("WASM module file");
        let catalog = ExtensionCatalog::from_directory(Some(directory.path())).expect("catalog");
        let manager = ExtensionManager::new(directory.path(), &catalog).expect("manager");
        let mut state = AppState::new(Storage::open_in_memory().expect("storage"));
        let host_pipeline = Pipeline::from_toml_str(
            r#"
version = 1
name = "extension-host-route"
[[stages]]
name = "Test"
[[stages.steps]]
name = "unit"
program = "true"
"#,
        )
        .expect("host pipeline");
        let host_pipeline_path = directory.path().join("Rivetfile.toml");
        fs::write(
            &host_pipeline_path,
            r#"
version = 1
name = "extension-host-route"
[[stages]]
name = "Test"
[[stages.steps]]
name = "unit"
program = "true"
"#,
        )
        .expect("host pipeline file");
        let host_project = Project::new(
            "extension-host-route",
            directory.path().to_string_lossy().into_owned(),
            host_pipeline_path.to_string_lossy().into_owned(),
        )
        .expect("host project");
        state
            .storage
            .create_project(&host_project, &host_pipeline)
            .expect("host project persistence");
        let host_plan =
            ExecutionPlan::from_pipeline(&host_pipeline, Uuid::new_v4(), host_project.id);
        let host_build = state
            .storage
            .create_build(&host_project, &host_plan, &host_pipeline, None)
            .expect("host build persistence");
        state.extensions = Arc::new(catalog);
        state.extension_manager = Some(Arc::new(manager));

        let response = router(state.clone())
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/api/v1/extensions/coverage.reporter/start")
                    .body(Body::empty())
                    .expect("start request"),
            )
            .await
            .expect("start response");
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), 16 * 1024)
            .await
            .expect("start body");
        assert_eq!(
            &body[..],
            br#"{"id":"coverage.reporter","active":true,"runtime_available":true}"#
        );

        let response = router(state.clone())
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/api/v1/extensions/coverage.reporter/request")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        format!(
                            r#"{{"permission":"read_builds","method":"builds.list","payload":{{"project_id":"{}"}}}}"#,
                            host_project.id
                        ),
                    ))
                    .expect("request"),
            )
            .await
            .expect("extension request response");
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), 16 * 1024)
            .await
            .expect("extension request body");
        assert_eq!(&body[..], br#"{"ok":true}"#);

        let response = router(state.clone())
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/api/v1/extensions/coverage.reporter/request")
                    .header("content-type", "application/json")
                    .body(Body::from(format!(
                        r#"{{"permission":"write_annotations","method":"build.annotate","payload":{{"build_id":"{}","kind":"quality","message":"coverage note"}}}}"#,
                        host_build.id
                    )))
                    .expect("annotation request"),
            )
            .await
            .expect("annotation response");
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            state
                .storage
                .annotations(host_build.id)
                .expect("host annotations")
                .len(),
            1
        );

        let response = router(state.clone())
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/api/v1/extensions/coverage.reporter/request")
                    .header("content-type", "application/json")
                    .body(Body::from(format!(
                        r#"{{"permission":"trigger_builds","method":"build.trigger","payload":{{"project_id":"{}","priority":10}}}}"#,
                        host_project.id
                    )))
                    .expect("trigger request"),
            )
            .await
            .expect("trigger response");
        assert_eq!(response.status(), StatusCode::OK);
        let builds = state
            .storage
            .list_builds(host_project.id)
            .expect("triggered builds");
        assert_eq!(builds.len(), 2);
        assert!(matches!(
            builds[1].status,
            BuildStatus::Pending | BuildStatus::Queued | BuildStatus::Running | BuildStatus::Passed
        ));

        let response = router(state)
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/api/v1/extensions/coverage.reporter/request")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"permission":"read_logs","method":"build.logs","payload":{}}"#,
                    ))
                    .expect("denied request"),
            )
            .await
            .expect("denied extension request response");
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn extension_host_methods_return_bounded_real_data_and_enforce_permissions() {
        let directory = tempdir().expect("workspace");
        let pipeline = Pipeline::from_toml_str(
            r#"
version = 1
name = "extension-host"
[[stages]]
name = "Test"
[[stages.steps]]
name = "unit"
program = "true"
"#,
        )
        .expect("pipeline");
        let pipeline_path = directory.path().join("Rivetfile.toml");
        fs::write(
            &pipeline_path,
            r#"
version = 1
name = "extension-host"
[[stages]]
name = "Test"
[[stages.steps]]
name = "unit"
program = "true"
"#,
        )
        .expect("pipeline file");
        let project = Project::new(
            "extension-host",
            directory.path().to_string_lossy().into_owned(),
            pipeline_path.to_string_lossy().into_owned(),
        )
        .expect("project");
        let storage = Storage::open_in_memory().expect("storage");
        storage
            .create_project(&project, &pipeline)
            .expect("persist project");
        let plan = ExecutionPlan::from_pipeline(&pipeline, Uuid::new_v4(), project.id);
        let build = storage
            .create_build(&project, &plan, &pipeline, None)
            .expect("persist build");
        let now = Utc::now();
        storage
            .apply_event(&BuildEvent::BuildQueued {
                build_id: build.id,
                project_id: project.id,
                timestamp: now,
            })
            .expect("queued event");
        storage
            .apply_event(&BuildEvent::BuildStarted {
                build_id: build.id,
                timestamp: now,
            })
            .expect("started event");
        storage
            .apply_event(&BuildEvent::StepOutput {
                build_id: build.id,
                stage_id: plan.stages[0].id,
                step_id: plan.stages[0].steps[0].id,
                stream: rivet_core::LogStream::Stdout,
                line: "safe output".into(),
                timestamp: now,
            })
            .expect("output event");
        let stage_id = plan.stages[0].id;
        let existing_annotation = storage
            .add_annotation(build.id, Some(stage_id), "warning", "lint issue")
            .expect("annotation");

        let state = AppState::new(storage);
        let build_list = extension_host_payload(
            &state,
            ExtensionPermission::ReadBuilds,
            "builds.list",
            json!({ "project_id": project.id }),
        )
        .expect("build list host method");
        let build_id = build.id.to_string();
        assert_eq!(
            build_list["data"]["builds"][0]["id"].as_str(),
            Some(build_id.as_str())
        );
        assert_eq!(build_list["host_protocol_version"], 1);

        let logs = extension_host_payload(
            &state,
            ExtensionPermission::ReadLogs,
            "build.logs",
            json!({ "build_id": build.id, "after_sequence": -1 }),
        )
        .expect("build logs host method");
        assert_eq!(logs["data"]["logs"][0]["line"], "safe output");

        let annotations = extension_host_payload(
            &state,
            ExtensionPermission::ReadBuilds,
            "build.annotations",
            json!({ "build_id": build.id }),
        )
        .expect("build annotations host method");
        assert_eq!(
            annotations["data"]["annotations"][0]["id"],
            existing_annotation.id.to_string()
        );

        let details = extension_host_payload(
            &state,
            ExtensionPermission::ReadBuilds,
            "build.details",
            json!({ "build_id": build.id }),
        )
        .expect("build details host method");
        assert_eq!(
            details["data"]["build"]["id"].as_str(),
            Some(build_id.as_str())
        );

        let created = extension_host_payload(
            &state,
            ExtensionPermission::WriteAnnotations,
            "build.annotate",
            json!({
                "build_id": build.id,
                "stage_id": stage_id,
                "kind": "quality",
                "message": "coverage is below target"
            }),
        )
        .expect("build annotate host method");
        assert_eq!(created["data"]["annotation"]["kind"], "quality");
        assert_eq!(
            state
                .storage
                .annotations(build.id)
                .expect("annotations")
                .len(),
            2
        );

        let triggered = extension_trigger_payload(
            &state,
            ExtensionPermission::TriggerBuilds,
            "build.trigger",
            json!({
                "project_id": project.id,
                "priority": 10,
                "parameters": {}
            }),
        )
        .await
        .expect("trigger host method");
        assert_eq!(triggered["data"]["status"], "queued");
        assert_eq!(
            state
                .storage
                .list_builds(project.id)
                .expect("triggered builds")
                .len(),
            2
        );

        let denied = extension_host_payload(
            &state,
            ExtensionPermission::ReadBuilds,
            "build.logs",
            json!({ "build_id": build.id }),
        )
        .expect_err("wrong permission must be rejected");
        assert!(matches!(denied, ApiError::BadRequest(message) if message.contains("ReadLogs")));

        let denied_annotation = extension_host_payload(
            &state,
            ExtensionPermission::ReadBuilds,
            "build.annotate",
            json!({
                "build_id": build.id,
                "kind": "quality",
                "message": "must not write"
            }),
        )
        .expect_err("wrong annotation permission must be rejected");
        assert!(
            matches!(denied_annotation, ApiError::BadRequest(message) if message.contains("WriteAnnotations"))
        );

        let custom = extension_host_payload(
            &state,
            ExtensionPermission::ReadBuilds,
            "custom.extension.method",
            json!({ "value": 7 }),
        )
        .expect("custom extension methods remain untouched");
        assert_eq!(custom, json!({ "value": 7 }));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn subprocess_extension_routes_start_and_stop_a_validated_process() {
        let directory = tempdir().expect("catalog directory");
        let manifest = ExtensionManifest {
            protocol_version: rivet_extension_protocol::PROTOCOL_VERSION,
            id: "coverage.reporter".into(),
            name: "Coverage reporter".into(),
            version: "1.0.0".into(),
            kind: ExtensionKind::Subprocess,
            entrypoint: "runner".into(),
            permissions: vec![ExtensionPermission::ReadBuilds],
        };
        fs::write(
            directory.path().join("coverage.json"),
            serde_json::to_vec(&manifest).expect("manifest JSON"),
        )
        .expect("manifest");
        let ready = encode_message(&ExtensionMessage::Ready {
            protocol_version: rivet_extension_protocol::PROTOCOL_VERSION,
            extension_id: manifest.id.clone(),
        })
        .expect("ready frame");
        let escaped = ready
            .iter()
            .map(|byte| format!("\\{byte:03o}"))
            .collect::<String>();
        let runner = directory.path().join("runner");
        fs::write(
            &runner,
            format!("#!/bin/sh\nprintf '{escaped}'\nsleep 60\n"),
        )
        .expect("runner");
        use std::os::unix::fs::PermissionsExt;
        let mut permissions = fs::metadata(&runner)
            .expect("runner metadata")
            .permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(&runner, permissions).expect("runner permissions");

        let catalog = ExtensionCatalog::from_directory(Some(directory.path())).expect("catalog");
        let manager = ExtensionManager::new(directory.path(), &catalog).expect("manager");
        let mut state = AppState::new(Storage::open_in_memory().expect("storage"));
        state.extensions = Arc::new(catalog);
        state.extension_manager = Some(Arc::new(manager));

        let response = router(state.clone())
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/api/v1/extensions/coverage.reporter/start")
                    .body(Body::empty())
                    .expect("start request"),
            )
            .await
            .expect("start response");
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), 16 * 1024)
            .await
            .expect("start body");
        assert_eq!(
            &body[..],
            br#"{"id":"coverage.reporter","active":true,"runtime_available":true}"#
        );

        let response = router(state)
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/api/v1/extensions/coverage.reporter/stop")
                    .body(Body::empty())
                    .expect("stop request"),
            )
            .await
            .expect("stop response");
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), 16 * 1024)
            .await
            .expect("stop body");
        assert_eq!(
            &body[..],
            br#"{"id":"coverage.reporter","active":false,"runtime_available":true}"#
        );
    }

    #[tokio::test]
    async fn agent_match_route_is_capacity_aware() {
        let state = AppState::new(Storage::open_in_memory().expect("storage"));
        let agent_id = uuid::Uuid::new_v4();
        state
            .agents
            .register(
                AgentRegistration {
                    protocol_version: PROTOCOL_VERSION,
                    agent_id,
                    name: "linux-builder".into(),
                    capabilities: AgentCapabilities {
                        os: "linux".into(),
                        arch: "x86_64".into(),
                        docker: true,
                        labels: vec!["build".into()],
                        executors: 2,
                        cpu_cores: Some(8),
                        memory_mb: Some(16 * 1024),
                    },
                },
                Utc::now(),
            )
            .await
            .expect("register agent");
        let body = serde_json::to_vec(&AgentRequirements {
            os: Some("linux".into()),
            docker: true,
            labels: vec!["build".into()],
            executors: Some(1),
            cpu_cores: Some(4),
            memory_mb: Some(8192),
            ..AgentRequirements::default()
        })
        .expect("request body");
        let response = router(state.clone())
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/api/v1/agents/match")
                    .header("content-type", "application/json")
                    .body(Body::from(body))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), 16 * 1024)
            .await
            .expect("body");
        let matches: serde_json::Value = serde_json::from_slice(&body).expect("JSON");
        assert_eq!(matches[0]["agent_id"], agent_id.to_string());
        assert_eq!(matches[0]["available_executors"], 2);
        assert_eq!(matches[0]["available_cpu_cores"].as_u64(), Some(8));
        assert_eq!(matches[0]["available_memory_mb"].as_u64(), Some(16 * 1024));

        let invalid = router(state)
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/api/v1/agents/match")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"executors":0}"#))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(invalid.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn api_uses_exact_origins_and_security_headers() {
        let app = router(AppState::new(Storage::open_in_memory().expect("storage")));
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/health")
                    .header("origin", "http://127.0.0.1:1420")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response
                .headers()
                .get("access-control-allow-origin")
                .and_then(|value| value.to_str().ok()),
            Some("http://127.0.0.1:1420")
        );
        assert_eq!(
            response
                .headers()
                .get("cache-control")
                .and_then(|value| value.to_str().ok()),
            Some("no-store")
        );
        assert_eq!(
            response
                .headers()
                .get("x-content-type-options")
                .and_then(|value| value.to_str().ok()),
            Some("nosniff")
        );
        assert_eq!(
            response
                .headers()
                .get("x-frame-options")
                .and_then(|value| value.to_str().ok()),
            Some("DENY")
        );

        let response = router(AppState::new(Storage::open_in_memory().expect("storage")))
            .oneshot(
                Request::builder()
                    .uri("/api/v1/health")
                    .header("origin", "http://tauri.localhost")
                    .body(Body::empty())
                    .expect("Tauri webview request"),
            )
            .await
            .expect("Tauri webview response");
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response
                .headers()
                .get("access-control-allow-origin")
                .and_then(|value| value.to_str().ok()),
            Some("http://tauri.localhost")
        );

        let response = router(AppState::new(Storage::open_in_memory().expect("storage")))
            .oneshot(
                Request::builder()
                    .uri("/api/v1/health")
                    .header("origin", "http://127.0.0.1:1421")
                    .body(Body::empty())
                    .expect("Vite fallback request"),
            )
            .await
            .expect("Vite fallback response");
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response
                .headers()
                .get("access-control-allow-origin")
                .and_then(|value| value.to_str().ok()),
            Some("http://127.0.0.1:1421")
        );

        let response = router(AppState::new(Storage::open_in_memory().expect("storage")))
            .oneshot(
                Request::builder()
                    .method(Method::OPTIONS)
                    .uri("/api/v1/health")
                    .header("origin", "http://127.0.0.1:1420")
                    .header("access-control-request-method", "GET")
                    .header("access-control-request-headers", "x-request-id")
                    .body(Body::empty())
                    .expect("preflight request"),
            )
            .await
            .expect("preflight response");
        assert_eq!(response.status(), StatusCode::OK);
        assert!(
            response
                .headers()
                .get("access-control-allow-headers")
                .and_then(|value| value.to_str().ok())
                .is_some_and(|value| value.to_ascii_lowercase().contains("x-request-id"))
        );

        let mut custom_origins = vec!["https://console.example".to_owned()];
        let response = router_with_origins(
            AppState::new(Storage::open_in_memory().expect("storage")),
            &custom_origins,
        )
        .expect("custom origins")
        .oneshot(
            Request::builder()
                .uri("/api/v1/health")
                .header("origin", "http://127.0.0.1:1420")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
        assert!(
            response
                .headers()
                .get("access-control-allow-origin")
                .is_none()
        );
        custom_origins[0] = "*".to_owned();
        assert!(matches!(
            cors_layer(&custom_origins),
            Err(ServerError::InvalidAllowedOrigin(_))
        ));
    }

    #[tokio::test]
    async fn agent_websocket_registers_and_acknowledges_heartbeats() {
        let state = AppState::new(Storage::open_in_memory().expect("storage"));
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
        let address = listener.local_addr().expect("address");
        let server = tokio::spawn(async move {
            axum::serve(listener, router(state.clone()))
                .await
                .expect("agent server");
        });
        let (mut socket, _) =
            tokio_tungstenite::connect_async(format!("ws://{address}/api/v1/agents/connect"))
                .await
                .expect("connect");
        let agent_id = uuid::Uuid::new_v4();
        let registration = AgentMessage::Register(AgentRegistration {
            protocol_version: PROTOCOL_VERSION,
            agent_id,
            name: "test-agent".into(),
            capabilities: AgentCapabilities {
                os: "macos".into(),
                arch: "aarch64".into(),
                docker: false,
                labels: vec!["local".into()],
                executors: 1,
                cpu_cores: Some(4),
                memory_mb: Some(8 * 1024),
            },
        });
        socket
            .send(ClientMessage::Text(
                serde_json::to_string(&AgentTransportMessage::message(registration))
                    .expect("registration JSON")
                    .into(),
            ))
            .await
            .expect("send registration");
        let registered = socket
            .next()
            .await
            .expect("registered message")
            .expect("registered frame");
        let ClientMessage::Text(registered) = registered else {
            panic!("expected registered text frame");
        };
        let AgentTransportMessage::Message {
            delivery_id: registered_delivery_id,
            payload: AgentMessage::Registered { session_id, .. },
            ..
        } = serde_json::from_str(registered.as_ref()).expect("registered JSON")
        else {
            panic!("expected registered response");
        };
        socket
            .send(ClientMessage::Text(
                serde_json::to_string(&AgentTransportMessage::ack(registered_delivery_id))
                    .expect("registered ACK JSON")
                    .into(),
            ))
            .await
            .expect("ack registered response");

        let heartbeat = AgentTransportMessage::message(AgentMessage::Heartbeat(AgentHeartbeat {
            protocol_version: PROTOCOL_VERSION,
            agent_id,
            session_id,
            sequence: 1,
            running: vec![],
            sent_at: Utc::now(),
        }));
        let heartbeat_delivery_id = heartbeat.delivery_id();
        socket
            .send(ClientMessage::Text(
                serde_json::to_string(&heartbeat)
                    .expect("heartbeat JSON")
                    .into(),
            ))
            .await
            .expect("send heartbeat");
        let heartbeat_ack_delivery_id = loop {
            let acknowledged = socket
                .next()
                .await
                .expect("heartbeat ack")
                .expect("heartbeat frame");
            let ClientMessage::Text(acknowledged) = acknowledged else {
                panic!("expected heartbeat text frame");
            };
            match serde_json::from_str(acknowledged.as_ref()).expect("ack JSON") {
                AgentTransportMessage::Ack { .. } => continue,
                AgentTransportMessage::Message {
                    delivery_id,
                    payload: AgentMessage::HeartbeatAck { sequence: 1, .. },
                    ..
                } => break delivery_id,
                _ => panic!("expected heartbeat ack response"),
            }
        };
        socket
            .send(ClientMessage::Text(
                serde_json::to_string(&AgentTransportMessage::ack(heartbeat_ack_delivery_id))
                    .expect("heartbeat response ACK JSON")
                    .into(),
            ))
            .await
            .expect("ack heartbeat response");

        socket
            .send(ClientMessage::Text(
                serde_json::to_string(&heartbeat)
                    .expect("duplicate heartbeat JSON")
                    .into(),
            ))
            .await
            .expect("send duplicate heartbeat");
        let duplicate_ack = tokio::time::timeout(Duration::from_secs(1), socket.next())
            .await
            .expect("duplicate heartbeat ACK timeout")
            .expect("duplicate heartbeat frame")
            .expect("duplicate heartbeat websocket message");
        let ClientMessage::Text(duplicate_ack) = duplicate_ack else {
            panic!("expected duplicate heartbeat ACK text frame");
        };
        assert_eq!(
            serde_json::from_str::<AgentTransportMessage>(duplicate_ack.as_ref())
                .expect("duplicate ACK JSON"),
            AgentTransportMessage::ack(heartbeat_delivery_id)
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(100), socket.next())
                .await
                .is_err()
        );
        socket.close(None).await.expect("close");
        server.abort();
    }

    #[tokio::test]
    async fn agent_websocket_retransmits_unacknowledged_server_messages() {
        let state = AppState::new(Storage::open_in_memory().expect("storage"));
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
        let address = listener.local_addr().expect("address");
        let server = tokio::spawn(async move {
            axum::serve(listener, router(state.clone()))
                .await
                .expect("agent server");
        });
        let (mut socket, _) =
            tokio_tungstenite::connect_async(format!("ws://{address}/api/v1/agents/connect"))
                .await
                .expect("connect");
        let registration =
            AgentTransportMessage::message(AgentMessage::Register(AgentRegistration {
                protocol_version: PROTOCOL_VERSION,
                agent_id: uuid::Uuid::new_v4(),
                name: "retransmit-agent".into(),
                capabilities: AgentCapabilities {
                    os: "linux".into(),
                    arch: "x86_64".into(),
                    docker: false,
                    labels: vec![],
                    executors: 1,
                    cpu_cores: Some(4),
                    memory_mb: Some(8 * 1024),
                },
            }));
        socket
            .send(ClientMessage::Text(
                serde_json::to_string(&registration)
                    .expect("registration JSON")
                    .into(),
            ))
            .await
            .expect("send registration");

        let first = tokio::time::timeout(Duration::from_secs(1), socket.next())
            .await
            .expect("registered message timeout")
            .expect("registered frame")
            .expect("registered websocket message");
        let ClientMessage::Text(first) = first else {
            panic!("expected registered text frame");
        };
        let first: AgentTransportMessage = serde_json::from_str(first.as_ref()).expect("JSON");
        let delivery_id = first.delivery_id();
        assert!(matches!(
            first,
            AgentTransportMessage::Message {
                payload: AgentMessage::Registered { .. },
                ..
            }
        ));

        let retransmitted = tokio::time::timeout(Duration::from_secs(4), async {
            loop {
                let message = socket
                    .next()
                    .await
                    .expect("retransmitted frame")
                    .expect("websocket message");
                let ClientMessage::Text(message) = message else {
                    continue;
                };
                let envelope: AgentTransportMessage =
                    serde_json::from_str(message.as_ref()).expect("retransmitted JSON");
                if matches!(
                    envelope,
                    AgentTransportMessage::Message { delivery_id: candidate, .. }
                        if candidate == delivery_id
                ) {
                    break envelope;
                }
            }
        })
        .await
        .expect("server did not retransmit the unacknowledged message");
        assert_eq!(retransmitted.delivery_id(), delivery_id);
        socket.close(None).await.expect("close");
        server.abort();
    }

    #[tokio::test]
    async fn stale_agent_sessions_cannot_relay_or_close_a_current_remote_route() {
        let state = AppState::new(Storage::open_in_memory().expect("storage"));
        let agent_id = uuid::Uuid::new_v4();
        let current_session = uuid::Uuid::new_v4();
        let stale_session = uuid::Uuid::new_v4();
        let build_id = uuid::Uuid::new_v4();
        let (sender, mut receiver) = mpsc::channel(4);
        state.remote_messages.lock().await.insert(
            build_id,
            RemoteBuildRoute {
                agent_id,
                session_id: current_session,
                messages: sender,
            },
        );

        let stale_result = route_agent_build_message(
            &state,
            agent_id,
            stale_session,
            build_id,
            AgentMessage::Error {
                protocol_version: PROTOCOL_VERSION,
                build_id: Some(build_id),
                code: "stale".into(),
                message: "must be fenced".into(),
            },
        )
        .await;
        assert_eq!(
            stale_result.expect_err("stale session must fail").0,
            "agent_session_fenced"
        );
        assert!(receiver.try_recv().is_err());

        notify_agent_disconnect(&state, agent_id, stale_session).await;
        assert!(receiver.try_recv().is_err());

        notify_agent_disconnect(&state, agent_id, current_session).await;
        let notification = receiver.recv().await.expect("current route notification");
        assert!(matches!(
            notification,
            AgentMessage::Error {
                build_id: Some(id),
                code,
                ..
            } if id == build_id && code == "agent_disconnected"
        ));
    }

    #[tokio::test]
    async fn remote_recovery_dispatcher_reuses_persisted_build_identity() {
        let directory = tempdir().expect("workspace");
        fs::write(directory.path().join("source.txt"), "source\n").expect("source");
        let project = Project::new(
            "remote-recovery",
            directory.path().to_string_lossy().into_owned(),
            "Rivetfile.toml",
        )
        .expect("project");
        let pipeline = Pipeline::from_toml_str(
            r#"
version = 1
name = "remote-recovery"
[[stages]]
name = "Test"
[[stages.steps]]
name = "unit"
program = "true"
agent = { os = "macos", arch = "aarch64", labels = ["recovery"] }
"#,
        )
        .expect("pipeline");
        let storage = Storage::open_in_memory().expect("storage");
        storage
            .create_project(&project, &pipeline)
            .expect("project");
        let plan = ExecutionPlan::from_pipeline(&pipeline, uuid::Uuid::new_v4(), project.id);
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
        let requirements = AgentRequirements {
            os: Some("macos".into()),
            arch: Some("aarch64".into()),
            labels: vec!["recovery".into()],
            ..AgentRequirements::default()
        };
        let _attempt = storage
            .create_remote_attempt(
                build.id,
                project.id,
                uuid::Uuid::new_v4(),
                &serde_json::to_string(&requirements).expect("requirements JSON"),
                &plan,
                &pipeline,
                &BTreeMap::new(),
            )
            .expect("remote attempt");
        let replacement_agent = uuid::Uuid::new_v4();
        let replacement_attempt = uuid::Uuid::new_v4();
        assert_eq!(
            storage
                .advance_remote_attempt(build.id, replacement_agent, replacement_attempt, 1)
                .expect("persist replacement recovery slot"),
            Some(1)
        );
        let attempt = storage
            .list_remote_attempts()
            .expect("reload remote attempt")
            .into_iter()
            .next()
            .expect("reloaded attempt");
        assert_eq!(attempt.agent_id, replacement_agent);
        assert_eq!(attempt.attempt_id, replacement_attempt);
        assert_eq!(attempt.recovery_attempts, 1);
        let state = AppState::new(storage);
        let (outbound, mut received) = mpsc::channel(16);
        state
            .agents
            .register_with_sender(
                AgentRegistration {
                    protocol_version: PROTOCOL_VERSION,
                    agent_id: attempt.agent_id,
                    name: "recovery-agent".into(),
                    capabilities: AgentCapabilities {
                        os: "macos".into(),
                        arch: "aarch64".into(),
                        docker: false,
                        labels: vec!["recovery".into()],
                        executors: 1,
                        cpu_cores: Some(4),
                        memory_mb: Some(8 * 1024),
                    },
                },
                Utc::now(),
                outbound,
            )
            .await
            .expect("agent");
        let shutdown = CancellationToken::new();
        spawn_remote_recovery_dispatcher(state.clone(), vec![attempt.clone()], shutdown.clone());
        let message = tokio::time::timeout(Duration::from_secs(2), received.recv())
            .await
            .expect("recovery assignment timeout")
            .expect("recovery assignment");
        let AgentTransportMessage::Message {
            payload:
                AgentMessage::Assign {
                    build_id: assigned_build,
                    attempt_id: assigned_attempt,
                    plan: assigned_plan,
                    ..
                },
            ..
        } = message
        else {
            panic!("expected a recovery assignment");
        };
        assert_eq!(assigned_build, build.id);
        assert_eq!(assigned_attempt, attempt.attempt_id);
        assert_eq!(assigned_plan, attempt.plan);

        shutdown.cancel();
        if let Some(cancellation) = state.active_builds.lock().await.remove(&build.id) {
            cancellation.cancel();
        }
    }

    #[tokio::test]
    async fn authenticated_server_keeps_health_public_and_protects_api_routes() {
        let health = router(AppState::with_token(
            Storage::open_in_memory().expect("storage"),
            "rivet-test-token",
        ));
        let response = health
            .oneshot(
                Request::builder()
                    .uri("/api/v1/health")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);

        let readiness = router(AppState::with_token(
            Storage::open_in_memory().expect("storage"),
            "rivet-test-token",
        ));
        let response = readiness
            .oneshot(
                Request::builder()
                    .uri("/api/v1/ready")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);

        let unauthorized = router(AppState::with_token(
            Storage::open_in_memory().expect("storage"),
            "rivet-test-token",
        ));
        let response = unauthorized
            .oneshot(
                Request::builder()
                    .uri("/api/v1/queue")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        assert!(response.headers().get("x-request-id").is_some());
        assert_eq!(
            response
                .headers()
                .get(axum::http::header::WWW_AUTHENTICATE)
                .and_then(|value| value.to_str().ok()),
            Some("Bearer")
        );

        let authorized = router(AppState::with_token(
            Storage::open_in_memory().expect("storage"),
            "rivet-test-token",
        ));
        let response = authorized
            .oneshot(
                Request::builder()
                    .uri("/api/v1/queue")
                    .header(axum::http::header::AUTHORIZATION, "Bearer rivet-test-token")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn authenticated_sessions_can_be_created_used_and_revoked_without_exposing_tokens() {
        let raw_token = "session-source-fixture-token";
        let storage = Storage::open_in_memory().expect("storage");
        let state = AppState::with_token(storage.clone(), raw_token);
        let app = router(state);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/api/v1/auth/sessions")
                    .header(
                        axum::http::header::AUTHORIZATION,
                        format!("Bearer {raw_token}"),
                    )
                    .body(Body::empty())
                    .expect("create session request"),
            )
            .await
            .expect("create session response");
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), 16 * 1024)
            .await
            .expect("session body");
        assert!(
            !body
                .windows(raw_token.len())
                .any(|window| window == raw_token.as_bytes())
        );
        let payload: serde_json::Value = serde_json::from_slice(&body).expect("session JSON");
        let session_token = payload["session_token"]
            .as_str()
            .expect("session token")
            .to_owned();
        assert_eq!(session_token.len(), 64);
        assert!(payload["expires_at"].is_string());

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/v1/auth/me")
                    .header(
                        axum::http::header::AUTHORIZATION,
                        format!("Bearer {session_token}"),
                    )
                    .body(Body::empty())
                    .expect("session identity request"),
            )
            .await
            .expect("session identity response");
        assert_eq!(response.status(), StatusCode::OK);
        let identity: serde_json::Value = serde_json::from_slice(
            &to_bytes(response.into_body(), 16 * 1024)
                .await
                .expect("identity body"),
        )
        .expect("identity JSON");
        assert_eq!(identity["id"], "legacy-token");
        assert_eq!(identity["local_mode"], false);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::DELETE)
                    .uri("/api/v1/auth/sessions/current")
                    .header(
                        axum::http::header::AUTHORIZATION,
                        format!("Bearer {session_token}"),
                    )
                    .body(Body::empty())
                    .expect("revoke session request"),
            )
            .await
            .expect("revoke session response");
        assert_eq!(response.status(), StatusCode::NO_CONTENT);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/auth/me")
                    .header(
                        axum::http::header::AUTHORIZATION,
                        format!("Bearer {session_token}"),
                    )
                    .body(Body::empty())
                    .expect("revoked identity request"),
            )
            .await
            .expect("revoked identity response");
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        assert!(
            storage
                .list_audit_events(50)
                .expect("session audit")
                .iter()
                .any(|event| event.action == "auth.session" && event.outcome == "created")
        );
        assert!(
            storage
                .list_audit_events(50)
                .expect("revoke audit")
                .iter()
                .any(|event| event.action == "auth.session" && event.outcome == "revoked")
        );
    }

    #[tokio::test]
    async fn authentication_audit_is_private_and_never_contains_the_token() {
        let raw_token = "audit-admin-fixture-token";
        let state = AppState::with_token(Storage::open_in_memory().expect("storage"), raw_token);
        let app = router(state);

        let unauthorized = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/v1/auth/me")
                    .body(Body::empty())
                    .expect("unauthorized request"),
            )
            .await
            .expect("unauthorized response");
        assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);

        let audit_response = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/audit")
                    .header(
                        axum::http::header::AUTHORIZATION,
                        format!("Bearer {raw_token}"),
                    )
                    .body(Body::empty())
                    .expect("audit request"),
            )
            .await
            .expect("audit response");
        assert_eq!(audit_response.status(), StatusCode::OK);
        let body = to_bytes(audit_response.into_body(), 64 * 1024)
            .await
            .expect("audit body");
        let body_text = String::from_utf8_lossy(&body);
        assert!(!body_text.contains(raw_token));
        let events: Vec<AuditEventRecord> = serde_json::from_slice(&body).expect("audit JSON");
        assert!(events.iter().any(|event| {
            event.action == "auth.authenticate"
                && event.outcome == "failure"
                && event.actor_id.is_none()
        }));
        assert!(events.iter().any(|event| {
            event.action == "auth.authenticate"
                && event.outcome == "success"
                && event.actor_id.as_deref() == Some("legacy-token")
        }));
    }

    #[tokio::test]
    async fn admin_credential_api_rotates_without_returning_secrets() {
        let directory = tempdir().expect("tempdir");
        let vault_path = directory.path().join("credentials.vault");
        let vault = CredentialVault::open_or_create(&vault_path, "credential-api-passphrase")
            .expect("vault");
        let storage = Storage::open_in_memory().expect("storage");
        let mut state = AppState::new(storage.clone());
        state.credentials = Some(Arc::new(Mutex::new(vault)));

        let response = router(state.clone())
            .oneshot(
                Request::builder()
                    .uri("/api/v1/credentials")
                    .body(Body::empty())
                    .expect("list request"),
            )
            .await
            .expect("list response");
        assert_eq!(response.status(), StatusCode::OK);
        let initial = to_bytes(response.into_body(), 16 * 1024)
            .await
            .expect("initial body");
        assert_eq!(&initial[..], b"[]");

        let put = |secret: &'static str| {
            Request::builder()
                .method("PUT")
                .uri("/api/v1/credentials/github")
                .header("content-type", "application/json")
                .body(Body::from(format!(
                    r#"{{"username":"oauth2","secret":"{secret}"}}"#
                )))
                .expect("credential request")
        };
        let response = router(state.clone())
            .oneshot(put("first-secret"))
            .await
            .expect("credential response");
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), 16 * 1024)
            .await
            .expect("credential body");
        let summary: serde_json::Value = serde_json::from_slice(&body).expect("summary JSON");
        assert_eq!(summary["id"], "github");
        assert_eq!(summary["username"], "oauth2");
        assert!(
            !body
                .windows(b"first-secret".len())
                .any(|window| { window == b"first-secret" })
        );

        let response = router(state.clone())
            .oneshot(put("rotated-secret"))
            .await
            .expect("rotation response");
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), 16 * 1024)
            .await
            .expect("rotation body");
        assert!(
            !body
                .windows(b"rotated-secret".len())
                .any(|window| { window == b"rotated-secret" })
        );
        let stored = state
            .credentials
            .as_ref()
            .expect("configured vault")
            .lock()
            .await
            .get("github")
            .expect("rotated credential");
        assert_eq!(stored.secret(), "rotated-secret");

        let response = router(state.clone())
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri("/api/v1/credentials/github")
                    .body(Body::empty())
                    .expect("delete request"),
            )
            .await
            .expect("delete response");
        assert_eq!(response.status(), StatusCode::NO_CONTENT);

        let events = storage.list_audit_events(10).expect("audit events");
        assert!(events.iter().any(|event| {
            event.action == "credentials.manage"
                && event.resource == "github"
                && event.outcome == "set"
        }));
        assert!(events.iter().any(|event| {
            event.action == "credentials.manage"
                && event.resource == "github"
                && event.outcome == "remove"
        }));
    }

    #[tokio::test]
    async fn scoped_credential_api_limits_scm_resolution_to_declared_projects() {
        let directory = tempdir().expect("tempdir");
        let vault_path = directory.path().join("credentials.vault");
        let vault = CredentialVault::open_or_create(&vault_path, "credential-scope-passphrase")
            .expect("vault");
        let mut state = AppState::new(Storage::open_in_memory().expect("storage"));
        state.credentials = Some(Arc::new(Mutex::new(vault)));

        let response = router(state.clone())
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/api/v1/credentials/release-token")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"username":"oauth2","secret":"scope-secret","projects":["release","web"]}"#,
                    ))
                    .expect("credential request"),
            )
            .await
            .expect("credential response");
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), 16 * 1024)
            .await
            .expect("credential body");
        let summary: serde_json::Value = serde_json::from_slice(&body).expect("summary JSON");
        assert_eq!(summary["projects"], serde_json::json!(["release", "web"]));
        assert!(
            !body
                .windows(b"scope-secret".len())
                .any(|window| { window == b"scope-secret" })
        );

        let ssh_response = router(state.clone())
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/api/v1/credentials/deploy-key")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"kind":"ssh_key","username":"git","secret":"-----BEGIN OPENSSH PRIVATE KEY-----\nfixture-key\n-----END OPENSSH PRIVATE KEY-----","projects":["release"]}"#,
                    ))
                    .expect("ssh credential request"),
            )
            .await
            .expect("ssh credential response");
        assert_eq!(ssh_response.status(), StatusCode::OK);
        let ssh_body = to_bytes(ssh_response.into_body(), 16 * 1024)
            .await
            .expect("ssh credential body");
        let ssh_summary: serde_json::Value =
            serde_json::from_slice(&ssh_body).expect("ssh summary JSON");
        assert_eq!(ssh_summary["kind"], "ssh_key");
        assert!(
            !ssh_body
                .windows(b"fixture-key".len())
                .any(|window| { window == b"fixture-key" })
        );

        assert!(
            resolve_git_credential(state.credentials.as_ref(), Some("release-token"), "release")
                .await
                .expect("allowed resolution")
                .is_some()
        );
        let denied = resolve_git_credential(
            state.credentials.as_ref(),
            Some("release-token"),
            "unrelated",
        )
        .await;
        assert!(
            matches!(denied, Err(ApiError::BadRequest(message)) if message.contains("not found"))
        );
        assert!(matches!(
            resolve_git_credential(state.credentials.as_ref(), Some("deploy-key"), "release")
                .await
                .expect("ssh resolution"),
            Some(GitCredential::SshKey(_))
        ));
    }

    #[tokio::test]
    async fn policy_tokens_report_identity_without_exposing_raw_tokens() {
        let raw_token = "operator-policy-fixture-token";
        let policy = AuthPolicy::from_document(AuthPolicyDocument {
            version: 1,
            tokens: vec![ApiTokenRecord {
                id: "operator".into(),
                sha256: hex::encode(Sha256::digest(raw_token.as_bytes())),
                role: Role::Operator,
                projects: vec!["demo".into()],
                expires_at: None,
            }],
        })
        .expect("policy");
        let mut state = AppState::new(Storage::open_in_memory().expect("storage"));
        state.auth_policy = Some(Arc::new(policy));

        let unauthorized = router(state.clone())
            .oneshot(
                Request::builder()
                    .uri("/api/v1/auth/me")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);

        let response = router(state)
            .oneshot(
                Request::builder()
                    .uri("/api/v1/auth/me")
                    .header(
                        axum::http::header::AUTHORIZATION,
                        format!("Bearer {raw_token}"),
                    )
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), 16 * 1024)
            .await
            .expect("body");
        let body_text = String::from_utf8_lossy(&body);
        assert!(!body_text.contains(raw_token));
        let payload: serde_json::Value = serde_json::from_slice(&body).expect("JSON");
        assert_eq!(payload["id"], "operator");
        assert_eq!(payload["role"], "operator");
        assert_eq!(payload["projects"][0], "demo");
        assert_eq!(payload["local_mode"], false);
    }

    #[tokio::test]
    async fn policy_project_scopes_filter_reads_and_block_mutations() {
        let directory = tempdir().expect("tempdir");
        let repository = directory.path().join("repository");
        fs::create_dir_all(&repository).expect("repository");
        let pipeline_path = repository.join("Rivetfile.toml");
        fs::write(
            &pipeline_path,
            "version = 1\nname = \"scope\"\n[[stages]]\nname = \"Test\"\n[[stages.steps]]\nname = \"noop\"\nprogram = \"true\"\n",
        )
        .expect("pipeline");
        let pipeline = Pipeline::load(&pipeline_path).expect("pipeline");
        let storage = Storage::open_in_memory().expect("storage");
        for name in ["allowed", "hidden"] {
            let project = Project::new(
                name,
                repository.to_string_lossy().into_owned(),
                pipeline_path.to_string_lossy().into_owned(),
            )
            .expect("project");
            storage
                .create_project(&project, &pipeline)
                .expect("create project");
        }
        let raw_token = "scoped-operator-fixture-token";
        let policy = AuthPolicy::from_document(AuthPolicyDocument {
            version: 1,
            tokens: vec![ApiTokenRecord {
                id: "operator".into(),
                sha256: hex::encode(Sha256::digest(raw_token.as_bytes())),
                role: Role::Operator,
                projects: vec!["allowed".into()],
                expires_at: None,
            }],
        })
        .expect("policy");
        let mut state = AppState::new(storage);
        state.auth_policy = Some(Arc::new(policy));

        let response = router(state.clone())
            .oneshot(
                Request::builder()
                    .uri("/api/v1/projects")
                    .header(
                        axum::http::header::AUTHORIZATION,
                        format!("Bearer {raw_token}"),
                    )
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);
        let projects: Vec<Project> = serde_json::from_slice(
            &to_bytes(response.into_body(), 16 * 1024)
                .await
                .expect("projects body"),
        )
        .expect("projects JSON");
        assert_eq!(
            projects
                .iter()
                .map(|project| project.name.as_str())
                .collect::<Vec<_>>(),
            ["allowed"]
        );

        let response = router(state.clone())
            .oneshot(
                Request::builder()
                    .uri("/api/v1/projects/allowed/builds")
                    .header(
                        axum::http::header::AUTHORIZATION,
                        format!("Bearer {raw_token}"),
                    )
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);

        let response = router(state.clone())
            .oneshot(
                Request::builder()
                    .uri("/api/v1/projects/hidden/builds")
                    .header(
                        axum::http::header::AUTHORIZATION,
                        format!("Bearer {raw_token}"),
                    )
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::FORBIDDEN);

        let response = router(state)
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/api/v1/projects")
                    .header(
                        axum::http::header::AUTHORIZATION,
                        format!("Bearer {raw_token}"),
                    )
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"name":"new","repository_path":"/not-used"}"#,
                    ))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn generic_webhook_verifies_signature_and_deduplicates_builds() {
        let directory = tempdir().expect("tempdir");
        let repository = directory.path().join("repository");
        fs::create_dir_all(&repository).expect("repository");
        let pipeline_path = repository.join("Rivetfile.toml");
        fs::write(
            &pipeline_path,
            r#"
version = 1
name = "webhook"
[[stages]]
name = "Test"
[[stages.steps]]
name = "unit"
program = "true"
"#,
        )
        .expect("pipeline file");
        let pipeline = Pipeline::load(&pipeline_path).expect("pipeline");
        let project = Project::new(
            "webhook-demo",
            repository.to_string_lossy().into_owned(),
            pipeline_path.to_string_lossy().into_owned(),
        )
        .expect("project");
        let storage = Storage::open_in_memory().expect("storage");
        storage
            .create_project(&project, &pipeline)
            .expect("project");
        let upstream_project = Project::new(
            "upstream-demo",
            repository.to_string_lossy().into_owned(),
            pipeline_path.to_string_lossy().into_owned(),
        )
        .expect("upstream project");
        storage
            .create_project(&upstream_project, &pipeline)
            .expect("upstream project");
        let upstream_plan =
            ExecutionPlan::from_pipeline(&pipeline, Uuid::new_v4(), upstream_project.id);
        let upstream_build = storage
            .create_build(&upstream_project, &upstream_plan, &pipeline, None)
            .expect("upstream build");
        storage
            .apply_event(&BuildEvent::BuildQueued {
                build_id: upstream_build.id,
                project_id: upstream_project.id,
                timestamp: Utc::now(),
            })
            .expect("upstream queue");
        storage
            .apply_event(&BuildEvent::BuildStarted {
                build_id: upstream_build.id,
                timestamp: Utc::now(),
            })
            .expect("upstream start");
        storage
            .apply_event(&BuildEvent::BuildFinished {
                build_id: upstream_build.id,
                status: BuildStatus::Passed,
                timestamp: Utc::now(),
            })
            .expect("upstream finish");
        let state = AppState::with_webhook_secret(storage.clone(), "webhook-test-secret");
        let failed_upstream = br#"{"event_id":"upstream-failed-1","project":"webhook-demo","upstream":{"project":"upstream-demo","build":1,"status":"failed"}}"#.to_vec();
        let failed_signature = sign_webhook("webhook-test-secret", &failed_upstream);
        let response = router(state.clone())
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/webhooks/generic")
                    .header("content-type", "application/json")
                    .header("x-rivet-signature", &failed_signature)
                    .body(Body::from(failed_upstream))
                    .expect("failed upstream request"),
            )
            .await
            .expect("failed upstream response");
        assert_eq!(response.status(), StatusCode::OK);
        let ignored: serde_json::Value = serde_json::from_slice(
            &to_bytes(response.into_body(), 16 * 1024)
                .await
                .expect("ignored upstream body"),
        )
        .expect("ignored upstream response");
        assert_eq!(ignored["status"], "ignored");
        assert_eq!(ignored["deduplicated"], false);
        assert!(ignored["build"].is_null());
        assert!(storage.list_builds(project.id).expect("builds").is_empty());

        let unrecorded_body = br#"{"event_id":"upstream-missing-1","project":"webhook-demo","upstream":{"project":"upstream-demo","build":99,"status":"passed"}}"#.to_vec();
        let unrecorded_signature = sign_webhook("webhook-test-secret", &unrecorded_body);
        let response = router(state.clone())
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/webhooks/generic")
                    .header("content-type", "application/json")
                    .header("x-rivet-signature", &unrecorded_signature)
                    .body(Body::from(unrecorded_body))
                    .expect("unrecorded upstream request"),
            )
            .await
            .expect("unrecorded upstream response");
        assert_eq!(response.status(), StatusCode::NOT_FOUND);

        let body = br#"{"event_id":"delivery-1","project":"webhook-demo","upstream":{"project":"upstream-demo","build":1,"status":"passed"}}"#.to_vec();

        let response = router(state.clone())
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/webhooks/generic")
                    .header("content-type", "application/json")
                    .body(Body::from(body.clone()))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

        let signature = sign_webhook("webhook-test-secret", &body);
        let response = router(state.clone())
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/webhooks/generic")
                    .header("content-type", "application/json")
                    .header("x-rivet-signature", &signature)
                    .body(Body::from(body.clone()))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::ACCEPTED);
        let queued: serde_json::Value = serde_json::from_slice(
            &to_bytes(response.into_body(), 16 * 1024)
                .await
                .expect("body"),
        )
        .expect("queued response");
        assert_eq!(queued["status"], "queued");
        assert_eq!(queued["deduplicated"], false);
        assert!(queued["build"]["id"].as_str().is_some());

        let response = router(state.clone())
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/webhooks/generic")
                    .header("content-type", "application/json")
                    .header("x-rivet-signature", &signature)
                    .body(Body::from(body))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);
        let duplicate: serde_json::Value = serde_json::from_slice(
            &to_bytes(response.into_body(), 16 * 1024)
                .await
                .expect("duplicate body"),
        )
        .expect("duplicate response");
        assert_eq!(duplicate["status"], "already_queued");
        assert_eq!(duplicate["deduplicated"], true);

        for _ in 0..50 {
            if storage
                .list_builds(project.id)
                .expect("builds")
                .first()
                .is_some_and(|build| build.status.is_terminal())
            {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
        let builds = storage.list_builds(project.id).expect("builds");
        assert_eq!(builds.len(), 1);
        assert_eq!(builds[0].status, BuildStatus::Passed);
    }

    #[tokio::test]
    async fn generic_webhook_rejects_invalid_signature_and_missing_configuration() {
        let body = br#"{"event_id":"delivery-1","project":"missing"}"#;
        let request = || {
            Request::builder()
                .method("POST")
                .uri("/api/v1/webhooks/generic")
                .header("content-type", "application/json")
                .header("x-rivet-signature", "sha256=00")
                .body(Body::from(body.as_slice()))
                .expect("request")
        };

        let response = router(AppState::new(Storage::open_in_memory().expect("storage")))
            .oneshot(request())
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);

        let state = AppState::with_webhook_secret(
            Storage::open_in_memory().expect("storage"),
            "webhook-test-secret",
        );
        let response = router(state).oneshot(request()).await.expect("response");
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn github_provider_route_uses_explicit_project_and_deduplicates_builds() {
        let directory = tempdir().expect("tempdir");
        let repository = directory.path().join("repository");
        fs::create_dir_all(&repository).expect("repository");
        let pipeline_path = repository.join("Rivetfile.toml");
        fs::write(
            &pipeline_path,
            r#"
version = 1
name = "provider-webhook"
[[stages]]
name = "Test"
[[stages.steps]]
name = "unit"
program = "true"
"#,
        )
        .expect("pipeline file");
        let pipeline = Pipeline::load(&pipeline_path).expect("pipeline");
        let project = Project::new(
            "github-demo",
            repository.to_string_lossy().into_owned(),
            pipeline_path.to_string_lossy().into_owned(),
        )
        .expect("project");
        let storage = Storage::open_in_memory().expect("storage");
        storage
            .create_project(&project, &pipeline)
            .expect("project");
        let mut state = AppState::new(storage.clone());
        state.github_webhook_secret = Some(b"github-route-secret".to_vec());

        let git = |args: &[&str]| -> String {
            let output = std::process::Command::new("git")
                .args(args)
                .current_dir(&repository)
                .output()
                .expect("git available");
            assert!(
                output.status.success(),
                "git {:?}: {}",
                args,
                String::from_utf8_lossy(&output.stderr)
            );
            String::from_utf8_lossy(&output.stdout).trim().to_owned()
        };
        git(&["init", "-q"]);
        git(&["config", "user.email", "rivet@example.test"]);
        git(&["config", "user.name", "Rivet Tests"]);
        git(&["add", "Rivetfile.toml"]);
        git(&["commit", "-qm", "provider fixture"]);
        let revision = git(&["rev-parse", "HEAD"]);
        let repository_string = repository.to_string_lossy().into_owned();
        let output = std::process::Command::new("git")
            .args(["remote", "add", "origin"])
            .arg(&repository_string)
            .current_dir(&repository)
            .output()
            .expect("git available");
        assert!(
            output.status.success(),
            "git remote add: {}",
            String::from_utf8_lossy(&output.stderr)
        );

        let body = format!(r#"{{"after":"{revision}","deleted":false}}"#).into_bytes();
        let signature = sign_webhook("github-route-secret", &body);
        let request = || {
            Request::builder()
                .method("POST")
                .uri("/api/v1/webhooks/github/github-demo")
                .header("content-type", "application/json")
                .header("x-github-event", "push")
                .header("x-github-delivery", "github-route-delivery-1")
                .header("x-hub-signature-256", &signature)
                .body(Body::from(body.clone()))
                .expect("request")
        };

        let response = router(state.clone())
            .oneshot(request())
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::ACCEPTED);
        let queued: serde_json::Value = serde_json::from_slice(
            &to_bytes(response.into_body(), 16 * 1024)
                .await
                .expect("body"),
        )
        .expect("queued response");
        assert_eq!(queued["status"], "queued");
        assert_eq!(queued["deduplicated"], false);
        assert!(queued["build"]["id"].as_str().is_some());

        let response = router(state.clone())
            .oneshot(request())
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);
        let duplicate: serde_json::Value = serde_json::from_slice(
            &to_bytes(response.into_body(), 16 * 1024)
                .await
                .expect("duplicate body"),
        )
        .expect("duplicate response");
        assert_eq!(duplicate["status"], "already_queued");
        assert_eq!(duplicate["deduplicated"], true);

        for _ in 0..50 {
            if storage
                .list_builds(project.id)
                .expect("builds")
                .first()
                .is_some_and(|build| build.status.is_terminal())
            {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
        let builds = storage.list_builds(project.id).expect("builds");
        assert_eq!(builds.len(), 1);
        assert!(builds[0].status.is_terminal());
    }

    #[tokio::test]
    async fn bitbucket_provider_route_queues_and_deduplicates_pushes() {
        let directory = tempdir().expect("tempdir");
        let repository = directory.path().join("repository");
        fs::create_dir_all(&repository).expect("repository");
        let pipeline_path = repository.join("Rivetfile.toml");
        fs::write(
            &pipeline_path,
            r#"
version = 1
name = "bitbucket-webhook"
[[stages]]
name = "Test"
[[stages.steps]]
name = "unit"
program = "true"
"#,
        )
        .expect("pipeline file");
        let pipeline = Pipeline::load(&pipeline_path).expect("pipeline");
        let project = Project::new(
            "bitbucket-demo",
            repository.to_string_lossy().into_owned(),
            pipeline_path.to_string_lossy().into_owned(),
        )
        .expect("project");
        let storage = Storage::open_in_memory().expect("storage");
        storage
            .create_project(&project, &pipeline)
            .expect("project");
        let mut state = AppState::new(storage.clone());
        state.bitbucket_webhook_secret = Some(b"bitbucket-route-secret".to_vec());

        let git = |args: &[&str]| -> String {
            let output = std::process::Command::new("git")
                .args(args)
                .current_dir(&repository)
                .output()
                .expect("git available");
            assert!(
                output.status.success(),
                "git {:?}: {}",
                args,
                String::from_utf8_lossy(&output.stderr)
            );
            String::from_utf8_lossy(&output.stdout).trim().to_owned()
        };
        git(&["init", "-q"]);
        git(&["config", "user.email", "rivet@example.test"]);
        git(&["config", "user.name", "Rivet Tests"]);
        git(&["add", "Rivetfile.toml"]);
        git(&["commit", "-qm", "bitbucket fixture"]);
        let revision = git(&["rev-parse", "HEAD"]);
        let repository_string = repository.to_string_lossy().into_owned();
        let output = std::process::Command::new("git")
            .args(["remote", "add", "origin"])
            .arg(&repository_string)
            .current_dir(&repository)
            .output()
            .expect("git available");
        assert!(
            output.status.success(),
            "git remote add: {}",
            String::from_utf8_lossy(&output.stderr)
        );

        let body = serde_json::json!({
            "push": {
                "changes": [{
                    "new": {
                        "name": "main",
                        "target": { "hash": revision }
                    }
                }]
            }
        })
        .to_string()
        .into_bytes();
        let signature = sign_webhook("bitbucket-route-secret", &body);
        let request = || {
            Request::builder()
                .method("POST")
                .uri("/api/v1/webhooks/bitbucket/bitbucket-demo")
                .header("content-type", "application/json")
                .header("x-event-key", "repo:push")
                .header("x-request-uuid", "bitbucket-route-delivery-1")
                .header("x-hub-signature", &signature)
                .body(Body::from(body.clone()))
                .expect("request")
        };

        let response = router(state.clone())
            .oneshot(request())
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::ACCEPTED);
        let queued: serde_json::Value = serde_json::from_slice(
            &to_bytes(response.into_body(), 16 * 1024)
                .await
                .expect("body"),
        )
        .expect("queued response");
        assert_eq!(queued["status"], "queued");
        assert_eq!(queued["deduplicated"], false);

        let response = router(state.clone())
            .oneshot(request())
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);
        let duplicate: serde_json::Value = serde_json::from_slice(
            &to_bytes(response.into_body(), 16 * 1024)
                .await
                .expect("duplicate body"),
        )
        .expect("duplicate response");
        assert_eq!(duplicate["status"], "already_queued");
        assert_eq!(duplicate["deduplicated"], true);

        for _ in 0..100 {
            if storage
                .list_builds(project.id)
                .expect("builds")
                .first()
                .is_some_and(|build| build.status.is_terminal())
            {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
        let builds = storage.list_builds(project.id).expect("builds");
        assert_eq!(builds.len(), 1);
        assert_eq!(builds[0].status, BuildStatus::Passed);
    }

    #[tokio::test]
    async fn repository_poll_queues_only_when_the_head_revision_changes() {
        let directory = tempdir().expect("tempdir");
        let repository = directory.path().join("repository");
        fs::create_dir_all(&repository).expect("repository");
        let pipeline_path = repository.join("Rivetfile.toml");
        fs::write(
            &pipeline_path,
            r#"
version = 1
name = "repository-poll"
[[stages]]
name = "Test"
[[stages.steps]]
name = "unit"
program = "true"
"#,
        )
        .expect("pipeline file");
        let pipeline = Pipeline::load(&pipeline_path).expect("pipeline");
        let project = Project::new(
            "repository-poll",
            repository.to_string_lossy().into_owned(),
            pipeline_path.to_string_lossy().into_owned(),
        )
        .expect("project");
        let storage = Storage::open_in_memory().expect("storage");
        storage
            .create_project(&project, &pipeline)
            .expect("project");
        let state = AppState::new(storage.clone());
        let git = |args: &[&str]| -> String {
            let output = std::process::Command::new("git")
                .args(args)
                .current_dir(&repository)
                .output()
                .expect("git available");
            assert!(
                output.status.success(),
                "git {:?}: {}",
                args,
                String::from_utf8_lossy(&output.stderr)
            );
            String::from_utf8_lossy(&output.stdout).trim().to_owned()
        };
        git(&["init", "-q"]);
        git(&["config", "user.email", "rivet@example.test"]);
        git(&["config", "user.name", "Rivet Tests"]);
        git(&["add", "Rivetfile.toml"]);
        git(&["commit", "-qm", "initial poll fixture"]);
        let first_revision = git(&["rev-parse", "HEAD"]);

        let poll = || {
            Request::builder()
                .method(Method::POST)
                .uri("/api/v1/projects/repository-poll/repository-changes")
                .header("content-type", "application/json")
                .body(Body::from("{}"))
                .expect("poll request")
        };
        let response = router(state.clone())
            .oneshot(poll())
            .await
            .expect("first poll response");
        let first_status = response.status();
        let first_body = to_bytes(response.into_body(), 16 * 1024)
            .await
            .expect("first poll body");
        assert_eq!(
            first_status,
            StatusCode::ACCEPTED,
            "{}",
            String::from_utf8_lossy(&first_body)
        );
        let first: serde_json::Value =
            serde_json::from_slice(&first_body).expect("first poll JSON");
        assert_eq!(first["status"], "queued");
        assert_eq!(first["changed"], true);
        assert_eq!(first["deduplicated"], false);
        assert_eq!(first["revision"], first_revision);

        for _ in 0..50 {
            if storage
                .list_builds(project.id)
                .expect("builds")
                .first()
                .is_some_and(|build| build.status.is_terminal())
            {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
        let response = router(state.clone())
            .oneshot(poll())
            .await
            .expect("unchanged poll response");
        assert_eq!(response.status(), StatusCode::OK);
        let unchanged: serde_json::Value = serde_json::from_slice(
            &to_bytes(response.into_body(), 16 * 1024)
                .await
                .expect("unchanged poll body"),
        )
        .expect("unchanged poll JSON");
        assert_eq!(unchanged["status"], "unchanged");
        assert_eq!(unchanged["changed"], false);
        assert_eq!(storage.list_builds(project.id).expect("one build").len(), 1);

        fs::write(repository.join("change.txt"), "revision two\n").expect("change");
        git(&["add", "change.txt"]);
        git(&["commit", "-qm", "second poll fixture"]);
        let second_revision = git(&["rev-parse", "HEAD"]);
        let response = router(state.clone())
            .oneshot(poll())
            .await
            .expect("changed poll response");
        assert_eq!(response.status(), StatusCode::ACCEPTED);
        let changed: serde_json::Value = serde_json::from_slice(
            &to_bytes(response.into_body(), 16 * 1024)
                .await
                .expect("changed poll body"),
        )
        .expect("changed poll JSON");
        assert_eq!(changed["status"], "queued");
        assert_eq!(changed["changed"], true);
        assert_eq!(changed["revision"], second_revision);
        assert_eq!(
            storage.list_builds(project.id).expect("two builds").len(),
            2
        );

        let response = router(state)
            .oneshot(poll())
            .await
            .expect("duplicate changed poll response");
        assert_eq!(response.status(), StatusCode::OK);
        let duplicate: serde_json::Value = serde_json::from_slice(
            &to_bytes(response.into_body(), 16 * 1024)
                .await
                .expect("duplicate changed poll body"),
        )
        .expect("duplicate changed poll JSON");
        assert_eq!(duplicate["status"], "unchanged");
        assert_eq!(
            storage
                .list_builds(project.id)
                .expect("still two builds")
                .len(),
            2
        );
    }

    #[tokio::test]
    async fn retry_requires_explicit_secret_values() {
        let directory = tempdir().expect("tempdir");
        let repository = directory.path().join("repository");
        fs::create_dir_all(&repository).expect("repository");
        let pipeline_path = repository.join("Rivetfile.toml");
        fs::write(
            &pipeline_path,
            r#"
version = 1
name = "secret-retry"
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
        .expect("pipeline file");
        let pipeline = Pipeline::load(&pipeline_path).expect("pipeline");
        let project = Project::new(
            "secret-retry",
            repository.to_string_lossy().into_owned(),
            pipeline_path.to_string_lossy().into_owned(),
        )
        .expect("project");
        let storage = Storage::open_in_memory().expect("storage");
        storage
            .create_project(&project, &pipeline)
            .expect("project");
        let state = AppState::new(storage.clone());

        let initial_body = br#"{"parameters":{"TOKEN":"initial-secret"}}"#;
        let response = router(state.clone())
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/projects/secret-retry/builds")
                    .header("content-type", "application/json")
                    .body(Body::from(initial_body.as_slice()))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::ACCEPTED);
        let queued = to_bytes(response.into_body(), 16 * 1024)
            .await
            .expect("queued body");
        assert!(
            !queued
                .windows(b"initial-secret".len())
                .any(|window| { window == b"initial-secret" })
        );

        for _ in 0..50 {
            if storage
                .list_builds(project.id)
                .expect("builds")
                .first()
                .is_some_and(|build| build.status.is_terminal())
            {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(
            storage.list_builds(project.id).expect("builds")[0].status,
            BuildStatus::Passed
        );

        let response = router(state.clone())
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/projects/secret-retry/builds/1/retry")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("retry response");
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let error: serde_json::Value = serde_json::from_slice(
            &to_bytes(response.into_body(), 16 * 1024)
                .await
                .expect("retry error body"),
        )
        .expect("retry error JSON");
        assert_eq!(
            error["error"],
            "retry requires explicit values for secret parameters"
        );

        let response = router(state.clone())
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/projects/secret-retry/builds/1/retry")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        br#"{"parameters":{"TOKEN":"retry-secret"}}"#.as_slice(),
                    ))
                    .expect("request"),
            )
            .await
            .expect("retry response");
        assert_eq!(response.status(), StatusCode::ACCEPTED);
        let queued = to_bytes(response.into_body(), 16 * 1024)
            .await
            .expect("retry queued body");
        assert!(
            !queued
                .windows(b"retry-secret".len())
                .any(|window| { window == b"retry-secret" })
        );
    }

    fn sign_webhook(secret: &str, body: &[u8]) -> String {
        let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).expect("secret");
        mac.update(body);
        format!("sha256={}", hex::encode(mac.finalize().into_bytes()))
    }

    fn sign_gitlab_webhook(
        signing_key: &[u8],
        message_id: &str,
        timestamp: i64,
        body: &[u8],
    ) -> String {
        let mut message =
            Vec::with_capacity(message_id.len() + timestamp.to_string().len() + body.len() + 2);
        message.extend_from_slice(message_id.as_bytes());
        message.push(b'.');
        message.extend_from_slice(timestamp.to_string().as_bytes());
        message.push(b'.');
        message.extend_from_slice(body);
        let mut mac = Hmac::<Sha256>::new_from_slice(signing_key).expect("signing key");
        mac.update(&message);
        format!("v1,{}", STANDARD.encode(mac.finalize().into_bytes()))
    }

    #[test]
    fn github_push_webhook_uses_official_signature_and_delivery_headers() {
        let body = br#"{"after":"c783c3523482029c449dcdff1209ed06409b83bc","deleted":false}"#;
        let mut headers = HeaderMap::new();
        headers.insert("x-github-event", HeaderValue::from_static("push"));
        headers.insert(
            "x-github-delivery",
            HeaderValue::from_static("github-delivery-1"),
        );
        let signature = sign_webhook("github-fixture-secret", body);
        headers.insert(
            "x-hub-signature-256",
            HeaderValue::from_str(&signature).expect("signature header"),
        );

        let request = normalize_github_webhook(
            b"github-fixture-secret",
            &headers,
            body,
            "demo".into(),
            Some("github".into()),
        )
        .expect("normalize")
        .expect("push request");
        assert_eq!(request.event_id, "github-delivery-1");
        assert_eq!(request.project, "demo");
        assert_eq!(
            request.revision.as_deref(),
            Some("c783c3523482029c449dcdff1209ed06409b83bc")
        );
        assert!(request.fetch);
        assert_eq!(request.credential_id.as_deref(), Some("github"));
    }

    #[test]
    fn github_pull_request_webhook_fetches_the_provider_ref() {
        let body = br#"{"action":"synchronize","number":42,"pull_request":{"head":{"sha":"c783c3523482029c449dcdff1209ed06409b83bc"}}}"#;
        let mut headers = HeaderMap::new();
        headers.insert("x-github-event", HeaderValue::from_static("pull_request"));
        headers.insert(
            "x-github-delivery",
            HeaderValue::from_static("github-pr-delivery-1"),
        );
        let signature = sign_webhook("github-fixture-secret", body);
        headers.insert(
            "x-hub-signature-256",
            HeaderValue::from_str(&signature).expect("signature header"),
        );

        let request = normalize_github_webhook(
            b"github-fixture-secret",
            &headers,
            body,
            "demo".into(),
            Some("github".into()),
        )
        .expect("normalize")
        .expect("pull request request");
        assert_eq!(request.event_id, "github-pr-delivery-1");
        assert_eq!(
            request.revision.as_deref(),
            Some("c783c3523482029c449dcdff1209ed06409b83bc")
        );
        assert_eq!(
            request.fetch_ref.as_deref(),
            Some("+refs/pull/42/head:refs/remotes/origin/pull/42")
        );
        assert!(request.fetch);

        let ignored = br#"{"action":"closed","number":42,"pull_request":{"head":{"sha":"c783c3523482029c449dcdff1209ed06409b83bc"}}}"#;
        let mut ignored_headers = headers;
        ignored_headers.insert(
            "x-hub-signature-256",
            HeaderValue::from_str(&sign_webhook("github-fixture-secret", ignored))
                .expect("ignored signature"),
        );
        assert!(
            normalize_github_webhook(
                b"github-fixture-secret",
                &ignored_headers,
                ignored,
                "demo".into(),
                None,
            )
            .expect("ignored normalize")
            .is_none()
        );
    }

    #[test]
    fn github_repository_dispatch_requires_an_exact_revision_and_maps_parameters() {
        let body = br#"{"client_payload":{"rivet_revision":"c783c3523482029c449dcdff1209ed06409b83bc","rivet_parameters":{"ENV":"staging"}}}"#;
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-github-event",
            HeaderValue::from_static("repository_dispatch"),
        );
        headers.insert(
            "x-github-delivery",
            HeaderValue::from_static("github-dispatch-delivery-1"),
        );
        headers.insert(
            "x-hub-signature-256",
            HeaderValue::from_str(&sign_webhook("github-fixture-secret", body)).expect("signature"),
        );

        let request = normalize_github_webhook(
            b"github-fixture-secret",
            &headers,
            body,
            "demo".into(),
            Some("github".into()),
        )
        .expect("normalize")
        .expect("dispatch request");
        assert_eq!(request.event_id, "github-dispatch-delivery-1");
        assert_eq!(
            request.revision.as_deref(),
            Some("c783c3523482029c449dcdff1209ed06409b83bc")
        );
        assert_eq!(request.parameters.get("ENV"), Some(&"staging".to_owned()));
        assert!(request.fetch);
        assert_eq!(request.credential_id.as_deref(), Some("github"));

        let invalid = br#"{"client_payload":{"rivet_parameters":{"ENV":"staging"}}}"#;
        let mut invalid_headers = headers;
        invalid_headers.insert(
            "x-hub-signature-256",
            HeaderValue::from_str(&sign_webhook("github-fixture-secret", invalid))
                .expect("signature"),
        );
        assert!(matches!(
            normalize_github_webhook(
                b"github-fixture-secret",
                &invalid_headers,
                invalid,
                "demo".into(),
                None,
            ),
            Err(ApiError::BadRequest(message)) if message.contains("rivet_revision")
        ));
    }

    #[test]
    fn gitlab_signed_push_webhook_checks_timestamp_and_normalizes_event_id() {
        let signing_key = b"gitlab-signing-fixture";
        let signing_token = format!("whsec_{}", STANDARD.encode(signing_key));
        let message_id = "gitlab-delivery-1";
        let timestamp = Utc::now().timestamp();
        let body = br#"{"after":"c783c3523482029c449dcdff1209ed06409b83bc"}"#;
        let mut headers = HeaderMap::new();
        headers.insert("x-gitlab-event", HeaderValue::from_static("Push Hook"));
        headers.insert("webhook-id", HeaderValue::from_static(message_id));
        headers.insert(
            "webhook-timestamp",
            HeaderValue::from_str(&timestamp.to_string()).expect("timestamp"),
        );
        headers.insert(
            "webhook-signature",
            HeaderValue::from_str(&sign_gitlab_webhook(
                signing_key,
                message_id,
                timestamp,
                body,
            ))
            .expect("signature"),
        );

        let request = normalize_gitlab_webhook(
            signing_token.as_bytes(),
            &headers,
            body,
            "demo".into(),
            None,
        )
        .expect("normalize")
        .expect("push request");
        assert_eq!(request.event_id, message_id);
        assert_eq!(
            request.revision.as_deref(),
            Some("c783c3523482029c449dcdff1209ed06409b83bc")
        );
        assert!(request.fetch);
    }

    #[test]
    fn gitlab_merge_request_webhook_fetches_the_provider_ref() {
        let signing_key = b"gitlab-signing-fixture";
        let signing_token = format!("whsec_{}", STANDARD.encode(signing_key));
        let message_id = "gitlab-mr-delivery-1";
        let timestamp = Utc::now().timestamp();
        let body = br#"{"object_attributes":{"action":"update","iid":7,"last_commit":{"id":"c783c3523482029c449dcdff1209ed06409b83bc"}}}"#;
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-gitlab-event",
            HeaderValue::from_static("Merge Request Hook"),
        );
        headers.insert("webhook-id", HeaderValue::from_static(message_id));
        headers.insert(
            "webhook-timestamp",
            HeaderValue::from_str(&timestamp.to_string()).expect("timestamp"),
        );
        headers.insert(
            "webhook-signature",
            HeaderValue::from_str(&sign_gitlab_webhook(
                signing_key,
                message_id,
                timestamp,
                body,
            ))
            .expect("signature"),
        );

        let request = normalize_gitlab_webhook(
            signing_token.as_bytes(),
            &headers,
            body,
            "demo".into(),
            None,
        )
        .expect("normalize")
        .expect("merge request request");
        assert_eq!(request.event_id, message_id);
        assert_eq!(
            request.fetch_ref.as_deref(),
            Some("+refs/merge-requests/7/head:refs/remotes/origin/merge-requests/7")
        );
        assert!(request.fetch);
    }

    #[test]
    fn gitlab_legacy_token_webhook_remains_supported_as_a_fallback() {
        let body = br#"{"after":"c783c3523482029c449dcdff1209ed06409b83bc"}"#;
        let mut headers = HeaderMap::new();
        headers.insert("x-gitlab-event", HeaderValue::from_static("Push Hook"));
        headers.insert(
            "x-gitlab-event-uuid",
            HeaderValue::from_static("gitlab-legacy-delivery-1"),
        );
        headers.insert(
            "x-gitlab-token",
            HeaderValue::from_static("gitlab-legacy-secret"),
        );

        let request =
            normalize_gitlab_webhook(b"gitlab-legacy-secret", &headers, body, "demo".into(), None)
                .expect("normalize")
                .expect("push request");
        assert_eq!(request.event_id, "gitlab-legacy-delivery-1");
        assert_eq!(request.project, "demo");
    }

    #[test]
    fn bitbucket_signed_push_webhook_normalizes_the_updated_revision() {
        let body = br#"{"push":{"changes":[{"new":{"name":"main","target":{"hash":"c783c3523482029c449dcdff1209ed06409b83bc"}}}]}}"#;
        let mut headers = HeaderMap::new();
        headers.insert("x-event-key", HeaderValue::from_static("repo:push"));
        headers.insert(
            "x-request-uuid",
            HeaderValue::from_static("bitbucket-request-1"),
        );
        headers.insert(
            "x-hub-signature",
            HeaderValue::from_str(&sign_webhook("bitbucket-fixture-secret", body))
                .expect("signature"),
        );

        let request = normalize_bitbucket_webhook(
            b"bitbucket-fixture-secret",
            &headers,
            body,
            "demo".into(),
            Some("bitbucket".into()),
        )
        .expect("normalize")
        .expect("push request");
        assert_eq!(request.event_id, "bitbucket-request-1");
        assert_eq!(request.project, "demo");
        assert_eq!(
            request.revision.as_deref(),
            Some("c783c3523482029c449dcdff1209ed06409b83bc")
        );
        assert!(request.fetch);
        assert_eq!(request.credential_id.as_deref(), Some("bitbucket"));
    }

    #[test]
    fn bitbucket_pullrequest_webhook_fetches_the_provider_ref() {
        let body = br#"{"pullrequest":{"id":17,"source":{"commit":{"hash":"c783c3523482029c449dcdff1209ed06409b83bc"}}}}"#;
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-event-key",
            HeaderValue::from_static("pullrequest:updated"),
        );
        headers.insert(
            "x-request-uuid",
            HeaderValue::from_static("bitbucket-request-2"),
        );
        headers.insert(
            "x-hub-signature",
            HeaderValue::from_str(&sign_webhook("bitbucket-fixture-secret", body))
                .expect("signature"),
        );

        let request = normalize_bitbucket_webhook(
            b"bitbucket-fixture-secret",
            &headers,
            body,
            "demo".into(),
            None,
        )
        .expect("normalize")
        .expect("pullrequest request");
        assert_eq!(request.event_id, "bitbucket-request-2");
        assert_eq!(
            request.fetch_ref.as_deref(),
            Some("+refs/pull-requests/17/from:refs/remotes/origin/pull-requests/17")
        );
        assert!(request.fetch);
    }

    #[test]
    fn bitbucket_signed_lifecycle_events_are_acknowledged_without_builds() {
        let ignored_events = [
            "repo:fork",
            "repo:updated",
            "repo:transfer",
            "repo:commit_comment_created",
            "repo:commit_status_created",
            "repo:commit_status_updated",
            "repo:deleted",
            "pullrequest:changes_request_created",
            "pullrequest:changes_request_removed",
            "pullrequest:approved",
            "pullrequest:unapproved",
            "pullrequest:fulfilled",
            "pullrequest:rejected",
            "pullrequest:comment_created",
            "pullrequest:comment_updated",
            "pullrequest:comment_deleted",
            "pullrequest:comment_resolved",
            "pullrequest:comment_reopened",
            "pipeline:span_created",
        ];

        for (index, event) in ignored_events.into_iter().enumerate() {
            let body = br#"{}"#;
            let mut headers = HeaderMap::new();
            headers.insert(
                "x-event-key",
                HeaderValue::from_str(event).expect("event header"),
            );
            headers.insert(
                "x-request-uuid",
                HeaderValue::from_str(&format!("bitbucket-ignored-{index}"))
                    .expect("request header"),
            );
            headers.insert(
                "x-hub-signature",
                HeaderValue::from_str(&sign_webhook("bitbucket-fixture-secret", body))
                    .expect("signature"),
            );

            assert!(
                normalize_bitbucket_webhook(
                    b"bitbucket-fixture-secret",
                    &headers,
                    body,
                    "demo".into(),
                    None,
                )
                .expect("lifecycle event should be accepted")
                .is_none(),
                "event {event} should not queue a build"
            );
        }
    }

    #[test]
    fn bitbucket_deleted_pushes_are_ignored_and_multiple_refs_are_rejected() {
        let deleted = br#"{"push":{"changes":[{"old":{"name":"gone"},"new":null}]}}"#;
        let mut deleted_headers = HeaderMap::new();
        deleted_headers.insert("x-event-key", HeaderValue::from_static("repo:push"));
        deleted_headers.insert(
            "x-request-uuid",
            HeaderValue::from_static("bitbucket-delete-1"),
        );
        deleted_headers.insert(
            "x-hub-signature",
            HeaderValue::from_str(&sign_webhook("bitbucket-fixture-secret", deleted))
                .expect("signature"),
        );
        assert!(
            normalize_bitbucket_webhook(
                b"bitbucket-fixture-secret",
                &deleted_headers,
                deleted,
                "demo".into(),
                None,
            )
            .expect("deleted normalize")
            .is_none()
        );

        let multiple = br#"{"push":{"changes":[{"new":{"target":{"hash":"c783c3523482029c449dcdff1209ed06409b83bc"}}},{"new":{"target":{"hash":"d783c3523482029c449dcdff1209ed06409b83bc"}}}]}}"#;
        let mut multiple_headers = deleted_headers;
        multiple_headers.insert(
            "x-hub-signature",
            HeaderValue::from_str(&sign_webhook("bitbucket-fixture-secret", multiple))
                .expect("signature"),
        );
        assert!(matches!(
            normalize_bitbucket_webhook(
                b"bitbucket-fixture-secret",
                &multiple_headers,
                multiple,
                "demo".into(),
                None,
            ),
            Err(ApiError::BadRequest(message)) if message.contains("multiple updated refs")
        ));
    }

    #[test]
    fn provider_webhooks_reject_bad_signatures_and_stale_gitlab_messages() {
        let body = br#"{"after":"c783c3523482029c449dcdff1209ed06409b83bc"}"#;
        let mut github_headers = HeaderMap::new();
        github_headers.insert("x-github-event", HeaderValue::from_static("push"));
        github_headers.insert("x-github-delivery", HeaderValue::from_static("delivery"));
        github_headers.insert("x-hub-signature-256", HeaderValue::from_static("sha256=00"));
        assert!(matches!(
            normalize_github_webhook(
                b"github-fixture-secret",
                &github_headers,
                body,
                "demo".into(),
                None,
            ),
            Err(ApiError::InvalidWebhookSignature)
        ));

        let signing_key = b"gitlab-signing-fixture";
        let message_id = "gitlab-delivery-stale";
        let timestamp = Utc::now().timestamp() - 301;
        let mut gitlab_headers = HeaderMap::new();
        gitlab_headers.insert("x-gitlab-event", HeaderValue::from_static("Push Hook"));
        gitlab_headers.insert("webhook-id", HeaderValue::from_static(message_id));
        gitlab_headers.insert(
            "webhook-timestamp",
            HeaderValue::from_str(&timestamp.to_string()).expect("timestamp"),
        );
        gitlab_headers.insert(
            "webhook-signature",
            HeaderValue::from_str(&sign_gitlab_webhook(
                signing_key,
                message_id,
                timestamp,
                body,
            ))
            .expect("signature"),
        );
        let signing_token = format!("whsec_{}", STANDARD.encode(signing_key));
        assert!(matches!(
            normalize_gitlab_webhook(
                signing_token.as_bytes(),
                &gitlab_headers,
                body,
                "demo".into(),
                None,
            ),
            Err(ApiError::InvalidWebhookSignature)
        ));
    }

    #[tokio::test]
    async fn schedule_api_validates_lists_toggles_and_deletes() {
        let storage = Storage::open_in_memory().expect("storage");
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
        storage
            .create_project(&project, &pipeline)
            .expect("project");
        let state = AppState::new(storage);

        let invalid = router(state.clone())
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/projects/demo/schedules")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"name":"bad","expression":"61 * * * *"}"#))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(invalid.status(), StatusCode::BAD_REQUEST);

        let response = router(state.clone())
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/projects/demo/schedules")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"name":"every-five","expression":"*/5 * * * *","trigger":"repository_poll","poll":{"remote":"upstream","fetch":true,"credential_id":"scm-prod"}}"#,
                    ))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::CREATED);
        let body = to_bytes(response.into_body(), 8192).await.expect("body");
        let schedule: ScheduleRecord = serde_json::from_slice(&body).expect("schedule");
        assert_eq!(schedule.trigger, ScheduleTrigger::RepositoryPoll);
        assert_eq!(
            schedule.poll,
            Some(SchedulePollConfig {
                remote: "upstream".into(),
                fetch: true,
                credential_id: Some("scm-prod".into()),
            })
        );

        let response = router(state.clone())
            .oneshot(
                Request::builder()
                    .uri("/api/v1/projects/demo/schedules")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), 8192).await.expect("body");
        let schedules: Vec<ScheduleRecord> = serde_json::from_slice(&body).expect("schedules");
        assert_eq!(schedules, vec![schedule.clone()]);

        let invalid_poll = router(state.clone())
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/projects/demo/schedules")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"name":"invalid-poll","expression":"*/5 * * * *","trigger":"repository_poll","poll":{"credential_id":"scm-prod"}}"#,
                    ))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(invalid_poll.status(), StatusCode::BAD_REQUEST);

        let response = router(state.clone())
            .oneshot(
                Request::builder()
                    .method("PATCH")
                    .uri(format!("/api/v1/projects/demo/schedules/{}", schedule.id))
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"enabled":false}"#))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), 8192).await.expect("body");
        let disabled: ScheduleRecord = serde_json::from_slice(&body).expect("disabled schedule");
        assert!(!disabled.enabled);

        let response = router(state)
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri(format!("/api/v1/projects/demo/schedules/{}", schedule.id))
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
    }

    #[tokio::test]
    async fn dispatcher_claims_due_schedule_and_queues_one_build() {
        let directory = tempdir().expect("tempdir");
        let repository = directory.path().join("repository");
        fs::create_dir_all(&repository).expect("repository");
        let pipeline_path = repository.join("Rivetfile.toml");
        fs::write(
            &pipeline_path,
            r#"
version = 1
name = "scheduled"
[[stages]]
name = "Test"
[[stages.steps]]
name = "unit"
program = "true"
"#,
        )
        .expect("pipeline file");
        let pipeline = Pipeline::load(&pipeline_path).expect("pipeline");
        let project = Project::new(
            "scheduled",
            repository.to_string_lossy().into_owned(),
            pipeline_path.to_string_lossy().into_owned(),
        )
        .expect("project");
        let storage = Storage::open_in_memory().expect("storage");
        storage
            .create_project(&project, &pipeline)
            .expect("project");
        let due_at = Utc
            .with_ymd_and_hms(2026, 9, 13, 12, 0, 0)
            .single()
            .expect("due timestamp");
        let schedule = storage
            .create_schedule(project.id, "every-minute", "* * * * *", true, due_at)
            .expect("schedule");
        let state = AppState::new(storage.clone());

        assert_eq!(
            dispatch_due_schedules(&state, due_at)
                .await
                .expect("dispatch"),
            1
        );
        assert_eq!(
            dispatch_due_schedules(&state, due_at)
                .await
                .expect("duplicate dispatch"),
            0
        );
        for _ in 0..50 {
            if storage
                .list_builds(project.id)
                .expect("builds")
                .first()
                .is_some_and(|build| build.status.is_terminal())
            {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
        let builds = storage.list_builds(project.id).expect("builds");
        assert_eq!(builds.len(), 1);
        assert_eq!(builds[0].status, BuildStatus::Passed);
        let persisted_schedule = storage
            .get_schedule(project.id, schedule.id)
            .expect("schedule")
            .expect("schedule exists");
        assert_eq!(persisted_schedule.last_run_at, Some(due_at));
        assert!(persisted_schedule.next_run_at > due_at);
    }

    #[tokio::test]
    async fn repository_poll_schedule_skips_an_unchanged_revision() {
        let directory = tempdir().expect("tempdir");
        let repository = directory.path().join("repository");
        fs::create_dir_all(&repository).expect("repository");
        let pipeline_path = repository.join("Rivetfile.toml");
        fs::write(
            &pipeline_path,
            r#"
version = 1
name = "scheduled-poll"
[[stages]]
name = "Test"
[[stages.steps]]
name = "unit"
program = "true"
"#,
        )
        .expect("pipeline file");
        let pipeline = Pipeline::load(&pipeline_path).expect("pipeline");
        let project = Project::new(
            "scheduled-poll",
            repository.to_string_lossy().into_owned(),
            pipeline_path.to_string_lossy().into_owned(),
        )
        .expect("project");
        let git = |args: &[&str]| {
            let output = std::process::Command::new("git")
                .args(args)
                .current_dir(&repository)
                .output()
                .expect("git available");
            assert!(
                output.status.success(),
                "git {:?}: {}",
                args,
                String::from_utf8_lossy(&output.stderr)
            );
        };
        git(&["init", "-q"]);
        git(&["config", "user.email", "rivet@example.test"]);
        git(&["config", "user.name", "Rivet Tests"]);
        git(&["add", "Rivetfile.toml"]);
        git(&["commit", "-qm", "scheduled poll fixture"]);

        let storage = Storage::open_in_memory().expect("storage");
        storage
            .create_project(&project, &pipeline)
            .expect("project");
        let due_at = Utc
            .with_ymd_and_hms(2026, 9, 13, 12, 0, 0)
            .single()
            .expect("due timestamp");
        let schedule = storage
            .create_schedule_with_trigger(
                project.id,
                "poll-every-minute",
                "* * * * *",
                ScheduleTrigger::RepositoryPoll,
                true,
                due_at,
            )
            .expect("schedule");
        let state = AppState::new(storage.clone());

        assert_eq!(
            dispatch_due_schedules(&state, due_at)
                .await
                .expect("first dispatch"),
            1
        );
        assert_eq!(
            storage.list_builds(project.id).expect("first build").len(),
            1
        );
        let first_schedule = storage
            .get_schedule(project.id, schedule.id)
            .expect("first schedule")
            .expect("schedule exists");
        assert!(
            storage
                .claim_schedule(schedule.id, first_schedule.next_run_at, due_at, due_at)
                .expect("rearm unchanged poll")
        );
        assert_eq!(
            dispatch_due_schedules(&state, due_at)
                .await
                .expect("unchanged dispatch"),
            0
        );
        assert_eq!(
            storage
                .list_builds(project.id)
                .expect("unchanged builds")
                .len(),
            1
        );

        fs::write(repository.join("change.txt"), "revision two\n").expect("change");
        git(&["add", "change.txt"]);
        git(&["commit", "-qm", "second scheduled poll fixture"]);
        let armed_schedule = storage
            .get_schedule(project.id, schedule.id)
            .expect("armed schedule")
            .expect("schedule exists");
        assert!(
            storage
                .claim_schedule(schedule.id, armed_schedule.next_run_at, due_at, due_at)
                .expect("rearm changed poll")
        );
        assert_eq!(
            dispatch_due_schedules(&state, due_at)
                .await
                .expect("changed dispatch"),
            1
        );
        assert_eq!(
            storage
                .list_builds(project.id)
                .expect("changed builds")
                .len(),
            2
        );
    }
}
