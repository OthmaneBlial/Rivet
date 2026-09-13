//! Headless Rivet HTTP/WebSocket service.
//!
//! The service is deliberately a thin transport layer. Pipeline validation,
//! queueing, process supervision, and state projection remain in the shared
//! crates so desktop and server modes execute the same code.

use axum::body::{Body, Bytes};
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{DefaultBodyLimit, Extension, Path as AxumPath, State};
use axum::http::{HeaderMap, HeaderName, HeaderValue, Method, StatusCode, header};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, patch, post, put};
use axum::{Json, Router};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use chrono::{DateTime, Utc};
use hmac::{Hmac, Mac};
use rivet_agent_protocol::{
    AgentMessage, AgentRequirements, MAX_WORKSPACE_CHUNK_BYTES, PROTOCOL_VERSION, WorkspaceTransfer,
};
use rivet_auth::{AuthError, AuthPolicy, Permission, Principal};
use rivet_core::{
    BuildEvent, BuildId, BuildStatus, CronExpression, ExecutionPlan, Pipeline, Project, ScheduleId,
    SourceSnapshot,
};
use rivet_credentials::{CredentialError, CredentialKeychain, CredentialSummary, CredentialVault};
use rivet_extension_protocol::{
    ExtensionCatalog, ExtensionCatalogError, ExtensionKind, ExtensionManager,
    ExtensionManagerError, ExtensionManifest,
};
use rivet_runner::{MAX_QUEUE_PRIORITY, MIN_QUEUE_PRIORITY, QueueHandle, QueueStats, Scheduler};
use rivet_scm::{GitHttpCredential, GitPrepareOptions, GitRepository, GitSnapshot, ScmError};
use rivet_storage::{
    ArtifactRecord, AuditEventRecord, BuildDetails, BuildRecord, LogRecord, ScheduleRecord,
    Storage, StorageError,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use subtle::ConstantTimeEq;
use thiserror::Error;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio::sync::{Mutex, broadcast, mpsc};
use tokio::time::{Duration, MissedTickBehavior};
use tokio_util::sync::CancellationToken;
use tower_http::cors::{AllowOrigin, CorsLayer};

mod agent_registry;
mod workspace_archive;

use agent_registry::{
    AgentLease, AgentRegistry, AgentRegistryError, AgentReservation, AgentSummary,
};
use workspace_archive::archive_workspace;

const DEFAULT_ALLOWED_ORIGINS: [&str; 5] = [
    "http://127.0.0.1:1420",
    "http://localhost:1420",
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
    auth_digest: Option<[u8; 32]>,
    auth_policy: Option<Arc<AuthPolicy>>,
    webhook_secret: Option<Vec<u8>>,
    github_webhook_secret: Option<Vec<u8>>,
    gitlab_webhook_secret: Option<Vec<u8>>,
    github_webhook_credential_id: Option<String>,
    gitlab_webhook_credential_id: Option<String>,
    credentials: Option<Arc<Mutex<CredentialVault>>>,
    extensions: Arc<ExtensionCatalog>,
    extension_manager: Option<Arc<ExtensionManager>>,
    agents: AgentRegistry,
    remote_messages: Arc<Mutex<HashMap<BuildId, RemoteBuildRoute>>>,
}

#[derive(Clone)]
struct RemoteBuildRoute {
    agent_id: rivet_agent_protocol::AgentId,
    messages: mpsc::Sender<AgentMessage>,
}

#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub bind: SocketAddr,
    pub auth_token: Option<String>,
    pub auth_policy_file: Option<PathBuf>,
    pub webhook_secret: Option<String>,
    pub github_webhook_secret: Option<String>,
    pub gitlab_webhook_secret: Option<String>,
    pub github_webhook_credential_id: Option<String>,
    pub gitlab_webhook_credential_id: Option<String>,
    pub credentials_file: Option<PathBuf>,
    pub credentials_passphrase: Option<String>,
    pub credentials_keychain_account: Option<String>,
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
    #[error("Rivet webhook secret cannot be empty")]
    EmptyWebhookSecret,
    #[error("{provider} webhook secret cannot be empty")]
    EmptyProviderWebhookSecret { provider: &'static str },
    #[error("{provider} webhook credential ID cannot be empty")]
    EmptyProviderWebhookCredential { provider: &'static str },
    #[error("credential vault configuration requires both a file and a passphrase")]
    IncompleteCredentialVaultConfig,
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
    #[error("credential vault is not configured")]
    CredentialsUnavailable,
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
            | Self::GitLabWebhookNotConfigured => StatusCode::SERVICE_UNAVAILABLE,
            Self::InvalidWebhookSignature => StatusCode::UNAUTHORIZED,
            Self::EmptyWebhookEventId
            | Self::WebhookEventIdTooLong
            | Self::InvalidWebhookEventId => StatusCode::BAD_REQUEST,
            Self::WebhookEventConflict => StatusCode::CONFLICT,
            Self::ArtifactNotFound(_) => StatusCode::NOT_FOUND,
            Self::ArtifactRead(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Self::CredentialsUnavailable => StatusCode::SERVICE_UNAVAILABLE,
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
                | rivet_scm::ScmError::InvalidCredential(_) => StatusCode::BAD_REQUEST,
                rivet_scm::ScmError::Command { .. }
                | rivet_scm::ScmError::InvalidOutput { .. }
                | rivet_scm::ScmError::Filesystem(_) => StatusCode::INTERNAL_SERVER_ERROR,
            },
            Self::Migration(_)
            | Self::Storage(_)
            | Self::Pipeline(_)
            | Self::Model(_)
            | Self::Scheduler(_) => StatusCode::INTERNAL_SERVER_ERROR,
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
struct AuthMeResponse {
    id: String,
    role: rivet_auth::Role,
    projects: Vec<String>,
    local_mode: bool,
}

#[derive(Debug, Deserialize)]
pub struct CreateProjectRequest {
    pub name: String,
    pub repository_path: PathBuf,
    pub pipeline_path: Option<PathBuf>,
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

#[derive(Debug, Deserialize)]
pub struct CreateScheduleRequest {
    pub name: String,
    pub expression: String,
    #[serde(default = "default_schedule_enabled")]
    pub enabled: bool,
}

#[derive(Debug, Deserialize)]
pub struct UpdateScheduleRequest {
    pub enabled: bool,
}

#[derive(Deserialize)]
struct CredentialWriteRequest {
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
    #[serde(default)]
    pub parameters: BTreeMap<String, String>,
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
struct QueueStatusResponse {
    queued: usize,
    running: usize,
    capacity: usize,
    paused: bool,
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
    secret: bool,
    default: Option<String>,
    required: bool,
}

#[derive(Debug, Serialize)]
struct ExtensionRuntimeStatusResponse {
    id: String,
    active: bool,
    runtime_available: bool,
}

pub fn router(state: AppState) -> Router {
    router_with_origins(state, &default_allowed_origins())
        .expect("default Rivet origins must be valid")
}

fn router_with_origins(state: AppState, allowed_origins: &[String]) -> Result<Router, ServerError> {
    let cors = cors_layer(allowed_origins)?;
    Ok(Router::new()
        .route("/api/v1/health", get(health))
        .route("/api/v1/auth/me", get(auth_me))
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
        .route("/api/v1/agents", get(list_agents))
        .route("/api/v1/agents/match", post(match_agents))
        .route("/api/v1/agents/connect", get(connect_agent))
        .route("/api/v1/migration/jenkinsfile", post(analyze_jenkinsfile))
        .route("/api/v1/webhooks/generic", post(webhook_build))
        .route("/api/v1/webhooks/github/{project}", post(github_webhook))
        .route("/api/v1/webhooks/gitlab/{project}", post(gitlab_webhook))
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
            .prepare_with_credential(
                &GitPrepareOptions {
                    remote: request.remote,
                    fetch: request.fetch,
                    revision: request.revision,
                    fetch_ref: request.fetch_ref,
                    clean: request.clean,
                    clean_ignored: request.clean_ignored,
                    credential_id: request.credential_id,
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
            webhook_secret: None,
            github_webhook_secret: None,
            gitlab_webhook_secret: None,
            github_webhook_credential_id: None,
            gitlab_webhook_credential_id: None,
            credentials_file: None,
            credentials_passphrase: None,
            credentials_keychain_account: None,
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
    let credentials = match (
        config.credentials_file.as_ref(),
        config.credentials_passphrase.as_deref(),
        config.credentials_keychain_account.as_deref(),
    ) {
        (Some(path), Some(passphrase), None) => Some(Arc::new(Mutex::new(CredentialVault::open(
            path, passphrase,
        )?))),
        (Some(path), None, Some(account)) => {
            let passphrase = CredentialKeychain::rivet().get_passphrase(account)?;
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
    let recovered_builds = storage.recover_incomplete_builds(Utc::now())?;
    for build_id in &recovered_builds {
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
    state.auth_policy = auth_policy;
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
    state.github_webhook_credential_id = config.github_webhook_credential_id;
    state.gitlab_webhook_credential_id = config.gitlab_webhook_credential_id;
    let allowed_origins = if config.allowed_origins.is_empty() {
        default_allowed_origins()
    } else {
        config.allowed_origins
    };
    tracing::info!(bind = %bind, "Rivet server listening");
    let shutdown = CancellationToken::new();
    spawn_schedule_dispatcher(state.clone(), shutdown.clone());
    let result = axum::serve(listener, router_with_origins(state, &allowed_origins)?)
        .with_graceful_shutdown(wait_for_shutdown_signal())
        .await;
    shutdown.cancel();
    result?;
    Ok(())
}

fn validate_config(config: &ServerConfig) -> Result<(), ServerError> {
    if !config.bind.ip().is_loopback()
        && config.auth_token.is_none()
        && config.auth_policy_file.is_none()
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
    match (
        config.credentials_file.is_some(),
        config.credentials_passphrase.is_some(),
        config.credentials_keychain_account.is_some(),
    ) {
        (false, false, false) | (true, true, false) | (true, false, true) => {}
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

impl AppState {
    pub fn new(storage: Storage) -> Self {
        let (events, _) = broadcast::channel(1024);
        let cache_root = storage.cache_root();
        Self {
            storage,
            scheduler: Arc::new(Scheduler::new_with_cache(2, Some(1), Some(cache_root))),
            active_builds: Arc::new(Mutex::new(HashMap::new())),
            events,
            auth_digest: None,
            auth_policy: None,
            webhook_secret: None,
            github_webhook_secret: None,
            gitlab_webhook_secret: None,
            github_webhook_credential_id: None,
            gitlab_webhook_credential_id: None,
            credentials: None,
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
    vault.set_http_basic_for_projects(
        id.clone(),
        request.username,
        request.secret,
        request.projects,
    )?;
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
    let auth_enabled = state.auth_digest.is_some() || state.auth_policy.is_some();
    if !auth_enabled {
        request.extensions_mut().insert(Principal::local_admin());
        return next.run(request).await;
    }
    if request.uri().path() == "/api/v1/health" {
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
                secret: parameter.secret,
                default: parameter.default,
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
                runtime_available: state.extension_manager.is_some()
                    && matches!(&manifest.kind, ExtensionKind::Subprocess),
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

async fn extension_status_for(
    state: &AppState,
    id: &str,
) -> Result<Json<ExtensionRuntimeStatusResponse>, ApiError> {
    let manifest = state
        .extensions
        .manifests()
        .iter()
        .find(|manifest| manifest.id == id)
        .ok_or_else(|| {
            ApiError::ExtensionManager(ExtensionManagerError::UnknownExtension(id.into()))
        })?;
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
        runtime_available: state.extension_manager.is_some()
            && matches!(&manifest.kind, ExtensionKind::Subprocess),
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
    let registration = match decode_agent_message(first_message) {
        Ok(AgentMessage::Register(registration)) => registration,
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
    let registered = AgentMessage::Registered {
        protocol_version: PROTOCOL_VERSION,
        agent_id: lease.agent_id,
        session_id: lease.session_id,
    };
    if !send_agent_message(&mut socket, registered).await {
        state
            .agents
            .unregister(lease.agent_id, lease.session_id)
            .await;
        return;
    }

    loop {
        tokio::select! {
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
                    message => match decode_agent_message(message) {
                        Ok(message) => match dispatch_agent_message(&state, &lease, message).await {
                            Ok(Some(response)) => {
                                if !send_agent_message(&mut socket, response).await {
                                    break;
                                }
                            }
                            Ok(None) => {}
                            Err((code, message)) => {
                                let _ = send_agent_error(&mut socket, &code, &message).await;
                                break;
                            }
                        },
                        Err(error) => {
                            let _ = send_agent_error(&mut socket, "invalid_message", &error).await;
                            break;
                        }
                    },
                }
            }
            outgoing = outbound_rx.recv() => {
                let Some(message) = outgoing else { break; };
                if !send_agent_message(&mut socket, message).await {
                    break;
                }
            }
        }
    }
    notify_agent_disconnect(&state, lease.agent_id).await;
    state
        .agents
        .unregister(lease.agent_id, lease.session_id)
        .await;
}

async fn notify_agent_disconnect(state: &AppState, agent_id: rivet_agent_protocol::AgentId) {
    let routes = state
        .remote_messages
        .lock()
        .await
        .iter()
        .filter(|(_, route)| route.agent_id == agent_id)
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
        AgentMessage::Event { event, .. } => {
            route_agent_build_message(
                state,
                lease.agent_id,
                build_id_from_event(&event),
                AgentMessage::Event {
                    protocol_version: PROTOCOL_VERSION,
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
            route_agent_build_message(state, lease.agent_id, build_id, message).await?;
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
    if route.agent_id != agent_id {
        return Err((
            "agent_identity_mismatch".into(),
            format!("agent is not assigned to build {build_id}"),
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

fn decode_agent_message(message: Message) -> Result<AgentMessage, String> {
    let payload = match message {
        Message::Text(text) => text.to_string(),
        Message::Binary(bytes) => String::from_utf8(bytes.to_vec())
            .map_err(|_| "agent messages must contain valid UTF-8 JSON".to_owned())?,
        Message::Ping(_) | Message::Pong(_) => {
            return Err("control frame is not an agent message".into());
        }
        Message::Close(_) => return Err("agent websocket closed".into()),
    };
    let message: AgentMessage = serde_json::from_str(&payload)
        .map_err(|error| format!("invalid agent message JSON: {error}"))?;
    message
        .validate()
        .map_err(|error| format!("invalid agent message: {error}"))?;
    Ok(message)
}

async fn send_agent_message(socket: &mut WebSocket, message: AgentMessage) -> bool {
    let Ok(payload) = serde_json::to_string(&message) else {
        return false;
    };
    socket.send(Message::Text(payload.into())).await.is_ok()
}

async fn send_agent_error(socket: &mut WebSocket, code: &str, message: &str) -> bool {
    send_agent_message(
        socket,
        AgentMessage::Error {
            protocol_version: PROTOCOL_VERSION,
            build_id: None,
            code: code.to_owned(),
            message: message.to_owned(),
        },
    )
    .await
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
    let schedule = state.storage.create_schedule(
        project.id,
        schedule_name,
        expression.expression(),
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

async fn enqueue_webhook_build(
    state: &AppState,
    principal: &Principal,
    request: WebhookBuildRequest,
) -> Result<(StatusCode, Json<WebhookBuildResponse>), ApiError> {
    let event_id = validate_webhook_event_id(request.event_id)?;
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
                parameters: BTreeMap::new(),
            }))
        }
        "pull_request" => {
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
                parameters: BTreeMap::new(),
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
                parameters: BTreeMap::new(),
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
                parameters: BTreeMap::new(),
            }))
        }
        _ => Err(ApiError::BadRequest(format!(
            "unsupported GitLab webhook event: {event}"
        ))),
    }
}

fn github_pull_request_refspec(number: u64) -> Result<String, ApiError> {
    provider_pull_request_refspec("refs/pull", number)
}

fn gitlab_merge_request_refspec(iid: u64) -> Result<String, ApiError> {
    provider_pull_request_refspec("refs/merge-requests", iid)
}

fn provider_pull_request_refspec(prefix: &str, number: u64) -> Result<String, ApiError> {
    if number == 0 || number > 9_999_999_999 {
        return Err(ApiError::BadRequest(
            "pull request number is outside the supported range".into(),
        ));
    }
    let destination = prefix.strip_prefix("refs/").unwrap_or(prefix);
    Ok(format!(
        "+{prefix}/{number}/head:refs/remotes/origin/{destination}/{number}"
    ))
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

async fn wait_for_shutdown_signal() {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .expect("install SIGTERM handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = terminate.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
    tracing::info!("Rivet server shutdown requested");
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
        if let Err(error) =
            enqueue_project_build(state, project, QueueBuildRequest::default()).await
        {
            tracing::error!(schedule = %schedule.id, ?error, "scheduled build could not be queued");
            continue;
        }
        dispatched += 1;
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

async fn create_project(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Json(request): Json<CreateProjectRequest>,
) -> Result<(StatusCode, Json<Project>), ApiError> {
    require_global(&principal, Permission::Administer)?;
    let repository = canonical_directory(&request.repository_path)?;
    let pipeline_path = request
        .pipeline_path
        .unwrap_or_else(|| repository.join("Rivetfile.toml"));
    if !pipeline_path.is_file() {
        return Err(ApiError::InvalidPipeline(pipeline_path));
    }
    let pipeline_path = std::fs::canonicalize(pipeline_path)
        .map_err(|_| ApiError::InvalidPipeline(request.repository_path.clone()))?;
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
) -> Result<Json<Vec<LogRecord>>, ApiError> {
    require_project(&principal, Permission::Read, &name)?;
    let project = project_by_name(&state.storage, &name)?;
    let build = build_by_number(&state.storage, project.id, &name, number)?;
    Ok(Json(state.storage.logs(build.id)?))
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
    let bytes = std::fs::read(path).map_err(ApiError::ArtifactRead)?;
    let filename = artifact
        .relative_path
        .rsplit('/')
        .next()
        .filter(|value| !value.is_empty())
        .unwrap_or("artifact")
        .replace(['"', '\r', '\n'], "_");
    let mut response = Response::new(Body::from(bytes));
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/octet-stream"),
    );
    if let Ok(value) = HeaderValue::from_str(&format!("attachment; filename=\"{filename}\"")) {
        response
            .headers_mut()
            .insert(header::CONTENT_DISPOSITION, value);
    }
    Ok(response)
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
    )
    .await?;
    let pipeline = Pipeline::load(&project.pipeline_path)?;
    let parameters = pipeline.resolve_parameters(&request.parameters)?;
    let plan = ExecutionPlan::from_pipeline(&pipeline, uuid::Uuid::new_v4(), project.id);
    let remote_requirements = remote_agent_requirements(&pipeline)?;
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
    let cancellation = CancellationToken::new();
    state
        .active_builds
        .lock()
        .await
        .insert(build.id, cancellation.clone());

    let (events, mut received_events) = mpsc::channel(512);
    let event_storage = state.storage.clone();
    let event_bus = state.events.clone();
    let artifact_pipeline = pipeline.clone();
    let artifact_workspace = repository_root.clone();
    let collect_local_artifacts = reservation.is_none();
    tokio::spawn(async move {
        while let Some(event) = received_events.recv().await {
            let event = if collect_local_artifacts {
                finalize_artifacts(
                    &event_storage,
                    &artifact_pipeline,
                    &artifact_workspace,
                    event,
                )
            } else {
                event
            };
            if let Err(error) = event_storage.apply_event(&event) {
                tracing::error!(?error, "could not project Rivet build event");
                continue;
            }
            let _ = event_bus.send(event);
        }
    });

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
        let remote_build_id = plan.build_id;
        let remote_state = state.clone();
        tokio::spawn(async move {
            let result = run_remote_build_with_recovery(
                remote_state.clone(),
                reservation,
                requirements,
                plan,
                pipeline.clone(),
                remote_workspace.expect("remote workspace was resolved"),
                parameters,
                cancellation,
                events.clone(),
            )
            .await;
            if let Err(error) = result {
                tracing::error!(
                    ?error,
                    build_id = %remote_build_id,
                    "remote Rivet build failed before completion"
                );
            }
            remote_state
                .active_builds
                .lock()
                .await
                .remove(&remote_build_id);
        });
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
) -> Result<(), RemoteBuildError> {
    let mut reservation = initial_reservation;
    let mut suppress_build_started = false;
    let mut active_stages = HashSet::new();
    let mut active_steps = HashSet::new();
    let mut build_started = false;
    for attempt in 0..=MAX_REMOTE_RECOVERY_ATTEMPTS {
        let (route_sender, received_messages) = mpsc::channel(1024);
        state.remote_messages.lock().await.insert(
            plan.build_id,
            RemoteBuildRoute {
                agent_id: reservation.agent_id,
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
                    Ok(reservation) => reservation,
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
                    AgentMessage::Event { event, .. } => {
                        validate_remote_event(&event, &plan, accepted, ready)?;
                        if matches!(event, BuildEvent::BuildStarted { .. }) {
                            *build_started = true;
                        }
                        if suppress_build_started
                            && matches!(event, BuildEvent::BuildStarted { .. })
                        {
                            continue;
                        }
                        track_remote_activity(&event, active_stages, active_steps);
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
                        events.send(event).await.map_err(|_| RemoteBuildError::EventChannelClosed)?;
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
                    AgentMessage::Event { event, .. } => {
                        if validate_remote_event(&event, plan, *accepted, *ready).is_err() {
                            continue;
                        }
                        track_remote_activity(&event, active_stages, active_steps);
                        let terminal = matches!(event, BuildEvent::BuildFinished { .. });
                        events.send(event).await.map_err(|_| RemoteBuildError::EventChannelClosed)?;
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
                        .prepare_with_credential(
                            &GitPrepareOptions {
                                remote: request.remote.clone(),
                                fetch: request.fetch,
                                revision: request.revision.clone(),
                                fetch_ref: request.fetch_ref.clone(),
                                clean: request.clean,
                                clean_ignored: request.clean_ignored,
                                credential_id: request.credential_id.clone(),
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
) -> Result<Option<GitHttpCredential>, ApiError> {
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
    GitHttpCredential::new(credential.username(), credential.secret())
        .map(Some)
        .map_err(|error| ApiError::BadRequest(error.to_string()))
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
        AgentCapabilities, AgentHeartbeat, AgentRegistration, AgentRequirements, PROTOCOL_VERSION,
    };
    use rivet_auth::{ApiTokenRecord, AuthPolicyDocument, Role};
    use rivet_extension_protocol::{
        ExtensionKind, ExtensionMessage, ExtensionPermission, encode_message,
    };
    use std::fs;
    use tempfile::tempdir;
    use tokio::time::sleep;
    use tokio_tungstenite::tungstenite::Message as ClientMessage;
    use tower::ServiceExt;

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
            "version = 1\nname = \"parameters\"\n\n[[parameters]]\nname = \"TARGET\"\ndefault = \"release\"\n\n[[parameters]]\nname = \"TOKEN\"\nsecret = true\n\n[[stages]]\nname = \"Test\"\n[[stages.steps]]\nname = \"noop\"\nprogram = \"true\"\n",
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

        let response = router(AppState::new(storage))
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
        assert_eq!(payload[0]["default"], "release");
        assert_eq!(payload[0]["required"], false);
        assert_eq!(payload[1]["name"], "TOKEN");
        assert_eq!(payload[1]["secret"], true);
        assert!(payload[1]["default"].is_null());
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
            br#"[{"id":"coverage.reporter","active":false,"runtime_available":false}]"#
        );
    }

    #[tokio::test]
    async fn wasm_extension_start_is_explicitly_gated() {
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
                    .method(Method::POST)
                    .uri("/api/v1/extensions/coverage.reporter/start")
                    .body(Body::empty())
                    .expect("start request"),
            )
            .await
            .expect("start response");
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = to_bytes(response.into_body(), 16 * 1024)
            .await
            .expect("start body");
        let payload: serde_json::Value = serde_json::from_slice(&body).expect("start JSON");
        assert_eq!(
            payload["error"],
            "WASM extension \"coverage.reporter\" cannot run before a sandboxed runtime is configured"
        );
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
            },
        });
        socket
            .send(ClientMessage::Text(
                serde_json::to_string(&registration)
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
        let AgentMessage::Registered { session_id, .. } =
            serde_json::from_str(registered.as_ref()).expect("registered JSON")
        else {
            panic!("expected registered response");
        };

        let heartbeat = AgentMessage::Heartbeat(AgentHeartbeat {
            protocol_version: PROTOCOL_VERSION,
            agent_id,
            session_id,
            sequence: 1,
            running: vec![],
            sent_at: Utc::now(),
        });
        socket
            .send(ClientMessage::Text(
                serde_json::to_string(&heartbeat)
                    .expect("heartbeat JSON")
                    .into(),
            ))
            .await
            .expect("send heartbeat");
        let acknowledged = socket
            .next()
            .await
            .expect("heartbeat ack")
            .expect("heartbeat frame");
        let ClientMessage::Text(acknowledged) = acknowledged else {
            panic!("expected heartbeat ack text frame");
        };
        let acknowledged: AgentMessage =
            serde_json::from_str(acknowledged.as_ref()).expect("ack JSON");
        assert!(matches!(
            acknowledged,
            AgentMessage::HeartbeatAck { sequence: 1, .. }
        ));
        socket.close(None).await.expect("close");
        server.abort();
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
        let state = AppState::with_webhook_secret(storage.clone(), "webhook-test-secret");
        let body = br#"{"event_id":"delivery-1","project":"webhook-demo"}"#.to_vec();

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
                        r#"{"name":"every-five","expression":"*/5 * * * *"}"#,
                    ))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::CREATED);
        let body = to_bytes(response.into_body(), 8192).await.expect("body");
        let schedule: ScheduleRecord = serde_json::from_slice(&body).expect("schedule");

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
}
