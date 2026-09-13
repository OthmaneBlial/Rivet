//! Headless Rivet HTTP/WebSocket service.
//!
//! The service is deliberately a thin transport layer. Pipeline validation,
//! queueing, process supervision, and state projection remain in the shared
//! crates so desktop and server modes execute the same code.

use axum::body::{Body, Bytes};
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{DefaultBodyLimit, Path as AxumPath, State};
use axum::http::{HeaderMap, HeaderName, HeaderValue, Method, StatusCode, header};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, patch, post};
use axum::{Json, Router};
use chrono::{DateTime, Utc};
use hmac::{Hmac, Mac};
use rivet_agent_protocol::{AgentMessage, PROTOCOL_VERSION};
use rivet_core::{
    BuildEvent, BuildId, BuildStatus, CronExpression, ExecutionPlan, Pipeline, Project, ScheduleId,
    SourceSnapshot,
};
use rivet_runner::{QueueHandle, QueueStats, Scheduler};
use rivet_scm::{GitPrepareOptions, GitRepository, GitSnapshot, ScmError};
use rivet_storage::{
    ArtifactRecord, BuildDetails, BuildRecord, LogRecord, ScheduleRecord, Storage, StorageError,
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
use tokio::net::TcpListener;
use tokio::sync::{Mutex, broadcast, mpsc};
use tokio::time::{Duration, MissedTickBehavior};
use tokio_util::sync::CancellationToken;
use tower_http::cors::{AllowOrigin, CorsLayer};

mod agent_registry;

use agent_registry::{AgentRegistry, AgentSummary};

const DEFAULT_ALLOWED_ORIGINS: [&str; 4] = [
    "http://127.0.0.1:1420",
    "http://localhost:1420",
    "tauri://localhost",
    "https://tauri.localhost",
];

#[derive(Clone)]
pub struct AppState {
    pub storage: Storage,
    scheduler: Arc<Scheduler>,
    active_builds: Arc<Mutex<HashMap<BuildId, CancellationToken>>>,
    events: broadcast::Sender<BuildEvent>,
    auth_digest: Option<[u8; 32]>,
    webhook_secret: Option<Vec<u8>>,
    agents: AgentRegistry,
}

#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub bind: SocketAddr,
    pub auth_token: Option<String>,
    pub webhook_secret: Option<String>,
    pub allowed_origins: Vec<String>,
}

#[derive(Debug, Error)]
pub enum ServerError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error("could not bind Rivet server: {0}")]
    Bind(#[from] std::io::Error),
    #[error("binding Rivet outside loopback requires an authentication token: {0}")]
    AuthRequired(SocketAddr),
    #[error("Rivet authentication token cannot be empty")]
    EmptyAuthToken,
    #[error("Rivet webhook secret cannot be empty")]
    EmptyWebhookSecret,
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
    #[error("{0}")]
    BadRequest(String),
    #[error("webhook delivery signatures are not configured")]
    WebhookNotConfigured,
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
            Self::WebhookNotConfigured => StatusCode::SERVICE_UNAVAILABLE,
            Self::InvalidWebhookSignature => StatusCode::UNAUTHORIZED,
            Self::EmptyWebhookEventId
            | Self::WebhookEventIdTooLong
            | Self::InvalidWebhookEventId => StatusCode::BAD_REQUEST,
            Self::WebhookEventConflict => StatusCode::CONFLICT,
            Self::ArtifactNotFound(_) => StatusCode::NOT_FOUND,
            Self::ArtifactRead(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Self::Scm(error) => match error {
                rivet_scm::ScmError::InvalidRepository(_)
                | rivet_scm::ScmError::NotGitRepository(_) => StatusCode::BAD_REQUEST,
                rivet_scm::ScmError::Command { .. }
                | rivet_scm::ScmError::InvalidOutput { .. }
                | rivet_scm::ScmError::Filesystem(_) => StatusCode::INTERNAL_SERVER_ERROR,
            },
            Self::Storage(_) | Self::Pipeline(_) | Self::Model(_) | Self::Scheduler(_) => {
                StatusCode::INTERNAL_SERVER_ERROR
            }
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

#[derive(Debug, Deserialize)]
pub struct WebhookBuildRequest {
    /// A provider delivery ID. Re-deliveries with the same ID are ignored.
    pub event_id: String,
    /// The Rivet project receiving the build.
    pub project: String,
    #[serde(default)]
    pub revision: Option<String>,
    #[serde(default)]
    pub remote: Option<String>,
    #[serde(default)]
    pub fetch: bool,
    #[serde(default)]
    pub parameters: BTreeMap<String, String>,
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
}

pub fn router(state: AppState) -> Router {
    router_with_origins(state, &default_allowed_origins())
        .expect("default Rivet origins must be valid")
}

fn router_with_origins(state: AppState, allowed_origins: &[String]) -> Result<Router, ServerError> {
    let cors = cors_layer(allowed_origins)?;
    Ok(Router::new()
        .route("/api/v1/health", get(health))
        .route("/api/v1/agents", get(list_agents))
        .route("/api/v1/agents/connect", get(connect_agent))
        .route("/api/v1/webhooks/generic", post(webhook_build))
        .route("/api/v1/queue", get(queue_status))
        .route("/api/v1/projects", get(list_projects).post(create_project))
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
        ]))
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct PrepareScmRequest {
    #[serde(default = "default_remote")]
    pub remote: String,
    #[serde(default)]
    pub fetch: bool,
    pub revision: Option<String>,
    #[serde(default)]
    pub clean: bool,
    #[serde(default)]
    pub clean_ignored: bool,
}

fn default_remote() -> String {
    "origin".to_owned()
}

async fn get_scm(
    State(state): State<AppState>,
    AxumPath(name): AxumPath<String>,
) -> Result<Json<GitSnapshot>, ApiError> {
    let project = project_by_name(&state.storage, &name)?;
    let repository = GitRepository::open(project.repository_path).await?;
    Ok(Json(repository.inspect().await?))
}

async fn prepare_scm(
    State(state): State<AppState>,
    AxumPath(name): AxumPath<String>,
    Json(request): Json<PrepareScmRequest>,
) -> Result<Json<GitSnapshot>, ApiError> {
    let project = project_by_name(&state.storage, &name)?;
    let repository = GitRepository::open(project.repository_path).await?;
    Ok(Json(
        repository
            .prepare(&GitPrepareOptions {
                remote: request.remote,
                fetch: request.fetch,
                revision: request.revision,
                clean: request.clean,
                clean_ignored: request.clean_ignored,
            })
            .await?,
    ))
}

pub async fn serve(storage_path: impl AsRef<Path>, bind: SocketAddr) -> Result<(), ServerError> {
    serve_with_config(
        storage_path,
        ServerConfig {
            bind,
            auth_token: None,
            webhook_secret: None,
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
    let state = AppState::with_security(
        Storage::open(storage_path)?,
        config.auth_token.as_deref(),
        config.webhook_secret.as_deref(),
    );
    let allowed_origins = if config.allowed_origins.is_empty() {
        default_allowed_origins()
    } else {
        config.allowed_origins
    };
    tracing::info!(bind = %bind, "Rivet server listening");
    spawn_schedule_dispatcher(state.clone());
    axum::serve(listener, router_with_origins(state, &allowed_origins)?).await?;
    Ok(())
}

fn validate_config(config: &ServerConfig) -> Result<(), ServerError> {
    if !config.bind.ip().is_loopback() && config.auth_token.is_none() {
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
    Ok(())
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
            webhook_secret: None,
            agents: AgentRegistry::default(),
        }
    }

    #[cfg(test)]
    fn with_token(storage: Storage, token: &str) -> Self {
        Self::with_security(storage, Some(token), None)
    }

    #[cfg(test)]
    fn with_webhook_secret(storage: Storage, secret: &str) -> Self {
        Self::with_security(storage, None, Some(secret))
    }

    fn with_security(storage: Storage, token: Option<&str>, webhook_secret: Option<&str>) -> Self {
        let mut state = Self::new(storage);
        state.auth_digest = token.map(|token| hash_token(token.as_bytes()));
        state.webhook_secret = webhook_secret.map(|secret| secret.as_bytes().to_vec());
        state
    }
}

async fn authenticate(
    State(state): State<AppState>,
    request: axum::http::Request<Body>,
    next: Next,
) -> Response {
    if state.auth_digest.is_none() || request.uri().path() == "/api/v1/health" {
        return next.run(request).await;
    }
    let authorized = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .map(|token| {
            let candidate = hash_token(token.as_bytes());
            bool::from(candidate.ct_eq(state.auth_digest.as_ref().expect("auth digest")))
        })
        .unwrap_or(false);
    if authorized {
        next.run(request).await
    } else {
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

fn hash_token(token: &[u8]) -> [u8; 32] {
    Sha256::digest(token).into()
}

async fn health() -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok",
        service: "rivet-server",
        timestamp: Utc::now(),
    })
}

async fn queue_status(State(state): State<AppState>) -> Json<QueueStatusResponse> {
    let QueueStats {
        queued,
        running,
        capacity,
    } = state.scheduler.stats();
    Json(QueueStatusResponse {
        queued,
        running,
        capacity,
    })
}

async fn list_projects(State(state): State<AppState>) -> Result<Json<Vec<Project>>, ApiError> {
    Ok(Json(state.storage.list_projects()?))
}

async fn list_agents(State(state): State<AppState>) -> Json<Vec<AgentSummary>> {
    Json(state.agents.list(Utc::now()).await)
}

async fn connect_agent(
    State(state): State<AppState>,
    websocket: WebSocketUpgrade,
) -> impl IntoResponse {
    websocket.on_upgrade(move |socket| handle_agent_socket(socket, state.agents.clone()))
}

async fn handle_agent_socket(mut socket: WebSocket, agents: AgentRegistry) {
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
    let lease = match agents.register(registration, Utc::now()).await {
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
        agents.unregister(lease.agent_id, lease.session_id).await;
        return;
    }

    while let Some(result) = socket.recv().await {
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
                Ok(AgentMessage::Heartbeat(heartbeat)) => {
                    if heartbeat.agent_id != lease.agent_id {
                        let _ = send_agent_error(
                            &mut socket,
                            "agent_identity_mismatch",
                            "heartbeat agent_id does not match the registered session",
                        )
                        .await;
                        break;
                    }
                    let sequence = heartbeat.sequence;
                    match agents.heartbeat(heartbeat, Utc::now()).await {
                        Ok(_) => {
                            if !send_agent_message(
                                &mut socket,
                                AgentMessage::HeartbeatAck {
                                    protocol_version: PROTOCOL_VERSION,
                                    sequence,
                                    server_time: Utc::now(),
                                },
                            )
                            .await
                            {
                                break;
                            }
                        }
                        Err(error) => {
                            let _ = send_agent_error(
                                &mut socket,
                                "heartbeat_rejected",
                                &error.to_string(),
                            )
                            .await;
                            break;
                        }
                    }
                }
                Ok(AgentMessage::Register(_)) => {
                    let _ = send_agent_error(
                        &mut socket,
                        "already_registered",
                        "an agent can register only once per connection",
                    )
                    .await;
                    break;
                }
                Ok(_) => {
                    let _ = send_agent_error(
                        &mut socket,
                        "unsupported_message",
                        "this server accepts heartbeat messages after registration",
                    )
                    .await;
                    break;
                }
                Err(error) => {
                    let _ = send_agent_error(&mut socket, "invalid_message", &error).await;
                    break;
                }
            },
        }
    }
    agents.unregister(lease.agent_id, lease.session_id).await;
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
            code: code.to_owned(),
            message: message.to_owned(),
        },
    )
    .await
}

async fn list_schedules(
    State(state): State<AppState>,
    AxumPath(name): AxumPath<String>,
) -> Result<Json<Vec<ScheduleRecord>>, ApiError> {
    let project = project_by_name(&state.storage, &name)?;
    Ok(Json(state.storage.list_schedules(project.id)?))
}

async fn create_schedule(
    State(state): State<AppState>,
    AxumPath(name): AxumPath<String>,
    Json(request): Json<CreateScheduleRequest>,
) -> Result<(StatusCode, Json<ScheduleRecord>), ApiError> {
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
    Json(request): Json<UpdateScheduleRequest>,
) -> Result<Json<ScheduleRecord>, ApiError> {
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
) -> Result<StatusCode, ApiError> {
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
    let event_id = validate_webhook_event_id(request.event_id)?;
    let project_name = request.project.trim();
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

    let scm = if request.fetch || request.revision.is_some() || request.remote.is_some() {
        Some(PrepareScmRequest {
            remote: request.remote.unwrap_or_else(default_remote),
            fetch: request.fetch,
            revision: request.revision,
            clean: false,
            clean_ignored: false,
        })
    } else {
        None
    };
    let queued = match enqueue_project_build(
        &state,
        project,
        QueueBuildRequest {
            scm,
            parameters: request.parameters,
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

fn verify_webhook_signature(
    secret: &[u8],
    headers: &HeaderMap,
    body: &[u8],
) -> Result<(), ApiError> {
    let encoded = headers
        .get("x-rivet-signature")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("sha256="))
        .ok_or(ApiError::InvalidWebhookSignature)?;
    let signature = hex::decode(encoded).map_err(|_| ApiError::InvalidWebhookSignature)?;
    let mut mac =
        Hmac::<Sha256>::new_from_slice(secret).map_err(|_| ApiError::InvalidWebhookSignature)?;
    mac.update(body);
    mac.verify_slice(&signature)
        .map_err(|_| ApiError::InvalidWebhookSignature)
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

fn spawn_schedule_dispatcher(state: AppState) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            interval.tick().await;
            if let Err(error) = dispatch_due_schedules(&state, Utc::now()).await {
                tracing::error!(?error, "Rivet schedule dispatcher failed");
            }
        }
    });
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
    Json(request): Json<CreateProjectRequest>,
) -> Result<(StatusCode, Json<Project>), ApiError> {
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
) -> Result<Json<Vec<BuildRecord>>, ApiError> {
    let project = project_by_name(&state.storage, &name)?;
    Ok(Json(state.storage.list_builds(project.id)?))
}

async fn get_build(
    State(state): State<AppState>,
    AxumPath((name, number)): AxumPath<(String, i64)>,
) -> Result<Json<BuildDetails>, ApiError> {
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
) -> Result<Json<Vec<LogRecord>>, ApiError> {
    let project = project_by_name(&state.storage, &name)?;
    let build = build_by_number(&state.storage, project.id, &name, number)?;
    Ok(Json(state.storage.logs(build.id)?))
}

async fn get_artifacts(
    State(state): State<AppState>,
    AxumPath((name, number)): AxumPath<(String, i64)>,
) -> Result<Json<Vec<ArtifactRecord>>, ApiError> {
    let project = project_by_name(&state.storage, &name)?;
    let build = build_by_number(&state.storage, project.id, &name, number)?;
    Ok(Json(state.storage.artifacts(build.id)?))
}

async fn download_artifact(
    State(state): State<AppState>,
    AxumPath((name, number, artifact_id)): AxumPath<(String, i64, uuid::Uuid)>,
) -> Result<Response, ApiError> {
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
    request: Option<Json<QueueBuildRequest>>,
) -> Result<(StatusCode, Json<QueueBuildResponse>), ApiError> {
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
    request: Option<Json<QueueBuildRequest>>,
) -> Result<(StatusCode, Json<QueueBuildResponse>), ApiError> {
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
    let repository_root = PathBuf::from(&project.repository_path);
    let source = capture_source_snapshot(&repository_root, request.scm.as_ref()).await?;
    let pipeline = Pipeline::load(&project.pipeline_path)?;
    let parameters = pipeline.resolve_parameters(&request.parameters)?;
    let plan = ExecutionPlan::from_pipeline(&pipeline, uuid::Uuid::new_v4(), project.id);
    let build = state.storage.create_build_with_parameters(
        &project,
        &plan,
        &pipeline,
        source.as_ref(),
        &parameters,
    )?;
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
    tokio::spawn(async move {
        while let Some(event) = received_events.recv().await {
            let event = finalize_artifacts(
                &event_storage,
                &artifact_pipeline,
                &artifact_workspace,
                event,
            );
            if let Err(error) = event_storage.apply_event(&event) {
                tracing::error!(?error, "could not project Rivet build event");
                continue;
            }
            let _ = event_bus.send(event);
        }
    });

    let handle = state
        .scheduler
        .enqueue_with_parameters(
            plan,
            pipeline,
            repository_root,
            parameters,
            cancellation.clone(),
            events,
        )
        .await?;
    spawn_build_reaper(state.active_builds.clone(), handle);
    let mut response_build = build;
    response_build.status = BuildStatus::Queued;
    Ok(QueueBuildResponse {
        build: response_build,
        status: BuildStatus::Queued,
    })
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
    prepare: Option<&PrepareScmRequest>,
) -> Result<Option<SourceSnapshot>, ApiError> {
    match GitRepository::open(path).await {
        Ok(repository) => {
            let snapshot = match prepare {
                Some(request) => {
                    repository
                        .prepare(&GitPrepareOptions {
                            remote: request.remote.clone(),
                            fetch: request.fetch,
                            revision: request.revision.clone(),
                            clean: request.clean,
                            clean_ignored: request.clean_ignored,
                        })
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

async fn cancel_build(
    State(state): State<AppState>,
    AxumPath((name, number)): AxumPath<(String, i64)>,
) -> Result<StatusCode, ApiError> {
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
    websocket: WebSocketUpgrade,
) -> Result<impl IntoResponse, ApiError> {
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
        AgentCapabilities, AgentHeartbeat, AgentRegistration, PROTOCOL_VERSION,
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
        assert_eq!(&body[..], br#"{"queued":0,"running":0,"capacity":2}"#);
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
