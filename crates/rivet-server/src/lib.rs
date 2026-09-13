//! Headless Rivet HTTP/WebSocket service.
//!
//! The service is deliberately a thin transport layer. Pipeline validation,
//! queueing, process supervision, and state projection remain in the shared
//! crates so desktop and server modes execute the same code.

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Path as AxumPath, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::Utc;
use rivet_core::{
    BuildEvent, BuildId, BuildStatus, ExecutionPlan, Pipeline, Project, SourceSnapshot,
};
use rivet_runner::{QueueHandle, Scheduler};
use rivet_scm::{GitPrepareOptions, GitRepository, GitSnapshot, ScmError};
use rivet_storage::{BuildDetails, BuildRecord, LogRecord, Storage, StorageError};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use thiserror::Error;
use tokio::net::TcpListener;
use tokio::sync::{Mutex, broadcast, mpsc};
use tokio_util::sync::CancellationToken;
use tower_http::cors::{Any, CorsLayer};

#[derive(Clone)]
pub struct AppState {
    pub storage: Storage,
    scheduler: Arc<Scheduler>,
    active_builds: Arc<Mutex<HashMap<BuildId, CancellationToken>>>,
    events: broadcast::Sender<BuildEvent>,
}

#[derive(Debug, Error)]
pub enum ServerError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error("could not bind Rivet server: {0}")]
    Bind(#[from] std::io::Error),
}

#[derive(Debug, Error)]
enum ApiError {
    #[error("project not found: {0}")]
    ProjectNotFound(String),
    #[error("build not found: {project} #{number}")]
    BuildNotFound { project: String, number: i64 },
    #[error("repository path is not a directory: {0}")]
    InvalidRepository(PathBuf),
    #[error("pipeline path is not a file: {0}")]
    InvalidPipeline(PathBuf),
    #[error("{0}")]
    BadRequest(String),
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
            Self::ProjectNotFound(_) | Self::BuildNotFound { .. } => StatusCode::NOT_FOUND,
            Self::InvalidRepository(_) | Self::InvalidPipeline(_) | Self::BadRequest(_) => {
                StatusCode::BAD_REQUEST
            }
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

#[derive(Debug, Serialize)]
struct QueueBuildResponse {
    build: BuildRecord,
    status: BuildStatus,
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/api/v1/health", get(health))
        .route("/api/v1/projects", get(list_projects).post(create_project))
        .route(
            "/api/v1/projects/{name}/builds",
            get(list_builds).post(queue_build),
        )
        .route("/api/v1/projects/{name}/builds/{number}", get(get_build))
        .route(
            "/api/v1/projects/{name}/builds/{number}/logs",
            get(get_logs),
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
            "/api/v1/projects/{name}/scm",
            get(get_scm).post(prepare_scm),
        )
        // The default server binds only to loopback and is consumed by the
        // local Tauri webview. Remote deployments should put an explicit
        // authenticated reverse proxy in front of this transport before
        // widening the bind address.
        .layer(
            CorsLayer::new()
                .allow_origin(Any)
                .allow_methods(Any)
                .allow_headers(Any),
        )
        .with_state(state)
}

#[derive(Debug, Deserialize, Default)]
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
    let state = AppState::new(Storage::open(storage_path)?);
    let listener = TcpListener::bind(bind).await?;
    tracing::info!(%bind, "Rivet server listening");
    axum::serve(listener, router(state)).await?;
    Ok(())
}

impl AppState {
    pub fn new(storage: Storage) -> Self {
        let (events, _) = broadcast::channel(1024);
        Self {
            storage,
            scheduler: Arc::new(Scheduler::new(2, Some(1))),
            active_builds: Arc::new(Mutex::new(HashMap::new())),
            events,
        }
    }
}

async fn health() -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok",
        service: "rivet-server",
        timestamp: Utc::now(),
    })
}

async fn list_projects(State(state): State<AppState>) -> Result<Json<Vec<Project>>, ApiError> {
    Ok(Json(state.storage.list_projects()?))
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

async fn queue_build(
    State(state): State<AppState>,
    AxumPath(name): AxumPath<String>,
) -> Result<(StatusCode, Json<QueueBuildResponse>), ApiError> {
    let project = project_by_name(&state.storage, &name)?;
    let repository_root = PathBuf::from(&project.repository_path);
    let pipeline = Pipeline::load(&project.pipeline_path)?;
    let plan = ExecutionPlan::from_pipeline(&pipeline, uuid::Uuid::new_v4(), project.id);
    let source = capture_source_snapshot(&repository_root).await;
    let build = state
        .storage
        .create_build(&project, &plan, &pipeline, source.as_ref())?;
    let cancellation = CancellationToken::new();
    state
        .active_builds
        .lock()
        .await
        .insert(build.id, cancellation.clone());

    let (events, mut received_events) = mpsc::channel(512);
    let event_storage = state.storage.clone();
    let event_bus = state.events.clone();
    tokio::spawn(async move {
        while let Some(event) = received_events.recv().await {
            if let Err(error) = event_storage.apply_event(&event) {
                tracing::error!(?error, "could not project Rivet build event");
                continue;
            }
            let _ = event_bus.send(event);
        }
    });

    let handle = state
        .scheduler
        .enqueue(
            plan,
            pipeline,
            repository_root,
            cancellation.clone(),
            events,
        )
        .await?;
    spawn_build_reaper(state.active_builds.clone(), handle);
    let mut response_build = build;
    response_build.status = BuildStatus::Queued;
    Ok((
        StatusCode::ACCEPTED,
        Json(QueueBuildResponse {
            build: response_build,
            status: BuildStatus::Queued,
        }),
    ))
}

async fn capture_source_snapshot(path: &Path) -> Option<SourceSnapshot> {
    match GitRepository::open(path).await {
        Ok(repository) => match repository.inspect().await {
            Ok(snapshot) => Some(snapshot.source_snapshot()),
            Err(error) => {
                tracing::warn!(?error, "source snapshot unavailable");
                None
            }
        },
        Err(ScmError::NotGitRepository(_)) => None,
        Err(error) => {
            tracing::warn!(?error, "source snapshot unavailable");
            None
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
    Ok(websocket.on_upgrade(move |socket| stream_build_events(socket, receiver, build.id)))
}

async fn stream_build_events(
    mut socket: WebSocket,
    mut receiver: broadcast::Receiver<BuildEvent>,
    build_id: BuildId,
) {
    while let Ok(event) = receiver.recv().await {
        if event_build_id(&event) != build_id {
            continue;
        }
        let payload = match serde_json::to_string(&event) {
            Ok(payload) => payload,
            Err(error) => {
                tracing::error!(?error, "could not serialize Rivet event");
                break;
            }
        };
        if socket.send(Message::Text(payload.into())).await.is_err() {
            break;
        }
        if matches!(event, BuildEvent::BuildFinished { .. }) {
            break;
        }
    }
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
    use axum::body::Body;
    use axum::http::Request;
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
}
