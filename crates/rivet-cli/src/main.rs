use chrono::Utc;
use clap::{Args, Parser, Subcommand};
use futures_util::{SinkExt, StreamExt};
use rivet_agent_protocol::{
    AgentCapabilities, AgentHeartbeat, AgentId, AgentMessage, AgentRegistration, PROTOCOL_VERSION,
};
use rivet_core::{
    BuildEvent, BuildStatus, CronExpression, ExecutionPlan, LogStream, Pipeline, Project,
    ScheduleId, SourceSnapshot,
};
use rivet_migration::{analyze_jenkinsfile_file, generate_rivetfile_draft_file};
use rivet_runner::{QueueHandle, Scheduler};
use rivet_scm::{GitPrepareOptions, GitRepository, ScmError};
use rivet_storage::Storage;
use std::collections::BTreeMap;
use std::fs;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use tokio::sync::mpsc;
use tokio::time::Duration;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message as AgentSocketMessage;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_util::sync::CancellationToken;

const SAMPLE_PIPELINE: &str = r#"version = 1
name = "sample"

[[stages]]
name = "Test"

[[stages.steps]]
name = "unit"
program = "cargo"
args = ["test", "--workspace"]
timeout_seconds = 600
"#;

#[derive(Debug, Parser)]
#[command(name = "rivet", version, about = "Rust-first CI/CD automation")]
struct Cli {
    /// Directory containing Rivet's local SQLite state.
    #[arg(long, global = true, default_value = ".rivet")]
    data_dir: PathBuf,
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Create a starter Rivetfile without overwriting an existing one.
    Init {
        #[arg(default_value = ".")]
        repository: PathBuf,
    },
    /// Manage local projects.
    Project {
        #[command(subcommand)]
        command: ProjectCommand,
    },
    /// Queue and execute a project's pipeline.
    Run(RunArgs),
    /// Show persisted build history.
    Builds { project: String },
    /// Show persisted output for a build number.
    Logs {
        project: String,
        #[arg(long)]
        build: i64,
    },
    /// Show artifacts collected for a build.
    Artifacts {
        project: String,
        #[arg(long)]
        build: i64,
    },
    /// Queue a new build using a completed build's parameters.
    Retry {
        project: String,
        #[arg(long)]
        build: i64,
        #[arg(long = "param", value_name = "NAME=VALUE")]
        parameters: Vec<String>,
    },
    /// Run the headless HTTP/WebSocket service.
    Server {
        #[arg(long, default_value = "127.0.0.1:7878")]
        bind: SocketAddr,
        /// Read a Bearer token from a private file without persisting it.
        #[arg(long)]
        token_file: Option<PathBuf>,
        /// Read the generic webhook HMAC secret from a private file.
        #[arg(long)]
        webhook_secret_file: Option<PathBuf>,
        /// Allow an additional exact browser origin for the API.
        #[arg(long = "allow-origin", value_name = "ORIGIN")]
        allowed_origins: Vec<String>,
    },
    /// Inspect or explicitly prepare a local Git repository.
    Scm {
        #[command(subcommand)]
        command: ScmCommand,
    },
    /// Manage persisted UTC cron schedules.
    Schedule {
        #[command(subcommand)]
        command: ScheduleCommand,
    },
    /// Analyze a Jenkinsfile without executing Groovy or plugin code.
    Analyze {
        #[command(subcommand)]
        command: AnalyzeCommand,
    },
    /// Connect this machine to a Rivet server as a heartbeat-only agent.
    Agent(AgentArgs),
}

#[derive(Debug, Subcommand)]
enum ProjectCommand {
    Create(CreateProject),
    List,
}

#[derive(Debug, Subcommand)]
enum ScheduleCommand {
    /// Create an enabled UTC cron schedule for a project.
    Create {
        project: String,
        #[arg(long)]
        name: String,
        #[arg(long)]
        expression: String,
        #[arg(long)]
        disabled: bool,
    },
    /// List the project's persisted schedules.
    List { project: String },
    /// Enable a schedule by UUID.
    Enable { project: String, id: ScheduleId },
    /// Disable a schedule by UUID.
    Disable { project: String, id: ScheduleId },
    /// Delete a schedule by UUID.
    Delete { project: String, id: ScheduleId },
}

#[derive(Debug, Subcommand)]
enum AnalyzeCommand {
    /// Report supported, partial, and unsupported Jenkins constructs as JSON.
    Jenkinsfile {
        path: PathBuf,
        /// Include a valid draft for simple, explicitly quoted sh/bat steps.
        #[arg(long)]
        draft: bool,
    },
}

#[derive(Debug, Args)]
struct CreateProject {
    name: String,
    #[arg(long, default_value = ".")]
    repository: PathBuf,
    #[arg(long)]
    pipeline: Option<PathBuf>,
}

#[derive(Debug, Args)]
struct RunArgs {
    project: String,
    #[arg(long, default_value = "origin")]
    remote: String,
    #[arg(long)]
    fetch: bool,
    #[arg(long)]
    revision: Option<String>,
    #[arg(long)]
    clean: bool,
    #[arg(long)]
    clean_ignored: bool,
    #[arg(long = "param", value_name = "NAME=VALUE")]
    parameters: Vec<String>,
}

#[derive(Debug, Args)]
struct AgentArgs {
    /// WebSocket endpoint exposed by the Rivet server.
    #[arg(long, default_value = "ws://127.0.0.1:7878/api/v1/agents/connect")]
    server: String,
    /// Stable agent identity; a new UUID is generated when omitted.
    #[arg(long)]
    id: Option<AgentId>,
    /// Human-readable name shown in the fleet registry.
    #[arg(long, default_value_t = default_agent_name())]
    name: String,
    /// Operating system capability advertised to the scheduler.
    #[arg(long, default_value_t = default_agent_os())]
    os: String,
    /// CPU architecture capability advertised to the scheduler.
    #[arg(long, default_value_t = default_agent_arch())]
    arch: String,
    /// Advertise Docker availability.
    #[arg(long)]
    docker: bool,
    /// Add an exact scheduler label. May be supplied more than once.
    #[arg(long = "label")]
    labels: Vec<String>,
    /// Number of local executor slots advertised to the scheduler.
    #[arg(long, default_value_t = 1)]
    executors: u16,
    /// Read a Bearer token from a private file without persisting it.
    #[arg(long)]
    token_file: Option<PathBuf>,
}

#[derive(Debug, Subcommand)]
enum ScmCommand {
    /// Print the current Git revision, branch, remote, and worktree state.
    Inspect { repository: PathBuf },
    /// Optionally fetch, checkout, and clean before printing the final state.
    Prepare {
        repository: PathBuf,
        #[arg(long, default_value = "origin")]
        remote: String,
        #[arg(long)]
        fetch: bool,
        #[arg(long)]
        revision: Option<String>,
        #[arg(long)]
        clean: bool,
        #[arg(long)]
        clean_ignored: bool,
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    match cli.command {
        Command::Init { repository } => init_repository(&cli.data_dir, &repository)?,
        Command::Project { command } => {
            let storage = open_storage(&cli.data_dir)?;
            match command {
                ProjectCommand::Create(args) => create_project(&storage, args)?,
                ProjectCommand::List => list_projects(&storage)?,
            }
        }
        Command::Run(args) => run_project(&cli.data_dir, args).await?,
        Command::Builds { project } => list_builds(&cli.data_dir, &project)?,
        Command::Logs { project, build } => show_logs(&cli.data_dir, &project, build)?,
        Command::Artifacts { project, build } => list_artifacts(&cli.data_dir, &project, build)?,
        Command::Retry {
            project,
            build,
            parameters,
        } => retry_project(&cli.data_dir, &project, build, parameters).await?,
        Command::Server {
            bind,
            token_file,
            webhook_secret_file,
            allowed_origins,
        } => {
            let auth_token = token_file.as_deref().map(read_auth_token).transpose()?;
            let webhook_secret = webhook_secret_file
                .as_deref()
                .map(read_webhook_secret)
                .transpose()?;
            rivet_server::serve_with_config(
                cli.data_dir.join("rivet.db"),
                rivet_server::ServerConfig {
                    bind,
                    auth_token,
                    webhook_secret,
                    allowed_origins,
                },
            )
            .await?
        }
        Command::Scm { command } => inspect_scm(command).await?,
        Command::Schedule { command } => manage_schedule(&cli.data_dir, command)?,
        Command::Analyze { command } => analyze_file(command)?,
        Command::Agent(args) => run_agent(args).await?,
    }
    Ok(())
}

fn analyze_file(command: AnalyzeCommand) -> Result<(), Box<dyn std::error::Error>> {
    match command {
        AnalyzeCommand::Jenkinsfile { path, draft } => {
            let analysis = analyze_jenkinsfile_file(&path)?;
            let output = if draft {
                let draft = generate_rivetfile_draft_file(&path)?;
                serde_json::json!({
                    "source": path,
                    "analysis": analysis,
                    "draft": draft,
                })
            } else {
                serde_json::json!({
                    "source": path,
                    "analysis": analysis,
                })
            };
            println!("{}", serde_json::to_string_pretty(&output)?);
        }
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AgentSessionResult {
    Stopped,
    Disconnected,
}

async fn run_agent(args: AgentArgs) -> Result<(), Box<dyn std::error::Error>> {
    let agent_id = args.id.unwrap_or_else(uuid::Uuid::new_v4);
    let registration = AgentRegistration {
        protocol_version: PROTOCOL_VERSION,
        agent_id,
        name: args.name.clone(),
        capabilities: AgentCapabilities {
            os: args.os.clone(),
            arch: args.arch.clone(),
            docker: args.docker,
            labels: args.labels.clone(),
            executors: args.executors,
        },
    };
    registration.validate()?;

    let mut reconnect_delay = Duration::from_secs(1);
    loop {
        match run_agent_session(&args, &registration).await {
            Ok(AgentSessionResult::Stopped) => break,
            Ok(AgentSessionResult::Disconnected) => {
                eprintln!(
                    "Agent connection closed; reconnecting in {}s...",
                    reconnect_delay.as_secs()
                );
            }
            Err(error) => {
                eprintln!(
                    "Agent connection failed: {error}; reconnecting in {}s...",
                    reconnect_delay.as_secs()
                );
            }
        }
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                eprintln!("Stopping agent heartbeat...");
                break;
            }
            _ = tokio::time::sleep(reconnect_delay) => {}
        }
        reconnect_delay = (reconnect_delay * 2).min(Duration::from_secs(10));
    }
    Ok(())
}

async fn run_agent_session(
    args: &AgentArgs,
    registration: &AgentRegistration,
) -> Result<AgentSessionResult, Box<dyn std::error::Error>> {
    let mut request = args.server.clone().into_client_request()?;
    if let Some(token_file) = args.token_file.as_deref() {
        let token = read_auth_token(token_file)?;
        request.headers_mut().insert(
            "authorization",
            HeaderValue::from_str(&format!("Bearer {token}"))?,
        );
    }
    let (mut socket, _) = connect_async(request).await?;
    send_agent_socket_message(&mut socket, AgentMessage::Register(registration.clone())).await?;
    let registered = socket
        .next()
        .await
        .ok_or("Rivet server closed the agent connection during registration")??;
    let session_id = match decode_agent_socket_message(registered)? {
        AgentMessage::Registered {
            agent_id: registered_agent,
            session_id,
            ..
        } if registered_agent == registration.agent_id => session_id,
        AgentMessage::Error { code, message, .. } => {
            return Err(format!("agent registration rejected ({code}): {message}").into());
        }
        _ => return Err("Rivet server did not acknowledge this agent registration".into()),
    };

    println!(
        "Connected agent {} ({}) with {} executor{}",
        registration.name,
        registration.agent_id,
        registration.capabilities.executors,
        if registration.capabilities.executors == 1 {
            ""
        } else {
            "s"
        }
    );
    let mut sequence = 0;
    let mut heartbeat = tokio::time::interval(Duration::from_secs(10));
    let mut session_result = AgentSessionResult::Disconnected;
    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                eprintln!("Stopping agent heartbeat...");
                session_result = AgentSessionResult::Stopped;
                break;
            }
            _ = heartbeat.tick() => {
                sequence += 1;
                send_agent_socket_message(
                    &mut socket,
                    AgentMessage::Heartbeat(AgentHeartbeat {
                        protocol_version: PROTOCOL_VERSION,
                        agent_id: registration.agent_id,
                        session_id,
                        sequence,
                        running: Vec::new(),
                        sent_at: Utc::now(),
                    }),
                ).await?;
            }
            message = socket.next() => {
                let Some(message) = message else { break; };
                match message? {
                    AgentSocketMessage::Ping(payload) => {
                        socket.send(AgentSocketMessage::Pong(payload)).await?;
                    }
                    AgentSocketMessage::Pong(_) => {}
                    AgentSocketMessage::Close(_) => break,
                    message => match decode_agent_socket_message(message)? {
                        AgentMessage::HeartbeatAck { .. } => {}
                        AgentMessage::Error { code, message, .. } => {
                            return Err(format!("agent connection error ({code}): {message}").into());
                        }
                        AgentMessage::Assign { .. } => {
                            send_agent_socket_message(
                                &mut socket,
                                AgentMessage::Error {
                                    protocol_version: PROTOCOL_VERSION,
                                    code: "assignment_not_supported".into(),
                                    message: "this heartbeat-only client cannot execute remote assignments".into(),
                                },
                            ).await?;
                            break;
                        }
                        _ => {}
                    },
                }
            }
        }
    }
    let _ = socket.close(None).await;
    Ok(session_result)
}

async fn send_agent_socket_message(
    socket: &mut tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    message: AgentMessage,
) -> Result<(), Box<dyn std::error::Error>> {
    let payload = serde_json::to_string(&message)?;
    socket
        .send(AgentSocketMessage::Text(payload.into()))
        .await?;
    Ok(())
}

fn decode_agent_socket_message(
    message: AgentSocketMessage,
) -> Result<AgentMessage, Box<dyn std::error::Error>> {
    let payload = match message {
        AgentSocketMessage::Text(text) => text.to_string(),
        AgentSocketMessage::Binary(bytes) => String::from_utf8(bytes.to_vec())?,
        AgentSocketMessage::Ping(_) | AgentSocketMessage::Pong(_) => {
            return Err("control frame is not an agent message".into());
        }
        AgentSocketMessage::Close(_) => return Err("agent websocket closed".into()),
        AgentSocketMessage::Frame(_) => {
            return Err("raw websocket frame is not an agent message".into());
        }
    };
    let message: AgentMessage = serde_json::from_str(&payload)?;
    message.validate()?;
    Ok(message)
}

fn default_agent_name() -> String {
    format!("{}-{}", default_agent_os(), default_agent_arch())
}

fn default_agent_os() -> String {
    std::env::consts::OS.to_owned()
}

fn default_agent_arch() -> String {
    std::env::consts::ARCH.to_owned()
}

async fn inspect_scm(command: ScmCommand) -> Result<(), Box<dyn std::error::Error>> {
    let (repository_path, options) = match command {
        ScmCommand::Inspect { repository } => (repository, None),
        ScmCommand::Prepare {
            repository,
            remote,
            fetch,
            revision,
            clean,
            clean_ignored,
        } => (
            repository,
            Some(GitPrepareOptions {
                remote,
                fetch,
                revision,
                clean,
                clean_ignored,
            }),
        ),
    };
    let repository = GitRepository::open(&repository_path).await?;
    let snapshot = match options {
        Some(options) => repository.prepare(&options).await?,
        None => repository.inspect().await?,
    };
    println!("{}", serde_json::to_string_pretty(&snapshot)?);
    Ok(())
}

fn open_storage(data_dir: &Path) -> Result<Storage, Box<dyn std::error::Error>> {
    Ok(Storage::open(data_dir.join("rivet.db"))?)
}

fn manage_schedule(
    data_dir: &Path,
    command: ScheduleCommand,
) -> Result<(), Box<dyn std::error::Error>> {
    let storage = open_storage(data_dir)?;
    match command {
        ScheduleCommand::Create {
            project,
            name,
            expression,
            disabled,
        } => {
            let project_record = storage
                .get_project_by_name(&project)?
                .ok_or_else(|| format!("project not found: {project}"))?;
            let name = validate_schedule_name(name)?;
            let expression = CronExpression::parse(&expression)?;
            let next_run_at = expression.next_after(Utc::now())?;
            let schedule = storage.create_schedule(
                project_record.id,
                name,
                expression.expression(),
                !disabled,
                next_run_at,
            )?;
            println!(
                "Created schedule {} ({}) next {}",
                schedule.name,
                schedule.id,
                schedule.next_run_at.to_rfc3339()
            );
        }
        ScheduleCommand::List { project } => {
            let project_record = storage
                .get_project_by_name(&project)?
                .ok_or_else(|| format!("project not found: {project}"))?;
            for schedule in storage.list_schedules(project_record.id)? {
                println!(
                    "{}\t{}\t{}\t{}\t{}",
                    schedule.id,
                    if schedule.enabled {
                        "enabled"
                    } else {
                        "disabled"
                    },
                    schedule.name,
                    schedule.expression,
                    schedule.next_run_at.to_rfc3339()
                );
            }
        }
        ScheduleCommand::Enable { project, id } => {
            set_schedule_enabled(&storage, &project, id, true)?;
        }
        ScheduleCommand::Disable { project, id } => {
            set_schedule_enabled(&storage, &project, id, false)?;
        }
        ScheduleCommand::Delete { project, id } => {
            let project_record = storage
                .get_project_by_name(&project)?
                .ok_or_else(|| format!("project not found: {project}"))?;
            if !storage.delete_schedule(project_record.id, id)? {
                return Err(format!("schedule not found: {project} {id}").into());
            }
            println!("Deleted schedule {id}");
        }
    }
    Ok(())
}

fn set_schedule_enabled(
    storage: &Storage,
    project: &str,
    id: ScheduleId,
    enabled: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let project_record = storage
        .get_project_by_name(project)?
        .ok_or_else(|| format!("project not found: {project}"))?;
    let schedule = storage
        .set_schedule_enabled(project_record.id, id, enabled)?
        .ok_or_else(|| format!("schedule not found: {project} {id}"))?;
    println!(
        "Schedule {} {}",
        schedule.id,
        if schedule.enabled {
            "enabled"
        } else {
            "disabled"
        }
    );
    Ok(())
}

fn validate_schedule_name(name: String) -> Result<String, Box<dyn std::error::Error>> {
    let name = name.trim();
    if name.is_empty() {
        return Err("schedule name cannot be empty".into());
    }
    if name.contains('/') || name.contains('\\') || name.chars().any(char::is_control) {
        return Err("schedule name cannot contain path separators or control characters".into());
    }
    Ok(name.to_owned())
}

fn read_auth_token(path: &Path) -> Result<String, Box<dyn std::error::Error>> {
    read_private_value(path, "token")
}

fn read_webhook_secret(path: &Path) -> Result<String, Box<dyn std::error::Error>> {
    read_private_value(path, "webhook secret")
}

fn read_private_value(path: &Path, label: &str) -> Result<String, Box<dyn std::error::Error>> {
    let metadata = fs::metadata(path)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if metadata.permissions().mode() & 0o077 != 0 {
            return Err(format!(
                "{label} file must not be group- or world-readable: {}",
                path.display()
            )
            .into());
        }
    }
    let value = fs::read_to_string(path)?.trim().to_owned();
    if value.is_empty() {
        return Err(format!("{label} file is empty: {}", path.display()).into());
    }
    Ok(value)
}

fn init_repository(data_dir: &Path, repository: &Path) -> Result<(), Box<dyn std::error::Error>> {
    fs::create_dir_all(repository)?;
    fs::create_dir_all(data_dir)?;
    let path = repository.join("Rivetfile.toml");
    if path.exists() {
        println!("Rivetfile already exists: {}", path.display());
    } else {
        fs::write(&path, SAMPLE_PIPELINE)?;
        println!("Created {}", path.display());
    }
    Ok(())
}

fn create_project(
    storage: &Storage,
    args: CreateProject,
) -> Result<(), Box<dyn std::error::Error>> {
    let repository = fs::canonicalize(&args.repository)?;
    let pipeline_path = args
        .pipeline
        .unwrap_or_else(|| repository.join("Rivetfile.toml"));
    let pipeline_path = fs::canonicalize(pipeline_path)?;
    let pipeline = Pipeline::load(&pipeline_path)?;
    let project = Project::new(
        args.name,
        repository.to_string_lossy().into_owned(),
        pipeline_path.to_string_lossy().into_owned(),
    )?;
    storage.create_project(&project, &pipeline)?;
    println!("Created project {} ({})", project.name, project.id);
    Ok(())
}

fn list_projects(storage: &Storage) -> Result<(), Box<dyn std::error::Error>> {
    for project in storage.list_projects()? {
        println!(
            "{}\t{}\t{}",
            project.name, project.id, project.repository_path
        );
    }
    Ok(())
}

async fn run_project(data_dir: &Path, args: RunArgs) -> Result<(), Box<dyn std::error::Error>> {
    let scm = if args.fetch
        || args.revision.is_some()
        || args.clean
        || args.clean_ignored
        || args.remote != "origin"
    {
        Some(GitPrepareOptions {
            remote: args.remote,
            fetch: args.fetch,
            revision: args.revision,
            clean: args.clean,
            clean_ignored: args.clean_ignored,
        })
    } else {
        None
    };
    let supplied_parameters = parse_parameters(&args.parameters)?;
    run_project_with_options(data_dir, &args.project, scm, supplied_parameters).await
}

async fn retry_project(
    data_dir: &Path,
    name: &str,
    number: i64,
    parameter_values: Vec<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    let storage = open_storage(data_dir)?;
    let project = storage
        .get_project_by_name(name)?
        .ok_or_else(|| format!("project not found: {name}"))?;
    let original = storage
        .list_builds(project.id)?
        .into_iter()
        .find(|build| build.number == number)
        .ok_or_else(|| format!("build not found: {name} #{number}"))?;
    if !original.status.is_terminal() {
        return Err(format!("build {name} #{number} is not finished and cannot be retried").into());
    }
    let pipeline = Pipeline::load(&project.pipeline_path)?;
    let replacements = parse_parameters(&parameter_values)?;
    if let Some(missing) = pipeline
        .parameters
        .iter()
        .find(|parameter| parameter.secret && !replacements.contains_key(&parameter.name))
    {
        return Err(format!(
            "retry requires an explicit value for secret parameter {:?}; pass it with --param {}=VALUE",
            missing.name, missing.name
        )
        .into());
    }
    let supplied_parameters = if replacements.is_empty() {
        original.parameters
    } else {
        let mut parameters = original.parameters;
        parameters.extend(replacements);
        parameters
    };
    run_project_with_options(data_dir, name, None, supplied_parameters).await
}

async fn run_project_with_options(
    data_dir: &Path,
    name: &str,
    scm: Option<GitPrepareOptions>,
    supplied_parameters: BTreeMap<String, String>,
) -> Result<(), Box<dyn std::error::Error>> {
    let storage = open_storage(data_dir)?;
    let project = storage
        .get_project_by_name(name)?
        .ok_or_else(|| format!("project not found: {name}"))?;
    let source = capture_source_snapshot(&project.repository_path, scm.as_ref()).await?;
    let pipeline = Pipeline::load(&project.pipeline_path)?;
    let parameters = pipeline.resolve_parameters(&supplied_parameters)?;
    let build_id = uuid::Uuid::new_v4();
    let plan = ExecutionPlan::from_pipeline(&pipeline, build_id, project.id);
    let build = storage.create_build_with_parameters(
        &project,
        &plan,
        &pipeline,
        source.as_ref(),
        &parameters,
    )?;
    println!("Queued {} #{} ({})", project.name, build.number, build.id);

    let (events, mut received_events) = mpsc::channel(512);
    let event_storage = storage.clone();
    let artifact_pipeline = pipeline.clone();
    let artifact_workspace = PathBuf::from(project.repository_path.clone());
    let event_consumer = tokio::spawn(async move {
        while let Some(event) = received_events.recv().await {
            let event = finalize_artifacts(
                &event_storage,
                &artifact_pipeline,
                &artifact_workspace,
                event,
            );
            print_event(&event);
            event_storage.apply_event(&event)?;
        }
        Ok::<(), rivet_storage::StorageError>(())
    });

    let cancellation = CancellationToken::new();
    let scheduler = Scheduler::new_with_cache(1, Some(1), Some(storage.cache_root()));
    let handle = scheduler
        .enqueue_with_parameters(
            plan,
            pipeline,
            PathBuf::from(project.repository_path.clone()),
            parameters,
            cancellation.clone(),
            events,
        )
        .await?;
    let status = wait_for_build(handle, cancellation).await?;
    let consume_result = event_consumer.await??;
    let _ = consume_result;
    let status = storage
        .list_builds(project.id)?
        .into_iter()
        .find(|record| record.id == build.id)
        .map(|record| record.status)
        .unwrap_or(status);

    println!("Build #{} {}", build.number, status_label(&status));
    if status != BuildStatus::Passed {
        return Err(format!("build finished {}", status_label(&status)).into());
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
        eprintln!("artifact collection failed for {build_id}: {error}");
        return BuildEvent::BuildFinished {
            build_id: *build_id,
            status: BuildStatus::Failed,
            timestamp: *timestamp,
        };
    }
    event
}

fn parse_parameters(values: &[String]) -> Result<BTreeMap<String, String>, String> {
    let mut parameters = BTreeMap::new();
    for value in values {
        let (name, parameter) = value
            .split_once('=')
            .ok_or_else(|| format!("parameter must use NAME=VALUE syntax: {value:?}"))?;
        if name.is_empty() {
            return Err(format!("parameter name cannot be empty: {value:?}"));
        }
        if parameters
            .insert(name.to_owned(), parameter.to_owned())
            .is_some()
        {
            return Err(format!("parameter {name:?} was provided more than once"));
        }
    }
    Ok(parameters)
}

async fn capture_source_snapshot(
    path: &str,
    prepare: Option<&GitPrepareOptions>,
) -> Result<Option<SourceSnapshot>, ScmError> {
    match GitRepository::open(path).await {
        Ok(repository) => {
            let snapshot = match prepare {
                Some(options) => repository.prepare(options).await?,
                None => repository.inspect().await?,
            };
            Ok(Some(snapshot.source_snapshot()))
        }
        Err(ScmError::NotGitRepository(_)) if prepare.is_none() => Ok(None),
        Err(error) => {
            if prepare.is_some() {
                Err(error)
            } else {
                eprintln!("warning: source snapshot unavailable: {error}");
                Ok(None)
            }
        }
    }
}

async fn wait_for_build(
    handle: QueueHandle,
    cancellation: CancellationToken,
) -> Result<BuildStatus, Box<dyn std::error::Error>> {
    let wait = handle.wait();
    tokio::pin!(wait);
    let result = tokio::select! {
        result = &mut wait => result?,
        _ = tokio::signal::ctrl_c() => {
            eprintln!("Cancellation requested; stopping the active process...");
            cancellation.cancel();
            (&mut wait).await?
        }
    }?;
    Ok(result)
}

fn list_builds(data_dir: &Path, name: &str) -> Result<(), Box<dyn std::error::Error>> {
    let storage = open_storage(data_dir)?;
    let project = storage
        .get_project_by_name(name)?
        .ok_or_else(|| format!("project not found: {name}"))?;
    for build in storage.list_builds(project.id)? {
        println!(
            "#{}\t{}\t{}\t{}",
            build.number,
            status_label(&build.status),
            build.id,
            build.queued_at.to_rfc3339()
        );
    }
    Ok(())
}

fn show_logs(data_dir: &Path, name: &str, number: i64) -> Result<(), Box<dyn std::error::Error>> {
    let storage = open_storage(data_dir)?;
    let project = storage
        .get_project_by_name(name)?
        .ok_or_else(|| format!("project not found: {name}"))?;
    let build = storage
        .list_builds(project.id)?
        .into_iter()
        .find(|build| build.number == number)
        .ok_or_else(|| format!("build not found: {name} #{number}"))?;
    for log in storage.logs(build.id)? {
        let stream = match log.stream {
            LogStream::Stdout => "out",
            LogStream::Stderr => "err",
            LogStream::System => "system",
        };
        println!("{} [{}] {}", log.timestamp.to_rfc3339(), stream, log.line);
    }
    Ok(())
}

fn list_artifacts(
    data_dir: &Path,
    name: &str,
    number: i64,
) -> Result<(), Box<dyn std::error::Error>> {
    let storage = open_storage(data_dir)?;
    let project = storage
        .get_project_by_name(name)?
        .ok_or_else(|| format!("project not found: {name}"))?;
    let build = storage
        .list_builds(project.id)?
        .into_iter()
        .find(|build| build.number == number)
        .ok_or_else(|| format!("build not found: {name} #{number}"))?;
    for artifact in storage.artifacts(build.id)? {
        println!(
            "{}\t{}\t{} bytes\t{}",
            artifact.name, artifact.relative_path, artifact.size_bytes, artifact.checksum
        );
    }
    Ok(())
}

fn print_event(event: &BuildEvent) {
    match event {
        BuildEvent::BuildQueued { .. } => println!("  queue: queued"),
        BuildEvent::BuildStarted { .. } => println!("  build: running"),
        BuildEvent::StageStarted { stage_name, .. } => println!("  stage: {stage_name} -> running"),
        BuildEvent::StepStarted { step_name, .. } => println!("    step: {step_name} -> running"),
        BuildEvent::StepOutput { stream, line, .. } => {
            let stream = match stream {
                LogStream::Stdout => "out",
                LogStream::Stderr => "err",
                LogStream::System => "system",
            };
            println!("      [{stream}] {line}");
        }
        BuildEvent::StepFinished {
            step_name, status, ..
        } => {
            println!("    step: {step_name} -> {}", status_label(status));
        }
        BuildEvent::StageFinished {
            stage_name, status, ..
        } => println!("  stage: {stage_name} -> {}", status_label(status)),
        BuildEvent::BuildCancelled { .. } => println!("  build: cancelled"),
        BuildEvent::BuildFinished { status, .. } => println!("  build: {}", status_label(status)),
    }
}

fn status_label<T>(status: &T) -> &'static str
where
    T: StatusLabel,
{
    status.label()
}

trait StatusLabel {
    fn label(&self) -> &'static str;
}

impl StatusLabel for BuildStatus {
    fn label(&self) -> &'static str {
        match self {
            BuildStatus::Pending => "pending",
            BuildStatus::Queued => "queued",
            BuildStatus::Running => "running",
            BuildStatus::Passed => "passed",
            BuildStatus::Failed => "failed",
            BuildStatus::Cancelled => "cancelled",
        }
    }
}

impl StatusLabel for rivet_core::StageStatus {
    fn label(&self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Running => "running",
            Self::Passed => "passed",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
            Self::Skipped => "skipped",
        }
    }
}

impl StatusLabel for rivet_core::StepStatus {
    fn label(&self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Running => "running",
            Self::Passed => "passed",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
            Self::Skipped => "skipped",
        }
    }
}
