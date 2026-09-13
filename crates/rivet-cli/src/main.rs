use chrono::{DateTime, Utc};
use clap::{Args, Parser, Subcommand, ValueEnum};
use futures_util::{SinkExt, StreamExt};
use globset::{Glob, GlobSetBuilder};
use rivet_agent_protocol::{
    AgentCapabilities, AgentHeartbeat, AgentId, AgentMessage, AgentRegistration,
    MAX_WORKSPACE_BYTES, MAX_WORKSPACE_CHUNK_BYTES, MAX_WORKSPACE_FILES, PROTOCOL_VERSION,
    WorkspaceTransfer,
};
use rivet_auth::{
    ApiTokenRecord, AuthPolicy, AuthPolicyDocument, Role as AuthRole, generate_token, token_digest,
};
use rivet_compat::{BehaviorFixture, compare_fixture};
use rivet_core::{
    BuildEvent, BuildStatus, CronExpression, ExecutionPlan, LogStream, Pipeline, Project,
    ScheduleId, SourceSnapshot,
};
use rivet_credentials::{CredentialKeychain, CredentialVault};
use rivet_migration::{analyze_jenkinsfile_file, generate_rivetfile_draft_file};
use rivet_runner::execute_pipeline_with_parameters;
use rivet_runner::{CacheStore, MAX_QUEUE_PRIORITY, MIN_QUEUE_PRIORITY, QueueHandle, Scheduler};
use rivet_scm::{GitHttpCredential, GitPrepareOptions, GitRepository, ScmError};
use rivet_storage::Storage;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fs;
use std::future::Future;
use std::io::Write;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use tar::Archive;
use tokio::sync::mpsc;
use tokio::time::Duration;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message as AgentSocketMessage;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_util::sync::CancellationToken;
use walkdir::{DirEntry, WalkDir};

type AgentSocket =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

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
    /// Inspect and prune artifacts stored from completed builds.
    Artifact {
        #[command(subcommand)]
        command: ArtifactCommand,
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
        /// Open a private SHA-256 token policy with roles and project scopes.
        #[arg(long)]
        auth_policy_file: Option<PathBuf>,
        /// Read the generic webhook HMAC secret from a private file.
        #[arg(long)]
        webhook_secret_file: Option<PathBuf>,
        /// Read the GitHub webhook HMAC secret from a private file.
        #[arg(long)]
        github_webhook_secret_file: Option<PathBuf>,
        /// Read the GitLab webhook signing/secret token from a private file.
        #[arg(long)]
        gitlab_webhook_secret_file: Option<PathBuf>,
        /// Default Rivet credential ID for GitHub push fetches.
        #[arg(long)]
        github_webhook_credential_id: Option<String>,
        /// Default Rivet credential ID for GitLab push fetches.
        #[arg(long)]
        gitlab_webhook_credential_id: Option<String>,
        /// Open the passphrase-encrypted SCM credential vault.
        #[arg(long)]
        credentials_file: Option<PathBuf>,
        /// Read the credential vault passphrase from a private file.
        #[arg(long)]
        credentials_passphrase_file: Option<PathBuf>,
        /// Read the credential vault passphrase from the OS keychain account.
        #[arg(long, conflicts_with = "credentials_passphrase_file")]
        credentials_keychain_account: Option<String>,
        /// Load regular JSON extension manifests from this local directory.
        #[arg(long)]
        extension_manifest_dir: Option<PathBuf>,
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
    /// Manage passphrase-encrypted SCM credentials.
    Credential {
        #[command(subcommand)]
        command: CredentialCommand,
    },
    /// Manage local API authentication tokens.
    Auth {
        #[command(subcommand)]
        command: AuthCommand,
    },
    /// Inspect and prune the local CI cache.
    Cache {
        #[command(subcommand)]
        command: CacheCommand,
    },
    /// Analyze a Jenkinsfile without executing Groovy or plugin code.
    Analyze {
        #[command(subcommand)]
        command: AnalyzeCommand,
    },
    /// Compare recorded Jenkins/Rivet semantics locally.
    Compat {
        #[command(subcommand)]
        command: CompatCommand,
    },
    /// Connect this machine to a Rivet server as a build agent.
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
enum CredentialCommand {
    /// Store an HTTP basic credential without placing its secret in argv.
    Set {
        id: String,
        #[arg(long)]
        username: String,
        /// Restrict use to one or more project names; repeat the flag.
        #[arg(long = "project")]
        projects: Vec<String>,
        #[arg(long)]
        secret_file: PathBuf,
        #[arg(long)]
        passphrase_file: PathBuf,
        #[arg(long)]
        vault_file: Option<PathBuf>,
    },
    /// Store a vault passphrase in the operating-system keychain.
    KeychainSet {
        account: String,
        #[arg(long)]
        passphrase_file: PathBuf,
    },
    /// Remove a vault passphrase from the operating-system keychain.
    KeychainRemove { account: String },
    /// List credential IDs and usernames without revealing secrets.
    List {
        #[arg(long)]
        passphrase_file: PathBuf,
        #[arg(long)]
        vault_file: Option<PathBuf>,
    },
    /// Remove one credential from the encrypted vault.
    Remove {
        id: String,
        #[arg(long)]
        passphrase_file: PathBuf,
        #[arg(long)]
        vault_file: Option<PathBuf>,
    },
}

#[derive(Debug, Subcommand)]
enum AuthCommand {
    /// Manage policy-backed Bearer tokens.
    Token {
        #[command(subcommand)]
        command: AuthTokenCommand,
    },
}

#[derive(Debug, Subcommand)]
enum AuthTokenCommand {
    /// Generate a token, save it privately, and add only its digest to policy.
    Create {
        id: String,
        #[arg(long, value_enum, default_value_t = AuthRoleArg::Operator)]
        role: AuthRoleArg,
        #[arg(long = "project")]
        projects: Vec<String>,
        #[arg(long)]
        policy_file: PathBuf,
        #[arg(long)]
        token_file: PathBuf,
        /// Optional RFC3339 expiry, for example 2026-12-31T23:59:59Z.
        #[arg(long)]
        expires_at: Option<String>,
    },
    /// List token IDs, roles, and project scopes without revealing tokens.
    List {
        #[arg(long)]
        policy_file: PathBuf,
    },
    /// Revoke one token ID while keeping the policy non-empty.
    Revoke {
        id: String,
        #[arg(long)]
        policy_file: PathBuf,
    },
}

#[derive(Debug, Subcommand)]
enum CacheCommand {
    /// Remove oldest cache archives until they fit under a byte budget.
    Prune {
        #[arg(long, value_name = "BYTES")]
        max_bytes: u64,
    },
}

#[derive(Debug, Subcommand)]
enum ArtifactCommand {
    /// Remove oldest completed-build artifacts until they fit a byte budget.
    Prune {
        #[arg(long, value_name = "BYTES")]
        max_bytes: u64,
    },
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum AuthRoleArg {
    Admin,
    Operator,
    Viewer,
    Agent,
}

impl From<AuthRoleArg> for AuthRole {
    fn from(role: AuthRoleArg) -> Self {
        match role {
            AuthRoleArg::Admin => Self::Admin,
            AuthRoleArg::Operator => Self::Operator,
            AuthRoleArg::Viewer => Self::Viewer,
            AuthRoleArg::Agent => Self::Agent,
        }
    }
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

#[derive(Debug, Subcommand)]
enum CompatCommand {
    /// Normalize and compare one exported behavior fixture.
    Compare { fixture: PathBuf },
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
    /// Resolve this non-secret ID from an encrypted vault before fetching.
    #[arg(long)]
    credential_id: Option<String>,
    /// Encrypted SCM credential vault used with --credential-id.
    #[arg(long, requires = "credential_id")]
    credentials_file: Option<PathBuf>,
    /// Private passphrase file used with --credential-id.
    #[arg(long, requires = "credential_id")]
    credentials_passphrase_file: Option<PathBuf>,
    #[arg(long = "param", value_name = "NAME=VALUE")]
    parameters: Vec<String>,
    /// Queue priority from -100 to 100; higher values run first.
    #[arg(long, default_value_t = 0)]
    priority: i32,
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
    /// Exact local parent directory used for one build workspace at a time.
    #[arg(long, default_value_os_t = default_agent_workspace_root())]
    workspace_root: PathBuf,
}

#[derive(Debug, Subcommand)]
enum ScmCommand {
    /// Print the current Git revision, branch, remote, and worktree state.
    Inspect { repository: PathBuf },
    /// Optionally fetch, checkout, and clean before printing the final state.
    Prepare {
        repository: PathBuf,
        /// Project context used to authorize a scoped credential.
        #[arg(long)]
        project: Option<String>,
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
        /// Resolve this non-secret ID from an encrypted vault before fetching.
        #[arg(long)]
        credential_id: Option<String>,
        /// Encrypted SCM credential vault used with --credential-id.
        #[arg(long, requires = "credential_id")]
        credentials_file: Option<PathBuf>,
        /// Private passphrase file used with --credential-id.
        #[arg(long, requires = "credential_id")]
        credentials_passphrase_file: Option<PathBuf>,
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
        Command::Artifact { command } => manage_artifacts(&cli.data_dir, command)?,
        Command::Retry {
            project,
            build,
            parameters,
        } => retry_project(&cli.data_dir, &project, build, parameters).await?,
        Command::Server {
            bind,
            token_file,
            auth_policy_file,
            webhook_secret_file,
            github_webhook_secret_file,
            gitlab_webhook_secret_file,
            github_webhook_credential_id,
            gitlab_webhook_credential_id,
            credentials_file,
            credentials_passphrase_file,
            credentials_keychain_account,
            extension_manifest_dir,
            allowed_origins,
        } => {
            let auth_token = token_file.as_deref().map(read_auth_token).transpose()?;
            let webhook_secret = webhook_secret_file
                .as_deref()
                .map(read_webhook_secret)
                .transpose()?;
            let github_webhook_secret = github_webhook_secret_file
                .as_deref()
                .map(|path| read_private_value(path, "GitHub webhook secret"))
                .transpose()?;
            let gitlab_webhook_secret = gitlab_webhook_secret_file
                .as_deref()
                .map(|path| read_private_value(path, "GitLab webhook secret"))
                .transpose()?;
            let credentials_passphrase = credentials_passphrase_file
                .as_deref()
                .map(|path| read_private_value(path, "credential vault passphrase"))
                .transpose()?;
            rivet_server::serve_with_config(
                cli.data_dir.join("rivet.db"),
                rivet_server::ServerConfig {
                    bind,
                    auth_token,
                    auth_policy_file,
                    webhook_secret,
                    github_webhook_secret,
                    gitlab_webhook_secret,
                    github_webhook_credential_id,
                    gitlab_webhook_credential_id,
                    credentials_file,
                    credentials_passphrase,
                    credentials_keychain_account,
                    extension_manifest_dir,
                    allowed_origins,
                },
            )
            .await?
        }
        Command::Scm { command } => inspect_scm(command).await?,
        Command::Schedule { command } => manage_schedule(&cli.data_dir, command)?,
        Command::Credential { command } => manage_credentials(&cli.data_dir, command)?,
        Command::Auth { command } => manage_auth(command)?,
        Command::Cache { command } => manage_cache(&cli.data_dir, command)?,
        Command::Analyze { command } => analyze_file(command)?,
        Command::Compat { command } => compare_compatibility(command)?,
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

fn compare_compatibility(command: CompatCommand) -> Result<(), Box<dyn std::error::Error>> {
    match command {
        CompatCommand::Compare { fixture } => {
            let fixture = BehaviorFixture::from_json(&fs::read(&fixture)?)?;
            let report = compare_fixture(&fixture)?;
            println!("{}", serde_json::to_string_pretty(&report)?);
            if !report.matches {
                return Err("compatibility fixture contains semantic differences".into());
            }
        }
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AgentSessionResult {
    Stopped,
    Disconnected,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AgentAssignmentResult {
    Complete { sequence: u64 },
    Stopped,
}

struct AgentShutdownSignal {
    #[cfg(unix)]
    terminate: tokio::signal::unix::Signal,
}

impl AgentShutdownSignal {
    fn new() -> Result<Self, std::io::Error> {
        Ok(Self {
            #[cfg(unix)]
            terminate: tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?,
        })
    }

    async fn recv(&mut self) {
        #[cfg(unix)]
        {
            tokio::select! {
                _ = self.terminate.recv() => {}
                _ = tokio::signal::ctrl_c() => {}
            }
        }
        #[cfg(not(unix))]
        {
            let _ = tokio::signal::ctrl_c().await;
        }
    }
}

async fn stop_agent_execution<F>(
    mut execution: Pin<&mut F>,
    event_receiver: &mut mpsc::Receiver<BuildEvent>,
    cancellation: &CancellationToken,
) where
    F: Future<Output = Result<BuildStatus, rivet_runner::RunnerError>>,
{
    cancellation.cancel();
    loop {
        tokio::select! {
            result = execution.as_mut() => {
                let _ = result;
                break;
            }
            event = event_receiver.recv() => {
                if event.is_none() {
                    break;
                }
            }
        }
    }
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
                eprintln!("Stopping agent...");
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
    let mut shutdown = AgentShutdownSignal::new()?;
    let mut running = Vec::new();
    let mut session_result = AgentSessionResult::Disconnected;
    loop {
        tokio::select! {
            _ = shutdown.recv() => {
                eprintln!("Stopping agent...");
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
                        running: running.clone(),
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
                    message => {
                        let decoded = decode_agent_socket_message(message)?;
                        match &decoded {
                            AgentMessage::HeartbeatAck { .. } => {}
                            AgentMessage::Error { code, message, .. } => {
                                return Err(format!("agent connection error ({code}): {message}").into());
                            }
                            AgentMessage::Assign { build_id, .. } => {
                                let build_id = *build_id;
                                running.push(build_id);
                                let assignment = run_agent_assignment(
                                    &mut socket,
                                    registration,
                                    session_id,
                                    sequence,
                                    decoded,
                                    &args.workspace_root,
                                ).await;
                                match assignment {
                                    Ok(AgentAssignmentResult::Complete { sequence: next }) => {
                                        sequence = next;
                                        running.retain(|running_build| *running_build != build_id);
                                    }
                                    Ok(AgentAssignmentResult::Stopped) => {
                                        session_result = AgentSessionResult::Stopped;
                                        break;
                                    }
                                    Err(error) => {
                                        running.retain(|running_build| *running_build != build_id);
                                        send_agent_socket_message(
                                            &mut socket,
                                            AgentMessage::Error {
                                                protocol_version: PROTOCOL_VERSION,
                                                build_id: Some(build_id),
                                                code: "assignment_failed".into(),
                                                message: error.to_string(),
                                            },
                                        ).await?;
                                    }
                                }
                            }
                            _ => {}
                        }
                    },
                }
            }
        }
    }
    let _ = socket.close(None).await;
    Ok(session_result)
}

async fn run_agent_assignment(
    socket: &mut AgentSocket,
    registration: &AgentRegistration,
    session_id: uuid::Uuid,
    mut sequence: u64,
    assignment: AgentMessage,
    workspace_root: &Path,
) -> Result<AgentAssignmentResult, Box<dyn std::error::Error>> {
    let AgentMessage::Assign {
        build_id,
        plan,
        pipeline,
        parameters,
        workspace,
        ..
    } = assignment
    else {
        return Err("agent assignment handler received a non-assignment message".into());
    };
    workspace.validate()?;
    fs::create_dir_all(workspace_root)?;
    let build_workspace = workspace_root.join(build_id.to_string());
    remove_exact_agent_path(&build_workspace)?;
    fs::create_dir(&build_workspace)?;
    let archive_path = workspace_root.join(format!("{build_id}.tar"));
    remove_exact_agent_path(&archive_path)?;

    send_agent_socket_message(
        socket,
        AgentMessage::AssignmentAccepted {
            protocol_version: PROTOCOL_VERSION,
            build_id,
        },
    )
    .await?;

    let transfer_result = receive_workspace_archive(
        socket,
        registration,
        session_id,
        &mut sequence,
        build_id,
        &workspace,
        &archive_path,
    )
    .await;
    if let Err(error) = transfer_result {
        let _ = remove_exact_agent_path(&build_workspace);
        let _ = remove_exact_agent_path(&archive_path);
        return Err(error);
    }
    extract_workspace_archive(&archive_path, &build_workspace, workspace.file_count)?;
    remove_exact_agent_path(&archive_path)?;
    send_agent_socket_message(
        socket,
        AgentMessage::WorkspaceReady {
            protocol_version: PROTOCOL_VERSION,
            build_id,
        },
    )
    .await?;

    let mut execution_plan = plan;
    let mut execution_pipeline = pipeline;
    clear_remote_requirements(&mut execution_plan, &mut execution_pipeline);
    let cancellation = CancellationToken::new();
    let (event_sender, mut event_receiver) = mpsc::channel(512);
    let mut shutdown = AgentShutdownSignal::new()?;
    let execution = execute_pipeline_with_parameters(
        &execution_plan,
        &execution_pipeline,
        &build_workspace,
        &parameters,
        cancellation.clone(),
        event_sender,
    );
    tokio::pin!(execution);
    let mut terminal_event_sent = false;
    let mut heartbeat = tokio::time::interval(Duration::from_secs(10));
    loop {
        tokio::select! {
            _ = shutdown.recv() => {
                stop_agent_execution(
                    execution.as_mut(),
                    &mut event_receiver,
                    &cancellation,
                ).await;
                let _ = remove_exact_agent_path(&build_workspace);
                return Ok(AgentAssignmentResult::Stopped);
            }
            _ = heartbeat.tick() => {
                sequence += 1;
                send_agent_socket_message(
                    socket,
                    AgentMessage::Heartbeat(AgentHeartbeat {
                        protocol_version: PROTOCOL_VERSION,
                        agent_id: registration.agent_id,
                        session_id,
                        sequence,
                        running: vec![build_id],
                        sent_at: Utc::now(),
                    }),
                ).await?;
            }
            message = event_receiver.recv() => {
                if let Some(event) = message {
                    terminal_event_sent |= send_execution_event(
                        socket,
                        event,
                        build_id,
                        &execution_pipeline,
                        &build_workspace,
                    ).await?;
                }
            }
            message = next_agent_message(socket) => {
                let message = match message {
                    Ok(Some(message)) => message,
                    Ok(None) => {
                        stop_agent_execution(
                            execution.as_mut(),
                            &mut event_receiver,
                            &cancellation,
                        ).await;
                        let _ = remove_exact_agent_path(&build_workspace);
                        return Err("Rivet server closed the agent connection during a build".into());
                    }
                    Err(error) => {
                        stop_agent_execution(
                            execution.as_mut(),
                            &mut event_receiver,
                            &cancellation,
                        ).await;
                        let _ = remove_exact_agent_path(&build_workspace);
                        return Err(error);
                    }
                };
                match message {
                    AgentMessage::Cancel { build_id: cancelled, .. } if cancelled == build_id => {
                        cancellation.cancel();
                    }
                    AgentMessage::HeartbeatAck { .. } => {}
                    AgentMessage::Error { code, message, .. } => {
                        stop_agent_execution(
                            execution.as_mut(),
                            &mut event_receiver,
                            &cancellation,
                        ).await;
                        let _ = remove_exact_agent_path(&build_workspace);
                        return Err(format!("Rivet server rejected build {build_id} ({code}): {message}").into());
                    }
                    _ => {
                        stop_agent_execution(
                            execution.as_mut(),
                            &mut event_receiver,
                            &cancellation,
                        ).await;
                        let _ = remove_exact_agent_path(&build_workspace);
                        return Err(format!("unexpected server message while running build {build_id}").into());
                    }
                }
            }
            result = &mut execution => {
                while let Some(event) = event_receiver.recv().await {
                    terminal_event_sent |= send_execution_event(
                        socket,
                        event,
                        build_id,
                        &execution_pipeline,
                        &build_workspace,
                    ).await?;
                }
                if let Err(error) = result {
                    if !terminal_event_sent {
                        send_agent_socket_message(
                            socket,
                            AgentMessage::Error {
                                protocol_version: PROTOCOL_VERSION,
                                build_id: Some(build_id),
                                code: "execution_failed".into(),
                                message: error.to_string(),
                            },
                        ).await?;
                    }
                }
                remove_exact_agent_path(&build_workspace)?;
                return Ok(AgentAssignmentResult::Complete { sequence });
            }
        }
    }
}

async fn send_execution_event(
    socket: &mut AgentSocket,
    event: BuildEvent,
    build_id: uuid::Uuid,
    pipeline: &Pipeline,
    workspace: &Path,
) -> Result<bool, Box<dyn std::error::Error>> {
    if matches!(
        event,
        BuildEvent::BuildFinished {
            status: BuildStatus::Passed,
            ..
        }
    ) && !pipeline.artifacts.is_empty()
    {
        if let Err(error) = send_artifact_archive(socket, build_id, pipeline, workspace).await {
            send_agent_socket_message(
                socket,
                AgentMessage::Error {
                    protocol_version: PROTOCOL_VERSION,
                    build_id: Some(build_id),
                    code: "artifact_collection_failed".into(),
                    message: error.to_string(),
                },
            )
            .await?;
            return Ok(false);
        }
    }
    let terminal = matches!(event, BuildEvent::BuildFinished { .. });
    send_agent_socket_message(
        socket,
        AgentMessage::Event {
            protocol_version: PROTOCOL_VERSION,
            event,
        },
    )
    .await?;
    Ok(terminal)
}

async fn send_artifact_archive(
    socket: &mut AgentSocket,
    build_id: uuid::Uuid,
    pipeline: &Pipeline,
    workspace: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let pipeline_for_archive = pipeline.clone();
    let workspace_for_archive = workspace.to_path_buf();
    let archive = match tokio::task::spawn_blocking(move || {
        archive_artifacts(&pipeline_for_archive, &workspace_for_archive)
    })
    .await
    {
        Ok(Ok(archive)) => archive,
        Ok(Err(error)) => return Err(std::io::Error::other(error).into()),
        Err(error) => return Err(std::io::Error::other(error.to_string()).into()),
    };
    send_agent_socket_message(
        socket,
        AgentMessage::ArtifactsReady {
            protocol_version: PROTOCOL_VERSION,
            build_id,
            transfer: archive.transfer,
        },
    )
    .await?;
    for (sequence, data) in archive.bytes.chunks(MAX_WORKSPACE_CHUNK_BYTES).enumerate() {
        let sequence =
            u32::try_from(sequence).map_err(|_| "artifact archive has too many chunks")?;
        send_agent_socket_message(
            socket,
            AgentMessage::ArtifactChunk {
                protocol_version: PROTOCOL_VERSION,
                build_id,
                sequence,
                data: data.to_vec(),
            },
        )
        .await?;
    }
    Ok(())
}

struct ArtifactArchive {
    bytes: Vec<u8>,
    transfer: WorkspaceTransfer,
}

fn archive_artifacts(pipeline: &Pipeline, workspace: &Path) -> Result<ArtifactArchive, String> {
    let root = fs::canonicalize(workspace).map_err(|error| error.to_string())?;
    if !root.is_dir() {
        return Err(format!(
            "artifact workspace is not a directory: {}",
            root.display()
        ));
    }
    let mut files = BTreeMap::new();
    for specification in &pipeline.artifacts {
        let mut matcher_builder = GlobSetBuilder::new();
        for pattern in &specification.paths {
            matcher_builder.add(Glob::new(pattern).map_err(|error| error.to_string())?);
        }
        let matcher = matcher_builder.build().map_err(|error| error.to_string())?;
        let mut found = false;
        for entry in WalkDir::new(&root)
            .follow_links(false)
            .into_iter()
            .filter_entry(|entry| !is_internal_artifact_entry(entry, &root))
        {
            let entry = entry.map_err(|error| error.to_string())?;
            if !entry.file_type().is_file() {
                continue;
            }
            let relative = entry
                .path()
                .strip_prefix(&root)
                .map_err(|error| error.to_string())?
                .to_string_lossy()
                .replace(std::path::MAIN_SEPARATOR, "/");
            if matcher.is_match(&relative) {
                found = true;
                files.insert(relative, entry.path().to_path_buf());
            }
        }
        if !found && !specification.allow_empty {
            return Err(format!(
                "artifact {:?} matched no files for pattern {:?}",
                specification.name,
                specification.paths.join(", ")
            ));
        }
    }
    if files.len() > MAX_WORKSPACE_FILES as usize {
        return Err("artifact archive contains too many files".into());
    }
    let mut builder = tar::Builder::new(Vec::new());
    for (relative, path) in &files {
        builder
            .append_path_with_name(path, relative)
            .map_err(|error| error.to_string())?;
    }
    let bytes = builder.into_inner().map_err(|error| error.to_string())?;
    if bytes.len() as u64 > MAX_WORKSPACE_BYTES {
        return Err("artifact archive exceeds the protocol size limit".into());
    }
    let transfer = WorkspaceTransfer {
        total_bytes: bytes.len() as u64,
        file_count: files.len() as u32,
        sha256: hex::encode(Sha256::digest(&bytes)),
    };
    transfer.validate().map_err(|error| error.to_string())?;
    Ok(ArtifactArchive { bytes, transfer })
}

fn is_internal_artifact_entry(entry: &DirEntry, root: &Path) -> bool {
    if entry.path() == root {
        return false;
    }
    let Some(first) = entry
        .path()
        .strip_prefix(root)
        .ok()
        .and_then(|path| path.components().next())
    else {
        return false;
    };
    matches!(first.as_os_str().to_str(), Some(".git" | ".rivet"))
}

async fn receive_workspace_archive(
    socket: &mut AgentSocket,
    registration: &AgentRegistration,
    session_id: uuid::Uuid,
    sequence: &mut u64,
    build_id: uuid::Uuid,
    transfer: &rivet_agent_protocol::WorkspaceTransfer,
    archive_path: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut file = fs::File::create(archive_path)?;
    let mut digest = Sha256::new();
    let mut received_bytes = 0_u64;
    let mut expected_sequence = 0_u32;
    let mut heartbeat = tokio::time::interval(Duration::from_secs(10));
    loop {
        if received_bytes == transfer.total_bytes {
            break;
        }
        tokio::select! {
            stop = tokio::signal::ctrl_c() => {
                stop?;
                return Err("agent stopped while receiving a workspace".into());
            }
            _ = heartbeat.tick() => {
                *sequence += 1;
                send_agent_socket_message(
                    socket,
                    AgentMessage::Heartbeat(AgentHeartbeat {
                        protocol_version: PROTOCOL_VERSION,
                        agent_id: registration.agent_id,
                        session_id,
                        sequence: *sequence,
                        running: vec![build_id],
                        sent_at: Utc::now(),
                    }),
                ).await?;
            }
            message = next_agent_message(socket) => {
                let Some(message) = message? else {
                    return Err("Rivet server closed the agent connection during workspace transfer".into());
                };
                match message {
                    AgentMessage::WorkspaceChunk { build_id: chunk_build, sequence: chunk_sequence, data, .. }
                        if chunk_build == build_id => {
                        if chunk_sequence != expected_sequence {
                            return Err(format!("workspace chunk sequence {chunk_sequence} arrived; expected {expected_sequence}").into());
                        }
                        if data.len() > MAX_WORKSPACE_CHUNK_BYTES {
                            return Err("workspace chunk exceeds the protocol limit".into());
                        }
                        received_bytes = received_bytes
                            .checked_add(data.len() as u64)
                            .ok_or("workspace transfer size overflow")?;
                        if received_bytes > transfer.total_bytes {
                            return Err("workspace transfer exceeded its declared size".into());
                        }
                        digest.update(&data);
                        file.write_all(&data)?;
                        expected_sequence = expected_sequence.checked_add(1).ok_or("workspace chunk sequence overflow")?;
                    }
                    AgentMessage::Cancel { build_id: cancelled, .. } if cancelled == build_id => {
                        return Err("workspace transfer cancelled by the server".into());
                    }
                    AgentMessage::HeartbeatAck { .. } => {}
                    AgentMessage::Error { code, message, .. } => {
                        return Err(format!("Rivet server rejected workspace transfer ({code}): {message}").into());
                    }
                    _ => return Err(format!("unexpected server message while receiving workspace {build_id}").into()),
                }
            }
        }
    }
    file.flush()?;
    let actual = hex::encode(digest.finalize());
    if actual != transfer.sha256 {
        return Err(format!(
            "workspace checksum mismatch: received {actual}, expected {}",
            transfer.sha256
        )
        .into());
    }
    Ok(())
}

fn extract_workspace_archive(
    archive_path: &Path,
    destination: &Path,
    expected_entries: u32,
) -> Result<(), Box<dyn std::error::Error>> {
    let file = fs::File::open(archive_path)?;
    let mut archive = Archive::new(file);
    let mut entries = 0_u32;
    for entry in archive.entries()? {
        let mut entry = entry?;
        entries = entries
            .checked_add(1)
            .ok_or("workspace entry count overflow")?;
        if entries > rivet_agent_protocol::MAX_WORKSPACE_FILES {
            return Err("workspace archive contains too many entries".into());
        }
        let path = entry.path()?.into_owned();
        validate_archive_path(&path)?;
        let target = destination.join(&path);
        if entry.header().entry_type().is_dir() {
            fs::create_dir_all(&target)?;
        } else if entry.header().entry_type().is_file() {
            if let Some(parent) = target.parent() {
                fs::create_dir_all(parent)?;
            }
            entry.unpack(&target)?;
        } else {
            return Err(format!(
                "workspace archive contains unsupported entry: {}",
                path.display()
            )
            .into());
        }
    }
    if entries != expected_entries {
        return Err(format!(
            "workspace archive entry count {entries} does not match declared {expected_entries}"
        )
        .into());
    }
    Ok(())
}

fn validate_archive_path(path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    if path.as_os_str().is_empty()
        || path.is_absolute()
        || path.components().any(|component| {
            matches!(
                component,
                std::path::Component::CurDir
                    | std::path::Component::ParentDir
                    | std::path::Component::Prefix(_)
            )
        })
    {
        return Err(format!("workspace archive path is unsafe: {}", path.display()).into());
    }
    Ok(())
}

fn remove_exact_agent_path(path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let Ok(metadata) = fs::symlink_metadata(path) else {
        return Ok(());
    };
    if metadata.file_type().is_symlink() {
        return Err(format!(
            "refusing to remove symlink workspace path: {}",
            path.display()
        )
        .into());
    }
    if metadata.is_dir() {
        fs::remove_dir_all(path)?;
    } else {
        fs::remove_file(path)?;
    }
    Ok(())
}

fn clear_remote_requirements(plan: &mut rivet_core::ExecutionPlan, pipeline: &mut Pipeline) {
    for stage in &mut plan.stages {
        for step in &mut stage.steps {
            step.definition.agent = None;
        }
    }
    for stage in &mut pipeline.stages {
        for step in &mut stage.steps {
            step.agent = None;
        }
    }
}

async fn next_agent_message(
    socket: &mut AgentSocket,
) -> Result<Option<AgentMessage>, Box<dyn std::error::Error>> {
    loop {
        let Some(message) = socket.next().await else {
            return Ok(None);
        };
        match message? {
            AgentSocketMessage::Ping(payload) => {
                socket.send(AgentSocketMessage::Pong(payload)).await?;
            }
            AgentSocketMessage::Pong(_) => {}
            AgentSocketMessage::Close(_) => return Ok(None),
            message => return decode_agent_socket_message(message).map(Some),
        }
    }
}

async fn send_agent_socket_message(
    socket: &mut AgentSocket,
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

fn default_agent_workspace_root() -> PathBuf {
    std::env::temp_dir().join("rivet-agent-workspaces")
}

async fn inspect_scm(command: ScmCommand) -> Result<(), Box<dyn std::error::Error>> {
    let (repository_path, options, credential) = match command {
        ScmCommand::Inspect { repository } => (repository, None, None),
        ScmCommand::Prepare {
            repository,
            project,
            remote,
            fetch,
            revision,
            clean,
            clean_ignored,
            credential_id,
            credentials_file,
            credentials_passphrase_file,
        } => (
            repository,
            Some({
                if credential_id.is_some() && !fetch {
                    return Err("an SCM credential requires --fetch".into());
                }
                GitPrepareOptions {
                    remote,
                    fetch,
                    revision,
                    fetch_ref: None,
                    clean,
                    clean_ignored,
                    credential_id: credential_id.clone(),
                }
            }),
            load_git_credential(
                credential_id.as_deref(),
                credentials_file.as_deref(),
                credentials_passphrase_file.as_deref(),
                project.as_deref(),
            )?,
        ),
    };
    let repository = GitRepository::open(&repository_path).await?;
    let snapshot = match options {
        Some(options) => {
            repository
                .prepare_with_credential(&options, credential.as_ref())
                .await?
        }
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

fn manage_auth(command: AuthCommand) -> Result<(), Box<dyn std::error::Error>> {
    let AuthCommand::Token { command } = command;
    match command {
        AuthTokenCommand::Create {
            id,
            role,
            projects,
            policy_file,
            token_file,
            expires_at,
        } => {
            if policy_file == token_file {
                return Err("policy file and token file must be different paths".into());
            }
            let mut document = load_auth_policy_for_create(&policy_file)?;
            let token = generate_token()?;
            let role: AuthRole = role.into();
            let expires_at = parse_token_expiry(expires_at.as_deref())?;
            document.tokens.push(ApiTokenRecord {
                id: id.clone(),
                sha256: token_digest(&token),
                role,
                projects,
                expires_at,
            });
            AuthPolicy::from_document(document.clone())?;

            write_private_atomic(&token_file, token.as_bytes(), false, "token file")?;
            if let Err(error) = write_auth_policy(&policy_file, &document) {
                let _ = fs::remove_file(&token_file);
                return Err(error);
            }
            println!("Created {role:?} token {id}");
            println!("Token saved to {}", token_file.display());
        }
        AuthTokenCommand::List { policy_file } => {
            let document = load_auth_policy_document(&policy_file)?;
            for token in document.tokens {
                let projects = if token.projects.is_empty() {
                    "*".to_owned()
                } else {
                    token.projects.join(",")
                };
                let expires_at = token
                    .expires_at
                    .map(|value| value.to_rfc3339())
                    .unwrap_or_else(|| "never".to_owned());
                println!(
                    "{}\t{:?}\t{}\t{}",
                    token.id, token.role, projects, expires_at
                );
            }
        }
        AuthTokenCommand::Revoke { id, policy_file } => {
            let mut document = load_auth_policy_document(&policy_file)?;
            let original_len = document.tokens.len();
            document.tokens.retain(|token| token.id != id);
            if document.tokens.len() == original_len {
                return Err(format!("authentication token not found: {id}").into());
            }
            if document.tokens.is_empty() {
                return Err(
                    "cannot revoke the last authentication token; create a replacement first"
                        .into(),
                );
            }
            AuthPolicy::from_document(document.clone())?;
            write_auth_policy(&policy_file, &document)?;
            println!("Revoked token {id}");
        }
    }
    Ok(())
}

fn manage_cache(data_dir: &Path, command: CacheCommand) -> Result<(), Box<dyn std::error::Error>> {
    match command {
        CacheCommand::Prune { max_bytes } => {
            let storage = open_storage(data_dir)?;
            let result = CacheStore::new(storage.cache_root()).prune(max_bytes)?;
            println!(
                "removed {} entr{} ({} bytes); {} bytes remain",
                result.removed_entries,
                if result.removed_entries == 1 {
                    "y"
                } else {
                    "ies"
                },
                result.removed_bytes,
                result.remaining_bytes
            );
        }
    }
    Ok(())
}

fn manage_artifacts(
    data_dir: &Path,
    command: ArtifactCommand,
) -> Result<(), Box<dyn std::error::Error>> {
    match command {
        ArtifactCommand::Prune { max_bytes } => {
            let storage = open_storage(data_dir)?;
            let result = storage.prune_artifacts(max_bytes)?;
            println!(
                "removed {} entr{} ({} bytes); {} bytes remain",
                result.removed_entries,
                if result.removed_entries == 1 {
                    "y"
                } else {
                    "ies"
                },
                result.removed_bytes,
                result.remaining_bytes
            );
        }
    }
    Ok(())
}

fn parse_token_expiry(
    value: Option<&str>,
) -> Result<Option<DateTime<Utc>>, Box<dyn std::error::Error>> {
    let Some(value) = value else {
        return Ok(None);
    };
    let expiry = DateTime::parse_from_rfc3339(value)
        .map_err(|error| format!("invalid token expiry {value:?}: {error}"))?
        .with_timezone(&Utc);
    if expiry <= Utc::now() {
        return Err("token expiry must be in the future".into());
    }
    Ok(Some(expiry))
}

fn load_auth_policy_for_create(
    path: &Path,
) -> Result<AuthPolicyDocument, Box<dyn std::error::Error>> {
    match fs::symlink_metadata(path) {
        Ok(_) => load_auth_policy_document(path),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            Ok(AuthPolicyDocument::empty())
        }
        Err(error) => Err(error.into()),
    }
}

fn load_auth_policy_document(
    path: &Path,
) -> Result<AuthPolicyDocument, Box<dyn std::error::Error>> {
    let bytes = read_private_bytes(path, "authentication policy")?;
    let document: AuthPolicyDocument = serde_json::from_slice(&bytes)?;
    AuthPolicy::from_document(document.clone())?;
    Ok(document)
}

fn write_auth_policy(
    path: &Path,
    document: &AuthPolicyDocument,
) -> Result<(), Box<dyn std::error::Error>> {
    AuthPolicy::from_document(document.clone())?;
    let mut bytes = serde_json::to_vec_pretty(document)?;
    bytes.push(b'\n');
    write_private_atomic(path, &bytes, true, "authentication policy")
}

fn write_private_atomic(
    path: &Path,
    bytes: &[u8],
    replace_existing: bool,
    label: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    if let Ok(metadata) = fs::symlink_metadata(path) {
        if metadata.file_type().is_symlink() {
            return Err(format!("{label} must not be a symbolic link: {}", path.display()).into());
        }
        if !metadata.is_file() {
            return Err(format!("{label} path must be a regular file: {}", path.display()).into());
        }
        if !replace_existing {
            return Err(format!("{label} already exists: {}", path.display()).into());
        }
    }
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent)?;
    let parent_metadata = fs::symlink_metadata(parent)?;
    if parent_metadata.file_type().is_symlink() {
        return Err(format!(
            "{label} parent must not be a symbolic link: {}",
            parent.display()
        )
        .into());
    }
    if !parent_metadata.is_dir() {
        return Err(format!("{label} parent must be a directory: {}", parent.display()).into());
    }
    let filename = path
        .file_name()
        .and_then(|value| value.to_str())
        .ok_or_else(|| format!("{label} path has no valid filename"))?;
    let temporary = parent.join(format!(".{filename}.{}.tmp", uuid::Uuid::new_v4()));
    let result = (|| -> Result<(), std::io::Error> {
        let mut options = fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options.open(&temporary)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        drop(file);
        if replace_existing {
            #[cfg(windows)]
            if path.exists() {
                fs::remove_file(path)?;
            }
            fs::rename(&temporary, path)?;
        } else {
            // A hard-link install fails atomically if another process created
            // the destination after the initial metadata check; rename would
            // silently replace that file on Unix.
            fs::hard_link(&temporary, path)?;
            fs::remove_file(&temporary)?;
        }
        #[cfg(unix)]
        if let Ok(directory) = fs::File::open(parent) {
            let _ = directory.sync_all();
        }
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result.map_err(|error| error.into())
}

fn manage_credentials(
    data_dir: &Path,
    command: CredentialCommand,
) -> Result<(), Box<dyn std::error::Error>> {
    match command {
        CredentialCommand::Set {
            id,
            username,
            projects,
            secret_file,
            passphrase_file,
            vault_file,
        } => {
            let vault_path = vault_file.unwrap_or_else(|| data_dir.join("credentials.vault"));
            let passphrase = read_private_value(&passphrase_file, "credential vault passphrase")?;
            let secret = read_private_value(&secret_file, "credential secret")?;
            let mut vault = CredentialVault::open_or_create(&vault_path, passphrase)?;
            vault.set_http_basic_for_projects(id.clone(), username, secret, projects)?;
            println!("Stored credential {id} in {}", vault.path().display());
        }
        CredentialCommand::KeychainSet {
            account,
            passphrase_file,
        } => {
            let passphrase = read_private_value(&passphrase_file, "credential vault passphrase")?;
            CredentialKeychain::rivet().set_passphrase(&account, &passphrase)?;
            println!("Stored the vault passphrase in the OS keychain account {account}");
        }
        CredentialCommand::KeychainRemove { account } => {
            CredentialKeychain::rivet().delete_passphrase(&account)?;
            println!("Removed the vault passphrase from the OS keychain account {account}");
        }
        CredentialCommand::List {
            passphrase_file,
            vault_file,
        } => {
            let vault_path = vault_file.unwrap_or_else(|| data_dir.join("credentials.vault"));
            let passphrase = read_private_value(&passphrase_file, "credential vault passphrase")?;
            let vault = CredentialVault::open(&vault_path, passphrase)?;
            for credential in vault.list() {
                let scope = if credential.projects.is_empty() {
                    "*".to_owned()
                } else {
                    credential.projects.join(",")
                };
                println!("{}\t{}\t{}", credential.id, credential.username, scope);
            }
        }
        CredentialCommand::Remove {
            id,
            passphrase_file,
            vault_file,
        } => {
            let vault_path = vault_file.unwrap_or_else(|| data_dir.join("credentials.vault"));
            let passphrase = read_private_value(&passphrase_file, "credential vault passphrase")?;
            let mut vault = CredentialVault::open(&vault_path, passphrase)?;
            vault.remove(&id)?;
            println!("Removed credential {id}");
        }
    }
    Ok(())
}

fn read_private_value(path: &Path, label: &str) -> Result<String, Box<dyn std::error::Error>> {
    let bytes = read_private_bytes(path, label)?;
    let value = String::from_utf8(bytes)?.trim().to_owned();
    if value.is_empty() {
        return Err(format!("{label} file is empty: {}", path.display()).into());
    }
    Ok(value)
}

fn read_private_bytes(path: &Path, label: &str) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() {
        return Err(format!(
            "{label} file must not be a symbolic link: {}",
            path.display()
        )
        .into());
    }
    if !metadata.is_file() {
        return Err(format!("{label} path must be a regular file: {}", path.display()).into());
    }
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
    let bytes = fs::read(path)?;
    if bytes.is_empty() {
        return Err(format!("{label} file is empty: {}", path.display()).into());
    }
    Ok(bytes)
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
        || args.credential_id.is_some()
    {
        Some(GitPrepareOptions {
            remote: args.remote,
            fetch: args.fetch,
            revision: args.revision,
            fetch_ref: None,
            clean: args.clean,
            clean_ignored: args.clean_ignored,
            credential_id: args.credential_id.clone(),
        })
    } else {
        None
    };
    if args.credential_id.is_some() && !args.fetch {
        return Err("an SCM credential requires --fetch".into());
    }
    let credential = load_git_credential(
        args.credential_id.as_deref(),
        args.credentials_file.as_deref(),
        args.credentials_passphrase_file.as_deref(),
        Some(&args.project),
    )?;
    let supplied_parameters = parse_parameters(&args.parameters)?;
    if !(MIN_QUEUE_PRIORITY..=MAX_QUEUE_PRIORITY).contains(&args.priority) {
        return Err(format!(
            "build priority must be between {MIN_QUEUE_PRIORITY} and {MAX_QUEUE_PRIORITY}"
        )
        .into());
    }
    run_project_with_options(
        data_dir,
        &args.project,
        scm,
        supplied_parameters,
        credential,
        args.priority,
    )
    .await
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
    run_project_with_options(data_dir, name, None, supplied_parameters, None, 0).await
}

async fn run_project_with_options(
    data_dir: &Path,
    name: &str,
    scm: Option<GitPrepareOptions>,
    supplied_parameters: BTreeMap<String, String>,
    credential: Option<GitHttpCredential>,
    priority: i32,
) -> Result<(), Box<dyn std::error::Error>> {
    let storage = open_storage(data_dir)?;
    let project = storage
        .get_project_by_name(name)?
        .ok_or_else(|| format!("project not found: {name}"))?;
    let source =
        capture_source_snapshot(&project.repository_path, scm.as_ref(), credential.as_ref())
            .await?;
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
        .enqueue_with_priority_and_parameters(
            plan,
            pipeline,
            PathBuf::from(project.repository_path.clone()),
            parameters,
            priority,
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
    credential: Option<&GitHttpCredential>,
) -> Result<Option<SourceSnapshot>, ScmError> {
    match GitRepository::open(path).await {
        Ok(repository) => {
            let snapshot = match prepare {
                Some(options) => {
                    repository
                        .prepare_with_credential(options, credential)
                        .await?
                }
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

fn load_git_credential(
    credential_id: Option<&str>,
    credentials_file: Option<&Path>,
    credentials_passphrase_file: Option<&Path>,
    project: Option<&str>,
) -> Result<Option<GitHttpCredential>, Box<dyn std::error::Error>> {
    match (credential_id, credentials_file, credentials_passphrase_file) {
        (None, None, None) => Ok(None),
        (None, Some(_), _) | (None, _, Some(_)) => Err(
            "--credentials-file and --credentials-passphrase-file require --credential-id".into(),
        ),
        (Some(_), None, _) | (Some(_), _, None) => Err(
            "--credential-id requires --credentials-file and --credentials-passphrase-file".into(),
        ),
        (Some(id), Some(vault_path), Some(passphrase_path)) => {
            let passphrase = read_private_value(passphrase_path, "credential vault passphrase")?;
            let vault = CredentialVault::open(vault_path, passphrase)?;
            let credential = match project {
                Some(project) => vault.get_for_project(id, project)?,
                None => {
                    let credential = vault.get(id)?;
                    if credential.projects().next().is_some() {
                        return Err(
                            "--project is required when the credential has project scope".into(),
                        );
                    }
                    credential
                }
            };
            Ok(Some(GitHttpCredential::new(
                credential.username(),
                credential.secret(),
            )?))
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
