use chrono::{DateTime, Utc};
use clap::{Args, Parser, Subcommand, ValueEnum};
use futures_util::{SinkExt, StreamExt};
use globset::{Glob, GlobSetBuilder};
use rivet_agent_protocol::{
    AgentCapabilities, AgentHeartbeat, AgentId, AgentMessage, AgentRegistration,
    AgentTransportMessage, MAX_WORKSPACE_BYTES, MAX_WORKSPACE_CHUNK_BYTES, MAX_WORKSPACE_FILES,
    PROTOCOL_VERSION, WorkspaceTransfer,
};
use rivet_auth::{
    AUTH_USERS_VERSION, ApiTokenRecord, AuthPolicy, AuthPolicyDocument, AuthUserRecord, AuthUsers,
    AuthUsersDocument, Role as AuthRole, generate_token, hash_password, token_digest,
};
use rivet_compat::{
    ArtifactSnapshot, BehaviorFixture, BehaviorSnapshot, StageSnapshot, StepSnapshot,
    compare_fixture,
};
use rivet_core::{
    BuildEvent, BuildStatus, CronExpression, ExecutionPlan, LogStream, Pipeline, Project,
    ScheduleId, SourceSnapshot,
};
use rivet_credentials::{CredentialKeychain, CredentialKind, CredentialVault};
use rivet_migration::{analyze_jenkinsfile_file, generate_rivetfile_draft_file};
use rivet_runner::execute_pipeline_with_parameters;
use rivet_runner::{CacheStore, MAX_QUEUE_PRIORITY, MIN_QUEUE_PRIORITY, QueueHandle, Scheduler};
use rivet_scm::{
    GitCloneOptions, GitCredential, GitHttpCredential, GitPrepareOptions, GitRepository,
    GitSshCredential, ScmError,
};
use rivet_storage::{SchedulePollConfig, ScheduleTrigger, Storage};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs;
use std::future::Future;
use std::io::{Read, Write};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::time::Instant;
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

const MAX_AGENT_PENDING_DELIVERIES: usize = 8192;
const MAX_AGENT_RECEIVED_DELIVERIES: usize = 8192;
const AGENT_RETRANSMIT_AFTER: Duration = Duration::from_secs(2);
const MAX_AGENT_RETRANSMITS: u8 = 5;

struct PendingAgentDelivery {
    envelope: AgentTransportMessage,
    last_sent: Instant,
    retransmits: u8,
}

struct AgentWireState {
    socket: AgentSocket,
    pending: HashMap<uuid::Uuid, PendingAgentDelivery>,
    received: HashSet<uuid::Uuid>,
    retransmit: tokio::time::Interval,
}

struct AgentInboundMessage {
    payload: AgentMessage,
}

impl AgentWireState {
    fn new(socket: AgentSocket) -> Self {
        Self {
            socket,
            pending: HashMap::new(),
            received: HashSet::new(),
            retransmit: tokio::time::interval(Duration::from_secs(1)),
        }
    }

    async fn send_message(
        &mut self,
        payload: AgentMessage,
    ) -> Result<(), Box<dyn std::error::Error>> {
        self.send_pending(AgentTransportMessage::message(payload))
            .await
    }

    async fn send_pending(
        &mut self,
        envelope: AgentTransportMessage,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if self.pending.len() >= MAX_AGENT_PENDING_DELIVERIES {
            return Err("agent pending delivery window is full".into());
        }
        let payload = serde_json::to_string(&envelope)?;
        self.socket
            .send(AgentSocketMessage::Text(payload.into()))
            .await?;
        self.pending.insert(
            envelope.delivery_id(),
            PendingAgentDelivery {
                envelope,
                last_sent: Instant::now(),
                retransmits: 0,
            },
        );
        Ok(())
    }

    async fn send_ack(
        &mut self,
        delivery_id: uuid::Uuid,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let payload = serde_json::to_string(&AgentTransportMessage::ack(delivery_id))?;
        self.socket
            .send(AgentSocketMessage::Text(payload.into()))
            .await?;
        Ok(())
    }

    fn acknowledge(&mut self, delivery_id: uuid::Uuid) {
        self.pending.remove(&delivery_id);
    }

    async fn retransmit_due(&mut self) -> Result<(), Box<dyn std::error::Error>> {
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
                return Err("agent delivery acknowledgement timed out".into());
            }
            let payload = serde_json::to_string(&delivery.envelope)?;
            self.socket
                .send(AgentSocketMessage::Text(payload.into()))
                .await?;
            delivery.last_sent = Instant::now();
            delivery.retransmits += 1;
        }
        Ok(())
    }

    async fn next_message(
        &mut self,
    ) -> Result<Option<AgentInboundMessage>, Box<dyn std::error::Error>> {
        loop {
            tokio::select! {
                message = self.socket.next() => {
                    let Some(message) = message else {
                        return Ok(None);
                    };
                    match message? {
                        AgentSocketMessage::Ping(payload) => {
                            self.socket.send(AgentSocketMessage::Pong(payload)).await?;
                        }
                        AgentSocketMessage::Pong(_) => {}
                        AgentSocketMessage::Close(_) => return Ok(None),
                        message => {
                            let envelope = decode_agent_socket_message(message)?;
                            match envelope {
                                AgentTransportMessage::Ack { delivery_id, .. } => {
                                    self.acknowledge(delivery_id);
                                }
                                AgentTransportMessage::Message {
                                    delivery_id,
                                    payload,
                                    ..
                                } => {
                                    if self.received.contains(&delivery_id) {
                                        self.send_ack(delivery_id).await?;
                                        continue;
                                    }
                                    if self.received.len() >= MAX_AGENT_RECEIVED_DELIVERIES {
                                        return Err("agent received delivery window is full".into());
                                    }
                                    self.received.insert(delivery_id);
                                    self.send_ack(delivery_id).await?;
                                    return Ok(Some(AgentInboundMessage { payload }));
                                }
                            }
                        }
                    }
                }
                _ = self.retransmit.tick() => {
                    self.retransmit_due().await?;
                }
            }
        }
    }

    async fn close(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        self.socket.close(None).await?;
        Ok(())
    }
}

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
    /// Inspect one persisted build, including stage and step outcomes.
    Inspect {
        project: String,
        #[arg(long)]
        build: i64,
    },
    /// Show persisted output for a build number.
    Logs {
        project: String,
        #[arg(long)]
        build: i64,
    },
    /// Request cancellation of a build owned by a running Rivet server.
    Cancel {
        project: String,
        #[arg(long)]
        build: i64,
        /// HTTP(S) origin of the Rivet server; mutations are never retried automatically.
        #[arg(long, default_value = "http://127.0.0.1:7878")]
        server: String,
        /// Read the Bearer token from a private file without persisting it.
        #[arg(long)]
        token_file: Option<PathBuf>,
    },
    /// Poll a server-managed repository and queue only a new Git revision.
    Poll(PollArgs),
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
        /// Open a private local-account policy with Argon2id password hashes.
        #[arg(long)]
        auth_users_file: Option<PathBuf>,
        /// Read the generic webhook HMAC secret from a private file.
        #[arg(long)]
        webhook_secret_file: Option<PathBuf>,
        /// Read the GitHub webhook HMAC secret from a private file.
        #[arg(long)]
        github_webhook_secret_file: Option<PathBuf>,
        /// Read the GitLab webhook signing/secret token from a private file.
        #[arg(long)]
        gitlab_webhook_secret_file: Option<PathBuf>,
        /// Read the Bitbucket Cloud webhook HMAC secret from a private file.
        #[arg(long)]
        bitbucket_webhook_secret_file: Option<PathBuf>,
        /// Default Rivet credential ID for GitHub push fetches.
        #[arg(long)]
        github_webhook_credential_id: Option<String>,
        /// Default Rivet credential ID for GitLab push fetches.
        #[arg(long)]
        gitlab_webhook_credential_id: Option<String>,
        /// Default Rivet credential ID for Bitbucket push fetches.
        #[arg(long)]
        bitbucket_webhook_credential_id: Option<String>,
        /// Open the passphrase-encrypted SCM credential vault.
        #[arg(long)]
        credentials_file: Option<PathBuf>,
        /// Read the credential vault passphrase from a private file.
        #[arg(long)]
        credentials_passphrase_file: Option<PathBuf>,
        /// Read the credential vault passphrase from the OS keychain account.
        #[arg(long, conflicts_with = "credentials_passphrase_file")]
        credentials_keychain_account: Option<String>,
        /// Use a deployment-specific OS keychain service with the account.
        #[arg(long, requires = "credentials_keychain_account")]
        credentials_keychain_service: Option<String>,
        /// Use this deployment-controlled OpenSSH known-hosts file for SSH SCM fetches.
        #[arg(long)]
        ssh_known_hosts_file: Option<PathBuf>,
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
    /// Manage internal upstream pipeline triggers.
    Upstream {
        #[command(subcommand)]
        command: UpstreamCommand,
    },
    /// Manage provider workflow/pipeline completion triggers.
    ProviderTrigger {
        #[command(subcommand)]
        command: ProviderTriggerCommand,
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
    /// Create a consistent local backup of SQLite state and stored artifacts.
    Backup {
        /// New directory that will contain rivet.db, artifacts/, and manifest.json.
        #[arg(long)]
        output: PathBuf,
    },
    /// Restore a local backup into an explicit target directory.
    Restore {
        /// Backup directory previously created by `rivet backup`.
        #[arg(long)]
        backup: PathBuf,
        /// Target data directory. It must be empty/nonexistent unless --replace is set.
        #[arg(long)]
        target: PathBuf,
        /// Move an existing target aside instead of overwriting or deleting it.
        #[arg(long)]
        replace: bool,
    },
    /// Analyze a Jenkinsfile without executing Groovy or plugin code.
    Analyze {
        #[command(subcommand)]
        command: AnalyzeCommand,
    },
    /// Generate a reviewed Rivetfile draft from a Jenkinsfile.
    Migrate {
        /// Jenkinsfile to analyze and convert.
        path: PathBuf,
        /// Write the generated draft to this path; without it, print TOML to stdout.
        #[arg(short, long)]
        output: Option<PathBuf>,
        /// Allow replacing an existing output file.
        #[arg(long)]
        force: bool,
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
        /// Trigger mode: build or repository-poll.
        #[arg(long, default_value = "build", value_parser = parse_schedule_trigger)]
        trigger: ScheduleTrigger,
        /// Git remote used by a repository-poll schedule.
        #[arg(long, default_value = "origin")]
        remote: String,
        /// Fetch the configured remote before checking the repository head.
        #[arg(long)]
        fetch: bool,
        /// Project-scoped credential ID used for an authenticated fetch.
        #[arg(long)]
        credential_id: Option<String>,
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
enum UpstreamCommand {
    /// Queue the downstream project after a passed upstream build.
    Create {
        downstream: String,
        #[arg(long)]
        upstream: String,
    },
    /// List upstream triggers targeting a downstream project.
    List { downstream: String },
    /// Remove an upstream trigger by UUID.
    Delete { downstream: String, id: uuid::Uuid },
}

#[derive(Debug, Subcommand)]
enum ProviderTriggerCommand {
    /// Queue a project after a successful GitHub, GitLab, or Bitbucket pipeline event.
    Create {
        downstream: String,
        #[arg(long)]
        provider: String,
        #[arg(long, value_name = "OWNER/REPOSITORY")]
        source_repository: String,
        #[arg(long)]
        source_pipeline: Option<String>,
    },
    /// List provider completion triggers targeting a project.
    List { downstream: String },
    /// Remove a provider completion trigger by UUID.
    Delete { downstream: String, id: uuid::Uuid },
}

#[derive(Debug, Subcommand)]
enum CredentialCommand {
    /// Store an HTTP or SSH credential without placing its secret in argv.
    Set {
        id: String,
        #[arg(long, value_enum, default_value_t = CredentialKindArg::HttpBasic)]
        kind: CredentialKindArg,
        #[arg(long)]
        username: String,
        /// Identity that owns this credential; defaults to `local`.
        #[arg(long)]
        owner: Option<String>,
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
        /// OS keychain service; defaults to the shared Rivet service.
        #[arg(long, default_value = "Rivet")]
        service: String,
    },
    /// Remove a vault passphrase from the operating-system keychain.
    KeychainRemove {
        account: String,
        /// OS keychain service; defaults to the shared Rivet service.
        #[arg(long, default_value = "Rivet")]
        service: String,
    },
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
    /// Manage private local user accounts.
    User {
        #[command(subcommand)]
        command: AuthUserCommand,
    },
}

#[derive(Debug, Subcommand)]
enum AuthUserCommand {
    /// Create a local user with a password read from a private file.
    Create {
        username: String,
        #[arg(long, value_enum, default_value_t = AuthRoleArg::Viewer)]
        role: AuthRoleArg,
        #[arg(long = "project")]
        projects: Vec<String>,
        #[arg(long)]
        users_file: PathBuf,
        #[arg(long)]
        password_file: PathBuf,
    },
    /// List user IDs, roles, project scopes, and disabled state.
    List {
        #[arg(long)]
        users_file: PathBuf,
    },
    /// Disable one username without removing its policy record.
    Disable {
        username: String,
        #[arg(long)]
        users_file: PathBuf,
    },
    /// Re-enable one username.
    Enable {
        username: String,
        #[arg(long)]
        users_file: PathBuf,
    },
    /// Replace one user's password from a private file.
    Password {
        username: String,
        #[arg(long)]
        users_file: PathBuf,
        #[arg(long)]
        password_file: PathBuf,
    },
    /// Remove one username while keeping at least one active administrator.
    Remove {
        username: String,
        #[arg(long)]
        users_file: PathBuf,
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

#[derive(Debug, Clone, Copy, ValueEnum)]
enum CredentialKindArg {
    #[value(name = "http-basic")]
    HttpBasic,
    #[value(name = "ssh-key")]
    SshKey,
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
    /// Capture one live Rivet build as a bounded behavior snapshot.
    CaptureRivet {
        project: String,
        #[arg(long)]
        build: i64,
        /// HTTP(S) origin or base URL of the Rivet server.
        #[arg(long, default_value = "http://127.0.0.1:7878")]
        server: String,
        /// Read an optional Bearer token from a private file.
        #[arg(long)]
        token_file: Option<PathBuf>,
        /// Include bounded Rivet log lines; omitted by default to avoid persisting output.
        #[arg(long)]
        include_logs: bool,
        /// Private output path for the snapshot JSON.
        #[arg(long)]
        output: PathBuf,
    },
    /// Capture one live Jenkins build as a bounded behavior snapshot.
    CaptureJenkins {
        job: String,
        #[arg(long)]
        build: i64,
        /// HTTP(S) origin or base URL of Jenkins.
        #[arg(long)]
        server: String,
        /// Jenkins username used with the API token.
        #[arg(long)]
        username: Option<String>,
        /// Read the Jenkins API token from a private file.
        #[arg(long)]
        token_file: Option<PathBuf>,
        /// Include bounded console lines; omitted by default to avoid persisting output.
        #[arg(long)]
        include_logs: bool,
        /// Private output path for the snapshot JSON.
        #[arg(long)]
        output: PathBuf,
    },
    /// Assemble two captured snapshots into a comparable fixture.
    Assemble {
        /// Stable scenario identifier for this comparison.
        #[arg(long)]
        scenario: String,
        /// Snapshot captured from Jenkins.
        #[arg(long)]
        jenkins: PathBuf,
        /// Snapshot captured from Rivet.
        #[arg(long)]
        rivet: PathBuf,
        /// Private output path for the fixture JSON.
        #[arg(long)]
        output: PathBuf,
    },
}

#[derive(Debug, Args)]
struct CreateProject {
    name: String,
    #[arg(long, default_value = ".")]
    repository: PathBuf,
    /// Remote repository to clone before registering the project.
    #[arg(long)]
    repository_url: Option<String>,
    /// New or empty destination for a remote repository clone.
    #[arg(long, requires = "repository_url")]
    clone_destination: Option<PathBuf>,
    /// Optional branch or tag passed to `git clone --branch`.
    #[arg(long, requires = "repository_url")]
    branch: Option<String>,
    /// Optional shallow history depth for the remote clone.
    #[arg(long, requires = "repository_url")]
    depth: Option<u32>,
    /// Optional revision checked out after the remote clone.
    #[arg(long, requires = "repository_url")]
    revision: Option<String>,
    /// Initialize and recursively update submodules after the remote clone.
    #[arg(long, requires = "repository_url")]
    submodules: bool,
    /// Resolve this non-secret ID from an encrypted vault before cloning.
    #[arg(long, requires = "repository_url")]
    credential_id: Option<String>,
    /// Encrypted SCM credential vault used with --credential-id.
    #[arg(long, requires = "credential_id")]
    credentials_file: Option<PathBuf>,
    /// Private passphrase file used with --credential-id.
    #[arg(long, requires = "credential_id")]
    credentials_passphrase_file: Option<PathBuf>,
    /// Use this OpenSSH known-hosts file for strict SSH host-key verification.
    #[arg(long, requires = "repository_url")]
    ssh_known_hosts_file: Option<PathBuf>,
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
    /// Initialize and recursively update Git submodules after preparation.
    #[arg(long)]
    submodules: bool,
    /// Resolve this non-secret ID from an encrypted vault before fetching.
    #[arg(long)]
    credential_id: Option<String>,
    /// Encrypted SCM credential vault used with --credential-id.
    #[arg(long, requires = "credential_id")]
    credentials_file: Option<PathBuf>,
    /// Private passphrase file used with --credential-id.
    #[arg(long, requires = "credential_id")]
    credentials_passphrase_file: Option<PathBuf>,
    /// Use this OpenSSH known-hosts file for strict SSH host-key verification.
    #[arg(long, requires = "fetch")]
    ssh_known_hosts_file: Option<PathBuf>,
    #[arg(long = "param", value_name = "NAME=VALUE")]
    parameters: Vec<String>,
    /// Queue priority from -100 to 100; higher values run first.
    #[arg(long, default_value_t = 0)]
    priority: i32,
}

#[derive(Debug, Args)]
struct PollArgs {
    project: String,
    /// HTTP(S) origin of the Rivet server.
    #[arg(long, default_value = "http://127.0.0.1:7878")]
    server: String,
    /// Read the Bearer token from a private file without persisting it.
    #[arg(long)]
    token_file: Option<PathBuf>,
    #[arg(long, default_value = "origin")]
    remote: String,
    /// Fetch the selected remote before comparing the repository revision.
    #[arg(long)]
    fetch: bool,
    /// Non-secret credential ID resolved by the server's encrypted vault.
    #[arg(long, requires = "fetch")]
    credential_id: Option<String>,
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
    /// Optional allocatable CPU capacity in cores. If omitted, CPU-specific
    /// pipeline requirements will not match this agent.
    #[arg(long)]
    cpu_cores: Option<u16>,
    /// Optional allocatable memory capacity in MiB. If omitted, memory-
    /// specific pipeline requirements will not match this agent.
    #[arg(long)]
    memory_mb: Option<u64>,
    /// Optional allocatable disk capacity in MiB. If omitted, disk-specific
    /// pipeline requirements will not match this agent.
    #[arg(long)]
    disk_mb: Option<u64>,
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
    /// Clone a Git repository into a new or empty destination.
    Clone {
        remote: String,
        destination: PathBuf,
        /// Project context used to authorize a scoped credential.
        #[arg(long)]
        project: Option<String>,
        /// Clone a branch or tag instead of the remote's default branch.
        #[arg(long, conflicts_with = "revision")]
        branch: Option<String>,
        /// Check out this revision after cloning.
        #[arg(long, conflicts_with = "branch")]
        revision: Option<String>,
        /// Request a shallow clone with this bounded history depth.
        #[arg(long)]
        depth: Option<u32>,
        /// Initialize and recursively update Git submodules after checkout.
        #[arg(long)]
        submodules: bool,
        /// Resolve this non-secret ID from an encrypted vault before cloning.
        #[arg(long)]
        credential_id: Option<String>,
        /// Encrypted SCM credential vault used with --credential-id.
        #[arg(long, requires = "credential_id")]
        credentials_file: Option<PathBuf>,
        /// Private passphrase file used with --credential-id.
        #[arg(long, requires = "credential_id")]
        credentials_passphrase_file: Option<PathBuf>,
        /// Use this OpenSSH known-hosts file for strict SSH host-key verification.
        #[arg(long)]
        ssh_known_hosts_file: Option<PathBuf>,
    },
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
        /// Initialize and recursively update Git submodules after preparation.
        #[arg(long)]
        submodules: bool,
        /// Resolve this non-secret ID from an encrypted vault before fetching.
        #[arg(long)]
        credential_id: Option<String>,
        /// Encrypted SCM credential vault used with --credential-id.
        #[arg(long, requires = "credential_id")]
        credentials_file: Option<PathBuf>,
        /// Private passphrase file used with --credential-id.
        #[arg(long, requires = "credential_id")]
        credentials_passphrase_file: Option<PathBuf>,
        /// Use this OpenSSH known-hosts file for strict SSH host-key verification.
        #[arg(long, requires = "fetch")]
        ssh_known_hosts_file: Option<PathBuf>,
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    init_tracing();
    let cli = Cli::parse();
    match cli.command {
        Command::Init { repository } => init_repository(&cli.data_dir, &repository)?,
        Command::Project { command } => {
            let storage = open_storage(&cli.data_dir)?;
            match command {
                ProjectCommand::Create(args) => create_project(&storage, args).await?,
                ProjectCommand::List => list_projects(&storage)?,
            }
        }
        Command::Run(args) => run_project(&cli.data_dir, args).await?,
        Command::Builds { project } => list_builds(&cli.data_dir, &project)?,
        Command::Inspect { project, build } => inspect_build(&cli.data_dir, &project, build)?,
        Command::Logs { project, build } => show_logs(&cli.data_dir, &project, build)?,
        Command::Cancel {
            project,
            build,
            server,
            token_file,
        } => cancel_remote_build(&project, build, &server, token_file.as_deref()).await?,
        Command::Poll(args) => poll_remote_repository(args).await?,
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
            auth_users_file,
            webhook_secret_file,
            github_webhook_secret_file,
            gitlab_webhook_secret_file,
            bitbucket_webhook_secret_file,
            github_webhook_credential_id,
            gitlab_webhook_credential_id,
            bitbucket_webhook_credential_id,
            credentials_file,
            credentials_passphrase_file,
            credentials_keychain_account,
            credentials_keychain_service,
            ssh_known_hosts_file,
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
            let bitbucket_webhook_secret = bitbucket_webhook_secret_file
                .as_deref()
                .map(|path| read_private_value(path, "Bitbucket webhook secret"))
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
                    auth_users_file,
                    webhook_secret,
                    github_webhook_secret,
                    gitlab_webhook_secret,
                    bitbucket_webhook_secret,
                    github_webhook_credential_id,
                    gitlab_webhook_credential_id,
                    bitbucket_webhook_credential_id,
                    credentials_file,
                    credentials_passphrase,
                    credentials_keychain_account,
                    credentials_keychain_service,
                    ssh_known_hosts_file,
                    extension_manifest_dir,
                    allowed_origins,
                },
            )
            .await?
        }
        Command::Scm { command } => inspect_scm(command).await?,
        Command::Schedule { command } => manage_schedule(&cli.data_dir, command)?,
        Command::Upstream { command } => manage_upstream(&cli.data_dir, command)?,
        Command::ProviderTrigger { command } => manage_provider_trigger(&cli.data_dir, command)?,
        Command::Credential { command } => manage_credentials(&cli.data_dir, command)?,
        Command::Auth { command } => manage_auth(&cli.data_dir, command)?,
        Command::Cache { command } => manage_cache(&cli.data_dir, command)?,
        Command::Backup { output } => backup_storage(&cli.data_dir, &output)?,
        Command::Restore {
            backup,
            target,
            replace,
        } => restore_storage(&backup, &target, replace)?,
        Command::Analyze { command } => analyze_file(command)?,
        Command::Migrate {
            path,
            output,
            force,
        } => migrate_file(&path, output.as_deref(), force)?,
        Command::Compat { command } => compare_compatibility(command).await?,
        Command::Agent(args) => run_agent(args).await?,
    }
    Ok(())
}

fn init_tracing() {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("rivet=info"));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .try_init();
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

fn migrate_file(
    path: &Path,
    output: Option<&Path>,
    force: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let draft = generate_rivetfile_draft_file(path)?;
    let Some(rivetfile) = draft.rivetfile_toml.as_deref() else {
        return Err("Jenkinsfile did not produce a deterministic Rivetfile draft".into());
    };

    if let Some(output) = output {
        let mut bytes = rivetfile.as_bytes().to_vec();
        if !bytes.ends_with(b"\n") {
            bytes.push(b'\n');
        }
        write_private_atomic(output, &bytes, force, "Rivetfile draft")?;
        eprintln!(
            "Generated Rivetfile draft at {} (status: {:?}, converted steps: {})",
            output.display(),
            draft.status,
            draft.converted_steps
        );
    } else {
        print!("{rivetfile}");
        if !rivetfile.ends_with('\n') {
            println!();
        }
        eprintln!(
            "Migration status: {:?} · converted steps: {}",
            draft.status, draft.converted_steps
        );
    }

    for warning in draft.warnings {
        eprintln!("warning: {warning}");
    }
    Ok(())
}

async fn compare_compatibility(command: CompatCommand) -> Result<(), Box<dyn std::error::Error>> {
    match command {
        CompatCommand::Compare { fixture } => {
            let fixture = BehaviorFixture::from_json(&fs::read(&fixture)?)?;
            let report = compare_fixture(&fixture)?;
            println!("{}", serde_json::to_string_pretty(&report)?);
            if !report.matches {
                return Err("compatibility fixture contains semantic differences".into());
            }
        }
        CompatCommand::CaptureRivet {
            project,
            build,
            server,
            token_file,
            include_logs,
            output,
        } => {
            let snapshot = capture_rivet_snapshot(
                &server,
                &project,
                build,
                token_file.as_deref(),
                include_logs,
            )
            .await?;
            write_compat_json(&output, &snapshot)?;
            println!("Captured Rivet snapshot to {}", output.display());
        }
        CompatCommand::CaptureJenkins {
            job,
            build,
            server,
            username,
            token_file,
            include_logs,
            output,
        } => {
            let snapshot = capture_jenkins_snapshot(
                &server,
                &job,
                build,
                username.as_deref(),
                token_file.as_deref(),
                include_logs,
            )
            .await?;
            write_compat_json(&output, &snapshot)?;
            println!("Captured Jenkins snapshot to {}", output.display());
        }
        CompatCommand::Assemble {
            scenario,
            jenkins,
            rivet,
            output,
        } => {
            let jenkins = read_compat_snapshot(&jenkins)?;
            let rivet = read_compat_snapshot(&rivet)?;
            let fixture = BehaviorFixture {
                schema_version: rivet_compat::FIXTURE_SCHEMA_VERSION,
                scenario,
                jenkins,
                rivet,
            };
            fixture.validate()?;
            write_compat_json(&output, &fixture)?;
            println!("Assembled compatibility fixture at {}", output.display());
        }
    }
    Ok(())
}

const MAX_COMPAT_CAPTURE_BYTES: usize = 2 * 1024 * 1024;
const MAX_COMPAT_CAPTURE_LOG_LINES: usize = 4096;
const MAX_COMPAT_CAPTURE_LOG_LINE_BYTES: usize = 8192;

async fn capture_rivet_snapshot(
    server: &str,
    project: &str,
    build: i64,
    token_file: Option<&Path>,
    include_logs: bool,
) -> Result<BehaviorSnapshot, Box<dyn std::error::Error>> {
    if build <= 0 {
        return Err("Rivet build number must be positive".into());
    }
    let token = token_file.map(read_auth_token).transpose()?;
    let client = compatibility_http_client()?;
    let details_url = rivet_capture_endpoint(server, project, build, &[])?;
    let details: rivet_storage::BuildDetails = serde_json::from_value(
        capture_json(
            capture_get(&client, details_url, token.as_deref(), None),
            "Rivet build details",
        )
        .await?,
    )?;
    let artifact_url = rivet_capture_endpoint(server, project, build, &["artifacts"])?;
    let artifacts: Vec<rivet_storage::ArtifactRecord> = serde_json::from_value(
        capture_json(
            capture_get(&client, artifact_url, token.as_deref(), None),
            "Rivet artifacts",
        )
        .await?,
    )?;
    let logs = if include_logs {
        let log_url = rivet_capture_endpoint(server, project, build, &["logs"])?;
        let records: Vec<rivet_storage::LogRecord> = serde_json::from_value(
            capture_json(
                capture_get(&client, log_url, token.as_deref(), None),
                "Rivet logs",
            )
            .await?,
        )?;
        records.into_iter().map(|record| record.line).collect()
    } else {
        Vec::new()
    };
    let rivet_storage::BuildDetails { build, stages } = details;
    let snapshot = BehaviorSnapshot {
        status: status_label(&build.status).to_owned(),
        stages: stages
            .into_iter()
            .map(|stage| StageSnapshot {
                name: stage.stage.name,
                status: status_label(&stage.stage.status).to_owned(),
                steps: stage
                    .steps
                    .into_iter()
                    .map(|step| StepSnapshot {
                        name: step.name,
                        status: status_label(&step.status).to_owned(),
                        exit_code: step.exit_code,
                    })
                    .collect(),
            })
            .collect(),
        parameters: build.parameters,
        artifacts: artifacts
            .into_iter()
            .map(|artifact| ArtifactSnapshot {
                name: artifact.name,
                checksum: Some(artifact.checksum),
            })
            .collect(),
        logs: validate_capture_logs(logs)?,
    };
    snapshot.validate()?;
    Ok(snapshot)
}

async fn capture_jenkins_snapshot(
    server: &str,
    job: &str,
    build: i64,
    username: Option<&str>,
    token_file: Option<&Path>,
    include_logs: bool,
) -> Result<BehaviorSnapshot, Box<dyn std::error::Error>> {
    if build <= 0 {
        return Err("Jenkins build number must be positive".into());
    }
    let username = username.map(str::trim).filter(|value| !value.is_empty());
    if username.is_some() != token_file.is_some() {
        return Err("Jenkins authentication requires both --username and --token-file".into());
    }
    let token = token_file.map(read_auth_token).transpose()?;
    let basic_auth = username.zip(token.as_deref());
    let client = compatibility_http_client()?;
    let mut build_url = jenkins_capture_endpoint(server, job, build, &["api", "json"])?;
    build_url.set_query(Some(
        "tree=result,building,actions[parameters[name,value]],artifacts[fileName,relativePath,checksum]",
    ));
    let build_payload = capture_json(
        capture_get(&client, build_url, None, basic_auth),
        "Jenkins build details",
    )
    .await?;
    let building = build_payload
        .get("building")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let status = jenkins_status(
        build_payload
            .get("result")
            .and_then(serde_json::Value::as_str),
        building,
    )?;
    let stage_payload = capture_optional_json(
        capture_get(
            &client,
            jenkins_capture_endpoint(server, job, build, &["wfapi", "describe"])?,
            None,
            basic_auth,
        ),
        "Jenkins pipeline stages",
    )
    .await?;
    let stages = parse_jenkins_stages(stage_payload.as_ref())?;
    let logs = if include_logs {
        let log_payload = capture_bytes(
            capture_get(
                &client,
                jenkins_capture_endpoint(server, job, build, &["consoleText"])?,
                None,
                basic_auth,
            ),
            "Jenkins console output",
        )
        .await?;
        let text = String::from_utf8(log_payload)?;
        validate_capture_logs(text.lines().map(str::to_owned).collect())?
    } else {
        Vec::new()
    };
    let snapshot = BehaviorSnapshot {
        status,
        stages,
        parameters: parse_jenkins_parameters(&build_payload),
        artifacts: parse_jenkins_artifacts(&build_payload)?,
        logs,
    };
    snapshot.validate()?;
    Ok(snapshot)
}

fn compatibility_http_client() -> Result<reqwest::Client, Box<dyn std::error::Error>> {
    Ok(reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(5))
        .timeout(Duration::from_secs(20))
        .user_agent("rivet-compat/0.1")
        .build()?)
}

fn parse_capture_base_url(
    server: &str,
    label: &str,
) -> Result<reqwest::Url, Box<dyn std::error::Error>> {
    let url = reqwest::Url::parse(server.trim_end_matches('/'))?;
    if !matches!(url.scheme(), "http" | "https")
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(format!("{label} must be a credential-free HTTP(S) base URL").into());
    }
    Ok(url)
}

fn append_capture_segments<I, S>(
    url: &mut reqwest::Url,
    segments: I,
) -> Result<(), Box<dyn std::error::Error>>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let mut path = url
        .path_segments_mut()
        .map_err(|_| "capture URL cannot be used as a base URL")?;
    path.pop_if_empty();
    for segment in segments {
        let segment = segment.as_ref();
        if segment.is_empty() || segment == "." || segment == ".." {
            return Err("capture URL contains an invalid empty or traversal segment".into());
        }
        path.push(segment);
    }
    Ok(())
}

fn rivet_capture_endpoint(
    server: &str,
    project: &str,
    build: i64,
    tail: &[&str],
) -> Result<reqwest::Url, Box<dyn std::error::Error>> {
    let mut url = parse_capture_base_url(server, "Rivet server")?;
    let number = build.to_string();
    let mut segments = vec!["api", "v1", "projects", project, "builds", number.as_str()];
    segments.extend_from_slice(tail);
    append_capture_segments(&mut url, segments)?;
    Ok(url)
}

fn jenkins_capture_endpoint(
    server: &str,
    job: &str,
    build: i64,
    tail: &[&str],
) -> Result<reqwest::Url, Box<dyn std::error::Error>> {
    let mut url = parse_capture_base_url(server, "Jenkins server")?;
    let job = job.trim_matches('/');
    if job.is_empty() {
        return Err("Jenkins job cannot be empty".into());
    }
    let mut segments = Vec::new();
    for part in job.split('/') {
        if part.is_empty() || part == "." || part == ".." || part.chars().any(char::is_control) {
            return Err("Jenkins job contains an invalid path segment".into());
        }
        segments.push("job");
        segments.push(part);
    }
    let number = build.to_string();
    segments.push(number.as_str());
    segments.extend_from_slice(tail);
    append_capture_segments(&mut url, segments)?;
    Ok(url)
}

fn capture_get(
    client: &reqwest::Client,
    url: reqwest::Url,
    bearer: Option<&str>,
    basic: Option<(&str, &str)>,
) -> reqwest::RequestBuilder {
    let mut request = client
        .get(url)
        .header("accept", "application/json")
        .header("x-request-id", uuid::Uuid::new_v4().to_string());
    if let Some(token) = bearer {
        request = request.bearer_auth(token);
    }
    if let Some((username, token)) = basic {
        request = request.basic_auth(username, Some(token));
    }
    request
}

async fn capture_bytes(
    request: reqwest::RequestBuilder,
    label: &str,
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let response = request.send().await?;
    let status = response.status();
    if response
        .content_length()
        .is_some_and(|length| length > MAX_COMPAT_CAPTURE_BYTES as u64)
    {
        return Err(
            format!("{label} response exceeds the {MAX_COMPAT_CAPTURE_BYTES}-byte limit").into(),
        );
    }
    let mut stream = response.bytes_stream();
    let mut body = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        if body.len().saturating_add(chunk.len()) > MAX_COMPAT_CAPTURE_BYTES {
            return Err(format!(
                "{label} response exceeds the {MAX_COMPAT_CAPTURE_BYTES}-byte limit"
            )
            .into());
        }
        body.extend_from_slice(&chunk);
    }
    if !status.is_success() {
        return Err(format!("{label} returned HTTP {status}").into());
    }
    Ok(body)
}

async fn capture_json(
    request: reqwest::RequestBuilder,
    label: &str,
) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
    let body = capture_bytes(request, label).await?;
    Ok(serde_json::from_slice(&body)?)
}

async fn capture_optional_json(
    request: reqwest::RequestBuilder,
    label: &str,
) -> Result<Option<serde_json::Value>, Box<dyn std::error::Error>> {
    let response = request.send().await?;
    let status = response.status();
    if status == reqwest::StatusCode::NOT_FOUND {
        return Ok(None);
    }
    if response
        .content_length()
        .is_some_and(|length| length > MAX_COMPAT_CAPTURE_BYTES as u64)
    {
        return Err(
            format!("{label} response exceeds the {MAX_COMPAT_CAPTURE_BYTES}-byte limit").into(),
        );
    }
    let mut stream = response.bytes_stream();
    let mut body = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        if body.len().saturating_add(chunk.len()) > MAX_COMPAT_CAPTURE_BYTES {
            return Err(format!(
                "{label} response exceeds the {MAX_COMPAT_CAPTURE_BYTES}-byte limit"
            )
            .into());
        }
        body.extend_from_slice(&chunk);
    }
    if !status.is_success() {
        return Err(format!("{label} returned HTTP {status}").into());
    }
    Ok(Some(serde_json::from_slice(&body)?))
}

fn jenkins_status(
    value: Option<&str>,
    building: bool,
) -> Result<String, Box<dyn std::error::Error>> {
    let status = value.unwrap_or_default().trim().to_ascii_uppercase();
    let mapped = match status.as_str() {
        "" if building => "running",
        "" => "pending",
        "SUCCESS" => "passed",
        "FAILURE" | "ERROR" => "failed",
        "ABORTED" => "cancelled",
        "UNSTABLE" => "unstable",
        "NOT_BUILT" => "skipped",
        "QUEUED" | "WAITING" | "BLOCKED" => "queued",
        "IN_PROGRESS" | "RUNNING" => "running",
        other => return Err(format!("unsupported Jenkins status: {other}").into()),
    };
    Ok(mapped.to_owned())
}

fn parse_jenkins_stages(
    payload: Option<&serde_json::Value>,
) -> Result<Vec<StageSnapshot>, Box<dyn std::error::Error>> {
    let Some(stages) = payload
        .and_then(|payload| payload.get("stages"))
        .and_then(serde_json::Value::as_array)
    else {
        return Ok(Vec::new());
    };
    stages
        .iter()
        .enumerate()
        .map(|(index, stage)| {
            let name = stage
                .get("name")
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| format!("Jenkins stage {index} has no name"))?
                .to_owned();
            let status = jenkins_status(
                stage.get("status").and_then(serde_json::Value::as_str),
                false,
            )?;
            let steps = stage
                .get("steps")
                .and_then(serde_json::Value::as_array)
                .map(|steps| {
                    steps
                        .iter()
                        .enumerate()
                        .map(|(step_index, step)| {
                            let name = step
                                .get("name")
                                .and_then(serde_json::Value::as_str)
                                .ok_or_else(|| format!("Jenkins step {step_index} has no name"))?
                                .to_owned();
                            Ok(StepSnapshot {
                                name,
                                status: jenkins_status(
                                    step.get("status").and_then(serde_json::Value::as_str),
                                    false,
                                )?,
                                exit_code: step
                                    .get("exit_code")
                                    .and_then(serde_json::Value::as_i64)
                                    .and_then(|value| i32::try_from(value).ok()),
                            })
                        })
                        .collect::<Result<Vec<_>, Box<dyn std::error::Error>>>()
                })
                .transpose()?
                .unwrap_or_default();
            Ok(StageSnapshot {
                name,
                status,
                steps,
            })
        })
        .collect()
}

fn parse_jenkins_parameters(payload: &serde_json::Value) -> BTreeMap<String, String> {
    let mut parameters = BTreeMap::new();
    let Some(actions) = payload.get("actions").and_then(serde_json::Value::as_array) else {
        return parameters;
    };
    for action in actions {
        let Some(values) = action
            .get("parameters")
            .and_then(serde_json::Value::as_array)
        else {
            continue;
        };
        for parameter in values {
            let Some(name) = parameter.get("name").and_then(serde_json::Value::as_str) else {
                continue;
            };
            let Some(value) = parameter.get("value") else {
                continue;
            };
            let value = value
                .as_str()
                .map(str::to_owned)
                .unwrap_or_else(|| value.to_string());
            parameters.insert(
                name.to_owned(),
                if sensitive_parameter_name(name) {
                    "<redacted>".to_owned()
                } else {
                    value
                },
            );
        }
    }
    parameters
}

fn sensitive_parameter_name(name: &str) -> bool {
    let name = name.to_ascii_uppercase();
    [
        "PASSWORD",
        "PASSWD",
        "TOKEN",
        "SECRET",
        "PRIVATE_KEY",
        "API_KEY",
        "CREDENTIAL",
    ]
    .iter()
    .any(|marker| name.contains(marker))
}

fn parse_jenkins_artifacts(
    payload: &serde_json::Value,
) -> Result<Vec<ArtifactSnapshot>, Box<dyn std::error::Error>> {
    let Some(artifacts) = payload
        .get("artifacts")
        .and_then(serde_json::Value::as_array)
    else {
        return Ok(Vec::new());
    };
    artifacts
        .iter()
        .enumerate()
        .map(|(index, artifact)| {
            let name = artifact
                .get("fileName")
                .or_else(|| artifact.get("relativePath"))
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| format!("Jenkins artifact {index} has no file name"))?
                .to_owned();
            Ok(ArtifactSnapshot {
                name,
                checksum: artifact
                    .get("checksum")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_owned),
            })
        })
        .collect()
}

fn validate_capture_logs(logs: Vec<String>) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    if logs.len() > MAX_COMPAT_CAPTURE_LOG_LINES {
        return Err(format!(
            "compatibility capture contains more than {MAX_COMPAT_CAPTURE_LOG_LINES} log lines"
        )
        .into());
    }
    for line in &logs {
        if line.len() > MAX_COMPAT_CAPTURE_LOG_LINE_BYTES
            || line
                .chars()
                .any(|character| character.is_control() && character != '\r' && character != '\n')
        {
            return Err("compatibility capture contains an invalid log line".into());
        }
    }
    Ok(logs)
}

fn read_compat_snapshot(path: &Path) -> Result<BehaviorSnapshot, Box<dyn std::error::Error>> {
    let bytes = read_bounded_compat_file(path, "compatibility snapshot")?;
    let snapshot: BehaviorSnapshot = serde_json::from_slice(&bytes)?;
    snapshot.validate()?;
    Ok(snapshot)
}

fn read_bounded_compat_file(
    path: &Path,
    label: &str,
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() {
        return Err(format!("{label} must not be a symbolic link: {}", path.display()).into());
    }
    if !metadata.is_file() {
        return Err(format!("{label} path must be a regular file: {}", path.display()).into());
    }
    if metadata.len() > MAX_COMPAT_CAPTURE_BYTES as u64 {
        return Err(format!("{label} exceeds the {MAX_COMPAT_CAPTURE_BYTES}-byte limit").into());
    }
    let bytes = fs::read(path)?;
    if bytes.len() > MAX_COMPAT_CAPTURE_BYTES {
        return Err(format!("{label} exceeds the {MAX_COMPAT_CAPTURE_BYTES}-byte limit").into());
    }
    Ok(bytes)
}

fn write_compat_json<T: Serialize>(
    path: &Path,
    value: &T,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut bytes = serde_json::to_vec_pretty(value)?;
    bytes.push(b'\n');
    write_private_atomic(path, &bytes, true, "compatibility capture")
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
            cpu_cores: args.cpu_cores,
            memory_mb: args.memory_mb,
            disk_mb: args.disk_mb,
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
    let (socket, _) = connect_async(request).await?;
    let mut transport = AgentWireState::new(socket);
    transport
        .send_message(AgentMessage::Register(registration.clone()))
        .await?;
    let registered = transport
        .next_message()
        .await?
        .ok_or("Rivet server closed the agent connection during registration")?;
    let session_id = match registered.payload {
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
                transport.send_message(
                    AgentMessage::Heartbeat(AgentHeartbeat {
                        protocol_version: PROTOCOL_VERSION,
                        agent_id: registration.agent_id,
                        session_id,
                        sequence,
                        running: running.clone(),
                        sent_at: Utc::now(),
                    }),
                )
                .await?;
            }
            message = transport.next_message() => {
                let Some(message) = message? else { break; };
                let AgentInboundMessage { payload } = message;
                match payload {
                    AgentMessage::HeartbeatAck { .. } => {}
                    AgentMessage::Error { code, message, .. } => {
                        return Err(format!("agent connection error ({code}): {message}").into());
                    }
                    payload @ AgentMessage::Assign { build_id, .. } => {
                        let assignment_build_id = build_id;
                        running.push(assignment_build_id);
                        let assignment = run_agent_assignment(
                            &mut transport,
                            registration,
                            session_id,
                            sequence,
                            payload,
                            &args.workspace_root,
                        ).await;
                        match assignment {
                            Ok(AgentAssignmentResult::Complete { sequence: next }) => {
                                sequence = next;
                                running.retain(|running_build| *running_build != assignment_build_id);
                            }
                            Ok(AgentAssignmentResult::Stopped) => {
                                session_result = AgentSessionResult::Stopped;
                                break;
                            }
                            Err(error) => {
                                running.retain(|running_build| *running_build != assignment_build_id);
                                transport.send_message(
                                    AgentMessage::Error {
                                        protocol_version: PROTOCOL_VERSION,
                                        build_id: Some(assignment_build_id),
                                        code: "assignment_failed".into(),
                                        message: error.to_string(),
                                    },
                                ).await?;
                            }
                        }
                    }
                    _ => {}
                }
            }
        }
    }
    let _ = transport.close().await;
    Ok(session_result)
}

async fn run_agent_assignment(
    transport: &mut AgentWireState,
    registration: &AgentRegistration,
    session_id: uuid::Uuid,
    mut sequence: u64,
    assignment: AgentMessage,
    workspace_root: &Path,
) -> Result<AgentAssignmentResult, Box<dyn std::error::Error>> {
    let AgentMessage::Assign {
        build_id,
        attempt_id,
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

    transport
        .send_message(AgentMessage::AssignmentAccepted {
            protocol_version: PROTOCOL_VERSION,
            build_id,
        })
        .await?;

    let transfer_result = receive_workspace_archive(
        transport,
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
    transport
        .send_message(AgentMessage::WorkspaceReady {
            protocol_version: PROTOCOL_VERSION,
            build_id,
        })
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
    let mut event_sequence = 0_u64;
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
                transport
                    .send_message(
                    AgentMessage::Heartbeat(AgentHeartbeat {
                        protocol_version: PROTOCOL_VERSION,
                        agent_id: registration.agent_id,
                        session_id,
                        sequence,
                        running: vec![build_id],
                        sent_at: Utc::now(),
                    }),
                    )
                    .await?;
            }
            message = event_receiver.recv() => {
                if let Some(event) = message {
                    terminal_event_sent |= send_execution_event(
                        transport,
                        event,
                        build_id,
                        attempt_id,
                        &mut event_sequence,
                        &execution_pipeline,
                        &build_workspace,
                    ).await?;
                }
            }
            message = transport.next_message() => {
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
                let AgentInboundMessage { payload } = message;
                match payload {
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
                        transport,
                        event,
                        build_id,
                        attempt_id,
                        &mut event_sequence,
                        &execution_pipeline,
                        &build_workspace,
                    ).await?;
                }
                if let Err(error) = result {
                    if !terminal_event_sent {
                        transport
                            .send_message(
                            AgentMessage::Error {
                                protocol_version: PROTOCOL_VERSION,
                                build_id: Some(build_id),
                                code: "execution_failed".into(),
                                message: error.to_string(),
                            },
                            )
                            .await?;
                    }
                }
                remove_exact_agent_path(&build_workspace)?;
                return Ok(AgentAssignmentResult::Complete { sequence });
            }
        }
    }
}

async fn send_execution_event(
    transport: &mut AgentWireState,
    event: BuildEvent,
    build_id: uuid::Uuid,
    attempt_id: uuid::Uuid,
    event_sequence: &mut u64,
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
        if let Err(error) = send_artifact_archive(transport, build_id, pipeline, workspace).await {
            transport
                .send_message(AgentMessage::Error {
                    protocol_version: PROTOCOL_VERSION,
                    build_id: Some(build_id),
                    code: "artifact_collection_failed".into(),
                    message: error.to_string(),
                })
                .await?;
            return Ok(false);
        }
    }
    let terminal = matches!(event, BuildEvent::BuildFinished { .. });
    *event_sequence = (*event_sequence)
        .checked_add(1)
        .ok_or("remote event sequence overflow")?;
    transport
        .send_message(AgentMessage::Event {
            protocol_version: PROTOCOL_VERSION,
            attempt_id,
            sequence: *event_sequence,
            event,
        })
        .await?;
    Ok(terminal)
}

async fn send_artifact_archive(
    transport: &mut AgentWireState,
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
    transport
        .send_message(AgentMessage::ArtifactsReady {
            protocol_version: PROTOCOL_VERSION,
            build_id,
            transfer: archive.transfer,
        })
        .await?;
    for (sequence, data) in archive.bytes.chunks(MAX_WORKSPACE_CHUNK_BYTES).enumerate() {
        let sequence =
            u32::try_from(sequence).map_err(|_| "artifact archive has too many chunks")?;
        transport
            .send_message(AgentMessage::ArtifactChunk {
                protocol_version: PROTOCOL_VERSION,
                build_id,
                sequence,
                data: data.to_vec(),
            })
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
    transport: &mut AgentWireState,
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
                transport
                    .send_message(
                    AgentMessage::Heartbeat(AgentHeartbeat {
                        protocol_version: PROTOCOL_VERSION,
                        agent_id: registration.agent_id,
                        session_id,
                        sequence: *sequence,
                        running: vec![build_id],
                        sent_at: Utc::now(),
                    }),
                    )
                    .await?;
            }
            message = transport.next_message() => {
                let Some(message) = message? else {
                    return Err("Rivet server closed the agent connection during workspace transfer".into());
                };
                let AgentInboundMessage { payload } = message;
                match payload {
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
                    _ => {
                        return Err(format!("unexpected server message while receiving workspace {build_id}").into());
                    }
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

fn decode_agent_socket_message(
    message: AgentSocketMessage,
) -> Result<AgentTransportMessage, Box<dyn std::error::Error>> {
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
    let message: AgentTransportMessage = serde_json::from_str(&payload)?;
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
        ScmCommand::Clone {
            remote,
            destination,
            project,
            branch,
            revision,
            depth,
            submodules,
            credential_id,
            credentials_file,
            credentials_passphrase_file,
            ssh_known_hosts_file,
        } => {
            let credential = load_git_credential(
                credential_id.as_deref(),
                credentials_file.as_deref(),
                credentials_passphrase_file.as_deref(),
                project.as_deref(),
            )?;
            let snapshot = GitRepository::clone_repository_with_auth(
                &GitCloneOptions {
                    remote,
                    destination,
                    branch,
                    depth,
                    revision,
                    submodules,
                    credential_id,
                    known_hosts_file: ssh_known_hosts_file,
                },
                credential.as_ref(),
            )
            .await?;
            println!("{}", serde_json::to_string_pretty(&snapshot)?);
            return Ok(());
        }
        ScmCommand::Prepare {
            repository,
            project,
            remote,
            fetch,
            revision,
            clean,
            clean_ignored,
            submodules,
            credential_id,
            credentials_file,
            credentials_passphrase_file,
            ssh_known_hosts_file,
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
                    submodules,
                    credential_id: credential_id.clone(),
                    known_hosts_file: ssh_known_hosts_file.clone(),
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
                .prepare_with_auth(&options, credential.as_ref())
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

const STORAGE_BACKUP_FORMAT: &str = "rivet-local-backup";
const STORAGE_BACKUP_VERSION: u32 = 1;

#[derive(Debug, Serialize, Deserialize)]
struct StorageBackupManifest {
    format: String,
    version: u32,
    created_at: String,
    database_bytes: u64,
    database_sha256: String,
    artifact_files: usize,
    artifact_bytes: u64,
    includes_cache: bool,
    includes_credentials: bool,
}

fn backup_storage(data_dir: &Path, output: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let storage = open_storage(data_dir)?;
    let report = storage.backup_to(output)?;
    let database_path = output.join("rivet.db");
    let database_sha256 = sha256_regular_file(&database_path)?;
    let manifest = StorageBackupManifest {
        format: STORAGE_BACKUP_FORMAT.to_owned(),
        version: STORAGE_BACKUP_VERSION,
        created_at: Utc::now().to_rfc3339(),
        database_bytes: report.database_bytes,
        database_sha256,
        artifact_files: report.artifact_files,
        artifact_bytes: report.artifact_bytes,
        includes_cache: false,
        includes_credentials: false,
    };
    let mut bytes = serde_json::to_vec_pretty(&manifest)?;
    bytes.push(b'\n');
    write_private_atomic(
        &output.join("manifest.json"),
        &bytes,
        false,
        "storage backup manifest",
    )?;
    println!(
        "Created Rivet backup at {} ({} database bytes, {} artifact files)",
        output.display(),
        report.database_bytes,
        report.artifact_files
    );
    println!(
        "External credential vaults and derived caches are not included; preserve them separately."
    );
    Ok(())
}

fn restore_storage(
    backup: &Path,
    target: &Path,
    replace: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let manifest_path = backup.join("manifest.json");
    let manifest_metadata = fs::symlink_metadata(&manifest_path)?;
    if manifest_metadata.file_type().is_symlink() || !manifest_metadata.is_file() {
        return Err(format!(
            "backup manifest must be a regular file: {}",
            manifest_path.display()
        )
        .into());
    }
    let manifest: StorageBackupManifest = serde_json::from_slice(&fs::read(&manifest_path)?)?;
    if manifest.format != STORAGE_BACKUP_FORMAT || manifest.version != STORAGE_BACKUP_VERSION {
        return Err(format!(
            "unsupported Rivet backup format in {}",
            manifest_path.display()
        )
        .into());
    }
    let database_path = backup.join("rivet.db");
    let database_metadata = fs::symlink_metadata(&database_path)?;
    if database_metadata.file_type().is_symlink() || !database_metadata.is_file() {
        return Err(format!(
            "backup database must be a regular file: {}",
            database_path.display()
        )
        .into());
    }
    if database_metadata.len() != manifest.database_bytes {
        return Err(format!(
            "backup database size mismatch: expected {}, found {}",
            manifest.database_bytes,
            database_metadata.len()
        )
        .into());
    }
    let actual_database_sha256 = sha256_regular_file(&database_path)?;
    if actual_database_sha256 != manifest.database_sha256 {
        return Err(format!(
            "backup database checksum mismatch: expected {}, found {}",
            manifest.database_sha256, actual_database_sha256
        )
        .into());
    }
    let artifact_source = backup.join("artifacts");
    let (artifact_files, artifact_bytes) = copy_backup_artifacts(&artifact_source, None)?;
    if artifact_files != manifest.artifact_files || artifact_bytes != manifest.artifact_bytes {
        return Err(format!(
            "backup artifact manifest mismatch: expected {} files/{} bytes, found {} files/{} bytes",
            manifest.artifact_files,
            manifest.artifact_bytes,
            artifact_files,
            artifact_bytes
        )
        .into());
    }

    let target_exists = match fs::symlink_metadata(target) {
        Ok(metadata) if metadata.file_type().is_symlink() => {
            return Err(format!(
                "restore target must not be a symbolic link: {}",
                target.display()
            )
            .into());
        }
        Ok(metadata) if !metadata.is_dir() => {
            return Err(format!("restore target must be a directory: {}", target.display()).into());
        }
        Ok(_) => true,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => false,
        Err(error) => return Err(error.into()),
    };
    if target_exists && !replace {
        return Err(format!(
            "restore target already exists; pass --replace to move it aside: {}",
            target.display()
        )
        .into());
    }
    let parent = target.parent().unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent)?;
    let parent_metadata = fs::symlink_metadata(parent)?;
    if parent_metadata.file_type().is_symlink() || !parent_metadata.is_dir() {
        return Err(format!(
            "restore target parent must be a directory: {}",
            parent.display()
        )
        .into());
    }
    let staging = parent.join(format!(".rivet-restore-{}", uuid::Uuid::new_v4()));
    fs::create_dir(&staging)?;
    let result = (|| -> Result<(), Box<dyn std::error::Error>> {
        fs::copy(&database_path, staging.join("rivet.db"))?;
        copy_backup_artifacts(&artifact_source, Some(&staging.join("artifacts")))?;
        let restored = Storage::open(staging.join("rivet.db"))?;
        restored.health_check()?;
        drop(restored);

        if target_exists {
            let target_name = target
                .file_name()
                .and_then(|name| name.to_str())
                .ok_or("restore target has no valid directory name")?;
            let previous = parent.join(format!(
                ".{target_name}.pre-restore-{}",
                uuid::Uuid::new_v4()
            ));
            fs::rename(target, &previous)?;
            fs::rename(&staging, target)?;
            println!("Moved previous data directory to {}", previous.display());
        } else {
            fs::rename(&staging, target)?;
        }
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_dir_all(&staging);
    }
    result?;
    println!("Restored Rivet backup into {}", target.display());
    println!("External credential vaults and derived caches remain separate from this restore.");
    Ok(())
}

fn sha256_regular_file(path: &Path) -> Result<String, Box<dyn std::error::Error>> {
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(format!("expected a regular file: {}", path.display()).into());
    }
    let mut file = fs::File::open(path)?;
    let mut digest = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
    }
    Ok(hex::encode(digest.finalize()))
}

fn copy_backup_artifacts(
    source: &Path,
    destination: Option<&Path>,
) -> Result<(usize, u64), Box<dyn std::error::Error>> {
    let source_metadata = match fs::symlink_metadata(source) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok((0, 0)),
        Err(error) => return Err(error.into()),
    };
    if source_metadata.file_type().is_symlink() || !source_metadata.is_dir() {
        return Err(format!("backup artifacts must be a directory: {}", source.display()).into());
    }
    let mut file_count = 0_usize;
    let mut byte_count = 0_u64;
    for entry in WalkDir::new(source).follow_links(false) {
        let entry = entry?;
        let relative = entry.path().strip_prefix(source)?.to_path_buf();
        if relative.as_os_str().is_empty() {
            continue;
        }
        validate_archive_path(&relative)?;
        let file_type = entry.file_type();
        if file_type.is_symlink() || (!file_type.is_dir() && !file_type.is_file()) {
            return Err(format!(
                "backup artifacts contain an unsupported path: {}",
                relative.display()
            )
            .into());
        }
        let target = destination.map(|root| root.join(&relative));
        if file_type.is_dir() {
            if let Some(target) = target {
                fs::create_dir_all(target)?;
            }
            continue;
        }
        let bytes = fs::symlink_metadata(entry.path())?.len();
        if let Some(target) = target {
            if let Some(parent) = target.parent() {
                fs::create_dir_all(parent)?;
            }
            fs::copy(entry.path(), target)?;
        }
        file_count = file_count.saturating_add(1);
        byte_count = byte_count.saturating_add(bytes);
    }
    Ok((file_count, byte_count))
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
            trigger,
            remote,
            fetch,
            credential_id,
            disabled,
        } => {
            let project_record = storage
                .get_project_by_name(&project)?
                .ok_or_else(|| format!("project not found: {project}"))?;
            let name = validate_schedule_name(name)?;
            let expression = CronExpression::parse(&expression)?;
            let next_run_at = expression.next_after(Utc::now())?;
            let poll = match trigger {
                ScheduleTrigger::Build => {
                    if remote != "origin" || fetch || credential_id.is_some() {
                        return Err(
                            "--remote, --fetch, and --credential-id require --trigger repository-poll"
                                .into(),
                        );
                    }
                    None
                }
                ScheduleTrigger::RepositoryPoll => Some(SchedulePollConfig {
                    remote,
                    fetch,
                    credential_id,
                }),
            };
            let schedule = storage.create_schedule_with_trigger_and_poll(
                project_record.id,
                name,
                expression.expression(),
                trigger,
                poll,
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
                    "{}\t{}\t{}\t{}\t{}\t{}",
                    schedule.id,
                    if schedule.enabled {
                        "enabled"
                    } else {
                        "disabled"
                    },
                    schedule.name,
                    schedule.expression,
                    schedule.trigger.as_str(),
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

fn manage_upstream(
    data_dir: &Path,
    command: UpstreamCommand,
) -> Result<(), Box<dyn std::error::Error>> {
    let storage = open_storage(data_dir)?;
    match command {
        UpstreamCommand::Create {
            downstream,
            upstream,
        } => {
            let downstream_record = storage
                .get_project_by_name(&downstream)?
                .ok_or_else(|| format!("project not found: {downstream}"))?;
            let upstream_record = storage
                .get_project_by_name(&upstream)?
                .ok_or_else(|| format!("project not found: {upstream}"))?;
            if downstream_record.id == upstream_record.id {
                return Err("an upstream trigger cannot reference the same project".into());
            }
            if storage.pipeline_trigger_reaches(downstream_record.id, upstream_record.id)? {
                return Err("upstream trigger would create a pipeline cycle".into());
            }
            let trigger = storage
                .create_pipeline_trigger(upstream_record.id, downstream_record.id)?
                .ok_or_else(|| "upstream trigger already exists".to_owned())?;
            println!(
                "Created upstream trigger {}: {} -> {}",
                trigger.id, upstream, downstream
            );
        }
        UpstreamCommand::List { downstream } => {
            let downstream_record = storage
                .get_project_by_name(&downstream)?
                .ok_or_else(|| format!("project not found: {downstream}"))?;
            let projects = storage
                .list_projects()?
                .into_iter()
                .map(|project| (project.id, project.name))
                .collect::<HashMap<_, _>>();
            for trigger in storage.list_pipeline_triggers_to(downstream_record.id)? {
                let upstream_name = projects
                    .get(&trigger.upstream_project_id)
                    .map(String::as_str)
                    .unwrap_or("<missing>");
                println!(
                    "{}\t{}\t{}\t{}",
                    trigger.id,
                    if trigger.enabled {
                        "enabled"
                    } else {
                        "disabled"
                    },
                    upstream_name,
                    trigger.created_at.to_rfc3339()
                );
            }
        }
        UpstreamCommand::Delete { downstream, id } => {
            let downstream_record = storage
                .get_project_by_name(&downstream)?
                .ok_or_else(|| format!("project not found: {downstream}"))?;
            if !storage.delete_pipeline_trigger(downstream_record.id, id)? {
                return Err(format!("upstream trigger not found: {downstream} {id}").into());
            }
            println!("Deleted upstream trigger {id}");
        }
    }
    Ok(())
}

fn manage_provider_trigger(
    data_dir: &Path,
    command: ProviderTriggerCommand,
) -> Result<(), Box<dyn std::error::Error>> {
    let storage = open_storage(data_dir)?;
    match command {
        ProviderTriggerCommand::Create {
            downstream,
            provider,
            source_repository,
            source_pipeline,
        } => {
            let downstream_record = storage
                .get_project_by_name(&downstream)?
                .ok_or_else(|| format!("project not found: {downstream}"))?;
            let provider = provider.trim().to_ascii_lowercase();
            if !matches!(provider.as_str(), "github" | "gitlab" | "bitbucket") {
                return Err("provider must be github, gitlab, or bitbucket".into());
            }
            let source_repository = source_repository.trim();
            if source_repository.is_empty()
                || source_repository.len() > 512
                || source_repository.chars().any(char::is_control)
            {
                return Err(
                    "source repository must be non-empty, bounded, and free of control characters"
                        .into(),
                );
            }
            let source_pipeline = source_pipeline
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_owned);
            if source_pipeline
                .as_deref()
                .is_some_and(|value| value.len() > 512 || value.chars().any(char::is_control))
            {
                return Err(
                    "source pipeline must be bounded and free of control characters".into(),
                );
            }
            let trigger = storage
                .create_provider_trigger(
                    downstream_record.id,
                    &provider,
                    source_repository,
                    source_pipeline.as_deref(),
                )?
                .ok_or_else(|| "provider trigger already exists".to_owned())?;
            println!(
                "Created provider trigger {}: {} {} [{}] -> {}",
                trigger.id,
                trigger.provider,
                trigger.source_repository,
                trigger.source_pipeline.as_deref().unwrap_or("<any>"),
                downstream
            );
        }
        ProviderTriggerCommand::List { downstream } => {
            let downstream_record = storage
                .get_project_by_name(&downstream)?
                .ok_or_else(|| format!("project not found: {downstream}"))?;
            for trigger in storage.list_provider_triggers_to(downstream_record.id)? {
                println!(
                    "{}\t{}\t{}\t{}\t{}",
                    trigger.id,
                    if trigger.enabled {
                        "enabled"
                    } else {
                        "disabled"
                    },
                    trigger.provider,
                    trigger.source_repository,
                    trigger.source_pipeline.as_deref().unwrap_or("<any>"),
                );
            }
        }
        ProviderTriggerCommand::Delete { downstream, id } => {
            let downstream_record = storage
                .get_project_by_name(&downstream)?
                .ok_or_else(|| format!("project not found: {downstream}"))?;
            if !storage.delete_provider_trigger(downstream_record.id, id)? {
                return Err(format!("provider trigger not found: {downstream} {id}").into());
            }
            println!("Deleted provider trigger {id}");
        }
    }
    Ok(())
}

fn parse_schedule_trigger(value: &str) -> Result<ScheduleTrigger, String> {
    match value.trim() {
        "build" => Ok(ScheduleTrigger::Build),
        "repository_poll" | "repository-poll" => Ok(ScheduleTrigger::RepositoryPoll),
        other => Err(format!(
            "invalid schedule trigger {other:?}; expected build or repository_poll"
        )),
    }
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

fn manage_auth(_data_dir: &Path, command: AuthCommand) -> Result<(), Box<dyn std::error::Error>> {
    match command {
        AuthCommand::Token { command } => match command {
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
        },
        AuthCommand::User { command } => manage_auth_user(command)?,
    }
    Ok(())
}

fn manage_auth_user(command: AuthUserCommand) -> Result<(), Box<dyn std::error::Error>> {
    match command {
        AuthUserCommand::Create {
            username,
            role,
            projects,
            users_file,
            password_file,
        } => {
            if users_file == password_file {
                return Err("users file and password file must be different paths".into());
            }
            let password = read_private_value(&password_file, "user password")?;
            let mut document = load_auth_users_for_create(&users_file)?;
            if document
                .users
                .iter()
                .any(|user| user.username.eq_ignore_ascii_case(&username))
            {
                return Err(format!("authentication username already exists: {username}").into());
            }
            document.users.push(AuthUserRecord {
                id: uuid::Uuid::new_v4().to_string(),
                username: username.clone(),
                password_hash: hash_password(&password)?,
                role: role.into(),
                projects,
                disabled: false,
            });
            write_auth_users(&users_file, &document)?;
            println!("Created local user {username}");
        }
        AuthUserCommand::List { users_file } => {
            let document = load_auth_users_document(&users_file)?;
            for user in document.users {
                let projects = if user.projects.is_empty() {
                    "*".to_owned()
                } else {
                    user.projects.join(",")
                };
                println!(
                    "{}\t{}\t{}\t{}\t{}",
                    user.id,
                    user.username,
                    auth_role_label(user.role),
                    projects,
                    if user.disabled { "disabled" } else { "active" }
                );
            }
        }
        AuthUserCommand::Disable {
            username,
            users_file,
        } => update_auth_user_state(&users_file, &username, true)?,
        AuthUserCommand::Enable {
            username,
            users_file,
        } => update_auth_user_state(&users_file, &username, false)?,
        AuthUserCommand::Password {
            username,
            users_file,
            password_file,
        } => {
            if users_file == password_file {
                return Err("users file and password file must be different paths".into());
            }
            let password = read_private_value(&password_file, "user password")?;
            let mut document = load_auth_users_document(&users_file)?;
            let user = document
                .users
                .iter_mut()
                .find(|user| user.username.eq_ignore_ascii_case(&username))
                .ok_or_else(|| format!("authentication username not found: {username}"))?;
            user.password_hash = hash_password(&password)?;
            let username = user.username.clone();
            write_auth_users(&users_file, &document)?;
            println!("Updated password for local user {username}");
        }
        AuthUserCommand::Remove {
            username,
            users_file,
        } => {
            let mut document = load_auth_users_document(&users_file)?;
            let position = document
                .users
                .iter()
                .position(|user| user.username.eq_ignore_ascii_case(&username))
                .ok_or_else(|| format!("authentication username not found: {username}"))?;
            let user = &document.users[position];
            if user.role == AuthRole::Admin
                && !user.disabled
                && document
                    .users
                    .iter()
                    .filter(|candidate| candidate.role == AuthRole::Admin && !candidate.disabled)
                    .count()
                    <= 1
            {
                return Err("cannot remove the last active administrator".into());
            }
            let removed = document.users.remove(position);
            write_auth_users(&users_file, &document)?;
            println!("Removed local user {}", removed.username);
        }
    }
    Ok(())
}

fn auth_role_label(role: AuthRole) -> &'static str {
    match role {
        AuthRole::Admin => "admin",
        AuthRole::Operator => "operator",
        AuthRole::Viewer => "viewer",
        AuthRole::Agent => "agent",
    }
}

fn update_auth_user_state(
    path: &Path,
    username: &str,
    disabled: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut document = load_auth_users_document(path)?;
    let position = document
        .users
        .iter()
        .position(|user| user.username.eq_ignore_ascii_case(username))
        .ok_or_else(|| format!("authentication username not found: {username}"))?;
    if disabled
        && document.users[position].role == AuthRole::Admin
        && !document.users[position].disabled
        && document
            .users
            .iter()
            .filter(|user| user.role == AuthRole::Admin && !user.disabled)
            .count()
            <= 1
    {
        return Err("cannot disable the last active administrator".into());
    }
    document.users[position].disabled = disabled;
    let username = document.users[position].username.clone();
    write_auth_users(path, &document)?;
    println!(
        "{} local user {username}",
        if disabled { "Disabled" } else { "Enabled" }
    );
    Ok(())
}

fn load_auth_users_for_create(
    path: &Path,
) -> Result<AuthUsersDocument, Box<dyn std::error::Error>> {
    match fs::symlink_metadata(path) {
        Ok(_) => load_auth_users_document(path),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(AuthUsersDocument {
            version: AUTH_USERS_VERSION,
            users: Vec::new(),
        }),
        Err(error) => Err(error.into()),
    }
}

fn load_auth_users_document(path: &Path) -> Result<AuthUsersDocument, Box<dyn std::error::Error>> {
    let bytes = read_private_bytes(path, "authentication users file")?;
    let document: AuthUsersDocument = serde_json::from_slice(&bytes)?;
    AuthUsers::from_document(document.clone())?;
    Ok(document)
}

fn write_auth_users(
    path: &Path,
    document: &AuthUsersDocument,
) -> Result<(), Box<dyn std::error::Error>> {
    AuthUsers::from_document(document.clone())?;
    let mut bytes = serde_json::to_vec_pretty(document)?;
    bytes.push(b'\n');
    write_private_atomic(path, &bytes, true, "authentication users file")
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
            kind,
            username,
            owner,
            projects,
            secret_file,
            passphrase_file,
            vault_file,
        } => {
            let vault_path = vault_file.unwrap_or_else(|| data_dir.join("credentials.vault"));
            let passphrase = read_private_value(&passphrase_file, "credential vault passphrase")?;
            let secret = read_private_value(&secret_file, "credential secret")?;
            let mut vault = CredentialVault::open_or_create(&vault_path, passphrase)?;
            match kind {
                CredentialKindArg::HttpBasic => {
                    if let Some(owner) = owner {
                        vault.set_http_basic_for_projects_owned(
                            id.clone(),
                            owner,
                            username,
                            secret,
                            projects,
                        )?;
                    } else {
                        vault.set_http_basic_for_projects(
                            id.clone(),
                            username,
                            secret,
                            projects,
                        )?;
                    }
                }
                CredentialKindArg::SshKey => {
                    if let Some(owner) = owner {
                        vault.set_ssh_key_for_projects_owned(
                            id.clone(),
                            owner,
                            username,
                            secret,
                            projects,
                        )?;
                    } else {
                        vault.set_ssh_key_for_projects(id.clone(), username, secret, projects)?;
                    }
                }
            }
            println!("Stored credential {id} in {}", vault.path().display());
        }
        CredentialCommand::KeychainSet {
            account,
            passphrase_file,
            service,
        } => {
            let passphrase = read_private_value(&passphrase_file, "credential vault passphrase")?;
            CredentialKeychain::new(service.clone())?.set_passphrase(&account, &passphrase)?;
            println!(
                "Stored the vault passphrase in the OS keychain service {service}, account {account}"
            );
        }
        CredentialCommand::KeychainRemove { account, service } => {
            CredentialKeychain::new(service.clone())?.delete_passphrase(&account)?;
            println!(
                "Removed the vault passphrase from the OS keychain service {service}, account {account}"
            );
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
                println!(
                    "{}\t{}\t{}\t{}\towner={}",
                    credential.id,
                    match credential.kind {
                        CredentialKind::HttpBasic => "http-basic",
                        CredentialKind::SshKey => "ssh-key",
                    },
                    credential.username,
                    scope,
                    credential.owner
                );
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

async fn create_project(
    storage: &Storage,
    args: CreateProject,
) -> Result<(), Box<dyn std::error::Error>> {
    let remote_requested = args.repository_url.is_some();
    let repository_url = args
        .repository_url
        .as_deref()
        .map(str::trim)
        .filter(|url| !url.is_empty());
    if remote_requested && repository_url.is_none() {
        return Err("--repository-url cannot be empty".into());
    }
    let repository = if let Some(repository_url) = repository_url {
        if args.repository != Path::new(".") {
            return Err("--repository cannot be combined with --repository-url".into());
        }
        let destination = args
            .clone_destination
            .clone()
            .ok_or("--clone-destination is required with --repository-url")?;
        let credential = load_git_credential(
            args.credential_id.as_deref(),
            args.credentials_file.as_deref(),
            args.credentials_passphrase_file.as_deref(),
            Some(&args.name),
        )?;
        let snapshot = GitRepository::clone_repository_with_auth(
            &GitCloneOptions {
                remote: repository_url.to_owned(),
                destination,
                branch: args.branch.clone(),
                depth: args.depth,
                revision: args.revision.clone(),
                submodules: args.submodules,
                credential_id: args.credential_id.clone(),
                known_hosts_file: args.ssh_known_hosts_file.clone(),
            },
            credential.as_ref(),
        )
        .await?;
        snapshot.root
    } else {
        if args.clone_destination.is_some()
            || args.branch.is_some()
            || args.depth.is_some()
            || args.revision.is_some()
            || args.submodules
            || args.credential_id.is_some()
            || args.credentials_file.is_some()
            || args.credentials_passphrase_file.is_some()
            || args.ssh_known_hosts_file.is_some()
        {
            return Err("remote clone options require --repository-url".into());
        }
        fs::canonicalize(&args.repository)?
    };
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
        || args.submodules
        || args.remote != "origin"
        || args.credential_id.is_some()
        || args.ssh_known_hosts_file.is_some()
    {
        Some(GitPrepareOptions {
            remote: args.remote,
            fetch: args.fetch,
            revision: args.revision,
            fetch_ref: None,
            clean: args.clean,
            clean_ignored: args.clean_ignored,
            submodules: args.submodules,
            credential_id: args.credential_id.clone(),
            known_hosts_file: args.ssh_known_hosts_file.clone(),
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
    credential: Option<GitCredential>,
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
    credential: Option<&GitCredential>,
) -> Result<Option<SourceSnapshot>, ScmError> {
    match GitRepository::open(path).await {
        Ok(repository) => {
            let snapshot = match prepare {
                Some(options) => repository.prepare_with_auth(options, credential).await?,
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
) -> Result<Option<GitCredential>, Box<dyn std::error::Error>> {
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
            let resolved = match credential.kind() {
                CredentialKind::HttpBasic => GitCredential::HttpBasic(GitHttpCredential::new(
                    credential.username(),
                    credential.secret(),
                )?),
                CredentialKind::SshKey => GitCredential::SshKey(GitSshCredential::new(
                    credential.username(),
                    credential.secret(),
                )?),
            };
            Ok(Some(resolved))
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

fn inspect_build(
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
    let details = storage
        .get_build_details(build.id)?
        .ok_or_else(|| format!("build details not found: {name} #{number}"))?;

    println!("Project: {}", project.name);
    println!("Build: #{} ({})", details.build.number, details.build.id);
    println!("Status: {}", status_label(&details.build.status));
    println!("Queued: {}", details.build.queued_at.to_rfc3339());
    if let Some(started_at) = details.build.started_at {
        println!("Started: {}", started_at.to_rfc3339());
    }
    if let Some(finished_at) = details.build.finished_at {
        println!("Finished: {}", finished_at.to_rfc3339());
    }
    if let Some(source) = details.build.source {
        println!(
            "Source: {} {} {} ({})",
            source.provider,
            source.revision,
            source.reference.as_deref().unwrap_or("detached"),
            if source.dirty { "dirty" } else { "clean" }
        );
    }
    if !details.build.parameters.is_empty() {
        println!("Parameters:");
        for (name, value) in details.build.parameters {
            println!("  {name} = {value}");
        }
    }

    println!("Stages:");
    for stage in details.stages {
        println!(
            "  [{}] {}",
            status_label(&stage.stage.status),
            stage.stage.name
        );
        for step in stage.steps {
            let exit_code = step
                .exit_code
                .map_or_else(|| "—".to_owned(), |code| code.to_string());
            println!(
                "    [{}] {} (exit {})",
                status_label(&step.status),
                step.name,
                exit_code
            );
        }
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

fn cancel_endpoint(
    server: &str,
    project: &str,
    number: i64,
) -> Result<reqwest::Url, Box<dyn std::error::Error>> {
    let mut endpoint = reqwest::Url::parse(server.trim_end_matches('/'))?;
    if !matches!(endpoint.scheme(), "http" | "https")
        || !endpoint.username().is_empty()
        || endpoint.password().is_some()
        || endpoint.query().is_some()
        || endpoint.fragment().is_some()
    {
        return Err("server must be a credential-free HTTP(S) origin or base URL".into());
    }
    {
        let mut segments = endpoint
            .path_segments_mut()
            .map_err(|_| "server URL cannot be used as a base URL")?;
        segments.pop_if_empty();
        segments
            .push("api")
            .push("v1")
            .push("projects")
            .push(project)
            .push("builds")
            .push(&number.to_string())
            .push("cancel");
    }
    Ok(endpoint)
}

fn repository_poll_endpoint(
    server: &str,
    project: &str,
) -> Result<reqwest::Url, Box<dyn std::error::Error>> {
    let mut endpoint = parse_capture_base_url(server, "Rivet server")?;
    append_capture_segments(
        &mut endpoint,
        ["api", "v1", "projects", project, "repository-changes"],
    )?;
    Ok(endpoint)
}

#[derive(Debug, Serialize)]
struct RepositoryPollRequestBody {
    remote: String,
    fetch: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    credential_id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct RepositoryPollResult {
    status: String,
    revision: String,
    build: Option<rivet_storage::BuildRecord>,
}

async fn poll_remote_repository(args: PollArgs) -> Result<(), Box<dyn std::error::Error>> {
    let endpoint = repository_poll_endpoint(&args.server, &args.project)?;
    let token = args
        .token_file
        .as_deref()
        .map(read_auth_token)
        .transpose()?;
    let client = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(5))
        .timeout(Duration::from_secs(30))
        .user_agent("rivet-cli/0.1")
        .build()?;
    let request_body = RepositoryPollRequestBody {
        remote: args.remote,
        fetch: args.fetch,
        credential_id: args.credential_id,
    };
    let mut request = client
        .post(endpoint)
        .header("accept", "application/json")
        .header("x-request-id", uuid::Uuid::new_v4().to_string())
        .json(&request_body);
    if let Some(token) = token {
        request = request.bearer_auth(token);
    }
    // A poll mutates admission state when a revision changes. Do not retry an
    // uncertain response and risk turning one operator action into a duplicate.
    let response = request.send().await?;
    let status = response.status();
    let body = response.bytes().await?;
    if body.len() > 64 * 1024 {
        return Err("server returned an oversized repository poll response".into());
    }
    if !matches!(
        status,
        reqwest::StatusCode::OK | reqwest::StatusCode::ACCEPTED
    ) {
        let message = serde_json::from_slice::<serde_json::Value>(&body)
            .ok()
            .and_then(|value| {
                value
                    .get("error")
                    .and_then(|error| error.as_str())
                    .map(str::to_owned)
            })
            .filter(|message| !message.trim().is_empty())
            .or_else(|| {
                let message = String::from_utf8_lossy(&body).trim().to_owned();
                (!message.is_empty()).then_some(message)
            })
            .unwrap_or_else(|| status.to_string());
        return Err(format!(
            "server refused repository poll for {}: {message}",
            args.project
        )
        .into());
    }
    let result: RepositoryPollResult = serde_json::from_slice(&body)?;
    let build = result
        .build
        .as_ref()
        .map(|build| format!(" #{}", build.number))
        .unwrap_or_default();
    println!(
        "Repository poll {}: {}{} ({})",
        args.project, result.status, build, result.revision
    );
    Ok(())
}

async fn cancel_remote_build(
    project: &str,
    number: i64,
    server: &str,
    token_file: Option<&Path>,
) -> Result<(), Box<dyn std::error::Error>> {
    let endpoint = cancel_endpoint(server, project, number)?;
    let token = token_file.map(read_auth_token).transpose()?;
    let client = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(5))
        .timeout(Duration::from_secs(15))
        .build()?;
    let mut request = client
        .post(endpoint)
        .header("accept", "application/json")
        .header("x-request-id", uuid::Uuid::new_v4().to_string());
    if let Some(token) = token {
        request = request.bearer_auth(token);
    }
    // This is deliberately one request: retrying a mutation after a network
    // interruption could turn an uncertain cancellation into duplicate work.
    let response = request.send().await?;
    let status = response.status();
    let body = response.bytes().await?;
    if body.len() > 64 * 1024 {
        return Err("server returned an oversized cancellation response".into());
    }
    if status == reqwest::StatusCode::ACCEPTED {
        println!("Cancellation requested for {project} #{number}");
        return Ok(());
    }
    let message = serde_json::from_slice::<serde_json::Value>(&body)
        .ok()
        .and_then(|value| {
            value
                .get("error")
                .and_then(|error| error.as_str())
                .map(str::to_owned)
        })
        .filter(|message| !message.trim().is_empty())
        .or_else(|| {
            let message = String::from_utf8_lossy(&body).trim().to_owned();
            (!message.is_empty()).then_some(message)
        })
        .unwrap_or_else(|| status.to_string());
    Err(format!("server refused cancellation for {project} #{number}: {message}").into())
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cancel_endpoint_appends_encoded_build_route_to_a_base_url() {
        let endpoint = cancel_endpoint("https://ci.example.test/rivet/", "team alpha", 42)
            .expect("cancel endpoint");
        assert_eq!(
            endpoint.as_str(),
            "https://ci.example.test/rivet/api/v1/projects/team%20alpha/builds/42/cancel"
        );
    }

    #[test]
    fn cancel_endpoint_rejects_credentials_query_and_non_http_urls() {
        for server in [
            "https://user:secret@ci.example.test",
            "https://ci.example.test?token=secret",
            "ws://ci.example.test",
        ] {
            assert!(cancel_endpoint(server, "project", 1).is_err(), "{server}");
        }
    }

    #[test]
    fn repository_poll_endpoint_encodes_project_without_exposing_credentials() {
        let endpoint = repository_poll_endpoint("https://ci.example.test/rivet/", "team alpha")
            .expect("repository poll endpoint");
        assert_eq!(
            endpoint.as_str(),
            "https://ci.example.test/rivet/api/v1/projects/team%20alpha/repository-changes"
        );
        assert!(!endpoint.as_str().contains("credential"));
    }

    #[test]
    fn repository_poll_request_omits_absent_credential_ids() {
        let body = serde_json::to_value(RepositoryPollRequestBody {
            remote: "origin".into(),
            fetch: false,
            credential_id: None,
        })
        .expect("poll body");
        assert_eq!(
            body,
            serde_json::json!({"remote": "origin", "fetch": false})
        );
    }

    #[test]
    fn compatibility_capture_urls_encode_project_and_nested_job_segments() {
        let rivet = rivet_capture_endpoint(
            "https://ci.example.test/rivet/",
            "team alpha",
            42,
            &["artifacts"],
        )
        .expect("Rivet capture URL");
        assert_eq!(
            rivet.as_str(),
            "https://ci.example.test/rivet/api/v1/projects/team%20alpha/builds/42/artifacts"
        );

        let jenkins = jenkins_capture_endpoint(
            "https://ci.example.test/jenkins/",
            "folder/service api",
            7,
            &["wfapi", "describe"],
        )
        .expect("Jenkins capture URL");
        assert_eq!(
            jenkins.as_str(),
            "https://ci.example.test/jenkins/job/folder/job/service%20api/7/wfapi/describe"
        );
    }

    #[test]
    fn compatibility_capture_maps_statuses_and_redacts_sensitive_parameters() {
        assert_eq!(jenkins_status(Some("SUCCESS"), false).unwrap(), "passed");
        assert_eq!(
            jenkins_status(Some("IN_PROGRESS"), false).unwrap(),
            "running"
        );
        assert_eq!(jenkins_status(None, true).unwrap(), "running");
        assert!(jenkins_status(Some("plugin-specific"), false).is_err());

        let payload = serde_json::json!({
            "actions": [{
                "parameters": [
                    {"name": "TARGET", "value": "release"},
                    {"name": "DEPLOY_TOKEN", "value": "must-not-persist"}
                ]
            }]
        });
        let parameters = parse_jenkins_parameters(&payload);
        assert_eq!(parameters.get("TARGET"), Some(&"release".to_owned()));
        assert_eq!(
            parameters.get("DEPLOY_TOKEN"),
            Some(&"<redacted>".to_owned())
        );
    }

    #[test]
    fn compatibility_capture_rejects_unbounded_logs() {
        assert!(validate_capture_logs(vec!["ok".into()]).is_ok());
        assert!(
            validate_capture_logs(vec!["x".repeat(MAX_COMPAT_CAPTURE_LOG_LINE_BYTES + 1)]).is_err()
        );
        assert!(
            validate_capture_logs(vec!["line".into(); MAX_COMPAT_CAPTURE_LOG_LINES + 1]).is_err()
        );
    }

    #[test]
    fn upstream_cli_manages_persisted_triggers() {
        let data_dir =
            std::env::temp_dir().join(format!("rivet-cli-trigger-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&data_dir).expect("data directory");
        let storage = Storage::open(data_dir.join("rivet.db")).expect("storage");
        let pipeline = Pipeline::from_toml_str(
            r#"
version = 1
name = "trigger"
[[stages]]
name = "Test"
[[stages.steps]]
name = "unit"
program = "true"
"#,
        )
        .expect("pipeline");
        let upstream = Project::new("upstream", ".", "Rivetfile.toml").expect("upstream");
        let downstream = Project::new("downstream", ".", "Rivetfile.toml").expect("downstream");
        storage
            .create_project(&upstream, &pipeline)
            .expect("upstream project");
        storage
            .create_project(&downstream, &pipeline)
            .expect("downstream project");
        drop(storage);

        manage_upstream(
            &data_dir,
            UpstreamCommand::Create {
                downstream: "downstream".into(),
                upstream: "upstream".into(),
            },
        )
        .expect("create upstream trigger");
        let storage = Storage::open(data_dir.join("rivet.db")).expect("reopen storage");
        let trigger = storage
            .list_pipeline_triggers_to(downstream.id)
            .expect("triggers")
            .pop()
            .expect("trigger");
        assert_eq!(trigger.upstream_project_id, upstream.id);
        assert!(
            manage_upstream(
                &data_dir,
                UpstreamCommand::Create {
                    downstream: "downstream".into(),
                    upstream: "upstream".into(),
                },
            )
            .is_err()
        );
        manage_upstream(
            &data_dir,
            UpstreamCommand::Delete {
                downstream: "downstream".into(),
                id: trigger.id,
            },
        )
        .expect("delete upstream trigger");
        assert!(
            storage
                .list_pipeline_triggers_to(downstream.id)
                .expect("triggers")
                .is_empty()
        );
        drop(storage);
        fs::remove_dir_all(data_dir).expect("remove test data directory");
    }

    #[test]
    fn provider_trigger_cli_manages_persisted_mappings() {
        let data_dir = std::env::temp_dir().join(format!(
            "rivet-cli-provider-trigger-{}",
            uuid::Uuid::new_v4()
        ));
        fs::create_dir_all(&data_dir).expect("data directory");
        let storage = Storage::open(data_dir.join("rivet.db")).expect("storage");
        let pipeline = Pipeline::from_toml_str(
            r#"
version = 1
name = "provider-trigger"
[[stages]]
name = "Test"
[[stages.steps]]
name = "unit"
program = "true"
"#,
        )
        .expect("pipeline");
        let downstream = Project::new("downstream", ".", "Rivetfile.toml").expect("project");
        storage
            .create_project(&downstream, &pipeline)
            .expect("downstream project");
        drop(storage);

        manage_provider_trigger(
            &data_dir,
            ProviderTriggerCommand::Create {
                downstream: "downstream".into(),
                provider: "GitHub".into(),
                source_repository: "acme/widgets".into(),
                source_pipeline: Some("Release".into()),
            },
        )
        .expect("create provider trigger");
        let storage = Storage::open(data_dir.join("rivet.db")).expect("reopen storage");
        let trigger = storage
            .list_provider_triggers_to(downstream.id)
            .expect("provider triggers")
            .pop()
            .expect("provider trigger");
        assert_eq!(trigger.provider, "github");
        assert_eq!(trigger.source_repository, "acme/widgets");
        assert_eq!(trigger.source_pipeline.as_deref(), Some("Release"));
        assert!(
            manage_provider_trigger(
                &data_dir,
                ProviderTriggerCommand::Create {
                    downstream: "downstream".into(),
                    provider: "github".into(),
                    source_repository: "acme/widgets".into(),
                    source_pipeline: Some("Release".into()),
                },
            )
            .is_err()
        );
        manage_provider_trigger(
            &data_dir,
            ProviderTriggerCommand::Delete {
                downstream: "downstream".into(),
                id: trigger.id,
            },
        )
        .expect("delete provider trigger");
        assert!(
            storage
                .list_provider_triggers_to(downstream.id)
                .expect("deleted provider triggers")
                .is_empty()
        );
        drop(storage);
        fs::remove_dir_all(data_dir).expect("remove test data directory");
    }
}
