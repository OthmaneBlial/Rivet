use clap::{Args, Parser, Subcommand};
use rivet_core::{BuildEvent, BuildStatus, ExecutionPlan, LogStream, Pipeline, Project};
use rivet_runner::{QueueHandle, Scheduler};
use rivet_scm::{GitPrepareOptions, GitRepository};
use rivet_storage::Storage;
use std::fs;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use tokio::sync::mpsc;
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
    Run { project: String },
    /// Show persisted build history.
    Builds { project: String },
    /// Show persisted output for a build number.
    Logs {
        project: String,
        #[arg(long)]
        build: i64,
    },
    /// Run the headless HTTP/WebSocket service.
    Server {
        #[arg(long, default_value = "127.0.0.1:7878")]
        bind: SocketAddr,
    },
    /// Inspect or explicitly prepare a local Git repository.
    Scm {
        #[command(subcommand)]
        command: ScmCommand,
    },
}

#[derive(Debug, Subcommand)]
enum ProjectCommand {
    Create(CreateProject),
    List,
}

#[derive(Debug, Args)]
struct CreateProject {
    name: String,
    #[arg(long, default_value = ".")]
    repository: PathBuf,
    #[arg(long)]
    pipeline: Option<PathBuf>,
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
        Command::Run { project } => run_project(&cli.data_dir, &project).await?,
        Command::Builds { project } => list_builds(&cli.data_dir, &project)?,
        Command::Logs { project, build } => show_logs(&cli.data_dir, &project, build)?,
        Command::Server { bind } => {
            rivet_server::serve(cli.data_dir.join("rivet.db"), bind).await?
        }
        Command::Scm { command } => inspect_scm(command).await?,
    }
    Ok(())
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

async fn run_project(data_dir: &Path, name: &str) -> Result<(), Box<dyn std::error::Error>> {
    let storage = open_storage(data_dir)?;
    let project = storage
        .get_project_by_name(name)?
        .ok_or_else(|| format!("project not found: {name}"))?;
    let pipeline = Pipeline::load(&project.pipeline_path)?;
    let build_id = uuid::Uuid::new_v4();
    let plan = ExecutionPlan::from_pipeline(&pipeline, build_id, project.id);
    let build = storage.create_build(&project, &plan, &pipeline)?;
    println!("Queued {} #{} ({})", project.name, build.number, build.id);

    let (events, mut received_events) = mpsc::channel(512);
    let event_storage = storage.clone();
    let event_consumer = tokio::spawn(async move {
        while let Some(event) = received_events.recv().await {
            print_event(&event);
            event_storage.apply_event(&event)?;
        }
        Ok::<(), rivet_storage::StorageError>(())
    });

    let cancellation = CancellationToken::new();
    let scheduler = Scheduler::new(1, Some(1));
    let handle = scheduler
        .enqueue(
            plan,
            pipeline,
            PathBuf::from(project.repository_path.clone()),
            cancellation.clone(),
            events,
        )
        .await?;
    let status = wait_for_build(handle, cancellation).await?;
    let consume_result = event_consumer.await??;
    let _ = consume_result;

    println!("Build #{} {}", build.number, status_label(&status));
    if status != BuildStatus::Passed {
        return Err(format!("build finished {}", status_label(&status)).into());
    }
    Ok(())
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
