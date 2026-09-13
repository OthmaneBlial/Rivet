use crate::{CacheStore, ProcessOutcome, ProcessSpec, run_process};
use chrono::Utc;
use rivet_core::{
    BuildEvent, BuildStatus, ExecutionPlan, ExecutionStage, ExecutionStep, Pipeline, StageStatus,
    StepStatus,
};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::time::Duration;
use thiserror::Error;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Error)]
pub enum RunnerError {
    #[error(transparent)]
    Pipeline(#[from] rivet_core::PipelineError),
    #[error(transparent)]
    Process(#[from] crate::ProcessError),
    #[error("build event receiver was closed")]
    EventChannelClosed,
    #[error("step working directory escapes the pipeline workspace: {0}")]
    WorkingDirectoryOutsideWorkspace(PathBuf),
    #[error("step {step:?} requires a remote agent; local execution was refused")]
    RemoteAgentRequired { step: String },
    #[error("validated execution graph could not make progress")]
    GraphBlocked,
    #[error("stage execution task failed: {0}")]
    StageTaskJoin(String),
}

/// Execute all stages in a validated plan, running ready independent stages
/// concurrently while honoring the stable dependency graph.
pub async fn execute_pipeline(
    plan: &ExecutionPlan,
    pipeline: &Pipeline,
    repository_root: impl AsRef<Path>,
    cancellation: CancellationToken,
    events: mpsc::Sender<BuildEvent>,
) -> Result<BuildStatus, RunnerError> {
    execute_pipeline_with_parameters(
        plan,
        pipeline,
        repository_root,
        &BTreeMap::new(),
        cancellation,
        events,
    )
    .await
}

pub async fn execute_pipeline_with_parameters(
    plan: &ExecutionPlan,
    pipeline: &Pipeline,
    repository_root: impl AsRef<Path>,
    parameters: &BTreeMap<String, String>,
    cancellation: CancellationToken,
    events: mpsc::Sender<BuildEvent>,
) -> Result<BuildStatus, RunnerError> {
    execute_pipeline_with_parameters_and_cache(
        plan,
        pipeline,
        repository_root,
        parameters,
        cancellation,
        events,
        None,
    )
    .await
}

pub async fn execute_pipeline_with_parameters_and_cache(
    plan: &ExecutionPlan,
    pipeline: &Pipeline,
    repository_root: impl AsRef<Path>,
    parameters: &BTreeMap<String, String>,
    cancellation: CancellationToken,
    events: mpsc::Sender<BuildEvent>,
    cache_root: Option<&Path>,
) -> Result<BuildStatus, RunnerError> {
    pipeline.validate()?;
    let parameters = pipeline.resolve_parameters(parameters)?;
    let secret_values = pipeline.secret_values(&parameters);
    let workspace = pipeline.resolve_workspace(repository_root)?;
    if cancellation.is_cancelled() {
        return finish_cancelled(plan, &events).await;
    }
    if let Some(cache_root) = cache_root {
        let cache_store = CacheStore::new(cache_root);
        for cache in &pipeline.caches {
            match cache_store.restore(plan.project_id, cache, &workspace) {
                Ok(true) => {
                    tracing::debug!(cache = %cache.name, key = %cache.key, "restored CI cache")
                }
                Ok(false) => {
                    tracing::debug!(cache = %cache.name, key = %cache.key, "CI cache miss")
                }
                Err(error) => {
                    tracing::warn!(cache = %cache.name, ?error, "could not restore CI cache")
                }
            }
        }
    }
    send(
        &events,
        BuildEvent::BuildStarted {
            build_id: plan.build_id,
            timestamp: Utc::now(),
        },
    )
    .await?;

    let execution_cancellation = cancellation.child_token();
    let mut completed = HashMap::new();
    let mut running = HashSet::new();
    let mut stage_tasks = tokio::task::JoinSet::new();
    let mut failed = false;
    let mut first_error = None;

    while completed.len() < plan.stages.len() && !failed {
        let mut made_progress = false;
        if cancellation.is_cancelled() {
            execution_cancellation.cancel();
        }
        if !cancellation.is_cancelled() {
            for stage in &plan.stages {
                if completed.contains_key(&stage.id) || running.contains(&stage.id) {
                    continue;
                }
                let dependencies_complete = stage
                    .depends_on
                    .iter()
                    .all(|dependency| completed.contains_key(dependency));
                if !dependencies_complete {
                    continue;
                }
                let dependencies_blocked = stage.depends_on.iter().any(|dependency| {
                    !matches!(completed.get(dependency), Some(StageStatus::Passed))
                });
                let condition_passed = pipeline
                    .stages
                    .iter()
                    .find(|definition| definition.name == stage.name)
                    .and_then(|definition| definition.condition.as_ref())
                    .is_none_or(|condition| condition.evaluate(&parameters));
                if dependencies_blocked || !condition_passed {
                    send_stage_finished(plan, stage, StageStatus::Skipped, &events).await?;
                    completed.insert(stage.id, StageStatus::Skipped);
                    made_progress = true;
                    continue;
                }
                let stage_id = stage.id;
                running.insert(stage_id);
                let plan = plan.clone();
                let pipeline = pipeline.clone();
                let stage = stage.clone();
                let workspace = workspace.clone();
                let parameters = parameters.clone();
                let secret_values = secret_values.clone();
                let stage_cancellation = execution_cancellation.clone();
                let events = events.clone();
                made_progress = true;
                stage_tasks.spawn(async move {
                    let result = execute_stage(
                        &plan,
                        &stage,
                        &pipeline,
                        &workspace,
                        &parameters,
                        &secret_values,
                        &stage_cancellation,
                        &events,
                    )
                    .await;
                    (stage_id, result)
                });
            }
        }

        if stage_tasks.is_empty() && made_progress {
            continue;
        }
        if stage_tasks.is_empty() {
            if cancellation.is_cancelled() {
                break;
            }
            first_error = Some(RunnerError::GraphBlocked);
            failed = true;
            break;
        }

        while let Some(result) = stage_tasks.join_next().await {
            match result {
                Ok((stage_id, Ok(status))) => {
                    running.remove(&stage_id);
                    if status == StageStatus::Failed {
                        failed = true;
                        execution_cancellation.cancel();
                    }
                    completed.insert(stage_id, status);
                }
                Ok((stage_id, Err(error))) => {
                    running.remove(&stage_id);
                    completed.insert(stage_id, StageStatus::Failed);
                    first_error.get_or_insert(error);
                    failed = true;
                    execution_cancellation.cancel();
                }
                Err(error) => {
                    first_error.get_or_insert(RunnerError::StageTaskJoin(error.to_string()));
                    failed = true;
                    execution_cancellation.cancel();
                }
            }
        }
    }

    if cancellation.is_cancelled() {
        return finish_cancelled(plan, &events).await;
    }
    if let Some(error) = first_error {
        let _ = finish_build(plan, BuildStatus::Failed, &events).await;
        return Err(error);
    }
    if failed {
        return finish_build(plan, BuildStatus::Failed, &events).await;
    }
    if completed.len() != plan.stages.len() {
        let _ = finish_build(plan, BuildStatus::Failed, &events).await;
        return Err(RunnerError::GraphBlocked);
    }

    if let Some(cache_root) = cache_root {
        let cache_store = CacheStore::new(cache_root);
        for cache in &pipeline.caches {
            match cache_store.save(plan.project_id, cache, &workspace) {
                Ok(true) => {
                    tracing::debug!(cache = %cache.name, key = %cache.key, "saved CI cache")
                }
                Ok(false) => {
                    tracing::debug!(cache = %cache.name, key = %cache.key, "CI cache already exists")
                }
                Err(error) => {
                    tracing::warn!(cache = %cache.name, ?error, "could not save CI cache")
                }
            }
        }
    }
    finish_build(plan, BuildStatus::Passed, &events).await
}

async fn execute_stage(
    plan: &ExecutionPlan,
    stage: &ExecutionStage,
    pipeline: &Pipeline,
    workspace: &Path,
    parameters: &BTreeMap<String, String>,
    secret_values: &[String],
    cancellation: &CancellationToken,
    events: &mpsc::Sender<BuildEvent>,
) -> Result<StageStatus, RunnerError> {
    send(
        events,
        BuildEvent::StageStarted {
            build_id: plan.build_id,
            stage_id: stage.id,
            stage_name: stage.name.clone(),
            timestamp: Utc::now(),
        },
    )
    .await?;

    for step in &stage.steps {
        if cancellation.is_cancelled() {
            send_stage_finished(plan, stage, StageStatus::Cancelled, events).await?;
            return Ok(StageStatus::Cancelled);
        }
        let result = execute_step(
            plan,
            stage,
            step,
            pipeline,
            workspace,
            parameters,
            secret_values,
            cancellation,
            events,
        )
        .await;
        match result {
            Ok(StepStatus::Passed) => {}
            Ok(StepStatus::Cancelled) => {
                send_stage_finished(plan, stage, StageStatus::Cancelled, events).await?;
                return Ok(StageStatus::Cancelled);
            }
            Ok(StepStatus::Failed)
            | Ok(StepStatus::Skipped)
            | Ok(StepStatus::Pending)
            | Ok(StepStatus::Running) => {
                send_stage_finished(plan, stage, StageStatus::Failed, events).await?;
                return Ok(StageStatus::Failed);
            }
            Err(error) => {
                send_stage_finished(plan, stage, StageStatus::Failed, events).await?;
                return Err(error);
            }
        }
    }
    send_stage_finished(plan, stage, StageStatus::Passed, events).await?;
    Ok(StageStatus::Passed)
}

async fn execute_step(
    plan: &ExecutionPlan,
    stage: &ExecutionStage,
    step: &ExecutionStep,
    pipeline: &Pipeline,
    workspace: &Path,
    parameters: &BTreeMap<String, String>,
    secret_values: &[String],
    cancellation: &CancellationToken,
    events: &mpsc::Sender<BuildEvent>,
) -> Result<StepStatus, RunnerError> {
    if step.definition.agent.is_some() {
        return Err(RunnerError::RemoteAgentRequired {
            step: step.definition.name.clone(),
        });
    }
    let retries = step.definition.retries;
    for attempt in 0..=retries {
        let status = execute_step_attempt(
            plan,
            stage,
            step,
            pipeline,
            workspace,
            parameters,
            secret_values,
            cancellation,
            events,
        )
        .await?;
        if status != StepStatus::Failed || attempt == retries {
            return Ok(status);
        }
        if cancellation.is_cancelled() {
            return Ok(StepStatus::Cancelled);
        }
        send(
            events,
            BuildEvent::StepOutput {
                build_id: plan.build_id,
                stage_id: stage.id,
                step_id: step.id,
                stream: rivet_core::LogStream::System,
                line: format!(
                    "retrying step after failed attempt {} of {}",
                    attempt + 1,
                    retries + 1
                ),
                timestamp: Utc::now(),
            },
        )
        .await?;
        if step.definition.retry_delay_seconds > 0 {
            tokio::select! {
                _ = cancellation.cancelled() => return Ok(StepStatus::Cancelled),
                _ = tokio::time::sleep(Duration::from_secs(step.definition.retry_delay_seconds)) => {}
            }
        }
    }
    unreachable!("a retry loop always returns after its final attempt")
}

async fn execute_step_attempt(
    plan: &ExecutionPlan,
    stage: &ExecutionStage,
    step: &ExecutionStep,
    pipeline: &Pipeline,
    workspace: &Path,
    parameters: &BTreeMap<String, String>,
    secret_values: &[String],
    cancellation: &CancellationToken,
    events: &mpsc::Sender<BuildEvent>,
) -> Result<StepStatus, RunnerError> {
    let working_dir = resolve_working_dir(workspace, step.definition.working_dir.as_deref())?;
    send(
        events,
        BuildEvent::StepStarted {
            build_id: plan.build_id,
            stage_id: stage.id,
            step_id: step.id,
            step_name: step.definition.name.clone(),
            timestamp: Utc::now(),
        },
    )
    .await?;

    let mut env = pipeline.environment.clone();
    env.extend(parameters.clone());
    env.extend(step.definition.env.clone());
    env.insert("CI".into(), "true".into());
    env.insert("RIVET_BUILD_ID".into(), plan.build_id.to_string());
    env.insert("RIVET_PROJECT_ID".into(), plan.project_id.to_string());
    let spec = build_process_spec(step, workspace, working_dir, env);

    let (output_tx, mut output_rx) = mpsc::channel(256);
    let process = run_process(spec, cancellation.clone(), output_tx);
    tokio::pin!(process);
    let process_result = loop {
        tokio::select! {
            line = output_rx.recv() => {
                if let Some(line) = line {
                    send(events, BuildEvent::StepOutput {
                        build_id: plan.build_id,
                        stage_id: stage.id,
                        step_id: step.id,
                        stream: line.stream,
                        line: mask_line(&line.line, secret_values),
                        timestamp: line.timestamp,
                    }).await?;
                }
            }
            result = &mut process => {
                break result?;
            }
        }
    };
    while let Some(line) = output_rx.recv().await {
        send(
            events,
            BuildEvent::StepOutput {
                build_id: plan.build_id,
                stage_id: stage.id,
                step_id: step.id,
                stream: line.stream,
                line: mask_line(&line.line, secret_values),
                timestamp: line.timestamp,
            },
        )
        .await?;
    }

    let status = match process_result.outcome {
        ProcessOutcome::Passed => StepStatus::Passed,
        ProcessOutcome::Cancelled => StepStatus::Cancelled,
        ProcessOutcome::TimedOut => {
            send(
                events,
                BuildEvent::StepOutput {
                    build_id: plan.build_id,
                    stage_id: stage.id,
                    step_id: step.id,
                    stream: rivet_core::LogStream::System,
                    line: "step timed out".into(),
                    timestamp: Utc::now(),
                },
            )
            .await?;
            StepStatus::Failed
        }
        ProcessOutcome::Failed => StepStatus::Failed,
    };
    send(
        events,
        BuildEvent::StepFinished {
            build_id: plan.build_id,
            stage_id: stage.id,
            step_id: step.id,
            step_name: step.definition.name.clone(),
            status: status.clone(),
            exit_code: process_result.exit_code,
            timestamp: Utc::now(),
        },
    )
    .await?;
    Ok(status)
}

fn build_process_spec(
    step: &ExecutionStep,
    workspace: &Path,
    working_dir: PathBuf,
    env: BTreeMap<String, String>,
) -> ProcessSpec {
    let timeout = step.definition.timeout_seconds.map(Duration::from_secs);
    let Some(container) = step.definition.container.as_ref() else {
        return ProcessSpec {
            program: step.definition.program.clone(),
            args: step.definition.args.clone(),
            env,
            working_dir,
            timeout,
        };
    };

    let container_workspace = Path::new("/rivet/workspace");
    let relative_working_dir = working_dir
        .strip_prefix(workspace)
        .unwrap_or_else(|_| Path::new("."));
    let container_working_dir = container_workspace.join(relative_working_dir);
    let mut args = vec![
        "run".to_owned(),
        "--rm".to_owned(),
        "--init".to_owned(),
        "--sig-proxy=true".to_owned(),
        "--pull".to_owned(),
        container.pull.docker_value().to_owned(),
    ];
    if let Some(network) = &container.network {
        args.push("--network".to_owned());
        args.push(network.clone());
    }
    args.extend([
        "--workdir".to_owned(),
        container_working_dir.to_string_lossy().into_owned(),
        "--volume".to_owned(),
        format!("{}:/rivet/workspace:rw", workspace.display()),
    ]);
    for volume in &container.volumes {
        args.push("--volume".to_owned());
        args.push(format!(
            "{}:{}:{}",
            workspace.join(&volume.source).display(),
            volume.target.display(),
            if volume.read_only { "ro" } else { "rw" }
        ));
    }
    for name in env.keys() {
        args.push("--env".to_owned());
        args.push(name.clone());
    }
    args.push(container.image.clone());
    args.push(step.definition.program.clone());
    args.extend(step.definition.args.clone());
    ProcessSpec {
        program: "docker".to_owned(),
        args,
        env,
        working_dir: workspace.to_owned(),
        timeout,
    }
}

fn mask_line(line: &str, secret_values: &[String]) -> String {
    secret_values
        .iter()
        .filter(|secret| !secret.is_empty())
        .fold(line.to_owned(), |line, secret| line.replace(secret, "***"))
}

fn resolve_working_dir(workspace: &Path, requested: Option<&Path>) -> Result<PathBuf, RunnerError> {
    let requested = requested.unwrap_or_else(|| Path::new("."));
    let candidate = if requested.is_absolute() {
        requested.to_path_buf()
    } else {
        workspace.join(requested)
    };
    let normalized = normalize_path(&candidate);
    if !normalized.starts_with(workspace) {
        return Err(RunnerError::WorkingDirectoryOutsideWorkspace(normalized));
    }
    Ok(normalized)
}

fn normalize_path(path: &Path) -> PathBuf {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            std::path::Component::CurDir => {}
            std::path::Component::ParentDir => {
                normalized.pop();
            }
            component => normalized.push(component.as_os_str()),
        }
    }
    normalized
}

async fn send(events: &mpsc::Sender<BuildEvent>, event: BuildEvent) -> Result<(), RunnerError> {
    events
        .send(event)
        .await
        .map_err(|_| RunnerError::EventChannelClosed)
}

async fn send_stage_finished(
    plan: &ExecutionPlan,
    stage: &ExecutionStage,
    status: StageStatus,
    events: &mpsc::Sender<BuildEvent>,
) -> Result<(), RunnerError> {
    send(
        events,
        BuildEvent::StageFinished {
            build_id: plan.build_id,
            stage_id: stage.id,
            stage_name: stage.name.clone(),
            status,
            timestamp: Utc::now(),
        },
    )
    .await
}

async fn finish_cancelled(
    plan: &ExecutionPlan,
    events: &mpsc::Sender<BuildEvent>,
) -> Result<BuildStatus, RunnerError> {
    send(
        events,
        BuildEvent::BuildCancelled {
            build_id: plan.build_id,
            timestamp: Utc::now(),
        },
    )
    .await?;
    finish_build(plan, BuildStatus::Cancelled, events).await
}

async fn finish_build(
    plan: &ExecutionPlan,
    status: BuildStatus,
    events: &mpsc::Sender<BuildEvent>,
) -> Result<BuildStatus, RunnerError> {
    send(
        events,
        BuildEvent::BuildFinished {
            build_id: plan.build_id,
            status: status.clone(),
            timestamp: Utc::now(),
        },
    )
    .await?;
    Ok(status)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use std::fs;
    use std::time::Instant;
    use tempfile::tempdir;

    #[tokio::test]
    async fn executes_stages_in_order_and_emits_live_output() {
        let dir = tempdir().expect("tempdir");
        let pipeline = Pipeline::from_toml_str(
            r#"
version = 1
name = "ordered"
[[stages]]
name = "first"
[[stages.steps]]
name = "write"
program = "sh"
args = ["-c", "printf first > order.txt"]
[[stages]]
name = "second"
depends_on = ["first"]
[[stages.steps]]
name = "read"
program = "sh"
args = ["-c", "printf second; test \"$(cat order.txt)\" = first"]
"#,
        )
        .expect("pipeline");
        let plan =
            ExecutionPlan::from_pipeline(&pipeline, uuid::Uuid::new_v4(), uuid::Uuid::new_v4());
        let (tx, mut rx) = mpsc::channel(64);
        let status = execute_pipeline(&plan, &pipeline, dir.path(), CancellationToken::new(), tx)
            .await
            .expect("runner");

        assert_eq!(status, BuildStatus::Passed);
        let mut events = Vec::new();
        while let Some(event) = rx.recv().await {
            events.push(event);
        }
        assert!(
            events.iter().any(
                |event| matches!(event, BuildEvent::StepOutput { line, .. } if line == "second")
            )
        );
        assert_eq!(
            fs::read_to_string(dir.path().join("order.txt")).expect("order file"),
            "first"
        );
        let stage_starts: Vec<_> = events
            .iter()
            .filter_map(|event| match event {
                BuildEvent::StageStarted { stage_name, .. } => Some(stage_name.as_str()),
                _ => None,
            })
            .collect();
        assert_eq!(stage_starts, ["first", "second"]);
    }

    #[tokio::test]
    async fn executes_dependency_order_even_when_stages_are_declared_out_of_order() {
        let dir = tempdir().expect("tempdir");
        let pipeline = Pipeline::from_toml_str(
            r#"
version = 1
name = "dependency-order"
[[stages]]
name = "Build"
depends_on = ["Test"]
[[stages.steps]]
name = "read-test-marker"
program = "sh"
args = ["-c", "test -f test.marker; touch build.marker"]
[[stages]]
name = "Test"
[[stages.steps]]
name = "write-test-marker"
program = "touch"
args = ["test.marker"]
"#,
        )
        .expect("dependency pipeline");
        let plan =
            ExecutionPlan::from_pipeline(&pipeline, uuid::Uuid::new_v4(), uuid::Uuid::new_v4());
        assert_eq!(
            plan.stages
                .iter()
                .map(|stage| stage.name.as_str())
                .collect::<Vec<_>>(),
            ["Test", "Build"]
        );
        assert_eq!(plan.stages[1].depends_on, vec![plan.stages[0].id]);

        let (tx, mut rx) = mpsc::channel(64);
        let status = execute_pipeline(&plan, &pipeline, dir.path(), CancellationToken::new(), tx)
            .await
            .expect("runner");
        assert_eq!(status, BuildStatus::Passed);
        while rx.recv().await.is_some() {}
        assert!(dir.path().join("build.marker").is_file());
    }

    #[tokio::test]
    async fn executes_independent_stages_in_parallel() {
        let dir = tempdir().expect("tempdir");
        let pipeline = Pipeline::from_toml_str(
            r#"
version = 1
name = "parallel"
[[stages]]
name = "left"
[[stages.steps]]
name = "left-step"
program = "sh"
args = ["-c", "sleep 0.2; printf left > left.done"]
[[stages]]
name = "right"
[[stages.steps]]
name = "right-step"
program = "sh"
args = ["-c", "sleep 0.2; printf right > right.done"]
"#,
        )
        .expect("pipeline");
        let plan =
            ExecutionPlan::from_pipeline(&pipeline, uuid::Uuid::new_v4(), uuid::Uuid::new_v4());
        let (tx, mut rx) = mpsc::channel(64);
        let started_at = Instant::now();
        let status = execute_pipeline(&plan, &pipeline, dir.path(), CancellationToken::new(), tx)
            .await
            .expect("runner");

        assert_eq!(status, BuildStatus::Passed);
        assert!(started_at.elapsed() < Duration::from_millis(360));
        assert_eq!(
            fs::read_to_string(dir.path().join("left.done")).expect("left"),
            "left"
        );
        assert_eq!(
            fs::read_to_string(dir.path().join("right.done")).expect("right"),
            "right"
        );
        while rx.recv().await.is_some() {}
    }

    #[tokio::test]
    async fn skips_conditioned_stages_and_dependents_without_running_steps() {
        let dir = tempdir().expect("tempdir");
        let pipeline = Pipeline::from_toml_str(
            r#"
version = 1
name = "conditional-run"
[[parameters]]
name = "DEPLOY"
default = "false"
[[stages]]
name = "Deploy"
[stages.condition]
parameter = "DEPLOY"
equals = "true"
[[stages.steps]]
name = "must-not-run"
program = "sh"
args = ["-c", "touch should-not-exist"]
[[stages]]
name = "Publish"
depends_on = ["Deploy"]
[[stages.steps]]
name = "also-must-not-run"
program = "sh"
args = ["-c", "touch should-not-exist-either"]
"#,
        )
        .expect("conditional pipeline");
        let plan =
            ExecutionPlan::from_pipeline(&pipeline, uuid::Uuid::new_v4(), uuid::Uuid::new_v4());
        let (tx, mut rx) = mpsc::channel(64);
        let status = execute_pipeline_with_parameters(
            &plan,
            &pipeline,
            dir.path(),
            &BTreeMap::from([(String::from("DEPLOY"), String::from("false"))]),
            CancellationToken::new(),
            tx,
        )
        .await
        .expect("runner");

        assert_eq!(status, BuildStatus::Passed);
        let mut events = Vec::new();
        while let Some(event) = rx.recv().await {
            events.push(event);
        }
        assert_eq!(
            events
                .iter()
                .filter_map(|event| match event {
                    BuildEvent::StageFinished {
                        stage_name, status, ..
                    } => {
                        Some((stage_name.as_str(), status.clone()))
                    }
                    _ => None,
                })
                .collect::<Vec<_>>(),
            vec![
                ("Deploy", StageStatus::Skipped),
                ("Publish", StageStatus::Skipped),
            ]
        );
        assert!(
            !events
                .iter()
                .any(|event| matches!(event, BuildEvent::StepStarted { .. }))
        );
        assert!(!dir.path().join("should-not-exist").exists());
        assert!(!dir.path().join("should-not-exist-either").exists());
    }

    #[tokio::test]
    async fn exposes_resolved_build_parameters_to_direct_processes() {
        let dir = tempdir().expect("tempdir");
        let pipeline = Pipeline::from_toml_str(
            r#"
version = 1
name = "parameters"

[environment]
BUILD_CHANNEL = "project"

[[parameters]]
name = "TARGET"
default = "debug"

[[stages]]
name = "Test"
[[stages.steps]]
name = "parameter-check"
program = "sh"
args = ["-c", "test \"$TARGET\" = release && test \"$BUILD_CHANNEL\" = step && test \"$CI\" = true"]
env = { BUILD_CHANNEL = "step" }
"#,
        )
        .expect("pipeline");
        let plan =
            ExecutionPlan::from_pipeline(&pipeline, uuid::Uuid::new_v4(), uuid::Uuid::new_v4());
        let (tx, mut rx) = mpsc::channel(64);
        let status = execute_pipeline_with_parameters(
            &plan,
            &pipeline,
            dir.path(),
            &BTreeMap::from([(String::from("TARGET"), String::from("release"))]),
            CancellationToken::new(),
            tx,
        )
        .await
        .expect("runner");

        assert_eq!(status, BuildStatus::Passed);
        while rx.recv().await.is_some() {}
    }

    #[tokio::test]
    async fn retries_a_failed_step_and_keeps_one_step_identity() {
        let dir = tempdir().expect("tempdir");
        let pipeline = Pipeline::from_toml_str(
            r#"
version = 1
name = "retry-step"
[[stages]]
name = "Test"
[[stages.steps]]
name = "eventual-pass"
program = "sh"
args = ["-c", "if test -f .rivet-first-attempt; then exit 0; else touch .rivet-first-attempt; exit 17; fi"]
retries = 1
retry_delay_seconds = 0
"#,
        )
        .expect("pipeline");
        let plan =
            ExecutionPlan::from_pipeline(&pipeline, uuid::Uuid::new_v4(), uuid::Uuid::new_v4());
        let (tx, mut rx) = mpsc::channel(64);
        let status = execute_pipeline(&plan, &pipeline, dir.path(), CancellationToken::new(), tx)
            .await
            .expect("runner");

        assert_eq!(status, BuildStatus::Passed);
        let mut events = Vec::new();
        while let Some(event) = rx.recv().await {
            events.push(event);
        }
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event, BuildEvent::StepStarted { .. }))
                .count(),
            2
        );
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event, BuildEvent::StepFinished { .. }))
                .count(),
            2
        );
        assert!(events.iter().any(|event| matches!(
            event,
            BuildEvent::StepOutput { stream: rivet_core::LogStream::System, line, .. }
                if line.contains("retrying step")
        )));
    }

    #[tokio::test]
    async fn cancellation_interrupts_a_retry_delay() {
        let dir = tempdir().expect("tempdir");
        let pipeline = Pipeline::from_toml_str(
            r#"
version = 1
name = "cancel-retry"
[[stages]]
name = "Test"
[[stages.steps]]
name = "always-fails"
program = "false"
retries = 5
retry_delay_seconds = 300
"#,
        )
        .expect("pipeline");
        let plan =
            ExecutionPlan::from_pipeline(&pipeline, uuid::Uuid::new_v4(), uuid::Uuid::new_v4());
        let cancellation = CancellationToken::new();
        let (tx, mut rx) = mpsc::channel(64);
        let execution = execute_pipeline(&plan, &pipeline, dir.path(), cancellation.clone(), tx);
        tokio::pin!(execution);
        let result = tokio::select! {
            result = &mut execution => result,
            _ = tokio::time::sleep(Duration::from_millis(20)) => {
                cancellation.cancel();
                execution.await
            }
        };
        assert_eq!(result.expect("runner result"), BuildStatus::Cancelled);
        while rx.recv().await.is_some() {}
    }

    #[tokio::test]
    async fn masks_secret_parameter_values_before_emitting_output() {
        let dir = tempdir().expect("tempdir");
        let pipeline = Pipeline::from_toml_str(
            r#"
version = 1
name = "masked-output"
[[parameters]]
name = "TOKEN"
secret = true
[[stages]]
name = "Test"
[[stages.steps]]
name = "print-secret"
program = "sh"
args = ["-c", "printf 'token=%s\\n' \"$TOKEN\""]
"#,
        )
        .expect("pipeline");
        let plan =
            ExecutionPlan::from_pipeline(&pipeline, uuid::Uuid::new_v4(), uuid::Uuid::new_v4());
        let (tx, mut rx) = mpsc::channel(64);
        let status = execute_pipeline_with_parameters(
            &plan,
            &pipeline,
            dir.path(),
            &BTreeMap::from([(String::from("TOKEN"), String::from("runtime-secret"))]),
            CancellationToken::new(),
            tx,
        )
        .await
        .expect("runner");
        assert_eq!(status, BuildStatus::Passed);
        let mut output = Vec::new();
        while let Some(event) = rx.recv().await {
            if let BuildEvent::StepOutput { line, .. } = event {
                output.push(line);
            }
        }
        assert!(output.iter().any(|line| line == "token=***"));
        assert!(output.iter().all(|line| !line.contains("runtime-secret")));
    }

    #[tokio::test]
    async fn restores_cache_before_execution_and_saves_after_success() {
        let directory = tempdir().expect("tempdir");
        let cache_root = directory.path().join("cache");
        let pipeline = Pipeline::from_toml_str(
            r#"
version = 1
name = "cached-build"
[[caches]]
name = "workspace-cache"
key = "deps-v1"
paths = ["cache.txt"]
[[stages]]
name = "Test"
[[stages.steps]]
name = "cache-check"
program = "sh"
args = ["-c", "if test -f cache.txt; then printf hit > result.txt; else printf miss > result.txt; fi; printf cached > cache.txt"]
"#,
        )
        .expect("pipeline");
        let project_id = uuid::Uuid::new_v4();
        let first_plan = ExecutionPlan::from_pipeline(&pipeline, uuid::Uuid::new_v4(), project_id);
        let (first_tx, mut first_rx) = mpsc::channel(64);
        assert_eq!(
            execute_pipeline_with_parameters_and_cache(
                &first_plan,
                &pipeline,
                directory.path(),
                &BTreeMap::new(),
                CancellationToken::new(),
                first_tx,
                Some(&cache_root),
            )
            .await
            .expect("first build"),
            BuildStatus::Passed
        );
        while first_rx.recv().await.is_some() {}
        fs::remove_file(directory.path().join("cache.txt")).expect("remove cache file");
        fs::remove_file(directory.path().join("result.txt")).expect("remove result");

        let second_plan = ExecutionPlan::from_pipeline(&pipeline, uuid::Uuid::new_v4(), project_id);
        let (second_tx, mut second_rx) = mpsc::channel(64);
        assert_eq!(
            execute_pipeline_with_parameters_and_cache(
                &second_plan,
                &pipeline,
                directory.path(),
                &BTreeMap::new(),
                CancellationToken::new(),
                second_tx,
                Some(&cache_root),
            )
            .await
            .expect("second build"),
            BuildStatus::Passed
        );
        while second_rx.recv().await.is_some() {}
        assert_eq!(
            fs::read_to_string(directory.path().join("result.txt")).expect("result"),
            "hit"
        );
    }

    #[test]
    fn builds_container_commands_with_a_bounded_workspace_mount() {
        let pipeline = Pipeline::from_toml_str(
            r#"
version = 1
name = "container-command"
[[stages]]
name = "Test"
[[stages.steps]]
name = "unit"
program = "cargo"
args = ["test", "--workspace"]
env = { TARGET = "release" }
working_dir = "subdir"
[stages.steps.container]
image = "rust:1.85"
pull = "always"
network = "ci-net"
[[stages.steps.container.volumes]]
source = "cache"
target = "/rivet/workspace/cache"
read_only = true
"#,
        )
        .expect("container pipeline");
        let plan =
            ExecutionPlan::from_pipeline(&pipeline, uuid::Uuid::new_v4(), uuid::Uuid::new_v4());
        let workspace = PathBuf::from("/tmp/rivet-workspace");
        let working_dir = workspace.join("subdir");
        let env = BTreeMap::from([
            (String::from("TARGET"), String::from("release")),
            (String::from("CI"), String::from("true")),
        ]);
        let spec = build_process_spec(&plan.stages[0].steps[0], &workspace, working_dir, env);

        assert_eq!(spec.program, "docker");
        assert_eq!(
            spec.args[0..14],
            [
                "run",
                "--rm",
                "--init",
                "--sig-proxy=true",
                "--pull",
                "always",
                "--network",
                "ci-net",
                "--workdir",
                "/rivet/workspace/subdir",
                "--volume",
                "/tmp/rivet-workspace:/rivet/workspace:rw",
                "--volume",
                "/tmp/rivet-workspace/cache:/rivet/workspace/cache:ro",
            ]
        );
        assert!(spec.args.windows(2).any(|args| args == ["--env", "CI"]));
        assert!(spec.args.windows(2).any(|args| args == ["--env", "TARGET"]));
        assert_eq!(
            spec.args[spec.args.len() - 4..],
            ["rust:1.85", "cargo", "test", "--workspace"]
        );
        assert_eq!(spec.env["TARGET"], "release");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn executes_container_command_through_a_runtime_shim_without_shell_interpolation() {
        use std::os::unix::fs::PermissionsExt;

        let directory = tempdir().expect("workspace");
        fs::create_dir(directory.path().join("cache")).expect("cache directory");
        let fake_runtime = directory.path().join("fake-runtime");
        fs::create_dir(&fake_runtime).expect("runtime directory");
        let docker = fake_runtime.join("docker");
        fs::write(
            &docker,
            r#"#!/bin/sh
set -eu
workspace=
while [ "$#" -gt 0 ]; do
  case "$1" in
    run) shift ;;
    --volume)
      [ -n "$workspace" ] || workspace=${2%%:/rivet/workspace:rw}
      shift 2
      ;;
    --env|--network|--pull|--workdir) shift 2 ;;
    --rm|--init|--sig-proxy=true) shift ;;
    *)
      image=$1
      shift
      break
      ;;
  esac
done
test -n "$workspace"
cd "$workspace"
exec "$@"
"#,
        )
        .expect("write runtime shim");
        let mut permissions = fs::metadata(&docker)
            .expect("runtime metadata")
            .permissions();
        permissions.set_mode(0o700);
        fs::set_permissions(&docker, permissions).expect("make runtime executable");

        let pipeline = Pipeline::from_toml_str(
            r#"
version = 1
name = "container-runtime"
[[stages]]
name = "Test"
[[stages.steps]]
name = "unit"
program = "sh"
args = ["-c", "printf 'container-ok\\n'"]
[stages.steps.container]
image = "fixture/runtime:1"
pull = "never"
network = "none"
"#,
        )
        .expect("container pipeline");
        let plan =
            ExecutionPlan::from_pipeline(&pipeline, uuid::Uuid::new_v4(), uuid::Uuid::new_v4());
        let mut spec = build_process_spec(
            &plan.stages[0].steps[0],
            directory.path(),
            directory.path().to_owned(),
            BTreeMap::from([(String::from("CI"), String::from("true"))]),
        );
        let original_path = std::env::var_os("PATH").expect("PATH");
        let mut runtime_path = fake_runtime.into_os_string();
        runtime_path.push(":");
        runtime_path.push(original_path);
        spec.env
            .insert("PATH".into(), runtime_path.to_string_lossy().into_owned());
        let (tx, mut rx) = mpsc::channel(64);
        let result = run_process(spec, CancellationToken::new(), tx)
            .await
            .expect("container runtime shim");
        let mut lines = Vec::new();
        while let Some(line) = rx.recv().await {
            lines.push(line.line);
        }
        assert_eq!(result.outcome, ProcessOutcome::Passed);
        assert!(lines.iter().any(|line| line == "container-ok"));
    }

    #[tokio::test]
    async fn refuses_remote_agent_steps_in_the_local_runner() {
        let directory = tempdir().expect("tempdir");
        let pipeline = Pipeline::from_toml_str(
            r#"
version = 1
name = "remote-only"
[[stages]]
name = "Test"
[[stages.steps]]
name = "linux-build"
program = "true"
[stages.steps.agent]
os = "linux"
labels = ["build"]
"#,
        )
        .expect("pipeline");
        let plan =
            ExecutionPlan::from_pipeline(&pipeline, uuid::Uuid::new_v4(), uuid::Uuid::new_v4());
        let (tx, mut rx) = mpsc::channel(64);
        let error = execute_pipeline(
            &plan,
            &pipeline,
            directory.path(),
            CancellationToken::new(),
            tx,
        )
        .await
        .expect_err("local runner must not ignore remote requirements");
        assert!(matches!(
            error,
            RunnerError::RemoteAgentRequired { ref step } if step == "linux-build"
        ));
        let events: Vec<_> = tokio::time::timeout(Duration::from_secs(1), async move {
            let mut events = Vec::new();
            while let Some(event) = rx.recv().await {
                events.push(event);
            }
            events
        })
        .await
        .expect("event stream")
        .into_iter()
        .collect();
        assert!(events.iter().any(|event| matches!(
            event,
            BuildEvent::BuildFinished {
                status: BuildStatus::Failed,
                ..
            }
        )));
    }
}
