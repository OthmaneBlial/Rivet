use crate::{ProcessOutcome, ProcessSpec, run_process};
use chrono::Utc;
use rivet_core::{
    BuildEvent, BuildStatus, ExecutionPlan, ExecutionStage, ExecutionStep, Pipeline, StageStatus,
    StepStatus,
};
use std::collections::BTreeMap;
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
}

/// Execute all stages in a validated plan in declaration order.
///
/// The stage/step loop is intentionally sequential in this first vertical
/// slice. The execution plan already has stable graph identities, so parallel
/// branches can be added later without changing persistence or event identity.
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
    pipeline.validate()?;
    let parameters = pipeline.resolve_parameters(parameters)?;
    let secret_values = pipeline.secret_values(&parameters);
    let workspace = pipeline.resolve_workspace(repository_root)?;
    if cancellation.is_cancelled() {
        return finish_cancelled(plan, &events).await;
    }
    send(
        &events,
        BuildEvent::BuildStarted {
            build_id: plan.build_id,
            timestamp: Utc::now(),
        },
    )
    .await?;

    for stage in &plan.stages {
        if cancellation.is_cancelled() {
            return finish_cancelled(plan, &events).await;
        }
        send(
            &events,
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
                send_stage_finished(plan, stage, StageStatus::Cancelled, &events).await?;
                return finish_cancelled(plan, &events).await;
            }
            let result = execute_step(
                plan,
                stage,
                step,
                &workspace,
                &parameters,
                &secret_values,
                &cancellation,
                &events,
            )
            .await;
            match result {
                Ok(StepStatus::Passed) => {}
                Ok(StepStatus::Cancelled) => {
                    send_stage_finished(plan, stage, StageStatus::Cancelled, &events).await?;
                    return finish_cancelled(plan, &events).await;
                }
                Ok(StepStatus::Failed)
                | Ok(StepStatus::Skipped)
                | Ok(StepStatus::Pending)
                | Ok(StepStatus::Running) => {
                    send_stage_finished(plan, stage, StageStatus::Failed, &events).await?;
                    return finish_build(plan, BuildStatus::Failed, &events).await;
                }
                Err(error) => {
                    send_stage_finished(plan, stage, StageStatus::Failed, &events).await?;
                    let _ = finish_build(plan, BuildStatus::Failed, &events).await;
                    return Err(error);
                }
            }
        }
        send_stage_finished(plan, stage, StageStatus::Passed, &events).await?;
    }

    finish_build(plan, BuildStatus::Passed, &events).await
}

async fn execute_step(
    plan: &ExecutionPlan,
    stage: &ExecutionStage,
    step: &ExecutionStep,
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

    let mut env = parameters.clone();
    env.extend(step.definition.env.clone());
    env.insert("CI".into(), "true".into());
    env.insert("RIVET_BUILD_ID".into(), plan.build_id.to_string());
    env.insert("RIVET_PROJECT_ID".into(), plan.project_id.to_string());
    let spec = ProcessSpec {
        program: step.definition.program.clone(),
        args: step.definition.args.clone(),
        env,
        working_dir,
        timeout: step.definition.timeout_seconds.map(Duration::from_secs),
    };

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
    async fn exposes_resolved_build_parameters_to_direct_processes() {
        let dir = tempdir().expect("tempdir");
        let pipeline = Pipeline::from_toml_str(
            r#"
version = 1
name = "parameters"

[[parameters]]
name = "TARGET"
default = "debug"

[[stages]]
name = "Test"
[[stages.steps]]
name = "parameter-check"
program = "sh"
args = ["-c", "test \"$TARGET\" = release && test \"$CI\" = true"]
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
}
