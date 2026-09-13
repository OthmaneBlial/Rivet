use chrono::{DateTime, Utc};
use rivet_core::LogStream;
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::process::Stdio;
use std::time::{Duration, Instant};
use thiserror::Error;
use tokio::io::{AsyncBufReadExt, AsyncRead, BufReader};
use tokio::process::{Child, Command};
use tokio::sync::mpsc;
use tokio::time::{sleep, timeout};
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone)]
pub struct ProcessSpec {
    pub program: String,
    pub args: Vec<String>,
    pub env: BTreeMap<String, String>,
    pub working_dir: PathBuf,
    pub timeout: Option<Duration>,
}

#[derive(Debug, Clone)]
pub struct LogLine {
    pub stream: LogStream,
    pub line: String,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcessOutcome {
    Passed,
    Failed,
    Cancelled,
    TimedOut,
}

#[derive(Debug, Clone, Copy)]
pub struct ProcessResult {
    pub outcome: ProcessOutcome,
    pub exit_code: Option<i32>,
    pub duration: Duration,
}

#[derive(Debug, Error)]
pub enum ProcessError {
    #[error("could not start {program:?}: {source}")]
    Spawn {
        program: String,
        source: std::io::Error,
    },
    #[error("failed while reading process output: {0}")]
    Read(#[from] std::io::Error),
    #[error("process output task failed: {0}")]
    Join(#[from] tokio::task::JoinError),
}

/// Execute one process with separate stdout/stderr pipes and cancellation.
///
/// Commands are launched directly with an argument vector. On Unix, the
/// process becomes the leader of its own process group so cancellation can
/// terminate descendants as well as the direct child.
pub async fn run_process(
    spec: ProcessSpec,
    cancellation: CancellationToken,
    output: mpsc::Sender<LogLine>,
) -> Result<ProcessResult, ProcessError> {
    let started = Instant::now();
    if cancellation.is_cancelled() {
        return Ok(ProcessResult {
            outcome: ProcessOutcome::Cancelled,
            exit_code: None,
            duration: started.elapsed(),
        });
    }

    let mut command = Command::new(&spec.program);
    command
        .args(&spec.args)
        .envs(&spec.env)
        .current_dir(&spec.working_dir)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    configure_process_group(&mut command);

    let mut child = command.spawn().map_err(|source| ProcessError::Spawn {
        program: spec.program.clone(),
        source,
    })?;

    let stdout = child.stdout.take();
    let stderr = child.stderr.take();
    let mut readers = Vec::with_capacity(2);
    if let Some(stdout) = stdout {
        readers.push(tokio::spawn(forward_lines(
            stdout,
            LogStream::Stdout,
            output.clone(),
        )));
    }
    if let Some(stderr) = stderr {
        readers.push(tokio::spawn(forward_lines(
            stderr,
            LogStream::Stderr,
            output,
        )));
    }

    let (outcome, exit_code) = wait_for_process(&mut child, &cancellation, spec.timeout).await?;

    for reader in readers {
        reader.await??;
    }

    Ok(ProcessResult {
        outcome,
        exit_code,
        duration: started.elapsed(),
    })
}

async fn forward_lines<R>(
    reader: R,
    stream: LogStream,
    output: mpsc::Sender<LogLine>,
) -> std::io::Result<()>
where
    R: AsyncRead + Unpin,
{
    let mut reader = BufReader::new(reader);
    let mut bytes = Vec::with_capacity(1024);
    loop {
        bytes.clear();
        let count = reader.read_until(b'\n', &mut bytes).await?;
        if count == 0 {
            break;
        }
        while matches!(bytes.last(), Some(b'\n' | b'\r')) {
            bytes.pop();
        }
        let line = String::from_utf8_lossy(&bytes).into_owned();
        if output
            .send(LogLine {
                stream,
                line,
                timestamp: Utc::now(),
            })
            .await
            .is_err()
        {
            break;
        }
    }
    Ok(())
}

async fn wait_for_process(
    child: &mut Child,
    cancellation: &CancellationToken,
    process_timeout: Option<Duration>,
) -> Result<(ProcessOutcome, Option<i32>), ProcessError> {
    let started = Instant::now();
    loop {
        if let Some(status) = child.try_wait()? {
            let exit_code = status.code();
            let outcome = if status.success() {
                ProcessOutcome::Passed
            } else {
                ProcessOutcome::Failed
            };
            return Ok((outcome, exit_code));
        }
        if cancellation.is_cancelled() {
            terminate_child(child).await;
            return Ok((ProcessOutcome::Cancelled, None));
        }

        let interval = Duration::from_millis(20);
        let sleep_for = process_timeout
            .and_then(|limit| limit.checked_sub(started.elapsed()))
            .map_or(interval, |remaining| remaining.min(interval));
        if process_timeout.is_some_and(|limit| started.elapsed() >= limit) {
            terminate_child(child).await;
            return Ok((ProcessOutcome::TimedOut, None));
        }
        tokio::select! {
            _ = cancellation.cancelled() => {
                terminate_child(child).await;
                return Ok((ProcessOutcome::Cancelled, None));
            }
            _ = sleep(sleep_for) => {}
        }
    }
}

async fn terminate_child(child: &mut Child) {
    if let Some(pid) = child.id() {
        graceful_group_termination(pid);
        match timeout(Duration::from_secs(2), child.wait()).await {
            Ok(Ok(_)) => {
                // A group leader can exit while a descendant keeps running.
                // The group remains addressable until its last member exits.
                force_group_termination(pid);
            }
            Ok(Err(_)) | Err(_) => {
                force_group_termination(pid);
                let _ = child.kill().await;
                let _ = child.wait().await;
            }
        }
    } else {
        let _ = child.kill().await;
        let _ = child.wait().await;
    }
}

#[cfg(unix)]
fn configure_process_group(command: &mut Command) {
    command.process_group(0);
}

#[cfg(windows)]
fn configure_process_group(command: &mut Command) {
    use std::os::windows::process::CommandExt;
    const CREATE_NEW_PROCESS_GROUP: u32 = 0x0000_0200;
    command.creation_flags(CREATE_NEW_PROCESS_GROUP);
}

#[cfg(not(any(unix, windows)))]
fn configure_process_group(_command: &mut Command) {}

#[cfg(unix)]
fn graceful_group_termination(pid: u32) {
    // The child is spawned as process-group leader, so a negative PID targets
    // its descendants too. A failed signal is harmless: the forceful path
    // below still reaps the direct child.
    unsafe {
        let _ = libc::kill(-(pid as i32), libc::SIGTERM);
    }
}

#[cfg(windows)]
fn graceful_group_termination(pid: u32) {
    // `taskkill /T` is the Windows equivalent of targeting a process group.
    // The command is intentionally best-effort; the direct child is always
    // awaited and force-killed if it does not exit within the grace period.
    let _ = std::process::Command::new("taskkill")
        .args(["/PID", &pid.to_string(), "/T"])
        .status();
}

#[cfg(not(any(unix, windows)))]
fn graceful_group_termination(_pid: u32) {}

#[cfg(unix)]
fn force_group_termination(pid: u32) {
    unsafe {
        let _ = libc::kill(-(pid as i32), libc::SIGKILL);
    }
}

#[cfg(windows)]
fn force_group_termination(pid: u32) {
    let _ = std::process::Command::new("taskkill")
        .args(["/PID", &pid.to_string(), "/T", "/F"])
        .status();
}

#[cfg(not(any(unix, windows)))]
fn force_group_termination(_pid: u32) {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use std::time::Duration;
    use tokio::time::timeout;

    fn shell(script: &str) -> ProcessSpec {
        ProcessSpec {
            program: "sh".into(),
            args: vec!["-c".into(), script.into()],
            env: BTreeMap::new(),
            working_dir: std::env::current_dir().expect("cwd"),
            timeout: None,
        }
    }

    #[tokio::test]
    async fn streams_stdout_and_stderr_without_merging_them() {
        let (tx, mut rx) = mpsc::channel(16);
        let result = run_process(
            shell("printf 'out\\n'; printf 'err\\n' >&2"),
            CancellationToken::new(),
            tx,
        )
        .await
        .expect("process succeeds");

        assert_eq!(result.outcome, ProcessOutcome::Passed);
        let mut lines = Vec::new();
        while let Some(line) = rx.recv().await {
            lines.push((line.stream, line.line));
        }
        assert!(lines.contains(&(LogStream::Stdout, "out".into())));
        assert!(lines.contains(&(LogStream::Stderr, "err".into())));
    }

    #[tokio::test]
    async fn non_zero_exit_is_a_failed_result() {
        let (tx, _rx) = mpsc::channel(16);
        let result = run_process(shell("exit 7"), CancellationToken::new(), tx)
            .await
            .expect("process was launched");

        assert_eq!(result.outcome, ProcessOutcome::Failed);
        assert_eq!(result.exit_code, Some(7));
    }

    #[tokio::test]
    async fn cancellation_reaps_a_long_running_process() {
        let (tx, _rx) = mpsc::channel(16);
        let cancellation = CancellationToken::new();
        let child_cancellation = cancellation.clone();
        let task =
            tokio::spawn(
                async move { run_process(shell("sleep 30"), child_cancellation, tx).await },
            );

        tokio::time::sleep(Duration::from_millis(100)).await;
        cancellation.cancel();
        let result = timeout(Duration::from_secs(4), task)
            .await
            .expect("cancellation completes promptly")
            .expect("task joins")
            .expect("process was managed");
        assert_eq!(result.outcome, ProcessOutcome::Cancelled);
    }

    #[tokio::test]
    async fn timeout_terminates_the_process_group() {
        let (tx, _rx) = mpsc::channel(16);
        let mut spec = shell("sleep 30");
        spec.timeout = Some(Duration::from_millis(50));
        let result = timeout(
            Duration::from_secs(4),
            run_process(spec, CancellationToken::new(), tx),
        )
        .await
        .expect("timeout completes promptly")
        .expect("process was managed");
        assert_eq!(result.outcome, ProcessOutcome::TimedOut);
    }
}
