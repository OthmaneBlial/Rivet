//! Provider-neutral source control boundaries with a direct Git adapter.
//!
//! The adapter deliberately invokes `git` with explicit argument arrays. It
//! never interpolates a repository path, revision, or remote into a shell
//! command. Fetching, checkout, and cleaning are opt-in operations so a
//! status inspection cannot mutate a developer's working tree by accident.

use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt;
use std::io::Write;
use std::path::{Path, PathBuf};
use tempfile::NamedTempFile;
use thiserror::Error;
use tokio::process::Command;
use zeroize::Zeroize;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GitSnapshot {
    pub root: PathBuf,
    pub revision: String,
    pub branch: Option<String>,
    pub remote: Option<String>,
    pub dirty: bool,
    pub changed_files: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GitPrepareOptions {
    pub remote: String,
    pub fetch: bool,
    pub revision: Option<String>,
    /// Optional provider refspec used to make a pull-request revision
    /// available before detached checkout.
    pub fetch_ref: Option<String>,
    pub clean: bool,
    pub clean_ignored: bool,
    /// Reference resolved by the hosting layer; the secret never enters this
    /// serializable request object.
    pub credential_id: Option<String>,
    /// Optional operator-provided OpenSSH known-hosts file used for strict SSH
    /// host-key verification. `None` uses OpenSSH's normal system/user files.
    pub known_hosts_file: Option<PathBuf>,
}

impl Default for GitPrepareOptions {
    fn default() -> Self {
        Self {
            remote: "origin".to_owned(),
            fetch: false,
            revision: None,
            fetch_ref: None,
            clean: false,
            clean_ignored: false,
            credential_id: None,
            known_hosts_file: None,
        }
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct GitHttpCredential {
    username: String,
    secret: String,
}

impl GitHttpCredential {
    pub fn new(username: impl Into<String>, secret: impl Into<String>) -> Result<Self, ScmError> {
        let username = username.into();
        let secret = secret.into();
        if username.is_empty() {
            return Err(ScmError::InvalidCredential(
                "username cannot be empty".into(),
            ));
        }
        if secret.is_empty() {
            return Err(ScmError::InvalidCredential("secret cannot be empty".into()));
        }
        Ok(Self { username, secret })
    }

    pub fn username(&self) -> &str {
        &self.username
    }

    pub fn secret(&self) -> &str {
        &self.secret
    }
}

impl fmt::Debug for GitHttpCredential {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("GitHttpCredential")
            .field("username", &self.username)
            .finish_non_exhaustive()
    }
}

impl Drop for GitHttpCredential {
    fn drop(&mut self) {
        self.secret.zeroize();
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct GitSshCredential {
    username: String,
    private_key: String,
}

impl GitSshCredential {
    pub fn new(
        username: impl Into<String>,
        private_key: impl Into<String>,
    ) -> Result<Self, ScmError> {
        let username = username.into();
        let private_key = private_key.into();
        if username.is_empty() {
            return Err(ScmError::InvalidCredential(
                "username cannot be empty".into(),
            ));
        }
        if private_key.is_empty() {
            return Err(ScmError::InvalidCredential(
                "private key cannot be empty".into(),
            ));
        }
        if !private_key.contains("PRIVATE KEY") {
            return Err(ScmError::InvalidCredential(
                "private key does not contain a private-key marker".into(),
            ));
        }
        Ok(Self {
            username,
            private_key,
        })
    }

    pub fn username(&self) -> &str {
        &self.username
    }

    pub fn private_key(&self) -> &str {
        &self.private_key
    }
}

impl fmt::Debug for GitSshCredential {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("GitSshCredential")
            .field("username", &self.username)
            .finish_non_exhaustive()
    }
}

impl Drop for GitSshCredential {
    fn drop(&mut self) {
        self.private_key.zeroize();
    }
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum GitCredential {
    HttpBasic(GitHttpCredential),
    SshKey(GitSshCredential),
}

#[derive(Debug, Error)]
pub enum ScmError {
    #[error("repository path is not a directory: {0}")]
    InvalidRepository(PathBuf),
    #[error("path is not a Git repository: {0}")]
    NotGitRepository(PathBuf),
    #[error("Git {operation} failed with exit code {code:?}: {message}")]
    Command {
        operation: &'static str,
        code: Option<i32>,
        message: String,
    },
    #[error("Git {operation} returned invalid UTF-8 output")]
    InvalidOutput { operation: &'static str },
    #[error("invalid Git credential: {0}")]
    InvalidCredential(String),
    #[error("invalid SSH host-key policy: {0}")]
    InvalidHostKeyPolicy(String),
    #[error("filesystem error: {0}")]
    Filesystem(#[from] std::io::Error),
}

#[derive(Debug, Clone)]
pub struct GitRepository {
    root: PathBuf,
}

impl GitRepository {
    pub async fn open(path: impl AsRef<Path>) -> Result<Self, ScmError> {
        let path = path.as_ref();
        if !path.is_dir() {
            return Err(ScmError::InvalidRepository(path.to_path_buf()));
        }

        let root_output = Command::new("git")
            .args(["rev-parse", "--show-toplevel"])
            .current_dir(path)
            .output()
            .await
            .map_err(ScmError::Filesystem)?;
        if !root_output.status.success() {
            return Err(ScmError::NotGitRepository(path.to_path_buf()));
        }
        let root = parse_stdout("discover repository root", &root_output.stdout)?;
        let root = PathBuf::from(root);
        if !root.is_dir() {
            return Err(ScmError::NotGitRepository(path.to_path_buf()));
        }
        Ok(Self { root })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub async fn inspect(&self) -> Result<GitSnapshot, ScmError> {
        let revision = self
            .run(["rev-parse", "--verify", "HEAD"], "inspect revision", None)
            .await?
            .stdout_trimmed("inspect revision")?;
        let branch = self
            .run(
                ["symbolic-ref", "--quiet", "--short", "HEAD"],
                "inspect branch",
                None,
            )
            .await
            .ok()
            .and_then(|output| output.stdout_trimmed("inspect branch").ok())
            .filter(|value| !value.is_empty());
        let remote = self
            .run(
                ["config", "--get", "remote.origin.url"],
                "inspect remote",
                None,
            )
            .await
            .ok()
            .and_then(|output| output.stdout_trimmed("inspect remote").ok())
            .filter(|value| !value.is_empty());
        let status = self
            .run(
                ["status", "--porcelain=v1", "--untracked-files=all"],
                "inspect status",
                None,
            )
            .await?
            .stdout_text("inspect status")?;
        let changed_files = status
            .lines()
            .filter_map(parse_status_path)
            .map(str::to_owned)
            .collect::<Vec<_>>();

        Ok(GitSnapshot {
            root: self.root.clone(),
            revision,
            branch,
            remote,
            dirty: !changed_files.is_empty(),
            changed_files,
        })
    }

    pub async fn fetch(&self, remote: &str) -> Result<(), ScmError> {
        self.fetch_with_credential(remote, None).await
    }

    pub async fn fetch_with_credential(
        &self,
        remote: &str,
        credential: Option<&GitHttpCredential>,
    ) -> Result<(), ScmError> {
        let credential = credential.map(|value| GitCredential::HttpBasic(value.clone()));
        self.fetch_with_auth(remote, credential.as_ref()).await
    }

    pub async fn fetch_with_auth(
        &self,
        remote: &str,
        credential: Option<&GitCredential>,
    ) -> Result<(), ScmError> {
        self.fetch_with_auth_and_known_hosts(remote, credential, None)
            .await
    }

    pub async fn fetch_with_auth_and_known_hosts(
        &self,
        remote: &str,
        credential: Option<&GitCredential>,
        known_hosts_file: Option<&Path>,
    ) -> Result<(), ScmError> {
        validate_argument(remote, "remote")?;
        self.run_with_known_hosts(
            ["fetch", "--prune", remote],
            "fetch",
            credential,
            known_hosts_file,
        )
        .await
        .map(|_| ())
    }

    pub async fn fetch_ref_with_credential(
        &self,
        remote: &str,
        refspec: &str,
        credential: Option<&GitHttpCredential>,
    ) -> Result<(), ScmError> {
        let credential = credential.map(|value| GitCredential::HttpBasic(value.clone()));
        self.fetch_ref_with_auth(remote, refspec, credential.as_ref())
            .await
    }

    pub async fn fetch_ref_with_auth(
        &self,
        remote: &str,
        refspec: &str,
        credential: Option<&GitCredential>,
    ) -> Result<(), ScmError> {
        self.fetch_ref_with_auth_and_known_hosts(remote, refspec, credential, None)
            .await
    }

    pub async fn fetch_ref_with_auth_and_known_hosts(
        &self,
        remote: &str,
        refspec: &str,
        credential: Option<&GitCredential>,
        known_hosts_file: Option<&Path>,
    ) -> Result<(), ScmError> {
        validate_argument(remote, "remote")?;
        validate_refspec(refspec)?;
        self.run_with_known_hosts(
            ["fetch", "--prune", remote, refspec],
            "fetch refspec",
            credential,
            known_hosts_file,
        )
        .await
        .map(|_| ())
    }

    pub async fn checkout(&self, revision: &str) -> Result<(), ScmError> {
        validate_argument(revision, "revision")?;
        self.run(["checkout", "--detach", revision], "checkout", None)
            .await
            .map(|_| ())
    }

    pub async fn clean(&self, include_ignored: bool) -> Result<(), ScmError> {
        let args = if include_ignored {
            ["clean", "-fdx"]
        } else {
            ["clean", "-fd"]
        };
        self.run(args, "clean", None).await.map(|_| ())
    }

    pub async fn prepare(&self, options: &GitPrepareOptions) -> Result<GitSnapshot, ScmError> {
        self.prepare_with_credential(options, None).await
    }

    pub async fn prepare_with_credential(
        &self,
        options: &GitPrepareOptions,
        credential: Option<&GitHttpCredential>,
    ) -> Result<GitSnapshot, ScmError> {
        let credential = credential.map(|value| GitCredential::HttpBasic(value.clone()));
        self.prepare_with_auth(options, credential.as_ref()).await
    }

    pub async fn prepare_with_auth(
        &self,
        options: &GitPrepareOptions,
        credential: Option<&GitCredential>,
    ) -> Result<GitSnapshot, ScmError> {
        if options.fetch {
            if let Some(refspec) = options.fetch_ref.as_deref() {
                self.fetch_ref_with_auth_and_known_hosts(
                    &options.remote,
                    refspec,
                    credential,
                    options.known_hosts_file.as_deref(),
                )
                .await?;
            } else {
                self.fetch_with_auth_and_known_hosts(
                    &options.remote,
                    credential,
                    options.known_hosts_file.as_deref(),
                )
                .await?;
            }
        }
        if let Some(revision) = options.revision.as_deref() {
            self.checkout(revision).await?;
        }
        if options.clean {
            self.clean(options.clean_ignored).await?;
        }
        self.inspect().await
    }

    async fn run<const N: usize>(
        &self,
        args: [&str; N],
        operation: &'static str,
        credential: Option<&GitCredential>,
    ) -> Result<GitCommandOutput, ScmError> {
        self.run_with_known_hosts(args, operation, credential, None)
            .await
    }

    async fn run_with_known_hosts<const N: usize>(
        &self,
        args: [&str; N],
        operation: &'static str,
        credential: Option<&GitCredential>,
        known_hosts_file: Option<&Path>,
    ) -> Result<GitCommandOutput, ScmError> {
        let authentication = git_auth_environment_with_known_hosts(credential, known_hosts_file)?;
        let output = Command::new("git")
            .args(args)
            .current_dir(&self.root)
            .envs(&authentication.values)
            .output()
            .await
            .map_err(ScmError::Filesystem)?;
        if !output.status.success() {
            let message =
                redact_credential(String::from_utf8_lossy(&output.stderr).trim(), credential);
            return Err(ScmError::Command {
                operation,
                code: output.status.code(),
                message,
            });
        }
        Ok(GitCommandOutput {
            stdout: output.stdout,
        })
    }
}

impl GitSnapshot {
    pub fn source_snapshot(&self) -> rivet_core::SourceSnapshot {
        rivet_core::SourceSnapshot {
            provider: "git".to_owned(),
            revision: self.revision.clone(),
            reference: self.branch.clone(),
            remote: self.remote.clone(),
            dirty: self.dirty,
        }
    }
}

#[derive(Debug)]
struct GitCommandOutput {
    stdout: Vec<u8>,
}

impl GitCommandOutput {
    fn stdout_text(&self, operation: &'static str) -> Result<String, ScmError> {
        String::from_utf8(self.stdout.clone()).map_err(|_| ScmError::InvalidOutput { operation })
    }

    fn stdout_trimmed(&self, operation: &'static str) -> Result<String, ScmError> {
        Ok(self.stdout_text(operation)?.trim().to_owned())
    }
}

struct GitAuthEnvironment {
    values: BTreeMap<String, String>,
    _ssh_key: Option<NamedTempFile>,
}

fn git_auth_environment_with_known_hosts(
    credential: Option<&GitCredential>,
    known_hosts_file: Option<&Path>,
) -> Result<GitAuthEnvironment, ScmError> {
    let mut environment = BTreeMap::new();
    let mut ssh_key = None;
    let Some(credential) = credential else {
        if known_hosts_file.is_some() {
            environment.insert(
                "GIT_SSH_COMMAND".into(),
                format!(
                    "ssh -o BatchMode=yes -o StrictHostKeyChecking=yes{}",
                    ssh_known_hosts_options(known_hosts_file)?
                ),
            );
            environment.insert("GIT_TERMINAL_PROMPT".into(), "0".into());
        }
        return Ok(GitAuthEnvironment {
            values: environment,
            _ssh_key: ssh_key,
        });
    };
    match credential {
        GitCredential::HttpBasic(credential) => {
            let encoded = STANDARD.encode(format!("{}:{}", credential.username, credential.secret));
            environment.insert("GIT_CONFIG_COUNT".into(), "2".into());
            environment.insert("GIT_CONFIG_KEY_0".into(), "http.extraHeader".into());
            environment.insert(
                "GIT_CONFIG_VALUE_0".into(),
                format!("Authorization: Basic {encoded}"),
            );
            environment.insert("GIT_CONFIG_KEY_1".into(), "credential.helper".into());
            environment.insert("GIT_CONFIG_VALUE_1".into(), String::new());
        }
        GitCredential::SshKey(credential) => {
            let mut temporary = tempfile::Builder::new()
                .prefix("rivet-ssh-key-")
                .tempfile()
                .map_err(ScmError::Filesystem)?;
            temporary
                .write_all(credential.private_key.as_bytes())
                .map_err(ScmError::Filesystem)?;
            temporary
                .as_file()
                .sync_all()
                .map_err(ScmError::Filesystem)?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                let mut permissions = temporary
                    .as_file()
                    .metadata()
                    .map_err(ScmError::Filesystem)?
                    .permissions();
                permissions.set_mode(0o600);
                temporary
                    .as_file()
                    .set_permissions(permissions)
                    .map_err(ScmError::Filesystem)?;
            }
            let key_path = shell_quote(&temporary.path().to_string_lossy());
            environment.insert(
                "GIT_SSH_COMMAND".into(),
                format!(
                    "ssh -i {} -o IdentitiesOnly=yes -o BatchMode=yes -o StrictHostKeyChecking=yes{}",
                    key_path,
                    ssh_known_hosts_options(known_hosts_file)?
                ),
            );
            ssh_key = Some(temporary);
        }
    }
    environment.insert("GIT_TERMINAL_PROMPT".into(), "0".into());
    Ok(GitAuthEnvironment {
        values: environment,
        _ssh_key: ssh_key,
    })
}

fn ssh_known_hosts_options(known_hosts_file: Option<&Path>) -> Result<String, ScmError> {
    let Some(path) = known_hosts_file else {
        return Ok(String::new());
    };
    let path = validate_known_hosts_file(path)?;
    Ok(format!(
        " -o UserKnownHostsFile={} -o GlobalKnownHostsFile=/dev/null",
        shell_quote(&path.to_string_lossy())
    ))
}

/// Validate and canonicalize an operator-provided OpenSSH known-hosts file.
///
/// A symlink or world-writable file is rejected so a deployment cannot silently
/// replace the trust root behind the server's back. The file contents remain
/// private to OpenSSH and are never returned by Rivet.
pub fn validate_known_hosts_file(path: &Path) -> Result<PathBuf, ScmError> {
    let metadata = std::fs::symlink_metadata(path)
        .map_err(|error| ScmError::InvalidHostKeyPolicy(format!("{}: {error}", path.display())))?;
    if !metadata.file_type().is_file() {
        return Err(ScmError::InvalidHostKeyPolicy(format!(
            "{} is not a regular file",
            path.display()
        )));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if metadata.permissions().mode() & 0o002 != 0 {
            return Err(ScmError::InvalidHostKeyPolicy(format!(
                "{} must not be world-writable",
                path.display()
            )));
        }
    }
    std::fs::canonicalize(path)
        .map_err(|error| ScmError::InvalidHostKeyPolicy(format!("{}: {error}", path.display())))
}

fn shell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\\"'\\\"'"))
}

fn redact_credential(message: &str, credential: Option<&GitCredential>) -> String {
    let Some(credential) = credential else {
        return message.to_owned();
    };
    match credential {
        GitCredential::HttpBasic(credential) => {
            let encoded = STANDARD.encode(format!("{}:{}", credential.username, credential.secret));
            message
                .replace(&credential.secret, "[redacted]")
                .replace(&encoded, "[redacted]")
        }
        GitCredential::SshKey(credential) => message.replace(&credential.private_key, "[redacted]"),
    }
}

fn parse_stdout(operation: &'static str, stdout: &[u8]) -> Result<String, ScmError> {
    String::from_utf8(stdout.to_vec())
        .map(|value| value.trim().to_owned())
        .map_err(|_| ScmError::InvalidOutput { operation })
}

fn parse_status_path(line: &str) -> Option<&str> {
    if line.len() < 4 {
        return None;
    }
    let path = line[3..].trim();
    if path.is_empty() {
        None
    } else if let Some((_, renamed)) = path.rsplit_once(" -> ") {
        Some(renamed)
    } else {
        Some(path)
    }
}

fn validate_argument(value: &str, label: &'static str) -> Result<(), ScmError> {
    if value.trim().is_empty() || value.contains('\0') {
        return Err(ScmError::Command {
            operation: label,
            code: None,
            message: "argument must be non-empty and NUL-free".to_owned(),
        });
    }
    Ok(())
}

fn validate_refspec(refspec: &str) -> Result<(), ScmError> {
    if refspec.is_empty()
        || refspec.len() > 512
        || refspec.starts_with('-')
        || refspec.bytes().any(|byte| {
            !(byte.is_ascii_alphanumeric()
                || matches!(byte, b'+' | b':' | b'/' | b'.' | b'_' | b'-' | b'*'))
        })
    {
        return Err(ScmError::Command {
            operation: "fetch refspec",
            code: None,
            message: "refspec contains unsupported characters or is too long".to_owned(),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::process::Stdio;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tempfile::TempDir;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};
    use tokio::sync::oneshot;

    const HTTP_USERNAME: &str = "rivet-fixture-user";
    const HTTP_SECRET: &str = "rivet-fixture-secret-not-persisted";

    async fn git(dir: &Path, args: &[&str]) -> String {
        let output = Command::new("git")
            .args(args)
            .current_dir(dir)
            .output()
            .await
            .expect("git available");
        assert!(
            output.status.success(),
            "git {:?}: {}",
            args,
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8_lossy(&output.stdout).trim().to_owned()
    }

    #[derive(Clone, Default)]
    struct GitHttpStats {
        authorized_requests: Arc<AtomicUsize>,
        unauthorized_requests: Arc<AtomicUsize>,
    }

    struct GitHttpServer {
        address: std::net::SocketAddr,
        shutdown: Option<oneshot::Sender<()>>,
        task: tokio::task::JoinHandle<()>,
        stats: GitHttpStats,
    }

    impl GitHttpServer {
        async fn start(project_root: PathBuf) -> Self {
            let listener = TcpListener::bind(("127.0.0.1", 0))
                .await
                .expect("HTTP listener");
            let address = listener.local_addr().expect("HTTP address");
            let exec_path = std::process::Command::new("git")
                .arg("--exec-path")
                .output()
                .expect("git exec path")
                .stdout;
            let backend = PathBuf::from(
                String::from_utf8(exec_path)
                    .expect("git exec path UTF-8")
                    .trim(),
            )
            .join("git-http-backend");
            let stats = GitHttpStats::default();
            let expected_authorization = format!(
                "Basic {}",
                STANDARD.encode(format!("{HTTP_USERNAME}:{HTTP_SECRET}"))
            );
            let (shutdown_sender, mut shutdown_receiver) = oneshot::channel();
            let task_stats = stats.clone();
            let task = tokio::spawn(async move {
                loop {
                    tokio::select! {
                        _ = &mut shutdown_receiver => break,
                        result = listener.accept() => {
                            let Ok((stream, _)) = result else { break };
                            let project_root = project_root.clone();
                            let backend = backend.clone();
                            let expected_authorization = expected_authorization.clone();
                            let task_stats = task_stats.clone();
                            tokio::spawn(async move {
                                if let Err(error) = serve_git_http(
                                    stream,
                                    &project_root,
                                    &backend,
                                    &expected_authorization,
                                    &task_stats,
                                ).await {
                                    let _ = error;
                                }
                            });
                        }
                    }
                }
            });
            Self {
                address,
                shutdown: Some(shutdown_sender),
                task,
                stats,
            }
        }

        fn url(&self, repository_name: &str) -> String {
            format!("http://{}/{repository_name}", self.address)
        }

        async fn stop(mut self) {
            let _ = self.shutdown.take().expect("shutdown sender").send(());
            self.task.await.expect("HTTP server task");
        }
    }

    async fn serve_git_http(
        mut stream: TcpStream,
        project_root: &Path,
        backend: &Path,
        expected_authorization: &str,
        stats: &GitHttpStats,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let (method, target, headers, body) = read_http_request(&mut stream).await?;
        let authorization = headers
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case("authorization"))
            .map(|(_, value)| value.as_str());
        if authorization != Some(expected_authorization) {
            stats.unauthorized_requests.fetch_add(1, Ordering::SeqCst);
            stream
                .write_all(
                    b"HTTP/1.1 401 Unauthorized\r\nWWW-Authenticate: Basic realm=\"rivet-test\"\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                )
                .await?;
            return Ok(());
        }
        stats.authorized_requests.fetch_add(1, Ordering::SeqCst);

        let (path, query) = target.split_once('?').unwrap_or((&target, ""));
        let mut command = tokio::process::Command::new(backend);
        command
            .env("GIT_PROJECT_ROOT", project_root)
            .env("GIT_HTTP_EXPORT_ALL", "1")
            .env("PATH_INFO", path)
            .env("REQUEST_METHOD", method)
            .env("QUERY_STRING", query)
            .env("SERVER_PROTOCOL", "HTTP/1.1")
            .env("SERVER_NAME", "127.0.0.1")
            .env("SERVER_PORT", "0")
            .env("REMOTE_ADDR", "127.0.0.1")
            .env("REMOTE_USER", HTTP_USERNAME)
            .env("CONTENT_LENGTH", body.len().to_string())
            .envs(headers.iter().filter_map(|(name, value)| {
                let key = name.to_ascii_uppercase().replace('-', "_");
                if key == "CONTENT_TYPE" || key == "CONTENT_LENGTH" {
                    Some((key, value.clone()))
                } else {
                    None
                }
            }))
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let mut child = command.spawn()?;
        if let Some(mut stdin) = child.stdin.take() {
            stdin.write_all(&body).await?;
        }
        let output = child.wait_with_output().await?;
        if !output.status.success() {
            return Err(format!(
                "git-http-backend failed: {}",
                String::from_utf8_lossy(&output.stderr)
            )
            .into());
        }
        let (cgi_headers, response_body) = output
            .stdout
            .split_once_bytes(b"\r\n\r\n")
            .ok_or("git-http-backend returned malformed CGI output")?;
        let mut cgi_response_headers = Vec::new();
        let mut status = "200 OK";
        for line in cgi_headers.split(|byte| *byte == b'\n') {
            let line = line.strip_suffix(b"\r").unwrap_or(line);
            if line.starts_with(b"Status:") {
                status = std::str::from_utf8(&line[7..])?.trim();
            } else if !line.is_empty() {
                cgi_response_headers.extend_from_slice(line);
                cgi_response_headers.extend_from_slice(b"\r\n");
            }
        }
        let mut wire = format!("HTTP/1.1 {status}\r\n").into_bytes();
        wire.extend_from_slice(&cgi_response_headers);
        wire.extend_from_slice(
            format!(
                "Content-Length: {}\r\nConnection: close\r\n\r\n",
                response_body.len()
            )
            .as_bytes(),
        );
        wire.extend_from_slice(response_body);
        stream.write_all(&wire).await?;
        Ok(())
    }

    async fn read_http_request(
        stream: &mut TcpStream,
    ) -> Result<
        (String, String, Vec<(String, String)>, Vec<u8>),
        Box<dyn std::error::Error + Send + Sync>,
    > {
        let mut bytes = Vec::new();
        let header_end = loop {
            let mut chunk = [0_u8; 4096];
            let count = stream.read(&mut chunk).await?;
            if count == 0 {
                return Err("client closed before HTTP headers".into());
            }
            bytes.extend_from_slice(&chunk[..count]);
            if bytes.len() > 256 * 1024 {
                return Err("HTTP headers too large".into());
            }
            if let Some(index) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
                break index + 4;
            }
        };
        let header_text = std::str::from_utf8(&bytes[..header_end])?;
        let mut lines = header_text.split("\r\n");
        let request_line = lines.next().ok_or("missing HTTP request line")?;
        let mut request_parts = request_line.split_whitespace();
        let method = request_parts
            .next()
            .ok_or("missing HTTP method")?
            .to_owned();
        let target = request_parts
            .next()
            .ok_or("missing HTTP target")?
            .to_owned();
        let headers = lines
            .filter(|line| !line.is_empty())
            .filter_map(|line| line.split_once(':'))
            .map(|(name, value)| (name.trim().to_owned(), value.trim().to_owned()))
            .collect::<Vec<_>>();
        let content_length = headers
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
            .map(|(_, value)| value.parse::<usize>())
            .transpose()?
            .unwrap_or(0);
        let mut body = bytes[header_end..].to_vec();
        while body.len() < content_length {
            let mut chunk = vec![0_u8; content_length - body.len()];
            let count = stream.read(&mut chunk).await?;
            if count == 0 {
                return Err("client closed before HTTP body".into());
            }
            body.extend_from_slice(&chunk[..count]);
        }
        body.truncate(content_length);
        Ok((method, target, headers, body))
    }

    trait SplitOnceBytes {
        fn split_once_bytes(&self, delimiter: &[u8]) -> Option<(&[u8], &[u8])>;
    }

    impl SplitOnceBytes for [u8] {
        fn split_once_bytes(&self, delimiter: &[u8]) -> Option<(&[u8], &[u8])> {
            self.windows(delimiter.len())
                .position(|window| window == delimiter)
                .map(|index| (&self[..index], &self[index + delimiter.len()..]))
        }
    }

    async fn repository() -> TempDir {
        let dir = tempfile::tempdir().expect("tempdir");
        git(dir.path(), &["init", "-q"]).await;
        git(dir.path(), &["config", "user.email", "rivet@example.test"]).await;
        git(dir.path(), &["config", "user.name", "Rivet Tests"]).await;
        fs::write(dir.path().join("README.md"), "first\n").expect("write");
        git(dir.path(), &["add", "README.md"]).await;
        git(dir.path(), &["commit", "-qm", "first"]).await;
        dir
    }

    #[tokio::test]
    async fn inspects_clean_repository_and_revision() {
        let dir = repository().await;
        let repository = GitRepository::open(dir.path()).await.expect("open");
        let snapshot = repository.inspect().await.expect("inspect");
        assert_eq!(
            snapshot.root,
            fs::canonicalize(dir.path()).expect("canonical")
        );
        assert_eq!(snapshot.revision.len(), 40);
        assert!(snapshot.branch.is_some());
        assert!(!snapshot.dirty);
        assert!(snapshot.changed_files.is_empty());
    }

    #[tokio::test]
    async fn reports_untracked_and_modified_files_without_mutating_them() {
        let dir = repository().await;
        fs::write(dir.path().join("README.md"), "changed\n").expect("modify");
        fs::write(dir.path().join("notes.txt"), "untracked\n").expect("create");
        let repository = GitRepository::open(dir.path()).await.expect("open");
        let snapshot = repository.inspect().await.expect("inspect");
        assert!(snapshot.dirty);
        assert_eq!(snapshot.changed_files, ["README.md", "notes.txt"]);
        assert_eq!(
            fs::read_to_string(dir.path().join("README.md")).unwrap(),
            "changed\n"
        );
    }

    #[tokio::test]
    async fn prepare_can_checkout_a_revision_and_clean_untracked_files() {
        let dir = repository().await;
        fs::write(dir.path().join("second.txt"), "second\n").expect("write");
        git(dir.path(), &["add", "second.txt"]).await;
        git(dir.path(), &["commit", "-qm", "second"]).await;
        let first_revision = git(dir.path(), &["rev-parse", "HEAD~1"]).await;
        fs::write(dir.path().join("throwaway.txt"), "remove\n").expect("write");
        let repository = GitRepository::open(dir.path()).await.expect("open");
        let snapshot = repository
            .prepare(&GitPrepareOptions {
                revision: Some(first_revision.clone()),
                fetch_ref: None,
                clean: true,
                credential_id: None,
                ..GitPrepareOptions::default()
            })
            .await
            .expect("prepare");
        assert_eq!(snapshot.revision, first_revision);
        assert_eq!(snapshot.branch, None);
        assert!(!dir.path().join("throwaway.txt").exists());
        assert!(!snapshot.dirty);
    }

    #[tokio::test]
    async fn prepare_fetches_a_bounded_refspec_before_checkout() {
        let repository_dir = repository().await;
        let bare_dir = tempfile::tempdir().expect("bare repository");
        git(bare_dir.path(), &["init", "--bare", "-q"]).await;
        let remote = bare_dir.path().to_str().expect("remote path");
        git(repository_dir.path(), &["remote", "add", "origin", remote]).await;
        git(
            repository_dir.path(),
            &["push", "-q", "origin", "HEAD:refs/heads/main"],
        )
        .await;
        let revision = git(repository_dir.path(), &["rev-parse", "HEAD"]).await;
        let repository = GitRepository::open(repository_dir.path())
            .await
            .expect("open");
        let snapshot = repository
            .prepare(&GitPrepareOptions {
                remote: "origin".into(),
                fetch: true,
                revision: Some(revision.clone()),
                fetch_ref: Some("+refs/heads/main:refs/remotes/origin/pull/42".into()),
                ..GitPrepareOptions::default()
            })
            .await
            .expect("prepare refspec");
        assert_eq!(snapshot.revision, revision);
    }

    #[tokio::test]
    async fn authenticated_fetch_checkout_and_clean_keep_credentials_ephemeral() {
        let source_dir = repository().await;
        let remote_parent = tempfile::tempdir().expect("remote parent");
        let remote_dir = remote_parent.path().join("fixture.git");
        fs::create_dir(&remote_dir).expect("remote directory");
        git(&remote_dir, &["init", "--bare", "-q"]).await;
        let remote = remote_dir.to_str().expect("remote path");
        git(source_dir.path(), &["remote", "add", "origin", remote]).await;
        git(
            source_dir.path(),
            &["push", "-q", "origin", "HEAD:refs/heads/main"],
        )
        .await;
        git(&remote_dir, &["symbolic-ref", "HEAD", "refs/heads/main"]).await;

        let checkout_parent = tempfile::tempdir().expect("checkout parent");
        let checkout_dir = checkout_parent.path().join("checkout");
        let checkout = checkout_dir.to_str().expect("checkout path");
        git(checkout_parent.path(), &["clone", "-q", remote, checkout]).await;

        fs::write(source_dir.path().join("README.md"), "second\n").expect("second commit");
        git(source_dir.path(), &["add", "README.md"]).await;
        git(source_dir.path(), &["commit", "-qm", "second"]).await;
        let revision = git(source_dir.path(), &["rev-parse", "HEAD"]).await;
        git(
            source_dir.path(),
            &["push", "-q", "origin", "HEAD:refs/heads/main"],
        )
        .await;

        let server = GitHttpServer::start(remote_parent.path().to_path_buf()).await;
        let remote_url = server.url("fixture.git");
        git(&checkout_dir, &["remote", "set-url", "origin", &remote_url]).await;
        fs::write(checkout_dir.join("throwaway.txt"), "remove me\n").expect("untracked file");

        let mut unauthenticated = TcpStream::connect(server.address)
            .await
            .expect("unauthenticated connection");
        unauthenticated
            .write_all(
                b"GET /fixture.git/info/refs?service=git-upload-pack HTTP/1.1\r\nHost: fixture\r\nConnection: close\r\n\r\n",
            )
            .await
            .expect("unauthenticated request");
        let mut unauthenticated_response = Vec::new();
        unauthenticated
            .read_to_end(&mut unauthenticated_response)
            .await
            .expect("unauthenticated response");
        assert!(
            String::from_utf8_lossy(&unauthenticated_response)
                .starts_with("HTTP/1.1 401 Unauthorized")
        );

        let repository = GitRepository::open(&checkout_dir)
            .await
            .expect("open checkout");
        let credential = GitHttpCredential::new(HTTP_USERNAME, HTTP_SECRET).expect("credential");
        let auth = GitCredential::HttpBasic(credential.clone());
        let snapshot = repository
            .prepare_with_auth(
                &GitPrepareOptions {
                    fetch: true,
                    revision: Some(revision.clone()),
                    clean: true,
                    ..GitPrepareOptions::default()
                },
                Some(&auth),
            )
            .await
            .expect("authenticated prepare");

        assert_eq!(snapshot.revision, revision);
        assert!(!snapshot.dirty);
        assert_eq!(
            fs::read_to_string(checkout_dir.join("README.md")).unwrap(),
            "second\n"
        );
        assert!(!checkout_dir.join("throwaway.txt").exists());
        assert_eq!(
            git(
                &checkout_dir,
                &["config", "--local", "--get", "remote.origin.url"]
            )
            .await,
            remote_url
        );
        let config = fs::read_to_string(checkout_dir.join(".git/config")).expect("git config");
        assert!(!config.contains(HTTP_SECRET));
        assert!(!config.contains("Authorization"));
        assert!(
            !git(&checkout_dir, &["config", "--local", "--list"])
                .await
                .contains(HTTP_SECRET)
        );
        assert!(!format!("{snapshot:?}").contains(HTTP_SECRET));
        assert_eq!(server.stats.unauthorized_requests.load(Ordering::SeqCst), 1);
        assert!(server.stats.authorized_requests.load(Ordering::SeqCst) >= 1);
        server.stop().await;
    }

    #[tokio::test]
    async fn rejects_non_repository_paths() {
        let dir = tempfile::tempdir().expect("tempdir");
        assert!(matches!(
            GitRepository::open(dir.path()).await,
            Err(ScmError::NotGitRepository(_))
        ));
    }

    #[test]
    fn builds_ephemeral_git_auth_and_redacts_secret_material() {
        let credential =
            GitHttpCredential::new("oauth2", "fixture-token-value").expect("credential");
        let credential = GitCredential::HttpBasic(credential);
        let environment =
            git_auth_environment_with_known_hosts(Some(&credential), None).expect("environment");
        let encoded = STANDARD.encode("oauth2:fixture-token-value");

        assert_eq!(
            environment
                .values
                .get("GIT_CONFIG_COUNT")
                .map(String::as_str),
            Some("2")
        );
        assert_eq!(
            environment
                .values
                .get("GIT_CONFIG_KEY_0")
                .map(String::as_str),
            Some("http.extraHeader")
        );
        assert_eq!(
            environment
                .values
                .get("GIT_CONFIG_KEY_1")
                .map(String::as_str),
            Some("credential.helper")
        );
        assert_eq!(
            environment
                .values
                .get("GIT_CONFIG_VALUE_0")
                .map(String::as_str),
            Some(format!("Authorization: Basic {encoded}").as_str())
        );
        assert_eq!(
            environment
                .values
                .get("GIT_TERMINAL_PROMPT")
                .map(String::as_str),
            Some("0")
        );

        let redacted = redact_credential(
            &format!("server rejected fixture-token-value ({encoded})"),
            Some(&credential),
        );
        assert!(!redacted.contains("fixture-token-value"));
        assert!(!redacted.contains(&encoded));
        assert!(redacted.contains("[redacted]"));
        assert!(!format!("{credential:?}").contains("fixture-token-value"));
    }

    #[test]
    fn writes_ssh_key_to_private_ephemeral_file_and_redacts_it() {
        let private_key =
            "-----BEGIN OPENSSH PRIVATE KEY-----\nfixture-key\n-----END OPENSSH PRIVATE KEY-----";
        let credential =
            GitCredential::SshKey(GitSshCredential::new("git", private_key).expect("credential"));
        let environment =
            git_auth_environment_with_known_hosts(Some(&credential), None).expect("environment");
        let key_path = environment
            ._ssh_key
            .as_ref()
            .expect("temporary key")
            .path()
            .to_path_buf();
        let ssh_command = environment
            .values
            .get("GIT_SSH_COMMAND")
            .expect("ssh command");
        assert!(ssh_command.contains(key_path.to_str().expect("key path")));
        assert!(ssh_command.contains("IdentitiesOnly=yes"));
        assert!(ssh_command.contains("BatchMode=yes"));
        assert!(ssh_command.contains("StrictHostKeyChecking=yes"));
        assert!(!ssh_command.contains("accept-new"));
        assert!(!ssh_command.contains(private_key));
        assert_eq!(
            environment
                .values
                .get("GIT_TERMINAL_PROMPT")
                .map(String::as_str),
            Some("0")
        );
        assert_eq!(
            fs::read_to_string(&key_path).expect("read key"),
            private_key
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                fs::metadata(&key_path)
                    .expect("key metadata")
                    .permissions()
                    .mode()
                    & 0o777,
                0o600
            );
        }

        let redacted =
            redact_credential(&format!("ssh failed with {private_key}"), Some(&credential));
        assert!(!redacted.contains(private_key));
        assert!(redacted.contains("[redacted]"));
        assert!(!format!("{credential:?}").contains(private_key));
        drop(environment);
        assert!(!key_path.exists());
    }

    #[test]
    fn applies_an_explicit_strict_known_hosts_file() {
        let tempdir = tempfile::tempdir().expect("tempdir");
        let known_hosts = tempdir.path().join("known_hosts");
        fs::write(&known_hosts, "github.com ssh-ed25519 AAAAfixture\n").expect("known hosts");
        let credential = GitCredential::SshKey(
            GitSshCredential::new(
                "git",
                "-----BEGIN OPENSSH PRIVATE KEY-----\nfixture\n-----END OPENSSH PRIVATE KEY-----",
            )
            .expect("credential"),
        );

        let environment =
            git_auth_environment_with_known_hosts(Some(&credential), Some(&known_hosts))
                .expect("environment");
        let command = environment
            .values
            .get("GIT_SSH_COMMAND")
            .expect("ssh command");
        let canonical = fs::canonicalize(&known_hosts).expect("canonical known hosts");
        assert!(command.contains("StrictHostKeyChecking=yes"));
        assert!(command.contains("GlobalKnownHostsFile=/dev/null"));
        assert!(command.contains(&format!(
            "UserKnownHostsFile={}",
            shell_quote(&canonical.to_string_lossy())
        )));
    }

    #[test]
    fn applies_known_hosts_policy_when_ssh_uses_the_system_agent() {
        let tempdir = tempfile::tempdir().expect("tempdir");
        let known_hosts = tempdir.path().join("known_hosts");
        fs::write(&known_hosts, "github.com ssh-ed25519 AAAAfixture\n").expect("known hosts");

        let environment =
            git_auth_environment_with_known_hosts(None, Some(&known_hosts)).expect("environment");
        let command = environment
            .values
            .get("GIT_SSH_COMMAND")
            .expect("ssh command");
        assert!(command.contains("BatchMode=yes"));
        assert!(command.contains("StrictHostKeyChecking=yes"));
        assert_eq!(
            environment
                .values
                .get("GIT_TERMINAL_PROMPT")
                .map(String::as_str),
            Some("0")
        );
    }

    #[test]
    fn rejects_missing_directories_symlinks_and_world_writable_known_hosts() {
        let tempdir = tempfile::tempdir().expect("tempdir");
        let missing = tempdir.path().join("missing");
        assert!(matches!(
            validate_known_hosts_file(&missing),
            Err(ScmError::InvalidHostKeyPolicy(_))
        ));
        assert!(matches!(
            validate_known_hosts_file(tempdir.path()),
            Err(ScmError::InvalidHostKeyPolicy(_))
        ));

        let known_hosts = tempdir.path().join("known_hosts");
        fs::write(&known_hosts, "fixture\n").expect("known hosts");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut permissions = fs::metadata(&known_hosts).expect("metadata").permissions();
            permissions.set_mode(0o666);
            fs::set_permissions(&known_hosts, permissions).expect("permissions");
            assert!(matches!(
                validate_known_hosts_file(&known_hosts),
                Err(ScmError::InvalidHostKeyPolicy(_))
            ));
        }

        #[cfg(unix)]
        {
            let target = tempdir.path().join("target");
            fs::write(&target, "fixture\n").expect("target");
            let link = tempdir.path().join("known_hosts.link");
            std::os::unix::fs::symlink(&target, &link).expect("symlink");
            assert!(matches!(
                validate_known_hosts_file(&link),
                Err(ScmError::InvalidHostKeyPolicy(_))
            ));
        }
    }

    #[test]
    fn rejects_unsafe_fetch_refspecs_before_git() {
        assert!(validate_refspec("+refs/pull/42/head:refs/remotes/origin/pull/42").is_ok());
        assert!(validate_refspec("--upload-pack=sh").is_err());
        assert!(validate_refspec("refs/pull/42/head:refs/remotes/origin/pull/42\n").is_err());
    }
}
