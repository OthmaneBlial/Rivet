//! Provider-neutral source control boundaries with a direct Git adapter.
//!
//! The adapter deliberately invokes `git` with explicit argument arrays. It
//! never interpolates a repository path, revision, or remote into a shell
//! command. Fetching, checkout, and cleaning are opt-in operations so a
//! status inspection cannot mutate a developer's working tree by accident.

use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use thiserror::Error;
use tokio::process::Command;

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
    pub clean: bool,
    pub clean_ignored: bool,
}

impl Default for GitPrepareOptions {
    fn default() -> Self {
        Self {
            remote: "origin".to_owned(),
            fetch: false,
            revision: None,
            clean: false,
            clean_ignored: false,
        }
    }
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
            .run(["rev-parse", "--verify", "HEAD"], "inspect revision")
            .await?
            .stdout_trimmed("inspect revision")?;
        let branch = self
            .run(
                ["symbolic-ref", "--quiet", "--short", "HEAD"],
                "inspect branch",
            )
            .await
            .ok()
            .and_then(|output| output.stdout_trimmed("inspect branch").ok())
            .filter(|value| !value.is_empty());
        let remote = self
            .run(["config", "--get", "remote.origin.url"], "inspect remote")
            .await
            .ok()
            .and_then(|output| output.stdout_trimmed("inspect remote").ok())
            .filter(|value| !value.is_empty());
        let status = self
            .run(
                ["status", "--porcelain=v1", "--untracked-files=all"],
                "inspect status",
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
        validate_argument(remote, "remote")?;
        self.run(["fetch", "--prune", remote], "fetch")
            .await
            .map(|_| ())
    }

    pub async fn checkout(&self, revision: &str) -> Result<(), ScmError> {
        validate_argument(revision, "revision")?;
        self.run(["checkout", "--detach", revision], "checkout")
            .await
            .map(|_| ())
    }

    pub async fn clean(&self, include_ignored: bool) -> Result<(), ScmError> {
        let args = if include_ignored {
            ["clean", "-fdx"]
        } else {
            ["clean", "-fd"]
        };
        self.run(args, "clean").await.map(|_| ())
    }

    pub async fn prepare(&self, options: &GitPrepareOptions) -> Result<GitSnapshot, ScmError> {
        if options.fetch {
            self.fetch(&options.remote).await?;
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
    ) -> Result<GitCommandOutput, ScmError> {
        let output = Command::new("git")
            .args(args)
            .current_dir(&self.root)
            .output()
            .await
            .map_err(ScmError::Filesystem)?;
        if !output.status.success() {
            let message = String::from_utf8_lossy(&output.stderr).trim().to_owned();
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

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
                clean: true,
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
    async fn rejects_non_repository_paths() {
        let dir = tempfile::tempdir().expect("tempdir");
        assert!(matches!(
            GitRepository::open(dir.path()).await,
            Err(ScmError::NotGitRepository(_))
        ));
    }
}
