use rivet_core::{CacheSpec, ProjectId};
use sha2::{Digest, Sha256};
use std::fs::{self, File, OpenOptions};
use std::io;
use std::path::{Component, Path, PathBuf};
use thiserror::Error;
use uuid::Uuid;

#[derive(Debug, Error)]
pub enum CacheError {
    #[error("cache filesystem error: {0}")]
    Filesystem(#[from] io::Error),
}

/// A project-scoped, filesystem-backed cache store.
///
/// Cache entries are immutable archives addressed by a SHA-256 digest of the
/// project ID and declared key. Writes use a unique temporary file followed by
/// an atomic rename, so an interrupted build cannot publish a partial entry.
pub struct CacheStore {
    root: PathBuf,
}

impl CacheStore {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    /// Restore an exact cache key into the workspace.
    ///
    /// A missing entry is a normal cache miss. Corrupt entries are removed so
    /// a later successful build can replace them rather than repeatedly
    /// failing to restore the same archive.
    pub fn restore(
        &self,
        project_id: ProjectId,
        spec: &CacheSpec,
        workspace: &Path,
    ) -> Result<bool, CacheError> {
        let archive_path = self.archive_path(project_id, spec);
        if !archive_path.is_file() {
            return Ok(false);
        }
        let file = File::open(&archive_path)?;
        let mut archive = tar::Archive::new(file);
        let restore_result = (|| {
            for entry in archive.entries()? {
                let mut entry = entry?;
                let path = entry.path()?.into_owned();
                if path.is_absolute()
                    || path.components().any(|component| {
                        matches!(
                            component,
                            Component::ParentDir | Component::RootDir | Component::Prefix(_)
                        )
                    })
                {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("cache entry escapes workspace: {}", path.display()),
                    ));
                }
                entry.unpack_in(workspace)?;
            }
            Ok(())
        })();
        if let Err(error) = restore_result {
            let _ = fs::remove_file(&archive_path);
            return Err(error.into());
        }
        Ok(true)
    }

    /// Save the selected workspace paths under an exact cache key.
    ///
    /// Missing paths are ignored, matching common CI cache behavior. The
    /// boolean reports whether this call published a new entry; a concurrent
    /// build that already published the same key is harmless.
    pub fn save(
        &self,
        project_id: ProjectId,
        spec: &CacheSpec,
        workspace: &Path,
    ) -> Result<bool, CacheError> {
        self.ensure_root()?;
        let archive_path = self.archive_path(project_id, spec);
        if archive_path.is_file() {
            return Ok(false);
        }
        let temporary_path = self
            .root
            .join(format!(".{}.{}.tmp", spec.name, Uuid::new_v4()));
        let result = (|| {
            let file = OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&temporary_path)?;
            set_private_file_permissions(&temporary_path)?;
            let mut archive = tar::Builder::new(file);
            let mut archived_path = false;
            for relative_path in &spec.paths {
                let source = workspace.join(relative_path);
                if source.is_dir() {
                    archive.append_dir_all(relative_path, &source)?;
                    archived_path = true;
                } else if source.is_file() {
                    archive.append_path_with_name(&source, relative_path)?;
                    archived_path = true;
                }
            }
            let file = archive.into_inner()?;
            file.sync_all()?;
            if !archived_path {
                fs::remove_file(&temporary_path)?;
                return Ok(false);
            }
            if archive_path.is_file() {
                return Ok(false);
            }
            fs::rename(&temporary_path, &archive_path)?;
            Ok(true)
        })();
        if result.is_err() {
            let _ = fs::remove_file(&temporary_path);
        }
        result
    }

    fn archive_path(&self, project_id: ProjectId, spec: &CacheSpec) -> PathBuf {
        let mut digest = Sha256::new();
        digest.update(project_id.as_bytes());
        digest.update([0]);
        digest.update(spec.key.as_bytes());
        self.root.join(format!(
            "{project_id}-{}.tar",
            hex::encode(digest.finalize())
        ))
    }

    fn ensure_root(&self) -> Result<(), CacheError> {
        fs::create_dir_all(&self.root)?;
        set_private_permissions(&self.root)
    }
}

fn set_private_permissions(path: &Path) -> Result<(), CacheError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
    }
    #[cfg(not(unix))]
    let _ = path;
    Ok(())
}

fn set_private_file_permissions(path: &Path) -> Result<(), CacheError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
    }
    #[cfg(not(unix))]
    let _ = path;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rivet_core::Pipeline;
    use std::fs;
    use tempfile::tempdir;

    fn cache_spec() -> CacheSpec {
        CacheSpec {
            name: "dependencies".into(),
            key: "deps-v1".into(),
            paths: vec!["target".into(), "lockfile".into()],
        }
    }

    #[test]
    fn cache_round_trip_is_project_scoped_and_reopenable() {
        let directory = tempdir().expect("tempdir");
        let workspace = directory.path().join("workspace");
        let cache_root = directory.path().join("cache");
        fs::create_dir_all(workspace.join("target")).expect("target");
        fs::write(workspace.join("target/output.txt"), "compiled").expect("output");
        fs::write(workspace.join("lockfile"), "lock-v1").expect("lockfile");
        let project_id = uuid::Uuid::new_v4();
        let other_project = uuid::Uuid::new_v4();
        let store = CacheStore::new(&cache_root);
        let spec = cache_spec();

        assert!(store.save(project_id, &spec, &workspace).expect("save"));
        fs::remove_dir_all(workspace.join("target")).expect("remove target");
        fs::remove_file(workspace.join("lockfile")).expect("remove lockfile");
        assert!(
            !store
                .restore(other_project, &spec, &workspace)
                .expect("miss")
        );
        assert!(
            store
                .restore(project_id, &spec, &workspace)
                .expect("restore")
        );
        assert_eq!(
            fs::read_to_string(workspace.join("target/output.txt")).expect("restored output"),
            "compiled"
        );
        assert_eq!(
            fs::read_to_string(workspace.join("lockfile")).expect("restored lockfile"),
            "lock-v1"
        );

        let pipeline = Pipeline::from_toml_str(
            r#"
version = 1
name = "cache"
[[caches]]
name = "dependencies"
key = "deps-v1"
paths = ["target", "lockfile"]
[[stages]]
name = "Test"
[[stages.steps]]
name = "unit"
program = "true"
"#,
        )
        .expect("pipeline with cache");
        assert_eq!(pipeline.caches, vec![spec]);
    }

    #[test]
    fn corrupt_cache_is_removed_and_reported() {
        let directory = tempdir().expect("tempdir");
        let workspace = directory.path().join("workspace");
        let cache_root = directory.path().join("cache");
        fs::create_dir_all(&workspace).expect("workspace");
        let project_id = uuid::Uuid::new_v4();
        let store = CacheStore::new(&cache_root);
        let spec = cache_spec();
        store.ensure_root().expect("cache root");
        fs::write(store.archive_path(project_id, &spec), "not a tar").expect("corrupt cache");

        assert!(store.restore(project_id, &spec, &workspace).is_err());
        assert!(!store.archive_path(project_id, &spec).exists());
    }
}
