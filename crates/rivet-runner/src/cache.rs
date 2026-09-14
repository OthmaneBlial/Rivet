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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CachePruneResult {
    pub removed_entries: usize,
    pub removed_bytes: u64,
    pub remaining_bytes: u64,
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
        let mut first_error = None;
        for key in
            std::iter::once(spec.key.as_str()).chain(spec.fallback_keys.iter().map(String::as_str))
        {
            let archive_path = self.archive_path_for_key(project_id, key);
            match self.restore_archive(&archive_path, workspace) {
                Ok(true) => return Ok(true),
                Ok(false) => {}
                Err(error) => {
                    first_error.get_or_insert(error);
                }
            }
        }
        first_error.map_or(Ok(false), Err)
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
            let digest = sha256_file(&temporary_path)?;
            if archive_path.is_file() {
                return Ok(false);
            }
            fs::rename(&temporary_path, &archive_path)?;
            let digest_path = digest_path(&archive_path);
            let digest_temporary_path =
                self.root
                    .join(format!(".{}.{}.sha256.tmp", spec.name, Uuid::new_v4()));
            let digest_result = (|| {
                let mut digest_file = OpenOptions::new()
                    .create_new(true)
                    .write(true)
                    .open(&digest_temporary_path)?;
                set_private_file_permissions(&digest_temporary_path)?;
                use std::io::Write;
                writeln!(digest_file, "{digest}")?;
                digest_file.sync_all()?;
                fs::rename(&digest_temporary_path, &digest_path)?;
                Ok::<_, CacheError>(())
            })();
            if digest_result.is_err() {
                let _ = fs::remove_file(&digest_temporary_path);
            }
            digest_result?;
            Ok(true)
        })();
        if result.is_err() {
            let _ = fs::remove_file(&temporary_path);
        }
        result
    }

    /// Remove oldest regular cache archives until the store is within the
    /// requested byte budget. Symlinks, non-archive files, and in-progress
    /// temporary files are never followed or removed.
    pub fn prune(&self, max_bytes: u64) -> Result<CachePruneResult, CacheError> {
        let metadata = match fs::symlink_metadata(&self.root) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                return Ok(CachePruneResult {
                    removed_entries: 0,
                    removed_bytes: 0,
                    remaining_bytes: 0,
                });
            }
            Err(error) => return Err(error.into()),
        };
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "cache root is not a regular directory: {}",
                    self.root.display()
                ),
            )
            .into());
        }
        set_private_permissions(&self.root)?;
        let mut archives = Vec::new();
        for entry in fs::read_dir(&self.root)? {
            let entry = entry?;
            let path = entry.path();
            if path.extension().and_then(|extension| extension.to_str()) != Some("tar") {
                continue;
            }
            let metadata = fs::symlink_metadata(&path)?;
            if metadata.file_type().is_symlink() || !metadata.is_file() {
                continue;
            }
            archives.push((
                metadata.len(),
                metadata.modified().unwrap_or(std::time::UNIX_EPOCH),
                path,
            ));
        }
        archives.sort_by(|left, right| left.1.cmp(&right.1).then_with(|| left.2.cmp(&right.2)));
        let mut remaining_bytes = archives.iter().map(|(size, _, _)| *size).sum::<u64>();
        let mut result = CachePruneResult {
            removed_entries: 0,
            removed_bytes: 0,
            remaining_bytes,
        };
        for (size, _, path) in archives {
            if remaining_bytes <= max_bytes {
                break;
            }
            let current = fs::symlink_metadata(&path)?;
            if current.file_type().is_symlink() || !current.is_file() {
                continue;
            }
            fs::remove_file(&path)?;
            let checksum_path = digest_path(&path);
            if let Ok(checksum_metadata) = fs::symlink_metadata(&checksum_path)
                && checksum_metadata.file_type().is_file()
            {
                fs::remove_file(checksum_path)?;
            }
            remaining_bytes = remaining_bytes.saturating_sub(size);
            result.removed_entries += 1;
            result.removed_bytes += size;
            result.remaining_bytes = remaining_bytes;
        }
        Ok(result)
    }

    fn archive_path(&self, project_id: ProjectId, spec: &CacheSpec) -> PathBuf {
        self.archive_path_for_key(project_id, &spec.key)
    }

    fn archive_path_for_key(&self, project_id: ProjectId, key: &str) -> PathBuf {
        let mut digest = Sha256::new();
        digest.update(project_id.as_bytes());
        digest.update([0]);
        digest.update(key.as_bytes());
        self.root.join(format!(
            "{project_id}-{}.tar",
            hex::encode(digest.finalize())
        ))
    }

    fn restore_archive(&self, archive_path: &Path, workspace: &Path) -> Result<bool, CacheError> {
        let metadata = match fs::symlink_metadata(archive_path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
            Err(error) => return Err(error.into()),
        };
        if metadata.file_type().is_symlink() || !metadata.is_file() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "cache archive is not a regular file: {}",
                    archive_path.display()
                ),
            )
            .into());
        }
        if let Some(expected) = read_digest(&digest_path(archive_path))? {
            let actual = sha256_file(archive_path)?;
            if actual != expected {
                let _ = fs::remove_file(archive_path);
                let _ = fs::remove_file(digest_path(archive_path));
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "cache archive checksum mismatch",
                )
                .into());
            }
        }
        let file = File::open(archive_path)?;
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
            let _ = fs::remove_file(archive_path);
            let _ = fs::remove_file(digest_path(archive_path));
            return Err(error.into());
        }
        Ok(true)
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

fn digest_path(archive_path: &Path) -> PathBuf {
    archive_path.with_extension("sha256")
}

fn sha256_file(path: &Path) -> Result<String, CacheError> {
    use std::io::Read;
    let mut file = File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(hex::encode(hasher.finalize()))
}

fn read_digest(path: &Path) -> Result<Option<String>, CacheError> {
    let contents = match fs::read_to_string(path) {
        Ok(contents) => contents,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let digest = contents.trim();
    if digest.len() != 64 || !digest.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "cache checksum sidecar is invalid",
        )
        .into());
    }
    Ok(Some(digest.to_ascii_lowercase()))
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
            fallback_keys: vec![],
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
        assert!(digest_path(&store.archive_path(project_id, &spec)).is_file());
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
    fn restore_uses_a_valid_fallback_after_a_corrupt_primary_entry() {
        let directory = tempdir().expect("tempdir");
        let workspace = directory.path().join("workspace");
        let cache_root = directory.path().join("cache");
        fs::create_dir_all(&workspace).expect("workspace");
        fs::write(workspace.join("lockfile"), "fallback").expect("lockfile");
        let project_id = uuid::Uuid::new_v4();
        let store = CacheStore::new(&cache_root);
        let fallback = cache_spec();
        let primary = CacheSpec {
            key: "branch-feature".into(),
            fallback_keys: vec![fallback.key.clone()],
            ..fallback.clone()
        };
        assert!(
            store
                .save(project_id, &fallback, &workspace)
                .expect("fallback save")
        );
        store.ensure_root().expect("cache root");
        fs::write(store.archive_path(project_id, &primary), "not a tar").expect("corrupt primary");
        fs::remove_file(workspace.join("lockfile")).expect("remove lockfile");

        assert!(
            store
                .restore(project_id, &primary, &workspace)
                .expect("fallback restore")
        );
        assert_eq!(
            fs::read_to_string(workspace.join("lockfile")).expect("restored lockfile"),
            "fallback"
        );
        assert!(!store.archive_path(project_id, &primary).exists());
    }

    #[test]
    fn prune_removes_oldest_archives_without_following_symlinks() {
        let directory = tempdir().expect("tempdir");
        let cache_root = directory.path().join("cache");
        let outside = directory.path().join("outside.tar");
        let store = CacheStore::new(&cache_root);
        store.ensure_root().expect("cache root");
        let oldest = cache_root.join("oldest.tar");
        let newest = cache_root.join("newest.tar");
        fs::write(&oldest, vec![0_u8; 10]).expect("oldest archive");
        std::thread::sleep(std::time::Duration::from_millis(20));
        fs::write(&newest, vec![1_u8; 20]).expect("newest archive");
        fs::write(&outside, b"outside").expect("outside file");
        #[cfg(unix)]
        std::os::unix::fs::symlink(&outside, cache_root.join("linked.tar")).expect("cache symlink");

        let result = store.prune(20).expect("prune");
        assert_eq!(result.removed_entries, 1);
        assert_eq!(result.removed_bytes, 10);
        assert_eq!(result.remaining_bytes, 20);
        assert!(!oldest.exists());
        assert!(newest.exists());
        assert!(outside.exists());
        #[cfg(unix)]
        assert!(cache_root.join("linked.tar").exists());
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

    #[test]
    fn checksum_sidecar_rejects_a_modified_valid_archive() {
        let directory = tempdir().expect("tempdir");
        let workspace = directory.path().join("workspace");
        let cache_root = directory.path().join("cache");
        fs::create_dir_all(&workspace).expect("workspace");
        fs::write(workspace.join("lockfile"), "cache").expect("lockfile");
        let project_id = uuid::Uuid::new_v4();
        let store = CacheStore::new(&cache_root);
        let spec = cache_spec();
        assert!(store.save(project_id, &spec, &workspace).expect("save"));
        let archive = store.archive_path(project_id, &spec);
        let original = fs::read(&archive).expect("archive");
        fs::write(&archive, [original.as_slice(), b"tamper"].concat()).expect("tamper");

        assert!(store.restore(project_id, &spec, &workspace).is_err());
        assert!(!archive.exists());
        assert!(!digest_path(&archive).exists());
    }
}
