use rivet_agent_protocol::WorkspaceTransfer;
use sha2::{Digest, Sha256};
use std::fs;
use std::path::{Path, PathBuf};
use tar::Builder;
use thiserror::Error;
use walkdir::{DirEntry, WalkDir};

const MAX_ARCHIVE_BYTES: u64 = 512 * 1024 * 1024;
const MAX_ARCHIVE_ENTRIES: u32 = 100_000;

#[derive(Debug)]
pub struct WorkspaceArchive {
    pub bytes: Vec<u8>,
    pub transfer: WorkspaceTransfer,
}

#[derive(Debug, Error)]
pub enum WorkspaceArchiveError {
    #[error("workspace root is not a directory: {0}")]
    InvalidRoot(PathBuf),
    #[error("could not inspect workspace entry: {0}")]
    Walk(#[from] walkdir::Error),
    #[error("workspace contains an unsupported symbolic link: {0}")]
    SymbolicLink(PathBuf),
    #[error("workspace contains too many entries")]
    TooManyEntries,
    #[error("workspace source files exceed the remote transfer limit")]
    TooLarge,
    #[error("could not create workspace archive: {0}")]
    Archive(#[from] std::io::Error),
}

pub fn archive_workspace(root: &Path) -> Result<WorkspaceArchive, WorkspaceArchiveError> {
    let root =
        fs::canonicalize(root).map_err(|_| WorkspaceArchiveError::InvalidRoot(root.into()))?;
    if !root.is_dir() {
        return Err(WorkspaceArchiveError::InvalidRoot(root));
    }

    let mut entries = Vec::new();
    let mut source_bytes = 0_u64;
    for entry in WalkDir::new(&root)
        .follow_links(false)
        .into_iter()
        .filter_entry(|entry| !is_internal_entry(entry, &root))
    {
        let entry = entry?;
        if entry.path() == root {
            continue;
        }
        let relative = entry
            .path()
            .strip_prefix(&root)
            .expect("walkdir entry is below workspace root")
            .to_owned();
        if entry.file_type().is_symlink() {
            return Err(WorkspaceArchiveError::SymbolicLink(relative));
        }
        if entry.file_type().is_file() {
            source_bytes = source_bytes
                .checked_add(entry.metadata()?.len())
                .ok_or(WorkspaceArchiveError::TooLarge)?;
            if source_bytes > MAX_ARCHIVE_BYTES {
                return Err(WorkspaceArchiveError::TooLarge);
            }
        }
        entries.push((entry.path().to_owned(), relative));
        if entries.len() > MAX_ARCHIVE_ENTRIES as usize {
            return Err(WorkspaceArchiveError::TooManyEntries);
        }
    }
    entries.sort_by(|left, right| left.1.cmp(&right.1));

    let mut builder = Builder::new(Vec::new());
    for (path, relative) in &entries {
        if path.is_dir() {
            builder.append_dir(relative, path)?;
        } else {
            builder.append_path_with_name(path, relative)?;
        }
    }
    let bytes = builder.into_inner()?;
    if bytes.len() as u64 > MAX_ARCHIVE_BYTES {
        return Err(WorkspaceArchiveError::TooLarge);
    }
    let sha256 = hex::encode(Sha256::digest(&bytes));
    let transfer = WorkspaceTransfer {
        total_bytes: bytes.len() as u64,
        file_count: entries.len() as u32,
        sha256,
    };
    Ok(WorkspaceArchive { bytes, transfer })
}

fn is_internal_entry(entry: &DirEntry, root: &Path) -> bool {
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::tempdir;

    #[test]
    fn archives_workspace_without_internal_state_and_with_stable_digest() {
        let first = tempdir().expect("first tempdir");
        fs::create_dir(first.path().join("src")).expect("src");
        fs::write(first.path().join("src/main.rs"), "fn main() {}\n").expect("source");
        fs::create_dir(first.path().join(".git")).expect("git");
        fs::write(first.path().join(".git/config"), "private").expect("git state");
        let archive = archive_workspace(first.path()).expect("archive");
        assert_eq!(archive.transfer.file_count, 2);
        assert_eq!(archive.transfer.total_bytes, archive.bytes.len() as u64);
        archive.transfer.validate().expect("valid transfer");
        let second = tempdir().expect("second tempdir");
        fs::create_dir(second.path().join("src")).expect("src");
        fs::write(second.path().join("src/main.rs"), "fn main() {}\n").expect("source");
        assert_eq!(
            archive_workspace(first.path())
                .expect("archive again")
                .transfer
                .sha256,
            archive_workspace(second.path())
                .expect("same archive")
                .transfer
                .sha256
        );
    }

    #[test]
    fn rejects_symbolic_links_in_a_remote_workspace() {
        let directory = tempdir().expect("tempdir");
        fs::write(directory.path().join("outside.txt"), "outside").expect("outside");
        #[cfg(unix)]
        std::os::unix::fs::symlink(
            directory.path().join("outside.txt"),
            directory.path().join("link.txt"),
        )
        .expect("symlink");
        #[cfg(unix)]
        assert!(matches!(
            archive_workspace(directory.path()),
            Err(WorkspaceArchiveError::SymbolicLink(path)) if path == Path::new("link.txt")
        ));
    }
}
