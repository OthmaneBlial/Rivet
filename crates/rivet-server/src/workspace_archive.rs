use rivet_agent_protocol::WorkspaceTransfer;
use sha2::{Digest, Sha256};
use std::collections::HashSet;
use std::fs;
#[cfg(test)]
use std::io::Cursor;
use std::io::Read;
use std::path::{Path, PathBuf};
use tar::{Archive, Builder};
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
    #[error("workspace archive path is unsafe: {0}")]
    UnsafeArchivePath(PathBuf),
    #[error("workspace archive contains an unsupported entry: {0}")]
    UnsupportedArchiveEntry(PathBuf),
    #[error("workspace archive contains a duplicate entry: {0}")]
    DuplicateArchivePath(PathBuf),
    #[error("workspace archive contains {actual} entries; expected {expected}")]
    EntryCountMismatch { actual: u32, expected: u32 },
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

#[cfg(test)]
fn extract_archive(
    bytes: &[u8],
    destination: &Path,
    expected_entries: u32,
) -> Result<(), WorkspaceArchiveError> {
    extract_archive_reader(Cursor::new(bytes), destination, expected_entries)
}

pub fn extract_archive_file(
    archive_path: &Path,
    destination: &Path,
    expected_entries: u32,
) -> Result<(), WorkspaceArchiveError> {
    let file = fs::File::open(archive_path)?;
    extract_archive_reader(file, destination, expected_entries)
}

fn extract_archive_reader<R: Read>(
    reader: R,
    destination: &Path,
    expected_entries: u32,
) -> Result<(), WorkspaceArchiveError> {
    fs::create_dir_all(destination)?;
    let mut archive = Archive::new(reader);
    let mut entries = 0_u32;
    let mut seen = HashSet::new();
    for entry in archive.entries()? {
        let mut entry = entry?;
        entries = entries.saturating_add(1);
        if entries > MAX_ARCHIVE_ENTRIES {
            return Err(WorkspaceArchiveError::TooManyEntries);
        }
        let relative = entry.path()?.into_owned();
        if !is_safe_archive_path(&relative) {
            return Err(WorkspaceArchiveError::UnsafeArchivePath(relative));
        }
        if !seen.insert(relative.clone()) {
            return Err(WorkspaceArchiveError::DuplicateArchivePath(relative));
        }
        let target = destination.join(&relative);
        if entry.header().entry_type().is_dir() {
            fs::create_dir_all(target)?;
        } else if entry.header().entry_type().is_file() {
            if let Some(parent) = target.parent() {
                fs::create_dir_all(parent)?;
            }
            entry.unpack(target)?;
        } else {
            return Err(WorkspaceArchiveError::UnsupportedArchiveEntry(relative));
        }
    }
    if entries != expected_entries {
        return Err(WorkspaceArchiveError::EntryCountMismatch {
            actual: entries,
            expected: expected_entries,
        });
    }
    Ok(())
}

fn is_safe_archive_path(path: &Path) -> bool {
    !path.as_os_str().is_empty()
        && !path.is_absolute()
        && path
            .components()
            .all(|component| matches!(component, std::path::Component::Normal(_)))
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

    #[test]
    fn rejects_unsupported_archive_entries() {
        let destination = tempdir().expect("destination");
        let mut bytes = Vec::new();
        {
            let mut builder = Builder::new(&mut bytes);
            let mut header = tar::Header::new_gnu();
            header.set_path("link.txt").expect("path");
            header.set_entry_type(tar::EntryType::Symlink);
            header.set_link_name("outside.txt").expect("link");
            header.set_size(0);
            header.set_cksum();
            builder.append(&header, &b""[..]).expect("symlink entry");
            builder.finish().expect("finish");
        }
        assert!(matches!(
            extract_archive(&bytes, destination.path(), 1),
            Err(WorkspaceArchiveError::UnsupportedArchiveEntry(path)) if path == Path::new("link.txt")
        ));
    }
}
