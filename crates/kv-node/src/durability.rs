//! Content and directory-metadata publication barriers.
//!
//! Linux uses fsync on an opened directory. Windows opens a directory with
//! GENERIC_WRITE and FILE_FLAG_BACKUP_SEMANTICS, then File::sync_all invokes
//! FlushFileBuffers. Unsupported filesystems or access policies fail explicitly.
//! These barriers depend on the OS/device honoring their documented contract.

use std::fs::{self, File};
use std::io::{self, Write};
use std::path::{Component, Path};

/// Sync a directory's metadata; never silently turn unsupported sync into success.
pub fn sync_directory(path: &Path) -> io::Result<()> {
    #[cfg(unix)]
    let directory = File::open(path)?;
    #[cfg(windows)]
    let directory = {
        use std::os::windows::fs::OpenOptionsExt;
        fs::OpenOptions::new()
            .write(true)
            .custom_flags(0x0200_0000) // FILE_FLAG_BACKUP_SEMANTICS
            .open(path)?
    };
    #[cfg(not(any(unix, windows)))]
    return Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "directory synchronization is unsupported",
    ));
    #[cfg(any(unix, windows))]
    {
        if !directory.metadata()?.is_dir() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "directory sync requires a directory",
            ));
        }
        directory.sync_all().map_err(|error| {
            io::Error::new(
                error.kind(),
                format!("directory sync {}: {error}", path.display()),
            )
        })
    }
}

/// Make missing ancestors durable from the deepest directory back to its parent.
pub fn create_dir_all(path: &Path) -> io::Result<()> {
    let absolute = std::path::absolute(path)?;
    let mut missing = Vec::new();
    let mut cursor = absolute.as_path();
    while !cursor.try_exists()? {
        missing.push(cursor.to_path_buf());
        cursor = cursor.parent().ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "directory has no parent")
        })?;
    }
    fs::create_dir_all(&absolute)?;
    for directory in &missing {
        sync_directory(directory)?;
    }
    if let Some(highest) = missing.last() {
        if let Some(parent) = highest.parent() {
            sync_directory(parent)?;
        }
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PublicationStep {
    ContentWritten,
    ContentSynced,
    Renamed,
    DirectorySynced,
}

fn filename(name: &str) -> io::Result<()> {
    let mut components = Path::new(name).components();
    if !matches!(components.next(), Some(Component::Normal(_)))
        || components.next().is_some()
        || name.is_empty()
        || !name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"._-".contains(&byte))
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "publication names must be single portable filenames",
        ));
    }
    Ok(())
}

/// Write and content-sync a sibling temporary file, rename, then sync the directory.
/// A post-rename error is uncertain publication: callers must stop using the store.
pub fn publish_file(dir: &Path, temporary: &str, destination: &str, data: &[u8]) -> io::Result<()> {
    publish_with_hook(dir, temporary, destination, data, |_| Ok(()))
}

pub(crate) fn publish_with_hook(
    dir: &Path,
    temporary: &str,
    destination: &str,
    data: &[u8],
    mut hook: impl FnMut(PublicationStep) -> io::Result<()>,
) -> io::Result<()> {
    filename(temporary)?;
    filename(destination)?;
    if temporary == destination {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "temporary and destination must differ",
        ));
    }
    let mut file = File::create(dir.join(temporary))?;
    file.write_all(data)?;
    hook(PublicationStep::ContentWritten)?;
    file.sync_all()?;
    hook(PublicationStep::ContentSynced)?;
    drop(file); // Windows requires closing our handle before replacing the path.
    fs::rename(dir.join(temporary), dir.join(destination))?;
    let mut finish = || -> io::Result<()> {
        hook(PublicationStep::Renamed)?;
        sync_directory(dir)?;
        hook(PublicationStep::DirectorySynced)
    };
    // Even when the rename is visible to this process, a failed metadata barrier
    // cannot be acknowledged as durable publication to an upstream caller.
    finish().map_err(|error| {
        io::Error::new(
            error.kind(),
            format!("uncertain publication after rename of {destination}: {error}"),
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT: AtomicU64 = AtomicU64::new(0);
    fn temporary() -> std::path::PathBuf {
        let directory = std::env::temp_dir().join(format!(
            "micro-raft-durable-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        create_dir_all(&directory).unwrap();
        directory
    }

    #[test]
    fn real_directory_sync_and_nested_creation() {
        let dir = temporary();
        let nested = dir.join("a").join("b");
        create_dir_all(&nested).unwrap();
        sync_directory(&nested).unwrap();
        fs::write(nested.join("regular"), b"data").unwrap();
        assert!(sync_directory(&nested.join("regular")).is_err());
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn publication_orders_content_rename_and_directory_barriers() {
        let dir = temporary();
        publish_file(&dir, "state.new", "state", b"old").unwrap();
        let mut steps = Vec::new();
        publish_with_hook(&dir, "state.new", "state", b"new", |step| {
            steps.push(step);
            let expected = if matches!(
                step,
                PublicationStep::ContentWritten | PublicationStep::ContentSynced
            ) {
                b"old"
            } else {
                b"new"
            };
            assert_eq!(fs::read(dir.join("state")).unwrap(), expected);
            Ok(())
        })
        .unwrap();
        assert_eq!(
            steps,
            [
                PublicationStep::ContentWritten,
                PublicationStep::ContentSynced,
                PublicationStep::Renamed,
                PublicationStep::DirectorySynced
            ]
        );
        assert!(!dir.join("state.new").exists());
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn interruption_before_rename_keeps_old_after_rename_is_uncertain() {
        for failure_at in [
            PublicationStep::ContentWritten,
            PublicationStep::ContentSynced,
            PublicationStep::Renamed,
            PublicationStep::DirectorySynced,
        ] {
            let dir = temporary();
            publish_file(&dir, "state.new", "state", b"old").unwrap();
            let error = publish_with_hook(&dir, "state.new", "state", b"new", |step| {
                if step == failure_at {
                    Err(io::Error::other("injected publication failure"))
                } else {
                    Ok(())
                }
            })
            .unwrap_err();
            let renamed = matches!(
                failure_at,
                PublicationStep::Renamed | PublicationStep::DirectorySynced
            );
            assert_eq!(
                fs::read(dir.join("state")).unwrap(),
                if renamed { b"new" } else { b"old" }
            );
            assert_eq!(error.to_string().contains("uncertain publication"), renamed);
            fs::remove_dir_all(dir).unwrap();
        }
    }

    #[test]
    fn invalid_names_do_not_change_existing_file() {
        let dir = temporary();
        for name in [
            "../escape",
            "",
            ".",
            "..",
            "/absolute",
            "stream:other",
            "x/y",
        ] {
            assert!(publish_file(&dir, "state.new", name, b"data").is_err());
        }
        assert!(publish_file(&dir, "state", "state", b"data").is_err());
        assert_eq!(fs::read_dir(&dir).unwrap().count(), 0);
        fs::remove_dir_all(dir).unwrap();
    }
}
