use crate::invalid;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

pub(crate) fn sync_directory(path: &Path) -> io::Result<()> {
    #[cfg(unix)]
    let file = File::open(path)?;
    #[cfg(windows)]
    let file = {
        use std::os::windows::fs::OpenOptionsExt;
        OpenOptions::new()
            .write(true)
            .custom_flags(0x0200_0000)
            .open(path)?
    };
    #[cfg(not(any(unix, windows)))]
    return Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "directory synchronization unsupported",
    ));
    #[cfg(any(unix, windows))]
    file.sync_all()
}

pub(crate) struct Directory {
    pub path: PathBuf,
    pub open_directory_syncs: u64,
    pub open_content_syncs: u64,
    pub open_metadata_bytes: u64,
    pub fresh: bool,
    _lock: File,
}

#[derive(Clone, Copy)]
pub(crate) enum Engine {
    Lsm,
    Redb,
}

impl Engine {
    fn identity(self) -> &'static [u8] {
        match self {
            Self::Lsm => b"micro-raft-state-store\nversion=1\nengine=lsm\n",
            Self::Redb => b"micro-raft-state-store\nversion=1\nengine=redb\n",
        }
    }
    fn primary(self) -> &'static str {
        match self {
            Self::Lsm => "CURRENT",
            Self::Redb => "state.redb",
        }
    }
}

impl Directory {
    pub fn open(path: &Path, engine: Engine) -> io::Result<Self> {
        let path = std::path::absolute(path)?;
        let mut missing = Vec::new();
        let mut ancestor = path.as_path();
        while !ancestor.try_exists()? {
            missing.push(ancestor.to_path_buf());
            ancestor = ancestor
                .parent()
                .ok_or_else(|| invalid("directory has no ancestor"))?;
        }
        fs::create_dir_all(&path)?;
        let mut open_directory_syncs = 0;
        for directory in &missing {
            sync_directory(directory)?;
            open_directory_syncs += 1;
        }
        if let Some(directory) = missing.last().and_then(|entry| entry.parent()) {
            sync_directory(directory)?;
            open_directory_syncs += 1;
        }
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path.join("LOCK"))?;
        lock.try_lock().map_err(io::Error::other)?;
        sync_directory(&path)?;
        open_directory_syncs += 1;
        let marker = path.join("ENGINE");
        let staged = path.join("ENGINE.new");
        match fs::symlink_metadata(&staged) {
            Ok(_) => {
                return Err(invalid(
                    "interrupted engine identity publication; recovery requires review",
                ))
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
        let fresh = match fs::symlink_metadata(&marker) {
            Ok(metadata) => {
                if !metadata.is_file()
                    || metadata.len() != engine.identity().len() as u64
                    || fs::read(&marker)? != engine.identity()
                {
                    return Err(invalid(
                        "store engine identity is corrupt or belongs to another engine",
                    ));
                }
                let payload = fs::symlink_metadata(path.join(engine.primary()))
                    .map_err(|error| if error.kind() == io::ErrorKind::NotFound {
                        invalid("selected engine payload is missing; refusing empty reinitialization")
                    } else { error })?;
                if !payload.is_file() {
                    return Err(invalid("selected engine payload is not a regular file"));
                }
                false
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                // Before a first payload is created, durably reserve its engine.
                // A crash afterward leaves explicit incomplete initialization;
                // subsequent opens must not invent an empty replacement state.
                for entry in fs::read_dir(&path)? {
                    if entry?.file_name() != "LOCK" {
                        return Err(invalid(
                            "unmarked nonempty store directory; refusing stale/new ambiguity",
                        ));
                    }
                }
                let mut file = OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(&staged)?;
                file.write_all(engine.identity())?;
                file.sync_all()?;
                drop(file);
                fs::rename(&staged, &marker)?;
                sync_directory(&path)?;
                open_directory_syncs += 1;
                true
            }
            Err(error) => return Err(error),
        };
        Ok(Self {
            path,
            open_directory_syncs,
            open_content_syncs: u64::from(fresh),
            open_metadata_bytes: if fresh {
                engine.identity().len() as u64
            } else {
                0
            },
            fresh,
            _lock: lock,
        })
    }
    pub fn disk_bytes(&self) -> io::Result<u64> {
        let mut total = 0u64;
        for entry in fs::read_dir(&self.path)? {
            let entry = entry?;
            if entry.file_type()?.is_file() {
                total = total
                    .checked_add(entry.metadata()?.len())
                    .ok_or_else(|| invalid("disk bytes overflow"))?;
            }
        }
        Ok(total)
    }
}
