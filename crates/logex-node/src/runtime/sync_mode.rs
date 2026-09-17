//! Startup policy persisted while the storage owner holds exclusive access.
//! Initialized parents must exist. Path replacement by unrelated writers and
//! mount identity require the separate expected-volume protection.

use std::fs::{self, File};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

const SYNC_MODE_FILE_NAME: &str = "sync-mode.json";
pub(super) const MAX_SYNC_MODE_BYTES: usize = 4096;

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct SyncModeState {
    pub(super) historical_sync_disabled: bool,
}

pub(super) fn sync_mode_state_path(data_dir: &Path) -> PathBuf {
    data_dir.join(SYNC_MODE_FILE_NAME)
}

fn open_parent(data_dir: &Path) -> io::Result<File> {
    let parent = File::open(data_dir)?;
    if !parent.metadata()?.is_dir() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "sync-mode parent is not a directory",
        ));
    }
    Ok(parent)
}

fn state_entry_exists(path: &Path) -> io::Result<bool> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_file() => Ok(true),
        Ok(_) => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "sync-mode state must be a regular file",
        )),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error),
    }
}

pub(super) fn read_sync_mode_state(data_dir: &Path) -> Result<Option<SyncModeState>, String> {
    let path = sync_mode_state_path(data_dir);
    let read = || -> io::Result<Option<SyncModeState>> {
        let parent = open_parent(data_dir)?;
        if !state_entry_exists(&path)? {
            // A previous conversion may have unlinked the marker before a
            // reported sync error. Harden the observed absence on retry.
            parent.sync_all()?;
            return Ok(None);
        }
        let file = File::open(&path)?;
        let metadata = file.metadata()?;
        if !metadata.is_file() || metadata.len() > MAX_SYNC_MODE_BYTES as u64 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "sync-mode state must be a regular file of at most 4096 bytes",
            ));
        }
        let mut contents = Vec::new();
        (&file)
            .take((MAX_SYNC_MODE_BYTES + 1) as u64)
            .read_to_end(&mut contents)?;
        if contents.len() > MAX_SYNC_MODE_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "sync-mode state exceeds 4096 bytes",
            ));
        }
        let state = serde_json::from_slice(&contents)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        // A prior publication may be visible despite an uncertain sync result.
        // Accept it only after the complete file and its name are durable.
        file.sync_all()?;
        parent.sync_all()?;
        Ok(Some(state))
    };
    read().map_err(|error| format!("failed to read {}: {error}", path.display()))
}

pub(super) fn write_sync_mode_state(data_dir: &Path, state: &SyncModeState) -> Result<(), String> {
    write_with_checkpoints(data_dir, state, |_| Ok(())).map_err(|error| {
        format!(
            "failed to persist {}: {error}",
            sync_mode_state_path(data_dir).display()
        )
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum WriteStep {
    Staged,
    Written,
    FileSynced,
    Replaced,
    DirectorySynced,
}

// The callback gives tests finite failure points in the actual publication path.
// Production passes a no-op; no global fault state or environment switches exist.
pub(super) fn write_with_checkpoints(
    data_dir: &Path,
    state: &SyncModeState,
    mut checkpoint: impl FnMut(WriteStep) -> io::Result<()>,
) -> io::Result<()> {
    let parent = open_parent(data_dir)?;
    let path = sync_mode_state_path(data_dir);
    state_entry_exists(&path)?;
    let contents = serde_json::to_vec_pretty(state).map_err(io::Error::other)?;
    let mut staged = logex_fs::StagedFile::new_in(data_dir, ".sync-mode-")?;
    checkpoint(WriteStep::Staged)?;
    staged.as_file_mut().write_all(&contents)?;
    checkpoint(WriteStep::Written)?;
    staged.as_file().sync_all()?;
    checkpoint(WriteStep::FileSynced)?;
    staged.persist(&path)?;
    checkpoint(WriteStep::Replaced)?;
    parent.sync_all()?;
    checkpoint(WriteStep::DirectorySynced)
}

pub(super) fn remove_sync_mode_state(data_dir: &Path) -> Result<(), String> {
    remove_with_checkpoints(data_dir, |_| Ok(())).map_err(|error| {
        format!(
            "failed to remove {}: {error}",
            sync_mode_state_path(data_dir).display()
        )
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RemoveStep {
    BeforeRemove,
    Removed,
    DirectorySynced,
}

pub(super) fn remove_with_checkpoints(
    data_dir: &Path,
    mut checkpoint: impl FnMut(RemoveStep) -> io::Result<()>,
) -> io::Result<()> {
    let parent = open_parent(data_dir)?;
    let path = sync_mode_state_path(data_dir);
    let exists = state_entry_exists(&path)?;
    checkpoint(RemoveStep::BeforeRemove)?;
    if exists {
        fs::remove_file(&path)?;
    }
    checkpoint(RemoveStep::Removed)?;
    // Also harden an already-absent marker before accepting a retried conversion.
    parent.sync_all()?;
    checkpoint(RemoveStep::DirectorySynced)
}
