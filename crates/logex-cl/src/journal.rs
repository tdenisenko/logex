//! Incremental consensus persistence with an atomically published commit frontier.
//! Bytes beyond CURRENT's end are uncommitted; damage within it is never repaired
//! by falling back to an older frame or generation.

use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use serde::{Serialize, de::DeserializeOwned};
use sha2::{Digest, Sha256};

use crate::{ConsensusSnapshot, snapshot_format};

const CURRENT_MAGIC: &[u8; 8] = b"LXCLHD01";
const JOURNAL_MAGIC: &[u8; 8] = b"LXCLJR01";
const FRAME_MAGIC: &[u8; 8] = b"LXCLDL01";
const CURRENT_LEN: usize = 152;
const JOURNAL_HEADER_LEN: usize = 104;
const FRAME_HEADER_LEN: usize = 104;
const CHECKSUM_LEN: u64 = 32;
const MIN_CHECKPOINT_BYTES: u64 = 1024 * 1024;
const GENERATION_PREFIX: &str = "generation-";
const CHECKPOINT_FILE: &str = "checkpoint.bin";
const JOURNAL_FILE: &str = "journal.bin";

#[derive(Debug, Clone, PartialEq, Eq)]
struct Frontier {
    generation: [u8; 16],
    checkpoint_sequence: u64,
    checkpoint: snapshot_format::Integrity,
    end: u64,
    sequence: u64,
    digest: [u8; 32],
}

#[derive(Debug)]
pub(super) struct Journal {
    frontier: Frontier,
    #[cfg(test)]
    force_checkpoint: bool,
}

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

fn at(path: &Path, operation: &str, error: io::Error) -> io::Error {
    io::Error::new(
        error.kind(),
        format!("consensus {operation} {}: {error}", path.display()),
    )
}

fn parent(path: &Path) -> io::Result<&Path> {
    path.parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .ok_or_else(|| invalid("consensus CURRENT requires a parent directory"))
}

fn json_error(error: serde_json::Error) -> io::Error {
    io::Error::new(
        error.io_error_kind().unwrap_or(io::ErrorKind::InvalidData),
        error,
    )
}

fn read_u64(bytes: &[u8]) -> u64 {
    u64::from_le_bytes(bytes.try_into().expect("fixed integer width"))
}

fn generation_path(current: &Path, generation: &[u8; 16]) -> io::Result<PathBuf> {
    let mut name = String::from(GENERATION_PREFIX);
    for byte in generation {
        use std::fmt::Write as _;
        write!(&mut name, "{byte:02x}").expect("String formatting cannot fail");
    }
    Ok(parent(current)?.join(name))
}

impl Frontier {
    fn encode(&self) -> [u8; CURRENT_LEN] {
        let mut bytes = [0; CURRENT_LEN];
        bytes[..8].copy_from_slice(CURRENT_MAGIC);
        bytes[8..24].copy_from_slice(&self.generation);
        bytes[24..32].copy_from_slice(&self.checkpoint_sequence.to_le_bytes());
        bytes[32..40].copy_from_slice(&self.checkpoint.encoded_len.to_le_bytes());
        bytes[40..72].copy_from_slice(&self.checkpoint.payload_digest);
        bytes[72..80].copy_from_slice(&self.end.to_le_bytes());
        bytes[80..88].copy_from_slice(&self.sequence.to_le_bytes());
        bytes[88..120].copy_from_slice(&self.digest);
        let digest = Sha256::digest(&bytes[..120]);
        bytes[120..].copy_from_slice(&digest);
        bytes
    }

    fn read(path: &Path) -> io::Result<Self> {
        let result = (|| {
            let mut file = File::open(path)?;
            if file.metadata()?.len() != CURRENT_LEN as u64 {
                return Err(invalid("CURRENT has an invalid exact length"));
            }
            let mut bytes = [0; CURRENT_LEN];
            file.read_exact(&mut bytes)?;
            if &bytes[..8] != CURRENT_MAGIC || Sha256::digest(&bytes[..120])[..] != bytes[120..] {
                return Err(invalid("CURRENT format or checksum is invalid"));
            }
            let mut extra = [0];
            if file.read(&mut extra)? != 0 {
                return Err(invalid("CURRENT has trailing bytes"));
            }
            let frontier = Self {
                generation: bytes[8..24].try_into().expect("fixed generation width"),
                checkpoint_sequence: read_u64(&bytes[24..32]),
                checkpoint: snapshot_format::Integrity {
                    encoded_len: read_u64(&bytes[32..40]),
                    payload_digest: bytes[40..72].try_into().expect("fixed digest width"),
                },
                end: read_u64(&bytes[72..80]),
                sequence: read_u64(&bytes[80..88]),
                digest: bytes[88..120].try_into().expect("fixed digest width"),
            };
            if frontier.end < JOURNAL_HEADER_LEN as u64
                || frontier.sequence < frontier.checkpoint_sequence
            {
                return Err(invalid("CURRENT has an invalid committed boundary"));
            }
            Ok(frontier)
        })();
        result.map_err(|error| at(path, "read frontier", error))
    }

    fn journal_header(&self) -> [u8; JOURNAL_HEADER_LEN] {
        let mut header = [0; JOURNAL_HEADER_LEN];
        header[..8].copy_from_slice(JOURNAL_MAGIC);
        header[8..24].copy_from_slice(&self.generation);
        header[24..32].copy_from_slice(&self.checkpoint_sequence.to_le_bytes());
        header[32..40].copy_from_slice(&self.checkpoint.encoded_len.to_le_bytes());
        header[40..72].copy_from_slice(&self.checkpoint.payload_digest);
        let digest = Sha256::digest(&header[..72]);
        header[72..].copy_from_slice(&digest);
        header
    }

    fn base_digest(&self) -> [u8; 32] {
        Sha256::digest(self.journal_header()).into()
    }
}

impl Journal {
    /// Initialize only inside the caller's exclusively owned empty staging
    /// directory. The caller publishes that whole directory before exposing it.
    pub(super) fn create(current: &Path, snapshot: &ConsensusSnapshot) -> io::Result<Self> {
        let directory = parent(current)?;
        let first = fs::read_dir(directory)
            .and_then(|mut entries| entries.next().transpose())
            .map_err(|error| at(directory, "inspect initial staging directory", error))?;
        if first.is_some() {
            return Err(invalid("initial consensus staging directory is not empty"));
        }
        let frontier = create_generation(current, snapshot, 0)?;
        publish_frontier(current, &frontier)?;
        Ok(Self {
            frontier,
            #[cfg(test)]
            force_checkpoint: false,
        })
    }

    /// Validate the complete committed history before returning any state. This
    /// is read-only, including when interrupted writes left an extra suffix.
    pub(super) fn open<D: DeserializeOwned>(
        current: &Path,
        restore_checkpoint: impl FnOnce(ConsensusSnapshot) -> io::Result<ConsensusSnapshot>,
        mut apply: impl FnMut(&mut ConsensusSnapshot, D) -> io::Result<()>,
    ) -> io::Result<(Self, ConsensusSnapshot)> {
        let frontier = Frontier::read(current)?;
        let generation = generation_path(current, &frontier.generation)?;
        let checkpoint_path = generation.join(CHECKPOINT_FILE);
        let checkpoint = File::open(&checkpoint_path)
            .map_err(|error| at(&checkpoint_path, "open checkpoint", error))?;
        let (snapshot, integrity) = snapshot_format::read_with_integrity(checkpoint)
            .map_err(|error| at(&checkpoint_path, "read checkpoint", error))?;
        if integrity != frontier.checkpoint {
            return Err(at(
                &checkpoint_path,
                "validate checkpoint",
                invalid("checkpoint differs from CURRENT"),
            ));
        }
        let mut snapshot = restore_checkpoint(snapshot)
            .map_err(|error| at(&checkpoint_path, "restore checkpoint", error))?;
        let journal_path = generation.join(JOURNAL_FILE);
        let file =
            File::open(&journal_path).map_err(|error| at(&journal_path, "open journal", error))?;
        let result = (|| {
            if file.metadata()?.len() < frontier.end {
                return Err(invalid("committed journal is truncated"));
            }
            let mut reader = BufReader::new(file.take(frontier.end));
            let mut header = [0; JOURNAL_HEADER_LEN];
            reader.read_exact(&mut header)?;
            if header != frontier.journal_header() {
                return Err(invalid(
                    "journal generation or checkpoint binding differs from CURRENT",
                ));
            }
            let mut sequence = frontier.checkpoint_sequence;
            let mut digest = frontier.base_digest();
            let mut offset = JOURNAL_HEADER_LEN as u64;
            while offset < frontier.end {
                let remaining = frontier.end - offset;
                if remaining < FRAME_HEADER_LEN as u64 + CHECKSUM_LEN {
                    return Err(invalid("committed frame header is truncated"));
                }
                let mut frame = [0; FRAME_HEADER_LEN];
                reader.read_exact(&mut frame)?;
                let expected_sequence = sequence
                    .checked_add(1)
                    .ok_or_else(|| invalid("journal sequence overflow"))?;
                if &frame[..8] != FRAME_MAGIC
                    || frame[8..24] != frontier.generation
                    || read_u64(&frame[24..32]) != expected_sequence
                    || frame[32..64] != frontier.checkpoint.payload_digest
                    || frame[64..96] != digest
                {
                    return Err(invalid(
                        "journal frame identity, sequence or parent digest differs",
                    ));
                }
                let payload_len = read_u64(&frame[96..104]);
                let frame_len = payload_len
                    .checked_add(FRAME_HEADER_LEN as u64 + CHECKSUM_LEN)
                    .filter(|&length| length <= remaining)
                    .ok_or_else(|| invalid("committed frame payload exceeds its exact boundary"))?;
                let mut hash = Sha256::new();
                hash.update(&frame[..96]);
                let mut payload = BufReader::new(HashReader {
                    inner: (&mut reader).take(payload_len),
                    hash,
                });
                let delta: D = serde_json::from_reader(&mut payload).map_err(json_error)?;
                let mut payload = payload.into_inner();
                if payload.inner.limit() != 0 {
                    return Err(invalid("committed frame payload is truncated"));
                }
                payload.hash.update(payload_len.to_le_bytes());
                let expected_digest: [u8; 32] = payload.hash.finalize().into();
                let mut stored_digest = [0; 32];
                reader.read_exact(&mut stored_digest)?;
                if stored_digest != expected_digest {
                    return Err(invalid("committed journal frame checksum differs"));
                }
                apply(&mut snapshot, delta)?;
                sequence = expected_sequence;
                digest = stored_digest;
                offset += frame_len;
            }
            if sequence != frontier.sequence || digest != frontier.digest || offset != frontier.end
            {
                return Err(invalid(
                    "journal final sequence, digest or end differs from CURRENT",
                ));
            }
            Ok(())
        })();
        result.map_err(|error| at(&journal_path, "replay committed journal", error))?;
        Ok((
            Self {
                frontier,
                #[cfg(test)]
                force_checkpoint: false,
            },
            snapshot,
        ))
    }

    /// Append only to the existing named journal. A save failure is terminal to
    /// the caller; the frontier remains unchanged in memory until fully durable.
    pub(super) fn append<D: Serialize>(&mut self, current: &Path, delta: &D) -> io::Result<()> {
        self.verify_current(current)?;
        let path = generation_path(current, &self.frontier.generation)?.join(JOURNAL_FILE);
        let result = (|| {
            let mut file = OpenOptions::new().read(true).write(true).open(&path)?;
            let length = file.metadata()?.len();
            if length < self.frontier.end {
                return Err(invalid("committed journal was truncated before append"));
            }
            let mut header = [0; JOURNAL_HEADER_LEN];
            file.read_exact(&mut header)?;
            if header != self.frontier.journal_header() {
                return Err(invalid("journal identity changed before append"));
            }
            if length > self.frontier.end {
                // Only the already-validated uncommitted suffix can be retired.
                file.set_len(self.frontier.end)?;
                file.sync_all()?;
                phase("tail_truncated")?;
            }
            let sequence = self
                .frontier
                .sequence
                .checked_add(1)
                .ok_or_else(|| invalid("journal sequence overflow"))?;
            let mut frame = [0; FRAME_HEADER_LEN];
            frame[..8].copy_from_slice(FRAME_MAGIC);
            frame[8..24].copy_from_slice(&self.frontier.generation);
            frame[24..32].copy_from_slice(&sequence.to_le_bytes());
            frame[32..64].copy_from_slice(&self.frontier.checkpoint.payload_digest);
            frame[64..96].copy_from_slice(&self.frontier.digest);
            file.seek(SeekFrom::Start(self.frontier.end))?;
            file.write_all(&frame)?;
            phase("frame_header_written")?;
            let (payload_len, mut hash) = {
                let mut hash = Sha256::new();
                hash.update(&frame[..96]);
                let mut writer = BufWriter::new(HashWriter {
                    inner: &mut file,
                    hash,
                    length: 0,
                });
                serde_json::to_writer(&mut writer, delta).map_err(json_error)?;
                writer.flush()?;
                let writer = writer.into_inner().map_err(|error| error.into_error())?;
                (writer.length, writer.hash)
            };
            hash.update(payload_len.to_le_bytes());
            let digest: [u8; 32] = hash.finalize().into();
            file.write_all(&digest)?;
            let end = self
                .frontier
                .end
                .checked_add(FRAME_HEADER_LEN as u64)
                .and_then(|end| end.checked_add(payload_len))
                .and_then(|end| end.checked_add(CHECKSUM_LEN))
                .ok_or_else(|| invalid("journal committed end overflow"))?;
            frame[96..104].copy_from_slice(&payload_len.to_le_bytes());
            file.seek(SeekFrom::Start(self.frontier.end))?;
            file.write_all(&frame)?;
            phase("frame_written")?;
            file.sync_all()?;
            phase("journal_synced")?;
            Ok(Frontier {
                end,
                sequence,
                digest,
                ..self.frontier.clone()
            })
        })();
        let next = result.map_err(|error| at(&path, "append journal", error))?;
        publish_frontier(current, &next)?;
        self.frontier = next;
        Ok(())
    }

    /// A periodic full checkpoint retains all state. Its cost is amortized by
    /// journal bytes relative to checkpoint size, not a fixed small update count.
    pub(super) fn checkpoint_due(&self) -> bool {
        #[cfg(test)]
        if self.force_checkpoint {
            return true;
        }
        self.frontier.end - JOURNAL_HEADER_LEN as u64
            >= self
                .frontier
                .checkpoint
                .encoded_len
                .max(MIN_CHECKPOINT_BYTES)
    }

    #[cfg(test)]
    pub(super) fn force_checkpoint_due_for_test(&mut self) {
        self.force_checkpoint = true;
    }

    pub(super) fn checkpoint(
        &mut self,
        current: &Path,
        snapshot: &ConsensusSnapshot,
    ) -> io::Result<()> {
        self.verify_current(current)?;
        let next = create_generation(current, snapshot, self.frontier.sequence)?;
        publish_frontier(current, &next)?;
        let old = std::mem::replace(&mut self.frontier, next);
        #[cfg(test)]
        {
            self.force_checkpoint = false;
        }
        // Publication already succeeded. Cleanup cannot turn it into a failed
        // business write; retain unknown entries and report cleanup separately.
        if let Err(error) = cleanup_generation(current, &old.generation) {
            tracing::warn!(%error, "retained unreachable consensus generation after checkpoint");
        }
        Ok(())
    }

    fn verify_current(&self, current: &Path) -> io::Result<()> {
        if Frontier::read(current)? != self.frontier {
            return Err(at(
                current,
                "validate frontier",
                invalid("CURRENT changed outside this writer; reopen required"),
            ));
        }
        Ok(())
    }
}

fn create_generation(
    current: &Path,
    snapshot: &ConsensusSnapshot,
    sequence: u64,
) -> io::Result<Frontier> {
    let root = parent(current)?;
    let staged = logex_fs::StagedDirectory::new_in(root, GENERATION_PREFIX)
        .map_err(|error| at(root, "create generation", error))?;
    let name = staged
        .path()
        .file_name()
        .and_then(|name| name.to_str())
        .and_then(|name| name.strip_prefix(GENERATION_PREFIX))
        .ok_or_else(|| invalid("owned consensus generation has an invalid name"))?;
    if name.len() != 32 {
        return Err(invalid(
            "owned consensus generation has an invalid identifier length",
        ));
    }
    let mut generation = [0; 16];
    for (byte, pair) in generation
        .iter_mut()
        .zip(name.as_bytes().as_chunks::<2>().0)
    {
        *byte = u8::from_str_radix(
            std::str::from_utf8(pair).map_err(|_| invalid("invalid generation name"))?,
            16,
        )
        .map_err(|_| invalid("invalid generation name"))?;
    }
    let checkpoint_path = staged.path().join(CHECKPOINT_FILE);
    let result = (|| {
        let mut checkpoint = logex_fs::StagedFile::new_in(staged.path(), ".checkpoint-")?;
        let integrity = snapshot_format::write_with_integrity(checkpoint.as_file_mut(), snapshot)?;
        checkpoint.as_file().sync_all()?;
        phase("checkpoint_synced")?;
        checkpoint.persist(&checkpoint_path)?;
        let mut frontier = Frontier {
            generation,
            checkpoint_sequence: sequence,
            checkpoint: integrity,
            end: JOURNAL_HEADER_LEN as u64,
            sequence,
            digest: [0; 32],
        };
        frontier.digest = frontier.base_digest();
        let mut journal = logex_fs::StagedFile::new_in(staged.path(), ".journal-")?;
        journal
            .as_file_mut()
            .write_all(&frontier.journal_header())?;
        journal.as_file().sync_all()?;
        journal.persist(&staged.path().join(JOURNAL_FILE))?;
        File::open(staged.path())?.sync_all()?;
        File::open(root)?.sync_all()?;
        phase("generation_synced")?;
        Ok(frontier)
    })();
    let frontier = result.map_err(|error| at(staged.path(), "publish generation", error))?;
    staged.keep();
    Ok(frontier)
}

fn publish_frontier(current: &Path, frontier: &Frontier) -> io::Result<()> {
    let result = (|| {
        let mut staged = logex_fs::StagedFile::new_in(parent(current)?, ".current-")?;
        staged.as_file_mut().write_all(&frontier.encode())?;
        staged.as_file().sync_all()?;
        phase("current_synced")?;
        staged.persist(current)?;
        phase("current_renamed")?;
        File::open(parent(current)?)?.sync_all()?;
        phase("current_directory_synced")
    })();
    result.map_err(|error| at(current, "publish frontier", error))
}

fn cleanup_generation(current: &Path, generation: &[u8; 16]) -> io::Result<()> {
    phase("cleanup_generation")?;
    let path = generation_path(current, generation)?;
    for name in [CHECKPOINT_FILE, JOURNAL_FILE] {
        match fs::remove_file(path.join(name)) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(at(&path, "remove retired generation file", error)),
        }
    }
    fs::remove_dir(&path).map_err(|error| at(&path, "remove empty retired generation", error))?;
    File::open(parent(current)?)?.sync_all()
}

struct HashReader<R> {
    inner: R,
    hash: Sha256,
}

impl<R: Read> Read for HashReader<R> {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        let count = self.inner.read(bytes)?;
        self.hash.update(&bytes[..count]);
        Ok(count)
    }
}

struct HashWriter<W> {
    inner: W,
    hash: Sha256,
    length: u64,
}

impl<W: Write> Write for HashWriter<W> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let count = self.inner.write(bytes)?;
        self.length = self
            .length
            .checked_add(count as u64)
            .ok_or_else(|| invalid("journal payload length overflow"))?;
        self.hash.update(&bytes[..count]);
        Ok(count)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

#[cfg(test)]
thread_local! {
    static FAIL_PHASE: std::cell::RefCell<Option<&'static str>> = const { std::cell::RefCell::new(None) };
}

fn phase(_name: &'static str) -> io::Result<()> {
    #[cfg(test)]
    if FAIL_PHASE.with_borrow_mut(|phase| {
        if *phase == Some(_name) {
            *phase = None;
            true
        } else {
            false
        }
    }) {
        return Err(io::Error::other(format!(
            "injected consensus journal failure at {_name}"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::B256;
    use logex_types::{ChainAnchors, ConsensusLightClientStatus, WeakSubjectivityCheckpoint};
    use serde::Deserialize;
    use tempfile::TempDir;

    #[derive(Clone, Debug, Serialize, Deserialize)]
    enum Delta {
        Slot(u64),
    }

    fn snapshot() -> ConsensusSnapshot {
        ConsensusSnapshot {
            checkpoint: WeakSubjectivityCheckpoint {
                beacon_root: B256::repeat_byte(1),
                beacon_slot: None,
            },
            anchors: ChainAnchors::default(),
            ordered_anchors: Vec::new(),
            anchor_gap_count: 0,
            light_client: ConsensusLightClientStatus::default(),
            light_client_payloads: Default::default(),
            verified_light_client_store: None,
        }
    }

    fn apply(snapshot: &mut ConsensusSnapshot, delta: Delta) -> io::Result<()> {
        match delta {
            Delta::Slot(u64::MAX) => return Err(invalid("fixture rejects invalid slot")),
            Delta::Slot(slot) => snapshot.checkpoint.beacon_slot = Some(slot),
        }
        Ok(())
    }

    fn fixture() -> (TempDir, PathBuf, Journal) {
        let tmp = TempDir::new().unwrap();
        let state = tmp.path().join("state");
        fs::create_dir(&state).unwrap();
        let current = state.join("CURRENT");
        let journal = Journal::create(&current, &snapshot()).unwrap();
        (tmp, current, journal)
    }

    fn reopen(current: &Path) -> io::Result<(Journal, ConsensusSnapshot)> {
        Journal::open(current, Ok, apply)
    }

    fn journal_path(current: &Path, journal: &Journal) -> PathBuf {
        generation_path(current, &journal.frontier.generation)
            .unwrap()
            .join(JOURNAL_FILE)
    }

    #[test]
    fn journal_replay_checkpoint_and_following_append_preserve_state() {
        let (_tmp, current, mut journal) = fixture();
        assert_eq!(reopen(&current).unwrap().1, snapshot());
        journal.append(&current, &Delta::Slot(1)).unwrap();
        journal.append(&current, &Delta::Slot(2)).unwrap();
        let mut expected = snapshot();
        expected.checkpoint.beacon_slot = Some(2);
        assert_eq!(reopen(&current).unwrap().1, expected);
        let old = generation_path(&current, &journal.frontier.generation).unwrap();
        journal.checkpoint(&current, &expected).unwrap();
        assert!(!old.exists());
        assert_eq!(journal.frontier.checkpoint_sequence, 2);
        assert_eq!(journal.frontier.end, JOURNAL_HEADER_LEN as u64);
        journal.append(&current, &Delta::Slot(3)).unwrap();
        expected.checkpoint.beacon_slot = Some(3);
        assert_eq!(reopen(&current).unwrap().1, expected);
    }

    #[test]
    fn uncommitted_suffix_is_ignored_read_only_then_retired_before_append() {
        let (_tmp, current, mut journal) = fixture();
        let old_current = fs::read(&current).unwrap();
        journal.append(&current, &Delta::Slot(99)).unwrap();
        let path = journal_path(&current, &journal);
        let tail_bytes = fs::read(&path).unwrap();
        fs::write(&current, old_current).unwrap();
        let (mut recovered, state) = reopen(&current).unwrap();
        assert_eq!(state, snapshot());
        assert_eq!(
            fs::read(&path).unwrap(),
            tail_bytes,
            "open must not mutate storage"
        );
        recovered.append(&current, &Delta::Slot(7)).unwrap();
        assert_eq!(recovered.frontier.sequence, 1);
        assert_eq!(reopen(&current).unwrap().1.checkpoint.beacon_slot, Some(7));
        assert_eq!(fs::metadata(path).unwrap().len(), recovered.frontier.end);
    }

    #[test]
    fn committed_damage_never_falls_back_to_an_earlier_frame() {
        let (_tmp, current, mut journal) = fixture();
        journal.append(&current, &Delta::Slot(1)).unwrap();
        let first_end = journal.frontier.end as usize;
        journal.append(&current, &Delta::Slot(2)).unwrap();
        let path = journal_path(&current, &journal);
        let original = fs::read(&path).unwrap();
        for length in [0, JOURNAL_HEADER_LEN - 1, first_end, original.len() - 1] {
            fs::write(&path, &original[..length]).unwrap();
            assert!(
                reopen(&current).is_err(),
                "truncation at {length} must fail"
            );
        }
        for offset in [
            0,
            8,
            24,
            40,
            72,
            JOURNAL_HEADER_LEN,
            JOURNAL_HEADER_LEN + 8,
            JOURNAL_HEADER_LEN + 24,
            JOURNAL_HEADER_LEN + 32,
            JOURNAL_HEADER_LEN + 64,
            JOURNAL_HEADER_LEN + 96,
            JOURNAL_HEADER_LEN + FRAME_HEADER_LEN,
            first_end - 1,
            original.len() - 1,
        ] {
            let mut bytes = original.clone();
            bytes[offset] ^= 1;
            fs::write(&path, bytes).unwrap();
            assert!(
                reopen(&current).is_err(),
                "corruption at {offset} must fail"
            );
        }
        let mut bytes = original;
        bytes[JOURNAL_HEADER_LEN + 96..JOURNAL_HEADER_LEN + 104]
            .copy_from_slice(&u64::MAX.to_le_bytes());
        fs::write(path, bytes).unwrap();
        assert!(
            reopen(&current)
                .unwrap_err()
                .to_string()
                .contains("boundary")
        );
    }

    #[test]
    fn missing_or_corrupt_frontier_never_selects_an_old_generation() {
        let (_tmp, current, mut journal) = fixture();
        let old_generation = generation_path(&current, &journal.frontier.generation).unwrap();
        FAIL_PHASE.with_borrow_mut(|phase| *phase = Some("cleanup_generation"));
        journal.checkpoint(&current, &snapshot()).unwrap();
        assert!(old_generation.exists());
        let original = fs::read(&current).unwrap();
        for offset in [0, 8, 24, 32, 40, 72, 80, 88, 120] {
            let mut bytes = original.clone();
            bytes[offset] ^= 1;
            fs::write(&current, bytes).unwrap();
            assert!(reopen(&current).is_err());
        }
        fs::write(&current, &original[..CURRENT_LEN - 1]).unwrap();
        assert!(reopen(&current).is_err());
        let mut extra = original;
        extra.push(0);
        fs::write(&current, extra).unwrap();
        assert!(reopen(&current).is_err());
        fs::remove_file(&current).unwrap();
        assert!(reopen(&current).is_err());
    }

    #[test]
    fn wrong_checkpoint_and_semantically_invalid_delta_are_rejected() {
        let (_tmp, current, mut journal) = fixture();
        let checkpoint_path = generation_path(&current, &journal.frontier.generation)
            .unwrap()
            .join(CHECKPOINT_FILE);
        let original = fs::read(&checkpoint_path).unwrap();
        let mut different = snapshot();
        different.checkpoint.beacon_slot = Some(77);
        snapshot_format::write(File::create(&checkpoint_path).unwrap(), &different).unwrap();
        assert!(
            reopen(&current)
                .unwrap_err()
                .to_string()
                .contains("differs from CURRENT")
        );
        fs::write(checkpoint_path, original).unwrap();
        journal.append(&current, &Delta::Slot(u64::MAX)).unwrap();
        assert!(
            reopen(&current)
                .unwrap_err()
                .to_string()
                .contains("fixture rejects")
        );
    }

    #[test]
    fn append_faults_keep_old_frontier_until_complete_publication() {
        for failure in [
            "frame_header_written",
            "frame_written",
            "journal_synced",
            "current_synced",
            "current_renamed",
            "current_directory_synced",
        ] {
            let (_tmp, current, mut journal) = fixture();
            let before = journal.frontier.clone();
            FAIL_PHASE.with_borrow_mut(|phase| *phase = Some(failure));
            assert!(journal.append(&current, &Delta::Slot(5)).is_err());
            assert_eq!(
                journal.frontier, before,
                "caller must not observe uncertain metadata"
            );
            let recovered = reopen(&current).unwrap().1;
            let expected = if matches!(failure, "current_renamed" | "current_directory_synced") {
                Some(5)
            } else {
                None
            };
            assert_eq!(
                recovered.checkpoint.beacon_slot, expected,
                "phase {failure}"
            );
        }
    }

    #[test]
    fn checkpoint_faults_preserve_old_generation_until_new_frontier_is_durable() {
        for failure in [
            "checkpoint_synced",
            "generation_synced",
            "current_synced",
            "current_renamed",
            "current_directory_synced",
        ] {
            let (_tmp, current, mut journal) = fixture();
            journal.append(&current, &Delta::Slot(5)).unwrap();
            let state = reopen(&current).unwrap().1;
            let old = generation_path(&current, &journal.frontier.generation).unwrap();
            let before = journal.frontier.clone();
            FAIL_PHASE.with_borrow_mut(|phase| *phase = Some(failure));
            assert!(journal.checkpoint(&current, &state).is_err());
            assert_eq!(journal.frontier, before);
            assert!(old.join(CHECKPOINT_FILE).exists());
            assert!(old.join(JOURNAL_FILE).exists());
            assert_eq!(reopen(&current).unwrap().1, state);
        }
    }

    #[test]
    fn cleanup_preserves_unknown_entries_and_does_not_fail_committed_checkpoint() {
        let (_tmp, current, mut journal) = fixture();
        let old = generation_path(&current, &journal.frontier.generation).unwrap();
        fs::write(old.join("unknown"), b"preserve me").unwrap();
        journal.checkpoint(&current, &snapshot()).unwrap();
        assert_eq!(fs::read(old.join("unknown")).unwrap(), b"preserve me");
        assert_eq!(reopen(&current).unwrap().1, snapshot());
    }

    #[test]
    fn append_does_not_recreate_missing_artifacts_or_follow_stale_frontier() {
        let (_tmp, current, mut journal) = fixture();
        let path = journal_path(&current, &journal);
        fs::remove_file(&path).unwrap();
        assert!(journal.append(&current, &Delta::Slot(1)).is_err());
        assert!(!path.exists());
        let state_dir = parent(&current).unwrap().to_owned();
        fs::rename(&state_dir, state_dir.with_extension("retired")).unwrap();
        assert!(journal.append(&current, &Delta::Slot(1)).is_err());
        assert!(!state_dir.exists());

        let (_tmp, current, mut stale) = fixture();
        let (mut other, _) = reopen(&current).unwrap();
        other.append(&current, &Delta::Slot(1)).unwrap();
        let bytes = fs::read(journal_path(&current, &other)).unwrap();
        assert!(stale.append(&current, &Delta::Slot(2)).is_err());
        assert_eq!(fs::read(journal_path(&current, &other)).unwrap(), bytes);
    }

    #[test]
    fn initial_generation_survives_whole_directory_publication() {
        let tmp = TempDir::new().unwrap();
        let staged = logex_fs::StagedDirectory::new_in(tmp.path(), ".state-").unwrap();
        let mut journal = Journal::create(&staged.path().join("CURRENT"), &snapshot()).unwrap();
        let original_path = staged.keep();
        let published = tmp.path().join("state");
        fs::rename(original_path, &published).unwrap();
        File::open(tmp.path()).unwrap().sync_all().unwrap();
        let current = published.join("CURRENT");
        journal.append(&current, &Delta::Slot(1)).unwrap();
        assert_eq!(reopen(&current).unwrap().1.checkpoint.beacon_slot, Some(1));
        assert!(Journal::create(&current, &snapshot()).is_err());
    }

    #[test]
    fn checkpoint_threshold_scales_with_checkpoint_size_without_update_count_limit() {
        let (_tmp, _current, mut journal) = fixture();
        journal.frontier.checkpoint.encoded_len = MIN_CHECKPOINT_BYTES * 3;
        journal.frontier.sequence = 1_000_000;
        journal.frontier.end = JOURNAL_HEADER_LEN as u64 + MIN_CHECKPOINT_BYTES * 3 - 1;
        assert!(!journal.checkpoint_due());
        journal.frontier.end += 1;
        assert!(journal.checkpoint_due());
    }

    #[test]
    fn checkpoint_restore_precedes_replay_and_can_reject_invalid_state() {
        let (_tmp, current, mut journal) = fixture();
        journal.append(&current, &Delta::Slot(7)).unwrap();
        let restored = std::cell::Cell::new(false);
        let (_, result) = Journal::open::<Delta>(
            &current,
            |mut state| {
                restored.set(true);
                state.anchor_gap_count = 4;
                Ok(state)
            },
            |state, delta| {
                assert!(restored.get());
                assert_eq!(state.anchor_gap_count, 4);
                apply(state, delta)
            },
        )
        .unwrap();
        assert_eq!(result.checkpoint.beacon_slot, Some(7));
        let error = Journal::open::<Delta>(
            &current,
            |_| Err(invalid("fixture rejected checkpoint")),
            |_, _| panic!("invalid checkpoint must not reach replay"),
        )
        .unwrap_err();
        assert!(error.to_string().contains("fixture rejected checkpoint"));
    }
    #[test]
    fn failed_initial_create_keeps_partial_owned_generation_unpublished() {
        let tmp = TempDir::new().unwrap();
        let current = tmp.path().join("CURRENT");
        FAIL_PHASE.with_borrow_mut(|phase| *phase = Some("generation_synced"));
        assert!(Journal::create(&current, &snapshot()).is_err());
        assert!(!current.exists());
        let entries = fs::read_dir(tmp.path())
            .unwrap()
            .collect::<io::Result<Vec<_>>>()
            .unwrap();
        assert_eq!(entries.len(), 1);
        let orphan = entries[0].path();
        assert!(orphan.join(CHECKPOINT_FILE).exists());
        assert!(orphan.join(JOURNAL_FILE).exists());
        assert!(reopen(&current).is_err());
        assert!(Journal::create(&current, &snapshot()).is_err());
        assert!(orphan.join(CHECKPOINT_FILE).exists());
    }

    #[test]
    fn partial_uncommitted_suffix_and_failed_truncation_leave_committed_prefix_intact() {
        for suffix in [&[0x7f][..], &FRAME_MAGIC[..5]] {
            let (_tmp, current, journal) = fixture();
            let path = journal_path(&current, &journal);
            let mut file = OpenOptions::new().append(true).open(&path).unwrap();
            file.write_all(suffix).unwrap();
            file.sync_all().unwrap();
            let before = fs::read(&path).unwrap();
            let (mut recovered, state) = reopen(&current).unwrap();
            assert_eq!(state, snapshot());
            assert_eq!(fs::read(&path).unwrap(), before);
            FAIL_PHASE.with_borrow_mut(|phase| *phase = Some("tail_truncated"));
            assert!(recovered.append(&current, &Delta::Slot(9)).is_err());
            assert_eq!(fs::metadata(&path).unwrap().len(), journal.frontier.end);
            let (mut reopened, state) = reopen(&current).unwrap();
            assert_eq!(state, snapshot());
            reopened.append(&current, &Delta::Slot(8)).unwrap();
            assert_eq!(reopen(&current).unwrap().1.checkpoint.beacon_slot, Some(8));
        }
    }

    #[test]
    fn checksummed_frontier_inconsistencies_fail_binding_and_exact_end_validation() {
        let (_tmp, current, mut journal) = fixture();
        journal.append(&current, &Delta::Slot(1)).unwrap();
        let first_end = journal.frontier.end;
        journal.append(&current, &Delta::Slot(2)).unwrap();
        let original = journal.frontier.clone();
        for case in 0..9 {
            let mut damaged = original.clone();
            match case {
                0 => damaged.end = JOURNAL_HEADER_LEN as u64 + 1,
                1 => damaged.end -= 1,
                2 => damaged.end += 1,
                3 => damaged.sequence += 1,
                4 => damaged.digest[0] ^= 1,
                5 => damaged.checkpoint.payload_digest[0] ^= 1,
                6 => damaged.checkpoint_sequence = 1,
                7 => damaged.end = first_end,
                8 => damaged.checkpoint.encoded_len += 1,
                _ => unreachable!(),
            }
            fs::write(&current, damaged.encode()).unwrap();
            assert!(
                reopen(&current).is_err(),
                "checksummed inconsistent frontier case {case}"
            );
        }
    }

    #[test]
    fn missing_checkpoint_is_an_error_even_with_a_complete_journal() {
        let (_tmp, current, mut journal) = fixture();
        journal.append(&current, &Delta::Slot(3)).unwrap();
        let checkpoint = generation_path(&current, &journal.frontier.generation)
            .unwrap()
            .join(CHECKPOINT_FILE);
        fs::remove_file(&checkpoint).unwrap();
        assert!(reopen(&current).is_err());
        assert!(!checkpoint.exists());
    }
}
