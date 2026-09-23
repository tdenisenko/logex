use crate::commitment::{Commitment, PrefixState};
use crate::durability;
use std::fs::{self, File, OpenOptions, TryLockError};
use std::io::{self, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::thread;

use crate::native::SegmentKind;
use logex_types::LogRow;

pub(crate) const SOURCE_MARKER_FILE: &str = ".source-publication";
const SOURCE_MARKER_MAGIC: &[u8; 8] = b"LXSRC002";
const SOURCE_MARKER_BYTES: usize = 87;
const SOURCE_UPDATING: u8 = 1;
const SOURCE_PREFIX_REWRITE: u8 = 2;
const SOURCE_COMMITTED: u8 = 3;
const CANONICAL_ENVELOPE_MAGIC: &[u8; 8] = b"LXCAN001";
const CANONICAL_ENVELOPE_VERSION: u8 = 4;
const CANONICAL_PENDING: u8 = 1;
const CANONICAL_COMMITTED: u8 = 2;
const CANONICAL_ENVELOPE_HEADER: usize = 136;
pub(crate) const CANONICAL_PREFIX_BYTES: usize = CANONICAL_ENVELOPE_HEADER + 8;

#[cfg(test)]
thread_local! {
    static BEFORE_ABSENT_APPEND_CREATE: std::cell::RefCell<Option<Box<dyn FnOnce()>>> = const { std::cell::RefCell::new(None) };
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SourceMarker {
    commitment: Option<Commitment>,
    state: u8,
    namespace: [u8; 16],
    prefix_rows: u64,
    generation: u64,
    segment_id: u64,
    kind: SegmentKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SourceIdentity {
    pub(crate) namespace: [u8; 16],
    pub(crate) generation: u64,
    pub(crate) segment_id: u64,
    pub(crate) kind: SegmentKind,
}

impl SourceMarker {
    fn with_commitment(mut self, commitment: Option<Commitment>) -> Self {
        self.commitment = commitment;
        self
    }
    fn new(identity: SourceIdentity, state: u8, prefix_rows: u64) -> Self {
        Self {
            commitment: None,
            state,
            namespace: identity.namespace,
            prefix_rows,
            generation: identity.generation,
            segment_id: identity.segment_id,
            kind: identity.kind,
        }
    }
}
pub(crate) struct SourceWriteGuard {
    file: File,
    dir: std::path::PathBuf,
    binding: Option<SourceBinding>,
}

pub(crate) struct PrefixRecoveryGuard {
    _owner: SourceWriteGuard,
    dir: std::path::PathBuf,
    namespace: [u8; 16],
    prefix_rows: u64,
    commitment: Option<Commitment>,
    generation: u64,
    segment_id: u64,
    kind: SegmentKind,
    pending: bool,
    completed: std::cell::Cell<bool>,
}

impl PrefixRecoveryGuard {
    pub(crate) fn namespace(&self) -> [u8; 16] {
        self.namespace
    }

    pub(crate) fn commitment(&self) -> Option<Commitment> {
        self.commitment
    }

    pub(crate) fn prefix_rows(&self) -> u64 {
        self.prefix_rows
    }

    pub(crate) fn generation(&self) -> u64 {
        self.generation
    }
    pub(crate) fn segment_id(&self) -> u64 {
        self.segment_id
    }
    pub(crate) fn pending(&self) -> bool {
        self.pending
    }
    pub(crate) fn dir(&self) -> &Path {
        &self.dir
    }
}

pub(crate) fn verify_prefix_recovery_pending(
    dir: &Path,
    owner: &PrefixRecoveryGuard,
) -> io::Result<()> {
    if owner.dir != dir {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "prefix recovery capability belongs to another segment",
        ));
    }
    if !owner.pending && !owner.completed.get() {
        return verify_owned_source(dir, owner.namespace, owner.generation, owner.segment_id)
            .map(|_| ());
    }
    match read_source_marker(dir)? {
        Some(marker)
            if marker.state == SOURCE_PREFIX_REWRITE
                && marker.namespace == owner.namespace
                && marker.prefix_rows == owner.prefix_rows
                && marker.generation == owner.generation
                && marker.segment_id == owner.segment_id
                && marker.kind == owner.kind
                && marker.commitment == owner.commitment =>
        {
            Ok(())
        }
        _ => Err(io::Error::new(
            io::ErrorKind::WouldBlock,
            "verified prefix recovery marker changed during capture",
        )),
    }
}

impl SourceWriteGuard {
    // Directory creation belongs only to full initialization and explicit
    // empty-prefix recovery, never to an existing-source mutation or check.
    fn create_and_acquire(dir: &Path) -> io::Result<Self> {
        fs::create_dir_all(dir)?;
        Self::acquire_existing(dir)
    }

    fn acquire_existing(dir: &Path) -> io::Result<Self> {
        // Use the segment directory inode, matching native maintenance
        // ownership without creating another persistent lock namespace.
        let file = File::open(dir)?;
        if !file.metadata()?.is_dir() {
            return Err(io::Error::new(
                io::ErrorKind::NotADirectory,
                "source path is not a directory",
            ));
        }
        file.try_lock().map_err(|error| match error {
            TryLockError::WouldBlock => io::Error::new(
                io::ErrorKind::WouldBlock,
                "raw source publication is already in progress",
            ),
            TryLockError::Error(error) => error,
        })?;
        Ok(Self {
            file,
            dir: dir.to_path_buf(),
            binding: None,
        })
    }

    pub(crate) fn acquire_bound(
        dir: &Path,
        namespace: [u8; 16],
        generation: u64,
        segment_id: u64,
    ) -> io::Result<Self> {
        let mut owner = Self::acquire_existing(dir)?;
        owner.binding = Some(verify_owned_source(dir, namespace, generation, segment_id)?);
        Ok(owner)
    }

    pub(crate) fn acquire_legacy(dir: &Path) -> io::Result<Self> {
        Self::acquire_existing(dir)?.with_legacy_binding()
    }

    pub(crate) fn acquire_legacy_prefix(dir: &Path, prefix_rows: u64) -> io::Result<Self> {
        Self::acquire_prefix(dir, prefix_rows)?.with_legacy_binding()
    }

    fn acquire_prefix(dir: &Path, prefix_rows: u64) -> io::Result<Self> {
        if prefix_rows == 0 {
            // Only an authoritative empty prefix can restore an absent source.
            Self::create_and_acquire(dir)
        } else {
            Self::acquire_existing(dir)
        }
    }

    fn with_legacy_binding(mut self) -> io::Result<Self> {
        // Legacy native sources remain scan-readable but cannot acquire a
        // trusted identity from their shape or an incidental standalone marker.
        self.binding = read_source_binding(&self.dir)?;
        Ok(self)
    }
}

pub(crate) fn verify_owned_source(
    dir: &Path,
    namespace: [u8; 16],
    generation: u64,
    segment_id: u64,
) -> io::Result<SourceBinding> {
    match read_source_marker(dir)? {
        Some(marker)
            if marker.state == SOURCE_COMMITTED
                && marker.namespace == namespace
                && marker.generation == generation
                && marker.segment_id == segment_id =>
        {
            Ok(SourceBinding {
                namespace,
                generation,
                segment_id,
            })
        }
        Some(SourceMarker {
            state: SOURCE_COMMITTED,
            ..
        }) => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "raw source binding differs from native catalog",
        )),
        Some(_) => Err(io::Error::new(
            io::ErrorKind::WouldBlock,
            "raw source replacement is incomplete; verified recovery is required",
        )),
        None => Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "raw source identity is missing",
        )),
    }
}

impl Drop for SourceWriteGuard {
    fn drop(&mut self) {
        if let Err(error) = self.file.unlock() {
            tracing::warn!(%error, "failed to release raw source publication lock");
        }
    }
}

fn read_source_marker(dir: &Path) -> io::Result<Option<SourceMarker>> {
    let path = dir.join(SOURCE_MARKER_FILE);
    let mut file = match File::open(&path) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            match fs::symlink_metadata(&path) {
                Ok(_) => return Err(error),
                Err(metadata_error) if metadata_error.kind() == io::ErrorKind::NotFound => {
                    return Ok(None);
                }
                Err(metadata_error) => return Err(metadata_error),
            }
        }
        Err(error) => return Err(error),
    };
    // Marker publication always replaces an immutable inode. Checking length
    // on this opened handle therefore describes the same bytes read below.
    let marker_len = file.metadata()?.len();
    if marker_len != 54 && marker_len != SOURCE_MARKER_BYTES as u64 {
        return Err(io::Error::new(
            if marker_len < SOURCE_MARKER_BYTES as u64 {
                io::ErrorKind::UnexpectedEof
            } else {
                io::ErrorKind::InvalidData
            },
            "invalid raw source marker length",
        ));
    }
    let mut bytes = vec![0; marker_len as usize];
    file.read_exact(&mut bytes)?;
    let old = marker_len == 54;
    let magic = if old {
        b"LXSRC001"
    } else {
        SOURCE_MARKER_MAGIC
    };
    let payload = bytes.len() - 4;
    if bytes.get(..8) != Some(magic.as_slice())
        || crc32fast::hash(&bytes[..payload]).to_le_bytes() != bytes[payload..]
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid raw source publication marker",
        ));
    }
    let commitment = if old {
        None
    } else {
        match bytes[50] {
            0 if bytes[51..83] == [0; 32] => None,
            1 => Some(Commitment::from_slice(&bytes[51..83])),
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "invalid prefix commitment",
                ));
            }
        }
    };
    if !matches!(
        bytes[8],
        SOURCE_UPDATING | SOURCE_PREFIX_REWRITE | SOURCE_COMMITTED
    ) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid raw source publication state",
        ));
    }
    let namespace = bytes[9..25].try_into().map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid source namespace length",
        )
    })?;
    let prefix_rows =
        u64::from_le_bytes(bytes[25..33].try_into().map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidData, "invalid source prefix length")
        })?);
    let generation =
        u64::from_le_bytes(bytes[33..41].try_into().map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidData, "invalid source generation")
        })?);
    let segment_id = u64::from_le_bytes(bytes[41..49].try_into().map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid source segment identity",
        )
    })?);
    let kind = match bytes[49] {
        0 => SegmentKind::Hot,
        1 => SegmentKind::Sealed,
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid source segment kind",
            ));
        }
    };
    Ok(Some(SourceMarker {
        commitment,
        state: bytes[8],
        namespace,
        prefix_rows,
        generation,
        segment_id,
        kind,
    }))
}

#[cfg(test)]
pub(crate) fn read_source_namespace(dir: &Path) -> io::Result<Option<[u8; 16]>> {
    Ok(read_source_binding(dir)?.map(|binding| binding.namespace))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SourceBinding {
    pub(crate) namespace: [u8; 16],
    pub(crate) generation: u64,
    pub(crate) segment_id: u64,
}

impl From<SourceIdentity> for SourceBinding {
    fn from(identity: SourceIdentity) -> Self {
        Self {
            namespace: identity.namespace,
            generation: identity.generation,
            segment_id: identity.segment_id,
        }
    }
}

/// Raw canonical metadata is separate from the generic and bundled bitmap formats.
/// Header CRC protects publication metadata and the expected payload CRC. The
/// latter covers exact serialized bitmap and restart-state bytes; metadata-only
/// capture deliberately defers that full-payload check until consumption.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RawCanonicalMetadata {
    pub(crate) previous: Option<(u64, Commitment)>,
    pub(crate) commitment: Option<Commitment>,
    binding: Option<SourceBinding>,
    pending: bool,
    pub(crate) rows: u64,
    offset: usize,
    state_len: u32,
    payload_checksum: Option<u32>,
}

impl RawCanonicalMetadata {
    pub(crate) fn parse(prefix: &[u8], file_len: u64) -> io::Result<Self> {
        let invalid = || {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid canonical source envelope or bitmap length",
            )
        };
        let read_u64 = |at: usize| -> io::Result<u64> {
            Ok(u64::from_le_bytes(
                prefix
                    .get(at..at + 8)
                    .ok_or_else(invalid)?
                    .try_into()
                    .map_err(|_| invalid())?,
            ))
        };
        if prefix.get(..8) != Some(CANONICAL_ENVELOPE_MAGIC.as_slice()) {
            let rows = read_u64(0)?;
            if file_len < 8 + rows.div_ceil(8) {
                return Err(invalid());
            }
            return Ok(Self {
                previous: None,
                commitment: None,
                binding: None,
                pending: false,
                rows,
                offset: 0,
                state_len: 0,
                payload_checksum: None,
            });
        }
        // A reserved magic prefix is always an envelope; corruption never falls
        // back to interpreting those bytes as a legacy row count.
        if let Some(&version) = prefix.get(8)
            && version != CANONICAL_ENVELOPE_VERSION
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "unsupported canonical envelope version {version}; expected {CANONICAL_ENVELOPE_VERSION}; original data preserved, no automatic migration"
                ),
            ));
        }
        let header = CANONICAL_ENVELOPE_HEADER;
        if prefix.get(8) != Some(&CANONICAL_ENVELOPE_VERSION)
            || prefix.len() < header + 8
            || !matches!(prefix[9], CANONICAL_PENDING | CANONICAL_COMMITTED)
            || crc32fast::hash(&prefix[..header - 4]).to_le_bytes() != prefix[header - 4..header]
        {
            return Err(invalid());
        }
        let commitment = match prefix[50] {
            0 if prefix[51..83] == [0; 32] => None,
            1 => Some(Commitment::from_slice(&prefix[51..83])),
            _ => return Err(invalid()),
        };
        let previous = match prefix[83] {
            0 if prefix[84..124] == [0; 40] => None,
            1 if commitment.is_some() => {
                Some((read_u64(84)?, Commitment::from_slice(&prefix[92..124])))
            }
            _ => return Err(invalid()),
        };
        let state_len = u32::from_le_bytes(prefix[124..128].try_into().map_err(|_| invalid())?);
        if state_len as usize > PrefixState::MAX_ENCODED_BYTES
            || (state_len != 0 && commitment.is_none())
        {
            return Err(invalid());
        }
        let rows = read_u64(42)?;
        if previous.is_some_and(|(boundary, _)| boundary >= rows) {
            return Err(invalid());
        }
        if rows > u64::from(u32::MAX)
            || read_u64(header)? != rows
            || file_len != (header + 8) as u64 + rows.div_ceil(8) + u64::from(state_len)
        {
            return Err(invalid());
        }
        Ok(Self {
            previous,
            commitment,
            binding: Some(SourceBinding {
                namespace: prefix[10..26].try_into().map_err(|_| invalid())?,
                generation: read_u64(26)?,
                segment_id: read_u64(34)?,
            }),
            pending: prefix[9] == CANONICAL_PENDING,
            rows,
            offset: header,
            state_len,
            payload_checksum: Some(u32::from_le_bytes(
                prefix[128..132].try_into().map_err(|_| invalid())?,
            )),
        })
    }

    pub(crate) fn validate_commitment(self, expected: Option<Commitment>) -> io::Result<Self> {
        if expected.is_some() && self.commitment != expected {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "canonical logical prefix commitment differs from captured publication",
            ));
        }
        Ok(self)
    }

    /// A captured manifest can precede the one canonical append publication.
    /// Writers use validate_commitment instead and require the physical boundary.
    pub(crate) fn validate_capture_commitment(
        self,
        expected: Option<Commitment>,
        rows: u64,
    ) -> io::Result<Self> {
        if let Some(expected) = expected
            && !((self.rows == rows && self.commitment == Some(expected))
                || self.previous == Some((rows, expected)))
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "canonical logical prefix differs from captured manifest",
            ));
        }
        Ok(self)
    }

    pub(crate) fn validate_exact_len(self, file_len: u64) -> io::Result<Self> {
        if file_len != self.offset as u64 + 8 + self.rows.div_ceil(8) + u64::from(self.state_len) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "canonical bitmap physical length differs from its rows",
            ));
        }
        Ok(self)
    }

    pub(crate) fn validate_committed(
        self,
        expected: Option<SourceBinding>,
        rows: u64,
    ) -> io::Result<Self> {
        if expected.is_some() && self.binding != expected {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "canonical source identity differs from manifest or is missing",
            ));
        }
        if self.pending {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "canonical source replacement is incomplete",
            ));
        }
        self.validate_rows(rows)?;
        Ok(self)
    }

    pub(crate) fn validate_recovery(self, owner: &PrefixRecoveryGuard) -> io::Result<Self> {
        verify_prefix_recovery_pending(owner.dir(), owner)?;
        if self.binding
            != Some(SourceBinding {
                namespace: owner.namespace,
                generation: owner.generation,
                segment_id: owner.segment_id,
            })
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "canonical identity differs from verified prefix recovery",
            ));
        }
        if self.pending
            && (!owner.pending
                || self.rows != owner.prefix_rows
                || self.commitment != owner.commitment)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "pending canonical prefix differs from recovery boundary",
            ));
        }
        // Both states are valid: a crash may follow canonical-last publication
        // while the sidecar still records the unfinished catalog transaction.
        self.validate_rows(owner.prefix_rows)?;
        Ok(self)
    }

    fn validate_rows(self, rows: u64) -> io::Result<()> {
        if self.rows < rows {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "canonical bitmap does not cover captured rows",
            ));
        }
        Ok(())
    }

    fn validate_payload_bytes(self, bytes: &[u8]) -> io::Result<()> {
        if Self::parse(bytes, bytes.len() as u64)? != self {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "captured canonical metadata changed",
            ));
        }
        if let Some(expected) = self.payload_checksum {
            self.validate_exact_len(bytes.len() as u64)?;
            let payload = bytes.get(self.offset..).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "truncated canonical payload")
            })?;
            if crc32fast::hash(payload) != expected {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "canonical payload checksum mismatch",
                ));
            }
        }
        Ok(())
    }

    /// Verify the complete physical bound payload with fixed scratch space.
    /// Metadata capture remains prefix-only; maintenance calls this before any
    /// source writes. Legacy unbound bitmaps have no checksum and retain their
    /// existing structural contract. This establishes local byte integrity, not
    /// consensus authentication. The reader position is unspecified on return.
    pub(crate) fn validate_payload_reader(self, reader: &mut (impl Read + Seek)) -> io::Result<()> {
        let file_len = reader.seek(SeekFrom::End(0))?;
        reader.seek(SeekFrom::Start(0))?;
        let mut prefix = [0u8; CANONICAL_PREFIX_BYTES];
        let prefix_len =
            usize::try_from(file_len.min(CANONICAL_PREFIX_BYTES as u64)).map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "canonical prefix length overflow",
                )
            })?;
        reader.read_exact(&mut prefix[..prefix_len])?;
        if Self::parse(&prefix[..prefix_len], file_len)? != self {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "captured canonical metadata changed",
            ));
        }
        if let Some(expected) = self.payload_checksum {
            self.validate_exact_len(file_len)?;
            reader.seek(SeekFrom::Start(self.offset as u64))?;
            let mut remaining = file_len - self.offset as u64;
            let mut checksum = crc32fast::Hasher::new();
            let mut scratch = [0u8; 64 * 1024];
            while remaining != 0 {
                let count = remaining.min(scratch.len() as u64) as usize;
                reader.read_exact(&mut scratch[..count])?;
                checksum.update(&scratch[..count]);
                remaining -= count as u64;
            }
            let mut extra = [0u8; 1];
            if reader.read(&mut extra)? != 0 || checksum.finalize() != expected {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "canonical payload checksum or length mismatch",
                ));
            }
        }
        Ok(())
    }

    pub(crate) fn bitmap(self, bytes: &[u8]) -> io::Result<NullBitmap> {
        self.validate_payload_bytes(bytes)?;
        let end = self.bitmap_end()?;
        NullBitmap::read_from(bytes.get(self.offset..end).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "truncated canonical bitmap")
        })?)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "corrupt canonical bitmap"))
    }

    fn bitmap_end(self) -> io::Result<usize> {
        usize::try_from(self.rows.div_ceil(8))
            .ok()
            .and_then(|len| self.offset.checked_add(8)?.checked_add(len))
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "canonical bitmap length overflow",
                )
            })
    }

    /// Verify payload integrity before trusting the bounded restart state.
    /// Metadata-only capture does not call this full-body path.
    pub(crate) fn resume_state(self, bytes: &[u8]) -> io::Result<Option<PrefixState>> {
        self.validate_payload_bytes(bytes)?;
        match (self.binding, self.commitment, self.state_len) {
            (_, None, 0) => Ok(None),
            (Some(binding), Some(root), len) if len != 0 => {
                let state =
                    PrefixState::from_bytes(bytes.get(self.bitmap_end()?..).ok_or_else(|| {
                        io::Error::new(
                            io::ErrorKind::InvalidData,
                            "truncated canonical restart state",
                        )
                    })?)?;
                state.validate(binding.namespace, self.rows, root)?;
                Ok(Some(state))
            }
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "canonical commitment lacks restart state",
            )),
        }
    }
}

pub(crate) fn write_raw_canonical(
    writer: &mut dyn Write,
    bitmap: &NullBitmap,
    binding: Option<SourceBinding>,
    commitment: Option<Commitment>,
    prefix_state: Option<&PrefixState>,
) -> io::Result<()> {
    write_raw_canonical_with_previous(writer, bitmap, binding, commitment, prefix_state, None)
}

pub(crate) fn write_raw_canonical_with_previous(
    writer: &mut dyn Write,
    bitmap: &NullBitmap,
    binding: Option<SourceBinding>,
    commitment: Option<Commitment>,
    prefix_state: Option<&PrefixState>,
    previous: Option<(u64, Commitment)>,
) -> io::Result<()> {
    write_raw_canonical_state(
        writer,
        bitmap,
        binding,
        commitment,
        prefix_state,
        previous,
        CANONICAL_COMMITTED,
    )
}

fn write_raw_canonical_state(
    writer: &mut dyn Write,
    bitmap: &NullBitmap,
    binding: Option<SourceBinding>,
    commitment: Option<Commitment>,
    prefix_state: Option<&PrefixState>,
    previous: Option<(u64, Commitment)>,
    state: u8,
) -> io::Result<()> {
    let encoded_state = match (binding, commitment, prefix_state) {
        (Some(binding), Some(root), Some(prefix)) => {
            prefix.validate(binding.namespace, bitmap.len(), root)?;
            prefix.to_bytes()
        }
        (_, None, None) if previous.is_none() => Vec::new(),
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "restart state requires a bound commitment",
            ));
        }
    };
    if let Some(binding) = binding {
        if bitmap.len() > u64::from(u32::MAX) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "canonical rows exceed segment addressing",
            ));
        }
        let mut header = [0; CANONICAL_ENVELOPE_HEADER];
        header[..8].copy_from_slice(CANONICAL_ENVELOPE_MAGIC);
        header[8] = CANONICAL_ENVELOPE_VERSION;
        header[9] = state;
        header[10..26].copy_from_slice(&binding.namespace);
        header[26..34].copy_from_slice(&binding.generation.to_le_bytes());
        header[34..42].copy_from_slice(&binding.segment_id.to_le_bytes());
        header[42..50].copy_from_slice(&bitmap.len().to_le_bytes());
        if let Some(commitment) = commitment {
            header[50] = 1;
            header[51..83].copy_from_slice(commitment.as_slice());
        }
        if let Some((rows, root)) = previous {
            if commitment.is_none() || rows >= bitmap.len() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "invalid canonical previous prefix",
                ));
            }
            header[83] = 1;
            header[84..92].copy_from_slice(&rows.to_le_bytes());
            header[92..124].copy_from_slice(root.as_slice());
        }
        header[124..128].copy_from_slice(&(encoded_state.len() as u32).to_le_bytes());
        let mut payload_checksum = crc32fast::Hasher::new();
        payload_checksum.update(&bitmap.len().to_le_bytes());
        payload_checksum.update(&bitmap.bits);
        payload_checksum.update(&encoded_state);
        header[128..132].copy_from_slice(&payload_checksum.finalize().to_le_bytes());
        let checksum = crc32fast::hash(&header[..132]);
        header[132..136].copy_from_slice(&checksum.to_le_bytes());
        writer.write_all(&header)?;
    }
    bitmap.write_to(writer)?;
    writer.write_all(&encoded_state)
}

/// Simulate a torn publication with a readable root but missing restart state.
/// Production writers must never create this combination.
#[cfg(test)]
fn write_canonical_with_missing_restart_state(
    writer: &mut dyn Write,
    bitmap: &NullBitmap,
    binding: SourceBinding,
    commitment: Commitment,
) -> io::Result<()> {
    let mut bytes = Vec::new();
    write_raw_canonical(&mut bytes, bitmap, Some(binding), None, None)?;
    bytes[50] = 1;
    bytes[51..83].copy_from_slice(commitment.as_slice());
    let checksum = crc32fast::hash(&bytes[..132]);
    bytes[132..136].copy_from_slice(&checksum.to_le_bytes());
    writer.write_all(&bytes)
}

fn publish_pending_canonical(
    dir: &Path,
    bitmap: &NullBitmap,
    binding: SourceBinding,
    commitment: Option<Commitment>,
    prefix_state: Option<&PrefixState>,
) -> io::Result<()> {
    let mut bytes = Vec::new();
    write_raw_canonical_state(
        &mut bytes,
        bitmap,
        Some(binding),
        commitment,
        prefix_state,
        None,
        CANONICAL_PENDING,
    )?;
    durability::write_bytes_ordered(&dir.join("canonical.bitmap"), &bytes)
}

pub(crate) fn read_canonical_bitmap(
    bytes: &[u8],
    expected: Option<SourceBinding>,
) -> io::Result<NullBitmap> {
    RawCanonicalMetadata::parse(bytes, bytes.len() as u64)?
        .validate_committed(expected, 0)?
        .bitmap(bytes)
}

pub(crate) fn read_source_binding(dir: &Path) -> io::Result<Option<SourceBinding>> {
    match read_source_marker(dir)? {
        None => Ok(None),
        Some(SourceMarker {
            state: SOURCE_COMMITTED,
            namespace,
            generation,
            segment_id,
            ..
        }) => Ok(Some(SourceBinding {
            namespace,
            generation,
            segment_id,
        })),
        Some(_) => Err(io::Error::new(
            io::ErrorKind::WouldBlock,
            "raw source replacement is incomplete; verified recovery is required",
        )),
    }
}

/// Complete only a private exact-prefix rewrite whose pending marker matches
/// authoritative recovery metadata. Every old/new artifact encodes the same N
/// logical rows, so any interrupted mixture remains that verified prefix.
pub(crate) fn begin_prefix_recovery(
    dir: &Path,
    namespace: [u8; 16],
    prefix_rows: u64,
    generation: u64,
    segment_id: u64,
    kind: SegmentKind,
    commitment: Option<Commitment>,
) -> io::Result<PrefixRecoveryGuard> {
    let owner = SourceWriteGuard::acquire_prefix(dir, prefix_rows)?;
    match read_source_marker(dir)? {
        Some(marker)
            if marker.state == SOURCE_PREFIX_REWRITE
                && marker.namespace == namespace
                && marker.prefix_rows == prefix_rows
                && marker.generation == generation
                && marker.segment_id == segment_id
                && marker.kind == kind
                && marker.commitment == commitment =>
        {
            Ok(PrefixRecoveryGuard {
                _owner: owner,
                dir: dir.to_path_buf(),
                namespace,
                prefix_rows,
                commitment,
                generation,
                segment_id,
                kind,
                pending: true,
                completed: std::cell::Cell::new(false),
            })
        }
        Some(SourceMarker {
            state: SOURCE_PREFIX_REWRITE,
            ..
        }) => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "pending prefix rewrite differs from authoritative recovery metadata",
        )),
        Some(marker)
            if prefix_rows == 0
                && marker.state == SOURCE_UPDATING
                && marker.namespace == namespace
                && marker.generation == generation
                && marker.segment_id == segment_id =>
        {
            Ok(PrefixRecoveryGuard {
                _owner: owner,
                dir: dir.to_path_buf(),
                namespace,
                prefix_rows,
                commitment,
                generation,
                segment_id,
                kind,
                pending: false,
                completed: std::cell::Cell::new(false),
            })
        }
        Some(SourceMarker {
            state: SOURCE_UPDATING,
            ..
        }) => Err(io::Error::new(
            io::ErrorKind::WouldBlock,
            "arbitrary source replacement is incomplete and cannot use prefix recovery",
        )),
        Some(marker)
            if marker.state == SOURCE_COMMITTED
                && (marker.namespace != namespace
                    || marker.generation != generation
                    || marker.segment_id != segment_id) =>
        {
            Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "committed raw source namespace differs from recovery metadata",
            ))
        }
        None if prefix_rows != 0 => Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "nonempty source recovery authority is missing",
        )),
        None
        | Some(SourceMarker {
            state: SOURCE_COMMITTED,
            ..
        }) => Ok(PrefixRecoveryGuard {
            _owner: owner,
            dir: dir.to_path_buf(),
            namespace,
            prefix_rows,
            commitment,
            generation,
            segment_id,
            kind,
            pending: false,
            completed: std::cell::Cell::new(false),
        }),
        Some(_) => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid raw source recovery state",
        )),
    }
}

pub(crate) fn prefix_rewrite_pending(
    dir: &Path,
    namespace: [u8; 16],
    prefix_rows: u64,
    generation: u64,
    segment_id: u64,
    kind: SegmentKind,
    commitment: Option<Commitment>,
) -> io::Result<bool> {
    match read_source_marker(dir)? {
        Some(marker)
            if marker.state == SOURCE_PREFIX_REWRITE
                && marker.namespace == namespace
                && marker.prefix_rows == prefix_rows
                && marker.generation == generation
                && marker.segment_id == segment_id
                && marker.kind == kind
                && marker.commitment == commitment =>
        {
            Ok(true)
        }
        Some(SourceMarker {
            state: SOURCE_PREFIX_REWRITE,
            ..
        }) => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "pending prefix rewrite differs from authoritative segment metadata",
        )),
        Some(SourceMarker {
            state: SOURCE_UPDATING,
            ..
        }) => Err(io::Error::new(
            io::ErrorKind::WouldBlock,
            "arbitrary source replacement is incomplete; verified recovery is required",
        )),
        _ => Ok(false),
    }
}

fn write_source_marker(
    dir: &Path,
    marker: SourceMarker,
    publication: durability::Publication,
) -> io::Result<()> {
    let mut bytes = [0; SOURCE_MARKER_BYTES];
    bytes[..8].copy_from_slice(SOURCE_MARKER_MAGIC);
    bytes[8] = marker.state;
    bytes[9..25].copy_from_slice(&marker.namespace);
    bytes[25..33].copy_from_slice(&marker.prefix_rows.to_le_bytes());
    bytes[33..41].copy_from_slice(&marker.generation.to_le_bytes());
    bytes[41..49].copy_from_slice(&marker.segment_id.to_le_bytes());
    bytes[49] = match marker.kind {
        SegmentKind::Hot => 0,
        SegmentKind::Sealed => 1,
    };
    if let Some(commitment) = marker.commitment {
        bytes[50] = 1;
        bytes[51..83].copy_from_slice(commitment.as_slice());
    }
    let checksum = crc32fast::hash(&bytes[..83]).to_le_bytes();
    bytes[83..].copy_from_slice(&checksum);
    let path = dir.join(SOURCE_MARKER_FILE);
    if marker.state == SOURCE_COMMITTED && publication == durability::Publication::Durable {
        // The committed marker publishes the complete replacement as one tree:
        // every column payload and rename is durable before this name can be.
        return durability::publish_tree(dir, &path, &bytes);
    }
    match publication {
        durability::Publication::Deferred => durability::write_bytes_deferred(&path, &bytes),
        durability::Publication::Ordered => durability::write_bytes_ordered(&path, &bytes),
        durability::Publication::Durable => durability::write_bytes(&path, &bytes),
    }
}

#[cfg(test)]
pub(crate) fn mark_prefix_rewrite_for_test(
    dir: &Path,
    namespace: [u8; 16],
    prefix_rows: u64,
    generation: u64,
    segment_id: u64,
    kind: SegmentKind,
) -> io::Result<()> {
    write_source_marker(
        dir,
        SourceMarker::new(
            SourceIdentity {
                namespace,
                generation,
                segment_id,
                kind,
            },
            SOURCE_PREFIX_REWRITE,
            prefix_rows,
        )
        .with_commitment(
            fs::read(dir.join("canonical.bitmap"))
                .ok()
                .and_then(|bytes| RawCanonicalMetadata::parse(&bytes, bytes.len() as u64).ok())
                .and_then(|metadata| {
                    if metadata.rows == prefix_rows {
                        metadata.commitment
                    } else {
                        metadata
                            .previous
                            .filter(|(rows, _)| *rows == prefix_rows)
                            .map(|(_, root)| root)
                    }
                }),
        ),
        durability::Publication::Durable,
    )
}

#[cfg(test)]
pub(crate) fn mark_source_updating_for_test(
    dir: &Path,
    identity: SourceIdentity,
) -> io::Result<()> {
    write_source_marker(
        dir,
        SourceMarker::new(identity, SOURCE_UPDATING, 0),
        durability::Publication::Deferred,
    )
}

#[cfg(test)]
pub(crate) fn remove_source_marker_for_test(dir: &Path) -> io::Result<()> {
    fs::remove_file(dir.join(SOURCE_MARKER_FILE))
}

/// Magic bytes identifying a LogEx column file.
const COLUMN_MAGIC: &[u8; 4] = b"LXCL";

/// Current column file format version.
pub(crate) const COLUMN_VERSION: u32 = 1;
const ZERO_B256: [u8; 32] = [0; 32];

/// Header written at the start of every `.col` file.
#[derive(Debug, Clone, Copy)]
pub struct ColumnFileHeader {
    pub version: u32,
    pub row_count: u64,
    /// 0 = no compression (used in write path; compression added later).
    pub compression: u8,
}

impl ColumnFileHeader {
    pub fn write_to(&self, w: &mut (impl Write + ?Sized)) -> io::Result<()> {
        w.write_all(COLUMN_MAGIC)?;
        w.write_all(&self.version.to_le_bytes())?;
        w.write_all(&self.row_count.to_le_bytes())?;
        w.write_all(&[self.compression])?;
        Ok(())
    }

    pub fn read_from(data: &[u8]) -> Option<Self> {
        if data.len() < Self::SIZE || &data[0..4] != COLUMN_MAGIC {
            return None;
        }
        let version = u32::from_le_bytes(data[4..8].try_into().ok()?);
        let row_count = u64::from_le_bytes(data[8..16].try_into().ok()?);
        let compression = data[16];
        Some(Self {
            version,
            row_count,
            compression,
        })
    }

    /// Total byte size of the header on disk.
    pub const SIZE: usize = 4 + 4 + 8 + 1; // magic + version + row_count + compression
}

/// A bitmap tracking which rows have null values (used for optional topic columns).
#[derive(Debug, Clone, Default)]
pub struct NullBitmap {
    /// One bit per row. `true` = value present, `false` = null. Unused bits
    /// in the last byte stay zero so appending a null can leave them untouched.
    bits: Vec<u8>,
    len: u64,
}

impl NullBitmap {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn push(&mut self, present: bool) {
        let byte_idx = (self.len / 8) as usize;
        let bit_idx = (self.len % 8) as u32;
        if byte_idx >= self.bits.len() {
            self.bits.push(0);
        }
        if present {
            self.bits[byte_idx] |= 1 << bit_idx;
        }
        self.len += 1;
    }

    /// Set the value at position `row`.
    pub fn set(&mut self, row: u64, present: bool) {
        if row >= self.len {
            return;
        }
        let byte_idx = (row / 8) as usize;
        let bit_idx = (row % 8) as u32;
        if byte_idx < self.bits.len() {
            if present {
                self.bits[byte_idx] |= 1 << bit_idx;
            } else {
                self.bits[byte_idx] &= !(1 << bit_idx);
            }
        }
    }

    pub fn is_present(&self, row: u64) -> bool {
        let Ok(byte_idx) = usize::try_from(row / 8) else {
            return false;
        };
        let bit_idx = (row % 8) as u32;
        if byte_idx >= self.bits.len() {
            return false;
        }
        (self.bits[byte_idx] >> bit_idx) & 1 == 1
    }

    pub fn len(&self) -> u64 {
        self.len
    }

    pub fn write_to(&self, w: &mut (impl Write + ?Sized)) -> io::Result<()> {
        w.write_all(&self.len.to_le_bytes())?;
        w.write_all(&self.bits)?;
        Ok(())
    }

    pub fn read_from(data: &[u8]) -> Option<Self> {
        if data.len() < 8 {
            return None;
        }
        let len = u64::from_le_bytes(data[0..8].try_into().ok()?);
        let byte_count = usize::try_from(len.div_ceil(8)).ok()?;
        let end = 8usize.checked_add(byte_count)?;
        let mut bits = data.get(8..end)?.to_vec();
        if !len.is_multiple_of(8) {
            // Padding is outside the declared row set. Normalize it before a
            // later append reuses those bit positions, preserving all real rows.
            *bits.last_mut()? &= (1u8 << (len % 8)) - 1;
        }
        Some(Self { bits, len })
    }
}

/// Handles writing column files for a partition directory.
/// Column replacements order their contents before rename. The storage caller
/// must synchronize the complete segment before publishing its manifest.
pub struct ColumnFile;

fn join_write_worker(handle: thread::ScopedJoinHandle<'_, io::Result<()>>) -> io::Result<()> {
    handle
        .join()
        .map_err(|_| io::Error::other("column write worker panicked"))?
}

impl ColumnFile {
    /// Write all fixed-size and variable-length column files for a batch of rows.
    pub fn write_batch(dir: &Path, rows: &[LogRow]) -> io::Result<()> {
        Self::write_batch_with_canonical(dir, rows, None)
    }

    pub(crate) fn write_batch_with_canonical(
        dir: &Path,
        rows: &[LogRow],
        canonical: Option<&NullBitmap>,
    ) -> io::Result<()> {
        Self::write_batch_with_publication(dir, rows, canonical, durability::Publication::Ordered)
    }

    pub(crate) fn write_batch_with_publication(
        dir: &Path,
        rows: &[LogRow],
        canonical: Option<&NullBitmap>,
        publication: durability::Publication,
    ) -> io::Result<()> {
        let _owner = SourceWriteGuard::create_and_acquire(dir)?;
        let mut namespace = [0; 16];
        getrandom::fill(&mut namespace).map_err(|error| io::Error::other(error.to_string()))?;
        Self::write_batch_with_owned_source(
            dir,
            rows,
            canonical,
            publication,
            false,
            SourceIdentity {
                namespace,
                generation: 0,
                segment_id: u64::MAX,
                kind: SegmentKind::Hot,
            },
            Some(&PrefixState::from_rows(namespace, rows)?),
        )
    }

    /// Initialize a catalog-owned raw source whose authoritative prefix is zero.
    /// The caller publishes the resulting columns through its manifest/catalog.
    pub(crate) fn write_initial_batch_with_source_identity(
        dir: &Path,
        rows: &[LogRow],
        canonical: Option<&NullBitmap>,
        publication: durability::Publication,
        identity: SourceIdentity,
        prefix_state: Option<&PrefixState>,
    ) -> io::Result<()> {
        let _owner = SourceWriteGuard::create_and_acquire(dir)?;
        match read_source_marker(dir)? {
            None => {}
            Some(marker)
                if marker.state == SOURCE_COMMITTED
                    && marker.prefix_rows == 0
                    && marker.namespace == identity.namespace
                    && marker.generation == identity.generation
                    && marker.segment_id == identity.segment_id => {}
            Some(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    "raw source must be restored to its catalog-zero prefix before initialization",
                ));
            }
        }
        Self::write_batch_with_owned_source(
            dir,
            rows,
            canonical,
            publication,
            true,
            identity,
            prefix_state,
        )
    }

    pub(crate) fn rewrite_verified_prefix(
        dir: &Path,
        rows: &[LogRow],
        canonical: &NullBitmap,
        namespace: [u8; 16],
        generation: u64,
        owner: &PrefixRecoveryGuard,
    ) -> io::Result<()> {
        if canonical.len() != rows.len() as u64 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "verified prefix bitmap length differs from its rows",
            ));
        }
        if owner.namespace != namespace
            || owner.dir != dir
            || owner.prefix_rows != rows.len() as u64
            || owner.generation != generation
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "verified prefix differs from its recovery capability",
            ));
        }
        let prefix_state = crate::commitment::verify(namespace, owner.commitment, rows)?;
        write_source_marker(
            dir,
            SourceMarker::new(
                SourceIdentity {
                    namespace,
                    generation,
                    segment_id: owner.segment_id,
                    kind: owner.kind,
                },
                SOURCE_PREFIX_REWRITE,
                rows.len() as u64,
            )
            .with_commitment(owner.commitment),
            durability::Publication::Ordered,
        )?;
        let binding = SourceBinding {
            namespace,
            generation,
            segment_id: owner.segment_id,
        };
        publish_pending_canonical(
            dir,
            canonical,
            binding,
            owner.commitment,
            prefix_state.as_ref(),
        )?;
        Self::write_batch_contents(
            dir,
            rows,
            Some(canonical),
            durability::Publication::Durable,
            Some(binding),
            prefix_state.as_ref(),
            true,
        )?;
        owner.completed.set(true);
        Ok(())
    }

    pub(crate) fn rewrite_unidentified_prefix(
        dir: &Path,
        rows: &[LogRow],
        canonical: &NullBitmap,
        owner: &SourceWriteGuard,
    ) -> io::Result<()> {
        if canonical.len() != rows.len() as u64 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "legacy prefix bitmap length differs from its rows",
            ));
        }
        if owner.dir != dir {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "legacy prefix owner belongs to another segment",
            ));
        }
        // Keep an existing incidental sidecar binding usable by future raw
        // appends. Native descriptor/manifest namespaces remain unidentified.
        Self::write_batch_contents(
            dir,
            rows,
            Some(canonical),
            durability::Publication::Durable,
            owner.binding,
            None,
            false,
        )
    }

    pub(crate) fn finish_verified_prefix(
        dir: &Path,
        owner: &PrefixRecoveryGuard,
    ) -> io::Result<()> {
        if owner.dir != dir {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "prefix recovery capability belongs to another segment",
            ));
        }
        if !owner.completed.get() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "prefix rewrite has not completed",
            ));
        }
        verify_prefix_recovery_pending(dir, owner)?;
        write_source_marker(
            dir,
            SourceMarker::new(
                SourceIdentity {
                    namespace: owner.namespace,
                    generation: owner.generation,
                    segment_id: owner.segment_id,
                    kind: owner.kind,
                },
                SOURCE_COMMITTED,
                owner.prefix_rows,
            ),
            durability::Publication::Durable,
        )
    }

    fn write_batch_with_owned_source(
        dir: &Path,
        rows: &[LogRow],
        canonical: Option<&NullBitmap>,
        publication: durability::Publication,
        defer_source_markers: bool,
        identity: SourceIdentity,
        prefix_state: Option<&PrefixState>,
    ) -> io::Result<()> {
        let (pending_publication, marker_publication) = if defer_source_markers {
            (
                durability::Publication::Deferred,
                durability::Publication::Deferred,
            )
        } else {
            (
                durability::Publication::Ordered,
                durability::Publication::Durable,
            )
        };
        if rows.len() as u64 > u64::from(u32::MAX) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "replacement rows exceed segment addressing",
            ));
        }
        if canonical.is_some_and(|bitmap| bitmap.len() != rows.len() as u64) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "canonical bitmap length differs from replacement rows",
            ));
        }
        if let Some(state) = prefix_state {
            state.validate(identity.namespace, rows.len() as u64, state.commitment())?;
        }
        // Readers must see the recovery-required state before column replacement.
        // Standalone replacement orders it before changing columns; catalog-zero initialization
        // may defer both markers because its caller publishes the complete tree.
        write_source_marker(
            dir,
            SourceMarker::new(identity, SOURCE_UPDATING, 0),
            pending_publication,
        )?;
        if pending_publication != durability::Publication::Deferred {
            // The pending fence precedes every noncanonical rename. Its binding
            // is the new incarnation, so old native manifests cannot accept it.
            let empty_state = PrefixState::empty(identity.namespace);
            publish_pending_canonical(
                dir,
                &NullBitmap::new(),
                identity.into(),
                Some(empty_state.commitment()),
                Some(&empty_state),
            )?;
        }
        Self::write_batch_contents(
            dir,
            rows,
            canonical,
            publication,
            Some(identity.into()),
            prefix_state,
            pending_publication != durability::Publication::Deferred,
        )?;
        write_source_marker(
            dir,
            SourceMarker::new(identity, SOURCE_COMMITTED, rows.len() as u64),
            marker_publication,
        )
    }

    // Every caller already owns a prepared segment directory. Repeating
    // directory preparation here would add filesystem work to every batch.
    fn write_batch_contents(
        dir: &Path,
        rows: &[LogRow],
        canonical: Option<&NullBitmap>,
        publication: durability::Publication,
        binding: Option<SourceBinding>,
        prefix_state: Option<&PrefixState>,
        order_names: bool,
    ) -> io::Result<()> {
        let row_count = rows.len() as u64;
        let replacements = durability::ReplacementBatch::new(publication);

        thread::scope(|scope| {
            let block_columns = scope.spawn(|| {
                Self::write_fixed_col(
                    dir,
                    &replacements,
                    "address.col",
                    row_count,
                    rows,
                    |w, r| w.write_all(r.address.as_slice()),
                )?;
                Self::write_fixed_col(
                    dir,
                    &replacements,
                    "block_number.col",
                    row_count,
                    rows,
                    |w, r| w.write_all(&r.block_number.to_le_bytes()),
                )?;
                Self::write_fixed_col(
                    dir,
                    &replacements,
                    "block_hash.col",
                    row_count,
                    rows,
                    |w, r| w.write_all(r.block_hash.as_slice()),
                )?;
                Self::write_fixed_col(
                    dir,
                    &replacements,
                    "timestamp.col",
                    row_count,
                    rows,
                    |w, r| w.write_all(&r.timestamp.to_le_bytes()),
                )?;
                Self::write_nullable_col(dir, &replacements, "topic0", row_count, rows, |row| {
                    row.topic0.as_ref()
                })?;
                Ok(())
            });
            let transaction_columns = scope.spawn(|| {
                Self::write_fixed_col(
                    dir,
                    &replacements,
                    "tx_hash.col",
                    row_count,
                    rows,
                    |w, r| w.write_all(r.tx_hash.as_slice()),
                )?;
                Self::write_fixed_col(
                    dir,
                    &replacements,
                    "tx_index.col",
                    row_count,
                    rows,
                    |w, r| w.write_all(&r.tx_index.to_le_bytes()),
                )?;
                Self::write_fixed_col(
                    dir,
                    &replacements,
                    "log_index.col",
                    row_count,
                    rows,
                    |w, r| w.write_all(&r.log_index.to_le_bytes()),
                )?;
                Self::write_fixed_col(
                    dir,
                    &replacements,
                    "data_len.col",
                    row_count,
                    rows,
                    |w, r| w.write_all(&r.data_len.to_le_bytes()),
                )?;
                Self::write_fixed_col(
                    dir,
                    &replacements,
                    "source.col",
                    row_count,
                    rows,
                    |w, r| w.write_all(&[r.source as u8]),
                )?;
                Self::write_nullable_col(dir, &replacements, "topic1", row_count, rows, |row| {
                    row.topic1.as_ref()
                })?;
                Ok(())
            });
            let remaining_topics = scope.spawn(|| {
                Self::write_nullable_col(dir, &replacements, "topic2", row_count, rows, |row| {
                    row.topic2.as_ref()
                })?;
                Self::write_nullable_col(dir, &replacements, "topic3", row_count, rows, |row| {
                    row.topic3.as_ref()
                })?;
                Ok(())
            });
            let variable_columns = scope.spawn(|| {
                Self::write_var_col(dir, &replacements, "data.col", row_count, rows)?;
                let mut bitmap = NullBitmap::new();
                let bitmap = match canonical {
                    Some(bitmap) => bitmap,
                    None => {
                        for _ in 0..row_count {
                            bitmap.push(true);
                        }
                        &bitmap
                    }
                };
                replacements.write(&dir.join("canonical.bitmap"), |writer| {
                    write_raw_canonical(
                        writer,
                        bitmap,
                        binding,
                        prefix_state.map(PrefixState::commitment),
                        prefix_state,
                    )
                })?;
                Ok(())
            });
            join_write_worker(block_columns)?;
            join_write_worker(transaction_columns)?;
            join_write_worker(remaining_topics)?;
            join_write_worker(variable_columns)?;
            Ok::<_, io::Error>(())
        })?;

        replacements.publish_canonical_last(&dir.join("canonical.bitmap"), order_names)
    }

    /// Append rows to existing column files (for the hot partition).
    pub fn append_batch(dir: &Path, rows: &[LogRow], existing_rows: u64) -> io::Result<()> {
        Self::append_batch_with_publication(
            dir,
            rows,
            existing_rows,
            durability::Publication::Ordered,
        )
    }

    pub(crate) fn append_batch_with_publication(
        dir: &Path,
        rows: &[LogRow],
        existing_rows: u64,
        publication: durability::Publication,
    ) -> io::Result<()> {
        let (_owner, initialize) = match SourceWriteGuard::acquire_existing(dir) {
            Ok(owner) => (owner, false),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                if existing_rows != 0 {
                    return Err(error);
                }
                #[cfg(test)]
                if let Some(hook) = BEFORE_ABSENT_APPEND_CREATE.with_borrow_mut(Option::take) {
                    hook();
                }
                let owner = SourceWriteGuard::create_and_acquire(dir)?;
                // Absence was observed before ownership. Another writer may
                // have published or interrupted a source in that interval.
                // Only a still-empty directory is safe to initialize here.
                if read_source_marker(dir)?.is_some()
                    || fs::read_dir(dir)?.next().transpose()?.is_some()
                {
                    return Err(io::Error::new(
                        io::ErrorKind::WouldBlock,
                        "source appeared during append initialization; recapture its committed state",
                    ));
                }
                (owner, true)
            }
            Err(error) => return Err(error),
        };
        if initialize {
            let mut namespace = [0; 16];
            getrandom::fill(&mut namespace).map_err(|error| io::Error::other(error.to_string()))?;
            return Self::write_batch_with_owned_source(
                dir,
                rows,
                None,
                publication,
                false,
                SourceIdentity {
                    namespace,
                    generation: 0,
                    segment_id: u64::MAX,
                    kind: SegmentKind::Hot,
                },
                Some(&PrefixState::from_rows(namespace, rows)?),
            );
        }
        let binding = read_source_binding(dir)?;
        Self::append_batch_owned(dir, rows, existing_rows, publication, binding, None)
    }

    fn append_batch_owned(
        dir: &Path,
        rows: &[LogRow],
        existing_rows: u64,
        publication: durability::Publication,
        binding: Option<SourceBinding>,
        revision: Option<&crate::commitment::AppendRevision>,
    ) -> io::Result<()> {
        // Validate the immutable canonical prefix before any in-place column
        // append. Reuse this one read in the existing replacement worker.
        let data = fs::read(dir.join("canonical.bitmap"))?;
        let metadata = RawCanonicalMetadata::parse(&data, data.len() as u64)?
            .validate_committed(binding, existing_rows)?
            .validate_exact_len(data.len() as u64)?;
        let computed_revision;
        let revision = match revision {
            Some(revision) => {
                if metadata.commitment != revision.previous {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "append revision differs from canonical prefix",
                    ));
                }
                revision
            }
            None => {
                computed_revision = crate::commitment::AppendRevision::new(
                    metadata.resume_state(&data)?.as_ref(),
                    rows,
                )?;
                &computed_revision
            }
        };
        let mut canonical = metadata.bitmap(&data)?;
        if canonical.len() != existing_rows {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "canonical bitmap row count differs from append prefix",
            ));
        }
        let new_row_count = existing_rows
            .checked_add(rows.len() as u64)
            .filter(|&count| binding.is_none() || count <= u64::from(u32::MAX))
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "append rows exceed segment addressing",
                )
            })?;
        match (binding, revision.next, revision.state.as_ref()) {
            (Some(binding), Some(root), Some(state)) => {
                state.validate(binding.namespace, new_row_count, root)?
            }
            (_, None, None) => {}
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "append revision lacks bound restart state",
                ));
            }
        }
        for _ in rows {
            canonical.push(true);
        }
        let replacements = durability::ReplacementBatch::new(publication);

        thread::scope(|scope| {
            let block_columns = scope.spawn(|| {
                Self::append_fixed_col::<20>(
                    dir,
                    "address.col",
                    existing_rows,
                    new_row_count,
                    rows,
                    |w, r| w.write_all(r.address.as_slice()),
                )?;
                Self::append_fixed_col::<8>(
                    dir,
                    "block_number.col",
                    existing_rows,
                    new_row_count,
                    rows,
                    |w, r| w.write_all(&r.block_number.to_le_bytes()),
                )?;
                Self::append_fixed_col::<32>(
                    dir,
                    "block_hash.col",
                    existing_rows,
                    new_row_count,
                    rows,
                    |w, r| w.write_all(r.block_hash.as_slice()),
                )?;
                Self::append_fixed_col::<8>(
                    dir,
                    "timestamp.col",
                    existing_rows,
                    new_row_count,
                    rows,
                    |w, r| w.write_all(&r.timestamp.to_le_bytes()),
                )?;
                Self::append_nullable_col(
                    dir,
                    &replacements,
                    "topic0",
                    existing_rows,
                    new_row_count,
                    rows,
                    |w, r| write_optional_b256(w, r.topic0.as_ref()),
                )?;
                Ok(())
            });
            let transaction_columns = scope.spawn(|| {
                Self::append_fixed_col::<32>(
                    dir,
                    "tx_hash.col",
                    existing_rows,
                    new_row_count,
                    rows,
                    |w, r| w.write_all(r.tx_hash.as_slice()),
                )?;
                Self::append_fixed_col::<4>(
                    dir,
                    "tx_index.col",
                    existing_rows,
                    new_row_count,
                    rows,
                    |w, r| w.write_all(&r.tx_index.to_le_bytes()),
                )?;
                Self::append_fixed_col::<4>(
                    dir,
                    "log_index.col",
                    existing_rows,
                    new_row_count,
                    rows,
                    |w, r| w.write_all(&r.log_index.to_le_bytes()),
                )?;
                Self::append_fixed_col::<4>(
                    dir,
                    "data_len.col",
                    existing_rows,
                    new_row_count,
                    rows,
                    |w, r| w.write_all(&r.data_len.to_le_bytes()),
                )?;
                Self::append_fixed_col::<1>(
                    dir,
                    "source.col",
                    existing_rows,
                    new_row_count,
                    rows,
                    |w, r| w.write_all(&[r.source as u8]),
                )?;
                Self::append_nullable_col(
                    dir,
                    &replacements,
                    "topic1",
                    existing_rows,
                    new_row_count,
                    rows,
                    |w, r| write_optional_b256(w, r.topic1.as_ref()),
                )?;
                Ok(())
            });
            let remaining_topics = scope.spawn(|| {
                Self::append_nullable_col(
                    dir,
                    &replacements,
                    "topic2",
                    existing_rows,
                    new_row_count,
                    rows,
                    |w, r| write_optional_b256(w, r.topic2.as_ref()),
                )?;
                Self::append_nullable_col(
                    dir,
                    &replacements,
                    "topic3",
                    existing_rows,
                    new_row_count,
                    rows,
                    |w, r| write_optional_b256(w, r.topic3.as_ref()),
                )?;
                Ok(())
            });
            let variable_columns = scope.spawn(|| {
                Self::append_var_col(
                    dir,
                    &replacements,
                    "data.col",
                    new_row_count,
                    rows,
                    existing_rows,
                )?;
                replacements.write(&dir.join("canonical.bitmap"), |writer| {
                    write_raw_canonical_with_previous(
                        writer,
                        &canonical,
                        binding,
                        revision.next,
                        revision.state.as_ref(),
                        if rows.is_empty() {
                            metadata.previous
                        } else {
                            revision.previous.map(|root| (existing_rows, root))
                        },
                    )
                })?;
                Ok(())
            });
            join_write_worker(block_columns)?;
            join_write_worker(transaction_columns)?;
            join_write_worker(remaining_topics)?;
            join_write_worker(variable_columns)?;
            Ok::<_, io::Error>(())
        })?;

        replacements.publish_canonical_last(&dir.join("canonical.bitmap"), false)
    }

    pub(crate) fn append_batch_with_source_binding(
        dir: &Path,
        rows: &[LogRow],
        existing_rows: u64,
        publication: durability::Publication,
        binding: SourceBinding,
        revision: &crate::commitment::AppendRevision,
    ) -> io::Result<()> {
        let owner = SourceWriteGuard::acquire_bound(
            dir,
            binding.namespace,
            binding.generation,
            binding.segment_id,
        )?;
        Self::append_batch_owned(
            dir,
            rows,
            existing_rows,
            publication,
            owner.binding,
            Some(revision),
        )
    }

    fn write_fixed_col(
        dir: &Path,
        replacements: &durability::ReplacementBatch,
        name: &str,
        row_count: u64,
        rows: &[LogRow],
        mut write_value: impl FnMut(&mut BufWriter<File>, &LogRow) -> io::Result<()>,
    ) -> io::Result<()> {
        replacements.write(&dir.join(name), |writer| {
            ColumnFileHeader {
                version: COLUMN_VERSION,
                row_count,
                compression: 0,
            }
            .write_to(writer)?;
            for row in rows {
                write_value(writer, row)?;
            }
            Ok(())
        })
    }

    fn append_fixed_col<const WIDTH: u64>(
        dir: &Path,
        name: &str,
        existing_rows: u64,
        new_row_count: u64,
        rows: &[LogRow],
        mut write_value: impl FnMut(&mut BufWriter<File>, &LogRow) -> io::Result<()>,
    ) -> io::Result<()> {
        let path = dir.join(name);
        let file = Self::open_col_for_append(&path, existing_rows, new_row_count, WIDTH)?;
        let mut file = BufWriter::new(file);
        for row in rows {
            write_value(&mut file, row)?;
        }
        file.flush()?;
        Ok(())
    }

    fn write_nullable_col(
        dir: &Path,
        replacements: &durability::ReplacementBatch,
        base_name: &str,
        row_count: u64,
        rows: &[LogRow],
        value: impl Fn(&LogRow) -> Option<&alloy_primitives::B256>,
    ) -> io::Result<()> {
        let all_null = rows.iter().all(|row| value(row).is_none());
        let mut nulls = if all_null {
            NullBitmap {
                bits: vec![0; rows.len().div_ceil(8)],
                len: row_count,
            }
        } else {
            NullBitmap::new()
        };
        replacements.write(&dir.join(format!("{base_name}.col")), |writer| {
            ColumnFileHeader {
                version: COLUMN_VERSION,
                row_count,
                compression: 0,
            }
            .write_to(writer)?;
            if all_null {
                let length = row_count
                    .checked_mul(32)
                    .and_then(|bytes| bytes.checked_add(ColumnFileHeader::SIZE as u64))
                    .ok_or_else(|| {
                        io::Error::new(
                            io::ErrorKind::InvalidInput,
                            "nullable column length overflow",
                        )
                    })?;
                // Replacement files are new/empty. Extension supplies the same
                // logical zero bytes without explicitly writing every null slot.
                // Normal replacement/checkpoint ordering also persists its length.
                writer.get_ref().set_len(length)?;
            } else {
                for row in rows {
                    nulls.push(write_optional_b256(writer, value(row))?);
                }
            }
            Ok(())
        })?;
        replacements.write(&dir.join(format!("{base_name}.null")), |writer| {
            nulls.write_to(writer)
        })
    }

    fn append_nullable_col(
        dir: &Path,
        replacements: &durability::ReplacementBatch,
        base_name: &str,
        existing_rows: u64,
        new_row_count: u64,
        rows: &[LogRow],
        mut write_value: impl FnMut(&mut BufWriter<File>, &LogRow) -> io::Result<bool>,
    ) -> io::Result<()> {
        let col_path = dir.join(format!("{base_name}.col"));
        let null_path = dir.join(format!("{base_name}.null"));

        let col_file = Self::open_col_for_append(&col_path, existing_rows, new_row_count, 32)?;
        let mut col_file = BufWriter::new(col_file);

        // Read existing null bitmap and append
        let null_data = fs::read(&null_path)?;
        let mut nulls = NullBitmap::read_from(&null_data)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "corrupt null bitmap"))?;
        if nulls.len() != existing_rows {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "null bitmap row count mismatch for {base_name}: expected {existing_rows}, got {}",
                    nulls.len()
                ),
            ));
        }

        for row in rows {
            nulls.push(write_value(&mut col_file, row)?);
        }
        col_file.flush()?;

        replacements.write(&null_path, |nw| nulls.write_to(nw))?;

        Ok(())
    }

    /// Variable-length column: 8-byte offsets (one per row plus sentinel), then data.
    fn write_var_col(
        dir: &Path,
        replacements: &durability::ReplacementBatch,
        name: &str,
        row_count: u64,
        rows: &[LogRow],
    ) -> io::Result<()> {
        replacements.write(&dir.join(name), |writer| {
            ColumnFileHeader {
                version: COLUMN_VERSION,
                row_count,
                compression: 0,
            }
            .write_to(writer)?;
            let mut offset = 0u64;
            for row in rows {
                writer.write_all(&offset.to_le_bytes())?;
                offset = offset.checked_add(row.data.len() as u64).ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "data column size overflow")
                })?;
            }
            writer.write_all(&offset.to_le_bytes())?;
            for row in rows {
                writer.write_all(&row.data)?;
            }
            Ok(())
        })
    }

    fn append_var_col(
        dir: &Path,
        replacements: &durability::ReplacementBatch,
        name: &str,
        new_row_count: u64,
        rows: &[LogRow],
        existing_rows: u64,
    ) -> io::Result<()> {
        let path = dir.join(name);
        // The append already reads the old file. Reuse the reader's complete
        // layout validation before constructing a replacement, and borrow its
        // encoded offsets instead of allocating another table for every row.
        let data = crate::reader::RawBytesColumn::open(&path)?;
        if data.row_count() as u64 != existing_rows {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "data column row count mismatch: expected {existing_rows}, got {}",
                    data.row_count()
                ),
            ));
        }
        let existing_data_len = data.payload().len() as u64;
        let final_offset = rows.iter().try_fold(existing_data_len, |offset, row| {
            offset.checked_add(row.data.len() as u64).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "data column size overflow")
            })
        })?;

        replacements.write(&path, |writer| {
            ColumnFileHeader {
                version: COLUMN_VERSION,
                row_count: new_row_count,
                compression: 0,
            }
            .write_to(writer)?;
            writer.write_all(data.encoded_row_offsets())?;
            let mut offset = existing_data_len;
            for row in rows {
                writer.write_all(&offset.to_le_bytes())?;
                // The immutable input slice's complete sum was checked above.
                offset += row.data.len() as u64;
            }
            writer.write_all(&final_offset.to_le_bytes())?;
            writer.write_all(data.payload())?;
            for row in rows {
                writer.write_all(&row.data)?;
            }
            Ok(())
        })
    }

    /// Write a canonical bitmap where all rows are marked canonical (all 1s).
    #[cfg(test)]
    pub(crate) fn write_canonical_bitmap(dir: &Path, row_count: u64) -> io::Result<()> {
        let mut bitmap = NullBitmap::new();
        for _ in 0..row_count {
            bitmap.push(true);
        }
        Self::replace_canonical_bitmap(dir, &bitmap)
    }

    #[cfg(test)]
    pub(crate) fn replace_canonical_bitmap(dir: &Path, bitmap: &NullBitmap) -> io::Result<()> {
        let mut owner = SourceWriteGuard::create_and_acquire(dir)?;
        owner.binding = read_source_binding(dir)?;
        if !dir.join("canonical.bitmap").exists() {
            return durability::atomic_write(&dir.join("canonical.bitmap"), |writer| {
                write_raw_canonical(writer, bitmap, owner.binding, None, None)
            });
        }
        let bytes = fs::read(dir.join("canonical.bitmap"))?;
        let metadata = RawCanonicalMetadata::parse(&bytes, bytes.len() as u64)?;
        if metadata.rows != bitmap.len() {
            let commitment = metadata
                .previous
                .filter(|(rows, _)| *rows == bitmap.len())
                .map(|(_, root)| root);
            return durability::atomic_write(&dir.join("canonical.bitmap"), |writer| {
                match (owner.binding, commitment) {
                    (Some(binding), Some(root)) => {
                        write_canonical_with_missing_restart_state(writer, bitmap, binding, root)
                    }
                    (_, None) => write_raw_canonical(writer, bitmap, owner.binding, None, None),
                    (None, Some(_)) => Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "test prefix root has no source binding",
                    )),
                }
            });
        }
        Self::replace_canonical_bitmap_owned(dir, bitmap, &owner)
    }

    pub(crate) fn replace_canonical_bitmap_owned(
        dir: &Path,
        bitmap: &NullBitmap,
        owner: &SourceWriteGuard,
    ) -> io::Result<()> {
        if owner.dir != dir {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "raw source owner belongs to another segment",
            ));
        }
        let data = fs::read(dir.join("canonical.bitmap"))?;
        let metadata = RawCanonicalMetadata::parse(&data, data.len() as u64)?
            .validate_committed(owner.binding, bitmap.len())?;
        if metadata.rows != bitmap.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "canonical update cannot change the physical prefix",
            ));
        }
        let prefix_state = metadata.resume_state(&data)?;
        durability::atomic_write(&dir.join("canonical.bitmap"), |writer| {
            write_raw_canonical_with_previous(
                writer,
                bitmap,
                owner.binding,
                metadata.commitment,
                prefix_state.as_ref(),
                metadata.previous,
            )
        })
    }

    fn open_col_for_append(
        path: &Path,
        existing_rows: u64,
        new_row_count: u64,
        item_size: u64,
    ) -> io::Result<File> {
        let mut file = OpenOptions::new().read(true).write(true).open(path)?;
        let mut header_buf = [0u8; ColumnFileHeader::SIZE];
        file.read_exact(&mut header_buf)?;
        let header = ColumnFileHeader::read_from(&header_buf).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("corrupt column header in {}", path.display()),
            )
        })?;
        if header.version != COLUMN_VERSION || header.compression != 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unsupported raw column format in {}", path.display()),
            ));
        }
        if header.row_count != existing_rows {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "column row count mismatch for {}: expected {existing_rows}, got {}",
                    path.display(),
                    header.row_count
                ),
            ));
        }

        let expected_len = existing_rows
            .checked_mul(item_size)
            .and_then(|length| length.checked_add(ColumnFileHeader::SIZE as u64))
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "column length overflow"))?;
        let actual_len = file.metadata()?.len();
        if actual_len != expected_len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "column length mismatch for {}: expected {expected_len}, got {actual_len}",
                    path.display()
                ),
            ));
        }

        file.seek(SeekFrom::Start(8))?;
        file.write_all(&new_row_count.to_le_bytes())?;
        file.seek(SeekFrom::End(0))?;
        Ok(file)
    }
}

fn write_optional_b256(
    writer: &mut BufWriter<File>,
    value: Option<&alloy_primitives::B256>,
) -> io::Result<bool> {
    if let Some(value) = value {
        writer.write_all(value.as_slice())?;
        Ok(true)
    } else {
        writer.write_all(&ZERO_B256)?;
        Ok(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::{Address, B256, Bytes};
    use logex_types::Source;

    fn row() -> LogRow {
        LogRow {
            block_number: 1,
            block_hash: B256::repeat_byte(1),
            timestamp: 2,
            tx_hash: B256::repeat_byte(2),
            tx_index: 0,
            log_index: 0,
            address: Address::repeat_byte(3),
            topic0: None,
            topic1: None,
            topic2: None,
            topic3: None,
            data: Bytes::new(),
            data_len: 0,
            source: Source::Receipt,
        }
    }

    #[test]
    fn canonical_envelope_checks_binding_state_rows_and_exact_length() {
        let binding = SourceBinding {
            namespace: [7; 16],
            generation: 3,
            segment_id: 9,
        };
        let mut bitmap = NullBitmap::new();
        bitmap.push(false);
        bitmap.push(true);
        let mut bytes = Vec::new();
        write_raw_canonical(&mut bytes, &bitmap, Some(binding), None, None).unwrap();
        assert!(
            !read_canonical_bitmap(&bytes, Some(binding))
                .unwrap()
                .is_present(0)
        );
        let foreign = SourceBinding {
            generation: 4,
            ..binding
        };
        assert!(read_canonical_bitmap(&bytes, Some(foreign)).is_err());
        for offset in [8, 9, 10, 26, 34, 42, 50, 54] {
            let mut damaged = bytes.clone();
            damaged[offset] ^= 1;
            assert!(
                read_canonical_bitmap(&damaged, Some(binding)).is_err(),
                "offset {offset}"
            );
        }
        for len in [0, 8, 53, 61, bytes.len() - 1, bytes.len() + 1] {
            let mut damaged = bytes.clone();
            damaged.resize(len, 0);
            assert!(
                read_canonical_bitmap(&damaged, Some(binding)).is_err(),
                "length {len}"
            );
        }
        let mut pending = Vec::new();
        write_raw_canonical_state(
            &mut pending,
            &bitmap,
            Some(binding),
            None,
            None,
            None,
            CANONICAL_PENDING,
        )
        .unwrap();
        assert_eq!(
            read_canonical_bitmap(&pending, Some(binding))
                .unwrap_err()
                .kind(),
            io::ErrorKind::WouldBlock
        );
        let mut legacy = Vec::new();
        bitmap.write_to(&mut legacy).unwrap();
        assert!(read_canonical_bitmap(&legacy, None).is_ok());
        assert!(read_canonical_bitmap(&legacy, Some(binding)).is_err());
        let oversized = u64::from(u32::MAX) + 1;
        bytes[42..50].copy_from_slice(&oversized.to_le_bytes());
        bytes[136..144].copy_from_slice(&oversized.to_le_bytes());
        let crc = crc32fast::hash(&bytes[..132]);
        bytes[132..136].copy_from_slice(&crc.to_le_bytes());
        assert!(RawCanonicalMetadata::parse(&bytes, 144 + oversized.div_ceil(8)).is_err());
    }

    #[test]
    fn canonical_writer_rejects_unpaired_or_unbound_restart_state_before_writing() {
        let binding = SourceBinding {
            namespace: [7; 16],
            generation: 3,
            segment_id: 9,
        };
        let state = PrefixState::empty(binding.namespace);
        let bitmap = NullBitmap::new();
        for (owner, root, prefix) in [
            (Some(binding), Some(state.commitment()), None),
            (Some(binding), None, Some(&state)),
            (None, Some(state.commitment()), Some(&state)),
            (None, None, Some(&state)),
            (None, Some(state.commitment()), None),
        ] {
            let mut bytes = Vec::new();
            assert!(write_raw_canonical(&mut bytes, &bitmap, owner, root, prefix).is_err());
            assert!(bytes.is_empty());
        }
    }

    fn corrupt_real_canonical_bit(dir: &Path) -> (Vec<u8>, RawCanonicalMetadata) {
        let path = dir.join("canonical.bitmap");
        let mut bytes = fs::read(&path).unwrap();
        let metadata = RawCanonicalMetadata::parse(&bytes, bytes.len() as u64).unwrap();
        assert!(metadata.bitmap(&bytes).unwrap().is_present(0));
        // Change a real row bit, preserving envelope, row count, file length,
        // logical row commitment and restart state exactly.
        bytes[metadata.offset + 8] ^= 1;
        fs::write(path, &bytes).unwrap();
        assert_eq!(
            RawCanonicalMetadata::parse(&bytes[..CANONICAL_PREFIX_BYTES], bytes.len() as u64)
                .unwrap(),
            metadata
        );
        (bytes, metadata)
    }

    #[test]
    fn canonical_bitmap_integrity_bit_corruption_is_rejected_by_actual_reader() {
        let tmp = tempfile::tempdir().unwrap();
        ColumnFile::write_batch(tmp.path(), &[row(), row()]).unwrap();
        let (bytes, _) = corrupt_real_canonical_bit(tmp.path());
        let reader = crate::SegmentReader::open(tmp.path()).unwrap();
        let result = reader.read_canonical();
        assert!(
            matches!(result, Err(ref error) if error.kind() == io::ErrorKind::InvalidData),
            "same-length real-row canonical bit corruption must fail actual reader"
        );
        assert_eq!(
            fs::read(tmp.path().join("canonical.bitmap")).unwrap(),
            bytes
        );
    }

    #[test]
    fn canonical_bitmap_integrity_bit_corruption_is_rejected_by_resume_state() {
        let tmp = tempfile::tempdir().unwrap();
        ColumnFile::write_batch(tmp.path(), &[row(), row()]).unwrap();
        let (bytes, metadata) = corrupt_real_canonical_bit(tmp.path());
        let result = metadata.resume_state(&bytes);
        assert!(
            matches!(result, Err(ref error) if error.kind() == io::ErrorKind::InvalidData),
            "restart state must not authenticate a corrupted canonical payload"
        );
    }

    #[test]
    fn canonical_bitmap_integrity_bit_corruption_prevents_append_before_column_changes() {
        let tmp = tempfile::tempdir().unwrap();
        ColumnFile::write_batch(tmp.path(), &[row(), row()]).unwrap();
        let (bytes, _) = corrupt_real_canonical_bit(tmp.path());
        let address_before = fs::read(tmp.path().join("address.col")).unwrap();
        let result = ColumnFile::append_batch(tmp.path(), &[row()], 2);
        assert!(
            matches!(result, Err(ref error) if error.kind() == io::ErrorKind::InvalidData),
            "append must reject a corrupted canonical prefix"
        );
        assert_eq!(
            fs::read(tmp.path().join("address.col")).unwrap(),
            address_before
        );
        assert_eq!(
            fs::read(tmp.path().join("canonical.bitmap")).unwrap(),
            bytes
        );
    }

    #[test]
    fn canonical_bitmap_integrity_bit_corruption_prevents_canonical_replacement() {
        let tmp = tempfile::tempdir().unwrap();
        ColumnFile::write_batch(tmp.path(), &[row(), row()]).unwrap();
        let mut replacement = crate::SegmentReader::open(tmp.path())
            .unwrap()
            .read_canonical()
            .unwrap();
        replacement.set(1, false);
        let (bytes, _) = corrupt_real_canonical_bit(tmp.path());
        let binding = read_source_binding(tmp.path()).unwrap().unwrap();
        let owner = SourceWriteGuard::acquire_bound(
            tmp.path(),
            binding.namespace,
            binding.generation,
            binding.segment_id,
        )
        .unwrap();
        let result = ColumnFile::replace_canonical_bitmap_owned(tmp.path(), &replacement, &owner);
        assert!(
            matches!(result, Err(ref error) if error.kind() == io::ErrorKind::InvalidData),
            "canonical update must not carry forward or conceal a corrupt payload"
        );
        assert_eq!(
            fs::read(tmp.path().join("canonical.bitmap")).unwrap(),
            bytes
        );
    }

    #[test]
    fn canonical_bitmap_integrity_checks_stored_padding_and_restart_bytes_before_use() {
        let tmp = tempfile::tempdir().unwrap();
        ColumnFile::write_batch(tmp.path(), &[row(), row()]).unwrap();
        let bytes = fs::read(tmp.path().join("canonical.bitmap")).unwrap();
        let metadata =
            RawCanonicalMetadata::parse(&bytes[..CANONICAL_PREFIX_BYTES], bytes.len() as u64)
                .unwrap();
        assert_eq!(bytes[8], 4);
        assert_eq!(metadata.offset, 136);
        metadata
            .validate_payload_reader(&mut io::Cursor::new(&bytes))
            .unwrap();
        assert!(metadata.bitmap(&bytes).unwrap().is_present(0));
        assert_eq!(
            metadata.resume_state(&bytes).unwrap().unwrap().row_count(),
            2
        );
        for (offset, mask) in [(metadata.offset + 8, 0x80), (bytes.len() - 1, 1)] {
            let mut corrupt = bytes.clone();
            corrupt[offset] ^= mask;
            // Unused padding must be checked before NullBitmap normalizes it.
            // Restart bytes must also be checked by bitmap-only consumers.
            assert_eq!(
                RawCanonicalMetadata::parse(
                    &corrupt[..CANONICAL_PREFIX_BYTES],
                    corrupt.len() as u64
                )
                .unwrap(),
                metadata
            );
            assert_eq!(
                metadata.bitmap(&corrupt).unwrap_err().kind(),
                io::ErrorKind::InvalidData
            );
            assert_eq!(
                metadata.resume_state(&corrupt).unwrap_err().kind(),
                io::ErrorKind::InvalidData
            );
            assert_eq!(
                metadata
                    .validate_payload_reader(&mut io::Cursor::new(corrupt))
                    .unwrap_err()
                    .kind(),
                io::ErrorKind::InvalidData
            );
        }
    }

    #[test]
    fn canonical_bitmap_integrity_stream_checks_metadata_and_exact_payload_length() {
        let tmp = tempfile::tempdir().unwrap();
        ColumnFile::write_batch(tmp.path(), &[row()]).unwrap();
        let bytes = fs::read(tmp.path().join("canonical.bitmap")).unwrap();
        let metadata = RawCanonicalMetadata::parse(&bytes, bytes.len() as u64).unwrap();
        for mode in 0..4 {
            let mut corrupt = bytes.clone();
            match mode {
                0 => {
                    corrupt.pop();
                }
                1 => corrupt.push(0),
                2 => corrupt[metadata.offset] ^= 1,
                _ => {
                    corrupt[128] ^= 1;
                    let header_crc = crc32fast::hash(&corrupt[..132]);
                    corrupt[132..136].copy_from_slice(&header_crc.to_le_bytes());
                }
            }
            assert_eq!(
                metadata
                    .validate_payload_reader(&mut io::Cursor::new(corrupt))
                    .unwrap_err()
                    .kind(),
                io::ErrorKind::InvalidData
            );
        }
    }

    #[test]
    fn canonical_bitmap_integrity_rejects_old_bound_version_without_guessing_migration() {
        let binding = SourceBinding {
            namespace: [7; 16],
            generation: 1,
            segment_id: 2,
        };
        let mut bitmap = NullBitmap::new();
        bitmap.push(false);
        let mut bytes = Vec::new();
        write_raw_canonical(&mut bytes, &bitmap, Some(binding), None, None).unwrap();
        bytes[8] = 3;
        let header_crc = crc32fast::hash(&bytes[..132]);
        bytes[132..136].copy_from_slice(&header_crc.to_le_bytes());
        assert_eq!(
            RawCanonicalMetadata::parse(&bytes, bytes.len() as u64)
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidData
        );
        // Bare unbound bitmaps remain the explicit legacy/generic representation;
        // neither their byte shape nor this API invents payload-checksum evidence.
        let mut legacy = Vec::new();
        write_raw_canonical(&mut legacy, &bitmap, None, None, None).unwrap();
        let metadata = RawCanonicalMetadata::parse(&legacy, legacy.len() as u64).unwrap();
        assert!(metadata.payload_checksum.is_none());
        metadata
            .validate_payload_reader(&mut io::Cursor::new(&legacy))
            .unwrap();
        assert!(!metadata.bitmap(&legacy).unwrap().is_present(0));
    }

    #[test]
    fn canonical_bitmap_integrity_stream_handles_empty_and_multiple_chunks() {
        let binding = SourceBinding {
            namespace: [7; 16],
            generation: 1,
            segment_id: 2,
        };
        let empty_state = PrefixState::empty(binding.namespace);
        for state in [None, Some(&empty_state)] {
            let mut bytes = Vec::new();
            write_raw_canonical(
                &mut bytes,
                &NullBitmap::new(),
                Some(binding),
                state.map(PrefixState::commitment),
                state,
            )
            .unwrap();
            let metadata = RawCanonicalMetadata::parse(&bytes, bytes.len() as u64).unwrap();
            metadata
                .validate_payload_reader(&mut io::Cursor::new(&bytes))
                .unwrap();
            assert_eq!(metadata.bitmap(&bytes).unwrap().len(), 0);
            assert_eq!(metadata.resume_state(&bytes).unwrap().as_ref(), state);
        }

        // A normal large segment's bitmap crosses the maintenance read buffer.
        // Construct only that small bitmap, without allocating any log rows.
        let mut bitmap = NullBitmap::new();
        for row in 0..(64 * 1024 * 8 + 17) {
            bitmap.push(row % 3 != 0);
        }
        let mut bytes = Vec::new();
        write_raw_canonical(&mut bytes, &bitmap, Some(binding), None, None).unwrap();
        let metadata = RawCanonicalMetadata::parse(&bytes, bytes.len() as u64).unwrap();
        metadata
            .validate_payload_reader(&mut io::Cursor::new(&bytes))
            .unwrap();
        assert_eq!(metadata.bitmap(&bytes).unwrap().len(), bitmap.len());
        bytes[CANONICAL_PREFIX_BYTES + 64 * 1024] ^= 1;
        assert_eq!(
            metadata
                .validate_payload_reader(&mut io::Cursor::new(bytes))
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidData
        );
    }

    #[test]
    fn canonical_restart_state_is_bounded_and_verified_before_append() {
        let tmp = tempfile::tempdir().unwrap();
        ColumnFile::write_batch(tmp.path(), &[row()]).unwrap();
        let path = tmp.path().join("canonical.bitmap");
        let bytes = fs::read(&path).unwrap();
        let metadata = RawCanonicalMetadata::parse(&bytes, bytes.len() as u64).unwrap();
        let state = metadata.resume_state(&bytes).unwrap().unwrap();
        assert_eq!(state.row_count(), 1);
        assert!(metadata.state_len as usize <= PrefixState::MAX_ENCODED_BYTES);
        assert_eq!(
            RawCanonicalMetadata::parse(&bytes[..CANONICAL_PREFIX_BYTES], bytes.len() as u64)
                .unwrap(),
            metadata
        );
        let column_before = fs::read(tmp.path().join("address.col")).unwrap();
        let mut corrupt = bytes.clone();
        *corrupt.last_mut().unwrap() ^= 1;
        // Query capture stays cheap; writers verify the complete bounded state.
        assert!(
            RawCanonicalMetadata::parse(&corrupt[..CANONICAL_PREFIX_BYTES], corrupt.len() as u64)
                .is_ok()
        );
        fs::write(&path, &corrupt).unwrap();
        assert!(ColumnFile::append_batch(tmp.path(), &[row()], 1).is_err());
        assert_eq!(
            fs::read(tmp.path().join("address.col")).unwrap(),
            column_before
        );
        let mut missing = Vec::new();
        write_canonical_with_missing_restart_state(
            &mut missing,
            &metadata.bitmap(&bytes).unwrap(),
            metadata.binding.unwrap(),
            metadata.commitment.unwrap(),
        )
        .unwrap();
        fs::write(&path, missing).unwrap();
        assert!(ColumnFile::append_batch(tmp.path(), &[row()], 1).is_err());
        assert_eq!(
            fs::read(tmp.path().join("address.col")).unwrap(),
            column_before
        );
        fs::write(&path, &bytes).unwrap();
        ColumnFile::append_batch(tmp.path(), &[row()], 1).unwrap();
        let appended = fs::read(&path).unwrap();
        let updated = RawCanonicalMetadata::parse(&appended, appended.len() as u64).unwrap();
        assert_eq!(
            updated.resume_state(&appended).unwrap().unwrap(),
            state.extend(&[row()]).unwrap()
        );
    }

    #[test]
    fn previous_commitment_is_reader_only_and_canonical_updates_preserve_it() {
        let tmp = tempfile::tempdir().unwrap();
        ColumnFile::write_batch(tmp.path(), &[row()]).unwrap();
        let first = crate::SegmentReader::open(tmp.path())
            .unwrap()
            .source_commitment()
            .unwrap()
            .unwrap();
        ColumnFile::append_batch(tmp.path(), &[row()], 1).unwrap();
        let bytes = fs::read(tmp.path().join("canonical.bitmap")).unwrap();
        let metadata = RawCanonicalMetadata::parse(&bytes, bytes.len() as u64).unwrap();
        assert!(
            metadata
                .validate_capture_commitment(Some(first.into()), 1)
                .is_ok()
        );
        assert!(
            metadata
                .validate_capture_commitment(Some(first.into()), 2)
                .is_err()
        );
        assert!(metadata.validate_commitment(Some(first.into())).is_err());
        let saved_state = metadata.resume_state(&bytes).unwrap();
        let mut bitmap = NullBitmap::new();
        bitmap.push(false);
        bitmap.push(true);
        ColumnFile::replace_canonical_bitmap(tmp.path(), &bitmap).unwrap();
        ColumnFile::append_batch(tmp.path(), &[], 2).unwrap();
        let bytes = fs::read(tmp.path().join("canonical.bitmap")).unwrap();
        let updated = RawCanonicalMetadata::parse(&bytes, bytes.len() as u64).unwrap();
        assert_eq!(updated.commitment, metadata.commitment);
        assert_eq!(updated.previous, metadata.previous);
        assert_eq!(updated.resume_state(&bytes).unwrap(), saved_state);
        assert!(!updated.bitmap(&bytes).unwrap().is_present(0));
    }

    #[test]
    fn replacement_fences_columns_and_publishes_canonical_last() {
        let tmp = tempfile::tempdir().unwrap();
        ColumnFile::write_batch(tmp.path(), &[row()]).unwrap();
        durability::inject_failure(usize::MAX);
        ColumnFile::write_batch(tmp.path(), &[row(), row()]).unwrap();
        let events = durability::take_events();
        let renames: Vec<_> = events
            .iter()
            .filter(|(op, _)| *op == "rename_temporary")
            .map(|(_, path)| path.file_name().unwrap().to_str().unwrap())
            .collect();
        assert_eq!(&renames[..2], &[SOURCE_MARKER_FILE, "canonical.bitmap"]);
        assert_eq!(
            &renames[renames.len() - 2..],
            &["canonical.bitmap", SOURCE_MARKER_FILE]
        );
        assert!(
            renames[2..renames.len() - 2]
                .iter()
                .all(|name| *name != "canonical.bitmap")
        );
    }

    #[test]
    fn append_rejects_invalid_canonical_before_mutating_columns() {
        let tmp = tempfile::tempdir().unwrap();
        ColumnFile::write_batch(tmp.path(), &[row()]).unwrap();
        let before = fs::read(tmp.path().join("address.col")).unwrap();
        let path = tmp.path().join("canonical.bitmap");
        let mut bytes = fs::read(&path).unwrap();
        bytes[10] ^= 1;
        fs::write(&path, bytes).unwrap();
        assert!(ColumnFile::append_batch(tmp.path(), &[row()], 1).is_err());
        assert_eq!(fs::read(tmp.path().join("address.col")).unwrap(), before);
    }

    #[test]
    fn repeated_append_preserves_empty_and_nonempty_payloads() {
        let tmp = tempfile::tempdir().unwrap();
        ColumnFile::write_batch(tmp.path(), &[]).unwrap();
        let mut expected = Vec::new();
        for lengths in [[0, 1, 7], [3, 0, 2], [0, 0, 5]] {
            let rows = lengths.map(|length| {
                let mut value = row();
                value.data = Bytes::from(vec![length; usize::from(length)]);
                value.data_len = u32::from(length);
                value
            });
            ColumnFile::append_batch(tmp.path(), &rows, expected.len() as u64).unwrap();
            expected.extend(rows);
            let reader = crate::SegmentReader::open(tmp.path()).unwrap();
            assert_eq!(reader.read_log_rows(None).unwrap(), expected);
            assert_eq!(
                reader.read_var_bytes("data", Some(&[2, 0, 2])).unwrap(),
                [
                    expected[2].data.clone(),
                    expected[0].data.clone(),
                    expected[2].data.clone()
                ]
            );
        }
    }

    #[test]
    fn append_rejects_invalid_variable_layout_without_publishing() {
        for case in 0..7 {
            let tmp = tempfile::tempdir().unwrap();
            let mut value = row();
            value.data = Bytes::from_static(b"ab");
            value.data_len = 2;
            ColumnFile::write_batch(tmp.path(), &[value.clone(), value.clone()]).unwrap();
            let canonical = fs::read(tmp.path().join("canonical.bitmap")).unwrap();
            let path = tmp.path().join("data.col");
            let mut bytes = fs::read(&path).unwrap();
            let offsets = ColumnFileHeader::SIZE;
            match case {
                0 => bytes[offsets..offsets + 8].copy_from_slice(&1u64.to_le_bytes()),
                1 => bytes[offsets + 8..offsets + 16].copy_from_slice(&5u64.to_le_bytes()),
                2 => bytes[offsets + 16..offsets + 24].copy_from_slice(&3u64.to_le_bytes()),
                3 => {
                    bytes.pop();
                }
                4 => bytes[4..8].copy_from_slice(&(COLUMN_VERSION + 1).to_le_bytes()),
                5 => bytes[16] = 1,
                6 => bytes.truncate(offsets + 23),
                _ => unreachable!(),
            }
            fs::write(&path, &bytes).unwrap();
            let error = ColumnFile::append_batch(tmp.path(), &[value], 2)
                .expect_err("an invalid variable-column layout must not be published");
            assert_eq!(error.kind(), io::ErrorKind::InvalidData, "case {case}");
            assert_eq!(fs::read(&path).unwrap(), bytes, "case {case}");
            assert_eq!(
                fs::read(tmp.path().join("canonical.bitmap")).unwrap(),
                canonical,
                "case {case}"
            );
        }
    }

    #[test]
    fn append_rejects_invalid_fixed_layout_without_publishing() {
        for (case, name) in [
            (0, "address.col"),
            (1, "block_number.col"),
            (2, "tx_hash.col"),
            (3, "topic2.col"),
        ] {
            let tmp = tempfile::tempdir().unwrap();
            ColumnFile::write_batch(tmp.path(), &[row(), row()]).unwrap();
            let canonical = fs::read(tmp.path().join("canonical.bitmap")).unwrap();
            let path = tmp.path().join(name);
            let mut bytes = fs::read(&path).unwrap();
            match case {
                0 => {
                    bytes.pop();
                }
                1 => bytes.push(0),
                2 => bytes[4..8].copy_from_slice(&(COLUMN_VERSION + 1).to_le_bytes()),
                3 => bytes[16] = 1,
                _ => unreachable!(),
            }
            fs::write(&path, &bytes).unwrap();
            let error = ColumnFile::append_batch(tmp.path(), &[row()], 2)
                .expect_err("an invalid fixed-column layout must not be published");
            assert_eq!(error.kind(), io::ErrorKind::InvalidData, "{name}");
            assert_eq!(fs::read(&path).unwrap(), bytes, "{name}");
            assert_eq!(
                fs::read(tmp.path().join("canonical.bitmap")).unwrap(),
                canonical
            );
        }
    }

    #[test]
    fn absent_append_preserves_source_published_before_ownership() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("new-source");
        let writer_dir = dir.clone();
        let mut replacement = row();
        replacement.block_number = 777;
        let writer_row = replacement.clone();
        BEFORE_ABSENT_APPEND_CREATE.with_borrow_mut(|hook| {
            *hook = Some(Box::new(move || {
                ColumnFile::write_batch(&writer_dir, &[writer_row]).unwrap()
            }));
        });
        let result = ColumnFile::append_batch(&dir, &[row()], 0);
        assert_eq!(
            crate::SegmentReader::open(&dir)
                .unwrap()
                .read_log_rows(None)
                .unwrap(),
            vec![replacement],
            "append used a stale absence observation; returned {result:?}"
        );
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::WouldBlock);
    }

    #[test]
    fn absent_append_rejects_nonzero_expected_prefix_without_creating_directory() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("missing-source");
        assert!(
            ColumnFile::append_batch(&dir, &[row()], 1).is_err(),
            "append ignored its expected nonzero prefix"
        );
        assert!(!dir.exists());
    }

    #[test]
    fn absent_append_rejects_pending_and_partial_sources_before_initialization() {
        use std::cell::RefCell;
        use std::rc::Rc;

        for pending in [false, true] {
            let tmp = tempfile::tempdir().unwrap();
            let dir = tmp.path().join("new-source");
            let writer_dir = dir.clone();
            let artifact = if pending {
                SOURCE_MARKER_FILE
            } else {
                "address.col"
            };
            let original = Rc::new(RefCell::new(Vec::new()));
            let captured = Rc::clone(&original);
            BEFORE_ABSENT_APPEND_CREATE.with_borrow_mut(|hook| {
                *hook = Some(Box::new(move || {
                    let _owner = SourceWriteGuard::create_and_acquire(&writer_dir).unwrap();
                    if pending {
                        write_source_marker(
                            &writer_dir,
                            SourceMarker::new(
                                SourceIdentity {
                                    namespace: [8; 16],
                                    generation: 0,
                                    segment_id: u64::MAX,
                                    kind: SegmentKind::Hot,
                                },
                                SOURCE_UPDATING,
                                0,
                            ),
                            durability::Publication::Ordered,
                        )
                        .unwrap();
                    } else {
                        fs::write(writer_dir.join(artifact), b"partial column").unwrap();
                    }
                    *captured.borrow_mut() = fs::read(writer_dir.join(artifact)).unwrap();
                }));
            });
            assert_eq!(
                ColumnFile::append_batch(&dir, &[row()], 0)
                    .unwrap_err()
                    .kind(),
                io::ErrorKind::WouldBlock
            );
            assert_eq!(fs::read(dir.join(artifact)).unwrap(), *original.borrow());
            assert_eq!(fs::read_dir(&dir).unwrap().count(), 1);
        }
    }

    #[test]
    fn existing_source_ownership_validates_directory_and_excludes_other_owners() {
        let tmp = tempfile::tempdir().unwrap();
        let owner = SourceWriteGuard::acquire_existing(tmp.path()).unwrap();
        assert_eq!(
            SourceWriteGuard::acquire_existing(tmp.path())
                .err()
                .unwrap()
                .kind(),
            io::ErrorKind::WouldBlock
        );
        drop(owner);
        drop(SourceWriteGuard::acquire_existing(tmp.path()).unwrap());
        let file = tmp.path().join("regular-file");
        fs::write(&file, b"preserve").unwrap();
        assert_eq!(
            SourceWriteGuard::acquire_existing(&file)
                .err()
                .unwrap()
                .kind(),
            io::ErrorKind::NotADirectory
        );
        assert!(ColumnFile::write_batch(&file, &[row()]).is_err());
        assert_eq!(fs::read(file).unwrap(), b"preserve");
    }

    #[test]
    fn existing_source_operations_do_not_create_missing_nonzero_sources() {
        let tmp = tempfile::tempdir().unwrap();
        let missing = tmp.path().join("missing");
        assert_eq!(
            SourceWriteGuard::acquire_bound(&missing, [7; 16], 0, 1)
                .err()
                .unwrap()
                .kind(),
            io::ErrorKind::NotFound
        );
        assert_eq!(
            SourceWriteGuard::acquire_legacy(&missing)
                .err()
                .unwrap()
                .kind(),
            io::ErrorKind::NotFound
        );
        assert_eq!(
            begin_prefix_recovery(&missing, [7; 16], 1, 0, 1, SegmentKind::Hot, None)
                .err()
                .unwrap()
                .kind(),
            io::ErrorKind::NotFound
        );
        assert_eq!(
            ColumnFile::append_batch_with_source_binding(
                &missing,
                &[row()],
                1,
                durability::Publication::Ordered,
                SourceBinding {
                    namespace: [7; 16],
                    generation: 0,
                    segment_id: 1
                },
                &crate::commitment::AppendRevision::new(None, &[row()]).unwrap()
            )
            .unwrap_err()
            .kind(),
            io::ErrorKind::NotFound
        );
        assert!(!missing.exists());
    }

    #[test]
    fn append_initializes_an_absent_source_without_reacquiring_ownership() {
        let parent = tempfile::tempdir().unwrap();
        let dir = parent.path().join("new-source");
        ColumnFile::append_batch(&dir, &[row()], 0).unwrap();
        let namespace = read_source_namespace(&dir).unwrap();
        ColumnFile::append_batch(&dir, &[row()], 1).unwrap();
        assert_eq!(read_source_namespace(&dir).unwrap(), namespace);
        assert_eq!(
            crate::SegmentReader::open(&dir)
                .unwrap()
                .read_log_rows(None)
                .unwrap(),
            vec![row(), row()]
        );
    }

    #[test]
    fn test_column_header_roundtrip() {
        let header = ColumnFileHeader {
            version: 1,
            row_count: 42,
            compression: 0,
        };
        let mut buf = Vec::new();
        header.write_to(&mut buf).unwrap();
        assert_eq!(buf.len(), ColumnFileHeader::SIZE);

        let parsed = ColumnFileHeader::read_from(&buf).unwrap();
        assert_eq!(parsed.version, 1);
        assert_eq!(parsed.row_count, 42);
        assert_eq!(parsed.compression, 0);
    }

    #[test]
    fn test_null_bitmap_roundtrip() {
        let mut bitmap = NullBitmap::new();
        bitmap.push(true);
        bitmap.push(false);
        bitmap.push(true);
        bitmap.push(true);
        bitmap.push(false);

        assert!(bitmap.is_present(0));
        assert!(!bitmap.is_present(1));
        assert!(bitmap.is_present(2));
        assert!(bitmap.is_present(3));
        assert!(!bitmap.is_present(4));
        assert_eq!(bitmap.len(), 5);

        let mut buf = Vec::new();
        bitmap.write_to(&mut buf).unwrap();

        let parsed = NullBitmap::read_from(&buf).unwrap();
        assert_eq!(parsed.len(), 5);
        assert!(parsed.is_present(0));
        assert!(!parsed.is_present(1));
        assert!(parsed.is_present(2));
        assert!(parsed.is_present(3));
        assert!(!parsed.is_present(4));
    }

    #[test]
    fn bitmap_padding_cannot_become_present_rows_after_append() {
        for len in 1u64..16 {
            let mut encoded = len.to_le_bytes().to_vec();
            encoded.resize(8 + len.div_ceil(8) as usize, 0xff);
            let mut bitmap = NullBitmap::read_from(&encoded).unwrap();
            for row in 0..len {
                assert!(bitmap.is_present(row));
            }
            for row in len..len + 16 {
                bitmap.push(false);
                assert!(!bitmap.is_present(row), "len {len}, appended row {row}");
            }
            assert!(!bitmap.is_present(u64::MAX));
        }
    }

    #[test]
    fn bitmap_out_of_range_set_cannot_change_future_rows() {
        let mut bitmap = NullBitmap::new();
        bitmap.push(true);
        for row in [1, 7, 8, u64::MAX] {
            bitmap.set(row, true);
            assert!(!bitmap.is_present(row));
        }
        for row in 1..16 {
            bitmap.push(false);
            assert!(!bitmap.is_present(row));
        }
        bitmap.set(0, false);
        bitmap.set(15, true);
        assert!(!bitmap.is_present(0));
        assert!(bitmap.is_present(15));
        assert!(NullBitmap::read_from(&u64::MAX.to_le_bytes()).is_none());
    }

    #[test]
    fn source_marker_distinguishes_missing_committed_and_interrupted_state() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(read_source_namespace(dir.path()).unwrap(), None);
        let namespace = [7; 16];
        write_source_marker(
            dir.path(),
            SourceMarker::new(
                SourceIdentity {
                    namespace,
                    generation: 0,
                    segment_id: u64::MAX,
                    kind: SegmentKind::Hot,
                },
                SOURCE_COMMITTED,
                0,
            ),
            durability::Publication::Deferred,
        )
        .unwrap();
        assert_eq!(read_source_namespace(dir.path()).unwrap(), Some(namespace));
        assert!(
            begin_prefix_recovery(dir.path(), [8; 16], 0, 0, 1, SegmentKind::Hot, None).is_err()
        );
        write_source_marker(
            dir.path(),
            SourceMarker::new(
                SourceIdentity {
                    namespace,
                    generation: 0,
                    segment_id: u64::MAX,
                    kind: SegmentKind::Hot,
                },
                SOURCE_UPDATING,
                0,
            ),
            durability::Publication::Deferred,
        )
        .unwrap();
        assert_eq!(
            read_source_namespace(dir.path()).unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );

        write_source_marker(
            dir.path(),
            SourceMarker::new(
                SourceIdentity {
                    namespace,
                    generation: 4,
                    segment_id: 9,
                    kind: SegmentKind::Sealed,
                },
                SOURCE_PREFIX_REWRITE,
                12,
            ),
            durability::Publication::Deferred,
        )
        .unwrap();
        assert!(
            begin_prefix_recovery(dir.path(), namespace, 11, 4, 9, SegmentKind::Sealed, None)
                .is_err()
        );
        assert!(
            begin_prefix_recovery(dir.path(), namespace, 12, 4, 10, SegmentKind::Sealed, None)
                .is_err()
        );
        assert_eq!(
            crate::SegmentReader::open(dir.path()).unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );
        assert_eq!(
            read_source_namespace(dir.path()).unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );
        let owner =
            begin_prefix_recovery(dir.path(), namespace, 12, 4, 9, SegmentKind::Sealed, None)
                .unwrap();
        assert!(
            ColumnFile::rewrite_verified_prefix(
                dir.path(),
                &[],
                &NullBitmap::new(),
                namespace,
                4,
                &owner,
            )
            .is_err()
        );
        assert_eq!(
            read_source_namespace(dir.path()).unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );
        assert!(ColumnFile::finish_verified_prefix(dir.path(), &owner).is_err());
        assert_eq!(
            read_source_namespace(dir.path()).unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );
        drop(owner);
    }

    #[test]
    fn native_initial_write_rejects_nonzero_or_incomplete_source_without_mutation() {
        let dir = tempfile::tempdir().unwrap();
        let identity = SourceIdentity {
            namespace: [9; 16],
            generation: 3,
            segment_id: 7,
            kind: SegmentKind::Hot,
        };
        let sentinel = dir.path().join("sentinel");
        fs::write(&sentinel, b"unchanged").unwrap();

        for marker in [
            SourceMarker::new(identity, SOURCE_COMMITTED, 1),
            SourceMarker::new(identity, SOURCE_UPDATING, 0),
        ] {
            write_source_marker(dir.path(), marker, durability::Publication::Deferred).unwrap();
            let marker_before = fs::read(dir.path().join(SOURCE_MARKER_FILE)).unwrap();
            let error = ColumnFile::write_initial_batch_with_source_identity(
                dir.path(),
                &[row()],
                None,
                durability::Publication::Deferred,
                identity,
                Some(&PrefixState::from_rows(identity.namespace, &[row()]).unwrap()),
            )
            .unwrap_err();
            assert_eq!(error.kind(), io::ErrorKind::WouldBlock);
            assert_eq!(
                fs::read(dir.path().join(SOURCE_MARKER_FILE)).unwrap(),
                marker_before
            );
            assert_eq!(fs::read(&sentinel).unwrap(), b"unchanged");
            assert!(!dir.path().join("address.col").exists());
        }
    }

    #[test]
    fn source_marker_rejects_truncated_and_oversized_files() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(SOURCE_MARKER_FILE);
        let identity = SourceIdentity {
            namespace: [4; 16],
            generation: 2,
            segment_id: 3,
            kind: SegmentKind::Hot,
        };
        write_source_marker(
            dir.path(),
            SourceMarker::new(identity, SOURCE_COMMITTED, 0),
            durability::Publication::Deferred,
        )
        .unwrap();
        let exact = fs::read(&path).unwrap();
        assert_eq!(
            read_source_namespace(dir.path()).unwrap(),
            Some(identity.namespace)
        );

        fs::write(&path, &exact[..exact.len() - 1]).unwrap();
        assert_eq!(
            read_source_namespace(dir.path()).unwrap_err().kind(),
            io::ErrorKind::UnexpectedEof
        );
        let mut oversized = exact;
        oversized.push(0);
        fs::write(path, oversized).unwrap();
        assert_eq!(
            read_source_namespace(dir.path()).unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
    }
}
