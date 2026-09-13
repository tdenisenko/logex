use crate::durability;
use std::fs::{self, File, OpenOptions, TryLockError};
use std::io::{self, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::thread;

use crate::native::SegmentKind;
use logex_types::LogRow;

const SOURCE_MARKER_FILE: &str = ".source-publication";
const SOURCE_MARKER_MAGIC: &[u8; 8] = b"LXSRC001";
const SOURCE_MARKER_BYTES: usize = 8 + 1 + 16 + 8 + 8 + 8 + 1 + 4;
const SOURCE_UPDATING: u8 = 1;
const SOURCE_PREFIX_REWRITE: u8 = 2;
const SOURCE_COMMITTED: u8 = 3;
const CANONICAL_ENVELOPE_MAGIC: &[u8; 8] = b"LXCAN001";
const CANONICAL_ENVELOPE_VERSION: u8 = 1;
const CANONICAL_PENDING: u8 = 1;
const CANONICAL_COMMITTED: u8 = 2;
const CANONICAL_ENVELOPE_HEADER: usize = 8 + 1 + 1 + 16 + 8 + 8 + 8 + 4;
pub(crate) const CANONICAL_PREFIX_BYTES: usize = CANONICAL_ENVELOPE_HEADER + 8;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SourceMarker {
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
    fn new(identity: SourceIdentity, state: u8, prefix_rows: u64) -> Self {
        Self {
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
    match read_source_marker(dir)? {
        Some(marker)
            if marker.state == SOURCE_PREFIX_REWRITE
                && marker.namespace == owner.namespace
                && marker.prefix_rows == owner.prefix_rows
                && marker.generation == owner.generation
                && marker.segment_id == owner.segment_id
                && marker.kind == owner.kind =>
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
    fn acquire(dir: &Path) -> io::Result<Self> {
        fs::create_dir_all(dir)?;
        // Use the segment directory inode, matching native maintenance
        // ownership without creating another persistent lock namespace.
        let file = File::open(dir)?;
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
        let mut owner = Self::acquire(dir)?;
        owner.binding = Some(verify_owned_source(dir, namespace, generation, segment_id)?);
        Ok(owner)
    }

    pub(crate) fn acquire_legacy(dir: &Path) -> io::Result<Self> {
        let mut owner = Self::acquire(dir)?;
        // Legacy native sources remain scan-readable but cannot acquire a
        // trusted identity from their shape or an incidental standalone marker.
        owner.binding = read_source_binding(dir)?;
        Ok(owner)
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
    if marker_len < SOURCE_MARKER_BYTES as u64 {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "truncated raw source publication marker",
        ));
    }
    if marker_len > SOURCE_MARKER_BYTES as u64 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "oversized raw source publication marker",
        ));
    }
    let mut bytes = [0; SOURCE_MARKER_BYTES];
    file.read_exact(&mut bytes)?;
    if bytes.get(..8) != Some(SOURCE_MARKER_MAGIC.as_slice())
        || crc32fast::hash(&bytes[..50]).to_le_bytes() != bytes[50..]
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid raw source publication marker",
        ));
    }
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
/// The CRC covers the version, publication state, source binding and row boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RawCanonicalMetadata {
    binding: Option<SourceBinding>,
    pending: bool,
    pub(crate) rows: u64,
    offset: usize,
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
                binding: None,
                pending: false,
                rows,
                offset: 0,
            });
        }
        // A reserved magic prefix is always an envelope; corruption never falls
        // back to interpreting those bytes as a legacy row count.
        if prefix.len() < CANONICAL_PREFIX_BYTES
            || prefix[8] != CANONICAL_ENVELOPE_VERSION
            || !matches!(prefix[9], CANONICAL_PENDING | CANONICAL_COMMITTED)
            || crc32fast::hash(&prefix[..CANONICAL_ENVELOPE_HEADER - 4]).to_le_bytes()
                != prefix[CANONICAL_ENVELOPE_HEADER - 4..CANONICAL_ENVELOPE_HEADER]
        {
            return Err(invalid());
        }
        let rows = read_u64(42)?;
        if rows > u64::from(u32::MAX)
            || read_u64(CANONICAL_ENVELOPE_HEADER)? != rows
            || file_len != CANONICAL_PREFIX_BYTES as u64 + rows.div_ceil(8)
        {
            return Err(invalid());
        }
        Ok(Self {
            binding: Some(SourceBinding {
                namespace: prefix[10..26].try_into().map_err(|_| invalid())?,
                generation: read_u64(26)?,
                segment_id: read_u64(34)?,
            }),
            pending: prefix[9] == CANONICAL_PENDING,
            rows,
            offset: CANONICAL_ENVELOPE_HEADER,
        })
    }

    pub(crate) fn validate_exact_len(self, file_len: u64) -> io::Result<Self> {
        if file_len != self.offset as u64 + 8 + self.rows.div_ceil(8) {
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
        if self.pending && self.rows != owner.prefix_rows {
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

    pub(crate) fn bitmap(self, bytes: &[u8]) -> io::Result<NullBitmap> {
        if Self::parse(bytes, bytes.len() as u64)? != self {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "captured canonical metadata changed",
            ));
        }
        NullBitmap::read_from(&bytes[self.offset..])
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "corrupt canonical bitmap"))
    }
}

pub(crate) fn write_raw_canonical(
    writer: &mut dyn Write,
    bitmap: &NullBitmap,
    binding: Option<SourceBinding>,
) -> io::Result<()> {
    write_raw_canonical_state(writer, bitmap, binding, CANONICAL_COMMITTED)
}

fn write_raw_canonical_state(
    writer: &mut dyn Write,
    bitmap: &NullBitmap,
    binding: Option<SourceBinding>,
    state: u8,
) -> io::Result<()> {
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
        let checksum = crc32fast::hash(&header[..50]);
        header[50..54].copy_from_slice(&checksum.to_le_bytes());
        writer.write_all(&header)?;
    }
    bitmap.write_to(writer)
}

fn publish_pending_canonical(
    dir: &Path,
    bitmap: &NullBitmap,
    binding: SourceBinding,
) -> io::Result<()> {
    let mut bytes = Vec::new();
    write_raw_canonical_state(&mut bytes, bitmap, Some(binding), CANONICAL_PENDING)?;
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
) -> io::Result<PrefixRecoveryGuard> {
    let owner = SourceWriteGuard::acquire(dir)?;
    match read_source_marker(dir)? {
        Some(marker)
            if marker.state == SOURCE_PREFIX_REWRITE
                && marker.namespace == namespace
                && marker.prefix_rows == prefix_rows
                && marker.generation == generation
                && marker.segment_id == segment_id
                && marker.kind == kind =>
        {
            Ok(PrefixRecoveryGuard {
                _owner: owner,
                dir: dir.to_path_buf(),
                namespace,
                prefix_rows,
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
) -> io::Result<bool> {
    match read_source_marker(dir)? {
        Some(marker)
            if marker.state == SOURCE_PREFIX_REWRITE
                && marker.namespace == namespace
                && marker.prefix_rows == prefix_rows
                && marker.generation == generation
                && marker.segment_id == segment_id
                && marker.kind == kind =>
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
    let checksum = crc32fast::hash(&bytes[..50]).to_le_bytes();
    bytes[50..].copy_from_slice(&checksum);
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
        let _owner = SourceWriteGuard::acquire(dir)?;
        let mut namespace = [0; 16];
        getrandom::fill(&mut namespace).map_err(|error| io::Error::other(error.to_string()))?;
        Self::write_batch_with_owned_source(
            dir,
            rows,
            canonical,
            publication,
            durability::Publication::Ordered,
            durability::Publication::Durable,
            SourceIdentity {
                namespace,
                generation: 0,
                segment_id: u64::MAX,
                kind: SegmentKind::Hot,
            },
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
    ) -> io::Result<()> {
        let _owner = SourceWriteGuard::acquire(dir)?;
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
            durability::Publication::Deferred,
            durability::Publication::Deferred,
            identity,
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
            ),
            durability::Publication::Ordered,
        )?;
        let binding = SourceBinding {
            namespace,
            generation,
            segment_id: owner.segment_id,
        };
        publish_pending_canonical(dir, canonical, binding)?;
        Self::write_batch_contents(
            dir,
            rows,
            Some(canonical),
            durability::Publication::Durable,
            Some(binding),
            true,
        )?;
        owner.completed.set(true);
        Ok(())
    }

    pub(crate) fn rewrite_unidentified_prefix(
        dir: &Path,
        rows: &[LogRow],
        canonical: &NullBitmap,
    ) -> io::Result<()> {
        if canonical.len() != rows.len() as u64 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "legacy prefix bitmap length differs from its rows",
            ));
        }
        let _owner = SourceWriteGuard::acquire(dir)?;
        Self::write_batch_contents(
            dir,
            rows,
            Some(canonical),
            durability::Publication::Durable,
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
        pending_publication: durability::Publication,
        marker_publication: durability::Publication,
        identity: SourceIdentity,
    ) -> io::Result<()> {
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
        fs::create_dir_all(dir)?;
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
            publish_pending_canonical(dir, &NullBitmap::new(), identity.into())?;
        }
        Self::write_batch_contents(
            dir,
            rows,
            canonical,
            publication,
            Some(identity.into()),
            pending_publication != durability::Publication::Deferred,
        )?;
        write_source_marker(
            dir,
            SourceMarker::new(identity, SOURCE_COMMITTED, rows.len() as u64),
            marker_publication,
        )
    }

    fn write_batch_contents(
        dir: &Path,
        rows: &[LogRow],
        canonical: Option<&NullBitmap>,
        publication: durability::Publication,
        binding: Option<SourceBinding>,
        order_names: bool,
    ) -> io::Result<()> {
        fs::create_dir_all(dir)?;
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
                    write_raw_canonical(writer, bitmap, binding)
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
        let initialize = !dir.try_exists()?;
        let _owner = SourceWriteGuard::acquire(dir)?;
        if initialize {
            let mut namespace = [0; 16];
            getrandom::fill(&mut namespace).map_err(|error| io::Error::other(error.to_string()))?;
            return Self::write_batch_with_owned_source(
                dir,
                rows,
                None,
                publication,
                durability::Publication::Ordered,
                durability::Publication::Durable,
                SourceIdentity {
                    namespace,
                    generation: 0,
                    segment_id: u64::MAX,
                    kind: SegmentKind::Hot,
                },
            );
        }
        let binding = read_source_binding(dir)?;
        Self::append_batch_owned(dir, rows, existing_rows, publication, binding)
    }

    fn append_batch_owned(
        dir: &Path,
        rows: &[LogRow],
        existing_rows: u64,
        publication: durability::Publication,
        binding: Option<SourceBinding>,
    ) -> io::Result<()> {
        // Validate the immutable canonical prefix before any in-place column
        // append. Reuse this one read in the existing replacement worker.
        let data = fs::read(dir.join("canonical.bitmap"))?;
        let mut canonical = read_canonical_bitmap(&data, binding)?;
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
        for _ in rows {
            canonical.push(true);
        }
        let replacements = durability::ReplacementBatch::new(publication);

        thread::scope(|scope| {
            let block_columns = scope.spawn(|| {
                Self::append_fixed_col(
                    dir,
                    "address.col",
                    existing_rows,
                    new_row_count,
                    rows,
                    |w, r| w.write_all(r.address.as_slice()),
                )?;
                Self::append_fixed_col(
                    dir,
                    "block_number.col",
                    existing_rows,
                    new_row_count,
                    rows,
                    |w, r| w.write_all(&r.block_number.to_le_bytes()),
                )?;
                Self::append_fixed_col(
                    dir,
                    "block_hash.col",
                    existing_rows,
                    new_row_count,
                    rows,
                    |w, r| w.write_all(r.block_hash.as_slice()),
                )?;
                Self::append_fixed_col(
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
                Self::append_fixed_col(
                    dir,
                    "tx_hash.col",
                    existing_rows,
                    new_row_count,
                    rows,
                    |w, r| w.write_all(r.tx_hash.as_slice()),
                )?;
                Self::append_fixed_col(
                    dir,
                    "tx_index.col",
                    existing_rows,
                    new_row_count,
                    rows,
                    |w, r| w.write_all(&r.tx_index.to_le_bytes()),
                )?;
                Self::append_fixed_col(
                    dir,
                    "log_index.col",
                    existing_rows,
                    new_row_count,
                    rows,
                    |w, r| w.write_all(&r.log_index.to_le_bytes()),
                )?;
                Self::append_fixed_col(
                    dir,
                    "data_len.col",
                    existing_rows,
                    new_row_count,
                    rows,
                    |w, r| w.write_all(&r.data_len.to_le_bytes()),
                )?;
                Self::append_fixed_col(
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
                    write_raw_canonical(writer, &canonical, binding)
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
        namespace: [u8; 16],
        generation: u64,
        segment_id: u64,
    ) -> io::Result<()> {
        let _owner = SourceWriteGuard::acquire(dir)?;
        let binding = verify_owned_source(dir, namespace, generation, segment_id)?;
        Self::append_batch_owned(dir, rows, existing_rows, publication, Some(binding))
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

    fn append_fixed_col(
        dir: &Path,
        name: &str,
        existing_rows: u64,
        new_row_count: u64,
        rows: &[LogRow],
        mut write_value: impl FnMut(&mut BufWriter<File>, &LogRow) -> io::Result<()>,
    ) -> io::Result<()> {
        let path = dir.join(name);
        let file = Self::open_col_for_append(&path, existing_rows, new_row_count)?;
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

        let col_file = Self::open_col_for_append(&col_path, existing_rows, new_row_count)?;
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
        _existing_rows: u64,
    ) -> io::Result<()> {
        let path = dir.join(name);
        let data = fs::read(&path)?;

        let header = ColumnFileHeader::read_from(&data)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "corrupt data column"))?;
        let old_count = header.row_count as usize;
        if header.row_count != _existing_rows {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "data column row count mismatch: expected {_existing_rows}, got {}",
                    header.row_count
                ),
            ));
        }

        // Read existing offsets
        let offset_start = ColumnFileHeader::SIZE;
        let offsets_size = (old_count + 1) * 8;
        let data_start = offset_start + offsets_size;

        let mut old_offsets = Vec::with_capacity(old_count + 1);
        for i in 0..=old_count {
            let pos = offset_start + i * 8;
            let end = pos + 8;
            let o = u64::from_le_bytes(
                data.get(pos..end)
                    .ok_or_else(|| {
                        io::Error::new(io::ErrorKind::InvalidData, "truncated offset array")
                    })?
                    .try_into()
                    .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid offset"))?,
            );
            old_offsets.push(o);
        }
        let existing_data = &data[data_start..];
        let existing_data_len = old_offsets.last().copied().unwrap_or(0);

        // Compute new offsets
        let mut new_offsets = Vec::with_capacity(rows.len() + 1);
        let mut off = existing_data_len;
        for row in rows {
            new_offsets.push(off);
            off += row.data.len() as u64;
        }
        new_offsets.push(off);

        replacements.write(&path, |w| {
            let new_header = ColumnFileHeader {
                version: COLUMN_VERSION,
                row_count: new_row_count,
                compression: 0,
            };
            new_header.write_to(w)?;

            // All offsets: old (without sentinel) + new (with sentinel)
            for o in &old_offsets[..old_count] {
                w.write_all(&o.to_le_bytes())?;
            }
            for o in &new_offsets {
                w.write_all(&o.to_le_bytes())?;
            }

            // All data
            w.write_all(existing_data)?;
            for row in rows {
                w.write_all(&row.data)?;
            }
            Ok(())
        })?;
        Ok(())
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
        let mut owner = SourceWriteGuard::acquire(dir)?;
        owner.binding = read_source_binding(dir)?;
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
        durability::atomic_write(&dir.join("canonical.bitmap"), |writer| {
            write_raw_canonical(writer, bitmap, owner.binding)
        })
    }

    fn open_col_for_append(
        path: &Path,
        existing_rows: u64,
        new_row_count: u64,
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
        write_raw_canonical(&mut bytes, &bitmap, Some(binding)).unwrap();
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
        write_raw_canonical_state(&mut pending, &bitmap, Some(binding), CANONICAL_PENDING).unwrap();
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
        bytes[54..62].copy_from_slice(&oversized.to_le_bytes());
        let crc = crc32fast::hash(&bytes[..50]);
        bytes[50..54].copy_from_slice(&crc.to_le_bytes());
        assert!(RawCanonicalMetadata::parse(&bytes, 62 + oversized.div_ceil(8)).is_err());
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
        assert!(begin_prefix_recovery(dir.path(), [8; 16], 0, 0, 1, SegmentKind::Hot,).is_err());
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
            begin_prefix_recovery(dir.path(), namespace, 11, 4, 9, SegmentKind::Sealed).is_err()
        );
        assert!(
            begin_prefix_recovery(dir.path(), namespace, 12, 4, 10, SegmentKind::Sealed).is_err()
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
            begin_prefix_recovery(dir.path(), namespace, 12, 4, 9, SegmentKind::Sealed).unwrap();
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
