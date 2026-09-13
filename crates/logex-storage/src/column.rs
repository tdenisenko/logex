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
        })
    }

    pub(crate) fn acquire_bound(
        dir: &Path,
        namespace: [u8; 16],
        generation: u64,
        segment_id: u64,
    ) -> io::Result<Self> {
        let owner = Self::acquire(dir)?;
        verify_owned_source(dir, namespace, generation, segment_id)?;
        Ok(owner)
    }

    pub(crate) fn acquire_legacy(dir: &Path) -> io::Result<Self> {
        let owner = Self::acquire(dir)?;
        // Legacy native sources remain scan-readable but cannot acquire a
        // trusted identity from their shape or an incidental standalone marker.
        read_source_namespace(dir)?;
        Ok(owner)
    }
}

fn verify_owned_source(
    dir: &Path,
    namespace: [u8; 16],
    generation: u64,
    segment_id: u64,
) -> io::Result<()> {
    match read_source_marker(dir)? {
        Some(marker)
            if marker.state == SOURCE_COMMITTED
                && marker.namespace == namespace
                && marker.generation == generation
                && marker.segment_id == segment_id =>
        {
            Ok(())
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
    let mut bytes = [0; SOURCE_MARKER_BYTES];
    file.read_exact(&mut bytes)?;
    let mut trailing = [0; 1];
    if file.read(&mut trailing)? != 0
        || bytes.get(..8) != Some(SOURCE_MARKER_MAGIC.as_slice())
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

pub(crate) fn read_source_namespace(dir: &Path) -> io::Result<Option<[u8; 16]>> {
    Ok(read_source_binding(dir)?.map(|binding| binding.namespace))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SourceBinding {
    pub(crate) namespace: [u8; 16],
    pub(crate) generation: u64,
    pub(crate) segment_id: u64,
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
        Self::write_batch_contents(dir, rows, Some(canonical), durability::Publication::Durable)?;
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
        Self::write_batch_contents(dir, rows, Some(canonical), durability::Publication::Durable)
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
        Self::write_batch_contents(dir, rows, canonical, publication)?;
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
                    bitmap.write_to(writer)
                })?;
                Ok(())
            });
            join_write_worker(block_columns)?;
            join_write_worker(transaction_columns)?;
            join_write_worker(remaining_topics)?;
            join_write_worker(variable_columns)?;
            Ok::<_, io::Error>(())
        })?;

        replacements.publish()
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
        read_source_namespace(dir)?;
        Self::append_batch_owned(dir, rows, existing_rows, publication)
    }

    fn append_batch_owned(
        dir: &Path,
        rows: &[LogRow],
        existing_rows: u64,
        publication: durability::Publication,
    ) -> io::Result<()> {
        let new_row_count = existing_rows + rows.len() as u64;
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
                Self::append_canonical_bitmap(
                    dir,
                    &replacements,
                    existing_rows,
                    rows.len() as u64,
                )?;
                Ok(())
            });
            join_write_worker(block_columns)?;
            join_write_worker(transaction_columns)?;
            join_write_worker(remaining_topics)?;
            join_write_worker(variable_columns)?;
            Ok::<_, io::Error>(())
        })?;

        replacements.publish()
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
        verify_owned_source(dir, namespace, generation, segment_id)?;
        Self::append_batch_owned(dir, rows, existing_rows, publication)
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
        durability::atomic_replace_ordered(&dir.join("canonical.bitmap"), |writer| {
            bitmap.write_to(writer)
        })
    }

    #[cfg(test)]
    pub(crate) fn replace_canonical_bitmap(dir: &Path, bitmap: &NullBitmap) -> io::Result<()> {
        let owner = SourceWriteGuard::acquire(dir)?;
        read_source_namespace(dir)?;
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
            bitmap.write_to(writer)
        })
    }

    fn append_canonical_bitmap(
        dir: &Path,
        replacements: &durability::ReplacementBatch,
        existing_rows: u64,
        new_rows: u64,
    ) -> io::Result<()> {
        let path = dir.join("canonical.bitmap");
        let data = fs::read(&path)?;
        let mut bitmap = NullBitmap::read_from(&data).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "corrupt canonical bitmap")
        })?;
        if bitmap.len() != existing_rows {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "canonical bitmap row count mismatch: expected {existing_rows}, got {}",
                    bitmap.len()
                ),
            ));
        }

        for _ in 0..new_rows {
            bitmap.push(true);
        }

        replacements.write(&path, |w| bitmap.write_to(w))?;
        Ok(())
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
}
