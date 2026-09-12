//! Integrity framing for immutable derived files. Page checks detect accidental
//! changes without scanning the whole index for each point lookup. They do not
//! authenticate a file or establish that its rows describe a particular segment.
use std::fs::File;
use std::io::{self, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::Path;

const MAGIC: &[u8; 8] = b"LXIDX001";
const VERSION: u32 = 1;
const HEADER_BYTES: usize = 48;
const PAGE_BYTES: usize = 4096;
const CHECKSUM_BYTES: u64 = 4;

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

fn physical_len(logical_len: u64) -> io::Result<u64> {
    logical_len
        .div_ceil(PAGE_BYTES as u64)
        .checked_mul(CHECKSUM_BYTES)
        .and_then(|checksums| checksums.checked_add(logical_len))
        .and_then(|body| body.checked_add(HEADER_BYTES as u64))
        .ok_or_else(|| invalid("index file length overflow"))
}

fn page_hasher(logical_len: u64, file_id: &[u8; 16], index: u64) -> crc32fast::Hasher {
    let mut hash = crc32fast::Hasher::new();
    hash.update(b"LogEx index page");
    hash.update(&VERSION.to_le_bytes());
    hash.update(&logical_len.to_le_bytes());
    hash.update(file_id);
    hash.update(&index.to_le_bytes());
    let length = (logical_len - index * PAGE_BYTES as u64).min(PAGE_BYTES as u64);
    hash.update(&length.to_le_bytes());
    hash
}

fn read_at(file: &File, bytes: &mut [u8], offset: u64) -> io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::FileExt;
        file.read_exact_at(bytes, offset)
    }
    #[cfg(not(unix))]
    {
        let mut file = file;
        file.seek(SeekFrom::Start(offset))?;
        file.read_exact(bytes)
    }
}

/// One opened handle and bounded caches of verified bytes. Index publication
/// locks must remain held while callers use this reader; files are not rewritten
/// in place while a published reader exists.
pub(crate) struct IndexFile {
    file: File,
    logical_len: u64,
    protected: bool,
    file_id: [u8; 16],
    position: u64,
    page: Box<[u8; PAGE_BYTES]>,
    page_index: Option<u64>,
    page_len: usize,
    checksums: Box<[u8; PAGE_BYTES]>,
    checksum_start: Option<u64>,
    checksum_len: usize,
}

impl IndexFile {
    pub(crate) fn open(path: &Path) -> io::Result<Self> {
        let file = File::open(path)?;
        let length = file.metadata()?.len();
        let mut header = [0; HEADER_BYTES];
        let header_len = length.min(HEADER_BYTES as u64) as usize;
        read_at(&file, &mut header[..header_len], 0)?;
        let protected = &header[..8] == MAGIC;
        let logical_len = if protected {
            if header_len != HEADER_BYTES
                || crc32fast::hash(&header[..44]).to_le_bytes() != header[44..]
                || header[8..12] != VERSION.to_le_bytes()
                || header[12..16] != (PAGE_BYTES as u32).to_le_bytes()
                || header[40..44] != [0; 4]
            {
                return Err(invalid("invalid index integrity header"));
            }
            let mut bytes = [0; 8];
            bytes.copy_from_slice(&header[16..24]);
            let logical_len = u64::from_le_bytes(bytes);
            if physical_len(logical_len)? != length {
                return Err(invalid("index integrity file length mismatch"));
            }
            logical_len
        } else {
            // Legacy bytes are exposed only to callers that explicitly validate
            // their old encoding. They have no persisted integrity guarantee.
            length
        };
        let mut file_id = [0; 16];
        file_id.copy_from_slice(&header[24..40]);
        Ok(Self {
            file,
            file_id,
            logical_len,
            protected,
            position: 0,
            page: Box::new([0; PAGE_BYTES]),
            page_index: None,
            page_len: 0,
            checksums: Box::new([0; PAGE_BYTES]),
            checksum_start: None,
            checksum_len: 0,
        })
    }

    pub(crate) fn logical_len(&self) -> u64 {
        self.logical_len
    }

    pub(crate) fn is_protected(&self) -> bool {
        self.protected
    }

    fn load_page(&mut self, index: u64) -> io::Result<()> {
        if self.page_index == Some(index) {
            return Ok(());
        }
        // Invalidate before any fallible read so a failed read cannot leave the
        // old cache identity attached to partially overwritten bytes.
        self.page_index = None;
        let start = index * PAGE_BYTES as u64;
        self.page_len = (self.logical_len - start).min(PAGE_BYTES as u64) as usize;
        read_at(
            &self.file,
            &mut self.page[..self.page_len],
            HEADER_BYTES as u64 + start,
        )?;
        let expected = self.page_checksum(index)?;
        let mut hash = page_hasher(self.logical_len, &self.file_id, index);
        hash.update(&self.page[..self.page_len]);
        if hash.finalize().to_le_bytes() != expected {
            return Err(invalid(
                "index page checksum mismatch; rebuild derived indexes",
            ));
        }
        self.page_index = Some(index);
        Ok(())
    }

    fn page_checksum(&mut self, index: u64) -> io::Result<[u8; 4]> {
        let checksum_offset = index * CHECKSUM_BYTES;
        let checksum_start = checksum_offset / PAGE_BYTES as u64 * PAGE_BYTES as u64;
        if self.checksum_start != Some(checksum_start) {
            self.checksum_start = None;
            let checksum_bytes = self.logical_len.div_ceil(PAGE_BYTES as u64) * CHECKSUM_BYTES;
            self.checksum_len = (checksum_bytes - checksum_start).min(PAGE_BYTES as u64) as usize;
            read_at(
                &self.file,
                &mut self.checksums[..self.checksum_len],
                HEADER_BYTES as u64 + self.logical_len + checksum_start,
            )?;
            self.checksum_start = Some(checksum_start);
        }
        let offset = (checksum_offset - checksum_start) as usize;
        let mut checksum = [0; 4];
        checksum.copy_from_slice(&self.checksums[offset..offset + 4]);
        Ok(checksum)
    }
}

impl Read for IndexFile {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        let length = self
            .logical_len
            .saturating_sub(self.position)
            .min(output.len() as u64) as usize;
        if length == 0 {
            return Ok(0);
        }
        if !self.protected {
            read_at(&self.file, &mut output[..length], self.position)?;
            self.position += length as u64;
            return Ok(length);
        }
        if length >= PAGE_BYTES && self.position.is_multiple_of(PAGE_BYTES as u64) {
            // Whole-index/range readers already provide a large destination.
            // Keep their contiguous read instead of issuing one syscall/page.
            let count = if self.position + length as u64 == self.logical_len {
                length
            } else {
                length / PAGE_BYTES * PAGE_BYTES
            };
            read_at(
                &self.file,
                &mut output[..count],
                HEADER_BYTES as u64 + self.position,
            )?;
            let first = self.position / PAGE_BYTES as u64;
            for (offset, page) in output[..count].chunks(PAGE_BYTES).enumerate() {
                let index = first + offset as u64;
                let mut hash = page_hasher(self.logical_len, &self.file_id, index);
                hash.update(page);
                if hash.finalize().to_le_bytes() != self.page_checksum(index)? {
                    return Err(invalid(
                        "index page checksum mismatch; rebuild derived indexes",
                    ));
                }
            }
            self.position += count as u64;
            return Ok(count);
        }
        // Large bitmap payloads need not start on a page boundary. Deliver their
        // first partial page so read_exact can use the contiguous bulk path for
        // the rest, instead of reading a large payload one page per syscall.
        let length = if length >= PAGE_BYTES {
            length.min(PAGE_BYTES - (self.position % PAGE_BYTES as u64) as usize)
        } else {
            length
        };
        let mut copied = 0;
        while copied < length {
            let index = self.position / PAGE_BYTES as u64;
            if let Err(error) = self.load_page(index) {
                // Report an already copied prefix as a short read. The next
                // read retries this page and reports its error without claiming
                // the bytes already delivered were never read.
                return if copied == 0 { Err(error) } else { Ok(copied) };
            }
            let offset = (self.position % PAGE_BYTES as u64) as usize;
            let count = (self.page_len - offset).min(length - copied);
            output[copied..copied + count].copy_from_slice(&self.page[offset..offset + count]);
            copied += count;
            self.position += count as u64;
        }
        Ok(copied)
    }
}

impl Seek for IndexFile {
    fn seek(&mut self, position: SeekFrom) -> io::Result<u64> {
        let position = match position {
            SeekFrom::Start(value) => Some(value),
            SeekFrom::Current(delta) => self.position.checked_add_signed(delta),
            SeekFrom::End(delta) => self.logical_len.checked_add_signed(delta),
        }
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "index seek out of range"))?;
        self.position = position;
        Ok(position)
    }
}

/// Stream logical bytes once, retaining only four checksum bytes per page.
/// Durability and publication ordering belong to IndexBuildCheckpoint.
pub(crate) fn write_index_file(
    path: &Path,
    logical_len: u64,
    write: impl FnOnce(&mut dyn Write) -> io::Result<()>,
) -> io::Result<()> {
    physical_len(logical_len)?;
    let count = usize::try_from(logical_len.div_ceil(PAGE_BYTES as u64))
        .map_err(|_| invalid("index checksum count exceeds address space"))?;
    let mut checksums = Vec::new();
    checksums
        .try_reserve_exact(count)
        .map_err(io::Error::other)?;
    let mut file_id = [0; 16];
    getrandom::fill(&mut file_id).map_err(|error| io::Error::other(error.to_string()))?;
    let mut output = BufWriter::new(File::create(path)?);
    let mut header = [0; HEADER_BYTES];
    header[..8].copy_from_slice(MAGIC);
    header[8..12].copy_from_slice(&VERSION.to_le_bytes());
    header[12..16].copy_from_slice(&(PAGE_BYTES as u32).to_le_bytes());
    header[16..24].copy_from_slice(&logical_len.to_le_bytes());
    header[24..40].copy_from_slice(&file_id);
    let checksum = crc32fast::hash(&header[..44]);
    header[44..].copy_from_slice(&checksum.to_le_bytes());
    output.write_all(&header)?;
    let mut writer = IndexWriter {
        output,
        logical_len,
        written: 0,
        hash: page_hasher(logical_len, &file_id, 0),
        file_id,
        checksums,
    };
    write(&mut writer)?;
    if writer.written != logical_len {
        return Err(invalid("index writer produced an incomplete logical file"));
    }
    if !logical_len.is_multiple_of(PAGE_BYTES as u64) {
        writer.checksums.push(writer.hash.finalize().to_le_bytes());
    }
    for checksum in writer.checksums {
        writer.output.write_all(&checksum)?;
    }
    writer.output.flush()
}

struct IndexWriter {
    output: BufWriter<File>,
    logical_len: u64,
    written: u64,
    hash: crc32fast::Hasher,
    file_id: [u8; 16],
    checksums: Vec<[u8; 4]>,
}

impl Write for IndexWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() as u64 > self.logical_len - self.written {
            return Err(invalid("index writer exceeded its declared logical length"));
        }
        self.output.write_all(bytes)?;
        let mut remaining = bytes;
        while !remaining.is_empty() {
            let available = PAGE_BYTES - (self.written % PAGE_BYTES as u64) as usize;
            let count = available.min(remaining.len());
            self.hash.update(&remaining[..count]);
            self.written += count as u64;
            remaining = &remaining[count..];
            if self.written.is_multiple_of(PAGE_BYTES as u64) {
                let next = page_hasher(
                    self.logical_len,
                    &self.file_id,
                    self.written / PAGE_BYTES as u64,
                );
                let hash = std::mem::replace(&mut self.hash, next);
                self.checksums.push(hash.finalize().to_le_bytes());
            }
        }
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.output.flush()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn data() -> Vec<u8> {
        (0..PAGE_BYTES * 3 + 123).map(|i| (i / 97) as u8).collect()
    }

    fn write(path: &Path, data: &[u8]) {
        write_index_file(path, data.len() as u64, |writer| {
            // Exercise page boundaries inside and between writes.
            for part in data.chunks(137) {
                writer.write_all(part)?;
            }
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn protected_files_preserve_small_large_and_cross_page_reads() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("index");
        let original = data();
        for length in [
            0,
            1,
            PAGE_BYTES - 1,
            PAGE_BYTES,
            PAGE_BYTES + 1,
            original.len(),
        ] {
            let expected = &original[..length];
            write(&path, expected);
            assert_eq!(
                fs::metadata(&path).unwrap().len(),
                physical_len(length as u64).unwrap()
            );
            let mut reader = IndexFile::open(&path).unwrap();
            assert!(reader.is_protected());
            let mut all = vec![0; length];
            reader.read_exact(&mut all).unwrap();
            assert_eq!(all, expected);
            assert_eq!(reader.read(&mut [0]).unwrap(), 0);
            for start in [0, 1, PAGE_BYTES - 3, PAGE_BYTES, length.saturating_sub(1)] {
                if start > length {
                    continue;
                }
                reader.seek(SeekFrom::Start(start as u64)).unwrap();
                let count = 29.min(length - start);
                let mut selected = vec![0; count];
                reader.read_exact(&mut selected).unwrap();
                assert_eq!(selected, expected[start..start + count]);
            }
            assert!(reader.seek(SeekFrom::End(-(length as i64) - 1)).is_err());
            reader.seek(SeekFrom::End(10)).unwrap();
            assert_eq!(reader.read(&mut [0]).unwrap(), 0);
        }
    }

    #[test]
    fn changed_page_and_checksum_bytes_are_read_errors() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("index");
        let logical = data();
        write(&path, &logical);
        let complete = fs::read(&path).unwrap();
        for page in 0..logical.len().div_ceil(PAGE_BYTES) {
            for checksum in [false, true] {
                let mut changed = complete.clone();
                let offset = if checksum {
                    HEADER_BYTES + logical.len() + page * 4
                } else {
                    HEADER_BYTES + page * PAGE_BYTES
                };
                changed[offset] ^= 1;
                fs::write(&path, changed).unwrap();
                let mut reader = IndexFile::open(&path).unwrap();
                reader
                    .seek(SeekFrom::Start((page * PAGE_BYTES) as u64))
                    .unwrap();
                assert!(
                    reader.read_exact(&mut [0]).is_err(),
                    "page {page}, checksum {checksum}"
                );
                let mut reader = IndexFile::open(&path).unwrap();
                let mut output = vec![0; logical.len()];
                assert!(reader.read_exact(&mut output).is_err());
            }
        }
        // Even moving a whole page together with its checksum must fail because
        // the checksum includes the logical page position.
        let mut changed = complete.clone();
        let (first, rest) =
            changed[HEADER_BYTES..HEADER_BYTES + 2 * PAGE_BYTES].split_at_mut(PAGE_BYTES);
        first.swap_with_slice(rest);
        let footer = HEADER_BYTES + logical.len();
        let (first, second) = changed[footer..footer + 8].split_at_mut(4);
        first.swap_with_slice(second);
        fs::write(&path, changed).unwrap();
        assert!(
            IndexFile::open(&path)
                .unwrap()
                .read_exact(&mut [0])
                .is_err()
        );
    }

    #[test]
    fn incomplete_and_extended_protected_files_are_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("index");
        write(&path, &data());
        let complete = fs::read(&path).unwrap();
        for end in (8..HEADER_BYTES).chain([HEADER_BYTES, PAGE_BYTES, complete.len() - 1]) {
            fs::write(&path, &complete[..end]).unwrap();
            assert!(IndexFile::open(&path).is_err(), "length {end}");
        }
        let mut extended = complete.clone();
        extended.push(0);
        fs::write(&path, extended).unwrap();
        assert!(IndexFile::open(&path).is_err());
        for position in 8..HEADER_BYTES {
            let mut changed = complete.clone();
            changed[position] ^= 1;
            fs::write(&path, changed).unwrap();
            assert!(
                IndexFile::open(&path).is_err(),
                "header position {position}"
            );
        }
        // A self-consistent header with an impossible length is rejected before
        // allocating from it. The fixture remains exactly 48 bytes.
        let mut header = complete[..HEADER_BYTES].to_vec();
        header[16..24].copy_from_slice(&u64::MAX.to_le_bytes());
        let checksum = crc32fast::hash(&header[..44]);
        header[44..48].copy_from_slice(&checksum.to_le_bytes());
        fs::write(&path, header).unwrap();
        assert!(IndexFile::open(&path).is_err());
    }

    #[test]
    fn copied_page_and_checksum_from_another_file_are_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("first");
        let other = dir.path().join("second");
        let original = data();
        write(&path, &original);
        let mut replacement = original.clone();
        replacement[0] ^= 1;
        write(&other, &replacement);
        let mut first = fs::read(&path).unwrap();
        let second = fs::read(&other).unwrap();
        first[HEADER_BYTES..HEADER_BYTES + PAGE_BYTES]
            .copy_from_slice(&second[HEADER_BYTES..HEADER_BYTES + PAGE_BYTES]);
        let footer = HEADER_BYTES + original.len();
        first[footer..footer + 4].copy_from_slice(&second[footer..footer + 4]);
        fs::write(&path, first).unwrap();
        assert!(
            IndexFile::open(&path)
                .unwrap()
                .read_exact(&mut [0])
                .is_err()
        );
    }

    #[test]
    fn writer_requires_the_declared_number_of_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("index");
        assert!(write_index_file(&path, 2, |writer| writer.write_all(&[1])).is_err());
        assert!(IndexFile::open(&path).is_err());
        assert!(write_index_file(&path, 1, |writer| writer.write_all(&[1, 2])).is_err());
        assert!(IndexFile::open(&path).is_err());
        assert!(write_index_file(&path, u64::MAX, |_| Ok(())).is_err());
    }

    #[test]
    fn verified_cache_owns_bytes_and_failed_reload_cannot_reuse_them() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("index");
        let original = data();
        write(&path, &original);
        let mut reader = IndexFile::open(&path).unwrap();
        let mut byte = [0];
        reader.read_exact(&mut byte).unwrap();
        let mut changed = fs::read(&path).unwrap();
        changed[HEADER_BYTES] ^= 1;
        fs::write(&path, changed).unwrap();
        reader.seek(SeekFrom::Start(0)).unwrap();
        reader.read_exact(&mut byte).unwrap();
        assert_eq!(byte[0], original[0]);
        reader.seek(SeekFrom::Start(PAGE_BYTES as u64)).unwrap();
        reader.read_exact(&mut byte).unwrap();
        for _ in 0..2 {
            reader.seek(SeekFrom::Start(0)).unwrap();
            assert!(reader.read_exact(&mut byte).is_err());
        }
    }

    #[test]
    fn cross_page_error_reports_the_valid_prefix_before_failing() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("index");
        let original = data();
        write(&path, &original);
        let mut changed = fs::read(&path).unwrap();
        changed[HEADER_BYTES + PAGE_BYTES] ^= 1;
        fs::write(&path, changed).unwrap();
        let mut reader = IndexFile::open(&path).unwrap();
        reader.seek(SeekFrom::Start(PAGE_BYTES as u64 - 1)).unwrap();
        let mut pair = [0; 2];
        assert_eq!(reader.read(&mut pair).unwrap(), 1);
        assert_eq!(pair[0], original[PAGE_BYTES - 1]);
        assert_eq!(reader.stream_position().unwrap(), PAGE_BYTES as u64);
        assert!(reader.read(&mut pair[1..]).is_err());
        assert_eq!(reader.stream_position().unwrap(), PAGE_BYTES as u64);
    }
}
