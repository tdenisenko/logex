//! Integrity framing for immutable derived files. Page checks detect accidental
//! changes without scanning the whole index for each point lookup. The header
//! retains CRC32; page fingerprints use noncryptographic metadata-seeded XXH3. Neither
//! authenticates a file or establishes that its rows describe a particular
//! segment, and XXH3 does not provide CRC burst-error guarantees.
use std::fs::File;
use std::io::{self, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::Path;

const MAGIC: &[u8; 8] = b"LXIDX001";
const VERSION: u32 = 6;
const HEADER_BYTES: usize = 48;
const PAGE_BYTES: usize = 8192;
const CHECKSUM_BYTES: u64 = 8;
const CHECKSUM_CACHE_BYTES: usize = 16 * 1024;
const WRITE_BUFFER_BYTES: usize = 64 * 1024;
const PAGE_DOMAIN: &[u8] = b"LogEx index page";
const FILE_CONTEXT_BYTES: usize = PAGE_DOMAIN.len() + 4 + 8 + 16;
type FileContext = [u8; FILE_CONTEXT_BYTES];

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

fn file_context(logical_len: u64, file_id: &[u8; 16]) -> FileContext {
    let mut context = [0; FILE_CONTEXT_BYTES];
    let version = PAGE_DOMAIN.len();
    context[..version].copy_from_slice(PAGE_DOMAIN);
    context[version..version + 4].copy_from_slice(&VERSION.to_le_bytes());
    context[version + 4..version + 12].copy_from_slice(&logical_len.to_le_bytes());
    context[version + 12..].copy_from_slice(file_id);
    context
}

fn page_fingerprint(
    logical_len: u64,
    context: &FileContext,
    index: u64,
    page: &[u8],
) -> [u8; CHECKSUM_BYTES as usize] {
    let length = (logical_len - index * PAGE_BYTES as u64).min(PAGE_BYTES as u64);
    let mut metadata = [0; FILE_CONTEXT_BYTES + 16];
    metadata[..FILE_CONTEXT_BYTES].copy_from_slice(context);
    metadata[FILE_CONTEXT_BYTES..FILE_CONTEXT_BYTES + 8].copy_from_slice(&index.to_le_bytes());
    metadata[FILE_CONTEXT_BYTES + 8..].copy_from_slice(&length.to_le_bytes());
    // Hash the complete metadata into a seed, then use standard
    // seeded XXH3 on the page. This differs from hashing metadata || page.
    let seed = twox_hash::XxHash3_64::oneshot(&metadata);
    twox_hash::XxHash3_64::oneshot_with_seed(seed, page).to_le_bytes()
}

fn verify_page(
    logical_len: u64,
    context: &FileContext,
    index: u64,
    page: &[u8],
    expected: [u8; CHECKSUM_BYTES as usize],
) -> io::Result<()> {
    if page_fingerprint(logical_len, context, index, page) != expected {
        return Err(invalid(
            "index page checksum mismatch; rebuild derived indexes",
        ));
    }
    Ok(())
}

// Both random and whole-file readers use the same header/extent rules.
fn parse_header(header: &[u8], length: u64) -> io::Result<Option<(u64, [u8; 16])>> {
    if header.get(..8) != Some(MAGIC.as_slice()) {
        return Ok(None);
    }
    if header.len() < HEADER_BYTES
        || crc32fast::hash(&header[..44]).to_le_bytes() != header[44..48]
        || header[8..12] != VERSION.to_le_bytes()
        || header[12..16] != (PAGE_BYTES as u32).to_le_bytes()
        || header[40..44] != [0; 4]
    {
        return Err(invalid("invalid index integrity header"));
    }
    let logical_len = u64::from_le_bytes(header[16..24].try_into().unwrap());
    if physical_len(logical_len)? != length {
        return Err(invalid("index integrity file length mismatch"));
    }
    let mut file_id = [0; 16];
    file_id.copy_from_slice(&header[24..40]);
    Ok(Some((logical_len, file_id)))
}

fn verify_body(logical: &[u8], checksums: &[u8], context: &FileContext) -> io::Result<()> {
    // Callers establish exact physical geometry before splitting the body.
    for (index, page) in logical.chunks(PAGE_BYTES).enumerate() {
        let offset = index * CHECKSUM_BYTES as usize;
        let expected = checksums[offset..offset + CHECKSUM_BYTES as usize]
            .try_into()
            .unwrap();
        verify_page(logical.len() as u64, context, index as u64, page, expected)?;
    }
    Ok(())
}

/// Own the original physical bytes while exposing only their checked logical
/// slice. Keeping the header in place avoids shifting the whole allocation.
pub(crate) struct IndexData {
    bytes: Vec<u8>,
    logical: std::ops::Range<usize>,
}

impl IndexData {
    pub(crate) fn as_slice(&self) -> &[u8] {
        &self.bytes[self.logical.clone()]
    }
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
    file_context: FileContext,
    position: u64,
    page: Box<[u8; PAGE_BYTES]>,
    page_index: Option<u64>,
    page_len: usize,
    // Page zero was read with the header but has not passed its checksum yet.
    prefetched_page: bool,
    checksums: Box<[u8]>,
    checksum_start: Option<u64>,
    checksum_len: usize,
}

impl IndexFile {
    pub(crate) fn open(path: &Path) -> io::Result<Self> {
        let file = File::open(path)?;
        let length = file.metadata()?.len();
        // One bounded read captures the header and first page. A one-page file
        // also fits its complete checksum footer, avoiding another small read.
        let mut prefetch = [0; HEADER_BYTES + PAGE_BYTES + CHECKSUM_BYTES as usize];
        let prefetch_len = length.min(prefetch.len() as u64) as usize;
        read_at(&file, &mut prefetch[..prefetch_len], 0)?;
        let parsed = parse_header(&prefetch[..prefetch_len.min(HEADER_BYTES)], length)?;
        let protected = parsed.is_some();
        // Legacy bytes are exposed only to callers that validate their encoding.
        let (logical_len, file_id) = parsed.unwrap_or((length, [0; 16]));
        // Cache a bounded footer window without allocating the full window for
        // small indexes or unprotected legacy files.
        let checksum_capacity = if protected {
            (logical_len.div_ceil(PAGE_BYTES as u64) * CHECKSUM_BYTES)
                .min(CHECKSUM_CACHE_BYTES as u64) as usize
        } else {
            0
        };
        let mut checksums = Vec::new();
        checksums
            .try_reserve_exact(checksum_capacity)
            .map_err(io::Error::other)?;
        checksums.resize(checksum_capacity, 0);
        let mut reader = Self {
            file,
            file_context: file_context(logical_len, &file_id),
            logical_len,
            protected,
            position: 0,
            page: Box::new([0; PAGE_BYTES]),
            page_index: None,
            page_len: 0,
            prefetched_page: false,
            checksums: checksums.into_boxed_slice(),
            checksum_start: None,
            checksum_len: 0,
        };
        if protected && logical_len != 0 {
            reader.page_len = logical_len.min(PAGE_BYTES as u64) as usize;
            reader.page[..reader.page_len]
                .copy_from_slice(&prefetch[HEADER_BYTES..HEADER_BYTES + reader.page_len]);
            reader.prefetched_page = true;
            if length == prefetch_len as u64 {
                // Exact physical geometry was checked above. Only a complete
                // one-page footer fits this prefix; no partial cache is valid.
                let footer = HEADER_BYTES + logical_len as usize;
                reader.checksum_len = prefetch_len - footer;
                reader.checksums[..reader.checksum_len]
                    .copy_from_slice(&prefetch[footer..prefetch_len]);
                reader.checksum_start = Some(0);
            }
        }
        Ok(reader)
    }

    /// Load a whole immutable file without constructing point-lookup caches.
    /// Read only the opened handle's actual extent; safe std reads initialize the
    /// reserved storage as they fill it, avoiding a separate whole-file zero fill.
    pub(crate) fn read_all_from_path(path: &Path) -> io::Result<IndexData> {
        let file = File::open(path)?;
        let length = file.metadata()?.len();
        let capacity = usize::try_from(length)
            .map_err(|_| invalid("index file too large for this platform"))?;
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(capacity)
            .map_err(|_| invalid("index allocation failed"))?;
        file.take(length).read_to_end(&mut bytes)?;
        if bytes.len() != capacity {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "truncated index file",
            ));
        }
        let logical = if let Some((logical_len, file_id)) = parse_header(&bytes, length)? {
            // Checked physical geometry bounds this sum by bytes.len().
            let end = HEADER_BYTES + logical_len as usize;
            let (body, checksums) = bytes[HEADER_BYTES..].split_at(logical_len as usize);
            verify_body(body, checksums, &file_context(logical_len, &file_id))?;
            HEADER_BYTES..end
        } else {
            0..bytes.len()
        };
        Ok(IndexData { bytes, logical })
    }

    pub(crate) fn logical_len(&self) -> u64 {
        self.logical_len
    }

    pub(crate) fn is_protected(&self) -> bool {
        self.protected
    }

    /// Full readers need the whole footer, so read it contiguously with the
    /// logical bytes instead of loading checksum cache windows separately.
    pub(crate) fn read_all(mut self) -> io::Result<Vec<u8>> {
        let logical_len = usize::try_from(self.logical_len)
            .map_err(|_| invalid("index file too large for this platform"))?;
        let length = if self.protected && logical_len > PAGE_BYTES {
            usize::try_from(physical_len(self.logical_len)? - HEADER_BYTES as u64)
                .map_err(|_| invalid("index file too large for this platform"))?
        } else {
            logical_len
        };
        let mut data = Vec::new();
        data.try_reserve_exact(length)
            .map_err(|_| invalid("index allocation failed"))?;
        data.resize(length, 0);
        if length == logical_len {
            // Reuse the already prefetched bytes and footer for one-page files.
            self.seek(SeekFrom::Start(0))?;
            self.read_exact(&mut data)?;
        } else {
            read_at(&self.file, &mut data, HEADER_BYTES as u64)?;
            let (logical, checksums) = data.split_at(logical_len);
            verify_body(logical, checksums, &self.file_context)?;
            data.truncate(logical_len);
        }
        Ok(data)
    }

    fn load_page(&mut self, index: u64) -> io::Result<()> {
        if self.page_index == Some(index) {
            return Ok(());
        }
        let prefetched = self.prefetched_page && index == 0;
        self.prefetched_page = false;
        // Invalidate before any fallible read so a failed read cannot leave the
        // old cache identity attached to partially overwritten bytes.
        self.page_index = None;
        let start = index * PAGE_BYTES as u64;
        self.page_len = (self.logical_len - start).min(PAGE_BYTES as u64) as usize;
        if !prefetched {
            read_at(
                &self.file,
                &mut self.page[..self.page_len],
                HEADER_BYTES as u64 + start,
            )?;
        }
        let expected = self.page_checksum(index)?;
        verify_page(
            self.logical_len,
            &self.file_context,
            index,
            &self.page[..self.page_len],
            expected,
        )?;
        self.page_index = Some(index);
        Ok(())
    }

    fn page_checksum(&mut self, index: u64) -> io::Result<[u8; CHECKSUM_BYTES as usize]> {
        let checksum_offset = index * CHECKSUM_BYTES;
        let checksum_start =
            checksum_offset / CHECKSUM_CACHE_BYTES as u64 * CHECKSUM_CACHE_BYTES as u64;
        if self.checksum_start != Some(checksum_start) {
            self.checksum_start = None;
            let checksum_bytes = self.logical_len.div_ceil(PAGE_BYTES as u64) * CHECKSUM_BYTES;
            self.checksum_len =
                (checksum_bytes - checksum_start).min(CHECKSUM_CACHE_BYTES as u64) as usize;
            read_at(
                &self.file,
                &mut self.checksums[..self.checksum_len],
                HEADER_BYTES as u64 + self.logical_len + checksum_start,
            )?;
            self.checksum_start = Some(checksum_start);
        }
        let offset = (checksum_offset - checksum_start) as usize;
        let mut checksum = [0; CHECKSUM_BYTES as usize];
        checksum.copy_from_slice(&self.checksums[offset..offset + CHECKSUM_BYTES as usize]);
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
        let index = self.position / PAGE_BYTES as u64;
        let offset = (self.position % PAGE_BYTES as u64) as usize;
        if (self.page_index == Some(index) || self.prefetched_page && index == 0)
            && length <= PAGE_BYTES - offset
        {
            self.load_page(index)?;
            output[..length].copy_from_slice(&self.page[offset..offset + length]);
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
            self.prefetched_page = false;
            read_at(
                &self.file,
                &mut output[..count],
                HEADER_BYTES as u64 + self.position,
            )?;
            let first = self.position / PAGE_BYTES as u64;
            for (offset, page) in output[..count].chunks(PAGE_BYTES).enumerate() {
                let index = first + offset as u64;
                let expected = self.page_checksum(index)?;
                verify_page(self.logical_len, &self.file_context, index, page, expected)?;
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

/// Stream logical bytes once, retaining only eight fingerprint bytes per page.
/// Durability and publication ordering belong to IndexBuildCheckpoint.
pub(crate) fn write_index_file(
    path: &Path,
    logical_len: u64,
    write: impl FnOnce(&mut dyn Write) -> io::Result<()>,
) -> io::Result<()> {
    let physical_len = physical_len(logical_len)?;
    let count = usize::try_from(logical_len.div_ceil(PAGE_BYTES as u64))
        .map_err(|_| invalid("index checksum count exceeds address space"))?;
    let mut checksums = Vec::new();
    checksums
        .try_reserve_exact(count)
        .map_err(io::Error::other)?;
    let mut file_id = [0; 16];
    getrandom::fill(&mut file_id).map_err(|error| io::Error::other(error.to_string()))?;
    // Keep the format header, table fragments and several bitmap pages in the
    // same bounded write buffer; tiny metadata writes must not split every page.
    let capacity = physical_len.min(WRITE_BUFFER_BYTES as u64) as usize;
    let mut output = BufWriter::with_capacity(capacity, File::create(path)?);
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
        page: [0; PAGE_BYTES],
        flushed: 0,
        failed: false,
        file_context: file_context(logical_len, &file_id),
        checksums,
    };
    write(&mut writer)?;
    if writer.failed {
        return Err(io::Error::other("index writer previously failed"));
    }
    if writer.written != logical_len {
        return Err(invalid("index writer produced an incomplete logical file"));
    }
    if !logical_len.is_multiple_of(PAGE_BYTES as u64) {
        writer.finish_buffered_page(
            (logical_len % PAGE_BYTES as u64) as usize,
            logical_len / PAGE_BYTES as u64,
        )?;
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
    page: [u8; PAGE_BYTES],
    // Bytes from the current page already emitted by an explicit flush.
    flushed: usize,
    failed: bool,
    file_context: FileContext,
    checksums: Vec<[u8; CHECKSUM_BYTES as usize]>,
}

impl IndexWriter {
    fn finish_buffered_page(&mut self, length: usize, index: u64) -> io::Result<()> {
        self.output.write_all(&self.page[self.flushed..length])?;
        self.checksums.push(page_fingerprint(
            self.logical_len,
            &self.file_context,
            index,
            &self.page[..length],
        ));
        self.flushed = 0;
        Ok(())
    }
}

impl IndexWriter {
    fn write_bytes(&mut self, bytes: &[u8]) -> io::Result<()> {
        if bytes.len() as u64 > self.logical_len - self.written {
            return Err(invalid("index writer exceeded its declared logical length"));
        }
        let mut remaining = bytes;
        while !remaining.is_empty() {
            let offset = (self.written % PAGE_BYTES as u64) as usize;
            if offset == 0 && remaining.len() >= PAGE_BYTES {
                // Large serialized bitmaps retain their contiguous write and
                // need no copy into the small-write accumulation buffer.
                let count = remaining.len() / PAGE_BYTES * PAGE_BYTES;
                self.output.write_all(&remaining[..count])?;
                let first = self.written / PAGE_BYTES as u64;
                for (page_index, page) in remaining[..count]
                    .as_chunks::<PAGE_BYTES>()
                    .0
                    .iter()
                    .enumerate()
                {
                    self.checksums.push(page_fingerprint(
                        self.logical_len,
                        &self.file_context,
                        first + page_index as u64,
                        page,
                    ));
                }
                self.written += count as u64;
                remaining = &remaining[count..];
            } else {
                // Roaring emits many small integer writes. Hashing each one
                // separately repeats checksum setup; collect a complete page.
                let count = (PAGE_BYTES - offset).min(remaining.len());
                self.page[offset..offset + count].copy_from_slice(&remaining[..count]);
                self.written += count as u64;
                remaining = &remaining[count..];
                if offset + count == PAGE_BYTES {
                    self.finish_buffered_page(PAGE_BYTES, self.written / PAGE_BYTES as u64 - 1)?;
                }
            }
        }
        Ok(())
    }

    fn flush_bytes(&mut self) -> io::Result<()> {
        let length = (self.written % PAGE_BYTES as u64) as usize;
        self.output.write_all(&self.page[self.flushed..length])?;
        self.flushed = length;
        self.output.flush()
    }
}

impl Write for IndexWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if self.failed {
            return Err(io::Error::other("index writer previously failed"));
        }
        let result = self.write_bytes(bytes);
        self.failed = result.is_err();
        result.map(|()| bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        if self.failed {
            return Err(io::Error::other("index writer previously failed"));
        }
        let result = self.flush_bytes();
        self.failed = result.is_err();
        result
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
                IndexFile::read_all_from_path(&path).unwrap().as_slice(),
                expected
            );
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
    fn persisted_page_fingerprints_match_independent_seeded_encoding() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("fingerprints");
        let original = data();
        for length in [0, 1, PAGE_BYTES, PAGE_BYTES + 1, original.len()] {
            let logical = &original[..length];
            write(&path, logical);
            let physical = fs::read(&path).unwrap();
            assert_eq!(&physical[8..12], &6u32.to_le_bytes());
            assert_eq!(&physical[12..16], &8192u32.to_le_bytes());
            assert_eq!(physical.len(), 48 + length + length.div_ceil(8192) * 8);
            assert_eq!(&physical[48..48 + length], logical);
            assert_eq!(
                crc32fast::hash(&physical[..44]).to_le_bytes(),
                physical[44..48]
            );
            for (index, page) in logical.chunks(8192).enumerate() {
                // Independently construct metadata for the seed without using
                // the writer's fixed context/extent helpers. Hash the page in a
                // separate standard seeded invocation, as required by version 6.
                let mut bytes = b"LogEx index page".to_vec();
                bytes.extend_from_slice(&6u32.to_le_bytes());
                bytes.extend_from_slice(&(length as u64).to_le_bytes());
                bytes.extend_from_slice(&physical[24..40]);
                bytes.extend_from_slice(&(index as u64).to_le_bytes());
                bytes.extend_from_slice(&(page.len() as u64).to_le_bytes());
                let seed = twox_hash::XxHash3_64::oneshot(&bytes);
                let expected = twox_hash::XxHash3_64::oneshot_with_seed(seed, page).to_le_bytes();
                let offset = 48 + length + index * 8;
                assert_eq!(&physical[offset..offset + 8], &expected);
            }
            assert_eq!(
                IndexFile::read_all_from_path(&path).unwrap().as_slice(),
                logical
            );
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
            for checksum in [None, Some(0), Some(CHECKSUM_BYTES as usize - 1)] {
                let mut changed = complete.clone();
                let offset = if let Some(byte) = checksum {
                    HEADER_BYTES + logical.len() + page * CHECKSUM_BYTES as usize + byte
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
                    "page {page}, checksum {checksum:?}"
                );
                let mut reader = IndexFile::open(&path).unwrap();
                let mut output = vec![0; logical.len()];
                assert!(reader.read_exact(&mut output).is_err());
                assert!(IndexFile::read_all_from_path(&path).is_err());
            }
        }
        // Even moving a whole page together with its checksum must fail because
        // the checksum includes the logical page position.
        let mut changed = complete.clone();
        let (first, rest) =
            changed[HEADER_BYTES..HEADER_BYTES + 2 * PAGE_BYTES].split_at_mut(PAGE_BYTES);
        first.swap_with_slice(rest);
        let footer = HEADER_BYTES + logical.len();
        let (first, second) = changed[footer..footer + 2 * CHECKSUM_BYTES as usize]
            .split_at_mut(CHECKSUM_BYTES as usize);
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
    fn reads_preserve_bytes_across_checksum_cache_windows() {
        let boundary = CHECKSUM_CACHE_BYTES / CHECKSUM_BYTES as usize * PAGE_BYTES;
        let expected: Vec<u8> = (0..boundary + PAGE_BYTES + 7)
            .map(|position| (position ^ (position >> 16)) as u8)
            .collect();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cache-windows");
        write_index_file(&path, expected.len() as u64, |writer| {
            writer.write_all(&expected)
        })
        .unwrap();
        let mut reader = IndexFile::open(&path).unwrap();
        // Cross the footer-cache boundary in both directions, with a data-page
        // crossing too. Each probe must preserve the exact original bytes.
        for start in [boundary - 1, boundary, boundary + 1, PAGE_BYTES - 1, 0] {
            reader.seek(SeekFrom::Start(start as u64)).unwrap();
            let mut actual = [0; 31];
            reader.read_exact(&mut actual).unwrap();
            assert_eq!(actual, expected[start..start + actual.len()]);
        }
        reader.seek(SeekFrom::Start(0)).unwrap();
        let mut all = vec![0; expected.len()];
        reader.read_exact(&mut all).unwrap();
        assert_eq!(all, expected);
        assert_eq!(
            IndexFile::open(&path).unwrap().read_all().unwrap(),
            expected
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
            assert!(
                IndexFile::read_all_from_path(&path).is_err(),
                "length {end}"
            );
        }
        let mut extended = complete.clone();
        extended.push(0);
        fs::write(&path, extended).unwrap();
        assert!(IndexFile::open(&path).is_err());
        assert!(IndexFile::read_all_from_path(&path).is_err());
        for position in 8..HEADER_BYTES {
            let mut changed = complete.clone();
            changed[position] ^= 1;
            fs::write(&path, changed).unwrap();
            assert!(
                IndexFile::open(&path).is_err(),
                "header position {position}"
            );
            assert!(IndexFile::read_all_from_path(&path).is_err());
        }
        // Discarded prototype versions do not become readable merely by
        // updating the header CRC to agree with their version field.
        for version in [1u32, 2, 3, 4, 5] {
            let mut old_version = complete.clone();
            old_version[8..12].copy_from_slice(&version.to_le_bytes());
            let checksum = crc32fast::hash(&old_version[..44]);
            old_version[44..48].copy_from_slice(&checksum.to_le_bytes());
            fs::write(&path, old_version).unwrap();
            assert!(IndexFile::open(&path).is_err());
            assert!(IndexFile::read_all_from_path(&path).is_err());
        }
        // A self-consistent header with an impossible length is rejected before
        // allocating from it. The fixture remains exactly 48 bytes.
        let mut header = complete[..HEADER_BYTES].to_vec();
        header[16..24].copy_from_slice(&u64::MAX.to_le_bytes());
        let checksum = crc32fast::hash(&header[..44]);
        header[44..48].copy_from_slice(&checksum.to_le_bytes());
        fs::write(&path, header).unwrap();
        assert!(IndexFile::open(&path).is_err());
        assert!(IndexFile::read_all_from_path(&path).is_err());
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
        first[footer..footer + CHECKSUM_BYTES as usize]
            .copy_from_slice(&second[footer..footer + CHECKSUM_BYTES as usize]);
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
    fn explicit_writer_flushes_preserve_page_checksums() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("index");
        let original = data();
        write_index_file(&path, original.len() as u64, |writer| {
            for chunk in original.chunks(137) {
                writer.write_all(chunk)?;
                writer.flush()?;
            }
            writer.flush()
        })
        .unwrap();
        let mut reader = IndexFile::open(&path).unwrap();
        let mut read = vec![0; original.len()];
        reader.read_exact(&mut read).unwrap();
        assert_eq!(read, original);
    }

    #[test]
    fn mixed_buffered_and_bulk_writes_preserve_the_complete_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("index");
        let original = data();
        write_index_file(&path, original.len() as u64, |writer| {
            writer.write_all(&original[..19])?;
            writer.flush()?;
            writer.write_all(&original[19..PAGE_BYTES])?;
            writer.write_all(&original[PAGE_BYTES..PAGE_BYTES * 3])?;
            writer.flush()?;
            writer.write_all(&original[PAGE_BYTES * 3..])?;
            writer.flush()
        })
        .unwrap();
        let mut reader = IndexFile::open(&path).unwrap();
        let mut read = vec![0; original.len()];
        reader.read_exact(&mut read).unwrap();
        assert_eq!(read, original);
    }

    #[test]
    fn failed_page_write_cannot_be_reused_or_published() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("index");
        let mut writer = IndexWriter {
            output: BufWriter::new(File::create(&path).unwrap()),
            logical_len: PAGE_BYTES as u64,
            written: 0,
            page: [0; PAGE_BYTES],
            flushed: 0,
            failed: false,
            file_context: file_context(PAGE_BYTES as u64, &[0; 16]),
            checksums: Vec::new(),
        };
        writer.write_all(&[1; 19]).unwrap();
        writer.flush().unwrap();
        // A read-only fixture handle produces a local write error at completion
        // of the already partially flushed page.
        writer.output = BufWriter::with_capacity(1, File::open(&path).unwrap());
        assert!(writer.write_all(&[1; PAGE_BYTES - 19]).is_err());
        assert!(writer.flush().is_err());
        assert!(writer.write_all(&[1]).is_err());
        drop(writer);
        assert_eq!(fs::read(&path).unwrap(), [1; 19]);
        assert!(
            write_index_file(&path, 1, |writer| {
                assert!(writer.write_all(&[1, 2]).is_err());
                Ok(())
            })
            .is_err()
        );
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
