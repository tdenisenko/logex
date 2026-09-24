//! Lazy bounded sources for repeated WHERE/CASE/aggregate selections.
use super::*;
use crate::reader::{validate_fixed_metadata, validate_null_metadata};

const WINDOW_ROWS: u64 = crate::page::MAX_PAGE_ROWS as u64;
const NAMES: [&str; 13] = [
    "address",
    "block_number",
    "block_hash",
    "timestamp",
    "tx_hash",
    "tx_index",
    "log_index",
    "data_len",
    "source",
    "topic0",
    "topic1",
    "topic2",
    "topic3",
];

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}
fn input(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

#[derive(Clone, Copy)]
enum Value {
    U64(u64),
    U32(u32),
    U8(u8),
    Address(Address),
    Hash(B256),
}
enum Values {
    Bytes(QueryBuffer<u8>),
    U64(QueryBuffer<u64>),
    U32(QueryBuffer<u32>),
    U8(QueryBuffer<u8>),
}
impl Values {
    fn get(&self, index: usize, column: usize) -> io::Result<Value> {
        Ok(match self {
            Self::U64(values) => Value::U64(
                *values
                    .get(index)
                    .ok_or_else(|| invalid("missing prepared row"))?,
            ),
            Self::U32(values) => Value::U32(
                *values
                    .get(index)
                    .ok_or_else(|| invalid("missing prepared row"))?,
            ),
            Self::U8(values) => Value::U8(
                *values
                    .get(index)
                    .ok_or_else(|| invalid("missing prepared row"))?,
            ),
            Self::Bytes(values) => {
                let width = width(column);
                let bytes = values
                    .get(index * width..(index + 1) * width)
                    .ok_or_else(|| invalid("missing prepared row"))?;
                match column {
                    0 => Value::Address(Address::from_slice(bytes)),
                    1 | 3 => Value::U64(u64::from_le_bytes(bytes.try_into().unwrap())),
                    5..=7 => Value::U32(u32::from_le_bytes(bytes.try_into().unwrap())),
                    8 => Value::U8(bytes[0]),
                    _ => Value::Hash(B256::from_slice(bytes)),
                }
            }
        })
    }
}
fn width(column: usize) -> usize {
    match column {
        0 => 20,
        1 | 3 => 8,
        5..=7 => 4,
        8 => 1,
        _ => 32,
    }
}
struct Window<T> {
    first: u64,
    rows: u64,
    values: T,
}
impl<T> Window<T> {
    fn contains(&self, row: u64) -> bool {
        row >= self.first && row - self.first < self.rows
    }
}
struct Fixed<'a> {
    descriptor: Option<&'a ColumnDescriptor>,
    index: Option<QueryBuffer<PageIndexEntry>>,
    window: Option<Window<Values>>,
    raw_rows: u64,
}
enum Nulls {
    Raw {
        rows: u64,
        window: Option<Window<QueryBuffer<u8>>>,
    },
    Bundled(QueryBuffer<u8>),
}
enum Payload<'a> {
    Raw(RawBytesColumn),
    Paged {
        descriptor: &'a ColumnDescriptor,
        index: QueryBuffer<PageIndexEntry>,
        window: Option<Window<QueryBuffer<Bytes>>>,
    },
}

/// Reusable captured sources with at most one fixed window/page per column.
/// Each selection must be sorted and unique; successive calls may overlap or
/// move backwards. Returned buffers and payload aliases do not borrow this cursor.
/// The captured reader's optional memory budget is used for every source/output.
/// Raw variable data retains its validated complete offset/payload source; bundled
/// null bitmaps retain their decoded body. Other raw fixed sources use bounded windows.
pub struct PreparedSegmentSelection<'a> {
    reader: &'a SegmentReader,
    fixed: [Option<Fixed<'a>>; 13],
    nulls: [Option<Nulls>; 4],
    payload: Option<Payload<'a>>,
}

macro_rules! typed_read {
    ($method:ident, $ty:ty, $variant:ident, $allowed:pat) => {
        pub fn $method(&mut self, column: &str, ids: &[u32]) -> io::Result<QueryBuffer<$ty>> {
            let column = self.column(column)?;
            if !matches!(column, $allowed) {
                return Err(input("column type does not match prepared read"));
            }
            self.validate(ids)?;
            let mut output = QueryBuffer::try_with_capacity(
                ids.len(),
                self.reader.memory.as_ref(),
                "prepared selection output",
            )?;
            for &id in ids {
                let Value::$variant(value) =
                    self.fixed_value(column, u64::from(id), selection_end(ids))?
                else {
                    return Err(invalid("prepared column type mismatch"));
                };
                output.try_push(value)?;
            }
            Ok(output)
        }
    };
}

impl<'a> PreparedSegmentSelection<'a> {
    pub(super) fn new(reader: &'a SegmentReader) -> Self {
        Self {
            reader,
            fixed: std::array::from_fn(|_| None),
            nulls: std::array::from_fn(|_| None),
            payload: None,
        }
    }
    fn column(&self, name: &str) -> io::Result<usize> {
        NAMES
            .iter()
            .position(|&n| n == name)
            .ok_or_else(|| input("unsupported prepared column"))
    }
    fn validate(&self, ids: &[u32]) -> io::Result<()> {
        let rows = self.reader.read_row_count()?;
        if ids.windows(2).any(|p| p[0] >= p[1])
            || ids.last().is_some_and(|&id| u64::from(id) >= rows)
        {
            return Err(input(
                "prepared selection requires increasing unique captured rows",
            ));
        }
        Ok(())
    }
    typed_read!(read_u64, u64, U64, 1 | 3);
    typed_read!(read_u32, u32, U32, 5..=7);
    typed_read!(read_u8, u8, U8, 8);
    typed_read!(read_b256, B256, Hash, 2 | 4 | 9..=12);
    pub fn read_address(&mut self, ids: &[u32]) -> io::Result<QueryBuffer<Address>> {
        self.validate(ids)?;
        let mut output = QueryBuffer::try_with_capacity(
            ids.len(),
            self.reader.memory.as_ref(),
            "prepared selection output",
        )?;
        for &id in ids {
            let Value::Address(value) = self.fixed_value(0, u64::from(id), selection_end(ids))?
            else {
                return Err(invalid("prepared address type mismatch"));
            };
            output.try_push(value)?;
        }
        Ok(output)
    }
    pub fn read_nullable_b256(
        &mut self,
        column: &str,
        ids: &[u32],
    ) -> io::Result<QueryBuffer<Option<B256>>> {
        let column = self.column(column)?;
        if !(9..=12).contains(&column) {
            return Err(input("prepared nullable column must be a topic"));
        }
        self.validate(ids)?;
        let mut output = QueryBuffer::try_with_capacity(
            ids.len(),
            self.reader.memory.as_ref(),
            "prepared nullable output",
        )?;
        let path = self.null_path(column)?;
        for &id in ids {
            let Value::Hash(value) = self.fixed_value(column, u64::from(id), selection_end(ids))?
            else {
                return Err(invalid("prepared topic type mismatch"));
            };
            output.try_push(
                self.present(column, path, u64::from(id), selection_end(ids))?
                    .then_some(value),
            )?;
        }
        Ok(output)
    }
    fn fixed_value(&mut self, column: usize, row: u64, requested_end: u64) -> io::Result<Value> {
        let reader = self.reader;
        let memory = reader.memory.as_ref();
        let name = NAMES[column];
        if self.fixed[column].is_none() {
            let descriptor = reader.compacted_column(name);
            let index = descriptor
                .map(|d| reader.read_compacted_page_index_accounted(d, None, memory))
                .transpose()?;
            let mut raw_rows = 0;
            if descriptor.is_none() {
                let path = raw_column_path(name);
                let header = reader
                    .artifacts
                    .read_range_accounted(path, 0..ColumnFileHeader::SIZE as u64)?;
                let rows =
                    usize::try_from(row + 1).map_err(|_| invalid("requested rows overflow"))?;
                let len = reader.artifacts.len(path)?;
                match width(column) {
                    1 => validate_fixed_metadata::<1>(
                        &reader.dir.join(path),
                        &header,
                        len,
                        Some(rows),
                    )?,
                    4 => validate_fixed_metadata::<4>(
                        &reader.dir.join(path),
                        &header,
                        len,
                        Some(rows),
                    )?,
                    8 => validate_fixed_metadata::<8>(
                        &reader.dir.join(path),
                        &header,
                        len,
                        Some(rows),
                    )?,
                    20 => validate_fixed_metadata::<20>(
                        &reader.dir.join(path),
                        &header,
                        len,
                        Some(rows),
                    )?,
                    _ => validate_fixed_metadata::<32>(
                        &reader.dir.join(path),
                        &header,
                        len,
                        Some(rows),
                    )?,
                };
                let declared = ColumnFileHeader::read_from(&header).unwrap().row_count;
                raw_rows = declared
                    .min(reader.read_row_count()?)
                    .min((len - ColumnFileHeader::SIZE as u64) / width(column) as u64);
            }
            self.fixed[column] = Some(Fixed {
                descriptor,
                index,
                window: None,
                raw_rows,
            });
        }
        let fixed = self.fixed[column].as_mut().unwrap();
        if !fixed
            .window
            .as_ref()
            .is_some_and(|window| window.contains(row))
        {
            fixed.window = None;
            let (first, rows, values) = if let Some(descriptor) = fixed.descriptor {
                let entry = page_for(fixed.index.as_ref().unwrap(), row)?;
                let encoded = reader.read_page_payload_accounted(descriptor, &entry, memory)?;
                let count = entry.row_count as usize;
                let values = match column {
                    1 | 3 => Values::U64(decode_u64_page_accounted(
                        &encoded,
                        count,
                        descriptor.codec,
                        memory,
                    )?),
                    5..=7 => Values::U32(decode_u32_page_accounted(
                        &encoded,
                        count,
                        descriptor.codec,
                        memory,
                    )?),
                    8 => Values::U8(decode_u8_page_accounted(
                        &encoded,
                        count,
                        descriptor.codec,
                        memory,
                    )?),
                    _ => Values::Bytes(decode_fixed_width_page_accounted(
                        &encoded,
                        count,
                        width(column),
                        descriptor.codec,
                        memory,
                    )?),
                };
                (entry.first_row, u64::from(entry.row_count), values)
            } else {
                // Every caller supplies an exclusive end above this row (the
                // selection maximum, or full companion page end). Start at the
                // cache miss, so sparse reads never fetch an unrelated prefix.
                let first = row;
                if row >= fixed.raw_rows {
                    return Err(invalid("requested raw row is incomplete"));
                }
                let rows = WINDOW_ROWS.min(fixed.raw_rows.min(requested_end) - first);
                let start = first
                    .checked_mul(width(column) as u64)
                    .and_then(|v| v.checked_add(ColumnFileHeader::SIZE as u64))
                    .ok_or_else(|| invalid("raw window overflow"))?;
                let end = start
                    .checked_add(rows * width(column) as u64)
                    .ok_or_else(|| invalid("raw window overflow"))?;
                let data = reader
                    .artifacts
                    .read_range_accounted(raw_column_path(name), start..end)?;
                #[cfg(test)]
                RAW_READS.with_borrow_mut(|reads| {
                    if let Some(reads) = reads {
                        reads.push((name.to_owned(), first, rows));
                    }
                });
                (first, rows, Values::Bytes(data))
            };
            fixed.window = Some(Window {
                first,
                rows,
                values,
            });
        }
        let window = fixed.window.as_ref().unwrap();
        window.values.get((row - window.first) as usize, column)
    }
    fn null_path(&self, column: usize) -> io::Result<&'a str> {
        let reader = self.reader;
        Ok(reader
            .compacted_column(NAMES[column])
            .map(|d| {
                d.null_bitmap_path
                    .as_deref()
                    .ok_or_else(|| invalid("missing null bitmap"))
            })
            .transpose()?
            .unwrap_or(["topic0.null", "topic1.null", "topic2.null", "topic3.null"][column - 9]))
    }

    fn present(
        &mut self,
        column: usize,
        path: &str,
        row: u64,
        requested_end: u64,
    ) -> io::Result<bool> {
        let reader = self.reader;
        let slot = &mut self.nulls[column - 9];
        if slot.is_none() {
            *slot = Some(if reader.artifacts.bundle().is_some() {
                let bytes = reader.artifacts.read_accounted(path)?;
                validate_null_bytes(&bytes, bytes.len() as u64, reader.read_row_count()?, true)?;
                Nulls::Bundled(bytes)
            } else {
                let header = reader.artifacts.read_range_accounted(path, 0..8)?;
                // Paged nullable readers validate every captured row's bitmap
                // coverage; raw fixed readers preserve selected-prefix tolerance.
                let required = if reader.compacted_column(NAMES[column]).is_some() {
                    reader.read_row_count()?
                } else {
                    row + 1
                };
                let rows =
                    validate_null_metadata(&header, reader.artifacts.len(path)?, required, true)?
                        .min(reader.read_row_count()?);
                Nulls::Raw { rows, window: None }
            });
        }
        match slot.as_mut().unwrap() {
            Nulls::Bundled(bytes) => Ok(bytes[8 + row as usize / 8] & (1 << (row % 8)) != 0),
            Nulls::Raw {
                rows: available,
                window,
            } => {
                if row >= *available {
                    return Err(invalid("null bitmap does not cover selected row"));
                }
                if !window.as_ref().is_some_and(|w| w.contains(row)) {
                    *window = None;
                    let first = row / 8 * 8;
                    let rows = WINDOW_ROWS.min((*available).min(requested_end) - first);
                    let bytes = reader.artifacts.read_range_accounted(
                        path,
                        8 + first / 8..8 + (first + rows).div_ceil(8),
                    )?;
                    *window = Some(Window {
                        first,
                        rows,
                        values: bytes,
                    });
                }
                let window = window.as_ref().unwrap();
                let local = row - window.first;
                Ok(window.values[local as usize / 8] & (1 << (local % 8)) != 0)
            }
        }
    }
    pub fn read_var_bytes(&mut self, column: &str, ids: &[u32]) -> io::Result<QueryBuffer<Bytes>> {
        if column != "data" {
            return Err(input("unsupported prepared variable column"));
        }
        self.validate(ids)?;
        if ids.is_empty() {
            return QueryBuffer::try_with_capacity(
                0,
                self.reader.memory.as_ref(),
                "prepared variable headers",
            );
        }
        if self.payload.is_none() {
            self.payload = Some(match self.reader.compacted_column("data") {
                Some(descriptor) => Payload::Paged {
                    descriptor,
                    index: self.reader.read_compacted_page_index_accounted(
                        descriptor,
                        None,
                        self.reader.memory.as_ref(),
                    )?,
                    window: None,
                },
                None => {
                    let source = RawBytesColumn::from_accounted(
                        &self.reader.dir.join("data.col"),
                        self.reader.artifacts.read_accounted("data.col")?,
                    )?;
                    if (source.row_count() as u64) < self.reader.read_row_count()? {
                        return Err(invalid("raw data does not cover captured rows"));
                    }
                    Payload::Raw(source)
                }
            });
        }
        if matches!(self.payload, Some(Payload::Raw(_))) {
            // Companion validation uses the same physical source/cache as explicit data_len reads.
            for &id in ids {
                let Value::U32(length) = self.fixed_value(7, u64::from(id), selection_end(ids))?
                else {
                    unreachable!()
                };
                let Some(Payload::Raw(source)) = &self.payload else {
                    unreachable!()
                };
                if source.row(id as usize)?.len() != length as usize {
                    return Err(invalid("data bytes differ from their row-length metadata"));
                }
            }
            let Some(Payload::Raw(source)) = &self.payload else {
                unreachable!()
            };
            return source.materialize_accounted(
                Some(ids),
                Some(self.reader.read_row_count()?),
                self.reader.memory.as_ref(),
            );
        }
        let mut output = QueryBuffer::try_with_capacity(
            ids.len(),
            self.reader.memory.as_ref(),
            "prepared variable headers",
        )?;
        for &id in ids {
            let row = u64::from(id);
            let Some(Payload::Paged {
                descriptor,
                index,
                window,
            }) = &self.payload
            else {
                unreachable!()
            };
            if !window.as_ref().is_some_and(|w| w.contains(row)) {
                let descriptor = *descriptor;
                let entry = page_for(index, row)?;
                if let Some(Payload::Paged { window, .. }) = &mut self.payload {
                    *window = None;
                }
                let mut lengths = QueryBuffer::try_with_capacity(
                    entry.row_count as usize,
                    self.reader.memory.as_ref(),
                    "prepared companion lengths",
                )?;
                for row in entry.first_row..entry.first_row + u64::from(entry.row_count) {
                    let Value::U32(length) =
                        self.fixed_value(7, row, entry.first_row + u64::from(entry.row_count))?
                    else {
                        unreachable!()
                    };
                    lengths.try_push(length)?;
                }
                let encoded = self.reader.read_page_payload_accounted(
                    descriptor,
                    &entry,
                    self.reader.memory.as_ref(),
                )?;
                let values = decode_var_bytes_page_selected_accounted(
                    &encoded,
                    descriptor.codec,
                    &lengths,
                    None,
                    self.reader.memory.as_ref(),
                )?;
                if let Some(Payload::Paged { window, .. }) = &mut self.payload {
                    *window = Some(Window {
                        first: entry.first_row,
                        rows: u64::from(entry.row_count),
                        values,
                    });
                }
            }
            let Some(Payload::Paged {
                window: Some(window),
                ..
            }) = &self.payload
            else {
                unreachable!()
            };
            output.try_push(window.values[(row - window.first) as usize].clone())?;
        }
        Ok(output)
    }
}
fn selection_end(ids: &[u32]) -> u64 {
    ids.last().map_or(0, |&id| u64::from(id) + 1)
}

fn page_for(index: &[PageIndexEntry], row: u64) -> io::Result<PageIndexEntry> {
    let position = index.partition_point(|e| e.first_row + u64::from(e.row_count) <= row);
    index
        .get(position)
        .copied()
        .filter(|e| e.first_row <= row)
        .ok_or_else(|| invalid("selected page missing"))
}
#[cfg(test)]
thread_local! { pub(super) static RAW_READS: std::cell::RefCell<Option<Vec<(String,u64,u64)>>> = const { std::cell::RefCell::new(None) }; }
