//! Convert Arrow SQL output without losing precision or leaving owned allocations uncharged.
use std::collections::HashSet;
use std::fmt::{Display, Write as _};

use datafusion::arrow::array::*;
use datafusion::arrow::datatypes::{
    ArrowDictionaryKeyType, ArrowNativeType, DataType, Decimal32Type, Decimal64Type,
    Decimal128Type, Decimal256Type, DecimalType, FieldRef, Fields, Int8Type, Int16Type, Int32Type,
    Int64Type, UInt8Type, UInt16Type, UInt32Type, UInt64Type, i256,
};
use datafusion::arrow::error::ArrowError;
use datafusion::arrow::json::writer::{EncoderOptions, NullableEncoder, make_encoder};
use datafusion::arrow::util::display::{ArrayFormatter, FormatOptions};
use datafusion::error::{DataFusionError, Result};
use logex_types::QueryMemoryError;
use serde_json::{Map, Value};

use crate::QueryCancelCheck;
use crate::result::{JsonBatchAppender, JsonResultBuilder, json_object_node_bytes};

const RESULT_STAGE: &str = "structured SQL result";
const CANCEL_VALUES: usize = 256;
const CANCEL_BYTES: usize = 64 * 1024;

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct JsonAllocationPlan {
    bytes: usize,
    float_scratch: bool,
}

impl JsonAllocationPlan {
    pub(crate) fn new() -> Self {
        Self::default()
    }
    pub(crate) fn bytes(self) -> usize {
        self.bytes
    }
    pub(crate) fn add_string_capacity(&mut self, capacity: usize) -> Result<()> {
        self.add(capacity)
    }
    pub(crate) fn add_array_capacity(&mut self, capacity: usize) -> Result<()> {
        self.add(value_slots(capacity)?)
    }
    pub(crate) fn add_object(&mut self, entries: usize) -> Result<()> {
        self.add(json_object_node_bytes(entries)?)
    }
    pub(crate) fn add_hex_string(&mut self, bytes: usize) -> Result<usize> {
        let capacity = bytes.checked_mul(2).ok_or_else(size_overflow)?;
        self.add(capacity)?;
        Ok(capacity)
    }
    pub(crate) fn add_prefixed_hex_string(&mut self, bytes: usize) -> Result<usize> {
        let capacity = bytes
            .checked_mul(2)
            .and_then(|bytes| bytes.checked_add(2))
            .ok_or_else(size_overflow)?;
        self.add(capacity)?;
        Ok(capacity)
    }
    fn add(&mut self, bytes: usize) -> Result<()> {
        self.bytes = self.bytes.checked_add(bytes).ok_or_else(size_overflow)?;
        Ok(())
    }
    fn require_float_scratch(&mut self) -> Result<()> {
        if !self.float_scratch {
            self.add(64)?;
            self.float_scratch = true;
        }
        Ok(())
    }
}

pub(crate) fn allocate_json_string(
    value: &str,
    output: &mut JsonBatchAppender<'_>,
) -> Result<String> {
    let mut string = String::new();
    string
        .try_reserve_exact(value.len())
        .map_err(allocation_error)?;
    output.observe_capacity(value.len(), string.capacity())?;
    string.push_str(value);
    Ok(string)
}

pub(crate) fn allocate_json_array(
    capacity: usize,
    output: &mut JsonBatchAppender<'_>,
) -> Result<Vec<Value>> {
    let requested = value_slots(capacity)?;
    let mut values = Vec::new();
    values
        .try_reserve_exact(capacity)
        .map_err(allocation_error)?;
    output.observe_capacity(requested, value_slots(values.capacity())?)?;
    Ok(values)
}

pub(crate) fn allocate_json_prefixed_hex(
    bytes: &[u8],
    output: &mut JsonBatchAppender<'_>,
) -> Result<String> {
    let bound = bytes
        .len()
        .checked_mul(2)
        .and_then(|bytes| bytes.checked_add(2))
        .ok_or_else(size_overflow)?;
    allocate_bounded_string(bound, output, |string| {
        string.write_str("0x")?;
        write_hex(string, bytes)
    })
}

pub(crate) fn allocate_json_prefixed_hex_with_cancel(
    bytes: &[u8],
    output: &mut JsonBatchAppender<'_>,
    cancel: Option<&QueryCancelCheck>,
) -> Result<String> {
    let bound = bytes
        .len()
        .checked_mul(2)
        .and_then(|bytes| bytes.checked_add(2))
        .ok_or_else(size_overflow)?;
    let mut value = String::new();
    value.try_reserve_exact(bound).map_err(allocation_error)?;
    output.observe_capacity(bound, value.capacity())?;
    value.push_str("0x");
    for chunk in bytes.chunks(CANCEL_BYTES) {
        write_hex(&mut value, chunk).expect("String writes cannot fail");
        if bytes.len() > CANCEL_BYTES && cancel.is_some_and(|check| check()) {
            return Err(DataFusionError::Execution("query canceled".into()));
        }
    }
    Ok(value)
}

fn write_hex(output: &mut impl std::fmt::Write, bytes: &[u8]) -> std::fmt::Result {
    if bytes.is_empty() {
        return Ok(());
    }
    if bytes.len() <= 32 {
        return write_hex_chunk::<64>(output, bytes);
    }
    write_hex_large(output, bytes)
}

// Keep the 8 KiB scratch frame out of the common address/hash/topic call path.
#[inline(never)]
fn write_hex_large(output: &mut impl std::fmt::Write, bytes: &[u8]) -> std::fmt::Result {
    for chunk in bytes.chunks(4_096) {
        write_hex_chunk::<8_192>(output, chunk)?;
    }
    Ok(())
}

#[inline(always)]
fn write_hex_chunk<const N: usize>(
    output: &mut impl std::fmt::Write,
    bytes: &[u8],
) -> std::fmt::Result {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    debug_assert!(bytes.len() <= N / 2);
    let mut encoded = [0_u8; N];
    for (index, byte) in bytes.iter().copied().enumerate() {
        encoded[index * 2] = DIGITS[(byte >> 4) as usize];
        encoded[index * 2 + 1] = DIGITS[(byte & 0x0f) as usize];
    }
    output.write_str(
        std::str::from_utf8(&encoded[..bytes.len() * 2]).expect("hexadecimal digits are UTF-8"),
    )
}

pub(crate) fn allocate_json_display(
    capacity_bound: usize,
    value: &impl Display,
    output: &mut JsonBatchAppender<'_>,
) -> Result<String> {
    allocate_bounded_string(capacity_bound, output, |string| write!(string, "{value}"))
}

fn allocate_bounded_string(
    bound: usize,
    output: &mut JsonBatchAppender<'_>,
    write_value: impl FnOnce(&mut BoundedString<'_>) -> std::fmt::Result,
) -> Result<String> {
    let mut value = String::new();
    value.try_reserve_exact(bound).map_err(allocation_error)?;
    output.observe_capacity(bound, value.capacity())?;
    write_value(&mut BoundedString {
        value: &mut value,
        bound,
    })
    .map_err(|_| {
        DataFusionError::Execution("structured SQL result exceeded its preflight bound".into())
    })?;
    Ok(value)
}

struct BoundedString<'a> {
    value: &'a mut String,
    bound: usize,
}
impl std::fmt::Write for BoundedString<'_> {
    fn write_str(&mut self, text: &str) -> std::fmt::Result {
        self.value
            .len()
            .checked_add(text.len())
            .filter(|length| *length <= self.bound)
            .ok_or(std::fmt::Error)?;
        self.value.push_str(text);
        Ok(())
    }
}

pub(crate) fn append_record_batch(
    batch: &RecordBatch,
    output: &mut JsonResultBuilder,
    cancel: Option<&QueryCancelCheck>,
) -> Result<()> {
    let schema = batch.schema();
    unique_fields(schema.fields())?;
    let encoder_options = EncoderOptions::default().with_explicit_nulls(true);
    let mut prepared = Prepared::default();
    for (field, array) in schema.fields().iter().zip(batch.columns()) {
        validate_type(array.as_ref())?;
        prepared.collect(field, array.as_ref(), &encoder_options, false)?;
    }

    let mut plan = JsonAllocationPlan::new();
    let mut checks = Cancel::new(cancel);
    checks.now()?;
    for row in 0..batch.num_rows() {
        checks.tick()?;
        plan.add_object(batch.num_columns())?;
        for (field, array) in schema.fields().iter().zip(batch.columns()) {
            plan.add_string_capacity(field.name().len())?;
            preflight_value(array.as_ref(), row, &mut plan, &prepared, &mut checks)?;
        }
    }
    if !prepared.floats.is_empty() {
        plan.require_float_scratch()?;
    }

    // The appender is declared before every partially built row/value below. On error,
    // Rust drops those allocations first, then its Drop truncates rows and releases charge.
    let mut appender = output.begin_batch(plan.bytes(), batch.num_rows())?;
    let mut float_scratch = Vec::new();
    if plan.float_scratch {
        float_scratch
            .try_reserve_exact(64)
            .map_err(allocation_error)?;
        appender.observe_capacity(64, float_scratch.capacity())?;
    }
    let mut checks = Cancel::new(cancel);
    checks.now()?;
    for row in 0..batch.num_rows() {
        checks.tick()?;
        let mut object = Map::new();
        for (field, array) in schema.fields().iter().zip(batch.columns()) {
            let key = copy_string(field.name(), &mut appender, &mut checks)?;
            let value = build_value(
                array.as_ref(),
                row,
                &mut appender,
                &mut prepared,
                &mut float_scratch,
                false,
                &mut checks,
            )?;
            object.insert(key, value);
        }
        appender.push_row(Value::Object(object));
    }
    if plan.float_scratch {
        let actual = float_scratch.capacity();
        drop(float_scratch);
        appender.observe_capacity(actual, 0)?;
    }
    appender.finish()
}

#[cfg(test)]
pub(crate) fn record_batches_to_json(
    batches: &[RecordBatch],
) -> Result<crate::result::QueryJsonRows> {
    use logex_types::{QueryMemoryBudget, QueryMemoryLimit};
    let memory = QueryMemoryBudget::new(QueryMemoryLimit::new(isize::MAX as usize).unwrap());
    let mut output = JsonResultBuilder::new(memory)?;
    for batch in batches {
        append_record_batch(batch, &mut output, None)?;
    }
    Ok(output.finish())
}

struct Cancel<'a> {
    check: Option<&'a QueryCancelCheck>,
    remaining: usize,
}
impl<'a> Cancel<'a> {
    fn new(check: Option<&'a QueryCancelCheck>) -> Self {
        Self {
            check,
            remaining: CANCEL_VALUES,
        }
    }
    fn now(&self) -> Result<()> {
        if self.check.is_some_and(|check| check()) {
            Err(DataFusionError::Execution("query canceled".into()))
        } else {
            Ok(())
        }
    }
    fn tick(&mut self) -> Result<()> {
        self.remaining -= 1;
        if self.remaining == 0 {
            self.now()?;
            self.remaining = CANCEL_VALUES;
        }
        Ok(())
    }
}

#[derive(Default)]
struct Prepared<'a> {
    floats: Vec<PreparedFloat<'a>>,
    temporals: Vec<PreparedTemporal<'a>>,
}

struct PreparedFloat<'a> {
    array: *const (),
    encoder: NullableEncoder<'a>,
}

struct PreparedTemporal<'a> {
    array: *const (),
    formatter: ArrayFormatter<'a>,
}

impl<'a> Prepared<'a> {
    fn collect(
        &mut self,
        field: &'a FieldRef,
        array: &'a dyn Array,
        options: &'a EncoderOptions,
        nested: bool,
    ) -> std::result::Result<(), ArrowError> {
        macro_rules! dict {
            ($key:ty) => {{
                let dictionary: &'a DictionaryArray<$key> = downcast(array)?;
                self.collect(field, dictionary.values().as_ref(), options, true)?;
            }};
        }
        match array.data_type() {
            DataType::Float16 | DataType::Float32 => self.floats.push(PreparedFloat {
                array: array_id(array),
                encoder: make_encoder(field, array, options)?,
            }),
            DataType::Float64 if nested => self.floats.push(PreparedFloat {
                array: array_id(array),
                encoder: make_encoder(field, array, options)?,
            }),
            DataType::List(child) => {
                let list: &'a ListArray = downcast(array)?;
                self.collect(child, list.values().as_ref(), options, true)?;
            }
            DataType::LargeList(child) => {
                let list: &'a LargeListArray = downcast(array)?;
                self.collect(child, list.values().as_ref(), options, true)?;
            }
            DataType::FixedSizeList(child, _) => {
                let list: &'a FixedSizeListArray = downcast(array)?;
                self.collect(child, list.values().as_ref(), options, true)?;
            }
            DataType::Struct(fields) => {
                let values: &'a StructArray = downcast(array)?;
                for (field, child) in fields.iter().zip(values.columns()) {
                    self.collect(field, child.as_ref(), options, true)?;
                }
            }
            DataType::Map(entries, _) => {
                let map: &'a MapArray = downcast(array)?;
                self.collect(
                    map_value_field(entries),
                    map.values().as_ref(),
                    options,
                    true,
                )?;
            }
            DataType::Dictionary(key, _) => match key.as_ref() {
                DataType::Int8 => dict!(Int8Type),
                DataType::Int16 => dict!(Int16Type),
                DataType::Int32 => dict!(Int32Type),
                DataType::Int64 => dict!(Int64Type),
                DataType::UInt8 => dict!(UInt8Type),
                DataType::UInt16 => dict!(UInt16Type),
                DataType::UInt32 => dict!(UInt32Type),
                DataType::UInt64 => dict!(UInt64Type),
                _ => unreachable!("validated dictionary key type"),
            },
            kind if kind.is_temporal() => self.temporals.push(PreparedTemporal {
                array: array_id(array),
                formatter: ArrayFormatter::try_new(array, &FormatOptions::default())?,
            }),
            _ => {}
        }
        Ok(())
    }

    fn temporal(&self, array: &dyn Array) -> &ArrayFormatter<'a> {
        let id = array_id(array);
        &self
            .temporals
            .iter()
            .find(|prepared| prepared.array == id)
            .expect("validated temporal array has a prepared formatter")
            .formatter
    }

    fn encode_float(
        &mut self,
        array: &dyn Array,
        row: usize,
        scratch: &mut Vec<u8>,
    ) -> Result<Value> {
        let id = array_id(array);
        let prepared = self
            .floats
            .iter_mut()
            .find(|prepared| prepared.array == id)
            .expect("validated float array has a prepared encoder");
        scratch.clear();
        prepared.encoder.encode(row, scratch);
        serde_json::from_slice(scratch).map_err(|error| {
            DataFusionError::Execution(format!("cannot encode SQL value as JSON: {error}"))
        })
    }
}

fn array_id(array: &dyn Array) -> *const () {
    array as *const dyn Array as *const ()
}

fn validate_type(array: &dyn Array) -> std::result::Result<(), ArrowError> {
    macro_rules! dict {
        ($key:ty) => {{
            let array: &DictionaryArray<$key> = downcast(array)?;
            validate_type(array.values().as_ref())?;
        }};
    }
    match array.data_type() {
        DataType::List(_) => {
            let array: &ListArray = downcast(array)?;
            validate_type(array.values().as_ref())?;
        }
        DataType::LargeList(_) => {
            let array: &LargeListArray = downcast(array)?;
            validate_type(array.values().as_ref())?;
        }
        DataType::FixedSizeList(_, _) => {
            let array: &FixedSizeListArray = downcast(array)?;
            validate_type(array.values().as_ref())?;
        }
        DataType::Struct(fields) => {
            unique_fields(fields)?;
            let array: &StructArray = downcast(array)?;
            for child in array.columns() {
                validate_type(child.as_ref())?;
            }
        }
        DataType::Map(_, _) => {
            let array: &MapArray = downcast(array)?;
            if !matches!(
                array.keys().data_type(),
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
            ) {
                return Err(ArrowError::JsonError(format!(
                    "Only UTF8 keys supported by JSON MapArray Writer: got {:?}",
                    array.keys().data_type()
                )));
            }
            if array.keys().null_count() != 0 {
                return Err(ArrowError::InvalidArgumentError(
                    "Encountered nulls in MapArray keys".into(),
                ));
            }
            if array
                .entries()
                .nulls()
                .is_some_and(|nulls| nulls.null_count() != 0)
            {
                return Err(ArrowError::InvalidArgumentError(
                    "Encountered nulls in MapArray entries".into(),
                ));
            }
            validate_type(array.values().as_ref())?;
        }
        DataType::Dictionary(key, _) => match key.as_ref() {
            DataType::Int8 => dict!(Int8Type),
            DataType::Int16 => dict!(Int16Type),
            DataType::Int32 => dict!(Int32Type),
            DataType::Int64 => dict!(Int64Type),
            DataType::UInt8 => dict!(UInt8Type),
            DataType::UInt16 => dict!(UInt16Type),
            DataType::UInt32 => dict!(UInt32Type),
            DataType::UInt64 => dict!(UInt64Type),
            _ => {
                return Err(ArrowError::JsonError(
                    "unsupported SQL dictionary key type".into(),
                ));
            }
        },
        DataType::Null
        | DataType::Boolean
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Float16
        | DataType::Float32
        | DataType::Float64
        | DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Utf8View
        | DataType::Binary
        | DataType::LargeBinary
        | DataType::FixedSizeBinary(_)
        | DataType::BinaryView
        | DataType::Decimal32(_, _)
        | DataType::Decimal64(_, _)
        | DataType::Decimal128(_, _)
        | DataType::Decimal256(_, _) => {}
        kind if kind.is_temporal() => {
            ArrayFormatter::try_new(array, &FormatOptions::default())?;
        }
        kind => {
            return Err(ArrowError::JsonError(format!(
                "Unsupported data type for JSON encoding: {kind:?}"
            )));
        }
    }
    Ok(())
}

fn preflight_value(
    array: &dyn Array,
    row: usize,
    plan: &mut JsonAllocationPlan,
    prepared: &Prepared<'_>,
    cancel: &mut Cancel<'_>,
) -> Result<()> {
    cancel.tick()?;
    if array.is_null(row) {
        return Ok(());
    }
    macro_rules! text {
        ($ty:ty) => {
            plan.add_string_capacity(downcast::<$ty>(array)?.value(row).len())?
        };
    }
    macro_rules! binary {
        ($ty:ty) => {{
            let _ = plan.add_hex_string(downcast::<$ty>(array)?.value(row).len())?;
        }};
    }
    macro_rules! list {
        ($ty:ty) => {{
            let list: &$ty = downcast(array)?;
            let (start, end) = offsets(list.value_offsets(), row)?;
            plan.add_array_capacity(end - start)?;
            for index in start..end {
                preflight_value(list.values().as_ref(), index, plan, prepared, cancel)?;
            }
        }};
    }
    macro_rules! dict {
        ($key:ty) => {{
            let dictionary: &DictionaryArray<$key> = downcast(array)?;
            let index = dictionary_index(dictionary, row)?;
            preflight_value(dictionary.values().as_ref(), index, plan, prepared, cancel)?;
        }};
    }
    match array.data_type() {
        DataType::Utf8 => text!(StringArray),
        DataType::LargeUtf8 => text!(LargeStringArray),
        DataType::Utf8View => text!(StringViewArray),
        DataType::Binary => binary!(BinaryArray),
        DataType::LargeBinary => binary!(LargeBinaryArray),
        DataType::FixedSizeBinary(_) => binary!(FixedSizeBinaryArray),
        DataType::BinaryView => binary!(BinaryViewArray),
        DataType::List(_) => list!(ListArray),
        DataType::LargeList(_) => list!(LargeListArray),
        DataType::FixedSizeList(_, length) => {
            let list: &FixedSizeListArray = downcast(array)?;
            let length = usize::try_from(*length).map_err(|_| size_overflow())?;
            plan.add_array_capacity(length)?;
            let start = usize::try_from(list.value_offset(row)).map_err(|_| size_overflow())?;
            for index in start..start.checked_add(length).ok_or_else(size_overflow)? {
                preflight_value(list.values().as_ref(), index, plan, prepared, cancel)?;
            }
        }
        DataType::Struct(fields) => {
            let values: &StructArray = downcast(array)?;
            plan.add_object(fields.len())?;
            for (field, child) in fields.iter().zip(values.columns()) {
                plan.add_string_capacity(field.name().len())?;
                preflight_value(child.as_ref(), row, plan, prepared, cancel)?;
            }
        }
        DataType::Map(_, _) => {
            let map: &MapArray = downcast(array)?;
            let (start, end) = offsets(map.value_offsets(), row)?;
            plan.add_object(end - start)?;
            for index in start..end {
                plan.add_string_capacity(map_key(map, index)?.len())?;
                preflight_value(map.values().as_ref(), index, plan, prepared, cancel)?;
            }
        }
        DataType::Dictionary(key, _) => match key.as_ref() {
            DataType::Int8 => dict!(Int8Type),
            DataType::Int16 => dict!(Int16Type),
            DataType::Int32 => dict!(Int32Type),
            DataType::Int64 => dict!(Int64Type),
            DataType::UInt8 => dict!(UInt8Type),
            DataType::UInt16 => dict!(UInt16Type),
            DataType::UInt32 => dict!(UInt32Type),
            DataType::UInt64 => dict!(UInt64Type),
            _ => unreachable!("validated dictionary key type"),
        },
        DataType::Decimal32(p, s)
        | DataType::Decimal64(p, s)
        | DataType::Decimal128(p, s)
        | DataType::Decimal256(p, s) => {
            plan.add_string_capacity(decimal_bound(*p, *s)?)?;
        }
        kind if kind.is_temporal() => {
            let mut counter = ByteCounter(0);
            prepared.temporal(array).value(row).write(&mut counter)?;
            plan.add_string_capacity(counter.0)?;
        }
        _ => {}
    }
    Ok(())
}

fn build_value(
    array: &dyn Array,
    row: usize,
    output: &mut JsonBatchAppender<'_>,
    prepared: &mut Prepared<'_>,
    float_scratch: &mut Vec<u8>,
    nested: bool,
    cancel: &mut Cancel<'_>,
) -> Result<Value> {
    cancel.tick()?;
    if array.is_null(row) {
        return Ok(Value::Null);
    }
    macro_rules! integer {
        ($ty:ty) => {
            Value::Number(downcast::<$ty>(array)?.value(row).into())
        };
    }
    macro_rules! text {
        ($ty:ty) => {
            Value::String(copy_string(
                downcast::<$ty>(array)?.value(row),
                output,
                cancel,
            )?)
        };
    }
    macro_rules! binary {
        ($ty:ty) => {
            Value::String(copy_hex(
                downcast::<$ty>(array)?.value(row),
                output,
                cancel,
            )?)
        };
    }
    macro_rules! list {
        ($ty:ty) => {{
            let list: &$ty = downcast(array)?;
            let (start, end) = offsets(list.value_offsets(), row)?;
            Value::Array(build_array(
                list.values().as_ref(),
                start,
                end,
                output,
                prepared,
                float_scratch,
                cancel,
            )?)
        }};
    }
    macro_rules! dict {
        ($key:ty) => {{
            let dictionary: &DictionaryArray<$key> = downcast(array)?;
            let index = dictionary_index(dictionary, row)?;
            build_value(
                dictionary.values().as_ref(),
                index,
                output,
                prepared,
                float_scratch,
                true,
                cancel,
            )?
        }};
    }
    Ok(match array.data_type() {
        DataType::Null => Value::Null,
        DataType::Boolean => Value::Bool(downcast::<BooleanArray>(array)?.value(row)),
        DataType::Int8 => integer!(Int8Array),
        DataType::Int16 => integer!(Int16Array),
        DataType::Int32 => integer!(Int32Array),
        DataType::Int64 => integer!(Int64Array),
        DataType::UInt8 => integer!(UInt8Array),
        DataType::UInt16 => integer!(UInt16Array),
        DataType::UInt32 => integer!(UInt32Array),
        DataType::UInt64 => integer!(UInt64Array),
        DataType::Float16 | DataType::Float32 => {
            prepared.encode_float(array, row, float_scratch)?
        }
        DataType::Float64 if nested => prepared.encode_float(array, row, float_scratch)?,
        DataType::Float64 => {
            serde_json::Number::from_f64(downcast::<Float64Array>(array)?.value(row))
                .map_or(Value::Null, Value::Number)
        }
        DataType::Utf8 => text!(StringArray),
        DataType::LargeUtf8 => text!(LargeStringArray),
        DataType::Utf8View => text!(StringViewArray),
        DataType::Binary => binary!(BinaryArray),
        DataType::LargeBinary => binary!(LargeBinaryArray),
        DataType::FixedSizeBinary(_) => binary!(FixedSizeBinaryArray),
        DataType::BinaryView => binary!(BinaryViewArray),
        DataType::List(_) => list!(ListArray),
        DataType::LargeList(_) => list!(LargeListArray),
        DataType::FixedSizeList(_, length) => {
            let list: &FixedSizeListArray = downcast(array)?;
            let length = usize::try_from(*length).map_err(|_| size_overflow())?;
            let start = usize::try_from(list.value_offset(row)).map_err(|_| size_overflow())?;
            let end = start.checked_add(length).ok_or_else(size_overflow)?;
            Value::Array(build_array(
                list.values().as_ref(),
                start,
                end,
                output,
                prepared,
                float_scratch,
                cancel,
            )?)
        }
        DataType::Struct(fields) => {
            let values: &StructArray = downcast(array)?;
            let mut object = Map::new();
            for (field, child) in fields.iter().zip(values.columns()) {
                let key = copy_string(field.name(), output, cancel)?;
                let value = build_value(
                    child.as_ref(),
                    row,
                    output,
                    prepared,
                    float_scratch,
                    true,
                    cancel,
                )?;
                object.insert(key, value);
            }
            Value::Object(object)
        }
        DataType::Map(_, _) => {
            let map: &MapArray = downcast(array)?;
            let (start, end) = offsets(map.value_offsets(), row)?;
            let mut object = Map::new();
            for index in start..end {
                let key = copy_string(map_key(map, index)?, output, cancel)?;
                if object.contains_key(&key) {
                    return Err(ArrowError::JsonError(
                        "duplicate SQL map key cannot be represented as a JSON object".into(),
                    )
                    .into());
                }
                let value = build_value(
                    map.values().as_ref(),
                    index,
                    output,
                    prepared,
                    float_scratch,
                    true,
                    cancel,
                )?;
                object.insert(key, value);
            }
            Value::Object(object)
        }
        DataType::Dictionary(key, _) => match key.as_ref() {
            DataType::Int8 => dict!(Int8Type),
            DataType::Int16 => dict!(Int16Type),
            DataType::Int32 => dict!(Int32Type),
            DataType::Int64 => dict!(Int64Type),
            DataType::UInt8 => dict!(UInt8Type),
            DataType::UInt16 => dict!(UInt16Type),
            DataType::UInt32 => dict!(UInt32Type),
            DataType::UInt64 => dict!(UInt64Type),
            _ => unreachable!("validated dictionary key type"),
        },
        DataType::Decimal32(p, s) => Value::String(decimal_string::<Decimal32Type>(
            downcast::<PrimitiveArray<Decimal32Type>>(array)?.value(row),
            *p,
            *s,
            output,
        )?),
        DataType::Decimal64(p, s) => Value::String(decimal_string::<Decimal64Type>(
            downcast::<PrimitiveArray<Decimal64Type>>(array)?.value(row),
            *p,
            *s,
            output,
        )?),
        DataType::Decimal128(p, s) => Value::String(decimal_string::<Decimal128Type>(
            downcast::<PrimitiveArray<Decimal128Type>>(array)?.value(row),
            *p,
            *s,
            output,
        )?),
        DataType::Decimal256(p, s) => Value::String(decimal_i256_string(
            downcast::<PrimitiveArray<Decimal256Type>>(array)?.value(row),
            *p,
            *s,
            output,
        )?),
        kind if kind.is_temporal() => {
            let value = prepared.temporal(array).value(row);
            let mut counter = ByteCounter(0);
            value.write(&mut counter)?;
            let mut string = String::new();
            string
                .try_reserve_exact(counter.0)
                .map_err(allocation_error)?;
            output.observe_capacity(counter.0, string.capacity())?;
            value.write(&mut BoundedString {
                value: &mut string,
                bound: counter.0,
            })?;
            Value::String(string)
        }
        _ => unreachable!("validated JSON type"),
    })
}

fn build_array(
    array: &dyn Array,
    start: usize,
    end: usize,
    output: &mut JsonBatchAppender<'_>,
    prepared: &mut Prepared<'_>,
    float_scratch: &mut Vec<u8>,
    cancel: &mut Cancel<'_>,
) -> Result<Vec<Value>> {
    let mut result = allocate_json_array(end - start, output)?;
    for index in start..end {
        result.push(build_value(
            array,
            index,
            output,
            prepared,
            float_scratch,
            true,
            cancel,
        )?);
    }
    Ok(result)
}

fn copy_string(
    value: &str,
    output: &mut JsonBatchAppender<'_>,
    cancel: &mut Cancel<'_>,
) -> Result<String> {
    let mut result = String::new();
    result
        .try_reserve_exact(value.len())
        .map_err(allocation_error)?;
    output.observe_capacity(value.len(), result.capacity())?;
    let mut start = 0;
    while start < value.len() {
        let mut end = (start + CANCEL_BYTES).min(value.len());
        while end < value.len() && !value.is_char_boundary(end) {
            end += 1;
        }
        result.push_str(&value[start..end]);
        if value.len() > CANCEL_BYTES {
            cancel.now()?;
        }
        start = end;
    }
    Ok(result)
}

fn copy_hex(
    value: &[u8],
    output: &mut JsonBatchAppender<'_>,
    cancel: &mut Cancel<'_>,
) -> Result<String> {
    let bound = value.len().checked_mul(2).ok_or_else(size_overflow)?;
    let mut result = String::new();
    result.try_reserve_exact(bound).map_err(allocation_error)?;
    output.observe_capacity(bound, result.capacity())?;
    for chunk in value.chunks(CANCEL_BYTES) {
        write_hex(&mut result, chunk).expect("String writes cannot fail");
        if value.len() > CANCEL_BYTES {
            cancel.now()?;
        }
    }
    Ok(result)
}

fn decimal_bound(precision: u8, scale: i8) -> Result<usize> {
    (precision as usize)
        .checked_add(scale.min(0).unsigned_abs() as usize)
        .and_then(|value| value.checked_add(3))
        .ok_or_else(size_overflow)
}

fn decimal_string<T: DecimalType>(
    value: T::Native,
    precision: u8,
    scale: i8,
    output: &mut JsonBatchAppender<'_>,
) -> Result<String>
where
    T::Native: DecimalNative,
{
    format_decimal_digits(value.decimal_text(), precision, scale, output)
}

fn decimal_i256_string(
    value: i256,
    precision: u8,
    scale: i8,
    output: &mut JsonBatchAppender<'_>,
) -> Result<String> {
    let negative = value < i256::ZERO;
    let magnitude = value.wrapping_abs().to_le_bytes();
    let mut limbs = [0_u64; 4];
    let (chunks, remainder) = magnitude.as_chunks::<8>();
    debug_assert!(remainder.is_empty());
    for (limb, bytes) in limbs.iter_mut().zip(chunks) {
        *limb = u64::from_le_bytes(*bytes);
    }
    let mut digits = [0_u8; 77];
    let mut cursor = digits.len();
    loop {
        let mut remainder = 0_u128;
        for limb in limbs.iter_mut().rev() {
            let dividend = (remainder << 64) | *limb as u128;
            *limb = (dividend / 10) as u64;
            remainder = dividend % 10;
        }
        cursor -= 1;
        digits[cursor] = b'0' + remainder as u8;
        if limbs.iter().all(|limb| *limb == 0) {
            break;
        }
    }
    format_decimal_digits(
        DecimalText {
            negative,
            digits,
            start: cursor,
        },
        precision,
        scale,
        output,
    )
}

struct DecimalText {
    negative: bool,
    digits: [u8; 77],
    start: usize,
}

impl DecimalText {
    fn digits(&self) -> &str {
        std::str::from_utf8(&self.digits[self.start..]).expect("decimal digits are UTF-8")
    }
}

trait DecimalNative {
    fn decimal_text(self) -> DecimalText;
}

macro_rules! decimal_native {
    ($($type:ty),+) => {$(
        impl DecimalNative for $type {
            fn decimal_text(self) -> DecimalText {
                let negative = self < 0;
                let mut magnitude = self.unsigned_abs() as u128;
                let mut digits = [0_u8; 77];
                let mut cursor = digits.len();
                loop {
                    cursor -= 1;
                    digits[cursor] = b'0' + (magnitude % 10) as u8;
                    magnitude /= 10;
                    if magnitude == 0 {
                        break;
                    }
                }
                DecimalText { negative, digits, start: cursor }
            }
        }
    )+};
}
decimal_native!(i32, i64, i128);

fn format_decimal_digits(
    raw: DecimalText,
    precision: u8,
    scale: i8,
    output: &mut JsonBatchAppender<'_>,
) -> Result<String> {
    let bound = decimal_bound(precision, scale)?;
    let mut value = String::new();
    value.try_reserve_exact(bound).map_err(allocation_error)?;
    output.observe_capacity(bound, value.capacity())?;
    let digits = raw.digits();
    let original_digits = digits.len();
    let retained_digits = original_digits.min(precision as usize);
    if raw.negative {
        value.push('-');
    }
    if scale == 0 {
        value.push_str(&digits[..retained_digits]);
        return Ok(value);
    }
    if scale < 0 {
        value.push_str(&digits[..retained_digits]);
        for _ in 0..scale.unsigned_abs() {
            value.push('0');
        }
        return Ok(value);
    }
    let scale = scale as usize;
    if original_digits > scale {
        let whole = retained_digits - scale;
        value.push_str(&digits[..whole]);
        value.push('.');
        value.push_str(&digits[whole..retained_digits]);
    } else {
        value.push_str("0.");
        for _ in original_digits..scale {
            value.push('0');
        }
        value.push_str(digits);
    }
    Ok(value)
}

fn dictionary_index<K: ArrowDictionaryKeyType>(
    array: &DictionaryArray<K>,
    row: usize,
) -> std::result::Result<usize, ArrowError> {
    let key = array.keys().value(row).as_usize();
    if key >= array.values().len() {
        return Err(ArrowError::JsonError(format!(
            "SQL dictionary key {key} is out of bounds for {} values",
            array.values().len()
        )));
    }
    Ok(key)
}

fn offsets<T: ArrowNativeType>(
    offsets: &[T],
    row: usize,
) -> std::result::Result<(usize, usize), ArrowError> {
    let start = offsets[row].as_usize();
    let end = offsets[row + 1].as_usize();
    if start > end {
        return Err(ArrowError::JsonError(
            "SQL list/map offsets are not monotonic".into(),
        ));
    }
    Ok((start, end))
}

fn map_key(map: &MapArray, index: usize) -> std::result::Result<&str, ArrowError> {
    match map.keys().data_type() {
        DataType::Utf8 => Ok(downcast::<StringArray>(map.keys().as_ref())?.value(index)),
        DataType::LargeUtf8 => Ok(downcast::<LargeStringArray>(map.keys().as_ref())?.value(index)),
        DataType::Utf8View => Ok(downcast::<StringViewArray>(map.keys().as_ref())?.value(index)),
        _ => unreachable!("validated map key type"),
    }
}
fn map_value_field(entries: &FieldRef) -> &FieldRef {
    match entries.data_type() {
        DataType::Struct(fields) if fields.len() == 2 => &fields[1],
        _ => entries,
    }
}

struct ByteCounter(usize);
impl std::fmt::Write for ByteCounter {
    fn write_str(&mut self, value: &str) -> std::fmt::Result {
        self.0 = self.0.checked_add(value.len()).ok_or(std::fmt::Error)?;
        Ok(())
    }
}

fn downcast<T: 'static>(array: &dyn Array) -> std::result::Result<&T, ArrowError> {
    array.as_any().downcast_ref().ok_or_else(|| {
        ArrowError::JsonError(format!(
            "SQL array does not match its declared type {}",
            array.data_type()
        ))
    })
}

fn unique_fields(fields: &Fields) -> std::result::Result<(), ArrowError> {
    unique_names(fields.iter().map(|field| field.name().as_str()))
}

pub(crate) fn unique_names<'a>(
    names: impl IntoIterator<Item = &'a str>,
) -> std::result::Result<(), ArrowError> {
    let mut names = names.into_iter();
    let Some(first) = names.next() else {
        return Ok(());
    };
    let Some(second) = names.next() else {
        return Ok(());
    };
    let mut seen = HashSet::new();
    seen.insert(first);
    for name in std::iter::once(second).chain(names) {
        if !seen.insert(name) {
            return Err(ArrowError::JsonError(format!(
                "duplicate SQL JSON field {name:?}; use unique field names or aliases"
            )));
        }
    }
    Ok(())
}

fn value_slots(capacity: usize) -> Result<usize> {
    capacity
        .checked_mul(std::mem::size_of::<Value>())
        .ok_or_else(size_overflow)
}
fn size_overflow() -> DataFusionError {
    DataFusionError::External(Box::new(QueryMemoryError::SizeOverflow {
        stage: RESULT_STAGE,
    }))
}
fn allocation_error(error: std::collections::TryReserveError) -> DataFusionError {
    DataFusionError::ResourcesExhausted(format!("cannot allocate structured SQL result: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{
        ArrayRef, Date32Array, Decimal32Array, Decimal64Array, Decimal128Array, Decimal256Array,
        FixedSizeListArray, Int8Array, Int32Builder, LargeListArray, ListBuilder, MapBuilder,
        NullArray, RecordBatchOptions, StringBuilder, StructArray,
    };
    use datafusion::arrow::buffer::{NullBuffer, ScalarBuffer};
    use datafusion::arrow::datatypes::{Date32Type, Field, Schema, i256};
    use serde_json::json;
    use std::sync::Arc;

    fn values(array: ArrayRef) -> Result<Vec<Value>> {
        let batch = RecordBatch::try_from_iter([("value", array)])?;
        Ok(record_batches_to_json(&[batch])?
            .iter()
            .map(|row| row["value"].clone())
            .collect())
    }

    #[test]
    fn hex_writer_matches_reference_across_scratch_boundaries() {
        for length in [0, 20, 32, 33, 4_096, 4_097] {
            let bytes = (0..length)
                .map(|index| (index as u8).wrapping_mul(37).wrapping_add(11))
                .collect::<Vec<_>>();
            let mut actual = String::new();
            write_hex(&mut actual, &bytes).unwrap();
            assert_eq!(actual, hex::encode(bytes));
        }
    }

    #[test]
    fn null_dictionary_values_remain_null() {
        let dictionary = DictionaryArray::<Int8Type>::try_new(
            Int8Array::from(vec![Some(0), Some(1), None]),
            Arc::new(StringArray::from(vec![None, Some("text")])),
        )
        .unwrap();
        assert_eq!(
            values(Arc::new(dictionary)).unwrap(),
            vec![Value::Null, json!("text"), Value::Null]
        );
        let all_null = DictionaryArray::<Int8Type>::try_new(
            Int8Array::from(vec![0]),
            Arc::new(NullArray::new(1)),
        )
        .unwrap();
        assert_eq!(values(Arc::new(all_null)).unwrap(), vec![Value::Null]);
    }

    #[test]
    fn unrepresentable_temporal_values_are_errors() {
        assert!(values(Arc::new(Date32Array::from(vec![i32::MAX]))).is_err());
        let nested = StructArray::from(vec![(
            Arc::new(Field::new("date", DataType::Date32, false)),
            Arc::new(Date32Array::from(vec![i32::MAX])) as ArrayRef,
        )]);
        assert!(values(Arc::new(nested)).is_err());
        let null = Date32Array::new(
            ScalarBuffer::from(vec![i32::MAX]),
            Some(NullBuffer::new_null(1)),
        );
        assert_eq!(values(Arc::new(null)).unwrap(), vec![Value::Null]);
    }

    #[test]
    fn hidden_temporal_values_do_not_invalidate_logical_nulls() {
        let dates: ArrayRef = Arc::new(Date32Array::from(vec![0, i32::MAX]));
        let hidden = StructArray::new(
            vec![Field::new("date", DataType::Date32, false)].into(),
            vec![dates.slice(1, 1)],
            Some(NullBuffer::new_null(1)),
        );
        assert_eq!(values(Arc::new(hidden)).unwrap(), vec![Value::Null]);
        let dictionary =
            DictionaryArray::<Int8Type>::try_new(Int8Array::from(vec![Some(0), None]), dates)
                .unwrap();
        assert_eq!(
            values(Arc::new(dictionary)).unwrap(),
            vec![json!("1970-01-01"), Value::Null]
        );
        let dates: ArrayRef = Arc::new(ListArray::from_iter_primitive::<Date32Type, _, _>(vec![
            None,
            Some(vec![Some(0)]),
            Some(vec![Some(i32::MAX)]),
        ]));
        assert_eq!(
            values(dates.slice(0, 2)).unwrap(),
            vec![Value::Null, json!(["1970-01-01"])]
        );
        assert!(values(dates).is_err());
    }

    #[test]
    fn map_key_checks_follow_slices_and_dictionary_references() {
        let mut builder = MapBuilder::new(None, StringBuilder::new(), Int32Builder::new());
        builder.keys().append_value("valid");
        builder.values().append_value(1);
        builder.append(true).unwrap();
        for value in [1, 2] {
            builder.keys().append_value("duplicate");
            builder.values().append_value(value);
        }
        builder.append(true).unwrap();
        let maps: ArrayRef = Arc::new(builder.finish());
        assert_eq!(values(maps.slice(0, 1)).unwrap(), vec![json!({"valid":1})]);
        assert!(values(maps.slice(1, 1)).is_err());
        let dictionary =
            DictionaryArray::<Int8Type>::try_new(Int8Array::from(vec![0]), maps.clone()).unwrap();
        assert_eq!(
            values(Arc::new(dictionary)).unwrap(),
            vec![json!({"valid":1})]
        );
        let dictionary =
            DictionaryArray::<Int8Type>::try_new(Int8Array::from(vec![1]), maps).unwrap();
        assert!(values(Arc::new(dictionary)).is_err());
    }

    #[test]
    fn decimals_preserve_sign_scale_precision_and_nulls() {
        let cases: Vec<(ArrayRef, Vec<Value>)> = vec![
            (
                Arc::new(
                    Decimal32Array::from(vec![Some(-12345), None, Some(0)])
                        .with_precision_and_scale(9, 3)
                        .unwrap(),
                ),
                vec![json!("-12.345"), Value::Null, json!("0.000")],
            ),
            (
                Arc::new(
                    Decimal64Array::from(vec![Some(9_007_199_254_740_993)])
                        .with_precision_and_scale(18, 0)
                        .unwrap(),
                ),
                vec![json!("9007199254740993")],
            ),
            (
                Arc::new(
                    Decimal128Array::from(vec![
                        Some(12345678901234567890123456789012345678),
                        Some(-123),
                    ])
                    .with_precision_and_scale(38, 18)
                    .unwrap(),
                ),
                vec![
                    json!("12345678901234567890.123456789012345678"),
                    json!("-0.000000000000000123"),
                ],
            ),
            (
                Arc::new(
                    Decimal256Array::from(vec![Some(
                        "123456789012345678901234567890123456789012345678901234567890"
                            .parse::<i256>()
                            .unwrap(),
                    )])
                    .with_precision_and_scale(76, 0)
                    .unwrap(),
                ),
                vec![json!(
                    "123456789012345678901234567890123456789012345678901234567890"
                )],
            ),
            (
                Arc::new(
                    Decimal128Array::from(vec![Some(-123), Some(0)])
                        .with_precision_and_scale(38, -3)
                        .unwrap(),
                ),
                vec![json!("-123000"), json!("0000")],
            ),
        ];
        for (array, expected) in cases {
            assert_eq!(values(array.clone()).unwrap(), expected);
            let fields = vec![(
                Arc::new(Field::new("amount", array.data_type().clone(), true)),
                array,
            )];
            assert_eq!(
                values(Arc::new(StructArray::from(fields))).unwrap(),
                expected
                    .into_iter()
                    .map(|amount| json!({"amount":amount}))
                    .collect::<Vec<_>>()
            );
        }
    }

    #[test]
    fn decimal_fixed_scratch_matches_arrow_for_extreme_and_overprecision_values() {
        use logex_types::{QueryMemoryBudget, QueryMemoryLimit};

        let cases: Vec<ArrayRef> = vec![
            Arc::new(
                Decimal32Array::from(vec![Some(i32::MIN), Some(-12_345)])
                    .with_precision_and_scale(3, 2)
                    .unwrap(),
            ),
            Arc::new(
                Decimal64Array::from(vec![Some(i64::MIN), Some(12_345)])
                    .with_precision_and_scale(3, -2)
                    .unwrap(),
            ),
            Arc::new(
                Decimal128Array::from(vec![Some(i128::MIN), Some(12_345)])
                    .with_precision_and_scale(3, 3)
                    .unwrap(),
            ),
            Arc::new(
                Decimal256Array::from(vec![Some(i256::MIN), Some(i256::from_i128(12_345))])
                    .with_precision_and_scale(76, 18)
                    .unwrap(),
            ),
        ];
        let expected = cases
            .iter()
            .map(|array| {
                let formatter =
                    ArrayFormatter::try_new(array.as_ref(), &FormatOptions::default()).unwrap();
                (0..array.len())
                    .map(|row| Value::String(formatter.value(row).try_to_string().unwrap()))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();

        let memory = QueryMemoryBudget::new(QueryMemoryLimit::new(1 << 20).unwrap());
        let mut output = JsonResultBuilder::new(memory.clone()).unwrap();
        for array in &cases {
            let batch = RecordBatch::try_from_iter([("value", array.clone())]).unwrap();
            append_record_batch(&batch, &mut output, None).unwrap();
        }
        let rows = output.finish();
        let actual = rows
            .iter()
            .map(|row| row["value"].clone())
            .collect::<Vec<_>>();
        assert_eq!(actual, expected.into_iter().flatten().collect::<Vec<_>>());
        let admitted = usize::try_from(memory.used()).unwrap();
        assert!(admitted > 1);
        drop(rows);
        assert_eq!(memory.used(), 0);

        let tight = QueryMemoryBudget::new(QueryMemoryLimit::new(admitted - 1).unwrap());
        let mut output = JsonResultBuilder::new(tight.clone()).unwrap();
        let mut rejected = false;
        for array in cases {
            let batch = RecordBatch::try_from_iter([("value", array)]).unwrap();
            if append_record_batch(&batch, &mut output, None).is_err() {
                rejected = true;
                break;
            }
        }
        assert!(rejected);
        drop(output);
        assert_eq!(tight.used(), 0);
    }

    #[test]
    fn string_layouts_and_existing_scalar_semantics_are_preserved() {
        let text = vec![
            Some("\"quoted\"\\\n雪"),
            None,
            Some(""),
            Some("a string longer than the inline view"),
        ];
        let expected: Vec<Value> = text.iter().map(|s| json!(s)).collect();
        for array in [
            Arc::new(StringArray::from(text.clone())) as ArrayRef,
            Arc::new(LargeStringArray::from(text.clone())),
            Arc::new(StringViewArray::from(text)),
        ] {
            assert_eq!(values(array).unwrap(), expected);
        }
        assert_eq!(
            values(Arc::new(UInt64Array::from(vec![u64::MAX]))).unwrap(),
            vec![json!(u64::MAX)]
        );
        assert_eq!(
            values(Arc::new(Int64Array::from(vec![i64::MIN]))).unwrap(),
            vec![json!(i64::MIN)]
        );
        assert_eq!(
            values(Arc::new(Float64Array::from(vec![
                Some(1.25),
                None,
                Some(f64::NAN),
                Some(f64::INFINITY),
                Some(f64::NEG_INFINITY)
            ])))
            .unwrap(),
            vec![
                json!(1.25),
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Null
            ]
        );
    }

    #[test]
    fn list_layouts_and_slices_preserve_nulls_and_empty_values() {
        let input = vec![
            Some(vec![Some(1), None]),
            None,
            Some(vec![]),
            Some(vec![Some(-7)]),
        ];
        let expected = vec![json!([1, null]), Value::Null, json!([]), json!([-7])];
        for array in [
            Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(
                input.clone(),
            )) as ArrayRef,
            Arc::new(LargeListArray::from_iter_primitive::<Int32Type, _, _>(
                input,
            )),
        ] {
            assert_eq!(values(array.clone()).unwrap(), expected);
            assert_eq!(values(array.slice(1, 2)).unwrap(), expected[1..3]);
        }
        let fixed = FixedSizeListArray::from_iter_primitive::<Int32Type, _, _>(
            vec![
                Some(vec![Some(1), None]),
                None,
                Some(vec![Some(-2), Some(3)]),
            ],
            2,
        );
        assert_eq!(
            values(Arc::new(fixed)).unwrap(),
            vec![json!([1, null]), Value::Null, json!([-2, 3])]
        );
        let mut strings = ListBuilder::new(StringBuilder::new());
        strings.values().append_value("hello");
        strings.values().append_null();
        strings.append(true);
        strings.append(false);
        strings.append(true);
        assert_eq!(
            values(Arc::new(strings.finish())).unwrap(),
            vec![json!(["hello", null]), Value::Null, json!([])]
        );
    }

    #[test]
    fn maps_preserve_nulls_and_reject_lossy_keys() {
        let mut map = MapBuilder::new(None, StringBuilder::new(), Int32Builder::new());
        map.keys().append_value("key\"雪");
        map.values().append_null();
        map.append(true).unwrap();
        map.append(false).unwrap();
        map.append(true).unwrap();
        assert_eq!(
            values(Arc::new(map.finish())).unwrap(),
            vec![json!({"key\"雪":null}), Value::Null, json!({})]
        );
        for value in [1, 2] {
            map.keys().append_value("duplicate");
            map.values().append_value(value);
        }
        map.append(true).unwrap();
        assert!(values(Arc::new(map.finish())).is_err());
        let mut numeric = MapBuilder::new(None, Int32Builder::new(), Int32Builder::new());
        numeric.keys().append_value(1);
        numeric.values().append_value(2);
        numeric.append(true).unwrap();
        assert!(values(Arc::new(numeric.finish())).is_err());
    }

    #[test]
    fn dictionaries_preserve_nested_values_and_logical_nulls() {
        let decimals = Decimal128Array::from(vec![Some(123), None])
            .with_precision_and_scale(38, 2)
            .unwrap();
        let dictionary = DictionaryArray::<UInt64Type>::try_new(
            UInt64Array::from(vec![Some(1), None, Some(0)]),
            Arc::new(decimals),
        )
        .unwrap();
        let nested = StructArray::from(vec![(
            Arc::new(Field::new("value", dictionary.data_type().clone(), true)),
            Arc::new(dictionary) as ArrayRef,
        )]);
        assert_eq!(
            values(Arc::new(nested)).unwrap(),
            vec![
                json!({"value":null}),
                json!({"value":null}),
                json!({"value":"1.23"})
            ]
        );
    }

    #[test]
    fn generated_nullable_lists_match_an_independent_value_oracle() {
        let mut state = 0x15_637_u64;
        let mut builder = ListBuilder::new(Int32Builder::new());
        let mut expected = Vec::new();
        for row in 0..512 {
            let mut items = Vec::new();
            for _ in 0..row % 11 {
                state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
                let value = (state as i32).checked_mul(2);
                builder.values().append_option(value);
                items.push(json!(value));
            }
            let valid = row % 7 != 0;
            builder.append(valid);
            expected.push(if valid {
                Value::Array(items)
            } else {
                Value::Null
            });
        }
        let array: ArrayRef = Arc::new(builder.finish());
        assert_eq!(values(array.clone()).unwrap(), expected);
        for offset in [0, 1, 63, 127, 255] {
            assert_eq!(
                values(array.slice(offset, 128)).unwrap(),
                expected[offset..offset + 128]
            );
        }
    }

    #[test]
    fn multiple_batches_empty_schema_and_duplicate_fields_are_explicit() {
        let a =
            RecordBatch::try_from_iter([("n", Arc::new(Int32Array::from(vec![1])) as ArrayRef)])
                .unwrap();
        assert_eq!(
            record_batches_to_json(&[a.clone(), a]).unwrap(),
            vec![json!({"n":1}), json!({"n":1})]
        );
        let empty = RecordBatch::try_new_with_options(
            Arc::new(Schema::empty()),
            vec![],
            &RecordBatchOptions::new().with_row_count(Some(2)),
        )
        .unwrap();
        assert_eq!(
            record_batches_to_json(&[empty]).unwrap(),
            vec![json!({}), json!({})]
        );
        let schema = Arc::new(Schema::new(vec![
            Field::new("n", DataType::Int32, false);
            2
        ]));
        let batch = RecordBatch::new_empty(schema);
        assert!(record_batches_to_json(&[batch]).is_err());
    }

    #[test]
    fn float32_and_binary_view_match_arrow_json_semantics() {
        use datafusion::arrow::json::writer::{EncoderOptions, make_encoder};

        fn arrow_oracle(array: &dyn Array) -> Vec<Value> {
            let field = Arc::new(Field::new("value", array.data_type().clone(), true));
            let options = EncoderOptions::default().with_explicit_nulls(true);
            let mut encoder = make_encoder(&field, array, &options).unwrap();
            let mut scratch = Vec::new();
            (0..array.len())
                .map(|row| {
                    if encoder.is_null(row) {
                        Value::Null
                    } else {
                        scratch.clear();
                        encoder.encode(row, &mut scratch);
                        serde_json::from_slice(&scratch).unwrap()
                    }
                })
                .collect()
        }

        let floats: ArrayRef = Arc::new(Float32Array::from(vec![
            Some(0.1),
            Some(f32::MIN_POSITIVE),
            Some(f32::MAX),
            Some(-0.0),
            Some(f32::NAN),
            None,
        ]));
        assert_eq!(
            values(floats.clone()).unwrap(),
            arrow_oracle(floats.as_ref())
        );

        let binary: ArrayRef = Arc::new(BinaryViewArray::from_iter([
            Some(&b"\x00\xab\xff"[..]),
            None,
            Some(&b"a payload longer than twelve bytes"[..]),
        ]));
        assert_eq!(
            values(binary.clone()).unwrap(),
            arrow_oracle(binary.as_ref())
        );
        assert_eq!(
            values(binary).unwrap(),
            vec![
                json!("00abff"),
                Value::Null,
                json!("61207061796c6f6164206c6f6e676572207468616e207477656c7665206279746573")
            ]
        );
    }

    #[test]
    fn nested_float64_matches_the_previous_arrow_encoder_oracle() {
        use datafusion::arrow::json::writer::{EncoderOptions, make_encoder};

        let mut state = 0x6a09_e667_f3bc_c909_u64;
        let mut input = Vec::new();
        while input.len() < 1_024 {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            let value = f64::from_bits(state);
            if value.is_finite() {
                input.push(value);
            }
        }
        input.extend([0.0, -0.0, 1.0, -1.0, f64::MIN_POSITIVE, f64::MAX]);
        let floats: ArrayRef = Arc::new(Float64Array::from(input.clone()));
        let nested: ArrayRef = Arc::new(StructArray::from(vec![(
            Arc::new(Field::new("number", DataType::Float64, false)),
            floats,
        )]));
        let field = Arc::new(Field::new("value", nested.data_type().clone(), false));
        let options = EncoderOptions::default().with_explicit_nulls(true);
        let mut encoder = make_encoder(&field, nested.as_ref(), &options).unwrap();
        let mut scratch = Vec::new();
        let expected = (0..nested.len())
            .map(|row| {
                scratch.clear();
                encoder.encode(row, &mut scratch);
                serde_json::from_slice(&scratch).unwrap()
            })
            .collect::<Vec<Value>>();
        drop(encoder);
        assert_eq!(values(nested).unwrap(), expected);

        assert_eq!(
            values(Arc::new(Float64Array::from(input.clone()))).unwrap(),
            input
                .into_iter()
                .map(|value| {
                    serde_json::Number::from_f64(value).map_or(Value::Null, Value::Number)
                })
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn capacity_and_cancellation_release_partial_batch_allocations() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        use logex_types::{QueryMemoryBudget, QueryMemoryLimit};

        let batch = RecordBatch::try_from_iter([(
            "large_name",
            Arc::new(StringArray::from(vec!["x".repeat(CANCEL_BYTES * 2)])) as ArrayRef,
        )])
        .unwrap();

        let tiny = QueryMemoryBudget::new(QueryMemoryLimit::new(1).unwrap());
        let mut output = JsonResultBuilder::new(tiny.clone()).unwrap();
        let error = append_record_batch(&batch, &mut output, None).unwrap_err();
        assert!(matches!(error, DataFusionError::External(_)));
        drop(output);
        assert_eq!(tiny.used(), 0);

        let memory = QueryMemoryBudget::new(QueryMemoryLimit::new(1 << 20).unwrap());
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = calls.clone();
        let cancel: QueryCancelCheck = Arc::new(move || {
            // Preflight and construction entry checks pass; copying the large value
            // is interrupted after the batch reservation and String allocation exist.
            observed.fetch_add(1, Ordering::Relaxed) >= 3
        });
        let mut output = JsonResultBuilder::new(memory.clone()).unwrap();
        let error = append_record_batch(&batch, &mut output, Some(&cancel)).unwrap_err();
        assert!(
            matches!(error, DataFusionError::Execution(message) if message == "query canceled")
        );
        drop(output);
        assert_eq!(memory.used(), 0);
    }

    #[test]
    fn nested_preflight_cancellation_stops_before_result_allocation() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        use logex_types::{QueryMemoryBudget, QueryMemoryLimit};

        let array: ArrayRef = Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>([Some(
            (0..1_024).map(Some).collect::<Vec<_>>(),
        )]));
        let batch = RecordBatch::try_from_iter([("nested", array)]).unwrap();
        let memory = QueryMemoryBudget::new(QueryMemoryLimit::new(1 << 20).unwrap());
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = calls.clone();
        let cancel: QueryCancelCheck =
            Arc::new(move || observed.fetch_add(1, Ordering::Relaxed) >= 1);
        let mut output = JsonResultBuilder::new(memory.clone()).unwrap();
        let error = append_record_batch(&batch, &mut output, Some(&cancel)).unwrap_err();
        assert!(
            matches!(error, DataFusionError::Execution(message) if message == "query canceled")
        );
        assert_eq!(memory.used(), 0);
    }

    #[test]
    fn batches_release_arrow_owners_while_accumulated_json_remains_charged() {
        use datafusion::arrow::buffer::{Buffer, OffsetBuffer};
        use logex_types::{QueryMemoryBudget, QueryMemoryLimit};

        struct SourceOwner {
            bytes: Vec<u8>,
            _lifetime: Arc<()>,
        }
        impl AsRef<[u8]> for SourceOwner {
            fn as_ref(&self) -> &[u8] {
                &self.bytes
            }
        }

        let lifetime = Arc::new(());
        let released = Arc::downgrade(&lifetime);
        let values = Buffer::from(bytes::Bytes::from_owner(SourceOwner {
            bytes: b"first batch payload".to_vec(),
            _lifetime: Arc::clone(&lifetime),
        }));
        drop(lifetime);
        let offsets = OffsetBuffer::new(ScalarBuffer::from(vec![0_i32, values.len() as i32]));
        let array: ArrayRef = Arc::new(StringArray::new(offsets, values, None));
        let first = RecordBatch::try_from_iter([("value", array)]).unwrap();

        let memory = QueryMemoryBudget::new(QueryMemoryLimit::new(1 << 20).unwrap());
        let mut output = JsonResultBuilder::new(memory.clone()).unwrap();
        append_record_batch(&first, &mut output, None).unwrap();
        assert!(released.upgrade().is_some());
        drop(first);
        assert!(released.upgrade().is_none());
        let after_first = memory.used();
        assert!(after_first > 0);

        let second = RecordBatch::try_from_iter([(
            "value",
            Arc::new(StringArray::from(vec!["second batch"])) as ArrayRef,
        )])
        .unwrap();
        append_record_batch(&second, &mut output, None).unwrap();
        let rows = output.finish();
        assert_eq!(
            rows.as_slice(),
            [
                json!({"value":"first batch payload"}),
                json!({"value":"second batch"})
            ]
        );
        assert!(memory.used() >= after_first);
        drop(rows);
        assert_eq!(memory.used(), 0);
    }

    #[test]
    fn fixed_size_list_slices_use_physical_child_offsets() {
        let array: ArrayRef = Arc::new(FixedSizeListArray::from_iter_primitive::<Int32Type, _, _>(
            vec![
                Some(vec![Some(1), Some(2)]),
                Some(vec![Some(3), Some(4)]),
                Some(vec![Some(5), Some(6)]),
            ],
            2,
        ));
        assert_eq!(values(array.slice(1, 1)).unwrap(), vec![json!([3, 4])]);
    }
}
