//! Convert SQL output without losing decimal precision or nested values.
use std::collections::HashSet;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, BooleanArray, DictionaryArray, Float64Array, Int32Array, Int64Array, LargeStringArray,
    ListArray, MapArray, PrimitiveArray, RecordBatch, StringArray, StringViewArray, UInt32Array,
    UInt64Array,
};
use datafusion::arrow::datatypes::{
    ArrowDictionaryKeyType, ArrowNativeType, DataType, Decimal32Type, Decimal64Type,
    Decimal128Type, Decimal256Type, DecimalType, FieldRef, Fields, Int8Type, Int16Type, Int32Type,
    Int64Type, UInt8Type, UInt16Type, UInt32Type, UInt64Type,
};
use datafusion::arrow::error::ArrowError;
use datafusion::arrow::json::writer::{
    Encoder, EncoderFactory, EncoderOptions, NullableEncoder, make_encoder,
};
use datafusion::arrow::util::display::{ArrayFormatter, FormatOptions};
use datafusion::error::{DataFusionError, Result};
use serde_json::{Map, Value};

pub(crate) fn record_batches_to_json(batches: &[RecordBatch]) -> Result<Vec<Value>> {
    let options = EncoderOptions::default()
        .with_explicit_nulls(true)
        .with_encoder_factory(Arc::new(SqlEncoderFactory));
    let mut rows = Vec::new();
    let mut scratch = Vec::new();
    for batch in batches {
        let schema = batch.schema();
        unique_fields(schema.fields())?;
        let mut columns = schema
            .fields()
            .iter()
            .zip(batch.columns())
            .map(|(field, array)| JsonColumn::new(field, array.as_ref(), &options))
            .collect::<std::result::Result<Vec<_>, _>>()?;
        rows.try_reserve(batch.num_rows()).map_err(|error| {
            DataFusionError::ResourcesExhausted(format!("cannot allocate SQL result rows: {error}"))
        })?;
        for row_index in 0..batch.num_rows() {
            let mut row = Map::with_capacity(columns.len());
            for (field, column) in schema.fields().iter().zip(&mut columns) {
                row.insert(field.name().clone(), column.value(row_index, &mut scratch)?);
            }
            rows.push(Value::Object(row));
        }
    }
    Ok(rows)
}

// Keep the established scalar/topic-list path free of JSON encoding and reparsing.
// Build other Arrow encoders once per column, reusing one value-sized scratch buffer.
struct JsonColumn<'a> {
    array: &'a dyn Array,
    encoding: ValueEncoding<'a>,
}

enum ValueEncoding<'a> {
    String(&'a StringArray),
    LargeString(&'a LargeStringArray),
    StringView(&'a StringViewArray),
    U64(&'a UInt64Array),
    U32(&'a UInt32Array),
    I64(&'a Int64Array),
    I32(&'a Int32Array),
    F64(&'a Float64Array),
    Bool(&'a BooleanArray),
    StringList(&'a ListArray),
    Temporal(ArrayFormatter<'a>),
    Encoded(NullableEncoder<'a>),
}

impl<'a> JsonColumn<'a> {
    fn new(
        field: &'a FieldRef,
        array: &'a dyn Array,
        options: &'a EncoderOptions,
    ) -> std::result::Result<Self, ArrowError> {
        let encoding = match array.data_type() {
            DataType::Utf8 => ValueEncoding::String(downcast(array)?),
            DataType::LargeUtf8 => ValueEncoding::LargeString(downcast(array)?),
            DataType::Utf8View => ValueEncoding::StringView(downcast(array)?),
            DataType::UInt64 => ValueEncoding::U64(downcast(array)?),
            DataType::UInt32 => ValueEncoding::U32(downcast(array)?),
            DataType::Int64 => ValueEncoding::I64(downcast(array)?),
            DataType::Int32 => ValueEncoding::I32(downcast(array)?),
            DataType::Float64 => ValueEncoding::F64(downcast(array)?),
            DataType::Boolean => ValueEncoding::Bool(downcast(array)?),
            DataType::List(child) if child.data_type() == &DataType::Utf8 => {
                ValueEncoding::StringList(downcast(array)?)
            }
            kind if kind.is_temporal() => {
                ValueEncoding::Temporal(ArrayFormatter::try_new(array, &FormatOptions::default())?)
            }
            _ => ValueEncoding::Encoded(make_encoder(field, array, options)?),
        };
        Ok(Self { array, encoding })
    }

    fn value(&mut self, row: usize, scratch: &mut Vec<u8>) -> Result<Value> {
        if self.array.is_null(row) {
            return Ok(Value::Null);
        }
        Ok(match &mut self.encoding {
            ValueEncoding::String(array) => Value::String(array.value(row).to_owned()),
            ValueEncoding::LargeString(array) => Value::String(array.value(row).to_owned()),
            ValueEncoding::StringView(array) => Value::String(array.value(row).to_owned()),
            ValueEncoding::U64(array) => Value::Number(array.value(row).into()),
            ValueEncoding::U32(array) => Value::Number(array.value(row).into()),
            ValueEncoding::I64(array) => Value::Number(array.value(row).into()),
            ValueEncoding::I32(array) => Value::Number(array.value(row).into()),
            ValueEncoding::F64(array) => {
                serde_json::Number::from_f64(array.value(row)).map_or(Value::Null, Value::Number)
            }
            ValueEncoding::Bool(array) => Value::Bool(array.value(row)),
            ValueEncoding::StringList(array) => {
                let values = array.value(row);
                let strings: &StringArray = downcast(values.as_ref())?;
                Value::Array(
                    strings
                        .iter()
                        .map(|s| s.map_or(Value::Null, |s| Value::String(s.to_owned())))
                        .collect(),
                )
            }
            ValueEncoding::Temporal(formatter) => {
                Value::String(formatter.value(row).try_to_string()?)
            }
            ValueEncoding::Encoded(encoder) => {
                if encoder.is_null(row) {
                    return Ok(Value::Null);
                }
                scratch.clear();
                encoder.encode(row, scratch);
                serde_json::from_slice(scratch).map_err(|error| {
                    DataFusionError::Execution(format!("cannot encode SQL value as JSON: {error}"))
                })?
            }
        })
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

#[derive(Debug)]
struct SqlEncoderFactory;

impl EncoderFactory for SqlEncoderFactory {
    fn make_default_encoder<'a>(
        &self,
        field: &'a FieldRef,
        array: &'a dyn Array,
        options: &'a EncoderOptions,
    ) -> std::result::Result<Option<NullableEncoder<'a>>, ArrowError> {
        macro_rules! decimal {
            ($type:ty) => {
                Some(NullableEncoder::new(
                    Box::new(DecimalEncoder::<$type>(downcast(array)?)),
                    array.nulls().cloned(),
                ))
            };
        }
        macro_rules! dictionary {
            ($type:ty) => {{
                let dictionary: &DictionaryArray<$type> = downcast(array)?;
                let values = make_encoder(field, dictionary.values().as_ref(), options)?;
                Some(NullableEncoder::new(
                    Box::new(DictionaryEncoder { dictionary, values }),
                    array.nulls().cloned(),
                ))
            }};
        }
        Ok(match array.data_type() {
            DataType::Decimal32(_, _) => decimal!(Decimal32Type),
            DataType::Decimal64(_, _) => decimal!(Decimal64Type),
            DataType::Decimal128(_, _) => decimal!(Decimal128Type),
            DataType::Decimal256(_, _) => decimal!(Decimal256Type),
            DataType::Struct(fields) => {
                unique_fields(fields)?;
                None
            }
            DataType::Map(_, _) => {
                unique_map_keys(downcast(array)?)?;
                None
            }
            DataType::Dictionary(key, _) => match key.as_ref() {
                DataType::Int8 => dictionary!(Int8Type),
                DataType::Int16 => dictionary!(Int16Type),
                DataType::Int32 => dictionary!(Int32Type),
                DataType::Int64 => dictionary!(Int64Type),
                DataType::UInt8 => dictionary!(UInt8Type),
                DataType::UInt16 => dictionary!(UInt16Type),
                DataType::UInt32 => dictionary!(UInt32Type),
                DataType::UInt64 => dictionary!(UInt64Type),
                _ => {
                    return Err(ArrowError::JsonError(
                        "unsupported SQL dictionary key type".to_owned(),
                    ));
                }
            },
            kind if kind.is_temporal() => {
                // Arrow's nested JSON formatter otherwise turns formatting
                // failures into successful "ERROR: ..." strings. Validate with
                // its fallible API before using that infallible encoder.
                let formatter = ArrayFormatter::try_new(array, &FormatOptions::default())?;
                for row in 0..array.len() {
                    if !array.is_null(row) {
                        formatter.value(row).write(&mut FormatSink)?;
                    }
                }
                None
            }
            _ => None,
        })
    }
}

struct FormatSink;
impl std::fmt::Write for FormatSink {
    fn write_str(&mut self, _: &str) -> std::fmt::Result {
        Ok(())
    }
}

struct DecimalEncoder<'a, T: DecimalType>(&'a PrimitiveArray<T>);
impl<T: DecimalType> Encoder for DecimalEncoder<'_, T> {
    fn encode(&mut self, row: usize, out: &mut Vec<u8>) {
        // Decimal formatting produces only a sign, digits and a decimal point.
        // Quote it to avoid rounding through serde_json's floating-point parser.
        out.push(b'"');
        out.extend_from_slice(self.0.value_as_string(row).as_bytes());
        out.push(b'"');
    }
}

// Arrow 57's default dictionary encoder does not check null dictionary VALUES.
// Keys can be non-null while referencing a null value; retain that logical null.
struct DictionaryEncoder<'a, K: ArrowDictionaryKeyType> {
    dictionary: &'a DictionaryArray<K>,
    values: NullableEncoder<'a>,
}
impl<K: ArrowDictionaryKeyType> Encoder for DictionaryEncoder<'_, K> {
    fn encode(&mut self, row: usize, out: &mut Vec<u8>) {
        let key = self.dictionary.keys().value(row).as_usize();
        if self.values.is_null(key) {
            out.extend_from_slice(b"null");
        } else {
            self.values.encode(key, out);
        }
    }
}

fn unique_map_keys(array: &MapArray) -> std::result::Result<(), ArrowError> {
    // Arrow's JSON map encoder accepts UTF8 keys; other key types remain errors.
    if array.keys().data_type() != &DataType::Utf8 {
        return Ok(());
    }
    let keys: &StringArray = downcast(array.keys().as_ref())?;
    for row in 0..array.len() {
        if array.is_null(row) {
            continue;
        }
        let mut seen = HashSet::new();
        let start = array.value_offsets()[row] as usize;
        let end = array.value_offsets()[row + 1] as usize;
        for key in start..end {
            if !keys.is_null(key) && !seen.insert(keys.value(key)) {
                return Err(ArrowError::JsonError(
                    "duplicate SQL map key cannot be represented as a JSON object".to_owned(),
                ));
            }
        }
    }
    Ok(())
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
    use datafusion::arrow::datatypes::{Field, Schema, i256};
    use serde_json::json;

    fn values(array: ArrayRef) -> Result<Vec<Value>> {
        let batch = RecordBatch::try_from_iter([("value", array)])?;
        Ok(record_batches_to_json(&[batch])?
            .into_iter()
            .map(|row| row["value"].clone())
            .collect())
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
}
