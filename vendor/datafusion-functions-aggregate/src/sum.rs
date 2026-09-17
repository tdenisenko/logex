// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Defines `SUM` and `SUM DISTINCT` aggregate accumulators

use ahash::RandomState;
use arrow::datatypes::DECIMAL32_MAX_PRECISION;
use arrow::datatypes::DECIMAL64_MAX_PRECISION;
use datafusion_expr::utils::AggregateOrderSensitivity;
use datafusion_expr::Expr;
use std::any::Any;
use std::mem::{size_of, size_of_val};
use std::sync::Arc;

use arrow::array::Array;
use arrow::array::ArrowNativeTypeOp;
use arrow::array::{ArrowNumericType, AsArray, BooleanArray, PrimitiveArray};
use arrow::datatypes::{ArrowNativeType, FieldRef};
use arrow::datatypes::{
    DataType, Decimal128Type, Decimal256Type, Decimal32Type, Decimal64Type, DecimalType,
    Float64Type, Int64Type, UInt64Type, DECIMAL128_MAX_PRECISION,
    DECIMAL256_MAX_PRECISION,
};
use arrow::{array::ArrayRef, datatypes::Field};
use datafusion_common::{
    exec_datafusion_err, exec_err, not_impl_err, utils::take_function_args, HashMap,
    Result, ScalarValue,
};
use datafusion_expr::function::AccumulatorArgs;
use datafusion_expr::function::StateFieldsArgs;
use datafusion_expr::utils::format_state_name;
use datafusion_expr::{
    Accumulator, AggregateUDFImpl, Documentation, EmitTo, GroupsAccumulator,
    ReversedUDAF, SetMonotonicity, Signature, Volatility,
};
use datafusion_functions_aggregate_common::aggregate::groups_accumulator::accumulate::NullState;
use datafusion_functions_aggregate_common::aggregate::groups_accumulator::nulls::{
    filtered_null_mask, set_nulls,
};
use datafusion_functions_aggregate_common::aggregate::groups_accumulator::prim_op::PrimitiveGroupsAccumulator;
use datafusion_functions_aggregate_common::aggregate::sum_distinct::DistinctSumAccumulator;
use datafusion_macros::user_doc;

make_udaf_expr_and_func!(
    Sum,
    sum,
    expression,
    "Returns the sum of a group of values.",
    sum_udaf
);

pub fn sum_distinct(expr: Expr) -> Expr {
    Expr::AggregateFunction(datafusion_expr::expr::AggregateFunction::new_udf(
        sum_udaf(),
        vec![expr],
        true,
        None,
        vec![],
        None,
    ))
}

/// Sum only supports a subset of numeric types, instead relying on type coercion
///
/// This macro is similar to [downcast_primitive](arrow::array::downcast_primitive)
///
/// `args` is [AccumulatorArgs]
/// `helper` is a macro accepting (ArrowPrimitiveType, DataType)
macro_rules! downcast_sum {
    ($args:ident, $helper:ident) => {
        match $args.return_field.data_type().clone() {
            DataType::UInt64 => {
                $helper!(UInt64Type, $args.return_field.data_type().clone())
            }
            DataType::Int64 => {
                $helper!(Int64Type, $args.return_field.data_type().clone())
            }
            DataType::Float64 => {
                $helper!(Float64Type, $args.return_field.data_type().clone())
            }
            DataType::Decimal32(_, _) => {
                $helper!(Decimal32Type, $args.return_field.data_type().clone())
            }
            DataType::Decimal64(_, _) => {
                $helper!(Decimal64Type, $args.return_field.data_type().clone())
            }
            DataType::Decimal128(_, _) => {
                $helper!(Decimal128Type, $args.return_field.data_type().clone())
            }
            DataType::Decimal256(_, _) => {
                $helper!(Decimal256Type, $args.return_field.data_type().clone())
            }
            _ => {
                not_impl_err!(
                    "Sum not supported for {}: {}",
                    $args.name,
                    $args.return_field.data_type()
                )
            }
        }
    };
}

#[user_doc(
    doc_section(label = "General Functions"),
    description = "Returns the sum of all values in the specified column.",
    syntax_example = "sum(expression)",
    sql_example = r#"```sql
> SELECT sum(column_name) FROM table_name;
+-----------------------+
| sum(column_name)       |
+-----------------------+
| 12345                 |
+-----------------------+
```"#,
    standard_argument(name = "expression",)
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct Sum {
    signature: Signature,
}

impl Sum {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl Default for Sum {
    fn default() -> Self {
        Self::new()
    }
}

impl AggregateUDFImpl for Sum {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "sum"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        let [args] = take_function_args(self.name(), arg_types)?;

        // Refer to https://www.postgresql.org/docs/8.2/functions-aggregate.html doc
        // smallint, int, bigint, real, double precision, decimal, or interval.

        fn coerced_type(data_type: &DataType) -> Result<DataType> {
            match data_type {
                DataType::Dictionary(_, v) => coerced_type(v),
                // in the spark, the result type is DECIMAL(min(38,precision+10), s)
                // ref: https://github.com/apache/spark/blob/fcf636d9eb8d645c24be3db2d599aba2d7e2955a/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/aggregate/Sum.scala#L66
                DataType::Decimal32(_, _)
                | DataType::Decimal64(_, _)
                | DataType::Decimal128(_, _)
                | DataType::Decimal256(_, _) => Ok(data_type.clone()),
                dt if dt.is_signed_integer() => Ok(DataType::Int64),
                dt if dt.is_unsigned_integer() => Ok(DataType::UInt64),
                dt if dt.is_floating() => Ok(DataType::Float64),
                _ => exec_err!("Sum not supported for {data_type}"),
            }
        }

        Ok(vec![coerced_type(args)?])
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match &arg_types[0] {
            DataType::Int64 => Ok(DataType::Int64),
            DataType::UInt64 => Ok(DataType::UInt64),
            DataType::Float64 => Ok(DataType::Float64),
            DataType::Decimal32(precision, scale) => {
                // in the spark, the result type is DECIMAL(min(38,precision+10), s)
                // ref: https://github.com/apache/spark/blob/fcf636d9eb8d645c24be3db2d599aba2d7e2955a/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/aggregate/Sum.scala#L66
                let new_precision = DECIMAL32_MAX_PRECISION.min(*precision + 10);
                Ok(DataType::Decimal32(new_precision, *scale))
            }
            DataType::Decimal64(precision, scale) => {
                // in the spark, the result type is DECIMAL(min(38,precision+10), s)
                // ref: https://github.com/apache/spark/blob/fcf636d9eb8d645c24be3db2d599aba2d7e2955a/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/aggregate/Sum.scala#L66
                let new_precision = DECIMAL64_MAX_PRECISION.min(*precision + 10);
                Ok(DataType::Decimal64(new_precision, *scale))
            }
            DataType::Decimal128(precision, scale) => {
                // in the spark, the result type is DECIMAL(min(38,precision+10), s)
                // ref: https://github.com/apache/spark/blob/fcf636d9eb8d645c24be3db2d599aba2d7e2955a/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/aggregate/Sum.scala#L66
                let new_precision = DECIMAL128_MAX_PRECISION.min(*precision + 10);
                Ok(DataType::Decimal128(new_precision, *scale))
            }
            DataType::Decimal256(precision, scale) => {
                // in the spark, the result type is DECIMAL(min(38,precision+10), s)
                // ref: https://github.com/apache/spark/blob/fcf636d9eb8d645c24be3db2d599aba2d7e2955a/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/aggregate/Sum.scala#L66
                let new_precision = DECIMAL256_MAX_PRECISION.min(*precision + 10);
                Ok(DataType::Decimal256(new_precision, *scale))
            }
            other => {
                exec_err!("[return_type] SUM not supported for {}", other)
            }
        }
    }

    fn accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        if args.is_distinct {
            macro_rules! helper {
                ($t:ty, $dt:expr) => {
                    if <$t as SumType>::FLOAT {
                        Ok(Box::new(DistinctSumAccumulator::<$t>::new(&$dt)))
                    } else {
                        Ok(Box::new(DistinctSumAccumulator::<$t>::new_checked(
                            &$dt,
                            <$t as SumType>::validate,
                        )))
                    }
                };
            }
            downcast_sum!(args, helper)
        } else {
            macro_rules! helper {
                ($t:ty, $dt:expr) => {
                    Ok(Box::new(SumAccumulator::<$t>::new($dt.clone())))
                };
            }
            downcast_sum!(args, helper)
        }
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        if args.is_distinct {
            Ok(vec![Field::new_list(
                format_state_name(args.name, "sum distinct"),
                // See COMMENTS.md to understand why nullable is set to true
                Field::new_list_field(args.return_type().clone(), true),
                false,
            )
            .into()])
        } else {
            Ok(vec![Field::new(
                format_state_name(args.name, "sum"),
                args.return_type().clone(),
                true,
            )
            .into()])
        }
    }

    fn groups_accumulator_supported(&self, args: AccumulatorArgs) -> bool {
        !args.is_distinct
    }

    fn create_groups_accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        macro_rules! helper {
            ($t:ty, $dt:expr) => {
                if <$t as SumType>::FLOAT {
                    Ok(Box::new(PrimitiveGroupsAccumulator::<$t, _>::new(
                        &$dt,
                        |x, y| *x = x.add_wrapping(y),
                    )))
                } else {
                    Ok(Box::new(CheckedSumGroupsAccumulator::<$t>::new($dt)))
                }
            };
        }
        downcast_sum!(args, helper)
    }

    fn create_sliding_accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> Result<Box<dyn Accumulator>> {
        if args.is_distinct {
            // distinct path: use our sliding‐window distinct‐sum
            macro_rules! helper_distinct {
                ($t:ty, $dt:expr) => {
                    Ok(Box::new(SlidingDistinctSumAccumulator::try_new(&$dt)?))
                };
            }
            downcast_sum!(args, helper_distinct)
        } else {
            // non‐distinct path: existing sliding sum
            macro_rules! helper {
                ($t:ty, $dt:expr) => {
                    Ok(Box::new(SlidingSumAccumulator::<$t>::new($dt.clone())))
                };
            }
            downcast_sum!(args, helper)
        }
    }

    fn reverse_expr(&self) -> ReversedUDAF {
        ReversedUDAF::Identical
    }

    fn order_sensitivity(&self) -> AggregateOrderSensitivity {
        AggregateOrderSensitivity::Insensitive
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }

    fn set_monotonicity(&self, data_type: &DataType) -> SetMonotonicity {
        // `SUM` is only monotonically increasing when its input is unsigned.
        // TODO: Expand these utilizing statistics.
        match data_type {
            DataType::UInt8 => SetMonotonicity::Increasing,
            DataType::UInt16 => SetMonotonicity::Increasing,
            DataType::UInt32 => SetMonotonicity::Increasing,
            DataType::UInt64 => SetMonotonicity::Increasing,
            _ => SetMonotonicity::NotMonotonic,
        }
    }
}

// LogEx modification: fixed-width SUM checks every running addition/subtraction,
// including decimal precision. Float64 keeps the original kernels and rounding.
trait SumType: ArrowNumericType + Send {
    const FLOAT: bool = false;
    fn validate(_value: Self::Native, _data_type: &DataType) -> Result<()> {
        Ok(())
    }
}
impl SumType for Int64Type {}
impl SumType for UInt64Type {}
impl SumType for Float64Type {
    const FLOAT: bool = true;
}
macro_rules! decimal_sum_type {
    ($t:ty, $variant:ident) => {
        impl SumType for $t {
            fn validate(value: Self::Native, data_type: &DataType) -> Result<()> {
                let DataType::$variant(precision, scale) = data_type else {
                    return exec_err!("SUM decimal type mismatch: {data_type}");
                };
                Self::validate_decimal_precision(value, *precision, *scale).map_err(
                    |error| {
                        exec_datafusion_err!(
                            "SUM precision overflow for {data_type}: {error}"
                        )
                    },
                )
            }
        }
    };
}
decimal_sum_type!(Decimal32Type, Decimal32);
decimal_sum_type!(Decimal64Type, Decimal64);
decimal_sum_type!(Decimal128Type, Decimal128);
decimal_sum_type!(Decimal256Type, Decimal256);

fn checked_sum_add<T: SumType>(
    a: T::Native,
    b: T::Native,
    dt: &DataType,
) -> Result<T::Native> {
    let value = a
        .add_checked(b)
        .map_err(|error| exec_datafusion_err!("SUM overflow: {error}"))?;
    T::validate(value, dt)?;
    Ok(value)
}
fn checked_sum_sub<T: SumType>(
    a: T::Native,
    b: T::Native,
    dt: &DataType,
) -> Result<T::Native> {
    let value = a.sub_checked(b).map_err(|error| {
        exec_datafusion_err!("SUM overflow during retraction: {error}")
    })?;
    T::validate(value, dt)?;
    Ok(value)
}

/// Checked SUM keeps the vectorized per-group layout and existing NULL/FILTER
/// tracking. A failed batch poisons this accumulator: partial state must not emit.
struct CheckedSumGroupsAccumulator<T: SumType> {
    values: Vec<T::Native>,
    null_state: NullState,
    data_type: DataType,
    failed: Option<String>,
}
impl<T: SumType> std::fmt::Debug for CheckedSumGroupsAccumulator<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CheckedSumGroupsAccumulator({})", self.data_type)
    }
}
impl<T: SumType> CheckedSumGroupsAccumulator<T> {
    fn new(data_type: DataType) -> Self {
        Self {
            values: vec![],
            null_state: NullState::new(),
            data_type,
            failed: None,
        }
    }
    fn check_failed(&self) -> Result<()> {
        if let Some(error) = &self.failed {
            return exec_err!("{error}");
        }
        Ok(())
    }
}
impl<T: SumType> GroupsAccumulator for CheckedSumGroupsAccumulator<T> {
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        indices: &[usize],
        filter: Option<&BooleanArray>,
        groups: usize,
    ) -> Result<()> {
        self.check_failed()?;
        let input = values[0].as_primitive::<T>();
        self.values.resize(groups, T::Native::usize_as(0));
        let mut error = None;
        self.null_state
            .accumulate(indices, input, filter, groups, |group, value| {
                if error.is_none() {
                    match checked_sum_add::<T>(self.values[group], value, &self.data_type)
                    {
                        Ok(sum) => self.values[group] = sum,
                        Err(e) => error = Some(e),
                    }
                }
            });
        if let Some(error) = error {
            self.failed = Some(error.to_string());
            return Err(error);
        }
        Ok(())
    }
    fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        self.check_failed()?;
        let values = emit_to.take_needed(&mut self.values);
        let nulls = self.null_state.build(emit_to);
        Ok(Arc::new(
            PrimitiveArray::<T>::new(values.into(), Some(nulls))
                .with_data_type(self.data_type.clone()),
        ))
    }
    fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        Ok(vec![self.evaluate(emit_to)?])
    }
    fn merge_batch(
        &mut self,
        states: &[ArrayRef],
        indices: &[usize],
        filter: Option<&BooleanArray>,
        groups: usize,
    ) -> Result<()> {
        self.update_batch(states, indices, filter, groups)
    }
    fn convert_to_state(
        &self,
        values: &[ArrayRef],
        filter: Option<&BooleanArray>,
    ) -> Result<Vec<ArrayRef>> {
        self.check_failed()?;
        let input = values[0].as_primitive::<T>();
        let values = set_nulls(input.clone(), filtered_null_mask(filter, input));
        if values.null_count() != values.len() {
            for value in values.iter().flatten() {
                T::validate(value, &self.data_type)?;
            }
        }
        Ok(vec![Arc::new(
            values.with_data_type(self.data_type.clone()),
        )])
    }
    fn supports_convert_to_state(&self) -> bool {
        true
    }
    fn size(&self) -> usize {
        size_of_val(self)
            + self.values.capacity() * size_of::<T::Native>()
            + self.null_state.size()
            + self.failed.as_ref().map_or(0, String::capacity)
    }
}

/// This accumulator computes SUM incrementally
struct SumAccumulator<T: SumType> {
    sum: Option<T::Native>,
    data_type: DataType,
}

impl<T: SumType> std::fmt::Debug for SumAccumulator<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SumAccumulator({})", self.data_type)
    }
}

impl<T: SumType> SumAccumulator<T> {
    fn new(data_type: DataType) -> Self {
        Self {
            sum: None,
            data_type,
        }
    }
}

impl<T: SumType> Accumulator for SumAccumulator<T> {
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![self.evaluate()?])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let values = values[0].as_primitive::<T>();
        if values.null_count() == values.len() {
            return Ok(());
        }
        let mut sum = self.sum;
        if T::FLOAT {
            if let Some(x) = arrow::compute::sum(values) {
                let v = sum.get_or_insert_with(|| T::Native::usize_as(0));
                *v = v.add_wrapping(x);
            }
        } else {
            for value in values.iter().flatten() {
                sum = Some(checked_sum_add::<T>(
                    sum.unwrap_or_else(|| T::Native::usize_as(0)),
                    value,
                    &self.data_type,
                )?);
            }
        }
        self.sum = sum;
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.update_batch(states)
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        ScalarValue::new_primitive::<T>(self.sum, &self.data_type)
    }

    fn size(&self) -> usize {
        size_of_val(self)
    }
}

/// This accumulator incrementally computes sums over a sliding window
///
/// This is separate from [`SumAccumulator`] as requires additional state
struct SlidingSumAccumulator<T: SumType> {
    sum: T::Native,
    count: u64,
    data_type: DataType,
}

impl<T: SumType> std::fmt::Debug for SlidingSumAccumulator<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SlidingSumAccumulator({})", self.data_type)
    }
}

impl<T: SumType> SlidingSumAccumulator<T> {
    fn new(data_type: DataType) -> Self {
        Self {
            sum: T::Native::usize_as(0),
            count: 0,
            data_type,
        }
    }
    fn add_values(&self, values: &PrimitiveArray<T>) -> Result<T::Native> {
        if values.null_count() == values.len() {
            return Ok(self.sum);
        }
        let mut sum = self.sum;
        if T::FLOAT {
            if let Some(x) = arrow::compute::sum(values) {
                sum = sum.add_wrapping(x);
            }
        } else {
            for value in values.iter().flatten() {
                sum = checked_sum_add::<T>(sum, value, &self.data_type)?;
            }
        }
        Ok(sum)
    }
}

impl<T: SumType> Accumulator for SlidingSumAccumulator<T> {
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![self.evaluate()?, self.count.into()])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let values = values[0].as_primitive::<T>();
        if values.null_count() == values.len() {
            return Ok(());
        }
        let count = self
            .count
            .checked_add((values.len() - values.null_count()) as u64)
            .ok_or_else(|| exec_datafusion_err!("SUM window row count overflow"))?;
        let sum = self.add_values(values)?;
        self.sum = sum;
        self.count = count;
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let added_count =
            arrow::compute::sum_checked(states[1].as_primitive::<UInt64Type>())
                .map_err(|error| {
                    exec_datafusion_err!("SUM window row count overflow: {error}")
                })?
                .unwrap_or(0);
        let count = self
            .count
            .checked_add(added_count)
            .ok_or_else(|| exec_datafusion_err!("SUM window row count overflow"))?;
        let sum = self.add_values(states[0].as_primitive::<T>())?;
        self.sum = sum;
        self.count = count;
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let v = (self.count != 0).then_some(self.sum);
        ScalarValue::new_primitive::<T>(v, &self.data_type)
    }

    fn size(&self) -> usize {
        size_of_val(self)
    }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let values = values[0].as_primitive::<T>();
        if values.null_count() == values.len() {
            return Ok(());
        }
        let count = self
            .count
            .checked_sub((values.len() - values.null_count()) as u64)
            .ok_or_else(|| {
                exec_datafusion_err!("SUM window retraction exceeds row count")
            })?;
        let mut sum = self.sum;
        if T::FLOAT {
            if let Some(x) = arrow::compute::sum(values) {
                sum = sum.sub_wrapping(x);
            }
        } else {
            for value in values.iter().flatten() {
                sum = checked_sum_sub::<T>(sum, value, &self.data_type)?;
            }
        }
        self.sum = sum;
        self.count = count;
        Ok(())
    }

    fn supports_retract_batch(&self) -> bool {
        true
    }
}

/// A sliding‐window accumulator for `SUM(DISTINCT)` over Int64 columns.
/// Maintains a running sum so that `evaluate()` is O(1).
#[derive(Debug)]
pub struct SlidingDistinctSumAccumulator {
    /// Map each distinct value → its current count in the window
    counts: HashMap<i64, usize, RandomState>,
    /// Running sum of all distinct keys currently in the window
    sum: i64,
    /// Data type (must be Int64)
    data_type: DataType,
    failed: Option<String>,
    // Removals can reduce usable HashMap capacity without releasing buckets.
    allocated_capacity: usize,
}

impl SlidingDistinctSumAccumulator {
    /// Create a new accumulator; only `DataType::Int64` is supported.
    pub fn try_new(data_type: &DataType) -> Result<Self> {
        // TODO support other numeric types
        if *data_type != DataType::Int64 {
            return exec_err!("SlidingDistinctSumAccumulator only supports Int64");
        }
        Ok(Self {
            counts: HashMap::default(),
            sum: 0,
            data_type: data_type.clone(),
            failed: None,
            allocated_capacity: 0,
        })
    }
    fn check_failed(&self) -> Result<()> {
        if let Some(error) = &self.failed {
            return exec_err!("{error}");
        }
        Ok(())
    }
    fn change_value(&mut self, value: i64, retract: bool) -> Result<()> {
        let old_count = self.counts.get(&value).copied().unwrap_or(0);
        let result = if retract {
            old_count.checked_sub(1).ok_or_else(|| {
                exec_datafusion_err!("SUM DISTINCT retraction exceeds value count")
            })
        } else {
            old_count
                .checked_add(1)
                .ok_or_else(|| exec_datafusion_err!("SUM DISTINCT value count overflow"))
        }
        .and_then(|count| {
            let sum = if !retract && old_count == 0 {
                checked_sum_add::<Int64Type>(self.sum, value, &self.data_type)?
            } else if retract && count == 0 {
                checked_sum_sub::<Int64Type>(self.sum, value, &self.data_type)?
            } else {
                self.sum
            };
            Ok((count, sum))
        });
        match result {
            Ok((count, sum)) => {
                self.sum = sum;
                if count == 0 {
                    self.counts.remove(&value);
                } else {
                    self.counts.insert(value, count);
                    self.allocated_capacity =
                        self.allocated_capacity.max(self.counts.capacity());
                }
                Ok(())
            }
            Err(error) => {
                self.failed = Some(error.to_string());
                Err(error)
            }
        }
    }
}

impl Accumulator for SlidingDistinctSumAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        self.check_failed()?;
        for value in values[0].as_primitive::<Int64Type>().iter().flatten() {
            self.change_value(value, false)?;
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        self.check_failed()?;
        Ok(ScalarValue::Int64(
            (!self.counts.is_empty()).then_some(self.sum),
        ))
    }

    fn size(&self) -> usize {
        // The existing table estimator includes spare buckets/control bytes. Track
        // the peak capacity because this map never explicitly shrinks and removals
        // can change usable capacity. This remains an estimate, not heap metering.
        datafusion_common::utils::memory::estimate_memory_size::<(i64, usize)>(
            self.allocated_capacity,
            size_of_val(self),
        )
        .unwrap_or(usize::MAX)
        .saturating_add(self.failed.as_ref().map_or(0, String::capacity))
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        self.check_failed()?;
        // Window execution does not serialize/merge this accumulator. For callers
        // that do, repeat keys so the existing list state preserves retractions.
        let len = self.counts.values().try_fold(0usize, |len, count| {
            len.checked_add(*count)
                .ok_or_else(|| exec_datafusion_err!("SUM DISTINCT state length overflow"))
        })?;
        i32::try_from(len).map_err(|_| {
            exec_datafusion_err!("SUM DISTINCT state exceeds List offset range")
        })?;
        let mut keys = Vec::new();
        keys.try_reserve_exact(len).map_err(|error| {
            exec_datafusion_err!("SUM DISTINCT state allocation failed: {error}")
        })?;
        for (key, count) in &self.counts {
            keys.extend(std::iter::repeat_n(*key, *count));
        }
        let values = Arc::new(arrow::array::Int64Array::from(keys));
        Ok(vec![
            datafusion_common::utils::SingleRowListArrayBuilder::new(values)
                .with_nullable(true)
                .build_list_scalar(),
        ])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.check_failed()?;
        for values in states[0].as_list::<i32>().iter().flatten() {
            self.update_batch(&[values])?;
        }
        Ok(())
    }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        self.check_failed()?;
        for value in values[0].as_primitive::<Int64Type>().iter().flatten() {
            self.change_value(value, true)?;
        }
        Ok(())
    }

    fn supports_retract_batch(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod logex_checked_tests {
    use super::*;
    use arrow::array::{Int64Array, UInt64Array};
    use arrow::buffer::NullBuffer;

    fn ints(values: Vec<i64>) -> ArrayRef {
        Arc::new(Int64Array::from(values))
    }

    #[test]
    fn scalar_partial_merge_is_checked_and_failed_batches_are_atomic() {
        let mut sum = SumAccumulator::<Int64Type>::new(DataType::Int64);
        sum.update_batch(&[ints(vec![i64::MAX])]).unwrap();
        assert!(sum.merge_batch(&[ints(vec![1])]).is_err());
        assert_eq!(sum.evaluate().unwrap(), ScalarValue::Int64(Some(i64::MAX)));
        sum.update_batch(&[ints(vec![-1])]).unwrap();
        assert_eq!(
            sum.evaluate().unwrap(),
            ScalarValue::Int64(Some(i64::MAX - 1))
        );
        let mut sum = SumAccumulator::<Int64Type>::new(DataType::Int64);
        assert!(sum.update_batch(&[ints(vec![i64::MAX, 1, -1])]).is_err());
        assert_eq!(sum.evaluate().unwrap(), ScalarValue::Int64(None));
        assert!(sum.update_batch(&[ints(vec![i64::MIN, -1])]).is_err());
    }

    #[test]
    fn grouped_merge_poisoning_and_prefix_emission_are_consistent() {
        let mut sum = CheckedSumGroupsAccumulator::<Int64Type>::new(DataType::Int64);
        sum.update_batch(&[ints(vec![2, 3])], &[0, 1], None, 2)
            .unwrap();
        assert_eq!(
            sum.evaluate(EmitTo::First(1))
                .unwrap()
                .as_primitive::<Int64Type>(),
            &Int64Array::from(vec![2])
        );
        sum.merge_batch(&[ints(vec![4])], &[0], None, 1).unwrap();
        assert_eq!(
            sum.state(EmitTo::All).unwrap()[0].as_primitive::<Int64Type>(),
            &Int64Array::from(vec![7])
        );
        sum.update_batch(&[ints(vec![i64::MAX])], &[0], None, 1)
            .unwrap();
        assert!(sum.merge_batch(&[ints(vec![1])], &[0], None, 1).is_err());
        assert!(sum.evaluate(EmitTo::All).is_err());
        assert!(sum.state(EmitTo::First(1)).is_err());
        assert!(sum.update_batch(&[ints(vec![-1])], &[0], None, 1).is_err());
        assert!(sum.convert_to_state(&[ints(vec![0])], None).is_err());
    }

    #[test]
    fn grouped_filter_nulls_and_conversion_ignore_invalid_hidden_values() {
        let mut sum = CheckedSumGroupsAccumulator::<Int64Type>::new(DataType::Int64);
        let filter = BooleanArray::from(vec![Some(true), Some(false), None]);
        sum.update_batch(&[ints(vec![i64::MAX, 1, 1])], &[0, 0, 0], Some(&filter), 2)
            .unwrap();
        assert_eq!(
            sum.evaluate(EmitTo::All)
                .unwrap()
                .as_primitive::<Int64Type>(),
            &Int64Array::from(vec![Some(i64::MAX), None])
        );
        let dt = DataType::Decimal128(38, 0);
        let sum = CheckedSumGroupsAccumulator::<Decimal128Type>::new(dt.clone());
        let invalid = 10_i128.pow(38);
        let values: ArrayRef = Arc::new(
            PrimitiveArray::<Decimal128Type>::new(
                vec![invalid].into(),
                Some(NullBuffer::new_null(1)),
            )
            .with_data_type(dt.clone()),
        );
        assert_eq!(
            sum.convert_to_state(&[values], None).unwrap()[0].null_count(),
            1
        );
        let values: ArrayRef = Arc::new(
            PrimitiveArray::<Decimal128Type>::from(vec![invalid]).with_data_type(dt),
        );
        assert_eq!(
            sum.convert_to_state(
                &[values.clone()],
                Some(&BooleanArray::from(vec![false]))
            )
            .unwrap()[0]
                .null_count(),
            1
        );
        assert!(sum.convert_to_state(&[values], None).is_err());
    }

    #[test]
    fn decimal_declared_precision_is_checked_for_every_storage_width() {
        macro_rules! check {
            ($t:ty, $dt:expr, $max:expr) => {{
                let dt = $dt;
                let input: ArrayRef = Arc::new(
                    PrimitiveArray::<$t>::from_iter_values([
                        $max,
                        <$t as arrow::datatypes::ArrowPrimitiveType>::Native::usize_as(1),
                    ])
                    .with_data_type(dt.clone()),
                );
                let mut scalar = SumAccumulator::<$t>::new(dt.clone());
                assert!(scalar
                    .update_batch(&[input.clone()])
                    .unwrap_err()
                    .to_string()
                    .contains("SUM precision overflow"));
                let mut groups = CheckedSumGroupsAccumulator::<$t>::new(dt.clone());
                assert!(groups
                    .update_batch(&[input.clone()], &[0, 0], None, 1)
                    .is_err());
                let mut distinct = DistinctSumAccumulator::<$t>::new_checked(
                    &dt,
                    <$t as SumType>::validate,
                );
                distinct.update_batch(&[input.clone()]).unwrap();
                assert!(distinct.evaluate().is_err());
                let mut sliding = SlidingSumAccumulator::<$t>::new(dt);
                assert!(sliding.update_batch(&[input]).is_err());
            }};
        }
        check!(Decimal32Type, DataType::Decimal32(9, 0), 999_999_999);
        check!(
            Decimal64Type,
            DataType::Decimal64(18, 0),
            999_999_999_999_999_999
        );
        check!(
            Decimal128Type,
            DataType::Decimal128(38, 0),
            10_i128.pow(38) - 1
        );
        check!(
            Decimal256Type,
            DataType::Decimal256(76, 0),
            "9".repeat(76).parse::<arrow::datatypes::i256>().unwrap()
        );
    }

    #[test]
    fn sliding_merge_counts_and_retraction_errors_are_atomic() {
        let mut sum = SlidingSumAccumulator::<Int64Type>::new(DataType::Int64);
        sum.update_batch(&[ints(vec![i64::MIN, 1])]).unwrap();
        assert!(sum.retract_batch(&[ints(vec![2])]).is_err());
        assert_eq!(
            sum.evaluate().unwrap(),
            ScalarValue::Int64(Some(i64::MIN + 1))
        );
        assert!(sum.retract_batch(&[ints(vec![0, 0, 0])]).is_err());
        let mut sum = SlidingSumAccumulator::<Int64Type>::new(DataType::Int64);
        sum.merge_batch(&[ints(vec![0]), Arc::new(UInt64Array::from(vec![u64::MAX]))])
            .unwrap();
        assert!(sum
            .merge_batch(&[ints(vec![0]), Arc::new(UInt64Array::from(vec![1]))])
            .is_err());
        assert_eq!(sum.count, u64::MAX);
    }

    #[test]
    fn sliding_distinct_null_backing_values_and_memory_are_handled() {
        let mut sum = SlidingDistinctSumAccumulator::try_new(&DataType::Int64).unwrap();
        let nulls: ArrayRef = Arc::new(Int64Array::new(
            vec![19].into(),
            Some(NullBuffer::new_null(1)),
        ));
        sum.update_batch(&[nulls.clone()]).unwrap();
        sum.retract_batch(&[nulls]).unwrap();
        assert_eq!(sum.evaluate().unwrap(), ScalarValue::Int64(None));
        let initial_size = sum.size();
        sum.update_batch(&[ints((0..100).collect())]).unwrap();
        assert!(sum.size() > initial_size);
        let allocated = sum.size();
        sum.retract_batch(&[ints((0..100).collect())]).unwrap();
        assert_eq!(sum.size(), allocated);
        assert_eq!(sum.evaluate().unwrap(), ScalarValue::Int64(None));
        assert!(sum.retract_batch(&[ints(vec![0])]).is_err());
        assert!(sum.evaluate().is_err());
        assert!(sum.state().is_err());
    }
    #[test]
    fn sliding_distinct_state_size_boundaries_fail_before_allocation() {
        let mut sum = SlidingDistinctSumAccumulator::try_new(&DataType::Int64).unwrap();
        sum.counts.insert(7, i32::MAX as usize + 1);
        assert!(sum
            .state()
            .unwrap_err()
            .to_string()
            .contains("List offset range"));
        sum.counts.insert(7, usize::MAX);
        sum.counts.insert(8, 1);
        assert!(sum
            .state()
            .unwrap_err()
            .to_string()
            .contains("state length overflow"));
    }
}
