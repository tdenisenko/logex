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

//! Defines `Avg` & `Mean` aggregate & accumulators

use arrow::array::{
    Array, ArrayRef, ArrowNativeTypeOp, ArrowNumericType, ArrowPrimitiveType, AsArray,
    BooleanArray, PrimitiveArray, PrimitiveBuilder, UInt64Array,
};

use arrow::compute::sum;
use arrow::datatypes::{
    i256, DataType, Decimal128Type, Decimal256Type, Decimal32Type, Decimal64Type,
    DecimalType, DurationMicrosecondType, DurationMillisecondType,
    DurationNanosecondType, DurationSecondType, Field, FieldRef, Float64Type, TimeUnit,
    UInt64Type, DECIMAL128_MAX_PRECISION, DECIMAL128_MAX_SCALE, DECIMAL256_MAX_PRECISION,
    DECIMAL256_MAX_SCALE, DECIMAL32_MAX_PRECISION, DECIMAL32_MAX_SCALE,
    DECIMAL64_MAX_PRECISION, DECIMAL64_MAX_SCALE,
};
use datafusion_common::plan_err;
use datafusion_common::{
    exec_datafusion_err, exec_err, not_impl_err, utils::take_function_args, Result,
    ScalarValue,
};
use datafusion_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion_expr::utils::format_state_name;
use datafusion_expr::Volatility::Immutable;
use datafusion_expr::{
    Accumulator, AggregateUDFImpl, Documentation, EmitTo, Expr, GroupsAccumulator,
    ReversedUDAF, Signature,
};

use datafusion_functions_aggregate_common::aggregate::avg_distinct::{
    DecimalDistinctAvgAccumulator, Float64DistinctAvgAccumulator,
};
use datafusion_functions_aggregate_common::aggregate::groups_accumulator::accumulate::NullState;
use datafusion_functions_aggregate_common::aggregate::groups_accumulator::nulls::{
    filtered_null_mask, set_nulls,
};

use datafusion_functions_aggregate_common::utils::DecimalAverager;
use datafusion_macros::user_doc;
use log::debug;
use std::any::Any;
use std::fmt::Debug;
use std::mem::{size_of, size_of_val};
use std::sync::Arc;

make_udaf_expr_and_func!(
    Avg,
    avg,
    expression,
    "Returns the avg of a group of values.",
    avg_udaf
);

pub fn avg_distinct(expr: Expr) -> Expr {
    Expr::AggregateFunction(datafusion_expr::expr::AggregateFunction::new_udf(
        avg_udaf(),
        vec![expr],
        true,
        None,
        vec![],
        None,
    ))
}

#[user_doc(
    doc_section(label = "General Functions"),
    description = "Returns the average of numeric values in the specified column.",
    syntax_example = "avg(expression)",
    sql_example = r#"```sql
> SELECT avg(column_name) FROM table_name;
+---------------------------+
| avg(column_name)           |
+---------------------------+
| 42.75                      |
+---------------------------+
```"#,
    standard_argument(name = "expression",)
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct Avg {
    signature: Signature,
    aliases: Vec<String>,
}

impl Avg {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Immutable),
            aliases: vec![String::from("mean")],
        }
    }
}

impl Default for Avg {
    fn default() -> Self {
        Self::new()
    }
}

impl AggregateUDFImpl for Avg {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "avg"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        let [args] = take_function_args(self.name(), arg_types)?;

        // Supported types smallint, int, bigint, real, double precision, decimal, or interval
        // Refer to https://www.postgresql.org/docs/8.2/functions-aggregate.html doc
        fn coerced_type(data_type: &DataType) -> Result<DataType> {
            match &data_type {
                DataType::Decimal32(p, s) => Ok(DataType::Decimal32(*p, *s)),
                DataType::Decimal64(p, s) => Ok(DataType::Decimal64(*p, *s)),
                DataType::Decimal128(p, s) => Ok(DataType::Decimal128(*p, *s)),
                DataType::Decimal256(p, s) => Ok(DataType::Decimal256(*p, *s)),
                d if d.is_numeric() => Ok(DataType::Float64),
                DataType::Duration(time_unit) => Ok(DataType::Duration(*time_unit)),
                DataType::Dictionary(_, v) => coerced_type(v.as_ref()),
                _ => {
                    plan_err!("Avg does not support inputs of type {data_type}.")
                }
            }
        }
        Ok(vec![coerced_type(args)?])
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match &arg_types[0] {
            DataType::Decimal32(precision, scale) => {
                // In the spark, the result type is DECIMAL(min(38,precision+4), min(38,scale+4)).
                // Ref: https://github.com/apache/spark/blob/fcf636d9eb8d645c24be3db2d599aba2d7e2955a/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/aggregate/Average.scala#L66
                let new_precision = DECIMAL32_MAX_PRECISION.min(*precision + 4);
                let new_scale = DECIMAL32_MAX_SCALE.min(*scale + 4);
                Ok(DataType::Decimal32(new_precision, new_scale))
            }
            DataType::Decimal64(precision, scale) => {
                // In the spark, the result type is DECIMAL(min(38,precision+4), min(38,scale+4)).
                // Ref: https://github.com/apache/spark/blob/fcf636d9eb8d645c24be3db2d599aba2d7e2955a/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/aggregate/Average.scala#L66
                let new_precision = DECIMAL64_MAX_PRECISION.min(*precision + 4);
                let new_scale = DECIMAL64_MAX_SCALE.min(*scale + 4);
                Ok(DataType::Decimal64(new_precision, new_scale))
            }
            DataType::Decimal128(precision, scale) => {
                // In the spark, the result type is DECIMAL(min(38,precision+4), min(38,scale+4)).
                // Ref: https://github.com/apache/spark/blob/fcf636d9eb8d645c24be3db2d599aba2d7e2955a/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/aggregate/Average.scala#L66
                let new_precision = DECIMAL128_MAX_PRECISION.min(*precision + 4);
                let new_scale = DECIMAL128_MAX_SCALE.min(*scale + 4);
                Ok(DataType::Decimal128(new_precision, new_scale))
            }
            DataType::Decimal256(precision, scale) => {
                // In the spark, the result type is DECIMAL(min(38,precision+4), min(38,scale+4)).
                // Ref: https://github.com/apache/spark/blob/fcf636d9eb8d645c24be3db2d599aba2d7e2955a/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/aggregate/Average.scala#L66
                let new_precision = DECIMAL256_MAX_PRECISION.min(*precision + 4);
                let new_scale = DECIMAL256_MAX_SCALE.min(*scale + 4);
                Ok(DataType::Decimal256(new_precision, new_scale))
            }
            DataType::Duration(time_unit) => Ok(DataType::Duration(*time_unit)),
            _ => Ok(DataType::Float64),
        }
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        let data_type = acc_args.expr_fields[0].data_type();
        use DataType::*;

        // instantiate specialized accumulator based for the type
        if acc_args.is_distinct {
            match (data_type, acc_args.return_type()) {
                // Numeric types are converted to Float64 via `coerce_avg_type` during logical plan creation
                (Float64, _) => Ok(Box::new(Float64DistinctAvgAccumulator::default())),

                (
                    Decimal32(_, scale),
                    Decimal32(target_precision, target_scale),
                ) => Ok(Box::new(DecimalDistinctAvgAccumulator::<Decimal32Type>::with_decimal_params(
                    *scale,
                    *target_precision,
                    *target_scale,
                ))),
                                (
                    Decimal64(_, scale),
                    Decimal64(target_precision, target_scale),
                ) => Ok(Box::new(DecimalDistinctAvgAccumulator::<Decimal64Type>::with_decimal_params(
                    *scale,
                    *target_precision,
                    *target_scale,
                ))),
                (
                    Decimal128(_, scale),
                    Decimal128(target_precision, target_scale),
                ) => Ok(Box::new(DecimalDistinctAvgAccumulator::<Decimal128Type>::with_decimal_params(
                    *scale,
                    *target_precision,
                    *target_scale,
                ))),

                (
                    Decimal256(_, scale),
                    Decimal256(target_precision, target_scale),
                ) => Ok(Box::new(DecimalDistinctAvgAccumulator::<Decimal256Type>::with_decimal_params(
                    *scale,
                    *target_precision,
                    *target_scale,
                ))),

                (dt, return_type) => exec_err!(
                    "AVG(DISTINCT) for ({} --> {}) not supported",
                    dt,
                    return_type
                ),
            }
        } else {
            match (&data_type, acc_args.return_type()) {
                (Float64, Float64) => Ok(Box::<AvgAccumulator>::default()),
                (
                    Decimal32(sum_precision, sum_scale),
                    Decimal32(target_precision, target_scale),
                ) => Ok(Box::new(DecimalAvgAccumulator::<Decimal32Type> {
                    sum: None,
                    count: 0,
                    sum_scale: *sum_scale,
                    sum_precision: *sum_precision,
                    target_precision: *target_precision,
                    target_scale: *target_scale,
                })),
                (
                    Decimal64(sum_precision, sum_scale),
                    Decimal64(target_precision, target_scale),
                ) => Ok(Box::new(DecimalAvgAccumulator::<Decimal64Type> {
                    sum: None,
                    count: 0,
                    sum_scale: *sum_scale,
                    sum_precision: *sum_precision,
                    target_precision: *target_precision,
                    target_scale: *target_scale,
                })),
                (
                    Decimal128(sum_precision, sum_scale),
                    Decimal128(target_precision, target_scale),
                ) => Ok(Box::new(DecimalAvgAccumulator::<Decimal128Type> {
                    sum: None,
                    count: 0,
                    sum_scale: *sum_scale,
                    sum_precision: *sum_precision,
                    target_precision: *target_precision,
                    target_scale: *target_scale,
                })),

                (
                    Decimal256(sum_precision, sum_scale),
                    Decimal256(target_precision, target_scale),
                ) => Ok(Box::new(DecimalAvgAccumulator::<Decimal256Type> {
                    sum: None,
                    count: 0,
                    sum_scale: *sum_scale,
                    sum_precision: *sum_precision,
                    target_precision: *target_precision,
                    target_scale: *target_scale,
                })),

                (Duration(time_unit), Duration(result_unit)) => {
                    Ok(Box::new(DurationAvgAccumulator {
                        sum: None,
                        count: 0,
                        time_unit: *time_unit,
                        result_unit: *result_unit,
                    }))
                }

                (dt, return_type) => {
                    exec_err!("AvgAccumulator for ({} --> {})", dt, return_type)
                }
            }
        }
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        if args.is_distinct {
            // Decimal accumulator actually uses a different precision during accumulation,
            // see DecimalDistinctAvgAccumulator::with_decimal_params
            let dt = match args.input_fields[0].data_type() {
                DataType::Decimal32(_, scale) => {
                    DataType::Decimal32(DECIMAL32_MAX_PRECISION, *scale)
                }
                DataType::Decimal64(_, scale) => {
                    DataType::Decimal64(DECIMAL64_MAX_PRECISION, *scale)
                }
                DataType::Decimal128(_, scale) => {
                    DataType::Decimal128(DECIMAL128_MAX_PRECISION, *scale)
                }
                DataType::Decimal256(_, scale) => {
                    DataType::Decimal256(DECIMAL256_MAX_PRECISION, *scale)
                }
                _ => args.return_type().clone(),
            };
            // Similar to datafusion_functions_aggregate::sum::Sum::state_fields
            // since the accumulator uses DistinctSumAccumulator internally.
            Ok(vec![Field::new_list(
                format_state_name(args.name, "avg distinct"),
                Field::new_list_field(dt, true),
                false,
            )
            .into()])
        } else {
            Ok(vec![
                Field::new(
                    format_state_name(args.name, "count"),
                    DataType::UInt64,
                    true,
                ),
                Field::new(
                    format_state_name(args.name, "sum"),
                    args.input_fields[0].data_type().clone(),
                    true,
                ),
            ]
            .into_iter()
            .map(Arc::new)
            .collect())
        }
    }

    fn groups_accumulator_supported(&self, args: AccumulatorArgs) -> bool {
        matches!(
            args.return_field.data_type(),
            DataType::Float64
                | DataType::Decimal32(_, _)
                | DataType::Decimal64(_, _)
                | DataType::Decimal128(_, _)
                | DataType::Decimal256(_, _)
                | DataType::Duration(_)
        ) && !args.is_distinct
    }

    fn create_groups_accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        use DataType::*;

        let data_type = args.expr_fields[0].data_type();

        // instantiate specialized accumulator based for the type
        match (data_type, args.return_field.data_type()) {
            (Float64, Float64) => {
                Ok(Box::new(AvgGroupsAccumulator::<Float64Type, _>::new(
                    data_type,
                    args.return_field.data_type(),
                    |sum: f64, count: u64| Ok(sum / count as f64),
                )))
            }
            (
                Decimal32(_sum_precision, sum_scale),
                Decimal32(target_precision, target_scale),
            ) => {
                let decimal_averager = DecimalAverager::<Decimal32Type>::try_new(
                    *sum_scale,
                    *target_precision,
                    *target_scale,
                )?;

                let avg_fn = move |sum: i32, count: u64| {
                    decimal_averager.avg_with_count(sum, count)
                };

                Ok(Box::new(
                    AvgGroupsAccumulator::<Decimal32Type, _>::new_checked(
                        data_type,
                        args.return_field.data_type(),
                        avg_fn,
                    ),
                ))
            }
            (
                Decimal64(_sum_precision, sum_scale),
                Decimal64(target_precision, target_scale),
            ) => {
                let decimal_averager = DecimalAverager::<Decimal64Type>::try_new(
                    *sum_scale,
                    *target_precision,
                    *target_scale,
                )?;

                let avg_fn = move |sum: i64, count: u64| {
                    decimal_averager.avg_with_count(sum, count)
                };

                Ok(Box::new(
                    AvgGroupsAccumulator::<Decimal64Type, _>::new_checked(
                        data_type,
                        args.return_field.data_type(),
                        avg_fn,
                    ),
                ))
            }
            (
                Decimal128(_sum_precision, sum_scale),
                Decimal128(target_precision, target_scale),
            ) => {
                let decimal_averager = DecimalAverager::<Decimal128Type>::try_new(
                    *sum_scale,
                    *target_precision,
                    *target_scale,
                )?;

                let avg_fn = move |sum: i128, count: u64| {
                    decimal_averager.avg_with_count(sum, count)
                };

                Ok(Box::new(
                    AvgGroupsAccumulator::<Decimal128Type, _>::new_checked(
                        data_type,
                        args.return_field.data_type(),
                        avg_fn,
                    ),
                ))
            }

            (
                Decimal256(_sum_precision, sum_scale),
                Decimal256(target_precision, target_scale),
            ) => {
                let decimal_averager = DecimalAverager::<Decimal256Type>::try_new(
                    *sum_scale,
                    *target_precision,
                    *target_scale,
                )?;

                let avg_fn = move |sum: i256, count: u64| {
                    decimal_averager.avg_with_count(sum, count)
                };

                Ok(Box::new(
                    AvgGroupsAccumulator::<Decimal256Type, _>::new_checked(
                        data_type,
                        args.return_field.data_type(),
                        avg_fn,
                    ),
                ))
            }

            (Duration(time_unit), Duration(_result_unit)) => {
                let avg_fn = move |sum: i64, count: u64| duration_average(sum, count);

                match time_unit {
                    TimeUnit::Second => Ok(Box::new(AvgGroupsAccumulator::<
                        DurationSecondType,
                        _,
                    >::new_checked(
                        data_type,
                        args.return_type(),
                        avg_fn,
                    ))),
                    TimeUnit::Millisecond => Ok(Box::new(AvgGroupsAccumulator::<
                        DurationMillisecondType,
                        _,
                    >::new_checked(
                        data_type,
                        args.return_type(),
                        avg_fn,
                    ))),
                    TimeUnit::Microsecond => Ok(Box::new(AvgGroupsAccumulator::<
                        DurationMicrosecondType,
                        _,
                    >::new_checked(
                        data_type,
                        args.return_type(),
                        avg_fn,
                    ))),
                    TimeUnit::Nanosecond => Ok(Box::new(AvgGroupsAccumulator::<
                        DurationNanosecondType,
                        _,
                    >::new_checked(
                        data_type,
                        args.return_type(),
                        avg_fn,
                    ))),
                }
            }

            _ => not_impl_err!(
                "AvgGroupsAccumulator for ({} --> {})",
                &data_type,
                args.return_field.data_type()
            ),
        }
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn reverse_expr(&self) -> ReversedUDAF {
        ReversedUDAF::Identical
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

/// An accumulator to compute the average
#[derive(Debug, Default)]
pub struct AvgAccumulator {
    sum: Option<f64>,
    count: u64,
}

impl Accumulator for AvgAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let values = values[0].as_primitive::<Float64Type>();
        self.count += (values.len() - values.null_count()) as u64;
        if let Some(x) = sum(values) {
            let v = self.sum.get_or_insert(0.);
            *v += x;
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        Ok(ScalarValue::Float64(
            self.sum
                .filter(|_| self.count != 0)
                .map(|f| f / self.count as f64),
        ))
    }

    fn size(&self) -> usize {
        size_of_val(self)
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![
            ScalarValue::from(self.count),
            ScalarValue::Float64(self.sum),
        ])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        // counts are summed
        self.count += sum(states[0].as_primitive::<UInt64Type>()).unwrap_or_default();

        // sums are summed
        if let Some(x) = sum(states[1].as_primitive::<Float64Type>()) {
            let v = self.sum.get_or_insert(0.);
            *v += x;
        }
        Ok(())
    }
    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let values = values[0].as_primitive::<Float64Type>();
        self.count -= (values.len() - values.null_count()) as u64;
        if self.count == 0 {
            self.sum = None;
            return Ok(());
        }
        if let Some(x) = sum(values) {
            self.sum = Some(self.sum.unwrap() - x);
        }
        Ok(())
    }

    fn supports_retract_batch(&self) -> bool {
        true
    }
}

/// An accumulator to compute the average for decimals
#[derive(Debug)]
struct DecimalAvgAccumulator<T: DecimalType + ArrowNumericType + Debug> {
    sum: Option<T::Native>,
    count: u64,
    sum_scale: i8,
    sum_precision: u8,
    target_precision: u8,
    target_scale: i8,
}

impl<T: DecimalType + ArrowNumericType + Debug> Accumulator for DecimalAvgAccumulator<T> {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let values = values[0].as_primitive::<T>();
        let count = self
            .count
            .checked_add((values.len() - values.null_count()) as u64)
            .ok_or_else(|| exec_datafusion_err!("AVG count overflow"))?;
        let mut sum = self.sum;
        if values.null_count() != values.len() {
            for value in values.iter().flatten() {
                sum = Some(
                    sum.unwrap_or_default()
                        .add_checked(value)
                        .map_err(|_| exec_datafusion_err!("AVG sum overflow"))?,
                );
            }
        }
        self.count = count;
        self.sum = sum;
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let v = self
            .sum
            .filter(|_| self.count != 0)
            .map(|v| {
                DecimalAverager::<T>::try_new(
                    self.sum_scale,
                    self.target_precision,
                    self.target_scale,
                )?
                .avg_with_count(v, self.count)
            })
            .transpose()?;

        ScalarValue::new_primitive::<T>(
            v,
            &T::TYPE_CONSTRUCTOR(self.target_precision, self.target_scale),
        )
    }

    fn size(&self) -> usize {
        size_of_val(self)
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![
            ScalarValue::from(self.count),
            ScalarValue::new_primitive::<T>(
                self.sum,
                &T::TYPE_CONSTRUCTOR(self.sum_precision, self.sum_scale),
            )?,
        ])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let mut count = self.count;
        for value in states[0].as_primitive::<UInt64Type>().iter().flatten() {
            count = count
                .checked_add(value)
                .ok_or_else(|| exec_datafusion_err!("AVG count overflow"))?;
        }
        let mut sum = self.sum;
        let values = states[1].as_primitive::<T>();
        if values.null_count() != values.len() {
            for value in values.iter().flatten() {
                sum = Some(
                    sum.unwrap_or_default()
                        .add_checked(value)
                        .map_err(|_| exec_datafusion_err!("AVG sum overflow"))?,
                );
            }
        }
        self.count = count;
        self.sum = sum;
        Ok(())
    }
    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let values = values[0].as_primitive::<T>();
        let count = self
            .count
            .checked_sub((values.len() - values.null_count()) as u64)
            .ok_or_else(|| exec_datafusion_err!("AVG retraction exceeds count"))?;
        let mut sum = self.sum;
        if values.null_count() != values.len() {
            for value in values.iter().flatten() {
                sum = Some(
                    sum.unwrap_or_default()
                        .sub_checked(value)
                        .map_err(|_| exec_datafusion_err!("AVG sum overflow"))?,
                );
            }
        }
        self.count = count;
        self.sum = if count == 0 { None } else { sum };
        Ok(())
    }

    fn supports_retract_batch(&self) -> bool {
        true
    }
}

/// An accumulator to compute the average for duration values
#[derive(Debug)]
struct DurationAvgAccumulator {
    sum: Option<i64>,
    count: u64,
    time_unit: TimeUnit,
    result_unit: TimeUnit,
}

fn duration_average(sum: i64, count: u64) -> Result<i64> {
    let count =
        i64::try_from(count).map_err(|_| exec_datafusion_err!("AVG count overflow"))?;
    sum.checked_div(count)
        .ok_or_else(|| exec_datafusion_err!("AVG invalid count or division overflow"))
}

impl DurationAvgAccumulator {
    /// Apply valid native values to a staged sum, preserving the captured unit.
    fn checked_sum(&self, array: &ArrayRef, initial: i64, retract: bool) -> Result<i64> {
        if array.null_count() == array.len() {
            return Ok(initial);
        }
        let values = match self.time_unit {
            TimeUnit::Second => array.as_primitive::<DurationSecondType>().values(),
            TimeUnit::Millisecond => {
                array.as_primitive::<DurationMillisecondType>().values()
            }
            TimeUnit::Microsecond => {
                array.as_primitive::<DurationMicrosecondType>().values()
            }
            TimeUnit::Nanosecond => {
                array.as_primitive::<DurationNanosecondType>().values()
            }
        };
        let nulls = array.nulls();
        let values = values
            .iter()
            .enumerate()
            .filter(|(index, _)| nulls.is_none_or(|nulls| nulls.is_valid(*index)));
        let mut sum = initial;
        if retract {
            for (_, value) in values {
                sum = sum
                    .checked_sub(*value)
                    .ok_or_else(|| exec_datafusion_err!("AVG sum overflow"))?;
            }
        } else {
            for (_, value) in values {
                sum = sum
                    .checked_add(*value)
                    .ok_or_else(|| exec_datafusion_err!("AVG sum overflow"))?;
            }
        }
        Ok(sum)
    }
}

impl Accumulator for DurationAvgAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let array = &values[0];
        let added = (array.len() - array.null_count()) as u64;
        let count = self
            .count
            .checked_add(added)
            .ok_or_else(|| exec_datafusion_err!("AVG count overflow"))?;
        let sum = if added == 0 {
            self.sum
        } else {
            Some(self.checked_sum(array, self.sum.unwrap_or_default(), false)?)
        };
        self.count = count;
        self.sum = sum;
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let avg = self
            .sum
            .filter(|_| self.count != 0)
            .map(|sum| duration_average(sum, self.count))
            .transpose()?;
        match self.result_unit {
            TimeUnit::Second => Ok(ScalarValue::DurationSecond(avg)),
            TimeUnit::Millisecond => Ok(ScalarValue::DurationMillisecond(avg)),
            TimeUnit::Microsecond => Ok(ScalarValue::DurationMicrosecond(avg)),
            TimeUnit::Nanosecond => Ok(ScalarValue::DurationNanosecond(avg)),
        }
    }

    fn size(&self) -> usize {
        size_of_val(self)
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let duration_value = match self.time_unit {
            TimeUnit::Second => ScalarValue::DurationSecond(self.sum),
            TimeUnit::Millisecond => ScalarValue::DurationMillisecond(self.sum),
            TimeUnit::Microsecond => ScalarValue::DurationMicrosecond(self.sum),
            TimeUnit::Nanosecond => ScalarValue::DurationNanosecond(self.sum),
        };
        Ok(vec![ScalarValue::from(self.count), duration_value])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let mut count = self.count;
        for partial in states[0].as_primitive::<UInt64Type>().iter().flatten() {
            count = count
                .checked_add(partial)
                .ok_or_else(|| exec_datafusion_err!("AVG count overflow"))?;
        }
        let values = &states[1];
        let sum = if values.null_count() == values.len() {
            self.sum
        } else {
            Some(self.checked_sum(values, self.sum.unwrap_or_default(), false)?)
        };
        self.count = count;
        self.sum = sum;
        Ok(())
    }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let array = &values[0];
        let count = self
            .count
            .checked_sub((array.len() - array.null_count()) as u64)
            .ok_or_else(|| exec_datafusion_err!("AVG retraction exceeds count"))?;
        let sum = self.checked_sum(array, self.sum.unwrap_or_default(), true)?;
        self.count = count;
        self.sum = if count == 0 { None } else { Some(sum) };
        Ok(())
    }

    fn supports_retract_batch(&self) -> bool {
        true
    }
}

/// An accumulator to compute the average of `[PrimitiveArray<T>]`.
/// Stores values as native types, and does overflow checking
///
/// F: Function that calculates the average value from a sum of
/// T::Native and a total count
#[derive(Debug)]
struct AvgGroupsAccumulator<T, F>
where
    T: ArrowNumericType + Send,
    F: Fn(T::Native, u64) -> Result<T::Native> + Send,
{
    /// The type of the internal sum
    sum_data_type: DataType,

    /// The type of the returned sum
    return_data_type: DataType,

    /// Count per group (use u64 to make UInt64Array)
    counts: Vec<u64>,

    /// Sums per group, stored as the native type
    sums: Vec<T::Native>,

    /// Track nulls in the input / filters
    null_state: NullState,

    /// Function that computes the final average (value / count)
    avg_fn: F,
    checked: bool,
    failed: Option<String>,
}

impl<T, F> AvgGroupsAccumulator<T, F>
where
    T: ArrowNumericType + Send,
    F: Fn(T::Native, u64) -> Result<T::Native> + Send,
{
    pub fn new(sum_data_type: &DataType, return_data_type: &DataType, avg_fn: F) -> Self {
        debug!(
            "AvgGroupsAccumulator ({}, sum type: {sum_data_type}) --> {return_data_type}",
            std::any::type_name::<T>()
        );

        Self {
            return_data_type: return_data_type.clone(),
            sum_data_type: sum_data_type.clone(),
            counts: vec![],
            sums: vec![],
            null_state: NullState::new(),
            avg_fn,
            checked: false,
            failed: None,
        }
    }
    fn new_checked(
        sum_data_type: &DataType,
        return_data_type: &DataType,
        avg_fn: F,
    ) -> Self {
        Self {
            checked: true,
            ..Self::new(sum_data_type, return_data_type, avg_fn)
        }
    }

    fn ensure_valid(&self) -> Result<()> {
        match &self.failed {
            Some(error) => exec_err!("{error}"),
            None => Ok(()),
        }
    }
}

impl<T, F> GroupsAccumulator for AvgGroupsAccumulator<T, F>
where
    T: ArrowNumericType + Send,
    F: Fn(T::Native, u64) -> Result<T::Native> + Send,
{
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        self.ensure_valid()?;
        assert_eq!(values.len(), 1, "single argument to update_batch");
        let values = values[0].as_primitive::<T>();

        // increment counts, update sums
        self.counts.resize(total_num_groups, 0);
        self.sums.resize(total_num_groups, T::default_value());
        if self.checked {
            let failed = &mut self.failed;
            self.null_state.accumulate(
                group_indices,
                values,
                opt_filter,
                total_num_groups,
                |index, value| {
                    if failed.is_some() {
                        return;
                    }
                    let next = self.sums[index]
                        .add_checked(value)
                        .map_err(|_| exec_datafusion_err!("AVG sum overflow"))
                        .and_then(|sum| {
                            self.counts[index]
                                .checked_add(1)
                                .map(|count| (sum, count))
                                .ok_or_else(|| exec_datafusion_err!("AVG count overflow"))
                        });
                    match next {
                        Ok((sum, count)) => {
                            self.sums[index] = sum;
                            self.counts[index] = count;
                        }
                        Err(error) => *failed = Some(error.to_string()),
                    }
                },
            );
            return self.ensure_valid();
        }
        self.null_state.accumulate(
            group_indices,
            values,
            opt_filter,
            total_num_groups,
            |group_index, new_value| {
                let sum = &mut self.sums[group_index];
                *sum = sum.add_wrapping(new_value);

                self.counts[group_index] += 1;
            },
        );

        Ok(())
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        self.ensure_valid()?;
        let counts = emit_to.take_needed(&mut self.counts);
        let sums = emit_to.take_needed(&mut self.sums);
        let nulls = self.null_state.build(emit_to);

        assert_eq!(nulls.len(), sums.len());
        assert_eq!(counts.len(), sums.len());

        // don't evaluate averages with null inputs to avoid errors on null values

        let result = (|| -> Result<ArrayRef> {
            let array: PrimitiveArray<T> = if nulls.null_count() > 0 {
                let mut builder = PrimitiveBuilder::<T>::with_capacity(nulls.len())
                    .with_data_type(self.return_data_type.clone());
                let iter = sums.into_iter().zip(counts).zip(nulls.iter());

                for ((sum, count), is_valid) in iter {
                    if is_valid {
                        builder.append_value((self.avg_fn)(sum, count)?)
                    } else {
                        builder.append_null();
                    }
                }
                builder.finish()
            } else {
                let averages: Vec<T::Native> = sums
                    .into_iter()
                    .zip(counts.into_iter())
                    .map(|(sum, count)| (self.avg_fn)(sum, count))
                    .collect::<Result<Vec<_>>>()?;
                PrimitiveArray::new(averages.into(), Some(nulls)) // no copy
                    .with_data_type(self.return_data_type.clone())
            };

            Ok(Arc::new(array))
        })();
        if self.checked {
            if let Err(error) = &result {
                self.failed = Some(error.to_string());
            }
        }
        result
    }

    // return arrays for sums and counts
    fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        self.ensure_valid()?;
        let nulls = self.null_state.build(emit_to);
        let nulls = Some(nulls);

        let counts = emit_to.take_needed(&mut self.counts);
        let counts = UInt64Array::new(counts.into(), nulls.clone()); // zero copy

        let sums = emit_to.take_needed(&mut self.sums);
        let sums = PrimitiveArray::<T>::new(sums.into(), nulls) // zero copy
            .with_data_type(self.sum_data_type.clone());

        Ok(vec![
            Arc::new(counts) as ArrayRef,
            Arc::new(sums) as ArrayRef,
        ])
    }

    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        self.ensure_valid()?;
        assert_eq!(values.len(), 2, "two arguments to merge_batch");
        // first batch is counts, second is partial sums
        let partial_counts = values[0].as_primitive::<UInt64Type>();
        let partial_sums = values[1].as_primitive::<T>();
        if self.checked {
            self.counts.resize(total_num_groups, 0);
            self.sums.resize(total_num_groups, T::default_value());
            let failed = &mut self.failed;
            self.null_state.accumulate(
                group_indices,
                partial_counts,
                opt_filter,
                total_num_groups,
                |index, value| {
                    if failed.is_some() {
                        return;
                    }
                    match self.counts[index].checked_add(value) {
                        Some(count) => self.counts[index] = count,
                        None => *failed = Some("AVG count overflow".into()),
                    }
                },
            );
            self.null_state.accumulate(
                group_indices,
                partial_sums,
                opt_filter,
                total_num_groups,
                |index, value| {
                    if failed.is_some() {
                        return;
                    }
                    match self.sums[index].add_checked(value) {
                        Ok(sum) => self.sums[index] = sum,
                        Err(_) => *failed = Some("AVG sum overflow".into()),
                    }
                },
            );
            return self.ensure_valid();
        }
        // update counts with partial counts
        self.counts.resize(total_num_groups, 0);
        self.null_state.accumulate(
            group_indices,
            partial_counts,
            opt_filter,
            total_num_groups,
            |group_index, partial_count| {
                self.counts[group_index] += partial_count;
            },
        );

        // update sums
        self.sums.resize(total_num_groups, T::default_value());
        self.null_state.accumulate(
            group_indices,
            partial_sums,
            opt_filter,
            total_num_groups,
            |group_index, new_value: <T as ArrowPrimitiveType>::Native| {
                let sum = &mut self.sums[group_index];
                *sum = sum.add_wrapping(new_value);
            },
        );

        Ok(())
    }

    fn convert_to_state(
        &self,
        values: &[ArrayRef],
        opt_filter: Option<&BooleanArray>,
    ) -> Result<Vec<ArrayRef>> {
        self.ensure_valid()?;
        let sums = values[0]
            .as_primitive::<T>()
            .clone()
            .with_data_type(self.sum_data_type.clone());
        let counts = UInt64Array::from_value(1, sums.len());

        let nulls = filtered_null_mask(opt_filter, &sums);

        // set nulls on the arrays
        let counts = set_nulls(counts, nulls.clone());
        let sums = set_nulls(sums, nulls);

        Ok(vec![Arc::new(counts) as ArrayRef, Arc::new(sums)])
    }

    fn supports_convert_to_state(&self) -> bool {
        true
    }

    fn size(&self) -> usize {
        size_of_val(self)
            + self.counts.capacity() * size_of::<u64>()
            + self.sums.capacity() * size_of::<T::Native>()
            + self.null_state.size()
            + self.failed.as_ref().map_or(0, String::capacity)
    }
}

#[cfg(test)]
mod logex_decimal_tests {
    use super::*;
    use arrow::array::Decimal128Array;

    fn scalar() -> DecimalAvgAccumulator<Decimal128Type> {
        DecimalAvgAccumulator {
            sum: None,
            count: 0,
            sum_scale: 0,
            sum_precision: 2,
            target_precision: 6,
            target_scale: 4,
        }
    }
    fn values(v: Vec<Option<i128>>) -> ArrayRef {
        Arc::new(
            Decimal128Array::from(v)
                .with_precision_and_scale(2, 0)
                .unwrap(),
        )
    }

    #[test]
    fn logex_duration_units_state_and_checked_failures() -> Result<()> {
        for unit in [
            TimeUnit::Second,
            TimeUnit::Millisecond,
            TimeUnit::Microsecond,
            TimeUnit::Nanosecond,
        ] {
            let make = || DurationAvgAccumulator {
                sum: None,
                count: 0,
                time_unit: unit,
                result_unit: unit,
            };
            let value = match unit {
                TimeUnit::Second => ScalarValue::DurationSecond(Some(5)),
                TimeUnit::Millisecond => ScalarValue::DurationMillisecond(Some(5)),
                TimeUnit::Microsecond => ScalarValue::DurationMicrosecond(Some(5)),
                TimeUnit::Nanosecond => ScalarValue::DurationNanosecond(Some(5)),
            };
            let input = value.to_array_of_size(2)?;
            let mut acc = make();
            acc.update_batch(&[input.clone()])?;
            assert_eq!(acc.evaluate()?, value);
            let state = acc
                .state()?
                .iter()
                .map(|value| value.to_array_of_size(1))
                .collect::<Result<Vec<_>>>()?;
            let mut merged = make();
            merged.merge_batch(&state)?;
            assert_eq!(merged.evaluate()?, value);
            merged.retract_batch(&[input.clone()])?;
            assert!(merged.evaluate()?.is_null());
            assert!(merged.retract_batch(&[input.clone()]).is_err());
            acc.sum = Some(i64::MAX);
            acc.count = 2;
            assert!(acc.update_batch(&[input.clone()]).is_err());
            assert_eq!(acc.sum, Some(i64::MAX));
            assert_eq!(acc.count, 2);
            assert!(acc.merge_batch(&state).is_err());
            assert_eq!(acc.sum, Some(i64::MAX));
            assert_eq!(acc.count, 2);
            acc.sum = Some(i64::MIN);
            assert!(acc.retract_batch(&[input.clone()]).is_err());
            assert_eq!(acc.sum, Some(i64::MIN));
            assert_eq!(acc.count, 2);
            acc.sum = Some(5);
            acc.count = u64::MAX;
            assert!(acc.update_batch(&[input]).is_err());
            assert!(acc.merge_batch(&state).is_err());
            assert!(acc.evaluate().is_err());
            assert_eq!(acc.sum, Some(5));
            assert_eq!(acc.count, u64::MAX);
        }
        assert!(duration_average(1, 0).is_err());
        assert_eq!(duration_average(-5, 2)?, -2);
        Ok(())
    }

    #[test]
    fn logex_float_empty_state_resets_nonfinite_sum() -> Result<()> {
        let mut acc = AvgAccumulator::default();
        let nonfinite: ArrayRef =
            Arc::new(arrow::array::Float64Array::from(vec![f64::INFINITY]));
        acc.update_batch(&[nonfinite.clone()])?;
        acc.retract_batch(&[nonfinite])?;
        assert_eq!(acc.evaluate()?, ScalarValue::Float64(None));
        let finite: ArrayRef = Arc::new(arrow::array::Float64Array::from(vec![5.0]));
        acc.update_batch(&[finite])?;
        assert_eq!(acc.evaluate()?, ScalarValue::Float64(Some(5.0)));
        Ok(())
    }

    #[test]
    fn logex_group_memory_includes_native_sum_capacity() -> Result<()> {
        let mut acc = AvgGroupsAccumulator::<Decimal128Type, _>::new(
            &DataType::Decimal128(2, 0),
            &DataType::Decimal128(6, 4),
            |sum, count| Ok(sum / i128::from(count)),
        );
        acc.update_batch(&[values(vec![Some(90), Some(90)])], &[0, 1], None, 2)?;
        let minimum = acc.counts.capacity() * size_of::<u64>()
            + acc.sums.capacity() * size_of::<i128>()
            + acc.null_state.size();
        assert!(
            acc.size() >= minimum,
            "reported {} must cover at least {minimum}",
            acc.size()
        );
        Ok(())
    }

    #[test]
    fn logex_decimal_scalar_storage_widths_and_count_bounds() -> Result<()> {
        macro_rules! check {
            ($ty:ty, $max:expr, $one:expr, $precision:expr) => {{
                let mut acc = DecimalAvgAccumulator::<$ty> {
                    sum: Some($max),
                    count: 1,
                    sum_scale: 0,
                    sum_precision: $precision,
                    target_precision: $precision,
                    target_scale: 0,
                };
                let input: ArrayRef =
                    Arc::new(PrimitiveArray::<$ty>::from_iter_values([$one]));
                assert!(acc.update_batch(&[input]).is_err());
                assert_eq!(acc.sum, Some($max));
                let counts: ArrayRef = Arc::new(UInt64Array::from(vec![1]));
                let sums: ArrayRef =
                    Arc::new(PrimitiveArray::<$ty>::from_iter_values([$one]));
                assert!(acc.merge_batch(&[counts, sums]).is_err());
                assert_eq!(acc.sum, Some($max));
            }};
        }
        check!(Decimal32Type, i32::MAX, 1, 9);
        check!(Decimal64Type, i64::MAX, 1, 18);
        check!(Decimal128Type, i128::MAX, 1, 38);
        check!(Decimal256Type, i256::MAX, i256::ONE, 76);
        let mut count = DecimalAvgAccumulator::<Decimal32Type> {
            sum: Some(1),
            count: i32::MAX as u64 + 1,
            sum_scale: 0,
            sum_precision: 9,
            target_precision: 9,
            target_scale: 0,
        };
        assert!(count.evaluate().is_err());
        Ok(())
    }

    #[test]
    fn logex_scalar_state_merge_retraction_and_failure() -> Result<()> {
        let mut acc = scalar();
        acc.update_batch(&[values(vec![Some(90), Some(90), None])])?;
        assert_eq!(acc.evaluate()?, ScalarValue::Decimal128(Some(900000), 6, 4));
        let state = acc.state()?;
        assert_eq!(state[1], ScalarValue::Decimal128(Some(180), 2, 0));
        let mut merged = scalar();
        merged.merge_batch(
            &state
                .iter()
                .map(|x| x.to_array_of_size(1).unwrap())
                .collect::<Vec<_>>(),
        )?;
        assert_eq!(merged.evaluate()?, acc.evaluate()?);
        merged.retract_batch(&[values(vec![Some(90), Some(90)])])?;
        assert_eq!(merged.evaluate()?, ScalarValue::Decimal128(None, 6, 4));
        assert!(merged.retract_batch(&[values(vec![Some(1)])]).is_err());
        assert_eq!(merged.count, 0);
        acc.sum = Some(i128::MAX);
        acc.count = 1;
        assert!(acc.update_batch(&[values(vec![Some(1)])]).is_err());
        assert_eq!(acc.sum, Some(i128::MAX));
        assert_eq!(acc.count, 1);
        acc.sum = Some(i128::MIN);
        assert!(acc.retract_batch(&[values(vec![Some(1)])]).is_err());
        assert_eq!(acc.sum, Some(i128::MIN));
        assert_eq!(acc.count, 1);
        acc.count = u64::MAX;
        acc.sum = Some(1);
        assert!(acc.update_batch(&[values(vec![Some(1)])]).is_err());
        assert_eq!(acc.count, u64::MAX);
        assert_eq!(acc.sum, Some(1));
        let counts: ArrayRef = Arc::new(UInt64Array::from(vec![1]));
        assert!(acc.merge_batch(&[counts, values(vec![Some(1)])]).is_err());
        assert_eq!(acc.count, u64::MAX);
        Ok(())
    }

    #[test]
    fn logex_checked_groups_masks_prefixes_merge_and_poison() -> Result<()> {
        fn group(
        ) -> AvgGroupsAccumulator<Decimal128Type, impl Fn(i128, u64) -> Result<i128> + Send>
        {
            let averager = DecimalAverager::<Decimal128Type>::try_new(0, 6, 4).unwrap();
            AvgGroupsAccumulator::new_checked(
                &DataType::Decimal128(2, 0),
                &DataType::Decimal128(6, 4),
                move |sum, count| averager.avg_with_count(sum, count),
            )
        }
        let mut acc = group();
        let filter = BooleanArray::from(vec![true, true, false, true]);
        let input = values(vec![Some(90), Some(90), Some(99), None]);
        let converted = acc.convert_to_state(&[input.clone()], Some(&filter))?;
        assert_eq!(converted[0].null_count(), 2);
        acc.merge_batch(&converted, &[0, 0, 1, 1], None, 2)?;
        let first = acc.evaluate(EmitTo::First(1))?;
        assert_eq!(first.as_primitive::<Decimal128Type>().value(0), 900000);
        assert_eq!(acc.evaluate(EmitTo::All)?.null_count(), 1);
        let mut overflow = group();
        overflow.sums = vec![i128::MAX];
        overflow.counts = vec![1];
        assert!(overflow
            .update_batch(&[values(vec![Some(1)])], &[0], None, 1)
            .is_err());
        assert!(overflow.state(EmitTo::All).is_err());
        assert!(overflow.evaluate(EmitTo::All).is_err());
        assert!(overflow.convert_to_state(&[input], None).is_err());
        for (sum, count) in [(i128::MAX, 1), (0, u64::MAX)] {
            let mut merge = group();
            merge.sums = vec![sum];
            merge.counts = vec![count];
            let partial_counts: ArrayRef = Arc::new(UInt64Array::from(vec![1]));
            let partial_sums = values(vec![Some(1)]);
            assert!(merge
                .merge_batch(&[partial_counts, partial_sums], &[0], None, 1)
                .is_err());
            assert!(merge.state(EmitTo::All).is_err());
            assert!(merge.evaluate(EmitTo::All).is_err());
            assert!(merge
                .update_batch(&[values(vec![Some(1)])], &[0], None, 1)
                .is_err());
        }
        let averager = DecimalAverager::<Decimal128Type>::try_new(0, 1, 0)?;
        let mut emission = AvgGroupsAccumulator::<Decimal128Type, _>::new_checked(
            &DataType::Decimal128(2, 0),
            &DataType::Decimal128(1, 0),
            move |sum, count| averager.avg_with_count(sum, count),
        );
        // Valid input/state, but output precision is deliberately too small.
        emission.update_batch(&[values(vec![Some(90)])], &[0], None, 1)?;
        assert!(emission.evaluate(EmitTo::All).is_err());
        assert!(emission.state(EmitTo::All).is_err());
        assert!(emission
            .update_batch(&[values(vec![Some(1)])], &[0], None, 1)
            .is_err());
        Ok(())
    }
}
