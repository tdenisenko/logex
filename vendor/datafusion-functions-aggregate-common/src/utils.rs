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

use arrow::array::{ArrayRef, ArrowNativeTypeOp};
use arrow::compute::SortOptions;
use arrow::datatypes::{
    ArrowNativeType, DataType, DecimalType, Field, FieldRef, ToByteSlice,
};
use datafusion_common::{exec_err, Result};
use datafusion_expr_common::accumulator::Accumulator;
use datafusion_physical_expr_common::sort_expr::{LexOrdering, PhysicalSortExpr};
use std::sync::Arc;

/// Convert scalar values from an accumulator into arrays.
pub fn get_accum_scalar_values_as_arrays(
    accum: &mut dyn Accumulator,
) -> Result<Vec<ArrayRef>> {
    accum
        .state()?
        .iter()
        .map(|s| s.to_array_of_size(1))
        .collect()
}

/// Construct corresponding fields for the expressions in an ORDER BY clause.
pub fn ordering_fields(
    order_bys: &[PhysicalSortExpr],
    // Data type of each expression in the ordering requirement
    data_types: &[DataType],
) -> Vec<FieldRef> {
    order_bys
        .iter()
        .zip(data_types.iter())
        .map(|(sort_expr, dtype)| {
            Field::new(
                sort_expr.expr.to_string().as_str(),
                dtype.clone(),
                // Multi partitions may be empty hence field should be nullable.
                true,
            )
        })
        .map(Arc::new)
        .collect()
}

/// Selects the sort option attribute from all the given `PhysicalSortExpr`s.
pub fn get_sort_options(ordering_req: &LexOrdering) -> Vec<SortOptions> {
    ordering_req.iter().map(|item| item.options).collect()
}

/// A wrapper around a type to provide hash for floats
#[derive(Copy, Clone, Debug)]
pub struct Hashable<T>(pub T);

impl<T: ToByteSlice> std::hash::Hash for Hashable<T> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.0.to_byte_slice().hash(state)
    }
}

impl<T: ArrowNativeTypeOp> PartialEq for Hashable<T> {
    fn eq(&self, other: &Self) -> bool {
        self.0.is_eq(other.0)
    }
}

impl<T: ArrowNativeTypeOp> Eq for Hashable<T> {}

/// Computes averages for all Arrow decimal storage widths, checking for overflow
///
/// This is needed because different decimal precisions can
/// store different ranges of values and thus sum/count may not fit in
/// the target type.
///
/// For example, the precision is 3, the max of value is `999` and the min
/// value is `-999`
pub struct DecimalAverager<T: DecimalType> {
    /// Relative scale factor, also valid for negative decimal scales.
    scale_mul: T::Native,
    /// the output precision
    target_precision: u8,
    /// the output scale
    target_scale: i8,
}

impl<T: DecimalType> DecimalAverager<T> {
    /// Create a new `DecimalAverager`:
    ///
    /// * sum_scale: the scale of `sum` values passed to [`Self::avg`]
    /// * target_precision: the output precision
    /// * target_scale: the output scale
    ///
    /// Errors if the resulting data can not be stored
    pub fn try_new(
        sum_scale: i8,
        target_precision: u8,
        target_scale: i8,
    ) -> Result<Self> {
        let delta = i16::from(target_scale) - i16::from(sum_scale);
        let exponent = u32::try_from(delta).map_err(|_| {
            datafusion_common::exec_datafusion_err!("AVG scale cannot decrease")
        })?;
        // Ten is representable in every Arrow decimal native storage type.
        let ten = T::Native::from_usize(10).unwrap();
        let scale_mul = ten
            .pow_checked(exponent)
            .map_err(|_| datafusion_common::exec_datafusion_err!("AVG scale overflow"))?;
        Ok(Self {
            scale_mul,
            target_precision,
            target_scale,
        })
    }

    /// Convert the accumulator's count without narrowing or panicking.
    pub fn avg_with_count(&self, sum: T::Native, count: u64) -> Result<T::Native> {
        let count = usize::try_from(count)
            .ok()
            .and_then(T::Native::from_usize)
            .ok_or_else(|| {
                datafusion_common::exec_datafusion_err!("AVG count overflow")
            })?;
        self.avg(sum, count)
    }

    /// Returns `sum`/`count` in the decimal native storage type with
    /// target_scale and target_precision and reporting overflow.
    ///
    /// * sum: The total sum in native storage with sum_scale
    ///   (passed to `Self::try_new`)
    /// * count: positive integer count, stored in the native type (not scaled)
    #[inline(always)]
    pub fn avg(&self, sum: T::Native, count: T::Native) -> Result<T::Native> {
        if count <= T::Native::default() {
            return exec_err!("AVG count must be positive");
        }
        if let Ok(value) = sum.mul_checked(self.scale_mul) {
            let new_value = value.div_checked(count).map_err(|_| {
                datafusion_common::exec_datafusion_err!(
                    "AVG invalid count or division overflow"
                )
            })?;

            let validate = T::validate_decimal_precision(
                new_value,
                self.target_precision,
                self.target_scale,
            );

            if validate.is_ok() {
                Ok(new_value)
            } else {
                exec_err!("AVG arithmetic overflow")
            }
        } else {
            // can't convert the lit decimal to the returned data type
            exec_err!("AVG arithmetic overflow")
        }
    }
}

#[cfg(test)]
mod logex_decimal_average_tests {
    use super::*;
    use arrow::datatypes::{
        i256, Decimal128Type, Decimal256Type, Decimal32Type, Decimal64Type,
    };

    #[test]
    fn decimal_average_scales_counts_and_output_bounds() -> Result<()> {
        let avg = DecimalAverager::<Decimal128Type>::try_new(-2, 14, 2)?;
        assert_eq!(avg.avg_with_count(3, 2)?, 15000);
        assert_eq!(avg.avg_with_count(-3, 2)?, -15000);
        assert!(avg.avg_with_count(3, 0).is_err());
        assert!(avg.avg(3, -1).is_err());
        assert!(DecimalAverager::<Decimal128Type>::try_new(2, 10, 1).is_err());
        assert!(DecimalAverager::<Decimal32Type>::try_new(-128, 9, 127).is_err());
        assert!(DecimalAverager::<Decimal128Type>::try_new(0, 2, 0)?
            .avg_with_count(100, 1)
            .is_err());
        assert!(DecimalAverager::<Decimal32Type>::try_new(0, 9, 0)?
            .avg_with_count(1, i32::MAX as u64 + 1)
            .is_err());
        assert!(DecimalAverager::<Decimal64Type>::try_new(0, 18, 0)?
            .avg_with_count(1, u64::MAX)
            .is_err());
        assert_eq!(
            DecimalAverager::<Decimal256Type>::try_new(0, 76, 0)?
                .avg_with_count(i256::from_i128(6), 3)?,
            i256::from_i128(2)
        );
        // Truncation is toward zero, with the existing target-scale contract.
        assert_eq!(
            DecimalAverager::<Decimal128Type>::try_new(0, 10, 4)?
                .avg_with_count(-1, 3)?,
            -3333
        );
        Ok(())
    }
}
