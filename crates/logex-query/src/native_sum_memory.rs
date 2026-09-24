use std::io;

use alloy_primitives::Bytes;
use num_bigint::{BigInt, BigUint};

use logex_types::{QueryBuffer, QueryMemoryBudget, QueryMemoryError, QueryMemoryReservation};

pub(crate) const NATIVE_SUM_STAGE: &str = "native data SUM working memory";
const LIMB_BYTES: usize = std::mem::size_of::<usize>();

/// Numeric state used inside the grouped SUM workspace. It deliberately owns
/// no reservation: the workspace admits a whole sparse batch before any state
/// mutates and retains one aggregate charge for every state backing.
pub(crate) struct RestrictedSumState {
    value: BigInt,
    capacity_limbs: usize,
    count: u64,
}

#[derive(Clone, Default)]
pub(crate) struct RestrictedSumBatchPlan {
    count: u64,
    max_bits: usize,
    max_bytes: usize,
    has_data: bool,
}

pub(crate) struct RestrictedSumAdmission {
    next_count: u64,
    retained_limbs: usize,
    scratch_bytes: usize,
}

impl RestrictedSumState {
    pub(crate) fn zero() -> Self {
        Self {
            value: BigInt::default(),
            capacity_limbs: 0,
            count: 0,
        }
    }

    pub(crate) fn value(&self) -> Option<&BigInt> {
        (self.count > 0).then_some(&self.value)
    }

    pub(crate) fn retained_bytes(&self) -> io::Result<usize> {
        limb_bytes(self.capacity_limbs)
    }

    pub(crate) fn plan_batch(
        &self,
        plan: &RestrictedSumBatchPlan,
    ) -> io::Result<RestrictedSumAdmission> {
        if plan.count == 0 {
            return Ok(RestrictedSumAdmission {
                next_count: self.count,
                retained_limbs: self.capacity_limbs,
                scratch_bytes: 0,
            });
        }
        let next_count = self
            .count
            .checked_add(plan.count)
            .ok_or_else(size_overflow)?;
        let batch_count = usize::try_from(plan.count).map_err(|_| size_overflow())?;
        let result_bits = usize::try_from(self.value.bits())
            .map_err(|_| size_overflow())?
            .max(
                plan.max_bits
                    .checked_add(ceil_log2(batch_count))
                    .ok_or_else(size_overflow)?,
            )
            .checked_add(1)
            .ok_or_else(size_overflow)?;
        let input_limbs = limbs_for_bits(plan.max_bits)?;
        let result_limbs = limbs_for_bits(result_bits)?;
        let retained_limbs = growth_bound(
            self.capacity_limbs.max(input_limbs),
            result_limbs
                .max(input_limbs)
                .checked_add(1)
                .ok_or_else(size_overflow)?,
        )?;
        let retained_bytes = limb_bytes(retained_limbs)?;
        let input_bytes = limb_bytes(input_limbs)?;
        let addition_peak = input_bytes
            .checked_add(retained_bytes.checked_mul(2).ok_or_else(size_overflow)?)
            .ok_or_else(size_overflow)?;
        let data_constructor = if plan.has_data {
            plan.max_bytes
                .checked_add(input_bytes.checked_mul(2).ok_or_else(size_overflow)?)
                .ok_or_else(size_overflow)?
        } else {
            0
        };
        Ok(RestrictedSumAdmission {
            next_count,
            retained_limbs,
            scratch_bytes: addition_peak.max(data_constructor),
        })
    }

    /// Install conservative history before mutation. Any later error is
    /// terminal for the containing workspace, so this metadata may overstate a
    /// partially completed batch but can never undercharge its live backing.
    pub(crate) fn begin_batch(&mut self, admission: &RestrictedSumAdmission) {
        self.count = admission.next_count;
        self.capacity_limbs = admission.retained_limbs;
    }

    pub(crate) fn add_data(&mut self, bytes: &[u8]) {
        self.value += BigInt::from(BigUint::from_bytes_be(bytes));
    }

    pub(crate) fn add_literal(&mut self, value: &BigInt) {
        // Pinned num-bigint AddAssign<&BigInt> borrows the prepared literal;
        // no per-row BigInt clone or transferable RHS backing is involved.
        self.value += value;
    }

    pub(crate) fn plan_merge(&self, other: &Self) -> io::Result<RestrictedSumAdmission> {
        let next_count = self
            .count
            .checked_add(other.count)
            .ok_or_else(size_overflow)?;
        if other.count == 0 {
            return Ok(RestrictedSumAdmission {
                next_count,
                retained_limbs: self.capacity_limbs,
                scratch_bytes: 0,
            });
        }
        let rhs_limbs =
            limbs_for_bits(usize::try_from(other.value.bits()).map_err(|_| size_overflow())?)?;
        let result_limbs = limbs_for_bits(
            usize::try_from(self.value.bits().max(other.value.bits()))
                .map_err(|_| size_overflow())?
                .checked_add(1)
                .ok_or_else(size_overflow)?,
        )?;
        let retained_limbs = growth_bound(
            self.capacity_limbs.max(rhs_limbs),
            result_limbs
                .max(rhs_limbs)
                .checked_add(1)
                .ok_or_else(size_overflow)?,
        )?;
        Ok(RestrictedSumAdmission {
            next_count,
            retained_limbs,
            // Both existing operands remain charged. Pinned owned AddAssign
            // forwards to borrowed AddAssign, leaving only lhs replacement /
            // normalization scratch to admit here.
            scratch_bytes: limb_bytes(retained_limbs)?
                .checked_mul(2)
                .ok_or_else(size_overflow)?,
        })
    }

    pub(crate) fn merge_from(&mut self, mut other: Self, admission: &RestrictedSumAdmission) {
        self.begin_batch(admission);
        self.value += std::mem::take(&mut other.value);
    }
}

impl RestrictedSumBatchPlan {
    pub(crate) fn include_data(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.count = self.count.checked_add(1).ok_or_else(size_overflow)?;
        self.max_bytes = self.max_bytes.max(bytes.len());
        self.max_bits = self
            .max_bits
            .max(bytes.len().checked_mul(8).ok_or_else(size_overflow)?);
        self.has_data = true;
        Ok(())
    }

    pub(crate) fn include_literal(&mut self, value: &BigInt) -> io::Result<()> {
        self.count = self.count.checked_add(1).ok_or_else(size_overflow)?;
        self.max_bits = self
            .max_bits
            .max(usize::try_from(value.bits()).map_err(|_| size_overflow())?);
        Ok(())
    }
}

impl RestrictedSumAdmission {
    pub(crate) fn retained_bytes(&self) -> io::Result<usize> {
        limb_bytes(self.retained_limbs)
    }

    pub(crate) fn scratch_bytes(&self) -> usize {
        self.scratch_bytes
    }
}

/// Restricted owner for the one accumulator used by the ungrouped data-only
/// native SUM path. `capacity_limbs` follows the pinned num-bigint 0.4.6 Vec
/// growth proof and the nightly-2026-08-24 RawVec behavior audited with it; both
/// pins require re-review. The reservation is last so the BigInt is destroyed
/// first. Any mutating method error is terminal: callers must drop this owner,
/// because a canceled arithmetic operation may have changed `value` while the
/// deliberately conservative scratch charge remains retained.
///
/// Source anchors: num-bigint `src/biguint/{convert,multiplication,division}.rs`,
/// `src/biguint/{addition,subtraction}.rs`, and Rust alloc `src/raw_vec/mod.rs`.
pub(crate) struct NativeDataSumAccumulator {
    value: BigInt,
    capacity_limbs: usize,
    count: u64,
    budget: QueryMemoryBudget,
    reservation: QueryMemoryReservation,
}

impl NativeDataSumAccumulator {
    pub(crate) fn new(budget: QueryMemoryBudget) -> io::Result<Self> {
        let reservation = budget
            .reserve(0, NATIVE_SUM_STAGE)
            .map_err(io::Error::other)?;
        Ok(Self {
            value: BigInt::default(),
            capacity_limbs: 0,
            count: 0,
            budget,
            reservation,
        })
    }

    pub(crate) fn value(&self) -> Option<&BigInt> {
        (self.count > 0).then_some(&self.value)
    }

    pub(crate) fn budget(&self) -> &QueryMemoryBudget {
        &self.budget
    }

    pub(crate) fn add_batch(
        &mut self,
        values: &QueryBuffer<Bytes>,
        mut canceled: impl FnMut() -> bool,
    ) -> io::Result<()> {
        if values.is_empty() {
            return Ok(());
        }
        let batch_count = u64::try_from(values.len()).map_err(|_| size_overflow())?;
        let next_count = self
            .count
            .checked_add(batch_count)
            .ok_or_else(size_overflow)?;
        let max_bytes = values.iter().map(|value| value.len()).max().unwrap_or(0);
        let input_limbs = limbs_for_bytes(max_bytes)?;
        let batch_bits = max_bytes
            .checked_mul(8)
            .and_then(|bits| bits.checked_add(ceil_log2(values.len())))
            .ok_or_else(size_overflow)?;
        let result_bits = usize::try_from(self.value.bits())
            .map_err(|_| size_overflow())?
            .max(batch_bits)
            .checked_add(1)
            .ok_or_else(size_overflow)?;
        let result_limbs = limbs_for_bits(result_bits)?;
        // num-bigint mutates the owned accumulator. Pinned RawVec growth is
        // G(C,R)=max(C,2*(R-1),R,4); keep the final G backing plus the larger
        // of from_bytes_be's byte-copy/two-limb-backing constructor peak and
        // AddAssign's input backing/two-G replacement peak.
        let retained_limbs = growth_bound(
            self.capacity_limbs.max(input_limbs),
            result_limbs
                .max(input_limbs)
                .checked_add(1)
                .ok_or_else(size_overflow)?,
        )?;
        let retained_bytes = limb_bytes(retained_limbs)?;
        let constructor_peak = max_bytes
            .checked_add(
                limb_bytes(input_limbs)?
                    .checked_mul(2)
                    .ok_or_else(size_overflow)?,
            )
            .ok_or_else(size_overflow)?;
        let addition_peak = limb_bytes(input_limbs)?
            .checked_add(
                limb_bytes(retained_limbs)?
                    .checked_mul(2)
                    .ok_or_else(size_overflow)?,
            )
            .ok_or_else(size_overflow)?;
        let scratch = constructor_peak.max(addition_peak);
        let admitted = retained_bytes
            .checked_add(scratch)
            .ok_or_else(size_overflow)?;
        self.grow_to(admitted)?;

        for value in values.iter() {
            self.value += BigInt::from(BigUint::from_bytes_be(value.as_ref()));
            if canceled() {
                return Err(io::Error::new(io::ErrorKind::Interrupted, "query canceled"));
            }
        }
        self.count = next_count;
        self.capacity_limbs = retained_limbs;
        self.shrink_to(retained_bytes)?;
        Ok(())
    }

    pub(crate) fn merge(
        &mut self,
        mut other: Self,
        canceled: impl FnOnce() -> bool,
    ) -> io::Result<()> {
        if canceled() {
            return Err(io::Error::new(io::ErrorKind::Interrupted, "query canceled"));
        }
        let next_count = self
            .count
            .checked_add(other.count)
            .ok_or_else(size_overflow)?;
        if other.count == 0 {
            self.count = next_count;
            return Ok(());
        }
        if self.count == 0 {
            // Moving the value preserves the source reservation until after the
            // backing has moved, then transfers its conservative retained charge.
            let other_bytes = limb_bytes(other.capacity_limbs)?;
            self.grow_to(other_bytes)?;
            self.value = std::mem::take(&mut other.value);
            self.capacity_limbs = other.capacity_limbs;
            self.count = next_count;
            return Ok(());
        }
        let result_limbs = limbs_for_bits(
            usize::try_from(self.value.bits().max(other.value.bits()))
                .map_err(|_| size_overflow())?
                .checked_add(1)
                .ok_or_else(size_overflow)?,
        )?;
        let retained_limbs = growth_bound(
            self.capacity_limbs.max(other.capacity_limbs),
            result_limbs
                .max(other.capacity_limbs)
                .checked_add(1)
                .ok_or_else(size_overflow)?,
        )?;
        let retained_bytes = limb_bytes(retained_limbs)?;
        let admitted = retained_bytes
            .checked_add(
                limb_bytes(retained_limbs)?
                    .checked_mul(2)
                    .ok_or_else(size_overflow)?,
            )
            .ok_or_else(size_overflow)?;
        self.grow_to(admitted)?;
        self.value += std::mem::take(&mut other.value);
        self.capacity_limbs = retained_limbs;
        self.count = next_count;
        drop(other);
        self.shrink_to(retained_bytes)?;
        Ok(())
    }

    pub(crate) fn reserve_decimal_scratch(&mut self, value: &BigInt) -> io::Result<usize> {
        let scratch = decimal_scratch_bytes(value)?;
        self.reservation
            .try_grow(scratch)
            .map_err(io::Error::other)?;
        Ok(scratch)
    }

    pub(crate) fn admit_expression_arena(&mut self, nodes: usize) -> io::Result<()> {
        if self.count == 0 || nodes == 0 {
            return Ok(());
        }
        let value_limbs = limbs_for_bits(
            usize::try_from(self.value.bits())
                .map_err(|_| size_overflow())?
                .checked_add(ceil_log2(nodes.saturating_add(1)))
                .and_then(|bits| bits.checked_add(1))
                .ok_or_else(size_overflow)?,
        )?;
        let retained = growth_bound(
            self.capacity_limbs.max(value_limbs),
            value_limbs.checked_add(1).ok_or_else(size_overflow)?,
        )?;
        // Every AST node can own one result backing while its parent operation
        // temporarily owns a replacement. This deliberately retains the whole
        // bounded expression arena through JSON conversion.
        let bytes = limb_bytes(retained)?
            .checked_mul(3)
            .and_then(|bytes| bytes.checked_mul(nodes))
            .ok_or_else(size_overflow)?;
        self.reservation.try_grow(bytes).map_err(io::Error::other)
    }

    pub(crate) fn release_scratch(&mut self, bytes: usize) -> io::Result<()> {
        self.reservation.shrink(bytes).map_err(io::Error::other)
    }

    fn grow_to(&mut self, bytes: usize) -> io::Result<()> {
        let current = usize::try_from(self.reservation.bytes()).map_err(|_| size_overflow())?;
        if bytes > current {
            self.reservation
                .try_grow(bytes - current)
                .map_err(io::Error::other)?;
        }
        Ok(())
    }

    fn shrink_to(&mut self, bytes: usize) -> io::Result<()> {
        let current = usize::try_from(self.reservation.bytes()).map_err(|_| size_overflow())?;
        if current > bytes {
            self.reservation
                .shrink(current - bytes)
                .map_err(io::Error::other)?;
        }
        Ok(())
    }
}

fn size_overflow() -> io::Error {
    io::Error::other(QueryMemoryError::SizeOverflow {
        stage: NATIVE_SUM_STAGE,
    })
}

fn limbs_for_bytes(bytes: usize) -> io::Result<usize> {
    bytes
        .checked_add(LIMB_BYTES - 1)
        .map(|bytes| bytes / LIMB_BYTES)
        .ok_or_else(size_overflow)
}

fn limbs_for_bits(bits: usize) -> io::Result<usize> {
    bits.checked_add(LIMB_BYTES * 8 - 1)
        .map(|bits| bits / (LIMB_BYTES * 8))
        .ok_or_else(size_overflow)
}

fn limb_bytes(limbs: usize) -> io::Result<usize> {
    limbs.checked_mul(LIMB_BYTES).ok_or_else(size_overflow)
}

fn ceil_log2(value: usize) -> usize {
    usize::BITS as usize - value.saturating_sub(1).leading_zeros() as usize
}

// alloc/src/raw_vec/mod.rs in nightly-2026-08-24 grows a Vec request R to at
// most max(C, 2*(R-1), R, 4) when the old capacity C is insufficient.
fn growth_bound(capacity: usize, request: usize) -> io::Result<usize> {
    if request == 0 {
        return Ok(capacity);
    }
    Ok(capacity
        .max(
            request
                .checked_sub(1)
                .and_then(|v| v.checked_mul(2))
                .ok_or_else(size_overflow)?,
        )
        .max(request)
        .max(4))
}

fn retained_capacity_bound(length: usize) -> io::Result<usize> {
    Ok(4usize.max(length.checked_mul(2).ok_or_else(size_overflow)?))
}

fn multiplication_scratch(limbs: usize) -> io::Result<usize> {
    if limbs <= 32 {
        return Ok(0);
    }
    let half = limbs / 2;
    // h(t)=t-floor((floor(t/2)+1)/2) bounds every Karatsuba child and
    // difference; children execute sequentially, giving S(t)=K(t)+S(h(t)).
    let child = limbs
        .checked_sub(half.div_ceil(2))
        .ok_or_else(size_overflow)?;
    // Pinned num-bigint-0.4.6 biguint/multiplication.rs: Karatsuba retains p
    // and a possible replacement (2*(t+2)), three child-sized difference
    // backings, then one recursively executing child.
    let karatsuba = limbs
        .checked_add(2)
        .and_then(|v| v.checked_mul(2))
        .and_then(|v| v.checked_add(child.checked_mul(3)?))
        .ok_or_else(size_overflow)?;
    let local = if limbs <= 256 {
        karatsuba
    } else {
        let toom_len = (limbs / 3)
            .checked_add(1)
            .and_then(|v| v.checked_mul(2))
            .and_then(|v| v.checked_add(3))
            .ok_or_else(size_overflow)?;
        // Toom-3 has 35 source-derived intermediate/result slots, each bounded
        // by C(2*(floor(t/3)+1)+3). Each slot may overlap one replacement,
        // hence the 35 * 2 * C term; its children are covered by S(h(t)).
        let toom = retained_capacity_bound(toom_len)?
            .checked_mul(70)
            .ok_or_else(size_overflow)?;
        karatsuba.max(toom)
    };
    local
        .checked_add(multiplication_scratch(child)?)
        .ok_or_else(size_overflow)
}

fn decimal_scratch_bytes(value: &BigInt) -> io::Result<usize> {
    let bits = usize::try_from(value.bits()).map_err(|_| size_overflow())?;
    let limbs = limbs_for_bits(bits)?;
    let limb_scratch = if limbs < 64 {
        limbs.checked_mul(2).ok_or_else(size_overflow)?
    } else {
        let root = integer_sqrt(limbs);
        let base = 4usize.max(root.checked_mul(2).ok_or_else(size_overflow)?);
        // Pinned biguint/convert.rs decimal formatting keeps the original
        // digit clone and sqrt-sized base. Borrowed division can overlap a
        // shifted numerator, shifted divisor and quotient with one replacement
        // of each; the loop is sequential, so this peak is not per iteration.
        let numerator = growth_bound(limbs, limbs.checked_add(1).ok_or_else(size_overflow)?)?;
        let divisor_limbs = root.checked_mul(2).ok_or_else(size_overflow)?;
        let divisor = growth_bound(
            divisor_limbs,
            divisor_limbs.checked_add(1).ok_or_else(size_overflow)?,
        )?;
        let quotient = limbs.checked_add(1).ok_or_else(size_overflow)?;
        let base_build = base
            .checked_mul(2)
            .and_then(|v| v.checked_add(multiplication_scratch(root.saturating_sub(1)).ok()?))
            .ok_or_else(size_overflow)?;
        let division = numerator
            .checked_mul(2)
            .and_then(|v| v.checked_add(divisor.checked_mul(2)?))
            .and_then(|v| v.checked_add(quotient.checked_mul(2)?))
            .ok_or_else(size_overflow)?;
        limbs
            .checked_add(base)
            .and_then(|v| v.checked_add(base_build.max(division)))
            .ok_or_else(size_overflow)?
    };
    let digits = bits
        .checked_mul(302)
        .and_then(|bits| bits.checked_add(999))
        .map(|bits| bits / 1000)
        .ok_or_else(size_overflow)?
        .max(1)
        .checked_add(usize::from(value.sign() == num_bigint::Sign::Minus))
        .ok_or_else(size_overflow)?;
    // num-bigint-0.4.6 biguint/convert.rs uses this floating estimate for its
    // initial decimal Vec. The final Vec can overlap one doubled replacement.
    let estimate = ((bits as f64) / 10f64.log2()).ceil() as usize;
    // If D is the signed decimal length, pinned RawVec growth from the initial
    // estimate E is bounded by max(E, 2*(D-1), D, 8). Charge twice that bound
    // because the old decimal Vec can coexist with its replacement.
    let decimal_capacity = estimate
        .max(
            digits
                .saturating_sub(1)
                .checked_mul(2)
                .ok_or_else(size_overflow)?,
        )
        .max(digits)
        .max(8);
    limb_bytes(limb_scratch)?
        .checked_add(decimal_capacity.checked_mul(2).ok_or_else(size_overflow)?)
        .ok_or_else(size_overflow)
}

fn integer_sqrt(value: usize) -> usize {
    if value < 2 {
        return value;
    }
    let mut low = 1usize;
    let mut high = value / 2 + 1;
    while low < high {
        let middle = low + (high - low).div_ceil(2);
        if middle <= value / middle {
            low = middle;
        } else {
            high = middle - 1;
        }
    }
    low
}

#[cfg(test)]
mod tests {
    use super::*;
    use logex_types::QueryMemoryLimit;

    #[test]
    fn checked_bounds_cover_thresholds_without_unbounded_inputs() {
        for limbs in [1, 32, 33, 63, 64, 256, 257] {
            let value = BigInt::from(BigUint::from(1u8) << (limbs * LIMB_BYTES * 8 - 1));
            assert!(decimal_scratch_bytes(&value).unwrap() >= limbs * LIMB_BYTES);
            assert_eq!(format!("{value}"), value.to_str_radix(10));
        }
        assert!(growth_bound(4, 5).unwrap() >= 8);
        assert_eq!(multiplication_scratch(32).unwrap(), 0);
        assert!(multiplication_scratch(33).unwrap() > 0);
        assert!(multiplication_scratch(257).unwrap() > multiplication_scratch(256).unwrap());
    }

    #[test]
    fn restricted_state_covers_leading_zero_signed_carry_and_merge() {
        let leading = [0, 0, 0, 0, 1];
        let negative = BigInt::from(-2);
        let mut plan = RestrictedSumBatchPlan::default();
        plan.include_data(&leading).unwrap();
        plan.include_literal(&negative).unwrap();
        let mut state = RestrictedSumState::zero();
        let admission = state.plan_batch(&plan).unwrap();
        assert!(admission.retained_bytes().unwrap() >= LIMB_BYTES * 4);
        state.begin_batch(&admission);
        state.add_data(&leading);
        state.add_literal(&negative);
        assert_eq!(state.value(), Some(&BigInt::from(-1)));

        let carry = vec![0xff; LIMB_BYTES];
        let mut source_plan = RestrictedSumBatchPlan::default();
        source_plan.include_data(&carry).unwrap();
        source_plan.include_data(&[1]).unwrap();
        let mut source = RestrictedSumState::zero();
        let source_admission = source.plan_batch(&source_plan).unwrap();
        source.begin_batch(&source_admission);
        source.add_data(&carry);
        source.add_data(&[1]);
        let expected = BigInt::from(BigUint::from_bytes_be(&carry)) + 1;
        assert_eq!(source.value(), Some(&expected));

        let merge = state.plan_merge(&source).unwrap();
        state.merge_from(source, &merge);
        assert_eq!(state.value(), Some(&(expected - 1)));
        assert_eq!(state.count, 4);
    }

    #[test]
    fn merge_uses_normalized_rhs_length_not_historical_capacity() {
        let destination = RestrictedSumState::zero();
        let source = RestrictedSumState {
            value: BigInt::from(1),
            capacity_limbs: 1_024,
            count: 1,
        };
        let admission = destination.plan_merge(&source).unwrap();
        assert!(admission.retained_limbs < source.capacity_limbs);
        assert!(admission.retained_limbs <= 4);
    }

    #[test]
    fn batch_denial_and_success_keep_the_charge_with_the_accumulator() {
        let memory = QueryMemoryBudget::new(QueryMemoryLimit::new(4096).unwrap());
        let mut values = QueryBuffer::try_with_capacity(2, Some(&memory), "fixture").unwrap();
        values.try_push(Bytes::from_static(&[1])).unwrap();
        values.try_push(Bytes::from_static(&[2])).unwrap();
        let mut sum = NativeDataSumAccumulator::new(memory.clone()).unwrap();
        sum.add_batch(&values, || false).unwrap();
        assert_eq!(sum.value().unwrap(), &BigInt::from(3));
        assert_eq!(sum.count, 2);
        drop(values);
        assert!(memory.used() > 0);
        drop(sum);
        assert_eq!(memory.used(), 0);
    }

    #[test]
    fn cancellation_after_mutation_and_capacity_denial_release_every_charge() {
        let values =
            QueryBuffer::unaccounted(vec![Bytes::from_static(&[1]), Bytes::from_static(&[2])]);
        let memory = QueryMemoryBudget::new(QueryMemoryLimit::new(4096).unwrap());
        let mut sum = NativeDataSumAccumulator::new(memory.clone()).unwrap();
        let error = sum.add_batch(&values, || memory.used() > 0).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::Interrupted);
        assert!(memory.used() > 0, "the canceled mutation must stay charged");
        drop(sum);
        assert_eq!(memory.used(), 0);

        let constrained = QueryMemoryBudget::new(QueryMemoryLimit::new(16).unwrap());
        let mut sum = NativeDataSumAccumulator::new(constrained.clone()).unwrap();
        let large = QueryBuffer::unaccounted(vec![Bytes::from(vec![0xff; 32])]);
        assert!(sum.add_batch(&large, || false).is_err());
        drop(sum);
        assert_eq!(constrained.used(), 0);
    }

    #[test]
    fn merge_and_decimal_denial_preserve_arithmetic_and_ownership() {
        let memory = QueryMemoryBudget::new(QueryMemoryLimit::new(4096).unwrap());
        let mut left = NativeDataSumAccumulator::new(memory.clone()).unwrap();
        left.add_batch(
            &QueryBuffer::unaccounted(vec![Bytes::from_static(&[1])]),
            || false,
        )
        .unwrap();
        let mut right = NativeDataSumAccumulator::new(memory.clone()).unwrap();
        right
            .add_batch(
                &QueryBuffer::unaccounted(vec![Bytes::from_static(&[2])]),
                || false,
            )
            .unwrap();
        left.merge(right, || false).unwrap();
        assert_eq!(left.value().unwrap(), &BigInt::from(3));
        assert_eq!(left.count, 2);

        let value = left.value().unwrap().clone();
        let scratch = decimal_scratch_bytes(&value).unwrap();
        let available = memory.limit() - usize::try_from(memory.used()).unwrap();
        assert!(available >= scratch);
        let held = memory
            .reserve(available - (scratch - 1), "competing query")
            .unwrap();
        assert!(left.reserve_decimal_scratch(&value).is_err());
        drop(held);
        drop(left);
        assert_eq!(memory.used(), 0);
    }

    #[test]
    fn leading_zeros_and_true_limb_carry_match_bigint() {
        let mut leading_zero = vec![0u8; LIMB_BYTES * 4];
        *leading_zero.last_mut().unwrap() = 1;
        let max_limb = vec![0xff; LIMB_BYTES];
        let values = QueryBuffer::unaccounted(vec![
            Bytes::from(leading_zero),
            Bytes::from(max_limb),
            Bytes::from_static(&[1]),
        ]);
        let memory = QueryMemoryBudget::new(QueryMemoryLimit::new(4096).unwrap());
        let mut sum = NativeDataSumAccumulator::new(memory.clone()).unwrap();
        sum.add_batch(&values, || false).unwrap();
        let expected = (BigUint::from(1u8) << (LIMB_BYTES * 8)) + BigUint::from(1u8);
        assert_eq!(sum.value().unwrap(), &BigInt::from(expected));
        drop(sum);
        assert_eq!(memory.used(), 0);
    }

    #[test]
    fn denied_merge_drops_the_consumed_partial_and_preserves_destination_charge() {
        let memory = QueryMemoryBudget::new(QueryMemoryLimit::new(4096).unwrap());
        let mut left = NativeDataSumAccumulator::new(memory.clone()).unwrap();
        left.add_batch(
            &QueryBuffer::unaccounted(vec![Bytes::from_static(&[1])]),
            || false,
        )
        .unwrap();
        let mut right = NativeDataSumAccumulator::new(memory.clone()).unwrap();
        right
            .add_batch(
                &QueryBuffer::unaccounted(vec![Bytes::from_static(&[2])]),
                || false,
            )
            .unwrap();
        let before_left = left.reservation.bytes();
        let available = memory.limit() - usize::try_from(memory.used()).unwrap();
        let held = memory.reserve(available, "competing query").unwrap();
        assert!(left.merge(right, || false).is_err());
        assert_eq!(left.reservation.bytes(), before_left);
        drop(held);
        drop(left);
        assert_eq!(memory.used(), 0);
    }
}
