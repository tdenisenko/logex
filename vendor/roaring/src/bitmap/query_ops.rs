//! Controlled query allocations. Ordinary bitmap operators remain unchanged.
//!
//! Callers reserve the prepared requested allowance before applying an operation.
//! Every newly allocated Vec is reported while locally owned, before it is filled
//! or installed. Allocator excess can therefore be charged immediately; this is
//! cooperative accounting, not a process RSS or allocator-overhead bound.
use super::container::{Container, ARRAY_LIMIT};
use super::store::{ArrayStore, Store};
use super::RoaringBitmap;
use std::{borrow::Cow, cmp::Reverse, io, mem};

const DENSE: usize = 8192;
#[cfg(feature = "simd")]
const TAIL: usize = 7;
#[cfg(not(feature = "simd"))]
const TAIL: usize = 0;
type Observer<'a> = dyn FnMut(usize, usize) -> io::Result<()> + 'a;

/// Operations needed by query candidate selection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QuerySetOp {
    /// Keep rows present in either input.
    Union,
    /// Keep rows present in both inputs.
    Intersection,
}

/// A clone whose source cannot change between preparation and materialization.
#[derive(Debug)]
pub struct PreparedClone<'a> {
    source: &'a RoaringBitmap,
    bytes: usize,
}
/// An assignment retaining borrowed RHS semantics.
#[derive(Debug)]
pub struct PreparedAssign<'a> {
    lhs: &'a mut RoaringBitmap,
    rhs: &'a RoaringBitmap,
    op: QuerySetOp,
    bytes: usize,
    count: usize,
}
/// An assignment consuming RHS contents, preserving owned-store moves.
#[derive(Debug)]
pub struct PreparedOwnedAssign<'a> {
    lhs: &'a mut RoaringBitmap,
    rhs: &'a mut RoaringBitmap,
    op: QuerySetOp,
    bytes: usize,
    count: usize,
}
/// Reference MultiOps union for an inclusive container-key span of at most eight.
#[derive(Debug)]
pub struct PreparedUnionRefs<'a> {
    sources: &'a [&'a RoaringBitmap],
    bytes: usize,
    count: usize,
}

// Mutating kernels can temporarily leave a non-normalized or empty container.
// An observer may fail or unwind at that point. Reset before returning public
// mutable operands; their callers still retain every input/operation charge.
struct ResetOnFailure<'a> {
    left: &'a mut RoaringBitmap,
    right: Option<&'a mut RoaringBitmap>,
    armed: bool,
}
impl Drop for ResetOnFailure<'_> {
    fn drop(&mut self) {
        if self.armed {
            *self.left = RoaringBitmap::new();
            if let Some(right) = self.right.as_deref_mut() {
                *right = RoaringBitmap::new();
            }
        }
    }
}

fn overflow() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidInput,
        "query bitmap allocation overflow",
    )
}
fn add(a: usize, b: usize) -> io::Result<usize> {
    a.checked_add(b).ok_or_else(overflow)
}
fn bytes<T>(n: usize) -> io::Result<usize> {
    n.checked_mul(mem::size_of::<T>()).ok_or_else(overflow)
}
fn vector<T>(n: usize, observe: &mut Observer<'_>) -> io::Result<Vec<T>> {
    let mut output = Vec::new();
    output.try_reserve_exact(n).map_err(|_| {
        io::Error::new(io::ErrorKind::OutOfMemory, "query bitmap allocation failed")
    })?;
    observe(bytes::<T>(n)?, bytes::<T>(output.capacity())?)?;
    Ok(output)
}
fn payload(s: &Store) -> io::Result<usize> {
    match s {
        Store::Array(a) => bytes::<u16>(a.len() as usize),
        Store::Bitmap(_) => Ok(DENSE),
    }
}
fn clone_array(a: &ArrayStore, observe: &mut Observer<'_>) -> io::Result<ArrayStore> {
    let mut v = vector(a.len() as usize, observe)?;
    v.extend_from_slice(a.as_slice());
    Ok(ArrayStore::from_vec_unchecked(v))
}
fn clone_store(s: &Store, observe: &mut Observer<'_>) -> io::Result<Store> {
    Ok(match s {
        Store::Array(a) => Store::Array(clone_array(a, observe)?),
        Store::Bitmap(b) => Store::Bitmap(b.clone()),
    })
}
fn normalize(s: &mut Store, observe: &mut Observer<'_>) -> io::Result<()> {
    match s {
        Store::Bitmap(b) if b.len() <= ARRAY_LIMIT => {
            let mut v = vector(b.len() as usize, observe)?;
            v.extend(b.iter());
            *s = Store::Array(ArrayStore::from_vec_unchecked(v));
        }
        Store::Array(a) if a.len() > ARRAY_LIMIT => {
            *s = Store::Bitmap(a.to_bitmap_store());
        }
        _ => {}
    }
    Ok(())
}
fn array_union(
    a: &ArrayStore,
    b: &ArrayStore,
    observe: &mut Observer<'_>,
) -> io::Result<ArrayStore> {
    let n = add(add(a.len() as usize, b.len() as usize)?, TAIL)?;
    Ok(a.query_union(b, vector(n, observe)?))
}
fn array_intersect(
    a: &mut ArrayStore,
    b: &ArrayStore,
    observe: &mut Observer<'_>,
) -> io::Result<()> {
    #[cfg(feature = "simd")]
    {
        let n = add(a.len().min(b.len()) as usize, TAIL)?;
        *a = a.query_intersection(b, vector(n, observe)?);
    }
    #[cfg(not(feature = "simd"))]
    {
        let _ = observe;
        *a &= b;
    }
    Ok(())
}
fn union_borrowed(a: &mut Store, b: &Store, observe: &mut Observer<'_>) -> io::Result<()> {
    match (a, b) {
        (Store::Array(a), Store::Array(b)) => *a = array_union(a, b, observe)?,
        (Store::Bitmap(a), Store::Array(b)) => *a |= b,
        (Store::Bitmap(a), Store::Bitmap(b)) => *a |= b,
        (a @ Store::Array(_), Store::Bitmap(b)) => {
            let mut out = b.clone();
            if let Store::Array(old) = a {
                out |= &*old;
            }
            *a = Store::Bitmap(out);
        }
    }
    Ok(())
}
fn union_owned(a: &mut Store, mut b: Store, observe: &mut Observer<'_>) -> io::Result<()> {
    if matches!((&*a, &b), (Store::Array(_), Store::Bitmap(_))) {
        mem::swap(a, &mut b);
    }
    union_borrowed(a, &b, observe)
}
fn intersect_borrowed(a: &mut Store, b: &Store, observe: &mut Observer<'_>) -> io::Result<()> {
    match (a, b) {
        (Store::Array(a), Store::Array(b)) => {
            if b.len() < a.len() {
                let mut smaller = clone_array(b, observe)?;
                array_intersect(&mut smaller, a, observe)?;
                *a = smaller;
            } else {
                array_intersect(a, b, observe)?;
            }
        }
        (Store::Bitmap(a), Store::Bitmap(b)) => *a &= b,
        (Store::Array(a), Store::Bitmap(b)) => *a &= b,
        (a @ Store::Bitmap(_), Store::Array(_)) => {
            let mut out = clone_store(b, observe)?;
            intersect_owned(
                &mut out,
                mem::replace(a, Store::Array(ArrayStore::new())),
                observe,
            )?;
            *a = out;
        }
    }
    Ok(())
}
fn intersect_owned(a: &mut Store, mut b: Store, observe: &mut Observer<'_>) -> io::Result<()> {
    match (&mut *a, &mut b) {
        (Store::Array(a), Store::Array(b)) => {
            if b.len() < a.len() {
                mem::swap(a, b);
            }
            array_intersect(a, b, observe)?;
        }
        (Store::Bitmap(a), Store::Bitmap(b)) => *a &= &*b,
        (Store::Array(a), Store::Bitmap(b)) => *a &= &*b,
        (Store::Bitmap(_), Store::Array(_)) => {
            mem::swap(a, &mut b);
            if let (Store::Array(a), Store::Bitmap(b)) = (a, &b) {
                *a &= b;
            }
        }
    }
    Ok(())
}
fn union_count(a: &RoaringBitmap, b: &RoaringBitmap) -> usize {
    a.containers.len()
        + b.containers
            .iter()
            .filter(|c| {
                a.containers
                    .binary_search_by_key(&c.key, |c| c.key)
                    .is_err()
            })
            .count()
}
fn extra(
    a: &RoaringBitmap,
    b: &RoaringBitmap,
    op: QuerySetOp,
    owned: bool,
) -> io::Result<(usize, usize)> {
    let (a, b) = if owned
        && match op {
            QuerySetOp::Union => a.len() < b.len(),
            QuerySetOp::Intersection => b.containers.len() < a.containers.len(),
        } {
        (b, a)
    } else {
        (a, b)
    };
    let count = if op == QuerySetOp::Union {
        union_count(a, b)
    } else {
        a.containers.len()
    };
    let mut total = if count > a.containers.capacity() {
        bytes::<Container>(count)?
    } else {
        0
    };
    let mut intersection_peak = 0;
    for rhs in &b.containers {
        match a.containers.binary_search_by_key(&rhs.key, |c| c.key) {
            Err(_) if op == QuerySetOp::Union && !owned => {
                total = add(total, payload(&rhs.store)?)?
            }
            Ok(i) => {
                let lhs = &a.containers[i];
                if op == QuerySetOp::Union {
                    let cost = match (&lhs.store, &rhs.store) {
                        (Store::Array(x), Store::Array(y)) => {
                            let n = add(x.len() as usize, y.len() as usize)?;
                            add(
                                bytes::<u16>(add(n, TAIL)?)?,
                                if n > ARRAY_LIMIT as usize { DENSE } else { 0 },
                            )?
                        }
                        (Store::Array(_), Store::Bitmap(_)) if !owned => DENSE,
                        _ => 0,
                    };
                    total = add(total, cost)?;
                } else {
                    // Original input charges cover retained stores. Only one
                    // replacement/normalization is live at a time.
                    let clone = if !owned {
                        match (&lhs.store, &rhs.store) {
                            (Store::Array(x), Store::Array(y)) if y.len() < x.len() => {
                                payload(&rhs.store)?
                            }
                            (Store::Bitmap(_), Store::Array(_)) => payload(&rhs.store)?,
                            _ => 0,
                        }
                    } else {
                        0
                    };
                    let normalize = if lhs.store.len().min(rhs.store.len()) > ARRAY_LIMIT
                        || matches!(
                            (&lhs.store, &rhs.store),
                            (Store::Bitmap(_), Store::Bitmap(_))
                        ) {
                        DENSE
                    } else {
                        0
                    };
                    #[cfg(feature = "simd")]
                    let scratch = match (&lhs.store, &rhs.store) {
                        (Store::Array(x), Store::Array(y)) => {
                            bytes::<u16>(add(x.len().min(y.len()) as usize, TAIL)?)?
                        }
                        _ => 0,
                    };
                    #[cfg(not(feature = "simd"))]
                    let scratch = 0;
                    intersection_peak =
                        intersection_peak.max(add(add(clone, normalize)?, scratch)?);
                    // SIMD retains its tail capacity in every resulting array.
                    #[cfg(feature = "simd")]
                    {
                        total = add(total, bytes::<u16>(TAIL)?)?;
                    }
                }
            }
            _ => {}
        }
    }
    Ok((add(total, intersection_peak)?, count))
}
fn reserve_containers(
    v: &mut Vec<Container>,
    count: usize,
    observe: &mut Observer<'_>,
) -> io::Result<()> {
    if count > v.capacity() {
        let mut replacement = vector(count, observe)?;
        replacement.append(v);
        *v = replacement;
    }
    Ok(())
}
impl RoaringBitmap {
    /// Prepare a clone with explicit payload and container-vector capacities.
    pub fn prepare_clone(&self) -> io::Result<PreparedClone<'_>> {
        let mut n = bytes::<Container>(self.containers.len())?;
        for c in &self.containers {
            n = add(n, payload(&c.store)?)?;
        }
        Ok(PreparedClone {
            source: self,
            bytes: n,
        })
    }
    /// Prepare assignment while retaining a borrowed RHS.
    pub fn prepare_assign<'a>(
        &'a mut self,
        rhs: &'a Self,
        op: QuerySetOp,
    ) -> io::Result<PreparedAssign<'a>> {
        let (bytes, count) = extra(self, rhs, op, false)?;
        Ok(PreparedAssign {
            lhs: self,
            rhs,
            op,
            bytes,
            count,
        })
    }
    /// Prepare destructive RHS transfer. Both input allocations must stay charged.
    /// Applying with an error or unwind resets both inputs to empty.
    /// Dropping an unapplied preparation leaves both inputs unchanged.
    pub fn prepare_owned_assign<'a>(
        &'a mut self,
        rhs: &'a mut Self,
        op: QuerySetOp,
    ) -> io::Result<PreparedOwnedAssign<'a>> {
        let (bytes, count) = extra(self, rhs, op, true)?;
        Ok(PreparedOwnedAssign {
            lhs: self,
            rhs,
            op,
            bytes,
            count,
        })
    }
    /// Prepare the reference MultiOps policy for a key span smaller than eight.
    pub fn prepare_union_refs<'a>(sources: &'a [&'a Self]) -> io::Result<PreparedUnionRefs<'a>> {
        let min = sources
            .iter()
            .filter_map(|b| b.containers.first().map(|c| c.key))
            .min();
        let max = sources
            .iter()
            .filter_map(|b| b.containers.last().map(|c| c.key))
            .max();
        let mut count = 0;
        let mut payloads = 0;
        let mut normalization = 0;
        if let (Some(min), Some(max)) = (min, max) {
            if max - min >= 8 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "reference query union requires key span below eight",
                ));
            }
            for key in u32::from(min)..=u32::from(max) {
                let mut hits = 0;
                let mut cardinality = 0;
                let mut single = 0;
                let mut oversized = false;
                for source in sources {
                    if let Ok(i) = source
                        .containers
                        .binary_search_by_key(&(key as u16), |c| c.key)
                    {
                        hits += 1;
                        let s = &source.containers[i].store;
                        cardinality = (cardinality + s.len()).min(65536);
                        single = payload(s)?;
                        oversized = matches!(s,Store::Array(a) if a.len()>ARRAY_LIMIT);
                    }
                }
                if hits > 0 {
                    count += 1;
                    payloads = add(payloads, if hits == 1 { single } else { DENSE })?;
                    if hits > 1 {
                        normalization =
                            normalization.max(bytes::<u16>(cardinality.min(ARRAY_LIMIT) as usize)?);
                    } else if oversized {
                        normalization = normalization.max(DENSE);
                    }
                }
            }
        }
        let mut n = add(payloads, normalization)?;
        n = add(n, bytes::<&Self>(prefix(sources.len()))?)?;
        n = add(n, bytes::<Cow<'_, Container>>(count)?)?;
        n = add(n, bytes::<Container>(count)?)?;
        Ok(PreparedUnionRefs {
            sources,
            bytes: n,
            count,
        })
    }
}
impl PreparedClone<'_> {
    /// Requested bytes, additional to the source's retained memory.
    pub fn allocation_bytes(&self) -> usize {
        self.bytes
    }
    /// Allocate a clone, reporting actual Vec capacities before installation.
    pub fn materialize(
        self,
        mut observe: impl FnMut(usize, usize) -> io::Result<()>,
    ) -> io::Result<RoaringBitmap> {
        let mut containers = vector(self.source.containers.len(), &mut observe)?;
        for c in &self.source.containers {
            containers.push(Container {
                key: c.key,
                store: clone_store(&c.store, &mut observe)?,
            });
        }
        Ok(RoaringBitmap { containers })
    }
}
impl PreparedAssign<'_> {
    /// Requested peak allowance beyond the existing LHS and RHS charges.
    pub fn additional_allocation_bytes(&self) -> usize {
        self.bytes
    }
    /// Apply the operation. Errors and unwinds reset LHS to empty; borrowed RHS is unchanged.
    pub fn apply(self, mut observe: impl FnMut(usize, usize) -> io::Result<()>) -> io::Result<()> {
        let mut reset = ResetOnFailure {
            left: self.lhs,
            right: None,
            armed: true,
        };
        let lhs = &mut *reset.left;
        if self.op == QuerySetOp::Union {
            reserve_containers(&mut lhs.containers, self.count, &mut observe)?;
            for c in &self.rhs.containers {
                match lhs.containers.binary_search_by_key(&c.key, |c| c.key) {
                    Err(i) => lhs.containers.insert(
                        i,
                        Container {
                            key: c.key,
                            store: clone_store(&c.store, &mut observe)?,
                        },
                    ),
                    Ok(i) => {
                        let s = &mut lhs.containers[i].store;
                        union_borrowed(s, &c.store, &mut observe)?;
                        normalize(s, &mut observe)?;
                    }
                }
            }
        } else {
            retain_fallible(&mut lhs.containers, |c| {
                if let Ok(j) = self.rhs.containers.binary_search_by_key(&c.key, |c| c.key) {
                    intersect_borrowed(&mut c.store, &self.rhs.containers[j].store, &mut observe)?;
                    normalize(&mut c.store, &mut observe)?;
                    Ok(!c.is_empty())
                } else {
                    Ok(false)
                }
            })?;
        }
        reset.armed = false;
        Ok(())
    }
}
impl PreparedOwnedAssign<'_> {
    /// Requested peak allowance beyond BOTH existing input charges.
    pub fn additional_allocation_bytes(&self) -> usize {
        self.bytes
    }
    /// Consume RHS contents, preserving owned operators' swaps and moves.
    /// Errors and unwinds reset both operands to empty.
    /// Keep both reservations until the result's charge has been reconciled.
    pub fn apply(self, mut observe: impl FnMut(usize, usize) -> io::Result<()>) -> io::Result<()> {
        let mut reset = ResetOnFailure {
            left: self.lhs,
            right: Some(self.rhs),
            armed: true,
        };
        let lhs = &mut *reset.left;
        let rhs = reset
            .right
            .as_deref_mut()
            .expect("owned assignment has RHS");
        if match self.op {
            QuerySetOp::Union => lhs.len() < rhs.len(),
            QuerySetOp::Intersection => rhs.containers.len() < lhs.containers.len(),
        } {
            mem::swap(lhs, rhs);
        }
        if self.op == QuerySetOp::Union {
            reserve_containers(&mut lhs.containers, self.count, &mut observe)?;
            for c in mem::take(&mut rhs.containers) {
                match lhs.containers.binary_search_by_key(&c.key, |c| c.key) {
                    Err(i) => lhs.containers.insert(i, c),
                    Ok(i) => {
                        let s = &mut lhs.containers[i].store;
                        union_owned(s, c.store, &mut observe)?;
                        normalize(s, &mut observe)?;
                    }
                }
            }
        } else {
            retain_fallible(&mut lhs.containers, |c| {
                if let Ok(j) = rhs.containers.binary_search_by_key(&c.key, |c| c.key) {
                    let rhs = mem::replace(&mut rhs.containers[j], Container::new(c.key));
                    intersect_owned(&mut c.store, rhs.store, &mut observe)?;
                    normalize(&mut c.store, &mut observe)?;
                    Ok(!c.is_empty())
                } else {
                    Ok(false)
                }
            })?;
            rhs.containers = Vec::new();
        }
        reset.armed = false;
        Ok(())
    }
}
fn retain_fallible(
    v: &mut Vec<Container>,
    mut f: impl FnMut(&mut Container) -> io::Result<bool>,
) -> io::Result<()> {
    let mut error = None;
    v.retain_mut(|c| {
        if error.is_some() {
            return true;
        }
        match f(c) {
            Ok(keep) => keep,
            Err(e) => {
                error = Some(e);
                true
            }
        }
    });
    match error {
        Some(e) => Err(e),
        None => Ok(()),
    }
}
fn prefix(n: usize) -> usize {
    if n <= 50 {
        n
    } else {
        10
    }
}
impl PreparedUnionRefs<'_> {
    /// Requested working allocation including reference and Cow vectors.
    pub fn allocation_bytes(&self) -> usize {
        self.bytes
    }
    /// Preserve reference MultiOps prefix sorting and lazy dense promotion.
    pub fn materialize(
        self,
        mut observe: impl FnMut(usize, usize) -> io::Result<()>,
    ) -> io::Result<RoaringBitmap> {
        let n = prefix(self.sources.len());
        let mut start = vector(n, &mut observe)?;
        start.extend_from_slice(&self.sources[..n]);
        start.sort_unstable_by_key(|b| Reverse(b.containers.len()));
        let mut containers: Vec<Cow<'_, Container>> = vector(self.count, &mut observe)?;
        for source in start
            .iter()
            .copied()
            .chain(self.sources[n..].iter().copied())
        {
            for rhs in &source.containers {
                match containers.binary_search_by_key(&rhs.key, |c| c.key) {
                    Err(i) => containers.insert(i, Cow::Borrowed(rhs)),
                    Ok(i) => {
                        let lhs = &mut containers[i];
                        match (&lhs.store, &rhs.store) {
                            (Store::Array(a), Store::Array(b)) => {
                                let mut bits = a.to_bitmap_store();
                                bits |= b;
                                *lhs = Cow::Owned(Container {
                                    key: rhs.key,
                                    store: Store::Bitmap(bits),
                                });
                            }
                            (Store::Array(a), Store::Bitmap(b)) => {
                                let mut bits = b.clone();
                                bits |= a;
                                *lhs = Cow::Owned(Container {
                                    key: rhs.key,
                                    store: Store::Bitmap(bits),
                                });
                            }
                            (Store::Bitmap(_), _) => {
                                if let Cow::Borrowed(c) = lhs {
                                    *lhs = Cow::Owned(Container {
                                        key: c.key,
                                        store: clone_store(&c.store, &mut observe)?,
                                    });
                                }
                                if let Cow::Owned(c) = lhs {
                                    union_borrowed(&mut c.store, &rhs.store, &mut observe)?;
                                }
                            }
                        }
                    }
                }
            }
        }
        let mut output = vector(self.count, &mut observe)?;
        for c in containers {
            let mut c = match c {
                Cow::Owned(c) => c,
                Cow::Borrowed(c) => Container {
                    key: c.key,
                    store: clone_store(&c.store, &mut observe)?,
                },
            };
            if !c.is_empty() {
                normalize(&mut c.store, &mut observe)?;
                output.push(c);
            }
        }
        Ok(RoaringBitmap { containers: output })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rows(bitmap: &RoaringBitmap) -> Vec<u32> {
        bitmap.iter().collect()
    }
    fn sample(key: u16, start: u32, len: u32) -> RoaringBitmap {
        (start..start + len)
            .map(|low| (u32::from(key) << 16) | low)
            .collect()
    }
    fn observe(requested: usize, actual: usize) -> io::Result<()> {
        assert!(actual >= requested);
        Ok(())
    }

    #[test]
    fn logex_query_ops_clone_and_binary_semantics() {
        let fixtures = [
            RoaringBitmap::new(),
            sample(0, 0, 3),
            sample(0, 1, 4096),
            sample(0, 0, 4097),
            sample(0, 3000, 5000),
            sample(65535, 0, 65536),
        ];
        for a in &fixtures {
            let plan = a.prepare_clone().unwrap();
            let allowance = plan.allocation_bytes();
            let cloned = plan.materialize(observe).unwrap();
            assert_eq!(rows(a), rows(&cloned));
            assert!(cloned.heap_size_bytes().unwrap() >= allowance);
            for b in &fixtures {
                for op in [QuerySetOp::Union, QuerySetOp::Intersection] {
                    let expected = match op {
                        QuerySetOp::Union => a | b,
                        QuerySetOp::Intersection => a & b,
                    };
                    let mut borrowed = a.clone();
                    borrowed
                        .prepare_assign(b, op)
                        .unwrap()
                        .apply(observe)
                        .unwrap();
                    assert_eq!(rows(&borrowed), rows(&expected));
                    let (mut left, mut right) = (a.clone(), b.clone());
                    left.prepare_owned_assign(&mut right, op)
                        .unwrap()
                        .apply(observe)
                        .unwrap();
                    assert_eq!(rows(&left), rows(&expected));
                    assert!(right.is_empty());
                    assert_eq!(right.heap_size_bytes().unwrap(), 0);
                }
            }
        }
    }

    #[test]
    fn logex_query_ops_owned_moves_and_small_allowances() {
        let mut a = sample(0, 0, 10);
        let mut b = sample(1, 0, 2);
        a.containers.reserve_exact(1);
        let right_array = match &b.containers[0].store {
            Store::Array(a) => a.as_slice().as_ptr(),
            _ => unreachable!(),
        };
        let plan = a.prepare_owned_assign(&mut b, QuerySetOp::Union).unwrap();
        assert_eq!(plan.additional_allocation_bytes(), 0);
        let mut calls = 0;
        plan.apply(|_, _| {
            calls += 1;
            Ok(())
        })
        .unwrap();
        assert_eq!(calls, 0);
        assert_eq!(
            match &a.containers[1].store {
                Store::Array(a) => a.as_slice().as_ptr(),
                _ => unreachable!(),
            },
            right_array
        );

        // Intersection selects the smaller container vector and reuses its array.
        let mut a = sample(0, 0, 30);
        a |= sample(1, 0, 10);
        let mut b = sample(0, 2, 3);
        let vector = b.containers.as_ptr();
        #[cfg(not(feature = "simd"))]
        let array = match &b.containers[0].store {
            Store::Array(a) => a.as_slice().as_ptr(),
            _ => unreachable!(),
        };
        a.prepare_owned_assign(&mut b, QuerySetOp::Intersection)
            .unwrap()
            .apply(observe)
            .unwrap();
        assert_eq!(a.containers.as_ptr(), vector);
        #[cfg(not(feature = "simd"))]
        assert_eq!(
            match &a.containers[0].store {
                Store::Array(a) => a.as_slice().as_ptr(),
                _ => unreachable!(),
            },
            array
        );

        let mut a = sample(0, 0, 1);
        let b = sample(0, 2, 1);
        let plan = a.prepare_assign(&b, QuerySetOp::Union).unwrap();
        assert_eq!(plan.additional_allocation_bytes(), (2 + TAIL) * 2);
        plan.apply(observe).unwrap();
        assert_eq!(rows(&a), [0, 2]);
    }

    #[test]
    fn logex_query_ops_normalization_and_spare_capacity() {
        let mut a = sample(0, 0, 8192);
        let b = sample(0, 8191, 8192);
        let plan = a.prepare_assign(&b, QuerySetOp::Intersection).unwrap();
        assert_eq!(plan.additional_allocation_bytes(), DENSE + TAIL * 2);
        let mut requested = Vec::new();
        plan.apply(|n, c| {
            requested.push(n);
            observe(n, c)
        })
        .unwrap();
        assert_eq!(rows(&a), [8191]);
        assert_eq!(requested, [2]);
        let mut a = sample(0, 0, 4096);
        let b = sample(0, 4096, 1);
        let plan = a.prepare_assign(&b, QuerySetOp::Union).unwrap();
        assert_eq!(
            plan.additional_allocation_bytes(),
            (4097 + TAIL) * 2 + DENSE
        );
        plan.apply(observe).unwrap();
        assert!(matches!(a.containers[0].store, Store::Bitmap(_)));
        let a = sample(0, 0, 3000);
        let mut b = a.clone();
        b.prepare_assign(&a, QuerySetOp::Union)
            .unwrap()
            .apply(observe)
            .unwrap();
        assert!(matches!(&b.containers[0].store,Store::Array(a) if a.capacity()>=6000));
        let cloned = b.prepare_clone().unwrap().materialize(observe).unwrap();
        assert_eq!(rows(&cloned), rows(&b));
        assert!(cloned.heap_size_bytes().unwrap() <= b.heap_size_bytes().unwrap());
    }

    #[test]
    fn logex_query_ops_reference_union_prefix_and_lazy_promotion() {
        use crate::MultiOps;
        for n in [0, 1, 2, 50, 51, 70] {
            let sources: Vec<_> = (0..n)
                .map(|i| {
                    sample(
                        (i % 8) as u16,
                        (i * 37) as u32,
                        if i % 3 == 0 { 4200 } else { 3 },
                    )
                })
                .collect();
            let refs: Vec<_> = sources.iter().collect();
            let expected = refs.iter().copied().union();
            let actual = RoaringBitmap::prepare_union_refs(&refs)
                .unwrap()
                .materialize(observe)
                .unwrap();
            assert_eq!(rows(&actual), rows(&expected));
        }
        let (a, b) = (sample(0, 0, 1), sample(0, 2, 1));
        let refs = [&a, &b];
        let plan = RoaringBitmap::prepare_union_refs(&refs).unwrap();
        let control = 2 * mem::size_of::<&RoaringBitmap>()
            + mem::size_of::<Cow<'_, Container>>()
            + mem::size_of::<Container>();
        assert_eq!(plan.allocation_bytes(), control + DENSE + 4);
        assert_eq!(rows(&plan.materialize(observe).unwrap()), [0, 2]);
        let far = sample(8, 0, 1);
        assert!(RoaringBitmap::prepare_union_refs(&[&a, &far]).is_err());
        let last = sample(65535, 1, 2);
        assert_eq!(
            rows(
                &RoaringBitmap::prepare_union_refs(&[&last])
                    .unwrap()
                    .materialize(observe)
                    .unwrap()
            ),
            rows(&last)
        );
    }

    fn assert_valid_empty(bitmap: &RoaringBitmap) {
        assert!(bitmap.is_empty());
        assert_eq!(bitmap.len(), 0);
        assert_eq!(bitmap.iter().next(), None);
        assert_eq!(bitmap.min(), None);
        assert_eq!(bitmap.max(), None);
        assert_eq!(bitmap.heap_size_bytes().unwrap(), 0);
        let mut encoded = Vec::new();
        bitmap.serialize_into(&mut encoded).unwrap();
        let decoded = RoaringBitmap::deserialize_from(&encoded[..]).unwrap();
        assert!(decoded.is_empty());
    }

    #[test]
    fn logex_query_ops_failed_mutations_reset_public_operands() {
        use std::panic::{catch_unwind, AssertUnwindSafe};
        for owned in [false, true] {
            for (op, left, right) in [
                (
                    QuerySetOp::Intersection,
                    sample(0, 0, 8192),
                    sample(0, 8191, 8192),
                ),
                (
                    QuerySetOp::Intersection,
                    sample(0, 0, 8192),
                    sample(0, 8192, 8192),
                ),
                (QuerySetOp::Union, sample(0, 0, 4000), sample(0, 3000, 4000)),
            ] {
                // The preparation itself performs no mutation or allocation.
                let (mut a, mut b) = (left.clone(), right.clone());
                if owned {
                    drop(a.prepare_owned_assign(&mut b, op).unwrap());
                } else {
                    drop(a.prepare_assign(&b, op).unwrap());
                }
                assert_eq!(rows(&a), rows(&left));
                assert_eq!(rows(&b), rows(&right));
                let mut calls = 0;
                if owned {
                    a.prepare_owned_assign(&mut b, op)
                        .unwrap()
                        .apply(|_, _| {
                            calls += 1;
                            Ok(())
                        })
                        .unwrap();
                } else {
                    a.prepare_assign(&b, op)
                        .unwrap()
                        .apply(|_, _| {
                            calls += 1;
                            Ok(())
                        })
                        .unwrap();
                }
                assert!(calls > 0);
                for fail in 0..calls {
                    for unwind in [false, true] {
                        let (mut a, mut b) = (left.clone(), right.clone());
                        let mut step = 0;
                        let outcome = catch_unwind(AssertUnwindSafe(|| {
                            let observer = |_, _| {
                                let reject = step == fail;
                                step += 1;
                                if reject {
                                    assert!(!unwind, "injected observer unwind");
                                    Err(io::Error::new(io::ErrorKind::Other, "injected"))
                                } else {
                                    Ok(())
                                }
                            };
                            if owned {
                                a.prepare_owned_assign(&mut b, op).unwrap().apply(observer)
                            } else {
                                a.prepare_assign(&b, op).unwrap().apply(observer)
                            }
                        }));
                        if unwind {
                            assert!(outcome.is_err());
                        } else {
                            assert!(outcome.unwrap().is_err());
                        }
                        assert_valid_empty(&a);
                        if owned {
                            assert_valid_empty(&b);
                        } else {
                            assert_eq!(rows(&b), rows(&right));
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn logex_query_ops_observer_errors_drop_local_allocations() {
        let mut source = sample(0, 0, 4000);
        source |= sample(1, 0, 2);
        let mut calls = 0;
        source
            .prepare_clone()
            .unwrap()
            .materialize(|_, _| {
                calls += 1;
                Ok(())
            })
            .unwrap();
        for fail in 0..calls {
            let mut step = 0;
            let error = source
                .prepare_clone()
                .unwrap()
                .materialize(|_, _| {
                    let reject = step == fail;
                    step += 1;
                    if reject {
                        Err(io::Error::new(io::ErrorKind::Other, "injected"))
                    } else {
                        Ok(())
                    }
                })
                .unwrap_err();
            assert_eq!(error.to_string(), "injected");
            assert_eq!(source.len(), 4002);
        }
        let other = sample(0, 3999, 4000);
        let references = [&source, &other];
        let mut count = 0;
        RoaringBitmap::prepare_union_refs(&references)
            .unwrap()
            .materialize(|_, _| {
                count += 1;
                Ok(())
            })
            .unwrap();
        for fail in 0..count {
            let mut step = 0;
            let result = RoaringBitmap::prepare_union_refs(&references)
                .unwrap()
                .materialize(|_, _| {
                    let reject = step == fail;
                    step += 1;
                    if reject {
                        Err(io::Error::new(io::ErrorKind::Other, "injected"))
                    } else {
                        Ok(())
                    }
                });
            assert!(result.is_err());
            assert_eq!(source.len(), 4002);
            assert_eq!(other.len(), 4000);
        }
        for owned in [false, true] {
            for op in [QuerySetOp::Union, QuerySetOp::Intersection] {
                let right = sample(0, 3000, 5000);
                let mut baseline = source.clone();
                let mut rhs = right.clone();
                let mut count = 0;
                if owned {
                    baseline
                        .prepare_owned_assign(&mut rhs, op)
                        .unwrap()
                        .apply(|_, _| {
                            count += 1;
                            Ok(())
                        })
                        .unwrap();
                } else {
                    baseline
                        .prepare_assign(&rhs, op)
                        .unwrap()
                        .apply(|_, _| {
                            count += 1;
                            Ok(())
                        })
                        .unwrap();
                }
                for fail in 0..count {
                    let mut left = source.clone();
                    let mut rhs = right.clone();
                    let mut step = 0;
                    let observer = |_, _| {
                        let reject = step == fail;
                        step += 1;
                        if reject {
                            Err(io::Error::new(io::ErrorKind::Other, "injected"))
                        } else {
                            Ok(())
                        }
                    };
                    let result = if owned {
                        left.prepare_owned_assign(&mut rhs, op)
                            .unwrap()
                            .apply(observer)
                    } else {
                        left.prepare_assign(&rhs, op).unwrap().apply(observer)
                    };
                    assert!(result.is_err());
                    assert!(left.containers.windows(2).all(|w| w[0].key < w[1].key));
                    assert!(rhs.containers.windows(2).all(|w| w[0].key < w[1].key));
                }
            }
        }
    }
}
