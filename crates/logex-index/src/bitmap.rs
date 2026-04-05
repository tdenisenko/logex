use roaring::RoaringBitmap;

/// Intersect multiple bitmaps. Returns an empty bitmap if the input is empty.
pub fn intersect(bitmaps: &[RoaringBitmap]) -> RoaringBitmap {
    match bitmaps.split_first() {
        None => RoaringBitmap::new(),
        Some((first, rest)) => {
            let mut result = first.clone();
            for bm in rest {
                result &= bm;
            }
            result
        }
    }
}

/// Union multiple bitmaps. Returns an empty bitmap if the input is empty.
pub fn union(bitmaps: &[RoaringBitmap]) -> RoaringBitmap {
    let mut result = RoaringBitmap::new();
    for bm in bitmaps {
        result |= bm;
    }
    result
}

/// Subtract `rhs` from `lhs`.
pub fn difference(lhs: &RoaringBitmap, rhs: &RoaringBitmap) -> RoaringBitmap {
    lhs - rhs
}

/// Intersect two bitmaps without cloning — consumes both.
pub fn intersect_owned(a: RoaringBitmap, b: RoaringBitmap) -> RoaringBitmap {
    a & b
}

/// Union two bitmaps without cloning — consumes both.
pub fn union_owned(a: RoaringBitmap, b: RoaringBitmap) -> RoaringBitmap {
    a | b
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bm(vals: &[u32]) -> RoaringBitmap {
        vals.iter().copied().collect()
    }

    #[test]
    fn test_intersect_empty() {
        assert!(intersect(&[]).is_empty());
    }

    #[test]
    fn test_intersect_single() {
        let a = bm(&[1, 2, 3]);
        assert_eq!(intersect(std::slice::from_ref(&a)), a);
    }

    #[test]
    fn test_intersect_multiple() {
        let a = bm(&[1, 2, 3, 4]);
        let b = bm(&[2, 3, 5]);
        let c = bm(&[3, 5, 6]);
        let result = intersect(&[a, b, c]);
        assert_eq!(result, bm(&[3]));
    }

    #[test]
    fn test_union_empty() {
        assert!(union(&[]).is_empty());
    }

    #[test]
    fn test_union_multiple() {
        let a = bm(&[1, 2]);
        let b = bm(&[3, 4]);
        let c = bm(&[2, 4, 5]);
        let result = union(&[a, b, c]);
        assert_eq!(result, bm(&[1, 2, 3, 4, 5]));
    }

    #[test]
    fn test_difference() {
        let a = bm(&[1, 2, 3, 4, 5]);
        let b = bm(&[2, 4]);
        assert_eq!(difference(&a, &b), bm(&[1, 3, 5]));
    }

    #[test]
    fn test_owned_ops() {
        let a = bm(&[1, 2, 3]);
        let b = bm(&[2, 3, 4]);
        assert_eq!(intersect_owned(a.clone(), b.clone()), bm(&[2, 3]));
        assert_eq!(union_owned(a, b), bm(&[1, 2, 3, 4]));
    }
}
