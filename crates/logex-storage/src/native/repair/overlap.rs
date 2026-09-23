//! Freeze fetch ranges from explicit seeds, then select intersecting owners.
//! Selection alone proves neither block coverage nor replacement authority.
use std::{collections::BTreeSet, io};

use super::super::catalog::{NativeStorageCatalog, SegmentDescriptor};

#[derive(Debug, PartialEq, Eq)]
pub(super) struct Selection {
    pub(super) segment_ids: Vec<u64>,
    pub(super) block_ranges: Vec<(u64, u64)>,
}

pub(super) fn select(
    catalog: &NativeStorageCatalog,
    selected_ids: &[u64],
    max_segments: usize,
    max_blocks: u64,
) -> io::Result<Selection> {
    select_segments(&catalog.segments, selected_ids, max_segments, max_blocks)
}

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

fn block_count(start: u64, end: u64) -> io::Result<u64> {
    end.checked_sub(start)
        .and_then(|span| span.checked_add(1))
        .ok_or_else(|| invalid("repair inclusive block count overflows"))
}

fn select_segments(
    segments: &[SegmentDescriptor],
    selected_ids: &[u64],
    max_segments: usize,
    max_blocks: u64,
) -> io::Result<Selection> {
    if selected_ids.len() > max_segments {
        return Err(invalid("repair selection exceeds segment budget"));
    }
    let mut seeds = BTreeSet::new();
    for &id in selected_ids {
        if !seeds.insert(id) {
            return Err(invalid("duplicate repair segment selection"));
        }
    }
    let mut seen = BTreeSet::new();
    let mut ranges = Vec::new();
    for segment in segments {
        if !seen.insert(segment.id) {
            return Err(invalid("duplicate catalog segment ID"));
        }
        match (segment.row_count, segment.min_block, segment.max_block) {
            (0, None, None) => {}
            (0, _, _) => return Err(invalid("empty segment has block bounds")),
            (_, Some(start), Some(end)) if start <= end => {
                if seeds.contains(&segment.id) {
                    ranges.push((start, end));
                }
            }
            _ => {
                return Err(invalid(
                    "nonempty segment has missing or inverted block bounds",
                ));
            }
        }
    }
    if !seeds.is_subset(&seen) {
        return Err(invalid("unknown repair segment selection"));
    }
    ranges.sort_unstable();
    let mut block_ranges: Vec<(u64, u64)> = Vec::new();
    for (start, end) in ranges {
        if let Some(last) = block_ranges.last_mut()
            && (start <= last.1 || last.1.checked_add(1) == Some(start))
        {
            last.1 = last.1.max(end);
        } else {
            block_ranges.push((start, end));
        }
    }
    let mut blocks = 0u64;
    for &(start, end) in &block_ranges {
        blocks = blocks
            .checked_add(block_count(start, end)?)
            .ok_or_else(|| invalid("repair block count overflow"))?;
        if blocks > max_blocks {
            return Err(invalid("repair selection exceeds block budget"));
        }
    }

    let mut owners = Vec::new();
    for segment in segments {
        let key = match (segment.min_block, segment.max_block) {
            (Some(start), Some(end)) => {
                // Normalized ranges are ordered and disjoint. Skip those ending
                // before this owner, then test only the first remaining range.
                let next = block_ranges.partition_point(|&(_, range_end)| range_end < start);
                if block_ranges
                    .get(next)
                    .is_none_or(|&(range_start, _)| range_start > end)
                {
                    continue;
                }
                (false, start, segment.id, end)
            }
            _ if seeds.contains(&segment.id) => (true, 0, segment.id, 0),
            _ => continue,
        };
        if owners.len() >= max_segments {
            return Err(invalid("repair selection exceeds segment budget"));
        }
        owners.push(key);
    }
    owners.sort_unstable();
    Ok(Selection {
        segment_ids: owners.into_iter().map(|(_, _, id, _)| id).collect(),
        block_ranges,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::native::catalog::SegmentKind;

    fn segment(id: u64, range: Option<(u64, u64)>) -> SegmentDescriptor {
        SegmentDescriptor {
            column_bundle: None,
            id,
            source_namespace: None,
            source_commitment: None,
            source_state: None,
            generation: 0,
            kind: SegmentKind::Sealed,
            relative_path: Default::default(),
            manifest_relative_path: Default::default(),
            min_block: range.map(|value| value.0),
            max_block: range.map(|value| value.1),
            min_timestamp: None,
            max_timestamp: None,
            row_count: u64::from(range.is_some()),
        }
    }

    #[test]
    fn explicit_ranges_do_not_expand_through_healthy_neighbors() {
        let input = vec![
            segment(4, Some((13, 14))),
            segment(2, Some((11, 12))),
            segment(1, Some((10, 11))),
            segment(3, Some((12, 13))),
        ];
        assert_eq!(
            select_segments(&input, &[2], 3, 2).unwrap(),
            Selection {
                segment_ids: vec![1, 2, 3],
                block_ranges: vec![(11, 12)]
            }
        );
        assert!(select_segments(&input, &[2], 2, 2).is_err());
        assert!(select_segments(&input, &[2], 3, 1).is_err());
    }

    #[test]
    fn disjoint_seed_union_counts_shared_owner_once_without_filling_gap() {
        let input = vec![
            segment(9, Some((0, u64::MAX))),
            segment(3, Some((20, 22))),
            segment(1, Some((2, 4))),
            segment(2, Some((4, 6))),
            segment(4, Some((7, 8))),
        ];
        let expected = Selection {
            segment_ids: vec![9, 1, 2, 4, 3],
            block_ranges: vec![(2, 8), (20, 22)],
        };
        assert_eq!(
            select_segments(&input, &[3, 4, 2, 1], 5, 10).unwrap(),
            expected
        );
        let mut reversed = input.clone();
        reversed.reverse();
        assert_eq!(
            select_segments(&reversed, &[1, 2, 4, 3], 5, 10).unwrap(),
            expected
        );
        assert!(select_segments(&input, &[1, 2, 4, 3], 4, 10).is_err());
        assert!(select_segments(&input, &[1, 2, 4, 3], 5, 9).is_err());
        // A broad valid healthy owner does not contribute its span to fetch work.
        assert_eq!(
            select_segments(&input, &[3], 2, 3).unwrap().block_ranges,
            vec![(20, 22)]
        );
    }

    #[test]
    fn empty_extreme_and_overflow_cases_are_checked() {
        let input = vec![
            segment(9, None),
            segment(2, Some((u64::MAX, u64::MAX))),
            segment(1, Some((0, 0))),
            segment(8, None),
        ];
        assert_eq!(
            select_segments(&input, &[9, 2, 1, 8], 4, 2).unwrap(),
            Selection {
                segment_ids: vec![1, 2, 8, 9],
                block_ranges: vec![(0, 0), (u64::MAX, u64::MAX)]
            }
        );
        assert!(select_segments(&input, &[1, 2], 2, 1).is_err());
        assert!(select_segments(&input, &[8], 0, 0).is_err());
        assert_eq!(
            select_segments(&input, &[8], 1, 0).unwrap(),
            Selection {
                segment_ids: vec![8],
                block_ranges: vec![]
            }
        );
        assert_eq!(
            select_segments(&input, &[], 0, 0).unwrap(),
            Selection {
                segment_ids: vec![],
                block_ranges: vec![]
            }
        );
        assert!(select_segments(&[segment(1, Some((0, u64::MAX)))], &[1], 1, u64::MAX).is_err());
        assert!(
            select_segments(
                &[
                    segment(1, Some((0, u64::MAX - 1))),
                    segment(2, Some((u64::MAX, u64::MAX)))
                ],
                &[1, 2],
                2,
                u64::MAX
            )
            .is_err()
        );
        assert_eq!(
            select_segments(
                &[
                    segment(1, Some((u64::MAX - 1, u64::MAX - 1))),
                    segment(2, Some((u64::MAX, u64::MAX)))
                ],
                &[1, 2],
                2,
                2
            )
            .unwrap()
            .block_ranges,
            vec![(u64::MAX - 1, u64::MAX)]
        );
    }

    #[test]
    fn malformed_disconnected_catalog_and_seed_ids_are_rejected() {
        let good = segment(1, Some((1, 2)));
        assert!(select_segments(std::slice::from_ref(&good), &[2], 5, 5).is_err());
        assert!(select_segments(std::slice::from_ref(&good), &[1, 1], 5, 5).is_err());
        assert!(select_segments(&[good.clone(), good.clone()], &[1], 5, 5).is_err());
        let mut missing = segment(2, Some((4, 5)));
        missing.max_block = None;
        let mut empty_bounds = segment(2, Some((4, 5)));
        empty_bounds.row_count = 0;
        for bad in [missing, empty_bounds, segment(2, Some((5, 4)))] {
            assert!(select_segments(&[good.clone(), bad.clone()], &[1], 5, 5).is_err());
            assert!(select_segments(&[good.clone(), bad], &[], 5, 5).is_err());
        }
    }
}
