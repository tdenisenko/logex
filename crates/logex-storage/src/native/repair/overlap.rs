//! Pure selection of potentially shared block owners, not proof of block
//! completeness, chain membership, provenance or permission to replace data.
use std::{collections::BTreeSet, io};

use super::super::catalog::{NativeStorageCatalog, SegmentDescriptor};
use super::RepairOwnershipGroup as Group;

pub(super) fn select(
    catalog: &NativeStorageCatalog,
    selected_ids: &[u64],
    max_segments: usize,
    max_blocks: u64,
) -> io::Result<Vec<Group>> {
    select_segments(&catalog.segments, selected_ids, max_segments, max_blocks)
}

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

fn block_count(start: u64, end: u64) -> io::Result<u64> {
    end.checked_sub(start)
        .and_then(|span| span.checked_add(1))
        .ok_or_else(|| invalid("repair overlap range is inverted or its inclusive size overflows"))
}

fn select_segments(
    segments: &[SegmentDescriptor],
    selected_ids: &[u64],
    max_segments: usize,
    max_blocks: u64,
) -> io::Result<Vec<Group>> {
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
    let mut intervals = Vec::new();
    let mut empty = Vec::new();
    for segment in segments {
        if !seen.insert(segment.id) {
            return Err(invalid("duplicate catalog segment ID"));
        }
        match (segment.row_count, segment.min_block, segment.max_block) {
            (0, None, None) => {
                if seeds.contains(&segment.id) {
                    if empty.len() >= max_segments {
                        return Err(invalid("repair overlap exceeds segment budget"));
                    }
                    empty.push(segment.id);
                }
            }
            (0, _, _) => return Err(invalid("empty segment has block bounds")),
            (_, Some(start), Some(end)) => {
                block_count(start, end)?;
                intervals.push((start, segment.id, end));
            }
            _ => return Err(invalid("nonempty segment lacks block bounds")),
        }
    }
    if !seeds.is_subset(&seen) {
        return Err(invalid("unknown repair segment selection"));
    }
    intervals.sort_unstable();
    empty.sort_unstable();

    // Plans borrow positions, not descriptors or payload. Check the cumulative
    // selected closure before allocating any output segment-ID vectors.
    let mut plans = Vec::new();
    let mut segment_count = empty.len();
    if segment_count > max_segments {
        return Err(invalid("repair overlap exceeds segment budget"));
    }
    let mut blocks = 0u64;
    let mut first = 0;
    while first < intervals.len() {
        let (start, id, mut end) = intervals[first];
        let mut selected = seeds.contains(&id);
        let mut next = first + 1;
        while next < intervals.len() && intervals[next].0 <= end {
            end = end.max(intervals[next].2);
            selected |= seeds.contains(&intervals[next].1);
            next += 1;
        }
        let component_blocks = block_count(start, end)?;
        if selected {
            segment_count = segment_count
                .checked_add(next - first)
                .ok_or_else(|| invalid("repair overlap segment count overflow"))?;
            blocks = blocks
                .checked_add(component_blocks)
                .ok_or_else(|| invalid("repair overlap block count overflow"))?;
            if segment_count > max_segments || blocks > max_blocks {
                return Err(invalid("repair overlap exceeds work budget"));
            }
            plans.push((first, next, start, end));
        }
        first = next;
    }
    let mut groups = Vec::new();
    for (first, next, start, end) in plans {
        groups.push(Group {
            segment_ids: intervals[first..next].iter().map(|entry| entry.1).collect(),
            range: Some((start, end)),
        });
    }
    // Empty segments have no range sort key: put them last, ordered by ID.
    for id in empty {
        groups.push(Group {
            segment_ids: vec![id],
            range: None,
        });
    }
    Ok(groups)
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
    fn transitive_nested_overlap_selects_whole_component_not_neighbors() {
        let mut input = vec![
            segment(7, Some((30, 40))),
            segment(3, Some((8, 12))),
            segment(1, Some((1, 5))),
            segment(2, Some((5, 9))),
            segment(4, Some((6, 7))),
            segment(5, Some((13, 20))),
        ];
        let expected = vec![Group {
            segment_ids: vec![1, 2, 4, 3],
            range: Some((1, 12)),
        }];
        assert_eq!(select_segments(&input, &[4], 4, 12).unwrap(), expected);
        input.reverse();
        assert_eq!(select_segments(&input, &[3, 1], 4, 12).unwrap(), expected);
        assert!(select_segments(&input, &[4], 3, 12).is_err());
        assert!(select_segments(&input, &[4], 4, 11).is_err());
    }

    #[test]
    fn empty_and_extreme_ranges_have_exact_cumulative_budgets() {
        let input = vec![
            segment(9, None),
            segment(2, Some((u64::MAX, u64::MAX))),
            segment(1, Some((0, 0))),
            segment(8, None),
        ];
        assert_eq!(
            select_segments(&input, &[9, 2, 1, 8], 4, 2).unwrap(),
            vec![
                Group {
                    segment_ids: vec![1],
                    range: Some((0, 0))
                },
                Group {
                    segment_ids: vec![2],
                    range: Some((u64::MAX, u64::MAX))
                },
                Group {
                    segment_ids: vec![8],
                    range: None
                },
                Group {
                    segment_ids: vec![9],
                    range: None
                },
            ]
        );
        assert!(select_segments(&input, &[1, 2], 2, 1).is_err());
        assert!(select_segments(&input, &[8], 0, 0).is_err());
        assert_eq!(select_segments(&input, &[8], 1, 0).unwrap()[0].range, None);
        assert!(select_segments(&input, &[], 0, 0).unwrap().is_empty());
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
    }

    #[test]
    fn malformed_catalog_and_selection_are_rejected_even_when_disconnected() {
        let good = segment(1, Some((1, 2)));
        assert!(select_segments(std::slice::from_ref(&good), &[2], 5, 5).is_err());
        assert!(select_segments(std::slice::from_ref(&good), &[1, 1], 5, 5).is_err());
        assert!(select_segments(&[good.clone(), good.clone()], &[1], 5, 5).is_err());
        let mut missing = segment(2, Some((4, 5)));
        missing.max_block = None;
        let mut empty_bounds = segment(2, Some((4, 5)));
        empty_bounds.row_count = 0;
        for bad in [missing, empty_bounds, segment(2, Some((5, 4)))] {
            assert!(select_segments(&[good.clone(), bad], &[1], 5, 5).is_err());
        }
    }
}
