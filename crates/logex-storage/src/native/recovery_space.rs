//! Additional logical file growth for one verified maintenance recovery attempt.
//! Original artifacts already consume available space and are never credited as
//! freed. This is a conservative peak bound, not a reservation: filesystem blocks,
//! quotas, directory entries and other writers remain outside this accounting.
use super::super::{
    recovery::MaintenanceWork,
    segment::{self, BundleAppendSizer},
};
use super::*;

fn add(total: &mut u64, value: u64) -> io::Result<()> {
    *total = total.checked_add(value).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "maintenance recovery size bound overflows",
        )
    })?;
    Ok(())
}

fn payload(rows: &[LogRow]) -> io::Result<u64> {
    rows.iter().try_fold(0u64, |total, row| {
        total.checked_add(row.data.len() as u64).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "recovery payload size overflows",
            )
        })
    })
}

fn present(rows: usize) -> NullBitmap {
    let mut canonical = NullBitmap::new();
    for _ in 0..rows {
        canonical.push(true);
    }
    canonical
}

enum Layout {
    Raw {
        payload: u64,
    },
    Bundle(Box<BundleAppendSizer>),
    /// Legacy per-file pages replace full indexes and bitmaps on each append.
    /// The payload encoder is identical to bundled pages; metadata is bounded
    /// separately using the retained index lengths and exact appended page count.
    Paged {
        index_bytes: u64,
    },
}

struct Owner {
    descriptor: SegmentDescriptor,
    layout: Layout,
}

impl NativeStorage {
    pub(super) fn estimate_recovery_bytes(&self, work: &MaintenanceWork) -> io::Result<u64> {
        // Verification precedes space estimation as well as ordinary recovery.
        // A retained WAL alone never authorizes replacing committed rows.
        self.verify_recovery_evidence()?;
        let applied = match &work.journal {
            Some(journal) => self.verify_wal_transaction(journal, &work.rows)?.len(),
            None if work.rows.is_empty() => 0,
            None => {
                self.reject_ambiguous_legacy_wal(&work.rows)?;
                0
            }
        };
        let remaining = &work.rows[applied..];
        let mut total = 2 * super::super::catalog::MAX_CATALOG_BYTES
            + 2 * super::super::recovery::MAX_JOURNAL_BYTES;

        for descriptor in &self.catalog.segments {
            let dir = self.paths.segment_dir(descriptor.id);
            if let Some(reference) = &descriptor.column_bundle {
                let pinned = pinned_manifest(descriptor);
                let previous =
                    SegmentManifest::load(&self.paths.segment_manifest_path(descriptor.id));
                let changed = match previous {
                    Ok(previous) => previous.as_ref() != Some(&pinned),
                    Err(error) if error.is_invalid_metadata() => true,
                    Err(error) => return Err(error.into()),
                };
                let tail = fs::metadata(crate::column_artifact::bundle_path(
                    &dir,
                    descriptor.generation,
                ))?
                .len()
                    != reference.end()?;
                if changed || tail {
                    add(&mut total, manifest_growth(&pinned)?)?;
                }
            } else {
                let active = self.catalog.active_hot_segment == Some(descriptor.id)
                    || self.catalog.active_historical_segment == Some(descriptor.id);
                let pending = descriptor
                    .source_namespace
                    .map(|namespace| {
                        crate::column::prefix_rewrite_pending(
                            &dir,
                            namespace.0,
                            descriptor.row_count,
                            descriptor.generation,
                            descriptor.id,
                            descriptor.kind,
                            descriptor.source_commitment,
                        )
                    })
                    .transpose()?
                    .unwrap_or(false);
                if active || pending {
                    let (rows, _) = self.recovery_prefix(descriptor)?;
                    add(
                        &mut total,
                        ColumnFile::estimate_prefix_rewrite_bytes(
                            descriptor.row_count,
                            payload(&rows)?,
                        )?,
                    )?;
                    add(
                        &mut total,
                        manifest_growth(&segment::manifest_with_columns(
                            descriptor,
                            segment::default_columns(),
                        ))?,
                    )?;
                }
            }
        }

        // Startup creates a missing hot owner even when a retained incomplete
        // WAL tail contains no complete rows. Carry that allocation forward so
        // later historical/live rotation sees the same available ID range.
        let mut allocation = self.catalog.clone();
        let startup_hot = if self.catalog.active_hot_segment().is_none() {
            Some(fresh_owner(&mut allocation, SegmentKind::Hot, &mut total)?)
        } else {
            None
        };
        if !remaining.is_empty() {
            match work
                .journal
                .as_ref()
                .map_or(IngestRoute::Live, RecoveryJournal::route)
            {
                IngestRoute::Live => {
                    self.size_live_replay(remaining, &mut allocation, startup_hot, &mut total)?
                }
                IngestRoute::Historical => {
                    self.size_historical_replay(remaining, &mut allocation, &mut total)?
                }
            }
        }
        self.size_pending_reorg(&mut total)?;
        Ok(total)
    }

    /// Catalog-selected rows, including interrupted raw publications. Zero-row
    /// prefixes require no source initialization or file creation while sizing.
    fn recovery_prefix(
        &self,
        descriptor: &SegmentDescriptor,
    ) -> io::Result<(Vec<LogRow>, NullBitmap)> {
        if descriptor.row_count == 0 {
            return Ok((Vec::new(), NullBitmap::new()));
        }
        let dir = self.paths.segment_dir(descriptor.id);
        let owner = if descriptor.column_bundle.is_none() {
            descriptor
                .source_namespace
                .map(|namespace| {
                    crate::column::begin_prefix_recovery(
                        &dir,
                        namespace.0,
                        descriptor.row_count,
                        descriptor.generation,
                        descriptor.id,
                        descriptor.kind,
                        descriptor.source_commitment,
                    )
                })
                .transpose()?
        } else {
            None
        };
        let reader = if descriptor.column_bundle.is_some() {
            SegmentReader::open_for_inspection_manifest(&dir, pinned_manifest(descriptor))?
        } else if let Some(owner) = &owner {
            SegmentReader::open_recovering_prefix(owner)?
        } else {
            SegmentReader::open_for_inspection(&dir)?
        };
        let count = u32::try_from(descriptor.row_count).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "recovery prefix exceeds row addressing",
            )
        })?;
        let flags = reader.read_canonical()?;
        if flags.len() < descriptor.row_count {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "recovery prefix is missing canonical flags",
            ));
        }
        let mut canonical = NullBitmap::new();
        for row in 0..descriptor.row_count {
            canonical.push(flags.is_present(row));
        }
        let mut rows = Vec::new();
        let mut first = 0u32;
        while first < count {
            let end = first.saturating_add(8192).min(count);
            let ids: Vec<_> = (first..end).collect();
            rows.extend(reader.read_log_rows(Some(&ids))?);
            first = end;
        }
        if rows.len() as u64 != descriptor.row_count {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "recovery prefix has an incomplete row selection",
            ));
        }
        Ok((rows, canonical))
    }

    fn existing_owner(
        &self,
        descriptor: &SegmentDescriptor,
        historical: bool,
        total: &mut u64,
    ) -> io::Result<Owner> {
        let dir = self.paths.segment_dir(descriptor.id);
        let layout = if descriptor.column_bundle.is_some() {
            Layout::Bundle(Box::new(BundleAppendSizer::from_existing(
                &dir,
                &pinned_manifest(descriptor),
            )?))
        } else {
            let (rows, canonical) = self.recovery_prefix(descriptor)?;
            if historical && !rows.is_empty() {
                // Conversion may rewrite existing legacy pages after rollback.
                // Charge a complete current encoding even when conversion is
                // unnecessary; do not credit its old representation as freed.
                add(
                    total,
                    segment::estimate_repair_bundle_bytes(&rows, &canonical)?,
                )?;
                add(
                    total,
                    ColumnFile::estimate_raw_bitmap_publication_bytes(descriptor.row_count)?,
                )?;
                let mut index_bytes = index_bytes_for_rows(descriptor.row_count)?;
                if let Some(manifest) =
                    SegmentManifest::load(&self.paths.segment_manifest_path(descriptor.id))
                        .map_err(io::Error::from)?
                {
                    let retained = manifest
                        .columns
                        .iter()
                        .filter_map(|column| column.page_index_path.as_ref())
                        .try_fold(0u64, |total, relative| {
                            let length = fs::metadata(dir.join(relative))?.len();
                            total.checked_add(length).ok_or_else(|| {
                                io::Error::new(
                                    io::ErrorKind::InvalidInput,
                                    "legacy index size overflows",
                                )
                            })
                        })?;
                    index_bytes = index_bytes.max(retained);
                }
                add(total, index_bytes)?;
                add(total, manifest_growth(&pinned_manifest(descriptor))?)?;
                Layout::Paged { index_bytes }
            } else {
                Layout::Raw {
                    payload: payload(&rows)?,
                }
            }
        };
        Ok(Owner {
            descriptor: descriptor.clone(),
            layout,
        })
    }

    fn size_live_replay(
        &self,
        rows: &[LogRow],
        allocation: &mut NativeStorageCatalog,
        startup_hot: Option<Owner>,
        total: &mut u64,
    ) -> io::Result<()> {
        let mut owner = startup_hot.or(self
            .catalog
            .active_hot_segment()
            .map(|descriptor| self.existing_owner(descriptor, false, total))
            .transpose()?);
        let target = self.config.hot_target_rows.max(1);
        let mut offset = 0;
        while offset < rows.len() {
            if owner.is_none() {
                owner = Some(fresh_owner(allocation, SegmentKind::Hot, total)?);
            }
            let active = owner.as_mut().unwrap();
            let remaining = target.saturating_sub(active.descriptor.row_count);
            if remaining == 0 {
                finalize(active, total)?;
                owner = None;
                continue;
            }
            let count = remaining.min((rows.len() - offset) as u64) as usize;
            let count = match &active.layout {
                Layout::Bundle(bundle) => bundle.capacity(&rows[offset..offset + count])?,
                _ => count,
            };
            if count == 0 {
                if active.descriptor.row_count == 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "row exceeds recovery bundle capacity",
                    ));
                }
                finalize(active, total)?;
                owner = None;
                continue;
            }
            append(active, &rows[offset..offset + count], total)?;
            offset += count;
            if active.descriptor.row_count >= target {
                finalize(active, total)?;
                // Production eagerly publishes an empty next hot owner.
                owner = Some(fresh_owner(allocation, SegmentKind::Hot, total)?);
            }
        }
        Ok(())
    }

    fn size_historical_replay(
        &self,
        rows: &[LogRow],
        allocation: &mut NativeStorageCatalog,
        total: &mut u64,
    ) -> io::Result<()> {
        let mut owner = self
            .catalog
            .active_historical_segment
            .and_then(|id| {
                self.catalog
                    .segments
                    .iter()
                    .find(|segment| segment.id == id)
            })
            .map(|descriptor| self.existing_owner(descriptor, true, total))
            .transpose()?;
        let target =
            usize::try_from(self.config.hot_target_rows.max(1)).map_err(io::Error::other)?;
        let mut offset = 0;
        if rows.len() >= dense_historical_batch_row_threshold(target) {
            if let Some(active) = &mut owner {
                finalize(active, total)?;
            }
            owner = None;
            let end = compacted_historical_row_prefix_len(rows.len(), target);
            while offset < end {
                let mut fresh = fresh_owner(allocation, SegmentKind::Sealed, total)?;
                let candidate = &rows[offset..end.min(offset.saturating_add(target))];
                let empty = BundleAppendSizer::from_rows(&[], &NullBitmap::new())?.0;
                let take = empty.capacity(candidate)?;
                if take == 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "row exceeds recovery bundle capacity",
                    ));
                }
                begin_bundle(&mut fresh, &candidate[..take], total)?;
                offset += take;
            }
        }
        while offset < rows.len() {
            if owner.is_none() {
                owner = Some(fresh_owner(allocation, SegmentKind::Sealed, total)?);
            }
            let active = owner.as_mut().unwrap();
            let capacity = target.saturating_sub(active.descriptor.row_count as usize);
            if capacity == 0 {
                finalize(active, total)?;
                owner = None;
                continue;
            }
            let candidate = &rows[offset..rows.len().min(offset.saturating_add(capacity))];
            let take = match &active.layout {
                Layout::Bundle(bundle) => bundle.capacity(candidate)?,
                _ if active.descriptor.row_count == 0 => {
                    BundleAppendSizer::from_rows(&[], &NullBitmap::new())?
                        .0
                        .capacity(candidate)?
                }
                _ => candidate.len(),
            };
            if take == 0 {
                if active.descriptor.row_count == 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "row exceeds recovery bundle capacity",
                    ));
                }
                finalize(active, total)?;
                owner = None;
                continue;
            }
            let chunk = &candidate[..take];
            if active.descriptor.row_count > 0
                && RowBounds::from_rows(chunk).is_some_and(|bounds| {
                    historical_segment_would_exceed_block_span(&active.descriptor, bounds)
                })
            {
                finalize(active, total)?;
                owner = None;
                continue;
            }
            if active.descriptor.row_count == 0 {
                begin_bundle(active, chunk, total)?;
            } else {
                append(active, chunk, total)?;
            }
            offset += take;
            if active.descriptor.row_count >= target as u64
                || historical_segment_block_span(&active.descriptor)
                    .is_some_and(|span| span >= HISTORICAL_STAGING_MAX_BLOCK_SPAN)
            {
                finalize(active, total)?;
                owner = None;
            }
        }
        Ok(())
    }

    fn size_pending_reorg(&self, total: &mut u64) -> io::Result<()> {
        let Some(intent) = &self.catalog.state.canonical_reorg else {
            return Ok(());
        };
        let retired: BTreeSet<_> = self.catalog.state.recent_headers
            [intent.retained_header_count..]
            .iter()
            .map(Header::hash_slow)
            .collect();
        for descriptor in &self.catalog.segments {
            if descriptor.row_count == 0 {
                continue;
            }
            let (rows, mut canonical) = self.recovery_prefix(descriptor)?;
            let mut changed = false;
            for (index, row) in rows.iter().enumerate() {
                if canonical.is_present(index as u64) && retired.contains(&row.block_hash) {
                    canonical.set(index as u64, false);
                    changed = true;
                }
            }
            if !changed {
                continue;
            }
            if descriptor.column_bundle.is_some() {
                let mut bundle = BundleAppendSizer::from_existing(
                    &self.paths.segment_dir(descriptor.id),
                    &pinned_manifest(descriptor),
                )?;
                add(total, bundle.rewrite_canonical(&canonical)?)?;
                add(total, manifest_growth(&pinned_manifest(descriptor))?)?;
            } else {
                add(
                    total,
                    ColumnFile::estimate_raw_bitmap_publication_bytes(descriptor.row_count)?,
                )?;
            }
        }
        Ok(())
    }
}

fn fresh_owner(
    catalog: &mut NativeStorageCatalog,
    kind: SegmentKind,
    total: &mut u64,
) -> io::Result<Owner> {
    let descriptor = catalog.allocate_segment(kind)?;
    add(
        total,
        manifest_growth(&segment::manifest_with_columns(
            &descriptor,
            segment::default_columns(),
        ))?,
    )?;
    Ok(Owner {
        descriptor,
        layout: Layout::Raw { payload: 0 },
    })
}

fn begin_bundle(owner: &mut Owner, rows: &[LogRow], total: &mut u64) -> io::Result<()> {
    let (bundle, bytes) = BundleAppendSizer::from_rows(rows, &present(rows.len()))?;
    add(total, bytes)?;
    owner.layout = Layout::Bundle(Box::new(bundle));
    apply_rows_to_descriptor(&mut owner.descriptor, rows);
    add(total, manifest_growth(&pinned_manifest(&owner.descriptor))?)
}

fn append(owner: &mut Owner, rows: &[LogRow], total: &mut u64) -> io::Result<()> {
    let old_rows = owner.descriptor.row_count;
    let new_rows = old_rows.checked_add(rows.len() as u64).ok_or_else(|| {
        io::Error::new(io::ErrorKind::InvalidInput, "recovery row count overflows")
    })?;
    match &mut owner.layout {
        Layout::Raw { payload: bytes } => {
            add(bytes, payload(rows)?)?;
            add(
                total,
                ColumnFile::estimate_append_growth_bytes(old_rows, rows.len() as u64, *bytes)?,
            )?;
        }
        Layout::Bundle(bundle) => add(total, bundle.append(rows)?)?,
        Layout::Paged { index_bytes } => {
            add(
                total,
                segment::estimate_repair_bundle_bytes(rows, &present(rows.len()))?,
            )?;
            add(index_bytes, page_index_entries_bytes(rows.len() as u64)?)?;
            add(total, *index_bytes)?;
            add(
                total,
                ColumnFile::estimate_raw_bitmap_publication_bytes(new_rows)?,
            )?;
        }
    }
    apply_rows_to_descriptor(&mut owner.descriptor, rows);
    let columns = if matches!(owner.layout, Layout::Raw { .. }) {
        segment::default_columns()
    } else {
        segment::current_compacted_columns()
    };
    add(
        total,
        manifest_growth(&segment::manifest_with_columns(&owner.descriptor, columns))?,
    )
}

fn finalize(owner: &mut Owner, total: &mut u64) -> io::Result<()> {
    owner.descriptor.kind = SegmentKind::Sealed;
    add(total, manifest_growth(&pinned_manifest(&owner.descriptor))?)
}

fn pinned_manifest(descriptor: &SegmentDescriptor) -> SegmentManifest {
    segment::manifest_with_columns(descriptor, segment::current_compacted_columns())
}

fn manifest_growth(manifest: &SegmentManifest) -> io::Result<u64> {
    // Bound numeric widths and optional identity/bundle fields before virtual
    // appends have their physical offsets or publication hashes. Historical
    // compaction may retain or create a rewrite directory rather than use the
    // standard column paths synthesized by sizing. Its admitted namespace has
    // an eight-digit process ID and sixteen-digit sequence followed by /columns
    // (see segment::is_rewrite_column_dir). Widen every path to cover it, while
    // retaining any longer supplied path. Pretty JSON bounds compact publication.
    let mut bound = manifest.clone();
    for column in &mut bound.columns {
        for path in std::iter::once(&mut column.data_path)
            .chain(column.page_index_path.iter_mut())
            .chain(column.null_bitmap_path.iter_mut())
        {
            let filename = Path::new(&*path)
                .file_name()
                .and_then(|name| name.to_str())
                .ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        "recovery column path has no filename",
                    )
                })?;
            let longest = format!(
                "columns_rewrite_{:08x}_{:016x}/columns/{filename}",
                u32::MAX,
                u64::MAX
            );
            if longest.len() > path.len() {
                *path = longest;
            }
        }
    }
    bound.segment_id = u64::MAX;
    bound.generation = u64::MAX;
    bound.row_count = u64::MAX;
    bound.source_namespace = Some(alloy_primitives::FixedBytes::repeat_byte(255));
    bound.source_commitment = Some(B256::repeat_byte(255));
    bound.min_block = Some(u64::MAX);
    bound.max_block = Some(u64::MAX);
    bound.min_timestamp = Some(u64::MAX);
    bound.max_timestamp = Some(u64::MAX);
    bound.column_bundle = Some(crate::BundleReference {
        sequence: u64::MAX,
        row_count: u64::MAX,
        table_offset: u64::MAX,
        table_len: u32::MAX,
        checksum: u32::MAX,
        depth: u32::MAX,
        chain_bytes: u32::MAX,
    });
    let bytes = serde_json::to_vec_pretty(&bound).map_err(io::Error::other)?;
    u64::try_from(bytes.len())
        .ok()
        .and_then(|length| length.checked_mul(2))
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "recovery manifest size overflows",
            )
        })
}

fn page_index_entries_bytes(rows: u64) -> io::Result<u64> {
    rows.div_ceil(u64::from(crate::page::MAX_PAGE_ROWS))
        .checked_mul(crate::page::PAGE_INDEX_ENTRY_BYTES as u64)
        .and_then(|bytes| bytes.checked_mul(crate::column_artifact::COLUMN_NAMES.len() as u64))
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "recovery page index size overflows",
            )
        })
}
fn index_bytes_for_rows(rows: u64) -> io::Result<u64> {
    // A complete framed empty index for each current column plus its entries.
    let empty = crate::page::write_page_index(&[]);
    page_index_entries_bytes(rows)?
        .checked_add((empty.len() * crate::column_artifact::COLUMN_NAMES.len()) as u64)
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "recovery page index size overflows",
            )
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::{Address, Bytes};
    use std::collections::BTreeMap;

    fn rows(start: usize, count: usize) -> Vec<LogRow> {
        (start..start + count)
            .map(|index| LogRow {
                block_number: 100 + (index / 1024) as u64,
                block_hash: B256::repeat_byte((index / 1024) as u8),
                timestamp: 1000 + index as u64,
                tx_hash: B256::repeat_byte(index as u8),
                tx_index: index as u32,
                log_index: index as u32,
                address: Address::repeat_byte(index as u8),
                topic0: (index % 2 == 0).then_some(B256::repeat_byte(1)),
                topic1: None,
                topic2: (index % 3 == 0).then_some(B256::repeat_byte(2)),
                topic3: None,
                data: Bytes::from(vec![index as u8; index % 37]),
                data_len: (index % 37) as u32,
                source: logex_types::Source::Receipt,
            })
            .collect()
    }

    fn files(root: &Path) -> BTreeMap<PathBuf, u64> {
        fn visit(root: &Path, dir: &Path, out: &mut BTreeMap<PathBuf, u64>) {
            for entry in fs::read_dir(dir).unwrap() {
                let entry = entry.unwrap();
                let metadata = entry.metadata().unwrap();
                if metadata.is_dir() {
                    visit(root, &entry.path(), out);
                } else {
                    assert!(metadata.is_file());
                    out.insert(
                        entry.path().strip_prefix(root).unwrap().to_owned(),
                        metadata.len(),
                    );
                }
            }
        }
        let mut result = BTreeMap::new();
        visit(root, root, &mut result);
        result
    }

    // Page payloads grow in place. Page indexes, bitmaps and manifests are
    // replaced atomically: charge the WHOLE resulting file, not merely its delta.
    fn publication_bytes(before: &BTreeMap<PathBuf, u64>, after: &BTreeMap<PathBuf, u64>) -> u64 {
        after
            .iter()
            .map(|(path, size)| {
                if path
                    .extension()
                    .is_some_and(|extension| extension == "pages")
                {
                    size.saturating_sub(*before.get(path).unwrap_or(&0))
                } else if path
                    .extension()
                    .is_some_and(|extension| extension == "idx" || extension == "null")
                    || path == Path::new("canonical.bitmap")
                    || path == Path::new("segment.json")
                {
                    *size
                } else {
                    size.saturating_sub(*before.get(path).unwrap_or(&0))
                }
            })
            .sum()
    }

    #[test]
    fn legacy_paged_layout_bounds_cover_conversion_and_repeated_append_without_global_reserves() {
        let tmp = tempfile::tempdir().unwrap();
        let mut storage = NativeStorage::open(NativeStorageConfig {
            data_dir: tmp.path().to_owned(),
            hot_target_rows: 1_000_000,
            ..Default::default()
        })
        .unwrap();
        let id = storage.ensure_active_historical_segment().unwrap();
        let index = storage
            .catalog
            .segments
            .iter()
            .position(|entry| entry.id == id)
            .unwrap();
        let mut descriptor = storage.catalog.segments[index].clone();
        let initial = rows(0, crate::page::MAX_PAGE_ROWS as usize + 3);
        let revision =
            crate::commitment::AppendRevision::new(descriptor.source_state.as_ref(), &initial)
                .unwrap();
        let identity = descriptor
            .source_namespace
            .map(|namespace| crate::column::SourceIdentity {
                namespace: namespace.0,
                generation: descriptor.generation,
                segment_id: id,
                kind: descriptor.kind,
            });
        let dir = storage.segment_path(id);
        append_ingest_rows(
            &dir,
            0,
            &initial,
            durability::Publication::Durable,
            identity,
            &revision,
        )
        .unwrap();
        apply_rows_to_descriptor(&mut descriptor, &initial);
        descriptor.source_commitment = revision.next;
        descriptor.source_state = revision.state;
        persist_ingest_manifest(
            &storage.paths,
            &descriptor,
            durability::Publication::Durable,
        )
        .unwrap();
        storage.catalog.segments[index] = descriptor.clone();
        storage.persist_catalog().unwrap();

        let before = files(&dir);
        let mut conversion = 0;
        let simulated = storage
            .existing_owner(&descriptor, true, &mut conversion)
            .unwrap();
        assert!(matches!(simulated.layout, Layout::Paged { .. }));
        assert_eq!(files(&dir), before, "conversion sizing wrote source files");
        compact_ingest_segment(
            &storage.paths,
            &descriptor,
            durability::Publication::Durable,
        )
        .unwrap();
        let actual = publication_bytes(&before, &files(&dir));
        assert!(
            actual > 0 && actual <= conversion,
            "conversion: actual={actual}, bound={conversion}"
        );

        // Resume a supported legacy layout whose paths already use the longest
        // admitted rewrite namespace, rather than only the standard columns/.
        let rewrite = "columns_rewrite_ffffffff_ffffffffffffffff";
        fs::create_dir(dir.join(rewrite)).unwrap();
        fs::rename(dir.join("columns"), dir.join(rewrite).join("columns")).unwrap();
        let mut manifest = SegmentManifest::load(&storage.paths.segment_manifest_path(id))
            .unwrap()
            .unwrap();
        for column in &mut manifest.columns {
            column.data_path = format!("{rewrite}/{}", column.data_path);
            if let Some(path) = &mut column.page_index_path {
                *path = format!("{rewrite}/{path}");
            }
            if let Some(path) = &mut column.null_bitmap_path {
                *path = format!("{rewrite}/{path}");
            }
        }
        segment::persist_segment_manifest_with_columns(
            &storage.paths,
            &descriptor,
            manifest.columns,
        )
        .unwrap();
        let mut ignored_conversion = 0;
        let mut simulated = storage
            .existing_owner(&descriptor, true, &mut ignored_conversion)
            .unwrap();
        for count in [crate::page::MAX_PAGE_ROWS as usize + 5, 19] {
            let appended = rows(descriptor.row_count as usize, count);
            let before = files(&dir);
            let mut allowance = 0;
            append(&mut simulated, &appended, &mut allowance).unwrap();
            assert_eq!(files(&dir), before, "append sizing wrote source files");
            let revision =
                crate::commitment::AppendRevision::new(descriptor.source_state.as_ref(), &appended)
                    .unwrap();
            let encoded = segment::append_compacted_rows_with_revision(
                &dir,
                descriptor.row_count,
                &appended,
                durability::Publication::Durable,
                None,
                &revision,
            )
            .unwrap();
            apply_rows_to_descriptor(&mut descriptor, &appended);
            descriptor.source_commitment = revision.next;
            descriptor.source_state = revision.state;
            let columns = encoded.apply_to(&mut descriptor);
            persist_ingest_manifest_with_columns(
                &storage.paths,
                &descriptor,
                columns,
                durability::Publication::Durable,
            )
            .unwrap();
            let actual = publication_bytes(&before, &files(&dir));
            assert!(
                actual > 0 && actual <= allowance,
                "append {count}: actual={actual}, bound={allowance}"
            );
        }
    }
}
