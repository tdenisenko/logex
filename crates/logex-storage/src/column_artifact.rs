//! Resolve the fixed logical column schema through one captured bundle table.
use std::fs;
use std::io;
use std::ops::Range;
use std::path::{Path, PathBuf};

use crate::bundle::BundleReader;
use crate::native::{STORAGE_FORMAT_VERSION, SegmentKind, SegmentManifest};

pub(crate) const BUNDLE_PATH: &str = "columns/segment.bundle";
pub(crate) const COLUMN_NAMES: [&str; 14] = [
    "address",
    "block_number",
    "block_hash",
    "timestamp",
    "tx_hash",
    "tx_index",
    "log_index",
    "data_len",
    "source",
    "topic0",
    "topic1",
    "topic2",
    "topic3",
    "data",
];

pub(crate) fn stream_id(path: &str) -> io::Result<u8> {
    let path = path
        .strip_prefix("columns/")
        .ok_or_else(|| invalid("invalid bundle directory"))?;
    let (name, offset) = if let Some(name) = path.strip_suffix(".pages.idx") {
        (name, 14)
    } else if let Some(name) = path.strip_suffix(".pages") {
        (name, 0)
    } else if let Some(name) = path.strip_suffix(".null") {
        (name, 19)
    } else {
        return Err(invalid("unknown bundled column artifact"));
    };
    let id = COLUMN_NAMES
        .iter()
        .position(|candidate| *candidate == name)
        .ok_or_else(|| invalid("unknown bundled column"))?;
    if offset == 19 && !(9..13).contains(&id) {
        return Err(invalid("nonnullable bundled column"));
    }
    Ok(id as u8 + offset)
}

#[derive(Debug, Clone)]
pub(crate) struct ColumnArtifacts {
    dir: PathBuf,
    bundle: Option<BundleReader>,
}

impl ColumnArtifacts {
    pub(crate) fn open(dir: &Path, manifest: Option<&SegmentManifest>) -> io::Result<Self> {
        let bundle = manifest
            .and_then(|manifest| {
                manifest
                    .column_bundle
                    .as_ref()
                    .map(|reference| (manifest, reference))
            })
            .map(|(manifest, reference)| {
                if manifest.format_version != STORAGE_FORMAT_VERSION
                    || manifest.kind != SegmentKind::Sealed
                    || manifest.row_count != reference.row_count
                    || manifest.canonical_rows_path != "canonical.bitmap"
                    || manifest.columns.len() != COLUMN_NAMES.len()
                {
                    return Err(invalid("invalid bundled segment identity or schema"));
                }
                for (name, codec) in crate::native::current_column_profile() {
                    let column = manifest
                        .columns
                        .iter()
                        .find(|column| column.name == *name)
                        .ok_or_else(|| invalid("missing bundled column"))?;
                    let topic = matches!(*name, "topic0" | "topic1" | "topic2" | "topic3");
                    if column.codec != *codec
                        || column.page_rows != 16_384
                        || column.data_path != format!("columns/{name}.pages")
                        || column.page_index_path != Some(format!("columns/{name}.pages.idx"))
                        || column.null_bitmap_path != topic.then(|| format!("columns/{name}.null"))
                    {
                        return Err(invalid("invalid bundled column descriptor"));
                    }
                }
                let reader = BundleReader::open(&dir.join(BUNDLE_PATH), reference)?;
                if !reader.has_complete_schema() {
                    return Err(invalid("incomplete bundled column streams"));
                }
                Ok(reader)
            })
            .transpose()?;
        Ok(Self {
            dir: dir.to_owned(),
            bundle,
        })
    }

    pub(crate) fn read(&self, path: &str) -> io::Result<Vec<u8>> {
        match &self.bundle {
            Some(bundle) => bundle.read_stream(stream_id(path)?),
            None => fs::read(self.dir.join(path)),
        }
    }

    pub(crate) fn len(&self, path: &str) -> io::Result<u64> {
        match &self.bundle {
            Some(bundle) => bundle.stream_len(stream_id(path)?),
            None => Ok(fs::metadata(self.dir.join(path))?.len()),
        }
    }

    pub(crate) fn read_bundle_range(
        &self,
        path: &str,
        range: Range<u64>,
    ) -> Option<io::Result<Vec<u8>>> {
        self.bundle
            .as_ref()
            .map(|bundle| stream_id(path).and_then(|id| bundle.read_range(id, range)))
    }

    pub(crate) fn verify_bundle(&self) -> io::Result<()> {
        self.bundle
            .as_ref()
            .map_or(Ok(()), BundleReader::verify_all)
    }
}

fn invalid(reason: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, reason)
}
