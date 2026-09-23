//! Offline row routing hints and required local carry-forward. Exact final
//! commitment verification is mandatory; these inputs are not publication proof.
use super::*;
use alloy_primitives::B256;
use logex_types::Source;

/// Per-owner read/work allowances, not total process or network memory limits.
#[derive(Debug, Clone, Copy)]
pub struct RepairReadLimits {
    /// Captured routing artifacts, indexes and shared metadata (raw includes canonical).
    pub max_routing_artifact_bytes: u64,
    pub max_carry_artifact_bytes: u64,
    pub max_carry_decoded_payload_bytes: u64,
    /// Actual data bytes retained in preserved rows.
    pub max_carry_data_bytes: u64,
}

/// Unverified original-order routing. `None` is a canonical Receipt slot inside
/// the frozen fetch union. `Some` must be carried without normalization.
#[derive(Debug)]
pub struct RepairRowInput {
    pub block_number: u64,
    pub block_hash: B256,
    pub log_index: u32,
    pub preserved: Option<LogRow>,
}

fn context(stage: &str, id: u64, error: io::Error) -> io::Error {
    io::Error::new(
        error.kind(),
        format!("repair segment {id} {stage}: {error}"),
    )
}

impl RepairOwnershipPlan {
    /// Prepare original-order hints while retaining exclusive inspection ownership.
    /// Required carry rows use the existing full-source preflight: whole raw
    /// buffers and selected compressed pages may include replaceable rows. Damage
    /// there can block preparation; no selective raw payload-read claim is made.
    pub fn prepare_candidate(
        &self,
        segment_id: u64,
        limits: RepairReadLimits,
    ) -> io::Result<(RepairCandidateVerifier<'_>, Vec<RepairRowInput>)> {
        let verifier = self.begin_candidate(segment_id)?;
        let descriptor = &self.catalog().segments[verifier.index];
        if descriptor.row_count == 0 {
            return Ok((verifier, Vec::new()));
        }
        let dir = self.inspection.paths.segment_dir(segment_id);
        let mut reader = SegmentReader::open_for_inspection_projected(
            &dir,
            Some(&["block_number", "block_hash", "log_index", "source"]),
        )
        .map_err(|e| context("capture routing", segment_id, e))?;
        inspection::verify_identity(&reader, descriptor)?;
        reader
            .repair_routing_preflight(limits.max_routing_artifact_bytes)
            .map_err(|e| context("routing preflight", segment_id, e))?;
        let numbers = reader.read_u64("block_number", None)?;
        let hashes = reader.read_b256("block_hash", None)?;
        let indexes = reader.read_u32("log_index", None)?;
        let sources = reader.read_u8("source", None)?;
        let count = usize::try_from(descriptor.row_count).map_err(io::Error::other)?;
        if [numbers.len(), hashes.len(), indexes.len(), sources.len()]
            .iter()
            .any(|&len| len != count)
        {
            return Err(invalid(
                "routing column count differs from original segment",
            ));
        }
        let mut inputs = Vec::new();
        inputs.try_reserve_exact(count).map_err(io::Error::other)?;
        let mut carry = Vec::new();
        for row in 0..count {
            let source = Source::from_u8(sources[row])
                .ok_or_else(|| invalid("invalid original routing source tag"))?;
            let next = self
                .block_ranges()
                .partition_point(|&(_, end)| end < numbers[row]);
            let in_range = self
                .block_ranges()
                .get(next)
                .is_some_and(|&(start, _)| start <= numbers[row]);
            if !in_range || source != Source::Receipt || !verifier.canonical.is_present(row as u64)
            {
                carry.push(u32::try_from(row).map_err(io::Error::other)?);
            }
            inputs.push(RepairRowInput {
                block_number: numbers[row],
                block_hash: hashes[row],
                log_index: indexes[row],
                preserved: None,
            });
        }
        drop(reader);
        // Routing is now retained in inputs. Release redundant fixed-column
        // vectors before materializing any required payload rows.
        drop((numbers, hashes, indexes));
        if !carry.is_empty() {
            let mut reader = SegmentReader::open_for_inspection(&dir)
                .map_err(|e| context("capture required carry", segment_id, e))?;
            inspection::verify_identity(&reader, descriptor)?;
            reader
                .inspection_preflight(
                    limits.max_carry_artifact_bytes,
                    limits.max_carry_decoded_payload_bytes,
                )
                .map_err(|e| {
                    let error = match e {
                        crate::segment_reader::InspectionPreflightError::Io(error) => error,
                        crate::segment_reader::InspectionPreflightError::LimitExceeded {
                            resource,
                            required,
                            limit,
                        } => io::Error::new(
                            io::ErrorKind::InvalidInput,
                            format!("{resource} requires {required} bytes; limit is {limit}"),
                        ),
                    };
                    context("carry preflight", segment_id, error)
                })?;
            let mut position = 0usize;
            let mut bytes = 0u64;
            for batch in reader.log_row_batches(&carry)? {
                for original in batch.map_err(|e| context("read required carry", segment_id, e))? {
                    let row = *carry
                        .get(position)
                        .ok_or_else(|| invalid("extra carry row"))?
                        as usize;
                    let input = &mut inputs[row];
                    if original.block_number != input.block_number
                        || original.block_hash != input.block_hash
                        || original.log_index != input.log_index
                        || original.source as u8 != sources[row]
                    {
                        return Err(invalid("carried row differs from captured routing"));
                    }
                    bytes = bytes
                        .checked_add(u64::try_from(original.data.len()).map_err(io::Error::other)?)
                        .filter(|&total| total <= limits.max_carry_data_bytes)
                        .ok_or_else(|| {
                            io::Error::new(
                                io::ErrorKind::InvalidInput,
                                "preserved row data exceeds carry budget",
                            )
                        })?;
                    input.preserved = Some(original);
                    position += 1;
                }
            }
            if position != carry.len() {
                return Err(invalid("missing required carry rows"));
            }
        }
        Ok((verifier, inputs))
    }
}

#[cfg(test)]
mod tests;
