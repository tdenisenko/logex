use std::ops::Index;

use datafusion::error::{DataFusionError, Result};
use logex_types::{QueryMemoryBudget, QueryMemoryError, QueryMemoryReservation};
use serde::Serialize;
use serde_json::Value;

pub(crate) const SQL_RESULT_STAGE: &str = "structured SQL result";

/// Structured query rows whose memory charge cannot be detached from the values.
#[derive(Debug, Serialize)]
#[serde(transparent)]
pub struct QueryJsonRows {
    rows: Vec<Value>,
    #[serde(skip)]
    _reservation: QueryMemoryReservation,
}

impl QueryJsonRows {
    pub fn len(&self) -> usize {
        self.rows.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    pub fn iter(&self) -> std::slice::Iter<'_, Value> {
        self.rows.iter()
    }

    pub fn as_slice(&self) -> &[Value] {
        &self.rows
    }
}

impl Index<usize> for QueryJsonRows {
    type Output = Value;

    fn index(&self, index: usize) -> &Self::Output {
        &self.rows[index]
    }
}

impl PartialEq for QueryJsonRows {
    fn eq(&self, other: &Self) -> bool {
        self.rows == other.rows
    }
}

impl PartialEq<Vec<Value>> for QueryJsonRows {
    fn eq(&self, other: &Vec<Value>) -> bool {
        self.rows == *other
    }
}

impl PartialEq<QueryJsonRows> for Vec<Value> {
    fn eq(&self, other: &QueryJsonRows) -> bool {
        *self == other.rows
    }
}

impl PartialEq<[Value]> for QueryJsonRows {
    fn eq(&self, other: &[Value]) -> bool {
        self.rows == other
    }
}

/// Mutable construction owner. Allocations are declared before they are made and rows
/// always drop before the aggregate reservation.
pub(crate) struct JsonResultBuilder {
    rows: Vec<Value>,
    reservation: QueryMemoryReservation,
    memory: QueryMemoryBudget,
}

impl JsonResultBuilder {
    pub(crate) fn new(memory: QueryMemoryBudget) -> Result<Self> {
        Ok(Self {
            rows: Vec::new(),
            reservation: memory.reserve(0, SQL_RESULT_STAGE).map_err(memory_error)?,
            memory,
        })
    }

    /// Admit retained payload bytes computed by a checked preflight.
    pub(crate) fn reserve_payload(&mut self, bytes: usize) -> Result<()> {
        self.reservation.try_grow(bytes).map_err(memory_error)
    }

    /// Reconcile allocator capacity beyond the admitted request before another scalable
    /// allocation occurs. This is infallible accounting of memory already owned.
    pub(crate) fn record_existing(&mut self, bytes: usize) {
        if bytes != 0 {
            self.reservation.record_existing(bytes);
        }
    }

    /// Ensure `additional` rows can be appended. When Vec must replace its allocation,
    /// charge the complete requested replacement while the old allocation is still live,
    /// then release the old charge only after replacement succeeds.
    pub(crate) fn reserve_rows(&mut self, additional: usize) -> Result<()> {
        let required = self.rows.len().checked_add(additional).ok_or_else(|| {
            memory_error(QueryMemoryError::SizeOverflow {
                stage: SQL_RESULT_STAGE,
            })
        })?;
        if required <= self.rows.capacity() {
            return Ok(());
        }
        let old_bytes = value_slots(self.rows.capacity())?;
        let target = self
            .rows
            .capacity()
            .checked_mul(2)
            .unwrap_or(required)
            .max(required);
        let requested_bytes = value_slots(target)?;
        self.reservation
            .try_grow(requested_bytes)
            .map_err(memory_error)?;
        let mut replacement = Vec::new();
        replacement.try_reserve_exact(target).map_err(|error| {
            DataFusionError::ResourcesExhausted(format!(
                "cannot allocate structured SQL result rows: {error}"
            ))
        })?;
        let actual_bytes = value_slots(replacement.capacity())?;
        if actual_bytes > requested_bytes {
            let excess = actual_bytes - requested_bytes;
            let used = self.memory.used();
            self.reservation.record_existing(excess);
            if self.memory.used() > self.memory.limit() as u128 {
                return Err(memory_error(QueryMemoryError::CapacityExceeded {
                    requested: excess,
                    used,
                    limit: self.memory.limit(),
                    stage: SQL_RESULT_STAGE,
                }));
            }
        }
        replacement.append(&mut self.rows);
        let old = std::mem::replace(&mut self.rows, replacement);
        drop(old);
        if old_bytes != 0 {
            self.reservation
                .shrink(old_bytes)
                .expect("old SQL result row capacity remains charged during replacement");
        }
        Ok(())
    }

    pub(crate) fn push_precharged(&mut self, row: Value) {
        assert!(
            self.rows.len() < self.rows.capacity(),
            "SQL result row must be admitted before insertion"
        );
        self.rows.push(row);
    }

    pub(crate) fn begin_batch(
        &mut self,
        tree_preflight_bytes: usize,
        additional_rows: usize,
    ) -> Result<JsonBatchAppender<'_>> {
        self.reserve_rows(additional_rows)?;
        self.reserve_payload(tree_preflight_bytes)?;
        let start_len = self.rows.len();
        Ok(JsonBatchAppender {
            builder: self,
            start_len,
            batch_charge: tree_preflight_bytes,
            refund: 0,
            finished: false,
        })
    }

    pub(crate) fn finish(self) -> QueryJsonRows {
        QueryJsonRows {
            rows: self.rows,
            _reservation: self.reservation,
        }
    }
}

pub(crate) struct JsonBatchAppender<'a> {
    builder: &'a mut JsonResultBuilder,
    start_len: usize,
    batch_charge: usize,
    refund: usize,
    finished: bool,
}

impl JsonBatchAppender<'_> {
    /// Reconcile one allocation immediately after construction. Callers must create this
    /// appender before every temporary String/Vec/Value covered by the batch charge and
    /// must not move an unpushed temporary outside its scope. Rust then drops partial
    /// allocations before this guard truncates rows and refunds the batch on error/panic.
    pub(crate) fn observe_capacity(
        &mut self,
        requested_bytes: usize,
        actual_bytes: usize,
    ) -> Result<()> {
        if actual_bytes > requested_bytes {
            let excess = actual_bytes - requested_bytes;
            self.builder.record_existing(excess);
            self.batch_charge = self
                .batch_charge
                .checked_add(excess)
                .ok_or_else(size_overflow)?;
            let used = self.builder.memory.used();
            if used > self.builder.memory.limit() as u128 {
                return Err(memory_error(QueryMemoryError::CapacityExceeded {
                    requested: excess,
                    used,
                    limit: self.builder.memory.limit(),
                    stage: SQL_RESULT_STAGE,
                }));
            }
        } else {
            self.refund = self
                .refund
                .checked_add(requested_bytes - actual_bytes)
                .ok_or_else(size_overflow)?;
        }
        Ok(())
    }

    pub(crate) fn push_row(&mut self, row: Value) {
        self.builder.push_precharged(row);
    }

    pub(crate) fn finish(mut self) -> Result<()> {
        if self.refund != 0 {
            self.builder
                .reservation
                .shrink(self.refund)
                .map_err(memory_error)?;
            self.batch_charge -= self.refund;
            self.refund = 0;
        }
        self.finished = true;
        Ok(())
    }
}

impl Drop for JsonBatchAppender<'_> {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        self.builder.rows.truncate(self.start_len);
        self.builder
            .reservation
            .shrink(self.batch_charge)
            .expect("unfinished JSON batch charge matches its partial rows");
    }
}

fn value_slots(capacity: usize) -> Result<usize> {
    capacity
        .checked_mul(std::mem::size_of::<Value>())
        .ok_or_else(|| {
            memory_error(QueryMemoryError::SizeOverflow {
                stage: SQL_RESULT_STAGE,
            })
        })
}

fn memory_error(error: QueryMemoryError) -> DataFusionError {
    DataFusionError::External(Box::new(error))
}

/// Conservative allocation bytes for an insertion-only serde_json object with `entries`
/// unique keys on the pinned nightly-2026-08-24 standard library.
///
/// serde_json 1.0.149 uses BTreeMap when `preserve_order` is disabled. Rust commit
/// fb6531d550e0075b9eb9a51464f404805eec87d9 uses B=6: nodes hold 11 key/value pairs,
/// internal nodes add 12 child pointers, and every completed non-root node has at least
/// five keys. Up to 11 entries remain in one leaf; inserting the twelfth splits it into
/// two leaves under a new root. Insertions do not free nodes, so the completed tree also
/// bounds transient allocations: above that first split, one root plus at most
/// `(entries - 1) / 5` non-root nodes. Treating every node as the larger internal layout
/// is conservative.
pub(crate) fn json_object_node_bytes(entries: usize) -> Result<usize> {
    if entries == 0 {
        return Ok(0);
    }
    let nodes = if entries <= 11 {
        1
    } else {
        1usize
            .checked_add((entries - 1) / 5)
            .ok_or_else(size_overflow)?
    };
    internal_node_upper_bytes()?
        .checked_mul(nodes)
        .ok_or_else(size_overflow)
}

fn internal_node_upper_bytes() -> Result<usize> {
    use std::mem::{align_of, size_of};

    let max_align = align_of::<String>()
        .max(align_of::<Value>())
        .max(align_of::<usize>());
    let fields = [
        size_of::<usize>(),
        size_of::<u16>(),
        size_of::<u16>(),
        size_of::<[String; 11]>(),
        size_of::<[Value; 11]>(),
        size_of::<[usize; 12]>(),
    ];
    // LeafNode has Rust layout, so do not rely on source field order. Allow maximum
    // alignment padding before every field and at the end; this remains an upper bound
    // if rustc reorders the private fields while retaining the pinned field set.
    let padding = fields
        .len()
        .checked_mul(max_align - 1)
        .ok_or_else(size_overflow)?;
    fields
        .into_iter()
        .try_fold(padding, |total, bytes| total.checked_add(bytes))
        .ok_or_else(size_overflow)
}

fn size_overflow() -> DataFusionError {
    memory_error(QueryMemoryError::SizeOverflow {
        stage: SQL_RESULT_STAGE,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use logex_types::QueryMemoryLimit;

    #[test]
    fn object_node_allowance_tracks_pinned_split_boundaries() {
        let node = internal_node_upper_bytes().unwrap();
        assert_eq!(json_object_node_bytes(0).unwrap(), 0);
        for entries in 1..=11 {
            assert_eq!(json_object_node_bytes(entries).unwrap(), node);
        }
        assert_eq!(json_object_node_bytes(12).unwrap(), node * 3);
    }

    #[test]
    fn row_owner_holds_charge_until_final_alias_drops() {
        let memory = QueryMemoryBudget::new(QueryMemoryLimit::new(4096).unwrap());
        let mut builder = JsonResultBuilder::new(memory.clone()).unwrap();
        builder.reserve_rows(1).unwrap();
        builder.reserve_payload(32).unwrap();
        let mut value = String::with_capacity(32);
        value.push_str("retained");
        builder.push_precharged(Value::String(value));
        let rows = std::sync::Arc::new(builder.finish());
        let charged = memory.used();
        assert!(charged > 32);
        let alias = rows.clone();
        drop(rows);
        assert_eq!(memory.used(), charged);
        drop(alias);
        assert_eq!(memory.used(), 0);
    }

    #[test]
    fn unfinished_batch_drops_partial_rows_before_refunding_payload() {
        let memory = QueryMemoryBudget::new(QueryMemoryLimit::new(16 * 1024).unwrap());
        let mut builder = JsonResultBuilder::new(memory.clone()).unwrap();
        let outer_bytes = std::mem::size_of::<Value>();
        {
            let mut batch = builder.begin_batch(128, 1).unwrap();
            let mut value = String::with_capacity(128);
            value.push_str("partial");
            batch.push_row(Value::String(value));
        }
        assert_eq!(builder.rows.len(), 0);
        assert_eq!(memory.used(), outer_bytes as u128);
        drop(builder);
        assert_eq!(memory.used(), 0);
    }

    #[test]
    fn serialized_owner_preserves_btree_object_order_assumption() {
        let memory = QueryMemoryBudget::new(QueryMemoryLimit::new(16 * 1024).unwrap());
        let mut builder = JsonResultBuilder::new(memory).unwrap();
        let mut batch = builder
            .begin_batch(json_object_node_bytes(2).unwrap() + 2, 1)
            .unwrap();
        let mut object = serde_json::Map::new();
        object.insert("b".to_owned(), Value::Null);
        object.insert("a".to_owned(), Value::Null);
        batch.push_row(Value::Object(object));
        batch.finish().unwrap();
        assert_eq!(
            serde_json::to_string(&builder.finish()).unwrap(),
            r#"[{"a":null,"b":null}]"#
        );
    }

    #[test]
    fn btree_allowance_is_reviewed_with_the_pinned_toolchain() {
        let toolchain = include_str!("../../../rust-toolchain.toml");
        assert!(
            toolchain.contains("nightly-2026-08-24"),
            "review the private BTreeMap node allowance when changing the Rust toolchain"
        );
        assert!(
            !std::mem::needs_drop::<serde_json::Number>(),
            "review scalar result accounting if serde_json arbitrary_precision is enabled"
        );
    }
}
