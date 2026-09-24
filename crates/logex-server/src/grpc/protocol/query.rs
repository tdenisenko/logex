use std::io;

use logex_query::{QueryCancelCheck, SqlQueryResult};
use logex_types::{QueryMemoryBudget, QueryMemoryError};
use serde::Serialize;

use crate::grpc::pb::{QueryResponse, QueryRow};

use super::{
    CheckedEncodedLen, OwnedResponse, PROTOCOL_STAGE, ResponseBuilder, bytes_field_len,
    check_cancel, checked_add, message_field_len, uint_field_len,
};

pub(in crate::grpc) fn query_response(
    result: &SqlQueryResult,
    limit: Option<usize>,
    offset: usize,
    memory: &QueryMemoryBudget,
    cancel: Option<&QueryCancelCheck>,
) -> io::Result<OwnedResponse<QueryResponse>> {
    build_query_response(
        result.rows.iter(),
        result.total_scanned,
        limit,
        offset,
        memory,
        cancel,
    )
}

fn build_query_response<'a, T: Serialize + 'a>(
    rows: impl ExactSizeIterator<Item = &'a T>,
    total_scanned: u64,
    limit: Option<usize>,
    offset: usize,
    memory: &QueryMemoryBudget,
    cancel: Option<&QueryCancelCheck>,
) -> io::Result<OwnedResponse<QueryResponse>> {
    check_cancel(cancel)?;
    let row_count_usize = rows.len();
    let row_count = usize_to_u64(row_count_usize)?;
    let limit_value = limit.map(usize_to_u64).transpose()?.unwrap_or(0);
    let offset_value = usize_to_u64(offset)?;
    let next_offset = if limit.is_some_and(|limit| limit > 0 && row_count_usize == limit) {
        let next = offset
            .checked_add(row_count_usize)
            .ok_or_else(size_overflow)?;
        Some(usize_to_u64(next)?)
    } else {
        None
    };
    let initial_bytes = QueryMemoryBudget::array_bytes::<QueryRow>(row_count_usize, PROTOCOL_STAGE)
        .map_err(io::Error::other)?;
    let message = QueryResponse {
        rows: Vec::new(),
        total_scanned,
        row_count,
        limit: limit_value,
        offset: offset_value,
        next_offset,
        max_limit: 0,
    };
    // The builder precedes every allocation covered by its aggregate charge, so
    // partial rows and the current String drop before the reservation on error.
    let mut builder = ResponseBuilder::new(message, memory, initial_bytes)?;
    let mut encoded_rows = builder.allocate_vec::<QueryRow>(row_count_usize)?;
    for row in rows {
        let json = builder.json(row, cancel)?;
        encoded_rows.push(QueryRow { json });
    }
    builder.message.rows = encoded_rows;
    check_cancel(cancel)?;
    builder.finish()
}

impl CheckedEncodedLen for QueryRow {
    fn checked_encoded_len(&self) -> io::Result<usize> {
        bytes_field_len(self.json.len(), !self.json.is_empty())
    }
}

impl CheckedEncodedLen for QueryResponse {
    fn checked_encoded_len(&self) -> io::Result<usize> {
        let mut length = 0;
        for row in &self.rows {
            checked_add(&mut length, message_field_len(row.checked_encoded_len()?)?)?;
        }
        checked_add(
            &mut length,
            uint_field_len(self.total_scanned, self.total_scanned != 0),
        )?;
        checked_add(
            &mut length,
            uint_field_len(self.row_count, self.row_count != 0),
        )?;
        checked_add(&mut length, uint_field_len(self.limit, self.limit != 0))?;
        checked_add(&mut length, uint_field_len(self.offset, self.offset != 0))?;
        if let Some(next_offset) = self.next_offset {
            checked_add(&mut length, uint_field_len(next_offset, true))?;
        }
        checked_add(
            &mut length,
            uint_field_len(self.max_limit, self.max_limit != 0),
        )?;
        Ok(length)
    }
}

fn usize_to_u64(value: usize) -> io::Result<u64> {
    u64::try_from(value).map_err(|_| size_overflow())
}

fn size_overflow() -> io::Error {
    io::Error::other(QueryMemoryError::SizeOverflow {
        stage: PROTOCOL_STAGE,
    })
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use logex_query::{
        NativeStorageSnapshot, SqlQueryPage, execute_sql_page_on_snapshot_with_memory,
    };
    use logex_types::QueryMemoryLimit;
    use prost::Message as _;
    use serde_json::json;

    use super::*;

    #[test]
    fn checked_lengths_match_prost_for_defaults_presence_and_boundaries() {
        let rows = [
            QueryRow {
                json: String::new(),
            },
            QueryRow {
                json: "null".to_owned(),
            },
            QueryRow {
                json: "\"ไทย🦀\\n\"".to_owned(),
            },
        ];
        for row in &rows {
            assert_eq!(row.checked_encoded_len().unwrap(), row.encoded_len());
        }

        for response in [
            QueryResponse::default(),
            QueryResponse {
                rows: rows.to_vec(),
                total_scanned: 127,
                row_count: 128,
                limit: 16_383,
                offset: 16_384,
                next_offset: Some(0),
                max_limit: u64::MAX,
            },
        ] {
            assert_eq!(
                response.checked_encoded_len().unwrap(),
                response.encoded_len()
            );
        }
    }

    #[tokio::test]
    async fn real_sql_rows_and_protocol_charges_overlap_then_release_in_owner_order() {
        let memory = QueryMemoryBudget::new(QueryMemoryLimit::new(8 * 1024 * 1024).unwrap());
        let result = execute_sql_page_on_snapshot_with_memory(
            "SELECT 7 AS value, 'retained' AS text",
            NativeStorageSnapshot::default(),
            0,
            SqlQueryPage::default(),
            None,
            memory.clone(),
        )
        .await
        .unwrap();
        let structured_charge = memory.used();
        assert!(structured_charge > 0);
        let response = query_response(&result, Some(1), 5, &memory, None).unwrap();
        assert_eq!(response.row_count, 1);
        assert_eq!(response.next_offset, Some(6));
        assert!(memory.used() > structured_charge);
        drop(result);
        assert!(memory.used() > 0, "the response must retain its own charge");
        assert_eq!(response.rows[0].json, r#"{"text":"retained","value":7}"#);
        drop(response);
        assert_eq!(memory.used(), 0);
    }

    #[test]
    fn exact_protocol_bound_succeeds_and_one_byte_less_is_denied() {
        let rows = [json!(true)];
        let exact = std::mem::size_of::<QueryRow>() + 128;
        let memory = QueryMemoryBudget::new(QueryMemoryLimit::new(exact).unwrap());
        let response = build_query_response(rows.iter(), 1, None, 0, &memory, None).unwrap();
        assert_eq!(memory.used(), exact as u128);
        drop(response);
        assert_eq!(memory.used(), 0);

        let memory = QueryMemoryBudget::new(QueryMemoryLimit::new(exact - 1).unwrap());
        let error = build_query_response(rows.iter(), 1, None, 0, &memory, None).unwrap_err();
        assert!(matches!(
            error
                .get_ref()
                .and_then(|error| error.downcast_ref::<QueryMemoryError>()),
            Some(QueryMemoryError::CapacityExceeded { .. })
        ));
        assert_eq!(memory.used(), 0);
    }

    #[test]
    fn pagination_offset_is_not_limited_by_allocation_addressability() {
        let rows = [json!(null)];
        let memory = QueryMemoryBudget::new(QueryMemoryLimit::new(1024).unwrap());
        let offset = isize::MAX as usize;
        let response =
            build_query_response(rows.iter(), 1, Some(1), offset, &memory, None).unwrap();

        assert_eq!(response.offset, u64::try_from(offset).unwrap());
        assert_eq!(
            response.next_offset,
            Some(u64::try_from(offset.checked_add(1).unwrap()).unwrap())
        );
    }

    #[test]
    fn cancellation_releases_partial_protocol_values() {
        let memory = QueryMemoryBudget::new(QueryMemoryLimit::new(256 * 1024).unwrap());
        let rows = [json!("x".repeat(128 * 1024))];
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&calls);
        let cancel: QueryCancelCheck =
            Arc::new(move || observed.fetch_add(1, Ordering::Relaxed) > 0);
        let error =
            build_query_response(rows.iter(), 1, None, 0, &memory, Some(&cancel)).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::Interrupted);
        assert!(calls.load(Ordering::Relaxed) >= 2);
        assert_eq!(memory.used(), 0);
    }
}
