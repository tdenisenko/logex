//! Serialize query responses into buffers charged through their final byte alias.
use std::io::{self, Write};

use bytes::Bytes;
use logex_query::QueryCancelCheck;
use logex_types::{QueryBuffer, QueryMemoryBudget, QueryMemoryError};
use serde::Serialize;

const CANCEL_INTERVAL: usize = 64 * 1024;

pub(crate) fn serialize_json<T: Serialize + ?Sized>(
    value: &T,
    memory: &QueryMemoryBudget,
    cancel: Option<&QueryCancelCheck>,
) -> io::Result<Bytes> {
    check_canceled(cancel)?;
    let mut writer = JsonWriter {
        buffer: QueryBuffer::try_with_capacity(128, Some(memory), "HTTP query response")?,
        cancel,
        until_cancel_check: CANCEL_INTERVAL,
    };
    // serde_json preserves an underlying I/O error on this conversion, including
    // the typed memory-capacity error. Do not flatten it into an error string.
    serde_json::to_writer(&mut writer, value).map_err(io::Error::from)?;
    check_canceled(cancel)?;
    Ok(writer.buffer.into_bytes().into())
}

pub(crate) fn is_capacity_error(error: &io::Error) -> bool {
    matches!(
        error
            .get_ref()
            .and_then(|error| error.downcast_ref::<QueryMemoryError>()),
        Some(QueryMemoryError::CapacityExceeded { .. } | QueryMemoryError::SizeOverflow { .. })
    )
}

fn check_canceled(cancel: Option<&QueryCancelCheck>) -> io::Result<()> {
    if cancel.is_some_and(|cancel| cancel()) {
        Err(io::Error::new(io::ErrorKind::Interrupted, "query canceled"))
    } else {
        Ok(())
    }
}

struct JsonWriter<'a> {
    buffer: QueryBuffer<u8>,
    cancel: Option<&'a QueryCancelCheck>,
    until_cancel_check: usize,
}

impl Write for JsonWriter<'_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let mut remaining = bytes;
        while !remaining.is_empty() {
            if self.until_cancel_check == 0 {
                check_canceled(self.cancel)?;
                self.until_cancel_check = CANCEL_INTERVAL;
            }
            let take = remaining.len().min(self.until_cancel_check);
            self.buffer.try_extend_from_slice(&remaining[..take])?;
            remaining = &remaining[take..];
            self.until_cancel_check -= take;
        }
        Ok(bytes.len())
    }

    fn write_all(&mut self, bytes: &[u8]) -> io::Result<()> {
        // `write` already consumes the complete slice or returns an error. The
        // standard default would retry Interrupted forever after cancellation.
        self.write(bytes).map(|_| ())
    }

    fn flush(&mut self) -> io::Result<()> {
        check_canceled(self.cancel)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use logex_types::QueryMemoryLimit;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    #[test]
    fn values_match_standard_encoding_and_aliases_retain_whole_capacity() {
        let memory = QueryMemoryBudget::new(QueryMemoryLimit::new(16 * 1024).unwrap());
        let value = serde_json::json!({
            "empty": [], "nested": [{"escaped": "line\n\"quote\"\\\u{0001}"}],
            "unicode": "ไทย🦀", "null": null, "number": u64::MAX,
        });
        let encoded = serialize_json(&value, &memory, None).unwrap();
        assert_eq!(encoded.as_ref(), serde_json::to_vec(&value).unwrap());
        let held = memory.used();
        assert!(held >= encoded.len() as u128);
        let clone = encoded.clone();
        let slice = encoded.slice(1..2);
        drop(encoded);
        drop(clone);
        assert_eq!(memory.used(), held);
        drop(slice);
        assert_eq!(memory.used(), 0);
    }

    #[test]
    fn growth_denial_preserves_typed_error_and_releases_partial_output() {
        let memory = QueryMemoryBudget::new(QueryMemoryLimit::new(300).unwrap());
        // The initial 128-byte output cannot coexist with its 256-byte replacement.
        let error = serialize_json(&"a".repeat(200), &memory, None).unwrap_err();
        assert!(is_capacity_error(&error), "{error}");
        assert_eq!(memory.used(), 0);
        let bytes = serialize_json(&true, &memory, None).unwrap();
        assert_eq!(bytes, "true");
        drop(bytes);
        assert_eq!(memory.used(), 0);
        assert!(!is_capacity_error(&io::Error::other(
            "serialization failed"
        )));
    }

    #[test]
    fn cancellation_during_one_large_write_releases_output() {
        let memory = QueryMemoryBudget::new(QueryMemoryLimit::new(1024 * 1024).unwrap());
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&calls);
        let cancel: QueryCancelCheck = Arc::new(move || {
            let call = observed.fetch_add(1, Ordering::Relaxed);
            assert!(call < 4, "a permanent cancellation must not be retried");
            call > 0
        });
        let error =
            serialize_json(&"a".repeat(CANCEL_INTERVAL * 2), &memory, Some(&cancel)).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::Interrupted);
        assert_eq!(calls.load(Ordering::Relaxed), 2);
        assert_eq!(memory.used(), 0);
    }

    #[test]
    fn serializer_errors_are_not_reported_as_capacity() {
        struct Invalid;
        impl Serialize for Invalid {
            fn serialize<S: serde::Serializer>(&self, _: S) -> Result<S::Ok, S::Error> {
                Err(serde::ser::Error::custom("intentional serialization error"))
            }
        }
        let memory = QueryMemoryBudget::new(QueryMemoryLimit::new(128).unwrap());
        let error = serialize_json(&Invalid, &memory, None).unwrap_err();
        assert!(!is_capacity_error(&error));
        assert!(
            error
                .to_string()
                .contains("intentional serialization error")
        );
        assert_eq!(memory.used(), 0);
    }
}
