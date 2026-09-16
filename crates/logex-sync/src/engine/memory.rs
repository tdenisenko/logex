//! Host memory estimates used by historical ingestion's existing batch policy.
//! These are advisory snapshots, not allocation reservations or process limits.

use std::sync::OnceLock;

#[cfg(target_os = "macos")]
mod darwin;

pub(super) fn total_bytes() -> Option<u64> {
    static TOTAL: OnceLock<Option<u64>> = OnceLock::new();
    *TOTAL.get_or_init(read_total_bytes)
}

#[cfg(target_os = "linux")]
fn read_total_bytes() -> Option<u64> {
    read_linux_meminfo_bytes("MemTotal:")
}

#[cfg(target_os = "linux")]
pub(super) fn available_bytes() -> Option<u64> {
    read_linux_meminfo_bytes("MemAvailable:")
}

#[cfg(target_os = "macos")]
fn read_total_bytes() -> Option<u64> {
    darwin::total_bytes()
}

#[cfg(target_os = "macos")]
pub(super) fn available_bytes() -> Option<u64> {
    darwin::available_bytes()
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn read_total_bytes() -> Option<u64> {
    None
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
pub(super) fn available_bytes() -> Option<u64> {
    None
}

#[cfg(target_os = "linux")]
fn read_linux_meminfo_bytes(prefix: &str) -> Option<u64> {
    parse_linux_meminfo_bytes(&std::fs::read_to_string("/proc/meminfo").ok()?, prefix)
}

#[cfg(any(target_os = "linux", test))]
fn parse_linux_meminfo_bytes(meminfo: &str, prefix: &str) -> Option<u64> {
    let rest = meminfo.lines().find_map(|line| line.strip_prefix(prefix))?;
    rest.split_whitespace()
        .next()?
        .parse::<u64>()
        .ok()?
        .checked_mul(1024)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn linux_snapshot_selects_the_requested_field() {
        let sample = "MemTotal: 16000 kB\nMemFree: 3000 kB\nMemAvailable: 7000 kB\n";
        assert_eq!(
            parse_linux_meminfo_bytes(sample, "MemAvailable:"),
            Some(7000 * 1024)
        );
        assert_eq!(
            parse_linux_meminfo_bytes(sample, "MemTotal:"),
            Some(16000 * 1024)
        );
        assert_eq!(parse_linux_meminfo_bytes(sample, "Missing:"), None);
    }

    #[test]
    fn linux_snapshot_rejects_missing_invalid_and_overflowing_numbers() {
        for sample in [
            "MemAvailable:",
            "MemAvailable: unknown kB",
            "MemAvailable: -1 kB",
            "MemAvailable: 18446744073709551615 kB",
        ] {
            assert_eq!(parse_linux_meminfo_bytes(sample, "MemAvailable:"), None);
        }
        assert_eq!(
            parse_linux_meminfo_bytes("MemAvailable: 0 kB", "MemAvailable:"),
            Some(0)
        );
    }

    #[test]
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    fn platform_snapshot_and_cached_total_are_available() {
        assert!(available_bytes().is_some());
        let total = total_bytes().expect("host total memory");
        assert!(total > 0);
        assert_eq!(total_bytes(), Some(total));
    }
}
