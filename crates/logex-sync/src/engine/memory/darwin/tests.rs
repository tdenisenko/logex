use super::*;

fn snapshot(
    free: u32,
    inactive: u32,
    speculative: u32,
    purgeable: u32,
) -> libc::vm_statistics64_data_t {
    // SAFETY: the structure contains only integers, all valid when zeroed.
    let mut stats: libc::vm_statistics64_data_t = unsafe { std::mem::zeroed() };
    stats.free_count = free;
    stats.inactive_count = inactive;
    stats.speculative_count = speculative;
    stats.purgeable_count = purgeable;
    stats
}

#[test]
fn overlapping_page_categories_are_not_added_twice() {
    let stats = snapshot(50, 20, 10, 15);
    for count in [REV0_COUNT, libc::HOST_VM_INFO64_COUNT] {
        for page_size in [4096, 16384] {
            assert_eq!(
                snapshot_available_bytes(&stats, count, page_size),
                Some(70 * page_size as u64)
            );
        }
    }
}

#[test]
fn invalid_counts_and_page_sizes_are_unavailable() {
    let stats = snapshot(50, 20, 10, 15);
    for count in [0, REV0_COUNT - 1, libc::HOST_VM_INFO64_COUNT + 1] {
        assert_eq!(snapshot_available_bytes(&stats, count, 4096), None);
    }
    for page_size in [-1, 0] {
        assert_eq!(
            snapshot_available_bytes(&stats, REV0_COUNT, page_size),
            None
        );
    }
}

#[test]
fn page_arithmetic_preserves_zero_and_checks_byte_overflow() {
    let zero = snapshot(0, 0, 0, 0);
    assert_eq!(snapshot_available_bytes(&zero, REV0_COUNT, 4096), Some(0));
    let large = snapshot(u32::MAX, u32::MAX, 0, 0);
    assert_eq!(
        snapshot_available_bytes(&large, REV0_COUNT, 16384),
        Some(2 * u64::from(u32::MAX) * 16384)
    );
    assert_eq!(
        snapshot_available_bytes(&large, REV0_COUNT, libc::c_long::MAX),
        None
    );
}

// These tests count only their own host-port references. Run each in a separate
// copy of this test executable to avoid unrelated concurrent host queries.
fn isolated(name: &str, run: impl FnOnce()) {
    const CHILD: &str = "LOGEX_MEMORY_REFERENCE_TEST_CHILD";
    let completed = format!("completed isolated memory reference control: {name}");
    if std::env::var(CHILD).as_deref() == Ok(name) {
        run();
        eprintln!("{completed}");
        return;
    }
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", name, "--nocapture"])
        .env(CHILD, name)
        .output()
        .unwrap();
    assert!(
        output.status.success() && String::from_utf8_lossy(&output.stderr).contains(&completed),
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

fn references(host: &HostPort) -> u32 {
    // SDK mach/mach_port.h: right and refs are natural_t (u32); SEND is zero.
    unsafe extern "C" {
        fn mach_port_get_refs(
            task: libc::mach_port_t,
            name: libc::mach_port_t,
            right: u32,
            refs: *mut u32,
        ) -> libc::kern_return_t;
    }
    let mut count = 0;
    #[allow(deprecated)]
    // SAFETY: owned host right, borrowed current task port, writable u32 output.
    let rc = unsafe { mach_port_get_refs(libc::mach_task_self(), host.0, 0, &mut count) };
    assert_eq!(rc, libc::KERN_SUCCESS);
    count
}

#[test]
fn repeated_probes_retain_no_extra_host_references() {
    isolated(
        concat!(
            module_path!(),
            "::repeated_probes_retain_no_extra_host_references"
        )
        .strip_prefix("logex_sync::")
        .unwrap(),
        || {
            let host = HostPort::acquire().unwrap();
            let before_initialization = references(&host);
            assert!(available_bytes().is_some());
            let initialized = references(&host);
            assert_eq!(initialized, before_initialization + 1);
            for _ in 0..2 {
                assert!(available_bytes().is_some());
            }
            assert_eq!(references(&host), initialized);
        },
    );
}

#[test]
fn owned_host_references_are_released_on_return_and_unwind() {
    isolated(
        concat!(
            module_path!(),
            "::owned_host_references_are_released_on_return_and_unwind"
        )
        .strip_prefix("logex_sync::")
        .unwrap(),
        || {
            let host = HostPort::acquire().unwrap();
            let before = references(&host);
            {
                let _other = HostPort::acquire().unwrap();
                assert_eq!(references(&host), before + 1);
            }
            assert_eq!(references(&host), before);
            let result = std::panic::catch_unwind(|| {
                let _other = HostPort::acquire().unwrap();
                panic!("controlled ownership unwind");
            });
            assert!(result.is_err());
            assert_eq!(references(&host), before);
        },
    );
}

#[test]
fn concurrent_cache_initialization_retains_only_one_reference() {
    isolated(
        concat!(
            module_path!(),
            "::concurrent_cache_initialization_retains_only_one_reference"
        )
        .strip_prefix("logex_sync::")
        .unwrap(),
        || {
            let host = HostPort::acquire().unwrap();
            let before = references(&host);
            let cache = OnceLock::new();
            let candidates = [HostPort::acquire().unwrap(), HostPort::acquire().unwrap()];
            std::thread::scope(|scope| {
                for candidate in candidates {
                    let cache = &cache;
                    scope.spawn(move || {
                        cache.get_or_init(|| candidate);
                    });
                }
            });
            assert_eq!(references(&host), before + 1);
            assert!(cached_host(&cache).is_some());
            assert_eq!(references(&host), before + 1);
            drop(cache);
            assert_eq!(references(&host), before);
        },
    );
}
