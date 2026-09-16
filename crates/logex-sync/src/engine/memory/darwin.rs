use std::sync::OnceLock;

// libc 0.2.184 does not expose this routine. The signature follows the macOS
// SDK's mach/mach_port.h; both ipc_space_t and mach_port_name_t are mach_port_t.
unsafe extern "C" {
    fn mach_port_deallocate(
        task: libc::mach_port_t,
        name: libc::mach_port_t,
    ) -> libc::kern_return_t;
}

/// Exactly one owned host send-right reference; never the borrowed task port.
struct HostPort(libc::mach_port_t);

impl HostPort {
    fn acquire() -> Option<Self> {
        #[allow(deprecated)]
        // SAFETY: no pointer arguments; acquires a send right in this task.
        let port = unsafe { libc::mach_host_self() };
        // MACH_PORT_NULL and MACH_PORT_DEAD are not usable send rights.
        (port != 0 && port != u32::MAX).then(|| Self(port))
    }
}

impl Drop for HostPort {
    fn drop(&mut self) {
        #[allow(deprecated)]
        // SAFETY: this wrapper owns exactly one reference in the current task.
        // mach_task_self() borrows the task port; only self.0 is deallocated.
        let rc = unsafe { mach_port_deallocate(libc::mach_task_self(), self.0) };
        if rc != libc::KERN_SUCCESS {
            tracing::warn!(
                code = rc,
                "Failed to release owned memory-probe host reference"
            );
        }
    }
}

fn cached_host(cache: &OnceLock<HostPort>) -> Option<&HostPort> {
    if let Some(host) = cache.get() {
        return Some(host);
    }
    let candidate = HostPort::acquire()?;
    // A concurrent initializer's unused closure drops its candidate reference.
    // A failed acquisition leaves the cache empty so a later probe can retry.
    Some(cache.get_or_init(|| candidate))
}

pub(super) fn total_bytes() -> Option<u64> {
    let mut value = 0u64;
    let mut size = std::mem::size_of_val(&value);
    // SAFETY: the name is NUL terminated, value/size are aligned writable outputs,
    // and a null new-value pointer makes this a read-only sysctl query.
    let rc = unsafe {
        libc::sysctlbyname(
            c"hw.memsize".as_ptr(),
            (&mut value as *mut u64).cast(),
            &mut size,
            std::ptr::null_mut(),
            0,
        )
    };
    (rc == 0 && size == std::mem::size_of_val(&value)).then_some(value)
}

pub(super) fn available_bytes() -> Option<u64> {
    // The host right is task-wide and permits concurrent read-only queries.
    // Retain one for the process lifetime instead of acquiring one per probe.
    // Static storage is reclaimed by the kernel when the process exits. Memory
    // statistics themselves are never cached.
    static HOST: OnceLock<HostPort> = OnceLock::new();
    let host = cached_host(&HOST)?;
    // SAFETY: this C structure contains only integers; all-zero is a valid value.
    let mut stats: libc::vm_statistics64_data_t = unsafe { std::mem::zeroed() };
    let mut count = libc::HOST_VM_INFO64_COUNT;
    // SAFETY: host remains owned, stats is aligned and writable for the advertised
    // count of integer_t words, and count is a valid in/out pointer. The SDK and
    // libc agree on this prefix of the versioned host-statistics structure.
    let rc = unsafe {
        libc::host_statistics64(
            host.0,
            libc::HOST_VM_INFO64,
            (&mut stats as *mut libc::vm_statistics64_data_t).cast(),
            &mut count,
        )
    };
    if rc != libc::KERN_SUCCESS {
        return None;
    }
    // SAFETY: sysconf takes no pointers; a failed/nonpositive result is rejected.
    let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    snapshot_available_bytes(&stats, count, page_size)
}

// SDK HOST_VM_INFO64_REV0_COUNT: all fields preceding `decompressions`. Accept
// this older complete revision as well as libc's current buffer size, rather
// than requiring fields that this estimate does not use.
const REV0_COUNT: libc::mach_msg_type_number_t =
    (std::mem::offset_of!(libc::vm_statistics64_data_t, decompressions)
        / std::mem::size_of::<libc::integer_t>()) as libc::mach_msg_type_number_t;

fn snapshot_available_bytes(
    stats: &libc::vm_statistics64_data_t,
    count: libc::mach_msg_type_number_t,
    page_size: libc::c_long,
) -> Option<u64> {
    if !(REV0_COUNT..=libc::HOST_VM_INFO64_COUNT).contains(&count) || page_size <= 0 {
        return None;
    }
    // Apple's vm_statistics.h includes speculative pages in free_count.
    // Purgeable pages overlap page queues and cannot be added as a disjoint pool.
    // Free + inactive is an estimate: inactive pages are not all immediately
    // reclaimable, and active purgeable pages are omitted.
    let pages = u64::from(stats.free_count) + u64::from(stats.inactive_count);
    pages.checked_mul(page_size as u64)
}

#[cfg(test)]
mod tests;
