use std::io;
use std::sync::mpsc;
use std::time::{Duration, Instant};

/// Protect synchronous filesystem startup and maintenance probes, where a
/// disconnected device may leave a syscall blocked. The observer does no path
/// resolution, so startup may still establish the process working directory.
pub(super) struct Deadline {
    completed: mpsc::Sender<Instant>,
    expires: Instant,
}

impl Deadline {
    pub(super) fn start(timeout: Duration) -> io::Result<Self> {
        Self::start_with(timeout, || std::process::exit(1))
    }

    pub(super) fn start_with(
        timeout: Duration,
        expired: impl FnOnce() + Send + 'static,
    ) -> io::Result<Self> {
        let expires = Instant::now().checked_add(timeout).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "volume deadline is too large")
        })?;
        let (completed, receiver) = mpsc::channel();
        std::thread::Builder::new().name("volume-deadline".into()).spawn(move || {
            if !matches!(receiver.recv_timeout(expires.saturating_duration_since(Instant::now())), Ok(completed) if completed <= expires) {
                expired();
            }
        })?;
        Ok(Self { completed, expires })
    }

    pub(super) fn complete(self) -> io::Result<()> {
        let now = Instant::now();
        if now > self.expires {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "volume operation exceeded its deadline",
            ));
        }
        self.completed
            .send(now)
            .map_err(|_| io::Error::other("volume deadline observer exited"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn completion_disarms_observer_and_drop_reports_incomplete_work() {
        for complete in [true, false] {
            let (sent, received) = mpsc::channel();
            let guard = Deadline::start_with(Duration::from_secs(5), move || {
                let _ = sent.send(());
            })
            .unwrap();
            if complete {
                guard.complete().unwrap();
            } else {
                drop(guard);
            }
            assert_eq!(
                received.recv_timeout(Duration::from_secs(5)),
                if complete {
                    Err(mpsc::RecvTimeoutError::Disconnected)
                } else {
                    Ok(())
                }
            );
        }
    }

    #[test]
    fn blocked_operation_expires_without_a_second_probe_or_join() {
        let (sent, received) = mpsc::channel();
        let guard = Deadline::start_with(Duration::from_millis(10), move || {
            let _ = sent.send(());
        })
        .unwrap();
        assert_eq!(received.recv_timeout(Duration::from_secs(5)), Ok(()));
        assert!(guard.complete().is_err());
    }
}
