use super::*;

#[test]
fn configuration_requires_pair_and_absolute_mount() {
    assert!(configured(None, None).unwrap().is_none());
    assert!(configured(Some("/volume".into()), None).is_err());
    assert!(configured(None, Some("abcd-1234".into())).is_err());
    assert!(configured(Some("volume".into()), Some("abcd-1234".into())).is_err());
    assert_eq!(
        configured(Some("/volume".into()), Some("ABCD-1234".into())).unwrap(),
        Some(("/volume".into(), "abcd-1234".into()))
    );
}

#[test]
fn identifiers_and_paths_reject_ambiguous_values() {
    for uuid in [
        "",
        "0",
        "0000-0000",
        "../abcd",
        "ab cd",
        "abcd/1234",
        "uuid=abcd",
    ] {
        assert!(normalize_uuid(uuid).is_err());
    }
    assert!(normalize_uuid(&"a".repeat(129)).is_err());
    assert_eq!(normalize_uuid("aBcD-1234").unwrap(), "abcd-1234");
    for path in ["relative", "/volume/../node", ""] {
        assert!(absolute_normalized(Path::new(path)).is_err());
    }
    assert_eq!(
        absolute_normalized(Path::new("/volume/./node/")).unwrap(),
        Path::new("/volume/node")
    );
}

#[test]
fn inline_checkpoints_and_absolute_descriptor_paths_keep_their_meaning() {
    for value in [
        None,
        Some("123@0x0123456789abcdef".to_owned()),
        Some("/absolute/checkpoint.json".to_owned()),
    ] {
        let mut checkpoint = value.clone();
        preserve_checkpoint_path(&mut checkpoint).unwrap();
        assert_eq!(checkpoint, value);
    }
}

#[test]
fn command_setup_and_monitor_cases_run_in_owned_children() {
    for case in [
        "checkpoint",
        "idle_drop",
        "failure",
        "blocked",
        "panic",
        "runtime_blocked",
        "callback_blocked",
    ] {
        let directory = tempfile::tempdir().unwrap();
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "volume::tests::command_setup_child",
                "--nocapture",
            ])
            .env("LOGEX_VOLUME_COMMAND_CASE", case)
            .current_dir(directory.path())
            .output()
            .unwrap();
        let expected = if ["checkpoint", "idle_drop"].contains(&case) {
            0
        } else {
            1
        };
        assert_eq!(output.status.code(), Some(expected), "{case}: {output:?}");
        if case == "runtime_blocked" {
            assert_eq!(
                std::fs::read(directory.path().join("verified")).unwrap(),
                b"failure observed without async progress"
            );
        } else if case == "callback_blocked" {
            assert!(directory.path().join("callback_entered").exists());
            assert!(!directory.path().join("callback_returned").exists());
        }
    }
}

#[test]
fn command_setup_child() {
    let Ok(case) = std::env::var("LOGEX_VOLUME_COMMAND_CASE") else {
        return;
    };
    if case == "checkpoint" {
        std::fs::write("checkpoint.json", b"owned descriptor fixture").unwrap();
        let original = std::env::current_dir().unwrap().join("checkpoint.json");
        let mut checkpoint = Some("checkpoint.json".to_owned());
        preserve_checkpoint_path(&mut checkpoint).unwrap();
        std::fs::create_dir("data").unwrap();
        std::env::set_current_dir("data").unwrap();
        assert_eq!(checkpoint.as_deref(), original.to_str());
        assert_eq!(
            std::fs::read(checkpoint.unwrap()).unwrap(),
            b"owned descriptor fixture"
        );
        return;
    }
    if ["runtime_blocked", "callback_blocked"].contains(&case.as_str()) {
        let (begin, allowed) = mpsc::channel();
        let (reported, failure) = mpsc::channel();
        let monitor = StorageMonitor::start_with(
            Duration::from_millis(1),
            Duration::from_secs(5),
            Duration::from_secs(1),
            false,
            move || {
                allowed.recv_timeout(Duration::from_secs(5)).unwrap();
                Err(io::Error::other("owned runtime probe failure"))
            },
        )
        .unwrap();
        let blocked = case == "callback_blocked";
        monitor
            .handle()
            .set_failure_handler(move |reason| {
                std::fs::write("callback_entered", reason).unwrap();
                reported.send(reason.to_owned()).unwrap();
                if blocked {
                    std::thread::sleep(Duration::from_secs(5));
                    std::fs::write("callback_returned", b"unexpected completion").unwrap();
                }
            })
            .unwrap();
        assert!(monitor.handle().set_failure_handler(|_| {}).is_err());
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        runtime.block_on(async {
            begin.send(()).unwrap();
            // This models a synchronous engine I/O poll. The observer must
            // notify from outside the async runtime, with no timer progress.
            assert_eq!(
                failure.recv_timeout(Duration::from_secs(5)).unwrap(),
                "owned runtime probe failure"
            );
        });
        if blocked {
            std::thread::sleep(Duration::from_secs(5));
            panic!("terminal deadline did not bound the blocked callback");
        }
        monitor.handle().report("later failure".into());
        assert_eq!(
            std::fs::read("callback_entered").unwrap(),
            b"owned runtime probe failure"
        );
        assert!(monitor.handle().set_failure_handler(|_| {}).is_err());
        std::fs::write("verified", b"failure observed without async progress").unwrap();
        // A latched failure remains exit 1 even if main reaches teardown first.
        drop(monitor);
        panic!("volume failure became a successful command exit");
    }
    let idle = case == "idle_drop";
    let guard = StorageMonitor::start_with(
        if idle {
            Duration::from_secs(30)
        } else {
            Duration::from_millis(1)
        },
        Duration::from_millis(50),
        Duration::from_secs(1),
        false,
        move || match case.as_str() {
            "failure" => Err(io::Error::other("owned probe failure")),
            "blocked" => {
                std::thread::sleep(Duration::from_secs(2));
                Ok(())
            }
            "panic" => panic!("owned probe unwind"),
            _ => panic!("idle monitor must not probe"),
        },
    )
    .unwrap();
    if idle {
        let started = std::time::Instant::now();
        drop(guard);
        assert!(started.elapsed() < Duration::from_secs(5));
    } else {
        std::thread::sleep(Duration::from_secs(5));
        panic!("failed monitor did not end its owned child process");
    }
}
