//! Invalid admission settings must fail before any storage or network startup.
use std::{
    fs,
    io::Read,
    path::Path,
    process::{Command, Output, Stdio},
    time::{Duration, Instant},
};

fn reject_setting_with_code(
    root: &Path,
    arguments: &[&str],
    setting: &str,
    expected_code: i32,
) -> Output {
    let mut child = Command::new(env!("CARGO_BIN_EXE_logex"))
        .arg("--data-dir")
        .arg(root)
        .args(arguments)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut stdout = child.stdout.take().unwrap();
    let mut stderr = child.stderr.take().unwrap();
    let out = std::thread::spawn(move || {
        let mut bytes = Vec::new();
        stdout.read_to_end(&mut bytes).unwrap();
        bytes
    });
    let err = std::thread::spawn(move || {
        let mut bytes = Vec::new();
        stderr.read_to_end(&mut bytes).unwrap();
        bytes
    });
    let deadline = Instant::now() + Duration::from_secs(30);
    let (status, expired) = loop {
        if let Some(status) = child.try_wait().unwrap() {
            break (status, false);
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            break (child.wait().unwrap(), true);
        }
        std::thread::sleep(Duration::from_millis(10));
    };
    let output = Output {
        status,
        stdout: out.join().unwrap(),
        stderr: err.join().unwrap(),
    };
    assert!(!expired, "invalid setting did not terminate promptly");
    assert_eq!(
        output.status.code(),
        Some(expected_code),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(output.stdout.is_empty());
    assert!(String::from_utf8_lossy(&output.stderr).contains(setting));
    assert!(!root.exists(), "invalid admission setting created storage");
    output
}

fn reject_setting(root: &Path, arguments: &[&str], setting: &str) -> Output {
    reject_setting_with_code(root, arguments, setting, 1)
}

fn reject(root: &Path, arguments: &[&str]) -> Output {
    reject_setting(root, arguments, "query-max-concurrent")
}

#[test]
fn invalid_browser_origins_reject_before_sync_or_repair_creates_storage() {
    for command in ["sync", "repair"] {
        for from_config in [false, true] {
            let temp = tempfile::tempdir().unwrap();
            let root = temp.path().join("absent-data");
            let config = temp.path().join("settings.toml");
            let contents = "http_allowed_origins = ['https://example.test/dashboard']\n\
                            dashboard_password = 'private-origin-fixture-value'\n";
            if from_config {
                fs::write(&config, contents).unwrap();
            }
            let before = fs::metadata(temp.path()).unwrap().modified().unwrap();
            let config_mtime =
                from_config.then(|| fs::metadata(&config).unwrap().modified().unwrap());
            let output = if from_config {
                reject_setting(
                    &root,
                    &["--config", config.to_str().unwrap(), command],
                    "failed to parse config",
                )
            } else {
                reject_setting_with_code(
                    &root,
                    &[
                        command,
                        "--http-allowed-origin",
                        "https://example.test/dashboard",
                    ],
                    "--http-allowed-origin",
                    2,
                )
            };
            assert!(
                !String::from_utf8_lossy(&output.stderr).contains("private-origin-fixture-value")
            );
            assert_eq!(
                fs::metadata(temp.path()).unwrap().modified().unwrap(),
                before
            );
            if let Some(mtime) = config_mtime {
                assert_eq!(fs::read_to_string(&config).unwrap(), contents);
                assert_eq!(fs::metadata(&config).unwrap().modified().unwrap(), mtime);
            }
        }
    }
}

#[test]
fn invalid_query_memory_rejects_before_storage_without_echoing_config() {
    // TOML integers are signed, so the platform-size overflow is exercised
    // through the u64 CLI parser; config zero exercises resolved validation.
    for (from_config, value) in [(false, 0), (false, isize::MAX as u64 + 1), (true, 0)] {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("absent-data");
        let config = temp.path().join("settings.toml");
        let credential = "private-memory-fixture-password";
        let contents =
            format!("query_memory_bytes = {value}\ndashboard_password = '{credential}'\n");
        if from_config {
            fs::write(&config, &contents).unwrap();
        }
        let parent_mtime = fs::metadata(temp.path()).unwrap().modified().unwrap();
        let config_mtime = from_config.then(|| fs::metadata(&config).unwrap().modified().unwrap());
        let number = value.to_string();
        let args = if from_config {
            vec!["--config", config.to_str().unwrap(), "sync"]
        } else {
            vec!["sync", "--query-memory-bytes", &number]
        };
        let output = reject_setting(&root, &args, "query-memory-bytes");
        assert!(!String::from_utf8_lossy(&output.stderr).contains(credential));
        assert_eq!(
            fs::metadata(temp.path()).unwrap().modified().unwrap(),
            parent_mtime
        );
        if let Some(mtime) = config_mtime {
            assert_eq!(fs::read_to_string(&config).unwrap(), contents);
            assert_eq!(fs::metadata(&config).unwrap().modified().unwrap(), mtime);
        }
    }
}

#[test]
fn invalid_cli_query_capacity_rejects_before_creating_storage() {
    for value in [0, tokio::sync::Semaphore::MAX_PERMITS + 1] {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("absent-data");
        let before = fs::metadata(temp.path()).unwrap().modified().unwrap();
        reject(
            &root,
            &["sync", "--query-max-concurrent", &value.to_string()],
        );
        assert_eq!(fs::read_dir(temp.path()).unwrap().count(), 0);
        assert_eq!(
            fs::metadata(temp.path()).unwrap().modified().unwrap(),
            before
        );
    }
}

#[test]
fn invalid_config_query_capacity_rejects_without_echoing_credentials_or_creating_storage() {
    for value in [0, tokio::sync::Semaphore::MAX_PERMITS + 1] {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("absent-data");
        let config = temp.path().join("settings.toml");
        let credential = "private-config-fixture-password-never-print";
        let contents =
            format!("query_max_concurrent = {value}\ndashboard_password = '{credential}'\n");
        fs::write(&config, &contents).unwrap();
        let parent_mtime = fs::metadata(temp.path()).unwrap().modified().unwrap();
        let config_mtime = fs::metadata(&config).unwrap().modified().unwrap();
        let output = reject(&root, &["--config", config.to_str().unwrap(), "sync"]);
        assert!(!String::from_utf8_lossy(&output.stderr).contains(credential));
        assert_eq!(fs::read_dir(temp.path()).unwrap().count(), 1);
        assert_eq!(fs::read_to_string(&config).unwrap(), contents);
        assert_eq!(
            fs::metadata(&config).unwrap().modified().unwrap(),
            config_mtime
        );
        assert_eq!(
            fs::metadata(temp.path()).unwrap().modified().unwrap(),
            parent_mtime
        );
    }
}
