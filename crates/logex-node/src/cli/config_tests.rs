use super::{Cli, Command, Config};
use clap::{CommandFactory, FromArgMatches};
use std::path::PathBuf;

fn resolve(args: &[&str], config: Config) -> Cli {
    let matches = Cli::command().try_get_matches_from(args).unwrap();
    let mut cli = Cli::from_arg_matches(&matches).unwrap();
    cli.apply_config(config, &matches);
    cli
}

fn file_config() -> Config {
    toml::from_str(
        r#"
data_dir = "file-data"
log_level = "warn"
partition_target_rows = 2000000
checkpoint = "file-checkpoint"
checkpoint_sync_url = "https://file.example.invalid"
nat = "none"
p2p_bind_ip = "::1"
execution_bootnodes = ["file-bootnode"]
execution_discv5_port = 9201
http_host = "::1"
grpc_host = "::1"
allow_public_grpc = false
dashboard_enabled = false
dashboard_password = "fixture-file-password"
"#,
    )
    .unwrap()
}

#[test]
fn explicit_data_directory_wins_before_and_after_every_subcommand() {
    for command in ["sync", "info", "compact", "build-indexes"] {
        for args in [
            vec!["logex", "--data-dir", "cli-data", command],
            vec!["logex", command, "--data-dir", "cli-data"],
        ] {
            assert_eq!(
                resolve(&args, file_config()).data_dir,
                Some(PathBuf::from("cli-data"))
            );
        }
    }
}

#[test]
fn explicit_default_log_level_wins_over_file() {
    let cli = resolve(&["logex", "sync", "--log-level", "info"], file_config());
    assert_eq!(
        super::super::normalize_info_log_filter(cli.log_level),
        "info,discv5=error"
    );
}

#[test]
fn explicit_default_partition_target_wins_over_file() {
    assert_eq!(
        resolve(
            &["logex", "--partition-target-rows", "1000000", "info"],
            file_config()
        )
        .partition_target_rows,
        1_000_000
    );
}

#[test]
fn explicit_checkpoint_settings_win_over_file() {
    let cli = resolve(
        &[
            "logex",
            "sync",
            "--checkpoint",
            "cli-checkpoint",
            "--checkpoint-sync-url",
            "https://cli.example.invalid",
        ],
        file_config(),
    );
    assert_eq!(cli.checkpoint.as_deref(), Some("cli-checkpoint"));
    assert_eq!(
        cli.checkpoint_sync_url.as_deref(),
        Some("https://cli.example.invalid")
    );
}

#[test]
fn explicit_listener_and_bind_addresses_win_over_file() {
    let cli = resolve(
        &[
            "logex",
            "sync",
            "--http-host",
            "127.0.0.1",
            "--grpc-host",
            "127.0.0.1",
            "--p2p-bind-ip",
            "127.0.0.1",
        ],
        file_config(),
    );
    let Command::Sync {
        http_host,
        grpc_host,
        p2p_bind_ip,
        ..
    } = cli.command
    else {
        panic!("sync");
    };
    assert_eq!(http_host, "127.0.0.1".parse::<std::net::IpAddr>().unwrap());
    assert_eq!(grpc_host, http_host);
    assert_eq!(p2p_bind_ip, Some(http_host));
}

#[test]
fn explicit_default_nat_and_discovery_port_win_over_file() {
    let cli = resolve(
        &[
            "logex",
            "sync",
            "--nat",
            "any",
            "--execution-discv5-port",
            "9200",
        ],
        file_config(),
    );
    let Command::Sync {
        nat,
        execution_discv5_port,
        ..
    } = cli.command
    else {
        panic!("sync");
    };
    assert_eq!(nat, "any");
    assert_eq!(execution_discv5_port, 9200);
}

#[test]
fn explicit_boolean_permission_wins_over_file() {
    let cli = resolve(&["logex", "sync", "--allow-public-grpc"], file_config());
    let Command::Sync {
        allow_public_grpc, ..
    } = cli.command
    else {
        panic!("sync");
    };
    assert!(allow_public_grpc);
}

#[test]
fn omitted_options_use_every_supported_file_setting() {
    let cli = resolve(&["logex", "sync"], file_config());
    assert_eq!(cli.data_dir, Some(PathBuf::from("file-data")));
    assert_eq!(cli.log_level, "warn");
    assert_eq!(cli.partition_target_rows, 2_000_000);
    assert_eq!(cli.checkpoint.as_deref(), Some("file-checkpoint"));
    assert_eq!(
        cli.checkpoint_sync_url.as_deref(),
        Some("https://file.example.invalid")
    );
    let Command::Sync {
        http_host,
        grpc_host,
        p2p_bind_ip,
        nat,
        execution_bootnodes,
        execution_discv5_port,
        allow_public_grpc,
        disable_dashboard,
        dashboard_password,
        ..
    } = cli.command
    else {
        panic!("sync");
    };
    assert_eq!(http_host, "::1".parse::<std::net::IpAddr>().unwrap());
    assert_eq!(grpc_host, http_host);
    assert_eq!(p2p_bind_ip, Some(http_host));
    assert_eq!(nat, "none");
    assert_eq!(execution_bootnodes, ["file-bootnode"]);
    assert_eq!(execution_discv5_port, 9201);
    assert!(!allow_public_grpc);
    assert!(disable_dashboard);
    assert_eq!(dashboard_password.as_deref(), Some("fixture-file-password"));
}

#[test]
fn explicit_bootnodes_password_and_dashboard_disable_keep_precedence() {
    let mut config = file_config();
    config.dashboard_enabled = Some(true);
    let cli = resolve(
        &[
            "logex",
            "sync",
            "--execution-bootnode",
            "cli-a,cli-b",
            "--execution-bootnode",
            "cli-c",
            "--dashboard-password",
            "fixture-cli-password",
            "--disable-dashboard",
        ],
        config,
    );
    let Command::Sync {
        execution_bootnodes,
        dashboard_password,
        disable_dashboard,
        ..
    } = cli.command
    else {
        panic!("sync");
    };
    assert_eq!(execution_bootnodes, ["cli-a", "cli-b", "cli-c"]);
    assert_eq!(dashboard_password.as_deref(), Some("fixture-cli-password"));
    assert!(disable_dashboard);
}

fn load_fixture(contents: &str) -> Result<Config, String> {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("config.toml");
    std::fs::write(&path, contents).unwrap();
    Config::load(&path)
}

#[test]
fn unknown_configuration_keys_are_rejected() {
    assert!(load_fixture("data_dri = 'fixture-data'\n").is_err());
}

#[test]
fn syntax_diagnostics_do_not_repeat_configuration_contents() {
    let error =
        load_fixture("dashboard_password = 'fixture-private-value' trailing\n").unwrap_err();
    assert!(!error.contains("fixture-private-value"));
}

#[test]
fn type_diagnostics_do_not_repeat_configuration_values() {
    let error = load_fixture("partition_target_rows = 'fixture-private-value'\n").unwrap_err();
    assert!(!error.contains("fixture-private-value"));
}

#[test]
fn empty_config_preserves_defaults_and_cli_only_options() {
    for args in [
        vec!["logex", "sync"],
        vec!["logex", "info"],
        vec!["logex", "compact", "--limit", "3"],
        vec!["logex", "build-indexes", "--sealed", "--jobs", "2"],
        vec![
            "logex",
            "sync",
            "--http-port",
            "18577",
            "--grpc-port",
            "18578",
            "--max-peers",
            "25",
            "--disable-historical-sync",
        ],
    ] {
        let matches = Cli::command().try_get_matches_from(&args).unwrap();
        let expected = Cli::from_arg_matches(&matches).unwrap();
        assert_eq!(
            format!("{:?}", resolve(&args, Config::default())),
            format!("{expected:?}")
        );
    }
}

#[test]
fn explicit_global_defaults_win_in_both_positions() {
    for command in ["sync", "info", "compact", "build-indexes"] {
        for args in [
            vec![
                "logex",
                "--log-level",
                "info",
                "--partition-target-rows",
                "1000000",
                command,
            ],
            vec![
                "logex",
                command,
                "--log-level",
                "info",
                "--partition-target-rows",
                "1000000",
            ],
        ] {
            let cli = resolve(&args, file_config());
            assert_eq!(cli.log_level, "info");
            assert_eq!(cli.partition_target_rows, 1_000_000);
        }
    }
}

#[test]
fn omitted_permission_inherits_true_and_dashboard_defaults_to_enabled() {
    let config = Config {
        allow_public_grpc: Some(true),
        ..Default::default()
    };
    let cli = resolve(&["logex", "sync"], config);
    let Command::Sync {
        allow_public_grpc,
        disable_dashboard,
        ..
    } = cli.command
    else {
        panic!("sync");
    };
    assert!(allow_public_grpc);
    assert!(!disable_dashboard);
}

#[test]
fn config_diagnostic_retains_path_and_unicode_location() {
    let error =
        load_fixture("# café\npartition_target_rows = 'fixture-private-value'\n").unwrap_err();
    assert!(error.contains("config.toml"));
    assert!(error.contains("at line 2, column 25"), "{error}");
    assert!(error.contains("supported keys and value types"));
    assert!(!error.contains("fixture-private-value"));

    let error = load_fixture("dashboard_password = 'café' trailing\n").unwrap_err();
    assert!(error.contains("at line 1, column 29"), "{error}");
    assert!(!error.contains("café"));
}

#[test]
fn config_rejects_unknown_tables_duplicate_keys_and_wrong_types() {
    for contents in [
        "[unexpected]\nvalue = 1\n",
        "log_level = 'info'\nlog_level = 'warn'\n",
        "execution_discv5_port = 65536\n",
        "allow_public_grpc = 'true'\n",
        "http_host = 'fixture.invalid'\n",
    ] {
        let error = load_fixture(contents).unwrap_err();
        assert!(error.contains("config.toml"));
        assert!(error.contains("line"));
    }
}

#[test]
fn missing_config_reports_its_path_without_creating_it() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("missing.toml");
    let error = Config::load(&path).unwrap_err();
    assert!(error.contains("failed to read config"));
    assert!(error.contains("missing.toml"));
    assert!(!path.exists());
}

#[test]
fn expected_volume_options_follow_cli_file_precedence_for_every_command() {
    for command in ["sync", "info", "compact", "build-indexes"] {
        for prefix in [true, false] {
            let config: Config = toml::from_str(
                "expected_volume_mount = '/file-volume'\nexpected_volume_uuid = 'abcd-1234'\n",
            )
            .unwrap();
            let args = if prefix {
                vec![
                    "logex",
                    "--expected-volume-mount",
                    "/cli-volume",
                    "--expected-volume-uuid",
                    "1234-abcd",
                    command,
                ]
            } else {
                vec![
                    "logex",
                    command,
                    "--expected-volume-mount",
                    "/cli-volume",
                    "--expected-volume-uuid",
                    "1234-abcd",
                ]
            };
            let cli = resolve(&args, config);
            assert_eq!(
                cli.expected_volume_mount,
                Some(PathBuf::from("/cli-volume"))
            );
            assert_eq!(cli.expected_volume_uuid.as_deref(), Some("1234-abcd"));
        }
        let config: Config = toml::from_str(
            "expected_volume_mount = '/file-volume'\nexpected_volume_uuid = 'abcd-1234'\n",
        )
        .unwrap();
        let cli = resolve(&["logex", command], config);
        assert_eq!(
            cli.expected_volume_mount,
            Some(PathBuf::from("/file-volume"))
        );
        assert_eq!(cli.expected_volume_uuid.as_deref(), Some("abcd-1234"));
    }
}

#[test]
fn query_admission_defaults_and_explicit_default_override_config() {
    for (arguments, configured, expected) in [
        (vec!["logex", "sync"], None, 8),
        (vec!["logex", "sync"], Some(3), 3),
        (
            vec!["logex", "sync", "--query-max-concurrent", "8"],
            Some(3),
            8,
        ),
        (
            vec!["logex", "sync", "--query-max-concurrent", "2"],
            Some(3),
            2,
        ),
    ] {
        let cli = resolve(
            &arguments,
            Config {
                query_max_concurrent: configured,
                ..Default::default()
            },
        );
        let limit = super::super::resolved_query_concurrency(&cli.command)
            .unwrap()
            .unwrap();
        assert_eq!(limit.get(), expected);
    }
    let parsed: Config = toml::from_str("query_max_concurrent = 4").unwrap();
    assert_eq!(parsed.query_max_concurrent, Some(4));
}

#[test]
fn invalid_resolved_query_admission_is_rejected_without_creating_data() {
    let temp = tempfile::tempdir().unwrap();
    let missing = temp.path().join("must-not-create");
    for invalid in [0, tokio::sync::Semaphore::MAX_PERMITS + 1, usize::MAX] {
        let cli = resolve(
            &["logex", "sync"],
            Config {
                data_dir: Some(missing.clone()),
                query_max_concurrent: Some(invalid),
                ..Default::default()
            },
        );
        let error = super::super::resolved_query_concurrency(&cli.command)
            .err()
            .unwrap();
        assert!(error.contains("query-max-concurrent"), "{error}");
        assert!(!missing.exists());
        let value = invalid.to_string();
        let cli = resolve(
            &["logex", "sync", "--query-max-concurrent", &value],
            Config {
                data_dir: Some(missing.clone()),
                query_max_concurrent: Some(8),
                ..Default::default()
            },
        );
        assert!(super::super::resolved_query_concurrency(&cli.command).is_err());
        assert!(!missing.exists());
    }
    assert_eq!(std::fs::read_dir(temp.path()).unwrap().count(), 0);
}

#[test]
fn repair_health_only_mode_does_not_apply_query_admission_configuration() {
    let cli = resolve(
        &["logex", "repair", "--dry-run"],
        Config {
            query_max_concurrent: Some(0),
            ..Default::default()
        },
    );
    assert!(
        super::super::resolved_query_concurrency(&cli.command)
            .unwrap()
            .is_none()
    );
    assert!(
        Cli::command()
            .try_get_matches_from(["logex", "repair", "--query-max-concurrent", "2"])
            .is_err()
    );
}
