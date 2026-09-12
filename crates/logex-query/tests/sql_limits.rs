//! Untrusted SQL must fail explicitly rather than aborting the whole process.
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use logex_query::execute_sql;
use logex_storage::{PartitionManager, PartitionManagerConfig};
use serde_json::json;

#[test]
fn complex_sql_is_bounded_in_child_processes() {
    for case in [
        "additions",
        "boolean_chain",
        "nested_unary",
        "nested_queries",
        "casts",
        "null_tests",
        "unions",
        "functions",
        "parentheses",
        "implicit_joins",
        "text_size",
        "rewritten_size",
        "boundary_additions",
        "boundary_joins",
        "boundary_booleans",
        "boundary_casts",
        "boundary_null_tests",
        "literal_list",
        "literal_and_comment",
        "text_boundary",
        "generated_expressions",
    ] {
        let output = tempfile::NamedTempFile::new().unwrap();
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args(["sql_limit_child", "--exact", "--ignored", "--nocapture"])
            .env("LOGEX_SQL_LIMIT_CASE", case)
            .stdin(Stdio::null())
            .stdout(output.as_file().try_clone().unwrap())
            .stderr(output.as_file().try_clone().unwrap())
            .spawn()
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(30);
        let status = loop {
            if let Some(status) = child.try_wait().unwrap() {
                break status;
            }
            if Instant::now() >= deadline {
                child.kill().unwrap();
                let _ = child.wait();
                panic!("SQL child did not terminate for {case}");
            }
            std::thread::sleep(Duration::from_millis(10));
        };
        assert!(
            status.success(),
            "{case}: {status}\n{}",
            std::fs::read_to_string(output.path()).unwrap()
        );
    }
}

#[tokio::test]
#[ignore = "subprocess entry invoked by complex_sql_is_bounded_in_child_processes"]
async fn sql_limit_child() {
    let case = std::env::var("LOGEX_SQL_LIMIT_CASE").unwrap();
    let repeated = |value: &str, count: usize, separator: &str| vec![value; count].join(separator);
    let sql = match case.as_str() {
        "additions" => format!("SELECT {} AS value", repeated("1", 1500, " + ")),
        "boolean_chain" => format!("SELECT {} AS value", repeated("true", 1500, " AND ")),
        "nested_unary" => format!("SELECT {}true AS value", "NOT ".repeat(52)),
        "nested_queries" => {
            let mut value = "1".to_owned();
            for _ in 0..20 {
                value = format!("(SELECT {value})");
            }
            format!("SELECT {value} AS value")
        }
        "boundary_booleans" => format!("SELECT {} AS value", repeated("true", 126, " AND ")),
        "boundary_casts" => format!("SELECT 1{} AS value", "::BIGINT".repeat(62)),
        "boundary_null_tests" => format!("SELECT NULL{} AS value", " IS NULL".repeat(125)),
        "casts" => format!("SELECT 1{} AS value", "::BIGINT".repeat(1500)),
        "null_tests" => format!("SELECT NULL{} AS value", " IS NULL".repeat(1500)),
        "unions" => repeated("SELECT 1 AS value", 1000, " UNION ALL "),
        "functions" => format!(
            "SELECT {}1{} AS value",
            "abs(".repeat(1000),
            ")".repeat(1000)
        ),
        "parentheses" => format!("SELECT {}1{} AS value", "(".repeat(1000), ")".repeat(1000)),
        "implicit_joins" => format!(
            "SELECT COUNT(*) AS value FROM {}",
            (0..1000)
                .map(|i| format!("logs l{i}"))
                .collect::<Vec<_>>()
                .join(",")
        ),
        "boundary_joins" => format!(
            "SELECT COUNT(*) AS value FROM {}",
            (0..60)
                .map(|i| format!("logs l{i}"))
                .collect::<Vec<_>>()
                .join(",")
        ),
        "text_size" => format!("SELECT '{}' AS value", "x".repeat(256 * 1024)),
        "rewritten_size" => format!(
            "SELECT 1 WHERE 'x' IN ({})",
            repeated("event'x'", 4096, ",")
        ),
        // SELECT/AS/value plus 125 operators consume exactly 128 syntax tokens.
        "boundary_additions" => format!("SELECT {} AS value", repeated("1", 126, " + ")),
        "literal_list" => format!(
            "SELECT 1 IN ({}) AS value",
            (0..10_000)
                .map(|n| n.to_string())
                .collect::<Vec<_>>()
                .join(",")
        ),
        "generated_expressions" => "SELECT 7 AS value".to_owned(),
        "text_boundary" => {
            let prefix = "SELECT 1 AS value /*";
            format!("{prefix}{}*/", " ".repeat(256 * 1024 - prefix.len() - 2))
        }
        "literal_and_comment" => format!(
            "SELECT '{}' AS value /* {} */",
            "+".repeat(5000),
            "AND (".repeat(5000)
        ),
        _ => panic!("unknown child case"),
    };
    let tmp = tempfile::tempdir().unwrap();
    let storage = PartitionManager::open(PartitionManagerConfig {
        data_dir: tmp.path().to_owned(),
        partition_target_rows: 1000,
        compaction_safety_margin_blocks: 0,
    })
    .unwrap();
    let result = execute_sql(&sql, &storage, storage.head_block()).await;
    match case.as_str() {
        "generated_expressions" => {
            assert_eq!(result.unwrap().rows, vec![json!({"value":7})]);
            exercise_generated_expressions(&storage).await;
        }
        "boundary_booleans" => assert_eq!(result.unwrap().rows, vec![json!({"value":true})]),
        "boundary_casts" => assert_eq!(result.unwrap().rows, vec![json!({"value":1})]),
        "boundary_null_tests" => assert_eq!(result.unwrap().rows, vec![json!({"value":false})]),
        "boundary_joins" => assert_eq!(result.unwrap().rows, vec![json!({"value":0})]),
        "text_boundary" => assert_eq!(result.unwrap().rows, vec![json!({"value":1})]),
        "boundary_additions" => assert_eq!(result.unwrap().rows, vec![json!({"value":126})]),
        "literal_list" => assert_eq!(result.unwrap().rows, vec![json!({"value":true})]),
        "literal_and_comment" => assert_eq!(
            result.unwrap().rows,
            vec![json!({"value":"+".repeat(5000)})]
        ),
        _ => {
            let error = result
                .expect_err("complex SQL must be rejected")
                .to_string();
            assert!(
                error.contains("limit"),
                "expected explicit limit error, got {error}"
            );
        }
    }
    // Rejection must leave the caller and subsequent queries usable.
    assert_eq!(
        execute_sql("SELECT 7 AS value", &storage, storage.head_block())
            .await
            .unwrap()
            .rows,
        vec![json!({"value":7})]
    );
}

// Deterministic valid/invalid expressions exercise several ways syntax can
// become deep. Small expressions must retain exact results; larger ones may
// return an explicit parser/limit error, but must not panic or abort.
async fn exercise_generated_expressions(storage: &PartitionManager) {
    let mut seed = 0x3d90_c84e_u32;
    for index in 0..144 {
        seed ^= seed << 13;
        seed ^= seed >> 17;
        seed ^= seed << 5;
        let count = if index < 24 {
            index / 6 + 1
        } else {
            (seed % 400 + 1) as usize
        };
        let (sql, expected) = match index % 6 {
            0 => (
                format!("SELECT {} AS value", vec!["1"; count].join(" + ")),
                json!(count),
            ),
            1 => (
                format!("SELECT {} AS value", vec!["true"; count].join(" AND ")),
                json!(true),
            ),
            2 => (
                format!("SELECT 1{} AS value", "::BIGINT".repeat(count)),
                json!(1),
            ),
            3 => (
                format!("SELECT NULL{} AS value", " IS NULL".repeat(count)),
                json!(count == 1),
            ),
            4 => (
                format!("SELECT {}true AS value", "NOT ".repeat(count)),
                json!(count % 2 == 0),
            ),
            _ => (
                format!(
                    "SELECT {}1{} AS value",
                    "abs(".repeat(count),
                    ")".repeat(count)
                ),
                json!(1),
            ),
        };
        eprintln!("generated shape={} count={count}", index % 6);
        match execute_sql(&sql, storage, storage.head_block()).await {
            Ok(result) => assert_eq!(
                result.rows,
                vec![json!({"value":expected})],
                "shape={}, count={count}",
                index % 6
            ),
            Err(_) => assert!(count > 4, "small valid expression rejected: {sql}"),
        }
        // An incomplete final expression is invalid even if an earlier prefix
        // was valid. It must be rejected before any successful partial result.
        let invalid = format!("{sql} + (");
        assert!(
            execute_sql(&invalid, storage, storage.head_block())
                .await
                .is_err()
        );
    }
}
