//! CLI smoke tests: spawn the real binary against a temp database.

use std::path::Path;
use std::process::{Command, Output};

use minisqlite::{CommitBatch, ControlPlaneStore, Event, Id};

fn seed_db(path: &Path) {
    let store = ControlPlaneStore::open(path).unwrap();
    let batch = CommitBatch::new(Id::from(1u128), 2_000)
        .append_event(Event::with_json_payload(
            Id::from(10u128),
            "s1",
            "created",
            1_000,
            b"{}",
        ))
        .append_event(Event::with_json_payload(
            Id::from(11u128),
            "s1",
            "updated",
            1_001,
            b"{}",
        ));
    store.commit(&batch).unwrap();
}

fn run_cli(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_minisqlite"))
        .args(args)
        .output()
        .unwrap()
}

fn stdout(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned()
}

#[test]
fn doctor_reports_healthy_store() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("db");
    seed_db(&db);
    let output = run_cli(&["doctor", "--db", db.to_str().unwrap()]);
    assert!(output.status.success());
    let out = stdout(&output);
    assert!(out.contains("schema version: 2"));
    assert!(out.contains("verify: ok"));
}

#[test]
fn verify_succeeds_on_healthy_store() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("db");
    seed_db(&db);
    let output = run_cli(&["verify", "--db", db.to_str().unwrap()]);
    assert!(output.status.success());
    assert!(stdout(&output).contains("ok"));
}

#[test]
fn stats_prints_counts() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("db");
    seed_db(&db);
    let output = run_cli(&["stats", "--db", db.to_str().unwrap()]);
    assert!(output.status.success());
    let out = stdout(&output);
    assert!(out.contains("transactions: 1"));
    assert!(out.contains("events: 2"));
}

#[test]
fn events_tail_respects_limit() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("db");
    seed_db(&db);
    let output = run_cli(&[
        "events",
        "tail",
        "--limit",
        "1",
        "--db",
        db.to_str().unwrap(),
    ]);
    assert!(output.status.success());
    let out = stdout(&output);
    assert_eq!(out.lines().count(), 1);
    assert!(out.contains("updated"));
}

#[test]
fn backup_writes_and_refuses_existing_destination() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("db");
    seed_db(&db);
    let dest = dir.path().join("backup.db");
    let dest_str = dest.to_str().unwrap();

    let output = run_cli(&["backup", dest_str, "--db", db.to_str().unwrap()]);
    assert!(output.status.success());
    assert!(stdout(&output).contains("schema version 2"));
    assert!(dest.exists());

    let refused = run_cli(&["backup", dest_str, "--db", db.to_str().unwrap()]);
    assert!(!refused.status.success());

    let overwritten = run_cli(&[
        "backup",
        dest_str,
        "--overwrite",
        "--db",
        db.to_str().unwrap(),
    ]);
    assert!(overwritten.status.success());
}

#[test]
fn diagnostic_export_writes_to_stdout_and_file() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("db");
    seed_db(&db);
    let output = run_cli(&["diagnostic-export", "--db", db.to_str().unwrap()]);
    assert!(output.status.success());
    let out = stdout(&output);
    assert!(out.contains("\"kind\":\"header\""));
    assert!(!out.contains("payload_hex"));

    let file = dir.path().join("export.jsonl");
    let output = run_cli(&[
        "diagnostic-export",
        "--out",
        file.to_str().unwrap(),
        "--include-payloads",
        "--db",
        db.to_str().unwrap(),
    ]);
    assert!(output.status.success());
    let contents = std::fs::read_to_string(&file).unwrap();
    assert!(contents.contains("\"payloads_included\":true"));
    assert!(contents.contains("payload_hex"));
    assert!(!contents.contains("lease_token"));
}

#[test]
fn migrations_status_lists_versions() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("db");
    seed_db(&db);
    let output = run_cli(&["migrations", "status", "--db", db.to_str().unwrap()]);
    assert!(output.status.success());
    assert!(stdout(&output).contains("v1"));
}

#[test]
fn missing_db_flag_and_unknown_command_fail() {
    let output = run_cli(&["stats"]);
    assert!(!output.status.success());
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("db");
    let output = run_cli(&["bogus", "--db", db.to_str().unwrap()]);
    assert!(!output.status.success());
}

#[test]
fn nonexistent_db_path_errors_without_creating_a_file() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("typo.db");
    let output = run_cli(&["stats", "--db", db.to_str().unwrap()]);
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("does not exist"));
    assert!(!db.exists(), "inspection command created a database file");
}

#[test]
fn verify_and_backup_error_on_nonexistent_db_without_creating_files() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("typo.db");
    let dest = dir.path().join("backup.db");

    let output = run_cli(&["verify", "--db", db.to_str().unwrap()]);
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("does not exist"));
    assert!(!db.exists(), "verify created a database file");

    let output = run_cli(&[
        "backup",
        dest.to_str().unwrap(),
        "--db",
        db.to_str().unwrap(),
    ]);
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("does not exist"));
    assert!(!db.exists(), "backup created a database file");
    assert!(!dest.exists(), "backup of a missing database wrote a file");
}

#[test]
fn unknown_command_creates_no_file() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("db");
    let output = run_cli(&["frobnicate", "--db", db.to_str().unwrap()]);
    assert!(!output.status.success());
    assert!(!db.exists(), "unknown command created a database file");
}

#[test]
fn exit_codes_classify_failures() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("db");
    seed_db(&db);
    let db_str = db.to_str().unwrap();

    // 2: usage errors.
    assert_eq!(
        run_cli(&["frobnicate", "--db", db_str]).status.code(),
        Some(2)
    );
    assert_eq!(
        run_cli(&["events", "stream", "--db", db_str]).status.code(),
        Some(2)
    );
    assert_eq!(
        run_cli(&["jobs", "show", "not-hex", "--db", db_str])
            .status
            .code(),
        Some(2)
    );

    // 4: not found (missing database, missing entities).
    let missing = dir.path().join("missing.db");
    assert_eq!(
        run_cli(&["verify", "--db", missing.to_str().unwrap()])
            .status
            .code(),
        Some(4)
    );
    assert_eq!(
        run_cli(&["events", "stream", "nope", "--db", db_str])
            .status
            .code(),
        Some(4)
    );
    assert_eq!(
        run_cli(&["jobs", "show", &"0".repeat(32), "--db", db_str])
            .status
            .code(),
        Some(4)
    );
    assert_eq!(
        run_cli(&["projections", "get", "p", "00", "--db", db_str])
            .status
            .code(),
        Some(4)
    );

    // 0: success.
    assert_eq!(run_cli(&["verify", "--db", db_str]).status.code(), Some(0));
}

#[test]
fn store_prefixed_commands_are_aliases() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("db");
    seed_db(&db);
    let output = run_cli(&["store", "verify", "--db", db.to_str().unwrap()]);
    assert!(output.status.success());
    assert!(stdout(&output).contains("ok"));
}

#[test]
fn events_stream_prints_one_stream() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("db");
    seed_db(&db);
    let output = run_cli(&["events", "stream", "s1", "--db", db.to_str().unwrap()]);
    assert!(output.status.success());
    let out = stdout(&output);
    assert!(out.contains("created"));
    assert!(out.contains("updated"));

    let from2 = run_cli(&[
        "events",
        "stream",
        "s1",
        "--from",
        "2",
        "--db",
        db.to_str().unwrap(),
    ]);
    assert!(from2.status.success());
    let out2 = stdout(&from2);
    assert!(!out2.contains("created"));
    assert!(out2.contains("updated"));
}

#[test]
fn jobs_show_uncertain_and_resolve_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("db");
    let db_str = db.to_str().unwrap();
    {
        let store = ControlPlaneStore::open(&db).unwrap();
        store
            .commit(&CommitBatch::new(Id::from(1u128), 1_000).enqueue_job(
                minisqlite::JobSpec::reconcilable(Id::from(10u128), "q", "p", vec![]),
            ))
            .unwrap();
        store
            .claim_jobs(&minisqlite::ClaimRequest {
                queue: "q".into(),
                worker_id: "w".into(),
                now_ms: 2_000,
                lease_ms: 1_000,
                limit: 1,
                partition_key: None,
            })
            .unwrap();
        // Expire the lease so maintenance marks the job uncertain.
        while let minisqlite::ClaimOutcome::MaintenanceCommitted(_) = store
            .claim_jobs(&minisqlite::ClaimRequest {
                queue: "q".into(),
                worker_id: "w".into(),
                now_ms: 10_000,
                lease_ms: 1_000,
                limit: 1,
                partition_key: None,
            })
            .unwrap()
        {}
    }
    let job_hex = Id::from(10u128).to_string();

    let show = run_cli(&["jobs", "show", &job_hex, "--db", db_str]);
    assert!(show.status.success());
    assert!(stdout(&show).contains("Uncertain"));

    let uncertain = run_cli(&["jobs", "uncertain", "--db", db_str]);
    assert!(uncertain.status.success());
    assert!(stdout(&uncertain).contains(&job_hex));

    let resolve = run_cli(&["jobs", "resolve", &job_hex, "dead", "--db", db_str]);
    assert!(resolve.status.success(), "{resolve:?}");

    let show = run_cli(&["jobs", "show", &job_hex, "--db", db_str]);
    assert!(stdout(&show).contains("Dead"));
}

#[test]
fn projections_scan_and_get() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("db");
    let db_str = db.to_str().unwrap();
    {
        let store = ControlPlaneStore::open(&db).unwrap();
        let patch = minisqlite::ProjectionPatch {
            projection: "p".into(),
            expected_version: 0,
            new_version: 1,
            mutations: vec![minisqlite::ProjectionMutation::Put {
                key: vec![0xab],
                value: vec![0xcd],
            }],
        };
        store
            .commit(&CommitBatch::new(Id::from(1u128), 1_000).apply_projection_patch(patch))
            .unwrap();
    }

    let scan = run_cli(&["projections", "scan", "p", "--db", db_str]);
    assert!(scan.status.success());
    assert!(stdout(&scan).contains("ab cd"));

    let get = run_cli(&["projections", "get", "p", "ab", "--db", db_str]);
    assert!(get.status.success());
    assert_eq!(stdout(&get).trim(), "cd");

    assert_eq!(
        run_cli(&["projections", "scan", "nope", "--db", db_str])
            .status
            .code(),
        Some(4)
    );
}

#[test]
fn jobs_list_pages_with_after_cursor() {
    use minisqlite::JobSpec;
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("db");
    let store = ControlPlaneStore::open(&db).unwrap();
    for id in 1..=3u128 {
        store
            .commit(&CommitBatch::new(Id::from(100 + id), 2_000).enqueue_job(
                JobSpec::reconcilable(Id::from(id), "q", format!("p{id}"), vec![]),
            ))
            .unwrap();
    }
    drop(store);

    // A full page prints a next-cursor hint.
    let output = run_cli(&["jobs", "list", "--limit", "2", "--db", db.to_str().unwrap()]);
    assert!(output.status.success());
    let out = stdout(&output);
    assert_eq!(out.lines().count(), 3);
    assert!(out.contains("next: --after 2"));

    // Following the hint returns the remaining job with no further hint.
    let output = run_cli(&[
        "jobs",
        "list",
        "--limit",
        "2",
        "--after",
        "2",
        "--db",
        db.to_str().unwrap(),
    ]);
    assert!(output.status.success());
    let out = stdout(&output);
    assert_eq!(out.lines().count(), 1);
    assert!(!out.contains("next:"));
    assert!(out.contains(&Id::from(3u128).to_hex()));
}
