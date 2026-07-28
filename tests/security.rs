//! Security tests (spec Part B, Layer 8): default diagnostics leak nothing
//! sensitive, oversized inputs are rejected, migration tampering is detected,
//! and backups refuse unsafe destinations.

mod common;

use common::{id, open_in, temp_dir};
use minisqlite::{
    ClaimOutcome, ClaimRequest, CommitBatch, ControlPlaneStore, Event, JobSpec, Limits,
};

const NOW: i64 = 10_000;

/// The default diagnostic export must not contain lease tokens, payload bytes,
/// or error summaries from an active store.
#[test]
fn default_export_contains_no_secrets() {
    let dir = temp_dir();
    let store = open_in(&dir);
    let secret_payload = b"payload-secret-DEADBEEF".to_vec();
    store
        .commit(
            &CommitBatch::new(id(1), NOW)
                .append_event(Event::with_json_payload(
                    id(10),
                    "s",
                    "t",
                    NOW,
                    b"{\"event-secret\":1}",
                ))
                .enqueue_job(JobSpec::reconcilable(id(20), "q", "p", secret_payload)),
        )
        .unwrap();
    let claimed = match store
        .claim_jobs(&ClaimRequest {
            queue: "q".into(),
            worker_id: "w".into(),
            now_ms: NOW,
            lease_ms: 60_000,
            limit: 1,
            partition_key: None,
        })
        .unwrap()
    {
        ClaimOutcome::Committed(claims) => claims.into_jobs().remove(0),
        other => panic!("expected committed claims, got {other:?}"),
    };
    store
        .commit(&CommitBatch::new(id(2), NOW).fail_job(
            claimed.job_id,
            claimed.lease_token,
            "error-secret-hunter2",
            None,
        ))
        .unwrap();

    let export = store.diagnostic_export().unwrap();
    assert!(!export.contains("DEADBEEF"));
    assert!(!export.contains("event-secret"));
    assert!(!export.contains("hunter2"));
    assert!(!export.contains("lease_token"));
    let token_hex = claimed.lease_token.to_string();
    assert!(!export.contains(&token_hex), "lease token leaked");
}

/// Oversized payloads and metadata are rejected by limits before any write.
#[test]
fn oversized_inputs_are_rejected() {
    let dir = temp_dir();
    let store = ControlPlaneStore::builder(common::db_path(&dir))
        .limits(Limits {
            max_event_payload: 8,
            max_metadata: 8,
            max_job_payload: 8,
            ..Limits::new()
        })
        .open()
        .unwrap();

    let big = vec![0u8; 9];
    let batch = CommitBatch::new(id(1), NOW).append_event(Event::with_json_payload(
        id(10),
        "s",
        "t",
        NOW,
        &big,
    ));
    assert!(store.commit(&batch).is_err());

    let batch = CommitBatch::new(id(2), NOW).with_metadata(big.clone());
    assert!(store.commit(&batch).is_err());

    let batch =
        CommitBatch::new(id(3), NOW).enqueue_job(JobSpec::reconcilable(id(20), "q", "p", big));
    assert!(store.commit(&batch).is_err());

    // Nothing was written.
    assert_eq!(store.stats().unwrap().transactions, 0);
}

/// Tampering with a recorded migration checksum is detected on reopen.
#[test]
fn migration_checksum_tampering_is_detected() {
    let dir = temp_dir();
    let path = common::db_path(&dir);
    drop(ControlPlaneStore::open(&path).unwrap());

    let conn = rusqlite::Connection::open(&path).unwrap();
    conn.execute(
        "UPDATE schema_migrations SET checksum = x'00' WHERE version = 1",
        [],
    )
    .unwrap();
    drop(conn);

    let err = ControlPlaneStore::open(&path).unwrap_err();
    assert!(
        err.to_string().contains("checksum"),
        "expected checksum mismatch, got: {err}"
    );
}

/// Backups refuse to overwrite an existing destination unless told to.
#[test]
fn backup_refuses_existing_destination_by_default() {
    let dir = temp_dir();
    let store = open_in(&dir);
    let dest = dir.path().join("backup.db");
    std::fs::write(&dest, b"precious").unwrap();
    assert!(store.backup(&dest, false).is_err());
    assert_eq!(std::fs::read(&dest).unwrap(), b"precious");
    store.backup(&dest, true).unwrap();
    ControlPlaneStore::open_existing(&dest).unwrap();
}

/// The store never creates world-writable files.
#[cfg(unix)]
#[test]
fn database_files_are_not_world_writable() {
    use std::os::unix::fs::PermissionsExt;
    let dir = temp_dir();
    let path = common::db_path(&dir);
    let store = ControlPlaneStore::open(&path).unwrap();
    store
        .commit(&CommitBatch::new(id(1), NOW).append_event(common::event(10, "s", "t")))
        .unwrap();
    for entry in std::fs::read_dir(dir.path()).unwrap() {
        let entry = entry.unwrap();
        let mode = entry.metadata().unwrap().permissions().mode();
        assert_eq!(
            mode & 0o002,
            0,
            "{} is world-writable (mode {mode:o})",
            entry.path().display()
        );
    }
}
