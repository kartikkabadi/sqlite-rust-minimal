use minisqlite::{CommitBatch, ControlPlaneStore, Event, Id};

fn seeded_store(path: &std::path::Path) -> ControlPlaneStore {
    let store = ControlPlaneStore::open(path).unwrap();
    let batch = CommitBatch::new(Id::from(1u128), 2_000)
        .append_event(Event::with_json_payload(
            Id::from(10u128),
            "s1",
            "created",
            1_000,
            b"{\"a\":1}",
        ))
        .append_event(Event::with_json_payload(
            Id::from(11u128),
            "s1",
            "updated",
            1_001,
            b"{}",
        ))
        .append_event(Event::with_json_payload(
            Id::from(12u128),
            "s2",
            "created",
            1_002,
            b"{}",
        ));
    store.commit(&batch).unwrap();
    store
}

#[test]
fn verify_reports_ok_on_healthy_store() {
    let dir = tempfile::tempdir().unwrap();
    let store = seeded_store(&dir.path().join("db"));
    let report = store.verify().unwrap();
    assert!(report.is_ok(), "unexpected findings: {:?}", report.findings);
}

#[test]
fn verify_reports_ok_on_empty_store() {
    let dir = tempfile::tempdir().unwrap();
    let store = ControlPlaneStore::open(dir.path().join("db")).unwrap();
    assert!(store.verify().unwrap().is_ok());
}

#[test]
fn verify_reports_migration_checksum_mismatch() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    drop(seeded_store(&path));
    let conn = rusqlite::Connection::open(&path).unwrap();
    conn.execute(
        "UPDATE schema_migrations SET checksum = X'00' WHERE version = 1",
        [],
    )
    .unwrap();
    drop(conn);
    // open_existing never migrates, so the mismatch is observable by verify.
    let store = ControlPlaneStore::open_existing(&path).unwrap();
    let report = store.verify().unwrap();
    assert!(report
        .findings
        .iter()
        .any(|f| f.check == "migration_checksums"));
}

#[test]
fn verify_reports_leased_jobs_missing_lease_fields_and_orphaned_receipts() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let store = ControlPlaneStore::open(&path).unwrap();
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
            lease_ms: 60_000,
            limit: 1,
            partition_key: None,
        })
        .unwrap();
    drop(store);
    let conn = rusqlite::Connection::open(&path).unwrap();
    // Simulate corruption that the v2 CHECK constraints would normally reject.
    conn.execute_batch("PRAGMA ignore_check_constraints=ON")
        .unwrap();
    conn.execute("UPDATE jobs SET lease_token = NULL", [])
        .unwrap();
    conn.execute("DELETE FROM jobs", []).unwrap();
    drop(conn);

    let store = ControlPlaneStore::open_existing(&path).unwrap();
    let report = store.verify().unwrap();
    assert!(report
        .findings
        .iter()
        .any(|f| f.check == "claim_receipts" && f.detail.contains("missing job")));

    // Rebuild just the lease-field corruption for the leased_jobs check.
    let dir2 = tempfile::tempdir().unwrap();
    let path2 = dir2.path().join("db");
    let store2 = ControlPlaneStore::open(&path2).unwrap();
    store2
        .commit(&CommitBatch::new(Id::from(1u128), 1_000).enqueue_job(
            minisqlite::JobSpec::reconcilable(Id::from(10u128), "q", "p", vec![]),
        ))
        .unwrap();
    store2
        .claim_jobs(&minisqlite::ClaimRequest {
            queue: "q".into(),
            worker_id: "w".into(),
            now_ms: 2_000,
            lease_ms: 60_000,
            limit: 1,
            partition_key: None,
        })
        .unwrap();
    drop(store2);
    let conn2 = rusqlite::Connection::open(&path2).unwrap();
    conn2
        .execute_batch("PRAGMA ignore_check_constraints=ON")
        .unwrap();
    conn2
        .execute("UPDATE jobs SET lease_token = NULL", [])
        .unwrap();
    drop(conn2);
    let store2 = ControlPlaneStore::open_existing(&path2).unwrap();
    let report2 = store2.verify().unwrap();
    assert!(report2.findings.iter().any(|f| f.check == "leased_jobs"));
}

#[test]
fn diagnostic_export_redacts_error_summary_by_default() {
    let dir = tempfile::tempdir().unwrap();
    let store = ControlPlaneStore::open(dir.path().join("db")).unwrap();
    store
        .commit(&CommitBatch::new(Id::from(1u128), 1_000).enqueue_job(
            minisqlite::JobSpec::reconcilable(Id::from(10u128), "q", "p", vec![]),
        ))
        .unwrap();
    let outcome = store
        .claim_jobs(&minisqlite::ClaimRequest {
            queue: "q".into(),
            worker_id: "w".into(),
            now_ms: 2_000,
            lease_ms: 60_000,
            limit: 1,
            partition_key: None,
        })
        .unwrap();
    let claims = match outcome {
        minisqlite::ClaimOutcome::Committed(claims) => claims.into_jobs(),
        other => panic!("expected committed claims, got {other:?}"),
    };
    store
        .commit(&CommitBatch::new(Id::from(2u128), 3_000).fail_job(
            claims[0].job_id,
            claims[0].lease_token,
            "secret token leaked: hunter2",
            None,
        ))
        .unwrap();

    let export = store.diagnostic_export().unwrap();
    assert!(!export.contains("hunter2"));
    assert!(!export.contains("\"error_summary\""));
    assert!(export.contains("\"error_summary_len\":28"));

    let full = store.diagnostic_export_with(true).unwrap();
    assert!(full.contains("hunter2"));
}

#[test]
fn stats_counts_are_correct() {
    let dir = tempfile::tempdir().unwrap();
    let store = seeded_store(&dir.path().join("db"));
    let stats = store.stats().unwrap();
    assert_eq!(stats.transactions, 1);
    assert_eq!(stats.events, 3);
    assert_eq!(stats.streams, 2);
    assert_eq!(stats.projections, 0);
    assert_eq!(stats.projection_entries, 0);
    assert!(stats.jobs_by_state.is_empty());
    assert_eq!(stats.active_partitions, 0);
    assert_eq!(stats.migration_version, 2);
    assert!(stats.file_size_bytes > 0);
    assert_eq!(stats.oldest_active_lease_ms, None);
    assert_eq!(stats.oldest_uncertain_job_ms, None);
}

#[test]
fn stats_report_oldest_uncertain_job() {
    let dir = tempfile::tempdir().unwrap();
    let store = ControlPlaneStore::open(dir.path().join("db")).unwrap();
    store
        .commit(&CommitBatch::new(Id::from(1u128), 1_000).enqueue_job(
            minisqlite::JobSpec::reconcilable(Id::from(10u128), "q", "p", vec![]),
        ))
        .unwrap();
    let request = |now_ms| minisqlite::ClaimRequest {
        queue: "q".into(),
        worker_id: "w".into(),
        now_ms,
        lease_ms: 60_000,
        limit: 1,
        partition_key: None,
    };
    store.claim_jobs(&request(2_000)).unwrap();
    // Expiry maintenance moves the reconcilable job to Uncertain at 70_000.
    store.claim_jobs(&request(70_000)).unwrap();
    let stats = store.stats().unwrap();
    assert_eq!(stats.oldest_uncertain_job_ms, Some(70_000));
}

#[test]
fn diagnostic_export_redacts_payloads_by_default() {
    let dir = tempfile::tempdir().unwrap();
    let store = seeded_store(&dir.path().join("db"));
    let export = store.diagnostic_export().unwrap();
    let first = export.lines().next().unwrap();
    assert!(first.contains("\"kind\":\"header\""));
    assert!(first.contains("\"restorable\":false"));
    assert!(first.contains("\"schema_version\":2"));
    assert!(first.contains("\"payloads_included\":false"));
    assert!(export.contains("\"payload_len\":7"));
    assert!(!export.contains("payload_hex"));
    assert!(!export.contains("lease_token"));
    // One line per record: header, stats, 1 transaction, 3 events, 2 streams.
    assert_eq!(export.lines().count(), 8);
}

#[test]
fn diagnostic_export_can_include_payloads_but_never_lease_tokens() {
    let dir = tempfile::tempdir().unwrap();
    let store = seeded_store(&dir.path().join("db"));
    let export = store.diagnostic_export_with(true).unwrap();
    assert!(export.contains("\"payloads_included\":true"));
    // b"{\"a\":1}" as hex.
    assert!(export.contains("\"payload_hex\":\"7b2261223a317d\""));
    assert!(!export.contains("lease_token"));
}
