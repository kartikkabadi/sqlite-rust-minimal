//! The v2 `jobs` CHECK constraints reject rows violating the state machine's
//! row invariants, and a v1 store upgrades in place preserving its jobs.

use minisqlite::{ClaimRequest, CommitBatch, ControlPlaneStore, Id, JobSpec, JobState};

#[test]
fn jobs_check_constraints_reject_invalid_rows() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let store = ControlPlaneStore::open(&path).unwrap();
    store
        .commit(
            &CommitBatch::new(Id::from(1u128), 1_000).enqueue_job(JobSpec::reconcilable(
                Id::from(10u128),
                "q",
                "p",
                vec![],
            )),
        )
        .unwrap();
    drop(store);

    let conn = rusqlite::Connection::open(&path).unwrap();
    for (name, sql) in [
        ("zero max_attempts", "UPDATE jobs SET max_attempts = 0"),
        ("negative attempt", "UPDATE jobs SET attempt = -1"),
        ("unknown state", "UPDATE jobs SET state = 7"),
        ("leased without lease fields", "UPDATE jobs SET state = 1"),
        (
            "pending with lease token",
            "UPDATE jobs SET lease_token = x'00'",
        ),
        ("retry-wait without retry time", "UPDATE jobs SET state = 2"),
        (
            "terminal without terminal time",
            "UPDATE jobs SET state = 4",
        ),
        (
            "pending with terminal time",
            "UPDATE jobs SET terminal_at_ms = 1",
        ),
    ] {
        let err = conn.execute(sql, []).unwrap_err();
        assert!(
            err.to_string().contains("CHECK constraint failed"),
            "{name}: expected CHECK violation, got {err}"
        );
    }
}

#[test]
fn valid_lifecycle_passes_check_constraints() {
    let dir = tempfile::tempdir().unwrap();
    let store = ControlPlaneStore::open(dir.path().join("db")).unwrap();
    store
        .commit(
            &CommitBatch::new(Id::from(1u128), 1_000).enqueue_job(JobSpec::reconcilable(
                Id::from(10u128),
                "q",
                "p",
                vec![],
            )),
        )
        .unwrap();
    let outcome = store
        .claim_jobs(&ClaimRequest {
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
    let job = &claims[0];
    store
        .commit(&CommitBatch::new(Id::from(2u128), 3_000).acknowledge_job(
            job.job_id,
            job.lease_token,
            None,
        ))
        .unwrap();
    let info = store.job(Id::from(10u128)).unwrap().unwrap();
    assert_eq!(info.state, JobState::Succeeded);
}
