//! Job state-machine transition tests: the full valid/invalid matrix through the
//! commit pipeline, plus lease-token and duplicate-enqueue rules.

use minisqlite::{
    ClaimOutcome, CommitBatch, CommitError, Conflict, ControlPlaneStore, Id, JobSpec, JobState,
    Resolution,
};

fn store() -> (tempfile::TempDir, ControlPlaneStore) {
    let dir = tempfile::tempdir().unwrap();
    let store = ControlPlaneStore::open(dir.path().join("db")).unwrap();
    (dir, store)
}

fn txid() -> Id {
    Id::new().unwrap()
}

fn enqueue(store: &ControlPlaneStore, spec: JobSpec) {
    store
        .commit(&CommitBatch::new(txid(), 1_000).enqueue_job(spec))
        .unwrap();
}

fn claim_one(store: &ControlPlaneStore, queue: &str, now_ms: i64) -> minisqlite::ClaimedJob {
    match store
        .claim_jobs(&minisqlite::ClaimRequest {
            queue: queue.into(),
            worker_id: "w1".into(),
            now_ms,
            lease_ms: 10_000,
            limit: 1,
            partition_key: None,
        })
        .unwrap()
    {
        ClaimOutcome::Committed(claims) => claims.into_jobs().remove(0),
        other => panic!("expected a committed claim, got {other:?}"),
    }
}

#[test]
fn enqueue_creates_pending_job() {
    let (_dir, store) = store();
    let id = Id::from(1u128);
    enqueue(&store, JobSpec::reconcilable(id, "q", "p", b"x".to_vec()));
    let info = store.job(id).unwrap().unwrap();
    assert_eq!(info.state, JobState::Pending);
    assert_eq!(info.attempt, 0);
    assert_eq!(info.spec.payload(), b"x".to_vec());
}

#[test]
fn duplicate_enqueue_in_new_transaction_is_rejected() {
    let (_dir, store) = store();
    let id = Id::from(1u128);
    enqueue(&store, JobSpec::reconcilable(id, "q", "p", vec![]));
    let err = store
        .commit(
            &CommitBatch::new(txid(), 1_001).enqueue_job(JobSpec::reconcilable(
                id,
                "q",
                "p",
                vec![],
            )),
        )
        .unwrap_err();
    assert!(matches!(err, CommitError::Validation(_)));
}

#[test]
fn lease_then_ack_succeeds() {
    let (_dir, store) = store();
    let id = Id::from(1u128);
    enqueue(&store, JobSpec::reconcilable(id, "q", "p", vec![]));
    let claimed = claim_one(&store, "q", 2_000);
    assert_eq!(claimed.job_id, id);
    assert_eq!(claimed.attempt, 1);
    assert_eq!(store.job(id).unwrap().unwrap().state, JobState::Leased);

    store
        .commit(&CommitBatch::new(txid(), 3_000).acknowledge_job(
            id,
            claimed.lease_token,
            Some(b"digest".to_vec()),
        ))
        .unwrap();
    let info = store.job(id).unwrap().unwrap();
    assert_eq!(info.state, JobState::Succeeded);
    assert_eq!(info.terminal_at_ms, Some(3_000));
    assert_eq!(info.result_digest, Some(b"digest".to_vec()));
    assert_eq!(info.lease_expires_at_ms, None);
    assert_eq!(info.worker_id, None);
}

#[test]
fn fail_with_attempts_remaining_moves_to_retry_wait() {
    let (_dir, store) = store();
    let id = Id::from(1u128);
    enqueue(
        &store,
        JobSpec::reconcilable(id, "q", "p", vec![]).with_max_attempts(3),
    );
    let claimed = claim_one(&store, "q", 2_000);
    store
        .commit(&CommitBatch::new(txid(), 3_000).fail_job(
            id,
            claimed.lease_token,
            "boom",
            Some(5_000),
        ))
        .unwrap();
    let info = store.job(id).unwrap().unwrap();
    assert_eq!(info.state, JobState::RetryWait);
    assert_eq!(info.attempt, 1);
    assert_eq!(info.retry_after_ms, Some(5_000));
    assert_eq!(info.error_summary, Some("boom".into()));

    // Not claimable before the retry time; claimable after.
    assert_eq!(
        store
            .claim_jobs(&minisqlite::ClaimRequest {
                queue: "q".into(),
                worker_id: "w1".into(),
                now_ms: 4_000,
                lease_ms: 10_000,
                limit: 1,
                partition_key: None,
            })
            .unwrap(),
        ClaimOutcome::Noop
    );
    let reclaimed = claim_one(&store, "q", 5_000);
    assert_eq!(reclaimed.attempt, 2);
}

#[test]
fn fail_at_max_attempts_moves_to_dead() {
    let (_dir, store) = store();
    let id = Id::from(1u128);
    enqueue(
        &store,
        JobSpec::reconcilable(id, "q", "p", vec![]).with_max_attempts(1),
    );
    let claimed = claim_one(&store, "q", 2_000);
    store
        .commit(&CommitBatch::new(txid(), 3_000).fail_job(id, claimed.lease_token, "boom", None))
        .unwrap();
    let info = store.job(id).unwrap().unwrap();
    assert_eq!(info.state, JobState::Dead);
    assert_eq!(info.terminal_at_ms, Some(3_000));
    assert_eq!(info.error_summary, Some("boom".into()));
}

#[test]
fn fail_with_default_retry_near_max_timestamp_is_rejected() {
    let (_dir, store) = store();
    let id = Id::from(1u128);
    enqueue(
        &store,
        JobSpec::reconcilable(id, "q", "p", vec![]).with_max_attempts(3),
    );
    let claimed = claim_one(&store, "q", 2_000);
    let tx = txid();
    let err = store
        .commit(&CommitBatch::new(tx, i64::MAX).fail_job(id, claimed.lease_token, "boom", None))
        .unwrap_err();
    assert!(matches!(err, CommitError::Validation(_)));
    // Nothing was committed and the job is untouched.
    assert!(matches!(
        store.recover_transaction(tx).unwrap(),
        minisqlite::TransactionRecovery::Absent
    ));
    let info = store.job(id).unwrap().unwrap();
    assert_eq!(info.state, JobState::Leased);
    assert_eq!(info.attempt, 1);
    assert_eq!(info.error_summary, None);

    // The largest timestamp whose default retry still fits is accepted.
    let boundary = i64::MAX - 30_000;
    store
        .commit(&CommitBatch::new(txid(), boundary).fail_job(id, claimed.lease_token, "boom", None))
        .unwrap();
    let info = store.job(id).unwrap().unwrap();
    assert_eq!(info.state, JobState::RetryWait);
    assert_eq!(info.retry_after_ms, Some(i64::MAX));
}

#[test]
fn claim_with_lease_expiry_overflow_is_rejected() {
    let (_dir, store) = store();
    let id = Id::from(1u128);
    enqueue(&store, JobSpec::reconcilable(id, "q", "p", vec![]));
    let err = store
        .claim_jobs(&minisqlite::ClaimRequest {
            queue: "q".into(),
            worker_id: "w1".into(),
            now_ms: i64::MAX,
            lease_ms: 10_000,
            limit: 1,
            partition_key: None,
        })
        .unwrap_err();
    assert!(matches!(err, minisqlite::ClaimError::Validation(_)));
    assert_eq!(store.job(id).unwrap().unwrap().state, JobState::Pending);
}

#[test]
fn cancel_leased_job_with_token() {
    let (_dir, store) = store();
    let id = Id::from(1u128);
    enqueue(&store, JobSpec::reconcilable(id, "q", "p", vec![]));
    let claimed = claim_one(&store, "q", 2_000);
    store
        .commit(&CommitBatch::new(txid(), 3_000).cancel_job(id, Some(claimed.lease_token)))
        .unwrap();
    assert_eq!(store.job(id).unwrap().unwrap().state, JobState::Cancelled);
}

#[test]
fn cancel_leased_job_without_token_is_rejected() {
    let (_dir, store) = store();
    let id = Id::from(1u128);
    enqueue(&store, JobSpec::reconcilable(id, "q", "p", vec![]));
    claim_one(&store, "q", 2_000);
    let err = store
        .commit(&CommitBatch::new(txid(), 3_000).cancel_job(id, None))
        .unwrap_err();
    assert!(matches!(err, CommitError::Validation(_)));
}

#[test]
fn cancel_pending_job_without_token_succeeds() {
    let (_dir, store) = store();
    let id = Id::from(1u128);
    enqueue(&store, JobSpec::reconcilable(id, "q", "p", vec![]));
    store
        .commit(&CommitBatch::new(txid(), 3_000).cancel_job(id, None))
        .unwrap();
    assert_eq!(store.job(id).unwrap().unwrap().state, JobState::Cancelled);
}

#[test]
fn cancel_retry_wait_job_is_invalid_transition() {
    let (_dir, store) = store();
    let id = Id::from(1u128);
    enqueue(&store, JobSpec::reconcilable(id, "q", "p", vec![]));
    let claimed = claim_one(&store, "q", 2_000);
    store
        .commit(&CommitBatch::new(txid(), 3_000).fail_job(id, claimed.lease_token, "e", None))
        .unwrap();
    let err = store
        .commit(&CommitBatch::new(txid(), 4_000).cancel_job(id, None))
        .unwrap_err();
    assert_eq!(
        err,
        CommitError::Conflict(Conflict::JobTransition {
            job_id: id,
            from: JobState::RetryWait,
            to: JobState::Cancelled,
        })
    );
}

fn make_uncertain(store: &ControlPlaneStore, id: Id) {
    enqueue(store, JobSpec::reconcilable(id, "q", "p", vec![]));
    claim_one(store, "q", 2_000);
    // The lease expires; maintenance moves the reconcilable job to Uncertain.
    let outcome = store
        .claim_jobs(&minisqlite::ClaimRequest {
            queue: "q".into(),
            worker_id: "w2".into(),
            now_ms: 20_000,
            lease_ms: 10_000,
            limit: 1,
            partition_key: None,
        })
        .unwrap();
    assert!(matches!(outcome, ClaimOutcome::MaintenanceCommitted(_)));
    assert_eq!(store.job(id).unwrap().unwrap().state, JobState::Uncertain);
}

#[test]
fn resolve_uncertain_to_pending() {
    let (_dir, store) = store();
    let id = Id::from(1u128);
    make_uncertain(&store, id);
    store
        .commit(&CommitBatch::new(txid(), 21_000).resolve_uncertain_job(id, Resolution::Retry))
        .unwrap();
    let info = store.job(id).unwrap().unwrap();
    assert_eq!(info.state, JobState::Pending);
    // Retry keeps the attempt count; a new lease increments it.
    assert_eq!(info.attempt, 1);
    assert_eq!(claim_one(&store, "q", 22_000).attempt, 2);
}

#[test]
fn resolve_uncertain_to_succeeded_and_dead() {
    let (_dir, store) = store();
    let id = Id::from(1u128);
    make_uncertain(&store, id);
    store
        .commit(
            &CommitBatch::new(txid(), 21_000).resolve_uncertain_job(id, Resolution::MarkSucceeded),
        )
        .unwrap();
    assert_eq!(store.job(id).unwrap().unwrap().state, JobState::Succeeded);

    let id2 = Id::from(2u128);
    make_uncertain(&store, id2);
    store
        .commit(&CommitBatch::new(txid(), 21_000).resolve_uncertain_job(id2, Resolution::MarkDead))
        .unwrap();
    assert_eq!(store.job(id2).unwrap().unwrap().state, JobState::Dead);
}

#[test]
fn invalid_transitions_are_typed_conflicts() {
    let (_dir, store) = store();
    let id = Id::from(1u128);
    enqueue(&store, JobSpec::reconcilable(id, "q", "p", vec![]));
    let fake = Id::from(99u128);

    // Pending -> Succeeded (ack) is invalid.
    let err = store
        .commit(&CommitBatch::new(txid(), 3_000).acknowledge_job(id, fake, None))
        .unwrap_err();
    assert!(matches!(
        err,
        CommitError::Conflict(Conflict::JobTransition { .. })
    ));

    // Pending -> RetryWait (fail) is invalid.
    let err = store
        .commit(&CommitBatch::new(txid(), 3_000).fail_job(id, fake, "e", None))
        .unwrap_err();
    assert!(matches!(
        err,
        CommitError::Conflict(Conflict::JobTransition { .. })
    ));

    // Pending -> resolve is invalid for all resolutions.
    for resolution in [
        Resolution::Retry,
        Resolution::MarkSucceeded,
        Resolution::MarkDead,
    ] {
        let err = store
            .commit(&CommitBatch::new(txid(), 3_000).resolve_uncertain_job(id, resolution))
            .unwrap_err();
        assert!(matches!(
            err,
            CommitError::Conflict(Conflict::JobTransition { .. })
        ));
    }

    // Terminal states reject everything.
    let claimed = claim_one(&store, "q", 2_000);
    store
        .commit(&CommitBatch::new(txid(), 3_000).acknowledge_job(id, claimed.lease_token, None))
        .unwrap();
    let err = store
        .commit(&CommitBatch::new(txid(), 4_000).cancel_job(id, None))
        .unwrap_err();
    assert!(matches!(
        err,
        CommitError::Conflict(Conflict::JobTransition { .. })
    ));
}

#[test]
fn stale_lease_token_is_rejected() {
    let (_dir, store) = store();
    let id = Id::from(1u128);
    enqueue(&store, JobSpec::reconcilable(id, "q", "p", vec![]));
    let claimed = claim_one(&store, "q", 2_000);
    let stale = Id::from(0xdead_beefu128);
    assert_ne!(stale, claimed.lease_token);

    for batch in [
        CommitBatch::new(txid(), 3_000).acknowledge_job(id, stale, None),
        CommitBatch::new(txid(), 3_000).fail_job(id, stale, "e", None),
        CommitBatch::new(txid(), 3_000).cancel_job(id, Some(stale)),
    ] {
        assert!(matches!(
            store.commit(&batch).unwrap_err(),
            CommitError::Validation(_)
        ));
    }
    // The job is untouched.
    assert_eq!(store.job(id).unwrap().unwrap().state, JobState::Leased);
}

#[test]
fn missing_job_operations_are_validation_errors() {
    let (_dir, store) = store();
    let ghost = Id::from(42u128);
    let err = store
        .commit(&CommitBatch::new(txid(), 1_000).acknowledge_job(ghost, Id::from(1u128), None))
        .unwrap_err();
    assert!(matches!(err, CommitError::Validation(_)));
}

#[test]
fn list_jobs_filters_by_queue_and_state() {
    let (_dir, store) = store();
    enqueue(
        &store,
        JobSpec::reconcilable(Id::from(1u128), "q1", "p", vec![]),
    );
    enqueue(
        &store,
        JobSpec::reconcilable(Id::from(2u128), "q2", "p", vec![]),
    );
    claim_one(&store, "q1", 2_000);

    let all = store.jobs(None, None, 10).unwrap();
    assert_eq!(all.len(), 2);
    assert_eq!(all[0].job_id, Id::from(1u128)); // enqueue order

    let q2 = store.jobs(Some("q2"), None, 10).unwrap();
    assert_eq!(q2.len(), 1);
    assert_eq!(q2[0].job_id, Id::from(2u128));

    let leased = store.jobs(None, Some(JobState::Leased), 10).unwrap();
    assert_eq!(leased.len(), 1);
    assert_eq!(leased[0].job_id, Id::from(1u128));

    let uncertain = store.jobs(None, Some(JobState::Uncertain), 10).unwrap();
    assert!(uncertain.is_empty());
}

#[test]
fn jobs_page_paginates_with_moving_cursor() {
    let (_dir, store) = store();
    for i in 1u128..=5 {
        enqueue(&store, JobSpec::reconcilable(Id::from(i), "q", "p", vec![]));
    }

    let (page, cursor) = store.jobs_page(Some("q"), None, 0, 2).unwrap();
    assert_eq!(page.len(), 2);
    assert_eq!(page[0].job_id, Id::from(1u128));
    assert_eq!(page[1].job_id, Id::from(2u128));

    let (page, cursor) = store.jobs_page(Some("q"), None, cursor, 2).unwrap();
    assert_eq!(page.len(), 2);
    assert_eq!(page[0].job_id, Id::from(3u128));

    let (page, cursor) = store.jobs_page(Some("q"), None, cursor, 2).unwrap();
    assert_eq!(page.len(), 1);
    assert_eq!(page[0].job_id, Id::from(5u128));

    let (page, _) = store.jobs_page(Some("q"), None, cursor, 2).unwrap();
    assert!(page.is_empty());
}

#[test]
fn jobs_page_rejects_out_of_range_cursor() {
    let (_dir, store) = store();
    enqueue(
        &store,
        JobSpec::reconcilable(Id::from(1u128), "q", "p", vec![]),
    );

    let err = store.jobs_page(Some("q"), None, u64::MAX, 2).unwrap_err();
    assert!(matches!(err, minisqlite::Error::Validation(_)), "{err:?}");

    let err = store
        .jobs_page(Some("q"), None, (i64::MAX as u64) + 1, 2)
        .unwrap_err();
    assert!(matches!(err, minisqlite::Error::Validation(_)), "{err:?}");
}
