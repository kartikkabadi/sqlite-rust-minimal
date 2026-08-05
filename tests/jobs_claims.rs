//! Claim algorithm tests: round-robin fairness, cursor rules, per-partition head
//! ordering, expired-lease maintenance, lease extension, and claim receipt recovery.

use minisqlite::{
    ClaimOutcome, ClaimRecovery, ClaimRequest, CommitBatch, ControlPlaneStore, Id, JobSpec,
    JobState, LeaseConflict, LeaseError,
};

fn store() -> (tempfile::TempDir, ControlPlaneStore) {
    let dir = tempfile::tempdir().unwrap();
    let store = ControlPlaneStore::open(dir.path().join("db")).unwrap();
    (dir, store)
}

fn txid() -> Id {
    Id::new().unwrap()
}

fn enqueue(store: &ControlPlaneStore, id: u128, partition: &str) {
    store
        .commit(
            &CommitBatch::new(txid(), 1_000).enqueue_job(JobSpec::reconcilable(
                Id::from(id),
                "q",
                partition,
                vec![],
            )),
        )
        .unwrap();
}

fn request(now_ms: i64, limit: usize) -> ClaimRequest {
    ClaimRequest {
        queue: "q".into(),
        worker_id: "w1".into(),
        now_ms,
        lease_ms: 10_000,
        limit,
        partition_key: None,
    }
}

fn claim(store: &ControlPlaneStore, now_ms: i64, limit: usize) -> Vec<minisqlite::ClaimedJob> {
    match store.claim_jobs(&request(now_ms, limit)).unwrap() {
        ClaimOutcome::Committed(claims) => claims.into_jobs(),
        other => panic!("expected committed claims, got {other:?}"),
    }
}

fn ack(store: &ControlPlaneStore, job: &minisqlite::ClaimedJob, at_ms: i64) {
    store
        .commit(&CommitBatch::new(txid(), at_ms).acknowledge_job(job.job_id, job.lease_token, None))
        .unwrap();
}

#[test]
fn round_robin_rotates_across_partitions() {
    let (_dir, store) = store();
    for (id, partition) in [(1, "a"), (2, "b"), (3, "c"), (4, "a"), (5, "b"), (6, "c")] {
        enqueue(&store, id, partition);
    }
    let mut order = Vec::new();
    for _ in 0..3 {
        let jobs = claim(&store, 2_000, 1);
        assert_eq!(jobs.len(), 1);
        ack(&store, &jobs[0], 2_500);
        order.push(jobs[0].partition_key.clone());
    }
    assert_eq!(order, ["a", "b", "c"]);
    // Wraps around after the last partition.
    let jobs = claim(&store, 3_000, 1);
    assert_eq!(jobs[0].partition_key, "a");
    assert_eq!(jobs[0].job_id, Id::from(4u128));
}

#[test]
fn claim_takes_at_most_one_head_job_per_partition() {
    let (_dir, store) = store();
    enqueue(&store, 1, "a");
    enqueue(&store, 2, "a");
    enqueue(&store, 3, "b");
    let jobs = claim(&store, 2_000, 10);
    assert_eq!(jobs.len(), 2);
    let ids: Vec<Id> = jobs.iter().map(|j| j.job_id).collect();
    assert!(ids.contains(&Id::from(1u128)));
    assert!(ids.contains(&Id::from(3u128)));
    // Job 2 waits behind job 1 in partition a.
    assert_eq!(
        store.job(Id::from(2u128)).unwrap().unwrap().state,
        JobState::Pending
    );
}

#[test]
fn head_job_follows_enqueue_order() {
    let (_dir, store) = store();
    enqueue(&store, 1, "a");
    enqueue(&store, 2, "a");
    let first = claim(&store, 2_000, 1);
    assert_eq!(first[0].job_id, Id::from(1u128));
    // The second job is blocked until the head is terminal.
    assert_eq!(
        store.claim_jobs(&request(2_100, 1)).unwrap(),
        ClaimOutcome::Noop
    );
    ack(&store, &first[0], 2_500);
    let second = claim(&store, 3_000, 1);
    assert_eq!(second[0].job_id, Id::from(2u128));
}

#[test]
fn out_of_order_acks_do_not_rewind_cursor() {
    let (_dir, store) = store();
    for (id, partition) in [(1, "a"), (2, "b"), (3, "c"), (4, "a"), (5, "b"), (6, "c")] {
        enqueue(&store, id, partition);
    }
    let first = claim(&store, 2_000, 1); // leases from a; cursor at a
    let second = claim(&store, 2_000, 1); // leases from b; cursor at b
    assert_eq!(first[0].partition_key, "a");
    assert_eq!(second[0].partition_key, "b");
    // Acks arrive out of order; the cursor stays at b.
    ack(&store, &second[0], 2_500);
    ack(&store, &first[0], 2_600);
    let third = claim(&store, 3_000, 1);
    assert_eq!(third[0].partition_key, "c");
}

#[test]
fn noop_when_nothing_ready_and_no_maintenance() {
    let (_dir, store) = store();
    assert_eq!(
        store.claim_jobs(&request(1_000, 1)).unwrap(),
        ClaimOutcome::Noop
    );
    // A job scheduled in the future is not ready.
    store
        .commit(&CommitBatch::new(txid(), 1_000).enqueue_job(
            JobSpec::reconcilable(Id::from(1u128), "q", "a", vec![]).with_not_before_ms(9_000),
        ))
        .unwrap();
    assert_eq!(
        store.claim_jobs(&request(2_000, 1)).unwrap(),
        ClaimOutcome::Noop
    );
    assert_eq!(claim(&store, 9_000, 1)[0].job_id, Id::from(1u128));
}

#[test]
fn expired_reconcilable_lease_becomes_uncertain() {
    let (_dir, store) = store();
    enqueue(&store, 1, "a");
    claim(&store, 2_000, 1);
    let outcome = store.claim_jobs(&request(20_000, 1)).unwrap();
    assert!(matches!(outcome, ClaimOutcome::MaintenanceCommitted(_)));
    assert_eq!(
        store.job(Id::from(1u128)).unwrap().unwrap().state,
        JobState::Uncertain
    );
    // Uncertain jobs are never re-leased.
    assert_eq!(
        store.claim_jobs(&request(21_000, 1)).unwrap(),
        ClaimOutcome::Noop
    );
}

#[test]
fn expired_idempotent_lease_retries_until_max_attempts() {
    let (_dir, store) = store();
    store
        .commit(
            &CommitBatch::new(txid(), 1_000).enqueue_job(
                JobSpec::intrinsically_idempotent(Id::from(1u128), "q", "a", vec![])
                    .with_max_attempts(2),
            ),
        )
        .unwrap();
    let first = claim(&store, 2_000, 1);
    assert_eq!(first[0].attempt, 1);
    // Expiry with attempts remaining returns the job to Pending, and the same call
    // does not re-lease it (head selection uses pre-maintenance state).
    let outcome = store.claim_jobs(&request(20_000, 1)).unwrap();
    assert!(matches!(outcome, ClaimOutcome::MaintenanceCommitted(_)));
    assert_eq!(
        store.job(Id::from(1u128)).unwrap().unwrap().state,
        JobState::Pending
    );
    let second = claim(&store, 21_000, 1);
    assert_eq!(second[0].attempt, 2);
    // Final-attempt expiry moves the job to Dead.
    let outcome = store.claim_jobs(&request(40_000, 1)).unwrap();
    assert!(matches!(outcome, ClaimOutcome::MaintenanceCommitted(_)));
    assert_eq!(
        store.job(Id::from(1u128)).unwrap().unwrap().state,
        JobState::Dead
    );
}

#[test]
fn maintenance_only_claim_with_ready_second_job_returns_maintenance_committed() {
    // Regression: an expired final-attempt head plus a ready second job must return
    // MaintenanceCommitted, not lease the second job in the same call.
    let (_dir, store) = store();
    store
        .commit(
            &CommitBatch::new(txid(), 1_000).enqueue_job(
                JobSpec::intrinsically_idempotent(Id::from(1u128), "q", "a", vec![])
                    .with_max_attempts(1),
            ),
        )
        .unwrap();
    enqueue(&store, 2, "a");
    claim(&store, 2_000, 1);
    let outcome = store.claim_jobs(&request(20_000, 10)).unwrap();
    assert!(matches!(outcome, ClaimOutcome::MaintenanceCommitted(_)));
    assert_eq!(
        store.job(Id::from(1u128)).unwrap().unwrap().state,
        JobState::Dead
    );
    // The next call leases the now-unblocked second job.
    let jobs = claim(&store, 21_000, 10);
    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].job_id, Id::from(2u128));
}

#[test]
fn extend_lease_rules() {
    let (_dir, store) = store();
    enqueue(&store, 1, "a");
    let id = Id::from(1u128);
    let claimed = claim(&store, 2_000, 1).remove(0);
    assert_eq!(claimed.lease_expires_at_ms, 12_000);

    // Unknown job.
    assert_eq!(
        store
            .extend_lease(Id::from(9u128), claimed.lease_token, 20_000, 3_000)
            .unwrap_err(),
        LeaseError::Conflict(LeaseConflict::JobNotFound(Id::from(9u128)))
    );
    // Wrong token.
    assert_eq!(
        store
            .extend_lease(id, Id::from(8u128), 20_000, 3_000)
            .unwrap_err(),
        LeaseError::Conflict(LeaseConflict::InvalidToken { job_id: id })
    );
    // Expiry must strictly increase.
    assert_eq!(
        store
            .extend_lease(id, claimed.lease_token, 12_000, 3_000)
            .unwrap_err(),
        LeaseError::Conflict(LeaseConflict::ExpiryNotLater {
            job_id: id,
            current_ms: 12_000,
            requested_ms: 12_000,
        })
    );
    // Success: expiry extended, attempt unchanged, durable.
    let receipt = store
        .extend_lease(id, claimed.lease_token, 20_000, 3_000)
        .unwrap();
    assert_eq!(receipt.attempt(), 1);
    assert_eq!(receipt.lease_expires_at_ms(), 20_000);
    let info = store.job(id).unwrap().unwrap();
    assert_eq!(info.lease_expires_at_ms, Some(20_000));
    assert_eq!(info.attempt, 1);
    // Not leased (terminal) jobs are rejected.
    store
        .commit(&CommitBatch::new(txid(), 4_000).acknowledge_job(id, claimed.lease_token, None))
        .unwrap();
    assert_eq!(
        store
            .extend_lease(id, claimed.lease_token, 30_000, 5_000)
            .unwrap_err(),
        LeaseError::Conflict(LeaseConflict::NotLeased {
            job_id: id,
            state: JobState::Succeeded,
        })
    );
}

#[test]
fn recover_claim_reconstructs_original_lease_tokens() {
    let (_dir, store) = store();
    enqueue(&store, 1, "a");
    enqueue(&store, 2, "b");
    let outcome = store.claim_jobs(&request(2_000, 10)).unwrap();
    let claims = match outcome {
        ClaimOutcome::Committed(claims) => claims,
        other => panic!("expected committed claims, got {other:?}"),
    };
    let recovered = store.recover_claim(claims.transaction_id(), 2_000).unwrap();
    match recovered {
        ClaimRecovery::Committed(recovered) => {
            let mut original = claims.jobs().to_vec();
            original.sort_by_key(|j| j.job_id);
            assert_eq!(recovered.jobs(), original.as_slice());
        }
        other => panic!("expected committed recovery, got {other:?}"),
    }
    // Unknown transaction IDs are Absent.
    assert_eq!(
        store.recover_claim(Id::from(77u128), 2_000).unwrap(),
        ClaimRecovery::Absent
    );
}

#[test]
fn historical_terminal_partitions_are_not_scheduled() {
    let (_dir, store) = store();
    enqueue(&store, 1, "a");
    let job = claim(&store, 2_000, 1).remove(0);
    ack(&store, &job, 3_000);
    // Partition a is drained; a fresh partition is claimed directly and no further
    // work exists afterwards.
    enqueue(&store, 2, "b");
    let jobs = claim(&store, 4_000, 10);
    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].partition_key, "b");
    ack(&store, &jobs[0], 5_000);
    assert_eq!(
        store.claim_jobs(&request(6_000, 10)).unwrap(),
        ClaimOutcome::Noop
    );
    // A drained partition is re-activated by a new enqueue.
    enqueue(&store, 3, "a");
    assert_eq!(claim(&store, 7_000, 10)[0].partition_key, "a");
}

#[test]
fn claim_request_validation() {
    let (_dir, store) = store();
    for bad in [
        ClaimRequest {
            queue: String::new(),
            worker_id: "w".into(),
            now_ms: 0,
            lease_ms: 1,
            limit: 1,
            partition_key: None,
        },
        ClaimRequest {
            queue: "q".into(),
            worker_id: String::new(),
            now_ms: 0,
            lease_ms: 1,
            limit: 1,
            partition_key: None,
        },
        ClaimRequest {
            queue: "q".into(),
            worker_id: "w".into(),
            now_ms: 0,
            lease_ms: 0,
            limit: 1,
            partition_key: None,
        },
    ] {
        assert!(matches!(
            store.claim_jobs(&bad).unwrap_err(),
            minisqlite::ClaimError::Validation(_)
        ));
    }
}

#[test]
fn lease_at_exact_expiry_is_current_everywhere() {
    // Expired iff lease_expires_at_ms < now_ms: at now == expiry the lease is
    // NOT reclaimed by maintenance, IS extendable, and IS recoverable.
    let (_dir, store) = store();
    enqueue(&store, 1, "a");
    let id = Id::from(1u128);
    let outcome = store.claim_jobs(&request(2_000, 1)).unwrap();
    let claims = match outcome {
        ClaimOutcome::Committed(claims) => claims,
        other => panic!("expected committed claims, got {other:?}"),
    };
    let claimed = claims.jobs()[0].clone();
    let expiry = claimed.lease_expires_at_ms;

    // Maintenance at now == expiry does not reclaim the lease.
    assert_eq!(
        store.claim_jobs(&request(expiry, 10)).unwrap(),
        ClaimOutcome::Noop
    );
    assert_eq!(store.job(id).unwrap().unwrap().state, JobState::Leased);

    // Recovery at now == expiry still returns the lease as current.
    match store
        .recover_claim(claims.transaction_id(), expiry)
        .unwrap()
    {
        ClaimRecovery::Committed(recovered) => {
            assert_eq!(recovered.jobs().len(), 1);
            assert!(recovered.stale_jobs().is_empty());
        }
        other => panic!("expected committed recovery, got {other:?}"),
    }

    // Extension at now == expiry succeeds.
    let receipt = store
        .extend_lease(id, claimed.lease_token, expiry + 1_000, expiry)
        .unwrap();
    assert_eq!(receipt.lease_expires_at_ms(), expiry + 1_000);

    // One tick past expiry, maintenance reclaims the lease.
    let outcome = store.claim_jobs(&request(expiry + 1_000 + 1, 0)).unwrap();
    assert!(matches!(outcome, ClaimOutcome::MaintenanceCommitted(_)));
    assert_eq!(store.job(id).unwrap().unwrap().state, JobState::Uncertain);
}

fn partition_request(now_ms: i64, limit: usize, partition: &str) -> ClaimRequest {
    ClaimRequest {
        partition_key: Some(partition.into()),
        ..request(now_ms, limit)
    }
}

#[test]
fn partition_filter_claims_only_that_partitions_head() {
    let (_dir, store) = store();
    for (id, partition) in [(1, "a"), (2, "b"), (3, "c"), (4, "b")] {
        enqueue(&store, id, partition);
    }
    let jobs = match store
        .claim_jobs(&partition_request(2_000, 10, "b"))
        .unwrap()
    {
        ClaimOutcome::Committed(claims) => claims.into_jobs(),
        other => panic!("expected committed claims, got {other:?}"),
    };
    // Only partition b's head is leased, even with a large limit.
    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].job_id, Id::from(2u128));
    assert_eq!(jobs[0].partition_key, "b");
    // Other partitions are untouched: still Pending with zero attempts.
    for id in [1u128, 3, 4] {
        let info = store.job(Id::from(id)).unwrap().unwrap();
        assert_eq!(info.state, JobState::Pending);
        assert_eq!(info.attempt, 0);
    }
}

#[test]
fn partition_filter_misses_are_noop() {
    let (_dir, store) = store();
    enqueue(&store, 1, "a");
    // Unknown partition, and a partition whose head is already leased.
    assert_eq!(
        store
            .claim_jobs(&partition_request(2_000, 1, "zz"))
            .unwrap(),
        ClaimOutcome::Noop
    );
    claim(&store, 2_000, 1);
    assert_eq!(
        store.claim_jobs(&partition_request(3_000, 1, "a")).unwrap(),
        ClaimOutcome::Noop
    );
}

#[test]
fn partition_filter_does_not_advance_round_robin_cursor() {
    let (_dir, store) = store();
    for (id, partition) in [(1, "a"), (2, "b"), (3, "c")] {
        enqueue(&store, id, partition);
    }
    let targeted = match store.claim_jobs(&partition_request(2_000, 1, "b")).unwrap() {
        ClaimOutcome::Committed(claims) => claims.into_jobs(),
        other => panic!("expected committed claims, got {other:?}"),
    };
    assert_eq!(targeted[0].partition_key, "b");
    // The next queue-wide claim still starts from partition a.
    assert_eq!(claim(&store, 3_000, 1)[0].partition_key, "a");
}

#[test]
fn partition_filter_still_runs_queue_wide_maintenance() {
    let (_dir, store) = store();
    enqueue(&store, 1, "a");
    enqueue(&store, 2, "b");
    claim(&store, 2_000, 10); // lease both
                              // A targeted claim of an empty partition after expiry still repairs both
                              // expired leases queue-wide.
    let outcome = store
        .claim_jobs(&partition_request(20_000, 1, "zz"))
        .unwrap();
    assert!(matches!(outcome, ClaimOutcome::MaintenanceCommitted(_)));
    for id in [1u128, 2] {
        assert_eq!(
            store.job(Id::from(id)).unwrap().unwrap().state,
            JobState::Uncertain
        );
    }
}

#[test]
fn empty_partition_filter_is_rejected() {
    let (_dir, store) = store();
    assert!(matches!(
        store
            .claim_jobs(&partition_request(2_000, 1, ""))
            .unwrap_err(),
        minisqlite::ClaimError::Validation(_)
    ));
}

#[test]
fn jobs_page_paginates_by_enqueue_sequence() {
    let (_dir, store) = store();
    for id in 1..=5u128 {
        enqueue(&store, id, "a");
    }
    let (first, cursor) = store.jobs_page(Some("q"), None, 0, 2).unwrap();
    assert_eq!(first.len(), 2);
    assert_eq!(first[0].job_id, Id::from(1u128));
    assert_eq!(first[1].job_id, Id::from(2u128));
    let (second, cursor) = store.jobs_page(Some("q"), None, cursor, 2).unwrap();
    assert_eq!(second.len(), 2);
    assert_eq!(second[0].job_id, Id::from(3u128));
    let (last, cursor) = store.jobs_page(Some("q"), None, cursor, 2).unwrap();
    assert_eq!(last.len(), 1);
    assert_eq!(last[0].job_id, Id::from(5u128));
    // An exhausted cursor returns an empty page and an unchanged cursor.
    let (empty, done) = store.jobs_page(Some("q"), None, cursor, 2).unwrap();
    assert!(empty.is_empty());
    assert_eq!(done, cursor);
    // State filters apply within the page.
    let (pending, _) = store
        .jobs_page(Some("q"), Some(JobState::Leased), 0, 10)
        .unwrap();
    assert!(pending.is_empty());
}
