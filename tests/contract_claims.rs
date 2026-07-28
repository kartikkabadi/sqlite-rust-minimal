//! Contract tests for durable jobs: enqueueing, claiming, leases, retries,
//! uncertainty, resolution, and claim recovery.

mod common;

use common::{db_path, id, open, open_in, temp_dir};
use minisqlite::{
    ClaimOutcome, ClaimRecovery, ClaimRequest, ClaimedJob, CommitBatch, ControlPlaneStore, Id,
    IndeterminateClaim, JobSpec, JobState, LeaseConflict, LeaseError, Resolution,
};

const NOW: i64 = 10_000;
const LEASE_MS: i64 = 1_000;

fn enqueue(store: &ControlPlaneStore, txn: u128, spec: JobSpec) {
    store
        .commit(&CommitBatch::new(id(txn), NOW).enqueue_job(spec))
        .unwrap();
}

fn claim_request(queue: &str, now_ms: i64) -> ClaimRequest {
    ClaimRequest {
        queue: queue.into(),
        worker_id: "w1".into(),
        now_ms,
        lease_ms: LEASE_MS,
        limit: 16,
        partition_key: None,
    }
}

fn claim_all(store: &ControlPlaneStore, queue: &str, now_ms: i64) -> Vec<ClaimedJob> {
    // MaintenanceCommitted means durable progress was made; poll again immediately.
    loop {
        match store.claim_jobs(&claim_request(queue, now_ms)).unwrap() {
            ClaimOutcome::Committed(claims) => return claims.into_jobs(),
            ClaimOutcome::MaintenanceCommitted(_) => continue,
            other => panic!("expected committed claims, got {other:?}"),
        }
    }
}

fn claim_one(store: &ControlPlaneStore, queue: &str, now_ms: i64) -> ClaimedJob {
    let mut jobs = claim_all(store, queue, now_ms);
    assert_eq!(jobs.len(), 1);
    jobs.remove(0)
}

fn job_state(store: &ControlPlaneStore, job_id: Id) -> JobState {
    store.job(job_id).unwrap().expect("job exists").state
}

// ----- enqueue -----

#[test]
fn duplicate_job_id_in_new_transaction_is_rejected() {
    let dir = temp_dir();
    let store = open_in(&dir);
    enqueue(&store, 1, JobSpec::reconcilable(id(10), "q", "p", vec![]));
    let dup = CommitBatch::new(id(2), NOW).enqueue_job(JobSpec::reconcilable(
        id(10),
        "q",
        "p2",
        b"other".to_vec(),
    ));
    assert!(store.commit(&dup).is_err());
    // The original job is untouched and the failed transaction left no trace.
    let info = store.job(id(10)).unwrap().unwrap();
    assert_eq!(info.state, JobState::Pending);
    assert_eq!(info.spec.partition_key(), "p");
}

#[test]
fn enqueue_is_idempotent_on_resubmission() {
    let dir = temp_dir();
    let store = open_in(&dir);
    let batch =
        CommitBatch::new(id(1), NOW).enqueue_job(JobSpec::reconcilable(id(10), "q", "p", vec![]));
    let first = store.commit(&batch).unwrap();
    assert_eq!(store.commit(&batch).unwrap(), first);
    assert_eq!(store.jobs(Some("q"), None, 100).unwrap().len(), 1);
}

// ----- claim outcomes -----

#[test]
fn claim_on_empty_queue_is_noop() {
    let dir = temp_dir();
    let store = open_in(&dir);
    assert_eq!(
        store.claim_jobs(&claim_request("q", NOW)).unwrap(),
        ClaimOutcome::Noop
    );
}

#[test]
fn claim_before_not_before_is_noop() {
    let dir = temp_dir();
    let store = open_in(&dir);
    enqueue(
        &store,
        1,
        JobSpec::reconcilable(id(10), "q", "p", vec![]).with_not_before_ms(NOW + 5_000),
    );
    assert_eq!(
        store.claim_jobs(&claim_request("q", NOW)).unwrap(),
        ClaimOutcome::Noop
    );
    let claimed = claim_one(&store, "q", NOW + 5_000);
    assert_eq!(claimed.job_id, id(10));
}

#[test]
fn claim_grants_committed_claims_with_accessors() {
    let dir = temp_dir();
    let store = open_in(&dir);
    enqueue(
        &store,
        1,
        JobSpec::reconcilable(id(10), "q", "p1", b"a".to_vec()),
    );
    enqueue(
        &store,
        2,
        JobSpec::reconcilable(id(11), "q", "p2", b"b".to_vec()),
    );

    let claims = match store.claim_jobs(&claim_request("q", NOW)).unwrap() {
        ClaimOutcome::Committed(claims) => claims,
        other => panic!("expected committed, got {other:?}"),
    };
    assert_eq!(claims.len(), 2);
    assert!(!claims.is_empty());
    assert_eq!(claims.jobs().len(), 2);
    let by_ref: Vec<&ClaimedJob> = (&claims).into_iter().collect();
    assert_eq!(by_ref.len(), 2);
    for job in claims {
        assert_eq!(job.worker_id, "w1");
        assert_eq!(job.attempt, 1);
        assert_eq!(job.lease_expires_at_ms, NOW + LEASE_MS);
        assert_ne!(job.lease_token, Id::ZERO);
        assert_eq!(job_state(&store, job.job_id), JobState::Leased);
    }
}

#[test]
fn claim_takes_at_most_one_head_job_per_partition() {
    let dir = temp_dir();
    let store = open_in(&dir);
    enqueue(
        &store,
        1,
        JobSpec::reconcilable(id(10), "q", "p", b"first".to_vec()),
    );
    enqueue(
        &store,
        2,
        JobSpec::reconcilable(id(11), "q", "p", b"second".to_vec()),
    );

    let claimed = claim_one(&store, "q", NOW);
    assert_eq!(claimed.job_id, id(10));
    // The second job in the same partition is not claimable while the head is leased.
    assert_eq!(
        store.claim_jobs(&claim_request("q", NOW)).unwrap(),
        ClaimOutcome::Noop
    );
}

// ----- lease-token validation and stale acknowledgement -----

#[test]
fn acknowledgement_requires_current_lease_token() {
    let dir = temp_dir();
    let store = open_in(&dir);
    enqueue(&store, 1, JobSpec::reconcilable(id(10), "q", "p", vec![]));
    let claimed = claim_one(&store, "q", NOW);

    let wrong = CommitBatch::new(id(2), NOW).acknowledge_job(id(10), id(999), None);
    assert!(store.commit(&wrong).is_err());
    assert_eq!(job_state(&store, id(10)), JobState::Leased);

    let right = CommitBatch::new(id(3), NOW).acknowledge_job(id(10), claimed.lease_token, None);
    store.commit(&right).unwrap();
    assert_eq!(job_state(&store, id(10)), JobState::Succeeded);
}

#[test]
fn stale_acknowledgement_after_reclaim_is_rejected() {
    let dir = temp_dir();
    let store = open_in(&dir);
    enqueue(
        &store,
        1,
        JobSpec::idempotent(id(10), "q", "p", vec![], "key").with_max_attempts(5),
    );
    let first = claim_one(&store, "q", NOW);

    // Lease expires; the idempotent job is requeued and reclaimed with a new token.
    let after_expiry = NOW + LEASE_MS + 1;
    let second = claim_one(&store, "q", after_expiry);
    assert_ne!(first.lease_token, second.lease_token);
    assert_eq!(second.attempt, 2);

    // The stale token from the first lease can no longer acknowledge the job.
    let stale =
        CommitBatch::new(id(2), after_expiry).acknowledge_job(id(10), first.lease_token, None);
    assert!(store.commit(&stale).is_err());
    assert_eq!(job_state(&store, id(10)), JobState::Leased);
}

// ----- failure, retries, and max attempts -----

#[test]
fn failure_before_max_attempts_enters_retry_wait_then_dead() {
    let dir = temp_dir();
    let store = open_in(&dir);
    enqueue(
        &store,
        1,
        JobSpec::reconcilable(id(10), "q", "p", vec![]).with_max_attempts(2),
    );

    let first = claim_one(&store, "q", NOW);
    store
        .commit(&CommitBatch::new(id(2), NOW).fail_job(id(10), first.lease_token, "boom", Some(0)))
        .unwrap();
    assert_eq!(job_state(&store, id(10)), JobState::RetryWait);

    let second = claim_one(&store, "q", NOW + 1);
    assert_eq!(second.attempt, 2);
    store
        .commit(&CommitBatch::new(id(3), NOW + 1).fail_job(
            id(10),
            second.lease_token,
            "boom again",
            Some(0),
        ))
        .unwrap();
    // Attempts are exhausted.
    let info = store.job(id(10)).unwrap().unwrap();
    assert_eq!(info.state, JobState::Dead);
    assert!(info.error_summary.is_some());
}

// ----- lease expiry per effect mode -----

#[test]
fn expired_reconcilable_lease_becomes_uncertain() {
    let dir = temp_dir();
    let store = open_in(&dir);
    enqueue(&store, 1, JobSpec::reconcilable(id(10), "q", "p", vec![]));
    claim_one(&store, "q", NOW);

    // Only maintenance happens: the expired lease moves the job to Uncertain and
    // uncertain jobs are not claimable.
    match store
        .claim_jobs(&claim_request("q", NOW + LEASE_MS + 1))
        .unwrap()
    {
        ClaimOutcome::MaintenanceCommitted(_) => {}
        other => panic!("expected maintenance, got {other:?}"),
    }
    assert_eq!(job_state(&store, id(10)), JobState::Uncertain);
}

#[test]
fn expired_idempotent_lease_is_retried() {
    let dir = temp_dir();
    let store = open_in(&dir);
    enqueue(
        &store,
        1,
        JobSpec::idempotent(id(10), "q", "p", vec![], "key").with_max_attempts(5),
    );
    claim_one(&store, "q", NOW);

    let reclaimed = claim_one(&store, "q", NOW + LEASE_MS + 1);
    assert_eq!(reclaimed.job_id, id(10));
    assert_eq!(reclaimed.attempt, 2);
    assert_eq!(job_state(&store, id(10)), JobState::Leased);
}

#[test]
fn expired_idempotent_lease_at_max_attempts_becomes_dead() {
    let dir = temp_dir();
    let store = open_in(&dir);
    enqueue(
        &store,
        1,
        JobSpec::intrinsically_idempotent(id(10), "q", "p", vec![]).with_max_attempts(1),
    );
    claim_one(&store, "q", NOW);

    match store
        .claim_jobs(&claim_request("q", NOW + LEASE_MS + 1))
        .unwrap()
    {
        ClaimOutcome::MaintenanceCommitted(_) => {}
        other => panic!("expected maintenance, got {other:?}"),
    }
    assert_eq!(job_state(&store, id(10)), JobState::Dead);
}

// ----- cancellation -----

#[test]
fn cancellation_of_pending_and_leased_jobs() {
    let dir = temp_dir();
    let store = open_in(&dir);
    enqueue(&store, 1, JobSpec::reconcilable(id(10), "q", "p1", vec![]));
    enqueue(&store, 2, JobSpec::reconcilable(id(11), "q", "p2", vec![]));

    // A pending job may be cancelled without a token (spec A5.6).
    store
        .commit(&CommitBatch::new(id(3), NOW).cancel_job(id(10), None))
        .unwrap();
    assert_eq!(job_state(&store, id(10)), JobState::Cancelled);

    // A leased job requires its current lease token.
    let mut jobs = claim_all(&store, "q", NOW);
    assert_eq!(jobs.len(), 1);
    let claimed = jobs.remove(0);
    assert_eq!(claimed.job_id, id(11));
    assert!(store
        .commit(&CommitBatch::new(id(4), NOW).cancel_job(id(11), None))
        .is_err());
    store
        .commit(&CommitBatch::new(id(5), NOW).cancel_job(id(11), Some(claimed.lease_token)))
        .unwrap();
    assert_eq!(job_state(&store, id(11)), JobState::Cancelled);

    // Terminal jobs cannot be cancelled again.
    assert!(store
        .commit(&CommitBatch::new(id(6), NOW).cancel_job(id(11), None))
        .is_err());
}

// ----- uncertain resolution -----

#[test]
fn uncertain_jobs_resolve_to_retry_succeeded_or_dead() {
    let dir = temp_dir();
    let store = open_in(&dir);
    for (i, job) in [10u128, 11, 12].into_iter().enumerate() {
        enqueue(
            &store,
            1 + i as u128,
            JobSpec::reconcilable(id(job), "q", format!("p{job}"), vec![]).with_max_attempts(5),
        );
    }
    claim_all(&store, "q", NOW);
    // Expire all leases into Uncertain.
    store
        .claim_jobs(&claim_request("q", NOW + LEASE_MS + 1))
        .unwrap();
    for job in [10u128, 11, 12] {
        assert_eq!(job_state(&store, id(job)), JobState::Uncertain);
    }

    store
        .commit(
            &CommitBatch::new(id(100), NOW)
                .resolve_uncertain_job(id(10), Resolution::Retry)
                .resolve_uncertain_job(id(11), Resolution::MarkSucceeded)
                .resolve_uncertain_job(id(12), Resolution::MarkDead),
        )
        .unwrap();
    assert_eq!(job_state(&store, id(10)), JobState::Pending);
    assert_eq!(job_state(&store, id(11)), JobState::Succeeded);
    assert_eq!(job_state(&store, id(12)), JobState::Dead);

    // Resolving a non-uncertain job is an invalid transition.
    assert!(store
        .commit(&CommitBatch::new(id(101), NOW).resolve_uncertain_job(id(11), Resolution::Retry))
        .is_err());
}

#[test]
fn retry_resolution_dead_letters_when_max_attempts_exhausted() {
    let dir = temp_dir();
    let store = open_in(&dir);
    enqueue(
        &store,
        1,
        JobSpec::reconcilable(id(10), "q", "p", vec![]).with_max_attempts(1),
    );
    claim_all(&store, "q", NOW);
    // Expire the lease into Uncertain; the single attempt is spent.
    store
        .claim_jobs(&claim_request("q", NOW + LEASE_MS + 1))
        .unwrap();
    assert_eq!(job_state(&store, id(10)), JobState::Uncertain);

    store
        .commit(&CommitBatch::new(id(100), NOW).resolve_uncertain_job(id(10), Resolution::Retry))
        .unwrap();
    assert_eq!(job_state(&store, id(10)), JobState::Dead);
}

// ----- lease extension -----

#[test]
fn lease_extension_rules() {
    let dir = temp_dir();
    let store = open_in(&dir);
    enqueue(&store, 1, JobSpec::reconcilable(id(10), "q", "p", vec![]));
    let claimed = claim_one(&store, "q", NOW);
    let expiry = claimed.lease_expires_at_ms;

    // Wrong token is rejected.
    assert!(matches!(
        store
            .extend_lease(id(10), id(999), expiry + 1_000, NOW)
            .unwrap_err(),
        LeaseError::Conflict(LeaseConflict::InvalidToken { .. })
    ));

    // The new expiry must be strictly later than the current expiry.
    assert!(matches!(
        store
            .extend_lease(id(10), claimed.lease_token, expiry, NOW)
            .unwrap_err(),
        LeaseError::Conflict(LeaseConflict::ExpiryNotLater { .. })
    ));

    // Unknown jobs are rejected.
    assert!(matches!(
        store
            .extend_lease(id(404), claimed.lease_token, expiry + 1_000, NOW)
            .unwrap_err(),
        LeaseError::Conflict(LeaseConflict::JobNotFound(_))
    ));

    // A valid extension moves the expiry without incrementing the attempt.
    let receipt = store
        .extend_lease(id(10), claimed.lease_token, expiry + 1_000, NOW)
        .unwrap();
    assert_eq!(receipt.job_id(), id(10));
    assert_eq!(receipt.attempt(), claimed.attempt);
    assert_eq!(receipt.lease_expires_at_ms(), expiry + 1_000);
    let info = store.job(id(10)).unwrap().unwrap();
    assert_eq!(info.attempt, claimed.attempt);
    assert_eq!(info.lease_expires_at_ms, Some(expiry + 1_000));

    // Terminal jobs are always rejected.
    store
        .commit(&CommitBatch::new(id(2), NOW).acknowledge_job(id(10), claimed.lease_token, None))
        .unwrap();
    assert!(matches!(
        store
            .extend_lease(id(10), claimed.lease_token, expiry + 2_000, NOW)
            .unwrap_err(),
        LeaseError::Conflict(LeaseConflict::NotLeased { .. })
    ));
}

#[test]
fn batched_lease_extension_commits_atomically() {
    let dir = temp_dir();
    let store = open_in(&dir);
    enqueue(&store, 1, JobSpec::reconcilable(id(10), "q", "p", vec![]));
    let claimed = claim_one(&store, "q", NOW);
    let expiry = claimed.lease_expires_at_ms;

    // A batch with a wrong token rejects atomically: no extension, no event.
    let bad = CommitBatch::new(id(2), NOW)
        .append_event(common::event(100, "s", "t"))
        .extend_lease(id(10), id(999), expiry + 1_000);
    assert!(store.commit(&bad).is_err());
    assert_eq!(store.stream_version("s").unwrap(), 0);
    let info = store.job(id(10)).unwrap().unwrap();
    assert_eq!(info.lease_expires_at_ms, Some(expiry));

    // A valid batched extension moves the expiry together with the event.
    let good = CommitBatch::new(id(3), NOW)
        .append_event(common::event(101, "s", "t"))
        .extend_lease(id(10), claimed.lease_token, expiry + 1_000);
    store.commit(&good).unwrap();
    assert_eq!(store.stream_version("s").unwrap(), 1);
    let info = store.job(id(10)).unwrap().unwrap();
    assert_eq!(info.lease_expires_at_ms, Some(expiry + 1_000));
    assert_eq!(info.attempt, claimed.attempt);

    // Zero lease tokens are rejected at validation time.
    let zero = CommitBatch::new(id(4), NOW).extend_lease(id(10), Id::ZERO, expiry + 2_000);
    assert!(store.commit(&zero).is_err());

    // Extensions past the batch's committed_at_ms "now" are rejected as expired.
    let late = CommitBatch::new(id(5), expiry + 2_000 + 1).extend_lease(
        id(10),
        claimed.lease_token,
        expiry + 3_000,
    );
    assert!(store.commit(&late).is_err());
}

#[test]
fn extending_an_expired_lease_fails() {
    let dir = temp_dir();
    let store = open_in(&dir);
    enqueue(&store, 1, JobSpec::reconcilable(id(10), "q", "p", vec![]));
    let claimed = claim_one(&store, "q", NOW);
    let expiry = claimed.lease_expires_at_ms;

    // Past the expiry, the lease is dead; extension must not revive it.
    assert!(matches!(
        store
            .extend_lease(id(10), claimed.lease_token, expiry + 10_000, expiry + 1)
            .unwrap_err(),
        LeaseError::Conflict(LeaseConflict::Expired { .. })
    ));
}

#[test]
fn recover_claim_returns_the_extended_expiry_after_extension() {
    let dir = temp_dir();
    let store = open_in(&dir);
    enqueue(&store, 1, JobSpec::reconcilable(id(10), "q", "p", vec![]));
    let claims = match store.claim_jobs(&claim_request("q", NOW)).unwrap() {
        ClaimOutcome::Committed(claims) => claims,
        other => panic!("expected committed, got {other:?}"),
    };
    let claimed = claims.jobs()[0].clone();
    let extended = claimed.lease_expires_at_ms + 5_000;
    store
        .extend_lease(id(10), claimed.lease_token, extended, NOW)
        .unwrap();

    match store.recover_claim(claims.transaction_id(), NOW).unwrap() {
        ClaimRecovery::Committed(recovered) => {
            assert_eq!(recovered.jobs()[0].lease_expires_at_ms, extended);
        }
        other => panic!("expected committed recovery, got {other:?}"),
    }
}

// ----- claim recovery -----

#[test]
fn recover_claim_reports_committed_with_original_tokens_and_absent() {
    let dir = temp_dir();
    let path = db_path(&dir);
    let store = open(&path);
    enqueue(&store, 1, JobSpec::reconcilable(id(10), "q", "p", vec![]));
    let claims = match store.claim_jobs(&claim_request("q", NOW)).unwrap() {
        ClaimOutcome::Committed(claims) => claims,
        other => panic!("expected committed, got {other:?}"),
    };
    let transaction_id = claims.transaction_id();
    let original = claims.jobs().to_vec();

    // Recovery works from a fresh handle and reconstructs the original tokens.
    drop(store);
    let store = open(&path);
    match store.recover_claim(transaction_id, NOW).unwrap() {
        ClaimRecovery::Committed(recovered) => {
            assert_eq!(recovered.transaction_id(), transaction_id);
            assert_eq!(recovered.jobs().len(), original.len());
            assert_eq!(recovered.jobs()[0].job_id, original[0].job_id);
            assert_eq!(recovered.jobs()[0].lease_token, original[0].lease_token);
        }
        other => panic!("expected committed recovery, got {other:?}"),
    }

    assert_eq!(
        store.recover_claim(id(404), NOW).unwrap(),
        ClaimRecovery::Absent
    );
}

#[test]
fn recover_claim_reports_expired_or_resolved_leases_as_stale() {
    let dir = temp_dir();
    let store = open_in(&dir);
    enqueue(&store, 1, JobSpec::reconcilable(id(10), "q", "p", vec![]));
    let claims = match store.claim_jobs(&claim_request("q", NOW)).unwrap() {
        ClaimOutcome::Committed(claims) => claims,
        other => panic!("expected committed claims, got {other:?}"),
    };
    let txn = claims.transaction_id();

    // Still current: recoverable as executable.
    match store.recover_claim(txn, NOW).unwrap() {
        ClaimRecovery::Committed(recovered) => {
            assert_eq!(recovered.jobs().len(), 1);
            assert!(recovered.stale_jobs().is_empty());
        }
        other => panic!("expected committed recovery, got {other:?}"),
    }

    // After expiry: the lease must not be handed back as executable.
    match store.recover_claim(txn, NOW + LEASE_MS + 1).unwrap() {
        ClaimRecovery::Committed(recovered) => {
            assert!(recovered.jobs().is_empty());
            assert_eq!(recovered.stale_jobs(), &[id(10)]);
        }
        other => panic!("expected committed recovery, got {other:?}"),
    }

    // After acknowledgement the lease is resolved: also stale.
    store
        .commit(&CommitBatch::new(id(100), NOW).acknowledge_job(
            id(10),
            claims.jobs()[0].lease_token,
            None,
        ))
        .unwrap();
    match store.recover_claim(txn, NOW).unwrap() {
        ClaimRecovery::Committed(recovered) => {
            assert!(recovered.jobs().is_empty());
            assert_eq!(recovered.stale_jobs(), &[id(10)]);
        }
        other => panic!("expected committed recovery, got {other:?}"),
    }
}

// ----- indeterminate claim API shape (compile-time) -----

/// Static assertion that a type does NOT implement a trait: the method resolution
/// for `some_item` is ambiguous (and fails to compile) if `T` implements the
/// forbidden trait, because both blanket impls would then apply.
macro_rules! assert_not_impl {
    ($ty:ty, $($trait:tt)+) => {
        const _: fn() = || {
            trait AmbiguousIfImpl<A> {
                fn some_item() {}
            }
            impl<T: ?Sized> AmbiguousIfImpl<()> for T {}
            struct Forbidden;
            impl<T: ?Sized + $($trait)+> AmbiguousIfImpl<Forbidden> for T {}
            let _ = <$ty as AmbiguousIfImpl<_>>::some_item;
        };
    };
}

// P0 safety contract: an indeterminate claim must not be executable. It exposes
// only the transaction ID and proposed job IDs; it is not iterable and does not
// deref to the claimed jobs.
assert_not_impl!(IndeterminateClaim, IntoIterator);
assert_not_impl!(&IndeterminateClaim, IntoIterator);
assert_not_impl!(IndeterminateClaim, std::ops::Deref);

#[test]
fn indeterminate_claim_exposes_only_verification_accessors() {
    // The two verification accessors exist with exactly these signatures.
    let _: fn(&IndeterminateClaim) -> Id = IndeterminateClaim::transaction_id;
    let _: fn(&IndeterminateClaim) -> &[Id] = IndeterminateClaim::proposed_jobs_for_verification;
}

#[test]
fn committed_claims_are_iterable_by_contract() {
    fn assert_into_iter<T: IntoIterator<Item = ClaimedJob>>() {}
    assert_into_iter::<minisqlite::CommittedClaims>();
}
