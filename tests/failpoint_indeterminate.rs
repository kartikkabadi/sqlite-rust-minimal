//! In-process indeterminate COMMIT outcomes via the `failpoints` feature:
//! the commit is durable, but the caller sees Indeterminate, so the writer
//! connection is poisoned and must be reopened by recovery.
#![cfg(feature = "failpoints")]

use std::sync::Mutex;

use minisqlite::{
    failpoints, ClaimError, ClaimOutcome, ClaimRecovery, ClaimRequest, CommitBatch, CommitError,
    ControlPlaneStore, Event, Id, JobSpec, JobState, LeaseError, TransactionRecovery,
};

// The failpoint flag is process-global; serialize the tests that arm it.
static FAILPOINT_LOCK: Mutex<()> = Mutex::new(());

const NOW: i64 = 10_000;

#[test]
fn indeterminate_commit_poisons_writer_and_recovery_reopens_honestly() {
    let _guard = FAILPOINT_LOCK.lock().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let store = ControlPlaneStore::open(dir.path().join("db")).unwrap();

    let batch = CommitBatch::new(Id::from(1u128), NOW).append_event(Event::with_json_payload(
        Id::from(10u128),
        "s1",
        "created",
        NOW,
        b"{}",
    ));
    failpoints::fail_next_commit();
    let err = store.commit(&batch).unwrap_err();
    let CommitError::Indeterminate(info) = err else {
        panic!("expected indeterminate commit, got {err:?}");
    };
    assert_eq!(info.transaction_id(), Id::from(1u128));
    assert!(info.storage_error().contains("failpoint"));

    // Recovery reopens the poisoned writer and reports the durable truth.
    match store.recover_transaction(Id::from(1u128)).unwrap() {
        TransactionRecovery::Committed(receipt) => {
            assert_eq!(receipt.transaction_id(), Id::from(1u128));
        }
        other => panic!("expected committed recovery, got {other:?}"),
    }
    assert_eq!(
        store.recover_transaction(Id::from(404u128)).unwrap(),
        TransactionRecovery::Absent
    );

    // The store stays fully usable on the reopened connection.
    store
        .commit(
            &CommitBatch::new(Id::from(2u128), NOW).append_event(Event::with_json_payload(
                Id::from(11u128),
                "s1",
                "updated",
                NOW,
                b"{}",
            )),
        )
        .unwrap();
}

#[test]
fn indeterminate_claim_poisons_writer_and_recovery_returns_original_tokens() {
    let _guard = FAILPOINT_LOCK.lock().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let store = ControlPlaneStore::open(dir.path().join("db")).unwrap();
    store
        .commit(
            &CommitBatch::new(Id::from(1u128), NOW).enqueue_job(JobSpec::reconcilable(
                Id::from(10u128),
                "q",
                "p",
                vec![],
            )),
        )
        .unwrap();

    failpoints::fail_next_commit();
    let err = store
        .claim_jobs(&ClaimRequest {
            queue: "q".into(),
            worker_id: "w".into(),
            now_ms: NOW,
            lease_ms: 60_000,
            limit: 1,
            partition_key: None,
        })
        .unwrap_err();
    let ClaimError::Indeterminate(info) = err else {
        panic!("expected indeterminate claim, got {err:?}");
    };
    assert_eq!(info.proposed_jobs_for_verification(), &[Id::from(10u128)]);
    assert!(info.storage_error().contains("failpoint"));

    // Recovery reopens the poisoned writer and returns the original lease token.
    let lease_token = match store.recover_claim(info.transaction_id(), NOW).unwrap() {
        ClaimRecovery::Committed(claims) => {
            assert_eq!(claims.jobs().len(), 1);
            assert_eq!(claims.jobs()[0].job_id, Id::from(10u128));
            claims.jobs()[0].lease_token
        }
        other => panic!("expected committed recovery, got {other:?}"),
    };
    assert_ne!(lease_token, Id::from(0u128));

    // The recovered lease is usable on the reopened connection.
    store
        .commit(&CommitBatch::new(Id::from(2u128), NOW).acknowledge_job(
            Id::from(10u128),
            lease_token,
            None,
        ))
        .unwrap();
}

#[test]
fn indeterminate_lease_extension_poisons_writer_and_job_read_verifies() {
    let _guard = FAILPOINT_LOCK.lock().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let store = ControlPlaneStore::open(dir.path().join("db")).unwrap();
    store
        .commit(
            &CommitBatch::new(Id::from(1u128), NOW).enqueue_job(JobSpec::reconcilable(
                Id::from(10u128),
                "q",
                "p",
                vec![],
            )),
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

    failpoints::fail_next_commit();
    let err = store
        .extend_lease(
            Id::from(10u128),
            claimed.lease_token,
            claimed.lease_expires_at_ms + 5_000,
            NOW,
        )
        .unwrap_err();
    let LeaseError::Indeterminate(info) = err else {
        panic!("expected indeterminate lease extension, got {err:?}");
    };
    assert_eq!(info.job_id(), Id::from(10u128));
    assert!(info.storage_error().contains("failpoint"));

    // Recovery: reading the job on the reopened connection reports the durable truth.
    let info = store.job(Id::from(10u128)).unwrap().unwrap();
    assert_eq!(info.state, JobState::Leased);
    assert_eq!(
        info.lease_expires_at_ms,
        Some(claimed.lease_expires_at_ms + 5_000)
    );

    // The store stays fully usable on the reopened writer connection.
    store
        .commit(&CommitBatch::new(Id::from(2u128), NOW).acknowledge_job(
            Id::from(10u128),
            claimed.lease_token,
            None,
        ))
        .unwrap();
}
