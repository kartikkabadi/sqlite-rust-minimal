//! Barrier-synchronized race tests (spec Part B, Layer 4): both sides of each
//! race start together; exactly one linearized outcome must win, and the store
//! must stay verifiably consistent afterwards.

mod common;

use std::sync::{Arc, Barrier};

use common::{id, open_in, temp_dir};
use minisqlite::{
    ClaimOutcome, ClaimRequest, ClaimedJob, CommitBatch, ControlPlaneStore, JobSpec, JobState,
};

const NOW: i64 = 10_000;

fn enqueue(store: &ControlPlaneStore, job: u128, txn: u128) {
    store
        .commit(
            &CommitBatch::new(id(txn), NOW).enqueue_job(JobSpec::reconcilable(
                id(job),
                "q",
                "p",
                vec![],
            )),
        )
        .unwrap();
}

fn claim_one(store: &ControlPlaneStore, now_ms: i64, lease_ms: i64) -> ClaimedJob {
    loop {
        match store
            .claim_jobs(&ClaimRequest {
                queue: "q".into(),
                worker_id: "w".into(),
                now_ms,
                lease_ms,
                limit: 1,
                partition_key: None,
            })
            .unwrap()
        {
            ClaimOutcome::Committed(claims) => {
                let mut jobs = claims.into_jobs();
                assert_eq!(jobs.len(), 1);
                return jobs.remove(0);
            }
            ClaimOutcome::MaintenanceCommitted(_) => continue,
            other => panic!("expected committed claims, got {other:?}"),
        }
    }
}

fn assert_verify_clean(store: &ControlPlaneStore) {
    let report = store.verify().unwrap();
    assert!(report.is_ok(), "verify findings: {:?}", report.findings);
}

/// Race an acknowledgement against expiry maintenance (a claim after the lease
/// expired). Exactly one wins: the job ends Succeeded (ack won) or Uncertain
/// (maintenance won and the reconcilable lease became uncertain).
#[test]
fn ack_races_lease_expiry_maintenance() {
    for round in 0..20u128 {
        let dir = temp_dir();
        let store = Arc::new(open_in(&dir));
        enqueue(&store, 100 + round, 1);
        let claimed = claim_one(&store, NOW, 1_000);
        let after_expiry = claimed.lease_expires_at_ms + 1;

        let barrier = Arc::new(Barrier::new(2));
        let acker = {
            let store = Arc::clone(&store);
            let barrier = Arc::clone(&barrier);
            let job = claimed.clone();
            std::thread::spawn(move || {
                barrier.wait();
                store
                    .commit(&CommitBatch::new(id(2), after_expiry).acknowledge_job(
                        job.job_id,
                        job.lease_token,
                        None,
                    ))
                    .is_ok()
            })
        };
        let maintainer = {
            let store = Arc::clone(&store);
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || {
                barrier.wait();
                // Maintenance runs as part of a claim past the expiry.
                let _ = store.claim_jobs(&ClaimRequest {
                    queue: "q".into(),
                    worker_id: "w2".into(),
                    now_ms: after_expiry,
                    lease_ms: 1_000,
                    limit: 1,
                    partition_key: None,
                });
            })
        };
        let acked = acker.join().unwrap();
        maintainer.join().unwrap();

        let state = store.job(claimed.job_id).unwrap().unwrap().state;
        if acked {
            assert_eq!(state, JobState::Succeeded);
        } else {
            assert_eq!(state, JobState::Uncertain);
        }
        assert_verify_clean(&store);
    }
}

/// Race a heartbeat (lease extension) against expiry maintenance. Either the
/// extension lands (job stays Leased with the later expiry) or maintenance wins
/// (job is Uncertain) — never both.
#[test]
fn heartbeat_races_lease_expiry_maintenance() {
    for round in 0..20u128 {
        let dir = temp_dir();
        let store = Arc::new(open_in(&dir));
        enqueue(&store, 200 + round, 1);
        let claimed = claim_one(&store, NOW, 1_000);
        let expiry = claimed.lease_expires_at_ms;

        let barrier = Arc::new(Barrier::new(2));
        let extender = {
            let store = Arc::clone(&store);
            let barrier = Arc::clone(&barrier);
            let job = claimed.clone();
            std::thread::spawn(move || {
                barrier.wait();
                store
                    .extend_lease(job.job_id, job.lease_token, expiry + 60_000, expiry)
                    .is_ok()
            })
        };
        let maintainer = {
            let store = Arc::clone(&store);
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || {
                barrier.wait();
                let _ = store.claim_jobs(&ClaimRequest {
                    queue: "q".into(),
                    worker_id: "w2".into(),
                    now_ms: expiry + 1,
                    lease_ms: 1_000,
                    limit: 1,
                    partition_key: None,
                });
            })
        };
        let extended = extender.join().unwrap();
        maintainer.join().unwrap();

        let info = store.job(claimed.job_id).unwrap().unwrap();
        if extended {
            // The extension moved the expiry past the maintainer's "now", so the
            // lease survived maintenance.
            assert_eq!(info.state, JobState::Leased);
            assert_eq!(info.lease_expires_at_ms, Some(expiry + 60_000));
        } else {
            assert_eq!(info.state, JobState::Uncertain);
        }
        assert_verify_clean(&store);
    }
}

/// Race a cancellation against an acknowledgement holding the same lease.
/// Exactly one commits; the job ends Cancelled or Succeeded, never a mixture.
#[test]
fn cancellation_races_acknowledgement() {
    for round in 0..20u128 {
        let dir = temp_dir();
        let store = Arc::new(open_in(&dir));
        enqueue(&store, 300 + round, 1);
        let claimed = claim_one(&store, NOW, 60_000);

        let barrier = Arc::new(Barrier::new(2));
        let canceller = {
            let store = Arc::clone(&store);
            let barrier = Arc::clone(&barrier);
            let job = claimed.clone();
            std::thread::spawn(move || {
                barrier.wait();
                store
                    .commit(
                        &CommitBatch::new(id(2), NOW + 1)
                            .cancel_job(job.job_id, Some(job.lease_token)),
                    )
                    .is_ok()
            })
        };
        let acker = {
            let store = Arc::clone(&store);
            let barrier = Arc::clone(&barrier);
            let job = claimed.clone();
            std::thread::spawn(move || {
                barrier.wait();
                store
                    .commit(&CommitBatch::new(id(3), NOW + 1).acknowledge_job(
                        job.job_id,
                        job.lease_token,
                        None,
                    ))
                    .is_ok()
            })
        };
        let cancelled = canceller.join().unwrap();
        let acked = acker.join().unwrap();
        assert!(
            cancelled != acked,
            "exactly one of cancel/ack must win (cancelled={cancelled}, acked={acked})"
        );

        let state = store.job(claimed.job_id).unwrap().unwrap().state;
        assert_eq!(
            state,
            if cancelled {
                JobState::Cancelled
            } else {
                JobState::Succeeded
            }
        );
        assert_verify_clean(&store);
    }
}

/// Race an uncertain-job resolution (retry) against a concurrent claim: the
/// resolved job may be re-leased at most once, and the store stays consistent.
#[test]
fn uncertain_resolution_races_claim() {
    for round in 0..20u128 {
        let dir = temp_dir();
        let store = Arc::new(open_in(&dir));
        enqueue(&store, 400 + round, 1);
        let claimed = claim_one(&store, NOW, 1_000);
        let after_expiry = claimed.lease_expires_at_ms + 1;
        // Expire the lease into Uncertain via maintenance.
        while store.job(claimed.job_id).unwrap().unwrap().state != JobState::Uncertain {
            let _ = store.claim_jobs(&ClaimRequest {
                queue: "q".into(),
                worker_id: "m".into(),
                now_ms: after_expiry,
                lease_ms: 1_000,
                limit: 1,
                partition_key: None,
            });
        }

        let barrier = Arc::new(Barrier::new(2));
        let resolver = {
            let store = Arc::clone(&store);
            let barrier = Arc::clone(&barrier);
            let job_id = claimed.job_id;
            std::thread::spawn(move || {
                barrier.wait();
                store
                    .commit(
                        &CommitBatch::new(id(50), after_expiry)
                            .resolve_uncertain_job(job_id, minisqlite::Resolution::Retry),
                    )
                    .is_ok()
            })
        };
        let claimer = {
            let store = Arc::clone(&store);
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || {
                barrier.wait();
                let mut leased = 0usize;
                for _ in 0..5 {
                    if let Ok(ClaimOutcome::Committed(claims)) = store.claim_jobs(&ClaimRequest {
                        queue: "q".into(),
                        worker_id: "w2".into(),
                        now_ms: after_expiry,
                        lease_ms: 60_000,
                        limit: 1,
                        partition_key: None,
                    }) {
                        leased += claims.into_jobs().len();
                    }
                }
                leased
            })
        };
        let resolved = resolver.join().unwrap();
        let leased = claimer.join().unwrap();
        assert!(resolved, "resolution of an uncertain job must succeed");
        assert!(leased <= 1, "job re-leased {leased} times");
        assert_verify_clean(&store);
    }
}

/// Run a live backup while a writer commits continuously: the backup must be a
/// consistent snapshot that opens and verifies cleanly.
#[test]
fn backup_races_concurrent_writes() {
    let dir = temp_dir();
    let store = Arc::new(open_in(&dir));
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));

    let writer = {
        let store = Arc::clone(&store);
        let stop = Arc::clone(&stop);
        std::thread::spawn(move || {
            let mut txn = 1_000u128;
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                store
                    .commit(&CommitBatch::new(id(txn), NOW).append_event(common::event(
                        txn + 1_000_000,
                        "s",
                        "t",
                    )))
                    .unwrap();
                txn += 1;
            }
        })
    };

    for i in 0..5 {
        let dest = dir.path().join(format!("backup-{i}.db"));
        store.backup(&dest, false).unwrap();
        let restored = ControlPlaneStore::open_existing(&dest).unwrap();
        assert_verify_clean(&restored);
    }
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    writer.join().unwrap();
    assert_verify_clean(&store);
}
