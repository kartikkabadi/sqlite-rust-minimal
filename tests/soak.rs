//! Bounded soak test (spec Part B, Layer 9): workers enqueue/claim/ack under
//! random crashes and intermittent heartbeats, with periodic backup/reopen and
//! integrity verification. The duration is short by default so it runs in CI;
//! set `MINISQLITE_SOAK_SECS` (e.g. to 86400) for a long operational soak.

mod common;

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use common::{open_in, temp_dir, Prng};
use minisqlite::{
    ClaimOutcome, ClaimRequest, ClaimedJob, CommitBatch, CommitError, ControlPlaneStore, Id,
    JobSpec, JobState, Resolution, TransactionRecovery,
};

fn soak_duration() -> Duration {
    std::env::var("MINISQLITE_SOAK_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .map(Duration::from_secs)
        .unwrap_or(Duration::from_secs(3))
}

/// Acknowledge a claimed job, resolving an indeterminate COMMIT with
/// `recover_transaction` so the count of successful acks is exact.
fn ack(store: &ControlPlaneStore, next_id: &AtomicU64, job: &ClaimedJob) -> bool {
    let txn = Id::from(u128::from(next_id.fetch_add(1, Ordering::Relaxed)));
    let batch = CommitBatch::new(txn, now_ms()).acknowledge_job(job.job_id, job.lease_token, None);
    match store.commit(&batch) {
        Ok(_) => true,
        Err(CommitError::Indeterminate(_)) => matches!(
            store.recover_transaction(txn),
            Ok(TransactionRecovery::Committed(_))
        ),
        Err(_) => false,
    }
}

fn now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

#[test]
fn soak_enqueue_claim_ack_with_crashes_and_backups() {
    let dir = temp_dir();
    let store = Arc::new(open_in(&dir));
    let stop = Arc::new(AtomicBool::new(false));
    let next_id = Arc::new(AtomicU64::new(1));
    let enqueued = Arc::new(AtomicU64::new(0));
    let acked = Arc::new(AtomicU64::new(0));
    let deadline = Instant::now() + soak_duration();

    // Producer: enqueue jobs continuously.
    let producer = {
        let store = Arc::clone(&store);
        let stop = Arc::clone(&stop);
        let next_id = Arc::clone(&next_id);
        let enqueued = Arc::clone(&enqueued);
        std::thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                let n = next_id.fetch_add(2, Ordering::Relaxed);
                let batch = CommitBatch::new(Id::from(u128::from(n)), now_ms()).enqueue_job(
                    JobSpec::reconcilable(Id::from(u128::from(n + 1)), "q", "p", vec![0u8; 32]),
                );
                store.commit(&batch).unwrap();
                enqueued.fetch_add(1, Ordering::Relaxed);
                std::thread::sleep(Duration::from_millis(1));
            }
        })
    };

    // Workers: claim, sometimes heartbeat, sometimes "crash" (drop the lease),
    // otherwise ack.
    let workers: Vec<_> = (0..3u64)
        .map(|w| {
            let store = Arc::clone(&store);
            let stop = Arc::clone(&stop);
            let next_id = Arc::clone(&next_id);
            let acked = Arc::clone(&acked);
            std::thread::spawn(move || {
                let mut rng = Prng::new(0x5eed + w);
                while !stop.load(Ordering::Relaxed) {
                    let now = now_ms();
                    let outcome = store.claim_jobs(&ClaimRequest {
                        queue: "q".into(),
                        worker_id: format!("w{w}"),
                        now_ms: now,
                        lease_ms: 200,
                        limit: 4,
                        partition_key: None,
                    });
                    let claims = match outcome {
                        Ok(ClaimOutcome::Committed(claims)) => claims.into_jobs(),
                        Ok(_) => continue,
                        Err(e) => panic!("claim failed: {e}"),
                    };
                    for job in claims {
                        match rng.below(10) {
                            // Simulated crash: abandon the lease.
                            0 => {}
                            // Intermittent heartbeat, then ack.
                            1..=2 => {
                                let _ = store.extend_lease(
                                    job.job_id,
                                    job.lease_token,
                                    now_ms() + 500,
                                    now_ms(),
                                );
                                if ack(&store, &next_id, &job) {
                                    acked.fetch_add(1, Ordering::Relaxed);
                                }
                            }
                            // Normal completion.
                            _ => {
                                if ack(&store, &next_id, &job) {
                                    acked.fetch_add(1, Ordering::Relaxed);
                                }
                            }
                        }
                    }
                }
            })
        })
        .collect();

    // Resolver: retry uncertain jobs left by crashed workers.
    let resolver = {
        let store = Arc::clone(&store);
        let stop = Arc::clone(&stop);
        let next_id = Arc::clone(&next_id);
        std::thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                let uncertain = store.jobs(None, Some(JobState::Uncertain), 16).unwrap();
                for job in uncertain {
                    let n = next_id.fetch_add(1, Ordering::Relaxed);
                    let _ = store.commit(
                        &CommitBatch::new(Id::from(u128::from(n)), now_ms())
                            .resolve_uncertain_job(job.job_id, Resolution::Retry),
                    );
                }
                std::thread::sleep(Duration::from_millis(20));
            }
        })
    };

    // Main thread: periodic backup + reopen + verify while everything runs.
    let mut backups = 0u32;
    while Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(500));
        let dest = dir.path().join(format!("soak-backup-{backups}.db"));
        store.backup(&dest, false).unwrap();
        let restored = ControlPlaneStore::open_existing(&dest).unwrap();
        let report = restored.verify().unwrap();
        assert!(
            report.is_ok(),
            "backup verify findings: {:?}",
            report.findings
        );
        backups += 1;
    }
    stop.store(true, Ordering::Relaxed);
    producer.join().unwrap();
    for worker in workers {
        worker.join().unwrap();
    }
    resolver.join().unwrap();

    // Final invariants: the live store verifies cleanly and made progress.
    let report = store.verify().unwrap();
    assert!(
        report.is_ok(),
        "final verify findings: {:?}",
        report.findings
    );
    let stats = store.stats().unwrap();
    assert!(enqueued.load(Ordering::Relaxed) > 0, "no jobs enqueued");
    assert!(acked.load(Ordering::Relaxed) > 0, "no jobs acknowledged");
    // Each successful ack moves exactly one job to Succeeded: no duplicate
    // completions and no lost completions.
    let succeeded = stats.jobs_by_state.get("succeeded").copied().unwrap_or(0);
    assert!(succeeded <= enqueued.load(Ordering::Relaxed));
    assert_eq!(succeeded, acked.load(Ordering::Relaxed));
}
