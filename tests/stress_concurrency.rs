//! Stress and concurrency tests: many threads committing to overlapping streams,
//! optimistic-conflict retry loops, rapid sequential commits, concurrent workers
//! claiming from overlapping partitions, and multi-process commits to one file.

mod common;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use minisqlite::{
    ClaimOutcome, ClaimRequest, CommitBatch, CommitError, Conflict, ControlPlaneStore, Event, Id,
    JobSpec, JobState,
};

fn txid() -> Id {
    Id::new().unwrap()
}

fn event(stream: &str, event_type: &str) -> Event {
    Event::with_json_payload(txid(), stream, event_type, 1_000, b"{}")
}

fn assert_clean(store: &ControlPlaneStore) {
    let report = store.verify().unwrap();
    assert!(report.findings.is_empty(), "verify: {:?}", report.findings);
}

/// Global sequences must be contiguous 1..=n and stream versions must equal the
/// per-stream event counts.
fn assert_event_invariants(store: &ControlPlaneStore, expected_total: u64) {
    let all = store.events_after(0, usize::MAX).unwrap();
    assert_eq!(all.len() as u64, expected_total);
    let mut per_stream: HashMap<String, u64> = HashMap::new();
    for (i, e) in all.iter().enumerate() {
        assert_eq!(e.global_sequence, i as u64 + 1, "global sequence gap");
        let version = per_stream.entry(e.event.stream_id.clone()).or_insert(0);
        *version += 1;
        assert_eq!(
            e.stream_version, *version,
            "stream {} version gap",
            e.event.stream_id
        );
    }
    for (stream, count) in per_stream {
        assert_eq!(store.stream_version(&stream).unwrap(), count);
    }
}

#[test]
fn concurrent_commits_from_threads_over_overlapping_streams() {
    let dir = common::temp_dir();
    let store = Arc::new(common::open_in(&dir));
    const THREADS: usize = 8;
    const COMMITS: usize = 50;

    let handles: Vec<_> = (0..THREADS)
        .map(|t| {
            let store = Arc::clone(&store);
            thread::spawn(move || {
                for i in 0..COMMITS {
                    // Streams deliberately overlap across threads.
                    let stream = format!("s{}", (t + i) % 4);
                    let batch =
                        CommitBatch::new(txid(), 2_000).append_event(event(&stream, "stress"));
                    store.commit(&batch).unwrap();
                }
            })
        })
        .collect();
    for handle in handles {
        handle.join().unwrap();
    }

    assert_event_invariants(&store, (THREADS * COMMITS) as u64);
    let stats = store.stats().unwrap();
    assert_eq!(stats.transactions, (THREADS * COMMITS) as u64);
    assert_clean(&store);
}

#[test]
fn concurrent_optimistic_appends_serialize_one_stream() {
    let dir = common::temp_dir();
    let store = Arc::new(common::open_in(&dir));
    const THREADS: usize = 6;
    const APPENDS: usize = 20;

    let handles: Vec<_> = (0..THREADS)
        .map(|_| {
            let store = Arc::clone(&store);
            thread::spawn(move || {
                let mut conflicts = 0u64;
                for _ in 0..APPENDS {
                    loop {
                        let version = store.stream_version("hot").unwrap();
                        let batch = CommitBatch::new(txid(), 2_000)
                            .expect_stream_version("hot", version)
                            .append_event(event("hot", "inc"));
                        match store.commit(&batch) {
                            Ok(_) => break,
                            Err(CommitError::Conflict(Conflict::StreamVersion {
                                expected,
                                actual,
                                ..
                            })) => {
                                assert_ne!(expected, actual);
                                conflicts += 1;
                            }
                            Err(other) => panic!("unexpected commit error: {other:?}"),
                        }
                    }
                }
                conflicts
            })
        })
        .collect();
    for handle in handles {
        handle.join().unwrap();
    }

    // Every append eventually succeeded exactly once.
    assert_eq!(
        store.stream_version("hot").unwrap(),
        (THREADS * APPENDS) as u64
    );
    assert_event_invariants(&store, (THREADS * APPENDS) as u64);
    assert_clean(&store);
}

#[test]
fn rapid_sequential_commits_keep_sequences_monotonic() {
    let dir = common::temp_dir();
    let store = common::open_in(&dir);
    let mut last_sequence = 0;
    for i in 0..2_000u64 {
        let stream = format!("s{}", i % 8);
        let receipt = store
            .commit(&CommitBatch::new(txid(), 2_000).append_event(event(&stream, "rapid")))
            .unwrap();
        assert_eq!(receipt.transaction_sequence(), last_sequence + 1);
        last_sequence = receipt.transaction_sequence();
    }
    assert_event_invariants(&store, 2_000);
    assert_clean(&store);
}

#[test]
fn concurrent_workers_never_double_lease_a_job() {
    let dir = common::temp_dir();
    let store = Arc::new(common::open_in(&dir));
    const JOBS: u128 = 200;
    const PARTITIONS: u128 = 20;
    const WORKERS: usize = 8;

    for job in 1..=JOBS {
        let spec =
            JobSpec::reconcilable(Id::from(job), "q", format!("p{}", job % PARTITIONS), vec![])
                .with_max_attempts(1);
        store
            .commit(&CommitBatch::new(txid(), 1_000).enqueue_job(spec))
            .unwrap();
    }

    let handles: Vec<_> = (0..WORKERS)
        .map(|w| {
            let store = Arc::clone(&store);
            thread::spawn(move || {
                let mut claimed: Vec<(Id, Id)> = Vec::new();
                let mut idle = 0;
                while idle < 50 {
                    let outcome = store
                        .claim_jobs(&ClaimRequest {
                            queue: "q".into(),
                            worker_id: format!("w{w}"),
                            now_ms: 2_000,
                            lease_ms: 60_000,
                            limit: 5,
                            partition_key: None,
                        })
                        .unwrap();
                    match outcome {
                        ClaimOutcome::Committed(claims) => {
                            idle = 0;
                            for job in claims {
                                assert_eq!(job.attempt, 1);
                                store
                                    .commit(&CommitBatch::new(txid(), 3_000).acknowledge_job(
                                        job.job_id,
                                        job.lease_token,
                                        None,
                                    ))
                                    .unwrap();
                                claimed.push((job.job_id, job.lease_token));
                            }
                        }
                        ClaimOutcome::Noop => idle += 1,
                        ClaimOutcome::MaintenanceCommitted(_) => {
                            panic!("no lease should expire at now_ms=2000")
                        }
                    }
                }
                claimed
            })
        })
        .collect();

    let mut seen_jobs = HashSet::new();
    let mut seen_tokens = HashSet::new();
    let mut total = 0usize;
    for handle in handles {
        for (job_id, token) in handle.join().unwrap() {
            total += 1;
            assert!(seen_jobs.insert(job_id), "job {job_id} leased twice");
            assert!(seen_tokens.insert(token), "lease token {token} reused");
        }
    }
    assert_eq!(total as u128, JOBS);
    for job in store.jobs(Some("q"), None, usize::MAX).unwrap() {
        assert_eq!(job.state, JobState::Succeeded, "job {}", job.job_id);
    }
    assert_clean(&store);
}

// ----- multi-process commits -----

/// Env-gated child body: commits `MINISQLITE_CHILD_COUNT` single-event batches to
/// `MINISQLITE_CHILD_DB`, then exits. A no-op in normal test runs.
#[test]
fn child_commit_worker() {
    let Ok(db) = std::env::var("MINISQLITE_CHILD_DB") else {
        return;
    };
    let count: u64 = std::env::var("MINISQLITE_CHILD_COUNT")
        .unwrap()
        .parse()
        .unwrap();
    let store = ControlPlaneStore::open(&db).unwrap();
    for i in 0..count {
        let stream = format!("s{}", i % 4);
        commit_with_retry(
            &store,
            CommitBatch::new(txid(), 2_000).append_event(event(&stream, "child")),
        );
    }
    std::process::exit(0);
}

/// Commit exactly once under heavy cross-process contention: retry with backoff
/// on busy/locked storage errors, and resolve indeterminate outcomes honestly
/// via `recover_transaction` before deciding whether to retry.
fn commit_with_retry(store: &ControlPlaneStore, batch: CommitBatch) {
    let mut delay = Duration::from_millis(5);
    loop {
        match store.commit(&batch) {
            Ok(_) => return,
            Err(minisqlite::CommitError::Indeterminate(info)) => {
                match store.recover_transaction(info.transaction_id()).unwrap() {
                    minisqlite::TransactionRecovery::Committed(_) => return,
                    minisqlite::TransactionRecovery::Absent => {}
                }
            }
            Err(minisqlite::CommitError::Storage(e)) => {
                let msg = e.to_string();
                assert!(
                    msg.contains("locked") || msg.contains("busy"),
                    "unexpected storage error: {msg}"
                );
            }
            Err(e) => panic!("unexpected commit error: {e:?}"),
        }
        std::thread::sleep(delay);
        delay = (delay * 2).min(Duration::from_millis(200));
    }
}

#[test]
fn concurrent_commits_from_multiple_processes() {
    let dir = common::temp_dir();
    let db = common::db_path(&dir);
    const PROCESSES: usize = 3;
    const COMMITS_PER_PROCESS: u64 = 100;

    // Create the database before racing children on first-open migrations.
    let store = common::open(&db);
    drop(store);

    let exe = std::env::current_exe().unwrap();
    let children: Vec<_> = (0..PROCESSES)
        .map(|_| {
            std::process::Command::new(&exe)
                .args(["child_commit_worker", "--exact", "--test-threads=1"])
                .env("MINISQLITE_CHILD_DB", &db)
                .env("MINISQLITE_CHILD_COUNT", COMMITS_PER_PROCESS.to_string())
                .spawn()
                .unwrap()
        })
        .collect();
    for mut child in children {
        assert!(child.wait().unwrap().success());
    }

    let store = common::open(&db);
    assert_event_invariants(&store, PROCESSES as u64 * COMMITS_PER_PROCESS);
    let stats = store.stats().unwrap();
    assert_eq!(stats.transactions, PROCESSES as u64 * COMMITS_PER_PROCESS);
    assert_clean(&store);
}
