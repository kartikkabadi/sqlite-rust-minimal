//! Crash-simulation tests: a child process is killed mid-`commit` or mid-`claim`,
//! then the store is reopened and its integrity and recovery contracts verified.
//!
//! Each child logs an intent line (fsynced) before an operation and a completion
//! line after it, so the parent can distinguish "definitely durable" from "torn"
//! operations and check `recover_transaction` / `recover_claim` against reality.

mod common;

use std::collections::HashSet;
use std::fs::File;
use std::io::Write;
use std::path::Path;
use std::time::Duration;

use minisqlite::{
    ClaimOutcome, ClaimRecovery, ClaimRequest, CommitBatch, ControlPlaneStore, Event, Id, JobSpec,
    JobState, TransactionRecovery,
};

const CRASH_TXN_BASE: u128 = 0xC0FFEE << 64;

fn txid() -> Id {
    Id::new().unwrap()
}

fn assert_clean(store: &ControlPlaneStore) {
    let report = store.verify().unwrap();
    assert!(report.findings.is_empty(), "verify: {:?}", report.findings);
}

fn log_line(log: &mut File, line: &str) {
    writeln!(log, "{line}").unwrap();
    log.flush().unwrap();
    log.sync_data().unwrap();
}

/// Spawn the env-gated child and kill it only after its log shows at least
/// `min_progress` completed operations (lines starting with `progress_prefix`),
/// so slow machines cannot fail the minimum-progress assertions.
fn spawn_killed_child(
    test_name: &str,
    db: &Path,
    log: &Path,
    progress_prefix: &str,
    min_progress: usize,
) {
    let exe = std::env::current_exe().unwrap();
    let mut child = std::process::Command::new(exe)
        .args([test_name, "--exact", "--test-threads=1"])
        .env("MINISQLITE_CRASH_DB", db)
        .env("MINISQLITE_CRASH_LOG", log)
        .spawn()
        .unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        let progress = std::fs::read_to_string(log)
            .map(|s| s.lines().filter(|l| l.starts_with(progress_prefix)).count())
            .unwrap_or(0);
        if progress >= min_progress || std::time::Instant::now() >= deadline {
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    child.kill().unwrap();
    child.wait().unwrap();
}

// ----- crash during commit -----

/// Env-gated child body: commits deterministic single-event batches forever,
/// logging `start {i}` before and `done {i}` after each commit. Killed by the
/// parent mid-operation.
#[test]
fn child_crash_commit_worker() {
    let Ok(db) = std::env::var("MINISQLITE_CRASH_DB") else {
        return;
    };
    let log_path = std::env::var("MINISQLITE_CRASH_LOG").unwrap();
    let store = ControlPlaneStore::open(&db).unwrap();
    let mut log = File::create(&log_path).unwrap();
    for i in 1u128.. {
        log_line(&mut log, &format!("start {i}"));
        let batch = CommitBatch::new(Id::from(CRASH_TXN_BASE + i), 2_000).append_event(
            Event::with_json_payload(txid(), format!("s{}", i % 4), "crash", 1_000, b"{}"),
        );
        store.commit(&batch).unwrap();
        log_line(&mut log, &format!("done {i}"));
    }
}

#[test]
fn killed_mid_commit_store_reopens_consistent_and_recovers_honestly() {
    let dir = common::temp_dir();
    let db = common::db_path(&dir);
    let log_path = dir.path().join("commit.log");
    drop(common::open(&db)); // run migrations before the child races on first open

    spawn_killed_child("child_crash_commit_worker", &db, &log_path, "done ", 11);

    let store = common::open(&db);
    assert_clean(&store);

    let log = std::fs::read_to_string(&log_path).unwrap();
    let started: Vec<u128> = log
        .lines()
        .filter_map(|l| l.strip_prefix("start "))
        .map(|n| n.parse().unwrap())
        .collect();
    let done: HashSet<u128> = log
        .lines()
        .filter_map(|l| l.strip_prefix("done "))
        .map(|n| n.parse().unwrap())
        .collect();
    assert!(done.len() > 10, "child made too little progress: {log}");

    // Sequential commits: the committed set must be a prefix of the started set,
    // with at most the final started commit torn away.
    let mut committed = 0u64;
    let mut absent_seen = false;
    for &i in &started {
        match store
            .recover_transaction(Id::from(CRASH_TXN_BASE + i))
            .unwrap()
        {
            TransactionRecovery::Committed(receipt) => {
                assert!(!absent_seen, "commit {i} durable after an earlier gap");
                assert_eq!(receipt.transaction_id(), Id::from(CRASH_TXN_BASE + i));
                committed += 1;
            }
            TransactionRecovery::Absent => {
                assert!(
                    !done.contains(&i),
                    "commit {i} completed in the child but is not durable"
                );
                absent_seen = true;
            }
        }
    }
    // Every commit the child saw complete must be durable.
    assert!(committed >= done.len() as u64);

    // Exactly one event per durable transaction; global sequences contiguous.
    let events = store.events_after(0, usize::MAX).unwrap();
    assert_eq!(events.len() as u64, committed);
    for (idx, e) in events.iter().enumerate() {
        assert_eq!(e.global_sequence, idx as u64 + 1);
    }
    assert_eq!(store.stats().unwrap().transactions, committed);

    // The store remains fully usable after the crash.
    store
        .commit(
            &CommitBatch::new(txid(), 3_000).append_event(Event::with_json_payload(
                txid(),
                "post-crash",
                "ok",
                1_000,
                b"{}",
            )),
        )
        .unwrap();
    assert_clean(&store);
}

// ----- deterministic crash immediately after COMMIT (contract B2.1-B2.4) -----

fn run_child_to_abort(test_name: &str, db: &Path, log: &Path) {
    let exe = std::env::current_exe().unwrap();
    let status = std::process::Command::new(exe)
        .args([test_name, "--exact", "--test-threads=1"])
        .env("MINISQLITE_CRASH_DB", db)
        .env("MINISQLITE_CRASH_LOG", log)
        .status()
        .unwrap();
    assert!(!status.success(), "child was expected to abort");
}

/// Env-gated child body: commits one transaction with a known ID, logs the
/// receipt, then aborts before the commit result can reach any caller.
#[test]
fn child_abort_after_commit() {
    let Ok(db) = std::env::var("MINISQLITE_CRASH_DB") else {
        return;
    };
    let log_path = std::env::var("MINISQLITE_CRASH_LOG").unwrap();
    let store = ControlPlaneStore::open(&db).unwrap();
    let mut log = File::create(&log_path).unwrap();
    let batch = CommitBatch::new(Id::from(CRASH_TXN_BASE + 1), 2_000).append_event(
        Event::with_json_payload(txid(), "s1", "crash", 1_000, b"{}"),
    );
    let receipt = store.commit(&batch).unwrap();
    log_line(&mut log, &format!("committed {}", receipt.transaction_id()));
    std::process::abort();
}

#[test]
fn crash_after_commit_recovers_as_committed_and_absent_when_never_committed() {
    let dir = common::temp_dir();
    let db = common::db_path(&dir);
    let log_path = dir.path().join("abort-commit.log");
    drop(common::open(&db));

    run_child_to_abort("child_abort_after_commit", &db, &log_path);
    let log = std::fs::read_to_string(&log_path).unwrap();
    assert!(log.contains("committed"), "child never committed: {log}");

    let store = common::open(&db);
    assert_clean(&store);
    // B2.1: a commit that became durable before the crash recovers as Committed.
    match store
        .recover_transaction(Id::from(CRASH_TXN_BASE + 1))
        .unwrap()
    {
        TransactionRecovery::Committed(receipt) => {
            assert_eq!(receipt.transaction_id(), Id::from(CRASH_TXN_BASE + 1));
        }
        other => panic!("durable commit recovered as {other:?}"),
    }
    // B2.2: a transaction that never committed recovers as Absent.
    assert_eq!(
        store
            .recover_transaction(Id::from(CRASH_TXN_BASE + 2))
            .unwrap(),
        TransactionRecovery::Absent
    );
}

/// Env-gated child body: claims one job, logs the transaction and lease token,
/// then aborts before the claim result can reach any caller.
#[test]
fn child_abort_after_claim() {
    let Ok(db) = std::env::var("MINISQLITE_CRASH_DB") else {
        return;
    };
    let log_path = std::env::var("MINISQLITE_CRASH_LOG").unwrap();
    let store = ControlPlaneStore::open(&db).unwrap();
    let mut log = File::create(&log_path).unwrap();
    let outcome = store
        .claim_jobs(&ClaimRequest {
            queue: "q".into(),
            worker_id: "abort-worker".into(),
            now_ms: 2_000,
            lease_ms: 600_000,
            limit: 1,
            partition_key: None,
        })
        .unwrap();
    let ClaimOutcome::Committed(claims) = outcome else {
        panic!("expected a committed claim, got {outcome:?}");
    };
    let job = &claims.jobs()[0];
    log_line(
        &mut log,
        &format!(
            "claim {} {} {}",
            claims.transaction_id().to_hex(),
            job.job_id.to_hex(),
            job.lease_token.to_hex()
        ),
    );
    std::process::abort();
}

#[test]
fn crash_after_claim_recovers_original_lease_tokens() {
    let dir = common::temp_dir();
    let db = common::db_path(&dir);
    let log_path = dir.path().join("abort-claim.log");
    let store = common::open(&db);
    store
        .commit(
            &CommitBatch::new(txid(), 1_000).enqueue_job(JobSpec::reconcilable(
                Id::from(1u128),
                "q",
                "p",
                vec![],
            )),
        )
        .unwrap();
    drop(store);

    run_child_to_abort("child_abort_after_claim", &db, &log_path);
    let log = std::fs::read_to_string(&log_path).unwrap();
    let line = log
        .lines()
        .find_map(|l| l.strip_prefix("claim "))
        .unwrap_or_else(|| panic!("child never claimed: {log}"));
    let mut parts = line.split(' ');
    let txn = Id::from_hex(parts.next().unwrap()).unwrap();
    let job_id = Id::from_hex(parts.next().unwrap()).unwrap();
    let lease_token = Id::from_hex(parts.next().unwrap()).unwrap();

    let store = common::open(&db);
    assert_clean(&store);
    // B2.3: crash-after-commit claim recovers as Committed with the original
    // lease token.
    match store.recover_claim(txn, 2_000).unwrap() {
        ClaimRecovery::Committed(claims) => {
            assert_eq!(claims.jobs().len(), 1);
            assert_eq!(claims.jobs()[0].job_id, job_id);
            assert_eq!(claims.jobs()[0].lease_token, lease_token);
        }
        other => panic!("durable claim recovered as {other:?}"),
    }
    // B2.4: a claim that never committed recovers as Absent.
    assert_eq!(
        store.recover_claim(Id::from(0xABADu128), 2_000).unwrap(),
        ClaimRecovery::Absent
    );
}

// ----- crash during claim -----

/// Env-gated child body: claims and acks jobs forever, logging every durable
/// claim transaction and its job IDs. Killed by the parent mid-operation.
#[test]
fn child_crash_claim_worker() {
    let Ok(db) = std::env::var("MINISQLITE_CRASH_DB") else {
        return;
    };
    let log_path = std::env::var("MINISQLITE_CRASH_LOG").unwrap();
    let store = ControlPlaneStore::open(&db).unwrap();
    let mut log = File::create(&log_path).unwrap();
    loop {
        let outcome = store
            .claim_jobs(&ClaimRequest {
                queue: "q".into(),
                worker_id: "crash-worker".into(),
                now_ms: 2_000,
                lease_ms: 600_000,
                limit: 3,
                partition_key: None,
            })
            .unwrap();
        if let ClaimOutcome::Committed(claims) = outcome {
            let ids: Vec<String> = claims.jobs().iter().map(|j| j.job_id.to_hex()).collect();
            log_line(
                &mut log,
                &format!(
                    "claim {} {}",
                    claims.transaction_id().to_hex(),
                    ids.join(" ")
                ),
            );
            for job in claims {
                store
                    .commit(&CommitBatch::new(txid(), 3_000).acknowledge_job(
                        job.job_id,
                        job.lease_token,
                        None,
                    ))
                    .unwrap();
                log_line(&mut log, &format!("ack {}", job.job_id.to_hex()));
            }
        }
    }
}

#[test]
fn killed_mid_claim_leases_are_recoverable_and_bounded() {
    let dir = common::temp_dir();
    let db = common::db_path(&dir);
    let log_path = dir.path().join("claim.log");

    const JOBS: u128 = 2_000;
    const LIMIT: usize = 3;
    let store = common::open(&db);
    for job in 1..=JOBS {
        store
            .commit(
                &CommitBatch::new(txid(), 1_000).enqueue_job(JobSpec::reconcilable(
                    Id::from(job),
                    "q",
                    format!("p{}", job % 10),
                    vec![],
                )),
            )
            .unwrap();
    }
    drop(store);

    spawn_killed_child("child_crash_claim_worker", &db, &log_path, "claim ", 6);

    let store = common::open(&db);
    assert_clean(&store);

    let log = std::fs::read_to_string(&log_path).unwrap();
    let mut logged_claims = 0;
    let mut logged_leased: HashSet<Id> = HashSet::new();
    let mut logged_acked: HashSet<Id> = HashSet::new();
    for line in log.lines() {
        if let Some(rest) = line.strip_prefix("claim ") {
            let mut parts = rest.split(' ');
            let txn = Id::from_hex(parts.next().unwrap()).unwrap();
            let job_ids: Vec<Id> = parts.map(|p| Id::from_hex(p).unwrap()).collect();
            // Every logged claim must be recoverable with its exact job set and
            // usable lease tokens.
            match store.recover_claim(txn, 2_000).unwrap() {
                ClaimRecovery::Committed(claims) => {
                    // Every leased job is accounted for: still executable, or
                    // reported stale (e.g. already acknowledged).
                    let recovered: HashSet<Id> = claims
                        .jobs()
                        .iter()
                        .map(|j| j.job_id)
                        .chain(claims.stale_jobs().iter().copied())
                        .collect();
                    assert_eq!(recovered, job_ids.iter().copied().collect::<HashSet<_>>());
                    for job in claims.jobs() {
                        assert_ne!(job.lease_token, Id::ZERO);
                    }
                }
                other => panic!("logged claim {txn} not recoverable: {other:?}"),
            }
            logged_leased.extend(job_ids);
            logged_claims += 1;
        } else if let Some(rest) = line.strip_prefix("ack ") {
            logged_acked.insert(Id::from_hex(rest).unwrap());
        }
    }
    assert!(logged_claims > 5, "child made too little progress: {log}");

    // Every job the store believes is leased or succeeded must be explained by the
    // log, except for at most one torn claim (<= LIMIT jobs) and one torn ack.
    let mut unexplained_leased = 0;
    for job in store.jobs(Some("q"), None, usize::MAX).unwrap() {
        match job.state {
            JobState::Pending => {
                assert!(
                    !logged_acked.contains(&job.job_id),
                    "acked job {} regressed to pending",
                    job.job_id
                );
            }
            JobState::Leased => {
                if !logged_leased.contains(&job.job_id) {
                    unexplained_leased += 1;
                }
            }
            JobState::Succeeded => {
                assert!(
                    logged_leased.contains(&job.job_id) || !logged_acked.contains(&job.job_id),
                    "job {} succeeded without any claim",
                    job.job_id
                );
            }
            other => panic!("unexpected state {other:?} for job {}", job.job_id),
        }
    }
    assert!(
        unexplained_leased <= LIMIT,
        "{unexplained_leased} leased jobs unexplained by the claim log (torn window is one claim of {LIMIT})"
    );

    // Unknown transactions recover as Absent, never as a granted claim.
    assert_eq!(
        store
            .recover_claim(Id::from(0xDEAD_BEEFu128), 2_000)
            .unwrap(),
        ClaimRecovery::Absent
    );
    assert_eq!(
        store
            .recover_transaction(Id::from(0xDEAD_BEEFu128))
            .unwrap(),
        TransactionRecovery::Absent
    );
    assert_clean(&store);
}
