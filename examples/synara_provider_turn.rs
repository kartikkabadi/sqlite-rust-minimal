//! Synara provider-turn vertical slice, end to end.
//!
//! Demonstrates the workflow from `docs/SYNARA_INTEGRATION.md` (currently on the
//! `phase/synara-design` branch, PR #13, until that merges) using only the public
//! kernel API:
//!
//! 1. thread created -> 2. turn requested -> 3. projection "queued" ->
//! 4. provider job enqueued (one atomic outbox commit) -> 5. worker claims ->
//! 6. provider effect (simulated) -> 7. completion event + 8. projection "idle" +
//! 9. job ack (one atomic commit).
//!
//! Then a failure drill: the claim commits but the worker "crashes" before
//! acknowledging. The store is reopened and `recover_claim` reconstructs the
//! original lease tokens, so the turn completes exactly once.
//!
//! Run with: `cargo run --example synara_provider_turn`

use minisqlite::{
    ClaimError, ClaimOutcome, ClaimRecovery, ClaimRequest, ClaimedJob, CommitBatch, CommitError,
    ControlPlaneStore, Event, Id, JobSpec, JobState, ProjectionPatch, TransactionRecovery,
};

const THREADS: &str = "threads";
const QUEUE: &str = "provider-command";

fn now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock before epoch")
        .as_millis() as i64
}

fn event(stream: &str, event_type: &str, payload: &str) -> Result<Event, minisqlite::Error> {
    Ok(Event::with_json_payload(
        Id::new()?,
        stream,
        event_type,
        now_ms(),
        payload.as_bytes(),
    ))
}

fn thread_status(store: &ControlPlaneStore, thread_id: &str) -> String {
    match store.projection_get(THREADS, thread_id.as_bytes()) {
        Ok(Some(value)) => String::from_utf8_lossy(&value).into_owned(),
        _ => "<absent>".into(),
    }
}

/// Steps 1-4: thread created, then turn requested + projection "queued" +
/// provider job enqueued in one atomic outbox commit.
fn request_turn(
    store: &ControlPlaneStore,
    thread_id: &str,
) -> Result<Id, Box<dyn std::error::Error>> {
    let stream = format!("thread:{thread_id}");

    // 1. Thread created.
    store.commit(
        &CommitBatch::new(Id::new()?, now_ms())
            .expect_stream_version(&stream, 0)
            .append_event(event(&stream, "thread.created", "{}")?)
            .apply_projection_patch(
                ProjectionPatch::new(THREADS, store.projection_version(THREADS)?)
                    .put(thread_id, r#"{"status":"idle"}"#),
            ),
    )?;
    println!("[commit] thread.created            threads/{thread_id} -> idle");

    // 2-4. Turn requested + projection "queued" + provider job (outbox), atomically.
    let job_id = Id::new()?;
    store.commit(
        &CommitBatch::new(Id::new()?, now_ms())
            .expect_stream_version(&stream, 1)
            .append_event(event(&stream, "thread.turn-requested", r#"{"turn":1}"#)?)
            .apply_projection_patch(
                ProjectionPatch::new(THREADS, store.projection_version(THREADS)?)
                    .put(thread_id, r#"{"status":"queued"}"#),
            )
            .enqueue_job(JobSpec::reconcilable(
                job_id,
                QUEUE,
                &stream,
                br#"{"turn":1,"provider":"simulated"}"#.to_vec(),
            )),
    )?;
    println!("[commit] thread.turn-requested     threads/{thread_id} -> queued, job enqueued");
    Ok(job_id)
}

/// Worker heartbeat: durably extend the lease so no other worker can claim the job.
fn heartbeat(
    store: &ControlPlaneStore,
    job: &ClaimedJob,
) -> Result<(), Box<dyn std::error::Error>> {
    // Must be strictly later than the current expiry (claim lease was now+30s).
    let now = now_ms();
    let receipt = store.extend_lease(job.job_id, job.lease_token, now + 60_000, now)?;
    println!(
        "[lease]  heartbeat extended lease for {} to now+60s (attempt {})",
        job.partition_key,
        receipt.attempt()
    );
    Ok(())
}

/// Worker protocol step 4: turn-started event + projection "running", one CommitBatch.
fn start_turn(
    store: &ControlPlaneStore,
    thread_id: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let stream = format!("thread:{thread_id}");
    store.commit(
        &CommitBatch::new(Id::new()?, now_ms())
            .append_event(event(&stream, "thread.turn-started", r#"{"turn":1}"#)?)
            .apply_projection_patch(
                ProjectionPatch::new(THREADS, store.projection_version(THREADS)?)
                    .put(thread_id, r#"{"status":"running"}"#),
            ),
    )?;
    println!("[commit] thread.turn-started       threads/{thread_id} -> running");
    Ok(())
}

/// Step 6: the external provider effect, simulated.
fn call_provider(job: &ClaimedJob) {
    println!(
        "[effect] provider call for {} attempt {} (simulated)",
        job.partition_key, job.attempt
    );
}

/// Steps 7-9: completion event + projection "idle" + job ack, atomically.
fn complete_turn(
    store: &ControlPlaneStore,
    thread_id: &str,
    job: &ClaimedJob,
) -> Result<(), Box<dyn std::error::Error>> {
    let stream = format!("thread:{thread_id}");
    let batch = CommitBatch::new(Id::new()?, now_ms())
        .append_event(event(&stream, "thread.turn-completed", r#"{"turn":1}"#)?)
        .apply_projection_patch(
            ProjectionPatch::new(THREADS, store.projection_version(THREADS)?)
                .put(thread_id, r#"{"status":"idle"}"#),
        )
        .acknowledge_job(job.job_id, job.lease_token, None);
    match store.commit(&batch) {
        Ok(_) => {}
        // The ack outcome is unknown; a blind retry could double-ack (or, in a
        // template that retries the whole turn, re-run the provider effect).
        // Resolve with recover_transaction: Committed means done, Absent means
        // the commit never landed and the same batch is safe to resubmit.
        Err(CommitError::Indeterminate(i)) => {
            match store.recover_transaction(i.transaction_id())? {
                TransactionRecovery::Committed(_) => {
                    println!("[commit] indeterminate ack resolved: committed; nothing to retry")
                }
                TransactionRecovery::Absent => {
                    println!("[commit] indeterminate ack resolved: absent; resubmitting");
                    store.commit(&batch)?;
                }
            }
        }
        Err(other) => return Err(other.into()),
    }
    println!("[commit] thread.turn-completed     threads/{thread_id} -> idle, job acked");
    Ok(())
}

/// Step 5: worker claims one job from the provider-command queue.
fn claim_one(
    store: &ControlPlaneStore,
    worker_id: &str,
) -> Result<(Id, ClaimedJob), Box<dyn std::error::Error>> {
    loop {
        match store.claim_jobs(&ClaimRequest {
            queue: QUEUE.into(),
            worker_id: worker_id.into(),
            now_ms: now_ms(),
            lease_ms: 30_000,
            limit: 1,
            partition_key: None,
        }) {
            Ok(ClaimOutcome::Committed(claims)) => {
                let tx = claims.transaction_id();
                let job = claims.into_jobs().remove(0);
                println!(
                    "[claim]  {worker_id} leased job on {} (attempt {})",
                    job.partition_key, job.attempt
                );
                return Ok((tx, job));
            }
            Ok(ClaimOutcome::MaintenanceCommitted(_)) => continue, // progress made; poll again
            Ok(ClaimOutcome::Noop) => return Err("queue unexpectedly empty".into()),
            Err(ClaimError::Indeterminate(claim)) => {
                // No executable data here — recover before doing anything.
                println!(
                    "[claim]  indeterminate; recovering {}",
                    claim.transaction_id()
                );
                match store.recover_claim(claim.transaction_id(), now_ms())? {
                    ClaimRecovery::Committed(claims) => {
                        // Stale jobs committed under this claim but must not be
                        // executed with the recovered tokens; reconcilable jobs
                        // surface as Uncertain (effect status unknown).
                        for stale in claims.stale_jobs() {
                            println!(
                                "[claim]  stale job {stale}: do not execute; surfaces as Uncertain"
                            );
                        }
                        if claims.is_empty() {
                            // Every receipt job went stale: no lease held. Claim again.
                            println!("[claim]  recovered claim holds no executable jobs");
                            continue;
                        }
                        let tx = claims.transaction_id();
                        return Ok((tx, claims.into_jobs().remove(0)));
                    }
                    // Maintenance-only or never leased; claim again.
                    ClaimRecovery::MaintenanceCommitted(_) | ClaimRecovery::Absent => continue,
                }
            }
            Err(other) => return Err(other.into()),
        }
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let db = dir.path().join("synara-control-plane.db");

    println!("=== happy path: one provider turn ===");
    {
        let store = ControlPlaneStore::open(&db)?;
        request_turn(&store, "t-100")?;
        let (_tx, job) = claim_one(&store, "worker-1")?;
        heartbeat(&store, &job)?;
        start_turn(&store, "t-100")?;
        call_provider(&job);
        complete_turn(&store, "t-100", &job)?;
        println!(
            "[state]  threads/t-100 = {}",
            thread_status(&store, "t-100")
        );
    }

    println!();
    println!("=== failure drill: worker crash after claim, before ack ===");
    let claim_tx = {
        let store = ControlPlaneStore::open(&db)?;
        request_turn(&store, "t-200")?;
        let (tx, job) = claim_one(&store, "worker-2")?;
        println!(
            "[crash]  worker-2 dies before acking job on {} (claim tx {tx})",
            job.partition_key
        );
        tx
        // store dropped here: simulated process crash
    };

    // A recovering process reopens the store. It knows only the claim
    // transaction id (e.g. from its WAL/journal) — no payloads, no lease tokens.
    let store = ControlPlaneStore::open(&db)?;
    println!("[reopen] store reopened; recovering claim {claim_tx}");
    match store.recover_claim(claim_tx, now_ms())? {
        ClaimRecovery::Committed(claims) => {
            // Jobs whose leases lapsed before recovery are stale: they must not
            // be executed with the recovered tokens. Reconcilable stale jobs
            // surface as Uncertain (effect status unknown) until reconciled.
            for stale in claims.stale_jobs() {
                println!("[recover] stale job {stale}: not executable; surfaces as Uncertain");
            }
            if claims.is_empty() {
                println!("[recover] claim committed but no executable jobs remain; no lease held");
            } else {
                println!(
                    "[recover] claim receipt found: {} job(s), original lease tokens restored",
                    claims.len()
                );
                for job in claims.into_jobs() {
                    heartbeat(&store, &job)?;
                    start_turn(&store, "t-200")?;
                    call_provider(&job); // exactly once, under the recovered lease
                    complete_turn(&store, "t-200", &job)?;
                }
            }
        }
        ClaimRecovery::MaintenanceCommitted(_) => {
            println!("[recover] claim recorded maintenance only; job still claimable")
        }
        ClaimRecovery::Absent => println!("[recover] claim never committed; job still claimable"),
    }
    println!(
        "[state]  threads/t-200 = {}",
        thread_status(&store, "t-200")
    );

    let succeeded = store.jobs(Some(QUEUE), Some(JobState::Succeeded), 10)?;
    println!(
        "[state]  {} succeeded job(s) on queue {QUEUE}; {} event(s) total",
        succeeded.len(),
        store.events_after(0, 100)?.len()
    );
    Ok(())
}
