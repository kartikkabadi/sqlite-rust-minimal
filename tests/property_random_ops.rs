//! Property-based test: apply long random sequences of commits (events, projection
//! patches, job enqueues) plus claim/ack/fail/cancel job operations against a simple
//! in-memory model, then verify every invariant the kernel promises — event counts
//! and stream versions, projection versions and full contents, and job states —
//! both live and after reopening the store from disk.

mod common;

use std::collections::{BTreeMap, HashMap};

use common::Prng;
use minisqlite::{
    ClaimOutcome, ClaimRequest, CommitBatch, ControlPlaneStore, Event, Id, JobSpec, JobState,
    ProjectionMutation, ProjectionPatch, Resolution,
};

type Entries = BTreeMap<Vec<u8>, Vec<u8>>;
type ProjectionState = (u64, Entries);

const STREAMS: u64 = 6;
const PROJECTIONS: u64 = 4;
const QUEUES: u64 = 2;
const PARTITIONS: u64 = 5;
const MAX_ATTEMPTS: u32 = 3;
const LEASE_MS: i64 = 1_000_000_000; // effectively never expires within a run

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ModelJobState {
    Pending,
    Leased,
    RetryWait,
    Succeeded,
    Dead,
    Cancelled,
}

struct ModelJob {
    queue: String,
    state: ModelJobState,
    lease_token: Option<Id>,
    attempt: u32,
}

#[derive(Default)]
struct Model {
    streams: HashMap<String, u64>,
    events_total: u64,
    transactions: u64,
    projections: HashMap<String, ProjectionState>,
    jobs: BTreeMap<Id, ModelJob>,
    next_id: u128,
    now_ms: i64,
}

impl Model {
    fn fresh_id(&mut self) -> Id {
        self.next_id += 1;
        Id::from(self.next_id)
    }

    fn jobs_in(&self, state: ModelJobState) -> Vec<Id> {
        self.jobs
            .iter()
            .filter(|(_, j)| j.state == state)
            .map(|(id, _)| *id)
            .collect()
    }
}

fn expected_state(state: ModelJobState) -> JobState {
    match state {
        ModelJobState::Pending => JobState::Pending,
        ModelJobState::Leased => JobState::Leased,
        ModelJobState::RetryWait => JobState::RetryWait,
        ModelJobState::Succeeded => JobState::Succeeded,
        ModelJobState::Dead => JobState::Dead,
        ModelJobState::Cancelled => JobState::Cancelled,
    }
}

fn random_commit(store: &ControlPlaneStore, model: &mut Model, rng: &mut Prng) {
    let mut batch = CommitBatch::new(model.fresh_id(), model.now_ms);
    let ops = 1 + rng.below(4);
    let mut staged_streams: HashMap<String, u64> = HashMap::new();
    let mut staged_projections: HashMap<String, ProjectionState> = HashMap::new();
    let mut staged_jobs: Vec<(Id, String)> = Vec::new();
    let mut staged_events = 0u64;
    for _ in 0..ops {
        match rng.below(3) {
            0 => {
                let stream = format!("s{}", rng.below(STREAMS));
                batch = batch.append_event(Event::with_json_payload(
                    model.fresh_id(),
                    &stream,
                    "evt",
                    model.now_ms,
                    b"{}",
                ));
                *staged_streams
                    .entry(stream.clone())
                    .or_insert_with(|| *model.streams.get(&stream).unwrap_or(&0)) += 1;
                staged_events += 1;
            }
            1 => {
                let name = format!("p{}", rng.below(PROJECTIONS));
                let (version, entries) = staged_projections
                    .entry(name.clone())
                    .or_insert_with(|| model.projections.get(&name).cloned().unwrap_or_default());
                let mut patch = ProjectionPatch::new(&name, *version);
                if rng.below(20) == 0 {
                    patch = patch.clear();
                    entries.clear();
                }
                let mut used: Vec<Vec<u8>> = Vec::new();
                for _ in 0..1 + rng.below(3) {
                    let key = format!("k{}", rng.below(12)).into_bytes();
                    if used.contains(&key) {
                        continue; // duplicate keys in one patch are rejected by design
                    }
                    used.push(key.clone());
                    if rng.below(4) == 0 {
                        patch = patch.delete(key.clone());
                        entries.remove(&key);
                    } else {
                        let value = format!("v{}", rng.next_u64()).into_bytes();
                        patch = patch.put(key.clone(), value.clone());
                        entries.insert(key, value);
                    }
                }
                *version += 1;
                batch = batch.apply_projection_patch(patch);
            }
            _ => {
                let job_id = model.fresh_id();
                let queue = format!("q{}", rng.below(QUEUES));
                batch = batch.enqueue_job(
                    JobSpec::intrinsically_idempotent(
                        job_id,
                        &queue,
                        format!("part{}", rng.below(PARTITIONS)),
                        vec![rng.next_u64() as u8],
                    )
                    .with_max_attempts(MAX_ATTEMPTS),
                );
                staged_jobs.push((job_id, queue));
            }
        }
    }
    store.commit(&batch).unwrap();
    // The batch is atomic: fold all staged effects into the model at once.
    model.transactions += 1;
    model.events_total += staged_events;
    for (stream, version) in staged_streams {
        model.streams.insert(stream, version);
    }
    for (name, state) in staged_projections {
        model.projections.insert(name, state);
    }
    for (job_id, queue) in staged_jobs {
        model.jobs.insert(
            job_id,
            ModelJob {
                queue,
                state: ModelJobState::Pending,
                lease_token: None,
                attempt: 0,
            },
        );
    }
}

fn random_claim(store: &ControlPlaneStore, model: &mut Model, rng: &mut Prng) {
    let queue = format!("q{}", rng.below(QUEUES));
    let outcome = store
        .claim_jobs(&ClaimRequest {
            queue: queue.clone(),
            worker_id: "prop-worker".into(),
            now_ms: model.now_ms,
            lease_ms: LEASE_MS,
            limit: 1 + rng.below(4) as usize,
            partition_key: None,
        })
        .unwrap();
    match outcome {
        ClaimOutcome::Noop => {}
        ClaimOutcome::MaintenanceCommitted(_) => {
            panic!("no lease can expire within this run (lease_ms={LEASE_MS})")
        }
        ClaimOutcome::Committed(claims) => {
            model.transactions += 1;
            for claimed in claims {
                let job = model.jobs.get_mut(&claimed.job_id).unwrap_or_else(|| {
                    panic!("claimed unknown job {}", claimed.job_id);
                });
                assert_eq!(
                    job.queue, queue,
                    "job {} claimed from wrong queue",
                    claimed.job_id
                );
                assert!(
                    matches!(job.state, ModelJobState::Pending | ModelJobState::RetryWait),
                    "job {} claimed while {:?}",
                    claimed.job_id,
                    job.state
                );
                assert_eq!(claimed.attempt, job.attempt + 1, "attempt counter skipped");
                job.state = ModelJobState::Leased;
                job.lease_token = Some(claimed.lease_token);
                job.attempt = claimed.attempt;
            }
        }
    }
}

fn random_job_transition(store: &ControlPlaneStore, model: &mut Model, rng: &mut Prng) {
    let leased = model.jobs_in(ModelJobState::Leased);
    if leased.is_empty() {
        return;
    }
    let job_id = leased[rng.below(leased.len() as u64) as usize];
    let token = model.jobs[&job_id].lease_token.unwrap();
    let txn = model.fresh_id();
    match rng.below(3) {
        0 => {
            store
                .commit(&CommitBatch::new(txn, model.now_ms).acknowledge_job(job_id, token, None))
                .unwrap();
            model.jobs.get_mut(&job_id).unwrap().state = ModelJobState::Succeeded;
        }
        1 => {
            store
                .commit(&CommitBatch::new(txn, model.now_ms).fail_job(
                    job_id,
                    token,
                    "induced failure",
                    Some(model.now_ms + 1),
                ))
                .unwrap();
            let job = model.jobs.get_mut(&job_id).unwrap();
            job.state = if job.attempt >= MAX_ATTEMPTS {
                ModelJobState::Dead
            } else {
                ModelJobState::RetryWait
            };
            job.lease_token = None;
        }
        _ => {
            store
                .commit(&CommitBatch::new(txn, model.now_ms).cancel_job(job_id, Some(token)))
                .unwrap();
            let job = model.jobs.get_mut(&job_id).unwrap();
            job.state = ModelJobState::Cancelled;
            job.lease_token = None;
        }
    }
    model.transactions += 1;
}

fn verify_against_model(store: &ControlPlaneStore, model: &Model) {
    let report = store.verify().unwrap();
    assert!(report.findings.is_empty(), "verify: {:?}", report.findings);

    assert_eq!(
        store.events_after(0, usize::MAX).unwrap().len() as u64,
        model.events_total
    );
    for i in 0..STREAMS {
        let stream = format!("s{i}");
        assert_eq!(
            store.stream_version(&stream).unwrap(),
            *model.streams.get(&stream).unwrap_or(&0),
            "stream {stream} version"
        );
    }
    for i in 0..PROJECTIONS {
        let name = format!("p{i}");
        let (version, entries) = model.projections.get(&name).cloned().unwrap_or_default();
        assert_eq!(
            store.projection_version(&name).unwrap(),
            version,
            "projection {name} version"
        );
        assert_eq!(
            store.projection_entry_count(&name).unwrap(),
            entries.len() as u64,
            "projection {name} entry count"
        );
        let stored: Entries = store
            .projection_scan_prefix(&name, b"", usize::MAX)
            .unwrap()
            .into_iter()
            .map(|e| (e.key, e.value))
            .collect();
        assert_eq!(stored, entries, "projection {name} contents");
    }
    for (job_id, job) in &model.jobs {
        let info = store.job(*job_id).unwrap().unwrap();
        assert_eq!(
            info.state,
            expected_state(job.state),
            "job {job_id} state (attempt {})",
            job.attempt
        );
        assert_eq!(info.attempt, job.attempt, "job {job_id} attempt");
    }
    let stats = store.stats().unwrap();
    assert_eq!(stats.transactions, model.transactions);
    assert_eq!(stats.events, model.events_total);
}

fn run_seed(seed: u64, steps: u64) {
    let dir = common::temp_dir();
    let db = common::db_path(&dir);
    let store = common::open(&db);
    let mut rng = Prng::new(seed);
    let mut model = Model {
        now_ms: 1_000,
        ..Model::default()
    };
    for _ in 0..steps {
        model.now_ms += 1 + rng.below(10) as i64;
        match rng.below(10) {
            0..=5 => random_commit(&store, &mut model, &mut rng),
            6..=7 => random_claim(&store, &mut model, &mut rng),
            _ => random_job_transition(&store, &mut model, &mut rng),
        }
    }
    verify_against_model(&store, &model);
    // Everything must survive a close and reopen.
    drop(store);
    let reopened = common::open(&db);
    verify_against_model(&reopened, &model);
}

#[test]
fn random_ops_seed_1_match_model() {
    run_seed(0xA11CE, 400);
}

#[test]
fn random_ops_seed_2_match_model() {
    run_seed(0xB0B, 400);
}

#[test]
fn random_ops_seed_3_match_model() {
    run_seed(0xC0DE, 400);
}

/// Structure-aware random-ops property (spec Part B, Layer 7): drive random
/// sequences of commits, claims, acknowledgements, failures, cancellations,
/// lease extensions, and uncertain resolutions against a fresh store. The
/// store must never panic, and `verify` must report a consistent database.
fn run_fuzz_ops(seed: u64, steps: u64) {
    let dir = common::temp_dir();
    let store = common::open(&common::db_path(&dir));
    let mut rng = Prng::new(seed);
    let mut next_id: u128 = 1;
    let mut leases: Vec<(Id, Id)> = Vec::new();
    let mut uncertain: Vec<Id> = Vec::new();

    let pick = |leases: &mut Vec<(Id, Id)>, rng: &mut Prng| -> Option<(Id, Id)> {
        if leases.is_empty() {
            return None;
        }
        let idx = rng.below(leases.len() as u64) as usize;
        Some(leases.swap_remove(idx))
    };

    for step in 0..steps {
        let now = 1_000 + step as i64;
        next_id += 1;
        let txn = Id::from(next_id);
        match rng.below(8) {
            0 => {
                next_id += 1;
                let event_id = Id::from(next_id);
                let stream = format!("s{}", rng.below(4));
                let _ = store.commit(
                    &CommitBatch::new(txn, now)
                        .append_event(Event::with_json_payload(event_id, &stream, "t", now, b"{}")),
                );
            }
            1 => {
                next_id += 1;
                let job_id = Id::from(next_id);
                let queue = format!("q{}", rng.below(2));
                let part = format!("p{}", rng.below(4));
                let _ = store.commit(&CommitBatch::new(txn, now).enqueue_job(
                    JobSpec::reconcilable(job_id, &queue, &part, vec![rng.next_u64() as u8]),
                ));
            }
            2 => {
                let queue = format!("q{}", rng.below(2));
                if let Ok(ClaimOutcome::Committed(claims)) = store.claim_jobs(&ClaimRequest {
                    queue,
                    worker_id: "w".into(),
                    now_ms: now,
                    lease_ms: 1 + rng.below(256) as i64,
                    limit: 1 + rng.below(4) as usize,
                    partition_key: None,
                }) {
                    for job in claims.into_jobs() {
                        leases.push((job.job_id, job.lease_token));
                    }
                }
            }
            3 => {
                if let Some((job_id, token)) = pick(&mut leases, &mut rng) {
                    let _ = store
                        .commit(&CommitBatch::new(txn, now).acknowledge_job(job_id, token, None));
                }
            }
            4 => {
                if let Some((job_id, token)) = pick(&mut leases, &mut rng) {
                    let retry = if rng.below(2) == 0 {
                        Some(now + rng.below(256) as i64)
                    } else {
                        None
                    };
                    let _ = store
                        .commit(&CommitBatch::new(txn, now).fail_job(job_id, token, "e", retry));
                }
            }
            5 => {
                if let Some((job_id, token)) = pick(&mut leases, &mut rng) {
                    let _ = store.extend_lease(job_id, token, now + rng.below(256) as i64, now);
                    leases.push((job_id, token));
                }
            }
            6 => {
                for job in store.jobs(None, Some(JobState::Uncertain), 4).unwrap() {
                    uncertain.push(job.job_id);
                }
                if let Some(job_id) = uncertain.pop() {
                    let resolution = match rng.below(3) {
                        0 => Resolution::Retry,
                        1 => Resolution::MarkSucceeded,
                        _ => Resolution::MarkDead,
                    };
                    let _ = store.commit(
                        &CommitBatch::new(txn, now).resolve_uncertain_job(job_id, resolution),
                    );
                }
            }
            _ => {
                let projection = format!("proj{}", rng.below(2));
                let version = store.projection_version(&projection).unwrap();
                let _ = store.commit(&CommitBatch::new(txn, now).apply_projection_patch(
                    ProjectionPatch {
                        projection,
                        expected_version: version,
                        new_version: version + 1,
                        mutations: vec![ProjectionMutation::Put {
                            key: vec![rng.next_u64() as u8],
                            value: vec![rng.next_u64() as u8],
                        }],
                    },
                ));
            }
        }
    }

    let report = store.verify().unwrap();
    assert!(report.is_ok(), "verify findings: {:?}", report.findings);
}

#[test]
fn fuzz_ops_property_seed_1() {
    run_fuzz_ops(0xF00D, 600);
}

#[test]
fn fuzz_ops_property_seed_2() {
    run_fuzz_ops(0xFEED, 600);
}
