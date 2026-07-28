//! Deterministic model-based contract test: a fixed-seed pseudo-random sequence of
//! valid and invalid operations is applied to both an in-memory reference model and
//! a file-backed store, with periodic close/reopen, asserting equivalence throughout.

mod common;

use std::collections::BTreeMap;

use common::{db_path, event, id, open, temp_dir, Prng};
use minisqlite::{
    ClaimOutcome, ClaimRequest, CommitBatch, CommitError, ControlPlaneStore, Id, JobSpec, JobState,
    ProjectionPatch,
};

const STREAMS: [&str; 4] = ["orders", "users", "billing", "audit"];
const LEASE_MS: i64 = 1_000_000; // large enough that leases never expire in-model

#[derive(Debug, Clone, Copy, PartialEq)]
enum ModelJobState {
    Ready, // Pending or RetryWait: claimable
    Leased,
    Succeeded,
    Dead,
}

#[derive(Debug, Clone)]
struct ModelJob {
    attempt: u32,
    max_attempts: u32,
    state: ModelJobState,
    lease_token: Id,
}

/// The in-memory reference model of streams, projections, and jobs.
#[derive(Default)]
struct Model {
    stream_versions: BTreeMap<String, u64>,
    // Global event log: (stream_id, event_id) in commit order.
    events: Vec<(String, u128)>,
    projection_version: u64,
    projection: BTreeMap<Vec<u8>, Vec<u8>>,
    jobs: BTreeMap<u128, ModelJob>,
}

impl Model {
    fn stream_version(&self, stream: &str) -> u64 {
        self.stream_versions.get(stream).copied().unwrap_or(0)
    }

    fn append(&mut self, stream: &str, event_id: u128) {
        let next = self.stream_version(stream) + 1;
        self.stream_versions.insert(stream.to_string(), next);
        self.events.push((stream.to_string(), event_id));
    }

    fn jobs_in(&self, state: ModelJobState) -> Vec<u128> {
        self.jobs
            .iter()
            .filter(|(_, j)| j.state == state)
            .map(|(&id, _)| id)
            .collect()
    }
}

struct Harness {
    store: Option<ControlPlaneStore>,
    model: Model,
    rng: Prng,
    next_txn: u128,
    next_event: u128,
    next_job: u128,
    now_ms: i64,
    // Committed batches retained for idempotent-resubmission checks.
    committed: Vec<CommitBatch>,
    with_projections_and_jobs: bool,
}

impl Harness {
    fn store(&self) -> &ControlPlaneStore {
        self.store.as_ref().expect("store open")
    }

    fn fresh_txn(&mut self) -> u128 {
        self.next_txn += 1;
        self.next_txn
    }

    fn commit_ok(&mut self, batch: CommitBatch) {
        self.store().commit(&batch).unwrap();
        self.committed.push(batch);
    }

    // --- operations ---

    fn op_append_events(&mut self) {
        let txn = self.fresh_txn();
        let stream = STREAMS[self.rng.below(STREAMS.len() as u64) as usize];
        let count = 1 + self.rng.below(3);
        let mut batch = CommitBatch::new(id(txn), self.now_ms);
        if self.rng.below(2) == 0 {
            batch = batch.expect_stream_version(stream, self.model.stream_version(stream));
        }
        let mut appended = Vec::new();
        for _ in 0..count {
            self.next_event += 1;
            batch = batch.append_event(event(self.next_event, stream, "tick"));
            appended.push(self.next_event);
        }
        self.commit_ok(batch);
        for event_id in appended {
            self.model.append(stream, event_id);
        }
    }

    fn op_wrong_stream_version(&mut self) {
        let txn = self.fresh_txn();
        let stream = STREAMS[self.rng.below(STREAMS.len() as u64) as usize];
        self.next_event += 1;
        let batch = CommitBatch::new(id(txn), self.now_ms)
            .expect_stream_version(stream, self.model.stream_version(stream) + 1)
            .append_event(event(self.next_event, stream, "tick"));
        assert!(matches!(
            self.store().commit(&batch).unwrap_err(),
            CommitError::Conflict(_)
        ));
        // Model unchanged: the invalid commit persisted nothing.
    }

    fn op_resubmit_identical(&mut self) {
        if self.committed.is_empty() {
            return self.op_append_events();
        }
        let idx = self.rng.below(self.committed.len() as u64) as usize;
        let batch = self.committed[idx].clone();
        // Idempotent resubmission succeeds and changes nothing.
        self.store().commit(&batch).unwrap();
    }

    fn op_resubmit_different(&mut self) {
        if self.committed.is_empty() {
            return self.op_append_events();
        }
        let idx = self.rng.below(self.committed.len() as u64) as usize;
        let txn_id = self.committed[idx].transaction_id();
        self.next_event += 1;
        let different = CommitBatch::new(txn_id, self.now_ms + 1).append_event(event(
            self.next_event,
            "orders",
            "tampered",
        ));
        assert_eq!(
            self.store().commit(&different).unwrap_err(),
            CommitError::DuplicateIdWithDifferentContent
        );
    }

    fn op_projection_patch(&mut self) {
        let txn = self.fresh_txn();
        let key = vec![b'k', self.rng.below(8) as u8];
        let expected = self.model.projection_version;
        let delete = self.rng.below(4) == 0 && self.model.projection.contains_key(&key);
        let patch = if delete {
            ProjectionPatch::new("model", expected).delete(key.clone())
        } else {
            let value = self.rng.next_u64().to_be_bytes().to_vec();
            self.model.projection.insert(key.clone(), value.clone());
            ProjectionPatch::new("model", expected).put(key.clone(), value)
        };
        if delete {
            self.model.projection.remove(&key);
        }
        self.commit_ok(CommitBatch::new(id(txn), self.now_ms).apply_projection_patch(patch));
        self.model.projection_version += 1;
    }

    fn op_projection_conflict(&mut self) {
        let txn = self.fresh_txn();
        let stale = ProjectionPatch::new("model", self.model.projection_version + 7).put("k", "v");
        assert!(matches!(
            self.store()
                .commit(&CommitBatch::new(id(txn), self.now_ms).apply_projection_patch(stale))
                .unwrap_err(),
            CommitError::Conflict(_) | CommitError::Validation(_)
        ));
    }

    fn op_enqueue_job(&mut self) {
        let txn = self.fresh_txn();
        self.next_job += 1;
        let job_id = self.next_job;
        let max_attempts = 1 + self.rng.below(3) as u32;
        // One partition per job keeps every ready job a claimable partition head.
        let spec = JobSpec::intrinsically_idempotent(
            id(job_id),
            "modelq",
            format!("part-{job_id}"),
            job_id.to_be_bytes().to_vec(),
        )
        .with_max_attempts(max_attempts);
        self.commit_ok(CommitBatch::new(id(txn), self.now_ms).enqueue_job(spec));
        self.model.jobs.insert(
            job_id,
            ModelJob {
                attempt: 0,
                max_attempts,
                state: ModelJobState::Ready,
                lease_token: Id::ZERO,
            },
        );
    }

    fn op_claim(&mut self) {
        let request = ClaimRequest {
            queue: "modelq".into(),
            worker_id: "model-worker".into(),
            now_ms: self.now_ms,
            lease_ms: LEASE_MS,
            limit: 1 + self.rng.below(4) as usize,
            partition_key: None,
        };
        let ready = self.model.jobs_in(ModelJobState::Ready);
        match self.store().claim_jobs(&request).unwrap() {
            ClaimOutcome::Noop | ClaimOutcome::MaintenanceCommitted(_) => {
                assert!(
                    ready.is_empty(),
                    "store granted no leases while the model had ready jobs {ready:?}"
                );
            }
            ClaimOutcome::Committed(claims) => {
                assert!(!claims.is_empty());
                assert!(claims.len() <= request.limit);
                for job in claims {
                    let job_key = u128::from(job.job_id);
                    let model_job = self
                        .model
                        .jobs
                        .get_mut(&job_key)
                        .expect("claimed job exists in model");
                    assert_eq!(
                        model_job.state,
                        ModelJobState::Ready,
                        "store leased job {job_key} that was not ready in the model"
                    );
                    assert_eq!(job.attempt, model_job.attempt + 1);
                    model_job.state = ModelJobState::Leased;
                    model_job.attempt += 1;
                    model_job.lease_token = job.lease_token;
                }
            }
        }
    }

    fn op_ack(&mut self) {
        let leased = self.model.jobs_in(ModelJobState::Leased);
        if leased.is_empty() {
            return self.op_append_events();
        }
        let job_id = leased[self.rng.below(leased.len() as u64) as usize];
        let token = self.model.jobs[&job_id].lease_token;
        let txn = self.fresh_txn();
        self.commit_ok(CommitBatch::new(id(txn), self.now_ms).acknowledge_job(
            id(job_id),
            token,
            None,
        ));
        self.model.jobs.get_mut(&job_id).unwrap().state = ModelJobState::Succeeded;
    }

    fn op_fail(&mut self) {
        let leased = self.model.jobs_in(ModelJobState::Leased);
        if leased.is_empty() {
            return self.op_append_events();
        }
        let job_id = leased[self.rng.below(leased.len() as u64) as usize];
        let (token, attempt, max_attempts) = {
            let j = &self.model.jobs[&job_id];
            (j.lease_token, j.attempt, j.max_attempts)
        };
        let txn = self.fresh_txn();
        self.commit_ok(CommitBatch::new(id(txn), self.now_ms).fail_job(
            id(job_id),
            token,
            "model failure",
            Some(0),
        ));
        let model_job = self.model.jobs.get_mut(&job_id).unwrap();
        model_job.state = if attempt >= max_attempts {
            ModelJobState::Dead
        } else {
            ModelJobState::Ready
        };
    }

    // --- equivalence ---

    fn check_equivalence(&self) {
        let store = self.store();

        // Streams: versions and full per-stream event sequences match.
        for stream in STREAMS {
            let expected_version = self.model.stream_version(stream);
            assert_eq!(store.stream_version(stream).unwrap(), expected_version);
            let events = store.stream_events(stream, 1, 10_000).unwrap();
            let expected_ids: Vec<u128> = self
                .model
                .events
                .iter()
                .filter(|(s, _)| s == stream)
                .map(|&(_, e)| e)
                .collect();
            assert_eq!(events.len() as u64, expected_version);
            for (persisted, expected_id) in events.iter().zip(&expected_ids) {
                assert_eq!(persisted.event.event_id, id(*expected_id));
            }
        }

        // Global log: total order matches the model's commit order.
        let all = store.events_after(0, 100_000).unwrap();
        assert_eq!(all.len(), self.model.events.len());
        for (persisted, (stream, event_id)) in all.iter().zip(&self.model.events) {
            assert_eq!(&persisted.event.stream_id, stream);
            assert_eq!(persisted.event.event_id, id(*event_id));
        }

        if !self.with_projections_and_jobs {
            return;
        }

        // Projection: version and full contents match.
        assert_eq!(
            store.projection_version("model").unwrap(),
            self.model.projection_version
        );
        let entries = store.projection_scan_prefix("model", b"", 10_000).unwrap();
        assert_eq!(entries.len(), self.model.projection.len());
        for entry in entries {
            assert_eq!(self.model.projection.get(&entry.key), Some(&entry.value));
        }

        // Jobs: per-job state matches.
        for (&job_id, model_job) in &self.model.jobs {
            let info = store.job(id(job_id)).unwrap().expect("job exists");
            let expected_states: &[JobState] = match model_job.state {
                ModelJobState::Ready => &[JobState::Pending, JobState::RetryWait],
                ModelJobState::Leased => &[JobState::Leased],
                ModelJobState::Succeeded => &[JobState::Succeeded],
                ModelJobState::Dead => &[JobState::Dead],
            };
            assert!(
                expected_states.contains(&info.state),
                "job {job_id}: store state {:?}, model state {:?}",
                info.state,
                model_job.state
            );
            assert_eq!(info.attempt, model_job.attempt);
        }
    }

    fn reopen(&mut self, path: &std::path::Path) {
        self.store = None;
        self.store = Some(open(path));
    }

    fn step(&mut self) {
        self.now_ms += 1 + self.rng.below(50) as i64;
        let roll = self.rng.below(100);
        if self.with_projections_and_jobs {
            match roll {
                0..=29 => self.op_append_events(),
                30..=36 => self.op_wrong_stream_version(),
                37..=42 => self.op_resubmit_identical(),
                43..=47 => self.op_resubmit_different(),
                48..=59 => self.op_projection_patch(),
                60..=64 => self.op_projection_conflict(),
                65..=74 => self.op_enqueue_job(),
                75..=84 => self.op_claim(),
                85..=92 => self.op_ack(),
                _ => self.op_fail(),
            }
        } else {
            match roll {
                0..=59 => self.op_append_events(),
                60..=74 => self.op_wrong_stream_version(),
                75..=87 => self.op_resubmit_identical(),
                _ => self.op_resubmit_different(),
            }
        }
    }
}

fn run_model(seed: u64, steps: u32, with_projections_and_jobs: bool) {
    let dir = temp_dir();
    let path = db_path(&dir);
    let mut harness = Harness {
        store: Some(open(&path)),
        model: Model::default(),
        rng: Prng::new(seed),
        next_txn: 0,
        next_event: 0,
        next_job: 0,
        now_ms: 1_000,
        committed: Vec::new(),
        with_projections_and_jobs,
    };

    for step in 1..=steps {
        harness.step();
        if step % 25 == 0 {
            harness.check_equivalence();
        }
        if step % 40 == 0 {
            harness.reopen(&path);
            // A fresh handle must answer reads immediately with no replay.
            harness.check_equivalence();
        }
    }
    harness.check_equivalence();
}

#[test]
fn model_equivalence_events_only() {
    run_model(0xDEC0DE, 300, false);
}

#[test]
fn model_equivalence_full() {
    run_model(0xC0FFEE, 300, true);
}
