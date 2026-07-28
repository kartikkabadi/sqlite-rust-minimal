//! Job apply, claim, lease, and read functions.
//!
//! Design notes on unspecified details (per shared spec, simplest consistent choice):
//! - `JobFailure::retry_after_ms` is an absolute timestamp; `None` resolves to
//!   `committed_at_ms + DEFAULT_RETRY_DELAY_MS`.
//! - Expired-lease maintenance is bounded to `MAINTENANCE_LIMIT` rows per claim call
//!   and scoped to the requested queue.
//! - An expired `ExplicitlyIdempotent` lease with attempts remaining returns to
//!   `Pending` (immediately claimable) rather than `RetryWait`.
//! - Head selection during a claim uses the pre-maintenance state, so a job whose
//!   expired lease is repaired in the same call is never leased by that call.
//! - Missing jobs and stale lease tokens in commit operations are `ValidationError`s;
//!   invalid state transitions are `Conflict::JobTransition`.

use std::sync::LazyLock;

use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior};

use crate::config::EffectMode;
use crate::error::{
    ClaimError, Conflict, Error, IndeterminateLease, LeaseConflict, LeaseError, RecoveryError,
    StorageError, ValidationError,
};
use crate::id::Id;
use crate::jobs::{
    ClaimOutcome, ClaimRecovery, ClaimRequest, ClaimedJob, CommittedClaims, IndeterminateClaim,
    JobAck, JobCancellation, JobInfo, JobResolution, JobSpec, JobState, LeaseExtension,
    LeaseExtensionReceipt, MaintenanceReceipt, Resolution,
};

/// Default retry delay applied when a `JobFailure` does not specify `retry_after_ms`.
const DEFAULT_RETRY_DELAY_MS: i64 = 30_000;

/// Maximum expired leases repaired per claim call.
const MAINTENANCE_LIMIT: usize = 64;

/// Comma-separated terminal state codes for SQL `IN` clauses, derived from
/// [`JobState::encode`] so the encoding lives in one place.
pub(crate) static TERMINAL_STATES_SQL: LazyLock<String> = LazyLock::new(|| {
    format!(
        "{}, {}, {}",
        JobState::Succeeded.encode(),
        JobState::Dead.encode(),
        JobState::Cancelled.encode()
    )
});

/// Comma-separated nonterminal state codes for SQL `IN` clauses.
pub(crate) static ACTIVE_STATES_SQL: LazyLock<String> = LazyLock::new(|| {
    format!(
        "{}, {}, {}, {}",
        JobState::Pending.encode(),
        JobState::Leased.encode(),
        JobState::RetryWait.encode(),
        JobState::Uncertain.encode()
    )
});

const JOB_COLUMNS: &str = "job_id, enqueue_sequence, queue, partition_key, payload, \
     not_before_ms, max_attempts, effect_mode, idempotency_key, state, attempt, \
     lease_token, worker_id, lease_expires_at_ms, retry_after_ms, terminal_at_ms, \
     result_digest, error_summary";

/// A fully decoded row from the `jobs` table.
struct JobRow {
    job_id: Id,
    queue: String,
    partition_key: String,
    payload: Vec<u8>,
    not_before_ms: i64,
    max_attempts: u32,
    effect_mode: EffectMode,
    idempotency_key: Option<String>,
    state: JobState,
    attempt: u32,
    lease_token: Option<Id>,
    worker_id: Option<String>,
    lease_expires_at_ms: Option<i64>,
    retry_after_ms: Option<i64>,
    terminal_at_ms: Option<i64>,
    result_digest: Option<Vec<u8>>,
    error_summary: Option<String>,
}

impl JobRow {
    fn into_info(self) -> JobInfo {
        JobInfo {
            job_id: self.job_id,
            spec: JobSpec {
                job_id: self.job_id,
                queue: self.queue,
                partition_key: self.partition_key,
                payload: self.payload,
                not_before_ms: self.not_before_ms,
                max_attempts: self.max_attempts,
                effect_mode: self.effect_mode,
                idempotency_key: self.idempotency_key,
            },
            state: self.state,
            attempt: self.attempt,
            lease_expires_at_ms: self.lease_expires_at_ms,
            worker_id: self.worker_id,
            retry_after_ms: self.retry_after_ms,
            terminal_at_ms: self.terminal_at_ms,
            result_digest: self.result_digest,
            error_summary: self.error_summary,
        }
    }
}

fn id_from_blob(blob: Vec<u8>) -> Result<Id, StorageError> {
    let bytes: [u8; 16] = blob
        .try_into()
        .map_err(|_| StorageError::Sqlite("corrupt 16-byte id column".into()))?;
    Ok(Id::from_bytes(bytes))
}

fn opt_id_from_blob(blob: Option<Vec<u8>>) -> Result<Option<Id>, StorageError> {
    blob.map(id_from_blob).transpose()
}

fn row_to_job(row: &rusqlite::Row<'_>) -> rusqlite::Result<Result<JobRow, StorageError>> {
    let job_id: Vec<u8> = row.get(0)?;
    let queue: String = row.get(2)?;
    let partition_key: String = row.get(3)?;
    let payload: Vec<u8> = row.get(4)?;
    let not_before_ms: i64 = row.get(5)?;
    let max_attempts: i64 = row.get(6)?;
    let effect_mode: i64 = row.get(7)?;
    let idempotency_key: Option<String> = row.get(8)?;
    let state: i64 = row.get(9)?;
    let attempt: i64 = row.get(10)?;
    let lease_token: Option<Vec<u8>> = row.get(11)?;
    let worker_id: Option<String> = row.get(12)?;
    let lease_expires_at_ms: Option<i64> = row.get(13)?;
    let retry_after_ms: Option<i64> = row.get(14)?;
    let terminal_at_ms: Option<i64> = row.get(15)?;
    let result_digest: Option<Vec<u8>> = row.get(16)?;
    let error_summary: Option<String> = row.get(17)?;
    Ok((|| {
        Ok(JobRow {
            job_id: id_from_blob(job_id)?,
            queue,
            partition_key,
            payload,
            not_before_ms,
            max_attempts: max_attempts as u32,
            effect_mode: EffectMode::decode(effect_mode).ok_or_else(|| {
                StorageError::Sqlite(format!("corrupt effect mode {effect_mode}"))
            })?,
            idempotency_key,
            state: JobState::decode(state)
                .ok_or_else(|| StorageError::Sqlite(format!("corrupt job state {state}")))?,
            attempt: attempt as u32,
            lease_token: opt_id_from_blob(lease_token)?,
            worker_id,
            lease_expires_at_ms,
            retry_after_ms,
            terminal_at_ms,
            result_digest,
            error_summary,
        })
    })())
}

fn load_job(conn: &Connection, job_id: Id) -> Result<Option<JobRow>, StorageError> {
    let row = conn
        .query_row(
            &format!("SELECT {JOB_COLUMNS} FROM jobs WHERE job_id = ?1"),
            [job_id.as_bytes().as_slice()],
            row_to_job,
        )
        .optional()
        .map_err(StorageError::from_sqlite)?;
    row.transpose()
}

fn require_job(conn: &Connection, job_id: Id) -> Result<JobRow, Error> {
    load_job(conn, job_id)?
        .ok_or_else(|| ValidationError(format!("job {job_id} does not exist")).into())
}

fn ensure_transition(job: &JobRow, to: JobState) -> Result<(), Error> {
    if job.state.can_transition_to(to) {
        Ok(())
    } else {
        Err(Conflict::JobTransition {
            job_id: job.job_id,
            from: job.state,
            to,
        }
        .into())
    }
}

fn ensure_lease_token(job: &JobRow, token: Id) -> Result<(), Error> {
    if job.lease_token == Some(token) {
        Ok(())
    } else {
        Err(ValidationError(format!("stale lease token for job {}", job.job_id)).into())
    }
}

/// Remove `(queue, partition_key)` from `active_partitions` when no nonterminal jobs
/// remain in that partition.
fn drain_partition_if_empty(
    conn: &Connection,
    queue: &str,
    partition_key: &str,
) -> Result<(), StorageError> {
    conn.execute(
        &format!(
            "DELETE FROM active_partitions WHERE queue = ?1 AND partition_key = ?2 AND NOT EXISTS \
             (SELECT 1 FROM jobs WHERE queue = ?1 AND partition_key = ?2 AND state IN ({}))",
            ACTIVE_STATES_SQL.as_str(),
        ),
        rusqlite::params![queue, partition_key],
    )
    .map_err(StorageError::from_sqlite)?;
    Ok(())
}

fn set_terminal(
    conn: &Connection,
    job: &JobRow,
    state: JobState,
    now_ms: i64,
    transaction_id: Id,
    result_digest: Option<&[u8]>,
    error_summary: Option<&str>,
) -> Result<(), StorageError> {
    conn.execute(
        "UPDATE jobs SET state = ?2, lease_token = NULL, worker_id = NULL, \
         lease_expires_at_ms = NULL, retry_after_ms = NULL, terminal_at_ms = ?3, \
         result_digest = COALESCE(?4, result_digest), \
         error_summary = COALESCE(?5, error_summary), updated_transaction_id = ?6 \
         WHERE job_id = ?1",
        rusqlite::params![
            job.job_id.as_bytes().as_slice(),
            state.encode(),
            now_ms,
            result_digest,
            error_summary,
            transaction_id.as_bytes().as_slice(),
        ],
    )
    .map_err(StorageError::from_sqlite)?;
    drain_partition_if_empty(conn, &job.queue, &job.partition_key)
}

fn set_nonterminal(
    conn: &Connection,
    job_id: Id,
    state: JobState,
    retry_after_ms: Option<i64>,
    transaction_id: Id,
) -> Result<(), StorageError> {
    conn.execute(
        "UPDATE jobs SET state = ?2, lease_token = NULL, worker_id = NULL, \
         lease_expires_at_ms = NULL, retry_after_ms = ?3, updated_transaction_id = ?4 \
         WHERE job_id = ?1",
        rusqlite::params![
            job_id.as_bytes().as_slice(),
            state.encode(),
            retry_after_ms,
            transaction_id.as_bytes().as_slice(),
        ],
    )
    .map_err(StorageError::from_sqlite)?;
    Ok(())
}

/// Insert a newly enqueued job row (and maintain `active_partitions`) inside the
/// commit transaction.
pub(crate) fn apply_enqueue(
    tx: &Transaction<'_>,
    transaction_id: Id,
    spec: &JobSpec,
) -> Result<(), Error> {
    if load_job(tx, spec.job_id)?.is_some() {
        return Err(ValidationError(format!("job {} already exists", spec.job_id)).into());
    }
    let sequence: i64 = tx
        .query_row(
            "SELECT COALESCE(MAX(enqueue_sequence), 0) + 1 FROM jobs",
            [],
            |row| row.get(0),
        )
        .map_err(StorageError::from_sqlite)?;
    tx.execute(
        "INSERT INTO jobs (job_id, enqueue_sequence, enqueue_transaction_id, queue, \
         partition_key, payload, not_before_ms, max_attempts, effect_mode, idempotency_key, \
         state, attempt, updated_transaction_id) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, 0, ?3)",
        rusqlite::params![
            spec.job_id.as_bytes().as_slice(),
            sequence,
            transaction_id.as_bytes().as_slice(),
            spec.queue,
            spec.partition_key,
            spec.payload,
            spec.not_before_ms,
            i64::from(spec.max_attempts),
            spec.effect_mode.encode(),
            spec.idempotency_key,
            JobState::Pending.encode(),
        ],
    )
    .map_err(StorageError::from_sqlite)?;
    tx.execute(
        "INSERT OR IGNORE INTO active_partitions (queue, partition_key, first_active_sequence) \
         VALUES (?1, ?2, ?3)",
        rusqlite::params![spec.queue, spec.partition_key, sequence],
    )
    .map_err(StorageError::from_sqlite)?;
    Ok(())
}

/// Acknowledge a leased job inside the commit transaction.
pub(crate) fn apply_ack(
    tx: &Transaction<'_>,
    transaction_id: Id,
    now_ms: i64,
    ack: &JobAck,
) -> Result<(), Error> {
    let job = require_job(tx, ack.job_id)?;
    ensure_transition(&job, JobState::Succeeded)?;
    ensure_lease_token(&job, ack.lease_token)?;
    set_terminal(
        tx,
        &job,
        JobState::Succeeded,
        now_ms,
        transaction_id,
        ack.result_digest.as_deref(),
        None,
    )?;
    Ok(())
}

/// Record a job failure (retry or dead) inside the commit transaction.
pub(crate) fn apply_fail(
    tx: &Transaction<'_>,
    transaction_id: Id,
    now_ms: i64,
    failure: &crate::jobs::JobFailure,
) -> Result<(), Error> {
    let job = require_job(tx, failure.job_id)?;
    let exhausted = job.attempt >= job.max_attempts;
    let to = if exhausted {
        JobState::Dead
    } else {
        JobState::RetryWait
    };
    ensure_transition(&job, to)?;
    ensure_lease_token(&job, failure.lease_token)?;
    if exhausted {
        set_terminal(
            tx,
            &job,
            JobState::Dead,
            now_ms,
            transaction_id,
            None,
            Some(&failure.error_summary),
        )?;
    } else {
        let retry_after = match failure.retry_after_ms {
            Some(ms) => ms,
            None => now_ms.checked_add(DEFAULT_RETRY_DELAY_MS).ok_or_else(|| {
                Error::Validation(ValidationError(
                    "default retry timestamp overflows i64; supply retry_after_ms explicitly"
                        .into(),
                ))
            })?,
        };
        set_nonterminal(
            tx,
            job.job_id,
            JobState::RetryWait,
            Some(retry_after),
            transaction_id,
        )?;
        tx.execute(
            "UPDATE jobs SET error_summary = ?2 WHERE job_id = ?1",
            rusqlite::params![job.job_id.as_bytes().as_slice(), failure.error_summary],
        )
        .map_err(StorageError::from_sqlite)?;
    }
    Ok(())
}

/// Cancel a job inside the commit transaction.
pub(crate) fn apply_cancel(
    tx: &Transaction<'_>,
    transaction_id: Id,
    now_ms: i64,
    cancellation: &JobCancellation,
) -> Result<(), Error> {
    let job = require_job(tx, cancellation.job_id)?;
    ensure_transition(&job, JobState::Cancelled)?;
    // Spec A5.6: a pending job may be cancelled without a token; cancelling a
    // leased job requires its current lease token.
    match cancellation.lease_token {
        Some(token) => ensure_lease_token(&job, token)?,
        None if job.state == JobState::Pending => {}
        None => {
            return Err(ValidationError(format!(
                "cancelling leased job {} requires its lease token",
                job.job_id
            ))
            .into())
        }
    }
    set_terminal(
        tx,
        &job,
        JobState::Cancelled,
        now_ms,
        transaction_id,
        None,
        None,
    )?;
    Ok(())
}

/// Resolve an uncertain job inside the commit transaction.
pub(crate) fn apply_resolve(
    tx: &Transaction<'_>,
    transaction_id: Id,
    now_ms: i64,
    resolution: &JobResolution,
) -> Result<(), Error> {
    let job = require_job(tx, resolution.job_id)?;
    match resolution.resolution {
        Resolution::Retry => {
            // A job with no attempts left cannot re-enter Pending: dead-letter it
            // so max_attempts cannot be exceeded via Uncertain -> Retry.
            if job.attempt >= job.max_attempts {
                ensure_transition(&job, JobState::Dead)?;
                set_terminal(
                    tx,
                    &job,
                    JobState::Dead,
                    now_ms,
                    transaction_id,
                    None,
                    Some("retry resolution exhausted max_attempts"),
                )?;
            } else {
                ensure_transition(&job, JobState::Pending)?;
                set_nonterminal(tx, job.job_id, JobState::Pending, None, transaction_id)?;
            }
        }
        Resolution::MarkSucceeded => {
            ensure_transition(&job, JobState::Succeeded)?;
            set_terminal(
                tx,
                &job,
                JobState::Succeeded,
                now_ms,
                transaction_id,
                None,
                None,
            )?;
        }
        Resolution::MarkDead => {
            ensure_transition(&job, JobState::Dead)?;
            set_terminal(
                tx,
                &job,
                JobState::Dead,
                now_ms,
                transaction_id,
                None,
                Some("resolved as dead"),
            )?;
        }
    }
    Ok(())
}

/// A maintenance transition planned from pre-write state.
struct MaintenanceAction {
    job: JobRow,
    to: JobState,
}

/// A lease planned from pre-write state.
struct ProposedLease {
    job: JobRow,
    lease_token: Id,
}

fn claim_storage(e: StorageError) -> ClaimError {
    ClaimError::Storage(e)
}

fn error_to_claim(e: Error) -> ClaimError {
    match e {
        Error::Storage(s) => ClaimError::Storage(s),
        Error::Validation(v) => ClaimError::Validation(v),
        Error::Conflict(c) => ClaimError::Conflict(c),
    }
}

/// Run the full claim algorithm: expired-lease maintenance, round-robin partition
/// selection, lease grants, claim receipts, and cursor advancement — all in one
/// `BEGIN IMMEDIATE` transaction.
pub(crate) fn claim_jobs(
    conn: &mut Connection,
    request: &ClaimRequest,
) -> Result<ClaimOutcome, ClaimError> {
    if request.queue.is_empty() {
        return Err(ClaimError::Validation(ValidationError(
            "claim queue cannot be empty".into(),
        )));
    }
    if request.worker_id.is_empty() {
        return Err(ClaimError::Validation(ValidationError(
            "claim worker id cannot be empty".into(),
        )));
    }
    if request.partition_key.as_deref() == Some("") {
        return Err(ClaimError::Validation(ValidationError(
            "claim partition_key cannot be empty".into(),
        )));
    }
    if request.lease_ms <= 0 {
        return Err(ClaimError::Validation(ValidationError(
            "claim lease_ms must be positive".into(),
        )));
    }
    let lease_expires_at_ms = request
        .now_ms
        .checked_add(request.lease_ms)
        .ok_or_else(|| {
            ClaimError::Validation(ValidationError(
                "lease expiry timestamp overflows i64".into(),
            ))
        })?;

    let tx = conn
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|e| claim_storage(StorageError::from_sqlite(e)))?;

    // Phase 1: plan expired-lease maintenance from current state.
    let maintenance = plan_maintenance(&tx, request).map_err(claim_storage)?;

    // Phase 2: plan leases round-robin over active partitions, using the
    // pre-maintenance state so a lease repaired this call is never re-leased here.
    let (leases, last_leased_partition) = plan_leases(&tx, request).map_err(error_to_claim)?;

    if maintenance.is_empty() && leases.is_empty() {
        // Nothing to do; roll back so no durable transaction is created.
        return Ok(ClaimOutcome::Noop);
    }

    let transaction_id = Id::new().map_err(error_to_claim)?;
    insert_claim_transaction(
        &tx,
        transaction_id,
        request,
        maintenance.len() + leases.len(),
    )
    .map_err(claim_storage)?;

    for action in &maintenance {
        ensure_transition(&action.job, action.to).map_err(error_to_claim)?;
        match action.to {
            JobState::Dead => set_terminal(
                &tx,
                &action.job,
                JobState::Dead,
                request.now_ms,
                transaction_id,
                None,
                Some("lease expired at final attempt"),
            )
            .map_err(claim_storage)?,
            to => set_nonterminal(&tx, action.job.job_id, to, None, transaction_id)
                .map_err(claim_storage)?,
        }
    }

    let mut claimed = Vec::with_capacity(leases.len());
    for lease in &leases {
        let attempt = lease.job.attempt + 1;
        tx.execute(
            "UPDATE jobs SET state = ?2, attempt = ?3, lease_token = ?4, worker_id = ?5, \
             lease_expires_at_ms = ?6, retry_after_ms = NULL, updated_transaction_id = ?7 \
             WHERE job_id = ?1",
            rusqlite::params![
                lease.job.job_id.as_bytes().as_slice(),
                JobState::Leased.encode(),
                i64::from(attempt),
                lease.lease_token.as_bytes().as_slice(),
                request.worker_id,
                lease_expires_at_ms,
                transaction_id.as_bytes().as_slice(),
            ],
        )
        .map_err(|e| claim_storage(StorageError::from_sqlite(e)))?;
        tx.execute(
            "INSERT INTO claim_receipts (transaction_id, job_id, lease_token, attempt, \
             worker_id, lease_expires_at_ms) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            rusqlite::params![
                transaction_id.as_bytes().as_slice(),
                lease.job.job_id.as_bytes().as_slice(),
                lease.lease_token.as_bytes().as_slice(),
                i64::from(attempt),
                request.worker_id,
                lease_expires_at_ms,
            ],
        )
        .map_err(|e| claim_storage(StorageError::from_sqlite(e)))?;
        claimed.push(ClaimedJob {
            job_id: lease.job.job_id,
            queue: lease.job.queue.clone(),
            partition_key: lease.job.partition_key.clone(),
            payload: lease.job.payload.clone(),
            worker_id: request.worker_id.clone(),
            lease_token: lease.lease_token,
            attempt,
            lease_expires_at_ms,
            idempotency_key: lease.job.idempotency_key.clone(),
        });
    }

    // The cursor advances only when a lease was granted.
    if let Some(partition) = last_leased_partition {
        tx.execute(
            "INSERT INTO queue_cursors (queue, last_partition_key) VALUES (?1, ?2) \
             ON CONFLICT(queue) DO UPDATE SET last_partition_key = excluded.last_partition_key",
            rusqlite::params![request.queue, partition],
        )
        .map_err(|e| claim_storage(StorageError::from_sqlite(e)))?;
    }

    let proposed_jobs: Vec<Id> = claimed.iter().map(|job| job.job_id).collect();
    if let Err(e) = tx.commit() {
        return Err(ClaimError::Indeterminate(IndeterminateClaim {
            transaction_id,
            proposed_jobs,
            storage_error: StorageError::from_sqlite(e).to_string(),
        }));
    }

    #[cfg(feature = "failpoints")]
    if crate::store::failpoints::take_fail_commit() {
        return Err(ClaimError::Indeterminate(IndeterminateClaim {
            transaction_id,
            proposed_jobs,
            storage_error: "failpoint: COMMIT outcome unknown".into(),
        }));
    }

    if claimed.is_empty() {
        Ok(ClaimOutcome::MaintenanceCommitted(MaintenanceReceipt {
            transaction_id,
        }))
    } else {
        Ok(ClaimOutcome::Committed(CommittedClaims {
            transaction_id,
            jobs: claimed,
            stale_jobs: Vec::new(),
        }))
    }
}

fn plan_maintenance(
    tx: &Transaction<'_>,
    request: &ClaimRequest,
) -> Result<Vec<MaintenanceAction>, StorageError> {
    let mut stmt = tx
        .prepare(&format!(
            "SELECT {JOB_COLUMNS} FROM jobs WHERE queue = ?1 AND state = ?2 \
         AND lease_expires_at_ms < ?3 ORDER BY lease_expires_at_ms LIMIT ?4"
        ))
        .map_err(StorageError::from_sqlite)?;
    let rows = stmt
        .query_map(
            rusqlite::params![
                request.queue,
                JobState::Leased.encode(),
                request.now_ms,
                MAINTENANCE_LIMIT as i64,
            ],
            row_to_job,
        )
        .map_err(StorageError::from_sqlite)?;
    let mut actions = Vec::new();
    for row in rows {
        let job = row.map_err(StorageError::from_sqlite)??;
        let to = match job.effect_mode {
            EffectMode::RequiresReconciliation => JobState::Uncertain,
            EffectMode::ExplicitlyIdempotent => {
                if job.attempt >= job.max_attempts {
                    JobState::Dead
                } else {
                    JobState::Pending
                }
            }
        };
        actions.push(MaintenanceAction { job, to });
    }
    Ok(actions)
}

fn plan_leases(
    tx: &Transaction<'_>,
    request: &ClaimRequest,
) -> Result<(Vec<ProposedLease>, Option<String>), Error> {
    if request.limit == 0 {
        return Ok((Vec::new(), None));
    }
    if let Some(partition) = &request.partition_key {
        let mut leases = Vec::new();
        if let Some(job) = ready_head(tx, request, partition)? {
            leases.push(ProposedLease {
                job,
                lease_token: Id::new()?,
            });
        }
        // Targeted claims never advance the round-robin cursor.
        return Ok((leases, None));
    }
    let cursor: Option<String> = tx
        .query_row(
            "SELECT last_partition_key FROM queue_cursors WHERE queue = ?1",
            [&request.queue],
            |row| row.get(0),
        )
        .optional()
        .map_err(StorageError::from_sqlite)?
        .flatten();
    let mut stmt = tx
        .prepare(
            "SELECT partition_key FROM active_partitions WHERE queue = ?1 ORDER BY partition_key",
        )
        .map_err(StorageError::from_sqlite)?;
    let partitions = stmt
        .query_map([&request.queue], |row| row.get::<_, String>(0))
        .map_err(StorageError::from_sqlite)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(StorageError::from_sqlite)?;
    let start = match &cursor {
        Some(cursor) => partitions.partition_point(|p| p.as_str() <= cursor.as_str()),
        None => 0,
    };

    let mut leases = Vec::new();
    let mut last_leased = None;
    for partition in partitions[start..].iter().chain(partitions[..start].iter()) {
        if leases.len() >= request.limit {
            break;
        }
        let Some(job) = ready_head(tx, request, partition)? else {
            continue;
        };
        last_leased = Some(partition.clone());
        leases.push(ProposedLease {
            job,
            lease_token: Id::new()?,
        });
    }
    Ok((leases, last_leased))
}

/// The partition's head job, if it exists and is ready to lease now.
fn ready_head(
    tx: &Transaction<'_>,
    request: &ClaimRequest,
    partition: &str,
) -> Result<Option<JobRow>, Error> {
    let head = tx
        .query_row(
            &format!(
                "SELECT {JOB_COLUMNS} FROM jobs WHERE queue = ?1 AND partition_key = ?2 \
                 AND state IN ({}) ORDER BY enqueue_sequence LIMIT 1",
                ACTIVE_STATES_SQL.as_str(),
            ),
            rusqlite::params![request.queue, partition],
            row_to_job,
        )
        .optional()
        .map_err(StorageError::from_sqlite)?
        .transpose()?;
    let Some(job) = head else { return Ok(None) };
    let ready = match job.state {
        JobState::Pending => job.not_before_ms <= request.now_ms,
        JobState::RetryWait => {
            job.not_before_ms <= request.now_ms
                && job.retry_after_ms.unwrap_or(i64::MIN) <= request.now_ms
        }
        _ => false,
    };
    Ok(if ready { Some(job) } else { None })
}

fn insert_claim_transaction(
    tx: &Transaction<'_>,
    transaction_id: Id,
    request: &ClaimRequest,
    operation_count: usize,
) -> Result<(), StorageError> {
    let sequence: i64 = tx
        .query_row(
            "SELECT COALESCE(MAX(transaction_sequence), 0) + 1 FROM transactions",
            [],
            |row| row.get(0),
        )
        .map_err(StorageError::from_sqlite)?;
    // Claim transactions are never resubmitted by ID, so the digest only needs to be
    // deterministic per request; reuse the migration FNV-1a-128 checksum.
    let digest = crate::store::migrations::checksum(&format!(
        "claim:{}:{}:{}:{}:{}:{}",
        transaction_id,
        request.queue,
        request.worker_id,
        request.now_ms,
        request.lease_ms,
        request.limit,
    ));
    tx.execute(
        "INSERT INTO transactions (transaction_id, transaction_sequence, committed_at_ms, \
         correlation_id, metadata, request_digest, operation_count) \
         VALUES (?1, ?2, ?3, NULL, X'', ?4, ?5)",
        rusqlite::params![
            transaction_id.as_bytes().as_slice(),
            sequence,
            request.now_ms,
            digest.as_slice(),
            operation_count as i64,
        ],
    )
    .map_err(StorageError::from_sqlite)?;
    Ok(())
}

/// Durably extend an active lease.
///
/// Pre-commit failures are typed conflicts or storage errors and persist nothing;
/// a failed COMMIT step is [`LeaseError::Indeterminate`] because the extension may
/// or may not be durable. Callers recover by reading the job's lease state via
/// `store.job(job_id)`.
pub(crate) fn extend_lease(
    conn: &mut Connection,
    job_id: Id,
    lease_token: Id,
    new_expiry_ms: i64,
    now_ms: i64,
) -> Result<LeaseExtensionReceipt, LeaseError> {
    let tx = conn
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(StorageError::from_sqlite)?;
    let receipt = apply_extend_lease(
        &tx,
        &LeaseExtension {
            job_id,
            lease_token,
            new_expiry_ms,
        },
        now_ms,
    )?;
    // Only the COMMIT step is indeterminate: it may or may not have persisted.
    if let Err(e) = tx.commit() {
        return Err(LeaseError::Indeterminate(IndeterminateLease {
            job_id,
            storage_error: StorageError::from_sqlite(e).to_string(),
        }));
    }

    #[cfg(feature = "failpoints")]
    if crate::store::failpoints::take_fail_commit() {
        return Err(LeaseError::Indeterminate(IndeterminateLease {
            job_id,
            storage_error: "failpoint: COMMIT outcome unknown".into(),
        }));
    }

    Ok(receipt)
}

/// Apply a lease extension inside an open transaction (also used by
/// `Operation::ExtendLease` in the commit pipeline; plan §6.1).
pub(crate) fn apply_extend_lease(
    tx: &Transaction<'_>,
    extension: &LeaseExtension,
    now_ms: i64,
) -> Result<LeaseExtensionReceipt, LeaseError> {
    let LeaseExtension {
        job_id,
        lease_token,
        new_expiry_ms,
    } = *extension;
    let job = load_job(tx, job_id)?.ok_or(LeaseConflict::JobNotFound(job_id))?;
    if job.state != JobState::Leased {
        return Err(LeaseConflict::NotLeased {
            job_id,
            state: job.state,
        }
        .into());
    }
    if job.lease_token != Some(lease_token) {
        return Err(LeaseConflict::InvalidToken { job_id }.into());
    }
    let current_ms = job.lease_expires_at_ms.unwrap_or(i64::MIN);
    if now_ms > current_ms {
        return Err(LeaseConflict::Expired {
            job_id,
            lease_expires_at_ms: current_ms,
            now_ms,
        }
        .into());
    }
    if new_expiry_ms <= current_ms {
        return Err(LeaseConflict::ExpiryNotLater {
            job_id,
            current_ms,
            requested_ms: new_expiry_ms,
        }
        .into());
    }
    tx.execute(
        "UPDATE jobs SET lease_expires_at_ms = ?2 WHERE job_id = ?1",
        rusqlite::params![job_id.as_bytes().as_slice(), new_expiry_ms],
    )
    .map_err(StorageError::from_sqlite)?;
    // Keep the claim receipt truthful so `recover_claim` reconstructs the
    // extended expiry, not the one granted at claim time.
    tx.execute(
        "UPDATE claim_receipts SET lease_expires_at_ms = ?3 WHERE job_id = ?1 AND lease_token = ?2",
        rusqlite::params![
            job_id.as_bytes().as_slice(),
            lease_token.as_bytes().as_slice(),
            new_expiry_ms,
        ],
    )
    .map_err(StorageError::from_sqlite)?;
    Ok(LeaseExtensionReceipt {
        job_id,
        attempt: job.attempt,
        lease_expires_at_ms: new_expiry_ms,
    })
}

/// Recover an indeterminate claim, reconstructing original lease tokens from
/// `claim_receipts`.
pub(crate) fn recover_claim(
    conn: &Connection,
    transaction_id: Id,
    now_ms: i64,
) -> Result<ClaimRecovery, RecoveryError> {
    let mut stmt = conn
        .prepare(
            "SELECT r.job_id, r.lease_token, r.attempt, r.worker_id, r.lease_expires_at_ms, \
             j.queue, j.partition_key, j.payload, j.idempotency_key, \
             j.state, j.lease_token, j.lease_expires_at_ms \
             FROM claim_receipts r JOIN jobs j ON j.job_id = r.job_id \
             WHERE r.transaction_id = ?1 ORDER BY r.job_id",
        )
        .map_err(StorageError::from_sqlite)?;
    let rows = stmt
        .query_map([transaction_id.as_bytes().as_slice()], |row| {
            let job_id: Vec<u8> = row.get(0)?;
            let lease_token: Vec<u8> = row.get(1)?;
            let attempt: i64 = row.get(2)?;
            let worker_id: String = row.get(3)?;
            let lease_expires_at_ms: i64 = row.get(4)?;
            let queue: String = row.get(5)?;
            let partition_key: String = row.get(6)?;
            let payload: Vec<u8> = row.get(7)?;
            let idempotency_key: Option<String> = row.get(8)?;
            let current_state: i64 = row.get(9)?;
            let current_token: Option<Vec<u8>> = row.get(10)?;
            let current_expiry: Option<i64> = row.get(11)?;
            Ok((
                (
                    job_id,
                    lease_token,
                    attempt,
                    worker_id,
                    lease_expires_at_ms,
                    queue,
                    partition_key,
                    payload,
                    idempotency_key,
                ),
                (current_state, current_token, current_expiry),
            ))
        })
        .map_err(StorageError::from_sqlite)?;
    let mut jobs = Vec::new();
    let mut stale_jobs = Vec::new();
    let mut any_receipt = false;
    for row in rows {
        let (
            (
                job_id,
                lease_token,
                attempt,
                worker_id,
                lease_expires_at_ms,
                queue,
                partition_key,
                payload,
                idempotency_key,
            ),
            (current_state, current_token, current_expiry),
        ) = row.map_err(StorageError::from_sqlite)?;
        any_receipt = true;
        let job_id = id_from_blob(job_id)?;
        let lease_token = id_from_blob(lease_token)?;
        // A recovered lease is executable only while it is still the job's
        // current, unexpired lease; anything else risks double execution.
        let still_current = current_state == JobState::Leased.encode()
            && opt_id_from_blob(current_token)? == Some(lease_token)
            && current_expiry.is_some_and(|expiry| expiry >= now_ms);
        if !still_current {
            stale_jobs.push(job_id);
            continue;
        }
        jobs.push(ClaimedJob {
            job_id,
            queue,
            partition_key,
            payload,
            worker_id,
            lease_token,
            attempt: attempt as u32,
            lease_expires_at_ms,
            idempotency_key,
        });
    }
    if any_receipt {
        return Ok(ClaimRecovery::Committed(CommittedClaims {
            transaction_id,
            jobs,
            stale_jobs,
        }));
    }
    // No receipts: the claim either never committed, or committed maintenance only.
    let committed: bool = conn
        .query_row(
            "SELECT 1 FROM transactions WHERE transaction_id = ?1",
            [transaction_id.as_bytes().as_slice()],
            |_| Ok(true),
        )
        .optional()
        .map_err(StorageError::from_sqlite)?
        .unwrap_or(false);
    if committed {
        // The transaction is durable but leased nothing: maintenance only. Never
        // report it as an empty grant.
        Ok(ClaimRecovery::MaintenanceCommitted(MaintenanceReceipt {
            transaction_id,
        }))
    } else {
        // SQLite commits are atomic; a missing row means the claim rolled back.
        Ok(ClaimRecovery::Absent)
    }
}

/// Look up one job by its ID.
pub(crate) fn get_job(conn: &Connection, job_id: Id) -> Result<Option<JobInfo>, Error> {
    Ok(load_job(conn, job_id)?.map(JobRow::into_info))
}

/// List jobs, optionally filtered by queue and state, in enqueue order.
pub(crate) fn list_jobs(
    conn: &Connection,
    queue: Option<&str>,
    state: Option<JobState>,
    limit: usize,
) -> Result<Vec<JobInfo>, Error> {
    let mut stmt = conn
        .prepare(&format!(
            "SELECT {JOB_COLUMNS} FROM jobs WHERE (?1 IS NULL OR queue = ?1) \
             AND (?2 IS NULL OR state = ?2) ORDER BY enqueue_sequence LIMIT ?3"
        ))
        .map_err(StorageError::from_sqlite)?;
    let rows = stmt
        .query_map(
            rusqlite::params![queue, state.map(JobState::encode), limit as i64],
            row_to_job,
        )
        .map_err(StorageError::from_sqlite)?;
    let mut jobs = Vec::new();
    for row in rows {
        let job = row.map_err(StorageError::from_sqlite)??;
        jobs.push(job.into_info());
    }
    Ok(jobs)
}

/// List one page of jobs after a pagination cursor (`enqueue_sequence`),
/// returning the page and the cursor for the next page.
pub(crate) fn list_jobs_page(
    conn: &Connection,
    queue: Option<&str>,
    state: Option<JobState>,
    after_sequence: u64,
    limit: usize,
) -> Result<(Vec<JobInfo>, u64), Error> {
    let after_sequence_i64 = i64::try_from(after_sequence).map_err(|_| {
        crate::error::ValidationError(format!(
            "jobs_page after_sequence {after_sequence} exceeds the supported range"
        ))
    })?;
    let limit_i64 = i64::try_from(limit).map_err(|_| {
        crate::error::ValidationError(format!(
            "jobs_page limit {limit} exceeds the supported range"
        ))
    })?;
    let mut stmt = conn
        .prepare(&format!(
            "SELECT {JOB_COLUMNS} FROM jobs WHERE enqueue_sequence > ?1 \
             AND (?2 IS NULL OR queue = ?2) AND (?3 IS NULL OR state = ?3) \
             ORDER BY enqueue_sequence LIMIT ?4"
        ))
        .map_err(StorageError::from_sqlite)?;
    let rows = stmt
        .query_map(
            rusqlite::params![
                after_sequence_i64,
                queue,
                state.map(JobState::encode),
                limit_i64
            ],
            |row| {
                let sequence: i64 = row.get(1)?;
                Ok((sequence, row_to_job(row)?))
            },
        )
        .map_err(StorageError::from_sqlite)?;
    let mut jobs = Vec::new();
    let mut cursor = after_sequence;
    for row in rows {
        let (sequence, job) = row.map_err(StorageError::from_sqlite)?;
        cursor = sequence as u64;
        jobs.push(job?.into_info());
    }
    Ok((jobs, cursor))
}
