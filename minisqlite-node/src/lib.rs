//! napi-rs bindings for the minisqlite control-plane kernel (spike).
//!
//! Binds the public Rust API directly: IDs are 32-char lower-case hex strings,
//! byte payloads are Buffers, and error enums surface as JS `Error`s whose
//! `code` property names the Rust error variant. Indeterminate outcomes embed
//! `transactionId=<hex>` in the message so callers can recover.

#[macro_use]
extern crate napi_derive;

use minisqlite as ms;
use napi::bindgen_prelude::*;

/// Custom error status so thrown JS errors carry a `code` naming the Rust
/// error variant (for example `"Conflict"` or `"CommitIndeterminate"`).
pub struct ErrorCode(String);

impl AsRef<str> for ErrorCode {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl From<Status> for ErrorCode {
    fn from(status: Status) -> Self {
        ErrorCode(status.as_ref().to_string())
    }
}

type JsError = Error<ErrorCode>;

fn err(code: &str, message: impl std::fmt::Display) -> JsError {
    Error::new(ErrorCode(code.to_string()), message.to_string())
}

fn map_error(e: ms::Error) -> JsError {
    match e {
        ms::Error::Storage(s) => err("Storage", s),
        ms::Error::Validation(v) => err("Validation", v),
        ms::Error::Conflict(c) => err("Conflict", c),
        other => err("Unknown", other),
    }
}

fn map_commit_error(e: ms::CommitError) -> JsError {
    match e {
        ms::CommitError::Conflict(c) => err("Conflict", c),
        ms::CommitError::Validation(v) => err("Validation", v),
        ms::CommitError::DuplicateIdWithDifferentContent => err(
            "DuplicateIdWithDifferentContent",
            "transaction id reused with different content",
        ),
        ms::CommitError::Indeterminate(i) => err(
            "CommitIndeterminate",
            format!(
                "commit outcome indeterminate; recoverTransaction to verify; transactionId={} ({})",
                i.transaction_id(),
                i.storage_error()
            ),
        ),
        ms::CommitError::Storage(s) => err("Storage", s),
        other => err("Unknown", other),
    }
}

fn map_claim_error(e: ms::ClaimError) -> JsError {
    match e {
        ms::ClaimError::Indeterminate(i) => err(
            "ClaimIndeterminate",
            format!(
                "claim outcome indeterminate; recoverClaim to verify; transactionId={} ({})",
                i.transaction_id(),
                i.storage_error()
            ),
        ),
        ms::ClaimError::Validation(v) => err("Validation", v),
        ms::ClaimError::Conflict(c) => err("Conflict", c),
        ms::ClaimError::Storage(s) => err("Storage", s),
        other => err("Unknown", other),
    }
}

fn map_lease_error(e: ms::LeaseError) -> JsError {
    match e {
        ms::LeaseError::Conflict(c) => err("LeaseConflict", c),
        ms::LeaseError::Indeterminate(i) => err(
            "LeaseIndeterminate",
            format!(
                "lease extension outcome indeterminate; read the job to verify; jobId={} ({})",
                i.job_id(),
                i.storage_error()
            ),
        ),
        ms::LeaseError::Storage(s) => err("Storage", s),
        other => err("Unknown", other),
    }
}

fn map_recovery_error(e: ms::RecoveryError) -> JsError {
    match e {
        ms::RecoveryError::Storage(s) => err("Storage", s),
        other => err("Unknown", other),
    }
}

fn parse_id(field: &str, hex: &str) -> Result<ms::Id, ErrorCode> {
    ms::Id::from_hex(hex).map_err(|_| err("InvalidId", format!("{field}: invalid id {hex:?}")))
}

/// Generate a new random 128-bit ID as a 32-char lower-case hex string.
#[napi]
pub fn new_id() -> Result<String, ErrorCode> {
    Ok(ms::Id::new().map_err(map_error)?.to_hex())
}

#[napi(object)]
pub struct ExpectedStreamVersionInput {
    pub stream_id: String,
    pub version: i64,
}

#[napi(object)]
pub struct EventInput {
    /// 32-char hex; generated when omitted.
    pub event_id: Option<String>,
    pub stream_id: String,
    pub event_type: String,
    pub occurred_at_ms: i64,
    pub payload: Option<Buffer>,
}

#[napi(object)]
pub struct ProjectionPutInput {
    pub key: Buffer,
    pub value: Buffer,
}

#[napi(object)]
pub struct ProjectionPatchInput {
    pub projection: String,
    pub expected_version: i64,
    pub puts: Option<Vec<ProjectionPutInput>>,
    pub deletes: Option<Vec<Buffer>>,
}

#[napi(object)]
pub struct JobSpecInput {
    /// 32-char hex; generated when omitted.
    pub job_id: Option<String>,
    pub queue: String,
    pub partition_key: String,
    pub payload: Buffer,
    /// "reconcilable" (default) or "idempotent" (requires `idempotencyKey`).
    pub effect_mode: Option<String>,
    pub idempotency_key: Option<String>,
    pub not_before_ms: Option<i64>,
    pub max_attempts: Option<u32>,
}

#[napi(object)]
pub struct JobAckInput {
    pub job_id: String,
    pub lease_token: String,
    pub result_digest: Option<Buffer>,
}

#[napi(object)]
pub struct JobFailureInput {
    pub job_id: String,
    pub lease_token: String,
    pub error_summary: Option<String>,
    /// Delay before the job becomes claimable again; store default when omitted.
    pub retry_after_ms: Option<i64>,
}

#[napi(object)]
pub struct JobCancellationInput {
    pub job_id: String,
    /// Required to cancel a leased job; omit for unleased jobs.
    pub lease_token: Option<String>,
}

#[napi(object)]
pub struct JobResolutionInput {
    pub job_id: String,
    /// "retry", "markSucceeded", or "markDead".
    pub resolution: String,
}

fn parse_resolution(resolution: &str) -> Result<ms::Resolution, ErrorCode> {
    Ok(match resolution {
        "retry" => ms::Resolution::Retry,
        "markSucceeded" => ms::Resolution::MarkSucceeded,
        "markDead" => ms::Resolution::MarkDead,
        other => return Err(err("Validation", format!("unknown resolution {other:?}"))),
    })
}

#[napi(object)]
pub struct CommitBatchInput {
    /// 32-char hex; generated when omitted. Supply one to be able to recover
    /// an indeterminate commit with `recoverTransaction`.
    pub transaction_id: Option<String>,
    pub committed_at_ms: i64,
    pub expected_stream_versions: Option<Vec<ExpectedStreamVersionInput>>,
    pub events: Option<Vec<EventInput>>,
    pub projection_patches: Option<Vec<ProjectionPatchInput>>,
    pub enqueue_jobs: Option<Vec<JobSpecInput>>,
    pub ack_jobs: Option<Vec<JobAckInput>>,
    pub fail_jobs: Option<Vec<JobFailureInput>>,
    pub cancel_jobs: Option<Vec<JobCancellationInput>>,
    pub resolve_uncertain_jobs: Option<Vec<JobResolutionInput>>,
}

#[napi(object)]
pub struct CommitReceiptOutput {
    pub transaction_id: String,
    pub transaction_sequence: i64,
    pub committed_at_ms: i64,
}

fn receipt_output(receipt: ms::CommitReceipt) -> CommitReceiptOutput {
    CommitReceiptOutput {
        transaction_id: receipt.transaction_id().to_hex(),
        transaction_sequence: receipt.transaction_sequence() as i64,
        committed_at_ms: receipt.committed_at_ms(),
    }
}

#[napi(object)]
pub struct ClaimRequestInput {
    pub queue: String,
    pub worker_id: String,
    pub now_ms: i64,
    pub lease_ms: i64,
    pub limit: u32,
}

#[napi(object)]
pub struct ClaimedJobOutput {
    pub job_id: String,
    pub queue: String,
    pub partition_key: String,
    pub payload: Buffer,
    pub worker_id: String,
    pub lease_token: String,
    pub attempt: u32,
    pub lease_expires_at_ms: i64,
    pub idempotency_key: Option<String>,
}

fn claimed_job_output(job: ms::ClaimedJob) -> ClaimedJobOutput {
    ClaimedJobOutput {
        job_id: job.job_id.to_hex(),
        queue: job.queue,
        partition_key: job.partition_key,
        payload: job.payload.into(),
        worker_id: job.worker_id,
        lease_token: job.lease_token.to_hex(),
        attempt: job.attempt,
        lease_expires_at_ms: job.lease_expires_at_ms,
        idempotency_key: job.idempotency_key,
    }
}

/// Outcome of `claimJobs` or `recoverClaim`.
///
/// `kind` is "noop", "maintenanceCommitted", "committed", or (recovery only)
/// "absent". `staleJobs` lists job IDs that committed under the claim but must
/// not be executed with the recovered tokens.
#[napi(object)]
pub struct ClaimOutcomeOutput {
    pub kind: String,
    pub transaction_id: Option<String>,
    pub jobs: Vec<ClaimedJobOutput>,
    pub stale_jobs: Vec<String>,
}

impl ClaimOutcomeOutput {
    fn bare(kind: &str) -> Self {
        Self {
            kind: kind.to_string(),
            transaction_id: None,
            jobs: Vec::new(),
            stale_jobs: Vec::new(),
        }
    }

    fn maintenance(receipt: ms::MaintenanceReceipt) -> Self {
        Self {
            transaction_id: Some(receipt.transaction_id().to_hex()),
            ..Self::bare("maintenanceCommitted")
        }
    }

    fn committed(claims: ms::CommittedClaims) -> Self {
        Self {
            kind: "committed".to_string(),
            transaction_id: Some(claims.transaction_id().to_hex()),
            stale_jobs: claims.stale_jobs().iter().map(ms::Id::to_hex).collect(),
            jobs: claims
                .into_jobs()
                .into_iter()
                .map(claimed_job_output)
                .collect(),
        }
    }
}

#[napi(object)]
pub struct LeaseExtensionReceiptOutput {
    pub job_id: String,
    pub attempt: u32,
    pub lease_expires_at_ms: i64,
}

/// Outcome of `recoverTransaction`: `kind` is "committed" (with the original
/// receipt fields) or "absent" (safe to resubmit the same batch).
#[napi(object)]
pub struct TransactionRecoveryOutput {
    pub kind: String,
    pub receipt: Option<CommitReceiptOutput>,
}

#[napi(object)]
pub struct JobInfoOutput {
    pub job_id: String,
    pub queue: String,
    pub partition_key: String,
    pub state: String,
    pub attempt: u32,
    pub lease_expires_at_ms: Option<i64>,
    pub worker_id: Option<String>,
    pub error_summary: Option<String>,
}

fn job_state_name(state: ms::JobState) -> &'static str {
    match state {
        ms::JobState::Pending => "pending",
        ms::JobState::Leased => "leased",
        ms::JobState::RetryWait => "retryWait",
        ms::JobState::Uncertain => "uncertain",
        ms::JobState::Succeeded => "succeeded",
        ms::JobState::Dead => "dead",
        ms::JobState::Cancelled => "cancelled",
    }
}

fn parse_job_state(state: &str) -> Result<ms::JobState, ErrorCode> {
    Ok(match state {
        "pending" => ms::JobState::Pending,
        "leased" => ms::JobState::Leased,
        "retryWait" => ms::JobState::RetryWait,
        "uncertain" => ms::JobState::Uncertain,
        "succeeded" => ms::JobState::Succeeded,
        "dead" => ms::JobState::Dead,
        "cancelled" => ms::JobState::Cancelled,
        other => return Err(err("Validation", format!("unknown job state {other:?}"))),
    })
}

fn job_info_output(info: ms::JobInfo) -> JobInfoOutput {
    JobInfoOutput {
        job_id: info.job_id.to_hex(),
        queue: info.spec.queue().to_string(),
        partition_key: info.spec.partition_key().to_string(),
        state: job_state_name(info.state).to_string(),
        attempt: info.attempt,
        lease_expires_at_ms: info.lease_expires_at_ms,
        worker_id: info.worker_id,
        error_summary: info.error_summary,
    }
}

/// One page from `jobsPage`; pass `nextAfterSequence` back to fetch the next page.
#[napi(object)]
pub struct JobsPageOutput {
    pub jobs: Vec<JobInfoOutput>,
    pub next_after_sequence: i64,
}

#[napi(object)]
pub struct PersistedEventOutput {
    pub transaction_id: String,
    pub global_sequence: i64,
    pub stream_version: i64,
    pub event_id: String,
    pub stream_id: String,
    pub event_type: String,
    pub occurred_at_ms: i64,
    pub payload: Buffer,
}

fn persisted_event_output(persisted: ms::PersistedEvent) -> PersistedEventOutput {
    PersistedEventOutput {
        transaction_id: persisted.transaction_id.to_hex(),
        global_sequence: persisted.global_sequence as i64,
        stream_version: persisted.stream_version as i64,
        event_id: persisted.event.event_id.to_hex(),
        stream_id: persisted.event.stream_id,
        event_type: persisted.event.event_type,
        occurred_at_ms: persisted.event.occurred_at_ms,
        payload: persisted.event.payload.into(),
    }
}

fn build_batch(input: CommitBatchInput) -> Result<ms::CommitBatch, ErrorCode> {
    let transaction_id = match &input.transaction_id {
        Some(hex) => parse_id("transactionId", hex)?,
        None => ms::Id::new().map_err(map_error)?,
    };
    let mut batch = ms::CommitBatch::new(transaction_id, input.committed_at_ms);
    for expected in input.expected_stream_versions.unwrap_or_default() {
        batch = batch.expect_stream_version(expected.stream_id, expected.version as u64);
    }
    for event in input.events.unwrap_or_default() {
        let event_id = match &event.event_id {
            Some(hex) => parse_id("eventId", hex)?,
            None => ms::Id::new().map_err(map_error)?,
        };
        batch = batch.append_event(ms::Event::with_json_payload(
            event_id,
            event.stream_id,
            event.event_type,
            event.occurred_at_ms,
            event.payload.as_deref().unwrap_or_default(),
        ));
    }
    for patch in input.projection_patches.unwrap_or_default() {
        let mut p = ms::ProjectionPatch::new(patch.projection, patch.expected_version as u64);
        for put in patch.puts.unwrap_or_default() {
            p = p.put(put.key.to_vec(), put.value.to_vec());
        }
        for key in patch.deletes.unwrap_or_default() {
            p = p.delete(key.to_vec());
        }
        batch = batch.apply_projection_patch(p);
    }
    for job in input.enqueue_jobs.unwrap_or_default() {
        let job_id = match &job.job_id {
            Some(hex) => parse_id("jobId", hex)?,
            None => ms::Id::new().map_err(map_error)?,
        };
        let payload = job.payload.to_vec();
        let mut spec = match job.effect_mode.as_deref() {
            None | Some("reconcilable") => {
                ms::JobSpec::reconcilable(job_id, job.queue, job.partition_key, payload)
            }
            Some("idempotent") => match job.idempotency_key {
                Some(key) => {
                    ms::JobSpec::idempotent(job_id, job.queue, job.partition_key, payload, key)
                }
                None => ms::JobSpec::intrinsically_idempotent(
                    job_id,
                    job.queue,
                    job.partition_key,
                    payload,
                ),
            },
            Some(other) => return Err(err("Validation", format!("unknown effect mode {other:?}"))),
        };
        if let Some(not_before_ms) = job.not_before_ms {
            spec = spec.with_not_before_ms(not_before_ms);
        }
        if let Some(max_attempts) = job.max_attempts {
            spec = spec.with_max_attempts(max_attempts);
        }
        batch = batch.enqueue_job(spec);
    }
    for ack in input.ack_jobs.unwrap_or_default() {
        batch = batch.acknowledge_job(
            parse_id("jobId", &ack.job_id)?,
            parse_id("leaseToken", &ack.lease_token)?,
            ack.result_digest.map(|d| d.to_vec()),
        );
    }
    for failure in input.fail_jobs.unwrap_or_default() {
        batch = batch.fail_job(
            parse_id("jobId", &failure.job_id)?,
            parse_id("leaseToken", &failure.lease_token)?,
            failure.error_summary.unwrap_or_default(),
            failure.retry_after_ms,
        );
    }
    for cancellation in input.cancel_jobs.unwrap_or_default() {
        let lease_token = cancellation
            .lease_token
            .as_deref()
            .map(|hex| parse_id("leaseToken", hex))
            .transpose()?;
        batch = batch.cancel_job(parse_id("jobId", &cancellation.job_id)?, lease_token);
    }
    for resolution in input.resolve_uncertain_jobs.unwrap_or_default() {
        batch = batch.resolve_uncertain_job(
            parse_id("jobId", &resolution.job_id)?,
            parse_resolution(&resolution.resolution)?,
        );
    }
    Ok(batch)
}

/// A handle to one control-plane store (`ControlPlaneStore`).
#[napi]
pub struct Store {
    inner: Option<ms::ControlPlaneStore>,
}

#[napi]
impl Store {
    /// Open (or create) a store at `path`. `durability` is "strict" (default)
    /// or "relaxed".
    #[napi(factory)]
    pub fn open(path: String, durability: Option<String>) -> Result<Store, ErrorCode> {
        let mut builder = ms::ControlPlaneStore::builder(path);
        builder = match durability.as_deref() {
            None | Some("strict") => builder.durability(ms::Durability::Strict),
            Some("relaxed") => builder.durability(ms::Durability::Relaxed),
            Some(other) => return Err(err("Validation", format!("unknown durability {other:?}"))),
        };
        Ok(Store {
            inner: Some(builder.open().map_err(map_error)?),
        })
    }

    /// Drop the underlying store and its connections. Further calls throw.
    #[napi]
    pub fn close(&mut self) {
        self.inner = None;
    }

    fn store(&self) -> Result<&ms::ControlPlaneStore, ErrorCode> {
        self.inner
            .as_ref()
            .ok_or_else(|| err("Closed", "store is closed"))
    }

    /// Atomically commit events, projection patches, and job operations.
    #[napi]
    pub fn commit(&self, batch: CommitBatchInput) -> Result<CommitReceiptOutput, ErrorCode> {
        let batch = build_batch(batch)?;
        let receipt = self.store()?.commit(&batch).map_err(map_commit_error)?;
        Ok(receipt_output(receipt))
    }

    /// Claim ready jobs from one queue.
    #[napi]
    pub fn claim_jobs(&self, request: ClaimRequestInput) -> Result<ClaimOutcomeOutput, ErrorCode> {
        let outcome = self
            .store()?
            .claim_jobs(&ms::ClaimRequest {
                queue: request.queue,
                worker_id: request.worker_id,
                now_ms: request.now_ms,
                lease_ms: request.lease_ms,
                limit: request.limit as usize,
            })
            .map_err(map_claim_error)?;
        Ok(match outcome {
            ms::ClaimOutcome::Noop => ClaimOutcomeOutput::bare("noop"),
            ms::ClaimOutcome::MaintenanceCommitted(receipt) => {
                ClaimOutcomeOutput::maintenance(receipt)
            }
            ms::ClaimOutcome::Committed(claims) => ClaimOutcomeOutput::committed(claims),
        })
    }

    /// Durably extend an active lease.
    #[napi]
    pub fn extend_lease(
        &self,
        job_id: String,
        lease_token: String,
        new_expiry_ms: i64,
        now_ms: i64,
    ) -> Result<LeaseExtensionReceiptOutput, ErrorCode> {
        let receipt = self
            .store()?
            .extend_lease(
                parse_id("jobId", &job_id)?,
                parse_id("leaseToken", &lease_token)?,
                new_expiry_ms,
                now_ms,
            )
            .map_err(map_lease_error)?;
        Ok(LeaseExtensionReceiptOutput {
            job_id: receipt.job_id().to_hex(),
            attempt: receipt.attempt(),
            lease_expires_at_ms: receipt.lease_expires_at_ms(),
        })
    }

    /// Recover an indeterminate claim, reconstructing original lease tokens.
    #[napi]
    pub fn recover_claim(
        &self,
        transaction_id: String,
        now_ms: i64,
    ) -> Result<ClaimOutcomeOutput, ErrorCode> {
        let recovery = self
            .store()?
            .recover_claim(parse_id("transactionId", &transaction_id)?, now_ms)
            .map_err(map_recovery_error)?;
        Ok(match recovery {
            ms::ClaimRecovery::Committed(claims) => ClaimOutcomeOutput::committed(claims),
            ms::ClaimRecovery::MaintenanceCommitted(receipt) => {
                ClaimOutcomeOutput::maintenance(receipt)
            }
            ms::ClaimRecovery::Absent => ClaimOutcomeOutput::bare("absent"),
        })
    }

    /// Recover the outcome of an indeterminate commit.
    #[napi]
    pub fn recover_transaction(
        &self,
        transaction_id: String,
    ) -> Result<TransactionRecoveryOutput, ErrorCode> {
        let recovery = self
            .store()?
            .recover_transaction(parse_id("transactionId", &transaction_id)?)
            .map_err(map_recovery_error)?;
        Ok(match recovery {
            ms::TransactionRecovery::Committed(receipt) => TransactionRecoveryOutput {
                kind: "committed".to_string(),
                receipt: Some(receipt_output(receipt)),
            },
            ms::TransactionRecovery::Absent => TransactionRecoveryOutput {
                kind: "absent".to_string(),
                receipt: None,
            },
        })
    }

    /// List jobs, optionally filtered by queue and state, in enqueue order.
    #[napi]
    pub fn jobs(
        &self,
        queue: Option<String>,
        state: Option<String>,
        limit: u32,
    ) -> Result<Vec<JobInfoOutput>, ErrorCode> {
        let state = state.as_deref().map(parse_job_state).transpose()?;
        let infos = self
            .store()?
            .jobs(queue.as_deref(), state, limit as usize)
            .map_err(map_error)?;
        Ok(infos.into_iter().map(job_info_output).collect())
    }

    /// Look up one job by its ID.
    #[napi]
    pub fn job(&self, job_id: String) -> Result<Option<JobInfoOutput>, ErrorCode> {
        let info = self
            .store()?
            .job(parse_id("jobId", &job_id)?)
            .map_err(map_error)?;
        Ok(info.map(job_info_output))
    }

    /// List one page of jobs after a pagination cursor (an enqueue-sequence
    /// value; start from 0), optionally filtered by queue and state. Returns
    /// the page and the cursor to pass for the next page.
    #[napi]
    pub fn jobs_page(
        &self,
        queue: Option<String>,
        state: Option<String>,
        after_sequence: i64,
        limit: u32,
    ) -> Result<JobsPageOutput, ErrorCode> {
        let state = state.as_deref().map(parse_job_state).transpose()?;
        let (infos, next_after_sequence) = self
            .store()?
            .jobs_page(
                queue.as_deref(),
                state,
                after_sequence as u64,
                limit as usize,
            )
            .map_err(map_error)?;
        Ok(JobsPageOutput {
            jobs: infos.into_iter().map(job_info_output).collect(),
            next_after_sequence: next_after_sequence as i64,
        })
    }

    /// Get one projection entry by key.
    #[napi]
    pub fn projection_get(
        &self,
        projection: String,
        key: Buffer,
    ) -> Result<Option<Buffer>, ErrorCode> {
        let value = self
            .store()?
            .projection_get(&projection, &key)
            .map_err(map_error)?;
        Ok(value.map(Buffer::from))
    }

    /// The current version of a projection (0 when it does not exist).
    #[napi]
    pub fn projection_version(&self, projection: String) -> Result<i64, ErrorCode> {
        Ok(self
            .store()?
            .projection_version(&projection)
            .map_err(map_error)? as i64)
    }

    /// The current durable version of a stream (0 when it does not exist).
    #[napi]
    pub fn stream_version(&self, stream_id: String) -> Result<i64, ErrorCode> {
        Ok(self
            .store()?
            .stream_version(&stream_id)
            .map_err(map_error)? as i64)
    }

    /// Events with a global sequence strictly greater than `after`, oldest first.
    #[napi]
    pub fn events_after(
        &self,
        after: i64,
        limit: u32,
    ) -> Result<Vec<PersistedEventOutput>, ErrorCode> {
        let events = self
            .store()?
            .events_after(after as u64, limit as usize)
            .map_err(map_error)?;
        Ok(events.into_iter().map(persisted_event_output).collect())
    }
}
