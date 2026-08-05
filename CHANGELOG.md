# Changelog

## 0.3.0-alpha.1 - Unreleased

Complete product rewrite: minisqlite now commits events, current state, and
durable background jobs in one atomic SQLite transaction, replacing the
from-scratch SQL engine.

### Added

- Semantic public API: `ControlPlaneStore`, `CommitBatch`, and typed
  operations for events, projection patches, and job lifecycle
- Event streams with global sequencing, per-stream versions, optimistic
  concurrency (`ExpectedStreamVersion`), and idempotent transaction
  resubmission via request digests
- Versioned projections with put/delete/clear/replace mutations, prefix and
  range scans, and strict version increments
- Durable jobs: partition-ordered claiming, leases with extension, retries,
  max attempts, cancellation, and explicit effect modes
  (`reconcilable`, `idempotent`, `intrinsically_idempotent`)
- Honest uncertainty handling: indeterminate commits and claims return typed
  errors (`IndeterminateCommit`, `IndeterminateClaim`) with no payloads or
  lease tokens, and are
  recovered explicitly via `recover_transaction` / `recover_claim`;
  uncertain claims can never yield executable work
- SQLite backend (WAL mode, checksummed forward-only migrations, `Strict` /
  `Relaxed` durability)
- Operational tooling: `verify`, `stats`, live `backup`, redacted
  `diagnostic_export`, migration status, and a CLI
  (`doctor`, `verify`, `stats`, `events tail`, `projections list`,
  `jobs list`, `backup`, `diagnostic-export`, `migrations status`)
- Backend-independent contract test suite
- Optional `partition_key` filter on `ClaimRequest` (and `partitionKey` on the
  Node binding's `ClaimRequestInput`) to claim only one partition's ready head
  job; maintenance stays queue-wide and the round-robin cursor is unaffected

### Breaking changes

- The SQL engine, parser, page-based storage, and the `Database` /
  `ExecuteResult` API are removed; there is no migration path from 0.2.x
  database files
- The interactive SQL CLI is replaced by the operational CLI above
- The append-only `.mini` journal prototype is archived on the
  `archive/append-only-journal-v1` branch
- Adds a dependency on `rusqlite` (bundled SQLite); the crate is no longer
  zero-dependency

### Non-goals

- SQL exposed through the public API, distributed consensus, multi-process
  writers, replication, a workflow DSL, a cron engine, a remote server, or a
  public multi-backend storage abstraction

## 0.2.1 - 2026-07-19

- Polished public API and library documentation
- Fixed all `cargo clippy` warnings and applied `cargo fmt`
- Added `hex_encode` helper for consistent blob rendering
- Improved README with crates.io badges, install instructions, and product positioning

## 0.2.0 - 2026-07-19

- Initial public release of `minisqlite`
- Page-based storage with a custom `MiniSQL2` file format
- SQL parser and executor supporting DDL, DML, `SELECT`, joins, aggregates, transactions, `PRAGMA`, and dot commands
- Pure Rust, zero external dependencies
- Library + CLI crate structure
- Examples and integration tests
