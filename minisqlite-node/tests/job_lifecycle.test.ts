// Covers the bindings used by the Synara revert-saga integration:
// failJobs (retry vs dead), cancelJobs, lease-expiry -> uncertain ->
// resolveUncertainJobs, and job/jobsPage lookups.
import { expect, test } from "bun:test";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Store, newId } from "../index.js";

const QUEUE = "revert-saga";
const buf = (s: string) => Buffer.from(s, "utf8");

function openStore(): Store {
  const dir = mkdtempSync(join(tmpdir(), "minisqlite-node-"));
  return Store.open(join(dir, "test.db"));
}

function enqueue(store: Store, nowMs: number, maxAttempts?: number): string {
  const jobId = newId();
  store.commit({
    committedAtMs: nowMs,
    enqueueJobs: [
      {
        jobId,
        queue: QUEUE,
        partitionKey: jobId,
        payload: buf("{}"),
        maxAttempts,
      },
    ],
  });
  return jobId;
}

function claimOne(store: Store, nowMs: number, leaseMs = 1_000) {
  const outcome = store.claimJobs({
    queue: QUEUE,
    workerId: "worker-1",
    nowMs,
    leaseMs,
    limit: 1,
  });
  expect(outcome.kind).toBe("committed");
  expect(outcome.jobs.length).toBe(1);
  return outcome.jobs[0];
}

test("failJobs sends the job to retryWait, then dead at maxAttempts", () => {
  const store = openStore();
  let now = 1_000;
  const jobId = enqueue(store, now, 2);

  let claimed = claimOne(store, now);
  store.commit({
    committedAtMs: now,
    failJobs: [
      {
        jobId,
        leaseToken: claimed.leaseToken,
        errorSummary: "provider timeout",
        retryAfterMs: 50,
      },
    ],
  });
  let info = store.job(jobId)!;
  expect(info.state).toBe("retryWait");
  expect(info.errorSummary).toBe("provider timeout");

  now += 100;
  claimed = claimOne(store, now);
  expect(claimed.attempt).toBe(2);
  store.commit({
    committedAtMs: now,
    failJobs: [{ jobId, leaseToken: claimed.leaseToken, errorSummary: "still failing" }],
  });
  expect(store.job(jobId)!.state).toBe("dead");
  store.close();
});

test("cancelJobs cancels a pending job", () => {
  const store = openStore();
  const now = 1_000;
  const jobId = enqueue(store, now);

  store.commit({ committedAtMs: now, cancelJobs: [{ jobId }] });
  expect(store.job(jobId)!.state).toBe("cancelled");
  store.close();
});

test("expired reconcilable lease -> uncertain -> resolveUncertainJobs", () => {
  const store = openStore();
  let now = 1_000;
  const jobId = enqueue(store, now);

  claimOne(store, now, 100);
  now += 1_000; // lease expires; next claim runs expiry maintenance
  store.claimJobs({ queue: QUEUE, workerId: "worker-2", nowMs: now, leaseMs: 100, limit: 1 });

  const uncertain = store.jobs(QUEUE, "uncertain", 10);
  expect(uncertain.map((j) => j.jobId)).toEqual([jobId]);

  store.commit({
    committedAtMs: now,
    resolveUncertainJobs: [{ jobId, resolution: "markSucceeded" }],
  });
  expect(store.job(jobId)!.state).toBe("succeeded");
  store.close();
});

test("resolveUncertainJobs supports retry and markDead", () => {
  const store = openStore();
  let now = 1_000;
  const first = enqueue(store, now);
  const second = enqueue(store, now);

  claimOne(store, now, 100);
  claimOne(store, now, 100);
  now += 1_000;
  store.claimJobs({ queue: QUEUE, workerId: "worker-2", nowMs: now, leaseMs: 100, limit: 1 });

  store.commit({
    committedAtMs: now,
    resolveUncertainJobs: [
      { jobId: first, resolution: "retry" },
      { jobId: second, resolution: "markDead" },
    ],
  });
  expect(store.job(first)!.state).toBe("pending");
  expect(store.job(second)!.state).toBe("dead");
  store.close();
});

test("job returns null for an unknown id", () => {
  const store = openStore();
  expect(store.job(newId())).toBeNull();
  store.close();
});

test("jobsPage pages jobs by cursor", () => {
  const store = openStore();
  const now = 1_000;
  const ids = [enqueue(store, now), enqueue(store, now), enqueue(store, now)];

  const first = store.jobsPage(QUEUE, null, 0, 2);
  expect(first.jobs.map((j) => j.jobId)).toEqual(ids.slice(0, 2));

  const second = store.jobsPage(QUEUE, null, first.nextAfterSequence, 2);
  expect(second.jobs.map((j) => j.jobId)).toEqual(ids.slice(2));

  const third = store.jobsPage(QUEUE, null, second.nextAfterSequence, 2);
  expect(third.jobs).toEqual([]);
  store.close();
});
