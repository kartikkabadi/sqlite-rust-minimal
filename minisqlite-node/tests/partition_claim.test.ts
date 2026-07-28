// Partition-scoped claiming: a claim with partitionKey leases only that
// partition's ready head job, leaving other partitions untouched.
import { expect, test } from "bun:test";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Store, newId } from "../index.js";

const buf = (s: string) => Buffer.from(s, "utf8");

function enqueue(store: Store, partitionKey: string): string {
  const jobId = newId();
  store.commit({
    committedAtMs: 1_000,
    enqueueJobs: [{ jobId, queue: "q", partitionKey, payload: buf("{}") }],
  });
  return jobId;
}

test("claimJobs with partitionKey leases only that partition's head", () => {
  const dir = mkdtempSync(join(tmpdir(), "minisqlite-node-"));
  const store = Store.open(join(dir, "db"));
  const a = enqueue(store, "a");
  const b = enqueue(store, "b");
  enqueue(store, "b");

  const outcome = store.claimJobs({
    queue: "q",
    workerId: "w1",
    nowMs: 2_000,
    leaseMs: 10_000,
    limit: 10,
    partitionKey: "b",
  });
  expect(outcome.kind).toBe("committed");
  expect(outcome.jobs.length).toBe(1);
  expect(outcome.jobs[0].jobId).toBe(b);
  expect(outcome.jobs[0].partitionKey).toBe("b");
  // Partition a is untouched.
  const jobA = store.jobs("q", null, 10).find((j) => j.jobId === a);
  expect(jobA?.state).toBe("pending");

  // A miss on an idle partition is a noop.
  const miss = store.claimJobs({
    queue: "q",
    workerId: "w1",
    nowMs: 3_000,
    leaseMs: 10_000,
    limit: 1,
    partitionKey: "zz",
  });
  expect(miss.kind).toBe("noop");
});
