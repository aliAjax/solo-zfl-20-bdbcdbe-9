// 多进程互斥测试：两个独立的 server.js 进程共用同一份数据文件，
// 验证同一个（批）作业不会被两个进程里的工作者同时认领。
const test = require("node:test");
const assert = require("node:assert/strict");
const { mkdtemp, rm } = require("fs/promises");
const os = require("os");
const path = require("path");
const { startChild } = require("./child");

async function tempDir() {
  return mkdtemp(path.join(os.tmpdir(), "rubbing-multiproc-"));
}

test("两个进程同时认领同一作业：恰好一个成功", async () => {
  const dir = await tempDir();
  const p1 = startChild(dir);
  const p2 = startChild(dir);
  await Promise.all([p1.ready, p2.ready]);
  try {
    const reg = await p1.call("POST", "/images", {
      rubbingId: "rubbing_demo",
      fileName: "m1.tif",
      checksum: "sha256:multi-1"
    });
    const jobId = reg.body.data.job.id;

    // 两个进程几乎同一刻发起认领。
    const [r1, r2] = await Promise.all([
      p1.call("POST", "/jobs/claim", { workerId: "worker-A", leaseMs: 60_000 }),
      p2.call("POST", "/jobs/claim", { workerId: "worker-B", leaseMs: 60_000 })
    ]);

    const winners = [r1, r2].filter((r) => r.body.claimed);
    assert.equal(winners.length, 1, "必须恰好一个进程认领成功");
    const loser = [r1, r2].find((r) => !r.body.claimed);
    assert.equal(loser.body.claimed, false);

    // 两边随后读到的持有者必须一致（读事务会持锁重读磁盘）。
    const [v1, v2] = await Promise.all([p1.call("GET", `/jobs/${jobId}`), p2.call("GET", `/jobs/${jobId}`)]);
    assert.equal(v1.body.data.workerId, v2.body.data.workerId);
    const holder = v1.body.data.workerId;
    assert.ok(["worker-A", "worker-B"].includes(holder));

    // 失败者硬交结果必须被拒；只有持有者能完成。
    const other = holder === "worker-A" ? ["worker-B", p2] : ["worker-A", p1];
    const rejected = await other[1].call("POST", `/jobs/${jobId}/complete`, {
      workerId: other[0],
      output: { forged: true }
    });
    assert.equal(rejected.status, 409);

    const owner = holder === "worker-A" ? p1 : p2;
    const done = await owner.call("POST", `/jobs/${jobId}/complete`, {
      workerId: holder,
      output: { ok: true }
    });
    assert.equal(done.status, 200);
    assert.equal(done.body.data.status, "completed");

    const stats = await p2.call("GET", "/jobs/stats");
    assert.equal(stats.body.data.counts.completed, 1);
    assert.equal(stats.body.data.counts.processing, 0);
  } finally {
    await Promise.all([p1.stop(), p2.stop()]);
    await rm(dir, { recursive: true, force: true });
  }
});

test("多进程并发抽干队列：每个作业恰好被认领并完成一次", async () => {
  const dir = await tempDir();
  const p1 = startChild(dir);
  const p2 = startChild(dir);
  await Promise.all([p1.ready, p2.ready]);
  try {
    const JOB_COUNT = 8;
    const jobIds = [];
    for (let i = 0; i < JOB_COUNT; i++) {
      const reg = await p1.call("POST", "/images", {
        rubbingId: "rubbing_demo",
        fileName: `m${i}.tif`,
        checksum: `sha256:multi-batch-${i}`
      });
      jobIds.push(reg.body.data.job.id);
    }

    // 每个进程连续“认领+完成”多轮；跨进程锁应保证不重不漏。
    async function drain(proc, worker, rounds) {
      for (let i = 0; i < rounds; i++) {
        const claim = await proc.call("POST", "/jobs/claim", { workerId: worker, leaseMs: 60_000 });
        if (!claim.body.claimed) continue;
        const res = await proc.call("POST", `/jobs/${claim.body.data.id}/complete`, {
          workerId: worker,
          output: { by: worker }
        });
        // 持有者持锁，可能被过期抢占的情况在长租约下不会发生；必须一次成功。
        assert.equal(res.status, 200, `工作者 ${worker} 完成自己认领的作业不应失败`);
      }
    }
    await Promise.all([drain(p1, "worker-A", JOB_COUNT), drain(p2, "worker-B", JOB_COUNT)]);

    const jobs = await p1.call("GET", "/jobs");
    assert.equal(jobs.body.data.length, JOB_COUNT);
    assert.ok(jobs.body.data.every((j) => j.status === "completed"));
    assert.ok(jobs.body.data.every((j) => j.result && j.result.workerId === j.workerId));

    const stats = await p2.call("GET", "/jobs/stats");
    assert.equal(stats.body.data.counts.completed, JOB_COUNT);
    assert.equal(stats.body.data.counts.queued, 0);
    assert.equal(stats.body.data.counts.processing, 0);
  } finally {
    await Promise.all([p1.stop(), p2.stop()]);
    await rm(dir, { recursive: true, force: true });
  }
});

test("不同进程重复登记同一 checksum 也只产生一份影像与作业", async () => {
  const dir = await tempDir();
  const p1 = startChild(dir);
  const p2 = startChild(dir);
  await Promise.all([p1.ready, p2.ready]);
  try {
    const payload = {
      rubbingId: "rubbing_demo",
      fileName: "dup.tif",
      checksum: "sha256:multi-dup"
    };
    const [r1, r2] = await Promise.all([
      p1.call("POST", "/images", payload),
      p2.call("POST", "/images", { ...payload, fileName: "different-name.tif" })
    ]);
    const created = [r1, r2].filter((r) => r.status === 201);
    const duplicates = [r1, r2].filter((r) => r.status === 200 && r.body.duplicated);
    assert.equal(created.length, 1);
    assert.equal(duplicates.length, 1);
    assert.equal(created[0].body.data.image.id, duplicates[0].body.data.image.id);

    const list = await p1.call("GET", "/images");
    assert.equal(list.body.data.length, 1);
  } finally {
    await Promise.all([p1.stop(), p2.stop()]);
    await rm(dir, { recursive: true, force: true });
  }
});
