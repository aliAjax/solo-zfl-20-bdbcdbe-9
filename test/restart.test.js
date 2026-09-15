const test = require("node:test");
const assert = require("node:assert/strict");
const { mkdtemp, rm, readFile, readdir } = require("fs/promises");
const os = require("os");
const path = require("path");
const { createApp } = require("../server");

// 每个测试针对同一数据文件反复“开服/关服”，关服即等价于进程退出。
async function harness(options = {}) {
  const dir = await mkdtemp(path.join(os.tmpdir(), "rubbing-restart-"));
  const dbFile = path.join(dir, "db.json");

  async function open(overrides = {}) {
    const { server } = createApp({
      dbFile,
      leaseMs: 60_000,
      maxAttempts: 3,
      ...options,
      ...overrides
    });
    await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
    const base = `http://127.0.0.1:${server.address().port}`;

    async function call(method, urlPath, body) {
      const init = { method };
      if (body !== undefined) {
        init.headers = { "Content-Type": "application/json" };
        init.body = JSON.stringify(body);
      }
      const res = await fetch(base + urlPath, init);
      return { status: res.status, body: await res.json() };
    }
    return {
      call,
      close: () => new Promise((resolve, reject) => server.close((err) => (err ? reject(err) : resolve())))
    };
  }

  async function rawDb() {
    return JSON.parse(await readFile(dbFile, "utf8"));
  }

  async function tempFiles() {
    return (await readdir(dir)).filter((name) => name.endsWith(".tmp"));
  }

  return { dir, dbFile, open, rawDb, tempFiles, cleanup: () => rm(dir, { recursive: true, force: true }) };
}

test("重启后排队作业不丢，可继续认领并完成", async () => {
  const h = await harness();
  try {
    let api = await h.open();
    const reg = await api.call("POST", "/images", {
      rubbingId: "rubbing_demo",
      fileName: "a.tif",
      checksum: "sha256:restart-1"
    });
    const jobId = reg.body.data.job.id;
    await api.close(); // 进程退出，作业仍在排队。

    api = await h.open();
    const claim = await api.call("POST", "/jobs/claim", { workerId: "w-after-restart" });
    assert.equal(claim.status, 200);
    assert.equal(claim.body.claimed, true);
    assert.equal(claim.body.data.id, jobId);

    const done = await api.call("POST", `/jobs/${jobId}/complete`, {
      workerId: "w-after-restart",
      output: { ok: true }
    });
    assert.equal(done.body.data.status, "completed");
    await api.close();
  } finally {
    await h.cleanup();
  }
});

test("已完成作业重启后不重跑：不被认领、结果仍唯一且可查", async () => {
  const h = await harness();
  try {
    let api = await h.open();
    const reg = await api.call("POST", "/images", {
      rubbingId: "rubbing_demo",
      fileName: "b.tif",
      checksum: "sha256:restart-2"
    });
    const jobId = reg.body.data.job.id;
    await api.call("POST", "/jobs/claim", { workerId: "w1" });
    await api.call("POST", `/jobs/${jobId}/complete`, {
      workerId: "w1",
      output: { storedAs: "s3://bucket/b.tif" }
    });
    await api.close();

    api = await h.open();
    // 没有别的作业，认领应为空。
    const claim = await api.call("POST", "/jobs/claim", { workerId: "w2" });
    assert.equal(claim.body.claimed, false);

    const job = await api.call("GET", `/jobs/${jobId}`);
    assert.equal(job.body.data.status, "completed");
    assert.equal(job.body.data.workerId, "w1");

    const result = await api.call("GET", `/results/${jobId}`);
    assert.equal(result.body.data.output.storedAs, "s3://bucket/b.tif");

    // 重启后尝试再交一次结果：已完成、未被当前工作者持有，拒绝。
    const again = await api.call("POST", `/jobs/${jobId}/complete`, { workerId: "w2", output: {} });
    assert.equal(again.status, 409);

    const db = await h.rawDb();
    assert.equal(db.results.filter((r) => r.jobId === jobId).length, 1);
    await api.close();
  } finally {
    await h.cleanup();
  }
});

test("处理中崩溃：重启后租约未到期前不可重领，过期后可继续处理且只产生一条结果", async () => {
  const h = await harness({ leaseMs: 10_000 });
  try {
    let api = await h.open();
    const reg = await api.call("POST", "/images", {
      rubbingId: "rubbing_demo",
      fileName: "c.tif",
      checksum: "sha256:restart-3"
    });
    const jobId = reg.body.data.job.id;
    await api.call("POST", "/jobs/claim", { workerId: "w1" });
    // 工作者在 processing 期间进程崩溃（服务直接关闭，租约时间仍写在磁盘上）。
    await api.close();

    // 立即重启：租约尚未过期，不允许别人抢占（等待原持有者恢复或超时）。
    api = await h.open();
    const tooSoon = await api.call("POST", "/jobs/claim", { workerId: "w2" });
    assert.equal(tooSoon.body.claimed, false);

    // 等到租约过期后重启，可重新认领。
    await api.close();
    const future = new Date(Date.now() + 11_000);
    api = await h.open({ clock: { now: () => future.getTime() } });
    const reclaim = await api.call("POST", "/jobs/claim", { workerId: "w2" });
    assert.equal(reclaim.body.claimed, true);
    assert.equal(reclaim.body.reclaimed, true);
    assert.equal(reclaim.body.data.id, jobId);

    const done = await api.call("POST", `/jobs/${jobId}/complete`, {
      workerId: "w2",
      output: { ok: true }
    });
    assert.equal(done.body.data.status, "completed");
    const db = await h.rawDb();
    assert.equal(db.jobs.filter((j) => j.id === jobId).length, 1);
    assert.equal(db.results.filter((r) => r.jobId === jobId).length, 1);
    await api.close();
  } finally {
    await h.cleanup();
  }
});

test("失败转排队的作业重启后仍可重试，死信状态也持久化", async () => {
  const h = await harness({ maxAttempts: 2 });
  try {
    let api = await h.open();
    const reg = await api.call("POST", "/images", {
      rubbingId: "rubbing_demo",
      fileName: "d.tif",
      checksum: "sha256:restart-4"
    });
    const jobId = reg.body.data.job.id;
    await api.call("POST", "/jobs/claim", { workerId: "w1" });
    const failed = await api.call("POST", `/jobs/${jobId}/fail`, { workerId: "w1", error: "retry me" });
    assert.equal(failed.body.data.status, "queued");
    await api.close();

    api = await h.open();
    const claim = await api.call("POST", "/jobs/claim", { workerId: "w2" });
    assert.equal(claim.body.data.attempts, 2);
    const failAgain = await api.call("POST", `/jobs/${jobId}/fail`, { workerId: "w2", error: "again" });
    assert.equal(failAgain.body.data.status, "dead_letter");
    await api.close();

    api = await h.open();
    const stats = await api.call("GET", "/jobs/stats");
    assert.equal(stats.body.data.counts.dead_letter, 1);
    const afterDead = await api.call("POST", "/jobs/claim", { workerId: "w3" });
    assert.equal(afterDead.body.claimed, false);
    await api.close();
  } finally {
    await h.cleanup();
  }
});

test("普通关服路径下没有 tmp 残留文件", async () => {
  const h = await harness();
  try {
    const api = await h.open();
    const reg = await api.call("POST", "/images", {
      rubbingId: "rubbing_demo",
      fileName: "e.tif",
      checksum: "sha256:restart-5"
    });
    const jobId = reg.body.data.job.id;
    await api.call("POST", "/jobs/claim", { workerId: "w1" });
    await api.call("POST", `/jobs/${jobId}/complete`, { workerId: "w1", output: {} });
    await api.close();
    assert.deepEqual(await h.tempFiles(), []);
    const db = await h.rawDb();
    assert.equal(db.results.filter((r) => r.jobId === jobId).length, 1);
  } finally {
    await h.cleanup();
  }
});
