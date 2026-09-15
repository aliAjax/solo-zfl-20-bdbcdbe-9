// 真实进程崩溃恢复测试：在“内存已改成 completed、结果已入内存、尚未落盘”的瞬间 SIGKILL，
// 验证磁盘不留半成品；重启后作业可继续、结果只有一条。
const test = require("node:test");
const assert = require("node:assert/strict");
const { mkdtemp, rm, writeFile, readFile, readdir } = require("fs/promises");
const os = require("os");
const path = require("path");
const { startChild, onExit } = require("./child");

test("SIGKILL 打断完成提交：不落半成品，重启后作业仍是 processing 且无结果，可继续完成", async () => {
  const dir = await mkdtemp(path.join(os.tmpdir(), "rubbing-crash-"));
  const flag = path.join(dir, "crash.flag");
  const dbFile = path.join(dir, "db.json");
  try {
    const api = startChild(dir, { crash: true });
    await api.ready;

    const reg = await api.call("POST", "/images", {
      rubbingId: "rubbing_demo",
      fileName: "crash.tif",
      checksum: "sha256:crash-case-1"
    });
    const jobId = reg.body.data.job.id;
    await api.call("POST", "/jobs/claim", { workerId: "w1" });

    await writeFile(flag, "1");
    const exited = onExit(api.child);
    let requestError = null;
    try {
      await api.call("POST", `/jobs/${jobId}/complete`, { workerId: "w1", output: { doomed: true } });
    } catch (error) {
      requestError = error; // 连接被重置是预期现象。
    }
    const { signal } = await exited;
    assert.equal(signal, "SIGKILL");
    assert.ok(requestError, "被杀时进行中的请求应当以连接错误告终");

    const db = JSON.parse(await readFile(dbFile, "utf8"));
    const job = db.jobs.find((j) => j.id === jobId);
    assert.equal(job.status, "processing");
    assert.equal(job.workerId, "w1");
    assert.equal(db.results.filter((r) => r.jobId === jobId).length, 0);
    const leftovers = (await readdir(dir)).filter((name) => name.endsWith(".tmp"));
    assert.deepEqual(leftovers, []);

    await rm(flag, { force: true });
    const api2 = startChild(dir);
    await api2.ready;
    try {
      const done = await api2.call("POST", `/jobs/${jobId}/complete`, {
        workerId: "w1",
        output: { recovered: true }
      });
      assert.equal(done.status, 200);
      assert.equal(done.body.data.status, "completed");
      assert.equal(done.body.data.result.output.recovered, true);

      const result = await api2.call("GET", `/results/${jobId}`);
      assert.equal(result.body.data.output.doomed, undefined);
      assert.equal(result.body.data.output.recovered, true);

      const db2 = JSON.parse(await readFile(dbFile, "utf8"));
      assert.equal(db2.results.filter((r) => r.jobId === jobId).length, 1);
    } finally {
      await api2.stop();
    }
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});

test("processing 中整机被杀，重启后等租约过期可被其他工作者重新认领并完成", async () => {
  const dir = await mkdtemp(path.join(os.tmpdir(), "rubbing-crash-"));
  const flag = path.join(dir, "crash.flag");
  const dbFile = path.join(dir, "db.json");
  try {
    const api = startChild(dir, { crash: true });
    await api.ready;
    const reg = await api.call("POST", "/images", {
      rubbingId: "rubbing_demo",
      fileName: "crash2.tif",
      checksum: "sha256:crash-case-2"
    });
    const jobId = reg.body.data.job.id;
    await api.call("POST", "/jobs/claim", { workerId: "w1", leaseMs: 3_000 });
    await writeFile(flag, "1");
    const exited = onExit(api.child);
    try {
      await api.call("POST", `/jobs/${jobId}/fail`, { workerId: "w1", error: "dying" });
    } catch {}
    await exited;

    await rm(flag, { force: true });
    const api2 = startChild(dir);
    await api2.ready;
    try {
      const early = await api2.call("POST", "/jobs/claim", { workerId: "w2" });
      assert.equal(early.body.claimed, false);

      const jobView = await api2.call("GET", `/jobs/${jobId}`);
      const remaining = Date.parse(jobView.body.data.leaseExpiresAt) - Date.now();
      if (remaining > 0) await new Promise((r) => setTimeout(r, remaining + 50));

      const reclaim = await api2.call("POST", "/jobs/claim", { workerId: "w2", leaseMs: 60_000 });
      assert.equal(reclaim.body.claimed, true);
      assert.equal(reclaim.body.reclaimed, true);
      assert.equal(reclaim.body.data.workerId, "w2");

      const done = await api2.call("POST", `/jobs/${jobId}/complete`, {
        workerId: "w2",
        output: { ok: 1 }
      });
      assert.equal(done.body.data.status, "completed");
      const db2 = JSON.parse(await readFile(dbFile, "utf8"));
      assert.equal(db2.results.length, 1);
    } finally {
      await api2.stop();
    }
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});
