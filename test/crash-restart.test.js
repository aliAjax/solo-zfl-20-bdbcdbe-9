// 真实进程崩溃恢复测试：把 server.js 作为独立子进程启动，
// 在“作业内存状态已改为 completed、结果已入内存，但尚未落盘”的瞬间 SIGKILL，
// 验证磁盘不留半成品；随后重启新进程验证作业可继续、结果仍只有一条。
const test = require("node:test");
const assert = require("node:assert/strict");
const { spawn } = require("child_process");
const { mkdtemp, rm, writeFile, readFile, readdir } = require("fs/promises");
const os = require("os");
const path = require("path");

const SERVER = path.join(__dirname, "..", "server.js");

async function startChild(dir, { crash = false } = {}) {
  const env = {
    ...process.env,
    PORT: "0",
    DB_FILE: path.join(dir, "db.json"),
    LEASE_MS: "60000",
    MAX_ATTEMPTS: "3"
  };
  if (crash) env.CRASH_SENTINEL = path.join(dir, "crash.flag");
  const child = spawn(process.execPath, [SERVER], { env });

  let base;
  child.stdout.setEncoding("utf8");
  child.stdout.on("data", (chunk) => {
    const match = chunk.match(/http:\/\/127\.0\.0\.1:(\d+)/);
    if (match) base = `http://127.0.0.1:${match[1]}`;
  });
  child.stderr.pipe(process.stderr);

  for (let i = 0; i < 100 && !base; i++) {
    await new Promise((r) => setTimeout(r, 30));
  }
  if (!base) throw new Error("子进程未在预期时间内启动");
  // 等服务可接请求。
  for (let i = 0; i < 50; i++) {
    try {
      const res = await fetch(`${base}/health`);
      if (res.ok) break;
    } catch {}
    await new Promise((r) => setTimeout(r, 30));
  }

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
    child,
    call,
    stop: () => {
      if (child.killed) return Promise.resolve();
      child.kill("SIGTERM");
      return new Promise((resolve) => child.on("exit", () => resolve()));
    }
  };
}

function onExit(child) {
  return new Promise((resolve) => child.on("exit", (code, signal) => resolve({ code, signal })));
}

test("SIGKILL 打断完成提交：不落半成品，重启后作业仍是 processing 且无结果，可继续完成", async () => {
  const dir = await mkdtemp(path.join(os.tmpdir(), "rubbing-crash-"));
  const flag = path.join(dir, "crash.flag");
  const dbFile = path.join(dir, "db.json");
  try {
    const api = await startChild(dir, { crash: true });

    const reg = await api.call("POST", "/images", {
      rubbingId: "rubbing_demo",
      fileName: "crash.tif",
      checksum: "sha256:crash-case-1"
    });
    const jobId = reg.body.data.job.id;
    await api.call("POST", "/jobs/claim", { workerId: "w1" });

    // 打开崩溃开关：下一个写事务在改完内存、落盘前被 SIGKILL。
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

    // 磁盘上：作业仍停在 processing，没有结果，没有半截 JSON/tmp 残留。
    const db = JSON.parse(await readFile(dbFile, "utf8"));
    const job = db.jobs.find((j) => j.id === jobId);
    assert.equal(job.status, "processing");
    assert.equal(job.workerId, "w1");
    assert.equal(db.results.filter((r) => r.jobId === jobId).length, 0);
    const leftovers = (await readdir(dir)).filter((name) => name.endsWith(".tmp"));
    assert.deepEqual(leftovers, []);

    // 移除崩溃开关，启动新进程（模拟服务重启）。
    await rm(flag, { force: true });
    const api2 = await startChild(dir);
    try {
      // 租约仍在有效期内：原持有者 w1 可继续提交（崩溃的是服务，不是工作者）。
      const done = await api2.call("POST", `/jobs/${jobId}/complete`, {
        workerId: "w1",
        output: { recovered: true }
      });
      assert.equal(done.status, 200);
      assert.equal(done.body.data.status, "completed");
      assert.equal(done.body.data.result.output.recovered, true);

      // 再查：只有一条结果。
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
    const api = await startChild(dir, { crash: true });
    const reg = await api.call("POST", "/images", {
      rubbingId: "rubbing_demo",
      fileName: "crash2.tif",
      checksum: "sha256:crash-case-2"
    });
    const jobId = reg.body.data.job.id;
    // 短租约：认领时覆盖为 3000ms，留足子进程启动余量。
    await api.call("POST", "/jobs/claim", { workerId: "w1", leaseMs: 3_000 });
    await writeFile(flag, "1");
    const exited = onExit(api.child);
    try {
      await api.call("POST", `/jobs/${jobId}/fail`, { workerId: "w1", error: "dying" });
    } catch {}
    await exited;

    await rm(flag, { force: true });
    const api2 = await startChild(dir);
    try {
      // 租约未过期前抢不到。
      const early = await api2.call("POST", "/jobs/claim", { workerId: "w2" });
      assert.equal(early.body.claimed, false);

      // 按作业上记录的到期时间精确等待，避免拍脑袋 sleep。
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
