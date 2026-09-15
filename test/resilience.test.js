// 可靠性补丁测试：
// 1) 租约反复过期再认领，投递次数到顶后作业自动转死信，计数不超上限；
// 2) 落盘失败（rename 前故障）时内存回滚，接口读不到未落盘变更，恢复后写入正常。
const test = require("node:test");
const assert = require("node:assert/strict");
const { writeFile, rm, readFile } = require("fs/promises");
const path = require("path");
const { startServer, registerImage } = require("./helpers");

test("租约反复过期：投递次数用尽的作业在下次认领时自动转死信，计数不超上限", async () => {
  let t = 5_000_000;
  const clock = { now: () => t };
  const api = await startServer({ clock, leaseMs: 10_000, maxAttempts: 3 });
  try {
    const { job } = await registerImage(api, { checksum: "sha256:expire-loop" });

    // 第 1 次认领。
    let claim = await api.call("POST", "/jobs/claim", { workerId: "w1" }, 200);
    assert.equal(claim.body.data.attempts, 1);
    assert.equal(claim.body.reclaimed, false);

    // 过期 -> 第 2 次认领（抢占）。
    t += 10_001;
    claim = await api.call("POST", "/jobs/claim", { workerId: "w2" }, 200);
    assert.equal(claim.body.data.attempts, 2);
    assert.equal(claim.body.reclaimed, true);

    // 过期 -> 第 3 次认领（达到上限）。
    t += 10_001;
    claim = await api.call("POST", "/jobs/claim", { workerId: "w3" }, 200);
    assert.equal(claim.body.data.attempts, 3);
    assert.equal(claim.body.reclaimed, true);

    // 再次过期：作业不能再被发出去（否则会第 4 次投递），而是被收尸进死信。
    t += 10_001;
    claim = await api.call("POST", "/jobs/claim", { workerId: "w4" }, 200);
    assert.equal(claim.body.claimed, false);
    assert.equal(claim.body.deadLettered, 1);

    const view = await api.call("GET", `/jobs/${job.id}`, undefined, 200);
    assert.equal(view.body.data.status, "dead_letter");
    assert.equal(view.body.data.attempts, 3, "attempts 不允许超过 maxAttempts");
    assert.ok(view.body.data.deadLetteredAt);
    assert.equal(view.body.data.result, null);

    // 旧持有者在过期后交结果依旧被拒。
    const stale = await api.call(
      "POST",
      `/jobs/${job.id}/complete`,
      { workerId: "w3", output: {} },
      409
    );
    assert.equal(stale.status, 409);

    const stats = await api.call("GET", "/jobs/stats", undefined, 200);
    assert.equal(stats.body.data.counts.dead_letter, 1);
    assert.equal(stats.body.data.deadLetters[0].id, job.id);
  } finally {
    await api.close();
  }
});

test("maxAttempts=1：首次投递过期后不再重投，直接进死信", async () => {
  let t = 9_000_000;
  const clock = { now: () => t };
  const api = await startServer({ clock, leaseMs: 1_000, maxAttempts: 1 });
  try {
    const { job } = await registerImage(api, { checksum: "sha256:expire-once" });
    await api.call("POST", "/jobs/claim", { workerId: "w1" }, 200);

    t += 1_001;
    const claim = await api.call("POST", "/jobs/claim", { workerId: "w2" }, 200);
    assert.equal(claim.body.claimed, false);
    assert.equal(claim.body.deadLettered, 1);

    const view = await api.call("GET", `/jobs/${job.id}`, undefined, 200);
    assert.equal(view.body.data.status, "dead_letter");
    assert.equal(view.body.data.attempts, 1);
  } finally {
    await api.close();
  }
});

const os = require("os");

test("落盘失败：接口报错且内存回滚，恢复后重提成功，磁盘与内存一致", async () => {
  const failFlag = path.join(os.tmpdir(), `rubbing-fail-${process.pid}-${Date.now()}.flag`);
  const api = await startServer({ failPersistSentinel: failFlag, leaseMs: 60_000, maxAttempts: 3 });
  try {
    const { job } = await registerImage(api, { checksum: "sha256:rollback-1" });
    const claim = await api.call("POST", "/jobs/claim", { workerId: "w1" }, 200);
    assert.equal(claim.body.claimed, true);

    // 打开“落盘故障”：complete 在 rename 前抛错。
    await writeFile(failFlag, "1");
    const failed = await api.call(
      "POST",
      `/jobs/${job.id}/complete`,
      { workerId: "w1", output: { should: "not persist" } }
    );
    assert.equal(failed.status, 500);
    assert.match(failed.body.error, /落盘失败/);
    await rm(failFlag, { force: true });

    // 内存已回滚：作业仍是 processing、租约仍在、查不到结果。
    const view = await api.call("GET", `/jobs/${job.id}`, undefined, 200);
    assert.equal(view.body.data.status, "processing");
    assert.equal(view.body.data.workerId, "w1");
    assert.equal(view.body.data.completedAt, null);
    assert.equal(view.body.data.result, null);

    const resultView = await api.call("GET", `/results/${job.id}`, undefined, 200);
    assert.equal(resultView.body.data, null);

    const stats = await api.call("GET", "/jobs/stats", undefined, 200);
    assert.equal(stats.body.data.counts.completed, 0);
    assert.equal(stats.body.data.counts.processing, 1);

    // 租约还在原持有者手里，故障排除后同一持有者重试提交成功。
    const retry = await api.call(
      "POST",
      `/jobs/${job.id}/complete`,
      { workerId: "w1", output: { persisted: true } }
    );
    assert.equal(retry.status, 200);
    assert.equal(retry.body.data.status, "completed");
    assert.equal(retry.body.data.result.output.persisted, true);

    // 磁盘内容与接口一致，且只有一条结果。
    const onDisk = JSON.parse(await readFile(api.dbFile, "utf8"));
    const diskJob = onDisk.jobs.find((j) => j.id === job.id);
    assert.equal(diskJob.status, "completed");
    assert.equal(onDisk.results.filter((r) => r.jobId === job.id).length, 1);
  } finally {
    await rm(failFlag, { force: true });
    await api.close();
  }
});

test("落盘失败不影响下一笔事务：登记失败不留影像/作业，恢复后可重新登记", async () => {
  const failFlag = path.join(os.tmpdir(), `rubbing-fail-${process.pid}-${Date.now()}-b.flag`);
  const api = await startServer({ failPersistSentinel: failFlag });
  try {
    await writeFile(failFlag, "1");
    const failed = await api.call("POST", "/images", {
      rubbingId: "rubbing_demo",
      fileName: "ghost.tif",
      checksum: "sha256:rollback-2"
    });
    assert.equal(failed.status, 500);
    await rm(failFlag, { force: true });

    const images = await api.call("GET", "/images?rubbingId=rubbing_demo", undefined, 200);
    assert.ok(!images.body.data.some((img) => img.checksum === "sha256:rollback-2"));
    const jobs = await api.call("GET", "/jobs?rubbingId=rubbing_demo", undefined, 200);
    assert.equal(jobs.body.data.length, 0);

    // 故障恢复后用同一 checksum 登记应被视为首次（上次登记已整体回滚）。
    const retry = await api.call("POST", "/images", {
      rubbingId: "rubbing_demo",
      fileName: "ghost.tif",
      checksum: "sha256:rollback-2"
    });
    assert.equal(retry.status, 201);
    assert.equal(retry.body.duplicated, false);
  } finally {
    await rm(failFlag, { force: true });
    await api.close();
  }
});
