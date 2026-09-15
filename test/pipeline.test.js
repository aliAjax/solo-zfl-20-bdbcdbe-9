const test = require("node:test");
const assert = require("node:assert/strict");
const { startServer, registerImage } = require("./helpers");

test("登记影像自动建作业，重复 checksum 只留一份且不新建作业", async () => {
  const api = await startServer();
  try {
    const first = await registerImage(api, { fileName: "a.tif", checksum: "sha256:dup-1" });
    assert.equal(first.job.status, "queued");

    const second = await api.call(
      "POST",
      "/images",
      { rubbingId: "rubbing_demo", fileName: "other-name.tif", checksum: "sha256:dup-1" },
      200
    );
    assert.equal(second.body.duplicated, true);
    assert.equal(second.body.data.image.id, first.image.id);
    assert.equal(second.body.data.job.id, first.job.id);

    const list = await api.call("GET", "/images?rubbingId=rubbing_demo", undefined, 200);
    assert.equal(list.body.data.length, 1);
  } finally {
    await api.close();
  }
});

test("登记不存在的拓片返回 404", async () => {
  const api = await startServer();
  try {
    await api.call(
      "POST",
      "/images",
      { rubbingId: "no-such", fileName: "a.tif", checksum: "sha256:x" },
      404
    );
  } finally {
    await api.close();
  }
});

test("状态机：queued -> processing -> completed，同一作业只有一条结果", async () => {
  const api = await startServer();
  try {
    const { job } = await registerImage(api);

    const empty = await api.call("POST", "/jobs/claim", { workerId: "w1" }, 200);
    assert.equal(empty.body.claimed, true);

    const complete = await api.call(
      "POST",
      `/jobs/${job.id}/complete`,
      { workerId: "w1", output: { storedAs: "s3://bucket/a.tif" }, metrics: { bytes: 12 } },
      200
    );
    assert.equal(complete.body.data.status, "completed");
    assert.ok(complete.body.data.completedAt);
    assert.equal(complete.body.data.result.output.storedAs, "s3://bucket/a.tif");

    // 已完成作业不会再被认领。
    const claimAgain = await api.call("POST", "/jobs/claim", { workerId: "w2" }, 200);
    assert.equal(claimAgain.body.claimed, false);

    // 结果可按作业查，且只有一条。
    const result = await api.call("GET", `/results/${job.id}`, undefined, 200);
    assert.equal(result.body.data.jobId, job.id);
  } finally {
    await api.close();
  }
});

test("同一时刻作业只被一个工作者认领，第二个工作者领不到", async () => {
  const api = await startServer();
  try {
    await registerImage(api);

    const w1 = await api.call("POST", "/jobs/claim", { workerId: "w1", leaseMs: 60_000 }, 200);
    assert.equal(w1.body.claimed, true);
    assert.equal(w1.body.data.workerId, "w1");
    assert.ok(w1.body.data.leaseExpiresAt);

    const w2 = await api.call("POST", "/jobs/claim", { workerId: "w2", leaseMs: 60_000 }, 200);
    assert.equal(w2.body.claimed, false);
  } finally {
    await api.close();
  }
});

test("租约未过期：其他工作者与冒充者交结果都被拒绝", async () => {
  const api = await startServer();
  try {
    const { job } = await registerImage(api);
    await api.call("POST", "/jobs/claim", { workerId: "w1", leaseMs: 60_000 }, 200);

    // 别的工作者冒交。
    const imposter = await api.call(
      "POST",
      `/jobs/${job.id}/complete`,
      { workerId: "w2", output: {} },
      409
    );
    assert.match(imposter.body.error, /未被该工作者持有/);

    // 持有者报失败也必须身份正确。
    const wrongFail = await api.call(
      "POST",
      `/jobs/${job.id}/fail`,
      { workerId: "w2", error: "x" },
      409
    );
    assert.equal(wrongFail.status, 409);
  } finally {
    await api.close();
  }
});

test("租约过期后可被新工作者抢占，旧持有者交结果被拒绝", async () => {
  let t = 1_000_000;
  const clock = { now: () => t };
  const api = await startServer({ clock, leaseMs: 10_000 });
  try {
    const { job } = await registerImage(api);

    const first = await api.call("POST", "/jobs/claim", { workerId: "w1" }, 200);
    assert.equal(first.body.data.attempts, 1);
    assert.equal(first.body.reclaimed, false);

    // 时间推进超过租约。
    t += 10_001;

    // 旧持有者 w1 在过期后交结果：拒绝。
    const stale = await api.call(
      "POST",
      `/jobs/${job.id}/complete`,
      { workerId: "w1", output: { stale: true } },
      409
    );
    assert.match(stale.body.error, /租约已过期/);

    // w2 重新认领成功，attempts 增加，reclaimed=true。
    const second = await api.call("POST", "/jobs/claim", { workerId: "w2" }, 200);
    assert.equal(second.body.claimed, true);
    assert.equal(second.body.reclaimed, true);
    assert.equal(second.body.data.workerId, "w2");
    assert.equal(second.body.data.attempts, 2);

    // w1 再交一次：现在持有者是 w2，仍拒绝。
    const stale2 = await api.call(
      "POST",
      `/jobs/${job.id}/complete`,
      { workerId: "w1", output: {} },
      409
    );
    assert.equal(stale2.status, 409);

    // w2 正常完成。
    const done = await api.call(
      "POST",
      `/jobs/${job.id}/complete`,
      { workerId: "w2", output: { ok: true } },
      200
    );
    assert.equal(done.body.data.status, "completed");
    assert.equal(done.body.data.result.workerId, "w2");
  } finally {
    await api.close();
  }
});

test("失败可重试，达到最大投递次数后转死信", async () => {
  const api = await startServer({ maxAttempts: 3, leaseMs: 60_000 });
  try {
    const { job } = await registerImage(api);

    // 第 1 次认领失败 -> queued（attempts=1，可再试 2 次）。
    await api.call("POST", "/jobs/claim", { workerId: "w1" }, 200);
    const f1 = await api.call("POST", `/jobs/${job.id}/fail`, { workerId: "w1", error: "boom-1" }, 200);
    assert.equal(f1.body.retried, true);
    assert.equal(f1.body.data.status, "queued");
    assert.equal(f1.body.data.lastError, "boom-1");

    // 第 2 次认领失败 -> queued。
    await api.call("POST", "/jobs/claim", { workerId: "w2" }, 200);
    const f2 = await api.call("POST", `/jobs/${job.id}/fail`, { workerId: "w2", error: "boom-2" }, 200);
    assert.equal(f2.body.data.status, "queued");
    assert.equal(f2.body.data.attempts, 2);

    // 第 3 次认领失败 -> dead_letter。
    await api.call("POST", "/jobs/claim", { workerId: "w3" }, 200);
    const f3 = await api.call("POST", `/jobs/${job.id}/fail`, { workerId: "w3", error: "boom-3" }, 200);
    assert.equal(f3.body.retried, false);
    assert.equal(f3.body.data.status, "dead_letter");
    assert.ok(f3.body.data.deadLetteredAt);

    // 死信不再被认领。
    const claim = await api.call("POST", "/jobs/claim", { workerId: "w4" }, 200);
    assert.equal(claim.body.claimed, false);

    // 死信没有结果。
    const result = await api.call("GET", `/results/${job.id}`, undefined, 200);
    assert.equal(result.body.data, null);
  } finally {
    await api.close();
  }
});

test("可按状态与拓片查作业，stats 给出状态数量与死信", async () => {
  const api = await startServer({ maxAttempts: 1, leaseMs: 60_000 });
  try {
    // 建两张拓片：demo + 新拓片。
    const rubbing = await api.call(
      "POST",
      "/rubbings",
      { code: "TP-新-001", source: "馆藏", paperSize: "30x40cm" },
      201
    );
    const otherRubbingId = rubbing.body.data.id;

    const j1 = await registerImage(api, { checksum: "sha256:j1" });
    const j2 = await registerImage(api, { checksum: "sha256:j2", rubbingId: otherRubbingId });
    const j3 = await registerImage(api, { checksum: "sha256:j3" });

    // j1 完成。
    await api.call("POST", "/jobs/claim", { workerId: "w" }, 200);
    await api.call("POST", `/jobs/${j1.job.id}/complete`, { workerId: "w", output: {} }, 200);

    // j2 一次失败进死信（maxAttempts=1）。
    await api.call("POST", "/jobs/claim", { workerId: "w" }, 200);
    await api.call("POST", `/jobs/${j2.job.id}/fail`, { workerId: "w", error: "x" }, 200);
    // j3 保持排队。

    const queued = await api.call("GET", "/jobs?status=queued", undefined, 200);
    assert.deepEqual(queued.body.data.map((j) => j.id), [j3.job.id]);

    const byRubbing = await api.call(
      "GET",
      `/jobs?rubbingId=rubbing_demo`,
      undefined,
      200
    );
    const ids = byRubbing.body.data.map((j) => j.id).sort();
    assert.deepEqual(ids, [j1.job.id, j3.job.id].sort());

    const dead = await api.call("GET", "/jobs?status=dead_letter", undefined, 200);
    assert.deepEqual(dead.body.data.map((j) => j.id), [j2.job.id]);

    const stats = await api.call("GET", "/jobs/stats", undefined, 200);
    assert.equal(stats.body.data.counts.queued, 1);
    assert.equal(stats.body.data.counts.processing, 0);
    assert.equal(stats.body.data.counts.completed, 1);
    assert.equal(stats.body.data.counts.dead_letter, 1);
    assert.equal(stats.body.data.deadLetters.length, 1);
    assert.equal(stats.body.data.deadLetters[0].id, j2.job.id);

    const scoped = await api.call("GET", `/jobs/stats?rubbingId=rubbing_demo`, undefined, 200);
    assert.equal(scoped.body.data.total, 2);
    assert.equal(scoped.body.data.counts.dead_letter, 0);

    const bad = await api.call("GET", "/jobs?status=nope", undefined, 400);
    assert.equal(bad.status, 400);
  } finally {
    await api.close();
  }
});

test("作业字段校验：认领必须带 workerId", async () => {
  const api = await startServer();
  try {
    const noWorker = await api.call("POST", "/jobs/claim", {}, 400);
    assert.match(noWorker.body.error, /workerId/);
  } finally {
    await api.close();
  }
});
