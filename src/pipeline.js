// 影像入库流水线领域逻辑。
//
// 状态机：
//   queued（排队） ──认领──▶ processing（处理中）
//     processing 租约未过期：只有持有者能交结果/报失败；
//     processing 租约已过期：可被任意工作者重新认领（attempts +1），旧持有者的提交一律拒绝；
//   processing ──成功──▶ completed（已完成，且同一作业只有一条结果）；
//   processing ──失败──▶ 投递次数未用尽：queued（可重试）；用尽：dead_letter（死信）。
//
// 所有状态变更都在 store.update 的同一事务里完成：作业状态与结果同生共死，
// 进程在落盘前被杀时两者都不存在，落盘后两者一起生效。

const STATUSES = ["queued", "processing", "completed", "dead_letter"];

function httpError(status, message) {
  const error = new Error(message);
  error.status = status;
  return error;
}

function nowIso(clock) {
  return new Date(clock.now()).toISOString();
}

function makeId(prefix, clock) {
  // 时间序前缀 + 随机后缀；配合作业上的 createdAt，认领按 FIFO 稳定排序。
  return `${prefix}_${clock.now().toString(36)}_${Math.random().toString(36).slice(2, 10)}`;
}

function requireString(body, field) {
  if (typeof body[field] !== "string" || body[field].trim() === "") {
    throw httpError(400, `缺少字段：${field}`);
  }
  return body[field].trim();
}

function getRubbing(state, rubbingId) {
  const rubbing = state.rubbings.find((item) => item.id === rubbingId);
  if (!rubbing) throw httpError(404, "拓片不存在");
  return rubbing;
}

function getJob(state, jobId) {
  const job = state.jobs.find((item) => item.id === jobId);
  if (!job) throw httpError(404, "作业不存在");
  return job;
}

// 登记影像：拓片 + 文件名 + 内容校验和；同一校验和全库只留一份。
// 已存在时不重复建作业，返回原影像/原作业并标记 duplicated。
function registerImage(state, body, clock, options) {
  const rubbingId = requireString(body, "rubbingId");
  const fileName = requireString(body, "fileName");
  const checksum = requireString(body, "checksum");
  getRubbing(state, rubbingId);

  const existing = state.images.find((item) => item.checksum === checksum);
  if (existing) {
    const job = state.jobs.find((item) => item.imageId === existing.id) || null;
    return { duplicated: true, image: existing, job };
  }

  const ts = nowIso(clock);
  const image = {
    id: makeId("img", clock),
    rubbingId,
    fileName,
    checksum,
    sizeBytes: Number.isFinite(body.sizeBytes) ? body.sizeBytes : null,
    contentType: typeof body.contentType === "string" ? body.contentType : "",
    createdAt: ts
  };
  const job = {
    id: makeId("job", clock),
    type: "image_ingest",
    imageId: image.id,
    rubbingId,
    status: "queued",
    attempts: 0,
    maxAttempts: options.maxAttempts,
    leaseMs: options.leaseMs,
    workerId: null,
    leaseExpiresAt: null,
    lastError: null,
    createdAt: ts,
    queuedAt: ts,
    startedAt: null,
    completedAt: null,
    deadLetteredAt: null
  };
  state.images.push(image);
  state.jobs.push(job);
  return { duplicated: false, image, job };
}

// 找下一个可认领作业：排队中，或 processing 但租约已过期（可抢占）。
// 统一按“最早可领取时间”排序：排队作业取 createdAt，过期作业取 leaseExpiresAt。
function findClaimable(state, now) {
  const candidates = state.jobs
    .map((job) => {
      if (job.status === "queued") return { job, availableAt: Date.parse(job.createdAt) };
      if (job.status === "processing" && job.leaseExpiresAt) {
        const expiresAt = Date.parse(job.leaseExpiresAt);
        if (expiresAt <= now) return { job, availableAt: expiresAt };
      }
      return null;
    })
    .filter(Boolean);
  candidates.sort((a, b) => a.availableAt - b.availableAt);
  return candidates.length ? candidates[0].job : null;
}

function claimNextJob(state, body, clock) {
  const workerId = requireString(body, "workerId");
  const now = clock.now();
  const job = findClaimable(state, now);
  if (!job) return null;

  const leaseMs = Number.isFinite(body.leaseMs) && body.leaseMs > 0 ? body.leaseMs : job.leaseMs;
  const reclaimed = job.status === "processing";
  job.status = "processing";
  job.attempts += 1;
  job.workerId = workerId;
  job.leaseExpiresAt = new Date(now + leaseMs).toISOString();
  job.startedAt = job.startedAt || new Date(now).toISOString();
  job.lastError = null;

  const image = state.images.find((item) => item.id === job.imageId) || null;
  return { job, image, reclaimed };
}

// 持租约校验：作业必须在 processing，且提交者是当前持有者、租约未过期。
// 过期后作业可能已被别人重新认领，旧持有者交结果必须被拒绝（409）。
function verifyLease(job, workerId, now) {
  if (job.status !== "processing" || job.workerId !== workerId) {
    throw httpError(409, "作业未被该工作者持有");
  }
  if (!job.leaseExpiresAt || Date.parse(job.leaseExpiresAt) <= now) {
    throw httpError(409, "租约已过期，作业可被其他工作者重新认领");
  }
}

// 提交成功结果：结果与 completed 状态在同一事务里生效；重复提交（已完成）409。
function completeJob(state, jobId, body, clock) {
  const workerId = requireString(body, "workerId");
  const job = getJob(state, jobId);
  const now = clock.now();
  verifyLease(job, workerId, now);
  if (state.results.some((item) => item.jobId === jobId)) {
    // 正常走不到：能持有效租约说明没完成；这是同一作业最多一条结果的双保险。
    throw httpError(409, "作业已有结果");
  }

  const ts = new Date(now).toISOString();
  const result = {
    id: makeId("res", clock),
    jobId: job.id,
    imageId: job.imageId,
    workerId,
    output: body.output ?? null,
    metrics: body.metrics && typeof body.metrics === "object" ? body.metrics : {},
    createdAt: ts
  };
  state.results.push(result);
  job.status = "completed";
  job.completedAt = ts;
  job.leaseExpiresAt = null;
  job.lastError = null;
  return { job, result };
}

// 报告失败：仍可重试则回到 queued，投递次数用尽转 dead_letter。
function failJob(state, jobId, body, clock) {
  const workerId = requireString(body, "workerId");
  const job = getJob(state, jobId);
  const now = clock.now();
  verifyLease(job, workerId, now);

  const error = typeof body.error === "string" && body.error.trim() ? body.error.trim() : "unknown error";
  job.lastError = error;
  job.workerId = null;
  job.leaseExpiresAt = null;

  if (job.attempts >= job.maxAttempts) {
    job.status = "dead_letter";
    job.deadLetteredAt = new Date(now).toISOString();
  } else {
    job.status = "queued";
    job.queuedAt = new Date(now).toISOString();
  }
  return { job, retried: job.status === "queued" };
}

function queryJobs(state, filters) {
  const { status, rubbingId, imageId, limit } = filters;
  let rows = state.jobs.slice();
  if (status) rows = rows.filter((job) => job.status === status);
  if (rubbingId) rows = rows.filter((job) => job.rubbingId === rubbingId);
  if (imageId) rows = rows.filter((job) => job.imageId === imageId);
  rows.sort((a, b) => (a.createdAt < b.createdAt ? -1 : 1));
  if (Number.isFinite(limit) && limit > 0) rows = rows.slice(0, limit);
  return rows;
}

// 状态数量统计 + 死信清单。
function jobStats(state, filters) {
  const rows = queryJobs(state, filters);
  const counts = { queued: 0, processing: 0, completed: 0, dead_letter: 0 };
  for (const job of rows) counts[job.status] = (counts[job.status] || 0) + 1;
  return {
    total: rows.length,
    counts,
    deadLetters: rows.filter((job) => job.status === "dead_letter")
  };
}

module.exports = {
  STATUSES,
  httpError,
  registerImage,
  claimNextJob,
  completeJob,
  failJob,
  queryJobs,
  jobStats,
  getJob,
  getRubbing
};
