// 古籍拓片：缺损修补 API + 影像入库流水线。
// 纯 Node 内置模块，无第三方依赖。
const http = require("http");
const path = require("path");
const { JsonStore } = require("./src/store");
const pipeline = require("./src/pipeline");

const PORT = Number(process.env.PORT || 3020);
const DB_FILE = process.env.DB_FILE || path.join(__dirname, "data", "db.json");
// 默认租约时长与最大投递次数（认领次数，含首次）。
const DEFAULT_LEASE_MS = Number(process.env.LEASE_MS || 30_000);
const DEFAULT_MAX_ATTEMPTS = Number(process.env.MAX_ATTEMPTS || 3);

const seed = () => ({
  rubbings: [
    {
      id: "rubbing_demo",
      code: "TP-清-014",
      source: "地方碑刻残页",
      paperSize: "42x68cm",
      note: "边缘有旧折痕",
      createdAt: new Date().toISOString()
    }
  ],
  damages: [
    {
      id: "damage_demo_1",
      rubbingId: "rubbing_demo",
      position: "左上角第3列题字旁",
      type: "虫蛀孔",
      beforePhotoUrl: "https://example.local/before-014-1.jpg",
      afterPhotoUrl: "",
      status: "pending",
      repairNote: "",
      batchId: null,
      createdAt: new Date().toISOString(),
      repairedAt: null
    },
    {
      id: "damage_demo_2",
      rubbingId: "rubbing_demo",
      position: "下边缘中央",
      type: "撕裂",
      beforePhotoUrl: "https://example.local/before-014-2.jpg",
      afterPhotoUrl: "",
      status: "pending",
      repairNote: "",
      batchId: null,
      createdAt: new Date().toISOString(),
      repairedAt: null
    }
  ]
});

const routes = [
  "GET /health",
  "GET /rubbings",
  "POST /rubbings",
  "GET /rubbings/:id/damages",
  "POST /rubbings/:id/damages",
  "GET /damages?status=&type=",
  "PATCH /damages/:id",
  "GET /batches",
  "POST /batches",
  "GET /batches/:id",
  "POST /batches/:id/complete",
  // 影像入库流水线
  "POST /images",
  "GET /images?rubbingId=",
  "GET /images/:id",
  "POST /jobs/claim",
  "POST /jobs/:id/complete",
  "POST /jobs/:id/fail",
  "GET /jobs?status=&rubbingId=&imageId=",
  "GET /jobs/:id",
  "GET /jobs/stats?rubbingId=",
  "GET /results/:jobId"
];

function createApp(options = {}) {
  const clock = options.clock || { now: () => Date.now() };
  const dbFile = options.dbFile || DB_FILE;
  const store = new JsonStore(dbFile, seed(), {
    crashSentinel: options.crashSentinel,
    failPersistSentinel: options.failPersistSentinel
  });
  const config = {
    leaseMs: options.leaseMs || DEFAULT_LEASE_MS,
    maxAttempts: options.maxAttempts || DEFAULT_MAX_ATTEMPTS
  };

  function makeId(prefix) {
    return `${prefix}_${clock.now().toString(36)}_${Math.random().toString(36).slice(2, 10)}`;
  }

  function send(res, status, body) {
    res.writeHead(status, { "Content-Type": "application/json; charset=utf-8" });
    res.end(JSON.stringify(body, null, 2));
  }

  async function parseBody(req) {
    const chunks = [];
    for await (const chunk of req) chunks.push(chunk);
    const raw = Buffer.concat(chunks).toString("utf8");
    if (!raw) return {};
    try {
      return JSON.parse(raw);
    } catch {
      const error = new Error("请求体必须是合法JSON");
      error.status = 400;
      throw error;
    }
  }

  function required(body, fields) {
    const missing = fields.filter((field) => body[field] === undefined || body[field] === "");
    if (missing.length) {
      const error = new Error(`缺少字段：${missing.join(", ")}`);
      error.status = 400;
      throw error;
    }
  }

  function findRubbing(state, rubbingId) {
    const rubbing = state.rubbings.find((item) => item.id === rubbingId);
    if (!rubbing) {
      const error = new Error("拓片不存在");
      error.status = 404;
      throw error;
    }
    return rubbing;
  }

  function enrichBatch(state, batch) {
    const damages = state.damages.filter((item) => batch.damageIds.includes(item.id));
    return {
      ...batch,
      damages,
      total: damages.length,
      repaired: damages.filter((item) => item.status === "repaired").length,
      pending: damages.filter((item) => item.status !== "repaired").length
    };
  }

  // 给作业附上影像与结果视图。
  function enrichJob(state, job) {
    return {
      ...job,
      image: state.images.find((item) => item.id === job.imageId) || null,
      result: state.results.find((item) => item.jobId === job.id) || null
    };
  }

  async function handle(req, res) {
    const url = new URL(req.url, `http://${req.headers.host}`);
    const pathname = url.pathname;

    if (req.method === "GET" && pathname === "/health") {
      return send(res, 200, { ok: true, service: "rubbing-repair-api", routes });
    }

    // ---------- 拓片与缺损（原有功能） ----------

    if (req.method === "GET" && pathname === "/rubbings") {
      const data = await store.read((state) =>
        state.rubbings.map((rubbing) => {
          const damages = state.damages.filter((item) => item.rubbingId === rubbing.id);
          return {
            ...rubbing,
            damageCount: damages.length,
            pendingDamages: damages.filter((item) => item.status !== "repaired").length
          };
        })
      );
      return send(res, 200, { data });
    }

    if (req.method === "POST" && pathname === "/rubbings") {
      const body = await parseBody(req);
      required(body, ["code", "source", "paperSize"]);
      const rubbing = await store.update((state) => {
        const item = {
          id: makeId("rubbing"),
          code: body.code,
          source: body.source,
          paperSize: body.paperSize,
          note: body.note || "",
          createdAt: new Date(clock.now()).toISOString()
        };
        state.rubbings.push(item);
        return item;
      });
      return send(res, 201, { data: rubbing });
    }

    const rubbingDamagesMatch = pathname.match(/^\/rubbings\/([^/]+)\/damages$/);
    if (rubbingDamagesMatch && req.method === "GET") {
      const rubbingId = rubbingDamagesMatch[1];
      const data = await store.read((state) => {
        findRubbing(state, rubbingId);
        return state.damages.filter((item) => item.rubbingId === rubbingId);
      });
      return send(res, 200, { data });
    }

    if (rubbingDamagesMatch && req.method === "POST") {
      const rubbingId = rubbingDamagesMatch[1];
      const body = await parseBody(req);
      required(body, ["position", "type", "beforePhotoUrl"]);
      const damage = await store.update((state) => {
        findRubbing(state, rubbingId);
        const item = {
          id: makeId("damage"),
          rubbingId,
          position: body.position,
          type: body.type,
          beforePhotoUrl: body.beforePhotoUrl,
          afterPhotoUrl: "",
          status: "pending",
          repairNote: "",
          batchId: null,
          createdAt: new Date(clock.now()).toISOString(),
          repairedAt: null
        };
        state.damages.push(item);
        return item;
      });
      return send(res, 201, { data: damage });
    }

    if (req.method === "GET" && pathname === "/damages") {
      const status = url.searchParams.get("status");
      const type = url.searchParams.get("type");
      const data = await store.read((state) =>
        state.damages.filter(
          (item) => (!status || item.status === status) && (!type || item.type === type)
        )
      );
      return send(res, 200, { data });
    }

    const damagePatchMatch = pathname.match(/^\/damages\/([^/]+)$/);
    if (damagePatchMatch && req.method === "PATCH") {
      const id = damagePatchMatch[1];
      const body = await parseBody(req);
      const damage = await store.update((state) => {
        const item = state.damages.find((entry) => entry.id === id);
        if (!item) {
          const error = new Error("缺损项不存在");
          error.status = 404;
          throw error;
        }
        Object.assign(item, {
          position: body.position ?? item.position,
          type: body.type ?? item.type,
          beforePhotoUrl: body.beforePhotoUrl ?? item.beforePhotoUrl,
          afterPhotoUrl: body.afterPhotoUrl ?? item.afterPhotoUrl,
          status: body.status ?? item.status,
          repairNote: body.repairNote ?? item.repairNote
        });
        item.repairedAt =
          item.status === "repaired" ? new Date(clock.now()).toISOString() : item.repairedAt;
        return item;
      });
      return send(res, 200, { data: damage });
    }

    if (req.method === "GET" && pathname === "/batches") {
      const data = await store.read((state) => state.batches.map((batch) => enrichBatch(state, batch)));
      return send(res, 200, { data });
    }

    if (req.method === "POST" && pathname === "/batches") {
      const body = await parseBody(req);
      required(body, ["name", "damageIds"]);
      if (!Array.isArray(body.damageIds) || body.damageIds.length === 0) {
        return send(res, 400, { error: "damageIds必须是非空数组" });
      }
      const batch = await store.update((state) => {
        const invalid = body.damageIds.filter((id) => !state.damages.find((damage) => damage.id === id));
        if (invalid.length) {
          const error = new Error(`缺损项不存在：${invalid.join(", ")}`);
          error.status = 400;
          throw error;
        }
        const item = {
          id: makeId("batch"),
          name: body.name,
          status: "open",
          damageIds: body.damageIds,
          note: body.note || "",
          createdAt: new Date(clock.now()).toISOString(),
          completedAt: null
        };
        state.batches.push(item);
        state.damages.forEach((damage) => {
          if (body.damageIds.includes(damage.id)) {
            damage.batchId = item.id;
            damage.status = "in_repair";
          }
        });
        return enrichBatch(state, item);
      });
      return send(res, 201, { data: batch });
    }

    const batchMatch = pathname.match(/^\/batches\/([^/]+)$/);
    if (batchMatch && req.method === "GET") {
      const id = batchMatch[1];
      const batch = await store.read((state) => {
        const item = state.batches.find((entry) => entry.id === id);
        if (!item) {
          const error = new Error("修补批次不存在");
          error.status = 404;
          throw error;
        }
        return enrichBatch(state, item);
      });
      return send(res, 200, { data: batch });
    }

    const completeBatchMatch = pathname.match(/^\/batches\/([^/]+)\/complete$/);
    if (completeBatchMatch && req.method === "POST") {
      const id = completeBatchMatch[1];
      const body = await parseBody(req);
      const batch = await store.update((state) => {
        const item = state.batches.find((entry) => entry.id === id);
        if (!item) {
          const error = new Error("修补批次不存在");
          error.status = 404;
          throw error;
        }
        const results = Array.isArray(body.results) ? body.results : [];
        item.status = "completed";
        item.completedAt = new Date(clock.now()).toISOString();
        item.note = body.note ?? item.note;
        state.damages.forEach((damage) => {
          if (!item.damageIds.includes(damage.id)) return;
          const result = results.find((entry) => entry.damageId === damage.id) || {};
          damage.status = "repaired";
          damage.afterPhotoUrl = result.afterPhotoUrl || body.defaultAfterPhotoUrl || damage.afterPhotoUrl;
          damage.repairNote = result.repairNote || body.defaultRepairNote || damage.repairNote;
          damage.repairedAt = new Date(clock.now()).toISOString();
        });
        return enrichBatch(state, item);
      });
      return send(res, 200, { data: batch });
    }

    // ---------- 影像入库流水线 ----------

    // 登记影像：同 checksum 只留一份；登记与自动建作业在同一事务内落盘。
    if (req.method === "POST" && pathname === "/images") {
      const body = await parseBody(req);
      const outcome = await store.update((state) =>
        pipeline.registerImage(state, body, clock, {
          leaseMs: config.leaseMs,
          maxAttempts: config.maxAttempts
        })
      );
      return send(res, outcome.duplicated ? 200 : 201, {
        data: { image: outcome.image, job: outcome.job },
        duplicated: outcome.duplicated
      });
    }

    if (req.method === "GET" && pathname === "/images") {
      const rubbingId = url.searchParams.get("rubbingId");
      const data = await store.read((state) =>
        state.images
          .filter((image) => !rubbingId || image.rubbingId === rubbingId)
          .map((image) => ({
            ...image,
            job: state.jobs.find((job) => job.imageId === image.id) || null
          }))
      );
      return send(res, 200, { data });
    }

    const imageMatch = pathname.match(/^\/images\/([^/]+)$/);
    if (imageMatch && req.method === "GET") {
      const id = imageMatch[1];
      const data = await store.read((state) => {
        const image = state.images.find((item) => item.id === id);
        if (!image) {
          const error = new Error("影像不存在");
          error.status = 404;
          throw error;
        }
        return {
          ...image,
          job: state.jobs.find((job) => job.imageId === image.id) || null
        };
      });
      return send(res, 200, { data });
    }

    // 工作者认领：同一时刻一个作业只被一个工作者持有，返回租约到期时间。
    // 认领事务内会把过期且用尽投递次数的作业自动转死信。
    if (req.method === "POST" && pathname === "/jobs/claim") {
      const body = await parseBody(req);
      const outcome = await store.update((state) => pipeline.claimNextJob(state, body, clock));
      if (!outcome.job) {
        return send(res, 200, { claimed: false, reclaimed: false, deadLettered: outcome.buried, data: null });
      }
      // update 已落盘；再开一次只读事务补影像/结果视图。
      const data = await store.read((state) => enrichJob(state, outcome.job));
      return send(res, 200, {
        claimed: true,
        reclaimed: outcome.reclaimed,
        deadLettered: outcome.buried,
        data
      });
    }

    // /jobs/stats 必须放在 /jobs/:id 之前匹配，否则会被当成作业 id。
    if (req.method === "GET" && pathname === "/jobs/stats") {
      const filters = { rubbingId: url.searchParams.get("rubbingId") };
      const data = await store.read((state) => {
        const stats = pipeline.jobStats(state, filters);
        return {
          total: stats.total,
          counts: stats.counts,
          deadLetters: stats.deadLetters.map((job) => enrichJob(state, job))
        };
      });
      return send(res, 200, { data });
    }

    const jobMatch = pathname.match(/^\/jobs\/([^/]+)$/);
    if (jobMatch && req.method === "GET") {
      const id = jobMatch[1];
      const data = await store.read((state) => enrichJob(state, pipeline.getJob(state, id)));
      return send(res, 200, { data });
    }

    const jobCompleteMatch = pathname.match(/^\/jobs\/([^/]+)\/complete$/);
    if (jobCompleteMatch && req.method === "POST") {
      const id = jobCompleteMatch[1];
      const body = await parseBody(req);
      const { job, result } = await store.update((state) => pipeline.completeJob(state, id, body, clock));
      const data = await store.read((state) => enrichJob(state, job));
      return send(res, 200, { data: { ...data, result } });
    }

    const jobFailMatch = pathname.match(/^\/jobs\/([^/]+)\/fail$/);
    if (jobFailMatch && req.method === "POST") {
      const id = jobFailMatch[1];
      const body = await parseBody(req);
      const { job, retried } = await store.update((state) => pipeline.failJob(state, id, body, clock));
      const data = await store.read((state) => enrichJob(state, job));
      return send(res, 200, { data, retried });
    }

    if (req.method === "GET" && pathname === "/jobs") {
      const filters = {
        status: url.searchParams.get("status"),
        rubbingId: url.searchParams.get("rubbingId"),
        imageId: url.searchParams.get("imageId"),
        limit: Number(url.searchParams.get("limit"))
      };
      if (filters.status && !pipeline.STATUSES.includes(filters.status)) {
        return send(res, 400, { error: `status 只能是：${pipeline.STATUSES.join(", ")}` });
      }
      const data = await store.read((state) =>
        pipeline.queryJobs(state, filters).map((job) => enrichJob(state, job))
      );
      return send(res, 200, { data });
    }

    const resultMatch = pathname.match(/^\/results\/([^/]+)$/);
    if (resultMatch && req.method === "GET") {
      const jobId = resultMatch[1];
      const data = await store.read((state) => {
        pipeline.getJob(state, jobId);
        const result = state.results.find((item) => item.jobId === jobId);
        return result || null;
      });
      return send(res, 200, { data });
    }

    return send(res, 404, { error: "接口不存在", routes });
  }

  const server = http.createServer((req, res) => {
    handle(req, res).catch((error) =>
      send(res, error.status || 500, { error: error.message || "服务器错误" })
    );
  });

  return { server, store };
}

function start() {
  const { server } = createApp();
  server.listen(PORT, () => {
    const address = server.address();
    const port = typeof address === "object" && address ? address.port : PORT;
    console.log(`Rubbing repair API running at http://127.0.0.1:${port}`);
  });
}

module.exports = { createApp, start };

if (require.main === module) start();
