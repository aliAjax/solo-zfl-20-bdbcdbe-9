// 原子 JSON 存储：单进程内串行事务 + 临时文件 rename 落盘。
// 目的：
// 1. update(fn) 串行执行，HTTP 请求之间不会出现交错写；
// 2. 落盘走 write tmp -> fsync -> rename，rename 在同一文件系统上是原子的，
//    进程在写入途中被 kill，磁盘上要么是旧文件要么是新文件，不会出现半截 JSON；
// 3. 状态同时写入内存与磁盘（先落盘后返回），重启后未完成作业不丢、已完成不重跑。
const { readFile, writeFile, rename, open, readdir } = require("fs/promises");
const { mkdir } = require("fs/promises");
const path = require("path");
const crypto = require("crypto");

const DEFAULT_COLLECTIONS = {
  rubbings: [],
  damages: [],
  batches: [],
  // 影像入库流水线
  images: [],
  jobs: [],
  results: []
};

class JsonStore {
  constructor(file, seed, options = {}) {
    this.file = file;
    this.dir = path.dirname(file);
    this.seed = seed || {};
    this.state = null;
    this.chain = Promise.resolve();
    // 崩溃注入：配置文件存在时，下次事务改完内存状态、落盘前立即退出。
    // 用于自动化测试验证“结果与状态一起生效，被打断不留半成品”。
    this.crashSentinel = options.crashSentinel || process.env.CRASH_SENTINEL || "";
  }

  // 首次访问时加载；若上次崩溃留下 tmp 残留，直接清理（它们从未被 rename，不影响正式文件）。
  async init() {
    if (this.state) return;
    await mkdir(this.dir, { recursive: true });
    try {
      const raw = await readFile(this.file, "utf8");
      this.state = JSON.parse(raw);
    } catch (error) {
      if (error.code !== "ENOENT") throw error;
      // 文件不存在：用种子数据初始化并立刻落盘。
      this.state = this.#cloneSeed();
      await this.#persist();
    }
    // 老版本数据缺少流水线集合时就地补齐。
    for (const [key, value] of Object.entries(DEFAULT_COLLECTIONS)) {
      if (!Array.isArray(this.state[key])) this.state[key] = [...value];
    }
    await this.#cleanTempFiles();
  }

  #cloneSeed() {
    const base = { ...DEFAULT_COLLECTIONS, ...this.seed };
    return JSON.parse(JSON.stringify(base));
  }

  async #cleanTempFiles() {
    const base = path.basename(this.file);
    let entries = [];
    try {
      entries = await readdir(this.dir);
    } catch {
      return;
    }
    await Promise.all(
      entries
        .filter((name) => name.startsWith(`${base}.`) && name.endsWith(".tmp"))
        .map((name) =>
          // 清理失败不致命：残留 tmp 不会被读取。
          rmQuiet(path.join(this.dir, name))
        )
    );
  }

  // 只读快照：同步取值（state 只在 init 与 update 的同步回调中变化）。
  async read(fn) {
    await this.init();
    return fn(this.state);
  }

  // 串行化的读写事务：mutator 必须同步修改 state 并返回结果；
  // 落盘完成（成功或失败）后才把结果交还给调用方。
  async update(mutator) {
    const run = this.chain.then(async () => {
      await this.init();
      const result = mutator(this.state);
      if (this.crashSentinel) {
        // 同步探测哨兵文件：存在则在落盘前直接退出，模拟事务中途断电/kill -9。
        const { existsSync } = require("fs");
        if (existsSync(this.crashSentinel)) {
          process.kill(process.pid, "SIGKILL");
          // SIGKILL 在极少数调度下未立即生效时，阻塞等待死亡。
          // eslint-disable-next-line no-constant-condition
          while (true) {}
        }
      }
      await this.#persist();
      return result;
    });
    // 无论本次事务成败，串行链都继续向下，避免一次失败锁死全部写请求。
    this.chain = run.then(
      () => undefined,
      () => undefined
    );
    return run;
  }

  async #persist() {
    const tmp = `${this.file}.${process.pid}.${crypto.randomBytes(6).toString("hex")}.tmp`;
    let handle;
    try {
      handle = await open(tmp, "wx");
      await handle.writeFile(JSON.stringify(this.state, null, 2), "utf8");
      // flush 用户态缓冲与文件内容到磁盘，再原子改名覆盖正式文件。
      await handle.sync();
      await handle.close();
      handle = null;
      await rename(tmp, this.file);
    } finally {
      if (handle) {
        await handle.close().catch(() => {});
      }
      await rmQuiet(tmp);
    }
  }
}

async function rmQuiet(target) {
  const { rm } = require("fs/promises");
  await rm(target, { force: true }).catch(() => {});
}

module.exports = { JsonStore, DEFAULT_COLLECTIONS };
