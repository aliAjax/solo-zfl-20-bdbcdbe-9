// JSON 存储：跨进程互斥 + 事务内重读 + 原子落盘 + 失败回滚。
//
// 为什么不能只靠内存：
//   多个服务进程可以共用同一个数据文件（NFS/同一台机器上开两个 node server.js）。
//   原来每个进程各持一份内存状态，两个工作者同时认领同一个作业都会成功，
//   最后落盘只留下后写的那一份。因此这里在文件系统层加互斥：
//
// 锁：
//   <data dir>/db.lock/ 目录（mkdir 在 POSIX/Windows 上对同一路径都是原子的“创建即占用”）。
//   目录内 owner.json 经“写临时文件 + rename”原子落地，内容为 { pid, startedAt, nonce }。
//   抢锁失败则重试；只有读到 owner 且确认持锁进程已死（kill(pid,0) 探测）或持锁超时
//   （僵死，如上次 SIGKILL）才强抢，强抢前校验 nonce 防止误删别人刚拿到的新锁。
//   owner.json 尚未出现的极短窗口内绝不强抢（避免与建锁中的进程双持），
//   仅当锁目录本身的年龄也超过僵死阈值（建锁进程当场被杀）才回收。
//
// 事务（多进程也安全）：
//   1) 加锁；2) 从磁盘重新读取最新内容（不相信进程内缓存）；
//   3) 执行回调改内存；4) 临时文件 + fsync + rename 原子落盘；
//   5) 成功后把这份磁盘内容保留为缓存；失败则把内存回滚到第 2 步的快照；
//   6) 释放锁。
//
// 崩溃注入（仅测试用）：
//   crashSentinel 文件存在 => 改完内存、落盘前 SIGKILL；
//   failPersistSentinel 文件存在 => 落盘在 rename 前抛错（模拟磁盘故障），
//   用来验证内存回滚。
const { readFile, writeFile, rename, open, readdir, rm, mkdir, stat } = require("fs/promises");
const fs = require("fs");
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

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

class JsonStore {
  constructor(file, seed, options = {}) {
    this.file = file;
    this.dir = path.dirname(file);
    this.lockDir = path.join(this.dir, `${path.basename(file)}.lock`);
    this.ownerFile = path.join(this.lockDir, "owner.json");
    this.seed = seed || {};
    this.state = null;
    // 同进程内的写事务串行链（跨进程安全由文件锁保证）。
    this.chain = Promise.resolve();
    this.nonce = crypto.randomBytes(8).toString("hex");
    this.lockRetryMs = options.lockRetryMs || Number(process.env.LOCK_RETRY_MS) || 20;
    // 持锁超过此时长视为僵死（正常事务只是一次 JSON 读写，远小于这个值）。
    this.lockStaleMs = options.lockStaleMs || Number(process.env.LOCK_STALE_MS) || 30_000;
    // 等锁上限，避免故障时无限挂起。
    this.lockTimeoutMs = options.lockTimeoutMs || Number(process.env.LOCK_TIMEOUT_MS) || 15_000;
    this.crashSentinel = options.crashSentinel || process.env.CRASH_SENTINEL || "";
    this.failPersistSentinel =
      options.failPersistSentinel || process.env.FAIL_PERSIST_SENTINEL || "";
  }

  #defaultState() {
    const base = { ...DEFAULT_COLLECTIONS, ...this.seed };
    return JSON.parse(JSON.stringify(base));
  }

  // 从磁盘加载（必须持锁）；文件不存在则用种子初始化并落盘。
  async #loadFromDisk() {
    let raw;
    try {
      raw = await readFile(this.file, "utf8");
    } catch (error) {
      if (error.code !== "ENOENT") throw error;
      this.state = this.#defaultState();
      await this.#writeAtomically();
      return;
    }
    const parsed = JSON.parse(raw);
    // 老版本数据缺少集合时就地补齐（在事务里，补齐随后会随事务一起落盘）。
    for (const [key, value] of Object.entries(DEFAULT_COLLECTIONS)) {
      if (!Array.isArray(parsed[key])) parsed[key] = [...value];
    }
    this.state = parsed;
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
        .map((name) => rm(path.join(this.dir, name), { force: true }).catch(() => {}))
    );
  }

  async init() {
    await this.withLock(() => this.#loadFromDisk());
  }

  // ---------- 跨进程锁 ----------

  async #tryAcquireOnce() {
    await mkdir(this.lockDir, { recursive: false });
    // owner 经 tmp+rename 原子发布：别的进程要么看不到 owner（等待），要么看到完整内容。
    const owner = { pid: process.pid, startedAt: Date.now(), nonce: this.nonce };
    const ownerTmp = path.join(this.lockDir, `owner.${this.nonce}.tmp`);
    await writeFile(ownerTmp, JSON.stringify(owner));
    await rename(ownerTmp, this.ownerFile);
    return true;
  }

  async #readOwner() {
    try {
      return JSON.parse(await readFile(this.ownerFile, "utf8"));
    } catch (error) {
      if (error.code === "ENOENT") return null;
      // owner 正在 rename 的瞬间读取，极少数平台会报别的错，一律当作“还不可见”。
      return null;
    }
  }

  // 判活：pid 存在且进程在；owner 文件损坏时退回“持锁时长”判据。
  static #pidAlive(pid) {
    if (!Number.isInteger(pid) || pid <= 0) return false;
    try {
      process.kill(pid, 0);
      return true;
    } catch (error) {
      // EPERM 说明进程在但无权发信号，也算存活。
      return error.code === "EPERM";
    }
  }

  async #isStale(owner) {
    if (owner) {
      const ageMs = Date.now() - Number(owner.startedAt || 0);
      if (ageMs > this.lockStaleMs) return true;
      if (!JsonStore.#pidAlive(owner.pid)) return true;
      return false;
    }
    // owner 还不可见：不能立刻抢（建锁者可能正活着）。
    // 只有锁目录本身也老得离谱（建锁进程在发布 owner 前被杀）才回收。
    try {
      const info = await stat(this.lockDir);
      return Date.now() - info.mtimeMs > this.lockStaleMs;
    } catch {
      return true; // 目录已不存在，自然可重新占用。
    }
  }

  // 强抢僵死锁：先确认 owner 僵死，再把锁目录原子改名到唯一“墓地”。
  // rename 在同一文件系统上是原子的：多个等待者里恰好一个成功，
  // 其余拿到 ENOENT，避免“甲刚重建锁、乙把甲的新锁删掉”这种双持窗口。
  async #stealStaleLock() {
    const owner = await this.#readOwner();
    if (!(await this.#isStale(owner))) return false;
    const graveyard = path.join(
      this.dir,
      `${path.basename(this.lockDir)}.dead.${crypto.randomBytes(6).toString("hex")}`
    );
    try {
      await rename(this.lockDir, graveyard);
    } catch (error) {
      if (error.code === "ENOENT") return false; // 别的回收者已抢先。
      throw error;
    }
    try {
      // 旧锁已被本进程独占移走，现在重新占用正式锁路径。
      await this.#tryAcquireOnce();
    } finally {
      await rm(graveyard, { recursive: true, force: true }).catch(() => {});
    }
    return true;
  }

  async #acquire() {
    const deadline = Date.now() + this.lockTimeoutMs;
    // 第一次清理上次崩溃可能留下的 tmp。
    let cleaned = false;
    for (;;) {
      try {
        await this.#tryAcquireOnce();
        if (!cleaned) {
          await this.#cleanTempFiles();
          cleaned = true;
        }
        return;
      } catch (error) {
        if (error.code !== "EEXIST") throw error;
      }
      const owner = await this.#readOwner();
      if (await this.#isStale(owner)) {
        if (await this.#stealStaleLock()) {
          if (!cleaned) {
            await this.#cleanTempFiles();
            cleaned = true;
          }
          return;
        }
      }
      if (Date.now() >= deadline) {
        throw Object.assign(new Error("获取数据文件锁超时"), { code: "LOCK_TIMEOUT" });
      }
      await sleep(this.lockRetryMs);
    }
  }

  async #release() {
    // 只删自己的锁：读回 owner 对 nonce，对不上说明锁已不属于本进程，绝不能动。
    const owner = await this.#readOwner();
    if (owner && owner.nonce !== this.nonce) return;
    await rm(this.lockDir, { recursive: true, force: true });
  }

  // 在跨进程锁内执行：进入即重读磁盘，保证多进程下看到的总是已提交的最新状态。
  async withLock(fn) {
    await mkdir(this.dir, { recursive: true });
    await this.#acquire();
    try {
      await this.#loadFromDisk();
      return await fn();
    } finally {
      await this.#release();
    }
  }

  // 只读事务：同样加锁并重读，避免读到本进程缓存里的陈旧内容。
  async read(fn) {
    return this.withLock(() => fn(this.state));
  }

  // 写事务：先快照，回调改内存，原子落盘；任一步失败都把内存回滚到磁盘上的版本。
  async update(mutator) {
    const run = this.chain.then(() =>
      this.withLock(async () => {
        const snapshot = JSON.stringify(this.state);
        let result;
        try {
          result = mutator(this.state);
          if (this.crashSentinel && fs.existsSync(this.crashSentinel)) {
            // 模拟落盘前断电 / kill -9：进程直接死亡，磁盘保持旧版本。
            process.kill(process.pid, "SIGKILL");
            while (true) {} // 极端情况下等待死亡，绝不能继续落盘。
          }
          if (this.failPersistSentinel && fs.existsSync(this.failPersistSentinel)) {
            const error = new Error("模拟的落盘失败（rename 前）");
            error.code = "SIMULATED_WRITE_FAILURE";
            throw error;
          }
          await this.#writeAtomically();
        } catch (error) {
          // 回滚内存到事务前的磁盘版本，随后读接口不会读到未落盘的变更。
          this.state = JSON.parse(snapshot);
          throw error;
        }
        return result;
      })
    );
    // 一次事务成败都不影响后续事务入队。
    this.chain = run.then(
      () => undefined,
      () => undefined
    );
    return run;
  }

  // 临时文件 + fsync + rename：要么完整新文件，要么完整旧文件。
  async #writeAtomically() {
    const tmp = `${this.file}.${process.pid}.${crypto.randomBytes(6).toString("hex")}.tmp`;
    let handle;
    try {
      handle = await open(tmp, "wx");
      await handle.writeFile(JSON.stringify(this.state, null, 2), "utf8");
      await handle.sync();
      await handle.close();
      handle = null;
      await rename(tmp, this.file);
    } finally {
      if (handle) await handle.close().catch(() => {});
      await rm(tmp, { force: true }).catch(() => {});
    }
  }
}

module.exports = { JsonStore, DEFAULT_COLLECTIONS };
