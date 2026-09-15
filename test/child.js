// 启动真实子进程跑 server.js，供多进程互斥、SIGKILL 崩溃恢复测试使用。
const { spawn } = require("child_process");
const path = require("path");

const SERVER = path.join(__dirname, "..", "server.js");

function startChild(dir, { crash = false, failPersist = false, leaseMs = 60_000, maxAttempts = 3 } = {}) {
  const env = {
    ...process.env,
    PORT: "0",
    DB_FILE: path.join(dir, "db.json"),
    LEASE_MS: String(leaseMs),
    MAX_ATTEMPTS: String(maxAttempts)
  };
  if (crash) env.CRASH_SENTINEL = path.join(dir, "crash.flag");
  if (failPersist) env.FAIL_PERSIST_SENTINEL = path.join(dir, "fail.flag");

  const child = spawn(process.execPath, [SERVER], { env });
  let base;
  child.stdout.setEncoding("utf8");
  child.stdout.on("data", (chunk) => {
    const match = chunk.match(/http:\/\/127\.0\.0\.1:(\d+)/);
    if (match) base = `http://127.0.0.1:${match[1]}`;
  });
  child.stderr.pipe(process.stderr);

  async function call(method, urlPath, body) {
    if (!base) throw new Error("子进程尚未监听");
    const init = { method };
    if (body !== undefined) {
      init.headers = { "Content-Type": "application/json" };
      init.body = JSON.stringify(body);
    }
    const res = await fetch(base + urlPath, init);
    return { status: res.status, body: await res.json() };
  }

  const ready = (async () => {
    for (let i = 0; i < 100 && !base; i++) await new Promise((r) => setTimeout(r, 30));
    if (!base) throw new Error("子进程未在预期时间内启动");
    for (let i = 0; i < 50; i++) {
      try {
        const res = await fetch(`${base}/health`);
        if (res.ok) return;
      } catch {}
      await new Promise((r) => setTimeout(r, 30));
    }
    throw new Error("子进程健康检查失败");
  })();

  return {
    child,
    ready,
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

module.exports = { startChild, onExit };
