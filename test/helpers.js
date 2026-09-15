// 测试辅助：起一个真实 HTTP 服务（PORT=0 自动端口），用独立临时数据目录。
const { mkdtemp, rm } = require("fs/promises");
const os = require("os");
const path = require("path");
const { createApp } = require("../server");

async function startServer(options = {}) {
  const dir = await mkdtemp(path.join(os.tmpdir(), "rubbing-test-"));
  const dbFile = path.join(dir, "db.json");
  const { server, store } = createApp({
    dbFile,
    leaseMs: options.leaseMs,
    maxAttempts: options.maxAttempts,
    clock: options.clock,
    crashSentinel: options.crashSentinel,
    failPersistSentinel: options.failPersistSentinel
  });
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  const port = server.address().port;
  const base = `http://127.0.0.1:${port}`;

  async function call(method, urlPath, body, expectedStatus) {
    const init = { method };
    if (body !== undefined) {
      init.headers = { "Content-Type": "application/json" };
      init.body = JSON.stringify(body);
    }
    const res = await fetch(base + urlPath, init);
    const json = await res.json();
    if (expectedStatus !== undefined && res.status !== expectedStatus) {
      throw new Error(`${method} ${urlPath} 期望 ${expectedStatus}，实际 ${res.status}：${JSON.stringify(json)}`);
    }
    return { status: res.status, body: json };
  }

  return {
    base,
    dbFile,
    dir,
    store,
    call,
    close: async () => {
      await new Promise((resolve) => server.close(resolve));
      await rm(dir, { recursive: true, force: true });
    }
  };
}

// 登记一张影像并返回其作业。
async function registerImage(api, overrides = {}) {
  const res = await api.call("POST", "/images", {
    rubbingId: "rubbing_demo",
    fileName: "IMG0001.tif",
    checksum: `sha256:${Math.random().toString(36).slice(2)}`,
    ...overrides
  }, 201);
  return res.body.data;
}

module.exports = { startServer, registerImage };
