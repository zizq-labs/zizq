// A `zizq` fixture giving each test its own server: the binary named by
// ZIZQ_BINARY, on free ports, with a temporary root directory. Tests can
// stop and start it again, and add jobs through its primary API.

import { test as base, expect } from "@playwright/test";
import { spawn } from "node:child_process";
import { mkdtemp, rm } from "node:fs/promises";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";

const BINARY = process.env.ZIZQ_BINARY;

function freePort() {
  return new Promise((resolve, reject) => {
    const server = createServer();
    server.on("error", reject);
    server.listen(0, "127.0.0.1", () => {
      const { port } = server.address();
      server.close(() => resolve(port));
    });
  });
}

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

class Zizq {
  constructor(root, port, adminPort) {
    this.root = root;
    this.port = port;
    this.adminPort = adminPort;
    this.log = "";
    this.process = null;
    this.takes = [];
  }

  // The web UI, on the admin listener.
  get url() {
    return `http://127.0.0.1:${this.adminPort}/`;
  }

  get api() {
    return `http://127.0.0.1:${this.port}`;
  }

  async start() {
    this.process = spawn(
      BINARY,
      [
        "serve",
        "--root-dir", this.root,
        "--port", String(this.port),
        "--admin-port", String(this.adminPort),
      ],
      { stdio: ["ignore", "pipe", "pipe"] },
    );
    const record = (chunk) => (this.log += chunk);
    this.process.stdout.on("data", record);
    this.process.stderr.on("data", record);

    for (let i = 0; i < 100; i++) {
      try {
        if ((await fetch(`${this.api}/jobs/count`)).ok) {
          return;
        }
      } catch {
        // Not listening yet.
      }
      await sleep(100);
    }
    throw new Error(`zizq did not start:\n${this.log}`);
  }

  async stop() {
    for (const take of this.takes.splice(0)) {
      take.abort();
    }
    const process = this.process;
    if (!process || process.exitCode !== null) {
      return;
    }
    const exited = new Promise((resolve) => process.once("exit", resolve));
    process.kill("SIGTERM");
    await exited;
  }

  async post(path, body) {
    const res = await fetch(`${this.api}${path}`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
    });
    if (!res.ok) {
      throw new Error(`POST ${path}: ${res.status} ${await res.text()}`);
    }
  }

  // Enqueues `jobs`, each `{ type, queue, ... }`, in batches.
  async enqueue(jobs) {
    for (let i = 0; i < jobs.length; i += 1000) {
      await this.post("/jobs/bulk", {
        jobs: jobs.slice(i, i + 1000).map((job) => ({ payload: {}, ...job })),
      });
    }
  }

  // Takes `n` jobs from `queue` and holds them in flight until the server
  // stops.
  async take(queue, n) {
    const controller = new AbortController();
    this.takes.push(controller);
    const res = await fetch(`${this.api}/jobs/take?queue=${queue}&prefetch=${n}`, {
      headers: { accept: "application/x-ndjson" },
      signal: controller.signal,
    });
    const reader = res.body.pipeThrough(new TextDecoderStream()).getReader();
    let taken = 0;
    let buffer = "";
    while (taken < n) {
      const { value, done } = await reader.read();
      if (done) {
        break;
      }
      buffer += value;
      const lines = buffer.split("\n");
      buffer = lines.pop();
      taken += lines.filter((line) => line.trim()).length;
    }
  }
}

export const test = base.extend({
  zizq: async ({}, use, testInfo) => {
    if (!BINARY) {
      throw new Error("ZIZQ_BINARY is not set; run the tests with run.sh");
    }
    const root = await mkdtemp(join(tmpdir(), "zizq-web-"));
    const zizq = new Zizq(root, await freePort(), await freePort());
    await zizq.start();
    try {
      await use(zizq);
    } finally {
      await zizq.stop();
      if (testInfo.status !== testInfo.expectedStatus) {
        await testInfo.attach("zizq.log", { body: zizq.log, contentType: "text/plain" });
      }
      await rm(root, { recursive: true, force: true });
    }
  },
});

export { expect };
