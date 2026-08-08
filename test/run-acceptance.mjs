#!/usr/bin/env node
import assert from "node:assert/strict";
import { mkdtemp, mkdir, readFile, rm, writeFile, chmod } from "node:fs/promises";
import { rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";
import net from "node:net";
import { setTimeout as delay } from "node:timers/promises";
import { brokerEndpoint } from "../dist/index.js";
import { lockDir } from "../dist/internal/launcher.js";

const root = resolve(fileURLToPath(new URL("..", import.meta.url)));
const harness = join(root, "test/harness/domain-client.mjs");
const testRuntime = await mkdtemp(join(tmpdir(), "pi-process-domain-runtime-"));
process.env.XDG_RUNTIME_DIR = testRuntime;
process.on("exit", () => rmSync(testRuntime, { recursive: true, force: true }));
const baseEnv = { ...process.env };
for (const key of ["PI_PROCESS_DOMAIN_ID", "PI_PROCESS_DOMAIN_KEY", "PI_PROCESS_DOMAIN_PROTOCOL", "PI_PROCESS_DOMAIN_RESERVATION"]) delete baseEnv[key];
const deadlineMs = 10_000;
let passed = 0;

function run(command, args = [], options = {}) {
  return new Promise((resolvePromise, reject) => {
    const child = spawn(command, args, { env: options.env ?? baseEnv, cwd: options.cwd ?? root, stdio: ["ignore", "pipe", "pipe"] });
    let stdout = "";
    let stderr = "";
    child.stdout.on("data", (chunk) => { stdout += chunk; });
    child.stderr.on("data", (chunk) => { stderr += chunk; });
    const timer = setTimeout(() => {
      child.kill("SIGKILL");
      reject(new Error(`timeout: ${command} ${args.join(" ")}\n${stdout}\n${stderr}`));
    }, options.timeout ?? deadlineMs);
    child.on("error", reject);
    child.on("exit", (code, signal) => {
      clearTimeout(timer);
      resolvePromise({ code, signal, stdout, stderr, child });
    });
  });
}

function start(args = [], options = {}) {
  const child = spawn(process.execPath, [harness, ...args], { env: options.env ?? baseEnv, cwd: root, stdio: ["ignore", "pipe", "pipe"] });
  let stdout = "";
  let stderr = "";
  let readySettled = false;
  child.stdout.on("data", (chunk) => { stdout += chunk; });
  child.stderr.on("data", (chunk) => { stderr += chunk; });
  const ready = new Promise((resolveReady, reject) => {
    const timer = setTimeout(() => reject(new Error(`child readiness timeout\n${stdout}\n${stderr}`)), options.timeout ?? deadlineMs);
    child.stdout.on("data", () => {
      if (readySettled) return;
      const line = stdout.split("\n").find(Boolean);
      if (line) {
        readySettled = true;
        clearTimeout(timer);
        resolveReady(JSON.parse(line));
      }
    });
    child.once("exit", (code) => {
      clearTimeout(timer);
      reject(new Error(`child exited before ready (${code})\n${stdout}\n${stderr}`));
    });
  });
  return { child, ready, output: () => ({ stdout, stderr }) };
}

function parseLast(result) {
  const lines = result.stdout.trim().split("\n").filter(Boolean);
  return JSON.parse(lines.at(-1));
}

function waitForExit(started, timeout = deadlineMs) {
  return new Promise((resolveExit, reject) => {
    const timer = setTimeout(() => {
      started.child.kill("SIGKILL");
      const { stdout, stderr } = started.output();
      reject(new Error(`child exit timeout\n${stdout}\n${stderr}`));
    }, timeout);
    started.child.once("exit", (code, signal) => {
      clearTimeout(timer);
      const { stdout, stderr } = started.output();
      resolveExit({ code, signal, stdout, stderr });
    });
  });
}

async function terminate(started) {
  if (started.child.exitCode !== null) return;
  started.child.kill("SIGTERM");
  await waitForExit(started).catch(() => {});
}

async function scenario(name, fn) {
  await fn();
  passed += 1;
  console.log(`ok ${passed} - ${name}`);
}

function brokerClaimPath() {
  return join(lockDir(), "election.lock", "claim.json");
}

async function readBrokerPid() {
  const until = Date.now() + deadlineMs;
  while (Date.now() < until) {
    try {
      const claim = JSON.parse(await readFile(brokerClaimPath(), "utf8"));
      if (Number.isInteger(claim.pid) && claim.pid > 0) return claim.pid;
    }
    catch { /* broker has not claimed ownership yet */ }
    await delay(50);
  }
  throw new Error("broker ownership claim did not become ready");
}

async function killBroker() {
  let pid = null;
  try {
    pid = await readBrokerPid();
    process.kill(pid, "SIGKILL");
  }
  catch { /* already gone */ }
  if (pid !== null) {
    const until = Date.now() + deadlineMs;
    while (Date.now() < until) {
      try {
        process.kill(pid, 0);
        await delay(25);
      }
      catch {
        break;
      }
    }
  }
  await delay(250);
}

function declarationFromReservation(env) {
  return {
    ...baseEnv,
    PI_PROCESS_DOMAIN_ID: env.PI_PROCESS_DOMAIN_ID,
    PI_PROCESS_DOMAIN_KEY: env.PI_PROCESS_DOMAIN_KEY,
    PI_PROCESS_DOMAIN_PROTOCOL: env.PI_PROCESS_DOMAIN_PROTOCOL,
  };
}

await scenario("new domain and declared join across processes", async () => {
  await killBroker();
  const reservation = await run(process.execPath, [harness, "reservation"], { env: baseEnv });
  assert.equal(reservation.code, 0);
  const reservationData = parseLast(reservation);
  const joined = await run(process.execPath, [harness, "snapshot"], { env: declarationFromReservation(reservationData.env) });
  assert.equal(joined.code, 0, joined.stderr);
  assert.equal(parseLast(joined).created, false);
});

await scenario("missing malformed and wrong declared keys fail nonzero", async () => {
  const partial = await run(process.execPath, [harness], { env: { ...baseEnv, PI_PROCESS_DOMAIN_ID: "valid-domain-id" } });
  assert.notEqual(partial.code, 0);
  const caught = await run(process.execPath, [harness, "caught"], { env: { ...baseEnv, PI_PROCESS_DOMAIN_ID: "valid-domain-id" } });
  assert.equal(caught.code, 78, caught.stderr);
  const caughtData = parseLast(caught);
  assert.equal(caughtData.code, "INVALID_DECLARATION");
  assert.equal(caughtData.exitCode, 78);
  const overridden = await run(process.execPath, [harness, "caught"], {
    env: { ...baseEnv, PI_PROCESS_DOMAIN_ID: "valid-domain-id", TEST_FATAL_OVERRIDE: "1" },
  });
  assert.equal(overridden.code, 78, overridden.stderr);
  const overriddenData = parseLast(overridden);
  assert.equal(overriddenData.code, "INVALID_DECLARATION");
  assert.equal(overriddenData.overriddenFatal, "INVALID_DECLARATION");
  const malformed = await run(process.execPath, [harness], { env: { ...baseEnv, PI_PROCESS_DOMAIN_ID: "valid-domain-id", PI_PROCESS_DOMAIN_KEY: "!!!", PI_PROCESS_DOMAIN_PROTOCOL: "1.0" } });
  assert.notEqual(malformed.code, 0);
  const minor = await run(process.execPath, [harness], {
    env: {
      ...baseEnv,
      PI_PROCESS_DOMAIN_ID: "valid-domain-id",
      PI_PROCESS_DOMAIN_KEY: Buffer.alloc(32, 1).toString("base64url"),
      PI_PROCESS_DOMAIN_PROTOCOL: "1.999",
    },
  });
  assert.equal(minor.code, 78, minor.stderr);
  assert.equal(parseLast(minor).code, "PROTOCOL_MISMATCH");

  const reservationResult = await run(process.execPath, [harness, "reservation"], { env: baseEnv });
  assert.equal(reservationResult.code, 0, reservationResult.stderr);
  const reservation = parseLast(reservationResult);
  const wrong = await run(process.execPath, [harness], { env: { ...declarationFromReservation(reservation.env), PI_PROCESS_DOMAIN_KEY: "A".repeat(43) } });
  assert.notEqual(wrong.code, 0);
});

await scenario("busy idle snapshots, subscription, and fence invalidation", async () => {
  const result = await run(process.execPath, [harness, "transitions"], { env: baseEnv });
  assert.equal(result.code, 0, result.stderr);
  const data = parseLast(result);
  assert.equal(data.busy.allIdle, false);
  assert.equal(data.idle.allIdle, true);
  assert.equal(data.confirmed, true);
  assert.equal(data.oldFenceConfirmed, false);
  assert.ok(data.observed.length >= 2);
  assert.notEqual(data.busy.activityGeneration, data.idle.activityGeneration);
});

await scenario("reservation adopt cancel replay and expiry fail closed", async () => {
  const made = parseLast(await run(process.execPath, [harness, "reservation"], { env: { ...baseEnv, TEST_TTL_MS: "1200" } }));
  assert.equal(made.snapshot.pendingSpawns, 1);
  const adopted = await run(process.execPath, [harness], { env: { ...baseEnv, ...made.env } });
  assert.equal(adopted.code, 0, adopted.stderr);
  const replay = await run(process.execPath, [harness], { env: { ...baseEnv, ...made.env } });
  assert.notEqual(replay.code, 0);

  const canceled = parseLast(await run(process.execPath, [harness, "reservation"], { env: { ...baseEnv, TEST_CANCEL: "1" } }));
  const canceledJoin = await run(process.execPath, [harness], { env: { ...baseEnv, ...canceled.env } });
  assert.notEqual(canceledJoin.code, 0);

  const expiring = parseLast(await run(process.execPath, [harness, "reservation"], { env: { ...baseEnv, TEST_TTL_MS: "1000" } }));
  await delay(1200);
  const expiredJoin = await run(process.execPath, [harness], { env: { ...baseEnv, ...expiring.env } });
  assert.notEqual(expiredJoin.code, 0);
});

await scenario("rejected child reservation does not disturb the holding parent", async () => {
  const made = parseLast(await run(process.execPath, [harness, "reservation"], { env: baseEnv }));
  const parentEnv = declarationFromReservation(made.env);
  const parent = start(["hold"], { env: parentEnv });
  await parent.ready;
  try {
    const adopted = await run(process.execPath, [harness], { env: { ...baseEnv, ...made.env } });
    assert.equal(adopted.code, 0, adopted.stderr);
    const replay = await run(process.execPath, [harness], { env: { ...baseEnv, ...made.env } });
    assert.notEqual(replay.code, 0, `replayed claim unexpectedly joined\n${replay.stdout}\n${replay.stderr}`);
    assert.equal(parent.child.exitCode, null, "replayed child claim must not terminate the holding parent");
    const observer = await run(process.execPath, [harness], { env: parentEnv });
    assert.equal(observer.code, 0, observer.stderr);
  }
  finally {
    await terminate(parent);
  }
});

await scenario("paused established client recovers after broker lease expiry", async () => {
  await killBroker();
  const client = start(["recover"], { env: baseEnv, timeout: 15_000 });
  await client.ready;
  process.kill(client.child.pid, "SIGSTOP");
  await delay(12_500);
  process.kill(client.child.pid, "SIGCONT");
  const result = await waitForExit(client, 25_000);
  assert.equal(result.code, 0, result.stderr);
  const recovered = parseLast(result);
  assert.equal(recovered.recovered, true, result.stdout);
  assert.equal(recovered.idle.certain, true);
  assert.equal(recovered.idle.allIdle, true);
});

await scenario("child crash makes the domain uncertain", async () => {
  const made = parseLast(await run(process.execPath, [harness, "reservation"], { env: baseEnv }));
  const holder = start(["hold"], { env: declarationFromReservation(made.env) });
  await holder.ready;
  holder.child.kill("SIGKILL");
  await new Promise((r) => holder.child.once("exit", r));
  const observer = await run(process.execPath, [harness], { env: declarationFromReservation(made.env) });
  assert.equal(observer.code, 0, observer.stderr);
  assert.equal(parseLast(observer).snapshot.allIdle, false);
});

await scenario("broker restart creates a new epoch and fails closed during recovery", async () => {
  const madeResult = await run(process.execPath, [harness, "reservation"], { env: baseEnv });
  assert.equal(madeResult.code, 0, madeResult.stderr);
  const made = parseLast(madeResult);
  const originalBrokerPid = await readBrokerPid();
  const restarted = await run(process.execPath, [harness, "restart"], {
    env: { ...declarationFromReservation(made.env), TEST_BROKER_CLAIM: brokerClaimPath() },
    timeout: 15_000,
  });
  assert.equal(restarted.code, 0, restarted.stderr);
  const data = parseLast(restarted);
  assert.equal(data.killedBrokerPid, originalBrokerPid);
  assert.notEqual(data.launcherPid, originalBrokerPid);
  assert.notEqual(data.after.brokerEpoch, data.before.brokerEpoch);
  assert.notEqual(await readBrokerPid(), originalBrokerPid);
  assert.equal(data.after.certain, false);
  assert.equal(data.after.allIdle, false);
  assert.equal(data.confirmedOldFence, false);
});

await scenario("malformed and oversized frames are rejected without killing broker", async () => {
  const endpoint = brokerEndpoint();
  for (const payload of [Buffer.from([0, 0, 0, 0]), Buffer.from([0, 1, 0, 1])]) {
    await new Promise((resolveSocket) => {
      const socket = net.connect(endpoint, () => socket.end(payload));
      socket.on("close", resolveSocket);
      socket.on("error", resolveSocket);
      setTimeout(() => { socket.destroy(); resolveSocket(); }, 1500);
    });
  }
  const healthy = await run(process.execPath, [harness], { env: baseEnv });
  assert.equal(healthy.code, 0, healthy.stderr);
});

await scenario("concurrent launch and stale socket recovery", async () => {
  const endpoint = brokerEndpoint();
  await killBroker();
  if (process.platform !== "win32") await writeFile(endpoint, "stale", { mode: 0o600 }).catch(() => {});
  const results = await Promise.all(Array.from({ length: 8 }, () => run(process.execPath, [harness], { env: baseEnv, timeout: 12_000 })));
  assert.ok(results.every((item) => item.code === 0), results.map((item) => item.stderr).join("\n"));
});

await scenario("clean packed artifact imports with committed dist", async () => {
  const work = await mkdtemp(join(tmpdir(), "pi-process-domain-acceptance-"));
  try {
    const pack = await run("npm", ["pack", "--silent", "--pack-destination", work], { cwd: root, timeout: 30_000 });
    assert.equal(pack.code, 0, pack.stderr);
    const tgz = pack.stdout.trim().split("\n").at(-1);
    const app = join(work, "app");
    await mkdir(app);
    await writeFile(join(app, "package.json"), '{"type":"module"}\n');
    const install = await run("npm", ["install", "--ignore-scripts", join(work, tgz)], { cwd: app, timeout: 30_000 });
    assert.equal(install.code, 0, install.stderr);
    const smoke = await run(process.execPath, ["--input-type=module", "-e", 'import("pi-process-domain").then(m=>console.log(typeof m.openDomain))'], { cwd: app });
    assert.equal(smoke.code, 0, smoke.stderr);
    assert.equal(smoke.stdout.trim(), "function");
    await chmod(join(app, "node_modules/pi-process-domain/bin/pi-process-domain-broker.mjs"), 0o755);
    const dist = await readFile(join(app, "node_modules/pi-process-domain/dist/index.js"), "utf8");
    assert.match(dist, /openDomain/);
  }
  finally {
    await rm(work, { recursive: true, force: true });
  }
});

console.log(`# ${passed} acceptance scenarios passed`);
