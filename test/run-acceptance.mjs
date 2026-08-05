#!/usr/bin/env node
import assert from "node:assert/strict";
import { mkdtemp, mkdir, readFile, rm, writeFile, chmod } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";
import net from "node:net";
import { setTimeout as delay } from "node:timers/promises";
import { brokerEndpoint } from "../dist/index.js";

const root = resolve(fileURLToPath(new URL("..", import.meta.url)));
const harness = join(root, "test/harness/domain-client.mjs");
const brokerBin = join(root, "bin/pi-process-domain-broker.mjs");
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
  child.stdout.on("data", (chunk) => { stdout += chunk; });
  child.stderr.on("data", (chunk) => { stderr += chunk; });
  const ready = new Promise((resolveReady, reject) => {
    const timer = setTimeout(() => reject(new Error(`child readiness timeout\n${stdout}\n${stderr}`)), options.timeout ?? deadlineMs);
    child.stdout.on("data", () => {
      const line = stdout.split("\n").find(Boolean);
      if (line) {
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

async function scenario(name, fn) {
  await fn();
  passed += 1;
  console.log(`ok ${passed} - ${name}`);
}

async function killBrokers() {
  if (process.platform === "win32") return;
  const result = await run("pgrep", ["-f", brokerBin]);
  if (result.code !== 0) return;
  for (const pid of result.stdout.trim().split("\n").filter(Boolean)) {
    try { process.kill(Number(pid), "SIGKILL"); } catch { /* already gone */ }
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
  await killBrokers();
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
  const malformed = await run(process.execPath, [harness], { env: { ...baseEnv, PI_PROCESS_DOMAIN_ID: "valid-domain-id", PI_PROCESS_DOMAIN_KEY: "!!!", PI_PROCESS_DOMAIN_PROTOCOL: "1.0" } });
  assert.notEqual(malformed.code, 0);
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
  const pidResult = await run("pgrep", ["-f", brokerBin]);
  assert.equal(pidResult.code, 0, pidResult.stderr);
  const brokerPid = pidResult.stdout.trim().split("\n").filter(Boolean).at(-1);
  const restarted = await run(process.execPath, [harness, "restart"], {
    env: { ...declarationFromReservation(made.env), TEST_BROKER_PID: brokerPid },
    timeout: 15_000,
  });
  assert.equal(restarted.code, 0, restarted.stderr);
  const data = parseLast(restarted);
  assert.notEqual(data.after.brokerEpoch, data.before.brokerEpoch);
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
  await killBrokers();
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
