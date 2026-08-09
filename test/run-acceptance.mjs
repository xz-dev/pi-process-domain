#!/usr/bin/env node
import assert from "node:assert/strict";
import { mkdtemp, mkdir, readFile, rm, stat, writeFile } from "node:fs/promises";
import { readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";
import net from "node:net";
import { setTimeout as delay } from "node:timers/promises";

const root = resolve(fileURLToPath(new URL("..", import.meta.url)));
const harness = join(root, "test/harness/domain-client.mjs");
const testRuntime = await mkdtemp(join(tmpdir(), "pi-process-domain-runtime-"));
process.env.XDG_RUNTIME_DIR = testRuntime;
process.on("exit", () => {
  try {
    const claim = JSON.parse(
      readFileSync(join(testRuntime, "pi-process-domain", "v1", "election", "election.lock", "claim.json"), "utf8"),
    );
    if (Number.isInteger(claim.pid) && claim.pid > 0) process.kill(claim.pid, "SIGTERM");
  }
  catch { /* broker absent or already stopped */ }
  rmSync(testRuntime, { recursive: true, force: true });
});
const baseEnv = { ...process.env };
for (const key of ["PI_PROCESS_DOMAIN_ID", "PI_PROCESS_DOMAIN_KEY", "PI_PROCESS_DOMAIN_PROTOCOL", "PI_PROCESS_DOMAIN_RESERVATION"]) delete baseEnv[key];
const npmExecPath = process.env.npm_execpath;
assert.ok(npmExecPath, "acceptance tests must run through npm");
const deadlineMs = 10_000;
let passed = 0;

function redact(output) {
  return output.replace(/("?(?:PI_PROCESS_DOMAIN_KEY|PI_PROCESS_DOMAIN_RESERVATION)"?\s*[:=]\s*")?[A-Za-z0-9_-]{32,}/g, "$1<redacted>");
}

function run(command, args = [], options = {}) {
  return new Promise((resolvePromise, reject) => {
    const child = spawn(command, args, {
      env: options.env ?? baseEnv,
      cwd: options.cwd ?? root,
      stdio: ["ignore", "pipe", "pipe"],
      shell: options.shell ?? false,
    });
    let stdout = "";
    let stderr = "";
    child.stdout.on("data", (chunk) => { stdout += chunk; });
    child.stderr.on("data", (chunk) => { stderr += chunk; });
    const timer = setTimeout(() => {
      child.kill("SIGKILL");
      reject(new Error(redact(`timeout: ${command} ${args.join(" ")}\n${stdout}\n${stderr}`)));
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
    const timer = setTimeout(() => reject(new Error(redact(`child readiness timeout\n${stdout}\n${stderr}`))), options.timeout ?? deadlineMs);
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
      reject(new Error(redact(`child exited before ready (${code})\n${stdout}\n${stderr}`)));
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
      reject(new Error(redact(`child exit timeout\n${stdout}\n${stderr}`)));
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

function declarationFromReservation(env) {
  return {
    ...baseEnv,
    PI_PROCESS_DOMAIN_ID: env.PI_PROCESS_DOMAIN_ID,
    PI_PROCESS_DOMAIN_KEY: env.PI_PROCESS_DOMAIN_KEY,
    PI_PROCESS_DOMAIN_PROTOCOL: env.PI_PROCESS_DOMAIN_PROTOCOL,
  };
}

await scenario("fresh roots own isolated broker endpoints", async () => {
  const rootA = start(["hold"], { env: baseEnv });
  const rootB = start(["hold"], { env: baseEnv });
  const [a, b] = await Promise.all([rootA.ready, rootB.ready]);
  try {
    assert.equal(a.created, true);
    assert.equal(b.created, true);
    assert.notEqual(a.endpoint, b.endpoint);
    await terminate(rootA);
    assert.equal(rootB.child.exitCode, null, "closing root A must not affect root B");
  }
  finally {
    await terminate(rootA);
    await terminate(rootB);
  }
});

await scenario("repeated opens in one root share ownership until the final close", async () => {
  const root = start(["hold-two"], { env: baseEnv });
  const owner = await root.ready;
  try {
    assert.equal(owner.created, true);
    assert.equal(owner.secondCreated, false);
    assert.equal(owner.snapshot.brokerEpoch, owner.secondSnapshot.brokerEpoch);
    const joined = await run(process.execPath, [harness], {
      env: declarationFromReservation(owner.env),
    });
    assert.equal(joined.code, 0, joined.stderr);
    assert.equal(parseLast(joined).endpoint, owner.endpoint);
  }
  finally {
    await terminate(root);
  }
});

await scenario("new domain and declared join across processes", async () => {
  const root = start(["hold-reservation"], { env: baseEnv });
  const reservationData = await root.ready;
  try {
    const joined = await run(process.execPath, [harness, "snapshot"], { env: declarationFromReservation(reservationData.env) });
    assert.equal(joined.code, 0, joined.stderr);
    assert.equal(parseLast(joined).created, false);
    assert.equal(parseLast(joined).endpoint, reservationData.endpoint);
  }
  finally {
    await terminate(root);
  }
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

  const root = start(["hold-reservation"], { env: baseEnv });
  const reservation = await root.ready;
  try {
    const wrongEnv = { ...declarationFromReservation(reservation.env), PI_PROCESS_DOMAIN_KEY: Buffer.alloc(32, 9).toString("base64url"), TEST_FATAL_OVERRIDE: "1" };
    const wrong = await run(process.execPath, [harness, "caught"], { env: wrongEnv });
    assert.equal(wrong.code, 78, redact(wrong.stderr));
    const wrongData = parseLast(wrong);
    assert.equal(wrongData.code, "AUTHENTICATION_FAILED");
    assert.equal(wrongData.exitCode, 78);
    assert.equal(wrongData.overriddenFatal, "AUTHENTICATION_FAILED");
  }
  finally {
    await terminate(root);
  }
  const absent = await run(process.execPath, [harness, "caught"], {
    env: {
      ...baseEnv,
      PI_PROCESS_DOMAIN_ID: "absent-domain-123",
      PI_PROCESS_DOMAIN_KEY: Buffer.alloc(32, 3).toString("base64url"),
      PI_PROCESS_DOMAIN_PROTOCOL: "2.0",
      TEST_FATAL_OVERRIDE: "1",
      TEST_CONNECT_TIMEOUT_MS: "500",
    },
    timeout: 3000,
  });
  assert.equal(absent.code, 78, redact(absent.stderr));
  const absentData = parseLast(absent);
  assert.equal(absentData.code, "BROKER_UNAVAILABLE");
  assert.equal(absentData.exitCode, 78);
  assert.equal(absentData.overriddenFatal, "BROKER_UNAVAILABLE");
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
  const root = start(["hold-reservation"], { env: { ...baseEnv, TEST_TTL_MS: "1200" } });
  const made = await root.ready;
  try {
    assert.equal(made.snapshot.pendingSpawns, 1);
    const adopted = await run(process.execPath, [harness], { env: { ...baseEnv, ...made.env } });
    assert.equal(adopted.code, 0, adopted.stderr);
    const replay = await run(process.execPath, [harness], { env: { ...baseEnv, ...made.env } });
    assert.notEqual(replay.code, 0);
  }
  finally {
    await terminate(root);
  }

  const canceledRoot = start(["hold-reservation"], { env: { ...baseEnv, TEST_CANCEL: "1" } });
  const canceled = await canceledRoot.ready;
  try {
    const canceledJoin = await run(process.execPath, [harness], { env: { ...baseEnv, ...canceled.env } });
    assert.notEqual(canceledJoin.code, 0);
  }
  finally {
    await terminate(canceledRoot);
  }

  const expiringRoot = start(["hold-reservation"], { env: { ...baseEnv, TEST_TTL_MS: "1000" } });
  const expiring = await expiringRoot.ready;
  try {
    await delay(1200);
    const expiredJoin = await run(process.execPath, [harness], { env: { ...baseEnv, ...expiring.env } });
    assert.notEqual(expiredJoin.code, 0);
  }
  finally {
    await terminate(expiringRoot);
  }
});

await scenario("rejected child reservation does not disturb the holding parent", async () => {
  const parent = start(["hold-reservation"], { env: baseEnv });
  const made = await parent.ready;
  const parentEnv = declarationFromReservation(made.env);
  try {
    const adopted = await run(process.execPath, [harness], { env: { ...baseEnv, ...made.env } });
    assert.equal(adopted.code, 0, adopted.stderr);
    const replay = await run(process.execPath, [harness], { env: { ...baseEnv, ...made.env } });
    assert.notEqual(replay.code, 0, redact(`replayed claim unexpectedly joined\n${replay.stdout}\n${replay.stderr}`));
    assert.equal(parent.child.exitCode, null, "replayed child claim must not terminate the holding parent");
    const observer = await run(process.execPath, [harness], { env: parentEnv });
    assert.equal(observer.code, 0, observer.stderr);
  }
  finally {
    await terminate(parent);
  }
});

if (process.platform !== "win32") await scenario("paused established child recovers after broker lease expiry", async () => {
  const root = start(["hold"], { env: baseEnv });
  const owner = await root.ready;
  const client = start(["recover"], { env: declarationFromReservation(owner.env), timeout: 15_000 });
  await client.ready;
  try {
    process.kill(client.child.pid, "SIGSTOP");
    await delay(12_500);
    process.kill(client.child.pid, "SIGCONT");
    const result = await waitForExit(client, 25_000);
    assert.equal(result.code, 0, result.stderr);
    const recovered = parseLast(result);
    assert.equal(recovered.recovered, true, result.stdout);
    assert.equal(recovered.idle.certain, true);
    assert.equal(recovered.idle.allIdle, true);
  }
  finally {
    if (client.child.exitCode === null) process.kill(client.child.pid, "SIGCONT");
    await terminate(client);
    await terminate(root);
  }
});

if (process.platform !== "win32") await scenario("heartbeat suspicion publishes one uncertainty transition to a peer", async () => {
  const root = start(["hold"], { env: baseEnv });
  const owner = await root.ready;
  const target = start(["hold"], { env: declarationFromReservation(owner.env) });
  await target.ready;
  const observer = start(["observe"], {
    env: { ...declarationFromReservation(owner.env), TEST_OBSERVE_MS: "8500" },
    timeout: 12_000,
  });
  await observer.ready;
  process.kill(target.child.pid, "SIGSTOP");
  try {
    const result = await waitForExit(observer, 12_000);
    assert.equal(result.code, 0, result.stderr);
    const data = parseLast(result);
    const firstUncertain = data.observed.findIndex(
      (snapshot) => snapshot.certain === false,
    );
    assert.notEqual(firstUncertain, -1, JSON.stringify(data.observed));
    const transition = data.observed[firstUncertain];
    const duplicates = data.observed.filter(
      (snapshot) =>
        snapshot.certain === false &&
        snapshot.brokerEpoch === transition.brokerEpoch &&
        snapshot.revision === transition.revision &&
        snapshot.activityGeneration === transition.activityGeneration,
    );
    assert.equal(
      duplicates.length,
      1,
      `suspicion transition must publish once: ${JSON.stringify(data.observed)}`,
    );
  }
  finally {
    process.kill(target.child.pid, "SIGCONT");
    await terminate(target);
    await terminate(observer);
    await terminate(root);
  }
});

await scenario("child crash makes the domain uncertain", async () => {
  const root = start(["hold"], { env: baseEnv });
  const owner = await root.ready;
  const holder = start(["hold"], { env: declarationFromReservation(owner.env) });
  await holder.ready;
  try {
    holder.child.kill("SIGKILL");
    await new Promise((r) => holder.child.once("exit", r));
    const observer = await run(process.execPath, [harness], { env: declarationFromReservation(owner.env) });
    assert.equal(observer.code, 0, observer.stderr);
    assert.equal(parseLast(observer).snapshot.allIdle, false);
  }
  finally {
    await terminate(holder);
    await terminate(root);
  }
});

await scenario("legacy v1 declaration joins only a baseline broker", async () => {
  const work = await mkdtemp(join(tmpdir(), "pi-process-domain-v1-baseline-"));
  try {
    const archive = spawn("git", ["archive", "85bb08a"], { cwd: root, stdio: ["ignore", "pipe", "pipe"] });
    const tar = spawn("tar", ["-x", "-C", work], { stdio: ["pipe", "ignore", "pipe"] });
    archive.stdout.pipe(tar.stdin);
    const [archiveExit, tarExit] = await Promise.all([
      new Promise((resolveExit) => archive.once("exit", resolveExit)),
      new Promise((resolveExit) => tar.once("exit", resolveExit)),
    ]);
    assert.equal(archiveExit, 0, "baseline archive failed");
    assert.equal(tarExit, 0, "baseline extraction failed");
    const install = await run(process.execPath, [npmExecPath, "install", "--ignore-scripts", "--no-audit", "--no-fund"], { cwd: work, timeout: 60_000 });
    assert.equal(install.code, 0, redact(install.stderr));
    const build = await run(process.execPath, [npmExecPath, "run", "build:dist"], { cwd: work, timeout: 30_000 });
    assert.equal(build.code, 0, redact(build.stderr));
    const legacyHarness = join(work, "legacy-root.mjs");
    await writeFile(legacyHarness, `import { tryAcquireElection } from "./dist/internal/launcher.js";\nimport { launchBrokerForCurrentUser } from "./dist/internal/broker.js";\nimport { openDomain } from "./dist/index.js";\nconst owner = tryAcquireElection();\nif (owner === null) throw new Error("legacy election unavailable");\nconst broker = await launchBrokerForCurrentUser(owner);\nconst { domain } = await openDomain({ connectTimeoutMs: 4000 });\nprocess.stdout.write(JSON.stringify({ env: { PI_PROCESS_DOMAIN_ID: process.env.PI_PROCESS_DOMAIN_ID, PI_PROCESS_DOMAIN_KEY: process.env.PI_PROCESS_DOMAIN_KEY, PI_PROCESS_DOMAIN_PROTOCOL: process.env.PI_PROCESS_DOMAIN_PROTOCOL } }) + "\\n");\nprocess.stdin.once("data", async () => { await domain.close(); await broker.close(); process.exit(0); });\n`);
    const legacy = spawn(process.execPath, [legacyHarness], { cwd: work, env: baseEnv, stdio: ["pipe", "pipe", "pipe"] });
    let legacyOutput = "";
    let legacyError = "";
    legacy.stdout.on("data", (chunk) => { legacyOutput += chunk; });
    legacy.stderr.on("data", (chunk) => { legacyError += chunk; });
    try {
      const until = Date.now() + deadlineMs;
      while (!legacyOutput.includes("\n") && legacy.exitCode === null && Date.now() < until) await delay(25);
      assert.equal(legacy.exitCode, null, redact(legacyError));
      const declaration = { ...baseEnv, ...JSON.parse(legacyOutput.trim()).env };
      const joined = await run(process.execPath, [harness], { env: declaration });
      assert.equal(joined.code, 0, redact(joined.stderr));
      assert.equal(parseLast(joined).created, false);
    }
    finally {
      const exit = new Promise((resolveExit, reject) => {
        let timedOut = false;
        const timer = setTimeout(() => {
          timedOut = true;
          legacy.kill("SIGKILL");
        }, deadlineMs);
        legacy.once("exit", (code, signal) => {
          clearTimeout(timer);
          if (timedOut) reject(new Error(redact(`legacy broker close timeout\n${legacyError}`)));
          else resolveExit({ code, signal });
        });
      });
      legacy.stdin.end("close\n");
      const result = await exit;
      assert.equal(result.code, 0, redact(`legacy broker close failed (${result.signal})\n${legacyError}`));
    }
  }
  finally {
    await rm(work, { recursive: true, force: true });
  }
});

await scenario("established child fails closed after root loss without affecting another root", async () => {
  const root = start(["hold"], { env: baseEnv });
  const independent = start(["hold"], { env: baseEnv });
  const [owner, other] = await Promise.all([root.ready, independent.ready]);
  const triggerFile = join(testRuntime, `root-loss-${Date.now()}`);
  const child = start(["root-loss"], {
    env: { ...declarationFromReservation(owner.env), TEST_RECOVERY_TIMEOUT_MS: "5000", TEST_TRIGGER_FILE: triggerFile },
    timeout: 10_000,
  });
  await child.ready;
  try {
    root.child.kill("SIGKILL");
    await waitForExit(root);
    await writeFile(triggerFile, "continue\n");
    const result = await waitForExit(child, 10_000);
    assert.equal(result.code, 0, redact(result.stderr));
    const after = parseLast(result);
    assert.equal(after.snapshot.certain, false);
    assert.equal(after.snapshot.allIdle, false);
    assert.equal(after.operationError, "BROKER_UNAVAILABLE");
    assert.equal(after.endpoint, owner.endpoint);
    assert.equal(independent.child.exitCode, null, "root loss must not affect an independent domain");
    assert.notEqual(other.endpoint, owner.endpoint);
    if (process.platform !== "win32") await assert.rejects(readFile(owner.endpoint), /ENOENT|ENXIO/);
    const missing = await run(process.execPath, [harness], {
      env: { ...declarationFromReservation(owner.env), TEST_CONNECT_TIMEOUT_MS: "500" },
      timeout: 3000,
    });
    assert.equal(missing.code, 78, redact(missing.stderr));
    assert.equal(parseLast(missing).code, "BROKER_UNAVAILABLE");
  }
  finally {
    await terminate(child);
    await terminate(root);
    await terminate(independent);
  }
});

await scenario("malformed and oversized frames are rejected without killing broker", async () => {
  const root = start(["hold"], { env: baseEnv });
  const owner = await root.ready;
  try {
    for (const payload of [Buffer.from([0, 0, 0, 0]), Buffer.from([0, 1, 0, 1])]) {
      await new Promise((resolveSocket) => {
        const socket = net.connect(owner.endpoint, () => socket.end(payload));
        socket.on("close", resolveSocket);
        socket.on("error", resolveSocket);
        setTimeout(() => { socket.destroy(); resolveSocket(); }, 1500);
      });
    }
    const healthy = await run(process.execPath, [harness], { env: declarationFromReservation(owner.env) });
    assert.equal(healthy.code, 0, healthy.stderr);
  }
  finally {
    await terminate(root);
  }
});

await scenario("clean packed artifact imports with committed dist", async () => {
  const work = await mkdtemp(join(tmpdir(), "pi-process-domain-acceptance-"));
  try {
    const pack = await run(process.execPath, [npmExecPath, "pack", "--silent", "--pack-destination", work], { cwd: root, timeout: 30_000 });
    assert.equal(pack.code, 0, pack.stderr);
    const tgz = pack.stdout.trim().split("\n").at(-1);
    const app = join(work, "app");
    await mkdir(app);
    await writeFile(join(app, "package.json"), '{"type":"module"}\n');
    const install = await run(process.execPath, [npmExecPath, "install", "--ignore-scripts", join(work, tgz)], { cwd: app, timeout: 30_000 });
    assert.equal(install.code, 0, install.stderr);
    const installedHarness = join(app, "smoke.mjs");
    await writeFile(installedHarness, `import { openDomain } from "pi-process-domain";\nconst json = (value) => JSON.stringify(value, (_key, item) => typeof item === "bigint" ? item.toString() : item);\nconst { domain, created } = await openDomain({ connectTimeoutMs: 3000 });\nif (process.argv[2] === "hold") {\n  process.stdout.write(json({ created, env: { PI_PROCESS_DOMAIN_ID: process.env.PI_PROCESS_DOMAIN_ID, PI_PROCESS_DOMAIN_KEY: process.env.PI_PROCESS_DOMAIN_KEY, PI_PROCESS_DOMAIN_PROTOCOL: process.env.PI_PROCESS_DOMAIN_PROTOCOL }, snapshot: domain.snapshot() }) + "\\n");\n  const finish = async () => { await domain.close(); process.exit(0); }; process.on("SIGTERM", () => void finish()); setInterval(() => {}, 60000);\n} else { process.stdout.write(json({ created, snapshot: domain.snapshot() }) + "\\n"); await domain.close(); }\n`);
    const packedRoot = spawn(process.execPath, [installedHarness, "hold"], { cwd: app, env: baseEnv, stdio: ["ignore", "pipe", "pipe"] });
    let packedOutput = "";
    let packedError = "";
    packedRoot.stdout.on("data", (chunk) => { packedOutput += chunk; });
    packedRoot.stderr.on("data", (chunk) => { packedError += chunk; });
    try {
      const until = Date.now() + deadlineMs;
      while (!packedOutput.includes("\n") && packedRoot.exitCode === null && Date.now() < until) await delay(25);
      assert.equal(packedRoot.exitCode, null, redact(packedError));
      const packed = JSON.parse(packedOutput.trim());
      assert.equal(packed.created, true);
      assert.equal(packed.snapshot.certain, true);
      const joined = await run(process.execPath, [installedHarness], { cwd: app, env: { ...baseEnv, ...packed.env } });
      assert.equal(joined.code, 0, redact(joined.stderr));
      assert.equal(JSON.parse(joined.stdout).created, false);
    }
    finally {
      if (packedRoot.exitCode === null) {
        const exited = new Promise((resolveExit) => packedRoot.once("exit", resolveExit));
        packedRoot.kill("SIGTERM");
        await exited;
      }
    }
    const bin = join(app, "node_modules/pi-process-domain/bin/pi-process-domain-broker.mjs");
    const shim = join(app, "node_modules/.bin", process.platform === "win32"
      ? "pi-process-domain-broker.cmd"
      : "pi-process-domain-broker");
    if (process.platform !== "win32") assert.notEqual((await stat(bin)).mode & 0o111, 0);
    const binSmoke = await run(shim, [], {
      cwd: app,
      env: baseEnv,
      timeout: 3000,
      shell: process.platform === "win32",
    });
    assert.equal(binSmoke.code, 78);
    assert.match(binSmoke.stderr, /missing broker election ownership token/);
  }
  finally {
    await rm(work, { recursive: true, force: true });
  }
});

console.log(`# ${passed} acceptance scenarios passed`);
