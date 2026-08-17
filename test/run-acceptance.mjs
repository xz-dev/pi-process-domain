import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";
import { openProcessDomain, ENV_NAMES, preferredTransport } from "../dist/process-domain/index.js";

const env = {};
const root = await openProcessDomain({
  env,
  connectTimeoutMs: 3000,
  heartbeatIntervalMs: 100,
  heartbeatTimeoutMs: 400,
  heartbeatTimeToLiveMs: 300,
});
let child;
try {
  assert.equal(root.transport, preferredTransport());
  assert.equal(root.endpoint.includes("pi-extension-utils"), false);
  child = spawn(process.execPath, [fileURLToPath(new URL("./harness-child.mjs", import.meta.url))], {
    env: { ...process.env, [ENV_NAMES.DECLARATION]: env[ENV_NAMES.DECLARATION] },
    stdio: ["ignore", "pipe", "pipe"],
  });
  let stdout = "";
  let stderr = "";
  let exit;
  child.stdout.setEncoding("utf8");
  child.stderr.setEncoding("utf8");
  child.stdout.on("data", (chunk) => { stdout += chunk; });
  child.stderr.on("data", (chunk) => { stderr += chunk; });
  child.once("exit", (code, signal) => { exit = { code, signal }; });
  const deadline = Date.now() + 5000;
  while (!stdout.includes("\n")) {
    if (exit !== undefined) throw new Error(`child exited before join: ${JSON.stringify(exit)} ${stderr}`);
    if (Date.now() > deadline) throw new Error(`child did not join: ${stderr}`);
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
  const nodeId = JSON.parse(stdout.trim().split("\n")[0]).nodeId;
  assert.equal(root.peers().some((peer) => peer.nodeId === nodeId && peer.status === "online"), true);

  const offline = new Promise((resolve) => {
    const stop = root.subscribeEvents((event) => {
      if (event.type === "peer" && event.peer.nodeId === nodeId && event.peer.status === "offline") {
        stop();
        resolve(event.peer);
      }
    });
  });
  if (process.platform === "win32") child.kill();
  else child.kill("SIGSTOP");
  await Promise.race([
    offline,
    new Promise((_, reject) => setTimeout(() => reject(new Error("ZMTP liveness did not report the child offline")), 5000)),
  ]);
  if (process.platform !== "win32") {
    const online = new Promise((resolve) => {
      const stop = root.subscribeEvents((event) => {
        if (event.type === "peer" && event.peer.nodeId === nodeId && event.peer.status === "online") {
          stop();
          resolve(event.peer);
        }
      });
    });
    child.kill("SIGCONT");
    await Promise.race([
      online,
      new Promise((_, reject) => setTimeout(() => reject(new Error("child did not reauthenticate after reconnect")), 5000)),
    ]);
    await root.send(nodeId, "acceptance-probe", { reconnected: true });
    const probeDeadline = Date.now() + 5000;
    while (!stdout.includes('"reconnected":true')) {
      if (exit !== undefined) throw new Error(`child exited before reconnect probe: ${JSON.stringify(exit)} ${stderr}`);
      if (Date.now() > probeDeadline) throw new Error(`child did not receive reconnect probe: ${stderr}`);
      await new Promise((resolve) => setTimeout(resolve, 20));
    }
  }
  child.kill("SIGTERM");
  await new Promise((resolve) => child.once("exit", resolve));
  console.log(JSON.stringify({ ok: true, transport: root.transport }));
} finally {
  if (child?.exitCode === null) {
    if (process.platform !== "win32") child.kill("SIGCONT");
    child.kill("SIGTERM");
  }
  await root.close();
}
