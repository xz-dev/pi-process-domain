import { openProcessDomain } from "../dist/process-domain/index.js";

const node = await openProcessDomain({
  metadata: { role: process.env.CHILD_ROLE ?? "child-process" },
  connectTimeoutMs: 3000,
  heartbeatIntervalMs: 100,
  heartbeatTimeoutMs: 400,
  heartbeatTimeToLiveMs: 300,
});

process.stdout.write(`${JSON.stringify({ nodeId: node.nodeId })}\n`);
node.subscribe("acceptance-probe", (message) => {
  process.stdout.write(`${JSON.stringify({ probe: message.value })}\n`);
});
process.on("SIGTERM", () => void node.close().finally(() => process.exit(0)));
setInterval(() => {}, 60_000);
