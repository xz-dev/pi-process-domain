import { spawn } from "node:child_process";
import { openDomain } from "../../dist/index.js";

if (process.env.PI_PROCESS_DOMAIN_CLOSE_CHILD === "1") {
  const { domain, created } = await openDomain({ initialActivity: "busy" });
  if (created) throw new Error("child unexpectedly created domain");
  await domain.setActivity("idle");
  await domain.close();
  process.exit(0);
}

for (const key of Object.keys(process.env)) {
  if (key.startsWith("PI_PROCESS_DOMAIN_")) delete process.env[key];
}

for (let iteration = 0; iteration < 10; iteration++) {
  const { domain, created } = await openDomain({ initialActivity: "idle" });
  if (!created) throw new Error("root did not create domain");

  const child = spawn(process.execPath, [new URL(import.meta.url).pathname], {
    env: { ...process.env, PI_PROCESS_DOMAIN_CLOSE_CHILD: "1" },
    stdio: "inherit",
  });
  const status = await new Promise((resolve, reject) => {
    child.once("error", reject);
    child.once("exit", (code, signal) => resolve({ code, signal }));
  });
  if (status.code !== 0) throw new Error(`child failed: ${JSON.stringify(status)}`);

  await domain.close();
}
