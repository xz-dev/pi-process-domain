#!/usr/bin/env node
import { readFile } from "node:fs/promises";
import { brokerEndpoint, openDomain } from "../../dist/index.js";

const command = process.argv[2] ?? "snapshot";
const timeoutMs = Number(process.env.TEST_CONNECT_TIMEOUT_MS ?? 4000);

function output(value) {
  process.stdout.write(`${JSON.stringify(value, (_key, item) => typeof item === "bigint" ? item.toString() : item)}\n`);
}

let overriddenFatal = null;
try {
  const options = {
    initialActivity: process.env.TEST_ACTIVITY === "busy" ? "busy" : "idle",
    connectTimeoutMs: timeoutMs,
  };
  if (process.env.TEST_FATAL_OVERRIDE === "1") {
    options.onFatal = (error) => { overriddenFatal = error?.code ?? error?.name ?? "Error"; };
  }
  const { domain, created } = await openDomain(options);

  if (command === "root-loss") {
    const observed = [];
    const unsubscribe = domain.subscribe((snapshot) => observed.push(snapshot));
    output({ ready: true, created, endpoint: brokerEndpoint(), snapshot: domain.snapshot() });
    const triggerFile = process.env.TEST_TRIGGER_FILE;
    if (!triggerFile) throw new Error("root-loss requires TEST_TRIGGER_FILE");
    for (;;) {
      try { await readFile(triggerFile); break; }
      catch { await new Promise((resolve) => setTimeout(resolve, 25)); }
    }
    const until = Date.now() + Number(process.env.TEST_RECOVERY_TIMEOUT_MS ?? 5000);
    while (domain.snapshot().certain && Date.now() < until) {
      await new Promise((resolve) => setTimeout(resolve, 50));
    }
    let operationError = null;
    try { await domain.setActivity("busy"); }
    catch (error) { operationError = error?.code ?? error?.name ?? "Error"; }
    unsubscribe();
    output({ snapshot: domain.snapshot(), observed, operationError, endpoint: brokerEndpoint() });
    await domain.close();
  }
  else if (command === "hold" || command === "recover" || command === "hold-reservation" || command === "hold-two") {
    let reservation = null;
    if (command === "hold-reservation") {
      reservation = await domain.reserveSpawn({ ttlMs: Number(process.env.TEST_TTL_MS ?? 2000) });
      if (process.env.TEST_CANCEL === "1") await reservation.cancel();
    }
    const second = command === "hold-two" ? await openDomain(options) : null;
    output({
      ready: true,
      created,
      secondCreated: second?.created ?? null,
      endpoint: brokerEndpoint(),
      env: reservation?.env ?? {
        PI_PROCESS_DOMAIN_ID: process.env.PI_PROCESS_DOMAIN_ID,
        PI_PROCESS_DOMAIN_KEY: process.env.PI_PROCESS_DOMAIN_KEY,
        PI_PROCESS_DOMAIN_PROTOCOL: process.env.PI_PROCESS_DOMAIN_PROTOCOL,
      },
      snapshot: domain.snapshot(),
      secondSnapshot: second?.domain.snapshot() ?? null,
    });
    if (second !== null) await domain.close();
    const finish = async () => {
      if (second !== null) await second.domain.close();
      else await domain.close();
      process.exit(0);
    };
    process.on("SIGTERM", () => void finish());
    process.on("SIGINT", () => void finish());
    if (command === "recover") {
      const originalEpoch = domain.snapshot().brokerEpoch;
      const observed = [];
      const unsubscribe = domain.subscribe((snapshot) => observed.push(snapshot));
      if (process.env.TEST_WAIT_FOR_RECOVERY_SIGNAL === "1") {
        await new Promise((resolve) => process.once("SIGUSR1", resolve));
      }
      const until = Date.now() + Number(process.env.TEST_RECOVERY_TIMEOUT_MS ?? 20_000);
      let recovered = false;
      let lastError = null;
      while (Date.now() < until) {
        const current = domain.snapshot();
        if (current.certain) {
          try {
            const busy = await domain.setActivity("busy");
            const idle = await domain.setActivity("idle");
            output({
              recovered: true,
              epochChanged: idle.brokerEpoch !== originalEpoch,
              busy,
              idle,
              observed,
            });
            recovered = true;
            break;
          }
          catch (error) {
            lastError = { code: error?.code ?? "UNKNOWN", message: error?.message ?? String(error) };
          }
        }
        await new Promise((resolve) => setTimeout(resolve, 250));
      }
      if (!recovered) {
        output({ recovered: false, snapshot: domain.snapshot(), lastError, observed });
        process.exitCode = 1;
      }
      unsubscribe();
      await domain.close();
      process.exit();
    }
    setInterval(() => {}, 60_000);
  }
  else if (command === "observe") {
    const observed = [];
    const unsubscribe = domain.subscribe((snapshot) => observed.push(snapshot));
    output({ ready: true, created, snapshot: domain.snapshot() });
    await new Promise((resolve) =>
      setTimeout(resolve, Number(process.env.TEST_OBSERVE_MS ?? 8000)),
    );
    unsubscribe();
    output({ observed, snapshot: domain.snapshot() });
    await domain.close();
  }
  else if (command === "reservation") {
    const reservation = await domain.reserveSpawn({ ttlMs: Number(process.env.TEST_TTL_MS ?? 2000) });
    output({ created, endpoint: brokerEndpoint(), env: reservation.env, snapshot: domain.snapshot() });
    if (process.env.TEST_CANCEL === "1") await reservation.cancel();
    await domain.close();
  }
  else if (command === "restart") {
    const before = domain.snapshot();
    const observed = [];
    const unsubscribe = domain.subscribe((snapshot) => observed.push(snapshot));
    const brokerPid = Number(JSON.parse(await readFile(process.env.TEST_BROKER_CLAIM, "utf8")).pid);
    process.kill(brokerPid, "SIGKILL");
    const until = Date.now() + Number(process.env.TEST_RECOVERY_TIMEOUT_MS ?? 9000);
    while (
      Date.now() < until &&
      (domain.snapshot().brokerEpoch === before.brokerEpoch ||
        (process.env.TEST_WAIT_CERTAIN === "1" && !domain.snapshot().certain))
    ) {
      await new Promise((resolve) => setTimeout(resolve, 100));
    }
    unsubscribe();
    output({
      created,
      launcherPid: process.pid,
      killedBrokerPid: brokerPid,
      before,
      after: domain.snapshot(),
      observed,
      confirmedOldFence: await domain.confirm(before.fence),
    });
    await domain.close();
  }
  else if (command === "transitions") {
    const observed = [];
    const unsubscribe = domain.subscribe((snapshot) => observed.push(snapshot));
    const before = domain.snapshot();
    const busy = await domain.setActivity("busy");
    const oldFenceConfirmed = await domain.confirm(before.fence);
    const idle = await domain.setActivity("idle");
    unsubscribe();
    output({ created, before, busy, idle, observed, oldFenceConfirmed, confirmed: await domain.confirm(idle.fence) });
    await domain.close();
  }
  else if (command === "caught") {
    output({ created, snapshot: domain.snapshot() });
    await domain.close();
  }
  else {
    output({ created, endpoint: brokerEndpoint(), snapshot: domain.snapshot() });
    await domain.close();
  }
}
catch (error) {
  output({
    error: error?.name ?? "Error",
    code: error?.code ?? "UNKNOWN",
    message: error?.message ?? String(error),
    exitCode: process.exitCode ?? null,
    overriddenFatal,
  });
  if (command !== "caught") process.exitCode = 78;
}
