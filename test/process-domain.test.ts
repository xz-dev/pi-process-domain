import { describe, expect, it } from "vitest";
import { attachPiLifecycle, ENV_NAMES, openProcessDomain, preferredTransport, wildcardEndpoint } from "../src/process-domain/index.js";

describe("process-domain transport", () => {
  it("selects the loopback TCP wildcard endpoint", () => {
    const transport = preferredTransport();
    expect(wildcardEndpoint()).toBe(transport === "ipc" ? "ipc://*" : "tcp://127.0.0.1:*");
  });

  it("opens root and child, sends in both directions, and reports lifecycle", async () => {
    const rootEnv: NodeJS.ProcessEnv = {};
    const root = await openProcessDomain({ env: rootEnv, connectTimeoutMs: 2_000, heartbeatIntervalMs: 100, heartbeatTimeoutMs: 400, heartbeatTimeToLiveMs: 300 });
    const child = await openProcessDomain({ env: { [ENV_NAMES.DECLARATION]: rootEnv[ENV_NAMES.DECLARATION] }, metadata: { role: "child" }, connectTimeoutMs: 2_000, heartbeatIntervalMs: 100, heartbeatTimeoutMs: 400, heartbeatTimeToLiveMs: 300 });
    try {
      expect(root.role).toBe("host");
      expect(child.role).toBe("client");
      expect(root.endpoint).not.toContain("pi-extension-utils");
      expect(root.transport).toBe(preferredTransport());
      expect(root.peers()).toEqual([expect.objectContaining({ nodeId: child.nodeId, status: "online", metadata: { role: "child" } })]);

      const childData = new Promise<unknown>((resolve) => child.subscribe("root-to-child", (message) => resolve(message.value)));
      await root.send(child.nodeId, "root-to-child", { ok: 1 });
      await expect(childData).resolves.toEqual({ ok: 1 });

      const rootData = new Promise<unknown>((resolve) => root.subscribe("child-to-root", (message) => resolve(message.value)));
      await child.send(root.nodeId, "child-to-root", { ok: 2 });
      await expect(rootData).resolves.toEqual({ ok: 2 });

      const secondChild = await openProcessDomain({ env: { [ENV_NAMES.DECLARATION]: rootEnv[ENV_NAMES.DECLARATION] }, metadata: { role: "second-child" }, connectTimeoutMs: 2_000, heartbeatIntervalMs: 100, heartbeatTimeoutMs: 400, heartbeatTimeToLiveMs: 300 });
      try {
        const broadcast = new Promise<unknown>((resolve) => secondChild.subscribe("broadcast", (message) => resolve(message.value)));
        await child.broadcast("broadcast", { ok: 3 });
        await expect(broadcast).resolves.toEqual({ ok: 3 });
      } finally {
        await secondChild.close();
      }

      const lifecycle = new Promise<string>((resolve) => root.subscribeEvents((event) => { if (event.type === "lifecycle") resolve(event.event.name); }));
      await child.reportLifecycle({ name: "agent_start", at: Date.now(), sessionId: "child-session" });
      await expect(lifecycle).resolves.toBe("agent_start");
    } finally {
      await child.close();
      await root.close();
    }
  });

  it("only removes the declaration value published by its host", async () => {
    const ownedEnv: NodeJS.ProcessEnv = {};
    const ownedRoot = await openProcessDomain({ env: ownedEnv });
    expect(ownedEnv[ENV_NAMES.DECLARATION]).toBeDefined();
    await ownedRoot.close();
    expect(ownedEnv[ENV_NAMES.DECLARATION]).toBeUndefined();

    const replacedEnv: NodeJS.ProcessEnv = {};
    const replacedRoot = await openProcessDomain({ env: replacedEnv });
    replacedEnv[ENV_NAMES.DECLARATION] = "replacement";
    await replacedRoot.close();
    expect(replacedEnv[ENV_NAMES.DECLARATION]).toBe("replacement");
  });

  it("does not mutate a client declaration environment", async () => {
    const rootEnv: NodeJS.ProcessEnv = {};
    const root = await openProcessDomain({ env: rootEnv });
    const clientEnv: NodeJS.ProcessEnv = { [ENV_NAMES.DECLARATION]: rootEnv[ENV_NAMES.DECLARATION] };
    const child = await openProcessDomain({ env: clientEnv });
    const inherited = clientEnv[ENV_NAMES.DECLARATION];
    try {
      await child.close();
      expect(clientEnv[ENV_NAMES.DECLARATION]).toBe(inherited);
    } finally {
      await root.close();
    }
  });

  it("reconnects after the host connection drops and resumes messaging", async () => {
    const timing = { connectTimeoutMs: 2_000, heartbeatIntervalMs: 50, heartbeatTimeoutMs: 200, heartbeatTimeToLiveMs: 100 } as const;
    const rootEnv: NodeJS.ProcessEnv = {};
    const root = await openProcessDomain({ env: rootEnv, ...timing });
    const child = await openProcessDomain({ env: { [ENV_NAMES.DECLARATION]: rootEnv[ENV_NAMES.DECLARATION] }, ...timing });
    const waitPeer = (node: typeof child, nodeId: string, status: "online" | "offline") => new Promise<void>((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error(`peer did not become ${status}`)), 5_000);
      const stop = node.subscribeEvents((event) => {
        if (event.type === "peer" && event.peer.nodeId === nodeId && event.peer.status === status) {
          clearTimeout(timer);
          stop();
          resolve();
        }
      });
    });
    try {
      const offline = waitPeer(child, root.nodeId, "offline");
      (child as unknown as { ownLink: { close(): void } | null }).ownLink?.close();
      await offline;
      await expect(child.send(root.nodeId, "during-offline", null)).rejects.toThrow("process-domain host is offline");
      await waitPeer(child, root.nodeId, "online");
      const received = new Promise<unknown>((resolve) => child.subscribe("after-reconnect", (message) => resolve(message.value)));
      await root.send(child.nodeId, "after-reconnect", { ok: true });
      await expect(received).resolves.toEqual({ ok: true });
    } finally {
      await child.close();
      await root.close();
    }
  });

  it("attaches Pi lifecycle without interpreting events", async () => {
    const env: NodeJS.ProcessEnv = {};
    const root = await openProcessDomain({ env });
    const handlers = new Map<string, (event: unknown, context: unknown) => unknown>();
    const observed = new Promise<string>((resolve) => root.subscribeEvents((event) => { if (event.type === "lifecycle") resolve(event.event.name); }));
    const attachment = attachPiLifecycle(root, { on: (name, handler) => handlers.set(name, handler) }, "session");
    try {
      handlers.get("agent_start")?.({}, {});
      await expect(observed).resolves.toBe("agent_start");
    } finally {
      attachment.close();
      await root.close();
    }
  });
});
