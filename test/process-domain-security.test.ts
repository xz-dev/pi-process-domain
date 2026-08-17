import { Dealer, Router, type Message } from "zeromq";
import { describe, expect, it } from "vitest";
import { bindTemporaryEndpoint } from "../src/process-domain/endpoint.js";
import {
  ENV_NAMES,
  isProcessDomainOpenError,
  openProcessDomain,
} from "../src/process-domain/index.js";
import {
  createProof,
  decodeDeclaration,
  decodeEnvelope,
  encodeDeclaration,
  encodeEnvelope,
  randomId,
  type WireEnvelope,
} from "../src/process-domain/protocol.js";
import type { ProcessDomainDeclaration, ProcessDomainNode } from "../src/process-domain/types.js";
import { PROCESS_DOMAIN_PROTOCOL } from "../src/process-domain/types.js";

const timing = {
  connectTimeoutMs: 1_000,
  heartbeatIntervalMs: 50,
  heartbeatTimeoutMs: 200,
  heartbeatTimeToLiveMs: 100,
} as const;

const socketTiming = {
  heartbeatInterval: timing.heartbeatIntervalMs,
  heartbeatTimeout: timing.heartbeatTimeoutMs,
  heartbeatTimeToLive: timing.heartbeatTimeToLiveMs,
  linger: 0,
} as const;

function wait<T>(promise: Promise<T>, timeoutMs = 2_000, label = "operation"): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`test wait timed out: ${label}`)), timeoutMs);
    promise.then(
      (value) => { clearTimeout(timer); resolve(value); },
      (error) => { clearTimeout(timer); reject(error); },
    );
  });
}

function receive(socket: Dealer, label = "DEALER receive"): Promise<WireEnvelope> {
  return wait(socket.receive().then((frames) => {
    const frame = frames.at(-1);
    if (frame === undefined) throw new Error("missing ZeroMQ frame");
    return decodeEnvelope(Buffer.from(frame));
  }), 2_000, label);
}

async function receiveRouter(socket: Router): Promise<{ identity: Buffer; envelope: WireEnvelope }> {
  const frames = await wait(socket.receive());
  const parts = frames as Message[];
  const identity = parts[0];
  const frame = parts.at(-1);
  if (identity === undefined || frame === undefined) throw new Error("missing ROUTER frame");
  return { identity: Buffer.from(identity), envelope: decodeEnvelope(Buffer.from(frame)) };
}

async function waitForPeer(root: ProcessDomainNode, nodeId: string, status: "online" | "offline"): Promise<void> {
  if (root.peers().some((peer) => peer.nodeId === nodeId && peer.status === status)) return;
  await wait(new Promise<void>((resolve) => {
    const stop = root.subscribeEvents((event) => {
      if (event.type === "peer" && event.peer.nodeId === nodeId && event.peer.status === status) {
        stop();
        resolve();
      }
    });
  }), 2_000, `peer ${status}`);
}

interface RawPeer {
  readonly nodeId: string;
  readonly declaration: ProcessDomainDeclaration;
  readonly endpoint: string;
  socket: Dealer;
}

async function bootstrapRaw(declaration: ProcessDomainDeclaration): Promise<RawPeer> {
  const nodeId = randomId();
  const bootstrap = new Dealer({ ...socketTiming, routingId: nodeId });
  bootstrap.connect(declaration.endpoint);
  const clientNonce = randomId();
  await bootstrap.send(encodeEnvelope({
    version: PROCESS_DOMAIN_PROTOCOL,
    type: "challenge-request",
    phase: "bootstrap",
    domainId: declaration.domainId,
    nodeId,
    clientNonce,
  }));
  const challenge = await receive(bootstrap, "bootstrap challenge");
  if (challenge.type !== "challenge") throw new Error("missing bootstrap challenge");
  await bootstrap.send(encodeEnvelope({
    version: PROCESS_DOMAIN_PROTOCOL,
    type: "bootstrap",
    domainId: declaration.domainId,
    nodeId,
    metadata: { role: "raw-test" },
    clientNonce,
    serverNonce: challenge.serverNonce,
    proof: createProof(declaration.capability, "bootstrap", [
      declaration.domainId,
      nodeId,
      { role: "raw-test" },
      clientNonce,
      challenge.serverNonce,
    ]),
  }));
  const ready = await receive(bootstrap, "bootstrap ready");
  bootstrap.close();
  if (ready.type !== "bootstrap-ready") throw new Error("missing bootstrap response");
  const socket = new Dealer({ ...socketTiming, routingId: nodeId });
  socket.connect(ready.endpoint);
  return { nodeId, declaration, endpoint: ready.endpoint, socket };
}

async function authenticateRaw(peer: RawPeer): Promise<WireEnvelope> {
  const clientNonce = randomId();
  await peer.socket.send(encodeEnvelope({
    version: PROCESS_DOMAIN_PROTOCOL,
    type: "challenge-request",
    phase: "hello",
    domainId: peer.declaration.domainId,
    nodeId: peer.nodeId,
    clientNonce,
  }));
  const challenge = await receive(peer.socket, "hello challenge");
  if (challenge.type !== "challenge") throw new Error("missing hello challenge");
  const hello: WireEnvelope = {
    version: PROCESS_DOMAIN_PROTOCOL,
    type: "hello",
    domainId: peer.declaration.domainId,
    nodeId: peer.nodeId,
    clientNonce,
    serverNonce: challenge.serverNonce,
    proof: createProof(peer.declaration.capability, "hello", [
      peer.declaration.domainId,
      peer.nodeId,
      clientNonce,
      challenge.serverNonce,
    ]),
  };
  await peer.socket.send(encodeEnvelope(hello));
  const ready = await receive(peer.socket, "hello ready");
  if (ready.type !== "ready") throw new Error("missing ready response");
  return hello;
}

function ack(socket: Dealer, id: string): Promise<void> {
  return socket.send(encodeEnvelope({ version: PROCESS_DOMAIN_PROTOCOL, type: "ack", id }));
}

describe("process-domain authentication and concurrent sends", () => {
  it("exposes typed and sanitized declaration and connection failures", async () => {
    const malformed = "not-a-process-domain-declaration";
    const declarationFailure = await openProcessDomain({
      env: { [ENV_NAMES.DECLARATION]: malformed },
      ...timing,
    }).then(
      () => null,
      (error: unknown) => error,
    );
    if (!isProcessDomainOpenError(declarationFailure)) throw declarationFailure;
    expect(declarationFailure.code).toBe("INVALID_DECLARATION");
    expect(declarationFailure.message).not.toContain(malformed);

    const unavailable = {
      version: PROCESS_DOMAIN_PROTOCOL,
      domainId: randomId(),
      endpoint: "tcp://127.0.0.1:1",
      capability: randomId(32),
      hostNodeId: randomId(),
    } as const;
    const unavailableEncoded = encodeDeclaration(unavailable);
    const connectionFailure = await openProcessDomain({
      env: { [ENV_NAMES.DECLARATION]: unavailableEncoded },
      ...timing,
      connectTimeoutMs: 100,
    }).then(
      () => null,
      (error: unknown) => error,
    );
    if (!isProcessDomainOpenError(connectionFailure)) throw connectionFailure;
    expect(connectionFailure.code).toBe("CONNECTION_UNAVAILABLE");
    expect(connectionFailure.message).not.toContain(unavailable.endpoint);
    expect(connectionFailure.message).not.toContain(unavailable.capability);
    expect(connectionFailure.message).not.toContain(unavailableEncoded);
  });

  it("binds bootstrap responses to the inherited domain and host", async () => {
    const capability = randomId(32);
    const domainId = randomId();
    const hostNodeId = randomId();
    const bootstrap = new Router({ ...socketTiming, handover: false });
    const bound = await bindTemporaryEndpoint(bootstrap);
    const declaration: ProcessDomainDeclaration = {
      version: PROCESS_DOMAIN_PROTOCOL,
      domainId,
      endpoint: bound.endpoint,
      capability,
      hostNodeId,
    };
    const server = (async () => {
      const request = await receiveRouter(bootstrap);
      if (request.envelope.type !== "challenge-request") throw new Error("missing challenge request");
      const serverNonce = randomId();
      await bootstrap.send([request.identity, encodeEnvelope({
        version: PROCESS_DOMAIN_PROTOCOL,
        type: "challenge",
        phase: "bootstrap",
        domainId,
        nodeId: request.envelope.nodeId,
        clientNonce: request.envelope.clientNonce,
        serverNonce,
      })]);
      const response = await receiveRouter(bootstrap);
      if (response.envelope.type !== "bootstrap") throw new Error("missing bootstrap response");
      const endpoint = "tcp://127.0.0.1:12345";
      const responseDomainId = randomId();
      const responseHostNodeId = randomId();
      const responseServerNonce = randomId();
      await bootstrap.send([response.identity, encodeEnvelope({
        version: PROCESS_DOMAIN_PROTOCOL,
        type: "bootstrap-ready",
        domainId: responseDomainId,
        nodeId: responseHostNodeId,
        endpoint,
        clientNonce: response.envelope.clientNonce,
        serverNonce: responseServerNonce,
        proof: createProof(capability, "bootstrap-ready", [
          domainId,
          responseHostNodeId,
          response.envelope.nodeId,
          endpoint,
          response.envelope.clientNonce,
          responseServerNonce,
        ]),
      })]);
    })();
    try {
      const failure = await openProcessDomain({
        env: { [ENV_NAMES.DECLARATION]: encodeDeclaration(declaration) },
        ...timing,
      }).then(
        () => null,
        (error: unknown) => error,
      );
      if (!isProcessDomainOpenError(failure)) throw failure;
      expect(failure.code).toBe("AUTHENTICATION_FAILED");
      await server;
    } finally {
      bootstrap.close();
    }
  });

  it("ignores stale ready frames without clearing a newer reconnect attempt", async () => {
    const env: NodeJS.ProcessEnv = {};
    const root = await openProcessDomain({ env, ...timing });
    const child = await openProcessDomain({ env: { [ENV_NAMES.DECLARATION]: env[ENV_NAMES.DECLARATION] }, ...timing });
    const runtime = child as unknown as {
      helloClientNonce: string | null;
      helloServerNonce: string | null;
      helloAttemptTimer: ReturnType<typeof setTimeout> | null;
      handleClientEnvelope(envelope: WireEnvelope): Promise<void>;
    };
    const activeClientNonce = randomId();
    const activeServerNonce = randomId();
    const timer = setTimeout(() => {}, 10_000);
    runtime.helloClientNonce = activeClientNonce;
    runtime.helloServerNonce = activeServerNonce;
    runtime.helloAttemptTimer = timer;
    try {
      await runtime.handleClientEnvelope({
        version: PROCESS_DOMAIN_PROTOCOL,
        type: "ready",
        domainId: child.declaration.domainId,
        nodeId: root.nodeId,
        clientNonce: randomId(),
        serverNonce: randomId(),
        proof: createProof(child.declaration.capability, "ready", [
          child.declaration.domainId,
          root.nodeId,
          child.nodeId,
          randomId(),
          randomId(),
        ]),
      });
      expect(runtime.helloClientNonce).toBe(activeClientNonce);
      expect(runtime.helloServerNonce).toBe(activeServerNonce);
      expect(runtime.helloAttemptTimer).toBe(timer);

      await runtime.handleClientEnvelope({
        version: PROCESS_DOMAIN_PROTOCOL,
        type: "ready",
        domainId: randomId(),
        nodeId: root.nodeId,
        clientNonce: activeClientNonce,
        serverNonce: activeServerNonce,
        proof: createProof(child.declaration.capability, "ready", [
          child.declaration.domainId,
          root.nodeId,
          child.nodeId,
          activeClientNonce,
          activeServerNonce,
        ]),
      });
      expect(runtime.helloClientNonce).toBe(activeClientNonce);
      expect(runtime.helloServerNonce).toBe(activeServerNonce);
      expect(runtime.helloAttemptTimer).toBe(timer);
    } finally {
      clearTimeout(timer);
      runtime.helloAttemptTimer = null;
      runtime.helloClientNonce = null;
      runtime.helloServerNonce = null;
      await child.close();
      await root.close();
    }
  });

  it("rejects an incorrect capability without publishing a peer or mutating the declaration", async () => {
    const env: NodeJS.ProcessEnv = {};
    const root = await openProcessDomain({ env, ...timing });
    const published = env[ENV_NAMES.DECLARATION];
    const declaration = decodeDeclaration(published);
    if (declaration === null) throw new Error("missing declaration");
    const badDeclaration = { ...declaration, capability: randomId(32) };
    const encodedBadDeclaration = encodeDeclaration(badDeclaration);
    try {
      const failure = await openProcessDomain({
        env: { [ENV_NAMES.DECLARATION]: encodedBadDeclaration },
        ...timing,
        connectTimeoutMs: 100,
      }).then(
        () => null,
        (error: unknown) => error,
      );
      if (!isProcessDomainOpenError(failure)) throw failure;
      expect(failure.code).toBe("AUTHENTICATION_FAILED");
      expect(failure.message).not.toContain(badDeclaration.capability);
      expect(failure.message).not.toContain(badDeclaration.endpoint);
      expect(failure.message).not.toContain(encodedBadDeclaration);
      expect(root.peers()).toEqual([]);
      expect(env[ENV_NAMES.DECLARATION]).toBe(published);
    } finally {
      await root.close();
    }
    expect(env[ENV_NAMES.DECLARATION]).toBeUndefined();
  });

  it("requires hello authentication and rejects replay across connections", async () => {
    const env: NodeJS.ProcessEnv = {};
    const root = await openProcessDomain({ env, ...timing });
    const declaration = decodeDeclaration(env[ENV_NAMES.DECLARATION]);
    if (declaration === null) throw new Error("missing declaration");
    const peer = await bootstrapRaw(declaration);
    let injected = false;
    const stop = root.subscribe("raw-injection", () => { injected = true; });
    try {
      await peer.socket.send(encodeEnvelope({
        version: PROCESS_DOMAIN_PROTOCOL,
        type: "data",
        id: randomId(),
        channel: "raw-injection",
        value: "before-hello",
        senderId: peer.nodeId,
        targetId: root.nodeId,
      }));
      await new Promise((resolve) => setTimeout(resolve, 50));
      expect(injected).toBe(false);

      const oldHello = await authenticateRaw(peer);
      await waitForPeer(root, peer.nodeId, "online");
      const offline = waitForPeer(root, peer.nodeId, "offline");
      peer.socket.close();
      await offline;

      peer.socket = new Dealer({ ...socketTiming, routingId: peer.nodeId });
      const reconnected = wait(new Promise<void>((resolve) => {
        peer.socket.events.on("handshake", () => resolve());
      }), 2_000, "reconnect handshake");
      peer.socket.connect(peer.endpoint);
      await reconnected;
      await peer.socket.send(encodeEnvelope(oldHello));
      await new Promise((resolve) => setTimeout(resolve, 50));
      expect(root.peers()).toEqual([expect.objectContaining({ nodeId: peer.nodeId, status: "offline" })]);

      await authenticateRaw(peer);
      await waitForPeer(root, peer.nodeId, "online");
    } finally {
      stop();
      peer.socket.close();
      await root.close();
    }
  });

  it("serializes only socket sends while ACKs resolve independently", async () => {
    const env: NodeJS.ProcessEnv = {};
    const root = await openProcessDomain({ env, ...timing });
    const declaration = decodeDeclaration(env[ENV_NAMES.DECLARATION]);
    if (declaration === null) throw new Error("missing declaration");
    const peer = await bootstrapRaw(declaration);
    await authenticateRaw(peer);
    await waitForPeer(root, peer.nodeId, "online");
    try {
      let firstResolved = false;
      const first = root.send(peer.nodeId, "ordered", 1).then(() => { firstResolved = true; });
      const firstEnvelope = await receive(peer.socket);
      if (firstEnvelope.type !== "data") throw new Error("missing first data");
      const second = root.send(peer.nodeId, "ordered", 2);
      const secondEnvelope = await receive(peer.socket);
      if (secondEnvelope.type !== "data") throw new Error("missing second data");
      await ack(peer.socket, secondEnvelope.id);
      await wait(second);
      expect(firstResolved).toBe(false);
      await ack(peer.socket, firstEnvelope.id);
      await wait(first);

      const timedOut = root.send(peer.nodeId, "ordered", 3, { timeoutMs: 100 }).then(
        () => null,
        (error: unknown) => error,
      );
      const thirdEnvelope = await receive(peer.socket);
      if (thirdEnvelope.type !== "data") throw new Error("missing third data");
      const fourth = root.send(peer.nodeId, "ordered", 4, { timeoutMs: 500 });
      const fourthEnvelope = await receive(peer.socket);
      if (fourthEnvelope.type !== "data") throw new Error("missing fourth data");
      await ack(peer.socket, fourthEnvelope.id);
      await wait(fourth);
      await expect(timedOut).resolves.toEqual(expect.objectContaining({ message: "process-domain acknowledgement timed out" }));
    } finally {
      peer.socket.close();
      await root.close();
    }
  });

  it("rejects pending sends when the peer disconnects", async () => {
    const env: NodeJS.ProcessEnv = {};
    const root = await openProcessDomain({ env, ...timing });
    const declaration = decodeDeclaration(env[ENV_NAMES.DECLARATION]);
    if (declaration === null) throw new Error("missing declaration");
    const peer = await bootstrapRaw(declaration);
    await authenticateRaw(peer);
    await waitForPeer(root, peer.nodeId, "online");
    try {
      const pending = root.send(peer.nodeId, "disconnect", true, { timeoutMs: 5_000 });
      const envelope = await receive(peer.socket);
      if (envelope.type !== "data") throw new Error("missing pending data");
      peer.socket.close();
      await expect(wait(pending)).rejects.toThrow("disconnected");
    } finally {
      if (!peer.socket.closed) peer.socket.close();
      await root.close();
    }
  });
});
