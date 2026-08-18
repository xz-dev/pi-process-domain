import { describe, expect, it } from "vitest";
import {
  ENV_NAMES,
  isProcessDomainOpenError,
  openProcessDomain,
} from "../src/process-domain/index.js";
import { connectLoopback, listenLoopback, type FrameLink } from "../src/process-domain/net.js";
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

function wait<T>(promise: Promise<T>, timeoutMs = 2_000, label = "operation"): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`test wait timed out: ${label}`)), timeoutMs);
    promise.then(
      (value) => { clearTimeout(timer); resolve(value); },
      (error) => { clearTimeout(timer); reject(error); },
    );
  });
}

/** A minimal framed client speaking the wire protocol, auto-answering liveness pings. */
interface RawPeer {
  readonly nodeId: string;
  readonly declaration: ProcessDomainDeclaration;
  readonly link: FrameLink;
  /** Resolves once the link reports closure (host-initiated or local). */
  readonly closed: Promise<void>;
  send(envelope: WireEnvelope): Promise<void>;
  receive(label?: string): Promise<WireEnvelope>;
}

function connectRaw(declaration: ProcessDomainDeclaration, nodeId = randomId()): Promise<RawPeer> {
  return connectLoopback(declaration.endpoint, 2_000, "raw connect timed out").then((link) => {
    const inbox: WireEnvelope[] = [];
    const waiters: Array<{ resolve: (envelope: WireEnvelope) => void; reject: (error: Error) => void }> = [];
    link.onFrame = (frame) => {
      let envelope: WireEnvelope;
      try {
        envelope = decodeEnvelope(frame);
      } catch {
        return;
      }
      if (envelope.type === "ping") {
        void link.send(encodeEnvelope({ version: PROCESS_DOMAIN_PROTOCOL, type: "pong", id: envelope.id }));
        return;
      }
      const waiter = waiters.shift();
      if (waiter !== undefined) waiter.resolve(envelope);
      else inbox.push(envelope);
    };
    let markClosed!: () => void;
    const closed = new Promise<void>((resolve) => { markClosed = resolve; });
    link.onClose = () => {
      markClosed();
      for (const waiter of waiters.splice(0)) waiter.reject(new Error("raw link closed"));
    };
    link.onError = () => {};
    return {
      nodeId,
      declaration,
      link,
      closed,
      send(envelope: WireEnvelope): Promise<void> {
        return link.send(encodeEnvelope(envelope));
      },
      receive(label = "raw receive"): Promise<WireEnvelope> {
        const queued = inbox.shift();
        if (queued !== undefined) return wait(Promise.resolve(queued), 2_000, label);
        return wait(new Promise<WireEnvelope>((resolve, reject) => {
          waiters.push({ resolve, reject });
        }), 2_000, label);
      },
    };
  });
}

async function authenticateRaw(peer: RawPeer, metadata: Readonly<Record<string, string>> = { role: "raw-test" }): Promise<WireEnvelope> {
  const clientNonce = randomId();
  await peer.send({
    version: PROCESS_DOMAIN_PROTOCOL,
    type: "challenge-request",
    phase: "bootstrap",
    domainId: peer.declaration.domainId,
    nodeId: peer.nodeId,
    clientNonce,
  });
  const challenge = await peer.receive("bootstrap challenge");
  if (challenge.type !== "challenge") throw new Error("missing bootstrap challenge");
  const bootstrap: WireEnvelope = {
    version: PROCESS_DOMAIN_PROTOCOL,
    type: "bootstrap",
    domainId: peer.declaration.domainId,
    nodeId: peer.nodeId,
    metadata,
    clientNonce,
    serverNonce: challenge.serverNonce,
    proof: createProof(peer.declaration.capability, "bootstrap", [
      peer.declaration.domainId,
      peer.nodeId,
      metadata,
      clientNonce,
      challenge.serverNonce,
    ]),
  };
  await peer.send(bootstrap);
  const ready = await peer.receive("bootstrap ready");
  if (ready.type !== "ready") throw new Error("missing ready response");
  return bootstrap;
}

function ack(peer: RawPeer, id: string): Promise<void> {
  return peer.send({ version: PROCESS_DOMAIN_PROTOCOL, type: "ack", id });
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

  it("binds ready responses to the inherited domain and host", async () => {
    const capability = randomId(32);
    const domainId = randomId();
    const hostNodeId = randomId();
    const server = await listenLoopback();
    const declaration: ProcessDomainDeclaration = {
      version: PROCESS_DOMAIN_PROTOCOL,
      domainId,
      endpoint: server.endpoint,
      capability,
      hostNodeId,
    };
    const serverDone = new Promise<void>((resolve, reject) => {
      server.onConnection = (link) => {
        let step = 0;
        let clientNonce = "";
        let peerNodeId = "";
        link.onFrame = (frame) => {
          void (async () => {
            const envelope = decodeEnvelope(frame);
            if (step === 0) {
              if (envelope.type !== "challenge-request") throw new Error("missing challenge request");
              step = 1;
              clientNonce = envelope.clientNonce;
              peerNodeId = envelope.nodeId;
              await link.send(encodeEnvelope({
                version: PROCESS_DOMAIN_PROTOCOL,
                type: "challenge",
                phase: "bootstrap",
                domainId,
                nodeId: envelope.nodeId,
                clientNonce,
                serverNonce: randomId(),
              }));
              return;
            }
            if (envelope.type !== "bootstrap") throw new Error("missing bootstrap");
            const responseDomainId = randomId();
            const responseHostNodeId = randomId();
            const responseServerNonce = randomId();
            await link.send(encodeEnvelope({
              version: PROCESS_DOMAIN_PROTOCOL,
              type: "ready",
              domainId: responseDomainId,
              nodeId: responseHostNodeId,
              clientNonce,
              serverNonce: responseServerNonce,
              proof: createProof(capability, "ready", [
                domainId,
                responseHostNodeId,
                peerNodeId,
                clientNonce,
                responseServerNonce,
              ]),
            }));
            resolve();
          })().catch(reject);
        };
        link.onError = () => {};
        link.onClose = () => {};
      };
    });
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
      await serverDone;
    } finally {
      await server.close();
    }
  });

  it("ignores stray control frames after authentication", async () => {
    const env: NodeJS.ProcessEnv = {};
    const root = await openProcessDomain({ env, ...timing });
    const child = await openProcessDomain({ env: { [ENV_NAMES.DECLARATION]: env[ENV_NAMES.DECLARATION] }, ...timing });
    const runtime = child as unknown as {
      peersMap: Map<string, unknown>;
      handleClientEnvelope(peer: unknown, envelope: WireEnvelope): Promise<void>;
    };
    const hostPeer = runtime.peersMap.get(root.nodeId);
    if (hostPeer === undefined) throw new Error("missing host peer");
    const events: string[] = [];
    const stop = child.subscribeEvents((event) => events.push(event.type));
    try {
      const forged: WireEnvelope = {
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
      };
      await runtime.handleClientEnvelope(hostPeer, forged);
      await runtime.handleClientEnvelope(hostPeer, {
        version: PROCESS_DOMAIN_PROTOCOL,
        type: "challenge",
        phase: "hello",
        domainId: child.declaration.domainId,
        nodeId: child.nodeId,
        clientNonce: randomId(),
        serverNonce: randomId(),
      });
      await runtime.handleClientEnvelope(hostPeer, {
        version: PROCESS_DOMAIN_PROTOCOL,
        type: "ack",
        id: randomId(),
      });
      expect(child.peers()).toEqual([expect.objectContaining({ nodeId: root.nodeId, status: "online" })]);
      expect(events).toEqual([]);
    } finally {
      stop();
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

  it("requires authentication and rejects replay across connections", async () => {
    const env: NodeJS.ProcessEnv = {};
    const root = await openProcessDomain({ env, ...timing });
    const declaration = decodeDeclaration(env[ENV_NAMES.DECLARATION]);
    if (declaration === null) throw new Error("missing declaration");
    const injector = await connectRaw(declaration);
    const peer = await connectRaw(declaration);
    let injected = false;
    const stop = root.subscribe("raw-injection", () => { injected = true; });
    try {
      await injector.send({
        version: PROCESS_DOMAIN_PROTOCOL,
        type: "data",
        id: randomId(),
        channel: "raw-injection",
        value: "before-auth",
        senderId: injector.nodeId,
        targetId: root.nodeId,
      });
      await new Promise((resolve) => setTimeout(resolve, 50));
      expect(injected).toBe(false);
      // Fail-closed: an unauthenticated connection sending data is dropped.
      await wait(injector.closed, 2_000, "injector close");
      await expect(injector.send({
        version: PROCESS_DOMAIN_PROTOCOL,
        type: "data",
        id: randomId(),
        channel: "raw-injection",
        value: "after-drop",
        senderId: injector.nodeId,
        targetId: root.nodeId,
      })).rejects.toThrow("closed");
      injector.link.close();
      expect(root.peers()).toEqual([]);

      const staleBootstrap = await authenticateRaw(peer);
      await waitForPeer(root, peer.nodeId, "online");
      const offline = waitForPeer(root, peer.nodeId, "offline");
      peer.link.close();
      await offline;

      // Replaying the consumed bootstrap on a fresh connection must not authenticate.
      const replay = await connectRaw(declaration, peer.nodeId);
      try {
        await replay.send(staleBootstrap);
        await wait(replay.closed, 2_000, "replay drop");
        expect(root.peers()).toEqual([expect.objectContaining({ nodeId: peer.nodeId, status: "offline" })]);
      } finally {
        replay.link.close();
      }

      // Even with a fresh challenge, stale nonces must not validate.
      const replayWithChallenge = await connectRaw(declaration, peer.nodeId);
      try {
        await replayWithChallenge.send({
          version: PROCESS_DOMAIN_PROTOCOL,
          type: "challenge-request",
          phase: "bootstrap",
          domainId: declaration.domainId,
          nodeId: peer.nodeId,
          clientNonce: randomId(),
        });
        await replayWithChallenge.receive("replay challenge");
        await replayWithChallenge.send(staleBootstrap);
        await wait(replayWithChallenge.closed, 2_000, "replay-with-challenge drop");
        expect(root.peers()).toEqual([expect.objectContaining({ nodeId: peer.nodeId, status: "offline" })]);
      } finally {
        replayWithChallenge.link.close();
      }

      const replacement = await connectRaw(declaration, peer.nodeId);
      try {
        await authenticateRaw(replacement);
        await waitForPeer(root, peer.nodeId, "online");
      } finally {
        replacement.link.close();
      }
    } finally {
      stop();
      await root.close();
    }
  });

  it("serializes only socket sends while ACKs resolve independently", async () => {
    const env: NodeJS.ProcessEnv = {};
    const root = await openProcessDomain({ env, ...timing });
    const declaration = decodeDeclaration(env[ENV_NAMES.DECLARATION]);
    if (declaration === null) throw new Error("missing declaration");
    const peer = await connectRaw(declaration);
    await authenticateRaw(peer);
    await waitForPeer(root, peer.nodeId, "online");
    try {
      let firstResolved = false;
      const first = root.send(peer.nodeId, "ordered", 1).then(() => { firstResolved = true; });
      const firstEnvelope = await peer.receive("first data");
      if (firstEnvelope.type !== "data") throw new Error("missing first data");
      const second = root.send(peer.nodeId, "ordered", 2);
      const secondEnvelope = await peer.receive("second data");
      if (secondEnvelope.type !== "data") throw new Error("missing second data");
      await ack(peer, secondEnvelope.id);
      await wait(second);
      expect(firstResolved).toBe(false);
      await ack(peer, firstEnvelope.id);
      await wait(first);

      const timedOut = root.send(peer.nodeId, "ordered", 3, { timeoutMs: 100 }).then(
        () => null,
        (error: unknown) => error,
      );
      const thirdEnvelope = await peer.receive("third data");
      if (thirdEnvelope.type !== "data") throw new Error("missing third data");
      const fourth = root.send(peer.nodeId, "ordered", 4, { timeoutMs: 500 });
      const fourthEnvelope = await peer.receive("fourth data");
      if (fourthEnvelope.type !== "data") throw new Error("missing fourth data");
      await ack(peer, fourthEnvelope.id);
      await wait(fourth);
      await expect(timedOut).resolves.toEqual(expect.objectContaining({ message: "process-domain acknowledgement timed out" }));
    } finally {
      peer.link.close();
      await root.close();
    }
  });

  it("rejects pending sends when the peer disconnects", async () => {
    const env: NodeJS.ProcessEnv = {};
    const root = await openProcessDomain({ env, ...timing });
    const declaration = decodeDeclaration(env[ENV_NAMES.DECLARATION]);
    if (declaration === null) throw new Error("missing declaration");
    const peer = await connectRaw(declaration);
    await authenticateRaw(peer);
    await waitForPeer(root, peer.nodeId, "online");
    try {
      const pending = root.send(peer.nodeId, "disconnect", true, { timeoutMs: 5_000 });
      const envelope = await peer.receive("pending data");
      if (envelope.type !== "data") throw new Error("missing pending data");
      peer.link.close();
      await expect(wait(pending)).rejects.toThrow("disconnected");
    } finally {
      peer.link.close();
      await root.close();
    }
  });
});
