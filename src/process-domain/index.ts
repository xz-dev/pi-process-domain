import { Dealer, Router, type Message } from "zeromq";
import { bindTemporaryEndpoint } from "./endpoint.js";
import {
  authenticationFailedError,
  connectionUnavailableError,
  invalidDeclarationError,
  isProcessDomainOpenError,
} from "./errors.js";
import {
  createProof,
  decodeDeclaration,
  decodeEnvelope,
  encodeDeclaration,
  encodeEnvelope,
  isValidChannel,
  isValidId,
  randomId,
  verifyProof,
  ENV_NAMES,
  type WireEnvelope,
} from "./protocol.js";
import {
  PROCESS_DOMAIN_PROTOCOL,
  type OpenProcessDomainOptions,
  type PiLifecycleEvent,
  type PiLifecycleExtensionApi,
  type ProcessDomainDataMessage,
  type ProcessDomainDeclaration,
  type ProcessDomainEvent,
  type ProcessDomainNode,
  type ProcessDomainPeer,
  type ProcessDomainTransport,
} from "./types.js";

export * from "./types.js";
export {
  isProcessDomainOpenError,
  ProcessDomainOpenError,
  type ProcessDomainOpenErrorCode,
} from "./errors.js";
export { ENV_NAMES } from "./protocol.js";
export { preferredTransport, wildcardEndpoint } from "./endpoint.js";

const DEFAULT_CONNECT_TIMEOUT_MS = 10_000;
const DEFAULT_HEARTBEAT_INTERVAL_MS = 2_000;
const DEFAULT_HEARTBEAT_TIMEOUT_MS = 6_000;
const DEFAULT_HEARTBEAT_TTL_MS = 5_000;
const AUTH_CHALLENGE_TTL_MS = 10_000;
const MAX_BOOTSTRAP_CHALLENGES = 1_024;
const MAX_BOOTSTRAP_SENDS = 1_024;

type PendingSend = { resolve: () => void; reject: (error: Error) => void; timer?: ReturnType<typeof setTimeout> };

interface AuthChallenge {
  readonly clientNonce: string;
  readonly serverNonce: string;
  readonly expiresAt: number;
  readonly expiryTimer?: ReturnType<typeof setTimeout>;
}

interface InternalPeer {
  readonly nodeId: string;
  status: "online" | "offline";
  readonly metadata: Readonly<Record<string, string>>;
  readonly connectedAt: number;
  disconnectedAt?: number;
  readonly socket: Dealer | Router;
  readonly endpoint: string;
  readonly pending: Map<string, PendingSend>;
  sendTail: Promise<void>;
  challenge?: AuthChallenge;
  everOnline: boolean;
  initialAuthTimer?: ReturnType<typeof setTimeout>;
}

function asError(error: unknown): Error {
  return error instanceof Error ? error : new Error(String(error));
}

function withTimeout<T>(promise: Promise<T>, timeoutMs: number, message: string): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(message)), timeoutMs);
    promise.then(
      (value) => {
        clearTimeout(timer);
        resolve(value);
      },
      (error) => {
        clearTimeout(timer);
        reject(error);
      },
    );
  });
}

function requireValidOptions(options: OpenProcessDomainOptions): Required<Pick<OpenProcessDomainOptions, "connectTimeoutMs" | "heartbeatIntervalMs" | "heartbeatTimeoutMs" | "heartbeatTimeToLiveMs">> {
  const values = {
    connectTimeoutMs: options.connectTimeoutMs ?? DEFAULT_CONNECT_TIMEOUT_MS,
    heartbeatIntervalMs: options.heartbeatIntervalMs ?? DEFAULT_HEARTBEAT_INTERVAL_MS,
    heartbeatTimeoutMs: options.heartbeatTimeoutMs ?? DEFAULT_HEARTBEAT_TIMEOUT_MS,
    heartbeatTimeToLiveMs: options.heartbeatTimeToLiveMs ?? DEFAULT_HEARTBEAT_TTL_MS,
  };
  for (const value of Object.values(values)) {
    if (!Number.isSafeInteger(value) || value <= 0) throw new RangeError("process-domain timing options must be positive safe integers");
  }
  if (values.heartbeatTimeToLiveMs < 100) throw new RangeError("heartbeatTimeToLiveMs must be at least 100ms");
  return values;
}

function socketOptions(options: OpenProcessDomainOptions): Record<string, number> {
  const values = requireValidOptions(options);
  return {
    heartbeatInterval: values.heartbeatIntervalMs,
    heartbeatTimeout: values.heartbeatTimeoutMs,
    heartbeatTimeToLive: values.heartbeatTimeToLiveMs,
    linger: 0,
  };
}

function framesToEnvelope(frames: readonly Message[]): WireEnvelope {
  const frame = frames.length === 1 ? frames[0] : frames.at(-1);
  if (frame === undefined) throw new TypeError("empty ZeroMQ message");
  return decodeEnvelope(Buffer.from(frame));
}

function peerInfo(peer: InternalPeer, status: "online" | "offline"): ProcessDomainPeer {
  return {
    nodeId: peer.nodeId,
    status,
    metadata: peer.metadata,
    connectedAt: peer.connectedAt,
    ...(peer.disconnectedAt === undefined ? {} : { disconnectedAt: peer.disconnectedAt }),
  };
}

function createDeclaration(endpoint: string, hostNodeId: string): { declaration: ProcessDomainDeclaration; capability: string } {
  const capability = randomId(32);
  return {
    capability,
    declaration: {
      version: PROCESS_DOMAIN_PROTOCOL,
      domainId: randomId(16),
      endpoint,
      capability,
      hostNodeId,
    },
  };
}

class DomainRuntime implements ProcessDomainNode {
  readonly peersMap = new Map<string, InternalPeer>();
  readonly subscribers = new Map<string, Set<(message: ProcessDomainDataMessage) => void>>();
  readonly eventListeners = new Set<(event: ProcessDomainEvent) => void>();
  readonly nodeId: string;
  readonly role: "host" | "client";
  readonly declaration: ProcessDomainDeclaration;
  readonly transport: ProcessDomainTransport;
  readonly endpoint: string;
  private closed = false;
  private bootstrap: Router | null = null;
  private bootstrapSendTail: Promise<void> = Promise.resolve();
  private bootstrapQueuedSends = 0;
  private ownSocket: Dealer | null = null;
  private hostChannels = new Map<string, Router>();
  private nodeChannelEndpoint: string | null = null;
  private readonly bootstrapChallenges = new Map<string, AuthChallenge>();
  private helloClientNonce: string | null = null;
  private helloServerNonce: string | null = null;
  private helloAttemptTimer: ReturnType<typeof setTimeout> | null = null;
  declarationEnv: NodeJS.ProcessEnv | null = null;
  publishedDeclaration: string | null = null;

  private constructor(
    role: "host" | "client",
    nodeId: string,
    declaration: ProcessDomainDeclaration,
    transport: ProcessDomainTransport,
    private readonly options: OpenProcessDomainOptions,
    private readonly metadata: Readonly<Record<string, string>>,
  ) {
    this.role = role;
    this.nodeId = nodeId;
    this.declaration = declaration;
    this.transport = transport;
    this.endpoint = declaration.endpoint;
  }

  static async host(options: OpenProcessDomainOptions, metadata: Readonly<Record<string, string>>): Promise<DomainRuntime> {
    const nodeId = randomId();
    const bootstrap = new Router({ ...socketOptions(options), handover: false });
    const bound = await bindTemporaryEndpoint(bootstrap);
    const { declaration } = createDeclaration(bound.endpoint, nodeId);
    const runtime = new DomainRuntime("host", nodeId, declaration, bound.transport, options, metadata);
    runtime.bootstrap = bootstrap;
    runtime.installHostBootstrap();
    runtime.emit({ type: "transport", transport: bound.transport, endpoint: bound.endpoint });
    return runtime;
  }

  static async client(declaration: ProcessDomainDeclaration, options: OpenProcessDomainOptions, metadata: Readonly<Record<string, string>>): Promise<DomainRuntime> {
    const runtime = new DomainRuntime("client", randomId(), declaration, declaration.endpoint.startsWith("ipc://") ? "ipc" : "tcp-loopback", options, metadata);
    const bootstrap = new Dealer({ ...socketOptions(options), routingId: runtime.nodeId });
    let channel: Dealer | null = null;
    let startupFailure: "connection" | "authentication" = "connection";
    try {
      bootstrap.connect(declaration.endpoint);
    const clientNonce = randomId();
    await runtime.sendOnSocket(bootstrap, {
      version: PROCESS_DOMAIN_PROTOCOL,
      type: "challenge-request",
      phase: "bootstrap",
      domainId: declaration.domainId,
      nodeId: runtime.nodeId,
      clientNonce,
    }, options.connectTimeoutMs ?? DEFAULT_CONNECT_TIMEOUT_MS);
    const challenge = await runtime.receiveOne(bootstrap, options.connectTimeoutMs ?? DEFAULT_CONNECT_TIMEOUT_MS);
    if (
      challenge.type !== "challenge" ||
      challenge.phase !== "bootstrap" ||
      challenge.domainId !== declaration.domainId ||
      challenge.nodeId !== runtime.nodeId ||
      challenge.clientNonce !== clientNonce
    ) throw authenticationFailedError(new Error("invalid bootstrap challenge"));
    await runtime.sendOnSocket(bootstrap, {
      version: PROCESS_DOMAIN_PROTOCOL,
      type: "bootstrap",
      domainId: declaration.domainId,
      nodeId: runtime.nodeId,
      metadata,
      clientNonce,
      serverNonce: challenge.serverNonce,
      proof: createProof(declaration.capability, "bootstrap", [
        declaration.domainId,
        runtime.nodeId,
        metadata,
        clientNonce,
        challenge.serverNonce,
      ]),
    }, options.connectTimeoutMs ?? DEFAULT_CONNECT_TIMEOUT_MS);
    startupFailure = "authentication";
    const response = await runtime.receiveOne(bootstrap, options.connectTimeoutMs ?? DEFAULT_CONNECT_TIMEOUT_MS);
    if (
      response.type !== "bootstrap-ready" ||
      response.domainId !== declaration.domainId ||
      response.nodeId !== declaration.hostNodeId ||
      response.clientNonce !== clientNonce ||
      !verifyProof(declaration.capability, "bootstrap-ready", [
        declaration.domainId,
        declaration.hostNodeId,
        runtime.nodeId,
        response.endpoint,
        clientNonce,
        response.serverNonce,
      ], response.proof)
    ) throw authenticationFailedError(new Error("invalid bootstrap response"));
    startupFailure = "connection";
    bootstrap.close();
    runtime.nodeChannelEndpoint = response.endpoint;
    const nodeChannel = new Dealer({ ...socketOptions(options), routingId: runtime.nodeId });
    channel = nodeChannel;
    runtime.ownSocket = nodeChannel;
    runtime.peersMap.set(declaration.hostNodeId, { nodeId: declaration.hostNodeId, status: "offline", metadata: {}, connectedAt: Date.now(), socket: nodeChannel, endpoint: response.endpoint, pending: new Map(), sendTail: Promise.resolve(), everOnline: false });
    nodeChannel.events.on("disconnect", () => runtime.markOffline());
    nodeChannel.connect(response.endpoint);
    const initialHello = await runtime.beginHello(nodeChannel);
    const { ready, clientNonce: helloClientNonce, serverNonce: helloServerNonce } = initialHello;
    if (
      ready.type !== "ready" ||
      ready.domainId !== declaration.domainId ||
      ready.nodeId !== declaration.hostNodeId ||
      ready.clientNonce !== helloClientNonce ||
      ready.serverNonce !== helloServerNonce ||
      !verifyProof(declaration.capability, "ready", [
        declaration.domainId,
        declaration.hostNodeId,
        runtime.nodeId,
        helloClientNonce,
        helloServerNonce,
      ], ready.proof)
    ) throw authenticationFailedError(new Error("invalid node-channel response"));
    runtime.markOnline();
    void runtime.readLoop(nodeChannel, (_identity, envelope) => runtime.handleClientEnvelope(envelope));
    nodeChannel.events.on("handshake", () => {
      const host = runtime.peersMap.get(declaration.hostNodeId);
      if (host?.status !== "offline" || runtime.helloClientNonce !== null) return;
      void runtime.requestHelloChallenge(nodeChannel).catch((error) => options.onError?.(asError(error)));
    });
      runtime.emit({ type: "transport", transport: runtime.transport, endpoint: response.endpoint });
      return runtime;
    } catch (error) {
      if (!bootstrap.closed) bootstrap.close();
      if (channel !== null && !channel.closed) channel.close();
      runtime.ownSocket = null;
      if (isProcessDomainOpenError(error)) throw error;
      throw startupFailure === "authentication"
        ? authenticationFailedError(error)
        : connectionUnavailableError(error);
    }
  }

  private installHostBootstrap(): void {
    const socket = this.bootstrap!;
    void this.readLoop(socket, async (identity, envelope) => {
      const routingId = identity.toString();
      if (
        envelope.type === "challenge-request" &&
        envelope.phase === "bootstrap" &&
        envelope.domainId === this.declaration.domainId &&
        isValidId(envelope.nodeId) &&
        routingId === envelope.nodeId &&
        isValidId(envelope.clientNonce)
      ) {
        if (!this.bootstrapChallenges.has(routingId) && this.bootstrapChallenges.size >= MAX_BOOTSTRAP_CHALLENGES) return;
        const previous = this.bootstrapChallenges.get(routingId);
        if (previous !== undefined && previous.expiresAt >= Date.now()) return;
        if (previous?.expiryTimer) clearTimeout(previous.expiryTimer);
        const challenge = {
          clientNonce: envelope.clientNonce,
          serverNonce: randomId(),
          expiresAt: Date.now() + AUTH_CHALLENGE_TTL_MS,
        };
        const expiryTimer = setTimeout(() => {
          const current = this.bootstrapChallenges.get(routingId);
          if (current?.serverNonce === challenge.serverNonce) this.bootstrapChallenges.delete(routingId);
        }, AUTH_CHALLENGE_TTL_MS);
        this.bootstrapChallenges.set(routingId, { ...challenge, expiryTimer });
        await this.queueBootstrapSend(identity, {
          version: PROCESS_DOMAIN_PROTOCOL,
          type: "challenge",
          phase: "bootstrap",
          domainId: this.declaration.domainId,
          nodeId: envelope.nodeId,
          clientNonce: challenge.clientNonce,
          serverNonce: challenge.serverNonce,
        });
        return;
      }
      if (
        envelope.type !== "bootstrap" ||
        envelope.domainId !== this.declaration.domainId ||
        !isValidId(envelope.nodeId) ||
        routingId !== envelope.nodeId
      ) return;
      const challenge = this.bootstrapChallenges.get(routingId);
      this.bootstrapChallenges.delete(routingId);
      if (challenge?.expiryTimer) clearTimeout(challenge.expiryTimer);
      if (
        challenge === undefined ||
        challenge.expiresAt < Date.now() ||
        challenge.clientNonce !== envelope.clientNonce ||
        challenge.serverNonce !== envelope.serverNonce ||
        !verifyProof(this.declaration.capability, "bootstrap", [
          envelope.domainId,
          envelope.nodeId,
          envelope.metadata,
          envelope.clientNonce,
          envelope.serverNonce,
        ], envelope.proof)
      ) return;
      await this.acceptClient(identity, envelope.nodeId, envelope.metadata, envelope.clientNonce);
    });
  }

  private async acceptClient(identity: Buffer, nodeId: string, metadata: Readonly<Record<string, string>>, clientNonce: string): Promise<void> {
    if (this.peersMap.has(nodeId)) return;
    const channel = new Router({ ...socketOptions(this.options), handover: false });
    const bound = await bindTemporaryEndpoint(channel, this.transport);
    const peer: InternalPeer = { nodeId, status: "offline", metadata, connectedAt: Date.now(), socket: channel, endpoint: bound.endpoint, pending: new Map(), sendTail: Promise.resolve(), everOnline: false };
    const serverNonce = randomId();
    this.hostChannels.set(nodeId, channel);
    this.peersMap.set(nodeId, peer);
    void this.readLoop(channel, (routingId, envelope) => this.handleHostEnvelope(peer, routingId, envelope));
    channel.events.on("disconnect", () => this.markPeerOffline(nodeId));
    try {
      await this.queueBootstrapSend(identity, {
        version: PROCESS_DOMAIN_PROTOCOL,
        type: "bootstrap-ready",
        domainId: this.declaration.domainId,
        nodeId: this.nodeId,
        endpoint: bound.endpoint,
        clientNonce,
        serverNonce,
        proof: createProof(this.declaration.capability, "bootstrap-ready", [
          this.declaration.domainId,
          this.nodeId,
          nodeId,
          bound.endpoint,
          clientNonce,
          serverNonce,
        ]),
      });
      peer.initialAuthTimer = setTimeout(() => {
        if (!peer.everOnline) this.removePeer(nodeId, peer);
      }, this.options.connectTimeoutMs ?? DEFAULT_CONNECT_TIMEOUT_MS);
    } catch (error) {
      this.removePeer(nodeId, peer);
      throw error;
    }
  }

  private requestHelloChallenge(channel: Dealer): Promise<void> {
    this.clearHelloAttempt();
    const clientNonce = randomId();
    this.helloClientNonce = clientNonce;
    const timeoutMs = this.options.connectTimeoutMs ?? DEFAULT_CONNECT_TIMEOUT_MS;
    const send = this.sendOnSocket(channel, {
      version: PROCESS_DOMAIN_PROTOCOL,
      type: "challenge-request",
      phase: "hello",
      domainId: this.declaration.domainId,
      nodeId: this.nodeId,
      clientNonce,
    }, timeoutMs);
    this.helloAttemptTimer = setTimeout(() => {
      this.helloAttemptTimer = null;
      this.helloClientNonce = null;
      this.helloServerNonce = null;
      const peer = this.peersMap.get(this.declaration.hostNodeId);
      if (!this.closed && peer?.status === "offline") {
        void this.requestHelloChallenge(channel).catch((error) => this.options.onError?.(asError(error)));
      }
    }, timeoutMs);
    return send;
  }

  private clearHelloAttempt(): void {
    if (this.helloAttemptTimer !== null) clearTimeout(this.helloAttemptTimer);
    this.helloAttemptTimer = null;
    this.helloClientNonce = null;
    this.helloServerNonce = null;
  }

  private async beginHello(channel: Dealer): Promise<{ readonly ready: WireEnvelope; readonly clientNonce: string; readonly serverNonce: string }> {
    await this.requestHelloChallenge(channel);
    const clientNonce = this.helloClientNonce;
    if (clientNonce === null) {
      throw connectionUnavailableError(new Error("hello state was lost"));
    }
    try {
      let challenge: WireEnvelope;
      try {
        challenge = await this.receiveOne(channel, this.options.connectTimeoutMs ?? DEFAULT_CONNECT_TIMEOUT_MS);
      } catch (error) {
        throw connectionUnavailableError(error);
      }
      if (
        challenge.type !== "challenge" ||
        challenge.phase !== "hello" ||
        challenge.domainId !== this.declaration.domainId ||
        challenge.nodeId !== this.nodeId ||
        challenge.clientNonce !== clientNonce
      ) throw authenticationFailedError(new Error("invalid hello challenge"));
      this.helloServerNonce = challenge.serverNonce;
      try {
        await this.sendOnSocket(channel, {
          version: PROCESS_DOMAIN_PROTOCOL,
          type: "hello",
          domainId: this.declaration.domainId,
          nodeId: this.nodeId,
          clientNonce,
          serverNonce: challenge.serverNonce,
          proof: createProof(this.declaration.capability, "hello", [
            this.declaration.domainId,
            this.nodeId,
            clientNonce,
            challenge.serverNonce,
          ]),
        }, this.options.connectTimeoutMs ?? DEFAULT_CONNECT_TIMEOUT_MS);
      } catch (error) {
        throw connectionUnavailableError(error);
      }
      let ready: WireEnvelope;
      try {
        ready = await this.receiveOne(channel, this.options.connectTimeoutMs ?? DEFAULT_CONNECT_TIMEOUT_MS);
      } catch (error) {
        throw authenticationFailedError(error);
      }
      return { ready, clientNonce, serverNonce: challenge.serverNonce };
    } finally {
      this.clearHelloAttempt();
    }
  }

  private async handleClientEnvelope(envelope: WireEnvelope): Promise<void> {
    if (
      envelope.type === "challenge" &&
      envelope.phase === "hello" &&
      envelope.domainId === this.declaration.domainId &&
      envelope.nodeId === this.nodeId &&
      envelope.clientNonce === this.helloClientNonce
    ) {
      this.helloServerNonce = envelope.serverNonce;
      const peer = this.peersMap.get(this.declaration.hostNodeId);
      if (peer !== undefined) {
        await this.queueRawSend(peer, {
          version: PROCESS_DOMAIN_PROTOCOL,
          type: "hello",
          domainId: this.declaration.domainId,
          nodeId: this.nodeId,
          clientNonce: envelope.clientNonce,
          serverNonce: envelope.serverNonce,
          proof: createProof(this.declaration.capability, "hello", [
            this.declaration.domainId,
            this.nodeId,
            envelope.clientNonce,
            envelope.serverNonce,
          ]),
        });
      }
      return;
    }
    if (envelope.type === "ready") {
      if (
        envelope.clientNonce !== this.helloClientNonce ||
        envelope.serverNonce !== this.helloServerNonce
      ) return;
      const valid =
        envelope.domainId === this.declaration.domainId &&
        envelope.nodeId === this.declaration.hostNodeId &&
        verifyProof(this.declaration.capability, "ready", [
          this.declaration.domainId,
          this.declaration.hostNodeId,
          this.nodeId,
          envelope.clientNonce,
          envelope.serverNonce,
        ], envelope.proof);
      if (valid) {
        this.clearHelloAttempt();
        this.markOnline();
      }
      return;
    }
    const peer = this.peersMap.get(this.declaration.hostNodeId);
    if (peer?.status !== "online") return;
    if (envelope.type === "ack") {
      const pending = peer.pending.get(envelope.id);
      if (pending?.timer) clearTimeout(pending.timer);
      pending?.resolve();
      peer.pending.delete(envelope.id);
      return;
    }
    if (envelope.type === "data" || envelope.type === "lifecycle") {
      this.dispatchEnvelope(envelope);
      await this.queueRawSend(peer, {
        version: PROCESS_DOMAIN_PROTOCOL,
        type: "ack",
        id: envelope.id,
      });
    }
  }

  private async handleHostEnvelope(peer: InternalPeer, routingId: Buffer, envelope: WireEnvelope): Promise<void> {
    if (routingId.toString() !== peer.nodeId) return;
    if (
      envelope.type === "challenge-request" &&
      envelope.phase === "hello" &&
      envelope.domainId === this.declaration.domainId &&
      envelope.nodeId === peer.nodeId &&
      isValidId(envelope.clientNonce)
    ) {
      if (peer.challenge?.expiryTimer) clearTimeout(peer.challenge.expiryTimer);
      const clientNonce = envelope.clientNonce;
      const serverNonce = randomId();
      const expiryTimer = setTimeout(() => {
        if (peer.challenge?.serverNonce === serverNonce) peer.challenge = undefined;
      }, AUTH_CHALLENGE_TTL_MS);
      peer.challenge = {
        clientNonce,
        serverNonce,
        expiresAt: Date.now() + AUTH_CHALLENGE_TTL_MS,
        expiryTimer,
      };
      await this.queueRawSend(peer, {
        version: PROCESS_DOMAIN_PROTOCOL,
        type: "challenge",
        phase: "hello",
        domainId: this.declaration.domainId,
        nodeId: peer.nodeId,
        clientNonce,
        serverNonce,
      });
      return;
    }
    if (envelope.type === "hello") {
      const challenge = peer.challenge;
      peer.challenge = undefined;
      if (challenge?.expiryTimer) clearTimeout(challenge.expiryTimer);
      if (
        envelope.domainId !== this.declaration.domainId ||
        envelope.nodeId !== peer.nodeId ||
        challenge === undefined ||
        challenge.expiresAt < Date.now() ||
        challenge.clientNonce !== envelope.clientNonce ||
        challenge.serverNonce !== envelope.serverNonce ||
        !verifyProof(this.declaration.capability, "hello", [
          envelope.domainId,
          envelope.nodeId,
          envelope.clientNonce,
          envelope.serverNonce,
        ], envelope.proof)
      ) return;
      await this.queueRawSend(peer, {
        version: PROCESS_DOMAIN_PROTOCOL,
        type: "ready",
        domainId: this.declaration.domainId,
        nodeId: this.nodeId,
        clientNonce: envelope.clientNonce,
        serverNonce: envelope.serverNonce,
        proof: createProof(this.declaration.capability, "ready", [
          this.declaration.domainId,
          this.nodeId,
          peer.nodeId,
          envelope.clientNonce,
          envelope.serverNonce,
        ]),
      });
      this.markPeerOnline(peer.nodeId);
      return;
    }
    if (peer.status !== "online") return;
    if (envelope.type === "data" && envelope.senderId !== peer.nodeId) return;
    if (envelope.type === "lifecycle" && envelope.senderId !== peer.nodeId) return;
    if (envelope.type === "ack") {
      const pending = peer.pending.get(envelope.id);
      if (pending?.timer) clearTimeout(pending.timer);
      pending?.resolve();
      peer.pending.delete(envelope.id);
      return;
    }
    if (envelope.type === "data") {
      if (envelope.targetId === "*") {
        this.dispatchEnvelope(envelope);
        await Promise.all(
          Array.from(this.peersMap.values())
            .filter((target) => target.nodeId !== peer.nodeId && target.status === "online")
            .map((target) => this.sendThroughPeer(target, envelope, this.options.connectTimeoutMs ?? DEFAULT_CONNECT_TIMEOUT_MS)),
        );
      } else if (envelope.targetId === this.nodeId) this.dispatchEnvelope(envelope);
      else {
        const target = this.peersMap.get(envelope.targetId);
        if (!target || target.status !== "online") throw new Error("target process-domain peer is offline");
        await this.sendThroughPeer(target, envelope, this.options.connectTimeoutMs ?? DEFAULT_CONNECT_TIMEOUT_MS);
      }
      await this.queueRawSend(peer, {
        version: PROCESS_DOMAIN_PROTOCOL,
        type: "ack",
        id: envelope.id,
      });
      return;
    }
    if (envelope.type === "lifecycle") {
      this.dispatchEnvelope(envelope);
      await this.queueRawSend(peer, {
        version: PROCESS_DOMAIN_PROTOCOL,
        type: "ack",
        id: envelope.id,
      });
    }
  }

  private async readLoop(socket: Router | Dealer, handler: (identity: Buffer, envelope: WireEnvelope) => void | Promise<void>): Promise<void> {
    try {
      for await (const frames of socket) {
        const parts = frames as Message[];
        const identity = parts.length > 1 && parts[0] !== undefined ? Buffer.from(parts[0]) : Buffer.alloc(0);
        try {
          const envelope = framesToEnvelope(parts);
          void Promise.resolve(handler(identity, envelope)).catch((error) => this.options.onError?.(asError(error)));
        } catch (error) {
          this.options.onError?.(asError(error));
        }
      }
    } catch (error) {
      if (!this.closed) this.options.onError?.(asError(error));
    }
  }

  private receiveOne(socket: Dealer, timeoutMs: number): Promise<WireEnvelope> {
    return withTimeout(
      socket.receive().then((frames) => framesToEnvelope(frames)),
      timeoutMs,
      "process-domain connection timed out",
    );
  }

  private sendOnSocket(socket: Dealer, envelope: WireEnvelope, timeoutMs: number): Promise<void> {
    return withTimeout(
      socket.send(encodeEnvelope(envelope)),
      timeoutMs,
      "process-domain send timed out",
    );
  }

  private emit(event: ProcessDomainEvent): void { for (const listener of this.eventListeners) listener(event); }
  private emitPeer(peer: ProcessDomainPeer): void { this.emit({ type: "peer", peer }); }

  private rejectPending(peer: InternalPeer, error: Error): void {
    for (const pending of peer.pending.values()) {
      if (pending.timer) clearTimeout(pending.timer);
      pending.reject(error);
    }
    peer.pending.clear();
  }

  private markOffline(): void {
    if (this.role !== "client") return;
    const peer = this.peersMap.get(this.declaration.hostNodeId);
    if (!peer) return;
    const wasOnline = peer.status === "online";
    peer.status = "offline";
    peer.disconnectedAt = Date.now();
    this.clearHelloAttempt();
    this.rejectPending(peer, new Error("process-domain host disconnected"));
    if (wasOnline) this.emitPeer(peerInfo(peer, "offline"));
  }

  private markOnline(): void {
    if (this.role !== "client") return;
    const peer = this.peersMap.get(this.declaration.hostNodeId);
    if (!peer || peer.status === "online") return;
    peer.status = "online";
    peer.disconnectedAt = undefined;
    this.emitPeer(peerInfo(peer, "online"));
  }

  private markPeerOnline(nodeId: string): void {
    const peer = this.peersMap.get(nodeId);
    if (!peer || peer.status === "online") return;
    peer.status = "online";
    peer.everOnline = true;
    if (peer.initialAuthTimer) clearTimeout(peer.initialAuthTimer);
    peer.initialAuthTimer = undefined;
    peer.disconnectedAt = undefined;
    this.emitPeer(peerInfo(peer, "online"));
  }

  private markPeerOffline(nodeId: string): void {
    const peer = this.peersMap.get(nodeId);
    if (!peer) return;
    const wasOnline = peer.status === "online";
    peer.status = "offline";
    peer.disconnectedAt = Date.now();
    if (peer.challenge?.expiryTimer) clearTimeout(peer.challenge.expiryTimer);
    peer.challenge = undefined;
    this.rejectPending(peer, new Error("process-domain peer disconnected"));
    if (wasOnline) this.emitPeer(peerInfo(peer, "offline"));
  }

  private removePeer(nodeId: string, expected: InternalPeer): void {
    if (this.peersMap.get(nodeId) !== expected) return;
    if (expected.initialAuthTimer) clearTimeout(expected.initialAuthTimer);
    if (expected.challenge?.expiryTimer) clearTimeout(expected.challenge.expiryTimer);
    this.rejectPending(expected, new Error("process-domain peer removed"));
    expected.socket.close();
    this.peersMap.delete(nodeId);
    this.hostChannels.delete(nodeId);
  }

  private queueBootstrapSend(identity: Buffer, envelope: WireEnvelope): Promise<void> {
    const socket = this.bootstrap;
    if (socket === null) return Promise.reject(new Error("process-domain bootstrap is closed"));
    if (this.bootstrapQueuedSends >= MAX_BOOTSTRAP_SENDS) return Promise.reject(new Error("process-domain bootstrap send queue is full"));
    this.bootstrapQueuedSends += 1;
    const rawSend = this.bootstrapSendTail.catch(() => {}).then(() => socket.send([identity, encodeEnvelope(envelope)]));
    this.bootstrapSendTail = rawSend.then(
      () => { this.bootstrapQueuedSends -= 1; },
      () => { this.bootstrapQueuedSends -= 1; },
    );
    return withTimeout(
      rawSend,
      this.options.connectTimeoutMs ?? DEFAULT_CONNECT_TIMEOUT_MS,
      "process-domain bootstrap send timed out",
    );
  }

  private dispatchEnvelope(envelope: WireEnvelope): void {
    if (envelope.type === "lifecycle") {
      this.emit({ type: "lifecycle", senderId: envelope.senderId, event: envelope.event as PiLifecycleEvent });
      return;
    }
    if (envelope.type !== "data") return;
    const message: ProcessDomainDataMessage = { id: envelope.id, channel: envelope.channel, value: envelope.value, senderId: envelope.senderId, targetId: envelope.targetId, receivedAt: Date.now() };
    for (const listener of this.subscribers.get(envelope.channel) ?? []) listener(message);
  }

  private queueRawSend(peer: InternalPeer, envelope: WireEnvelope): Promise<void> {
    const send = peer.sendTail.catch(() => {}).then(async () => {
      if (this.role === "host") {
        await (peer.socket as Router).send([
          Buffer.from(peer.nodeId),
          encodeEnvelope(envelope),
        ]);
      } else {
        await (peer.socket as Dealer).send(encodeEnvelope(envelope));
      }
    });
    peer.sendTail = send.then(
      () => {},
      () => {},
    );
    return send;
  }

  private sendThroughPeer(peer: InternalPeer, envelope: WireEnvelope, timeoutMs: number): Promise<void> {
    const id = envelope.type === "data" || envelope.type === "lifecycle" ? envelope.id : "";
    let pending!: PendingSend;
    const acknowledgement = new Promise<void>((resolve, reject) => {
      pending = { resolve, reject };
      peer.pending.set(id, pending);
    });
    const send = this.queueRawSend(peer, envelope);
    return send.then(
      () => {
        if (peer.pending.get(id) === pending) {
          pending.timer = setTimeout(() => {
            if (peer.pending.get(id) === pending) peer.pending.delete(id);
            pending.reject(new Error("process-domain acknowledgement timed out"));
          }, timeoutMs);
        }
        return acknowledgement;
      },
      (error) => {
        if (peer.pending.get(id) === pending) peer.pending.delete(id);
        throw error;
      },
    );
  }

  peers(): readonly ProcessDomainPeer[] { return Array.from(this.peersMap.values(), (peer) => peerInfo(peer, peer.status)); }

  async send<T>(targetId: string, channel: string, value: T, options: { readonly timeoutMs?: number } = {}): Promise<void> {
    if (!isValidId(targetId) || !isValidChannel(channel)) throw new TypeError("invalid process-domain target or channel");
    const peer = this.peersMap.get(targetId);
    const timeoutMs = options.timeoutMs ?? this.options.connectTimeoutMs ?? DEFAULT_CONNECT_TIMEOUT_MS;
    const envelope: WireEnvelope = { version: PROCESS_DOMAIN_PROTOCOL, type: "data", id: randomId(), channel, value, senderId: this.nodeId, targetId };
    if (this.role === "host") {
      if (!peer || peer.status !== "online") throw new Error("target process-domain peer is offline");
      await this.sendThroughPeer(peer, envelope, timeoutMs);
    } else {
      if (!this.ownSocket || !peer || peer.status !== "online") throw new Error("process-domain host is offline");
      await this.sendThroughPeer(peer, envelope, timeoutMs);
    }
  }

  async broadcast<T>(channel: string, value: T, options: { readonly timeoutMs?: number } = {}): Promise<void> {
    if (!isValidChannel(channel)) throw new TypeError("invalid process-domain channel");
    if (this.role === "host") {
      await Promise.all(this.peers().filter((peer) => peer.status === "online").map((peer) => this.send(peer.nodeId, channel, value, options)));
      return;
    }
    const peer = this.peersMap.get(this.declaration.hostNodeId);
    if (!peer || peer.status !== "online") throw new Error("process-domain host is offline");
    const envelope: WireEnvelope = { version: PROCESS_DOMAIN_PROTOCOL, type: "data", id: randomId(), channel, value, senderId: this.nodeId, targetId: "*" };
    await this.sendThroughPeer(peer, envelope, options.timeoutMs ?? this.options.connectTimeoutMs ?? DEFAULT_CONNECT_TIMEOUT_MS);
  }

  async reportLifecycle(event: PiLifecycleEvent, options: { readonly timeoutMs?: number } = {}): Promise<void> {
    const envelope: WireEnvelope = { version: PROCESS_DOMAIN_PROTOCOL, type: "lifecycle", id: randomId(), senderId: this.nodeId, event };
    const timeoutMs = options.timeoutMs ?? this.options.connectTimeoutMs ?? DEFAULT_CONNECT_TIMEOUT_MS;
    if (this.role === "host") {
      this.dispatchEnvelope(envelope);
      return;
    }
    if (this.ownSocket) {
      const peer = this.peersMap.get(this.declaration.hostNodeId);
      if (!peer || peer.status !== "online") throw new Error("process-domain host is offline");
      await this.sendThroughPeer(peer, envelope, timeoutMs);
    }
  }

  subscribe(channel: string, listener: (message: ProcessDomainDataMessage) => void): () => void { if (!isValidChannel(channel)) throw new TypeError("invalid process-domain channel"); let listeners = this.subscribers.get(channel); if (!listeners) { listeners = new Set(); this.subscribers.set(channel, listeners); } listeners.add(listener); return () => listeners?.delete(listener); }
  subscribeEvents(listener: (event: ProcessDomainEvent) => void): () => void { this.eventListeners.add(listener); return () => this.eventListeners.delete(listener); }

  attachPiLifecycle(pi: PiLifecycleExtensionApi, sessionId?: string): { close(): void } {
    const names = ["session_start", "agent_start", "agent_end", "agent_settled", "turn_start", "turn_end", "session_shutdown"] as const;
    let active = true;
    for (const name of names) {
      pi.on(name, async () => {
        if (!active) return;
        try {
          await this.reportLifecycle({ name, at: Date.now(), sessionId });
        } catch (error) {
          this.options.onError?.(asError(error));
        }
      });
    }
    return { close: () => { active = false; } };
  }

  async close(): Promise<void> {
    if (this.closed) return;
    this.closed = true;
    this.clearHelloAttempt();
    for (const challenge of this.bootstrapChallenges.values()) {
      if (challenge.expiryTimer) clearTimeout(challenge.expiryTimer);
    }
    this.bootstrapChallenges.clear();
    for (const peer of this.peersMap.values()) {
      if (peer.initialAuthTimer) clearTimeout(peer.initialAuthTimer);
      if (peer.challenge?.expiryTimer) clearTimeout(peer.challenge.expiryTimer);
      this.rejectPending(peer, new Error("process-domain closed"));
    }
    for (const socket of this.hostChannels.values()) socket.close();
    this.hostChannels.clear();
    this.ownSocket?.close();
    this.ownSocket = null;
    this.bootstrap?.close();
    this.bootstrap = null;
    this.peersMap.clear();
    if (
      this.role === "host" &&
      this.declarationEnv !== null &&
      this.publishedDeclaration !== null &&
      this.declarationEnv[ENV_NAMES.DECLARATION] === this.publishedDeclaration
    ) {
      delete this.declarationEnv[ENV_NAMES.DECLARATION];
    }
    this.declarationEnv = null;
    this.publishedDeclaration = null;
    this.eventListeners.clear();
    this.subscribers.clear();
  }
}

export async function openProcessDomain(options: OpenProcessDomainOptions = {}): Promise<ProcessDomainNode> {
  requireValidOptions(options);
  const env = options.env ?? process.env;
  const metadata = options.metadata ?? {};
  const encoded = env[ENV_NAMES.DECLARATION];
  if (encoded === undefined) {
    let runtime: DomainRuntime;
    try {
      runtime = await DomainRuntime.host(options, metadata);
    } catch (error) {
      throw connectionUnavailableError(error);
    }
    const declaration = encodeDeclaration(runtime.declaration);
    env[ENV_NAMES.DECLARATION] = declaration;
    runtime.declarationEnv = env;
    runtime.publishedDeclaration = declaration;
    return runtime;
  }
  let declaration: ProcessDomainDeclaration | null;
  try {
    declaration = decodeDeclaration(encoded);
  } catch (error) {
    throw invalidDeclarationError(error);
  }
  if (declaration === null) {
    throw invalidDeclarationError(new TypeError("missing declaration"));
  }
  try {
    return await DomainRuntime.client(declaration, options, metadata);
  } catch (error) {
    if (isProcessDomainOpenError(error)) throw error;
    throw connectionUnavailableError(error);
  }
}

export function attachPiLifecycle(node: ProcessDomainNode, pi: PiLifecycleExtensionApi, sessionId?: string): { close(): void } {
  return (node as DomainRuntime).attachPiLifecycle(pi, sessionId);
}
