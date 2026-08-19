import {
  authenticationFailedError,
  connectionUnavailableError,
  invalidDeclarationError,
  isProcessDomainOpenError,
} from "./errors.js";
import { connectLoopback, listenLoopback, type FrameLink, type FrameServer } from "./net.js";
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
const MAX_PENDING_AUTH = 1_024;
const RECONNECT_DELAY_MS = 1_000;

type PendingSend = { resolve: () => void; reject: (error: Error) => void; timer?: ReturnType<typeof setTimeout> };

interface AuthChallenge {
  readonly clientNonce: string;
  readonly serverNonce: string;
  readonly expiresAt: number;
  readonly expiryTimer?: ReturnType<typeof setTimeout>;
}

/** A host-side connection that has not completed the authentication handshake yet. */
interface PendingAuth {
  readonly state: LinkState;
  challenge?: AuthChallenge;
  readonly authTimer: ReturnType<typeof setTimeout>;
}

interface InternalPeer {
  readonly nodeId: string;
  status: "online" | "offline";
  readonly metadata: Readonly<Record<string, string>>;
  readonly connectedAt: number;
  disconnectedAt?: number;
  readonly link: FrameLink;
  readonly state: LinkState;
  readonly pending: Map<string, PendingSend>;
  everOnline: boolean;
  liveness: Liveness | null;
}

interface Liveness {
  stop(): void;
  noteInbound(): void;
  notePong(id: string): void;
}

/**
 * Per-link frame dispatch state. Exactly one permanent onFrame callback is
 * installed per link; consumers either pull through waiters (handshake) or get
 * a handler attached once authentication completes. Frames that arrive while no
 * handler is attached are queued in inbox and drained at attach time, so frames
 * coalesced into one TCP chunk with the handshake-finalizing frame are never
 * delivered to a stale one-shot handler.
 */
interface LinkState {
  readonly link: FrameLink;
  readonly inbox: WireEnvelope[];
  readonly waiters: Array<{ resolve: (envelope: WireEnvelope) => void; reject: (error: Error) => void }>;
  handler: ((envelope: WireEnvelope) => void) | null;
  onClose: (() => void) | null;
}

interface HeartbeatTiming {
  readonly heartbeatIntervalMs: number;
  readonly heartbeatTimeoutMs: number;
  readonly heartbeatTimeToLiveMs: number;
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

/**
 * Application-level heartbeat. Both sides ping every
 * heartbeatIntervalMs; any inbound frame refreshes the silence window
 * (heartbeatTimeoutMs) and a pong older than heartbeatTimeToLiveMs marks the
 * peer dead. A frozen (SIGSTOP) peer therefore fails both checks.
 */
function startLiveness(timing: HeartbeatTiming, sendPing: (id: string) => Promise<void>, onDead: () => void): Liveness {
  let stopped = false;
  let lastInbound = Date.now();
  let awaitingId: string | null = null;
  let awaitingSince = 0;
  const stop = (): void => {
    if (stopped) return;
    stopped = true;
    clearInterval(timer);
  };
  const timer = setInterval(() => {
    if (stopped) return;
    const now = Date.now();
    if (now - lastInbound > timing.heartbeatTimeoutMs) {
      stop();
      onDead();
      return;
    }
    if (awaitingId !== null) {
      if (now - awaitingSince > timing.heartbeatTimeToLiveMs) {
        stop();
        onDead();
      }
      return;
    }
    const id = randomId();
    awaitingId = id;
    awaitingSince = now;
    void sendPing(id).catch(() => {
      // Send failures are handled by the link close path.
    });
  }, timing.heartbeatIntervalMs);
  return {
    stop,
    noteInbound() {
      lastInbound = Date.now();
    },
    notePong(id: string) {
      if (id === awaitingId) awaitingId = null;
    },
  };
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
  private server: FrameServer | null = null;
  private readonly pendingAuth = new Set<PendingAuth>();
  private ownLink: FrameLink | null = null;
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
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

  private get connectTimeoutMs(): number {
    return this.options.connectTimeoutMs ?? DEFAULT_CONNECT_TIMEOUT_MS;
  }

  private get heartbeatTiming(): HeartbeatTiming {
    const values = requireValidOptions(this.options);
    return {
      heartbeatIntervalMs: values.heartbeatIntervalMs,
      heartbeatTimeoutMs: values.heartbeatTimeoutMs,
      heartbeatTimeToLiveMs: values.heartbeatTimeToLiveMs,
    };
  }

  static async host(options: OpenProcessDomainOptions, metadata: Readonly<Record<string, string>>): Promise<DomainRuntime> {
    const server = await listenLoopback();
    const nodeId = randomId();
    const { declaration } = createDeclaration(server.endpoint, nodeId);
    const runtime = new DomainRuntime("host", nodeId, declaration, "tcp-loopback", options, metadata);
    runtime.server = server;
    server.onConnection = (link) => runtime.handleConnection(link);
    runtime.emit({ type: "transport", transport: "tcp-loopback", endpoint: server.endpoint });
    return runtime;
  }

  static async client(declaration: ProcessDomainDeclaration, options: OpenProcessDomainOptions, metadata: Readonly<Record<string, string>>): Promise<DomainRuntime> {
    const runtime = new DomainRuntime("client", randomId(), declaration, "tcp-loopback", options, metadata);
    const startup = { failure: "connection" as "connection" | "authentication" };
    let link: FrameLink | null = null;
    try {
      link = await connectLoopback(declaration.endpoint, runtime.connectTimeoutMs, "process-domain connection timed out");
      const state = runtime.installLinkState(link);
      await runtime.handshakeClient(state, () => {
        startup.failure = "authentication";
      });
      runtime.attachClientLink(state);
      runtime.emit({ type: "transport", transport: runtime.transport, endpoint: declaration.endpoint });
      return runtime;
    } catch (error) {
      link?.close();
      if (isProcessDomainOpenError(error)) throw error;
      throw startup.failure === "authentication"
        ? authenticationFailedError(error)
        : connectionUnavailableError(error);
    }
  }

  private installLinkState(link: FrameLink): LinkState {
    const state: LinkState = { link, inbox: [], waiters: [], handler: null, onClose: null };
    link.onFrame = (frame) => {
      let envelope: WireEnvelope;
      try {
        envelope = decodeEnvelope(frame);
      } catch (error) {
        this.options.onError?.(asError(error));
        if (state.handler === null) link.close();
        return;
      }
      if (state.handler !== null) {
        state.handler(envelope);
        return;
      }
      const waiter = state.waiters.shift();
      if (waiter !== undefined) waiter.resolve(envelope);
      else state.inbox.push(envelope);
    };
    link.onError = (error) => this.options.onError?.(error);
    link.onClose = () => {
      for (const waiter of state.waiters.splice(0)) waiter.reject(new Error("process-domain connection closed"));
      state.onClose?.();
    };
    return state;
  }

  private nextEnvelope(state: LinkState, timeoutMs: number): Promise<WireEnvelope> {
    const queued = state.inbox.shift();
    if (queued !== undefined) return Promise.resolve(queued);
    return withTimeout(
      new Promise<WireEnvelope>((resolve, reject) => {
        state.waiters.push({ resolve, reject });
      }),
      timeoutMs,
      "process-domain connection timed out",
    );
  }

  /** Swaps a link into post-auth dispatch and replays any frames queued mid-handshake. */
  private attachLinkHandler(state: LinkState, onClose: () => void, handler: (envelope: WireEnvelope) => void): void {
    state.onClose = onClose;
    state.handler = handler;
    const drained = state.inbox.splice(0);
    for (const envelope of drained) handler(envelope);
  }

  private detachLinkHandler(state: LinkState): void {
    state.onClose = null;
    state.handler = null;
  }

  // ---------------------------------------------------------------- host auth

  private handleConnection(link: FrameLink): void {
    if (this.closed || this.pendingAuth.size >= MAX_PENDING_AUTH) {
      link.close();
      return;
    }
    const state = this.installLinkState(link);
    const entry: PendingAuth = {
      state,
      authTimer: setTimeout(() => {
        link.close();
      }, this.connectTimeoutMs),
    };
    this.pendingAuth.add(entry);
    state.onClose = () => {
      clearTimeout(entry.authTimer);
      if (entry.challenge?.expiryTimer) clearTimeout(entry.challenge.expiryTimer);
      this.pendingAuth.delete(entry);
    };
    state.handler = (envelope) => {
      void this.handlePreAuth(entry, envelope).catch((error) => {
        this.options.onError?.(asError(error));
        link.close();
      });
    };
  }

  private queueAuthSend(entry: PendingAuth, envelope: WireEnvelope): Promise<void> {
    return entry.state.link.send(encodeEnvelope(envelope));
  }

  private async handlePreAuth(entry: PendingAuth, envelope: WireEnvelope): Promise<void> {
    if (
      envelope.type === "challenge-request" &&
      envelope.phase === "bootstrap" &&
      envelope.domainId === this.declaration.domainId &&
      isValidId(envelope.nodeId) &&
      isValidId(envelope.clientNonce)
    ) {
      const previous = entry.challenge;
      if (previous !== undefined && previous.expiresAt >= Date.now()) return;
      if (previous?.expiryTimer) clearTimeout(previous.expiryTimer);
      const challenge: AuthChallenge = {
        clientNonce: envelope.clientNonce,
        serverNonce: randomId(),
        expiresAt: Date.now() + AUTH_CHALLENGE_TTL_MS,
      };
      const expiryTimer = setTimeout(() => {
        if (entry.challenge?.serverNonce === challenge.serverNonce) entry.challenge = undefined;
      }, AUTH_CHALLENGE_TTL_MS);
      entry.challenge = { ...challenge, expiryTimer };
      await this.queueAuthSend(entry, {
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
    if (envelope.type !== "bootstrap" || envelope.domainId !== this.declaration.domainId || !isValidId(envelope.nodeId)) {
      entry.state.link.close();
      return;
    }
    const challenge = entry.challenge;
    entry.challenge = undefined;
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
    ) {
      entry.state.link.close();
      return;
    }
    await this.completeAuth(entry, envelope.nodeId, envelope.metadata, envelope.clientNonce, envelope.serverNonce);
  }

  private async completeAuth(entry: PendingAuth, nodeId: string, metadata: Readonly<Record<string, string>>, clientNonce: string, serverNonce: string): Promise<void> {
    clearTimeout(entry.authTimer);
    this.pendingAuth.delete(entry);

    const replaced = this.peersMap.get(nodeId);
    if (replaced !== undefined) {
      // Fencing: a freshly authenticated connection with the same nodeId replaces
      // the stale incarnation; its pending sends are rejected and its handlers are
      // detached before closing so no duplicate offline event is emitted.
      replaced.liveness?.stop();
      replaced.liveness = null;
      this.rejectPending(replaced, new Error("process-domain peer replaced"));
      this.detachLinkHandler(replaced.state);
      replaced.link.close();
      this.peersMap.delete(nodeId);
      if (replaced.status === "online") {
        replaced.status = "offline";
        replaced.disconnectedAt = Date.now();
        this.emitPeer(peerInfo(replaced, "offline"));
      }
    }

    const link = entry.state.link;
    const peer: InternalPeer = {
      nodeId,
      status: "offline",
      metadata,
      connectedAt: Date.now(),
      link,
      state: entry.state,
      pending: new Map(),
      everOnline: false,
      liveness: null,
    };
    this.peersMap.set(nodeId, peer);
    try {
      await this.queueRawSend(peer, {
        version: PROCESS_DOMAIN_PROTOCOL,
        type: "ready",
        domainId: this.declaration.domainId,
        nodeId: this.nodeId,
        clientNonce,
        serverNonce,
        proof: createProof(this.declaration.capability, "ready", [
          this.declaration.domainId,
          this.nodeId,
          nodeId,
          clientNonce,
          serverNonce,
        ]),
      });
    } catch (error) {
      this.detachLinkHandler(entry.state);
      this.peersMap.delete(nodeId);
      this.rejectPending(peer, asError(error));
      link.close();
      throw error;
    }
    // The peer must be online before the post-auth dispatcher is attached so
    // frames drained from the handshake boundary are accepted, not dropped.
    this.markPeerOnline(nodeId);
    this.attachLinkHandler(entry.state, () => this.handlePeerClose(peer), (envelope) => this.dispatchHostEnvelope(peer, envelope));
    peer.liveness = startLiveness(
      this.heartbeatTiming,
      (id) =>
        this.queueRawSend(peer, {
          version: PROCESS_DOMAIN_PROTOCOL,
          type: "ping",
          id,
        }),
      () => link.close(),
    );
  }

  // ------------------------------------------------------------ host messaging

  private dispatchHostEnvelope(peer: InternalPeer, envelope: WireEnvelope): void {
    if (this.peersMap.get(peer.nodeId) !== peer) return;
    void this.handleHostEnvelope(peer, envelope).catch((error) => {
      this.options.onError?.(asError(error));
    });
  }

  private handlePeerClose(peer: InternalPeer): void {
    if (this.peersMap.get(peer.nodeId) !== peer) return;
    this.markPeerOffline(peer.nodeId);
  }

  private async handleHostEnvelope(peer: InternalPeer, envelope: WireEnvelope): Promise<void> {
    peer.liveness?.noteInbound();
    if (envelope.type === "ping") {
      await this.queueRawSend(peer, {
        version: PROCESS_DOMAIN_PROTOCOL,
        type: "pong",
        id: envelope.id,
      });
      return;
    }
    if (envelope.type === "pong") {
      peer.liveness?.notePong(envelope.id);
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
      if (envelope.targetId === this.nodeId) {
        // Leaf delivery: the receipt ACK is written before local dispatch so a
        // listener that synchronously closes the node cannot strand it.
        const acknowledged = this.queueRawSend(peer, {
          version: PROCESS_DOMAIN_PROTOCOL,
          type: "ack",
          id: envelope.id,
        });
        this.dispatchEnvelope(envelope);
        await acknowledged;
        return;
      }
      if (envelope.targetId === "*") {
        this.dispatchEnvelope(envelope);
        await Promise.all(
          Array.from(this.peersMap.values())
            .filter((target) => target.nodeId !== peer.nodeId && target.status === "online")
            .map((target) => this.sendThroughPeer(target, envelope, this.connectTimeoutMs)),
        );
      } else {
        const target = this.peersMap.get(envelope.targetId);
        if (!target || target.status !== "online") throw new Error("target process-domain peer is offline");
        await this.sendThroughPeer(target, envelope, this.connectTimeoutMs);
      }
      // Routed messages acknowledge only after every downstream hop acknowledged.
      await this.queueRawSend(peer, {
        version: PROCESS_DOMAIN_PROTOCOL,
        type: "ack",
        id: envelope.id,
      });
      return;
    }
    if (envelope.type === "lifecycle") {
      const acknowledged = this.queueRawSend(peer, {
        version: PROCESS_DOMAIN_PROTOCOL,
        type: "ack",
        id: envelope.id,
      });
      this.dispatchEnvelope(envelope);
      await acknowledged;
    }
  }

  // ------------------------------------------------------------------ client

  private async handshakeClient(state: LinkState, onBootstrapSent?: () => void): Promise<void> {
    const timeoutMs = this.connectTimeoutMs;
    const clientNonce = randomId();
    await withTimeout(
      state.link.send(encodeEnvelope({
        version: PROCESS_DOMAIN_PROTOCOL,
        type: "challenge-request",
        phase: "bootstrap",
        domainId: this.declaration.domainId,
        nodeId: this.nodeId,
        clientNonce,
      })),
      timeoutMs,
      "process-domain send timed out",
    );
    const challenge = await this.nextEnvelope(state, timeoutMs);
    if (
      challenge.type !== "challenge" ||
      challenge.phase !== "bootstrap" ||
      challenge.domainId !== this.declaration.domainId ||
      challenge.nodeId !== this.nodeId ||
      challenge.clientNonce !== clientNonce
    ) throw authenticationFailedError(new Error("invalid bootstrap challenge"));
    await withTimeout(
      state.link.send(encodeEnvelope({
        version: PROCESS_DOMAIN_PROTOCOL,
        type: "bootstrap",
        domainId: this.declaration.domainId,
        nodeId: this.nodeId,
        metadata: this.metadata,
        clientNonce,
        serverNonce: challenge.serverNonce,
        proof: createProof(this.declaration.capability, "bootstrap", [
          this.declaration.domainId,
          this.nodeId,
          this.metadata,
          clientNonce,
          challenge.serverNonce,
        ]),
      })),
      timeoutMs,
      "process-domain send timed out",
    );
    onBootstrapSent?.();
    const ready = await this.nextEnvelope(state, timeoutMs);
    if (
      ready.type !== "ready" ||
      ready.domainId !== this.declaration.domainId ||
      ready.nodeId !== this.declaration.hostNodeId ||
      ready.clientNonce !== clientNonce ||
      ready.serverNonce !== challenge.serverNonce ||
      !verifyProof(this.declaration.capability, "ready", [
        this.declaration.domainId,
        this.declaration.hostNodeId,
        this.nodeId,
        clientNonce,
        challenge.serverNonce,
      ], ready.proof)
    ) throw authenticationFailedError(new Error("invalid bootstrap response"));
  }

  private attachClientLink(state: LinkState): void {
    const link = state.link;
    const hostNodeId = this.declaration.hostNodeId;
    const previous = this.peersMap.get(hostNodeId);
    if (previous !== undefined) {
      previous.liveness?.stop();
      this.rejectPending(previous, new Error("process-domain host disconnected"));
      this.detachLinkHandler(previous.state);
      previous.link.close();
      this.peersMap.delete(hostNodeId);
    }
    const peer: InternalPeer = {
      nodeId: hostNodeId,
      status: "offline",
      metadata: {},
      connectedAt: Date.now(),
      link,
      state,
      pending: new Map(),
      everOnline: false,
      liveness: null,
    };
    this.peersMap.set(hostNodeId, peer);
    this.ownLink = link;
    // Online before attaching the dispatcher: frames that were coalesced with
    // the handshake-complete frame are drained through the online peer.
    this.markOnline();
    this.attachLinkHandler(state, () => this.handleClientLinkClose(peer), (envelope) => this.dispatchClientEnvelope(peer, envelope));
    peer.liveness = startLiveness(
      this.heartbeatTiming,
      (id) =>
        this.queueRawSend(peer, {
          version: PROCESS_DOMAIN_PROTOCOL,
          type: "ping",
          id,
        }),
      () => link.close(),
    );
  }

  private dispatchClientEnvelope(peer: InternalPeer, envelope: WireEnvelope): void {
    if (this.peersMap.get(this.declaration.hostNodeId) !== peer) return;
    void this.handleClientEnvelope(peer, envelope).catch((error) => {
      this.options.onError?.(asError(error));
    });
  }

  private handleClientLinkClose(peer: InternalPeer): void {
    if (this.peersMap.get(this.declaration.hostNodeId) !== peer) return;
    this.markOffline();
    this.scheduleReconnect();
  }

  private scheduleReconnect(): void {
    if (this.closed || this.role !== "client" || this.reconnectTimer !== null) return;
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      void this.reconnectAttempt();
    }, RECONNECT_DELAY_MS);
  }

  private async reconnectAttempt(): Promise<void> {
    if (this.closed) return;
    let link: FrameLink | null = null;
    try {
      link = await connectLoopback(this.declaration.endpoint, this.connectTimeoutMs, "process-domain connection timed out");
      const state = this.installLinkState(link);
      await this.handshakeClient(state);
      this.attachClientLink(state);
    } catch (error) {
      link?.close();
      if (!this.closed) this.options.onError?.(asError(error));
      this.scheduleReconnect();
    }
  }

  private async handleClientEnvelope(peer: InternalPeer, envelope: WireEnvelope): Promise<void> {
    peer.liveness?.noteInbound();
    if (envelope.type === "ping") {
      await this.queueRawSend(peer, {
        version: PROCESS_DOMAIN_PROTOCOL,
        type: "pong",
        id: envelope.id,
      });
      return;
    }
    if (envelope.type === "pong") {
      peer.liveness?.notePong(envelope.id);
      return;
    }
    if (peer.status !== "online") return;
    if (envelope.type === "ack") {
      const pending = peer.pending.get(envelope.id);
      if (pending?.timer) clearTimeout(pending.timer);
      pending?.resolve();
      peer.pending.delete(envelope.id);
      return;
    }
    if (envelope.type === "data" || envelope.type === "lifecycle") {
      // The receipt ACK is written before local dispatch so a listener that
      // synchronously closes the node cannot strand it.
      const acknowledged = this.queueRawSend(peer, {
        version: PROCESS_DOMAIN_PROTOCOL,
        type: "ack",
        id: envelope.id,
      });
      this.dispatchEnvelope(envelope);
      await acknowledged;
    }
  }

  // ------------------------------------------------------------------- shared

  private markOffline(): void {
    if (this.role !== "client") return;
    const peer = this.peersMap.get(this.declaration.hostNodeId);
    if (!peer) return;
    const wasOnline = peer.status === "online";
    peer.status = "offline";
    peer.disconnectedAt = Date.now();
    peer.liveness?.stop();
    peer.liveness = null;
    this.ownLink = null;
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
    peer.disconnectedAt = undefined;
    this.emitPeer(peerInfo(peer, "online"));
  }

  private markPeerOffline(nodeId: string): void {
    const peer = this.peersMap.get(nodeId);
    if (!peer) return;
    const wasOnline = peer.status === "online";
    peer.status = "offline";
    peer.disconnectedAt = Date.now();
    peer.liveness?.stop();
    peer.liveness = null;
    this.rejectPending(peer, new Error("process-domain peer disconnected"));
    if (wasOnline) this.emitPeer(peerInfo(peer, "offline"));
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
    // TCP streams serialize writes in call order, so no client-side send queue
    // is required; starting the write synchronously matters for ACKs that race
    // with a listener-initiated close().
    return peer.link.send(encodeEnvelope(envelope));
  }

  private sendThroughPeer(peer: InternalPeer, envelope: WireEnvelope, timeoutMs: number): Promise<void> {
    const id = envelope.type === "data" || envelope.type === "lifecycle" ? envelope.id : "";
    let pending!: PendingSend;
    const acknowledgement = new Promise<void>((resolve, reject) => {
      pending = { resolve, reject };
      peer.pending.set(id, pending);
    });
    // If the write below fails, this promise is never adopted by a caller; sink
    // its rejection so a concurrent rejectPending cannot surface as unhandled.
    acknowledgement.catch(() => {});
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

  private emit(event: ProcessDomainEvent): void { for (const listener of this.eventListeners) listener(event); }
  private emitPeer(peer: ProcessDomainPeer): void { this.emit({ type: "peer", peer }); }

  private rejectPending(peer: InternalPeer, error: Error): void {
    for (const pending of peer.pending.values()) {
      if (pending.timer) clearTimeout(pending.timer);
      pending.reject(error);
    }
    peer.pending.clear();
  }

  peers(): readonly ProcessDomainPeer[] { return Array.from(this.peersMap.values(), (peer) => peerInfo(peer, peer.status)); }

  async send<T>(targetId: string, channel: string, value: T, options: { readonly timeoutMs?: number } = {}): Promise<void> {
    if (!isValidId(targetId) || !isValidChannel(channel)) throw new TypeError("invalid process-domain target or channel");
    const peer = this.peersMap.get(targetId);
    const timeoutMs = options.timeoutMs ?? this.connectTimeoutMs;
    const envelope: WireEnvelope = { version: PROCESS_DOMAIN_PROTOCOL, type: "data", id: randomId(), channel, value, senderId: this.nodeId, targetId };
    if (this.role === "host") {
      if (!peer || peer.status !== "online") throw new Error("target process-domain peer is offline");
      await this.sendThroughPeer(peer, envelope, timeoutMs);
    } else {
      if (this.ownLink === null || !peer || peer.status !== "online") throw new Error("process-domain host is offline");
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
    await this.sendThroughPeer(peer, envelope, options.timeoutMs ?? this.connectTimeoutMs);
  }

  async reportLifecycle(event: PiLifecycleEvent, options: { readonly timeoutMs?: number } = {}): Promise<void> {
    const envelope: WireEnvelope = { version: PROCESS_DOMAIN_PROTOCOL, type: "lifecycle", id: randomId(), senderId: this.nodeId, event };
    const timeoutMs = options.timeoutMs ?? this.connectTimeoutMs;
    if (this.role === "host") {
      this.dispatchEnvelope(envelope);
      return;
    }
    const peer = this.peersMap.get(this.declaration.hostNodeId);
    if (peer === undefined) return;
    if (peer.status !== "online") throw new Error("process-domain host is offline");
    await this.sendThroughPeer(peer, envelope, timeoutMs);
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
    if (this.reconnectTimer !== null) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
    for (const entry of this.pendingAuth) {
      clearTimeout(entry.authTimer);
      if (entry.challenge?.expiryTimer) clearTimeout(entry.challenge.expiryTimer);
      this.detachLinkHandler(entry.state);
      entry.state.link.close();
    }
    this.pendingAuth.clear();
    for (const peer of this.peersMap.values()) {
      peer.liveness?.stop();
      peer.liveness = null;
      this.rejectPending(peer, new Error("process-domain closed"));
      this.detachLinkHandler(peer.state);
      peer.link.close();
    }
    this.peersMap.clear();
    this.ownLink = null;
    if (this.server !== null) {
      await this.server.close();
      this.server = null;
    }
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
