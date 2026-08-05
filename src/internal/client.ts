/**
 * Client. Connects to the broker (starting it on demand via election), performs
 * the authenticated handshake, registers as a participant, and exposes the
 * ProcessDomain public interface.
 *
 * Correctness guarantees:
 *   - `openDomain()` resolves only after the initial authenticated `join`
 *     succeeds, so the returned handle is always already a participant.
 *   - Initial joins and reconnects share ONE awaited join path with the same
 *     stable participant identity and a strictly increasing incarnation.
 *   - The client sends real periodic heartbeats; only received traffic updates
 *     broker-side liveness.
 *   - Optional wire fields are omitted (never emitted as undefined).
 *   - A reservation claim (from env) is presented for exact, single-use adoption.
 */

import * as net from "node:net";
import {
  clientMac,
  deriveDomainAuthKey,
  macEqual,
  randomNonce,
  serverMac,
  unbase64url,
} from "./auth.js";
import { decodeReservationClaim, type DomainDeclaration } from "./declaration.js";
import { computeSnapshot, type ActivityState, type DomainFence, type DomainSnapshot } from "./domain-types.js";
import { PROCESS_DOMAIN_PROTOCOL_MAJOR, ProcessDomainFatalError, isProcessDomainFatalError, type ProcessDomainFatalCode } from "./errors.js";
import { type CanonicalObject } from "./framing.js";
import { RawChannel, createRpcPeer } from "./rpc.js";
import { resolveEndpoint, type RuntimeEndpoint } from "./runtime-path.js";
import { startBrokerProcess } from "./launcher.js";
import { DEFAULT_HEARTBEAT_MS } from "./broker-state.js";

interface PendingConfirm {
  fence: DomainFence;
  resolve: (ok: boolean) => void;
}

export interface ClientOptions {
  declaration: DomainDeclaration;
  initialActivity: ActivityState;
  metadata: Readonly<Record<string, string>>;
  connectTimeoutMs: number;
  onFatal: (error: Error) => void;
  /** When true, the broker may create the domain on first contact. */
  createDomain?: boolean;
  /** True when this is a broker-restart rejoin (broker may need recovery mode). */
  recover?: boolean;
}

function wireToSnapshot(wire: Record<string, unknown>, domainId: string): DomainSnapshot {
  const epoch = typeof wire.brokerEpoch === "string" ? wire.brokerEpoch : "";
  const revisionText = typeof wire.revision === "string" && /^(0|[1-9][0-9]*)$/.test(wire.revision) ? wire.revision : "";
  const generationText = typeof wire.activityGeneration === "string" && /^(0|[1-9][0-9]*)$/.test(wire.activityGeneration) ? wire.activityGeneration : "";
  const counts = [wire.participants, wire.busyParticipants, wire.pendingSpawns];
  if (!epoch || !revisionText || !generationText || counts.some((value) => !Number.isSafeInteger(value) || Number(value) < 0) || typeof wire.certain !== "boolean") {
    throw new ProcessDomainFatalError("DOMAIN_UNRECOVERABLE", "broker returned a malformed snapshot");
  }
  const participants = Number(wire.participants);
  const busyParticipants = Number(wire.busyParticipants);
  if (busyParticipants > participants) {
    throw new ProcessDomainFatalError("DOMAIN_UNRECOVERABLE", "broker returned inconsistent snapshot counts");
  }
  return computeSnapshot({
    domainId,
    brokerEpoch: epoch,
    revision: BigInt(revisionText),
    activityGeneration: BigInt(generationText),
    participants,
    busyParticipants,
    pendingSpawns: Number(wire.pendingSpawns),
    certain: wire.certain,
  });
}

/** Return a wire object omitting any `undefined` values (canonical layer rejects them). */
function compact(obj: Record<string, unknown>): Record<string, unknown> {
  const out: Record<string, unknown> = {};
  for (const [k, v] of Object.entries(obj)) {
    if (v !== undefined) out[k] = v;
  }
  return out;
}

export class DomainClient {
  private endpoint: RuntimeEndpoint;
  private options: ClientOptions;
  private authKey: Uint8Array;
  private raw: RawChannel | null = null;
  private peer: ReturnType<typeof createRpcPeer> | null = null;
  private participantId: string | null = null;
  private resumeKey = "";
  private incarnation = 0n;
  private lastSnapshot: DomainSnapshot;
  private listeners = new Set<(s: DomainSnapshot) => void>();
  private signalListeners = new Map<string, Set<(s: unknown) => void>>();
  private closed = false;
  private fatalEmitted = false;
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  private heartbeatTimer: ReturnType<typeof setInterval> | null = null;
  private brokerEpoch = "";
  private pendingConfirms: PendingConfirm[] = [];
  private joining: Promise<void> | null = null;
  private everJoined = false;

  constructor(options: ClientOptions) {
    this.options = options;
    this.endpoint = resolveEndpoint();
    this.authKey = deriveDomainAuthKey(options.declaration.domainKey, options.declaration.domainId);
    this.lastSnapshot = computeSnapshot({
      domainId: options.declaration.domainId,
      brokerEpoch: "",
      revision: 0n,
      activityGeneration: 0n,
      participants: 0,
      busyParticipants: 0,
      pendingSpawns: 0,
      certain: false,
    });
  }

  async open(): Promise<DomainClient> {
    try {
      await this.joinThroughElection(this.options.createDomain === true, this.options.recover === true);
      return this;
    }
    catch (error) {
      const fatal = isProcessDomainFatalError(error)
        ? error
        : new ProcessDomainFatalError("BROKER_UNAVAILABLE", "failed to open process domain", error);
      this.emitFatal(fatal);
      await this.close();
      throw fatal;
    }
  }

  /**
   * Single join path. Attempts a fresh authenticated join (with optional
   * resume identity) and retries the whole flow through broker election.
   * Resolves only after the join succeeds; rejects on fatal errors.
   */
  private async joinThroughElection(create: boolean, recover: boolean): Promise<void> {
    if (this.joining) return this.joining;
    const deadline = Date.now() + this.options.connectTimeoutMs;
    let started = false;

    const attempt = async (): Promise<void> => {
      for (;;) {
        const remaining = deadline - Date.now();
        if (remaining <= 0) {
          throw new ProcessDomainFatalError("BROKER_UNAVAILABLE", "broker unavailable within connect deadline");
        }
        try {
          await this.connectAndJoin(remaining, create, recover);
          return;
        }
        catch (err) {
          this.resetTransport();
          if (isProcessDomainFatalError(err)) throw err;
          const code = (err as NodeJS.ErrnoException).code;
          if ((code === "ECONNREFUSED" || code === "ENOENT" || code === "EPIPE" || code === "ECONNRESET") && !started) {
            started = true;
            try {
              await Promise.race([
                startBrokerProcess(Math.max(1, remaining)),
                this.sleep(Math.max(1, remaining)).then(() => { throw new Error("broker launch timed out"); }),
              ]);
            }
            catch {
              /* another contender may have started it; keep retrying */
            }
          }
          await this.sleep(Math.max(1, Math.min(100, deadline - Date.now())));
        }
      }
    };

    this.joining = attempt();
    try {
      await this.joining;
    }
    finally {
      this.joining = null;
    }
  }

  private sleep(ms: number): Promise<void> {
    return new Promise((r) => setTimeout(r, ms));
  }

  private resetTransport(): void {
    const peer = this.peer;
    const raw = this.raw;
    this.peer = null;
    this.raw = null;
    if (peer && !peer.closed) peer.close();
    else if (raw && !raw.destroyed) raw.destroy();
  }

  private connectAndJoin(timeoutMs: number, create: boolean, recover: boolean): Promise<void> {
    return new Promise((resolve, reject) => {
      const socket = net.connect(this.endpoint.endpointPath);
      let done = false;
      const timer = setTimeout(() => {
        if (!done) {
          done = true;
          socket.destroy();
          reject(new ProcessDomainFatalError("BROKER_UNAVAILABLE", "broker connect timed out"));
        }
      }, timeoutMs);

      socket.once("error", (err) => {
        if (!done) {
          done = true;
          clearTimeout(timer);
          socket.destroy();
          reject(err);
        }
      });

      socket.once("connect", () => {
        if (done) return;
        clearTimeout(timer);
        const failBeforeJoin = (error?: Error) => {
          if (done) return;
          done = true;
          reject(error ?? new Error("broker connection closed"));
        };
        this.raw = new RawChannel(socket, failBeforeJoin, failBeforeJoin);
        void this.performHandshakeAndJoin(create, recover).then(
          () => {
            if (done) return;
            done = true;
            const raw = this.raw;
            raw?.onClose((error) => {
              if (!this.closed) this.scheduleReconnect(error);
            });
            resolve();
          },
          (error) => {
            if (done) return;
            done = true;
            reject(error);
          },
        );
      });
    });
  }

  private async performHandshakeAndJoin(create: boolean, recover: boolean): Promise<void> {
    const raw = this.raw!;
    const domainId = this.options.declaration.domainId;
    const clientNonce = randomNonce();

    const challenge = await new Promise<CanonicalObject>((res, rej) => {
      const timer = setTimeout(() => rej(new ProcessDomainFatalError("BROKER_UNAVAILABLE", "handshake timeout")), Math.min(10000, this.options.connectTimeoutMs));
      const handler = (frame: CanonicalObject) => {
        if (frame.t === "error") {
          clearTimeout(timer);
          raw.removeHandler(handler);
          rej(new ProcessDomainFatalError(frame.code as ProcessDomainFatalCode, "broker rejected handshake"));
        }
        else if (frame.t === "challenge") {
          clearTimeout(timer);
          raw.removeHandler(handler);
          res(frame);
        }
      };
      raw.addHandler(handler);
      // Omit optional fields (domainKey only when creating; recover flag).
      const hello: Record<string, unknown> = {
        t: "hello",
        protocolMajor: PROCESS_DOMAIN_PROTOCOL_MAJOR,
        protocolMinor: this.options.declaration.protocolMinor,
        domainId,
        create,
        clientNonce: Buffer.from(clientNonce).toString("base64url"),
      };
      if (create) {
        hello.domainKey = Buffer.from(this.options.declaration.domainKey).toString("base64url");
      }
      if (recover) hello.recover = true;
      if (!raw.send(hello)) {
        rej(new ProcessDomainFatalError("BROKER_UNAVAILABLE", "failed to send hello"));
      }
    });

    const brokerNonceRaw = unbase64url(typeof challenge.brokerNonce === "string" ? challenge.brokerNonce : "", 16);
    const brokerEpoch = typeof challenge.brokerEpoch === "string" ? challenge.brokerEpoch : "";
    if (!brokerNonceRaw || !brokerEpoch) {
      throw new ProcessDomainFatalError("AUTHENTICATION_FAILED", "broker returned a malformed challenge");
    }
    const brokerNonce = brokerNonceRaw;
    this.brokerEpoch = brokerEpoch;

    const mac = clientMac(this.authKey, domainId, clientNonce, brokerNonce, brokerEpoch);
    const expectedServerMac = serverMac(this.authKey, domainId, clientNonce, brokerNonce, brokerEpoch);

    const welcome = await new Promise<CanonicalObject>((res, rej) => {
      const timer = setTimeout(() => rej(new ProcessDomainFatalError("BROKER_UNAVAILABLE", "prove timeout")), Math.min(10000, this.options.connectTimeoutMs));
      const handler = (frame: CanonicalObject) => {
        if (frame.t === "error") {
          clearTimeout(timer);
          raw.removeHandler(handler);
          const code = frame.code as ProcessDomainFatalCode;
          const fatal = code === "AUTHENTICATION_FAILED" || code === "DOMAIN_ABSENT" || code === "PROTOCOL_MISMATCH" || code === "INVALID_DECLARATION";
          rej(new ProcessDomainFatalError(fatal ? code : "AUTHENTICATION_FAILED", "broker rejected authentication"));
        }
        else if (frame.t === "welcome") {
          clearTimeout(timer);
          raw.removeHandler(handler);
          res(frame);
        }
      };
      raw.addHandler(handler);
      if (!raw.send({ t: "prove", clientNonce: Buffer.from(clientNonce).toString("base64url"), brokerNonce: Buffer.from(brokerNonce).toString("base64url"), mac: Buffer.from(mac).toString("base64url") })) {
        rej(new ProcessDomainFatalError("BROKER_UNAVAILABLE", "failed to send prove"));
      }
    });

    const serverMacRaw = unbase64url(typeof welcome.serverMac === "string" ? welcome.serverMac : "", 32);
    if (serverMacRaw === null || serverMacRaw.length !== expectedServerMac.length || !macEqual(expectedServerMac, serverMacRaw)) {
      throw new ProcessDomainFatalError("AUTHENTICATION_FAILED", "server MAC verification failed");
    }

    this.upgradeToPeer();
    await this.join();
  }

  private upgradeToPeer(): void {
    const raw = this.raw!;
    const local = {
      snapshot: async (wire: Record<string, unknown>): Promise<Record<string, unknown>> => {
        this.applySnapshot(wireToSnapshot(wire, this.options.declaration.domainId));
        return { ok: true };
      },
      signal: async (payload: Record<string, unknown>): Promise<Record<string, unknown>> => {
        this.dispatchSignal(payload);
        return { ok: true };
      },
    };
    const peer = createRpcPeer(raw, local);
    this.peer = peer;
  }

  /** The single authenticated join (initial or reconnect resume). */
  private async join(): Promise<void> {
    const peer = this.peer;
    if (!peer) throw new ProcessDomainFatalError("BROKER_UNAVAILABLE", "not connected");
    const claim = decodeReservationClaim(process.env.PI_PROCESS_DOMAIN_RESERVATION);
    const nextIncarnation = this.incarnation + 1n;
    const args: Record<string, unknown> = {
      incarnation: nextIncarnation.toString(),
      activity: this.options.initialActivity,
      metadata: this.options.metadata,
    };
    if (this.participantId) {
      args.participantId = this.participantId;
      args.resumeKey = this.resumeKey;
    }
    if (claim) {
      args.reservationClaim = Buffer.from(claim).toString("base64url");
    }
    const result = (await peer.rpc.$call("join", compact(args))) as { participantId: string; resumeKey?: string; snapshot?: Record<string, unknown> };
    const participantId = typeof result.participantId === "string" ? result.participantId : "";
    const resumeKey = typeof result.resumeKey === "string" ? result.resumeKey : "";
    if (!participantId || !resumeKey || !result.snapshot) {
      throw new ProcessDomainFatalError("LEASE_REJECTED", "broker returned an invalid join result");
    }
    this.participantId = participantId;
    this.resumeKey = resumeKey;
    this.incarnation = nextIncarnation;
    this.applySnapshot(wireToSnapshot(result.snapshot, this.options.declaration.domainId));
    if (claim) delete process.env.PI_PROCESS_DOMAIN_RESERVATION;
    this.everJoined = true;
    this.startHeartbeat();
  }

  private startHeartbeat(): void {
    if (this.heartbeatTimer) clearInterval(this.heartbeatTimer);
    this.heartbeatTimer = setInterval(() => {
      const peer = this.peer;
      if (!peer || peer.closed) return;
      void peer.rpc.$call("heartbeat").catch(() => {
        /* heartbeat failure handled by reconnect path */
      });
    }, DEFAULT_HEARTBEAT_MS);
    if (typeof this.heartbeatTimer === "object" && "unref" in this.heartbeatTimer) {
      (this.heartbeatTimer as ReturnType<typeof setTimeout>).unref?.();
    }
  }

  private applySnapshot(snap: DomainSnapshot): void {
    this.lastSnapshot = snap;
    this.brokerEpoch = snap.brokerEpoch;
    this.flushPendingConfirms(snap);
    const copy = Array.from(this.listeners);
    for (const fn of copy) {
      try {
        fn(snap);
      }
      catch {
        /* listener error isolated */
      }
    }
  }

  private flushPendingConfirms(snap: DomainSnapshot): void {
    const pending = this.pendingConfirms;
    this.pendingConfirms = [];
    for (const p of pending) {
      const ok = p.fence.brokerEpoch === snap.brokerEpoch && p.fence.activityGeneration === snap.activityGeneration && snap.certain && snap.allIdle;
      p.resolve(ok);
    }
  }

  private scheduleReconnect(_error?: Error): void {
    if (this.closed || !this.everJoined) return;
    if (this.reconnectTimer) return;
    if (this.heartbeatTimer) clearInterval(this.heartbeatTimer);
    this.resetTransport();
    // Broker restart changes epoch; mark uncertain until exact re-registration.
    this.certainToUncertain();
    const delay = 200;
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      void this.joinThroughElection(true, true)
        .catch((err) => {
          if (!this.closed) {
            const fatal = isProcessDomainFatalError(err)
              ? err
              : new ProcessDomainFatalError("DOMAIN_UNRECOVERABLE", "process domain reconnect failed", err);
            this.certainToUncertain();
            this.emitFatal(fatal);
            void this.close();
          }
        });
    }, delay);
  }

  private certainToUncertain(): void {
    const snap = computeSnapshot({
      domainId: this.options.declaration.domainId,
      brokerEpoch: this.brokerEpoch,
      revision: this.lastSnapshot.revision,
      activityGeneration: this.lastSnapshot.activityGeneration,
      participants: this.lastSnapshot.participants,
      busyParticipants: this.lastSnapshot.busyParticipants,
      pendingSpawns: this.lastSnapshot.pendingSpawns,
      certain: false,
    });
    this.applySnapshot(snap);
  }

  // ---- Public interface ----

  snapshot(): DomainSnapshot {
    return this.lastSnapshot;
  }

  async setActivity(state: ActivityState): Promise<DomainSnapshot> {
    const peer = this.requirePeer();
    const result = (await peer.rpc.$call("setActivity", { activity: state })) as { snapshot: Record<string, unknown> };
    const snap = wireToSnapshot(result.snapshot, this.options.declaration.domainId);
    this.applySnapshot(snap);
    return snap;
  }

  async reserveSpawn(options?: { ttlMs?: number }): Promise<{ env: Record<string, string>; token: string; cancel: () => Promise<void> }> {
    const peer = this.requirePeer();
    const args: Record<string, unknown> = {};
    if (options?.ttlMs !== undefined) args.ttlMs = options.ttlMs;
    const result = (await peer.rpc.$call("reserveSpawn", args)) as { token: string; reservationId: string; expiry: number };
    const token = String(result.token);
    const env: Record<string, string> = {
      PI_PROCESS_DOMAIN_ID: this.options.declaration.domainId,
      PI_PROCESS_DOMAIN_KEY: Buffer.from(this.options.declaration.domainKey).toString("base64url"),
      PI_PROCESS_DOMAIN_PROTOCOL: `${PROCESS_DOMAIN_PROTOCOL_MAJOR}.${this.options.declaration.protocolMinor}`,
      PI_PROCESS_DOMAIN_RESERVATION: token,
    };
    const cancel = async () => {
      try {
        await peer.rpc.$call("cancelReservation", { token });
      }
      catch {
        /* ignore */
      }
    };
    return { env, token, cancel };
  }

  subscribe(listener: (snapshot: DomainSnapshot) => void): () => void {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }

  async publish(name: string, value: unknown): Promise<void> {
    const peer = this.requirePeer();
    let encoded: string;
    try {
      encoded = JSON.stringify(value);
    }
    catch (error) {
      throw new ProcessDomainFatalError("LEASE_REJECTED", "signal value is not JSON serializable", error);
    }
    if (encoded === undefined) {
      throw new ProcessDomainFatalError("LEASE_REJECTED", "signal value is not JSON serializable");
    }
    await peer.rpc.$call("publish", { name, value: encoded });
  }

  subscribeSignals(name: string, listener: (signal: unknown) => void): () => void {
    let set = this.signalListeners.get(name);
    if (!set) {
      set = new Set();
      this.signalListeners.set(name, set);
    }
    set.add(listener);
    return () => set.delete(listener);
  }

  private dispatchSignal(payload: Record<string, unknown>): void {
    const name = String(payload.name);
    const set = this.signalListeners.get(name);
    if (!set || set.size === 0) return;
    let value: unknown = payload.value;
    if (typeof payload.value === "string") {
      try {
        value = JSON.parse(payload.value);
      }
      catch {
        value = payload.value;
      }
    }
    const signal = {
      name,
      value,
      brokerEpoch: String(payload.brokerEpoch ?? ""),
      revision: BigInt(String(payload.revision ?? "0")),
      senderId: String(payload.senderId ?? ""),
    };
    for (const fn of Array.from(set)) {
      try {
        fn(signal);
      }
      catch {
        /* listener error isolated */
      }
    }
  }

  async confirm(fence: DomainFence): Promise<boolean> {
    const peer = this.peer;
    if (!peer || peer.closed || !this.lastSnapshot.certain) return false;
    try {
      const result = (await peer.rpc.$call("confirm", { brokerEpoch: fence.brokerEpoch, activityGeneration: fence.activityGeneration.toString() })) as { ok: boolean };
      return result?.ok === true;
    }
    catch {
      return false;
    }
  }

  async close(): Promise<void> {
    if (this.closed) return;
    this.closed = true;
    if (this.reconnectTimer) clearTimeout(this.reconnectTimer);
    if (this.heartbeatTimer) clearInterval(this.heartbeatTimer);
    const peer = this.peer;
    if (peer) {
      try {
        await peer.rpc.$callEvent("leave");
      }
      catch {
        /* ignore */
      }
    }
    this.resetTransport();
    this.listeners.clear();
    this.signalListeners.clear();
    const pending = this.pendingConfirms;
    this.pendingConfirms = [];
    for (const item of pending) item.resolve(false);
  }

  private requirePeer(): NonNullable<typeof this.peer> {
    if (!this.peer || this.peer.closed) {
      throw new ProcessDomainFatalError("BROKER_UNAVAILABLE", "not connected to broker");
    }
    return this.peer;
  }

  private emitFatal(error: Error): void {
    if (this.fatalEmitted) return;
    this.fatalEmitted = true;
    try {
      this.options.onFatal(error);
    }
    catch {
      /* ignore */
    }
  }
}
