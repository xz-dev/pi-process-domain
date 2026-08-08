/**
 * Broker server. Listens on the per-user endpoint (Unix socket / Windows named
 * pipe), authenticates each connection via the HMAC challenge, and serves the
 * domain state machine over a birpc channel. This is an internal implementation
 * detail, launched lazily by the client; it is not a user-managed daemon.
 *
 * Correctness guarantees implemented here:
 *   - Only *received* traffic updates a participant's `lastSeen`; the broker
 *     never refreshes liveness on its own (real client heartbeats).
 *   - Disconnect/suspect/expiry transitions drive `certain=false` during the
 *     uncertainty interval; `allIdle` is never asserted while uncertain.
 *   - Broker restart creates the domain in recovery mode (`certain=false`)
 *     until the recovery/lease window authoritatively expires unknown prior
 *     membership.
 *   - Reservation adoption is fail-closed and exact (token+domain+expiry+single
 *     use); cancel is authorized only by the reservation creator.
 *   - Participant identity + broker-issued resume key with a strictly increasing
 *     incarnation; stale/equal incarnations are rejected.
 */

import type { Server, Socket } from "node:net";
import * as net from "node:net";
import * as fs from "node:fs";
import {
  clientMac,
  deriveDomainAuthKey,
  macEqual,
  randomNonce,
  serverMac,
  unbase64url,
} from "./auth.js";
import {
  DEFAULT_HEARTBEAT_MS,
  EXPIRE_MS,
  MAX_DOMAIN_ID_LENGTH,
  MAX_INCARNATION,
  MAX_METADATA_KEYS,
  MAX_METADATA_KEY_LENGTH,
  MAX_METADATA_VALUE_LENGTH,
  MAX_PARTICIPANT_ID_LENGTH,
  MAX_PARTICIPANTS_PER_DOMAIN,
  MAX_RESERVATIONS_PER_DOMAIN,
  MAX_RESERVATIONS_PER_PARTICIPANT,
  MAX_SIGNAL_NAME_LENGTH,
  MAX_SIGNAL_VALUE_BYTES,
  MIN_INCARNATION,
  MIN_RESERVATION_TTL_MS,
  RESERVATION_MAX_TTL_MS,
  RESERVATION_TTL_MS,
  SUSPECT_MS,
  hashToken,
  makeParticipantId,
  makeReservationId,
  makeResumeKey,
  newDomain,
  reservationToken,
  snapshotOf,
  snapshotToWire,
  type DomainState,
  type ParticipantLease,
  type ReservationLease,
} from "./broker-state.js";
import { PROCESS_DOMAIN_PROTOCOL_MAJOR, PROCESS_DOMAIN_PROTOCOL_MINOR, ProcessDomainFatalError } from "./errors.js";
import { type CanonicalObject } from "./framing.js";
import { RawChannel, createRpcPeer } from "./rpc.js";
import { resolveEndpoint, type RuntimeEndpoint } from "./runtime-path.js";
import { claimElectionForBroker, tryAcquireElection, reclaimStaleElection, releaseElection } from "./launcher.js";

export interface BrokerOptions {
  endpoint: RuntimeEndpoint;
  electionOwner?: string;
}

interface Conn {
  socket: Socket;
  domainId: string | null;
  participantId: string | null;
  incarnation: bigint;
  raw: RawChannel | null;
  peer: ReturnType<typeof createRpcPeer> | null;
  heartbeatTimer: ReturnType<typeof setInterval> | null;
  sweepTimer: ReturnType<typeof setInterval> | null;
  domainState: DomainState | null;
  clientNonce: Uint8Array | null;
  brokerNonce: Uint8Array | null;
  handshakeHandler: ((frame: CanonicalObject) => void) | null;
}

function wireSnapshot(state: DomainState): Record<string, unknown> {
  return snapshotToWire(snapshotOf(state));
}

function isValidId(id: string, max: number): boolean {
  return id.length > 0 && id.length <= max && /^[A-Za-z0-9_-]+$/.test(id);
}

function validateMetadata(metadata: unknown): Record<string, string> {
  if (metadata === null || typeof metadata !== "object" || Array.isArray(metadata)) {
    throw new ProcessDomainFatalError("LEASE_REJECTED", "metadata must be an object");
  }
  const entries = Object.entries(metadata as Record<string, unknown>);
  if (entries.length > MAX_METADATA_KEYS) {
    throw new ProcessDomainFatalError("LEASE_REJECTED", "too many metadata entries");
  }
  const out: Record<string, string> = {};
  for (const [k, v] of entries) {
    if (k.length === 0 || k.length > MAX_METADATA_KEY_LENGTH) {
      throw new ProcessDomainFatalError("LEASE_REJECTED", "invalid metadata key");
    }
    if (typeof v !== "string" || v.length > MAX_METADATA_VALUE_LENGTH) {
      throw new ProcessDomainFatalError("LEASE_REJECTED", "invalid metadata value");
    }
    out[k] = v;
  }
  return out;
}

export class Broker {
  private domains = new Map<string, DomainState>();
  private conns = new Set<Conn>();
  private server: Server;
  private electionOwner: string | null;

  constructor(private options: BrokerOptions) {
    this.electionOwner = options.electionOwner ?? null;
    this.server = net.createServer((socket) => this.onConnection(socket));
  }

  async start(): Promise<void> {
    const ep = this.options.endpoint.endpointPath;
    try {
      await this.listen(ep);
    }
    catch (err) {
      const code = (err as NodeJS.ErrnoException).code;
      if (code === "EADDRINUSE") {
        // A broker may be listening on this endpoint, or a stale Unix socket may
        // linger. Probe the endpoint; only if no live broker answers do we
        // reclaim the stale socket and retry.
        if (this.options.endpoint.platform === "unix") {
          const live = await this.probeEndpoint(ep);
          if (!live) {
            try {
              fs.unlinkSync(ep);
            }
            catch {
              /* best-effort */
            }
            await this.listen(ep);
            return;
          }
        }
        throw new ProcessDomainFatalError(
          "BROKER_UNAVAILABLE",
          `broker endpoint already in use by a live broker: ${ep}`,
          err,
        );
      }
      throw err;
    }
  }

  /** Connect to the endpoint and confirm a live broker answers (or the socket is absent). */
  private probeEndpoint(ep: string): Promise<boolean> {
    return new Promise((resolve) => {
      const socket = net.connect(ep);
      const timer = setTimeout(() => {
        socket.destroy();
        resolve(false);
      }, 1500);
      socket.once("connect", () => {
        clearTimeout(timer);
        socket.destroy();
        resolve(true);
      });
      socket.once("error", () => {
        clearTimeout(timer);
        socket.destroy();
        resolve(false);
      });
    });
  }

  private listen(ep: string): Promise<void> {
    return new Promise((resolve, reject) => {
      this.server.once("error", reject);
      // Unix socket: restrictive permissions so other users cannot connect.
      const opts = this.options.endpoint.platform === "unix"
        ? { path: ep, mode: 0o600 }
        : { path: ep };
      this.server.listen(opts as never, () => {
        this.server.removeListener("error", reject);
        resolve();
      });
    });
  }

  private onConnection(socket: Socket): void {
    const conn: Conn = {
      socket,
      domainId: null,
      participantId: null,
      incarnation: 0n,
      raw: null,
      peer: null,
      heartbeatTimer: null,
      sweepTimer: null,
      domainState: null,
      clientNonce: null,
      brokerNonce: null,
      handshakeHandler: null,
    };
    this.conns.add(conn);

    const raw = new RawChannel(
      socket,
      () => {
        this.teardown(conn);
      },
      (error) => {
        this.teardown(conn, error);
      },
    );
    conn.raw = raw;

    const handshakeHandler = (frame: CanonicalObject) => {
      void this.onHandshake(conn, frame).catch(() => raw.destroy());
    };
    conn.handshakeHandler = handshakeHandler;
    raw.addHandler(handshakeHandler);
  }

  private async onHandshake(conn: Conn, frame: CanonicalObject): Promise<void> {
    const raw = conn.raw;
    if (!raw) return;

    if (frame.t === "hello") {
      const major = frame.protocolMajor;
      const minor = frame.protocolMinor;
      if (
        typeof major !== "number" || !Number.isInteger(major) || major !== PROCESS_DOMAIN_PROTOCOL_MAJOR ||
        typeof minor !== "number" || !Number.isInteger(minor) || minor !== PROCESS_DOMAIN_PROTOCOL_MINOR
      ) {
        raw.send({ t: "error", code: "PROTOCOL_MISMATCH" });
        raw.destroy();
        return;
      }
      const domainId = typeof frame.domainId === "string" ? frame.domainId : "";
      if (!isValidId(domainId, MAX_DOMAIN_ID_LENGTH)) {
        raw.send({ t: "error", code: "INVALID_DECLARATION" });
        raw.destroy();
        return;
      }
      const clientNonceRaw = unbase64url(typeof frame.clientNonce === "string" ? frame.clientNonce : "", 16);
      if (clientNonceRaw === null) {
        raw.destroy();
        return;
      }
      let state = this.domains.get(domainId);
      if (!state) {
        if (frame.create === true) {
          const keyBytes = unbase64url(typeof frame.domainKey === "string" ? frame.domainKey : "", 32);
          if (!keyBytes) {
            raw.send({ t: "error", code: "INVALID_DECLARATION" });
            raw.destroy();
            return;
          }
          state = newDomain(domainId, deriveDomainAuthKey(keyBytes, domainId), {
            recover: frame.recover === true,
          });
          this.domains.set(domainId, state);
        }
        else {
          raw.send({ t: "error", code: "DOMAIN_ABSENT" });
          raw.destroy();
          return;
        }
      }
      conn.domainId = domainId;
      conn.domainState = state;
      conn.clientNonce = clientNonceRaw;
      const brokerNonce = randomNonce();
      conn.brokerNonce = brokerNonce;
      raw.send({
        t: "challenge",
        protocolMajor: PROCESS_DOMAIN_PROTOCOL_MAJOR,
        protocolMinor: PROCESS_DOMAIN_PROTOCOL_MINOR,
        brokerEpoch: state.brokerEpoch,
        brokerNonce: Buffer.from(brokerNonce).toString("base64url"),
      });
      return;
    }

    if (frame.t === "prove") {
      const state = conn.domainState;
      if (!state || !conn.domainId) {
        raw.destroy();
        return;
      }
      const clientNonceRaw = unbase64url(typeof frame.clientNonce === "string" ? frame.clientNonce : "", 16);
      const brokerNonceRaw = unbase64url(typeof frame.brokerNonce === "string" ? frame.brokerNonce : "", 16);
      const theirMacRaw = unbase64url(typeof frame.mac === "string" ? frame.mac : "", 32);
      if (
        clientNonceRaw === null || brokerNonceRaw === null || theirMacRaw === null ||
        conn.clientNonce === null || conn.brokerNonce === null ||
        !macEqual(clientNonceRaw, conn.clientNonce) || !macEqual(brokerNonceRaw, conn.brokerNonce)
      ) {
        raw.send({ t: "error", code: "AUTHENTICATION_FAILED" });
        raw.destroy();
        return;
      }
      const expected = clientMac(state.domainAuthKey, state.domainId, clientNonceRaw, brokerNonceRaw, state.brokerEpoch);
      if (theirMacRaw.length !== expected.length || !macEqual(expected, theirMacRaw)) {
        raw.send({ t: "error", code: "AUTHENTICATION_FAILED" });
        raw.destroy();
        return;
      }
      const serverMacBuf = serverMac(state.domainAuthKey, state.domainId, clientNonceRaw, brokerNonceRaw, state.brokerEpoch);
      raw.send({
        t: "welcome",
        revision: state.revision.toString(),
        leaseParams: { heartbeatMs: DEFAULT_HEARTBEAT_MS, expireMs: EXPIRE_MS },
        serverMac: Buffer.from(serverMacBuf).toString("base64url"),
      });
      conn.clientNonce = null;
      conn.brokerNonce = null;
      if (conn.handshakeHandler) raw.removeHandler(conn.handshakeHandler);
      conn.handshakeHandler = null;
      this.upgradeToPeer(conn, state);
      return;
    }

    raw.destroy();
  }

  private upgradeToPeer(conn: Conn, state: DomainState): void {
    const raw = conn.raw;
    if (!raw) return;

    const local = {
      join: async (args: CanonicalObject): Promise<Record<string, unknown>> => {
        let incarnationRaw: bigint;
        try {
          incarnationRaw = BigInt(typeof args.incarnation === "string" && /^[1-9][0-9]*$/.test(args.incarnation) ? args.incarnation : "0");
        }
        catch {
          throw new ProcessDomainFatalError("LEASE_REJECTED", "incarnation is malformed");
        }
        if (incarnationRaw < MIN_INCARNATION || incarnationRaw > MAX_INCARNATION) {
          throw new ProcessDomainFatalError("LEASE_REJECTED", "incarnation out of range");
        }
        if (args.activity !== "busy" && args.activity !== "idle") {
          throw new ProcessDomainFatalError("LEASE_REJECTED", "invalid activity state");
        }
        const activity = args.activity;
        const metadata = validateMetadata(args.metadata);

        let participantId = typeof args.participantId === "string" ? args.participantId : "";
        const suppliedResumeKey = typeof args.resumeKey === "string" ? args.resumeKey : "";
        let returnedResumeKey = suppliedResumeKey;
        let lease: ParticipantLease | undefined;

        if (participantId.length > 0 || suppliedResumeKey.length > 0) {
          if (!isValidId(participantId, MAX_PARTICIPANT_ID_LENGTH)) {
            throw new ProcessDomainFatalError("LEASE_REJECTED", "invalid resume identity");
          }
          const resumeBytes = unbase64url(suppliedResumeKey, 32);
          if (!resumeBytes) {
            throw new ProcessDomainFatalError("LEASE_REJECTED", "invalid resume capability");
          }
          const existing = state.participants.get(participantId);
          if (existing) {
            if (existing.resumeKeyHash !== hashToken(resumeBytes)) {
              throw new ProcessDomainFatalError("LEASE_REJECTED", "resume key mismatch");
            }
            if (incarnationRaw <= existing.incarnation) {
              throw new ProcessDomainFatalError("LEASE_REJECTED", "stale or equal incarnation rejected");
            }
            for (const other of this.conns) {
              if (other !== conn && other.domainState === state && other.participantId === participantId) {
                other.participantId = null;
                other.peer?.close(new ProcessDomainFatalError("LEASE_REJECTED", "participant incarnation superseded"));
              }
            }
            lease = existing;
            lease.incarnation = incarnationRaw;
            lease.activity = activity;
            lease.metadata = metadata;
            lease.connected = true;
            lease.lastSeen = Date.now();
            lease.disconnectedSince = null;
          }
          else if (state.recoveryDeadline !== null) {
            // New broker epoch: authenticate continuity with the old resume
            // capability, preserve the exact participant identity, and remain
            // uncertain until the full recovery lease window closes.
            lease = {
              participantId,
              incarnation: incarnationRaw,
              resumeKeyHash: hashToken(resumeBytes),
              activity,
              connected: true,
              lastSeen: Date.now(),
              disconnectedSince: null,
              metadata,
            };
            state.recoveryParticipantSeen = true;
          }
          else {
            throw new ProcessDomainFatalError("LEASE_REJECTED", "unknown resume identity");
          }
        }

        if (!lease) {
          if (state.participants.size >= MAX_PARTICIPANTS_PER_DOMAIN) {
            throw new ProcessDomainFatalError("LEASE_REJECTED", "too many participants");
          }
          participantId = makeParticipantId();
          returnedResumeKey = makeResumeKey();
          const resumeBytes = unbase64url(returnedResumeKey, 32);
          if (!resumeBytes) throw new ProcessDomainFatalError("LEASE_REJECTED", "failed to issue resume capability");
          lease = {
            participantId,
            incarnation: incarnationRaw,
            resumeKeyHash: hashToken(resumeBytes),
            activity,
            connected: true,
            lastSeen: Date.now(),
            disconnectedSince: null,
            metadata,
          };
        }

        // Reservation claim: fail-closed mandatory adoption.
        if (typeof args.reservationClaim === "string" && args.reservationClaim.length > 0) {
          const claimRaw = unbase64url(args.reservationClaim);
          if (!claimRaw) {
            throw new ProcessDomainFatalError("LEASE_REJECTED", "invalid reservation claim");
          }
          const res = state.reservations.get(hashToken(claimRaw));
          if (!res || !res.active) {
            throw new ProcessDomainFatalError("LEASE_REJECTED", "reservation missing or already used");
          }
          if (res.domainId !== state.domainId) {
            throw new ProcessDomainFatalError("LEASE_REJECTED", "reservation bound to a different domain");
          }
          if (Date.now() > res.expiry) {
            this.clearReservationTimer(res);
            state.reservations.delete(res.tokenHash);
            throw new ProcessDomainFatalError("LEASE_REJECTED", "reservation expired");
          }
          // Single-use adoption: consume atomically.
          this.clearReservationTimer(res);
          state.reservations.delete(res.tokenHash);
          state.activityGeneration += 1n;
        }

        state.participants.set(participantId, lease);
        state.revision += 1n;
        state.activityGeneration += 1n;
        conn.participantId = participantId;
        conn.incarnation = incarnationRaw;
        this.recomputeCertain(state);
        this.startHeartbeat(conn, state);
        const joinedSnapshot = wireSnapshot(state);
        // Complete the joining RPC before publishing membership to peers. A
        // synchronous snapshot callback into this same connection can otherwise
        // race the join response on slow or newly upgraded transports.
        queueMicrotask(() => this.broadcastSnapshot(state));
        return { participantId, resumeKey: returnedResumeKey, snapshot: joinedSnapshot };
      },

      setActivity: async (args: CanonicalObject): Promise<Record<string, unknown>> => {
        const p = this.requireParticipant(conn, state);
        if (args.activity !== "busy" && args.activity !== "idle") {
          throw new ProcessDomainFatalError("LEASE_REJECTED", "invalid activity state");
        }
        const activity = args.activity;
        if (p.activity !== activity) {
          p.activity = activity;
          state.revision += 1n;
          state.activityGeneration += 1n;
          this.broadcastSnapshot(state);
        }
        return { snapshot: wireSnapshot(state) };
      },

      heartbeat: async (): Promise<Record<string, unknown>> => {
        const p = this.requireParticipant(conn, state);
        p.lastSeen = Date.now();
        if (!p.connected) {
          p.connected = true;
          p.disconnectedSince = null;
          state.revision += 1n;
          this.recomputeCertain(state);
          this.broadcastSnapshot(state);
        }
        return { ok: true };
      },

      reserveSpawn: async (args: CanonicalObject): Promise<Record<string, unknown>> => {
        const p = this.requireParticipant(conn, state);
        let ttl = RESERVATION_TTL_MS;
        if (args.ttlMs !== undefined) {
          if (!Number.isSafeInteger(args.ttlMs) || Number(args.ttlMs) < MIN_RESERVATION_TTL_MS || Number(args.ttlMs) > RESERVATION_MAX_TTL_MS) {
            throw new ProcessDomainFatalError("LEASE_REJECTED", "reservation ttl is out of range");
          }
          ttl = Number(args.ttlMs);
        }
        let count = 0;
        for (const r of state.reservations.values()) {
          if (r.creatorParticipantId === p.participantId) count += 1;
        }
        if (count >= MAX_RESERVATIONS_PER_PARTICIPANT || state.reservations.size >= MAX_RESERVATIONS_PER_DOMAIN) {
          throw new ProcessDomainFatalError("LEASE_REJECTED", "too many reservations");
        }
        const token = reservationToken();
        const reservationId = makeReservationId();
        const expiry = Date.now() + ttl;
        const reservation: ReservationLease = {
          tokenHash: hashToken(token),
          reservationId,
          domainId: state.domainId,
          creatorParticipantId: p.participantId,
          expiry,
          active: true,
        };
        reservation.timer = setTimeout(() => this.expireReservation(state, reservation), ttl);
        state.reservations.set(reservation.tokenHash, reservation);
        state.revision += 1n;
        state.activityGeneration += 1n;
        this.broadcastSnapshot(state);
        return {
          token: Buffer.from(token).toString("base64url"),
          reservationId,
          expiry,
        };
      },

      cancelReservation: async (args: CanonicalObject): Promise<Record<string, unknown>> => {
        const p = this.requireParticipant(conn, state);
        const tokenRaw = unbase64url(typeof args.token === "string" ? args.token : "");
        if (!tokenRaw) {
          throw new ProcessDomainFatalError("LEASE_REJECTED", "invalid reservation token");
        }
        const res = state.reservations.get(hashToken(tokenRaw));
        if (!res || !res.active) {
          throw new ProcessDomainFatalError("LEASE_REJECTED", "reservation missing or already consumed");
        }
        if (res.creatorParticipantId !== p.participantId) {
          throw new ProcessDomainFatalError("LEASE_REJECTED", "only the reservation creator may cancel");
        }
        this.clearReservationTimer(res);
        state.reservations.delete(res.tokenHash);
        state.revision += 1n;
        state.activityGeneration += 1n;
        this.broadcastSnapshot(state);
        return { ok: true };
      },

      snapshot: async (): Promise<Record<string, unknown>> => {
        this.requireParticipant(conn, state);
        return wireSnapshot(state);
      },

      confirm: async (args: CanonicalObject): Promise<Record<string, unknown>> => {
        this.requireParticipant(conn, state);
        const fenceEpoch = typeof args.brokerEpoch === "string" ? args.brokerEpoch : "";
        const fenceGen = BigInt(typeof args.activityGeneration === "string" && args.activityGeneration !== "" ? args.activityGeneration : "0");
        const snap = snapshotOf(state);
        const ok = fenceEpoch === state.brokerEpoch && fenceGen === state.activityGeneration && snap.certain && snap.allIdle;
        return { ok };
      },

      leave: async (): Promise<Record<string, unknown>> => {
        this.removeParticipant(conn, state);
        return { ok: true };
      },

      publish: async (args: CanonicalObject): Promise<Record<string, unknown>> => {
        const p = this.requireParticipant(conn, state);
        const name = typeof args.name === "string" ? args.name : "";
        if (name.length === 0 || name.length > MAX_SIGNAL_NAME_LENGTH) {
          throw new ProcessDomainFatalError("LEASE_REJECTED", "invalid signal name");
        }
        let value: unknown = args.value;
        if (typeof args.value === "string") {
          try {
            value = JSON.parse(args.value);
          }
          catch {
            throw new ProcessDomainFatalError("LEASE_REJECTED", "invalid signal value");
          }
        }
        if (Buffer.byteLength(typeof value === "string" ? value : JSON.stringify(value)) > MAX_SIGNAL_VALUE_BYTES) {
          throw new ProcessDomainFatalError("LEASE_REJECTED", "signal value too large");
        }
        state.revision += 1n;
        this.broadcastSignal(state, { name, value, senderId: p.participantId });
        return { ok: true };
      },
    };

    const peer = createRpcPeer(raw, local);
    conn.peer = peer;
  }

  private clearReservationTimer(res: ReservationLease): void {
    if (res.timer) clearTimeout(res.timer);
    res.timer = undefined;
  }

  private expireReservation(state: DomainState, res: ReservationLease): void {
    const current = state.reservations.get(res.tokenHash);
    if (current === res && res.active) {
      state.reservations.delete(res.tokenHash);
      state.revision += 1n;
      state.activityGeneration += 1n;
      this.broadcastSnapshot(state);
    }
  }

  private requireParticipant(conn: Conn, state: DomainState): ParticipantLease {
    const p = conn.participantId ? state.participants.get(conn.participantId) : undefined;
    if (!p || p.incarnation !== conn.incarnation || !p.connected) {
      throw new ProcessDomainFatalError("LEASE_REJECTED", "participant lease is not current");
    }
    p.lastSeen = Date.now();
    return p;
  }

  /**
   * Real heartbeat liveness: only received `heartbeat` RPCs refresh lastSeen.
   * This periodic sweep detects stale (suspected) and expired participants based
   * on that received-evidence timestamp and drives certainty transitions.
   */
  private startHeartbeat(conn: Conn, state: DomainState): void {
    if (conn.heartbeatTimer) clearInterval(conn.heartbeatTimer);
    if (conn.sweepTimer) clearInterval(conn.sweepTimer);
    const sweep = () => {
      const p = conn.participantId ? state.participants.get(conn.participantId) : undefined;
      if (!p) return;
      if (!p.connected) {
        if (p.disconnectedSince !== null && Date.now() - p.disconnectedSince > EXPIRE_MS) {
          this.expireParticipant(state, p.participantId);
        }
        return;
      }
      const idleMs = Date.now() - p.lastSeen;
      if (idleMs > EXPIRE_MS) {
        this.expireParticipant(state, p.participantId);
      }
      else if (idleMs > SUSPECT_MS) {
        // Suspected: mark certain=false until a heartbeat resumes it.
        if (state.certain) {
          this.recomputeCertain(state);
          this.broadcastSnapshot(state);
        }
      }
    };
    conn.heartbeatTimer = setInterval(sweep, DEFAULT_HEARTBEAT_MS);
    // A slower sweep also enforces recovery-certainty transitions;
    // recomputeCertain publishes exactly once when certainty changes.
    conn.sweepTimer = setInterval(() => this.recomputeCertain(state), 1000);
  }

  private expireParticipant(state: DomainState, participantId: string): void {
    const p = state.participants.get(participantId);
    if (!p) return;
    state.participants.delete(participantId);
    state.revision += 1n;
    state.activityGeneration += 1n;
    this.recomputeCertain(state);
    this.broadcastSnapshot(state);
  }

  private recomputeCertain(state: DomainState): void {
    let uncertain = false;
    if (state.recoveryDeadline !== null && (Date.now() < state.recoveryDeadline || !state.recoveryParticipantSeen)) {
      // Restart-recovery window not yet closed: unknown prior membership means
      // certainty would be a false assertion. A domain with no successful
      // authenticated rejoin remains uncertain indefinitely.
      uncertain = true;
    }
    if (!uncertain) {
      for (const p of state.participants.values()) {
        if (!p.connected) { uncertain = true; break; }
        if (Date.now() - p.lastSeen > SUSPECT_MS) { uncertain = true; break; }
      }
    }
    const next = !uncertain;
    if (next !== state.certain) {
      state.certain = next;
      if (next && state.recoveryDeadline !== null) {
        // The recovery window has authoritatively expired the unknown set.
        state.recoveryDeadline = null;
      }
      this.broadcastSnapshot(state);
    }
  }

  private removeParticipant(conn: Conn, state: DomainState): void {
    if (conn.participantId) {
      const p = state.participants.get(conn.participantId);
      if (p?.incarnation === conn.incarnation) this.expireParticipant(state, conn.participantId);
    }
    conn.participantId = null;
    if (conn.heartbeatTimer) clearInterval(conn.heartbeatTimer);
    if (conn.sweepTimer) clearInterval(conn.sweepTimer);
  }

  private teardown(conn: Conn, _error?: Error): void {
    if (conn.heartbeatTimer) clearInterval(conn.heartbeatTimer);
    if (conn.sweepTimer) clearInterval(conn.sweepTimer);
    this.conns.delete(conn);
    if (conn.participantId && conn.domainState) {
      const state = conn.domainState;
      const p = state.participants.get(conn.participantId);
      if (p && p.incarnation === conn.incarnation) {
        p.connected = false;
        p.disconnectedSince = Date.now();
        state.revision += 1n;
        this.recomputeCertain(state);
        this.broadcastSnapshot(state);
        // Expire after the lease if not reconnected.
        const pid = conn.participantId;
        setTimeout(() => {
          const nowP = state.participants.get(pid);
          if (nowP && !nowP.connected) this.expireParticipant(state, pid);
        }, EXPIRE_MS);
      }
    }
  }

  private broadcastSnapshot(state: DomainState): void {
    const snap = wireSnapshot(state);
    for (const conn of this.conns) {
      if (conn.domainState === state && conn.peer && conn.participantId) {
        void conn.peer.rpc.$callEvent("snapshot", snap);
      }
    }
  }

  private broadcastSignal(state: DomainState, signal: { name: string; value: unknown; senderId: string }): void {
    const payload = {
      name: signal.name,
      value: JSON.stringify(signal.value),
      senderId: signal.senderId,
      brokerEpoch: state.brokerEpoch,
      revision: state.revision.toString(),
    };
    for (const conn of this.conns) {
      if (conn.domainState === state && conn.peer && conn.participantId) {
        void conn.peer.rpc.$callEvent("signal", payload);
      }
    }
  }

  async close(): Promise<void> {
    for (const conn of Array.from(this.conns)) this.teardown(conn);
    this.conns.clear();
    await new Promise<void>((resolve) => this.server.close(() => resolve()));
    if (this.options.endpoint.platform === "unix") {
      try {
        fs.unlinkSync(this.options.endpoint.endpointPath);
      }
      catch {
        /* ignore */
      }
    }
    if (this.electionOwner !== null) releaseElection(this.electionOwner);
  }
}

/** Launch a broker for the current user endpoint (used by the CLI harness). */
export async function launchBrokerForCurrentUser(electionOwner?: string): Promise<Broker> {
  reclaimStaleElection();
  const endpoint = resolveEndpoint();
  const owner = electionOwner ?? tryAcquireElection();
  if (owner === null || !claimElectionForBroker(owner)) {
    throw new ProcessDomainFatalError("BROKER_UNAVAILABLE", "another broker holds the election lock");
  }
  const broker = new Broker({ endpoint, electionOwner: owner });
  try {
    await broker.start();
    return broker;
  }
  catch (error) {
    releaseElection(owner);
    throw error;
  }
}
