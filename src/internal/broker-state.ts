/**
 * Broker domain state (v1: in-memory, no durable journal).
 *
 * Broker restart is scoped to a new random epoch plus client reconnect and
 * re-registration. To guarantee `allIdle` is never asserted before all prior
 * participants have had a chance to reconcile, a domain that is (re)provisioned
 * after a broker restart starts `certain=false` and only becomes certain after
 * the recovery/lease window elapses with at least one registered participant.
 * A genuinely brand-new domain (first-ever join, never rejoining) may start
 * certain once its creator has joined.
 *
 * Participant identity: each participant has a stable `participantId` and a
 * broker-issued `resumeKey` capability. Reconnects resume the same identity by
 * presenting `participantId + resumeKey` with a strictly increasing
 * `incarnation`; stale or equal incarnations are rejected.
 *
 * Reservations: a spawn reservation binds token + domain + reservationId +
 * expiry + creator participant; adoption is single-use and exact.
 */

import { randomBytes } from "node:crypto";
import { base64url, sha256Hex } from "./auth.js";
import { computeSnapshot, type ActivityState, type DomainSnapshot } from "./domain-types.js";

export const DEFAULT_HEARTBEAT_MS = 2000;
export const SUSPECT_MS = 6000;
export const EXPIRE_MS = 10000;
/** Window after a broker restart during which certainty remains false. */
export const RECOVERY_MS = 15000;
export const RESERVATION_TTL_MS = 30000;
export const RESERVATION_MAX_TTL_MS = 120000;
export const MIN_RESERVATION_TTL_MS = 1000;

// ---- Wire / state bounds ----
export const MAX_PARTICIPANTS_PER_DOMAIN = 256;
export const MAX_RESERVATIONS_PER_DOMAIN = 128;
export const MAX_RESERVATIONS_PER_PARTICIPANT = 32;
export const MAX_METADATA_KEYS = 64;
export const MAX_METADATA_KEY_LENGTH = 64;
export const MAX_METADATA_VALUE_LENGTH = 4096;
export const MAX_SIGNAL_NAME_LENGTH = 128;
export const MAX_SIGNAL_VALUE_BYTES = 64 * 1024;
export const MAX_PARTICIPANT_ID_LENGTH = 128;
export const MAX_DOMAIN_ID_LENGTH = 128;
export const MIN_INCARNATION = 1n;
export const MAX_INCARNATION = 9223372036854775807n;

export interface ParticipantLease {
  participantId: string;
  incarnation: bigint;
  /** Hash of the broker-issued resume capability (never stored in plaintext). */
  resumeKeyHash: string;
  activity: ActivityState;
  connected: boolean;
  lastSeen: number;
  disconnectedSince: number | null;
  metadata: Readonly<Record<string, string>>;
}

export interface ReservationLease {
  tokenHash: string;
  reservationId: string;
  domainId: string;
  creatorParticipantId: string;
  expiry: number;
  active: boolean;
  timer?: ReturnType<typeof setTimeout>;
}

export interface DomainState {
  domainId: string;
  domainAuthKey: Uint8Array;
  brokerEpoch: string;
  revision: bigint;
  activityGeneration: bigint;
  participants: Map<string, ParticipantLease>;
  reservations: Map<string, ReservationLease>;
  /** True when the membership/lease state is authoritatively known. */
  certain: boolean;
  /** When set, certainty may become true only after this timestamp (recovery window). */
  recoveryDeadline: number | null;
  /** A restarted domain needs at least one authenticated rejoin before recovery can finish. */
  recoveryParticipantSeen: boolean;
  createdAt: number;
}

export function newDomain(
  domainId: string,
  domainAuthKey: Uint8Array,
  opts: { recover?: boolean } = {},
): DomainState {
  const recover = opts.recover === true;
  return {
    domainId,
    domainAuthKey,
    brokerEpoch: base64url(randomBytes(16)),
    revision: 0n,
    activityGeneration: 0n,
    participants: new Map(),
    reservations: new Map(),
    certain: !recover,
    recoveryDeadline: recover ? Date.now() + RECOVERY_MS : null,
    recoveryParticipantSeen: !recover,
    createdAt: Date.now(),
  };
}

export function makeParticipantId(): string {
  return base64url(randomBytes(16));
}

export function makeResumeKey(): string {
  return base64url(randomBytes(32));
}

export function makeReservationId(): string {
  return base64url(randomBytes(12));
}

export function hashToken(token: Uint8Array): string {
  return sha256Hex(Buffer.from(token).toString("base64url"));
}

export function reservationToken(): Uint8Array {
  return randomBytes(32);
}

export function snapshotOf(state: DomainState): DomainSnapshot {
  let busy = 0;
  for (const p of state.participants.values()) {
    if (p.activity === "busy") busy += 1;
  }
  return computeSnapshot({
    domainId: state.domainId,
    brokerEpoch: state.brokerEpoch,
    revision: state.revision,
    activityGeneration: state.activityGeneration,
    participants: state.participants.size,
    busyParticipants: busy,
    pendingSpawns: state.reservations.size,
    certain: state.certain,
  });
}

/** Wire representation of a snapshot (bigint as decimal string). */
export function snapshotToWire(s: DomainSnapshot): Record<string, unknown> {
  return {
    domainId: s.domainId,
    brokerEpoch: s.brokerEpoch,
    revision: s.revision.toString(),
    activityGeneration: s.activityGeneration.toString(),
    participants: s.participants,
    busyParticipants: s.busyParticipants,
    pendingSpawns: s.pendingSpawns,
    allIdle: s.allIdle,
    certain: s.certain,
    fence: { brokerEpoch: s.brokerEpoch, activityGeneration: s.activityGeneration.toString() },
  };
}
