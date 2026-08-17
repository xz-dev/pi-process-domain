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
import { type ActivityState, type CycleCounterSnapshot, type DomainSnapshot } from "./domain-types.js";
export declare const DEFAULT_HEARTBEAT_MS = 2000;
export declare const SUSPECT_MS = 6000;
export declare const EXPIRE_MS = 10000;
/** Window after a broker restart during which certainty remains false. */
export declare const RECOVERY_MS = 15000;
export declare const RESERVATION_TTL_MS = 30000;
export declare const RESERVATION_MAX_TTL_MS = 120000;
export declare const MIN_RESERVATION_TTL_MS = 1000;
export declare const MAX_PARTICIPANTS_PER_DOMAIN = 256;
export declare const MAX_RESERVATIONS_PER_DOMAIN = 128;
export declare const MAX_RESERVATIONS_PER_PARTICIPANT = 32;
export declare const MAX_METADATA_KEYS = 64;
export declare const MAX_METADATA_KEY_LENGTH = 64;
export declare const MAX_METADATA_VALUE_LENGTH = 4096;
export declare const MAX_SIGNAL_NAME_LENGTH = 128;
export declare const MAX_SIGNAL_VALUE_BYTES: number;
export declare const MAX_CYCLE_COUNTERS_PER_DOMAIN = 64;
export declare const MAX_CYCLE_COUNTER_NAME_LENGTH = 128;
export declare const MAX_CYCLE_COUNTER_VALUE = 9223372036854775807n;
export declare const MAX_PARTICIPANT_ID_LENGTH = 128;
export declare const MAX_DOMAIN_ID_LENGTH = 128;
export declare const MIN_INCARNATION = 1n;
export declare const MAX_INCARNATION = 9223372036854775807n;
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
export interface CycleCounterState {
    name: string;
    value: bigint;
    paused: boolean;
    generation: bigint;
    ownerParticipantId: string | null;
}
export interface DomainState {
    domainId: string;
    domainAuthKey: Uint8Array;
    brokerEpoch: string;
    revision: bigint;
    activityGeneration: bigint;
    participants: Map<string, ParticipantLease>;
    reservations: Map<string, ReservationLease>;
    cycleCounters: Map<string, CycleCounterState>;
    /** True when the membership/lease state is authoritatively known. */
    certain: boolean;
    /** When set, certainty may become true only after this timestamp (recovery window). */
    recoveryDeadline: number | null;
    /** A restarted domain needs at least one authenticated rejoin before recovery can finish. */
    recoveryParticipantSeen: boolean;
    createdAt: number;
}
export declare function newDomain(domainId: string, domainAuthKey: Uint8Array, opts?: {
    recover?: boolean;
}): DomainState;
export declare function makeParticipantId(): string;
export declare function makeResumeKey(): string;
export declare function makeReservationId(): string;
export declare function hashToken(token: Uint8Array): string;
export declare function reservationToken(): Uint8Array;
export declare function cycleCounterSnapshot(counter: CycleCounterState): CycleCounterSnapshot;
export declare function snapshotOf(state: DomainState): DomainSnapshot;
/** Wire representation of a snapshot (bigint as decimal string). */
export declare function snapshotToWire(s: DomainSnapshot): Record<string, unknown>;
