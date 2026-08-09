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
import { type RuntimeEndpoint } from "./runtime-path.js";
export interface BrokerOptions {
    endpoint: RuntimeEndpoint;
    electionOwner?: string;
    /** Embedded brokers serve exactly one authenticated domain. */
    domain?: {
        readonly domainId: string;
        readonly domainKey: Uint8Array;
    };
}
export declare class Broker {
    private options;
    private domains;
    private conns;
    private server;
    private electionOwner;
    private expiryTimers;
    private listening;
    private closed;
    constructor(options: BrokerOptions);
    start(): Promise<void>;
    /** Connect to the endpoint and confirm a live broker answers (or the socket is absent). */
    private probeEndpoint;
    private listen;
    private onConnection;
    private onHandshake;
    private upgradeToPeer;
    private clearReservationTimer;
    private expireReservation;
    private requireParticipant;
    /**
     * Real heartbeat liveness: only received `heartbeat` RPCs refresh lastSeen.
     * This periodic sweep detects stale (suspected) and expired participants based
     * on that received-evidence timestamp and drives certainty transitions.
     */
    private startHeartbeat;
    private expireParticipant;
    /** Recompute certainty without publishing; true means callers must publish. */
    private recomputeCertain;
    private removeParticipant;
    private teardown;
    private broadcastSnapshot;
    private broadcastSignal;
    close(): Promise<void>;
}
/** Launch a broker for the current user endpoint (used by the CLI harness). */
export declare function launchBrokerForCurrentUser(electionOwner?: string): Promise<Broker>;
