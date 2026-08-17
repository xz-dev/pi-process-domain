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
import { type DomainDeclaration } from "./declaration.js";
import { type ActivityState, type CycleCounterSnapshot, type DomainFence, type DomainSignal, type DomainSnapshot } from "./domain-types.js";
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
export declare class DomainClient {
    private endpoint;
    private options;
    private authKey;
    private raw;
    private peer;
    private participantId;
    private resumeKey;
    private incarnation;
    private lastSnapshot;
    private listeners;
    private signalListeners;
    private cycleCounterListeners;
    private closed;
    private fatalEmitted;
    private reconnectTimer;
    private heartbeatTimer;
    private brokerEpoch;
    private pendingConfirms;
    private joining;
    private everJoined;
    constructor(options: ClientOptions);
    open(): Promise<DomainClient>;
    /**
     * Single join path. Attempts a fresh authenticated join (with optional
     * resume identity) and retries the whole flow through broker election.
     * Resolves only after the join succeeds; rejects on fatal errors.
     */
    private joinThroughElection;
    private sleep;
    private resetTransport;
    private connectAndJoin;
    private performHandshakeAndJoin;
    private upgradeToPeer;
    /** The single authenticated join (initial or reconnect resume). */
    private join;
    private startHeartbeat;
    private applySnapshot;
    private flushPendingConfirms;
    private scheduleReconnect;
    private rejoinAfterRuntimeLoss;
    private certainToUncertain;
    snapshot(): DomainSnapshot;
    setActivity(state: ActivityState): Promise<DomainSnapshot>;
    reserveSpawn(options?: {
        ttlMs?: number;
    }): Promise<{
        env: Record<string, string>;
        token: string;
        cancel: () => Promise<void>;
    }>;
    subscribe(listener: (snapshot: DomainSnapshot) => void): () => void;
    claimCycleCounter(name: string): Promise<CycleCounterSnapshot>;
    getCycleCounter(name: string): Promise<CycleCounterSnapshot>;
    subscribeCycleCounter(name: string, listener: (counter: CycleCounterSnapshot) => void): () => void;
    private dispatchCycleCounter;
    incrementCycleCounter(name: string, delta?: bigint, generation?: bigint): Promise<CycleCounterSnapshot>;
    resetCycleCounter(name: string, generation: bigint): Promise<CycleCounterSnapshot>;
    setCycleCounterPaused(name: string, paused: boolean, generation: bigint): Promise<CycleCounterSnapshot>;
    publish(name: string, value: unknown): Promise<void>;
    subscribeSignals(name: string, listener: (signal: DomainSignal) => void): () => void;
    private dispatchSignal;
    confirm(fence: DomainFence): Promise<boolean>;
    close(): Promise<void>;
    private requirePeer;
    private emitFatal;
}
