/**
 * Public domain snapshot types. These are the immutable, broker-committed view
 * of one authenticated domain at one exact broker revision.
 */
export type ActivityState = "busy" | "idle";
export interface DomainSnapshot {
    readonly domainId: string;
    readonly brokerEpoch: string;
    readonly revision: bigint;
    readonly activityGeneration: bigint;
    readonly participants: number;
    readonly busyParticipants: number;
    readonly pendingSpawns: number;
    readonly allIdle: boolean;
    readonly certain: boolean;
    readonly fence: DomainFence;
}
export interface DomainFence {
    readonly brokerEpoch: string;
    readonly activityGeneration: bigint;
}
export interface SpawnReservation {
    readonly env: Readonly<Record<string, string>>;
    cancel(): Promise<void>;
}
export interface DomainSignal {
    readonly name: string;
    readonly value: unknown;
    readonly brokerEpoch: string;
    readonly revision: bigint;
    readonly senderId: string;
}
export interface ProcessDomain {
    snapshot(): DomainSnapshot;
    setActivity(state: ActivityState): Promise<DomainSnapshot>;
    reserveSpawn(options?: {
        ttlMs?: number;
    }): Promise<SpawnReservation>;
    subscribe(listener: (snapshot: DomainSnapshot) => void): () => void;
    publish(name: string, value: unknown): Promise<void>;
    subscribeSignals(name: string, listener: (signal: DomainSignal) => void): () => void;
    confirm(fence: DomainFence): Promise<boolean>;
    close(): Promise<void>;
}
export interface OpenDomainOptions {
    readonly initialActivity?: ActivityState;
    readonly metadata?: Readonly<Record<string, string>>;
    readonly connectTimeoutMs?: number;
    readonly onFatal: (error: Error) => void;
}
/** Compute the immutable aggregate snapshot from committed state counters. */
export declare function computeSnapshot(input: {
    domainId: string;
    brokerEpoch: string;
    revision: bigint;
    activityGeneration: bigint;
    participants: number;
    busyParticipants: number;
    pendingSpawns: number;
    certain: boolean;
}): DomainSnapshot;
