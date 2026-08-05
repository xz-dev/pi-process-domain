/**
 * Lazy broker launch / election.
 *
 * When a client finds no broker at the deterministic endpoint, it races for a
 * single per-user atomic lock record (an atomically created lock directory with
 * an ownership/claim file). Exactly one contender wins the `mkdir` and launches
 * the detached broker process; every other contender retries the socket until
 * the winner's broker becomes available.
 *
 * Stale lock/socket recovery:
 *   - a lock whose claim has expired AND whose owner PID is gone is removed and
 *     re-claimed (single-winner recovery, never two active brokers);
 *   - a stale Unix socket file is removed by the broker only after probing the
 *     endpoint and proving no live broker is listening (see broker.ts).
 */
/** Per-user lock directory, shared by all contenders regardless of cwd. */
export declare function lockDir(): string;
export declare function releaseElection(): void;
/** Atomically claim the startup lock. Returns true if this process won. */
export declare function tryAcquireElection(ttlMs?: number): boolean;
/** Remove a stale lock whose claim has expired and whose owner PID is gone. */
export declare function reclaimStaleElection(): void;
/** Launch the detached broker process with ignored stdio. */
export declare function spawnBrokerProcess(): void;
/** Launch broker if elected; retry the socket until it responds or deadline. */
export declare function startBrokerProcess(connectTimeoutMs?: number): Promise<void>;
/** Poll the endpoint until a broker accepts connections or the deadline passes. */
export declare function waitForBroker(timeoutMs: number): Promise<void>;
