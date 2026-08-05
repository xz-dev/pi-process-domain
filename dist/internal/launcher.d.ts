/**
 * Lazy broker launch / election.
 *
 * When a client finds no broker at the deterministic endpoint, it races for a
 * single per-user atomic lock record (an atomically created lock directory with
 * an ownership/claim file). Exactly one contender wins the `mkdir` and launches
 * the detached broker process; the broker then atomically takes ownership of the
 * claim with its own PID. Every other contender retries the socket until the
 * winner's broker becomes available.
 *
 * Stale lock/socket recovery:
 *   - a startup claim (`pid: 0`) is removed after its bounded launch window;
 *   - a transferred broker claim is removed as soon as its broker PID is gone;
 *   - a stale Unix socket file is removed by the broker only after probing the
 *     endpoint and proving no live broker is listening (see broker.ts).
 */
export declare const ELECTION_OWNER_ENV = "PI_PROCESS_DOMAIN_ELECTION_OWNER";
/** Per-user lock directory, shared by all contenders regardless of cwd. */
export declare function lockDir(): string;
export declare function releaseElection(owner: string): void;
/** Atomically claim the startup lock. Returns its transfer token if won. */
export declare function tryAcquireElection(ttlMs?: number): string | null;
/** Atomically transfer a won startup claim to the actual broker process. */
export declare function claimElectionForBroker(owner: string): boolean;
/** Remove an expired startup claim or a claim owned by a dead broker. */
export declare function reclaimStaleElection(): void;
/** Launch the detached broker process with ignored stdio. */
export declare function spawnBrokerProcess(electionOwner: string): void;
/** Launch broker if elected; retry the socket until it responds or deadline. */
export declare function startBrokerProcess(connectTimeoutMs?: number): Promise<void>;
/** Poll the endpoint until a broker accepts connections or the deadline passes. */
export declare function waitForBroker(timeoutMs: number): Promise<void>;
