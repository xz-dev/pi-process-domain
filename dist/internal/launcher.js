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
import * as fs from "node:fs";
import * as path from "node:path";
import * as os from "node:os";
import * as net from "node:net";
import { spawn } from "node:child_process";
import { randomBytes, createHash } from "node:crypto";
import { fileURLToPath } from "node:url";
import { resolveEndpoint } from "./runtime-path.js";
const ELECTION_TTL_MS = 15000;
export const ELECTION_OWNER_ENV = "PI_PROCESS_DOMAIN_ELECTION_OWNER";
function ownerId() {
    return `${process.pid}-${randomBytes(6).toString("hex")}`;
}
/** Per-user lock directory, shared by all contenders regardless of cwd. */
export function lockDir() {
    const endpoint = resolveEndpoint();
    if (endpoint.platform === "unix")
        return path.join(endpoint.runtimeDir, "election");
    const user = os.userInfo?.().username ?? "user";
    const tag = createHash("sha256").update(`user:${user}`).digest("hex").slice(0, 16);
    return path.join(os.tmpdir(), "pi-process-domain-lock", tag);
}
function lockPath() {
    return path.join(lockDir(), "election.lock");
}
export function releaseElection(owner) {
    try {
        const claim = readClaim();
        if (claim?.ownerId !== owner)
            return;
        fs.rmSync(lockPath(), { recursive: true, force: true });
    }
    catch {
        /* broker bind remains the final single-owner authority */
    }
}
/** Atomically claim the startup lock. Returns its transfer token if won. */
export function tryAcquireElection(ttlMs = ELECTION_TTL_MS) {
    const lp = lockPath();
    fs.mkdirSync(lockDir(), { recursive: true, mode: 0o700 });
    try {
        fs.chmodSync(lockDir(), 0o700);
    }
    catch { /* validated runtime dir remains authoritative */ }
    const claim = {
        ownerId: ownerId(),
        expires: Date.now() + ttlMs,
        pid: 0,
    };
    try {
        fs.mkdirSync(lp, { mode: 0o700 });
        try {
            fs.writeFileSync(path.join(lp, "claim.json"), JSON.stringify(claim), { mode: 0o600 });
        }
        catch {
            fs.rmSync(lp, { recursive: true, force: true });
            return null;
        }
        return claim.ownerId;
    }
    catch (err) {
        if (err.code === "EEXIST")
            return null;
        throw err;
    }
}
function readClaim() {
    try {
        const parsed = JSON.parse(fs.readFileSync(path.join(lockPath(), "claim.json"), "utf8"));
        if (typeof parsed.ownerId !== "string" ||
            !Number.isFinite(parsed.expires) ||
            !Number.isInteger(parsed.pid))
            return null;
        return parsed;
    }
    catch {
        return null;
    }
}
/** Atomically transfer a won startup claim to the actual broker process. */
export function claimElectionForBroker(owner) {
    const claim = readClaim();
    if (claim?.ownerId !== owner || claim.pid !== 0 || claim.expires <= Date.now())
        return false;
    const next = {
        ownerId: owner,
        expires: Number.MAX_SAFE_INTEGER,
        pid: process.pid,
    };
    const temporary = path.join(lockPath(), `claim.${process.pid}.json`);
    try {
        fs.writeFileSync(temporary, JSON.stringify(next), { mode: 0o600 });
        fs.renameSync(temporary, path.join(lockPath(), "claim.json"));
        return true;
    }
    catch {
        try {
            fs.rmSync(temporary, { force: true });
        }
        catch { /* ignore */ }
        return false;
    }
}
function isProcessAlive(pid) {
    try {
        process.kill(pid, 0);
        return true;
    }
    catch {
        return false;
    }
}
/** Remove an expired startup claim or a claim owned by a dead broker. */
export function reclaimStaleElection() {
    const lp = lockPath();
    if (!fs.existsSync(lp))
        return;
    const claim = readClaim();
    if (claim === null) {
        // Unreadable claim: remove the lock so recovery can proceed; ownership
        // enforcement (never two active brokers) is guaranteed by the atomic
        // mkdir + broker bind, not by this lock record.
        try {
            fs.rmSync(lp, { recursive: true, force: true });
        }
        catch {
            /* ignore */
        }
        return;
    }
    const stale = claim.expires <= Date.now() || (claim.pid > 0 && !isProcessAlive(claim.pid));
    if (!stale)
        return;
    try {
        const current = readClaim();
        if (current?.ownerId === claim.ownerId && current.pid === claim.pid) {
            fs.rmSync(lp, { recursive: true, force: true });
        }
    }
    catch {
        /* ignore */
    }
}
function brokerEntrypoint() {
    // In the source tree the broker bin is at <pkg>/bin/pi-process-domain-broker.mjs.
    // In the built dist the bin file is preserved relative to the package root.
    return path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../bin/pi-process-domain-broker.mjs");
}
/** Launch the detached broker process with ignored stdio. */
export function spawnBrokerProcess(electionOwner) {
    const child = spawn(process.execPath, [brokerEntrypoint()], {
        detached: true,
        stdio: "ignore",
        env: { ...process.env, [ELECTION_OWNER_ENV]: electionOwner },
    });
    child.unref();
}
/** Launch broker if elected; retry the socket until it responds or deadline. */
export async function startBrokerProcess(connectTimeoutMs = 10000) {
    reclaimStaleElection();
    const electionOwner = tryAcquireElection();
    if (electionOwner !== null) {
        spawnBrokerProcess(electionOwner);
        await waitForBroker(connectTimeoutMs);
    }
    else {
        await waitForBroker(connectTimeoutMs);
    }
}
/** Poll the endpoint until a broker accepts connections or the deadline passes. */
export async function waitForBroker(timeoutMs) {
    const ep = resolveEndpoint();
    const deadline = Date.now() + timeoutMs;
    for (;;) {
        if (Date.now() > deadline) {
            throw new Error("broker did not become available in time");
        }
        if (await canConnect(ep.endpointPath))
            return;
        await new Promise((r) => setTimeout(r, 100));
    }
}
function canConnect(endpointPath) {
    return new Promise((resolve) => {
        const socket = net.connect(endpointPath);
        const timer = setTimeout(() => {
            socket.destroy();
            resolve(false);
        }, 1000);
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
