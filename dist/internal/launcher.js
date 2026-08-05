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
import * as fs from "node:fs";
import * as path from "node:path";
import * as os from "node:os";
import * as net from "node:net";
import { spawn } from "node:child_process";
import { randomBytes } from "node:crypto";
import { createHash } from "node:crypto";
import { fileURLToPath } from "node:url";
import { resolveEndpoint } from "./runtime-path.js";
const ELECTION_TTL_MS = 15000;
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
export function releaseElection() {
    try {
        fs.rmSync(lockPath(), { recursive: true, force: true });
    }
    catch {
        /* broker bind remains the final single-owner authority */
    }
}
/** Atomically claim the startup lock. Returns true if this process won. */
export function tryAcquireElection(ttlMs = ELECTION_TTL_MS) {
    const lp = lockPath();
    fs.mkdirSync(lockDir(), { recursive: true, mode: 0o700 });
    try {
        fs.chmodSync(lockDir(), 0o700);
    }
    catch { /* validated runtime dir remains authoritative */ }
    const claim = { ownerId: ownerId(), expires: Date.now() + ttlMs, pid: process.pid };
    try {
        fs.mkdirSync(lp, { mode: 0o700 });
        try {
            fs.writeFileSync(path.join(lp, "claim.json"), JSON.stringify(claim), { mode: 0o600 });
        }
        catch {
            fs.rmSync(lp, { recursive: true, force: true });
            return false;
        }
        return true;
    }
    catch (err) {
        if (err.code === "EEXIST")
            return false;
        throw err;
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
/** Remove a stale lock whose claim has expired and whose owner PID is gone. */
export function reclaimStaleElection() {
    const lp = lockPath();
    if (!fs.existsSync(lp))
        return;
    let claim = null;
    try {
        const raw = fs.readFileSync(path.join(lp, "claim.json"), "utf8");
        claim = JSON.parse(raw);
    }
    catch {
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
    if (!isProcessAlive(claim.pid)) {
        try {
            fs.rmSync(lp, { recursive: true, force: true });
        }
        catch {
            /* ignore */
        }
    }
}
function brokerEntrypoint() {
    // In the source tree the broker bin is at <pkg>/bin/pi-process-domain-broker.mjs.
    // In the built dist the bin file is preserved relative to the package root.
    return path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../bin/pi-process-domain-broker.mjs");
}
/** Launch the detached broker process with ignored stdio. */
export function spawnBrokerProcess() {
    const child = spawn(process.execPath, [brokerEntrypoint()], {
        detached: true,
        stdio: "ignore",
        env: process.env,
    });
    child.unref();
}
/** Launch broker if elected; retry the socket until it responds or deadline. */
export async function startBrokerProcess(connectTimeoutMs = 10000) {
    reclaimStaleElection();
    const won = tryAcquireElection();
    if (won) {
        spawnBrokerProcess();
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
