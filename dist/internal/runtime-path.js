/**
 * Per-user broker endpoint resolution and secure runtime directory.
 *
 * - Unix (Linux/macOS/FreeBSD): a private per-UID runtime directory containing
 *   `broker.sock` (Unix pathname socket). Prefer XDG_RUNTIME_DIR when present,
 *   otherwise a private `0700` per-UID directory under os.tmpdir().
 * - Windows: a named pipe `\\.\pipe\pi-process-domain-v1-<user-hash>`.
 *
 * Security: every existing component of the runtime directory is validated with
 * no-follow metadata (must be an absolute path, a real directory, owned by the
 * current user, and not a symlink). The directory is created with `0700`. A
 * pre-existing unsafe path or a symlink in any ancestor that escapes the base is
 * a fatal `RUNTIME_UNSAFE` error — never followed or chmod'ed.
 */
import * as os from "node:os";
import * as fs from "node:fs";
import * as path from "node:path";
import { createHash } from "node:crypto";
import { ProcessDomainFatalError } from "./errors.js";
/**
 * Pure endpoint derivation for a given platform/tmpdir/user. Testable across
 * all platforms on any host; `resolveEndpoint()` is a thin wrapper feeding the
 * live process values. This function does not touch the filesystem.
 */
export function resolveEndpointFor(input) {
    const platform = input.platform;
    const uid = input.uid;
    const user = input.username ?? input.userInfo?.().username ?? "user";
    const uidMaterial = uid !== undefined && Number.isInteger(uid) ? String(uid) : user;
    const uidTagOut = uidMaterial === user
        ? createHash("sha256").update(user).digest("hex").slice(0, 16)
        : uidMaterial;
    const userMaterial = uid !== undefined && Number.isInteger(uid) ? `uid:${uid}` : `user:${user}`;
    const userHashOut = createHash("sha256").update(userMaterial).digest("hex").slice(0, 16);
    if (platform === "win32") {
        return {
            runtimeDir: "",
            endpointPath: `\\\\.\\pipe\\pi-process-domain-v1-${userHashOut}`,
            platform: "win32",
        };
    }
    let base;
    if (input.xdg && input.xdg.length > 0) {
        base = `${input.xdg}/pi-process-domain/v1`;
    }
    else {
        base = `${input.tmpdir}/pi-process-domain/uid-${uidTagOut}`;
    }
    return {
        runtimeDir: base,
        endpointPath: `${base}/broker.sock`,
        platform: "unix",
    };
}
/**
 * Walk an absolute path from the root and verify no component is a symlink and
 * every component is a real directory. Throws ProcessDomainFatalError on any
 * violation. Prevents path traversal / symlink redirection of the runtime dir.
 */
function assertNoSymlinkTraversal(absDir) {
    if (!path.isAbsolute(absDir)) {
        throw new ProcessDomainFatalError("RUNTIME_UNSAFE", `runtime path is not absolute: ${absDir}`);
    }
    const normalized = path.normalize(absDir);
    const parts = normalized.split(path.sep).filter((p) => p.length > 0);
    let current = path.parse(normalized).root || path.sep;
    for (const part of parts) {
        current = path.join(current, part);
        let st;
        try {
            st = fs.lstatSync(current);
        }
        catch {
            // Component doesn't exist yet: the directory will be created later. For
            // ancestors we only require that existing components are safe.
            continue;
        }
        if (st.isSymbolicLink()) {
            throw new ProcessDomainFatalError("RUNTIME_UNSAFE", `runtime path component is a symlink: ${current}`);
        }
        if (!st.isDirectory()) {
            throw new ProcessDomainFatalError("RUNTIME_UNSAFE", `runtime path component is not a directory: ${current}`);
        }
    }
}
/** Validate that the XDG_RUNTIME_DIR is absolute and not a symlink, and create the domain dir safely. */
function ensurePrivateDir(base, runtimeDir) {
    assertNoSymlinkTraversal(runtimeDir);
    if (base) {
        const normalizedBase = path.resolve(base);
        const normalizedRuntime = path.resolve(runtimeDir);
        if (normalizedRuntime !== normalizedBase && !normalizedRuntime.startsWith(`${normalizedBase}${path.sep}`)) {
            throw new ProcessDomainFatalError("RUNTIME_UNSAFE", "runtime directory escapes its configured base");
        }
        let baseStat;
        try {
            baseStat = fs.lstatSync(normalizedBase);
        }
        catch (err) {
            throw new ProcessDomainFatalError("RUNTIME_UNSAFE", `cannot stat runtime base: ${normalizedBase}`, err);
        }
        if (baseStat.isSymbolicLink() || !baseStat.isDirectory()) {
            throw new ProcessDomainFatalError("RUNTIME_UNSAFE", `runtime base is unsafe: ${normalizedBase}`);
        }
        const uid = process.getuid?.();
        if (uid !== undefined && baseStat.uid !== uid) {
            throw new ProcessDomainFatalError("RUNTIME_UNSAFE", `runtime base not owned by current user: ${normalizedBase}`);
        }
    }
    // Create the runtime directory and its ancestors with restrictive modes using
    // lstat-no-follow checks; refuse pre-existing unsafe (symlink/non-dir) targets.
    try {
        fs.mkdirSync(runtimeDir, { recursive: true, mode: 0o700 });
    }
    catch (err) {
        throw new ProcessDomainFatalError("RUNTIME_UNSAFE", `cannot create runtime directory: ${runtimeDir}`, err);
    }
    // Verify ownership and mode of the final directory (no-follow).
    let st;
    try {
        st = fs.lstatSync(runtimeDir);
    }
    catch (err) {
        throw new ProcessDomainFatalError("RUNTIME_UNSAFE", `cannot stat runtime directory: ${runtimeDir}`, err);
    }
    if (st.isSymbolicLink() || !st.isDirectory()) {
        throw new ProcessDomainFatalError("RUNTIME_UNSAFE", `runtime directory is a symlink or not a directory: ${runtimeDir}`);
    }
    // Only chmod if we can confirm current ownership; chmod a non-owned dir is a
    // no-op risk, but the mkdir above used 0700 for newly created dirs.
    const uid = process.getuid?.();
    if (uid !== undefined && st.uid !== uid) {
        throw new ProcessDomainFatalError("RUNTIME_UNSAFE", `runtime directory not owned by current user: ${runtimeDir}`);
    }
    if ((st.mode & 0o077) !== 0) {
        try {
            fs.chmodSync(runtimeDir, 0o700);
        }
        catch (err) {
            throw new ProcessDomainFatalError("RUNTIME_UNSAFE", `cannot secure runtime directory: ${runtimeDir}`, err);
        }
        const secured = fs.lstatSync(runtimeDir);
        if ((secured.mode & 0o077) !== 0) {
            throw new ProcessDomainFatalError("RUNTIME_UNSAFE", `runtime directory permissions are too broad: ${runtimeDir}`);
        }
    }
}
export function resolveEndpoint() {
    const ep = resolveEndpointFor({
        platform: process.platform,
        xdg: process.env.XDG_RUNTIME_DIR,
        uid: process.getuid?.(),
        username: os.userInfo?.().username,
        tmpdir: os.tmpdir(),
        userInfo: os.userInfo,
    });
    // Ensure the private runtime directory exists on unix (broker/socket target).
    if (ep.platform === "unix" && ep.runtimeDir.length > 0) {
        ensurePrivateDir(process.env.XDG_RUNTIME_DIR ?? "", ep.runtimeDir);
    }
    return ep;
}
