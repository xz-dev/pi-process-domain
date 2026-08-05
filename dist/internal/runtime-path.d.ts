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
export interface RuntimeEndpoint {
    /** Directory holding broker metadata/journal (empty on win32). */
    runtimeDir: string;
    /** Endpoint path passed to net.Server.listen / net.Socket.connect. */
    endpointPath: string;
    platform: "unix" | "win32";
}
export interface PlatformInput {
    platform: NodeJS.Platform;
    xdg?: string;
    uid?: number;
    username?: string;
    tmpdir: string;
    userInfo?: () => {
        username: string;
    };
}
/**
 * Pure endpoint derivation for a given platform/tmpdir/user. Testable across
 * all platforms on any host; `resolveEndpoint()` is a thin wrapper feeding the
 * live process values. This function does not touch the filesystem.
 */
export declare function resolveEndpointFor(input: PlatformInput): RuntimeEndpoint;
export declare function resolveEndpoint(): RuntimeEndpoint;
