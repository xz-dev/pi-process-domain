/**
 * Secure broker endpoint resolution.
 *
 * Protocol 2 endpoints are scoped by a bounded hash of the public domain ID;
 * protocol 1 retains the historical per-user endpoint for existing sessions.
 *
 * Security: every existing component of the runtime directory is validated with
 * no-follow metadata (must be an absolute path, a real directory, owned by the
 * current user, and not a symlink). The directory is created with `0700`. A
 * pre-existing unsafe path or a symlink in any ancestor that escapes the base is
 * a fatal `RUNTIME_UNSAFE` error — never followed or chmod'ed.
 */
export interface EndpointIdentity {
    readonly protocolMajor: number;
    readonly domainId: string;
}
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
export declare function resolveEndpointFor(input: PlatformInput, identity?: EndpointIdentity): RuntimeEndpoint;
export declare function resolveEndpoint(identity?: EndpointIdentity): RuntimeEndpoint;
