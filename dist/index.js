/**
 * pi-process-domain — public entry point.
 *
 * A small, deep API for authenticated cross-Pi process-domain coordination.
 * One per-user broker is launched lazily on demand; clients authenticate over
 * an HMAC-SHA-256 challenge and share an immutable aggregate snapshot of a
 * domain (exact revisions, activity generation, busy/idle, spawn reservations,
 * and lease/disconnect certainty). This module exposes only the public surface;
 * birpc and the wire protocol are internal implementation details.
 */
import { DomainClient } from "./internal/client.js";
import { createDeclaration, readDeclaration } from "./internal/declaration.js";
import { ProcessDomainFatalError, isProcessDomainFatalError, FATAL_EXIT_CODE } from "./internal/errors.js";
import { resolveEndpoint } from "./internal/runtime-path.js";
export { ProcessDomainFatalError, isProcessDomainFatalError, FATAL_EXIT_CODE };
/** Environment variable names that form a domain declaration. */
export const ENV_NAMES = {
    DOMAIN_ID: "PI_PROCESS_DOMAIN_ID",
    DOMAIN_KEY: "PI_PROCESS_DOMAIN_KEY",
    PROTOCOL: "PI_PROCESS_DOMAIN_PROTOCOL",
    RESERVATION: "PI_PROCESS_DOMAIN_RESERVATION",
};
const DEFAULT_CONNECT_TIMEOUT_MS = 10_000;
/**
 * Open (join) a process domain.
 *
 * If a complete domain declaration exists in the environment (set by a parent
 * that called `openDomain()` with `created === true`), this joins that existing
 * domain with the declared key. Otherwise a new domain and a fresh 32-byte key
 * are created and written to the environment for descendants to inherit.
 *
 * Fail-closed: any partial/malformed/wrong declaration is a fatal error rather
 * than being silently ignored or regenerated.
 */
export async function openDomain(options) {
    const defaultFatal = (error) => {
        process.exitCode = FATAL_EXIT_CODE;
        console.error(`pi-process-domain fatal: ${error.message}`);
    };
    const onFatal = options?.onFatal ?? defaultFatal;
    let client = null;
    try {
        const existing = readDeclaration();
        const created = existing === null;
        const declaration = existing ?? createDeclaration();
        const timeout = options?.connectTimeoutMs ?? DEFAULT_CONNECT_TIMEOUT_MS;
        if (!Number.isFinite(timeout) || timeout <= 0) {
            throw new ProcessDomainFatalError("INVALID_DECLARATION", "connectTimeoutMs must be a positive finite number");
        }
        client = new DomainClient({
            declaration,
            initialActivity: options?.initialActivity ?? "idle",
            metadata: options?.metadata ?? {},
            connectTimeoutMs: timeout,
            onFatal,
            createDomain: created,
        });
        await client.open();
        return { domain: client, created };
    }
    catch (error) {
        const fatal = isProcessDomainFatalError(error)
            ? error
            : new ProcessDomainFatalError("INVALID_DECLARATION", "failed to configure process domain", error);
        if (client === null) {
            if (options?.onFatal === undefined)
                defaultFatal(fatal);
            else {
                process.exitCode = FATAL_EXIT_CODE;
                try {
                    onFatal(fatal);
                }
                catch { /* preserve the original typed failure */ }
            }
        }
        throw fatal;
    }
}
/** Resolve the per-user broker endpoint (path or named pipe) for diagnostics. */
export function brokerEndpoint() {
    return resolveEndpoint().endpointPath;
}
