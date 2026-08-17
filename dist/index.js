/**
 * pi-process-domain — public entry point.
 *
 * A small, deep API for authenticated cross-Pi process-domain coordination.
 * Fresh roots host one single-domain broker in-process; inherited descendants
 * authenticate over that domain's private endpoint. Protocol 1 declarations
 * retain the legacy shared-client path for already-running sessions only.
 */
import { timingSafeEqual } from "node:crypto";
import { Broker } from "./internal/broker.js";
import { DomainClient } from "./internal/client.js";
import { createDeclaration, publishDeclaration, readDeclaration, } from "./internal/declaration.js";
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
const PROCESS_DOMAIN_OWNER = Symbol.for("pi-process-domain:embedded-root:v2");
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
        const host = globalThis;
        await Promise.resolve();
        let owned = host[PROCESS_DOMAIN_OWNER];
        if (owned?.closing !== null && owned?.closing !== undefined) {
            await owned.closing;
            owned = host[PROCESS_DOMAIN_OWNER];
        }
        const existing = readDeclaration();
        if (owned !== undefined && existing !== null && !sameDeclaration(existing, owned.declaration)) {
            throw new ProcessDomainFatalError("INVALID_DECLARATION", "process already owns a different embedded process domain");
        }
        const created = existing === null && owned === undefined;
        const declaration = existing ?? owned?.declaration ?? createDeclaration(false);
        const timeout = options?.connectTimeoutMs ?? DEFAULT_CONNECT_TIMEOUT_MS;
        if (!Number.isFinite(timeout) || timeout <= 0) {
            throw new ProcessDomainFatalError("INVALID_DECLARATION", "connectTimeoutMs must be a positive finite number");
        }
        let root = owned;
        if (created) {
            const broker = new Broker({
                endpoint: resolveEndpoint(declaration),
                domain: { domainId: declaration.domainId, domainKey: declaration.domainKey },
            });
            root = { declaration, broker, ready: broker.start(), closing: null, references: 0 };
            host[PROCESS_DOMAIN_OWNER] = root;
        }
        if (root !== undefined) {
            root.references += 1;
            try {
                await root.ready;
            }
            catch (error) {
                root.references -= 1;
                if (root.references === 0)
                    await closeEmbeddedRoot(host, root);
                throw error;
            }
        }
        client = new DomainClient({
            declaration,
            initialActivity: options?.initialActivity ?? "idle",
            metadata: options?.metadata ?? {},
            connectTimeoutMs: timeout,
            onFatal,
            createDomain: false,
        });
        try {
            await client.open();
        }
        catch (error) {
            if (root !== undefined) {
                root.references -= 1;
                if (root.references === 0)
                    await closeEmbeddedRoot(host, root);
            }
            throw error;
        }
        if (root !== undefined && process.env[ENV_NAMES.DOMAIN_ID] === undefined) {
            publishDeclaration(root.declaration);
        }
        if (root === undefined)
            return { domain: client, created, hosted: false };
        return { domain: ownedDomain(client, host, root), created, hosted: true };
    }
    catch (error) {
        const fatal = isProcessDomainFatalError(error)
            ? error
            : new ProcessDomainFatalError("INVALID_DECLARATION", "failed to configure process domain", error);
        process.exitCode = FATAL_EXIT_CODE;
        if (client === null) {
            if (options?.onFatal === undefined)
                defaultFatal(fatal);
            else
                try {
                    onFatal(fatal);
                }
                catch { /* preserve the original typed failure */ }
        }
        throw fatal;
    }
}
function ownedDomain(client, host, root) {
    let closed = false;
    return {
        snapshot: () => client.snapshot(),
        setActivity: (state) => client.setActivity(state),
        reserveSpawn: (options) => client.reserveSpawn(options),
        subscribe: (listener) => client.subscribe(listener),
        publish: (name, value) => client.publish(name, value),
        subscribeSignals: (name, listener) => client.subscribeSignals(name, listener),
        claimCycleCounter: (name) => client.claimCycleCounter(name),
        getCycleCounter: (name) => client.getCycleCounter(name),
        subscribeCycleCounter: (name, listener) => client.subscribeCycleCounter(name, listener),
        incrementCycleCounter: (name, delta, generation) => client.incrementCycleCounter(name, delta, generation),
        resetCycleCounter: (name, generation) => client.resetCycleCounter(name, generation),
        setCycleCounterPaused: (name, paused, generation) => client.setCycleCounterPaused(name, paused, generation),
        confirm: (fence) => client.confirm(fence),
        async close() {
            if (closed)
                return;
            closed = true;
            await client.close();
            root.references -= 1;
            if (root.references === 0 && host[PROCESS_DOMAIN_OWNER] === root) {
                const closing = closeEmbeddedRoot(host, root);
                await Promise.resolve();
                await closing;
            }
        },
    };
}
async function closeEmbeddedRoot(host, root) {
    if (root.closing !== null)
        return root.closing;
    root.closing = (async () => {
        await root.broker.close();
        if (process.env[ENV_NAMES.DOMAIN_ID] === root.declaration.domainId &&
            process.env[ENV_NAMES.PROTOCOL] === `${root.declaration.protocolMajor}.${root.declaration.protocolMinor}`)
            clearDeclaration();
        if (host[PROCESS_DOMAIN_OWNER] === root)
            delete host[PROCESS_DOMAIN_OWNER];
    })();
    return root.closing;
}
function sameDeclaration(left, right) {
    return left.domainId === right.domainId &&
        left.protocolMajor === right.protocolMajor &&
        left.protocolMinor === right.protocolMinor &&
        left.domainKey.length === right.domainKey.length &&
        timingSafeEqual(left.domainKey, right.domainKey);
}
function clearDeclaration() {
    delete process.env[ENV_NAMES.DOMAIN_ID];
    delete process.env[ENV_NAMES.DOMAIN_KEY];
    delete process.env[ENV_NAMES.PROTOCOL];
    delete process.env[ENV_NAMES.RESERVATION];
}
/** Resolve the current declaration's broker endpoint (legacy when undeclared). */
export function brokerEndpoint() {
    return resolveEndpoint(readDeclaration() ?? undefined).endpointPath;
}
