/**
 * pi-process-domain — public entry point.
 *
 * A small, deep API for authenticated cross-Pi process-domain coordination.
 * Fresh roots host one single-domain broker in-process; inherited descendants
 * authenticate over that domain's private endpoint. Protocol 1 declarations
 * retain the legacy shared-client path for already-running sessions only.
 */
import { ProcessDomainFatalError, isProcessDomainFatalError, FATAL_EXIT_CODE } from "./internal/errors.js";
import type { ActivityState, DomainFence, DomainSignal, DomainSnapshot, OpenDomainOptions, ProcessDomain, SpawnReservation } from "./internal/domain-types.js";
export type { ActivityState, DomainFence, DomainSignal, DomainSnapshot, OpenDomainOptions, ProcessDomain, SpawnReservation, };
export { ProcessDomainFatalError, isProcessDomainFatalError, FATAL_EXIT_CODE };
/** Environment variable names that form a domain declaration. */
export declare const ENV_NAMES: {
    readonly DOMAIN_ID: "PI_PROCESS_DOMAIN_ID";
    readonly DOMAIN_KEY: "PI_PROCESS_DOMAIN_KEY";
    readonly PROTOCOL: "PI_PROCESS_DOMAIN_PROTOCOL";
    readonly RESERVATION: "PI_PROCESS_DOMAIN_RESERVATION";
};
export interface OpenDomainResult {
    /** The process-domain handle (already joined as a participant). */
    domain: ProcessDomain;
    /** True when this process created a brand-new domain declaration. */
    created: boolean;
}
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
export declare function openDomain(options?: Partial<OpenDomainOptions>): Promise<OpenDomainResult>;
/** Resolve the current declaration's broker endpoint (legacy when undeclared). */
export declare function brokerEndpoint(): string;
