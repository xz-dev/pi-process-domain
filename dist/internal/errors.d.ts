/**
 * Typed fatal errors for pi-process-domain.
 */
export declare const LEGACY_PROCESS_DOMAIN_PROTOCOL_MAJOR: 1;
export declare const EMBEDDED_PROCESS_DOMAIN_PROTOCOL_MAJOR: 2;
export declare const PROCESS_DOMAIN_PROTOCOL_MAJOR: 2;
export declare const PROCESS_DOMAIN_PROTOCOL_MINOR: 0;
export declare function isSupportedProtocol(major: number, minor: number): boolean;
export type ProcessDomainFatalCode = "INVALID_DECLARATION" | "AUTHENTICATION_FAILED" | "DOMAIN_ABSENT" | "BROKER_UNAVAILABLE" | "PROTOCOL_MISMATCH" | "DOMAIN_UNRECOVERABLE" | "LEASE_REJECTED" | "RUNTIME_UNSAFE";
export declare class ProcessDomainFatalError extends Error {
    readonly code: ProcessDomainFatalCode;
    readonly isProcessDomainFatalError: true;
    constructor(code: ProcessDomainFatalCode, message: string, cause?: unknown);
}
/** Stable fatal exit code used by the CLI harness / declared-domain processes. */
export declare const FATAL_EXIT_CODE = 78;
export declare function isProcessDomainFatalError(value: unknown): value is ProcessDomainFatalError;
