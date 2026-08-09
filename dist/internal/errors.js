/**
 * Typed fatal errors for pi-process-domain.
 */
export const LEGACY_PROCESS_DOMAIN_PROTOCOL_MAJOR = 1;
export const EMBEDDED_PROCESS_DOMAIN_PROTOCOL_MAJOR = 2;
export const PROCESS_DOMAIN_PROTOCOL_MAJOR = EMBEDDED_PROCESS_DOMAIN_PROTOCOL_MAJOR;
export const PROCESS_DOMAIN_PROTOCOL_MINOR = 0;
export function isSupportedProtocol(major, minor) {
    return minor === PROCESS_DOMAIN_PROTOCOL_MINOR &&
        (major === LEGACY_PROCESS_DOMAIN_PROTOCOL_MAJOR || major === EMBEDDED_PROCESS_DOMAIN_PROTOCOL_MAJOR);
}
export class ProcessDomainFatalError extends Error {
    code;
    isProcessDomainFatalError = true;
    constructor(code, message, cause) {
        super(message);
        this.name = "ProcessDomainFatalError";
        this.code = code;
        if (cause !== undefined) {
            this.cause = cause;
        }
    }
}
/** Stable fatal exit code used by the CLI harness / declared-domain processes. */
export const FATAL_EXIT_CODE = 78;
export function isProcessDomainFatalError(value) {
    return (value instanceof Error &&
        value.isProcessDomainFatalError === true);
}
