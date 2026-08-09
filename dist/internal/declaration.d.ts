/**
 * Domain environment declaration and bootstrap.
 *
 * Environment variable names:
 *   PI_PROCESS_DOMAIN_ID          -- domain id (128 random bits, base64url)
 *   PI_PROCESS_DOMAIN_KEY         -- 32 random bytes, base64url
 *   PI_PROCESS_DOMAIN_PROTOCOL    -- "<major>.<minor>"
 *   PI_PROCESS_DOMAIN_RESERVATION -- optional single-use child claim
 *
 * If no declaration variable is present, openDomain() creates a new domain and
 * a cryptographically random 32-byte key, writes the complete declaration to
 * process.env, and joins it. If any declaration variable is present, the whole
 * group must be present and valid; malformed/missing/wrong fields are fatal.
 */
export declare const ENV: {
    readonly DOMAIN_ID: "PI_PROCESS_DOMAIN_ID";
    readonly DOMAIN_KEY: "PI_PROCESS_DOMAIN_KEY";
    readonly PROTOCOL: "PI_PROCESS_DOMAIN_PROTOCOL";
    readonly RESERVATION: "PI_PROCESS_DOMAIN_RESERVATION";
};
export interface DomainDeclaration {
    domainId: string;
    /** Raw 32-byte environment key (capability, not to be logged). */
    domainKey: Uint8Array;
    protocolMajor: number;
    protocolMinor: number;
}
export declare function protocolString(): string;
/** Create a fresh domain declaration, publishing it unless explicitly deferred. */
export declare function createDeclaration(publish?: boolean): DomainDeclaration;
/** Publish a complete declaration only after its root broker and lease are ready. */
export declare function publishDeclaration(declaration: DomainDeclaration): void;
export declare function readDeclaration(): DomainDeclaration | null;
/** Base64url encode a reservation claim for environment transport. */
export declare function encodeReservationClaim(token: Uint8Array): string;
export declare function decodeReservationClaim(value: string | undefined): Uint8Array | null;
